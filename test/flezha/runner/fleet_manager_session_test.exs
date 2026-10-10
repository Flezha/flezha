defmodule FlezhaTest.Runner.FleetManagerSessionTest do
  @moduledoc """
  Reconnect binding continuity (runner-controller-connection-healing, Phase 2).

  A runner that restarts on the same persistence path re-presents the same
  durable STABLE IDENTITY. A relay is owned by that identity and routed by
  the per-connection session token, so on reconnect each Overwatch is told the
  new session token and re-publishes its own binding (no mint-and-evict, no
  lost relay). This drives a real Rust runner through the proven
  stop → restart pattern (matrix 18.7) with a fixed `FLEZHA_RUNNER_ID_PATH`,
  and asserts the relay follows the runner onto its new session.
  """
  # async: false: Mimic global mode; Mimic stubs; a shared on-disk root.
  use FlezhaTest.Case, fixtures: [:tmp_dir], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  @moduletag :capture_log

  setup :set_mimic_global_until_exit

  import ExUnit.CaptureLog

  alias Flezha.AuthZ.Replica
  alias Flezha.AuthZ.RunnerProof
  alias Flezha.Observability.RingBufferSink
  alias Flezha.Observability.TestSinks
  alias Flezha.Identificator
  alias Flezha.Relay.Builder
  alias Flezha.Relay.Overwatch
  alias Flezha.Runner.DropMarks
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Service.Fleet
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.State.TestHelpers
  alias Flezha.Transport.Server
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.ControlFrameTestHelper
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias FlezhaTest.Helper.MessageTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "reconnect-binding-test"
  @factory "do_nothing"
  @stale_runner "runner_from_a_previous_boot"
  @drop_context %{source: :cli}

  setup %{tmp: tmp} do
    on_exit(fn -> FleetManager.stop_namespace(@namespace) end)

    {:ok, id_path: Path.join(tmp, "flezha-stable-id")}
  end

  test "a relay survives a runner restart via the stable-id binding swap", %{
    id_path: id_path
  } do
    env = [{"FLEZHA_RUNNER_ID_PATH", id_path}]

    # First boot: the SDK derives + persists a stable id; the relay binds to
    # this connection's session token (session1).
    session1 = NodeTestHelper.start_runner(@namespace, env: env)
    assert session1 in FleetManager.active_runners(@namespace)

    relay = %Flezha.Relay.Builder{id: "reconnect_binding_op", factory_id: @factory}
    {:ok, _} = RelayManager.start_relay(@namespace, relay)
    assert bound_to?(session1, relay.id)

    # Restart on the SAME persistence path → the fresh process re-presents the
    # SAME stable id. The controller mints session2 and swaps the relay's
    # binding onto it rather than minting-and-evicting.
    NodeTestHelper.stop_runner(@namespace, session1)

    session2 = NodeTestHelper.start_runner(@namespace, env: env)
    assert session2 != session1, "restart must mint a fresh session token"
    assert session2 in FleetManager.active_runners(@namespace)

    # The relay now belongs to the new session, and the old session is gone.
    assert NodeTestHelper.wait_until(fn -> bound_to?(session2, relay.id) end, 1_000, 25),
           "relay must follow the runner onto its new session token"

    # The Overwatch writes the tag itself, after it re-binds, so a tag naming the
    # new session is an Overwatch naming it too.
    {:ok, overwatch} = RelayManager.get_relay_pid(@namespace, relay.id)
    assert :sys.get_state(overwatch).runner_id == session2

    refute session1 in FleetManager.active_runners(@namespace)
  end

  # fails when: a warm-restarted relay binds to a live runner but its durable
  # row keeps naming the previous boot's runner and the live runner's
  # StreamChannels never learn its pid — every output frame is dropped while the
  # relay still lists as placed and healthy, and no reconciler sees it.
  test "a relay binding away from its recorded runner republishes the binding" do
    session = NodeTestHelper.start_runner(@namespace)
    relay = %Builder{id: "rebind_repair_op", factory_id: @factory}

    # The durable row a degraded recovery replay leaves behind: cspec args and
    # tag both name a runner whose boot is gone.
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)

    HubRegistryTestHelper.inject_relay(@namespace, relay.id, [{node(), self()}], %{
      id: relay.id,
      start: {Overwatch, :start_link, [{@namespace, @stale_runner, relay}]}
    })

    :ok = HubManager.relay_set_runner(@namespace, relay.id, @stale_runner)
    refute bound_to?(session, relay.id), "setup must start from the stale tag"

    # `dist_sup` starts persisted children from exactly this cspec on a warm
    # restart, with no `alter_child_data` in between.
    # Linked, so it is torn down with the test process.
    {:ok, overwatch} = Overwatch.start_link({@namespace, @stale_runner, relay})

    assert NodeTestHelper.wait_until(fn -> bound_to?(session, relay.id) end, 10_000, 25),
           "the registry tag still names the dead runner, so nothing can reconcile the binding"

    assert NodeTestHelper.wait_until(
             fn -> mapped?(session, relay.id, overwatch) end,
             10_000,
             25
           ),
           "the live runner's StreamChannels never learned the relay pid, so every output frame hits the `[] -> :ok` drop"
  end

  # fails when: the rebind publishes the runner `await_runner` handed back rather
  # than the one `complete_init` re-resolved and actually started the relay
  # on, so the durable row and the StreamChannel route both name a runner the
  # relay does not run on — the same silent output drop, one layer down.
  test "the rebind publishes the runner the relay actually started on" do
    session = NodeTestHelper.start_runner(@namespace)
    relay = %Builder{id: "retry_rebind_op", factory_id: @factory}

    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)

    HubRegistryTestHelper.inject_relay(@namespace, relay.id, [{node(), self()}], %{
      id: relay.id,
      start: {Overwatch, :start_link, [{@namespace, @stale_runner, relay}]}
    })

    :ok = HubManager.relay_set_runner(@namespace, relay.id, @stale_runner)

    # The awaited runner goes away before the SDK start, so `complete_init`'s own
    # re-resolve moves the relay onto `session` — the runner it ends up on.
    stub(RelayManager, :await_runner, fn _, _, _, _, _ -> {:ok, @stale_runner} end)
    stub(RelayManager, :assign_runner, fn _, _, _ -> session end)

    {:ok, overwatch} = Overwatch.start_link({@namespace, @stale_runner, relay})
    assert :sys.get_state(overwatch).runner_id == session

    assert NodeTestHelper.wait_until(fn -> bound_to?(session, relay.id) end, 10_000, 25),
           "the tag names the awaited runner, not the one the relay started on"

    assert NodeTestHelper.wait_until(
             fn -> mapped?(session, relay.id, overwatch) end,
             10_000,
             25
           ),
           "the route was seeded into the awaited runner's channels, not the live one's"
  end

  describe "the handshake is recorded from the runner's answers" do
    setup do
      namespace = "handshake-#{System.unique_integer([:positive])}"
      on_exit(fn -> FleetManager.stop_namespace(namespace) end)
      %{ns: namespace}
    end

    test "a refused socket connect ends the session instead of admitting it", %{ns: ns} do
      {sdk, session} = stand_in_runner(ns, &refuse_the_connect/1)

      assert_receive {:flz_closed, _, _},
                     5_000,
                     "the refused session was left standing: " <>
                       inspect(FleetManager.active_runners(ns))

      refute session in FleetManager.active_runners(ns)
      refute tree_running?(session)
      Flezha.Transport.Stream.close(sdk)
    end

    test "an accepted socket connect completes the handshake", %{ns: ns} do
      identity = "stand-in-#{System.unique_integer([:positive])}"
      {sdk, session} = stand_in_runner(ns, &accept/1, %{"stable_id" => identity})

      assert NodeTestHelper.wait_until(
               fn -> session in FleetManager.active_runners(ns) end,
               5_000
             )

      # The session's channels read its identity as they start, and it is
      # forgotten with the session.
      assert Marks.identity_of(session) == identity

      Flezha.Transport.Stream.close(sdk)
      assert NodeTestHelper.wait_until(fn -> Marks.identity_of(session) == session end, 5_000)
    end

    test "a handshake with no answer by its bound ends the session", %{ns: ns} do
      {_sdk, session} = stand_in_runner(ns, fn _ -> :silent end)

      # The bound's own timer, delivered now rather than waited for.
      send(FleetManager, {:handshake_timeout, session})

      assert_receive {:flz_closed, _, _}, 5_000
      refute session in FleetManager.active_runners(ns)
    end

    test "a request sent during the handshake is judged once the session is admitted",
         %{ns: ns} do
      {sdk, {:ok, session}} = connect(ns, %{})
      answer_next_frame(sdk, &accept/1)
      connect_socket = next_frame()

      request =
        Flezha.SDK.Codec.encode_request(
          "admin-during-handshake",
          Flezha.SDK.Codec.admin_request_command("Relay.list_relays", "[]")
        )

      :ok = Flezha.Transport.Stream.send_frame(sdk, "control_command", request, %{})

      assert NodeTestHelper.wait_until(fn -> waiting_on?(session) end, 5_000),
             "the request was judged before the session was admitted"

      refute_received {:flz_data, _, "control_command", _, _}

      answer_frame(sdk, connect_socket, &accept/1)

      assert {:admin_response, %{error: nil}} =
               ControlFrameTestHelper.await_answering_handshake(sdk, "admin-during-handshake")

      assert session in FleetManager.active_runners(ns)
      Flezha.Transport.Stream.close(sdk)
    end

    test "no node-wide call waits on a runner that has not answered", %{ns: ns} do
      {_sdk, _session} = stand_in_runner(ns, fn _ -> :silent end)

      assert FleetManager.active_runners("another-namespace", 1_000) == []
    end

    test "a disconnect tears the tree down though the roster no longer keys the namespace",
         %{ns: ns} do
      {sdk, session} = stand_in_runner(ns, &accept/1)

      assert NodeTestHelper.wait_until(
               fn -> session in FleetManager.active_runners(ns) end,
               5_000
             )

      :sys.replace_state(FleetManager, fn state ->
        %{state | runner_namespaces: Map.delete(state.runner_namespaces, ns)}
      end)

      Flezha.Transport.Stream.close(sdk)

      assert NodeTestHelper.wait_until(fn -> not tree_running?(session) end, 5_000),
             "the runner's tree outlived its session because the roster had no key for #{ns}"
    end

    test "a repeated disconnect changes nothing", %{ns: ns} do
      {sdk, session} = stand_in_runner(ns, &accept/1)

      assert NodeTestHelper.wait_until(
               fn -> session in FleetManager.active_runners(ns) end,
               5_000
             )

      Flezha.Transport.Stream.close(sdk)
      assert NodeTestHelper.wait_until(fn -> not tree_running?(session) end, 5_000)

      before = :sys.get_state(FleetManager)
      assert :ok = GenServer.call(FleetManager, {:runner_disconnected, ns, session})
      assert :sys.get_state(FleetManager) == before
    end
  end

  describe "a runner whose supervision tree cannot start" do
    setup do
      namespace = "tree-#{System.unique_integer([:positive])}"
      on_exit(fn -> FleetManager.stop_namespace(namespace) end)
      %{ns: namespace}
    end

    test "is refused naming the start's own error, not a missing process", %{ns: ns} do
      # No data-socket name fits under this directory, so the runner's acceptor
      # refuses to start and takes the tree with it.
      too_long = Path.join(System.tmp_dir!(), String.duplicate("d", 70))
      on_exit(TestHelpers.override_env(:socket_files_path, too_long))
      MessageTestHelper.forward_telemetry([:flezha, :transport, :connect_rejected], :rejected)

      log = socket_connect_log(ns)

      assert_received {:rejected, %{count: 1}, %{reason: {:runner_supervisor_down, start_error}}}
      refute match?({:noproc, _}, start_error)
      assert inspect(start_error) =~ "Set FLEZHA_SOCKET_FILES_PATH to a shorter directory"

      # The crash report names the path whatever the refusal says, so the
      # refusal's own line is what is read.
      refusal = log |> String.split("\n") |> Enum.find("", &(&1 =~ "on #{ns} has no control"))
      assert refusal =~ "#{too_long}/primary_stream_"
    end
  end

  describe "a runner whose namespace hub cannot start" do
    setup do
      namespace = "hub-start-#{System.unique_integer([:positive])}"
      on_exit(fn -> FleetManager.stop_namespace(namespace) end)
      %{ns: namespace}
    end

    test "is refused naming the hub start's own error", %{ns: ns} do
      start_error = {:shutdown, {:failed_to_start_child, :hub_under_test, :enospc}}
      stub_hub_start(ns, fn -> {:error, start_error} end)
      MessageTestHelper.forward_telemetry([:flezha, :transport, :connect_rejected], :rejected)

      log = socket_connect_log(ns)

      assert_received {:rejected, %{count: 1}, %{reason: reason}}
      assert inspect(reason) =~ inspect(start_error)

      refusal =
        log
        |> String.split("\n")
        |> Enum.find("", &(&1 =~ "on #{ns} " and &1 =~ "refusing the connect"))

      assert refusal =~ inspect(start_error)
      assert log =~ "Failed to connect runner"
      assert log =~ "Rejected runner connect"
    end

    test "still connects when a concurrent start answers already started", %{ns: ns} do
      stub_hub_start(ns, fn ->
        {:ok, pid} = call_original(HubManager, :start_hub, [ns])
        {:error, {:already_started, pid}}
      end)

      {_sdk, session} = stand_in_runner(ns, &accept/1)

      assert NodeTestHelper.wait_until(
               fn -> session in FleetManager.active_runners(ns) end,
               5_000
             )
    end
  end

  describe "a stable identity presented under another credential" do
    setup do
      namespace = "pin-#{System.unique_integer([:positive])}"
      on_exit(fn -> FleetManager.stop_namespace(namespace) end)

      # The proof answers the credential the payload names, as a credentialed
      # posture does.
      stub(RunnerProof, :prove, fn _stream, claimed, payload, _transport ->
        {:ok, %{namespace: claimed, credential: payload["credential"]}}
      end)

      MessageTestHelper.forward_telemetry([:flezha, :transport, :connect_rejected], :rejected)

      %{ns: namespace, identity: "pinned-#{System.unique_integer([:positive])}"}
    end

    test "is taken over when the credential is the same", %{ns: ns, identity: id} do
      first = active_stand_in(ns, %{"stable_id" => id, "credential" => "c1"})
      second = active_stand_in(ns, %{"stable_id" => id, "credential" => "c1"})

      assert NodeTestHelper.wait_until(
               fn -> first not in FleetManager.active_runners(ns) end,
               5_000
             )

      assert second in FleetManager.active_runners(ns)
    end

    test "is refused while the identity's session is held", %{ns: ns, identity: id} do
      first = active_stand_in(ns, %{"stable_id" => id, "credential" => "c1"})

      assert {_sdk, {:error, {:identity_taken, ^id}}} =
               connect(ns, %{"stable_id" => id, "credential" => "c2"})

      assert_received {:rejected, %{count: 1}, %{reason: {:identity_taken, ^id}}}
      assert FleetManager.active_runners(ns) == [first]
    end

    test "is admitted and pinned anew once the identity's session has left the roster",
         %{ns: ns, identity: id} do
      first = active_stand_in(ns, %{"stable_id" => id, "credential" => "c1"})
      :ok = FleetManager.evict_runner(first, proved_wedged: true)

      second = active_stand_in(ns, %{"stable_id" => id, "credential" => "c2"})

      assert FleetManager.active_runners(ns) == [second]
      assert :sys.get_state(FleetManager).stable_sessions[id] == {second, "c2", nil}
    end

    test "is neither pinned nor refused when the connect carries no credential",
         %{ns: ns, identity: id} do
      first = active_stand_in(ns, %{"stable_id" => id, "credential" => "c1"})
      second = active_stand_in(ns, %{"stable_id" => id})

      assert NodeTestHelper.wait_until(
               fn -> first not in FleetManager.active_runners(ns) end,
               5_000
             )

      assert second in FleetManager.active_runners(ns)
      assert :sys.get_state(FleetManager).stable_sessions[id] == {second, nil, nil}
    end
  end

  describe "a stable identity presented again" do
    setup do
      namespace = "incarnation-#{System.unique_integer([:positive])}"
      on_exit(fn -> FleetManager.stop_namespace(namespace) end)

      # This test process stands in for the one Overwatch the old session runs.
      test_pid = self()
      stub(HubManager, :runner_relays, fn _ns, _session -> [{"op", [test_pid]}] end)
      stub(HubManager, :live_pid, fn _node_pids -> test_pid end)

      %{ns: namespace, identity: "incarnated-#{System.unique_integer([:positive])}"}
    end

    # fails when: the same process reconnecting is bound afresh, re-running
    # what its runner still holds.
    test "with the same incarnation re-points the binding", %{ns: ns, identity: id} do
      first = active_stand_in(ns, %{"stable_id" => id, "incarnation" => "i1"})
      second = active_stand_in(ns, %{"stable_id" => id, "incarnation" => "i1"})

      assert_receive {:"$gen_cast", {:session_replaced, ^first, ^second, :reconnected}}, 5_000
    end

    # fails when: a restarted process only has the binding re-pointed at it,
    # so it never runs its relays again.
    test "with a new incarnation binds afresh", %{ns: ns, identity: id} do
      first = active_stand_in(ns, %{"stable_id" => id, "incarnation" => "i1"})
      second = active_stand_in(ns, %{"stable_id" => id, "incarnation" => "i2"})

      assert_receive {:"$gen_cast", {:session_replaced, ^first, ^second, :restarted}}, 5_000
    end

    # fails when: two connects that carry no incarnation read as the same
    # process, so a restarted runner that cannot prove otherwise is never bound.
    test "with no incarnation binds afresh", %{ns: ns, identity: id} do
      first = active_stand_in(ns, %{"stable_id" => id})
      second = active_stand_in(ns, %{"stable_id" => id})

      assert_receive {:"$gen_cast", {:session_replaced, ^first, ^second, :restarted}}, 5_000
    end
  end

  describe "a dropped runner process" do
    setup do
      AuthTestHelper.setup_auth_stores(
        prefix: "flz_runner_drop",
        stores: [Replica, DropMarks],
        enabled: false
      )

      namespace = "drop-#{System.unique_integer([:positive])}"
      on_exit(fn -> FleetManager.stop_namespace(namespace) end)

      MessageTestHelper.forward_telemetry([:flezha, :transport, :connect_rejected], :rejected)

      for event <- [:dropped, :readmitted, :drop_refused],
          do: MessageTestHelper.forward_telemetry([:flezha, :runner, event], event)

      %{ns: namespace, identity: "dropped-#{System.unique_integer([:positive])}"}
    end

    test "is closed as a retiring runner, answered as a mark, and refused when it retries",
         %{ns: ns, identity: id} do
      process = %{"stable_id" => id, "incarnation" => "i1"}
      {sdk, session} = stand_in_runner(ns, &accept/1, process)

      assert NodeTestHelper.wait_until(
               fn -> session in FleetManager.active_runners(ns) end,
               5_000
             )

      restore = NodeTestHelper.override_log_level(:info)

      log =
        capture_log(fn ->
          assert {:ok, mark} = Fleet.drop_runner(ns, session, @drop_context)

          assert %{namespace: ^ns, stable_id: ^id, incarnation: "i1", pid: nil, dropped_by: nil} =
                   mark

          assert mark.node == node()
          assert is_integer(mark.dropped_at)

          sdk_stream = Flezha.Transport.Stream.stream_id(sdk)
          assert_receive {:flz_closed, ^sdk_stream, _reason}, 5_000

          assert NodeTestHelper.wait_until(
                   fn -> session not in FleetManager.active_runners(ns) end,
                   5_000
                 )
        end)

      restore.()
      assert log =~ "Control socket closed for retiring runner #{session}"
      assert_received {:dropped, %{count: 1}, %{namespace: ^ns, stable_id: ^id}}

      assert {_sdk, {:error, {:runner_dropped, ^ns, ^id}}} = connect(ns, process)

      assert_received {:drop_refused, %{count: 1},
                       %{namespace: ^ns, stable_id: ^id, pid: nil, node: refused_on}}

      assert refused_on == node()
      assert_received {:rejected, %{count: 1}, %{reason: {:runner_dropped, ^ns, ^id}}}

      assert {:ok, [%{stable_id: ^id, incarnation: "i1"}]} =
               Fleet.dropped_runners(ns, @drop_context)
    end

    # fails when: the drop tears the session down but leaves the runner's socket
    # open, so the process never retries and is never refused.
    test "over a real runner's socket, is refused on its retry and admitted once readmitted",
         %{ns: ns, tmp: tmp} do
      env = [{"FLEZHA_RUNNER_ID_PATH", Path.join(tmp, "dropped-runner-id")}]
      session = NodeTestHelper.start_runner(ns, env: env)

      assert {:ok, %{pid: pid, stable_id: id} = mark} =
               Fleet.drop_runner(ns, session, @drop_context)

      assert is_integer(pid)
      assert_receive {:drop_refused, %{count: 1}, %{stable_id: ^id, pid: ^pid}}, 10_000
      refute session in FleetManager.active_runners(ns)

      {:ok, ^mark} = Fleet.readmit_runner(ns, id, @drop_context)

      assert NodeTestHelper.wait_until(fn -> FleetManager.active_runners(ns) != [] end, 10_000),
             "the readmitted process never reconnected"
    end

    test "still in its handshake, is ended by a drop placed on another controller",
         %{ns: ns, identity: id} do
      {sdk, session} =
        stand_in_runner(ns, fn _ -> :silent end, %{"stable_id" => id, "incarnation" => "i1"})

      assert Map.has_key?(:sys.get_state(FleetManager).pending_handshakes, session)

      assert FleetManager.drop_matching(id, "i2") == 0
      assert FleetManager.drop_matching(id, "i1") == 1

      sdk_stream = Flezha.Transport.Stream.stream_id(sdk)
      assert_receive {:flz_closed, ^sdk_stream, _reason}, 5_000
      refute Map.has_key?(:sys.get_state(FleetManager).pending_handshakes, session)
    end

    test "is refused with a hint naming the readmit", %{ns: ns, identity: id} do
      error = Flezha.Error.from_reason({:runner_dropped, ns, id}, :"wire.connect")

      assert %Flezha.Error{code: "runner.dropped", class: :denied} = error
      assert error.hint == "flz runner readmit #{id} --namespace #{ns}"
    end

    test "is refused again after the store reloads", %{ns: ns, identity: id} do
      session = active_stand_in(ns, %{"stable_id" => id, "incarnation" => "i1"})
      {:ok, _mark} = Fleet.drop_runner(ns, session, @drop_context)

      AuthTestHelper.stop_if_running([DropMarks, Replica])
      {:ok, _} = Replica.start_link([])
      {:ok, _} = DropMarks.start_link([])

      assert DropMarks.match?(ns, id, "i1")

      assert {_sdk, {:error, {:runner_dropped, ^ns, ^id}}} =
               connect(ns, %{"stable_id" => id, "incarnation" => "i1"})
    end

    test "is replaced by a new process of the same runner, which lifts the mark",
         %{ns: ns, identity: id} do
      first = active_stand_in(ns, %{"stable_id" => id, "incarnation" => "i1"})
      {:ok, _mark} = Fleet.drop_runner(ns, first, @drop_context)

      second = active_stand_in(ns, %{"stable_id" => id, "incarnation" => "i2"})

      assert NodeTestHelper.wait_until(fn -> DropMarks.list(ns) == [] end, 5_000)
      assert_receive {:readmitted, %{count: 1}, %{namespace: ^ns, stable_id: ^id}}, 5_000
      assert FleetManager.active_runners(ns) == [second]
    end

    test "is admitted again once readmitted", %{ns: ns, identity: id} do
      process = %{"stable_id" => id, "incarnation" => "i1"}
      first = active_stand_in(ns, process)
      {:ok, _mark} = Fleet.drop_runner(ns, first, @drop_context)

      assert {:ok, %{stable_id: ^id, incarnation: "i1"}} =
               Fleet.readmit_runner(ns, id, @drop_context)

      assert_received {:readmitted, %{count: 1}, %{namespace: ^ns, stable_id: ^id}}
      assert {:ok, []} = Fleet.dropped_runners(ns, @drop_context)

      again = active_stand_in(ns, process)
      assert FleetManager.active_runners(ns) == [again]

      assert {:error, %Flezha.Error{code: "runner.not_found"}} =
               Fleet.readmit_runner(ns, id, @drop_context)
    end

    test "cannot be dropped when the runner cannot be recognised again", %{ns: ns, identity: id} do
      for presented <- [%{"stable_id" => id}, %{"stable_id" => "", "incarnation" => "i1"}] do
        session = active_stand_in(ns, presented)

        assert {:error, %Flezha.Error{code: "runner.untrackable"}} =
                 Fleet.drop_runner(ns, session, @drop_context)

        assert session in FleetManager.active_runners(ns)
      end

      assert DropMarks.list(ns) == []
    end

    test "cannot be dropped when no node holds the runner", %{ns: ns} do
      assert {:error, %Flezha.Error{code: "runner.not_found", class: :not_found}} =
               Fleet.drop_runner(ns, "no_such_runner", @drop_context)
    end
  end

  describe "dropping a runner under enforced authorization" do
    setup do
      AuthTestHelper.setup_auth_stores(
        prefix: "flz_runner_drop_authz",
        claim: true,
        register_source: true
      )

      %{ns: "drop-authz-#{System.unique_integer([:positive])}"}
    end

    test "is refused to a viewer, and the refusal is audited", %{ns: ns} do
      viewer = %{source: :cli, principal: "viewer-caller", namespace: ns, role: :viewer}
      boundary = System.monotonic_time(:microsecond)

      assert {:error, %Flezha.Error{class: :denied}} = Fleet.drop_runner(ns, "r", viewer)
      assert {:error, %Flezha.Error{class: :denied}} = Fleet.readmit_runner(ns, "s", viewer)
      assert {:ok, []} = Fleet.dropped_runners(ns, viewer)

      TestSinks.settle(RingBufferSink)

      assert [_drop, _readmit] =
               for(
                 %{name: :authorization_denied} = denial <-
                   RingBufferSink.query(:platform_authz, boundary),
                 denial.metadata.action == :"runner.drop",
                 do: denial
               )
    end

    test "is audited with its outcome for an ops caller", %{ns: ns} do
      ops = %{source: :cli, principal: "ops-caller", namespace: ns, role: :ops}
      boundary = System.monotonic_time(:microsecond)

      assert {:error, %Flezha.Error{code: "runner.not_found"}} =
               Fleet.drop_runner(ns, "no_such_runner", ops)

      TestSinks.settle(RingBufferSink)

      assert [record] = RingBufferSink.query(:fleet_service, boundary)
      assert record.name == :drop
      assert record.metadata.action == :"runner.drop"
      assert record.metadata.principal == "ops-caller"
      assert record.metadata.runner == "no_such_runner"
      assert record.metadata.outcome == :error
      assert record.metadata.error_code == "runner.not_found"
    end
  end

  # A stand-in that answers its handshake, once the fleet lists it.
  defp active_stand_in(namespace, extra) do
    {_sdk, session} = stand_in_runner(namespace, &accept/1, extra)

    assert NodeTestHelper.wait_until(
             fn -> session in FleetManager.active_runners(namespace) end,
             5_000
           )

    session
  end

  # `start_hub/1` answers `start` for `namespace` only; every other hub the
  # node starts meanwhile starts for real.
  defp stub_hub_start(namespace, start) do
    stub(HubManager, :start_hub, fn
      ^namespace -> start.()
      other -> call_original(HubManager, :start_hub, [other])
    end)
  end

  # A socket connect for `namespace` driven through the acceptor on a stream
  # that is never opened, answering everything it logged.
  defp socket_connect_log(namespace) do
    state = %{transport: :uds, pending_streams: %{7 => make_ref()}}
    payload = Jason.encode!(%{"namespace" => namespace, "factories" => []})

    capture_log(fn ->
      Server.handle_info({:flz_data, 7, "connect_control_channel", payload, %{}}, state)
    end)
  end

  # A runner played by this process on a real in-node stream: it connects the
  # way the in-node SDK does, with `extra` merged into its handshake payload,
  # then answers each handshake frame as `answer` says. `:silent` answers
  # nothing.
  defp stand_in_runner(namespace, answer, extra \\ %{}) do
    {sdk, {:ok, session}} = connect(namespace, extra)

    for _frame <- 1..2, do: answer_next_frame(sdk, answer)

    {sdk, session}
  end

  defp answer_next_frame(sdk, answer), do: answer_frame(sdk, next_frame(), answer)

  defp next_frame do
    assert_receive {:flz_data, _, "control_command", bin, _}, 5_000
    {:ok, %{reference_id: ref, command: {kind, _}}} = Flezha.SDK.Codec.decode_command(bin)
    {ref, kind}
  end

  defp answer_frame(sdk, {ref, kind}, answer) do
    with {status, errors} <- answer.(kind) do
      response = Flezha.SDK.Codec.encode_response(ref, status, errors)
      Flezha.Transport.Stream.send_frame(sdk, "control_command", response, %{})
    end
  end

  defp waiting_on?(session) do
    match?(
      %{^session => %{waiters: [_ | _]}},
      :sys.get_state(FleetManager).pending_handshakes
    )
  end

  # Open an in-node stream and connect over it, answering the connect's result.
  defp connect(namespace, extra) do
    Flezha.Transport.Server.ensure_started(:native)
    {sdk, controller_side} = Flezha.SDK.LocalStream.open()
    :ok = Flezha.Transport.Stream.controlling_process(sdk, self())
    :ok = Flezha.Transport.Stream.set_active(sdk)

    payload =
      Map.merge(
        %{
          "namespace" => namespace,
          "factory_ids" => [],
          "factories" => [],
          "stable_id" => "stand-in-#{System.unique_integer([:positive])}"
        },
        extra
      )

    {sdk, Flezha.Transport.Server.connect(payload, controller_side)}
  end

  defp accept(_kind), do: {"success", []}

  defp refuse_the_connect(:connect_socket),
    do: {"error", [Flezha.SDK.Error.new("runner.data_streams_failed", reason: "no data socket")]}

  defp refuse_the_connect(_kind), do: {"success", []}

  defp tree_running?(session) do
    Flezha.Runner.FleetSupervisor
    |> Supervisor.which_children()
    |> Enum.any?(fn {id, _, _, _} -> id == session end)
  end

  # True when one of `session`'s primary StreamChannels routes `relay_id` to
  # `pid` — the mapping whose absence makes `stream_channel.ex` drop frames.
  defp mapped?(session, relay_id, pid) do
    Identificator.stream_sup_primary(session)
    |> Supervisor.which_children()
    |> Enum.any?(fn
      {_, channel, _, _} when is_pid(channel) ->
        :sys.get_state(channel)[:relay_mapping][relay_id] == [pid]

      _ ->
        false
    end)
  catch
    :exit, _ -> false
  end

  defp bound_to?(session_token, relay_id) do
    HubManager.runner_relays(@namespace, session_token)
    |> Enum.any?(fn {oid, _node_pids} -> oid == relay_id end)
  end
end
