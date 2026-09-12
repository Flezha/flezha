defmodule FlezhaTest.Runner.FleetManagerSessionTest do
  @moduledoc """
  Reconnect binding continuity (runner-controller-connection-healing, Phase 2).

  A runner that restarts on the same persistence path re-presents the same
  durable STABLE IDENTITY. The controller keys operatives on the per-connection
  session token, so on reconnect it SWAPS the prior session's bindings onto the
  new session token (no mint-and-evict, no lost operative) — exactly the
  recurring-outage fix. This drives a real Rust runner through the proven
  stop → restart pattern (matrix 18.7) with a fixed `FLEZHA_RUNNER_ID_PATH`,
  and asserts the operative follows the runner onto its new session.
  """
  # async: false: Mimic global mode; Mimic stubs; a shared on-disk root.
  use FlezhaTest.Case, fixtures: [:tmp_dir], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  @moduletag :capture_log

  setup :set_mimic_global_until_exit

  import ExUnit.CaptureLog

  alias Flezha.Identificator
  alias Flezha.Operative.Builder
  alias Flezha.Operative.Overwatch
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.TestHelpers
  alias Flezha.Transport.Server
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias FlezhaTest.Helper.MessageTestHelper
  alias FlezhaTest.Helper.NodeTestHelper
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager

  @namespace "reconnect-binding-test"
  @factory "do_nothing"
  @stale_runner "runner_from_a_previous_boot"

  setup %{tmp: tmp} do
    on_exit(fn -> FleetManager.stop_namespace(@namespace) end)

    {:ok, id_path: Path.join(tmp, "flezha-stable-id")}
  end

  test "an operative survives a runner restart via the stable-id binding swap", %{
    id_path: id_path
  } do
    env = [{"FLEZHA_RUNNER_ID_PATH", id_path}]

    # First boot: the SDK derives + persists a stable id; the operative binds to
    # this connection's session token (session1).
    session1 = NodeTestHelper.start_runner(@namespace, env: env)
    assert session1 in FleetManager.active_runners(@namespace)

    operative = %Flezha.Operative.Builder{id: "reconnect_binding_op", factory_id: @factory}
    {:ok, _} = OperativeManager.start_operative(@namespace, operative)
    assert bound_to?(session1, operative.id)

    # Restart on the SAME persistence path → the fresh process re-presents the
    # SAME stable id. The controller mints session2 and swaps the operative's
    # binding onto it rather than minting-and-evicting.
    NodeTestHelper.stop_runner(@namespace, session1)

    session2 = NodeTestHelper.start_runner(@namespace, env: env)
    assert session2 != session1, "restart must mint a fresh session token"
    assert session2 in FleetManager.active_runners(@namespace)

    # The operative now belongs to the new session, and the old session is gone.
    assert NodeTestHelper.wait_until(fn -> bound_to?(session2, operative.id) end, 1_000, 25),
           "operative must follow the runner onto its new session token"

    refute session1 in FleetManager.active_runners(@namespace)
  end

  # fails when: a warm-restarted operative binds to a live runner but its durable
  # row keeps naming the previous boot's runner and the live runner's
  # StreamChannels never learn its pid — every output frame is dropped while the
  # operative still lists as placed and healthy, and no reconciler sees it.
  test "an operative binding away from its recorded runner republishes the binding" do
    session = NodeTestHelper.start_runner(@namespace)
    operative = %Builder{id: "rebind_repair_op", factory_id: @factory}

    # The durable row a degraded recovery replay leaves behind: cspec args and
    # tag both name a runner whose boot is gone.
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)

    HubRegistryTestHelper.inject_operative(@namespace, operative.id, [{node(), self()}], %{
      id: operative.id,
      start: {Overwatch, :start_link, [{@namespace, @stale_runner, operative}]}
    })

    :ok = HubManager.operative_set_runner(@namespace, operative.id, @stale_runner)
    refute bound_to?(session, operative.id), "setup must start from the stale tag"

    # `dist_sup` starts persisted children from exactly this cspec on a warm
    # restart, with no `alter_child_data` in between.
    # Linked, so it is torn down with the test process.
    {:ok, overwatch} = Overwatch.start_link({@namespace, @stale_runner, operative})

    assert NodeTestHelper.wait_until(fn -> bound_to?(session, operative.id) end, 10_000, 25),
           "the registry tag still names the dead runner, so nothing can reconcile the binding"

    assert NodeTestHelper.wait_until(
             fn -> mapped?(session, operative.id, overwatch) end,
             10_000,
             25
           ),
           "the live runner's StreamChannels never learned the operative pid, so every output frame hits the `[] -> :ok` drop"
  end

  # fails when: the rebind publishes the runner `await_runner` handed back rather
  # than the one `complete_init` re-resolved and actually started the operative
  # on, so the durable row and the StreamChannel route both name a runner the
  # operative does not run on — the same silent output drop, one layer down.
  test "the rebind publishes the runner the operative actually started on" do
    session = NodeTestHelper.start_runner(@namespace)
    operative = %Builder{id: "retry_rebind_op", factory_id: @factory}

    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)

    HubRegistryTestHelper.inject_operative(@namespace, operative.id, [{node(), self()}], %{
      id: operative.id,
      start: {Overwatch, :start_link, [{@namespace, @stale_runner, operative}]}
    })

    :ok = HubManager.operative_set_runner(@namespace, operative.id, @stale_runner)

    # The awaited runner goes away before the SDK start, so `complete_init`'s own
    # re-resolve moves the operative onto `session` — the runner it ends up on.
    stub(OperativeManager, :await_runner, fn _, _, _, _, _ -> {:ok, @stale_runner} end)
    stub(OperativeManager, :assign_runner, fn _, _, _ -> session end)

    {:ok, overwatch} = Overwatch.start_link({@namespace, @stale_runner, operative})
    assert :sys.get_state(overwatch).runner_id == session

    assert NodeTestHelper.wait_until(fn -> bound_to?(session, operative.id) end, 10_000, 25),
           "the tag names the awaited runner, not the one the operative started on"

    assert NodeTestHelper.wait_until(
             fn -> mapped?(session, operative.id, overwatch) end,
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
      {_sdk, session} = stand_in_runner(ns, &accept/1)

      assert NodeTestHelper.wait_until(
               fn -> session in FleetManager.active_runners(ns) end,
               5_000
             )
    end

    test "a handshake with no answer by its bound ends the session", %{ns: ns} do
      {_sdk, session} = stand_in_runner(ns, fn _ -> :silent end)

      # The bound's own timer, delivered now rather than waited for.
      send(FleetManager, {:handshake_timeout, session})

      assert_receive {:flz_closed, _, _}, 5_000
      refute session in FleetManager.active_runners(ns)
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

      state = %{transport: :uds, pending_streams: %{7 => make_ref()}}
      payload = Jason.encode!(%{"namespace" => ns, "factories" => []})

      log =
        capture_log(fn ->
          Server.handle_info({:flz_data, 7, "connect_control_channel", payload, %{}}, state)
        end)

      assert_received {:rejected, %{count: 1}, %{reason: {:runner_supervisor_down, start_error}}}
      refute match?({:noproc, _}, start_error)
      assert inspect(start_error) =~ "Set FLEZHA_SOCKET_FILES_PATH to a shorter directory"

      # The crash report names the path whatever the refusal says, so the
      # refusal's own line is what is read.
      refusal = log |> String.split("\n") |> Enum.find("", &(&1 =~ "on #{ns} has no control"))
      assert refusal =~ "#{too_long}/primary_stream_"

      # The hook is taken before the tree, and a refused connect leaves it on
      # the hub for its lifetime otherwise: a runner retrying a refusal adds
      # one handler per attempt, none of which any runner will ever answer.
      handlers = child_registered_handlers(ns)

      assert :flezha_registry_written in handlers,
             "read no handler table for #{ns}'s hub, so an empty insertion list proves nothing"

      assert Enum.filter(handlers, &insertion_hook?/1) == []
    end
  end

  # Every `child_registered` handler this namespace's hub holds, by id.
  defp child_registered_handlers(namespace) do
    hub = ProcessHub.Coordinator.get_hub(Identificator.hub(namespace))

    hub.storage.hook
    |> HookManager.registered_handlers(Hook.child_registered())
    |> Enum.map(& &1.id)
  end

  defp insertion_hook?(id), do: is_binary(id) and String.ends_with?(id, "_hub_pid_insertion")

  # A runner played by this process on a real in-node stream: it connects the
  # way the in-node SDK does, then answers each handshake frame as `answer`
  # says. `:silent` answers nothing.
  defp stand_in_runner(namespace, answer) do
    Flezha.Transport.Server.ensure_started(:native)
    {sdk, controller_side} = Flezha.Transport.Local.Stream.open()
    :ok = Flezha.Transport.Stream.controlling_process(sdk, self())
    :ok = Flezha.Transport.Stream.set_active(sdk)

    payload = %{
      "namespace" => namespace,
      "factory_ids" => [],
      "factories" => [],
      "stable_id" => "stand-in-#{System.unique_integer([:positive])}"
    }

    {:ok, session} = Flezha.Transport.Server.connect(payload, controller_side)

    for _frame <- 1..2 do
      assert_receive {:flz_data, _, "control_command", bin, _}, 5_000
      {:ok, %{reference_id: ref, command: {kind, _}}} = Flezha.SDK.Codec.decode_command(bin)

      with {status, errors} <- answer.(kind) do
        response = Flezha.SDK.Codec.encode_response(ref, status, errors)
        Flezha.Transport.Stream.send_frame(sdk, "control_command", response, %{})
      end
    end

    {sdk, session}
  end

  defp accept(_kind), do: {"success", []}

  defp refuse_the_connect(:connect_socket), do: {"error", ["no data socket"]}
  defp refuse_the_connect(_kind), do: {"success", []}

  defp tree_running?(session) do
    Flezha.Runner.FleetSupervisor
    |> Supervisor.which_children()
    |> Enum.any?(fn {id, _, _, _} -> id == session end)
  end

  # True when one of `session`'s primary StreamChannels routes `operative_id` to
  # `pid` — the mapping whose absence makes `stream_channel.ex` drop frames.
  defp mapped?(session, operative_id, pid) do
    Identificator.stream_sup_primary(session)
    |> Supervisor.which_children()
    |> Enum.any?(fn
      {_, channel, _, _} when is_pid(channel) ->
        :sys.get_state(channel)[:operative_mapping][operative_id] == [pid]

      _ ->
        false
    end)
  catch
    :exit, _ -> false
  end

  defp bound_to?(session_token, operative_id) do
    HubManager.runner_operatives(@namespace, session_token)
    |> Enum.any?(fn {oid, _node_pids} -> oid == operative_id end)
  end
end
