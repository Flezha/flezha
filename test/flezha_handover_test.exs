defmodule FlezhaTest.Handover do
  alias Flezha.Convergence.Loop
  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Identificator
  alias Flezha.Relay.Replay
  alias Flezha.Payloader
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.StepsResumeHarness
  alias FlezhaTest.Helper.MessageTestHelper
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.RelayTestHelper
  alias ProcessHub.Constant.Hook

  import FlezhaTest.Helper.MessageTestHelper, only: [collect: 1]

  use ExUnit.Case
  use Flezha.Constant

  @default_namespace "default"
  @temp_namespace "test_temp_namespace"

  @test_store_input_factory "store_input"

  setup_all do
    %{}
  end

  setup _context do
    # Restore now rehydrates a relay from its WAL on (re)bind, so leftover
    # per-Cell state from an earlier test that reused the same relay_id
    # would leak in. Start each test from empty cells.
    TestHelpers.reset_all_cells()

    on_exit(fn ->
      RelayManager.stop_all_relays(@default_namespace)
      FleetManager.stop_namespace(@temp_namespace)
    end)

    :ok
  end

  @tag :multi_node
  @tag :drain_migration
  test "graceful shutdown drains a relay to a peer through live handover" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_node1)

    relay_id = start_relay_on_peer(peer_node, "drain_mig")
    RelayTestHelper.send_sync_io(relay_id, @default_namespace, "before_drain", %{})

    assert :ok = shutdown_peer(peer_node, 30_000)

    # One :drain_progress per hub — match the namespace the relay is in, or
    # a peer namespace with nothing to move answers for it with migrated: 0.
    assert_receive {[:flezha, :shutdown, :drain_progress], _ref, m,
                    %{namespace: @default_namespace}},
                   10_000

    assert m.migrated >= 1
    assert m.forced == 0

    # The drain reports when the local children are gone, which is the last
    # step the departing node controls; the registry row naming their new home
    # converges just after, so poll for it.
    assert NodeTestHelper.wait_until(
             fn ->
               match?(
                 {:ok, %{arrived_via_hotswap?: true}},
                 RelayManager.get_overwatch_state(@default_namespace, relay_id)
               )
             end,
             5_000,
             25
           ),
           "expected live HotSwap handover, not a WAL cold restore"

    {:ok, json_state} = RelayManager.get_cell_state(@default_namespace, relay_id)
    state = Payloader.json_to_map(json_state)
    assert "before_drain" in get_in(state, ["store_input_tasklet", "text_inputs"])

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # fails when: the waiting callers stay behind with the departing Overwatch,
  # which then answers them `call.callee_ended` as it stops, or the arriving one
  # does not re-arm their deadlines.
  @tag :multi_node
  @tag :drain_migration_calls
  test "a caller waiting on a relay that drains to a peer is answered from there" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_calls_node)
    relay_id = start_relay_on_peer(peer_node, "drain_call")
    {:ok, source} = RelayManager.get_relay_pid(@default_namespace, relay_id)

    # Both calls name an answer channel `store_input` never emits on, so its
    # own output answers neither before the move; the answer is handed to the
    # arriving Overwatch below.
    # The drain takes seconds, so both deadlines outlast it.
    started = System.monotonic_time(:millisecond)
    waits = %{"flz.call_answer_channel" => "moved"}

    answered =
      Task.async(fn -> Flezha.Relay.Calls.call(source, "x", waits, 40_000, relay_id, "cli") end)

    timed =
      Task.async(fn -> Flezha.Relay.Calls.call(source, "y", waits, 15_000, relay_id, "cli") end)

    assert NodeTestHelper.wait_until(
             fn -> map_size(:sys.get_state(source).calls.rows) == 2 end,
             5_000
           )

    [token] =
      for {token, %{deadline_ms: 40_000}} <- :sys.get_state(source).calls.rows, do: token

    assert :ok = shutdown_peer(peer_node, 30_000)

    assert NodeTestHelper.wait_until(
             fn ->
               match?(
                 {:ok, %{arrived_via_hotswap?: true}},
                 RelayManager.get_overwatch_state(@default_namespace, relay_id)
               )
             end,
             5_000,
             25
           )

    {:ok, target} = RelayManager.get_relay_pid(@default_namespace, relay_id)
    assert node(target) == node()
    refute Task.yield(answered, 0), "the moved caller was answered by the departing node"
    GenServer.cast(target, {:io_out, "moved", %{"flz.reply_to" => token, "channel" => "moved"}})
    assert {:ok, "moved", _} = Task.await(answered, 5_000)
    assert {:error, %Flezha.Error{code: "call.timeout"}} = Task.await(timed, 20_000)

    assert System.monotonic_time(:millisecond) - started >= 15_000,
           "the moved deadline came early"

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  @tag :multi_node
  @tag :drain_migration_forced
  test "a deferring relay is force-migrated at the drain deadline without wedging" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_node2)

    relay_id = start_relay_on_peer(peer_node, "drain_forced")
    RelayTestHelper.send_sync_io(relay_id, @default_namespace, "seed", %{})

    # Hold the busy signal: a correction that never leaves its debounce
    # window, so `migration_consent/1` answers :defer for the whole drain.
    :ok =
      :erpc.call(peer_node, Application, :put_env, [:flezha, :correction_debounce_ms, 600_000])

    hub_id = Identificator.hub(@default_namespace)

    {^relay_id, ow_pid} =
      :erpc.call(peer_node, ProcessHub, :process_list, [hub_id, :local])
      |> Enum.find(fn {id, _pid} -> id == relay_id end)

    GenServer.cast(
      ow_pid,
      {:correction_request, {relay_id, "store_input_tasklet", "poke"}, 1}
    )

    # Sync barrier: the cast is folded into state before the drain begins.
    _ = :sys.get_state(ow_pid)

    test_pid = self()

    :erpc.call(peer_node, ProcessHub, :register_hook_handlers, [
      hub_id,
      Hook.migration_deferred(),
      [
        %ProcessHub.Service.HookManager{
          id: "deferred_wait",
          m: Process,
          f: :send,
          a: [test_pid, :deferred, []]
        }
      ]
    ])

    assert :ok = shutdown_peer(peer_node, 6_000)

    assert_receive :deferred, 1_000

    assert_receive {[:flezha, :shutdown, :drain_progress], _ref, m,
                    %{namespace: @default_namespace}},
                   5_000

    assert m.forced >= 1

    # Forced onto this node through the best-effort handover: state carried,
    # protocol still live. The drain reports when the local children are gone,
    # which is the last step the departing node controls — the registry row
    # naming their new home converges just after, so poll for it.
    assert NodeTestHelper.wait_until(
             fn ->
               match?(
                 {:ok, %{arrived_via_hotswap?: true}},
                 RelayManager.get_overwatch_state(@default_namespace, relay_id)
               )
             end,
             5_000,
             25
           ),
           "the relay must become addressable at its new home after the forced handover"

    RelayTestHelper.send_sync_io(relay_id, @default_namespace, "after_force", %{})
    {:ok, json_state} = RelayManager.get_cell_state(@default_namespace, relay_id)
    state = Payloader.json_to_map(json_state)
    assert "seed" in get_in(state, ["store_input_tasklet", "text_inputs"])
    assert "after_force" in get_in(state, ["store_input_tasklet", "text_inputs"])

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # Attach the peer's drain telemetry (forwarded here as messages), then drive
  # its graceful shutdown to completion.
  defp shutdown_peer(peer_node, timeout_ms) do
    :erpc.call(peer_node, :telemetry_test, :attach_event_handlers, [
      self(),
      [[:flezha, :shutdown, :drain_progress]]
    ])

    :erpc.call(
      peer_node,
      Flezha.Shutdown.Coordinator,
      :shutdown,
      [:cli, [timeout_ms: timeout_ms]],
      timeout_ms + 15_000
    )
  end

  defp start_relay_on_peer(peer_node, prefix, namespace \\ @default_namespace) do
    id =
      RelayFixture.start_on(peer_node, prefix, @test_store_input_factory, namespace: namespace)

    RelayFixture.attach_output_msg({:ok, %{relay_id: id}}, namespace)
    id
  end

  @tag :runner_disconnect
  test "runner disconnect" do
    namespace = @temp_namespace
    runner_id = NodeTestHelper.start_runner(namespace)

    NodeTestHelper.stop_runner(namespace, runner_id)

    assert FleetManager.active_runners(namespace) === []
  end

  @tag :runner_disconnect_migrate_op
  test "runner disconnect migrate relay" do
    namespace = @temp_namespace
    relay_id_1 = "relay_1"
    relay_id_2 = "relay_2"

    # The disconnect is declared and the LOOP rebinds; nothing reroutes inside
    # the session handler any more. Reset the recovery cache before any hub
    # exists, so this namespace's own hub seeds itself `:normal` and the gate
    # does not exclude it.
    ConvergenceSnapshot.reset_recovery_states()
    {:ok, _} = start_supervised({Loop, tick_ms: 1_000})

    # Start 2 runners under the same namespace.
    runner_id_1 = NodeTestHelper.start_runner(namespace)
    runner_id_2 = NodeTestHelper.start_runner(namespace)

    # Start a few relays and store some state on them.
    {:ok, %{pid: _relay_pid_1, runner_id: pipe1_op}} =
      relay_id_1
      |> RelayFixture.get(@test_store_input_factory)
      |> RelayFixture.start(namespace)
      |> RelayFixture.attach_output_msg(namespace)

    {:ok, %{pid: _relay_pid_2, runner_id: _pipe2_op}} =
      relay_id_2
      |> RelayFixture.get(@test_store_input_factory)
      |> RelayFixture.start(namespace)
      |> RelayFixture.attach_output_msg(namespace)

    RelayTestHelper.send_sync_io(relay_id_1, namespace, "state_1", %{})
    RelayTestHelper.send_sync_io(relay_id_2, namespace, "state_2", %{})

    active_runner =
      if pipe1_op === runner_id_1, do: runner_id_2, else: runner_id_1

    for id <- [relay_id_1, relay_id_2], do: await_durable(node(), id)

    # Trigger the runner disconnect.
    NodeTestHelper.stop_runner(namespace, pipe1_op)

    # Make sure all the relays are migrated to the other runner. The loop
    # plans `rebind_relay` from the declared disconnect and commits only on
    # a confirmed migrate, so the tags land asynchronously.
    assert NodeTestHelper.wait_until(
             fn ->
               RelayManager.relay_registry(namespace)
               |> Enum.all?(fn {_relay_id, {_cspec, _cnodes, metadata}} ->
                 metadata[:tag] === active_runner
               end)
             end,
             10_000
           ),
           "relays did not rebind to #{active_runner} within timeout"

    # The rebound runner's state arrives with the WAL restore, after the tag flips.
    stored = &{:ok, ~s({"store_input_tasklet":{"binary_inputs":[],"text_inputs":["#{&1}"]}})}
    states = fn -> Enum.map([relay_id_1, relay_id_2], &cell_state/1) end

    assert NodeTestHelper.wait_until(
             fn -> states.() == [stored.("state_1"), stored.("state_2")] end,
             15_000
           ),
           "relay states did not survive the rebind: #{inspect(states.())}"
  end

  @tag :runner_disconnect_preserve
  test "runner disconnect preserve" do
    namespace = @temp_namespace
    num_relays = 15
    relay_ids = for i <- 1..num_relays, do: "relay_#{i}"

    # Fresh-process runner takeover (an :inactive peer's bindings
    # are migrated onto the freshly-connected runner) is driven by
    # `Flezha.Convergence.Loop`, which is OFF in the default test config
    # to avoid global-tick races. Start it locally for this test, gated
    # on `:runner_connected_event` so the rebind fires within ~ms of
    # `runner_id_3` connecting.
    # The Loop's recovery gate (`Probe.await_recovery_complete/2`) requires
    # every hub the always-on Snapshot has ever observed to be settled. That
    # cache accumulates across test files and is never cleared, so a hub left
    # mid-recovery by an earlier test would block this Loop's first tick
    # forever. Reset it so the gate only weighs this test's hubs.
    ConvergenceSnapshot.reset_recovery_states()

    # Short periodic tick so the reseat reconciles within the assertion
    # budget even if the single `:runner_connected_event` is missed under
    # in-suite contention (other tests also drive the global EventManager).
    {:ok, _} =
      start_supervised({Flezha.Convergence.Loop, tick_ms: 1_000})

    runner_id_1 = NodeTestHelper.start_runner(namespace)
    runner_id_2 = NodeTestHelper.start_runner(namespace)

    # Start relays and store some state on them.

    relay_infos =
      for relay_id <- relay_ids do
        {:ok, info} =
          relay_id
          |> RelayFixture.get(@test_store_input_factory)
          |> RelayFixture.start(namespace)
          |> RelayFixture.attach_output_msg(namespace)

        {relay_id, info}
      end

    # Send state to all relays
    for {relay_id, _info} <- relay_infos do
      RelayTestHelper.send_sync_io(relay_id, namespace, "state_#{relay_id}", %{})
    end

    # Trigger the runner disconnect.
    NodeTestHelper.stop_runner(namespace, runner_id_1)
    NodeTestHelper.stop_runner(namespace, runner_id_2)

    # Start a new runner under the same namespace.
    runner_id_3 = NodeTestHelper.start_runner(namespace)

    # The convergence Loop reseats every relay tagged to a dead runner
    # onto `runner_id_3`. In production a periodic tick + the
    # `:runner_connected_event` drive this; in-suite, async tick delivery
    # and the global recovery gate are non-deterministic, so drive the real
    # reconcile path synchronously via `tick_now/0` on each poll and wait
    # for the reseat to land.
    assert NodeTestHelper.wait_until(
             fn ->
               Loop.tick_now()

               RelayManager.relay_registry(namespace)
               |> Enum.all?(fn {_relay_id, {_cspec, _cnodes, %{tag: tag}}} ->
                 tag === runner_id_3
               end)
             end,
             15_000
           ),
           "relays did not reseat to runner_id_3 within timeout"

    # The rebind tag flips before the WAL restore finishes replaying the
    # change-log onto the reseated runner, so poll the migrated state
    # rather than reading it the instant the tag lands.
    expected_state = fn relay_id ->
      "{\"store_input_tasklet\":{\"binary_inputs\":[],\"text_inputs\":[\"state_#{relay_id}\"]}}"
    end

    # The convergence Loop dispatches the WAL restore once, inside the rebind
    # heal — but once the tag is `runner_id_3` the relay is not misbound,
    # so a later tick won't re-dispatch. Re-drive the documented,
    # idempotent restore re-dispatch path per relay each poll so a restore
    # that raced the new runner's readiness retries until state converges.
    assert NodeTestHelper.wait_until(
             fn ->
               for {relay_id, _info} <- relay_infos do
                 case HubManager.get_pid(namespace, relay_id) do
                   {:ok, pid} when is_pid(pid) ->
                     Replay.kick_restore_for_bound_runner(
                       pid,
                       runner_id_3
                     )

                   _ ->
                     :ok
                 end
               end

               Enum.all?(relay_infos, fn {relay_id, _info} ->
                 RelayManager.get_cell_state(namespace, relay_id) ===
                   {:ok, expected_state.(relay_id)}
               end)
             end,
             15_000
           ),
           "relay states did not migrate to runner_id_3 within timeout"

    # Stop the new runner.
    NodeTestHelper.stop_runner(namespace, runner_id_3)

    # Adjust the runner cleanup interval and trigger the cleanup.
    Application.put_env(:flezha, :runner_keepalive, "0")
    GenServer.call(FleetManager, :runner_cleanup)

    # Reset the runner keepalive to its default value.
    Application.put_env(:flezha, :runner_keepalive, "30000")

    # Cleanup reaps the dead runner ref but must NOT tear the hub down while it
    # still holds the 15 registered relays — they stay addressable and
    # reseat onto the next runner (relay.md, hub-outlives-runner).
    assert HubManager.registered_relays?(namespace)
    assert HubManager.hub_alive?(namespace) === true
  end

  @tag :multi_node
  @tag :runner_disconnect_migrate_hub
  test "runner disconnect migrate hub" do
    namespace = @temp_namespace
    num_relays = 15
    relay_ids = for i <- 1..num_relays, do: "relay_#{i}"

    # Losing its last runner for a factory makes a controller stop serving that
    # factory's group, and the hub then reassigns every relay of it through
    # the migration-consent gate with handover. No convergence op, no hub drain.
    {peer_pid, peer_node} = NodeTestHelper.start_healing_peer(:anode1)

    # The withdrawal's grace is what a returning runner buys; this test has no
    # returning runner, so it is cut to keep the move prompt.
    :erpc.call(peer_node, Application, :put_env, [:flezha, :group_withdraw_grace_ms, 500])

    # Start the runners.
    runner_id_1 = NodeTestHelper.start_runner(namespace)
    rem_runner_1 = NodeTestHelper.start_runner_remote(peer_node, namespace)
    rem_runner_2 = NodeTestHelper.start_runner_remote(peer_node, namespace)

    # Some relays must land on the peer, so every controller has to name it
    # among the factory's servers before the first one is placed.
    NodeTestHelper.await_serving(peer_node, namespace, @test_store_input_factory)

    self = self()
    _node = node()

    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      Hook.child_registered(),
      [
        %ProcessHub.Service.HookManager{
          id: "hub_insert_hook_test",
          m: Process,
          f: :send,
          a: [self, :hook_insert_recv, []]
        }
      ]
    )

    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      Hook.post_children_start(),
      [
        %ProcessHub.Service.HookManager{
          id: "hub_pstartup_hook_test",
          m: Process,
          f: :send,
          a: [self, :hook_pstartup_recv, []]
        }
      ]
    )

    # Start relays and store some state on them.
    relay_infos =
      for relay_id <- relay_ids do
        {:ok, info} =
          relay_id
          |> RelayFixture.get(@test_store_input_factory)
          |> RelayFixture.start(namespace)
          |> RelayFixture.attach_output_msg(namespace)

        {relay_id, info}
      end

    # The relays the peer's runners hold; at least one must move.
    moved = for {id, %{runner_id: rid}} <- relay_infos, rid !== runner_id_1, do: id
    assert moved != []

    # Send state to all relays
    for {relay_id, _info} <- relay_infos do
      RelayTestHelper.send_sync_io(relay_id, namespace, "state_#{relay_id}", %{})
    end

    # Check the states before runner migrations.
    for {relay_id, _info} <- relay_infos do
      {:ok, json_state} = RelayManager.get_cell_state(namespace, relay_id)

      assert json_state ===
               "{\"store_input_tasklet\":{\"binary_inputs\":[],\"text_inputs\":[\"state_#{relay_id}\"]}}"
    end

    # No snapshot is taken: the peer's runners hold the only live copy, so the
    # handover is empty and the destination must pull the source's files.
    for {id, %{node: owner}} <- relay_infos, do: await_durable(owner, id)

    NodeTestHelper.stop_runner_remote(peer_node, namespace, rem_runner_1)
    NodeTestHelper.stop_runner_remote(peer_node, namespace, rem_runner_2)

    # Wait for cross-node ProcessHub sync via hook messages (pure message passing).
    # Each :hook_insert_recv arrives when the local registry syncs a tag update.
    # We check the condition after each message — no polling, no sleeping.
    MessageTestHelper.recv_until(
      :hook_insert_recv,
      fn ->
        RelayManager.relay_registry(namespace)
        |> Enum.all?(fn {_, {_, _, %{tag: tag}}} -> tag === runner_id_1 end)
      end,
      15_000
    )

    collect(do: (:hook_pstartup_recv -> :ok))

    # The tag converges when the registry row moves; the destination Overwatch
    # still has to finish binding and take the handover before it can answer for
    # its Cells. Poll that out — migrated-with-state is an eventual property,
    # and reading it at the first converged tag races the bind.
    assert NodeTestHelper.wait_until(
             fn ->
               Enum.all?(relay_infos, fn {relay_id, _info} ->
                 RelayManager.get_cell_state(namespace, relay_id) ===
                   {:ok,
                    "{\"store_input_tasklet\":{\"binary_inputs\":[],\"text_inputs\":[\"state_#{relay_id}\"]}}"}
               end)
             end,
             15_000
           ),
           "relay states did not survive the cross-controller migration: " <>
             inspect(for id <- moved, do: {id, cell_state(id)})

    for id <- moved do
      assert {:ok, %{arrived_via_hotswap?: false}} =
               RelayManager.get_overwatch_state(namespace, id)
    end

    # A controller whose fleet does not serve the namespace leaves its
    # cluster on the loop's next tick after the hub has moved its last
    # relay away, so this is an eventual property like the two above it.
    assert NodeTestHelper.wait_until(
             fn -> length(HubManager.get_hub_nodes(namespace, true)) === 1 end,
             10_000
           ),
           "the peer kept a hub for a namespace its fleet cannot serve"

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # A stale snapshot arises two ways: a runner's graceful shutdown hands one
  # over, and a caller reads one. A migration away from a runner that later dies
  # hard must ship neither in place of the changes made after it.
  @tag :multi_node
  @tag :stale_snapshot
  @tag timeout: 180_000
  test "a migration after a hard kill ships neither a shutdown snapshot nor one read earlier" do
    {peer_pid, peer} = NodeTestHelper.start_healing_peer(:stale_snap)
    local_runner = NodeTestHelper.start_runner(@temp_namespace)
    {rem_a, os_pid_a} = NodeTestHelper.start_runner_remote_with_pid(peer, @temp_namespace)
    op = start_relay_on_peer(peer, "stale_snapshot", @temp_namespace)
    assert bound_runner(op) == rem_a

    # B is up before A leaves, so the peer rebinds rather than re-placing.
    {rem_b, os_pid_b} = NodeTestHelper.start_runner_remote_with_pid(peer, @temp_namespace)

    RelayTestHelper.send_sync_io(op, @temp_namespace, "before_snapshot", %{})
    assert NodeTestHelper.wait_until(fn -> state_has?(op, "before_snapshot") end, 10_000)

    # The peer's loop waits while A's snapshot is observed, so no rebind can
    # consume it before the test sees that it arrived.
    :ok = :erpc.call(peer, :sys, :suspend, [Loop])
    signal_runner(os_pid_a, "-TERM")

    assert NodeTestHelper.wait_until(
             fn -> contains?(overwatch(op).snapshot, "before_snapshot") end,
             10_000
           ),
           "runner A's shutdown snapshot never reached the Overwatch"

    :ok = :erpc.call(peer, :sys, :resume, [Loop])

    assert NodeTestHelper.wait_until(fn -> bound_runner(op) == rem_b end, 30_000),
           "the relay never rebound to runner B: #{inspect(bound_runner(op))}"

    assert NodeTestHelper.wait_until(fn -> state_has?(op, "before_snapshot") end, 15_000),
           "runner B never received the state: #{inspect(cell_state(op))}"

    # The rebind dropped A's shutdown snapshot, so a read kept from here would
    # be the only snapshot left to ship.
    assert {:ok, snapshot} = RelayManager.create_snapshot(@temp_namespace, op)
    assert contains?(snapshot, "before_snapshot")

    RelayTestHelper.send_sync_io(op, @temp_namespace, "after_snapshot", %{})
    assert NodeTestHelper.wait_until(fn -> state_has?(op, "after_snapshot") end, 10_000)

    await_durable(peer, op)
    signal_runner(os_pid_b, "-KILL")
    assert_pulled_with(op, local_runner, "after_snapshot")
    NodeTestHelper.stop_peer_node(peer_pid, peer)
  end

  # A durable loop on the peer whose only runner dies is moved to this
  # controller, which must bind it on the journal, input log and Cells the peer
  # held: iterations committed there are not run again, and the next message
  # commits exactly one more.
  @tag :multi_node
  @tag :durable_move
  @tag timeout: 240_000
  test "a durable loop moved after its runner died resumes without re-running input" do
    {peer_pid, peer} = NodeTestHelper.start_healing_peer(:durable_move)
    :erpc.call(peer, Application, :put_env, [:flezha, :group_withdraw_grace_ms, 500])
    local_runner = NodeTestHelper.start_runner(@temp_namespace)
    {_rem, os_pid} = NodeTestHelper.start_runner_remote_with_pid(peer, @temp_namespace)
    collector = spawn_link(fn -> StepsResumeHarness.collect_loop([]) end)

    op =
      StepsResumeHarness.start_steps_on_peer(peer, @temp_namespace, "keyed_loop_steps", collector)

    for _ <- 1..3, do: send_trade(op, "SOL-USD:1")

    assert NodeTestHelper.wait_until(
             fn -> StepsResumeHarness.committed_iters(op, peer) == [0, 1, 2] end,
             30_000
           ),
           "the loop never committed three iterations on the peer"

    signal_runner(os_pid, "-KILL")

    assert NodeTestHelper.wait_until(fn -> bound_runner(op) == local_runner end, 150_000),
           "the loop never moved to this controller"

    # This controller's runner is reached by moving here, never across controllers.
    assert overwatch_node(op) == node()

    send_trade(op, "SOL-USD:1")

    assert NodeTestHelper.wait_until(
             fn -> StepsResumeHarness.committed_iters(op, node()) == [0, 1, 2, 3] end,
             30_000
           ),
           "the message sent after the move did not commit one iteration: " <>
             inspect(
               {StepsResumeHarness.committed_iters(op, node()),
                StepsResumeHarness.collected(collector),
                RelayManager.get_overwatch_state(@temp_namespace, op)
                |> then(fn {:ok, s} ->
                  Map.take(s, [
                    :runner_id,
                    :suspended,
                    :parked,
                    :init_error,
                    :migrate_task,
                    :durable_steps
                  ])
                end), Flezha.State.InputLog.tail(op),
                FleetManager.active_runners(@temp_namespace)},
               limit: :infinity
             )

    Process.sleep(1_500)

    # An output the source's flush cursor had not passed is delivered again
    # under the same identity, which a sink absorbs; no iteration ran twice.
    emitted =
      collector
      |> StepsResumeHarness.collected_with_meta()
      |> Enum.uniq_by(fn {_payload, md} -> {md["flz.cell"], md["flz.seq"], md["flz.epoch"]} end)
      |> Enum.map(&elem(&1, 0))

    assert emitted == ["SOL-USD:1", "SOL-USD:2", "SOL-USD:3", "SOL-USD:4"]

    NodeTestHelper.stop_peer_node(peer_pid, peer)
  end

  defp send_trade(op, text), do: StepsResumeHarness.send_turn(@temp_namespace, op, text, "trades")

  defp overwatch_node(op) do
    case RelayManager.get_relay_pid(@temp_namespace, op) do
      {:ok, pid} when is_pid(pid) -> node(pid)
      _ -> nil
    end
  end

  defp assert_pulled_with(op, local_runner, text) do
    assert NodeTestHelper.wait_until(fn -> bound_runner(op) == local_runner end, 30_000),
           "the relay never migrated to this controller: #{inspect(bound_runner(op))}"

    assert NodeTestHelper.wait_until(fn -> state_has?(op, text) end, 15_000),
           "state on the destination lacks #{inspect(text)}: " <>
             "state=#{inspect(cell_state(op))} " <>
             "arrived_via_hotswap?=#{inspect(overwatch(op).arrived_via_hotswap?)}"

    refute overwatch(op).arrived_via_hotswap?
  end

  # A live read can be ahead of the WAL, and a hard kill loses that gap by
  # design, so a test that kills a runner first waits for the WAL to catch up.
  defp await_durable(node, op, namespace \\ @temp_namespace) do
    cell_id = {op, "store_input_tasklet", ""}

    observe = fn ->
      with {:ok, _json, seq} = read <-
             RelayManager.get_cell_state(namespace, op, %{tasklet: elem(cell_id, 1)}),
           {:ok, writer} <- :erpc.call(node, Flezha.State.WriterRegistry, :lookup, [cell_id]) do
        {read, :erpc.call(node, Flezha.State.WalWriter, :last_persisted_seq, [writer]), seq}
      end
    end

    # `change_seq` counts the records applied; the WAL numbers them from 0.
    caught_up? = fn -> match?({_, persisted, seq} when persisted + 1 >= seq, observe.()) end

    assert NodeTestHelper.wait_until(caught_up?, 15_000),
           "the WAL on #{node} never caught up with #{op}: #{inspect(observe.())}"
  end

  defp signal_runner(os_pid, signal),
    do: {_, 0} = System.cmd("kill", [signal, Integer.to_string(os_pid)], stderr_to_stdout: true)

  defp overwatch(op) do
    case RelayManager.get_overwatch_state(@temp_namespace, op) do
      {:ok, %{} = state} -> state
      _ -> %{runner_id: nil, snapshot: nil, arrived_via_hotswap?: nil}
    end
  end

  defp bound_runner(op), do: overwatch(op).runner_id
  defp cell_state(op), do: RelayManager.get_cell_state(@temp_namespace, op)
  defp contains?(bin, text), do: is_binary(bin) and String.contains?(bin, text)

  defp state_has?(op, text) do
    case cell_state(op) do
      {:ok, json} -> contains?(json, text)
      _ -> false
    end
  end
end
