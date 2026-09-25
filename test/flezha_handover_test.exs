defmodule FlezhaTest.Handover do
  alias Flezha.Convergence.Loop
  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Identificator
  alias Flezha.Operator.Replay
  alias Flezha.Payloader
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.OperatorFixture
  alias FlezhaTest.Helper.MessageTestHelper
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.OperatorTestHelper
  alias ProcessHub.Constant.Hook

  use ExUnit.Case
  use Flezha.Constant

  @default_namespace "default"
  @temp_namespace "test_temp_namespace"

  @test_store_input_factory "store_input"

  setup_all do
    %{}
  end

  setup _context do
    # Restore now rehydrates an operator from its WAL on (re)bind, so leftover
    # per-Cell state from an earlier test that reused the same operator_id
    # would leak in. Start each test from empty cells.
    TestHelpers.reset_all_cells()

    on_exit(fn ->
      OperatorManager.stop_all_operators(@default_namespace)
      FleetManager.stop_namespace(@temp_namespace)
    end)

    :ok
  end

  @tag :multi_node
  @tag :drain_migration
  test "graceful shutdown drains an operator to a peer through live handover" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_node1)

    operator_id = start_operator_on_peer(peer_node, "drain_mig")
    OperatorTestHelper.send_sync_io(operator_id, @default_namespace, "before_drain", %{})

    assert :ok = shutdown_peer(peer_node, 30_000)

    # One :drain_progress per hub — match the namespace the operator is in, or
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
                 OperatorManager.get_overwatch_state(@default_namespace, operator_id)
               )
             end,
             5_000,
             25
           ),
           "expected live HotSwap handover, not a WAL cold restore"

    {:ok, json_state} = OperatorManager.get_cell_state(@default_namespace, operator_id)
    state = Payloader.json_to_map(json_state)
    assert "before_drain" in get_in(state, ["store_input_tasklet", "text_inputs"])

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  @tag :multi_node
  @tag :drain_migration_forced
  test "a deferring operator is force-migrated at the drain deadline without wedging" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_node2)

    operator_id = start_operator_on_peer(peer_node, "drain_forced")
    OperatorTestHelper.send_sync_io(operator_id, @default_namespace, "seed", %{})

    # Hold the busy signal: a correction that never leaves its debounce
    # window, so `migration_consent/1` answers :defer for the whole drain.
    :ok =
      :erpc.call(peer_node, Application, :put_env, [:flezha, :correction_debounce_ms, 600_000])

    hub_id = Identificator.hub(@default_namespace)

    {^operator_id, ow_pid} =
      :erpc.call(peer_node, ProcessHub, :process_list, [hub_id, :local])
      |> Enum.find(fn {id, _pid} -> id == operator_id end)

    GenServer.cast(
      ow_pid,
      {:correction_request, {operator_id, "store_input_tasklet", "poke"}, 1}
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
                 OperatorManager.get_overwatch_state(@default_namespace, operator_id)
               )
             end,
             5_000,
             25
           ),
           "the operator must become addressable at its new home after the forced handover"

    OperatorTestHelper.send_sync_io(operator_id, @default_namespace, "after_force", %{})
    {:ok, json_state} = OperatorManager.get_cell_state(@default_namespace, operator_id)
    state = Payloader.json_to_map(json_state)
    assert "seed" in get_in(state, ["store_input_tasklet", "text_inputs"])
    assert "after_force" in get_in(state, ["store_input_tasklet", "text_inputs"])

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # Attach the peer's drain telemetry (relayed here as messages), then drive
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

  # The consistent-hash ring decides placement by id, so probe candidate ids
  # until one lands on the peer; the rejects are stopped again. Placement can
  # only reach the peer after it has joined the hub cluster, hence the wait.
  defp start_operator_on_peer(peer_node, prefix, namespace \\ @default_namespace) do
    NodeTestHelper.wait_until(
      fn -> peer_node in HubManager.get_hub_nodes(namespace, false) end,
      10_000
    ) || flunk("peer #{inspect(peer_node)} never joined the hub cluster")

    Enum.reduce_while(1..20, nil, fn i, _acc ->
      id = "#{prefix}_#{i}"

      {:ok, %{node: pnode}} =
        id
        |> OperatorFixture.get(@test_store_input_factory)
        |> OperatorFixture.start(namespace)
        |> OperatorFixture.attach_output_msg(namespace)

      if pnode == peer_node do
        {:halt, id}
      else
        OperatorFixture.stop(id, namespace)
        {:cont, nil}
      end
    end) || flunk("no operator landed on #{inspect(peer_node)} in 20 attempts")
  end

  @tag :runner_disconnect
  test "runner disconnect" do
    namespace = @temp_namespace
    runner_id = NodeTestHelper.start_runner(namespace)

    NodeTestHelper.stop_runner(namespace, runner_id)

    assert FleetManager.active_runners(namespace) === []
  end

  @tag :runner_disconnect_migrate_op
  test "runner disconnect migrate operator" do
    namespace = @temp_namespace
    operator_id_1 = "operator_1"
    operator_id_2 = "operator_2"

    # The disconnect is declared and the LOOP rebinds; nothing reroutes inside
    # the session handler any more. Reset the recovery cache before any hub
    # exists, so this namespace's own hub seeds itself `:normal` and the gate
    # does not exclude it.
    ConvergenceSnapshot.reset_recovery_states()
    {:ok, _} = start_supervised({Loop, tick_ms: 1_000})

    # Start 2 runners under the same namespace.
    runner_id_1 = NodeTestHelper.start_runner(namespace)
    runner_id_2 = NodeTestHelper.start_runner(namespace)

    # Start a few operators and store some state on them.
    {:ok, %{pid: _operator_pid_1, runner_id: pipe1_op}} =
      operator_id_1
      |> OperatorFixture.get(@test_store_input_factory)
      |> OperatorFixture.start(namespace)
      |> OperatorFixture.attach_output_msg(namespace)

    {:ok, %{pid: _operator_pid_2, runner_id: _pipe2_op}} =
      operator_id_2
      |> OperatorFixture.get(@test_store_input_factory)
      |> OperatorFixture.start(namespace)
      |> OperatorFixture.attach_output_msg(namespace)

    OperatorTestHelper.send_sync_io(operator_id_1, namespace, "state_1", %{})
    OperatorTestHelper.send_sync_io(operator_id_2, namespace, "state_2", %{})

    active_runner =
      if pipe1_op === runner_id_1, do: runner_id_2, else: runner_id_1

    for id <- [operator_id_1, operator_id_2], do: await_durable(node(), id)

    # Trigger the runner disconnect.
    NodeTestHelper.stop_runner(namespace, pipe1_op)

    # Make sure all the operators are migrated to the other runner. The loop
    # plans `rebind_operator` from the declared disconnect and commits only on
    # a confirmed migrate, so the tags land asynchronously.
    assert NodeTestHelper.wait_until(
             fn ->
               OperatorManager.operator_registry(namespace)
               |> Enum.all?(fn {_operator_id, {_cspec, _cnodes, metadata}} ->
                 metadata[:tag] === active_runner
               end)
             end,
             10_000
           ),
           "operators did not rebind to #{active_runner} within timeout"

    # The rebound runner's state arrives with the WAL restore, after the tag flips.
    stored = &{:ok, ~s({"store_input_tasklet":{"binary_inputs":[],"text_inputs":["#{&1}"]}})}
    states = fn -> Enum.map([operator_id_1, operator_id_2], &cell_state/1) end

    assert NodeTestHelper.wait_until(
             fn -> states.() == [stored.("state_1"), stored.("state_2")] end,
             15_000
           ),
           "operator states did not survive the rebind: #{inspect(states.())}"
  end

  @tag :runner_disconnect_preserve
  test "runner disconnect preserve" do
    namespace = @temp_namespace
    num_operators = 15
    operator_ids = for i <- 1..num_operators, do: "operator_#{i}"

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

    # Start operators and store some state on them.

    operator_infos =
      for operator_id <- operator_ids do
        {:ok, info} =
          operator_id
          |> OperatorFixture.get(@test_store_input_factory)
          |> OperatorFixture.start(namespace)
          |> OperatorFixture.attach_output_msg(namespace)

        {operator_id, info}
      end

    # Send state to all operators
    for {operator_id, _info} <- operator_infos do
      OperatorTestHelper.send_sync_io(operator_id, namespace, "state_#{operator_id}", %{})
    end

    # Trigger the runner disconnect.
    NodeTestHelper.stop_runner(namespace, runner_id_1)
    NodeTestHelper.stop_runner(namespace, runner_id_2)

    # Start a new runner under the same namespace.
    runner_id_3 = NodeTestHelper.start_runner(namespace)

    # The convergence Loop reseats every operator tagged to a dead runner
    # onto `runner_id_3`. In production a periodic tick + the
    # `:runner_connected_event` drive this; in-suite, async tick delivery
    # and the global recovery gate are non-deterministic, so drive the real
    # reconcile path synchronously via `tick_now/0` on each poll and wait
    # for the reseat to land.
    assert NodeTestHelper.wait_until(
             fn ->
               Loop.tick_now()

               OperatorManager.operator_registry(namespace)
               |> Enum.all?(fn {_operator_id, {_cspec, _cnodes, %{tag: tag}}} ->
                 tag === runner_id_3
               end)
             end,
             15_000
           ),
           "operators did not reseat to runner_id_3 within timeout"

    # The rebind tag flips before the WAL restore finishes replaying the
    # change-log onto the reseated runner, so poll the migrated state
    # rather than reading it the instant the tag lands.
    expected_state = fn operator_id ->
      "{\"store_input_tasklet\":{\"binary_inputs\":[],\"text_inputs\":[\"state_#{operator_id}\"]}}"
    end

    # The convergence Loop dispatches the WAL restore once, inside the rebind
    # heal — but once the tag is `runner_id_3` the operator is not misbound,
    # so a later tick won't re-dispatch. Re-drive the documented,
    # idempotent restore re-dispatch path per operator each poll so a restore
    # that raced the new runner's readiness retries until state converges.
    assert NodeTestHelper.wait_until(
             fn ->
               for {operator_id, _info} <- operator_infos do
                 case HubManager.get_pid(namespace, operator_id) do
                   {:ok, pid} when is_pid(pid) ->
                     Replay.kick_restore_for_bound_runner(
                       pid,
                       runner_id_3
                     )

                   _ ->
                     :ok
                 end
               end

               Enum.all?(operator_infos, fn {operator_id, _info} ->
                 OperatorManager.get_cell_state(namespace, operator_id) ===
                   {:ok, expected_state.(operator_id)}
               end)
             end,
             15_000
           ),
           "operator states did not migrate to runner_id_3 within timeout"

    # Stop the new runner.
    NodeTestHelper.stop_runner(namespace, runner_id_3)

    # Adjust the runner cleanup interval and trigger the cleanup.
    Application.put_env(:flezha, :runner_keepalive, "0")
    GenServer.call(FleetManager, :runner_cleanup)

    # Reset the runner keepalive to its default value.
    Application.put_env(:flezha, :runner_keepalive, "30000")

    # Cleanup reaps the dead runner ref but must NOT tear the hub down while it
    # still holds the 15 registered operators — they stay addressable and
    # reseat onto the next runner (bounded-operator.md, hub-outlives-runner).
    assert HubManager.registered_operators?(namespace)
    assert HubManager.hub_alive?(namespace) === true
  end

  @tag :multi_node
  @tag :runner_disconnect_migrate_hub
  test "runner disconnect migrate hub" do
    namespace = @temp_namespace
    num_operators = 15
    operator_ids = for i <- 1..num_operators, do: "operator_#{i}"

    # Losing its last runner for a factory makes a controller stop serving that
    # factory's group, and the hub then reassigns every operator of it through
    # the migration-consent gate with handover. No convergence op, no hub drain.
    {peer_pid, peer_node} = start_healing_peer(:anode1)

    # The withdrawal's grace is what a returning runner buys; this test has no
    # returning runner, so it is cut to keep the move prompt.
    :erpc.call(peer_node, Application, :put_env, [:flezha, :group_withdraw_grace_ms, 500])

    # Start the runners.
    runner_id_1 = NodeTestHelper.start_runner(namespace)
    rem_runner_1 = NodeTestHelper.start_runner_remote(peer_node, namespace)
    rem_runner_2 = NodeTestHelper.start_runner_remote(peer_node, namespace)

    # Wait for peer node to join the ProcessHub hub cluster
    hub_id = Identificator.hub(namespace)
    test_pid = self()

    ProcessHub.register_hook_handlers(
      hub_id,
      Hook.post_node_join(),
      [
        %ProcessHub.Service.HookManager{
          id: "hub_node_join_wait",
          m: Process,
          f: :send,
          a: [test_pid, :peer_node_joined, []]
        }
      ]
    )

    if length(HubManager.get_hub_nodes(namespace, true)) < 2 do
      receive do
        :peer_node_joined -> :ok
      after
        5_000 -> flunk("Peer node did not join the hub cluster within 5 seconds")
      end
    end

    ProcessHub.cancel_hook_handlers(hub_id, Hook.post_node_join(), [
      "hub_node_join_wait"
    ])

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

    # Start operators and store some state on them.
    operator_infos =
      for operator_id <- operator_ids do
        {:ok, info} =
          operator_id
          |> OperatorFixture.get(@test_store_input_factory)
          |> OperatorFixture.start(namespace)
          |> OperatorFixture.attach_output_msg(namespace)

        {operator_id, info}
      end

    # The operators the peer's runners hold; at least one must move.
    moved = for {id, %{runner_id: rid}} <- operator_infos, rid !== runner_id_1, do: id
    assert moved != []

    # Send state to all operators
    for {operator_id, _info} <- operator_infos do
      OperatorTestHelper.send_sync_io(operator_id, namespace, "state_#{operator_id}", %{})
    end

    # Check the states before runner migrations.
    for {operator_id, _info} <- operator_infos do
      {:ok, json_state} = OperatorManager.get_cell_state(namespace, operator_id)

      assert json_state ===
               "{\"store_input_tasklet\":{\"binary_inputs\":[],\"text_inputs\":[\"state_#{operator_id}\"]}}"
    end

    # No snapshot is taken: the peer's runners hold the only live copy, so the
    # handover is empty and the destination must pull the source's files.
    for {id, %{node: owner}} <- operator_infos, do: await_durable(owner, id)

    NodeTestHelper.stop_runner_remote(peer_node, namespace, rem_runner_1)
    NodeTestHelper.stop_runner_remote(peer_node, namespace, rem_runner_2)

    # Wait for cross-node ProcessHub sync via hook messages (pure message passing).
    # Each :hook_insert_recv arrives when the local registry syncs a tag update.
    # We check the condition after each message — no polling, no sleeping.
    MessageTestHelper.recv_until(
      :hook_insert_recv,
      fn ->
        OperatorManager.operator_registry(namespace)
        |> Enum.all?(fn {_, {_, _, %{tag: tag}}} -> tag === runner_id_1 end)
      end,
      15_000
    )

    MessageTestHelper.flush(:hook_pstartup_recv)

    # The tag converges when the registry row moves; the destination Overwatch
    # still has to finish binding and take the handover before it can answer for
    # its Cells. Poll that out — migrated-with-state is an eventual property,
    # and reading it at the first converged tag races the bind.
    assert NodeTestHelper.wait_until(
             fn ->
               Enum.all?(operator_infos, fn {operator_id, _info} ->
                 OperatorManager.get_cell_state(namespace, operator_id) ===
                   {:ok,
                    "{\"store_input_tasklet\":{\"binary_inputs\":[],\"text_inputs\":[\"state_#{operator_id}\"]}}"}
               end)
             end,
             15_000
           ),
           "operator states did not survive the cross-controller migration: " <>
             inspect(for id <- moved, do: {id, cell_state(id)})

    for id <- moved do
      assert {:ok, %{arrived_via_hotswap?: false}} =
               OperatorManager.get_overwatch_state(namespace, id)
    end

    # A controller whose fleet does not serve the namespace leaves its
    # cluster on the loop's next tick after the hub has moved its last
    # operator away, so this is an eventual property like the two above it.
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
    {peer_pid, peer} = start_healing_peer(:stale_snap)
    local_runner = NodeTestHelper.start_runner(@temp_namespace)
    {rem_a, os_pid_a} = start_remote_runner(peer)
    op = start_operator_on_peer(peer, "stale_snapshot", @temp_namespace)
    assert bound_runner(op) == rem_a

    # B is up before A leaves, so the peer rebinds rather than re-placing.
    {rem_b, os_pid_b} = start_remote_runner(peer)

    OperatorTestHelper.send_sync_io(op, @temp_namespace, "before_snapshot", %{})
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
           "the operator never rebound to runner B: #{inspect(bound_runner(op))}"

    assert NodeTestHelper.wait_until(fn -> state_has?(op, "before_snapshot") end, 15_000),
           "runner B never received the state: #{inspect(cell_state(op))}"

    # The rebind dropped A's shutdown snapshot, so a read kept from here would
    # be the only snapshot left to ship.
    assert {:ok, snapshot} = OperatorManager.create_snapshot(@temp_namespace, op)
    assert contains?(snapshot, "before_snapshot")

    OperatorTestHelper.send_sync_io(op, @temp_namespace, "after_snapshot", %{})
    assert NodeTestHelper.wait_until(fn -> state_has?(op, "after_snapshot") end, 10_000)

    await_durable(peer, op)
    signal_runner(os_pid_b, "-KILL")
    assert_pulled_with(op, local_runner, "after_snapshot")
    NodeTestHelper.stop_peer_node(peer_pid, peer)
  end

  # Both controllers heal, as in production: only the peer's own loop may
  # re-place the operators it hosts.
  defp start_healing_peer(name) do
    {:ok, _} = start_supervised({Loop, tick_ms: 1_000})

    NodeTestHelper.start_peer_node(name,
      app_env: [convergence_health_enabled: true, convergence_tick_ms: 1_000]
    )
  end

  defp assert_pulled_with(op, local_runner, text) do
    assert NodeTestHelper.wait_until(fn -> bound_runner(op) == local_runner end, 30_000),
           "the operator never migrated to this controller: #{inspect(bound_runner(op))}"

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
             OperatorManager.get_cell_state(namespace, op, %{tasklet: elem(cell_id, 1)}),
           {:ok, writer} <- :erpc.call(node, Flezha.State.WriterRegistry, :lookup, [cell_id]) do
        {read, :erpc.call(node, Flezha.State.WalWriter, :last_persisted_seq, [writer]), seq}
      end
    end

    # `change_seq` counts the records applied; the WAL numbers them from 0.
    caught_up? = fn -> match?({_, persisted, seq} when persisted + 1 >= seq, observe.()) end

    assert NodeTestHelper.wait_until(caught_up?, 15_000),
           "the WAL on #{node} never caught up with #{op}: #{inspect(observe.())}"
  end

  # A runner started on the peer, with the OS pid of the port that launched it.
  defp start_remote_runner(peer) do
    before = runner_os_pids(peer)
    runner_id = NodeTestHelper.start_runner_remote(peer, @temp_namespace)

    case runner_os_pids(peer) -- before do
      [os_pid] -> {runner_id, os_pid}
      other -> flunk("expected one new runner port on the peer, got #{inspect(other)}")
    end
  end

  defp runner_os_pids(peer) do
    for port <- :erpc.call(peer, :erlang, :ports, []),
        {:os_pid, os_pid} <- [:erpc.call(peer, :erlang, :port_info, [port, :os_pid])],
        is_integer(os_pid),
        do: os_pid
  end

  defp signal_runner(os_pid, signal),
    do: {_, 0} = System.cmd("kill", [signal, Integer.to_string(os_pid)], stderr_to_stdout: true)

  defp overwatch(op) do
    case OperatorManager.get_overwatch_state(@temp_namespace, op) do
      {:ok, %{} = state} -> state
      _ -> %{runner_id: nil, snapshot: nil, arrived_via_hotswap?: nil}
    end
  end

  defp bound_runner(op), do: overwatch(op).runner_id
  defp cell_state(op), do: OperatorManager.get_cell_state(@temp_namespace, op)
  defp contains?(bin, text), do: is_binary(bin) and String.contains?(bin, text)

  defp state_has?(op, text) do
    case cell_state(op) do
      {:ok, json} -> contains?(json, text)
      _ -> false
    end
  end
end
