defmodule FlezhaTest.Handover do
  alias Flezha.Identificator
  alias Flezha.Payloader
  alias Flezha.Service.RelayManager
  alias Flezha.State.Commands
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.RelayTestHelper
  alias FlezhaTest.Helper.TierE2EHarness
  alias ProcessHub.Constant.Hook

  use ExUnit.Case
  use Flezha.Constant

  @default_namespace "default"

  @test_store_input_factory "store_input"

  setup _context do
    # Restore now rehydrates a relay from its WAL on (re)bind, so leftover
    # per-Cell state from an earlier test that reused the same relay_id
    # would leak in. Start each test from empty cells.
    TestHelpers.reset_all_cells()
    on_exit(fn -> RelayManager.stop_all_relays(@default_namespace) end)
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
    assert arrived?(relay_id), "expected live HotSwap handover, not a WAL cold restore"

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
      Task.async(fn ->
        Flezha.Relay.Calls.Inbound.call(source, "x", waits, 40_000, relay_id, "cli")
      end)

    timed =
      Task.async(fn ->
        Flezha.Relay.Calls.Inbound.call(source, "y", waits, 15_000, relay_id, "cli")
      end)

    assert NodeTestHelper.wait_until(
             fn -> map_size(:sys.get_state(source).calls.rows) == 2 end,
             5_000
           )

    [token] =
      for {token, %{deadline_ms: 40_000}} <- :sys.get_state(source).calls.rows, do: token

    assert :ok = shutdown_peer(peer_node, 30_000)

    assert arrived?(relay_id)

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

    # The shortest budget that still leaves the drain its one-second floor of
    # consent and the whole forced-handover grace after it.
    assert :ok = shutdown_peer(peer_node, 4_000)

    assert_receive :deferred, 5_000

    assert_receive {[:flezha, :shutdown, :drain_progress], _ref, m,
                    %{namespace: @default_namespace}},
                   5_000

    assert m.forced >= 1

    # Forced onto this node through the best-effort handover: state carried,
    # protocol still live. The drain reports when the local children are gone,
    # which is the last step the departing node controls — the registry row
    # naming their new home converges just after, so poll for it.
    assert arrived?(relay_id),
           "the relay must become addressable at its new home after the forced handover"

    RelayTestHelper.send_sync_io(relay_id, @default_namespace, "after_force", %{})
    {:ok, json_state} = RelayManager.get_cell_state(@default_namespace, relay_id)
    state = Payloader.json_to_map(json_state)
    assert "seed" in get_in(state, ["store_input_tasklet", "text_inputs"])
    assert "after_force" in get_in(state, ["store_input_tasklet", "text_inputs"])

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # fails when: a compaction where the relay moved snapshots a Cell holding no
  # timers, so the restore after it rebuilds none and the session never closes;
  # or the destination's own bind restores nothing, so `idle`, moved and then
  # left alone, never closes.
  @tag :multi_node
  @tag :drain_migration_timers
  @tag timeout: 180_000
  test "a pending timer survives a live move, a compaction there and a restore" do
    # The peer's runner inherits it: a session closes 30 s after its input.
    System.put_env("TIMER_TEST_TIMEOUT_MS", "30000")
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_timer_node)
    System.delete_env("TIMER_TEST_TIMEOUT_MS")
    op = RelayFixture.start_on(peer_node, "drain_timer", "session_timeout")
    idle = RelayFixture.start_on(peer_node, "drain_timer_idle", "session_timeout")

    for id <- [op, idle] do
      assert NodeTestHelper.wait_until(fn -> session(id) != nil end, 30_000)
      RelayTestHelper.cast_io(id, @default_namespace, "hello", %{"input_type" => "2"})
      assert NodeTestHelper.wait_until(fn -> session(id)["touched"] == ["hello"] end, 15_000)
    end

    assert :ok = shutdown_peer(peer_node, 30_000)
    assert arrived?(op) and arrived?(idle)
    assert session(idle) == %{"touched" => ["hello"], "closed_at" => []}
    :telemetry_test.attach_event_handlers(self(), [[:flezha, :state, :compaction_completed]])
    cell = {op, "session_timeout_tasklet", ""}
    {:ok, %{runner_id: runner}} = RelayManager.get_overwatch_state(@default_namespace, op)
    {:ok, _} = Commands.dispatch_compact_cell(runner, cell)
    assert_receive {[:flezha, :state, :compaction_completed], _, _, %{relay: ^op}}, 10_000
    assert session(op)["closed_at"] == [], "the timer fired before the restore"

    TierE2EHarness.force_fresh_rebind(@default_namespace, op)
    assert NodeTestHelper.wait_until(fn -> session(op)["closed_at"] not in [nil, []] end, 60_000)

    assert NodeTestHelper.wait_until(
             fn -> session(idle)["closed_at"] not in [nil, []] end,
             30_000
           ),
           "the destination's bind did not restore the moved Cell's timer"

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # Whether the relay was handed over live to this controller, polled: the
  # registry row naming its new home converges just after the drain reports.
  defp arrived?(op) do
    NodeTestHelper.wait_until(
      fn ->
        match?(
          {:ok, %{arrived_via_hotswap?: true}},
          RelayManager.get_overwatch_state(@default_namespace, op)
        )
      end,
      5_000,
      25
    )
  end

  defp session(op) do
    with {:ok, json} <- RelayManager.get_cell_state(@default_namespace, op),
         do: Payloader.json_to_map(json)["session_timeout_tasklet"],
         else: (_unreadable -> nil)
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

  defp start_relay_on_peer(peer_node, prefix) do
    id = RelayFixture.start_on(peer_node, prefix, @test_store_input_factory)
    RelayFixture.attach_output_msg({:ok, %{relay_id: id}}, @default_namespace)
    id
  end
end
