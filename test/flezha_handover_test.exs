defmodule FlezhaTest.Handover do
  @moduledoc """
  Relays moving between controllers on a drain: by cold swap, the default, the
  source stops before the destination starts and the destination restores
  from the copy it pulls; in a namespace set to hot, the destination starts
  first and takes the state live.
  """
  alias Flezha.Cluster.GroupedHashing
  alias Flezha.Identificator
  alias Flezha.Payloader
  alias Flezha.Process.Hub
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Service.RelayManager
  alias Flezha.State.Commands
  alias Flezha.State.TestHelpers
  alias Flezha.State.WriterRegistry
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.RelayTestHelper
  alias FlezhaTest.Helper.TierE2EHarness
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias ProcessHub.Worker.WorkerQueue

  use ExUnit.Case
  use Flezha.Constant

  @default_namespace "default"

  @test_store_input_factory "store_input"

  # A peer whose `default` hub moves its relays by hot swap.
  @hot [migrations: [{"default", :hot}]]

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
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_node1, @hot)

    relay_id = start_relay_on_peer(peer_node, "drain_mig")
    RelayTestHelper.send_sync_io(relay_id, @default_namespace, "before_drain", %{})

    assert :ok = shutdown_peer(peer_node, 30_000)

    m = drain_progress(10_000)

    assert m.migrated >= 1
    assert m.forced == 0

    # The drain reports when the local children are gone, which is the last
    # step the departing node controls; the registry row naming their new home
    # converges just after, so poll for it.
    assert arrived?(relay_id), "expected live HotSwap handover, not a WAL cold restore"

    assert "before_drain" in text_inputs(relay_id)

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # fails when: a redistribution pass that runs while the drain's hot swap is
  # still in flight takes the relay's state again, after its runner has given
  # it up, and that empty answer is the one the destination receives.
  @tag :multi_node
  @tag :drain_recheck_in_flight
  test "a recheck during a drain's live handover does not hand the relay over again" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_recheck_node, @hot)
    relay_id = start_relay_on_peer(peer_node, "drain_recheck")
    RelayTestHelper.send_sync_io(relay_id, @default_namespace, "before_drain", %{})
    hub_id = Identificator.hub(@default_namespace)
    peer_hub = :erpc.call(peer_node, ProcessHub.Hub, :get, [hub_id])
    test_pid = self()

    # This node starts the moved relay on its hub's worker queue: held, the
    # move stays in flight while the peer runs its recheck.
    here = ProcessHub.Hub.get(hub_id).procs.worker_queue
    :ok = :sys.suspend(here)

    try do
      shutdown = Task.async(fn -> shutdown_peer(peer_node, 30_000, test_pid) end)

      assert NodeTestHelper.wait_until(
               fn -> relay_id in :erpc.call(peer_node, Hub, :pending_handovers, [hub_id]) end,
               10_000
             )

      :ok = :erpc.call(peer_node, GroupedHashing, :run_recheck, [peer_hub])
      # The recheck runs on the peer's worker queue; this call returns behind it.
      :erpc.call(peer_node, WorkerQueue, :run_on, [peer_hub.procs.worker_queue, &:erlang.self/0])
      :ok = :sys.resume(here)

      assert :ok = Task.await(shutdown, 45_000)
    after
      :sys.resume(here)
    end

    assert arrived?(relay_id), "the destination took an empty second handover"
    assert "before_drain" in text_inputs(relay_id)

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # fails when: the waiting callers stay behind with the departing Overwatch,
  # which then answers them `call.callee_ended` as it stops, or the arriving one
  # does not re-arm their deadlines.
  @tag :multi_node
  @tag :drain_migration_calls
  test "a caller waiting on a relay that drains to a peer is answered from there" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_calls_node, @hot)
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
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_node2, @hot)
    relay_id = deferring_relay_on(peer_node)

    # The shortest budget that still leaves the drain its one-second floor of
    # consent and the whole forced-handover grace after it.
    assert :ok = shutdown_peer(peer_node, 4_000)

    m = drain_progress(5_000)
    assert_receive :deferred, 5_000
    assert m.forced >= 1

    # Forced onto this node through the best-effort handover: state carried,
    # protocol still live. The drain reports when the local children are gone,
    # which is the last step the departing node controls — the registry row
    # naming their new home converges just after, so poll for it.
    assert arrived?(relay_id),
           "the relay must become addressable at its new home after the forced handover"

    RelayTestHelper.send_sync_io(relay_id, @default_namespace, "after_force", %{})
    assert "seed" in text_inputs(relay_id)
    assert "after_force" in text_inputs(relay_id)

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # fails when: the drain waits for, or counts as moved, a relay that ended on
  # the departing node instead of moving.
  @tag :multi_node
  @tag :drain_ended_relay
  @tag timeout: 120_000
  test "a drain waits for and counts only the relays that move" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_ended_node)
    moved = start_relay_on_peer(peer_node, "drain_moves")
    ended = deferring_relay_on(peer_node)
    hub_id = Identificator.hub(@default_namespace)

    assert NodeTestHelper.wait_until(fn -> local_child(peer_node, hub_id, moved) end, 10_000),
           "the relay meant to move did not settle on the peer"

    test_pid = self()
    shutdown = Task.async(fn -> shutdown_peer(peer_node, 60_000, test_pid) end)

    assert_receive :deferred, 10_000
    {:ok, :stop_initiated} = :erpc.call(peer_node, ProcessHub, :stop_child, [hub_id, ended])
    assert :ok = Task.await(shutdown, 80_000)

    m = drain_progress(1_000)

    assert m.migrated == 1
    assert m.elapsed_ms < 30_000, "the drain waited #{m.elapsed_ms} ms for the relay that ended"
    assert landed_here?(moved)

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
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_timer_node, @hot)
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

  # fails when: a cold drain lets the relay run on two controllers at once,
  # binds here without the state the source flushed as it stopped, or loses an
  # input a confirming sender was told was taken.
  @tag :multi_node
  @tag :cold_drain
  test "a cold drain stops the relay before it starts here and loses no confirmed input" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:cold_drain_node)
    relay_id = start_relay_on_peer(peer_node, "cold_drain")
    {:ok, source} = RelayManager.get_relay_pid(@default_namespace, relay_id)
    watch_arrival(relay_id, peer_node, source)
    RelayTestHelper.send_sync_io(relay_id, @default_namespace, "before_drain", %{})
    feeder = start_feeder(relay_id, &"fed_#{&1}")

    assert :ok = shutdown_peer(peer_node, 30_000)

    m = drain_progress(10_000)

    assert m.migrated >= 1
    assert_receive {:arriving, ^relay_id, source_alive?, open_writers}, 10_000
    refute source_alive?, "the destination started while the source Overwatch ran"
    assert open_writers == [], "the source's writers were open as the destination started"
    assert landed_here?(relay_id)

    sent = stop_feeder(feeder, 5)
    # A taken input reaches the state once its runner has run it.
    NodeTestHelper.wait_until(fn -> sent -- text_inputs(relay_id) == [] end, 10_000)
    inputs = text_inputs(relay_id)
    assert "before_drain" in inputs
    assert sent -- inputs == [], "confirmed inputs lost in the move: #{inspect(sent -- inputs)}"

    assert {:ok, %{arrived_via_hotswap?: false}} =
             RelayManager.get_overwatch_state(@default_namespace, relay_id)

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # fails when: a cold drain drops a relay without an input log while its Cell
  # is busy, so the confirmed inputs queued behind the running one are lost.
  @tag :multi_node
  @tag :cold_drain_busy
  test "a cold drain applies the confirmed inputs a busy Cell held queued" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:cold_busy_node)
    relay_id = start_relay_on_peer(peer_node, "cold_busy")
    release = Path.join(System.tmp_dir!(), "#{relay_id}.release")
    on_exit(fn -> File.rm(release) end)
    sent = ["hold:#{release}" | for(n <- 1..5, do: "queued_#{n}")]
    Enum.each(sent, &confirm(relay_id, &1))

    drain = Task.async(fn -> shutdown_peer(peer_node, 30_000) end)
    # The class entry goes in the step before the runner is told to stop.
    assert NodeTestHelper.wait_until(
             fn -> :erpc.call(peer_node, ClassRegistry, :owner, [relay_id]) == :error end,
             15_000
           )

    File.write!(release, "")
    assert :ok = Task.await(drain, 45_000)
    assert landed_here?(relay_id)
    NodeTestHelper.wait_until(fn -> sent -- text_inputs(relay_id) == [] end, 10_000)
    inputs = text_inputs(relay_id)
    assert sent -- inputs == [], "confirmed inputs lost in the move: #{inspect(sent -- inputs)}"
    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # fails when: a cold move of a class that keeps nothing on disk ships no
  # in-band state, so the relay arrives empty, or reads it before the runner
  # applied the confirmed inputs still on its stream, so they are lost.
  @tag :multi_node
  @tag :cold_drain_memory
  test "a cold drain carries the state of a class that keeps none on disk" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:cold_memory_node)
    relay_id = RelayFixture.start_on(peer_node, "cold_memory", "persistence_off_counter")
    for n <- ["3", "4"], do: confirm(relay_id, n)

    assert :ok = shutdown_peer(peer_node, 30_000)
    assert arrived?(relay_id), "the in-band state did not reach the destination's runner"
    assert runner_state(relay_id) =~ ~s("value":7)
    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # fails when: an effectively-once relay moved cold drops or applies twice a
  # record its confirming source sent around the move.
  @tag :multi_node
  @tag :cold_drain_effectively_once
  # The peer's boot budget (45 s) and the drain plus finalise (30 s + 15 s) exceed 60 s.
  @tag timeout: 120_000
  test "an effectively-once relay fed throughout a cold move applies every record once" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:cold_once_node)
    relay_id = RelayFixture.start_on(peer_node, "cold_once", "tier_store")

    RelayFixture.attach_output_msg({:ok, %{relay_id: relay_id}}, @default_namespace, %{
      include_metadata: true
    })

    feeder = start_feeder(relay_id, &"k#{&1}:v#{&1}")

    assert :ok = shutdown_peer(peer_node, 30_000)
    assert landed_here?(relay_id)
    sent = stop_feeder(feeder, 5)
    keys = Enum.map(sent, &(&1 |> String.split(":") |> hd()))

    assert NodeTestHelper.wait_until(
             fn ->
               keys -- Map.keys(TierE2EHarness.changelog(TierE2EHarness.peek_outputs())) == []
             end,
             30_000
           ),
           "a confirmed record never produced its output"

    twice =
      TierE2EHarness.drain_outputs()
      |> Enum.group_by(fn {_msg, md} -> md["flz.key"] end, fn {_msg, md} -> md end)
      |> Enum.filter(fn {_key, mds} -> length(Enum.uniq_by(mds, & &1["flz.seq"])) != 1 end)

    assert twice == [], "applied more than once: #{inspect(twice)}"
    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # Called from inside this node's hub as it starts `relay_id`, before the
  # Overwatch starts: whether the source still runs, and its writers still open.
  def report_arrival(child_data, test_pid, relay_id, peer, source) do
    if child_data.child_spec.id == relay_id do
      alive? = :erpc.call(peer, Process, :alive?, [source])
      writers = :erpc.call(peer, WriterRegistry, :live_writers, [relay_id])
      send(test_pid, {:arriving, relay_id, alive?, writers})
    end

    child_data
  catch
    kind, reason ->
      send(test_pid, {:arriving, relay_id, {kind, reason}, :unknown})
      child_data
  end

  defp watch_arrival(relay_id, peer, source) do
    hub_id = Identificator.hub(@default_namespace)

    handler = %HookManager{
      id: "arrival_watch",
      m: __MODULE__,
      f: :report_arrival,
      a: [:_, self(), relay_id, peer, source]
    }

    ProcessHub.register_hook_handlers(hub_id, Hook.child_data_alter(), [handler])

    on_exit(fn ->
      ProcessHub.cancel_hook_handlers(hub_id, Hook.child_data_alter(), ["arrival_watch"])
    end)
  end

  # Sends `input.(1)`, `input.(2)`, ... as a confirming source does, each
  # again until the relay takes it, until told to stop. Each send waits for its
  # answer, so one is sent again only when no relay could take it.
  defp start_feeder(relay_id, input), do: Task.async(fn -> feed(relay_id, input, 1, []) end)

  defp feed(relay_id, input, n, sent) do
    receive do
      {:stop_after, more} when more > 0 ->
        send(self(), {:stop_after, more - 1})
        confirm(relay_id, input.(n))
        feed(relay_id, input, n + 1, [input.(n) | sent])

      {:stop_after, 0} ->
        Enum.reverse(sent)
    after
      0 ->
        confirm(relay_id, input.(n))
        feed(relay_id, input, n + 1, [input.(n) | sent])
    end
  end

  # Stops the feeder once it sent `more` inputs after the move.
  defp stop_feeder(feeder, more) do
    send(feeder.pid, {:stop_after, more})
    Task.await(feeder, 60_000)
  end

  defp confirm(relay_id, input) do
    deadline = System.monotonic_time(:millisecond) + 30_000
    RelayTestHelper.send_confirmed(relay_id, @default_namespace, input, %{}, deadline)
  end

  defp landed_here?(relay_id) do
    NodeTestHelper.wait_until(
      fn ->
        match?(
          {:ok, pid} when node(pid) == node(),
          RelayManager.get_relay_pid(@default_namespace, relay_id)
        )
      end,
      10_000
    )
  end

  defp text_inputs(relay_id) do
    {:ok, json} = RelayManager.get_cell_state(@default_namespace, relay_id)
    get_in(Payloader.json_to_map(json), ["store_input_tasklet", "text_inputs"])
  end

  # The relay's state as its runner holds it, "" when unreadable: the
  # controller refuses a live read of a class that keeps nothing on disk.
  defp runner_state(relay_id) do
    case RelayManager.create_snapshot(@default_namespace, relay_id) do
      {:ok, snapshot} when is_binary(snapshot) -> snapshot
      _unreadable -> ""
    end
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
  defp shutdown_peer(peer_node, timeout_ms, listener \\ self()) do
    :erpc.call(peer_node, :telemetry_test, :attach_event_handlers, [
      listener,
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

  # The default namespace's drain outcome, sent once the drain returned. A
  # peer namespace with nothing to move answers for itself with migrated: 0.
  defp drain_progress(timeout_ms) do
    assert_receive {[:flezha, :shutdown, :drain_progress], _ref, m,
                    %{namespace: @default_namespace} = meta},
                   timeout_ms

    assert meta.refused == nil, "the drain was refused: #{inspect(meta.refused)}"
    m
  end

  # A relay on `peer_node` that defers every migration: it holds a correction
  # that never leaves its debounce window, so `migration_consent/1` answers
  # :defer. Each deferral reaches the test as `:deferred`. Placement can still
  # settle just after a start, so a relay that left the peer before holding
  # the signal is replaced by a new one.
  defp deferring_relay_on(peer_node) do
    hub_id = Identificator.hub(@default_namespace)
    NodeTestHelper.await_serving(peer_node, @default_namespace, @test_store_input_factory)

    :ok =
      :erpc.call(peer_node, Application, :put_env, [:flezha, :correction_debounce_ms, 600_000])

    :erpc.call(peer_node, ProcessHub, :register_hook_handlers, [
      hub_id,
      Hook.migration_deferred(),
      [
        %ProcessHub.Service.HookManager{
          id: "deferred_wait",
          m: Process,
          f: :send,
          a: [self(), :deferred, []]
        }
      ]
    ])

    Enum.find_value(1..5, fn attempt ->
      relay_id = start_relay_on_peer(peer_node, "drain_forced#{attempt}")
      RelayTestHelper.send_sync_io(relay_id, @default_namespace, "seed", %{})

      with {^relay_id, ow_pid} <- local_child(peer_node, hub_id, relay_id) do
        GenServer.cast(
          ow_pid,
          {:correction_request, {relay_id, "store_input_tasklet", "poke"}, 1}
        )

        # The cast is folded into state before the placement is read again.
        _ = :sys.get_state(ow_pid)
        if local_child(peer_node, hub_id, relay_id) == {relay_id, ow_pid}, do: relay_id
      end
    end) || flunk("no relay stayed on #{inspect(peer_node)} long enough to hold the signal")
  end

  defp local_child(peer_node, hub_id, relay_id) do
    :erpc.call(peer_node, ProcessHub, :process_list, [hub_id, :local])
    |> Enum.find(fn {id, _pid} -> id == relay_id end)
  end

  defp start_relay_on_peer(peer_node, prefix) do
    id = RelayFixture.start_on(peer_node, prefix, @test_store_input_factory)
    RelayFixture.attach_output_msg({:ok, %{relay_id: id}}, @default_namespace)
    id
  end
end
