defmodule Flezha.SDK.EffectivelyOnceTest do
  @moduledoc """
  The effectively-once tier in the SDK's own core: the one effect record an
  effectful invocation ships in place of the change records and the outputs,
  the consumed offset it carries and the floor a restore rebuilds. No
  controller, no real transport — frames are read off a native `LocalStream`
  the test owns.
  """
  use ExUnit.Case, async: true

  import FlezhaTest.Helper.SdkOperatorHarness

  alias Flezha.SDK.{Counters, Cell, Record}
  alias Flezha.SDK.Operator.Inbox

  alias FlezhaTest.Helper.SdkTierFixtures.{
    CheckpointBatchedOp,
    CheckpointEveryOp,
    CheckpointHeldOp,
    CheckpointStoreOp,
    StatelessTierOp,
    TierEdgeOp,
    TierStoreOp,
    TierTimerOp
  }

  @task "tier_store_tasklet"

  defp push(op, input, offset),
    do: Inbox.input(op, "input", input, %{"flz.offset" => Integer.to_string(offset)})

  test "an effectful invocation ships ONE effect record carrying its change, its output and its offset" do
    %{op: op} = start_operator(TierStoreOp)
    push(op, "k1=a", 3)

    assert [first] = frames(1)
    assert {0, body} = effect(first)
    assert body.changes == [Record.encode_body({:set, "k1", "a"})]
    assert body.outputs == [%{channel: "sink", key: "k1", metadata: [], payload: "k1=a"}]
    assert body.consumed_offset == 3
    assert body.dlq == nil
    assert body.timers == []

    now = System.system_time(:millisecond)
    assert_in_delta body.event_time, now, 60_000

    # The next frame on the stream is the NEXT dispatch's record: no
    # `cell_state_change` and no output frame left between the two.
    push(op, "k2=b", 4)
    assert [second] = frames(1)
    assert {1, %{consumed_offset: 4, changes: [_], outputs: [_]}} = effect(second)
  end

  test "an input at or below the restored floor is a completed dispatch, and the one above it runs" do
    %{op: op, counters: counters} = start_operator(TierStoreOp)
    pid = cell!(op, @task)

    log =
      effect_record(0, %{
        changes: [Record.encode_body({:set, "k0", "seed"})],
        outputs: [],
        consumed_offset: 5,
        dlq: nil,
        event_time: 7,
        timers: []
      })

    assert {:ok, 0, false} = Cell.restore(pid, <<>>, <<>>, log, 0)
    assert {:ok, %{"k0" => "seed"}, 1} = Cell.read(pid, 1_000)
    before = Counters.dispatches(counters)

    # 4 is at or below the floor the restored record made durable: the
    # controller re-forwarded it, and it is a completed dispatch, not a re-run.
    push(op, "k4=x", 4)
    push(op, "k6=y", 6)

    assert [frame] = frames(1)
    assert {1, %{consumed_offset: 6, changes: [_]}} = effect(frame)
    assert {:ok, %{"k6" => "y"} = state, 2} = Cell.read(pid, 1_000)
    refute Map.has_key?(state, "k4")
    assert Counters.dispatches(counters) - before == 2
  end

  test "a stateless tasklet under the tier ships its output and its offset in a record" do
    %{op: op} = start_operator(StatelessTierOp)
    push(op, "k1=a", 5)

    # The router delivers no sink for a tier class — the record is the output
    # path — so a class that held no state would lose the output entirely.
    assert [only] = frames(1)
    assert {0, body} = effect(only)
    assert body.changes == []
    assert body.outputs == [%{channel: "sink", key: "k1", metadata: [], payload: "k1=a"}]
    assert body.consumed_offset == 5
  end

  test "a mid-interval emit acknowledges nothing, and the checkpoint ships the interval" do
    %{op: op} = start_operator(CheckpointStoreOp)
    push(op, "k1=a", 1)
    push(op, "k2=b", 2)

    # As-produced: each output leaves in a record that carries neither the
    # change it came from nor an offset — the state is not durable yet.
    assert [f0, f1] = frames(2)
    assert {0, %{changes: [], consumed_offset: nil, outputs: [%{payload: "k1=a"}]}} = effect(f0)
    assert {1, %{changes: [], consumed_offset: nil, outputs: [%{payload: "k2=b"}]}} = effect(f1)

    push(op, "commit:now", 3)
    assert [f2] = frames(1)
    assert {2, body} = effect(f2)

    assert body.changes == [
             Record.encode_body({:set, "k1", "a"}),
             Record.encode_body({:set, "k2", "b"})
           ]

    assert body.consumed_offset == 3
    assert body.outputs == []
  end

  test "checkpoint_every closes on the nth input, carrying the interval and its last offset" do
    %{op: op} = start_operator(CheckpointEveryOp)
    for {input, offset} <- [{"k1=a", 1}, {"k2=b", 2}], do: push(op, input, offset)

    assert [f0, f1] = frames(2)
    assert {0, %{changes: [], consumed_offset: nil}} = effect(f0)
    assert {1, %{changes: [], consumed_offset: nil}} = effect(f1)

    push(op, "k3=c", 3)
    assert [f2] = frames(1)
    assert {2, body} = effect(f2)
    assert length(body.changes) == 3
    assert body.consumed_offset == 3
    assert Enum.map(body.outputs, & &1.payload) == ["k3=c"]
  end

  test "held outputs ride the closing record in production order under one seq" do
    %{op: op} = start_operator(CheckpointBatchedOp)
    for {input, offset} <- [{"k1=a", 1}, {"k2=b", 2}, {"k3=c", 3}], do: push(op, input, offset)
    push(op, "commit:now", 4)

    assert [only] = frames(1)
    assert {0, body} = effect(only)
    assert Enum.map(body.outputs, & &1.payload) == ["k1=a", "k2=b", "k3=c"]
    assert length(body.changes) == 3
    assert body.consumed_offset == 4
  end

  test "the delay closes a quiet interval, and a close that already happened leaves nothing to do" do
    %{op: op} = start_operator(CheckpointHeldOp)
    push(op, "k1=a", 1)

    # Nothing ships on the invocation: the deadline is what closes it.
    assert [closed] = frames(1)
    assert {0, %{consumed_offset: 1, outputs: [%{payload: "k1=a"}]}} = effect(closed)

    # An explicit close beats the deadline; the armed message then finds
    # nothing open, so the next frame is the NEXT interval's, at seq 2.
    push(op, "k2=b", 2)
    push(op, "commit:now", 3)
    assert [committed] = frames(1)
    assert {1, %{consumed_offset: 3, outputs: [%{payload: "k2=b"}]}} = effect(committed)

    push(op, "k4=d", 4)
    assert [next] = frames(1)
    assert {2, %{consumed_offset: 4, outputs: [%{payload: "k4=d"}]}} = effect(next)
  end

  test "a poison input parks inside the record, with no park_payload frame and no ack to wait for" do
    %{op: op} = start_operator(TierStoreOp, runner: self())
    push(op, "poison", 7)

    assert [parked] = frames(1)
    assert {0, body} = effect(parked)
    assert body.changes == []
    assert body.consumed_offset == 7
    assert %{channel: "input", error: "poison value", attempts: 1, payload: "poison"} = body.dlq

    # The queued input dispatches: the tier park holds nothing, so no
    # `ParkPayloadAck` has to come back first.
    push(op, "k1=a", 8)
    assert [after_park] = frames(1)
    assert {1, %{consumed_offset: 8, changes: [_]}} = effect(after_park)
  end

  test "a firing ships one record with its change, its output and its own removal" do
    %{op: op} = start_operator(TierTimerOp)
    push(op, "arm:0", 1)

    # The registration's record, then the firing's — the removal rides the
    # firing's own record, and no format-3 timer-ops record is written.
    assert [armed] = frames(1)

    assert {0, %{changes: [], consumed_offset: 1, timers: [%{register: true} = op_registered]}} =
             effect(armed)

    assert [fired] = frames(1)
    assert {1, body} = effect(fired)
    assert body.consumed_offset == nil
    assert body.changes == [Record.encode_body({:set, "fired", op_registered.scheduled_at})]
    assert [%{key: "fired"}] = body.outputs
    assert [%{register: false, scheduled_at: at}] = body.timers
    assert at == op_registered.scheduled_at
  end

  test "a restore discards the staged interval, and a replacement re-holds it from the slot" do
    %{op: op, counters: counters} = start_operator(CheckpointStoreOp)
    pid = cell!(op, @task)
    push(op, "k1=a", 1)
    push(op, "k2=b", 2)
    assert [_, _] = frames(2)
    before = Counters.restore_discarded_inputs(counters)

    # The two changes are staged, not durable: a restore re-seats the Cell
    # on a past in which neither was applied.
    assert {:ok, 0, false} = Cell.restore(pid, <<>>, <<>>, <<>>, 0)

    # And the two inputs it threw away are reported, under the key every other
    # runtime carries: the controller re-forwards them, but a user has to
    # be able to see that they went.
    assert Counters.restore_discarded_inputs(counters) - before == 2
    assert {:ok, %{}, 1} = Cell.read(pid, 1_000)

    push(op, "commit:now", 3)
    assert [after_restore] = frames(1)
    assert {1, %{changes: [], consumed_offset: 3}} = effect(after_restore)
  end

  # The staged inputs are still held, so no replacement is minted over them:
  # the Operator ends, and the controller's rebind re-forwards them.
  test "an untrappable kill with an interval staged ends the Operator and ships nothing" do
    Process.flag(:trap_exit, true)
    %{op: op} = start_operator(CheckpointBatchedOp)
    pid = cell!(op, @task)
    push(op, "k1=a", 1)
    push(op, "k2=b", 2)
    assert {:ok, _state, 0} = Cell.read(pid, 1_000)

    ExUnit.CaptureLog.capture_log(fn ->
      push(op, "kill", 3)
      assert_receive {:EXIT, ^op, {:cell_exited, :killed}}, 2_000
    end)

    refute_receive {:flz_data, _, "cell_effect_record", _, _}, 100
  end

  test "an inter-tasklet output holds its record until the consumer admits the push" do
    %{op: op} = start_operator(TierEdgeOp)
    producer = cell!(op, "edge_producer")

    # This one's push was admitted before the consumer went into `run/3`, so
    # its record ships; the consumer is inside `run/3` from here on.
    push(op, "hold=1", 1)
    assert [f0] = frames(1)
    assert {0, %{consumed_offset: 1}} = effect(f0)

    consumer = cell!(op, "held_consumer")
    assert {:error, :busy} = Cell.read(consumer, 200)

    # The consumer's one queue slot is free while it runs, so this push is
    # admitted at once, and its record ships.
    push(op, "k1=a", 2)
    assert [f1] = frames(1)
    assert {1, %{consumed_offset: 2}} = effect(f1)

    # Queued into the producer directly: the router is exercised for the EMIT,
    # and a synchronous enqueue then read is what makes "the settle has
    # happened" a fact rather than a wait. The consumer's queue is full now,
    # so the router is inside a push it will not admit, and this offset may
    # not become durable ahead of the output it produced.
    :ok = Cell.enqueue_block(producer, %Cell.Input{value: "k2=b", offset: 3})
    assert {:ok, %{"k2" => "b"}, 3} = Cell.read(producer, 1_000)
    refute_received {:flz_data, _, "cell_effect_record", _, _}

    send(consumer, :release)
    assert [f2] = frames(1)
    assert {2, %{consumed_offset: 3}} = effect(f2)
  end
end
