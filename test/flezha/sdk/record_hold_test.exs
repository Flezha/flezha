defmodule Flezha.SDK.RecordHoldTest do
  @moduledoc """
  The Entity's record path when no stream will take a record: it is held and
  replayed on the next bound one, in `change_seq` order, still ahead of the
  outputs of its own dispatch — and the credit window is debited by the write
  rather than by the hold.
  """
  # async: false: the cap test writes `FLEZHA_RECONNECT_RING_CAPACITY`, which is
  # VM-global — an async writer of it races every other holder's reading of it.
  use ExUnit.Case, async: false

  import FlezhaTest.Helper.SdkOperativeHarness

  alias Flezha.SDK.{Counters, Credit, Entity, Record}
  alias Flezha.SDK.Operative.Inbox
  alias Flezha.State.Wire
  alias FlezhaTest.Helper.SdkOperativeHarness
  alias FlezhaTest.Helper.SdkPersistenceFixtures.{CounterOp, TimerOp}

  # One input into `CounterOp` is one dispatch that writes a change record and
  # then a sink output — the two frames whose order the spec governs.
  defp feed(op, n), do: Inbox.input(op, "in", Integer.to_string(n), %{})

  defp kinds(n), do: n |> frames() |> Enum.map(&kind/1)

  # Every dispatch is finished and every output already cast to the router: a
  # read answers only behind the queued inputs, and a cast is in the router's
  # mailbox before the dispatch that sent it returns. Without it a rebind can
  # overtake the dispatches it is meant to follow.
  defp settled(op, procedure, seq),
    do: assert({:ok, _state, ^seq} = Entity.read(entity!(op, procedure), 1_000))

  # A secondary this process owns whose stream is already gone: what a rebind
  # onto a wire that died again hands the Entity.
  defp dead_secondary do
    {sdk, _ctrl} = SdkOperativeHarness.own_secondary()
    :ok = close_secondary(sdk)
    sdk
  end

  describe "a record no stream would take" do
    test "is held and replayed when the write failed on a dead stream" do
      %{op: op, sdk_secondary: sdk} = start_operative(CounterOp)
      :ok = close_secondary(sdk)

      feed(op, 3)
      rebind_secondary(op)

      assert [{:change, 0, {:delta, 3}}, {:output, "op-1", "3", _}] = kinds(2)
    end

    test "is held and replayed when there was no stream at all" do
      %{op: op} = start_operative(CounterOp)
      send(op, {:secondaries, []})

      feed(op, 5)
      rebind_secondary(op)

      assert [{:change, 0, {:delta, 5}}, {:output, "op-1", "5", _}] = kinds(2)
    end

    test "keeps its dispatch's timer-ops record behind its change, in seq order" do
      %{op: op} = start_operative(TimerOp)
      send(op, {:secondaries, []})

      feed(op, 1)
      rebind_secondary(op)

      assert [{:change, 0, {:delta, 1}}, {:timer_ops, 1, [%{register: true}]}, {:output, _, _, _}] =
               kinds(3)
    end
  end

  test "a replayed output never arrives without the record of the change that produced it" do
    %{op: op, sdk_secondary: sdk} = start_operative(CounterOp)
    :ok = close_secondary(sdk)

    for n <- 1..3, do: feed(op, n)
    settled(op, "counter", 3)
    rebind_secondary(op)

    # Each hold drains as a run, so the records arrive as a block ahead of the
    # outputs; what the spec governs is that no dispatch's output precedes its
    # own record, and that is what this order says.
    assert [
             {:change, 0, {:delta, 1}},
             {:change, 1, {:delta, 2}},
             {:change, 2, {:delta, 3}},
             {:output, "op-1", "1", _},
             {:output, "op-1", "3", _},
             {:output, "op-1", "6", _}
           ] = kinds(6)
  end

  test "the hold is bounded, and what it sheds is counted where every drop-oldest is" do
    System.put_env("FLEZHA_RECONNECT_RING_CAPACITY", "2")
    on_exit(fn -> System.delete_env("FLEZHA_RECONNECT_RING_CAPACITY") end)

    %{op: op, counters: counters} = start_operative(CounterOp)
    send(op, {:secondaries, []})
    for n <- 1..5, do: feed(op, n)
    settled(op, "counter", 5)

    rebind_secondary(op)

    # Drop-oldest: the last two records survive, and the three shed are counted
    # rather than lost in silence. The Operative's own hold sheds three outputs
    # at the same cap, so the counter carries six.
    assert [{:change, 3, {:delta, 4}}, {:change, 4, {:delta, 5}} | _] = kinds(2)
    assert Counters.drops_oldest(counters) == 6
  end

  test "a replay onto a stream that is gone again re-holds rather than dropping" do
    %{op: op, counters: counters} = start_operative(CounterOp)
    send(op, {:secondaries, []})
    feed(op, 4)

    # The rebind lands on a wire that is already dead: every replayed write
    # fails and every frame goes back into the hold, none of it shed.
    send(op, {:secondaries, [dead_secondary()]})
    rebind_secondary(op)

    assert [{:change, 0, {:delta, 4}}, {:output, "op-1", "4", _}] = kinds(2)
    assert Counters.drops_oldest(counters) == 0
  end

  test "the credit window is debited by the write, never by the hold" do
    counters = Counters.new()
    {:ok, credit} = Credit.start_link(counters: counters)
    %{op: op} = start_operative(CounterOp, credit: credit)

    # A window one byte wide: a record that debited on the way into the hold
    # would park the Entity here, waiting on a controller that is not there.
    Credit.arm(credit, "session-1", 1)
    assert Credit.session(credit) == "session-1"

    send(op, {:secondaries, []})
    for n <- 1..3, do: feed(op, n)
    assert :ok = Entity.rebind(entity!(op, "counter"), [])
    assert Counters.credit_parked(counters) == 0

    # A reconnect disarms the session before it re-establishes the sockets, so
    # the replay's own debit belongs to the next window, never to this one twice.
    Credit.disarm(credit)
    rebind_secondary(op)

    assert [{:change, 0, _}, {:change, 1, _}, {:change, 2, _} | _] = kinds(4)
  end

  test "compaction still ships the snapshot below its re-stated timers after a replay" do
    %{op: op} = start_operative(TimerOp)
    send(op, {:secondaries, []})

    feed(op, 1)
    rebind_secondary(op)
    assert [{:change, 0, _}, {:timer_ops, 1, _}, {:output, _, _, _}] = kinds(3)

    pid = entity!(op, "timer")
    assert {:ok, 1, _size} = Entity.snapshot(pid)
    [{"entity_state_snapshot", frame, _}, restated] = frames(2)

    # The snapshot folds every seq the replay put on the wire — `change_seq - 1`
    # — and the pending timers are re-stated above it, exactly as they are when
    # nothing was ever held.
    assert {:ok, %{seq: 1, body: body}} = Wire.decode_frame(frame)
    assert {:ok, %{value: 1, fired: []}} = Record.decode_body(body)
    assert {2, [%{kind: :processing_time, register: true}], _} = timer_ops(restated)
  end

  test "an Entity evicted while holding leaves no queue behind and no wedged router" do
    %{op: op} = start_operative(CounterOp)
    send(op, {:secondaries, []})
    feed(op, 2)

    entity = entity!(op, "counter")
    ref = Process.monitor(entity)
    assert Entity.evict(entity) == :evicted
    assert_receive {:DOWN, ^ref, :process, ^entity, :normal}, 1_000

    # The hold goes with the process — the Operative's own output for that
    # dispatch outlives it. What must survive is the router: the replacement
    # Entity binds to the fresh stream and ships its own records.
    rebind_secondary(op)
    feed(op, 7)

    assert [
             {:output, "op-1", "2", _},
             {:change, 0, {:delta, 7}},
             {:output, "op-1", "7", _}
           ] = kinds(3)
  end
end
