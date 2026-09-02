defmodule Flezha.SDK.EntityTimersTest do
  @moduledoc """
  The Entity fires: a processing-time timer off the BEAM's timer service
  through its own queue, an event-time timer at admission ahead of the input
  that crossed it, a firing through the same ladder and park as an input, the
  pending set rebuilt by a restore and by a replacement, discarded by an
  eviction, bounded by the cap, and counted — each read off the harness's
  stream, with no controller behind it.
  """
  use ExUnit.Case, async: true

  import FlezhaTest.Helper.SdkOperativeHarness

  alias Flezha.SDK.{Counters, Entity, Operative, Timers}
  alias Flezha.SDK.Operative.Inbox

  alias FlezhaTest.Helper.SdkPersistenceFixtures.{
    CancelOp,
    CappedTimersOp,
    EventTimerOp,
    GatedTimerOp,
    KillableTimerOp,
    NoHandlerOp,
    PoisonTimerOp,
    TimerOp,
    TtlTimerOp
  }

  setup do
    id = "timers-#{System.unique_integer([:positive])}"
    pid = self()

    :telemetry.attach(
      id,
      [:flezha, :sdk, :timers],
      fn _event, measurements, meta, _ -> send(pid, {:timers, measurements, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
    :ok
  end

  test "a processing-time timer fires with no input: on_timer's change, its removal and its output leave in order, now equal to the scheduled time" do
    %{op: op, counters: counters} = start_operative(TimerOp)
    Inbox.input(op, "in", "0", %{})
    [r0, r1, out] = frames(3)
    assert {0, {:delta, 0}, _} = record(r0)
    assert {1, [%{register: true, scheduled_at: at}], _} = timer_ops(r1)
    assert {"op-1", "0", _} = out
    assert_receive {:timers, %{count: 1}, %{event: :registered, procedure: "timer"}}, 1_000

    [r2, r3, fired] = frames(3)
    assert {2, {:fired, ^at}, _} = record(r2)
    assert {3, [%{kind: :processing_time, register: false, scheduled_at: ^at}], _} = timer_ops(r3)
    expected = "fired@#{at}@#{at}"
    assert {"op-1", ^expected, _} = fired
    assert {:ok, %{value: 0, fired: [^at]}, 4} = Entity.read(entity!(op, "timer"), 1_000)
    assert_receive {:timers, %{count: 1}, %{event: :fired, procedure: "timer"}}, 1_000
    assert Counters.timers_fired(counters) == 1
    assert Counters.timers_armed(counters) == 0
  end

  test "event-time timers fire ascending before the input that crossed the high-water; a quiet Entity fires nothing" do
    %{op: op} = start_operative(EventTimerOp)
    for input <- ~w(arm:100 arm:200 arm:300 50), do: Inbox.input(op, "in", input, %{})
    assert [{"op-1", "tick@50", _}] = frames(1)

    Inbox.input(op, "in", "250", %{})

    assert for({"op-1", out, _} <- frames(3), do: out) ==
             ["fired@100:event_time:100", "fired@200:event_time:200", "tick@250"]

    Inbox.input(op, "in", "260", %{})
    assert [{"op-1", "tick@260", _}] = frames(1)
    pending = :sys.get_state(entity!(op, "event_timer")).timers.pending
    assert Timers.all(pending, :event_time) == [300]
  end

  test "a firing holds the Entity: an input pushed while on_timer runs is dispatched after it" do
    Process.register(self(), :timer_gate_probe)
    %{op: op} = start_operative(GatedTimerOp)
    pid = entity!(op, "gated_timer")
    Inbox.input(op, "in", "arm", %{})
    assert [{"op-1", "armed", _}] = frames(1)
    assert_receive :firing, 2_000

    Inbox.input(op, "in", "b", %{})
    send(pid, :go)
    assert for({"op-1", out, _} <- frames(2), do: out) == ["fired", "b"]
  end

  test "a poison firing parks once with its removal committed, and does not fire again after release" do
    %{op: op} = start_operative(PoisonTimerOp)
    Inbox.input(op, "in", "arm", %{})
    [_r0, r1] = frames(2)
    assert {1, [%{scheduled_at: at}], _} = timer_ops(r1)

    [r2] = frames(1)
    assert {2, [%{register: false, scheduled_at: ^at}], _} = timer_ops(r2)

    assert_receive {:"$gen_cast", {:park, pid, _identity, %{error: "poison firing"} = park}},
                   2_000

    Entity.release_park(pid, park.park_id)
    Inbox.input(op, "in", "x", %{})
    assert [{"op-1", "x", _}] = frames(1)
    assert Timers.empty?(:sys.get_state(pid).timers.pending)
  end

  test "a Procedure without on_timer/4 parks the firing naming the procedure and the callback" do
    %{op: op} = start_operative(NoHandlerOp)
    Inbox.input(op, "in", "arm", %{})
    assert_receive {:"$gen_cast", {:park, _pid, _identity, %{error: error, attempts: 1}}}, 2_000
    assert error == "procedure `no_handler` registered a timer but does not export on_timer/4"
  end

  test "a restore over a log with overdue timers fires them at once, ascending" do
    %{op: op} = start_operative(TimerOp)
    pid = entity!(op, "timer")

    log =
      timer_ops_record(0, [
        Timers.register_op(:processing_time, 200),
        Timers.register_op(:processing_time, 100)
      ])

    assert {:ok, 0, false} = Entity.restore(pid, <<>>, <<>>, log, 0)
    assert for({"op-1", out, _} <- frames(6), do: out) == ["fired@100@100", "fired@200@200"]
    assert {:ok, %{value: 0, fired: [100, 200]}, 5} = Entity.read(pid, 1_000)
  end

  test "a cancelled timer is disarmed, and a stale due message fires nothing" do
    %{op: op, counters: counters} = start_operative(CancelOp)
    pid = entity!(op, "cancel")
    Inbox.input(op, "in", "arm", %{})
    [_r0, r1] = frames(2)
    assert {1, [%{scheduled_at: at}], _} = timer_ops(r1)
    # The records leave before the arm; the arm's own report is the settle.
    assert_receive {:timers, %{count: 1}, %{event: :registered}}, 1_000
    assert Counters.timers_armed(counters) == 1

    Inbox.input(op, "in", "cancel", %{})
    [r2, out] = frames(2)
    assert {2, [%{register: false, scheduled_at: ^at}], _} = timer_ops(r2)
    assert {"op-1", "cancelled:true", _} = out
    assert_receive {:timers, %{count: 1}, %{event: :cancelled}}, 1_000
    assert Counters.timers_armed(counters) == 0

    send(pid, {:timer_due, at})
    Inbox.input(op, "in", "after", %{})
    assert [{"op-1", "after", _}] = frames(1)
  end

  test "a replacement after an untrappable kill re-arms from the slot and fires" do
    %{op: op} = start_operative(KillableTimerOp)
    Inbox.input(op, "in", "arm", %{})
    [r0] = frames(1)
    assert {0, [%{scheduled_at: at}], _} = timer_ops(r0)
    first = kill!(op, "killable_timer")

    [r1, r2, out] = frames(3)
    assert {1, {:fired, ^at}, _} = record(r1)
    assert {2, [%{register: false, scheduled_at: ^at}], _} = timer_ops(r2)
    expected = "fired@#{at}"
    assert {"op-1", ^expected, _} = out
    assert entity!(op, "killable_timer") != first
  end

  test "an evicted Entity's timer goes with it, and its successor starts with none" do
    %{op: op, counters: counters} = start_operative(TtlTimerOp)
    Inbox.input(op, "in", "k1:arm", %{})
    [_r0, _r1] = frames(2)
    assert_receive {:timers, %{count: 1}, %{event: :registered}}, 1_000
    assert Counters.timers_armed(counters) == 1
    {:ok, pid} = Operative.lookup(op, "ttl_timer", "k1")
    assert_evicted(pid, {"op-1", "ttl_timer", "k1"})
    assert Counters.timers_armed(counters) == 0

    Inbox.input(op, "in", "k1:again", %{})
    [_r0, r1] = frames(2)
    assert {1, [%{register: true}], _} = timer_ops(r1)
    {:ok, fresh} = Operative.lookup(op, "ttl_timer", "k1")
    assert Timers.size(:sys.get_state(fresh).timers.pending) == 1
  end

  test "past the cap a registration is refused naming the procedure and the bound, reported, with the set unchanged" do
    %{op: op, counters: counters} = start_operative(CappedTimersOp)
    Inbox.input(op, "in", "1", %{})
    assert [{"op-1", "{:ok, " <> _, _}] = frames(1)
    Inbox.input(op, "in", "2", %{})
    assert [{"op-1", ~s({:error, {:timer_cap_exceeded, "capped_timers", 1}}), _}] = frames(1)
    assert_receive {:timers, %{count: 1}, %{event: :refused, procedure: "capped_timers"}}, 1_000
    assert Counters.timers_armed(counters) == 1
    assert Timers.size(:sys.get_state(entity!(op, "capped_timers")).timers.pending) == 1
  end
end
