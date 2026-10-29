defmodule Flezha.Process.BacklogAdmissionTest do
  @moduledoc """
  The one sink admission over every policy: `Backlog.dispatch/2` as a sink's
  dispatch calls it, and `offer/5`, `room/1` and `block_timeout/2` as a sink
  worker calls them. The test process stands in for the worker, so a block
  timer's message and a waiter's answer arrive here.
  """
  use ExUnit.Case, async: true

  # A 10 ms block timer or a fresh task's first message; a loaded machine can
  # deliver either past the 100 ms default.
  @arrives_within 5_000

  alias Flezha.Config.Durations
  alias Flezha.Process.Backlog

  defp sink(policy, opts \\ []),
    do: Backlog.new(:infinity, 2, [policy: policy, flag: Backlog.flag(), tag: :sink] ++ opts)

  defp fill(b) do
    Enum.reduce([:a, :b], b, fn entry, b ->
      {:queued, b, []} = Backlog.offer(b, entry, 1, :cast)
      b
    end)
  end

  defp waiter(kind) do
    ref = make_ref()
    {{kind, {self(), ref}}, ref}
  end

  defp up?(b), do: :atomics.get(b.flag, 1) > 0

  defp drain_one(b) do
    {_entry, b} = Backlog.out(b)
    Backlog.room(b)
  end

  test "a dispatch casts while the flag is down and waits in a call while it is up" do
    registry = :"#{__MODULE__}.#{System.unique_integer([:positive])}"
    start_supervised!({Registry, keys: :unique, name: registry})
    flag = Backlog.flag()
    {:ok, _} = Registry.register(registry, :sink, flag)
    via = {:via, Registry, {registry, :sink}}

    assert :ok = Backlog.dispatch(via, :first)
    assert_received {:"$gen_cast", :first}

    :atomics.add(flag, 1, 1)
    task = Task.async(fn -> Backlog.dispatch(via, :second) end)
    assert_receive {:"$gen_call", from, :second}, @arrives_within
    GenServer.reply(from, :ok)
    assert :ok = Task.await(task)

    assert :none = Backlog.dispatch({:via, Registry, {registry, :gone}}, :third)
  end

  # Fails when: a dispatch waits on a worker that never answers with no bound.
  test "a waiting dispatch gives up at the block timeout plus the headroom" do
    registry = :"#{__MODULE__}.#{System.unique_integer([:positive])}"
    start_supervised!({Registry, keys: :unique, name: registry})
    flag = Backlog.flag()
    _queue = Backlog.new(:infinity, 2, flag: flag, block_ms: 10)
    {:ok, _} = Registry.register(registry, :sink, flag)
    :atomics.add(flag, 1, 1)

    task = Task.async(fn -> Backlog.dispatch({:via, Registry, {registry, :sink}}, :stuck) end)
    assert_receive {:"$gen_call", _from, :stuck}, @arrives_within
    bound = 10 + Durations.get(:backlog_wait_headroom_ms)
    assert {:ok, :ok} = Task.yield(task, bound * 5)
  end

  # Fails when: an entry larger than the byte bound waits for room an empty
  # queue can never give it, and the waiters behind it wait too.
  test "a waiter larger than the byte bound is admitted once the queue is empty" do
    b = Backlog.new(10, 4, policy: :drop_nothing, flag: Backlog.flag(), tag: :sink)
    {:queued, b, []} = Backlog.offer(b, :small, 8, :cast)
    {big, big_ref} = waiter(:wait)
    {next, next_ref} = waiter(:wait)

    assert {:waiting, b, []} = Backlog.offer(b, :big, 20, big)
    assert {:waiting, b, []} = Backlog.offer(b, :next, 3, next)

    b = drain_one(b)
    assert_received {^big_ref, :ok}
    refute_received {^next_ref, _}
    assert Backlog.to_list(b) == [:big] and up?(b)

    b = drain_one(b)
    assert_received {^next_ref, :ok}
    assert Backlog.to_list(b) == [:next]
    refute up?(b)

    {:next, empty} = Backlog.out(b)
    {late, late_ref} = waiter(:wait)
    assert {:queued, b, []} = Backlog.offer(empty, :late, 20, late)
    assert_received {^late_ref, :ok}
    assert Backlog.to_list(b) == [:late] and up?(b)
  end

  # Fails when: the admission never makes a dispatch wait.
  test "drop_nothing holds a dispatch waiter, then lets it go at the block timeout, overflowed" do
    b = fill(sink(:drop_nothing, block_ms: 10))
    {wait, ref} = waiter(:wait)

    assert {:waiting, b, []} = Backlog.offer(b, :late, 1, wait)
    assert up?(b) and Backlog.waiting(b) == 1
    refute_received {^ref, _}

    assert_receive {:block_timeout, :sink, ^wait}, @arrives_within
    assert {b, [:late]} = Backlog.block_timeout(b, wait)
    assert_received {^ref, :ok}
    assert Backlog.to_list(b) == [:a, :b] and Backlog.waiting(b) == 0
  end

  test "drop_nothing with no block timeout holds the caller until room is made" do
    b = fill(sink(:drop_nothing))
    {wait, ref} = waiter(:wait)

    assert {:waiting, b, []} = Backlog.offer(b, :late, 1, wait)
    assert b.timers == %{}
    refute_received {^ref, _}

    b = drain_one(b)
    assert_received {^ref, :ok}
    assert Backlog.to_list(b) == [:b, :late] and up?(b)

    refute up?(drain_one(b))
  end

  test "drop_oldest evicts the oldest entry the holder names droppable, and refuses past them" do
    b = Backlog.new(:infinity, 2, policy: :drop_oldest)
    plain? = &(not &1.guaranteed)

    {:queued, b, []} = Backlog.offer(b, %{id: 1, guaranteed: true}, 1, :cast, plain?)
    {:queued, b, []} = Backlog.offer(b, %{id: 2, guaranteed: false}, 1, :cast, plain?)

    assert {:queued, b, [%{id: 2}]} =
             Backlog.offer(b, %{id: 3, guaranteed: true}, 1, :cast, plain?)

    assert {:refused, b, []} = Backlog.offer(b, %{id: 4, guaranteed: false}, 1, :cast, plain?)
    assert Enum.map(Backlog.to_list(b), & &1.id) == [1, 3]
  end

  test "drop_newest refuses the arrival, answering a synchronous caller and letting a dispatch go" do
    b = fill(sink(:drop_newest))
    {call, call_ref} = waiter(:call)
    {wait, wait_ref} = waiter(:wait)

    assert {:refused, b, []} = Backlog.offer(b, :called, 1, call)
    assert_received {^call_ref, {:error, {:queue_full, :overflow}}}
    assert {:refused, b, []} = Backlog.offer(b, :waited, 1, wait)
    assert_received {^wait_ref, :ok}
    assert Backlog.to_list(b) == [:a, :b]
    refute up?(b)
  end

  test "a cast on its way when the queue filled is admitted past the bound and raises the flag" do
    b = fill(sink(:drop_nothing))
    refute up?(b)

    assert {:queued, b, []} = Backlog.offer(b, :on_its_way, 1, :cast)
    assert b.frames == 3 and up?(b)

    b = drain_one(b)
    assert up?(b)
    refute up?(drain_one(b))
  end

  test "at the block timeout a synchronous caller is answered queue_full and a dispatch's entry overflows" do
    b = fill(sink(:drop_nothing, block_ms: 10))
    {call, call_ref} = waiter(:call)
    {wait, wait_ref} = waiter(:wait)
    {:waiting, b, []} = Backlog.offer(b, :called, 1, call)
    {:waiting, b, []} = Backlog.offer(b, :waited, 1, wait)

    assert_receive {:block_timeout, :sink, ^call}, @arrives_within
    assert {b, []} = Backlog.block_timeout(b, call)
    assert_received {^call_ref, {:error, {:queue_full, :timeout}}}

    assert_receive {:block_timeout, :sink, ^wait}, @arrives_within
    assert {_b, [:waited]} = Backlog.block_timeout(b, wait)
    assert_received {^wait_ref, :ok}
  end

  # Fails when: a queue lowers the flag it shares on any drain.
  test "with two queues sharing a flag, one draining while the other is full leaves the flag up" do
    flag = Backlog.flag()
    past_bound = &(&1 |> fill() |> Backlog.offer(:on_its_way, 1, :cast) |> elem(1))
    full = past_bound.(Backlog.new(:infinity, 2, flag: flag))
    draining = past_bound.(Backlog.new(:infinity, 2, flag: flag))

    drained = draining |> drain_one() |> drain_one()
    refute drained.full
    assert full.full and :atomics.get(flag, 1) > 0

    refute drain_one(drain_one(full)).full
    assert :atomics.get(flag, 1) == 0
  end
end
