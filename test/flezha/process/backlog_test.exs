defmodule Flezha.Process.BacklogTest do
  @moduledoc """
  The full-queue policy every holder of a `Flezha.Process.Backlog` decides
  with, and the reads a holder that sends in batches needs: the head without
  taking it, everything held, and entries put back at the head.
  """
  use ExUnit.Case, async: true

  alias Flezha.Process.Backlog

  # Two entries of at most ten bytes in all.
  defp two(), do: Backlog.new(10, 2) |> Backlog.push(:a, 4) |> Backlog.push(:b, 4)

  defp entries(backlog), do: Backlog.to_list(backlog)

  test "an entry that fits is queued under every policy" do
    for policy <- [:drop_nothing, :drop_newest, :drop_oldest] do
      assert {:queued, backlog, []} = Backlog.admit(Backlog.new(10, 2), :a, 4, policy)
      assert entries(backlog) == [:a]
    end
  end

  test "drop_nothing answers full and keeps the queue as it was" do
    assert {:full, backlog} = Backlog.admit(two(), :c, 1, :drop_nothing)
    assert entries(backlog) == [:a, :b]
  end

  test "drop_newest refuses the entry and drops nothing" do
    assert {:refused, backlog, []} = Backlog.admit(two(), :c, 1, :drop_newest)
    assert entries(backlog) == [:a, :b]
  end

  test "drop_oldest drops the oldest entries until the new one fits, oldest first" do
    backlog = Backlog.new(10, 3) |> Backlog.push(:a, 4) |> Backlog.push(:b, 4)
    assert {:queued, backlog, [:a, :b]} = Backlog.admit(backlog, :c, 9, :drop_oldest)
    assert entries(backlog) == [:c]
    assert %{bytes: 9, frames: 1} = backlog
  end

  test "drop_oldest drops only what the holder names droppable, and refuses past them" do
    kept = &(&1 != :a)
    assert {:queued, backlog, [:b]} = Backlog.admit(two(), :c, 4, :drop_oldest, kept)
    assert entries(backlog) == [:a, :c]

    assert {:refused, backlog, [:c]} = Backlog.admit(backlog, :d, 8, :drop_oldest, kept)
    assert entries(backlog) == [:a]
  end

  test "drop_oldest refuses an entry larger than the bounds once the queue is empty" do
    assert {:refused, backlog, [:a, :b]} = Backlog.admit(two(), :c, 11, :drop_oldest)
    assert %{frames: 0, bytes: 0} = backlog
  end

  test "an unbounded byte size bounds the entries alone" do
    backlog = Backlog.new(:infinity, 1)
    assert {:queued, backlog, []} = Backlog.admit(backlog, :a, 1_000_000, :drop_nothing)
    assert {:full, _backlog} = Backlog.admit(backlog, :b, 1, :drop_nothing)
  end

  test "peek reads the head, and requeue puts entries back before it in their order" do
    assert Backlog.peek(Backlog.new(10, 2)) == :empty
    assert Backlog.peek(two()) == {:value, :a}

    {:a, backlog} = Backlog.out(two())
    backlog = Backlog.requeue(backlog, [{:x, 1}, {:a, 4}])
    assert entries(backlog) == [:x, :a, :b]
    assert %{bytes: 9, frames: 3} = backlog
  end
end
