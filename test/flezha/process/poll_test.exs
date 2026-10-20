defmodule Flezha.Process.PollTest do
  @moduledoc """
  `Flezha.Process.Poll.until/4`, the one wait-until loop: it answers the first
  `{:ok, _}`, answers the last refusal once the deadline passes, and never
  pauses past the deadline.
  """
  use ExUnit.Case, async: true

  alias Flezha.Process.Poll

  # A condition answering `answers` in turn, the last one repeating, that counts
  # how often it was asked.
  defp condition(answers) do
    asked = :counters.new(1, [])

    fun = fn ->
      :counters.add(asked, 1, 1)
      Enum.at(answers, :counters.get(asked, 1) - 1, List.last(answers))
    end

    {fun, fn -> :counters.get(asked, 1) end}
  end

  # A pause that records what it was asked to wait, then waits it.
  defp recorded_pause do
    pauses = :ets.new(:pauses, [:duplicate_bag, :public])
    pause = fn ms -> :ets.insert(pauses, {:pause, ms}) && Process.sleep(ms) end
    {pause, fn -> for {:pause, ms} <- :ets.tab2list(pauses), do: ms end}
  end

  defp deadline_in(ms), do: System.monotonic_time(:millisecond) + ms

  @tag timeout: 5_000
  test "answers the first {:ok, _} and asks no more" do
    {fun, asked} = condition([{:error, :a}, {:error, :b}, {:ok, :held}, {:ok, :later}])

    assert Poll.until(deadline_in(2_000), 1, fun) == {:ok, :held}
    assert asked.() == 3
  end

  @tag timeout: 5_000
  test "at the deadline answers the last refusal, every pause inside the deadline" do
    {pause, pauses} = recorded_pause()
    asked = :counters.new(1, [])

    fun = fn ->
      :counters.add(asked, 1, 1)
      {:error, :counters.get(asked, 1)}
    end

    assert {:error, last} = Poll.until(deadline_in(25), 10, fun, pause)

    assert last == :counters.get(asked, 1)
    assert length(pauses.()) == last - 1
    assert Enum.all?(pauses.(), &(&1 in 1..10))
    assert Enum.sum(pauses.()) <= 25
  end

  test "a deadline already passed asks once and never pauses" do
    {pause, pauses} = recorded_pause()
    {fun, asked} = condition([{:error, :timeout}])

    assert Poll.until(deadline_in(-1), 10, fun, pause) == {:error, :timeout}
    assert asked.() == 1
    assert pauses.() == []
  end
end
