defmodule Flezha.Runner.StreamChannel.PrefixTest do
  @moduledoc """
  A processed-prefix marker is confirmed only once every writer of its
  instance fsynced past the bytes it had been handed when the marker arrived,
  and only once the stream's bind flush landed.
  """
  use ExUnit.Case, async: true

  alias Flezha.Runner.StreamChannel.Prefix

  @btc {"op", "task", "BTC-USD"}
  @sol {"op", "task", "SOL-USD"}

  defp fenced do
    {[], prefix} = Prefix.fenced(Prefix.new())
    prefix
  end

  test "a marker waits for the fsync of the records ahead of it" do
    prefix = fenced() |> Prefix.received(@sol, 100)

    {[], prefix} = Prefix.marker(prefix, "op", 5)
    # A record behind the marker is not what it waits on.
    prefix = Prefix.received(prefix, @sol, 50)
    {[], prefix} = Prefix.drained(prefix, @sol, 60)
    assert {[{"op", 5, 1}], _prefix} = Prefix.drained(prefix, @sol, 40)
  end

  test "a writer with nothing outstanding holds no marker" do
    prefix = fenced() |> Prefix.received(@btc, 10)
    {[], prefix} = Prefix.drained(prefix, @btc, 10)
    prefix = Prefix.received(prefix, @sol, 20)

    {[], prefix} = Prefix.marker(prefix, "op", 1005)
    assert {[{"op", 1005, 1}], prefix} = Prefix.drained(prefix, @sol, 20)
    assert prefix.outstanding == %{} and prefix.markers == %{}
  end

  test "markers confirm in order, the latest drained one wins" do
    prefix = fenced() |> Prefix.received(@btc, 10)
    {[], prefix} = Prefix.marker(prefix, "op", 1)
    prefix = Prefix.received(prefix, @btc, 10)
    {[], prefix} = Prefix.marker(prefix, "op", 2)

    assert {[{"op", 2, 1}], _prefix} = Prefix.drained(prefix, @btc, 20)
  end

  test "released bytes confirm nothing they covered and stop holding later markers" do
    prefix = fenced() |> Prefix.received(@btc, 10)
    {[], prefix} = Prefix.marker(prefix, "op", 1)

    # The writer let go of its unshipped record; a restarted one carries on.
    {[], prefix} = Prefix.released(prefix, @btc, 10)
    prefix = Prefix.received(prefix, @sol, 5)
    {[], prefix} = Prefix.marker(prefix, "op", 2)

    assert {[{"op", 2, 1}], prefix} = Prefix.drained(prefix, @sol, 5)
    assert prefix.outstanding == %{} and prefix.markers == %{}
  end

  test "released bytes leave the writer's other bytes owed" do
    prefix = fenced() |> Prefix.received(@btc, 10)
    {[], prefix} = Prefix.released(prefix, @btc, 4)
    {[], prefix} = Prefix.marker(prefix, "op", 3)

    assert {[], prefix} = Prefix.drained(prefix, @sol, 0)
    assert {[{"op", 3, 1}], _prefix} = Prefix.drained(prefix, @btc, 6)
  end

  test "an unfenced stream confirms nothing until its fence flush lands" do
    {[], prefix} = Prefix.marker(Prefix.new(), "op", 3)
    assert {[{"op", 3, 1}], _prefix} = Prefix.fenced(prefix)
  end

  test "a stream asks for its fence flush once, at its first marker" do
    assert {true, asked} = Prefix.ask_fence(Prefix.new())
    assert {false, ^asked} = Prefix.ask_fence(asked)
    {[], fenced} = Prefix.fenced(asked)
    assert {false, ^fenced} = Prefix.ask_fence(fenced)
  end
end
