defmodule FlezhaTest.Service.PluginRecordsTest do
  @moduledoc """
  The enabled-plugin listing builds each plugin's row from its own group of
  bindings and running relays, so its cost grows linearly with the fleet.
  """

  use FlezhaTest.Case, async: true

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.File.Sink, as: FileSink
  alias Flezha.Service.PluginRecords

  @namespace "plugin_records_ns"

  defp entry(name), do: %Plugin{id: name, provider: FileSink}
  defp bound(relay_id, id), do: {relay_id, id, %Plugin{id: id, provider: FileSink}}

  test "each row names its own relays and the nodes running it" do
    bindings = [
      bound("a", "sink"),
      bound("a", "sink"),
      bound("orders#p1", "sink#p1"),
      bound("b", "other")
    ]

    running = [{:n1@host, ["sink"]}, {:n2@host, ["sink#p1", "other"]}, {:n1@host, ["other"]}]

    rows =
      PluginRecords.rows(
        @namespace,
        [entry("sink"), entry("other"), entry("idle")],
        bindings,
        running
      )

    assert [
             %{name: "sink", relays: ["a", "orders#p1"], controllers: [:n1@host, :n2@host]},
             %{name: "other", relays: ["b"], controllers: [:n1@host, :n2@host]},
             %{name: "idle", relays: [], controllers: [], lagging: []}
           ] = rows
  end

  # Best of several runs, so one scheduler hiccup does not decide the ratio.
  defp best_us(fun), do: Enum.min(for _ <- 1..5, do: elem(:timer.tc(fun), 0))

  # `n` plugins, each bound to and running on its own relay.
  defp listing(n) do
    entries = for i <- 1..n, do: entry("p#{i}")
    bindings = for i <- 1..n, do: bound("r#{i}", "p#{i}")
    running = for i <- 1..n, do: {node(), ["p#{i}"]}
    fn -> PluginRecords.rows(@namespace, entries, bindings, running) end
  end

  test "listing grows linearly with plugins bound one per relay" do
    small = best_us(listing(500))
    large = best_us(listing(2_000))

    assert large / max(small, 1) < 8,
           "500 plugins listed in #{small} µs, 2,000 in #{large} µs"
  end
end
