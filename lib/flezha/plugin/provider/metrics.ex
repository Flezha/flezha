defmodule Flezha.Plugin.Provider.Metrics do
  @moduledoc """
  Persistence-counter exporter.

  Exposes per-Segment WAL/snapshot counters from
  `Flezha.State.Metrics` under the metric prefix
  `flezha.segment.<OperativeClass>.<OperationId>.<SegmentKey>.<counter_name>`.

  Consumers fetch the snapshot via `gather/0`. A future Prometheus /
  StatsD bridge can subscribe to a `:telemetry` poll loop calling
  `gather/0`; for v1 the exporter is pull-only and is read by
  `flz state list` and ad-hoc executor inspection.
  """

  alias Flezha.State.{Metrics, Registry}

  @doc """
  Build the flat metrics map.
  """
  @spec gather() :: %{binary() => integer()}
  def gather() do
    Metrics.snapshot_all()
    |> Enum.flat_map(&flatten/1)
    |> Map.new()
  end

  @doc """
  Build the per-Segment metrics list — keyed structurally rather than
  flattened to a metric name. Useful for the CLI (`flz state list`) and
  the `flz operative info` extension.
  """
  @spec by_segment() :: [{Registry.segment_id(), %{atom() => integer()}}]
  def by_segment(), do: Metrics.snapshot_all()

  defp flatten({{_ce, operative_class, operation_id, segment_key}, counters}) do
    prefix =
      "flezha.segment." <>
        operative_class <> "." <> operation_id <> "." <> Registry.encode_segment_key(segment_key)

    Enum.map(counters, fn {name, value} ->
      {prefix <> "." <> Atom.to_string(name), value}
    end)
  end
end
