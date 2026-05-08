defmodule Flezha.Plugin.Provider.Metrics do
  @moduledoc """
  Persistence-counter exporter.

  Exposes per-Partition WAL/snapshot counters from
  `Flezha.State.Metrics` under the metric prefix
  `flezha.partition.<RelayClass>.<ShardId>.<PartitionKey>.<counter_name>`.

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
  Build the per-Partition metrics list — keyed structurally rather than
  flattened to a metric name. Useful for the CLI (`flz state list`) and
  the `flz relay info` extension.
  """
  @spec by_partition() :: [{Registry.partition_id(), %{atom() => integer()}}]
  def by_partition(), do: Metrics.snapshot_all()

  defp flatten({{_ce, relay_class, shard_id, partition_key}, counters}) do
    prefix =
      "flezha.partition." <>
        relay_class <> "." <> shard_id <> "." <> Registry.encode_partition_key(partition_key)

    Enum.map(counters, fn {name, value} ->
      {prefix <> "." <> Atom.to_string(name), value}
    end)
  end
end
