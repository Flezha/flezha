defmodule Flezha.State.Metrics do
  @moduledoc """
  Per-Entity `:counters` references for the persistence subsystem.

  Eleven counter slots per Entity: `:persisted_changes_total` and
  `:persisted_bytes_total` (monotonic); `:wal_size_bytes`, `:snapshot_seq`,
  `:flush_latency_ms_p99` and `:fsync_durability_lag_ms` (gauges); plus
  `:dedup_drops_total`, `:wal_seq` (the last persisted seq), `:wal_shed_total`
  (appends dropped past the hard in-memory bound), `:stale_writer_refused_total`
  (frames refused from a takeover-superseded session) and `:fsync_total` (syncs
  the writer ran — what a batch of appends shares).

  Allocations are lazy and stored in a module-level `:ets` table keyed by
  `entity_id`.
  """

  alias Flezha.State.Paths

  @table __MODULE__

  # `:write_concurrency` rather than `:atomics`: a per-Entity table written by
  # the writer and read by the exporter, not a hot single-process counter.
  use Flezha.SDK.CounterTable,
    table: :counter,
    new_opts: [:write_concurrency],
    slots: [
      [name: :persisted_changes_total, kind: :count],
      [name: :persisted_bytes_total, kind: :count],
      [name: :wal_size_bytes, kind: :set],
      [name: :snapshot_seq, kind: :set],
      [name: :flush_latency_ms_p99, kind: :set],
      [name: :fsync_durability_lag_ms, kind: :set],
      [name: :dedup_drops_total, kind: :count],
      [name: :wal_seq, kind: :set],
      [name: :wal_shed_total, kind: :count],
      [name: :stale_writer_refused_total, kind: :count],
      [name: :fsync_total, kind: :count]
    ]

  @counters Enum.map(@counter_rows, &Keyword.fetch!(&1, :name))

  @spec counters() :: [atom()]
  def counters(), do: @counters

  @spec for_entity(Paths.entity_id()) :: :counters.counters_ref()
  def for_entity(entity_id) do
    ensure_table()

    case :ets.lookup(@table, entity_id) do
      [{^entity_id, ref}] ->
        ref

      [] ->
        ref = counter_new()
        # `:ets.insert_new/2` keeps the first writer's ref under races.
        case :ets.insert_new(@table, {entity_id, ref}) do
          true -> ref
          false -> :ets.lookup_element(@table, entity_id, 2)
        end
    end
  end

  @spec add(:counters.counters_ref(), atom(), integer()) :: :ok
  def add(ref, name, delta) when is_atom(name) do
    counter_add(ref, name, delta)
  end

  @spec put(:counters.counters_ref(), atom(), integer()) :: :ok
  def put(ref, name, value) when is_atom(name) do
    counter_set(ref, name, value)
  end

  @spec get(:counters.counters_ref(), atom()) :: integer()
  def get(ref, name) when is_atom(name) do
    counter_get(ref, name)
  end

  @spec to_map(:counters.counters_ref()) :: %{atom() => integer()}
  def to_map(ref) do
    counter_snapshot(ref)
  end

  @spec drop_entity(Paths.entity_id()) :: :ok
  def drop_entity(entity_id) do
    ensure_table()
    :ets.delete(@table, entity_id)
    :ok
  end

  @doc """
  Snapshot every Entity's counter set. Returns
  `[{entity_id, %{counter_name => value}}]`. Used by the metrics
  exporter and by `flz state list`.
  """
  @spec snapshot_all() :: [{Paths.entity_id(), %{atom() => integer()}}]
  def snapshot_all() do
    ensure_table()

    :ets.tab2list(@table)
    |> Enum.map(fn {entity_id, ref} -> {entity_id, to_map(ref)} end)
  end

  @doc """
  Build the flat metrics map.
  """
  @spec gather() :: %{binary() => integer()}
  def gather() do
    snapshot_all()
    |> Enum.flat_map(&flatten/1)
    |> Map.new()
  end

  @doc """
  Build the per-Entity metrics list — keyed structurally rather than
  flattened to a metric name. Useful for the CLI (`flz state list`) and
  the `flz operative info` extension.
  """
  @spec by_entity() :: [{Paths.entity_id(), %{atom() => integer()}}]
  def by_entity(), do: snapshot_all()

  defp ensure_table() do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
        rescue
          ArgumentError -> :ok
        end

      _ ->
        :ok
    end
  end

  defp flatten({{_ce, operative_class, procedure_id, entity_key}, counters}) do
    prefix =
      "flezha.entity." <>
        operative_class <> "." <> procedure_id <> "." <> Paths.encode_entity_key(entity_key)

    Enum.map(counters, fn {name, value} ->
      {prefix <> "." <> Atom.to_string(name), value}
    end)
  end
end
