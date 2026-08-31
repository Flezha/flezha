defmodule Flezha.Observability.RingBufferSink do
  @moduledoc """
  In-memory per-source ring buffer of recent signals.

  One `:protected` ETS `:ordered_set` per registered source, written by
  this sink alone and read from any process: `query/3` runs in the
  caller and never serializes through this GenServer. Entries are
  keyed `{ts, seq}` — `seq` is what keeps two same-tick emits apart.

  Two retention layers, an opportunistic cap on insert and a periodic
  age sweep; both bounds are in
  `docs/operations/observability.md` → Default sinks.
  """

  use GenServer, restart: :transient

  @behaviour Flezha.Observability.Sink

  alias Flezha.Observability.Registry, as: ObsRegistry

  @index_table :flezha_observability_ring_buffer_index

  # ---------- Sink behaviour ----------

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: :any, sources: :any}

  # ---------- Public API ----------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Range query over a source's ring buffer. Runs in the caller process
  via direct ETS access — does not call into the sink.

  `from_ts` is a monotonic-time lower bound in microseconds
  (`System.monotonic_time(:microsecond)`). A value of `0` is treated
  as a sentinel meaning *no lower bound* — callers asking for
  "everything in the buffer" can pass `0` rather than reasoning about
  the BEAM monotonic-time origin (which is typically negative on
  freshly-booted nodes).

  Options:
    * `:limit` — cap the returned list to N most-recent signals.
  """
  @spec query(atom(), integer(), keyword()) :: [Flezha.Observability.Signal.t()]
  def query(source, from_ts, opts \\ []) when is_atom(source) and is_integer(from_ts) do
    case lookup_table(source) do
      :error ->
        []

      {:ok, ref} ->
        ms =
          if from_ts == 0 do
            [{{:_, :"$1"}, [], [:"$1"]}]
          else
            [{{{:"$1", :_}, :"$2"}, [{:>=, :"$1", from_ts}], [:"$2"]}]
          end

        results = :ets.select(ref, ms)

        case Keyword.get(opts, :limit) do
          nil -> results
          limit when is_integer(limit) and limit >= 0 -> Enum.take(results, -limit)
        end
    end
  end

  @spec count(atom()) :: non_neg_integer()
  def count(source) when is_atom(source) do
    case lookup_table(source) do
      :error -> 0
      {:ok, ref} -> :ets.info(ref, :size)
    end
  end

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    config = observability_config()

    retention_ms = Keyword.get(config, :ring_buffer_retention_ms, 900_000)
    max_entries = Keyword.get(config, :ring_buffer_max_entries_per_source, 10_000)
    sweep_ms = Keyword.get(config, :retention_check_interval_ms, 5_000)

    ensure_index_table()

    seq_ref = :counters.new(1, [:atomics])

    state = %{
      tables: %{},
      retention_ms: retention_ms,
      max_entries: max_entries,
      sweep_ms: sweep_ms,
      seq_ref: seq_ref
    }

    state =
      ObsRegistry.list_sources()
      |> Enum.reduce(state, fn %{name: name}, acc -> ensure_table(acc, name) end)

    _ = ObsRegistry.subscribe_registry_events()
    schedule_sweep(sweep_ms)

    {:ok, state}
  end

  @impl true
  def handle_info({:signal, signal}, state) do
    state = ensure_table(state, signal.source)
    insert_signal(state, signal)
    {:noreply, state}
  end

  def handle_info(
        {:flezha_observability_registry, {:source_registered, name}},
        state
      ) do
    {:noreply, ensure_table(state, name)}
  end

  def handle_info(
        {:flezha_observability_registry, {:source_unregistered, name}},
        state
      ) do
    {:noreply, drop_table(state, name)}
  end

  def handle_info({:flezha_observability_registry, _other}, state), do: {:noreply, state}

  def handle_info(:sweep, state) do
    cutoff = System.monotonic_time(:microsecond) - state.retention_ms * 1_000

    Enum.each(state.tables, fn {_source, ref} ->
      ms = [{{{:"$1", :_}, :_}, [{:<, :"$1", cutoff}], [true]}]
      _ = :ets.select_delete(ref, ms)
    end)

    schedule_sweep(state.sweep_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------- Internal helpers ----------

  defp insert_signal(state, signal) do
    case Map.fetch(state.tables, signal.source) do
      :error ->
        :ok

      {:ok, ref} ->
        :counters.add(state.seq_ref, 1, 1)
        seq = :counters.get(state.seq_ref, 1)
        :ets.insert(ref, {{signal.ts, seq}, signal})

        if :ets.info(ref, :size) > state.max_entries do
          case :ets.first(ref) do
            :"$end_of_table" -> :ok
            key -> :ets.delete(ref, key)
          end
        end

        :ok
    end
  end

  defp ensure_table(state, source) do
    case Map.fetch(state.tables, source) do
      {:ok, _ref} ->
        state

      :error ->
        ref =
          :ets.new(:flezha_observability_ring_buffer, [
            :ordered_set,
            :protected,
            read_concurrency: true
          ])

        :ets.insert(@index_table, {source, ref})
        %{state | tables: Map.put(state.tables, source, ref)}
    end
  end

  defp drop_table(state, source) do
    case Map.pop(state.tables, source) do
      {nil, _} ->
        state

      {ref, tables} ->
        :ets.delete(@index_table, source)
        :ets.delete(ref)
        %{state | tables: tables}
    end
  end

  defp ensure_index_table() do
    case :ets.whereis(@index_table) do
      :undefined ->
        :ets.new(@index_table, [
          :set,
          :protected,
          :named_table,
          read_concurrency: true
        ])

      _ ->
        :ok
    end
  end

  defp lookup_table(source) do
    case :ets.whereis(@index_table) do
      :undefined ->
        :error

      _ ->
        case :ets.lookup(@index_table, source) do
          [{^source, ref}] -> {:ok, ref}
          [] -> :error
        end
    end
  end

  defp schedule_sweep(ms) do
    Process.send_after(self(), :sweep, ms)
  end

  defp observability_config() do
    Application.get_env(:flezha, :observability, [])
  end
end
