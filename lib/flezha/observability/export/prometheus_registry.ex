defmodule Flezha.Observability.Export.PrometheusRegistry do
  @moduledoc """
  In-memory aggregation registers for the Prometheus exporter.

  One GenServer owns three protected ETS `:set` tables — counters,
  gauges, histograms — each keyed by
  `{source_atom, metric_name_atom, label_tuple}`. The Sink calls
  `record/1` on signal receipt; the HTTP server calls `render/0` to
  produce the Prometheus text exposition body via direct ETS reads.

  Metric type is inferred from `Signal.measurements` keys per
  proposal D6; producers can override via `metadata.prometheus_type`.

  Per-`{source, name}` value-cardinality cap honours the source's
  declared `cardinality_budget` (read from the foundation
  `Flezha.Observability.Registry`). When new label-values would push
  the metric over budget the registry aggregates the over-budget
  emit into a synthetic `__truncated__: "true"` series and increments
  `:prometheus_label_truncations` on the source's counter.
  """

  use GenServer

  alias Flezha.Observability.Bus
  alias Flezha.Observability.Registry, as: ObsRegistry
  alias Flezha.Observability.Signal

  @counters_tab :flezha_prom_counters
  @gauges_tab :flezha_prom_gauges
  @histograms_tab :flezha_prom_histograms
  @label_index_tab :flezha_prom_label_index
  @truncation_tab :flezha_prom_truncation_counters

  @duration_buckets_us [
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000
  ]

  @bytes_buckets [
    1024,
    4096,
    16_384,
    65_536,
    262_144,
    1_048_576,
    4_194_304,
    16_777_216
  ]

  @truncated_label_value "true"
  @truncated_label_key "__truncated__"

  # ---------- Public API ----------

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Record a `:metric` signal into the appropriate register. Non-metric
  kinds are ignored.
  """
  @spec record(Signal.t()) :: :ok
  def record(%Signal{kind: :metric} = signal) do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) -> GenServer.cast(pid, {:record, signal})
      _ -> :ok
    end
  end

  def record(_signal), do: :ok

  @doc """
  Render the Prometheus text exposition body for the entire
  registry. Returns iodata plus the total series count (used by the
  HTTP layer to emit a `:prometheus_scrape` event).
  """
  @spec render() :: {iodata(), non_neg_integer()}
  def render() do
    ensure_tables!()

    counters = render_counters()
    gauges = render_gauges()
    histograms = render_histograms()

    series_count =
      :ets.info(@counters_tab, :size) +
        :ets.info(@gauges_tab, :size) +
        :ets.info(@histograms_tab, :size)

    {[counters, gauges, histograms], series_count}
  end

  @doc """
  Read the count of truncations for a given source. Used by tests
  and `flz observe sources --json`.
  """
  @spec truncation_count(atom()) :: non_neg_integer()
  def truncation_count(source) when is_atom(source) do
    ensure_tables!()

    case :ets.lookup(@truncation_tab, source) do
      [{^source, n}] -> n
      [] -> 0
    end
  end

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    :ets.new(@counters_tab, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(@gauges_tab, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(@histograms_tab, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(@label_index_tab, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(@truncation_tab, [:set, :public, :named_table, read_concurrency: true])

    {:ok, %{}}
  end

  @impl true
  def handle_cast({:record, signal}, state) do
    do_record(signal)
    {:noreply, state}
  end

  # ---------- Recording ----------

  defp do_record(%Signal{source: source, name: name, measurements: meas, metadata: meta}) do
    type = infer_type(meas, meta)
    {labels, meas_for_type} = labels_and_measurement(meta, meas, type)

    case check_cardinality(source, name, labels) do
      :ok ->
        write_metric(type, source, name, labels, meas_for_type)

      :truncate ->
        bump_truncation(source)
        emit_truncation_event(source, name)

        write_metric(
          type,
          source,
          name,
          [{@truncated_label_key, @truncated_label_value}],
          meas_for_type
        )
    end
  end

  defp infer_type(meas, meta) do
    case Map.get(meta, :prometheus_type) do
      type when type in [:counter, :gauge, :histogram] ->
        type

      _ ->
        cond do
          Map.has_key?(meas, :duration_us) -> :histogram
          Map.has_key?(meas, :bytes) -> :histogram
          Map.has_key?(meas, :buckets) -> :histogram
          Map.has_key?(meas, :count) -> :counter
          Map.has_key?(meas, :gauge) -> :gauge
          Map.has_key?(meas, :value) -> :gauge
          true -> :gauge
        end
    end
  end

  defp labels_and_measurement(meta, meas, _type) do
    # Drop the override key from labels so it doesn't leak into the
    # rendered exposition.
    labels =
      meta
      |> Map.drop([:prometheus_type, :buckets])
      |> Enum.map(fn {k, v} -> {to_string(k), to_string(v)} end)
      |> Enum.sort()

    {labels, meas}
  end

  defp check_cardinality(source, name, labels) do
    key = {source, name}
    budget = source_budget(source)

    case :ets.lookup(@label_index_tab, key) do
      [{^key, set}] ->
        if MapSet.member?(set, labels) do
          :ok
        else
          if MapSet.size(set) >= budget do
            :truncate
          else
            :ets.insert(@label_index_tab, {key, MapSet.put(set, labels)})
            :ok
          end
        end

      [] ->
        :ets.insert(@label_index_tab, {key, MapSet.new([labels])})
        :ok
    end
  end

  defp source_budget(source) do
    case ObsRegistry.lookup_source(source) do
      {:ok, %{cardinality_budget: b}} -> b
      _ -> 1_000
    end
  end

  defp bump_truncation(source) do
    :ets.update_counter(@truncation_tab, source, {2, 1}, {source, 0})
  end

  defp emit_truncation_event(source, name) do
    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: :observability_export,
        name: :prometheus_label_truncated,
        measurements: %{count: 1},
        metadata: %{source: source, metric: name}
      })
    )
  end

  defp write_metric(:counter, source, name, labels, meas) do
    delta = Map.get(meas, :count, 0)
    key = {source, name, labels}
    _ = :ets.update_counter(@counters_tab, key, {2, delta}, {key, 0})
    :ok
  end

  defp write_metric(:gauge, source, name, labels, meas) do
    value = Map.get(meas, :gauge) || Map.get(meas, :value) || 0
    key = {source, name, labels}
    :ets.insert(@gauges_tab, {key, value})
    :ok
  end

  defp write_metric(:histogram, source, name, labels, meas) do
    {sample, buckets, unit} = histogram_sample(meas)
    key = {source, name, labels}

    init = %{
      buckets: Enum.map(buckets, &{&1, 0}),
      sum: 0,
      count: 0,
      unit: unit
    }

    current = lookup_or_init(@histograms_tab, key, init)

    new_buckets =
      Enum.map(current.buckets, fn {ub, hits} ->
        if sample <= ub, do: {ub, hits + 1}, else: {ub, hits}
      end)

    new_state = %{
      current
      | buckets: new_buckets,
        sum: current.sum + sample,
        count: current.count + 1,
        unit: unit
    }

    :ets.insert(@histograms_tab, {key, new_state})
    :ok
  end

  defp lookup_or_init(tab, key, init) do
    case :ets.lookup(tab, key) do
      [{^key, value}] -> value
      [] -> init
    end
  end

  defp histogram_sample(%{duration_us: us} = meas) do
    buckets = Map.get(meas, :buckets, @duration_buckets_us)
    {us, buckets, :microseconds}
  end

  defp histogram_sample(%{bytes: b} = meas) do
    buckets = Map.get(meas, :buckets, @bytes_buckets)
    {b, buckets, :bytes}
  end

  defp histogram_sample(%{buckets: buckets, value: v}), do: {v, buckets, :unit}
  defp histogram_sample(%{value: v}), do: {v, @duration_buckets_us, :unit}
  defp histogram_sample(_meas), do: {0, @duration_buckets_us, :unit}

  # ---------- Rendering ----------

  defp render_counters() do
    grouped = group_by_metric(@counters_tab)

    Enum.map(grouped, fn {{source, name}, rows} ->
      metric_name = exposition_name(source, name)

      [
        "# HELP ",
        metric_name,
        " (counter)\n",
        "# TYPE ",
        metric_name,
        " counter\n",
        Enum.map(Enum.sort_by(rows, fn {labels, _v} -> labels end), fn {labels, value} ->
          [metric_name, render_labels(labels), " ", format_number(value), "\n"]
        end)
      ]
    end)
  end

  defp render_gauges() do
    grouped = group_by_metric(@gauges_tab)

    Enum.map(grouped, fn {{source, name}, rows} ->
      metric_name = exposition_name(source, name)

      [
        "# HELP ",
        metric_name,
        " (gauge)\n",
        "# TYPE ",
        metric_name,
        " gauge\n",
        Enum.map(Enum.sort_by(rows, fn {labels, _v} -> labels end), fn {labels, value} ->
          [metric_name, render_labels(labels), " ", format_number(value), "\n"]
        end)
      ]
    end)
  end

  defp render_histograms() do
    grouped = group_by_metric(@histograms_tab)

    Enum.map(grouped, fn {{source, name}, rows} ->
      metric_name = exposition_name(source, name)

      [
        "# HELP ",
        metric_name,
        " (histogram)\n",
        "# TYPE ",
        metric_name,
        " histogram\n",
        Enum.map(Enum.sort_by(rows, fn {labels, _v} -> labels end), fn {labels, state} ->
          render_histogram_rows(metric_name, labels, state)
        end)
      ]
    end)
  end

  defp render_histogram_rows(metric_name, labels, state) do
    bucket_lines =
      Enum.map(state.buckets, fn {ub, hits} ->
        label_pairs = labels ++ [{"le", format_number(ub)}]

        [metric_name, "_bucket", render_labels(label_pairs), " ", Integer.to_string(hits), "\n"]
      end)

    inf_line = [
      metric_name,
      "_bucket",
      render_labels(labels ++ [{"le", "+Inf"}]),
      " ",
      Integer.to_string(state.count),
      "\n"
    ]

    sum_line = [
      metric_name,
      "_sum",
      render_labels(labels),
      " ",
      format_number(state.sum),
      "\n"
    ]

    count_line = [
      metric_name,
      "_count",
      render_labels(labels),
      " ",
      Integer.to_string(state.count),
      "\n"
    ]

    [bucket_lines, inf_line, sum_line, count_line]
  end

  defp group_by_metric(tab) do
    ensure_tables!()

    :ets.tab2list(tab)
    |> Enum.group_by(
      fn {{source, name, _labels}, _} -> {source, name} end,
      fn {{_, _, labels}, value} -> {labels, value} end
    )
    |> Enum.sort_by(fn {key, _} -> key end)
  end

  defp exposition_name(source, name) do
    Atom.to_string(source) <> "_" <> Atom.to_string(name)
  end

  defp render_labels([]), do: ""

  defp render_labels(labels) do
    inner =
      Enum.map_join(labels, ",", fn {k, v} ->
        "#{k}=\"#{escape_label_value(v)}\""
      end)

    "{" <> inner <> "}"
  end

  defp escape_label_value(v) do
    v
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("\n", "\\n")
  end

  defp format_number(n) when is_integer(n), do: Integer.to_string(n)

  defp format_number(n) when is_float(n) do
    cond do
      n == :math.floor(n) and abs(n) < 1.0e15 ->
        Integer.to_string(trunc(n)) <> ".0"

      true ->
        :erlang.float_to_binary(n, [:short])
    end
  end

  defp ensure_tables!() do
    Enum.each(
      [@counters_tab, @gauges_tab, @histograms_tab, @label_index_tab, @truncation_tab],
      fn tab ->
        case :ets.whereis(tab) do
          :undefined -> raise "Prometheus registry not started"
          _ -> :ok
        end
      end
    )
  end
end
