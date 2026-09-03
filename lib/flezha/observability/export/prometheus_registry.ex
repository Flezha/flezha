defmodule Flezha.Observability.Export.PrometheusRegistry do
  @moduledoc """
  In-memory aggregation registers for the Prometheus exporter.

  One GenServer owns three protected ETS `:set` tables — counters,
  gauges, histograms — keyed by
  `{source, metric_name, label_tuple}`. The sink calls `record/1` per
  signal; the HTTP server calls `render/0`, which reads ETS directly
  rather than going through this process.

  A metric's type is inferred from its measurement keys unless
  `metadata.prometheus_type` says otherwise, and label-values over the
  source's declared `cardinality_budget` are folded into one synthetic
  `__truncated__` series rather than dropped.
  """

  use GenServer

  alias Flezha.Observability.Bus
  alias Flezha.Observability.Registry, as: ObsRegistry
  alias Flezha.Observability.Signal

  @counters_tab :flezha_prom_counters
  @gauges_tab :flezha_prom_gauges
  @histograms_tab :flezha_prom_histograms
  @label_index_tab :flezha_prom_label_index
  # Two key shapes, deliberately: a bare `source` counts label truncations, a
  # `{source, :label_coercions}` tuple counts label-name rewrites and drops.
  # They cannot collide, so one table serves both.
  @registry_counters :flezha_prom_registry_counters

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

  # Reserved by the histogram renderer: `histogram_rows/2` appends its own
  # `{"le", …}` to every bucket line.
  @bucket_label "le"

  # ---------- Public API ----------

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

    counters = render_metrics(@counters_tab, "counter", &value_row/2)
    gauges = render_metrics(@gauges_tab, "gauge", &value_row/2)
    histograms = render_metrics(@histograms_tab, "histogram", &histogram_rows/2)

    series_count =
      :ets.info(@counters_tab, :size) +
        :ets.info(@gauges_tab, :size) +
        :ets.info(@histograms_tab, :size)

    {[counters, gauges, histograms], series_count}
  end

  @doc """
  Read the count of truncations for a given source. Read by the tests that
  pin the truncation rule; no operator-facing surface renders it.
  """
  @spec truncation_count(atom()) :: non_neg_integer()
  def truncation_count(source) when is_atom(source) do
    ensure_tables!()

    case :ets.lookup(@registry_counters, source) do
      [{^source, n}] -> n
      [] -> 0
    end
  end

  @doc """
  Read the count of label names this source had rewritten or dropped to keep
  the exposition well-formed. The rewrite itself is self-evident in the scrape
  — the sanitized name is what an operator reads — so this is a counter, not an
  event per emit.
  """
  @spec coercion_count(atom()) :: non_neg_integer()
  def coercion_count(source) when is_atom(source) do
    ensure_tables!()
    key = {source, :label_coercions}

    case :ets.lookup(@registry_counters, key) do
      [{^key, n}] -> n
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
    :ets.new(@registry_counters, [:set, :public, :named_table, read_concurrency: true])

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
    {labels, meas_for_type, coercions} = labels_and_measurement(meta, meas, type)
    bump_coercions(source, coercions)

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

  defp labels_and_measurement(meta, meas, type) do
    # Drop the override key from labels so it doesn't leak into the
    # rendered exposition.
    #
    # Sanitizing happens HERE, at record time, and not at render time: the
    # label list is part of the ETS key, so two raw label sets that sanitize
    # alike must merge into one series. Sanitizing at render would leave them
    # as two rows emitting identical series lines, which Prometheus rejects as
    # hard as a duplicate label.
    {labels, coercions} =
      meta
      |> Map.drop([:prometheus_type, :buckets])
      |> Enum.map(fn {k, v} -> {Signal.metadata_text(k), Signal.metadata_text(v)} end)
      |> Enum.sort()
      |> sanitize_labels(type)

    {labels, meas, coercions}
  end

  # The raw pairs arrive sorted, so a within-signal collision resolves to the
  # same survivor every time. The kept pairs are re-sorted on the sanitized
  # name, which is the one the exposition carries.
  defp sanitize_labels(pairs, type) do
    # `kept` doubles as the seen-set: a signal carries a handful of labels, and
    # at that width a list scan beats allocating a MapSet on every emit. It is
    # built head-first, so an earlier pair is always found — first-wins.
    {kept, coercions} =
      Enum.reduce(pairs, {[], 0}, fn {raw_name, value}, {kept, n} ->
        name = sanitize_label_name(raw_name)

        cond do
          type == :histogram and name == @bucket_label ->
            {kept, n + 1}

          List.keymember?(kept, name, 0) ->
            {kept, n + 1}

          true ->
            coerced = if name == raw_name, do: 0, else: 1
            {[{name, value} | kept], n + coerced}
        end
      end)

    {Enum.sort(kept), coercions}
  end

  # A Prometheus label name must match [a-zA-Z_][a-zA-Z0-9_]*, and a malformed
  # one fails the WHOLE scrape — one bad key blinds every metric the controller
  # has. A metadata key is a runtime term forwarded verbatim by TelemetryBridge,
  # so it is sanitized rather than refused: refusing would drop a caller's
  # dimension with nothing in the output to say so, while `http.status_code`
  # sanitizes to `http_status_code`, the published OTel -> Prometheus mapping.
  #
  # The leading `_` run collapses to one so no producer key can enter
  # Prometheus's reserved `__` namespace or forge this module's own
  # `__truncated__` marker.
  #
  # The grammar itself is `Signal.legal_exposition_name?/1` — the same one a
  # source name and a metric name are refused against. A label name is the half
  # that is rewritten rather than refused, and that difference is the only one.
  #
  # The rewrite is one pass over the bytes, then the leading run — deliberately
  # not two `String.replace/3` regexes, which are equivalent on every input and
  # cost ~9.7 us per name against ~0.8 us here.
  defp sanitize_label_name(name) do
    if Signal.legal_exposition_name?(name), do: name, else: rewrite_label_name(name)
  end

  defp rewrite_label_name(name) do
    name
    |> map_name_bytes()
    |> collapse_leading_underscores()
  end

  defp map_name_bytes(name) do
    for <<c <- name>>, into: "" do
      if c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_, do: <<c>>, else: "_"
    end
  end

  defp collapse_leading_underscores(""), do: "_"

  defp collapse_leading_underscores("_" <> rest), do: "_" <> drop_underscores(rest)

  defp collapse_leading_underscores(<<c, _::binary>> = name) when c in ?0..?9,
    do: "_" <> name

  defp collapse_leading_underscores(name), do: name

  defp drop_underscores("_" <> rest), do: drop_underscores(rest)
  defp drop_underscores(rest), do: rest

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
    :ets.update_counter(@registry_counters, source, {2, 1}, {source, 0})
  end

  defp bump_coercions(_source, 0), do: :ok

  defp bump_coercions(source, n) do
    key = {source, :label_coercions}
    _ = :ets.update_counter(@registry_counters, key, {2, n}, {key, 0})
    :ok
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

  # One exposition block per metric name: the two HELP/TYPE header lines, then
  # the metric's rows in label order. `row` is what a single stored row of this
  # table renders as — one line for a counter or gauge, a bucket family for a
  # histogram.
  defp render_metrics(table, type, row) do
    table
    |> group_by_metric()
    |> Enum.map(fn {{source, name}, rows} ->
      metric_name = Signal.exposition_name(source, name, "_")

      [
        "# HELP ",
        metric_name,
        " (",
        type,
        ")\n",
        "# TYPE ",
        metric_name,
        " ",
        type,
        "\n",
        rows
        |> Enum.sort_by(fn {labels, _v} -> labels end)
        |> Enum.map(&row.(metric_name, &1))
      ]
    end)
  end

  defp value_row(metric_name, {labels, value}) do
    [metric_name, render_labels(labels), " ", format_number(value), "\n"]
  end

  defp histogram_rows(metric_name, {labels, state}) do
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
      [@counters_tab, @gauges_tab, @histograms_tab, @label_index_tab, @registry_counters],
      fn tab ->
        case :ets.whereis(tab) do
          :undefined -> raise "Prometheus registry not started"
          _ -> :ok
        end
      end
    )
  end
end
