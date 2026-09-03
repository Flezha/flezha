defmodule Flezha.Observability.Export.OtlpCodec do
  @moduledoc """
  Hand-rolled protobuf encoder for the OTLP/HTTP messages the OTLP sink
  sends: `ExportMetricsServiceRequest`, `ExportLogsServiceRequest` and
  `ExportTraceServiceRequest`, v1 schema, only the fields the
  `Flezha.Observability.Signal` shape populates.

  Deliberately minimal — no varint table, no schema introspection. Each
  public function documents the field-number → wire-type mapping it
  emits. Timestamps are absolute nanoseconds since the Unix epoch;
  Signal carries monotonic microseconds and this module converts.

  One invariant: encoding never raises on the shape of an attribute and
  never puts bytes a field cannot carry on the wire. An attribute key is
  text and takes `Flezha.Observability.Signal.metadata_text/1`; a value
  takes the typed rule, `Signal.metadata_json/1`, and `any_value/1` maps
  its closed output domain onto `AnyValue`'s oneof.
  """

  import Bitwise

  alias Flezha.Observability.Signal

  @wire_varint 0
  @wire_64bit 1
  @wire_length_delim 2

  @int64_min -0x8000_0000_0000_0000
  @int64_max 0x7FFF_FFFF_FFFF_FFFF

  # ---------- Public API ----------

  @doc "Encode metric signals as an `ExportMetricsServiceRequest`."
  @spec encode_metrics([Signal.t()]) :: iodata()
  def encode_metrics(signals) when is_list(signals) do
    resource_metrics = build_resource_metrics(signals)
    # ExportMetricsServiceRequest { resource_metrics = 1 }
    field(1, @wire_length_delim, encode_message(resource_metrics))
  end

  @doc "Encode log signals as an `ExportLogsServiceRequest`."
  @spec encode_logs([Signal.t()]) :: iodata()
  def encode_logs(signals) when is_list(signals) do
    resource_logs = build_resource_logs(signals)
    field(1, @wire_length_delim, encode_message(resource_logs))
  end

  @doc "Encode trace_span signals as an `ExportTraceServiceRequest`."
  @spec encode_traces([Signal.t()]) :: iodata()
  def encode_traces(signals) when is_list(signals) do
    resource_spans = build_resource_spans(signals)
    field(1, @wire_length_delim, encode_message(resource_spans))
  end

  @doc "Encode a heterogeneous batch as a 3-tuple of (metrics, logs, traces) iodata."
  @spec encode_batch([Signal.t()]) :: %{
          metrics: iodata() | nil,
          logs: iodata() | nil,
          traces: iodata() | nil
        }
  def encode_batch(signals) do
    {metrics, logs, traces} =
      Enum.reduce(signals, {[], [], []}, fn
        %Signal{kind: :metric} = s, {m, l, t} -> {[s | m], l, t}
        %Signal{kind: :log} = s, {m, l, t} -> {m, [s | l], t}
        %Signal{kind: :trace_span} = s, {m, l, t} -> {m, l, [s | t]}
        # :event signals are exported as logs with severity=INFO.
        %Signal{kind: :event} = s, {m, l, t} -> {m, [event_to_log(s) | l], t}
        _, acc -> acc
      end)

    %{
      metrics: maybe_encode(metrics, &encode_metrics/1),
      logs: maybe_encode(logs, &encode_logs/1),
      traces: maybe_encode(traces, &encode_traces/1)
    }
  end

  defp maybe_encode([], _fun), do: nil
  defp maybe_encode(signals, fun), do: fun.(Enum.reverse(signals))

  # ---------- Resource / scope helpers ----------

  defp build_resource_metrics(signals) do
    scope_metrics = build_scope_metrics(signals)
    # ResourceMetrics { resource = 1, scope_metrics = 2 }
    [
      field(1, @wire_length_delim, encode_message(default_resource())),
      field(2, @wire_length_delim, encode_message(scope_metrics))
    ]
  end

  defp build_resource_logs(signals) do
    scope_logs = build_scope_logs(signals)

    [
      field(1, @wire_length_delim, encode_message(default_resource())),
      field(2, @wire_length_delim, encode_message(scope_logs))
    ]
  end

  defp build_resource_spans(signals) do
    scope_spans = build_scope_spans(signals)

    [
      field(1, @wire_length_delim, encode_message(default_resource())),
      field(2, @wire_length_delim, encode_message(scope_spans))
    ]
  end

  defp default_resource() do
    # Resource { attributes = 1, dropped_attributes_count = 2 }
    attrs = [
      kv_attribute("service.name", "flezha"),
      kv_attribute("service.version", Application.spec(:flezha, :vsn) |> to_string())
    ]

    Enum.map(attrs, fn iodata -> field(1, @wire_length_delim, encode_message(iodata)) end)
  end

  defp build_scope_metrics(signals) do
    metrics =
      signals
      |> Enum.map(&signal_to_metric/1)
      |> Enum.map(fn iodata -> field(2, @wire_length_delim, encode_message(iodata)) end)

    # ScopeMetrics { scope = 1, metrics = 2 }
    [field(1, @wire_length_delim, encode_message(instrumentation_scope())) | metrics]
  end

  defp build_scope_logs(signals) do
    logs =
      signals
      |> Enum.map(&signal_to_log_record/1)
      |> Enum.map(fn iodata -> field(2, @wire_length_delim, encode_message(iodata)) end)

    [field(1, @wire_length_delim, encode_message(instrumentation_scope())) | logs]
  end

  defp build_scope_spans(signals) do
    spans =
      signals
      |> Enum.map(&signal_to_span/1)
      |> Enum.map(fn iodata -> field(2, @wire_length_delim, encode_message(iodata)) end)

    [field(1, @wire_length_delim, encode_message(instrumentation_scope())) | spans]
  end

  defp instrumentation_scope() do
    # InstrumentationScope { name = 1, version = 2 }
    [
      field(1, @wire_length_delim, encode_string("flezha.observability")),
      field(2, @wire_length_delim, encode_string("1"))
    ]
  end

  # ---------- Per-signal encoders ----------

  defp signal_to_metric(%Signal{} = signal) do
    metric_name = exposition_name(signal.source, signal.name)
    data_point = build_number_data_point(signal)

    sum_msg =
      [
        # Sum { data_points = 1, aggregation_temporality = 2, is_monotonic = 3 }
        field(1, @wire_length_delim, encode_message(data_point)),
        # aggregation_temporality = AGGREGATION_TEMPORALITY_CUMULATIVE (2)
        field(2, @wire_varint, encode_varint(2)),
        field(3, @wire_varint, encode_varint(1))
      ]

    gauge_msg =
      [
        # Gauge { data_points = 1 }
        field(1, @wire_length_delim, encode_message(data_point))
      ]

    # Metric { name = 1, description = 2, unit = 3, data = oneof {gauge=5, sum=7, histogram=9} }
    common = [
      field(1, @wire_length_delim, encode_string(metric_name))
    ]

    payload =
      case classify(signal) do
        :counter -> [field(7, @wire_length_delim, encode_message(sum_msg))]
        _ -> [field(5, @wire_length_delim, encode_message(gauge_msg))]
      end

    common ++ payload
  end

  defp classify(%Signal{measurements: meas, metadata: meta}) do
    case Map.get(meta, :prometheus_type) do
      :counter -> :counter
      :gauge -> :gauge
      _ -> if Map.has_key?(meas, :count), do: :counter, else: :gauge
    end
  end

  defp build_number_data_point(signal) do
    ts_ns = signal_time_ns(signal)
    value = measurement_value(signal.measurements)

    base = [
      # NumberDataPoint {
      #   attributes = 7, start_time_unix_nano = 2, time_unix_nano = 3,
      #   as_double = 4, as_int = 6
      # }
      field(2, @wire_64bit, encode_fixed64(0)),
      field(3, @wire_64bit, encode_fixed64(ts_ns))
    ]

    val_field =
      cond do
        is_integer(value) -> [field(6, @wire_64bit, encode_sfixed64(value))]
        is_float(value) -> [field(4, @wire_64bit, encode_double(value))]
        true -> [field(6, @wire_64bit, encode_sfixed64(0))]
      end

    attr_fields =
      Enum.map(signal.metadata, fn {k, v} ->
        field(7, @wire_length_delim, encode_message(kv_attribute(k, v)))
      end)

    base ++ val_field ++ attr_fields
  end

  defp signal_to_log_record(%Signal{} = signal) do
    ts_ns = signal_time_ns(signal)

    # LogRecord {
    #   time_unix_nano = 1, severity_number = 2, severity_text = 3,
    #   body = 5 (AnyValue), attributes = 6, observed_time_unix_nano = 11
    # }
    body_str = Atom.to_string(signal.name)

    attrs =
      Enum.map(signal.metadata, fn {k, v} ->
        field(6, @wire_length_delim, encode_message(kv_attribute(k, v)))
      end)

    [
      field(1, @wire_64bit, encode_fixed64(ts_ns)),
      field(11, @wire_64bit, encode_fixed64(ts_ns)),
      field(2, @wire_varint, encode_varint(9)),
      field(3, @wire_length_delim, encode_string("INFO")),
      field(5, @wire_length_delim, encode_message(any_value(body_str)))
    ] ++ attrs
  end

  defp signal_to_span(%Signal{} = signal) do
    ts_ns = signal_time_ns(signal)

    duration_us =
      Map.get(signal.measurements, :duration_us) ||
        Map.get(signal.measurements, :duration_ns, 0)

    duration_ns =
      if duration_us > 1_000_000_000, do: duration_us, else: duration_us * 1_000

    end_ns = ts_ns + duration_ns

    # Span { trace_id = 1, span_id = 2, name = 5, kind = 6,
    #        start_time_unix_nano = 7, end_time_unix_nano = 8, attributes = 9 }
    attrs =
      Enum.map(signal.metadata, fn {k, v} ->
        field(9, @wire_length_delim, encode_message(kv_attribute(k, v)))
      end)

    [
      field(1, @wire_length_delim, encode_bytes(random_bytes(16))),
      field(2, @wire_length_delim, encode_bytes(random_bytes(8))),
      field(5, @wire_length_delim, encode_string(Atom.to_string(signal.name))),
      # SpanKind = SPAN_KIND_INTERNAL (1)
      field(6, @wire_varint, encode_varint(1)),
      field(7, @wire_64bit, encode_fixed64(ts_ns)),
      field(8, @wire_64bit, encode_fixed64(end_ns))
    ] ++ attrs
  end

  defp event_to_log(%Signal{} = signal), do: %{signal | kind: :log}

  defp measurement_value(meas) do
    cond do
      Map.has_key?(meas, :count) -> Map.get(meas, :count)
      Map.has_key?(meas, :value) -> Map.get(meas, :value)
      Map.has_key?(meas, :gauge) -> Map.get(meas, :gauge)
      true -> 0
    end
  end

  defp signal_time_ns(%Signal{ts: ts}) when is_integer(ts) do
    # Signal.ts is a monotonic-time microsecond. Add the system time
    # offset to convert to absolute Unix time (also in microseconds),
    # then scale to nanoseconds for OTLP.
    (ts + System.time_offset(:microsecond)) * 1_000
  end

  defp signal_time_ns(_), do: System.system_time(:nanosecond)

  defp exposition_name(source, name) do
    Atom.to_string(source) <> "." <> Atom.to_string(name)
  end

  # ---------- Common helpers ----------

  # A signal's metadata holds arbitrary terms. A key is text in every format and
  # takes the text rule; a value takes the typed rule, whose output domain is
  # `binary | number | boolean | nil | list | map with binary keys`.
  defp kv_attribute(key, value),
    do: attribute(Signal.metadata_text(key), Signal.metadata_json(value))

  # KeyValue { key = 1, value = 2 (AnyValue) }. A nested map's entries are built
  # here too, so a `kvlist_value` cannot drift from a top-level attribute.
  defp attribute(key, json_value) when is_binary(key) do
    [
      field(1, @wire_length_delim, encode_string(key)),
      field(2, @wire_length_delim, encode_message(any_value(json_value)))
    ]
  end

  # AnyValue { string_value = 1, bool_value = 2, int_value = 3, double_value = 4,
  #            array_value = 5, kvlist_value = 6, bytes_value = 7 }
  #
  # `nil` sets no member: OTLP has no null literal, and an unset oneof is the
  # protocol's own "no type", distinguishable on the wire from an empty string.
  defp any_value(nil), do: []

  defp any_value(value) when is_boolean(value),
    do: [field(2, @wire_varint, encode_varint(if(value, do: 1, else: 0)))]

  defp any_value(value) when is_integer(value) and value >= @int64_min and value <= @int64_max,
    do: [field(3, @wire_varint, encode_varint_int64(value))]

  defp any_value(value) when is_float(value),
    do: [field(4, @wire_64bit, encode_double(value))]

  defp any_value(value) when is_list(value) do
    # ArrayValue { values = 1 }
    elements = Enum.map(value, &field(1, @wire_length_delim, encode_message(any_value(&1))))
    [field(5, @wire_length_delim, encode_message(elements))]
  end

  defp any_value(%{} = value) when not is_struct(value) do
    # KeyValueList { values = 1 }
    entries =
      Enum.map(value, fn {k, v} ->
        field(1, @wire_length_delim, encode_message(attribute(k, v)))
      end)

    [field(6, @wire_length_delim, encode_message(entries))]
  end

  # A binary, an atom the typed rule already named, and — the reason this clause
  # is total rather than a binary clause — an integer with no `int64` to be.
  # `bytes_value` is not used: a binary that is not valid UTF-8 takes the text
  # rule's inspection so the raw bytes reach the wire nowhere.
  defp any_value(value),
    do: [field(1, @wire_length_delim, encode_string(Signal.metadata_text(value)))]

  defp random_bytes(n), do: :crypto.strong_rand_bytes(n)

  # ---------- Wire-format primitives ----------

  @doc false
  def field(field_number, wire_type, payload) do
    tag = field_number <<< 3 ||| wire_type

    case wire_type do
      @wire_length_delim ->
        body = IO.iodata_to_binary(payload)
        [encode_varint(tag), encode_varint(byte_size(body)), body]

      @wire_varint ->
        [encode_varint(tag), payload]

      @wire_64bit ->
        [encode_varint(tag), payload]
    end
  end

  defp encode_message(iodata), do: IO.iodata_to_binary(iodata)

  defp encode_string(value) when is_binary(value), do: value

  defp encode_bytes(value) when is_binary(value), do: value

  defp encode_varint_int64(value) when value < 0,
    do: encode_varint(value + 0x1_0000_0000_0000_0000)

  defp encode_varint_int64(value), do: encode_varint(value)

  defp encode_varint(value) when value < 0x80, do: <<value>>

  defp encode_varint(value) do
    <<1::1, value::7, encode_varint(value >>> 7)::binary>>
  end

  defp encode_fixed64(value), do: <<value::little-unsigned-64>>

  defp encode_sfixed64(value), do: <<value::little-signed-64>>

  defp encode_double(value), do: <<value::little-float-64>>
end
