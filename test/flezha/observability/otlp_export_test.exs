defmodule Flezha.Observability.OtlpExportTest do
  # async: false: writes Application env; the observability registry; registered singletons.
  use FlezhaTest.Case, fixtures: [:env], async: false

  import Bitwise

  alias Flezha.Observability.Export.OtlpClient
  alias Flezha.Observability.Export.OtlpCodec
  alias Flezha.Observability.Export.OtlpSink
  alias Flezha.Observability.LoggerHandler
  alias Flezha.Observability.Registry
  alias Flezha.Observability.Signal
  alias Flezha.Observability.TestSinks
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :observability_export

  setup do
    on_exit(fn ->
      Registry.unregister_sink(:prometheus)
      Registry.unregister_sink(:otlp)
    end)

    :ok
  end

  describe "OtlpCodec attribute values" do
    setup do
      sink = :"otlp_attrs_#{:erlang.unique_integer([:positive])}"

      {:ok, _pid} =
        Registry.register_sink(%{
          name: sink,
          module: TestSinks.Counting,
          config: %{
            counter_ref: :counters.new(1, [:atomics]),
            parent: self(),
            sources: [:logger]
          }
        })

      on_exit(fn -> Registry.unregister_sink(sink) end)
      :ok
    end

    test "a signal from the real Logger bridge encodes, map typed and tuple readable" do
      message = "otlp-attribute-probe-#{:erlang.unique_integer([:positive])}"

      LoggerHandler.log(
        %{
          level: :warning,
          msg: {:string, message},
          meta: %{mfa: {__MODULE__, :probe, 2}, file: ~c"lib/probe.ex", line: 412, pid: self()}
        },
        %{}
      )

      signal =
        TestSinks.receive_signal(&(&1.metadata[:message] == message)) ||
          flunk("the Logger bridge produced no signal carrying #{message}")

      # The bridge puts both shapes `to_string/1` refuses on every log event.
      assert is_map(signal.metadata.logger)
      assert is_tuple(signal.metadata.mfa)

      attrs = log_attributes(OtlpCodec.encode_logs([signal]))

      # The map crosses as a nested KeyValueList, so a collector can index into
      # it; the tuple has no OTLP shape and crosses as its inspection.
      assert {:kvlist, logger} = attrs["logger"]
      assert {:int, 412} = logger["line"]
      assert {:string, mfa} = attrs["mfa"]
      assert String.starts_with?(mfa, "{")
      assert mfa =~ "probe"
      assert {:string, ^message} = attrs["message"]

      # `file:` is a charlist, which is a list of integers and nothing else:
      # the shared typed rule cannot tell it from `[108, 105, ...]`.
      assert {:array, chars} = attrs["file"]
      assert Enum.map(chars, fn {:int, c} -> c end) == ~c"lib/probe.ex"
    end

    test "a list attribute is an array of typed elements, not a string" do
      attrs = log_attributes(OtlpCodec.encode_logs([attr_signal(:log, %{l: [1, 2, 3]})]))

      assert attrs["l"] == {:array, [{:int, 1}, {:int, 2}, {:int, 3}]}
    end

    test "a binary attribute is unchanged" do
      attrs = log_attributes(OtlpCodec.encode_logs([attr_signal(:log, %{path: "/x"})]))

      assert attrs["path"] == {:string, "/x"}
    end

    test "each scalar takes the AnyValue member that carries its type" do
      meta = %{i: 412, neg: -412, big: 1 <<< 70, ok: true, no: false, f: 1.5, a: :some_atom}

      attrs = log_attributes(OtlpCodec.encode_logs([attr_signal(:log, meta)]))

      assert attrs["i"] == {:int, 412}
      assert attrs["neg"] == {:int, -412}
      assert attrs["ok"] == {:bool, true}
      assert attrs["no"] == {:bool, false}
      assert attrs["f"] == {:double, 1.5}
      assert attrs["a"] == {:string, "some_atom"}
      # An integer with no int64 to be is its decimal rendering, not a varint
      # no decoder can read.
      assert attrs["big"] == {:string, Integer.to_string(1 <<< 70)}
    end

    test "a term OTLP has no shape for crosses as its inspection" do
      meta = %{t: {:a, :b}, p: self(), s: %Signal{kind: :log, source: :s, name: :n}}

      attrs = log_attributes(OtlpCodec.encode_logs([attr_signal(:log, meta)]))

      for key <- ~w(t p s) do
        assert {:string, rendered} = attrs[key]
        assert String.valid?(rendered)
      end

      assert {:string, "{:a, :b}"} = attrs["t"]
      assert {:string, "#PID" <> _} = attrs["p"]
      assert {:string, "%Flezha.Observability.Signal{" <> _} = attrs["s"]
    end

    test "a nested map is built by the same rule as a top-level attribute" do
      meta = %{m: %{{:tup, 1} => [true], a: 1, b: %{c: "x"}}}

      attrs = log_attributes(OtlpCodec.encode_logs([attr_signal(:log, meta)]))

      assert {:kvlist, m} = attrs["m"]
      assert m["a"] == {:int, 1}
      assert m["b"] == {:kvlist, %{"c" => {:string, "x"}}}
      # A key is text in every format, at every depth.
      assert m["{:tup, 1}"] == {:array, [{:bool, true}]}
    end

    test "all three encode sites coerce alike" do
      meta = %{m: %{a: 1}, t: {:a, :b}, l: [1, 2, 3]}

      metric = metric_attributes(OtlpCodec.encode_metrics([attr_signal(:metric, meta)]))
      log = log_attributes(OtlpCodec.encode_logs([attr_signal(:log, meta)]))
      span = span_attributes(OtlpCodec.encode_traces([attr_signal(:trace_span, meta)]))

      assert metric == log
      assert log == span
      assert log["m"] == {:kvlist, %{"a" => {:int, 1}}}
      assert log["t"] == {:string, "{:a, :b}"}
      assert log["l"] == {:array, [{:int, 1}, {:int, 2}, {:int, 3}]}
    end

    test "a non-string attribute key is coerced the same way" do
      keys =
        OtlpCodec.encode_logs([attr_signal(:log, %{{:a, :b} => 1, [1, 2, 3] => 2})])
        |> log_attributes()
        |> Map.keys()

      assert Enum.any?(keys, &String.starts_with?(&1, "{"))
      assert Enum.any?(keys, &String.starts_with?(&1, "["))
      refute <<1, 2, 3>> in keys
    end

    test "a binary that is not valid UTF-8 never reaches a protobuf string field" do
      # A protobuf `string` field is defined as UTF-8. The encoded body itself
      # is NOT a UTF-8 assertion — a fixed64 timestamp makes every OTLP body
      # invalid UTF-8, clean or not — so what proves it is the attribute read
      # back out, plus the raw bytes being absent from the body entirely.
      # `bytes_value` would carry them legally and is deliberately not used.
      raw = <<0xFF, 0x00, 0xFE>>
      body = OtlpCodec.encode_logs([attr_signal(:log, %{v: raw})])

      attrs = log_attributes(body)

      assert attrs["v"] == {:string, inspect(raw)}
      assert :binary.match(IO.iodata_to_binary(body), raw) == :nomatch
    end

    test "a nil key names itself and a nil value carries no type" do
      attrs =
        log_attributes(OtlpCodec.encode_logs([attr_signal(:log, %{nil => 1, v: nil})]))

      # A key with no name is not a key; a value with no type is exactly what
      # the empty AnyValue is for, and it is distinguishable on the wire from
      # a zero-length string_value.
      assert attrs["nil"] == {:int, 1}
      assert attrs["v"] == {:empty, nil}
      refute attrs["v"] == {:string, ""}
    end

    test "the encoder represents any term it is handed" do
      meta = %{
        pid: self(),
        ref: make_ref(),
        fun: &OtlpCodec.encode_logs/1,
        struct: %Signal{kind: :log, source: :s, name: :n},
        nested: %{a: [%{b: {1, 2}}]},
        nil_value: nil
      }

      attrs = log_attributes(OtlpCodec.encode_logs([attr_signal(:log, meta)]))

      assert map_size(attrs) == map_size(meta)
    end
  end

  describe "OtlpSink time-trigger flush" do
    setup do
      Application.put_env(
        :flezha,
        :observability_export,
        prometheus_enabled: false,
        prometheus_port: 9568,
        prometheus_bind: "127.0.0.1",
        otlp_enabled: true,
        otlp_endpoint: "http://127.0.0.1:1/v1/metrics",
        otlp_headers: %{},
        otlp_flush_interval_ms: 50,
        otlp_batch_max: 1_000
      )

      {:ok, _client} = OtlpClient.start_link([])
      {:ok, sink} = OtlpSink.start_link([])
      on_exit(fn -> NodeTestHelper.ensure_stopped(sink) end)
      {:ok, sink: sink}
    end

    test "time-trigger flushes drain the buffer when the endpoint is unreachable", %{sink: sink} do
      # Direct GenServer.call routes the test signal in without going
      # through Bus, sidestepping unrelated signal traffic that would
      # also enter the :any/:any sink in this process.
      # The metadata carries the two shapes the Logger bridge attaches to every
      # log signal; encoding either must not raise inside `flush/1`, which would
      # kill the sink before it could drain anything.
      :ok =
        GenServer.call(
          sink,
          {:signal,
           Signal.new(%{
             kind: :metric,
             source: :flush_test,
             name: :hits,
             measurements: %{count: 1},
             metadata: %{logger: %{line: 412}, mfa: {__MODULE__, :probe, 2}}
           })}
        )

      # The periodic flush tick hands the batch to the client and empties the
      # buffer whether or not the endpoint answers.
      assert NodeTestHelper.wait_until(
               fn -> GenServer.call(sink, :stats).buffered == 0 end,
               5_000
             ),
             "the flush tick never drained the buffer"

      # The sink is alive (didn't deadlock under :block) and emitted a
      # :otlp_batch_dropped event for at least one batch — we assert
      # liveness rather than exact buffer count because the :any/:any
      # filter means concurrent Bus traffic can re-fill the buffer.
      assert Process.alive?(sink)
    end
  end

  defp attr_signal(kind, metadata) do
    Signal.new(%{
      kind: kind,
      source: :otlp_attr_test,
      name: :probe,
      measurements: %{count: 1},
      metadata: metadata
    })
  end

  # ---------- OTLP attribute reader ----------
  #
  # Reads the KeyValue attributes back out of an encoded body. Decoding is
  # what makes the list assertion honest: `<<1, 2, 3>>` also occurs inside
  # varints and random span ids, so a substring match on the whole body
  # would not tell a corrupted attribute from a coincidence.

  # ExportLogsServiceRequest{1} > ResourceLogs{2} > ScopeLogs{2} > LogRecord{6}
  defp log_attributes(body), do: body |> pb_nested([1, 2, 2]) |> pb_attributes(6)

  # ExportTraceServiceRequest{1} > ResourceSpans{2} > ScopeSpans{2} > Span{9}
  defp span_attributes(body), do: body |> pb_nested([1, 2, 2]) |> pb_attributes(9)

  # ExportMetricsServiceRequest{1} > ResourceMetrics{2} > ScopeMetrics{2} >
  # Metric > Sum{7} or Gauge{5} > NumberDataPoint{1} > attributes{7}
  defp metric_attributes(body) do
    metric = pb_nested(body, [1, 2, 2])
    # Metric.data is a oneof: Sum{7} for a counter, Gauge{5} otherwise.
    [aggregation] = pb_get(metric, 7) ++ pb_get(metric, 5)

    aggregation |> pb_nested([1]) |> pb_attributes(7)
  end

  defp pb_nested(body, path) do
    Enum.reduce(path, IO.iodata_to_binary(body), fn field, msg -> hd(pb_get(msg, field)) end)
  end

  defp pb_attributes(msg, field), do: Map.new(pb_get(msg, field), &pb_kv/1)

  # KeyValue { key = 1, value = 2 } -> {key, the AnyValue member it set}. The
  # member is the assertion: reading field 1 alone cannot tell a typed value
  # from a value that was stringified.
  defp pb_kv(kv) do
    [key] = pb_get(kv, 1)
    {key, pb_any_value(hd(pb_get(kv, 2)))}
  end

  # AnyValue { string=1, bool=2, int=3, double=4, array=5, kvlist=6, bytes=7 }
  defp pb_any_value(<<>>), do: {:empty, nil}

  defp pb_any_value(msg) do
    case pb_fields(msg) do
      [{1, v}] -> {:string, v}
      [{2, v}] -> {:bool, v == 1}
      [{3, v}] -> {:int, pb_signed64(v)}
      [{4, <<v::little-float-64>>}] -> {:double, v}
      [{5, v}] -> {:array, Enum.map(pb_get(v, 1), &pb_any_value/1)}
      [{6, v}] -> {:kvlist, Map.new(pb_get(v, 1), &pb_kv/1)}
    end
  end

  defp pb_signed64(v) when v >= 0x8000_0000_0000_0000, do: v - 0x1_0000_0000_0000_0000
  defp pb_signed64(v), do: v

  defp pb_get(msg, field), do: for({^field, value} <- pb_fields(msg), do: value)

  defp pb_fields(<<>>), do: []

  defp pb_fields(msg) do
    {tag, rest} = pb_varint(msg)

    {value, tail} =
      case tag &&& 0x7 do
        0 ->
          pb_varint(rest)

        1 ->
          <<value::binary-size(8), tail::binary>> = rest
          {value, tail}

        2 ->
          {len, body} = pb_varint(rest)
          <<value::binary-size(^len), tail::binary>> = body
          {value, tail}
      end

    [{tag >>> 3, value} | pb_fields(tail)]
  end

  defp pb_varint(msg), do: pb_varint(msg, 0, 0)

  defp pb_varint(<<0::1, byte::7, rest::binary>>, acc, shift),
    do: {acc ||| byte <<< shift, rest}

  defp pb_varint(<<1::1, byte::7, rest::binary>>, acc, shift),
    do: pb_varint(rest, acc ||| byte <<< shift, shift + 7)
end
