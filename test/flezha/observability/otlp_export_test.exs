defmodule Flezha.Observability.OtlpExportTest do
  # async: false: writes Application env; the observability registry; registered singletons.
  use ExUnit.Case, async: false

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

    test "a signal from the real Logger bridge encodes, map and tuple readable" do
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

      assert attrs["logger"] =~ "mfa:"
      assert attrs["logger"] =~ "412"
      assert String.starts_with?(attrs["mfa"], "{")
      assert attrs["mfa"] =~ "probe"
      assert attrs["message"] == message

      # `file:` is a charlist, and `to_string/1` rendered it indistinguishable
      # from a binary carrying the same text.
      assert attrs["file"] =~ "lib/probe.ex"
      refute attrs["file"] == "lib/probe.ex"
    end

    test "a list attribute reads as the list, not the binary of its bytes" do
      attrs = log_attributes(OtlpCodec.encode_logs([attr_signal(:log, %{l: [1, 2, 3]})]))

      refute attrs["l"] == <<1, 2, 3>>
      assert String.starts_with?(attrs["l"], "[")
      assert attrs["l"] =~ "3"
    end

    test "a binary attribute is unchanged" do
      attrs = log_attributes(OtlpCodec.encode_logs([attr_signal(:log, %{path: "/x"})]))

      assert attrs["path"] == "/x"
    end

    test "all three encode sites coerce alike" do
      meta = %{m: %{a: 1}, t: {:a, :b}, l: [1, 2, 3]}

      metric = metric_attributes(OtlpCodec.encode_metrics([attr_signal(:metric, meta)]))
      log = log_attributes(OtlpCodec.encode_logs([attr_signal(:log, meta)]))
      span = span_attributes(OtlpCodec.encode_traces([attr_signal(:trace_span, meta)]))

      assert metric == log
      assert log == span
      assert String.starts_with?(log["m"], "%{")
      assert String.starts_with?(log["t"], "{")
      refute log["l"] == <<1, 2, 3>>
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
      raw = <<0xFF, 0x00, 0xFE>>
      body = OtlpCodec.encode_logs([attr_signal(:log, %{v: raw})])

      attrs = log_attributes(body)

      assert attrs["v"] == inspect(raw)
      assert String.valid?(attrs["v"])
      assert :binary.match(IO.iodata_to_binary(body), raw) == :nomatch
    end

    test "the encoder represents any term it is handed" do
      meta = %{
        pid: self(),
        ref: make_ref(),
        fun: &OtlpCodec.encode_logs/1,
        struct: %Signal{kind: :log, source: :s, name: :n},
        nested: %{a: [%{b: {1, 2}}]},
        # `to_string(nil)` is "", which the `&1 != ""` assertion below catches:
        # an attribute that names nothing is not an encodable attribute.
        nil_value: nil
      }

      attrs = log_attributes(OtlpCodec.encode_logs([attr_signal(:log, meta)]))

      assert map_size(attrs) == map_size(meta)
      assert Enum.all?(Map.values(attrs), &(is_binary(&1) and &1 != ""))
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
      # log signal: encoding them used to raise inside `flush/1` and kill the
      # sink before it could drain anything.
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

  defp pb_attributes(msg, field) do
    Map.new(pb_get(msg, field), fn kv ->
      [key] = pb_get(kv, 1)
      [value] = kv |> pb_get(2) |> hd() |> pb_get(1)
      {key, value}
    end)
  end

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
