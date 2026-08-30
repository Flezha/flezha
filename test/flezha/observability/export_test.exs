defmodule Flezha.Observability.ExportTest do
  # async: false: writes Application env; the observability registry; registered singletons.
  use ExUnit.Case, async: false

  import Bitwise

  alias Flezha.Observability.{Bus, LoggerHandler, Registry, Signal}
  alias Flezha.Observability.TestSinks

  alias Flezha.Observability.Export.{
    OtlpCodec,
    OtlpSink,
    PrometheusHttp,
    PrometheusRegistry,
    PrometheusSink
  }

  @moduletag :observability_export

  setup do
    on_exit(fn ->
      Registry.unregister_sink(:prometheus)
      Registry.unregister_sink(:otlp)
    end)

    :ok
  end

  describe "PrometheusRegistry" do
    setup do
      start_supervised!(PrometheusRegistry)
      :ok
    end

    test "counter aggregation accumulates the :count delta" do
      source = :"bench_#{:erlang.unique_integer([:positive])}"

      Registry.register_source(%{
        name: source,
        kinds: [:metric],
        flavour: :declarative,
        cardinality_budget: 50
      })

      Enum.each(1..5, fn _ ->
        PrometheusRegistry.record(
          Signal.new(%{
            kind: :metric,
            source: source,
            name: :requests,
            measurements: %{count: 1},
            metadata: %{path: "/x"}
          })
        )
      end)

      Flezha.Observability.TestSinks.settle(PrometheusRegistry)

      {body, _series_count} = PrometheusRegistry.render()
      text = IO.iodata_to_binary(body)

      assert text =~ "_requests"
      assert text =~ ~s(path="/x")
      assert text =~ " 5\n"
    end

    test "gauge replaces value" do
      source = :"gauge_#{:erlang.unique_integer([:positive])}"

      Registry.register_source(%{
        name: source,
        kinds: [:metric],
        flavour: :declarative,
        cardinality_budget: 5
      })

      PrometheusRegistry.record(
        Signal.new(%{
          kind: :metric,
          source: source,
          name: :rss_bytes,
          measurements: %{value: 100}
        })
      )

      PrometheusRegistry.record(
        Signal.new(%{
          kind: :metric,
          source: source,
          name: :rss_bytes,
          measurements: %{value: 200}
        })
      )

      Flezha.Observability.TestSinks.settle(PrometheusRegistry)

      {body, _} = PrometheusRegistry.render()
      text = IO.iodata_to_binary(body)
      assert text =~ "_rss_bytes"
      assert text =~ " 200\n"
      refute text =~ " 100\n"
    end

    test "producer prometheus_type override wins" do
      source = :"override_#{:erlang.unique_integer([:positive])}"

      Registry.register_source(%{
        name: source,
        kinds: [:metric],
        flavour: :declarative,
        cardinality_budget: 5
      })

      PrometheusRegistry.record(
        Signal.new(%{
          kind: :metric,
          source: source,
          name: :flag,
          measurements: %{count: 1},
          metadata: %{prometheus_type: :gauge}
        })
      )

      Flezha.Observability.TestSinks.settle(PrometheusRegistry)
      {body, _} = PrometheusRegistry.render()
      assert IO.iodata_to_binary(body) =~ "# TYPE " <> Atom.to_string(source) <> "_flag gauge"
    end

    test "histogram bucketing" do
      source = :"hist_#{:erlang.unique_integer([:positive])}"

      Registry.register_source(%{
        name: source,
        kinds: [:metric],
        flavour: :declarative,
        cardinality_budget: 5
      })

      Enum.each([100, 500, 1_500], fn us ->
        PrometheusRegistry.record(
          Signal.new(%{
            kind: :metric,
            source: source,
            name: :fsync_tick,
            measurements: %{duration_us: us}
          })
        )
      end)

      Flezha.Observability.TestSinks.settle(PrometheusRegistry)

      {body, _} = PrometheusRegistry.render()
      text = IO.iodata_to_binary(body)

      assert text =~ "# TYPE " <> Atom.to_string(source) <> "_fsync_tick histogram"
      assert text =~ "_fsync_tick_bucket"
      assert text =~ "_fsync_tick_sum"
      assert text =~ "_fsync_tick_count 3"
    end

    test "label-value cardinality truncation produces a __truncated__ series" do
      source = :"trunc_#{:erlang.unique_integer([:positive])}"

      Registry.register_source(%{
        name: source,
        kinds: [:metric],
        flavour: :declarative,
        # Tiny budget so truncation triggers quickly.
        cardinality_budget: 3
      })

      Enum.each(1..10, fn i ->
        PrometheusRegistry.record(
          Signal.new(%{
            kind: :metric,
            source: source,
            name: :requests,
            measurements: %{count: 1},
            metadata: %{tenant: "t#{i}"}
          })
        )
      end)

      Flezha.Observability.TestSinks.settle(PrometheusRegistry)
      {body, _} = PrometheusRegistry.render()
      text = IO.iodata_to_binary(body)

      assert text =~ "__truncated__=\"true\""
      assert PrometheusRegistry.truncation_count(source) > 0
    end
  end

  describe "PrometheusSink + PrometheusHttp end-to-end" do
    setup do
      start_supervised!(PrometheusRegistry)
      start_supervised!(PrometheusSink)
      # Bind to ephemeral port via :prometheus_port: 0
      Application.put_env(
        :flezha,
        :observability_export,
        prometheus_enabled: true,
        prometheus_port: 0,
        prometheus_bind: "127.0.0.1",
        otlp_enabled: false,
        otlp_endpoint: nil,
        otlp_headers: %{},
        otlp_flush_interval_ms: 10_000,
        otlp_batch_max: 512
      )

      {:ok, pid_h} = PrometheusHttp.start_link([])
      {:ok, port} = PrometheusHttp.listen_port()

      on_exit(fn ->
        if Process.alive?(pid_h) do
          GenServer.stop(pid_h, :normal, 1_000)
        end
      end)

      {:ok, port: port}
    end

    test "scrape returns the rendered exposition body", %{port: port} do
      source = :"scrape_e2e_#{:erlang.unique_integer([:positive])}"

      Registry.register_source(%{
        name: source,
        kinds: [:metric],
        flavour: :declarative,
        cardinality_budget: 10
      })

      Enum.each(1..3, fn _ ->
        Bus.emit(
          Signal.new(%{
            kind: :metric,
            source: source,
            name: :scrape_e2e_counter,
            measurements: %{count: 1},
            metadata: %{path: "/x"}
          })
        )
      end)

      # Bus → PrometheusSink → PrometheusRegistry: settle each hop in order.
      Flezha.Observability.TestSinks.settle(Flezha.Observability.Export.PrometheusSink)
      Flezha.Observability.TestSinks.settle(PrometheusRegistry)

      body = http_get(port, "/metrics")
      assert body =~ "_scrape_e2e_counter"
      assert body =~ ~s(path="/x")
    end

    test "GET / returns the index page", %{port: port} do
      body = http_get(port, "/")
      assert body =~ "/metrics"
    end

    test "unknown path returns 404", %{port: port} do
      assert http_status(port, "GET", "/admin") == 404
    end

    test "disallowed method returns 405", %{port: port} do
      assert http_status(port, "POST", "/metrics") == 405
    end
  end

  describe "OtlpCodec" do
    test "encode_metrics produces a parseable protobuf body" do
      signal =
        Signal.new(%{
          kind: :metric,
          source: :bench,
          name: :requests,
          measurements: %{count: 5},
          metadata: %{path: "/x"}
        })

      body = OtlpCodec.encode_metrics([signal])
      bytes = IO.iodata_to_binary(body)
      assert byte_size(bytes) > 0
      # Field 1 wire-type 2 -> tag byte 0x0a
      assert <<10, _len::8, _rest::binary>> = bytes
    end

    test "encode_batch routes signal kinds to the right service request" do
      signals = [
        Signal.new(%{kind: :metric, source: :s, name: :m, measurements: %{count: 1}}),
        Signal.new(%{kind: :log, source: :s, name: :l, measurements: %{}}),
        Signal.new(%{kind: :event, source: :s, name: :e, measurements: %{count: 1}})
      ]

      %{metrics: m, logs: l, traces: t} = OtlpCodec.encode_batch(signals)
      assert m != nil
      # Both :log and :event end up in logs
      assert l != nil
      # No :trace_span signal so traces is nil
      assert t == nil
    end
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

    test "the encoder represents any term it is handed" do
      meta = %{
        pid: self(),
        ref: make_ref(),
        fun: &OtlpCodec.encode_logs/1,
        struct: %Signal{kind: :log, source: :s, name: :n},
        nested: %{a: [%{b: {1, 2}}]}
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

      {:ok, _client} = Flezha.Observability.Export.OtlpClient.start_link([])
      {:ok, sink} = OtlpSink.start_link([])
      on_exit(fn -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(sink) end)
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
      assert FlezhaTest.Helper.NodeTestHelper.wait_until(
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

  defp http_get(port, path) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: :raw, active: false])

    req =
      "GET #{path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"

    :ok = :gen_tcp.send(socket, req)
    body = recv_all(socket, "")
    :gen_tcp.close(socket)

    # Strip headers
    case String.split(body, "\r\n\r\n", parts: 2) do
      [_headers, body_part] -> body_part
      _ -> body
    end
  end

  defp http_status(port, method, path) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: :raw, active: false])

    req =
      "#{method} #{path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"

    :ok = :gen_tcp.send(socket, req)
    body = recv_all(socket, "")
    :gen_tcp.close(socket)

    case body do
      "HTTP/1.1 " <> rest ->
        {n, _} = Integer.parse(rest)
        n

      _ ->
        0
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

  defp recv_all(socket, acc) do
    case :gen_tcp.recv(socket, 0, 1_500) do
      {:ok, data} -> recv_all(socket, acc <> data)
      {:error, :closed} -> acc
      {:error, _} -> acc
    end
  end
end
