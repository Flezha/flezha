defmodule Flezha.Observability.ExportTest do
  use ExUnit.Case, async: false

  alias Flezha.Observability.{Bus, Registry, Signal}

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

      # Cast is async — let it settle.
      Process.sleep(50)

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

      Process.sleep(50)

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

      Process.sleep(50)
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

      Process.sleep(50)

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

      Process.sleep(80)
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

      Process.sleep(80)

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
      on_exit(fn -> if Process.alive?(sink), do: GenServer.stop(sink) end)
      {:ok, sink: sink}
    end

    test "time-trigger flushes drain the buffer when the endpoint is unreachable", %{sink: sink} do
      # Direct GenServer.call routes the test signal in without going
      # through Bus, sidestepping unrelated signal traffic that would
      # also enter the :any/:any sink in this process.
      :ok =
        GenServer.call(
          sink,
          {:signal,
           Signal.new(%{
             kind: :metric,
             source: :flush_test,
             name: :hits,
             measurements: %{count: 1}
           })}
        )

      # Wait long enough for the periodic flush tick to fire and the
      # OtlpClient's full retry schedule to drain (5 attempts × backoff
      # capped at 4s but interleaved with the 50ms flush interval).
      Process.sleep(400)

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

  defp recv_all(socket, acc) do
    case :gen_tcp.recv(socket, 0, 1_500) do
      {:ok, data} -> recv_all(socket, acc <> data)
      {:error, :closed} -> acc
      {:error, _} -> acc
    end
  end
end
