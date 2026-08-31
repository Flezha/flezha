defmodule Flezha.Observability.ExportTest do
  # async: false: writes Application env; the observability registry; registered singletons.
  use ExUnit.Case, async: false

  import Bitwise

  alias Flezha.Observability.{Bus, LoggerHandler, Registry, Signal}
  alias Flezha.Observability.Export.OtlpClient
  alias Flezha.Observability.TestSinks
  alias FlezhaTest.Helper.NodeTestHelper

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

      TestSinks.settle(PrometheusRegistry)

      {body, _series_count} = PrometheusRegistry.render()
      text = assert_valid_exposition(body)

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

      TestSinks.settle(PrometheusRegistry)

      {body, _} = PrometheusRegistry.render()
      text = assert_valid_exposition(body)
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

      TestSinks.settle(PrometheusRegistry)
      {body, _} = PrometheusRegistry.render()
      text = assert_valid_exposition(body)
      assert text =~ "# TYPE " <> Atom.to_string(source) <> "_flag gauge"
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

      TestSinks.settle(PrometheusRegistry)

      {body, _} = PrometheusRegistry.render()
      text = assert_valid_exposition(body)

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

      TestSinks.settle(PrometheusRegistry)
      {body, _} = PrometheusRegistry.render()
      text = assert_valid_exposition(body)

      assert text =~ "__truncated__=\"true\""
      assert PrometheusRegistry.truncation_count(source) > 0
    end
  end

  describe "PrometheusRegistry label coercion" do
    setup do
      start_supervised!(PrometheusRegistry)
      :ok
    end

    test "a tuple metadata value renders instead of killing the registry" do
      # The shape two live TelemetryBridge paths carry today: the WAL writer's
      # 3-tuple entity id, and operative-forward's {:unreachable, _} reason.
      source = metric_source(:wal_tuple)
      registry = Process.whereis(PrometheusRegistry)

      text =
        record_and_render(source, :wal_backpressure, %{value: 1}, %{
          entity_id: {"order_class", "proc_a", "key-1"},
          status: :full
        })

      assert Process.alive?(registry)

      labels = series_labels(text, "#{source}_wal_backpressure")
      assert labels["entity_id"] == ~s({"order_class", "proc_a", "key-1"})
      assert labels["status"] == "full"
    end

    test "the registry owns five ETS tables and a poison signal wipes none of them" do
      # This is the part of the defect an operator feels: `init/1` creates all
      # five named tables in the registry process, so its death took every
      # counter, gauge and histogram since boot with it.
      healthy = metric_source(:healthy_before)
      poison = metric_source(:poison_after)

      record_and_render(healthy, :requests, %{count: 1}, %{path: "/x"})
      record_and_render(poison, :backpressure, %{value: 1}, %{entity_id: {:a, :b, :c}})

      for tab <- [
            :flezha_prom_counters,
            :flezha_prom_gauges,
            :flezha_prom_histograms,
            :flezha_prom_label_index,
            :flezha_prom_registry_counters
          ] do
        assert :ets.whereis(tab) != :undefined, "#{tab} was destroyed"
      end

      {body, _} = PrometheusRegistry.render()
      text = assert_valid_exposition(body)
      assert series_line(text, "#{healthy}_requests") =~ ~s(path="/x")
    end

    test "a list value reads as the list, never as the binary of its bytes" do
      source = metric_source(:list_value)
      text = record_and_render(source, :subject, %{count: 1}, %{l: [1, 2, 3]})

      assert series_line(text, "#{source}_subject") =~ ~s(l="[1, 2, 3]")
      refute String.contains?(text, <<1, 2, 3>>)
    end

    test "a non-UTF-8 binary value leaves the body valid UTF-8" do
      # The Prometheus half of the UTF-8 clause. `prometheus_http.ex` declares
      # charset=utf-8 on the scrape response.
      source = metric_source(:utf8_value)
      raw = <<0xFF, 0x00, 0xFE>>
      text = record_and_render(source, :subject, %{count: 1}, %{v: raw})

      assert String.valid?(text)
      assert :binary.match(text, raw) == :nomatch
      assert series_line(text, "#{source}_subject") =~ ~s(v="<<255, 0, 254>>")
    end

    test "a malformed label name is sanitized, an already-valid one is untouched" do
      dotted = metric_source(:dotted)
      valid = metric_source(:already_valid)

      dotted_text =
        record_and_render(dotted, :subject, %{count: 1}, %{
          "a-b c" => "v",
          "http.status_code" => "200",
          "3bad" => "d",
          "" => "e",
          {:a, :b} => "t"
        })

      line = series_line(dotted_text, "#{dotted}_subject")
      # `http.status_code` -> `http_status_code` is the published OTel mapping.
      assert line =~ ~s(http_status_code="200")
      assert line =~ ~s(a_b_c="v")
      assert line =~ ~s(_3bad="d")
      assert line =~ ~s(_="e")
      assert line =~ ~s(_a___b_="t")
      assert PrometheusRegistry.coercion_count(dotted) == 5

      # The negative control: a sanitizer that rewrote everything, or nothing,
      # would look identical on the cases above.
      valid_text = record_and_render(valid, :subject, %{count: 1}, %{path: "/x", tenant: "t1"})
      assert series_line(valid_text, "#{valid}_subject") =~ ~s({path="/x",tenant="t1"})
      assert PrometheusRegistry.coercion_count(valid) == 0
    end

    test "a leading underscore run collapses so the reserved __ namespace is closed" do
      source = metric_source(:reserved)

      text =
        record_and_render(source, :subject, %{count: 1}, %{"__truncated__" => "forged"})

      line = series_line(text, "#{source}_subject")
      assert line =~ ~s(_truncated__="forged")
      refute line =~ ~s(__truncated__="forged")
    end

    test "an `le` key is dropped on a histogram and kept on a counter" do
      hist = metric_source(:le_hist)
      counter = metric_source(:le_counter)

      hist_text = record_and_render(hist, :subject, %{duration_us: 3_000}, %{le: "collide"})
      bucket = series_line(hist_text, "#{hist}_subject_bucket")

      # `render_histogram_rows/3` appends its own {"le", …}; a second one makes
      # the exposition invalid, which `assert_valid_exposition/1` now catches.
      assert bucket =~ ~s(le=")
      refute bucket =~ "collide"
      assert PrometheusRegistry.coercion_count(hist) == 1

      counter_text = record_and_render(counter, :subject, %{count: 1}, %{le: "kept"})
      assert series_line(counter_text, "#{counter}_subject") =~ ~s(le="kept")
      assert PrometheusRegistry.coercion_count(counter) == 0
    end

    test "two keys that sanitize alike produce one label, not a duplicate" do
      source = metric_source(:collide)

      text =
        record_and_render(source, :subject, %{count: 1}, %{"a-b" => "1", "a.b" => "2", :z => "k"})

      line = series_line(text, "#{source}_subject")
      # Raw pairs are sorted before sanitizing, so "a-b" wins deterministically.
      assert line =~ ~s({a_b="1",z="k"})
      refute line =~ ~s("2")
      assert PrometheusRegistry.coercion_count(source) == 2
    end

    test "coercion_count/1 and truncation_count/1 share a table without colliding" do
      source = metric_source(:both_counters)

      Enum.each(1..10, fn i ->
        PrometheusRegistry.record(
          Signal.new(%{
            kind: :metric,
            source: source,
            name: :requests,
            measurements: %{count: 1},
            # One key needing coercion, one label value driving cardinality.
            metadata: %{"a-b" => "x", tenant: "t#{i}"}
          })
        )
      end)

      TestSinks.settle(PrometheusRegistry)

      # The budget above is 50, so nothing truncates; the bare-`source` key
      # must still read 0 and not pick up the coercion tuple's count.
      assert PrometheusRegistry.coercion_count(source) == 10
      assert PrometheusRegistry.truncation_count(source) == 0
    end
  end

  describe "Export.Supervisor under poison signals" do
    setup do
      prev = Application.get_env(:flezha, :observability_export, [])

      Application.put_env(
        :flezha,
        :observability_export,
        Keyword.merge(prev,
          prometheus_enabled: true,
          prometheus_port: 0,
          prometheus_bind: "127.0.0.1",
          otlp_enabled: false
        )
      )

      on_exit(fn -> Application.put_env(:flezha, :observability_export, prev) end)
      :ok
    end

    test "four poison signals inside the restart window leave every child alive" do
      # Driven, not inferred: four registry crashes in ~0.5 s exceed
      # `Supervisor.init/2`'s default 3-in-5 s and took the listener, the sink
      # and the registry down with the supervisor — export dying exactly when
      # back-pressure means an operator needs it.
      #
      # Started unnamed via the module's own `init/1`, so this is the real
      # child list and the real restart intensity; the app's own
      # Export.Supervisor holds the registered name with zero children.
      # `restart: :temporary` so a death is observable rather than papered
      # over by ExUnit's own supervisor restarting it.
      sup =
        start_supervised!(%{
          id: :export_supervisor_probe,
          start: {Supervisor, :start_link, [Flezha.Observability.Export.Supervisor, []]},
          type: :supervisor,
          restart: :temporary
        })

      ref = Process.monitor(sup)
      source = metric_source(:flap)

      for {status, i} <- Enum.with_index([:full, :open, :full, :open]) do
        PrometheusRegistry.record(
          Signal.new(%{
            kind: :metric,
            source: source,
            name: :wal_backpressure,
            measurements: %{value: i},
            metadata: %{entity_id: {"c", "p", "k#{i}"}, status: status}
          })
        )

        TestSinks.settle(PrometheusRegistry)
      end

      refute_receive {:DOWN, ^ref, :process, ^sup, _reason}, 200

      ids =
        sup
        |> Supervisor.which_children()
        |> Enum.map(fn {id, pid, _, _} -> {id, is_pid(pid) and Process.alive?(pid)} end)
        |> Map.new()

      assert ids[PrometheusRegistry] == true
      assert ids[PrometheusSink] == true
      assert ids[PrometheusHttp] == true
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
      TestSinks.settle(Flezha.Observability.Export.PrometheusSink)
      TestSinks.settle(PrometheusRegistry)

      body = http_get(port, "/metrics")
      assert_valid_exposition(body)
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

  # ---------- 4.1 exposition validity ----------
  #
  # Asserted over the WHOLE rendered body, not a substring: a malformed label
  # name or a duplicate one inside a single `{}` fails the entire Prometheus
  # scrape, so every metric the controller has is lost, not just the bad one.
  # Substring assertions cannot see either.

  @metric_name_re ~r/^[a-zA-Z_:][a-zA-Z0-9_:]*$/
  @label_name_re ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/

  defp assert_valid_exposition(body) do
    text = IO.iodata_to_binary(body)
    assert String.valid?(text), "the exposition body is not valid UTF-8"

    for line <- String.split(text, "\n", trim: true), not String.starts_with?(line, "#") do
      {name, labels} = parse_series(line)

      assert Regex.match?(@metric_name_re, name),
             "malformed metric name #{inspect(name)} in #{inspect(line)}"

      keys = Enum.map(labels, fn {k, _v} -> k end)

      for k <- keys do
        assert Regex.match?(@label_name_re, k),
               "malformed label name #{inspect(k)} in #{inspect(line)}"
      end

      assert Enum.uniq(keys) == keys,
             "duplicate label name in #{inspect(line)}"
    end

    text
  end

  defp parse_series(line) do
    case String.split(line, "{", parts: 2) do
      [head] ->
        [name | _] = String.split(head, " ", parts: 2)
        {name, []}

      [name, rest] ->
        {name, parse_labels(rest)}
    end
  end

  defp parse_labels("}" <> _), do: []

  defp parse_labels(rest) do
    [key, ~s(") <> quoted] = String.split(rest, "=", parts: 2)
    {value, tail} = scan_quoted(quoted, "")

    case tail do
      "," <> more -> [{key, value} | parse_labels(more)]
      "}" <> _ -> [{key, value}]
    end
  end

  defp scan_quoted("\\" <> <<c::utf8, rest::binary>>, acc),
    do: scan_quoted(rest, acc <> <<c::utf8>>)

  defp scan_quoted(~s(") <> rest, acc), do: {acc, rest}

  defp scan_quoted(<<c::utf8, rest::binary>>, acc), do: scan_quoted(rest, acc <> <<c::utf8>>)

  defp metric_source(prefix) do
    source = :"#{prefix}_#{:erlang.unique_integer([:positive])}"

    Registry.register_source(%{
      name: source,
      kinds: [:metric],
      flavour: :declarative,
      cardinality_budget: 50
    })

    source
  end

  defp record_and_render(source, name, measurements, metadata) do
    PrometheusRegistry.record(
      Signal.new(%{
        kind: :metric,
        source: source,
        name: name,
        measurements: measurements,
        metadata: metadata
      })
    )

    TestSinks.settle(PrometheusRegistry)
    {body, _} = PrometheusRegistry.render()
    assert_valid_exposition(body)
  end

  defp series_line(text, prefix) do
    text
    |> String.split("\n", trim: true)
    |> Enum.find(&(String.starts_with?(&1, prefix) and not String.starts_with?(&1, "#")))
  end

  # The rendered value is escaped (`escape_label_value/1`); this reads it back.
  defp series_labels(text, prefix) do
    {_name, labels} = text |> series_line(prefix) |> parse_series()
    Map.new(labels)
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
