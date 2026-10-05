defmodule Flezha.Observability.PrometheusExportTest do
  # async: false: writes Application env; the observability registry; registered singletons.
  use FlezhaTest.Case, fixtures: [:env], async: false

  alias Flezha.Observability.Bus
  alias Flezha.Observability.Export.PrometheusHttp
  alias Flezha.Observability.Export.PrometheusRegistry
  alias Flezha.Observability.Export.PrometheusSink
  alias Flezha.Observability.Registry
  alias Flezha.Observability.Signal
  alias Flezha.Observability.TestSinks

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
      # 3-tuple cell id, and relay-forward's {:unreachable, _} reason.
      source = metric_source(:wal_tuple)
      registry = Process.whereis(PrometheusRegistry)

      text =
        record_and_render(source, :wal_backpressure, %{value: 1}, %{
          cell_id: {"order_class", "task_a", "key-1"},
          status: :full
        })

      assert Process.alive?(registry)

      labels = series_labels(text, "#{source}_wal_backpressure")
      assert labels["cell_id"] == ~s({"order_class", "task_a", "key-1"})
      assert labels["status"] == "full"
    end

    test "the registry owns five ETS tables and a poison signal wipes none of them" do
      # This is the part of the defect a user feels: `init/1` creates all
      # five named tables in the registry process, so its death took every
      # counter, gauge and histogram since boot with it.
      healthy = metric_source(:healthy_before)
      poison = metric_source(:poison_after)

      record_and_render(healthy, :requests, %{count: 1}, %{path: "/x"})
      record_and_render(poison, :backpressure, %{value: 1}, %{cell_id: {:a, :b, :c}})

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
      Application.put_env(
        :flezha,
        :observability_export,
        Keyword.merge(Application.get_env(:flezha, :observability_export, []),
          prometheus_enabled: true,
          prometheus_port: 0,
          prometheus_bind: "127.0.0.1",
          otlp_enabled: false
        )
      )

      :ok
    end

    test "four poison signals inside the restart window leave every child alive" do
      # Driven, not inferred: four registry crashes in ~0.5 s exceed
      # `Supervisor.init/2`'s default 3-in-5 s and took the listener, the sink
      # and the registry down with the supervisor — export dying exactly when
      # back-pressure means a user needs it.
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
            metadata: %{cell_id: {"c", "p", "k#{i}"}, status: status}
          })
        )

        TestSinks.settle(PrometheusRegistry)
      end

      refute_receive {:DOWN, ^ref, :process, ^sup, _reason}, 100

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

  defp recv_all(socket, acc) do
    case :gen_tcp.recv(socket, 0, 1_500) do
      {:ok, data} -> recv_all(socket, acc <> data)
      {:error, :closed} -> acc
      {:error, _} -> acc
    end
  end
end
