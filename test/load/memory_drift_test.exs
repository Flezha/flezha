defmodule Flezha.Load.MemoryDriftTest do
  @moduledoc """
  Long-running load harness that measures Controller BEAM memory
  growth under realistic WebSocket + Kafka traffic. Drives a operative
  with a configurable WS source (synthetic testnet or live Kraken WS)
  and, optionally, a Kafka producer plugin on `:on_input` so each
  inbound frame is also published.

  Outputs:

    * `mem_top_<isoz>.json` snapshots every `MEMORY_DRIFT_SNAPSHOT_MIN`
      minutes (default 30) into `test/load/artifacts/`.
    * `memory_drift_series_<isoz>.json` time series at the end of the
      run, plus the linear-fit growth rate in MiB/hour printed to
      stdout.

  Triple-locked from default CI:

    1. `:memory_drift` ExUnit tag (excluded in `test/test_helper.exs`).
    2. `MIX_RUN_MEMORY_DRIFT=1` env var (also applies `@moduletag :skip`
       at compile time so `--include memory_drift` alone is silent).
    3. `MIX_ENV=test` (the implicit `mix test` requirement).

  Example invocation:

      MIX_RUN_MEMORY_DRIFT=1 \\
      MEMORY_DRIFT_DURATION_MIN=240 \\
      FLEZHA_MEMORY_REPORT_INTERVAL_MS=10000 \\
        mix test test/load/memory_drift_test.exs

  Environment variables:

    * `MEMORY_DRIFT_DURATION_MIN` — total wall-clock budget (default 240).
    * `MEMORY_DRIFT_SNAPSHOT_MIN` — `mem-top` snapshot cadence
      (default 30).
    * `FLEZHA_MEMORY_REPORT_INTERVAL_MS` — in-process sampling cadence
      (default 10_000).
    * `MEMORY_DRIFT_WORKLOAD` — `ws` | `kafka` | `combined`
      (default `combined`).
    * `MEMORY_DRIFT_WS_SOURCE` — `testnet` | `kraken`
      (default `testnet`).
    * `MEMORY_DRIFT_WS_INTERVAL_MS` — testnet echo interval in ms
      (default 3, i.e. ~333 msg/sec).
    * `MEMORY_DRIFT_WS_URI` / `MEMORY_DRIFT_WS_SUBSCRIBE` — override
      the Kraken endpoint and subscribe message.
    * `MEMORY_DRIFT_PAYLOAD` — override the testnet echo body (must
      be ≥ 64 B to exercise the refc-binary path).
    * `MEMORY_DRIFT_KAFKA_HOST` / `MEMORY_DRIFT_KAFKA_PORT` — broker
      address for the Kafka producer plugin (default `localhost:9092`).
    * `MEMORY_DRIFT_KAFKA_TOPIC` — destination topic
      (default `memory_drift_test`).
  """

  # async: false: the live controller and its fleet; registered singletons; node-global tables.

  use ExUnit.Case, async: false

  @moduletag :memory_drift

  if System.get_env("MIX_RUN_MEMORY_DRIFT") != "1" do
    @moduletag :skip
  end

  alias Flezha.Diagnostics.MemoryReporter
  alias Flezha.Plugin
  alias Flezha.Service.CliManager
  alias Flezha.Service.PluginManager
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.NodeTestHelper

  @artifact_dir Path.expand("artifacts", __DIR__)

  # The Flezha.Testnet child (test config has testnet_enabled: true)
  # spawns `npm start --prefix test/apps/testnet`, which listens on
  # ws://localhost:4500. The harness waits for it to be reachable
  # before starting the load operative.
  @testnet_ws_uri "ws://localhost:4500"
  @testnet_port 4500
  @testnet_wait_timeout_ms 30_000

  @namespace "default"
  @operative_id "memory_drift_load"
  @ws_plugin_id "memory_drift_ws"
  @kafka_plugin_id "memory_drift_kafka"
  @operative_factory "do_nothing"

  # Kraken public WS v1 book (depth) subscription on a fan-out of major
  # pairs. The `book` channel — not `trade` — is what produces sustained
  # throughput on the order of 100-300 msg/sec; one update per order-book
  # mutation per pair.
  @kraken_ws_uri "wss://ws.kraken.com"
  @kraken_pairs ~s(["BTC/USD","ETH/USD","BTC/EUR","ETH/EUR","XRP/USD","SOL/USD","ADA/USD","DOT/USD"])
  @kraken_subscribe_message ~s({"event":"subscribe","subscription":{"name":"book","depth":1000},"pair":) <>
                              @kraken_pairs <> "}"

  @kafka_host System.get_env("MEMORY_DRIFT_KAFKA_HOST", "localhost")
  @kafka_port String.to_integer(System.get_env("MEMORY_DRIFT_KAFKA_PORT", "9092"))

  setup_all do
    File.mkdir_p!(@artifact_dir)
    :ok
  end

  @tag timeout: :infinity
  test "controller memory drift over the configured window" do
    duration_min = int_env("MEMORY_DRIFT_DURATION_MIN", 240)
    sample_interval_ms = int_env("FLEZHA_MEMORY_REPORT_INTERVAL_MS", 10_000)
    snapshot_interval_min = int_env("MEMORY_DRIFT_SNAPSHOT_MIN", 30)
    ws_interval_ms = int_env("MEMORY_DRIFT_WS_INTERVAL_MS", 3)
    workload = String.downcase(System.get_env("MEMORY_DRIFT_WORKLOAD", "combined"))
    ws_source = String.downcase(System.get_env("MEMORY_DRIFT_WS_SOURCE", "testnet"))

    IO.puts(
      "[memory_drift] duration=#{duration_min}m sample=#{sample_interval_ms}ms " <>
        "snapshot=#{snapshot_interval_min}m ws_interval=#{ws_interval_ms}ms " <>
        "workload=#{workload} ws_source=#{ws_source}"
    )

    if ws_source == "testnet", do: wait_for_ws_endpoint!()
    if workload in ["combined", "kafka"], do: assert_kafka_reachable!()

    start_load_operative!(workload, ws_source)
    on_exit(&stop_load_operative/0)

    ensure_reporter_started(sample_interval_ms)

    # In-process sampling: call `MemoryReporter.capture/0` directly on
    # the same cadence and stash each row in an ETS table for the linear
    # fit. Avoids the Elixir Logger ↔ OTP `:logger` translation layer
    # that a log-line parser would have to traverse.
    samples_ref = :ets.new(:memory_drift_samples, [:ordered_set, :public])
    sampler_pid = spawn_link(fn -> sampler_loop(samples_ref, sample_interval_ms) end)
    on_exit(fn -> Process.exit(sampler_pid, :kill) end)

    deadline_ms = System.monotonic_time(:millisecond) + duration_min * 60 * 1000
    capture_loop(deadline_ms, snapshot_interval_min)

    series = collect_series(samples_ref)
    write_artifact("memory_drift_series", series)

    growth_bytes_per_hour = linear_fit_bytes_per_hour(series)
    growth_mib_per_hour = growth_bytes_per_hour / (1024 * 1024)

    IO.puts(
      "[memory_drift] linear-fit growth rate = #{Float.round(growth_mib_per_hour, 2)} Mi/hour"
    )

    # The harness reports — it doesn't assert pass/fail. The threshold
    # for "leak vs noise" depends on the workload shape; an operator
    # reads the printed rate and the artifacts to judge.
    assert is_number(growth_bytes_per_hour)
  end

  ## ----- bootstrap helpers --------------------------------------------------

  defp wait_for_ws_endpoint! do
    reachable? =
      NodeTestHelper.wait_until(
        fn ->
          case :gen_tcp.connect(~c"localhost", @testnet_port, [:binary, active: false], 500) do
            {:ok, sock} -> :gen_tcp.close(sock) == :ok
            {:error, _reason} -> false
          end
        end,
        @testnet_wait_timeout_ms,
        250
      )

    unless reachable? do
      flunk(
        "Testnet WS source #{@testnet_ws_uri} not reachable within " <>
          "#{@testnet_wait_timeout_ms} ms. Verify `testnet_enabled: true` " <>
          "in config/test.exs and that the bundle at " <>
          "test/apps/testnet/dist/index.js exists (run `mix test.setup` " <>
          "if not)."
      )
    end

    :ok
  end

  defp assert_kafka_reachable! do
    case :gen_tcp.connect(
           String.to_charlist(@kafka_host),
           @kafka_port,
           [:binary, active: false],
           1_000
         ) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, reason} ->
        flunk(
          "Kafka broker #{@kafka_host}:#{@kafka_port} not reachable " <>
            "(#{inspect(reason)}). Either bring up a broker or set " <>
            "MEMORY_DRIFT_WORKLOAD=ws to skip the Kafka path."
        )
    end
  end

  defp ensure_reporter_started(sample_interval_ms) do
    # When `FLEZHA_MEMORY_REPORT_INTERVAL_MS` is set, `Flezha.Application`
    # already supervises the reporter at boot — reuse it. Otherwise start
    # a transient instance for the duration of this test.
    case Process.whereis(Flezha.Diagnostics.MemoryReporter) do
      nil ->
        {:ok, _pid} =
          GenServer.start_link(
            Flezha.Diagnostics.MemoryReporter,
            %{interval_ms: sample_interval_ms},
            name: Flezha.Diagnostics.MemoryReporter
          )

      pid when is_pid(pid) ->
        IO.puts("[memory_drift] reporter already supervised (pid=#{inspect(pid)})")
    end
  end

  ## ----- operative + plugin wiring ---------------------------------------------

  defp start_load_operative!(workload, ws_source) do
    operative = %Flezha.Operative.Builder{id: @operative_id, factory_id: @operative_factory}
    {:ok, _} = OperativeManager.start_operative(@namespace, operative)

    if workload in ["ws", "combined"], do: enable_ws_plugin!(ws_source)
    if workload in ["kafka", "combined"], do: enable_kafka_plugin!()

    IO.puts(
      "[memory_drift] load operative '#{@operative_id}' up; " <>
        "ws=#{workload in ["ws", "combined"]} (source=#{ws_source}) " <>
        "kafka=#{workload in ["kafka", "combined"]}"
    )
  end

  defp enable_ws_plugin!(ws_source) do
    {uri, message} = ws_endpoint(ws_source)

    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(@ws_plugin_id, %Plugin{
        id: @ws_plugin_id,
        namespace: @namespace,
        operative_id: @operative_id,
        provider: Flezha.Plugin.Provider.Websocket,
        opts: %{
          uri: uri,
          post_connect: %{payload: %{"type" => "text", "message" => message}}
        },
        subscriptions: [{:on_terminate, []}]
      })

    IO.puts("[memory_drift] WS plugin '#{@ws_plugin_id}' → #{uri}")
  end

  defp enable_kafka_plugin! do
    topic = System.get_env("MEMORY_DRIFT_KAFKA_TOPIC", "memory_drift_test")

    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(@kafka_plugin_id, %Plugin{
        id: @kafka_plugin_id,
        namespace: @namespace,
        operative_id: @operative_id,
        provider: Flezha.Plugin.Provider.Kafka.Producer,
        opts: %{
          hosts: [{@kafka_host, @kafka_port}],
          topic: topic,
          entity: 0,
          linger_ms: 10,
          queue_max: 10_000,
          # Dev brokers default to RF=1; the plugin default of 2 would
          # fail topic auto-creation on a single-broker cluster.
          auto_create_replication_factor: 1,
          auto_create_entities: 1,
          required_acks: -1,
          idempotent: true,
          delivery_timeout_ms: 30_000
        },
        subscriptions: [{:on_input, []}]
      })

    IO.puts("[memory_drift] Kafka producer plugin '#{@kafka_plugin_id}' → topic=#{topic}")
  end

  defp ws_endpoint("kraken") do
    uri = System.get_env("MEMORY_DRIFT_WS_URI", @kraken_ws_uri)
    msg = System.get_env("MEMORY_DRIFT_WS_SUBSCRIBE", @kraken_subscribe_message)
    {uri, msg}
  end

  defp ws_endpoint(_) do
    payload = synthetic_payload()
    interval = int_env("MEMORY_DRIFT_WS_INTERVAL_MS", 3)

    msg =
      Jason.encode!(%{
        event_type: "subscribe",
        event_data: %{default_message: payload, interval: interval}
      })

    {@testnet_ws_uri, msg}
  end

  defp stop_load_operative do
    for plugin_id <- [@ws_plugin_id, @kafka_plugin_id] do
      try do
        _ = PluginManager.disable_plugin(plugin_id)
      catch
        _, _ -> :ok
      end
    end

    try do
      _ = OperativeManager.stop_operative(@namespace, @operative_id)
    catch
      _, _ -> :ok
    end
  end

  ## ----- synthetic payload --------------------------------------------------

  # ~1 KiB book-update-shaped frame. Static so the testnet's legacy echo
  # mode can repeat it verbatim, and > 64 B so each delivered frame lands
  # on the refc-binary path rather than staying as a private heap binary.
  defp synthetic_payload do
    case System.get_env("MEMORY_DRIFT_PAYLOAD") do
      raw when is_binary(raw) and byte_size(raw) >= 64 ->
        raw

      _ ->
        bids = book_levels(99_000, -5, :bid, 10)
        asks = book_levels(99_500, 5, :ask, 10)

        Jason.encode!(%{
          channel: "book",
          type: "update",
          data: [
            %{
              symbol: "BTC/USD",
              bids: bids,
              asks: asks,
              checksum: 1_234_567_890,
              timestamp: "1970-01-01T00:00:00.000000Z"
            }
          ]
        })
    end
  end

  defp book_levels(base_price, step, side, count) do
    Enum.map(1..count, fn i ->
      [
        "#{base_price + i * step}.#{rem(:erlang.phash2(i), 100)}",
        "#{rem(:erlang.phash2({i, side}), 1000)}.#{i}",
        "0.#{i}"
      ]
    end)
  end

  ## ----- sampling + capture loop -------------------------------------------

  defp sampler_loop(samples_ref, interval_ms) do
    {:ok, _} = :timer.send_interval(interval_ms, :sample)
    sample_on_tick(samples_ref)
  end

  defp sample_on_tick(samples_ref) do
    reading = MemoryReporter.capture()
    :ets.insert(samples_ref, {System.system_time(:millisecond), reading})
    receive(do: (:sample -> sample_on_tick(samples_ref)))
  end

  defp capture_loop(deadline_ms, snapshot_interval_min) do
    snapshot_interval_ms = snapshot_interval_min * 60 * 1000
    next_snapshot = System.monotonic_time(:millisecond) + snapshot_interval_ms
    do_capture_loop(deadline_ms, next_snapshot, snapshot_interval_ms)
  end

  defp do_capture_loop(deadline_ms, next_snapshot, snapshot_interval_ms) do
    now = System.monotonic_time(:millisecond)

    cond do
      now >= deadline_ms ->
        capture_mem_top_snapshot()
        :ok

      now >= next_snapshot ->
        capture_mem_top_snapshot()
        do_capture_loop(deadline_ms, now + snapshot_interval_ms, snapshot_interval_ms)

      true ->
        # A timer to the earlier of the two deadlines, not a sleep: the loop
        # is driven by its own schedule.
        Process.send_after(self(), :capture_due, min(deadline_ms - now, next_snapshot - now))
        receive(do: (:capture_due -> :ok))
        do_capture_loop(deadline_ms, next_snapshot, snapshot_interval_ms)
    end
  end

  defp capture_mem_top_snapshot do
    snap = CliManager.build_mem_top_snapshot()
    write_artifact("mem_top", snap)
  end

  defp write_artifact(prefix, payload) do
    iso = DateTime.utc_now() |> DateTime.to_iso8601() |> String.replace(":", "-")
    path = Path.join(@artifact_dir, "#{prefix}_#{iso}.json")
    File.write!(path, Jason.encode!(payload, pretty: true))
    IO.puts("[memory_drift] wrote #{path}")
  end

  defp collect_series(samples_ref) do
    :ets.tab2list(samples_ref)
    |> Enum.sort_by(fn {ts, _} -> ts end)
    |> Enum.map(fn {ts, sample} ->
      %{
        timestamp_ms: ts,
        erlang_memory_total: sample.erlang_memory_total,
        erlang_memory_binary: sample.erlang_memory_binary,
        erlang_memory_processes: sample.erlang_memory_processes,
        erlang_memory_ets: sample.erlang_memory_ets,
        process_count: sample.process_count,
        port_count: sample.port_count,
        binary_alloc_carriers_size: sample.binary_alloc_carriers_size
      }
    end)
  end

  # Linear least-squares fit of `erlang_memory_total` vs time, returning
  # slope in bytes/hour. Timestamps are shifted by the first sample's
  # value so the regression operates on small floats — otherwise
  # `n * sum_xx` and `sum_x * sum_x` are both ~1e25 and their difference
  # loses precision to zero.
  defp linear_fit_bytes_per_hour([]), do: 0.0
  defp linear_fit_bytes_per_hour([_only_one]), do: 0.0

  defp linear_fit_bytes_per_hour(series) do
    t0 = hd(series).timestamp_ms

    {sum_x, sum_y, sum_xx, sum_xy, n} =
      Enum.reduce(series, {0.0, 0.0, 0.0, 0.0, 0}, fn point, {sx, sy, sxx, sxy, count} ->
        x = (point.timestamp_ms - t0) / 1.0
        y = point.erlang_memory_total / 1.0
        {sx + x, sy + y, sxx + x * x, sxy + x * y, count + 1}
      end)

    denom = n * sum_xx - sum_x * sum_x

    slope_per_ms =
      if denom == 0.0, do: 0.0, else: (n * sum_xy - sum_x * sum_y) / denom

    slope_per_ms * 1000 * 60 * 60
  end

  ## ----- misc helpers -------------------------------------------------------

  defp int_env(name, default) do
    case System.get_env(name) do
      nil ->
        default

      raw ->
        case Integer.parse(raw) do
          {n, ""} when n > 0 -> n
          _ -> default
        end
    end
  end
end
