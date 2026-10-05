defmodule Flezha.Load.MemoryDriftTest do
  @moduledoc """
  Long-running load harness that measures Controller BEAM memory
  growth under realistic WebSocket traffic. Drives a relay with a
  configurable WS source (synthetic testnet or a live exchange feed). A
  workload that also publishes each frame through a sink of a plugin package
  runs from that package.

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
    * `MEMORY_DRIFT_WS_SOURCE` — `testnet` | `live`
      (default `testnet`).
    * `MEMORY_DRIFT_WS_INTERVAL_MS` — testnet echo interval in ms
      (default 3, i.e. ~333 msg/sec).
    * `MEMORY_DRIFT_WS_URI` / `MEMORY_DRIFT_WS_SUBSCRIBE` — the live
      feed's endpoint and subscribe message; both required with `live`.
    * `MEMORY_DRIFT_PAYLOAD` — override the testnet echo body (must
      be ≥ 64 B to exercise the refc-binary path).
  """

  # async: false: the live controller and its fleet; registered singletons; node-global tables.

  use ExUnit.Case, async: false

  @moduletag :memory_drift

  if System.get_env("MIX_RUN_MEMORY_DRIFT") != "1" do
    @moduletag :skip
  end

  alias Flezha.Diagnostics.MemoryReporter
  alias Flezha.Plugin
  alias Flezha.Service.Controller.MemTop
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Flezha.Testnet
  alias FlezhaTest.Helper.NodeTestHelper

  @artifact_dir Path.expand("artifacts", __DIR__)

  # The Flezha.Testnet child (test config has testnet_enabled: true) spawns
  # `npm start --prefix test/apps/testnet` on the port the run derives, and
  # `Testnet.ws_port/0` is where both sides read it. The harness waits for the
  # daemon to be reachable before starting the load relay.
  @testnet_wait_timeout_ms 30_000

  @namespace "default"
  @relay_id "memory_drift_load"
  @ws_plugin_id "memory_drift_ws"
  @relay_factory "do_nothing"

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
    ws_source = String.downcase(System.get_env("MEMORY_DRIFT_WS_SOURCE", "testnet"))

    IO.puts(
      "[memory_drift] duration=#{duration_min}m sample=#{sample_interval_ms}ms " <>
        "snapshot=#{snapshot_interval_min}m ws_interval=#{ws_interval_ms}ms " <>
        "ws_source=#{ws_source}"
    )

    if ws_source == "testnet", do: wait_for_ws_endpoint!()
    start_load_relay!(ws_source)
    on_exit(&stop_load_relay/0)

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
    # for "leak vs noise" depends on the workload shape; a user
    # reads the printed rate and the artifacts to judge.
    assert is_number(growth_bytes_per_hour)
  end

  ## ----- bootstrap helpers --------------------------------------------------

  defp wait_for_ws_endpoint! do
    reachable? =
      NodeTestHelper.wait_until(
        fn ->
          case :gen_tcp.connect(~c"localhost", Testnet.ws_port(), [:binary, active: false], 500) do
            {:ok, sock} -> :gen_tcp.close(sock) == :ok
            {:error, _reason} -> false
          end
        end,
        @testnet_wait_timeout_ms,
        250
      )

    unless reachable? do
      flunk(
        "Testnet WS source #{Testnet.ws_uri()} not reachable within " <>
          "#{@testnet_wait_timeout_ms} ms. Verify `testnet_enabled: true` " <>
          "in config/test.exs and that the bundle at " <>
          "test/apps/testnet/dist/index.js exists (run `mix test.setup` " <>
          "if not)."
      )
    end

    :ok
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

  ## ----- relay + plugin wiring ---------------------------------------------

  defp start_load_relay!(ws_source) do
    relay = %Flezha.Relay.Builder{id: @relay_id, factory_id: @relay_factory}
    {:ok, _} = RelayManager.start_relay(@namespace, relay)

    enable_ws_plugin!(ws_source)

    IO.puts("[memory_drift] load relay '#{@relay_id}' up; ws source=#{ws_source}")
  end

  defp enable_ws_plugin!(ws_source) do
    {uri, message} = ws_endpoint(ws_source)

    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(@ws_plugin_id, %Plugin{
        id: @ws_plugin_id,
        namespace: @namespace,
        relay_id: @relay_id,
        provider: Flezha.Plugin.Provider.Websocket,
        opts: %{
          uri: uri,
          post_connect: %{payload: %{"type" => "text", "message" => message}}
        },
        hooks: [{:on_terminate, []}]
      })

    IO.puts("[memory_drift] WS plugin '#{@ws_plugin_id}' → #{uri}")
  end

  defp ws_endpoint("live") do
    {System.fetch_env!("MEMORY_DRIFT_WS_URI"), System.fetch_env!("MEMORY_DRIFT_WS_SUBSCRIBE")}
  end

  defp ws_endpoint(_) do
    msg =
      Testnet.subscribe_message(synthetic_payload(), int_env("MEMORY_DRIFT_WS_INTERVAL_MS", 3))

    {Testnet.ws_uri(), msg}
  end

  defp stop_load_relay do
    for plugin_id <- [@ws_plugin_id] do
      try do
        _ = PluginManager.disable_plugin(@namespace, plugin_id)
      catch
        _, _ -> :ok
      end
    end

    try do
      _ = RelayManager.stop_relay(@namespace, @relay_id)
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
    snap = MemTop.snapshot()
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
