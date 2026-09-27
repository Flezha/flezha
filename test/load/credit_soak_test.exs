defmodule Flezha.Load.CreditSoakTest do
  @moduledoc """
  Credit-grant flow-control soak against the 06-21 mock exchange load.

  Recreates the incident shape: the mock exchange WS feed drives a
  state-persisting operator at ≥1100 msg/s sustained, against a
  deliberately slowed fsync path (long batcher tick + periodic injected
  fsync stalls). Acceptance:

    * flat controller memory (linear-fit growth below the failure-class
      threshold — the 06-21 OOM grew GBs/hour under this load),
    * zero sheds / zero credit overruns / zero credit anomaly events,
    * zero data loss (no seq gaps; persisted throughput tracks the
      offered load),
    * park/resume cycling visible in counters (the SDK's `credit_parked`
      self-report advances across multiple stall cycles).

  Triple-locked from default CI, mirroring `memory_drift_test.exs`:

    1. `:credit_soak` ExUnit tag (excluded in `test/test_helper.exs`).
    2. `MIX_RUN_CREDIT_SOAK=1` env var.
    3. `MIX_ENV=test`.

  Example invocation (10-minute short run; double the duration on a
  clean pass per the rapid-soak workflow):

      MIX_RUN_CREDIT_SOAK=1 CREDIT_SOAK_DURATION_MIN=10 \\
        mix test test/load/credit_soak_test.exs --include credit_soak

  Environment variables:

    * `CREDIT_SOAK_DURATION_MIN` — measurement window (default 10).
    * `CREDIT_SOAK_MOCK_DIR` — checkout of the mock exchange feed from the
      `flezha_market` repository, with `npm install` run. Required.
    * `CREDIT_SOAK_BOOK_RATE` — book updates/s per pair (default 600;
      two pairs + trades ≈ 1220 msg/s offered).
  """

  # async: false: writes Application env; the live controller and its fleet; global telemetry handlers.

  use ExUnit.Case, async: false

  @moduletag :credit_soak
  @moduletag timeout: :infinity

  if System.get_env("MIX_RUN_CREDIT_SOAK") != "1" do
    @moduletag :skip
  end

  import FlezhaTest.Helper.CreditTestHelper
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2, ensure_port_dead: 1]

  alias Flezha.Operator.Builder
  alias Flezha.Plugin
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.Metrics
  alias Flezha.State.WalWriter
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "credit_soak"
  @factory "store_input"
  @tasklet "store_input_tasklet"
  @ws_plugin_id "credit_soak_ws"

  @mock_port 8097
  @pairs "XBT/USD,XBT/EUR"
  @subscribe ~s({"event":"subscribe","subscription":{"name":"book","depth":1000},"pair":["XBT/USD","XBT/EUR"]})

  # Slowed fsync: 200 ms batcher tick (40x the 5 ms default) plus a 1.5 s
  # injected full stall every 30 s. The 512 KiB window exhausts ~0.9 s into
  # each stall, parking the SDK until the resume mints credit.
  @fsync_tick_ms 200
  @window_bytes 524_288
  @stall_every_ms 30_000
  @stall_ms 1_500
  @sample_ms 10_000

  test "mock exchange load: flat memory, zero sheds, zero loss, park/resume cycling" do
    duration_min = int_env("CREDIT_SOAK_DURATION_MIN", 10)
    book_rate = int_env("CREDIT_SOAK_BOOK_RATE", 600)

    mock_port = start_mock_exchange!(book_rate)
    on_exit(fn -> ensure_port_dead(mock_port) end)

    prev_fsync = Application.get_env(:flezha, :agent_fsync_interval_ms)
    Application.put_env(:flezha, :agent_fsync_interval_ms, @fsync_tick_ms)
    restart_fsync_batcher()

    on_exit(fn ->
      Application.delete_env(:flezha, :credit_window_bytes)

      if prev_fsync,
        do: Application.put_env(:flezha, :agent_fsync_interval_ms, prev_fsync),
        else: Application.delete_env(:flezha, :agent_fsync_interval_ms)

      restart_fsync_batcher()

      try do
        OperatorManager.stop_all_operators(@namespace)
      catch
        _, _ -> :ok
      end

      try do
        FleetManager.stop_namespace(@namespace)
      catch
        _, _ -> :ok
      end
    end)

    operator_id = "credit_soak_#{System.unique_integer([:positive])}"
    Application.put_env(:flezha, :credit_window_bytes, %{operator_id => @window_bytes})

    # Roomy SDK input mailbox so injected fsync stalls buffer runner-side
    # (park, never drop) instead of overflowing the input edge.
    {exec_port, ce_id} =
      NodeTestHelper.start_runner_port(@namespace,
        transport: :uds,
        env: [
          {~c"FLEZHA_MAILBOX_CAPACITY", ~c"20000"},
          {~c"FLEZHA_DATA_HEARTBEAT_MS", ~c"1000"}
        ]
      )

    on_exit(fn -> ensure_port_dead(exec_port) end)

    anomalies = attach_anomaly_counters()

    builder = %Builder{id: operator_id, factory_id: @factory, init_values: %{}, opts: []}
    assert {:ok, info} = OperatorManager.start_operator(@namespace, builder)
    assert info.runner_id == ce_id

    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(@ws_plugin_id, %Plugin{
        id: @ws_plugin_id,
        namespace: @namespace,
        operator_id: operator_id,
        provider: Flezha.Plugin.Provider.Websocket,
        opts: %{
          uri: "ws://localhost:#{@mock_port}",
          # String keys throughout: `subscribe_frame/1` indexes
          # `post_connect["payload"]["type"|"message"]`.
          post_connect: %{"payload" => %{"type" => "text", "message" => @subscribe}}
        },
        hooks: [{:on_terminate, []}]
      })

    on_exit(fn ->
      try do
        PluginManager.disable_plugin(@ws_plugin_id)
      catch
        _, _ -> :ok
      end
    end)

    # Wait for the feed to flow and the credit handshake to complete.
    cell_id = wait_for_cell(operator_id, @tasklet, 60_000)
    writer = wait_for_writer(cell_id, 10_000)

    assert wait_until(fn -> active_credit_row(ce_id) != nil end, 30_000),
           "credit handshake never completed"

    IO.puts(
      "[credit_soak] up: operator=#{operator_id} window=#{@window_bytes}B " <>
        "fsync_tick=#{@fsync_tick_ms}ms stall=#{@stall_ms}ms/#{@stall_every_ms}ms " <>
        "duration=#{duration_min}m offered≈#{book_rate * 2 + 20} msg/s"
    )

    samples = run_measurement(writer, ce_id, duration_min * 60_000)

    # Stop the feed, then let in-flight input drain fully — proves the
    # parked backlog resumes to completion instead of wedging.
    _ = PluginManager.disable_plugin(@ws_plugin_id)
    assert wait_until(fn -> quiesced?(writer) end, 60_000), "WAL never quiesced after the run"

    first = List.first(samples)
    last = List.last(samples)
    elapsed_s = (last.at_ms - first.at_ms) / 1000
    rate = (last.seq - first.seq) / elapsed_s
    growth_mib_h = linear_fit_bytes_per_hour(samples) / (1024 * 1024)

    parked_deltas =
      samples
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.count(fn [a, b] -> b.parked > a.parked end)

    row = active_credit_row(ce_id)
    shed = Metrics.get(Metrics.for_cell(cell_id), :wal_shed_total)
    counts = anomaly_counts(anomalies)

    IO.puts(
      "[credit_soak] rate=#{Float.round(rate, 1)} msg/s over #{Float.round(elapsed_s / 60, 1)}m; " <>
        "memory fit=#{Float.round(growth_mib_h, 1)} MiB/h; " <>
        "park cycles(sample deltas)=#{parked_deltas}; shed=#{shed} " <>
        "overrun=#{row.credit_overrun_total} anomalies=#{inspect(counts)}"
    )

    assert rate >= 1_100, "persisted throughput #{Float.round(rate, 1)} msg/s below 1100 msg/s"

    # Flat memory: the 06-21 failure class grew at GiB/hour under this load.
    assert growth_mib_h < 150,
           "controller memory grew at #{Float.round(growth_mib_h, 1)} MiB/h — not flat"

    assert shed == 0, "wal_shed_total=#{shed} — a credit-speaking sender must never shed"
    assert row.credit_overrun_total == 0
    assert counts.credit_anomaly == 0
    assert counts.seq_gap == 0, "seq gaps under soak — data-loss/repair cycle engaged"

    assert last.parked > 0, "SDK never parked despite the injected fsync stalls"

    assert parked_deltas >= 2,
           "park/resume cycling not visible: parked counter advanced in only " <>
             "#{parked_deltas} sampling interval(s)"
  end

  ## ----- measurement loop -----------------------------------------------------

  defp run_measurement(writer, ce_id, duration_ms) do
    started = System.monotonic_time(:millisecond)
    do_measure(writer, ce_id, started + duration_ms, started, [])
  end

  defp do_measure(writer, ce_id, deadline, next_stall_at, samples) do
    now = System.monotonic_time(:millisecond)

    if now >= deadline do
      Enum.reverse(samples)
    else
      if now >= next_stall_at, do: inject_fsync_stall()

      sample = %{
        at_ms: now,
        seq: WalWriter.last_persisted_seq(writer),
        mem: :erlang.memory(:total),
        parked: parked_counter(ce_id)
      }

      # The sampling cadence is a timer, so the stall injected above keeps
      # running while the next sample is due.
      Process.send_after(self(), :sample_due, @sample_ms)
      receive(do: (:sample_due -> :ok))

      next_stall =
        if now >= next_stall_at, do: next_stall_at + @stall_every_ms, else: next_stall_at

      do_measure(writer, ce_id, deadline, next_stall, [sample | samples])
    end
  end

  # A full fsync stall: the batcher stops driving `WalWriter.fsync/1`, so no
  # credit is minted until the resume — the sender exhausts its window and
  # parks (spec: "stalled fsync stops minting").
  defp inject_fsync_stall() do
    case GenServer.whereis(Flezha.State.FsyncBatcher) do
      nil ->
        :ok

      pid ->
        :ok = :sys.suspend(pid)
        {:ok, _} = :timer.apply_after(@stall_ms, :sys, :resume, [pid])
    end
  end

  # No persisted progress for two seconds.
  defp quiesced?(writer) do
    a = WalWriter.last_persisted_seq(writer)

    not NodeTestHelper.wait_until(
      fn -> WalWriter.last_persisted_seq(writer) != a end,
      2_000
    )
  end

  ## ----- anomaly telemetry ------------------------------------------------------

  defp attach_anomaly_counters() do
    ref = :ets.new(:credit_soak_anomalies, [:public])
    :ets.insert(ref, [{:credit_anomaly, 0}, {:seq_gap, 0}])
    id = "credit_soak_#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      id,
      [[:flezha, :state, :credit_anomaly], [:flezha, :state, :seq_gap]],
      fn [:flezha, :state, kind], _m, _md, table -> :ets.update_counter(table, kind, 1) end,
      ref
    )

    on_exit(fn -> :telemetry.detach(id) end)
    ref
  end

  defp anomaly_counts(ref) do
    %{
      credit_anomaly: :ets.lookup_element(ref, :credit_anomaly, 2),
      seq_gap: :ets.lookup_element(ref, :seq_gap, 2)
    }
  end

  ## ----- mock exchange -----------------------------------------------------------

  defp start_mock_exchange!(book_rate) do
    dir = System.get_env("CREDIT_SOAK_MOCK_DIR", "")

    unless File.exists?(Path.join(dir, "server.js")) do
      flunk(
        "no mock exchange feed at #{inspect(dir)}: set CREDIT_SOAK_MOCK_DIR to a checkout " <>
          "of the mock exchange feed from the flezha_market repository (npm install run)."
      )
    end

    port =
      Port.open({:spawn_executable, System.find_executable("node")}, [
        :binary,
        :exit_status,
        args: ["server.js"],
        cd: dir,
        env: [
          {~c"PORT", ~c"#{@mock_port}"},
          {~c"PAIRS", String.to_charlist(@pairs)},
          {~c"BOOK_UPDATES_PER_SEC", ~c"#{book_rate}"},
          {~c"TRADES_PER_SEC", ~c"10"}
        ]
      ])

    assert wait_until(fn -> tcp_up?(@mock_port) end, 20_000),
           "mock exchange never came up on :#{@mock_port}"

    port
  end

  defp tcp_up?(port) do
    case :gen_tcp.connect(~c"localhost", port, [:binary, active: false], 500) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        true

      _ ->
        false
    end
  end

  ## ----- misc -----------------------------------------------------------------

  # Least-squares slope of memory-vs-time, scaled to bytes/hour.
  defp linear_fit_bytes_per_hour(samples) when length(samples) < 2, do: 0.0

  defp linear_fit_bytes_per_hour(samples) do
    n = length(samples)
    xs = Enum.map(samples, fn s -> s.at_ms / 3_600_000 end)
    ys = Enum.map(samples, fn s -> s.mem * 1.0 end)
    mean_x = Enum.sum(xs) / n
    mean_y = Enum.sum(ys) / n

    num =
      Enum.zip(xs, ys)
      |> Enum.reduce(0.0, fn {x, y}, acc -> acc + (x - mean_x) * (y - mean_y) end)

    den = Enum.reduce(xs, 0.0, fn x, acc -> acc + (x - mean_x) * (x - mean_x) end)

    if den == 0.0, do: 0.0, else: num / den
  end

  defp int_env(name, default) do
    case System.get_env(name) do
      nil -> default
      val -> String.to_integer(val)
    end
  end
end
