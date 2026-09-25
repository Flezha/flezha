defmodule Flezha.Observability.TelemetryBridgeTest do
  # async: false: writes Application env; a shared on-disk root; the observability registry.
  use ExUnit.Case, async: false

  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Observability.{Registry, Signal, TelemetryBridge}
  alias Flezha.Observability.TestSinks.Counting
  alias Flezha.Operator.{Lifecycle, Overwatch}
  alias FlezhaTest.Helper.DlqFixtures
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    sink_name = :"obs_tbridge_test_sink_#{:erlang.unique_integer([:positive])}"
    parent = self()
    ref = :counters.new(1, [:atomics])

    {:ok, _pid} =
      Registry.register_sink(%{
        name: sink_name,
        module: Counting,
        config: %{counter_ref: ref, parent: parent, sources: [:flezha_state]}
      })

    on_exit(fn -> Registry.unregister_sink(sink_name) end)
    {:ok, sink_name: sink_name}
  end

  test "bridged event becomes a signal" do
    drain_mailbox()

    :telemetry.execute(
      [:flezha, :state, :wal, :backpressure],
      %{bytes: 1024},
      %{operator: "r1"}
    )

    signal =
      receive_until(
        fn s ->
          s.source == :flezha_state and s.name == :wal_backpressure
        end,
        500
      )

    assert signal
    assert signal.kind == :metric
    assert signal.measurements[:bytes] == 1024
    assert signal.metadata[:operator] == "r1"
  end

  test "websocket re-subscribe event is bridged" do
    ws_sink = :"obs_tbridge_ws_sink_#{:erlang.unique_integer([:positive])}"
    ref = :counters.new(1, [:atomics])

    {:ok, _pid} =
      Registry.register_sink(%{
        name: ws_sink,
        module: Counting,
        config: %{counter_ref: ref, parent: self(), sources: [:flezha_ws]}
      })

    on_exit(fn -> Registry.unregister_sink(ws_sink) end)
    drain_mailbox()

    :telemetry.execute(
      [:flezha, :plugin, :websocket, :resubscribe],
      %{data_silence_ms: 120_000},
      %{plugin_id: "wss://ws.kraken.com"}
    )

    signal =
      receive_until(fn s -> s.source == :flezha_ws and s.name == :resubscribe end, 500)

    assert signal
    assert signal.measurements[:data_silence_ms] == 120_000
    assert signal.metadata[:plugin_id] == "wss://ws.kraken.com"
  end

  test "persistence stall/gap/compaction events are bridged" do
    drain_mailbox()

    :telemetry.execute(
      [:flezha, :state, :output_stall],
      %{stalled_ms: 60_000, last_seq: 100},
      %{operator: "btc_kraken", tasklet: "ohlc", key: "1m"}
    )

    stall =
      receive_until(fn s -> s.source == :flezha_state and s.name == :output_stall end, 500)

    assert stall
    assert stall.measurements[:stalled_ms] == 60_000
    assert stall.measurements[:last_seq] == 100
    assert stall.metadata[:operator] == "btc_kraken"

    :telemetry.execute([:flezha, :state, :seq_gap], %{expected: 101, got: 137}, %{operator: "r"})
    gap = receive_until(fn s -> s.source == :flezha_state and s.name == :seq_gap end, 500)
    assert gap
    assert gap.measurements[:expected] == 101
    assert gap.measurements[:got] == 137

    :telemetry.execute([:flezha, :state, :compaction_timeout], %{waited_ms: 5_000}, %{
      operator: "r"
    })

    compaction =
      receive_until(fn s -> s.source == :flezha_state and s.name == :compaction_timeout end, 500)

    assert compaction
    assert compaction.measurements[:waited_ms] == 5_000

    :telemetry.execute(
      [:flezha, :state, :stale_writer_refused],
      %{count: 1},
      %{operator: "r", tasklet: "s", key: "k", session: "sess_old"}
    )

    refused =
      receive_until(
        fn s -> s.source == :flezha_state and s.name == :stale_writer_refused end,
        500
      )

    assert refused
    assert refused.kind == :event
    assert refused.metadata[:session] == "sess_old"
  end

  test "a newly bridged detection verdict and a provider verb both reach the bus" do
    sink = :"obs_tbridge_verdict_sink_#{:erlang.unique_integer([:positive])}"
    ref = :counters.new(1, [:atomics])

    {:ok, _pid} =
      Registry.register_sink(%{
        name: sink,
        module: Counting,
        config: %{counter_ref: ref, parent: self(), sources: [:flezha_state, :flezha_ws]}
      })

    on_exit(fn -> Registry.unregister_sink(sink) end)
    drain_mailbox()

    :telemetry.execute(
      [:flezha, :state, :input_log_syncer_died],
      %{count: 1},
      %{operator: "btc_kraken", reason: :killed}
    )

    died =
      receive_until(
        fn s -> s.source == :flezha_state and s.name == :input_log_syncer_died end,
        500
      )

    assert died
    assert died.kind == :event
    assert died.metadata[:reason] == :killed

    :telemetry.execute([:flezha, :plugin, :websocket, :shipped], %{count: 3}, %{
      plugin_id: "wss://ws.kraken.com"
    })

    shipped = receive_until(fn s -> s.source == :flezha_ws and s.name == :shipped end, 500)

    assert shipped
    assert shipped.kind == :metric
    assert shipped.measurements[:count] == 3
  end

  test "all bridged events have a mapping" do
    # Sanity check — every event in mapping/0 maps to {source, name, kind}.
    Enum.each(TelemetryBridge.mapping(), fn {event, {source, name, kind}} ->
      assert is_list(event)
      assert is_atom(source)
      assert is_atom(name)
      assert kind in [:metric, :event, :log, :trace_span]
    end)
  end

  test "convergence stuck / intent-mismatch verdicts are bridged" do
    conv_sink = :"obs_tbridge_convergence_sink_#{:erlang.unique_integer([:positive])}"
    ref = :counters.new(1, [:atomics])

    {:ok, _pid} =
      Registry.register_sink(%{
        name: conv_sink,
        module: Counting,
        config: %{
          counter_ref: ref,
          parent: self(),
          sources: [:flezha_convergence, :flezha_health]
        }
      })

    on_exit(fn -> Registry.unregister_sink(conv_sink) end)
    drain_mailbox()

    :telemetry.execute(
      [:flezha, :convergence, :operator_stuck],
      %{count: 1},
      %{operator_id: "btc_kraken", detail: :wal_frozen, cell: "ohlc/1m"}
    )

    stuck =
      receive_until(
        fn s -> s.source == :flezha_health and s.name == :operator_stuck end,
        500
      )

    assert stuck
    assert stuck.kind == :event
    assert stuck.metadata[:detail] == :wal_frozen
    assert stuck.metadata[:operator_id] == "btc_kraken"

    :telemetry.execute(
      [:flezha, :convergence, :intent_mismatch],
      %{count: 2},
      %{operators: ["btc_kraken", "eth_kraken"]}
    )

    mismatch =
      receive_until(
        fn s -> s.source == :flezha_convergence and s.name == :intent_mismatch end,
        500
      )

    assert mismatch
    assert mismatch.measurements[:count] == 2
    assert mismatch.metadata[:operators] == ["btc_kraken", "eth_kraken"]
  end

  test "an operator park reaches the bus from the real lifecycle call site" do
    park_sink = :"obs_tbridge_park_sink_#{:erlang.unique_integer([:positive])}"
    ref = :counters.new(1, [:atomics])

    {:ok, _pid} =
      Registry.register_sink(%{
        name: park_sink,
        module: Counting,
        config: %{counter_ref: ref, parent: self(), sources: [:flezha_operator]}
      })

    on_exit(fn -> Registry.unregister_sink(park_sink) end)
    drain_mailbox()

    reason = {:plugin_prepare_failed, "kraken_sink", {:module_not_loaded, MyOrg.Uninstalled}}

    Lifecycle.park(
      %Overwatch{id: "obs_park_operator", namespace: "default"},
      reason
    )

    parked =
      receive_until(
        fn s -> s.source == :flezha_operator and s.name == :operator_parked_total end,
        500
      )

    assert parked
    assert parked.kind == :metric
    assert parked.measurements[:count] == 1
    assert parked.metadata[:operator_id] == "obs_park_operator"
    assert parked.metadata[:plugin_id] == "kraken_sink"
    assert parked.metadata[:reason] == reason
  end

  test "DLQ park / retention / usage counters reach the bus through the real store" do
    dlq_sink = :"obs_tbridge_dlq_sink_#{:erlang.unique_integer([:positive])}"
    ref = :counters.new(1, [:atomics])

    {:ok, _pid} =
      Registry.register_sink(%{
        name: dlq_sink,
        module: Counting,
        config: %{counter_ref: ref, parent: self(), sources: [:flezha_dlq]}
      })

    DlqFixtures.isolated_dir(retention_seconds: 0)
    on_exit(fn -> Registry.unregister_sink(dlq_sink) end)

    drain_mailbox()

    {:ok, _ref} =
      DlqStore.append(%{
        operator_class: "ObsOperator",
        tasklet_id: "task",
        cell_key: "k",
        channel: "input",
        origin_offset: nil,
        error: "boom",
        attempts: 3,
        payload: "poison"
      })

    parked =
      receive_until(fn s -> s.source == :flezha_dlq and s.name == :dlq_parked_total end, 1_000)

    assert parked
    assert parked.kind == :metric
    assert parked.measurements[:count] == 1
    assert parked.metadata[:operator_class] == "ObsOperator"
    assert parked.metadata[:channel] == "input"

    usage = receive_until(fn s -> s.source == :flezha_dlq and s.name == :dlq_usage end, 1_000)
    assert usage
    assert usage.measurements[:entries] == 1
    assert usage.measurements[:bytes] > 0

    # retention_seconds = 0: the sweep drops the just-parked entry, loudly —
    # once the entry's own millisecond has passed (expiry is strictly older).
    [%{"ts_ms" => ts_ms}] = DlqStore.list("ObsOperator")

    assert NodeTestHelper.wait_until(
             fn -> System.system_time(:millisecond) > ts_ms end,
             1_000,
             1
           )

    :ok = DlqStore.sweep()

    dropped =
      receive_until(
        fn s -> s.source == :flezha_dlq and s.name == :dlq_dropped_by_retention_total end,
        1_000
      )

    assert dropped
    assert dropped.measurements[:count] == 1
    assert dropped.measurements[:bytes] > 0
    assert dropped.metadata[:reason] == :age
    assert dropped.metadata[:operator_class] == "ObsOperator"
  end

  defp receive_until(_pred, timeout) when timeout <= 0, do: nil

  defp receive_until(pred, timeout) do
    start = System.monotonic_time(:millisecond)

    receive do
      {:counted, %Signal{} = sig} ->
        if pred.(sig) do
          sig
        else
          elapsed = System.monotonic_time(:millisecond) - start
          receive_until(pred, timeout - elapsed)
        end
    after
      timeout -> nil
    end
  end

  defp drain_mailbox() do
    receive do
      _ -> drain_mailbox()
    after
      0 -> :ok
    end
  end
end
