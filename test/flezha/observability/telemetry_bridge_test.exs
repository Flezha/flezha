defmodule Flezha.Observability.TelemetryBridgeTest do
  # async: false: writes Application env; a shared on-disk root; the observability registry.
  use ExUnit.Case, async: false

  import FlezhaTest.Helper.MessageTestHelper, only: [flush_all: 0]
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Observability.{Registry, Signal, TelemetryBridge}
  alias Flezha.Observability.TestSinks.Counting
  alias Flezha.Relay.{Calls, Lifecycle, Overwatch}
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
    flush_all()

    :telemetry.execute(
      [:flezha, :state, :wal, :backpressure],
      %{bytes: 1024},
      %{relay: "r1"}
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
    assert signal.metadata[:relay] == "r1"
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
    flush_all()

    :telemetry.execute(
      [:flezha, :plugin, :websocket, :resubscribe],
      %{data_silence_ms: 120_000},
      %{plugin_id: "wss://feed.example.com"}
    )

    signal =
      receive_until(fn s -> s.source == :flezha_ws and s.name == :resubscribe end, 500)

    assert signal
    assert signal.measurements[:data_silence_ms] == 120_000
    assert signal.metadata[:plugin_id] == "wss://feed.example.com"
  end

  test "persistence stall/gap/compaction events are bridged" do
    flush_all()

    :telemetry.execute(
      [:flezha, :state, :output_stall],
      %{stalled_ms: 60_000, last_seq: 100},
      %{relay: "btc_feed", tasklet: "ohlc", key: "1m"}
    )

    stall =
      receive_until(fn s -> s.source == :flezha_state and s.name == :output_stall end, 500)

    assert stall
    assert stall.measurements[:stalled_ms] == 60_000
    assert stall.measurements[:last_seq] == 100
    assert stall.metadata[:relay] == "btc_feed"

    :telemetry.execute([:flezha, :state, :seq_gap], %{expected: 101, got: 137}, %{relay: "r"})
    gap = receive_until(fn s -> s.source == :flezha_state and s.name == :seq_gap end, 500)
    assert gap
    assert gap.measurements[:expected] == 101
    assert gap.measurements[:got] == 137

    :telemetry.execute([:flezha, :state, :compaction_timeout], %{waited_ms: 5_000}, %{
      relay: "r"
    })

    compaction =
      receive_until(fn s -> s.source == :flezha_state and s.name == :compaction_timeout end, 500)

    assert compaction
    assert compaction.measurements[:waited_ms] == 5_000

    :telemetry.execute(
      [:flezha, :state, :stale_writer_refused],
      %{count: 1},
      %{relay: "r", tasklet: "s", key: "k", session: "sess_old"}
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
    flush_all()

    :telemetry.execute(
      [:flezha, :state, :input_log_syncer_died],
      %{count: 1},
      %{relay: "btc_feed", reason: :killed}
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
      plugin_id: "wss://feed.example.com"
    })

    shipped = receive_until(fn s -> s.source == :flezha_ws and s.name == :shipped end, 500)

    assert shipped
    assert shipped.kind == :metric
    assert shipped.measurements[:count] == 3
  end

  test "a runner drop, its lifting and a refused retry reach :flezha_runner" do
    sink = :"obs_tbridge_runner_sink_#{:erlang.unique_integer([:positive])}"
    ref = :counters.new(1, [:atomics])

    {:ok, _pid} =
      Registry.register_sink(%{
        name: sink,
        module: Counting,
        config: %{counter_ref: ref, parent: self(), sources: [:flezha_runner]}
      })

    on_exit(fn -> Registry.unregister_sink(sink) end)
    flush_all()

    for {event, name} <- [
          dropped: :runner_dropped,
          readmitted: :runner_readmitted,
          drop_refused: :runner_drop_refused
        ] do
      :telemetry.execute([:flezha, :runner, event], %{count: 1}, %{
        namespace: "ns",
        stable_id: "stable-1",
        pid: 4242,
        node: node()
      })

      signal = receive_until(fn s -> s.source == :flezha_runner and s.name == name end, 500)

      assert signal, "#{name} never reached the bus"
      assert signal.kind == :event
      assert signal.metadata[:namespace] == "ns"
      assert signal.metadata[:stable_id] == "stable-1"
    end
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
    flush_all()

    :telemetry.execute(
      [:flezha, :convergence, :relay_stuck],
      %{count: 1},
      %{relay_id: "btc_feed", detail: :wal_frozen, cell: "ohlc/1m"}
    )

    stuck =
      receive_until(
        fn s -> s.source == :flezha_health and s.name == :relay_stuck end,
        500
      )

    assert stuck
    assert stuck.kind == :event
    assert stuck.metadata[:detail] == :wal_frozen
    assert stuck.metadata[:relay_id] == "btc_feed"

    :telemetry.execute(
      [:flezha, :convergence, :intent_mismatch],
      %{count: 2},
      %{relays: ["btc_feed", "eth_feed"]}
    )

    mismatch =
      receive_until(
        fn s -> s.source == :flezha_convergence and s.name == :intent_mismatch end,
        500
      )

    assert mismatch
    assert mismatch.measurements[:count] == 2
    assert mismatch.metadata[:relays] == ["btc_feed", "eth_feed"]
  end

  test "a relay park reaches the bus from the real lifecycle call site" do
    park_sink = :"obs_tbridge_park_sink_#{:erlang.unique_integer([:positive])}"
    ref = :counters.new(1, [:atomics])

    {:ok, _pid} =
      Registry.register_sink(%{
        name: park_sink,
        module: Counting,
        config: %{counter_ref: ref, parent: self(), sources: [:flezha_relay]}
      })

    on_exit(fn -> Registry.unregister_sink(park_sink) end)
    flush_all()

    reason = {:plugin_prepare_failed, "exchange_sink", {:provider_refused, "pkg", "sink", :bad}}
    Lifecycle.park(%Overwatch{id: "obs_park_relay", namespace: "default"}, reason)

    parked =
      receive_until(
        fn s -> s.source == :flezha_relay and s.name == :relay_parked_total end,
        500
      )

    assert parked
    assert parked.kind == :metric
    assert parked.measurements[:count] == 1
    assert parked.metadata[:relay_id] == "obs_park_relay"
    assert parked.metadata[:plugin_id] == "exchange_sink"
    assert parked.metadata[:reason] == reason
  end

  test "a call's five events reach the bus from the table's own call sites" do
    call_sink = :"obs_tbridge_call_sink_#{:erlang.unique_integer([:positive])}"
    ref = :counters.new(1, [:atomics])

    {:ok, _pid} =
      Registry.register_sink(%{
        name: call_sink,
        module: Counting,
        config: %{counter_ref: ref, parent: self(), sources: [:flezha_call]}
      })

    on_exit(fn -> Registry.unregister_sink(call_sink) end)
    flush_all()

    callee = %{namespace: "default", relay_id: "obs_call_relay"}
    entry = %{"channel" => "in"}
    {:ok, answered, calls} = Calls.admit(%Calls{}, callee, {self(), make_ref()}, entry, 5_000)
    {:ok, waiting, calls} = Calls.admit(calls, callee, {self(), make_ref()}, entry, 5_000)
    {:answered, calls, _} = Calls.divert(calls, callee, "a", %{"flz.reply_to" => answered})
    :ok = Calls.unclaimed(callee)
    _ = Calls.expire(calls, callee, waiting)
    _ = Calls.refused(callee, "call.busy", %{pending_max: 1})
    deadline_at = Integer.to_string(System.system_time(:millisecond))

    stamps = %{
      "flz.call" => "id:pricing",
      "flz.call_response" => "r",
      "flz.deadline_at" => deadline_at
    }

    {_id, row} = Calls.request("o1", stamps, false)
    :ok = Calls.recovered_unanswered(callee, row)

    for {name, measurement} <- [
          flezha_call_answer_duration: :duration_us,
          flezha_call_answers_dropped_total: :count,
          flezha_call_timeouts_total: :count,
          flezha_call_refused_total: :count,
          flezha_call_recovered_unanswered_total: :count
        ] do
      signal = receive_until(fn s -> s.source == :flezha_call and s.name == name end, 500)
      assert signal, "#{name} never reached the bus"
      assert signal.kind == :metric
      assert is_integer(signal.measurements[measurement])
      assert signal.metadata[:relay_id] == "obs_call_relay"
    end
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

    flush_all()

    {:ok, _ref} =
      DlqStore.append(%{
        relay_class: "ObsRelay",
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
    assert parked.metadata[:relay_class] == "ObsRelay"
    assert parked.metadata[:channel] == "input"

    usage = receive_until(fn s -> s.source == :flezha_dlq and s.name == :dlq_usage end, 1_000)
    assert usage
    assert usage.measurements[:entries] == 1
    assert usage.measurements[:bytes] > 0

    # retention_seconds = 0: the sweep drops the just-parked entry, loudly —
    # once the entry's own millisecond has passed (expiry is strictly older).
    [%{"ts_ms" => ts_ms}] = DlqStore.list("ObsRelay")

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
    assert dropped.metadata[:relay_class] == "ObsRelay"
  end

  defmodule AcmeSink do
    @moduledoc false
    use Flezha.Plugin.ProviderBehaviour,
      name: "acme_sink",
      telemetry: [rebalanced: {:event, "The sink moved its partitions."}, paused: :event]
  end

  # A provider compiled against a core whose declaration had no `telemetry`
  # field, as a package built on an earlier patch release is.
  defmodule OldSink do
    @moduledoc false
    def __provider__,
      do: AcmeSink.__provider__() |> Map.delete(:telemetry) |> Map.put(:name, "old_sink")
  end

  # A package whose own verb carries a blank meaning, which its declaration
  # would now refuse at compile time; built here by hand as an older core's
  # compiler would have let it through.
  defmodule BlankSink do
    @moduledoc false
    def __provider__,
      do:
        AcmeSink.__provider__()
        |> Map.put(:telemetry, rebalanced: {:event, " "})
        |> Map.put(:name, "blank_sink")
  end

  describe "the half derived from the provider table" do
    # A package's provider is a row before the bridge starts, as the boot loader
    # adds it; the bridge is restarted here to read the row the same way.
    setup do
      :ok = Flezha.Plugin.Providers.add("acme_sink", AcmeSink)
      restart_bridge()

      on_exit(fn ->
        Flezha.Plugin.Providers.reset_installed()
        restart_bridge()
      end)

      sink = :"obs_tbridge_provider_sink_#{:erlang.unique_integer([:positive])}"

      {:ok, _pid} =
        Registry.register_sink(%{
          name: sink,
          module: Counting,
          config: %{
            counter_ref: :counters.new(1, [:atomics]),
            parent: self(),
            sources: [:flezha_acme_sink, :flezha_http_poll]
          }
        })

      on_exit(fn -> Registry.unregister_sink(sink) end)
      flush_all()
      :ok
    end

    test "a package provider's declared verbs are exported under its name" do
      :telemetry.execute([:flezha, :plugin, :acme_sink, :shipped], %{count: 2}, %{})
      :telemetry.execute([:flezha, :plugin, :acme_sink, :rebalanced], %{}, %{topic: "t"})

      shipped = receive_until(&(&1.source == :flezha_acme_sink and &1.name == :shipped), 500)
      assert shipped.kind == :metric
      assert shipped.measurements[:count] == 2

      rebalanced =
        receive_until(&(&1.source == :flezha_acme_sink and &1.name == :rebalanced), 500)

      assert rebalanced.kind == :event
      assert rebalanced.metadata[:topic] == "t"
      refute inspect(TelemetryBridge.mapping()) =~ "acme_sink"
    end

    test "a package source is registered with an area, a description and its verbs' meanings" do
      {:ok, entry} = Registry.lookup_source(:flezha_acme_sink)

      assert entry.area == :plugins
      assert entry.description == "The acme_sink plugin, installed from a package."

      meanings = Map.new(entry.signals, &{&1.name, &1.description})
      assert meanings[:rebalanced] == "The sink moved its partitions."
      assert meanings[:paused] == "No description from the plugin."

      assert meanings[:shipped] ==
               Keyword.fetch!(Flezha.Plugin.ProviderBehaviour.verb_descriptions(), :shipped)
    end

    test "each derived event has a handler of its own carrying only its line" do
      [handler] = :telemetry.list_handlers([:flezha, :plugin, :acme_sink, :shipped])
      assert handler.config == {:flezha_acme_sink, :shipped, :metric}

      [bridge] = :telemetry.list_handlers([:flezha, :state, :wal, :backpressure])
      assert bridge.id == TelemetryBridge.handler_id()
      assert bridge.config == nil
    end

    test "an own verb the provider does not declare is not exported" do
      :telemetry.execute([:flezha, :plugin, :acme_sink, :undeclared], %{}, %{})
      refute receive_until(&(&1.source == :flezha_acme_sink), 200)
    end

    test "a first-party hand-kept line wins over the derived one" do
      :telemetry.execute([:flezha, :plugin, :http_poll, :shipped], %{count: 1}, %{})

      assert %Signal{kind: :metric} =
               receive_until(&(&1.source == :flezha_http_poll and &1.name == :injected), 500)

      refute receive_until(&(&1.source == :flezha_http_poll and &1.name == :shipped), 100)
    end
  end

  test "a core with no package derives nothing: no source for a first-party provider" do
    Flezha.Plugin.Providers.reset_installed()
    restart_bridge()

    assert TelemetryBridge.provider_mapping() == %{}
    assert Registry.lookup_source(:flezha_std_out) == :error
    assert Registry.lookup_source(:flezha_producer) == :error
  end

  test "a package provider declared before `telemetry:` existed still starts the bridge" do
    :ok = Flezha.Plugin.Providers.add("old_sink", OldSink)

    on_exit(fn ->
      Flezha.Plugin.Providers.reset_installed()
      restart_bridge()
    end)

    restart_bridge()
    assert [_handler] = :telemetry.list_handlers([:flezha, :plugin, :old_sink, :shipped])
  end

  test "a source the registry refuses is logged, not dropped in silence" do
    :ok = Flezha.Plugin.Providers.add("blank_sink", BlankSink)

    on_exit(fn ->
      Flezha.Plugin.Providers.reset_installed()
      restart_bridge()
    end)

    log = ExUnit.CaptureLog.capture_log(fn -> restart_bridge() end)

    assert log =~ "source :flezha_blank_sink not registered"
    assert Registry.lookup_source(:flezha_blank_sink) == :error
  end

  test "an own verb that repeats the vocabulary fails the provider's compilation" do
    source = """
    defmodule FlezhaTest.RepeatsShipped do
      use Flezha.Plugin.ProviderBehaviour, telemetry: [shipped: :metric]
    end
    """

    assert_raise CompileError, ~r/telemetry verb :shipped/, fn -> Code.compile_string(source) end
  end

  defp restart_bridge() do
    sup = Flezha.Observability.Supervisor
    :ok = Supervisor.terminate_child(sup, TelemetryBridge)
    {:ok, _pid} = Supervisor.restart_child(sup, TelemetryBridge)
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
end
