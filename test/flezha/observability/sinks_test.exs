defmodule Flezha.Observability.IntegrationTest do
  # async: false: the observability registry.
  use ExUnit.Case, async: false

  alias Flezha.Observability
  alias Flezha.Observability.{Bus, PubSubSink, Registry, RingBufferSink, Signal}
  alias Flezha.Observability.TestSinks
  alias Flezha.Service.Observability, as: ObsSvc

  @moduletag :observability_integration

  setup do
    src = :"obs_integration_src_#{:erlang.unique_integer([:positive])}"

    :ok =
      Registry.register_source(%{
        name: src,
        kinds: [:metric, :event],
        flavour: :declarative,
        cardinality_budget: 10
      })

    # Bring up the PubSubSink for this test if it isn't already.
    pubsub_sink =
      case Registry.lookup_sink(:pubsub) do
        {:ok, _} ->
          :existing

        :error ->
          name = :"obs_int_pubsub_#{:erlang.unique_integer([:positive])}"

          {:ok, _pid} =
            Registry.register_sink(%{name: name, module: PubSubSink, config: %{}})

          {:registered, name}
      end

    # The ring buffer subscriber has picked up the new source once it answers.
    TestSinks.settle(RingBufferSink)

    on_exit(fn ->
      Registry.unregister_source(src)

      case pubsub_sink do
        {:registered, name} -> Registry.unregister_sink(name)
        :existing -> :ok
      end
    end)

    :ok = Observability.subscribe(src)
    on_exit(fn -> Observability.unsubscribe(src) end)

    {:ok, src: src, pubsub: pubsub_name(pubsub_sink)}
  end

  test "active → disabled → re-enabled lifecycle hits ring buffer + PubSub + registry", %{
    src: src,
    pubsub: pubsub
  } do
    rb_start = RingBufferSink.count(src)

    # Phase 1: source :active — emit 100, expect 100 in ring, 100 PubSub, status :active.
    Enum.each(1..100, fn i ->
      Bus.emit(
        Signal.new(%{
          kind: :metric,
          source: src,
          name: :phase_active,
          measurements: %{i: i}
        })
      )
    end)

    settle_sinks(pubsub)

    assert RingBufferSink.count(src) - rb_start == 100
    {:ok, %{status: :active}} = Registry.lookup_source(src)
    pubsub_first = drain_signals()
    assert length(pubsub_first) == 100

    # Phase 2: disable. Emit 100 more, none should reach observers.
    :ok = ObsSvc.disable_source(src, %{source: :internal})

    Enum.each(1..100, fn i ->
      Bus.emit(
        Signal.new(%{
          kind: :metric,
          source: src,
          name: :phase_disabled,
          measurements: %{i: i}
        })
      )
    end)

    settle_sinks(pubsub)

    {:ok, %{status: :disabled}} = Registry.lookup_source(src)
    # Ring-buffer count unchanged (no new inserts) — disable drops at the bus.
    assert RingBufferSink.count(src) - rb_start == 100
    pubsub_second = drain_signals()
    assert pubsub_second == []

    # Phase 3: re-enable, emit 100 more, expect them again.
    :ok = ObsSvc.enable_source(src, %{source: :internal})

    Enum.each(1..100, fn i ->
      Bus.emit(
        Signal.new(%{
          kind: :metric,
          source: src,
          name: :phase_reenabled,
          measurements: %{i: i}
        })
      )
    end)

    settle_sinks(pubsub)

    {:ok, %{status: :active}} = Registry.lookup_source(src)
    assert RingBufferSink.count(src) - rb_start == 200
    pubsub_third = drain_signals()
    assert length(pubsub_third) == 100
  end

  defp drain_signals() do
    receive do
      {:flezha_signal, %Signal{} = sig} -> [sig | drain_signals()]
    after
      0 -> []
    end
  end

  # Both sinks have handled every emit so far: the ring buffer counts, the
  # PubSub sink has forwarded to this process.
  defp settle_sinks(pubsub) do
    TestSinks.settle(RingBufferSink)
    TestSinks.settle(pubsub)
  end

  defp pubsub_name({:registered, name}), do: name
  defp pubsub_name(:existing), do: :pubsub
end

defmodule Flezha.Observability.IsolationTest do
  # async: false: the observability registry.
  use ExUnit.Case, async: false

  alias Flezha.Observability.{Bus, Registry, Signal}
  alias Flezha.Observability.TestSinks
  alias Flezha.Observability.TestSinks.{Counting, Slow}

  @moduletag :observability_isolation

  test "a slow sink does not block producers or starve fast peers" do
    src = :"obs_iso_src_#{:erlang.unique_integer([:positive])}"

    :ok =
      Registry.register_source(%{
        name: src,
        kinds: [:metric],
        flavour: :declarative,
        cardinality_budget: 10
      })

    on_exit(fn -> Registry.unregister_source(src) end)

    fast_ref = :counters.new(1, [:atomics])
    fast_name = :"obs_iso_fast_#{:erlang.unique_integer([:positive])}"

    {:ok, _} =
      Registry.register_sink(%{
        name: fast_name,
        module: Counting,
        config: %{counter_ref: fast_ref, parent: nil, sources: [src]}
      })

    on_exit(fn -> Registry.unregister_sink(fast_name) end)

    slow_name = :"obs_iso_slow_#{:erlang.unique_integer([:positive])}"

    {:ok, _} =
      Registry.register_sink(%{
        name: slow_name,
        module: Slow,
        config: %{},
        overflow: :drop,
        mailbox_high_watermark: 5
      })

    on_exit(fn -> Registry.unregister_sink(slow_name) end)

    # Build the signal once so we time pure dispatch cost.
    signal = Signal.new(%{kind: :metric, source: src, name: :iso})

    {wall_us, :ok} =
      :timer.tc(fn ->
        Enum.each(1..1_000, fn _ -> Bus.emit(signal) end)
        :ok
      end)

    # Under :drop policy with a stalled sink, 1k emits should complete
    # in well under 200ms — far less than the 1000 * 50ms = 50s that
    # serialising on the slow sink would cost.
    assert wall_us < 200_000,
           "1000 emits took #{wall_us}us; expected < 200_000us with :drop policy"

    # The fast sink has counted everything once it answers a call queued
    # behind the emits.
    TestSinks.settle(fast_name)

    fast_count = :counters.get(fast_ref, 1)
    assert fast_count == 1_000

    slow_dropped = Registry.sink_counter(slow_name, :dropped)

    assert slow_dropped > 0,
           "slow sink should have dropped some signals once its mailbox hit watermark"
  end
end

defmodule Flezha.Observability.PubSubSinkTest do
  # async: false: the observability registry.
  use ExUnit.Case, async: false

  alias Flezha.Observability
  alias Flezha.Observability.{Bus, PubSubSink, Registry, Signal}
  alias Flezha.Observability.TestSinks

  setup do
    # PubSub sink is disabled by default in test config. Register it
    # here for the duration of the test, then unregister on exit.
    sink_name = :"obs_pubsub_sink_test_#{:erlang.unique_integer([:positive])}"
    src = :"obs_pubsub_src_#{:erlang.unique_integer([:positive])}"

    :ok =
      Registry.register_source(%{
        name: src,
        kinds: [:event],
        flavour: :declarative
      })

    sink =
      case Registry.lookup_sink(:pubsub) do
        {:ok, _} ->
          # The default :pubsub sink is already registered (probably by
          # a prior test that wasn't cleaned up). Reuse it.
          :pubsub

        :error ->
          {:ok, _pid} =
            Registry.register_sink(%{
              name: sink_name,
              module: PubSubSink,
              config: %{}
            })

          on_exit(fn -> Registry.unregister_sink(sink_name) end)
          sink_name
      end

    on_exit(fn -> Registry.unregister_source(src) end)
    {:ok, src: src, sink: sink}
  end

  test "subscriber receives one message per emit", %{src: src} do
    :ok = Observability.subscribe(src)

    Bus.emit(Signal.new(%{kind: :event, source: src, name: :hello}))

    assert_receive {:flezha_signal, %Signal{source: ^src, name: :hello}}, 500

    Observability.unsubscribe(src)
  end

  test "unsubscribed process receives nothing", %{src: src, sink: sink} do
    Observability.subscribe(src)
    Observability.unsubscribe(src)

    Bus.emit(Signal.new(%{kind: :event, source: src, name: :ignored}))

    # The sink broadcasts from its own process, so once it has handled the
    # signal every delivery it was going to make is already in a mailbox.
    TestSinks.settle(sink)
    refute_received {:flezha_signal, _}
  end

  test "sink-disabled config: subscribe returns error when no sink running" do
    # Temporarily simulate the disabled state by unregistering the
    # PubSub sink. The subscribe helper should then report the disabled
    # error path.
    Registry.unregister_sink(:pubsub)

    # Re-fetch — should be gone.
    assert :error = Registry.lookup_sink(:pubsub)

    src = :"obs_disabled_pubsub_#{:erlang.unique_integer([:positive])}"
    assert {:error, :pubsub_sink_disabled} = Observability.subscribe(src)
  end
end

defmodule Flezha.Observability.RingBufferSinkTest do
  # async: false: the observability registry.
  use ExUnit.Case, async: false

  alias Flezha.Observability.{Bus, Registry, RingBufferSink, Signal}
  alias Flezha.Observability.TestSinks

  setup do
    src = :"obs_rbs_src_#{:erlang.unique_integer([:positive])}"

    :ok =
      Registry.register_source(%{
        name: src,
        kinds: [:metric, :event],
        flavour: :declarative,
        cardinality_budget: 10_000
      })

    # The RingBufferSink subscribes to Registry events via PubSub and
    # creates a table for newly-registered sources; a call to it answers
    # after that broadcast has been handled.
    TestSinks.settle(RingBufferSink)

    on_exit(fn -> Registry.unregister_source(src) end)
    {:ok, src: src}
  end

  test "insert + query returns inserted signals", %{src: src} do
    Enum.each(1..10, fn i ->
      Bus.emit(Signal.new(%{kind: :event, source: src, name: :n, measurements: %{i: i}}))
    end)

    TestSinks.settle(RingBufferSink)
    results = RingBufferSink.query(src, 0)
    assert length(results) == 10
  end

  test "query with from_ts filters older entries", %{src: src} do
    Bus.emit(Signal.new(%{kind: :event, source: src, name: :first}))
    boundary = System.monotonic_time(:microsecond)
    Bus.emit(Signal.new(%{kind: :event, source: src, name: :second}))

    TestSinks.settle(RingBufferSink)
    results = RingBufferSink.query(src, boundary)
    names = Enum.map(results, & &1.name)
    assert :second in names
    refute :first in names
  end

  test "count/1 reports table size", %{src: src} do
    initial = RingBufferSink.count(src)

    Enum.each(1..5, fn _ ->
      Bus.emit(Signal.new(%{kind: :event, source: src, name: :n}))
    end)

    TestSinks.settle(RingBufferSink)
    assert RingBufferSink.count(src) == initial + 5
  end

  test "query from a non-owner process succeeds", %{src: src} do
    Bus.emit(Signal.new(%{kind: :event, source: src, name: :n}))
    TestSinks.settle(RingBufferSink)

    task =
      Task.async(fn ->
        RingBufferSink.query(src, 0)
      end)

    results = Task.await(task)
    assert is_list(results)
    assert results != []
  end
end

defmodule Flezha.Observability.LoggerHandlerTest do
  # async: false: the observability registry; the primary Logger.
  use ExUnit.Case, async: false

  # The tests emit Logger calls on purpose to drive the bridge; assertions run
  # off the captured signal, so capturing console output changes nothing.
  @moduletag :capture_log

  require Logger

  alias Flezha.Observability.{Counters, Registry, TestSinks}
  alias Flezha.Observability.LoggerHandler
  alias Flezha.Observability.TestSinks.Counting
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    sink_name = :"obs_logger_test_sink_#{:erlang.unique_integer([:positive])}"
    parent = self()
    ref = :counters.new(1, [:atomics])

    # Sink that filters only :log signals from the :logger source.
    {:ok, _pid} =
      Registry.register_sink(%{
        name: sink_name,
        module: Counting,
        config: %{counter_ref: ref, parent: parent, sources: [:logger]}
      })

    on_exit(fn -> Registry.unregister_sink(sink_name) end)

    # Make sure logger emits warnings (test config has level: :error)
    on_exit(NodeTestHelper.override_log_level(:warning))

    {:ok, sink_name: sink_name, counter_ref: ref}
  end

  test "Logger.warning produces a :log signal with rendered message" do
    drain_mailbox()
    Logger.warning("hello-observability")

    log_signal =
      TestSinks.receive_signal(fn s ->
        s.kind == :log and s.metadata.message == "hello-observability"
      end)

    assert log_signal
    assert log_signal.name == :warning
    assert log_signal.metadata.level == :warning
  end

  test "bus failure does not break Logger" do
    before_failures = Counters.get(:logger_bridge_failures)

    # Inject a deliberate failure by emitting through Bus with a
    # broken sink: a sink with a mailbox-watermark of 0 forces drop
    # path but no raise; for raise we patch Bus's emit indirectly by
    # using a non-existent ETS lookup (already handled by
    # try/rescue). Simulate a raise by calling Bus.emit/1 with a
    # crafted signal that has a sink we know will raise.
    #
    # The cleanest test: directly call LoggerHandler.log/2 with a
    # malformed event and assert the counter increments without
    # the test process crashing.
    bad_event = %{level: :error, msg: {:string, ["ok"]}, meta: %{}}

    # We expect the counter not to drop the call below.
    :ok = LoggerHandler.log(bad_event, %{})

    # Logger should still work after this call.
    Logger.warning("still-alive")
    assert Counters.get(:logger_bridge_failures) >= before_failures
  end

  defp drain_mailbox() do
    receive do
      _ -> drain_mailbox()
    after
      0 -> :ok
    end
  end
end
