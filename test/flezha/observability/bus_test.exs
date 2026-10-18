defmodule Flezha.Observability.BusTest do
  # async: false: the observability registry.
  use ExUnit.Case, async: false

  import FlezhaTest.Helper.MessageTestHelper, only: [collect: 2]
  alias Flezha.Observability.{Bus, Registry, Signal}
  alias Flezha.Observability.TestSinks.{Blocking, Counting, Slow}

  setup do
    src = :"obs_bus_test_src_#{:erlang.unique_integer([:positive])}"

    :ok =
      Registry.register_source(%{
        name: src,
        kinds: [:metric, :event, :log, :trace_span],
        flavour: :declarative,
        cardinality_budget: 10_000
      })

    sinks = []
    on_exit(fn -> Registry.unregister_source(src) end)
    {:ok, src: src, sinks: sinks}
  end

  defp register_counting_sink(name, parent, sources) do
    ref = :counters.new(1, [:atomics])
    config = %{counter_ref: ref, parent: parent}
    config = if sources, do: Map.put(config, :sources, sources), else: config

    {:ok, _pid} =
      Registry.register_sink(%{
        name: name,
        module: Counting,
        config: config
      })

    on_exit(fn -> Registry.unregister_sink(name) end)
    ref
  end

  defp emit_metric(src, metadata \\ %{}) do
    Bus.emit(Signal.new(%{kind: :metric, source: src, name: :n, metadata: metadata}))
  end

  # Two sinks delivering to the test process: one naming `src`, one that names
  # no source and so takes every one.
  defp register_named_and_catch_all(src) do
    for {suffix, sources} <- [named: [src], any: :any] do
      name = :"obs_bus_#{suffix}_#{:erlang.unique_integer([:positive])}"

      :ok =
        Registry.register_external_sink(%{
          name: name,
          module: __MODULE__,
          pid: self(),
          filter: %{kinds: :any, sources: sources}
        })

      on_exit(fn -> Registry.unregister_sink(name) end)
    end
  end

  test "emit_named reaches the sinks naming its source and no catch-all one", %{src: src} do
    register_named_and_catch_all(src)

    assert :ok = Bus.emit_named(Signal.new(%{kind: :event, source: src, name: :n}))

    assert_receive {:signal, %Signal{source: ^src}}, 500
    refute_receive {:signal, _}, 200
  end

  test "emit reaches a catch-all sink for every source", %{src: src} do
    register_named_and_catch_all(src)

    assert :ok = emit_metric(src)

    assert_receive {:signal, _}, 500
    assert_receive {:signal, _}, 500
  end

  test "emit to zero sinks is a no-op", %{src: src} do
    # No sinks registered for this source; emit should not raise.
    assert :ok = emit_metric(src)
  end

  test "emit reaches one sink", %{src: src} do
    name = :"obs_bus_one_sink_#{:erlang.unique_integer([:positive])}"
    ref = register_counting_sink(name, self(), [src])

    emit_metric(src)
    assert_receive {:counted, _}, 500
    assert :counters.get(ref, 1) == 1
  end

  test "emit reaches multiple sinks", %{src: src} do
    name_a = :"obs_bus_multi_a_#{:erlang.unique_integer([:positive])}"
    name_b = :"obs_bus_multi_b_#{:erlang.unique_integer([:positive])}"
    ref_a = register_counting_sink(name_a, self(), [src])
    ref_b = register_counting_sink(name_b, self(), [src])

    emit_metric(src)

    assert_receive {:counted, _}, 500
    assert_receive {:counted, _}, 500
    assert :counters.get(ref_a, 1) == 1
    assert :counters.get(ref_b, 1) == 1
  end

  test "emit at watermark drops with counter increment", %{src: src} do
    name = :"obs_bus_drop_#{:erlang.unique_integer([:positive])}"

    {:ok, _pid} =
      Registry.register_sink(%{
        name: name,
        module: Slow,
        config: %{},
        overflow: :drop,
        mailbox_high_watermark: 2
      })

    on_exit(fn -> Registry.unregister_sink(name) end)

    # Saturate the sink mailbox; only the first 2 land before the
    # watermark trips and subsequent emits are dropped with counter
    # bumps — by the emitter itself, so the counter is current on return.
    Enum.each(1..20, fn _ -> emit_metric(src) end)

    dropped = Registry.sink_counter(name, :dropped)
    assert dropped > 0
  end

  test "emit under :block policy blocks until the sink accepts", %{src: src} do
    name = :"obs_bus_block_#{:erlang.unique_integer([:positive])}"

    {:ok, _pid} =
      Registry.register_sink(%{
        name: name,
        module: Blocking,
        config: %{parent: self()},
        overflow: :block,
        block_timeout_ms: 5_000
      })

    on_exit(fn -> Registry.unregister_sink(name) end)

    emit_metric(src)
    assert_receive {:blocked_received, %Signal{}}, 500
  end

  test "a first budget breach does not hold the registry's own process" do
    sink = :"obs_bus_budget_hold_sink_#{:erlang.unique_integer([:positive])}"

    {:ok, sink_pid} =
      Registry.register_sink(%{
        name: sink,
        module: Blocking,
        config: %{parent: self(), hold: :cardinality_budget_exceeded},
        overflow: :block,
        block_timeout_ms: 2_000
      })

    on_exit(fn ->
      send(sink_pid, :release)
      Registry.unregister_sink(sink)
    end)

    src = :"obs_bus_budget_src_#{:erlang.unique_integer([:positive])}"

    :ok =
      Registry.register_source(%{
        name: src,
        kinds: [:metric],
        flavour: :declarative,
        cardinality_budget: 1
      })

    on_exit(fn -> Registry.unregister_source(src) end)

    # First shape fits the budget of 1 and is delivered normally.
    emit_metric(src, %{a: 1})
    assert_receive {:blocked_received, %Signal{kind: :metric}}, 500

    # Second shape breaches it. The breach event is dispatched to a `:block`
    # sink that never answers, so whichever process emits it is held.
    producer = spawn(fn -> emit_metric(src, %{b: 1}) end)
    on_exit(fn -> Process.exit(producer, :kill) end)

    assert_receive {:blocked_received,
                    %Signal{name: :cardinality_budget_exceeded, metadata: %{source: ^src}}},
                   1_000

    # The held process must not be the Registry: with the sink still holding,
    # a registration from a third process has to answer promptly.
    other = :"obs_bus_budget_other_#{:erlang.unique_integer([:positive])}"

    entry = %{
      name: other,
      kinds: [:metric],
      flavour: :declarative,
      cardinality_budget: 10
    }

    {elapsed_us, result} =
      :timer.tc(fn ->
        try do
          GenServer.call(Registry, {:register_source, entry}, 500)
        catch
          :exit, {:timeout, _} -> :registry_blocked
        end
      end)

    on_exit(fn -> Registry.unregister_source(other) end)
    send(sink_pid, :release)

    assert result == :ok
    assert elapsed_us < 250_000
  end

  test "a disabled :observability_registry suppresses the kind-violation event" do
    src = :"obs_bus_kv_disabled_#{:erlang.unique_integer([:positive])}"
    marker = :"obs_bus_kv_marker_#{:erlang.unique_integer([:positive])}"

    for {name, kinds} <- [{src, [:metric]}, {marker, [:event]}] do
      :ok =
        Registry.register_source(%{
          name: name,
          kinds: kinds,
          flavour: :declarative
        })

      on_exit(fn -> Registry.unregister_source(name) end)
    end

    sink_name = :"obs_bus_kv_disabled_sink_#{:erlang.unique_integer([:positive])}"

    {:ok, _pid} =
      Registry.register_sink(%{
        name: sink_name,
        module: Counting,
        config: %{
          counter_ref: :counters.new(1, [:atomics]),
          parent: self(),
          sources: [:observability_registry, marker]
        }
      })

    on_exit(fn -> Registry.unregister_sink(sink_name) end)

    marker_signal = Signal.new(%{kind: :event, source: marker, name: :marker})
    violating_signal = Signal.new(%{kind: :log, source: src, name: :uh_oh})

    # Enabled: the violation event is delivered — the positive control that
    # makes the refutation below load-bearing.
    Bus.emit(violating_signal)
    Bus.emit(marker_signal)

    assert Enum.any?(drain_counted(500), &(&1.name == :source_kind_violation))

    :ok = Registry.disable_source(:observability_registry)
    on_exit(fn -> Registry.enable_source(:observability_registry) end)

    Bus.emit(violating_signal)
    Bus.emit(marker_signal)

    seen = drain_counted(500)
    assert Enum.any?(seen, &(&1.name == :marker))
    refute Enum.any?(seen, &(&1.name == :source_kind_violation))
  end

  test "kind violation: source emits a kind it did not declare" do
    src = :"obs_bus_kind_violation_#{:erlang.unique_integer([:positive])}"

    :ok =
      Registry.register_source(%{
        name: src,
        kinds: [:metric],
        flavour: :declarative
      })

    on_exit(fn -> Registry.unregister_source(src) end)

    sink_name = :"obs_bus_kv_sink_#{:erlang.unique_integer([:positive])}"
    parent = self()

    {:ok, _pid} =
      Registry.register_sink(%{
        name: sink_name,
        module: Counting,
        config: %{
          counter_ref: :counters.new(1, [:atomics]),
          parent: parent,
          sources: [src]
        }
      })

    on_exit(fn -> Registry.unregister_sink(sink_name) end)

    # Emit a :log signal — the source only declared :metric. The bus
    # SHOULD drop the original and fan-out a :source_kind_violation
    # event on :observability_registry instead.
    Bus.emit(Signal.new(%{kind: :log, source: src, name: :uh_oh}))

    # The Counting sink only accepts signals from the test source, so
    # the violation event on :observability_registry is filtered out
    # — we should receive nothing if the bus correctly dropped the
    # :log emit.
    received = drain_counted(150)
    refute Enum.any?(received, &(&1.kind == :log))
  end

  test "disabled source drops emits silently" do
    src = :"obs_bus_disabled_#{:erlang.unique_integer([:positive])}"

    :ok =
      Registry.register_source(%{
        name: src,
        kinds: [:metric],
        flavour: :declarative
      })

    on_exit(fn -> Registry.unregister_source(src) end)

    sink_name = :"obs_bus_disabled_sink_#{:erlang.unique_integer([:positive])}"

    {:ok, _pid} =
      Registry.register_sink(%{
        name: sink_name,
        module: Counting,
        config: %{
          counter_ref: :counters.new(1, [:atomics]),
          parent: self(),
          sources: [src]
        }
      })

    on_exit(fn -> Registry.unregister_sink(sink_name) end)

    :ok = Registry.disable_source(src)
    Bus.emit(Signal.new(%{kind: :metric, source: src, name: :n}))

    received = drain_counted(100)
    assert received == []
  end

  defp drain_counted(timeout_ms) do
    collect after: timeout_ms do
      {:counted, sig} -> sig
    end
  end
end
