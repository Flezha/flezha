defmodule Flezha.Observability.BusTest do
  # async: false: the observability registry.
  use ExUnit.Case, async: false

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
    receive do
      {:counted, sig} -> [sig | drain_counted(timeout_ms)]
    after
      timeout_ms -> []
    end
  end
end
