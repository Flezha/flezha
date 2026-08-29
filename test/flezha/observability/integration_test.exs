defmodule Flezha.Observability.IntegrationTest do
  # async: false: the observability registry.
  use ExUnit.Case, async: false

  alias Flezha.Observability
  alias Flezha.Observability.{Bus, PubSubSink, Registry, RingBufferSink, Signal}
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
    Flezha.Observability.TestSinks.settle(RingBufferSink)

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
    Flezha.Observability.TestSinks.settle(RingBufferSink)
    Flezha.Observability.TestSinks.settle(pubsub)
  end

  defp pubsub_name({:registered, name}), do: name
  defp pubsub_name(:existing), do: :pubsub
end
