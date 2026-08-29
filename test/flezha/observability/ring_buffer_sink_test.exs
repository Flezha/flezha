defmodule Flezha.Observability.RingBufferSinkTest do
  # async: false: the observability registry.
  use ExUnit.Case, async: false

  alias Flezha.Observability.{Bus, Registry, RingBufferSink, Signal}

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
    Flezha.Observability.TestSinks.settle(RingBufferSink)

    on_exit(fn -> Registry.unregister_source(src) end)
    {:ok, src: src}
  end

  test "insert + query returns inserted signals", %{src: src} do
    Enum.each(1..10, fn i ->
      Bus.emit(Signal.new(%{kind: :event, source: src, name: :n, measurements: %{i: i}}))
    end)

    Flezha.Observability.TestSinks.settle(RingBufferSink)
    results = RingBufferSink.query(src, 0)
    assert length(results) == 10
  end

  test "query with from_ts filters older entries", %{src: src} do
    Bus.emit(Signal.new(%{kind: :event, source: src, name: :first}))
    boundary = System.monotonic_time(:microsecond)
    Bus.emit(Signal.new(%{kind: :event, source: src, name: :second}))

    Flezha.Observability.TestSinks.settle(RingBufferSink)
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

    Flezha.Observability.TestSinks.settle(RingBufferSink)
    assert RingBufferSink.count(src) == initial + 5
  end

  test "query from a non-owner process succeeds", %{src: src} do
    Bus.emit(Signal.new(%{kind: :event, source: src, name: :n}))
    Flezha.Observability.TestSinks.settle(RingBufferSink)

    task =
      Task.async(fn ->
        RingBufferSink.query(src, 0)
      end)

    results = Task.await(task)
    assert is_list(results)
    assert length(results) >= 1
  end
end
