defmodule Flezha.Observability.IsolationTest do
  # async: false: the observability registry.
  use ExUnit.Case, async: false

  alias Flezha.Observability.{Bus, Registry, Signal}
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
    Flezha.Observability.TestSinks.settle(fast_name)

    fast_count = :counters.get(fast_ref, 1)
    assert fast_count == 1_000

    slow_dropped = Registry.sink_counter(slow_name, :dropped)

    assert slow_dropped > 0,
           "slow sink should have dropped some signals once its mailbox hit watermark"
  end
end
