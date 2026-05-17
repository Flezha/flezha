defmodule Flezha.Observability.TelemetryBridgeTest do
  use ExUnit.Case, async: false

  alias Flezha.Observability.{Registry, Signal, TelemetryBridge}
  alias Flezha.Observability.TestSinks.Counting

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

  test "all bridged events have a mapping" do
    # Sanity check — every event in mapping/0 maps to {source, name, kind}.
    Enum.each(TelemetryBridge.mapping(), fn {event, {source, name, kind}} ->
      assert is_list(event)
      assert is_atom(source)
      assert is_atom(name)
      assert kind in [:metric, :event, :log, :trace_span]
    end)
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
