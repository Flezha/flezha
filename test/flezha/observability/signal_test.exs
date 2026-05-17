defmodule Flezha.Observability.SignalTest do
  use ExUnit.Case, async: true

  alias Flezha.Observability.Signal

  describe "new/1" do
    test "constructs a metric signal and auto-fills ts" do
      signal =
        Signal.new(%{
          kind: :metric,
          source: :s,
          name: :n,
          measurements: %{count: 1, bytes: 16_384},
          metadata: %{relay: "r", shard: 3}
        })

      assert %Signal{kind: :metric, source: :s, name: :n} = signal
      assert signal.measurements == %{count: 1, bytes: 16_384}
      assert signal.metadata == %{relay: "r", shard: 3}
      assert is_integer(signal.ts)
    end

    test "accepts an explicit ts" do
      signal = Signal.new(%{kind: :event, source: :s, name: :n, ts: 42})
      assert signal.ts == 42
    end

    test "rejects unknown kind with ArgumentError" do
      assert_raise ArgumentError, ~r/invalid Signal kind/, fn ->
        Signal.new(%{kind: :unknown_kind, source: :s, name: :n})
      end
    end

    test "rejects non-atom source" do
      assert_raise ArgumentError, ~r/source must be an atom/, fn ->
        Signal.new(%{kind: :event, source: "not_atom", name: :n})
      end
    end

    test "rejects non-atom name" do
      assert_raise ArgumentError, ~r/name must be an atom/, fn ->
        Signal.new(%{kind: :event, source: :s, name: "not_atom"})
      end
    end

    test "rejects keyword list measurements" do
      assert_raise ArgumentError, ~r/measurements must be a plain map/, fn ->
        Signal.new(%{kind: :event, source: :s, name: :n, measurements: [count: 1]})
      end
    end

    test "rejects keyword list metadata" do
      assert_raise ArgumentError, ~r/metadata must be a plain map/, fn ->
        Signal.new(%{kind: :event, source: :s, name: :n, metadata: [a: 1]})
      end
    end

    test "all four kinds are accepted" do
      for kind <- [:metric, :event, :log, :trace_span] do
        signal = Signal.new(%{kind: kind, source: :s, name: :n})
        assert signal.kind == kind
      end
    end
  end
end
