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
          metadata: %{operative: "r", procedure: 3}
        })

      assert %Signal{kind: :metric, source: :s, name: :n} = signal
      assert signal.measurements == %{count: 1, bytes: 16_384}
      assert signal.metadata == %{operative: "r", procedure: 3}
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

  describe "metadata_text/1" do
    test "nil renders as its name, not as the empty string" do
      # `to_string(nil)` is "", which as a key means no name at all and
      # collides with a genuine "" key. Every other atom is its own name.
      assert Signal.metadata_text(nil) == "nil"
      assert Signal.metadata_text(true) == "true"
      assert Signal.metadata_text(false) == "false"
      assert Signal.metadata_text(:a) == "a"
    end

    test "a UTF-8 binary passes through and a non-UTF-8 one is inspected" do
      assert Signal.metadata_text("ok \u00e9") == "ok \u00e9"
      assert Signal.metadata_text(<<0xFF, 0x00, 0xFE>>) == "<<255, 0, 254>>"
    end

    test "a list is the list, never the binary of its bytes" do
      assert Signal.metadata_text([1, 2, 3]) == "[1, 2, 3]"
      refute Signal.metadata_text([1, 2, 3]) == <<1, 2, 3>>
    end

    test "it is total: no term raises, and every answer is a valid UTF-8 string" do
      terms = [
        nil,
        :a,
        1,
        1.5,
        "s",
        <<0xFF>>,
        [1, 2, 3],
        ~c"cl",
        {:a, :b},
        %{a: 1},
        self(),
        make_ref(),
        &Signal.metadata_text/1,
        %Signal{kind: :log, source: :s, name: :n}
      ]

      for term <- terms do
        text = Signal.metadata_text(term)
        assert is_binary(text) and text != ""
        assert String.valid?(text)
      end
    end
  end
end
