defmodule FlezhaTest.Relay.KindTest do
  @moduledoc """
  `Flezha.Relay.Kind` capability model + the `:kind` field on
  `Flezha.Relay.Builder`, plus the no-wire-surface guard (the `AddRelay`
  protobuf carries no `kind` field).
  """
  use ExUnit.Case, async: true

  alias Flezha.Relay.Builder
  alias Flezha.Relay.Kind

  describe "capabilities" do
    test ":relay declares persistent_state?" do
      assert Kind.capable?(:relay, :persistent_state?)
      assert Kind.capabilities(:relay) == %{persistent_state?: true}
    end

    test "an undeclared capability reads false" do
      # :relay does not declare it, and an unknown kind declares nothing.
      refute Kind.capable?(:relay, :scheduled?)
      refute Kind.capable?(:made_up_kind, :persistent_state?)
      assert Kind.capabilities(:made_up_kind) == %{}
    end

    test "known?/1 accepts :relay and rejects others" do
      assert Kind.known?(:relay)
      refute Kind.known?(:task)
      refute Kind.known?(:made_up_kind)
    end
  end

  describe "Builder kind field" do
    test "an unspecified kind defaults to :relay" do
      assert %Builder{id: "r", factory_id: "f"}.kind == :relay
    end

    test "validate_relay rejects an unknown kind, naming it" do
      relay = %Builder{id: "r", factory_id: "f", kind: :made_up_kind}
      assert {:error, msg} = Builder.validate_relay("ns", relay)
      assert msg =~ "made_up_kind"
    end
  end

  test "AddRelay protobuf carries no kind field (no wire surface)" do
    refute :kind in Map.keys(ExecutorCommands.AddRelay.__struct__())
  end
end
