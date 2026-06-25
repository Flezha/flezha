defmodule FlezhaTest.Relay.KindTest do
  @moduledoc """
  `Flezha.Relay.Kind` capability model + the `:kind` field on
  `Flezha.Relay.Builder`, including the `:task` kind and the `AddRelay`
  wire `kind` field added by `relay-task-kind`.
  """
  use ExUnit.Case, async: true

  alias Flezha.Relay.Builder
  alias Flezha.Relay.Kind
  alias Flezha.Service.Validator

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

    test ":task is stateless — declares no capabilities" do
      assert Kind.capabilities(:task) == %{}
      refute Kind.capable?(:task, :persistent_state?)
    end

    test "known?/1 accepts :relay and :task, rejects others" do
      assert Kind.known?(:relay)
      assert Kind.known?(:task)
      refute Kind.known?(:made_up_kind)
    end
  end

  describe "Builder kind field" do
    test "an unspecified kind defaults to :relay" do
      assert %Builder{id: "r", factory_id: "f"}.kind == :relay
    end

    test "valid_kind accepts :relay and :task" do
      assert Validator.valid_kind(:relay) == :ok
      assert Validator.valid_kind(:task) == :ok
    end

    test "validate_relay rejects an unknown kind, naming it" do
      relay = %Builder{id: "r", factory_id: "f", kind: :made_up_kind}
      assert {:error, msg} = Builder.validate_relay("ns", relay)
      assert msg =~ "made_up_kind"
    end
  end

  test "AddRelay protobuf carries an optional kind field" do
    assert :kind in Map.keys(ExecutorCommands.AddRelay.__struct__())
    # Default is the empty string ⇒ interpreted as :relay on the SDK side.
    assert ExecutorCommands.AddRelay.__struct__().kind == ""
  end
end
