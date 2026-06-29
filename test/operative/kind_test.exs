defmodule FlezhaTest.Operative.KindTest do
  @moduledoc """
  `Flezha.Operative.Kind` capability model + the `:kind` field on
  `Flezha.Operative.Builder`, including the `:bounded` kind and the `AddOperative`
  wire `kind` field added by `operative-task-kind`.
  """
  use ExUnit.Case, async: true

  alias Flezha.Operative.Builder
  alias Flezha.Operative.Kind
  alias Flezha.Service.Validator

  describe "capabilities" do
    test ":continuous declares persistent_state?" do
      assert Kind.capable?(:continuous, :persistent_state?)
      assert Kind.capabilities(:continuous) == %{persistent_state?: true}
    end

    test "an undeclared capability reads false" do
      # :continuous does not declare it, and an unknown kind declares nothing.
      refute Kind.capable?(:continuous, :scheduled?)
      refute Kind.capable?(:made_up_kind, :persistent_state?)
      assert Kind.capabilities(:made_up_kind) == %{}
    end

    test ":bounded is stateless — declares no capabilities" do
      assert Kind.capabilities(:bounded) == %{}
      refute Kind.capable?(:bounded, :persistent_state?)
    end

    test "known?/1 accepts :continuous and :bounded, rejects others" do
      assert Kind.known?(:continuous)
      assert Kind.known?(:bounded)
      refute Kind.known?(:made_up_kind)
    end
  end

  describe "Builder kind field" do
    test "an unspecified kind defaults to :continuous" do
      assert %Builder{id: "r", factory_id: "f"}.kind == :continuous
    end

    test "valid_kind accepts :continuous and :bounded" do
      assert Validator.valid_kind(:continuous) == :ok
      assert Validator.valid_kind(:bounded) == :ok
    end

    test "validate_operative rejects an unknown kind, naming it" do
      operative = %Builder{id: "r", factory_id: "f", kind: :made_up_kind}
      assert {:error, msg} = Builder.validate_operative("ns", operative)
      assert msg =~ "made_up_kind"
    end
  end

  test "AddOperative protobuf carries an optional kind field" do
    assert :kind in Map.keys(ExecutorCommands.AddOperative.__struct__())
    # Default is the empty string ⇒ interpreted as :continuous on the SDK side.
    assert ExecutorCommands.AddOperative.__struct__().kind == ""
  end
end
