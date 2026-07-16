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

  @persistence_flags [:persistent_state?, :time_travel?, :dynamic_segments?, :output_stall_watch?]

  describe "capabilities" do
    test ":unbounded declares the full persistence surface" do
      for flag <- @persistence_flags, do: assert(Kind.capable?(:unbounded, flag))

      assert Kind.capabilities(:unbounded) == %{
               persistent_state?: true,
               time_travel?: true,
               dynamic_segments?: true,
               output_stall_watch?: true
             }
    end

    test "an undeclared capability reads false" do
      # :unbounded does not declare it, and an unknown kind declares nothing.
      refute Kind.capable?(:unbounded, :scheduled?)
      refute Kind.capable?(:made_up_kind, :persistent_state?)
      assert Kind.capabilities(:made_up_kind) == %{}
    end

    test ":bounded is stateless — declares no capabilities" do
      assert Kind.capabilities(:bounded) == %{}
      for flag <- @persistence_flags, do: refute(Kind.capable?(:bounded, flag))
    end

    test "known?/1 accepts :unbounded and :bounded, rejects others" do
      assert Kind.known?(:unbounded)
      assert Kind.known?(:bounded)
      refute Kind.known?(:made_up_kind)
    end
  end

  describe "Builder kind field" do
    test "an unspecified kind defaults to :unbounded" do
      assert %Builder{id: "r", factory_id: "f"}.kind == :unbounded
    end

    test "valid_kind accepts :unbounded and :bounded" do
      assert Validator.valid_kind(:unbounded) == :ok
      assert Validator.valid_kind(:bounded) == :ok
    end

    test "validate_operative rejects an unknown kind, naming it" do
      operative = %Builder{id: "r", factory_id: "f", kind: :made_up_kind}
      assert {:error, msg} = Builder.validate_operative("ns", operative)
      assert msg =~ "made_up_kind"
    end
  end

  test "AddOperative protobuf carries an optional kind field" do
    assert :kind in Map.keys(RunnerCommands.AddOperative.__struct__())
    # Default is the empty string ⇒ interpreted as :unbounded on the SDK side.
    assert RunnerCommands.AddOperative.__struct__().kind == ""
  end
end
