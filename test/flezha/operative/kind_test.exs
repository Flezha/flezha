defmodule FlezhaTest.Operative.KindTest do
  @moduledoc """
  `Flezha.Operative.Kind` capability model + the `:kind` field on
  `Flezha.Operative.Builder`, including the `:workflow` kind and the `AddOperative`
  wire `kind` field added by `operative-task-kind`.
  """
  use ExUnit.Case, async: true

  alias Flezha.Operative.Builder
  alias Flezha.Operative.Kind
  alias Flezha.Service.OperativeManager.Admission
  alias Flezha.Service.Validator

  @persistence_flags [:persistent_state?, :time_travel?, :dynamic_cells?, :output_stall_watch?]

  describe "capabilities" do
    test ":stream declares the full persistence surface" do
      for flag <- @persistence_flags, do: assert(Kind.capable?(:stream, flag))

      assert Kind.capabilities(:stream) == %{
               persistent_state?: true,
               time_travel?: true,
               dynamic_cells?: true,
               output_stall_watch?: true
             }
    end

    test "an undeclared capability reads false" do
      # :stream does not declare it, and an unknown kind declares nothing.
      refute Kind.capable?(:stream, :scheduled?)
      refute Kind.capable?(:made_up_kind, :persistent_state?)
      assert Kind.capabilities(:made_up_kind) == %{}
    end

    test ":workflow is stateless — declares no capabilities" do
      assert Kind.capabilities(:workflow) == %{}
      for flag <- @persistence_flags, do: refute(Kind.capable?(:workflow, flag))
    end

    test "known?/1 accepts :stream and :workflow, rejects others" do
      assert Kind.known?(:stream)
      assert Kind.known?(:workflow)
      refute Kind.known?(:made_up_kind)
    end
  end

  describe "Builder kind field" do
    test "an unspecified kind defaults to :stream" do
      assert %Builder{id: "r", factory_id: "f"}.kind == :stream
    end

    test "valid_kind accepts :stream and :workflow" do
      assert Validator.valid_kind(:stream) == :ok
      assert Validator.valid_kind(:workflow) == :ok
    end

    test "validate_operative rejects an unknown kind, naming it" do
      operative = %Builder{id: "r", factory_id: "f", kind: :made_up_kind}
      assert {:error, msg} = Admission.validate_operative("ns", operative)
      assert msg =~ "made_up_kind"
    end
  end

  test "AddOperative protobuf carries an optional kind field" do
    assert :kind in Map.keys(RunnerCommands.AddOperative.__struct__())
    # Default is the empty string ⇒ interpreted as :stream on the SDK side.
    assert RunnerCommands.AddOperative.__struct__().kind == ""
  end
end
