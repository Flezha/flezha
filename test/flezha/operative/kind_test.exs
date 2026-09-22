defmodule FlezhaTest.Operative.KindTest do
  @moduledoc """
  `Flezha.Operative.Kind` capability model + the `:kind` field on
  `Flezha.Operative.Builder`, including the `:workflow` kind and the `AddOperative`
  wire `kind` field added by `operative-task-kind`.
  """
  use ExUnit.Case, async: true

  alias Flezha.Operative.Builder
  alias Flezha.Operative.Kind

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
    test "a builder names no kind until the start resolves the factory's" do
      assert %Builder{id: "r", factory_id: "f"}.kind == nil
    end

    test "a caller's written kind is refused, as any field it may not write" do
      assert Builder.build(factory_id: "f", kind: "workflow") ==
               {:error, {:unknown_fields, [:kind]}}
    end
  end

  test "AddOperative protobuf carries an optional kind field" do
    assert :kind in Map.keys(RunnerCommands.AddOperative.__struct__())
    # Default is the empty string ⇒ interpreted as :stream on the SDK side.
    assert RunnerCommands.AddOperative.__struct__().kind == ""
  end
end
