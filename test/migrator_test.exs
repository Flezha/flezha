defmodule MigratorTest do
  use ExUnit.Case
  doctest Aflecha.Computation.Migrator

  @default_group "default"

  test "serialize computation" do
    computation = %Aflecha.Computation.Builder{
      id: "serialize_computation_test",
      factory_id: "increment_state"
    }

    Aflecha.add_computation(@default_group, computation)

    serialized_data =
      Aflecha.Computation.Migrator.serialize_computation(
        @default_group,
        "serialize_computation_test"
      )

    assert is_list(serialized_data)
    assert length(serialized_data) > 0
  end

  test "deserialize computation" do
    computation = %Aflecha.Computation.Builder{
      id: "serialize_computation_test",
      factory_id: "increment_state"
    }

    Aflecha.add_computation(@default_group, computation)

    serialized_data =
      Aflecha.Computation.Migrator.serialize_computation(
        @default_group,
        "serialize_computation_test"
      )

    assert :ok ===
             Aflecha.Computation.Migrator.deserialize_computation(
               @default_group,
               "serialize_computation_test",
               serialized_data
             )
  end
end
