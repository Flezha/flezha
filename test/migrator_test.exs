defmodule MigratorTest do
  use ExUnit.Case
  doctest Aflecha.Computation.Migrator

  test "serialize computation" do
    computation = %Aflecha.Computation.Builder{
      id: "serialize_computation_test",
      factory_id: "increment_state"
    }

    Aflecha.add_computation(computation)

    res = Aflecha.Computation.Migrator.serialize_computation("serialize_computation_test")

    assert is_list(res)
    assert is_tuple(Enum.at(res, 0))
    assert Enum.at(res, 0) |> elem(0) === "default"
    assert is_list(Enum.at(res, 0) |> elem(1))
    assert length(Enum.at(res, 0) |> elem(1)) > 0
  end

  test "deserialize computation" do
    computation = %Aflecha.Computation.Builder{
      id: "serialize_computation_test",
      factory_id: "increment_state"
    }

    Aflecha.add_computation(computation)

    serialized_data =
      Aflecha.Computation.Migrator.serialize_computation("serialize_computation_test")

    assert [{"default", :ok}] ===
             Aflecha.Computation.Migrator.deserialize_computation(
               "serialize_computation_test",
               serialized_data
             )
  end
end
