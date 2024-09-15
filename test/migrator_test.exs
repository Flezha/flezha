defmodule MigratorTest do
  use ExUnit.Case
  doctest Flecha.Computation.Migrator

  test "serialize computation" do
    computation = %Flecha.Computation{
      id: "serialize_computation_test",
      template_id: "increment_state"
    }

    Flecha.add_computation(computation)

    res = Flecha.Computation.Migrator.serialize_computation("serialize_computation_test")

    assert is_list(res)
    assert is_tuple(Enum.at(res, 0))
    assert Enum.at(res, 0) |> elem(0) === "default"
    assert is_list(Enum.at(res, 0) |> elem(1))
    assert length(Enum.at(res, 0) |> elem(1)) > 0
  end
end
