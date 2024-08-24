defmodule FlechaTest do
  use ExUnit.Case
  doctest Flecha

  test "add computation" do
    computation = %Flecha.Computation{
      computation_id: "add_compuation_test",
      computation_name: "do_nothing"
    }

    assert :ok === Flecha.add_computation(computation)
  end
end
