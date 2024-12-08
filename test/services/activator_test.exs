defmodule AflechaTest.Service.ActivatorTest do
  use ExUnit.Case
  use Aflecha.Constant

  import Aflecha.Computation.Service.Activator

  doctest Aflecha.Computation.Service.Activator

  test "start computation" do
    computation_id = "test_computation_start"

    computation = %Aflecha.Computation.Builder{
      id: computation_id,
      factory_id: @factory_do_nothing
    }

    result = start_computation(@default_operator, computation)

    assert elem(result, 0) === :ok
    assert is_tuple(elem(result, 1))
    assert result |> elem(1) |> elem(0) === computation_id
    assert is_list(result |> elem(1) |> elem(1))
  end

  test "computation validations" do
    computation = AflechaTest.Fixtures.Computation.get("test_computation_validation", nil)

    result1 = start_computation(@default_operator, computation)
    result2 = start_computation(@default_operator, computation)

    assert elem(result1, 0) === :ok
    assert elem(result2, 0) === :error
    assert elem(result2, 1) === "computation id already in use"
  end
end
