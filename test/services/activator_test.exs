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

  test "stop computation" do
    computation = %Aflecha.Computation.Builder{
      id: "rem_compuation_test",
      factory_id: @factory_do_nothing
    }

    {:ok, _} = start_computation(@default_operator, computation)

    assert stop_computation(@default_operator, computation.id) === :ok
  end

  test "computation validations" do
    computation = AflechaTest.Fixtures.Computation.get("test_computation_validation", nil)

    result1 = start_computation(@default_operator, computation)
    result2 = start_computation(@default_operator, computation)

    assert elem(result1, 0) === :ok
    assert elem(result2, 0) === :error
    assert elem(result2, 1) === "computation id already in use"
  end

  test "validate computation length" do
    valid_computation = %Aflecha.Computation.Builder{
      id: "computation_string_32_bytes_long",
      factory_id: @factory_do_nothing
    }

    invalid_computation = %Aflecha.Computation.Builder{
      id: "some_very_long_computation_id_that_should_fail",
      factory_id: @factory_do_nothing
    }

    result1 = start_computation(@default_operator, valid_computation)
    result2 = start_computation(@default_operator, invalid_computation)

    assert elem(result1, 0) === :ok
    assert result2 === {:error, "computation id exceeds 32 bytes"}
  end
end
