defmodule AflechaTest.RpcCookbookServiceTest do
  use ExUnit.Case
  use Aflecha.Constant

  setup do
    %{}
  end

  test "rpc start computation" do
    computation_id = "test_rpc_start_comp"
    factory_id = "increment_state"
    {:ok, variables} = Jason.encode(%{"value" => 100})

    result =
      Aflecha.Service.RpcCookbook.computation_start(
        @default_operator,
        computation_id,
        factory_id,
        variables
      )

    assert elem(result, 0) === :ok
  end

  test "rpc stop computation" do
    computation_id = "test_rpc_stop_comp"

    AflechaTest.Fixtures.Computation.get(computation_id, nil)
    |> AflechaTest.Fixtures.Computation.start()

    result1 = Aflecha.Service.RpcCookbook.computation_stop(@default_operator, computation_id)
    result2 = Aflecha.Service.RpcCookbook.computation_stop(@default_operator, "non_existent")

    assert result1 === :ok
    assert result2 |> elem(0) === :error
    assert result2 |> elem(1) |> elem(0) === "non_existent"
    assert is_list(result2 |> elem(1) |> elem(1))
  end

  test "rpc no operator specified" do
    computation_id = "test_rpc_no_operator"

    result1 =
      Aflecha.Service.RpcCookbook.computation_start("", computation_id, @factory_do_nothing, "{}")

    result2 = Aflecha.Service.RpcCookbook.computation_stop("", computation_id)

    assert elem(result1, 0) === :ok
    assert result2 === :ok
  end
end
