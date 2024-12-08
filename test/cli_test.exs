defmodule CliTest do
  use ExUnit.Case
  use Aflecha.Constant

  setup do
    %{}
  end

  test "add computation" do
    computation_id = "test_cli_add_comp"
    factory_id = "increment_state"
    {:ok, variables} = Jason.encode(%{"value" => 100})

    Aflecha.Service.Cli.computation_add(@default_operator, computation_id, factory_id, variables)

    assert false
  end
end
