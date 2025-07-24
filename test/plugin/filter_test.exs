defmodule FlezhaTest.Plugin.FilterTest do
  alias Flezha.Plugin.ComputationBinding

  use ExUnit.Case

  test "filter input" do
    settings = %Flezha.Plugin.Provider.Filter{
      id: "filter_1",
      regex_pattern: "^{\"event\": \"heartbeat\"}$",
      inverted: false,
      group: "test_group",
      computation_id: "test_computation",
      trigger: :test
    }

    result = ComputationBinding.bind(settings)

    assert elem(result, 0) == :ok
    assert is_struct(elem(result, 1), Flezha.Plugin.Provider.Filter)

    payload = {:text, "{\"event\": \"heartbeat\"}"}
    exec_result = ComputationBinding.exec(elem(result, 1), payload)

    assert elem(exec_result, 0) == :ok
    assert elem(exec_result, 1) == payload

    payload2 = {:text, "{\"event\": \"heartbeat2\"}"}
    exec_result2 = ComputationBinding.exec(elem(result, 1), payload2)

    assert elem(exec_result2, 0) == :skip
    assert elem(exec_result2, 1) == :normal
  end

  test "filter input inverted" do
    settings = %Flezha.Plugin.Provider.Filter{
      id: "filter_1",
      regex_pattern: "^{\"event\": \"heartbeat\"}$",
      inverted: true,
      group: "test_group",
      computation_id: "test_computation",
      trigger: :test
    }

    result = ComputationBinding.bind(settings)

    assert elem(result, 0) == :ok
    assert is_struct(elem(result, 1), Flezha.Plugin.Provider.Filter)

    payload = {:text, "{\"event\": \"heartbeat\"}"}
    exec_result = ComputationBinding.exec(elem(result, 1), payload)

    assert elem(exec_result, 0) == :skip
    assert elem(exec_result, 1) == :normal

    payload2 = {:text, "{\"event\": \"heartbeat2\"}"}
    exec_result2 = ComputationBinding.exec(elem(result, 1), payload2)

    assert elem(exec_result2, 0) == :ok
    assert elem(exec_result2, 1) == {:text, "{\"event\": \"heartbeat2\"}"}
  end
end
