defmodule FlezhaTest.Plugin.FilterTest do
  alias Flezha.Plugin.PipelineBinding

  use ExUnit.Case

  test "filter input" do
    settings = %Flezha.Plugin.Provider.Filter{
      regex_pattern: "^heartbeat message$",
      inverted: false,
      group: "test_group",
      pipeline_id: "test_pipeline",
      trigger: :test
    }

    result = PipelineBinding.bind(settings)

    assert elem(result, 0) == :ok
    assert is_struct(elem(result, 1), Flezha.Plugin.Provider.Filter)

    payload = {:text, "heartbeat message"}
    exec_result = PipelineBinding.exec(elem(result, 1), payload)

    assert elem(exec_result, 0) == :ok
    assert elem(exec_result, 1) == payload

    payload2 = {:text, "heartbeat message 2"}
    exec_result2 = PipelineBinding.exec(elem(result, 1), payload2)

    assert elem(exec_result2, 0) == :skip
    assert elem(exec_result2, 1) == :normal
  end

  test "filter input inverted" do
    settings = %Flezha.Plugin.Provider.Filter{
      regex_pattern: "^heartbeat message$",
      inverted: true,
      group: "test_group",
      pipeline_id: "test_pipeline",
      trigger: :test
    }

    result = PipelineBinding.bind(settings)

    assert elem(result, 0) == :ok
    assert is_struct(elem(result, 1), Flezha.Plugin.Provider.Filter)

    payload = {:text, "heartbeat message"}
    exec_result = PipelineBinding.exec(elem(result, 1), payload)

    assert elem(exec_result, 0) == :skip
    assert elem(exec_result, 1) == :normal

    payload2 = {:text, "heartbeat message 2"}
    exec_result2 = PipelineBinding.exec(elem(result, 1), payload2)

    assert elem(exec_result2, 0) == :ok
    assert elem(exec_result2, 1) == {:text, "heartbeat message 2"}
  end
end
