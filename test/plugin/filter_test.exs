defmodule FlezhaTest.Plugin.FilterTest do
  alias Flezha.Computation.IOBinding

  use ExUnit.Case

  test "filter input" do
    provider = %Flezha.Plugin.Provider.Filter{
      regex_pattern: "^{\"event\": \"heartbeat\"}$",
      inverted: false
    }

    result = IOBinding.bind(provider)

    assert elem(result, 0) == :ok
    assert is_struct(elem(result, 1), Flezha.Plugin.Provider.Filter)

    payload = {"{\"event\": \"heartbeat\"}", %{}}
    exec_result = IOBinding.exec(elem(result, 1), payload)

    assert elem(exec_result, 0) == :ok
    assert elem(exec_result, 1) == payload

    payload2 = {"{\"event\": \"heartbeat2\"}", %{}}
    exec_result2 = IOBinding.exec(elem(result, 1), payload2)

    assert elem(exec_result2, 0) == :skip
    assert elem(exec_result2, 1) == :normal
  end

  test "filter input inverted" do
    provider = %Flezha.Plugin.Provider.Filter{
      regex_pattern: "^{\"event\": \"heartbeat\"}$",
      inverted: true
    }

    result = IOBinding.bind(provider)

    assert elem(result, 0) == :ok
    assert is_struct(elem(result, 1), Flezha.Plugin.Provider.Filter)

    payload = {"{\"event\": \"heartbeat\"}", %{}}
    exec_result = IOBinding.exec(elem(result, 1), payload)

    assert elem(exec_result, 0) == :skip
    assert elem(exec_result, 1) == :normal

    payload2 = {"{\"event\": \"heartbeat2\"}", %{}}
    exec_result2 = IOBinding.exec(elem(result, 1), payload2)

    assert elem(exec_result2, 0) == :ok
    assert elem(exec_result2, 1) == {"{\"event\": \"heartbeat2\"}", %{}}
  end
end
