defmodule FlezhaTest.Plugin.Provider.FilterTest do
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.Filter

  use ExUnit.Case, async: true

  defp init_filter(opts) do
    {:ok, opts} = Filter.init_provider(opts)
    opts
  end

  test "filter input (match passes)" do
    opts = init_filter(%{regex_pattern: "^{\"event\": \"heartbeat\"}$", inverted: false})

    ctx = %Ctx.IO{
      relay_id: "p",
      namespace: "n",
      message: "{\"event\": \"heartbeat\"}",
      metadata: %{}
    }

    assert {:ok, ^ctx} = Filter.on_input(ctx, opts)
  end

  test "filter input (no match skips)" do
    opts = init_filter(%{regex_pattern: "^{\"event\": \"heartbeat\"}$", inverted: false})

    ctx = %Ctx.IO{
      relay_id: "p",
      namespace: "n",
      message: "{\"event\": \"heartbeat2\"}",
      metadata: %{}
    }

    assert {:skip, :normal} = Filter.on_input(ctx, opts)
  end

  test "filter input inverted (match skips)" do
    opts = init_filter(%{regex_pattern: "^{\"event\": \"heartbeat\"}$", inverted: true})

    ctx = %Ctx.IO{
      relay_id: "p",
      namespace: "n",
      message: "{\"event\": \"heartbeat\"}",
      metadata: %{}
    }

    assert {:skip, :normal} = Filter.on_input(ctx, opts)
  end

  test "filter input inverted (no match passes)" do
    opts = init_filter(%{regex_pattern: "^{\"event\": \"heartbeat\"}$", inverted: true})

    ctx = %Ctx.IO{
      relay_id: "p",
      namespace: "n",
      message: "{\"event\": \"heartbeat2\"}",
      metadata: %{}
    }

    assert {:ok, ^ctx} = Filter.on_input(ctx, opts)
  end
end
