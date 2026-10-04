defmodule Flezha.Service.Cli.RunnersTest do
  # async: false: Mimic stubs.
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureIO

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.Cli

  setup do
    stub(FleetManager, :global_runner_pids, fn "trading" ->
      {:ok, [{"trading_2bfeedb5e74f", 48_213}, {"trading_d4c7e284bc33", nil}]}
    end)

    :ok
  end

  # fails when: the list drops the process id, so a user who wants to crash a
  # runner has nothing to kill it by.
  test "a runner is listed with its process id, and one whose transport cannot say without" do
    output = capture_io(fn -> Cli.call("Fleet.runners", Jason.encode!(["trading"]), []) end)

    assert output =~ "  - trading_2bfeedb5e74f  pid 48213\n"
    assert output =~ "  - trading_d4c7e284bc33\n"
  end

  test "the JSON form carries each runner with its process id, or none" do
    output =
      capture_io(fn -> Cli.call("Fleet.runners", Jason.encode!(["trading"]), output: "json") end)

    assert Jason.decode!(output) == [
             %{"id" => "trading_2bfeedb5e74f", "pid" => 48_213},
             %{"id" => "trading_d4c7e284bc33", "pid" => nil}
           ]
  end
end
