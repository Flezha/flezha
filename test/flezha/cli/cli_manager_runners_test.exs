defmodule Flezha.Service.CliManagerRunnersTest do
  # async: false: Mimic stubs.
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureIO

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.CliManager

  setup do
    stub(FleetManager, :global_runner_pids, fn "trading" ->
      {:ok, [{"trading_2bfeedb5e74f", 48_213}, {"trading_d4c7e284bc33", nil}]}
    end)

    :ok
  end

  # fails when: the list drops the process id, so a user who wants to crash a
  # runner has nothing to kill it by.
  test "a runner is listed with its process id, and one whose transport cannot say without" do
    output = capture_io(fn -> CliManager.list_runners("trading") end)

    assert output =~ "  - trading_2bfeedb5e74f  pid 48213\n"
    assert output =~ "  - trading_d4c7e284bc33\n"
  end

  test "the JSON form keeps the runner list and adds the known process ids beside it" do
    output = capture_io(fn -> CliManager.list_runners("trading", "json") end)

    assert Jason.decode!(output) == %{
             "namespace" => "trading",
             "runners" => ["trading_2bfeedb5e74f", "trading_d4c7e284bc33"],
             "pids" => %{"trading_2bfeedb5e74f" => 48_213}
           }
  end
end
