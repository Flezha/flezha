defmodule FlezhaTest.Runner.ControlUnitBoundedStopTest do
  @moduledoc """
  §5a.1/§6.2: the cooperative external-stop send. `ControlUnit.bounded_stop/2`
  ships a `BoundedStop` command over the control channel — distinct from the hard
  `RemOperative` the unbounded stop uses. Fire-and-forget: the SDK trips its
  cancellation token and the teardown follows on `BoundedCompleted`.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.Service.Identificator

  @namespace "ctrl-unit-bounded-stop-test"

  setup :set_mimic_global

  test "bounded_stop/2 sends a BoundedStop command (cooperative), not RemOperative" do
    test_pid = self()
    runner_id = "exec-bs-#{System.unique_integer([:positive])}"

    ControlChannel
    |> stub(:send_control_async, fn ^runner_id, command ->
      send(test_pid, {:sent, command})
      :ok
    end)

    {:ok, pid} = ControlUnit.start_link({@namespace, runner_id})
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000) end)

    assert :ok = ControlUnit.bounded_stop(runner_id, "op-1")

    assert_receive {:sent, {:bounded_stop, %RunnerCommands.BoundedStop{operative_id: "op-1"}}},
                   500

    # Cooperative stop must NOT use the hard removal command.
    refute_received {:sent, {:rem_operative, _}}
  end

  test "bounded_stop/2 returns {:error, :no_control_unit} when the runner is gone" do
    missing = "exec-missing-#{System.unique_integer([:positive])}"
    assert GenServer.whereis(Identificator.control_unit(missing)) == nil
    assert {:error, :no_control_unit} = ControlUnit.bounded_stop(missing, "op-1")
  end
end
