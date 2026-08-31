defmodule FlezhaTest.Runner.ControlUnitWorkflowStopTest do
  @moduledoc """
  §5a.1/§6.2: the cooperative external-stop send. `ControlUnit.workflow_stop/2`
  ships a `WorkflowStop` command over the control channel — distinct from the hard
  `RemOperative` the stream stop uses. Fire-and-forget: the SDK trips its
  cancellation token and the teardown follows on `WorkflowCompleted`.
  """

  # async: false: Mimic global mode; Mimic stubs; registered runner channels.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Identificator
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "ctrl-unit-workflow-stop-test"

  setup :set_mimic_global_until_exit

  test "workflow_stop/2 sends a WorkflowStop command (cooperative), not RemOperative" do
    test_pid = self()
    runner_id = "exec-bs-#{System.unique_integer([:positive])}"

    ControlChannel
    |> stub(:send_control_async, fn ^runner_id, command ->
      send(test_pid, {:sent, command})
      :ok
    end)

    {:ok, pid} = ControlUnit.start_link({@namespace, runner_id})
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid, :normal, 1_000) end)

    assert :ok = ControlUnit.workflow_stop(runner_id, "op-1")

    assert_receive {:sent, {:workflow_stop, %RunnerCommands.WorkflowStop{operative_id: "op-1"}}},
                   500

    # Cooperative stop must NOT use the hard removal command.
    refute_received {:sent, {:rem_operative, _}}
  end

  test "workflow_stop/2 returns {:error, :no_control_unit} when the runner is gone" do
    missing = "exec-missing-#{System.unique_integer([:positive])}"
    assert GenServer.whereis(Identificator.control_unit(missing)) == nil
    assert {:error, :no_control_unit} = ControlUnit.workflow_stop(missing, "op-1")
  end
end
