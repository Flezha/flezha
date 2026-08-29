defmodule FlezhaTest.System.TaskKindE2ETest do
  @moduledoc """
  End-to-end proof for `operative-task-kind` against a live Rust runner: a
  unit started with `kind: :workflow` flows `AddOperative --kind task` → SDK spawns
  a task → runs `run` once → emits `WorkflowCompleted` → the controller drives
  deregister-on-completion. The whole loop is observable as the unit
  disappearing from the durable registry without an explicit stop. Tagged
  `:system` (boots a real subprocess); run with
  `mix test --include system test/system/task_kind_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :workflow

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2, ensure_port_dead: 1]

  alias FlezhaTest.Helper.NodeTestHelper
  alias Flezha.Runner.FleetManager
  alias Flezha.Operative.Builder
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager

  @namespace "task_kind_e2e"
  @factory "echo_workflow"

  # One fleet for the module: every scenario names its own operative.
  setup_all do
    {port, _ce_id} = NodeTestHelper.start_runner_port(@namespace)

    on_exit(fn ->
      try do
        FleetManager.stop_namespace(@namespace)
      catch
        _, _ -> :ok
      end

      ensure_port_dead(port)
    end)

    :ok
  end

  setup do
    on_exit(fn ->
      try do
        OperativeManager.stop_all_operatives(@namespace)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  @tag :task_kind_e2e
  test "task runs once on start and is deregistered on completion" do
    operative_id = "task_e2e_#{System.unique_integer([:positive])}"

    builder = %Builder{
      id: operative_id,
      factory_id: @factory,
      kind: :workflow,
      init_values: %{"msg" => "hi"},
      opts: []
    }

    # Start succeeds even for a near-instant task: a run-once unit may
    # complete (and self-deregister) before its binding is observable, and a
    # clean exit during start confirmation is not a failure.
    assert {:ok, _info} = OperativeManager.start_operative(@namespace, builder)

    # The SDK runs the task once and ships `WorkflowCompleted`; the controller
    # tears it down. Deregister-on-completion: the unit drops out of the
    # durable registry with no explicit stop — proof the full loop ran.
    assert wait_until(fn -> not registered?(operative_id) end, 5_000),
           "task was never deregistered after completion"
  end

  defp registered?(operative_id) do
    match?({:ok, pid} when is_pid(pid), HubManager.get_pid(@namespace, operative_id))
  end
end
