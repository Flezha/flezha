defmodule FlezhaTest.Service.OperatorManagerTeardownTest do
  @moduledoc """
  The `:state_dir` step of an operator stop, driven on its own: what a stop
  owes the instance's durable writers whether its files are deleted or kept for
  a rescale to move.
  """
  # async: false: a shared on-disk root and the node-wide writer registry.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias FlezhaTest.Helper.NodeTestHelper

  alias Flezha.Service.OperatorManager.Teardown
  alias Flezha.Shutdown
  alias Flezha.State.{InputLog, Paths, Supervisor, TaskJournal}

  defp state_dir_step(), do: Enum.filter(Teardown.shutdown_steps(), &(&1.name == :state_dir))

  defp stop(instance, preserve?, node \\ node()) do
    target = %{
      namespace: "_system",
      operator_id: instance,
      node: node,
      runner_id: nil,
      preserve_state?: preserve?,
      cell_count: 0
    }

    assert {:ok, _report} = Shutdown.run(:operator, target, 5_000, state_dir_step())
  end

  # A registration outlives its process until the registry handles the exit.
  defp closed?(instance) do
    wait_until(
      fn ->
        Supervisor.live_writers(instance) == [] and
          Supervisor.lookup({instance, :input_log}) == :error
      end,
      2_000
    )
  end

  # fails when: a preserving stop skips the writer close, so a member
  # restarted under the same id after a rescale moved its files appends
  # through the descriptor of the file that moved.
  test "a stop that keeps the files closes every durable writer of the instance" do
    instance = "teardown_keep_#{System.unique_integer([:positive])}"
    assert :ok = TaskJournal.append_iter_committed(instance, 0, 0, [])
    assert {:ok, 0} = InputLog.append(instance, "m", %{})

    stop(instance, true)

    assert closed?(instance)
    assert File.exists?(Paths.journal_path(instance))
    assert File.exists?(Paths.input_log_path(instance))

    File.rename!(Paths.journal_path(instance), Paths.journal_path(instance) <> ".moved")
    assert :ok = TaskJournal.append_iter_committed(instance, 1, 1, [])

    assert {:ok, [%{iter: 1}]} = TaskJournal.read(instance)
  end

  # fails when: an Overwatch that ends on this node, a move away among them,
  # leaves its writers open, and the operator's later return finds its own
  # stale copy held open.
  test "an operator no longer bound here closes its writers and keeps its files" do
    instance = "teardown_moved_#{System.unique_integer([:positive])}"
    assert :ok = TaskJournal.append_iter_committed(instance, 0, 0, [])
    assert {:ok, 0} = InputLog.append(instance, "m", %{})

    state = %Flezha.Operator.Overwatch{
      id: instance,
      namespace: "_system",
      builder: %Flezha.Operator.Builder{id: instance, factory_id: "f", opts: []}
    }

    steps = Enum.filter(Flezha.Operator.Migration.shutdown_steps(), &(&1.name == :writers))
    assert {:ok, _report} = Shutdown.run(:process, {state, :shutdown}, 5_000, steps)

    assert closed?(instance)
    assert File.exists?(Paths.input_log_path(instance))
  end

  test "a stop that deletes the files closes the writers and removes the tree" do
    instance = "teardown_drop_#{System.unique_integer([:positive])}"
    assert :ok = TaskJournal.append_iter_committed(instance, 0, 0, [])
    assert {:ok, 0} = InputLog.append(instance, "m", %{})

    stop(instance, false)

    assert closed?(instance)
    refute File.exists?(Paths.class_dir(instance))
  end

  # fails when: the step runs on the node that issued the stop, which for a
  # rescale run from another controller is not where the member's writers are.
  @tag :multi_node
  test "a stop reaches the writers on the node the operator ran on" do
    instance = "teardown_peer_#{System.unique_integer([:positive])}"

    {peer_pid, peer_node} =
      NodeTestHelper.start_peer_node(:teardown1, state_root: Paths.state_root())

    :ok = :erpc.call(peer_node, TaskJournal, :append_iter_committed, [instance, 0, 0, []])
    assert [_writer] = :erpc.call(peer_node, Supervisor, :live_writers, [instance])

    stop(instance, true, peer_node)

    assert wait_until(
             fn -> :erpc.call(peer_node, Supervisor, :live_writers, [instance]) == [] end,
             2_000
           )

    assert :erpc.call(peer_node, File, :exists?, [Paths.journal_path(instance)])

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end
end
