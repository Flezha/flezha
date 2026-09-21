defmodule FlezhaTest.Service.OperativeManagerRequestStopTest do
  @moduledoc """
  §5a.1: `OperativeManager.request_stop/2` routes by kind — a bound `:workflow`
  operative is stopped cooperatively (`ControlUnit.workflow_stop`), everything else
  takes the hard `stop_operative/2` teardown: the `:operative` walk of the
  shutdown plan, which releases BOTH the `:max_operatives` and
  `:max_cells_per_operative` quota, symmetrically with `start_operative/2`
  (the cell-quota leak fix), and retains a finished workflow's state.

  A minimal stub stands in for the Overwatch GenServer: it answers
  `:get_overwatch_state` (kind / runner / cell count). The hard stop itself
  rides `ProcessHub.stop_children` (the hub stop path removes the declared
  entry before the child terminates), stubbed here at the ProcessHub seam.
  """

  # async: false: writes Application env; Mimic global mode; Mimic stubs.

  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.AuthZ.Quotas
  alias Flezha.Operative.Builder
  alias Flezha.Runner.ControlUnit
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.{Completion, Paths}
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "op-mgr-request-stop-test"

  setup :set_mimic_global_until_exit

  defp start_stub(state_reply), do: StubOverwatch.start_link(state_reply, self())

  # The hub stop seam: `do_stop_operative` pipes `ProcessHub.stop_children`
  # into `Future.await`; stub both ends and signal the test.
  defp stub_hub_stop(test_pid, task_id) do
    ProcessHub
    |> stub(:stop_children, fn _hub_id, [^task_id], _opts ->
      send(test_pid, {:hub_stop, task_id})
      {:awaitable, task_id}
    end)

    ProcessHub.Future
    |> stub(:await, fn {:awaitable, ^task_id} ->
      %ProcessHub.StopResult{status: :ok, stopped: [{task_id, [node()]}], errors: []}
    end)
  end

  defp unique_id, do: "op-#{System.unique_integer([:positive])}"

  test "workflow + bound → cooperative WorkflowStop, no hard teardown" do
    test_pid = self()
    task_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :workflow}, runner_id: "exec1"})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^task_id -> {:ok, stub} end)

    ControlUnit
    |> stub(:workflow_stop, fn "exec1", ^task_id ->
      send(test_pid, {:workflow_stop_called, task_id})
      :ok
    end)

    assert {:ok, :cooperative} = OperativeManager.request_stop(@namespace, task_id)
    assert_receive {:workflow_stop_called, ^task_id}, 500
    # Cooperative stop does not drive the hard teardown — the SDK winds down itself.
    refute_received {:stop_operative_received, _}
  end

  test "stream → hard teardown (RemOperative), no cooperative stop" do
    test_pid = self()
    task_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :stream}, runner_id: "exec1"})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^task_id -> {:ok, stub} end)
    Quotas |> stub(:record_deallocation, fn _ns, _kind, _n -> :ok end)
    stub_hub_stop(test_pid, task_id)

    ControlUnit
    |> stub(:workflow_stop, fn _e, _o ->
      send(test_pid, {:workflow_stop_called, task_id})
      :ok
    end)

    assert :ok = OperativeManager.request_stop(@namespace, task_id)
    assert_receive {:hub_stop, ^task_id}, 500
    refute_received {:workflow_stop_called, _}
  end

  test "stop_operative releases both the operative and cell quota" do
    test_pid = self()
    task_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :workflow, cells: ["a", "b"]}})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^task_id -> {:ok, stub} end)
    stub_hub_stop(test_pid, task_id)

    Quotas
    |> stub(:record_deallocation, fn @namespace, kind, n ->
      send(test_pid, {:dealloc, kind, n})
      :ok
    end)

    assert :ok = OperativeManager.stop_operative(@namespace, task_id)

    assert_receive {:dealloc, :max_operatives, 1}, 500
    assert_receive {:dealloc, :max_cells_per_operative, 2}, 500
  end

  test "stop_operative with no cells releases only the operative quota" do
    test_pid = self()
    task_id = unique_id()
    builder = %Builder{id: task_id, factory_id: "f", kind: :workflow}
    {:ok, stub} = start_stub(%{builder: builder})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^task_id -> {:ok, stub} end)
    stub_hub_stop(test_pid, task_id)

    Quotas
    |> stub(:record_deallocation, fn @namespace, kind, n ->
      send(test_pid, {:dealloc, kind, n})
      :ok
    end)

    assert :ok = OperativeManager.stop_operative(@namespace, task_id)

    assert_receive {:dealloc, :max_operatives, 1}, 500
    # A `%Builder{}` declares no cells → no cell-quota release.
    refute_received {:dealloc, :max_cells_per_operative, _}
  end

  test "stop_operative retains state + record for a finished workflow operative" do
    task_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :workflow}})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^task_id -> {:ok, stub} end)
    Quotas |> stub(:record_deallocation, fn _ns, _kind, _n -> :ok end)
    stub_hub_stop(self(), task_id)

    # Simulate what `handle_workflow_completed` wrote before teardown: a completion
    # record + retained Cell state.
    :ok =
      Completion.write(task_id, %{
        namespace: @namespace,
        status: "ok",
        error: "",
        result: "",
        finished_at: System.system_time(:second)
      })

    wal = Paths.wal_path({task_id, "op0", "seg"})
    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, "x")

    assert :ok = OperativeManager.stop_operative(@namespace, task_id)

    # Retain-not-delete: the record and the WAL survive teardown for the window.
    assert Completion.exists?(task_id)
    assert File.exists?(wal)
  end

  test "stop_operative deletes state when there is no completion record" do
    task_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :stream}})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^task_id -> {:ok, stub} end)
    Quotas |> stub(:record_deallocation, fn _ns, _kind, _n -> :ok end)
    stub_hub_stop(self(), task_id)

    wal = Paths.wal_path({task_id, "op0", "seg"})
    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, "x")

    assert :ok = OperativeManager.stop_operative(@namespace, task_id)

    # No completion record → the class dir is deleted as before.
    refute File.dir?(Paths.class_dir(task_id))
  end
end

defmodule StubOverwatch do
  @moduledoc false
  use GenServer

  def start_link(state_reply, notify), do: GenServer.start_link(__MODULE__, {state_reply, notify})

  @impl true
  def init(s), do: {:ok, s}

  @impl true
  def handle_call(:get_overwatch_state, _from, {state_reply, _notify} = s),
    do: {:reply, state_reply, s}
end
