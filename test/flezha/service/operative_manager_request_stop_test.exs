defmodule FlezhaTest.Service.OperativeManagerRequestStopTest do
  @moduledoc """
  §5a.1: `OperativeManager.request_stop/2` routes by kind — a bound `:workflow`
  operative is stopped cooperatively (`ControlUnit.workflow_stop`), everything else
  takes the hard `stop_operative/2` teardown. `stop_operative/2` releases BOTH the
  `:max_operatives` and `:max_entities_per_operative` quota, symmetrically with
  `start_operative/2` (the entity-quota leak fix).

  A minimal stub stands in for the Overwatch GenServer: it answers
  `:get_overwatch_state` (kind / runner / entity count). The hard stop itself
  rides `ProcessHub.stop_children` (the hub stop path removes the declared
  entry before the child terminates), stubbed here at the ProcessHub seam.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.AuthZ.Quotas
  alias Flezha.Runner.ControlUnit
  alias Flezha.Operative.Builder
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.{Completion, Registry, TestHelpers}

  @namespace "op-mgr-request-stop-test"

  setup :set_mimic_global

  setup do
    # Isolate the on-disk state dir the teardown deletes-or-retains.
    base = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    :ok
  end

  defp start_stub(state_reply), do: StubOverwatch.start_link(state_reply, self())

  # The hub stop seam: `do_stop_operative` pipes `ProcessHub.stop_children`
  # into `Future.await`; stub both ends and signal the test.
  defp stub_hub_stop(test_pid, proc_id) do
    ProcessHub
    |> stub(:stop_children, fn _hub_id, [^proc_id], _opts ->
      send(test_pid, {:hub_stop, proc_id})
      {:awaitable, proc_id}
    end)

    ProcessHub.Future
    |> stub(:await, fn {:awaitable, ^proc_id} ->
      %ProcessHub.StopResult{status: :ok, stopped: [{proc_id, [node()]}], errors: []}
    end)
  end

  defp unique_id, do: "op-#{System.unique_integer([:positive])}"

  test "workflow + bound → cooperative WorkflowStop, no hard teardown" do
    test_pid = self()
    proc_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :workflow}, runner_id: "exec1"})
    on_exit(fn -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^proc_id -> {:ok, stub} end)

    ControlUnit
    |> stub(:workflow_stop, fn "exec1", ^proc_id ->
      send(test_pid, {:workflow_stop_called, proc_id})
      :ok
    end)

    assert {:ok, :cooperative} = OperativeManager.request_stop(@namespace, proc_id)
    assert_receive {:workflow_stop_called, ^proc_id}, 500
    # Cooperative stop does not drive the hard teardown — the SDK winds down itself.
    refute_received {:stop_operative_received, _}
  end

  test "stream → hard teardown (RemOperative), no cooperative stop" do
    test_pid = self()
    proc_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :stream}, runner_id: "exec1"})
    on_exit(fn -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^proc_id -> {:ok, stub} end)
    PluginManager |> stub(:disable_operative_plugins, fn @namespace, ^proc_id -> :ok end)
    Quotas |> stub(:record_deallocation, fn _ns, _kind, _n -> :ok end)
    stub_hub_stop(test_pid, proc_id)

    ControlUnit
    |> stub(:workflow_stop, fn _e, _o ->
      send(test_pid, {:workflow_stop_called, proc_id})
      :ok
    end)

    assert :ok = OperativeManager.request_stop(@namespace, proc_id)
    assert_receive {:hub_stop, ^proc_id}, 500
    refute_received {:workflow_stop_called, _}
  end

  test "stop_operative releases both the operative and entity quota" do
    test_pid = self()
    proc_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :workflow, entities: ["a", "b"]}})
    on_exit(fn -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^proc_id -> {:ok, stub} end)
    PluginManager |> stub(:disable_operative_plugins, fn @namespace, ^proc_id -> :ok end)
    stub_hub_stop(test_pid, proc_id)

    Quotas
    |> stub(:record_deallocation, fn @namespace, kind, n ->
      send(test_pid, {:dealloc, kind, n})
      :ok
    end)

    assert :ok = OperativeManager.stop_operative(@namespace, proc_id)

    assert_receive {:dealloc, :max_operatives, 1}, 500
    assert_receive {:dealloc, :max_entities_per_operative, 2}, 500
  end

  test "stop_operative with no entities releases only the operative quota" do
    test_pid = self()
    proc_id = unique_id()
    builder = %Builder{id: proc_id, factory_id: "f", kind: :workflow}
    {:ok, stub} = start_stub(%{builder: builder})
    on_exit(fn -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^proc_id -> {:ok, stub} end)
    PluginManager |> stub(:disable_operative_plugins, fn @namespace, ^proc_id -> :ok end)
    stub_hub_stop(test_pid, proc_id)

    Quotas
    |> stub(:record_deallocation, fn @namespace, kind, n ->
      send(test_pid, {:dealloc, kind, n})
      :ok
    end)

    assert :ok = OperativeManager.stop_operative(@namespace, proc_id)

    assert_receive {:dealloc, :max_operatives, 1}, 500
    # A `%Builder{}` declares no entities → no entity-quota release.
    refute_received {:dealloc, :max_entities_per_operative, _}
  end

  test "stop_operative retains state + record for a finished workflow operative" do
    proc_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :workflow}})
    on_exit(fn -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^proc_id -> {:ok, stub} end)
    PluginManager |> stub(:disable_operative_plugins, fn @namespace, ^proc_id -> :ok end)
    Quotas |> stub(:record_deallocation, fn _ns, _kind, _n -> :ok end)
    stub_hub_stop(self(), proc_id)

    # Simulate what `handle_workflow_completed` wrote before teardown: a completion
    # record + retained Entity state.
    :ok =
      Completion.write(proc_id, %{
        namespace: @namespace,
        status: "ok",
        error: "",
        result: "",
        finished_at: System.system_time(:second)
      })

    wal = Registry.wal_path({proc_id, "op0", "seg"})
    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, "x")

    assert :ok = OperativeManager.stop_operative(@namespace, proc_id)

    # Retain-not-delete: the record and the WAL survive teardown for the window.
    assert Completion.exists?(proc_id)
    assert File.exists?(wal)
  end

  test "stop_operative deletes state when there is no completion record" do
    proc_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :stream}})
    on_exit(fn -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^proc_id -> {:ok, stub} end)
    PluginManager |> stub(:disable_operative_plugins, fn @namespace, ^proc_id -> :ok end)
    Quotas |> stub(:record_deallocation, fn _ns, _kind, _n -> :ok end)
    stub_hub_stop(self(), proc_id)

    wal = Registry.wal_path({proc_id, "op0", "seg"})
    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, "x")

    assert :ok = OperativeManager.stop_operative(@namespace, proc_id)

    # No completion record → the class dir is deleted as before.
    refute File.dir?(Registry.class_dir(proc_id))
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
