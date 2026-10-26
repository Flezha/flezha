defmodule FlezhaTest.Service.RelayManagerRequestStopTest do
  @moduledoc """
  §5a.1: `RelayManager.request_stop/2` routes by kind — a bound `:steps`
  relay is stopped cooperatively (`ControlUnit.steps_stop`), everything else
  takes the hard `stop_relay/2` teardown: the `:relay` walk of the
  shutdown plan, which releases BOTH the `:max_relays` and
  `:max_cells_per_relay` quota, symmetrically with `start_relay/2`
  (the cell-quota leak fix), and retains a finished steps relay's state.

  A minimal stub stands in for the Overwatch GenServer: it answers
  `:get_overwatch_state` and a stop's `{:cause, _}` (kind / runner / cell count). The hard stop itself
  rides `ProcessHub.stop_children` (the hub stop path removes the declared
  entry before the child terminates), stubbed here at the ProcessHub seam.
  """

  # async: false: writes Application env; Mimic global mode; Mimic stubs.

  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.AuthZ.Quotas
  alias Flezha.Relay.Builder
  alias Flezha.Runner.ControlUnit
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.State.{Completion, Paths}
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "op-mgr-request-stop-test"

  setup :set_mimic_global_until_exit

  defp start_stub(state_reply), do: StubOverwatch.start_link(state_reply, self())

  # The hub stop seam: `Teardown.stop_relay` pipes `ProcessHub.stop_children`
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

  test "steps + bound → cooperative StepsStop, no hard teardown" do
    test_pid = self()
    task_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{run: :steps}, runner_id: "exec1"})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^task_id -> {:ok, stub} end)

    ControlUnit
    |> stub(:steps_stop, fn "exec1", ^task_id ->
      send(test_pid, {:steps_stop_called, task_id})
      :ok
    end)

    assert {:ok, :cooperative} = RelayManager.request_stop(@namespace, task_id)
    assert_receive {:steps_stop_called, ^task_id}, 500
    # Cooperative stop does not drive the hard teardown — the SDK winds down itself.
    refute_received {:stop_relay_received, _}
  end

  test "stream → hard teardown (RemRelay), no cooperative stop" do
    test_pid = self()
    task_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{run: :flow}, runner_id: "exec1"})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^task_id -> {:ok, stub} end)
    Quotas |> stub(:record_deallocation, fn _ns, _kind, _n -> :ok end)
    stub_hub_stop(test_pid, task_id)

    ControlUnit
    |> stub(:steps_stop, fn _e, _o ->
      send(test_pid, {:steps_stop_called, task_id})
      :ok
    end)

    assert :ok = RelayManager.request_stop(@namespace, task_id)
    assert_receive {:hub_stop, ^task_id}, 500
    refute_received {:steps_stop_called, _}
  end

  test "stop_relay releases both the relay and cell quota" do
    test_pid = self()
    task_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{run: :steps, cells: ["a", "b"]}})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^task_id -> {:ok, stub} end)
    stub_hub_stop(test_pid, task_id)

    Quotas
    |> stub(:record_deallocation, fn @namespace, kind, n ->
      send(test_pid, {:dealloc, kind, n})
      :ok
    end)

    assert :ok = RelayManager.stop_relay(@namespace, task_id)

    # Named before the hub stop, in the call that read the Cells.
    assert_received {:cause, :stop}
    assert_receive {:dealloc, :max_relays, 1}, 500
    assert_receive {:dealloc, :max_cells_per_relay, 2}, 500
  end

  test "stop_relay with no cells releases only the relay quota" do
    test_pid = self()
    task_id = unique_id()
    builder = %Builder{id: task_id, factory_id: "f", run: :steps}
    {:ok, stub} = start_stub(%{builder: builder})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^task_id -> {:ok, stub} end)
    stub_hub_stop(test_pid, task_id)

    Quotas
    |> stub(:record_deallocation, fn @namespace, kind, n ->
      send(test_pid, {:dealloc, kind, n})
      :ok
    end)

    assert :ok = RelayManager.stop_relay(@namespace, task_id)

    assert_receive {:dealloc, :max_relays, 1}, 500
    # A `%Builder{}` declares no cells → no cell-quota release.
    refute_received {:dealloc, :max_cells_per_relay, _}
  end

  test "stop_relay retains state + record for a finished steps relay" do
    task_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{run: :steps}})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^task_id -> {:ok, stub} end)
    Quotas |> stub(:record_deallocation, fn _ns, _kind, _n -> :ok end)
    stub_hub_stop(self(), task_id)

    # Simulate what `handle_relay_completed` wrote before teardown: a completion
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

    assert :ok = RelayManager.stop_relay(@namespace, task_id)

    # Retain-not-delete: the record and the WAL survive teardown for the window.
    assert Completion.exists?(task_id)
    assert File.exists?(wal)
  end

  test "stop_relay deletes state when there is no completion record" do
    task_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{run: :flow}})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^task_id -> {:ok, stub} end)
    Quotas |> stub(:record_deallocation, fn _ns, _kind, _n -> :ok end)
    stub_hub_stop(self(), task_id)

    wal = Paths.wal_path({task_id, "op0", "seg"})
    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, "x")

    assert :ok = RelayManager.stop_relay(@namespace, task_id)

    # No completion record → the class dir is deleted as before.
    refute File.dir?(Paths.class_dir(task_id))
  end

  # A start answers once the relay's own node registered it, before this node's
  # copy of the hub has the row. The other members are stood in for by this
  # node: a lookup from the test process is this node's miss, a lookup from the
  # remote-call process is the member that holds the row.
  test "a stop whose row has not reached this node stops the relay a member holds" do
    test_pid = self()
    task_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{run: :flow}})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)

    HubManager
    |> stub(:get_pid, fn @namespace, ^task_id ->
      if self() == test_pid,
        do: {:error, {:relay_not_found, @namespace, task_id}},
        else: {:ok, stub}
    end)
    |> stub(:get_hub_nodes, fn @namespace -> [node()] end)

    Quotas |> stub(:record_deallocation, fn _ns, _kind, _n -> :ok end)
    stub_hub_stop(test_pid, task_id)

    assert :ok = RelayManager.stop_relay(@namespace, task_id)
    assert_received {:cause, :stop}
    assert_receive {:hub_stop, ^task_id}, 500
  end

  test "a stop no member finds the relay for answers relay_not_found" do
    task_id = unique_id()

    HubManager
    |> stub(:get_pid, fn @namespace, ^task_id ->
      {:error, {:relay_not_found, @namespace, task_id}}
    end)
    |> stub(:get_hub_nodes, fn @namespace -> [node()] end)

    assert {:error, {:relay_not_found, @namespace, ^task_id}} =
             RelayManager.stop_relay(@namespace, task_id)
  end

  # A wedged Overwatch still has a runner and Cells on its registry row; the hard
  # stop must flush that runner and give back the Cells its start allocated.
  test "a stop whose Overwatch times out takes its runner and Cells from the row" do
    test_pid = self()
    task_id = unique_id()
    prior = Application.get_env(:flezha, :overwatch_call_timeout_ms)
    Application.put_env(:flezha, :overwatch_call_timeout_ms, 200)

    on_exit(fn ->
      if prior,
        do: Application.put_env(:flezha, :overwatch_call_timeout_ms, prior),
        else: Application.delete_env(:flezha, :overwatch_call_timeout_ms)
    end)

    {:ok, stub} = start_stub(%{builder: %{run: :steps}, runner_id: "exec1"})
    on_exit(fn -> NodeTestHelper.ensure_stopped(stub) end)
    :ok = :sys.suspend(stub)

    HubManager
    |> stub(:get_pid, fn @namespace, ^task_id -> {:ok, stub} end)
    |> stub(:relay_spec, fn @namespace, ^task_id ->
      {:ok, %{id: task_id, cells: ["a", "b"]}, %{runner: "exec1", node: node()}}
    end)

    stub_hub_stop(test_pid, task_id)

    Flezha.Runner.StreamPool
    |> stub(:flush_channels, fn runner, _ms -> send(test_pid, {:flushed, runner}) end)

    Quotas
    |> stub(:record_deallocation, fn @namespace, kind, n ->
      send(test_pid, {:dealloc, kind, n})
      :ok
    end)

    assert :ok = RelayManager.stop_relay(@namespace, task_id)

    assert_receive {:hub_stop, ^task_id}, 500
    assert_receive {:dealloc, :max_relays, 1}, 500
    assert_receive {:flushed, "exec1"}, 500
    assert_receive {:dealloc, :max_cells_per_relay, 2}, 500
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

  def handle_call({:cause, cause}, _from, {state_reply, notify} = s) do
    send(notify, {:cause, cause})
    {:reply, state_reply, s}
  end
end
