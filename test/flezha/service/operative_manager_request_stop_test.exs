defmodule FlezhaTest.Service.OperativeManagerRequestStopTest do
  @moduledoc """
  §5a.1: `OperativeManager.request_stop/2` routes by kind — a bound `:bounded`
  operative is stopped cooperatively (`ControlUnit.bounded_stop`), everything else
  takes the hard `stop_operative/2` teardown. `stop_operative/2` releases BOTH the
  `:max_operatives` and `:max_segments_per_operative` quota, symmetrically with
  `start_operative/2` (the segment-quota leak fix).

  A minimal stub stands in for the Overwatch GenServer: it answers
  `:get_overwatch_state` (kind / executor / segment count) and `:stop_operative`,
  and notifies the test when the hard stop is driven.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.AuthZ.Quotas
  alias Flezha.Executor.ControlUnit
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

  defp unique_id, do: "op-#{System.unique_integer([:positive])}"

  test "bounded + bound → cooperative BoundedStop, no hard teardown" do
    test_pid = self()
    op_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :bounded}, executor_id: "exec1"})
    on_exit(fn -> if Process.alive?(stub), do: GenServer.stop(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^op_id -> {:ok, stub} end)

    ControlUnit
    |> stub(:bounded_stop, fn "exec1", ^op_id ->
      send(test_pid, {:bounded_stop_called, op_id})
      :ok
    end)

    assert {:ok, :cooperative} = OperativeManager.request_stop(@namespace, op_id)
    assert_receive {:bounded_stop_called, ^op_id}, 500
    # Cooperative stop does not drive the hard teardown — the SDK winds down itself.
    refute_received {:stop_operative_received, _}
  end

  test "unbounded → hard teardown (RemOperative), no cooperative stop" do
    test_pid = self()
    op_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :unbounded}, executor_id: "exec1"})
    on_exit(fn -> if Process.alive?(stub), do: GenServer.stop(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^op_id -> {:ok, stub} end)
    PluginManager |> stub(:disable_operative_plugins, fn @namespace, ^op_id -> :ok end)
    Quotas |> stub(:record_deallocation, fn _ns, _kind, _n -> :ok end)

    ControlUnit
    |> stub(:bounded_stop, fn _e, _o ->
      send(test_pid, {:bounded_stop_called, op_id})
      :ok
    end)

    assert :ok = OperativeManager.request_stop(@namespace, op_id)
    assert_receive {:stop_operative_received, ^stub}, 500
    refute_received {:bounded_stop_called, _}
  end

  test "stop_operative releases both the operative and segment quota" do
    test_pid = self()
    op_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :bounded, segments: ["a", "b"]}})
    on_exit(fn -> if Process.alive?(stub), do: GenServer.stop(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^op_id -> {:ok, stub} end)
    PluginManager |> stub(:disable_operative_plugins, fn @namespace, ^op_id -> :ok end)

    Quotas
    |> stub(:record_deallocation, fn @namespace, kind, n ->
      send(test_pid, {:dealloc, kind, n})
      :ok
    end)

    assert :ok = OperativeManager.stop_operative(@namespace, op_id)

    assert_receive {:dealloc, :max_operatives, 1}, 500
    assert_receive {:dealloc, :max_segments_per_operative, 2}, 500
  end

  test "stop_operative with no segments releases only the operative quota" do
    test_pid = self()
    op_id = unique_id()
    builder = %Builder{id: op_id, factory_id: "f", kind: :bounded}
    {:ok, stub} = start_stub(%{builder: builder})
    on_exit(fn -> if Process.alive?(stub), do: GenServer.stop(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^op_id -> {:ok, stub} end)
    PluginManager |> stub(:disable_operative_plugins, fn @namespace, ^op_id -> :ok end)

    Quotas
    |> stub(:record_deallocation, fn @namespace, kind, n ->
      send(test_pid, {:dealloc, kind, n})
      :ok
    end)

    assert :ok = OperativeManager.stop_operative(@namespace, op_id)

    assert_receive {:dealloc, :max_operatives, 1}, 500
    # A `%Builder{}` declares no segments → no segment-quota release.
    refute_received {:dealloc, :max_segments_per_operative, _}
  end

  test "stop_operative retains state + record for a finished bounded operative" do
    op_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :bounded}})
    on_exit(fn -> if Process.alive?(stub), do: GenServer.stop(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^op_id -> {:ok, stub} end)
    PluginManager |> stub(:disable_operative_plugins, fn @namespace, ^op_id -> :ok end)
    Quotas |> stub(:record_deallocation, fn _ns, _kind, _n -> :ok end)

    # Simulate what `handle_bounded_completed` wrote before teardown: a completion
    # record + retained Segment state.
    :ok =
      Completion.write(op_id, %{
        namespace: @namespace,
        status: "ok",
        error: "",
        result: "",
        finished_at: System.system_time(:second)
      })

    wal = Path.join([Registry.state_root(), op_id, "op0", "seg.wal"])
    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, "x")

    assert :ok = OperativeManager.stop_operative(@namespace, op_id)

    # Retain-not-delete: the record and the WAL survive teardown for the window.
    assert Completion.exists?(op_id)
    assert File.exists?(wal)
  end

  test "stop_operative deletes state when there is no completion record" do
    op_id = unique_id()
    {:ok, stub} = start_stub(%{builder: %{kind: :unbounded}})
    on_exit(fn -> if Process.alive?(stub), do: GenServer.stop(stub) end)

    HubManager |> stub(:get_pid, fn @namespace, ^op_id -> {:ok, stub} end)
    PluginManager |> stub(:disable_operative_plugins, fn @namespace, ^op_id -> :ok end)
    Quotas |> stub(:record_deallocation, fn _ns, _kind, _n -> :ok end)

    wal = Path.join([Registry.state_root(), op_id, "op0", "seg.wal"])
    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, "x")

    assert :ok = OperativeManager.stop_operative(@namespace, op_id)

    # No completion record → the class dir is deleted as before.
    refute File.dir?(Path.join(Registry.state_root(), op_id))
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

  @impl true
  def handle_call(:stop_operative, _from, {_state_reply, notify} = s) do
    if notify, do: send(notify, {:stop_operative_received, self()})
    {:reply, :ok, s}
  end
end
