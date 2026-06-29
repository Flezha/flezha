defmodule FlezhaTest.System.TaskKindE2ETest do
  @moduledoc """
  End-to-end proof for `operative-task-kind` against a live Rust executor: a
  unit started with `kind: :bounded` flows `AddOperative --kind task` → SDK spawns
  a task → runs `run` once → emits `BoundedCompleted` → the controller drives
  deregister-on-completion. The whole loop is observable as the unit
  disappearing from the durable registry without an explicit stop. Tagged
  `:system` (boots a real subprocess); run with
  `mix test --include system test/system/task_kind_e2e_test.exs`.
  """

  use ExUnit.Case, async: false

  alias Flezha.Executor.FleetManager
  alias Flezha.Operative.Builder
  alias Flezha.Service.EventManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.PortPilot
  alias Flezha.Service.OperativeManager

  @moduletag :system
  @namespace "task_kind_e2e"
  @factory "echo_bounded"

  setup do
    on_exit(fn ->
      try do
        OperativeManager.stop_all_operatives(@namespace)
      catch
        _, _ -> :ok
      end

      try do
        FleetManager.stop_namespace(@namespace)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  @tag :task_kind_e2e
  test "task runs once on start and is deregistered on completion" do
    {port, _ce_id} = start_executor()
    on_exit(fn -> ensure_port_dead(port) end)

    operative_id = "task_e2e_#{System.unique_integer([:positive])}"

    builder = %Builder{
      id: operative_id,
      factory_id: @factory,
      kind: :bounded,
      init_values: %{"msg" => "hi"},
      opts: []
    }

    # Start succeeds even for a near-instant task: a run-once unit may
    # complete (and self-deregister) before its binding is observable, and a
    # clean exit during start confirmation is not a failure.
    assert {:ok, _info} = OperativeManager.start_operative(@namespace, builder)

    # The SDK runs the task once and ships `BoundedCompleted`; the controller
    # tears it down. Deregister-on-completion: the unit drops out of the
    # durable registry with no explicit stop — proof the full loop ran.
    assert wait_until(fn -> not registered?(operative_id) end, 5_000),
           "task was never deregistered after completion"
  end

  defp registered?(operative_id) do
    match?({:ok, pid} when is_pid(pid), HubManager.get_pid(@namespace, operative_id))
  end

  defp start_executor() do
    {:ok, current_path} = :file.get_cwd()

    command =
      "cargo run --quiet --manifest-path=#{current_path}/test/apps/rust_executor/Cargo.toml " <>
        "--target-dir=./target -- " <> @namespace

    EventManager.add_handler(:executor_connected_event)
    {port, _command} = PortPilot.open_port(command, transport: :uds)

    ce_id =
      receive do
        {:executor_connected_event, {@namespace, ce_id}} -> ce_id
      after
        120_000 -> raise "compute engine never connected"
      end

    EventManager.rem_handler(:executor_connected_event)
    {port, ce_id}
  end

  defp wait_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(25)
        do_wait_until(fun, deadline)
    end
  end

  defp ensure_port_dead(port) do
    if is_port(port) and Enum.member?(:erlang.ports(), port) do
      try do
        Port.close(port)
      catch
        _, _ -> :ok
      end
    end
  end
end
