defmodule FlezhaTest.System.DynamicEntityE2ETest do
  @moduledoc """
  End-to-end proof for dynamic entity mode against a live Rust
  runner: an input for a fresh key lazily creates an Entity whose state
  is shipped (controller writes a WAL file); after the 200ms TTL the reaper
  evicts it, ships `delete_entity_state`, and the controller drops the
  files — the SDK reaper → delete-RPC → agent-handler path with no other
  end-to-end coverage. Tagged `:system` (boots a real subprocess); run with
  `mix test --include system test/system/dynamic_entity_e2e_test.exs`.
  """

  use ExUnit.Case, async: false

  alias Flezha.Runner.FleetManager
  alias Flezha.Operative.Builder
  alias Flezha.Service.EventManager
  alias Flezha.Service.PortPilot
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Registry

  @moduletag :system
  @namespace "dynamic_entity_e2e"
  @factory "dynamic_entity"
  @procedure "dynamic_entity_procedure"

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

  @tag :dynamic_entity_e2e
  test "lazy-created Entity's state is shipped, then reaped + deleted controller-side" do
    {port, ce_id} = start_runner()
    on_exit(fn -> ensure_port_dead(port) end)

    operative_id = "dyn_e2e_#{System.unique_integer([:positive])}"
    key = "k#{System.unique_integer([:positive])}"
    entity_id = {operative_id, @procedure, key}

    builder = %Builder{id: operative_id, factory_id: @factory, init_values: %{}, opts: []}
    assert {:ok, info} = OperativeManager.start_operative(@namespace, builder)
    assert info.runner_id == ce_id

    # An input for a never-seen key: the SDK lazily creates the Entity,
    # runs it, and ships the Change. The controller writes a WAL file.
    OperativeManager.send_io(operative_id, @namespace, "#{key}:hello", %{"input_type" => "2"})

    assert wait_until(fn -> File.exists?(Registry.wal_path(entity_id)) end, 5_000),
           "controller never wrote a WAL file for the lazily-created Entity"

    # No further input: after the 200ms idle TTL the reaper evicts the
    # Entity and ships delete_entity_state; the controller removes
    # the file. Generous budget for sweep cadence + wire + teardown.
    assert wait_until(fn -> not File.exists?(Registry.wal_path(entity_id)) end, 5_000),
           "reaper/delete-RPC never removed the evicted Entity's WAL file"

    assert Registry.snapshot_paths(entity_id) == []
  end

  defp start_runner() do
    {:ok, current_path} = :file.get_cwd()

    command =
      "cargo run --quiet --manifest-path=#{current_path}/test/apps/rust_runner/Cargo.toml " <>
        "--target-dir=./target -- " <> @namespace

    EventManager.add_handler(:runner_connected_event)
    # 200ms idle TTL so the dynamic_entity operative's reaper evicts + ships
    # delete_entity_state within the test (default is no eviction).
    {port, _command} =
      PortPilot.open_port(command, transport: :uds, env: [{"DYN_TEST_TTL_MS", "200"}])

    ce_id =
      receive do
        {:runner_connected_event, {@namespace, ce_id}} -> ce_id
      after
        120_000 -> raise "compute engine never connected"
      end

    EventManager.rem_handler(:runner_connected_event)
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
