defmodule FlezhaTest.System.DynamicPartitionE2ETest do
  @moduledoc """
  End-to-end proof for `relay-dynamic-partitioning` against a live Rust
  executor: an input for a fresh key lazily creates a Partition whose state
  is shipped (controller writes a WAL file); after the 200ms TTL the reaper
  evicts it, ships `delete_partition_state`, and the controller drops the
  files — the SDK reaper → delete-RPC → agent-handler path with no other
  end-to-end coverage. Tagged `:system` (boots a real subprocess); run with
  `mix test --include system test/system/dynamic_partition_e2e_test.exs`.
  """

  use ExUnit.Case, async: false

  alias Flezha.Executor.FleetManager
  alias Flezha.Relay.Builder
  alias Flezha.Service.EventManager
  alias Flezha.Service.PortPilot
  alias Flezha.Service.RelayManager
  alias Flezha.State.Registry

  @moduletag :system
  @namespace "dynamic_partition_e2e"
  @factory "dynamic_partition"
  @shard "dynamic_partition_shard"

  setup do
    on_exit(fn ->
      try do
        RelayManager.stop_all_relays(@namespace)
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

  @tag :dynamic_partition_e2e
  test "lazy-created Partition's state is shipped, then reaped + deleted controller-side" do
    {port, ce_id} = start_executor()
    on_exit(fn -> ensure_port_dead(port) end)

    relay_id = "dyn_e2e_#{System.unique_integer([:positive])}"
    key = "k#{System.unique_integer([:positive])}"
    partition_id = {relay_id, @shard, key}

    builder = %Builder{id: relay_id, factory_id: @factory, init_values: %{}, opts: []}
    assert {:ok, info} = RelayManager.start_relay(@namespace, builder)
    assert info.executor_id == ce_id

    # An input for a never-seen key: the SDK lazily creates the Partition,
    # runs it, and ships the Change. The controller writes a WAL file.
    RelayManager.send_io(relay_id, @namespace, "#{key}:hello", %{"input_type" => "2"})

    assert wait_until(fn -> File.exists?(Registry.wal_path(partition_id)) end, 5_000),
           "controller never wrote a WAL file for the lazily-created Partition"

    # No further input: after the 200ms idle TTL the reaper evicts the
    # Partition and ships delete_partition_state; the controller removes
    # the file. Generous budget for sweep cadence + wire + teardown.
    assert wait_until(fn -> not File.exists?(Registry.wal_path(partition_id)) end, 5_000),
           "reaper/delete-RPC never removed the evicted Partition's WAL file"

    assert Registry.snapshot_paths(partition_id) == []
  end

  defp start_executor() do
    {:ok, current_path} = :file.get_cwd()

    command =
      "cargo run --quiet --manifest-path=#{current_path}/test/apps/rust_executor/Cargo.toml " <>
        "--target-dir=./target -- " <> @namespace

    EventManager.add_handler(:executor_connected_event)
    # 200ms idle TTL so the dynamic_partition relay's reaper evicts + ships
    # delete_partition_state within the test (default is no eviction).
    {port, _command} =
      PortPilot.open_port(command, transport: :uds, env: [{"DYN_TEST_TTL_MS", "200"}])

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
