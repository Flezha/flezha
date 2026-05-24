defmodule Flezha.Service.HubManagerRegistryTest do
  @moduledoc """
  Multi-node-safe registry access (self-healing-control-plane, Phase 3 / D9).

  `get_all_relays/1`, `executor_switch/3`, and `FleetManager.executor_reroute/4`
  historically pattern-matched a single-element `[{_node, pid}]` registry entry,
  raising `FunctionClauseError` the moment a relay was registered on more than
  one node (the `flz relay list` crash). These tests exercise the shared
  `HubManager.live_pid/1` resolver and inject a synthetic two-node registry entry
  to assert the consumers select the live/local pid instead of crashing.
  """
  use ExUnit.Case, async: false

  alias Flezha.Boot.RecoveryHooks
  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias ProcessHub.Service.ProcessRegistry

  @namespace "mn_registry_test"

  defp dead_pid() do
    pid = spawn(fn -> :ok end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    pid
  end

  describe "live_pid/1" do
    test "returns nil for an empty node_pids list" do
      assert HubManager.live_pid([]) == nil
    end

    test "returns nil for a non-list" do
      assert HubManager.live_pid(nil) == nil
    end

    test "returns the local pid when it is alive" do
      assert HubManager.live_pid([{node(), self()}]) == self()
    end

    test "drops a dead-node entry and keeps the live local pid" do
      pid = self()

      assert HubManager.live_pid([{:dead@nohost, dead_pid()}, {node(), pid}]) == pid
    end

    test "returns nil when the only local entry's pid is dead" do
      assert HubManager.live_pid([{node(), dead_pid()}]) == nil
    end

    test "returns nil when every entry is on a dead node" do
      assert HubManager.live_pid([{:dead1@nohost, self()}, {:dead2@nohost, self()}]) == nil
    end
  end

  describe "multi-node-safe consumers" do
    setup do
      {:ok, _} = ensure_hub(@namespace)
      on_exit(fn -> HubManager.stop_hub(@namespace) end)
      :ok
    end

    test "get_all_relays/1 returns the live/local pid for a relay registered on two nodes" do
      dead_node_pid = spawn(fn -> Process.sleep(:infinity) end)
      local_pid = self()

      inject_relay(@namespace, "relay_mn", [
        {:dead@nohost, dead_node_pid},
        {node(), local_pid}
      ])

      relays = HubManager.get_all_relays(@namespace)

      assert {"relay_mn", got_pid} = Enum.find(relays, fn {id, _} -> id == "relay_mn" end)
      assert got_pid == local_pid
    end

    test "get_all_relays/1 omits a relay that has only dead-node entries" do
      inject_relay(@namespace, "relay_dead", [{:dead@nohost, spawn(fn -> :ok end)}])

      relays = HubManager.get_all_relays(@namespace)

      assert Enum.find(relays, fn {id, _} -> id == "relay_dead" end) == nil
    end

    test "flz relay list (Flezha.list_relays/1) lists a multi-node relay without crashing" do
      # Backs `flz relay list` via RelayManager.get_relay_ids → get_all_relays.
      dead_node_pid = spawn(fn -> Process.sleep(:infinity) end)

      inject_relay(@namespace, "relay_cli_mn", [
        {:dead@nohost, dead_node_pid},
        {node(), self()}
      ])

      assert {:ok, ids} = Flezha.list_relays(@namespace)
      assert "relay_cli_mn" in ids
    end
  end

  describe "dead-node purge on cold-boot replay (pre_recovery_replay)" do
    @purge_ns "mn_purge_test"

    setup do
      {:ok, _} = ensure_hub(@purge_ns)
      on_exit(fn -> HubManager.stop_hub(@purge_ns) end)
      :ok
    end

    test "drops {dead_node, pid} pairs, keeping the live local entry" do
      hub_id = Identificator.hub(@purge_ns)

      # Relay only on a dead node: the live registry entry is removed (the replay
      # dump, captured by ProcessHub before this hook, still re-places it).
      inject_relay(@purge_ns, "relay_dead_only", [{:dead@nohost, spawn(fn -> :ok end)}])

      # Relay on dead + live(local): only the dead pair is dropped.
      inject_relay(@purge_ns, "relay_mixed", [
        {:dead@nohost, spawn(fn -> :ok end)},
        {node(), self()}
      ])

      assert :ok = RecoveryHooks.pre_recovery_replay(hub_id, %{})

      dump = ProcessRegistry.dump(hub_id)

      refute Map.has_key?(dump, "relay_dead_only")

      assert {_spec, node_pids, _meta} = Map.get(dump, "relay_mixed")
      assert node_pids == [{node(), self()}]
    end
  end

  defp ensure_hub(ns) do
    case HubManager.start_hub(ns) do
      {:ok, pid} -> {:ok, pid}
      {:ok, pid, _info} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end

  # Inject a synthetic registry entry with an explicit `node_pids` list so we can
  # reproduce a multi-node relay without spinning up a real peer node + executor.
  defp inject_relay(ns, relay_id, node_pids) do
    hub_id = Identificator.hub(ns)

    child_spec = %{
      id: relay_id,
      start: {Flezha.Relay.Overwatch, :start_link, [{ns, relay_id}]}
    }

    :ok = ProcessRegistry.bulk_insert(hub_id, %{relay_id => {child_spec, node_pids, %{}}})
  end
end
