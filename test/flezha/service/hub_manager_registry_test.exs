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

  describe "durable_registry_pending?/0" do
    @pending_ns "mn_pending_test"

    setup do
      # Start the hub while auto-restore is off so it keeps the in-memory `:ets`
      # registry backend (no DETS file under the shared tmp root, so the
      # unstarted-hub on-disk branch stays out of these assertions). Then flip
      # the flag on — the detection only runs when auto-restore is enabled.
      {:ok, _} = ensure_hub(@pending_ns)

      prev_restore = Application.get_env(:flezha, :auto_restore_enabled)
      prev_root = Application.get_env(:flezha, :process_hub_registry_root)
      # Empty root so the unstarted-hub on-disk branch can never fire from
      # residue left by other suites — these assertions isolate the
      # running-hub `dump`-vs-`get_all_relays` discriminator.
      root = Path.join(System.tmp_dir!(), "flezha_pending_test_#{System.unique_integer([:positive])}")
      File.mkdir_p!(root)
      Application.put_env(:flezha, :process_hub_registry_root, root)
      Application.put_env(:flezha, :auto_restore_enabled, true)

      on_exit(fn ->
        restore_env(:auto_restore_enabled, prev_restore)
        restore_env(:process_hub_registry_root, prev_root)
        File.rm_rf(root)
        HubManager.stop_hub(@pending_ns)
      end)

      :ok
    end

    test "false when every durable relay is bound to a live pid" do
      inject_relay(@pending_ns, "relay_bound", [{node(), self()}])

      refute HubManager.durable_registry_pending?()
    end

    test "true when a durable relay has no live pid (replayed but not yet reseated)" do
      # The DETS row survives a restart with a stale node entry, so `dump/1`
      # counts it while `get_all_relays/1` (live-pid filtered) drops it — the
      # registry-loaded-but-unbound window.
      inject_relay(@pending_ns, "relay_unbound", [{:dead@nohost, spawn(fn -> :ok end)}])

      assert HubManager.durable_registry_pending?()
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:flezha, key)
  defp restore_env(key, val), do: Application.put_env(:flezha, key, val)

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
