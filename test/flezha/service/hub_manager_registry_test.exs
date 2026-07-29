defmodule Flezha.Service.HubManagerRegistryTest do
  @moduledoc """
  Multi-node-safe registry access (self-healing-control-plane, Phase 3 / D9).

  `get_all_operatives/1`, `runner_switch/3`, and `FleetManager.runner_reroute/4`
  historically pattern-matched a single-element `[{_node, pid}]` registry entry,
  raising `FunctionClauseError` the moment a operative was registered on more than
  one node (the `flz operative list` crash). These tests exercise the shared
  `HubManager.live_pid/1` resolver and inject a synthetic two-node registry entry
  to assert the consumers select the live/local pid instead of crashing.
  """
  use ExUnit.Case, async: false

  alias Flezha.Convergence.Snapshot
  import FlezhaTest.Helper.HubRegistryTestHelper

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager

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

    test "get_all_operatives/1 returns the live/local pid for a operative registered on two nodes" do
      dead_node_pid = spawn(fn -> Process.sleep(:infinity) end)
      local_pid = self()

      inject_operative(@namespace, "operative_mn", [
        {:dead@nohost, dead_node_pid},
        {node(), local_pid}
      ])

      operatives = HubManager.get_all_operatives(@namespace)

      assert {"operative_mn", got_pid} =
               Enum.find(operatives, fn {id, _} -> id == "operative_mn" end)

      assert got_pid == local_pid
    end

    test "get_all_operatives/1 omits a operative that has only dead-node entries" do
      inject_operative(@namespace, "operative_dead", [{:dead@nohost, spawn(fn -> :ok end)}])

      operatives = HubManager.get_all_operatives(@namespace)

      assert Enum.find(operatives, fn {id, _} -> id == "operative_dead" end) == nil
    end

    test "flz operative list (Flezha.list_operatives/1) lists a multi-node operative without crashing" do
      # Backs `flz operative list` via OperativeManager.get_operative_ids → get_all_operatives.
      dead_node_pid = spawn(fn -> Process.sleep(:infinity) end)

      inject_operative(@namespace, "operative_cli_mn", [
        {:dead@nohost, dead_node_pid},
        {node(), self()}
      ])

      assert {:ok, ids} = Flezha.list_operatives(@namespace)
      assert "operative_cli_mn" in ids
    end
  end

  describe "operative_rows/1 health surface" do
    @health_ns "mn_health_test"

    setup do
      {:ok, _} = ensure_hub(@health_ns)
      prev = Snapshot.current()

      on_exit(fn ->
        Snapshot.publish(prev)
        HubManager.stop_hub(@health_ns)
      end)

      :ok
    end

    test "reflects the Convergence Loop's classified health, not the absent metadata key" do
      # Regression: operative_rows/1 used to read `metadata[:health]`, a key nothing
      # ever writes, so every operative reported `:unknown` forever even while
      # producing. It must instead read the loop's published per-operative health.
      inject_operative(@health_ns, "operative_health", [{node(), self()}])

      Snapshot.publish(%{operative_health: %{{@health_ns, "operative_health"} => :healthy}})

      row = Enum.find(HubManager.operative_rows(@health_ns), &(&1.id == "operative_health"))
      assert row.health == :healthy
    end

    test "a operative the loop has not yet observed reports :unknown, not a crash" do
      inject_operative(@health_ns, "operative_unseen", [{node(), self()}])
      Snapshot.publish(%{operative_health: %{}})

      row = Enum.find(HubManager.operative_rows(@health_ns), &(&1.id == "operative_unseen"))
      assert row.health == :unknown
    end
  end

  describe "durable_registry_pending?/0" do
    @pending_ns "mn_pending_test"

    setup do
      # Start the hub while auto-restore is off so it keeps the in-memory `:ets`
      # registry backend (no DETS file under the shared tmp root, so the
      # unstarted-hub on-disk branch stays out of these assertions). Then flip
      # the flag on — the detection only runs when auto-restore is enabled.
      # `durable_registry_pending?/0` reads EVERY running hub, so an unbound row
      # left by an earlier file answers for this one. Retire those hubs first —
      # and bring them back afterwards: `default` is shared suite-wide, and
      # leaving it down makes every later `start_operative` fail
      # `Validator.valid_namespace/1` with "no runners in namespace default".
      retired =
        HubManager.get_running_namespaces()
        |> Enum.reject(&(&1 == @pending_ns))

      Enum.each(retired, &HubManager.stop_hub/1)

      # Restoring the hub is not enough: each runner's `child_registered` hook
      # was registered against the hub we just stopped, and without it a fresh
      # hub never seeds the StreamChannels' operative routes — so every later
      # data-plane test (state reads, migration, drain handover) silently gets
      # no frames back.
      on_exit(fn ->
        Enum.each(retired, fn ns ->
          ensure_hub(ns)
          Enum.each(FleetManager.active_runners(ns), &HubManager.register_handler(ns, &1))
        end)
      end)

      {:ok, _} = ensure_hub(@pending_ns)

      prev_restore = Application.get_env(:flezha, :auto_restore_enabled)
      prev_root = Application.get_env(:flezha, :process_hub_registry_root)
      # Empty root so the unstarted-hub on-disk branch can never fire from
      # residue left by other suites — these assertions isolate the
      # running-hub `dump`-vs-`get_all_operatives` discriminator.
      root =
        Path.join(System.tmp_dir!(), "flezha_pending_test_#{System.unique_integer([:positive])}")

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

    test "false when every durable operative is bound to a live pid" do
      inject_operative(@pending_ns, "operative_bound", [{node(), self()}])

      refute HubManager.durable_registry_pending?()
    end

    test "true when a durable operative has no live pid (replayed but not yet reseated)" do
      # The DETS row survives a restart with a stale node entry, so `dump/1`
      # counts it while `get_all_operatives/1` (live-pid filtered) drops it — the
      # registry-loaded-but-unbound window.
      inject_operative(@pending_ns, "operative_unbound", [{:dead@nohost, spawn(fn -> :ok end)}])

      assert HubManager.durable_registry_pending?()
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:flezha, key)
  defp restore_env(key, val), do: Application.put_env(:flezha, key, val)
end
