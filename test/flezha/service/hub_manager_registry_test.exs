defmodule Flezha.Service.HubManagerRegistryTest do
  @moduledoc """
  Multi-node-safe registry access.

  Pattern-matching a single-element `[{_node, pid}]` registry entry raises
  `FunctionClauseError` the moment an operator is registered on more than one
  node, which is the `flz operator list` crash. These tests exercise the shared
  `HubManager.live_pid/1` resolver and inject a synthetic two-node registry entry
  to assert the consumers select the live/local pid instead of crashing.
  """
  # async: false: writes Application env; a shared on-disk root; the live controller and its fleet.
  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  alias Flezha.Convergence.Snapshot
  alias FlezhaTest.Helper.NodeTestHelper
  import FlezhaTest.Helper.HubRegistryTestHelper

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

    test "get_all_operators/1 returns the live/local pid for an operator registered on two nodes" do
      dead_node_pid = spawn(&NodeTestHelper.park/0)
      local_pid = self()

      inject_operator(@namespace, "operator_mn", [
        {:dead@nohost, dead_node_pid},
        {node(), local_pid}
      ])

      operators = HubManager.get_all_operators(@namespace)

      assert {"operator_mn", got_pid} =
               Enum.find(operators, fn {id, _} -> id == "operator_mn" end)

      assert got_pid == local_pid
    end

    test "get_all_operators/1 omits an operator that has only dead-node entries" do
      inject_operator(@namespace, "operator_dead", [{:dead@nohost, spawn(fn -> :ok end)}])

      operators = HubManager.get_all_operators(@namespace)

      assert Enum.find(operators, fn {id, _} -> id == "operator_dead" end) == nil
    end

    test "flz operator list (Flezha.list_operators/1) lists a multi-node operator without crashing" do
      # Backs `flz operator list` via OperatorManager.get_operator_ids → get_all_operators.
      dead_node_pid = spawn(&NodeTestHelper.park/0)

      inject_operator(@namespace, "operator_cli_mn", [
        {:dead@nohost, dead_node_pid},
        {node(), self()}
      ])

      assert {:ok, ids} = Flezha.list_operators(@namespace)
      assert "operator_cli_mn" in ids
    end
  end

  describe "operator_rows/1 health surface" do
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
      # `metadata[:health]` is a key nothing ever writes, so reading it reports
      # `:unknown` forever even for a producing operator. `operator_rows/1`
      # must read the loop's published per-operator health instead.
      inject_operator(@health_ns, "operator_health", [{node(), self()}])

      Snapshot.publish(%{
        operator_health:
          FlezhaTest.Helper.ConvergenceFixtures.verdicts([
            {{:operator, {@health_ns, "operator_health"}}, :healthy}
          ])
      })

      row = Enum.find(HubManager.operator_rows(@health_ns), &(&1.id == "operator_health"))
      assert row.health == :healthy
    end

    test "a failed operator reads recovering inside the unmet-intent grace, failed past it" do
      inject_operator(@health_ns, "operator_moving", [{node(), self()}])

      health_after = fn unmet_intent_ms ->
        Snapshot.publish(%{
          operator_health:
            FlezhaTest.Helper.ConvergenceFixtures.verdicts([
              {{:operator, {@health_ns, "operator_moving"}}, :failed}
            ]),
          unmet_intent_ms: unmet_intent_ms
        })

        Enum.find(HubManager.operator_rows(@health_ns), &(&1.id == "operator_moving")).health
      end

      assert health_after.(2_000) == :recovering
      assert health_after.(500_000) == :failed
    end

    test "an operator the loop has not yet observed reports :unknown, not a crash" do
      inject_operator(@health_ns, "operator_unseen", [{node(), self()}])
      Snapshot.publish(%{operator_health: %{}})

      row = Enum.find(HubManager.operator_rows(@health_ns), &(&1.id == "operator_unseen"))
      assert row.health == :unknown
    end
  end

  describe "durable_registry_pending?/0" do
    @pending_ns "mn_pending_test"

    setup %{tmp: root} do
      # Start the hub while auto-restore is off so it keeps the in-memory `:ets`
      # registry backend (no DETS file under the shared tmp root, so the
      # unstarted-hub on-disk branch stays out of these assertions). Then flip
      # the flag on — the detection only runs when auto-restore is enabled.
      # `durable_registry_pending?/0` reads EVERY running hub, so an unbound row
      # left by an earlier file answers for this one. Retire those hubs first —
      # and bring them back afterwards: `default` is shared suite-wide, and
      # leaving it down makes every later `start_operator` fail
      # `Validator.valid_namespace/1` with "no runners in namespace default".
      retired =
        HubManager.get_running_namespaces()
        |> Enum.reject(&(&1 == @pending_ns))

      Enum.each(retired, &HubManager.stop_hub/1)

      on_exit(fn -> Enum.each(retired, &ensure_hub/1) end)

      {:ok, _} = ensure_hub(@pending_ns)

      # Empty root so the unstarted-hub on-disk branch can never fire from
      # residue left by other suites — these assertions isolate the
      # running-hub `dump`-vs-`get_all_operators` discriminator.
      Application.put_env(:flezha, :process_hub_registry_root, root)
      Application.put_env(:flezha, :auto_restore_enabled, true)

      on_exit(fn -> HubManager.stop_hub(@pending_ns) end)

      :ok
    end

    test "false when every durable operator is bound to a live pid" do
      inject_operator(@pending_ns, "operator_bound", [{node(), self()}])

      refute HubManager.durable_registry_pending?()
    end

    test "true when a durable operator has no live pid (replayed but not yet reseated)" do
      # The DETS row survives a restart with a stale node entry, so `dump/1`
      # counts it while `get_all_operators/1` (live-pid filtered) drops it — the
      # registry-loaded-but-unbound window.
      inject_operator(@pending_ns, "operator_unbound", [{:dead@nohost, spawn(fn -> :ok end)}])

      assert HubManager.durable_registry_pending?()
    end
  end
end
