defmodule Flezha.Service.HubManagerRegistryTest do
  @moduledoc """
  Multi-node-safe registry access.

  Pattern-matching a single-element `[{_node, pid}]` registry entry raises
  `FunctionClauseError` the moment a relay is registered on more than one
  node, which is the `flz relay list` crash. These tests exercise the shared
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

    test "get_all_relays/1 returns the live/local pid for a relay registered on two nodes" do
      dead_node_pid = spawn(&NodeTestHelper.park/0)
      local_pid = self()

      inject_relay(@namespace, "relay_mn", [
        {:dead@nohost, dead_node_pid},
        {node(), local_pid}
      ])

      relays = HubManager.get_all_relays(@namespace)

      assert {"relay_mn", got_pid} =
               Enum.find(relays, fn {id, _} -> id == "relay_mn" end)

      assert got_pid == local_pid
    end

    test "get_pid/2 (a call's target) skips a dead-node entry, else answers the head" do
      parked = spawn(&NodeTestHelper.park/0)
      on_exit(fn -> Process.exit(parked, :kill) end)

      inject_relay(@namespace, "relay_moved", [{:dead@nohost, parked}, {node(), self()}])
      assert HubManager.get_pid(@namespace, "relay_moved") == {:ok, self()}

      inject_relay(@namespace, "relay_handover", [{node(), parked}, {node(), self()}])
      assert HubManager.get_pid(@namespace, "relay_handover") == {:ok, parked}
    end

    # The row a move leaves on a node its old copy's exit has not reached yet.
    test "get_pid/2 skips a head whose process has exited" do
      inject_relay(@namespace, "relay_left", [{node(), dead_pid()}, {node(), self()}])
      assert HubManager.get_pid(@namespace, "relay_left") == {:ok, self()}
    end

    # The same row seen from a third node, where both copies are remote.
    test "get_all_relays/1 skips a remote head whose process has exited" do
      {_peer_pid, peer} = NodeTestHelper.start_bare_peer(:live_pid_remote)
      gone = Node.spawn(peer, :erlang, :node, [])
      ref = Process.monitor(gone)
      assert_receive {:DOWN, ^ref, :process, ^gone, _}, 5_000
      moved = Node.spawn(peer, :timer, :sleep, [:infinity])

      inject_relay(@namespace, "relay_left_remote", [{peer, gone}, {peer, moved}])

      assert {"relay_left_remote", moved} in HubManager.get_all_relays(@namespace)
    end

    test "get_all_relays/1 omits a relay that has only dead-node entries" do
      inject_relay(@namespace, "relay_dead", [{:dead@nohost, spawn(fn -> :ok end)}])

      relays = HubManager.get_all_relays(@namespace)

      assert Enum.find(relays, fn {id, _} -> id == "relay_dead" end) == nil
    end

    test "flz relay list (Flezha.list_relays/1) lists a multi-node relay without crashing" do
      # Backs `flz relay list` via RelayManager.get_relay_ids → get_all_relays.
      dead_node_pid = spawn(&NodeTestHelper.park/0)

      inject_relay(@namespace, "relay_cli_mn", [
        {:dead@nohost, dead_node_pid},
        {node(), self()}
      ])

      assert {:ok, ids} = Flezha.list_relays(@namespace)
      assert "relay_cli_mn" in ids
    end
  end

  describe "relay_rows/1 health surface" do
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
      # `:unknown` forever even for a producing relay. `relay_rows/1`
      # must read the loop's published per-relay health instead.
      inject_relay(@health_ns, "relay_health", [{node(), self()}])

      Snapshot.publish(%{
        relay_health:
          FlezhaTest.Helper.ConvergenceFixtures.verdicts([
            {{:relay, {@health_ns, "relay_health"}}, :healthy}
          ])
      })

      row = Enum.find(HubManager.relay_rows(@health_ns), &(&1.id == "relay_health"))
      assert row.health == :healthy
    end

    test "a failed relay reads recovering inside the unmet-intent grace, failed past it" do
      inject_relay(@health_ns, "relay_moving", [{node(), self()}])

      health_after = fn unmet_intent_ms ->
        Snapshot.publish(%{
          relay_health:
            FlezhaTest.Helper.ConvergenceFixtures.verdicts([
              {{:relay, {@health_ns, "relay_moving"}}, :failed}
            ]),
          unmet_intent_ms: unmet_intent_ms
        })

        Enum.find(HubManager.relay_rows(@health_ns), &(&1.id == "relay_moving")).health
      end

      assert health_after.(2_000) == :recovering
      assert health_after.(500_000) == :failed
    end

    test "a relay the loop has not yet observed reports :unknown, not a crash" do
      inject_relay(@health_ns, "relay_unseen", [{node(), self()}])
      Snapshot.publish(%{relay_health: %{}})

      row = Enum.find(HubManager.relay_rows(@health_ns), &(&1.id == "relay_unseen"))
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
      # leaving it down makes every later `start_relay` fail
      # `Validator.valid_namespace/1` with "no runners in namespace default".
      retired =
        HubManager.get_running_namespaces()
        |> Enum.reject(&(&1 == @pending_ns))

      Enum.each(retired, &HubManager.stop_hub/1)

      on_exit(fn -> Enum.each(retired, &ensure_hub/1) end)

      {:ok, _} = ensure_hub(@pending_ns)

      # Empty root so the unstarted-hub on-disk branch can never fire from
      # residue left by other suites — these assertions isolate the
      # running-hub `dump`-vs-`get_all_relays` discriminator.
      Application.put_env(:flezha, :process_hub_registry_root, root)
      Application.put_env(:flezha, :auto_restore_enabled, true)

      on_exit(fn -> HubManager.stop_hub(@pending_ns) end)

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
end
