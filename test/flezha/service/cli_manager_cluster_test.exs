defmodule Flezha.Service.CliManager.ClusterTest do
  @moduledoc """
  CLI surface for `flz operative list` and `flz cluster status`. The data
  sources (`HubManager` registry access) are stubbed so the rendering
  and shaping are asserted deterministically.

  After `convergence-self-healing` §4 deleted the
  `Flezha.Reconciler.Leader` GenServer, the cluster_status leader
  column degrades to the local node — the cluster-wide registry-
  mutating actions that needed leader election are gone (process_hub's
  coordinator handles election where it is still required).
  """
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureIO

  alias Flezha.Service.CliManager
  alias Flezha.Service.HubManager

  describe "list_operatives/2 (health column)" do
    test "json output carries per-operative health, runner, and node" do
      stub(HubManager, :operative_rows, fn _ns ->
        [
          %{id: "r1", health: :healthy, runner: "e1", node: :a@h},
          %{id: "r2", health: :failed, runner: nil, node: nil}
        ]
      end)

      out = capture_io(fn -> CliManager.list_operatives("shop", "json") end)

      assert {:ok, decoded} = Jason.decode(out)
      assert decoded["namespace"] == "shop"

      assert decoded["operatives"] == [
               %{"id" => "r1", "health" => "healthy", "runner" => "e1", "node" => "a@h"},
               %{"id" => "r2", "health" => "failed", "runner" => nil, "node" => nil}
             ]
    end

    test "text output shows a HEALTH column and surfaces a FAILED, unplaced operative" do
      stub(HubManager, :operative_rows, fn _ns ->
        [%{id: "r2", health: :failed, runner: nil, node: nil}]
      end)

      out = capture_io(fn -> CliManager.list_operatives("shop", "text") end)

      assert out =~ "HEALTH"
      assert out =~ "r2"
      assert out =~ "failed"
      assert out =~ "(unplaced)"
    end

    test "empty registry renders a friendly message, not a crash" do
      stub(HubManager, :operative_rows, fn _ns -> [] end)
      out = capture_io(fn -> CliManager.list_operatives("shop", "text") end)
      assert out =~ "No operatives found"
    end
  end

  describe "cluster_status/2" do
    test "json reports the local node as leader, per-node operative counts, unplaced, and a health rollup" do
      stub(HubManager, :get_hub_nodes, fn _ns, true -> [node()] end)

      stub(HubManager, :operative_rows, fn _ns ->
        [
          %{id: "r1", health: :healthy, runner: "e1", node: node()},
          %{id: "r2", health: :degraded, runner: "e2", node: node()},
          %{id: "r4", health: :failed, runner: nil, node: nil}
        ]
      end)

      stub(HubManager, :recovery_state, fn _hub_id -> {:ok, :normal} end)
      stub(HubManager, :resolved_marker_path, fn _hub_id -> "/tmp/cluster.healthy" end)

      out = capture_io(fn -> CliManager.cluster_status("shop", "json") end)

      assert {:ok, d} = Jason.decode(out)
      assert d["leader"] == to_string(node())
      assert d["is_leader"] == true
      assert d["unplaced_operatives"] == 1
      assert d["operative_health"] == %{"healthy" => 1, "degraded" => 1, "failed" => 1}
    end

    test "text output names the local node as leader" do
      stub(HubManager, :get_hub_nodes, fn _ns, true -> [node()] end)

      stub(HubManager, :operative_rows, fn _ns ->
        [%{id: "r1", health: :healthy, runner: "e1", node: node()}]
      end)

      stub(HubManager, :recovery_state, fn _hub_id -> {:ok, :normal} end)
      stub(HubManager, :resolved_marker_path, fn _hub_id -> "/tmp/cluster.healthy" end)

      out = capture_io(fn -> CliManager.cluster_status("shop", "text") end)

      assert out =~ "Cluster status for namespace 'shop'"
      assert out =~ "Leader:"
      assert out =~ to_string(node())
      assert out =~ "1 operative(s)"
      assert out =~ "[leader]"
      assert out =~ "healthy=1"
    end
  end
end
