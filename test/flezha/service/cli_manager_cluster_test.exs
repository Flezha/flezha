defmodule Flezha.Service.CliManager.ClusterTest do
  @moduledoc """
  CLI surface for the self-healing control plane (Phase 10.2/10.3): the
  `flz relay list` health column and `flz cluster status` (nodes, leader,
  per-node relay counts, health rollup). The data sources (`HubManager`
  registry access + `Reconciler.Leader`) are stubbed so the rendering and
  shaping are asserted deterministically.
  """
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureIO

  alias Flezha.Reconciler.Leader
  alias Flezha.Service.CliManager
  alias Flezha.Service.HubManager

  describe "list_relays/2 (health column)" do
    test "json output carries per-relay health, executor, and node" do
      stub(HubManager, :relay_rows, fn _ns ->
        [
          %{id: "r1", health: :healthy, executor: "e1", node: :a@h},
          %{id: "r2", health: :failed, executor: nil, node: nil}
        ]
      end)

      out = capture_io(fn -> CliManager.list_relays("shop", "json") end)

      assert {:ok, decoded} = Jason.decode(out)
      assert decoded["namespace"] == "shop"

      assert decoded["relays"] == [
               %{"id" => "r1", "health" => "healthy", "executor" => "e1", "node" => "a@h"},
               %{"id" => "r2", "health" => "failed", "executor" => nil, "node" => nil}
             ]
    end

    test "text output shows a HEALTH column and surfaces a FAILED, unplaced relay" do
      stub(HubManager, :relay_rows, fn _ns ->
        [%{id: "r2", health: :failed, executor: nil, node: nil}]
      end)

      out = capture_io(fn -> CliManager.list_relays("shop", "text") end)

      assert out =~ "HEALTH"
      assert out =~ "r2"
      assert out =~ "failed"
      assert out =~ "(unplaced)"
    end

    test "empty registry renders a friendly message, not a crash" do
      stub(HubManager, :relay_rows, fn _ns -> [] end)
      out = capture_io(fn -> CliManager.list_relays("shop", "text") end)
      assert out =~ "No relays found"
    end
  end

  describe "cluster_status/2" do
    test "json reports leader, per-node relay counts, unplaced, and a health rollup" do
      stub(HubManager, :get_hub_nodes, fn _ns, true -> [:a@h, :b@h] end)

      stub(HubManager, :relay_rows, fn _ns ->
        [
          %{id: "r1", health: :healthy, executor: "e1", node: :a@h},
          %{id: "r2", health: :degraded, executor: "e2", node: :a@h},
          %{id: "r3", health: :healthy, executor: "e3", node: :b@h},
          %{id: "r4", health: :failed, executor: nil, node: nil}
        ]
      end)

      stub(Leader, :leader, fn -> :a@h end)
      stub(Leader, :is_leader?, fn -> false end)

      out = capture_io(fn -> CliManager.cluster_status("shop", "json") end)

      assert {:ok, d} = Jason.decode(out)
      assert d["leader"] == "a@h"
      assert d["is_leader"] == false
      assert d["unplaced_relays"] == 1
      assert d["relay_health"] == %{"healthy" => 2, "degraded" => 1, "failed" => 1}

      nodes = Map.new(d["nodes"], fn n -> {n["node"], n} end)
      assert nodes["a@h"]["relay_count"] == 2
      assert nodes["a@h"]["leader"] == true
      assert nodes["b@h"]["relay_count"] == 1
      assert nodes["b@h"]["leader"] == false
    end

    test "text output names the leader and per-node counts" do
      stub(HubManager, :get_hub_nodes, fn _ns, true -> [:a@h] end)

      stub(HubManager, :relay_rows, fn _ns ->
        [%{id: "r1", health: :healthy, executor: "e1", node: :a@h}]
      end)

      stub(Leader, :leader, fn -> :a@h end)
      stub(Leader, :is_leader?, fn -> true end)

      out = capture_io(fn -> CliManager.cluster_status("shop", "text") end)

      assert out =~ "Cluster status for namespace 'shop'"
      assert out =~ "Leader:"
      assert out =~ "a@h"
      assert out =~ "1 relay(s)"
      assert out =~ "[leader]"
      assert out =~ "healthy=1"
    end
  end
end
