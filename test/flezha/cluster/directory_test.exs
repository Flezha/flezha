defmodule Flezha.Cluster.DirectoryTest do
  @moduledoc """
  The cluster directory (`schedule-spawn-without-runner`): membership diffs
  for capability groups, hub advertisement that dies with the hub pid, and
  the FleetManager write seam — every fleet mutation re-advertises node-local
  truth, so a factory sync or disconnect is visible to `capable_nodes`
  readers without a cluster round.
  """

  use ExUnit.Case, async: false

  alias Flezha.Cluster.Directory
  alias Flezha.Runner.FleetManager
  alias FlezhaTest.Helper.FleetStateHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @scope :flz_directory

  describe "sync_membership/2" do
    test "joins missing groups, leaves extra ones, and reads answer node sets" do
      ns = "dir_sync_#{System.unique_integer([:positive])}"
      g_factory = Directory.factory_group(ns, "f1")
      g_runners = Directory.runners_group(ns)

      joined = Directory.sync_membership(MapSet.new([g_factory, g_runners]), MapSet.new())
      assert Directory.factory_nodes(ns, "f1") == [node()]
      assert Directory.runner_nodes(ns) == [node()]

      joined = Directory.sync_membership(MapSet.new([g_factory]), joined)
      assert Directory.runner_nodes(ns) == []
      assert Directory.factory_nodes(ns, "f1") == [node()]

      _ = Directory.sync_membership(MapSet.new(), joined)
      assert Directory.factory_nodes(ns, "f1") == []
    end
  end

  describe "join_hub/2" do
    test "advertises the pid's node, is idempotent, and membership dies with the pid" do
      ns = "dir_hub_#{System.unique_integer([:positive])}"
      pid = spawn(fn -> receive do: (:stop -> :ok) end)

      :ok = Directory.join_hub(pid, ns)
      :ok = Directory.join_hub(pid, ns)

      assert Directory.hub_nodes(ns) == [node()]
      assert ns in Directory.namespaces()
      assert :pg.get_local_members(@scope, {:hub, ns}) == [pid]

      send(pid, :stop)
      # No leave call exists: `:pg`'s monitor cleans the dead member (async).
      assert NodeTestHelper.wait_until(fn -> Directory.hub_nodes(ns) == [] end, 2_000)
    end
  end

  describe "the FleetManager write seam" do
    setup do
      ns = "dir_fleet_#{System.unique_integer([:positive])}"
      original = :sys.get_state(FleetManager)
      fm = Process.whereis(FleetManager)

      on_exit(fn ->
        :sys.replace_state(FleetManager, fn _ -> original end)
        # The groups were joined by the FleetManager pid; drop this test's.
        for group <- [
              Directory.factory_group(ns, "fa"),
              Directory.factory_group(ns, "fb"),
              Directory.runners_group(ns)
            ] do
          _ = :pg.leave(@scope, group, fm)
        end
      end)

      {:ok, ns: ns}
    end

    test "a factory sync advertises capability and a full-set replacement withdraws it",
         %{ns: ns} do
      FleetStateHelper.seed(ns, %{"r1" => ["fa"]})

      :ok = FleetStateHelper.sync(ns, "r1", ["fa", "fb"])
      assert Directory.factory_nodes(ns, "fa") == [node()]
      assert Directory.factory_nodes(ns, "fb") == [node()]
      assert Directory.runner_nodes(ns) == [node()]

      :ok = FleetStateHelper.sync(ns, "r1", ["fa"])
      assert Directory.factory_nodes(ns, "fb") == []
      assert Directory.factory_nodes(ns, "fa") == [node()]
    end

    test "a runner disconnect withdraws every advertisement its node no longer backs",
         %{ns: ns} do
      FleetStateHelper.seed(ns, %{"r1" => ["fa"]})
      :ok = FleetStateHelper.sync(ns, "r1", ["fa"])
      assert Directory.factory_nodes(ns, "fa") == [node()]

      :ok = GenServer.call(FleetManager, {:runner_disconnected, ns, "r1"})

      assert Directory.factory_nodes(ns, "fa") == []
      assert Directory.runner_nodes(ns) == []
    end
  end
end
