defmodule Flezha.Cluster.DirectoryTest do
  @moduledoc """
  The cluster directory (`schedule-spawn-without-runner`): membership diffs
  for capability groups, hub advertisement that dies with the hub pid, and
  the FleetManager write seam — every fleet mutation re-advertises node-local
  truth, so a factory sync or disconnect is visible to `capable_nodes`
  readers without a cluster round.
  """

  # async: false: the live controller and its fleet.

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

      :ok = FleetStateHelper.disconnected(ns, "r1")

      assert Directory.factory_nodes(ns, "fa") == []
      assert Directory.runner_nodes(ns) == []
    end
  end
end

defmodule Flezha.Cluster.DirectoryTwoNodeTest do
  @moduledoc """
  The core cross-node pin for `schedule-spawn-without-runner`: a REAL runner
  connects to a peer controller BEAM in a namespace this node has never seen.
  The peer's FleetManager advertises it into the directory, and a start
  submitted HERE — the node without the runner — executes THERE, as do the
  command verbs against the started relay. Needs a peer BEAM, so it is
  slow and excluded by default: `mix test --include multi_node`.
  """

  # async: false: the live controller and its fleet; the primary Logger; a peer BEAM.

  use ExUnit.Case, async: false

  @moduletag :multi_node
  @moduletag :capture_log
  @moduletag timeout: 120_000

  alias Flezha.Cluster.Directory
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.NodeTestHelper

  use Flezha.Constant

  @ns "routing_xnode_ns"

  test "a start submitted on the runner-less node executes on the capable peer" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:routingnode1)

    runner_id = NodeTestHelper.start_runner_remote(peer_node, @ns)

    # The peer's FleetManager advertises into the directory; `:pg` syncs the
    # membership here without this node doing a cluster round.
    assert NodeTestHelper.wait_until(
             fn -> peer_node in Directory.factory_nodes(@ns, @factory_do_nothing) end,
             15_000
           ),
           "peer capability never became visible on this node"

    # The whole point: this node is NOT a member of the namespace's hub.
    refute HubManager.hub_alive?(@ns)

    # Start routed at fire time — executes on the peer, verdict verbatim.
    relay = RelayFixture.get("xnode_routed", @factory_do_nothing)

    assert {:ok, %{relay_id: "xnode_routed", node: ^peer_node}} =
             RelayManager.start_relay(@ns, relay)

    # Command plane from the non-member node: the pid resolves through the
    # peer's hub and the read executes inside the relay's own node.
    assert {:ok, pid} = RelayManager.get_relay_pid(@ns, "xnode_routed")
    assert node(pid) == peer_node

    assert {:ok, %{runner_id: _}} = RelayManager.get_overwatch_state(@ns, "xnode_routed")

    # Stop forwards whole-call, so its node-local cleanup runs on the peer.
    assert :ok = RelayManager.stop_relay(@ns, "xnode_routed")

    NodeTestHelper.stop_runner_remote(peer_node, @ns, runner_id)
    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end
end

defmodule Flezha.Cluster.OrchestratorTest do
  @moduledoc """
  Cluster-formation gate.

  `cluster_ready?/0` is the lock-free gate the on-demand `start_hub` path consults
  so a namespace hub is never created on a not-yet-clustered node. These tests
  drive the running singleton (which boots solo-ready under the test config) and
  restart it under a synthetic unreachable-peer config to exercise the
  degrade-to-solo path.
  """
  # async: false: writes Application env.
  use ExUnit.Case, async: false

  alias Flezha.Cluster.Orchestrator

  describe "cluster_ready? (solo / running app)" do
    test "a node with no discovered peers is ready and await returns :ok" do
      # Test config uses manual_discovery with no initial_nodes → solo-ready.
      assert Orchestrator.cluster_ready?() == true
      assert Orchestrator.await_cluster_ready(100) == :ok
    end
  end

  describe "degrade-to-solo when a discovered peer is unreachable" do
    setup do
      saved = %{
        cluster_discovery: Application.fetch_env!(:flezha, :cluster_discovery),
        initial_nodes: Application.fetch_env!(:flezha, :initial_nodes),
        cluster_formation_timeout_ms:
          Application.fetch_env!(:flezha, :cluster_formation_timeout_ms),
        cluster_update_interval: Application.fetch_env!(:flezha, :cluster_update_interval)
      }

      on_exit(fn ->
        Enum.each(saved, fn {k, v} -> Application.put_env(:flezha, k, v) end)
        restart_orchestrator()
        # Leave the singleton solo-ready for the rest of the suite.
        assert Orchestrator.await_cluster_ready(3_000) == :ok
      end)

      :ok
    end

    test "stays not-ready until the formation timeout, then degrades to ready" do
      Application.put_env(:flezha, :cluster_discovery, "manual_discovery")
      Application.put_env(:flezha, :initial_nodes, [:"ghost@127.0.0.1"])
      # Degrade window well below the periodic sweep so the degrade timer (not
      # the sweep's discovery reset) is what flips readiness.
      Application.put_env(:flezha, :cluster_formation_timeout_ms, 500)
      Application.put_env(:flezha, :cluster_update_interval, 60_000)
      restart_orchestrator()

      # Unreachable peer → not ready yet; the on-demand hub gate would hold.
      refute Orchestrator.cluster_ready?()
      assert Orchestrator.await_cluster_ready(200) == :timeout

      # After the degrade-to-solo window elapses, it declares ready so a
      # single-node deployment still boots.
      assert Orchestrator.await_cluster_ready(3_000) == :ok
      assert Orchestrator.cluster_ready?()
    end
  end

  defp restart_orchestrator() do
    :ok = Supervisor.terminate_child(Flezha.Supervisor, Orchestrator)

    case Supervisor.restart_child(Flezha.Supervisor, Orchestrator) do
      {:ok, _} -> :ok
      {:ok, _, _} -> :ok
    end
  end
end
