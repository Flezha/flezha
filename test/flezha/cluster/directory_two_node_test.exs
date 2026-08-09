defmodule Flezha.Cluster.DirectoryTwoNodeTest do
  @moduledoc """
  The core cross-node pin for `schedule-spawn-without-runner`: a REAL runner
  connects to a peer controller BEAM in a namespace this node has never seen.
  The peer's FleetManager advertises it into the directory, and a start
  submitted HERE — the node without the runner — executes THERE, as do the
  command verbs against the started operative. Needs a peer BEAM, so it is
  slow and excluded by default: `mix test --include multi_node`.
  """

  use ExUnit.Case, async: false

  @moduletag :multi_node
  @moduletag :capture_log
  @moduletag timeout: 120_000

  alias Flezha.Cluster.Directory
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Fixtures.OperativeFixture

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
    operative = OperativeFixture.get("xnode_routed", @factory_do_nothing)

    assert {:ok, %{operative_id: "xnode_routed", node: ^peer_node}} =
             OperativeManager.start_operative(@ns, operative)

    # Command plane from the non-member node: the pid resolves through the
    # peer's hub and the read executes inside the operative's own node.
    assert {:ok, pid} = OperativeManager.get_operative_pid(@ns, "xnode_routed")
    assert node(pid) == peer_node

    assert {:ok, %{runner_id: _}} = OperativeManager.get_overwatch_state(@ns, "xnode_routed")

    # Stop forwards whole-call, so its node-local cleanup runs on the peer.
    assert :ok = OperativeManager.stop_operative(@ns, "xnode_routed")

    NodeTestHelper.stop_runner_remote(peer_node, @ns, runner_id)
    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end
end
