defmodule FlezhaTest.Helper.NodeTestHelperTest do
  @moduledoc """
  A peer controller logs through this node's logger, so a test captures what
  it provokes on a peer as it does here; and a placement probe waits until
  every controller's group table agrees.
  """

  use ExUnit.Case, async: false

  alias Flezha.Cluster.GroupedHashing
  alias Flezha.Identificator
  alias Flezha.Service.HubManager
  alias FlezhaTest.Helper.GroupedHubTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :multi_node

  test "a line a peer controller logs reaches this node's capture" do
    {pid, peer} = NodeTestHelper.start_peer_node(:peer_log)
    on_exit(fn -> NodeTestHelper.stop_peer_node(pid, peer) end)

    # The peer's own group leader: a line from a process this node spawned
    # there reaches this node through OTP already.
    peer_user = :erpc.call(peer, Process, :whereis, [:user])

    NodeTestHelper.with_only_logged([{~r/logged on the peer/, :always}], fn ->
      :erpc.call(peer, :logger, :error, ["logged on the peer", %{gl: peer_user}])
    end)
  end

  test "serving waits until every controller's group table names the same members" do
    {pid, peer} = NodeTestHelper.start_peer_node(:serving_alike)
    on_exit(fn -> NodeTestHelper.stop_peer_node(pid, peer) end)

    namespace = "default"
    hub_id = Identificator.hub(namespace)
    group = "serving_alike_probe"
    peer_view = fn -> :erpc.call(peer, GroupedHashing, :group_nodes, [hub_id, group]) end

    assert NodeTestHelper.wait_until(
             fn -> peer in HubManager.get_hub_nodes(namespace) end,
             30_000
           )

    assert NodeTestHelper.wait_until(
             fn -> node() in :erpc.call(peer, HubManager, :get_hub_nodes, [namespace]) end,
             30_000
           )

    :ok = :erpc.call(peer, GroupedHashing, :serve_groups, [hub_id, [group]])

    assert NodeTestHelper.wait_until(
             fn -> GroupedHashing.group_nodes(hub_id, group) == [peer] end,
             10_000
           )

    # The peer alone believes this node serves the group too, as a late
    # announcement leaves it: it would re-place and forward a start it receives.
    :ok = :erpc.call(peer, GroupedHubTestHelper, :announce_locally, [hub_id, node(), [group]])
    assert NodeTestHelper.wait_until(fn -> peer_view.() == Enum.sort([node(), peer]) end, 10_000)

    assert_raise ExUnit.AssertionError, ~r/never served #{group}/, fn ->
      NodeTestHelper.await_serving(peer, namespace, group, 1_000)
    end

    :ok = :erpc.call(peer, GroupedHubTestHelper, :announce_locally, [hub_id, node(), []])
    assert NodeTestHelper.wait_until(fn -> peer_view.() == [peer] end, 10_000)
    assert NodeTestHelper.await_serving(peer, namespace, group, 10_000)
  end
end
