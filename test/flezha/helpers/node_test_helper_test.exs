defmodule FlezhaTest.Helper.NodeTestHelperTest do
  @moduledoc """
  A peer controller logs through this node's logger, so a test captures what
  it provokes on a peer as it does here.
  """

  use ExUnit.Case, async: false

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
end
