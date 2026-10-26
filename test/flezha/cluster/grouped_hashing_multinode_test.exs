defmodule Flezha.Cluster.GroupedHashingMultiNodeTest do
  @moduledoc """
  `Flezha.Cluster.GroupedHashing` across real nodes: the scenarios of
  `grouped-hashing-distribution` that only a cluster can answer.

  The hub here holds nothing of Flezha's — groups, children and nodes only —
  because the strategy names no namespace, runner or factory and is meant to
  move into ProcessHub as it stands.

  Needs a peer BEAM, so it is excluded by default:
  `mix test --include multi_node test/flezha/cluster/grouped_hashing_multinode_test.exs`.
  """

  # async: false: a peer BEAM and a hub under the node's hub supervisor.

  use ExUnit.Case, async: false

  @moduletag :multi_node
  @moduletag :capture_log
  @moduletag timeout: 300_000

  alias FlezhaTest.Helper.GroupedHubTestHelper, as: Helper
  alias FlezhaTest.Helper.NodeTestHelper
  alias ProcessHub.Service.ProcessRegistry

  # One peer for the module: every test runs its own hub, under its own id, on
  # both nodes and stops it on both, and none touches the peer outside it.
  setup_all do
    {peer_pid, peer} = NodeTestHelper.start_peer_node(:groupedhashing2)
    on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, peer) end)
    {:ok, peer: peer}
  end

  setup %{peer: peer} = context do
    hub_id = :"grouped_hashing_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Helper.stop_hub(hub_id)
      on(peer, :stop_hub, [hub_id])
    end)

    reconcile =
      [reconcile: context[:reconcile] == true] ++
        Map.to_list(Map.take(context, [:sync_interval]))

    {:ok, _} = Helper.start_hub(hub_id, reconcile)
    {:ok, _} = :erpc.call(peer, Helper, :start_hub, [hub_id, reconcile], 60_000)

    both = Enum.sort([node(), peer])

    # Both views, not just this one: a node announces what it serves to the
    # peers its own hub knows about, so a declaration made before the peer has
    # seen this node would be sent to nobody.
    assert wait(fn -> Helper.hub_nodes(hub_id) == both end),
           "the hub never held both nodes here: #{inspect(Helper.hub_nodes(hub_id))}"

    assert wait(fn -> on(peer, :hub_nodes, [hub_id]) == both end),
           "the peer's hub never held both nodes: #{inspect(on(peer, :hub_nodes, [hub_id]))}"

    {:ok, hub_id: hub_id}
  end

  test "a grouped child starts on the only node that serves its group, wherever it is asked for",
       %{hub_id: hub_id, peer: peer} do
    :ok = on(peer, :serve, [hub_id, ["report"]])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [peer] end)

    # From the node that does NOT serve it: the start is forwarded, and the
    # receiving node validates it with the child's own group.
    for index <- 1..20 do
      assert Helper.start_child(hub_id, "report_#{index}", "report", index) == {:ok, peer}
    end
  end

  test "a forwarded start whose signature has moved is still validated by group",
       %{hub_id: hub_id, peer: peer} do
    :ok = on(peer, :serve, [hub_id, ["report"]])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [peer] end)

    # A group this node declares after the peer computed its signature: the two
    # no longer agree, so the receiving node takes the full validation path
    # rather than trusting the request's node list.
    :ok = Helper.serve(hub_id, ["local_only"])
    assert wait(fn -> Helper.group_nodes(hub_id, "local_only") == [node()] end)

    assert Helper.start_child(hub_id, "forwarded", "report", :carried) == {:ok, peer}
    assert wait(fn -> Helper.child_held(hub_id, "forwarded") == :carried end)
  end

  test "a child with no group is placed over the whole hub", %{hub_id: hub_id, peer: peer} do
    :ok = on(peer, :serve, [hub_id, ["report"]])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [peer] end)

    nodes =
      for index <- 1..40 do
        assert {:ok, node} = Helper.start_child(hub_id, "plain_#{index}", nil, index)
        node
      end

    assert Enum.sort(Enum.uniq(nodes)) == Enum.sort([node(), peer])
  end

  test "serving a group moves its children to the new node with their state",
       %{hub_id: hub_id, peer: peer} do
    :ok = on(peer, :serve, [hub_id, ["report"]])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [peer] end)

    ids = for index <- 1..20, do: "moving_#{index}"
    for id <- ids, do: assert(Helper.start_child(hub_id, id, "report", id) == {:ok, peer})

    :ok = Helper.serve(hub_id, ["report"])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == Enum.sort([node(), peer]) end)

    # The ring over both nodes now assigns some of them here, and each arrives
    # with the term it handed over.
    assert wait(fn -> Enum.any?(ids, &(Helper.child_nodes(hub_id, &1) == [node()])) end),
           "serving the group moved none of its children"

    assert wait(fn ->
             Enum.all?(ids, fn id ->
               Helper.child_nodes(hub_id, id) != [] and Helper.child_held(hub_id, id) == id
             end)
           end),
           "a child lost its state or its home on the way"
  end

  test "the last serving node withdrawing holds its children where they run",
       %{hub_id: hub_id, peer: peer} do
    :ok = on(peer, :serve, [hub_id, ["report"]])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [peer] end)

    assert Helper.start_child(hub_id, "held", "report", :kept) == {:ok, peer}
    assert wait(fn -> Helper.child_nodes(hub_id, "held") == [peer] end)

    :ok = on(peer, :unserve, [hub_id, ["report"]])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [] end)

    # Ten samples: an unserved group must move nothing and stop nothing.
    for _ <- 1..10 do
      Process.sleep(200)
      assert Helper.child_nodes(hub_id, "held") == [peer]
    end

    assert Helper.child_held(hub_id, "held") == :kept
  end

  test "a start into a group nobody serves is refused and registers nothing",
       %{hub_id: hub_id} do
    assert {:error, {:no_group_members, ["nowhere"]}} =
             Helper.start_child(hub_id, "refused", "nowhere", :never)

    assert Helper.child_nodes(hub_id, "refused") == []
  end

  test "a node whose hub stops leaves every group while the node keeps running",
       %{hub_id: hub_id, peer: peer} do
    :ok = on(peer, :serve, [hub_id, ["echo"]])
    :ok = Helper.serve(hub_id, ["echo"])
    assert wait(fn -> Helper.group_nodes(hub_id, "echo") == Enum.sort([node(), peer]) end)

    ids = for index <- 1..20, do: "echo_#{index}"
    for id <- ids, do: assert(match?({:ok, _}, Helper.start_child(hub_id, id, "echo", id)))

    :ok = on(peer, :stop_hub, [hub_id])

    assert wait(fn -> Helper.group_nodes(hub_id, "echo") == [node()] end),
           "the stopped hub's node is still serving: " <>
             inspect(Helper.group_nodes(hub_id, "echo"))

    assert wait(fn -> Enum.all?(ids, &(Helper.child_nodes(hub_id, &1) == [node()])) end),
           "children of `echo` did not all end up on the node that still serves it"
  end

  test "a joining node is told who serves what", %{hub_id: hub_id, peer: peer} do
    :ok = on(peer, :serve, [hub_id, ["report"]])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [peer] end)

    {joiner_pid, joiner} = NodeTestHelper.start_peer_node(:groupedhashing3)
    {:ok, _} = on(joiner, :start_hub, [hub_id])

    assert wait(fn -> Helper.hub_nodes(hub_id) == Enum.sort([node(), peer, joiner]) end)

    # The joiner serves nothing, and learns from every node already in the hub
    # who serves what — so a start it submits runs where the group is served.
    assert wait(fn -> on(joiner, :group_nodes, [hub_id, "report"]) == [peer] end),
           "the joining node never learned the table: " <>
             inspect(on(joiner, :group_nodes, [hub_id, "report"]))

    assert on(joiner, :start_child, [hub_id, "from_joiner", "report", :ok]) == {:ok, peer}

    NodeTestHelper.stop_peer_node(joiner_pid, joiner)
  end

  test "two nodes holding different tables settle on one owner", %{hub_id: hub_id, peer: peer} do
    # This node is told the peer serves `split`; the peer has not been told it
    # serves anything, which is the state a lost announcement leaves behind.
    # The two signatures differ, so a receiving node validates every start
    # against its own table rather than trusting the sender's node list.
    :ok = Helper.announce_locally(hub_id, peer, ["split"])
    assert wait(fn -> Helper.group_nodes(hub_id, "split") == [peer] end)
    assert on(peer, :group_nodes, [hub_id, "split"]) == []

    # While they disagree the child does not quietly land on a node that does
    # not serve its group.
    assert {:error, _} = Helper.start_child(hub_id, "split_early", "split", :early)
    assert Helper.child_nodes(hub_id, "split_early") == []

    # The peer's own declaration carries its whole set, so one announcement
    # settles the disagreement whichever way it started.
    :ok = on(peer, :serve, [hub_id, ["split"]])
    assert wait(fn -> on(peer, :group_nodes, [hub_id, "split"]) == [peer] end)
    assert wait(fn -> Helper.group_nodes(hub_id, "split") == [peer] end)

    ids = for index <- 1..20, do: "split_#{index}"
    for id <- ids, do: assert(Helper.start_child(hub_id, id, "split", id) == {:ok, peer})

    # One owner per child, on both nodes' registries once they have synced.
    assert wait(fn ->
             Enum.all?(ids, fn id ->
               Helper.child_nodes(hub_id, id) == [peer] and
                 on(peer, :child_nodes, [hub_id, id]) == [peer]
             end)
           end)
  end

  # The sync tick also opens rounds; pushed past the test so only serving the
  # group can start the child within the bound below.
  @tag reconcile: true, sync_interval: 600_000
  test "a declared child whose group nobody serves waits for one, and keeps its metadata",
       %{hub_id: hub_id, peer: peer} do
    :ok = on(peer, :serve, [hub_id, ["report"]])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [peer] end)

    assert Helper.start_child(hub_id, "waiting", "report", :declared, durable: true) ==
             {:ok, peer}

    assert wait(fn -> Helper.declared(hub_id) == ["waiting"] end)

    # The only node serving `report` leaves. The child stays declared, and the
    # reconcile rounds on the node that cannot run it start it nowhere.
    :ok = on(peer, :stop_hub, [hub_id])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [] end)

    # The departed peer leaves an empty durable row, which serving the group
    # would place by redistribution alone. Without the row, as a controller
    # restart can leave a declared child, only a reconcile round starts it.
    assert wait(fn ->
             match?({_, []}, ProcessRegistry.lookup(hub_id, "waiting", include_empty: true))
           end)

    _ = ProcessRegistry.delete(hub_id, "waiting")
    assert ProcessRegistry.lookup(hub_id, "waiting", include_empty: true) == nil

    for _ <- 1..5 do
      Process.sleep(500)
      assert Helper.child_nodes(hub_id, "waiting") == []
      assert Helper.declared(hub_id) == ["waiting"]
    end

    # Serving the group asks for a round soon, which starts it here — with the
    # group it was declared with, which is what puts it on this node at all.
    :ok = Helper.serve(hub_id, ["report"])

    assert NodeTestHelper.wait_until(
             fn -> Helper.child_nodes(hub_id, "waiting") == [node()] end,
             5_000
           ),
           "the declared child waited for a later round after its group gained a member"
  end

  # Fails when: a peer keeps the restarted node's old row, or the restarted
  # node never hears the rows of the peers that kept running.
  test "a restarted coordinator starts empty, its old row is replaced and the peers' rows come back",
       %{hub_id: hub_id, peer: peer} do
    :ok = Helper.serve(hub_id, ["local"])
    :ok = on(peer, :serve, [hub_id, ["report"]])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [peer] end)
    assert wait(fn -> on(peer, :group_nodes, [hub_id, "local"]) == [node()] end)

    # Killed, so no leave reaches this node: only the restart can clear the row.
    old = :erpc.call(peer, Process, :whereis, [hub_id])
    true = :erpc.call(peer, Process, :exit, [old, :kill])

    assert wait(fn -> :erpc.call(peer, Process, :whereis, [hub_id]) not in [nil, old] end)
    # Answers once the new coordinator's post-init setup ran: its table was
    # written empty and the restart hook has fired.
    _ = :erpc.call(peer, :sys, :get_state, [hub_id])

    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [] end)
    assert wait(fn -> on(peer, :group_nodes, [hub_id, "local"]) == [node()] end)

    # Nothing re-declares on a bare hub; the owner does, and starts place again.
    :ok = on(peer, :serve, [hub_id, ["report"]])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [peer] end)
    assert Helper.start_child(hub_id, "after_restart", "report", :kept) == {:ok, peer}
  end

  defp on(peer, fun, args), do: :erpc.call(peer, Helper, fun, args, 60_000)

  defp wait(predicate), do: NodeTestHelper.wait_until(predicate, 60_000)
end
