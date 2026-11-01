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

  alias Flezha.Cluster.GroupedHashing
  alias FlezhaTest.Helper.GroupedHubTestHelper, as: Helper
  alias FlezhaTest.Helper.NodeTestHelper
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias ProcessHub.Service.ProcessRegistry
  alias ProcessHub.Service.Recovery

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
        Map.to_list(Map.take(context, [:sync_interval, :reconcile_interval]))

    {:ok, _} = Helper.start_hub(hub_id, reconcile)
    {:ok, _} = :erpc.call(peer, Helper, :start_hub, [hub_id, reconcile], 60_000)

    assert_hub_holds_both(hub_id, peer)
    {:ok, hub_id: hub_id, hub_opts: reconcile}
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
    # The reconcile rounds on the node that cannot run it start it nowhere.
    declare_orphan(hub_id, peer, "waiting")

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

  # The interval is also the budget of the hook that holds a first round, so it
  # outlasts the test; the sync tick is pushed out so only the rounds the test
  # provokes run.
  # Fails when: a first round places its declared children before this node
  # serves their group, where in a cluster they land on a peer.
  @tag reconcile: true, sync_interval: 600_000, reconcile_interval: 30_000
  test "a restarted node's first round starts its declared children only once it serves their group",
       %{hub_id: hub_id, peer: peer, hub_opts: hub_opts} do
    declare_orphan(hub_id, peer, "home")

    # The peer comes back serving nothing, so no node can place the child and
    # this node's first round is not alone in the hub.
    {:ok, _} = on(peer, :start_hub, [hub_id, hub_opts])
    assert_hub_holds_both(hub_id, peer)
    assert wait(fn -> Recovery.recovery_state(hub_id) == :normal end)

    # A row the peer keeps for this node is what makes it a returning node.
    :ok = Helper.serve(hub_id, ["other"])
    assert wait(fn -> on(peer, :group_nodes, [hub_id, "other"]) == [node()] end)

    test_pid = self()

    :ok =
      ProcessHub.register_hook_handlers(hub_id, Hook.pre_recovery_replay(), [
        %HookManager{
          id: :test_held,
          m: __MODULE__,
          f: :report_held,
          a: [test_pid, hub_id, :_],
          p: 200
        }
      ])

    :ok =
      ProcessHub.register_hook_handlers(hub_id, Hook.reconcile_round(), [
        %HookManager{id: :test_round, m: __MODULE__, f: :report_round, a: [test_pid, :_]}
      ])

    # A restarted coordinator starts from an empty table and opens a first round.
    old = Process.whereis(hub_id)
    Process.exit(old, :kill)
    assert wait(fn -> Process.whereis(hub_id) not in [nil, old] end)

    # Runs ahead of the hold, so the round has reached its starts here.
    assert_receive {:first_round_starts, []}, 10_000

    :ok = Helper.serve(hub_id, ["report"])

    assert_receive {:first_round, started}, 10_000
    assert started == 1, "the first round started #{started} children before this node served"
    assert wait(fn -> Helper.child_nodes(hub_id, "home") == [node()] end)
  end

  # Fails when: a node whose hub started afresh announces its empty row before
  # it serves, so the survivor drops its row and starts its children itself.
  @tag reconcile: true, sync_interval: 600_000, reconcile_interval: 30_000
  test "a survivor starts none of a restarted node's children before that node serves again",
       %{hub_id: hub_id, peer: peer} do
    on_peer = crash_peer_with_children(hub_id, peer)
    await_orphan_round(hub_id)

    # The peer can place nowhere a child of a group it does not serve yet, and
    # says so at once rather than leaving the start to time out.
    assert Enum.any?(1..20, fn index ->
             match?(
               {:error, [{_id, :no_target_nodes}]},
               Helper.start_child(hub_id, "probe_#{index}", "report", :probe)
             )
           end),
           "no start placed on the restarted peer was refused with :no_target_nodes"

    :ok = on(peer, :serve, [hub_id, ["report"]])
    assert wait(fn -> Enum.all?(on_peer, &(Helper.child_nodes(hub_id, &1) == [peer])) end)

    here = node()
    for id <- on_peer, do: refute_received({:child_started, ^id, ^here})
  end

  # The fresh row's bound is the reconcile interval, kept short here.
  # Fails when: a node that never serves again keeps its children held for it.
  @tag reconcile: true, sync_interval: 600_000, reconcile_interval: 5_000
  test "a restarted node that never serves again gives its children to the survivor",
       %{hub_id: hub_id, peer: peer} do
    on_peer = crash_peer_with_children(hub_id, peer)

    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [node()] end),
           "the survivor still holds a row for the peer that never declared"

    assert wait(fn ->
             Recovery.request_round(hub_id)
             Enum.all?(on_peer, &(Helper.child_nodes(hub_id, &1) == [node()]))
           end),
           "the survivor did not take over the children of the peer that never declared"
  end

  # Both nodes serve `report` and ten declared children start over them; the
  # peer's hub then dies without a leave, as a crash takes it, and starts
  # afresh serving nothing. Answers the children that ran on the peer.
  defp crash_peer_with_children(hub_id, peer) do
    both = Enum.sort([node(), peer])
    :ok = Helper.serve(hub_id, ["report"])
    :ok = on(peer, :serve, [hub_id, ["report"]])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == both end)
    assert wait(fn -> on(peer, :group_nodes, [hub_id, "report"]) == both end)

    test_pid = self()
    ids = for index <- 1..10, do: "back_#{index}"

    homes =
      Map.new(ids, fn id ->
        {:ok, home} =
          Helper.start_child(hub_id, id, "report", {:notify, test_pid, id}, durable: true)

        assert_receive {:child_started, ^id, ^home}, 10_000
        {id, home}
      end)

    on_peer = for {id, ^peer} <- homes, do: id
    assert on_peer != [], "the ring placed none of #{inspect(ids)} on the peer"
    assert wait(fn -> Helper.declared(hub_id) == Enum.sort(ids) end)

    old = on(peer, :kill_hub, [hub_id])
    assert wait(fn -> on(peer, :hub_pid, [hub_id]) not in [nil, old] end)
    assert wait(fn -> Enum.all?(on_peer, &(Helper.child_nodes(hub_id, &1) == [])) end)
    on_peer
  end

  # Runs rounds here until one finds the crashed peer's children running nowhere.
  defp await_orphan_round(hub_id) do
    :ok =
      ProcessHub.register_hook_handlers(hub_id, Hook.reconcile_round(), [
        %HookManager{id: :test_orphans, m: __MODULE__, f: :report_orphans, a: [self(), :_]}
      ])

    assert wait(fn ->
             Recovery.request_round(hub_id)

             receive do
               {:orphans, count} when count > 0 -> true
             after
               1_000 -> false
             end
           end),
           "no round here found the restarted node's children"
  end

  @doc false
  def report_orphans(test_pid, %{measurements: %{orphans: orphans}}),
    do: send(test_pid, {:orphans, orphans})

  @doc false
  def report_held(test_pid, hub_id, _data),
    do: send(test_pid, {:first_round_starts, GroupedHashing.served_groups(hub_id)})

  @doc false
  def report_round(test_pid, %{first_round: true, measurements: %{started: started}}),
    do: send(test_pid, {:first_round, started})

  def report_round(_test_pid, _data), do: :ok

  # Both views, not just this one: a node announces what it serves to the peers
  # its own hub knows about, so a declaration made before the peer has seen this
  # node would be sent to nobody.
  defp assert_hub_holds_both(hub_id, peer) do
    both = Enum.sort([node(), peer])

    assert wait(fn -> Helper.hub_nodes(hub_id) == both end),
           "the hub never held both nodes here: #{inspect(Helper.hub_nodes(hub_id))}"

    assert wait(fn -> on(peer, :hub_nodes, [hub_id]) == both end),
           "the peer's hub never held both nodes: #{inspect(on(peer, :hub_nodes, [hub_id]))}"
  end

  # Leaves `child_id` of `report` declared, running nowhere and with no row:
  # the peer, its only server, runs it and then stops its hub. The departed
  # peer leaves an empty durable row, which serving the group would place by
  # redistribution alone; without the row, as a controller restart can leave a
  # declared child, only a reconcile round starts it.
  defp declare_orphan(hub_id, peer, child_id) do
    :ok = on(peer, :serve, [hub_id, ["report"]])
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [peer] end)

    assert Helper.start_child(hub_id, child_id, "report", :declared, durable: true) ==
             {:ok, peer}

    assert wait(fn -> Helper.declared(hub_id) == [child_id] end)

    :ok = on(peer, :stop_hub, [hub_id])

    # A peer still listed here means its hub's leave never arrived.
    assert wait(fn -> Helper.group_nodes(hub_id, "report") == [] end),
           "the stopped peer still serves `report`; hub nodes here: " <>
             inspect(Helper.hub_nodes(hub_id))

    assert wait(fn ->
             match?({_, []}, ProcessRegistry.lookup(hub_id, child_id, include_empty: true))
           end)

    _ = ProcessRegistry.delete(hub_id, child_id)
    assert ProcessRegistry.lookup(hub_id, child_id, include_empty: true) == nil
  end

  defp on(peer, fun, args), do: :erpc.call(peer, Helper, fun, args, 60_000)

  defp wait(predicate), do: NodeTestHelper.wait_until(predicate, 60_000)
end
