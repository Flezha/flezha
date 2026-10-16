defmodule Flezha.Cluster.GroupedPlacementTest do
  @moduledoc """
  Which controller a relay is placed on when the controllers of a
  namespace do not all run its factory.

  Two controllers share one namespace hub and only the peer's fleet advertises
  the factory under test. Every relay of that factory belongs on the peer —
  at its first start, and still after a third controller that cannot run it
  joins the hub. A controller whose fleet cannot run the factory is never a placement
  candidate, so nothing has to repair a placement afterwards.

  Needs peer BEAMs and a real runner on each, so it is excluded by default:
  `mix test --include multi_node test/flezha/cluster/grouped_placement_test.exs`.
  """

  # async: false: the live controller and its fleet; two peer BEAMs.

  use ExUnit.Case, async: false

  @moduletag :multi_node
  @moduletag :capture_log
  @moduletag timeout: 600_000

  alias Flezha.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.FleetStateHelper
  alias FlezhaTest.Helper.NodeTestHelper

  use Flezha.Constant

  @ns "grouped_placement_ns"

  # Both factories are registered by every test runner. `@served` is the one a
  # controller stops advertising by re-syncing its runner down to `@kept`,
  # which keeps that runner — and so the controller's hub — in the namespace.
  @served "candle"
  @kept @factory_do_nothing

  @relays 20

  setup do
    peers = start_supervised_peers()

    on_exit(fn ->
      NodeTestHelper.stop_peers_and_namespace([{peers.peer_pid, peers.peer_node}], @ns)
    end)

    {:ok, peers}
  end

  test "every relay of a factory starts on the only controller that runs it",
       %{peer_node: peer_node} do
    placements = "first" |> start_relays() |> Enum.map(&elem(&1, 1))

    assert Enum.uniq(placements) == [peer_node],
           "relays were not all placed on the controller that runs #{@served}: " <>
             inspect(Enum.frequencies(placements))
  end

  test "a joining controller that cannot run the factory is handed none of them",
       %{peer_node: peer_node} do
    started = start_relays("join")
    ids = Enum.map(started, &elem(&1, 0))

    assert Enum.uniq(Enum.map(started, &elem(&1, 1))) == [peer_node]

    {joiner_pid, joiner_node} = start_controller(:groupedplacement3, serves?: false)
    wait_for_hub_members([node(), peer_node, joiner_node])

    # The joining controller's runner declared the factory as it connected, and
    # the cut-down withdraws it only after the grace, so the join settles when
    # every node's table names the peer alone and every relay has been handed
    # back. The peer moves relays by its own table, not by this node's.
    wait_for_sole_server(peer_node)
    settled? = NodeTestHelper.wait_until(fn -> strays(ids, peer_node) == [] end, 120_000)

    # And it stays settled: a later topology event must not reassign one to a
    # controller that cannot run it.
    strays =
      Enum.flat_map(1..10, fn _ ->
        Process.sleep(500)
        strays(ids, peer_node)
      end)

    NodeTestHelper.stop_peer_node(joiner_pid, joiner_node)

    assert settled?, "relays never came back to the controller that runs #{@served}"

    assert strays == [],
           "a controller that does not run #{@served} was left holding relays of it: " <>
             inspect(Enum.frequencies(strays))
  end

  defp strays(ids, peer_node),
    do: ids |> Enum.map(&hosting_node/1) |> Enum.reject(&(&1 == peer_node))

  # ---------- the cluster this test runs against ----------

  # This node and one peer, each with a real runner in the namespace, and only
  # the peer left advertising `@served`.
  defp start_supervised_peers do
    local_runner = NodeTestHelper.start_runner(@ns)
    {peer_pid, peer_node} = start_controller(:groupedplacement2, serves?: true)

    wait_for_hub_members([node(), peer_node])

    # This controller keeps its runner, and with it the namespace's hub, but
    # stops advertising the factory under test.
    FleetStateHelper.sync(@ns, local_runner, [@kept])
    wait_for_sole_server(peer_node)

    %{peer_pid: peer_pid, peer_node: peer_node, local_runner: local_runner}
  end

  # A controller that stops being able to run a factory withdraws it only after
  # `group_withdraw_grace_ms`, so "only the peer serves it" is a state the hub's
  # tables reach, not one the fleet reaches. Each hub node answers from its own
  # table, and a start forwarded between two that disagree loses its answer.
  defp wait_for_sole_server(peer_node) do
    assert NodeTestHelper.wait_until(
             fn -> Enum.uniq(views()) == [{:ok, [peer_node]}] end,
             60_000
           ),
           "#{@served} is still served by #{inspect(views())}"
  end

  defp views, do: NodeTestHelper.group_views(@ns, @served)

  # A peer BEAM with a runner in the namespace. `serves?: false` cuts that
  # runner down to `@kept`, so the peer holds the namespace's hub without being
  # able to run `@served`.
  defp start_controller(name, serves?: serves?) do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(name)
    runner_id = NodeTestHelper.start_runner_remote(peer_node, @ns)

    unless serves? do
      :erpc.call(peer_node, FleetStateHelper, :sync, [@ns, runner_id, [@kept]])
      # The connect's declaration of every factory is already queued at the
      # hub; once the hub has taken it, its table drops `@served` only by the
      # withdrawal, never because the declaration has not landed yet.
      :erpc.call(peer_node, :sys, :get_state, [Identificator.hub(@ns)])
    end

    {peer_pid, peer_node}
  end

  defp wait_for_hub_members(expected) do
    want = Enum.sort(expected)

    assert NodeTestHelper.wait_until(fn -> hub_members() == want end, 60_000),
           "the namespace hub never held #{inspect(want)}: #{inspect(hub_members())}"
  end

  defp hub_members, do: @ns |> HubManager.get_hub_nodes(true) |> Enum.sort()

  # `{id, placement}` per relay, where a placement is the node it started
  # on or, for a start that failed, the verdict — a start refused because the
  # chosen controller has no runner for the factory is a placement answer too.
  defp start_relays(prefix) do
    for index <- 1..@relays do
      id = "#{prefix}_#{index}"

      placement =
        case RelayManager.start_relay(@ns, RelayFixture.get(id, @served)) do
          {:ok, %{node: node}} -> node
          other -> other
        end

      {id, placement}
    end
  end

  defp hosting_node(id) do
    case RelayManager.get_relay_pid(@ns, id) do
      {:ok, pid} -> node(pid)
      other -> other
    end
  end
end
