defmodule Flezha.Cluster.GroupedPlacementTest do
  @moduledoc """
  Which controller an operator is placed on when the controllers of a
  namespace do not all run its factory.

  Two controllers share one namespace hub and only the peer's fleet advertises
  the factory under test. Every operator of that factory belongs on the peer —
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

  alias Flezha.Cluster.GroupedHashing
  alias Flezha.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias FlezhaTest.Fixtures.OperatorFixture
  alias FlezhaTest.Helper.FleetStateHelper
  alias FlezhaTest.Helper.NodeTestHelper

  use Flezha.Constant

  @ns "grouped_placement_ns"

  # Both factories are registered by every test runner. `@served` is the one a
  # controller stops advertising by re-syncing its runner down to `@kept`,
  # which keeps that runner — and so the controller's hub — in the namespace.
  @served "candle"
  @kept @factory_do_nothing

  @operators 20

  setup do
    peers = start_supervised_peers()
    on_exit(fn -> teardown(peers) end)
    {:ok, peers}
  end

  test "every operator of a factory starts on the only controller that runs it",
       %{peer_node: peer_node} do
    placements = "first" |> start_operators() |> Enum.map(&elem(&1, 1))

    assert Enum.uniq(placements) == [peer_node],
           "operators were not all placed on the controller that runs #{@served}: " <>
             inspect(Enum.frequencies(placements))
  end

  test "a joining controller that cannot run the factory is handed none of them",
       %{peer_node: peer_node} do
    started = start_operators("join")
    ids = Enum.map(started, &elem(&1, 0))

    assert Enum.uniq(Enum.map(started, &elem(&1, 1))) == [peer_node]

    {joiner_pid, joiner_node} = start_controller(:groupedplacement3, serves?: false)
    wait_for_hub_members([node(), peer_node, joiner_node])

    # The joining controller's own runner advertises the factory until the test
    # cuts it down, so the join settles when the hub's table names the peer
    # alone and every operator has been handed back.
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

    assert settled?, "operators never came back to the controller that runs #{@served}"

    assert strays == [],
           "a controller that does not run #{@served} was left holding operators of it: " <>
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
  # own table reaches, not one the fleet reaches.
  defp wait_for_sole_server(peer_node) do
    hub_id = Identificator.hub(@ns)

    assert NodeTestHelper.wait_until(
             fn -> GroupedHashing.group_nodes(hub_id, @served) == [peer_node] end,
             60_000
           ),
           "#{@served} is still served by #{inspect(GroupedHashing.group_nodes(hub_id, @served))}"
  end

  # A peer BEAM with a runner in the namespace. `serves?: false` cuts that
  # runner down to `@kept`, so the peer holds the namespace's hub without being
  # able to run `@served`.
  defp start_controller(name, serves?: serves?) do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(name)
    runner_id = NodeTestHelper.start_runner_remote(peer_node, @ns)

    unless serves? do
      :erpc.call(peer_node, FleetStateHelper, :sync, [@ns, runner_id, [@kept]])
    end

    {peer_pid, peer_node}
  end

  defp wait_for_hub_members(expected) do
    want = Enum.sort(expected)

    assert NodeTestHelper.wait_until(fn -> hub_members() == want end, 60_000),
           "the namespace hub never held #{inspect(want)}: #{inspect(hub_members())}"
  end

  defp hub_members do
    if HubManager.hub_alive?(@ns) do
      @ns |> Identificator.hub() |> ProcessHub.nodes([:include_local]) |> Enum.sort()
    else
      []
    end
  end

  # `{id, placement}` per operator, where a placement is the node it started
  # on or, for a start that failed, the verdict — a start refused because the
  # chosen controller has no runner for the factory is a placement answer too.
  defp start_operators(prefix) do
    for index <- 1..@operators do
      id = "#{prefix}_#{index}"

      placement =
        case OperatorManager.start_operator(@ns, OperatorFixture.get(id, @served)) do
          {:ok, %{node: node}} -> node
          other -> other
        end

      {id, placement}
    end
  end

  defp hosting_node(id) do
    case OperatorManager.get_operator_pid(@ns, id) do
      {:ok, pid} -> node(pid)
      other -> other
    end
  end

  defp teardown(%{peer_pid: peer_pid, peer_node: peer_node}) do
    for index <- 1..@operators, prefix <- ["first", "join"] do
      OperatorManager.stop_operator(@ns, "#{prefix}_#{index}")
    end

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end
end
