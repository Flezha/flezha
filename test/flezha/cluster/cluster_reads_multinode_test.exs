defmodule Flezha.Cluster.ReadsMultiNodeTest do
  @moduledoc """
  Reads that answer for the whole cluster, asked of a controller that holds
  none of what they report: relay health and rows, factories, namespaces,
  cluster status, steps reads, info and diagnosis, and the bound that keeps a
  wedged controller from holding a read up.
  """

  # async: false: peer BEAMs, the live controller and the auth stores.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_cluster_reads", enabled: false}],
    async: false

  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.Cluster.Reads
  alias Flezha.Convergence.Snapshot
  alias Flezha.Dlq.Entry
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Relay.Builder
  alias Flezha.Service.Controller
  alias Flezha.Service.Fleet
  alias Flezha.Service.Relay
  alias Flezha.Service.RelayManager
  alias Flezha.Service.Steps
  alias Flezha.State.Completion
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.StepsResumeHarness

  @moduletag :multi_node
  @moduletag timeout: 180_000

  @factory "chat_loop_steps"
  @context %{source: :ui, principal: "console@_system"}

  setup do
    TestHelpers.reset_all_cells()
    Snapshot.reset_recovery_states()
    %{ns: "reads_ns_#{System.unique_integer([:positive])}"}
  end

  # This controller runs no runner of the namespace, so it is no member of its
  # hub and holds none of its relays, factories or state.
  test "a controller outside the hub reads the peer's relays, factories and steps state", %{
    ns: ns
  } do
    {peer_pid, peer} = NodeTestHelper.start_healing_peer(:reads_outside)
    on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, peer) end)

    _runner = NodeTestHelper.start_runner_remote(peer, ns)
    :erpc.call(peer, NodeTestHelper, :await_serving, [peer, ns, @factory])
    id = start_on(peer, ns)

    StepsResumeHarness.send_turn(ns, id, "hello", "in")
    assert NodeTestHelper.wait_until(fn -> committed?(peer, id) end, 30_000)
    assert NodeTestHelper.wait_until(fn -> peer_health(peer, ns, id) == :healthy end, 30_000)

    assert {:ok, %{rows: [%{id: ^id, health: :healthy, node: ^peer}], unreachable: []}} =
             Relay.list(ns, @context)

    assert {:ok, %{rows: factories, unreachable: []}} = Fleet.factories(ns, @context)
    assert %{nodes: [^peer], runners: [_runner]} = Enum.find(factories, &(&1.id == @factory))

    assert {:ok, %{rows: [_runner], unreachable: []}} =
             Fleet.runners_for_factory(ns, @factory, @context)

    {:ok, _} = NamespaceStore.create(ns)
    assert {:ok, %{rows: namespaces}} = Relay.list_namespaces(@context)
    assert %{running: true, nodes: [^peer]} = Enum.find(namespaces, &(&1.namespace == ns))

    assert {:ok, status} = Controller.cluster_status(ns, @context)
    assert %{answered_by: answered_by, relay_health: %{healthy: 1}, unreachable: []} = status
    assert answered_by == node()
    refute Map.has_key?(status, :leader)

    assert Steps.journal(id, @context) ==
             :erpc.call(peer, Flezha.Service.Steps.Journal, :report, [id])

    assert {:ok, %{cells: [_ | _]}} = Steps.cells(id, @context)

    park_dead_letter(peer, id)
    assert {:ok, %{findings: findings}} = Relay.diagnose(ns, id, @context)
    assert Enum.any?(findings, &(&1.code == "diagnose.dead_letters"))

    StepsResumeHarness.send_turn(ns, id, "bye", "in")

    assert NodeTestHelper.wait_until(
             fn -> match?({:ok, %{status: _}}, :erpc.call(peer, Completion, :read, [id])) end,
             30_000
           )

    assert Steps.completion(id, @context) == :erpc.call(peer, Completion, :read, [id])

    # Read once the relay is gone: an Overwatch read racing its exit is no
    # cluster question.
    assert NodeTestHelper.wait_until(
             fn -> match?({:error, _}, RelayManager.get_relay_pid(ns, id)) end,
             30_000
           )

    assert {:ok, %{relays: relays, unreachable: []}} = Relay.info(ns, @context)
    assert [%{phase: "finished", node: ^peer}] = Enum.filter(relays, &(&1.id == id))
  end

  # Both controllers run a runner, so both are hub members: each judges only
  # the relays it hosts, and the read asks the host.
  test "a relay on the other controller reads its host's health; a shared factory is one row",
       %{ns: ns} do
    {peer_pid, peer} = NodeTestHelper.start_healing_peer(:reads_both)
    on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, peer) end)
    on_exit(fn -> Flezha.Runner.FleetManager.stop_namespace(ns) end)

    local_runner = NodeTestHelper.start_runner(ns)
    peer_runner = NodeTestHelper.start_runner_remote(peer, ns)
    collector = spawn_link(fn -> StepsResumeHarness.collect_loop([]) end)
    id = StepsResumeHarness.start_steps_on_peer(peer, ns, @factory, collector)

    StepsResumeHarness.send_turn(ns, id, "hello", "in")
    assert NodeTestHelper.wait_until(fn -> committed?(peer, id) end, 30_000)
    assert NodeTestHelper.wait_until(fn -> peer_health(peer, ns, id) == :healthy end, 30_000)

    assert {:ok, %{rows: rows, unreachable: []}} = Relay.list(ns, @context)
    assert %{health: :healthy, node: ^peer} = Enum.find(rows, &(&1.id == id))

    assert {:ok, %{rows: factories}} = Fleet.factories(ns, @context)
    assert [%{nodes: nodes, runners: runners}] = Enum.filter(factories, &(&1.id == @factory))
    assert Enum.sort(nodes) == Enum.sort([node(), peer])
    assert Enum.sort(runners) == Enum.sort([local_runner, peer_runner])

    NodeTestHelper.stop_namespace(ns)
  end

  test "a wedged controller costs one bound and is named; a non-controller node is not asked" do
    {peer_pid, peer} = NodeTestHelper.start_peer_node(:reads_wedged)
    on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, peer) end)
    {bare_pid, bare} = NodeTestHelper.start_bare_peer(:reads_console)
    on_exit(fn -> NodeTestHelper.stop_peer_node(bare_pid, bare) end)

    assert NodeTestHelper.wait_until(fn -> peer in Reads.controllers() end, 10_000)
    assert {:ok, %{rows: rows, unreachable: []}} = Controller.controllers(@context)
    assert Enum.map(rows, & &1.node) == [node(), peer]

    started = System.monotonic_time(:millisecond)

    answer =
      Reads.rows([node(), peer], __MODULE__, :row_or_block, [node()], timeout: 300)

    assert System.monotonic_time(:millisecond) - started < 2_000
    assert answer == %{rows: [%{at: node(), node: node()}], unreachable: [peer]}
  end

  @doc false
  # The local half of the wedged read: this node answers, any other blocks.
  def row_or_block(answering) do
    if node() == answering, do: [%{at: node()}], else: Process.sleep(:infinity)
  end

  defp start_on(peer, ns) do
    id = "reads_#{System.unique_integer([:positive])}"
    builder = %Builder{id: id, factory_id: @factory, run: :steps, init_values: %{}, opts: []}
    assert {:ok, %{node: ^peer}} = RelayManager.start_relay(ns, builder)
    id
  end

  defp committed?(peer, id), do: StepsResumeHarness.committed_iters(id, peer) != []

  defp peer_health(peer, ns, id), do: :erpc.call(peer, Snapshot, :relay_health, [ns, id])

  defp park_dead_letter(peer, id) do
    {:ok, entry} =
      Entry.new(%{
        relay_class: id,
        tasklet_id: "chat_turn",
        cell_key: "k1",
        channel: "in",
        error: "parked by the test",
        payload: "bytes"
      })

    {:ok, _ref} = :erpc.call(peer, DlqStore, :append, [entry])
  end
end
