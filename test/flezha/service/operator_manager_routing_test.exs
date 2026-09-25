defmodule Flezha.Service.OperatorManagerRoutingTest do
  @moduledoc """
  Single-node half of the start/command routing seam
  (`schedule-spawn-without-runner`): the cluster-wide refusal wording, the
  `routed:` no-re-forward guard, and self-exclusion from the capable set.
  The actual cross-node forward is pinned by
  `Flezha.Cluster.DirectoryTwoNodeTest` (`:multi_node`).
  """

  # async: false: the live controller and its fleet; a peer BEAM.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Cluster.Directory
  alias Flezha.Operator.Builder
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.OperatorManager.Placement

  @scope :flz_directory

  defp builder(id, factory_id) do
    %Builder{id: id, factory_id: factory_id, kind: :bounded, init_values: %{}, opts: []}
  end

  defp unique_ns, do: "routing_ns_#{System.unique_integer([:positive])}"

  defp caller(ns), do: %{source: :sdk, principal: "runner@" <> ns, namespace: ns}

  defp join(group) do
    test_pid = self()
    _ = Directory.sync_membership(MapSet.new([group]), MapSet.new())
    on_exit(fn -> _ = :pg.leave(@scope, group, test_pid) end)
  end

  test "a start with no capable node anywhere refuses cluster-wide, naming the factory" do
    ns = unique_ns()

    assert {:error, %{node: origin, reason: reason}} =
             OperatorManager.start_operator(ns, builder("op1", "facx"))

    assert origin == node()
    assert reason == "no runners for factory facx in namespace #{ns} on any cluster node"
  end

  test "a factory-less start refuses with the namespace wording" do
    ns = unique_ns()

    assert {:error, %{reason: reason}} = OperatorManager.start_operator(ns, builder("op2", nil))
    assert reason == "no runners in namespace #{ns} on any cluster node"
  end

  test "a routed start never re-forwards: it answers the local verdict" do
    ns = unique_ns()
    # Even with a capable-looking directory entry present, `routed:` takes the
    # local path — the refusal is the node-local one, not the cluster-wide one.
    join(Directory.factory_group(ns, "facx"))

    assert {:error, %{node: origin, reason: reason}} =
             OperatorManager.start_operator(ns, builder("op3", "facx"), routed: true)

    assert origin == node()
    assert reason == "no runners in namespace #{ns}"
  end

  test "capability advertised only by this node cannot satisfy a local miss" do
    ns = unique_ns()
    # Self is excluded from the capable set — a local miss must not erpc-loop
    # onto itself; it refuses cluster-wide instead.
    join(Directory.factory_group(ns, "selfy"))

    assert {:error, %{reason: "no runners for factory selfy" <> _}} =
             OperatorManager.start_operator(ns, builder("op4", "selfy"))
  end

  test "a start without an id is forwarded under the id this node drew" do
    ns = unique_ns()
    test_pid = self()
    set_mimic_global_until_exit()
    # The directory names a capable peer and the pick lands on this node, so the
    # real forward runs and its verdict is the executing node's.
    stub(Directory, :factory_nodes, fn _ns, _factory -> [:"capable-peer@127.0.0.1"] end)
    stub(Placement, :pick_runner, fn _nodes, _id -> node() end)

    mint = fn factory ->
      send(test_pid, :drew)
      factory <> "-origin"
    end

    assert {:error, %{operator_id: "facx-origin", node: executing, reason: reason}} =
             OperatorManager.start_operator(ns, builder(nil, "facx"), mint: mint)

    assert executing == node()
    assert reason == "no runners in namespace #{ns}"
    assert_received :drew
    refute_received :drew
  end

  test "spawn_or_deliver with no capable node anywhere answers factory_unknown" do
    ns = unique_ns()

    assert {:factory_unknown, "i1"} =
             OperatorManager.spawn_or_deliver(ns, "i1", "facx", "p", :deliver, caller(ns))
  end

  test "a spawn whose factory only this node advertises answers factory_unknown" do
    ns = unique_ns()
    # Self is excluded from the capable set, as for a plain start: the spawn
    # must not forward onto its own node.
    join(Directory.factory_group(ns, "facy"))

    assert {:factory_unknown, "i2"} =
             OperatorManager.spawn_or_deliver(ns, "i2", "facy", "p", :deliver, caller(ns))
  end

  test "a routed pid lookup takes the local path and reports the dead hub" do
    ns = unique_ns()

    assert {:error, "hub not alive"} =
             OperatorManager.get_operator_pid(ns, "op5", routed: true)
  end

  test "a stop with no hub anywhere keeps today's local refusal" do
    ns = unique_ns()

    assert {:error, "hub not alive"} = OperatorManager.stop_operator(ns, "op6")
  end
end
