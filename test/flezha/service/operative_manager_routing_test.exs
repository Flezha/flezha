defmodule Flezha.Service.OperativeManagerRoutingTest do
  @moduledoc """
  Single-node half of the start/command routing seam
  (`schedule-spawn-without-runner`): the cluster-wide refusal wording, the
  `routed:` no-re-forward guard, and self-exclusion from the capable set.
  The actual cross-node forward is pinned by
  `Flezha.Cluster.DirectoryTwoNodeTest` (`:multi_node`).
  """

  use ExUnit.Case, async: false

  alias Flezha.Cluster.Directory
  alias Flezha.Operative.Builder
  alias Flezha.Service.OperativeManager

  @scope :flz_directory

  defp builder(id, factory_id) do
    %Builder{id: id, factory_id: factory_id, kind: :workflow, init_values: %{}, opts: []}
  end

  defp unique_ns, do: "routing_ns_#{System.unique_integer([:positive])}"

  defp join(group) do
    test_pid = self()
    _ = Directory.sync_membership(MapSet.new([group]), MapSet.new())
    on_exit(fn -> _ = :pg.leave(@scope, group, test_pid) end)
  end

  test "a start with no capable node anywhere refuses cluster-wide, naming the factory" do
    ns = unique_ns()

    assert {:error, %{node: origin, reason: reason}} =
             OperativeManager.start_operative(ns, builder("op1", "facx"))

    assert origin == node()
    assert reason == "no runners for factory facx in namespace #{ns} on any cluster node"
  end

  test "a factory-less start refuses with the namespace wording" do
    ns = unique_ns()

    assert {:error, %{reason: reason}} = OperativeManager.start_operative(ns, builder("op2", nil))
    assert reason == "no runners in namespace #{ns} on any cluster node"
  end

  test "a routed start never re-forwards: it answers the local verdict" do
    ns = unique_ns()
    # Even with a capable-looking directory entry present, `routed:` takes the
    # local path — the refusal is the node-local one, not the cluster-wide one.
    join(Directory.factory_group(ns, "facx"))

    assert {:error, %{node: origin, reason: reason}} =
             OperativeManager.start_operative(ns, builder("op3", "facx"), routed: true)

    assert origin == node()
    assert reason == "no runners in namespace #{ns}"
  end

  test "capability advertised only by this node cannot satisfy a local miss" do
    ns = unique_ns()
    # Self is excluded from the capable set — a local miss must not erpc-loop
    # onto itself; it refuses cluster-wide instead.
    join(Directory.factory_group(ns, "selfy"))

    assert {:error, %{reason: "no runners for factory selfy" <> _}} =
             OperativeManager.start_operative(ns, builder("op4", "selfy"))
  end

  test "spawn_or_deliver with no capable node anywhere answers factory_unknown" do
    assert :factory_unknown =
             OperativeManager.spawn_or_deliver(unique_ns(), "i1", "facx", "p", :deliver)
  end

  test "a routed spawn_or_deliver finding no local runner answers factory_unknown" do
    ns = unique_ns()
    join(Directory.factory_group(ns, "facy"))

    assert :factory_unknown =
             OperativeManager.spawn_or_deliver(ns, "i2", "facy", "p", :deliver, routed: true)
  end

  test "a routed pid lookup takes the local path and reports the dead hub" do
    ns = unique_ns()

    assert {:error, "hub not alive"} =
             OperativeManager.get_operative_pid(ns, "op5", routed: true)
  end

  test "a stop with no hub anywhere keeps today's local refusal" do
    ns = unique_ns()

    assert {:error, "hub not alive"} = OperativeManager.stop_operative(ns, "op6")
  end
end
