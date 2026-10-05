defmodule Flezha.Service.RelayManagerRoutingTest do
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
  alias Flezha.Relay.Builder
  alias Flezha.Service.RelayManager
  alias Flezha.Service.RelayManager.Placement

  @scope :flz_directory

  defp builder(id, factory_id) do
    %Builder{id: id, factory_id: factory_id, run: :steps, init_values: %{}, opts: []}
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

    assert {:error, {:factory_unknown, ^ns, "facx"}} =
             RelayManager.start_relay(ns, builder("op1", "facx"))
  end

  test "a factory-less start refuses with the namespace wording" do
    ns = unique_ns()

    assert {:error, {:no_runners, ^ns}} = RelayManager.start_relay(ns, builder("op2", nil))
  end

  test "a routed start never re-forwards: it answers the local verdict" do
    ns = unique_ns()
    # Even with a capable-looking directory entry present, `routed:` takes the
    # local path — the refusal is the node-local one, not the cluster-wide one.
    join(Directory.factory_group(ns, "facx"))

    assert {:error, {:no_runners, ^ns}} =
             RelayManager.start_relay(ns, builder("op3", "facx"), routed: true)
  end

  test "capability advertised only by this node cannot satisfy a local miss" do
    ns = unique_ns()
    # Self is excluded from the capable set — a local miss must not erpc-loop
    # onto itself; it refuses cluster-wide instead.
    join(Directory.factory_group(ns, "selfy"))

    assert {:error, {:factory_unknown, ^ns, "selfy"}} =
             RelayManager.start_relay(ns, builder("op4", "selfy"))
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

    assert {:error, {:no_runners, ^ns}} =
             RelayManager.start_relay(ns, builder(nil, "facx"), mint: mint)

    assert_received :drew
    refute_received :drew
  end

  test "spawn_or_deliver with no capable node anywhere answers factory_unknown" do
    ns = unique_ns()

    assert {{:refused, %Flezha.Error{code: "relay.factory_unknown"}}, "i1"} =
             RelayManager.spawn_or_deliver(ns, "i1", "facx", "p", :deliver, caller(ns))
  end

  test "a spawn whose factory only this node advertises answers factory_unknown" do
    ns = unique_ns()
    # Self is excluded from the capable set, as for a plain start: the spawn
    # must not forward onto its own node.
    join(Directory.factory_group(ns, "facy"))

    assert {{:refused, %Flezha.Error{code: "relay.factory_unknown"}}, "i2"} =
             RelayManager.spawn_or_deliver(ns, "i2", "facy", "p", :deliver, caller(ns))
  end

  test "a routed pid lookup takes the local path and reports the dead hub" do
    ns = unique_ns()

    assert {:error, {:hub_down, ^ns}} = RelayManager.get_relay_pid(ns, "op5", routed: true)
  end

  test "a stop with no hub anywhere keeps today's local refusal" do
    ns = unique_ns()

    assert {:error, {:hub_down, ^ns}} = RelayManager.stop_relay(ns, "op6")
  end
end
