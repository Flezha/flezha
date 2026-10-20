defmodule Flezha.Convergence.ProbePlacementTest do
  @moduledoc """
  Who the probe reads as hosting a relay's factory elsewhere: the nodes serving
  the factory's group on the namespace hub, the table the hub places by, never
  the nodes whose runners merely advertise it. A relay with no local runner is
  waiting only while one of those nodes exists.
  """

  # async: false: Mimic stubs on modules the live controller also calls.
  use ExUnit.Case, async: false
  use Mimic

  import FlezhaTest.Helper.ConvergenceFixtures, only: [diff: 2]

  alias Flezha.Cluster.{Directory, GroupedHashing}
  alias Flezha.Convergence.{Intent, Probe}
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias FlezhaTest.Helper.OverwatchHost

  @factory "probe_placement_factory"
  @peer :peer@probe_placement

  setup do
    ns = "probe_placement_#{System.unique_integer([:positive])}"
    id = "relay"
    state = OverwatchHost.state(id, namespace: ns, runner_id: nil, factory_id: @factory)
    {:ok, pid} = start_supervised({OverwatchHost, state})

    stub(HubManager, :get_running_namespaces, fn -> [ns] end)
    stub(HubManager, :local_children, fn ^ns, :relays -> [{id, pid}] end)
    stub(HubManager, :relay_runner_tag, fn ^ns, ^id -> nil end)
    stub(FleetManager, :active_runners, fn ^ns -> [] end)
    stub(FleetManager, :runners_for_factory, fn ^ns, @factory -> [] end)
    # A peer's runner advertises the factory, whether or not the peer serves it.
    stub(Directory, :factory_nodes, fn ^ns, @factory -> [node(), @peer] end)

    intent = %Intent{
      relays: [%{namespace: ns, relay_id: id, factory_id: @factory, plugins: []}]
    }

    %{ns: ns, ref: {ns, id}, intent: intent}
  end

  # fails when: the probe reads the peers that advertise the factory, so a
  # relay whose only advertiser does not serve its group is classified waiting
  # and never planned: no controller the hub places on can ever take it.
  test "a peer that advertises the factory without serving it is not waited on", ctx do
    stub(GroupedHashing, :group_nodes, fn _hub, @factory -> [node()] end)

    reality = Probe.snapshot()

    assert reality.placement == %{{ctx.ns, @factory} => %{local: [], remote_nodes: []}}
    assert diff(ctx.intent, reality).unbound_relays == [ctx.ref]
  end

  # fails when: a hub the probe cannot read answers the advertised peers, so
  # the relay waits on a move nobody can be shown to make.
  test "an unreadable hub names no remote node, so the relay is planned", ctx do
    reality = Probe.snapshot()

    assert reality.placement == %{{ctx.ns, @factory} => %{local: [], remote_nodes: []}}
    assert diff(ctx.intent, reality).unbound_relays == [ctx.ref]
  end

  test "CONTROL: a peer serving the group is waited on", ctx do
    stub(GroupedHashing, :group_nodes, fn _hub, @factory -> [node(), @peer] end)

    reality = Probe.snapshot()

    assert reality.placement == %{{ctx.ns, @factory} => %{local: [], remote_nodes: [@peer]}}
    assert diff(ctx.intent, reality).unbound_relays == []
  end
end
