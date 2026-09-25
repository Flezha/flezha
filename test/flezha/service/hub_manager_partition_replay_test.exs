defmodule Flezha.Service.HubManagerPartitionReplayTest do
  @moduledoc """
  A partition member's identity is written on its registry row at start
  (`metadata.partition`), and `HubManager.partition_members/2` answers from that
  key alone, for every start and every plugin enable that asks who is in a
  group. A reconcile replay registers the row again, and the identity has to
  survive it, or a running group answers `[]`.

  Two replays, because they reach the row two ways. Inside a running hub the
  row is still there, unbound, and the replay writes over it. After a warm
  restart the live registry starts empty, and the replay writes the row afresh
  from the declared entry.
  """

  # async: false: writes Application env; a shared on-disk root; the live controller and its fleet.
  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  alias Flezha.Identificator
  alias Flezha.Partition
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry
  alias ProcessHub.Service.ProcessRegistry

  @namespace "partition_replay_ns"
  @group "partition_replay_group"
  @count 2
  @runner "partition_replay_runner"

  setup %{tmp: tmp_dir} do
    fleet_state = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> fleet_state end) end)

    Application.put_env(:flezha, :auto_restore_enabled, true)
    Application.put_env(:flezha, :process_hub_registry_root, tmp_dir)
    # A 50 ms grace opens every boot's first round before the 2 s settle window.
    Application.put_env(:flezha, :process_hub_reconcile_grace_ms, 50)
    # The rate limit a requested round has to clear, at upstream's floor.
    Application.put_env(:flezha, :process_hub_reconcile_interval_ms, 1_000)
    on_exit(fn -> HubManager.stop_hub(@namespace) end)

    HubRegistryTestHelper.seed_runner(@namespace, @runner, "market")
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id()], 5_000)

    members = Enum.map(0..(@count - 1), &start_member/1)

    assert member_ids() == members,
           "the group was never visible, so no replay can be what hid it"

    Telemetry.capture([:flezha, :boot, :reconcile_round], filter: &(&1.hub_id == hub_id()))

    %{members: members}
  end

  # fails when: a replay over a row that still exists replaces the row's metadata
  # with the replay's own, which carries only the placement tag.
  test "a replay over a running group's unbound rows keeps each member's identity", %{
    members: members
  } do
    hub_id = hub_id()
    bound = row_metadata(hub_id, members)

    # What a peer keeps once it stops observing this node (a boot announcement
    # purge, a sync payload that no longer names the child): the row, with no
    # node, while the child runs on. The reconcile replays it, the start finds
    # the child running, and registers it again.
    ProcessRegistry.withdraw_observations(hub_id, ProcessRegistry.dump(hub_id), fn _id, n ->
      n == node()
    end)

    unbound = row_metadata(hub_id, members)

    # A registered, unbound child is started on the second round that sees it.
    assert NodeTestHelper.wait_until(fn -> replay_round?(hub_id) end, 10_000),
           "no reconcile round replayed the group"

    assert await_bound(hub_id, members), "the replay never registered the group again"

    assert member_ids() == members,
           "partition_members/2 lost a running group across a replay. " <>
             "Row metadata bound: #{inspect(bound)}; unbound: #{inspect(unbound)}; " <>
             "after the replay: #{inspect(row_metadata(hub_id, members))}"
  end

  # fails when: the replay a warm restart runs from the declared entry submits no
  # metadata, so the row it writes carries only the placement tag.
  test "a warm restart's replay puts each member's identity back on its row", %{
    members: members
  } do
    hub_id = hub_id()
    before = row_metadata(hub_id, members)

    HubRegistryTestHelper.stop_hub_keeping_registry(@namespace)
    {:ok, _} = HubManager.start_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 10_000)

    assert_receive {:telemetry, _event, %{started: @count}, %{first_round: true}}, 10_000
    assert await_bound(hub_id, members), "the restart never registered the group again"

    assert member_ids() == members,
           "partition_members/2 lost a running group across a warm restart. " <>
             "Row metadata before: #{inspect(before)}; " <>
             "after the replay: #{inspect(row_metadata(hub_id, members))}"
  end

  defp hub_id, do: Identificator.hub(@namespace)

  defp member_ids, do: @namespace |> HubManager.partition_members(@group) |> Enum.map(& &1.id)

  # The start `OperatorManager.Init.start_child/2` submits for a partition
  # member: the declared two-element start args, durable, and the identity on
  # the row's metadata from the first registration.
  defp start_member(index) do
    id = Partition.member_id(@group, index)
    range = Enum.at(Partition.ranges(@count), index)
    opts = Partition.member_opts(@group, index, @count, range)

    spec = %{
      id: id,
      start:
        {HubRegistryTestHelper.StubChild, :start_link,
         [{@namespace, %{id: id, factory_id: "market", opts: opts}}]},
      restart: :transient
    }

    %ProcessHub.StartResult{status: :ok} =
      hub_id()
      |> ProcessHub.start_child(spec,
        awaitable: true,
        durable: true,
        timeout: 5_000,
        metadata: %{partition: Partition.identity(opts)}
      )
      |> ProcessHub.Future.await()

    id
  end

  # Asks the hub for a round, the message its own grace timer sends; the rate
  # limit keeps it to one a second. True once a round started the whole group.
  defp replay_round?(hub_id) do
    send(hub_id, :reconcile_round)

    receive do
      {:telemetry, _event, %{started: started}, _meta} -> started == @count
    after
      200 -> false
    end
  end

  defp await_bound(hub_id, ids) do
    NodeTestHelper.wait_until(
      fn ->
        Enum.all?(ids, fn id ->
          case ProcessRegistry.lookup(hub_id, id) do
            {_cspec, node_pids} -> HubManager.live_node(node_pids) != nil
            nil -> false
          end
        end)
      end,
      10_000
    )
  end

  # The two caller keys Flezha writes on a row, per member.
  defp row_metadata(hub_id, ids) do
    Map.new(ids, fn id ->
      case ProcessRegistry.lookup(hub_id, id, with_metadata: true, include_empty: true) do
        {_cspec, _node_pids, metadata} -> {id, Map.take(metadata, [:partition, :tag])}
        nil -> {id, :no_row}
      end
    end)
  end
end
