defmodule Flezha.Schedule.HubTakeoverTest do
  # The cluster-wide half of the contract: `Singularity` must leave exactly one
  # schedule process across two nodes, and losing the owning node must not lose
  # the schedule. Needs a real peer BEAM, so it is slow, strictly sequential,
  # and excluded by default — starting and stopping a peer disturbs the shared
  # cluster state other suites run against. `mix test --include multi_node`.
  use ExUnit.Case, async: false

  @moduletag :multi_node
  @moduletag :capture_log

  alias Flezha.Schedule.{Hub, Store}
  alias Flezha.Service.HubManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.ScheduleTestHelper, as: Helper

  # Enough names that consistent hashing puts some on the peer.
  @names ~w(sa sb sc sd se sf)

  setup do
    Helper.isolated_store_setup()
    Helper.schedule_hub_setup()
    :ok
  end

  defp add_all() do
    Enum.map(@names, fn name ->
      {:ok, entry} = Store.add(Helper.entry_fields(%{name: name, cron: "0 0 1 1 *"}))
      entry
    end)
  end

  # Hub discovery runs on a 10s interval, so the two hubs are not peers the
  # instant both are up. Asserting before they join would place every child
  # locally and make the takeover vacuous.
  defp await_hub_cluster(peer_node, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_hub_cluster(peer_node, deadline)
  end

  defp do_await_hub_cluster(peer_node, deadline) do
    nodes = ProcessHub.nodes(HubManager.schedule_hub_id(), [:include_local])

    cond do
      peer_node in nodes ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("schedule hub cluster did not include #{inspect(peer_node)}: #{inspect(nodes)}")

      true ->
        Process.sleep(200)
        do_await_hub_cluster(peer_node, deadline)
    end
  end

  defp placements() do
    HubManager.schedule_hub_id()
    |> ProcessHub.process_list(:global)
    |> Map.new(fn {name, node_pids} -> {name, node_pids} end)
  end

  defp await_all_on(node, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_all_on(node, deadline)
  end

  defp do_await_all_on(node, deadline) do
    current = placements()

    settled? =
      Enum.all?(@names, fn name ->
        case Map.get(current, name) do
          [{^node, pid}] -> Process.alive?(pid)
          _ -> false
        end
      end)

    cond do
      settled? ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("schedules did not settle on #{inspect(node)}: #{inspect(current)}")

      true ->
        Process.sleep(100)
        do_await_all_on(node, deadline)
    end
  end

  test "exactly one process per schedule across two nodes, and the survivor takes over" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:schednode1)

    # The peer runs the same hub id, so the two form one hub cluster. Its
    # `application_managed: false` means nothing schedule-related autostarts
    # there — this is the only schedule process source on that node.
    {:ok, _} = :erpc.call(peer_node, HubManager, :start_schedule_hub, [])
    await_hub_cluster(peer_node, 30_000)

    entries = add_all()
    Enum.each(entries, &Hub.assert/1)
    Enum.each(@names, &Helper.await_registered(&1, 5_000))

    current = placements()

    # The exactly-once claim: one placement per name, cluster-wide.
    Enum.each(@names, fn name ->
      assert [{_node, _pid}] = Map.get(current, name), "#{name}: #{inspect(current[name])}"
    end)

    # And the distribution claim: the peer really did take a share, so the
    # takeover below is exercised rather than vacuous.
    owner_nodes = current |> Map.values() |> Enum.map(fn [{n, _}] -> n end) |> Enum.uniq()

    assert peer_node in owner_nodes,
           "expected some schedules on the peer, got #{inspect(owner_nodes)}"

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)

    # Every schedule the peer owned must come back on the survivor, still one each.
    await_all_on(node(), 20_000)
  end
end
