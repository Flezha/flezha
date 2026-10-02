defmodule FlezhaTest.System.PartitionedOperatorsE2ETest do
  @moduledoc """
  End-to-end proof for partitioned operators against real runners: a logical
  stream running as N ordinary instances across more than one machine.

  What only an end-to-end run can show is that the three halves agree — the
  controller's placement, the SDK's guard, and the durable layout. The
  controller decides which member owns a key; the SDK refuses a key it does not
  own; the state lands under that member's class tree. A one-bit disagreement
  between the two hashes would park every frame, and no unit suite can catch
  that because each side is pinned against fixtures, not against the other.

  Tagged `:system` (boots real runner subprocesses). Run with
  `mix test --include system test/system/partitioned_operators_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :partitioned

  import ExUnit.CaptureIO

  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Operator.Builder
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Operator.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.KeyGroup
  alias Flezha.Service.CliManager
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager
  alias Flezha.Service.Steps
  alias Flezha.State.Paths
  alias Flezha.State.TaskJournal
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.StepsResumeHarness

  @relay_tasklet "partition_relay_tasklet"
  @dynamic_tasklet "dynamic_cell_tasklet"
  @loop_tasklet "keyed_loop_count"

  # Key groups under `ranges(3)` = [{0,84},{85,169},{170,255}]: SOL-USD hashes
  # to 60, BTC-USD to 129, ETH-USD to 213 — one key per member, and the same
  # fixtures `Flezha.PartitionTest` and `flz_core`'s `key_group` tests assert.
  @keys ["SOL-USD", "BTC-USD", "ETH-USD"]

  defp start_fleet(namespace, count) do
    runners =
      for _ <- 1..count do
        {port, runner_id} = NodeTestHelper.start_runner_port(namespace)

        on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)
        runner_id
      end

    on_exit(fn ->
      try do
        OperatorManager.stop_all_operators(namespace)
        FleetManager.stop_namespace(namespace)
      catch
        _, _ -> :ok
      end
    end)

    runners
  end

  defp builder(id, factory), do: %Builder{id: id, factory_id: factory, init_values: %{}, opts: []}

  defp owner(group, keys_count, key) do
    ranges = Partition.ranges(keys_count)
    Partition.member_id(group, Partition.owner(ranges, KeyGroup.group_of(key)))
  end

  defp live_state(namespace, member, key) do
    case OperatorManager.get_cell_state(namespace, member, %{
           tasklet: @relay_tasklet,
           cell_key: key
         }) do
      {:ok, json, _seq} -> json
      other -> inspect(other)
    end
  end

  # Durable per-member state is what proves the split, so this scenario needs
  # persistence on top of partitioning.
  @tag capability: :state_persistence,
       skip: FlezhaTest.SystemCase.deferral(:state_persistence)
  test "a group splits its key space across runners, refuses a mis-route, and rescales" do
    namespace = "partitioned_e2e"
    runners = start_fleet(namespace, 2)
    group = "pgrp#{System.unique_integer([:positive])}"

    assert {:ok, result} =
             PartitionGroup.start(namespace, builder(group, "partition_relay"), 3)

    assert result.members == for(index <- 0..2, do: Partition.member_id(group, index))

    # --- every member is a full operator, bound to a real runner ---
    members = PartitionGroup.members(namespace, group)
    assert length(members) == 3

    placements =
      for member <- members do
        assert {:ok, state} = OperatorManager.get_overwatch_state(namespace, member.id)
        assert state.runner_id in runners
        state.runner_id
      end

    assert Enum.uniq(placements) != [], "every member must be placed"

    # --- each key lands on the member whose range owns it ---
    for key <- @keys do
      member = owner(group, 3, key)

      OperatorManager.send_io(member, namespace, "#{key}:one", %{"input_type" => "2"})

      assert NodeTestHelper.wait_until(
               fn ->
                 File.exists?(Paths.wal_path({member, @relay_tasklet, key}))
               end,
               20_000
             ),
             "#{key} never produced durable state under its owner #{member}"
    end

    # No member holds another member's Cell — the whole point of the split.
    for key <- @keys, member <- Enum.map(members, & &1.id), member != owner(group, 3, key) do
      refute File.exists?(Paths.wal_path({member, @relay_tasklet, key})),
             "#{member} must not hold #{key}, which it does not own"
    end

    # --- a mis-routed key is refused on its first frame, loudly ---
    stranger = owner(group, 3, "BTC-USD")
    victim = Enum.find(Enum.map(members, & &1.id), &(&1 != stranger))

    OperatorManager.send_io(victim, namespace, "BTC-USD:mis", %{"input_type" => "2"})

    assert NodeTestHelper.wait_until(fn -> DlqStore.list(victim) != [] end, 20_000),
           "a key #{victim} does not own must be parked, not folded into its state"

    parked = DlqStore.list(victim) |> hd()
    assert parked["error"] =~ "BTC-USD"
    assert parked["error"] =~ "key group 129"
    assert parked["error"] =~ "key_by"

    # --- the user's view ---
    list_out = capture_io(fn -> CliManager.list_operators(namespace) end)
    assert list_out =~ group
    assert list_out =~ "3/3"

    refute list_out =~ Partition.member_id(group, 0),
           "the group is one logical row; members are per-partition detail"

    info_out = capture_io(fn -> CliManager.operator_info(namespace) end)
    assert info_out =~ "Partition group: #{group} (3/3)"
    assert info_out =~ "key groups 0-84"
    assert info_out =~ "all 256 key groups owned exactly once"

    state_out =
      capture_io(fn -> CliManager.state_get("#{group}/#{@relay_tasklet}/BTC-USD") end)

    assert state_out =~ "BTC-USD:one",
           "a keyed read against the GROUP must resolve the owning member"

    # --- a parked entry blocks a rescale until the user resolves it ---
    assert {:error, refusal} = PartitionGroup.rescale(namespace, group, 2)
    assert refusal =~ "parked DLQ entries"
    assert refusal =~ victim

    :ok =
      DlqStore.drop("#{parked["operator_class"]}/#{parked["tasklet_id"]}/#{parked["id"]}")

    # --- rescale: every key keeps its history under its new owner ---
    assert {:ok, rescaled} = PartitionGroup.rescale(namespace, group, 2)
    assert rescaled.from == 3
    assert rescaled.to == 2
    assert rescaled.members == [Partition.member_id(group, 0), Partition.member_id(group, 1)]

    for key <- @keys do
      new_owner = owner(group, 2, key)

      assert NodeTestHelper.wait_until(
               fn ->
                 File.exists?(Paths.wal_path({new_owner, @relay_tasklet, key}))
               end,
               20_000
             ),
             "#{key} lost its durable state across the rescale (expected under #{new_owner})"
    end

    # Coverage is clean again, and the vacated member's tree is gone.
    info_after = capture_io(fn -> CliManager.operator_info(namespace) end)
    assert info_after =~ "Partition group: #{group} (2/2)"
    assert info_after =~ "all 256 key groups owned exactly once"
    refute File.exists?(Paths.class_dir(Partition.member_id(group, 2)))

    # The move is only half the promise: the new owner must RESTORE what it
    # inherited, so the key continues from its pre-rescale state.
    moved_key = "ETH-USD"
    new_owner = owner(group, 2, moved_key)

    assert NodeTestHelper.wait_until(
             fn ->
               live_state(namespace, new_owner, moved_key) =~ "#{moved_key}:one"
             end,
             20_000
           ),
           "the new owner never restored the Cell it inherited: " <>
             inspect(live_state(namespace, new_owner, moved_key))

    # And it still serves: a fresh input folds onto the restored state.
    OperatorManager.send_io(new_owner, namespace, "#{moved_key}:two", %{"input_type" => "2"})

    assert NodeTestHelper.wait_until(
             fn ->
               state = live_state(namespace, new_owner, moved_key)
               state =~ "#{moved_key}:one" and state =~ "#{moved_key}:two"
             end,
             20_000
           ),
           "the moved Cell must continue from its pre-rescale state, not restart empty"
  end

  # The forward resolves the downstream member from the output's key, so this
  # scenario needs keyed channels on top of partitioning.
  @tag capability: :keyed_channels,
       skip: FlezhaTest.SystemCase.deferral(:keyed_channels)
  test "a keyed output forwards to the partition of the target group that owns its key" do
    namespace = "partitioned_forward_e2e"
    start_fleet(namespace, 2)
    suffix = System.unique_integer([:positive])
    upstream = "pup#{suffix}"
    downstream = "pdn#{suffix}"

    # The downstream group exists first: the forward resolves its member set at
    # enable time.
    assert {:ok, _} =
             PartitionGroup.start(
               namespace,
               builder(downstream, "dynamic_cell"),
               2
             )

    plugins =
      Jason.encode!([
        %{
          "name" => "fwd_#{suffix}",
          "provider" => "operator_forward",
          "opts" => %{"target_operator_id" => downstream},
          "hooks" => [%{"name" => "on_output"}]
        }
      ])

    assert {:ok, _} =
             PartitionGroup.start(
               namespace,
               builder(upstream, "partition_relay"),
               3,
               plugins
             )

    for key <- @keys do
      OperatorManager.send_io(owner(upstream, 3, key), namespace, "#{key}:fwd", %{
        "input_type" => "2"
      })
    end

    for key <- @keys do
      target = owner(downstream, 2, key)

      assert NodeTestHelper.wait_until(
               fn ->
                 File.exists?(Paths.wal_path({target, @dynamic_tasklet, key}))
               end,
               20_000
             ),
             "#{key} was not forwarded to #{target}, the downstream member owning its key group"
    end

    # And nowhere else: a key routed by hash lands on exactly one member.
    for key <- @keys, index <- 0..1 do
      member = Partition.member_id(downstream, index)

      if member != owner(downstream, 2, key) do
        refute File.exists?(Paths.wal_path({member, @dynamic_tasklet, key})),
               "#{member} received #{key}, which belongs to another partition"
      end
    end

    # --- the target rescales, and the upstream follows it with no user help ---
    #
    # ETH-USD (key group 213) belongs to downstream p1 of two and to p2 of three.
    # An upstream that kept routing by the pre-rescale ranges would queue the
    # payload behind a worker whose member is gone, and it would never arrive.
    assert {:ok, _rescaled} = PartitionGroup.rescale(namespace, downstream, 3)

    moved_key = "ETH-USD"
    new_target = owner(downstream, 3, moved_key)
    assert new_target != owner(downstream, 2, moved_key), "the fixture must move the key"

    OperatorManager.send_io(owner(upstream, 3, moved_key), namespace, "#{moved_key}:after", %{
      "input_type" => "2"
    })

    assert NodeTestHelper.wait_until(
             fn ->
               File.exists?(Paths.wal_path({new_target, @dynamic_tasklet, moved_key}))
             end,
             20_000
           ),
           "#{moved_key} never reached #{new_target}, its owner after the rescale — " <>
             "the upstream forward is still routing by the old member set"
  end

  # --- a per-input steps operator as a group ---

  # Every output of every member, into one collector; a rescale re-mints the
  # plugin onto the new members from the live table.
  defp collect_group(namespace, group) do
    collector = spawn_link(fn -> StepsResumeHarness.collect_loop([]) end)
    plugin_id = "klw_sink_#{System.unique_integer([:positive])}"

    sink = %{
      StepsResumeHarness.forward_sink(plugin_id, collector)
      | namespace: namespace,
        operator_id: group
    }

    assert {:ok, _} = PluginManager.enable_plugin(plugin_id, sink)
    collector
  end

  defp trade(namespace, member, text),
    do: StepsResumeHarness.send_turn(namespace, member, text, "trades")

  defp await_sink(collector, expected) do
    assert NodeTestHelper.wait_until(
             fn ->
               Enum.sort(StepsResumeHarness.collected(collector)) == Enum.sort(expected)
             end,
             30_000
           ),
           "the sink holds #{inspect(StepsResumeHarness.collected(collector))}, " <>
             "expected #{inspect(expected)}"
  end

  defp await_iterations(member, expected) do
    assert NodeTestHelper.wait_until(
             fn -> TaskJournal.committed_iterations(member) == expected end,
             30_000
           ),
           "#{member} committed #{inspect(TaskJournal.committed_iterations(member))}, " <>
             "expected #{inspect(expected)}"
  end

  @tag capability: :steps, skip: FlezhaTest.SystemCase.deferral(:steps)
  test "a per-input steps runs as a group: by key, a mis-route parked, a rescale sealed" do
    namespace = "partitioned_steps_e2e"
    start_fleet(namespace, 2)
    group = "kloop#{System.unique_integer([:positive])}"

    assert {:ok, %{members: [p0, p1]}} =
             PartitionGroup.start(namespace, builder(group, "keyed_loop_steps"), 2)

    collector = collect_group(namespace, group)

    # --- messages by key reach their member, each member commits its own passes ---
    # Under two members SOL-USD (60) is p0's, BTC-USD (129) and ETH-USD (213) p1's.
    for text <- ["SOL-USD:1", "BTC-USD:1", "SOL-USD:2", "ETH-USD:1", "BTC-USD:2"] do
      [symbol | _] = String.split(text, ":")
      trade(namespace, owner(group, 2, symbol), text)
    end

    await_sink(collector, ~w(SOL-USD:1 SOL-USD:2 BTC-USD:1 BTC-USD:2 ETH-USD:1))
    await_iterations(p0, [0, 1])
    await_iterations(p1, [0, 1, 2])

    for key <- @keys, member <- [p0, p1] do
      assert File.exists?(Paths.wal_path({member, @loop_tasklet, key})) ==
               (member == owner(group, 2, key)),
             "#{key}'s Cell must exist under its owner and nowhere else"
    end

    info = capture_io(fn -> CliManager.operator_info(namespace) end)
    assert info =~ "Partition group: #{group} (2/2)"
    assert info =~ "all 256 key groups owned exactly once"

    # --- a mis-routed key parks and opens no pass; the next in-range one runs ---
    trade(namespace, p1, "SOL-USD:x")

    assert NodeTestHelper.wait_until(fn -> DlqStore.list(p1) != [] end, 30_000),
           "a key #{p1} does not own must be parked"

    [parked] = DlqStore.list(p1)
    assert parked["error"] =~ "SOL-USD"
    assert parked["error"] =~ "key group 60"
    assert parked["error"] =~ "key_by"

    trade(namespace, p1, "BTC-USD:3")
    await_sink(collector, ~w(SOL-USD:1 SOL-USD:2 BTC-USD:1 BTC-USD:2 BTC-USD:3 ETH-USD:1))
    await_iterations(p1, [0, 1, 2, 3])

    :ok =
      DlqStore.drop("#{parked["operator_class"]}/#{parked["tasklet_id"]}/#{parked["id"]}")

    # --- a rescale between passes: every key continues on its new owner ---
    # Under three members ETH-USD moves to p2; the other two keys stay.
    assert {:ok, rescaled} = PartitionGroup.rescale(namespace, group, 3)
    assert rescaled.sealed == [p0, p1]
    p2 = Partition.member_id(group, 2)
    assert rescaled.members == [p0, p1, p2]

    await_iterations(p0, [1])
    await_iterations(p1, [3])

    assert {:error, {:uncommitted_iteration, [1]}} =
             Steps.rewind_instance(namespace, p0, 0)

    for text <- ["SOL-USD:3", "BTC-USD:4", "ETH-USD:2"] do
      [symbol | _] = String.split(text, ":")
      trade(namespace, owner(group, 3, symbol), text)
    end

    await_sink(
      collector,
      ~w(SOL-USD:1 SOL-USD:2 SOL-USD:3 BTC-USD:1 BTC-USD:2 BTC-USD:3 BTC-USD:4 ETH-USD:1 ETH-USD:2)
    )

    await_iterations(p0, [1, 2])
    await_iterations(p1, [3, 4])
    await_iterations(p2, [0])

    info_after = capture_io(fn -> CliManager.operator_info(namespace) end)
    assert info_after =~ "Partition group: #{group} (3/3)"
    assert info_after =~ "all 256 key groups owned exactly once"
  end

  @tag capability: :steps, skip: FlezhaTest.SystemCase.deferral(:steps)
  test "a steps operator that reads one message is refused partitions" do
    namespace = "partitioned_once_e2e"
    start_fleet(namespace, 2)
    group = "once#{System.unique_integer([:positive])}"

    # A class whose loop mode no bind has declared is refused at its first
    # member's bind; the record is node-wide, so forget any earlier test's.
    ClassRegistry.put_partitioning("echo_steps", %{keyless: false, each: nil})
    assert {:error, refusal} = PartitionGroup.start(namespace, builder(group, "echo_steps"), 2)
    assert refusal =~ "failed to start"
    assert refusal =~ "PartitionsWithoutPerInputLoop"
    refute refusal =~ "PartitionsWithoutKeyedTasklet"
    assert PartitionGroup.members(namespace, group) == []

    # Once a bind has declared its loop mode, the refusal comes before any start.
    single = "once_single#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             OperatorManager.start_operator(namespace, builder(single, "echo_steps"))

    :ok = OperatorManager.stop_operator(namespace, single)

    assert {:error, "PartitionsWithoutPerInputLoop: class echo_steps" <> _} =
             PartitionGroup.start(namespace, builder(group, "echo_steps"), 2)

    assert PartitionGroup.members(namespace, group) == []
  end
end
