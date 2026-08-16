defmodule FlezhaTest.System.PartitionedOperativesE2ETest do
  @moduledoc """
  End-to-end proof for partitioned operatives against real runners: a logical
  stream running as N ordinary instances across more than one machine.

  What only an end-to-end run can show is that the three halves agree — the
  controller's placement, the SDK's guard, and the durable layout. The
  controller decides which member owns a key; the SDK refuses a key it does not
  own; the state lands under that member's class tree. A one-bit disagreement
  between the two hashes would park every frame, and no unit suite can catch
  that because each side is pinned against fixtures, not against the other.

  Tagged `:system` (boots real runner subprocesses). Run with
  `mix test --include system test/system/partitioned_operatives_e2e_test.exs`.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Flezha.Operative.Builder
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.CliManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Registry
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :system

  @relay_procedure "partition_relay_procedure"
  @dynamic_procedure "dynamic_entity_procedure"

  # Key groups under `ranges(3)` = [{0,84},{85,169},{170,255}]: SOL-USD hashes
  # to 60, BTC-USD to 129, ETH-USD to 213 — one key per member, and the same
  # fixtures `Flezha.PartitionTest` and `flz_core`'s `key_group` tests assert.
  @keys ["SOL-USD", "BTC-USD", "ETH-USD"]

  defp start_fleet(namespace, count) do
    runners =
      for _ <- 1..count do
        {port, runner_id} =
          NodeTestHelper.start_runner_port(namespace, runtime: :rust, connect_timeout: 120_000)

        on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)
        runner_id
      end

    on_exit(fn ->
      try do
        OperativeManager.stop_all_operatives(namespace)
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
    Partition.member_id(group, Partition.owner(ranges, Partition.group_of(key)))
  end

  defp live_state(namespace, member, key) do
    case OperativeManager.get_entity_state(namespace, member, %{
           procedure: @relay_procedure,
           entity_key: key
         }) do
      {:ok, json, _seq} -> json
      other -> inspect(other)
    end
  end

  defp wait_until(fun, timeout_ms \\ 20_000), do: NodeTestHelper.wait_until(fun, timeout_ms)

  test "a group splits its key space across runners, refuses a mis-route, and rescales" do
    namespace = "partitioned_e2e"
    runners = start_fleet(namespace, 2)
    group = "pgrp#{System.unique_integer([:positive])}"

    assert {:ok, result} =
             PartitionGroup.start(namespace, builder(group, "partition_relay"), 3)

    assert result.members == for(index <- 0..2, do: Partition.member_id(group, index))

    # --- every member is a full operative, bound to a real runner ---
    members = PartitionGroup.members(namespace, group)
    assert length(members) == 3

    placements =
      for member <- members do
        assert {:ok, state} = OperativeManager.get_overwatch_state(namespace, member.id)
        assert state.runner_id in runners
        state.runner_id
      end

    assert Enum.uniq(placements) != [], "every member must be placed"

    # --- each key lands on the member whose range owns it ---
    for key <- @keys do
      member = owner(group, 3, key)

      OperativeManager.send_io(member, namespace, "#{key}:one", %{"input_type" => "2"})

      assert wait_until(fn ->
               File.exists?(Registry.wal_path({member, @relay_procedure, key}))
             end),
             "#{key} never produced durable state under its owner #{member}"
    end

    # No member holds another member's Entity — the whole point of the split.
    for key <- @keys, member <- Enum.map(members, & &1.id), member != owner(group, 3, key) do
      refute File.exists?(Registry.wal_path({member, @relay_procedure, key})),
             "#{member} must not hold #{key}, which it does not own"
    end

    # --- a mis-routed key is refused on its first frame, loudly ---
    stranger = owner(group, 3, "BTC-USD")
    victim = Enum.find(Enum.map(members, & &1.id), &(&1 != stranger))

    OperativeManager.send_io(victim, namespace, "BTC-USD:mis", %{"input_type" => "2"})

    assert wait_until(fn -> Flezha.Dlq.Store.list(victim) != [] end),
           "a key #{victim} does not own must be parked, not folded into its state"

    parked = Flezha.Dlq.Store.list(victim) |> hd()
    assert parked["error"] =~ "BTC-USD"
    assert parked["error"] =~ "key group 129"
    assert parked["error"] =~ "key_by"

    # --- the operator's view ---
    list_out = capture_io(fn -> CliManager.list_operatives(namespace) end)
    assert list_out =~ group
    assert list_out =~ "3/3"

    refute list_out =~ Partition.member_id(group, 0),
           "the group is one logical row; members are per-partition detail"

    info_out = capture_io(fn -> CliManager.operative_info(namespace) end)
    assert info_out =~ "Partition group: #{group} (3/3)"
    assert info_out =~ "key groups 0-84"
    assert info_out =~ "all 256 key groups owned exactly once"

    state_out =
      capture_io(fn -> CliManager.state_get("#{group}/#{@relay_procedure}/BTC-USD") end)

    assert state_out =~ "BTC-USD:one",
           "a keyed read against the GROUP must resolve the owning member"

    # --- a parked entry blocks a rescale until the operator resolves it ---
    assert {:error, refusal} = PartitionGroup.rescale(namespace, group, 2)
    assert refusal =~ "parked DLQ entries"
    assert refusal =~ victim

    :ok =
      Flezha.Dlq.Store.drop(
        "#{parked["operative_class"]}/#{parked["procedure_id"]}/#{parked["id"]}"
      )

    # --- rescale: every key keeps its history under its new owner ---
    assert {:ok, rescaled} = PartitionGroup.rescale(namespace, group, 2)
    assert rescaled.from == 3
    assert rescaled.to == 2
    assert rescaled.members == [Partition.member_id(group, 0), Partition.member_id(group, 1)]

    for key <- @keys do
      new_owner = owner(group, 2, key)

      assert wait_until(fn ->
               File.exists?(Registry.wal_path({new_owner, @relay_procedure, key}))
             end),
             "#{key} lost its durable state across the rescale (expected under #{new_owner})"
    end

    # Coverage is clean again, and the vacated member's tree is gone.
    info_after = capture_io(fn -> CliManager.operative_info(namespace) end)
    assert info_after =~ "Partition group: #{group} (2/2)"
    assert info_after =~ "all 256 key groups owned exactly once"
    refute File.exists?(Path.join(Registry.state_root(), Partition.member_id(group, 2)))

    # The move is only half the promise: the new owner must RESTORE what it
    # inherited, so the key continues from its pre-rescale state.
    moved_key = "ETH-USD"
    new_owner = owner(group, 2, moved_key)

    assert wait_until(fn ->
             live_state(namespace, new_owner, moved_key) =~ "#{moved_key}:one"
           end),
           "the new owner never restored the Entity it inherited: " <>
             inspect(live_state(namespace, new_owner, moved_key))

    # And it still serves: a fresh input folds onto the restored state.
    OperativeManager.send_io(new_owner, namespace, "#{moved_key}:two", %{"input_type" => "2"})

    assert wait_until(fn ->
             state = live_state(namespace, new_owner, moved_key)
             state =~ "#{moved_key}:one" and state =~ "#{moved_key}:two"
           end),
           "the moved Entity must continue from its pre-rescale state, not restart empty"
  end

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
               builder(downstream, "dynamic_entity"),
               2
             )

    plugins =
      Jason.encode!([
        %{
          "id" => "fwd_#{suffix}",
          "provider" => "operative_forward",
          "opts" => %{"target_operative_id" => downstream},
          "subscriptions" => [%{"name" => "on_output"}]
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
      OperativeManager.send_io(owner(upstream, 3, key), namespace, "#{key}:fwd", %{
        "input_type" => "2"
      })
    end

    for key <- @keys do
      target = owner(downstream, 2, key)

      assert wait_until(fn ->
               File.exists?(Registry.wal_path({target, @dynamic_procedure, key}))
             end),
             "#{key} was not forwarded to #{target}, the downstream member owning its key group"
    end

    # And nowhere else: a key routed by hash lands on exactly one member.
    for key <- @keys, index <- 0..1 do
      member = Partition.member_id(downstream, index)

      if member != owner(downstream, 2, key) do
        refute File.exists?(Registry.wal_path({member, @dynamic_procedure, key})),
               "#{member} received #{key}, which belongs to another partition"
      end
    end

    # --- the target rescales, and the upstream follows it with no operator help ---
    #
    # ETH-USD (key group 213) belongs to downstream p1 of two and to p2 of three.
    # Before this was fixed the upstream kept routing by the old ranges: the
    # payload queued behind a worker whose member no longer existed and never
    # arrived.
    assert {:ok, rescaled} = PartitionGroup.rescale(namespace, downstream, 3)
    assert rescaled.forwards_refreshed >= 1

    moved_key = "ETH-USD"
    new_target = owner(downstream, 3, moved_key)
    assert new_target != owner(downstream, 2, moved_key), "the fixture must move the key"

    OperativeManager.send_io(owner(upstream, 3, moved_key), namespace, "#{moved_key}:after", %{
      "input_type" => "2"
    })

    assert wait_until(fn ->
             File.exists?(Registry.wal_path({new_target, @dynamic_procedure, moved_key}))
           end),
           "#{moved_key} never reached #{new_target}, its owner after the rescale — " <>
             "the upstream forward is still routing by the old member set"
  end
end
