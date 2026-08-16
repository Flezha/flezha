defmodule FlezhaTest.Operative.PartitionGroupRescaleTest do
  @moduledoc """
  `PartitionGroup.rescale/4` — quiesce, move, restart.

  The order is what makes the operation safe rather than merely sequential, so
  what these pin is the order's guarantees: every refusal leaves the group
  exactly as it was, and once files start moving each Entity's move is atomic
  and re-derivable, which is what lets a crashed rescale be re-run.

  The registry is `FakeStartRegistry`, so the rescale drives the real start and
  stop paths rather than doubles of them.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.AuthZ.Quotas
  alias Flezha.Dlq
  alias Flezha.Operative.Builder
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.InputLog
  alias Flezha.State.Registry
  alias Flezha.State.TestHelpers
  alias Flezha.State.WalTransfer
  alias FlezhaTest.Helper.FakeStartRegistry

  @namespace "rescale-test"
  @group "ohlc"
  @factory "candles"

  setup :set_mimic_global

  setup do
    base = TestHelpers.tmp_app_data_dir()
    parent = self()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    # Three live members, each owning a third of the key space.
    seed =
      for {range, index} <- Enum.with_index(Partition.ranges(3)) do
        id = Partition.member_id(@group, index)

        %Builder{
          id: id,
          factory_id: @factory,
          kind: :stream,
          opts: [plugins: []] ++ Partition.member_opts(@group, index, 3, range)
        }
      end

    registry = FakeStartRegistry.install(seed: seed, runners: ["r1", "r2", "r3", "r4", "r5"])

    stub(PluginManager, :disable_operative_plugins, fn _ns, id ->
      send(parent, {:sources_off, id})
      :ok
    end)

    stub(Dlq.Store, :list, fn _class -> [] end)
    stub(InputLog, :last_offset, fn _class -> -1 end)
    stub(InputLog, :watermark, fn _class -> nil end)

    ClassRegistry.ensure_table()
    ClassRegistry.put_keyless(@factory, false)

    %{base: base, registry: registry}
  end

  defp write_entity(class, procedure, key, bytes) do
    entity = {class, procedure, key}
    File.mkdir_p!(Registry.state_dir(entity))
    File.write!(Registry.wal_path(entity), bytes)
    entity
  end

  defp drain(tag) do
    receive do
      {^tag, value} -> [value | drain(tag)]
    after
      0 -> []
    end
  end

  describe "quiesce" do
    test "disables every member's sources before anything else" do
      assert {:ok, _} = PartitionGroup.rescale(@namespace, @group, 2)
      # The quiesce disables them; the stop that follows disables them again,
      # which is the ordinary teardown doing its own job.
      assert :sources_off |> drain() |> Enum.uniq() |> Enum.sort() ==
               ["ohlc#p0", "ohlc#p1", "ohlc#p2"]
    end

    test "aborts naming the laggard, having stopped and moved nothing", %{registry: registry} do
      stub(InputLog, :last_offset, fn class -> if class == "ohlc#p1", do: 42, else: -1 end)
      stub(InputLog, :watermark, fn _class -> 7 end)

      assert {:error, message} =
               PartitionGroup.rescale(@namespace, @group, 2, quiesce_timeout_ms: 0)

      assert message =~ "ohlc#p1 has not consumed its input log"
      assert message =~ "watermark 7"
      assert message =~ "last offset 42"
      assert message =~ "nothing was stopped or moved"
      assert message =~ "checkpoint"

      assert FakeStartRegistry.started() == []
      assert length(:ets.tab2list(registry)) == 3
    end

    test "refuses while any member still has parked DLQ entries", %{registry: registry} do
      stub(Dlq.Store, :list, fn class ->
        if class == "ohlc#p2", do: [%{id: "a"}, %{id: "b"}], else: []
      end)

      assert {:error, message} = PartitionGroup.rescale(@namespace, @group, 2)

      assert message =~ "parked DLQ entries"
      assert message =~ "ohlc#p2: 2 entries"
      assert length(:ets.tab2list(registry)) == 3
    end
  end

  describe "preflight" do
    test "a quota refusal for the net delta leaves every member running", %{registry: registry} do
      expect(Quotas, :check_allocation, fn _ns, :max_operatives, 2 ->
        {:error, {:quota_exceeded, :max_operatives, 2}}
      end)

      assert {:error, {:quota_exceeded, _, _}} = PartitionGroup.rescale(@namespace, @group, 5)
      assert drain(:sources_off) == []
      assert length(:ets.tab2list(registry)) == 3
    end

    test "an unknown group is refused by name" do
      assert {:error, message} = PartitionGroup.rescale(@namespace, "nope", 2)
      assert message =~ "no partition group named nope"
    end
  end

  describe "movement" do
    test "3 to 2 absorbs the vacated member's entities and removes its tree" do
      # SOL-USD hashes to key group 60 — p0 under both 3 and 2 members — and
      # ETH-USD to 213: p2 of three, p1 of two.
      write_entity("ohlc#p0", "candle", "SOL-USD", "sol")
      moved = write_entity("ohlc#p2", "candle", "ETH-USD", "eth")

      assert {:ok, result} = PartitionGroup.rescale(@namespace, @group, 2)

      assert result.from == 3
      assert result.to == 2
      assert result.members == ["ohlc#p0", "ohlc#p1"]
      assert result.entities_moved == 1
      assert result.vacated == ["ohlc#p2"]

      # The Entity is now p1's, byte-for-byte, and the source is gone.
      assert File.read!(Registry.wal_path({"ohlc#p1", "candle", "ETH-USD"})) == "eth"
      refute File.exists?(Registry.wal_path(moved))
      refute File.exists?(Path.join(Registry.state_root(), "ohlc#p2"))

      # The Entity that did not change owner was left exactly where it was.
      assert File.read!(Registry.wal_path({"ohlc#p0", "candle", "SOL-USD"})) == "sol"
    end

    test "new members carry the recomputed ranges and the node the state was assembled on" do
      assert {:ok, _} = PartitionGroup.rescale(@namespace, @group, 2)

      started = FakeStartRegistry.started() |> Enum.sort_by(& &1.id)
      assert Enum.map(started, & &1.id) == ["ohlc#p0", "ohlc#p1"]

      ranges =
        Enum.map(started, fn m ->
          {m.id, {Keyword.get(m.opts, :key_group_from), Keyword.get(m.opts, :key_group_to)}}
        end)

      assert :ok == Partition.coverage(ranges)
      assert Enum.all?(started, &(Keyword.get(&1.opts, :state_source_node) == node()))
      assert Enum.all?(started, &(Keyword.get(&1.opts, :partitions) == 2))
      assert Enum.all?(started, &(&1.factory_id == @factory))
    end

    test "a re-run after a crash mid-move completes the remainder and moves nothing twice" do
      write_entity("ohlc#p2", "candle", "ETH-USD", "eth")

      assert {:ok, %{entities_moved: 1}} = PartitionGroup.rescale(@namespace, @group, 2)
      assert File.read!(Registry.wal_path({"ohlc#p1", "candle", "ETH-USD"})) == "eth"

      # A re-run sees the source gone: nothing is owed, nothing is clobbered.
      assert {:ok, %{entities_moved: 0}} = PartitionGroup.rescale(@namespace, @group, 2)
      assert File.read!(Registry.wal_path({"ohlc#p1", "candle", "ETH-USD"})) == "eth"
    end
  end

  describe "dependents and concurrency" do
    test "a rescale nudges exactly the forwards that target this group" do
      parent = self()
      overwatch = spawn_link(fn -> Process.sleep(:infinity) end)

      stub(HubManager, :operative_plugins, fn _ns ->
        [
          {"upstream",
           [
             %Flezha.Plugin{
               id: "fwd_here",
               provider: Flezha.Plugin.Provider.OperativeForward,
               opts: %{target_operative_id: @group}
             },
             %Flezha.Plugin{
               id: "fwd_elsewhere",
               provider: Flezha.Plugin.Provider.OperativeForward,
               opts: %{target_operative_id: "other_group"}
             },
             %Flezha.Plugin{
               id: "a_websocket",
               provider: Flezha.Plugin.Provider.Websocket,
               opts: %{}
             }
           ]}
        ]
      end)

      stub(OperativeManager, :get_operative_pid, fn _ns, id ->
        send(parent, {:resolved, id})
        {:ok, overwatch}
      end)

      assert {:ok, %{forwards_refreshed: 1}} = PartitionGroup.rescale(@namespace, @group, 2)

      assert_receive {:resolved, "upstream"}, 500
      # Only the forward pointing at THIS group is nudged; the other forward and
      # the unrelated provider are left alone.
      refute_receive {:resolved, _}, 100
    end

    test "an unreachable dependent is logged, not fatal — the rescale already succeeded" do
      stub(HubManager, :operative_plugins, fn _ns ->
        [
          {"gone",
           [
             %Flezha.Plugin{
               id: "fwd",
               provider: Flezha.Plugin.Provider.OperativeForward,
               opts: %{target_operative_id: @group}
             }
           ]}
        ]
      end)

      stub(OperativeManager, :get_operative_pid, fn _ns, _id ->
        {:error, "operative not found"}
      end)

      on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:warning))

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:ok, %{forwards_refreshed: 0, to: 2}} =
                   PartitionGroup.rescale(@namespace, @group, 2)
        end)

      assert log =~ "gone"
      assert log =~ "unreachable"
    end

    test "a second rescale of the same group is refused while one is running", %{
      registry: registry
    } do
      parent = self()

      # A helper holds the lock, standing in for the concurrent command. The
      # requester id must be the HELPER's pid: `:global` treats a re-request by
      # the same requester as re-entrant and grants it.
      holder =
        spawn_link(fn ->
          lock = {{:flezha_partition_rescale, @namespace, @group}, self()}
          true = :global.set_lock(lock, [node()], 0)
          send(parent, :locked)

          receive do
            :release -> :global.del_lock(lock, [node()])
          end
        end)

      assert_receive :locked, 1_000

      assert {:error, message} = PartitionGroup.rescale(@namespace, @group, 2)
      assert message =~ "a rescale of #{@group} is already running"

      # Nothing was touched: no source disabled, no member stopped or started.
      assert drain(:sources_off) == []
      assert FakeStartRegistry.started() == []
      assert length(:ets.tab2list(registry)) == 3

      send(holder, :release)
    end
  end

  describe "WalTransfer.move_entity/3" do
    test "moves an Entity's whole file set and is idempotent" do
      source = write_entity("class_a", "proc", "k1", "wal-bytes")
      File.write!(Registry.outbox_dedup_path(source), "dedup")
      destination = {"class_b", "proc", "k1"}

      assert {:ok, 2} = WalTransfer.move_entity(node(), source, destination)

      assert File.read!(Registry.wal_path(destination)) == "wal-bytes"
      assert File.read!(Registry.outbox_dedup_path(destination)) == "dedup"
      refute File.exists?(Registry.wal_path(source))

      # Nothing left to move — and the destination is untouched.
      assert {:ok, 0} = WalTransfer.move_entity(node(), source, destination)
      assert File.read!(Registry.wal_path(destination)) == "wal-bytes"
    end

    test "a move onto itself is a no-op" do
      source = write_entity("class_a", "proc", "k1", "wal-bytes")
      assert {:ok, 0} = WalTransfer.move_entity(node(), source, source)
      assert File.read!(Registry.wal_path(source)) == "wal-bytes"
    end
  end
end
