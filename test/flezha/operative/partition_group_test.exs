defmodule FlezhaTest.Operative.PartitionGroupTest do
  @moduledoc """
  `PartitionGroup.start/4` — the group start.

  A partition group is N ordinary starts, so what is worth pinning is not that
  they start but what each carries and what happens when one of them cannot:
  every refusal runs before the first member, and a mid-group failure leaves
  nothing standing.

  The registry is `FakeStartRegistry`, whose `insert_new` is ProcessHub's
  atomic id claim — the same harness the spawn suite drives.
  """
  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.AuthZ.Quotas
  alias Flezha.Operative.Builder
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Paths
  alias FlezhaTest.Helper.FakeStartRegistry

  @namespace "partition-start-test"
  @factory "ohlc-factory"

  setup :set_mimic_global_until_exit

  setup do
    registry = FakeStartRegistry.install()

    ClassRegistry.ensure_table()
    ClassRegistry.put_keyless(@factory, false)

    %{registry: registry}
  end

  defp builder(id \\ "ohlc", kind \\ :stream) do
    %Builder{id: id, factory_id: @factory, kind: kind}
  end

  defp started_members, do: FakeStartRegistry.started()

  describe "a partitioned start" do
    test "creates N self-describing members whose ranges cover the space exactly once" do
      assert {:ok, result} = PartitionGroup.start(@namespace, builder(), 3)
      assert result.group == "ohlc"
      assert result.members == ["ohlc#p0", "ohlc#p1", "ohlc#p2"]

      # Members start together, so the start log is not in partition order.
      members = Enum.sort_by(started_members(), &Keyword.get(&1.opts, :partition))
      assert length(members) == 3

      for {member, index} <- Enum.with_index(members) do
        assert member.id == "ohlc#p#{index}"
        assert Keyword.get(member.opts, :group) == "ohlc"
        assert Keyword.get(member.opts, :partition) == index
        assert Keyword.get(member.opts, :partitions) == 3
      end

      ranges =
        Enum.map(members, fn m ->
          {m.id, {Keyword.get(m.opts, :key_group_from), Keyword.get(m.opts, :key_group_to)}}
        end)

      assert :ok == Partition.coverage(ranges)
    end

    test "warns loudly when there are fewer runners than partitions" do
      stub(FleetManager, :active_runners, fn _ns -> ["runner-1"] end)

      assert {:ok, %{warnings: [warning]}} =
               PartitionGroup.start(@namespace, builder(), 3)

      assert warning =~ "3 partitions but only 1 active runner"
      assert length(started_members()) == 3
    end

    test "says nothing when the fleet is big enough" do
      assert {:ok, %{warnings: []}} = PartitionGroup.start(@namespace, builder(), 3)
    end
  end

  describe "refusals, all of which run before the first member starts" do
    test "a workflow operative cannot be partitioned" do
      assert {:error, message} =
               PartitionGroup.start(@namespace, builder("chat", :workflow), 2)

      assert message =~ "stream operatives only"
      assert started_members() == []
    end

    test "fewer than two partitions is not a group" do
      assert {:error, message} = PartitionGroup.start(@namespace, builder(), 1)
      assert message =~ "between 2 and 256"
      assert started_members() == []
    end

    test "more partitions than key groups is refused" do
      assert {:error, message} = PartitionGroup.start(@namespace, builder(), 257)
      assert message =~ "between 2 and 256"
      assert started_members() == []
    end

    test "the id budget is checked against the widest suffix" do
      base = String.duplicate("a", 30)

      assert {:error, message} = PartitionGroup.start(@namespace, builder(base), 3)
      assert message =~ "32"
      assert message =~ "at most 29 bytes"
      assert started_members() == []
    end

    test "the quota is preflighted for all N units at once" do
      expect(Quotas, :check_allocation, fn _ns, :max_operatives, 3 ->
        {:error, {:quota_exceeded, :max_operatives, 3}}
      end)

      assert {:error, {:quota_exceeded, :max_operatives, 3}} =
               PartitionGroup.start(@namespace, builder(), 3)

      assert started_members() == []
    end

    test "a class already known to be keyless is refused without touching a runner" do
      ClassRegistry.put_keyless(@factory, true)

      assert {:error, message} = PartitionGroup.start(@namespace, builder(), 3)
      assert message =~ "PartitionsWithoutKeyedProcedure"
      assert message =~ "key_by"
      assert started_members() == []
    end
  end

  describe "a mid-group failure" do
    test "stops the members that did start and names the one that did not", %{registry: registry} do
      # Member 1 loses the id claim to a squatter already in the registry.
      FakeStartRegistry.insert(registry, self(), builder("ohlc#p1"))

      assert {:error, message} = PartitionGroup.start(@namespace, builder(), 3)

      assert message =~ "partition 1 of group ohlc failed to start"
      assert message =~ "ohlc#p1"
      assert message =~ "2 already-started member(s) were stopped"

      # Members start together: p0 and p2 came up beside the failing p1 and
      # were both rolled back.
      assert Enum.sort(Enum.map(started_members(), & &1.id)) == ["ohlc#p0", "ohlc#p2"]
    end
  end

  describe "a converge start over an existing group" do
    test "starts only the missing member and leaves the survivors untouched", %{
      registry: registry
    } do
      assert {:ok, _} = PartitionGroup.start(@namespace, builder(), 3)
      _ = started_members()

      {:ok, p0_pid} = FakeStartRegistry.pid_of(registry, "ohlc#p0")
      :ets.delete(registry, "ohlc#p2")

      assert {:ok, result} = PartitionGroup.start(@namespace, builder(), 3)
      assert result.members == ["ohlc#p2"]
      assert result.already_running == ["ohlc#p0", "ohlc#p1"]

      assert [member] = started_members()
      assert member.id == "ohlc#p2"
      assert Keyword.get(member.opts, :partition) == 2
      assert Keyword.get(member.opts, :partitions) == 3

      assert {Keyword.get(member.opts, :key_group_from), Keyword.get(member.opts, :key_group_to)} ==
               Enum.at(Partition.ranges(3), 2)

      # The survivor keeps its pid: converge never stops or rebinds it.
      assert {:ok, ^p0_pid} = FakeStartRegistry.pid_of(registry, "ohlc#p0")
    end

    test "a fully-present group re-run starts nothing and reports every member" do
      assert {:ok, _} = PartitionGroup.start(@namespace, builder(), 3)
      _ = started_members()

      assert {:ok, result} = PartitionGroup.start(@namespace, builder(), 3)
      assert result.members == []
      assert result.already_running == ["ohlc#p0", "ohlc#p1", "ohlc#p2"]
      assert started_members() == []
    end

    test "an N mismatch refuses toward scale before starting anything", %{registry: registry} do
      assert {:ok, _} = PartitionGroup.start(@namespace, builder(), 3)
      _ = started_members()
      :ets.delete(registry, "ohlc#p2")

      assert {:error, message} = PartitionGroup.start(@namespace, builder(), 4)
      assert message =~ "cannot converge group ohlc to 4"
      assert message =~ "declares 3"
      assert message =~ "flz operative scale"
      assert started_members() == []
    end

    test "a member racing into existence mid-run is skipped, not failed", %{registry: registry} do
      assert {:ok, _} = PartitionGroup.start(@namespace, builder(), 3)
      _ = started_members()
      :ets.delete(registry, "ohlc#p2")

      # A concurrent starter wins the id claim between the member read and the
      # start; by id-shape the winner is a member of this same group.
      FakeStartRegistry.insert(registry, self(), builder("ohlc#p2"))

      assert {:ok, result} = PartitionGroup.start(@namespace, builder(), 3)
      assert result.members == []
      assert result.already_running == ["ohlc#p0", "ohlc#p1", "ohlc#p2"]
    end

    test "a mid-converge real failure rolls back only this run's starts", %{registry: registry} do
      assert {:ok, _} = PartitionGroup.start(@namespace, builder(), 3)
      _ = started_members()

      {:ok, p0_pid} = FakeStartRegistry.pid_of(registry, "ohlc#p0")
      :ets.delete(registry, "ohlc#p1")
      :ets.delete(registry, "ohlc#p2")

      # Converge preflights the missing count, then each member start prices
      # its own unit. Members start CONCURRENTLY, so which of p1/p2 prices
      # first is not fixed: arm by call ordinal instead of by an assumed
      # order — one unit passes, the next fails, whichever arrives first.
      priced = :atomics.new(1, [])

      stub(Quotas, :check_allocation, fn
        _ns, :max_operatives, 2 ->
          :ok

        _ns, :max_operatives, 1 ->
          if :atomics.add_get(priced, 1, 1) == 1,
            do: :ok,
            else: {:error, {:quota_exceeded, :max_operatives, 1}}
      end)

      assert {:error, message} = PartitionGroup.start(@namespace, builder(), 3)
      assert message =~ "1 already-started member(s) were stopped"

      # One of this run's two members failed and the other was rolled back;
      # the survivor from the first run is untouched either way.
      assert [rolled_back] = Enum.map(started_members(), & &1.id)
      assert rolled_back in ["ohlc#p1", "ohlc#p2"]
      assert message =~ if(rolled_back == "ohlc#p1", do: "ohlc#p2", else: "ohlc#p1")
      assert {:error, _} = FakeStartRegistry.pid_of(registry, rolled_back)
      assert {:ok, ^p0_pid} = FakeStartRegistry.pid_of(registry, "ohlc#p0")
    end

    test "converge leaves the missing member's durable state untouched", %{registry: registry} do
      assert {:ok, _} = PartitionGroup.start(@namespace, builder(), 3)
      _ = started_members()
      :ets.delete(registry, "ohlc#p2")

      wal = Paths.wal_path({"ohlc#p2", "proc", "key"})
      File.mkdir_p!(Path.dirname(wal))
      File.write!(wal, "durable-bytes")
      on_exit(fn -> File.rm_rf(Paths.class_dir("ohlc#p2")) end)

      assert {:ok, %{members: ["ohlc#p2"]}} = PartitionGroup.start(@namespace, builder(), 3)

      # The restart restores from this WAL at bind; converge must not touch it.
      assert File.read!(wal) == "durable-bytes"
    end
  end

  describe "the base-id / singleton collision guards" do
    test "a plain start of a live group's base id is refused naming the remedy" do
      assert {:ok, _} = PartitionGroup.start(@namespace, builder(), 3)
      _ = started_members()

      assert {:error, %{reason: reason}} =
               OperativeManager.start_operative(@namespace, builder("ohlc"))

      assert reason =~ "partition group of 3 live member(s)"
      assert reason =~ "--partitions 3"
      assert started_members() == []
    end

    test "a partitioned start over a live unpartitioned operative is refused" do
      assert {:ok, _} = OperativeManager.start_operative(@namespace, builder("solo"))
      _ = started_members()

      assert {:error, message} = PartitionGroup.start(@namespace, builder("solo"), 2)
      assert message =~ "already running as an unpartitioned operative"
      assert started_members() == []
    end
  end

  describe "group_members/2" do
    test "answers the member set in partition order, with each one's identity" do
      assert {:ok, _} = PartitionGroup.start(@namespace, builder(), 3)

      members = PartitionGroup.members(@namespace, "ohlc")
      assert Enum.map(members, & &1.id) == ["ohlc#p0", "ohlc#p1", "ohlc#p2"]
      assert Enum.map(members, & &1.partition.index) == [0, 1, 2]
      assert Enum.all?(members, &(&1.partition.group == "ohlc"))
    end

    test "is empty for an ordinary operative" do
      assert {:ok, _} = OperativeManager.start_operative(@namespace, builder("plain"))
      assert PartitionGroup.members(@namespace, "plain") == []
    end
  end

  describe "an unpartitioned start" do
    test "carries no partition opts at all" do
      assert {:ok, _} = OperativeManager.start_operative(@namespace, builder("plain"))

      [member] = started_members()
      assert member.id == "plain"
      assert Keyword.take(member.opts, Partition.opt_keys()) == []
      assert Partition.identity(member.opts) == nil
    end
  end
end

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
  # async: false: writes Application env; Mimic global mode; Mimic stubs.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.AuthZ.Quotas
  alias Flezha.Dlq
  alias Flezha.Operative.Builder
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.InputLog
  alias Flezha.State.Paths
  alias Flezha.State.WalTransfer
  alias FlezhaTest.Helper.FakeStartRegistry

  @namespace "rescale-test"
  @group "ohlc"
  @factory "candles"

  setup :set_mimic_global_until_exit

  setup do
    parent = self()

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

    stub(PluginManager, :disable_operatives, fn _ns, ids ->
      for id <- ids, do: send(parent, {:sources_off, id})
      :ok
    end)

    stub(Dlq.Store, :list, fn _class -> [] end)
    stub(InputLog, :last_offset, fn _class -> -1 end)
    stub(InputLog, :watermark, fn _class -> nil end)

    ClassRegistry.ensure_table()
    ClassRegistry.put_keyless(@factory, false)

    %{registry: registry}
  end

  defp write_entity(class, procedure, key, bytes) do
    entity = {class, procedure, key}
    File.mkdir_p!(Paths.state_dir(entity))
    File.write!(Paths.wal_path(entity), bytes)
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
      # One request naming every member, so M unreachable ones cost one bound.
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
      assert File.read!(Paths.wal_path({"ohlc#p1", "candle", "ETH-USD"})) == "eth"
      refute File.exists?(Paths.wal_path(moved))
      refute File.exists?(Paths.class_dir("ohlc#p2"))

      # The Entity that did not change owner was left exactly where it was.
      assert File.read!(Paths.wal_path({"ohlc#p0", "candle", "SOL-USD"})) == "sol"
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

    test "members are re-minted from plugins attached after the start, not just --plugins" do
      # The shape every start script here produces: `flz operative start` with no
      # --plugins, then `flz plugin enable <group>`. Those plugins never reach
      # builder.opts, so reading only there rescales into members with no plugins
      # at all — healthy, fully covering the key space, producing nothing.
      plugin = %Plugin{
        id: "ohlc_kraken_ws#p0",
        provider: :operative_websocket,
        operative_id: "ohlc#p0",
        opts: %{uri: "wss://example/v2"}
      }

      table = %{Table.new(%{}) | plugins: %{plugin.id => plugin}}
      FakeStartRegistry.put_live_extensions(@group <> "#p0", table)

      assert {:ok, _} = PartitionGroup.rescale(@namespace, @group, 2)

      started = FakeStartRegistry.started() |> Enum.sort_by(& &1.id)

      assert Enum.map(started, fn m ->
               m.opts |> Keyword.fetch!(:plugins) |> Enum.map(& &1.id)
             end) == [["ohlc_kraken_ws#p0"], ["ohlc_kraken_ws#p1"]]

      # The member's own partition identity rides along in each plugin's opts.
      assert Enum.all?(started, fn m ->
               [plugin] = Keyword.fetch!(m.opts, :plugins)
               plugin.operative_id == m.id and is_integer(plugin.opts.key_group_from)
             end)
    end

    test "a re-run after a crash mid-move completes the remainder and moves nothing twice" do
      write_entity("ohlc#p2", "candle", "ETH-USD", "eth")

      assert {:ok, %{entities_moved: 1}} = PartitionGroup.rescale(@namespace, @group, 2)
      assert File.read!(Paths.wal_path({"ohlc#p1", "candle", "ETH-USD"})) == "eth"

      # A re-run sees the source gone: nothing is owed, nothing is clobbered.
      assert {:ok, %{entities_moved: 0}} = PartitionGroup.rescale(@namespace, @group, 2)
      assert File.read!(Paths.wal_path({"ohlc#p1", "candle", "ETH-USD"})) == "eth"
    end
  end

  describe "dependents and concurrency" do
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
      File.write!(Paths.outbox_dedup_path(source), "dedup")
      destination = {"class_b", "proc", "k1"}

      assert {:ok, 2} = WalTransfer.move_entity(node(), source, destination)

      assert File.read!(Paths.wal_path(destination)) == "wal-bytes"
      assert File.read!(Paths.outbox_dedup_path(destination)) == "dedup"
      refute File.exists?(Paths.wal_path(source))

      # Nothing left to move — and the destination is untouched.
      assert {:ok, 0} = WalTransfer.move_entity(node(), source, destination)
      assert File.read!(Paths.wal_path(destination)) == "wal-bytes"
    end

    test "a move onto itself is a no-op" do
      source = write_entity("class_a", "proc", "k1", "wal-bytes")
      assert {:ok, 0} = WalTransfer.move_entity(node(), source, source)
      assert File.read!(Paths.wal_path(source)) == "wal-bytes"
    end
  end
end
