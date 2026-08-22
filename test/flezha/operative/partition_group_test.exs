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
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.AuthZ.Quotas
  alias Flezha.Operative.Builder
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Registry, as: StateRegistry
  alias FlezhaTest.Helper.FakeStartRegistry

  @namespace "partition-start-test"
  @factory "ohlc-factory"

  setup :set_mimic_global

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

      members = started_members()
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
      # its own unit — the queue arms p1 to pass and p2 to fail.
      expect(Quotas, :check_allocation, fn _ns, :max_operatives, 2 -> :ok end)
      expect(Quotas, :check_allocation, fn _ns, :max_operatives, 1 -> :ok end)

      expect(Quotas, :check_allocation, fn _ns, :max_operatives, 1 ->
        {:error, {:quota_exceeded, :max_operatives, 1}}
      end)

      assert {:error, message} = PartitionGroup.start(@namespace, builder(), 3)
      assert message =~ "ohlc#p2"
      assert message =~ "1 already-started member(s) were stopped"

      # p1 was started by this run and rolled back; the survivor is untouched.
      assert Enum.map(started_members(), & &1.id) == ["ohlc#p1"]
      assert {:error, _} = FakeStartRegistry.pid_of(registry, "ohlc#p1")
      assert {:ok, ^p0_pid} = FakeStartRegistry.pid_of(registry, "ohlc#p0")
    end

    test "converge leaves the missing member's durable state untouched", %{registry: registry} do
      assert {:ok, _} = PartitionGroup.start(@namespace, builder(), 3)
      _ = started_members()
      :ets.delete(registry, "ohlc#p2")

      wal = StateRegistry.wal_path({"ohlc#p2", "proc", "key"})
      File.mkdir_p!(Path.dirname(wal))
      File.write!(wal, "durable-bytes")
      on_exit(fn -> File.rm_rf(StateRegistry.class_dir("ohlc#p2")) end)

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
