defmodule FlezhaTest.Relay.PartitionGroupTest do
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
  alias Flezha.Identificator
  alias Flezha.Relay.Builder
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.RelayManager
  alias Flezha.State.Paths
  alias FlezhaTest.Helper.FakeStartRegistry

  @namespace "partition-start-test"
  @factory "ohlc-factory"

  setup :set_mimic_global_until_exit

  setup do
    registry = FakeStartRegistry.install()

    ClassRegistry.ensure_table()
    ClassRegistry.put_partitioning(@factory, %{keyless: false, each: nil})

    %{registry: registry}
  end

  defp builder(id \\ "ohlc", run \\ :flow) do
    %Builder{id: id, factory_id: @factory, run: run}
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
  end

  describe "refusals, all of which run before the first member starts" do
    test "a per-input steps passes the preflight and starts as N ordinary members" do
      ClassRegistry.put_partitioning(@factory, %{keyless: false, each: :per_input})
      stub(FleetManager, :factory_run, fn _ns, @factory -> {:ok, :steps} end)

      assert {:ok, %{members: ["chat#p0", "chat#p1"]}} =
               PartitionGroup.start(@namespace, builder("chat", :steps), 2)

      assert Enum.all?(started_members(), &(&1.run == :steps))
    end

    test "a steps class known to read one message is refused without touching a runner" do
      for mode <- [:once, :chained] do
        ClassRegistry.put_partitioning(@factory, %{keyless: false, each: mode})

        assert {:error, {:partitions_without_per_input_loop, @factory, ^mode, 2}} =
                 PartitionGroup.start(@namespace, builder("chat", :steps), 2)

        assert started_members() == []
      end
    end

    test "a first message is refused: it names one instance, and a group's reaches a member by key" do
      relay = %{builder() | opts: [seed_input: {"AAPL:1", %{}}]}

      assert {:error, {:partitioned_first_message, "ohlc#p0"}} =
               PartitionGroup.start(@namespace, relay, 2)

      assert started_members() == []
    end

    test "fewer than two partitions is not a group" do
      assert {:error, {:partitions_out_of_range, 1, 256}} =
               PartitionGroup.start(@namespace, builder(), 1)

      assert started_members() == []
    end

    test "more partitions than key groups is refused" do
      assert {:error, {:partitions_out_of_range, 257, 256}} =
               PartitionGroup.start(@namespace, builder(), 257)

      assert started_members() == []
    end

    test "the id budget is checked against the widest suffix" do
      base = String.duplicate("a", 30)

      assert {:error, {:group_id_too_long, ^base, 3, 29}} =
               PartitionGroup.start(@namespace, builder(base), 3)

      assert started_members() == []
    end

    test "the quota is preflighted for all N units at once" do
      expect(Quotas, :check_allocation, fn _ns, :max_relays, 3 ->
        {:error, {:quota_exceeded, :max_relays, 3}}
      end)

      assert {:error, {:quota_exceeded, :max_relays, 3}} =
               PartitionGroup.start(@namespace, builder(), 3)

      assert started_members() == []
    end

    test "a class already known to be keyless is refused without touching a runner" do
      ClassRegistry.put_partitioning(@factory, %{keyless: true, each: nil})

      assert {:error, {:partitions_without_keyed_tasklet, @factory}} =
               PartitionGroup.start(@namespace, builder(), 3)

      assert started_members() == []
    end
  end

  describe "a mid-group failure" do
    test "stops the members that did start and answers the one that did not", %{
      registry: registry
    } do
      # Member 1 loses the id claim to a squatter already in the registry.
      FakeStartRegistry.insert(registry, self(), builder("ohlc#p1"))

      assert {:error, {:id_in_use, "ohlc#p1"}} = PartitionGroup.start(@namespace, builder(), 3)

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

      assert {:error, {:group_partition_mismatch, "ohlc", 4, _member, 3}} =
               PartitionGroup.start(@namespace, builder(), 4)

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

      stub(Quotas, :record_allocation, fn
        _ns, :max_relays, [_id] ->
          if :atomics.add_get(priced, 1, 1) == 1,
            do: :ok,
            else: {:error, {:quota_exceeded, :max_relays, 1}}

        _ns, _cells, _n ->
          :ok
      end)

      assert {:error, {:quota_exceeded, :max_relays, 1}} =
               PartitionGroup.start(@namespace, builder(), 3)

      # One of this run's two members failed and the other was rolled back;
      # the survivor from the first run is untouched either way.
      assert [rolled_back] = Enum.map(started_members(), & &1.id)
      assert rolled_back in ["ohlc#p1", "ohlc#p2"]
      assert :error = FakeStartRegistry.pid_of(registry, rolled_back)
      assert {:ok, ^p0_pid} = FakeStartRegistry.pid_of(registry, "ohlc#p0")
    end

    test "converge leaves the missing member's durable state untouched", %{registry: registry} do
      assert {:ok, _} = PartitionGroup.start(@namespace, builder(), 3)
      _ = started_members()
      :ets.delete(registry, "ohlc#p2")

      wal = Paths.wal_path({"ohlc#p2", "task", "key"})
      File.mkdir_p!(Path.dirname(wal))
      File.write!(wal, "durable-bytes")
      on_exit(fn -> File.rm_rf(Paths.class_dir("ohlc#p2")) end)

      assert {:ok, %{members: ["ohlc#p2"]}} = PartitionGroup.start(@namespace, builder(), 3)

      # The restart restores from this WAL at bind; converge must not touch it.
      assert File.read!(wal) == "durable-bytes"
    end

    # Fails when: converge builds the ranges before the range check, so naming
    # more partitions than key groups raises instead of being refused.
    test "naming more partitions than key groups is refused, not raised" do
      assert {:ok, _} = PartitionGroup.start(@namespace, builder(), 3)
      _ = started_members()

      for n <- [257, 1_000_000] do
        assert {:error, {:partitions_out_of_range, ^n, 256}} =
                 PartitionGroup.start(@namespace, builder(), n)
      end

      assert started_members() == []
    end
  end

  describe "the base-id / singleton collision guards" do
    test "a plain start of a live group's base id is refused naming the remedy" do
      assert {:ok, _} = PartitionGroup.start(@namespace, builder(), 3)
      _ = started_members()

      assert {:error, {:group_base_claim, "ohlc", 3, 3}} =
               RelayManager.start_relay(@namespace, builder("ohlc"))

      assert started_members() == []
    end

    # A spawn carries no key, so it can only name one instance; the group's
    # base is none, and the start authority refuses it as it refuses any start.
    test "a spawn naming a live steps group's base is refused, and nothing is delivered" do
      stub(FleetManager, :factory_run, fn _ns, @factory -> {:ok, :steps} end)
      stub(FleetManager, :placement_refusal, fn _ns, @factory -> :none end)
      ClassRegistry.put_partitioning(@factory, %{keyless: false, each: :per_input})
      assert {:ok, _} = PartitionGroup.start(@namespace, builder("orders", :steps), 2)
      _ = started_members()

      caller = %{source: :sdk, principal: "runner@" <> @namespace, namespace: @namespace}

      assert {{:refused, reason}, "orders"} =
               RelayManager.spawn_or_deliver(
                 @namespace,
                 "orders",
                 @factory,
                 "x",
                 :deliver,
                 caller
               )

      assert %Flezha.Error{code: "relay.group_base_taken", details: %{"members" => 2}} = reason
      assert started_members() == []
    end

    test "a partitioned start over a live unpartitioned relay is refused" do
      assert {:ok, _} = RelayManager.start_relay(@namespace, builder("solo"))
      _ = started_members()

      assert {:error, {:unpartitioned_running, "solo"}} =
               PartitionGroup.start(@namespace, builder("solo"), 2)

      assert started_members() == []
    end
  end

  describe "a partitioned start without an id" do
    test "generates a base that leaves room for its largest member" do
      factory = "orders_enrichment_pipeline_v2x"
      assert byte_size(factory) == 30
      ClassRegistry.put_partitioning(factory, %{keyless: false, each: nil})

      assert {:ok, result} =
               PartitionGroup.start(@namespace, %Builder{id: nil, factory_id: factory}, 16)

      assert String.starts_with?(result.group, binary_part(factory, 0, 11))
      assert length(result.members) == 16

      for member <- result.members do
        assert Identificator.validate(:relay, member) == :ok, member
      end
    end

    test "never converges a live group, and starts a fresh one beside it" do
      assert {:ok, _} = PartitionGroup.start(@namespace, builder("demo"), 3)
      demo = PartitionGroup.members(@namespace, "demo")
      _ = started_members()

      assert {:ok, %{group: base, members: members}} =
               PartitionGroup.start(@namespace, builder(nil), 3)

      refute base == "demo"
      assert length(members) == 3
      assert PartitionGroup.members(@namespace, "demo") == demo
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

    test "is empty for an ordinary relay" do
      assert {:ok, _} = RelayManager.start_relay(@namespace, builder("plain"))
      assert PartitionGroup.members(@namespace, "plain") == []
    end
  end

  describe "an unpartitioned start" do
    test "carries no partition opts at all" do
      assert {:ok, _} = RelayManager.start_relay(@namespace, builder("plain"))

      [member] = started_members()
      assert member.id == "plain"
      assert Keyword.take(member.opts, Partition.opt_keys()) == []
      assert Partition.identity(member.opts) == nil
    end
  end
end

defmodule FlezhaTest.Relay.PartitionGroupRescaleTest do
  @moduledoc """
  `PartitionGroup.rescale/4` — quiesce, move, restart.

  The order is what makes the operation safe rather than merely sequential, so
  what these pin is the order's guarantees: every refusal leaves the group
  exactly as it was, and once files start moving each Cell's move is atomic
  and re-derivable, which is what lets a crashed rescale be re-run.

  The registry is `FakeStartRegistry`, so the rescale drives the real start and
  stop paths rather than doubles of them.
  """
  # async: false: writes Application env; Mimic global mode; Mimic stubs.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal
  import FlezhaTest.Helper.MessageTestHelper, only: [collect: 1]

  alias Flezha.AuthZ.Quotas
  alias Flezha.Dlq
  alias Flezha.Relay.Builder
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Plugin
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.InputLog
  alias Flezha.State.Paths
  alias Flezha.State.Supervisor, as: StateSupervisor
  alias Flezha.State.TaskJournal
  alias Flezha.State.WalTransfer
  alias FlezhaTest.Helper.FakeStartRegistry

  @namespace "rescale-test"
  @group "ohlc"
  @factory "candles"

  setup :set_mimic_global_until_exit

  setup do
    parent = self()

    registry =
      FakeStartRegistry.install(seed: seed(:flow), runners: ["r1", "r2", "r3", "r4", "r5"])

    stub(PluginManager, :unbind_all, fn _ns, ids ->
      for id <- ids, do: send(parent, {:sources_off, id})
      :ok
    end)

    stub(Dlq.Store, :list, fn _class -> [] end)
    stub(InputLog, :tail, fn _class -> {:ok, -1, -1} end)

    ClassRegistry.ensure_table()
    ClassRegistry.put_partitioning(@factory, %{keyless: false, each: nil})

    %{registry: registry}
  end

  # Three live members, each owning a third of the key space.
  defp seed(kind) do
    for {range, index} <- Enum.with_index(Partition.ranges(3)) do
      %Builder{
        id: Partition.member_id(@group, index),
        factory_id: @factory,
        run: kind,
        opts: [bindings: []] ++ Partition.member_opts(@group, index, 3, range)
      }
    end
  end

  defp write_cell(class, tasklet, key, bytes) do
    cell = {class, tasklet, key}
    File.mkdir_p!(Paths.state_dir(cell))
    File.write!(Paths.wal_path(cell), bytes)
    cell
  end

  defp drain(tag) do
    collect do
      {^tag, value} -> value
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
      stub(InputLog, :tail, fn class ->
        if class == "ohlc#p1", do: {:ok, 7, 42}, else: {:ok, -1, -1}
      end)

      assert {:error, {:quiesce_timeout, "ohlc#p1", :flow, progress}} =
               PartitionGroup.rescale(@namespace, @group, 2, quiesce_timeout_ms: 0)

      assert progress == %{last_offset: 42, watermark: 7}

      assert FakeStartRegistry.started() == []
      assert length(:ets.tab2list(registry)) == 3
    end

    # Fails when: a rescale that times out leaves the group running with no
    # sources and no outputs, and a re-run re-mints members with no plugins,
    # since it reads them from the bindings this quiesce removed.
    test "a quiesce that times out binds back every binding it removed" do
      parent = self()
      binding = %Plugin{id: "ohlc_ws", provider: nil, namespace: @namespace}

      stub(HubManager, :relay_bindings, fn @namespace, _member ->
        {:ok, [{binding.id, binding}]}
      end)

      stub(PluginManager, :rebind_all, fn @namespace, saved ->
        send(parent, {:rebound, saved}) && :ok
      end)

      stub(InputLog, :tail, fn class ->
        if class == "ohlc#p1", do: {:ok, 7, 42}, else: {:ok, -1, -1}
      end)

      assert {:error, {:quiesce_timeout, "ohlc#p1", :flow, _}} =
               PartitionGroup.rescale(@namespace, @group, 2, quiesce_timeout_ms: 0)

      assert [saved] = drain(:rebound)

      assert Enum.sort(saved) ==
               for(m <- ["ohlc#p0", "ohlc#p1", "ohlc#p2"], do: {m, [{binding.id, binding}]})
    end

    # Fails when: a rescale refused for parked entries has already stripped the
    # group of its plugins, so the operator who drops the entries and re-runs
    # it as told gets members with no sources and no outputs.
    test "refuses while any member still has parked DLQ entries, leaving its plugins bound",
         %{registry: registry} do
      stub(Dlq.Store, :list, fn class ->
        if class == "ohlc#p2", do: [%{id: "a"}, %{id: "b"}], else: []
      end)

      assert {:error, {:parked_entries, ["ohlc#p2 (2)"]}} =
               PartitionGroup.rescale(@namespace, @group, 2)

      assert drain(:sources_off) == []
      assert length(:ets.tab2list(registry)) == 3
    end
  end

  describe "preflight" do
    test "a quota refusal for the net delta leaves every member running", %{registry: registry} do
      expect(Quotas, :check_allocation, fn _ns, :max_relays, 2 ->
        {:error, {:quota_exceeded, :max_relays, 2}}
      end)

      assert {:error, {:quota_exceeded, _, _}} = PartitionGroup.rescale(@namespace, @group, 5)
      assert drain(:sources_off) == []
      assert length(:ets.tab2list(registry)) == 3
    end

    test "an unknown group is refused by name" do
      assert {:error, {:group_not_found, @namespace, "nope"}} =
               PartitionGroup.rescale(@namespace, "nope", 2)
    end
  end

  describe "movement" do
    test "3 to 2 absorbs the vacated member's cells and removes its tree" do
      # SOL-USD hashes to key group 60 — p0 under both 3 and 2 members — and
      # ETH-USD to 213: p2 of three, p1 of two.
      write_cell("ohlc#p0", "candle", "SOL-USD", "sol")
      moved = write_cell("ohlc#p2", "candle", "ETH-USD", "eth")

      assert {:ok, result} = PartitionGroup.rescale(@namespace, @group, 2)

      assert result.from == 3
      assert result.to == 2
      assert result.members == ["ohlc#p0", "ohlc#p1"]
      assert result.cells_moved == 1
      assert result.vacated == ["ohlc#p2"]

      # The Cell is now p1's, byte-for-byte, and the source is gone.
      assert File.read!(Paths.wal_path({"ohlc#p1", "candle", "ETH-USD"})) == "eth"
      refute File.exists?(Paths.wal_path(moved))
      refute File.exists?(Paths.class_dir("ohlc#p2"))

      # The Cell that did not change owner was left exactly where it was.
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

    test "members are re-minted from bindings made after the start, not just --plugins" do
      # The shape every start script here produces: `flz relay start` with no
      # --plugins, then `flz plugin bind <name> --relay_id <group>`. Those
      # bindings never reach builder.opts, so reading only there rescales into
      # members with no plugins at all — healthy, covering the key space,
      # producing nothing.
      binding = %Plugin{
        id: "ohlc_exchange_ws#p0",
        provider: Flezha.Plugin.Provider.File.Sink,
        relay_id: "ohlc#p0",
        opts: %{partition_index: 0}
      }

      stub(HubManager, :relay_bindings, fn @namespace, _member ->
        {:ok, [{binding.id, binding}]}
      end)

      assert {:ok, _} = PartitionGroup.rescale(@namespace, @group, 2)

      started = FakeStartRegistry.started() |> Enum.sort_by(& &1.id)

      assert Enum.map(started, fn m ->
               m.opts |> Keyword.fetch!(:bindings) |> Enum.map(& &1.id)
             end) == [["ohlc_exchange_ws#p0"], ["ohlc_exchange_ws#p1"]]

      # The member's own partition identity rides along in each binding's opts.
      assert Enum.all?(started, fn m ->
               [binding] = Keyword.fetch!(m.opts, :bindings)
               binding.relay_id == m.id and is_integer(binding.opts.key_group_from)
             end)
    end

    test "a re-run after a crash mid-move completes the remainder and moves nothing twice" do
      write_cell("ohlc#p2", "candle", "ETH-USD", "eth")

      assert {:ok, %{cells_moved: 1}} = PartitionGroup.rescale(@namespace, @group, 2)
      assert File.read!(Paths.wal_path({"ohlc#p1", "candle", "ETH-USD"})) == "eth"

      # A re-run sees the source gone: nothing is owed, nothing is clobbered.
      assert {:ok, %{cells_moved: 0}} = PartitionGroup.rescale(@namespace, @group, 2)
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

      assert {:error, {:rescale_running, @namespace, @group}} =
               PartitionGroup.rescale(@namespace, @group, 2)

      # Nothing was touched: no source disabled, no member stopped or started.
      assert drain(:sources_off) == []
      assert FakeStartRegistry.started() == []
      assert length(:ets.tab2list(registry)) == 3

      send(holder, :release)
    end
  end

  describe "a steps group" do
    setup do
      parent = self()

      FakeStartRegistry.install(
        seed: seed(:steps),
        runners: ["r1", "r2", "r3", "r4"],
        run: :steps
      )

      stub(InputLog, :tail, fn class ->
        send(parent, {:tail, class})
        {:ok, -1, -1}
      end)

      stub(TaskJournal, :cut, fn class, opts ->
        send(parent, {:sealed, class, opts})
        Mimic.call_original(TaskJournal, :cut, [class, opts])
      end)

      stub(WalTransfer, :move_cell, fn source_node, {:cell, {class, _, _}} = source, dest ->
        send(parent, {:writers_at_move, class, StateSupervisor.live_writers(class)})
        Mimic.call_original(WalTransfer, :move_cell, [source_node, source, dest])
      end)

      for id <- ["ohlc#p0", "ohlc#p1", "ohlc#p2"],
          iter <- 0..2,
          do: :ok = TaskJournal.append_iter_committed(id, iter, iter, [])

      :ok
    end

    test "quiesces on the log, closes the writers before moving, seals only the survivors" do
      # ETH-USD hashes to key group 213: p2 of three, p1 of two.
      write_cell("ohlc#p2", "candle", "ETH-USD", "eth")

      assert {:ok, result} = PartitionGroup.rescale(@namespace, @group, 2)
      assert result.sealed == ["ohlc#p0", "ohlc#p1"]
      assert result.vacated == ["ohlc#p2"]

      assert :tail |> drain() |> MapSet.new() ==
               MapSet.new(["ohlc#p0", "ohlc#p1", "ohlc#p2"])

      assert [{"ohlc#p2", []}] =
               for({class, writers} <- drain_pairs(:writers_at_move), do: {class, writers})

      assert Enum.sort(for {class, opts} <- drain_pairs(:sealed), do: {class, opts}) ==
               [{"ohlc#p0", [seal: true]}, {"ohlc#p1", [seal: true]}]

      for survivor <- ["ohlc#p0", "ohlc#p1"],
          do: assert(TaskJournal.committed_iterations(survivor) == [2])

      refute File.exists?(Paths.class_dir("ohlc#p2"))
    end

    test "a new member starts with no journal and is never sealed" do
      assert {:ok, result} = PartitionGroup.rescale(@namespace, @group, 4)
      assert result.sealed == ["ohlc#p0", "ohlc#p1", "ohlc#p2"]
      assert Enum.sort(for {class, _opts} <- drain_pairs(:sealed), do: class) == result.sealed
      refute File.exists?(Paths.journal_path("ohlc#p3"))
    end
  end

  defp drain_pairs(tag) do
    collect do
      {^tag, a, b} -> {a, b}
    end
  end

  describe "WalTransfer.move_cell/3" do
    test "moves a Cell's whole file set and is idempotent" do
      source = write_cell("class_a", "task", "k1", "wal-bytes")
      File.write!(Paths.outbox_dedup_path(source), "dedup")
      destination = {"class_b", "task", "k1"}

      assert {:ok, 2} = WalTransfer.move_cell(node(), {:cell, source}, {:cell, destination})

      assert File.read!(Paths.wal_path(destination)) == "wal-bytes"
      assert File.read!(Paths.outbox_dedup_path(destination)) == "dedup"
      refute File.exists?(Paths.wal_path(source))

      # Nothing left to move — and the destination is untouched.
      assert {:ok, 0} = WalTransfer.move_cell(node(), {:cell, source}, {:cell, destination})
      assert File.read!(Paths.wal_path(destination)) == "wal-bytes"
    end

    test "a move onto itself is a no-op" do
      source = write_cell("class_a", "task", "k1", "wal-bytes")
      assert {:ok, 0} = WalTransfer.move_cell(node(), {:cell, source}, {:cell, source})
      assert File.read!(Paths.wal_path(source)) == "wal-bytes"
    end
  end
end
