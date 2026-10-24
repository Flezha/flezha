defmodule Flezha.Cluster.GroupedHashingTest do
  @moduledoc """
  The distribution strategy on its own: where a child's group is read from,
  what an ungrouped child answers, how a declaration reaches the table and what
  a group nobody serves does to children already running.

  One hub, no peers. The placement answers are driven against a bare hub —
  storage tables and a node list, which is everything `belongs_to/4` reads —
  because a fake node list is the only way to hold the ungrouped answer against
  `ConsistentHashing`'s over the same nodes. The declaration API needs a live
  coordinator and gets one.
  """

  use ExUnit.Case, async: false

  alias Flezha.Cluster.GroupedHashing
  alias FlezhaTest.Helper.NodeTestHelper
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Constant.StorageKey
  alias ProcessHub.Service.HookManager
  alias ProcessHub.Service.Storage
  alias ProcessHub.Strategy.Distribution.Base, as: DistributionStrategy
  alias ProcessHub.Strategy.Distribution.ConsistentHashing
  alias ProcessHub.Strategy.Redundancy.Base, as: RedundancyStrategy
  alias ProcessHub.Strategy.Redundancy.Replication

  @a :"a@127.0.0.1"
  @b :"b@127.0.0.1"
  @c :"c@127.0.0.1"

  describe "belongs_to/4" do
    setup do
      {:ok, hub: bare_hub()}
    end

    test "a grouped child is hashed over the nodes serving its group and no other", %{hub: hub} do
      serve(hub, @a, ["report"])
      serve(hub, @b, ["report"])

      placements = hub |> placements(ids("grouped", 60), "report") |> Map.values()

      assert Enum.sort(Enum.uniq(List.flatten(placements))) == [@a, @b]
      assert Enum.all?(placements, &(length(&1) == 1))
    end

    test "a replication factor above the group size answers every serving node and no other",
         %{hub: hub} do
      serve(hub, @a, ["report"])
      serve(hub, @b, ["report"])

      for factor <- [5, :cluster_size] do
        replicas = RedundancyStrategy.replication_factor(%Replication{replication_factor: factor})

        assert %{"wide" => nodes} = placements(hub, ["wide"], "report", replicas)
        assert nodes -- [@a, @b] == [], "a replica landed on a node that does not serve the group"
        assert Enum.uniq(nodes) == nodes
        assert length(nodes) == min(replicas, 2)
      end
    end

    test "a child with no group is placed where ConsistentHashing places it", %{hub: hub} do
      serve(hub, @a, ["report"])
      ids = ids("ungrouped", 100)

      consistent_hub = bare_hub()
      consistent = DistributionStrategy.init(%ConsistentHashing{}, consistent_hub)

      assert DistributionStrategy.belongs_to(%GroupedHashing{}, hub, ids, 1) ==
               DistributionStrategy.belongs_to(consistent, consistent_hub, ids, 1)
    end

    test "a group nobody serves holds its children where they run", %{hub: hub} do
      register(hub, "held", @a, %{group: "gone"})

      assert DistributionStrategy.belongs_to(%GroupedHashing{}, hub, ["held"], 1) == %{
               "held" => [@a]
             }
    end

    test "a child of an unserved group that runs nowhere belongs nowhere", %{hub: hub} do
      assert placements(hub, ["never_started"], "gone") == %{"never_started" => []}
    end

    test "the group is read from the child's registry row once it has one", %{hub: hub} do
      serve(hub, @a, ["report"])
      register(hub, "from_row", @c, %{group: "report"})

      assert DistributionStrategy.belongs_to(%GroupedHashing{}, hub, ["from_row"], 1) == %{
               "from_row" => [@a]
             }
    end

    test "the strategy's group_key names the metadata key", %{hub: hub} do
      strategy = GroupedHashing.init(%GroupedHashing{group_key: :factory}, hub)
      serve(hub, @a, ["report"])
      register(hub, "keyed", @c, %{factory: "report", group: "ignored"})

      assert DistributionStrategy.belongs_to(strategy, hub, ["keyed"], 1) == %{"keyed" => [@a]}
    end

    test "a group is never turned into an atom", %{hub: hub} do
      group = "gh_group_#{System.unique_integer([:positive])}"
      serve(hub, @a, [group])

      assert placements(hub, ["atomless"], group) == %{"atomless" => [@a]}
      assert_raise ArgumentError, fn -> String.to_existing_atom(group) end
    end

    test "groups with equal member sets share one ring, and an unused ring is dropped", %{
      hub: hub
    } do
      serve(hub, @a, ["one", "two"])
      serve(hub, @b, ["one", "two"])

      placements(hub, ["x"], "one")
      placements(hub, ["y"], "two")

      # Both groups are served by `[a, b]`, so they share the one ring.
      assert MapSet.size(GroupedHashing.cached_ring_sets(hub.hub_id)) == 1

      # An ungrouped child hashes over the whole hub, which is a member set of
      # its own and stays in use for as long as the hub does.
      DistributionStrategy.belongs_to(%GroupedHashing{}, hub, ["plain"], 1)
      assert MapSet.size(GroupedHashing.cached_ring_sets(hub.hub_id)) == 2

      serve(hub, @a, [])
      serve(hub, @b, [])

      assert GroupedHashing.cached_ring_sets(hub.hub_id) ==
               MapSet.new([Enum.sort([node(), @a, @b, @c])])
    end
  end

  describe "children_init/4" do
    setup do
      {:ok, hub: bare_hub()}
    end

    test "a start naming a group nobody serves is refused", %{hub: hub} do
      assert children_init(hub, ["refused"], metadata: %{group: "gone"}) ==
               {:error, {:no_group_members, ["gone"]}}
    end

    test "a served group, and a child with no group at all, are admitted", %{hub: hub} do
      serve(hub, @a, ["report"])

      assert children_init(hub, ["served"], metadata: %{group: "report"}) == :ok
      assert children_init(hub, ["plain"], []) == :ok
    end

    test "a per-child group is read from :child_metadata", %{hub: hub} do
      serve(hub, @a, ["report"])

      assert children_init(hub, ["per_child"], child_metadata: %{"per_child" => %{group: "gone"}}) ==
               {:error, {:no_group_members, ["gone"]}}
    end

    test "the start's group is answered before the child has a registry row", %{hub: hub} do
      serve(hub, @a, ["report"])

      assert :ok = children_init(hub, ["stashed"], metadata: %{group: "report"})

      assert DistributionStrategy.belongs_to(%GroupedHashing{}, hub, ["stashed"], 1) == %{
               "stashed" => [@a]
             }
    end
  end

  describe "a table change re-checks the local children" do
    setup do
      hub = bare_hub()
      watch_redistribution(hub)

      {:ok, hub: hub}
    end

    test "one pass per announcement, however many groups it names", %{hub: hub} do
      serve(hub, @a, Enum.map(1..100, &"group_#{&1}"))

      assert_receive :pre_redistribution, 1_000
      refute_receive :pre_redistribution, 200
    end

    test "an announcement that moves no member set starts no pass", %{hub: hub} do
      serve(hub, @a, ["report"])
      assert_receive :pre_redistribution, 1_000

      serve(hub, @a, ["report"])
      refute_receive :pre_redistribution, 200
    end

    test "a local change starts one pass, and repeating it starts none", %{hub: hub} do
      GroupedHashing.apply_local_change(hub, ["report"], [])
      assert_receive :pre_redistribution, 1_000

      GroupedHashing.apply_local_change(hub, ["report"], [])
      refute_receive :pre_redistribution, 200
    end

    test "a node leaving is dropped from every group without a pass of its own", %{hub: hub} do
      serve(hub, @a, ["report"])
      serve(hub, @b, ["report"])
      assert_receive :pre_redistribution, 1_000
      assert_receive :pre_redistribution, 1_000

      GroupedHashing.handle_node_leave(hub, %{node: @a})

      assert GroupedHashing.group_nodes(hub.hub_id, "report") == [@b]
      assert GroupedHashing.served_groups(hub.hub_id, @a) == []
      refute_receive :pre_redistribution, 200
    end

    test "an announcement lost on the way is repaired by the next one", %{hub: hub} do
      # `a` served `report` before this node's hub existed and its cast was
      # dropped, so the table never held the row.
      assert GroupedHashing.group_nodes(hub.hub_id, "report") == []

      # Every announcement carries the whole set rather than a delta, so the
      # next one `a` makes for any reason repairs it.
      serve(hub, @a, ["report", "echo"])

      assert GroupedHashing.group_nodes(hub.hub_id, "report") == [@a]
    end

    test "a joining node is sent this node's whole served set", %{hub: hub} do
      GroupedHashing.apply_local_change(hub, ["report", "echo"], [])

      # Stand in for the joining node's hub: an announcement is a cast to the
      # process registered under the hub's name there.
      Process.register(self(), hub.hub_id)
      GroupedHashing.handle_node_join(hub, %{node: node()})

      assert_receive {:"$gen_cast",
                      {:exec_cast, {GroupedHashing, :apply_announcement, [_from, groups]}}}

      assert Enum.sort(groups) == ["echo", "report"]
    end

    # fails when: the joining node is sent this node's set before it is in
    # the hub's node list, so a change landing in between is announced to
    # every node but it, and the two disagree on the group from then on.
    test "a joining node is sent the set once it is in the hub's node list", %{hub: hub} do
      handlers =
        &Enum.map(HookManager.registered_handlers(hub.storage.hook, &1), fn h -> h.id end)

      assert :gh_join in handlers.(Hook.post_node_join())
      refute :gh_join in handlers.(Hook.pre_node_join())
    end

    test "a stopping hub leaves every group and forgets the hub", %{hub: hub} do
      GroupedHashing.apply_local_change(hub, ["report"], [])
      assert GroupedHashing.group_nodes(hub.hub_id, "report") == [node()]

      GroupedHashing.handle_shutdown(hub)

      assert GroupedHashing.group_nodes(hub.hub_id, "report") == []
      assert GroupedHashing.serve_groups(hub.hub_id, ["report"]) == {:error, :not_grouped_hashing}
    end
  end

  describe "distribution_signature/2" do
    test "two nodes whose group tables differ do not share a signature" do
      one = bare_hub()
      other = bare_hub()

      assert DistributionStrategy.distribution_signature(%GroupedHashing{}, one) ==
               DistributionStrategy.distribution_signature(%GroupedHashing{}, other)

      serve(one, @a, ["report"])

      refute DistributionStrategy.distribution_signature(%GroupedHashing{}, one) ==
               DistributionStrategy.distribution_signature(%GroupedHashing{}, other)
    end
  end

  describe "serve_groups/2 and unserve_groups/2" do
    test "a declaration reaches the table and repeating it changes nothing" do
      hub_id = live_hub(%GroupedHashing{})

      assert :ok = GroupedHashing.serve_groups(hub_id, ["report", "echo"])
      assert settled(hub_id, ["echo", "report"])
      assert GroupedHashing.group_nodes(hub_id, "report") == [node()]

      assert :ok = GroupedHashing.serve_groups(hub_id, ["report"])
      assert settled(hub_id, ["echo", "report"])

      assert :ok = GroupedHashing.unserve_groups(hub_id, ["report"])
      assert settled(hub_id, ["echo"])
      assert GroupedHashing.group_nodes(hub_id, "report") == []

      assert :ok = GroupedHashing.unserve_groups(hub_id, ["report"])
      assert settled(hub_id, ["echo"])
    end

    test "the declaration is refused on a hub distributing some other way" do
      hub_id = live_hub(%ConsistentHashing{})

      assert GroupedHashing.serve_groups(hub_id, ["report"]) == {:error, :not_grouped_hashing}
      assert GroupedHashing.unserve_groups(hub_id, ["report"]) == {:error, :not_grouped_hashing}
      assert GroupedHashing.served_groups(hub_id) == []
    end
  end

  # ---------- the hubs these tests run against ----------

  # Storage tables and a node list: everything `belongs_to/4` reads, and nothing
  # more. The registry table is named after the hub because `ProcessRegistry`
  # addresses it by hub id.
  defp bare_hub do
    hub_id = :"gh_bare_#{System.unique_integer([:positive])}"
    :ets.new(hub_id, [:set, :public, :named_table])
    misc = :ets.new(:gh_misc, [:set, :public])
    hook = :ets.new(:gh_hook, [:set, :public])

    Storage.insert(misc, StorageKey.hn(), [node(), @a, @b, @c])
    Storage.insert(misc, StorageKey.strdist(), %GroupedHashing{})
    Storage.insert(misc, StorageKey.strred(), %ProcessHub.Strategy.Redundancy.Singularity{})
    Storage.insert(misc, StorageKey.strmigr(), %ProcessHub.Strategy.Migration.ColdSwap{})
    Storage.insert(misc, StorageKey.strsyn(), %ProcessHub.Strategy.Synchronization.PubSub{})

    Storage.insert(
      misc,
      StorageKey.strpart(),
      %ProcessHub.Strategy.PartitionTolerance.Divergence{}
    )

    # Real ones: a table change schedules the redistribution pass on the task
    # supervisor, and a pass that never ran would leave that half of the module
    # untested. The pass asks the system registry for the distributed
    # supervisor to decide whether the hub is whole; this process stands in for
    # it, so the pass runs rather than short-circuiting as partitioned.
    task_sup = start_supervised!({Task.Supervisor, []}, id: {:gh_task_sup, hub_id})
    registry = :"gh_registry_#{System.unique_integer([:positive])}"
    start_supervised!({Registry, keys: :unique, name: registry}, id: registry)
    {:ok, _} = Registry.register(registry, "dist_sup", nil)

    # The pass hands its migration to the worker queue, named as the hub names it.
    worker_queue = {:via, Registry, {registry, "worker_queue"}}

    start_supervised!({ProcessHub.Worker.WorkerQueue, {hub_id, worker_queue, misc}},
      id: {:gh_worker_queue, hub_id}
    )

    hub = %ProcessHub.Hub{
      hub_id: hub_id,
      procs: %{task_sup: task_sup, system_registry: registry, worker_queue: worker_queue},
      storage: %{misc: misc, hook: hook}
    }

    GroupedHashing.init(%GroupedHashing{}, hub)
    on_exit(fn -> :persistent_term.erase({GroupedHashing, hub_id}) end)

    hub
  end

  defp live_hub(strategy) do
    hub_id = :"gh_live_#{System.unique_integer([:positive])}"

    start_supervised!(
      ProcessHub.child_spec(%ProcessHub{hub_id: hub_id, distribution_strategy: strategy})
    )

    hub_id
  end

  # A node's row is only ever written by that node's announcement, so the bare
  # hub's table is driven through the same entry point a peer's cast reaches.
  defp serve(hub, node, groups), do: GroupedHashing.apply_announcement(hub, node, groups)

  defp ids(prefix, count), do: for(index <- 1..count, do: "#{prefix}_#{index}")

  defp children_init(hub, ids, opts) do
    specs = Enum.map(ids, &%{id: &1, start: {Kernel, :node, []}})

    DistributionStrategy.children_init(%GroupedHashing{}, hub, specs, opts)
  end

  # The start path: `children_init/4` records each child's group, then the
  # placement is asked for with the ids alone, as every caller asks it.
  defp placements(hub, ids, group, replication_factor \\ 1) do
    children_init(hub, ids, metadata: %{group: group})

    DistributionStrategy.belongs_to(%GroupedHashing{}, hub, ids, replication_factor)
  end

  defp register(hub, child_id, node, metadata) do
    child_spec = %{id: child_id, start: {Kernel, :node, []}}

    Storage.insert(hub.hub_id, child_id, {child_spec, [{node, self()}], metadata})
  end

  # The pass runs off the coordinator, so the hook it dispatches is what a test
  # can wait on.
  defp watch_redistribution(hub) do
    test = self()

    ProcessHub.Service.HookManager.register_handler(
      hub.storage.hook,
      ProcessHub.Constant.Hook.pre_redistribution(),
      %ProcessHub.Service.HookManager{
        id: :gh_test_probe,
        m: Process,
        f: :send,
        a: [test, :pre_redistribution, []]
      }
    )
  end

  defp settled(hub_id, groups) do
    NodeTestHelper.wait_until(
      fn -> Enum.sort(GroupedHashing.served_groups(hub_id)) == groups end,
      2_000
    )
  end
end
