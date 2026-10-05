defmodule Flezha.Cluster.GroupedHashing do
  @moduledoc """
  A ProcessHub distribution strategy that hashes a child over the nodes serving
  its GROUP, and every other child over the whole hub.

  A child MAY carry a group in its caller metadata under `group_key` (`:group`
  by default); any term is a group. A node declares the groups it serves with
  `serve_groups/2` and withdraws them with `unserve_groups/2`. A grouped child
  is hashed over that group's nodes and over no other, so a node that does not
  serve the group is never handed the child and nothing has to repair the
  placement afterwards. A child with no group is hashed over every hub node,
  which is what `ProcessHub.Strategy.Distribution.ConsistentHashing` does.

  Nothing here names a namespace, a runner or a factory. Flezha passes a
  relay's factory id as its group and `Flezha.Runner.FleetManager` declares
  the factories its fleet can run, but that vocabulary stays outside.

  ## What holds one answer together on every node

    * **The table** is `%{node => served groups}`, one row per node, owned by
      that node. A change sends the owner's WHOLE set to every peer hub, so an
      announcement is idempotent and a later one repairs a lost earlier one. A
      joining node is sent every node's row; a leaving node's row is dropped.
      `distribution_signature/2` covers the table, so two nodes that disagree
      take the full validation path rather than the start fast path.
    * **One ring per member set.** Groups served by the same nodes share a ring,
      each cached under its own storage key because misc storage copies a value
      out on every read. A ring is built on first use and dropped once no group
      maps to its member set.
    * **A table change re-checks the local children** through the same
      redistribution pass a drain runs, so a child that no longer belongs here
      moves through ProcessHub's migration-consent gate with its handover.
    * **A group nobody serves holds its children where they run.**
      `belongs_to/4` answers the nodes the child is registered on, so every
      migration strategy sees "where it is is where it belongs" and moves
      nothing; answering `[]` would make `SwapMigration` stop it instead. A
      start naming such a group is refused.

  `belongs_to/4` is asked with child ids alone, so a group is resolved from a
  short-lived stash this module fills — in `children_init/4` on the node that
  originates a start, and from the `pre_children_start` hook on a node that
  receives a forwarded one — falling back to the child's registry row once it
  exists.
  """

  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.Cluster
  alias ProcessHub.Service.HookManager
  alias ProcessHub.Service.ProcessRegistry
  alias ProcessHub.Service.Ring
  alias ProcessHub.Service.Storage
  alias ProcessHub.Strategy.Distribution.Base, as: DistributionStrategy
  alias ProcessHub.Task.ClusterUpdateTask.NodeUp

  @type group() :: term()
  @type t() :: %__MODULE__{group_key: atom()}

  defstruct group_key: :group

  # Misc-storage keys. The table is two views of one truth: rows as their owning
  # nodes announced them, and the members-per-group map every placement reads.
  @node_groups :grouped_hashing_node_groups
  @group_nodes :grouped_hashing_group_nodes
  @ring_sets :grouped_hashing_ring_sets

  # Long enough to cover a start request's whole round trip, short enough that a
  # child whose start never registered a row does not keep an entry for the life
  # of the hub. The hub's janitor sweeps expired entries.
  @stash_ttl_ms :timer.minutes(5)

  ##############################################################################
  ### Declaring what this node serves
  ##############################################################################

  @doc """
  Add `groups` to the set this node serves on `hub_id` and announce the new set.

  Idempotent: groups already served change nothing, announce nothing and start
  no redistribution pass.
  """
  @spec serve_groups(atom(), [group()]) :: :ok | {:error, :not_grouped_hashing}
  def serve_groups(hub_id, groups), do: change(hub_id, groups, [])

  @doc "Remove `groups` from the set this node serves on `hub_id`. See `serve_groups/2`."
  @spec unserve_groups(atom(), [group()]) :: :ok | {:error, :not_grouped_hashing}
  def unserve_groups(hub_id, groups), do: change(hub_id, [], groups)

  @doc "The groups `node` serves on `hub_id`, as this node's table has them."
  @spec served_groups(atom(), node()) :: [group()]
  def served_groups(hub_id, node \\ node()),
    do: read(hub_id, [], &Map.get(node_groups(&1), node, []))

  @doc false
  # The member sets a ring is cached for. One ring per member set is an
  # invariant of this module rather than an answer anyone asks for, so this
  # exists for the test that holds it.
  def cached_ring_sets(hub_id),
    do: read(hub_id, MapSet.new(), &(Storage.get(&1, @ring_sets) || MapSet.new()))

  @doc "The nodes serving `group` on `hub_id`, sorted."
  @spec group_nodes(atom(), group()) :: [node()]
  def group_nodes(hub_id, group), do: read(hub_id, [], &Map.get(group_nodes_map(&1), group, []))

  # Every reader keyed by a hub id answers through here, so a hub distributing
  # some other way — or one that is not running — answers each of them as a hub
  # serving nothing would, rather than raising on a table it never had.
  defp read(hub_id, empty, fun) do
    case misc(hub_id) do
      nil -> empty
      misc -> fun.(misc)
    end
  end

  # The hub's misc storage, and the marker that the hub runs this strategy at
  # all: nothing else writes the entry, so a hub distributing some other way
  # answers `nil` here and refuses the declaration.
  defp misc(hub_id), do: :persistent_term.get({__MODULE__, hub_id}, nil)

  defp change(hub_id, add, remove) do
    case misc(hub_id) do
      nil ->
        {:error, :not_grouped_hashing}

      _misc ->
        # A cast, so a declaration never waits on the coordinator. A hub that
        # dies before it lands takes its whole table with it, and the caller
        # re-declares from scratch when the hub starts again.
        GenServer.cast(hub_id, {:exec_cast, {__MODULE__, :apply_local_change, [add, remove]}})
        :ok
    end
  end

  ##############################################################################
  ### Table writes, all of them inside the hub's coordinator
  ##############################################################################

  @doc false
  # Reached through the coordinator's `exec_cast`, which serializes every write
  # to this node's row and makes "read the current set, then change it" safe.
  def apply_local_change(hub, add, remove) do
    current = MapSet.new(Map.get(node_groups(hub.storage.misc), node(), []))
    next = current |> MapSet.union(MapSet.new(add)) |> MapSet.difference(MapSet.new(remove))

    unless MapSet.equal?(next, current) do
      groups = MapSet.to_list(next)
      put_node_groups(hub, node(), groups)
      announce(hub, groups)
      recheck(hub)
    end

    :ok
  end

  @doc false
  # A peer's whole served set, carried by its own `exec_cast`.
  def apply_announcement(hub, from_node, groups) do
    if put_node_groups(hub, from_node, groups), do: recheck(hub)

    :ok
  end

  @doc "Sends the joining node this node's served set."
  @spec handle_node_join(ProcessHub.Hub.t(), %{node: node()}) :: :ok
  def handle_node_join(hub, %{node: joining}) do
    cast_announcement(hub, joining, Map.get(node_groups(hub.storage.misc), node(), []))
    # The hub's node set is one of the member sets rings are cached for.
    prune_rings(hub, group_nodes_map(hub.storage.misc))

    :ok
  end

  @doc """
  Drops the leaving node from every group.

  No redistribution pass is started here: ProcessHub's own node-down handling
  re-places the children that were on that node, and this table is written
  first, so it re-places them among the nodes that still serve their group.
  """
  @spec handle_node_leave(ProcessHub.Hub.t(), %{node: node()}) :: :ok
  def handle_node_leave(hub, %{node: leaving}) do
    put_node_groups(hub, leaving, [])

    :ok
  end

  @doc """
  Drops this node from every group as its hub stops.

  The local table dies with the hub; peers take the same path through the
  cluster leave the coordinator announces as it terminates.
  """
  @spec handle_shutdown(ProcessHub.Hub.t()) :: :ok
  def handle_shutdown(hub) do
    put_node_groups(hub, node(), [])
    :persistent_term.erase({__MODULE__, hub.hub_id})

    :ok
  end

  @doc """
  Records the group of every child of an incoming start request.

  Dispatched before the receiving node works out which of the children are its
  own, so `belongs_to/4` has each child's group while the row is still to come.
  """
  @spec handle_children_start(ProcessHub.Hub.t(), atom(), %{request: struct()}) :: :ok
  def handle_children_start(hub, group_key, %{request: request}) do
    stash(hub, group_key, Enum.map(request.children, &{&1.child_spec.id, &1.metadata}))

    :ok
  end

  ##############################################################################
  ### The protocol's answers, kept here so the impl stays a delegation
  ##############################################################################

  @doc false
  def init(strategy, hub) do
    misc = hub.storage.misc

    :persistent_term.put({__MODULE__, hub.hub_id}, misc)
    Storage.insert(misc, @node_groups, %{})
    Storage.insert(misc, @group_nodes, %{})
    Storage.insert(misc, @ring_sets, MapSet.new())

    register_handler(hub, Hook.pre_node_join(), :gh_join, :handle_node_join, [hub, :_])
    register_handler(hub, Hook.pre_node_leave(), :gh_leave, :handle_node_leave, [hub, :_])
    register_handler(hub, Hook.coordinator_shutdown(), :gh_shutdown, :handle_shutdown, [hub])

    register_handler(hub, Hook.pre_children_start(), :gh_children_start, :handle_children_start, [
      hub,
      strategy.group_key,
      :_
    ])

    strategy
  end

  @doc false
  def belongs_to(strategy, hub, child_ids, replication_factor) do
    misc = hub.storage.misc
    group_nodes = group_nodes_map(misc)
    hub_nodes = hub_nodes(misc)

    Map.new(child_ids, fn child_id ->
      nodes =
        case group_of(strategy, hub, child_id) do
          nil ->
            ring_nodes(hub, hub_nodes, child_id, replication_factor)

          group ->
            case Map.get(group_nodes, group, []) do
              [] -> registered_nodes(hub, child_id)
              members -> ring_nodes(hub, members, child_id, replication_factor)
            end
        end

      {child_id, nodes}
    end)
  end

  @doc false
  def children_init(strategy, hub, child_specs, opts) do
    global = Keyword.get(opts, :metadata) || %{}
    per_child = Keyword.get(opts, :child_metadata) || %{}

    groups =
      stash(
        hub,
        strategy.group_key,
        for(%{id: id} <- child_specs, do: {id, Map.get(per_child, id, global)})
      )

    case unserved(hub, groups) do
      [] -> :ok
      unserved -> {:error, {:no_group_members, unserved}}
    end
  end

  @doc false
  def distribution_signature(hub) do
    misc = hub.storage.misc

    :erlang.phash2({hub_nodes(misc), group_nodes_map(misc)})
  end

  ##############################################################################
  ### Internals
  ##############################################################################

  # Writes `node`'s row and the derived members-per-group map, and answers
  # whether any group's member set moved — the one question that decides
  # whether children have to be re-checked.
  defp put_node_groups(hub, node, groups) do
    misc = hub.storage.misc
    before = group_nodes_map(misc)

    rows =
      case Enum.uniq(groups) do
        [] -> Map.delete(node_groups(misc), node)
        groups -> Map.put(node_groups(misc), node, groups)
      end

    derived = derive_group_nodes(rows)

    Storage.insert(misc, @node_groups, rows)
    Storage.insert(misc, @group_nodes, derived)
    prune_rings(hub, derived)

    derived != before
  end

  defp derive_group_nodes(rows) do
    rows
    |> Enum.reduce(%{}, fn {node, groups}, acc ->
      Enum.reduce(groups, acc, &Map.update(&2, &1, [node], fn nodes -> [node | nodes] end))
    end)
    |> Map.new(fn {group, nodes} -> {group, Enum.sort(nodes)} end)
  end

  defp announce(hub, groups) do
    hub.storage.misc
    |> Cluster.nodes()
    |> Enum.each(&cast_announcement(hub, &1, groups))
  end

  defp cast_announcement(hub, peer, groups) do
    GenServer.cast(
      {hub.hub_id, peer},
      {:exec_cast, {__MODULE__, :apply_announcement, [node(), groups]}}
    )
  end

  # The pass a drain runs after taking itself out of the distribution: every
  # local child is recomputed and whatever no longer belongs here moves through
  # the migration strategy, consent and handover included. Off the coordinator,
  # so a table write never waits on a migration.
  defp recheck(hub) do
    Task.Supervisor.start_child(hub.procs.task_sup, fn ->
      NodeUp.handle(%NodeUp{joined_nodes: [], hub: hub})
    end)

    :ok
  end

  defp register_handler(hub, hook, id, fun, args) do
    HookManager.register_handler(hub.storage.hook, hook, %HookManager{
      id: id,
      m: __MODULE__,
      f: fun,
      a: args,
      p: 100
    })
  end

  defp node_groups(misc), do: Storage.get(misc, @node_groups) || %{}
  defp group_nodes_map(misc), do: Storage.get(misc, @group_nodes) || %{}
  defp hub_nodes(misc), do: misc |> Cluster.nodes([:include_local]) |> Enum.sort()

  defp group_of(strategy, hub, child_id) do
    case Storage.get(hub.storage.misc, stash_key(child_id)) do
      nil -> registry_group(hub, strategy.group_key, child_id)
      group -> group
    end
  end

  defp registry_group(hub, group_key, child_id) do
    case ProcessRegistry.lookup(hub.hub_id, child_id, with_metadata: true) do
      {_child_spec, _node_pids, metadata} when is_map(metadata) -> Map.get(metadata, group_key)
      _ -> nil
    end
  end

  defp registered_nodes(hub, child_id) do
    case ProcessRegistry.lookup(hub.hub_id, child_id) do
      {_child_spec, node_pids} -> Keyword.keys(node_pids)
      _ -> []
    end
  end

  defp ring_nodes(_hub, [], _child_id, _replication_factor), do: []

  defp ring_nodes(hub, members, child_id, replication_factor) do
    hub
    |> ring(members)
    |> Ring.key_to_nodes(child_id, replication_factor)
  end

  defp ring(hub, members) do
    misc = hub.storage.misc

    case Storage.get(misc, ring_key(members)) do
      nil ->
        ring = Ring.create_ring(members)
        Storage.insert(misc, ring_key(members), ring)
        Storage.update(misc, @ring_sets, &MapSet.put(&1 || MapSet.new(), members))
        ring

      ring ->
        ring
    end
  end

  # A ring is worth its memory only while some group — or the hub itself, which
  # is the member set an ungrouped child hashes over — still maps to its nodes.
  defp prune_rings(hub, group_nodes) do
    misc = hub.storage.misc
    live = group_nodes |> Map.values() |> MapSet.new() |> MapSet.put(hub_nodes(misc))
    cached = Storage.get(misc, @ring_sets) || MapSet.new()

    Enum.each(MapSet.difference(cached, live), &Storage.remove(misc, ring_key(&1)))
    Storage.insert(misc, @ring_sets, MapSet.intersection(cached, live))
  end

  defp unserved(hub, groups) do
    table = group_nodes_map(hub.storage.misc)

    groups
    |> Map.values()
    |> Enum.uniq()
    |> Enum.filter(&(Map.get(table, &1, []) === []))
  end

  # The groups of `pairs` (`{child_id, caller metadata}`), recorded so
  # `belongs_to/4` can answer for a child that has no registry row yet, and
  # answered back so the caller can judge them. A child carrying no group is in
  # neither: it is placed over the whole hub and nothing has to be remembered.
  defp stash(hub, group_key, pairs) do
    groups =
      Enum.reduce(pairs, %{}, fn {child_id, metadata}, acc ->
        case Map.get(metadata || %{}, group_key) do
          nil -> acc
          group -> Map.put(acc, child_id, group)
        end
      end)

    Enum.each(groups, fn {child_id, group} ->
      Storage.insert(hub.storage.misc, stash_key(child_id), group, ttl: @stash_ttl_ms)
    end)

    groups
  end

  defp stash_key(child_id), do: {:grouped_hashing_group, child_id}
  defp ring_key(members), do: {:grouped_hashing_ring, members}

  defimpl DistributionStrategy, for: Flezha.Cluster.GroupedHashing do
    alias Flezha.Cluster.GroupedHashing

    @impl true
    def init(strategy, hub), do: GroupedHashing.init(strategy, hub)

    @impl true
    def belongs_to(strategy, hub, child_ids, replication_factor),
      do: GroupedHashing.belongs_to(strategy, hub, child_ids, replication_factor)

    @impl true
    def children_init(strategy, hub, child_specs, opts),
      do: GroupedHashing.children_init(strategy, hub, child_specs, opts)

    @impl true
    def deterministic?(_strategy), do: true

    @impl true
    def distribution_signature(_strategy, hub), do: GroupedHashing.distribution_signature(hub)
  end
end
