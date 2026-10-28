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

  `spec_group`, a `{module, function}` taking a child spec, reads a group from
  the spec itself. It is the last place a group is looked for, after the start's
  metadata and the registry row, so a declared child replayed by a node that has
  neither, one whose registry was lost, still lands in its group.

  Nothing here names a namespace, a runner or a factory. Flezha passes a
  relay's factory id as its group and `Flezha.Runner.FleetManager` declares
  the factories its fleet can run, but that vocabulary stays outside.

  ## What holds one answer together on every node

    * **The table** is `%{node => served groups}`, one row per node, owned by
      that node. A change sends the owner's WHOLE set to every peer hub, so an
      announcement is idempotent and a later one repairs a lost earlier one. A
      joining node is sent every node's row; a leaving node's row is dropped.
      A node whose coordinator restarted starts from an empty table, never a
      saved one. Its owner declares its set again, and only then does the node
      send that row to every peer, which replaces the old one and answers with
      its own, so the node never re-checks its children against a table that
      lacks its own row. A node whose hub started afresh, after a crash
      included, sends no empty row to a joining peer until its owner declares
      or `:reconcile_interval_ms` passes, so its peers keep the row they hold
      for it and place its children on it, where a start it can place nowhere
      is refused with `:no_target_nodes`, rather than on themselves. A row
      still empty at that bound is sent then, so the peers drop theirs.
      `distribution_signature/2` covers the table, so two nodes that disagree
      take the full validation path rather than the start fast path.
    * **One ring per member set.** Groups served by the same nodes share a ring,
      each cached under its own storage key because misc storage copies a value
      out on every read. A ring is built on first use and dropped once no group
      maps to its member set.
    * **A table change re-checks the local children** through the same
      redistribution pass a drain runs, so a child that no longer belongs here
      moves through ProcessHub's migration-consent gate with its handover.
    * **A node's first reconcile round waits until the node serves a group.**
      A node that has just started serves nothing until its owner declares its
      set, and a declared child placed before then lands on whichever peers
      serve its group, never back on the node that ran it. When a peer kept a
      row for this node, the round's blocking `pre_recovery_replay` hook holds
      it until this node's row is written, at most the hook's budget
      (`:reconcile_interval_ms`) less one poll; past that it places as it would
      have. A node no peer remembers, alone or on a cold start of the whole
      cluster, is not held.
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

  alias Flezha.Config.Durations
  alias Flezha.Process.Poll
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.Cluster
  alias ProcessHub.Service.DeclaredChildren
  alias ProcessHub.Service.HookManager
  alias ProcessHub.Service.ProcessRegistry
  alias ProcessHub.Service.Recovery
  alias ProcessHub.Service.Ring
  alias ProcessHub.Service.Storage
  alias ProcessHub.Strategy.Distribution.Base, as: DistributionStrategy
  alias ProcessHub.Task.ClusterUpdateTask.NodeUp

  @type group() :: term()
  @type t() :: %__MODULE__{group_key: atom(), spec_group: {module(), atom()} | nil}

  defstruct group_key: :group, spec_group: nil

  # Misc-storage keys. The table is two views of one truth: rows as their owning
  # nodes announced them, and the members-per-group map every placement reads.
  @node_groups :grouped_hashing_node_groups
  @group_nodes :grouped_hashing_group_nodes
  @ring_sets :grouped_hashing_ring_sets
  @restart_pending :grouped_hashing_restart_pending
  @recheck_pending :grouped_hashing_recheck_pending

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

  @doc false
  # Whether a redistribution pass is armed and has not run yet, for a test that
  # must not let one land between its own steps.
  def recheck_pending?(hub_id),
    do: read(hub_id, false, &(Storage.get(&1, @recheck_pending) != nil))

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

  defp change(hub_id, add, remove), do: in_coordinator(hub_id, :apply_local_change, [add, remove])

  # A cast, so a caller never waits on the coordinator. A hub that dies before
  # it lands takes its whole table with it, and the caller re-declares from
  # scratch when the hub starts again.
  defp in_coordinator(hub_id, fun, args) do
    case misc(hub_id) do
      nil ->
        {:error, :not_grouped_hashing}

      _misc ->
        GenServer.cast(hub_id, {:exec_cast, {__MODULE__, fun, args}})
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
    current = MapSet.new(own_groups(hub))
    next = current |> MapSet.union(MapSet.new(add)) |> MapSet.difference(MapSet.new(remove))

    unless MapSet.equal?(next, current) do
      groups = MapSet.to_list(next)
      put_node_groups(hub, node(), groups)
      announce(hub, groups, :apply_announcement)
      recheck(hub)
      # A declared child the last reconcile round refused for want of a member
      # can start now, without waiting for the reconcile interval.
      unless MapSet.subset?(next, current), do: Recovery.request_round(hub.hub_id)
    end

    :ok
  end

  @doc false
  # A peer's whole served set, carried by its own `exec_cast`.
  def apply_announcement(hub, from_node, groups) do
    if put_node_groups(hub, from_node, groups), do: recheck(hub)

    :ok
  end

  @doc false
  # A restarted peer's set replaces the row held for it, and is answered with
  # this node's own row as a joining node is.
  def apply_restart_announcement(hub, from_node, groups) do
    apply_announcement(hub, from_node, groups)
    handle_node_join(hub, %{node: from_node})
  end

  @doc """
  Sends the joining node this node's served set.

  Run once the node is in the hub's node list. A change of this node's set
  that lands meanwhile then either finds the node in that list and announces
  to it, or was written before this reads the set.
  """
  @spec handle_node_join(ProcessHub.Hub.t(), %{node: node()}) :: :ok
  def handle_node_join(hub, %{node: joining}) do
    # Until its owner declares, an empty row says nothing: sent, it would erase
    # the row the joining node holds for this one.
    case {own_groups(hub), Storage.get(hub.storage.misc, @restart_pending)} do
      {[], pending} when pending != nil -> :ok
      {groups, _pending} -> cast_announcement(hub, joining, groups, :apply_announcement)
    end

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
  Waits, after its hub's coordinator restarted, for the owner to declare this
  node's set again (`restart_declared/1`), at most `process_hub_restart_declare_ms`.

  `init/2` has just written an empty table. Announcing that empty row now would
  have the peers' answers re-check this node's children against a table that
  lacks its own row.
  """
  @spec handle_restart(ProcessHub.Hub.t()) :: :ok
  def handle_restart(hub) do
    await_declaration(hub, :restart, :declared, Durations.get(:process_hub_restart_declare_ms))
  end

  @doc """
  Tells `hub_id` its owner has declared this node's set after a coordinator
  restart. Queued behind that declaration when sent from the same process.
  A no-op on a hub that did not restart.
  """
  @spec restart_declared(atom()) :: :ok | {:error, :not_grouped_hashing}
  def restart_declared(hub_id), do: in_coordinator(hub_id, :finish_restart, [])

  @doc false
  # Sends every peer this node's row once after a restart: each replaces what it
  # held for this node and answers with its own row. A hub that started afresh
  # ends its wait only at its bound, since the declaration that counts there
  # comes from runners still reconnecting and announces itself when it lands;
  # at the bound only a row still empty is sent, which drops whatever a peer
  # kept for this node and changes nothing on the others.
  def finish_restart(hub, reason \\ :declared) do
    case {Storage.get(hub.storage.misc, @restart_pending), reason} do
      {nil, _reason} ->
        :ok

      {:fresh, :declared} ->
        :ok

      {:fresh, :bound} ->
        Storage.remove(hub.storage.misc, @restart_pending)
        if own_groups(hub) == [], do: announce(hub, [], :apply_announcement)

      {:restart, _reason} ->
        Storage.remove(hub.storage.misc, @restart_pending)
        announce(hub, own_groups(hub), :apply_restart_announcement)
    end

    :ok
  end

  @doc """
  Holds the first reconcile round of a node returning to a running hub, one
  whose row a peer kept, before it starts any declared child, until this node
  serves a group, at most the hook's budget less one poll.

  A node no peer remembers, as on a cold start of the whole cluster, is not
  held: its runners may wait for it to be ready. Runs in the round's hook
  worker, never the coordinator, so the declaration it waits for lands
  meanwhile.
  """
  @spec handle_first_round(ProcessHub.Hub.t()) :: :ok
  def handle_first_round(hub) do
    if remembered_by_peer?(hub) do
      poll = Durations.get(:process_hub_own_row_poll_ms)

      deadline =
        System.monotonic_time(:millisecond) + hub.recovery_config.reconcile_interval_ms - poll

      _ =
        Poll.until(deadline, poll, fn ->
          if own_groups(hub) == [], do: {:error, :not_serving}, else: {:ok, :serving}
        end)
    end

    :ok
  end

  # Asked of every peer at once and bounded, so a slow peer only shortens the
  # answer to "not remembered".
  defp remembered_by_peer?(hub) do
    ask = Durations.get(:process_hub_peer_ask_ms)

    own_groups(hub) == [] and
      hub.storage.misc
      |> Cluster.nodes()
      |> :erpc.multicall(__MODULE__, :served_groups, [hub.hub_id, node()], ask)
      |> Enum.any?(&match?({:ok, [_ | _]}, &1))
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
    hold_fresh_row(hub)

    register_handler(hub, Hook.post_node_join(), :gh_join, :handle_node_join, [hub, :_])
    register_handler(hub, Hook.pre_node_leave(), :gh_leave, :handle_node_leave, [hub, :_])
    register_handler(hub, Hook.coordinator_shutdown(), :gh_shutdown, :handle_shutdown, [hub])
    register_handler(hub, Hook.coordinator_restarted(), :gh_restart, :handle_restart, [hub])
    register_handler(hub, Hook.pre_recovery_replay(), :gh_first_round, :handle_first_round, [hub])

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

  defp announce(hub, groups, apply) do
    hub.storage.misc
    |> Cluster.nodes()
    |> Enum.each(&cast_announcement(hub, &1, groups, apply))
  end

  defp cast_announcement(hub, peer, groups, apply) do
    GenServer.cast({hub.hub_id, peer}, {:exec_cast, {__MODULE__, apply, [node(), groups]}})
  end

  # The pass a drain runs after taking itself out of the distribution: every
  # local child is recomputed and whatever no longer belongs here moves through
  # the migration strategy, consent and handover included. Off the coordinator,
  # so a table write never waits on a migration.
  #
  # Runs in the coordinator. The pending mark names the coordinator that armed
  # it, so one restarted before its pass ran arms a new one.
  defp recheck(hub) do
    unless Storage.get(hub.storage.misc, @recheck_pending) == self() do
      Storage.insert(hub.storage.misc, @recheck_pending, self())
      callback = {:post_action_callback, __MODULE__, :run_recheck, []}
      Process.send_after(self(), callback, Durations.get(:grouped_hashing_recheck_window_ms))
    end

    :ok
  end

  @doc false
  def run_recheck(hub) do
    Storage.remove(hub.storage.misc, @recheck_pending)

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

  # A node whose hub has just started serves nothing until its owner declares.
  # Its peers keep the row they hold for it meanwhile, so the children that ran
  # here are not placed elsewhere, at most the first reconcile round's budget.
  defp hold_fresh_row(hub),
    do: await_declaration(hub, :fresh, :bound, hub.recovery_config.reconcile_interval_ms)

  # Marks this node's row as awaiting its owner's declaration and arms the
  # bound after which `finish_restart/2` sends it whatever it holds.
  defp await_declaration(hub, kind, reason, bound_ms) do
    Storage.insert(hub.storage.misc, @restart_pending, kind)

    Process.send_after(
      self(),
      {:post_action_callback, __MODULE__, :finish_restart, [reason]},
      bound_ms
    )

    :ok
  end

  defp node_groups(misc), do: Storage.get(misc, @node_groups) || %{}
  defp own_groups(hub), do: Map.get(node_groups(hub.storage.misc), node(), [])
  defp group_nodes_map(misc), do: Storage.get(misc, @group_nodes) || %{}
  defp hub_nodes(misc), do: misc |> Cluster.nodes([:include_local]) |> Enum.sort()

  defp group_of(strategy, hub, child_id) do
    with nil <- Storage.get(hub.storage.misc, stash_key(child_id)),
         nil <- registry_group(hub, strategy.group_key, child_id),
         do: declared_group(strategy, hub, child_id)
  end

  defp declared_group(%{spec_group: nil}, _hub, _child_id), do: nil

  defp declared_group(%{spec_group: {module, fun}}, hub, child_id) do
    case DeclaredChildren.declared_child(hub.hub_id, child_id) do
      %{} = child_spec -> apply(module, fun, [child_spec])
      nil -> nil
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
