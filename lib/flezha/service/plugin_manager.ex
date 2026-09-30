defmodule Flezha.Service.PluginManager do
  @moduledoc """
  Per-node registry of enabled `%Flezha.Plugin{}` entries. Routes
  `enable_plugin/2` into the owning operator's Overwatch and persists the
  plugin into that operator's ProcessHub child-spec, so it survives an
  Overwatch restart, a controller restart and a cluster cold boot.

  Ordering is canonicalise, then durable write, then runtime mutation: an
  ill-typed or unknown hook is refused before any durable record
  exists, so the convergence loop is never handed intent that cannot be
  satisfied. The durable write is two records at one seam, the hub's registry
  row and its declared set, because only the declared set is read back after a
  restart. A failed durable write touches no runtime, so the caller may retry;
  a failed runtime call rolls both records back, so no surface is left claiming
  a plugin whose enable never succeeded.

  Every call this manager makes into an operator it does not own carries a
  bound, because the manager is a node-wide singleton whose table starts empty:
  an exit taken from a busy Overwatch would forget every plugin on the node. A
  disable that could not reach its operator names the plugins it could not drop.
  """

  use GenServer

  require Logger

  @behaviour Flezha.Shutdown.Step
  use Flezha.AuthZ.Verb

  alias Flezha.AuthZ.Namespace
  alias Flezha.Cluster.Directory
  alias Flezha.Config.Durations
  alias Flezha.Config.Backoff
  alias Flezha.Identificator
  alias Flezha.Operator.Overwatch
  alias Flezha.Operator.PartitionGroup
  alias Flezha.Parallel
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Process.Hub
  alias Flezha.SDK.SafeCall
  alias Flezha.Service.CallerContext
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.RegistryUpdate
  alias ProcessHub.Service.ProcessRegistry

  # This manager is a node-wide singleton and its table does not survive it:
  # `init/1` starts empty, so an exit here forgets every plugin enabled on the
  # node, not just the one being disabled. The calls it makes into an operator
  # carry `Overwatch`'s own bound, which is the smaller of the two so the
  # manager answers before the caller waiting on it stops waiting.

  # ProcessHub's own answer for a row it does not hold (yet).
  @no_child "No child found"

  ## Public API

  # Canonicalised here, before anything reads the hooks and before any durable
  # write: the declaration-only half of validation needs no live table, only the
  # host catalog. A refusal therefore costs nothing and leaves nothing, and
  # `refuse_init_subscription/1` below matches a name the user typed as
  # text. What is persisted is the canonical struct.
  def enable_plugin(plugin_id, %Plugin{} = plugin) do
    with {:ok, plugin} <- Table.canonicalise(Overwatch.__extensions_catalog__(), plugin),
         :ok <- refuse_init_subscription(plugin) do
      case PartitionGroup.members(plugin.namespace, plugin.operator_id) do
        [] -> do_enable_plugin(plugin_id, plugin)
        members -> enable_across_group(plugin_id, plugin, members)
      end
    end
  end

  # The manager serialises the id claim, never the enable itself: the cspec
  # write and the subscribe run here, in the caller, so enables of different
  # plugins overlap and their registry writes share one durable sync. A claim
  # whose enable fails is released, so a failed enable leaves no entry behind.
  defp do_enable_plugin(plugin_id, plugin) do
    with :ok <- check_operator_target(plugin),
         :claimed <- GenServer.call(__MODULE__, {:claim, plugin_id, plugin}) do
      enable_claimed(plugin_id, plugin)
    end
  end

  defp enable_claimed(plugin_id, plugin) do
    case write_enable_to_cspec(plugin, plugin_id) do
      :ok ->
        subscribe_claimed(plugin_id, plugin)

      err ->
        rollback_enable(plugin_id, plugin)
        err
    end
  end

  # Past the durable write, so a failure here removes it as well as the claim.
  # Leaving it is worse than either outcome alone: the manager's table is what
  # `flz plugin list` reads, so the plugin goes invisible while the durable
  # records still carry it, and the next start subscribes it and parks the
  # operator on `:plugin_prepare_failed` if it still cannot come up. A
  # controller that DIES here runs no rollback, which is the crash case
  # `auto-restore-on-boot` requires to self-heal on the next start.
  defp subscribe_claimed(plugin_id, %Plugin{} = plugin) do
    with {:ok, pid} <- resolve_operator(plugin),
         :ok <- Overwatch.plugin_subscribe(pid, plugin_id, plugin) do
      GenServer.cast(__MODULE__, {:confirm, plugin_id})
      {:ok, "plugin enabled"}
    else
      err ->
        rollback_enable(plugin_id, plugin)
        err
    end
  end

  # `unregister_plugin/1` alone is not the rollback: it is also the teardown
  # path (`Flezha.Operator.Plugins.unregister/1`), where removing the durable
  # entry would make every migration erase the manifest. The durable half
  # belongs here, where only a failed enable reaches it.
  defp rollback_enable(plugin_id, %Plugin{namespace: ns, operator_id: operator_id}) do
    case write_disable_to_cspec(ns, operator_id, plugin_id) do
      :ok ->
        :ok

      # The same unreachable hub or absent operator that failed the enable fails
      # its removal, and there was nothing to leave behind either way.
      {:error, {:operator_not_found, _namespace, _operator_id}} ->
        :ok

      {:error, reason} when reason in [:registry_unavailable, :operator_not_declared] ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "[plugin_manager] rollback left #{plugin_id} on #{ns}/#{operator_id}: " <>
            "#{inspect(reason)}; the next restart re-applies it from the declared set"
        )
    end

    unregister_plugin(plugin_id)
  end

  # A group id names no operator — it names N of them. Enable runs once per
  # member with that member's partition opts injected and its `#pK` appended to
  # the plugin id, which is also what keeps this node's plugin ids unique when
  # two members land here. Every member's outcome is reported: a partial enable
  # is a real state and must never read as success.
  defp enable_across_group(plugin_id, plugin, members) do
    results =
      Flezha.Parallel.map(members, fn %{id: member_id, partition: partition} ->
        member_plugin = Plugin.for_partition(%{plugin | id: plugin_id}, member_id, partition)
        {member_id, do_enable_plugin(member_plugin.id, member_plugin)}
      end)

    case Enum.reject(results, &match?({_id, {:ok, _}}, &1)) do
      [] ->
        {:ok, group_enable_summary(results)}

      # Nothing was enabled, so nothing is partial: the members' own refusal
      # says why.
      [{_member, refused} | _] = failed when length(failed) == length(results) ->
        refused

      failed ->
        {:error,
         {:group_enable_partial, length(results) - length(failed), length(results),
          Enum.map(failed, &elem(&1, 0))}}
    end
  end

  # An already-enabled member is success, reported distinctly; a group whose
  # members all already carry the plugin degenerates to the single-plugin
  # no-op verdict.
  defp group_enable_summary(results) do
    case Enum.split_with(results, &match?({_id, {:ok, :already_enabled}}, &1)) do
      {[], enabled} ->
        "plugin enabled on #{length(enabled)} partitions"

      {_already, []} ->
        :already_enabled

      {already, enabled} ->
        "plugin enabled on #{length(enabled)} partition(s) " <>
          "(#{length(already)} already enabled)"
    end
  end

  defp refuse_init_subscription(%Plugin{hooks: subs}) do
    if Enum.any?(subs, &match?({:on_operator_init, _}, &1)),
      do: {:error, :init_plugin_after_start},
      else: :ok
  end

  @doc """
  Disable `plugin_id` wherever it is declared: the operator whose plugin list
  holds it, or, for an id a group enable fanned out as `<plugin_id>#p0` …, every
  current member of that group (`detach/3`). The target is read from the
  namespaces' registry rows, never from this node's table, so a plugin on an
  operator that moved to another controller, or on a member a rescale added, is
  disabled from any node. Only an enable still in flight on this node is
  refused.
  """
  @spec disable_plugin_group(binary()) :: {:ok, binary()} | {:error, term()}
  def disable_plugin_group(plugin_id) do
    cond do
      MapSet.member?(pending_ids(), plugin_id) -> {:error, {:plugin_enabling, plugin_id}}
      target = locate(plugin_id) -> detach_located(target)
      true -> {:error, {:plugin_not_found, plugin_id}}
    end
  end

  defp detach_located({namespace, operator_id, plugin_id}) do
    case {detach(namespace, operator_id, plugin_id), targets(namespace, operator_id, plugin_id)} do
      {:ok, [_one]} -> {:ok, "plugin disabled"}
      {:ok, members} -> {:ok, "plugin disabled on #{length(members)} partitions"}
      {{:error, _reason} = error, _} -> error
    end
  end

  @doc """
  Where `plugin_id` on `operator_id` lives, as `{operator_id, plugin_id}`
  pairs: the operator itself, or every current member of the partition group
  it names, each under its `#pK` id, members a rescale added included.
  """
  @spec targets(binary(), binary(), binary()) :: [{binary(), binary()}]
  def targets(namespace, operator_id, plugin_id) do
    case PartitionGroup.members(namespace, operator_id) do
      [] ->
        [{operator_id, plugin_id}]

      members ->
        for %{id: member_id, partition: partition} <- members,
            do: {member_id, plugin_id <> Flezha.Partition.member_suffix(partition.index)}
    end
  end

  @doc """
  Whether `plugin_id` is still on `operator_id`, read from the rows on a node
  that hosts the namespace's hub, which a move keeps and a disable rewrites:
  `:attached` while any target's row carries it, `:detached` when the targets
  are registered without it, `:operator_ended` when none is registered or
  declared, and `:unknown` when the rows cannot be read or a declared target has
  no row yet.
  """
  @spec attachment(binary(), binary(), binary()) ::
          :attached | :detached | :operator_ended | :unknown
  def attachment(namespace, operator_id, plugin_id) do
    on_hub_node(
      namespace,
      :unknown,
      fn ->
        declared = declared_ids(namespace)

        namespace
        |> targets(operator_id, plugin_id)
        |> Enum.map(fn {rid, id} -> row_attachment(namespace, rid, id, declared) end)
        |> Enum.reduce(&stronger_attachment/2)
      end,
      {__MODULE__, :attachment, [namespace, operator_id, plugin_id]}
    )
  end

  # A declared operator with no row is between two homes, a move or a rescale
  # away from one, and not ended: only an operator the declared set no longer
  # names, nor any member of the group it names, has ended.
  defp row_attachment(namespace, operator_id, plugin_id, declared) do
    case HubManager.operator_plugins(namespace, operator_id) do
      {:ok, plugins} ->
        if Enum.any?(plugins, &match?({^plugin_id, _}, &1)), do: :attached, else: :detached

      {:error, {:operator_not_found, _, _}} ->
        if declared?(declared, operator_id), do: :unknown, else: :operator_ended

      {:error, _hub_down} ->
        :unknown
    end
  end

  defp declared_ids(namespace),
    do: namespace |> HubManager.declared_operators() |> MapSet.new(& &1.id)

  defp declared?(declared, operator_id) do
    MapSet.member?(declared, operator_id) or
      Enum.any?(declared, &match?({^operator_id, _}, Flezha.Partition.split_member_suffix(&1)))
  end

  # One member still carrying the row is enough to go on.
  defp stronger_attachment(a, b),
    do: Enum.find([:attached, :unknown, :detached, :operator_ended], &(&1 in [a, b]))

  @doc """
  Remove `plugin_id` from `operator_id`, or from every current member of the
  partition group `operator_id` names (each under its `#pK` id): the durable
  row and declared set first, then the running Overwatch; any entry the
  caller's node and the hub node hold for those ids goes with it. Addressed by target, so it holds whichever
  node ran the enable and wherever the operator runs now; a listener and the
  heartbeat of a listener's plugin detach this way.
  """
  @spec detach(binary(), binary(), binary()) :: :ok | {:error, term()}
  def detach(namespace, operator_id, plugin_id) do
    targets = targets(namespace, operator_id, plugin_id)
    GenServer.cast(__MODULE__, {:unregister_plugins, Enum.map(targets, &elem(&1, 1))})

    on_hub_node(
      namespace,
      {:error, :registry_unavailable},
      fn -> detach_here(namespace, operator_id, plugin_id) end,
      {__MODULE__, :detach, [namespace, operator_id, plugin_id]}
    )
  end

  defp detach_here(namespace, operator_id, plugin_id) do
    targets = targets(namespace, operator_id, plugin_id)
    results = Parallel.map(targets, fn {rid, id} -> {id, detach_one(namespace, rid, id)} end)

    case {targets, Enum.reject(results, &match?({_id, :ok}, &1))} do
      {_, []} -> :ok
      {[_one], [{_id, error}]} -> error
      {_, failed} -> {:error, {:group_disable_partial, Enum.map(failed, &elem(&1, 0))}}
    end
  end

  defp detach_one(namespace, operator_id, plugin_id) do
    case write_disable_to_cspec(namespace, operator_id, plugin_id) do
      :ok ->
        _ = unsubscribe(namespace, operator_id, plugin_id)
        :ok

      {:error, {:operator_not_found, _namespace, _operator_id}} ->
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  # `{namespace, operator_id, plugin_id}` for the operator whose plugin list
  # holds `plugin_id`, or `{namespace, group_id, plugin_id}` when only a group's
  # fanned-out `<plugin_id>#pK` ids are found; `nil` when no namespace holds it.
  defp locate(plugin_id) do
    Enum.find_value(Directory.namespaces(), fn namespace ->
      rows = declared_plugins(namespace)

      Enum.find_value(rows, fn {operator_id, ids} ->
        if plugin_id in ids, do: {namespace, operator_id, plugin_id}
      end) ||
        Enum.find_value(rows, fn {operator_id, ids} ->
          if Enum.any?(ids, &fanned_out_from?(&1, plugin_id)),
            do: {namespace, group_of(operator_id), plugin_id}
        end)
    end)
  end

  defp group_of(member_id) do
    case Flezha.Partition.split_member_suffix(member_id) do
      {group_id, _index} -> group_id
      :error -> member_id
    end
  end

  # Every operator's plugin ids in `namespace`, read on a node that hosts its
  # hub: this one when it does, otherwise one that does, under a bound.
  defp declared_plugins(namespace) do
    on_hub_node(
      namespace,
      [],
      fn -> plugin_ids(namespace) end,
      {__MODULE__, :plugin_ids, [namespace]}
    )
  end

  @doc false
  # Run on a node hosting the namespace's hub: the registry rows, and the
  # declared set that holds a moving operator's plugins while its row is between
  # its two homes.
  @spec plugin_ids(binary()) :: [{binary(), [binary()]}]
  def plugin_ids(namespace) do
    for {operator_id, plugins} <-
          HubManager.operator_plugins(namespace) ++ HubManager.declared_plugins(namespace),
        do: {operator_id, Enum.map(plugins, &plugin_id/1)}
  end

  # A namespace's rows are written and read on a node that hosts its hub: this
  # one when it does, otherwise one that does, under a bound; `none` when no
  # node hosts it or the one asked does not answer.
  defp on_hub_node(namespace, none, here, {module, function, args}) do
    case Directory.hub_nodes(namespace) do
      [] ->
        none

      hub_nodes ->
        if node() in hub_nodes,
          do: here.(),
          else: remote(hd(hub_nodes), module, function, args, none)
    end
  end

  defp remote(hub_node, module, function, args, none) do
    :erpc.call(hub_node, module, function, args, Durations.get(:route_call_timeout_ms))
  catch
    _kind, _reason -> none
  end

  defp plugin_id({id, %Plugin{}}), do: id
  defp plugin_id(%Plugin{id: id}), do: id

  def disable_operator_plugins(namespace, operator_id)
      when is_binary(namespace) and is_binary(operator_id) do
    call_service({:disable_operator_plugins, namespace, operator_id})
  end

  @doc """
  Disable every plugin of `operator_ids`, in ONE request.

  A caller that walked the ids itself paid the per-operator `Overwatch`
  unsubscribe bound M times over: every item funnels into this single
  registered manager, which serves one message at a time, so a caller-side
  fan-out only fills its mailbox and the waits still run end to end. The
  overlap has to be created where the serialisation is — here — so the
  mailbox is occupied once and M unreachable operators cost one bound, not M.

  Answers `:ok`, or `{:error, {:undisabled, plugin_ids}}` naming the ones that
  could not be unsubscribed. Each is named and stepped over, never a reason to
  abandon the operators behind it.
  """
  @spec disable_operators(binary(), [binary()]) :: :ok | {:error, {:undisabled, [binary()]}}
  def disable_operators(namespace, operator_ids)
      when is_binary(namespace) and is_list(operator_ids) do
    call_service({:disable_operators, namespace, operator_ids})
  end

  def disable_namespace_plugins(namespace) when is_binary(namespace) do
    call_service({:disable_namespace_plugins, namespace})
  end

  # The `:namespace` walk's plugins step: every registration of the namespace
  # is dropped — the operators are gone with the hub, and the registry is the
  # only thing that survives without explicit cleanup.
  @impl Flezha.Shutdown.Step
  def shutdown_steps do
    [%{scope: :namespace, order: 30, name: :plugins, run: &plugins_step/2}]
  end

  defp plugins_step(%{namespace: namespace}, _budget) do
    _ = disable_namespace_plugins(to_string(namespace))
    :ok
  end

  def register_plugin(plugin_id, info) do
    GenServer.cast(__MODULE__, {:register_plugin, plugin_id, info})
  end

  def unregister_plugin(plugin_id) do
    GenServer.cast(__MODULE__, {:unregister_plugin, plugin_id})
  end

  def get_plugins do
    GenServer.call(__MODULE__, :get_plugins)
  end

  @doc "The plugin ids whose enable is in flight: claimed, cspec written, not yet subscribed."
  @spec pending_ids() :: MapSet.t()
  def pending_ids do
    GenServer.call(__MODULE__, :pending_ids)
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  ## GenServer

  @impl GenServer
  def init(:ok) do
    {:ok, %{}}
  end

  # An entry is `{namespace, operator_id}` once enabled and
  # `{namespace, operator_id, :pending}` while its enable is in flight.
  @impl true
  def handle_call(:get_plugins, _from, state) do
    {:reply, Map.reject(state, fn {_id, entry} -> pending?(entry) end), state}
  end

  @impl true
  def handle_call(:pending_ids, _from, state) do
    pending = for {id, entry} <- state, pending?(entry), into: MapSet.new(), do: id
    {:reply, pending, state}
  end

  @impl true
  def handle_call({:claim, plugin_id, %Plugin{} = plugin}, _from, state) do
    case Map.get(state, plugin_id) do
      # Idempotent re-enable: a presence check, not config convergence — the
      # live instance, its workers and its cspec row are all left untouched.
      {ns, rid} when ns == plugin.namespace and rid == plugin.operator_id ->
        {:reply, {:ok, :already_enabled}, state}

      {ns, rid, :pending} when ns == plugin.namespace and rid == plugin.operator_id ->
        {:reply, {:error, {:plugin_enabling, plugin_id}}, state}

      nil ->
        {:reply, :claimed,
         Map.put(state, plugin_id, {plugin.namespace, plugin.operator_id, :pending})}

      entry ->
        {ns, rid} = target(entry)
        {:reply, {:error, {:plugin_id_taken, plugin_id, ns, rid}}, state}
    end
  end

  # The namespace is going away wholesale, so there is nothing to unsubscribe
  # from and no cspec left to write: the entries are simply dropped.
  def handle_call({:disable_namespace_plugins, namespace}, _from, state) do
    ids = for {id, _ns, _rid} <- targets(state, fn ns, _rid -> ns == namespace end), do: id
    {:reply, :ok, Map.drop(state, ids)}
  end

  def handle_call({:disable_operator_plugins, namespace, operator_id}, _from, state) do
    disable_matching(state, fn ns, rid -> ns == namespace and rid == operator_id end)
  end

  def handle_call({:disable_operators, namespace, operator_ids}, _from, state) do
    ids = MapSet.new(operator_ids)
    disable_matching(state, fn ns, rid -> ns == namespace and MapSet.member?(ids, rid) end)
  end

  # Every matching registration is disabled concurrently, so the per-operator
  # `Overwatch` unsubscribe waits overlap inside this one turn. The work is a
  # cspec write and a bounded call and nothing else — it creates no ETS table,
  # unlinks nothing and seeds no route — which is what makes it safe to run off
  # this process.
  defp disable_matching(state, match?) do
    outcomes =
      state
      |> targets(match?)
      |> Parallel.results(
        fn {plugin_id, ns, rid} ->
          _ = write_disable_to_cspec(ns, rid, plugin_id)
          unsubscribe(ns, rid, plugin_id)
        end,
        timeout: Durations.get(:plugin_disable_fanout_ms)
      )

    {new_state, undisabled} =
      Enum.reduce(outcomes, {state, []}, fn {{plugin_id, _ns, _rid}, outcome}, {acc, failed} ->
        failed = if outcome == {:ok, :ok}, do: failed, else: [plugin_id | failed]
        {Map.delete(acc, plugin_id), failed}
      end)

    reply =
      case Enum.reverse(undisabled) do
        [] -> :ok
        ids -> {:error, {:undisabled, ids}}
      end

    {:reply, reply, new_state}
  end

  @impl true
  def handle_cast({:register_plugin, plugin_id, info}, state) do
    {:noreply, Map.put(state, plugin_id, info)}
  end

  @impl true
  def handle_cast({:confirm, plugin_id}, state) do
    {:noreply,
     Map.update(state, plugin_id, nil, fn
       {ns, rid, :pending} -> {ns, rid}
       entry -> entry
     end)}
  end

  @impl true
  def handle_cast({:unregister_plugin, plugin_id}, state) do
    {:noreply, Map.delete(state, plugin_id)}
  end

  def handle_cast({:unregister_plugins, plugin_ids}, state) do
    {:noreply, Map.drop(state, plugin_ids)}
  end

  ## Private

  defp pending?({_ns, _rid, :pending}), do: true
  defp pending?(_entry), do: false

  # Every enabled plugin whose target satisfies `match?`, as
  # `{plugin_id, namespace, operator_id}`. A pending entry is a three-tuple
  # and matches nothing here, which is what keeps an enable in flight out of
  # every bulk disable.
  defp targets(state, match?) do
    for {id, {ns, rid}} <- state, match?.(ns, rid), do: {id, ns, rid}
  end

  defp target({ns, rid}), do: {ns, rid}
  defp target({ns, rid, :pending}), do: {ns, rid}

  # `id` is `base` with a partition suffix — `ohlc_ws#p2` for base `ohlc_ws`.
  defp fanned_out_from?(id, base),
    do: match?({^base, _index}, Flezha.Partition.split_member_suffix(id))

  defp check_operator_target(%Plugin{namespace: ns, operator_id: rid})
       when is_binary(ns) and is_binary(rid),
       do: :ok

  defp check_operator_target(%Plugin{namespace: ns, operator_id: rid}),
    do: {:error, {:plugin_target_missing, ns, rid}}

  defp resolve_operator(%Plugin{namespace: ns, operator_id: pid_id})
       when is_binary(ns) and is_binary(pid_id) do
    OperatorManager.get_operator_pid(ns, pid_id)
  end

  # A bounded call into the manager itself, so a caller tearing a namespace
  # down inherits a busy manager as a value rather than as its own exit.
  defp call_service(request) do
    case SafeCall.bounded(__MODULE__, request, Durations.get(:plugin_service_call_ms)) do
      {:ok, reply} -> reply
      {:error, _} = err -> err
    end
  end

  # The runtime half of a disable. The cspec write has already recorded the
  # intent, so an operator that cannot be reached loses the plugin at
  # its next `Overwatch.init/1`; what this answers is whether the live process
  # dropped it now. An operator that is gone has nothing to drop.
  defp unsubscribe(namespace, operator_id, plugin_id) do
    case OperatorManager.get_operator_pid(namespace, operator_id) do
      {:ok, pid} -> Overwatch.plugin_unsubscribe(pid, plugin_id)
      _ -> :ok
    end
  end

  @doc """
  Persist one provider-committed option onto an enabled plugin's cspec record,
  so a controller restart re-applies the plugin with it. Rides the same
  registry-write path as enable and disable — there is one place a plugin's
  cspec row is rewritten.

  Called from the host operator, off its own loop: the write is a cluster-wide
  registry round-trip with its own retry ladder, and a provider committing state
  must never be able to stall the operator it feeds.
  """
  @spec persist_plugin_opt(binary() | atom(), binary(), binary(), atom(), term()) ::
          :ok | {:error, term()}
  def persist_plugin_opt(namespace, operator_id, plugin_id, key, value) do
    update_cspec_plugins(
      namespace,
      operator_id,
      RegistryUpdate.put_plugin_opt(plugin_id, key, value)
    )
  end

  defp write_enable_to_cspec(
         %Plugin{namespace: ns, operator_id: operator_id} = plugin,
         plugin_id
       ) do
    stripped = Plugin.strip_transient(plugin)

    update_cspec_plugins(ns, operator_id, RegistryUpdate.add_plugin(plugin_id, stripped))
  end

  defp write_disable_to_cspec(namespace, operator_id, plugin_id) do
    update_cspec_plugins(namespace, operator_id, RegistryUpdate.remove_plugin(plugin_id))
  end

  # Two records, one mutation, and every plugin write goes through here. The row
  # is what `flz operator info` and the convergence backstop read while the
  # operator runs. The declared set is what a restart rebuilds it from, and it
  # is the ONLY one a restart reads: an auto-recovery hub never replays its
  # registry from disk (`ProcessHub.Initializer.setup_storage/3` forces
  # `recovery_replay: false`, so the live registry starts empty), which is why a
  # plugin written only to the row does not survive a controller restart.
  #
  # The row goes first and is the source the declared write copies from: its
  # transform runs inside the registry, so concurrent writes for one operator
  # merge there atomically rather than in two callers that cannot see each other.
  #
  # Both or neither. A failure at either half answers, no runtime mutation is
  # issued, and `rollback_enable/2` removes from both.
  defp update_cspec_plugins(namespace, operator_id, update_fn) do
    with :ok <- do_update_cspec_plugins(namespace, operator_id, update_fn, 0) do
      OperatorManager.redeclare_plugins(namespace, operator_id)
    end
  end

  defp do_update_cspec_plugins(namespace, operator_id, update_fn, attempt) do
    hub_id = Identificator.hub(namespace)

    cond do
      not Hub.alive?(hub_id) ->
        {:error, :registry_unavailable}

      true ->
        result =
          try do
            # propagate: true — every node's row must carry the plugin, because
            # `flz operator info` and the convergence backstop read it from
            # whichever node answers. It is NOT what survives a restart: this
            # write reaches the registry row only, and no restart reads a row
            # back. The declared set is written separately, by the caller.
            ProcessRegistry.update(
              hub_id,
              operator_id,
              RegistryUpdate.rewrite_cspec_plugins(update_fn),
              propagate: true
            )
          catch
            :exit, _ -> {:error, :no_response}
          end

        case result do
          :ok ->
            :ok

          {:error, :no_response} ->
            retry_or_give_up(namespace, operator_id, update_fn, attempt, result)

          # The operator row may live on a peer node whose pubsub-based
          # synchronization hasn't fanned out to us yet — retry briefly
          # before surfacing the error.
          {:error, @no_child} = no_child ->
            retry_or_give_up(namespace, operator_id, update_fn, attempt, no_child)

          {:error, :not_found} ->
            {:error, {:operator_not_found, namespace, operator_id}}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  # `:no_response` (the local hub's `dist_sup` was momentarily missing, so
  # `execute/2` short-circuited without replying) and "No child found" (a peer
  # node's row has not fanned out yet) are one condition — the hub has not
  # settled — so they walk one ladder; only the answer they give up with
  # differs.
  defp retry_or_give_up(namespace, operator_id, update_fn, attempt, result) do
    if Backoff.give_up?(:no_response, attempt) do
      exhausted(result, namespace, operator_id)
    else
      Backoff.wait(:no_response, attempt + 1, operator_id)
      do_update_cspec_plugins(namespace, operator_id, update_fn, attempt + 1)
    end
  end

  defp exhausted({:error, @no_child}, namespace, operator_id),
    do: {:error, {:operator_not_found, namespace, operator_id}}

  defp exhausted(error, _namespace, _operator_id), do: error

  # ---------- Published surface ----------
  #
  # The context-taking half a consumer is allowed to call. Each function binds
  # the row `flz plugin` binds for the same verb, judged in the plugin's own
  # namespace, and answers data. Everything above stays internal.

  @doc "Every enabled plugin, by id."
  @spec list(CallerContext.t()) :: {:ok, map()} | {:error, term()}
  verb list(caller_context), verb: :plugin_list do
    {:ok, get_plugins()}
  end

  @doc """
  Enable a plugin built from `fields`.

  The plugin is constructed here rather than by the caller, so a consumer never
  has to name `Flezha.Plugin` or put a struct on the wire. `fields` needs at
  least `:id` and `:provider`; its `:namespace` is the scope the enable is
  authorized in, defaulting to `"default"` as the CLI's does.
  """
  @spec enable(String.t(), keyword(), CallerContext.t()) :: term() | {:error, term()}
  verb enable(plugin_id, fields, caller_context) when is_binary(plugin_id) and is_list(fields),
    verb: :plugin_enable,
    row: &fields_row/1 do
    with {:ok, plugin} <- Plugin.from_fields(fields), do: enable_plugin(plugin_id, plugin)
  end

  @doc """
  Disable a plugin — or, when the id was fanned out over a partition group,
  every `#pK` instance of it.
  """
  @spec disable(String.t(), CallerContext.t()) :: term() | {:error, term()}
  verb disable(plugin_id, caller_context) when is_binary(plugin_id),
    verb: :plugin_disable,
    row: &plugin_row/1 do
    disable_plugin_group(plugin_id)
  end

  @doc """
  The namespace a plugin is declared in, or `"_system"` when none declares it.

  The scope a plugin verb is judged in; shared with `Flezha.Service.Cli.Plugin`
  so the two callers cannot disagree about it.
  """
  @spec target_namespace(String.t()) :: String.t()
  def target_namespace(plugin_id) when is_binary(plugin_id) do
    case locate(plugin_id) do
      {namespace, _operator_id, _plugin_id} -> namespace
      nil -> Namespace.system_name()
    end
  end

  defp fields_row([_plugin_id, fields]),
    do: {:ok, %{namespace: Keyword.get(fields, :namespace, "default")}}

  defp plugin_row([plugin_id]), do: {:ok, %{namespace: target_namespace(plugin_id)}}
end
