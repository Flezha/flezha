defmodule Flezha.Service.PluginManager do
  @moduledoc """
  Per-node registry of enabled `%Flezha.Plugin{}` entries. Routes
  `enable_plugin/2` into the owning operative's Overwatch and persists the
  subscription into that operative's ProcessHub child-spec, so it survives an
  Overwatch restart, a controller restart and a cluster cold boot.

  Ordering is canonicalise, then cspec write, then runtime mutation: an
  ill-typed or unknown subscription is refused before any durable record
  exists, so the convergence loop is never handed intent that cannot be
  satisfied. A failed cspec write returns `{:error, :registry_unavailable}`
  and touches no runtime, so the caller may retry; a failed runtime call
  returns its own error with the durable record in place, and the next
  `Overwatch.init/1` reconciles it.

  Every call this manager makes into an operative it does not own carries a
  bound, because the manager is a node-wide singleton whose table starts empty:
  an exit taken from a busy Overwatch would forget every plugin on the node. A
  disable that could not reach its operative names the plugins it could not drop.
  """

  use GenServer

  @behaviour Flezha.Shutdown.Step
  use Flezha.AuthZ.Verb

  alias Flezha.AuthZ.Namespace
  alias Flezha.Config.Durations
  alias Flezha.Config.Backoff
  alias Flezha.Identificator
  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.SafeCall
  alias Flezha.Service.CallerContext
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.RegistryUpdate
  alias ProcessHub.Service.ProcessRegistry

  # This manager is a node-wide singleton and its table does not survive it:
  # `init/1` starts empty, so an exit here forgets every plugin enabled on the
  # node, not just the one being disabled. The calls it makes into an operative
  # carry `Overwatch`'s own bound, which is the smaller of the two so the
  # manager answers before the caller waiting on it stops waiting.

  ## Public API

  # Canonicalised here, before anything reads the subscriptions and before any
  # durable write: the declaration-only half of validation needs no live table,
  # only the host catalog. A refusal therefore costs nothing and leaves nothing,
  # and `has_init_subscription?/1` below matches a name the operator typed as
  # text. What is persisted is the canonical struct.
  def enable_plugin(plugin_id, %Plugin{} = plugin) do
    with {:ok, plugin} <- Table.canonicalise(Overwatch.__extensions_catalog__(), plugin),
         :ok <- refuse_init_subscription(plugin) do
      case PartitionGroup.members(plugin.namespace, plugin.operative_id) do
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
    with :ok <- check_operative_target(plugin),
         :claimed <- GenServer.call(__MODULE__, {:claim, plugin_id, plugin}) do
      enable_claimed(plugin_id, plugin)
    end
  end

  defp enable_claimed(plugin_id, plugin) do
    with :ok <- write_enable_to_cspec(plugin, plugin_id),
         {:ok, pid} <- resolve_operative(plugin),
         :ok <- Overwatch.plugin_subscribe(pid, plugin_id, plugin) do
      GenServer.cast(__MODULE__, {:confirm, plugin_id})
      {:ok, "plugin enabled"}
    else
      err ->
        unregister_plugin(plugin_id)
        err
    end
  end

  # A group id names no operative — it names N of them. Enable runs once per
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

      failed ->
        {:error,
         "enabled on #{length(results) - length(failed)} of #{length(results)} partitions; " <>
           Enum.map_join(failed, "; ", fn {id, err} -> "#{id}: #{inspect(err)}" end)}
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

  defp refuse_init_subscription(%Plugin{subscriptions: subs}) do
    if Enum.any?(subs, &match?({:on_operative_init, _}, &1)),
      do: {:error, "init-state plugins must be attached at operative start"},
      else: :ok
  end

  def disable_plugin(plugin_id) do
    case GenServer.call(__MODULE__, {:disable_plugin, plugin_id}) do
      {:ok, msg} -> {:ok, msg}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Disable every plugin instance a group enable fanned out — `<plugin_id>#p0`
  … — or the plain `plugin_id` when it names an ordinary plugin.
  """
  @spec disable_plugin_group(binary()) :: {:ok, binary()} | {:error, term()}
  def disable_plugin_group(plugin_id) do
    case GenServer.call(__MODULE__, {:fanned_out_ids, plugin_id}) do
      [] ->
        disable_plugin(plugin_id)

      ids ->
        results = Enum.map(ids, &{&1, disable_plugin(&1)})

        case Enum.reject(results, &match?({_id, {:ok, _}}, &1)) do
          [] ->
            {:ok, "plugin disabled on #{length(results)} partitions"}

          failed ->
            {:error, Enum.map_join(failed, "; ", fn {id, err} -> "#{id}: #{inspect(err)}" end)}
        end
    end
  end

  def disable_operative_plugins(namespace, operative_id)
      when is_binary(namespace) and is_binary(operative_id) do
    call_service({:disable_operative_plugins, namespace, operative_id})
  end

  def disable_namespace_plugins(namespace) when is_binary(namespace) do
    call_service({:disable_namespace_plugins, namespace})
  end

  # The `:namespace` walk's plugins step: every registration of the namespace
  # is dropped — the operatives are gone with the hub, and the registry is the
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

  # An entry is `{namespace, operative_id}` once enabled and
  # `{namespace, operative_id, :pending}` while its enable is in flight.
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
      {ns, rid} when ns == plugin.namespace and rid == plugin.operative_id ->
        {:reply, {:ok, :already_enabled}, state}

      {ns, rid, :pending} when ns == plugin.namespace and rid == plugin.operative_id ->
        {:reply, {:error, "plugin #{plugin_id} is already being enabled"}, state}

      nil ->
        {:reply, :claimed,
         Map.put(state, plugin_id, {plugin.namespace, plugin.operative_id, :pending})}

      entry ->
        {ns, rid} = target(entry)
        {:reply, {:error, "plugin id #{plugin_id} is already registered for #{ns}/#{rid}"}, state}
    end
  end

  def handle_call({:fanned_out_ids, plugin_id}, _from, state) do
    ids =
      if Map.has_key?(state, plugin_id) do
        []
      else
        state
        |> Map.keys()
        |> Enum.filter(&fanned_out_from?(&1, plugin_id))
        |> Enum.sort()
      end

    {:reply, ids, state}
  end

  def handle_call({:disable_plugin, plugin_id}, _from, state) do
    case Map.get(state, plugin_id) do
      nil ->
        {:reply, {:error, "plugin not found"}, state}

      {_ns, _rid, :pending} ->
        {:reply, {:error, "plugin #{plugin_id} is still being enabled"}, state}

      {namespace, operative_id} ->
        case write_disable_to_cspec(namespace, operative_id, plugin_id) do
          :ok ->
            _ = unsubscribe(namespace, operative_id, plugin_id)
            {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}

          {:error, :operative_not_found} ->
            {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  # The namespace is going away wholesale, so there is nothing to unsubscribe
  # from and no cspec left to write: the entries are simply dropped.
  def handle_call({:disable_namespace_plugins, namespace}, _from, state) do
    ids = for {id, _ns, _rid} <- targets(state, fn ns, _rid -> ns == namespace end), do: id
    {:reply, :ok, Map.drop(state, ids)}
  end

  def handle_call({:disable_operative_plugins, namespace, operative_id}, _from, state) do
    matching = targets(state, fn ns, rid -> ns == namespace and rid == operative_id end)

    {new_state, undisabled} =
      Enum.reduce(matching, {state, []}, fn {plugin_id, ns, rid}, {acc, failed} ->
        _ = write_disable_to_cspec(ns, rid, plugin_id)

        failed =
          case unsubscribe(ns, rid, plugin_id) do
            :ok -> failed
            {:error, _} -> [plugin_id | failed]
          end

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

  ## Private

  defp pending?({_ns, _rid, :pending}), do: true
  defp pending?(_entry), do: false

  # Every enabled plugin whose target satisfies `match?`, as
  # `{plugin_id, namespace, operative_id}`. A pending entry is a three-tuple
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

  defp check_operative_target(%Plugin{namespace: ns, operative_id: rid})
       when is_binary(ns) and is_binary(rid),
       do: :ok

  defp check_operative_target(%Plugin{namespace: ns, operative_id: rid}) do
    {:error,
     "plugin missing namespace or operative_id (got namespace=#{inspect(ns)}, operative_id=#{inspect(rid)})"}
  end

  defp resolve_operative(%Plugin{namespace: ns, operative_id: pid_id})
       when is_binary(ns) and is_binary(pid_id) do
    OperativeManager.get_operative_pid(ns, pid_id)
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
  # intent, so an operative that cannot be reached loses the subscription at
  # its next `Overwatch.init/1`; what this answers is whether the live process
  # dropped it now. An operative that is gone has nothing to drop.
  defp unsubscribe(namespace, operative_id, plugin_id) do
    case OperativeManager.get_operative_pid(namespace, operative_id) do
      {:ok, pid} -> Overwatch.plugin_unsubscribe(pid, plugin_id)
      _ -> :ok
    end
  end

  @doc """
  Persist one provider-committed option onto an enabled plugin's cspec record,
  so a controller restart re-applies the plugin with it. Rides the same
  registry-write path as enable and disable — there is one place a plugin's
  cspec row is rewritten.

  Called from the host operative, off its own loop: the write is a cluster-wide
  registry round-trip with its own retry ladder, and a provider committing state
  must never be able to stall the operative it feeds.
  """
  @spec persist_plugin_opt(binary() | atom(), binary(), binary(), atom(), term()) ::
          :ok | {:error, term()}
  def persist_plugin_opt(namespace, operative_id, plugin_id, key, value) do
    update_cspec_plugins(
      namespace,
      operative_id,
      RegistryUpdate.put_plugin_opt(plugin_id, key, value)
    )
  end

  defp write_enable_to_cspec(
         %Plugin{namespace: ns, operative_id: operative_id} = plugin,
         plugin_id
       ) do
    stripped = Plugin.strip_transient(plugin)

    update_cspec_plugins(ns, operative_id, RegistryUpdate.add_plugin(plugin_id, stripped))
  end

  defp write_disable_to_cspec(namespace, operative_id, plugin_id) do
    update_cspec_plugins(namespace, operative_id, RegistryUpdate.remove_plugin(plugin_id))
  end

  defp update_cspec_plugins(namespace, operative_id, update_fn) do
    do_update_cspec_plugins(namespace, operative_id, update_fn, 0)
  end

  defp do_update_cspec_plugins(namespace, operative_id, update_fn, attempt) do
    hub_id = Identificator.hub(namespace)

    cond do
      not ProcessHub.is_alive?(hub_id) ->
        {:error, :registry_unavailable}

      true ->
        result =
          try do
            # propagate: true — the manifest must be declared on every hub
            # node so a restart driven from any node's registry restores it.
            ProcessRegistry.update(
              hub_id,
              operative_id,
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
            retry_or_give_up(namespace, operative_id, update_fn, attempt, result)

          # The operative row may live on a peer node whose pubsub-based
          # synchronization hasn't fanned out to us yet — retry briefly
          # before surfacing the error.
          {:error, "No child found"} = no_child ->
            retry_or_give_up(namespace, operative_id, update_fn, attempt, no_child)

          {:error, :not_found} ->
            {:error, :operative_not_found}

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
  defp retry_or_give_up(namespace, operative_id, update_fn, attempt, result) do
    if Backoff.give_up?(:no_response, attempt) do
      exhausted(result)
    else
      Backoff.wait(:no_response, attempt + 1, operative_id)
      do_update_cspec_plugins(namespace, operative_id, update_fn, attempt + 1)
    end
  end

  defp exhausted({:error, "No child found"}), do: {:error, :operative_not_found}
  defp exhausted(error), do: error

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
    enable_plugin(plugin_id, Plugin.new(fields))
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
  The namespace a plugin is enabled in, or `"_system"` when it is not enabled.

  The scope a plugin verb is judged in; shared with `Flezha.Service.Cli.Plugin`
  so the two callers cannot disagree about it.
  """
  @spec target_namespace(String.t()) :: String.t()
  def target_namespace(plugin_id) when is_binary(plugin_id) do
    case Map.get(get_plugins(), plugin_id) do
      {namespace, _operative_id} when is_binary(namespace) -> namespace
      _other -> Namespace.system_name()
    end
  end

  defp fields_row([_plugin_id, fields]),
    do: {:ok, %{namespace: Keyword.get(fields, :namespace, "default")}}

  defp plugin_row([plugin_id]), do: {:ok, %{namespace: target_namespace(plugin_id)}}
end
