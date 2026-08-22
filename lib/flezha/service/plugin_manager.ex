defmodule Flezha.Service.PluginManager do
  @moduledoc """
  Per-node registry of enabled `%Flezha.Plugin{}` entries. Routes
  `enable_plugin/2` into the owning operative's Overwatch via the unified
  `{:plugin_subscribe, ...}` / `{:plugin_unsubscribe, ...}` messages and
  persists the subscription into the operative's ProcessHub child-spec
  (now backed by ProcessHub's durable registry) so it survives any
  subsequent Overwatch (re)start, controller restart, or full cluster
  cold boot.

  Ordering: cspec-write-first, then runtime mutation. A successful
  subscribe is always preceded by a durable record. Failure to write
  the cspec returns `{:error, :registry_unavailable}` without touching
  the runtime — the caller can retry. Failure of the subsequent runtime
  call returns the error directly; the durable record is still in
  place, and the next `Overwatch.init/1` will reconcile.
  """

  use GenServer

  alias Flezha.Plugin
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Service.RegistryUpdate
  alias Flezha.Service.Identificator
  alias Flezha.Service.OperativeManager
  alias ProcessHub.Service.ProcessRegistry

  @no_response_retry_attempts 8
  @no_response_retry_initial_ms 150

  ## Public API

  def enable_plugin(plugin_id, %Plugin{} = plugin) do
    cond do
      has_init_subscription?(plugin) ->
        {:error, "init-state plugins must be attached at operative start"}

      true ->
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
      with :ok <- write_enable_to_cspec(plugin, plugin_id),
           {:ok, pid} <- resolve_operative(plugin),
           :ok <- forward_subscribe(pid, plugin_id, plugin) do
        GenServer.cast(__MODULE__, {:confirm, plugin_id})
        {:ok, "plugin enabled"}
      else
        err ->
          unregister_plugin(plugin_id)
          err
      end
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

  defp has_init_subscription?(%Plugin{subscriptions: subs}) when is_list(subs) do
    Enum.any?(subs, fn
      {:on_operative_init, _} -> true
      :on_operative_init -> true
      _ -> false
    end)
  end

  defp has_init_subscription?(_), do: false

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
    GenServer.call(__MODULE__, {:disable_operative_plugins, namespace, operative_id})
  end

  def disable_namespace_plugins(namespace) when is_binary(namespace) do
    GenServer.call(__MODULE__, {:disable_namespace_plugins, namespace})
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
            case OperativeManager.get_operative_pid(namespace, operative_id) do
              {:ok, pid} ->
                _ = GenServer.call(pid, {:plugin_unsubscribe, plugin_id})
                {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}

              _ ->
                {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}
            end

          {:error, :operative_not_found} ->
            {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:disable_namespace_plugins, namespace}, _from, state) do
    matching =
      state
      |> Enum.filter(fn
        {_id, {ns, _rid}} -> ns == namespace
        _ -> false
      end)
      |> Enum.map(fn {id, _} -> id end)

    new_state =
      Enum.reduce(matching, state, fn plugin_id, acc -> Map.delete(acc, plugin_id) end)

    {:reply, :ok, new_state}
  end

  def handle_call({:disable_operative_plugins, namespace, operative_id}, _from, state) do
    matching =
      state
      |> Enum.filter(fn
        {_id, {ns, rid}} -> ns == namespace and rid == operative_id
        _ -> false
      end)
      |> Enum.map(fn {id, _} -> id end)

    new_state =
      Enum.reduce(matching, state, fn plugin_id, acc ->
        case Map.get(acc, plugin_id) do
          nil ->
            acc

          {ns, rid} ->
            _ = write_disable_to_cspec(ns, rid, plugin_id)

            case OperativeManager.get_operative_pid(ns, rid) do
              {:ok, pid} -> _ = GenServer.call(pid, {:plugin_unsubscribe, plugin_id})
              _ -> :ok
            end

            Map.delete(acc, plugin_id)
        end
      end)

    {:reply, :ok, new_state}
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

  defp forward_subscribe(pid, plugin_id, plugin) do
    case GenServer.call(pid, {:plugin_subscribe, plugin_id, plugin}) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
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

          {:error, :no_response} when attempt < @no_response_retry_attempts ->
            backoff = min(@no_response_retry_initial_ms * Bitwise.bsl(1, attempt), 5_000)
            Process.sleep(backoff)
            do_update_cspec_plugins(namespace, operative_id, update_fn, attempt + 1)

          # The operative row may live on a peer node whose pubsub-based
          # synchronization hasn't fanned out to us yet — retry briefly
          # before surfacing the error.
          {:error, "No child found"} when attempt < @no_response_retry_attempts ->
            backoff = min(@no_response_retry_initial_ms * Bitwise.bsl(1, attempt), 5_000)
            Process.sleep(backoff)
            do_update_cspec_plugins(namespace, operative_id, update_fn, attempt + 1)

          {:error, :not_found} ->
            {:error, :operative_not_found}

          {:error, "No child found"} ->
            {:error, :operative_not_found}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end
end
