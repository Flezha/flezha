defmodule Flezha.Service.PluginManager do
  @moduledoc """
  Per-node registry of enabled `%Flezha.Plugin{}` entries. Routes
  `enable_plugin/2` into the owning relay's Overwatch via the unified
  `{:plugin_subscribe, ...}` / `{:plugin_unsubscribe, ...}` messages and
  persists the subscription into the relay's ProcessHub child-spec
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

  require Logger

  alias Flezha.Plugin
  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.RelayManager
  alias ProcessHub.Service.ProcessRegistry

  @no_response_retry_attempts 8
  @no_response_retry_initial_ms 150

  ## Public API

  def enable_plugin(plugin_id, %Plugin{} = plugin) do
    cond do
      has_init_subscription?(plugin) ->
        {:error, "init-state plugins must be attached at relay start"}

      true ->
        case GenServer.call(__MODULE__, {:enable_plugin, plugin_id, plugin}) do
          {:ok, msg} -> {:ok, msg}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp has_init_subscription?(%Plugin{subscriptions: subs}) when is_list(subs) do
    Enum.any?(subs, fn
      {:on_relay_init, _} -> true
      :on_relay_init -> true
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

  def disable_relay_plugins(namespace, relay_id)
      when is_binary(namespace) and is_binary(relay_id) do
    GenServer.call(__MODULE__, {:disable_relay_plugins, namespace, relay_id})
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

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  ## GenServer

  @impl GenServer
  def init(:ok) do
    {:ok, %{}}
  end

  @impl true
  def handle_call(:get_plugins, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:enable_plugin, plugin_id, %Plugin{} = plugin}, _from, state) do
    {reply, new_state} =
      with :ok <- plugin_id_unique(state, plugin_id),
           :ok <- check_relay_target(plugin),
           :ok <- write_enable_to_cspec(plugin, plugin_id),
           {:ok, pid} <- resolve_relay(plugin),
           :ok <- forward_subscribe(pid, plugin_id, plugin) do
        {{:ok, "plugin enabled"}, Map.put(state, plugin_id, {plugin.namespace, plugin.relay_id})}
      else
        err -> {err, state}
      end

    {:reply, reply, new_state}
  end

  def handle_call({:disable_plugin, plugin_id}, _from, state) do
    case Map.get(state, plugin_id) do
      nil ->
        {:reply, {:error, "plugin not found"}, state}

      {namespace, relay_id} ->
        case write_disable_to_cspec(namespace, relay_id, plugin_id) do
          :ok ->
            case RelayManager.get_relay_pid(namespace, relay_id) do
              {:ok, pid} ->
                _ = GenServer.call(pid, {:plugin_unsubscribe, plugin_id})
                {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}

              _ ->
                {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}
            end

          {:error, :relay_not_found} ->
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

  def handle_call({:disable_relay_plugins, namespace, relay_id}, _from, state) do
    matching =
      state
      |> Enum.filter(fn
        {_id, {ns, rid}} -> ns == namespace and rid == relay_id
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

            case RelayManager.get_relay_pid(ns, rid) do
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
  def handle_cast({:unregister_plugin, plugin_id}, state) do
    {:noreply, Map.delete(state, plugin_id)}
  end

  ## Private

  defp plugin_id_unique(plugins, plugin_id) do
    case Map.get(plugins, plugin_id, nil) do
      nil -> :ok
      _ -> {:error, "plugin already registered"}
    end
  end

  defp check_relay_target(%Plugin{namespace: ns, relay_id: rid})
       when is_binary(ns) and is_binary(rid),
       do: :ok

  defp check_relay_target(%Plugin{namespace: ns, relay_id: rid}) do
    {:error,
     "plugin missing namespace or relay_id (got namespace=#{inspect(ns)}, relay_id=#{inspect(rid)})"}
  end

  defp resolve_relay(%Plugin{namespace: ns, relay_id: pid_id})
       when is_binary(ns) and is_binary(pid_id) do
    RelayManager.get_relay_pid(ns, pid_id)
  end

  defp forward_subscribe(pid, plugin_id, plugin) do
    case GenServer.call(pid, {:plugin_subscribe, plugin_id, plugin}) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp write_enable_to_cspec(%Plugin{namespace: ns, relay_id: relay_id} = plugin, plugin_id) do
    stripped = Plugin.strip_transient(plugin)

    update_cspec_plugins(ns, relay_id, fn plugins ->
      plugins
      |> Enum.reject(fn {id, _} -> id == plugin_id end)
      |> Kernel.++([{plugin_id, stripped}])
    end)
  end

  defp write_disable_to_cspec(namespace, relay_id, plugin_id) do
    update_cspec_plugins(namespace, relay_id, fn plugins ->
      Enum.reject(plugins, fn {id, _plugin} -> id == plugin_id end)
    end)
  end

  defp update_cspec_plugins(namespace, relay_id, update_fn) do
    do_update_cspec_plugins(namespace, relay_id, update_fn, 0)
  end

  defp do_update_cspec_plugins(namespace, relay_id, update_fn, attempt) do
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
              relay_id,
              fn cspec, node_pids, metadata ->
                {HubManager.rewrite_cspec_plugins(cspec, update_fn), node_pids, metadata}
              end,
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
            do_update_cspec_plugins(namespace, relay_id, update_fn, attempt + 1)

          # The relay row may live on a peer node whose pubsub-based
          # synchronization hasn't fanned out to us yet — retry briefly
          # before surfacing the error.
          {:error, "No child found"} when attempt < @no_response_retry_attempts ->
            backoff = min(@no_response_retry_initial_ms * Bitwise.bsl(1, attempt), 5_000)
            Process.sleep(backoff)
            do_update_cspec_plugins(namespace, relay_id, update_fn, attempt + 1)

          {:error, :not_found} ->
            {:error, :relay_not_found}

          {:error, "No child found"} ->
            {:error, :relay_not_found}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end
end
