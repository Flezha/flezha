defmodule Flezha.Service.PluginManager do
  @moduledoc """
  Per-node registry of enabled `%Flezha.Plugin{}` entries. Routes
  `enable_plugin/2` into the owning relay's Overwatch via the unified
  `{:plugin_subscribe, ...}` / `{:plugin_unsubscribe, ...}` messages and
  persists the subscription into the relay's ProcessHub child-spec
  so it survives any subsequent Overwatch (re)start.

  The cspec is the source of truth for plugin subscriptions; the
  runtime `Overwatch.state.extensions` table is rebuilt from the cspec
  on every `init/1`. See
  `openspec/specs/plugin-subscription-persistence/spec.md` for the
  full contract.
  """

  use GenServer

  require Logger

  alias Flezha.Plugin
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager

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

  # `:on_relay_init` fires exactly once during `Overwatch.init/1`. Late
  # enable via `flz plugin enable` cannot hook it — returning an explicit
  # error keeps executors from silently losing init-state wiring.
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

  @doc """
  Disable every plugin whose `(namespace, relay_id)` tuple matches the
  given pair. Used by `RelayManager.stop_relay/2` and
  `RelayManager.stop_all_relays/1` to drop registry entries that would
  otherwise survive into the next test or relay restart and surface as
  `{:error, "plugin already registered"}` on re-enable.

  Returns `:ok` after attempting to disable each matching plugin
  individually; a single failure does not block subsequent plugins.
  """
  def disable_relay_plugins(namespace, relay_id)
      when is_binary(namespace) and is_binary(relay_id) do
    GenServer.call(__MODULE__, {:disable_relay_plugins, namespace, relay_id})
  end

  @doc """
  Disable every plugin registered against the given namespace, regardless
  of `relay_id`. Used by `Flezha.Executor.FleetManager.stop_namespace/1`
  for namespace teardown — once the namespace is being dropped, every
  plugin keyed against it must drop too or the next test re-using a
  same-named relay collides on `{:error, "plugin already registered"}`.
  """
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
           {:ok, pid} <- resolve_relay(plugin),
           :ok <- forward_subscribe(pid, plugin_id, plugin) do
        # Per design.md Decision 2: subscribe-first, cspec-update-second.
        # If the cspec write fails after a successful subscribe, the
        # runtime is correct but the cspec is stale; log at :error so
        # executors see the divergence and reply :ok (the next plugin op
        # for this relay will canonicalise the cspec).
        persist_enable_to_cspec(plugin, plugin_id)

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
        # Per design.md Decision 2: cspec-update-first, unsubscribe-second
        # for the disable path. If the cspec update fails the unsubscribe
        # SHALL NOT run — surface the error so the caller can retry. If
        # the unsubscribe fails after the cspec has already been
        # rewritten, the runtime extensions table is stale-but-correct
        # (still subscribed); the next Overwatch (re)start re-subscribes
        # only the cspec-listed plugins, dropping the stale entry.
        case HubManager.update_relay_plugins(namespace, relay_id, fn plugins ->
               Enum.reject(plugins, fn {id, _plugin} -> id == plugin_id end)
             end) do
          :ok ->
            case RelayManager.get_relay_pid(namespace, relay_id) do
              {:ok, pid} ->
                _ = GenServer.call(pid, {:plugin_unsubscribe, plugin_id})
                {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}

              _ ->
                # Relay gone — treat as disabled locally.
                {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}
            end

          {:error, "relay not found"} ->
            # Relay gone — treat as disabled locally; cspec already
            # absent so there is nothing to canonicalise.
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
            # Best-effort cleanup: rewrite the cspec, then unsubscribe
            # the runtime extension. We tolerate failures from both —
            # `stop_relay` is the caller's last hook before the relay
            # process exits, so the runtime is about to disappear anyway.
            case HubManager.update_relay_plugins(ns, rid, fn plugins ->
                   Enum.reject(plugins, fn {id, _plugin} -> id == plugin_id end)
                 end) do
              :ok ->
                case RelayManager.get_relay_pid(ns, rid) do
                  {:ok, pid} -> _ = GenServer.call(pid, {:plugin_unsubscribe, plugin_id})
                  _ -> :ok
                end

              _other ->
                :ok
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

  defp resolve_relay(%Plugin{namespace: ns, relay_id: pid_id})
       when is_binary(ns) and is_binary(pid_id) do
    RelayManager.get_relay_pid(ns, pid_id)
  end

  # Plugins with nil namespace/relay_id can reach this path when an
  # executor enables a plugin via the installer CLI without first
  # binding it to a target — surface a structured error instead of
  # letting the call cascade into ProcessHub's storage layer where a
  # nil hub_id raises an ETS lookup ArgumentError.
  defp resolve_relay(%Plugin{namespace: ns, relay_id: pid_id}) do
    {:error,
     "plugin missing namespace or relay_id (got namespace=#{inspect(ns)}, relay_id=#{inspect(pid_id)})"}
  end

  defp forward_subscribe(pid, plugin_id, plugin) do
    case GenServer.call(pid, {:plugin_subscribe, plugin_id, plugin}) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist_enable_to_cspec(%Plugin{namespace: ns, relay_id: pid_id} = plugin, plugin_id)
       when is_binary(ns) and is_binary(pid_id) do
    stripped = Plugin.strip_transient(plugin)

    case HubManager.update_relay_plugins(ns, pid_id, fn plugins ->
           plugins
           |> Enum.reject(fn {id, _} -> id == plugin_id end)
           |> Kernel.++([{plugin_id, stripped}])
         end) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "[plugin_manager] plugin #{inspect(plugin_id)} subscribed in runtime " <>
            "but cspec update failed for relay #{inspect(pid_id)} in " <>
            "namespace #{inspect(ns)}: #{inspect(reason)}"
        )

        :ok
    end
  end

  defp persist_enable_to_cspec(_plugin, _plugin_id), do: :ok
end
