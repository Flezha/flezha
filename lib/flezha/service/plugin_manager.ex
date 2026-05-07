defmodule Flezha.Service.PluginManager do
  @moduledoc """
  Per-node registry of enabled `%Flezha.Plugin{}` entries. Routes
  `enable_plugin/2` into the owning processor's Overwatch via the unified
  `{:plugin_subscribe, ...}` / `{:plugin_unsubscribe, ...}` messages and
  persists the subscription into the processor's ProcessHub child-spec
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
  alias Flezha.Service.ProcessorManager

  ## Public API

  def enable_plugin(plugin_id, %Plugin{} = plugin) do
    cond do
      has_init_subscription?(plugin) ->
        {:error, "init-state plugins must be attached at processor start"}

      true ->
        case GenServer.call(__MODULE__, {:enable_plugin, plugin_id, plugin}) do
          {:ok, msg} -> {:ok, msg}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  # `:on_processor_init` fires exactly once during `Overwatch.init/1`. Late
  # enable via `flz plugin enable` cannot hook it — returning an explicit
  # error keeps operators from silently losing init-state wiring.
  defp has_init_subscription?(%Plugin{subscriptions: subs}) when is_list(subs) do
    Enum.any?(subs, fn
      {:on_processor_init, _} -> true
      :on_processor_init -> true
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
           {:ok, pid} <- resolve_processor(plugin),
           :ok <- forward_subscribe(pid, plugin_id, plugin) do
        # Per design.md Decision 2: subscribe-first, cspec-update-second.
        # If the cspec write fails after a successful subscribe, the
        # runtime is correct but the cspec is stale; log at :error so
        # operators see the divergence and reply :ok (the next plugin op
        # for this processor will canonicalise the cspec).
        persist_enable_to_cspec(plugin, plugin_id)

        {{:ok, "plugin enabled"},
         Map.put(state, plugin_id, {plugin.namespace, plugin.processor_id})}
      else
        err -> {err, state}
      end

    {:reply, reply, new_state}
  end

  def handle_call({:disable_plugin, plugin_id}, _from, state) do
    case Map.get(state, plugin_id) do
      nil ->
        {:reply, {:error, "plugin not found"}, state}

      {namespace, processor_id} ->
        # Per design.md Decision 2: cspec-update-first, unsubscribe-second
        # for the disable path. If the cspec update fails the unsubscribe
        # SHALL NOT run — surface the error so the caller can retry. If
        # the unsubscribe fails after the cspec has already been
        # rewritten, the runtime extensions table is stale-but-correct
        # (still subscribed); the next Overwatch (re)start re-subscribes
        # only the cspec-listed plugins, dropping the stale entry.
        case HubManager.update_processor_plugins(namespace, processor_id, fn plugins ->
               Enum.reject(plugins, fn {id, _plugin} -> id == plugin_id end)
             end) do
          :ok ->
            case ProcessorManager.get_processor_pid(namespace, processor_id) do
              {:ok, pid} ->
                _ = GenServer.call(pid, {:plugin_unsubscribe, plugin_id})
                {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}

              _ ->
                # Processor gone — treat as disabled locally.
                {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}
            end

          {:error, "processor not found"} ->
            # Processor gone — treat as disabled locally; cspec already
            # absent so there is nothing to canonicalise.
            {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
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

  defp resolve_processor(%Plugin{namespace: ns, processor_id: pid_id})
       when is_binary(ns) and is_binary(pid_id) do
    ProcessorManager.get_processor_pid(ns, pid_id)
  end

  # Plugins with nil namespace/processor_id can reach this path when an
  # operator enables a plugin via the installer CLI without first
  # binding it to a target — surface a structured error instead of
  # letting the call cascade into ProcessHub's storage layer where a
  # nil hub_id raises an ETS lookup ArgumentError.
  defp resolve_processor(%Plugin{namespace: ns, processor_id: pid_id}) do
    {:error,
     "plugin missing namespace or processor_id (got namespace=#{inspect(ns)}, processor_id=#{inspect(pid_id)})"}
  end

  defp forward_subscribe(pid, plugin_id, plugin) do
    case GenServer.call(pid, {:plugin_subscribe, plugin_id, plugin}) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist_enable_to_cspec(%Plugin{namespace: ns, processor_id: pid_id} = plugin, plugin_id)
       when is_binary(ns) and is_binary(pid_id) do
    stripped = Plugin.strip_transient(plugin)

    case HubManager.update_processor_plugins(ns, pid_id, fn plugins ->
           plugins
           |> Enum.reject(fn {id, _} -> id == plugin_id end)
           |> Kernel.++([{plugin_id, stripped}])
         end) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "[plugin_manager] plugin #{inspect(plugin_id)} subscribed in runtime " <>
            "but cspec update failed for processor #{inspect(pid_id)} in " <>
            "namespace #{inspect(ns)}: #{inspect(reason)}"
        )

        :ok
    end
  end

  defp persist_enable_to_cspec(_plugin, _plugin_id), do: :ok
end
