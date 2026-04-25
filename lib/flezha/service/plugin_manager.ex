defmodule Flezha.Service.PluginManager do
  @moduledoc """
  Per-node registry of enabled `%Flezha.Plugin{}` entries. Routes
  `enable_plugin/2` into the owning processor's Overwatch via the unified
  `{:plugin_subscribe, ...}` / `{:plugin_unsubscribe, ...}` messages.
  """

  use GenServer

  alias Flezha.Plugin
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
        notify_recovery_enabled(plugin_id, plugin)

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
        case ProcessorManager.get_processor_pid(namespace, processor_id) do
          {:ok, pid} ->
            _ = GenServer.call(pid, {:plugin_unsubscribe, plugin_id})
            notify_recovery_disabled(plugin_id)
            {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}

          _ ->
            # Processor gone — treat as disabled locally.
            notify_recovery_disabled(plugin_id)
            {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}
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

  defp notify_recovery_enabled(plugin_id, plugin) do
    try do
      Flezha.Recovery.Coordinator.on_plugin_enabled(plugin_id, plugin)
    catch
      _kind, _reason -> :ok
    end
  end

  defp notify_recovery_disabled(plugin_id) do
    try do
      Flezha.Recovery.Coordinator.on_plugin_disabled(plugin_id)
    catch
      _kind, _reason -> :ok
    end
  end
end
