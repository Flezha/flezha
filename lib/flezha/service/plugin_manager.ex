defmodule Flezha.Service.PluginManager do
  use GenServer

  def enable_plugin(plugin_id, plugin) do
    case GenServer.call(__MODULE__, {:enable_plugin, plugin_id, plugin}) do
      {:ok, msg} -> {:ok, msg}
      {:error, reason} -> {:error, reason}
    end
  end

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

  @impl GenServer
  def init(:ok) do
    {:ok, %{}}
  end

  @impl true
  def handle_call(:get_plugins, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:enable_plugin, plugin_id, plugin}, _from, state) do
    module = plugin.__struct__

    {reply, new_state} =
      with :ok <- plugin_id_unique(state, plugin_id),
           {:ok, info} <- apply(module, :enable, [plugin_id, plugin]) do
        notify_recovery_enabled(plugin_id, plugin)
        {{:ok, "plugin enabled"}, Map.put(state, plugin_id, info)}
      else
        err -> {err, state}
      end

    {:reply, reply, new_state}
  end

  def handle_call({:disable_plugin, plugin_id}, _from, state) do
    case Map.get(state, plugin_id) do
      nil ->
        {:reply, {:error, "plugin not found"}, state}

      {module, info} ->
        apply(module, :disable, [plugin_id, info])
        notify_recovery_disabled(plugin_id)
        {:reply, {:ok, "plugin disabled"}, Map.delete(state, plugin_id)}
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

  defp plugin_id_unique(plugins, plugin_id) do
    case Map.get(plugins, plugin_id, nil) do
      nil -> :ok
      _ -> {:error, "plugin already registered"}
    end
  end

  # Forward enable/disable events to the failure-recovery subsystem so it can
  # persist recoverable plugins and drop them on disable. Kept as a module
  # call (not a GenServer message) so the persist happens synchronously with
  # enable success; try/catch guards test isolation when Coordinator/store
  # aren't running.
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
