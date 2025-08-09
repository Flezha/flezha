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

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl GenServer
  def init(:ok) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:enable_plugin, plugin_id, plugin}, _from, state) do
    module = plugin.__struct__

    {reply, new_state} =
      case apply(module, :enable, [plugin_id, plugin]) do
        {:ok, info} -> {{:ok, "plugin enabled"}, Map.put(state, plugin_id, info)}
        {:error, reason} -> {{:error, reason}, state}
      end

    {:reply, reply, new_state}
  end

  def handle_call({:disable_plugin, plugin_id}, _from, state) do
    case Map.get(state, plugin_id) do
      nil ->
        {:reply, {:error, "plugin not found"}, state}

      {module, info} ->
        apply(module, :disable, [plugin_id, info])
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
end
