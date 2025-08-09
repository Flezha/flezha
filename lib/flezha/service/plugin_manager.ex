defmodule Flezha.Service.PluginManager do
  alias Flezha.Plugin.PluginProtocol

  use GenServer

  def enable_plugin(plugin_id, plugin) do
    case GenServer.call(__MODULE__, {:enable_plugin, plugin_id, plugin}) do
      {:ok, msg} -> {:ok, msg}
      {:error, reason} -> raise "Error enabling plugin: #{reason}"
    end
  end

  def disable_plugin(plugin) do
    PluginProtocol.disable(plugin)
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl GenServer
  def init(:ok) do
    {:ok, %{}}
  end

  def handle_call({:enable_plugin, plugin_id, plugin}, _from, state) do
    module = plugin.__struct__

    {reply, new_state} =
      case apply(module, :enable, [plugin_id, plugin]) do
        {:ok, info} -> {{:ok, "plugin enabled"}, Map.put(state, plugin_id, info)}
        {:error, reason} -> {{:error, reason}, state}
      end

    {:reply, reply, new_state}
  end
end
