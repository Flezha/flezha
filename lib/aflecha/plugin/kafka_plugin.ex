defmodule Aflecha.Plugin.KafkaPlugin do
  alias Aflecha.Plugin.ManagedProcess
  alias Aflecha.Plugin.PluginContract

  use ManagedProcess
  use GenServer

  @impl PluginContract
  def plugin_type() do
    :managed_process
  end

  @impl PluginContract
  def load() do
    :ok
  end

  @impl ManagedProcess
  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl GenServer
  def init(_args) do
    {:ok, nil}
  end
end
