defmodule Aflecha.Plugin.KafkaPlugin do
  alias Aflecha.IO.Handlers.Kafka
  alias Aflecha.Plugin.ManagedProcess
  alias Aflecha.Plugin.PluginContract

  use ManagedProcess
  use GenServer

  defmodule KafkaStart do
    @enforce_keys [:kafka_worker_id]
    defstruct [
      :kafka_worker_id,
      :uris
    ]
  end

  @impl PluginContract
  def plugin_type() do
    :managed_process
  end

  @impl ManagedProcess
  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl GenServer
  def init(_args) do
    {:ok, %{}}
  end

  # TODO: Validate the settings, check if worker alrady exists and error handling.
  @impl GenServer
  def handle_call({:start_connection, settings}, _from, state) do
    id = settings.kafka_worker_id

    {:ok, pid} =
      KafkaEx.start_link_worker(
        settings.kafka_worker_id,
        settings.uris
      )

    state = Map.put(state, id, pid)

    {:reply, {:ok, pid}, state}
  end

  def start_connection(%KafkaStart{} = settings) do
    GenServer.call(__MODULE__, {:start_connection, settings})
  end
end
