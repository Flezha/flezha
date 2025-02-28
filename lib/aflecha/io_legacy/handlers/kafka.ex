defmodule Aflecha.IO.Handlers.Kafka do
  @moduledoc """
  Kafka handler for Aflecha.IO
  """

  alias KafkaEx.Protocol.Fetch.Message

  require Logger

  use KafkaEx.GenConsumer
  use Aflecha.IO.Contract

  defstruct [
    :worker_id,
    :brokers
  ]

  def start_link(_pipeline_pid, %__MODULE__{worker_id: _worker_id, brokers: _brokers}, _opts) do
    # KafkaEx.GenConsumer.start_link(__MODULE__, group_name, topic, partition, opts)

    # KafkaEx.start_link_worker(worker_id, [{:uris, brokers}])
    # KafkaEx.start_link_worker(:no_name, {:uris, brokers})
  end

  @impl true
  @spec send_payload(any(), any()) :: :ok
  def send_payload(_pid, _msg) do
    :ok
  end

  @impl true
  def init(_topic, _partition) do
    {:ok, %{}}
  end

  @impl true
  def init(_topic, _partition, _extra_args) do
    {:ok, %{}}
  end

  @impl true
  def handle_message_set(message_set, state) do
    for %Message{value: message} <- message_set do
      Logger.debug(fn -> "message: " <> inspect(message) end)
    end

    {:async_commit, state}
  end
end
