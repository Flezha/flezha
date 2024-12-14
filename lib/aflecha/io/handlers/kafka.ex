defmodule Aflecha.IO.Handlers.Kafka do
  @moduledoc """
  Kafka handler for Aflecha.IO
  """

  require Logger

  use Aflecha.IO.Contract

  @impl true
  def send_payload(_pid, _msg) do
    :ok
  end


  # TODO: REMOVE LATER
  use KafkaEx.GenConsumer

  alias KafkaEx.Protocol.Fetch.Message

  require Logger

  @impl KafkaEx.GenConsumer
  def handle_message_set(message_set, state) do
    for %Message{value: message} <- message_set do
      Logger.debug(fn -> "message: " <> inspect(message) end)
    end
    {:async_commit, state}
  end
end
