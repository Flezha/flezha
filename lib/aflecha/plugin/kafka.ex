defmodule Aflecha.Plugin.Kafka do
  alias Aflecha.Plugin.WorkerSubscriber

  @enforce_keys [:hosts, :topic, :partition, :type]
  defstruct @enforce_keys

  defimpl WorkerSubscriber, for: Aflecha.Plugin.Kafka do
    # TODO: The system needs to be intelligent enough to know when to reuse a connection
    # and when to create a new one. We may need a separate GenServer handling the connections.
    @impl true
    def subscribe(%Aflecha.Plugin.Kafka{
          hosts: hosts,
          topic: topic,
          partition: partition,
          type: type
        }) do
      {:ok, pid} =
        KafkaEx.start_link_worker(
          :no_name,
          uris: hosts
        )

      case type do
        :producer ->
          fn data, _metadata ->
            KafkaEx.produce(topic, partition, data, worker_name: pid)
          end

        :consumer ->
          # TODO: implement
          fn _, _ -> nil end
      end
    end
  end
end
