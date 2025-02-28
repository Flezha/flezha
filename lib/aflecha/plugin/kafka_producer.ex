defmodule Aflecha.Plugin.KafkaProducer do
  alias Aflecha.Plugin.WorkerBinding

  @enforce_keys [:hosts, :topic, :partition]
  defstruct @enforce_keys ++ [:conn]

  defimpl WorkerBinding, for: Aflecha.Plugin.KafkaProducer do
    # TODO: The system needs to be intelligent enough to know when to reuse a connection
    # and when to create a new one. We may need a separate GenServer handling the connections.
    @impl true
    def bind(settings) do
      res = KafkaEx.start_link_worker(:no_name, uris: settings.hosts)

      case res do
        {:ok, pid} -> {:ok, %Aflecha.Plugin.KafkaProducer{settings | conn: pid}}
        {:error, reason} -> {:error, reason}
      end
    end

    @impl true
    def exec(settings, data) do
      KafkaEx.produce(
        settings.topic,
        settings.partition,
        data,
        worker_name: settings.conn
      )
    end
  end
end
