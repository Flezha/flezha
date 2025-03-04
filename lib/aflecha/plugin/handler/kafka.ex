defmodule Aflecha.Plugin.Handler.Kafka do
  alias Aflecha.Plugin.WorkerBinding

  defmodule Producer do
    @enforce_keys [:hosts, :topic, :partition]
    defstruct @enforce_keys ++ [:conn]

    defimpl WorkerBinding, for: Producer do
      # TODO: The system needs to be intelligent enough to know when to reuse a connection
      # and when to create a new one. We may need a separate GenServer handling the connections.
      @impl true
      def bind(settings) do
        res = KafkaEx.start_link_worker(:no_name, uris: settings.hosts)

        case res do
          {:ok, pid} -> {:ok, %Producer{settings | conn: pid}}
          {:error, reason} -> {:error, reason}
        end
      end

      @impl true
      def exec(settings, payload) do
        res =
          KafkaEx.produce(
            settings.topic,
            settings.partition,
            payload,
            worker_name: settings.conn
          )

        case res do
          :ok -> {:ok, payload}
          {:ok, any} -> {:ok, payload}
          {:error, reason} -> {:error, reason}
          any -> {:error, any}
        end
      end
    end
  end
end
