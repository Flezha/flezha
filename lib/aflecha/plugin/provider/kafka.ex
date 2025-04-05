defmodule Aflecha.Plugin.Provider.Kafka do
  alias Aflecha.Plugin.PipelineBinding
  alias Aflecha.Plugin.PipelinePlugin
  alias Aflecha.Plugin.PluginBase
  alias Aflecha.Plugin.PluginProtocol

  defmodule Producer do
    @behaviour PluginBase

    @enforce_keys PipelinePlugin.default_keys() ++ [:hosts, :topic, :partition]
    defstruct @enforce_keys ++ [:conn]

    @impl true
    def plugin_id(), do: "pipeline_kafka_producer"

    defimpl PluginProtocol, for: Aflecha.Plugin.Provider.Kafka.Producer do
      use PipelinePlugin
    end

    defimpl PipelineBinding, for: Aflecha.Plugin.Provider.Kafka.Producer do
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
      def exec(settings, {_type, message} = payload) do
        res =
          KafkaEx.produce(
            settings.topic,
            settings.partition,
            message,
            worker_name: settings.conn
          )

        case res do
          :ok -> {:ok, payload}
          {:ok, _any} -> {:ok, payload}
          {:error, reason} -> {:error, reason}
          any -> {:error, any}
        end
      end
    end
  end
end
