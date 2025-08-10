defmodule Flezha.Plugin.Provider.Kafka do
  alias Flezha.Processor.IOBinding

  defmodule Producer do
    alias Flezha.Plugin.ProviderBehaviour

    use ProviderBehaviour

    @enforce_keys [:hosts, :topic]
    defstruct @enforce_keys ++
                [
                  :conn,
                  partition: nil,
                  required_acks: 1
                ]

    @impl true
    def plugin_type(), do: :io_binding

    defp convert_value(:partition, value) when is_binary(value) do
      case Integer.parse(value) do
        {int_val, ""} -> int_val
        _ -> nil
      end
    end

    defp convert_value(:required_acks, value) when is_binary(value) do
      case Integer.parse(value) do
        {int_val, ""} -> int_val
        _ -> 1
      end
    end

    defimpl IOBinding, for: Flezha.Plugin.Provider.Kafka.Producer do
      # TODO: The system needs to be intelligent enough to know when to reuse a connection
      # and when to create a new one. We may need a separate GenServer handling the connections.
      @impl true
      def bind(settings) do
        settings = pre_bind(settings)
        res = KafkaEx.start_link_worker(:no_name, uris: settings.hosts)

        case res do
          {:ok, pid} -> {:ok, {%Producer{settings | conn: pid}, pid}}
          {:error, reason} -> {:error, reason}
        end
      end

      @impl true
      def exec(settings, {message, metadata} = payload) do
        partition =
          case Map.get(metadata, "kafka_msg_partition") do
            nil -> settings.partition
            string_val -> String.to_integer(string_val)
          end

        res =
          KafkaEx.produce(
            %KafkaEx.Protocol.Produce.Request{
              topic: settings.topic,
              partition: partition,
              required_acks: settings.required_acks,
              messages: [
                %KafkaEx.Protocol.Produce.Message{
                  value: message,
                  key: Map.get(metadata, "kafka_msg_key", nil),
                  timestamp: System.system_time(:millisecond)
                }
              ],
              api_version: 3
            },
            worker_name: settings.conn
          )

        case res do
          :ok -> {:ok, payload}
          {:ok, _any} -> {:ok, payload}
          {:error, reason} -> {:error, reason}
          any -> {:error, any}
        end
      end

      defp pre_bind(settings) do
        if is_binary(settings.hosts) do
          # If hosts is a string, we assume it's a comma-separated list of host:port pairs.
          hosts =
            settings.hosts
            |> String.split(",")
            |> Enum.map(fn host ->
              case String.split(host, ":") do
                [host, port] -> {host, String.to_integer(port)}
                _ -> throw("Invalid host format: #{host}")
              end
            end)

          %{settings | hosts: hosts}
        else
          settings
        end
      end
    end
  end
end
