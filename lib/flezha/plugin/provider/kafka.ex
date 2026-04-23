defmodule Flezha.Plugin.Provider.Kafka do
  @moduledoc false

  defmodule Producer do
    @moduledoc """
    First-party `:on_input` / `:on_output` transform subscriber that produces
    to a Kafka topic.

    Required opts:
      * `:hosts` (list of `{host, port}` tuples OR comma-separated string)
      * `:topic` (string)

    Optional opts:
      * `:partition` (integer, default 0)
      * `:required_acks` (integer, default 1)
    """

    alias Flezha.Plugin.Extension.Ctx
    alias Flezha.Plugin.ProviderBehaviour

    use ProviderBehaviour

    @enforce_keys [:hosts, :topic]
    defstruct @enforce_keys ++
                [
                  :conn,
                  partition: 0,
                  required_acks: 1
                ]

    @impl true
    def plugin_type(), do: :io_binding

    defp convert_value(:partition, value) when is_binary(value) do
      case Integer.parse(value) do
        {int_val, ""} -> {:ok, int_val}
        _ -> {:error, "Invalid value for partition: #{value}"}
      end
    end

    defp convert_value(:required_acks, value) when is_binary(value) do
      case Integer.parse(value) do
        {int_val, ""} -> {:ok, int_val}
        _ -> {:error, "Invalid value for required_acks: #{value}"}
      end
    end

    def init_provider(opts) do
      hosts = parse_hosts(Map.fetch!(opts, :hosts))

      case KafkaEx.start_link_worker(:no_name, uris: hosts) do
        {:ok, pid} -> {:ok, opts |> Map.put(:hosts, hosts) |> Map.put(:conn, pid), pid}
        {:error, reason} -> {:error, reason}
      end
    end

    def on_input(%Ctx.IO{} = ctx, opts), do: produce(ctx, opts)
    def on_output(%Ctx.IO{} = ctx, opts), do: produce(ctx, opts)

    defp produce(%Ctx.IO{message: msg, metadata: md} = ctx, opts) do
      partition =
        case Map.get(md, "kafka_msg_partition") do
          nil -> Map.get(opts, :partition, 0)
          string_val -> String.to_integer(string_val)
        end

      res =
        KafkaEx.produce(
          %KafkaEx.Protocol.Produce.Request{
            topic: Map.fetch!(opts, :topic),
            partition: partition,
            required_acks: Map.get(opts, :required_acks, 1),
            messages: [
              %KafkaEx.Protocol.Produce.Message{
                value: msg,
                key: Map.get(md, "kafka_msg_key", nil),
                timestamp: System.system_time(:millisecond)
              }
            ],
            api_version: 3
          },
          worker_name: Map.fetch!(opts, :conn)
        )

      case res do
        :ok -> {:ok, ctx}
        {:ok, _any} -> {:ok, ctx}
        {:error, reason} -> {:error, reason}
        any -> {:error, any}
      end
    end

    defp parse_hosts(hosts) when is_binary(hosts) do
      hosts
      |> String.split(",")
      |> Enum.map(fn host ->
        case String.split(host, ":") do
          [h, port] -> {h, String.to_integer(port)}
          _ -> throw("Invalid host format: #{host}")
        end
      end)
    end

    defp parse_hosts(hosts), do: hosts
  end
end
