defmodule Flezha.Plugin.Provider.Kafka do
  @moduledoc false

  defmodule Producer do
    @moduledoc """
    First-party `:on_input` / `:on_output` transform subscriber that produces
    to a Kafka topic with retries, idempotence, batching, bounded queueing,
    and active metadata refresh on transient broker errors.

    See `docs/kafka-producer.md` for the full executor-facing contract,
    parameter table, and telemetry events. The plugin defaults are tuned
    for streaming market data at 50–500 msg/sec/partition; `:linger_ms`,
    `:queue_max`, and `:retries` are the knobs executors most commonly tune
    per stream.

    Required opts:
      * `:hosts`  — list of `{host, port}` tuples OR comma-separated string
      * `:topic`  — destination topic name

    Producer-behaviour opts (all optional, defaults below):
      * `:partition`                       — int, default `0`
      * `:channel`                         — gate produces by metadata "channel" tag
      * `:required_acks`                   — int, default `-1` (acks=all). BREAKING: was `1`.
      * `:retries`                         — int, default `5`
      * `:retry_backoff_ms`                — int, default `100`
      * `:retry_max_backoff_ms`            — int, default `5000`
      * `:delivery_timeout_ms`             — int, default `30_000`
      * `:linger_ms`                       — int, default `10`
      * `:max_batch_bytes`                 — int, default `1_048_576`
      * `:queue_max`                       — int, default `10_000`
      * `:queue_overflow`                  — `:drop_oldest` (default), `:drop_newest`, or `:block`
      * `:queue_block_timeout_ms`          — int, default `5_000`
      * `:idempotent`                      — bool, default `true`
      * `:auto_create_topic`               — bool, default `true`
      * `:auto_create_partitions`          — int, default `1`
      * `:auto_create_replication_factor`  — int, default `2`
      * `:topic_configs`                   — keyword/list of `{name, value}` pairs, default `[]`
      * `:metadata_refresh_timeout_ms`     — int, default `5_000`
      * `:partitioner`                     — `{module, function}` partitioner, default `nil`
    """

    alias Flezha.Plugin.Extension.Ctx
    alias Flezha.Plugin.Provider.Kafka.{ProducerSup, ProducerWorker}
    alias Flezha.Plugin.ProviderBehaviour

    use ProviderBehaviour

    @enforce_keys [:hosts, :topic]
    defstruct @enforce_keys ++
                [
                  :conn,
                  :registry_ref,
                  :producer_pid,
                  :plugin_id,
                  :channel,
                  :partitioner,
                  partition: 0,
                  required_acks: -1,
                  retries: 5,
                  retry_backoff_ms: 100,
                  retry_max_backoff_ms: 5_000,
                  delivery_timeout_ms: 30_000,
                  linger_ms: 10,
                  max_batch_bytes: 1_048_576,
                  queue_max: 10_000,
                  queue_overflow: :drop_oldest,
                  queue_block_timeout_ms: 5_000,
                  idempotent: true,
                  auto_create_topic: true,
                  auto_create_partitions: 1,
                  auto_create_replication_factor: 2,
                  topic_configs: [],
                  metadata_refresh_timeout_ms: 5_000
                ]

    @impl true
    def plugin_type(), do: :io_binding

    def init_provider(opts) do
      hosts = parse_hosts(Map.fetch!(opts, :hosts))
      topic = Map.fetch!(opts, :topic)
      partition = coerce_int(Map.get(opts, :partition, 0))
      plugin_id = Map.get(opts, :plugin_id, "default-#{System.unique_integer([:positive])}")

      identity = %{
        hosts: hosts,
        ssl_options: Map.get(opts, :ssl_options),
        use_ssl: Map.get(opts, :use_ssl),
        auth: Map.get(opts, :auth),
        metadata_update_interval: Map.get(opts, :metadata_update_interval)
      }

      worker_opts = producer_opts(opts, identity, topic, partition, plugin_id)

      case ProducerSup.ensure_worker(worker_opts) do
        {:ok, producer_pid} ->
          opts =
            opts
            |> Map.put(:hosts, hosts)
            |> Map.put(:topic, topic)
            |> Map.put(:partition, partition)
            |> Map.put(:producer_pid, producer_pid)
            |> Map.put(:plugin_id, plugin_id)

          {:ok, opts, producer_pid}

        {:error, reason} ->
          {:error, reason}
      end
    end

    def on_input(%Ctx.IO{} = ctx, opts), do: produce(ctx, opts)
    def on_output(%Ctx.IO{} = ctx, opts), do: produce(ctx, opts)

    defp produce(%Ctx.IO{message: msg, metadata: md} = ctx, opts) do
      case Map.get(opts, :channel) do
        nil ->
          do_produce(ctx, opts, msg, md)

        group ->
          if Map.get(md, "channel") == group,
            do: do_produce(ctx, opts, msg, md),
            else: {:ok, ctx}
      end
    end

    defp do_produce(%Ctx.IO{} = ctx, opts, msg, md) do
      producer_pid = Map.fetch!(opts, :producer_pid)
      :ok = ProducerWorker.cast_enqueue(producer_pid, msg, md)
      {:ok, ctx}
    end

    defp producer_opts(opts, identity, topic, partition, plugin_id) do
      [
        connection_identity: identity,
        topic: topic,
        partition: partition,
        plugin_id: plugin_id,
        linger_ms: coerce_int(Map.get(opts, :linger_ms, 10)),
        max_batch_bytes: coerce_int(Map.get(opts, :max_batch_bytes, 1_048_576)),
        queue_max: coerce_int(Map.get(opts, :queue_max, 10_000)),
        queue_overflow: coerce_atom(Map.get(opts, :queue_overflow, :drop_oldest)),
        queue_block_timeout_ms: coerce_int(Map.get(opts, :queue_block_timeout_ms, 5_000)),
        retries: coerce_int(Map.get(opts, :retries, 5)),
        retry_backoff_ms: coerce_int(Map.get(opts, :retry_backoff_ms, 100)),
        retry_max_backoff_ms: coerce_int(Map.get(opts, :retry_max_backoff_ms, 5_000)),
        delivery_timeout_ms: coerce_int(Map.get(opts, :delivery_timeout_ms, 30_000)),
        required_acks: coerce_int(Map.get(opts, :required_acks, -1)),
        idempotent: coerce_bool(Map.get(opts, :idempotent, true)),
        auto_create_topic: coerce_bool(Map.get(opts, :auto_create_topic, true)),
        auto_create_partitions: coerce_int(Map.get(opts, :auto_create_partitions, 1)),
        auto_create_replication_factor:
          coerce_int(Map.get(opts, :auto_create_replication_factor, 2)),
        topic_configs: Map.get(opts, :topic_configs, []),
        metadata_refresh_timeout_ms:
          coerce_int(Map.get(opts, :metadata_refresh_timeout_ms, 5_000))
      ]
    end

    @doc """
    Synchronous read of producer counters for `plugin_id`. Each plugin
    instance owns exactly one producer worker (one topic+partition per
    `init_provider/1` call), so the lookup is unique.

    Returns `{:ok, %{dropped_total, queue_depth, in_flight, last_error,
    topic, partition}}` or `{:error, :no_producer}`.
    """
    @spec stats(binary()) :: {:ok, map()} | {:error, :no_producer}
    def stats(plugin_id) when is_binary(plugin_id) do
      pattern = {{plugin_id, :"$1", :"$2"}, :"$3", :_}

      case Registry.select(ProducerSup.registry_name(), [{pattern, [], [:"$$"]}]) do
        [[topic, partition, pid] | _] ->
          stats =
            pid
            |> ProducerWorker.stats()
            |> Map.put(:topic, topic)
            |> Map.put(:partition, partition)

          {:ok, stats}

        [] ->
          {:error, :no_producer}
      end
    end

    @doc """
    Same as `stats/1` but with explicit `(topic, partition)` for the rare
    case of a registry collision or programmatic lookup.
    """
    @spec stats(binary(), binary(), non_neg_integer()) ::
            {:ok, map()} | {:error, :no_producer}
    def stats(plugin_id, topic, partition) do
      case Registry.lookup(ProducerSup.registry_name(), {plugin_id, topic, partition}) do
        [{pid, _}] ->
          {:ok,
           pid
           |> ProducerWorker.stats()
           |> Map.put(:topic, topic)
           |> Map.put(:partition, partition)}

        [] ->
          {:error, :no_producer}
      end
    end

    defp convert_value(:partition, value), do: parse_int(value, "partition")
    defp convert_value(:required_acks, value), do: parse_int(value, "required_acks")
    defp convert_value(:retries, value), do: parse_int(value, "retries")
    defp convert_value(:retry_backoff_ms, value), do: parse_int(value, "retry_backoff_ms")
    defp convert_value(:retry_max_backoff_ms, value), do: parse_int(value, "retry_max_backoff_ms")
    defp convert_value(:delivery_timeout_ms, value), do: parse_int(value, "delivery_timeout_ms")
    defp convert_value(:linger_ms, value), do: parse_int(value, "linger_ms")
    defp convert_value(:max_batch_bytes, value), do: parse_int(value, "max_batch_bytes")
    defp convert_value(:queue_max, value), do: parse_int(value, "queue_max")

    defp convert_value(:queue_block_timeout_ms, value),
      do: parse_int(value, "queue_block_timeout_ms")

    defp convert_value(:auto_create_partitions, value),
      do: parse_int(value, "auto_create_partitions")

    defp convert_value(:auto_create_replication_factor, value),
      do: parse_int(value, "auto_create_replication_factor")

    defp convert_value(:metadata_refresh_timeout_ms, value),
      do: parse_int(value, "metadata_refresh_timeout_ms")

    defp convert_value(:idempotent, value), do: parse_bool(value, "idempotent")
    defp convert_value(:auto_create_topic, value), do: parse_bool(value, "auto_create_topic")

    defp convert_value(:queue_overflow, value) when is_binary(value) do
      case value do
        "drop_oldest" -> {:ok, :drop_oldest}
        "drop_newest" -> {:ok, :drop_newest}
        "block" -> {:ok, :block}
        other -> {:error, "Invalid value for queue_overflow: #{other}"}
      end
    end

    defp convert_value(:queue_overflow, value)
         when value in [:drop_oldest, :drop_newest, :block],
         do: {:ok, value}

    defp parse_int(value, _field) when is_integer(value), do: {:ok, value}

    defp parse_int(value, field) when is_binary(value) do
      case Integer.parse(value) do
        {int_val, ""} -> {:ok, int_val}
        _ -> {:error, "Invalid value for #{field}: #{value}"}
      end
    end

    defp parse_bool(true, _), do: {:ok, true}
    defp parse_bool(false, _), do: {:ok, false}
    defp parse_bool("true", _), do: {:ok, true}
    defp parse_bool("false", _), do: {:ok, false}
    defp parse_bool(value, field), do: {:error, "Invalid value for #{field}: #{inspect(value)}"}

    defp coerce_int(v) when is_integer(v), do: v
    defp coerce_int(v) when is_binary(v), do: String.to_integer(v)

    defp coerce_bool(true), do: true
    defp coerce_bool(false), do: false
    defp coerce_bool("true"), do: true
    defp coerce_bool("false"), do: false

    defp coerce_atom(v) when is_atom(v), do: v
    defp coerce_atom(v) when is_binary(v), do: String.to_existing_atom(v)

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
