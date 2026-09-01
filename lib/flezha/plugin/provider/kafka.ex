defmodule Flezha.Plugin.Provider.Kafka do
  @moduledoc false

  defmodule Producer do
    @moduledoc """
    First-party `:on_input` / `:on_output` transform subscriber that produces
    to a Kafka topic with retries, idempotence, batching, bounded queueing,
    and active metadata refresh on transient broker errors.

    See `docs/plugins/kafka_producer.md` for the full runner-facing contract,
    parameter table, and telemetry events. The plugin defaults are tuned
    for streaming market data at 50–500 msg/sec/partition; `:linger_ms`,
    `:queue_max`, and `:retries` are the knobs runners most commonly tune
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
      * `:queue_overflow`                  — `:drop_nothing` (default), `:drop_oldest`, or `:drop_newest`
      * `:queue_block_timeout_ms`          — int, default `5_000`
      * `:idempotent`                      — bool, default `false` (opt in for exactly-once)
      * `:max_in_flight`                   — int, default `5` with `:idempotent`, else `1`
      * `:auto_create_topic`               — bool, default `true`
      * `:auto_create_partitions`          — int, default `1`
      * `:auto_create_replication_factor`  — int, default `2`
      * `:topic_configs`                   — keyword/list of `{name, value}` pairs, default `[]`
      * `:metadata_refresh_timeout_ms`     — int, default `5_000`
    """

    alias Flezha.Plugin.Extension.Ctx
    alias Flezha.Plugin.Provider.Kafka.{ProducerSup, ProducerWorker}
    alias Flezha.Plugin.ProviderBehaviour
    alias Flezha.State.Outbox
    alias KafkaEx.Auth.Config, as: KafkaAuthConfig

    use ProviderBehaviour

    import ProviderBehaviour,
      only: [convert_opts: 4, parse_bool: 2, parse_enum: 3, parse_int: 2, refuse_value: 2]

    # The one default table. `defstruct` and the conversion table both derive
    # from it, so a new tunable is added in exactly one place.
    @int_opts [
      partition: 0,
      required_acks: -1,
      retries: 5,
      retry_backoff_ms: 100,
      retry_max_backoff_ms: 5_000,
      delivery_timeout_ms: 30_000,
      linger_ms: 10,
      max_batch_bytes: 1_048_576,
      queue_max: 10_000,
      queue_block_timeout_ms: 5_000,
      # Unset: the window's width follows `:idempotent`.
      max_in_flight: nil,
      auto_create_partitions: 1,
      auto_create_replication_factor: 2,
      metadata_refresh_timeout_ms: 5_000
    ]

    @bool_opts [idempotent: false, auto_create_topic: true]

    @int_fields Keyword.keys(@int_opts)
    @bool_fields Keyword.keys(@bool_opts)

    @queue_overflow_policies [:drop_nothing, :drop_oldest, :drop_newest]

    # The keys and the mechanisms `KafkaEx.Auth.Config` accepts.
    @auth_keys [:mechanism, :username, :password, :mechanism_opts]
    @auth_mechanisms [:plain, :scram, :oauthbearer, :msk_iam]

    # Every option `init_provider/1` interprets, converted once, before any
    # worker or broker client exists.
    @converted_fields @int_fields ++ @bool_fields ++ [:queue_overflow, :auth]

    # Field-guarded, and the value-guarded options (`:queue_overflow`, `:auth`)
    # carry their own terminal clause, so the table is already total.
    @converted_opts []

    @enforce_keys [:hosts, :topic]
    defstruct @enforce_keys ++
                [:conn, :registry_ref, :producer_pid, :plugin_id, :channel] ++
                @int_opts ++
                @bool_opts ++ [queue_overflow: :drop_nothing, topic_configs: []]

    @impl true
    def plugin_type(), do: :io_binding

    def init_provider(opts) do
      with {:ok, tuned} <- convert_opts(__MODULE__, opts, @converted_fields, &convert_value/2),
           opts = Map.replace(opts, :auth, tuned.auth),
           hosts = parse_hosts(Map.fetch!(opts, :hosts)),
           topic = Map.fetch!(opts, :topic),
           plugin_id = Map.get(opts, :plugin_id, "default-#{System.unique_integer([:positive])}"),
           worker_opts =
             producer_opts(opts, tuned, identity(opts, hosts, tuned), topic, plugin_id),
           :ok <- validate_window(worker_opts),
           {:ok, producer_pid} <- ProducerSup.ensure_worker(worker_opts) do
        opts =
          opts
          |> Map.put(:hosts, hosts)
          |> Map.put(:topic, topic)
          |> Map.put(:partition, tuned.partition)
          |> Map.put(:producer_pid, producer_pid)
          |> Map.put(:plugin_id, plugin_id)
          |> redact_sasl()

        {:ok, opts, producer_pid}
      end
    end

    # A `KafkaEx.Client` answers one request at a time, encoding each batch in
    # its own process, so every partition behind one client serialises on it.
    # The lane spreads a cluster's partitions over as many clients as there are
    # schedulers — one connection per core at most, which is all the encode
    # parallelism the node has to give.
    #
    # SASL credentials ride the closure into the client start (mirroring the
    # Postgres provider): they never land in worker state, child args, or the
    # persisted/returned opts. `auth_key` is what lets two plugins with the same
    # credentials share one broker client without the registry ever seeing the
    # secret. Prefer `:sasl_password_env` — an inline password cannot survive a
    # restart from the redacted persisted opts.
    defp identity(opts, hosts, tuned) do
      resolved_auth = resolve_auth(opts)

      %{
        hosts: hosts,
        ssl_options: Map.get(opts, :ssl_options),
        use_ssl: Map.get(opts, :use_ssl),
        auth_fun: fn -> resolve_auth(opts) end,
        auth_key: resolved_auth && :erlang.phash2(resolved_auth),
        metadata_update_interval: Map.get(opts, :metadata_update_interval),
        lane: rem(tuned.partition, System.schedulers_online())
      }
    end

    # Only the broker's per-partition sequence check keeps a retry inside the
    # window ordered, so a window wider than one without idempotence would
    # trade the plugin's unconditional FIFO guarantee for throughput.
    defp validate_window(worker_opts) do
      max_in_flight = Keyword.fetch!(worker_opts, :max_in_flight)

      if max_in_flight > 1 and not Keyword.fetch!(worker_opts, :idempotent) do
        {:error, {:max_in_flight_requires_idempotent, max_in_flight}}
      else
        :ok
      end
    end

    @redacted "**redacted**"

    # Resolve the SASL config presented by the plugin opts into a
    # `KafkaEx.Auth.Config`. `:sasl_password_env` (env-var name) overrides an
    # inline password, so credentials can stay out of the persisted opts
    # entirely. Called at every client (re)start, so a rotated env var takes
    # effect on the next reconnect — and it can only do that safely because the
    # keys and the mechanism were closed at preparation.
    defp resolve_auth(opts) do
      case Map.get(opts, :auth) do
        nil ->
          nil

        %KafkaEx.Auth.Config{} = config ->
          maybe_override_password(config, opts)

        %{} = map ->
          map
          |> Map.put_new(:mechanism, :plain)
          |> KafkaAuthConfig.new()
          |> maybe_override_password(opts)
      end
    end

    defp maybe_override_password(%KafkaEx.Auth.Config{} = config, opts) do
      case Map.get(opts, :sasl_password_env) do
        env when is_binary(env) and env != "" ->
          %{config | password: System.get_env(env)}

        _ ->
          config
      end
    end

    defp redact_sasl(opts) do
      case Map.get(opts, :auth) do
        %KafkaEx.Auth.Config{password: p} = config when is_binary(p) and p != "" ->
          Map.put(opts, :auth, %{config | password: @redacted})

        %{} = map ->
          Map.put(opts, :auth, redact_map_password(map))

        _ ->
          opts
      end
    end

    defp redact_map_password(%{password: p} = map) when is_binary(p) and p != "",
      do: %{map | password: @redacted}

    defp redact_map_password(map), do: map

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
      # Take ownership of a delivery-guaranteed output before handing it over:
      # the worker answers the hold when the broker acks its batch, or parks it
      # when the retry ladder is permanently exhausted. A plain output holds
      # nothing and this is a no-op.
      Outbox.hold(md)
      :ok = ProducerWorker.cast_enqueue(producer_pid, msg, md)
      {:ok, ctx}
    end

    # Every tunable is already converted and already carries its `defstruct`
    # default, so this names only what the worker needs and the table cannot
    # supply: the identity, the two required opts, the one list option, and the
    # window whose default follows `:idempotent`. `:auth` is dropped — the
    # credentials travel in the identity closure and never in worker opts.
    defp producer_opts(opts, tuned, identity, topic, plugin_id) do
      [
        connection_identity: identity,
        topic: topic,
        plugin_id: plugin_id,
        topic_configs: Map.get(opts, :topic_configs, []),
        # 5 is Kafka's own ceiling for ordered idempotent retries; without a
        # producer session there is nothing to keep a retry in order.
        max_in_flight: tuned.max_in_flight || if(tuned.idempotent, do: 5, else: 1)
      ] ++ Map.to_list(Map.drop(tuned, [:auth, :max_in_flight]))
    end

    @doc """
    Synchronous read of producer counters for `plugin_id`. Each plugin
    instance owns exactly one producer worker (one topic+partition per
    `init_provider/1` call), so the lookup is unique.

    Returns `{:ok, %{queue_depth, in_flight, last_error, consecutive_failures,
    topic, partition}}` or `{:error, :no_producer}`.
    """
    @spec stats(binary()) :: {:ok, map()} | {:error, :no_producer}
    def stats(plugin_id) when is_binary(plugin_id) do
      pattern = {{plugin_id, :"$1", :"$2"}, :"$3", :_}

      case Registry.select(ProducerSup.registry_name(), [{pattern, [], [:"$$"]}]) do
        [[topic, partition, pid] | _] ->
          # The Registry entry is unregistered asynchronously when the
          # worker exits, so we may see a stale pid here for a brief
          # window after `ProducerSup.stop_worker/3`.
          try do
            stats =
              pid
              |> ProducerWorker.stats()
              |> Map.put(:topic, topic)
              |> Map.put(:partition, partition)

            {:ok, stats}
          catch
            :exit, {:noproc, _} -> {:error, :no_producer}
            :exit, {:normal, _} -> {:error, :no_producer}
          end

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
          try do
            {:ok,
             pid
             |> ProducerWorker.stats()
             |> Map.put(:topic, topic)
             |> Map.put(:partition, partition)}
          catch
            :exit, {:noproc, _} -> {:error, :no_producer}
            :exit, {:normal, _} -> {:error, :no_producer}
          end

        [] ->
          {:error, :no_producer}
      end
    end

    # A worker that has given up on this many consecutive batches is
    # reported `:produce_stalled` so the convergence loop can escalate to a
    # plugin restart. The worker self-refreshes its broker client on the
    # same threshold; health flags a stall that a fresh connection did not
    # clear, which is the signal worth escalating.
    @stall_health_threshold 3

    @doc """
    Health verdict for the producer worker (plugin health contract). Healthy
    means the worker pid is alive and not permanently parked — a worker whose
    `last_error` is `{:swap_exhausted, _}` has exhausted its in-process
    broker-swap recovery and the queue cannot drain, and a worker with
    a sustained run of failed batches (`:produce_stalled`) is wedged against a
    healthy-but-unreachable broker, so the convergence loop escalates by
    restarting the provider.
    """
    @impl true
    @spec health(map()) :: Flezha.Plugin.ProviderBehaviour.health_verdict()
    def health(opts) do
      ProviderBehaviour.worker_health(
        opts,
        [pid: [:producer_pid, :owned_pid], absent: :no_producer, dead: :worker_dead],
        &health_from_stats/1
      )
    end

    defp health_from_stats(pid) do
      case ProducerWorker.stats(pid) do
        %{last_error: {:swap_exhausted, reason}} ->
          {:unhealthy, {:swap_exhausted, reason}}

        %{consecutive_failures: n} when n >= @stall_health_threshold ->
          {:unhealthy, :produce_stalled}

        %{} ->
          :healthy
      end
    end

    defp convert_value(field, value) when field in @int_fields, do: parse_int(value, field)
    defp convert_value(field, value) when field in @bool_fields, do: parse_bool(value, field)

    defp convert_value(:auth, %KafkaEx.Auth.Config{} = config), do: {:ok, config}

    defp convert_value(:auth, %{} = map),
      do: Enum.reduce_while(map, {:ok, %{}}, &put_auth_opt/2)

    defp convert_value(:auth, value), do: refuse_value(:auth, value)

    # Total, because `init_provider/1` reads the stored option through this
    # table: a value that is not a policy is refused at enable time rather than
    # reaching `overflow/4` and killing the worker when the queue first fills.
    defp convert_value(:queue_overflow, value),
      do: parse_enum(value, "queue_overflow", @queue_overflow_policies)

    # `KafkaEx.Auth.Config.new/1` takes only these four and `Map.take/2`s the
    # rest away, so a misspelled key is a missing credential rather than an
    # error — and a mechanism outside its own set is an `ArgumentError` raised
    # inside the closure `WorkerRegistry` re-invokes on every client restart.
    # Both sets are the dependency's, closed here, once, at preparation.
    defp put_auth_opt({key, value}, {:ok, acc}) do
      with {:ok, field} <- parse_enum(key, "auth key", @auth_keys),
           {:ok, value} <- auth_value(field, value) do
        {:cont, {:ok, Map.put(acc, field, value)}}
      else
        {:error, _} = err -> {:halt, err}
      end
    end

    defp auth_value(:mechanism, value), do: parse_enum(value, "auth mechanism", @auth_mechanisms)
    defp auth_value(_field, value), do: {:ok, value}

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
