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
    alias Flezha.Plugin.Provider.Kafka.ProducerWorker
    alias Flezha.Plugin.Provider.WorkerSup
    alias Flezha.Plugin.ProviderBehaviour
    alias Flezha.State.Outbox
    alias KafkaEx.Auth.Config, as: KafkaAuthConfig

    import ProviderBehaviour, only: [parse_enum: 3, refuse_value: 2]

    @queue_overflow_policies [:drop_nothing, :drop_oldest, :drop_newest]

    # The keys and the mechanisms `KafkaEx.Auth.Config` accepts.
    @auth_keys [:mechanism, :username, :password, :mechanism_opts]
    @auth_mechanisms [:plain, :scram, :oauthbearer, :msk_iam]

    use ProviderBehaviour,
      name: "kafka",
      options: [
        hosts: [
          type: :any,
          required: true,
          doc: "`{host, port}` list, or a comma-separated string"
        ],
        topic: [type: :string, required: true, doc: "Destination topic"],
        plugin_id: [type: :string, doc: "Plugin id; defaults to a generated one"],
        channel: [type: :string, doc: "Produce only records whose metadata channel matches"],
        partition: [type: :int, default: 0, min: 0, doc: "Destination partition"],
        required_acks: [type: :int, default: -1, doc: "Broker acks; -1 is acks=all"],
        retries: [type: :int, default: 5, min: 0, doc: "Attempts per batch before parking it"],
        retry_backoff_ms: [type: :int, default: 100, min: 1, doc: "First delay between retries"],
        retry_max_backoff_ms: [type: :int, default: 5_000, min: 1, doc: "Ceiling for that delay"],
        delivery_timeout_ms: [
          type: :int,
          default: 30_000,
          min: 1,
          doc: "Total budget for one batch before it is parked"
        ],
        linger_ms: [type: :int, default: 10, min: 0, doc: "How long a batch waits to fill"],
        max_batch_bytes: [type: :int, default: 1_048_576, min: 1, doc: "Largest batch produced"],
        queue_max: [
          type: :int,
          default: 10_000,
          min: 1,
          doc: "Records held before the policy applies"
        ],
        queue_overflow: [
          type: :enum,
          values: @queue_overflow_policies,
          default: :drop_nothing,
          doc: "What a full queue does: block, drop the oldest, or drop the newest"
        ],
        queue_block_timeout_ms: [
          type: :int,
          default: 5_000,
          min: 1,
          doc: "How long an enqueue parks on a full queue before it is refused"
        ],
        idempotent: [type: :bool, default: false, doc: "Opt in to the broker's producer session"],
        max_in_flight: [
          type: :int,
          min: 1,
          doc: "Batches in flight; derived as 5 with `idempotent`, else 1"
        ],
        auto_create_topic: [type: :bool, default: true, doc: "Create the topic when missing"],
        auto_create_partitions: [type: :int, default: 1, min: 1, doc: "Partitions for that topic"],
        auto_create_replication_factor: [
          type: :int,
          default: 2,
          min: 1,
          doc: "Replication factor for that topic"
        ],
        topic_configs: [type: :list, default: [], doc: "`{name, value}` pairs for that topic"],
        metadata_refresh_timeout_ms: [
          type: :int,
          default: 5_000,
          min: 1,
          doc: "Budget for one broker metadata refresh"
        ],
        request_timeout_ms: [
          type: :int,
          default: 10_000,
          min: 1,
          doc: "Budget the broker is asked to honour for one produce request"
        ],
        dispatch_timeout_ms: [
          type: :int,
          default: 10_000,
          min: 1,
          doc: "Budget for one in-flight batch before it is treated as lost"
        ],
        auth: [type: :any, doc: "SASL configuration, as a `KafkaEx.Auth.Config` or a map"],
        # The resolved credential, held in memory so the client start and every
        # reconnect on this node authenticate with the real secret while the
        # stored opts carry only the placeholder. Never persisted, so a handover
        # to another node resolves from `sasl_password_env` or from nothing,
        # which is what the page already says.
        auth_secret: [type: :any, internal: true, transient: true],
        sasl_password_env: [
          type: :string,
          doc: "Environment variable holding the SASL password, preferred over an inline one"
        ],
        use_ssl: [type: :bool, doc: "TLS to the brokers"],
        ssl_options: [type: :any, doc: "Erlang `:ssl` options for that connection"],
        metadata_update_interval: [
          type: :int,
          min: 1,
          doc: "How often the shared broker client refreshes metadata"
        ]
      ],
      worker: [
        module: ProducerWorker,
        restart: :permanent,
        reusable: {ProducerWorker, :reusable?}
      ]

    @impl true
    def init_provider(opts) do
      hosts = parse_hosts(opts.hosts)
      plugin_id = opts.plugin_id || "default-#{System.unique_integer([:positive])}"

      with :ok <- validate_window(opts) do
        {:ok,
         opts
         |> Map.merge(%{
           hosts: hosts,
           plugin_id: plugin_id,
           auth_secret: opts.auth_secret || opts.auth
         })
         |> redact_sasl()}
      end
    end

    @doc """
    One producer worker per `{plugin_id, topic, partition}`, which is what lets
    two plugins on one cluster share a broker client without sharing a queue.

    Every tunable is already converted and already carries its declared default,
    so this names only what the worker needs and the declaration cannot supply:
    the connection identity, and the window whose default follows `:idempotent`.
    `:auth` never appears: the credentials travel in the identity closure.
    """
    @impl true
    def worker_specs(opts) do
      [
        {key(opts),
         opts
         |> Map.take([
           :topic,
           :plugin_id,
           :partition,
           :required_acks,
           :retries,
           :retry_backoff_ms,
           :retry_max_backoff_ms,
           :delivery_timeout_ms,
           :dispatch_timeout_ms,
           :request_timeout_ms,
           :linger_ms,
           :max_batch_bytes,
           :queue_max,
           :queue_overflow,
           :queue_block_timeout_ms,
           :idempotent,
           :auto_create_topic,
           :auto_create_partitions,
           :auto_create_replication_factor,
           :topic_configs,
           :metadata_refresh_timeout_ms
         ])
         |> Map.merge(%{
           connection_identity: identity(opts),
           # 5 is Kafka's own ceiling for ordered idempotent retries; without a
           # producer session there is nothing to keep a retry in order.
           max_in_flight: opts.max_in_flight || if(opts.idempotent, do: 5, else: 1)
         })}
      ]
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
    defp identity(opts) do
      # The credential the closure resolves is the transient one, which the
      # stored opts do not carry: `resolve_auth/1` reads it and the redacted
      # `:auth` never reaches the client.
      opts = Map.put(opts, :auth, opts.auth_secret || opts.auth)
      resolved_auth = resolve_auth(opts)

      %{
        hosts: opts.hosts,
        ssl_options: opts.ssl_options,
        use_ssl: opts.use_ssl,
        auth_fun: fn -> resolve_auth(opts) end,
        auth_key: resolved_auth && :erlang.phash2(resolved_auth),
        metadata_update_interval: opts.metadata_update_interval,
        lane: rem(opts.partition, System.schedulers_online())
      }
    end

    # Only the broker's per-partition sequence check keeps a retry inside the
    # window ordered, so a window wider than one without idempotence would
    # trade the plugin's unconditional FIFO guarantee for throughput.
    defp validate_window(%{max_in_flight: window, idempotent: idempotent})
         when is_integer(window) and window > 1 and not idempotent do
      {:error,
       "kafka max_in_flight #{window} requires idempotent: only the broker's per-partition " <>
         "sequence check keeps a retry inside the window ordered"}
    end

    defp validate_window(_opts), do: :ok

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

    @impl true
    def on_input(%Ctx.IO{} = ctx, opts), do: produce(ctx, opts)
    @impl true
    def on_output(%Ctx.IO{} = ctx, opts), do: produce(ctx, opts)

    defp produce(%Ctx.IO{message: msg, metadata: md} = ctx, opts) do
      case opts.channel do
        nil ->
          do_produce(ctx, opts, msg, md)

        group ->
          if Map.get(md, "channel") == group,
            do: do_produce(ctx, opts, msg, md),
            else: {:ok, ctx}
      end
    end

    defp do_produce(%Ctx.IO{} = ctx, opts, msg, md) do
      # Take ownership of a delivery-guaranteed output before handing it over:
      # the worker answers the hold when the broker acks its batch, or parks it
      # when the retry ladder is permanently exhausted. A plain output holds
      # nothing and this is a no-op.
      Outbox.hold(md)
      :ok = ProducerWorker.cast_enqueue(worker(opts), msg, md)
      {:ok, ctx}
    end

    # The key one producer is registered under: a plugin instance owns exactly
    # one `(topic, partition)`, and this names it once for both the declaration
    # and the dispatch path.
    defp key(opts), do: {opts.plugin_id, opts.topic, opts.partition}

    defp worker(opts), do: WorkerSup.worker(__MODULE__, key(opts))

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

      case Registry.select(registry(), [{pattern, [], [:"$$"]}]) do
        [[topic, partition, pid] | _] ->
          # The Registry entry is unregistered asynchronously when the worker
          # exits, so we may see a stale pid here for a brief window after the
          # table stopped it.
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
      case Registry.lookup(registry(), {plugin_id, topic, partition}) do
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
        [absent: :no_producer, dead: :worker_dead],
        &health_from_stats/1
      )
    end

    defp registry(), do: WorkerSup.registry_name(WorkerSup.family(__MODULE__))

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

    defp convert_value(:auth, %KafkaEx.Auth.Config{} = config), do: {:ok, config}

    defp convert_value(:auth, %{} = map),
      do: Enum.reduce_while(map, {:ok, %{}}, &put_auth_opt/2)

    defp convert_value(:auth, value), do: refuse_value(:auth, value)

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
