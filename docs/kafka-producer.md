# Kafka Producer Plugin

`Flezha.Plugin.Provider.Kafka.Producer` is the first-party `:on_input` /
`:on_output` transform-subscriber that publishes chain messages to a Kafka
topic. It is hardened for streaming production workloads: transient broker
errors are retried transparently, messages are batched, idempotence is on
by default, and the in-process queue is bounded to keep the BEAM out of
OOM trouble during sustained Kafka outages.

If you previously used the thin `KafkaEx.produce`-per-call wrapper, see the
[Migration](#migration) section below — the most likely change you need is
to *do nothing* (the new defaults are durability-correct), but the default
`:required_acks` shift from `1` to `-1` is BREAKING for callers that
relied on single-broker-ack latency.

## Contract

The plugin SHALL:

* Retry every transient broker error (`:no_broker`, `:not_leader_for_partition`,
  `:network_exception`, `:request_timed_out`, `:not_enough_replicas`,
  `:not_enough_replicas_after_append`, `:replica_not_available`,
  `:leader_not_available`, `:broker_not_available`) until either delivery
  succeeds or `:delivery_timeout_ms` elapses.
* Force a metadata refresh before retrying `:not_leader_for_partition` /
  `:no_broker` / `:unknown_topic_or_partition`.
* Auto-create the destination topic on first publish when
  `:auto_create_topic = true` (default), with a shape controlled by
  `:auto_create_partitions` and `:auto_create_replication_factor`.
* Batch messages per `:linger_ms` window or until `:max_batch_bytes` is
  reached, whichever comes first.
* Cap in-process buffering at `:queue_max` items, applying the
  `:queue_overflow` strategy when saturated.
* Use idempotent producer semantics (Produce v5+ with per-`(topic, partition)`
  sequence numbers) when `:idempotent = true` (default) — retries cannot
  duplicate messages on the broker.
* Surface every state transition as a `:telemetry` event under the
  `[:flezha, :plugin, :kafka, _, _]` namespace.

The plugin SHALL NOT pass any of `:no_broker | :not_leader_for_partition |
:unknown_topic_or_partition | :network_exception | :request_timed_out |
:not_enough_replicas` to the chain caller as a raw `{:error, reason}` —
those are retried internally. After the timeout budget is exhausted, the
chain receives `{:error, {:delivery_failed, last_reason}}`. Chain code that
hits `{:error, _}` from this plugin is observing a real failure, not a
transient broker burp.

## Parameters

| Parameter | Default | Type | Notes |
|---|---|---|---|
| `:hosts` | required | `[{host, port}]` or `"host:port,host:port"` | bootstrap brokers |
| `:topic` | required | `binary` | destination topic |
| `:partition` | `0` | `int` | overridden per-message via metadata `"kafka_msg_partition"` |
| `:plugin_group` | `nil` | `binary` | gate produces by metadata `"plugin_group"` tag |
| `:required_acks` | **`-1` (was `1`)** | `int` | `-1`/`0`/`1`/`>1`. Idempotence makes `acks=all` safe. |
| `:retries` | `5` | `int` | per-batch attempt cap |
| `:retry_backoff_ms` | `100` | `int` | initial backoff; doubles per attempt |
| `:retry_max_backoff_ms` | `5_000` | `int` | backoff ceiling |
| `:delivery_timeout_ms` | `30_000` | `int` | total per-message budget incl. retries |
| `:linger_ms` | `10` | `int` | batch window. `0` disables batching. |
| `:max_batch_bytes` | `1_048_576` | `int` | flush early at 1 MiB by default |
| `:queue_max` | `10_000` | `int` | per-`(plugin, topic, partition)` queue depth |
| `:queue_overflow` | `:drop_oldest` | atom | `:drop_oldest` / `:drop_newest` / `:block` |
| `:queue_block_timeout_ms` | `5_000` | `int` | only used when `:queue_overflow = :block` |
| `:idempotent` | `true` | `bool` | requires Kafka 0.11+; gracefully falls back to non-idempotent if `InitProducerId` fails |
| `:auto_create_topic` | `true` | `bool` | issues `CreateTopics` on first `:unknown_topic_or_partition` |
| `:auto_create_partitions` | `1` | `int` | partition count at auto-create |
| `:auto_create_replication_factor` | `2` | `int` | RF at auto-create |
| `:topic_configs` | `[]` | `[{name, value}]` | extra topic configs (e.g. retention.ms) |
| `:metadata_refresh_timeout_ms` | `5_000` | `int` | bound on active metadata refresh |

### Sizing `:queue_max`

`:queue_max = expected_rate × max_outage_seconds × safety_factor`. For
example, a 50 trades/sec/pair stream that should absorb a 60 s leader
election with 2× headroom: `50 × 60 × 2 = 6000`. The default of `10_000`
covers most market-data streams; bump it for higher-volume pairs (e.g.
500 msg/sec → `60_000`).

### `:queue_overflow` modes

* **`:drop_oldest`** (default) — preserves *recency*. After an outage the
  queue holds the most-recent N messages; the oldest are dropped first.
  Right for streaming market data where stale prices are useless.
* **`:drop_newest`** — preserves *order*. Once full, new arrivals are
  rejected until space frees up. Right for audit-log streams where
  preserving the prefix matters.
* **`:block`** — synchronous back-pressure to the chain caller. The
  `on_input`/`on_output` call blocks up to `:queue_block_timeout_ms` until
  space is available. Right for callers that can absorb back-pressure
  upstream (e.g. a websocket reader that stops draining its socket).

## Telemetry events

Every state transition emits a `:telemetry.execute/3` event under the
`[:flezha, :plugin, :kafka, _, _]` prefix. The events ARE the operator API
for monitoring producer health — operators integrating with
`:telemetry_metrics_prometheus_core` get every event as a Prometheus metric
without any code change.

| Event | Measurements | Metadata |
|---|---|---|
| `[:flezha, :plugin, :kafka, :produce, :ok]` | `%{batch_size, latency_ms}` | `%{topic, partition, plugin_id, base_offset, attempt}` |
| `[:flezha, :plugin, :kafka, :produce, :retry]` | `%{attempt, backoff_ms}` | `%{topic, partition, plugin_id, reason}` |
| `[:flezha, :plugin, :kafka, :produce, :dropped]` | `%{count}` | `%{topic, partition, plugin_id, queue_depth, overflow_strategy, reason}` |
| `[:flezha, :plugin, :kafka, :produce, :failed]` | `%{attempts, total_ms}` | `%{topic, partition, plugin_id, last_reason}` |
| `[:flezha, :plugin, :kafka, :metadata, :refresh]` | `%{latency_ms}` | `%{trigger, error, plugin_id, topic, partition}` |
| `[:flezha, :plugin, :kafka, :topic, :created]` | `%{partitions, replication_factor}` | `%{topic, plugin_id}` |

### Quick-attach Logger handler

For operators without a metrics stack, this one-line handler logs every
producer event:

```elixir
:telemetry.attach_many(
  "flezha-kafka-producer-log",
  [
    [:flezha, :plugin, :kafka, :produce, :ok],
    [:flezha, :plugin, :kafka, :produce, :retry],
    [:flezha, :plugin, :kafka, :produce, :dropped],
    [:flezha, :plugin, :kafka, :produce, :failed],
    [:flezha, :plugin, :kafka, :metadata, :refresh],
    [:flezha, :plugin, :kafka, :topic, :created]
  ],
  fn name, measurements, metadata, _ ->
    Logger.info("[kafka.producer] #{inspect(name)} m=#{inspect(measurements)} md=#{inspect(metadata)}")
  end,
  nil
)
```

## Synchronous stats

```elixir
{:ok, stats} = Flezha.Plugin.Provider.Kafka.Producer.stats(plugin_id)
# %{dropped_total: 4, queue_depth: 0, in_flight: 0, last_error: nil,
#   topic: "trades", partition: 0}
```

Returns `{:error, :no_producer}` if no producer worker is registered for
the given plugin id (plugin not running, or already torn down).

## Durability tradeoffs

* `:required_acks = -1` (default) waits for all in-sync replicas to ack.
  On a cluster with `min.insync.replicas = 1` this is equivalent latency
  to `acks = 1` in steady state — the difference only shows up when the
  ISR shrinks. With idempotence on, `acks=-1` is the durability-correct
  default; messages are written once and stay written.
* `:required_acks = 1` waits for the leader-broker write only. Faster
  steady-state latency, but a leader crash before replication loses the
  message. Appropriate only for derived/regenerable streams.
* `:required_acks = 0` is fire-and-forget. The broker doesn't ack at all;
  the producer assumes success. Only appropriate when the data is
  cheap-to-replay (e.g. derived ticker stats from a separately-persisted
  trades stream).

## Worker sharing

Multiple plugin instances configured against the same `:hosts` (and SSL +
auth opts) share a single underlying `KafkaEx.New.Client` connection pool
via `Flezha.Plugin.Provider.Kafka.WorkerRegistry`. The registry refcounts
owners; the underlying worker is stopped 5 s after the last owner releases.
This eliminates the "one-worker-per-plugin" cardinality blowup.

Each plugin instance still owns its own per-`(topic, partition)`
`ProducerWorker` (and therefore its own bounded queue, producer-id, and
sequence counter). On a producer-worker restart, in-flight unacked
messages may produce duplicates equal to the in-flight batch size at
restart — the bounded queue limits this to at most one batch.

## Migration

### From the legacy thin wrapper

If your previous configuration looked like:

```elixir
%{hosts: "kafka:9092", topic: "trades", partition: 0}
```

…you get the new behavior **automatically**:

* `:required_acks` is now `-1` (was `1`)
* `:idempotent` is now `true`
* Retries handle transient broker unavailability up to 30 s
* The destination topic is auto-created on first publish

If you depended on the old fast-but-lossy latency profile (single broker
ack, no retries, no idempotence), opt back in explicitly:

```elixir
%{
  hosts: "kafka:9092",
  topic: "ticker_stats",
  partition: 0,
  required_acks: 1,
  idempotent: false,
  retries: 0
}
```

If you relied on `KafkaEx.create_worker` returning a per-plugin worker pid
(third element of `init_provider/1` was the kafka_ex pid), note that the
third element is now the `ProducerWorker` pid. The kafka_ex worker pid is
shared via `WorkerRegistry` and is internal — code that needed direct
access to `KafkaEx.produce` should call `Producer.stats/1` or attach a
telemetry handler instead.

### CLI invocation

All new opts are X-prefixed for the CLI installer:

```bash
flz plugin enable trades-producer kafka_producer \
  --Xhosts=kafka:9092 \
  --Xtopic=trades \
  --Xqueue_max=20000 \
  --Xlinger_ms=20 \
  --Xqueue_overflow=drop_oldest
```
