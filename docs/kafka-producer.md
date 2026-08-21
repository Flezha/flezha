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
  `:queue_overflow` strategy when saturated — `:block` by default, so no
  record is dropped unless an operator configured that explicitly.
* Use idempotent producer semantics (Produce v5+ with per-`(topic, partition)`
  sequence numbers) when `:idempotent = true` (opt-in; default `false`) —
  retries cannot duplicate messages on the broker. On a broker-induced fence
  (`out_of_order_sequence_number` / `invalid_producer_epoch`) the worker
  re-inits the producer session and resends rather than dropping the batch.
* Surface every state transition as a `:telemetry` event under the
  `[:flezha, :plugin, :kafka, _, _]` namespace.

The plugin SHALL NOT pass any of `:no_broker | :not_leader_for_partition |
:unknown_topic_or_partition | :network_exception | :request_timed_out |
:not_enough_replicas` to the chain caller as a raw `{:error, reason}` —
those are retried internally. After the timeout budget is exhausted, the
chain receives `{:error, {:delivery_failed, last_reason}}`. Chain code that
hits `{:error, _}` from this plugin is observing a real failure, not a
transient broker burp.

## Effectively-once tier: message keys, identity headers, and the exactness contract

Records produced by an `effectively_once()` operative arrive with the
durable outbox identity stamps (`flz.entity`, `flz.seq` = the outbox seq
`<record WAL seq>.<emit ordinal>`, `flz.epoch`). For those records the
plugin:

* uses **`flz.key`** (the emitting Entity's key, or `emit_keyed`'s) as the Kafka message
  key — stable partitioning across replays, latest-value-per-key semantics
  on compacted topics;
* writes the full identity as the record header
  **`flezha.dedup_key = <flz.entity>/<outbox_seq>/<epoch>`** (e.g.
  `MarketOperative/s1/k1/42.0/1`).

Records without the tier stamps keep the pre-tier behaviour: the
caller-supplied `kafka_msg_key` (or a caller-stamped `flezha.dedup_key`
metadata value) is the message key and no header is written.

**The exactness contract.** The Kafka client has no transactional
producer, so the plugin does not — and cannot — claim broker-side
exactly-once. What the tier guarantees is that a controller restart
re-drives unflushed outputs with *identical* key, header, and partition;
"coordination leaves your system, not the universe":

* **Changelog / compacted topics** (the recommended shape for candles,
  indicators, any latest-value stream): re-driven records upsert the same
  message key, so compaction converges to exactly one visible value per
  key. Consumers need nothing.
* **Append-only topics**: a re-drive can append the same logical record
  twice. Exact-once *consumption* requires the consumer to dedup on the
  `flezha.dedup_key` header — the value is unique per logical output and
  identical across every re-drive.

## Parameters

| Parameter | Default | Type | Notes |
|---|---|---|---|
| `:hosts` | required | `[{host, port}]` or `"host:port,host:port"` | bootstrap brokers |
| `:topic` | required | `binary` | destination topic |
| `:partition` | `0` | `int` | one plugin instance serves one `(topic, partition)` |
| `:channel` | `nil` | `binary` | gate produces by metadata `"channel"` tag |
| `:required_acks` | **`-1` (was `1`)** | `int` | `-1`/`0`/`1`/`>1`. Idempotence makes `acks=all` safe. |
| `:retries` | `5` | `int` | per-batch attempt cap |
| `:retry_backoff_ms` | `100` | `int` | initial backoff; doubles per attempt |
| `:retry_max_backoff_ms` | `5_000` | `int` | backoff ceiling |
| `:delivery_timeout_ms` | `30_000` | `int` | total per-message budget incl. retries |
| `:linger_ms` | `10` | `int` | batch window. `0` disables batching. |
| `:max_batch_bytes` | `1_048_576` | `int` | flush early at 1 MiB by default |
| `:queue_max` | `10_000` | `int` | per-`(plugin, topic, partition)` queue depth |
| `:queue_overflow` | `:block` | atom | `:block` / `:drop_oldest` / `:drop_newest`; a drop strategy is always an explicit choice |
| `:queue_block_timeout_ms` | `5_000` | `int` | only used when `:queue_overflow = :block` |
| `:idempotent` | `false` | `bool` | opt-in exactly-once; requires Kafka 0.11+; gracefully falls back to non-idempotent if `InitProducerId` fails |
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

The default is `:block` for every record, guaranteed or not: a saturated
queue back-pressures the chain that feeds it (visibly slower ingest) instead
of discarding output (invisibly fewer records). A drop mode is a stated
choice, and every drop it causes is counted in `stats/1`'s `dropped_total`.

* **`:drop_oldest`** — preserves *recency*. After an outage the queue holds
  the most-recent N messages; the oldest are dropped first. Right for
  streaming market data where stale prices are useless.
* **`:drop_newest`** — preserves *order*. Once full, new arrivals are
  rejected until space frees up. Right for audit-log streams where
  preserving the prefix matters.
* **`:block`** — back-pressure instead of loss. A sync `enqueue/4` caller
  waits out `:queue_block_timeout_ms`; a chain-dispatched record waits for
  room without ever blocking the process that enqueued it, so the pressure
  lands on the outbox flush that produced it rather than on ingest.

Dropping a delivery-guaranteed record under an explicitly configured `drop_*`
is alarm-grade: it logs at `error` and tags its
`[:flezha, :plugin, :kafka, :produce, :dropped]` event `guaranteed: true`. The
record's outbox cursor does not advance, so the loss shows up as an outbox that
stops draining until the entry is re-driven.

### Delivery confirmation

A record carrying `flz.ack` is held by the producer on behalf of the
controller's outbox cursor. The worker answers that hold at its existing
per-batch completion point: an acked batch **confirms** every guaranteed record
it carried in ONE report to each owning writer (`Outbox.confirm_many/1`), and a
batch that exhausts its retry budget **parks** them through the dead-letter
store with their own `(entity, seq, epoch)` identity — one park per record,
since a park carries the payload — so the cursor advances past a recorded,
replayable loss rather than a silent one. Records without the token belong to a
plain chain and confirm nothing.

## Chain dispatch is non-blocking

`Flezha.Plugin.Provider.Kafka.Producer.on_input/2` and `on_output/2` SHALL
return `{:ok, ctx}` to the chain in O(1) wall-clock time relative to
broker latency. They are implemented as `GenServer.cast` to the per-(
plugin_id, topic, partition) `ProducerWorker` and return immediately
after handing the message to the worker's mailbox.

This is what makes the bounded queue the back-pressure boundary. With a
synchronous chain dispatch, the host process (e.g.
`Flezha.Operative.Overwatch`) blocks on every event for the full broker
round-trip; under sustained input above the broker drain rate, the host's
mailbox grows without bound long before the worker's queue ever sees
pressure. With cast-based admission the host stays drainable; the worker's
bounded queue absorbs short outages; during a sustained one the configured
`:queue_overflow` decides — `:block` holds the record until a batch drains,
a drop mode sheds — and the BEAM is never OOM-killed either way.

Two API surfaces, two contracts:

| Function | Caller | Return | Failure surfacing |
|---|---|---|---|
| `ProducerWorker.cast_enqueue/3` | chain plugins (`on_input` / `on_output`) | `:ok` immediately | telemetry only |
| `ProducerWorker.enqueue/4` | tests, ad-hoc tooling, explicit-flush call sites | `:ok` after broker ack OR `{:error, _}` after retries exhausted | return value |

Async-admitted records that ultimately fail emit a per-record
`[:flezha, :plugin, :kafka, :produce, :failed]` event with `caller:
:async` in metadata. Telemetry is the canonical channel by which
runners observe permanent delivery failures of chain-admitted records;
logs are not the channel; the chain return value is not the channel.

### Migration from synchronous error returns

Pre-change, a chain caller could observe `{:error, {:delivery_failed,
reason}}` from `on_input/2` / `on_output/2` when a message exceeded
`:delivery_timeout_ms`. After this change, the chain receives `{:ok,
ctx}` immediately and the failure surfaces only via telemetry.

If you previously inspected the synchronous error reply for
control-flow, attach a telemetry handler instead:

```elixir
:telemetry.attach(
  "my-app-kafka-failures",
  [:flezha, :plugin, :kafka, :produce, :failed],
  fn _name, measurements, metadata, _ ->
    case metadata.caller do
      :async ->
        # Chain-admitted record: observe and react (alert, dead-letter, etc.)
        MyApp.Alerts.kafka_delivery_failed(metadata.topic, metadata.last_reason, measurements)

      :sync ->
        # Sync caller already received `{:error, _}`; this event is for parity.
        :ok
    end
  end,
  nil
)
```

If you genuinely need the synchronous reply (e.g., a load-test harness
whose assertions depend on per-message delivery confirmation), call
`ProducerWorker.enqueue/4` directly against the registered worker pid:

```elixir
alias Flezha.Plugin.Provider.Kafka.{ProducerSup, ProducerWorker}

[{producer_pid, _}] =
  Registry.lookup(ProducerSup.registry_name(), {plugin_id, topic, partition})

case ProducerWorker.enqueue(producer_pid, msg, metadata, timeout: 35_000) do
  :ok -> :ok
  {:error, {:delivery_failed, reason}} -> handle_failure(reason)
end
```

## Telemetry events

Every state transition emits a `:telemetry.execute/3` event under the
`[:flezha, :plugin, :kafka, _, _]` prefix. The events ARE the runner API
for monitoring producer health — runners integrating with
`:telemetry_metrics_prometheus_core` get every event as a Prometheus metric
without any code change.

| Event | Measurements | Metadata |
|---|---|---|
| `[:flezha, :plugin, :kafka, :produce, :ok]` | `%{batch_size, latency_ms}` | `%{topic, partition, plugin_id, base_offset, attempt}` |
| `[:flezha, :plugin, :kafka, :produce, :retry]` | `%{attempt, backoff_ms}` | `%{topic, partition, plugin_id, reason}` |
| `[:flezha, :plugin, :kafka, :produce, :dropped]` | `%{count}` | `%{topic, partition, plugin_id, queue_depth, overflow_strategy, reason}` |
| `[:flezha, :plugin, :kafka, :produce, :failed]` | `%{attempts, total_ms}` | `%{topic, partition, plugin_id, last_reason, caller}` |
| `[:flezha, :plugin, :kafka, :metadata, :refresh]` | `%{latency_ms}` | `%{trigger, error, plugin_id, topic, partition}` |
| `[:flezha, :plugin, :kafka, :topic, :created]` | `%{partitions, replication_factor}` | `%{topic, plugin_id}` |

### Quick-attach Logger handler

For runners without a metrics stack, this one-line handler logs every
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

Plugin instances configured against the same `:hosts` (and SSL + auth opts)
share underlying `KafkaEx.New.Client` connections via
`Flezha.Plugin.Provider.Kafka.WorkerRegistry`, at most one per scheduler per
cluster: a client answers one request at a time and encodes each batch in its
own process, so a single shared client would serialise every partition's
batches behind one core. Partitions are spread over the clients by
`partition mod schedulers`. The registry refcounts owners; a worker is
stopped 5 s after its last owner releases. This bounds the connection count
by the node's cores instead of by the plugin count.

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
* `:idempotent` is `false` by default — opt in with `idempotent: true` for exactly-once
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
