# Kafka producer plugin

For operators attaching `Flezha.Plugin.Provider.Kafka.Producer` (provider id
`operative_kafka_producer`) and for anyone consuming what it writes. The plugin is an
`:on_input` / `:on_output` subscriber that publishes chain messages to one Kafka
`(topic, partition)` with retries, linger batching, a bounded in-process queue, an
optional idempotent producer session and a window of in-flight requests. This page is the
contract: what is retried, how effectively-once records are keyed, every parameter and
default, the overflow and confirmation semantics, telemetry, stats and health, the
durability trade-offs, and how broker clients are shared.

## Contract

The plugin:

- Retries every transient broker error — `:no_broker`, `:not_leader_for_partition`,
  `:network_exception`, `:request_timed_out`, `:not_enough_replicas`,
  `:not_enough_replicas_after_append`, `:replica_not_available`, `:leader_not_available`,
  `:broker_not_available`, `:closed`, `:correlation_mismatch` — until the batch is acked
  or its `:retries` / `:delivery_timeout_ms` budget is spent.
- Forces a metadata refresh before retrying `:not_leader_for_partition`, `:no_broker` and
  `:unknown_topic_or_partition`; on `:unknown_topic_or_partition` with
  `:auto_create_topic` it issues `CreateTopics` first.
- Fails a batch immediately on a permanent error: `:invalid_topic_exception`,
  `:invalid_topic`, `:record_list_too_large`, `:message_size_too_large`,
  `:offset_metadata_too_large`, `:topic_authorization_failed`,
  `:cluster_authorization_failed`.
- Cuts a batch at `:linger_ms` or `:max_batch_bytes`, whichever comes first, and only into
  a free slot of the `:max_in_flight` window; a batch that fails retriably halts new sends
  and is resent with everything behind it, in order, under its original sequence numbers.
- Caps buffering at `:queue_max` records and applies `:queue_overflow` when full —
  `:drop_nothing` by default, so nothing is dropped unless an operator chose a drop mode.
- Under `:idempotent = true` uses Produce v5+ with per-partition sequence numbers, acks a
  `:duplicate_sequence_number` as delivered, and on a fence
  (`:out_of_order_sequence_number`, `:invalid_producer_epoch`) re-initialises the producer
  session and resends rather than dropping.
- Emits a `:telemetry` event under `[:flezha, :plugin, :kafka, _, _]` for every transition.

Chain callers never see a transient reason as a raw `{:error, _}`; a chain-dispatched
record reports only through telemetry (below), and a synchronous `enqueue/4` caller gets
`{:error, {:delivery_failed, last_reason}}` once the budget is exhausted or
`{:error, {:permanent, reason}}` on a permanent error.

## Effectively-once records

Records from an `effectively_once()` operative carry the outbox identity stamps
`flz.entity`, `flz.seq` (`<record WAL seq>.<emit ordinal>`) and `flz.epoch`. For them the
plugin uses `flz.key` (the emitting Entity's key, or `emit_keyed`'s) as the Kafka message
key — stable partitioning across replays, latest-value semantics on compacted topics — and
writes the full identity as the record header
`flezha.dedup_key = <flz.entity>/<outbox seq>/<epoch>` (e.g. `MarketOperative/s1/k1/42.0/1`).
Records without the stamps use `flz.key`, else a caller-stamped `flezha.dedup_key`, else
`kafka_msg_key` as the message key and carry no header.

**The exactness contract.** The client has no transactional producer, so the plugin does
not claim broker-side exactly-once. A controller restart re-drives unflushed outputs with
identical key, header and partition:

- **Compacted / changelog topics** (candles, indicators, any latest-value stream):
  re-driven records upsert the same key; compaction converges to one value per key.
- **Append-only topics**: a re-drive can append the same logical record twice; exact-once
  consumption means deduplicating on the `flezha.dedup_key` header, which is unique per
  logical output and identical across re-drives.

## Parameters

<!-- provider-options:begin -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `hosts` | any | &mdash; | yes | `{host, port}` list, or a comma-separated string |
| `topic` | string | &mdash; | yes | Destination topic |
| `plugin_id` | string | &mdash; | no | Plugin id; defaults to a generated one |
| `channel` | string | &mdash; | no | Produce only records whose metadata channel matches |
| `partition` | int | `0` | no | Destination partition |
| `required_acks` | int | `-1` | no | Broker acks; -1 is acks=all |
| `retries` | int | `5` | no | Attempts per batch before parking it |
| `retry_backoff_ms` | int | `100` | no | First delay between retries |
| `retry_max_backoff_ms` | int | `5000` | no | Ceiling for that delay |
| `delivery_timeout_ms` | int | `30000` | no | Total budget for one batch before it is parked |
| `linger_ms` | int | `10` | no | How long a batch waits to fill |
| `max_batch_bytes` | int | `1048576` | no | Largest batch produced |
| `queue_max` | int | `10000` | no | Records held before the policy applies |
| `queue_overflow` | enum | `:drop_nothing` | no | What a full queue does: block, drop the oldest, or drop the newest |
| `queue_block_timeout_ms` | int | `5000` | no | How long an enqueue parks on a full queue before it is refused |
| `idempotent` | bool | `false` | no | Opt in to the broker's producer session |
| `max_in_flight` | int | &mdash; | no | Batches in flight; derived as 5 with `idempotent`, else 1 |
| `auto_create_topic` | bool | `true` | no | Create the topic when missing |
| `auto_create_partitions` | int | `1` | no | Partitions for that topic |
| `auto_create_replication_factor` | int | `2` | no | Replication factor for that topic |
| `topic_configs` | list | `[]` | no | `{name, value}` pairs for that topic |
| `metadata_refresh_timeout_ms` | int | `5000` | no | Budget for one broker metadata refresh |
| `request_timeout_ms` | int | `10000` | no | Budget the broker is asked to honour for one produce request |
| `dispatch_timeout_ms` | int | `10000` | no | Budget for one in-flight batch before it is treated as lost |
| `auth` | any | &mdash; | no | SASL configuration, as a `KafkaEx.Auth.Config` or a map |
| `sasl_password_env` | string | &mdash; | no | Environment variable holding the SASL password, preferred over an inline one |
| `use_ssl` | bool | &mdash; | no | TLS to the brokers |
| `ssl_options` | any | &mdash; | no | Erlang `:ssl` options for that connection |
| `metadata_update_interval` | int | &mdash; | no | How often the shared broker client refreshes metadata |
<!-- provider-options:end -->

Passwords are redacted in the persisted and returned opts.

### Sizing `:queue_max`

`queue_max = expected rate × longest outage in seconds × headroom` — 50 msg/s absorbing a
60 s leader election with 2× headroom is 6 000. A batch leaves the queue when it is sent,
so the window holds up to `:max_in_flight × :max_batch_bytes` beyond the queue — 5 MiB per
partition at the defaults.

### `:max_in_flight`

A broker answers one connection's requests in order, and under idempotence it rejects a
batch whose sequence numbers do not continue the last one it stored. Those two facts let
several Produce requests be outstanding without losing partition order: a failed batch
halts new sends, waits for every batch behind it to come back, and goes out again with all
of them in the original order under the original sequence numbers. Nothing preserves that
order without idempotence, so a wider window without `:idempotent` is refused.

### `:queue_overflow`

- **`:drop_nothing`** (default) — back-pressure instead of loss. A sync `enqueue/4` caller waits
  out `:queue_block_timeout_ms`; a chain-dispatched record waits for room without blocking
  the process that enqueued it, so the pressure lands on the outbox flush that produced
  it rather than on ingest.
- **`:drop_oldest`** — keeps recency: the oldest queued record is evicted. Suits market
  data where stale prices are useless.
- **`:drop_newest`** — keeps order: new arrivals are rejected until space frees. Suits
  audit streams where the prefix matters.

Any other value is refused at preparation — `{:error, "Invalid value for queue_overflow: …"}`
naming the offered value — so a misspelled policy is a plugin that does not enable rather
than one that serves until its queue fills.

Every drop is counted in `stats/1`'s `dropped_total` and emits
`[:flezha, :plugin, :kafka, :produce, :dropped]`. Dropping a delivery-guaranteed record is
alarm-grade: it logs at `error`, tags the event `guaranteed: true`, and leaves the record's
outbox cursor where it is, so the loss shows as an outbox that stops draining until the
entry is re-driven.

### Delivery confirmation

`on_input/2` / `on_output/2` take ownership of a delivery-guaranteed record
(`Flezha.State.Outbox.hold/1`) before handing it to the worker. An acked batch confirms
every guaranteed record it carried in one report per owning writer
(`Outbox.confirm_many/1`); a batch that exhausts its budget parks each of them through the
dead-letter store under its own `(entity, seq, epoch)` identity (`Outbox.park/4`, one per
record because a park carries the payload), so the cursor advances past a recorded,
replayable loss rather than a silent one. Records without the `flz.ack` token settle nothing.

## Chain dispatch is non-blocking

`on_input/2` and `on_output/2` return `{:ok, ctx}` in O(1) relative to broker latency:
they `cast` to the per-`(plugin_id, topic, partition)` `ProducerWorker` and return. The
bounded queue, not the host's mailbox, is the back-pressure boundary.

| Function | Caller | Return | Failure surfacing |
| --- | --- | --- | --- |
| `ProducerWorker.cast_enqueue/3` | chain plugins | `:ok` immediately | telemetry; outbox park for guaranteed records |
| `ProducerWorker.enqueue/4` | tests, tooling, explicit-flush sites | `:ok` after the broker ack, or `{:error, _}` | return value (default `timeout: 35_000`) |

A chain-admitted record that ultimately fails emits one
`[:flezha, :plugin, :kafka, :produce, :failed]` per record with `caller: :async`. Telemetry
is the channel by which operators observe permanent failures of chain-admitted records —
not logs, not the chain return value:

```elixir
:telemetry.attach(
  "my-app-kafka-failures",
  [:flezha, :plugin, :kafka, :produce, :failed],
  fn
    _name, measurements, %{caller: :async} = metadata, _ ->
      MyApp.Alerts.kafka_delivery_failed(metadata.topic, metadata.last_reason, measurements)

    _name, _measurements, _metadata, _ ->
      :ok
  end,
  nil
)
```

For a per-message synchronous reply (a load-test harness, say), call the worker directly:

```elixir
alias Flezha.Plugin.Provider.Kafka.{ProducerSup, ProducerWorker}

[{producer_pid, _}] =
  Registry.lookup(ProducerSup.registry_name(), {plugin_id, topic, partition})

case ProducerWorker.enqueue(producer_pid, msg, metadata, timeout: 35_000) do
  :ok -> :ok
  {:error, {:delivery_failed, reason}} -> handle_failure(reason)
end
```

## Telemetry

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:flezha, :plugin, :kafka, :produce, :ok]` | `%{batch_size, latency_ms}` | `%{plugin_id, topic, partition, base_offset, attempt}` |
| `[:flezha, :plugin, :kafka, :produce, :retry]` | `%{attempt, backoff_ms}` | `%{plugin_id, topic, partition, reason}` |
| `[:flezha, :plugin, :kafka, :produce, :dropped]` | `%{count}` | `%{plugin_id, topic, partition, queue_depth, overflow_strategy, guaranteed, reason}` |
| `[:flezha, :plugin, :kafka, :produce, :failed]` | `%{attempts, total_ms}` | `%{plugin_id, topic, partition, last_reason, caller}` |
| `[:flezha, :plugin, :kafka, :metadata, :refresh]` | `%{latency_ms}` | `%{plugin_id, topic, partition, trigger, error}` |
| `[:flezha, :plugin, :kafka, :topic, :created]` | `%{partitions, replication_factor}` | `%{plugin_id, topic}` |
| `[:flezha, :plugin, :kafka, :worker, :swapped]` | `%{queue_depth, in_flight_records, queue_bytes}` | `%{plugin_id, topic, partition, reason, producer_id_before, producer_id_after}` |

A quick Logger handler for installations without a metrics stack:

```elixir
:telemetry.attach_many(
  "flezha-kafka-producer-log",
  Enum.map([[:produce, :ok], [:produce, :retry], [:produce, :dropped], [:produce, :failed],
            [:metadata, :refresh], [:topic, :created], [:worker, :swapped]],
           &([:flezha, :plugin, :kafka] ++ &1)),
  fn name, measurements, metadata, _ ->
    Logger.info("[kafka.producer] #{inspect(name)} m=#{inspect(measurements)} md=#{inspect(metadata)}")
  end,
  nil
)
```

## Stats and health

```elixir
{:ok, stats} = Flezha.Plugin.Provider.Kafka.Producer.stats(plugin_id)
# %{queue_depth: 0, in_flight: 0, dropped_total: 4, last_error: nil,
#   consecutive_failures: 0, topic: "trades", partition: 0}
```

`stats/1` returns `{:error, :no_producer}` when no worker is registered for the plugin id;
`stats/3` takes an explicit `(plugin_id, topic, partition)`.

`health/1` reports `{:unhealthy, :produce_stalled}` after 3 consecutive failed batches
that a fresh broker client did not clear, and `{:unhealthy, {:swap_exhausted, reason}}`
when the worker has given up replacing its client; either escalates to a plugin restart by
the convergence loop. A single batch failing with a reason that means the client has no
working broker connection (`:no_broker`, `:broker_not_available`, `:network_exception`,
`:leader_not_available`, `:closed`, `:correlation_mismatch`) forces a client replacement
immediately, throttled to one per 2 s.

## Durability trade-offs

- `:required_acks = -1` (default) waits for all in-sync replicas. With
  `min.insync.replicas = 1` the steady-state latency equals `acks = 1`; the difference
  shows only when the ISR shrinks. Combined with idempotence, written once and stays written.
- `:required_acks = 1` waits for the leader only: faster, but a leader crash before
  replication loses the message. For derived, regenerable streams.
- `:required_acks = 0` is fire-and-forget. Only for data that is cheap to replay.
- Producer-id rotation (a worker restart or a client swap) may duplicate the whole in-flight
  window — up to `:max_in_flight` batches. `:max_in_flight = 1` restores a one-batch bound.
- The fast-but-lossy profile is an explicit choice:
  `%{required_acks: 1, idempotent: false, retries: 0}`.

## Worker sharing

Plugin instances with the same connection identity (`:hosts`, SSL options, auth) share
`KafkaEx.Client` processes through `Flezha.Plugin.Provider.Kafka.WorkerRegistry`, at most
one per scheduler per cluster: a client encodes each batch in its own process, so one shared
client would serialise every partition's encoding on one core. Partitions are spread by
`partition mod schedulers_online`. The registry refcounts owners and stops a client 5 s
after its last owner releases it, which bounds connections by the node's cores rather than
the plugin count. The client pipelines requests, so the lane buys encode parallelism, not
wire concurrency.

Each plugin instance still owns its own `ProducerWorker` per `(topic, partition)` — its own
queue, producer id and sequence counter. A worker restart rewinds the in-flight window to
the head of the queue; the duplicate bound is the window, as above.

## CLI

```bash
flz plugin enable trades-producer operative_kafka_producer \
  --Xhosts=kafka:9092 \
  --Xtopic=trades \
  --Xqueue_max=20000 \
  --Xlinger_ms=20 \
  --Xqueue_overflow=drop_oldest
```

## See also

- [../resilience/data_durability.md](../resilience/data_durability.md) — the outbox, confirmation and the effectively-once tier
- [lifecycle.md](lifecycle.md) — how the producer's worker fits the provider lifecycle
- [../configuration.md](../configuration.md) — plugin options as a configuration layer
