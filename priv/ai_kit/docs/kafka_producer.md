# Kafka producer plugin

The Kafka producer plugin publishes what a relay ingests or emits to a Kafka topic. It
hooks `:on_input` or `:on_output` and publishes chain messages to the topic, choosing each
record's partition by its key, with retries, linger batching, gzip compression, a bounded
in-process queue per partition, an idempotent producer session per partition and a window
of in-flight requests. `FlezhaKafka.Producer` is its provider, under the provider id
`kafka_producer`. This page is for users attaching the producer and for anyone consuming
what it writes: what is retried, how records are keyed, partitioned and headed, every
option and default, the overflow and confirmation semantics, telemetry, stats and health,
the durability trade-offs, and how broker clients are shared.

| | |
| --- | --- |
| Type | relay plugin |
| Ships | package `flezha_kafka` |
| Shareable | yes: every relay bound to it publishes through one producer per controller, or one in the cluster with `--single` |
| Partitioned relays | allowed: each member runs a producer of its own |
| Durability | a delivery-guaranteed output is confirmed when its batch is acked and parked as `kafka.exhausted` when it is given up; an effectively-once relay's records carry a `flezha.dedup_key` header |

## Options

<!-- provider-options:begin -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `hosts` | names | none | yes | `host:port` names, comma-separated or as a list |
| `auth` | map | none | no | SASL configuration, as a `KafkaEx.Auth.Config` or a map |
| `password` | string | none | no | The SASL password, redacted once the plugin is prepared |
| `password_env` | string | none | no | Environment variable holding the SASL password, read at every connect and preferred over an inline one |
| `use_ssl` | bool | none | no | TLS to the brokers |
| `ssl_options` | list | none | no | Erlang `:ssl` options for that connection |
| `topic` | string | none | yes | Destination topic |
| `partition` | int | none | no | A fixed destination partition for every record; unset, `partitioner` chooses |
| `partitioner` | enum | `:murmur2` | no | Without `partition`: `murmur2` sends a keyed record where Kafka's Java client would and an unkeyed one to the sticky partition; `sticky` sends every record to the sticky partition, which moves after each batch |
| `compression` | enum | `:none` | no | Batch compression; `zstd` needs Kafka 2.1 or later; `snappy` and `lz4` are refused, the client library encodes them only with native code |
| `headers` | headers | `%{}` | no | Static record headers, a JSON object of strings; names starting `flz.` and `flezha.dedup_key` are refused |
| `required_acks` | int | `-1` | no | Broker acks; -1 is acks=all |
| `retries` | int | `5` | no | Attempts per batch before parking it |
| `delivery_timeout_ms` | int | `30000` | no | Total budget for one batch before it is parked |
| `linger_ms` | int | `10` | no | How long a batch waits to fill |
| `max_batch_bytes` | int | `1048576` | no | Largest record batch produced, in encoded bytes |
| `idempotent` | bool | `true` | no | Use the broker's idempotent producer session |
| `max_in_flight` | int | none | no | Batches in flight; derived as 5 with `idempotent`, else 1 |
| `auto_create_topic` | bool | `true` | no | Create the topic when missing |
| `auto_create_partitions` | int | `1` | no | Partitions for that topic |
| `auto_create_replication_factor` | int | `2` | no | Replication factor for that topic |
| `topic_configs` | list | `[]` | no | `{name, value}` pairs for that topic |
| `metadata_refresh_timeout_ms` | int | `5000` | no | Budget for one broker metadata refresh |
| `request_timeout_ms` | int | `10000` | no | Budget the broker is asked to honour for one produce request |
| `dispatch_timeout_ms` | int | `10000` | no | Budget for one in-flight batch before it is treated as lost |
| `metadata_update_interval` | int | none | no | How often the shared broker client refreshes metadata |
| `channel` | names | none | no | The channel this plugin is attached to: only a record naming it reaches the plugin. Several names, comma-separated, take them all |
| `channel_out` | names | none | no | Only a record whose metadata channel matches reaches the plugin; the explicit spelling of `channel`. Several names, comma-separated, take them all |
| `plugin_id` | string | none | no | Plugin id; defaults to a generated one |
| `retry_backoff_ms` | int | `100` | no | First delay before a failed attempt is made again |
| `retry_max_backoff_ms` | int | `5000` | no | Ceiling for that delay |
| `queue_max` | int | `10000` | no | Outputs the queue holds before it is full |
| `queue_overflow` | enum | `:drop_nothing` | no | What a full queue does: make the dispatch wait, drop the oldest, or drop the newest |
| `queue_block_timeout_ms` | int | `5000` | no | How long a dispatch waits on a full queue before its output overflows |
<!-- provider-options:end -->

## Example

```sh
$ flz plugin enable trades-producer kafka_producer \
  --relay_id=trades \
  --hook=on_output \
  --Xhosts=kafka:9092 \
  --Xtopic=trades \
  --Xqueue_max=20000 \
  --Xlinger_ms=20 \
  --Xqueue_overflow=drop_oldest
```

The hook is what makes this a producer: `--hook=on_output` publishes what the
relay emits, and `--hook=on_input` publishes what it ingests instead. An
enable that names no hook publishes nothing.

## Installing

The package `flezha_kafka` is built in its own repository, `Flezha/flezha_kafka_plugin`,
which this repository carries as the submodule `plugins/flezha_kafka_plugin`; its README
covers building and testing it. The controller release does not carry Kafka: a controller
produces to Kafka once the package is installed.

Build the archive from the package (its README: `mix flezha.plugin.package`), then install
`flezha_kafka-0.1.1.fpz` on the controller as
[`installing_plugins.md`](../topics/plugins/installing_plugins.md#install-uninstall-and-sync)
describes.

The package bundles the `kafka_ex` client and starts none of that library's own workers; the
checksum library the Kafka record format needs, `crc32cer`, is native code, which a package
may not carry, so the release ships it. Before the package existed the provider was built in
with the id `relay_kafka_producer`; a plugin enabled under that id is not converted, so
enable `kafka_producer` in its place.

## Contract

The plugin:

- Can be shared: every relay bound to one plugin publishes through one producer per
  controller, or one in the whole cluster with `--single`. Each member of a partitioned
  relay runs a producer of its own.
- Retries every transient broker error (`:no_broker`, `:not_leader_for_partition`,
  `:network_exception`, `:request_timed_out`, `:not_enough_replicas`,
  `:not_enough_replicas_after_append`, `:replica_not_available`, `:leader_not_available`,
  `:broker_not_available`, `:closed`, `:correlation_mismatch`) until the batch is acked
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
- Caps buffering at `:queue_max` records and applies `:queue_overflow` when full:
  `:drop_nothing` by default, so nothing is dropped unless a user chose a drop mode.
- Under `:idempotent = true`, the default, uses Produce v5 with per-partition sequence numbers, acks a
  `:duplicate_sequence_number` as delivered, and on a fence
  (`:out_of_order_sequence_number`, `:invalid_producer_epoch`) re-initialises the producer
  session and resends rather than dropping.
- Emits a `:telemetry` event under `[:flezha, :plugin, :kafka, _, _]` for every transition.

Chain callers never see a transient reason as a raw `{:error, _}`; a chain-dispatched
record reports only through telemetry (below), and a synchronous `enqueue/4` caller gets
`{:error, {:delivery_failed, last_reason}}` once the budget is exhausted or
`{:error, {:permanent, reason}}` on a permanent error.

## Effectively-once records

Records from an `exactly: effectively_once` relay carry the outbox identity stamps
`flz.cell`, `flz.seq` (`<record WAL seq>.<emit ordinal>`) and `flz.epoch`. For them the
plugin uses `flz.key` (the emitting Cell's key, or `emit_keyed`'s) as the Kafka message
key (stable partitioning across replays, latest-value semantics on compacted topics) and
writes the full identity as the record header
`flezha.dedup_key = <flz.cell>/<outbox seq>/<epoch>` (e.g. `MarketRelay/s1/k1/42.0/1`).
Records without the stamps use `flz.key`, else a caller-stamped `flezha.dedup_key`, else
`kafka_msg_key` as the message key and carry no `flezha.dedup_key` header.

## Partitioning

With `partition` set, every record goes to that partition. Without it, the producer chooses:

- `partitioner: murmur2` (the default): a record with a message key goes to
  `(murmur2(key) & 0x7FFFFFFF) mod n`, `n` the topic's partition count: the partition
  Kafka's Java client chooses for the same key, so Flezha's records co-partition with other
  producers' and consumers'. A record with no key but a durable identity (an
  effectively-once output) goes to the partition its `flezha.dedup_key` hashes to the same
  way, so a redrive lands where the original did. A record with neither goes to the sticky
  partition.
- `partitioner: sticky`: a record with a durable identity is placed the same way; every other
  record goes to the sticky partition, which moves to another partition, chosen at random,
  once a batch has left from it.

Order is kept per partition; records without a key keep no order among themselves. The choice is made by the plugin's one worker per topic, not by
the relay's hook: every relay bound to the plugin casts its records to that worker,
which keeps a lane per partition, each with its own queue, window and producer session. The
partition count is read from the topic's metadata when the worker starts, after a broker
client swap, after a metadata refresh and after the producer creates the topic; a topic the
broker does not know counts `auto_create_partitions` when `auto_create_topic` is on. Records
held for a partition the topic turns out not to have go where the count read sends them. While no broker answers the read,
records wait in the worker under the queue's bound and overflow rule. When a topic gains
partitions, keys map anew, as with every Kafka client. A broker whose
`auto.create.topics.enable` is on creates a topic a metadata read names, with its own default
partition count: create the topic first where the count matters.

## Headers and compression

The record of an output the outbox holds (a delivery-guaranteed or `effectively_once` one)
carries a header for each identity field its output has: `flz.key`, `flz.cell`, `flz.seq`,
`flz.epoch`. A plain output's record carries none of them, since nothing deduplicates it.
Then comes `flezha.dedup_key` as above, then every entry of the `headers` option
(`--Xheaders='{"source":"flezha"}'`). A `headers` name starting with `flz.`, or
`flezha.dedup_key`, is refused at enable with `plugin.headers_invalid`.

`compression: gzip` or `compression: zstd` compresses each record batch; `none` is the
default. A zstd batch is encoded by OTP's own `zstd` module and sent as a Produce v7 request,
which a broker accepts from Kafka 2.1. `snappy` and `lz4` are refused at enable with
`plugin.option_invalid`: the client library encodes them only with native libraries, which a
package cannot carry.

**The exactness contract.** The client has no transactional producer, so the plugin does
not claim broker-side exactly-once. A controller restart re-drives unflushed outputs with
identical key, header and partition:

- **Compacted / changelog topics** (candles, indicators, any latest-value stream):
  re-driven records upsert the same key; compaction converges to one value per key.
- **Append-only topics**: a re-drive can append the same logical record twice; exact-once
  consumption means deduplicating on the `flezha.dedup_key` header, which is unique per
  logical output and identical across re-drives.

## Options in detail

`channel` names the channel this plugin is attached to; the producer only acts on the
records passing through the relay, so on it that means "produce only records whose
metadata names this channel". `channel_out` is the explicit spelling of the same
restriction, for a user writing one vocabulary across every provider. Writing both,
or writing either on a plugin with neither an `on_input` nor an `on_output` hook, is
refused when the plugin is enabled, because nothing would read it. There is no
`channel_in`: the producer never feeds the relay.

Either spelling takes several names, comma-separated, and produces the records of all of
them: `--Xchannel alerts,audit` on a relay that also produces `metrics` produces the
first two and leaves the third to pass through the chain untouched.

```sh
$ flz plugin enable alerts kafka_producer \
  --relay_id=op1 \
  --hook=on_output \
  --Xhosts=broker-1:9092 \
  --Xtopic=alerts \
  --Xchannel=alerts,audit
```

This **selects** records; it does not merge channels. Each record keeps its own `channel`
metadata, so a consumer reading the topic can still tell an `alerts` record from an
`audit` one. Merging two outputs into one stream is a different question, and the answer
to it is a tasklet that carries both rather than a plugin that groups them.

Order does not matter and a repeated name changes nothing, since neither can change which
records match. Whitespace around a name is trimmed, so `alerts, audit` names the same two
channels as `alerts,audit` and a channel whose own name carries a leading or trailing space
is not reachable this way. An empty segment is a typo rather than a shorthand, so
`alerts,,audit`, `alerts,` and `,` are each refused when the plugin is enabled, naming the
option. A channel
whose name itself contains a comma cannot be written with this flag; a caller building the
options programmatically can still pass the list.

Passwords are redacted in the persisted and returned opts.

### Sizing `:queue_max`

`queue_max = expected rate × longest outage in seconds × headroom`: 50 msg/s absorbing a
60 s leader election with 2× headroom is 6 000. A batch leaves the queue when it is sent,
so the window holds up to `:max_in_flight × :max_batch_bytes` beyond the queue: 5 MiB per
partition at the defaults. Each partition the worker produces to has a queue of its own.

Under the default `:drop_nothing`, a dispatch waits while its worker has a full queue, at
most `:queue_block_timeout_ms`, and its record then overflows. Records already on their way
when a queue fills are admitted past `queue_max`, so the bound is exceeded by those alone.

### `:max_in_flight`

A broker answers one connection's requests in order, and under idempotence it rejects a
batch whose sequence numbers do not continue the last one it stored. Those two facts let
several Produce requests be outstanding without losing partition order: a failed batch
halts new sends, waits for every batch behind it to come back, and goes out again with all
of them in the original order under the original sequence numbers. Nothing preserves that
order without idempotence, so a wider window without `:idempotent` is refused.

### `:queue_overflow`

- **`:drop_nothing`** (default): the dispatch waits until there is room, at most
  `:queue_block_timeout_ms`, and the record then overflows. The dispatch waits only while a
  queue of the worker is full; records already on their way when it fills are admitted past
  the bound. A sync `enqueue/4` caller waits the same budget and is then answered
  `{:error, {:queue_full, :timeout}}`.
- **`:drop_oldest`**: keeps recency: the oldest queued record is evicted. Suits market
  data where stale prices are useless.
- **`:drop_newest`**: keeps order: new arrivals are rejected until space frees. Suits
  audit streams where the prefix matters.

Any other value is refused at preparation (`{:error, "Invalid value for queue_overflow: …"}`
naming the offered value) so a misspelled policy is a plugin that does not enable rather
than one that serves until its queue fills.

Every drop and overflow is counted in `stats/1`'s `dropped_total` and emits
`[:flezha, :plugin, :kafka, :overflowed]`. A delivery-guaranteed record that overflows under
`:drop_nothing` is parked as `kafka.exhausted` with the reason `queue_full`. Dropping one
under `:drop_oldest` or `:drop_newest` is alarm-grade: it logs at `error`, tags the event `guaranteed: true`, and
leaves the record's outbox cursor where it is, so the loss shows as an outbox that stops
draining until the entry is re-driven.

### Delivery confirmation

`on_input/2` / `on_output/2` take ownership of a delivery-guaranteed record
(`Flezha.State.Outbox.hold/1`) before handing it to the worker. An acked batch confirms
every guaranteed record it carried in one report per owning writer
(`Outbox.confirm_many/1`); a batch that exhausts its budget, or a record still held when a
client swap is retried past `delivery_timeout_ms`, parks each of them through the
dead-letter store under its own `(cell, seq, epoch)` identity (`Outbox.park/4`, one per
record because a park carries the payload), so the cursor advances past a recorded,
replayable loss rather than a silent one. Records without the `flz.ack` token settle nothing.

## Chain dispatch does not wait on the broker

`on_input/2` and `on_output/2` return `{:ok, ctx}` in O(1) relative to broker latency:
they hold a delivery-guaranteed output, `cast` to the plugin's one `ProducerWorker` per
topic and return. The bounded queue, not the host's mailbox, is the back-pressure boundary:
under `:drop_nothing` the dispatch waits while the worker has a full queue, at most
`:queue_block_timeout_ms`.

| Function | Caller | Return | Failure surfacing |
| --- | --- | --- | --- |
| `ProducerWorker.cast_enqueue/3` | chain plugins | `:ok` once handed over, or once a full queue was waited out | telemetry; outbox park for guaranteed records |
| `ProducerWorker.enqueue/4` | tests, tooling, explicit-flush sites | `:ok` after the broker ack, or `{:error, _}` | return value (default `timeout: 35_000`) |

A chain-admitted record that ultimately fails emits one
`[:flezha, :plugin, :kafka, :failed]` per record with `caller: :async`. Telemetry
is the channel by which users observe permanent failures of chain-admitted records:
not logs, not the chain return value:

```elixir
:telemetry.attach(
  "my-app-kafka-failures",
  [:flezha, :plugin, :kafka, :failed],
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
alias Flezha.Plugin.Provider.WorkerSup
alias FlezhaKafka.{Producer, ProducerWorker}

producer_pid = WorkerSup.worker_pid(Producer, {namespace, plugin_id, topic})

case ProducerWorker.enqueue(producer_pid, msg, metadata, timeout: 35_000) do
  :ok -> :ok
  {:error, {:delivery_failed, reason}} -> handle_failure(reason)
end
```

## Telemetry

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:flezha, :plugin, :kafka, :shipped]` | `%{batch_size, latency_ms}` | `%{plugin_id, topic, partition, base_offset, attempt}` |
| `[:flezha, :plugin, :kafka, :retried]` | `%{attempt, backoff_ms}` | `%{plugin_id, topic, partition, reason}` |
| `[:flezha, :plugin, :kafka, :overflowed]` | `%{count}` | `%{plugin_id, topic, partition, queue_depth, overflow_strategy, guaranteed, reason}` |
| `[:flezha, :plugin, :kafka, :failed]` | `%{attempts, total_ms}` | `%{plugin_id, topic, partition, last_reason, caller}` |
| `[:flezha, :plugin, :kafka, :metadata_refresh]` | `%{latency_ms}` | `%{plugin_id, topic, partition, trigger, error}` |
| `[:flezha, :plugin, :kafka, :topic_created]` | `%{partitions, replication_factor}` | `%{plugin_id, topic}` |
| `[:flezha, :plugin, :kafka, :worker_swapped]` | `%{queue_depth, in_flight_records, queue_bytes}` | `%{plugin_id, topic, partition, reason, producer_id_before, producer_id_after}` |

`partition` is the record's. A swap is the worker's, summed over its partitions: its
`partition` and producer ids are the fixed partition's, `nil` when the worker chooses. The
package declares the three verbs beyond the shared four, and the controller exports all
seven under the source `flezha_kafka`.

A quick Logger handler for installations without a metrics stack:

```elixir
:telemetry.attach_many(
  "flezha-kafka-producer-log",
  Enum.map([:shipped, :retried, :overflowed, :failed, :metadata_refresh, :topic_created,
            :worker_swapped],
           &[:flezha, :plugin, :kafka, &1]),
  fn name, measurements, metadata, _ ->
    Logger.info("[kafka.producer] #{inspect(name)} m=#{inspect(measurements)} md=#{inspect(metadata)}")
  end,
  nil
)
```

## Stats and health

```elixir
{:ok, stats} = FlezhaKafka.Producer.stats(plugin_id)
# %{queue_depth: 0, in_flight: 0, waiting: 0, dropped_total: 4, last_error: nil,
#   consecutive_failures: 0, topic: "trades",
#   partitions: %{0 => %{queue_depth: 0, in_flight: 0}}}
```

`stats/1` sums the worker's partitions and lists each under `partitions`; its
`consecutive_failures` and `last_error` are the partition that has failed the most batches in
a row (or the broker client's own trouble), and it returns `{:error, :no_producer}` when no
worker is registered for the plugin id. A partition count the worker cannot read counts as a
failed batch. `waiting` counts the records held back by a full queue: dispatches and
`enqueue/4` callers waiting for room.

A partition's idempotent producer session opens on its first record, with a synchronous
`InitProducerId` bounded by `metadata_refresh_timeout_ms`; one that fails leaves the
partition producing without a session, one batch in flight at a time, until the next broker
client swap opens it again.

`health/1` reports `{:unhealthy, :produce_stalled}` after 3 consecutive failed batches
that a fresh broker client did not clear, and `{:unhealthy, {:swap_exhausted, reason}}`
when the worker has given up replacing its client; either escalates to a plugin restart by
the convergence loop. A single batch failing with a reason that means the client has no
working broker connection (`:no_broker`, `:broker_not_available`, `:network_exception`,
`:leader_not_available`, `:closed`, `:correlation_mismatch`) forces a client replacement
immediately, throttled to one per 2 s.

The package sets the `kafka_ex` library's `sync_timeout`, which bounds every request that
names no timeout of its own, to 5 s: the consumer's fetch needs it
([`kafka_consumer.md`](kafka_consumer.md#the-client-timeout)). While a broker is away the
worker can wait that long on its client, so a `stats/1` answer can come up to 5 s late; no
record is lost.

## Durability trade-offs

- `:required_acks = -1` (default) waits for all in-sync replicas. With
  `min.insync.replicas = 1` the steady-state latency equals `acks = 1`; the difference
  shows only when the ISR shrinks. Combined with idempotence, written once and stays written.
- `:required_acks = 1` waits for the leader only: faster, but a leader crash before
  replication loses the message. For derived, regenerable streams.
- `:required_acks = 0` is fire-and-forget. Only for data that is cheap to replay.
- Producer-id rotation (a worker restart or a client swap) may duplicate the whole in-flight
  window: up to `:max_in_flight` batches. `:max_in_flight = 1` restores a one-batch bound.
- The fast-but-lossy profile is an explicit choice:
  `%{required_acks: 1, idempotent: false, retries: 0}`.

## Worker sharing

Producer workers with the same connection identity (`:hosts`, SSL options, auth) share
`KafkaEx.Client` processes through `FlezhaKafka.WorkerRegistry`, at most one per scheduler
per cluster: a client encodes each batch in its own process, so one shared client would
serialise every producer's encoding on one core. Workers are spread by the hash of their key
(namespace, plugin, topic) `mod schedulers_online`. The registry refcounts owners and stops a
client 5 s after its last owner releases it, which bounds connections by the node's cores
rather than the plugin count. The client pipelines requests, so the lane buys encode
parallelism, not wire concurrency. The registry has no application to start it: the first
producer starts it under the provider's own worker family, where no plugin's teardown stops
it.

One worker serves a plugin's topic on a controller, per namespace: two namespaces enabling
the same name do not share it, and each member of a partitioned relay, whose plugin id
carries its `#pK` suffix, runs its own. Within it each partition keeps its own queue,
producer id and sequence counter. A worker restart rewinds every partition's in-flight
window to the head of its queue; the duplicate bound is the window, as above.

## See also

- [durability.md](../topics/core/durability.md): the outbox, confirmation and the effectively-once tier
- [effectively_once.md](../topics/core/effectively_once.md): writing each output once
- [installing_plugins.md](../topics/plugins/installing_plugins.md): installing a plugin package
- [kafka_consumer.md](kafka_consumer.md): reading Kafka topics
- [Plugin options](../configuration/plugin_options.md): plugin options as a configuration layer
