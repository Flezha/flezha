# kafka-producer-plugin Specification

## Purpose

The kafka-producer-plugin capability defines the production-grade contract that any Flezha Kafka producer plugin SHALL satisfy when publishing messages from a processor chain to a Kafka cluster. The contract supersedes the prior "thin `KafkaEx.produce` wrapper" behaviour, which surfaced every transient broker condition (`:not_leader_for_partition`, `:no_broker`, `:unknown_topic_or_partition`, `:network_exception`, `:request_timed_out`, `:not_enough_replicas`) directly to the chain, dropped messages on every error, spawned one `KafkaEx.Server` worker per plugin instance, and let upstream mailboxes grow unbounded during outages until the BEAM was OOM-killed. This capability specifies the retry, idempotence, batching, bounded-queue, shared-worker, active metadata refresh, auto-topic-creation, and telemetry semantics a conforming producer SHALL implement, and the configuration surface (`:retries`, `:retry_backoff_ms`, `:retry_max_backoff_ms`, `:delivery_timeout_ms`, `:linger_ms`, `:max_batch_bytes`, `:queue_max`, `:queue_overflow`, `:idempotent`, `:auto_create_topic`, `:auto_create_partitions`, `:auto_create_replication_factor`, `:required_acks`) operators tune to express durability and back-pressure trade-offs. A conforming producer SHALL never surface raw transient broker errors to the chain for messages that ultimately succeed, SHALL bound mailbox and queue growth under sustained outages, and SHALL expose every state transition as a `:telemetry` event so operators can monitor health without parsing log lines.

## Requirements

### Requirement: Producer retries every transient broker error transparently

The Kafka producer plugin SHALL retry the following error classes returned by `KafkaEx.produce` (or any downstream Kafka client) without surfacing them to the chain caller, until either delivery succeeds or `:delivery_timeout_ms` is exceeded:

- `:no_broker`
- `:not_leader_for_partition` (Kafka error code 6)
- `:network_exception` (error code 13)
- `:request_timed_out` (error code 7)
- `:not_enough_replicas` (error code 19)
- `:not_enough_replicas_after_append` (error code 20)
- `:replica_not_available` (error code 9)

For `:unknown_topic_or_partition` (error code 3) the plugin SHALL retry only when `:auto_create_topic` is `true` (the default) OR when metadata refresh reveals the topic now exists. After auto-creation, retries continue until the topic appears in the cluster metadata or `:delivery_timeout_ms` elapses.

Backoff: `min(:retry_backoff_ms × 2^attempt + jitter(0, 100), :retry_max_backoff_ms)`. Jitter SHALL be at least uniform [0, 100ms] to prevent thundering-herd retries.

Non-retriable errors: `:invalid_topic_exception`, `:record_list_too_large`, `:offset_metadata_too_large`, `:topic_authorization_failed`, `:cluster_authorization_failed`. These return `{:error, {:permanent, reason}}` immediately.

#### Scenario: Single `:not_leader_for_partition` recovers within one backoff cycle
- **GIVEN** a healthy producer configured with defaults
- **WHEN** `KafkaEx.produce` returns `{:error, :not_leader_for_partition}` once
- **AND** the next metadata refresh reveals the new partition leader
- **THEN** the plugin SHALL force a metadata refresh, sleep `:retry_backoff_ms` ms, and retry the same Produce request
- **AND** the chain SHALL receive `{:ok, ctx}` from the plugin
- **AND** the chain SHALL NOT log a `transform subscriber ... returned error: :not_leader_for_partition`

#### Scenario: 30-second leader-election storm is fully absorbed
- **GIVEN** a producer is sending 50 messages/sec to a topic
- **WHEN** the broker hosting the partition leader cycles through KRaft `Unattached` states for 30 seconds
- **AND** every Produce returns `:no_broker` or `:not_leader_for_partition` during that window
- **THEN** within `:delivery_timeout_ms` of recovery, every message SHALL be delivered (subject to bounded-queue drops)
- **AND** the chain caller SHALL NOT receive a single `{:error, _}` for messages that ultimately succeeded
- **AND** a `[:flezha, :plugin, :kafka, :produce, :retry]` telemetry event SHALL be emitted for each retry attempt

#### Scenario: Permanent error returns immediately
- **GIVEN** a producer
- **WHEN** `KafkaEx.produce` returns `{:error, :record_list_too_large}`
- **THEN** the plugin SHALL return `{:error, {:permanent, :record_list_too_large}}` without retrying
- **AND** SHALL emit a `[:flezha, :plugin, :kafka, :produce, :failed]` telemetry event with `last_reason: :record_list_too_large`

#### Scenario: Delivery timeout exhausts all retries
- **GIVEN** a producer with `:delivery_timeout_ms = 5000`
- **WHEN** every attempt in 5 seconds fails with `:no_broker`
- **THEN** the plugin SHALL return `{:error, {:delivery_failed, :no_broker}}` to the chain
- **AND** SHALL emit a `[:flezha, :plugin, :kafka, :produce, :failed]` telemetry event with `attempts: <n>` and `total_ms: ~5000`

### Requirement: Producer auto-creates the topic on first publish when configured

When `:auto_create_topic` is `true` (default) and a Produce request returns `:unknown_topic_or_partition`, the producer SHALL issue a `CreateTopics` request with the configured `:auto_create_partitions` (default 1), `:auto_create_replication_factor` (default 2), and any `:topic_configs` map (default empty), then wait for metadata to reflect the topic (bounded by 5 seconds), then retry the original Produce.

When `:auto_create_topic` is `false`, the plugin SHALL treat `:unknown_topic_or_partition` as a non-retriable error and return `{:error, {:permanent, :unknown_topic_or_partition}}`.

The CreateTopics call SHALL be idempotent — if the topic was created concurrently by another producer, the `TOPIC_ALREADY_EXISTS` response SHALL be treated as success.

#### Scenario: First publish to a non-existent topic auto-creates and succeeds
- **GIVEN** a producer with `:auto_create_topic = true` and `:auto_create_partitions = 1`
- **AND** the topic does not exist on the cluster
- **WHEN** the plugin's `on_output` is called with a message destined for that topic
- **THEN** the plugin SHALL detect the `:unknown_topic_or_partition` response on the first attempt
- **AND** SHALL issue a `CreateTopics` request with `partitions: 1, replication_factor: 2`
- **AND** within 5 seconds the topic SHALL exist on the cluster with the configured shape
- **AND** the original message SHALL be successfully delivered
- **AND** a `[:flezha, :plugin, :kafka, :topic, :created]` telemetry event SHALL be emitted

#### Scenario: Concurrent topic creation does not error
- **GIVEN** two producer plugin instances both publishing to the same non-existent topic at the same instant
- **WHEN** both call `CreateTopics` concurrently
- **THEN** one SHALL succeed and the other SHALL receive `TOPIC_ALREADY_EXISTS`
- **AND** both SHALL treat their respective responses as success and proceed with the original Produce
- **AND** the topic SHALL exist with the requested partition count

#### Scenario: Auto-create disabled drops the message immediately
- **GIVEN** a producer with `:auto_create_topic = false`
- **WHEN** Produce returns `:unknown_topic_or_partition`
- **THEN** the plugin SHALL return `{:error, {:permanent, :unknown_topic_or_partition}}` without retry
- **AND** SHALL NOT issue a CreateTopics request

### Requirement: Producer uses idempotent semantics by default

When `:idempotent` is `true` (default) the producer SHALL:

1. Request a producer-id from the broker via `InitProducerId` at worker startup.
2. Maintain a per-`(topic, partition)` 32-bit sequence counter starting from 0.
3. Include the producer-id and sequence number in every Produce request via `api_version: 5` (or higher) encoding.
4. On retry, send the SAME producer-id + sequence number (the broker dedupes server-side).
5. On worker restart (and only then) accept that the producer-id rotates; in-flight unacked messages may produce duplicates equal to the in-flight batch size at restart.

When `:idempotent` is `false` the producer SHALL fall back to `api_version: 3` Produce requests without producer-id state. This mode is for callers who deliberately want at-most-once semantics.

#### Scenario: Idempotent retry does not duplicate
- **GIVEN** an idempotent producer
- **WHEN** a Produce succeeds server-side but the client times out before receiving the response
- **AND** the plugin retries with the same sequence number
- **THEN** the broker SHALL return success without writing a second copy
- **AND** the topic's log SHALL contain exactly one copy of the message
- **AND** the chain SHALL receive `{:ok, ctx}` exactly once

#### Scenario: Producer-id rotation on worker restart
- **GIVEN** an idempotent producer with 5 messages in flight (sent, awaiting ack)
- **WHEN** the worker process crashes and is restarted by its supervisor
- **THEN** the new worker SHALL request a new producer-id via `InitProducerId`
- **AND** the in-flight messages, if redelivered from a recovered queue, SHALL produce duplicates equal to the in-flight batch (≤5 in this scenario)
- **AND** the duplicate count SHALL be observable via the `[:flezha, :plugin, :kafka, :produce, :ok]` telemetry stream's `attempt: 1` count vs delivered count

#### Scenario: Non-idempotent mode skips InitProducerId
- **GIVEN** a producer with `:idempotent = false`
- **WHEN** the worker starts
- **THEN** the worker SHALL NOT issue an `InitProducerId` request
- **AND** Produce requests SHALL use `api_version: 3` encoding without producer-id fields
- **AND** retries are still performed but without idempotence guarantees

### Requirement: Producer batches messages within a linger window

The producer SHALL accumulate messages destined for the same `(topic, partition)` into a single Produce request when:

- The accumulated wire size has not exceeded `:max_batch_bytes` (default 1 048 576 = 1 MiB), AND
- Less than `:linger_ms` milliseconds have elapsed since the first un-sent message in the batch.

When either condition is exceeded, the batch SHALL be flushed (sent) immediately. Setting `:linger_ms = 0` disables batching — every message produces a Produce request immediately.

#### Scenario: 50 trades in 10 ms become one Produce request
- **GIVEN** a producer with `:linger_ms = 10` accepting 50 messages destined for the same partition over 10 ms
- **WHEN** the linger window closes
- **THEN** exactly one Produce request SHALL be issued containing all 50 messages
- **AND** all 50 messages SHALL share the same producer-id and consecutive sequence numbers
- **AND** a single `[:flezha, :plugin, :kafka, :produce, :ok]` telemetry event SHALL be emitted with `batch_size: 50`

#### Scenario: 1 MiB batch flushes mid-window
- **GIVEN** a producer with `:max_batch_bytes = 1048576` and `:linger_ms = 1000`
- **WHEN** accumulated messages reach 1 MiB after 50 ms
- **THEN** the batch SHALL flush at 50 ms without waiting the remaining 950 ms

#### Scenario: linger_ms = 0 disables batching
- **GIVEN** a producer with `:linger_ms = 0`
- **WHEN** a single message arrives
- **THEN** a single-message Produce request SHALL be issued immediately
- **AND** subsequent messages SHALL each produce their own Produce request

### Requirement: Bounded outbound queue with configurable overflow strategy

Each `(worker, topic, partition)` triple SHALL maintain its own bounded outbound queue. When the queue has reached `:queue_max` (default 10 000) items, the producer SHALL apply the configured `:queue_overflow` strategy:

- `:drop_oldest` (default): pop the head of the queue, push the new message at the tail. Increment the per-`(topic, partition)` `dropped_total` counter.
- `:drop_newest`: reject the new message. Increment `dropped_total`.
- `:block`: synchronously block the chain caller until queue space is available, with a hard timeout of `:queue_block_timeout_ms` (default 5000); if the timeout fires, return `{:error, {:queue_full, :timeout}}`.

The `dropped_total` counter SHALL be exposed via:

- `:telemetry` event `[:flezha, :plugin, :kafka, :produce, :dropped]` per drop event.
- A function `Flezha.Plugin.Provider.Kafka.Producer.stats(plugin_id) :: %{dropped_total: integer, queue_depth: integer, ...}` for synchronous read.

#### Scenario: Drop-oldest preserves recency under sustained outage
- **GIVEN** a producer with `:queue_max = 100` and `:queue_overflow = :drop_oldest`
- **WHEN** the broker is unreachable for 60 seconds while 200 messages arrive
- **THEN** the queue SHALL contain exactly 100 messages — the most recent 100
- **AND** `dropped_total` SHALL equal 100
- **AND** when the broker returns, the most recent 100 messages SHALL be delivered in order

#### Scenario: Drop-newest preserves order under saturation
- **GIVEN** a producer with `:queue_max = 100` and `:queue_overflow = :drop_newest`
- **WHEN** 200 messages arrive while the queue is at capacity
- **THEN** the queue SHALL contain exactly 100 messages — the first 100 to arrive
- **AND** the second 100 SHALL each result in `{:error, {:queue_full, :overflow}}` returned to the chain
- **AND** `dropped_total` SHALL equal 100

#### Scenario: Block mode applies back-pressure
- **GIVEN** a producer with `:queue_overflow = :block` and `:queue_block_timeout_ms = 1000`
- **WHEN** the queue is at capacity
- **THEN** the next `on_output` call SHALL block until either space is available OR 1 second elapses
- **AND** if the queue drains within 1 second, the call SHALL succeed normally
- **AND** if 1 second elapses without space, the call SHALL return `{:error, {:queue_full, :timeout}}`

### Requirement: Single shared worker per cluster identity

The producer SHALL share a single `KafkaEx.Server` worker process across all plugin instances configured against the same `:hosts` (and `:ssl_options`, if any). The worker registry SHALL key on the canonical-form connection identity, not on plugin-instance ID.

The worker SHALL refcount its plugin owners. When the last owning plugin terminates (`:terminate` callback), the worker SHALL stop. New plugins arriving with the same connection identity reuse the same worker.

#### Scenario: 5 producer plugins share one worker
- **GIVEN** five `Flezha.Plugin.Provider.Kafka.Producer` plugin instances configured with `hosts: "kafka:9092"` and different topics
- **WHEN** all five complete `init_provider`
- **THEN** exactly ONE `KafkaEx.Server` worker SHALL be running
- **AND** all five plugins SHALL hold a reference to that worker
- **AND** Process.list() SHALL show one worker, not five

#### Scenario: Two clusters get two workers
- **GIVEN** plugin A configured with `hosts: "kafka-a:9092"` and plugin B with `hosts: "kafka-b:9092"`
- **WHEN** both initialize
- **THEN** exactly TWO workers SHALL be running (one per distinct cluster identity)

#### Scenario: Last plugin termination stops the worker
- **GIVEN** the only owner of a worker terminates
- **WHEN** the plugin's `:terminate` callback fires
- **THEN** the worker process SHALL stop within 5 seconds
- **AND** the registry entry SHALL be removed

### Requirement: Active metadata refresh on error classes

When a Produce returns `:not_leader_for_partition`, `:no_broker`, or `:unknown_topic_or_partition`, the producer SHALL force a metadata refresh BEFORE the next retry attempt. The refresh SHALL be bounded by `:metadata_refresh_timeout_ms` (default 5000) and SHALL emit `[:flezha, :plugin, :kafka, :metadata, :refresh]` with `trigger: :error_recovery, error: <reason>`.

The default kafka_ex `metadata_update_interval` (passive timer-based refresh) SHALL remain active as a fallback. Active refresh on error is in addition to, not in place of, the timer.

#### Scenario: Active refresh after :not_leader_for_partition
- **GIVEN** a producer that just received `:not_leader_for_partition`
- **WHEN** the retry logic kicks in
- **THEN** before sleeping the backoff, the producer SHALL call `KafkaEx.metadata` (or equivalent) to force a refresh
- **AND** SHALL emit `[:flezha, :plugin, :kafka, :metadata, :refresh]` with `trigger: :error_recovery, error: :not_leader_for_partition`
- **AND** the next Produce attempt SHALL use the freshly-refreshed metadata to target the new leader

#### Scenario: Refresh failure does not block retry
- **GIVEN** the metadata refresh itself times out after 5 seconds
- **WHEN** the producer's retry logic continues
- **THEN** the retry SHALL still proceed using the stale metadata
- **AND** SHALL emit `[:flezha, :plugin, :kafka, :metadata, :refresh]` with `error: :timeout`

### Requirement: Telemetry events for every state transition

The producer SHALL emit `:telemetry.execute/3` events on every produce attempt, retry, drop, failure, success, and metadata refresh. The events form the operator-facing API for monitoring producer health.

| Event | Measurements | Metadata |
|---|---|---|
| `[:flezha, :plugin, :kafka, :produce, :ok]` | `%{batch_size, latency_ms}` | `%{topic, partition, plugin_id}` |
| `[:flezha, :plugin, :kafka, :produce, :retry]` | `%{attempt, backoff_ms}` | `%{topic, partition, plugin_id, reason}` |
| `[:flezha, :plugin, :kafka, :produce, :dropped]` | `%{count}` | `%{topic, partition, plugin_id, queue_depth, overflow_strategy}` |
| `[:flezha, :plugin, :kafka, :produce, :failed]` | `%{attempts, total_ms}` | `%{topic, partition, plugin_id, last_reason}` |
| `[:flezha, :plugin, :kafka, :metadata, :refresh]` | `%{latency_ms}` | `%{trigger, error}` |
| `[:flezha, :plugin, :kafka, :topic, :created]` | `%{partitions, replication_factor}` | `%{topic, plugin_id}` |

Events SHALL be emitted via `:telemetry.execute` (asynchronous fire-and-forget) so attached handlers cannot back-pressure the producer.

#### Scenario: Operator attaches a Prometheus counter
- **GIVEN** a Prometheus telemetry handler attached to `[:flezha, :plugin, :kafka, :produce, :dropped]`
- **WHEN** the producer drops a message via the bounded queue
- **THEN** the Prometheus counter SHALL increment by `count: 1`
- **AND** the counter SHALL include labels `topic, partition, overflow_strategy`

#### Scenario: Slow telemetry handler does not slow produce
- **GIVEN** a deliberately-slow handler attached to `[:flezha, :plugin, :kafka, :produce, :ok]` that sleeps 1 second
- **WHEN** a produce succeeds
- **THEN** the produce-completion latency observed by the chain caller SHALL NOT include the handler's 1-second sleep
- **AND** the slow handler runs asynchronously without blocking the producer

### Requirement: Chain dispatch is non-blocking

The Kafka producer plugin's transform-subscriber callbacks (`on_input/2`, `on_output/2`) SHALL return to their caller in O(1) wall-clock time relative to broker latency, queue depth, and in-flight batch state. "O(1) relative to broker latency" means: the callback's runtime SHALL NOT include any synchronous wait on a broker round-trip, on `KafkaEx` reply timing, or on the producer worker's batch flush schedule.

The callback SHALL be implemented as a `GenServer.cast` to the per-(plugin_id, topic, partition) worker, returning immediately after admission. Admission SHALL apply the configured `:queue_overflow` strategy (per the existing "Bounded outbound queue with configurable overflow strategy" requirement). The synchronous `enqueue/4` API is retained for tests, ad-hoc tooling, and explicit-flush call sites; chain-facing callbacks SHALL NOT use it.

The `:block` overflow strategy is not honored for chain-facing callbacks. When a worker is configured with `:queue_overflow = :block` and a chain admission would block, the worker SHALL log a deprecation warning once per process lifetime and treat the admission as `:drop_newest` (i.e., drop the new message and emit telemetry). Sync `enqueue/4` callers continue to honor `:block` exactly as before.

#### Scenario: Chain dispatch returns within a millisecond regardless of broker latency
- **GIVEN** a producer plugin attached to a chain with a `KafkaEx.Server` configured to delay 30 seconds on Produce
- **WHEN** `on_output/2` is invoked with a message
- **THEN** the callback SHALL return `{:ok, ctx}` within 1 millisecond
- **AND** the message SHALL be queued in the worker's bounded queue
- **AND** the broker round-trip SHALL proceed asynchronously per the existing batching, retry, and idempotence requirements

#### Scenario: Sustained input above the broker drain rate triggers queue drops, not mailbox growth
- **GIVEN** a chain emits 80 events/sec to a producer worker whose broker round-trip takes 50 ms
- **AND** `:queue_max = 10_000`, `:queue_overflow = :drop_oldest`
- **WHEN** the chain runs for 10 minutes
- **THEN** the host process running the chain (the Overwatch GenServer) SHALL have a `:message_queue_len` no greater than 100 at any sample point
- **AND** the worker's queue depth SHALL trend toward `:queue_max` and trigger `[:flezha, :plugin, :kafka, :produce, :dropped]` events under the configured overflow strategy
- **AND** zero `OOM` events SHALL occur
- **AND** the protobuf `timestamp_ms` (or any caller-provided event timestamp) SHALL NOT diverge from the host process's wall-clock by more than the worker's `:linger_ms + :request_timeout_ms` window

#### Scenario: Block-mode admission from chain logs and drop-newest's
- **GIVEN** a producer worker with `:queue_overflow = :block`
- **AND** the worker's queue is at `:queue_max`
- **WHEN** `on_output/2` admits a new message via cast
- **THEN** the worker SHALL log a `Logger.warning/1` exactly once per worker process lifetime explaining that block-mode is not supported on the chain admission path
- **AND** SHALL emit `[:flezha, :plugin, :kafka, :produce, :dropped]` with `overflow_strategy: :block_in_cast_unsupported`
- **AND** the chain caller SHALL still observe `{:ok, ctx}`
- **AND** the synchronous `enqueue/4` call from a test-mode caller SHALL still apply the documented block-mode semantics (block until space or `:queue_block_timeout_ms`)

### Requirement: Failure surfacing for async-admitted records

Records admitted via `cast_enqueue/3` (the chain dispatch path) SHALL NOT receive a synchronous reply — there is no caller to reply to. The worker SHALL track the record's caller as `:async` in its internal envelope. On permanent failure of a batch containing async-admitted records, the worker SHALL:

- Emit `[:flezha, :plugin, :kafka, :produce, :failed]` for each affected record, with metadata including `caller: :async`, `last_reason`, `attempt_count`, `delivery_elapsed_ms`, `topic`, `partition`, `plugin_id`.
- Drop the records from the queue without invoking `GenServer.reply/2`.
- Continue draining subsequent batches.

The telemetry event becomes the canonical channel by which operators observe permanent delivery failures of async-admitted records. Logs are not the channel; the chain's return value is not the channel; only telemetry is.

Sync-admitted records (`enqueue/4`) continue to receive `{:error, {:permanent, reason}}` or `{:error, {:delivery_failed, reason}}` per the existing requirements; the cast vs call dichotomy is observable only from the producer worker's perspective.

#### Scenario: Async-admitted record permanently fails
- **GIVEN** a chain has admitted a message via `on_output/2` (cast path)
- **WHEN** `KafkaEx.produce` returns `{:error, :record_list_too_large}` (a permanent error)
- **THEN** the worker SHALL emit one `[:flezha, :plugin, :kafka, :produce, :failed]` event with `caller: :async, last_reason: :record_list_too_large`
- **AND** SHALL NOT call `GenServer.reply/2` (there is no `from`)
- **AND** the chain caller SHALL have ALREADY observed `{:ok, ctx}` (synchronously, at admission time)
- **AND** the worker SHALL continue draining subsequent batches

#### Scenario: Mixed sync and async records in the same batch
- **GIVEN** a worker with both an async-admitted record (admitted by the chain via cast) and a sync-admitted record (admitted by a test via `enqueue/4`) in the same outgoing batch
- **WHEN** the batch fails permanently
- **THEN** the sync-admitted record's caller SHALL receive `{:error, {:delivery_failed, reason}}`
- **AND** the async-admitted record's failure SHALL be reported only via the `:failed` telemetry event
- **AND** both events SHALL be emitted with their respective `caller: :sync | :async` metadata

### Requirement: Admission order is preserved across sync and cast modes

The bounded outbound queue SHALL be FIFO regardless of admission primitive. Records admitted via `cast_enqueue/3` and `enqueue/4` against the same `(plugin_id, topic, partition)` worker SHALL be drained in the order they were admitted, preserving Kafka's per-partition ordering guarantee.

#### Scenario: Interleaved sync + async admissions preserve order
- **WHEN** a chain admits records `A` (cast), `B` (cast), `C` (cast) and a test admits records `X` (call), `Y` (call) interleaved as `A, X, B, Y, C`
- **THEN** the worker SHALL drain them to the broker in the order `A, X, B, Y, C`
- **AND** the broker SHALL receive a single Produce request batch containing the records in that order (subject to `:linger_ms` and `:max_batch_bytes`)
