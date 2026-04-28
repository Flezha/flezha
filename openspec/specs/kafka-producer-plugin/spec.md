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
