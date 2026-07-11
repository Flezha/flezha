# kafka-producer-plugin Specification

## Purpose

The kafka-producer-plugin capability defines the production-grade contract that any Flezha Kafka producer plugin SHALL satisfy when publishing messages from an operative chain to a Kafka cluster. The contract supersedes the prior "thin `KafkaEx.produce` wrapper" behaviour, which surfaced every transient broker condition (`:not_leader_for_partition`, `:no_broker`, `:unknown_topic_or_partition`, `:network_exception`, `:request_timed_out`, `:not_enough_replicas`) directly to the chain, dropped messages on every error, spawned one `KafkaEx.Server` worker per plugin instance, and let upstream mailboxes grow unbounded during outages until the BEAM was OOM-killed. This capability specifies the retry, idempotence, batching, bounded-queue, shared-worker, active metadata refresh, auto-topic-creation, and telemetry semantics a conforming producer SHALL implement, and the configuration surface (`:retries`, `:retry_backoff_ms`, `:retry_max_backoff_ms`, `:delivery_timeout_ms`, `:linger_ms`, `:max_batch_bytes`, `:queue_max`, `:queue_overflow`, `:idempotent`, `:auto_create_topic`, `:auto_create_partitions`, `:auto_create_replication_factor`, `:required_acks`) executors tune to express durability and back-pressure trade-offs. A conforming producer SHALL never surface raw transient broker errors to the chain for messages that ultimately succeed, SHALL bound mailbox and queue growth under sustained outages, and SHALL expose every state transition as a `:telemetry` event so executors can monitor health without parsing log lines.
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

### Requirement: Producer holds at most one shared `KafkaEx.New.Client` worker pid per canonical connection identity

The producer plugin SHALL acquire its `KafkaEx.New.Client` worker pid through `Flezha.Plugin.Provider.Kafka.WorkerRegistry` (the shared, refcounted, per-cluster registry). The registry SHALL key entries on the canonical tuple `{sorted_hosts, phash2(ssl_options), phash2(auth)}` so two plugin instances with the same broker target reuse the same worker. The registry SHALL monitor the worker process; on DOWN it SHALL clear the entry AND proactively deliver a typed `{:kafka_worker_dead, {key, token}, reason}` message to every caller pid that still holds a live token against the dead entry. The next `acquire/2` for the same identity SHALL start a fresh worker. The registry SHALL expose a `swap(ref, server \\ __MODULE__) :: {:ok, pid(), ref()} | {:error, term()}` API that performs an in-place re-acquire for an existing caller without double-counting refcount or triggering the linger-stop machinery, intended for callers handling the death-notification message.

Linger: a worker with refs = 0 SHALL be stopped after a configurable `:linger_ms` (default 5000 ms) to absorb plugin-restart churn. The linger timer SHALL be cancelled on the next `acquire/2`.

The `release/2` token returned by `acquire/2` SHALL be opaque to callers — they pass it back unchanged. A token issued against a now-dead entry SHALL no-op on `release/2`.

#### Scenario: Two plugins same identity share a worker
- **GIVEN** plugin A and plugin B configured with the same `:hosts`, `:ssl_options`, `:auth`
- **WHEN** both call `WorkerRegistry.acquire/2` at boot
- **THEN** both SHALL receive the same `worker_pid`
- **AND** the registry entry SHALL show `refs: 2`

#### Scenario: Worker death notifies every active token holder
- **GIVEN** plugins A, B, and C have each acquired tokens against the same canonical identity
- **WHEN** the underlying `KafkaEx.New.Client` worker exits (any reason — `:shutdown`, `:normal`, `:killed`, an upstream `Kayrock` parse error crash, etc.)
- **THEN** the registry's `handle_info({:DOWN, _, :process, worker_pid, reason}, …)` clause SHALL deliver `{:kafka_worker_dead, {key, token_a}, reason}` to A, `{:kafka_worker_dead, {key, token_b}, reason}` to B, and `{:kafka_worker_dead, {key, token_c}, reason}` to C BEFORE deleting the entry from `state.entries`
- **AND** each producer SHALL be able to call `swap/2` and continue draining its queue without losing records
- **AND** stale `release/2` calls with the dead token SHALL be silently accepted (return `:ok`)

#### Scenario: `swap/2` acquires without inflating refcount
- **GIVEN** an entry with `refs: 3` whose worker has just died
- **WHEN** caller A calls `WorkerRegistry.swap(old_ref)` to replace its dead token
- **THEN** the entry is recreated with a new worker pid and `refs: 3` (the swap reuses A's existing seat — it does not become `refs: 4`)
- **AND** A receives `{:ok, new_pid, new_ref}` where `new_ref` is a fresh token bound to A's existing caller monitor
- **AND** the old `new_ref → token` mapping for A's stale token is dropped from `state.caller_monitors`

#### Scenario: Linger absorbs plugin restart
- **GIVEN** plugin A has the only token against worker W with `:linger_ms = 5000`
- **WHEN** plugin A calls `release/2`
- **AND** within 1 second a new instance of plugin A (same canonical identity) calls `acquire/2`
- **THEN** the new instance SHALL receive the same `worker_pid` W
- **AND** the linger timer SHALL be cancelled
- **AND** the entry's `refs` SHALL be back to 1

### Requirement: Producer survives upstream Kafka worker crashes with zero queued-record loss

When the `KafkaEx.New.Client` worker pid referenced by `state.worker_pid` terminates for any reason (including upstream `Kayrock` parse errors, broker connection loss returning `:einval`, OS-suspend/wake leaving the socket half-dead, or an explicit `KafkaEx.stop_worker/1`), the producer SHALL continue to drain `state.queue`, `state.in_flight`, and `state.blocked` against a freshly-acquired worker for the same canonical connection identity, **without losing any record that was already admitted to those structures** and **without emitting any `[:flezha, :plugin, :kafka, :produce, :failed]` telemetry for records the new worker has not yet positively rejected**.

The producer SHALL learn of the worker's death through a typed `{:kafka_worker_dead, ref, reason}` message delivered by `Flezha.Plugin.Provider.Kafka.WorkerRegistry` to every caller pid that holds a live token against the dying entry (see the `Modified` requirement on the registry below). The producer SHALL NOT rely on the linked-process `:EXIT` signal to drive recovery, although it SHALL match a defensive `handle_info({:EXIT, _pid, _reason}, state)` clause that logs at `:debug` and returns `{:noreply, state}` so any future linked-process addition cannot regress the producer with `FunctionClauseError`.

On receiving `{:kafka_worker_dead, registry_ref, reason}` whose `registry_ref` matches `state.registry_ref`, the producer SHALL:

1. Cancel `state.linger_timer` and `state.retry_timer` if either is set (`Process.cancel_timer/1`), so no stale `:flush_batch` or `:retry_batch` fires between the swap and the next `maybe_kick_batch/1`.
2. If `state.in_flight` is not `nil`, move it back to the head of `state.queue` with its `attempt` field incremented by 1 and `last_error` set to `{:worker_died, reason}`. The records of an in-flight batch SHALL NOT be replied to or marked failed.
3. Call `WorkerRegistry.swap(state.registry_ref)` to obtain a fresh `{:ok, new_pid, new_ref}` for the same identity. On `{:error, reason}` (the broker is genuinely unreachable), schedule a retry of the swap using the existing `retry_backoff_ms(state, attempt)` curve and leave the queue untouched.
4. On successful swap, set `state.worker_pid = new_pid`, `state.registry_ref = new_ref`, `state.producer_id = nil`, `state.producer_epoch = nil`, `state.sequence = 0`, `state.in_flight = nil`, and re-enter `handle_continue(:maybe_init_producer_id, state)` so the next batch acquires a fresh idempotent producer-id against the new worker.
5. Emit exactly one `[:flezha, :plugin, :kafka, :worker, :swapped]` telemetry event with measurements `%{queue_depth: state.queue_size, in_flight_records: prior_in_flight_size, queue_bytes: state.queue_bytes}` and metadata `%{plugin_id, topic, partition, reason, producer_id_before, producer_id_after, old_worker_pid, new_worker_pid}`.

`state.blocked` callers SHALL remain blocked across the swap. Their existing `:block_timeout` timers continue to fire normally; a swap is not a `:queue_full` condition.

The producer-id rotation introduced by step 4 SHALL be bounded by the existing `kafka-producer-plugin` "Producer-id rotation on worker restart" requirement — i.e. the in-flight batch (now back at the head of the queue) may produce up to `length(in_flight.records)` duplicate writes against the broker, exactly as on supervisor-driven restart.

#### Scenario: KafkaEx worker crashes mid-batch and queued records continue flowing
- **GIVEN** a `ProducerWorker` is steadily delivering `btc_demo.ohlc.v1` records at 50 msg/sec and has 200 records in `state.queue`, 50 in `state.in_flight`, and 0 in `state.blocked`
- **AND** the operator runs `docker compose -f /home/moon/Code/flezha_market/docker/dev/docker-compose.yml exec -T flezha-controller /flezha/bin/iex --remsh flezha@... -e ':sys.terminate(pid, :forced)'` against the live KafkaEx worker pid for the operative's canonical identity
- **WHEN** `WorkerRegistry` observes the worker's DOWN
- **THEN** within 1 second the producer SHALL receive `{:kafka_worker_dead, registry_ref, _reason}`, acquire a new worker via `swap/2`, re-run `InitProducerId`, and resume sending
- **AND** the topic end-offset of `btc_demo.ohlc.v1` (read via `docker compose ... exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic btc_demo.ohlc.v1`) SHALL advance by at least 200 records within the next 10 seconds
- **AND** zero `[:flezha, :plugin, :kafka, :produce, :failed]` telemetry events SHALL fire for those records (verified via a `:telemetry` handler attached for the duration of the test)
- **AND** exactly one `[:flezha, :plugin, :kafka, :worker, :swapped]` event SHALL fire with `queue_depth ≥ 200` and `in_flight_records: 50`

#### Scenario: Upstream Kayrock parse error does not crash the producer
- **GIVEN** the same producer setup as above
- **WHEN** the KafkaEx worker crashes with `(RuntimeError) Parse error during %Kayrock.Metadata.V5.Request{...} response deserializer` (the exact upstream failure observed in production at 05:25:15) and is restarted by its supervisor
- **THEN** the producer SHALL NOT crash with `FunctionClauseError no function clause matching in Flezha.Plugin.Provider.Kafka.ProducerWorker.handle_info/2`
- **AND** the producer's `state.queue`, `state.in_flight`, and `state.blocked` SHALL be observably preserved across the swap (verified via `:sys.get_state/1` snapshots taken before the crash injection and after the swap completes)
- **AND** no records SHALL be silently dropped between the crash and the swap (verified by comparing the topic end-offset delta to the number of records admitted during the window)

#### Scenario: Swap fails because the broker is unreachable, queue stays admitted
- **GIVEN** a producer with 100 records in `state.queue` and 0 in `state.in_flight`
- **WHEN** the KafkaEx worker dies AND the registry's `:starter` callback returns `{:error, :no_broker}` for the swap attempt
- **THEN** the producer SHALL schedule a retry of the swap using the existing exponential backoff (`retry_backoff_ms(state, attempt)`)
- **AND** all 100 records SHALL remain in `state.queue` with their `from`, `metadata`, `timestamp`, `bytes`, and `key` fields unchanged
- **AND** no `[:produce, :failed]` event SHALL fire for any of them during the outage
- **AND** when the broker recovers within the next `:delivery_timeout_ms` window, the swap SHALL succeed and the 100 records SHALL be delivered

#### Scenario: Defensive EXIT clause does not regress on an unrelated linked-process death
- **GIVEN** a `ProducerWorker` with `trap_exit: true` (the default — required for `terminate/2` cleanliness)
- **WHEN** any linked process other than `state.worker_pid` dies and sends `{:EXIT, pid, reason}` to the producer (today no such linked process exists; this scenario defends against a future code change that adds one)
- **THEN** the producer SHALL log at `:debug` level and continue running
- **AND** SHALL NOT crash with `FunctionClauseError`
- **AND** SHALL NOT initiate a worker swap (the swap is driven only by the typed `{:kafka_worker_dead, …}` message from `WorkerRegistry`)

#### Scenario: Worker swap re-issues InitProducerId and resets sequence
- **GIVEN** an idempotent producer with `state.producer_id = 42`, `state.producer_epoch = 0`, `state.sequence = 1234`
- **WHEN** the worker dies and the swap acquires a fresh worker pid
- **THEN** the producer SHALL re-enter `handle_continue(:maybe_init_producer_id, …)` against the new worker
- **AND** the next outgoing Produce request's `record_batch.producer_id` SHALL be the new `producer_id` returned by `InitProducerId` on the new worker (NOT 42)
- **AND** `state.sequence` SHALL reset to 0 (the broker treats the new producer-id as a fresh idempotent producer)
- **AND** the `[:worker, :swapped]` telemetry event's `producer_id_before` SHALL be 42 and its `producer_id_after` SHALL be the new value

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

The producer SHALL emit `:telemetry.execute/3` events on every produce attempt, retry, drop, failure, success, and metadata refresh. The events form the executor-facing API for monitoring producer health.

| Event | Measurements | Metadata |
|---|---|---|
| `[:flezha, :plugin, :kafka, :produce, :ok]` | `%{batch_size, latency_ms}` | `%{topic, partition, plugin_id}` |
| `[:flezha, :plugin, :kafka, :produce, :retry]` | `%{attempt, backoff_ms}` | `%{topic, partition, plugin_id, reason}` |
| `[:flezha, :plugin, :kafka, :produce, :dropped]` | `%{count}` | `%{topic, partition, plugin_id, queue_depth, overflow_strategy}` |
| `[:flezha, :plugin, :kafka, :produce, :failed]` | `%{attempts, total_ms}` | `%{topic, partition, plugin_id, last_reason}` |
| `[:flezha, :plugin, :kafka, :metadata, :refresh]` | `%{latency_ms}` | `%{trigger, error}` |
| `[:flezha, :plugin, :kafka, :topic, :created]` | `%{partitions, replication_factor}` | `%{topic, plugin_id}` |

Events SHALL be emitted via `:telemetry.execute` (asynchronous fire-and-forget) so attached handlers cannot back-pressure the producer.

#### Scenario: Executor attaches a Prometheus counter
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

The telemetry event becomes the canonical channel by which executors observe permanent delivery failures of async-admitted records. Logs are not the channel; the chain's return value is not the channel; only telemetry is.

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

### Requirement: Dedup key format

The Kafka producer plugin SHALL compute its dedup key from the durable outbox
identity as `<flz.segment>/<outbox_seq>/<epoch>` (where `flz.segment` is
`operative_class/operation_id/segment_key` and `outbox_seq` is
`<record WAL seq>.<emit ordinal>`). When outbox identity metadata is absent
(at-least-once operatives), the plugin SHALL fall back to the caller-supplied
`kafka_msg_key` behaviour unchanged. The plugin SHALL NOT use any value derived
from the prior `processor_id` term in the dedup key composition; per-Operative
identity in the dedup key is captured by `<operative_class>` (the Operative's
module/class name) within `flz.segment`.

#### Scenario: Dedup key uses outbox identity

- **WHEN** the plugin emits a tier record produced by Segment `k1` of Operation
  `s1` of Operative class `MarketOperative` with outbox seq `42.0` and epoch `1`
- **THEN** the message MUST carry a `flezha.dedup_key` header equal to
  `MarketOperative/s1/k1/42.0/1`

#### Scenario: Backward-compat keys are not produced

- **WHEN** any record is emitted by the plugin
- **THEN** its `flezha.dedup_key` MUST NOT contain the substring `processor_` or
  `compute_engine_` (verifiable by a Kafka header inspection in tests)

### Requirement: Effectively-once mode uses the natural key as the message key

For records carrying tier identity metadata, the plugin SHALL use `flz.key` as
the Kafka message key, so compacted/changelog topics converge to the latest value
per key and partitioning is stable across replays. The full identity
(`flezha.dedup_key`) SHALL travel as a record header for consumer-side exact
dedup on append-only topics.

#### Scenario: Message key from flz.key

- **WHEN** a tier record with `flz.key = "BTC/1m/1720000000"` is published
- **THEN** the Kafka message key is `BTC/1m/1720000000` and the identity header
  is present

#### Scenario: Replay lands on the same partition with the same key

- **WHEN** the same logical output is re-driven after a controller restart
- **THEN** the re-published message has an identical message key, identical
  identity header, and the same partition as the original

### Requirement: Append-topic exactness contract is documented

Because the Kafka client provides no transactional producer, the plugin SHALL
document (spec + operator docs) that append-only topics achieve exact-once
consumption via the identity header (consumer-side dedup), while
compacted/changelog topics achieve it via message-key upsert with no consumer
work. The plugin SHALL NOT claim broker-side exactly-once.

#### Scenario: Docs state the boundary

- **WHEN** an operator reads the Kafka sink documentation for the tier
- **THEN** the changelog-vs-append exactness contract and the consumer-side
  header check are stated explicitly

