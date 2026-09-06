# kafka-producer-plugin Specification

## Purpose

The kafka-producer-plugin capability defines the production-grade contract that any Flezha Kafka producer plugin SHALL satisfy when publishing messages from an operative chain to a Kafka cluster. The contract supersedes the prior "thin `KafkaEx.produce` wrapper" behaviour, which surfaced every transient broker condition (`:not_leader_for_partition`, `:no_broker`, `:unknown_topic_or_partition`, `:network_exception`, `:request_timed_out`, `:not_enough_replicas`) directly to the chain, dropped messages on every error, spawned one `KafkaEx.Server` worker per plugin instance, and let upstream mailboxes grow unbounded during outages until the BEAM was OOM-killed. This capability specifies the retry, idempotence, batching, bounded-queue, shared-worker, active metadata refresh, auto-topic-creation, and telemetry semantics a conforming producer SHALL implement, and the configuration surface (`:retries`, `:retry_backoff_ms`, `:retry_max_backoff_ms`, `:delivery_timeout_ms`, `:linger_ms`, `:max_batch_bytes`, `:queue_max`, `:queue_overflow`, `:idempotent`, `:auto_create_topic`, `:auto_create_partitions`, `:auto_create_replication_factor`, `:required_acks`) runners tune to express durability and back-pressure trade-offs. A conforming producer SHALL never surface raw transient broker errors to the chain for messages that ultimately succeed, SHALL bound mailbox and queue growth under sustained outages, and SHALL expose every state transition as a `:telemetry` event so runners can monitor health without parsing log lines.
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
- **AND** a `[:flezha, :plugin, :kafka, :retried]` telemetry event SHALL be emitted for each retry attempt

#### Scenario: Permanent error returns immediately
- **GIVEN** a producer
- **WHEN** `KafkaEx.produce` returns `{:error, :record_list_too_large}`
- **THEN** the plugin SHALL return `{:error, {:permanent, :record_list_too_large}}` without retrying
- **AND** SHALL emit a `[:flezha, :plugin, :kafka, :failed]` telemetry event with `last_reason: :record_list_too_large`

#### Scenario: Delivery timeout exhausts all retries
- **GIVEN** a producer with `:delivery_timeout_ms = 5000`
- **WHEN** every attempt in 5 seconds fails with `:no_broker`
- **THEN** the plugin SHALL return `{:error, {:delivery_failed, :no_broker}}` to the chain
- **AND** SHALL emit a `[:flezha, :plugin, :kafka, :failed]` telemetry event with `attempts: <n>` and `total_ms: ~5000`

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
- **AND** a `[:flezha, :plugin, :kafka, :topic_created]` telemetry event SHALL be emitted

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
5. On worker restart (and only then) accept that the producer-id rotates; in-flight unacked messages may produce duplicates bounded by the records of the in-flight window (`:max_in_flight` batches) at restart.

When `:idempotent` is `false` the producer SHALL fall back to `api_version: 3` Produce requests without producer-id state. This mode is for callers who deliberately want at-most-once semantics.

#### Scenario: Idempotent retry does not duplicate
- **GIVEN** an idempotent producer
- **WHEN** a Produce succeeds server-side but the client times out before receiving the response
- **AND** the plugin retries with the same sequence number
- **THEN** the broker SHALL return success without writing a second copy
- **AND** the topic's log SHALL contain exactly one copy of the message
- **AND** the chain SHALL receive `{:ok, ctx}` exactly once

#### Scenario: Producer-id rotation on worker restart
- **GIVEN** an idempotent producer with two batches of 5 messages in flight (sent, awaiting ack)
- **WHEN** the worker process crashes and is restarted by its supervisor
- **THEN** the new worker SHALL request a new producer-id via `InitProducerId`
- **AND** the in-flight messages, if redelivered from a recovered queue, SHALL produce duplicates bounded by the in-flight window (≤10 in this scenario)
- **AND** the duplicate count SHALL be observable via the `[:flezha, :plugin, :kafka, :shipped]` telemetry stream's `attempt: 1` count vs delivered count

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

When either condition is exceeded and a slot of the in-flight window is free, the batch SHALL be flushed (sent) immediately; while the window is full, arriving messages SHALL accumulate into the next batch, bounded by `:max_batch_bytes`, and leave when a slot frees. Setting `:linger_ms = 0` disables the wait — a request leaves as soon as a slot is free carrying everything queued, so at a free window every message produces its own Produce request. Assembling a batch SHALL be linear in the records it drains: the accumulated byte total rides with the accumulator rather than being re-summed per record.

#### Scenario: 50 trades in 10 ms become one Produce request
- **GIVEN** a producer with `:linger_ms = 10` accepting 50 messages destined for the same partition over 10 ms
- **WHEN** the linger window closes
- **THEN** exactly one Produce request SHALL be issued containing all 50 messages
- **AND** all 50 messages SHALL share the same producer-id and consecutive sequence numbers
- **AND** a single `[:flezha, :plugin, :kafka, :shipped]` telemetry event SHALL be emitted with `batch_size: 50`

#### Scenario: 1 MiB batch flushes mid-window
- **GIVEN** a producer with `:max_batch_bytes = 1048576` and `:linger_ms = 1000`
- **WHEN** accumulated messages reach 1 MiB after 50 ms
- **THEN** the batch SHALL flush at 50 ms without waiting the remaining 950 ms

#### Scenario: linger_ms = 0 sends without waiting
- **GIVEN** a producer with `:linger_ms = 0` and a free window slot
- **WHEN** a single message arrives
- **THEN** a single-message Produce request SHALL be issued immediately
- **AND** messages arriving while the window is full SHALL leave together in the next Produce request when a slot frees

### Requirement: Bounded outbound queue with configurable overflow strategy

Each `(worker, topic, partition)` triple SHALL maintain its own bounded outbound queue. When the queue has reached `:queue_max` (default 10 000) items, the producer SHALL apply the configured `:queue_overflow` strategy:

- `:drop_nothing` (default): synchronously block the chain caller until queue space is available, with a hard timeout of `:queue_block_timeout_ms` (default 5000); if the timeout fires, return `{:error, {:queue_full, :timeout}}`.
- `:drop_oldest`: pop the head of the queue, push the new message at the tail. Increment the per-`(topic, partition)` `dropped_total` counter.
- `:drop_newest`: reject the new message. Increment `dropped_total`.

The default SHALL be `:drop_nothing` for every record, whether or not it serves a delivery-guaranteed outbox. A producer SHALL NOT discard a record by default: dropping output SHALL be a strategy the runner selects explicitly, never one derived from the record's durability class. Back-pressure runs off the emit hot path, so blocking degrades ingest rate rather than silently reducing delivered volume.

The `dropped_total` counter SHALL be exposed via:

- `:telemetry` event `[:flezha, :plugin, :kafka, :overflowed]` per drop event.
- A function `Flezha.Plugin.Provider.Kafka.Producer.stats(plugin_id) :: %{dropped_total: integer, queue_depth: integer, ...}` for synchronous read.

Telemetry alone SHALL NOT satisfy this requirement: `stats/1` is the synchronous surface a runner polls, and a drop that is invisible there is indistinguishable from no drop at all.

#### Scenario: Default overflow blocks rather than dropping
- **GIVEN** a producer configured without `:queue_overflow`, serving output that is not delivery-guaranteed
- **WHEN** the queue reaches `:queue_max` while further messages arrive
- **THEN** the producer SHALL apply `:drop_nothing` back-pressure and SHALL NOT discard any message
- **AND** `dropped_total` SHALL remain 0

#### Scenario: `stats/1` reports drops
- **GIVEN** a producer with `:queue_overflow = :drop_oldest` that has discarded 100 messages
- **WHEN** a runner calls `Flezha.Plugin.Provider.Kafka.Producer.stats(plugin_id)`
- **THEN** the returned map SHALL include a `:dropped_total` key equal to 100
- **AND** the map SHALL also include `:queue_depth`

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

#### Scenario: Drop-nothing mode applies back-pressure
- **GIVEN** a producer with `:queue_overflow = :drop_nothing` and `:queue_block_timeout_ms = 1000`
- **WHEN** the queue is at capacity
- **THEN** the next `on_output` call SHALL block until either space is available OR 1 second elapses
- **AND** if the queue drains within 1 second, the call SHALL succeed normally
- **AND** if 1 second elapses without space, the call SHALL return `{:error, {:queue_full, :timeout}}`

### Requirement: Producer holds at most one shared `KafkaEx.New.Client` worker pid per canonical connection identity

The producer plugin SHALL acquire its `KafkaEx.New.Client` worker pid through `Flezha.Plugin.Provider.Kafka.WorkerRegistry` (the shared, refcounted registry). The registry SHALL key entries on the canonical tuple `{sorted_hosts, phash2(ssl_options), auth fingerprint, lane}`, where `lane` is the provider's `partition mod schedulers_online`: a client encodes each request in its own process, so one client per cluster would serialise every partition's encoding behind one core. Sharing is therefore per scheduler per cluster — two plugin instances with the same broker target whose partitions fall in the same lane reuse the same worker, and a cluster's connection count is bounded by the node's cores, never by its plugin or partition count. The registry SHALL monitor the worker process; on DOWN it SHALL clear the entry AND proactively deliver a typed `{:kafka_worker_dead, {key, token}, reason}` message to every caller pid that still holds a live token against the dead entry. The next `acquire/2` for the same identity SHALL start a fresh worker. The registry SHALL expose a `swap(ref, server \\ __MODULE__) :: {:ok, pid(), ref()} | {:error, term()}` API that performs an in-place re-acquire for an existing caller without double-counting refcount or triggering the linger-stop machinery, intended for callers handling the death-notification message.

Linger: a worker with refs = 0 SHALL be stopped after a configurable `:linger_ms` (default 5000 ms) to absorb plugin-restart churn. The linger timer SHALL be cancelled on the next `acquire/2`.

The `release/2` token returned by `acquire/2` SHALL be opaque to callers — they pass it back unchanged. A token issued against a now-dead entry SHALL no-op on `release/2`.

#### Scenario: Two plugins same identity share a worker
- **GIVEN** plugin A and plugin B configured with the same `:hosts`, `:ssl_options`, `:auth`, producing to partitions in the same lane
- **WHEN** both call `WorkerRegistry.acquire/2` at boot
- **THEN** both SHALL receive the same `worker_pid`
- **AND** the registry entry SHALL show `refs: 2`

#### Scenario: A cluster's partitions spread over one client per scheduler
- **GIVEN** a node with 8 schedulers and one plugin producing to 16 partitions of one cluster
- **WHEN** its per-partition workers acquire their clients
- **THEN** partitions `p` and `p + 8` share a client and the cluster has at most 8 clients, however many plugins or partitions target it

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

When the `KafkaEx.New.Client` worker pid referenced by `state.worker_pid` terminates for any reason (including upstream `Kayrock` parse errors, broker connection loss returning `:einval`, OS-suspend/wake leaving the socket half-dead, or an explicit `KafkaEx.stop_worker/1`), the producer SHALL continue to drain `state.queue`, the in-flight window, and `state.blocked` against a freshly-acquired worker for the same canonical connection identity, **without losing any record that was already admitted to those structures** and **without emitting any `[:flezha, :plugin, :kafka, :failed]` telemetry for records the new worker has not yet positively rejected**.

The producer SHALL learn of the worker's death through a typed `{:kafka_worker_dead, ref, reason}` message delivered by `Flezha.Plugin.Provider.Kafka.WorkerRegistry` to every caller pid that holds a live token against the dying entry (see the `Modified` requirement on the registry below). The producer SHALL NOT rely on the linked-process `:EXIT` signal to drive recovery, although it SHALL match a defensive `handle_info({:EXIT, _pid, _reason}, state)` clause that logs at `:debug` and returns `{:noreply, state}` so any future linked-process addition cannot regress the producer with `FunctionClauseError`.

On receiving `{:kafka_worker_dead, registry_ref, reason}` whose `registry_ref` matches `state.registry_ref`, the producer SHALL:

1. Cancel `state.linger_timer` and `state.retry_timer` if either is set (`Process.cancel_timer/1`), so no stale `:flush_batch` or `:retry_batch` fires between the swap and the next `maybe_kick_batch/1`.
2. Move every in-flight batch back to the head of `state.queue`, oldest first and in order, each with its `attempt` field incremented by 1 and `last_error` set to `{:worker_died, reason}`. The records of an in-flight batch SHALL NOT be replied to or marked failed.
3. Call `WorkerRegistry.swap(state.registry_ref)` to obtain a fresh `{:ok, new_pid, new_ref}` for the same identity. On `{:error, reason}` (the broker is genuinely unreachable), schedule a retry of the swap using the existing `retry_backoff_ms(state, attempt)` curve and leave the queue untouched.
4. On successful swap, set `state.worker_pid = new_pid`, `state.registry_ref = new_ref`, `state.producer_id = nil`, `state.producer_epoch = nil`, `state.sequence = 0`, empty the in-flight window, and re-enter `handle_continue(:maybe_init_producer_id, state)` so the next batch acquires a fresh idempotent producer-id against the new worker.
5. Emit exactly one `[:flezha, :plugin, :kafka, :worker_swapped]` telemetry event with measurements `%{queue_depth: state.queue_size, in_flight_records: prior_in_flight_size, queue_bytes: state.queue_bytes}` — `prior_in_flight_size` counting the records of every batch that was in the window — and metadata `%{plugin_id, topic, partition, reason, producer_id_before, producer_id_after, old_worker_pid, new_worker_pid}`.

`state.blocked` callers SHALL remain blocked across the swap. Their existing `:block_timeout` timers continue to fire normally; a swap is not a `:queue_full` condition.

The producer-id rotation introduced by step 4 SHALL be bounded by the existing `kafka-producer-plugin` "Producer-id rotation on worker restart" requirement — i.e. the in-flight window (now back at the head of the queue) may produce up to its record count in duplicate writes against the broker, exactly as on supervisor-driven restart.

#### Scenario: KafkaEx worker crashes mid-batch and queued records continue flowing
- **GIVEN** a `ProducerWorker` is steadily delivering `btc_demo.ohlc.v1` records at 50 msg/sec and has 200 records in `state.queue`, 50 across the in-flight window, and 0 in `state.blocked`
- **AND** `$MARKET_COMPOSE` names `docker/dev/docker-compose.yml` in the operator's own checkout of the separate `flezha_market` repository, which owns that stack and which this repository does not carry
- **AND** the operator runs `docker compose -f "$MARKET_COMPOSE" exec -T flezha-controller /flezha/bin/iex --remsh flezha@... -e ':sys.terminate(pid, :forced)'` against the live KafkaEx worker pid for the operative's canonical identity
- **WHEN** `WorkerRegistry` observes the worker's DOWN
- **THEN** within 1 second the producer SHALL receive `{:kafka_worker_dead, registry_ref, _reason}`, acquire a new worker via `swap/2`, re-run `InitProducerId`, and resume sending
- **AND** the topic end-offset of `btc_demo.ohlc.v1` (read via `docker compose ... exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic btc_demo.ohlc.v1`) SHALL advance by at least 200 records within the next 10 seconds
- **AND** zero `[:flezha, :plugin, :kafka, :failed]` telemetry events SHALL fire for those records (verified via a `:telemetry` handler attached for the duration of the test)
- **AND** exactly one `[:flezha, :plugin, :kafka, :worker_swapped]` event SHALL fire with `queue_depth ≥ 200` and `in_flight_records: 50`

#### Scenario: Upstream Kayrock parse error does not crash the producer
- **GIVEN** the same producer setup as above
- **WHEN** the KafkaEx worker crashes with `(RuntimeError) Parse error during %Kayrock.Metadata.V5.Request{...} response deserializer` (the exact upstream failure observed in production at 05:25:15) and is restarted by its supervisor
- **THEN** the producer SHALL NOT crash with `FunctionClauseError no function clause matching in Flezha.Plugin.Provider.Kafka.ProducerWorker.handle_info/2`
- **AND** the producer's `state.queue`, the in-flight window, and `state.blocked` SHALL be observably preserved across the swap (verified via `:sys.get_state/1` snapshots taken before the crash injection and after the swap completes)
- **AND** no records SHALL be silently dropped between the crash and the swap (verified by comparing the topic end-offset delta to the number of records admitted during the window)

#### Scenario: Swap fails because the broker is unreachable, queue stays admitted
- **GIVEN** a producer with 100 records in `state.queue` and an empty in-flight window
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

#### Scenario: Worker death rewinds the whole window in order
- **GIVEN** three batches outstanding when the shared client dies
- **WHEN** the producer swaps to a fresh client
- **THEN** the three batches return to the head of `state.queue` oldest first with their attempts incremented, are resent under the new producer id in that order, and the `:swapped` event's `in_flight_records` counts all three batches' records

### Requirement: Active metadata refresh on error classes

When a Produce returns `:not_leader_for_partition`, `:no_broker`, or `:unknown_topic_or_partition`, the producer SHALL force a metadata refresh BEFORE the next retry attempt. The refresh SHALL be bounded by `:metadata_refresh_timeout_ms` (default 5000) and SHALL emit `[:flezha, :plugin, :kafka, :metadata_refresh]` with `trigger: :error_recovery, error: <reason>`.

The default kafka_ex `metadata_update_interval` (passive timer-based refresh) SHALL remain active as a fallback. Active refresh on error is in addition to, not in place of, the timer.

#### Scenario: Active refresh after :not_leader_for_partition
- **GIVEN** a producer that just received `:not_leader_for_partition`
- **WHEN** the retry logic kicks in
- **THEN** before sleeping the backoff, the producer SHALL call `KafkaEx.metadata` (or equivalent) to force a refresh
- **AND** SHALL emit `[:flezha, :plugin, :kafka, :metadata_refresh]` with `trigger: :error_recovery, error: :not_leader_for_partition`
- **AND** the next Produce attempt SHALL use the freshly-refreshed metadata to target the new leader

#### Scenario: Refresh failure does not block retry
- **GIVEN** the metadata refresh itself times out after 5 seconds
- **WHEN** the producer's retry logic continues
- **THEN** the retry SHALL still proceed using the stale metadata
- **AND** SHALL emit `[:flezha, :plugin, :kafka, :metadata_refresh]` with `error: :timeout`

### Requirement: Telemetry events for every state transition

The producer SHALL emit `:telemetry.execute/3` events on every produce attempt, retry, drop, failure, success, and metadata refresh. The events form the runner-facing API for monitoring producer health.

| Event | Measurements | Metadata |
|---|---|---|
| `[:flezha, :plugin, :kafka, :shipped]` | `%{batch_size, latency_ms}` | `%{topic, partition, plugin_id}` |
| `[:flezha, :plugin, :kafka, :retried]` | `%{attempt, backoff_ms}` | `%{topic, partition, plugin_id, reason}` |
| `[:flezha, :plugin, :kafka, :overflowed]` | `%{count}` | `%{topic, partition, plugin_id, queue_depth, overflow_strategy}` |
| `[:flezha, :plugin, :kafka, :failed]` | `%{attempts, total_ms}` | `%{topic, partition, plugin_id, last_reason}` |
| `[:flezha, :plugin, :kafka, :metadata_refresh]` | `%{latency_ms}` | `%{trigger, error}` |
| `[:flezha, :plugin, :kafka, :topic_created]` | `%{partitions, replication_factor}` | `%{topic, plugin_id}` |

Events SHALL be emitted via `:telemetry.execute` (asynchronous fire-and-forget) so attached handlers cannot back-pressure the producer.

#### Scenario: Runner attaches a Prometheus counter
- **GIVEN** a Prometheus telemetry handler attached to `[:flezha, :plugin, :kafka, :overflowed]`
- **WHEN** the producer drops a message via the bounded queue
- **THEN** the Prometheus counter SHALL increment by `count: 1`
- **AND** the counter SHALL include labels `topic, partition, overflow_strategy`

#### Scenario: Slow telemetry handler does not slow produce
- **GIVEN** a deliberately-slow handler attached to `[:flezha, :plugin, :kafka, :shipped]` that sleeps 1 second
- **WHEN** a produce succeeds
- **THEN** the produce-completion latency observed by the chain caller SHALL NOT include the handler's 1-second sleep
- **AND** the slow handler runs asynchronously without blocking the producer

### Requirement: Chain dispatch is non-blocking

The Kafka producer plugin's transform-subscriber callbacks (`on_input/2`, `on_output/2`) SHALL return to their caller in O(1) wall-clock time relative to broker latency, queue depth, and in-flight batch state. "O(1) relative to broker latency" means: the callback's runtime SHALL NOT include any synchronous wait on a broker round-trip, on `KafkaEx` reply timing, or on the producer worker's batch flush schedule.

The callback SHALL be implemented as a `GenServer.cast` to the per-(plugin_id, topic, partition) worker, returning immediately after admission. Admission SHALL apply the configured `:queue_overflow` strategy (per the existing "Bounded outbound queue with configurable overflow strategy" requirement). The synchronous `enqueue/4` API is retained for tests, ad-hoc tooling, and explicit-flush call sites; chain-facing callbacks SHALL NOT use it.

Under `:drop_nothing` — the default — a cast-admitted record that finds the queue full SHALL wait inside the worker for room instead of being dropped; the chain caller is never blocked, because the record, not the caller, is what waits. Room SHALL be made only by a drained batch, never by evicting an admitted record. A synchronous `enqueue/4` caller additionally waits out its own `:queue_block_timeout_ms`.

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
- **AND** the worker's queue depth SHALL trend toward `:queue_max` and trigger `[:flezha, :plugin, :kafka, :overflowed]` events under the configured overflow strategy
- **AND** zero `OOM` events SHALL occur
- **AND** the protobuf `timestamp_ms` (or any caller-provided event timestamp) SHALL NOT diverge from the host process's wall-clock by more than the worker's `:linger_ms + :request_timeout_ms` window

#### Scenario: Drop-nothing admission from the chain holds the record, not the caller
- **GIVEN** a producer worker with `:queue_overflow` unset (so `:drop_nothing`)
- **AND** the worker's queue is at `:queue_max`
- **WHEN** `on_output/2` admits a new message via cast
- **THEN** the chain caller SHALL still observe `{:ok, ctx}` immediately
- **AND** the record SHALL wait in the worker and be produced once a batch drains, with no `[:flezha, :plugin, :kafka, :overflowed]` event and `dropped_total` unchanged
- **AND** the synchronous `enqueue/4` call from a test-mode caller SHALL still apply the documented drop-nothing semantics (block until space or `:queue_block_timeout_ms`)

### Requirement: Failure surfacing for async-admitted records

Records admitted via `cast_enqueue/3` (the chain dispatch path) SHALL NOT receive a synchronous reply — there is no caller to reply to. The worker SHALL track the record's caller as `:async` in its internal envelope. On permanent failure of a batch containing async-admitted records, the worker SHALL:

- Emit `[:flezha, :plugin, :kafka, :failed]` for each affected record, with metadata including `caller: :async`, `last_reason`, `attempt_count`, `delivery_elapsed_ms`, `topic`, `partition`, `plugin_id`.
- Drop the records from the queue without invoking `GenServer.reply/2`.
- Continue draining subsequent batches.

The telemetry event becomes the canonical channel by which runners observe permanent delivery failures of async-admitted records. Logs are not the channel; the chain's return value is not the channel; only telemetry is.

Sync-admitted records (`enqueue/4`) continue to receive `{:error, {:permanent, reason}}` or `{:error, {:delivery_failed, reason}}` per the existing requirements; the cast vs call dichotomy is observable only from the producer worker's perspective.

#### Scenario: Async-admitted record permanently fails
- **GIVEN** a chain has admitted a message via `on_output/2` (cast path)
- **WHEN** `KafkaEx.produce` returns `{:error, :record_list_too_large}` (a permanent error)
- **THEN** the worker SHALL emit one `[:flezha, :plugin, :kafka, :failed]` event with `caller: :async, last_reason: :record_list_too_large`
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
identity as `<flz.entity>/<outbox_seq>/<epoch>` (where `flz.entity` is
`operative_class/procedure_id/entity_key` and `outbox_seq` is
`<record WAL seq>.<emit ordinal>`). When outbox identity metadata is absent
(at-least-once operatives), the plugin SHALL fall back to the caller-supplied
`kafka_msg_key` behaviour unchanged. The plugin SHALL NOT use any value derived
from the prior `processor_id` term in the dedup key composition; per-Operative
identity in the dedup key is captured by `<operative_class>` (the Operative's
module/class name) within `flz.entity`.

#### Scenario: Dedup key uses outbox identity

- **WHEN** the plugin emits a tier record produced by Entity `k1` of Procedure
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

### Requirement: A delivery-guaranteed outbox is served without silent loss

A producer chain serving an effectively-once (delivery-guaranteed) outbox SHALL
back-pressure the outbox flush — which runs off the ingest hot path — under the
`:drop_nothing` default rather than evict admitted records. An operator MAY still
configure `drop_oldest` or `drop_newest` explicitly on such a chain; each
resulting drop SHALL emit an alarm-grade event, not only the `dropped_total`
counter. On permanent failure of a batch carrying guaranteed records, the
producer SHALL surface the failure to the outbox confirmation seam (so the batch
parks through the dead-letter path) instead of dropping the records with only
`:produce, :failed` telemetry. Plain (non-guaranteed) chains share the
`:drop_nothing` default; the alarm-grade drop event and the outbox park path remain specific to
guaranteed records.

#### Scenario: Overflow back-pressures the flush, not the broker's data

- **WHEN** the broker is unreachable and a guaranteed chain's queue reaches
  `:queue_max`
- **THEN** the outbox flush blocks and no admitted record is evicted; ingest
  and effect-record durability are unaffected

#### Scenario: Confirmed batches report to the outbox

- **WHEN** a produce for a guaranteed batch is acknowledged by the broker
- **THEN** the producer reports the batch's outbox range confirmed, at the
  same completion point where produce telemetry is emitted today

#### Scenario: Permanent failure escalates instead of vanishing

- **WHEN** a guaranteed batch exhausts its retry budget with a permanent error
- **THEN** the records are handed to the outbox's park path with their range,
  and are not dropped from the queue as if delivered

### Requirement: Every documented producer option is implemented

Each option the producer documents — in its `@moduledoc` parameter table, its struct, or `docs/plugins/kafka_producer.md` — SHALL be read by the implementation and SHALL affect behaviour as documented. An option that is carried but never read, or documented but absent, SHALL be removed from the documented surface rather than left inert.

A silently inert option is a correctness hazard: a runner sets it to express a durability or routing intent, observes no error, and reasonably concludes the intent took effect.

#### Scenario: A carried-but-unread option is removed or implemented
- **GIVEN** `:partitioner` is present in the producer's option table and struct but is read nowhere in the produce path
- **WHEN** the producer's documented surface is reconciled
- **THEN** `:partitioner` SHALL either select the destination partition as documented, or be absent from both the struct and the documented option table

#### Scenario: A documented metadata override exists or is not documented
- **GIVEN** `docs/plugins/kafka_producer.md` documents a per-message `kafka_msg_partition` metadata override while the partition is fixed from worker state
- **WHEN** the producer's documented surface is reconciled
- **THEN** the override SHALL either route the record to the named partition, or be absent from the documentation

### Requirement: A window of Produce requests is in flight per partition

The producer SHALL keep up to `:max_in_flight` Produce requests outstanding per `(topic, partition)` on one broker connection, sent in batch order. `:max_in_flight` SHALL default to `5` when `:idempotent` is `true` and to `1` otherwise, and a value above `1` together with `:idempotent = false` SHALL be rejected at plugin preparation with a typed configuration error naming both options, because only the broker's per-partition sequence check makes a retry inside a window order-preserving. The transport SHALL pipeline: the next request leaves before the previous response returns, responses are matched to requests in connection order by correlation id, and a frame whose correlation id is not the oldest outstanding request's SHALL close the connection and fail every request outstanding on it with a retriable error rather than be delivered to any request. A synchronous request issued on a connection with pipelined requests outstanding SHALL be answered in its turn behind them.

#### Scenario: Five batches leave before the first response
- **GIVEN** an idempotent producer with `:max_in_flight = 5` and `:linger_ms = 0` fed 5 000 records against a broker with a 2 ms round trip
- **WHEN** the worker drains its queue
- **THEN** up to five Produce requests are outstanding at once, each batch's sequence numbers continue the previous batch's, and the broker's log holds the 5 000 records exactly once, in admission order

#### Scenario: Without idempotence the window is one
- **WHEN** a plugin is prepared with `:idempotent = false` and no `:max_in_flight`
- **THEN** exactly one Produce request is ever outstanding per partition, as before this change

#### Scenario: A wide window without idempotence is refused
- **WHEN** a plugin is prepared with `:idempotent = false` and `:max_in_flight = 3`
- **THEN** preparation fails with a typed error naming `:max_in_flight` and `:idempotent`, and no producer worker starts

#### Scenario: A misordered frame never acknowledges the wrong batch
- **WHEN** a frame arrives on a pipelined connection whose correlation id is not the oldest outstanding request's
- **THEN** the connection is closed, every request outstanding on it fails with a retriable error, and no batch is marked delivered by that frame

#### Scenario: A synchronous request waits its turn
- **WHEN** the worker issues a metadata refresh on a connection with three Produce requests outstanding
- **THEN** the refresh's response is read after the three Produce responses and each response reaches its own request

### Requirement: A retry inside the window preserves partition order

When a batch fails retriably while later batches are outstanding behind it, the producer SHALL stop issuing new requests, await the outcome of every later outstanding batch, and then resend the failed batch and every batch behind it in their original order with their original sequence numbers before anything newer leaves. A later batch the broker rejected with `out_of_order_sequence_number` while an earlier batch is unresolved SHALL be treated as failed behind that earlier batch, not as a producer fence. A per-batch dispatch timeout SHALL be a retriable failure of that batch under the same rule. A fence on the oldest outstanding batch SHALL re-initialise the producer session and realign the sequence numbers of the whole window in order before resending it in order.

#### Scenario: The head fails, the tail is resent behind it
- **GIVEN** batches k, k+1 and k+2 outstanding
- **WHEN** k fails with `:request_timed_out` and the broker answers k+1 and k+2 with `out_of_order_sequence_number`
- **THEN** the producer resends k, then k+1, then k+2 with their original base sequences, the broker's log receives them once in that order, and no newer batch leaves before k+2 has been resent

#### Scenario: A tail failure does not resend the head
- **GIVEN** batches k and k+1 outstanding
- **WHEN** k succeeds and k+1 fails retriably
- **THEN** only k+1 is resent, with its original sequence numbers, and k's records are confirmed exactly once

#### Scenario: A timed-out head is retried in order
- **GIVEN** batches k and k+1 outstanding
- **WHEN** k's dispatch timeout fires before any response
- **THEN** the producer awaits k+1's outcome, then resends k and k+1 in that order with their original sequence numbers

### Requirement: `:queue_overflow` names what it discards, and the retired spelling is refused

The `:queue_overflow` option's three values SHALL be `:drop_nothing`, `:drop_oldest` and `:drop_newest`, spelled exactly as the SDK overflow policy spells them, because both surfaces answer one question — what does a full queue discard? — and an operator reading a Flezha pipeline SHALL meet one answer to it.

<!-- overflow-naming-record:begin -->
The retired spelling `:block` SHALL NOT be accepted on this option, on any path: no alias, no coercion, no deprecation window.
<!-- overflow-naming-record:end -->
A configuration carrying it SHALL be refused with a message naming the option and the offered value, never silently mapped onto the default. A CI gate SHALL fail a build that reintroduces the retired spelling on this surface, and that gate SHALL match the option's own name rather than requiring the retired value to appear beside a sibling value.

The option remains operator configuration on a controller-side plugin provider. It is not an authoring surface, carries no SDK spelling and crosses no wire, so no SDK is owed a companion change.

#### Scenario: A configuration offering the retired spelling is refused

- **WHEN** a plugin is configured with `queue_overflow` set to the string `block`
- **THEN** the configuration SHALL be refused with a message naming `queue_overflow` and the offered value
- **AND** the producer SHALL NOT fall back to the default policy

#### Scenario: The gate catches the option's own shape

- **WHEN** a source, document or specification line names `:queue_overflow` and the retired policy spelling, with no sibling policy value beside it
- **THEN** `scripts/check-overflow-naming.sh` SHALL exit non-zero naming the file and line

### Requirement: The queue-overflow policy is validated where it is read

`init_provider/1` SHALL refuse a `:queue_overflow` value that is not one of the three policies, in either its atom or its CLI-string spelling, and SHALL do so before any producer worker is started. The refusal SHALL name the option and the offered value, and SHALL be returned as `{:error, binary()}` — the arm the plugin host already handles — never raised.

An absent option, and an option explicitly set to `nil`, SHALL take the default policy: the option is optional, and only a value that was supplied and is not a policy is an error.

Validation SHALL live on the path an operator's stored configuration travels. A check reachable only from the CLI string-map builder does not satisfy this requirement, because a plugin spec and a stored capability row reach `init_provider/1` without ever constructing the provider struct.

A non-policy value SHALL NOT be coerced, defaulted or carried into the worker. Carrying it there makes an invalid configuration indistinguishable from a valid one until the queue reaches `:queue_max`, at which point the worker exits and the records it held are lost.

#### Scenario: A misspelled policy is refused at enable time

- **GIVEN** a plugin configured with `queue_overflow` set to `:drop_odlest`
- **WHEN** the plugin is enabled
- **THEN** `init_provider/1` SHALL return `{:error, message}` where the message names `queue_overflow` and the offered value
- **AND** no producer worker SHALL be registered for that `(plugin, topic, partition)`

#### Scenario: A misspelled policy in its string form is refused the same way

- **GIVEN** a plugin configured with `queue_overflow` set to the string `drop_odlest`
- **WHEN** the plugin is enabled
- **THEN** `init_provider/1` SHALL return `{:error, message}` naming the option and the offered value
- **AND** it SHALL NOT raise, whether or not the string happens to name an atom already loaded in the VM

#### Scenario: Every policy is accepted in both spellings

- **GIVEN** a plugin configured with `queue_overflow` set to any of `:drop_nothing`, `:drop_oldest`, `:drop_newest` or their string forms
- **WHEN** the plugin is enabled
- **THEN** the producer worker SHALL be started with that policy in force
- **AND** the options returned to the host SHALL be unchanged by the validation

#### Scenario: An unset policy takes the default

- **GIVEN** a plugin whose configuration omits `queue_overflow`, or sets it to `nil`
- **WHEN** the plugin is enabled
- **THEN** the producer worker SHALL be started with `:drop_nothing` in force

### Requirement: The SASL configuration is validated where it is read

`init_provider/1` SHALL refuse an `:auth` map carrying a key outside `mechanism`, `username`, `password` and `mechanism_opts`, or a `mechanism` outside the set the broker client supports, and SHALL do so before any producer worker or broker client is started. The refusal SHALL name the key or the mechanism and SHALL be returned as `{:error, binary()}`, never raised.

An unknown key SHALL NOT be silently dropped. The broker client's constructor takes only the four keys it knows, so a misspelled `username` today means *no username* — a configuration that reads as accepted and authenticates as anonymous.

Operator-supplied auth text SHALL NOT be resolved to an atom by name. Resolving it raises for a string that names no existing atom and succeeds for any string that happens to name one, which is the same defect in both directions: a crash for one typo and a silently wrong value for another.

Validation SHALL happen once, at preparation, and the resolved configuration SHALL be what the client-restart closure re-reads. The closure is re-invoked on every client restart so a rotated password environment variable takes effect; a validation living inside it would raise inside the shared worker registry rather than at the enable that supplied the bad value.

A `%KafkaEx.Auth.Config{}` supplied directly, and an absent `:auth`, SHALL be accepted unchanged.

#### Scenario: An unsupported mechanism is refused at enable time

- **GIVEN** a plugin configured with `auth` carrying `mechanism` set to a value the broker client does not support
- **WHEN** the plugin is enabled
- **THEN** `init_provider/1` SHALL return `{:error, message}` naming the mechanism
- **AND** no producer worker and no broker client SHALL have been started

#### Scenario: An unknown auth key is refused rather than dropped

- **GIVEN** a plugin configured with `auth` carrying a key that is not one of the four the broker client accepts
- **WHEN** the plugin is enabled
- **THEN** `init_provider/1` SHALL return `{:error, message}` naming the key

#### Scenario: A valid SASL configuration is unaffected

- **GIVEN** a plugin configured with a supported mechanism, a username and a password, in either their atom or their string spelling
- **WHEN** the plugin is enabled
- **THEN** the resolved auth configuration SHALL be the one the broker client receives
- **AND** the password SHALL still be redacted in the returned and persisted opts

