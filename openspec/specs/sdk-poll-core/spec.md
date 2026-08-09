# sdk-poll-core Specification

## Purpose
TBD - created by archiving change extract-sdk-poll-core. Update Purpose after archive.
## Requirements
### Requirement: Poll/complete is the only invocation surface

`flz_core` SHALL expose work to the host exclusively through `poll() -> Activation` and
`complete(ActivationResult)`. The core SHALL NOT invoke host code, hold references to
host functions, or create threads that execute host code. All user-code parallelism is
the host's: the host chooses how many workers poll and what concurrency primitive they
use.

#### Scenario: Host owns worker count
- **WHEN** a host runs 1 poll worker and then 8 poll workers against the same core and workload
- **THEN** the core SHALL function correctly in both configurations, and distinct entities' activations MAY be processed concurrently when multiple workers poll

#### Scenario: No host callback registration exists
- **WHEN** the core's public API surface is enumerated
- **THEN** it SHALL contain no function that accepts a host function pointer or closure for later invocation on a core-owned thread (the log callback on the FFI layer excepted)

### Requirement: Per-entity exclusivity by handout

The core SHALL NOT hand out two activations for the same `(operative_instance,
procedure, entity_key)` concurrently: an entity with an outstanding (polled,
uncompleted) activation SHALL NOT appear in any other poll result until its completion
arrives. This restates the existing single-active-drainer invariant at the boundary.

#### Scenario: Same entity never outstanding twice
- **WHEN** two host workers poll concurrently while 100 inputs for one entity and 100 for another are queued
- **THEN** at no point SHALL both workers hold activations for the same entity, and each entity's inputs SHALL be delivered in enqueue order across its successive activations

#### Scenario: Completion releases the entity
- **WHEN** an activation for entity E is completed and E has further queued inputs
- **THEN** a subsequent poll SHALL be able to return E's next activation

### Requirement: Activation is a per-entity batch

An Activation SHALL target exactly one entity and carry a batch of one or more items,
each being an input (payload bytes, metadata, `consumed_offset`, replay flag) or a
timer firing (kind, scheduled timestamp). Each item SHALL carry the core-evaluated
`now` for the declared clock; the host SHALL NOT substitute ambient time for
`ctx.now()`. Timer firings SHALL be validated against the pending set by the core
before inclusion (a stale entry fires nothing).

#### Scenario: Batch amortizes the boundary
- **WHEN** 50 inputs for one entity are queued before any worker polls
- **THEN** the core MAY return them as one activation batch, and the host SHALL process them in order within the batch

#### Scenario: fired_at is the scheduled timestamp
- **WHEN** a timer item with scheduled timestamp T is included in an activation
- **THEN** the item's `now` SHALL equal T, so a chained `timer_after` anchors on the firing that woke it (replay-stable)

### Requirement: ActivationResult carries the effect record

An ActivationResult SHALL carry, per processed item: host-encoded change bytes
(core-opaque), outputs, `consumed_offset`, timer ops, optional checkpoint flag,
optional DLQ entry, and a verdict of `ok`, `retryable_error`, or `non_retryable_error`.
The core SHALL consume it exactly as it consumes a flush-buffer drain today: same WAL
framing, same credit debit, same retry→park ladder (a `non_retryable_error` parks
immediately; exhausted retries park; a park closes the checkpoint interval).

#### Scenario: Non-retryable parks immediately
- **WHEN** a completion reports `non_retryable_error` for an item
- **THEN** the core SHALL park the payload via the existing DLQ path without further redelivery attempts

#### Scenario: Retryable re-delivers with backoff
- **WHEN** a completion reports `retryable_error` and the channel's retry budget is not exhausted
- **THEN** the core SHALL re-hand the item in a later activation for that entity after the existing backoff ladder, and the retry counter SHALL ride the existing telemetry

### Requirement: Host-participating commands are activation kinds

The five control interactions requiring host participation SHALL be expressed as
activation kinds — `RestoreFold`, `SnapshotRequest`, `LiveStateRead`, `ApplyState`,
`MigrationSerialize` — subject to the same per-entity exclusivity. All other control
commands SHALL be handled entirely inside the core and SHALL NOT surface to the host.

#### Scenario: Restore is a fold handed to the host
- **WHEN** the controller ships `RestoreInitState` for an entity with a persisted change log
- **THEN** the host SHALL receive a `RestoreFold` activation carrying the records, and input activations for that entity SHALL NOT be handed out before its fold completes

#### Scenario: Snapshot request preserves compaction semantics
- **WHEN** a `SnapshotRequest` activation completes with encoded state bytes
- **THEN** the core SHALL drain pending changes first and stamp the snapshot with the highest already-shipped seq (`change_seq - 1` semantics), unchanged from today

#### Scenario: Ping never reaches the host
- **WHEN** the controller sends `Ping`
- **THEN** the core SHALL answer without any activation being generated

### Requirement: Edge transforms and keys evaluate producer-side

Consumer-edge transform chains and `key_by` SHALL be applied by the host when
completing the activation that produced the value; outputs SHALL carry
`(consumer_edge_id, bytes, key?)` and the core SHALL route on `(edge, key)` without
evaluating host closures. Unkeyed entry inputs into a keyed edge SHALL take a batched
`KeyRequest` activation before routing. A transform or key failure SHALL be reported in
the completion and SHALL enter the existing retry→park ladder.

#### Scenario: Replay re-keys identically
- **WHEN** a journaled slice is re-forwarded by the controller through a keyed edge
- **THEN** the host applies the same transform chain and key function at the same seam, and each value SHALL route to the same entity as the live run

#### Scenario: Entry input into a keyed edge
- **WHEN** an entry-channel input arrives for a consumer reached only via a keyed edge
- **THEN** the core SHALL obtain the key via a `KeyRequest` activation before enqueueing to any entity mailbox, and no keyless drop path SHALL exist

### Requirement: Host classes reach full capability

The host layer (`flz_core::host`) SHALL support, for byte-level host
classes, everything the typed Rust host can declare: single, static, and
dynamic entity modes (dynamic with idle-TTL eviction and the bounded
evicted-keys trace), the effectively-once tier and checkpoint
ack-granularity (completions carrying `consumed_offset` / `checkpoint`
drive the same effect-record and interval semantics), late-input
correction (window declaration, admission classification, slice replay),
per-entity timers of both kinds, and inter-procedure edges whose values
cross as bytes routed on `(edge, key)`. Class declarations SHALL carry
these facts and the core SHALL populate `AddOperative` response metadata
from them exactly as the typed path does.

#### Scenario: A dynamic host class routes by key and evicts idle entities
- **WHEN** a host class declares a dynamic procedure with an idle TTL and inputs for keys `a` and `b` arrive
- **THEN** the core SHALL create the entities lazily via the prepare crossing's keys, hand out per-entity activations exclusively, and after the TTL SHALL evict destructively, recording the key in the eviction trace

#### Scenario: An effectively-once host class holds its watermark to checkpoints
- **WHEN** a checkpoint-ack host class completes three inputs without `checkpoint` and a fourth with it
- **THEN** the shipped effect record SHALL carry the accumulated changes with the fourth input's offset, and a crash before it SHALL replay all four inputs

#### Scenario: Inter-procedure values cross as bytes
- **WHEN** a host procedure's completion carries an output targeting a consumer edge with a key
- **THEN** the core SHALL enqueue the bytes to the consumer entity selected by `(edge, key)` and the consumer's next activation SHALL carry them as an input item, with no host closure evaluated in routing

### Requirement: The prepare crossing serves keys and event times

The core SHALL batch, for host classes whose edges are keyed or whose
clock is an event time, the questions it cannot answer from bytes into one
prepare crossing (the widened `KeyRequest`): per payload the host SHALL
return the key and/or the extracted event time, and the core SHALL resolve
the crossing before any mailbox enqueue.
Admission, classification, the high-water, and due-timer generation remain
core-side; a prepare failure enters the input channel's retry→park ladder.

#### Scenario: Event time arrives before admission
- **WHEN** an input for an event-time-clocked host class is received
- **THEN** the core SHALL obtain its event time via the prepare crossing and classify it against the high-water BEFORE enqueueing, and a Correctable verdict SHALL drop-and-request exactly as for the typed host

#### Scenario: A key failure parks instead of dropping keylessly
- **WHEN** the host answers a prepare batch with a failure for one payload
- **THEN** that payload SHALL climb the channel's retry→park ladder and no keyless-drop path SHALL exist

### Requirement: Bounded lifecycles run under poll

Bounded host classes SHALL be driven by the core's bounded driver with
stage execution handed out as work activations: the driver accumulates a
stage's per-entity batch, hands it to the host, and its barriers await
host completions AND controller acks in the same order the driver
scenarios pin per loop mode (`Once | PerInput | Chained`). Saga
compensation crosses as a dedicated work-item flavor carrying the recorded
outputs; the proc-journal, staged outbox, durable loop, suspension,
entity-retry, and rewind semantics are unchanged. The `run`-sugar class is
expressible with no journal and no barriers.

#### Scenario: A run-sugar bounded host class completes
- **WHEN** a bounded host class declaring only a run body is spawned with args
- **THEN** the host receives one activation carrying the seed, its completion's terminal outcome drives the same `BoundedCompleted` teardown the typed path produces

#### Scenario: A structured chain's barrier waits for the host and the ack
- **WHEN** a durable structured bounded host class finishes a stage's activations
- **THEN** the driver SHALL commit the proc boundary only after every entity's completion arrived AND the controller acked `ProcCommit`, and a crash-resume SHALL skip journaled procs exactly as the typed suites pin
