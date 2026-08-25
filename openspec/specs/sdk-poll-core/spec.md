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

Because the handout is what gates the entity, every polled activation SHALL be
answered exactly once, by one of three calls: **complete**, **fail** (the
retry-ladder path), or **abandon** — a new call that re-hands the activation's
untouched items and releases the entity. A host SHALL make the answer
structural, so that an unwind, an exception, or an early return cannot skip it;
discipline at each return site is not sufficient. Abandon SHALL re-hand only
items the host has not processed, SHALL NOT count against the retry ladder, and
SHALL leave the entity exactly as if it had never been polled.

The core SHALL NOT reclaim an outstanding activation on a timer or deadline. A
long-running activation and a lost handout are indistinguishable at the
boundary, and re-handing on a deadline would re-execute work the
effectively-once tier promises once. Recovery from a handout a host lost despite
the above is the controller's operative rebind, which resumes from durable
state.

#### Scenario: Same entity never outstanding twice
- **WHEN** two host workers poll concurrently while 100 inputs for one entity and 100 for another are queued
- **THEN** at no point SHALL both workers hold activations for the same entity, and each entity's inputs SHALL be delivered in enqueue order across its successive activations

#### Scenario: Completion releases the entity
- **WHEN** an activation for entity E is completed and E has further queued inputs
- **THEN** a subsequent poll SHALL be able to return E's next activation

#### Scenario: An unwound dispatch abandons rather than retiring the entity
- **WHEN** a host worker holding an activation for entity E unwinds (panic, exception, or early return) without completing it
- **THEN** the activation SHALL be abandoned, E's items SHALL be re-handed untouched, a subsequent poll SHALL be able to return E's next activation, and no retry-ladder attempt SHALL have been consumed

#### Scenario: A slow activation is never reclaimed
- **WHEN** an activation stays outstanding for longer than any configured interval because user code is still running
- **THEN** the core SHALL NOT re-hand it, SHALL NOT hand the entity to another worker, and the activation SHALL complete normally whenever user code returns

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
dynamic entity modes (dynamic with idle-TTL eviction and the workflow
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

### Requirement: Workflow lifecycles run under poll

Workflow classes of BOTH language paths SHALL be driven by the core's workflow
driver with stage execution handed out as activations through the poll seam:
the driver accumulates a stage's per-entity batch, submits one step activation
per entity partition, and its barriers await step completions AND controller
acks in the same order the driver scenarios pin per loop mode
(`Once | PerInput | Chained`). No core-owned thread SHALL execute user code
and no core thread SHALL block awaiting a host completion — the driver's
count-based barrier is the only wait, and the work-stealing pool SHALL NOT
exist. Saga compensation crosses as a dedicated activation flavor carrying the
recorded outputs; the proc-journal, staged outbox, durable loop, suspension,
entity-retry, and rewind semantics are unchanged. The `run`-sugar class is
expressible with no journal and no barriers.

#### Scenario: A run-sugar workflow host class completes
- **WHEN** a workflow host class declaring only a run body is spawned with args
- **THEN** the host receives one activation carrying the seed, its completion's terminal outcome drives the same `WorkflowCompleted` teardown the typed path produces

#### Scenario: A structured chain's barrier waits for the host and the ack
- **WHEN** a durable structured workflow class finishes a stage's activations
- **THEN** the driver SHALL commit the proc boundary only after every entity's completion arrived AND the controller acked `ProcCommit`, and a crash-resume SHALL skip journaled procs exactly as the driver suites pin

#### Scenario: A Java workflow step costs one thread, not two
- **WHEN** a Java workflow class runs a stage step
- **THEN** exactly one thread SHALL be occupied by that step (the SDK thread
  running the code); no core thread SHALL be parked awaiting its reply — the
  reply SHALL be collected by the driver at its stage barrier

#### Scenario: Typed Rust steps run on the SDK's poll workers
- **WHEN** a typed Rust workflow class runs a parallel stage
- **THEN** its per-entity step activations SHALL be picked up and executed by
  the same worker threads that serve stream activations, sibling entities
  MAY genuinely interleave, and a step that panics SHALL report failure
  through the barrier without killing the worker that ran it

### Requirement: Deferred wakes never hold a thread

The core SHALL maintain one deferred-wake list — "this entity becomes eligible
at time T" — serviced by threads already waiting in `poll` and checked on every
`poll` entry. Recording a deferred wake SHALL return the calling thread
immediately. A retryable failure's backoff SHALL be expressed as a deferred
wake: the failed head and unprocessed tail ride the existing per-entity
holdback, the entity's exclusivity gates stay held across the window exactly as
they are across an in-line backoff, and the release-then-reclaim runs when the
entry comes due. No thread SHALL sleep out a backoff delay, and no dedicated
thread SHALL exist to service the list.

#### Scenario: Correlated failure does not stall the runner

- **WHEN** every entity's activation is failing retryably (a shared dependency
  is down) on a runner whose worker count equals the CPU count
- **THEN** workers SHALL remain available to poll and run activations for
  entities whose work would succeed, and each failing entity SHALL be re-handed
  no earlier than its backoff due time

#### Scenario: Backoff preserves ordering and the holdback contract

- **WHEN** a retryable failure defers an entity with a failed head and an
  unprocessed tail
- **THEN** the next activation for that entity SHALL begin with the held-back
  items in their original order, ahead of anything that arrived during the
  backoff window, and no other thread SHALL drain that entity before the due
  time

#### Scenario: Shutdown is not delayed by backoff

- **WHEN** shutdown begins while deferred wakes are pending
- **THEN** pollers SHALL observe shutdown immediately, with no backoff sleep
  left to run out

### Requirement: The core caps jobs underway and signals starvation

The core SHALL own one process-wide limit on concurrently outstanding work
activations (default 1024, env `FLEZHA_MAX_INFLIGHT_ACTIVATIONS`), enforced at
work handout: when the limit is reached, `poll` SHALL NOT hand out entity work,
and a completion that drops the count below the limit SHALL resume handout.
The limit SHALL NOT gate direct command activations (restore folds, snapshot
requests, workflow step crossings — these are barrier-bounded by their drivers)
nor the inline-serve deadlock-avoidance path.

The core SHALL expose starvation — runnable work exists, no thread is inside
`poll`, and the limit is not reached — as a blocking wait a host thread may
park in, answering in the same three-way shape as `poll` (observed / timeout /
shutting down). No host-callback registration SHALL be introduced.

Starvation SHALL mean stuck, not busy: queued entity work SHALL count only
once it has waited a dwell interval during which no thread entered `poll` at
all, so that a saturated runner — which re-enters `poll` once per batch —
never reads as starved. A due deferred wake SHALL count immediately, since
every `poll` pass promotes what has come due.

#### Scenario: Handout stops at the cap and resumes on completion

- **WHEN** the outstanding work count reaches the configured limit while more
  entity work is queued
- **THEN** polling threads SHALL block as if no work were ready, and a single
  completion SHALL allow the next poll to return an activation

#### Scenario: The cap does not gate command activations

- **WHEN** the outstanding work count is at the limit and a direct command
  activation (e.g., a restore fold) is enqueued
- **THEN** the next poll SHALL return the command activation ahead of any
  entity work

#### Scenario: Starvation wakes a parked supervisor

- **WHEN** entity work is queued while every poller is inside user code (none
  waiting in `poll`) and the limit is not reached
- **THEN** a host thread parked in the starvation wait SHALL be woken

### Requirement: A running workflow instance holds one driver thread

The core SHALL drive each actively-running workflow instance from one dedicated
thread, which holds the chain's position as a call stack and SHALL NOT run
user code. This is a stated design position, not an incidental one: the
per-instance resident cost SHALL be dominated by the instance's heap rather
than by that thread, and the thread SHALL NOT poll while waiting at a stage
barrier. Its stack size SHALL be configurable
(`FLEZHA_WORKFLOW_DRIVER_STACK_KIB`) and SHALL default to the platform default.

A change that would make a workflow instance's concurrency bounded by work in
flight rather than by instance count SHALL be justified against a measurement,
not a prediction; `sdk/rust/tests/workflow_scaling.rs` is that measurement, and
`docs/resilience/workflow_operative.md` records the envelope it produced.

#### Scenario: A blocked population's cost is its heap, not its threads

- **WHEN** thousands of workflow instances are simultaneously blocked
- **THEN** the resident cost per instance SHALL be dominated by per-instance
  heap, and SHALL NOT change materially with the configured driver stack size —
  a blocked driver's stack is not resident

#### Scenario: A driver waiting at a stage barrier consumes no CPU

- **WHEN** a workflow instance is blocked mid-step, waiting on its stage barrier
- **THEN** its driver SHALL wait without polling, so a population blocked in
  that state SHALL consume no measurable CPU

#### Scenario: The scaling probe answers the question on demand

- **WHEN** the per-instance cost of the driver thread is re-opened
- **THEN** the probe SHALL report resident and virtual cost per instance,
  thread count per instance, CPU while blocked, start and teardown latency, and
  an unrelated instance's latency beside the population — for a population
  blocked mid-step and for one blocked in the inbox wait

### Requirement: Handout liveness is observable without a new frame

The core SHALL expose two counters over its existing self-report: the number of
activations currently outstanding, and the age in milliseconds of the oldest
outstanding activation (zero when none). Every SDK SHALL carry them in the
data-channel heartbeat's metadata map beside the counters it already reports
(`dispatches`, `blocked_parks`, `credit_parked`, the timer counters), so the
controller reads them through the liveness snapshot it already builds — no new
frame, no new payload id, and no new sensor process.

The counters are reporting only: nothing in the SDK SHALL change behaviour based
on them.

#### Scenario: A stalled entity is visible while the runner looks healthy
- **GIVEN** a runner whose overall dispatch counter is still advancing
- **WHEN** one entity's activation has been outstanding far longer than any other
- **THEN** the heartbeat SHALL report a non-zero outstanding count and an oldest-age reflecting that activation

#### Scenario: An idle runner reports zero
- **WHEN** a runner has no outstanding activations
- **THEN** the heartbeat SHALL report an outstanding count of zero and an oldest-age of zero

#### Scenario: The counters change no behaviour
- **WHEN** the oldest-age counter grows without bound
- **THEN** the SDK SHALL take no action of its own — no reclaim, no re-hand, no pool change

### Requirement: A DropNothing push drains the consumer inline only from a pool thread

When a `DropNothing` push finds its consumer's mailbox full, the core SHALL
serve that consumer inline on the pushing thread only while the thread is
marked as a serving pool thread — one running an activation, or one promoting
a due deferred wake from inside `poll` — the only producers that may be the
lone thread able to run the consumer's round (parking there self-deadlocks a
fully blocked pool). The host SHALL run every activation, including one it
serves inline, inside the core's activation scope so the rule can tell the
two apart. Any other producer — the transport reader feeding every operative
on the runner, a host thread outside that scope — SHALL park, bounded and
woken per pop, while the pool drains the consumer; a reader SHALL never run
one consumer's round while every other operative on its connection waits on
it.

#### Scenario: A pool worker blocked on a full consumer serves it inline

- **WHEN** an activation's emit pushes under `DropNothing` into a consumer
  whose mailbox is full and whose round no worker is serving
- **THEN** the pushing worker assembles and runs the consumer's round itself,
  then completes its push — the one-worker pool makes progress

#### Scenario: A due firing into a saturated consumer drains it

- **WHEN** a processing-time timer comes due for an entity whose mailbox is
  full and whose round no worker is serving
- **THEN** the poller promoting it runs the entity's round inline and the
  firing lands, instead of the poller parking on a mailbox nobody else drains

#### Scenario: A transport reader parks instead of serving

- **WHEN** the runner's stream reader pushes an entry-channel frame under
  `DropNothing` into a full mailbox
- **THEN** the reader parks until a pop frees a slot and a pool worker drains
  the consumer; the reader runs no procedure code

