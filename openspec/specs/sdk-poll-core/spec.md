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

For a **stream** activation, consumer-edge transform chains and `key_by` SHALL be
applied by the host when completing the activation that produced the value;
outputs SHALL carry `(consumer_edge_id, bytes, key?)` and the core SHALL route on
`(edge, key)` without evaluating host closures. Unkeyed entry inputs into a keyed
edge SHALL take a batched `KeyRequest` activation before routing. A transform or
key failure SHALL be reported in the completion and SHALL enter the existing
retry→park ladder.

For a **workflow stage** activation the shape is different, because the core does
not route a workflow stage's output on `(edge, key)`: the driver holds the
released batch, hands it to every child of the node, and journals it per
producing channel. A host completing a workflow stage SHALL therefore emit
**exactly one edge output per released value**, naming the **producing channel**,
carrying the value **untransformed and unkeyed** — one emit SHALL NOT become one
output per consumer, whatever the channel's consumer count. Each consumer edge's
transform chain and key SHALL be evaluated **at the consumer**, when the host
receives the stage input carrying that value, so that a live run and a journal
re-seed apply the edge at the same seam exactly once. The core SHALL NOT consult
an edge output's consumer field on this path.

#### Scenario: Replay re-keys identically
- **WHEN** a journaled slice is re-forwarded by the controller through a keyed edge
- **THEN** the host applies the same transform chain and key function at the same seam, and each value SHALL route to the same entity as the live run

#### Scenario: Entry input into a keyed edge
- **WHEN** an entry-channel input arrives for a consumer reached only via a keyed edge
- **THEN** the core SHALL obtain the key via a `KeyRequest` activation before enqueueing to any entity mailbox, and no keyless drop path SHALL exist

#### Scenario: A workflow stage emits once per released value
- **WHEN** a host workflow procedure emits one value on a producing channel that declares two consumers
- **THEN** the completion SHALL carry exactly one edge output for that value, and the stage's released batch SHALL hold one value — not one per consumer — so the journaled handoff, every downstream node's input batch and every downstream plugin output SHALL be identical to the single-consumer case

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

### Requirement: A due wake that cannot be delivered stays on the wake list

When servicing the deferred-wake list, the core SHALL NOT hold a thread waiting for room in the target's mailbox. If the wake's item cannot be admitted, the entry SHALL be re-recorded at a later due time and the servicing pass SHALL continue; the wake list itself is the holdback. No poller SHALL park inside `poll` waiting on a mailbox, because a poller inside `poll` is counted by `pollers_inside` and therefore reads to the starvation wait as a thread about to take work — so a parked poller both stops the runner and hides that it has.

A re-recorded wake SHALL be re-recorded at a strictly later due time than the pass that observed it, so servicing does not spin, and the existing nearest-due wait shortening SHALL govern the retry cadence.

A timer firing that is deferred this way SHALL NOT be dropped, and the ordering between a deferred firing and a later input for the same entity SHALL be no weaker than it is when both race for mailbox room today.

#### Scenario: A due timer into a full mailbox does not wedge a single-poller host

- **WHEN** a host runs exactly one poll thread, installs no inline-serve hook, and a processing-time timer comes due for an entity whose mailbox is full
- **THEN** the poller SHALL return from the pass without parking, the entity's queued round SHALL be served, and the firing SHALL land on a later pass

#### Scenario: The undeliverable wake is visible as starvation

- **WHEN** a due wake cannot be admitted and no thread is inside `poll`
- **THEN** the starvation wait SHALL report starvation with no dwell, because the still-due entry proves no pass has serviced it

### Requirement: A poll host SHALL bring a starvation supervisor

Every host that drives `poll` SHALL also park a thread in `wait_starvation` and SHALL start one additional worker on `Starvation::Observed`, up to a declared ceiling. A host that ships a fixed worker count with no supervisor SHALL be treated as a defect of that SDK, not as a configuration choice: the supervisor is what buys a thread against a consumer that nobody is draining, and without it a host whose workers are all held wedges with no recovery and no report.

The supervisor SHALL NOT be claimed to cover the conformance `pool_wedge` condition, which is user code that never returns and which no ceiling can outrun.

#### Scenario: Java grows its fetcher pool under starvation

- **WHEN** work is queued, no Java fetcher is inside `poll`, and the dwell elapses
- **THEN** the Java host SHALL start one more fetcher, up to its configured ceiling, and SHALL log the growth

### Requirement: No core call blocks a host thread

A call into the core SHALL NOT park the calling host thread waiting for a
condition only host code can satisfy. The core cannot know how many threads a
host has, whether it can create more, or whether its "threads" pin a shared
carrier — so a park that is safe on one host is a permanent stop on another. A
single-threaded host is a supported shape, and it is supported by the core never
blocking, not by the host promising to bring more threads.

Backpressure SHALL therefore be expressed by **withholding work from the
producer**, never by holding the producer's caller.

The one park that remains is the transport reader's on the entry path. It is
permitted because the reader is a core-owned thread that no host language
observes, it has no producing entity to withhold work from, and its stalling is
what carries ingress backpressure to the controller.

#### Scenario: A single-threaded host completes an activation that cannot deliver

- **WHEN** a host with exactly one thread completes an activation whose
  inter-procedure output cannot be admitted to a full consumer mailbox
- **THEN** the completion call SHALL return without waiting, and that thread
  SHALL be free to poll again

### Requirement: An undeliverable inter-procedure output defers and gates its producer

An inter-procedure output that cannot be admitted SHALL be appended to the
producing entity's push holdback, and that entity's handout gates SHALL stay
held until the holdback drains. The producing
thread SHALL return immediately.

While a producing entity's holdback is non-empty, no durable record of that
entity SHALL be shipped. An inter-procedure output has no durable copy, so the
only thing that can re-produce it is the producer re-running its input: a
consumed offset that became durable ahead of the delivery it derives from would
leave the replay floor skipping that input with the consumer never fed.

The holdback SHALL be re-attempted when the consumer frees room, driven by the
same wait list the entry path's park uses and by the same release-and-rearm path
a retry backoff uses.

Ordering SHALL be preserved by two rules, which together reproduce exactly what
a parked producing thread achieved:

- An entity with a non-empty push holdback SHALL NOT be handed a new activation.
- A push made while that entity's holdback is non-empty SHALL be appended to it
  rather than attempted. This is the rule that stops a batch's later items
  overtaking a deferred earlier one.

The second rule SHALL be evaluated through a lock-free flag, never by inspecting the
holdback under a lock: the check runs on every push including the overwhelming
majority that succeed, where an atomic load is free and a mutex acquisition is
not.

The holdback SHALL be bounded. A single invocation that emits past the bound
SHALL wait for its own earlier pushes to land — parking its calling thread as a
last resort, and never admitting past what it already holds, so the bound cannot
reorder the sequence it exists to bound — a case only the in-process typed
host can reach, because an FFI host's outputs are already materialized before the
core sees them, and the typed host is the one with a supervised worker pool
behind it. Reaching the bound SHALL be reported.

#### Scenario: Per-producer emit order survives a deferral

- **WHEN** one activation emits `e1` into a full consumer and then emits `e2` on
  the same channel
- **THEN** the consumer SHALL receive `e1` before `e2`, and the producing entity
  SHALL run no further activation until both have been admitted

#### Scenario: A batch's later items do not overtake a deferred earlier one

- **WHEN** a host reports effects for items 1..N in one completion and item 1's
  edge output cannot be admitted
- **THEN** items 2..N's outputs for that consumer SHALL be appended behind item
  1's and delivered in that order

#### Scenario: A gated producer is not evicted

- **WHEN** a dynamic-mode entity's idle TTL elapses while its push holdback is
  non-empty
- **THEN** the reaper SHALL skip it and SHALL NOT delete its state

#### Scenario: A gated producer is reported, and is not diagnosed as a wedge

- **WHEN** an entity has held an undelivered push for longer than the reporting
  threshold
- **THEN** the data-channel heartbeat SHALL carry a pending-push count and the
  age of the oldest, and the controller's classifier SHALL report the condition
  without minting a heal op — the entity named by the count is the one WAITING,
  and the remedy for a producer whose consumer is behind is never to rebind the
  producer

#### Scenario: The consumer that is behind is named where an identity can be carried

- **WHEN** a producing entity has held pushes against one consumer past the
  holdback bound
- **THEN** the runner SHALL log one throttled line naming BOTH the producing
  entity and the consumer that is behind, because the heartbeat's cross-SDK
  contract carries counters and not identities

