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
- **THEN** the core SHALL function correctly in both configurations, and distinct cells' activations MAY be processed concurrently when multiple workers poll

#### Scenario: No host callback registration exists
- **WHEN** the core's public API surface is enumerated
- **THEN** it SHALL contain no function that accepts a host function pointer or closure for later invocation on a core-owned thread (the log callback on the FFI layer excepted)

### Requirement: Per-cell exclusivity by handout

The core SHALL NOT hand out two activations for the same `(relay_instance,
tasklet, cell_key)` concurrently: a cell with an outstanding (polled,
uncompleted) activation SHALL NOT appear in any other poll result until its completion
arrives. This restates the existing single-active-drainer invariant at the boundary.

Because the handout is what gates the cell, every polled activation SHALL be
answered exactly once, by one of three calls: **complete**, **fail** (the
retry-ladder path), or **abandon** — a new call that re-hands the activation's
untouched items and releases the cell. A host SHALL make the answer
structural, so that an unwind, an exception, or an early return cannot skip it;
discipline at each return site is not sufficient. Abandon SHALL re-hand only
items the host has not processed, SHALL NOT count against the retry ladder, and
SHALL leave the cell exactly as if it had never been polled.

The core SHALL NOT reclaim an outstanding activation on a timer or deadline. A
long-running activation and a lost handout are indistinguishable at the
boundary, and re-handing on a deadline would re-execute work the
effectively-once tier promises once. Recovery from a handout a host lost despite
the above is the controller's relay rebind, which resumes from durable
state.

#### Scenario: Same cell never outstanding twice
- **WHEN** two host workers poll concurrently while 100 inputs for one cell and 100 for another are queued
- **THEN** at no point SHALL both workers hold activations for the same cell, and each cell's inputs SHALL be delivered in enqueue order across its successive activations

#### Scenario: Completion releases the cell
- **WHEN** an activation for cell E is completed and E has further queued inputs
- **THEN** a subsequent poll SHALL be able to return E's next activation

#### Scenario: An unwound dispatch abandons rather than retiring the cell
- **WHEN** a host worker holding an activation for cell E unwinds (panic, exception, or early return) without completing it
- **THEN** the activation SHALL be abandoned, E's items SHALL be re-handed untouched, a subsequent poll SHALL be able to return E's next activation, and no retry-ladder attempt SHALL have been consumed

#### Scenario: A slow activation is never reclaimed
- **WHEN** an activation stays outstanding for longer than any configured interval because user code is still running
- **THEN** the core SHALL NOT re-hand it, SHALL NOT hand the cell to another worker, and the activation SHALL complete normally whenever user code returns

### Requirement: Activation is a per-cell batch

An Activation SHALL target exactly one cell and carry a batch of one or more items,
each being an input (payload bytes, metadata, `consumed_offset`, replay flag) or a
timer firing (kind, scheduled timestamp). Each item SHALL carry the core-evaluated
`now` for the declared clock; the host SHALL NOT substitute ambient time for
`ctx.now()`. Timer firings SHALL be validated against the pending set by the core
before inclusion (a stale entry fires nothing).

#### Scenario: Batch amortizes the boundary
- **WHEN** 50 inputs for one cell are queued before any worker polls
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
- **THEN** the core SHALL re-hand the item in a later activation for that cell after the existing backoff ladder, and the retry counter SHALL ride the existing telemetry

### Requirement: Host-participating commands are activation kinds

The four control interactions requiring host participation SHALL be expressed as
activation kinds — `RestoreFold`, `SnapshotRequest`, `LiveStateRead`, `ApplyState` —
subject to the same per-cell exclusivity; a live read and a migration both read the
state as a `LiveStateRead`. All other control
commands SHALL be handled entirely inside the core and SHALL NOT surface to the host.

#### Scenario: Restore is a fold handed to the host
- **WHEN** the controller ships `RestoreInitState` for a cell with a persisted change log
- **THEN** the host SHALL receive a `RestoreFold` activation carrying the records, and input activations for that cell SHALL NOT be handed out before its fold completes

#### Scenario: Snapshot request preserves compaction semantics
- **WHEN** a `SnapshotRequest` activation completes with encoded state bytes
- **THEN** the core SHALL drain pending changes first and stamp the snapshot with the highest already-shipped seq (`change_seq - 1` semantics), unchanged from today

#### Scenario: Ping never reaches the host
- **WHEN** the controller sends `Ping`
- **THEN** the core SHALL answer without any activation being generated

### Requirement: A Cell or a relay leaving the runner is told to the host

The core SHALL hand the host a `Leave` activation when a host-run relay leaves the
runner (stopped, finished or migrated away), with `whole_relay` set and the relay as
its target, once no activation handed out for that relay is still running; and when
it evicts an idle host-run Cell, with the Cell as its target, under the Cell's gate
and before the Cell's key is removed, waiting for the host's answer; once the relay
stops, a Cell's `Leave` no host has taken yet is withdrawn, the relay's own `Leave`
covering the Cell, and a reaper sends no Cell of a stopped relay its own `Leave`. The
host SHALL
drop the state it holds for the target, or for every Cell of the relay, and SHALL
answer `StateApplied`. An `AddRelay` for an id whose relay's leave the host has not
answered SHALL be handled, and answered, only after it has. A host-run Cell whose
records or processed-prefix holds have not all shipped SHALL NOT be evicted until they
have. A typed relay's state lives in the core's Cells and SHALL NOT produce a `Leave`.

#### Scenario: A relay id started again begins at init_state
- **WHEN** a host-run relay is stopped and an instance is started again under the same id
- **THEN** the host SHALL have received a `Leave` with `whole_relay` for that id, and the new instance's Cells SHALL begin at `init_state`

#### Scenario: A start waits for its predecessor's leave
- **WHEN** an `AddRelay` arrives for an id whose stopped relay's leave the host has not answered
- **THEN** the core SHALL hand the host nothing of the new instance, and SHALL answer the `AddRelay`, only after the host answered that leave

#### Scenario: A stop waits out the relay's running work
- **WHEN** a host-run relay is stopped while an activation handed out for it is still running
- **THEN** the `Leave` SHALL be handed out only after that activation is answered, so no write it makes outlives the leave

#### Scenario: A Cell evicted idle begins again at init_state
- **WHEN** the reaper evicts an idle dynamic Cell of a host-run relay and an input for its key arrives later
- **THEN** the host SHALL have answered a `Leave` for that Cell before any Cell minted for the key is handed work, and that Cell SHALL begin at `init_state`

#### Scenario: A stop does not wait out an eviction's leave
- **WHEN** a host-run relay is stopped while a Cell's `Leave` the reaper sent is still queued for the host
- **THEN** the core SHALL withdraw that `Leave` and send no other Cell one, and the stop SHALL NOT wait for a host's answer

#### Scenario: A relay's leave waits for a Cell's leave a host took
- **WHEN** a host-run relay is stopped while a host holds a Cell's `Leave` unanswered
- **THEN** the relay's `Leave` SHALL be handed out only after that Cell's `Leave` is answered

#### Scenario: An eviction waits for the Cell's records to ship
- **WHEN** an idle host-run Cell still keeps a hold on its relay's processed prefix whose record has not shipped
- **THEN** the reaper SHALL skip the Cell, send no `Leave` for it, and keep the hold

### Requirement: Edge transforms and keys evaluate producer-side

For a **stream** activation, consumer-edge transform chains and `key_by` SHALL be
applied by the host when completing the activation that produced the value, and
SHALL NOT be applied again when the consuming host receives that value; outputs
SHALL carry `(consumer_edge_id, bytes, key?)` and the core SHALL route on
`(edge, key)` without evaluating host closures. Unkeyed entry inputs into a keyed
edge SHALL take a batched `KeyRequest` activation before routing. A transform or
key failure SHALL be reported in the completion and SHALL enter the existing
retry→park ladder.

A host receiving an input SHALL apply the consuming edge's transform chain only
when the payload reached it untransformed — an **entry** payload, whose producer
is the wire, or a **steps stage's** released value — and SHALL learn which from the
input item itself: the core SHALL mark every input item it hands a host with its form,
`untransformed`, `transformed` (a producer evaluated the edge) or `prepared` (the prepare
crossing evaluated it and the item carries the resulting values), and no host SHALL
re-derive the form from the channel kind or the run. The prepare crossing SHALL always
apply the chain to the payloads it carries.

For a **steps stage** activation the shape is different, because the core does
not route a steps stage's output on `(edge, key)` at emit time: it holds the released
value on the instance's pass sheet, journals it per producing channel, and on the stage's
commit ack delivers it to every child's cells (a static consumer's declared cells all receive
it, and the host keeps the ones `cells_for` names). A host completing a steps stage SHALL
therefore emit **exactly one edge output per released value**, naming the **producing channel**,
carrying the value **untransformed and unkeyed** — one emit SHALL NOT become one
output per consumer, whatever the channel's consumer count. Each consumer edge's
transform chain and key SHALL be evaluated **at the consumer**, when the host
receives the stage input carrying that value or, for a consumer that needs keys, at the
prepare crossing on the consumer's channel, so that a live run and a journal
re-seed apply the edge at the same seam exactly once. The core SHALL NOT consult
an edge output's consumer field on this path. For a typed class the core runs the edge chain
and `cells_for` on the producing worker before the output is held, and journals the value
before the chain.

#### Scenario: Replay re-keys identically
- **WHEN** a journaled slice is re-forwarded by the controller through a keyed edge
- **THEN** the host applies the same transform chain and key function at the same seam, and each value SHALL route to the same cell as the live run

#### Scenario: Entry input into a keyed edge
- **WHEN** an entry-channel input arrives for a consumer reached only via a keyed edge
- **THEN** the core SHALL obtain the key via a `KeyRequest` activation before enqueueing to any cell mailbox, and no keyless drop path SHALL exist

#### Scenario: A steps stage emits once per released value
- **WHEN** a host steps tasklet emits one value on a producing channel that declares two consumers
- **THEN** the completion SHALL carry exactly one edge output for that value, and the stage's held batch SHALL hold one value — not one per consumer — so the journaled handoff, every downstream node's input batch and every downstream plugin output SHALL be identical to the single-consumer case

#### Scenario: A flow inter-tasklet transform applies exactly once
- **WHEN** a `map` declared on an inter-tasklet edge between two flow tasklets carries a value from the producing activation to the consuming one
- **THEN** the consuming tasklet SHALL receive the value with the map applied exactly once, and no host SHALL compose the chain with itself at delivery

#### Scenario: A typed steps edge applies on the worker, before the hold
- **WHEN** a Rust steps tasklet emits a value on a channel whose consumer edge declares a `map` and whose consumer declares static `cell_keys`
- **THEN** the map and `cells_for` SHALL run on the worker completing that item, the held output SHALL name the consumer cells, and the thread that later releases it on the commit ack SHALL run neither

#### Scenario: The input form comes from the core
- **WHEN** a Python, Ruby, PHP, Node, Java, Go or Elixir host receives an entry payload, a steps stage's released value and a stream edge's value in one activation
- **THEN** the items carry the forms `untransformed`, `untransformed` and `transformed`, and the host applies the consuming edge's chain to the first two only

### Requirement: Host classes reach full capability

The host layer (`flz_core::host`) SHALL support, for byte-level host
classes, everything the typed Rust host can declare: single, static, and
dynamic cell modes (dynamic with idle-TTL eviction and the steps relay
evicted-keys trace), the effectively-once tier and checkpoint
ack-granularity (completions carrying `consumed_offset` / `checkpoint`
drive the same effect-record and interval semantics), late-input
correction (window declaration, admission classification, slice replay),
per-cell timers of both kinds, and inter-tasklet edges whose values
cross as bytes routed on `(edge, key)`. Class declarations SHALL carry
these facts and the core SHALL populate `AddRelay` response metadata
from them exactly as the typed path does.

#### Scenario: A dynamic host class routes by key and evicts idle cells
- **WHEN** a host class declares a dynamic tasklet with an idle TTL and inputs for keys `a` and `b` arrive
- **THEN** the core SHALL create the cells lazily via the prepare crossing's keys, hand out per-cell activations exclusively, and after the TTL SHALL evict destructively, recording the key in the eviction trace

#### Scenario: An effectively-once host class holds its watermark to checkpoints
- **WHEN** a checkpoint-ack host class completes three inputs without `checkpoint` and a fourth with it
- **THEN** the shipped effect record SHALL carry the accumulated changes with the fourth input's offset, and a crash before it SHALL replay all four inputs

#### Scenario: Inter-tasklet values cross as bytes
- **WHEN** a host tasklet's completion carries an output targeting a consumer edge with a key
- **THEN** the core SHALL enqueue the bytes to the consumer cell selected by `(edge, key)` and the consumer's next activation SHALL carry them as an input item, with no host closure evaluated in routing

### Requirement: The prepare crossing serves keys and event times

The core SHALL batch, for host classes whose edges are keyed or whose
clock is an event time, the questions it cannot answer from bytes into one
prepare crossing (the widened `KeyRequest`): per payload the host SHALL
return the extracted event time and, when keys are wanted, the payload's values
per consumer edge that needs keys, each value as the bytes of the edge's transformed
value with the keys it routes to, and the core SHALL resolve the crossing before any
mailbox enqueue. The host SHALL decode the payload once and run each such edge's chain
once per crossing; the core SHALL enqueue one item per receiving Cell carrying that Cell's values in order,
marked `prepared`; the item SHALL ship no payload bytes to the host, and the receiving Cells
SHALL share one copy of the original payload, what a park records; and the host SHALL run the handler once per value without decoding
the entry again, applying the edge again or re-checking the value's key. Which consumers
need keys SHALL be computed once per channel when the topology is built, not per
payload. The question SHALL be asked
wherever a keyless payload meets a consumer that needs keys, on an entry
channel as today and on an inter-tasklet channel of a steps instance,
whose host releases a value untransformed and unkeyed for the consumer to
apply the edge on delivery: the core SHALL stage such a value for the
crossing on its channel, live or re-seeded from the journal, and SHALL
address it to the keyed consumer's Cells from the answer, minting them as
a flow relay's are minted. A payload that already carries its keys SHALL
cross nothing. A staged value SHALL count as an item the RECEIVING stage
still owes, so that stage cannot settle ahead of it; the live release and
the journal re-seed are then one mechanism, because both arrive at the
receiving stage.
Admission, classification, the high-water, and due-timer generation remain
core-side; a prepare failure enters the input channel's retry→park ladder,
and where that ladder would park a steps instance's value it SHALL
fail the receiving stage instead, and nothing SHALL be parked, as the two
other park sites do for a steps relay.

#### Scenario: Event time arrives before admission
- **WHEN** an input for an event-time-clocked host class is received
- **THEN** the core SHALL obtain its event time via the prepare crossing and classify it against the high-water BEFORE enqueueing, and a Correctable verdict SHALL drop-and-request exactly as for the typed host

#### Scenario: A key failure parks instead of dropping keylessly
- **WHEN** the host answers a prepare batch with a failure for one payload
- **THEN** that payload SHALL climb the channel's retry→park ladder and no keyless-drop path SHALL exist

#### Scenario: A host steps relay's release is keyed by the crossing
- **WHEN** a Java, Node, Python, Ruby, PHP or Go steps relay's first tasklet releases three values into a tasklet reached by `keyBy`
- **THEN** the core stages the three payloads for one crossing on the inter-tasklet channel, the host answers each value's key by running the edge's chain, three Cells are minted from the answers, and the keyed stage settles only after the three are addressed and have run

#### Scenario: A resumed host steps relay re-seeds its keyed consumer the same way
- **WHEN** a host steps relay whose keyed tasklet is the first uncommitted stage is resumed from its journal
- **THEN** the journaled handoff crosses for its keys as the live release did, and the pass resumes on the same Cells the crash left behind

#### Scenario: A crossing that fails terminally fails the stage
- **WHEN** a host answers every re-attempt for one released value with a non-retryable error
- **THEN** the receiving stage of the steps instance fails without committing, compensation starts from there, and nothing is parked to the dead-letter queue

#### Scenario: A keyed entry payload is decoded and transformed once
- **WHEN** a hosted SDK's entry payload fans through a keyed edge's `flat_map` into three values for keys `a`, `a` and `b`
- **THEN** the host decodes the payload and runs the chain once, at the prepare crossing; Cell `a` receives one item carrying two prepared values and Cell `b` one item carrying one; the handler runs three times; and no work activation decodes the entry payload

#### Scenario: The Cells of one payload share one copy of it
- **WHEN** a keyed entry payload's answer names three Cells of one consumer
- **THEN** each Cell's item ships no payload bytes to its host, the three share one copy of the payload, and a park of any of them records the whole payload

#### Scenario: Two keyed consumers each route by their own edge
- **WHEN** one entry channel feeds two dynamic consumers whose edges key the same payload differently
- **THEN** each consumer's Cells are named by its own edge's keys and receive its own edge's values

### Requirement: Steps relay lifecycles run under poll

Steps classes of BOTH language paths SHALL run their stages through the poll seam as
ordinary cell work: a stage's items are admitted to its cells' mailboxes through the stream's
routing (`deliver_to_consumer`: single, static and dynamic delivery, the static form's host
branch included), handed out as `Work` activations under per-cell exclusivity, fairness and the
in-flight cap, and retried on the core's per-item ladder. A cell's inter-tasklet output SHALL
be held on the instance's pass sheet at the routing seam, post-transform and addressed to its
consumer cells, and SHALL be released into those mailboxes only on the producer stage's commit
ack. A cell's plugin output SHALL NOT be held on the sheet: it is settled into the cell's effect
record with the item's state changes, through the one settle path a flow cell uses, and the
record is framed and sent on the worker that settles the item, before the stage's barrier; the
barrier names each cell's last record of the stage by seq. A stage SHALL be done when every item
admitted for it has settled, counted, and SHALL
commit then, whether or not a sibling stage is still running. No core-owned thread SHALL
execute user code, no core thread SHALL block awaiting a host completion, and no thread SHALL
exist per instance. Saga compensation crosses as the `CompensateRun` activation, carrying the
recorded outputs, for both language paths, with its completion an event on the sheet and its
undo output settled into the compensating cell's own record; the
task-journal, durable loop, suspension and rewind semantics are unchanged. Every
steps class declares tasklets, so every steps relay host class runs through work activations; a
completion MAY carry a tasklet's completion request, which the sheet folds at the iteration
seal.

#### Scenario: A host step completion ends its steps relay

- **WHEN** a steps relay host class's tasklet answers its activation with a completion request and a result
- **THEN** the sheet SHALL seal the iteration on the pass's last ack and drive the same `RelayCompleted` teardown the typed path produces, carrying that result

#### Scenario: A stage commits on its last item and releases on the ack

- **WHEN** a durable structured steps class's stage has every admitted item settled
- **THEN** the worker that settled the last one SHALL send `TaskCommit` for that stage naming each cell's record, the stage's children SHALL receive their input only once the controller acks it, and a crash-resume SHALL skip journaled tasks exactly as the driver suites pin

#### Scenario: A cell's record leaves on the worker, ahead of the barrier

- **WHEN** a durable steps cell's item settles with a change and a plugin output
- **THEN** the worker that settled it frames and sends the cell's effect record before any barrier for that stage is sent, and the barrier carries that record's seq for the cell

#### Scenario: A steps relay step costs no thread beyond the one running it

- **WHEN** a Java or a Rust steps class runs a stage
- **THEN** the only thread occupied by that stage SHALL be the SDK thread running the code; no
  core thread SHALL wait for its reply, and its answer SHALL advance the sheet on the thread
  that completes it

#### Scenario: Stage work takes its fair turn under the cap

- **WHEN** a steps stage's items are admitted while flow cells on the same runner have work
  queued and the in-flight cap is reached
- **THEN** the stage's cells SHALL be handed out in the ready queue's order like any cell, and
  SHALL NOT be served ahead of queued cell work or outside the cap

#### Scenario: A host steps relay's static cells receive their items

- **WHEN** a Java, Node, Python, Ruby, PHP or Go steps tasklet declares static `cell_keys` and a
  stage releases a value to it
- **THEN** every declared cell SHALL be offered the value and each host cell SHALL run the ones
  `cells_for` names, exactly as a flow tasklet of that host with the same keys does

#### Scenario: A sibling failure compensates a committed sibling

- **WHEN** in an A→(B,C) tree B commits and C then fails after exhausting its ladder
- **THEN** B's commit SHALL stand until compensation, and compensation SHALL undo C's cells that
  ran, then B, then A, in reverse completion order

### Requirement: Deferred wakes never hold a thread

The core SHALL maintain one deferred-wake list — "this cell becomes eligible
at time T", or "this instance's deadline is due at T", serviced by threads already
waiting in `poll` and checked on every `poll` entry. Recording a deferred wake SHALL return the
calling thread immediately. A retryable failure's backoff SHALL be expressed as a deferred
wake: the failed head and unprocessed tail ride the existing per-cell
holdback, the cell's exclusivity gates stay held across the window exactly as
they are across an in-line backoff, and the release-then-reclaim runs when the
entry comes due. An instance's deadlines SHALL be entries on the same list, keyed by instance,
deadline kind and due time: the idle deadline (the class's `sleep_after`) of any
instance whose class sleeps, and a steps relay's `max_duration`, commit-ack
re-send and restore-gate cap. Re-arming an instance deadline SHALL go through one re-arm call that
removes the entry it replaces; an entry its owner has since re-decided SHALL be a no-op at
promotion, judged by the owner's own record of its current due time. A processing-time timer's
entry SHALL be due on the wall clock, its scheduled time being an absolute wall time; every other
entry (a retry, a push re-arm, an interval close, an instance deadline, a sweep) SHALL be due on
the monotonic clock, so a step of the wall clock neither delays nor advances it. The idle-TTL
sweep of a relay's dynamic Cells SHALL be an entry on this list, re-armed after each sweep, and
no thread SHALL exist per relay to run it. A promoted deadline SHALL decide over what the instance holds
(the pass sheet under its lock for a steps relay, the cells' mailboxes, holdbacks, buffers and
timers for either kind) and act without waiting; a deadline that no longer applies SHALL be a
no-op, and an idle deadline that finds the instance busy or its cells active more recently than
the window SHALL re-arm rather than sleep. The idle deadline SHALL be armed once per bind (and,
for a steps relay, when its sheet enters `Waiting`) and never per admitted input. No thread SHALL
sleep out a backoff delay or an instance deadline, and no dedicated thread SHALL exist to
service the list.

#### Scenario: Correlated failure does not stall the runner

- **WHEN** every cell's activation is failing retryably (a shared dependency
  is down) on a runner whose worker count equals the CPU count
- **THEN** workers SHALL remain available to poll and run activations for
  cells whose work would succeed, and each failing cell SHALL be re-handed
  no earlier than its backoff due time

#### Scenario: Backoff preserves ordering and the holdback contract

- **WHEN** a retryable failure defers a cell with a failed head and an
  unprocessed tail
- **THEN** the next activation for that cell SHALL begin with the held-back
  items in their original order, ahead of anything that arrived during the
  backoff window, and no other thread SHALL drain that cell before the due
  time

#### Scenario: Shutdown is not delayed by backoff

- **WHEN** shutdown begins while deferred wakes are pending
- **THEN** pollers SHALL observe shutdown immediately, with no backoff sleep
  left to run out

#### Scenario: A steps deadline that no longer applies does nothing

- **WHEN** a `PerInput` instance's idle deadline comes due after a message already started
  its next pass, or a commit-ack deadline comes due after the ack arrived
- **THEN** the promoting poller SHALL find the deadline stale under the instance's lock and
  SHALL neither park the instance nor re-send the barrier

#### Scenario: A due park-idle parks without a thread

- **WHEN** a journaling `PerInput` instance has waited past its class's `sleep_after` at a
  committed boundary with nothing queued and no input tail owed
- **THEN** the poller promoting the deadline SHALL send `IterPark` and mark the instance parked,
  and no thread SHALL have waited out the idle window

#### Scenario: A due idle deadline parks a flow relay without a thread

- **WHEN** a flow instance whose class sleeps has had no cell activity for its class's `sleep_after`, every cell's mailbox, holdback and flush buffer is empty, no retry or timer wake names one of its cells, and no input tail is owed
- **THEN** the poller promoting the deadline SHALL send `IterPark` with iteration `0` and remove the instance, and no thread SHALL have waited out the idle window

#### Scenario: Recent activity re-arms instead of sleeping

- **WHEN** an idle deadline comes due and a cell of the instance was active more recently than the window
- **THEN** the poller SHALL re-arm the deadline for the remainder of the window from that activity and SHALL NOT sleep the instance

#### Scenario: Admitting an input does not touch the wake list

- **WHEN** ten thousand inputs are admitted into a class that sleeps
- **THEN** no deferred-wake entry SHALL be recorded or replaced on their account; only the cells' activity stamps move

#### Scenario: Re-arming a deadline leaves one entry

- **WHEN** a flow relay's completion re-checks its Cells five times before they are idle, re-arming its `Complete` deadline each time
- **THEN** the list SHALL hold one `Complete` entry for that instance after each re-arm, and a disarm naming the deadline and its due time SHALL leave none

#### Scenario: A wall-clock step does not move a retry

- **WHEN** a Cell's retry is armed 500 ms out and the wall clock is then set back one hour
- **THEN** the retry SHALL come due about 500 ms after it was armed, while a processing-time timer scheduled for a wall time SHALL come due when the wall clock reaches it

#### Scenario: The idle-TTL sweep needs no thread

- **WHEN** a relay whose tasklet declares an idle TTL is bound
- **THEN** no `flezha-reaper` thread SHALL exist, and a dynamic Cell idle past the TTL SHALL be evicted when the sweep entry is promoted: on the promoting poller, or, for a host-run Cell whose leave waits for the host, on a short-lived `flezha-leave` thread

### Requirement: The core caps jobs underway and signals starvation

The core SHALL own one process-wide limit on concurrently outstanding work
activations (default 1024, env `FLEZHA_MAX_INFLIGHT_ACTIVATIONS`), enforced at
work handout: when the limit is reached, `poll` SHALL NOT hand out cell work,
and a completion that drops the count below the limit SHALL resume handout.
The limit SHALL NOT gate direct command activations (restore folds, snapshot
requests, live state reads, the steps relay undo's `CompensateRun`, prepare crossings: these
answer a rendezvous or an event, never a queued work item) nor the inline-serve
deadlock-avoidance path. A steps stage's cell work is cell work and SHALL be counted and
capped like any other.

The core SHALL expose starvation — runnable work exists, no thread is inside
`poll`, and the limit is not reached — as a blocking wait a host thread may
park in, answering in the same three-way shape as `poll` (observed / timeout /
shutting down). No host-callback registration SHALL be introduced.

Starvation SHALL mean stuck, not busy: queued cell work SHALL count only
once it has waited a dwell interval during which no thread entered `poll` at
all, so that a saturated runner — which re-enters `poll` once per batch —
never reads as starved. A due deferred wake SHALL count immediately, since
every `poll` pass promotes what has come due.

#### Scenario: Handout stops at the cap and resumes on completion

- **WHEN** the outstanding work count reaches the configured limit while more
  cell work is queued
- **THEN** polling threads SHALL block as if no work were ready, and a single
  completion SHALL allow the next poll to return an activation

#### Scenario: The cap does not gate command activations

- **WHEN** the outstanding work count is at the limit and a direct command
  activation (e.g., a restore fold) is enqueued
- **THEN** the next poll SHALL return the command activation ahead of any
  cell work

#### Scenario: Starvation wakes a parked supervisor

- **WHEN** cell work is queued while every poller is inside user code (none
  waiting in `poll`) and the limit is not reached
- **THEN** a host thread parked in the starvation wait SHALL be woken

#### Scenario: A steps stage is counted against the cap

- **WHEN** the outstanding work count is at the limit and a steps stage's items are admitted
- **THEN** the stage's cells SHALL wait in the ready queue like any cell, and SHALL be handed
  out only as completions bring the count below the limit

### Requirement: Handout liveness is observable without a new frame

The core SHALL expose two counters over its existing self-report: the number of
activations currently outstanding, and the age in milliseconds of the oldest
outstanding activation (zero when none). Every SDK SHALL carry them in the
data-channel heartbeat's metadata map beside the counters it already reports
(`dispatches`, `blocked_parks`, `credit_parked`, the timer counters), so the
controller reads them through the liveness snapshot it already builds — no new
frame, no new payload id, and no new sensor process.

The heartbeat SHALL also carry `handouts`, the number of activations the core
has issued (its latest activation token), which moves when work is handed out,
a retry's re-handout included, and does not wait for the work to settle.

The counters are reporting only: nothing in the SDK SHALL change behaviour based
on them.

#### Scenario: A stalled cell is visible while the runner looks healthy
- **GIVEN** a runner whose overall dispatch counter is still advancing
- **WHEN** one cell's activation has been outstanding far longer than any other
- **THEN** the heartbeat SHALL report a non-zero outstanding count and an oldest-age reflecting that activation

#### Scenario: A handout moves the handout count before it settles
- **WHEN** a cell's work is handed out, fails and waits out its retry backoff
- **THEN** the heartbeat's `handouts` SHALL have moved on the handout, and SHALL move again when the retry is re-handed, while no activation is outstanding in between

#### Scenario: An idle runner reports zero
- **WHEN** a runner has no outstanding activations
- **THEN** the heartbeat SHALL report an outstanding count of zero and an oldest-age of zero

#### Scenario: The counters change no behaviour
- **WHEN** the oldest-age counter grows without bound
- **THEN** the SDK SHALL take no action of its own — no reclaim, no re-hand, no pool change

### Requirement: A due wake that cannot be delivered stays on the wake list

When servicing the deferred-wake list, the core SHALL NOT hold a thread waiting for room in the target's mailbox. If the wake's item cannot be admitted, the entry SHALL be re-recorded at a later due time and the servicing pass SHALL continue; the wake list itself is the holdback. No poller SHALL park inside `poll` waiting on a mailbox, because a poller inside `poll` is counted by `pollers_inside` and therefore reads to the starvation wait as a thread about to take work — so a parked poller both stops the runner and hides that it has.

A re-recorded wake SHALL be re-recorded at a strictly later due time than the pass that observed it, so servicing does not spin, and the existing nearest-due wait shortening SHALL govern the retry cadence.

A timer firing that is deferred this way SHALL NOT be dropped, and the ordering between a deferred firing and a later input for the same cell SHALL be no weaker than it is when both race for mailbox room today.

#### Scenario: A due timer into a full mailbox does not wedge a single-poller host

- **WHEN** a host runs exactly one poll thread, installs no inline-serve hook, and a processing-time timer comes due for a cell whose mailbox is full
- **THEN** the poller SHALL return from the pass without parking, the cell's queued round SHALL be served, and the firing SHALL land on a later pass

#### Scenario: The undeliverable wake is visible as starvation

- **WHEN** a due wake cannot be admitted and no thread is inside `poll`
- **THEN** the starvation wait SHALL report starvation with no dwell, because the still-due entry proves no pass has serviced it

### Requirement: A poll host SHALL bring a starvation supervisor

Every host that drives `poll` SHALL also bring the core governor's supervisor (`flz_core::poll_pool`) and SHALL start one additional worker on each spawn decision it returns, up to a declared ceiling. The pool policy SHALL be the core's, the same in every SDK: the base and the ceiling read from `FLEZHA_WORKER_POOL_SIZE` and `FLEZHA_WORKER_POOL_MAX` by the core over the runtime's own base default (Java's `DEFAULT_FETCHERS`, 2, is that default, not a pool size of its own); a supervisor wait of 1 s, inside which the core waits on starvation itself; a poll timeout of 250 ms; a worker above the base retired after 30 s without work; a pool below its base topped up on the next supervisor wake; a drain bound of 5 s. A host SHALL implement only how a worker is spawned and what it runs: the Rust SDK, the Node host's fetchers and the Elixir host through the governor's spawner, the Python, Ruby, Java, PHP and Go hosts by driving `flz_core_pool_supervise` from a thread of their own (a goroutine in Go) and answering the decisions it returns (spawn a grown worker, spawn a base worker, idle, stop), at least one per supervisor wait. Node's completion pool SHALL take its base, ceiling and retire time from the governor and SHALL keep its own growth on a queued completion. No SDK SHALL carry its own pool policy constant (base, ceiling, retire time or supervisor wait); a synchronous executor's poll turn, which runs no pool (PHP's `Runner::POLL_TIMEOUT_MS`), and a host's shutdown budget (the Elixir runner's drain inside OTP's shutdown) stay the host's. A host that ships a fixed worker count with no supervisor SHALL be treated as a defect of that SDK, not as a configuration choice: the supervisor is what buys a thread against a consumer that nobody is draining, and without it a host whose workers are all held wedges with no recovery and no report.

The supervisor SHALL NOT be claimed to cover the conformance `pool_wedge` condition, which is user code that never returns and which no ceiling can outrun.

#### Scenario: Java grows its fetcher pool under starvation

- **WHEN** work is queued, no Java fetcher is inside `poll`, and the dwell elapses
- **THEN** the Java host SHALL start one more fetcher on the governor's spawn answer, up to its configured ceiling

#### Scenario: A grown worker retires after the same idle time in every SDK
- **WHEN** a Python, Ruby, Java, PHP (threaded executor), Node, Go, Elixir or Rust runner has grown its pool under starvation and the work then stops
- **THEN** each grown worker SHALL leave after 30 s without work, the base workers SHALL stay, and no SDK source SHALL hold a retire time or a supervisor wait of its own

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
observes, it has no producing cell to withhold work from, and its stalling is
what carries ingress backpressure to the controller.

#### Scenario: A single-threaded host completes an activation that cannot deliver

- **WHEN** a host with exactly one thread completes an activation whose
  inter-tasklet output cannot be admitted to a full consumer mailbox
- **THEN** the completion call SHALL return without waiting, and that thread
  SHALL be free to poll again

### Requirement: An undeliverable inter-tasklet output defers and gates its producer

An inter-tasklet output that cannot be admitted SHALL be appended to the
producing cell's push holdback, and that cell's handout gates SHALL stay
held until the holdback drains. The producing
thread SHALL return immediately.

While a producing cell's holdback is non-empty, no durable record of that
cell SHALL be shipped. An inter-tasklet output has no durable copy, so the
only thing that can re-produce it is the producer re-running its input: a
consumed offset that became durable ahead of the delivery it derives from would
leave the replay floor skipping that input with the consumer never fed.

The holdback SHALL be re-attempted when the consumer frees room, driven by the
same wait list the entry path's park uses and by the same release-and-rearm path
a retry backoff uses.

Ordering SHALL be preserved by two rules, which together reproduce exactly what
a parked producing thread achieved:

- A cell with a non-empty push holdback SHALL NOT be handed a new activation.
- A push made while that cell's holdback is non-empty SHALL be appended to it
  rather than attempted. This is the rule that stops a batch's later items
  overtaking a deferred earlier one.

The second rule SHALL be evaluated through a lock-free flag, never by inspecting the
holdback under a lock: the check runs on every push including the overwhelming
majority that succeed, where an atomic load is free and a mutex acquisition is
not.

The holdback SHALL be steps. A single invocation that emits past the bound
SHALL wait for its own earlier pushes to land — parking its calling thread as a
last resort, and never admitting past what it already holds, so the bound cannot
reorder the sequence it exists to bound — a case only the in-process typed
host can reach, because an FFI host's outputs are already materialized before the
core sees them, and the typed host is the one with a supervised worker pool
behind it. Reaching the bound SHALL be reported.

#### Scenario: Per-producer emit order survives a deferral

- **WHEN** one activation emits `e1` into a full consumer and then emits `e2` on
  the same channel
- **THEN** the consumer SHALL receive `e1` before `e2`, and the producing cell
  SHALL run no further activation until both have been admitted

#### Scenario: A batch's later items do not overtake a deferred earlier one

- **WHEN** a host reports effects for items 1..N in one completion and item 1's
  edge output cannot be admitted
- **THEN** items 2..N's outputs for that consumer SHALL be appended behind item
  1's and delivered in that order

#### Scenario: A gated producer is not evicted

- **WHEN** a dynamic-mode cell's idle TTL elapses while its push holdback is
  non-empty
- **THEN** the reaper SHALL skip it and SHALL NOT delete its state

#### Scenario: A gated producer is reported, and is not diagnosed as a wedge

- **WHEN** a cell has held an undelivered push for longer than the reporting
  threshold
- **THEN** the data-channel heartbeat SHALL carry a pending-push count and the
  age of the oldest, and the controller's classifier SHALL report the condition
  without minting a heal op — the cell named by the count is the one WAITING,
  and the remedy for a producer whose consumer is behind is never to rebind the
  producer

#### Scenario: The consumer that is behind is named where an identity can be carried

- **WHEN** a producing cell has held pushes against one consumer past the
  holdback bound
- **THEN** the runner SHALL log one throttled line naming BOTH the producing
  cell and the consumer that is behind, because the heartbeat's cross-SDK
  contract carries counters and not identities

### Requirement: A host encodes an output without allocating a buffer per value

A host that must encode a value into bytes to hand the core an edge output SHALL reuse one
encode buffer per thread rather than allocate a buffer, a flow relay or a reader for each value, and
SHALL NOT copy the encoder's own output a second time before handing it over. The only copy the
host MAY make is the one that transfers ownership of the finished bytes out of a reused buffer,
because a later encode overwrites that buffer.

This governs only how the bytes are produced. Which bytes they are is settled by the existing
edge-output requirement and does not change: the same value encodes to the same bytes before and
after.

Reuse makes the buffer shared mutable state for the duration of one encode. A host SHALL
therefore guarantee that an encode nested inside another encode on the same thread does not
share its buffer, either by construction or by detecting the nesting and refusing it. A host
that cannot make that guarantee SHALL allocate per value instead, and its spec SHALL say so.

A host whose encoder already hands back the finished bytes, with nothing layered on top,
satisfies this requirement as written and needs no buffer of its own. A host that does not
encode on the hop at all, because it owns its core and passes the value itself, is outside this
requirement.

#### Scenario: Two encodes on one thread return independent bytes

- **WHEN** one host thread encodes value A and then value B through the same codec, and both
  returned byte sequences are read after the second encode completes
- **THEN** A's bytes SHALL still be A's bytes, unchanged by the encode of B

#### Scenario: The bytes are the same as before the buffer was reused

- **WHEN** the same value is encoded by a host that reuses its buffer and by one that allocates
  per value
- **THEN** the two byte sequences SHALL be identical, so no journal, replay or cross-SDK
  fixture changes meaning

#### Scenario: A host that layers nothing on its encoder adds no buffer

- **WHEN** a host's encoder returns the finished bytes directly, as MessagePack and JSON
  encoders in the Python and PHP SDKs do
- **THEN** that host SHALL hand those bytes over unchanged and SHALL NOT introduce a buffer, a
  copy or a conversion to satisfy this requirement

### Requirement: A running steps instance holds no thread of its own

A steps instance SHALL own no thread and SHALL wait on no thread. Its position in the pass
SHALL be data on the instance (the pass sheet: iteration, stages in flight and done, held
outputs, completion, phase) guarded by one per-instance lock, and every transition SHALL be made
by the thread that brings the event: the worker that settles a stage's last item marks the stage
done and sends its journal barrier; the control-channel reader that brings the barrier's ack
releases the held outputs into the next stage's cell mailboxes; the data-channel reader that
brings a message starts a pass or queues the message; the reader that brings a stop marks
cancel; the reader that brings the last restore record opens the restore gate; park-idle,
`max_duration`, the commit-ack re-send and the restore-gate cap are entries on the deferred-wake
list. Bookkeeping SHALL NOT block and SHALL NOT run author code; edge transforms and `cells_for`
run on the producing worker before an output is held, a journal write is a frame and its ack an
event. There SHALL be no bookkeeping thread pool. A live instance and a resumed one SHALL share
one representation: the sheet is what the journal's resume state describes.

The reason is one mechanism: after a proper sleep the only duty a per-instance thread would
have is to hold a stage number, and a mechanism whose sole purpose is to hold one number beside
the mechanism every flow relay runs on is a parallel implementation. `sdk/rust/benches/steps_scaling.rs`
stays as the regression gate: after this change ten thousand awake instances SHALL NOT cost more
resident memory than the envelope recorded in `docs/internal/resilience/steps_relay.md`, and a
population waiting in any state SHALL consume no measurable CPU, because no wait polls.

#### Scenario: A waiting population costs no CPU

- **WHEN** ten thousand `PerInput` instances wait for their next message, or ten thousand
  instances wait for a commit ack
- **THEN** no thread SHALL exist on their behalf and the process SHALL consume no measurable CPU
  for them, where the polling waits cost about 1.6 of 8 cores before

#### Scenario: A blocked population's cost is its heap alone

- **WHEN** ten thousand instances are blocked mid-stage in author code
- **THEN** the threads in use SHALL be the SDK workers running that code and nothing else, and
  the resident cost per instance SHALL NOT exceed the recorded envelope

#### Scenario: The probe answers the question on demand

- **WHEN** the per-instance cost of a steps relay is re-opened
- **THEN** the probe SHALL report resident and virtual cost per instance, thread count per
  instance, CPU while waiting, start and teardown latency, and an unrelated instance's latency
  beside the population, for a population blocked mid-stage and for one waiting for input

#### Scenario: A stop reaches an instance with no thread to interrupt

- **WHEN** `StepsStop` arrives while an instance waits for a message, waits for an ack, or
  has a cell in a retry backoff
- **THEN** the reader that brings it SHALL end the wait at once: a waiting instance ends
  `cancelled`, an owed ack is dropped and compensation starts, a pending backoff is disarmed and
  the cell's gates released, with no tick to wait for

### Requirement: A host steps emit SHALL be released by the core's derivation of its channel

A host SDK SHALL release a steps tasklet's emit to the pass, as one untransformed edge output
naming the producing channel, exactly when the core derived that channel as inter-tasklet in
the declaration answer it gave the SDK; a channel the core derived plugin-bound, a call-end
terminal included, SHALL release nothing. No SDK SHALL re-derive the rule from its own channel
spec, and the core's hold SHALL keep nothing for a plugin-bound channel.

#### Scenario: A call-end terminal releases nothing in every SDK
- **WHEN** the last tasklet of a `per_input` steps class emits on a channel whose only chain ends in a call, on each SDK
- **THEN** no runner hands the core an edge output for that emit, and the iteration's journaled handoff is the same on every SDK

### Requirement: An activation SHALL carry its Cell's identity bytes

The core SHALL hand a host the Cell's wire identity bytes, the same bytes the core routes and
stamps with, on the first activation for that Cell since the host last dropped or rebuilt it (the
core's `Leave` or `RestoreFold` for the Cell), on the first after an activation for it was
abandoned or failed whole, which may never have reached the host, and on no other; the host SHALL
keep them with the Cell, a re-sent copy as the first, and read `stable_id`'s identity input from them instead of building the layout itself. The FNV-1a-128 derivation stays in each
SDK, and the cross-language vector SHALL be asserted by every SDK's tests.

#### Scenario: The identity crosses once per host Cell
- **WHEN** a hosted runner handles three activations for one Cell, then the Cell leaves and returns
- **THEN** the identity bytes SHALL ride the first activation and the first after the return, and no other

#### Scenario: An abandoned activation hands the identity again
- **WHEN** a Cell's first activation is abandoned because the worker it was handed to is gone
- **THEN** the identity bytes SHALL ride the re-handed activation

#### Scenario: stable_id is equal across the SDKs
- **WHEN** a Cell of relay `r`, tasklet `t` and key `k` calls `stable_id` twice at change seq 7 on each SDK
- **THEN** the two answers are equal across every SDK, and each SDK's test pins the shared vector

