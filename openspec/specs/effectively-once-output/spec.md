# effectively-once-output Specification

## Purpose
TBD - created by archiving change effectively-once-output. Update Purpose after archive.
## Requirements
### Requirement: Per-operative opt-in tier with cost-free default

The SDK SHALL provide a per-operative `effectively_once` declaration carried in
operative metadata and resolved by the controller. The tier SHALL default to OFF.
With the tier off, every ingest, emit, WAL, and plugin-dispatch path SHALL be
byte-identical to pre-change behaviour with no added per-message work. Building an
opted-in operative with any plugin-bound channel not declared `.keyed()` SHALL
fail with a typed error.

#### Scenario: Default off is cost-free

- **WHEN** an operative without the tier processes messages
- **THEN** wire frames, WAL records, and plugin dispatch are identical to the
  pre-change implementation and the dispatch bench shows no regression

#### Scenario: Unkeyed sink channel rejected under the tier

- **WHEN** an operative declares `effectively_once()` and has a plugin-bound
  channel without `.keyed()`
- **THEN** the build fails with a typed error naming the channel

### Requirement: Atomic effect record — state, outputs, and offset in one durable write

In tier mode the SDK SHALL ship an invocation's state change(s), plugin emits,
and consumed input offset as ONE `entity_effect_record` on the reliable state
sink, and the controller SHALL persist it as ONE WAL record covered by ONE fsync
window.

State and offset are the pair that SHALL never tear: no record SHALL make a
state change durable without the offset that acknowledges the input producing
it, and none SHALL advance the offset without that state. Under the default
per-input granularity the record's offset is the invocation's own input; under
`checkpoint` ack-granularity it is the offset of the input that closed the
interval, and the record carries every change the interval accumulated.

Outputs are not part of that pair. A record MAY carry outputs with no offset —
which is how a checkpoint-mode operative's mid-interval emits reach their
plugins without acknowledging an input whose state is not durable yet. Such
outputs are emitted again when the interval replays, and the keyed-sink dedup
the tier requires absorbs them.

#### Scenario: Record cannot tear

- **WHEN** the controller crashes at any point while handling an effect record
- **THEN** after restart either the change, its outputs, and its offset are all
  durable, or none of them are

#### Scenario: State is never durable ahead of its offset

- **WHEN** a checkpoint-mode Entity applies changes for offsets 4701..4710
  without checkpointing
- **THEN** no durable record contains those changes, and the durable offset
  stays at the last checkpoint

#### Scenario: Emits leave the primary stream in tier mode

- **WHEN** an opted-in operative's invocation emits on a keyed channel
- **THEN** the output travels inside the effect record on the reliable sink, and
  no output frame for it appears on the primary stream

### Requirement: Fsync-gated outbox flush

For tier operatives the controller SHALL invoke the `:on_output` plugin chain for
an output only after its effect record is fsynced. Each outbox entry SHALL carry
the durable identity `(flz.entity, outbox_seq, epoch)` where `outbox_seq` is
`<record WAL seq>.<emit ordinal>`. This closes the `flz.seq` exactness boundary:
`outbox_seq` SHALL be a sufficient handle for exact append-only dedup.

The controller SHALL bound the entries one Entity has inside the plugin chain
unconfirmed to `outbox_inflight_max` (default 1 000) — the window is the set of
dispatched entries still awaiting completion, nothing else — and SHALL refill
it on every event that can open it: an fsync that makes further entries
durable, and a completed confirmation that frees a slot. The periodic retry
tick SHALL be a backstop for an operative that was not yet bound at dispatch
time, never the pump that drains a burst.

#### Scenario: No flush before durable point

- **WHEN** an effect record is received but its fsync has not completed
- **THEN** its outputs have not been dispatched to any plugin

#### Scenario: Durable identity on every flushed output

- **WHEN** an output is dispatched to the plugin chain in tier mode
- **THEN** its metadata carries `flz.entity`, the durable `outbox_seq`, and the
  epoch

#### Scenario: A confirmation alone refills the window

- **WHEN** ingest has stopped, so no further fsync occurs, while more durable
  entries are pending than the window admits
- **THEN** each completed confirmation dispatches the next pending entry and
  the whole backlog drains at the sink's confirmation rate, not one window
  per retry tick

### Requirement: Restart re-drives unflushed outbox entries

The controller SHALL maintain a per-Entity `outbox_flushed_seq` cursor that
never runs ahead of what the external sink has confirmed delivered. An
in-memory hand-off (a cast to the operative or an async admission into a
plugin queue) SHALL NOT advance the cursor. On restart the controller SHALL
re-drive every durable outbox entry above the cursor, in order. Re-driven
entries carry identical identity so idempotent/dedup sinks absorb them:
outputs are never lost and never applied twice at a conforming sink.

#### Scenario: Crash between fsync and flush

- **WHEN** the controller crashes after fsyncing an effect record but before
  dispatching its outputs, then restarts
- **THEN** the outputs are dispatched to the plugin chain with their original
  identity and confirmed by the sink exactly once

#### Scenario: Crash after dispatch but before sink confirmation

- **WHEN** the controller crashes after handing outputs to the plugin chain
  but before the sink confirms the write, then restarts
- **THEN** the cursor has not advanced past those entries and they are
  re-driven with identical identity; a conforming sink deduplicates any
  duplicate that did reach it

#### Scenario: Crash between confirmation and cursor write

- **WHEN** the controller crashes after the sink confirmed a batch but before
  the advanced cursor is durable, then restarts
- **THEN** the same outputs are re-dispatched with identical identity and the
  sink deduplicates them

### Requirement: Sink delivery confirmation gates the outbox cursor

Delivery-guaranteed sinks SHALL confirm delivery per batch, keyed by the
outbox range the batch covers, once the write is durable at the sink (e.g. a
Kafka produce acknowledged, a Postgres transaction committed, a forwarded
ingest confirmed). The confirmation gates only the cursor advance: emits, the
effect-record fsync, and runner acknowledgements SHALL NOT wait on sink
confirmation. A batch whose retry budget is permanently exhausted SHALL be
parked through the existing dead-letter retry-then-park with its outbox range
attached — recorded and replayable, never silently dropped — and only a parked
or confirmed range may advance the cursor. Unconfirmed-outbox age SHALL be
observable and feed the output-stall verdict.

Every flushed output SHALL carry an opaque confirmation token (`flz.ack`)
naming the writer that owns the cursor and the entry one confirmation
completes. A delivery-guaranteed sink SHALL hold the token as it takes
ownership of the output and SHALL confirm it once its write is durable (or
park it). The dispatcher SHALL run each output's chain as one run that counts
the holds the run takes and settles the entry with that count in a single
report; an entry completes when every output has settled and every hold has
been answered, so a chain with no guaranteed sink completes as soon as it has
run. A chain that crashes SHALL settle nothing — the entry stays pending and
the resettle re-dispatches it under identical identity. A hold taken outside
a dispatcher run (a sink fed by something other than the dispatcher) SHALL
report through the token itself.

#### Scenario: Confirmation is off the hot path

- **WHEN** a sink is slow to confirm while inputs keep arriving
- **THEN** ingest, effect-record fsync, and runner acks proceed unchanged;
  only `outbox_flushed_seq` lags, visibly

#### Scenario: Permanent failure parks loudly instead of dropping

- **WHEN** a batch exhausts its retry ladder against a permanently failing
  sink
- **THEN** the batch parks via the dead-letter path with its outbox range, an
  alarm-grade event fires, and the cursor advances past exactly the parked
  range

#### Scenario: A filtered output completes on settlement

- **WHEN** an `:on_output` plugin filters an output so no delivery-guaranteed
  sink takes it
- **THEN** the dispatcher's settlement carries zero holds and the entry
  completes without waiting on any confirmation

#### Scenario: A crashed chain run leaves its entry owed

- **WHEN** the operative host crashes while running an output's chain, after
  a sink held the token
- **THEN** no settlement is reported, the entry stays pending with its holds
  unanswered, and the resettle re-dispatches its outputs under identical
  identity instead of completing a run that never finished

#### Scenario: A hold outside a dispatcher run still counts

- **WHEN** a sink holds an output's token from a process that is not running
  the dispatcher's chain
- **THEN** the hold reaches the owning writer through the token and the entry
  waits for its confirmation exactly as for a hold taken inside a run

### Requirement: End-to-end effectively-once over crash-replay

A tier-enabled keyed-channel pipeline SHALL deliver each logical output
effectively once, source to conforming sink, across runner crash, controller
crash, and input replay: the sink's final visible state contains no duplicates
and no gaps. The end-to-end property SHALL be exercised by an
integration test that kills each party at every phase boundary.

#### Scenario: Runner kill mid-stream

- **WHEN** the runner is killed while processing and the Entity is restored
  and replays from its durable consumed-offset
- **THEN** the conforming sink's final state equals the no-crash run's final
  state

#### Scenario: Controller kill mid-flush

- **WHEN** the controller is killed between fsync and sink flush and restarts
- **THEN** the conforming sink's final state equals the no-crash run's final
  state

### Requirement: Tier applies to both operative kinds

Both stream and workflow operatives SHALL be able to opt into the tier with
identical per-record semantics. Workflow proc-boundary staged flush (buffering
outputs until proc commit) is explicitly out of scope and SHALL NOT be implied by
this capability.

#### Scenario: Workflow operative under the tier

- **WHEN** a workflow operative with the tier emits on a keyed channel and crashes
  before completion, then resumes
- **THEN** its re-emitted outputs carry identical identity and a conforming sink
  deduplicates them

