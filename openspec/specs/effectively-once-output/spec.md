# effectively-once-output Specification

## Purpose
TBD - created by archiving change effectively-once-output. Update Purpose after archive.
## Requirements
### Requirement: Atomic effect record — state, outputs, and offset in one durable write

In tier mode the SDK SHALL ship an invocation's state change(s), plugin emits,
and consumed input offset as ONE `cell_effect_record` on the reliable state
sink, and the controller SHALL persist it as ONE WAL record covered by ONE fsync
window. A Cell of a durable steps class SHALL ship the same record for every
invocation, with no consumed offset, and the controller SHALL persist it the same way.

State and offset are the pair that SHALL never tear: no record SHALL make a
state change durable without the offset that acknowledges the input producing
it, and none SHALL advance the offset without that state. Under the default
per-input granularity the record's offset is the invocation's own input; under
`checkpoint` ack-granularity it is the offset of the input that closed the
interval, and the record carries every change the interval accumulated. For a
steps operator Cell the pair is state and stage: no `task_committed` entry SHALL be
durable without every record it names being durable first.

Outputs are not part of that pair. A record MAY carry outputs with no offset —
which is how a checkpoint-mode operator's mid-interval emits reach their
plugins without acknowledging an input whose state is not durable yet. Such
outputs are emitted again when the interval replays, and the keyed-sink dedup
the tier requires absorbs them.

#### Scenario: Record cannot tear

- **WHEN** the controller crashes at any point while handling an effect record
- **THEN** after restart either the change, its outputs, and its offset are all
  durable, or none of them are

#### Scenario: State is never durable ahead of its offset

- **WHEN** a checkpoint-mode Cell applies changes for offsets 4701..4710
  without checkpointing
- **THEN** no durable record contains those changes, and the durable offset
  stays at the last checkpoint

#### Scenario: A stage marker is never durable ahead of its state

- **WHEN** the controller crashes at any point between a steps operator Cell's effect record arriving and its stage's `task_committed` entry being acked
- **THEN** after restart the journal holds the entry only if the Cell's WAL holds the record it names

#### Scenario: Emits leave the primary stream in tier mode

- **WHEN** an opted-in operator's invocation emits on a keyed channel
- **THEN** the output travels inside the effect record on the reliable sink, and
  no output frame for it appears on the primary stream

### Requirement: Fsync-gated outbox flush

For tier operators and for durable steps operators the controller SHALL invoke the
`:on_output` plugin chain for an output only after its effect record is fsynced and the kind's
commit unit has committed it: for a flow operator Cell the record's fsync is the commit; for a steps operator
Cell the commit is the durable `task_committed` or `compensated` entry that names the record
within its Cell's range, and the commit worker SHALL mark that range committed on the Cell's
writer after the entry's ack. Each outbox entry SHALL carry the durable identity `(flz.cell, outbox_seq, epoch)`
where `outbox_seq` is `<record WAL seq>.<emit ordinal>` and `epoch` is the Cell's realign epoch,
plus `flz.key` when the channel is keyed and the author's metadata, for both kinds. This closes
the `flz.seq` exactness boundary: `outbox_seq` SHALL be a sufficient handle for exact append-only
dedup.

A steps operator Cell's entries SHALL stay queued, on disk, until a marker names them; an entry that
no marker will name, because the stage was interrupted and re-run, failed, or was unwound by a
rewind, SHALL be dropped without reaching the chain: by the Cell's next committed range, which
starts above it, or at the instance's next bind, which re-derives the committed ranges from the
journal's live entries. An entry already handed to the chain SHALL be left to its
confirmation. A steps instance's teardown SHALL wait, bounded, until every Cell writer of
the instance reports no entry pending and none awaiting confirmation.

The controller SHALL bound the entries one Cell has inside the plugin chain
unconfirmed to `outbox_inflight_max` (default 1 000) — the window is the set of
dispatched entries still awaiting completion, nothing else — and SHALL refill
it on every event that can open it: an fsync that makes further entries
durable, a committed range that admits further entries, and a completed confirmation that frees a slot. The periodic retry
tick SHALL be a backstop for an operator that was not yet bound at dispatch
time, never the pump that drains a burst.

#### Scenario: No flush before durable point

- **WHEN** an effect record is received but its fsync has not completed
- **THEN** its outputs have not been dispatched to any plugin

#### Scenario: No flush before the stage marker

- **WHEN** a steps operator Cell's effect record is durable and its stage's `task_committed` entry is not yet acked
- **THEN** its outputs have not been dispatched to any plugin, and they are dispatched once the entry naming the record is acked

#### Scenario: A failed stage delivers nothing

- **WHEN** a Cell of tasklet B writes a record with an output and B then fails, because a later item of that Cell or a sibling Cell exhausts its ladder
- **THEN** B's output never reaches a plugin: no marker names the record, and the entry stays held until a bind drops it or the instance's state goes

#### Scenario: Durable identity on every flushed output

- **WHEN** an output is dispatched to the plugin chain in tier mode or from a durable steps operator Cell
- **THEN** its metadata carries `flz.cell`, the durable `outbox_seq`, the
  epoch, `channel`, `flz.key` when the channel is keyed, and the author's metadata

#### Scenario: A confirmation alone refills the window

- **WHEN** ingest has stopped, so no further fsync occurs, while more durable
  entries are pending than the window admits
- **THEN** each completed confirmation dispatches the next pending entry and
  the whole backlog drains at the sink's confirmation rate, not one window
  per retry tick

### Requirement: Restart re-drives unflushed outbox entries

The controller SHALL maintain a per-Cell `outbox_flushed_seq` cursor that
never runs ahead of what the external sink has confirmed delivered, for a tier flow operator Cell and
a durable steps operator Cell alike. An
in-memory hand-off (a cast to the operator or an async admission into a
plugin queue) SHALL NOT advance the cursor. On restart the controller SHALL
re-drive every durable outbox entry above the cursor, in order; a steps operator Cell's writer SHALL
release only the re-driven entries a live journal entry names, SHALL drop those below the last
range it names that no entry names, and SHALL hold the rest until the instance's bind, which
drops them. The writer SHALL learn that its Cell commits by stage from the class registry,
which records the kind before the instance's runner starts. Re-driven
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

#### Scenario: A steps operator's committed output re-drives, its uncommitted output does not

- **WHEN** the controller crashes after journaling `task_committed(A)` naming A's Cell record at seq 3 but before dispatching its output, while B's Cell holds a record at seq 1 that no entry names, then restarts and the instance rebinds
- **THEN** A's output is dispatched with its original identity and confirmed once, and B's record's output is dropped from the queue and never dispatched

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
the holds the run takes and settles the entry with that count; an entry
completes when every output has settled and every hold has been answered, so
a chain with no guaranteed sink completes as soon as it has run. A chain that
crashes SHALL settle nothing — the entry stays pending and the resettle
re-dispatches it under identical identity. A hold taken outside a dispatcher
run (a sink fed by something other than the dispatcher) SHALL report through
the token itself.

The reports SHALL be batched, never the facts: a sink SHALL confirm a
completed batch with one report covering every entry the batch completed, and
the dispatcher SHALL hand an fsync window's outputs to the chain as one batch
that settles with one report carrying each entry's holds. A batch report is
the same per-entry facts delivered at once; per-entry completion, the cursor's
leading-run advance, and the handling of a report for an entry a resettle
superseded are unchanged by batching.

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

- **WHEN** the operator host crashes while running an output's chain, after
  a sink held the token
- **THEN** no settlement is reported, the entry stays pending with its holds
  unanswered, and the resettle re-dispatches its outputs under identical
  identity instead of completing a run that never finished

#### Scenario: A hold outside a dispatcher run still counts

- **WHEN** a sink holds an output's token from a process that is not running
  the dispatcher's chain
- **THEN** the hold reaches the owning writer through the token and the entry
  waits for its confirmation exactly as for a hold taken inside a run

#### Scenario: A batch of outputs costs one confirmation report

- **WHEN** a sink's batch of 200 guaranteed outputs spanning 200 entries is
  acknowledged
- **THEN** the writer receives one report naming the 200 entries, each entry
  completes exactly as it would have from 200 single reports, and the cursor
  advances over the leading completed run

#### Scenario: A batch report for a superseded entry changes nothing

- **WHEN** a batch report names an entry the resettle re-dispatched meanwhile
- **THEN** that entry's tally is left to its live dispatch and every other
  entry in the report completes normally

### Requirement: End-to-end effectively-once over crash-replay

A tier-enabled keyed-channel pipeline SHALL deliver each logical output
effectively once, source to conforming sink, across runner crash, controller
crash, and input replay: the sink's final visible state contains no duplicates
and no gaps. The end-to-end property SHALL be exercised by an
integration test that kills each party at every phase boundary.

#### Scenario: Runner kill mid-stream

- **WHEN** the runner is killed while processing and the Cell is restored
  and replays from its durable consumed-offset
- **THEN** the conforming sink's final state equals the no-crash run's final
  state

#### Scenario: Controller kill mid-flush

- **WHEN** the controller is killed between fsync and sink flush and restarts
- **THEN** the conforming sink's final state equals the no-crash run's final
  state

### Requirement: An inter-tasklet output's offset is never durable ahead of the output

An invocation's consumed input offset SHALL NOT become durable before every inter-tasklet (edge) output that invocation produced has been admitted to its consumer's mailbox. An item with an undelivered edge output SHALL be treated as unresolved: its changes, its plugin outputs and its offset SHALL be held out of the flush buffer until the delivery completes, exactly as a failed-but-retryable attempt's changes are held until its item resolves.

An edge output SHALL also be durable in the producer's outbox, inside the effect record that consumed its input, exactly as a plugin-bound output is, and SHALL be delivered to its consumer from that outbox entry until the consumer's own record acknowledges it. A restore SHALL re-deliver an edge entry that no consumer record acknowledged, so a value admitted to a consumer's mailbox and lost with it before the consumer's record became durable is delivered again rather than lost; the producer's floor skipping the input no longer strands the consumer.

With the ordering correct, at-least-once edge delivery composes into effectively-once without a second mechanism: the edge payload carries the producer's input offset, the consumer's own effect record acknowledges that offset, and the consumer's restored floor skips a re-emitted duplicate. A fan-out invocation SHALL hold its record until **every** consumer's push has landed; a replay after a partial fan-out re-emits to all consumers and each one's own floor decides whether it is a duplicate.

A steps operator Cell's edge output is the one edge output with a durable copy: it is held on the instance's pass sheet and journaled as the stage's handoff in the `task_committed` entry, and its consumers are fed from that entry on resume. A steps operator Cell's record therefore carries no offset and SHALL NOT be held for an edge delivery: it ships as soon as the item settles, and the flusher's deferred-push gate never applies to it.

This requirement binds the host (C-ABI) resolution path, where an activation's outputs are reported at completion and were previously folded before being routed. The typed in-process path emits during the invocation, before the effect block is assembled, and already satisfies it. The Elixir SDK, whose Cell ships its own records and whose router is one process that blocks on a full consumer, holds an invocation's effect record in a FIFO until its router reports every edge push admitted and ships the FIFO in seq order, never gating its own queue — a gated Cell and a router blocked on it would wait on each other; a replacement after an untrappable kill loses the held records, whose offsets were never durable, and the controller's re-forward reproduces them.

#### Scenario: A crash between the fold and the delivery loses nothing

- **WHEN** a tier-mode host activation produces an edge output, the consumer's mailbox is full, and the runner dies before the output is admitted
- **THEN** no durable record SHALL carry that invocation's consumed offset, the controller SHALL re-forward the input, and the producer SHALL re-run and re-emit it

#### Scenario: The re-emitted output is not applied twice

- **WHEN** a producer re-runs an input after a crash and re-emits to a consumer that had already durably consumed that offset
- **THEN** the consumer SHALL skip the re-delivered value against its restored consumed floor and its state SHALL be unchanged

#### Scenario: A partial fan-out is completed by replay

- **WHEN** an invocation emits to three consumers, two pushes land, and the runner dies before the third
- **THEN** the invocation's offset SHALL NOT be durable, and after restart all three consumers SHALL end in the state they would have reached had the invocation completed once

#### Scenario: A steps operator Cell's record is not held for its edge output

- **WHEN** a steps operator Cell emits towards the next stage and its item settles
- **THEN** its effect record ships at once, the edge value waits on the pass sheet for the stage's commit ack, and a crash before that ack re-seeds the consumer from the journaled handoff rather than from a re-run of the producer

#### Scenario: The Elixir Cell holds its record until the edge is admitted
- **WHEN** a tier-mode Elixir Tasklet emits towards a consumer whose mailbox of one is full and whose dispatch is held
- **THEN** the producer's effect record for that input is not on the stream while the push waits, and ships as soon as the consumer admits it

#### Scenario: A value lost from the consumer's mailbox is delivered again

- **WHEN** a tier producer's effect record for offset 7 is durable, its edge value was admitted to the consumer's mailbox, and the runner dies before the consumer's record for offset 7 is durable
- **THEN** after the restore the producer SHALL NOT re-run offset 7, the consumer SHALL receive the edge value from the producer's outbox, and the consumer's state SHALL reflect offset 7 exactly once

### Requirement: A durable steps operator Cell writes the effect record without declaring the tier

A Cell of a durable steps class SHALL settle every invocation into one `cell_effect_record`
exactly as a Cell of a tier flow class does: its state changes, its plugin outputs as
`OutputRecord {channel, key, metadata, payload}`, its timer ops, no consumed offset (the
iteration commit carries the pass's offset), no dead-letter park (an exhausted ladder fails the
stage). The record SHALL be framed and sent on the worker that settles the item, before the
stage's barrier is sent, never left to the flusher's linger. A steps class SHALL NOT declare
`effectively_once`; the record is what its durability means. A stage that ran a Cell and
produced no change and no output SHALL write no record for it, and the stage marker SHALL name
that Cell with no seq.

There SHALL be one settle path for a steps operator Cell's item on every SDK: a retryable failure
holds the attempt's changes until the item resolves and ships them with the resolving attempt,
and an attempt that exhausts the ladder ships the held changes in a record carrying no output,
so the Cell's WAL matches the host's memory on every path.

#### Scenario: One record per invocation, before the barrier

- **WHEN** a durable steps tasklet's Cell applies two changes and emits once on a keyed channel with metadata `{"trace": "t1"}`
- **THEN** exactly one `cell_effect_record` frame for that Cell leaves the runner before the stage's `TaskCommit`, its body carrying both changes and one output with the channel, the key, the metadata and the payload, and no `cell_state_change` frame is sent for the invocation

#### Scenario: A host steps operator Cell's record leaves ahead of its barrier

- **WHEN** a Java steps tasklet commits a state change and the runner's flush linger is raised to 20 s
- **THEN** the Cell's effect record reaches the controller before its stage's `TaskCommit`, and the controller's journal never holds a `task_committed` entry naming a record the Cell's WAL does not hold

#### Scenario: A failed attempt's changes ship with the attempt that resolves

- **WHEN** a steps operator Cell's attempt applies a change and fails retryably, and the next attempt succeeds
- **THEN** one record ships carrying both attempts' changes, and no record shipped between the two

#### Scenario: A steps class cannot declare the tier

- **WHEN** a steps class writes `exactly` under its run in any SDK
- **THEN** it cannot be written in Rust and Java and is refused as `topology.unknown_field` naming `exactly` and `steps` elsewhere, and a durable steps class that declares nothing writes effect records

### Requirement: The effectively-once tier is the `exactly` setting under `run: flow`

A flow class SHALL declare its duplicate rule as `exactly: at_least_once` (the default,
adding no latency and permitting a duplicate output on replay) or
`exactly: effectively_once`, which SHALL turn on the input log, the effect record and the
outbox and SHALL require every plugin-bound channel to be keyed, exactly as the builder
call did. No SDK SHALL offer the tier as a builder call. A steps class SHALL take no
`exactly` setting: a durable steps instance writes effect records and deduplicates across
hops without declaring it, as today.

#### Scenario: The setting turns the tier on

- **WHEN** a flow class declares `run: {flow, exactly: effectively_once}` and `durable: local`
- **THEN** its registration SHALL announce the tier, the controller SHALL open the instance's input log and outbox, and an unkeyed plugin-bound channel SHALL be refused with `topology.unkeyed_channel_under_effectively_once`

#### Scenario: The builder call is gone

- **WHEN** each SDK's channel builder surface is listed
- **THEN** none SHALL offer a tier, checkpoint or correction call, and the conformance surface snapshot SHALL match

