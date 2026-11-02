# durability-effectively-once Specification

## Purpose

The effectively-once tier: a flow relay whose state changes, outputs and consumed offset are written
as one atomic durable record per invocation, so a crash and replay neither lose nor repeat an
output. Outputs leave through an outbox flushed only after fsync and advanced only on the output
plugin's confirmation, and unflushed entries are re-driven on restart. A relay that folds many
inputs into one state can acknowledge at checkpoints instead of per invocation. With an event-time
clock and a `correction_window`, a late or corrected input inside the window triggers a restore and
event-time-ordered replay under a bumped epoch, so conforming outputs overwrite stale rows; inputs
beyond the window follow the declared policy.

## Requirements

### Requirement: Atomic effect record — state, outputs, and offset in one durable write

In tier mode the SDK SHALL ship an invocation's state change(s), plugin emits,
and consumed input offset as ONE `cell_effect_record` on the reliable state
output plugin, and the controller SHALL persist it as ONE WAL record covered by ONE fsync
window. A Cell of a durable steps class SHALL ship the same record for every
invocation, with no consumed offset, and the controller SHALL persist it the same way.

State and offset are the pair that SHALL never tear: no record SHALL make a
state change durable without the offset that acknowledges the input producing
it, and none SHALL advance the offset without that state. Under the default
per-input granularity the record's offset is the invocation's own input; under
`checkpoint` ack-granularity it is the offset of the input that closed the
interval, and the record carries every change the interval accumulated. For a
steps relay Cell the pair is state and stage: no `task_committed` entry SHALL be
durable without every record it names being durable first.

Outputs are not part of that pair. A record MAY carry outputs with no offset —
which is how a checkpoint-mode relay's mid-interval emits reach their
plugins without acknowledging an input whose state is not durable yet. Such
outputs are emitted again when the interval replays, and the keyed-output plugin dedup
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

- **WHEN** the controller crashes at any point between a steps relay Cell's effect record arriving and its stage's `task_committed` entry being acked
- **THEN** after restart the journal holds the entry only if the Cell's WAL holds the record it names

#### Scenario: Emits leave the primary stream in tier mode

- **WHEN** an opted-in relay's invocation emits on a keyed channel
- **THEN** the output travels inside the effect record on the reliable output plugin, and
  no output frame for it appears on the primary stream

### Requirement: Fsync-gated outbox flush

For tier relays and for durable steps relays the controller SHALL invoke the
`:on_output` plugin chain for an output only after its effect record is fsynced and the kind's
commit unit has committed it: for a flow relay Cell the record's fsync is the commit; for a steps relay
Cell the commit is the durable `task_committed` or `compensated` entry that names the record
within its Cell's range, and the commit worker SHALL mark that range committed on the Cell's
writer after the entry's ack. Each outbox entry SHALL carry the durable identity `(flz.cell, outbox_seq, epoch)`
where `outbox_seq` is `<record WAL seq>.<emit ordinal>` and `epoch` is the Cell's WAL epoch,
plus `flz.key` when the channel is keyed and the author's metadata, for both kinds. This closes
the `flz.seq` exactness boundary: `outbox_seq` SHALL be a sufficient handle for exact append-only
dedup.

A steps relay Cell's entries SHALL stay queued, on disk, until a marker names them; an entry that
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
tick SHALL be a backstop for a relay that was not yet bound at dispatch
time, never the pump that drains a burst.

#### Scenario: No flush before durable point

- **WHEN** an effect record is received but its fsync has not completed
- **THEN** its outputs have not been dispatched to any plugin

#### Scenario: No flush before the stage marker

- **WHEN** a steps relay Cell's effect record is durable and its stage's `task_committed` entry is not yet acked
- **THEN** its outputs have not been dispatched to any plugin, and they are dispatched once the entry naming the record is acked

#### Scenario: A failed stage delivers nothing

- **WHEN** a Cell of tasklet B writes a record with an output and B then fails, because a later item of that Cell or a sibling Cell exhausts its ladder
- **THEN** B's output never reaches a plugin: no marker names the record, and the entry stays held until a bind drops it or the instance's state goes

#### Scenario: Durable identity on every flushed output

- **WHEN** an output is dispatched to the plugin chain in tier mode or from a durable steps relay Cell
- **THEN** its metadata carries `flz.cell`, the durable `outbox_seq`, the
  epoch, `channel`, `flz.key` when the channel is keyed, and the author's metadata

#### Scenario: A confirmation alone refills the window

- **WHEN** ingest has stopped, so no further fsync occurs, while more durable
  entries are pending than the window admits
- **THEN** each completed confirmation dispatches the next pending entry and
  the whole backlog drains at the output plugin's confirmation rate, not one window
  per retry tick

### Requirement: Restart re-drives unflushed outbox entries

The controller SHALL maintain a per-Cell `outbox_flushed_seq` cursor that
never runs ahead of what the external output plugin has confirmed delivered, for a tier flow relay Cell and
a durable steps relay Cell alike. An
in-memory hand-off (a cast to the relay or an async admission into a
plugin queue) SHALL NOT advance the cursor. On restart the controller SHALL
re-drive every durable outbox entry above the cursor, in order; a steps relay Cell's writer SHALL
release only the re-driven entries a live journal entry names, SHALL drop those below the last
range it names that no entry names, and SHALL hold the rest until the instance's bind, which
drops them. The writer SHALL learn that its Cell commits by stage from the class registry,
which records the kind before the instance's runner starts. Re-driven
entries carry identical identity so idempotent/dedup output plugins absorb them:
outputs are never lost and never applied twice at a conforming output plugin.

#### Scenario: Crash between fsync and flush

- **WHEN** the controller crashes after fsyncing an effect record but before
  dispatching its outputs, then restarts
- **THEN** the outputs are dispatched to the plugin chain with their original
  identity and confirmed by the output plugin exactly once

#### Scenario: Crash after dispatch but before output plugin confirmation

- **WHEN** the controller crashes after handing outputs to the plugin chain
  but before the output plugin confirms the write, then restarts
- **THEN** the cursor has not advanced past those entries and they are
  re-driven with identical identity; a conforming output plugin deduplicates any
  duplicate that did reach it

#### Scenario: Crash between confirmation and cursor write

- **WHEN** the controller crashes after the output plugin confirmed a batch but before
  the advanced cursor is durable, then restarts
- **THEN** the same outputs are re-dispatched with identical identity and the
  output plugin deduplicates them

#### Scenario: A steps relay's committed output re-drives, its uncommitted output does not

- **WHEN** the controller crashes after journaling `task_committed(A)` naming A's Cell record at seq 3 but before dispatching its output, while B's Cell holds a record at seq 1 that no entry names, then restarts and the instance rebinds
- **THEN** A's output is dispatched with its original identity and confirmed once, and B's record's output is dropped from the queue and never dispatched

### Requirement: Output plugin delivery confirmation gates the outbox cursor

Delivery-guaranteed output plugins SHALL confirm delivery per batch, keyed by the
outbox range the batch covers, once the write is durable at the output plugin (e.g. a
Kafka produce acknowledged, a Postgres transaction committed, a forwarded
ingest confirmed). The confirmation gates only the cursor advance: emits, the
effect-record fsync, and runner acknowledgements SHALL NOT wait on output plugin
confirmation. A batch whose retry budget is permanently exhausted SHALL be
parked through the existing dead-letter retry-then-park with its outbox range
attached — recorded and replayable, never silently dropped — and only a parked
or confirmed range may advance the cursor. Unconfirmed-outbox age SHALL be
observable and feed the output-stall verdict.

Every flushed output SHALL carry an opaque confirmation token (`flz.ack`)
naming the writer that owns the cursor and the entry one confirmation
completes. A delivery-guaranteed output plugin SHALL hold the token as it takes
ownership of the output and SHALL confirm it once its write is durable (or
park it). The dispatcher SHALL run each output's chain as one run that counts
the holds the run takes and settles the entry with that count; an entry
completes when every output has settled and every hold has been answered, so
a chain with no guaranteed output plugin completes as soon as it has run. A chain that
crashes SHALL settle nothing — the entry stays pending and the resettle
re-dispatches it under identical identity. A hold taken outside a dispatcher
run (an output plugin fed by something other than the dispatcher) SHALL report through
the token itself.

The reports SHALL be batched, never the facts: an output plugin SHALL confirm a
completed batch with one report covering every entry the batch completed, and
the dispatcher SHALL hand an fsync window's outputs to the chain as one batch
that settles with one report carrying each entry's holds. A batch report is
the same per-entry facts delivered at once; per-entry completion, the cursor's
leading-run advance, and the handling of a report for an entry a resettle
superseded are unchanged by batching.

#### Scenario: Confirmation is off the hot path

- **WHEN** an output plugin is slow to confirm while inputs keep arriving
- **THEN** ingest, effect-record fsync, and runner acks proceed unchanged;
  only `outbox_flushed_seq` lags, visibly

#### Scenario: Permanent failure parks loudly instead of dropping

- **WHEN** a batch exhausts its retry ladder against a permanently failing
  output plugin
- **THEN** the batch parks via the dead-letter path with its outbox range, an
  alarm-grade event fires, and the cursor advances past exactly the parked
  range

#### Scenario: A filtered output completes on settlement

- **WHEN** an `:on_output` plugin filters an output so no delivery-guaranteed
  output plugin takes it
- **THEN** the dispatcher's settlement carries zero holds and the entry
  completes without waiting on any confirmation

#### Scenario: A crashed chain run leaves its entry owed

- **WHEN** the relay host crashes while running an output's chain, after
  an output plugin held the token
- **THEN** no settlement is reported, the entry stays pending with its holds
  unanswered, and the resettle re-dispatches its outputs under identical
  identity instead of completing a run that never finished

#### Scenario: A hold outside a dispatcher run still counts

- **WHEN** an output plugin holds an output's token from a process that is not running
  the dispatcher's chain
- **THEN** the hold reaches the owning writer through the token and the entry
  waits for its confirmation exactly as for a hold taken inside a run

#### Scenario: A batch of outputs costs one confirmation report

- **WHEN** an output plugin's batch of 200 guaranteed outputs spanning 200 entries is
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
effectively once, source to conforming output plugin, across runner crash, controller
crash, and input replay: the output plugin's final visible state contains no duplicates
and no gaps. The end-to-end property SHALL be exercised by an
integration test that kills each party at every phase boundary.

#### Scenario: Runner kill mid-stream

- **WHEN** the runner is killed while processing and the Cell is restored
  and replays from its durable consumed-offset
- **THEN** the conforming output plugin's final state equals the no-crash run's final
  state

#### Scenario: Controller kill mid-flush

- **WHEN** the controller is killed between fsync and output plugin flush and restarts
- **THEN** the conforming output plugin's final state equals the no-crash run's final
  state

### Requirement: An inter-tasklet output's offset is never durable ahead of the output

An invocation's consumed input offset SHALL NOT become durable before every inter-tasklet (edge) output that invocation produced has been admitted to its consumer's mailbox. An item with an undelivered edge output SHALL be treated as unresolved: its changes, its plugin outputs and its offset SHALL be held out of the flush buffer until the delivery completes, exactly as a failed-but-retryable attempt's changes are held until its item resolves.

An edge output SHALL also be durable in the producer's outbox, inside the effect record that consumed its input, exactly as a plugin-bound output is, and SHALL be delivered to its consumer from that outbox entry until the consumer's own record acknowledges it. A restore SHALL re-deliver an edge entry that no consumer record acknowledged, so a value admitted to a consumer's mailbox and lost with it before the consumer's record became durable is delivered again rather than lost; the producer's floor skipping the input no longer strands the consumer.

With the ordering correct, at-least-once edge delivery composes into effectively-once without a second mechanism: the edge payload carries the producer's input offset, the consumer's own effect record acknowledges that offset, and the consumer's restored floor skips a re-emitted duplicate. A fan-out invocation SHALL hold its record until **every** consumer's push has landed; a replay after a partial fan-out re-emits to all consumers and each one's own floor decides whether it is a duplicate.

A steps relay Cell's edge output is the one edge output with a durable copy: it is held on the instance's pass sheet and journaled as the stage's handoff in the `task_committed` entry, and its consumers are fed from that entry on resume. A steps relay Cell's record therefore carries no offset and SHALL NOT be held for an edge delivery: it ships as soon as the item settles, and the flusher's deferred-push gate never applies to it.

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

#### Scenario: A steps relay Cell's record is not held for its edge output

- **WHEN** a steps relay Cell emits towards the next stage and its item settles
- **THEN** its effect record ships at once, the edge value waits on the pass sheet for the stage's commit ack, and a crash before that ack re-seeds the consumer from the journaled handoff rather than from a re-run of the producer

#### Scenario: The Elixir Cell holds its record until the edge is admitted
- **WHEN** a tier-mode Elixir Tasklet emits towards a consumer whose mailbox of one is full and whose dispatch is held
- **THEN** the producer's effect record for that input is not on the stream while the push waits, and ships as soon as the consumer admits it

#### Scenario: A value lost from the consumer's mailbox is delivered again

- **WHEN** a tier producer's effect record for offset 7 is durable, its edge value was admitted to the consumer's mailbox, and the runner dies before the consumer's record for offset 7 is durable
- **THEN** after the restore the producer SHALL NOT re-run offset 7, the consumer SHALL receive the edge value from the producer's outbox, and the consumer's state SHALL reflect offset 7 exactly once

### Requirement: A durable steps relay Cell writes the effect record without declaring the tier

A Cell of a durable steps class SHALL settle every invocation into one `cell_effect_record`
exactly as a Cell of a tier flow class does: its state changes, its plugin outputs as
`OutputRecord {channel, key, metadata, payload}`, its timer ops, no consumed offset (the
iteration commit carries the pass's offset), no dead-letter park (an exhausted ladder fails the
stage). The record SHALL be framed and sent on the worker that settles the item, before the
stage's barrier is sent, never left to the flusher's linger. A steps class SHALL NOT declare
`effectively_once`; the record is what its durability means. A stage that ran a Cell and
produced no change and no output SHALL write no record for it, and the stage marker SHALL name
that Cell with no seq.

There SHALL be one settle path for a steps relay Cell's item on every SDK: a retryable failure
holds the attempt's changes until the item resolves and ships them with the resolving attempt,
and an attempt that exhausts the ladder ships the held changes in a record carrying no output,
so the Cell's WAL matches the host's memory on every path.

#### Scenario: One record per invocation, before the barrier

- **WHEN** a durable steps tasklet's Cell applies two changes and emits once on a keyed channel with metadata `{"trace": "t1"}`
- **THEN** exactly one `cell_effect_record` frame for that Cell leaves the runner before the stage's `TaskCommit`, its body carrying both changes and one output with the channel, the key, the metadata and the payload, and no `cell_state_change` frame is sent for the invocation

#### Scenario: A host steps relay Cell's record leaves ahead of its barrier

- **WHEN** a Java steps tasklet commits a state change and the runner's flush linger is raised to 20 s
- **THEN** the Cell's effect record reaches the controller before its stage's `TaskCommit`, and the controller's journal never holds a `task_committed` entry naming a record the Cell's WAL does not hold

#### Scenario: A failed attempt's changes ship with the attempt that resolves

- **WHEN** a steps relay Cell's attempt applies a change and fails retryably, and the next attempt succeeds
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

### Requirement: Consumed-offset advances only at checkpoint boundaries

In checkpoint mode the Cell's durable `consumed_offset` SHALL advance only
when an interval closes: when the relay calls `ctx.checkpoint()`, or when a
compaction or a handover closes it early. Changes applied between closes
SHALL be held in the Cell's flush buffer and SHALL NOT be made durable on
their own. The record written at the close SHALL carry the interval's
accumulated changes and the closing input's consumed-offset as one fsynced unit
— state and offset advance together or not at all.

A compaction (`CompactCell`) and a handover (`MigrationSerialization`, and the
graceful-shutdown state that shares its serializer) SHALL close an open
interval early, into the record its own close would write, and SHALL ship that
record under the Cell's gate and buffer lock before the snapshot or the
handed-over state is taken, so the state the cut holds and the consumed offset
the controller holds name the same inputs. For a handover, the source controller
SHALL release the handed-over state only once those records are durable on it,
within the forced handover grace and what is left of the hub's state query
timeout, because the destination reads its floor from the copy it pulls next;
past that it SHALL hand over no state, and the destination restores from that
copy and re-forwards above its floor. A migration's walk SHALL share one
serializer deadline across its Cells. A cut SHALL NOT wait for room in the
controller's credit window past its deadline: the Cell is then refused as busy
before anything is drained.
Only those two cuts SHALL close an interval early; nothing SHALL be added to the
per-input path for it.

A Cell holding the changes of an attempt awaiting retry SHALL NOT be cut, since
no record can carry those changes until the item resolves: a compaction SHALL
refuse it with `state.cell_busy` and ship no snapshot, and a handover SHALL
leave it out of the handed-over state and log it, as it leaves out a Cell still
inside an activation at its gate wait (a retry's backoff holds the gate).

A checkpoint that accumulated no changes SHALL still write its record: the
record exists to carry the offset.

#### Scenario: Inputs between checkpoints advance nothing durable

- **WHEN** a Cell processes offsets 4701..4710 without checkpointing
- **THEN** its durable consumed-offset remains at the last checkpoint's offset
  4700, and none of the changes those inputs applied are durable

#### Scenario: Checkpoint closes the interval atomically

- **WHEN** the relay checkpoints after processing offset 4710
- **THEN** one record carrying the accumulated changes and consumed-offset 4710
  is fsynced, and only then does the durable offset advance

#### Scenario: A compaction closes the open interval early

- **GIVEN** a Cell checkpointed at offset 4700 that processed offsets
  4701..4703 without checkpointing
- **WHEN** the Cell receives `CompactCell`
- **THEN** one record carrying the 4701..4703 changes and consumed-offset 4703
  ships before the snapshot, the snapshot holds it, and the compaction is not
  refused

#### Scenario: A deadline-only Cell under steady input compacts

- **GIVEN** a Cell declaring only `max_delay_ms` whose interval is open at
  every compaction sweep
- **WHEN** the Watchdog asks it for a snapshot
- **THEN** each compaction closes the interval early and writes its snapshot
  instead of being refused

#### Scenario: A handover inside an open interval counts each input once

- **GIVEN** a Cell checkpointed at offset 4700 that processed offsets
  4701..4703 without checkpointing
- **WHEN** the relay is migrated
- **THEN** the record carrying consumed-offset 4703 ships before the
  handed-over state, and the destination, whichever of the state and the
  re-forward from 4704 it takes first, holds each input once

#### Scenario: A cut waits out a retried attempt

- **GIVEN** a Cell holding the changes of an attempt awaiting retry
- **WHEN** the Cell receives `CompactCell`, or its relay is migrated
- **THEN** the compaction is refused with `state.cell_busy` and no snapshot,
  and the handed-over state leaves the Cell out

### Requirement: Mid-interval outputs flush when they are produced

By default an emit made before the interval closes SHALL be flushed by the
controller as soon as its record is fsynced, in a record carrying NO
consumed-offset. Checkpointing moves the acknowledgement, not the output. An
interval's outputs are therefore emitted again when the interval replays, and
the keyed-output plugin dedup the tier already requires SHALL absorb them.

A checkpoint-ack relay MAY declare `outputs: :at_checkpoint` with a
`max_delay_ms`: its mid-interval outputs SHALL then ride the interval's
checkpoint record and be flushed as one batch after that record is fsynced,
and the interval SHALL close after `every` inputs or `max_delay_ms` since its
first un-checkpointed input, whichever comes first, so a quiet Cell never
holds an output longer than `max_delay_ms`. Replay semantics SHALL be
unchanged by the option: outputs of an interval that did not checkpoint are
re-produced when the interval replays, outputs of a checkpointed record are
re-driven with identical identity from the cursor, and a conforming keyed output plugin
absorbs both.

#### Scenario: An emit does not wait for the checkpoint

- **WHEN** a checkpoint-mode invocation emits on a keyed channel and does not
  checkpoint
- **THEN** the output is flushed to the plugin after its record is fsynced, and
  the Cell's durable consumed-offset is unchanged

#### Scenario: Batched outputs flush with the checkpoint record

- **WHEN** a relay declared `outputs: :at_checkpoint` emits 300 outputs
  across an interval of 500 inputs
- **THEN** no output reaches a plugin before the checkpoint, and after the
  checkpoint record is fsynced all 300 are handed to the chain as one batch
  with identities `<record seq>.0` … `<record seq>.299`

#### Scenario: A quiet Cell's outputs flush within the delay

- **WHEN** an `outputs: :at_checkpoint` Cell with `max_delay_ms: 50` emits
  once and receives no further input
- **THEN** its interval closes within 50 ms and the output is flushed, with the
  consumed-offset acknowledged at that close

#### Scenario: An interval opened after a checkpoint waits its own delay

- **WHEN** a checkpoint closes an interval and the next input opens a new one
  before the closed interval's `max_delay_ms` would have passed
- **THEN** the new interval SHALL stay open until `max_delay_ms` has passed
  since its own first input, unless a checkpoint or `every` closes it first

#### Scenario: A crash before the checkpoint loses nothing and duplicates nothing durable

- **WHEN** an `outputs: :at_checkpoint` Cell crashes after emitting but
  before its interval checkpoints
- **THEN** no output of that interval reached a plugin, the interval replays
  from the checkpointed offset, and the re-produced outputs reach the output plugin once

### Requirement: A dead-letter park closes the interval

An invocation that exhausts its retries and parks to the dead-letter queue SHALL
close the interval as if it had checkpointed: its record carries the accumulated
changes, the park, and the consumed-offset together. The poison input is thereby
acknowledged and is never re-forwarded onto the tasklet by a later interval
replay.

#### Scenario: A parked poison input is not replayed back onto the tasklet

- **GIVEN** a checkpoint-mode Cell that has processed offsets 4701..4707
  without checkpointing
- **WHEN** offset 4708 exhausts its retries and parks
- **THEN** one record carries the 4701..4708 changes, the DLQ entry, and
  consumed-offset 4708 — and a subsequent recovery replays from 4709, not 4701

### Requirement: Recovery restores the checkpoint and replays the interval

On recovery, a checkpoint-mode Cell SHALL be restored from its durable
records and the controller SHALL re-forward the input log from the restored
consumed-offset through the relay in order. The replay SHALL keep the
current epoch. The Cell SHALL discard any open interval accumulated before
the restore — the restored state does not contain it, and the re-forwarded
inputs produce it again.

#### Scenario: Crash mid-interval replays from the checkpoint

- **GIVEN** a Cell checkpointed at offset 4700 that crashed after processing
  offset 4708
- **WHEN** the Cell is restored
- **THEN** state is restored to the 4700 checkpoint and offsets 4701..4708 (and
  any newer retained inputs) are re-forwarded in order under the same epoch

#### Scenario: State never resumes past inputs it did not see

- **WHEN** a checkpoint-mode Cell is restored at any point in its interval
- **THEN** the offset it resumes from is one the restored state has already
  folded in

### Requirement: Retention holds at least one checkpoint interval and the watermark steps

The input-log retention floor for a checkpoint-mode Cell SHALL be its last
checkpointed offset — at least one full checkpoint interval is always retained.
The relay's min-watermark SHALL advance in steps at checkpoint cadence, and
the source-ingestion credit loop SHALL grant accordingly (burst grants at
watermark steps, park in between when credit is exhausted).

#### Scenario: Truncation never crosses a live interval

- **WHEN** a checkpoint-mode Cell's last checkpoint is at offset 4700
- **THEN** input-log truncation removes only offsets strictly below 4700
  regardless of how far the Cell has processed ahead

### Requirement: A held watermark is not read as a stall

The controller's liveness detectors SHALL NOT report a checkpoint-mode relay
as a stalled or dead data-flow merely because it made nothing durable for an
interval. Silence between checkpoints is the mode's steady state.

#### Scenario: A quiet interval does not trip the input-silence sweep

- **WHEN** a checkpoint-mode relay that has produced output before makes
  nothing durable for longer than the input-silence timeout
- **THEN** no `input_silence` is reported for it

### Requirement: Checkpoint acknowledgement is the `ack` setting under `effectively_once`

`effectively_once` SHALL carry `ack`, one of `per_input` (the default) and `checkpoint`
with `every` (a positive count) and `max_delay_ms` (a positive delay), and the checkpoint
behaviour SHALL be what the retired requirement described: the consumed offset advances at
checkpoint boundaries, mid-interval outputs flush when produced, a park closes the interval,
recovery replays the interval. `ack` SHALL be writable nowhere else. A zero `every` or
`max_delay_ms` SHALL stay refused by the existing value codes.

#### Scenario: A checkpointing flow class

- **WHEN** a flow class declares `exactly: {effectively_once, ack: {checkpoint, every: 100, max_delay_ms: 1000}}`
- **THEN** its consumed offset SHALL advance only at `ctx.checkpoint()` or at the hundredth input or after a second, whichever the retired requirement's rules reach first, and a crash SHALL replay from the last checkpoint

#### Scenario: `ack` cannot be written under `at_least_once` or under `steps`

- **WHEN** a class writes `ack` under `exactly: at_least_once`, or under `run: steps`
- **THEN** the SDK SHALL fail to compile where its language can and otherwise refuse registration with `topology.unknown_field` naming `ack` and its parent

### Requirement: Late inputs inside the window trigger automatic correction

Each Cell SHALL track its event-time high-water. An input whose extracted
event time is behind the high-water by no more than the correction window SHALL
NOT be processed as arrival; the SDK SHALL request correction and hold the
Cell's drain. The engine SHALL rewind the Cell to the last state seq whose
event time precedes the late input's, restore the runner to that baseline, and
re-forward the input-log range above the baseline's consumed offset marked as a
replay slice. The SDK SHALL buffer that slice to its end-of-slice marker, order
it by extracted event time — stably, tie-broken by ascending input-log offset —
and only then dispatch it.

The late input is already in the input log at its arrival offset, because inputs
are journaled before they are forwarded; the slice therefore contains it by
construction and no separate splice SHALL be performed.

#### Scenario: Late trade corrects a candle

- **WHEN** a Cell building 1m candles has high-water 14:06:00 and receives a
  trade with event time 14:05:10, within a 10-minute window
- **THEN** after correction completes, the Cell's state equals the state that
  a run receiving the trade in event-time order would have produced

#### Scenario: Slice membership is durable, not timing-dependent

- **WHEN** the same correction is re-run after a crash
- **THEN** the replayed slice contains exactly the same inputs in exactly the same
  order, because its membership is an input-log offset range and its order is a
  function of the inputs alone

#### Scenario: Live inputs are not lost during correction

- **WHEN** new inputs arrive while a correction replay is in progress
- **THEN** none are dropped: those journaled at or below the slice's upper bound
  are dispatched as members of the sorted slice, and those journaled above it are
  dispatched in arrival order after the slice

### Requirement: Beyond-window late inputs follow the declared policy

Late inputs older than the correction window SHALL follow the relay's
declared policy: `process_as_arrival` (default) or `drop`.

#### Scenario: Default policy processes as arrival

- **WHEN** an input arrives 2 hours late against a 10-minute window with no
  policy declared
- **THEN** it is processed as a normal arrival

#### Scenario: Drop policy drops the input

- **WHEN** the relay declares `drop` and a beyond-window input arrives
- **THEN** the input is not processed

### Requirement: Corrected outputs republish under a bumped epoch

A deliberate correction or replay SHALL bump the Cell's epoch. Corrected
effect records SHALL supersede WAL history above the correction boundary via a
non-destructive rewind that retains records at or below the baseline seq and
drops only the snapshots above it; the epoch bump SHALL be durable before any
output can be re-emitted under it. Corrected outputs SHALL flow through the
normal outbox flush carrying the new epoch so conforming output plugins overwrite stale
rows by natural key. Crash-driven re-emits SHALL NOT bump the epoch.

#### Scenario: Output plugin converges to corrected values

- **WHEN** a correction replays a window that had already published candle
  `BTC/1m/14:05` to a changelog Kafka topic and a guarded Postgres table
- **THEN** both output plugins' final visible row for `BTC/1m/14:05` is the corrected
  value, with no duplicate rows

#### Scenario: Crash during correction resumes without a second bump

- **WHEN** the controller crashes mid-correction and the correction re-runs
  after restart
- **THEN** the re-run reuses the same bumped epoch (no double increment) and the
  output plugin outcome is identical to an uninterrupted correction

### Requirement: Correction requests coalesce per Cell

Concurrent or bursty late inputs for one Cell SHALL coalesce into a single
correction whose target is the oldest requested event time and whose splice
contains all pending late inputs. At most one correction per Cell SHALL be in
flight.

#### Scenario: Burst of late trades replays once

- **WHEN** five late trades for one Cell arrive within the debounce interval
- **THEN** exactly one restore-splice-replay cycle runs and its splice contains
  all five trades in event-time order

### Requirement: Manual replay verb

`flz relay replay <cell> --from <seq|ts>` SHALL re-process the retained
input window from the given point through the current relay code and
republish outputs under a bumped epoch, using the same correction transaction
with an empty splice. Targets older than retention SHALL be refused with an
error coded `relay.outside_retention` (class `invalid`, exit 2), its details naming the earliest available point.

A Cell of a steps class SHALL be refused before anything is rewound, with an
error stating that a steps relay's baseline is an iteration boundary across every Cell of the
instance, recorded in its journal, and not one Cell's WAL seq, so the per-Cell correction
transaction has no baseline to rewind to; and that a steps relay's inputs are released to
retention at each iteration commit, so there is no retained window to re-process. The refusal
SHALL name `flz relay rewind` as the steps relay's verb for taking a loop back. The
instance's input log SHALL be untouched by the refusal.

#### Scenario: Replay heals an output plugin after a code fix

- **WHEN** a relay with a calculation bug is fixed and redeployed, and the
  user runs `flz relay replay` from a point inside retention
- **THEN** the output plugin's rows for the replayed range converge to values computed by
  the fixed code

#### Scenario: Replay outside retention refused

- **WHEN** the user requests a replay from a timestamp older than the
  retained window
- **THEN** the verb fails with the code `relay.outside_retention` and reports the earliest
  available target

#### Scenario: Replay of a steps relay Cell is refused naming the instance-wide baseline

- **WHEN** the user runs `flz relay replay` naming a Cell of a durable
  steps instance
- **THEN** the verb fails naming the iteration boundary as the instance's baseline and
  `flz relay rewind` as the verb to use, no Cell is rewound, no epoch is
  bumped, and the instance's input log and floor are unchanged

### Requirement: The correction window is `correction_ms` under `effectively_once`

`effectively_once` SHALL carry `correction_ms`, a positive window in milliseconds, absent
by default. A window SHALL be refused with `topology.correction_window_zero` when zero and
with `topology.correction_without_event_time` when the class's tasklets declare no
event-time clock, as today. The correction behaviour inside and beyond the window SHALL be
what the remaining requirements of this capability describe.

#### Scenario: A window on a flow class with event time

- **WHEN** a flow class declares `exactly: {effectively_once, correction_ms: 5000}` and its keyed tasklet declares an event-time clock
- **THEN** a late input inside the window SHALL trigger the automatic correction the capability describes, and the registration SHALL be accepted

#### Scenario: A window without event time stays refused

- **WHEN** the same class declares no event-time clock
- **THEN** registration SHALL be refused with `topology.correction_without_event_time`
