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

In tier mode the SDK SHALL ship each invocation's state change(s), plugin emits,
and consumed input offset as ONE `segment_effect_record` on the reliable state
sink, and the controller SHALL persist it as ONE WAL record covered by ONE fsync
window. No partial combination (state without outputs, outputs without offset)
SHALL ever be durable.

#### Scenario: Record cannot tear

- **WHEN** the controller crashes at any point while handling an effect record
- **THEN** after restart either the change, its outputs, and its offset are all
  durable, or none of them are

#### Scenario: Emits leave the primary stream in tier mode

- **WHEN** an opted-in operative's invocation emits on a keyed channel
- **THEN** the output travels inside the effect record on the reliable sink, and
  no output frame for it appears on the primary stream

### Requirement: Fsync-gated outbox flush

For tier operatives the controller SHALL invoke the `:on_output` plugin chain for
an output only after its effect record is fsynced. Each outbox entry SHALL carry
the durable identity `(flz.segment, outbox_seq, epoch)` where `outbox_seq` is
`<record WAL seq>.<emit ordinal>`. This closes the `flz.seq` exactness boundary:
`outbox_seq` SHALL be a sufficient handle for exact append-only dedup.

#### Scenario: No flush before durable point

- **WHEN** an effect record is received but its fsync has not completed
- **THEN** its outputs have not been dispatched to any plugin

#### Scenario: Durable identity on every flushed output

- **WHEN** an output is dispatched to the plugin chain in tier mode
- **THEN** its metadata carries `flz.segment`, the durable `outbox_seq`, and the
  epoch

### Requirement: Restart re-drives unflushed outbox entries

The controller SHALL maintain a per-Segment `outbox_flushed_seq` cursor that
never runs ahead of what was actually dispatched. On restart it SHALL re-drive
every durable outbox entry above the cursor, in order. Re-driven entries carry
identical identity so idempotent/dedup sinks absorb them: outputs are never lost
and never applied twice at a conforming sink.

#### Scenario: Crash between fsync and flush

- **WHEN** the controller crashes after fsyncing an effect record but before
  dispatching its outputs, then restarts
- **THEN** the outputs are dispatched exactly once to the plugin chain with their
  original identity

#### Scenario: Crash between flush and cursor write

- **WHEN** the controller crashes after dispatching outputs but before advancing
  `outbox_flushed_seq`, then restarts
- **THEN** the same outputs are re-dispatched with identical identity and a
  conforming sink deduplicates them

### Requirement: End-to-end effectively-once over crash-replay

A tier-enabled keyed-channel pipeline SHALL deliver each logical output
effectively once, source to conforming sink, across executor crash, controller
crash, and input replay: the sink's final visible state contains no duplicates
and no gaps. The end-to-end property SHALL be exercised by an
integration test that kills each party at every phase boundary.

#### Scenario: Executor kill mid-stream

- **WHEN** the executor is killed while processing and the Segment is restored
  and replays from its durable consumed-offset
- **THEN** the conforming sink's final state equals the no-crash run's final
  state

#### Scenario: Controller kill mid-flush

- **WHEN** the controller is killed between fsync and sink flush and restarts
- **THEN** the conforming sink's final state equals the no-crash run's final
  state

### Requirement: Tier applies to both operative kinds

Both unbounded and bounded operatives SHALL be able to opt into the tier with
identical per-record semantics. Bounded op-boundary staged flush (buffering
outputs until op commit) is explicitly out of scope and SHALL NOT be implied by
this capability.

#### Scenario: Bounded operative under the tier

- **WHEN** a bounded operative with the tier emits on a keyed channel and crashes
  before completion, then resumes
- **THEN** its re-emitted outputs carry identical identity and a conforming sink
  deduplicates them

