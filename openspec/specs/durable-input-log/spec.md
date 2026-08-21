# durable-input-log Specification

## Purpose
TBD - created by archiving change effectively-once-output. Update Purpose after archive.
## Requirements
### Requirement: Per-operative offset-addressable input-log, gated by the tier

For an operative with the effectively-once tier enabled, the controller SHALL
journal every inbound payload (after the `:on_input` transform chain) into a
durable per-operative input-log before forwarding it to the runner, assigning a
strictly monotonic offset. Operatives without the tier SHALL NOT write an
input-log and their ingest path SHALL be byte-identical to the pre-change
behaviour.

#### Scenario: Payload journaled before forward

- **WHEN** a source plugin delivers a payload to an opted-in operative
- **THEN** the payload is appended to the operative's input-log with offset N
  before the runner receives it

#### Scenario: Tier off means no log

- **WHEN** an operative without the tier receives payloads
- **THEN** no input-log file exists for it and the forward path performs no
  journaling work

### Requirement: Offset travels with the forwarded input

The assigned offset SHALL be stamped on the forwarded frame as reserved metadata
`flz.offset`, and the SDK SHALL thread it through to the invocation so the
resulting atomic effect record carries the same offset as its consumed-offset.

#### Scenario: Offset round-trips

- **WHEN** input at offset 4711 is dispatched and the invocation produces an
  effect record
- **THEN** the effect record's consumed-offset equals 4711

### Requirement: Crash-safe, replayable log records

Input-log records SHALL be crc-protected appends (same durability class as the
per-Entity WAL, fsync-batched). On restart the controller SHALL be able to read
back any retained offset range in order; a torn tail record SHALL be discarded
without affecting earlier records.

#### Scenario: Replay after crash

- **WHEN** the controller restarts after a crash and an Entity's durable
  consumed-offset is 4700 while the log holds offsets up to 4720
- **THEN** the controller re-forwards offsets 4701..4720 to the restored Entity
  in order

#### Scenario: Torn tail discarded

- **WHEN** the last append was interrupted mid-write
- **THEN** reopening the log yields all complete records and drops only the torn
  tail

### Requirement: Min-watermark retention

The controller SHALL track the minimum durable consumed-offset across all
Entities of the operative (the watermark) and SHALL truncate the input-log only
strictly below `min(watermark, now − hold_window)`. An Entity that has consumed
nothing holds the watermark at the log start.

`hold_window` SHALL resolve **per operative** as the greater of the operator-configured
value and the operative's own declared `correction_window`. It SHALL default to zero
for an operative that declares no correction window and for which the operator has
configured none, so a non-correcting operative truncates at its watermark exactly as it
does with no hold window at all.

#### Scenario: Truncation respects the slowest entity

- **WHEN** entity A has durably consumed to offset 4711 and entity B to 4710
- **THEN** truncation removes only offsets below 4710

#### Scenario: A declared correction window holds the log

- **WHEN** an operative declares `correction_window(60_000)`, every Entity has
  consumed to offset 4711, and offset 4711 was appended 10 s ago
- **THEN** offset 4711 is retained, because it is newer than the derived 60 s hold
  window

#### Scenario: An operative with no correction window has no hold

- **WHEN** an operative declares no correction window, the operator configures no hold
  window, and every Entity has consumed to offset 4711
- **THEN** truncation removes every offset below 4711 immediately

### Requirement: The log owns its watermark

The input-log process SHALL keep each Entity's durable consumed-offset itself
and derive the operative's min-watermark from that map; a consumed-offset
report SHALL update the log's copy and SHALL NOT scan any shared table. A
burst of reports SHALL fold into one watermark recompute and one credit drain.
The published watermark table SHALL be a rebuildable cache for readers outside
the log — the retention sweep, replay cursors, the CLI — created and owned by
the state supervisor so it outlives every process that reports into it; a
(re)started log SHALL seed its own map from that table. Offsets are assigned
monotonically, so the bytes a watermark advance frees SHALL be taken as a
prefix of the tracked offsets, costing the number freed rather than the number
still in flight.

#### Scenario: A restarted log recovers its watermark

- **WHEN** the input-log process restarts while its Entities' last
  consumed-offsets are in the published table
- **THEN** the reopened log computes the same min-watermark as before the
  restart without waiting for a fresh report from every Entity

#### Scenario: The table survives its reporters

- **WHEN** every `WalWriter` that reported a consumed-offset has exited
- **THEN** the published table and its rows are intact, because the state
  supervisor owns it

#### Scenario: A burst of advances recomputes once

- **WHEN** N Entities report consumed-offset advances within one scheduling
  window of the log
- **THEN** the log recomputes its min-watermark and drains source credit once
  for the burst, not once per report

### Requirement: An append never waits on a sync

The log SHALL sync on a companion process holding a second handle on the same
file (a datasync covers the inode, not the handle), driven by the fsync
batcher's tick, so an append is answered while a datasync is in progress.
Appends queued behind one another SHALL be folded into one write in arrival
order, each answered with its own offset; the caller encodes its record before
the call. The record that first overruns source credit SHALL park, and so
SHALL every record queued behind it in that batch, so a later record is never
admitted ahead of an earlier parked one. The synchronous flush (`flush_now`,
shutdown) SHALL round-trip to the companion, and truncation SHALL reopen the
companion's handle on the replacement file.

#### Scenario: An append is answered during a datasync

- **WHEN** the companion is inside a datasync when an append arrives
- **THEN** the append is written and answered without waiting for the sync
  to return

#### Scenario: A burst of appends is one write, in order

- **WHEN** several appends are queued at the log at once
- **THEN** they are written with one write call, assigned consecutive offsets
  in arrival order, and each caller receives its own offset

#### Scenario: A credit overrun parks the whole tail in order

- **WHEN** the second of four queued appends overruns source credit while the
  third is small enough to fit
- **THEN** the first is written and the second, third and fourth park in that
  order; when credit mints, they are admitted in the same order

#### Scenario: Truncation keeps the companion on the live file

- **WHEN** the log truncates below its watermark, replacing the file
- **THEN** the next sync covers the replacement file, not the unlinked one

### Requirement: A truncation costs the tail and never the sweeper

The log SHALL keep a sparse offset-to-position index of its own records (one
sample per fixed number of appends), locate a truncation floor within one
sample's span, and copy only the retained tail into the replacement file, so a
truncation's work is the retained tail rather than the whole log. The
retention sweeper SHALL request a truncation without waiting on the log, and a
truncation whose file has vanished (its class directory removed) SHALL be a
no-op. Records written before a log restart are not indexed; the first
truncation after a restart MAY scan from the start of the file once.

#### Scenario: A large consumed prefix is cut in tail time

- **WHEN** the log holds hundreds of thousands of consumed records below the
  floor and a few thousand above it
- **THEN** the truncation decodes at most one index span to find the floor,
  copies the tail as bytes, and appends resume immediately after

#### Scenario: A busy log cannot crash the sweeper

- **WHEN** the retention sweep requests a truncation of a log that is busy
- **THEN** the sweeper continues with its other work and the log truncates in
  its turn; no call timeout exists to kill the sweeper

#### Scenario: A vanished file is nothing to truncate

- **WHEN** a queued truncation runs after the log's class directory was removed
- **THEN** the log keeps running and cuts nothing

### Requirement: A parked append parks the source, never the host

The host process of an effectively-once operative SHALL NOT wait on the input log: it SHALL hand each append to the log and forward the input when the log answers, keeping forward order equal to offset order and answering each confirming call in call order. While the log parks appends at zero source credit, the host SHALL keep running its output chains, its control calls and its timers; the park SHALL be observable only as the source's owed call. A log that dies with an append in flight SHALL cause that append to be re-issued once and then answered as refused, never left owed.

#### Scenario: Zero credit parks the source's call and nothing else

- **WHEN** the operative's source credit is exhausted and a source's confirming ingest is in flight
- **THEN** the source's call stays unanswered, and the host still answers a control call and runs an `{:io_out}` chain dispatched to it meanwhile

#### Scenario: The host's own confirmations free its parked appends

- **WHEN** the appends of an operative are parked and the sink confirms outputs the host's chain delivered while parked
- **THEN** the resulting watermark advance mints credit, the log answers the parked appends in arrival order, and the host forwards the inputs and answers their verdicts in the same order

#### Scenario: Forward order is offset order

- **WHEN** three inputs are ingested in call order a, b, c
- **THEN** they are forwarded to the runner in offset order a, b, c and their verdicts answer in that order

#### Scenario: A log that dies mid-append answers refused

- **WHEN** the input log exits while an append is in flight and the re-issued append finds no log
- **THEN** the confirming call receives `{:error, :refused}` and the input is not forwarded
