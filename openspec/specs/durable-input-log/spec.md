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

