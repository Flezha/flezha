# unbounded-correction Specification

## Purpose

The unbounded-correction capability lets an unbounded operative fix the past when a source sends a late or corrected event: an operative with the effectively-once tier, an `EventTimeFrom` clock, and keyed plugin channels may declare a `correction_window`, and an input that lands behind the Segment's event-time high-water but inside that window triggers an automatic restore-and-replay of the input-log slice above a rewound baseline, event-time-ordered, under a bumped epoch — so conforming sinks overwrite the stale rows by natural key and converge on the corrected values. Inputs are journaled before they are forwarded, so the late input is already in the log at its arrival offset and the slice contains it by construction. Beyond-window inputs follow a declared policy, bursts of late inputs coalesce into one correction per Segment, and the same transaction backs the operator verb `flz operative replay`.
## Requirements
### Requirement: Correction window declaration with validated prerequisites

An unbounded operative MAY declare a `correction_window`. Declaration SHALL
require the effectively-once tier, an `EventTimeFrom` clock, and keyed plugin
channels, and SHALL be refused with a typed error naming the missing prerequisite
otherwise. Those three are authoring errors the engine cannot repair.

Retention SHALL NOT be a prerequisite. A declared `correction_window` SHALL instead
**derive** the retention it needs: the operative's input-log hold window SHALL resolve
to at least the declared window, and its snapshot age floor SHALL guarantee a baseline
inside the declared window. An operator-configured value that is already more generous
SHALL be preserved. Operative start SHALL NOT be refused on retention grounds.

A derivation that raises a retention floor above its configured value SHALL be
observable: it SHALL log once for that operative and emit telemetry naming the floor,
its configured value, and its derived value — the disk a declaration commits must
never be committed silently.

#### Scenario: Declaration without event time refused

- **WHEN** an operative declares `correction_window` with `type Clock = WallClock`
- **THEN** operative start fails with a typed error naming the clock requirement

#### Scenario: Declaration derives its own hold window

- **WHEN** an operative declares `correction_window(60_000)` and no input-log hold
  window is configured
- **THEN** the operative starts, its input-log hold window resolves to 60 000 ms, and
  truncation never removes an offset newer than that window

#### Scenario: Declaration derives its own snapshot floor

- **WHEN** an operative declares `correction_window(60_000)` and the resolved snapshot
  age floor is shorter than the window
- **THEN** the operative starts and its age floor resolves to at least the window, so
  compaction always leaves a baseline the rewind can land on

#### Scenario: A more generous operator setting wins

- **WHEN** an operative declares `correction_window(60_000)` and the operator has
  configured an input-log hold window of 300 000 ms
- **THEN** the resolved hold window stays 300 000 ms

#### Scenario: A raised floor is announced

- **WHEN** a declared window raises a retention floor above its configured value
- **THEN** the raise is logged once for that operative and emitted as telemetry naming
  the floor, its configured value, and its derived value

### Requirement: Late inputs inside the window trigger automatic correction

Each Segment SHALL track its event-time high-water. An input whose extracted
event time is behind the high-water by no more than the correction window SHALL
NOT be processed as arrival; the SDK SHALL request correction and hold the
Segment's drain. The engine SHALL rewind the Segment to the last state seq whose
event time precedes the late input's, restore the executor to that baseline, and
re-forward the input-log range above the baseline's consumed offset marked as a
replay slice. The SDK SHALL buffer that slice to its end-of-slice marker, order
it by extracted event time — stably, tie-broken by ascending input-log offset —
and only then dispatch it.

The late input is already in the input log at its arrival offset, because inputs
are journaled before they are forwarded; the slice therefore contains it by
construction and no separate splice SHALL be performed.

#### Scenario: Late trade corrects a candle

- **WHEN** a Segment building 1m candles has high-water 14:06:00 and receives a
  trade with event time 14:05:10, within a 10-minute window
- **THEN** after correction completes, the Segment's state equals the state that
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

Late inputs older than the correction window SHALL follow the operative's
declared policy: `process_as_arrival` (default) or `drop`. Both outcomes SHALL
emit telemetry identifying the segment and the lateness magnitude.

#### Scenario: Default policy processes as arrival

- **WHEN** an input arrives 2 hours late against a 10-minute window with no
  policy declared
- **THEN** it is processed as a normal arrival and beyond-window telemetry is
  emitted

#### Scenario: Drop policy drops with telemetry

- **WHEN** the operative declares `drop` and a beyond-window input arrives
- **THEN** the input is not processed and drop telemetry is emitted

### Requirement: Corrected outputs republish under a bumped epoch

A deliberate correction or replay SHALL bump the Segment's epoch. Corrected
effect records SHALL supersede WAL history above the correction boundary via a
non-destructive rewind that retains records at or below the baseline seq and
drops only the snapshots above it; the epoch bump SHALL be durable before any
output can be re-emitted under it. Corrected outputs SHALL flow through the
normal outbox flush carrying the new epoch so conforming sinks overwrite stale
rows by natural key. Crash-driven re-emits SHALL NOT bump the epoch.

#### Scenario: Sink converges to corrected values

- **WHEN** a correction replays a window that had already published candle
  `BTC/1m/14:05` to a changelog Kafka topic and a guarded Postgres table
- **THEN** both sinks' final visible row for `BTC/1m/14:05` is the corrected
  value, with no duplicate rows

#### Scenario: Crash during correction resumes without a second bump

- **WHEN** the controller crashes mid-correction and the correction re-runs
  after restart
- **THEN** the re-run reuses the same bumped epoch (no double increment) and the
  sink outcome is identical to an uninterrupted correction

### Requirement: Correction requests coalesce per Segment

Concurrent or bursty late inputs for one Segment SHALL coalesce into a single
correction whose target is the oldest requested event time and whose splice
contains all pending late inputs. At most one correction per Segment SHALL be in
flight.

#### Scenario: Burst of late trades replays once

- **WHEN** five late trades for one Segment arrive within the debounce interval
- **THEN** exactly one restore-splice-replay cycle runs and its splice contains
  all five trades in event-time order

### Requirement: Operator replay verb

`flz operative replay <segment> --from <seq|ts>` SHALL re-process the retained
input window from the given point through the current operative code and
republish outputs under a bumped epoch, using the same correction transaction
with an empty splice. Targets older than retention SHALL be refused with an
`outside_retention` error naming the earliest available point.

#### Scenario: Replay heals a sink after a code fix

- **WHEN** an operative with a calculation bug is fixed and redeployed, and the
  operator runs `flz operative replay` from a point inside retention
- **THEN** the sink's rows for the replayed range converge to values computed by
  the fixed code

#### Scenario: Replay outside retention refused

- **WHEN** the operator requests a replay from a timestamp older than the
  retained window
- **THEN** the verb fails with `outside_retention` and reports the earliest
  available target
</content>
</invoke>

