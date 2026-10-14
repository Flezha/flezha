# effectively-once-correction Specification

## Purpose

The effectively-once-correction capability lets a flow relay fix the past when a source sends a late or corrected event: a relay with the effectively-once tier, an event-time clock, and keyed plugin channels may declare a `correction_window`, and an input that lands behind the Cell's event-time high-water but inside that window triggers an automatic restore-and-replay of the input-log slice above a rewound baseline, event-time-ordered, under a bumped epoch — so conforming output plugins overwrite the stale rows by natural key and converge on the corrected values. Inputs are journaled before they are forwarded, so the late input is already in the log at its arrival offset and the slice contains it by construction. Beyond-window inputs follow a declared policy, bursts of late inputs coalesce into one correction per Cell, and the same transaction backs the administration verb `flz relay replay`.
## Requirements
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

