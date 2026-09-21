# cell-timers Specification

## Purpose
TBD - created by archiving change cell-timers. Update Purpose after archive.
## Requirements
### Requirement: A Tasklet SHALL be able to register per-Cell timers of two kinds

`TaskletCtx` SHALL offer `timer_at_event_time(ts_ms)` and `timer_after(duration)`
(Rust) / `timerAtEventTime(long)` and `timerAfter(Duration)` (Java) / `Flezha.SDK.Ctx.timer_at_event_time/2` and `timer_after/2` taking milliseconds and answering `{:ok, at}` (Elixir), each returning
the absolute millisecond timestamp the timer is scheduled at, and each SHALL offer a
cancellation taking that timestamp. Registering the same kind and timestamp twice on
one Cell SHALL be idempotent — pending timers are a set, not a queue. Timers SHALL
be scoped to the Cell that registered them and SHALL NOT be visible to any other
Cell.

#### Scenario: A timer is registered and returns its absolute time

- **WHEN** a Tasklet calls `timer_after(Duration.ofSeconds(30))` while `ctx.now()` is 1000
- **THEN** the call SHALL return 31000 and a processing-time timer SHALL be pending for that Cell at 31000

#### Scenario: Duplicate registration is idempotent

- **WHEN** a Tasklet registers a processing-time timer at 31000 twice
- **THEN** exactly one timer SHALL be pending and `on_timer` SHALL be invoked exactly once for it

#### Scenario: Cancellation removes a pending timer

- **WHEN** a Tasklet registers a timer at 31000 and later cancels 31000 before it fires
- **THEN** `on_timer` SHALL NOT be invoked for 31000

#### Scenario: Timers are per Cell

- **WHEN** Cell `a` registers a timer at 31000 and Cell `b` of the same Tasklet does not
- **THEN** `on_timer` SHALL be invoked for `a` only, with `a`'s state

#### Scenario: The Elixir ctx registers the same absolute timestamp and cancels by it
- **WHEN** an Elixir Tasklet calls `Ctx.timer_after(ctx, 30_000)` while `Ctx.now(ctx)` is 1000, calls it again, and then calls `Ctx.cancel_processing_time_timer(ctx, 31000)`
- **THEN** both registrations answer `{:ok, 31000}` and leave exactly one processing-time timer pending at 31000, and the cancellation answers `true` and leaves none

### Requirement: A fired timer SHALL invoke `on_timer` with the scheduled timestamp

`Tasklet` SHALL gain `on_timer(fired_at, kind, state, ctx)` receiving the same
`StateHandle` and `TaskletCtx` as `run`, so it MAY apply changes, emit, and
register further timers. `fired_at` SHALL be the timestamp the timer was **scheduled
at**, never the wall-clock time at which it actually fired. The default
implementation SHALL panic with a message naming the Tasklet, because registering
a timer without handling it is an authoring error not detectable at build time; in
the Elixir SDK, whose Tasklet is a behaviour with optional callbacks, a firing on a
Tasklet that exports no `on_timer/4` SHALL park non-retryably with a message naming
the Tasklet and the callback — the end state the panic reaches through the retry
ladder, without the retries.

#### Scenario: A late-firing processing-time timer reports its scheduled time

- **WHEN** a timer scheduled for 31000 fires at wall-clock 31480 because the Cell was busy
- **THEN** `on_timer` SHALL receive `fired_at = 31000`

#### Scenario: on_timer may emit and mutate state

- **WHEN** an `on_timer` body applies a Change and emits a value
- **THEN** the Change SHALL be journaled and the value SHALL be emitted exactly as from `run`

#### Scenario: on_timer may register a further timer

- **WHEN** an `on_timer` body calls `timer_after(...)`
- **THEN** the new timer SHALL become pending for that Cell

#### Scenario: An unhandled timer names the Tasklet

- **WHEN** a Tasklet registers a timer and does not override `on_timer`
- **THEN** firing SHALL panic with a message naming the Tasklet and directing the author to implement `on_timer`

#### Scenario: An Elixir Tasklet without on_timer/4 parks the firing by name
- **WHEN** an Elixir Tasklet registers a timer and exports no `on_timer/4`
- **THEN** the firing parks on its first attempt with a message naming the Tasklet and `on_timer/4`, the fired timer's removal is committed, and the Cell is held until the park's ack exactly as for a poison input

### Requirement: Event-time timers SHALL fire on the Cell's event-time high-water

An event-time timer SHALL fire when the Cell's event-time high-water becomes
greater than or equal to its scheduled timestamp. All such timers SHALL fire in
ascending scheduled order, and SHALL fire before the input that advanced the
high-water is dispatched to that Cell. A Cell receiving no input SHALL NOT fire
event-time timers, because nothing advances its clock. A Tasklet whose declared
Clock does not derive time from the input SHALL be refused when it registers an
event-time timer.

#### Scenario: Crossing the high-water fires in order

- **WHEN** a Cell has event-time timers pending at 100, 200 and 300, its high-water is 50, and an input with event time 250 arrives
- **THEN** `on_timer` SHALL be invoked for 100 then 200, and the input SHALL be dispatched afterwards; 300 SHALL remain pending

#### Scenario: A quiet Cell does not fire

- **WHEN** a Cell has an event-time timer pending at 100, its high-water is 50, and no further input arrives
- **THEN** `on_timer` SHALL NOT be invoked, however much wall-clock time passes

#### Scenario: An event-time timer without an event-time clock is refused

- **WHEN** a Tasklet declaring a wall-clock or logical clock calls `timer_at_event_time(...)`
- **THEN** the call SHALL return an error naming the Tasklet and its declared clock, and no timer SHALL be registered

### Requirement: Processing-time timers SHALL fire onto the Cell's mailbox

A processing-time timer SHALL be tracked on the poll core's shared
deferred-wake list — one list per process, serviced by threads already waiting
in `poll` — and, on expiry, SHALL be enqueued onto the target Cell's existing
mailbox. No per-Operative timer thread SHALL exist. In the Elixir SDK, which is not a poll
host, the BEAM's own timer service SHALL be that list: the Cell process arms one
`Process.send_after/3` per pending processing-time timer and, on a due message,
enqueues every timer due at or below the later of the scheduled and the current
time onto its own input queue in ascending order, so the Cell's single drain
serialises firings against `run/3` as it serialises inputs; a due message that
finds no room re-arms itself 5 ms later, never drops; a firing whose timer is no
longer pending fires nothing. `on_timer` SHALL therefore
never run concurrently with `run` on the same Cell, and firing SHALL obey the
same ordering and back-pressure the mailbox already applies to inputs. Due
entries SHALL fire in ascending scheduled order, ties broken by Cell
identity.

#### Scenario: Firing is serialized against run

- **WHEN** a processing-time timer expires while the Cell is inside a `run` invocation
- **THEN** `on_timer` SHALL be invoked only after that invocation returns

#### Scenario: A due timer fires on restore

- **WHEN** an Operative restarts and a Cell's restored pending set contains a processing-time timer whose scheduled time has already passed
- **THEN** it SHALL fire immediately after restore, in ascending scheduled order relative to other due timers

#### Scenario: An Operative that uses timers costs no thread

- **WHEN** an Operative registers processing-time timers across many Cells
- **THEN** no thread SHALL be created on its behalf; its due firings SHALL be
  promoted by whichever thread next enters or wakes inside `poll`, and
  `fired_at` SHALL remain the scheduled timestamp regardless of promotion delay

#### Scenario: Instance scoping survives the shared list

- **WHEN** two Operative instances each arm a timer with the same tasklet id,
  cell key, and scheduled time, and one instance cancels its timer
- **THEN** only that instance's timer SHALL be disarmed and the other SHALL
  fire on schedule

#### Scenario: An Elixir Cell fires from the BEAM's timer service through its own queue
- **WHEN** an Elixir Cell holds processing-time timers at 100 and 200 that are both overdue after a restore, with one input queued ahead of the due message and one arriving after it
- **THEN** `on_timer` runs for 100 then 200 after the first input and before the second, through the Cell's own queue and drain

### Requirement: Pending timers SHALL be durable state, requiring no new store

A timer registration, cancellation, and firing SHALL each be expressed as a
`Change`, so the pending set is rebuilt by the ordinary change-log fold on restore.
There SHALL be no timer side-file, no timer journal entry type, no crash marker and
no completeness oracle. A timer registration made during a replay-driven invocation
SHALL be registered, not skipped — a timer is internal state, unlike
`ctx.spawn_workflow`, which is skipped on replay because it is an external effect.

#### Scenario: A crash before firing leaves the timer pending

- **WHEN** a Cell registers a timer, the change is journaled, and the runner crashes before the timer fires
- **THEN** after restore the timer SHALL be pending and SHALL fire when its condition is met

#### Scenario: A crash after firing does not re-fire

- **WHEN** a timer fires, its effects and its removal from the pending set are journaled, and the runner then crashes
- **THEN** after restore the timer SHALL NOT be pending and `on_timer` SHALL NOT be invoked again for it

#### Scenario: Replay registers the same timers

- **WHEN** a Cell is replayed over the same inputs that originally registered timers
- **THEN** the replayed pending set SHALL equal the original, and registration SHALL NOT be skipped as a replay side effect

#### Scenario: Eviction discards the Cell's timers

- **WHEN** a dynamic-mode Cell with pending timers is idle-evicted
- **THEN** its timers SHALL be discarded with its state, and a later input for that key SHALL cold-start a Cell with no pending timers

### Requirement: A firing SHALL be atomic with its effects under the effectively-once tier

The SDK SHALL write one effect record per firing on a Tasklet declaring the
effectively-once tier, carrying the Changes applied by `on_timer`, the values it
emitted, and the removal of the fired timer from the pending set. That record's
`consumed_offset` SHALL be absent, because a firing consumes no input offset.
Absent a declared tier, firing SHALL degrade to at-least-once exactly as input
processing does.

In the Elixir SDK the firing's ops — its removal as op 0 — ride the record's `timers` beside the changes and the captured outputs, and no `format_id = 3` record is written under the tier.

#### Scenario: A firing produces a nil-offset effect record

- **WHEN** an `on_timer` body on an effectively-once Tasklet applies a change and emits
- **THEN** one effect record SHALL be written carrying the change, the output and no consumed offset, and the input-log watermark SHALL NOT advance as a result

#### Scenario: A crash mid-firing replays the whole firing

- **WHEN** the runner crashes after `on_timer` returns but before its effect record is durable
- **THEN** after restore the timer SHALL still be pending and the firing SHALL be repeated in full, with no partial state change surviving

#### Scenario: An Elixir firing rides the effect record
- **WHEN** a processing-time timer fires on an Elixir Tasklet declaring `effectively_once/1`
- **THEN** one `cell_effect_record` carries `on_timer/4`'s change, its output, the removal op and no consumed offset, and no separate timer-ops record ships

### Requirement: Pending timers per Cell SHALL be bounded

Each Tasklet SHALL carry a `max_timers_per_cell` bound. Registering past it
SHALL return an error naming the Tasklet and the bound rather than allocating.
Wheel occupancy and pending-timer counts SHALL be exposed as per-Tasklet counters.

#### Scenario: Registration past the cap is refused

- **WHEN** a Cell holding `max_timers_per_cell` pending timers registers one more
- **THEN** the call SHALL return an error naming the Tasklet and the bound, and the pending set SHALL be unchanged

#### Scenario: Occupancy is observable

- **WHEN** timers are pending across an Operative's Cells
- **THEN** a per-Tasklet pending-timer count SHALL be reported alongside the existing per-Tasklet counters

