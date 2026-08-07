# entity-timers Specification

## Purpose
TBD - created by archiving change entity-timers. Update Purpose after archive.
## Requirements
### Requirement: A Procedure SHALL be able to register per-Entity timers of two kinds

`ProcedureCtx` SHALL offer `timer_at_event_time(ts_ms)` and `timer_after(duration)`
(Rust) / `timerAtEventTime(long)` and `timerAfter(Duration)` (Java), each returning
the absolute millisecond timestamp the timer is scheduled at, and each SHALL offer a
cancellation taking that timestamp. Registering the same kind and timestamp twice on
one Entity SHALL be idempotent — pending timers are a set, not a queue. Timers SHALL
be scoped to the Entity that registered them and SHALL NOT be visible to any other
Entity.

#### Scenario: A timer is registered and returns its absolute time

- **WHEN** a Procedure calls `timer_after(Duration.ofSeconds(30))` while `ctx.now()` is 1000
- **THEN** the call SHALL return 31000 and a processing-time timer SHALL be pending for that Entity at 31000

#### Scenario: Duplicate registration is idempotent

- **WHEN** a Procedure registers a processing-time timer at 31000 twice
- **THEN** exactly one timer SHALL be pending and `on_timer` SHALL be invoked exactly once for it

#### Scenario: Cancellation removes a pending timer

- **WHEN** a Procedure registers a timer at 31000 and later cancels 31000 before it fires
- **THEN** `on_timer` SHALL NOT be invoked for 31000

#### Scenario: Timers are per Entity

- **WHEN** Entity `a` registers a timer at 31000 and Entity `b` of the same Procedure does not
- **THEN** `on_timer` SHALL be invoked for `a` only, with `a`'s state

### Requirement: A fired timer SHALL invoke `on_timer` with the scheduled timestamp

`Procedure` SHALL gain `on_timer(fired_at, kind, state, ctx)` receiving the same
`StateHandle` and `ProcedureCtx` as `run`, so it MAY apply changes, emit, and
register further timers. `fired_at` SHALL be the timestamp the timer was **scheduled
at**, never the wall-clock time at which it actually fired. The default
implementation SHALL panic with a message naming the Procedure, because registering
a timer without handling it is an authoring error not detectable at build time.

#### Scenario: A late-firing processing-time timer reports its scheduled time

- **WHEN** a timer scheduled for 31000 fires at wall-clock 31480 because the Entity was busy
- **THEN** `on_timer` SHALL receive `fired_at = 31000`

#### Scenario: on_timer may emit and mutate state

- **WHEN** an `on_timer` body applies a Change and emits a value
- **THEN** the Change SHALL be journaled and the value SHALL be emitted exactly as from `run`

#### Scenario: on_timer may register a further timer

- **WHEN** an `on_timer` body calls `timer_after(...)`
- **THEN** the new timer SHALL become pending for that Entity

#### Scenario: An unhandled timer names the Procedure

- **WHEN** a Procedure registers a timer and does not override `on_timer`
- **THEN** firing SHALL panic with a message naming the Procedure and directing the author to implement `on_timer`

### Requirement: Event-time timers SHALL fire on the Entity's event-time high-water

An event-time timer SHALL fire when the Entity's event-time high-water becomes
greater than or equal to its scheduled timestamp. All such timers SHALL fire in
ascending scheduled order, and SHALL fire before the input that advanced the
high-water is dispatched to that Entity. An Entity receiving no input SHALL NOT fire
event-time timers, because nothing advances its clock. A Procedure whose declared
Clock does not derive time from the input SHALL be refused when it registers an
event-time timer.

#### Scenario: Crossing the high-water fires in order

- **WHEN** an Entity has event-time timers pending at 100, 200 and 300, its high-water is 50, and an input with event time 250 arrives
- **THEN** `on_timer` SHALL be invoked for 100 then 200, and the input SHALL be dispatched afterwards; 300 SHALL remain pending

#### Scenario: A quiet Entity does not fire

- **WHEN** an Entity has an event-time timer pending at 100, its high-water is 50, and no further input arrives
- **THEN** `on_timer` SHALL NOT be invoked, however much wall-clock time passes

#### Scenario: An event-time timer without an event-time clock is refused

- **WHEN** a Procedure declaring a wall-clock or logical clock calls `timer_at_event_time(...)`
- **THEN** the call SHALL return an error naming the Procedure and its declared clock, and no timer SHALL be registered

### Requirement: Processing-time timers SHALL fire onto the Entity's mailbox

A processing-time timer SHALL be tracked by one timer wheel per Operative and, on
expiry, SHALL be enqueued onto the target Entity's existing mailbox. `on_timer`
SHALL therefore never run concurrently with `run` on the same Entity, and firing
SHALL obey the same ordering and back-pressure the mailbox already applies to
inputs.

#### Scenario: Firing is serialized against run

- **WHEN** a processing-time timer expires while the Entity is inside a `run` invocation
- **THEN** `on_timer` SHALL be invoked only after that invocation returns

#### Scenario: A due timer fires on restore

- **WHEN** an Operative restarts and an Entity's restored pending set contains a processing-time timer whose scheduled time has already passed
- **THEN** it SHALL fire immediately after restore, in ascending scheduled order relative to other due timers

### Requirement: Pending timers SHALL be durable state, requiring no new store

A timer registration, cancellation, and firing SHALL each be expressed as a
`Change`, so the pending set is rebuilt by the ordinary change-log fold on restore.
There SHALL be no timer side-file, no timer journal entry type, no crash marker and
no completeness oracle. A timer registration made during a replay-driven invocation
SHALL be registered, not skipped — a timer is internal state, unlike
`ctx.spawn_bounded`, which is skipped on replay because it is an external effect.

#### Scenario: A crash before firing leaves the timer pending

- **WHEN** an Entity registers a timer, the change is journaled, and the runner crashes before the timer fires
- **THEN** after restore the timer SHALL be pending and SHALL fire when its condition is met

#### Scenario: A crash after firing does not re-fire

- **WHEN** a timer fires, its effects and its removal from the pending set are journaled, and the runner then crashes
- **THEN** after restore the timer SHALL NOT be pending and `on_timer` SHALL NOT be invoked again for it

#### Scenario: Replay registers the same timers

- **WHEN** an Entity is replayed over the same inputs that originally registered timers
- **THEN** the replayed pending set SHALL equal the original, and registration SHALL NOT be skipped as a replay side effect

#### Scenario: Eviction discards the Entity's timers

- **WHEN** a dynamic-mode Entity with pending timers is idle-evicted
- **THEN** its timers SHALL be discarded with its state, and a later input for that key SHALL cold-start an Entity with no pending timers

### Requirement: A firing SHALL be atomic with its effects under the effectively-once tier

The SDK SHALL write one effect record per firing on a Procedure declaring the
effectively-once tier, carrying the Changes applied by `on_timer`, the values it
emitted, and the removal of the fired timer from the pending set. That record's
`consumed_offset` SHALL be absent, because a firing consumes no input offset.
Absent a declared tier, firing SHALL degrade to at-least-once exactly as input
processing does.

#### Scenario: A firing produces a nil-offset effect record

- **WHEN** an `on_timer` body on an effectively-once Procedure applies a change and emits
- **THEN** one effect record SHALL be written carrying the change, the output and no consumed offset, and the input-log watermark SHALL NOT advance as a result

#### Scenario: A crash mid-firing replays the whole firing

- **WHEN** the runner crashes after `on_timer` returns but before its effect record is durable
- **THEN** after restore the timer SHALL still be pending and the firing SHALL be repeated in full, with no partial state change surviving

### Requirement: Pending timers per Entity SHALL be bounded

Each Procedure SHALL carry a `max_timers_per_entity` bound. Registering past it
SHALL return an error naming the Procedure and the bound rather than allocating.
Wheel occupancy and pending-timer counts SHALL be exposed as per-Procedure counters.

#### Scenario: Registration past the cap is refused

- **WHEN** an Entity holding `max_timers_per_entity` pending timers registers one more
- **THEN** the call SHALL return an error naming the Procedure and the bound, and the pending set SHALL be unchanged

#### Scenario: Occupancy is observable

- **WHEN** timers are pending across an Operative's Entities
- **THEN** a per-Procedure pending-timer count SHALL be reported alongside the existing per-Procedure counters

