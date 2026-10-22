# cell-timers Specification

## Purpose
TBD - created by archiving change cell-timers. Update Purpose after archive.
## Requirements
### Requirement: A Tasklet SHALL be able to register per-Cell timers of two kinds

The tasklet context (`Ctx`) SHALL offer `timer_at_event_time(ts_ms)` and `timer_after(duration)`
(Rust) / `timerAtEventTime(long)` and `timerAfter(Duration)` (Java) / `Flezha.SDK.Ctx.timer_at_event_time/2` and `timer_after/2` taking milliseconds and answering `{:ok, at}` (Elixir) / `TimerAtEventTime(uint64)` and `TimerAfter(uint64)` taking milliseconds (Go), each returning
the absolute millisecond timestamp the timer is scheduled at, and each SHALL offer one
cancellation taking the timer's kind and that timestamp: `cancel_timer(kind, at)` (Python, Ruby), `cancelTimer(kind, at)` (Node, PHP), `cancelTimer(TimerKind, long)` (Java), `cancel_timer(TimerKind, u64)` (Rust), `CancelTimer(TimerKind, uint64)` (Go) and `Flezha.SDK.Ctx.cancel_timer/3` (Elixir). No SDK SHALL offer a cancellation per kind. Registering the same kind and timestamp twice on
one Cell SHALL be idempotent — pending timers are a set, not a queue. Timers SHALL
be scoped to the Cell that registered them and SHALL NOT be visible to any other
Cell. A hosted SDK's cancellation SHALL answer nothing about whether the timer was pending
(Java `void`, Elixir `:ok`, Go only an `error`, as Python, Ruby, PHP and Node already do): the pending set is the
core's, and the core applies the cancellation when the item settles.

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
- **WHEN** an Elixir Tasklet calls `Ctx.timer_after(ctx, 30_000)` while `Ctx.now(ctx)` is 1000, calls it again, and then calls `Ctx.cancel_timer(ctx, :processing_time, 31000)`
- **THEN** both registrations answer `{:ok, 31000}`, the cancellation answers `:ok`, and once the item settles no processing-time timer is pending at 31000

#### Scenario: One cancellation serves both kinds

- **WHEN** a Tasklet in any SDK registers an event-time timer at 5000 and a processing-time timer at 5000, then cancels the event-time kind at 5000
- **THEN** only the processing-time timer SHALL stay pending, and no SDK SHALL expose `cancel_event_time_timer`, `cancel_processing_time_timer` or a spelling of either

### Requirement: A fired timer SHALL invoke `on_timer` with the scheduled timestamp

`Tasklet` SHALL gain an `on_timer` hook receiving one firing record, which carries the
timer's `kind` and its `scheduled_at`, and the same `Ctx` as `run`: `on_timer(firing, ctx)`, and
`on_timer(firing, state, ctx)` in Elixir. The record SHALL be `TimerFiring` in Rust, Java, PHP, Node, Go
and Elixir (`Flezha.SDK.TimerFiring`) and a mapping with `kind` and `scheduled_at` keys in Python
and Ruby; no SDK SHALL pass the two as separate arguments. The hook receives the `Ctx` `run` receives, so it MAY apply changes, emit, and
register further timers. The record's `scheduled_at` SHALL be the timestamp the timer was **scheduled
at**, never the wall-clock time at which it actually fired. The default
implementation SHALL panic with a message naming the Tasklet, because registering
a timer without handling it is an authoring error not detectable at build time; in
the Elixir SDK, whose Tasklet is a behaviour with optional callbacks, a firing on a
Tasklet that exports no `on_timer/3` SHALL park non-retryably with a message naming
the Tasklet and the callback — the end state the panic reaches through the retry
ladder, without the retries.

#### Scenario: A late-firing processing-time timer reports its scheduled time

- **WHEN** a timer scheduled for 31000 fires at wall-clock 31480 because the Cell was busy
- **THEN** `on_timer` SHALL receive a firing whose `scheduled_at` is 31000

#### Scenario: on_timer may emit and mutate state

- **WHEN** an `on_timer` body applies a Change and emits a value
- **THEN** the Change SHALL be journaled and the value SHALL be emitted exactly as from `run`

#### Scenario: on_timer may register a further timer

- **WHEN** an `on_timer` body calls `timer_after(...)`
- **THEN** the new timer SHALL become pending for that Cell

#### Scenario: An unhandled timer names the Tasklet

- **WHEN** a Tasklet registers a timer and does not override `on_timer`
- **THEN** firing SHALL panic with a message naming the Tasklet and directing the author to implement `on_timer`

#### Scenario: An Elixir Tasklet without on_timer/3 parks the firing by name
- **WHEN** an Elixir Tasklet registers a timer and exports no `on_timer/3`
- **THEN** the firing parks on its first attempt with a message naming the Tasklet and `on_timer/3`, the fired timer's removal is committed, and the Cell is held until the park's ack exactly as for a poison input

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
mailbox. No per-Relay timer thread SHALL exist. In the Elixir SDK, which is not a poll
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

- **WHEN** a Relay restarts and a Cell's restored pending set contains a processing-time timer whose scheduled time has already passed
- **THEN** it SHALL fire immediately after restore, in ascending scheduled order relative to other due timers

#### Scenario: A Relay that uses timers costs no thread

- **WHEN** a Relay registers processing-time timers across many Cells
- **THEN** no thread SHALL be created on its behalf; its due firings SHALL be
  promoted by whichever thread next enters or wakes inside `poll`, and
  `fired_at` SHALL remain the scheduled timestamp regardless of promotion delay

#### Scenario: Instance scoping survives the shared list

- **WHEN** two Relay instances each arm a timer with the same tasklet id,
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
`ctx.spawn_steps`, which is skipped on replay because it is an external effect.

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

Each Tasklet SHALL carry a `max_timers_per_cell` bound, declared in every SDK (default 10 000) and
carried in the class declaration the core receives. For the SDKs hosted on `flz_core` the core SHALL judge
an item's timer ops once, when the item settles and before its verdict is acted on, through its
one timer session, against the Cell's pending set without copying it: registrations that would
leave more than the bound pending SHALL make the item a non-retryable item failure with
`runtime.timer_cap_exceeded` naming the Tasklet and the bound, settled as any non-retryable item
failure is (no output shipped, no registration committed, parked or failing its stage), with the
pending set unchanged, and the host's state for the Cell SHALL equal its durable state afterwards.
No hosted SDK SHALL check the bound at the registering call, and an item that registers no timer
SHALL pay nothing for the check. The typed Rust SDK, which runs the core's session in process,
SHALL refuse at the registering call instead. The items of one activation SHALL be judged in
order, each against the set the items before it committed. Pending, set, fired, cancelled and refused
counts SHALL be exposed as per-Tasklet counters, and the heartbeat's `timers_fired` SHALL count
every committed firing, in every SDK.

#### Scenario: Registration past the cap is refused

- **WHEN** a Cell holding `max_timers_per_cell` pending timers registers one more, in any SDK hosted on `flz_core`
- **THEN** the item SHALL fail with `runtime.timer_cap_exceeded` naming the Tasklet and the bound, none of its outputs SHALL ship, and the pending set SHALL be unchanged

#### Scenario: A refused item leaves the host and the record in agreement

- **WHEN** an item that records a state change and then registers a timer past the cap is refused, in any SDK hosted on `flz_core`
- **THEN** the Cell's live state read SHALL equal the state a restore from the Cell's records folds

#### Scenario: Rust refuses at the call

- **WHEN** a Rust tasklet registers a timer past the cap
- **THEN** `timer_after` SHALL return the refusal and the tasklet MAY handle it

#### Scenario: One batch's items see each other's timers in order

- **WHEN** two items of one activation for a Cell one below the cap each register a new timer
- **THEN** the first SHALL commit and the second SHALL be refused

#### Scenario: Occupancy is observable

- **WHEN** timers are pending across a Relay's Cells
- **THEN** a per-Tasklet pending-timer count SHALL be reported alongside the existing per-Tasklet counters

#### Scenario: Firings move the counter on every SDK

- **WHEN** a processing-time timer fires and its item settles on a Python, Ruby, PHP, Node, Java, Elixir, Go or Rust runner
- **THEN** the runner's heartbeat `timers_fired` SHALL rise by one and the Tasklet's fired counter SHALL rise by one

### Requirement: A Cell timer belongs to a tasklet of either kind

Every requirement of this capability SHALL hold for a tasklet of a steps relay as it holds for a tasklet of a flow relay, in every SDK: `timer_after` / `timerAfter` and, where the tasklet declares an event-time clock, `timer_at_event_time` / `timerAtEventTime` SHALL register a per-Cell timer and answer the absolute timestamp that is its cancel handle; pending timers SHALL be a set per Cell, folded from the Cell's own records, re-stated by compaction above the snapshot seq and re-armed on restore; a firing SHALL run the tasklet's `on_timer` hook on that Cell with `fired_at` equal to `ctx.now()`; processing-time firings SHALL ride the poll core's deferred-wake list (the BEAM's timer service in the Elixir SDK) and event-time firings SHALL be generated ahead of the input that advanced the Cell's high-water. No SDK's context SHALL refuse a timer call because the tasklet runs inside a steps relay, and the shared core SHALL commit a steps cell's timer ops as it commits a flow cell's. The `compensate` hook SHALL keep no timer session in either kind. A `wake_after` / `wakeAfter` class setting SHALL NOT exist in any SDK.

#### Scenario: A steps tasklet arms a processing-time timer and its firing reaches on_timer

- **WHEN** a tasklet of a `per_input` steps relay calls `timer_after(300 ms)` while handling a message, in any SDK
- **THEN** the call SHALL answer the absolute timestamp, no further message SHALL be needed, and about 300 ms later `on_timer` SHALL run on that Cell with `fired_at` equal to that timestamp and with the state the message left

#### Scenario: A steps tasklet with an event-time clock arms an event-time timer

- **WHEN** an entry-fed tasklet of a steps relay declaring an event-time clock registers `timer_at_event_time(t + 100)` on a message with event time `t`, and a message with event time `t + 150` follows
- **THEN** `on_timer` SHALL run for `t + 100` before the second message is dispatched to that Cell, on every runtime

#### Scenario: A steps tasklet's timer is folded, compacted and restored like a flow relay's

- **WHEN** a steps tasklet registers a processing-time timer, its Cell is compacted, and the runner is killed and restarted before the timer is due
- **THEN** the restored Cell SHALL hold the timer pending, it SHALL be re-armed, and it SHALL fire at its scheduled time

#### Scenario: The compensate hook still has no timers

- **WHEN** a steps tasklet's `compensate` hook calls `timer_after`
- **THEN** the call SHALL be refused with the SDK's `runtime.timers_unavailable` form, as before

### Requirement: A firing inside a steps relay is a trigger of the run

A timer firing on a Cell of a steps instance SHALL be handled by the instance's pass bookkeeping before it reaches the Cell, at the point the firing is put in front of the Cell: the promotion of a processing-time deadline and the assembly of an event-time firing chained behind the high-water. When no pass is running and no trigger is queued ahead, the firing SHALL open a pass whose origin is that tasklet and that Cell: the tasklet's chain (every tasklet reachable from it, with in-chain parent counts) SHALL be the pass's chain, the stage SHALL be in flight owing the firing, and the firing's emits SHALL flow down the chain and commit stage by stage as any stage output does. When a pass is running and the firing's own stage is in flight, the firing SHALL join that stage: it SHALL be counted among the stage's owed items, its `on_timer` SHALL run on that Cell inside the stage and its emits SHALL be held with the stage's. A processing-time timer that an item of a running stage registers SHALL enter its Cell's pending set and record at settle, as any timer op does, and SHALL be put on the deferred-wake list only once that stage has settled every item it owes, unless an item of the stage cancels it first, so a processing-time firing SHALL never join the stage that set it. Otherwise the firing SHALL wait in the instance's queue, in arrival order with messages, and SHALL be served at the next iteration boundary before the loop mode's own continuation, a `CHAINED` loop's carry included. An event-time firing generated ahead of an input SHALL join the stage that delivered the input. One pass SHALL run at a time. A firing SHALL never be handed to a Cell whose pass has failed or been cancelled.

#### Scenario: A firing while the instance waits opens a pass at the tasklet that set it

- **WHEN** a `per_input` steps relay `in -> tick -> tock -> out` has committed its pass and `tick`'s timer comes due while the instance waits for a message
- **THEN** a new pass SHALL open with `tick` as its root, `on_timer` SHALL run on `tick`'s Cell, what it emits SHALL reach `tock` and then `out`, `tick` and `tock` SHALL each commit as a stage, and the iteration SHALL commit

#### Scenario: A firing during a pass waits its turn

- **WHEN** `tick`'s timer comes due while a pass is running `tock` over a message
- **THEN** `on_timer` SHALL NOT run until that pass's iteration has committed, and SHALL then open the next pass ahead of any message that arrived after the firing came due

#### Scenario: A firing joins its own stage while that stage runs

- **WHEN** `tick`'s timer, registered in an earlier pass, comes due while `tick`'s Cell is still running over a message of the current pass
- **THEN** `on_timer` SHALL run on that Cell after `run` returns, inside the same pass, its emits SHALL be released with the stage's on that stage's commit, and no second pass SHALL open for it

#### Scenario: A timer a stage sets fires in a pass of its own after that stage

- **WHEN** an item of `tick`'s stage registers a processing-time timer whose due time passes while that stage still owes another item
- **THEN** the firing SHALL NOT join that stage, the stage SHALL settle its items and commit without it, and `on_timer` SHALL then run in a pass of its own whose root is `tick`

#### Scenario: A timer a stage sets and cancels never fires

- **WHEN** an item of `tick`'s stage registers a processing-time timer and an item of the same stage cancels it, its due time already past
- **THEN** no firing SHALL run for it and no pass SHALL open for it after the stage commits

#### Scenario: A chained loop's carry yields to a queued firing

- **WHEN** a `chained` loop's pass commits with a firing queued behind it
- **THEN** the firing's pass SHALL run next, and the carry SHALL feed the pass after it

### Requirement: A firing served inside a pass leaves the pending set when its stage commits

For a steps class that journals, the removal of a fired timer from its Cell's pending set SHALL become durable on the acknowledgement of the stage that served the firing, not on the settle of the attempt that ran `on_timer`: until that acknowledgement the timer SHALL stay in the Cell's live pending set, off the deferred-wake list, so a compaction re-states it and a restore re-arms it, and on the acknowledgement the removal SHALL be applied to the set and written as a Cell timer-ops record. Every other timer op of the invocation SHALL commit at settle as in a flow relay. For a class that journals nothing the removal SHALL commit at settle. On a crash-resume, a restored timer whose firing's stage the journal records as committed SHALL be dropped before re-arming; a restored timer with no such record SHALL re-arm and fire again, opening the pass afresh. A registration made by a re-run `run` or `on_timer` SHALL be registered, pending timers being a set.

#### Scenario: A crash before the root's commit fires the timer again

- **WHEN** `on_timer` on `tick` completes and the runner is killed before `tick`'s `TaskCommit` is acknowledged
- **THEN** after restore the timer SHALL be pending, SHALL fire again, and the pass SHALL run from `tick` over that firing

#### Scenario: A crash after the root's commit does not fire it again

- **WHEN** `tick`'s `TaskCommit` for a timer-fed pass is acknowledged and the runner is killed before `tock` commits
- **THEN** after restore the timer SHALL NOT be pending, `on_timer` SHALL NOT run again, and `tock` SHALL run seeded from `tick`'s journaled handoff

#### Scenario: An opted-out class commits the removal at settle

- **WHEN** a steps class with persistence off fires a timer
- **THEN** the removal SHALL be applied when `on_timer`'s attempt settles, and no stage acknowledgement SHALL be waited for

### Requirement: An event-time timer SHALL be refused in a class without an event-time clock, in every SDK

A registration of an event-time timer by a Tasklet whose clock is not event time SHALL be refused
with `runtime.event_time_timer_without_event_time_clock` in every SDK: judged by the core at
settle like the bound in the SDKs hosted on `flz_core`, the item failing non-retryably, and at the call in
the Rust SDK. Such a timer would never fire: the event-time
trigger runs only for event-time classes, and a pending timer keeps a sleeping flow relay awake.

#### Scenario: A processing-time class registers an event-time timer

- **WHEN** a Python tasklet whose class declares no event-time clock calls `timer_at_event_time`
- **THEN** the item SHALL fail with `runtime.event_time_timer_without_event_time_clock` and no timer SHALL be pending for the Cell

