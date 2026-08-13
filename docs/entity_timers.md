# Per-Entity timers

A Procedure could only ever react to an input. It could not say "and in thirty
seconds, if nothing else has happened, do this" — so there were no timeouts, no
idle flushes, no join expiry, and no windows of any kind. A session that went
quiet was never closed; a buffered join half whose partner never arrived was
held forever.

Timers close that. A Procedure registers one from `run` (or from a firing), and
the runtime calls `on_timer` when it comes due — with no input to carry it.

```rust
impl Procedure for Session {
    // …
    fn run(&mut self, input: Click, mut state: StateHandle<'_, Self>, ctx: &mut ProcedureCtx<'_>)
        -> Result<(), ProcedureError>
    {
        state.apply(SessionChange::Touched(input.at));
        ctx.timer_after(Duration::from_secs(30))?;   // push the idle deadline out
        Ok(())
    }

    fn on_timer(&mut self, fired_at: u64, _kind: TimerKind,
                mut state: StateHandle<'_, Self>, ctx: &mut ProcedureCtx<'_>)
        -> Result<(), ProcedureError>
    {
        state.apply(SessionChange::Closed(fired_at));
        ctx.emit(state.summary())
    }
}
```

```java
@Override
public void run(Click input, StateHandle<SessionState, SessionChange> state, ProcedureCtx ctx)
        throws ProcedureException {
    state.apply(SessionChange.touched(input.at()));
    ctx.timerAfter(Duration.ofSeconds(30));
}

@Override
public void onTimer(long firedAt, TimerKind kind,
        StateHandle<SessionState, SessionChange> state, ProcedureCtx ctx)
        throws ProcedureException {
    state.apply(SessionChange.closed(firedAt));
    ctx.emit(state.summary());
}
```

## The surface

| Rust | Java | Fires when |
| --- | --- | --- |
| `ctx.timer_at_event_time(ts)` | `ctx.timerAtEventTime(ts)` | the Entity's event-time high-water reaches `ts` |
| `ctx.timer_after(d)` | `ctx.timerAfter(d)` | wall-clock time reaches `ctx.now() + d` |
| `ctx.cancel_event_time_timer(ts)` | `ctx.cancelEventTimeTimer(ts)` | — |
| `ctx.cancel_processing_time_timer(ts)` | `ctx.cancelProcessingTimeTimer(ts)` | — |

Registration returns the **absolute** timestamp the timer is scheduled at; that
value is the handle a cancellation takes. Pending timers are a **set**, so
registering the same kind and timestamp twice is idempotent, and cancelling one
that was never registered changes nothing.

`Procedure::on_timer` / `Procedure.onTimer` receives the same `StateHandle` and
`ProcedureCtx` as `run`, so a firing may apply Changes, emit, and register
further timers. Its default panics (Rust) / throws (Java) naming the Procedure:
registering a timer is a runtime call, so a Procedure that registers one without
handling it cannot be caught at build time.

## `fired_at` is the SCHEDULED time, never the wall clock

A processing-time timer scheduled for T that fires at T+δ still reports T.
Passing T+δ would make a replay observe a different value and break the
determinism contract in exactly the way `SystemTime::now()` inside `run` does.

For the same reason `ctx.now()` **equals `fired_at`** inside `on_timer`, so a
chained `ctx.timer_after(..)` schedules off the timer that woke it — a periodic
tick is exactly reproducible. Wall-clock lateness is not exposed; a Procedure
that needs it is reaching outside the contract.

## A quiet Entity never fires its EVENT-TIME timers

This is the property to internalise before choosing a kind.

An event-time timer fires when the Entity's event-time high-water crosses it,
and the only thing that advances that high-water is an input arriving at **that
Entity**. An Entity nothing arrives at therefore never fires one, however much
wall-clock time passes. That is the same behaviour Flink has with an idle
source, and it is why both kinds exist: **the processing-time timer is the
answer to "nothing has arrived"**, and it is the one a session timeout, an idle
flush or a join expiry wants.

Use `timer_at_event_time` when the deadline is a property of the data — closing
a tumbling window at the boundary the stream itself reaches. Use `timer_after`
when the deadline is a property of the wall clock — anything whose whole point is
that no more data came.

`timer_at_event_time` is refused unless the Procedure declares
`type Clock = EventTimeFrom<_>` (Rust) / `clockType() == EventTimeFrom.class`
(Java): without an event-time clock there is no high-water to fire against.

### The anchor of `timer_after`

`timer_after(d)` schedules at `ctx.now() + d`, not at an ambient read — that is
what makes a replayed invocation register the same absolute timestamp the live
one did. Under an `EventTimeFrom` clock the anchor is therefore the input's
event time: a stream that is caught up schedules the same instant a wall clock
would, and one replaying history schedules a past instant, which fires at once.

## How each kind is triggered

**Event time** — after an input advances the Entity's event-time high-water,
every pending event-time timer at or below it fires in ascending order,
**before** that input is dispatched. Closing a window after the record that
belongs to the next one would be wrong, so the ordering is part of the contract.

**Processing time** — an entry on the poll core's shared **deferred-wake list**,
keyed `(scheduled_at, entity identity)`. On expiry it pushes a timer item onto
the target Entity's existing **mailbox**, so `on_timer` can never run
concurrently with `run` on the same Entity: the single-active-drainer gate that
already serializes inputs serializes firings too, with no second lock. Due
entries fire in ascending scheduled order, ties broken by Entity identity.

There is **no timer thread**. The list is serviced by whichever thread next
enters or wakes inside `PollCore::poll` — the same pollers that hand out
activations — and the nearest entry shortens their wait. An Operative that
registers timers therefore costs no thread at all, however many Entities it
spreads them across. The cost is that a firing whose deadline passes while
every worker is inside user code is promoted when the first one re-polls; the
scheduled timestamp it reports is unaffected (see Limits).

## Durability: timers are folded state

A registration, a cancellation and a firing are each a durable record in the
Entity's own change log, so the pending set is rebuilt by the ordinary fold on
restore. There is no timer side-file, no timer journal entry type, no crash
marker and no completeness oracle.

Consequences, all of which fall out rather than being built:

- **A crash before firing leaves the timer pending.** It fires when its
  condition is next met.
- **A crash after firing does not re-fire.** The removal folded in.
- **A replay reproduces the pending set**, because the same inputs register the
  same timers.
- **Retention and compaction already cover it** — it is in the same WAL. A
  compaction re-states the full pending set at a seq ABOVE the snapshot's, so
  the truncation the snapshot authorizes cannot take the timers with it, and a
  crash between the two leaves the untruncated log, which still holds them.

The ops ride the wire two ways, both of which the controller already stores
verbatim — it never decodes them, exactly as it never decodes the change bytes
inside an effect record:

- outside the effectively-once tier, as a `format_id = 3` record under the
  ordinary `entity_state_change` payload-id, whose body is a postcard
  `Vec<TimerOp>`;
- inside the tier, as `EffectRecordBody.timers`.

### A firing is atomic with its effects under the tier

On a Procedure declaring `effectively_once()`, one firing writes ONE effect
record carrying the Changes `on_timer` applied, the values it emitted, and the
removal of the fired timer. That is what makes firing exactly-once: a crash
between "fired" and "durable" replays the whole thing, because neither the
effects nor the removal landed.

That record's `consumed_offset` is **absent** — a firing consumes no input
offset, so the input-log watermark does not advance on it. Every controller
reader already tolerates a nil offset.

**Without the tier, firing degrades to at-least-once**, exactly as input
processing does under the same conditions. `on_timer` bodies are subject to the
same idempotence advice as `run`.

### A registration made during a replay IS registered

`ctx.spawn_workflow` answers `SkippedOnReplay` because it is an external side
effect that must not happen twice. A timer is the opposite: it is internal
state, and skipping it on replay would rebuild a pending set the live run never
had. The two calls sit on the same `ProcedureCtx`, so the contrast is worth
remembering.

## Failure of a firing

An `on_timer` panic or error climbs the input channel's retry ladder and parks
to the dead-letter queue exactly as a failing `run` does. The fired timer's
removal is durable either way, so a **poison timer parks once instead of firing
forever** — the same reason a park closes a checkpoint interval.

## Bounds and observability

Each Procedure carries `max_timers_per_entity()` / `maxTimersPerEntity()`
(default 10 000). Registering past it returns an error naming the Procedure and
the bound rather than allocating: a per-Entity timer set is author-controlled
memory multiplied by a stream Entity count.

Per-Procedure counters track pending timers, registrations, firings,
cancellations and refusals. Every `data_heartbeat` frame carries `timers_armed`
(processing-time entries on the wake list, across the process) and
`timers_fired` beside the existing `dispatches` / `blocked_parks`, so a runaway
registration is visible before it is fatal and a `timers_fired` that stops
moving while `timers_armed` climbs reads as promotion wedged rather than the
Operative idle.

## Limits

- **Eviction discards an Entity's timers.** Dynamic-mode eviction is destructive
  by existing decision and a recreate cold-starts, so a Procedure that both
  registers timers and declares `entity_idle_ttl` is declaring that its timers
  are disposable. The combination is legal and documented as such.
- **A hot-swap migration does not carry pending timers.** A handover ships the
  Entity's state through the JSON migration serializer, which covers `State`
  alone; the receiving runner suppresses the WAL restore that would have folded
  the pending set. The timers are still on disk — a later restore rebuilds them
  — but the migrated Entity does not owe them until then.
- **Workflow operatives have no timers.** They have `wake_after` and an iteration
  model already; a per-Entity timer would be a second, competing wake source.
  `ProcedureCtx` in a workflow stage or a `compensate` hook carries no timer
  session, so the refusal is structural.
- **Resolution is milliseconds.** Sub-millisecond scheduling is a non-goal.
- **A firing during a restore backlog can run well after its scheduled time.**
  So can one whose deadline passes while every poll worker is inside user code,
  since promotion needs a thread in `poll`. `fired_at` still reports the
  scheduled time, so behaviour is unchanged; the lateness is observable in
  metrics, not in the callback.

## Where the code lives

- Rust: `flz_core/src/timer.rs` (the pending set, its ops, the policy, the
  session, and the `apply_ops` / `rearm_restored` arming seam; the typed
  `timer_policy_for::<S>()` wrapper sits in `sdk/rust/src/entity/timer.rs`),
  `procedure/ctx.rs` (the surface), `procedure/mod.rs` (`on_timer`),
  `flz_core/src/poll_core.rs` (both triggers: the event-time one at batch
  assembly, the processing-time one on the deferred-wake list),
  `sdk/rust/src/operative/dispatcher.rs` (`invoke_once`),
  `sdk/rust/src/entity/restore.rs` (the typed fold).
- Java: `runtime/state/{TimerOp,PendingTimers,TimerPolicy,TimerSession}.java`,
  `runtime/entity/TimerCounters.java`, `procedure/ProcedureCtx.java`,
  `procedure/Procedure.java`, `runtime/core/ActivationDispatcher.java` (the
  invocation and the `RestoreFold` that rebuilds the pending set). Arming goes
  through the shared core — the Java host has no timer thread of its own either.
- Controller: `Flezha.State.Wire` decodes the effect record's `timers` field.
  Nothing else changes — a firing reaches it as an ordinary effect record with
  a nil consumed offset.

## Tests

`sdk/rust/tests/entity_timers.rs` and Java's `EntityTimersTest` assert the same
contract; `test/system/entity_timers_e2e_test.exs` drives a session-timeout
fixture on both runtimes against a live runner, including a re-seat from disk
that proves the pending timer survived it.
