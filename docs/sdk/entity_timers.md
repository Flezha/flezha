# Per-Entity timers

For operative authors who need a Procedure to act on the passage of time —
session timeouts, idle flushes, join expiry, windows — and for SDK
implementers: the timer surface in Rust, Java and Elixir, which kind fires
when, how a firing is triggered without a timer thread, why timers are durable
for free, what a failed firing does, the bounds, the limits, and where the code
lives. The Elixir SDK carries it in its own core
([`elixir_sdk.md`](elixir_sdk.md#timers-and-clocks)).

A Procedure registers a timer from `run` (or from a firing), and the runtime
calls `on_timer` when it comes due, with no input to carry it:

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

| Rust | Java | Elixir | Fires when |
| --- | --- | --- | --- |
| `ctx.timer_at_event_time(ts)` | `ctx.timerAtEventTime(ts)` | `Ctx.timer_at_event_time(ctx, ts)` | the Entity's event-time high-water reaches `ts` |
| `ctx.timer_after(d)` | `ctx.timerAfter(d)` | `Ctx.timer_after(ctx, ms)` | wall-clock time reaches `ctx.now() + d` |
| `ctx.cancel_event_time_timer(ts)` | `ctx.cancelEventTimeTimer(ts)` | `Ctx.cancel_event_time_timer(ctx, ts)` | — |
| `ctx.cancel_processing_time_timer(ts)` | `ctx.cancelProcessingTimeTimer(ts)` | `Ctx.cancel_processing_time_timer(ctx, ts)` | — |

Registration returns the **absolute** scheduled timestamp, which is the handle
a cancellation takes. Pending timers are a **set**: a duplicate registration
is idempotent, and cancelling an unregistered timer changes nothing.
`on_timer` / `onTimer` receives the same `StateHandle` and `ProcedureCtx` as
`run`, so a firing may apply Changes, emit and register further timers. Its
default panics (Rust) / throws (Java) naming the Procedure — registration is
a runtime call, so an unhandled timer cannot be caught at build time; in
Elixir, where `on_timer/4` is an optional behaviour callback, a firing on a
Procedure without it parks non-retryably naming the Procedure and the callback.

**`fired_at` is the scheduled time, never the wall clock.** A timer scheduled
for T that fires at T+δ reports T, and `ctx.now()` equals `fired_at` inside
`on_timer`, so a chained `timer_after` schedules off the timer that woke it
and a replay observes the same values. Wall-clock lateness is not exposed.

**A quiet Entity never fires its event-time timers.** Only an input arriving
at that Entity advances its event-time high-water, so an Entity nothing
arrives at never fires one, however much wall-clock time passes. That is why
both kinds exist: the processing-time timer is the answer to "nothing has
arrived" — a session timeout, an idle flush, a join expiry — and the
event-time timer is for a deadline the data itself reaches, such as a window
boundary. `timer_at_event_time` is refused
(`EventTimeTimerWithoutEventTimeClock`) unless the Procedure declares
`type Clock = EventTimeFrom<_>` / `clockType() == EventTimeFrom.class`.

**`timer_after(d)` anchors on `ctx.now()`**, not an ambient read, so a
replayed invocation registers the same absolute timestamp the live one did.
Under an `EventTimeFrom` clock the anchor is the input's event time: a
caught-up stream schedules the same instant a wall clock would, and a replay
of history schedules a past instant, which fires at once.

## How each kind is triggered

**Event time** — after an input advances the high-water, every pending
event-time timer at or below it fires in ascending order **before** that
input is dispatched: closing a window after the record that belongs to the
next one would be wrong.

**Processing time** — an entry on the poll core's deferred-wake list, keyed
`(entity, scheduled_at)`. On expiry it pushes a timer item onto the Entity's
existing **mailbox** under the `DropNothing` policy, so a firing is delayed rather
than dropped, and `on_timer` can never run concurrently with `run` on the same
Entity. Due entries fire in ascending scheduled order, ties broken by Entity
identity. There is **no timer thread**: the list is serviced by whichever
thread next enters `PollCore::poll`, and the nearest entry shortens its wait
([`../architecture/runner.md`](../architecture/runner.md), "The ladder, and
the list that owns no thread"). In the Elixir SDK, which is not a poll host,
the BEAM's own timer service is that list: the Entity process arms one
`Process.send_after/3` per pending processing-time timer, and its due message
folds every timer due by then onto the Entity's own queue, ascending, so the
one drain serialises firings against `run/3`; a due message that finds no
room comes back 5 ms later.

## Durability: timers are folded state

A registration, a cancellation and a firing are each a record in the Entity's
own change log, so the pending set is rebuilt by the ordinary restore fold —
no side-file, no journal entry type, no crash marker. A crash before firing
leaves the timer pending; a crash after firing does not re-fire; a replay
reproduces the pending set; retention and compaction already cover it — a
compaction re-states the full set at a seq above the snapshot's, so the
truncation the snapshot authorizes cannot take the timers with it.

The ops ride the wire two ways, both stored verbatim by the controller:
outside the effectively-once tier as a `format_id = 3` record under the
ordinary `entity_state_change` payload-id (body: postcard `Vec<TimerOp>`);
inside the tier as `EffectRecordBody.timers`.

Under `effectively_once()` one firing writes ONE effect record carrying the
Changes `on_timer` applied, the values it emitted and the fired timer's
removal, so a crash between "fired" and "durable" replays the whole thing.
That record's `consumed_offset` is absent — a firing consumes no input
offset, so the input-log watermark does not advance on it. Without the tier,
firing is at-least-once, exactly as input processing is; `on_timer` carries
the same idempotence advice as `run`.

A registration made during a replay **is** registered — the opposite of
`ctx.spawn_workflow`, which answers `SkippedOnReplay`. A timer is internal
state; skipping it would rebuild a pending set the live run never had.

## Failure of a firing

An `on_timer` panic or error climbs the input channel's retry ladder and
parks to the dead-letter queue exactly as a failing `run` does. The removal is
durable either way, so a **poison timer parks once instead of firing forever**.

## Bounds and observability

`max_timers_per_entity()` / `maxTimersPerEntity()` (default 10 000) bounds
the pending set per Entity; registering past it returns `TimerCapExceeded`
naming the Procedure and the bound rather than allocating. Per-Procedure
`TimerCounters` track `pending`, `registered_total`, `fired_total`,
`cancelled_total` and `refused_total`, and every `data_heartbeat` frame
carries `timers_armed` (processing-time entries on the wake list,
process-wide) and `timers_fired` beside `dispatches` / `blocked_parks`: a
`timers_fired` that stops moving while `timers_armed` climbs is promotion
wedged, not an idle Operative.

## Limits

- **Eviction discards an Entity's timers.** A Procedure that registers timers
  and declares `entity_idle_ttl` is declaring them disposable.
- **A hot-swap migration does not carry pending timers.** The handover ships
  `State` alone, and a handover that carried state suppresses the WAL restore
  that would fold the pending set; the timers are still on disk, so a later
  restore rebuilds them.
- **Workflow operatives have no timers** — they have `wake_after` /
  `wakeAfter`. `ProcedureCtx` in a workflow stage or `compensate` hook carries
  no timer session, so every timer call answers `TimersUnavailable`.
- **Resolution is milliseconds.**
- **A firing can run late** — during a restore backlog, or while every poll
  worker is inside user code, since promotion needs a thread in `poll`.
  `fired_at` still reports the scheduled time.

## Where the code lives

| Piece | Rust | Java | Elixir |
| --- | --- | --- | --- |
| Pending set, `TimerOp`, `TimerPolicy`, `TimerSession`, `TimerCounters`, arming (`apply_ops` / `rearm_restored`) | `flz_core/src/timer.rs` (re-exported by `sdk/rust/src/entity/timer.rs`, which adds `timer_policy_for::<S>()`) | `flezha-sdk-runtime/.../state/{TimerOp,PendingTimers,TimerPolicy,TimerSession}.java`, `.../entity/TimerCounters.java` | `lib/flezha/sdk/timers.ex` (the set and the session); arming in `lib/flezha/sdk/entity.ex` (`arm`, `disarm`, `rearm`) |
| Both triggers: event-time due generation at batch assembly, processing-time on the deferred-wake list | `flz_core/src/poll_core/` | shared core | `entity.ex` — `admit/2` at admission, `handle_info({:timer_due, _})` off the BEAM's timer service |
| The surface and the hook | `sdk/rust/src/procedure/{ctx,mod}.rs` | `flezha-sdk-api/.../procedure/{ProcedureCtx,Procedure}.java`, `.../state/TimerKind.java` | `lib/flezha/sdk/{ctx,procedure}.ex` |
| The invocation (`invoke_once`) and the restore fold | `sdk/rust/src/operative/dispatcher.rs`, `sdk/rust/src/entity/restore.rs` | `flezha-sdk-runtime/.../core/ActivationDispatcher.java` (`RestoreFold`) | `entity.ex` (`dispatch/2`, `call/3`), `lib/flezha/sdk/record.ex` (the fold), `lib/flezha/sdk/entity/records.ex` (the record, the restatement, the slot) |
| Record formats (`FORMAT_ID_TIMER_OPS`, `EffectRecordBody.timers`) | `flz_core/src/{header,schema}.rs` | shared core | `Flezha.State.Wire.encode_timer_ops/1` — the same bytes |
| Controller: `Flezha.State.Wire` decodes the effect record's `timers` field; nothing else changes | `lib/flezha/state/wire.ex` | — | — |

Tests: `sdk/rust/tests/timer_{durability,firing,lifecycle,surface}.rs`, Java `ActivationDispatcherTest`,
Elixir `test/flezha/sdk/{timers_test,entity_timers_test}.exs`,
and `test/system/entity_timers_e2e_test.exs`, which drives a session-timeout
fixture on every runtime, including a re-seat from disk that proves the
pending timer survived it. Examples: `sdk/rust/examples/session_timeout.rs`,
Java `E12_SessionTimeout`.

## See also

- [`stream_transforms.md`](stream_transforms.md) — `key_by`, which creates the per-key Entity a timer lives in.
- [`state_persistence.md`](state_persistence.md) — the change log and restore fold the pending set rides.
- [`../architecture/kernel.md`](../architecture/kernel.md) — the effect record, compaction and the determinism contract.
