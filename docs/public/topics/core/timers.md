# Timers

Some work is triggered by time rather than by input: a session that closes after 30 seconds of
quiet, an idle buffer that flushes, a join that gives up waiting, a window that closes. A
tasklet can set a **timer** on its cell, and the runtime calls the tasklet's `on_timer` when the
timer comes due, with no input to carry it.

Timers belong to a cell, so each key has its own. They are saved with the cell's state, so a
timer set before a crash or a restart still fires afterwards.

## How it works

A tasklet sets a timer from `run`, or from `on_timer` itself. There are two kinds:

- A **processing-time** timer fires when the wall clock reaches a time: `ctx.now()` plus a
  delay. This is the answer to "nothing has arrived": a session timeout, an idle flush, a join
  expiry.
- An **event-time** timer fires when the cell's event-time high-water mark (the latest event
  time of its input) reaches a time. This is for a deadline the data itself reaches, such as a
  window boundary.

**A quiet cell never fires its event-time timers.** Only an input arriving at that cell moves
its high-water mark, so a cell that receives nothing never fires one, however much wall-clock
time passes. That is why both kinds exist.

When a timer is due:

- **Event time.** After an input moves the high-water mark, every pending event-time timer at
  or below it fires in ascending order, **before** that input is dispatched: closing a window
  after the record that belongs to the next one would be wrong.
- **Processing time.** The firing is queued on the cell's own mailbox, never dropped, so
  `on_timer` never runs at the same time as `run` on the same cell. Due timers fire in
  ascending order of their scheduled time, ties broken by cell. No thread is dedicated to
  timers: in every SDK, whichever worker thread next polls serves them.

`on_timer` receives the same state and context as `run`, so a firing may record changes, emit,
and set further timers.

**`fired_at` is the scheduled time, never the wall clock.** A timer scheduled for T that fires
at T plus a little reports T, and `ctx.now()` equals it inside `on_timer`, so a chained
`timer_after` schedules from the timer that woke it, and a replay sees the same values. How late
a firing ran is not exposed.

**`timer_after(d)` counts from `ctx.now()`**, not from a fresh clock read, so a replayed
invocation sets the same absolute time the live one did. Under an event-time clock `ctx.now()`
is the input's event time: a stream that is caught up schedules the instant a wall clock would,
and a replay of history schedules a past instant, which fires at once.

## The calls

| SDK | Set (processing time) | Set (event time) | Cancel | Called when due |
| --- | --- | --- | --- | --- |
| Rust | `ctx.timer_after(d)` | `ctx.timer_at_event_time(ts)` | `ctx.cancel_processing_time_timer(ts)`, `ctx.cancel_event_time_timer(ts)` | `on_timer(fired_at, kind, ctx)` |
| Java | `ctx.timerAfter(d)` | `ctx.timerAtEventTime(ts)` | `ctx.cancelProcessingTimeTimer(ts)`, `ctx.cancelEventTimeTimer(ts)` | `onTimer(firedAt, kind, ctx)` |
| Node | `ctx.timerAfter(ms)` | `ctx.timerAtEventTime(ts)` | `ctx.cancelTimer(kind, ts)` | `onTimer` |
| Python | `ctx.timer_after(ms)` | `ctx.timer_at_event_time(ts)` | `ctx.cancel_timer(kind, ts)` | `on_timer` |
| Ruby | `ctx.timer_after(ms)` | `ctx.timer_at_event_time(ts)` | `ctx.cancel_timer(kind, ts)` | `on_timer` |
| PHP | `$ctx->timerAfter($ms)` | `$ctx->timerAtEventTime($ts)` | `$ctx->cancelTimer($kind, $ts)` | `onTimer(Timer $timer, Ctx $ctx)` |
| Elixir | `timer_after(ctx, ms)` | `timer_at_event_time(ctx, ts)` | `cancel_processing_time_timer(ctx, ts)`, `cancel_event_time_timer(ctx, ts)` | `on_timer/4` |

In the host SDKs, `kind` is `"processing_time"` or `"event_time"`. The Elixir functions are
`Flezha.SDK.Ctx`'s, imported by `use Flezha.SDK.Tasklet`.

- Setting a timer returns its **absolute** scheduled time, which is what a cancel takes.
- Pending timers are a **set**: setting the same timer twice is one timer, and cancelling one
  that is not set changes nothing.
- An event-time timer needs an event-time clock: the tasklet must declare `event_time(input)`
  (Rust), `eventTime(input)` (Java) or `event_time/1` (Elixir). Otherwise it is refused as
  `runtime.event_time_timer_without_event_time_clock`.
- A tasklet that sets a timer must handle it. Setting one is a runtime call, so a missing
  handler cannot be caught at build time: the default `on_timer` panics (Rust) or throws (Java)
  naming the tasklet; in Node, Python, Ruby and PHP a firing on a tasklet without the callback
  fails as `runtime.timer_without_on_timer`; in Elixir it parks, non-retryable, naming the
  tasklet and the callback.
- Resolution is milliseconds.

## Durability

Setting, cancelling and firing a timer are each a record in the cell's own change log, so the
pending set is rebuilt by the ordinary restore: no side file and no separate journal. A crash
before a firing leaves the timer pending; a crash after it does not fire it again; a replay
reproduces the pending set; and retention and compaction keep the pending timers, because a
compaction states the full set again.

- Under `exactly: effectively_once`, one firing writes **one** effect record carrying the
  changes `on_timer` made, the values it emitted and the timer's removal, so a crash between
  firing and the record being on disk replays the whole firing. A firing consumes no input
  offset, so the input log's watermark does not move on it.
- Without effectively-once, a firing is at-least-once, as input processing is: write `on_timer`
  to be safe to run twice, as you would `run`.
- A timer set during a replay **is** set (unlike `ctx.spawn_steps`, which a replay skips): a
  timer is internal state, and skipping it would rebuild a pending set the live run never had.

## When a firing fails

An `on_timer` that fails climbs the input channel's retry ladder and then goes to the
dead-letter queue, exactly as a failing `run` does. The timer's removal is durable either way,
so a poison timer parks once instead of firing forever.

## Timers in a steps relay

A steps tasklet's cell sets, saves and restores its timers as a flow relay's cell does. What
differs is when a firing runs, since a steps relay runs one pass at a time and commits it
step by step:

- **A firing starts a pass** at the tasklet that set the timer; `on_timer`'s emits travel down
  that tasklet's chain and commit step by step, as a message's pass does from the entry channel.
- **It joins or waits.** A firing whose tasklet's step is running in the current pass joins that
  step. Any other firing waits in the instance's queue beside messages, in arrival order, and
  opens its own pass at the next committed boundary. A processing-time timer a step sets never
  joins that step: it is armed once the step has finished its work, so one due sooner fires a
  little late, in a pass of its own. A `chained` loop's carried value waits behind a queued
  firing, so a loop that feeds itself never starves a timer.
- **The removal is durable when the step commits.** A served firing stays pending until the step
  that ran it commits. A crash before the pass's root commits fires the timer again into a new
  pass; a crash after it does not. A relay that journals nothing commits the removal with
  the firing, as a flow relay does.
- **A sleeping instance wakes at its earliest timer.** A parked instance records its earliest
  pending processing-time timer as its wake time; the controller wakes it then, and the timer
  fires into a new pass. Event-time timers need an input, and an input wakes the instance
  anyway.
- **Timers end with the instance.** When an instance completes, fails, is stopped or ends its
  `once` pass, its cells and their pending timers are removed. A `once` instance therefore never
  fires a processing-time timer one of its steps sets, since the instance ends with that pass;
  to wait on a timer, run the relay `each: per_input`.
- **An `on_timer` failure is a tasklet failure**: it climbs the retry ladder and, once retries
  are spent, the relay's failure rule applies (its rollback, for example).

## Example

In the tradedesk example's PHP runner, `confirm_fill` asks the exchange whether an order filled.
The paper exchange answers ten seconds later. A PHP runner serves every relay on one thread,
so a `sleep()` would hold up the price stream too; instead `run` keeps the order in the cell and
sets a timer, and `onTimer` reads the answer and ends the instance:

<!-- tabs-open -->

### PHP

```php
// ConfirmFill.php: asks the exchange whether the order filled, on a timer.
    public function run(mixed $input, Ctx $ctx): void
    {
        $ctx->change($input);
        $ctx->timerAfter(10_000);                            // the paper exchange answers ten seconds later
    }

    public function onTimer(Timer $timer, Ctx $ctx): void
    {
        $placed = $ctx->state['placed'];
        $quantity = $placed['order']['quantity'];
        if ($quantity > self::EXCHANGE_MAX) {
            throw new \RuntimeException(                     // tried again, then the order is undone
                'the exchange fills at most ' . self::EXCHANGE_MAX . " a time, and this is $quantity",
            );
        }
        $ctx->complete("filled {$placed['order']['symbol']} $quantity as {$placed['order_id']}");
    }
```

[Full file](https://github.com/flezha/flezha_php_sdk/blob/main/examples/tradedesk/src/ConfirmFill.php)

<!-- tabs-close -->

The timer is saved with the cell, so it still fires after a runner restart. Because the timer
fires after the relay's last step has run, `order_executor` runs `each: per_input`: a `once`
instance would end, timers and all, as soon as its last step committed.

<!-- tabs-open -->

### PHP

```php
// OrderExecutorRelay.php: per_input keeps the instance alive until the timer ends it.
#[Relay(factoryId: 'order_executor', run: ['steps' => ['each' => 'per_input', 'on_failure' => 'undo_all']])]
```

[Full file](https://github.com/flezha/flezha_php_sdk/blob/main/examples/tradedesk/src/OrderExecutorRelay.php)

<!-- tabs-close -->

The other languages' tradedesk runners wait for the exchange inside `run`; only the PHP runner
uses a timer.

## Bounds and observing

- `max_timers_per_cell()` / `maxTimersPerCell()` (default 10 000) bounds a cell's pending set.
  Setting a timer past it is refused as `runtime.timer_cap_exceeded`, naming the tasklet and
  the bound.
- Per tasklet, counters track pending, set, fired, cancelled and refused timers. Every runner
  heartbeat carries `timers_armed` (processing-time timers waiting, across the runner) and
  `timers_fired`. A `timers_fired` that stops moving while `timers_armed` climbs means firings
  are stuck, not that the relay is idle.

## Limits

- **Eviction discards a cell's timers.** A tasklet that sets timers and declares
  `cell_idle_ttl` is declaring them disposable.
- **A live move to another runner does not carry pending timers.** The move ships the state
  alone; the timers are still on disk, so a later restore rebuilds them.
- **A `compensate` hook has no timers.** Every SDK answers a timer call there with
  `runtime.timers_unavailable`.
- **In the Java, Node, Python, Ruby and PHP SDKs, a steps tasklet fed by another tasklet has no
  event time.** Their output is passed on without an event time, so such a tasklet reads wall
  time under an event-time clock and its event-time timers never come due. A tasklet fed by the
  entry channel is not affected, nor is any tasklet in Rust or Elixir.
- **A firing can run late**: during a restore backlog, or while every worker thread is inside
  user code. `fired_at` still reports the scheduled time.

## See also

- [State](state.md): the change log timers are saved in
- [Workflows](workflows.md): steps, passes and `each`
- [Durability](durability.md): effectively-once and the dead-letter queue
- [Sessions](sessions.md): a session that ends itself on a timer
- [Errors](../../errors.md): the `runtime.timer*` codes
- Tutorial: [place orders](tutorials/place_orders.html)
