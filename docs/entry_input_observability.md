# Entry-Input Observability

When input goes silent, a Flezha runner does nothing wrong and nothing visible:
every procedure simply stops being called, output freezes, the process stays
alive, sockets stay open. On 2026-06-05 a `flezha_market` feed went quiet and
the application had no way to notice, because the only component that knows a
frame arrived is the SDK — it owns the IO threads, and application code never
sees the data stream.

This capability exposes that one fact and nothing more:

- **The signal is the SDK's.** When did this entry channel last receive a frame?
  Only the runner knows.
- **The policy is the application's.** Is the silence anomalous? A quiet
  overnight market is not a dead feed, and a bursty source is not a wedged one.
  So the SDK ships **no default threshold and no default action** — no alarm, no
  automatic reconnect, no log line you did not ask for.

## The API

Both SDKs expose the same two calls on the runner handle, in each language's
spelling (`docs/sdk_naming_conventions.md` rule 4).

### Rust

```rust
use std::time::Duration;
use flezha_runner::{Flezha, IdleObserver};

let runner = Flezha::create_runner("market")?;

// The raw signal.
let last: Option<std::time::Instant> = runner.last_entry_delivery("trades_in");

// The observer. A bare closure is an `IdleObserver` whose `resumed` does nothing.
runner.on_entry_idle("trades_in", Duration::from_secs(90), |gap: Duration| {
    tracing::error!(target: "market", "trades_in silent for {gap:?}");
});

// Or implement both halves.
struct FeedAlarm;
impl IdleObserver for FeedAlarm {
    fn idle(&self, gap: Duration) { /* page someone */ }
    fn resumed(&self, gap: Duration) { /* clear the alarm */ }
}
runner.on_entry_idle("book_in", Duration::from_secs(30), FeedAlarm);

runner.run()?;
```

### Java

```java
FlezhaRunner runner = Flezha.createRunner("market");

Optional<Instant> last = runner.lastEntryDelivery("trades_in");

runner.onEntryIdle("trades_in", Duration.ofSeconds(90),
        gap -> LOG.error("trades_in silent for {}", gap));

runner.onEntryIdle("book_in", Duration.ofSeconds(30), new IdleObserver() {
    @Override public void idle(Duration gap) { /* page someone */ }
    @Override public void resumed(Duration gap) { /* clear the alarm */ }
});

runner.run();
```

## Semantics

**Absent, not zero, before the first frame.** `last_entry_delivery` /
`lastEntryDelivery` reads absent until a frame actually arrives — never a
start-time value that would read as "we had input a moment ago". A channel name
no declared operative has answers the same way: nothing has arrived there
either.

**Episodes, not ticks.** The observer fires **once** when the gap first exceeds
the threshold, not once per tick — an alarm that repeats every second is noise.
It fires again only after a `resumed` notification re-arms it. The resume's
`gap` is measured to the newest frame the tick saw, so a burst that resumes and
continues within one tick reports the silence plus that burst; the overstatement
is bounded by the tick, at most a quarter of the threshold. The delivery path
stores one stamp, not a first-frame-after-silence — a second stamp would be a
second write on the hot path to sharpen a number nobody alerts on.

**Cold start is never an episode.** An operative that has started and is waiting
for its subscriptions to take has been "silent" since boot; that is not an
incident, and the observer is not called. The latch arms on the channel's first
frame.

**Off the delivery path.** Recording a delivery is one relaxed atomic store with
no allocation, no lock and no name lookup — the observation cell is resolved
once, when the topology is built, and cached on the channel. Evaluation runs on
one background tick per runner at `threshold / 4`, clamped to `[250 ms, 1 s]`,
so an episode is reported within 25% of the threshold and a fleet of observers
never wakes more often than 4 Hz.

**A misbehaving observer costs the data plane nothing.** A panic (Rust) or throw
(Java) is caught and logged — on the `flezha::entry_idle` target and the
`EntryIdleWatcher` logger respectively. Delivery is unaffected, the tick
survives, and later episodes still reach the observer.

**Registration is legal before and after `start()`**, and several observers may
watch one channel. Registrations belong to the runner: dropping the Rust runner
ends its tick thread, and `close()` drops the Java runner's watches so a second
runner in the same JVM does not inherit them.

**A replayed frame counts as a delivery.** The controller re-forwards input on a
crash-resume or a late-input correction, and those frames prove the feed is
alive just as live ones do.

## Scope

Entry channels only — the boundary the application cannot see. Inter-procedure
and plugin-bound channels have the application at both ends, and per-entity
idleness is what [entity timers](entity_timers.md) are for (a processing-time
timer is the answer to "this key has gone quiet"). A workflow instance's inbox
is not an entry channel: silence is a durable workflow's steady state — it
parks.

## Where it lives

The delivery stamp is in the **shared core** (`flz_core::entry_input`), written
on the one entry-delivery seam (`flz_core::fast::dispatch`). The Java SDK is a
poll host over that same core, so both SDKs observe one stamp rather than
keeping two in step; Java reads it through the C ABI
(`flz_core_entry_last_delivery_nanos` / `flz_core_monotonic_nanos`). The
threshold, the episode latch and the tick are per-SDK
(`flezha_runner::entry_idle`, `com.flezha.sdk.runtime.EntryIdleWatcher`),
because that is host-layer policy.

## Related

- [`resilience/transport_liveness.md`](resilience/transport_liveness.md) — the
  controller-side view: which *layer* wedged when a data plane goes silent. That
  is an operator's diagnosis; this is the application's own signal.
- [`entity_timers.md`](entity_timers.md) — per-entity quiet, inside a procedure.
- [`websocket_subscription_source.md`](websocket_subscription_source.md) — the
  plugin-side data-liveness watchdog that re-subscribes a stalled socket. It is
  upstream of this signal and unchanged by it.
