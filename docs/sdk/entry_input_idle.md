# Entry-input idle signal

For operative authors who need to notice that an entry channel has gone
quiet, and for SDK implementers: the two runner-handle calls —
`last_entry_delivery` / `lastEntryDelivery` and `on_entry_idle` /
`onEntryIdle` — their semantics, their scope, and where the code lives.

**Rust and Java carry them; the other five SDKs defer `entry_idle` by name.**
The capability is two layers. The delivery *stamp* is in the shared core, so
every SDK hosted on `flz_core` — Node, Ruby, PHP and Python as well — records
it and can read it across the C ABI. The *policy* half on this page (the two
runner-handle calls, the observer, the episode latch, the tick) is per-SDK host
code that only Rust and Java have. Elixir has neither layer: its SDK is its own
core ([`elixir_sdk.md`](elixir_sdk.md)).

When input goes silent a runner does nothing visible: procedures stop being
called, output freezes, the process and its sockets stay up. Only the SDK,
which owns the IO threads, knows a frame arrived. This capability exposes that
one fact and nothing more: **the signal is the SDK's** (when did this entry
channel last receive a frame?) and **the policy is the application's** (is the
silence anomalous? a quiet overnight market is not a dead feed). The SDK ships
no default threshold and no default action — no alarm, no automatic reconnect,
no unrequested log line.

## The API

Both SDKs expose the same two calls on the runner handle
([`naming_conventions.md`](naming_conventions.md) rule 12). An `IdleObserver`
has `idle(gap)` and a default no-op `resumed(gap)`; a bare closure / lambda is
an observer whose `resumed` does nothing.

```rust
use std::time::Duration;
use flezha_runner::{Flezha, IdleObserver};

let runner = Flezha::create_runner("market")?;

let last: Option<std::time::Instant> = runner.last_entry_delivery("trades_in");

runner.on_entry_idle("trades_in", Duration::from_secs(90), |gap: Duration| {
    tracing::error!(target: "market", "trades_in silent for {gap:?}");
});

struct FeedAlarm;
impl IdleObserver for FeedAlarm {
    fn idle(&self, gap: Duration) { /* page someone */ }
    fn resumed(&self, gap: Duration) { /* clear the alarm */ }
}
runner.on_entry_idle("book_in", Duration::from_secs(30), FeedAlarm);

runner.run()?;
```

```java
FlezhaRunner runner = Flezha.createRunner("market");

Optional<Instant> last = runner.lastEntryDelivery("trades_in");

runner.onEntryIdle("trades_in", Duration.ofSeconds(90),
        gap -> LOG.error("trades_in silent for {}", gap));

runner.run();
```

## Semantics

- **Absent before the first frame.** `last_entry_delivery` reads absent until
  a frame arrives — never a start-time value — and a channel name no declared
  operative has answers the same way.
- **A frame counts before it is parsed.** The stamp is taken ahead of anything
  that can decline the frame, so a payload that fails to parse still proves
  the feed is alive; a replayed frame (crash-resume, late-input correction)
  counts for the same reason.
- **Episodes, not ticks.** `idle` fires **once** when the gap first exceeds
  the threshold, and again only after `resumed` re-arms it. The resume's gap
  is measured stamp to stamp — from the last frame before the silence to the
  newest frame the tick saw — so a burst that resumes and continues within one
  tick reports the silence plus that burst.
- **Cold start is never an episode.** An operative still waiting for its
  subscriptions has been "silent" since boot; the latch arms on the channel's
  first frame.
- **Off the delivery path.** Recording a delivery is one relaxed atomic store
  into a cell resolved once at topology build and cached on the channel
  descriptor. Evaluation runs on one background tick at `threshold / 4`,
  clamped to `[250 ms, 1 s]`: an episode is reported within one tick, and a
  fleet of observers never wakes more often than 4 Hz.
- **A misbehaving observer costs the data plane nothing.** A panic / throw is
  caught and logged (the `flezha::entry_idle` tracing target in Rust, the
  `EntryIdleWatcher` logger in Java); delivery and the tick are unaffected,
  and later episodes still reach the observer.
- **One channel name is one observed input.** The stamp is interned by the
  channel's bare name, so every class declaring an entry channel called `input`
  shares its cell and `last_entry_delivery("input")` answers for all of them at
  once. Watch a name that means one feed. Registering before the class is
  declared is safe for the same reason: the registration and the later topology
  resolve the same cell.
- **Registration is legal before and after `start()`**, and several observers
  may watch one channel. Note the asymmetry with declaring an operative:
  declaring is passive until traffic arrives, whereas the first `on_entry_idle`
  **spawns the tick thread immediately** and it runs for the process's life. An
  always-on registration is a running thread, not a dormant declaration. The Rust tick thread ends with its runner; the Java
  runner's `close()` resets the JVM-wide watcher so a second runner inherits
  nothing.

## Scope

Entry channels only — the boundary the application cannot see.
Inter-procedure and plugin-bound channels have the application at both ends,
and per-Entity quiet is what [entity timers](entity_timers.md) are for. A
workflow instance's inbox is not an entry channel: silence is a durable
workflow's steady state — it parks.

## Where the code lives

| Piece | Location |
| --- | --- |
| The delivery stamp (`EntryObservation`, `last_delivery`), one cell shared by both poll hosts | `flz_core/src/entry_input.rs` |
| The one write, `ChannelDescriptor::observe_delivery`, on the entry-delivery seam | `flz_core/src/channel.rs`; called from `OperativeDispatchHandle::dispatch` in `flz_core/src/routing.rs` |
| C ABI reads (`flz_core_entry_last_delivery_nanos`, `flz_core_monotonic_nanos`) | `flz_core_ffi/src/lib.rs`, JNI glue in `flz_core_ffi/src/jni_glue.rs` |
| Rust policy: `IdleObserver`, the episode latch, the tick; the runner methods | `sdk/rust/src/entry_idle.rs`, `sdk/rust/src/lib.rs` |
| Java policy | `flezha-sdk-api/.../IdleObserver.java`, `FlezhaRunner.java`; `flezha-sdk-runtime/.../EntryIdleWatcher.java` |

Threshold, latch and tick are per-SDK because they are host-layer policy; the
stamp lives once in the shared core so both SDKs observe one fact. Tests:
`sdk/rust/tests/entry_idle.rs`, Java `EntryIdleWatcherTest`,
`flz_core_ffi/tests/abi_scenario.rs`, and end to end against a live controller
`test/system/entry_idle_e2e_test.exs` over the `entry_idle_probe` fixture —
which the Rust and Java test apps carry and the other five defer, because the
policy half above exists in those two SDKs alone.

## See also

- [`entity_timers.md`](entity_timers.md) — per-Entity quiet, inside a Procedure.
- [`../internals/transport_liveness.md`](../internals/transport_liveness.md) — the controller-side view of which layer wedged; an operator's diagnosis, where this is the application's own signal.
- [`../plugins/websocket_subscription.md`](../plugins/websocket_subscription.md) — the plugin-side liveness watchdog that re-subscribes a stalled socket, upstream of this signal.
