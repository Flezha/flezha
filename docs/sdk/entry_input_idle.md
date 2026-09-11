# Entry-input idle signal

For operative authors who need to notice that an entry channel has gone
quiet, and for SDK implementers: the two runner-handle calls —
`last_entry_delivery` / `lastEntryDelivery` and `on_entry_idle` /
`onEntryIdle` — their semantics, their scope, and where the code lives.

**Every SDK carries them.** The capability is two layers. The delivery *stamp*
and the *episode latch* live once in the shared core, so every SDK hosted on
`flz_core` — Rust, Java, Node, Ruby, PHP and Python — records one fact and
runs one state machine over it, reached by the Rust SDK directly and by the
veneers over the C ABI by watch id. The *policy* half on this page — the two
runner-handle calls, the observer, the tick that advances the latch and the
contained delivery — is per-SDK host code, because a tick and a callback are
things only the host can place. The Elixir SDK is its own core and
re-expresses both layers in Elixir ([`elixir_sdk.md`](elixir_sdk.md)).

When input goes silent a runner does nothing visible: procedures stop being
called, output freezes, the process and its sockets stay up. Only the SDK,
which owns the IO threads, knows a frame arrived. This capability exposes that
one fact and nothing more: **the signal is the SDK's** (when did this entry
channel last receive a frame?) and **the policy is the application's** (is the
silence anomalous? a quiet overnight market is not a dead feed). The SDK ships
no default threshold and no default action — no alarm, no automatic reconnect,
no unrequested log line.

## The API

Every SDK exposes the same two calls on the runner handle
([`naming_conventions.md`](naming_conventions.md) rule 12). An `IdleObserver`
has `idle(gap)` and a default no-op `resumed(gap)`; a bare closure / lambda /
fun is an observer whose `resumed` does nothing. Durations are the SDK's own
spelling: `Duration` where the language has one, milliseconds where the SDK's
timers already speak milliseconds.

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

| SDK | accessor | registration | observer | duration |
| --- | --- | --- | --- | --- |
| Rust | `last_entry_delivery(channel) -> Option<Instant>` | `on_entry_idle(channel, threshold, observer)` | `IdleObserver` trait, or a closure | `Duration` |
| Java | `lastEntryDelivery(channel) -> Optional<Instant>` | `onEntryIdle(channel, threshold, observer)` | `IdleObserver` (functional), or a lambda | `Duration` |
| Node | `lastEntryDelivery(channel): Date \| null` | `onEntryIdle(channel, thresholdMs, observer)` | `IdleObserver { idle; resumed? }`, or a function | ms |
| Ruby | `last_entry_delivery(channel) -> Time \| nil` | `on_entry_idle(channel, threshold_ms, observer = nil, &block)` | an object answering `idle` (and `resumed`), or the block | ms |
| PHP | `lastEntryDelivery(string): ?DateTimeImmutable` | `onEntryIdle(string, int $thresholdMs, IdleObserver\|callable)` | `IdleObserver` interface, or a callable | ms |
| Python | `last_entry_delivery(channel) -> datetime \| None` | `on_entry_idle(channel, threshold_ms, observer)` | `IdleObserver` subclass, or a callable | ms |
| Elixir | `last_entry_delivery(handle, channel) :: DateTime.t() \| nil` | `on_entry_idle(handle, channel, threshold_ms, observer)` | a module implementing `Flezha.SDK.IdleObserver`, or a fun/1 | ms |

The accessor's instant is the host's wall-clock type, derived from the
monotonic gap — `wall_now − (now − last)` — so a clock step between the
delivery and the read cannot skew it. Both calls are legal before the runner
is started: the reads and the watch calls take no context.

## Semantics

- **Absent before the first frame.** `last_entry_delivery` reads absent until
  a frame arrives — never a start-time value — and a channel name no declared
  operative has answers the same way; a read never interns a name.
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
  descriptor. Evaluation runs on one tick at `threshold / 4`, clamped to
  `[250 ms, 1 s]`: an episode is reported within one tick, and a fleet of
  observers never wakes more often than 4 Hz.
- **Where the tick runs is the host's, and that bounds the tick, never the
  count.** The latch is the contract: an episode is reported once and a resume
  once, because it compares stamps rather than clock readings. "Within one
  tick" holds when the ticking thread is free; on a host where the observer
  shares its thread with handler bodies or with the interpreter lock, a body
  that runs past the tick delays the report by its length.

  | host | the tick | exact when |
  | --- | --- | --- |
  | Rust, Java | a dedicated thread | always |
  | Node | a timer on the main event loop, `unref`ed | `workers(n > 0)`; under `workers(0)` a synchronous body holds the loop |
  | Ruby | one thread woken by `Queue#pop(timeout:)` | the GVL is free; a pure-Ruby body delays it |
  | Python | one daemon thread woken by `Event.wait` | the GIL is free; a pure-Python body delays it |
  | PHP | between polls of the thread that started the runner — `SyncExecutor`'s loop, `SwooleExecutor`'s supervising thread | `swoole`: within one starvation wait; `sync`: a body delays it |
  | Elixir | a timer message in one SDK process | always — the BEAM preempts |

- **A misbehaving observer costs the data plane nothing.** A panic, throw,
  raise or exit is caught and logged (the `flezha::entry_idle` tracing target
  in Rust, the `EntryIdleWatcher` logger in Java, each veneer's SDK logger,
  `Logger.warning` in Elixir); delivery and the tick are unaffected, and later
  episodes still reach the observer.
- **One channel name is one observed input.** The stamp is interned by the
  channel's bare name, so every class declaring an entry channel called `input`
  shares its cell and `last_entry_delivery("input")` answers for all of them at
  once. Watch a name that means one feed. Registering before the class is
  declared is safe for the same reason: the registration and the later topology
  resolve the same cell.
- **The observer runs where it was registered.** On a host whose dispatch runs
  in isolates that share no memory with that thread — Node under
  `FLEZHA_NODE_WORKERS`, PHP under `SwooleExecutor` — what an observer records
  reaches a procedure only through a channel the application builds (a
  `BroadcastChannel`, a file). Under Swoole the entry script re-runs in every
  worker interpreter, and a worker's handle registers nothing: only the thread
  that started the runner owns its observers, as it owns its signals.
- **Registration is legal before and after `start()`**, and several observers
  may watch one channel. Note the asymmetry with declaring an operative:
  declaring is passive until traffic arrives, whereas the first `on_entry_idle`
  **starts the tick** (a thread, a timer or a process) and it runs for the
  runner's life. An always-on registration is a running tick, not a dormant
  declaration. Each host's stop / drain ends the tick and frees its watches;
  the Java runner's `close()` resets the JVM-wide watcher so a second runner
  inherits nothing.

## Scope

Entry channels only — the boundary the application cannot see.
Inter-procedure and plugin-bound channels have the application at both ends,
and per-Entity quiet is what [entity timers](entity_timers.md) are for. A
workflow instance's inbox is not an entry channel: silence is a durable
workflow's steady state — it parks.

## Where the code lives

| Piece | Location |
| --- | --- |
| The delivery stamp (`EntryObservation`, `last_delivery`), one cell per name shared by every `flz_core` host | `flz_core/src/entry_input.rs` |
| The episode latch (`IdleWatch::evaluate`), one state machine for every `flz_core` host | `flz_core/src/entry_input.rs` |
| The one write, `ChannelDescriptor::observe_delivery`, on the entry-delivery seam | `flz_core/src/channel.rs`; called from `OperativeDispatchHandle::dispatch` in `flz_core/src/routing.rs` |
| C ABI reads and the watch calls (`flz_core_monotonic_nanos`, `flz_core_entry_last_delivery_nanos`, `flz_core_idle_watch_new` / `_poll` / `_free`) | `flz_core_ffi/src/lib.rs`, typed in `veneer/mod.rs`, bound in `napi_glue.rs`, `ruby_glue.rs`, `python_glue.rs`, `sdk/php/src/Ffi/Binding.php` |
| Rust policy: `IdleObserver`, the tick thread, the runner methods | `sdk/rust/src/entry_idle.rs`, `sdk/rust/src/runner_handle.rs` |
| Java policy (its own latch copy, owed to `move-java-entry-idle-latch-onto-the-core`) | `flezha-sdk-api/.../IdleObserver.java`, `FlezhaRunner.java`; `flezha-sdk-runtime/.../EntryIdleWatcher.java` |
| Node policy | `sdk/node/src/entry-idle.ts`, `runner.ts` |
| Ruby policy | `sdk/ruby/lib/flezha/entry_idle.rb`, `runner.rb`; `Core.context_free` in `native.rb` |
| PHP policy | `sdk/php/src/EntryIdle.php`, `IdleObserver.php`, `EntryIdleBridge.php`, `Runner.php`; `Core::contextFree()` |
| Python policy | `sdk/python/src/flezha/entry_idle.py`, `runner.py` |
| Elixir, both layers | `lib/flezha/sdk/entry_input.ex` (the cell, the anchored clock), `entry_idle.ex` (the process, `evaluate/2`), `idle_observer.ex`; the calls in `lib/flezha/sdk.ex` |

Threshold, tick and delivery are per-SDK because they are host-layer policy;
the stamp and the latch live once in the shared core so six SDKs observe one
fact through one state machine. Tests: `flz_core/src/entry_input.rs` (the
latch), `flz_core_ffi/tests/abi/lifecycle.rs` (the ABI, with a zero threshold
and a bounded spin), `sdk/rust/tests/entry_idle.rs`, Java
`EntryIdleWatcherTest`, `sdk/node/test/entry-idle.test.mjs`,
`sdk/ruby/test/entry_idle_test.rb`, `sdk/php/tests/EntryIdleTest.php`,
`sdk/python/tests/test_entry_idle.py`, `test/flezha/sdk/entry_idle_test.exs`,
and end to end against a live controller
`test/system/entry_idle_e2e_test.exs` over the `entry_idle_probe` fixture,
which every test app carries. `entry_idle` is in the conformance suite's
required set: no manifest may defer it.

## See also

- [`entity_timers.md`](entity_timers.md) — per-Entity quiet, inside a Procedure.
- [`../internals/transport_liveness.md`](../internals/transport_liveness.md) — the controller-side view of which layer wedged; an operator's diagnosis, where this is the application's own signal.
- [`../plugins/websocket_subscription.md`](../plugins/websocket_subscription.md) — the plugin-side liveness watchdog that re-subscribes a stalled socket, upstream of this signal.
