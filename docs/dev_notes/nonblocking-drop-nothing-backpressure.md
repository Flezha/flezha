# nonblocking-drop-nothing-backpressure — implementation notes

`DropNothing` stopped holding a host thread and started withholding work from
the producing Entity. The inline-serve escape hatch is deleted with it.

## The four-probe wedge table (shared with `close-overflow-silent-failures`)

| probe | inline serve | pool base/max | result |
| --- | --- | --- | --- |
| producer emit | no-op hook (FFI shape) | 1 / 1 | **0 of 20 delivered, wedged 15 s** |
| producer emit | no-op hook | 1 / 16 | 20 of 20 in 0.74 s — the supervisor rescues |
| `promote_due` | no-op hook | 1 / 16 | 1 of 3, wedged 15 s |
| `promote_due` | real hook | 1 / 16 | 3 of 3 in 0.65 s |

Rows 3–4 were already fixed by `close-overflow-silent-failures` (the wake list
is the holdback). Row 1 is this change's: `sdk/rust/tests/nonparking_backpressure.rs`
pins `FLEZHA_WORKER_POOL_SIZE=FLEZHA_WORKER_POOL_MAX=1` in its own test binary,
because the pool is a process-wide `OnceLock` and pinning the ceiling is the
whole point — with a growable pool the supervisor rescues and the probe proves
nothing. Its own binary, and its cases take a `SERIAL` mutex, because the
assertions read process-global engine state (`outstanding_count`,
`pending_push_count`).

## The park's overhead was never the cost — the held thread was

From the proposal's measurement: 3,609 park episodes over 40,000 pushes held
the producer 76 ms; at ~1.2 µs per park that is ~4.3 ms, **6% of the hold
time**. The other 94% is genuinely waiting for the consumer, which is real
backpressure and is kept. So the win is not speed, it is that no host thread is
held — which is the only thing that makes a single-threaded host (PHP's
`SyncExecutor`, a Node host on the event loop) a supported shape rather than a
guaranteed failure.

## The `AtomicBool` guard is the design, not an optimisation

Rule 2 ("a push made while the holdback is non-empty appends rather than
attempting") is evaluated on EVERY push, including the ~99% that succeed.
Measured per successful push: `try_push` alone 14.79 ns; with an `AtomicBool`
guard 14.39 ns (free, within noise); with a `Mutex<VecDeque>::is_empty()` peek
24.21 ns — **+64% on the hottest path in the system**. `push_deferred` is
therefore an atomic, and the holdback's lock is taken only once that atomic says
it must be. The flag is written ONLY while the holdback's lock is held, which is
what makes "observed set ⇒ append behind what is already held" total: a pusher
that observes it set and then acquires the lock either still sees it set (and
appends) or sees it clear (and the list is provably empty).

## The claim protocol — publish `push_gated` UNCONDITIONALLY, first

**This was the change's one real defect, and it cost the most to find.** Two
things can observe an emptied holdback: the completion path
(`release_and_rearm`) and a consumer freeing room (`BlockParker::unpark_one` →
`rearm_deferred_pushes`). Exactly one may hand the Entity's gates back.

`push_gated` is that claim. The first two drafts published it **conditionally**,
inside `if push_deferred`:

```
fn hold_or_release_pushes(slot) -> bool {
    if slot.push_deferred.load() {         // ← re-read; a drain may have cleared it
        slot.push_gated.store(true);       // ← claim published only on this branch
        drain_push_holdback(slot);
        if slot.push_deferred.load() { record wake; return true; }
    }
    if !slot.push_gated.swap(false) { return true; }   // ← "somebody else is releasing"
    false
}
```

The losing interleaving: the caller's guard reads `push_deferred == true`, a
consumer's drain empties the holdback and clears the flag, `rearm_deferred_pushes`
reads `push_gated == false` (nobody published it yet) and so does NOT release,
and then this function's own re-read of `push_deferred` is `false` — so it skips
the whole block, never publishes the claim, and `swap(false)` returns `false`,
which it reads as "somebody else is releasing". **Both sides decide the other
one has it.** `drain_active` stays true with no handout outstanding, the
holdback is empty, `pending_pushes` reads `0`, and that Entity is stopped for
the life of the process. Every later push to it queues behind a round that will
never run.

The fix is one line moved: publish `push_gated` on EVERY entry, before anything
is read. Then `swap(false) == false` can only mean "another thread took my
claim", which is what the code always assumed it meant.

Symptom to recognise, because nothing else looks like it: `drain_active=true`,
`push_deferred=false`, `push_gated=false`, holdback empty, **`outstanding=0`**.
An Entity holding a round with no handout out is a leaked gate, always.

The fallback wake (`Wake::PushRetry`, 50 ms — mirroring the entry path's
`BLOCK_PARK_TIMEOUT`) is kept anyway. It is a no-op on an already-released
producer, and it is the only thing standing between a lost wake and a permanent
hold. It does **not** rescue a leaked gate: `rearm_deferred_pushes` only
releases when `push_gated` is set, and a leak is precisely the state where it
is not.

### How it was found, and what it cost

Every unit test passed. The whole `flezha_runner` suite passed under both flag
settings. What failed was `cargo bench --bench hybrid_channel_throughput` at
`FLEZHA_MAILBOX_CAPACITY=4` with the flag on: the process sat at a frozen CPU
count with every thread in `futex_do_wait` — warm-up completed and estimated
15.6 s, then collection never finished. `ptrace_scope` blocked `gdb -p`, so the
diagnosis came from a watchdog thread in a test that dumped the slot's gates.

**A sequential assertion cannot reach this.** The regression test is
`concurrent_feeders_against_a_live_drain_never_leak_a_gate`: four feeder threads
against a capacity-1 hop, so the Entity gates and un-gates once per message and
a completion races a live drain thousands of times a second. Reverting just the
one-line publication order reproduces it in 60 s (5 986 of 24 000 arrived, then
nothing); with the fix the same 24 000 take 1.5 s.

## Gates HELD, not released — and why that decides three other questions

The deferral keeps `drain_active` and inflight held, exactly as
`retry_after_failure` does across a backoff window. That was a choice against
the alternative (release the gates, enforce rule 1 inside `try_assemble`), and
it is the one that makes three of the design's claims true for free:

- **Eviction.** `reap_sweep` skips a candidate on `!try_mark_inflight()`. A
  gated Entity holds inflight, so the reaper skips it with no new check. Under
  the release-the-gates alternative this claim would have been FALSE and an
  evicted producer would have dropped its holdback silently.
- **Restore / migration / compaction.** They run under `with_entity_exclusive`,
  which spins on `drain_active`. A gated Entity blocks them exactly as a
  retry-backoff Entity does — and exactly as a PARKED producer did, since a
  parked producer's activation was still outstanding. Same precedent, same
  behaviour, with no time limit in either.
- **`try_assemble` needs no guard at all.** Nothing can win `drain_active` while
  the Entity is gated, so rule 1 is structural. `tasks.md` 2.2 put the holdback
  drain in `try_assemble`/`assemble_batch`; that is the wrong place under
  gates-held — the drain lives in `release_and_rearm` (the completion path) and
  in `rearm_deferred_pushes` (the consumer's).

## The wait list leaked dead entries, and the mechanism made it reachable

`BlockParker::wait_one` returned early on its post-registration recheck, and
`wait_one_timeout` returned on timeout, **both leaving their entry in the
list** — a `#[test]` even asserted it (`assert_eq!(parker.waiter_count(), 1)`)
with a comment calling it harmless. It was not: `unpark_one` serves ONE waiter
per pop, so a dead entry absorbs a wake a live producer was owed, which makes
that producer's next timeout more likely, which leaves another dead entry. The
deficit only grows, and past the point where every wake is absorbed a live
producer advances one 50 ms park at a time.

Pre-existing, and harmless until this change: the gate adds a scheduling
round-trip before the producer's own round pops again, which is what makes the
entry-path feeder time out often enough to start the feedback. Both wait forms
now share one body that removes its own entry by ticket on every exit path, and
`wait_producer` de-duplicates per producer for the same reason (a producer
re-registers after every failed drain). `unpark_one` also skips a waiter whose
Entity has been dropped rather than letting it absorb the room.

`a_timed_out_waiter_removes_its_own_entry` pins it.

## The 5 ms wake cadence would have been catastrophic — the wait list is the wake

Relying on `Wake::PushRetry` alone as the re-arm (which is what `tasks.md` 2.3
describes) would cost up to 50 ms per deferral. At `FLEZHA_MAILBOX_CAPACITY=4`
and 5,000 messages that is minutes, not milliseconds. **`BlockParker` is the
re-arm**: its waiter list now holds either a parked `Unparker` (the entry-path
reader) or a `Weak<EntitySlot>` producer, and `unpark_one` — already called per
pop by `assemble_batch` — serves whichever is at the front. One place, both
mechanisms, which is what `tasks.md` 2.4 asked for.

The post-registration re-check is the same one `wait_one` makes and for the same
reason: a drain that emptied the mailbox between the failed admission and the
registration would otherwise leave nobody to wake it. A stale registration left
by a producer that drained on its own is inert — re-arming an un-gated Entity is
a no-op.

## The durability hold: the flusher gate

**This is the part the proposal's D3/D4 asserted and no task asked for.** The
proposal says "an item with a pending push is unresolved and its offset is
therefore not durable". That was not true of a deferral by itself:
`resolve_item` routes edge outputs and then folds the effect record, and the
flusher ships it ~5 ms later — so with a naive deferral the consumed offset
becomes durable while the value still sits in the holdback, and a crash loses
it. That is precisely the window `close-overflow-silent-failures` item 3 had
just closed, re-opened by this change.

`EntitySlot::push_deferred` is an `Arc<AtomicBool>` shared into the Entity's
`FlusherEntry`, and `EntityFlusher::drain_pass` skips a buffer whose flag is
set. Bounded by construction: a gated Entity is handed no further work, so at
most one item's record ever waits there. `SlotParts` carries the flag because
the flusher entry is registered BEFORE the slot exists.

This also makes `discard_queued_input_on_restore`'s holdback clear correct
(task 2.7): a non-empty holdback means the offset is not durable, so replay
re-produces exactly what is dropped. In practice that clear is unreachable — a
restore cannot win `drain_active` from a gated Entity — but it is the honest
statement of the invariant, and the flag is cleared under the holdback's lock so
a concurrent push cannot append behind a list nobody will drain.

The two `edge_output_ordering_tests` in `flz_core/src/host.rs` were rewritten
around this: they used to assert `!resolving.is_finished()` (i.e. the thread is
parked), which is the mechanism, not the guarantee. They now assert that the
Entity's `change_seq` does not advance across a forced `drain_now_for_tests()`
— "no record was shipped" — which means the same thing under BOTH flag
settings, and they drive `resolve_item` directly, which is the exact function
`flz_core_complete` calls. That is the Java coverage: the FFI completion path is
under test on both settings without a Java process.

## The bound parks — and the first draft's park REORDERED

`FLEZHA_PUSH_HOLDBACK_MAX` (default 4096). The first implementation, at the
bound, called `enqueue` (the parking form) with the item — which lands it in the
consumer's mailbox AHEAD of the producer's own held items. An ordering violation
introduced by the mechanism that exists to preserve ordering.
`wait_for_holdback_room` instead waits for THIS producer's earlier pushes to
land and re-loops, so the bound can never reorder the sequence it bounds.

The bound's park needs a pool that can GROW (D5: "the safety valve is the
supervisor, not re-entrancy"), so its test cannot live in the pinned
one-worker binary — `sdk/rust/tests/nonparking_holdback_bound.rs` is a second
test binary with the default pool. A test that pins the ceiling at 1 AND
exercises the bound wedges by design, which is how this was found.

**An FFI host cannot reach the bound in one activation**: its outputs are
materialized as bytes before the core sees them, so its holdback is bounded by
one completion's output count — memory the host already allocated. Do not look
for an FFI-side guard.

## A typed batch stops at the deferral

`run_activation` hands the untouched tail back (`Handout::hand_back` →
`core.abandon`) the moment `push_deferred` is set. Without it a wall-clock class
— which batches the whole mailbox freely — would run thousands of items into the
holdback while its first push was still held. The parked producer stopped there
too: its thread never returned into the batch.

## Reporting: an existing verdict, plus one that mints no op

`pending_pushes` / `pending_push_oldest_ms` on the data heartbeat, from
`PollCore::deferred_pushes` — a `DashMap` keyed by entity identity, the same
shape as `outstanding` and read the same way.

`runner_pool_wedged` needed **no change**, and that was checked rather than
assumed: `note_blocked` still fires on every failed admission — the initial
deferral, every failed re-drain, and every 50 ms fallback — so `blocked_parks`
keeps moving under the new mechanism at the same order of rate the 50 ms park
gave it. The verdict's backpressure exemption therefore still reads correctly.

What is genuinely lost is `inflight_oldest_ms` visibility: a PARKED producer
kept its activation outstanding, a gated one does not. Hence
`:producer_push_stalled`, ranked last and declared in `@layer_scopes` with a
**nil scope** so `Diff.resolve_wedge` mints no heal op. That is deliberate: the
Entity the count names is the one WAITING, and a rebind of the producer moves
the congestion rather than clearing it.

**The consumer's identity cannot ride the heartbeat.** `LivenessCounters` slots
are `:counters` — integers — so a string cannot be mirrored through
`stream_channel.ex`. The spec delta's original scenario ("the classifier verdict
SHALL name the consumer") was amended to what is deliverable: the classifier
reports the condition, and the RUNNER names both ends in one throttled
`flezha::mailbox` warning (`routing::report_holdback_full`, one line per 5 s
process-wide). Do not re-attempt the identity-on-the-heartbeat route without
changing what a liveness slot is.

## Cross-SDK

- **Rust**: directly. `ctx.emit` no longer parks; `poll_pool::serve_slot` and
  both `in_activation` wrappers are gone.
- **Java**: below the seam, no Java source change. `flz_core_complete` →
  `apply_result` → `resolve_item` → `deliver_host_output` is the path, and it is
  the path `edge_output_ordering_tests` drives under both settings. The five
  committed native binaries and both fat jars were rebuilt
  (`scripts/build-dist.sh --host`) because `flz_core` is compiled into them.

  **A stale artifact is silent, and it bit this change's review.** The harness
  picks the FIRST artifact path in a `conformance.json` that *exists* and never
  checks whether it is current: `test/apps/rust_runner/conformance.json` lists
  `target/release/rust_runner` first, and a copy three days old — built before
  `ed856088` added `sdk_lang` to the advertisement — advertised `0`, so every
  placement was correctly refused and `mix bench` looked like a placement bug.
  `cargo build -p rust_runner --release` fixed it. Same trap already documented
  for `java_runner.jar`. **Rebuild every committed artifact whose source you
  touched, and suspect staleness before logic when a runner is refused.** Not
  fixed here — the freshness check belongs to the harness, not to this change.
- **Elixir**: **no mechanism change, and that is a decision.**
  `Flezha.SDK.Entity.handle_call({:enqueue, …})` already defers its reply into a
  `waiters` FIFO drained by `admit_waiter/1` — it was the reference design for
  what `flz_core` now does. It DID take the reporting half, because heartbeat
  keys are a cross-SDK contract: `Counters` grew a second block of start-time
  slots (`:deferred`) behind one parameterised core (`mark_started`/
  `mark_finished`/`liveness`), so `handout_liveness/1` and
  `pending_push_liveness/1` are two callers of one scan rather than two copies.
  The proposal's Cross-SDK table said "Elixir: Nothing" — **stale**, in the same
  way `close-overflow-silent-failures`' table was.

## Stale claims found in the proposal / tasks

- `tasks.md` 3.3 names an `in_activation` call site in
  `sdk/rust/src/operative/dispatcher.rs`. There is none; both were in
  `poll_pool.rs`.
- `tasks.md` 1.4 says `EntityEmitSink` "has the producing entity's context". It
  does not hold the `Arc<EntitySlot>` — it holds `entity_label`,
  `entity_identity` and `change_seq`. The slot is threaded through
  `ProcedureDispatcher::dispatch_item`, which has exactly one caller
  (`run_activation`) and two impls.
- `tasks.md` 2.2 puts the holdback drain in `try_assemble`/`assemble_batch`.
  Wrong under gates-held (see above).
- `tasks.md` 2.3's 50 ms wake as the primary re-arm would have destroyed the
  contended benchmark. It is the fallback; the wait list is the re-arm.
- `tasks.md` 5.3 says `lib/flezha/sdk/counters.ex` + `runner_server.ex` carry
  the keys. True, but the CONTROLLER mirror is `lib/flezha/runner/stream_channel.ex`
  + `liveness_counters.ex` + `transport_source.ex`, which the task did not name
  — the same "written by the kernel and read by nothing" trap
  `close-overflow-silent-failures` hit one layer up.
- The proposal's `promote_due` probe rows and the deletion of
  `crate::routing::in_activation` from `poll_core.rs` were already done by
  `close-overflow-silent-failures`.

## Measurements

`sdk/rust/benches/hybrid_channel_throughput.rs`, 5,000 messages per iteration,
criterion defaults (15 s measurement, 10 samples), median of the three-figure
interval, this box. `cargo bench -p flezha_runner --features bench-internals
--bench hybrid_channel_throughput -- pure_inter_procedure`.

| arm | 1 consumer | 3 consumers |
| --- | --- | --- |
| default capacity (1024), flag off — the happy path | 7.36 ms | 25.22 ms |
| default capacity, flag **on** | **7.19 ms** (p = 0.46 — no change) | 22.25 ms |
| `FLEZHA_MAILBOX_CAPACITY=4`, flag off (the park) | 18.92 ms | 46.10 ms |
| `FLEZHA_MAILBOX_CAPACITY=4`, flag **on** (the holdback) | **25.78 ms** (+36%) | **59.75 ms** (+30%) |

Reading them honestly, including the row that does not flatter the change:

- **The happy path did not regress** (task 7.2). The `AtomicBool` guard costs
  nothing measurable at default capacity: p = 0.46 on the 1-consumer arm, which
  is exactly what the 14.39 ns vs 14.79 ns microbenchmark predicted.
- **Under sustained saturation the holdback is 30–36% SLOWER than the park.**
  This contradicts the proposal's expectation ("cheaper per event: 745–822 ns
  against 1090–1380 ns"). That figure was measured single-threaded and excluded
  what dominates here: the re-arm is a scheduling round-trip (consumer pop →
  wait list → drain → release gates → schedule the producer's round → a poller
  assembles it) where the park was a futex wake straight back into the emit.
  The change buys the freed thread, not throughput, and this is the price.
- This is the **contended** figure task 7.3 asked for — producer on the
  dispatch worker, consumers draining on other pool threads — which the
  single-threaded microbenchmark could not produce.
- The 3-consumer numbers are noisier on this box than the proposal's HEAD
  baseline (25.22 ms measured against its 18.02 ms); the 1-consumer arm matches
  (7.36 against 7.16). Treat the 3c column as same-order, not precise.

**The flag-on capacity-4 arm is also what found the gate leak**: before the
claim-order fix this arm never finished at all — warm-up estimated 15.6 s and
collection hung with every thread parked. A benchmark is a load test, and this
one earned its place in the task list.

### OWED FOLLOW-UP: the Java round-trip bench (task 7.4)

**Not run, and it is owed by name.** `mix bench.roundtrip --runtime java` under
both flag settings. The numbers above prove ONE runtime, and
`docs/architecture/runner.md`'s "One change, every SDK" says exactly that: a
perf number measured on one runtime is proven on one runtime. It was accepted
as unticked at the checkpoint only because the flag ships **off**, so it gates
the future flip and nothing that ships now — which is why it is the second of
the three flip gates below, not a loose end.

Java is reached below the seam and needs no Java source change, so what the
bench would measure is `flz_core`'s mechanism through the JNI completion path
(`flz_core_complete` → `apply_result` → `resolve_item` → `deliver_host_output`)
— the same path `flz_core::host::edge_output_ordering_tests` already drives
under both settings for CORRECTNESS. Correctness is covered; throughput is not.

## The flag's exit criteria — named, not left to drift

`FLEZHA_DROP_NOTHING_NONPARKING` ships default **off** in this change.

1. **Flip the default to on** when THREE things hold, and not before:
   - one soak (≥ 2 h, the `feedback_soak_*` bar: 100% data integrity) with
     `FLEZHA_DROP_NOTHING_NONPARKING=1` against a workload that actually
     saturates a consumer mailbox;
   - the Java round-trip bench run under both settings (owed, task 7.4);
   - **the 30–36% saturated-throughput cost measured above is either closed or
     accepted in writing.** It is the one number that argues against flipping,
     and flipping without answering it would bury it. The likely place to close
     it is the re-arm's scheduling round-trip: a consumer's drain already holds
     the producer's held items and could admit them without a full
     release-schedule-assemble cycle.
2. **Delete the flag** in the change that flips it, or the next one, by name.
   Deleting it means deleting `ENV_NONPARKING`, `NONPARKING`,
   `nonparking_enabled`, `set_nonparking_for_tests`, and the `enqueue` fallback
   arm at the head of `admit_from_entity` — after which `enqueue` has exactly
   one caller left, the entry path, and can be renamed to say so.

Until then, with the flag OFF the behaviour is "park, minus the inline serve" —
i.e. what `close-overflow-silent-failures` left Java with, for every SDK. That
is a real change of default even with the flag off, and the supervisor is what
carries it (probe row 2).

## Test map

| What | Where |
| --- | --- |
| a chain of full mailboxes drains on a pinned one-worker host, cap never breached | `sdk/rust/tests/nonparking_backpressure.rs` |
| per-producer emit order survives every deferral | same |
| a gated producer holds its round (the reaper's skip) and is reported | same |
| emitting past the bound delivers everything, in order, without deadlock | `sdk/rust/tests/nonparking_holdback_bound.rs` |
| **concurrent feeders against a live drain never leak a gate** | same — the ONE test that catches the claim-order defect |
| a parked waiter leaves no dead entry behind (both wait forms) | `flz_core/src/entity.rs` `tests` |
| no record ships while an edge push is undelivered (single + fan-out), both flag settings | `flz_core/src/host.rs` `edge_output_ordering_tests` |
| the heartbeat carries the deferred-push keys | `flz_core/src/fast.rs` `heartbeat_tests` |
| the whole overflow set under both flag settings | `cargo test -p flezha_runner --features "bench-internals testing"` with and without `FLEZHA_DROP_NOTHING_NONPARKING=1` |

## The system tier: characterised, NOT baselined

`mix test --include system` came back 1838/1847 with 9 failures and 4 invalid
(the 4 are `RemoteDurabilityE2ETest`'s `setup_all` failing on a missing env
var). Re-running the four files closest to this change gave 5/6, and the one
that failed was a DIFFERENT test than in the full run; `dynamic_entity_e2e_test.exs`
alone gives pass / fail / pass with the failing test differing again.

The structural argument for pre-existing, which is the load-bearing half:
`nonparking_enabled()` is consulted at exactly ONE site
(`routing.rs`, short-circuiting `|| !nonparking_enabled()` in
`admit_from_entity`), so **with the flag off every behavioural path this change
adds is unreachable** — `push_deferred` is never set, so the holdback, the gate
and the flusher skip are all inert. The one always-live change is the parker's
entry cleanup, and it REMOVES a dead entry rather than adding one.

**Not baselined against HEAD**, because checking out `d2d0b4a5` would have
touched the working tree. Recorded as characterised, not proven. If someone
needs certainty, that is the one run to do.

### The conformance gate, per runtime

| runtime | invocation | result |
| --- | --- | --- |
| static gate | `test/sdk_conformance/gate_test.exs`, untagged | **green** — rides plain `mix test` (1771 passed) |
| rust (reference) | `mix test --include system` | 1838/1847, **9 failed**, 4 invalid |
| java | `FLEZHA_E2E_RUNTIME=java mix test --include system` | 1841/1847, **6 failed**, 4 invalid |
| elixir | `FLEZHA_E2E_RUNTIME=elixir mix test --include system` | **NOT COMPLETED** — cut off by the run's time budget |

The 4 invalid are the same on both: `RemoteDurabilityE2ETest`'s `setup_all`
failing on a missing env var. **Different failure COUNTS on the same tree with
the same flag setting** (9 vs 6), overlapping on
`DynamicEntityE2ETest`'s "lazy-created Entity's state is shipped, then reaped +
deleted controller-side" — which is the same instability the per-file re-runs
showed. A deterministic break does not change its count between runtimes.

The Elixir runtime's tier is **owed**, and it is the cheapest of the three to
finish: `FLEZHA_E2E_RUNTIME=elixir mix test --include system` on a quiet box.
