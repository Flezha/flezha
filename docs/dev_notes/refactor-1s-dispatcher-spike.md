# Dispatcher-seam A/B spike (`refactor-1s-dispatcher-spike`) — implementation notes

The measurement spike behind charter §3's "one host core" paragraph: does
putting the typed Rust dispatcher and the erased host on one effect buffer
and one settlement tail cost throughput? Verdict **converge** — every row
inside its floor, allocations flat. Nothing merged: the seam lives on branch
`spike/1s-dispatcher` (`3b61380d` = arm A, the benches; `f480f89e` = arm B,
the seam) off tag `refactor-baseline-1s`, and area 1 (`refactor-1-kernel`)
re-implements it under the charter rather than merging it. The table is in
the change's `design.md`; what is here is what area 1 must know to write it
again.

## The seam's real shape — a codec marker, not a trait on the change

**`ChangeBody` on `C` (design D2) cannot be written.** `S::Change` is the
author's type, so `sdk/rust` can only give it a core trait through a blanket
impl over a trait local to the crate that defines `ChangeBody` — which means
moving `Change` into `flz_core`. That turns every downstream
`impl Change for <std type>` into an orphan-rule error (E0117): the testing
harness does exactly that (`sdk/rust/src/testing/tests/harness_smoke.rs:28`,
`impl Change for u32 {}`), and so may any user with `type Change = i64`. Tried
on the branch and reverted. `Change` and `CoalesceKey` stay in
`sdk/rust/src/procedure/change.rs`.

**The shape that works is a codec marker per class kind:**
`flz_core::flusher::Bodies { type Change: Send + 'static; fn coalesce(Vec<Change>)
-> Vec<Change>; fn into_bodies(Vec<Change>) -> Vec<Vec<u8>> }`. `RawBodies`
(core, `Change = Vec<u8>`, both fns the identity) and `TypedBodies<S>`
(`sdk/rust/src/entity/flusher.rs`, `PhantomData<fn() -> S>`, `Change =
S::Change`, `coalesce` by key then postcard on the flusher thread). A local
type implementing a foreign trait is orphan-safe, the marker is zero-sized,
and no authoring surface moves. `EffectBuffer<B: Bodies>` carries `changes:
Vec<B::Change>`, `staged: StagedInterval<B::Change>`, `effects:
Vec<EffectEntry<B::Change>>`, `timer_ops`, `pending_attempt`; ONE
`FlushableBuffer` impl; `BytesBuffer = EffectBuffer<RawBodies>` and
`TypedBuffer<S> = EffectBuffer<TypedBodies<S>>` are type aliases, so every
`downcast_mut::<BytesBuffer>()` / `::<TypedBuffer<S>>()` site compiles
unchanged. `EffectEntry<C>` is generic (it replaces the SDK's
`EffectEntry<S>` and the host's direct `EffectRecordBody` construction) and
becomes the wire body only at drain, `into_body::<B>()`; `EffectRecordBody`
stays the postcard fixture the controller decodes and loses `from_shipped`.
`EffectBuffer::close_interval` replaces the two identical `close_interval`
bodies in `HostDispatcher` and `ProcedureDispatcherImpl`.

**`settle_effects<B>(buffer, SettlePolicy { effectively_once, checkpoint,
buffered }, InvocationEffects<B::Change>, dlq) -> Option<DlqEntryBody>`** owns
exactly the fold-or-extend: it re-takes `pending_attempt` first (a move when
it is empty, never a re-collect), folds through `StagedInterval::fold` under
the tier and pushes `EffectEntry::from_shipped(record, dlq)`, or outside the
tier keeps `changes` (assignment when the buffer is empty — an `extend` into an
empty `Vec` is an allocation the typed path never paid) and `timer_ops`, and
hands the park BACK for the caller's blocking `park_tier_off` once the buffer
lock is released. `buffered` is `true` for the typed path (a `NoChange` class
inline-flushes its timer ops afterwards, as before) and
`slot.flusher_entry_id.is_some()` for the host path (a non-persistent host
class keeps nothing, as before). What stays with the callers, per D3: the
typed `EntityEmitSink` and its in-process edge delivery, the host's
`deliver_host_output` BEFORE the fold, plugin-frame shipping, the timer apply
and `arm_interval_deadline` (the typed path applies ops in `run_activation`
outside the slot locks; moving that into the seam would change what is held
while a due timer re-enters the mailbox).

## The bytes buffer must never reach `coalesce`

Design risk 2 says `coalesce` "early-returns" when `coalesce_key` answers
`None`. It does not: `coalesce` allocates its `HashMap::with_capacity(n)` and
`keep_mask` on ANY non-empty buffer and rebuilds the output `Vec`. The erased
path avoids all three by construction — `RawBodies::coalesce` and
`into_bodies` return their argument — and
`flusher::effect_buffer_tests::a_bytes_buffer_drains_its_bodies_by_move` pins
it by pointer identity (the same allocation goes in and comes out; an
allocation counter is not needed to prove a move).
`a_bytes_buffer_drain_is_frame_change_bodies` pins that the generic drain
produces byte-identical frames to `frame_change_bodies` on the same bodies,
and `settle_effects_keeps_the_tier_off_park_for_the_caller_and_folds_the_tier_on_one`
pins the two tails' contract. Zero behaviour change is otherwise proved by
the existing suites: `flz_core` (153), `flezha_runner --features
bench-internals,testing` (every binary), `flz_core_ffi` (the ABI scenario,
which drives `resolve_item` through the real `flz_core_complete`).

## The erased path's own cost is not the seam

Both arms: the typed regression shape pays **7.02 allocs/elem**, the erased
twin **8.09**. The extra allocation is pre-existing and lives in the poll
crossing, not the tail: `to_schema` clones every item's payload AND metadata
(`host_input_parts`), mints `operative_of` (a `String`) and
`factory_id.to_string()` per activation and postcard-encodes it; the fetcher
decodes; `resolve_item` calls `entity_ref_of(slot).operative` — another
`String` — per item even when it has no plugin outputs. That is area 1's
kernel work on the host path; the seam moved the count by ≤ 0.004.

## Measurement traps

- **Row 5 (`host_loop`) needs `FLEZHA_BENCH_MEASUREMENT_SECS=60`.** At the
  design's 30 s its three-pass floor is 7.44 % (one pass at `load_1m` 5.3
  straight behind an entity-stress burst); at 60 s it is 2.60 % and that is
  what the rounds used, per D6's re-run clause.
- **Row 7 (`--runtime python -p 2`) is bimodal on placement.** With the
  default `--runners 2`, two GIL-bound Python runners serve two operatives and
  a run lands on one of two placements: drain window 0.95–1.02 s (~33 k
  rec/s) or 1.69–1.82 s (~20 k). Both arms hit both modes (A 4/5 fast, B 1/5)
  and the report records nothing about placement, so the D5 shape reads as a
  +37.5 % "loss" that is an artefact. `--runners 1` removes the mode; with
  `-m 40000` (80 000 records) the window is 3.7–4.1 s and the row measures
  something (+3.21 %, paired 2/5 — still a row that measures the Python host,
  not an ~830 ns tail). D5's "~12–14 k rec/s" is stale: the veneer does
  20–34 k here, so its 40 000-record window was 1.2–2.0 s, not ≥ 3 s.
- **`FLEZHA_BENCH_SKIP_BUILD=1` measures whatever is on disk**, and the
  tree's installed Python veneer (`sdk/python/src/flezha/
  flz_core_ffi.cpython-312-x86_64-linux-gnu.so`) was a DEBUG build — 24.5 MB
  from `scripts/build-python-sdk.sh` without `--release`, the e2e's default.
  Both arms here were release builds copied in per round with the sha256
  checked in place before every run, and the original restored after.
- **`test/apps/python_runner/__pycache__/fixtures.cpython-312.pyc` is
  committed**, stale against its source (recompiles to a different size), and
  rewritten by every Python runner launch — a bench run dirties the tree.
- `git_sha` in the report cannot discriminate arms (both built from a dirty
  tree); the artifact sha256 per arm can, and the roundtrip script refuses to
  run when the copied artifact's hash does not match.
- The counting allocator is sharded (64 cache-padded atomics, one per
  thread): a single global atomic under eight workers would have been the
  measurement perturbing itself. It costs nothing measurable (1.434 Melem/s
  with it vs the 1.45 in `baseline.json`).
- One `CRITERION_HOME` per arm, or criterion's "change" comparison mixes arms.

## The line count

The proposal expected ≈ −130 production lines; the branch measures ≈ −10
(+98 with the three new tests). The `Bodies` trait, the `EffectBuffer` docs
and `SettlePolicy` cost about what the two ~90-line tails saved. It is still
worth doing, for the reason the ack granularity was unified before it: the
rules for what an invocation ships — park atomic with the offset advance,
tier-off park after the lock, pending-attempt re-take, checkpoint fold — now
exist in ONE function both hosts call, so the typed and the erased tail cannot
drift; the gain is one settlement rule and one buffer, not fewer lines.

## The numbers

Box `moon`, i7-8650U, 8 threads, `powersave`, on mains; five rounds per row,
arm order alternating (A B, B A, …), `FLEZHA_WORKER_POOL_SIZE=8`; medians of
five, loss = `1 − B/A`; floors from three arm-A passes before the seam was
compiled.

| row | unit | floor | A | B | loss | allocs/elem A → B |
|---|---|---|---|---|---|---|
| 1 `regression_100x1x1x10k` | Melem/s | 0.73 % | 1.4071 | 1.4007 | +0.45 % | 7.020 → 7.018 |
| 2 `entity_stress_8x4x8x20k/8` | Melem/s | 1.91 % | 1.9558 | 1.9628 | −0.36 % | 3.778 → 3.776 |
| 3 `tier_off` | Melem/s | — | 0.2235 | 0.2211 | +1.07 % | n/a |
| 4 `tier_on` | Melem/s | — | 0.6467 | 0.6373 | +1.45 % | n/a |
| 5 `host_loop` (60 s) | Melem/s | 2.60 % | 1.2345 | 1.2225 | +0.97 % | 8.088 → 8.092 |
| 6 roundtrip rust keyed/on `-p 4 -m 20000` | rec/s | — | 24 072 | 26 567 | −10.4 % (B ahead 5/5) | n/a |
| 7 roundtrip python identity/off, D5 shape | rec/s | — | 32 586 | 20 360 | bimodal, see above | n/a |
| 7 supplement `--runners 1 -m 40000` | rec/s | — | 20 793 | 20 124 | +3.21 % (paired 2/5) | n/a |

Verdict: `2026-08-29 1s: converge — 4 tier_on: A 0.6467, B 0.6373 Melem/s
(loss 1.45 %); every pre-floored row inside its floor; allocs flat; row 7
unresolvable`.
