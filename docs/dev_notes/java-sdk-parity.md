# Java SDK parity (`java-sdk-parity`)

- **The Java SDK is now at functional parity with Rust.** Every capability an end
  user can author in Rust — the effectively-once tier, checkpoint ack, late-input
  correction, the full workflow durable stack (proc-journal, staged outbox, durable
  loop + suspension, entity retry, loop rewind, branching), dynamic entity mode,
  and the determinism contract — is available in Java. The e2e matrix no longer
  branches per runtime; there is ONE contract.
- **The unblocker was `AddOperativeHandler` returning `new byte[0]`.**
  `AddOperativeResponse.metadata` is the ONE seam `ClassRegistry.decode_declarations/1`
  reads. With it empty the controller could not be TOLD a Java class is
  `effectively_once` / has a `correction_window` / an `ack_granularity` /
  `compensable_procs` / `leaf_procs` — so every declaration-gated capability was
  blocked, including ones not yet written. `Declarations.java` builds it now. A
  class declaring nothing still encodes to **zero bytes** (= "declared nothing"),
  which is what keeps existing classes byte-identical.
- **Two codecs coexist by design.** `EffectRecordBody` is postcard (the controller
  decodes it) but its `changes` elements are the SDK's own bytes — **Kryo** in
  Java, postcard in Rust — because the controller stores them opaquely and
  re-ships them on restore. `format_id` discriminates the FRAME kind (2 = effect
  record), not the change codec.
- **`Postcard.java`** is the ONE postcard implementation (extracted from
  `DlqPark`'s private encoder). **`AckRegistry.java`** is the ONE ack rendezvous
  (`RunnerShutdownAck`, `ParkPayloadAck`, `ProcCommitResponse`,
  `IterCommitResponse`). Callers MUST register before sending — that ordering is
  what makes it race-free.
- **`StableId`** is two longs, not `BigInteger`: the FNV prime is `2^88 + 0x13b`,
  so the 128-bit multiply is one shift + one `Math.unsignedMultiplyHigh` —
  allocation-free, which matters on a path called inside `run`. Pinned
  cross-language on BOTH sides (`determinism.rs` +
  `StableIdTest`).
- **Clocks are type-level markers.** `Clock.now()` was deleted — `WallClock.now()`
  returned ambient `Instant.now()`, an ambient-time trap inside the very type
  meant to prevent one. The runtime evaluates the declared clock ONCE per
  invocation via `clockType()` and serves it from `ctx.now()`.
- **Erasure ⇒ registration-time refusals.** What Rust rejects at compile time (a
  `TimeExtractor<Input>` bound, `UnservableClock`) Java rejects at registration,
  with the same error names. Permanent asymmetry, equal safety.
- **GOTCHA — the topology builder throws its accumulated `errors` BEFORE the
  descriptor-materialization loop.** A refusal added to that loop is added after
  the throw point and never fires. Put new refusals in the validation phase.
- **GOTCHA — never assert on `EntitySlot.effectBuffer()`.** The background
  flusher drains it within a ~5 ms linger tick, so such a test passes alone and
  flakes under load. Use `EffectRecordCapture` (test helper) which captures what
  actually SHIPPED. `stagedChanges()` IS safe — the flusher deliberately never
  drains it (an open interval is not flushable).
- **GOTCHA — a workflow unit test's `WireWriter` IS the controller.** It must (a)
  `AckRegistry.complete()` every `ProcCommit`/`IterCommit` (the driver BLOCKS at each
  barrier; a silent fake controller hangs it for 30 s), (b) FLUSH each commit's
  outbox onto the secondary stream (output is staged now — the SDK no longer ships
  it, so every emit assertion depends on this), and (c) count its completion latch
  only on `WORKFLOW_COMPLETED`.
- **GOTCHA — classify beyond-window at ADMISSION, not dispatch.** Dispatch is async
  on the pool, so folding the event-time high-water there means a burst classifies
  against a mark that has not moved yet.
- **A park is SUSPENSION, not completion**: `IterPark` exits WITHOUT
  `WorkflowCompleted`. Emitting one would tear the instance down and lose the
  iteration it parked at.
- **Branching needed NO planner change**: BFS numbering already makes completion
  order topological and `planCompensation` already reverses it. What it DID need is
  per-proc input routing — a fan-in must **accumulate** both producers' output, not
  overwrite.
- **`data_heartbeat` now carries `dispatches` + `blocked_parks`** (was
  `credit_parked` only). This was a LIVE operational gap: a wedged Java runner
  looked healthy to the controller's `StallClassifier`.
- **GOTCHA — `ProcCommit.iter` MUST be stamped; Java silently did not.**
  `commitProcBoundary` set only `operative_id`/`proc_idx`, so every `proc_committed`
  landed at `iter=0` while `iter_committed` advanced normally. The controller
  keys an entry's flush marker by `{type, proc_idx, iter}`
  (`ProcJournal.pending_entries/1`), so on a loop's SECOND and later iterations the
  commit collided with iteration 0's already-written `outbox_flushed` and its
  staged output was **journaled but never delivered** — silent, and invisible to
  every test that ran one iteration. Rust always stamped it (`driver.rs`, both the
  forward and `compensating` builds). Java now stamps both, `StageRecord` gained
  `iter` (Rust parity), and `resumeIter/1` is the single definition of "the
  iteration a resumed instance runs next". Pinned by
  `WorkflowKindTest.perInputLoopCommitsPerIterationThenParks`, which asserts
  `opCommitIters() == [0, 1]` — it fails with `[0, 0]` against the old code.
  Symptom to recognise: a `PER_INPUT`/`CHAINED` Java operative emits fine on its
  first turn and is silent forever after, with `flz state journal` showing an
  `proc_committed` carrying an outbox and no following `outbox_flushed`.

