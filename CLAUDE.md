# Flezha repo notes

## Runner entry point + dynamic type set (`sdk-runner-entrypoint-redesign`)

- **Runner-as-handle, no builder, mirrored across both SDKs.**
  `Flezha.createRunner(ns).defineUnbounded(A.class).run()` (Java) /
  `Flezha::create_runner(ns)?.define_unbounded::<A>()?.run()?` (Rust) —
  Beam's `Pipeline.create()` shape: ONE concrete `FlezhaRunner` carries
  declaration and lifecycle. `RunnerBuilder`, `newRunner`/`new_runner`, both
  Java registry facades (`UnboundedOperativeRegistry`/`BoundedOperativeRegistry`),
  freeze/unfreeze, and the zero-op refusal are GONE (no shim). Zero-operative
  start is LEGAL (the controller always accepted an empty factory list).
  `run()` = `start()` + `awaitTermination()` — never independent. Naming rules
  (incl. `create*` over `new*`, `remove*` as `define*`'s inverse, prefer
  Beam/Spark/Kafka-Streams vocabulary) live in `docs/sdk_naming_conventions.md`.
- **The declared factory set is PER-RUNNER and starts EMPTY each generation**
  (gen 2 must re-define; inventory harvest merges at every `start()`). The
  process-global class registries are overwrite-on-insert + `unregister_class`
  — duplicate detection lives in the runner's declared set. `define*`/`remove*`
  are legal pre- AND post-start: post-start changes ship `UpdateFactories`
  (proto tag 33, full set, fire-and-forget, NO ack) via ControlChannel →
  `FleetManager.handle_cast({:sync_factories,..})` (remove_ce_factories +
  store_factories composed); the reconnect init frame — serialized from the
  same per-runner set at send time — stays the resync of record.
- **Removal authority is SDK-side and race-free by construction.** Rust: a
  `RunnerMessage::RemoveFactory` handled on the coordinator loop (serialized
  with `AddOperative`; seam = `RunnerNode::remove_factory`, unit-tested in
  `fast.rs`); `BoundedInstance` carries `factory_id` for the scan. Java (no
  single coordinator thread): `FlezhaImpl.removeType` = unregister-first →
  scan live instances → re-register + throw `OperativeTypeInUseException`
  (extends ISE) on a live hit — the transient window is benign (a failing
  `AddOperative` is re-placed by convergence). Live instances of the factory ⇒
  `FlezhaError::OperativeTypeInUse` / that exception; undefined ⇒
  `NotDefined` / IAE naming the id.
- **Rust: ONE public error.** `FlezhaError` (thiserror; `InvalidNamespace,
  AlreadyRunning, ConnectFailed, InvalidTopology(#[from] TopologyBuildError),
  InvalidBounded(#[from] BuildBoundedError), DuplicateFactoryId, NotDefined,
  OperativeTypeInUse, NotStarted, RunnerUnavailable`) — the whole chain
  compiles under one `?`. `RunnerStartError`/`RunnerError` deleted. The
  per-issue topology enum was renamed `BuildTopologyError` → `TopologyIssue`
  (the per-kind `TopologyBuildError` keeps its documented name).
  `operatives()` returns `Result<HashMap<String, Arc<UnboundedInstance>>, _>`
  (the `Arc<RwLock<Arc<Mutex<..>>>>` view + `snapshot_as_operative_view` are
  gone); `id()`/`getId()` → `namespace()`/`getNamespace()`.
- **The slot is claimed at `start()`, not create.** Any number of unstarted
  runners may exist; CAS in `start()`; the loser never dials nor touches
  shared state; owner-never-started Drop/close is a no-op; double-`start()`
  on one runner errors. Rust `RunnerCore { namespace, declared, started,
  terminated+condvar }` behind `ACTIVE_RUNNER: Mutex<Option<ActiveRunner>>`;
  Java `FlezhaRunner` holds two instance-scoped `RegistryCore`s + an
  `AtomicReference<RunnerHandle>` (`internal/RunnerHandle` = the runtime SPI:
  runnerId/operatives/awaitTermination/close/removeType/syncFactories).
  `FlezhaImpl.start(String, Supplier<List<String>>, Runnable)` — the supplier
  feeds the initial handshake AND reconnect re-handshakes. `FactorySync`
  (runtime) ships `UpdateFactories` via the GracefulShutdown frame-write
  pattern; the processor aggregator generates `defineAll(FlezhaRunner)`.
- **GOTCHA — a chained `start()` temporary Drops at statement end** (Rust):
  always `let runner = ...start()?;`. `run()` is immune.
- **GOTCHA — the e2e harness kills runners by closing stdin.** `Port.close/1`
  is how `node_test_helper` tears down runner processes, so both test apps
  keep a stdin-EOF **watcher** thread (exit-on-EOF) even though keep-alive is
  `run()`'s job now. Deleting it orphans every e2e runner.
- **GOTCHA — the committed dist jars embed the api classes.** Any change to
  `flezha-sdk-api` needs `scripts/build-dist.sh` (or the jar steps manually)
  + committing `sdk/java/flezha-sdk/dist/` and `test/apps/java_runner/dist/`.
  The fat-jar module shades its own stale `target/classes` FIRST — after an
  api change, `mvn clean` in sdk/java or the shaded jar keeps old classes.
- **Known limitation (documented, not fixed)**: a parked/suspended durable
  bounded instance is absent from the SDK instance registry, so removing its
  type succeeds SDK-side and rehydration later fails placement. FleetManager's
  dropped-factory warning is the tripwire
  (`docs/resilience/bounded_operative.md`).
- **Lifecycle tests**: Rust `sdk/rust/tests/runner_lifecycle.rs` — still ONE
  sequential `#[test]`; the fake `UnixListener` controller now CAPTURES +
  DECODES frames (init JSON factory_ids, UpdateFactories protobuf);
  `FLEZHA_CONTROL_ACK_TIMEOUT_MS` raised so the frame-less fake never triggers
  reconnect churn. Java `RunnerLifecycleTest` in flezha-sdk-api with test
  doubles under the runtime FQNs (`com.flezha.sdk.runtime.FlezhaImpl`
  implements `RunnerHandle`, fakeable `LIVE_INSTANCES`, recorded
  `SYNCED_SETS`); controller side in
  `test/flezha/runner/fleet_manager_sync_factories_test.exs` +
  `control_channel_update_factories_test.exs`. The real runtime wiring is
  covered by the e2e matrix.

## Java SDK parity (`java-sdk-parity`)

- **The Java SDK is now at functional parity with Rust.** Every capability an end
  user can author in Rust — the effectively-once tier, checkpoint ack, late-input
  correction, the full bounded durable stack (op-journal, staged outbox, durable
  loop + suspension, segment retry, loop rewind, branching), dynamic segmentation,
  and the determinism contract — is available in Java. The e2e matrix no longer
  branches per runtime; there is ONE contract.
- **The unblocker was `AddOperativeHandler` returning `new byte[0]`.**
  `AddOperativeResponse.metadata` is the ONE seam `Overwatch.decode_declarations/1`
  reads. With it empty the controller could not be TOLD a Java class is
  `effectively_once` / has a `correction_window` / an `ack_granularity` /
  `compensable_ops` / `leaf_ops` — so every declaration-gated capability was
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
  (`RunnerShutdownAck`, `ParkPayloadAck`, `OpCommitResponse`,
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
- **GOTCHA — never assert on `SegmentSlot.effectBuffer()`.** The background
  flusher drains it within a ~5 ms linger tick, so such a test passes alone and
  flakes under load. Use `EffectRecordCapture` (test helper) which captures what
  actually SHIPPED. `stagedChanges()` IS safe — the flusher deliberately never
  drains it (an open interval is not flushable).
- **GOTCHA — a bounded unit test's `WireWriter` IS the controller.** It must (a)
  `AckRegistry.complete()` every `OpCommit`/`IterCommit` (the driver BLOCKS at each
  barrier; a silent fake controller hangs it for 30 s), (b) FLUSH each commit's
  outbox onto the secondary stream (output is staged now — the SDK no longer ships
  it, so every emit assertion depends on this), and (c) count its completion latch
  only on `BOUNDED_COMPLETED`.
- **GOTCHA — classify beyond-window at ADMISSION, not dispatch.** Dispatch is async
  on the pool, so folding the event-time high-water there means a burst classifies
  against a mark that has not moved yet.
- **A park is SUSPENSION, not completion**: `IterPark` exits WITHOUT
  `BoundedCompleted`. Emitting one would tear the instance down and lose the
  iteration it parked at.
- **Branching needed NO planner change**: BFS numbering already makes completion
  order topological and `planCompensation` already reverses it. What it DID need is
  per-op input routing — a fan-in must **accumulate** both producers' output, not
  overwrite.
- **`data_heartbeat` now carries `dispatches` + `blocked_parks`** (was
  `credit_parked` only). This was a LIVE operational gap: a wedged Java runner
  looked healthy to the controller's `StallClassifier`.

## Checkpoint ack-granularity (`effectively-once-checkpoint-granularity`)

- **The kernel was already checkpoint-capable.** `EffectRecordBody.consumed_offset`
  is `Option<u64>` and EVERY controller reader already tolerates `nil`:
  `dispatch_one_effect` advances the watermark only `if is_integer(...)`,
  `max_offset/2` skips nils rebuilding the durable watermark at open,
  `consumed_offset_of/1` folds nil→`-1` for the correction baseline. So the mode
  is a **producer-side** change: no new record type, no new control message, no
  proto change, no new retention rule.
- **Interval replay = the EXISTING crash-resume replay.**
  `Overwatch.restore_and_replay_journal/1` already re-forwards
  `InputLog.watermark + 1 .. last_offset` on every tier bind. Hold the watermark
  at the last checkpoint and that path IS the interval replay. Same for retention
  (truncates below the min durable consumed-offset = the last checkpoint) and for
  credit (drains on `:watermark_advance` → bursts at checkpoint cadence). All
  three requirements fell out with ZERO controller code.
- **SDK**: `c.checkpoint_ack()` (needs `effectively_once()`, else
  `CheckpointAckWithoutTier`) + `ctx.checkpoint()`. `TypedBuffer` gained a third
  vec `staged` — the open interval's changes. `drain_to_frames`/`is_empty`
  deliberately IGNORE it (an open interval is not flushable; draining it alone
  would put state ahead of its offset — the exact tear the mode prevents).
  Dispatcher emission: `closing = checkpointed || dlq.is_some()` → ship
  `{staged, consumed_offset}`; else ship `{[], outputs, None}` iff outputs exist.
- **Mid-interval emits flush immediately** (open question A/B → **A**). Option B
  (stage until the checkpoint) buys nothing: the interval's outputs are re-emitted
  on replay under either option, because the inputs are re-forwarded. Keyed sinks
  absorb it — the tier already requires them.
- **A park CLOSES the interval** (the DLQ open question). The park record already
  carries `consumed_offset`; letting it also carry the staged changes means the
  poison offset is acknowledged and the replay starts AFTER it. No offset-skip set,
  no DLQ lookup on replay. It is also the only correct choice — otherwise the
  poison is acked while the state before it is not durable.
- **A restore discards the open interval** (`FlushableBuffer::discard_staged`,
  called in `restore_into_slot` beside the mailbox/replay-buffer clears). Same
  rationale: it belongs to a past that no longer exists.
- **The controller needs the declaration for exactly ONE reason**:
  `Watchdog.sweep_catastrophic_dataflow` reads "produced output before, silent
  since" as a dead feed — a lie for a checkpoint class, whose silence is its steady
  state. Rides `AddOperativeResponse.metadata` (`ack_granularity=checkpoint`) →
  `decode_declarations/1` → `ClassRegistry.put_checkpoint_ack/2` (ETS + sidecar,
  same shape as the tier; `put_flag/4`+`flag?/3` now share that shape).
  `sweep_output_staleness` needs NO gate — it fires only when changes are still
  arriving but persistence froze, which is a genuine wedge in checkpoint mode too.
  `StallClassifier.watermark_frozen?` would misfire, but it is currently
  unreachable (`progress_markers/0` never populates the tier cursors).
- **Tests**: `operative_dispatch.rs` (interval holds / park closes / restore
  discards), `builder.rs` (`checkpoint_ack_without_the_tier_is_refused`),
  `wal_writer_outbox_test.exs` (nil-offset record flushes outputs, holds the
  watermark), `class_registry_test.exs`, `add_operative_response_test.exs`,
  `watchdog_test.exs` (with a positive control — the pre-existing bounded
  catastrophic refute was vacuous: `:catastrophic_dataflow_timeout_ms` defaults to
  60 s and no test armed it), e2e `effectively_once_checkpoint_e2e_test.exs`
  (fixture `checkpoint_store`, `commit:` closes the interval).
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `EffectRecord` encoder + `c.checkpointAck()`
  + `ctx.checkpoint()` + the staged interval.

## Unbounded late-input correction (`unbounded-late-input-correction`)

- **THERE IS NO SPLICE.** `Overwatch.journal_input/2` appends to the input log
  BEFORE forwarding (`overwatch.ex:634`); the SDK extracts event time later, in
  `dispatch`. So a late input is already durable in the log at its arrival offset
  by the time anything knows it is late. The slice is an offset range and already
  contains it. The correction = the EXISTING tier restore-and-replay path with
  (i) a rewound baseline and (ii) an event-time-sorted slice. **No new control
  message, no correction journal, no crash marker, no proto change.**
- **Crash-mid-correction needs no marker** — before the rewind commits, nothing
  durable changed (the late input is still unconsumed → re-classified → correction
  re-runs); after it commits, the on-disk state IS the baseline and the ordinary
  resume replay IS the correction. Hence: for a correction-declaring class, EVERY
  journaled replay is marked and sorted.
- **Baseline = scan FORWARD to the first record with `event_time >= T`, keep the
  one before it.** A backward scan for "first earlier event time" is WRONG — it
  stops at an out-of-order straggler (a beyond-window `process_as_arrival` input)
  and leaves the records that overtook the late input in place, correcting
  nothing. Pinned by `wal_rewind_test.exs` "folds an out-of-order straggler".
- **`WalWriter.rewind_to(seq, epoch)`** — keeps records ≤ seq, drops ONLY snapshots
  > seq, re-derives `pending_outbox` from the rewritten file, lowers the input-log
  watermark (`InputLog.rewind_consumed/3`). NOT `realign_epoch/4`, which truncates
  the WAL to zero and `File.rm`s every snapshot (a cold-takeover wipe). Shared tail
  `bump_epoch/2` (outbox cursors + `persist_dedup` = the COMMIT POINT). `epoch` is a
  PARAMETER, not `current+1` — that is what makes a crash-resumed correction
  idempotent instead of double-bumping.
- **`flz.replay_end` is a BROADCAST, not a mark on the last input.** An input routes
  to ONE segment by key, so a marker riding the last one strands the slice every
  OTHER segment buffered. It is a payload-less frame intercepted in `fast.rs` before
  channel lookup → `dispatcher::end_replay_slice`.
- **Drain ordering gotcha**: a closed slice must flush BEFORE any live input popped
  behind it (`maybe_dispatch_replay_slice` runs both mid-pop-loop and after it) —
  otherwise the live input jumps ahead of the very inputs being re-ordered. Caught by
  `a_replayed_slice_dispatches_in_event_time_order`.
- **`do_sweep` gates on the FLOOR, not the watermark** — under a hold window the
  floor advances with the clock while the watermark stands still, so the old
  watermark-advanced guard never truncated.
- **Effect record gained `event_time: u64`** (same `format_id = 2`; no legacy shim —
  the library is unpublished). Serves both the correction baseline AND
  `TimeTravel`'s `{:timestamp, ts}` (which used to approximate from the WAL file's
  mtime; `pick_snapshot_by_mtime/2` deleted). Cross-language fixture in
  `flusher.rs` + `wire_test.exs`.
- **No snapshot-header high-water needed**: `compact_after_age_ms >= correction_window`
  guarantees the retained WAL always covers the window, so max `event_time` over the
  restored change log IS the high-water (anything older is beyond-window anyway).
  `compact_after_age_ms` therefore moved onto the per-operative `resolve/5` cascade —
  it is a correctness constraint now, not a tuning knob.
- **Declaration**: `c.correction_window(ms)` + `c.beyond_window_policy(..)`; rides
  `AddOperativeResponse.metadata` → `Overwatch.decode_declarations/1`. Refusals: tier
  + `EventTimeFrom` + all-channels-keyed (SDK build); hold-window + snapshot age-floor
  ≥ window (controller, at start). Beyond-window default = `process_as_arrival` (it is
  today's behaviour — declaring a window must never silently start dropping data).
- **Limit worth knowing**: a correction overwrites the keys it re-emits; it does NOT
  retract a key the stale run emitted and the corrected run does not. Safe for
  event-time-bucketed keys (candles); documented in `data_durability.md`.
- **SCOPE DECISION — no `input.idx` side-file.** The hold window now actually bounds
  the log, so a header-only scan is O(retained). A durable index would add
  crash-consistency + truncation-trimming + rebuild for a speculative win.
- **NEVER reset the event-time high-water on a replay.** Every replay is preceded
  by a restore, which already rebuilt it from the change log. Zeroing it first
  (an early draft did) is lossy on a crash-resume whose unconsumed tail carries a
  LOWER max event time than the recovered state — the segment comes back with a
  high-water below the truth and silently reads a genuinely late input as on-time.
  Pinned by `a_replay_does_not_lower_the_event_time_high_water`.
- **`WalWriter.fold_records/3`** is now the ONE place the
  `decode_record → recurse → halt-on-torn-tail` walk lives. There were **11**
  copies across `wal_writer`/`input_log`/`time_travel`/`record_log`/`retention`;
  all but `scan_for_last_valid_seq` (its error branch truncates the torn tail —
  a side effect, not a fold) now go through it. `Wire.decode_effect_record/2` is
  the matching "header check + body decode" pair. `Retention.rewrite_offline/2`
  reuses `WalWriter.rewrite_keep_seq_gt/2` instead of its own byte-copy.
- **CLI**: `flz operative replay <operative/operation/key> --from <seq|ts>`.
  Telemetry `[:flezha, :correction, :completed|:refused]`, `[:flezha, :state, :wal_rewind]`.
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `c.correctionWindow(ms)` + event-time slice.

## Bounded loop rewind (`bounded-loop-rewind`)

- **Marker, not truncation**: `rewound` = op-journal entry type 6, target =
  the entry's own `iter` field (empty payload), appended with
  `current_epoch + 1` — the FIRST non-zero epoch anything writes. The journal
  stays append-only; `OpJournal.live_entries/1` is the one fold that applies
  markers (**on `rewound(N)`, purge accumulated entries with `iter > N`; keep
  everything appended after it**). `restore_state/1`, `pending_entries/1`,
  `consumed_offset/1`, `committed_iterations/1` all run over it — so a rewound
  iteration's unflushed outbox **ceases to be owed**, not merely gets skipped.
- **Undo is derived, not marked**: `restore_state/1` also returns `rewind_undo`
  = op_committed entries before the marker with `iter > N`, minus those with a
  `compensated` entry appended *after* their own commit (that "after its own
  commit" test is what keeps a re-run distinguishable from the original it
  replaced, and survives a second rewind). No done-marker, no completeness
  oracle → crash-mid-rewind resumes with exactly the outstanding work.
- **Wire**: ONE additive field, `OpJournalState.rewind_undo = 8`
  (`repeated OpCommittedOp`). No new control message — the controller writes the
  marker and rebinds; the folded journal is all the SDK sees.
- **SDK**: `drive_chain` gains one branch before the forward loop →
  `run_rewind_undo` reuses `plan_compensation(AllOrNothing, groups, &[])`
  verbatim (reverse iteration, reverse stage) — **forced** AllOrNothing whatever
  the class's `rollback_mode` says, since a rewind unwinds committed iterations
  by definition. `StageRecord` gained `iter` (the `compensated` entry's `iter`
  was hardcoded `0` before). Journals ONE `compensated` per op — **even an empty
  one for a hookless op**, which is what drains `rewind_undo`; and it does NOT
  consult the per-segment done markers (they are per *segment*, and a segment
  that ran in two rewound iterations must be undone once per iteration).
  The two undo passes deliberately stay separate — one is **marker**-backed
  (crash resume, per segment), the other **journal**-backed (rewind, per op) —
  but share `plan_compensation` (the plan), `compensate_segment` (the effect) and
  `journal_undo` (the `compensated` record).
- **Resume = rebind, not park**: `Overwatch.handle_call({:rewind, N})` → guard →
  `append_rewound` → evict (`evict_runner/2`, extracted from `enter_suspended`)
  → `rehydrate/1`. No `parked` entry, no wake timer.
- **Coverage guard** (`rewind_guard/2`, controller-side, before anything durable
  changes): an unwound op that staged plugin output MUST declare `compensate`.
  The keyed-channel half of the original spec is **unreachable for bounded** —
  `BoundedSegmentSink` stages `(channel, bytes)` and `outbox_items` drops the
  key, so a bounded emit carries no `flz.key`. Facts ride the EXISTING
  `AddOperativeResponse.metadata` map (`"compensable_ops" => "0,2"`, no proto
  change) → `ClassRegistry.put_compensable_ops/2` (same sidecar shape as the
  tier). The guard asks `OpJournal.staged_channels_above/2` — the journal owns
  its fold, Overwatch owns the policy (`live_entries/1` is private).
- **`Overwatch.decode_declarations/1`** is the ONE place the `AddOperative`
  response is decoded → `%{effectively_once:, compensable_ops:}`. A new
  SDK-declared capability is added there and nowhere else. (`compensable_ops`
  `nil` = the class declared nothing — Java/unbounded/run-sugar — vs `[]` = a
  chain with nothing compensable; a rewind treats those differently.)
- **GOTCHA — a rewind does NOT replay inputs.** `ControlChannel` truncates the
  inbox to the consumed offset at every iteration commit
  (`control_channel.ex:629`), so a committed turn's input is GONE. Moving
  `consumed_offset` back cannot resurrect it. Rewind is **undo-and-continue**:
  the loop resumes waiting and the NEXT message becomes iteration N+1. (The e2e
  caught this — an earlier draft asserted re-delivery and failed.)
- **Segment State** is unwound by the `compensate` hooks (the saga contract), NOT
  by truncating the state WAL. A hookless op leaves its State change in place.
- **CLI**: `flz operative rewind <instance> --to-iteration N`
  (`CliManager.operative_rewind/3`); `flz state journal` renders `rewound`.
  Telemetry `[:flezha, :bounded, :rewind]`.
- **Tests**: `op_journal_test.exs` (epoch bump, fold purge, outbox no longer
  owed, `rewind_undo` drain, re-run + second rewind),
  `overwatch_suspension_test.exs` (rewind + both refusals),
  `cli_manager_rewind_test.exs`, `bounded_driver_scenarios` (reverse-order undo,
  hookless drain), e2e "loop rewind (rust/uds)" in
  `bounded_crash_resume_e2e_test.exs` (fixture `chat_rewind_bounded` —
  `ChatReplyUndoable` keeps its undo handle in State and retracts in reverse;
  asserts `flz.epoch` 0 before / 1 after).
- **Split**: unbounded late-input correction + `flz operative replay` were carved
  out into the `unbounded-late-input-correction` openspec — its design needs four
  primitives that do NOT exist (event-time→seq map, non-destructive WAL rebase,
  event-time-ordered replay splice, an actually-enforced input-log `hold_window`;
  `realign_epoch/4` wipes the WAL and every snapshot — it cannot be "scoped").

## Bounded segment retry (`bounded-segment-retry`)

- **Ladder (SDK, Rust)**: `run_segment_retrying` in `bounded/driver.rs` wraps the
  pool task — only the FAILING segment re-runs its batch (siblings stay buffered
  at the barrier), fresh sink per attempt (a failed attempt's staged output is
  dropped, never double-emitted). Backoff REUSES `dispatcher::retry_backoff`
  (the unbounded DLQ ladder's helper: `base × 2^(n-1)` ±50% jitter, capped
  `RETRY_BACKOFF_MAX_MS` = 5 s) and the wait is cancellation-aware
  (`sleep_cancellable`, ≤200 ms poll → a stop/`max_duration` ends it at once).
  Retries count into `segment::park::RETRIES_TOTAL` (same `data_heartbeat`
  metric as unbounded).
- **Policy**: `BoundedOperative::retry_attempts()` (default 3 TOTAL attempts) +
  `retry_backoff()` (default 500 ms base) → resolved onto the SHARED
  `channel::RetryPolicy` in `BoundedSpec.retry`; `0` collapses to one attempt
  (fail-fast); `attempts > 1` with a zero backoff is rejected at registration
  (`BuildBoundedError::ZeroRetryBackoff`).
- **Classification**: `BoundedError { message, non_retryable }` (was a tuple
  struct); `BoundedError::non_retryable/1`, `From<String>/<&str>` stay
  retryable. An operation body classifies with the EXISTING
  `OperationError::non_retryable` — `run_collecting` carries `is_non_retryable()`
  through to the ladder. A panic stays retryable (author never classified it).
- **Boundaries**: retry is strictly PRE-commit (a journaled op is never
  retried); a crash mid-ladder resumes as a normal uncommitted-op re-run
  (attempt counter resets). `run`-sugar has no segments → terminal on first Err.
  A stage error while the cancel token is set now reports `cancelled` (was
  `error`) — `run_stage_parallel`'s caller folds it through `abnormal_status`.
- **Tests**: `bounded_driver_scenarios` (recover / exhaust+compensate /
  non-retryable / attempts=0 / stop-interrupts-backoff / run-sugar counted
  once), `retry_policy_is_resolved_and_validated`, e2e "segment retry (rust/uds)"
  in `test/system/bounded_crash_resume_e2e_test.exs` (fixture `retry_bounded`,
  failures armed via `FLEZHA_BOUNDED_RETRY_FAILS`). GOTCHA: an e2e/unit scenario
  must never busy-wait on the main test thread — it is the fake controller and
  must stay in `drain` to ack `OpCommit`, else the driver's 30 s ack timeout
  fires (cancel from a helper thread instead).
- **Docs**: `docs/resilience/bounded_operative.md` "Segment retry: the rung
  before the abort" (incl. the bounded-vs-unbounded escape-ladder split),
  `docs/resilience/data_durability.md` DLQ "Bounded equivalence".
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `retryAttempts()` / `retryBackoff()` ladder.

## Bounded durable loop + suspension (`bounded-durable-loop-and-suspension`)

- **Loop (SDK)**: `BoundedOperative::loop_mode()` → `Once` (default, the
  old single pass, byte-identical) | `PerInput` (one iteration per inbox
  item — chat turns) | `Chained` (seeded by the previous iteration's
  terminal handoff). `drive_chain` is now an iteration loop around the
  SAME stage machinery (`run_iteration` = the old for-loop body).
  Registration rejects a loop on run-sugar (`LoopWithoutChain`) and a
  durable `Chained` whose terminal op has no codec'd output channel
  (`LoopTerminalWithoutCodec` — else a crash re-seeds empty and reads as
  "converged").
- **Iteration commit**: `IterCommit` (proto tag 30) / `IterCommitResponse`
  (31) at every iteration boundary → ONE `iter_committed(iter,
  consumed_offset)` journal entry (type 3, ACTIVE) carrying the terminal
  handoff; ack; THEN boundary `CompactSegment` per segment + outbox flush
  + inbox truncate (post-ack cleanup — a failure there costs work, not
  correctness). Every `op_committed` is stamped with its enclosing `iter`.
- **KeepProgress is real**: `plan_compensation`'s `committed` is rebuilt
  from journaled `committed_ops` grouped by iter (`journaled_iterations`).
  Planner UNCHANGED. A journal-rebuilt iteration's `SegmentRun.outputs` is
  empty (the at-least-once undo-handle-in-State contract) — the tests key
  on that (`undo-loop:1` in-flight vs `undo-loop:0` journaled).
- **Durable inbox**: `lib/flezha/state/op_inbox.ex` — per-instance
  `inbox.wal` beside `op.journal` on the SAME WalWriter substrate,
  identity `Registry.inbox_id/1` (`{inst, inst, "_op_inbox"}`).
  **Offset = WAL seq + 1** (the substrate REQUIRES seq 0 first — an
  empty WAL receiving any other seq sheds the frame and pulls a baseline
  — while offset 0 must mean "nothing consumed"/"unjournaled").
  `Registry.@reserved_files` keeps `inbox.wal` out of `scan_dir` (it
  would otherwise decode as a Segment named "inbox").
- **Append-before-forward**: `Overwatch.journal_input/2` — the ONE durable
  ingest seam (eff-once InputLog clause + durable-bounded OpInbox clause).
  Stamps `flz.inbox_offset`; a duplicate (`flz.msg_id`, else payload hash,
  inside the undelivered window) returns nil = absorbed; a failed append
  is NOT forwarded.
- **Suspension**: SDK ships `IterPark` (tag 32) and exits WITHOUT
  `BoundedCompleted` (`RunOutcome::Parked`). Controller journals
  `parked(N[, deadline])` (type 4, ACTIVE) THEN
  `Overwatch.suspend/3` → `RemOperative` + `operative_set_runner(nil)`;
  Overwatch + registry entry STAY (zero *runner* resources).
  `Convergence.Diff`/`Loop` skip `suspended?` (else they'd rebind it and
  undo the eviction). `flz operative info` → phase `suspended`.
- **Rehydrate = bind**: `complete_init` always delivers the undelivered
  inbox tail (`deliver_inbox_tail`), so crash-resume / migrate / wake all
  use ONE path; `rehydrate/1` reuses the existing `{:unpark, ...}` migrate
  task. The wake deadline is an Overwatch `Process.send_after` re-armed
  from the journal at boot (NOT a Watchdog sweep — fires exactly, no
  scheduler); it wakes by appending a synthetic empty inbox item
  (`flz.wake=timer`), so racing wake sources collapse to one rehydration
  by construction. Boot: `suspended_on_disk?` (journal's last entry is
  `parked` AND nothing undelivered) → enter suspended, never bind.
- **Knobs**: `FLEZHA_BOUNDED_PARK_IDLE_MS` (SDK-side, default 5 s
  hysteresis), `BoundedOperative::wake_after()`.
- **CLI**: `flz state inbox <instance>`; `flz state journal` renders
  `iter_committed`/`parked`.
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `loopMode()` / `wakeAfter()` / `IterCommit` / `IterPark`.
- **Tests**: `bounded_driver_scenarios` (loop turns → iteration commits +
  park + resume-at-iteration; journal-backed KeepProgress/AllOrNothing),
  `loop_authoring_is_validated`, `test/flezha/state/op_inbox_test.exs`,
  `op_journal_test.exs` (iter/park entries),
  `control_channel_op_commit_test.exs` (IterCommit journal→ack→truncate,
  boundary snapshot, IterPark→suspend),
  `test/flezha/operative/overwatch_suspension_test.exs` (park/rehydrate/
  timer/redeploy).
- **Docs**: `docs/resilience/bounded_operative.md` "Durable loop and
  suspension", `docs/resilience/data_durability.md` "the durable inbox",
  `docs/sdk_protocol_spec.md` `IterCommit`/`IterCommitResponse`/`IterPark`
  + `flz.inbox_offset`.
- **Unblocks**: `operative-rewind-and-correction` → `bounded-loop-rewind`.

## Bounded staged commit + transactional outbox (`bounded-staged-commit-and-outbox`)

- **Staging (SDK)**: inside a structured op `ctx.emit` NEVER hits the wire —
  `BoundedSegmentSink` (per segment, per stage) buffers `(channel, bytes)`
  when `ChainCtx.journaling()` (durable class AND coordinator installed);
  otherwise (opted-out / transport-less) it forwards direct, as before.
  `BoundedCollectSink::emit_plugin_bytes(channel, bytes)` carries the
  channel name (`descriptor.name`, same as the eff-once effect record).
  A failing stage drops its staged bytes with the `Err` — nothing leaks.
  `run`-sugar keeps the direct path (no barrier to stage against).
- **Transport**: extends the SAME `OpCommit` handshake — `outbox`
  (`OpOutboxItem{segment_id, seq_in_op, channel, payload}`, tag 6) +
  `compensating` (tag 7). No second protocol. `ship_op_commit/3` is shared
  by the forward barrier and the undo record; the undo wait ignores the
  cancel token (rollback IS the response to that cancel).
- **Journal**: `op_committed` carries segments+handoff+outbox in ONE record;
  `outbox_flushed` (type 2, payload `[for_type u8]` — forward vs undo are
  gated independently) and `compensated` (type 5, outbox only) are now
  ACTIVE. Epoch = u32 in the WAL record **header** (bytes 0–3; byte 9 is the
  effect-record format-id, so no collision), 0 until rewind bumps it.
- **Flush**: `OpJournal.flush_pending/2` is the ONE path — post-ack
  (`ControlChannel`, retry timer on `:no_overwatch`) and restore re-drive
  (`Overwatch.replay_persisted_segments`, gated `kind == :bounded`, passes
  its own pid). Journal state (not memory) decides what is owed →
  idempotent by construction. Delivery = `GenServer.cast(overwatch,
  {:io_out, payload, stamps})`, same fan-out as `WalWriter`'s eff-once
  outbox. Stamps: `flz.segment`, `flz.seq` = `<op_idx>.<seq_in_op>`
  (undo: trailing `.1` — numeric so the Postgres sink's
  `string_to_array(seq)::bigint[]` guard still orders it), `flz.epoch`,
  `channel`. Telemetry `[:flezha, :state, :op_outbox_flush]`.
- **Ordering gotcha**: the undo record is journaled BEFORE the
  `compensation_done` marker — a crash between them re-runs the (idempotent)
  hook rather than losing its undo output.
- **Tests**: `bounded_driver_scenarios` (staging, leak-nothing, run-sugar
  direct, `UndoSaga` comp staging; the fake controller now FLUSHES each
  commit's outbox on ack, which is why the old output assertions still
  hold), `test/flezha/state/op_journal_test.exs` (crash-before-flush
  re-drive with identical keys = truncate journal back to pre-flush bytes),
  `control_channel_op_commit_test.exs` (sync on `:sys.get_state(channel)` —
  the marker append lands after the `:io_out` cast).
- **Java SDK**: AT PARITY (`java-sdk-parity`) — staged commit + `OpCommit` outbox.
- **Docs**: `docs/sdk_protocol_spec.md` `OpCommit`/`OpCommitResponse`,
  `docs/resilience/data_durability.md` "Bounded operatives: staged commit
  and the transactional outbox".

## Bounded op-journal (`bounded-op-journal`)

- **Store**: `lib/flezha/state/op_journal.ex` — per-instance `op.journal` at
  `<state_root>/<inst>/<inst>/op.journal` on the WalWriter+FsyncBatcher
  substrate under reserved identity `Registry.journal_id/1`
  (`{inst, inst, "_op_journal"}`); entry codec
  `[type u8][op_idx u16][iter u32][payload]` inside normal WAL framing,
  type 1 = `op_committed` (segments-ran + channel-tagged handoff blobs),
  2–5 reserved (tolerated on decode). Size bound =
  `:wal_high_watermark_bytes` → `{:error, {:oversized_handoff, ..}}`. The
  journal writer skips the outbox dedup side-file so it never registers a
  phantom Segment.
- **Handshake**: SDK→controller `OpCommit` (proto tag 28) at every op
  barrier of a durable structured bounded; ControlChannel appends +
  fsyncs, replies `OpCommitResponse` (tag 29) under the SAME reference_id
  (journal-then-ack); driver blocks ≤30 s (`bounded/driver.rs
  commit_op_boundary`), failure/timeout → terminal error + compensation.
  Telemetry `[:flezha, :state, :op_commit]`.
- **Resume**: journal state rides `RestoreInitState.op_journal`
  (`OpJournalState`); `drive_chain` skips committed ops and seeds the next
  batch from the journaled handoff via the channel codec. Compensation
  precedence: a comp-**done** marker wins (intents alone don't — they also
  accumulate during forward progress). Skipped ops still roll back on a
  later abnormal exit (folded from intent markers).
- **Codec**: `.codec()` (postcard/serde) or `.codec_with(enc, dec)` on
  inter-op channels; REQUIRED for durable structured bounded classes —
  registration fails `MissingInterOpCodec` naming the channel. Opted-out
  classes skip journal + handshake entirely (re-run-from-top preserved).
- **CLI**: `flz state journal <operative/instance>`
  (`CliManager.state_journal/2`).
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `OpCommit` handshake + journal-gated
  resume-skip. The e2e no longer branches per runtime.
- **Tests**: `test/flezha/state/op_journal_test.exs`,
  `test/flezha/runner/control_channel_op_commit_test.exs`, Rust
  `bounded_driver_scenarios` (gating + resume-skip + codec validation),
  e2e `test/system/bounded_crash_resume_e2e_test.exs` (op A NOT re-run on
  rust; opted-out `optout_commit_block` re-runs from top).
- **Docs**: `docs/sdk_protocol_spec.md` `OpCommit`/`OpCommitResponse`,
  `docs/resilience/bounded_operative.md` "Durability" section.

## Dead-letter queue (`platform-dead-letter-queue`)

- **SDK retry-then-park**: both dispatchers re-run a failing invocation
  (returned error or panic/exception, one policy) up to the input
  channel's `retry_max` (default 3 total attempts, exponential backoff +
  jitter capped 5 s); `.retry_max(n)` / `.retry_backoff_ms(b)` on the
  channel builders, `OperationError::NonRetryable` /
  `OperationException.NonRetryable` parks immediately. Rust:
  `sdk/rust/src/operative/dispatcher.rs` retry loop +
  `sdk/rust/src/segment/park.rs` (blocking wire park, ack registry);
  Java: `OperationDispatcher.runOnce` + `runtime/state/DlqPark.java`
  (hand-rolled postcard encoder, fixture-pinned cross-language).
- **Protocol**: data-stream payload-id `park_payload` (postcard
  `ParkBody`, same outer frame as `segment_state_change`, zero header)
  acked by control command `ParkPayloadAck` (proto tag 27) AFTER the
  store fsync — park-then-advance, SDK re-sends every 2 s until acked.
  Tier-on parks skip the frame entirely: `EffectRecordBody` gained a
  trailing `dlq: Option<DlqEntryBody>` so the park and the
  consumed-offset advance are ONE atomic WAL record; the WalWriter
  materializes the store entry synchronously before
  `InputLog.record_consumed` (watermark un-freezes after the park).
- **Store**: `lib/flezha/dlq/store.ex` — one GenServer,
  `app_data/dlq/<class>/<operation>/entries.jsonl` + `<id>.bin` blobs,
  fsync-before-ack, idempotent on origin identity
  (`segment@offset` tier-on, `channel:sha256(payload)` tier-off);
  re-park while `status=replaying` bumps `generation`. Retention age AND
  size (30 d / 4 GiB, more restrictive wins), loud
  `[:flezha, :dlq, :retention_dropped]` per deletion. Config `:dlq`
  block in runtime.exs (`FLEZHA_DLQ_DIR`, `FLEZHA_DLQ_RETENTION_DAYS`).
- **CLI**: `flz dlq list|show|replay|drop` (`cli/src/dlq.rs` →
  `CliManager.dlq_*`). Replay re-enters via
  `GenServer.cast(overwatch, {:io_in, payload, %{"channel" => ...}})` —
  fresh output identity by construction (new input → new WAL seqs, NO
  epoch bump); success detected by absence of a re-park within
  `:dlq[:replay_grace_ms]` (5 s) then entry removed; failing replay
  re-parks generation+1. `drop`/`replay` audited via `:flezha_dlq`
  events (TelemetryBridge mapping + audit-sink allow-list).
- **Observability**: `dlq_parked_total`, `dlq_dropped_by_retention_total`,
  `dlq_usage` metrics bridge to the bus (source `:flezha_dlq`).
- **Bounded profile**: out of scope — AbortAll + saga compensation is the
  equivalent escape; nothing is written to the DLQ store.
- **Tests**: `sdk/rust/tests/dlq_retry.rs`, Java `DlqRetryTest`,
  `test/flezha/dlq/store_test.exs`, WalWriter park cases in
  `wal_writer_outbox_test.exs`, CLI `cli_manager_dlq_test.exs`, e2e
  `test/system/dlq_e2e_test.exs` (Rust + Java + tier watermark;
  `--include system [--include java_sdk]`). Poison test ops:
  `poison_probe` (Rust) / `java_poison_probe` (rebuild the committed jar
  with `scripts/build-dist.sh` after SDK changes!) / `tier_store` value
  `"poison"`.
- **Docs**: `docs/resilience/data_durability.md` "Dead-letter queue",
  `docs/sdk_protocol_spec.md` `park_payload`/`ParkPayloadAck`, glossary.

## Postgres sink plugin (`effectively-once-output-postgres-sink`)

- **Modules**: `lib/flezha/plugin/provider/postgres.ex`
  (`Flezha.Plugin.Provider.Postgres`, `:io_binding`, registered as
  `operative_postgres`) + `postgres/{sink_sup,sink_worker}.ex`. `SinkSup`
  and Kafka's `ProducerSup` are thin wrappers over the shared
  `Flezha.Plugin.Provider.WorkerSup` core (one Registry + DynamicSupervisor
  instance per provider family; ensure/reap/stop semantics, reuse policy
  injectable — Kafka passes `ProducerWorker.reusable?/1`). A re-prepared
  plugin reuses its live worker so buffered records survive Overwatch
  restarts. New top-level dep `postgrex ~> 0.22` (active only when the
  plugin is configured, like `:kafka_ex`).
- **Semantics**: per flush batch ONE `Postgrex.transaction` — bulk upsert
  into `flezha_records` (key = `flz.key`, fallback `<segment>/<seq>` for
  keyless tier outputs, unique `_anon/N` for unstamped records) + one
  `flezha_sink_progress` watermark upsert per segment. Both guarded by
  `(epoch, string_to_array(seq)::bigint[])` row comparison so re-drives
  and stale epochs are byte-exact no-ops. Same-key records within a batch
  are collapsed to the newest identity first (`ON CONFLICT` double-touch).
- **Failure posture**: unreachable DB → retry with exponential backoff
  (`retry_backoff_ms` 250 → `retry_max_backoff_ms` 5000), records kept;
  enqueue callers park past `queue_max` (10_000) — delay, never loss.
  `auto_create` default false: missing tables fail `health/1` with
  `{:missing_tables, [...]}` and no DDL runs. `health/1` also reports
  `{:commit_stalled, reason}` when pending + failing past
  `unhealthy_after_ms` (30 s) — feeds the plugin-health → convergence path.
- **Credential hygiene**: password rides a closure into DBConnection's
  `:configure` (never in worker state/child args); `init_provider` returns
  opts with `:password` redacted; failure logs carry `host:port/db` only.
- **Tests**: `test/plugin/provider/postgres_sink_test.exs`
  (`POSTGRES_INTEGRATION=1 mix test --include postgres_integration`, needs
  `docker compose -f docker/dev/docker-compose.yml up -d postgres-sink` —
  service is named `postgres-sink` on host port **5433** because the
  flezha_market dev stack shares compose project "dev" and owns `postgres`
  on 5432); e2e crash matrix `test/system/effectively_once_postgres_e2e_test.exs`
  (also needs `--include system`). Shared kill/rebind harness for both
  tier e2e suites: `test/helpers/tier_e2e_harness.ex`.
- **Docs**: `docs/resilience/data_durability.md` "Delivery semantics".

## Deterministic effects + output identity (`deterministic-effects-and-output-identity`)

- **Determinism contract** (documented on the `Operation` rustdoc): `run`
  / `apply_change` / `compensate` obtain time via `ctx.now()` and
  identifiers via `ctx.stable_id()` — no ambient `SystemTime::now()` /
  `Uuid::new_v4()` / external reads. Verified by
  `OperationTestHarness::replay_emits_equal` (output-side sibling of
  `replay_equals`; injectable fixed/scripted wall clock).
- **Clocks** (`sdk/rust/src/operation/clock.rs`): no static
  `Clock::now()` — the dispatcher evaluates the declared clock once per
  invocation via `ClockFor<Input>` (all `Time = u64`). `WallClock` =
  epoch ms at dispatch entry; `EventTimeFrom<F>` = `F:
  TimeExtractor<Input>` over the current input (compile-enforced);
  `LogicalClock` = the raw durable `change_seq` (re-armed `terminal+1` on
  restore). Build rejects `LogicalClock` + `Change = NoChange`
  (`BuildTopologyError::UnservableClock`).
- **`ctx.stable_id()`**: frozen FNV-1a-128 over
  `segment identity ‖ change_seq ‖ per-invocation ordinal`.
- **Keyed channels**: `.keyed()` (plugin wire required, else
  `KeyedChannelWithoutPluginWire`) + `ctx.emit_keyed(key, value)`; plugin
  frames stamped `flz.segment` (`instance/operation/segment_key`),
  `flz.seq` (`<change_seq>.<emit_ordinal>`, read after flush-before-emit),
  `flz.key`. User `flz.*` metadata rejected (`ReservedMetadataKey`).
  Non-keyed channels byte-identical (guarded by test + criterion bench).
  `flz.seq` is NOT an exact append-dedup handle until
  `effectively-once-output` journals it; bounded operatives reject keyed
  emits (their plugin wire carries no metadata). Controller untouched —
  stamps pass through `StreamChannel` → `Overwatch` `:on_output`.
- **Downstream (not this change)**: flezha_market migration to the ctx
  APIs; Java SDK ctx parity; Kafka producer adopting `flz.key` as message
  key.
- **Docs**: `docs/sdk_protocol_spec.md` "Output identity stamps",
  `docs/resilience/data_durability.md` "Output identity".

## Per-Segment state persistence (`operative-state-persistence`)

- **Modules**: `lib/flezha/state/` — `Registry`, `Wire`, `Metrics`,
  `Outbox`, `PersistenceResolver`, `Supervisor`, `WalWriter`,
  `FsyncBatcher`, `Snapshot`, `Retention`, `Restore`, `TimeTravel`,
  `TestHelpers`.
- **On-disk layout**: `app_data/state/{operative_class}/{operation_id}/{segment_key}.{wal,snap.{seq},outbox.dedup}` (flat — `runner_id` was dropped by `auto-restore-on-boot`; `Flezha.Boot.StatePathMigrator` handles upgrade migration).
- **Wire payload-ids**: `segment_state_change`, `segment_state_snapshot`,
  `delete_segment_state` (data stream); `RestoreInitState`, `RestoreInitStateResponse`,
  `CompactSegment`, `CompactSegmentResponse`, `SegmentMailboxStatus`
  (control channel). See `docs/sdk_protocol_spec.md` "Per-Segment
  State Persistence".
- **Dynamic segmentation (`operative-dynamic-segmenting`)**: SDK-side
  `sdk/rust/src/segment/dynamic.rs` (`DynamicSegmentRegistry` +
  `Reaper`). Dynamic-mode Operations (`operation_key()` overridden, empty
  `operation_keys()`) create Segments lazily per key. Eviction is OFF by
  default (`segment_idle_ttl() -> None`); when a TTL is opted in, the
  per-Operation reaper evicts idle Segments **destructively** — it ships
  `delete_segment_state` (SDK→controller) and the recreate cold-starts
  fresh (no restore-from-disk, by decision 2026-06-16). Controller handler:
  `Flezha.State.Supervisor.delete_segment/1` (stops WalWriter, drops
  FsyncBatcher mark, unlinks files via `Registry.delete_files/1`).
- **Runtime config knobs**: `:state_root` (default `app_data/state`),
  `:agent_fsync_interval_ms` (5 ms), `:wal_high_watermark_bytes`
  (16 MiB), `:wal_low_watermark_bytes` (`high / 2`),
  `:time_travel_retention_check_interval_ms` (60 s),
  `:time_travel_retention_hours` (per-Operative map), `:operative_persistence`
  (per-Operative map), `:default_compact_after_bytes` (8 MiB, env
  `FLEZHA_COMPACT_AFTER_BYTES`; SDK trait default in lockstep),
  `:compact_after_age_ms` (300 s, `0` disables — and is `0` in `:test`;
  env `FLEZHA_COMPACT_AFTER_AGE_MS`).
- **Snapshot baseline durability (`snapshot-baseline-durability`)**: the
  `Watchdog` sweep auto-triggers `CompactSegment` on WAL **size or age**
  (`[:flezha, :state, :compaction_trigger]`); a fresh controller with an
  empty WAL for a live runner's Segment **pulls** a baseline
  (WalWriter awaiting-baseline sheds + one `CompactSegment`,
  `[:flezha, :state, :baseline_seed]`) instead of erroring; the resync
  path never pushes an empty `RestoreInitState`
  (`Restore.has_persisted_state?/1`, zero-byte WAL ≡ no state).
- **Credit-grant flow control (`credit-grant-flow-control`)**:
  runner→controller state-stream back-pressure is credit-based — the
  `StreamChannel` offers `CreditWindowInit` on the first state frame
  (per-Operative `:credit_window_bytes`, default `wal_high_watermark_bytes`;
  kill switch `FLEZHA_CREDIT_FLOW_CONTROL`), debits each frame's wire size,
  and mints one batched `CreditGrant` per fsync-drain below
  `:credit_low_watermark_bytes` (default `window / 2`). SDK windows:
  `sdk/rust/src/segment/credit.rs`, `sdk/java .../state/CreditWindow.java`
  (park in the flusher under the buffer lock; `credit_parked` in
  `data_heartbeat` metadata). Credit sessions get no `SegmentMailboxStatus`;
  shed/overrun on one emits `[:flezha, :state, :credit_anomaly]`. Soak:
  `test/load/credit_soak_test.exs` (`MIX_RUN_CREDIT_SOAK=1`). The
  source-ingestion credit loop is deferred until `effectively-once-output`
  lands (input-log + min-watermark do not exist yet).
- **CLI**: `flz state list <operative>`, `flz state at <operative/operation/key> <seq|ts>`,
  `flz state rotate <operative/operation/key>`, `flz state retention <operative> <hours>`,
  `flz state delete <operative/operation/key>`. Implemented in `cli/src/state.rs`;
  RPCs into `Flezha.Service.CliManager.state_*/N`.
- **Detailed notes**: `docs/resilience/state_persistence.md`.

## Auto-restore on boot (`auto-restore-on-boot` + `consume-recovery-mode-and-self-heal`)

- **ProcessHub dependency**: `process_hub ~> 0.6.0` from hex (the
  `durable-registry`, `coordinator-bootstrap-recovery`, and
  `recovery-mode-opt-in` capabilities shipped in the 0.6.0 release;
  the former GitHub dev-branch pin is gone). `FLEZHA_PROCESS_HUB_PATH`
  still overrides with a local checkout.
- **Modules**: `lib/flezha/boot/` — `StatePathMigrator`, `RecoveryHooks`.
  The `StatePathMigrator` runs synchronously from
  `Flezha.Application.start/2` before `Flezha.State.Supervisor`.
  `Flezha.Boot.WalTransfer` was deleted by
  `consume-recovery-mode-and-self-heal` (cross-controller WAL transfer
  is out of scope — cross-node WAL replication is a future-work non-goal).
- **HubManager wiring**: `Flezha.Service.HubManager.process_hub/1`
  passes `registry_backend: {:dets, [path: ...]}` and `auto_recovery:
  [marker_path: marker_path(hub_id), replay_timeout_ms,
  recovery_timeout_ms]` (the marker path is now folded into the single
  `:auto_recovery` option) to every `ProcessHub.child_spec/1`. After each `start_hub/1` it calls
  `Flezha.Boot.RecoveryHooks.register/1` to install
  `pre_recovery_replay` (thin telemetry forwarder; the upstream
  marker gate queues cluster events for us) and `post_recovery_replay`
  (telemetry). The marker file defaults to
  `<process_hub_registry_root>/<hub_id>/cluster.healthy` on the same
  hostPath as the DETS file so the two never drift.
- **Recovery tracker**:
  `Flezha.Service.HubManager.RecoveryTracker` is a per-supervisor
  GenServer subscribed to `[:process_hub, :recovery, _]` telemetry.
  `HubManager.recovery_state/1` and `await_recovery_complete/1,2`
  expose the per-hub state machine; `Flezha.BootLoader.await_namespace_hubs/0`
  and `Flezha.Runner.FleetManager.init/1` gate registry reads on it.
- **Boot CLI**: `flz boot status`, `flz boot await`,
  `flz boot import-legacy-snapshot` — implemented in `cli/src/boot.rs`
  / `Flezha.Service.CliManager.boot_*`. Cluster prepare-recovery:
  `flz cluster prepare-recovery` (Rust `cli/src/cluster.rs` →
  `Flezha.Service.CliManager.cluster_prepare_recovery/1` →
  `ProcessHub.prepare_recovery_cluster/1`); `flz cluster status`
  now surfaces `recovery_state` and `recovery_armed` per node.
- **Start-fresh workflow**: `flz controller stop && flz reset --yes`
  wipes durable runtime data (`app_data/state/*`, `app_data/process_hub/*`).
  Implemented in `cli/src/reset.rs`; refuses to run while the Controller
  is up. See `docs/operations/reset.md`.
- **Persistence layers**: ProcessHub durable registry at
  `app_data/process_hub/<hub_id>/registry.dets` (cspec + `opts[:plugins]`)
  + per-Segment WAL at `app_data/state/<operative_class>/<operation_id>/...`
  (change-by-change progress) + marker file at
  `app_data/process_hub/<hub_id>/cluster.healthy` (operator gate).
- **Plugin enable/disable** uses cspec-write-first ordering via
  `ProcessRegistry.update/3`; `Flezha.Service.HubManager.update_operative_plugins/3`
  was deleted (the persistent registry covers durability).
- **Runtime config knobs**: `FLEZHA_PROCESS_HUB_RECOVERY_MARKER_PATH`,
  `FLEZHA_PROCESS_HUB_RECOVERY_TIMEOUT_MS` (default 30 s),
  `FLEZHA_AUTO_RESTORE_ENABLED` (default `true`; `false` kills the
  marker gate entirely and reverts to in-memory `:ets`).
- **Removed by `consume-recovery-mode-and-self-heal`**:
  `Flezha.Boot.WalTransfer`; `:pre_replay_fleet_wait_ms` config key;
  `Flezha.Reconciler.{Loop, Actions, Health, Probe, Activity, Leader}`
  (replaced by `Flezha.Convergence.Health`); `Flezha.State.Outbox`;
  `Flezha.Reconciler.Supervisor` (top-level); `FLEZHA_RECONCILER_ENABLED`,
  `FLEZHA_LEADERSHIP_ENABLED`, `FLEZHA_RECONCILE_*` env vars.
- **Detailed notes**: `docs/resilience/auto_restore_on_boot.md`.

## Convergence Health watcher (`consume-recovery-mode-and-self-heal`)

- **Modules**: `lib/flezha/convergence/health.ex` — the single
  controller-side GenServer driving bind/rebind/plugin-restart
  decisions. `lib/flezha/healthz/server.ex` — opt-in `:gen_tcp`
  HTTP/1.1 `/healthz` listener following the
  `Flezha.Observability.Export.PrometheusHttp` pattern.
- **Supervision tree**: `Flezha.Convergence.Health` is inserted in
  `Flezha.Application.start/2` AFTER `Flezha.State.Watchdog`;
  `Flezha.Healthz.Server` is inserted right after, gated on
  `:healthz_enabled` (default `false` — flezha_market opts in via
  its ConfigMap).
- **Behaviour**: one tick per `convergence_tick_ms` (default 5_000).
  On init: `HubManager.await_recovery_complete/1` blocks emissions
  until every per-namespace hub reaches `:normal | :skipped`. On
  every tick: walk `HubManager.get_all_operatives/1` per running
  namespace, classify each operative as `:healthy | :degraded | :failed
  | :unbound`, emit at most one action per operative (`:bind`,
  `:rebind`, `:restart_plugin`, `:noop`).
- **Output-stall → forced rebind**: subscribes to
  `[:flezha, :state, :output_stall]` (emitted by
  `Flezha.State.Watchdog`); on receipt, queues a `:rebind` with
  exponential back-off `5s/30s/60s/300s`. Single signal closes the
  silent-freeze failure class.
- **Plugin escalation**: `handle_plugin_failure/2` tracks strikes
  per operative-plugin and escalates to an operative restart at
  `convergence_plugin_strike_threshold` (default 2). Emits
  `[:flezha, :convergence, :plugin_restart]` and
  `[:flezha, :convergence, :plugin_escalated]` telemetry.
- **/healthz contract** (when `FLEZHA_HEALTHZ_ENABLED=true`):
  `GET /` → `pong\n`; `GET /healthz` → `200 healthy`,
  `200 recovering` (kubelet must not restart during planned
  recovery), or `503 unhealthy` with `reason ∈ tick_stale |
  output_stalled | recovery_failed`. Unknown paths → 404; non-GET
  → 405; malformed → 400. Bind defaults to `127.0.0.1`.
- **Runtime config knobs**: `FLEZHA_USE_CONVERGENCE_HEALTH`
  (default `true`), `FLEZHA_CONVERGENCE_TICK_MS` (default 5_000),
  `FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS` (default 60_000),
  `FLEZHA_CONVERGENCE_PLUGIN_STRIKE_THRESHOLD` (default 2),
  `FLEZHA_HEALTHZ_ENABLED` (default `false`),
  `FLEZHA_HEALTHZ_PORT` (default 9569).
- **flezha_market deployment**: `kubernetes/flezha-daemonset.yaml`
  ships a `livenessProbe: httpGet /healthz:9569` block;
  ConfigMap sets `FLEZHA_HEALTHZ_ENABLED=true` and the marker path.
  `kubernetes/prepare-recovery.sh` wraps
  `flz cluster prepare-recovery` for the operator-driven cold-boot
  workflow.
- **Fresh-process runner takeover**: phase 5 of the change keeps
  the takeover on the same self-healing loop instead of bolting on
  a parallel path. `FleetManager.handle_call(:runner_connect, ...)`
  dispatches `:runner_connected_event` and returns; nothing else.
  `Flezha.Convergence.Health` subscribes to that event via
  `EventManager.add_handler/1` and runs an immediate tick. The
  existing `:rebind_if_runner_available` action body — already
  responsible for migrating operatives whose `bound_runner` is no
  longer active — flips the registry tag via
  `HubManager.operative_set_runner/3` and seeds the new runner's
  pre-spawned `StreamChannel.operative_mapping` via
  `HubManager.hook_insert_pid/2` (the same fan-out the
  `child_registered` hook uses), then emits
  `[:flezha, :runner, :reconnected_reassigned]` +
  `[:flezha, :runner, :reconnected]` (mode: `:reassigned`,
  reason: `:convergence_rebind`) per reseated operative. The existing
  back-off (5s/30s/60s/300s) carries the retry semantics — a
  failed Overwatch migrate leaves the registry untouched and the
  operative stays `:degraded` for the next tick.

## Data-plane liveness instrumentation (`data-plane-liveness-instrumentation`)

- **Purpose**: localize a silent `io_in`/output stall to one layer.
  PURE instrumentation — zero data-plane behaviour change, no
  remediation. Closes the 22:14 candle incident's "which layer
  wedged?" gap (BEAM channel vs NIF driver thread vs runner).
- **NIF driver self-report**: `flz_nif/src/driver_stats.rs` —
  `DriverStats{loop_iterations, last_loop_ms, cmd_queue_depth:
  AtomicU64; streams: Mutex<HashMap>}` shared `Arc` into the
  flz-uds-driver loop (`uds_driver.rs` bumps the heartbeat once per
  iteration, rebuilds the per-stream snapshot once per iteration —
  never per frame). `driver_stats/1` NIF (in `server.rs`, registered
  via auto-discovery `init!`) returns the map; `:undefined` for QUIC
  (`TransportServer.stats: Option<Arc<DriverStats>>`).
- **BEAM counters**: `lib/flezha/transport/liveness_counters.ex` —
  always-on `:counters`/`:atomics` + public ETS registry (mirrors
  `Observability.Counters`). `StreamChannel` bumps `io_in_received /
  send_frame_calls / nil_drops / flz_data_received / last_send_ms`;
  `Operative.Overwatch` bumps `io_in_received / forwarded / filtered_nil`.
  Dead-owner entries pruned on `snapshot_all/0`.
- **Metric emission**: `Transport.Server.get_stats/1` merges
  `Native.driver_stats`; `OtpBackend.TransportSource` (the
  `:flezha_transport` source) emits per-transport driver metrics +
  flat global BEAM aggregates (bounded cardinality — classifier does
  per-entity localization).
- **Stall classifier**: `lib/flezha/convergence/stall_classifier.ex`
  — pure `classify(current, prev, opts)` returning one of
  `driver_loop_frozen | nif_queue_backing_up | socket_write_blocked |
  runner_not_consuming | channel_not_sending | operative_not_forwarding
  | :ok` (deepest-wedged-layer-wins; each verdict has an "is it being
  fed?" advancing-delta precondition). `Convergence.Loop` builds the
  snapshot per tick and emits `[:flezha, :data_plane,
  :stall_localized]` (with `%{layer: …}`) on verdict change —
  detection only, feeds NO heal; bridged to the bus via
  `TelemetryBridge`.
- **Runtime config knob**: `:observability, :data_plane_liveness`
  (default `true`, env `FLEZHA_DATA_PLANE_LIVENESS`) gates the ~1 Hz
  poll + classifier; counters stay always-on.
- **Ops fallback** (docs, not built): sample
  `/proc/<beam_pid>/task/*/` for `flz-uds-driver` threads in `D`/frozen
  `utime` (`bin/flzvm pid`).
- **Detailed notes**: `docs/resilience/transport_liveness.md`.

## Graceful shutdown (`controller-graceful-shutdown`)

- **Modules**: `lib/flezha/shutdown/` — `Coordinator`, `SignalHandler`.
  Coordinator drives `quiesce → drain → finalise`; SignalHandler
  installs OS handlers via `:os.set_signal/2` so SIGTERM/SIGINT
  trigger graceful shutdown in every BEAM run mode.
- **Supervision tree**: SignalHandler is the **first** child in
  `Flezha.Application.start/2` (handlers installed before any
  service can observe a delivered signal); Coordinator is inserted
  after `Flezha.Service.HubManager` (drain queries the hub).
  `prep_stop/1` calls `Coordinator.shutdown(:prep_stop)`.
- **Hot path gating**: `Coordinator.shutting_down?/0` is an ETS-backed
  flag (no GenServer hop). Read by `OperativeManager.start_operative/2` and
  `FleetManager.connect_handler/4`.
- **Runtime config knobs**: `:graceful_shutdown_enabled` (default
  `true`, env `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED`),
  `:graceful_shutdown_timeout_ms` (default `60_000`, env
  `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`). **Pairing rule**: orchestrator
  grace period (`terminationGracePeriodSeconds` /
  `stop_grace_period`) MUST be `(timeout_ms / 1000) + 15s`. Bump in
  lockstep.
- **CLI**: `flz controller stop` (drives the coordinator over the
  CLI socket — works in release/mix/iex), `flz controller status`
  (surfaces `shutdown_in_progress`, `shutdown_phase`,
  `signal_handler_status`). Implemented in `cli/src/controller.rs` /
  `Flezha.Service.CliManager.controller_{stop,status}`.
- **Telemetry**: `[:flezha, :shutdown, :requested|:phase_started|:phase_completed|:drain_progress|:timeout|:complete]`.
- **Test config**: `config/test.exs` sets
  `:graceful_shutdown_enabled` to `false` so `Application.stop/1`
  teardown remains fast.
- **Detailed notes**: `docs/resilience/graceful_shutdown.md`.

## Observability foundation (`platform-observability-foundation`)

- **Modules**: `lib/flezha/observability/` — `Signal`, `Source`,
  `Sink` (behaviours), `Registry`, `Bus`, `RingBufferSink`,
  `PubSubSink`, `LoggerHandler`, `TelemetryBridge`, `SinkSupervisor`,
  `SourceSupervisor`, `Supervisor`, `Counters`, `DefaultSinks`. The
  service surface lives at `lib/flezha/service/observability.ex`.
- **Supervision tree**: `Flezha.Observability.Supervisor` is inserted
  in `Flezha.Application.start/2` AFTER `Flezha.Service.HubManager`
  and BEFORE `Flezha.Shutdown.Coordinator`. Children, in start
  order: `Registry` → `Phoenix.PubSub` (server name
  `Flezha.Observability.PubSub`) → `SinkSupervisor` → `SourceSupervisor`
  → `LoggerHandler` → `TelemetryBridge` → `DefaultSinks` (one-shot
  worker that registers `:ring_buffer` and `:pubsub` sinks).
- **Signal envelope**: one struct, four kinds — `:metric`, `:event`,
  `:log`, `:trace_span`. Constructor `Flezha.Observability.Signal.new/1`
  validates kind / atoms / map shapes and auto-fills monotonic `ts`.
- **Bus**: `Flezha.Observability.Bus.emit/1` runs in the caller, never
  blocks under default `:drop` overflow policy, checks per-source
  cardinality on `:metric` signals only, and wraps the dispatch body
  in `try/rescue/catch` so a broken sink cannot crash a producer.
- **Default sinks**: `RingBufferSink` (per-source ETS `:ordered_set`
  with TTL'd retention; queries run in the caller process) and
  `PubSubSink` (per-source topic `flezha.observability.source.<src>`,
  disabled by default in test config).
- **Bridges**: `LoggerHandler` installs an OTP `:logger` handler that
  routes log records into the bus as `:log` signals from the `:logger`
  source. `TelemetryBridge` attaches a single `:telemetry.attach_many/4`
  handler that lifts the known `[:flezha, ...]` events onto the bus
  without modifying any producer call-site.
- **Service API**: `Flezha.Service.Observability` exposes `subscribe/2`,
  `query/2`, `list_sources/1`, `list_sinks/1`, `enable_source/2`,
  `disable_source/2`. Every call takes a `caller_context` (`:source`
  in `[:cli, :ui, :sdk, :internal]`, optional `:principal`, optional
  `:request_id`). Mutating calls emit audit `:event` signals on
  `:observability_service` — the seam `platform-audit-log` will
  consume. Thin CLI wrappers live in `Flezha.Service.CliManager`
  (`observability_list_sources/1`, `_enable_source/2`, etc.).
- **Runtime config knobs**: `:observability` map — `:enabled` (default
  `true`, env `FLEZHA_OBSERVABILITY_ENABLED`), `:ring_buffer_retention_ms`
  (`900_000`), `:ring_buffer_max_entries_per_source` (`10_000`),
  `:overflow_policy` (`:drop`), `:cardinality_budget_per_source`
  (`1_000`), `:pubsub_sink_enabled` (`true` in prod, `false` in test).
- **Counters**: `Flezha.Observability.Counters` holds atomic
  `:dispatcher_failure_count`, `:logger_bridge_failures`,
  `:telemetry_bridge_failures` plus a public ETS table used to
  de-dup "log once per unique error class".
- **Bench**: `mix flezha.bench.observability [iterations]` boots
  just the observability supervisor (no Controller / runners) and
  reports nanoseconds per emit against zero sinks.
- **Detailed notes**: `docs/observability_foundation.md`.

## OTP backend sources (`platform-observability-otp-backend`)

- **Modules**: `lib/flezha/observability/otp_backend/` —
  `Supervisor`, `BeamVmSource`, `HostSource`, `ProcParser`,
  `ProcessHubSource`, `TransportSource`, `BoundedMailboxSource`.
- **Supervision tree**: `Flezha.Observability.OtpBackend.Supervisor`
  is inserted in `Flezha.Observability.Supervisor` AFTER
  `DefaultSinks` and BEFORE `Export.Supervisor`. `:one_for_one` so a
  per-source crash leaves peers running.
- **Shipped sources** — `:beam_vm` (memory / schedulers / GC /
  process+port counts, 1s default), `:host` (Linux `/proc` RSS /
  CPU / FD / loadavg, 5s default; one-shot `:host_source_degraded`
  event on non-Linux), `:processhub` (per-hub registered processes +
  pending recoveries, 1s), `:flezha_transport` (per-transport
  pending-stream count via `Flezha.Transport.Server.get_stats/1`,
  1s), `:flezha_bounded_mailbox` (registered, one-shot
  `:bounded_mailbox_unavailable` event until the Rust→Elixir mailbox
  surface lands).
- **Runtime config knobs**: `:observability, :otp_backend_enabled`
  (default `true` in prod, `false` in test); `:observability,
  :otp_backend, :poll_intervals` map per-source override.
- **CLI**: `flz observe sources`, `flz observe sinks`,
  `flz observe tail <source>`, `flz observe enable <source>`,
  `flz observe disable <source>` — implemented in
  `cli/src/observe.rs` and
  `Flezha.Service.CliManager.observability_render_*`.
- **Detailed notes**: `docs/observability_otp_backend.md`.

## Observability export (`platform-observability-export`)

- **Modules**: `lib/flezha/observability/export/` — `Supervisor`,
  `PrometheusRegistry`, `PrometheusSink`, `PrometheusHttp`,
  `OtlpCodec`, `OtlpClient`, `OtlpSink`.
- **Supervision tree**: `Flezha.Observability.Export.Supervisor` is
  the last child of `Flezha.Observability.Supervisor`, started AFTER
  the OTP backend. Registers the declarative
  `:observability_export` source on init so the exporters'
  operational events participate in bus fan-out.
- **Prometheus**: hand-rolled `:gen_tcp` HTTP/1.1 listener (~250
  LOC, no Cowboy/Plug) at default `127.0.0.1:9568/metrics`. Accepts
  only `GET /metrics` and `GET /`; everything else returns 404 / 405
  / 400. Type inferred from `Signal.measurements` keys (`:count` →
  counter, `:value`/`:gauge` → gauge, `:duration_us`/`:bytes` →
  histogram); `metadata.prometheus_type` overrides. Cardinality
  enforcement aggregates over-budget label values into a synthetic
  `{__truncated__="true"}` series and emits
  `:prometheus_label_truncated` events.
- **OTLP**: OTLP/HTTP via `:mint` (top-level dep, was transitive
  via `:mint_web_socket`). Hand-rolled protobuf encoder in
  `OtlpCodec` matches OpenTelemetry v1 schema for metrics / logs /
  traces. Batched with size + time triggers (`:otlp_batch_max` 512,
  `:otlp_flush_interval_ms` 10s). Overflow policy `:block` —
  producer back-pressure during OTLP outages, bounded by
  `mailbox_watermark × sum-of-backoffs`. Backoff: 250/500/1000/2000/4000
  ms, max 5 attempts.
- **Runtime config knobs**: `:observability_export` map with
  `:prometheus_enabled` (default `false`), `:prometheus_port`
  (`9568`), `:prometheus_bind` (`"127.0.0.1"`), `:otlp_enabled`
  (`false`), `:otlp_endpoint`, `:otlp_headers` (map for bearer
  tokens), `:otlp_flush_interval_ms`, `:otlp_batch_max`. Env-var
  overrides: `FLEZHA_PROMETHEUS_ENABLED`, `FLEZHA_PROMETHEUS_PORT`,
  `FLEZHA_OTLP_ENABLED`, `FLEZHA_OTLP_ENDPOINT`.
- **Registry extension**: `Flezha.Observability.Registry.register_external_sink/1`
  lets exporters register a sink whose pid is owned by a different
  supervisor — required because `Export.Supervisor` owns the
  Prometheus and OTLP sinks, not the foundation `SinkSupervisor`.
- **Detailed notes**: `docs/observability_export.md`.

## Durable audit log (`platform-audit-log`)

- **Modules**: `lib/flezha/observability/audit_log/` — `Codec`,
  `Index`, `Reader`, `Writer`, `Rotation`, `Retention`, `Sink`,
  `Supervisor`. Service surface at
  `lib/flezha/service/audit_log.ex`.
- **Supervision tree**: `Flezha.Observability.AuditLog.Supervisor`
  is the LAST child of `Flezha.Observability.Supervisor` (gated on
  `:audit_log, :enabled`, default `false`). Children, in start
  order: `Writer` → `Rotation` → `Retention` → `Sink`. The Sink is
  last so subscriptions go live only after the Writer is ready to
  accept records.
- **Default allow-list**: `:observability_service`, `:flezha_shutdown`,
  `:flezha_plugin_installer`, `:audit_log` (audit-of-audit). Extend
  via `:audit_log, :extra_subscriptions`.
- **On-disk layout**: `app_data/audit/INDEX` (binary catalog),
  `app_data/audit/audit-current.jsonl` (active file),
  `app_data/audit/audit-YYYYMMDDTHHMMSSZ-N.jsonl[.gz]` (rotated).
  `INDEX` is rebuildable on boot if missing or corrupt.
- **JSON Lines record**: stable field order `ts_us, ts_iso, source,
  name, measurements, metadata, caller`; `caller` is denormalised
  from `metadata.caller_source / :principal / :request_id`.
- **Fsync**: `:per_record` (default, ~1-10 ms/record on local SSD)
  or `:batched` (every `:fsync_interval_ms`, bounded loss window).
  Overflow policy `:block` so audit emits are never silently
  dropped.
- **Rotation**: size (`:max_file_bytes`, 64 MiB) OR age
  (`:max_file_age_seconds`, 1 day), checked every
  `:rotation_check_interval_ms` (60 s) and on every write.
- **Retention**: age (`:retention_seconds`, 30 d) AND total size
  (`:max_total_bytes`, 4 GiB) — more restrictive wins. Active file
  is never deleted by retention.
- **Service API**: `Flezha.Service.AuditLog` —
  `list_files/1`, `read/2`, `tail/2`, `unsubscribe/2`, `stats/1`,
  `rotate/1`. Every public call emits an audit-of-audit event on
  `:audit_log` source. Master flag check returns
  `{:error, :observability_disabled}`; subsystem flag check returns
  `{:error, :audit_log_disabled}`.
- **CLI**: `flz audit list`, `flz audit read <file>`,
  `flz audit tail`, `flz audit stats`, `flz audit rotate` —
  implemented in `cli/src/audit.rs` and
  `Flezha.Service.CliManager.audit_log_render_*`.
- **Shutdown integration**: `Flezha.Shutdown.Coordinator.do_finalise`
  calls `Writer.close/0` BEFORE the `FsyncBatcher` flush, bounded
  by a 5 s timeout. Post-close signals are dropped with a per-sink
  counter increment.
- **Runtime config knobs**: `:audit_log` map (see
  `docs/audit_log.md` for the full list). Env-var overrides:
  `FLEZHA_AUDIT_ENABLED`, `FLEZHA_AUDIT_DIR`,
  `FLEZHA_AUDIT_RETENTION_DAYS`.
- **Detailed notes**: `docs/audit_log.md`.

## Platform authn/authz foundation (`platform-authn-authz`)

- **Modules**: `lib/flezha/authz/` — `Action`, `Role`, `Principal`,
  `Namespace`, `Scope`, `Policy`, `Config`, `Store`, `AuthState`,
  `RoleStore`, `NamespaceStore`, `TokenStore`, `QuotaCounters`,
  `Quotas`, `Bootstrap`, `Bootstrap.OnBoot`, `SourceRegistration`,
  `Supervisor`; top-level `Flezha.AuthZ` exposes `authorize/3`,
  `require_bound_principal/1`, `enabled?/0`. Cert side lives at
  `lib/flezha/authn/cert.ex` (`principal_from_peer/1` +
  `namespace_from_peer/1`, RFC 4514 normalised DN / SPIFFE URI SAN).
- **Supervision tree**: `Flezha.AuthZ.Supervisor` is inserted in
  `Flezha.Application.start/2` AFTER `Flezha.Observability.Supervisor`
  and BEFORE `Flezha.Shutdown.Coordinator`. Children, in order
  (`:rest_for_one`): `AuthState` → `RoleStore` → `NamespaceStore`
  → `TokenStore` → `QuotaCounters` → `SourceRegistration` (one-shot
  `:platform_authz` source registration) → `Bootstrap.OnBoot`
  (boot-time token writer + refuse-to-start gate). Gated by
  `:authn_authz, :application_managed` (default `true`; tests flip
  to `false` so they can bring up isolated stores against a tmp
  `auth_dir`).
- **On-disk layout**: `app_data/auth/` (mode `0700`) —
  `state.dets` (durable `:enabled` flag),
  `roles.dets` (principal → {namespace, role}),
  `namespaces.dets` (catalog + quotas),
  `tokens.dets` (SHA-256-hashed registry),
  `quota_counters.dets` (per-namespace counters),
  `bootstrap.token` (one-time, mode `0600`, unlinked on claim).
  Persistent-state tree under `app_data/state/_system/...` after the
  one-shot namespace migration in `Flezha.Boot.StatePathMigrator`.
- **Policy point**: `Flezha.AuthZ.authorize(caller_context, action,
  scope) :: :ok | {:error, {:forbidden, reason}}`. Four roles:
  `viewer`, `operator`, `admin`, `runner`. Role→action map is
  compile-time in `Flezha.AuthZ.Policy`. Auth-state mutations
  (`:authz.*`) are additionally gated to `admin` on the bound
  namespace; only `admin@_system` may target other namespaces.
- **Tokens**: wire form `flzt_<base32>` (256-bit); storage form
  SHA-256 hash. Raw value returned to caller exactly once at
  `TokenStore.issue/5`. Revocation is instant
  (`TokenStore.revoke/2` sets `revoked_at`; `verify/1` rejects).
  Mutations emit `:token_issued` / `:token_revoked` on
  `:platform_authz`.
- **Quota engine**: `Flezha.AuthZ.Quotas.check_allocation/3` —
  one ETS lookup per call (NamespaceStore cache + QuotaCounters
  cache). `record_allocation/3` / `record_deallocation/3` apply
  async. `OperativeManager.start_operative/2` enforces `max_operatives` /
  `max_segments_per_operative` at allocation time and records on
  success; `stop_operative/2` decrements on stop. Mailbox / disk /
  runner enforcement points land in the namespace-state
  follow-up.
- **Bootstrap**: on first boot when no `admin@_system` binding
  exists AND `:enabled` is `true`, `Bootstrap.OnBoot` writes
  `app_data/auth/bootstrap.token` (mode `0600`) after verifying the
  parent dir's mode (aborts with `error` on group/other-write
  bits). `flz auth bootstrap <token>` (Elixir wrapper:
  `Flezha.Service.CliManager.authz_bootstrap/2`) claims it,
  resolves the principal (cert SAN > cert DN > `--principal` >
  freshly-minted admin token), binds `admin@_system`, flips
  `AuthState.enabled` durably, unlinks the token file, emits
  `:bootstrap_completed`.
- **CLI surface (Elixir-side wrappers ready; Rust verbs deferred)**:
  `Flezha.Service.CliManager.authz_{bootstrap,issue,revoke,list,whoami}`,
  `namespace_{create,delete,list,show,set_quota}`,
  `role_{bind,unbind,list}`. Each wraps in `AuthZ.authorize/3` and
  emits a `:platform_authz` audit event on mutating verbs.
- **Audit log integration**: `:platform_authz` in the default
  subscription allow-list (`Flezha.Observability.AuditLog.Sink`);
  JSONL records gain a top-level `namespace` field (defaults to
  `"_system"` when absent on pre-upgrade rows); `caller.principal`
  is the verified principal when `:enabled` is `true`, else the
  caller-source atom (preserving pre-change behaviour).
- **Compatibility mode**: `:authn_authz, :enabled` defaults `false`
  in `config/runtime.exs` and `config/test.exs`. While `false`,
  `AuthZ.authorize/3` returns `:ok` unconditionally and
  `require_bound_principal/1` is a no-op — every existing service
  caller continues to work without producing a verified principal.
  Flips to `true` durably on `Bootstrap.claim/2` success;
  `FLEZHA_AUTHZ_ENABLED=false` is an explicit opt-out that emits a
  `warn` on every boot.
- **Runtime config knobs**: `:authn_authz` map —
  `:enabled` (default `false`, env `FLEZHA_AUTHZ_ENABLED`),
  `:auth_dir` (default `app_data/auth`, env `FLEZHA_AUTHZ_DIR`),
  `:bootstrap_token_path` (default `<auth_dir>/bootstrap.token`,
  env `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH`),
  `:token_ttl_default_seconds` (`86_400`),
  `:sdk_token_ttl_seconds` (`2_592_000`),
  `:namespaces` (map of per-namespace quota overrides),
  `:application_managed` (default `true`; tests set `false`).
- **Deferred to follow-ups**: transport-layer cert wiring + CLI
  socket handshake + Rust CLI verbs
  (`platform-authn-authz-transport`); namespace prefix through
  State subsystem + ProcessHub registry + remaining service-module
  refactors + remaining quota points
  (`platform-authn-authz-namespace-state`); operator guide and
  multi-tenant integration suite
  (`platform-authn-authz-docs-and-e2e`).
