# Multi-input procedures (`add-multi-input-procedures`)

- **`c.merge::<I>()` / `c.merge(I.class)`** — subscribe two or more source
  channels to ONE procedure whose `Input` is an author-declared sum type, each
  `.from(source, map)` supplying the mapping. Entry streams and inter-proc
  streams are both sources on a STREAM operative, freely mixed (workflow
  takes inter-proc only — see below). Single-input `.into` is unchanged.
- **The whole feature is ONE field on the consumer edge.**
  `ChannelDescriptor.consumers` went from `Vec<ProcedureId>` to
  `Vec<ConsumerEdge>` (today `procedure` + a composed `transform` + optional
  `key` closure, since `stream-dsl-keyby-and-transforms`); `consumer_ids()` /
  `consumerIds()` is the id-only view the graph walks and the workflow DAG use.
  No wire change, no controller change, no journal-format change.
- **GOTCHA — map at the ROUTING seam, not the delivery seam.** The mapping is
  applied in `flz_core::routing::route_to_consumers` (both languages; Java
  evaluates `ConsumerEdge.transformValue` producer-side in
  `ActivationDispatcher`), which is the one path both live
  delivery and the controller's re-forwarded replay slice travel. That is why
  replay determinism cost zero code: frames are still journaled per source
  channel, and the slice re-maps identically. Mapping later (inside dispatch)
  would have forked live and replay.
- **`entities_for` / the edge `.key_by` see the MAPPED value** — they run
  after the edge map (`entity_key()` no longer exists), so per-source entity
  mode is encoded in the enum, not per edge.
- **Workflow works over INTER-PROC channels only**: `ChainCtx::edge` +
  `extend_mapped` apply the edge map where the driver accumulates a producer's
  batch into its child AND where the resume path re-seeds from the journaled
  handoff. The chain is the shared core's, so Java runs the SAME driver over
  JNI — nothing is mirrored. Without this a merge
  in a workflow chain would downcast-panic.
- **GOTCHA — a merge mapping is the ONE author closure on a workflow chain that
  runs on the DRIVER thread**, not inside an entity invocation, so it is the one
  the existing `catch_unwind`s miss. The core's `extend_mapped` contains it
  (one site, both languages) into the chain's existing abort-and-compensate
  path; without that an
  unwind skips `emit_workflow_completed` and the instance is stranded with no
  terminal frame. No retry — a mapping is pure by contract, so a re-run panics
  identically. Stream is already covered on the inter-proc side (the producer's
  invocation guard); a stream ENTRY source is unguarded, exactly as the
  entry parser on that same wire-reader seam already is.
- **GOTCHA — the consumer-release block must sit AFTER the intent markers, the
  stage record and the proc-commit barrier.** That ordering is load-bearing now
  that a mapping can throw there: release-first leaves the producer whose edge
  blew up out of `completed` AND off disk (no intent markers), so it is never
  compensated and a crash mid-abort cannot recover it either. Java had it
  release-first and leaked exactly that (comp log `[undo-a]` instead of
  `[undo-b, undo-a]`); today both languages run the core's single
  `run_iteration`. The abort scope is
  "stages recorded as completed" — a Rust sibling still in flight in the same
  level is NOT undone, which is the pre-existing mid-level abort shape (a failed
  `commit` at the same spot behaves identically), not a merge-specific hole.
- **The former Java resume-compensation limitation is CLOSED by construction.**
  The two mapping sites (forward accumulate, crash-resume re-seed) used to have
  split undo scopes: Rust seeded the resumed iteration's `completed` from the
  journal while Java's mirrored `runIteration` started empty, so a resumed Java
  chain compensated only what re-ran. Since the poll-core rehost both SDKs go
  through the core's single seeding path (`driver.rs` `journaled_stages` →
  `completed`), so the journaled-committed procedures are unwound identically.
  (Separately, a Java diamond-workflow resume e2e regression is open as of
  2026-08-17 — a different defect, proven by a jar swap.)
- **GOTCHA — a workflow merge over an ENTRY channel is refused, not supported.**
  The workflow seed (`driver.rs seed_bytes`, fed by the spawn args and by
  `PerInput` inbox items) is a bare payload naming no channel, so no mapping
  could be selected and every entry channel past the first is unreadable —
  `build_dag` would happily accept the shape (`entry.consumers.len() == 1`
  holds) and every invocation would then fail the input-type check. Hence
  `BuildWorkflowError::MergeFromEntryChannel` / the same name in Java's
  `WorkflowSpec`. Stream is unaffected: the controller stamps
  `metadata["channel"]` there, which is exactly what the workflow seed lacks.
- **Refusals**: `TopologyIssue::MergeTooFewSources` / `DuplicateMergeSource` /
  `EdgeAlreadyWired` (both SDKs, same names) + the workflow
  `MergeFromEntryChannel`. Mapped-type mismatch is a COMPILE error in Rust
  (`Procedure<Input = I>` bound on `.into::<Proc>()`) and a registration error in
  Java (`wireConsumer` compares the declared merge type, not the channel
  payload type) — the standard erasure asymmetry.
- **GOTCHA — `attach_consumer` must not silently drop a mapping.** It dedups by
  consumer id, so a second declaration on an existing edge used to keep the
  first mapping (or none) and deliver the wrong value forever, with nothing
  said. It now refuses whenever either side carries a mapping;
  mapping-free re-declaration stays the legal fan-in / repeated-`.into` no-op.
  A duplicated source reports ONLY `DuplicateMergeSource` — `Merge` counts
  `declared` (every `.from` written), not surviving sources, so the arity check
  no longer also blames the author for declaring too few.
- **GOTCHA — a Java test's instance id IS the 32-byte wire payload-id.**
  `factoryId + "-instance"` truncates and the frame finds no operative
  (`dispatch: operative instance ... not found`). Use the bare factory id.
- **GOTCHA — `mvn test -pl flezha-sdk-runtime` without `-am` resolves
  `flezha-sdk-api` from the stale installed jar**, so a new api class throws
  `NoClassDefFoundError`. Always `-am` after touching the api module, and
  rebuild the committed dist jars (`mvn clean` first — the fat jar shades stale
  `target/classes`).
- **Tests**: `sdk/rust/tests/merged_input.rs` (its own `[[test]]` entry,
  `required-features = ["bench-internals"]`; the three entry-merge cases share
  one static observer and serialize on `tuned_test_guard()`),
  `E09_MergedInputTest.java` (`MergedInputTest.java`, `WorkflowKindTest` and
  the Java workflow fixtures went with the kernel mirror — only the Rust
  halves survive). The workflow half is the
  diamond fan-in re-authored as a merge — `MergeWorkflow` in
  `workflow_driver_scenarios` —
  run forward AND resumed from a journal, which is what covers the
  accumulate and re-seed mapping sites; the entry-source refusal is pinned in
  `dag_topology_is_validated`. `PanickingMergeWorkflow` /
  `ThrowingMergeWorkflowOperative` pinned the containment — both were confirmed to
  FAIL (driver thread gone, `drain` times out / the completion latch never
  counts down) with the guard removed, and both assert the EXACT undo list on the
  FORWARD site (`undo-b` then `undo-a` — Rust's literals carry the output too,
  a fixture-format difference) so a re-divergence in release ordering is caught
  rather than assumed away. Both fixtures are ALSO driven through the
  crash-resume re-seed site via the resume harness + a journal, asserting
  containment and the terminal report; Rust additionally asserts the journaled
  stages were undone in reverse, read off the compensation-DONE markers (the
  comp log is empty there — `diamond_op!` logs per completed output and a
  journal-rebuilt `EntityRun` has none, a trap worth remembering). The Java
  arm of that assertion went with the kernel mirror; the limitation it pinned
  is closed (see above).
- **`sdk/rust/public-api.txt` is part of "done" for any SDK-surface change**
  (`sdk/rust/README.md` "Public API workflow"). It is human-curated commentary,
  not `cargo public-api` output, so it is hand-edited — and a CHANGED signature
  (here `ChannelDescriptor::consumers`) must be recorded, not just additions.
- **Docs/examples**: `docs/sdk/multi_input_procedures.md`, `docs/glossary.md`
  ("Merge"), `sdk/rust/examples/merged_input.rs`, Java `E09_MergedInput`.

