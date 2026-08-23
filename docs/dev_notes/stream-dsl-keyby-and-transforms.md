# Stream transforms + edge keying (`stream-dsl-keyby-and-transforms`)

- **`map` / `filter` / `flat_map` / `key_by` (Rust) — `map` / `filter` /
  `flatMap` / `keyBy` (Java)** on BOTH stream handles. Transforms are closures
  on a `ConsumerEdge`, not Procedure nodes: no Entity, no mailbox, no dispatch
  hop. **BREAKING — `Procedure::entity_key` + `ENTITY_KEY_OVERRIDDEN` /
  `entityKey` + `entityKeyOverridden` are DELETED, no shim.** Dynamic keying is
  `.key_by(f)` on the wiring, full stop.
- **Fusion is STRUCTURAL, not an optimizer pass.** Every transform declared
  before the closing `.into` is composed into ONE stored closure
  (`BuilderState::pending` → `extend_pending` → `compose`), so the runtime
  applies at most one transform + one key per edge and never learns a chain
  existed. That is why there is no `disableChaining()` to write: `.into` IS the
  boundary. The Rust test asserts it by COUNTING traversals (3 inputs → 3
  predicate calls), not by inspecting the topology.
- **GOTCHA — same seam as the merge mapping, and that is the whole design.**
  Closures are evaluated in `flz_core::routing::route_to_consumers`, the ONE
  path live delivery and the controller's re-forwarded replay slice both
  travel (Java has no `routeToConsumers` any more — it evaluates edges
  producer-side in `ActivationDispatcher.collectEdgeOutputs` and re-runs
  symmetrically at delivery in `valuesForEntity`, because only bytes cross
  the core), so replay determinism cost zero code — exactly as
  `add-multi-input-procedures` found. A merge mapping is now literally one link
  of an edge transform chain (`erase_merge_map` builds an `EdgeYield::One`).
- **`EdgeTransformFn` takes `&Arc<dyn Any + Send + Sync>`, not `&dyn Any`** —
  that is what lets a surviving `filter` re-yield `One(Arc::clone(v))` (a
  refcount bump) instead of cloning the payload. `map`/`flat_map` still need
  `T: Clone` (the author writes `Fn(T) -> U`), matching the merge precedent.
- **Entity mode is read off the WIRING now.** `build_topology` collects
  `keyed_consumers` from the channel edges BEFORE the descriptor loop consumes
  them (that loop moves `entry.consumers`), then `mode = (entity_keys.is_empty(),
  keyed)`. Java mirrors it with a `dynamic` flag set during validation and
  carried on `ProcedureDescriptor` (replacing `entityKeyOverridden()`).
- **GOTCHA — a keyed edge into a procedure ALSO reached by an unkeyed edge is a
  hole the spec's own totality rule closes.** `UnkeyedEdgeIntoDynamicProcedure`
  refuses it; without it that input has no Entity and would silently vanish.
  Not in the proposal's refusal list — added because "the key function is total,
  so there is no keyless-drop path" cannot otherwise hold.
- **GOTCHA — a transform declared AFTER `key_by` is refused
  (`TransformAfterKeyBy`).** The key runs on the FINAL post-transform value
  (design D4), so a later transform would hand it a type it was never written
  against — a downcast panic at routing time instead of a build error.
- **`deliver_edge` (`flz_core/src/routing.rs`) is the ONE place edge closures
  become mailbox pushes** — live routing AND the retry ladder's re-attempt both
  call it, so a re-attempt cannot deliver differently from the attempt it
  retries (the old Java `deliverEdge` is gone; the host push seam is the
  core's `deliver_host_output`).
  A transform-free edge takes its `None` / `null` arm and allocates nothing
  (`transform_value` returns `Option`, `transformValue` returns `@Nullable`) —
  that is what keeps the "costs nothing when unused" promise true.
- **Containment: `ConsumerEdge::transform_value` / `key_of` catch_unwind ONCE**,
  and both seams read that one result. Stream → `spawn_edge_ladder` →
  `run_retry_park_ladder` (extracted from `run_entry_parse_ladder`; now the
  core's ONE shared ladder in `flz_core/src/routing.rs` — the old Java
  `DlqPark.retryThenPark` / `PrimaryStreamReader` are gone) → retry per the
  channel budget → DLQ park. Workflow → `extend_mapped` → terminal chain error the
  saga compensates. Both verified to FAIL with the guard removed.
- **An edge KEY never reaches the workflow driver** — a keyed edge makes its
  consumer dynamic and `build_dag` refuses a dynamic workflow procedure. A
  workflow ENTRY edge refuses ANY closure (`MergeFromEntryChannel`, message
  widened): the workflow seed bypasses the routing seam entirely.
- **D8 landed as ONE type, not two.** Rust: `Stream<'b, T, K = NodeTarget>` with
  `EntryStream<'b, T>` a type ALIAS (`K = EntryTarget`); the shared surface —
  `into`, the four transforms, `on_full`/`retry_*` — is one impl block, and
  `named`/`codec`/`keyed`/`sink`/`sealed`/`branch` stay on `NodeTarget`.
  Java: `EdgeStream<T>` interface, `EntryChannel extends` it (now an empty
  marker), `Stream implements` it; `StreamImpl` HOLDS an `EdgeStreamImpl` and
  forwards, because `Stream` must stay an abstract class for `@SafeVarargs
  branch` and Java has no second superclass. Cost: trybuild error text now says
  `Stream<'_, String, EntryTarget>`.
- **`Merge` keys ONCE, on the merge.** `Merge::key_by` extends each source's
  chain at wire time, so a key declared on a SOURCE handle trips
  `TransformAfterKeyBy` for free — that IS the "declared once" rule, not a
  separate check. `MergeSource::merge_channel_id` → `merge_site` (channel +
  pending chain); Java collapsed its two `from` overloads into one over
  `EdgeStream`.
- **Retired**: `inputs_dropped_no_key_total`, `TopologyIssue::StaticAndDynamic`
  (now `KeyByOnUnkeyableTarget`), Java's `@EntityKey` code generation (the
  annotation survives ONLY so declaring it errors with `.keyBy(f)` named).
- **FIXED IN PASSING (pre-existing Java bug the e2e needed):** the then-Java
  `StreamOperativeStateSerializer` branched on `entityKeys().isEmpty()`, so a
  dynamic operative's `EntityState` reported only `entities.get(0)`. That class
  is gone — the per-key map now comes from the shared core
  (`flz_core/src/command/entity_state.rs`). **The once-open flusher gap is
  CLOSED by the poll-core rehost:** a lazily-created Java Entity registers with
  the shared `EntityFlusher` exactly like a static one, so its state DOES reach
  disk — the Java e2e arm's live-state-only assertion in
  `dynamic_entity_e2e_test.exs` is now merely untightened.
- **Tests**: `sdk/rust/tests/stream_transforms.rs` (own `[[test]]` entry,
  `bench-internals`; two cases share `ChainOperative`'s statics and serialize on
  `CHAIN_LOCK`; the Java `StreamTransformsTest` went with the kernel mirror —
  Java transform coverage lives in `StreamOperativeTopologyBuilderTest` /
  `ActivationDispatcherTest`), workflow `PanickingTransformWorkflow`
  / the existing `PanickingMergeWorkflow` (Rust message `edge transform
  panicked`; the Java `edge transform threw` wrapper is gone — a throw now
  surfaces as the caller's park-ladder failure),
  `tests/ui/procedure_entity_key_removed.rs` +
  `entry_stream_has_no_plugin_sink.rs`, e2e
  `test/system/dynamic_entity_e2e_test.exs` on BOTH runtimes.
- **Docs/examples**: `docs/sdk/stream_transforms.md` (the proposal named
  `docs/sdk_graph_authoring.md`, which does not exist — the graph surface is
  documented in `docs/sdk/rust_operative_architecture.md`),
  `sdk/rust/examples/stream_transforms.rs`, Java `E10_StreamTransforms`.

