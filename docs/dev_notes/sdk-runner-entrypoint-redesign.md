# Runner entry point + dynamic type set (`sdk-runner-entrypoint-redesign`)

- **Runner-as-handle, no builder, mirrored across both SDKs.**
  `Flezha.createRunner(ns).defineStream(A.class).run()` (Java) /
  `Flezha::create_runner(ns)?.define_stream::<A>()?.run()?` (Rust) —
  Beam's `Pipeline.create()` shape: ONE concrete `FlezhaRunner` carries
  declaration and lifecycle. `RunnerBuilder`, `newRunner`/`new_runner`, both
  Java registry facades (`StreamOperativeRegistry`/`WorkflowOperativeRegistry`),
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
  `fast.rs`); `WorkflowInstance` carries `factory_id` for the scan. Java (no
  single coordinator thread): `FlezhaImpl.removeType` = unregister-first →
  scan live instances → re-register + throw `OperativeTypeInUseException`
  (extends ISE) on a live hit — the transient window is benign (a failing
  `AddOperative` is re-placed by convergence). Live instances of the factory ⇒
  `FlezhaError::OperativeTypeInUse` / that exception; undefined ⇒
  `NotDefined` / IAE naming the id.
- **Rust: ONE public error.** `FlezhaError` (thiserror; `InvalidNamespace,
  AlreadyRunning, ConnectFailed, InvalidTopology(#[from] TopologyBuildError),
  InvalidWorkflow(#[from] BuildWorkflowError), DuplicateFactoryId, NotDefined,
  OperativeTypeInUse, NotStarted, RunnerUnavailable`) — the whole chain
  compiles under one `?`. `RunnerStartError`/`RunnerError` deleted. The
  per-issue topology enum was renamed `BuildTopologyError` → `TopologyIssue`
  (the per-kind `TopologyBuildError` keeps its documented name).
  `operatives()` returns `Result<HashMap<String, Arc<StreamInstance>>, _>`
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
  workflow instance is absent from the SDK instance registry, so removing its
  type succeeds SDK-side and rehydration later fails placement. FleetManager's
  dropped-factory warning is the tripwire
  (`docs/resilience/workflow_operative.md`).
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

