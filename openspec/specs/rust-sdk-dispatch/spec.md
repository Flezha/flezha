# rust-sdk-dispatch Specification

## Purpose
This capability covers the Rust SDK's Operative/Procedure/Entity dispatch path: sticky per-Entity ordering invariants under ThreadPool dispatch, the dispatch throughput floor on the reference workload, the per-message dispatch-overhead budget, live Operative-instance registry concurrency, reconnect semantics across transient and intentional disconnects, command-dispatch byte-stability for surviving wire commands, the per-Entity flusher's single-thread linger drain (with toggle-aware semantics for `persistence_enabled = false`), the workflow `state.apply` hot-path cost, drain-time coalescing, the public `ProcedureTestHarness` testing surface, and the 16-byte version header stamped on persisted Changes.
## Requirements
### Requirement: Sticky Per-Entity Ordering Under ThreadPool Dispatch

Under the SDK's ThreadPool dispatch (the only dispatch mode in v1; see the REMOVED requirement below), the Rust SDK SHALL guarantee that for any single `(operative_instance, procedure, entity)` triple, two invocations of that Procedure's `run(...)` method on that Entity never execute concurrently, and invocations observe their inputs in the exact order those inputs were enqueued onto the Entity's mailbox. This invariant MUST hold regardless of worker-pool size, number of concurrent Operative instances, number of Entities per Procedure, or transport (UDS/QUIC).

#### Scenario: Same entity never runs concurrently under load
- **WHEN** 10,000 inputs targeting the same `(operative_instance, procedure, entity_key)` triple arrive back-to-back on one or more entry channels while the SDK uses a 16-thread worker pool
- **THEN** at any wall-clock instant, at most one thread SHALL be inside that Entity's `run(...)` method

#### Scenario: Different entities of same procedure may run concurrently
- **WHEN** inputs arrive simultaneously for two distinct entities of the same Procedure (for example `("ohlc", "1m")` and `("ohlc", "1h")`) and the worker pool has ≥ 2 threads
- **THEN** the two Entities' `run(...)` methods MAY execute concurrently on different threads

#### Scenario: Per-entity inputs delivered in arrival order
- **WHEN** inputs `"a"`, `"b"`, `"c"` are enqueued onto one Entity's mailbox in that order with an append-to-state Procedure
- **THEN** the Entity's State SHALL end with `["a", "b", "c"]` in exactly that order

#### Scenario: Ordering holds across multiple entry streams
- **WHEN** the agent opens 4 entry connections for a single Operative instance and the same Entity receives interleaved inputs from all 4
- **THEN** `run(...)` calls for that Entity SHALL still be non-overlapping, though the interleaving order between streams is not constrained by this requirement

### Requirement: Dispatch Throughput Floor on Reference Workload

On the regression bench (`100 operatives × 1 procedure × 1 entity × 10K msg`, no-op Procedure, UDS transport, default worker-pool size; see `operative-architecture/design.md` Decision 16) invokable via `mix bench --runtime rust --shape regression -n 100 -m 10000`, the Rust SDK SHALL sustain at least **480_000 messages/second** of message-processing throughput on commodity developer hardware (modern x86_64 laptop, 8+ cores, non-constrained CPU).

The floor SHALL be measured over the **receive window** — total messages divided by the elapsed time from the send loop returning to the last response received — because this requirement gates *dispatch* throughput, and the send window it excludes is the controller's own cast overhead rather than anything the SDK's dispatch path controls. It SHALL be read from the bench run's structured report field `metrics.receive_window_msg_per_sec`, not matched out of printed prose.

The bench SHALL nevertheless report the **end-to-end** window (first input dispatched to last response received) as its headline throughput, and SHALL name the window on every rate it reports. The two are not interchangeable — they differ by the send window — and a figure quoted without its window is not attributable. A future change that moves this floor onto a different window SHALL re-measure it rather than convert it.

The literal floor value SHALL be recorded in `docs/operations/benchmarking.md` and updated only in tandem with this requirement: when a future change intentionally raises or lowers the floor, the spec line and the doc value SHALL be updated in the same PR. CI SHALL verify spec/doc agreement on every build (`mix bench.floor` prints the recorded number; the `bench-regression` GitHub Actions job parses both the spec line and the doc value, fails the build if they disagree). The job SHALL read the spec literal from the **live capability spec** at `openspec/specs/rust-sdk-dispatch/spec.md`, which is the file this requirement is archived into and the only copy a future change edits.

The CI gate SHALL fail the build when measured throughput on the `bench-regression` job is below `0.9 × BENCH_FLOOR_MSG_PER_SEC` (10% tolerance band absorbs runner-image variance). Below `0.9 × BENCH_FLOOR_MSG_PER_SEC` is treated as a regression even if no individual code change in the PR is the obvious culprit; investigate before merging.

The umbrella's prior 350,000 msg/s floor (from before the cutover) SHALL NOT be carried forward as the gate.

#### Scenario: Reference bench meets the floor
- **WHEN** a developer runs `MIX_ENV=prod mix bench --runtime rust --shape regression -n 100 -m 10000` on a modern 8-core x86_64 laptop with a warm build
- **THEN** the receive-window throughput recorded in the run's structured report SHALL be ≥ the recorded floor value
- **AND** the run's printed output SHALL lead with the end-to-end throughput, each rate naming its window

#### Scenario: CI gate fails on a 15% regression
- **WHEN** a PR's `bench-regression` job measures throughput at `0.85 × BENCH_FLOOR_MSG_PER_SEC` (15% below the recorded floor)
- **THEN** the CI job SHALL fail with a message naming the recorded floor, the measured throughput, the percentage shortfall, and a pointer to `docs/operations/benchmarking.md` for triage instructions

#### Scenario: CI gate passes within tolerance
- **WHEN** a PR's `bench-regression` job measures throughput at `0.92 × BENCH_FLOOR_MSG_PER_SEC` (8% below the recorded floor — within the 10% tolerance band)
- **THEN** the CI job SHALL pass with a warning logged to the job output noting the proximity to the gate

#### Scenario: Spec/doc floor sync check fails when the two diverge
- **WHEN** a contributor edits the `BENCH_FLOOR_MSG_PER_SEC` value in `docs/operations/benchmarking.md` without updating the literal in this requirement
- **THEN** the `bench-regression` CI job's pre-bench sync check SHALL fail with a message identifying the divergence and pointing at both files

#### Scenario: The sync check reads a file that exists
- **WHEN** the `bench-regression` job runs on a checkout where the change that introduced this requirement has been archived
- **THEN** the sync check SHALL read the live capability spec, find the recorded literal, and SHALL NOT fail on a missing file

#### Scenario: Entity-stress bench scales with worker pool size
- **WHEN** the entity-stress bench (`8 operatives × 4 procedures × 8 entities × 20K msg/operative`, fan-out per input to all 8 entities of the entry procedure; invoked via `mix bench --shape entity --pool-sizes 1,8`) is run on an 8-core host
- **THEN** the throughput at pool size 8 SHALL be at least 4× the throughput at pool size 1, demonstrating that entity-mode dispatch is not the bottleneck
- **AND** the `bench-entity-scaling` CI job SHALL read the measured ratio from the run's structured report and report it to the PR check output without blocking the merge (informational gate)

### Requirement: Per-Message Dispatch-Overhead Budget

The Rust SDK's dispatch hot path — from a completed frame being delivered by the transport layer to the targeted Entity's `Procedure::run(...)` being entered — SHALL consume no more than **4 synchronization operations per message** (where a synchronization operation is a mutex lock, atomic CAS, channel send, or equivalent memory-ordering primitive). Counted under single-threaded contention on a Procedure with `entity_keys() == []` (single-entity mode), exercising the entry-channel → registry → mailbox → drain path.

The 4-op budget covers: (1) registry / topology lookup for the entry channel ID → Operative instance pointer (one DashMap read or equivalent); (2) mailbox enqueue (`crossbeam::queue::ArrayQueue::try_push`, one logical step counted as one proc even though its lock-free implementation may use a CAS on the tail plus a release-store on the slot); (3) `pushes_total` counter increment (one Relaxed atomic); (4) inflight CAS (one atomic compare-exchange).

The budget rises from 3 to 4 procs compared with the prior unbounded-mailbox shape (`crossbeam::queue::SegQueue` push without success counter). The added proc is the success counter introduced by `operative-bounded-mailbox`; the underlying push transition from SegQueue to ArrayQueue is counted as the same logical step (one mailbox enqueue) and does not on its own raise the budget.

For static-mode procedures, the additional per-input cost of entity fan-out (HashMap lookup per entity key, one mailbox enqueue + one counter increment + one inflight CAS per target entity) is intrinsic to the workload and counted separately — it is not part of the per-message budget. The fan-out formula updates from `3 + N × 2` to `4 + N × 3` where N is the entity fan-out width.

The 4-op invariant is an analytical bound enforced by code review, consistent with the convention in `sdk/rust/benches/correctness.rs:13-19`. The microbench reports raw msg/s for diagnostic comparison against the recorded `SINGLE_ENTITY_FLOOR_MSG_PER_SEC` value in `docs/operations/benchmarking.md`; it does not assert the sync-op count programmatically.

#### Scenario: Single-entity microbench confirms the budget
- **WHEN** the `throughput_single_entity` Criterion bench function in `sdk/rust/benches/correctness.rs` runs with contention pinned to one thread, single-entity Procedure, no fan-out (invoked via `cargo bench --bench correctness -- throughput_single_entity`)
- **THEN** the analytical sync-op count per message SHALL be ≤ 4 procedures on the hot path (registry lookup, mailbox enqueue, success counter, inflight CAS)
- **AND** the bench SHALL emit a `Throughput:` line in its Criterion output that callers can compare against the recorded `SINGLE_ENTITY_FLOOR_MSG_PER_SEC` value in `docs/operations/benchmarking.md`

#### Scenario: Static-mode entity fan-out cost is proportional to entity count
- **WHEN** the `throughput_static_fanout` Criterion bench function in `sdk/rust/benches/correctness.rs` is run against a Procedure with `entity_keys()` of length 4 and `entities_for` returning all four keys per input (invoked via `cargo bench --bench correctness -- throughput_static_fanout`)
- **THEN** the per-input sync-op count SHALL be approximately `4 + 4 × 3 = 16` (one registry lookup + 4 entities × (mailbox enqueue + success counter + inflight CAS)), and SHALL NOT exceed `4 + N × 3` where N is the entity fan-out width
- **AND** the bench SHALL emit a `Throughput:` line whose value, divided by the single-entity floor, SHALL be approximately `1 / (1 + 4 × 3 / 4) = 0.25` ± 30% (the analytical bound is approximate; the bench reports raw msg/s for sanity checking, not as a strict gate)

### Requirement: Live Registry Concurrent Access

The live Operative-instance registry used by the Rust SDK SHALL support concurrent reads from many threads without inter-reader blocking, and writes (Operative instance create / destroy) SHALL NOT block readers for longer than a single procedure's critical section. Operative classes are declared via `FlezhaRunner::define_stream::<R: StreamOperative>()`, callable before and after `.start()`/`.run()` on a runner obtained from `Flezha::create_runner(namespace)`; the class registries SHALL be overwrite-on-insert and SHALL support `unregister_class` for on-demand removal. `AddOperative` / `RemOperative` wire commands add and remove Operative instances by what the wire calls `operative_id` (mapped to the SDK's `operative_instance_id`); lookup by that id returns an `Arc<StreamInstance>` or equivalent handle. Workflow instances SHALL record their `factory_id` so live instances of a factory are attributable during a removal check.

The "registry" name carries forward from the pre-change implementation; what's stored in it is unchanged by this requirement. The concurrency guarantees are unchanged.

#### Scenario: 100 reader threads do not stall on a single writer
- **WHEN** 100 threads each perform 10,000 registry lookups while a 101st thread registers and removes Operative instances in a loop
- **THEN** no reader thread observes a latency spike greater than 1 millisecond attributable to the writer

#### Scenario: Registry preserves the declaration contract
- **WHEN** user code calls `.define_stream::<R>()` on a `FlezhaRunner` and later `AddOperative` triggers an Operative instance creation
- **THEN** the resulting `StreamInstance` SHALL be looked up by id and have its entry channels routable exactly as the spec requires, observable via the `operative-procedure-entity-model` capability's scenarios

#### Scenario: Post-start removal is serialized with AddOperative
- **WHEN** a `remove_stream` request and an `AddOperative` command for the same factory race
- **THEN** both SHALL be processed on the coordinator loop in some serial order — either the add lands first and the removal returns `Err(FlezhaError::OperativeTypeInUse { .. })`, or the removal lands first and the add fails as an unknown factory — with no interleaving where the removal succeeds while an instance exists

### Requirement: Reconnect Semantics Preserved

Across a control-channel reconnect the Rust SDK SHALL preserve its `operative_registry`, shared I/O streams, and `runner_id` (no clear-and-reassign window where frames are dropped). In addition:

1. **Live-sink resolution** — the per-Entity state send path (`EntityStateSink`) SHALL resolve the *live* secondary stream on every call rather than caching a stream handle, so a reconnect's freshly-wired stream is used without re-registering Entities.
2. **Reconnect ring** — when no sink is installed or a write fails (the reconnect window), **every frame the runner writes on the data plane** SHALL be buffered in one bounded ring (drop-oldest on overflow) and replayed when the fresh stream is wired, so nothing produced mid-reconnect is silently lost. This covers the durable records the flusher ships and the plugin outputs an operative emits: a plugin output arrives already framed with metadata that identifies its channel and its Entity, so it is replayed onto the live secondary as it stands rather than re-framed.
3. **Broken-pipe flood → control reconnect** — sustained write failures (a flood within a short window) SHALL trigger a full control-channel reconnect via the coordinator rather than spinning on a dead stream, whichever writer saw them.
4. **A shed frame is counted** — when the ring is full and drops its oldest entry, the runner SHALL count it on the process-wide drop-oldest counter its heartbeat already reports, and report the number lost when the stream recovers. A frame that is ultimately not delivered is never silent.

#### Scenario: state changes produced during a reconnect are not lost
- **GIVEN** the secondary stream drops and the SDK is reconnecting
- **WHEN** the operative produces state changes before the new stream is wired
- **THEN** the frames are buffered in the reconnect ring and replayed once the new sink is installed, preserving seq order.

#### Scenario: a broken-pipe flood drives a control reconnect
- **GIVEN** the state send path sees a flood of write failures within the detection window
- **WHEN** the threshold is crossed
- **THEN** the SDK requests a full control-channel reconnect from the coordinator instead of continuing to write to the dead stream.

#### Scenario: a plugin output whose write fails is held and replayed
- **GIVEN** an operative emits to a plugin sink on a stream the controller has closed
- **WHEN** the write fails
- **THEN** the frame is buffered in the same reconnect ring rather than discarded, the failure feeds the same flood ladder, and the next installed sink delivers the frame byte-for-byte on the live secondary.

#### Scenario: an overflowing ring reports what it shed
- **GIVEN** the reconnect ring is at its configured capacity while the stream is down
- **WHEN** a further frame is held
- **THEN** the oldest frame is dropped, the runner's `drops_oldest` heartbeat counter advances, and the recovery reports how many frames were lost before it.

### Requirement: Command Dispatch Behavior Preserved Byte-for-Byte for Surviving Commands

Every `CommandRequest` variant that survives the cutover (`AssignRunnerId`, `ConnectSocket`, `AddOperative`, `RemOperative`, `RunnerDisconnect`) SHALL produce a `CommandResponse` byte-for-byte identical to the pre-change implementation for every input that exercised a given path before the change. New error paths MAY be introduced only if pre-change code hit them via panics; otherwise these command responses MUST NOT change.

`EntityState` and `MigrationSerialization` are EXCLUDED from this byte-identical guarantee per `operative-mvp-rust/design.md` Decision 10: their on-the-wire JSON state-blob shape changes from `<state>` to `{"<procedure_name>": <state>}` (or, for static-mode procedures, `{"<procedure_name>": {"<entity_key>": <state>, ...}, ...}`). This is acceptable because (a) state migration is umbrella Non-Goal 2 and tracked in `operative-state-migration`, (b) the umbrella's hard-cutover policy means no production scenario crosses old-SDK and new-SDK state blobs, and (c) the new shape is documented in `docs/sdk/protocol_spec.md`.

`OperativeMessage` and `OperativeMessageResponse` continue to exist on the wire and the agent side; the SDK side responds with a canonical "unsupported" error for every request until `operative-message-handlers` adds per-Procedure `handle_message` (see `operative-mvp-rust/design.md` Decision 9). This is byte-equivalent to today's behavior for any operative that did not implement the optional `handle_message` callback.

#### Scenario: AddOperative with init_values produces same response shape
- **WHEN** the agent sends `AddOperative{operative_id:"p1", factory_id:"market", init_values:"{\"symbol\":\"BTC\"}"}` and the identical command is sent to both pre- and post-change SDK builds where `"market"` is registered via the new `register_stream::<MarketOperative>()` (post-change) or `register_factory("market", ...)` (pre-change)
- **THEN** both SDKs SHALL return a `CommandResponse` with the same `reference_id`, `status="success"`, and `errors=[]` (the `raw_data` bytes are not part of this requirement because the post-change `init_values` semantics are per-Procedure, see the `operative-procedure-entity-model` capability)

#### Scenario: RunnerDisconnect closes sockets identically
- **WHEN** `RunnerDisconnect{}` is sent under both implementations
- **THEN** both SDKs SHALL close their primary control stream and return identical `CommandResponse` bytes

#### Scenario: OperativeMessage returns canonical unsupported error pre-operative-message-handlers
- **WHEN** an agent sends `OperativeMessage{operative_id:"p1", payload:<...>}` to a v1 SDK
- **THEN** the SDK SHALL respond with `OperativeMessageResponse{ payload: "", error: "operative does not support handle_message" }`

### Requirement: Per-Entity flusher single-thread linger drain

The Rust SDK SHALL spawn at most one `EntityFlusher` OS thread per process. The thread SHALL be lazy-initialized via `OnceLock` on the first `FlezhaRunner::define_stream::<R>()` call that declares a Procedure with a non-`NoChange` Change type. Programs that declare only `NoChange` Procedures SHALL NOT spawn a flusher thread.

The flusher SHALL run an infinite loop that on each iteration: (a) sleeps for `linger_ms` milliseconds (default 5; configurable via env var `FLEZHA_FLUSH_LINGER_MS`); (b) iterates the registered Entities; (c) for each Entity, attempts to acquire the buffer's `Mutex`, and on success calls `FlushableBuffer::drain_to_frames` which: applies coalescing, builds wire frames, releases the lock; (d) for each frame, invokes the Entity's `send_frame` function pointer.

#### Scenario: NoChange-only program does not spawn flusher

- **WHEN** a Rust process calls `.define_stream::<R>()` for an Operative whose Procedures all have `Change = NoChange`
- **THEN** no `EntityFlusher` thread SHALL be spawned, observable via thread-count assertions in the test (`std::thread::available_parallelism` baseline + assertion that no `flezha-flusher` named thread exists)

#### Scenario: First non-NoChange declaration starts the flusher

- **WHEN** a Rust process calls `.define_stream::<R>()` for an Operative containing a Procedure with `Change = MyChange`
- **THEN** exactly one OS thread named `flezha-flusher` SHALL be spawned for the lifetime of the process

### Requirement: Toggle-aware drain semantics for persistence_enabled = false

The Rust SDK's `EntityFlusher` SHALL inspect each registered Entity's `persistence_enabled` flag at drain time. When `false`, the flusher SHALL acquire the buffer's Mutex, invoke `FlushableBuffer::drain_to_frames` (which still applies coalescing), and discard the resulting frames without invoking `send_frame`. The Entity's `change_seq` counter SHALL still advance through the discarded frames.

When `true`, the flusher SHALL invoke `send_frame` for each frame and propagate any `Err(SendError)` by decrementing the seq counter so the next drain re-sends from the right point (no gaps in the agent's WAL view).

#### Scenario: Toggle-off drain discards without wire I/O

- **WHEN** an Operative with `persistence_enabled = false` records 10 Changes via `state.apply(...)` and the flusher's linger elapses
- **THEN** the Entity's flush buffer SHALL be empty after the tick AND the Entity's `send_frame` function SHALL NOT have been invoked AND the Entity's `change_seq` SHALL have advanced by the post-coalesce record count

#### Scenario: Toggle-on drain invokes send_frame per frame

- **WHEN** an Operative with `persistence_enabled = true` records 10 Changes (no coalescing) via `state.apply(...)` and the flusher's linger elapses
- **THEN** the Entity's `send_frame` function SHALL have been invoked exactly 10 times with frame bytes built from `[entity_id][seq: u64 BE][header: 16B][change_bytes][crc32: u32 BE]`

#### Scenario: send_frame failure preserves seq monotonicity

- **WHEN** the Entity's `send_frame` returns `Err(SendError::TransportDown)` for the third frame in a 10-frame batch
- **THEN** the flusher SHALL stop the batch at that frame, decrement `change_seq` to undo the failed-and-following advances (the first two frames' seq advances stay), and SHALL retain the un-sent Changes in the buffer for the next drain (records 3-10 are re-pushed via a side channel that preserves their post-coalesce order)

### Requirement: Hot-path cost is bounded to one Vec::push per state.apply

The Rust SDK's `state.apply(change)` SHALL pay exactly: one `Procedure::apply_change(state, &change)` call (user-supplied; cost varies) plus one `Vec::push(change)` procedure. The hot path SHALL NOT serialize, fsync, network-send, or acquire any lock beyond the existing per-Entity sticky CAS and the `&mut self` borrow on the buffer (which is already exclusive via the runner thread's ownership of the entity's buffer for the duration of `Procedure::run`).

The microbench `state_apply_throughput` (`sdk/rust/benches/state.rs`) SHALL exercise this requirement and report nanoseconds per `state.apply(...)` call across change-payload-size and coalesce-key dimensions; CI SHALL fail the build if any dimension regresses >50% from the recorded baseline.

#### Scenario: No serialization on state.apply

- **WHEN** a Procedure's `run` calls `state.apply(MyChange::Foo)` 100 times in a tight loop
- **THEN** zero serialization calls (`serde::Serialize::serialize`, `postcard::to_allocvec`, `bincode::serialize`, etc.) SHALL be issued from the runner thread during these 100 calls

#### Scenario: No I/O on state.apply

- **WHEN** a Procedure's `run` calls `state.apply(MyChange::Foo)` 100 times
- **THEN** zero `write`, `send`, `fsync`, `fdatasync`, or NIF-call syscalls SHALL be issued from the runner thread during these 100 calls

#### Scenario: state_apply_throughput microbench reports < 200 ns/proc for SmallChange + NoCoalesceKey

- **WHEN** the `state_apply_throughput` Criterion bench runs with `change_kind = SmallChange (8B)` and `coalesce_kind = NoCoalesceKey` and the buffer is preallocated to capacity 1024
- **THEN** the reported median time per `state.apply(...)` SHALL be < 200 ns on the reference host

### Requirement: Coalescing is applied at drain time, not on the hot path

The Rust SDK SHALL apply coalescing inside `FlushableBuffer::drain_to_frames`, AFTER the buffer's Mutex is acquired by the flusher. The hot path of `state.apply(...)` SHALL NOT touch any coalescing data structure.

The drain-time coalescing algorithm SHALL: (a) build a `HashMap<CoalesceKey, usize>` mapping each key to the index of the latest record carrying that key; (b) build a parallel `Vec<bool>` mask marking earlier records with the same key as discarded; (c) emit kept records in their original buffer order. Records with `coalesce_key() == None` SHALL be retained verbatim.

#### Scenario: Same-key records collapse to latest

- **WHEN** a Procedure records 50 `BarTick { interval: M1, ts: T }` Changes via `state.apply(...)` within one linger window, all returning `Some(CoalesceKey::from_hash(&(M1, T)))` from `coalesce_key()`
- **THEN** the flusher's drain SHALL emit exactly one frame whose Change is the 50th `BarTick`

#### Scenario: No-key records are preserved verbatim

- **WHEN** a Procedure records 10 Changes whose `coalesce_key() == None` within one linger window
- **THEN** the flusher's drain SHALL emit 10 frames in the same order they were pushed

#### Scenario: Mixed coalesce-key and no-key records

- **WHEN** a Procedure records, in order: A (no key), B (key=K1), C (key=K1), D (no key), E (key=K2)
- **THEN** the flusher's drain SHALL emit frames in order: A, C, D, E (B is collapsed to C; A and D are preserved verbatim; E is retained)

### Requirement: ProcedureTestHarness in public API surface (Rust SDK)

The Rust SDK SHALL export `ProcedureTestHarness::<S: Procedure>` under `flezha_runner::testing::ProcedureTestHarness`, gated by `cfg(any(test, feature = "testing"))`. The harness SHALL support:

- `ProcedureTestHarness::<S>::new()` (requires `S: Default`) — constructs a harness with `S::init_state(None, None)`.
- `ProcedureTestHarness::<S>::with_initial_state(state: S::State)` — explicit initial state.
- `ProcedureTestHarness::<S>::with_payload(entity_key, payload)` — replicates the runtime's snapshot+changelog restoration flow.
- `apply_inputs(iter: impl IntoIterator<Item = S::Input>) -> &mut Self` — invokes `S::run` for each input on the harness's owned state, recording emits and Changes.
- `expect_changes(self, expected: &[S::Change]) -> Self` (consuming) — asserts equality, requires `S::Change: PartialEq + Debug`.
- `expect_emits<C>(self, expected: &[C]) -> Self` (consuming) — asserts emits of type `C` equal expected.
- `expect_state(self, expected: &S::State) -> Self` (consuming) — asserts state equality, requires `S::State: PartialEq + Debug`.
- `replay(initial: S::State, log: &[S::Change]) -> S::State` — folds the log via `S::apply_change`.
- `replay_equals(&self) -> Result<(), ReplayMismatch>` — replays self.recorded_changes against a re-constructed initial state and asserts byte-equality (via `serde::Serialize` round-trip) with `self.state`.

#### Scenario: Harness asserts on recorded Changes

- **WHEN** a test runs `ProcedureTestHarness::<CounterProcedure>::new().apply_inputs(vec![Input::Add(5), Input::Add(3)]).expect_changes(&[CounterChange { delta: 5 }, CounterChange { delta: 3 }])`
- **THEN** the chain SHALL succeed and produce a harness whose `recorded_changes` equals the expected slice

#### Scenario: Harness replays for determinism check

- **WHEN** a test runs `ProcedureTestHarness::<CounterProcedure>::new().apply_inputs(inputs).replay_equals()`
- **THEN** the result SHALL be `Ok(())` because `CounterProcedure::apply_change` is deterministic

#### Scenario: Harness detects non-deterministic apply_change

- **WHEN** a Procedure's `apply_change` reads `SystemTime::now()` and uses it to seed a state field, AND the harness runs `replay_equals()`
- **THEN** the result SHALL be `Err(ReplayMismatch { expected_serialized, actual_serialized })` whose `Display` impl renders a hex-diff identifying the divergent state field

#### Scenario: Harness compiled-out without testing feature

- **WHEN** a downstream crate depends on `flezha_runner` without enabling the `testing` feature flag and does not build under `#[cfg(test)]`
- **THEN** `flezha_runner::testing` SHALL NOT be visible and references to `ProcedureTestHarness` SHALL fail to compile

### Requirement: 16-byte version header stamped on persisted Changes

The Rust SDK SHALL stamp a 16-byte header on every Change record emitted to the wire path, per the umbrella's spec:

- bytes 0:1 — `sdk_lang: u8` SHALL equal `1` (Rust).
- bytes 1:5 — `sdk_version: u32 BE` SHALL be parsed from `env!("CARGO_PKG_VERSION")` at SDK build time as `(major << 16) | (minor << 8) | patch`.
- bytes 5:9 — `schema_version: u32 BE` SHALL equal `S::SCHEMA_VERSION` for the Procedure owning the Change.
- bytes 9:10 — `format_id: u8` SHALL equal `1` (postcard) in v1.
- bytes 10:12 — reserved, MUST be zero.
- bytes 12:16 — `crc32: u32 BE` SHALL be `crc32fast::hash(&change_bytes)`.

The header SHALL be stamped in `HeaderTemplate::stamp(change_bytes: &[u8]) -> [u8; 16]` and verified in `decode_header` on the restoration path.

#### Scenario: Header bytes match the umbrella spec

- **WHEN** a Rust SDK at `version 0.5.3` and Procedure `SCHEMA_VERSION = 1` stamps a header for change_bytes `&[0x01, 0x02, 0x03]`
- **THEN** the resulting 16-byte header SHALL equal `[0x01, 0x00, 0x00, 0x05, 0x03, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, <crc32 BE bytes of [0x01, 0x02, 0x03]>]`

#### Scenario: Cross-language load fails loud

- **WHEN** the Rust SDK attempts to decode a header whose `sdk_lang` byte is `2` (Java)
- **THEN** `decode_header` SHALL return `Err(RestoreError::LangMismatch { expected: 1, found: 2 })` and SHALL NOT proceed to deserialization

#### Scenario: schema_version mismatch fails loud

- **WHEN** the Rust SDK at Procedure `SCHEMA_VERSION = 2` attempts to decode a record whose header carries `schema_version = 1`
- **THEN** `decode_header` SHALL return `Err(RestoreError::SchemaMismatch { expected: 2, found: 1 })` and SHALL NOT proceed to deserialization (registered migrators are reserved by the umbrella but not implemented in this change)

#### Scenario: crc32 mismatch fails loud

- **WHEN** the Rust SDK decodes a record whose header crc32 does not match a recomputed crc32 over the change_bytes
- **THEN** `decode_header` SHALL return `Err(RestoreError::CrcMismatch { entity, seq })` and SHALL NOT proceed to deserialization

### Requirement: Public Rust SDK API uses canonical Operative/Runner vocabulary

The Rust SDK SHALL expose its top-level dispatch types under the names `RunnerNode` (formerly `ComputeEngineNode`) and provide a method `get_operatives()` (formerly `get_processors()`) returning the registered Operatives on the current Runner. The module path SHALL be `sdk/rust/src/runner/` (formerly `sdk/rust/src/compute_engine/`). Generated protobuf types SHALL reflect the renamed messages: `AssignRunnerId`, `RunnerDisconnect`, `AddOperative`, `RemOperative`, `EntityState`, `EntityApplyState`, `OperativeMessage`, `OperativeMessageResponse`, plus existing `RestoreInitState`, `CompactEntity`, `EntityMailboxStatus`.

#### Scenario: Public types compile under new names
- **WHEN** a downstream Rust crate depends on `flezha-sdk` and writes `use flezha_sdk::runner::RunnerNode;`
- **THEN** the import MUST resolve and the compiler MUST accept it; an import via the legacy path `flezha_sdk::compute_engine::ComputeEngineNode` MUST NOT compile

#### Scenario: get_operatives returns the local Operative registry
- **WHEN** a Rust SDK Runner has Operatives `r1` and `r2` registered locally
- **THEN** `runner_node.get_operatives()` MUST return a collection containing exactly `r1` and `r2`; the legacy method `get_processors()` MUST NOT be exposed

### Requirement: Generated payload-id constants use canonical names

The Rust SDK's payload-id constants (those that map to the binary framing's `payload_id(32)` byte slot) SHALL be named consistent with the canonical vocabulary: `ENTITY_STATE_CHANGE`, `ENTITY_STATE_SNAPSHOT`, `RESTORE_INIT_STATE`, `COMPACT_ENTITY`, `ENTITY_MAILBOX_STATUS`, `CONNECT_CONTROL_CHANNEL`, `CONTROL_COMMAND`. The previous `PROCESSOR_*` family SHALL be removed.

#### Scenario: Payload-id constants compile and resolve
- **WHEN** the Rust SDK is built
- **THEN** the constants `ENTITY_STATE_CHANGE` and `ENTITY_STATE_SNAPSHOT` MUST be defined and exported; references to `PROCESSOR_STATE_CHANGE` MUST not exist

### Requirement: Test fixtures' Procedures exercise the output channel path

The test fixtures under `test/apps/rust_runner/src/main.rs` SHALL emit at least one output frame per input received, via `ProcedureCtx::output(channel, value, metadata)`, on Operatives exercised by the controller's `:on_output`-subscribed plugin tests. Specifically:

- `StoreInputOperative::StoreInputProcedure::run` SHALL emit the input string on a sink Channel (named `"output"`) after applying the `PushText` change.
- `CompareFilterOperative::CompareFilterProcedure::run` SHALL emit the input string on a sink Channel when `state.comparable == input` (matching the existing test's expectation of `"hello world"` round-tripping through the filter).

This requirement covers fixture behavior only; it does not change the SDK's `Procedure` trait, the dispatch contract, or any wire format. A future production Operative may emit no outputs and remain conformant to the SDK's other requirements.

#### Scenario: StoreInputOperative round-trips input via :on_output

- **WHEN** the controller's `BeamMsgForward` plugin is enabled with `subscriptions: [{:on_output, []}]` against a `StoreInputOperative` instance and the test sends `"state_1"` to the operative
- **THEN** the test SHALL receive `{:message_forward, "state_1"}` within 2 seconds (UDS) or 5 seconds (QUIC)

#### Scenario: CompareFilterOperative round-trips matching input via :on_output

- **WHEN** a `CompareFilterOperative` instance is initialized with `comparable = "hello world"` and the test sends `"hello world"` to the operative
- **THEN** the test SHALL receive `{:message_forward, "hello world"}` within 2 seconds

### Requirement: `RunnerReconnected` control message defined and emitted

The SDK SHALL emit an `RunnerReconnected { previous_id, new_id, reason }` control frame as the first message after re-establishing the control stream, and SHALL emit a single structured reconnect log line. The controller SHALL use this frame to compute the binding delta: a stable reconnect under the same id is a zero-frame no-op; a controller-side reassignment during the disconnect window triggers `ReassignRunnerId { new_id }` and a re-issue of the operative set. `reason` SHALL be one of the transport-edge causes (e.g. broken pipe, read/write error, keepalive timeout, controller-requested).

#### Scenario: a stable reconnect is a zero-delta no-op
- **GIVEN** the SDK reconnects within the disconnect window holding the same `runner_id`
- **WHEN** it sends `RunnerReconnected{previous_id == current, new_id == current}`
- **THEN** the controller's bindings already agree and no `AddOperative`/`RemOperative` frames are issued.

### Requirement: Node-local persisted runner id announced as `previous_runner_id`

On a fresh process start (not a reconnect) the SDK SHALL load its node-local persisted runner id (atomically written via tmp+rename at `FLEZHA_RUNNER_ID_PATH`) and announce it as `previous_runner_id` in the connect handshake, so the controller evicts the stale binding from the prior process before binding the new one. Reconnects SHALL NOT re-announce it (identity is preserved in-place).

#### Scenario: a restarted process reclaims its bindings
- **GIVEN** an SDK process held `runner_id = default_X` and persisted it locally
- **WHEN** the process is killed and a fresh process starts, reads `default_X`, and announces it as `previous_runner_id`
- **THEN** the controller evicts the stale `default_X` binding before the new bind, so no operative is left bound to the dead process.

### Requirement: Rust SDK Entry Point

The Rust SDK SHALL provide `Flezha::create_runner(namespace: impl Into<String>) -> Result<FlezhaRunner, FlezhaError>` as the single entry point, returning a concrete `FlezhaRunner` directly — there SHALL be no separate `RunnerBuilder` type. Creating a runner SHALL NOT connect, claim the process singleton slot, or spawn any thread. Calling `.start(&self)` MUST connect to the controller's control socket, send the handshake init message per `docs/sdk/protocol_spec.md` §5.2 carrying the runner's currently-declared factory set (possibly empty), wait for `AssignRunnerId`, and return `Ok(&Self)` for chaining. The started runner SHALL expose `namespace()` (replacing `id()` — the controller-assigned per-session token lives on the runner thread and rotates across reconnects, so it is never the handle's identity) and `operatives()`. `.run(&self)` SHALL be sugar for `.start()` followed by `await_termination()`, never an independent implementation. This requirement mirrors `java-sdk`'s "Java SDK Entry Point" requirement — the two SDKs' entry-point behavior is intentionally identical.

#### Scenario: Successful connection returns a live handle
- **WHEN** an SDK user calls `Flezha::create_runner("my_app")?.define_stream::<MarketOperative>()?.start()` (binding the runner with `let`) with the controller running
- **THEN** the call blocks until the connect handshake completes, and the runner's `namespace()` returns `"my_app"`

#### Scenario: Controller unavailable triggers exponential backoff retries
- **WHEN** an SDK user calls `.start()` on a runner and the controller's control socket is not yet listening
- **THEN** the SDK SHALL retry the connect with exponential backoff starting at 200 ms and capped at 30 s, and succeed once the controller becomes available

#### Scenario: start() with zero defined operative types is legal
- **WHEN** `.start()` is called on a `FlezhaRunner` with no `.define_stream(...)`/`.define_workflow(...)` calls and no `inventory`-harvested classes on the binary
- **THEN** the runner SHALL connect successfully with an empty factory list in the handshake init message, and operative types MAY be defined later on the live runner

#### Scenario: RunnerBuilder does not compile
- **WHEN** user code references `flezha::RunnerBuilder`, `Flezha::new_runner`, `RunnerStartError`, or `RunnerError`
- **THEN** compilation SHALL fail — the builder type, the old entry-point name, and the superseded error types are deleted with no shim

### Requirement: Runner declaration and connection failures surface as typed Results

Every fallible call on the entry-point chain SHALL return `Result<_, FlezhaError>` — one unified public error type (`thiserror`-derived) so `Flezha::create_runner(ns)?.define_stream::<A>()?.run()?` compiles under a single `?`. `FlezhaError` SHALL carry at least: `InvalidNamespace`, `AlreadyRunning { namespace }`, `ConnectFailed(String)`, `InvalidTopology(#[from] TopologyBuildError)`, `InvalidWorkflow(#[from] BuildWorkflowError)`, `DuplicateFactoryId { factory_id }`, `NotDefined { factory_id }`, `OperativeTypeInUse { factory_id, live_instances }`, `NotStarted`, `RunnerUnavailable(String)`. An invalid topology (duplicate channel names, unwired channels, missing `entities_for` overrides, etc.) SHALL be reported as a typed `Err` at the call site, not a panic; a consumer whose `Input` does not match the channel payload SHALL instead be refused by the compiler at the `.into::<S>()` call site, so `TopologyIssue` SHALL carry no runtime type-mismatch variant; connect-handshake failures (socket error, or the confirmation channel closing unexpectedly) SHALL be `Err(FlezhaError::ConnectFailed(..))`, not a panic. The per-issue topology enum formerly named `BuildTopologyError` SHALL be renamed `TopologyIssue`, resolving its collision with the per-kind `TopologyBuildError`; Java throws the identically-named failure classes per naming rule 6.

`operatives()` SHALL return `Result<HashMap<String, Arc<StreamInstance>>, FlezhaError>` — `Err(FlezhaError::NotStarted)` on a never-started runner, never a panic; the internal snapshot path SHALL contain no `expect`/`unwrap` reachable from it.

#### Scenario: Invalid topology is reported as a Result, not a panic
- **WHEN** an Operative declares the same channel name twice
- **THEN** `.define_stream::<ThatOperative>()` SHALL return `Err(FlezhaError::InvalidTopology(..))` carrying the `TopologyIssue::DuplicateChannel` diagnosis naming the channel — the call SHALL NOT panic

#### Scenario: A payload type mismatch is a compile error
- **WHEN** user code wires a consumer whose `Procedure::Input` is not the channel's payload type
- **THEN** the `.into::<S>()` call SHALL fail to compile at the call site, and no `TopologyIssue` variant SHALL exist to report it at registration time

#### Scenario: A connection failure at start() is a Result, not a panic
- **WHEN** the confirmation channel from the runner thread closes without delivering `RunnerIdConfirmation` (e.g. the runner thread panicked internally)
- **THEN** `.start()` SHALL return `Err(FlezhaError::ConnectFailed(..))` — the caller's thread SHALL NOT panic

#### Scenario: The fluent chain compiles under one question mark
- **WHEN** user code writes `Flezha::create_runner("ns")?.define_stream::<A>()?.define_workflow::<B>()?.run()?` in a function returning `Result<(), FlezhaError>`
- **THEN** it SHALL compile without intermediate `map_err` calls

#### Scenario: operatives() on a never-started runner
- **WHEN** code calls `.operatives()` on a runner created but never started
- **THEN** the call SHALL return `Err(FlezhaError::NotStarted)` and SHALL NOT panic

### Requirement: Runner process singleton guard

The Rust SDK SHALL allow at most one runner to be **started** per process. `Flezha::create_runner(...)` SHALL NOT claim the singleton slot — any number of unstarted runners may exist. `.start()`/`.run()` SHALL claim a process-global guard (a mutex-held slot storing the running namespace — releasable on close/Drop, which a `OnceLock` is not) by compare-and-set before connecting; if a prior runner's `.start()`/`.run()` has already succeeded in this process and not yet shut down, the call SHALL return `Err(FlezhaError::AlreadyRunning { namespace })` naming the already-running namespace, and SHALL NOT connect, mutate any shared registry, or spawn any background thread. Calling `.start()` twice on the same runner SHALL also be an error.

#### Scenario: Second start() in the same process is rejected
- **GIVEN** a process where a runner for `"ns1"` created via `Flezha::create_runner("ns1")?` has been started
- **WHEN** code in the same process starts a second runner for `"ns2"`
- **THEN** the call SHALL return `Err(FlezhaError::AlreadyRunning { namespace: "ns1".into() })`
- **AND** no connection SHALL be attempted for `"ns2"`, and the process's `STABLE_ID`, `entity_state_sink`, and `credit::global()` state SHALL remain exclusively `"ns1"`'s

#### Scenario: Creating runners never claims the slot
- **GIVEN** a process where a runner for `"ns1"` is already started
- **WHEN** code calls `Flezha::create_runner("ns2")?` and mutates it with `define_*`/`remove_*` without calling `.start()`
- **THEN** no error SHALL occur and `"ns1"` SHALL remain the process's active runner

#### Scenario: A runner may be started again after the first one is dropped
- **GIVEN** a process where a started runner for `"ns1"` was dropped (triggering `Drop`'s graceful-shutdown sequence)
- **WHEN** code in the same process creates and starts a runner for `"ns2"`
- **THEN** the call SHALL succeed and `"ns2"` SHALL become the process's active runner
- **AND** the new runner's declared factory set SHALL start empty — generation 2 must re-define its types (no cross-generation declaration leak)

#### Scenario: Concurrent start() calls — exactly one wins
- **GIVEN** a process with no runner started yet
- **WHEN** two threads each call `.start()` on a distinct `FlezhaRunner` (`"ns1"` and `"ns2"`) at the same time
- **THEN** exactly one call SHALL succeed and become the process's active runner, and the other SHALL return `Err(FlezhaError::AlreadyRunning { namespace })` naming the winner — the guard's compare-and-set SHALL make this outcome deterministic per-run (no scenario where both succeed or both fail)

### Requirement: Runner lifecycle — start, run, await_termination, Drop

`FlezhaRunner::start(&self) -> Result<&Self, FlezhaError>` SHALL connect and return without blocking the calling thread for the runner's ongoing lifetime. `FlezhaRunner::await_termination(&self)` SHALL block the calling thread until the runner shuts down (via `Drop`, SIGTERM-triggered graceful shutdown, or a controller-driven stop); on a never-started runner it SHALL return `Err(FlezhaError::NotStarted)`. `FlezhaRunner::run(&self) -> Result<(), FlezhaError>` SHALL be exactly `start()` followed by `await_termination()`, with no independently-implemented blocking logic. The owning handle's `Drop` SHALL run the graceful-shutdown sequence, release the singleton slot, and wake any thread blocked in `await_termination()` — but ONLY if the runner was started; an owner-never-started `Drop` SHALL be a no-op. Observer handles from `Flezha::current_runner()` (`owner: false`) SHALL have a no-op `Drop`.

#### Scenario: start() returns immediately after the handshake, before shutdown
- **WHEN** an SDK user calls `.start()` on a runner
- **THEN** the call returns as soon as the runner has an assigned id, without waiting for the runner to ever shut down
- **AND** the runner continues dispatching in background threads exactly as it does today

#### Scenario: run() blocks until shutdown
- **WHEN** an SDK user calls `.run()` on a runner in a standalone `fn main()`
- **THEN** the call blocks the calling thread until the runner shuts down, replacing the hand-rolled stdin-reading `std_keeper()` pattern used by `test/apps/rust_runner/src/main.rs` today

#### Scenario: Dropping a never-started runner is free
- **WHEN** a `FlezhaRunner` created via `Flezha::create_runner(..)` is dropped without `.start()` having been called
- **THEN** `Drop` SHALL perform no shutdown work, touch no shared state, and release no slot (none was claimed)

#### Scenario: await_termination() on a never-started runner
- **WHEN** code calls `.await_termination()` on a runner created but never started
- **THEN** the call SHALL return `Err(FlezhaError::NotStarted)` immediately and SHALL NOT block

### Requirement: On-demand operative type declaration and removal (Rust)

`FlezhaRunner` SHALL provide `define_stream::<R>()` / `define_workflow::<T>()` and their inverses `remove_stream::<R>()` / `remove_workflow::<T>()`, each returning `Result<&Self, FlezhaError>`, legal before and after `.start()`. The declared factory set SHALL be per-runner, starting empty each generation (inventory-harvested classes are merged into it at every `.start()`). A post-start define or remove SHALL update the process-global class registries and ship the runner's full current factory set to the controller via the `UpdateFactories` control message (see the `runner-factory-sync` capability); the reconnect init frame SHALL serialize the same per-runner set, so reconnect naturally resyncs post-start mutations. `remove_*` SHALL return `Err(FlezhaError::NotDefined { .. })` for an undefined factory and `Err(FlezhaError::OperativeTypeInUse { factory_id, live_instances })` when live instances of the factory exist, leaving the type defined. Duplicate definition of the same factory id on one runner SHALL return `Err(FlezhaError::DuplicateFactoryId { .. })`.

#### Scenario: Pre-start remove shapes the init frame
- **WHEN** user code defines A and B on an unstarted runner, removes B, then starts
- **THEN** the handshake init frame's factory list SHALL contain only A

#### Scenario: Post-start define ships UpdateFactories
- **WHEN** user code calls `.define_stream::<C>()` on a started runner already declaring A
- **THEN** the SDK SHALL send an `UpdateFactories` control message carrying the full set {A, C}, and a subsequent `AddOperative{factory_id: C}` SHALL succeed

#### Scenario: Post-start remove with live instances refuses
- **GIVEN** a started runner with one live instance of factory `"market"`
- **WHEN** user code calls `.remove_stream::<MarketOperative>()`
- **THEN** the call SHALL return `Err(FlezhaError::OperativeTypeInUse { factory_id: "market", live_instances: 1 })` and the type SHALL remain defined

#### Scenario: Removing an undefined factory
- **WHEN** user code calls `.remove_stream::<X>()` and X's factory id is not in the runner's declared set
- **THEN** the call SHALL return `Err(FlezhaError::NotDefined { .. })`

### Requirement: A restore SHALL NOT silently discard un-recoverable queued input

A restore SHALL discard an Entity's queued input only where the Controller will re-forward it. Where the Controller will not re-forward it, the queued input SHALL NOT be silently destroyed.

`restore_into_slot` today clears the mailbox on every restore, justified by the claim that every queued input "is durable in the input log the controller is about to re-forward". That claim holds only for classes whose inputs are journaled before forwarding — `Ingest.journal_input/2` journals for the effectively-once tier and for durable workflow instances, and passes every other operative through untouched. A plain stateful stream operative, which is the default configuration, has no such log.

#### Scenario: Journaled input may be discarded
- **GIVEN** an Entity whose class journals its inputs before forwarding
- **WHEN** a restore rebuilds that Entity
- **THEN** the queued input MAY be discarded, because the Controller re-forwards it from the durable log

#### Scenario: Un-journaled input is not silently destroyed
- **GIVEN** an Entity whose class does not journal its inputs
- **WHEN** a restore rebuilds that Entity
- **THEN** its queued input SHALL NOT be silently discarded

#### Scenario: The replay buffer and open interval clears are unchanged
- **WHEN** a restore rebuilds an Entity
- **THEN** the replay buffer, replay flags, and any open checkpoint interval SHALL continue to be cleared as they are today, on every class

### Requirement: Input discarded by a restore SHALL be counted

`Mailbox::clear` is a bare drain loop with no counter, telemetry, or log line, so a discard leaves no trace and the loss can only be inferred by reading the code. Any input discarded on the restore path SHALL be observable.

#### Scenario: A discard emits a count
- **WHEN** a restore discards queued input for an Entity
- **THEN** the number of discarded items SHALL be recorded and exposed, naming the Entity

### Requirement: The two SDKs SHALL agree on restore-time mailbox handling

The Rust SDK clears the mailbox on restore; the Java SDK does not, clearing only its staged changes. Neither behaviour is documented as the intended one. Both SDKs SHALL implement the same documented behaviour.

#### Scenario: Equivalent restores behave equivalently
- **GIVEN** equivalent Entities of the same class on a Rust runner and a Java runner, each with queued input
- **WHEN** each is restored
- **THEN** the fate of the queued input SHALL be the same on both

### Requirement: The Rust SDK SHALL emit an entity_state_snapshot on CompactEntity

`flz_core/src/command/compact.rs` currently ignores its runner argument and answers a hardcoded `CompactEntityResponse{snapshot_seq: 0, snapshot_bytes_size: 0}` without shipping any frame, and pushes nothing onto `errors` — so the failure presents to the Controller as a successful handshake. The handler SHALL locate the addressed Entity, capture its state, ship an `entity_state_snapshot` data frame, and answer with the seq it actually captured.

The snapshot body SHALL be encoded with the same codec the restore path decodes — postcard, per `entity/restore.rs`. The JSON serializer used for graceful shutdown and live migration SHALL NOT be used for the snapshot body.

The frame SHALL carry a payload-id constant defined alongside the sink's other payload-ids, and SHALL be shipped through the existing reliable sink rather than a separate transport path.

#### Scenario: A live Entity produces a snapshot at its current seq

- **GIVEN** a live Entity with persisted Changes and a current `change_seq`
- **WHEN** the SDK receives `CompactEntity` addressing that Entity
- **THEN** the SDK SHALL ship one `entity_state_snapshot` frame carrying the Entity's serialized state stamped with a seq the Entity has reached
- **AND** SHALL answer `CompactEntityResponse` reporting that same seq and the body's byte size

#### Scenario: The snapshot body round-trips through the restore path

- **GIVEN** a snapshot produced by the SDK for an Entity
- **WHEN** that snapshot is later shipped back to the SDK as `RestoreInitState.snapshot_bytes`
- **THEN** the restore path SHALL decode it without error and SHALL reconstruct the state the Entity held when the snapshot was taken

#### Scenario: The seq and the state are captured together

- **WHEN** the SDK captures state and seq for a snapshot
- **THEN** the pair SHALL be captured such that the Controller may truncate the WAL up to that seq without discarding a record the snapshot does not already contain

#### Scenario: An unaddressable Entity reports an error

- **WHEN** the SDK receives `CompactEntity` for an `entity_id` matching no live Entity
- **THEN** the handler SHALL report an error rather than answering as though compaction succeeded

### Requirement: The Rust SDK SHALL take the proc-journal from the bind

`start_workflow_resumable` SHALL accept the instance's proc-journal alongside the
restore count and SHALL pass it to `WorkflowInstance::new`, so the resume state is
present before the driver is spawned. The `RestoreInitState` handler SHALL NOT
carry the journal — that frame is per-Entity and is not shipped for a class with
no persisted Entity.

#### Scenario: A resuming bind with no Entity restores still resumes at its iteration

- **GIVEN** a durable workflow class whose procedures hold no per-Entity state, so no `RestoreInitState` is shipped
- **WHEN** the controller binds it with `AddOperative.proc_journal`
- **THEN** the driver SHALL enter the journaled iteration and skip the procedures committed in it

#### Scenario: A fresh bind carries no journal

- **WHEN** `AddOperative` arrives with no `proc_journal`
- **THEN** `proc_journal_resume()` SHALL be `None` and the chain SHALL run from the top

### Requirement: The worker pool grows under starvation and shrinks when idle

The Rust SDK SHALL spawn its base poll-worker pool as today
(`FLEZHA_WORKER_POOL_SIZE`, default = CPU count) and SHALL additionally park
one supervisor thread in the core's starvation wait. On each starvation wake
the supervisor SHALL spawn one additional poll worker, up to a ceiling
(`FLEZHA_WORKER_POOL_MAX`, default 10× CPU count). Workers above the base
count SHALL exit after a sustained idle streak; base workers SHALL NOT exit.
A workload that never starves SHALL observe exactly the base pool.

The pool's live-worker accounting SHALL be unwind-safe: a worker that leaves for
any reason — clean exit, panic, or any other unwind — SHALL release its slot, so
a departed worker can never occupy capacity in the ceiling. The supervisor SHALL
additionally restore the pool to its base size whenever it finds the live count
below `base`, replacing a worker that died rather than waiting for a starvation
signal that a shrunken-but-sufficient pool may never produce. Each replacement
SHALL be logged with the reason available, so a pool respawning into a repeating
crash is visible.

Every poll worker SHALL answer each activation it takes exactly once — complete,
fail, or abandon — through a guard that runs on unwind, so a panic escaping the
dispatcher's own guards re-hands the activation instead of retiring its entity.

#### Scenario: Blocking user code no longer halts the runner

- **WHEN** activations blocking longer than the starvation detection interval
  occupy every base worker while work for other entities is queued
- **THEN** additional workers SHALL be spawned (one per starvation signal, up
  to the ceiling) and the queued work SHALL be served without waiting for the
  blocked activations to finish

#### Scenario: The pool shrinks back after the burst

- **WHEN** spawned workers above the base count receive no work for the idle
  streak duration
- **THEN** they SHALL exit, returning the pool to its base size

#### Scenario: The ceiling bounds growth

- **WHEN** starvation persists with the pool at `FLEZHA_WORKER_POOL_MAX`
- **THEN** no further threads SHALL be spawned and remaining work SHALL wait
  for a completion, exactly as the whole pool does today at CPU count

#### Scenario: A dead base worker is replaced

- **WHEN** a base poll worker unwinds out of its loop
- **THEN** its slot SHALL be released, the supervisor SHALL spawn a replacement
  returning the pool to `base`, and the replacement SHALL be logged

#### Scenario: A dead worker does not consume the ceiling

- **GIVEN** a pool at base size where one worker has died
- **WHEN** starvation is subsequently observed
- **THEN** growth SHALL be measured against the live worker count, and the
  ceiling SHALL NOT be reached earlier because of the departed worker

#### Scenario: An escaping panic re-hands rather than retiring the entity

- **WHEN** a panic escapes the dispatcher's own guards while a worker holds an
  activation for entity E
- **THEN** the activation SHALL be abandoned by the worker's guard, E SHALL be
  pollable again with its items intact, and the replacement worker SHALL serve it

### Requirement: Pool workers run activations inside the core's activation scope

A poll worker SHALL run every activation — polled from the core or served
inline for a consumer it is blocked on — inside `flz_core`'s activation scope,
so a `DropNothing` push made from user code into a full consumer drains that
consumer inline instead of parking (see `sdk-poll-core`). The wire-reader
thread SHALL NOT enter that scope: its pushes park, and the pool drains the
consumer.

#### Scenario: An inline-served round counts as an activation

- **WHEN** a worker blocked on a full consumer serves that consumer's round
  inline, and a procedure in that round emits under `DropNothing` into another
  full consumer
- **THEN** the nested push also serves inline, because the thread is still
  inside an activation

#### Scenario: The wire reader never serves

- **WHEN** the stream reader's push into an entry mailbox finds it full under
  `DropNothing`
- **THEN** the reader parks on the consumer's parker until the pool pops a
  slot, and no procedure runs on the reader thread

