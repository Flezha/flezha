# rust-sdk-dispatch Specification

## Purpose
This capability covers the Rust SDK's Operative/Operation/Segment dispatch path: sticky per-Segment ordering invariants under ThreadPool dispatch, the dispatch throughput floor on the reference workload, the per-message dispatch-overhead budget, live Operative-instance registry concurrency, reconnect semantics across transient and intentional disconnects, command-dispatch byte-stability for surviving wire commands, the per-Segment flusher's single-thread linger drain (with toggle-aware semantics for `persistence_enabled = false`), the bounded `state.apply` hot-path cost, drain-time coalescing, the public `OperationTestHarness` testing surface, and the 16-byte version header stamped on persisted Changes.
## Requirements
### Requirement: Sticky Per-Segment Ordering Under ThreadPool Dispatch

Under the SDK's ThreadPool dispatch (the only dispatch mode in v1; see the REMOVED requirement below), the Rust SDK SHALL guarantee that for any single `(operative_instance, operation, segment)` triple, two invocations of that Operation's `run(...)` method on that Segment never execute concurrently, and invocations observe their inputs in the exact order those inputs were enqueued onto the Segment's mailbox. This invariant MUST hold regardless of worker-pool size, number of concurrent Operative instances, number of Segments per Operation, or transport (UDS/QUIC).

#### Scenario: Same segment never runs concurrently under load
- **WHEN** 10,000 inputs targeting the same `(operative_instance, operation, segment_key)` triple arrive back-to-back on one or more entry channels while the SDK uses a 16-thread worker pool
- **THEN** at any wall-clock instant, at most one thread SHALL be inside that Segment's `run(...)` method

#### Scenario: Different segments of same operation may run concurrently
- **WHEN** inputs arrive simultaneously for two distinct segments of the same Operation (for example `("ohlc", "1m")` and `("ohlc", "1h")`) and the worker pool has ≥ 2 threads
- **THEN** the two Segments' `run(...)` methods MAY execute concurrently on different threads

#### Scenario: Per-segment inputs delivered in arrival order
- **WHEN** inputs `"a"`, `"b"`, `"c"` are enqueued onto one Segment's mailbox in that order with an append-to-state Operation
- **THEN** the Segment's State SHALL end with `["a", "b", "c"]` in exactly that order

#### Scenario: Ordering holds across multiple entry streams
- **WHEN** the agent opens 4 entry connections for a single Operative instance and the same Segment receives interleaved inputs from all 4
- **THEN** `run(...)` calls for that Segment SHALL still be non-overlapping, though the interleaving order between streams is not constrained by this requirement

### Requirement: Dispatch Throughput Floor on Reference Workload

On the regression bench (`100 operatives × 1 operation × 1 segment × 10K msg`, no-op Operation, UDS transport, default worker-pool size; see `operative-architecture/design.md` Decision 16) invokable via `mix bench --runtime rust --shape regression -n 100 -m 10000`, the Rust SDK SHALL sustain at least **the floor measured against this change at merge time** of end-to-end message-processing throughput on commodity developer hardware (modern x86_64 laptop, 8+ cores, non-constrained CPU). Measured on the `Throughput:` line of the bench output (total messages ÷ message-processing elapsed time).

The literal floor value SHALL be recorded in this requirement at merge time as a literal number, replacing the placeholder `<RECORDED_AT_MERGE>`. The floor recorded by `operative-bench-harness` (480_000 msg/s) SHALL NOT be carried forward without measurement; this change adds a per-push success-counter atomic op and may shift the floor by single-digit percent. The CI gate SHALL track the new value.

The literal floor value SHALL be recorded in `docs/benchmarking.md` and updated only in tandem with this requirement: when a future change intentionally raises or lowers the floor, the spec line and the doc value SHALL be updated in the same PR. CI SHALL verify spec/doc agreement on every build (`mix bench.floor` prints the recorded number; the `bench-regression` GitHub Actions job parses both the spec line and the doc value, fails the build if they disagree).

The CI gate SHALL fail the build when measured throughput on the `bench-regression` job is below `0.9 × BENCH_FLOOR_MSG_PER_SEC` (10% tolerance band absorbs runner-image variance). Below `0.9 × BENCH_FLOOR_MSG_PER_SEC` is treated as a regression even if no individual code change in the PR is the obvious culprit; investigate before merging.

The umbrella's prior 350,000 msg/s floor (from before the cutover) SHALL NOT be carried forward as the gate.

#### Scenario: Reference bench meets the floor
- **WHEN** a developer runs `MIX_ENV=prod mix bench --runtime rust --shape regression -n 100 -m 10000` on a modern 8-core x86_64 laptop with a warm build
- **THEN** the reported `Throughput:` line SHALL be ≥ the recorded floor value

#### Scenario: CI gate fails on a 15% regression
- **WHEN** a PR's `bench-regression` job measures throughput at `0.85 × BENCH_FLOOR_MSG_PER_SEC` (15% below the recorded floor)
- **THEN** the CI job SHALL fail with a message naming the recorded floor, the measured throughput, the percentage shortfall, and a pointer to `docs/benchmarking.md` for triage instructions

#### Scenario: CI gate passes within tolerance
- **WHEN** a PR's `bench-regression` job measures throughput at `0.92 × BENCH_FLOOR_MSG_PER_SEC` (8% below the recorded floor — within the 10% tolerance band)
- **THEN** the CI job SHALL pass with a warning logged to the job output noting the proximity to the gate

#### Scenario: Spec/doc floor sync check fails when the two diverge
- **WHEN** a contributor edits the `BENCH_FLOOR_MSG_PER_SEC` value in `docs/benchmarking.md` without updating the literal in this requirement
- **THEN** the `bench-regression` CI job's pre-bench sync check SHALL fail with a message identifying the divergence and pointing at both files

#### Scenario: Segmenting-stress bench scales with worker pool size
- **WHEN** the segmenting-stress bench (`8 operatives × 4 operations × 8 segments × 20K msg/operative`, fan-out per input to all 8 segments of the entry operation; invoked via `mix bench --shape segmenting --pool-sizes 1,8`) is run on an 8-core host
- **THEN** the throughput at pool size 8 SHALL be at least 4× the throughput at pool size 1, demonstrating that segmenting-mode dispatch is not the bottleneck
- **AND** the `bench-segmenting-scaling` CI job SHALL report the measured ratio to the PR check output without blocking the merge (informational gate)

### Requirement: Per-Message Dispatch-Overhead Budget

The Rust SDK's dispatch hot path — from a completed frame being delivered by the transport layer to the targeted Segment's `Operation::run(...)` being entered — SHALL consume no more than **4 synchronization operations per message** (where a synchronization operation is a mutex lock, atomic CAS, channel send, or equivalent memory-ordering primitive). Counted under single-threaded contention on an Operation with `operation_keys() == []` (single-segment mode), exercising the entry-channel → registry → mailbox → drain path.

The 4-op budget covers: (1) registry / topology lookup for the entry channel ID → Operative instance pointer (one DashMap read or equivalent); (2) mailbox enqueue (`crossbeam::queue::ArrayQueue::try_push`, one logical step counted as one op even though its lock-free implementation may use a CAS on the tail plus a release-store on the slot); (3) `pushes_total` counter increment (one Relaxed atomic); (4) inflight CAS (one atomic compare-exchange).

The budget rises from 3 to 4 ops compared with the prior unbounded-mailbox shape (`crossbeam::queue::SegQueue` push without success counter). The added op is the success counter introduced by `operative-bounded-mailbox`; the underlying push transition from SegQueue to ArrayQueue is counted as the same logical step (one mailbox enqueue) and does not on its own raise the budget.

For static-mode operations, the additional per-input cost of segment fan-out (HashMap lookup per segment key, one mailbox enqueue + one counter increment + one inflight CAS per target segment) is intrinsic to the workload and counted separately — it is not part of the per-message budget. The fan-out formula updates from `3 + N × 2` to `4 + N × 3` where N is the segment fan-out width.

The 4-op invariant is an analytical bound enforced by code review, consistent with the convention in `sdk/rust/benches/correctness.rs:13-19`. The microbench reports raw msg/s for diagnostic comparison against the recorded `SINGLE_SEGMENT_FLOOR_MSG_PER_SEC` value in `docs/benchmarking.md`; it does not assert the sync-op count programmatically.

#### Scenario: Single-segment microbench confirms the budget
- **WHEN** the `throughput_single_segment` Criterion bench function in `sdk/rust/benches/correctness.rs` runs with contention pinned to one thread, single-segment Operation, no fan-out (invoked via `cargo bench --bench correctness -- throughput_single_segment`)
- **THEN** the analytical sync-op count per message SHALL be ≤ 4 operations on the hot path (registry lookup, mailbox enqueue, success counter, inflight CAS)
- **AND** the bench SHALL emit a `Throughput:` line in its Criterion output that callers can compare against the recorded `SINGLE_SEGMENT_FLOOR_MSG_PER_SEC` value in `docs/benchmarking.md`

#### Scenario: Static-mode segment fan-out cost is proportional to segment count
- **WHEN** the `throughput_static_fanout` Criterion bench function in `sdk/rust/benches/correctness.rs` is run against an Operation with `operation_keys()` of length 4 and `segment_for` returning all four keys per input (invoked via `cargo bench --bench correctness -- throughput_static_fanout`)
- **THEN** the per-input sync-op count SHALL be approximately `4 + 4 × 3 = 16` (one registry lookup + 4 segments × (mailbox enqueue + success counter + inflight CAS)), and SHALL NOT exceed `4 + N × 3` where N is the segment fan-out width
- **AND** the bench SHALL emit a `Throughput:` line whose value, divided by the single-segment floor, SHALL be approximately `1 / (1 + 4 × 3 / 4) = 0.25` ± 30% (the analytical bound is approximate; the bench reports raw msg/s for sanity checking, not as a strict gate)

### Requirement: Live Registry Concurrent Access

The live Operative-instance registry used by the Rust SDK SHALL support concurrent reads from many threads without inter-reader blocking, and writes (Operative instance create / destroy) SHALL NOT block readers for longer than a single operation's critical section. The registry SHALL preserve the existing public API shape: Operative classes register at startup via `register_unbounded::<R: UnboundedOperative>()`; `AddOperative` / `RemOperative` wire commands add and remove Operative instances by what the wire calls `operative_id` (mapped to the SDK's `operative_instance_id`); lookup by that id returns an `Arc<UnboundedInstance>` or equivalent handle.

The "registry" name carries forward from the pre-change implementation; what's stored in it changes from `Arc<Mutex<UnboundedBox>>` to `Arc<UnboundedInstance>`. The concurrency guarantees are unchanged.

#### Scenario: 100 reader threads do not stall on a single writer
- **WHEN** 100 threads each perform 10,000 registry lookups while a 101st thread registers and removes Operative instances in a loop
- **THEN** no reader thread observes a latency spike greater than 1 millisecond attributable to the writer

#### Scenario: Registry preserves public registration contract
- **WHEN** user code calls `register_unbounded::<R>()` at startup and later `AddOperative` triggers an Operative instance creation
- **THEN** the resulting `UnboundedInstance` SHALL be looked up by id and have its entry channels routable exactly as the spec requires, observable via the `operative-operation-segment-model` capability's scenarios

### Requirement: Reconnect Semantics Preserved

Across a control-channel reconnect the Rust SDK SHALL preserve its `operative_registry`, shared I/O streams, and `executor_id` (no clear-and-reassign window where frames are dropped). In addition:

1. **Live-sink resolution** — the per-Segment state send path (`SegmentStateSink`) SHALL resolve the *live* secondary stream on every send rather than caching a stream handle, so a reconnect's freshly-wired stream is used without re-registering Segments.
2. **Reconnect ring** — when no sink is installed or a write fails (the reconnect window), state-change frames SHALL be buffered in a bounded ring (drop-oldest on overflow) and replayed through the new sink on install, so changes produced mid-reconnect are not silently lost.
3. **Broken-pipe flood → control reconnect** — sustained write failures (a flood within a short window) SHALL trigger a full control-channel reconnect via the coordinator rather than spinning on a dead stream.

#### Scenario: state changes produced during a reconnect are not lost
- **GIVEN** the secondary stream drops and the SDK is reconnecting
- **WHEN** the operative produces state changes before the new stream is wired
- **THEN** the frames are buffered in the reconnect ring and replayed once the new sink is installed, preserving seq order.

#### Scenario: a broken-pipe flood drives a control reconnect
- **GIVEN** the state send path sees a flood of write failures within the detection window
- **WHEN** the threshold is crossed
- **THEN** the SDK requests a full control-channel reconnect from the coordinator instead of continuing to write to the dead stream.

### Requirement: Command Dispatch Behavior Preserved Byte-for-Byte for Surviving Commands

Every `CommandRequest` variant that survives the cutover (`AssignExecutorId`, `ConnectSocket`, `AddOperative`, `RemOperative`, `ExecutorDisconnect`) SHALL produce a `CommandResponse` byte-for-byte identical to the pre-change implementation for every input that exercised a given path before the change. New error paths MAY be introduced only if pre-change code hit them via panics; otherwise these command responses MUST NOT change.

`SegmentState` and `MigrationSerialization` are EXCLUDED from this byte-identical guarantee per `operative-mvp-rust/design.md` Decision 10: their on-the-wire JSON state-blob shape changes from `<state>` to `{"<operation_name>": <state>}` (or, for static-mode operations, `{"<operation_name>": {"<operation_key>": <state>, ...}, ...}`). This is acceptable because (a) state migration is umbrella Non-Goal 2 and tracked in `operative-state-migration`, (b) the umbrella's hard-cutover policy means no production scenario crosses old-SDK and new-SDK state blobs, and (c) the new shape is documented in `docs/sdk_protocol_spec.md`.

`OperationMessage` and `OperationMessageResponse` continue to exist on the wire and the agent side; the SDK side responds with a canonical "unsupported" error for every request until `operative-message-handlers` adds per-Operation `handle_message` (see `operative-mvp-rust/design.md` Decision 9). This is byte-equivalent to today's behavior for any operative that did not implement the optional `handle_message` callback.

#### Scenario: AddOperative with init_values produces same response shape
- **WHEN** the agent sends `AddOperative{operative_id:"p1", factory_id:"market", init_values:"{\"symbol\":\"BTC\"}"}` and the identical command is sent to both pre- and post-change SDK builds where `"market"` is registered via the new `register_unbounded::<MarketOperative>()` (post-change) or `register_factory("market", ...)` (pre-change)
- **THEN** both SDKs SHALL return a `CommandResponse` with the same `reference_id`, `status="success"`, and `errors=[]` (the `raw_data` bytes are not part of this requirement because the post-change `init_values` semantics are per-Operation, see the `operative-operation-segment-model` capability)

#### Scenario: ExecutorDisconnect closes sockets identically
- **WHEN** `ExecutorDisconnect{}` is sent under both implementations
- **THEN** both SDKs SHALL close their primary control stream and return identical `CommandResponse` bytes

#### Scenario: OperationMessage returns canonical unsupported error pre-operative-message-handlers
- **WHEN** an agent sends `OperationMessage{operative_id:"p1", payload:<...>}` to a v1 SDK
- **THEN** the SDK SHALL respond with `OperationMessageResponse{ payload: "", error: "operative does not support handle_message" }`

### Requirement: Per-Segment flusher single-thread linger drain

The Rust SDK SHALL spawn at most one `SegmentFlusher` OS thread per process. The thread SHALL be lazy-initialized via `OnceLock` on the first `register_unbounded::<R>()` call that registers an Operation with a non-`NoChange` Change type. Programs that register only `NoChange` Operations SHALL NOT spawn a flusher thread.

The flusher SHALL run an infinite loop that on each iteration: (a) sleeps for `linger_ms` milliseconds (default 5; configurable via env var `FLEZHA_FLUSH_LINGER_MS`); (b) iterates the registered Segments; (c) for each Segment, attempts to acquire the buffer's `Mutex`, and on success calls `FlushableBuffer::drain_to_frames` which: applies coalescing, builds wire frames, releases the lock; (d) for each frame, invokes the Segment's `send_frame` function pointer (stub in this change; agent-wired in `operative-state-persistence`).

#### Scenario: NoChange-only program does not spawn flusher

- **WHEN** a Rust process registers `register_unbounded::<R>()` for an Operative whose Operations all have `Change = NoChange`
- **THEN** no `SegmentFlusher` thread SHALL be spawned, observable via thread-count assertions in the test (`std::thread::available_parallelism` baseline + assertion that no `flezha-flusher` named thread exists)

#### Scenario: First non-NoChange registration starts the flusher

- **WHEN** a Rust process registers `register_unbounded::<R>()` for an Operative containing an Operation with `Change = MyChange`
- **THEN** exactly one OS thread named `flezha-flusher` SHALL be spawned for the lifetime of the process

#### Scenario: Flusher drains all non-empty buffers per tick

- **WHEN** four Segments across two Operatives each have non-empty flush buffers and the flusher's linger window elapses
- **THEN** the flusher SHALL drain all four buffers in a single iteration and the buffers SHALL be empty after the drain

#### Scenario: Flusher honors FLEZHA_FLUSH_LINGER_MS

- **WHEN** `FLEZHA_FLUSH_LINGER_MS=20` is set at process start
- **THEN** the flusher SHALL sleep 20 ms between iterations; durability lag observed by the test fixture SHALL be at most 20 ms plus the agent fsync window

### Requirement: Toggle-aware drain semantics for persistence_enabled = false

The Rust SDK's `SegmentFlusher` SHALL inspect each registered Segment's `persistence_enabled` flag at drain time. When `false`, the flusher SHALL acquire the buffer's Mutex, invoke `FlushableBuffer::drain_to_frames` (which still applies coalescing), and discard the resulting frames without invoking `send_frame`. The Segment's `change_seq` counter SHALL still advance through the discarded frames.

When `true`, the flusher SHALL invoke `send_frame` for each frame and propagate any `Err(SendError)` by decrementing the seq counter so the next drain re-sends from the right point (no gaps in the agent's WAL view).

#### Scenario: Toggle-off drain discards without wire I/O

- **WHEN** an Operative with `persistence_enabled = false` records 10 Changes via `state.apply(...)` and the flusher's linger elapses
- **THEN** the Segment's flush buffer SHALL be empty after the tick AND the Segment's `send_frame` function SHALL NOT have been invoked AND the Segment's `change_seq` SHALL have advanced by the post-coalesce record count

#### Scenario: Toggle-on drain invokes send_frame per frame

- **WHEN** an Operative with `persistence_enabled = true` records 10 Changes (no coalescing) via `state.apply(...)` and the flusher's linger elapses
- **THEN** the Segment's `send_frame` function SHALL have been invoked exactly 10 times with frame bytes built from `[segment_id][seq: u64 BE][header: 16B][change_bytes][crc32: u32 BE]`

#### Scenario: send_frame failure preserves seq monotonicity

- **WHEN** the Segment's `send_frame` returns `Err(SendError::TransportDown)` for the third frame in a 10-frame batch
- **THEN** the flusher SHALL stop the batch at that frame, decrement `change_seq` to undo the failed-and-following advances (the first two frames' seq advances stay), and SHALL retain the un-sent Changes in the buffer for the next drain (records 3-10 are re-pushed via a side channel that preserves their post-coalesce order)

### Requirement: Hot-path cost is bounded to one Vec::push per state.apply

The Rust SDK's `state.apply(change)` SHALL pay exactly: one `Operation::apply_change(state, &change)` call (user-supplied; cost varies) plus one `Vec::push(change)` operation. The hot path SHALL NOT serialize, fsync, network-send, or acquire any lock beyond the existing per-Segment sticky CAS and the `&mut self` borrow on the buffer (which is already exclusive via the runner thread's ownership of the segment's buffer for the duration of `Operation::run`).

The microbench `state_apply_throughput` (`sdk/rust/benches/state.rs`) SHALL exercise this requirement and report nanoseconds per `state.apply(...)` call across change-payload-size and coalesce-key dimensions; CI SHALL fail the build if any dimension regresses >50% from the recorded baseline.

#### Scenario: No serialization on state.apply

- **WHEN** an Operation's `run` calls `state.apply(MyChange::Foo)` 100 times in a tight loop
- **THEN** zero serialization calls (`serde::Serialize::serialize`, `postcard::to_allocvec`, `bincode::serialize`, etc.) SHALL be issued from the runner thread during these 100 calls

#### Scenario: No I/O on state.apply

- **WHEN** an Operation's `run` calls `state.apply(MyChange::Foo)` 100 times
- **THEN** zero `write`, `send`, `fsync`, `fdatasync`, or NIF-call syscalls SHALL be issued from the runner thread during these 100 calls

#### Scenario: state_apply_throughput microbench reports < 200 ns/op for SmallChange + NoCoalesceKey

- **WHEN** the `state_apply_throughput` Criterion bench runs with `change_kind = SmallChange (8B)` and `coalesce_kind = NoCoalesceKey` and the buffer is preallocated to capacity 1024
- **THEN** the reported median time per `state.apply(...)` SHALL be < 200 ns on the reference host

### Requirement: Coalescing is applied at drain time, not on the hot path

The Rust SDK SHALL apply coalescing inside `FlushableBuffer::drain_to_frames`, AFTER the buffer's Mutex is acquired by the flusher. The hot path of `state.apply(...)` SHALL NOT touch any coalescing data structure.

The drain-time coalescing algorithm SHALL: (a) build a `HashMap<CoalesceKey, usize>` mapping each key to the index of the latest record carrying that key; (b) build a parallel `Vec<bool>` mask marking earlier records with the same key as discarded; (c) emit kept records in their original buffer order. Records with `coalesce_key() == None` SHALL be retained verbatim.

#### Scenario: Same-key records collapse to latest

- **WHEN** an Operation records 50 `BarTick { interval: M1, ts: T }` Changes via `state.apply(...)` within one linger window, all returning `Some(CoalesceKey::from_hash(&(M1, T)))` from `coalesce_key()`
- **THEN** the flusher's drain SHALL emit exactly one frame whose Change is the 50th `BarTick`

#### Scenario: No-key records are preserved verbatim

- **WHEN** an Operation records 10 Changes whose `coalesce_key() == None` within one linger window
- **THEN** the flusher's drain SHALL emit 10 frames in the same order they were pushed

#### Scenario: Mixed coalesce-key and no-key records

- **WHEN** an Operation records, in order: A (no key), B (key=K1), C (key=K1), D (no key), E (key=K2)
- **THEN** the flusher's drain SHALL emit frames in order: A, C, D, E (B is collapsed to C; A and D are preserved verbatim; E is retained)

### Requirement: OperationTestHarness in public API surface (Rust SDK)

The Rust SDK SHALL export `OperationTestHarness::<S: Operation>` under `flezha_executor::testing::OperationTestHarness`, gated by `cfg(any(test, feature = "testing"))`. The harness SHALL support:

- `OperationTestHarness::<S>::new()` (requires `S: Default`) — constructs a harness with `S::init_state(None, None)`.
- `OperationTestHarness::<S>::with_initial_state(state: S::State)` — explicit initial state.
- `OperationTestHarness::<S>::with_payload(operation_key, payload)` — replicates the runtime's snapshot+changelog restoration flow.
- `apply_inputs(iter: impl IntoIterator<Item = S::Input>) -> &mut Self` — invokes `S::run` for each input on the harness's owned state, recording emits and Changes.
- `expect_changes(self, expected: &[S::Change]) -> Self` (consuming) — asserts equality, requires `S::Change: PartialEq + Debug`.
- `expect_emits<C>(self, expected: &[C]) -> Self` (consuming) — asserts emits of type `C` equal expected.
- `expect_state(self, expected: &S::State) -> Self` (consuming) — asserts state equality, requires `S::State: PartialEq + Debug`.
- `replay(initial: S::State, log: &[S::Change]) -> S::State` — folds the log via `S::apply_change`.
- `replay_equals(&self) -> Result<(), ReplayMismatch>` — replays self.recorded_changes against a re-constructed initial state and asserts byte-equality (via `serde::Serialize` round-trip) with `self.state`.

#### Scenario: Harness asserts on recorded Changes

- **WHEN** a test runs `OperationTestHarness::<CounterOperation>::new().apply_inputs(vec![Input::Add(5), Input::Add(3)]).expect_changes(&[CounterChange { delta: 5 }, CounterChange { delta: 3 }])`
- **THEN** the chain SHALL succeed and produce a harness whose `recorded_changes` equals the expected slice

#### Scenario: Harness replays for determinism check

- **WHEN** a test runs `OperationTestHarness::<CounterOperation>::new().apply_inputs(inputs).replay_equals()`
- **THEN** the result SHALL be `Ok(())` because `CounterOperation::apply_change` is deterministic

#### Scenario: Harness detects non-deterministic apply_change

- **WHEN** an Operation's `apply_change` reads `SystemTime::now()` and uses it to seed a state field, AND the harness runs `replay_equals()`
- **THEN** the result SHALL be `Err(ReplayMismatch { expected_serialized, actual_serialized })` whose `Display` impl renders a hex-diff identifying the divergent state field

#### Scenario: Harness compiled-out without testing feature

- **WHEN** a downstream crate depends on `flezha_executor` without enabling the `testing` feature flag and does not build under `#[cfg(test)]`
- **THEN** `flezha_executor::testing` SHALL NOT be visible and references to `OperationTestHarness` SHALL fail to compile

### Requirement: 16-byte version header stamped on persisted Changes

The Rust SDK SHALL stamp a 16-byte header on every Change record emitted to the wire path, per the umbrella's spec:

- bytes 0:1 — `sdk_lang: u8` SHALL equal `1` (Rust).
- bytes 1:5 — `sdk_version: u32 BE` SHALL be parsed from `env!("CARGO_PKG_VERSION")` at SDK build time as `(major << 16) | (minor << 8) | patch`.
- bytes 5:9 — `schema_version: u32 BE` SHALL equal `S::SCHEMA_VERSION` for the Operation owning the Change.
- bytes 9:10 — `format_id: u8` SHALL equal `1` (postcard) in v1.
- bytes 10:12 — reserved, MUST be zero.
- bytes 12:16 — `crc32: u32 BE` SHALL be `crc32fast::hash(&change_bytes)`.

The header SHALL be stamped in `HeaderTemplate::stamp(change_bytes: &[u8]) -> [u8; 16]` and verified in `decode_header` on the restoration path.

#### Scenario: Header bytes match the umbrella spec

- **WHEN** a Rust SDK at `version 0.5.3` and Operation `SCHEMA_VERSION = 1` stamps a header for change_bytes `&[0x01, 0x02, 0x03]`
- **THEN** the resulting 16-byte header SHALL equal `[0x01, 0x00, 0x00, 0x05, 0x03, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, <crc32 BE bytes of [0x01, 0x02, 0x03]>]`

#### Scenario: Cross-language load fails loud

- **WHEN** the Rust SDK attempts to decode a header whose `sdk_lang` byte is `2` (Java)
- **THEN** `decode_header` SHALL return `Err(RestoreError::LangMismatch { expected: 1, found: 2 })` and SHALL NOT proceed to deserialization

#### Scenario: schema_version mismatch fails loud

- **WHEN** the Rust SDK at Operation `SCHEMA_VERSION = 2` attempts to decode a record whose header carries `schema_version = 1`
- **THEN** `decode_header` SHALL return `Err(RestoreError::SchemaMismatch { expected: 2, found: 1 })` and SHALL NOT proceed to deserialization (registered migrators are reserved by the umbrella but not implemented in this change)

#### Scenario: crc32 mismatch fails loud

- **WHEN** the Rust SDK decodes a record whose header crc32 does not match a recomputed crc32 over the change_bytes
- **THEN** `decode_header` SHALL return `Err(RestoreError::CrcMismatch { segment, seq })` and SHALL NOT proceed to deserialization

### Requirement: Public Rust SDK API uses canonical Operative/Executor vocabulary

The Rust SDK SHALL expose its top-level dispatch types under the names `ExecutorNode` (formerly `ComputeEngineNode`) and provide a method `get_operatives()` (formerly `get_processors()`) returning the registered Operatives on the current Executor. The module path SHALL be `sdk/rust/src/executor/` (formerly `sdk/rust/src/compute_engine/`). Generated protobuf types SHALL reflect the renamed messages: `AssignExecutorId`, `ExecutorDisconnect`, `AddOperative`, `RemOperative`, `SegmentState`, `SegmentApplyState`, `OperationMessage`, `OperationMessageResponse`, plus existing `RestoreInitState`, `CompactSegment`, `SegmentMailboxStatus`.

#### Scenario: Public types compile under new names
- **WHEN** a downstream Rust crate depends on `flezha-sdk` and writes `use flezha_sdk::executor::ExecutorNode;`
- **THEN** the import MUST resolve and the compiler MUST accept it; an import via the legacy path `flezha_sdk::compute_engine::ComputeEngineNode` MUST NOT compile

#### Scenario: get_operatives returns the local Operative registry
- **WHEN** a Rust SDK Executor has Operatives `r1` and `r2` registered locally
- **THEN** `executor_node.get_operatives()` MUST return a collection containing exactly `r1` and `r2`; the legacy method `get_processors()` MUST NOT be exposed

### Requirement: Generated payload-id constants use canonical names

The Rust SDK's payload-id constants (those that map to the binary framing's `payload_id(32)` byte slot) SHALL be named consistent with the canonical vocabulary: `SEGMENT_STATE_CHANGE`, `SEGMENT_STATE_SNAPSHOT`, `RESTORE_INIT_STATE`, `COMPACT_SEGMENT`, `SEGMENT_MAILBOX_STATUS`, `CONNECT_CONTROL_CHANNEL`, `CONTROL_COMMAND`. The previous `PROCESSOR_*` family SHALL be removed.

#### Scenario: Payload-id constants compile and resolve
- **WHEN** the Rust SDK is built
- **THEN** the constants `SEGMENT_STATE_CHANGE` and `SEGMENT_STATE_SNAPSHOT` MUST be defined and exported; references to `PROCESSOR_STATE_CHANGE` MUST not exist

### Requirement: Test fixtures' Operations exercise the output channel path

The test fixtures under `test/apps/rust_executor/src/main.rs` SHALL emit at least one output frame per input received, via `OperationCtx::output(channel, value, metadata)`, on Operatives exercised by the controller's `:on_output`-subscribed plugin tests. Specifically:

- `StoreInputOperative::StoreInputOperation::run` SHALL emit the input string on a sink Channel (named `"output"`) after applying the `PushText` change.
- `CompareFilterOperative::CompareFilterOperation::run` SHALL emit the input string on a sink Channel when `state.comparable == input` (matching the existing test's expectation of `"hello world"` round-tripping through the filter).

This requirement covers fixture behavior only; it does not change the SDK's `Operation` trait, the dispatch contract, or any wire format. A future production Operative may emit no outputs and remain conformant to the SDK's other requirements.

#### Scenario: StoreInputOperative round-trips input via :on_output

- **WHEN** the controller's `BeamMsgForward` plugin is enabled with `subscriptions: [{:on_output, []}]` against a `StoreInputOperative` instance and the test sends `"state_1"` to the operative
- **THEN** the test SHALL receive `{:message_forward, "state_1"}` within 2 seconds (UDS) or 5 seconds (QUIC)

#### Scenario: CompareFilterOperative round-trips matching input via :on_output

- **WHEN** a `CompareFilterOperative` instance is initialized with `comparable = "hello world"` and the test sends `"hello world"` to the operative
- **THEN** the test SHALL receive `{:message_forward, "hello world"}` within 2 seconds

### Requirement: `ExecutorReconnected` control message defined and emitted

The SDK SHALL emit an `ExecutorReconnected { previous_id, new_id, reason }` control frame as the first message after re-establishing the control stream, and SHALL emit a single structured reconnect log line. The controller SHALL use this frame to compute the binding delta: a stable reconnect under the same id is a zero-frame no-op; a controller-side reassignment during the disconnect window triggers `ReassignExecutorId { new_id }` and a re-issue of the operative set. `reason` SHALL be one of the transport-edge causes (e.g. broken pipe, read/write error, keepalive timeout, controller-requested).

#### Scenario: a stable reconnect is a zero-delta no-op
- **GIVEN** the SDK reconnects within the disconnect window holding the same `executor_id`
- **WHEN** it sends `ExecutorReconnected{previous_id == current, new_id == current}`
- **THEN** the controller's bindings already agree and no `AddOperative`/`RemOperative` frames are issued.

### Requirement: Node-local persisted executor id announced as `previous_executor_id`

On a fresh process start (not a reconnect) the SDK SHALL load its node-local persisted executor id (atomically written via tmp+rename at `FLEZHA_EXECUTOR_ID_PATH`) and announce it as `previous_executor_id` in the connect handshake, so the controller evicts the stale binding from the prior process before binding the new one. Reconnects SHALL NOT re-announce it (identity is preserved in-place).

#### Scenario: a restarted process reclaims its bindings
- **GIVEN** an SDK process held `executor_id = default_X` and persisted it locally
- **WHEN** the process is killed and a fresh process starts, reads `default_X`, and announces it as `previous_executor_id`
- **THEN** the controller evicts the stale `default_X` binding before the new bind, so no operative is left bound to the dead process.
