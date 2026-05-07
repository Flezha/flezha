# rust-sdk-dispatch Specification

## Purpose
TBD - created by archiving change optimize-rust-sdk-dispatch. Update Purpose after archive.
## Requirements
### Requirement: Sticky Per-Processor Ordering Under ThreadPool Runner

Under `ProcessorRunner::ThreadPool`, the Rust SDK SHALL guarantee that for any single processor instance, two invocations of its `process(...)` method never execute concurrently, and invocations observe their inputs in the exact order those inputs were received on the primary stream. This invariant MUST hold regardless of worker-pool size, number of concurrent processors, or transport (UDS/QUIC).

#### Scenario: Same processor never runs concurrently under load
- **WHEN** 10,000 frames targeting the same processor id arrive back-to-back on one or more primary streams while the compute engine uses a 16-thread worker pool
- **THEN** at any wall-clock instant, at most one thread SHALL be inside that processor's `process(...)` method

#### Scenario: Inputs are delivered in arrival order
- **WHEN** inputs `"a"`, `"b"`, `"c"` are written to the primary stream in that order, targeting a single processor with an append-to-state handler
- **THEN** the processor's final state SHALL end with `["a", "b", "c"]` in that order

#### Scenario: Ordering holds across multiple primary streams
- **WHEN** the agent opens 4 primary streams and the same processor receives interleaved inputs from all 4
- **THEN** `process(...)` calls for that processor SHALL still be non-overlapping, though the interleaving order between streams is not constrained by this requirement

### Requirement: ThreadSpawn Runner Preserves Per-Invocation Isolation

Under `ProcessorRunner::ThreadSpawn`, the Rust SDK SHALL invoke each processor input on a freshly-spawned OS thread (or a dedicated equivalent with identical semantics). Invocations SHALL NOT share any worker-pool state across calls, and MAY run concurrently for the same processor id — ordering is NOT guaranteed for this runner, matching the behavior documented in `docs/sdk_protocol_spec.md` §8.

#### Scenario: Per-invocation thread start
- **WHEN** a processor configured with `ProcessorRunner::ThreadSpawn` receives N inputs in rapid succession
- **THEN** up to N distinct threads SHALL be observed executing `process(...)` concurrently

### Requirement: Dispatch Throughput Floor on Reference Workload

On the regression bench (`100 relays × 1 shard × 1 partition × 10K msg`, no-op Shard, UDS transport, default worker-pool size; see `relay-architecture/design.md` Decision 16) invokable via `mix bench --runtime rust --shape regression -n 100 -m 10000`, the Rust SDK SHALL sustain at least **the floor measured against this change at merge time** of end-to-end message-processing throughput on commodity developer hardware (modern x86_64 laptop, 8+ cores, non-constrained CPU). Measured on the `Throughput:` line of the bench output (total messages ÷ message-processing elapsed time).

The literal floor value SHALL be recorded in this requirement at merge time as a literal number, replacing the placeholder `<RECORDED_AT_MERGE>`. The floor recorded by `relay-bench-harness` (480_000 msg/s) SHALL NOT be carried forward without measurement; this change adds a per-push success-counter atomic op and may shift the floor by single-digit percent. The CI gate SHALL track the new value.

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

#### Scenario: Partitioning-stress bench scales with worker pool size
- **WHEN** the partitioning-stress bench (`8 relays × 4 shards × 8 partitions × 20K msg/relay`, fan-out per input to all 8 partitions of the entry shard; invoked via `mix bench --runtime rust --shape partitioning --pool-sizes 1,8`) is run on an 8-core host
- **THEN** the throughput at pool size 8 SHALL be at least 4× the throughput at pool size 1, demonstrating that partitioning-mode dispatch is not the bottleneck
- **AND** the `bench-partitioning-scaling` CI job SHALL report the measured ratio to the PR check output without blocking the merge (informational gate)

### Requirement: Per-Message Dispatch-Overhead Budget

The Rust SDK's dispatch hot path — from a completed frame being delivered by the transport layer to the targeted Partition's `Shard::run(...)` being entered — SHALL consume no more than **4 synchronization operations per message** (where a synchronization operation is a mutex lock, atomic CAS, channel send, or equivalent memory-ordering primitive). Counted under single-threaded contention on a Shard with `shard_keys() == []` (single-partition mode), exercising the entry-channel → registry → mailbox → drain path.

The 4-op budget covers: (1) registry / topology lookup for the entry channel ID → Relay instance pointer (one DashMap read or equivalent); (2) mailbox enqueue (`crossbeam::queue::ArrayQueue::try_push`, one logical step counted as one op even though its lock-free implementation may use a CAS on the tail plus a release-store on the slot); (3) `pushes_total` counter increment (one Relaxed atomic); (4) inflight CAS (one atomic compare-exchange).

The budget rises from 3 to 4 ops compared with the prior unbounded-mailbox shape (`crossbeam::queue::SegQueue` push without success counter). The added op is the success counter introduced by `relay-bounded-mailbox`; the underlying push transition from SegQueue to ArrayQueue is counted as the same logical step (one mailbox enqueue) and does not on its own raise the budget.

For static-mode shards, the additional per-input cost of partition fan-out (HashMap lookup per partition key, one mailbox enqueue + one counter increment + one inflight CAS per target partition) is intrinsic to the workload and counted separately — it is not part of the per-message budget. The fan-out formula updates from `3 + N × 2` to `4 + N × 3` where N is the partition fan-out width.

The 4-op invariant is an analytical bound enforced by code review, consistent with the convention in `sdk/rust/benches/correctness.rs:13-19`. The microbench reports raw msg/s for diagnostic comparison against the recorded `SINGLE_PARTITION_FLOOR_MSG_PER_SEC` value in `docs/benchmarking.md`; it does not assert the sync-op count programmatically.

#### Scenario: Single-partition microbench confirms the budget
- **WHEN** the `throughput_single_partition` Criterion bench function in `sdk/rust/benches/correctness.rs` runs with contention pinned to one thread, single-partition Shard, no fan-out (invoked via `cargo bench --bench correctness -- throughput_single_partition`)
- **THEN** the analytical sync-op count per message SHALL be ≤ 4 operations on the hot path (registry lookup, mailbox enqueue, success counter, inflight CAS)
- **AND** the bench SHALL emit a `Throughput:` line in its Criterion output that callers can compare against the recorded `SINGLE_PARTITION_FLOOR_MSG_PER_SEC` value in `docs/benchmarking.md`

#### Scenario: Static-mode partition fan-out cost is proportional to partition count
- **WHEN** the `throughput_static_fanout` Criterion bench function in `sdk/rust/benches/correctness.rs` is run against a Shard with `shard_keys()` of length 4 and `partition_for` returning all four keys per input (invoked via `cargo bench --bench correctness -- throughput_static_fanout`)
- **THEN** the per-input sync-op count SHALL be approximately `4 + 4 × 3 = 16` (one registry lookup + 4 partitions × (mailbox enqueue + success counter + inflight CAS)), and SHALL NOT exceed `4 + N × 3` where N is the partition fan-out width
- **AND** the bench SHALL emit a `Throughput:` line whose value, divided by the single-partition floor, SHALL be approximately `1 / (1 + 4 × 3 / 4) = 0.25` ± 30% (the analytical bound is approximate; the bench reports raw msg/s for sanity checking, not as a strict gate)

### Requirement: Processor Registry Concurrent Access

The live processor registry used by the Rust SDK SHALL support concurrent reads from many threads without inter-reader blocking, and writes (add / remove / migrate) SHALL NOT block readers for longer than a single shard's critical section. The registry SHALL preserve the existing public API: factories register at startup; `AddProcessor` / `RemProcessor` add and remove instances by processor id; lookup by processor id returns an `Arc<Mutex<ProcessorBox>>` or equivalent handle.

#### Scenario: 100 reader threads do not stall on a single writer
- **WHEN** 100 threads each perform 10,000 registry lookups while a 101st thread registers and removes processors in a loop
- **THEN** no reader thread observes a latency spike greater than 1 millisecond attributable to the writer

#### Scenario: Registry preserves public trait contract
- **WHEN** user code calls `register_factory(...)` at startup and later `AddProcessor` triggers the factory
- **THEN** the resulting processor instance SHALL be looked up by id and be invocable exactly as under the pre-change implementation (the user observes no API change)

### Requirement: Reconnect Semantics Preserved

On unexpected disconnect (BrokenPipe / UnexpectedEof / ConnectionReset / QUIC connection lost) the Rust SDK SHALL clear `io_streams`, clear the live processor registry, drain in-flight work, reset `compute_engine_id` to `"undefined"`, and reconnect with exponential backoff (200 ms → 30 s cap) per `docs/sdk_protocol_spec.md` §5. On intentional disconnect (`ComputeEngineDisconnect` handled) the SDK SHALL NOT reconnect. These semantics MUST hold identically to the pre-change implementation.

#### Scenario: Unexpected disconnect triggers full state reset + reconnect
- **WHEN** the agent process is killed while the SDK has 10 live processors with in-flight work in their mailboxes
- **THEN** the SDK SHALL drop all queued work, close mailboxes, reset state, and re-establish a fresh `compute_engine_id` once the agent is available again

#### Scenario: Intentional disconnect halts the reconnect loop
- **WHEN** the agent sends `ComputeEngineDisconnect{}` and the SDK handles it
- **THEN** the SDK SHALL close sockets and not attempt any reconnection

### Requirement: Command Dispatch Behavior Preserved Byte-for-Byte

Every `CommandRequest` variant (`AssignComputeEngineId`, `ConnectSocket`, `AddProcessor`, `RemProcessor`, `ProcessorState`, `MigrationSerialization`, `MigrationDeserialization`, `ComputeEngineDisconnect`) SHALL produce a `CommandResponse` byte-for-byte identical to the pre-change implementation for every input that exercised a given path before the change. New error paths MAY be introduced only if pre-change code hit them via panics; otherwise command responses MUST NOT change.

#### Scenario: AddProcessor with init_values deserializes state identically
- **WHEN** the agent sends `AddProcessor{processor_id:"p1", factory_id:"echo", init_values:"{\"n\":42}"}` and the identical command is sent to both pre- and post-change SDK builds
- **THEN** both SDKs SHALL return a `CommandResponse` with the same `reference_id`, `status="success"`, `errors=[]`, and `raw_data` bytes

#### Scenario: MigrationSerialization returns JSON-serialized state byte-identical
- **WHEN** a processor with non-trivial state receives `MigrationSerialization{processor_id:"p1"}` under both implementations
- **THEN** the `raw_data` bytes in the response SHALL be byte-identical between implementations

### Requirement: Public Rust API Unchanged

The Rust SDK crate `flezha_compute_engine` SHALL expose the same public items (functions, traits, types, modules) with the same signatures before and after this change. User code depending on this crate at any pinned version SHALL continue to compile and run without source modification.

#### Scenario: Consumer code compiles unchanged
- **WHEN** `test/apps/rust_compute_engine/src/main.rs` is compiled against the post-change crate with zero source modifications
- **THEN** compilation SHALL succeed and the produced binary SHALL exhibit the same runtime behavior observable via the SDK protocol

#### Scenario: Public API item diff is empty
- **WHEN** `cargo public-api` (or equivalent) is run comparing the pre- and post-change crate
- **THEN** the diff SHALL be empty except for items explicitly marked as private / internal in both versions

### Requirement: Per-Partition flusher single-thread linger drain

The Rust SDK SHALL spawn at most one `PartitionFlusher` OS thread per process. The thread SHALL be lazy-initialized via `OnceLock` on the first `register_relay::<R>()` call that registers a Shard with a non-`NoChange` Change type. Programs that register only `NoChange` Shards SHALL NOT spawn a flusher thread.

The flusher SHALL run an infinite loop that on each iteration: (a) sleeps for `linger_ms` milliseconds (default 5; configurable via env var `FLEZHA_FLUSH_LINGER_MS`); (b) iterates the registered Partitions; (c) for each Partition, attempts to acquire the buffer's `Mutex`, and on success calls `FlushableBuffer::drain_to_frames` which: applies coalescing, builds wire frames, releases the lock; (d) for each frame, invokes the Partition's `send_frame` function pointer (stub in this change; agent-wired in `relay-state-persistence-agent`).

#### Scenario: NoChange-only program does not spawn flusher

- **WHEN** a Rust process registers `register_relay::<R>()` for a Relay whose Shards all have `Change = NoChange`
- **THEN** no `PartitionFlusher` thread SHALL be spawned, observable via thread-count assertions in the test (`std::thread::available_parallelism` baseline + assertion that no `flezha-flusher` named thread exists)

#### Scenario: First non-NoChange registration starts the flusher

- **WHEN** a Rust process registers `register_relay::<R>()` for a Relay containing a Shard with `Change = MyChange`
- **THEN** exactly one OS thread named `flezha-flusher` SHALL be spawned for the lifetime of the process

#### Scenario: Flusher drains all non-empty buffers per tick

- **WHEN** four Partitions across two Relays each have non-empty flush buffers and the flusher's linger window elapses
- **THEN** the flusher SHALL drain all four buffers in a single iteration and the buffers SHALL be empty after the drain

#### Scenario: Flusher honors FLEZHA_FLUSH_LINGER_MS

- **WHEN** `FLEZHA_FLUSH_LINGER_MS=20` is set at process start
- **THEN** the flusher SHALL sleep 20 ms between iterations; durability lag observed by the test fixture SHALL be at most 20 ms plus the agent fsync window

### Requirement: Toggle-aware drain semantics for persistence_enabled = false

The Rust SDK's `PartitionFlusher` SHALL inspect each registered Partition's `persistence_enabled` flag at drain time. When `false`, the flusher SHALL acquire the buffer's Mutex, invoke `FlushableBuffer::drain_to_frames` (which still applies coalescing), and discard the resulting frames without invoking `send_frame`. The Partition's `change_seq` counter SHALL still advance through the discarded frames.

When `true`, the flusher SHALL invoke `send_frame` for each frame and propagate any `Err(SendError)` by decrementing the seq counter so the next drain re-sends from the right point (no gaps in the agent's WAL view).

#### Scenario: Toggle-off drain discards without wire I/O

- **WHEN** a Relay with `persistence_enabled = false` records 10 Changes via `state.apply(...)` and the flusher's linger elapses
- **THEN** the Partition's flush buffer SHALL be empty after the tick AND the Partition's `send_frame` function SHALL NOT have been invoked AND the Partition's `change_seq` SHALL have advanced by the post-coalesce record count

#### Scenario: Toggle-on drain invokes send_frame per frame

- **WHEN** a Relay with `persistence_enabled = true` records 10 Changes (no coalescing) via `state.apply(...)` and the flusher's linger elapses
- **THEN** the Partition's `send_frame` function SHALL have been invoked exactly 10 times with frame bytes built from `[partition_id][seq: u64 BE][header: 16B][change_bytes][crc32: u32 BE]`

#### Scenario: send_frame failure preserves seq monotonicity

- **WHEN** the Partition's `send_frame` returns `Err(SendError::TransportDown)` for the third frame in a 10-frame batch
- **THEN** the flusher SHALL stop the batch at that frame, decrement `change_seq` to undo the failed-and-following advances (the first two frames' seq advances stay), and SHALL retain the un-sent Changes in the buffer for the next drain (records 3-10 are re-pushed via a side channel that preserves their post-coalesce order)

### Requirement: Hot-path cost is bounded to one Vec::push per state.apply

The Rust SDK's `state.apply(change)` SHALL pay exactly: one `Shard::apply_change(state, &change)` call (user-supplied; cost varies) plus one `Vec::push(change)` operation. The hot path SHALL NOT serialize, fsync, network-send, or acquire any lock beyond the existing per-Partition sticky CAS and the `&mut self` borrow on the buffer (which is already exclusive via the runner thread's ownership of the partition's buffer for the duration of `Shard::run`).

The microbench `state_apply_throughput` (`sdk/rust/benches/state.rs`) SHALL exercise this requirement and report nanoseconds per `state.apply(...)` call across change-payload-size and coalesce-key dimensions; CI SHALL fail the build if any dimension regresses >50% from the recorded baseline.

#### Scenario: No serialization on state.apply

- **WHEN** a Shard's `run` calls `state.apply(MyChange::Foo)` 100 times in a tight loop
- **THEN** zero serialization calls (`serde::Serialize::serialize`, `postcard::to_allocvec`, `bincode::serialize`, etc.) SHALL be issued from the runner thread during these 100 calls

#### Scenario: No I/O on state.apply

- **WHEN** a Shard's `run` calls `state.apply(MyChange::Foo)` 100 times
- **THEN** zero `write`, `send`, `fsync`, `fdatasync`, or NIF-call syscalls SHALL be issued from the runner thread during these 100 calls

#### Scenario: state_apply_throughput microbench reports < 200 ns/op for SmallChange + NoCoalesceKey

- **WHEN** the `state_apply_throughput` Criterion bench runs with `change_kind = SmallChange (8B)` and `coalesce_kind = NoCoalesceKey` and the buffer is preallocated to capacity 1024
- **THEN** the reported median time per `state.apply(...)` SHALL be < 200 ns on the reference host

### Requirement: Coalescing is applied at drain time, not on the hot path

The Rust SDK SHALL apply coalescing inside `FlushableBuffer::drain_to_frames`, AFTER the buffer's Mutex is acquired by the flusher. The hot path of `state.apply(...)` SHALL NOT touch any coalescing data structure.

The drain-time coalescing algorithm SHALL: (a) build a `HashMap<CoalesceKey, usize>` mapping each key to the index of the latest record carrying that key; (b) build a parallel `Vec<bool>` mask marking earlier records with the same key as discarded; (c) emit kept records in their original buffer order. Records with `coalesce_key() == None` SHALL be retained verbatim.

#### Scenario: Same-key records collapse to latest

- **WHEN** a Shard records 50 `BarTick { interval: M1, ts: T }` Changes via `state.apply(...)` within one linger window, all returning `Some(CoalesceKey::from_hash(&(M1, T)))` from `coalesce_key()`
- **THEN** the flusher's drain SHALL emit exactly one frame whose Change is the 50th `BarTick`

#### Scenario: No-key records are preserved verbatim

- **WHEN** a Shard records 10 Changes whose `coalesce_key() == None` within one linger window
- **THEN** the flusher's drain SHALL emit 10 frames in the same order they were pushed

#### Scenario: Mixed coalesce-key and no-key records

- **WHEN** a Shard records, in order: A (no key), B (key=K1), C (key=K1), D (no key), E (key=K2)
- **THEN** the flusher's drain SHALL emit frames in order: A, C, D, E (B is collapsed to C; A and D are preserved verbatim; E is retained)

### Requirement: ShardTestHarness in public API surface (Rust SDK)

The Rust SDK SHALL export `ShardTestHarness::<S: Shard>` under `flezha_compute_engine::testing::ShardTestHarness`, gated by `cfg(any(test, feature = "testing"))`. The harness SHALL support:

- `ShardTestHarness::<S>::new()` (requires `S: Default`) — constructs a harness with `S::init_state(None, None)`.
- `ShardTestHarness::<S>::with_initial_state(state: S::State)` — explicit initial state.
- `ShardTestHarness::<S>::with_payload(shard_key, payload)` — replicates the runtime's snapshot+changelog restoration flow.
- `apply_inputs(iter: impl IntoIterator<Item = S::Input>) -> &mut Self` — invokes `S::run` for each input on the harness's owned state, recording emits and Changes.
- `expect_changes(self, expected: &[S::Change]) -> Self` (consuming) — asserts equality, requires `S::Change: PartialEq + Debug`.
- `expect_emits<C>(self, expected: &[C]) -> Self` (consuming) — asserts emits of type `C` equal expected.
- `expect_state(self, expected: &S::State) -> Self` (consuming) — asserts state equality, requires `S::State: PartialEq + Debug`.
- `replay(initial: S::State, log: &[S::Change]) -> S::State` — folds the log via `S::apply_change`.
- `replay_equals(&self) -> Result<(), ReplayMismatch>` — replays self.recorded_changes against a re-constructed initial state and asserts byte-equality (via `serde::Serialize` round-trip) with `self.state`.

#### Scenario: Harness asserts on recorded Changes

- **WHEN** a test runs `ShardTestHarness::<CounterShard>::new().apply_inputs(vec![Input::Add(5), Input::Add(3)]).expect_changes(&[CounterChange { delta: 5 }, CounterChange { delta: 3 }])`
- **THEN** the chain SHALL succeed and produce a harness whose `recorded_changes` equals the expected slice

#### Scenario: Harness replays for determinism check

- **WHEN** a test runs `ShardTestHarness::<CounterShard>::new().apply_inputs(inputs).replay_equals()`
- **THEN** the result SHALL be `Ok(())` because `CounterShard::apply_change` is deterministic

#### Scenario: Harness detects non-deterministic apply_change

- **WHEN** a Shard's `apply_change` reads `SystemTime::now()` and uses it to seed a state field, AND the harness runs `replay_equals()`
- **THEN** the result SHALL be `Err(ReplayMismatch { expected_serialized, actual_serialized })` whose `Display` impl renders a hex-diff identifying the divergent state field

#### Scenario: Harness compiled-out without testing feature

- **WHEN** a downstream crate depends on `flezha_compute_engine` without enabling the `testing` feature flag and does not build under `#[cfg(test)]`
- **THEN** `flezha_compute_engine::testing` SHALL NOT be visible and references to `ShardTestHarness` SHALL fail to compile

### Requirement: 16-byte version header stamped on persisted Changes

The Rust SDK SHALL stamp a 16-byte header on every Change record emitted to the wire path, per the umbrella's spec:

- bytes 0:1 — `sdk_lang: u8` SHALL equal `1` (Rust).
- bytes 1:5 — `sdk_version: u32 BE` SHALL be parsed from `env!("CARGO_PKG_VERSION")` at SDK build time as `(major << 16) | (minor << 8) | patch`.
- bytes 5:9 — `schema_version: u32 BE` SHALL equal `S::SCHEMA_VERSION` for the Shard owning the Change.
- bytes 9:10 — `format_id: u8` SHALL equal `1` (postcard) in v1.
- bytes 10:12 — reserved, MUST be zero.
- bytes 12:16 — `crc32: u32 BE` SHALL be `crc32fast::hash(&change_bytes)`.

The header SHALL be stamped in `HeaderTemplate::stamp(change_bytes: &[u8]) -> [u8; 16]` and verified in `decode_header` on the restoration path.

#### Scenario: Header bytes match the umbrella spec

- **WHEN** a Rust SDK at `version 0.5.3` and Shard `SCHEMA_VERSION = 1` stamps a header for change_bytes `&[0x01, 0x02, 0x03]`
- **THEN** the resulting 16-byte header SHALL equal `[0x01, 0x00, 0x00, 0x05, 0x03, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, <crc32 BE bytes of [0x01, 0x02, 0x03]>]`

#### Scenario: Cross-language load fails loud

- **WHEN** the Rust SDK attempts to decode a header whose `sdk_lang` byte is `2` (Java)
- **THEN** `decode_header` SHALL return `Err(RestoreError::LangMismatch { expected: 1, found: 2 })` and SHALL NOT proceed to deserialization

#### Scenario: schema_version mismatch fails loud

- **WHEN** the Rust SDK at Shard `SCHEMA_VERSION = 2` attempts to decode a record whose header carries `schema_version = 1`
- **THEN** `decode_header` SHALL return `Err(RestoreError::SchemaMismatch { expected: 2, found: 1 })` and SHALL NOT proceed to deserialization (registered migrators are reserved by the umbrella but not implemented in this change)

#### Scenario: crc32 mismatch fails loud

- **WHEN** the Rust SDK decodes a record whose header crc32 does not match a recomputed crc32 over the change_bytes
- **THEN** `decode_header` SHALL return `Err(RestoreError::CrcMismatch { partition, seq })` and SHALL NOT proceed to deserialization

