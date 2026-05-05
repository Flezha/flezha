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

