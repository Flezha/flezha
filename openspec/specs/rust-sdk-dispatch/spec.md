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

On the `mix bench --runtime rust -n 100 -m 10000` workload (100 processors × 10,000 messages = 1,000,000 messages, `do_nothing` handler, UDS transport, default worker-pool size), the Rust SDK SHALL sustain at least **350,000 messages/second** of end-to-end message-processing throughput on commodity developer hardware (modern x86_64 laptop, 8+ cores, non-constrained CPU). Measured on the `Message processing` line of the bench output (total messages ÷ message-processing elapsed time).

#### Scenario: Reference bench meets the floor
- **WHEN** a developer runs `MIX_ENV=prod mix bench --runtime rust -n 100 -m 10000` on a modern 8-core x86_64 laptop with a warm build
- **THEN** the reported `Throughput:` line SHALL be ≥ 350,000 messages/second

#### Scenario: Regression detector fails under pre-change code
- **WHEN** the same bench is run against the pre-change implementation (coordinator + RwLock registry + fixed-worker fan-out)
- **THEN** the reported throughput SHALL be significantly below the floor (observed ~90,000 msg/s), triggering the performance-gate CI check to fail

### Requirement: Per-Message Dispatch-Overhead Budget

The Rust SDK's dispatch hot path — from a completed frame being delivered by the transport layer to the processor's `process(...)` being entered — SHALL consume no more than **3 synchronization operations per message** (where a synchronization operation is a mutex lock, atomic CAS, channel send, or equivalent memory-ordering primitive). Counted on the `ThreadPool` runner under single-threaded contention.

#### Scenario: Microbench confirms the budget
- **WHEN** the `throughput_single_processor` Criterion bench in `sdk/rust/benches/dispatch.rs` runs with contention pinned to one thread
- **THEN** the instrumented sync-op count per message SHALL report ≤ 3 operations on the hot path (registry lookup, mailbox enqueue, inflight CAS)

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

