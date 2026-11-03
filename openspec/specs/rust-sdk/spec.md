# rust-sdk Specification

## Purpose

The Rust SDK. Its authoring surface is the `#[flezha::tasklet]` attribute macro, which completes an
`impl Tasklet` block, the `#[flezha::relay]` short form and the `flezha::relay!` linear-pipeline
macro, with one relay trait carrying one body and one settings tree, canonical relay and runner
names in the public API, and a typed builder checked against the reference topology. Its runner
covers the entry point and lifecycle, typed declaration and connection failures, a process singleton
guard, sticky per-Cell ordering on a worker pool that grows under starvation, a bounded per-message
dispatch overhead, reconnects, restore and compaction handling, and the per-Cell flusher's drain. It
also exports the default tracing directives for every diagnostic target the SDK emits on, with an
opt-in subscriber installer, and a public tasklet test harness.

## Requirements

### Requirement: Sticky Per-Cell Ordering Under ThreadPool Dispatch

Under the SDK's ThreadPool dispatch (the only dispatch mode in v1; see the REMOVED requirement below), the Rust SDK SHALL guarantee that for any single `(relay_instance, tasklet, cell)` triple, two invocations of that Tasklet's `run(...)` method on that Cell never execute concurrently, and invocations observe their inputs in the exact order those inputs were enqueued onto the Cell's mailbox. This invariant MUST hold regardless of worker-pool size, number of concurrent Relay instances, number of Cells per Tasklet, or transport (UDS/QUIC).

#### Scenario: Same cell never runs concurrently under load
- **WHEN** 10,000 inputs targeting the same `(relay_instance, tasklet, cell_key)` triple arrive back-to-back on one or more entry channels while the SDK uses a 16-thread worker pool
- **THEN** at any wall-clock instant, at most one thread SHALL be inside that Cell's `run(...)` method

#### Scenario: Different cells of same tasklet may run concurrently
- **WHEN** inputs arrive simultaneously for two distinct cells of the same Tasklet (for example `("ohlc", "1m")` and `("ohlc", "1h")`) and the worker pool has ≥ 2 threads
- **THEN** the two Cells' `run(...)` methods MAY execute concurrently on different threads

#### Scenario: Per-cell inputs delivered in arrival order
- **WHEN** inputs `"a"`, `"b"`, `"c"` are enqueued onto one Cell's mailbox in that order with an append-to-state Tasklet
- **THEN** the Cell's State SHALL end with `["a", "b", "c"]` in exactly that order

#### Scenario: Ordering holds across multiple entry streams
- **WHEN** the agent opens 4 entry connections for a single Relay instance and the same Cell receives interleaved inputs from all 4
- **THEN** `run(...)` calls for that Cell SHALL still be non-overlapping, though the interleaving order between streams is not constrained by this requirement

### Requirement: Per-Message Dispatch-Overhead Budget

The Rust SDK's dispatch hot path — from a completed frame being delivered by the transport layer to the targeted Cell's `Tasklet::run(...)` being entered — SHALL consume no more than **4 synchronization operations per message** (where a synchronization operation is a mutex lock, atomic CAS, channel send, or equivalent memory-ordering primitive). Counted under single-threaded contention on a Tasklet with `cell_keys() == []` (single-cell mode), exercising the entry-channel → registry → mailbox → drain path.

The 4-op budget covers: (1) registry / topology lookup for the entry channel ID → Relay instance pointer (one DashMap read or equivalent); (2) mailbox enqueue (`crossbeam::queue::ArrayQueue::try_push`, one logical step counted as one op even though its lock-free implementation may use a CAS on the tail plus a release-store on the slot); (3) `pushes_total` counter increment (one Relaxed atomic); (4) inflight CAS (one atomic compare-exchange).

The budget rises from 3 to 4 ops compared with the prior unbounded-mailbox shape (`crossbeam::queue::SegQueue` push without success counter). The added op is the success counter introduced by `relay-bounded-mailbox`; the underlying push transition from SegQueue to ArrayQueue is counted as the same logical step (one mailbox enqueue) and does not on its own raise the budget.

For static-mode tasklets, the additional per-input cost of cell fan-out (HashMap lookup per cell key, one mailbox enqueue + one counter increment + one inflight CAS per target cell) is intrinsic to the workload and counted separately — it is not part of the per-message budget. The fan-out formula updates from `3 + N × 2` to `4 + N × 3` where N is the cell fan-out width.

The 4-op invariant is an analytical bound enforced by code review, consistent with the convention in `sdk/rust/benches/correctness.rs:13-19`. The microbench reports raw msg/s for diagnostic comparison against the recorded `SINGLE_CELL_FLOOR_MSG_PER_SEC` value in `docs/internal/operations/benchmarking.md`; it does not assert the sync-op count programmatically.

#### Scenario: Single-cell microbench confirms the budget
- **WHEN** the `throughput_single_cell` Criterion bench function in `sdk/rust/benches/correctness.rs` runs with contention pinned to one thread, single-cell Tasklet, no fan-out (invoked via `cargo bench --bench correctness -- throughput_single_cell`)
- **THEN** the analytical sync-op count per message SHALL be ≤ 4 tasklets on the hot path (registry lookup, mailbox enqueue, success counter, inflight CAS)
- **AND** the bench SHALL emit a `Throughput:` line in its Criterion output that callers can compare against the recorded `SINGLE_CELL_FLOOR_MSG_PER_SEC` value in `docs/internal/operations/benchmarking.md`

#### Scenario: Static-mode cell fan-out cost is proportional to cell count
- **WHEN** the `throughput_static_fanout` Criterion bench function in `sdk/rust/benches/correctness.rs` is run against a Tasklet with `cell_keys()` of length 4 and `cells_for` returning all four keys per input (invoked via `cargo bench --bench correctness -- throughput_static_fanout`)
- **THEN** the per-input sync-op count SHALL be approximately `4 + 4 × 3 = 16` (one registry lookup + 4 cells × (mailbox enqueue + success counter + inflight CAS)), and SHALL NOT exceed `4 + N × 3` where N is the cell fan-out width
- **AND** the bench SHALL emit a `Throughput:` line whose value, divided by the single-cell floor, SHALL be approximately `1 / (1 + 4 × 3 / 4) = 0.25` ± 30% (the analytical bound is approximate; the bench reports raw msg/s for sanity checking, not as a strict gate)

### Requirement: Live Registry Concurrent Access

The live Relay-instance registry used by the Rust SDK SHALL support concurrent reads from many threads without inter-reader blocking, and writes (Relay instance create / destroy) SHALL NOT block readers for longer than a single tasklet's critical section. Relay classes are declared via `FlezhaRunner::register_factory(relay)` for either kind, callable before and after `.start()`/`.run()` on a runner obtained from `Flezha::create_runner(namespace)`; the class registries SHALL be overwrite-on-insert and SHALL support `unregister_class` for on-demand removal. `AddRelay` / `RemRelay` wire commands add and remove Relay instances by what the wire calls `relay_id` (mapped to the SDK's `relay_instance_id`); lookup by that id returns an `Arc<RelayInstance>` or equivalent handle. Steps instances SHALL record their `factory_id` so live instances of a factory are attributable during a removal check.

The "registry" name carries forward from the pre-change implementation; what's stored in it is unchanged by this requirement. The concurrency guarantees are unchanged.

#### Scenario: 100 reader threads do not stall on a single writer
- **WHEN** 100 threads each perform 10,000 registry lookups while a 101st thread registers and removes Relay instances in a loop
- **THEN** no reader thread observes a latency spike greater than 1 millisecond attributable to the writer

#### Scenario: Registry preserves the declaration contract
- **WHEN** user code calls `.register_factory(R)` on a `FlezhaRunner` and later `AddRelay` triggers a Relay instance creation
- **THEN** the resulting `RelayInstance` SHALL be looked up by id and have its entry channels routable exactly as the spec requires, observable via the `relay-tasklet-cell-model` capability's scenarios

#### Scenario: Post-start removal is serialized with AddRelay
- **WHEN** an `unregister_factory` request and an `AddRelay` command for the same factory race
- **THEN** both SHALL be processed on the coordinator loop in some serial order — either the add lands first and the removal returns `Err(FlezhaError::RelayTypeInUse { .. })`, or the removal lands first and the add fails as an unknown factory — with no interleaving where the removal succeeds while an instance exists

### Requirement: Reconnect Semantics Preserved

Across a control-channel reconnect the Rust SDK SHALL preserve its `relay_registry`, shared I/O streams, and `runner_id` (no clear-and-reassign window where frames are dropped). In addition:

1. **Live-output resolution** — the per-Cell state send path (`CellStateSink`) SHALL resolve the *live* secondary stream on every call rather than caching a stream handle, so a reconnect's freshly-wired stream is used without re-registering Cells.
2. **Reconnect ring** — when no output is installed or a write fails (the reconnect window), **every frame the runner writes on the data plane** SHALL be buffered in one bounded ring (drop-oldest on overflow) and replayed when the fresh stream is wired, so nothing produced mid-reconnect is silently lost. This covers the durable records the flusher ships and the plugin outputs a relay emits: a plugin output arrives already framed with metadata that identifies its channel and its Cell, so it is replayed onto the live secondary as it stands rather than re-framed.
3. **Broken-pipe flood → control reconnect** — sustained write failures (a flood within a short window) SHALL trigger a full control-channel reconnect via the coordinator rather than spinning on a dead stream, whichever writer saw them.
4. **A shed frame is counted** — when the ring is full and drops its oldest entry, the runner SHALL count it on the process-wide drop-oldest counter its heartbeat already reports, and report the number lost when the stream recovers. A frame that is ultimately not delivered is never silent.

#### Scenario: state changes produced during a reconnect are not lost
- **GIVEN** the secondary stream drops and the SDK is reconnecting
- **WHEN** the relay produces state changes before the new stream is wired
- **THEN** the frames are buffered in the reconnect ring and replayed once the new output is installed, preserving seq order.

#### Scenario: a broken-pipe flood drives a control reconnect
- **GIVEN** the state send path sees a flood of write failures within the detection window
- **WHEN** the threshold is crossed
- **THEN** the SDK requests a full control-channel reconnect from the coordinator instead of continuing to write to the dead stream.

#### Scenario: a plugin output whose write fails is held and replayed
- **GIVEN** a relay emits to a plugin output on a stream the controller has closed
- **WHEN** the write fails
- **THEN** the frame is buffered in the same reconnect ring rather than discarded, the failure feeds the same flood ladder, and the next installed output delivers the frame byte-for-byte on the live secondary.

#### Scenario: an overflowing ring reports what it shed
- **GIVEN** the reconnect ring is at its configured capacity while the stream is down
- **WHEN** a further frame is held
- **THEN** the oldest frame is dropped, the runner's `drops_oldest` heartbeat counter advances, and the recovery reports how many frames were lost before it.

### Requirement: Command Dispatch Behavior Preserved Byte-for-Byte for Surviving Commands

Every `CommandRequest` variant that survives the cutover (`AssignRunnerId`, `ConnectSocket`, `AddRelay`, `RemRelay`, `RunnerDisconnect`) SHALL produce a `CommandResponse` byte-for-byte identical to the pre-change implementation for every input that exercised a given path before the change. New error paths MAY be introduced only if pre-change code hit them via panics; otherwise these command responses MUST NOT change.

`CellState` and `MigrationSerialization` are EXCLUDED from this byte-identical guarantee per `relay-mvp-rust/design.md` Decision 10: their on-the-wire JSON state-blob shape changes from `<state>` to `{"<tasklet_name>": <state>}` (or, for static-mode tasklets, `{"<tasklet_name>": {"<cell_key>": <state>, ...}, ...}`). This is acceptable because (a) state migration is umbrella Non-Goal 2 and tracked in `relay-state-migration`, (b) the umbrella's hard-cutover policy means no production scenario crosses old-SDK and new-SDK state blobs, and (c) the new shape is documented in `docs/internal/sdk/protocol_spec.md`.

`RelayMessage` and `RelayMessageResponse` continue to exist on the wire and the agent side; the SDK side responds with a canonical "unsupported" error for every request until `relay-messaging` adds per-Tasklet `handle_message` (see `relay-mvp-rust/design.md` Decision 9). This is byte-equivalent to today's behavior for any relay that did not implement the optional `handle_message` callback.

#### Scenario: AddRelay with init_values produces same response shape
- **WHEN** the agent sends `AddRelay{relay_id:"p1", factory_id:"market", init_values:"{\"symbol\":\"BTC\"}"}` and the identical command is sent to both pre- and post-change SDK builds where `"market"` is registered via the new `register_stream::<MarketRelay>()` (post-change) or `register_factory("market", ...)` (pre-change)
- **THEN** both SDKs SHALL return a `CommandResponse` with the same `reference_id`, `status="success"`, and `errors=[]` (the `raw_data` bytes are not part of this requirement because the post-change `init_values` semantics are per-Tasklet, see the `relay-tasklet-cell-model` capability)

#### Scenario: RunnerDisconnect closes sockets identically
- **WHEN** `RunnerDisconnect{}` is sent under both implementations
- **THEN** both SDKs SHALL close their primary control stream and return identical `CommandResponse` bytes

#### Scenario: RelayMessage returns canonical unsupported error pre-relay-message-handlers
- **WHEN** an agent sends `RelayMessage{relay_id:"p1", payload:<...>}` to a v1 SDK
- **THEN** the SDK SHALL respond with `RelayMessageResponse{ payload: "", error: "relay does not support handle_message" }`

### Requirement: Per-Cell flusher single-thread linger drain

The Rust SDK SHALL spawn at most one `CellFlusher` OS thread per process. The thread SHALL be lazy-initialized via `OnceLock` on the first `FlezhaRunner::register_factory(relay)` call that declares a Tasklet with a non-`NoChange` Change type. Programs that declare only `NoChange` Tasklets SHALL NOT spawn a flusher thread.

The flusher SHALL run an infinite loop that on each iteration: (a) sleeps for `linger_ms` milliseconds (default 5; configurable via env var `FLEZHA_FLUSH_LINGER_MS`); (b) iterates the registered Cells; (c) for each Cell, attempts to acquire the buffer's `Mutex`, and on success calls `FlushableBuffer::drain_to_frames` which: applies coalescing, builds wire frames, releases the lock; (d) for each frame, invokes the Cell's `send_frame` function pointer.

#### Scenario: NoChange-only program does not spawn flusher

- **WHEN** a Rust process calls `.register_factory(R)` for a Relay whose Tasklets all have `Change = NoChange`
- **THEN** no `CellFlusher` thread SHALL be spawned, observable via thread-count assertions in the test (`std::thread::available_parallelism` baseline + assertion that no `flezha-flusher` named thread exists)

#### Scenario: First non-NoChange declaration starts the flusher

- **WHEN** a Rust process calls `.register_factory(R)` for a Relay containing a Tasklet with `Change = MyChange`
- **THEN** exactly one OS thread named `flezha-flusher` SHALL be spawned for the lifetime of the process

### Requirement: Toggle-aware drain semantics for keeps_state = false

The Rust SDK's `CellFlusher` SHALL inspect each registered Cell's `keeps_state` flag at drain time. When `false`, the flusher SHALL acquire the buffer's Mutex, invoke `FlushableBuffer::drain_to_frames` (which still applies coalescing), and discard the resulting frames without invoking `send_frame`. The Cell's `change_seq` counter SHALL still advance through the discarded frames.

When `true`, the flusher SHALL invoke `send_frame` for each frame and propagate any `Err(SendError)` by decrementing the seq counter so the next drain re-sends from the right point (no gaps in the agent's WAL view).

#### Scenario: Toggle-off drain discards without wire I/O

- **WHEN** a Relay with `keeps_state = false` records 10 Changes via `ctx.change(...)` and the flusher's linger elapses
- **THEN** the Cell's flush buffer SHALL be empty after the tick AND the Cell's `send_frame` function SHALL NOT have been invoked AND the Cell's `change_seq` SHALL have advanced by the post-coalesce record count

#### Scenario: Toggle-on drain invokes send_frame per frame

- **WHEN** a Relay with `keeps_state = true` records 10 Changes (no coalescing) via `ctx.change(...)` and the flusher's linger elapses
- **THEN** the Cell's `send_frame` function SHALL have been invoked exactly 10 times with frame bytes built from `[cell_id][seq: u64 BE][header: 16B][change_bytes][crc32: u32 BE]`

#### Scenario: send_frame failure preserves seq monotonicity

- **WHEN** the Cell's `send_frame` returns `Err(SendError::TransportDown)` for the third frame in a 10-frame batch
- **THEN** the flusher SHALL stop the batch at that frame, decrement `change_seq` to undo the failed-and-following advances (the first two frames' seq advances stay), and SHALL retain the un-sent Changes in the buffer for the next drain (records 3-10 are re-pushed via a side channel that preserves their post-coalesce order)

### Requirement: Hot-path cost is bounded to one Vec::push per state.apply

The Rust SDK's `ctx.change(change)` SHALL pay exactly: one `change.apply_to(state)` call (user-supplied; cost varies) plus one `Vec::push(change)` tasklet. The hot path SHALL NOT serialize, fsync, network-send, or acquire any lock beyond the existing per-Cell sticky CAS: the change is pushed onto the invocation's own change list, which only the dispatching thread touches during `Tasklet::run`, and the Cell's flush buffer is taken only when the invocation settles or an emit drains it.

The microbench `state_apply_throughput` (`sdk/rust/benches/state.rs`) SHALL exercise this requirement and report nanoseconds per `ctx.change(...)` call across change-payload-size and coalesce-key dimensions; CI SHALL fail the build if any dimension regresses >50% from the recorded baseline.

#### Scenario: No serialization on state.apply

- **WHEN** a Tasklet's `run` calls `ctx.change(MyChange::Foo)` 100 times in a tight loop
- **THEN** zero serialization calls (`serde::Serialize::serialize`, `postcard::to_allocvec`, `bincode::serialize`, etc.) SHALL be issued from the runner thread during these 100 calls

#### Scenario: No I/O on state.apply

- **WHEN** a Tasklet's `run` calls `ctx.change(MyChange::Foo)` 100 times
- **THEN** zero `write`, `send`, `fsync`, `fdatasync`, or NIF-call syscalls SHALL be issued from the runner thread during these 100 calls

#### Scenario: state_apply_throughput microbench reports < 200 ns/op for SmallChange + NoCoalesceKey

- **WHEN** the `state_apply_throughput` Criterion bench runs with `change_kind = SmallChange (8B)` and `coalesce_kind = NoCoalesceKey` and the buffer is preallocated to capacity 1024
- **THEN** the reported median time per `ctx.change(...)` SHALL be < 200 ns on the reference host

### Requirement: Coalescing is applied at drain time, not on the hot path

The Rust SDK SHALL apply coalescing inside `FlushableBuffer::drain_to_frames`, AFTER the buffer's Mutex is acquired by the flusher. The hot path of `ctx.change(...)` SHALL NOT touch any coalescing data structure.

The drain-time coalescing algorithm SHALL: (a) build a `HashMap<CoalesceKey, usize>` mapping each key to the index of the latest record carrying that key; (b) build a parallel `Vec<bool>` mask marking earlier records with the same key as discarded; (c) emit kept records in their original buffer order. Records with `coalesce_key() == None` SHALL be retained verbatim.

#### Scenario: Same-key records collapse to latest

- **WHEN** a Tasklet records 50 `BarTick { interval: M1, ts: T }` Changes via `ctx.change(...)` within one linger window, all returning `Some(CoalesceKey::from_hash(&(M1, T)))` from `coalesce_key()`
- **THEN** the flusher's drain SHALL emit exactly one frame whose Change is the 50th `BarTick`

#### Scenario: No-key records are preserved verbatim

- **WHEN** a Tasklet records 10 Changes whose `coalesce_key() == None` within one linger window
- **THEN** the flusher's drain SHALL emit 10 frames in the same order they were pushed

#### Scenario: Mixed coalesce-key and no-key records

- **WHEN** a Tasklet records, in order: A (no key), B (key=K1), C (key=K1), D (no key), E (key=K2)
- **THEN** the flusher's drain SHALL emit frames in order: A, C, D, E (B is collapsed to C; A and D are preserved verbatim; E is retained)

### Requirement: TaskletTestHarness in public API surface (Rust SDK)

The Rust SDK SHALL export `TaskletTestHarness::<S: Tasklet>` under `flezha_runner::testing::TaskletTestHarness`, gated by `cfg(any(test, feature = "testing"))`. The harness SHALL support:

- `TaskletTestHarness::<S>::new()` (requires `S: Default`), which constructs a harness with `S::init_state(None)`.
- `TaskletTestHarness::<S>::with_initial_state(state: S::State)`, an explicit initial state.
- `TaskletTestHarness::<S>::with_payload(cell_key, payload)`, which replicates the runtime's snapshot and change-log restoration flow.
- `apply_inputs(iter: impl IntoIterator<Item = S::Input>) -> &mut Self`, which invokes `S::run` for each input on the harness's owned state, recording emits and every `ctx.change` call.
- `expect_changes(self, expected: &[S::Change]) -> Self` (consuming), which asserts equality and requires `S::Change: PartialEq + Debug`.
- `expect_emits(self, expected: &[S::Output]) -> Self` (consuming), which asserts the emitted values equal expected.
- `expect_state(self, expected: &S::State) -> Self` (consuming), which asserts state equality and requires `S::State: PartialEq + Debug`.
- `replay(initial: S::State, log: &[S::Change]) -> S::State`, which encodes each change as the runtime's flusher writes it, decodes it as the restore fold reads it, and folds it through its `apply_to`, so a change that does not survive persistence fails the replay.
- `replay_equals(&self) -> Result<(), ReplayMismatch>`, which replays the recorded changes against the captured initial state and asserts byte-equality (via `serde::Serialize` round-trip) with the harness's state.

#### Scenario: Harness asserts on recorded Changes

- **WHEN** a test calls `apply_inputs(vec![Input::Add(5), Input::Add(3)])` on a `TaskletTestHarness::<Counter>::new()` and then `expect_changes(&[Added { delta: 5 }, Added { delta: 3 }])`
- **THEN** the assertion SHALL succeed, because the recorded changes equal the expected slice

#### Scenario: Harness replays for determinism check

- **WHEN** a test calls `apply_inputs(inputs)` on a `TaskletTestHarness::<Counter>::new()` and then `replay_equals()`
- **THEN** the result SHALL be `Ok(())` because `Added::apply_to` is deterministic

#### Scenario: Harness detects a non-deterministic apply_to

- **WHEN** a change's `apply_to` reads ambient state, such as `SystemTime::now()` or a process-wide counter, into a state field, AND the harness runs `replay_equals()`
- **THEN** the result SHALL be `Err(ReplayMismatch { expected_serialized, actual_serialized })` whose `Display` impl renders a hex-diff identifying the divergent state field

#### Scenario: A change that does not survive persistence fails the replay

- **WHEN** a change type skips a field when serialized, and the harness runs `replay_equals()` after recording one
- **THEN** the result SHALL be `Err(ReplayMismatch { .. })`, because the replayed state folds the change as the runtime would restore it

#### Scenario: Harness compiled-out without testing feature

- **WHEN** a downstream crate depends on `flezha_runner` without enabling the `testing` feature flag and does not build under `#[cfg(test)]`
- **THEN** `flezha_runner::testing` SHALL NOT be visible and references to `TaskletTestHarness` SHALL fail to compile

### Requirement: 16-byte version header stamped on persisted Changes

The Rust SDK SHALL stamp a 16-byte header on every Change record emitted to the wire path, per the umbrella's spec:

- bytes 0:1 — `sdk_lang: u8` SHALL equal `1` (Rust).
- bytes 1:5 — `sdk_version: u32 BE` SHALL be the **Rust SDK crate's** own version, read from `env!("CARGO_PKG_VERSION")` expanded inside that crate, packed as `(major << 16) | (minor << 8) | patch`. It SHALL NOT be the version of any crate the SDK depends on: a macro that reads a package version expands where it is written, so the reading SHALL happen in the SDK crate and the packing SHALL take the string as an argument.
- bytes 5:9 — `schema_version: u32 BE` SHALL equal `S::SCHEMA_VERSION` for the Tasklet owning the Change.
- bytes 9:10 — `format_id: u8` SHALL equal `1` (postcard) in v1.
- bytes 10:12 — reserved, MUST be zero.
- bytes 12:16 — `crc32: u32 BE` SHALL be `crc32fast::hash(&change_bytes)`. This field is stamped and carried; it is NOT the value the restore path compares. The WAL frame carries its own trailing crc32 over the header and the body together, and that is the one a fold checks.

The header SHALL be stamped in `HeaderTemplate::stamp(change_bytes: &[u8]) -> [u8; 16]`. Decoding it SHALL be split across the functions that own each refusal, and this requirement SHALL name the function that raises each one rather than attributing them all to the parser: `decode_header` parses the sixteen bytes and refuses only a header it cannot parse; the language byte is refused by `refuse_foreign_language`; the schema decision is taken by `decide_schema` against a readable floor, reached through `resolve_record` and `resolve_state`; and the crc a fold refuses on is the WAL frame's trailing one, read before the header is decoded. A scenario that names a refusal no function raises, or a variant the crate does not declare, is unfalsifiable and SHALL NOT stand as a requirement.

#### Scenario: Header bytes match the umbrella spec

- **WHEN** a Rust SDK at `version 0.5.3` and Tasklet `SCHEMA_VERSION = 1` stamps a header for change_bytes `&[0x01, 0x02, 0x03]`
- **THEN** the resulting 16-byte header SHALL equal `[0x01, 0x00, 0x00, 0x05, 0x03, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, <crc32 BE bytes of [0x01, 0x02, 0x03]>]`

#### Scenario: The stamped version follows the SDK crate, not the kernel

- **GIVEN** the SDK crate's manifest declaring a version the kernel crate's manifest does not
- **WHEN** a Tasklet stamps a header
- **THEN** bytes 1:5 SHALL carry the SDK crate's version

#### Scenario: An unparseable header is refused by the parser

- **WHEN** `decode_header` is given fewer than sixteen bytes
- **THEN** it SHALL return `Err(RestoreError::HeaderInvalid)`, which is the only refusal that function raises

#### Scenario: Cross-language load fails loud

- **WHEN** the Rust SDK attempts to restore a record whose `sdk_lang` byte is `2` (Java)
- **THEN** the language check SHALL return `Err(RestoreError::LangMismatch { expected: 1, found: 2 })` and SHALL NOT proceed to deserialization

#### Scenario: A schema older than the readable floor fails loud

- **WHEN** the Rust SDK restores a record whose header carries a `schema_version` below the Tasklet's readable floor
- **THEN** the schema decision SHALL return `Err(RestoreError::SchemaTooOld { .. })` and SHALL NOT proceed to deserialization

#### Scenario: A schema newer than this build fails loud

- **WHEN** the Rust SDK at Tasklet `SCHEMA_VERSION = 1` restores a record whose header carries `schema_version = 2`
- **THEN** the schema decision SHALL return `Err(RestoreError::SchemaFromTheFuture { .. })` and SHALL NOT proceed to deserialization

#### Scenario: crc32 mismatch fails loud

- **WHEN** the Rust SDK folds a change-log record whose trailing crc32 does not match a crc32 recomputed over that record's header and change bytes
- **THEN** the fold SHALL return `Err(RestoreError::CrcMismatch { cell, seq })` before the header is decoded, and SHALL NOT proceed to deserialization

#### Scenario: Every error variant a scenario names is declared by the crate

- **WHEN** the `RestoreError` variants this requirement names are looked up in `flz_core`
- **THEN** each SHALL exist, so no scenario here can be satisfied by no code at all

### Requirement: Public Rust SDK API uses canonical Relay/Runner vocabulary

The Rust SDK SHALL expose its top-level dispatch types under the names `RunnerNode` and provide a method `get_relays()` returning the registered Relays on the current Runner. The module path SHALL be `sdk/rust/src/runner/`. Generated protobuf types SHALL carry the messages `AssignRunnerId`, `RunnerDisconnect`, `AddRelay`, `RemRelay`, `CellState`, `RelayMessage`, `RelayMessageResponse`, plus existing `RestoreInitState`, `CompactCell`, `CellMailboxStatus`.

#### Scenario: Public types compile under their names
- **WHEN** a downstream Rust crate depends on `flezha-sdk` and writes `use flezha_sdk::runner::RunnerNode;`
- **THEN** the import MUST resolve and the compiler MUST accept it

#### Scenario: get_relays returns the local Relay registry
- **WHEN** a Rust SDK Runner has Relays `r1` and `r2` registered locally
- **THEN** `runner_node.get_relays()` MUST return a collection containing exactly `r1` and `r2`

### Requirement: Generated payload-id constants use canonical names

The Rust SDK's payload-id constants (those that map to the binary framing's `payload_id(32)` byte slot) SHALL be named consistent with the canonical vocabulary: `CELL_STATE_CHANGE`, `CELL_STATE_SNAPSHOT`, `RESTORE_INIT_STATE`, `COMPACT_CELL`, `CELL_MAILBOX_STATUS`, `CONNECT_CONTROL_CHANNEL`, `CONTROL_COMMAND`. The previous `PROCESSOR_*` family SHALL be removed.

#### Scenario: Payload-id constants compile and resolve
- **WHEN** the Rust SDK is built
- **THEN** the constants `CELL_STATE_CHANGE` and `CELL_STATE_SNAPSHOT` MUST be defined and exported; references to `PROCESSOR_STATE_CHANGE` MUST not exist

### Requirement: Test fixtures' Tasklets exercise the output channel path

The test fixtures under `test/apps/rust_runner/src/main.rs` SHALL emit at least one output frame per input received, via `ctx.emit(value)`, on Relays exercised by the controller's plugin tests with an `:on_output` hook. Specifically:

- `StoreInputRelay::StoreInputTasklet::run` SHALL emit the input string on an output Channel (named `"output"`) after applying the `PushText` change.
- `CompareFilterRelay::CompareFilterTasklet::run` SHALL emit the input string on an output Channel when `state.comparable == input` (matching the existing test's expectation of `"hello world"` round-tripping through the filter).

This requirement covers fixture behavior only; it does not change the SDK's `Tasklet` trait, the dispatch contract, or any wire format. A future production Relay may emit no outputs and remain conformant to the SDK's other requirements.

#### Scenario: StoreInputRelay round-trips input via :on_output

- **WHEN** the controller's `BeamMsgForward` plugin is enabled with `hooks: [{:on_output, []}]` against a `StoreInputRelay` instance and the test sends `"state_1"` to the relay
- **THEN** the test SHALL receive `{:message_forward, "state_1"}` within 2 seconds (UDS) or 5 seconds (QUIC)

#### Scenario: CompareFilterRelay round-trips matching input via :on_output

- **WHEN** a `CompareFilterRelay` instance is initialized with `comparable = "hello world"` and the test sends `"hello world"` to the relay
- **THEN** the test SHALL receive `{:message_forward, "hello world"}` within 2 seconds

### Requirement: Node-local persisted runner id announced as `previous_runner_id`

On a fresh process start (not a reconnect) the SDK SHALL load its node-local persisted runner id (atomically written via tmp+rename at `FLEZHA_RUNNER_ID_PATH`) and announce it as `previous_runner_id` in the connect handshake, so the controller evicts the stale binding from the prior process before binding the new one. Reconnects SHALL NOT re-announce it (identity is preserved in-place).

#### Scenario: a restarted process reclaims its bindings
- **GIVEN** an SDK process held `runner_id = default_X` and persisted it locally
- **WHEN** the process is killed and a fresh process starts, reads `default_X`, and announces it as `previous_runner_id`
- **THEN** the controller evicts the stale `default_X` binding before the new bind, so no relay is left bound to the dead process.

### Requirement: Rust SDK Entry Point

The Rust SDK SHALL provide `Flezha::create_runner(namespace: impl Into<String>) -> Result<FlezhaRunner, FlezhaError>` as the single entry point, returning a concrete `FlezhaRunner` directly — there SHALL be no separate `RunnerBuilder` type. Creating a runner SHALL NOT connect, claim the process singleton slot, or spawn any thread. Calling `.start(&self)` MUST connect to the controller's control socket, send the handshake init message per `docs/internal/sdk/protocol_spec.md` §5.2 carrying the runner's currently-declared factory set (possibly empty), wait for `AssignRunnerId`, and return `Ok(&Self)` for chaining. The started runner SHALL expose `namespace()` (replacing `id()` — the controller-assigned per-session token lives on the runner thread and rotates across reconnects, so it is never the handle's identity) and `relays()`. `.run(&self)` SHALL be sugar for `.start()` followed by `await_termination()`, never an independent implementation. This requirement mirrors `java-sdk`'s "Java SDK Entry Point" requirement — the two SDKs' entry-point behavior is intentionally identical.

#### Scenario: Successful connection returns a live handle
- **WHEN** an SDK user calls `Flezha::create_runner("my_app")?.register_factory(MarketRelay)?.start()` (binding the runner with `let`) with the controller running
- **THEN** the call blocks until the connect handshake completes, and the runner's `namespace()` returns `"my_app"`

#### Scenario: Controller unavailable triggers exponential backoff retries
- **WHEN** an SDK user calls `.start()` on a runner and the controller's control socket is not yet listening
- **THEN** the SDK SHALL retry the connect with exponential backoff starting at 200 ms and capped at 30 s, and succeed once the controller becomes available

#### Scenario: start() with zero defined relay types is legal
- **WHEN** `.start()` is called on a `FlezhaRunner` with no `.register_factory(...)` calls, whatever short-form or `relay!` types the binary contains
- **THEN** the runner SHALL connect successfully with an empty factory list in the handshake init message, and relay types MAY be defined later on the live runner

#### Scenario: RunnerBuilder does not compile
- **WHEN** user code references `flezha::RunnerBuilder`, `Flezha::new_runner`, `RunnerStartError`, or `RunnerError`
- **THEN** compilation SHALL fail — the builder type, the old entry-point name, and the superseded error types are deleted with no shim

### Requirement: Runner declaration and connection failures surface as typed Results

Every fallible call on the entry-point chain SHALL return `Result<_, FlezhaError>` — one unified public error type (`thiserror`-derived) so `Flezha::create_runner(ns)?.register_factory(A)?.run()?` compiles under a single `?`. `FlezhaError` SHALL carry at least: `InvalidNamespace`, `AlreadyRunning { namespace }`, `ConnectFailed(String)`, `InvalidTopology(#[from] TopologyBuildError)`, `DuplicateFactoryId { factory_id }`, `NotDefined { factory_id }`, `RelayTypeInUse { factory_id, live_instances }`, `NotStarted`, `RunnerUnavailable(String)`. An invalid topology (duplicate channel names, unwired channels, missing `cells_for` overrides, etc.) SHALL be reported as a typed `Err` at the call site, not a panic; a consumer whose `Input` does not match the channel payload SHALL instead be refused by the compiler at the `.into::<S>()` call site, so `TopologyIssue` SHALL carry no runtime type-mismatch variant; connect-handshake failures (socket error, or the confirmation channel closing unexpectedly) SHALL be `Err(FlezhaError::ConnectFailed(..))`, not a panic. The per-issue topology enum formerly named `BuildTopologyError` SHALL be renamed `TopologyIssue`, resolving its collision with the per-kind `TopologyBuildError`; Java throws the identically-named failure classes per naming rule 6.

`relays()` SHALL return `Result<HashMap<String, Arc<RelayInstance>>, FlezhaError>` — `Err(FlezhaError::NotStarted)` on a never-started runner, never a panic; the internal snapshot path SHALL contain no `expect`/`unwrap` reachable from it.

#### Scenario: Invalid topology is reported as a Result, not a panic
- **WHEN** a Relay declares the same channel name twice
- **THEN** `.register_factory(ThatRelay)` SHALL return `Err(FlezhaError::InvalidTopology(..))` carrying the `TopologyIssue::DuplicateChannel` diagnosis naming the channel — the call SHALL NOT panic

#### Scenario: A payload type mismatch is a compile error
- **WHEN** user code wires a consumer whose `Tasklet::Input` is not the channel's payload type
- **THEN** the `.into::<S>()` call SHALL fail to compile at the call site, and no `TopologyIssue` variant SHALL exist to report it at registration time

#### Scenario: A connection failure at start() is a Result, not a panic
- **WHEN** the confirmation channel from the runner thread closes without delivering `RunnerIdConfirmation` (e.g. the runner thread panicked internally)
- **THEN** `.start()` SHALL return `Err(FlezhaError::ConnectFailed(..))` — the caller's thread SHALL NOT panic

#### Scenario: The fluent chain compiles under one question mark
- **WHEN** user code writes `Flezha::create_runner("ns")?.register_factory(A)?.register_factory(B)?.run()?` in a function returning `Result<(), FlezhaError>`
- **THEN** it SHALL compile without intermediate `map_err` calls

#### Scenario: relays() on a never-started runner
- **WHEN** code calls `.relays()` on a runner created but never started
- **THEN** the call SHALL return `Err(FlezhaError::NotStarted)` and SHALL NOT panic

### Requirement: Runner process singleton guard

The Rust SDK SHALL allow at most one runner to be **started** per process. `Flezha::create_runner(...)` SHALL NOT claim the singleton slot — any number of unstarted runners may exist. `.start()`/`.run()` SHALL claim a process-global guard (a mutex-held slot storing the running namespace — releasable on close/Drop, which a `OnceLock` is not) by compare-and-set before connecting; if a prior runner's `.start()`/`.run()` has already succeeded in this process and not yet shut down, the call SHALL return `Err(FlezhaError::AlreadyRunning { namespace })` naming the already-running namespace, and SHALL NOT connect, mutate any shared registry, or spawn any background thread. Calling `.start()` twice on the same runner SHALL also be an error.

#### Scenario: Second start() in the same process is rejected
- **GIVEN** a process where a runner for `"ns1"` created via `Flezha::create_runner("ns1")?` has been started
- **WHEN** code in the same process starts a second runner for `"ns2"`
- **THEN** the call SHALL return `Err(FlezhaError::AlreadyRunning { namespace: "ns1".into() })`
- **AND** no connection SHALL be attempted for `"ns2"`, and the process's `STABLE_ID`, `cell_state_sink`, and `credit::global()` state SHALL remain exclusively `"ns1"`'s

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

### Requirement: On-demand relay type declaration and removal (Rust)

`FlezhaRunner` SHALL provide `register_factory(relay)` and its inverse `unregister_factory(relay)`, each generic as `fn register_factory<O: Relay>(&self, relay: O) -> Result<&Self, FlezhaError>`, so one method takes a relay of either run, passed as a value, and reads the run from the class's `settings()`; both are legal before and after `.start()`. There SHALL be no `define_relay`, `remove_stream` or `remove_steps`. The declared factory set SHALL be per-runner, starting empty each generation and holding exactly what `register_factory` and `unregister_factory` put in it; nothing SHALL be merged into it at `.start()`. A post-start `register_factory` or `unregister_factory` SHALL update the process-global class registries and ship the runner's full current factory set to the controller via the `UpdateFactories` control message (see the `runner-factory-sync` capability); the reconnect init frame SHALL serialize the same per-runner set, so reconnect naturally resyncs post-start mutations. `unregister_factory` SHALL return `Err(FlezhaError::NotDefined { .. })` for an undefined factory and `Err(FlezhaError::RelayTypeInUse { factory_id, live_instances })` when live instances of the factory exist, leaving the type defined. Duplicate definition of the same factory id on one runner, of either run, SHALL return `Err(FlezhaError::DuplicateFactoryId { .. })`, whose `code()` SHALL be `topology.duplicate_factory_id`.

#### Scenario: Pre-start unregister shapes the init frame
- **WHEN** user code registers A and B on an unstarted runner, unregisters B, then starts
- **THEN** the handshake init frame's factory list SHALL contain only A

#### Scenario: Post-start register ships UpdateFactories
- **WHEN** user code calls `.register_factory(C)` on a started runner already declaring A
- **THEN** the SDK SHALL send an `UpdateFactories` control message carrying the full set {A, C}, and a subsequent `AddRelay{factory_id: C}` SHALL succeed

#### Scenario: Post-start remove with live instances refuses
- **GIVEN** a started runner with one live instance of factory `"market"`
- **WHEN** user code calls `.unregister_factory(MarketRelay)`
- **THEN** the call SHALL return `Err(FlezhaError::RelayTypeInUse { factory_id: "market", live_instances: 1 })` and the type SHALL remain defined

#### Scenario: Removing an undefined factory
- **WHEN** user code calls `.unregister_factory(X)` and X's factory id is not in the runner's declared set
- **THEN** the call SHALL return `Err(FlezhaError::NotDefined { .. })`

#### Scenario: One method registers either run
- **WHEN** user code calls `.register_factory(EchoRelay)` for a flow `Relay` and `.register_factory(PlaceOrder)` for a steps `Relay` on one runner
- **THEN** both factory ids SHALL be in the declared set and the class registry, the second announced with run `steps`

#### Scenario: A type that is not a relay does not compile
- **WHEN** a type that does not implement `Relay` is passed to `.register_factory(...)`
- **THEN** compilation SHALL fail, because the bound `O: Relay` is not met

#### Scenario: A duplicate across runs is refused, never skipped
- **WHEN** user code calls `.register_factory(A)` for a flow relay and then `.register_factory(B)` for a steps relay where both declare factory id `plain`
- **THEN** the second call SHALL return `Err(FlezhaError::DuplicateFactoryId { .. })`, the declared set SHALL still hold only A, and the process-global registry entry for `plain` SHALL still be A's

### Requirement: A restore SHALL NOT silently discard un-recoverable queued input

A restore SHALL discard a Cell's queued input only where the Controller will re-forward it. Where the Controller will not re-forward it, the queued input SHALL NOT be silently destroyed.

`restore_into_slot` today clears the mailbox on every restore, justified by the claim that every queued input "is durable in the input log the controller is about to re-forward". That claim holds only for classes whose inputs are journaled before forwarding — `Ingest.journal_input/2` journals for the effectively-once tier and for durable steps instances, and passes every other relay through untouched. A plain stateful flow relay, which is the default configuration, has no such log.

#### Scenario: Journaled input may be discarded
- **GIVEN** a Cell whose class journals its inputs before forwarding
- **WHEN** a restore rebuilds that Cell
- **THEN** the queued input MAY be discarded, because the Controller re-forwards it from the durable log

#### Scenario: Un-journaled input is not silently destroyed
- **GIVEN** a Cell whose class does not journal its inputs
- **WHEN** a restore rebuilds that Cell
- **THEN** its queued input SHALL NOT be silently discarded

#### Scenario: The replay buffer and open interval clears are unchanged
- **WHEN** a restore rebuilds a Cell
- **THEN** the replay buffer, replay flags, and any open checkpoint interval SHALL continue to be cleared as they are today, on every class

### Requirement: Input discarded by a restore SHALL be counted

`Mailbox::clear` is a bare drain loop with no counter, telemetry, or log line, so a discard leaves no trace and the loss can only be inferred by reading the code. Any input discarded on the restore path SHALL be observable.

#### Scenario: A discard emits a count
- **WHEN** a restore discards queued input for a Cell
- **THEN** the number of discarded items SHALL be recorded and exposed, naming the Cell

### Requirement: The two SDKs SHALL agree on restore-time mailbox handling

The Rust SDK clears the mailbox on restore; the Java SDK does not, clearing only its staged changes. Neither behaviour is documented as the intended one. Both SDKs SHALL implement the same documented behaviour.

#### Scenario: Equivalent restores behave equivalently
- **GIVEN** equivalent Cells of the same class on a Rust runner and a Java runner, each with queued input
- **WHEN** each is restored
- **THEN** the fate of the queued input SHALL be the same on both

### Requirement: The Rust SDK SHALL emit a cell_state_snapshot on CompactCell

`crates/flz_core/src/command/compact.rs` currently ignores its runner argument and answers a hardcoded `CompactCellResponse{snapshot_seq: 0, snapshot_bytes_size: 0}` without shipping any frame, and pushes nothing onto `errors` — so the failure presents to the Controller as a successful handshake. The handler SHALL locate the addressed Cell, capture its state, ship a `cell_state_snapshot` data frame, and answer with the seq it actually captured.

The snapshot body SHALL be encoded with the same codec the restore path decodes — postcard, per `cell/restore.rs`. The JSON serializer used for graceful shutdown and live migration SHALL NOT be used for the snapshot body.

The frame SHALL carry a payload-id constant defined alongside the output's other payload-ids, and SHALL be shipped through the existing reliable output rather than a separate transport path.

#### Scenario: A live Cell produces a snapshot at its current seq

- **GIVEN** a live Cell with persisted Changes, a current `change_seq`, and no change its state holds left unshipped
- **WHEN** the SDK receives `CompactCell` addressing that Cell
- **THEN** the SDK SHALL ship one `cell_state_snapshot` frame carrying the Cell's serialized state stamped with a seq the Cell has reached
- **AND** SHALL answer `CompactCellResponse` reporting that same seq and the body's byte size

#### Scenario: The snapshot body round-trips through the restore path

- **GIVEN** a snapshot produced by the SDK for a Cell
- **WHEN** that snapshot is later shipped back to the SDK as `RestoreInitState.snapshot_bytes`
- **THEN** the restore path SHALL decode it without error and SHALL reconstruct the state the Cell held when the snapshot was taken

#### Scenario: The seq and the state are captured together

- **WHEN** the SDK captures state and seq for a snapshot
- **THEN** the pair SHALL be captured such that the Controller may truncate the WAL up to that seq without discarding a record the snapshot does not already contain

#### Scenario: An unaddressable Cell reports an error

- **WHEN** the SDK receives `CompactCell` for a `cell_id` matching no live Cell
- **THEN** the handler SHALL report an error rather than answering as though compaction succeeded

#### Scenario: A Cell inside an open checkpoint interval closes it first

- **GIVEN** a live Cell inside an open checkpoint interval
- **WHEN** the SDK receives `CompactCell` addressing that Cell
- **THEN** the handler SHALL close the interval early and ship its record, then SHALL ship the snapshot at a seq at or above that record's

#### Scenario: A Cell holding a retried attempt's changes refuses the snapshot

- **GIVEN** a live Cell whose state holds the changes of an attempt awaiting retry, which no shipped record carries
- **WHEN** the SDK receives `CompactCell` addressing that Cell
- **THEN** the handler SHALL ship no snapshot and SHALL report `state.cell_busy`, a retry-later refusal

### Requirement: The Rust SDK SHALL take the task-journal from the bind

`start_steps_resumable` SHALL accept the instance's task-journal alongside the
restore count and SHALL pass it to `StepsInstance::new`, so the resume state is
present before the driver is spawned. The `RestoreInitState` handler SHALL NOT
carry the journal — that frame is per-Cell and is not shipped for a class with
no persisted Cell.

#### Scenario: A resuming bind with no Cell restores still resumes at its iteration

- **GIVEN** a durable steps class whose tasklets hold no per-Cell state, so no `RestoreInitState` is shipped
- **WHEN** the controller binds it with `AddRelay.task_journal`
- **THEN** the driver SHALL enter the journaled iteration and skip the tasklets committed in it

#### Scenario: A fresh bind carries no journal

- **WHEN** `AddRelay` arrives with no `task_journal`
- **THEN** `task_journal_resume()` SHALL be `None` and the chain SHALL run from the top

### Requirement: The worker pool grows under starvation and shrinks when idle

The Rust SDK SHALL spawn its base poll-worker pool as today
(`FLEZHA_WORKER_POOL_SIZE`, default = CPU count) through the core pool
governor (`flz_core::poll_pool`), implementing only its spawner, and the
governor SHALL park one supervisor thread in the core's starvation wait. On each starvation wake
the supervisor SHALL spawn one additional poll worker, up to a ceiling
(`FLEZHA_WORKER_POOL_MAX`, default 10× CPU count). Workers above the base
count SHALL exit after the governor's idle time, 30 s without work; base workers SHALL NOT exit.
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
dispatcher's own guards re-hands the activation instead of retiring its cell.

#### Scenario: Blocking user code no longer halts the runner

- **WHEN** activations blocking longer than the starvation detection interval
  occupy every base worker while work for other cells is queued
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

#### Scenario: An escaping panic re-hands rather than retiring the cell

- **WHEN** a panic escapes the dispatcher's own guards while a worker holds an
  activation for cell E
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
  inline, and a tasklet in that round emits under `DropNothing` into another
  full consumer
- **THEN** the nested push also serves inline, because the thread is still
  inside an activation

#### Scenario: The wire reader never serves

- **WHEN** the stream reader's push into an entry mailbox finds it full under
  `DropNothing`
- **THEN** the reader parks on the consumer's parker until the pool pops a
  slot, and no tasklet runs on the reader thread

### Requirement: A steps output whose write fails SHALL be held for the reconnect ring

A plugin-bound steps output whose write to the secondary stream fails SHALL be
held in the same reconnect ring a failed plugin-emit write uses, and SHALL be
replayed when a stream is next bound. It SHALL NOT be reported only to stderr and
discarded, and a write the host cannot hold SHALL be counted where a user
reads counts, never dropped in silence.

#### Scenario: The secondary stream is closed when a steps relay emits

- **WHEN** `write_steps_output` finds no stream, or its write returns an error
- **THEN** the framed output SHALL enter the reconnect ring rather than being
  printed and dropped, and SHALL be delivered once a stream binds

#### Scenario: A held output keeps the identity it was framed with

- **WHEN** a held steps output is replayed after a rebind
- **THEN** it SHALL carry the metadata it was framed with, so a keyed or
  identity-stamped output is not replayed with an empty metadata map

### Requirement: The typed builder submits its topology to the reference check

The Rust SDK's builder state SHALL write the same declaration record an erased host
writes — closures held aside per chain, never in the record — SHALL take its verdict from
`flz_core::host::register::check`, and SHALL materialise its typed `RelayTopology`
from the derived declaration, attaching closures by channel and chain. `TopologyIssue`
SHALL be the core's type, re-exported; the builder SHALL raise no variant of its own. The
compile-time edge refusals and their `tests/ui` fixtures SHALL stay as they are.

#### Scenario: The typed and erased paths cannot drift

- **WHEN** a refusal is added to the core's vocabulary with its fixture row
- **THEN** the typed builder refuses it with no change to `sdk/rust`, and its refusal
  message lists the same sentence an erased host receives

#### Scenario: A well-formed typed topology derives to the reference bytes

- **WHEN** the reference fixtures `hybrid_channel` and `commit_block_chain` are built through the typed builder
- **THEN** the derived declaration equals the seven-column lines of `test/fixtures/reference_topologies.txt`

### Requirement: Dispatch Throughput Reported on Reference Workload

The regression bench (`100 relays × 1 tasklet × 1 cell × 10K msg`, no-op Tasklet, UDS transport, default worker-pool size; see `relay-architecture/design.md` Decision 16), invokable via `mix bench --runtime rust --shape regression -n 100 -m 10000`, SHALL report the Rust SDK's message-processing throughput and SHALL NOT be held to an absolute throughput number: no floor is recorded for it.

The bench SHALL report the **end-to-end** window (first input dispatched to last response received) as its headline throughput and the **receive** window (send loop returning to last response received) beside it, and SHALL name the window on every rate it reports. The two are not interchangeable, since they differ by the send window, and a figure quoted without its window is not attributable. Both SHALL be written to the run's structured report, as `metrics.throughput_msg_per_sec` and `metrics.receive_window_msg_per_sec`.

#### Scenario: Reference bench reports both windows
- **WHEN** a developer runs `MIX_ENV=prod mix bench --runtime rust --shape regression -n 100 -m 10000`
- **THEN** the run's structured report SHALL carry the end-to-end rate as `metrics.throughput_msg_per_sec` and the receive-window rate as `metrics.receive_window_msg_per_sec`
- **AND** the run's printed output SHALL lead with the end-to-end throughput, each rate naming its window

#### Scenario: Cell-stress bench scales with worker pool size
- **WHEN** the cell-stress bench (`8 relays × 4 tasklets × 8 cells × 20K msg/relay`, fan-out per input to all 8 cells of the entry tasklet; invoked via `mix bench --shape cell --pool-sizes 1,8`) is run on an 8-core host
- **THEN** the throughput at pool size 8 SHALL be at least 4× the throughput at pool size 1, demonstrating that cell-mode dispatch is not the bottleneck

### Requirement: `FlezhaError` SHALL expose the error model's fields

`FlezhaError` SHALL be the Rust SDK's one error type. Every variant SHALL answer `code()`, `class()`, `details()` and `hint()`, and its `Display` SHALL be the catalogue message. The payload types it wraps (`TopologyIssue`, `TaskletError`, and the administration and spawn failures) SHALL answer the same accessors, each code taken from an exhaustive match. The variant identifiers keep their Rust spelling.

#### Scenario: A duplicate factory id reads as its code

- **WHEN** `register_factory` is called twice with the same factory id
- **THEN** the second call SHALL return an `Err` whose `code()` is `topology.duplicate_factory_id` and whose `class()` is invalid

### Requirement: `#[flezha::tasklet]` attribute macro

The Rust SDK SHALL provide a `#[flezha::tasklet]` attribute macro applied to an `impl Tasklet for T` block, where `T` is the author's own type. It SHALL generate no type: it SHALL only add to that block the items the author did not write, namely the `NAME` const from the required `name` key, the setting items for the keys written, the `COMPENSATE_OVERRIDDEN`, `CELLS_FOR_OVERRIDDEN`, `MAX_CELLS_OVERRIDDEN` and `MAILBOX_CAPACITY_OVERRIDDEN` flags from the matching `fn` the block writes (`compensate`, `cells_for`, `max_cells`, `mailbox_capacity`) or the matching key (`max_cells`, `capacity`), the `CLOCK` const, `init_state` returning `Default::default()` when none is written, and `State = ()`, `Change = NoChange` and `Output = ()` when not written. An item the block writes that the attribute would also add, and an unknown key, SHALL fail to compile naming it. The attribute SHALL NOT accept `kind`, `output`, `input` or `on_full`, which describe a relay and its entry channel, and SHALL fail to compile naming the key and directing the author to `#[flezha::relay]` when one is written. The attribute on a function SHALL fail to compile, directing the author to the `impl` block.

#### Scenario: A stateless tasklet writes only its types and run

- **WHEN** `#[flezha::tasklet(name = "echo")]` annotates `impl Tasklet for Echo` writing `type Input = String`, `type Output = String` and `run`
- **THEN** the block compiles with `State = ()`, `Change = NoChange`, `NAME = "echo"`, a default `init_state` and the wall clock, and no type other than `Echo` exists for it

#### Scenario: A stateful tasklet names its state and change

- **WHEN** a `#[flezha::tasklet(name = "quiet_symbol")]` block writes `type State = LastTrade` and `type Change = Traded`
- **THEN** `ctx.state()` reads a `LastTrade` and `ctx.change` takes a `Traded`, and `init_state` returns `LastTrade::default()`

#### Scenario: The author's type is referenceable in channels

- **WHEN** a tasklet is declared on `struct StaticEntry` with `#[flezha::tasklet]`
- **THEN** `StaticEntry` itself can be named in a `channels()` body of a relay of either kind via `.from`/`.into`

#### Scenario: A written hook sets its flag

- **WHEN** a `#[flezha::tasklet]` block writes `fn compensate`
- **THEN** `COMPENSATE_OVERRIDDEN` SHALL be `true` for the tasklet, and the runtime SHALL run it on a rollback

#### Scenario: A relay key on a tasklet is refused

- **WHEN** `#[flezha::tasklet(name = "do_nothing", output = "echo")]`, or the same with `kind`, `input` or `on_full`, annotates an `impl Tasklet` block
- **THEN** the macro fails to compile naming the key and the short-form attribute that carries it

#### Scenario: The attribute on a function is refused

- **WHEN** `#[flezha::tasklet(name = "echo")]` annotates `fn echo(input: String) -> String`
- **THEN** the macro fails to compile, directing the author to `impl Tasklet` or to the short-form attributes

### Requirement: Attribute opt-ins for cell and mailbox behavior

The `#[flezha::tasklet]` macro SHALL accept keys that set the corresponding `Tasklet` items without the author writing them: `cell_keys`, `mailbox_capacity`, `max_cells`, `idle_ttl_seconds`, `schema_version`, `min_readable_schema_version`, `compact_after_bytes`, `max_timers_per_cell` and `logical_clock`. `cells_for` SHALL be a hook the block writes, not a key.

#### Scenario: Static cell keys and cells_for

- **WHEN** `#[flezha::tasklet(name = "static_entry_tasklet", cell_keys = ["a","b","c","d"])]` annotates a block that writes `fn cells_for`
- **THEN** the tasklet's `cell_keys()` are the listed keys, `CELLS_FOR_OVERRIDDEN` is `true`, and inputs are routed by the written `cells_for`

#### Scenario: Mailbox capacity

- **WHEN** a `mailbox_capacity` key is supplied
- **THEN** the tasklet's `mailbox_capacity()` returns it and `MAILBOX_CAPACITY_OVERRIDDEN` is `true`

#### Scenario: Logical clock

- **WHEN** `#[flezha::tasklet(name = "seq", logical_clock)]` annotates a block with a non-`NoChange` change
- **THEN** the tasklet's `CLOCK` is `Logical`

#### Scenario: Dynamic-mode knobs

- **WHEN** `#[flezha::tasklet(name = "rsi", idle_ttl_seconds = 5, max_cells = 10)]` is used
- **THEN** the tasklet's `idle_ttl()` and `max_cells()` return those values, `cell_keys()` is empty, and the tasklet declares no key of its own, because the key is `key_by` on the wiring

#### Scenario: The attribute offers no dynamic key hook

- **WHEN** a `#[flezha::tasklet]` declares a `cell_key` key
- **THEN** the macro fails to compile with a message directing the author to declare `.key_by(f)` on the stream that feeds the tasklet

### Requirement: `flezha::relay!` linear-pipeline macro

The Rust SDK SHALL provide a `flezha::relay!` declarative macro that declares a named relay type for a strictly linear pipeline and lowers to a normal `channels()` body. The macro SHALL begin with the type it declares, `pub struct Name;` (any visibility), and that type SHALL implement `Relay` so it is registered with `runner.register_factory(Name)`. The pipeline's `input` head SHALL accept an optional channel name in the same position the `plugin` terminator takes one, and SHALL lower to the entry channel name `input` when none is written.

#### Scenario: Linear pipeline removes the channels block

- **WHEN** `flezha::relay!{ pub struct StaticCell; id: "static_cell", pipeline: input::<String> -> StaticEntry -> StaticDownstream -> plugin::<Vec<u8>>("test_kafka") }` is written
- **THEN** `StaticCell` implements `Relay` wiring input → `StaticEntry` → `StaticDownstream` → the `test_kafka` plugin, whose entry channel is named `input`, and `runner.register_factory(StaticCell)` registers it

#### Scenario: The pipeline head names its entry channel

- **WHEN** a pipeline head is written `input::<String>("prices")`
- **THEN** the generated relay's entry channel SHALL be named `prices`

#### Scenario: Non-linear graph is rejected

- **WHEN** a `relay!` pipeline expresses fan-out or multi-input
- **THEN** the macro fails to compile with a message directing the author to an explicit `channels()` body

#### Scenario: An empty entry channel name is rejected

- **WHEN** a pipeline head is written `input::<String>("")`
- **THEN** the macro fails to compile, stating that an entry channel name may not be empty

### Requirement: Sugared and raw tasklets compose

A relay SHALL be able to mix tasklets declared with `#[flezha::tasklet]` and tasklets whose `impl Tasklet` block states every item by hand.

#### Scenario: Mixed relay builds and runs

- **WHEN** a relay wires a `#[flezha::tasklet]` tasklet to a tasklet whose `impl Tasklet` writes `NAME`, `CLOCK`, its flags and `init_state` by hand
- **THEN** the relay compiles, registers, and processes input through both tasklets correctly

### Requirement: `#[flezha::relay]` short-form attribute

The Rust SDK SHALL provide one attribute macro applied to a single `run` function that declares a whole single-tasklet relay, running as its `run` key says (`flow` when unwritten): `#[flezha::relay]`. It SHALL require an `factory_id`, SHALL accept an optional `output` naming a plugin channel, an optional `input` naming the entry channel, naming it `input` when the key is absent, and an optional `on_full` setting that entry channel's overflow policy. `#[flezha::relay]` SHALL additionally accept an optional `each` key taking `"once"`, `"per_input"` or `"chained"`, which the generated relay SHALL return from `each()`, and SHALL fail to compile naming any other value; `#[flezha::relay]` SHALL refuse the key naming the steps attribute. No other class-level setting SHALL be a key of either attribute. The kind SHALL NOT be expressible as a key. Two attributes are taken rather than one `#[flezha::relay]` because `flezha::relay!` is already a `#[proc_macro]` of that name in the same module (`sdk/rust/macros/src/lib.rs:42`).

Each attribute SHALL take one of two targets:

With an `output`, the generated topology SHALL wire the entry channel into the tasklet and the tasklet to that plugin output. With no `output`, it SHALL wire the entry channel into the tasklet and declare no plugin channel. An `output` on a run function with a unit return and no `emits` SHALL fail to compile with a message beginning `topology.output_without_type`.

#### Scenario: A flow short form emits a relay and a tasklet

- **WHEN** `#[flezha::relay(factory_id = "do_nothing", output = "echo")]` annotates `fn do_nothing(input: String) -> Vec<u8>`
- **THEN** the macro generates `DoNothing` implementing `Relay` with `factory_id()` `do_nothing` and a `channels()` body wiring `input` into `DoNothingTasklet` and `DoNothingTasklet` to the `echo` plugin output, and `DoNothingTasklet` implementing `Tasklet` with `Input = String`, `Output = Vec<u8>`, `State = ()`, `Change = NoChange` and `NAME` `do_nothing`

#### Scenario: The relay takes the author's name

- **WHEN** a function short form is registered
- **THEN** `runner.register_factory(DoNothing)` SHALL register the relay, and `DoNothing` SHALL NOT implement `Tasklet`

#### Scenario: A steps short form implements the steps relay trait

- **WHEN** `#[flezha::relay(factory_id = "place", output = "placed", input = "order")]` annotates `fn place(order: String) -> String`
- **THEN** `Place` implements `Relay` with `factory_id()` `place` and a `channels()` body wiring `order` into `PlaceTasklet` and `PlaceTasklet` to the `placed` plugin output, every other steps item at its default, and `runner.register_factory(Place)` registers it

#### Scenario: A steps short form declares its loop mode

- **WHEN** `#[flezha::relay(factory_id = "chat", output = "replies", each = "per_input")]` annotates `fn chat(message: String) -> String`, `Chat` is registered, and two messages reach its entry channel
- **THEN** `Chat::each()` is `Each::PerInput`, the tasklet runs once per message, and the instance does not complete after the first

#### Scenario: An unknown loop mode does not compile

- **WHEN** `#[flezha::relay(factory_id = "chat", each = "forever")]` annotates a run function
- **THEN** the macro fails to compile naming `each` and the three accepted values

#### Scenario: A short form with no output wires only the entry channel

- **WHEN** `#[flezha::relay(factory_id = "counter", input = "ticks")]` annotates `impl Tasklet for Counter` with `type Input = String`
- **THEN** `Counter`'s `channels()` body wires `ticks` into `Counter` and declares no plugin output, and the relay registers and runs

#### Scenario: A stateful short form sits on the impl block

- **WHEN** `#[flezha::relay(factory_id = "store_input", output = "sink", max_cells = 10)]` annotates `impl Tasklet for StoreInput` with `type State = StoreInputState` and `type Change = Stored`
- **THEN** `StoreInput` implements both `Tasklet` and `Relay`, its tasklet is named `store_input` with cells capped at 10, and `runner.register_factory(StoreInput)` registers it with no generated type

#### Scenario: A function short form with state is refused

- **WHEN** `#[flezha::relay(factory_id = "store_input")]` annotates `fn store_input(input: String, state: &mut S) -> String`
- **THEN** the macro fails to compile with a message beginning `ShortFormStateOnFunction`

#### Scenario: The generated tasklet is nameable in another topology

- **WHEN** `EchoTasklet` is named in a hand-written `channels()` body of a different relay
- **THEN** it wires as any tasklet does, and `Echo`'s own relay is declared only if `Echo` is also registered

#### Scenario: The attribute keeps the default entry channel name

- **WHEN** `#[flezha::relay(factory_id = "do_nothing", output = "echo")]` declares no `input` key
- **THEN** the relay's entry channel SHALL be named `input`

#### Scenario: The attribute names the generated entry channel

- **WHEN** `#[flezha::relay(factory_id = "do_nothing", output = "echo", input = "prices")]` annotates a run function
- **THEN** the relay's entry channel SHALL be named `prices`

#### Scenario: An output on a unit return is refused

- **WHEN** `#[flezha::relay(factory_id = "drain", output = "out")]` annotates a run function with a unit return and no `emits`
- **THEN** the macro fails to compile with a message beginning `topology.output_without_type`

#### Scenario: An empty entry channel name is rejected

- **WHEN** `#[flezha::relay(factory_id = "do_nothing", output = "echo", input = "")]` annotates a run function
- **THEN** the macro fails to compile, stating that an entry channel name may not be empty

#### Scenario: A topology body beside a short form does not compile

- **WHEN** a type carrying a short-form attribute also has a hand-written `Relay` impl
- **THEN** compilation fails, and the compile-fail fixture pinning it cites `topology.short_form_with_topology`

### Requirement: The Rust relay trait carries one body and one settings tree

`Relay` SHALL declare `fn channels(c: &ChannelBuilder);`, required, and SHALL declare no `tasklets`, `retry_attempts`, `retry_backoff`, `barrier_retry_attempts` or `wake_after` item; its items SHALL be `factory_id`, `channels` and `settings` (a `RelaySettings` holding `run`, `durable`, `max_duration` and `description`). The tier, its checkpoint and its correction window SHALL be members of `Run::Flow`, so a steps run cannot carry them, and `ZeroRetryBackoff` SHALL NOT exist. A steps cell SHALL write no effect record: the collecting path SHALL carry no effectively-once branch, and `steps_metadata` in the shared core SHALL advertise no `effectively_once` for a steps class. The step ladder SHALL take the consuming edge's `RetryPolicy` from the partition's dispatcher, as the stream ladders do, and the barrier re-send count SHALL be a constant beside the acknowledgement timeout. The `TaskletCtx` a steps cell's `run` and `on_timer` receive SHALL carry the same timer session a flow cell's does, so `timer_after`, `timer_at_event_time` and `cancel_timer` answer as in a flow relay; only a `compensate` hook's context SHALL answer `TaskletError::TimersUnavailable`. `sdk/rust/public-api.txt` SHALL record the new signature and none of the removed items.

#### Scenario: A steps relay writes channels and no retry knob

- **WHEN** a type implements `Relay` with `fn channels(c: &ChannelBuilder)` wiring `c.input::<String>("in").retry_max(2).into(Flaky)` and `Flaky` fails on every attempt
- **THEN** the class registers, the cell runs exactly twice before the instance aborts and compensates, and a type writing `fn retry_attempts() -> u32` in the same impl fails to compile

#### Scenario: The tier on a steps relay does not compile

- **WHEN** a `Relay` writes `Run::Steps { .. }` with an `exactly` member, or calls `c.effectively_once()` in `channels`
- **THEN** compilation fails, neither member nor builder call existing, and no `AddRelayResponse` for a steps class could carry `exactly`

#### Scenario: A stale body does not compile

- **WHEN** a type implements `Relay` with `fn tasklets(c: &ChannelBuilder)` and no `channels`
- **THEN** compilation fails naming the missing `channels` and the unknown `tasklets`

#### Scenario: The retired wake item does not compile

- **WHEN** a type implements `Relay` with `fn wake_after() -> Option<Duration>`
- **THEN** compilation fails naming `wake_after` as not a member of the trait

#### Scenario: A timer inside a steps tasklet fires

- **WHEN** a tasklet of a running `PerInput` steps relay calls `ctx.timer_after(Duration::from_millis(300))` in `run`
- **THEN** the call answers `Ok(at)`, and `on_timer` runs on that cell with a firing whose `scheduled_at == at`, opening a pass at that tasklet

### Requirement: The Rust context SHALL carry the replay flag and the crate's surface SHALL be gated

`Ctx::replay()` SHALL answer whether the invocation re-runs recorded history. The firing record
`TimerFiring` SHALL be the one `flz_core::timer` defines, re-exported by the crate, not a second
struct of the SDK's. The crate's public surface SHALL NOT carry `relay::dispatcher`, `cell::header`, `cell::flusher` or the
re-exports `pb`, `transport`, `service`, `errors` and `command`, and `sdk/rust/public-api.txt` SHALL
equal what `cargo public-api` prints for the crate, checked by a row of the gate list.

#### Scenario: A replayed invocation knows it

- **WHEN** a tasklet's `run` is invoked while the runner re-runs recorded history
- **THEN** `ctx.replay()` SHALL answer true, and false on a live invocation

#### Scenario: The snapshot cannot drift

- **WHEN** a public item is added to the crate and `public-api.txt` is not regenerated
- **THEN** the gate row SHALL fail naming the difference

### Requirement: Exported default tracing directives cover every SDK diagnostic target

The Rust SDK SHALL export a public constant `flezha_runner::diag::DEFAULT_DIRECTIVES` in `tracing_subscriber::EnvFilter` directive syntax. The directive list SHALL pass (at `warn` level and above) every tracing target the SDK's diagnostics are emitted on: the explicit `flezha::*` target family and the module-path targets of every crate the SDK's untargeted events inherit (`flz_core`, `flz_transport`, `flezha_runner`). An SDK-side test SHALL emit one probe event per diagnostic target family through an `EnvFilter` built from the constant and SHALL fail if any probe is discarded, so a new diagnostic target added without updating the constant fails the SDK build rather than silently disappearing downstream.

#### Scenario: Every diagnostic target family passes the exported directives

- **WHEN** one `warn`-level probe event is emitted per SDK diagnostic target family under an `EnvFilter` built from `DEFAULT_DIRECTIVES`
- **THEN** every probe SHALL be observed by the subscriber; none is discarded

#### Scenario: A new target family without a directive is a red build

- **WHEN** a diagnostic target family is added to the SDK without extending `DEFAULT_DIRECTIVES`
- **THEN** the SDK's directive-coverage test SHALL fail, naming the discarded target

### Requirement: Opt-in default subscriber installation

The Rust SDK SHALL export `flezha_runner::diag::init_default_tracing()`, which installs a `tracing_subscriber::fmt` subscriber whose filter is taken from the `RUST_LOG` environment variable when set, otherwise from `DEFAULT_DIRECTIVES` at the level `FLEZHA_LOG_LEVEL` names (`warn` when unset), and which registers the core's notice hook so core notices are emitted as `tracing` events on target `flezha`. The SDK SHALL NOT install any global subscriber implicitly — the helper runs only when the application calls it. A second call SHALL be a no-op that logs a warning rather than panicking, so two components defensively initializing tracing cannot take the process down.

#### Scenario: RUST_LOG overrides the exported defaults

- **WHEN** an application calls `init_default_tracing()` with `RUST_LOG=flezha=debug` set
- **THEN** the installed filter SHALL be built from `RUST_LOG`, not from `DEFAULT_DIRECTIVES`

#### Scenario: No implicit installation

- **WHEN** an application creates and runs a runner without calling `init_default_tracing()`
- **THEN** the SDK SHALL NOT have installed a global tracing subscriber

#### Scenario: Double initialization is survivable

- **WHEN** `init_default_tracing()` is called after a global subscriber is already installed
- **THEN** the call SHALL return without panicking and SHALL emit a single warning

#### Scenario: FLEZHA_LOG_LEVEL sets the default level

- **WHEN** an application calls `init_default_tracing()` with `RUST_LOG` unset and `FLEZHA_LOG_LEVEL=info`
- **THEN** the SDK's diagnostic targets SHALL pass at `info`

#### Scenario: A core notice reaches the subscriber

- **WHEN** an application called `init_default_tracing()` and a local run starts
- **THEN** the local-mode notice SHALL be emitted on target `flezha` and nothing SHALL be written to standard error by the core

### Requirement: The repository's own runner applications SHALL install the default subscriber

Every runner application this repository ships for exercising the platform SHALL
call `flezha_runner::diag::init_default_tracing()` before it starts a runner, so
the shared core's diagnostics reach the harness that captures the process's
output. An application that installs no subscriber discards every `tracing`
event the SDK and the shared kernel emit, which makes the reconnect, flood-ladder
and mailbox-stall diagnostics unobservable in exactly the drills that exist to
test recovery.

This SHALL NOT weaken the no-implicit-installation rule: the SDK still installs
nothing on its own, and these applications are the callers that rule assumes.

Where a host cannot call the Rust helper — the FFI veneers, which link
`flz_core_ffi` rather than the Rust SDK — the repository SHALL record whether
that host installs a subscriber, so the absence is a decision on the record
rather than an omission nobody has noticed.

#### Scenario: The reference runner's core diagnostics are observable

- **WHEN** a drill runs `test/apps/rust_runner` and the shared core emits a `warn`-level diagnostic
- **THEN** the event SHALL appear in the runner's captured output

#### Scenario: A runner application that installs nothing is a red build

- **WHEN** a runner application in `test/apps` links the Rust SDK and does not call `init_default_tracing()`
- **THEN** the conformance gate SHALL fail, naming that application

#### Scenario: The opt-in stays the application's

- **WHEN** a runner application is examined for how its subscriber is installed
- **THEN** the installation SHALL be that application's own call, and the SDK SHALL still install none on its own

#### Scenario: A veneer host's position is recorded

- **WHEN** a veneer host built on `flz_core_ffi` is examined for a tracing subscriber
- **THEN** the repository SHALL state whether that host installs one and why
