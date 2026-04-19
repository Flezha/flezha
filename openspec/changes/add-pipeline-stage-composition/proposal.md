## Why

Today Flezha exposes a single `ProcessorHandler<S, I, O>` trait: one registered factory equals one monolithic processor that owns all its state and executes every transformation inside one `process()` call. The `market-processor` downstream consumer already chains OHLC aggregation → indicator computation → order-book shaping inside one handler, with no visible seam between stages. Three upcoming requirements break this shape:

1. **Composable DAGs.** We want to fan one trade stream out to many parallel compute stages (dozens of user-parameterised EMAs per symbol). The monolith forces them into one `process()` body.
2. **Hot attach/detach.** User-driven alert subscriptions appear and disappear at runtime; reshaping a monolithic processor per subscription means either restarting it or funnelling all subscription state through one handler.
3. **Performance boundaries.** The monolith hides which stage is slow, prevents pinning a single expensive stage to its own core, and offers no per-stage backpressure.

Without staged composition, the market-data pipeline cannot scale cleanly to user-driven dynamic computation (tracked in the consumer repo's `add-dynamic-indicator-workers` change), and distributed worker coordination (tracked in `add-distributed-worker-coordination`) has no unit to assign across nodes.

## What Changes

- Introduce a **`Stage` trait** in the Rust SDK (`sdk/rust/src/pipeline/stage.rs`). A stage owns typed input/output, its own state, and its own lifecycle (`init`, `process`, `shutdown`). `ProcessorHandler` stays; it becomes a trivial single-stage topology adapter, so existing consumers keep working unchanged.
- Introduce a **`Topology` builder** — declarative DAG construction wiring stages by typed, bounded channels. Channels are `crossbeam::channel` (already a dependency), carrying `Arc<T>` for zero-copy fanout. Serialization stays at external boundaries only (Kafka, sockets).
- **Fanout is an explicit stage.** `Fanout<T>` takes one input and emits `Arc<T>` to N downstream channels. No message cloning between in-process stages.
- **Bounded backpressure.** Every inter-stage channel has a fixed capacity. Overflow policy (`Block`, `DropOldest`, `DropNewest`) is per-stage configuration.
- **Per-stage runner control.** Each stage declares its preferred runner: `Inline`, `ThreadPool` (shared default), or `DedicatedThread { cpu_affinity: Option<usize> }`. The operator honours these when wiring the topology.
- **Hot attach/detach.** `Topology::attach` / `Topology::detach` add or drain a downstream stage at runtime without restarting upstream producers. The fanout table is swapped under a write lock; steady-state sends remain read-lock (near-atomic).
- **Per-stage observability.** Each stage emits input queue depth, output queue depth, processing-latency histogram (p50/p99), drop count, processed count — exposed via a new control-channel message type.
- **BREAKING** (internal only): the `ProcessorSettings` factory path now routes through the topology engine. Public API unchanged; downstream market-processor keeps building with no edits.

## Capabilities

### New Capabilities

- `pipeline-stage`: composable stage primitive — trait, typed I/O, lifecycle, runner declaration.
- `pipeline-topology`: declarative DAG construction, typed channels, zero-copy Arc fanout, bounded backpressure, hot attach/detach.
- `pipeline-observability`: per-stage metrics surface exposed via the existing control-channel protocol.

### Modified Capabilities

<!-- None. `ProcessorHandler` becomes a single-stage adapter but keeps its public contract. -->

## Impact

- **Flezha Rust SDK (`sdk/rust/src/`)**: new `pipeline/` module (`stage.rs`, `topology.rs`, `channel.rs`, `fanout.rs`, `runner.rs`, `metrics.rs`). `processor/` keeps existing trait; its implementation delegates to a one-stage topology.
- **Flezha operator (`sdk/rust/src/compute_engine/`)**: factory registration wires a one-stage topology when called via the legacy path; new callers can build multi-stage topologies directly.
- **Flezha agent (Elixir, `lib/flezha/`)**: control-channel protocol gains `StageLifecycleEvent` and `StageMetrics` message types. No behavioural change to existing routing — stages for now are treated as opaque units within a single node (Proposal 2 lifts that restriction to cross-node).
- **Dependencies**: `core_affinity` (optional feature, pulled in only for `DedicatedThread` with affinity). Otherwise no new crates — `crossbeam` already present.
- **Docs**: new `docs/pipeline-stages.md`; update `docs/architecture.md` with a DAG diagram and a migration note for existing `ProcessorHandler` users (no action required).
- **Tests**: new `tests/pipeline/` harness with source → map → sink fixtures; benchmark comparing single-stage topology to legacy monolithic path (target: ≤ 500 ns/message overhead).
- **Compatibility**: market-processor builds unchanged; CI for both repos must run a cross-build smoke test (pull this SDK branch via `cargo patch`, build market-processor, run its test suite).
