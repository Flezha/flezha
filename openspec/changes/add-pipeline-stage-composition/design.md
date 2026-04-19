## Context

Flezha's current contract is "one processor per source". That fit the original use case: one registered factory ingested one feed and produced one output shape. As Flezha consumers move toward user-driven dynamic computation (the alert-rule and indicator-subscription work in the flezha_market consumer), they need to fan a single source into many parallel computation paths that come and go at runtime. The monolithic processor is the wrong unit.

The question is whether to layer composition on top of the existing trait or replace it. We choose to layer because external consumers (market-processor) already depend on `ProcessorHandler` and we do not want to force a breaking migration for a change whose value is additive.

## Goals / Non-Goals

**Goals:**
- Compose stages into a DAG via a declarative Rust builder.
- Keep the hot path allocation-light: zero-copy fanout via `Arc<T>`, no serialization between in-process stages.
- Per-stage runner control (inline, shared pool, dedicated thread with optional CPU pinning).
- Per-stage backpressure with configurable overflow policy.
- Hot attach/detach of downstream stages without restarting upstream producers.
- Backward compatibility: existing `ProcessorHandler` consumers keep working unchanged.

**Non-Goals:**
- Cross-node stage distribution — covered by `add-distributed-worker-coordination`.
- Durable state, checkpointing, or state replication — each stage owns its state in RAM; persistence is the consumer's problem (covered by `add-partitioned-state-bootstrap` in the market consumer).
- A config-file DSL for topology — topologies are Rust builder code in v1. A YAML/TOML surface can come later.
- Async-first design — most stages are CPU-bound sync work; we stay sync at the stage boundary.

## Decisions

### `Stage` layered under `ProcessorHandler`

`Stage` is the new primitive. `ProcessorHandler` is kept and re-implemented as a trivial single-stage topology. Consumers don't change code.

Alternatives:
- **Replace `ProcessorHandler`**: breaks every Flezha user. Rejected.
- **Keep both primitives independent**: doubles surface area and semantics drift. Rejected.

### Channels: `crossbeam::channel`, not `tokio::sync::mpsc`

`crossbeam` is already a dependency, offers bounded mpsc with lock-free steady state, and supports `try_send` / `send_timeout` for overflow policy. Tokio channels would force async through every stage boundary, which is inappropriate for the sync CPU-bound work most market-data stages do.

### Fanout via `Arc<T>`, not cloning

A fanout from one source to 20 EMA stages must not clone 20 copies of a trade struct. `Arc::new(trade)` → each downstream receives a refcount increment. Requirement: `T: Send + Sync + 'static`. Fanout arity capped at 64 in v1; if we hit that, consider a broadcast channel with buffer rotation.

### Hot attach/detach with `ArcSwap` on the fanout table

The fanout table (`Vec<Sender<Arc<T>>>`) sits behind `ArcSwap`. Steady-state send is a read (an atomic pointer load). Attach/detach swaps the whole table atomically. This keeps the hot path lock-free while supporting dynamic topology changes. Overhead measured in ns per send.

### Per-stage runner declaration

Three modes:
- `Inline` — runs on the caller's thread. Appropriate for trivial transforms (filter, project, light arithmetic) where a thread hop would dominate the work.
- `ThreadPool` — shared pool, default. Fit for most stages.
- `DedicatedThread { cpu_affinity: Option<usize> }` — its own OS thread, optional core pinning. Fit for hot stages (e.g. the per-symbol OHLC aggregator on a busy pair).

Affinity is a hint; the SDK uses `core_affinity` when available and silently no-ops otherwise.

### Overflow policies as explicit configuration

Three policies: `Block` (default for correctness-critical stages), `DropOldest` (for metric-style stages where recency matters), `DropNewest` (when late arrivals are preferred dropped).

## Risks / Trade-offs

- **Overhead vs monolithic processor.** Zero-copy fanout plus bounded channels add ~100–500 ns per stage hop. Target envelope verified by a benchmark comparing a single-stage topology to the legacy path. If we exceed 500 ns we revisit channel choice.
- **Arc contention at very high rates.** At 1M trades/sec × 100 fanout subscribers = 100M atomic increments/sec — not free. Mitigation: v1 caps fanout arity at 64; higher requires a broadcast channel redesign.
- **Hot-attach races.** `ArcSwap` means new subscribers might miss the in-flight message at the instant of swap. Acceptable — attach is rare, and consumers are responsible for their own bootstrap protocol (which the market-app's Postgres-shipping design already handles).
- **Backward-compat correctness.** The `ProcessorHandler` adapter must behave identically to the current implementation. Verified by running market-processor's existing test suite against a cargo-patch build of the SDK.
- **Metrics overhead.** Per-stage latency histograms have a cost. Metrics are opt-in per stage to keep trivial stages cheap.
