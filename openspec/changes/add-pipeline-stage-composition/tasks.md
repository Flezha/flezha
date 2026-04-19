## 1. SDK core primitives

- [ ] 1.1 Create `sdk/rust/src/pipeline/` module with `mod.rs`, `stage.rs`, `topology.rs`, `channel.rs`, `fanout.rs`, `runner.rs`, `metrics.rs`.
- [ ] 1.2 Define `Stage` trait: `type Input`, `type Output`, `init(&mut self)`, `process(&mut self, msg: Arc<Self::Input>) -> StageOutput<Self::Output>`, `shutdown(&mut self)`.
- [ ] 1.3 Implement `Channel<T>` backed by `crossbeam::channel::bounded`, with `send` / `try_send` / `send_timeout` obeying `OverflowPolicy`.
- [ ] 1.4 Implement `Fanout<T>` with `ArcSwap<Vec<Sender<Arc<T>>>>` for hot attach/detach.
- [ ] 1.5 Implement `Topology` builder: `add_stage`, `connect(from, to)`, `fanout(from, [to...])`, `start()`, `shutdown()`.

## 2. Runner control

- [ ] 2.1 Add `StageRunner` enum: `Inline`, `ThreadPool`, `DedicatedThread { cpu_affinity: Option<usize> }`.
- [ ] 2.2 Wire runner selection in `Topology::start()` so each stage launches on its declared runner.
- [ ] 2.3 Add optional `core_affinity` feature flag; silently no-op when unavailable.

## 3. Backpressure

- [ ] 3.1 Add `OverflowPolicy` enum: `Block`, `DropOldest`, `DropNewest`.
- [ ] 3.2 Enforce policy in `Channel::send()` per-stage.
- [ ] 3.3 Track `dropped_messages` and `blocked_nanos` counters per stage.

## 4. Hot attach/detach

- [ ] 4.1 `Topology::attach(stage, after: StageId)` — wire a new stage downstream of an existing producer at runtime via `ArcSwap` table swap.
- [ ] 4.2 `Topology::detach(stage_id)` — drain, signal graceful shutdown, remove from fanout tables.
- [ ] 4.3 Emit `StageLifecycleEvent` on the agent control channel on every attach/detach.

## 5. Backward-compat adapter

- [ ] 5.1 Re-implement `ProcessorHandler<S, I, O>` as a single-stage topology: input transformer → stage → output transformer.
- [ ] 5.2 Keep `ProcessorSettings` and `register_factory` API byte-for-byte compatible.
- [ ] 5.3 Cross-build smoke test: `cargo patch` the market-processor repo to this SDK branch and run its full test suite.

## 6. Observability

- [ ] 6.1 Per-stage metrics: `input_queue_depth`, `output_queue_depth`, `processing_latency_ns` histogram, `dropped_messages`, `processed_count`.
- [ ] 6.2 New `StageMetrics` message on the control channel, emitted on an operator-configurable interval (default 5s).
- [ ] 6.3 Metrics are opt-in per stage (`Stage::metrics_enabled(&self) -> bool`, default true) so trivial stages can skip the histogram cost.

## 7. Agent protocol

- [ ] 7.1 Extend `compute_engine_control_channel.sock` protocol with `StageLifecycleEvent { stage_id, event: Attached | Detached | Errored }`.
- [ ] 7.2 Extend with `StageMetrics { stage_id, ... }`.
- [ ] 7.3 Update Elixir agent decoder in `lib/flezha/` to parse and log new event types (routing behaviour deferred to Proposal 2).

## 8. Tests

- [ ] 8.1 Unit: `Channel` bounded semantics under each `OverflowPolicy`.
- [ ] 8.2 Unit: `Fanout` Arc sharing (verify refcount), hot attach/detach under load.
- [ ] 8.3 Unit: `ArcSwap` table swap does not lose steady-state messages.
- [ ] 8.4 Integration: 3-stage topology (source → map → sink), asserting message flow, backpressure, metrics.
- [ ] 8.5 Benchmark: single-stage topology vs legacy `ProcessorHandler` path. Target: overhead ≤ 500 ns/message.
- [ ] 8.6 Cross-repo smoke: market-processor test suite against this SDK.

## 9. Docs

- [ ] 9.1 New `docs/pipeline-stages.md` — stage trait, topology builder, runner modes, backpressure, fanout, hot attach.
- [ ] 9.2 Update `docs/architecture.md` with a DAG diagram showing a typical market-data topology (source → fanout → aggregators → sinks).
- [ ] 9.3 Update top-level `README.md` with a "Composing stages" section.
- [ ] 9.4 Add migration note in `sdk/rust/PROTOBUF_README.md` confirming `ProcessorHandler` remains supported unchanged.
