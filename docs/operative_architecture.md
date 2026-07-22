# Rust SDK Operative Architecture

The Rust SDK exposes a small set of traits for declaring how compute
work is structured inside one logical runner instance. This doc
walks the concepts (Unbounded Operative, Operation, Segment, Runner),
the data flow end-to-end, and the public API. For the rationale, design
decisions, and capability spec deltas, see
`openspec/changes/operative-architecture/` (umbrella) and
`openspec/changes/operative-mvp-rust/` (v1 cutover).

## Concepts at a glance

```
                        Wire (UDS / QUIC)
                                │
                                ▼
                  ┌──────────────────────────┐
                  │ OperativeDispatchHandle   │  metadata["channel"] →
                  │ (per-process)             │  topology lookup
                  └──────────────────────────┘
                                │
                                ▼
            ┌─────────────────────────────────────────┐
            │             OperativeInstance            │
            │   (one per `flz operative start`)        │
            │                                          │
            │   ┌────────────┐   ┌──────────────┐      │
            │   │ Operation A│ ->│ Operation B  │ -> ... -> Plugin
            │   │ (seg 0)    │   │ (seg 1m)     │      │
            │   │ (seg 1)    │   │ (seg 1h)     │      │
            │   └────────────┘   └──────────────┘      │
            └─────────────────────────────────────────┘
```

- **Unbounded Operative class** — the type-level declaration. One
  Unbounded Operative class declares its channels and the Operations
  that produce/consume them, once per runner via
  `FlezhaRunner::define_unbounded::<R>()` (legal before and after `start()`;
  `remove_unbounded::<R>()` is its inverse). The topology is built and
  validated at the declaring call site and shared (`Arc`) by every instance
  of that class.

- **Operative instance** — the live runtime object spawned on
  `AddOperative`. Each instance has its own per-Segment state
  (per-Operation) and its own mailboxes; instances are independent of
  each other and routed by `operative_id` (the wire calls it that; the
  SDK calls it `instance_id`).

- **Operation** — the unit of compute. Has typed `Input` / `Output` / `State`
  associated types, a sync `run` method, and a segmentation declaration.
  One Operation inside an Operative can have one or more **Segments**.

- **Segment** — a "lane" of an Operation. Single-mode (default) means one
  segment per Operation; static mode declares fixed `operation_keys()` and
  routes inputs to one or more named segments via `segment_for`;
  dynamic mode overrides `operation_key()` (with empty `operation_keys()`) to
  create segments lazily per key seen at runtime (see "Dynamic
  segmentation" below). At most one drain task runs per segment at any
  time (sticky ordering).

- **Channel** — the typed wire between producers/consumers. Three kinds:
  `Entry` (controller → SDK; any payload type — `Vec<u8>` passthrough,
  `String` UTF-8-decoded, anything else deserialized at the entry edge
  with the SDK's default JSON codec, or a custom parser via `input_with`;
  a failing parse rides the channel's retry→DLQ ladder),
  `InterOperation` (`Arc<T>` zero-copy between Operations), and `PluginBound`
  (typed value serialized to bytes; sent to a named plugin via the
  controller).

- **Runner** — the work-stealing pool that drains mailboxes. One pool
  per runner (`FlezhaRunner::start()`); size controlled by `FLEZHA_WORKER_POOL_SIZE`.

### Operative kind and capabilities (controller-side)

On the controller, every operative is a **compute unit** that carries a **kind** (an
atom, default `:unbounded`, set at start and frozen for its lifetime). Each kind
declares a **sparse,
default-`false` capability descriptor** in `Flezha.Operative.Kind` — the single
source of truth listing only the capabilities a kind *has* (as `true`); the
control plane reads every flag with `Flezha.Operative.Kind.capable?/2`, so any
capability a kind does not declare reads `false`. The `:unbounded` kind declares
`persistent_state?` (per-segment WAL/snapshot/restore/time-travel/state
introspection). Adding a
capability later is a one-line extension: add the single gate that reads the
flag (defaulting absent to `false`) and opt the built-in kinds that need it
into it in `Flezha.Operative.Kind`; kinds that do not opt in are unaffected. The
first gate is per-segment state introspection, which returns
`{:error, {:unsupported_for_kind, :persistent_state}}` for a kind without
`persistent_state?` instead of querying a compute unit that has no state.

### The `:bounded` kind (run-once jobs)

`:bounded` is a stateless, run-once compute unit — a job. It declares **no**
capabilities (so `persistent_state?` reads `false`, and the state-introspection
gate rejects `flz operative state <bounded>`), has no operations, segments, or input
channel, and may emit output to bound plugins like an unbounded operative.

Lifecycle: `flz operative start <id> <factory> --kind bounded` populates the optional
`kind` field on the `AddOperative` wire message (empty ⇒ `:unbounded`, the
default). The SDK holds a sealed unit registry (unbounded or bounded) resolved once
per frame at the dispatch boundary; for a bounded operative it schedules the author's
`BoundedOperative::run(ctx, args)` once on the work pool, with `args` taken from
`init_values`. When `run` returns, the SDK emits a `BoundedCompleted`
SDK→Controller control message; the controller drives **deregister-on-completion**
through the normal stop/teardown path (`OperativeManager.stop_operative/2`), so a
finished bounded operative drops out of the durable registry — never replayed on boot, never
misjudged by convergence. A `run` that errors is terminal (deregister + report
`[:flezha, :bounded, :failed]`; no retry).

Authoring (Rust SDK): a distinct lightweight trait, declared as a sibling of
`define_unbounded`:

```rust
struct Greet;
impl flezha_runner::BoundedOperative for Greet {
    fn factory_id() -> &'static str { "greet" }
    fn run(ctx: &mut flezha_runner::BoundedCtx, args: &str)
        -> Result<(), flezha_runner::BoundedError> {
        ctx.emit(format!("hello {args}").into_bytes())?;
        Ok(())
    }
}
flezha_runner::Flezha::create_runner("my_app")?.define_bounded::<Greet>()?.run()?;
```

Authoring (Java SDK): a distinct `com.flezha.sdk.bounded.BoundedOperative` interface, declared
as a sibling of `defineUnbounded`:

```java
public final class GreetBounded implements BoundedOperative {
    public String factoryId() { return "greet"; }
    public void run(BoundedCtx ctx, String args) throws BoundedException {
        ctx.emit("hello " + args);
    }
}
Flezha.createRunner("my_app").defineBounded(GreetBounded.class).run();
```

The Java runtime mirrors the Rust shape: a sealed `Operative` (provisional name)
permits an unbounded instance or a bounded instance in one per-runner registry,
resolved once per frame at `PrimaryStreamReader.dispatch`; an `AddOperative` whose
`kind` resolves to `:bounded` schedules `BoundedOperative.run` once on the worker pool with
`init_values` as `args`; `BoundedCtx.emit` writes an instance-id-keyed,
empty-metadata frame on a secondary stream; and `BoundedCompleted` is emitted on
the control stream byte-identically to the Rust SDK, so the controller drives a
Java bounded operative's teardown unchanged. Java bounded factories are advertised on connect
alongside unbounded factories. Java v1 ships hand-written `BoundedOperative` impls only (no
`@BoundedOperative` annotation sugar) and no bounded test harness; it inherits the same deferred
gaps below.

A near-instant bounded operative may run to completion and self-deregister before the start
RPC confirms the unit's binding; that clean exit is reported as a successful
start (`{:ok, …}` with no live `runner_id`), not a failure.

Deferred for a later change (v1 known gaps): mid-run controller reboot may
replay an in-flight bounded operative from the durable registry; no exactly-once
dedup or in-flight visibility/cancel. (A failing *segment* of a structured chain
does retry with backoff — see
`docs/resilience/bounded_operative.md` "Segment retry".)

## Public API

The full surface is small:

```rust
use flezha_runner::{
    Flezha, ChannelBuilder, UnboundedOperative, Operation, OperationCtx, OperationError,
};
```

`FlezhaRunner::define_unbounded::<R: UnboundedOperative>()` declares one
Unbounded Operative class on the runner `Flezha::create_runner(namespace)`
returns; `.start()` / `.run()` boots the SDK; the controller later spawns
instances by calling `flz operative start <factory_id>`. A second `define_*`
of the same factory id on the same runner is refused
(`FlezhaError::DuplicateFactoryId` / `IllegalArgumentException`) — that
refusal is what "declared once" means, and it is scoped to the runner's
declared set, which starts empty for every runner even though the underlying
class registry is process-global.

Starting with zero declared classes is legal, and `define_*` / `remove_*`
stay callable after `start()`, with three caveats worth knowing:

- `remove_*` on a started runner **refuses while live instances of that
  factory exist** (`FlezhaError::OperativeTypeInUse` /
  `OperativeTypeInUseException`); the type stays defined.
- A post-start change ships the runner's full factory set as an
  `UpdateFactories` control request, which is **fire-and-forget — there is no
  ack**. The reconnect init frame, serialized from the same per-runner set at
  send time, is the resync of record.
- A parked/suspended durable bounded instance is absent from the SDK's
  live-instance registry, so the removal check cannot see it and `remove_*`
  succeeds; rehydration then fails placement. See
  `docs/resilience/bounded_operative.md`.

Calling `define_*` / `remove_*` after `start()` requires binding the runner:
in Rust a chained `Flezha::create_runner(ns)?.…start()?` drops the temporary
at the end of the statement, so write `let runner = …start()?;` (`run()` is
immune — it blocks until shutdown, which is why the samples below chain it).
The authoritative surface is the rustdoc on `sdk/rust/src/lib.rs`; nothing
doctests the snippets in this file.

## Authoring sugar (default path)

`#[flezha::operation]`, `#[derive(flezha::State)]`, and `flezha::operative!` collapse
the trait ceremony — they are **pure expansion** over the `UnboundedOperative`/`Operation`/
`Change`/`Channel` traits below, so a sugared operation and a hand-written one
compose in the same operative and "ejecting" a sugared operation to a full impl is a
paste of its expansion. The single contract binding every SDK (Rust, Java, and
future Python/Elixir) is `docs/sdk_authoring_contract.md`.

```rust
use flezha_runner::state::StateHandle;
use serde::{Deserialize, Serialize};

// `&mut State` ⇒ stateful; the return value is auto-emitted to `sink`.
#[derive(Serialize, Deserialize, Clone, Default, flezha_runner::State)]
struct OhlcState { candles: Vec<f64> }

#[flezha_runner::operation(id = "parse_op")]
fn parse(input: String) -> f64 {
    input.parse().unwrap_or(0.0)
}

#[flezha_runner::operation(id = "ohlc_op", keys = ["1m", "1h", "1d", "1w"], segment_for = all_tfs)]
fn ohlc(price: f64, state: &mut OhlcState) -> Vec<u8> {
    state.push_candles(price);            // journaled via #[derive(State)]
    format!("{price}").into_bytes()
}

fn all_tfs(_: &f64) -> Vec<&'static str> { vec!["1m", "1h", "1d", "1w"] }

// Linear pipeline ⇒ no `channels()` block, auto-registered (no `define_unbounded`).
flezha_runner::operative! {
    id: "market",
    pipeline: input::<String> -> Parse -> Ohlc -> plugin::<Vec<u8>>("ohlc_kafka")
}
```

Single-operation `input → sink` operatives add `sink = "…"` to `#[flezha::operation]` and
need no `operative!` and no `define_unbounded`. Attribute opt-ins: `id`, `sink`,
`keys` + `segment_for` (static mode), `operation_key` + `idle_ttl` +
`max_segments` (dynamic mode), `capacity`, `on_full`, `clock`. Static and
dynamic opt-ins are mutually exclusive (a compile error). Anything beyond a
single operation or a linear chain — fan-out, multi-input — stays on an explicit
`channels()` body (the escape hatch below).

## Worked example (raw traits — the escape hatch)

The sugar above expands to exactly this hand-written form; reach for it
directly when an operative's topology is not a single operation or a linear chain.

```rust
use flezha_runner::{
    Flezha, ChannelBuilder, UnboundedOperative, Operation, OperationCtx, OperationError,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default)]
struct OhlcState {
    candles: Vec<f64>,
}

#[derive(Default)]
struct ParseOp;
impl Operation for ParseOp {
    type Input = String;
    type Output = f64;
    type State = ();

    fn name() -> &'static str { "parse_op" }
    fn init_state(_: Option<&str>) {}

    fn run(&mut self, input: String, _: &mut (), ctx: &mut OperationCtx<'_>)
        -> Result<(), OperationError>
    {
        let price: f64 = input.parse().map_err(|e: std::num::ParseFloatError| {
            OperationError::Custom(e.to_string())
        })?;
        ctx.emit::<f64>(price)
    }
}

#[derive(Default)]
struct OhlcOp;
impl Operation for OhlcOp {
    type Input = f64;
    type Output = Vec<u8>;
    type State = OhlcState;

    fn name() -> &'static str { "ohlc_op" }
    fn init_state(_: Option<&str>) -> OhlcState {
        OhlcState::default()
    }

    fn operation_keys() -> Vec<&'static str> {
        vec!["1m", "1h", "1d", "1w"]
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(_: &f64) -> Vec<&'static str> {
        vec!["1m", "1h", "1d", "1w"]
    }

    fn run(&mut self, price: f64, state: &mut OhlcState, ctx: &mut OperationCtx<'_>)
        -> Result<(), OperationError>
    {
        state.candles.push(price);
        ctx.emit::<Vec<u8>>(format!("{}", price).into_bytes())
    }
}

struct MarketOperative;
impl UnboundedOperative for MarketOperative {
    fn factory_id() -> &'static str { "market" }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("trade.raw")
            .into::<ParseOp>()          // returns ParseOp's output stream handle
            .named("trade.parsed")
            .into::<OhlcOp>()           // returns OhlcOp's output stream handle
            .named("ohlc.candles")
            .output("ohlc_kafka");      // plugin-bound terminal
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    Flezha::create_runner("market_ns")?
        .define_unbounded::<MarketOperative>()?
        .run()?;
    // The controller's `flz operative start <market>` spawns an instance.
    Ok(())
}
```

### Java SDK equivalent

The Java SDK is a port of the same model. Idiomatic differences:

- `Operation<Input, State, Output>` is a parameterized interface (no associated
  types in Java); implementers expose `Class<Input>` / `Class<Output>`
  accessors so the runtime can verify wiring at registration time
  (Java's type erasure means most type bounds the Rust trait system
  enforces at compile time become runtime-checked
  `UnboundedOperativeConfigurationException`s thrown by
  `FlezhaRunner.defineUnbounded(...)`).
- `segmentForOverridden()` / `operationKeyOverridden()` boolean default
  methods replace Rust's `const SEGMENT_FOR_OVERRIDDEN: bool` /
  `const OPERATION_KEY_OVERRIDDEN: bool` sentinels.
- `OperationException` is a checked exception subclass of `Exception`;
  `Operation.run` declares `throws OperationException`.
- Inter-operation "zero-copy" passes the value by reference (Java has no
  `Arc<T>`); the user MUST treat emitted values as immutable from the
  producer side after `ctx.emit` returns. The
  `Inter-operation zero-copy preserves value identity` capability scenario
  asserts `==` reference-equality on the value received downstream.

```java
public record OhlcState(java.util.List<Double> candles) {}

public final class ParseOp implements Operation<String, Object, Double> {
    @Override public String name() { return "parse_op"; }
    @Override public Class<String> inputType() { return String.class; }
    @Override public Class<Double> outputType() { return Double.class; }
    @Override public Object initState(Optional<String> operationKey) { return new Object(); }

    @Override public void run(String input, Object state, OperationCtx ctx) throws OperationException {
        try {
            ctx.emit(Double.parseDouble(input));
        } catch (NumberFormatException e) {
            throw new OperationException("parse failed: " + input, e);
        }
    }
}

public final class OhlcOp implements Operation<Double, OhlcState, byte[]> {
    @Override public String name() { return "ohlc_op"; }
    @Override public Class<Double> inputType() { return Double.class; }
    @Override public Class<byte[]> outputType() { return byte[].class; }
    @Override public OhlcState initState(Optional<String> operationKey) {
        return new OhlcState(new java.util.ArrayList<>());
    }

    @Override public java.util.List<String> operationKeys() { return java.util.List.of("1m", "1h", "1d", "1w"); }
    @Override public boolean segmentForOverridden() { return true; }
    @Override public java.util.List<String> segmentFor(Double price) { return operationKeys(); }

    @Override public void run(Double price, OhlcState state, OperationCtx ctx) throws OperationException {
        state.candles().add(price);
        ctx.emit(price.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}

public final class MarketOperative implements UnboundedOperative {
    @Override public String factoryId() { return "market"; }
    @Override public void channels(ChannelBuilder c) {
        c.input(String.class, "trade.raw")
                .into(ParseOp.class)     // returns ParseOp's output stream handle
                .named("trade.parsed")
                .into(OhlcOp.class)      // returns OhlcOp's output stream handle
                .named("ohlc.candles")
                .output("ohlc_kafka");   // plugin-bound terminal
    }
}

public final class App {
    public static void main(String[] args) throws Exception {
        Flezha.createRunner("market_ns")
                .defineUnbounded(MarketOperative.class)
                .run();
        // Controller's `flz operative start <market>` spawns an instance.
    }
}
```

#### Java authoring sugar

The `@Operation` / `@State` / `@UnboundedOperative` / `@SegmentFor` annotations (annotation
processor `flezha-sdk-processor`) generate the impls above, mirroring the Rust
macros under the same `docs/sdk_authoring_contract.md` contract. An `@Operation`
class declares a `run` method (`Output run(Input)` or
`Output run(Input, StateHandle<State, Change>)`); the processor generates
`<ClassName>Operation` (the five `*Type()` getters, `name`, `initState`,
`applyChange`), a single-operation operative when `sink` is set, and a
`GeneratedUnboundedOperatives.defineAll(FlezhaRunner)` aggregator so the author
declares every sugared operative on the runner in one startup call — it takes
the runner because the declared set is per-runner, not process-global.
`@State` generates `<ClassName>Change` plus a `<ClassName>Handle` carrying a
`set<Field>` / `push<Field>` mutator per field; a stateful `run` takes that
handle and journals with `state.push<Field>(v)` — no change type named, the
same shape as Rust's `state.push_field(v)`. Worked
twins live in `test/apps/java_runner` (`SugarDoNothing`, `SugarStore`,
`SugarEntry`/`SugarDownstream` + `SugarStaticSegmentOperative`). The dynamic-mode
`@OperationKey` opt-in generates the dynamic surface but is not yet runnable: the
Java runtime has not landed `operative-dynamic-segmenting` (no `segmentIdleTtl`/
`maxSegments` methods, and the v1 runtime rejects `operationKeyOverridden`).

## Hot-path lookups

The dispatch path is engineered to keep the per-message cost low:

1. **Wire input → entry channel id** — one `HashMap<&'static str, u32>`
   read on the `metadata["channel"]` key, then a bounds-elided `Vec`
   index into `topology.channels[id]`.
2. **Entry channel → consumer segment** — for each consumer Operation:
   - Single-mode: jump straight to segment index 0.
   - Static-mode: one HashMap lookup per segment key returned by
     `S::segment_for(&input)`.
   - Dynamic-mode: one `S::operation_key(&input)` call, then one DashMap
     lookup on the per-Operation `DynamicSegmentRegistry` (lazily creating
     the segment on a miss). `None` from `operation_key` drops the input.
3. **Mailbox push + counter + inflight CAS** — one `ArrayQueue::try_push`,
   one Relaxed `pushes_total` increment, and one atomic compare-exchange.
   If the CAS wins, submit a drain task to the pool; otherwise an
   in-progress drain will pick up the new item via the missed-wake
   recovery loop. On a full mailbox the channel's declared
   `OverflowPolicy` (Block / DropNewest / DropOldest) decides what
   happens — see "Backpressure and bounded mailboxes" below.
4. **Drain → Operation::run** — the dispatcher downcasts `Arc<dyn Any>` to
   `Arc<S::Input>`, falls back to a value-clone (`Arc::unwrap_or_clone`)
   if the Arc was shared by fan-out, and calls `Operation::run` with the
   typed signature.

The umbrella's "Per-Message Dispatch-Overhead Budget" requirement caps
this at **4 sync ops per message** on the single-segment fast path
(registry lookup + mailbox enqueue + success counter + inflight CAS);
static-mode fan-out adds `N × 3` ops where N is the segment fan-out
width (try_push + counter + inflight CAS per target). The added counter
op compared with the pre-`operative-bounded-mailbox` baseline is detailed
in `openspec/changes/operative-bounded-mailbox/design.md` Decision 7.

For the recorded throughput floor and how it's measured, see
`docs/benchmarking.md` — it documents the regression / segmentation /
single-segment / static-fanout bench shapes, the recorded
`BENCH_FLOOR_MSG_PER_SEC` and `SINGLE_SEGMENT_FLOOR_MSG_PER_SEC`
values, the floor-update protocol, and CI triage instructions when the
`bench-regression` gate fires.

## Dynamic segmentation

Dynamic mode lifts the compile-time-keyset requirement: an Operation opts in by
overriding `operation_key()` (with `OPERATION_KEY_OVERRIDDEN = true`) and an empty
`operation_keys()`. Use it only when the keyset is genuinely unbounded
(per-user, per-session, per-arbitrary-symbol); for a bounded keyset, static
mode is simpler and cheaper.

A dynamic Operation owns zero Segments at startup. The first input with
`operation_key(&input) == Some(key)` lazily creates a `SegmentSlot` (cold
`init_state(Some(key), None)`) under a per-Operation `DynamicSegmentRegistry`
(`DashMap`); `None` drops the input. Concurrent producers for a fresh key
share one slot (the DashMap insert serializes), and the inflight CAS then
serializes dispatch as in static mode.

Two per-Operation knobs:

- `segment_idle_ttl() -> Option<Duration>` — **default `None` = no
  eviction** (Segments live as long as the Operative; no reaper spawned).
  `Some(ttl)` spawns a per-Operation reaper that evicts any Segment idle
  longer than `ttl`, sweeping every `min(ttl/4, 30s)`
  (`FLEZHA_REAP_INTERVAL_MS` overrides for tests). It takes a Segment's
  drain gate before evicting, so it never interrupts a running `Operation::run`.
  **Eviction is destructive:** the slot and (when persistent) the on-disk
  WAL/snapshot are dropped (a fire-and-forget `delete_segment_state` to
  the controller), and a later input cold-starts fresh — no
  restore-from-disk. (Deliberate: keeps recreate off any blocking
  controller round-trip that could stall a pool worker.)
- `max_segments() -> usize` (default `100_000`) — soft cap. At the cap,
  new-key inputs are dropped (`inputs_dropped_capped_total`); existing keys
  are unaffected. Bounds memory against a runaway `operation_key`, not
  admission.

Per-Operation counters (`DynamicSegmentCounters`): `segments_created_total`,
`segments_evicted_total`, `inputs_dropped_no_key_total`,
`inputs_dropped_capped_total`.

## Backpressure and bounded mailboxes

Each Segment's mailbox is a fixed-size lock-free
`crossbeam::queue::ArrayQueue` allocated at operative startup. Capacity is
declared per Operation; the policy applied when the mailbox is full is
declared per channel. Counters on the consumer-side `SegmentSlot`
record every push, drop, and block event for future metrics export.

### Capacity declaration (consumer side)

```rust
impl Operation for OhlcOp {
    fn mailbox_capacity() -> usize { 4096 }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    // ... other items ...
}
```

Resolution order at `define_unbounded::<R>()` time:

1. `Operation::mailbox_capacity()` if `MAILBOX_CAPACITY_OVERRIDDEN = true`.
2. `FLEZHA_MAILBOX_CAPACITY` env var (parsed as `usize`).
3. Built-in default (1024).

Capacity must be in `1..=1_048_576`; out-of-range values fail
registration with the offending Operation's name. Static-mode Operations
allocate one mailbox per declared segment key, all sharing the
same capacity.

### Policy declaration (producer / wiring side)

```rust
c.input::<Vec<u8>>("trade.raw")
    .on_full(OverflowPolicy::DropNewest)   // entry-side policy
    .into::<ParseOp>()
    .into::<OhlcOp>()                      // OhlcOp's output stream handle
    .named("ohlc.candles")
    .on_full(OverflowPolicy::Block)        // inter-operation policy
    .into::<IndicatorOp>();
```

`OverflowPolicy` is one of:

- **`Block`** — the producer first drains the full mailbox inline when no
  drainer is actively running (the slot's drain may exist only as a queued
  pool task; parking on it self-deadlocks a fully-blocked worker pool —
  the 2026-06-10 production freeze), otherwise parks briefly on the
  consumer's `BlockParker` and retries. Preserves ordering, loses nothing,
  and never stalls indefinitely; a producer blocked >5 s logs a
  `flezha::mailbox` warning naming the consumer operation. Requires an
  acyclic operation graph.
- **`DropNewest`** — incoming item discarded; consumer's
  `drops_newest_total` counter increments by 1.
- **`DropOldest`** — consumer's mailbox atomically pops its oldest item
  and accepts the new one (`ArrayQueue::force_push`); consumer's
  `drops_oldest_total` counter increments by 1. Best-effort under MPMC
  contention.

The plugin-bound terminal `output(...)` returns a finalized `Channel`
with no further builder methods, so nothing — `.on_full(...)` included —
can chain after it. There is no SDK-side mailbox between a plugin-bound
emit and the wire, so a policy declared earlier on a stream that ends
plugin-bound is never consulted.

### Default policy by channel kind

| Channel kind   | Default | Why |
|---|---|---|
| Entry          | `DropNewest` | The wire reader services every Operative sharing one physical connection; blocking on one Operative's saturation head-of-line-blocks siblings. |
| InterOperation | `Block`      | Stateful pipelines need ordering preservation; mid-pipeline drops silently corrupt derived state. |
| PluginBound    | (n/a)        | No SDK-side mailbox sits between the producer Operation and the wire; backpressure is OS socket buffer + transport flow control. |

A runner who wants strict end-to-end ordering at the entry edge can
declare `.on_full(OverflowPolicy::Block)` on their entry channel — the
wire reader then drains the saturated Operative's mailbox inline (briefly
delaying sibling Operatives on the same physical connection) instead of
dropping. The default `DropNewest` keeps the reader free of that
co-tenancy cost and records every drop on the per-Segment
`drops_newest_total` counter.

### Per-Segment counter set

Every `SegmentSlot.mailbox` carries a `SegmentCounters` block with
four `AtomicU64` counters, all bumped under `Ordering::Relaxed`:

| Counter | Bumped on |
|---|---|
| `pushes_total`         | every successful `try_push` |
| `pushes_blocked_total` | every `Block`-policy park event (one per `wait_one`) |
| `drops_newest_total`   | every drop under `DropNewest` |
| `drops_oldest_total`   | every eviction under `DropOldest` |

Counters are read-only from outside the dispatch path (`segment.counters()`
or `segment.mailbox.counters()`); export to a metrics backend (Prometheus,
StatsD, log emit) is **out of scope** for `operative-bounded-mailbox` and
deferred to a separate observability change. Storing the data here keeps
that change purely additive.

### Worker-pool sizing under `Block`

A Block-policy producer parks the worker thread it ran on. A pipeline
of depth D under `Block` requires the work-stealing pool to have
`FLEZHA_WORKER_POOL_SIZE ≥ 2 × D + 1` to avoid a deadlock: D parked
producers + 1 thread free to drain the deepest consumer + spare for
upstream drains. The SDK does not enforce this at startup; runners
running deep pipelines should size the pool accordingly.

Single-stage pipelines (entry → one Operation, no fan-out) are unaffected:
the wire reader is the only producer, and its park does not consume a
worker-pool thread.

## Inter-operation zero-copy

`ctx.emit::<C>(value)` on an inter-operation channel does:

```
    Arc::new(value)  →  Arc::clone() per consumer segment
                          → push onto mailbox
                            → drain → Arc::unwrap_or_clone → Operation::run(T)
```

For single-consumer case the Arc is uniquely held by the time the
drain pops it, so `unwrap_or_clone` returns the inner `T` without a
clone — true zero-copy. For fan-out, each consumer pays one value
clone (the cost of multi-consumer parallelism).

## Plugin-bound emit

`ctx.emit::<C>(value)` on a plugin-bound channel does:

1. Serialize `value` to bytes via the channel's `plugin_serializer`
   closure (currently `T: AsRef<[u8]>` → `to_vec()`; future versions
   will accept `prost::Message`).
2. Build a payload frame with `payloader::build_from_binary`, attaching
   a `channel: <name>` metadata entry.
3. Write the frame on the segment's wire stream (the secondary
   FlezhaStream for the connection that the originating wire input
   arrived on).

The controller's plugin layer (e.g.
`Flezha.Plugin.Provider.Kafka.Producer`) matches on
`metadata["channel"]` to gate which channel's outputs it consumes.

`ctx.emit_with_meta::<C>(value, map)` is the same with user-attached
metadata appended after the reserved `channel` key. It returns
`OperationError::MetadataNotAllowedOnInterOperationChannel` if called on an
inter-operation channel that does not also have a plugin sink (see the
hybrid-channel section below).

## Hybrid channels: inter-operation fan-out + plugin sink

A hybrid channel publishes a single typed value to both inter-operation
consumers (typed, zero-copy) and a named plugin (serialized to bytes)
without forcing the producer Operation to declare two output channels. The
builder method is `also_output_with(plugin_name, serializer)` on the
producer's output stream handle (`Stream<T>`); `also_output(plugin_name)`
is the same with the default serialization rules of `output(...)`:

```rust
sma_stream                       // SmaOp's output handle, from `.into::<SmaOp>()`
    .named("sma_out")
    .also_output_with("indicators", sma_to_pb_bytes)
    .into::<BollingerOp>();
```

Java equivalent (same shape via the `alsoOutput` overloads on
`Stream<T>`):

```java
smaStream                        // SmaOp's output handle, from `.into(SmaOp.class)`
    .named("sma_out")
    .alsoOutput("indicators", SmaCodec::toBytes)
    .into(BollingerOp.class);
```

The Java surface mirrors Rust: `kind` stays `INTER_OPERATION`, the channel
descriptor carries `Optional<PluginSink> pluginSink`, duplicates raise
`UnboundedOperativeConfigurationException.duplicatePluginSink(channel)` at
registration time, and a serializer that throws
surfaces as `OperationException.PluginSerializerFailure` (Java's structured-throw
analog of Rust's `OperationError::PluginSerializerPanic`). Wire frames produced
by either SDK are byte-indistinguishable at the agent's plugin layer.

The channel's `kind` stays `InterOperation`. The plugin sink lives on a
parallel `plugin_sink: Option<PluginSink>` field. Order of
`also_output_with` and `.into::<S>()` is irrelevant — the
descriptor accumulates both regardless. Calling `also_output_with` (or
`also_output`) twice on the same channel is rejected at registration with
`TopologyIssue::DuplicatePluginSink`. The type system already
forbids `also_output_with` after `output` (the latter returns a
finalized `Channel<T>`); a trybuild test pins this.

### Per-emit semantics

`ctx.emit::<T>(value)` on a hybrid channel:

1. Fan out the typed `Arc<T>` value to every consumer mailbox, per the
   channel's `OverflowPolicy`. This step completes (modulo
   `OverflowPolicy::Block` parking) before step 2.
2. If `plugin_sink` is `Some(sink)`, invoke `sink.serializer(&value)`
   exactly once inside `std::panic::catch_unwind`, then send the
   resulting bytes through the existing plugin-wire framing path
   (`channel = <channel name>` in the metadata, identical to a
   pure-`PluginBound` frame).

The serializer runs **once per emit** regardless of consumer count.
Consumer fan-out completes **before** the serializer runs, so a
panicking serializer never prevents inter-operation delivery — the
`OperationError::PluginSerializerPanic { channel, message }` returned to
the producing Operation is purely a plugin-side signal. The dispatcher
thread stays alive across the panic; subsequent emits proceed
normally.

`ctx.emit_with_meta::<T>(value, meta)` on a hybrid channel attaches
`meta` to the plugin-side wire frame only — inter-operation consumers
receive the typed value with no metadata visible. The reserved
`channel` key is always replaced with the real channel name; any
user-supplied `channel` entry is dropped.

### Wire format and agent fan-in

Hybrid frames are byte-indistinguishable from `output(...)` frames
for the same plugin name and bytes. Two channels sharing a
`plugin_name` (typically `also_output_with("indicators", ...)`
across several Operations) fan in cleanly at the agent's plugin layer; the
plugin distinguishes them by the `channel: String` metadata key. No
new wire payload-id, no new agent code.

## Per-Segment state

Each SegmentSlot owns a `(Operation, S::State)` pair behind a `Mutex`
that the inflight CAS keeps uncontended in practice:

- The Operation's `&mut self` is a per-segment scratch buffer (e.g. a
  reusable `Vec<u8>` for serialization).
- `S::State` is the user's domain state. `init_state(operation_key)` is
  called once per segment; subsequent runs mutate in place.
- State migration (`MigrationSerialization` /
  `SegmentState`) walks the segments per-Operation and produces a
  per-Operation / per-Segment JSON shape (see
  `docs/sdk_protocol_spec.md` § Metadata Serialization → State
  migration JSON shape). v1 ships the wiring; the
  `operative-state-migration` change extends the format with
  segment-aware roundtrip.

## What's NOT in v1

The umbrella enumerates non-goals (`operative-architecture/proposal.md`
§Non-Goals); v1 inherits them:

- **Bounded mailboxes / cross-operation backpressure** — `crossbeam::SegQueue`
  is unbounded. Tracked in `operative-bounded-mailbox`.
- **Dynamic segmentation** — `Operation::operation_key` is reserved; v1 rejects
  Operatives that override it without `operation_keys()`. Tracked in
  `operative-dynamic-segmenting`.
- **Per-Operation `handle_message`** — the wire command exists, but the SDK
  side returns the canonical "unsupported" error until
  `operative-message-handlers` ships. Tests that exercise the round-trip
  are tagged `OPERATIVE_MESSAGE_HANDLERS_PENDING`.
- **Java SDK** — the Java SDK is not yet ported to the new model;
  `operative-mvp-java` does that work. Java↔controller integration tests are
  tagged `OPERATIVE_MVP_JAVA_PENDING`.
- **flezha_market migration** — downstream runner app migrates in its
  own repo (umbrella Decision 17).

## Where to look in the code

| Concern | File |
|---|---|
| Unbounded Operative trait | `sdk/rust/src/operative/mod.rs` |
| Operation trait + OperationCtx | `sdk/rust/src/operation/mod.rs`, `ctx.rs`, `error.rs` |
| Channel<T> + ChannelBuilder | `sdk/rust/src/channel/mod.rs`, `builder.rs` |
| Topology builder | `sdk/rust/src/operative/builder.rs` |
| OperativeInstance + segment layout | `sdk/rust/src/operative/instance.rs` |
| Per-Operation typed dispatcher | `sdk/rust/src/operative/dispatcher.rs` |
| SegmentSlot + mailbox | `sdk/rust/src/segment/mod.rs`, `runner/mailbox.rs` |
| Class + instance registries | `sdk/rust/src/operative/registry.rs` |
| Wire dispatch | `sdk/rust/src/runner/fast.rs::OperativeDispatchHandle` |
| Wire input routing | `sdk/rust/src/command/streams_connect.rs` |
| Plugin emit framing | `sdk/rust/src/operative/dispatcher.rs::send_plugin_frame` |
| State migration JSON shape | `sdk/rust/src/command/segment_state.rs` |

## State persistence

Per-Segment state is persisted via the `Change`-trait machinery added
by `operative-state-persistence-rust` and wired end-to-end by
`operative-state-persistence-controller`. The hot path is a single
`Vec::push(change)` per `state.apply(...)` call; serialization,
coalescing, and the wire send happen on a per-process flusher thread
on a 5 ms linger cadence (configurable via `FLEZHA_FLUSH_LINGER_MS`).

Operations opt out by inheriting `type Change = NoChange` (the trait's
uninhabited default). Runner-controlled operative-instance override:
`operative_persistence.<OperativeClass>` in operative-start metadata.

The umbrella commits to eight Tier-1 properties — wire format, header
layout, `Change` trait shape, deterministic `apply_change`, replay
equality via `OperationTestHarness`, single-thread flusher, frozen-at-start
toggle, postcard format. The Rust SDK implements all eight; the controller
side ships durable WAL, fsync batcher, and snapshot lifecycle in the
follow-up change.

See `docs/operative_state_persistence.md` for the full Rust trait surface and
worked examples. The Java SDK port lives at
`docs/java_state_persistence.md` (six-parameter `Operation` generic,
`StateHandle<State, Change>` chokepoint, Kryo-backed serialization with
stable registered type IDs; same wire format, `sdk_lang = 2`,
`format_id = 4`).

## Mapping back to the umbrella

Decisions referenced above:

- **Decision 1 (umbrella)** — Unbounded Operative/Operation trait surface, sync `run`,
  associated types over generics.
- **Decision 2 (mvp-rust)** — Fluent channel builder with compile-time
  wiring constraints.
- **Decision 3 (mvp-rust)** — Per-Operative channel-id table layout
  (interned `u32` IDs frozen at `define_unbounded`).
- **Decision 4 (mvp-rust)** — Cached `output_channel_id` on the
  dispatcher; zero name lookups during emit.
- **Decision 7 (umbrella)** — Single-mode is the default; static-mode
  declared via `operation_keys()` + `segment_for` + the
  `SEGMENT_FOR_OVERRIDDEN` const sentinel.
- **Decision 10 (umbrella)** — Inter-operation `Arc<T>` zero-copy;
  plugin-bound serializes at the producer edge.
- **Decision 12 (umbrella)** — Entry channels accepted only `Vec<u8>` /
  `String` in v1 (the sealed `EntryChannelType`). Superseded by
  `sdk-graph-authoring`: `input::<T>` accepts any type, JSON-decoded at
  the entry edge (`input_with` for custom parsers), with a failing parse
  riding the channel's retry→DLQ ladder.
- **Decision 15 (umbrella)** — Channel name on the wire as a UTF-8
  string; integer fast-path reserved for a future change.

## Controller-side Overwatch lifecycle (cold-restart, retry, two-phase plugins)

Each Operative has a controller-side `Flezha.Operative.Overwatch` GenServer
owning startup, plugin lifecycle, ingress, and teardown. Its boot
sequence:

1. **`init/1`** returns quickly with `{:ok, state, {:continue, …}}` so
   `dist_sup`'s synchronous DETS replay unblocks (FleetManager /
   HubManager are not yet answering calls at this point).
2. **`handle_continue({:bind_runner, …}, _)`** waits for an active
   runner advertising the operative's `factory_id` via
   `OperativeManager.assign_runner/2`. The persisted `runner_id` is
   advisory only — see the `operative-state-persistence` capability.
3. **`complete_init/3`** runs three phases:
   - **prepare** — `prepare_initial_plugins/2` calls each provider's
     `init_provider/1`. Providers register their GenServer and validate
     config; they MUST NOT open upstream connections, issue subscribe
     handshakes, or otherwise depend on the SDK being bound. Plugins
     enter the `:prepared` phase in `Flezha.Plugin.Extension.Table`.
   - **resolve init values** — the `:on_operative_init` fold chain runs
     unless inline `init_values` are non-empty.
   - **start SDK with retry** — `start_operative_sdk_with_retry/3` repeats
     `ping → start_operative_sdk` under a bounded retry governor. The
     default schedule is `200ms, 500ms, 1s, 2s, 5s, 10s` with full
     jitter (~19 s aggregate). On every retry the live runner is
     re-resolved so a fresh runner that came online during a backoff
     is picked up; the channel handle is re-attached to the final
     runner on success.
4. **activate** — on `{:ok, _}` from `start_operative_sdk`, the Overwatch
   calls `Table.bulk_activate/1` which transitions each plugin to
   `:running`. Providers implementing the optional `activate/1`
   callback run it now (`Plugin.Provider.Websocket` opens its TLS
   connection and issues the subscribe handshake here, NOT during
   `init_provider/1`).

### End-to-end ping before `:start_operative`

Each retry attempt issues a `Ping` control-channel round-trip via
`Flezha.Runner.ControlUnit.ping_runner/2` before `AddOperative`. The
ping traverses the same `ControlChannel → SDK → ControlChannel` path
`AddOperative` uses, so a successful round-trip proves the channel is
bound and the SDK reply path is intact. On timeout the Overwatch
invokes `Flezha.Runner.FleetManager.reset_control_channel/1` to
clear the `ControlChannel`'s `stream_ref` — the SDK's next data
stream then claims it via the existing rebind path in
`runner-controller-connection-healing`.

The ping is gated by `:overwatch_ping_enabled` (default `true`); SDKs
that haven't picked up the `Ping` handler can be accommodated by
setting it `false` during a rolling deploy.

### Parked-error state

When the bounded-retry budget exhausts, the Overwatch parks rather
than crashing the supervisor:

- `state.init_error` holds the last failure reason
  (e.g. `{:ping_failed, :timeout}`, `{:start_operative_failed, _}`).
- `:registry_runner_set` replies `{:error, reason}` so callers learn
  the operative is degraded.
- `:io_in` calls reply `{:error, :startup_failed}`.
- `:io_in` casts are silently dropped.
- `:ingest` calls reply `{:error, :not_ready}`, so a forwarding sink
  **retains and retries** rather than losing the payload (see
  "Ingest entry points" below).
- A periodic `:retry_startup` timer (default 60 s) re-attempts the
  full cold-start sequence; on success `init_error` clears and the
  timer is not re-armed.

Plugins remain in `:prepared` for the entire parked duration:
providers never open upstream connections, no `:io_in` casts arrive
from plugins, and the controller's process memory holds steady at the
idle baseline. This is the structural fix for the memory cascade
observed in the 2026-05-15 live incident.

### Ingest entry points

Every inbound payload runs exactly one pre-delivery path — the `:on_input`
extension chain, then `journal_input/2` (the durable input log for the
effectively-once tier, the durable inbox for a durable bounded instance). Three
entry points drive it, differing only in what they report back:

| Entry point | Caller | Reports |
|---|---|---|
| `cast {:io_in, msg, md}` | plugins, source adapters, `OperativeManager.send_io/4` | nothing (fire-and-forget) |
| `call {:io_in, msg, md}` | source adapters in blocking-ingest mode (e.g. the WebSocket provider) | `:ok`, or `{:error, :startup_failed}` when parked |
| `call {:ingest, msg, md}` | `OperativeManager.send_io_confirming/5` — the operative-forward plugin | a typed verdict |

The blocking `:io_in` call exists because the effectively-once tier parks
ingestion at zero source credit, which only back-pressures a source that awaits
the reply. It reports coarsely: adapters act on "accepted" vs "parked" and
nothing finer.

The confirming `:ingest` call reports what actually happened, because its caller
is a retry loop that must distinguish "done" from "try again":

| Verdict | Meaning |
|---|---|
| `:ok` | forwarded to the bound runner, or durably journaled |
| `{:ok, :absorbed}` | filtered by an `:on_input` plugin, or absorbed as a duplicate |
| `{:ok, :suspended}` | durable in the inbox; delivered when the instance rehydrates |
| `{:error, :not_ready}` | parked with an `init_error`, or unbound with no durable log |
| `{:error, :refused}` | the durable log rejected the append |

`{:error, :not_found}` (no pid for `(namespace, operative_id)`) is minted by
`OperativeManager`, not by the Overwatch.

Two subtleties worth knowing:

- **"Filtered" and "duplicate" are successes.** Both mean the receiver decided
  the payload needs no further delivery. Retrying either is wasted work at best,
  and an infinite loop at worst — a deterministic filter filters the retry too.
- **An unbound stream is not uniformly an error.** For a target with a durable
  log the payload is already journaled and the bind will deliver it, so the
  verdict is `:ok`. Reporting `not_ready` there would make the sender retry a
  payload the receiver already holds — which for an effectively-once target
  appends at a *new* offset, manufacturing a duplicate rather than absorbing one.

See [`operative_forward_plugin.md`](operative_forward_plugin.md) for the plugin
that consumes these verdicts.

For investigating sustained BEAM memory growth under steady load, see
[`docs/operator/memory-drift.md`](operator/memory-drift.md) — the
sampler + `flz controller mem-top` surface and the load harness
operators use to localize and verify memory-leak fixes.

### Runtime config

| Key | Default | Purpose |
| --- | --- | --- |
| `:overwatch_retry_backoff_ms` | `[200, 500, 1_000, 2_000, 5_000, 10_000]` | Per-attempt delay schedule with full jitter |
| `:overwatch_retry_park_interval_ms` | `60_000` | Periodic re-attempt while parked |
| `:overwatch_ping_timeout_ms` | `2_000` | Per-ping deadline |
| `:overwatch_ping_enabled` | `true` | Master switch for the ping pre-check |
