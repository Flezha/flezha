# Rust SDK Relay Architecture

The Rust SDK exposes a small set of traits for declaring how compute
work is structured inside one logical executor instance. This doc
walks the concepts (Relay, Shard, Partition, Runner), the data flow
end-to-end, and the public API. For the rationale, design decisions,
and capability spec deltas, see
`openspec/changes/relay-architecture/` (umbrella) and
`openspec/changes/relay-mvp-rust/` (v1 cutover).

## Concepts at a glance

```
                        Wire (UDS / QUIC)
                                │
                                ▼
                   ┌────────────────────────┐
                   │   RelayDispatchHandle  │   metadata["channel"] →
                   │   (per-process)        │   topology lookup
                   └────────────────────────┘
                                │
                                ▼
            ┌─────────────────────────────────────────┐
            │              RelayInstance              │
            │   (one per `flz relay start`)       │
            │                                         │
            │   ┌───────────┐    ┌──────────────┐     │
            │   │ Shard A   │ -> │ Shard B      │ -> ... -> Plugin
            │   │ (partn 0) │    │ (partn 1m)   │     │
            │   │ (partn 1) │    │ (partn 1h)   │     │
            │   └───────────┘    └──────────────┘     │
            └─────────────────────────────────────────┘
```

- **Relay class** — the type-level declaration. One Relay class declares
  its channels and the Shards that produce/consume them, exactly once
  per process via `register_relay::<R>()`. The topology is frozen at
  registration and shared (`Arc`) by every instance of that class.

- **Relay instance** — the live runtime object spawned on `AddRelay`.
  Each instance has its own per-Partition state (per-Shard) and its
  own mailboxes; instances are independent of each other and routed by
  `relay_id` (the wire calls it that; the SDK calls it
  `instance_id`).

- **Shard** — the unit of compute. Has typed `Input` / `Output` / `State`
  associated types, a sync `run` method, and a partitioning declaration.
  One Shard inside a Relay can have one or more **Partitions**.

- **Partition** — a "lane" of a Shard. Single-mode (default) means one
  partition per Shard; static mode declares fixed `shard_keys()` and
  routes inputs to one or more named partitions via `partition_for`.
  At most one drain task runs per partition at any time (sticky
  ordering).

- **Channel** — the typed wire between producers/consumers. Three kinds:
  `Entry` (controller → SDK, restricted to `Vec<u8>` / `String`),
  `InterShard` (`Arc<T>` zero-copy between Shards), and `PluginBound`
  (typed value serialized to bytes; sent to a named plugin via the
  controller).

- **Runner** — the work-stealing pool that drains mailboxes. One pool
  per `Flezha::new()`; size controlled by `FLEZHA_WORKER_POOL_SIZE`.

## Public API

The full surface is small:

```rust
use flezha_executor::{
    register_relay, ChannelBuilder, Relay, Shard, ShardCtx, ShardError,
};
```

`register_relay::<R: Relay>()` is called once per Relay class at
process startup. `Flezha::new(namespace)` boots the SDK; the controller
later spawns instances by calling `flz relay start <factory_id>`.

## Worked example

```rust
use flezha_executor::{
    register_relay, ChannelBuilder, Relay, Shard, ShardCtx, ShardError,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default)]
struct OhlcState {
    candles: Vec<f64>,
}

#[derive(Default)]
struct ParseShard;
impl Shard for ParseShard {
    type Input = String;
    type Output = f64;
    type State = ();

    fn name() -> &'static str { "parse_shard" }
    fn init_state(_: Option<&str>) {}

    fn run(&mut self, input: String, _: &mut (), ctx: &mut ShardCtx<'_>)
        -> Result<(), ShardError>
    {
        let price: f64 = input.parse().map_err(|e: std::num::ParseFloatError| {
            ShardError::Custom(e.to_string())
        })?;
        ctx.emit::<f64>(price)
    }
}

#[derive(Default)]
struct OhlcShard;
impl Shard for OhlcShard {
    type Input = f64;
    type Output = Vec<u8>;
    type State = OhlcState;

    fn name() -> &'static str { "ohlc_shard" }
    fn init_state(_: Option<&str>) -> OhlcState {
        OhlcState::default()
    }

    fn shard_keys() -> Vec<&'static str> {
        vec!["1m", "1h", "1d", "1w"]
    }
    const PARTITION_FOR_OVERRIDDEN: bool = true;
    fn partition_for(_: &f64) -> Vec<&'static str> {
        vec!["1m", "1h", "1d", "1w"]
    }

    fn run(&mut self, price: f64, state: &mut OhlcState, ctx: &mut ShardCtx<'_>)
        -> Result<(), ShardError>
    {
        state.candles.push(price);
        ctx.emit::<Vec<u8>>(format!("{}", price).into_bytes())
    }
}

struct MarketRelay;
impl Relay for MarketRelay {
    fn factory_id() -> &'static str { "market" }

    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("trade.raw").into::<ParseShard>();
        c.channel::<f64>("trade.parsed")
            .from::<ParseShard>()
            .into::<OhlcShard>();
        c.channel::<Vec<u8>>("ohlc.candles")
            .from::<OhlcShard>()
            .to_plugin("ohlc_kafka");
    }
}

fn main() {
    register_relay::<MarketRelay>();
    let _ = flezha_executor::new("market_ns".to_string());
    // The controller's `flz relay start <market>` spawns an instance.
}
```

### Java SDK equivalent

The Java SDK is a port of the same model. Idiomatic differences:

- `Shard<Input, State, Output>` is a parameterized interface (no associated
  types in Java); implementers expose `Class<Input>` / `Class<Output>`
  accessors so the runtime can verify wiring at registration time
  (Java's type erasure means most type bounds the Rust trait system
  enforces at compile time become runtime-checked
  `RelayConfigurationException`s thrown by `Flezha.registerRelay(...)`).
- `partitionForOverridden()` / `shardKeyOverridden()` boolean default
  methods replace Rust's `const PARTITION_FOR_OVERRIDDEN: bool` /
  `const SHARD_KEY_OVERRIDDEN: bool` sentinels.
- `ShardException` is a checked exception subclass of `Exception`;
  `Shard.run` declares `throws ShardException`.
- Inter-shard "zero-copy" passes the value by reference (Java has no
  `Arc<T>`); the user MUST treat emitted values as immutable from the
  producer side after `ctx.emit` returns. The
  `Inter-shard zero-copy preserves value identity` capability scenario
  asserts `==` reference-equality on the value received downstream.

```java
public record OhlcState(java.util.List<Double> candles) {}

public final class ParseShard implements Shard<String, Object, Double> {
    @Override public String name() { return "parse_shard"; }
    @Override public Class<String> inputType() { return String.class; }
    @Override public Class<Double> outputType() { return Double.class; }
    @Override public Object initState(Optional<String> shardKey) { return new Object(); }

    @Override public void run(String input, Object state, ShardCtx ctx) throws ShardException {
        try {
            ctx.emit(Double.parseDouble(input));
        } catch (NumberFormatException e) {
            throw new ShardException("parse failed: " + input, e);
        }
    }
}

public final class OhlcShard implements Shard<Double, OhlcState, byte[]> {
    @Override public String name() { return "ohlc_shard"; }
    @Override public Class<Double> inputType() { return Double.class; }
    @Override public Class<byte[]> outputType() { return byte[].class; }
    @Override public OhlcState initState(Optional<String> shardKey) {
        return new OhlcState(new java.util.ArrayList<>());
    }

    @Override public java.util.List<String> shardKeys() { return java.util.List.of("1m", "1h", "1d", "1w"); }
    @Override public boolean partitionForOverridden() { return true; }
    @Override public java.util.List<String> partitionFor(Double price) { return shardKeys(); }

    @Override public void run(Double price, OhlcState state, ShardCtx ctx) throws ShardException {
        state.candles().add(price);
        ctx.emit(price.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}

public final class MarketRelay implements Relay {
    @Override public String factoryId() { return "market"; }
    @Override public void channels(ChannelBuilder c) {
        c.input(String.class, "trade.raw").into(ParseShard.class);
        c.channel(Double.class, "trade.parsed")
                .from(ParseShard.class)
                .into(OhlcShard.class);
        c.channel(byte[].class, "ohlc.candles")
                .from(OhlcShard.class)
                .toPlugin("ohlc_kafka");
    }
}

public final class App {
    public static void main(String[] args) {
        Flezha.registerRelay(MarketRelay.class);
        Flezha unused = Flezha.newInstance("market_ns");
        // Controller's `flz relay start <market>` spawns an instance.
    }
}
```

## Hot-path lookups

The dispatch path is engineered to keep the per-message cost low:

1. **Wire input → entry channel id** — one `HashMap<&'static str, u32>`
   read on the `metadata["channel"]` key, then a bounds-elided `Vec`
   index into `topology.channels[id]`.
2. **Entry channel → consumer partition** — for each consumer Shard:
   - Single-mode: jump straight to partition index 0.
   - Static-mode: one HashMap lookup per partition key returned by
     `S::partition_for(&input)`.
3. **Mailbox push + counter + inflight CAS** — one `ArrayQueue::try_push`,
   one Relaxed `pushes_total` increment, and one atomic compare-exchange.
   If the CAS wins, submit a drain task to the pool; otherwise an
   in-progress drain will pick up the new item via the missed-wake
   recovery loop. On a full mailbox the channel's declared
   `OverflowPolicy` (Block / DropNewest / DropOldest) decides what
   happens — see "Backpressure and bounded mailboxes" below.
4. **Drain → Shard::run** — the dispatcher downcasts `Arc<dyn Any>` to
   `Arc<S::Input>`, falls back to a value-clone (`Arc::unwrap_or_clone`)
   if the Arc was shared by fan-out, and calls `Shard::run` with the
   typed signature.

The umbrella's "Per-Message Dispatch-Overhead Budget" requirement caps
this at **4 sync ops per message** on the single-partition fast path
(registry lookup + mailbox enqueue + success counter + inflight CAS);
static-mode fan-out adds `N × 3` ops where N is the partition fan-out
width (try_push + counter + inflight CAS per target). The added counter
op compared with the pre-`relay-bounded-mailbox` baseline is detailed
in `openspec/changes/relay-bounded-mailbox/design.md` Decision 7.

For the recorded throughput floor and how it's measured, see
`docs/benchmarking.md` — it documents the regression / partitioning /
single-partition / static-fanout bench shapes, the recorded
`BENCH_FLOOR_MSG_PER_SEC` and `SINGLE_PARTITION_FLOOR_MSG_PER_SEC`
values, the floor-update protocol, and CI triage instructions when the
`bench-regression` gate fires.

## Backpressure and bounded mailboxes

Each Partition's mailbox is a fixed-size lock-free
`crossbeam::queue::ArrayQueue` allocated at relay startup. Capacity is
declared per Shard; the policy applied when the mailbox is full is
declared per channel. Counters on the consumer-side `PartitionSlot`
record every push, drop, and block event for future metrics export.

### Capacity declaration (consumer side)

```rust
impl Shard for OhlcShard {
    fn mailbox_capacity() -> usize { 4096 }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    // ... other items ...
}
```

Resolution order at `register_relay::<R>()` time:

1. `Shard::mailbox_capacity()` if `MAILBOX_CAPACITY_OVERRIDDEN = true`.
2. `FLEZHA_MAILBOX_CAPACITY` env var (parsed as `usize`).
3. Built-in default (1024).

Capacity must be in `1..=1_048_576`; out-of-range values fail
registration with the offending Shard's name. Static-mode Shards
allocate one mailbox per declared partition key, all sharing the
same capacity.

### Policy declaration (producer / wiring side)

```rust
c.input::<Vec<u8>>("trade.raw")
    .on_full(OverflowPolicy::DropNewest)
    .into::<ParseShard>();

c.channel::<Candle>("ohlc.candles")
    .from::<OhlcShard>()
    .on_full(OverflowPolicy::Block)
    .into::<IndicatorShard>();
```

`OverflowPolicy` is one of:

- **`Block`** — the producer parks on the consumer's `BlockParker` until
  the drain pops an item, then retries the push. Preserves ordering at
  the cost of producer-thread stall under sustained pressure.
- **`DropNewest`** — incoming item discarded; consumer's
  `drops_newest_total` counter increments by 1.
- **`DropOldest`** — consumer's mailbox atomically pops its oldest item
  and accepts the new one (`ArrayQueue::force_push`); consumer's
  `drops_oldest_total` counter increments by 1. Best-effort under MPMC
  contention.

Plugin-bound channels (`.to_plugin(...)`) reject `.on_full(...)` at
compile time — there is no SDK-side mailbox between a plugin-bound emit
and the wire, so the policy enum has nothing to act on.

### Default policy by channel kind

| Channel kind | Default | Why |
|---|---|---|
| Entry        | `DropNewest` | The wire reader services every Relay sharing one physical connection; blocking on one Relay's saturation head-of-line-blocks siblings. |
| InterShard   | `Block`      | Stateful pipelines need ordering preservation; mid-pipeline drops silently corrupt derived state. |
| PluginBound  | (n/a)        | No SDK-side mailbox sits between the producer Shard and the wire; backpressure is OS socket buffer + transport flow control. |

An executor who wants strict end-to-end ordering at the entry edge can
declare `.on_full(OverflowPolicy::Block)` on their entry channel — they
opt into the documented co-tenancy hazard (the wire reader parks while
this one Relay's mailbox is full, so sibling Relays sharing the same
physical connection stop receiving frames until it drains). The default
`DropNewest` keeps the reader running and records every drop on the
per-Partition `drops_newest_total` counter.

### Per-Partition counter set

Every `PartitionSlot.mailbox` carries a `PartitionCounters` block with
four `AtomicU64` counters, all bumped under `Ordering::Relaxed`:

| Counter | Bumped on |
|---|---|
| `pushes_total`         | every successful `try_push` |
| `pushes_blocked_total` | every `Block`-policy park event (one per `wait_one`) |
| `drops_newest_total`   | every drop under `DropNewest` |
| `drops_oldest_total`   | every eviction under `DropOldest` |

Counters are read-only from outside the dispatch path (`partition.counters()`
or `partition.mailbox.counters()`); export to a metrics backend (Prometheus,
StatsD, log emit) is **out of scope** for `relay-bounded-mailbox` and
deferred to a separate observability change. Storing the data here keeps
that change purely additive.

### Worker-pool sizing under `Block`

A Block-policy producer parks the worker thread it ran on. A pipeline
of depth D under `Block` requires the work-stealing pool to have
`FLEZHA_WORKER_POOL_SIZE ≥ 2 × D + 1` to avoid a deadlock: D parked
producers + 1 thread free to drain the deepest consumer + spare for
upstream drains. The SDK does not enforce this at startup; executors
running deep pipelines should size the pool accordingly.

Single-stage pipelines (entry → one Shard, no fan-out) are unaffected:
the wire reader is the only producer, and its park does not consume a
worker-pool thread.

## Inter-shard zero-copy

`ctx.emit::<C>(value)` on an inter-shard channel does:

```
    Arc::new(value)  →  Arc::clone() per consumer partition
                          → push onto mailbox
                            → drain → Arc::unwrap_or_clone → Shard::run(T)
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
3. Write the frame on the partition's wire stream (the secondary
   FlezhaStream for the connection that the originating wire input
   arrived on).

The controller's plugin layer (e.g.
`Flezha.Plugin.Provider.Kafka.Producer`) matches on
`metadata["channel"]` to gate which channel's outputs it consumes.

`ctx.emit_with_meta::<C>(value, map)` is the same with user-attached
metadata appended after the reserved `channel` key. It returns
`ShardError::MetadataNotAllowedOnInterShardChannel` if called on an
inter-shard channel that does not also have a plugin sink (see the
hybrid-channel section below).

## Hybrid channels: inter-shard fan-out + plugin sink

A hybrid channel publishes a single typed value to both inter-shard
consumers (typed, zero-copy) and a named plugin (serialized to bytes)
without forcing the producer Shard to declare two output channels. The
builder method is `also_to_plugin_with(plugin_name, serializer)` on
`ChannelDeclWithProducer<T>`:

```rust
c.channel::<SmaValue>("sma_out")
    .from::<SmaShard>()
    .also_to_plugin_with("indicators", sma_to_pb_bytes)
    .into::<BollingerShard>();
```

Java equivalent (same shape via the `alsoToPluginWith` method on
`ChannelDeclWithProducer<T>`):

```java
c.channel(SmaValue.class, "sma_out")
    .from(SmaShard.class)
    .alsoToPluginWith("indicators", SmaCodec::toBytes)
    .into(BollingerShard.class);
```

The Java surface mirrors Rust: `kind` stays `INTER_SHARD`, the channel
descriptor carries `Optional<PluginSink> pluginSink`, duplicates raise
`RelayConfigurationException.duplicatePluginSink(channel)` at
`RelayRegistry.registerRelay(...)` time, and a serializer that throws
surfaces as `ShardException.PluginSerializerFailure` (Java's structured-throw
analog of Rust's `ShardError::PluginSerializerPanic`). Wire frames produced
by either SDK are byte-indistinguishable at the agent's plugin layer.

The channel's `kind` stays `InterShard`. The plugin sink lives on a
parallel `plugin_sink: Option<PluginSink>` field. Order of
`also_to_plugin_with` and `.into::<S>()` is irrelevant — the
descriptor accumulates both regardless. Calling `also_to_plugin_with`
twice on the same channel is rejected at registration with
`BuildTopologyError::DuplicatePluginSink`. The type system already
forbids `also_to_plugin_with` after `to_plugin` (the latter returns a
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
panicking serializer never prevents inter-shard delivery — the
`ShardError::PluginSerializerPanic { channel, message }` returned to
the producing Shard is purely a plugin-side signal. The dispatcher
thread stays alive across the panic; subsequent emits proceed
normally.

`ctx.emit_with_meta::<T>(value, meta)` on a hybrid channel attaches
`meta` to the plugin-side wire frame only — inter-shard consumers
receive the typed value with no metadata visible. The reserved
`channel` key is always replaced with the real channel name; any
user-supplied `channel` entry is dropped.

### Wire format and agent fan-in

Hybrid frames are byte-indistinguishable from `to_plugin(...)` frames
for the same plugin name and bytes. Two channels sharing a
`plugin_name` (typically `also_to_plugin_with("indicators", ...)`
across several Shards) fan in cleanly at the agent's plugin layer; the
plugin distinguishes them by the `channel: String` metadata key. No
new wire payload-id, no new agent code.

## Per-Partition state

Each PartitionSlot owns a `(Shard, S::State)` pair behind a `Mutex`
that the inflight CAS keeps uncontended in practice:

- The Shard's `&mut self` is a per-partition scratch buffer (e.g. a
  reusable `Vec<u8>` for serialization).
- `S::State` is the user's domain state. `init_state(shard_key)` is
  called once per partition; subsequent runs mutate in place.
- State migration (`MigrationSerialization` /
  `PartitionState`) walks the partitions per-Shard and produces a
  per-Shard / per-Partition JSON shape (see
  `docs/sdk_protocol_spec.md` § Metadata Serialization → State
  migration JSON shape). v1 ships the wiring; the
  `relay-state-migration` change extends the format with
  partition-aware roundtrip.

## What's NOT in v1

The umbrella enumerates non-goals (`relay-architecture/proposal.md`
§Non-Goals); v1 inherits them:

- **Bounded mailboxes / cross-shard backpressure** — `crossbeam::SegQueue`
  is unbounded. Tracked in `relay-bounded-mailbox`.
- **Dynamic partitioning** — `Shard::shard_key` is reserved; v1 rejects
  Relays that override it without `shard_keys()`. Tracked in
  `relay-dynamic-partitioning`.
- **Per-Shard `handle_message`** — the wire command exists, but the SDK
  side returns the canonical "unsupported" error until
  `relay-message-handlers` ships. Tests that exercise the round-trip
  are tagged `RELAY_MESSAGE_HANDLERS_PENDING`.
- **Java SDK** — the Java SDK is not yet ported to the new model;
  `relay-mvp-java` does that work. Java↔controller integration tests are
  tagged `RELAY_MVP_JAVA_PENDING`.
- **flezha_market migration** — downstream executor app migrates in its
  own repo (umbrella Decision 17).

## Where to look in the code

| Concern | File |
|---|---|
| Relay trait | `sdk/rust/src/relay/mod.rs` |
| Shard trait + ShardCtx | `sdk/rust/src/shard/mod.rs`, `ctx.rs`, `error.rs` |
| Channel<T> + ChannelBuilder | `sdk/rust/src/channel/mod.rs`, `builder.rs` |
| Topology builder | `sdk/rust/src/relay/builder.rs` |
| RelayInstance + partition layout | `sdk/rust/src/relay/instance.rs` |
| Per-Shard typed dispatcher | `sdk/rust/src/relay/dispatcher.rs` |
| PartitionSlot + mailbox | `sdk/rust/src/partition/mod.rs`, `executor/mailbox.rs` |
| Class + instance registries | `sdk/rust/src/relay/registry.rs` |
| Wire dispatch | `sdk/rust/src/executor/fast.rs::RelayDispatchHandle` |
| Wire input routing | `sdk/rust/src/command/streams_connect.rs` |
| Plugin emit framing | `sdk/rust/src/relay/dispatcher.rs::send_plugin_frame` |
| State migration JSON shape | `sdk/rust/src/command/partition_state.rs` |

## State persistence

Per-Partition state is persisted via the `Change`-trait machinery added
by `relay-state-persistence-rust` and wired end-to-end by
`relay-state-persistence-controller`. The hot path is a single
`Vec::push(change)` per `state.apply(...)` call; serialization,
coalescing, and the wire send happen on a per-process flusher thread
on a 5 ms linger cadence (configurable via `FLEZHA_FLUSH_LINGER_MS`).

Shards opt out by inheriting `type Change = NoChange` (the trait's
uninhabited default). Executor-controlled relay-instance override:
`relay_persistence.<RelayClass>` in relay-start metadata.

The umbrella commits to eight Tier-1 properties — wire format, header
layout, `Change` trait shape, deterministic `apply_change`, replay
equality via `ShardTestHarness`, single-thread flusher, frozen-at-start
toggle, postcard format. The Rust SDK implements all eight; the controller
side ships durable WAL, fsync batcher, and snapshot lifecycle in the
follow-up change.

See `docs/relay_state_persistence.md` for the full Rust trait surface and
worked examples. The Java SDK port lives at
`docs/java_state_persistence.md` (six-parameter `Shard` generic,
`StateHandle<State, Change>` chokepoint, Kryo-backed serialization with
stable registered type IDs; same wire format, `sdk_lang = 2`,
`format_id = 4`).

## Mapping back to the umbrella

Decisions referenced above:

- **Decision 1 (umbrella)** — Relay/Shard trait surface, sync `run`,
  associated types over generics.
- **Decision 2 (mvp-rust)** — Fluent channel builder with compile-time
  wiring constraints.
- **Decision 3 (mvp-rust)** — Per-Relay channel-id table layout
  (interned `u32` IDs frozen at `register_relay`).
- **Decision 4 (mvp-rust)** — Cached `output_channel_id` on the
  dispatcher; zero name lookups during emit.
- **Decision 7 (umbrella)** — Single-mode is the default; static-mode
  declared via `shard_keys()` + `partition_for` + the
  `PARTITION_FOR_OVERRIDDEN` const sentinel.
- **Decision 10 (umbrella)** — Inter-shard `Arc<T>` zero-copy;
  plugin-bound serializes at the producer edge.
- **Decision 12 (umbrella)** — Entry channels accept only `Vec<u8>` /
  `String` in v1 (sealed `EntryChannelType`).
- **Decision 15 (umbrella)** — Channel name on the wire as a UTF-8
  string; integer fast-path reserved for a future change.

## Controller-side Overwatch lifecycle (cold-restart, retry, two-phase plugins)

Each Relay has a controller-side `Flezha.Relay.Overwatch` GenServer
owning startup, plugin lifecycle, ingress, and teardown. Its boot
sequence:

1. **`init/1`** returns quickly with `{:ok, state, {:continue, …}}` so
   `dist_sup`'s synchronous DETS replay unblocks (FleetManager /
   HubManager are not yet answering calls at this point).
2. **`handle_continue({:bind_executor, …}, _)`** waits for an active
   executor advertising the relay's `factory_id` via
   `RelayManager.assign_executor/2`. The persisted `executor_id` is
   advisory only — see the `relay-state-persistence` capability.
3. **`complete_init/3`** runs three phases:
   - **prepare** — `prepare_initial_plugins/2` calls each provider's
     `init_provider/1`. Providers register their GenServer and validate
     config; they MUST NOT open upstream connections, issue subscribe
     handshakes, or otherwise depend on the SDK being bound. Plugins
     enter the `:prepared` phase in `Flezha.Plugin.Extension.Table`.
   - **resolve init values** — the `:on_relay_init` fold chain runs
     unless inline `init_values` are non-empty.
   - **start SDK with retry** — `start_relay_sdk_with_retry/3` repeats
     `ping → start_relay_sdk` under a bounded retry governor. The
     default schedule is `200ms, 500ms, 1s, 2s, 5s, 10s` with full
     jitter (~19 s aggregate). On every retry the live executor is
     re-resolved so a fresh executor that came online during a backoff
     is picked up; the channel handle is re-attached to the final
     executor on success.
4. **activate** — on `{:ok, _}` from `start_relay_sdk`, the Overwatch
   calls `Table.bulk_activate/1` which transitions each plugin to
   `:running`. Providers implementing the optional `activate/1`
   callback run it now (`Plugin.Provider.Websocket` opens its TLS
   connection and issues the subscribe handshake here, NOT during
   `init_provider/1`).

### End-to-end ping before `:start_relay`

Each retry attempt issues a `Ping` control-channel round-trip via
`Flezha.Executor.ControlUnit.ping_executor/2` before `AddRelay`. The
ping traverses the same `ControlChannel → SDK → ControlChannel` path
`AddRelay` uses, so a successful round-trip proves the channel is
bound and the SDK reply path is intact. On timeout the Overwatch
invokes `Flezha.Executor.FleetManager.reset_control_channel/1` to
clear the `ControlChannel`'s `stream_ref` — the SDK's next data
stream then claims it via the existing rebind path in
`executor-controller-connection-healing`.

The ping is gated by `:overwatch_ping_enabled` (default `true`); SDKs
that haven't picked up the `Ping` handler can be accommodated by
setting it `false` during a rolling deploy.

### Parked-error state

When the bounded-retry budget exhausts, the Overwatch parks rather
than crashing the supervisor:

- `state.init_error` holds the last failure reason
  (e.g. `{:ping_failed, :timeout}`, `{:start_relay_failed, _}`).
- `:registry_executor_set` replies `{:error, reason}` so callers learn
  the relay is degraded.
- `:io_in` calls reply `{:error, :startup_failed}`.
- `:io_in` casts are silently dropped.
- A periodic `:retry_startup` timer (default 60 s) re-attempts the
  full cold-start sequence; on success `init_error` clears and the
  timer is not re-armed.

Plugins remain in `:prepared` for the entire parked duration:
providers never open upstream connections, no `:io_in` casts arrive
from plugins, and the controller's process memory holds steady at the
idle baseline. This is the structural fix for the memory cascade
observed in the 2026-05-15 live incident.

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
