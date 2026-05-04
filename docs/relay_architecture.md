# Rust SDK Relay Architecture

The Rust SDK exposes a small set of traits for declaring how compute
work is structured inside one logical operator instance. This doc
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
            │   (one per `flz processor start`)       │
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

- **Relay instance** — the live runtime object spawned on `AddProcessor`.
  Each instance has its own per-Partition state (per-Shard) and its
  own mailboxes; instances are independent of each other and routed by
  `processor_id` (the wire calls it that; the SDK calls it
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
  `Entry` (agent → SDK, restricted to `Vec<u8>` / `String`),
  `InterShard` (`Arc<T>` zero-copy between Shards), and `PluginBound`
  (typed value serialized to bytes; sent to a named plugin via the
  agent).

- **Runner** — the work-stealing pool that drains mailboxes. One pool
  per `Flezha::new()`; size controlled by `FLEZHA_WORKER_POOL_SIZE`.

## Public API

The full surface is small:

```rust
use flezha_compute_engine::{
    register_relay, ChannelBuilder, Relay, Shard, ShardCtx, ShardError,
};
```

`register_relay::<R: Relay>()` is called once per Relay class at
process startup. `Flezha::new(namespace)` boots the SDK; the agent
later spawns instances by calling `flz processor start <factory_id>`.

## Worked example

```rust
use flezha_compute_engine::{
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
    let _ = flezha_compute_engine::new("market_ns".to_string());
    // The agent's `flz processor start <market>` spawns an instance.
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
3. **Mailbox push + inflight CAS** — one SegQueue push and one atomic
   compare-exchange. If the CAS wins, submit a drain task to the pool;
   otherwise an in-progress drain will pick up the new item via the
   missed-wake recovery loop.
4. **Drain → Shard::run** — the dispatcher downcasts `Arc<dyn Any>` to
   `Arc<S::Input>`, falls back to a value-clone (`Arc::unwrap_or_clone`)
   if the Arc was shared by fan-out, and calls `Shard::run` with the
   typed signature.

The umbrella's "Per-Message Dispatch-Overhead Budget" requirement caps
this at **3 sync ops per message** on the single-partition fast path
(registry lookup + mailbox enqueue + inflight CAS); static-mode
fan-out adds `N × 2` ops where N is the partition fan-out width.

For the recorded throughput floor and how it's measured, see
`docs/benchmarking.md` — it documents the regression / partitioning /
single-partition / static-fanout bench shapes, the recorded
`BENCH_FLOOR_MSG_PER_SEC` and `SINGLE_PARTITION_FLOOR_MSG_PER_SEC`
values, the floor-update protocol, and CI triage instructions when the
`bench-regression` gate fires.

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

The agent's plugin layer (e.g.
`Flezha.Plugin.Provider.Kafka.Producer`) matches on
`metadata["channel"]` to gate which channel's outputs it consumes.

`ctx.emit_with_meta::<C>(value, map)` is the same with user-attached
metadata appended after the reserved `channel` key. It returns
`ShardError::MetadataNotAllowedOnInterShardChannel` if called on an
inter-shard channel.

## Per-Partition state

Each PartitionSlot owns a `(Shard, S::State)` pair behind a `Mutex`
that the inflight CAS keeps uncontended in practice:

- The Shard's `&mut self` is a per-partition scratch buffer (e.g. a
  reusable `Vec<u8>` for serialization).
- `S::State` is the user's domain state. `init_state(shard_key)` is
  called once per partition; subsequent runs mutate in place.
- State migration (`MigrationSerialization` /
  `ProcessorState`) walks the partitions per-Shard and produces a
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
  `relay-mvp-java` does that work. Java↔agent integration tests are
  tagged `RELAY_MVP_JAVA_PENDING`.
- **flezha_market migration** — downstream operator app migrates in its
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
| PartitionSlot + mailbox | `sdk/rust/src/partition/mod.rs`, `compute_engine/mailbox.rs` |
| Class + instance registries | `sdk/rust/src/relay/registry.rs` |
| Wire dispatch | `sdk/rust/src/compute_engine/fast.rs::RelayDispatchHandle` |
| Wire input routing | `sdk/rust/src/command/streams_connect.rs` |
| Plugin emit framing | `sdk/rust/src/relay/dispatcher.rs::send_plugin_frame` |
| State migration JSON shape | `sdk/rust/src/command/processor_state.rs` |

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
