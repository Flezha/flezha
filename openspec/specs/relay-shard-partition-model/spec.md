# relay-shard-partition-model Specification

## Purpose
TBD - created during sync of `relay-bounded-mailbox`. The umbrella `relay-architecture` change defines the canonical Relay/Shard/Partition model and is the source of additional requirements once it archives. Update Purpose then.

## Requirements

### Requirement: Per-Partition mailbox is bounded with declared capacity

Each PartitionSlot's mailbox SHALL have a fixed capacity declared at relay startup. The mailbox SHALL refuse a push that would exceed this capacity; the SDK then applies the source channel's `OverflowPolicy` (see "Overflow policy declared per channel"). Capacity SHALL NOT change at runtime once the relay is started.

The capacity SHALL be the value returned by the consumer Shard's `mailbox_capacity()` trait method, evaluated once per Shard at `register_relay::<R>()` time after applying the precedence rules in "Mailbox capacity precedence". Static-mode Shards SHALL apply the same capacity to every Partition declared by `shard_keys()`.

The mailbox SHALL refuse capacity values of 0 (which would always overflow) and values strictly greater than 1_048_576 (1 MiB items). Relay startup SHALL fail with an error naming the offending Shard when `mailbox_capacity()` returns an out-of-range value.

#### Scenario: Default capacity is 1024
- **WHEN** a Shard does not override `mailbox_capacity()` and no `FLEZHA_MAILBOX_CAPACITY` env var is set
- **THEN** the runtime SHALL allocate a mailbox of capacity 1024 for every Partition of that Shard

#### Scenario: Override on the Shard takes precedence
- **WHEN** a Shard overrides `mailbox_capacity() -> usize { 4096 }` and sets `MAILBOX_CAPACITY_OVERRIDDEN = true`
- **THEN** every Partition of that Shard SHALL have a mailbox of capacity 4096, regardless of any `FLEZHA_MAILBOX_CAPACITY` env value

#### Scenario: Env var override applies when no Shard override
- **WHEN** `FLEZHA_MAILBOX_CAPACITY=2048` is set and a Shard does not override `mailbox_capacity()`
- **THEN** every Partition of that Shard SHALL have a mailbox of capacity 2048

#### Scenario: Static-mode partitions share one capacity
- **WHEN** a Shard declares `shard_keys() = ["1m","1h","1d","1w"]` and `mailbox_capacity() = 4096`
- **THEN** the runtime SHALL create four mailboxes, each of capacity 4096

#### Scenario: Capacity 0 is rejected at startup
- **WHEN** a Shard returns 0 from `mailbox_capacity()`
- **THEN** Relay startup SHALL fail with an error naming the Shard

#### Scenario: Capacity above maximum is rejected at startup
- **WHEN** a Shard returns 2_000_000 from `mailbox_capacity()`
- **THEN** Relay startup SHALL fail with an error naming the Shard

### Requirement: Mailbox capacity precedence

The SDK SHALL resolve a Shard's mailbox capacity at `register_relay::<R>()` time using the following ordered rule:

1. If `Shard::MAILBOX_CAPACITY_OVERRIDDEN` is `true`, use the value returned by `Shard::mailbox_capacity()`.
2. Otherwise, if the `FLEZHA_MAILBOX_CAPACITY` environment variable is set to a non-empty string parseable as `usize`, use that value.
3. Otherwise, use the built-in default of 1024.

The override-detection const `MAILBOX_CAPACITY_OVERRIDDEN` SHALL exist on the `Shard` trait alongside the existing `PARTITION_FOR_OVERRIDDEN` and `SHARD_KEY_OVERRIDDEN` consts; users overriding `mailbox_capacity()` MUST also set `MAILBOX_CAPACITY_OVERRIDDEN = true`.

#### Scenario: Override-flag false is treated as "not overridden"
- **WHEN** a Shard overrides `mailbox_capacity() -> usize { 4096 }` but leaves `MAILBOX_CAPACITY_OVERRIDDEN` at its `false` default and `FLEZHA_MAILBOX_CAPACITY=2048` is set
- **THEN** the resolved capacity SHALL be 2048 (env-var precedence applies because the SDK does not see the override flag as true)

#### Scenario: Env var set but unparseable is ignored
- **WHEN** `FLEZHA_MAILBOX_CAPACITY=banana` is set and the Shard does not override
- **THEN** the resolved capacity SHALL fall through to the default 1024

### Requirement: Overflow policy declared per channel

Each channel declared via `c.input::<T>(...)` (entry) or `c.channel::<T>(...).from::<S>()` (inter-shard) SHALL carry an `OverflowPolicy` value applied at the producer-side push site when the consumer's mailbox is full. Plugin-bound channels (`.to_plugin(...)`) SHALL NOT carry an `OverflowPolicy` because no SDK-side mailbox sits between the producer Shard and the wire; declaring `.on_full(...)` on a plugin-bound channel SHALL be a compile error (the chained-builder shape after `.to_plugin(...)` MUST NOT expose `.on_full(...)`).

`OverflowPolicy` SHALL be an enum with exactly three variants:

- `Block` — the producer SHALL park its calling thread until the consumer drains an item, then re-attempt the push.
- `DropNewest` — the incoming item SHALL be discarded; the consumer's `drops_newest_total` counter SHALL be incremented by 1.
- `DropOldest` — the consumer's mailbox SHALL atomically pop its oldest item and push the incoming one (best-effort under MPMC contention); the consumer's `drops_oldest_total` counter SHALL be incremented by 1.

The policy SHALL be declared via the channel-builder method `.on_full(OverflowPolicy)` on `EntryChannel`, `ChannelDecl`, and `ChannelDeclWithProducer`. When `.on_full(...)` is not called the channel SHALL use the kind-specific default (see "Default overflow policy by channel kind").

#### Scenario: Block waits then retries
- **WHEN** an inter-shard channel declares `.on_full(OverflowPolicy::Block)`, the consumer's mailbox is full, and a producer calls `ctx.emit::<C>(value)`
- **THEN** the producer's thread SHALL park until the consumer pops an item, then the push SHALL retry; on success the producer's `Shard::run` SHALL return `Ok(())` (assuming no other error)
- **AND** the producer's call to `ctx.emit::<C>(value)` SHALL eventually return `Ok(())` (no `ShardError` is produced for the backpressure event)

#### Scenario: DropNewest discards the incoming item
- **WHEN** an entry channel declares `.on_full(OverflowPolicy::DropNewest)` and a wire frame arrives while the consumer's mailbox is full
- **THEN** the SDK SHALL discard the frame's typed payload; the consumer Partition's `drops_newest_total` SHALL increase by 1; no scheduling of a drain task SHALL occur as a result of this push

#### Scenario: DropOldest replaces the head item
- **WHEN** an inter-shard channel declares `.on_full(OverflowPolicy::DropOldest)`, the consumer's mailbox is full, and a producer emits a value
- **THEN** the consumer's mailbox SHALL drop its oldest item and SHALL accept the new item; the consumer Partition's `drops_oldest_total` SHALL increase by 1; the producer's `ctx.emit::<C>(value)` SHALL return `Ok(())`

#### Scenario: Plugin-bound channel rejects on_full at compile time
- **WHEN** a Relay declares `c.channel::<Vec<u8>>("p").from::<S>().to_plugin("kafka").on_full(OverflowPolicy::Block)`
- **THEN** the Rust compiler SHALL emit a "no method named `on_full`" error and the program MUST NOT build

### Requirement: Default overflow policy by channel kind

When a channel does not call `.on_full(...)`, the runtime SHALL apply a default policy determined by the channel's kind:

- Entry channels: `OverflowPolicy::DropNewest`.
- Inter-shard channels: `OverflowPolicy::Block`.
- Plugin-bound channels: not applicable (no SDK-side mailbox).

These defaults SHALL be reflected in the `ChannelDescriptor.overflow_policy` field set by `build_topology` for every channel for which the user did not explicitly call `.on_full(...)`.

#### Scenario: Default entry policy is DropNewest
- **WHEN** a Relay declares `c.input::<Vec<u8>>("trade.raw").into::<ParseShard>()` without `.on_full(...)` and a frame arrives while the consumer mailbox is full
- **THEN** the frame SHALL be discarded and `drops_newest_total` SHALL increase by 1

#### Scenario: Default inter-shard policy is Block
- **WHEN** a Relay declares `c.channel::<Candle>("ohlc.candles").from::<OhlcShard>().into::<IndicatorShard>()` without `.on_full(...)` and IndicatorShard's mailbox is full while OhlcShard emits
- **THEN** the OhlcShard's calling thread SHALL park until IndicatorShard drains an item, and then the push SHALL retry until success; OhlcShard's `pushes_blocked_total` SHALL increase by at least 1

### Requirement: Per-Partition backpressure counters

Each PartitionSlot's mailbox SHALL expose four `AtomicU64` counters readable via a `PartitionCounters` accessor:

- `pushes_total` — incremented on every successful push.
- `pushes_blocked_total` — incremented once per `Block` park event (multiple parks for one push due to multiple-producer contention SHALL each increment).
- `drops_newest_total` — incremented per discarded item under `DropNewest`.
- `drops_oldest_total` — incremented per discarded item under `DropOldest`.

The counters SHALL use `Ordering::Relaxed` for both reads and writes. The counters SHALL be visible to in-process code (e.g. via `partition.mailbox.counters()`); export to external metrics systems (Prometheus, StatsD, log) is OUT OF SCOPE for this requirement.

#### Scenario: Counters present at runtime
- **WHEN** a PartitionSlot is constructed at relay startup
- **THEN** the slot's mailbox SHALL expose a `counters()` accessor returning a struct with the four named atomics, all initialized to 0

#### Scenario: Successful push increments pushes_total
- **WHEN** an inter-shard producer emits 10 items into a non-full consumer mailbox
- **THEN** the consumer Partition's `pushes_total` SHALL increase by exactly 10

#### Scenario: DropNewest increments only drops_newest_total
- **WHEN** an entry channel under `DropNewest` policy receives 5 frames while the consumer mailbox is full
- **THEN** `drops_newest_total` SHALL increase by 5; `drops_oldest_total` SHALL be unchanged; `pushes_blocked_total` SHALL be unchanged; `pushes_total` SHALL be unchanged

### Requirement: Block parking does not violate sticky per-partition ordering

When a producer thread is parked on a full consumer mailbox under `OverflowPolicy::Block`, the runtime SHALL preserve the existing "Sticky per-partition ordering" requirement on the *consumer* Partition. Specifically:

- The consumer Partition's drain loop SHALL continue to run on whichever worker thread holds its inflight CAS; no parked producer SHALL block consumer drain.
- Producers parked on the same consumer mailbox SHALL be unparked one at a time as items pop, in the order they parked (FIFO unpark fairness).

This requirement is in addition to and does not modify the "Sticky per-partition ordering" requirement from the umbrella `relay-architecture` capability spec; it constrains how Block must integrate with that invariant.

#### Scenario: Parked producer does not block its consumer's drain
- **WHEN** Producer A's thread is parked because Consumer X's mailbox is full, and the worker pool has one other thread that picks up X's drain
- **THEN** X's drain SHALL run to completion (popping items, calling `Shard::run`); each pop SHALL unpark one waiting producer (FIFO order); A SHALL eventually be unparked and complete its push

#### Scenario: FIFO unpark order
- **WHEN** Producers A, B, C park in that order on the same full consumer mailbox
- **THEN** the consumer's first three pops SHALL unpark A, B, C respectively (best-effort under MPMC contention; the *intent* is FIFO order via a `VecDeque<Unparker>` implementation)

### Requirement: Mailbox-full handling does not stall sibling Relays at the entry edge under default policy

Under the default entry-channel policy (`DropNewest`), the SDK's wire-reader thread (the per-stream loop in `command/streams_connect.rs`) SHALL NOT block on a full mailbox. The frame's typed payload SHALL be discarded; the loop SHALL immediately attempt to read the next frame from the wire.

If an operator opts into `OverflowPolicy::Block` on an entry channel by explicit `.on_full(OverflowPolicy::Block)` declaration, the wire-reader thread MAY park while pushing to that channel's consumer mailbox. The SDK SHALL document this co-tenancy hazard in `docs/relay_architecture.md` and `docs/sdk_protocol_spec.md`. No runtime mechanism prevents an operator from making this choice.

#### Scenario: Default entry policy keeps wire reader running
- **WHEN** Relay A's entry mailbox is full under default `DropNewest` policy and Relay B (sharing the physical wire connection) has frames pending
- **THEN** the wire reader SHALL discard the frame addressed to A and continue dispatching the next frame; Relay B's frames SHALL not be delayed by A's saturation

#### Scenario: Opt-in entry Block opts into co-tenancy hazard
- **WHEN** a Relay declares `c.input::<Vec<u8>>("x").on_full(OverflowPolicy::Block).into::<S>()` and the entry mailbox saturates
- **THEN** the wire-reader thread SHALL park on the push; Relay B sharing the same connection SHALL NOT receive new frames until the parked push completes (this is documented behavior under explicit opt-in, not a defect)
