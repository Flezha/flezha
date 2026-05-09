# relay-shard-partition-model Specification

## Purpose
TBD - created during sync of `relay-bounded-mailbox`. The umbrella `relay-architecture` change defines the canonical Relay/Shard/Partition model and is the source of additional requirements once it archives. Update Purpose then.
## Requirements

### Requirement: Canonical role vocabulary

The system SHALL use exactly these terms for top-level roles, and documentation, code, protobuf, CLI, and on-disk artifacts SHALL NOT introduce synonyms:

- **Controller** — the Elixir/OTP control-plane process. Owns cluster topology, ProcessHub-driven placement of Relays, WAL/snapshot persistence, and the CLI/UI surface.
- **Executor** — the Rust/Java SDK process. Hosts Relays at the direction of a Controller. One or more Executors connect to a Controller. Replaces the prior terms "Compute Engine" and "Operator" as a process synonym.
- **Relay** — the top-level user-defined unit of work. One Relay per `flz relay start` invocation. Owned by the Controller, instantiated by an Executor.
- **Shard** — a typed stage within a Relay (multiple per Relay).
- **Partition** — a per-key lane within a Shard (one or many per Shard depending on declared keys).
- **Channel** — a typed I/O port on a Shard.
- **Operator (ProcessHub-internal)** — a node in ProcessHub's distribution ring. Not user-facing in Flezha; appears only in code that interfaces with the ProcessHub library and in `docs/process_hub_architecture.md`.

#### Scenario: Documentation uses canonical terms
- **WHEN** any spec, README, doc under `docs/`, or `CLAUDE.md` references the orchestrator process
- **THEN** it SHALL use the term "Controller" (not "Agent")

#### Scenario: Documentation uses canonical terms for SDK process
- **WHEN** any spec, README, doc under `docs/`, or `CLAUDE.md` references the SDK process that runs Relays
- **THEN** it SHALL use the term "Executor" (not "Compute Engine" or "Operator", except where "Operator" is explicitly qualified as ProcessHub-internal)

#### Scenario: Glossary exists at a stable location
- **WHEN** a contributor needs to look up a Flezha role term
- **THEN** `docs/glossary.md` SHALL exist at the repo root under `docs/` and SHALL define each of: Controller, Executor, Relay, Shard, Partition, Channel, and Operator (ProcessHub-internal)

### Requirement: Hierarchy and placement

The system SHALL realize the role relationship as follows:

```
Controller (1 per deployment)
  ↓ (runs N ProcessHub instances, one per namespace)
  → Namespace
    ↓ (ProcessHub picks placement by consistent hash)
    → Relay instance (1 per `flz relay start`)
      ↓ (Controller's `alter_child_data` hook picks an `executor_id`)
      → Executor (SDK process) — may host multiple Relays
        ↓ (SDK-internal partitioning; not ProcessHub-aware)
        → Shard (declared per Relay class)
          ↓
          → Partition (per declared key, or default 1)
            ↓
            → Channel (typed I/O port)
```

The Controller SHALL hold the authoritative placement table. Executors SHALL register with the Controller via the control channel and SHALL receive `AddRelay` / `RemRelay` commands. Partitioning beneath the Relay level (Shards, Partitions, Channels) is SDK-internal and SHALL NOT be ProcessHub-aware.

#### Scenario: Controller distributes Relays across Executors
- **WHEN** a Controller has two healthy Executors A and B and `flz relay start` is invoked twice
- **THEN** ProcessHub SHALL place each Relay on an Executor according to its consistent-hash policy; the Controller SHALL issue `AddRelay` over the corresponding Executor's control channel

#### Scenario: Each Relay runs on exactly one Executor at a time
- **WHEN** a Relay is placed on Executor A
- **THEN** ProcessHub SHALL ensure no concurrent `AddRelay` for the same `relay_id` is issued to any other Executor; migrations SHALL go through the documented HotSwap-with-handover path
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

If an executor opts into `OverflowPolicy::Block` on an entry channel by explicit `.on_full(OverflowPolicy::Block)` declaration, the wire-reader thread MAY park while pushing to that channel's consumer mailbox. The SDK SHALL document this co-tenancy hazard in `docs/relay_architecture.md` and `docs/sdk_protocol_spec.md`. No runtime mechanism prevents an executor from making this choice.

#### Scenario: Default entry policy keeps wire reader running
- **WHEN** Relay A's entry mailbox is full under default `DropNewest` policy and Relay B (sharing the physical wire connection) has frames pending
- **THEN** the wire reader SHALL discard the frame addressed to A and continue dispatching the next frame; Relay B's frames SHALL not be delayed by A's saturation

#### Scenario: Opt-in entry Block opts into co-tenancy hazard
- **WHEN** a Relay declares `c.input::<Vec<u8>>("x").on_full(OverflowPolicy::Block).into::<S>()` and the entry mailbox saturates
- **THEN** the wire-reader thread SHALL park on the push; Relay B sharing the same connection SHALL NOT receive new frames until the parked push completes (this is documented behavior under explicit opt-in, not a defect)

### Requirement: Shard declares Change associated type and apply_change function (Rust SDK)

The Rust SDK's `Shard` trait SHALL declare an associated `type Change: flezha_executor::state::Change` with default `flezha_executor::state::NoChange`. The `Change` trait SHALL bound on `Serialize + DeserializeOwned + Send + Sync + 'static` and SHALL declare `fn coalesce_key(&self) -> Option<CoalesceKey>` defaulting to `None`.

The `Shard` trait SHALL declare a function `fn apply_change(state: &mut Self::State, change: &Self::Change)` whose default implementation panics with a descriptive error string for any `Change` other than `NoChange`, and is a no-op for `NoChange`. Shards declaring a non-`NoChange` Change SHALL override `apply_change` to make their state mutation deterministic.

`apply_change` SHALL be deterministic: replay equality is required. The Rust SDK SHALL NOT enforce purity in the type system; instead, `flezha_executor::testing::ShardTestHarness::<S>::replay_equals` SHALL be the determinism gate exercised by user tests.

#### Scenario: Default Change is NoChange skips persistence machinery

- **WHEN** a Rust Shard does not declare its `type Change` and the runtime constructs a Partition for it
- **THEN** `Shard::Change` SHALL resolve to `NoChange`, the runtime SHALL detect this via `TypeId::of::<S::Change>() == TypeId::of::<NoChange>()` at relay-build time and SHALL NOT register a flush buffer or invoke the per-Partition flusher for this Partition

#### Scenario: User-defined Change opts the Shard into persistence

- **WHEN** a Rust Shard declares `type Change = MyChange` (any non-`NoChange` user type implementing the `Change` trait)
- **THEN** the runtime SHALL register a typed flush buffer (`Vec<MyChange>`) on each Partition of that Shard and SHALL include those Partitions in the per-process flusher's drain rotation

#### Scenario: Default apply_change panics on non-NoChange Change without override

- **WHEN** a Rust Shard declares `type Change = MyChange` but does NOT override `apply_change`
- **THEN** the relay registration step SHALL succeed (the trait default impl exists) but the first `state.apply(MyChange::Foo)` call SHALL panic with a descriptive error pointing the user at the missing override

#### Scenario: Determinism verified by replay_equals

- **WHEN** a test invokes `ShardTestHarness::<S>::new().apply_inputs(inputs).replay_equals()` against a deterministic `apply_change`
- **THEN** the returned `Result<(), ReplayMismatch>` SHALL be `Ok(())` and the test SHALL pass; for a non-deterministic `apply_change` (e.g. one that reads `SystemTime::now()`), the result SHALL be `Err(ReplayMismatch { ... })` carrying serialized expected and actual states

### Requirement: Shard declares Clock associated type with default WallClock (Rust SDK)

The Rust SDK's `Shard` trait SHALL declare an associated `type Clock: flezha_executor::state::Clock` with default `WallClock`. `Clock` SHALL be a sealed trait with v1 implementations `WallClock`, `LogicalClock`, and `EventTimeFrom<F>`. v1 of the Rust SDK SHALL fully wire only `WallClock`; Shards declaring a non-default Clock SHALL fail Relay startup with `RelayStartError::ClockNotImplementedYet { clock_type }` until a follow-up change wires `LogicalClock` and `EventTimeFrom`.

#### Scenario: Default Clock is WallClock

- **WHEN** a Rust Shard does not declare its `type Clock`
- **THEN** `Shard::Clock` SHALL resolve to `WallClock` and Relay startup SHALL succeed

#### Scenario: Non-WallClock fails startup in v1

- **WHEN** a Rust Shard declares `type Clock = LogicalClock`
- **THEN** `register_relay::<R>()` SHALL fail with `RelayStartError::ClockNotImplementedYet { clock_type: "LogicalClock" }` and the relay SHALL NOT be registered

### Requirement: Shard declares Config associated type with typed constructor (Rust SDK)

The Rust SDK's `Shard` trait SHALL declare an associated `type Config: DeserializeOwned + Send + Sync + 'static` with default `()`, and a constructor `fn from_config(cfg: Self::Config) -> Self`. When `Config = ()` (the default), `from_config(())` SHALL invoke `Self::default()` (requiring `Shard: Default` for unit-Config shards; this is satisfied by the existing `relay-mvp-rust` requirement).

#### Scenario: Default unit Config

- **WHEN** a Rust Shard does not declare `type Config`
- **THEN** the runtime SHALL invoke `Shard::default()` (via the trait default's `from_config(())`) at Partition init

#### Scenario: User-defined typed Config

- **WHEN** a Rust Shard declares `type Config = MyConfig` and the relay-start metadata carries matching bytes
- **THEN** the runtime SHALL deserialize the bytes into `MyConfig` (postcard) and SHALL invoke `MyShard::from_config(cfg)` to construct the Shard at Partition init; deserialization failure SHALL produce `RelayStartError::ConfigDeserializeFailed { shard, source }`

### Requirement: Shard declares SCHEMA_VERSION const

The Rust SDK's `Shard` trait SHALL declare `const SCHEMA_VERSION: u32 = 1`. The runtime SHALL stamp this value into the 16-byte header of every persisted Change record for this Shard. Users SHALL bump `SCHEMA_VERSION` when they make a non-additive Change-schema edit (e.g. removing or renaming a Change variant).

#### Scenario: Default SCHEMA_VERSION is 1

- **WHEN** a Rust Shard does not declare `SCHEMA_VERSION`
- **THEN** the trait default `1` SHALL be inherited and stamped into headers

### Requirement: Shard declares compact_after_bytes per-Partition WAL threshold (Rust SDK)

The Rust SDK's `Shard` trait SHALL declare `fn compact_after_bytes() -> usize { 64 * 1024 * 1024 }` (default 64 MiB). The runtime SHALL report this value to the agent at relay startup in the existing relay-start metadata so the agent's compaction trigger uses the per-Shard value.

#### Scenario: Default 64 MiB threshold reported

- **WHEN** a Rust Shard does not override `compact_after_bytes`
- **THEN** the relay-start metadata SHALL carry `compact_after_bytes = 67108864` for this Shard

#### Scenario: Per-Shard override propagated to agent

- **WHEN** a Rust Shard declares `fn compact_after_bytes() -> usize { 16 * 1024 * 1024 }`
- **THEN** the relay-start metadata SHALL carry `compact_after_bytes = 16777216` for this Shard

### Requirement: Shard run receives StateHandle, not direct mutable reference (Rust SDK)

The Rust SDK's `Shard::run` SHALL receive `state: StateHandle<'_, Self>` instead of `state: &mut Self::State`. `StateHandle<'a, S: Shard>` SHALL implement `std::ops::Deref<Target = S::State>` for read access and SHALL expose a single mutation method `apply(&mut self, change: S::Change)` that runs `S::apply_change(state, &change)` and pushes the Change onto the per-Partition flush buffer. `StateHandle` SHALL NOT implement `DerefMut`.

#### Scenario: Direct mutation does not compile

- **WHEN** a Rust Shard's `run` body attempts `state.field = value` directly via `StateHandle`
- **THEN** the Rust compiler SHALL fail with a "trait bound `DerefMut` not satisfied" error and the program MUST NOT build

#### Scenario: Mutation via apply succeeds, runs apply_change, and records

- **WHEN** a Rust Shard's `run` body calls `state.apply(MyChange::Foo)`
- **THEN** `S::apply_change(state, &MyChange::Foo)` SHALL run on the underlying state AND `MyChange::Foo` SHALL be appended to the per-Partition flush buffer (as observed by `ShardTestHarness::recorded_changes()` and by the per-process flusher on its next tick)

#### Scenario: Read access via Deref is allocation-free

- **WHEN** a Rust Shard's `run` body reads `state.some_field`
- **THEN** the read SHALL succeed via `StateHandle`'s `Deref` impl with no Change recording, no buffer mutation, and zero heap allocations

### Requirement: Relay declares PERSISTENCE_ENABLED const, frozen at instance start (Rust SDK)

The Rust SDK's `Relay` trait SHALL declare `const PERSISTENCE_ENABLED: bool = true`. The runtime SHALL resolve a per-Relay-instance `persistence_enabled: bool` at `start_relay(factory_id, instance_id)` time using:

1. If the relay-start metadata from the agent contains `relay_persistence.<RelayClass>` with a boolean value, use that.
2. Otherwise, use the value of `Relay::PERSISTENCE_ENABLED`.
3. The default of the trait const is `true`.

The resolved value SHALL be bound to the `RelayInstance` and SHALL be propagated to every `PartitionSlot` constructed for that instance. Subsequent changes to the executor-side metadata or to the `Relay::PERSISTENCE_ENABLED` const (via SDK redeploy) SHALL NOT affect already-running instances; new values take effect on the next `stop_relay` + `start_relay` cycle for that instance.

#### Scenario: Default const value is true

- **WHEN** a Relay does not declare `PERSISTENCE_ENABLED` and no executor override is present in relay-start metadata
- **THEN** `RelayInstance::persistence_enabled()` SHALL return `true`

#### Scenario: Const-declared opt-out is honored

- **WHEN** `MarketRelay` declares `const PERSISTENCE_ENABLED: bool = false` and no executor override is present
- **THEN** `RelayInstance::persistence_enabled()` SHALL return `false`; the per-Process flusher SHALL drain-and-discard for this instance's Partitions; `stub_send_frame` (or its real counterpart) SHALL NOT be invoked for this instance

#### Scenario: Executor override takes precedence over const

- **WHEN** `MarketRelay` declares `const PERSISTENCE_ENABLED: bool = true` AND the relay-start metadata carries `relay_persistence.MarketRelay = false`
- **THEN** `RelayInstance::persistence_enabled()` SHALL return `false`

#### Scenario: Resolved value frozen at instance start

- **WHEN** a `RelayInstance` is constructed with resolved `persistence_enabled = true`, AND the executor subsequently issues a runtime config change setting `relay_persistence.<RelayClass> = false`
- **THEN** the running `RelayInstance` SHALL continue with `persistence_enabled = true`; the new value SHALL take effect only after `stop_relay(instance_id)` and a subsequent `start_relay(...)` invocation

### Requirement: init_state accepts optional restoration payload (Rust SDK)

The Rust SDK's `Shard::init_state` signature SHALL be `fn init_state(shard_key: Option<&str>, init_payload: Option<RestoreInitPayload<'_>>) -> Self::State`. When `init_payload = None`, `init_state` SHALL behave as a fresh-start constructor. When `init_payload = Some(payload)`, the user MAY inspect or ignore the payload bytes; the runtime SHALL automatically apply the payload (snapshot decode + change-log fold via `Shard::apply_change`) AFTER `init_state` returns, replacing the user-constructed state with the restored state.

#### Scenario: Cold start passes None

- **WHEN** a Rust Partition is constructed for the first time with no prior persisted state on the agent
- **THEN** the runtime SHALL invoke `init_state(shard_key, None)`

#### Scenario: Restart passes Some payload

- **WHEN** a Rust Partition is reconnected and the agent has snapshot or WAL bytes for it
- **THEN** the runtime SHALL invoke `init_state(shard_key, Some(payload))` AND, after `init_state` returns, the runtime SHALL decode `payload.snapshot_bytes` into `Self::State` (replacing the user's value) if the snapshot is non-empty, AND the runtime SHALL fold `payload.change_log_bytes` via `Shard::apply_change` in seq order

#### Scenario: User can inspect payload without applying

- **WHEN** a Rust Shard's `init_state` reads `payload.change_log_bytes.len()` for instrumentation purposes
- **THEN** the read SHALL succeed; the runtime SHALL still apply the payload after `init_state` returns; the user's instrumentation SHALL not affect the restored state

### Requirement: Per-Partition flush buffer registered for non-NoChange Shards

The Rust SDK's `PartitionSlot` SHALL hold a type-erased flush buffer (`Arc<Mutex<Box<dyn FlushableBuffer>>>`) for any Partition whose Shard's `Change` type is not `NoChange`. The buffer SHALL be a `Vec<S::Change>` initially empty; `state.apply(...)` SHALL push onto it. The per-process flusher SHALL acquire the Mutex on each linger tick to drain the buffer.

For Partitions whose Shard's `Change == NoChange`, the runtime SHALL NOT allocate a flush buffer or register the Partition with the flusher.

#### Scenario: NoChange Shard Partitions are not registered with the flusher

- **WHEN** the runtime constructs a Partition for a Shard with `Change = NoChange`
- **THEN** the `PartitionFlusher`'s registry SHALL NOT contain an entry for this Partition

#### Scenario: Non-NoChange Shard Partitions are registered

- **WHEN** the runtime constructs a Partition for a Shard with `Change = MyChange`
- **THEN** the `PartitionFlusher`'s registry SHALL contain an entry keyed by `(relay_instance_id, shard_id, partition_key)` with the typed buffer reference, the resolved `persistence_enabled` flag, the `change_seq: AtomicU64`, and the wire-frame send fn pointer

### Requirement: Four-layer abstraction with defined responsibilities

The SDK SHALL expose a four-layer abstraction comprising Relay, Shard, Partition, and Runner. Each layer SHALL have the following exclusive responsibilities and no others SHALL be implicitly added:

- **Relay** — a user-defined struct implementing the `Relay` trait. Spawned by the agent once per `flz relay start` invocation. Owns the channel topology declaration and the registered Shards. Holds no domain state of its own.
- **Shard** — a user-defined struct implementing the `Shard` trait. Defines a stage's typed input, typed output, state type, run logic, and partitioning strategy. Holds no runtime state of its own; acts as the template for Partitions.
- **Partition** — a runtime instance of a Shard, identified by an optional string key. Holds exactly one State value, exactly one mailbox, and exactly one `inflight` flag. Created by the SDK at relay startup; not user-instantiable.
- **Runner** — an OS thread in the SDK's worker pool. Drains Partition mailboxes. Not user-visible at the API surface; pool size is configured via the existing `FLEZHA_WORKER_POOL_SIZE` mechanism.

#### Scenario: Each layer has a distinct programmer-visible role
- **WHEN** a developer reads the public API of the new SDK
- **THEN** they SHALL find a `Relay` trait, a `Shard` trait, but no `Partition` trait or `Runner` trait — Partitions and Runners are runtime artifacts created by the SDK, not types the user implements

#### Scenario: A Relay holds no domain state
- **WHEN** a `Relay` impl is examined
- **THEN** the trait MUST NOT include a state-init or state-handle method; all stateful behavior MUST live on Shards

### Requirement: Static and single partitioning modes are supported in v1

A Shard SHALL run in one of exactly two partitioning modes:

- **Single partition (default)**: when the Shard implements neither `shard_keys()` nor `shard_key()`. The runtime SHALL create exactly one Partition with `shard_key = None` at relay startup, and SHALL call `Shard::init_state(None)` to initialize its state.
- **Static partitions**: when the Shard implements `fn shard_keys() -> Vec<&'static str>` returning a non-empty list. The runtime SHALL create one Partition per declared key at relay startup, and SHALL call `Shard::init_state(Some(key))` for each.

The runtime SHALL refuse to start a Relay where any Shard:
- declares both `shard_keys()` and `shard_key()` simultaneously;
- declares only `shard_key()` without `shard_keys()` (this is the API shape reserved for the future dynamic mode and MUST fail with a clear error message in v1).

#### Scenario: Single partition for a Shard with no partitioning declared
- **WHEN** a Shard implements neither `shard_keys()` nor `shard_key()` and the Relay is started
- **THEN** the SDK SHALL create exactly one Partition for that Shard with `shard_key = None`, and `Shard::init_state(None)` SHALL be called exactly once during startup

#### Scenario: Static partitions created at relay startup
- **WHEN** a Shard declares `shard_keys() = ["1m", "1h", "1d", "1w"]` and the Relay is started
- **THEN** the SDK SHALL create exactly four Partitions for that Shard, with `shard_key = Some("1m")`, `Some("1h")`, `Some("1d")`, `Some("1w")` respectively; `init_state` SHALL be called exactly once for each

#### Scenario: Dynamic-mode-only declaration is rejected in v1
- **WHEN** a Shard implements `shard_key(input: &Input) -> Option<String>` without also implementing `shard_keys()`
- **THEN** the Relay startup SHALL fail with an error referring to the dynamic-mode reservation, and the Relay MUST NOT be partially registered

### Requirement: Sticky per-partition ordering

For any single Partition, two invocations of its Shard's `run(...)` method SHALL NOT execute concurrently, and invocations SHALL observe their inputs in the order those inputs were enqueued onto the Partition's mailbox. This invariant MUST hold across all dispatch modes, all runner pool sizes, and all transport types.

#### Scenario: Same partition is never concurrent
- **WHEN** 10,000 inputs targeting one Partition arrive on its mailbox while the runner pool has 16 threads
- **THEN** at any wall-clock instant, at most one runner thread SHALL be inside that Partition's `run(...)` method

#### Scenario: Different partitions of the same shard run in parallel
- **WHEN** inputs arrive simultaneously for two distinct Partitions of the same Shard (for example `("ohlc","1m")` and `("ohlc","1h")`) and the runner pool has ≥ 2 threads
- **THEN** the two Partitions' `run(...)` methods MAY execute concurrently on different threads

#### Scenario: Per-partition input order is preserved
- **WHEN** inputs `a`, `b`, `c` are enqueued onto one Partition's mailbox in that order with an append-to-state Shard
- **THEN** the Partition's State SHALL end with `[a, b, c]` in exactly that order

### Requirement: Typed channels with compile-time wiring checks

All inter-Shard and Shard-to-plugin routing SHALL go through Channels declared on the Relay. Each Channel SHALL carry a phantom type parameter `Channel<T>`. The Relay's channel-wiring API SHALL refuse to compile in any of these cases:

- A producer Shard whose `Output` associated type is not `T` is connected via `.from::<X>()` to a `Channel<T>`.
- A consumer Shard whose `Input` associated type is not `T` is connected via `.into::<X>()` to a `Channel<T>`.
- A Channel is declared with two `.from::<...>()` clauses (multi-producer is forbidden in v1).

#### Scenario: Type-mismatched producer fails to compile
- **WHEN** a Shard with `type Output = TradeMsg` is wired into a `Channel<Candle>` via `.from::<That>()`
- **THEN** the Rust compiler SHALL emit a trait-bound error and the program MUST NOT build

#### Scenario: Type-mismatched consumer fails to compile
- **WHEN** a Shard with `type Input = TradeMsg` is wired as a consumer of a `Channel<Candle>` via `.into::<That>()`
- **THEN** the Rust compiler SHALL emit a trait-bound error and the program MUST NOT build

#### Scenario: Multi-producer channel fails to compile
- **WHEN** the Relay's `channels()` declaration contains two `.from::<X>()` calls on the same channel
- **THEN** the program MUST fail at compile time (or at startup time if compile-time enforcement is impossible for the chosen builder shape)

### Requirement: Per-relay channel namespace

Channel names SHALL be scoped to a single Relay instance. Two Relay instances (for example the BTC Relay and the ETH Relay) SHALL each have their own independent channel namespace; a channel named `"trade.raw"` on one Relay SHALL be a distinct logical channel from a channel of the same name on another Relay.

#### Scenario: Same-named channels on different relays are independent
- **WHEN** the BTC Relay declares `c.input::<Trade>("trade.raw")` and the ETH Relay also declares `c.input::<Trade>("trade.raw")`
- **THEN** an input pushed to the BTC Relay's `"trade.raw"` channel SHALL NOT be observable by the ETH Relay's Shards, and vice versa

### Requirement: Hot-path channel dispatch uses interned u32 IDs

At Relay startup, channel names SHALL be interned to `u32` IDs once. Runtime input-routing decisions inside the SDK hot path SHALL use the integer ID, not the string name. Channel-name strings SHALL appear only in: the user-facing API, registration time, and the on-the-wire metadata field crossing the NIF boundary.

#### Scenario: Hot path is allocation-free with respect to channel names
- **WHEN** an input is dispatched from a Relay's input channel through to a Partition's mailbox during steady-state operation
- **THEN** no per-message allocation of `String` for the channel name SHALL be observable in profiling

### Requirement: Inter-shard transport is in-process zero-copy

When a Channel connects two Shards inside the same Relay, the SDK SHALL transport values as `Arc<T>` references between Partitions, performing no serialization, no allocation of an output buffer, and no NIF-boundary crossing for that transport.

#### Scenario: Pipeline between two shards does not serialize
- **WHEN** OhlcShard emits a `Candle` to a channel that IndicatorShard subscribes to (and the channel has no `to_plugin(...)` clause)
- **THEN** the IndicatorShard's `run(...)` SHALL receive the same `Arc<Candle>` instance OhlcShard emitted (or an `Arc` to byte-identical content with no intermediate serialization step)

### Requirement: Plugin emission semantics

Channels declared with `.to_plugin("name")` (pure-plugin) and channels declared with `.also_to_plugin_with("name", serializer)` (hybrid) SHALL both route a value through the agent's plugin layer using the channel name as the wire identifier. The numeric `plugin_group: u8` metadata field is removed from the wire format. The replacement metadata field SHALL be `channel: String` — the literal channel name as a string in v1 (see `design.md` Decision 15). A per-channel integer fast-path (sending an interned `u32` instead of the string) MAY be added by a future change as an opt-in alternative, but SHALL NOT be in v1.

For pure-plugin channels, the wire bytes are produced by the channel's `plugin_serializer` (set by `to_plugin(...)`). For hybrid channels, the wire bytes are produced by the channel's `plugin_sink.serializer` (set by `also_to_plugin_with(...)`). In both cases the byte buffer SHALL be sent through the same wire framing path (`channel: String` metadata + payload bytes) so the agent's plugin layer SHALL NOT distinguish between pure and hybrid channels at the wire — both arrive identically and route by channel name.

#### Scenario: Pure-plugin emit reaches the agent's plugin layer
- **WHEN** a Shard publishes to a channel declared with `.to_plugin("ohlc_kafka")`
- **THEN** the agent's `ohlc_kafka` plugin SHALL receive the bytes with `channel = <channel name>` in the metadata

#### Scenario: Hybrid emit reaches the agent's plugin layer with identical framing
- **WHEN** a Shard publishes to a channel declared with `.also_to_plugin_with("indicators", sma_to_pb_bytes)` on a channel that also has inter-shard consumers
- **THEN** the agent's `indicators` plugin SHALL receive the serializer-output bytes with `channel = <channel name>` in the metadata
- **AND** the wire frame SHALL be byte-indistinguishable from a pure-plugin channel's frame for the same plugin name and bytes

#### Scenario: Two channels sharing a plugin name fan in at the agent
- **WHEN** a Relay declares two hybrid channels (e.g. `sma_out` with `also_to_plugin_with("indicators", sma_serde)` and `ema_out` with `also_to_plugin_with("indicators", ema_serde)`)
- **THEN** the agent's `indicators` plugin SHALL receive frames from both channels, distinguishable only by the `channel: String` metadata key
- **AND** the SDK's topology builder SHALL NOT reject this declaration as a `MultiProducer` error, because each channel still has exactly one producer Shard

#### Scenario: Plugin subscribes by channel name
- **WHEN** an agent-side plugin is configured to consume outputs from `"ohlc_kafka"`
- **THEN** that plugin SHALL receive every message whose channel name matches `"ohlc_kafka"`, regardless of which Shard produced it

### Requirement: ShardCtx emit semantics

A Shard's `run` method SHALL receive a mutable reference to a `ShardCtx`. The Shard SHALL emit zero, one, or more outputs by calling `ctx.emit::<C>(value)` for each output, where `C` is the channel-payload type (which the compiler resolves to the matching channel via the wiring declaration). For outputs that need a string-keyed user metadata bag attached, the Shard SHALL call `ctx.emit_with_meta::<C>(value, HashMap<String, String>)` instead. The `run` method's return type SHALL be `Result<(), ShardError>` for success / hard-error signaling. Producing no output SHALL be expressed by simply not calling `ctx.emit` (or `ctx.emit_with_meta`); there SHALL be no explicit "no-output" return variant.

`ctx.emit_with_meta` SHALL be valid only for channels declared with `.to_plugin("name")`. Calling `emit_with_meta` on a channel that flows to another Shard inside the same Relay SHALL return a `ShardError` and SHALL NOT propagate the value or the metadata. The error message SHALL name the offending channel and instruct the user to either move the metadata into the typed value or add `.to_plugin(...)` to the channel.

When `emit_with_meta` is used on a plugin-bound channel, the supplied `HashMap<String, String>` SHALL be attached to the output as it crosses the NIF boundary using the existing wire-format `[key_len][val_len][key][value]` block. The channel name (or its interned `u32` equivalent) SHALL be one of the keys; the user-supplied keys SHALL be appended without further validation. Reserved key names (e.g. `channel`) SHALL NOT be overridable via the user-supplied map; an attempt to set one SHALL be silently dropped during framing.

#### Scenario: Fan-out via multiple emit calls
- **WHEN** a ParseShard's `run` calls `ctx.emit::<ParsedTrade>(p)` followed by `ctx.emit::<RawSummary>(s)` and returns `Ok(())`
- **THEN** both downstream channels SHALL receive their respective values, and the order of emission SHALL match the order of the `emit` calls

#### Scenario: No-output via no-call
- **WHEN** a Shard's `run` returns `Ok(())` without calling `ctx.emit`
- **THEN** the SDK SHALL treat the input as fully processed with no downstream propagation, with no error and no special enum variant required

#### Scenario: Hard error via Err return
- **WHEN** a Shard's `run` returns `Err(e)`
- **THEN** the SDK SHALL log the error against the (relay, shard, partition) triple and SHALL NOT propagate any partial output that may have been emitted before the error

#### Scenario: emit_with_meta on a plugin-bound channel
- **WHEN** a Shard's `run` calls `ctx.emit_with_meta::<Candle>(c, {"trace_id": "abc123"})` on a channel declared with `.to_plugin("ohlc_kafka")`
- **THEN** the message reaching the agent SHALL carry the channel name AND the `trace_id: "abc123"` key in its metadata block, and the agent's plugin SHALL receive both

#### Scenario: emit_with_meta on an inter-shard channel returns error
- **WHEN** a Shard's `run` calls `ctx.emit_with_meta::<ParsedTrade>(p, {"trace_id": "abc123"})` on a channel that flows to another Shard inside the same Relay (no `.to_plugin(...)` clause)
- **THEN** the call SHALL return `Err(ShardError::MetadataNotAllowedOnInterShardChannel { channel })`, and the value `p` SHALL NOT be propagated to the downstream Shard

### Requirement: Hybrid channel builder method

The Rust SDK SHALL expose a method `also_to_plugin_with<F>(plugin_name: &'static str, serializer: F) -> Self` on `ChannelDeclWithProducer<T>` (in `sdk/rust/src/channel/builder.rs`) where `F: Fn(&T) -> Vec<u8> + Send + Sync + 'static`. Calling this method SHALL leave the channel's `kind = ChannelKind::InterShard` and SHALL attach a plugin sink (the named plugin and the user-supplied serializer, type-erased to `PluginSerializeFn`) alongside the channel's existing consumer fan-out. After the call, further `.into::<S>()` consumer wiring SHALL chain naturally — the method's return type is `Self`.

The plugin sink SHALL be stored on the channel descriptor as a `plugin_sink: Option<PluginSink>` field where `PluginSink { name: &'static str, serializer: PluginSerializeFn }`. Pure-`PluginBound` channels (produced by `to_plugin(...)`) SHALL continue to populate the existing `plugin_name` and `plugin_serializer` fields and SHALL NOT populate `plugin_sink`. Hybrid channels (`InterShard` + `also_to_plugin_with`) SHALL populate `plugin_sink` and SHALL NOT populate `plugin_name`/`plugin_serializer`. The two paths SHALL stay textually distinct so existing pattern matches over `ChannelKind` continue to compile unchanged.

#### Scenario: Hybrid wiring is accepted at registration
- **WHEN** a Relay declares `c.channel::<SmaValue>("sma_out").from::<SmaShard>().also_to_plugin_with("indicators", sma_to_pb_bytes).into::<BollingerShard>()`
- **THEN** `register_relay::<MarketRelay>()` SHALL succeed
- **AND** the resulting `ChannelDescriptor` for `"sma_out"` SHALL have `kind = InterShard`, exactly one entry in `consumers` (the `BollingerShard` shard id), and `plugin_sink = Some(PluginSink { name: "indicators", serializer: <fn-ptr> })`

#### Scenario: also_to_plugin_with chains in either order with into::<S>()
- **WHEN** a Relay declares `c.channel::<EmaValue>("ema_out").from::<EmaShard>().into::<MacdShard>().also_to_plugin_with("indicators", ema_to_pb_bytes)`
- **THEN** `register_relay::<MarketRelay>()` SHALL succeed
- **AND** the resulting descriptor SHALL be identical to the same declaration with the calls swapped (`.also_to_plugin_with(...).into::<MacdShard>()`)

#### Scenario: Calling also_to_plugin_with twice on the same channel is a topology error
- **WHEN** a Relay calls `c.channel::<T>("foo").from::<S>().also_to_plugin_with("p1", f1).also_to_plugin_with("p2", f2)`
- **THEN** `register_relay::<R>()` SHALL panic with `BuildTopologyError::DuplicatePluginSink { channel: "foo" }`
- **AND** the offending channel name SHALL appear verbatim in the error message

#### Scenario: also_to_plugin_with is unreachable after to_plugin
- **WHEN** a Relay attempts `c.channel::<Vec<u8>>("foo").from::<S>().to_plugin("p").also_to_plugin_with(...)`
- **THEN** the code SHALL fail to compile because `to_plugin` returns `Channel<T>` (a finalized handle with no further methods)
- **AND** a trybuild test under `sdk/rust/tests/build_failures/` SHALL pin this as a stable compile error

### Requirement: Hybrid channel emission — consumer fan-out then single serializer call

When a Shard's `Shard::run` calls `ctx.emit(value)` on a hybrid channel (`kind = InterShard, plugin_sink = Some(_)`), the dispatcher SHALL execute the following sequence on the producer thread:

1. Fan out the typed `Arc<dyn Any + Send + Sync>` value to every consumer mailbox listed in the channel's `consumers` per the channel's `OverflowPolicy`. This step SHALL complete (modulo `OverflowPolicy::Block` parking) before step 2 begins.
2. If `plugin_sink` is `Some(sink)`, invoke `sink.serializer(value.as_ref())` exactly once inside `std::panic::catch_unwind(AssertUnwindSafe(...))`. On `Ok(bytes)`, send the bytes through the existing plugin-wire framing path (`channel: String` wire-metadata key set to the channel name; `[key_len][val_len][key][value]` block format). On `Err(panic)`, return `ShardError::PluginSerializerPanic { channel: <channel name>, message: <formatted panic payload> }` to the producing Shard's `ctx.emit` call without emitting any plugin bytes.

The dispatcher thread SHALL remain alive across a serializer panic. Subsequent emits SHALL proceed normally; only the producing Shard sees the per-emit error.

The serializer SHALL run exactly once per `ctx.emit` invocation regardless of consumer count. The byte buffer produced by the serializer SHALL NOT be cached or reused across emits.

#### Scenario: Hybrid emit fans out typed value to all consumers and bytes to plugin
- **WHEN** a producer Shard's `run` calls `ctx.emit(value)` on a channel wired to `[ConsumerA, ConsumerB]` and with `plugin_sink = Some(("indicators", serializer))`
- **THEN** `ConsumerA::run` and `ConsumerB::run` SHALL each be invoked with the typed `value` (cloned per the existing inter-shard delivery semantics)
- **AND** the agent-side plugin "indicators" SHALL receive a single wire frame with bytes equal to `serializer(&value)` and `channel = <channel name>` in the metadata

#### Scenario: Single serializer call regardless of consumer count
- **WHEN** a hybrid channel has N inter-shard consumers and the producer emits one value
- **THEN** the serializer SHALL be invoked exactly once for that emit
- **AND** N is unconstrained (1, 3, 100 — same single-call guarantee)

#### Scenario: Panicking serializer surfaces as ShardError without crashing the dispatcher
- **WHEN** the serializer panics on a particular input
- **THEN** the inter-shard consumers SHALL still receive the typed value (consumer fan-out completed before serialization)
- **AND** `ctx.emit(...)` SHALL return `Err(ShardError::PluginSerializerPanic { channel, message })`
- **AND** the dispatcher thread SHALL remain alive — a subsequent `ctx.emit` call SHALL execute normally

#### Scenario: Serializer runs after consumer fan-out, not before
- **WHEN** an `OverflowPolicy::Block` consumer mailbox is full and the producer emits on a hybrid channel
- **THEN** the producer SHALL park on the consumer mailbox
- **AND** the plugin sink SHALL NOT receive any bytes until the consumer drains and the producer unparks
- **AND** when the producer unparks, the serializer SHALL be invoked exactly once and the plugin sink SHALL receive the corresponding bytes

### Requirement: emit_with_meta is valid on hybrid channels

`ShardCtx::emit_with_meta(value, meta)` SHALL accept the call when either:

- the channel is `kind = PluginBound` (existing rule), OR
- the channel has `plugin_sink = Some(_)` (new rule for hybrid channels).

On a hybrid channel, the inter-shard consumers SHALL receive the typed value with no metadata (metadata is plugin-framing only and has no place in the typed delivery model). The plugin sink SHALL receive bytes (via the channel's serializer) with the supplied `HashMap<String, String>` attached to the wire frame using the existing `[key_len][val_len][key][value]` block format. The reserved `channel: String` key SHALL be set automatically and SHALL NOT be overridable by user-supplied keys; an attempt to set a reserved key SHALL be silently dropped during framing (matches the existing rule for `PluginBound` channels).

On a pure-`InterShard` channel with no plugin sink, `emit_with_meta` SHALL continue to return `ShardError::MetadataNotAllowedOnInterShardChannel { channel }` without delivering the value or the metadata.

#### Scenario: emit_with_meta on a hybrid channel forwards metadata to plugin only
- **WHEN** a producer Shard's `run` calls `ctx.emit_with_meta(value, {"trace_id": "abc123"})` on a hybrid channel wired to one consumer and one plugin sink
- **THEN** the consumer SHALL receive the typed `value` with no `trace_id` visible
- **AND** the plugin wire frame SHALL contain `trace_id = "abc123"` plus the implicit `channel = <channel name>` key

#### Scenario: emit_with_meta on a pure inter-shard channel still rejects metadata
- **WHEN** a producer Shard's `run` calls `ctx.emit_with_meta(value, {"trace_id": "abc123"})` on a channel that has consumers but no `plugin_sink`
- **THEN** `ctx.emit_with_meta` SHALL return `ShardError::MetadataNotAllowedOnInterShardChannel { channel }`
- **AND** neither the consumers nor any plugin SHALL receive the value or the metadata

#### Scenario: Reserved metadata keys are stripped on hybrid channels
- **WHEN** a producer calls `ctx.emit_with_meta(value, {"channel": "spoof", "trace_id": "abc"})` on a hybrid channel
- **THEN** the wire frame SHALL contain `channel = <real channel name>` (the reserved value, not "spoof") and `trace_id = "abc"`

### Requirement: New BuildTopologyError variant — `DuplicatePluginSink`

`BuildTopologyError` (in `sdk/rust/src/channel/builder.rs`) SHALL gain a `DuplicatePluginSink { channel: &'static str }` variant. The variant SHALL be raised at `register_relay::<R>()` time when `also_to_plugin_with` is called more than once on the same channel. The first call registers the sink; the second SHALL push the error onto the builder's error vector and SHALL NOT overwrite the previously-registered sink. The aggregate `register_relay::<R>()` validation SHALL fail with the standard "topology rejected" message that lists every accumulated `BuildTopologyError`.

#### Scenario: Second also_to_plugin_with call is rejected at registration
- **WHEN** a Relay's `channels(c)` method calls `also_to_plugin_with("p1", f1)` followed by `also_to_plugin_with("p2", f2)` on the same channel
- **THEN** `register_relay::<R>()` SHALL panic with the topology error including `DuplicatePluginSink { channel: "<name>" }`
- **AND** the registered topology SHALL be discarded

### Requirement: New ShardError variant — `PluginSerializerPanic`

`ShardError` (in `sdk/rust/src/shard/error.rs`) SHALL gain a `PluginSerializerPanic { channel: String, message: String }` variant. The variant SHALL be returned from `ctx.emit(...)` (and `ctx.emit_with_meta(...)`) on a hybrid channel when the user-supplied serializer panics. `channel` SHALL be the channel name as declared in `ChannelBuilder`; `message` SHALL be the panic payload formatted via the existing panic-message helper (a string description, not the raw `Box<dyn Any>` payload).

The producing Shard's `Shard::run` SHALL receive the error via the normal `Result<(), ShardError>` return path of `ctx.emit`. Returning `Err` from `Shard::run` continues to discard partial emits per the existing trait contract; ignoring the error is also valid and SHALL leave the inter-shard consumers' delivery intact.

#### Scenario: Producer Shard sees PluginSerializerPanic and chooses how to handle it
- **WHEN** a hybrid channel's serializer panics during `ctx.emit(value)`
- **THEN** `ctx.emit(value)` SHALL return `Err(ShardError::PluginSerializerPanic { channel, message })`
- **AND** the producing Shard MAY ignore, log, or `return Err(...)` — all three are valid handlers
- **AND** the inter-shard consumer fan-out SHALL have already completed before the panic was caught

### Requirement: Entry channel value types limited to bytes or string in v1

Channels declared via `c.input::<T>("name")` (entry channels fed by the agent over the wire) SHALL accept only the following payload types in v1:

- `Channel<Vec<u8>>` — bytes are delivered to the consuming Shard unchanged.
- `Channel<String>` — the SDK SHALL perform UTF-8 decoding at the input edge. An invalid-UTF-8 byte sequence SHALL be logged against the (relay, channel) pair and the input SHALL be dropped without invoking the consuming Shard.

Any other `T` on an entry channel SHALL fail at Relay startup with an error message naming the unsupported type and instructing the user to use `Vec<u8>` or `String` as the entry channel type and add a decoding Shard.

This restriction applies only to entry channels. Inter-shard channels (`c.channel::<T>(...).from::<X>().into::<Y>()`) and plugin-output channels (`.to_plugin(...)`) MAY carry any `T` that satisfies the channel's wiring constraints.

#### Scenario: Entry channel of bytes
- **WHEN** a Relay declares `c.input::<Vec<u8>>("trade.raw")` and an input arrives over the wire
- **THEN** the consuming Shard's `run` SHALL receive the bytes unchanged

#### Scenario: Entry channel of string with valid UTF-8
- **WHEN** a Relay declares `c.input::<String>("trade.raw")` and a UTF-8-valid byte sequence arrives
- **THEN** the consuming Shard's `run` SHALL receive the decoded `String`

#### Scenario: Entry channel of string with invalid UTF-8
- **WHEN** a Relay declares `c.input::<String>("trade.raw")` and a byte sequence that is not valid UTF-8 arrives
- **THEN** the SDK SHALL log the decode failure against the (relay, channel) pair and SHALL NOT invoke the consuming Shard's `run`

#### Scenario: Entry channel of arbitrary type fails at startup
- **WHEN** a Relay declares `c.input::<RawTrade>("trade.raw")` where `RawTrade` is neither `Vec<u8>` nor `String`
- **THEN** the Relay startup SHALL fail with an error message naming the unsupported type and instructing the user to use `Vec<u8>` or `String` as the entry channel type and add a decoding Shard

### Requirement: Worker pool reuses existing dispatch infrastructure

The Runner pool SHALL be the existing Chase-Lev work-stealing pool implementation (`sdk/rust/src/executor/work_stealing_pool.rs`). The `FLEZHA_WORKER_POOL_SIZE` environment variable SHALL continue to control pool size, defaulting to `Runtime.availableRelays()`. The per-Partition `inflight` CAS guard SHALL be the same mechanism the existing per-`RelaySlot` guard uses.

#### Scenario: Default pool sizing
- **WHEN** the SDK is started without `FLEZHA_WORKER_POOL_SIZE` set on a host with 8 cores
- **THEN** the runner pool SHALL contain 8 OS threads

#### Scenario: Env override honored
- **WHEN** `FLEZHA_WORKER_POOL_SIZE=4` is set on a host with 8 cores
- **THEN** the runner pool SHALL contain exactly 4 OS threads

### Requirement: No coexistence with RelayHandler API

The `RelayHandler` trait, `RelayResult` enum, `RelayRunner` enum, `register_factory()` function, `FactoryMetadata` struct, and `InputTransformer` / `OutputTransformer` traits SHALL be removed from the public Rust SDK API in the same change that introduces the `Relay` / `Shard` traits. There SHALL be no transitional period in which both APIs are usable simultaneously.

**Reason**: The library is unpublished, the only known executor-app consumer is in this monorepo and migrates atomically as part of the same change, and the user has an explicit "no legacy / backward-compat shims" policy. Coexistence would also force the new abstraction to retain dispatch-by-relay-id baggage that the new model deliberately leaves behind.

**Migration**: executor apps using `RelayHandler` rewrite to `Relay` + one or more `Shard` impls in the same change. Concrete example: `flezha_market/market-relay/src/main.rs` and `market_computation.rs` are rewritten in the `relay-mvp-rust` change. State (formerly `Assets`) is decomposed into per-Shard state types.

#### Scenario: Old API is not importable
- **WHEN** a Rust file attempts to `use flezha_executor::RelayHandler` after the cutover
- **THEN** the compilation SHALL fail with an unresolved-import error

#### Scenario: Old registration function is removed
- **WHEN** a Rust file attempts to call `register_factory(...)` after the cutover
- **THEN** the compilation SHALL fail with an unresolved-name error

