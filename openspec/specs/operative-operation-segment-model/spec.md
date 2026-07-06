# operative-operation-segment-model Specification

## Purpose
This capability defines the structural execution model for Operatives: bounded per-Segment mailboxes with a per-channel overflow policy, the `Operation` stage trait surface (state, change, clock, config, persistence), typed compile-checked channels, and dynamic-Operation segmenting (lazy creation, idle-TTL eviction, and soft caps).

## Requirements

### Requirement: Hierarchy and placement

The system SHALL realize the role relationship as follows:

```
Controller (1 per deployment)
  ↓ (runs N ProcessHub instances, one per namespace)
  → Namespace
    ↓ (ProcessHub picks placement by consistent hash)
    → Operative instance (1 per `flz operative start`)
      ↓ (Controller's `alter_child_data` hook picks an `executor_id`)
      → Executor (SDK process) — may host multiple Operatives
        ↓ (SDK-internal segmenting; not ProcessHub-aware)
        → Operation (declared per Operative class)
          ↓
          → Segment (per declared key, or default 1)
            ↓
            → Channel (typed I/O port)
```

The Controller SHALL hold the authoritative placement table. Executors SHALL register with the Controller via the control channel and SHALL receive `AddOperative` / `RemOperative` commands. Segmenting beneath the Operative level (Operations, Segments, Channels) is SDK-internal and SHALL NOT be ProcessHub-aware.

#### Scenario: Controller distributes Operatives across Executors
- **WHEN** a Controller has two healthy Executors A and B and `flz operative start` is invoked twice
- **THEN** ProcessHub SHALL place each Operative on an Executor according to its consistent-hash policy; the Controller SHALL issue `AddOperative` over the corresponding Executor's control channel

#### Scenario: Each Operative runs on exactly one Executor at a time
- **WHEN** a Operative is placed on Executor A
- **THEN** ProcessHub SHALL ensure no concurrent `AddOperative` for the same `operative_id` is issued to any other Executor; migrations SHALL go through the documented HotSwap-with-handover path
### Requirement: Per-Segment mailbox is bounded with declared capacity

Each SegmentSlot's mailbox SHALL have a fixed capacity declared at operative startup. The mailbox SHALL refuse a push that would exceed this capacity; the SDK then applies the source channel's `OverflowPolicy` (see "Overflow policy declared per channel"). Capacity SHALL NOT change at runtime once the operative is started.

The capacity SHALL be the value returned by the consumer Operation's `mailbox_capacity()` trait method, evaluated once per Operation at `register_unbounded::<R>()` time after applying the precedence rules in "Mailbox capacity precedence". Static-mode Operations SHALL apply the same capacity to every Segment declared by `operation_keys()`.

The mailbox SHALL refuse capacity values of 0 (which would always overflow) and values strictly greater than 1_048_576 (1 MiB items). Operative startup SHALL fail with an error naming the offending Operation when `mailbox_capacity()` returns an out-of-range value.

#### Scenario: Default capacity is 1024
- **WHEN** a Operation does not override `mailbox_capacity()` and no `FLEZHA_MAILBOX_CAPACITY` env var is set
- **THEN** the runtime SHALL allocate a mailbox of capacity 1024 for every Segment of that Operation

#### Scenario: Override on the Operation takes precedence
- **WHEN** a Operation overrides `mailbox_capacity() -> usize { 4096 }` and sets `MAILBOX_CAPACITY_OVERRIDDEN = true`
- **THEN** every Segment of that Operation SHALL have a mailbox of capacity 4096, regardless of any `FLEZHA_MAILBOX_CAPACITY` env value

#### Scenario: Env var override applies when no Operation override
- **WHEN** `FLEZHA_MAILBOX_CAPACITY=2048` is set and a Operation does not override `mailbox_capacity()`
- **THEN** every Segment of that Operation SHALL have a mailbox of capacity 2048

#### Scenario: Static-mode segments share one capacity
- **WHEN** a Operation declares `operation_keys() = ["1m","1h","1d","1w"]` and `mailbox_capacity() = 4096`
- **THEN** the runtime SHALL create four mailboxes, each of capacity 4096

#### Scenario: Capacity 0 is rejected at startup
- **WHEN** a Operation returns 0 from `mailbox_capacity()`
- **THEN** Operative startup SHALL fail with an error naming the Operation

#### Scenario: Capacity above maximum is rejected at startup
- **WHEN** a Operation returns 2_000_000 from `mailbox_capacity()`
- **THEN** Operative startup SHALL fail with an error naming the Operation

### Requirement: Mailbox capacity precedence

The SDK SHALL resolve a Operation's mailbox capacity at `register_unbounded::<R>()` time using the following ordered rule:

1. If `Operation::MAILBOX_CAPACITY_OVERRIDDEN` is `true`, use the value returned by `Operation::mailbox_capacity()`.
2. Otherwise, if the `FLEZHA_MAILBOX_CAPACITY` environment variable is set to a non-empty string parseable as `usize`, use that value.
3. Otherwise, use the built-in default of 1024.

The override-detection const `MAILBOX_CAPACITY_OVERRIDDEN` SHALL exist on the `Operation` trait alongside the existing `SEGMENT_FOR_OVERRIDDEN` and `OPERATION_KEY_OVERRIDDEN` consts; users overriding `mailbox_capacity()` MUST also set `MAILBOX_CAPACITY_OVERRIDDEN = true`.

#### Scenario: Override-flag false is treated as "not overridden"
- **WHEN** a Operation overrides `mailbox_capacity() -> usize { 4096 }` but leaves `MAILBOX_CAPACITY_OVERRIDDEN` at its `false` default and `FLEZHA_MAILBOX_CAPACITY=2048` is set
- **THEN** the resolved capacity SHALL be 2048 (env-var precedence applies because the SDK does not see the override flag as true)

#### Scenario: Env var set but unparseable is ignored
- **WHEN** `FLEZHA_MAILBOX_CAPACITY=banana` is set and the Operation does not override
- **THEN** the resolved capacity SHALL fall through to the default 1024

### Requirement: Overflow policy declared per channel

Each channel declared via `c.input::<T>(...)` (entry) or `c.channel::<T>(...).from::<S>()` (inter-operation) SHALL carry an `OverflowPolicy` value applied at the producer-side push site when the consumer's mailbox is full. Plugin-bound channels (`.to_plugin(...)`) SHALL NOT carry an `OverflowPolicy` because no SDK-side mailbox sits between the producer Operation and the wire; declaring `.on_full(...)` on a plugin-bound channel SHALL be a compile error (the chained-builder shape after `.to_plugin(...)` MUST NOT expose `.on_full(...)`).

`OverflowPolicy` SHALL be an enum with exactly three variants:

- `Block` — the producer SHALL park its calling thread until the consumer drains an item, then re-attempt the push.
- `DropNewest` — the incoming item SHALL be discarded; the consumer's `drops_newest_total` counter SHALL be incremented by 1.
- `DropOldest` — the consumer's mailbox SHALL atomically pop its oldest item and push the incoming one (best-effort under MPMC contention); the consumer's `drops_oldest_total` counter SHALL be incremented by 1.

The policy SHALL be declared via the channel-builder method `.on_full(OverflowPolicy)` on `EntryChannel`, `ChannelDecl`, and `ChannelDeclWithProducer`. When `.on_full(...)` is not called the channel SHALL use the kind-specific default (see "Default overflow policy by channel kind").

#### Scenario: Block waits then retries
- **WHEN** an inter-operation channel declares `.on_full(OverflowPolicy::Block)`, the consumer's mailbox is full, and a producer calls `ctx.emit::<C>(value)`
- **THEN** the producer's thread SHALL park until the consumer pops an item, then the push SHALL retry; on success the producer's `Operation::run` SHALL return `Ok(())` (assuming no other error)
- **AND** the producer's call to `ctx.emit::<C>(value)` SHALL eventually return `Ok(())` (no `OperationError` is produced for the backpressure event)

#### Scenario: DropNewest discards the incoming item
- **WHEN** an entry channel declares `.on_full(OverflowPolicy::DropNewest)` and a wire frame arrives while the consumer's mailbox is full
- **THEN** the SDK SHALL discard the frame's typed payload; the consumer Segment's `drops_newest_total` SHALL increase by 1; no scheduling of a drain task SHALL occur as a result of this push

#### Scenario: DropOldest replaces the head item
- **WHEN** an inter-operation channel declares `.on_full(OverflowPolicy::DropOldest)`, the consumer's mailbox is full, and a producer emits a value
- **THEN** the consumer's mailbox SHALL drop its oldest item and SHALL accept the new item; the consumer Segment's `drops_oldest_total` SHALL increase by 1; the producer's `ctx.emit::<C>(value)` SHALL return `Ok(())`

#### Scenario: Plugin-bound channel rejects on_full at compile time
- **WHEN** a Operative declares `c.channel::<Vec<u8>>("p").from::<S>().to_plugin("kafka").on_full(OverflowPolicy::Block)`
- **THEN** the Rust compiler SHALL emit a "no method named `on_full`" error and the program MUST NOT build

### Requirement: Default overflow policy by channel kind

When a channel does not call `.on_full(...)`, the runtime SHALL apply a default policy determined by the channel's kind:

- Entry channels: `OverflowPolicy::DropNewest`.
- Inter-operation channels: `OverflowPolicy::Block`.
- Plugin-bound channels: not applicable (no SDK-side mailbox).

These defaults SHALL be reflected in the `ChannelDescriptor.overflow_policy` field set by `build_topology` for every channel for which the user did not explicitly call `.on_full(...)`.

#### Scenario: Default entry policy is DropNewest
- **WHEN** a Operative declares `c.input::<Vec<u8>>("trade.raw").into::<ParseOperation>()` without `.on_full(...)` and a frame arrives while the consumer mailbox is full
- **THEN** the frame SHALL be discarded and `drops_newest_total` SHALL increase by 1

#### Scenario: Default inter-operation policy is Block
- **WHEN** a Operative declares `c.channel::<Candle>("ohlc.candles").from::<OhlcOperation>().into::<IndicatorOperation>()` without `.on_full(...)` and IndicatorOperation's mailbox is full while OhlcOperation emits
- **THEN** the OhlcOperation's calling thread SHALL park until IndicatorOperation drains an item, and then the push SHALL retry until success; OhlcOperation's `pushes_blocked_total` SHALL increase by at least 1

### Requirement: Per-Segment backpressure counters

Each SegmentSlot's mailbox SHALL expose four `AtomicU64` counters readable via a `SegmentCounters` accessor:

- `pushes_total` — incremented on every successful push.
- `pushes_blocked_total` — incremented once per `Block` park event (multiple parks for one push due to multiple-producer contention SHALL each increment).
- `drops_newest_total` — incremented per discarded item under `DropNewest`.
- `drops_oldest_total` — incremented per discarded item under `DropOldest`.

The counters SHALL use `Ordering::Relaxed` for both reads and writes. The counters SHALL be visible to in-process code (e.g. via `segment.mailbox.counters()`); export to external metrics systems (Prometheus, StatsD, log) is OUT OF SCOPE for this requirement.

#### Scenario: Counters present at runtime
- **WHEN** a SegmentSlot is constructed at operative startup
- **THEN** the slot's mailbox SHALL expose a `counters()` accessor returning a struct with the four named atomics, all initialized to 0

#### Scenario: Successful push increments pushes_total
- **WHEN** an inter-operation producer emits 10 items into a non-full consumer mailbox
- **THEN** the consumer Segment's `pushes_total` SHALL increase by exactly 10

#### Scenario: DropNewest increments only drops_newest_total
- **WHEN** an entry channel under `DropNewest` policy receives 5 frames while the consumer mailbox is full
- **THEN** `drops_newest_total` SHALL increase by 5; `drops_oldest_total` SHALL be unchanged; `pushes_blocked_total` SHALL be unchanged; `pushes_total` SHALL be unchanged

### Requirement: Block parking does not violate sticky per-segment ordering

When a producer thread is parked on a full consumer mailbox under `OverflowPolicy::Block`, the runtime SHALL preserve the existing "Sticky per-segment ordering" requirement on the *consumer* Segment. Specifically:

- The consumer Segment's drain loop SHALL continue to run on whichever worker thread holds its inflight CAS; no parked producer SHALL block consumer drain.
- Producers parked on the same consumer mailbox SHALL be unparked one at a time as items pop, in the order they parked (FIFO unpark fairness).

This requirement is in addition to and does not modify the "Sticky per-segment ordering" requirement from the umbrella `operative-architecture` capability spec; it constrains how Block must integrate with that invariant.

#### Scenario: Parked producer does not block its consumer's drain
- **WHEN** Producer A's thread is parked because Consumer X's mailbox is full, and the worker pool has one other thread that picks up X's drain
- **THEN** X's drain SHALL run to completion (popping items, calling `Operation::run`); each pop SHALL unpark one waiting producer (FIFO order); A SHALL eventually be unparked and complete its push

#### Scenario: FIFO unpark order
- **WHEN** Producers A, B, C park in that order on the same full consumer mailbox
- **THEN** the consumer's first three pops SHALL unpark A, B, C respectively (best-effort under MPMC contention; the *intent* is FIFO order via a `VecDeque<Unparker>` implementation)

### Requirement: Mailbox-full handling does not stall sibling Operatives at the entry edge under default policy

Under the default entry-channel policy (`DropNewest`), the SDK's wire-reader thread (the per-stream loop in `command/streams_connect.rs`) SHALL NOT block on a full mailbox. The frame's typed payload SHALL be discarded; the loop SHALL immediately attempt to read the next frame from the wire.

If an executor opts into `OverflowPolicy::Block` on an entry channel by explicit `.on_full(OverflowPolicy::Block)` declaration, the wire-reader thread MAY park while pushing to that channel's consumer mailbox. The SDK SHALL document this co-tenancy hazard in `docs/operative_architecture.md` and `docs/sdk_protocol_spec.md`. No runtime mechanism prevents an executor from making this choice.

#### Scenario: Default entry policy keeps wire reader running
- **WHEN** Operative A's entry mailbox is full under default `DropNewest` policy and Operative B (sharing the physical wire connection) has frames pending
- **THEN** the wire reader SHALL discard the frame addressed to A and continue dispatching the next frame; Operative B's frames SHALL not be delayed by A's saturation

#### Scenario: Opt-in entry Block opts into co-tenancy hazard
- **WHEN** a Operative declares `c.input::<Vec<u8>>("x").on_full(OverflowPolicy::Block).into::<S>()` and the entry mailbox saturates
- **THEN** the wire-reader thread SHALL park on the push; Operative B sharing the same connection SHALL NOT receive new frames until the parked push completes (this is documented behavior under explicit opt-in, not a defect)

### Requirement: Operation declares Change associated type and apply_change function (Rust SDK)

The Rust SDK's `Operation` trait SHALL declare an associated `type Change: flezha_executor::state::Change` with default `flezha_executor::state::NoChange`. The `Change` trait SHALL bound on `Serialize + DeserializeOwned + Send + Sync + 'static` and SHALL declare `fn coalesce_key(&self) -> Option<CoalesceKey>` defaulting to `None`.

The `Operation` trait SHALL declare a function `fn apply_change(state: &mut Self::State, change: &Self::Change)` whose default implementation panics with a descriptive error string for any `Change` other than `NoChange`, and is a no-op for `NoChange`. Operations declaring a non-`NoChange` Change SHALL override `apply_change` to make their state mutation deterministic.

`apply_change` SHALL be deterministic: replay equality is required. The Rust SDK SHALL NOT enforce purity in the type system; instead, `flezha_executor::testing::OperationTestHarness::<S>::replay_equals` SHALL be the determinism gate exercised by user tests.

#### Scenario: Default Change is NoChange skips persistence machinery

- **WHEN** a Rust Operation does not declare its `type Change` and the runtime constructs a Segment for it
- **THEN** `Operation::Change` SHALL resolve to `NoChange`, the runtime SHALL detect this via `TypeId::of::<S::Change>() == TypeId::of::<NoChange>()` at operative-build time and SHALL NOT register a flush buffer or invoke the per-Segment flusher for this Segment

#### Scenario: User-defined Change opts the Operation into persistence

- **WHEN** a Rust Operation declares `type Change = MyChange` (any non-`NoChange` user type implementing the `Change` trait)
- **THEN** the runtime SHALL register a typed flush buffer (`Vec<MyChange>`) on each Segment of that Operation and SHALL include those Segments in the per-process flusher's drain rotation

#### Scenario: Default apply_change panics on non-NoChange Change without override

- **WHEN** a Rust Operation declares `type Change = MyChange` but does NOT override `apply_change`
- **THEN** the operative registration step SHALL succeed (the trait default impl exists) but the first `state.apply(MyChange::Foo)` call SHALL panic with a descriptive error pointing the user at the missing override

#### Scenario: Determinism verified by replay_equals

- **WHEN** a test invokes `OperationTestHarness::<S>::new().apply_inputs(inputs).replay_equals()` against a deterministic `apply_change`
- **THEN** the returned `Result<(), ReplayMismatch>` SHALL be `Ok(())` and the test SHALL pass; for a non-deterministic `apply_change` (e.g. one that reads `SystemTime::now()`), the result SHALL be `Err(ReplayMismatch { ... })` carrying serialized expected and actual states

### Requirement: Operation declares Clock associated type with default WallClock (Rust SDK)

The Rust SDK's `Operation` trait SHALL declare an associated `type Clock: flezha_executor::state::Clock` with default `WallClock`. `Clock` SHALL be a sealed trait with v1 implementations `WallClock`, `LogicalClock`, and `EventTimeFrom<F>`. v1 of the Rust SDK SHALL fully wire only `WallClock`; Operations declaring a non-default Clock SHALL fail Operative startup with `OperativeStartError::ClockNotImplementedYet { clock_type }` until a follow-up change wires `LogicalClock` and `EventTimeFrom`.

#### Scenario: Default Clock is WallClock

- **WHEN** a Rust Operation does not declare its `type Clock`
- **THEN** `Operation::Clock` SHALL resolve to `WallClock` and Operative startup SHALL succeed

#### Scenario: Non-WallClock fails startup in v1

- **WHEN** a Rust Operation declares `type Clock = LogicalClock`
- **THEN** `register_unbounded::<R>()` SHALL fail with `OperativeStartError::ClockNotImplementedYet { clock_type: "LogicalClock" }` and the operative SHALL NOT be registered

### Requirement: Operation declares Config associated type with typed constructor (Rust SDK)

The Rust SDK's `Operation` trait SHALL declare an associated `type Config: DeserializeOwned + Send + Sync + 'static` with default `()`, and a constructor `fn from_config(cfg: Self::Config) -> Self`. When `Config = ()` (the default), `from_config(())` SHALL invoke `Self::default()` (requiring `Operation: Default` for unit-Config operations; this is satisfied by the existing `operative-mvp-rust` requirement).

#### Scenario: Default unit Config

- **WHEN** a Rust Operation does not declare `type Config`
- **THEN** the runtime SHALL invoke `Operation::default()` (via the trait default's `from_config(())`) at Segment init

#### Scenario: User-defined typed Config

- **WHEN** a Rust Operation declares `type Config = MyConfig` and the operative-start metadata carries matching bytes
- **THEN** the runtime SHALL deserialize the bytes into `MyConfig` (postcard) and SHALL invoke `MyOperation::from_config(cfg)` to construct the Operation at Segment init; deserialization failure SHALL produce `OperativeStartError::ConfigDeserializeFailed { operation, source }`

### Requirement: Operation declares SCHEMA_VERSION const

The Rust SDK's `Operation` trait SHALL declare `const SCHEMA_VERSION: u32 = 1`. The runtime SHALL stamp this value into the 16-byte header of every persisted Change record for this Operation. Users SHALL bump `SCHEMA_VERSION` when they make a non-additive Change-schema edit (e.g. removing or renaming a Change variant).

#### Scenario: Default SCHEMA_VERSION is 1

- **WHEN** a Rust Operation does not declare `SCHEMA_VERSION`
- **THEN** the trait default `1` SHALL be inherited and stamped into headers

### Requirement: Operation declares compact_after_bytes per-Segment WAL threshold (Rust SDK)

The Rust SDK's `Operation` trait SHALL declare `fn compact_after_bytes() -> usize { 64 * 1024 * 1024 }` (default 64 MiB). The runtime SHALL report this value to the agent at operative startup in the existing operative-start metadata so the agent's compaction trigger uses the per-Operation value.

#### Scenario: Default 64 MiB threshold reported

- **WHEN** a Rust Operation does not override `compact_after_bytes`
- **THEN** the operative-start metadata SHALL carry `compact_after_bytes = 67108864` for this Operation

#### Scenario: Per-Operation override propagated to agent

- **WHEN** a Rust Operation declares `fn compact_after_bytes() -> usize { 16 * 1024 * 1024 }`
- **THEN** the operative-start metadata SHALL carry `compact_after_bytes = 16777216` for this Operation

### Requirement: Operation run receives StateHandle, not direct mutable reference (Rust SDK)

The Rust SDK's `Operation::run` SHALL receive `state: StateHandle<'_, Self>` instead of `state: &mut Self::State`. `StateHandle<'a, S: Operation>` SHALL implement `std::ops::Deref<Target = S::State>` for read access and SHALL expose a single mutation method `apply(&mut self, change: S::Change)` that runs `S::apply_change(state, &change)` and pushes the Change onto the per-Segment flush buffer. `StateHandle` SHALL NOT implement `DerefMut`.

#### Scenario: Direct mutation does not compile

- **WHEN** a Rust Operation's `run` body attempts `state.field = value` directly via `StateHandle`
- **THEN** the Rust compiler SHALL fail with a "trait bound `DerefMut` not satisfied" error and the program MUST NOT build

#### Scenario: Mutation via apply succeeds, runs apply_change, and records

- **WHEN** a Rust Operation's `run` body calls `state.apply(MyChange::Foo)`
- **THEN** `S::apply_change(state, &MyChange::Foo)` SHALL run on the underlying state AND `MyChange::Foo` SHALL be appended to the per-Segment flush buffer (as observed by `OperationTestHarness::recorded_changes()` and by the per-process flusher on its next tick)

#### Scenario: Read access via Deref is allocation-free

- **WHEN** a Rust Operation's `run` body reads `state.some_field`
- **THEN** the read SHALL succeed via `StateHandle`'s `Deref` impl with no Change recording, no buffer mutation, and zero heap allocations

### Requirement: Operative declares PERSISTENCE_ENABLED const, frozen at instance start (Rust SDK)

The Rust SDK's `UnboundedOperative` trait SHALL declare `const PERSISTENCE_ENABLED: bool = true`. The runtime SHALL resolve a per-Operative-instance `persistence_enabled: bool` at `start_operative(factory_id, instance_id)` time using:

1. If the operative-start metadata from the agent contains `operative_persistence.<OperativeClass>` with a boolean value, use that.
2. Otherwise, use the value of `UnboundedOperative::PERSISTENCE_ENABLED`.
3. The default of the trait const is `true`.

The resolved value SHALL be bound to the `UnboundedInstance` and SHALL be propagated to every `SegmentSlot` constructed for that instance. Subsequent changes to the executor-side metadata or to the `UnboundedOperative::PERSISTENCE_ENABLED` const (via SDK redeploy) SHALL NOT affect already-running instances; new values take effect on the next `stop_operative` + `start_operative` cycle for that instance.

#### Scenario: Default const value is true

- **WHEN** a Operative does not declare `PERSISTENCE_ENABLED` and no executor override is present in operative-start metadata
- **THEN** `UnboundedInstance::persistence_enabled()` SHALL return `true`

#### Scenario: Const-declared opt-out is honored

- **WHEN** `MarketOperative` declares `const PERSISTENCE_ENABLED: bool = false` and no executor override is present
- **THEN** `UnboundedInstance::persistence_enabled()` SHALL return `false`; the per-Process flusher SHALL drain-and-discard for this instance's Segments; `stub_send_frame` (or its real counterpart) SHALL NOT be invoked for this instance

#### Scenario: Executor override takes precedence over const

- **WHEN** `MarketOperative` declares `const PERSISTENCE_ENABLED: bool = true` AND the operative-start metadata carries `operative_persistence.MarketOperative = false`
- **THEN** `UnboundedInstance::persistence_enabled()` SHALL return `false`

#### Scenario: Resolved value frozen at instance start

- **WHEN** a `UnboundedInstance` is constructed with resolved `persistence_enabled = true`, AND the executor subsequently issues a runtime config change setting `operative_persistence.<OperativeClass> = false`
- **THEN** the running `UnboundedInstance` SHALL continue with `persistence_enabled = true`; the new value SHALL take effect only after `stop_operative(instance_id)` and a subsequent `start_operative(...)` invocation

### Requirement: init_state accepts optional restoration payload (Rust SDK)

The Rust SDK's `Operation::init_state` signature SHALL be `fn init_state(operation_key: Option<&str>, init_payload: Option<RestoreInitPayload<'_>>) -> Self::State`. When `init_payload = None`, `init_state` SHALL behave as a fresh-start constructor. When `init_payload = Some(payload)`, the user MAY inspect or ignore the payload bytes; the runtime SHALL automatically apply the payload (snapshot decode + change-log fold via `Operation::apply_change`) AFTER `init_state` returns, replacing the user-constructed state with the restored state.

#### Scenario: Cold start passes None

- **WHEN** a Rust Segment is constructed for the first time with no prior persisted state on the agent
- **THEN** the runtime SHALL invoke `init_state(operation_key, None)`

#### Scenario: Restart passes Some payload

- **WHEN** a Rust Segment is reconnected and the agent has snapshot or WAL bytes for it
- **THEN** the runtime SHALL invoke `init_state(operation_key, Some(payload))` AND, after `init_state` returns, the runtime SHALL decode `payload.snapshot_bytes` into `Self::State` (replacing the user's value) if the snapshot is non-empty, AND the runtime SHALL fold `payload.change_log_bytes` via `Operation::apply_change` in seq order

#### Scenario: User can inspect payload without applying

- **WHEN** a Rust Operation's `init_state` reads `payload.change_log_bytes.len()` for instrumentation purposes
- **THEN** the read SHALL succeed; the runtime SHALL still apply the payload after `init_state` returns; the user's instrumentation SHALL not affect the restored state

### Requirement: Per-Segment flush buffer registered for non-NoChange Operations

The Rust SDK's `SegmentSlot` SHALL hold a type-erased flush buffer (`Arc<Mutex<Box<dyn FlushableBuffer>>>`) for any Segment whose Operation's `Change` type is not `NoChange`. The buffer SHALL be a `Vec<S::Change>` initially empty; `state.apply(...)` SHALL push onto it. The per-process flusher SHALL acquire the Mutex on each linger tick to drain the buffer.

For Segments whose Operation's `Change == NoChange`, the runtime SHALL NOT allocate a flush buffer or register the Segment with the flusher.

#### Scenario: NoChange Operation Segments are not registered with the flusher

- **WHEN** the runtime constructs a Segment for a Operation with `Change = NoChange`
- **THEN** the `SegmentFlusher`'s registry SHALL NOT contain an entry for this Segment

#### Scenario: Non-NoChange Operation Segments are registered

- **WHEN** the runtime constructs a Segment for a Operation with `Change = MyChange`
- **THEN** the `SegmentFlusher`'s registry SHALL contain an entry keyed by `(operative_instance_id, operation_id, segment_key)` with the typed buffer reference, the resolved `persistence_enabled` flag, the `change_seq: AtomicU64`, and the wire-frame send fn pointer

### Requirement: Four-layer abstraction with defined responsibilities

The SDK SHALL expose a four-layer abstraction comprising Operative, Operation, Segment, and Runner. Each layer SHALL have the following exclusive responsibilities and no others SHALL be implicitly added:

- **Operative** — a user-defined struct implementing the `UnboundedOperative` trait. Spawned by the agent once per `flz operative start` invocation. Owns the channel topology declaration and the registered Operations. Holds no domain state of its own.
- **Operation** — a user-defined struct implementing the `Operation` trait. Defines a stage's typed input, typed output, state type, run logic, and segmenting strategy. Holds no runtime state of its own; acts as the template for Segments.
- **Segment** — a runtime instance of a Operation, identified by an optional string key. Holds exactly one State value, exactly one mailbox, and exactly one `inflight` flag. Created by the SDK at operative startup; not user-instantiable.
- **Runner** — an OS thread in the SDK's worker pool. Drains Segment mailboxes. Not user-visible at the API surface; pool size is configured via the existing `FLEZHA_WORKER_POOL_SIZE` mechanism.

#### Scenario: Each layer has a distinct programmer-visible role
- **WHEN** a developer reads the public API of the new SDK
- **THEN** they SHALL find a `UnboundedOperative` trait, a `Operation` trait, but no `Segment` trait or `Runner` trait — Segments and Runners are runtime artifacts created by the SDK, not types the user implements

#### Scenario: A Operative holds no domain state
- **WHEN** a `UnboundedOperative` impl is examined
- **THEN** the trait MUST NOT include a state-init or state-handle method; all stateful behavior MUST live on Operations

### Requirement: Static and single segmenting modes are supported in v1

A Operation SHALL run in one of exactly three segmenting modes (the v1 title is retained for MODIFIED-matcher binding; the body below replaces the v1 wording end-to-end and adds dynamic mode):

- **Single segment (default)**: when the Operation implements neither `operation_keys()` nor `operation_key()` (i.e. `OPERATION_KEY_OVERRIDDEN = false` and `operation_keys()` returns empty). The runtime SHALL create exactly one Segment with `operation_key = None` at operative startup, and SHALL call `Operation::init_state(None)` to initialize its state.
- **Static segments**: when the Operation implements `fn operation_keys() -> Vec<&'static str>` returning a non-empty list. The runtime SHALL create one Segment per declared key at operative startup, and SHALL call `Operation::init_state(Some(key))` for each.
- **Dynamic segments**: when the Operation overrides `operation_key(input: &Input) -> Option<String>` AND sets `OPERATION_KEY_OVERRIDDEN = true` AND leaves `operation_keys()` empty. The runtime SHALL own zero Segments for the Operation at operative startup; Segments SHALL be created lazily on the first input whose `operation_key(&input)` returns `Some(k)` (see "Dynamic-mode lazy segment creation"). Inputs whose `operation_key(&input)` returns `None` SHALL be dropped without allocating a Segment; the per-Operation `inputs_dropped_no_key_total` counter SHALL be incremented.

The Operative startup SHALL fail with a clear error message if a Operation:

- declares both `operation_keys()` and `operation_key()` simultaneously (i.e. `operation_keys()` non-empty AND `OPERATION_KEY_OVERRIDDEN = true`).

#### Scenario: Single segment for a Operation with no segmenting declared
- **WHEN** a Operation implements neither `operation_keys()` nor `operation_key()` and the Operative is started
- **THEN** the SDK SHALL create exactly one Segment for that Operation with `operation_key = None`, and `Operation::init_state(None)` SHALL be called exactly once during startup

#### Scenario: Static segments created at operative startup
- **WHEN** a Operation declares `operation_keys() = ["1m", "1h", "1d", "1w"]` and the Operative is started
- **THEN** the SDK SHALL create exactly four Segments for that Operation at startup, with `operation_key = Some("1m")`, `Some("1h")`, `Some("1d")`, `Some("1w")` respectively; `init_state` SHALL be called exactly once for each

#### Scenario: Dynamic-mode declaration is accepted at operative startup
- **WHEN** a Operation overrides `operation_key()` and sets `OPERATION_KEY_OVERRIDDEN = true` while leaving `operation_keys()` empty
- **THEN** the Operative startup SHALL succeed and the Operation SHALL be registered in dynamic mode with zero Segments

#### Scenario: Both operation_keys and operation_key declared is rejected
- **WHEN** a Operation returns a non-empty `operation_keys()` AND sets `OPERATION_KEY_OVERRIDDEN = true`
- **THEN** Operative startup SHALL fail with an error naming the conflicting Operation

### Requirement: Sticky per-segment ordering

For any single Segment, two invocations of its Operation's `run(...)` method SHALL NOT execute concurrently, and invocations SHALL observe their inputs in the order those inputs were enqueued onto the Segment's mailbox. This invariant MUST hold across all dispatch modes, all runner pool sizes, and all transport types.

#### Scenario: Same segment is never concurrent
- **WHEN** 10,000 inputs targeting one Segment arrive on its mailbox while the runner pool has 16 threads
- **THEN** at any wall-clock instant, at most one runner thread SHALL be inside that Segment's `run(...)` method

#### Scenario: Different segments of the same operation run in parallel
- **WHEN** inputs arrive simultaneously for two distinct Segments of the same Operation (for example `("ohlc","1m")` and `("ohlc","1h")`) and the runner pool has ≥ 2 threads
- **THEN** the two Segments' `run(...)` methods MAY execute concurrently on different threads

#### Scenario: Per-segment input order is preserved
- **WHEN** inputs `a`, `b`, `c` are enqueued onto one Segment's mailbox in that order with an append-to-state Operation
- **THEN** the Segment's State SHALL end with `[a, b, c]` in exactly that order

### Requirement: Typed channels with compile-time wiring checks

All inter-Operation and Operation-to-plugin routing SHALL go through Channels declared on the Operative. Each Channel SHALL carry a phantom type parameter `Channel<T>`. The Operative's channel-wiring API SHALL refuse to compile in any of these cases:

- A producer Operation whose `Output` associated type is not `T` is connected via `.from::<X>()` to a `Channel<T>`.
- A consumer Operation whose `Input` associated type is not `T` is connected via `.into::<X>()` to a `Channel<T>`.
- A Channel is declared with two `.from::<...>()` clauses (multi-producer is forbidden in v1).

#### Scenario: Type-mismatched producer fails to compile
- **WHEN** a Operation with `type Output = TradeMsg` is wired into a `Channel<Candle>` via `.from::<That>()`
- **THEN** the Rust compiler SHALL emit a trait-bound error and the program MUST NOT build

#### Scenario: Type-mismatched consumer fails to compile
- **WHEN** a Operation with `type Input = TradeMsg` is wired as a consumer of a `Channel<Candle>` via `.into::<That>()`
- **THEN** the Rust compiler SHALL emit a trait-bound error and the program MUST NOT build

#### Scenario: Multi-producer channel fails to compile
- **WHEN** the Operative's `channels()` declaration contains two `.from::<X>()` calls on the same channel
- **THEN** the program MUST fail at compile time (or at startup time if compile-time enforcement is impossible for the chosen builder shape)

### Requirement: Per-operative channel namespace

Channel names SHALL be scoped to a single Operative instance. Two Operative instances (for example the BTC Operative and the ETH Operative) SHALL each have their own independent channel namespace; a channel named `"trade.raw"` on one Operative SHALL be a distinct logical channel from a channel of the same name on another Operative.

#### Scenario: Same-named channels on different operatives are independent
- **WHEN** the BTC Operative declares `c.input::<Trade>("trade.raw")` and the ETH Operative also declares `c.input::<Trade>("trade.raw")`
- **THEN** an input pushed to the BTC Operative's `"trade.raw"` channel SHALL NOT be observable by the ETH Operative's Operations, and vice versa

### Requirement: Hot-path channel dispatch uses interned u32 IDs

At Operative startup, channel names SHALL be interned to `u32` IDs once. Runtime input-routing decisions inside the SDK hot path SHALL use the integer ID, not the string name. Channel-name strings SHALL appear only in: the user-facing API, registration time, and the on-the-wire metadata field crossing the NIF boundary.

#### Scenario: Hot path is allocation-free with respect to channel names
- **WHEN** an input is dispatched from a Operative's input channel through to a Segment's mailbox during steady-state operation
- **THEN** no per-message allocation of `String` for the channel name SHALL be observable in profiling

### Requirement: Inter-operation transport is in-process zero-copy

When a Channel connects two Operations inside the same Operative, the SDK SHALL transport values as `Arc<T>` references between Segments, performing no serialization, no allocation of an output buffer, and no NIF-boundary crossing for that transport.

#### Scenario: Pipeline between two operations does not serialize
- **WHEN** OhlcOperation emits a `Candle` to a channel that IndicatorOperation subscribes to (and the channel has no `to_plugin(...)` clause)
- **THEN** the IndicatorOperation's `run(...)` SHALL receive the same `Arc<Candle>` instance OhlcOperation emitted (or an `Arc` to byte-identical content with no intermediate serialization step)

### Requirement: Plugin emission semantics

Channels declared with `.toPlugin("name")` (pure-plugin) and channels declared with `.alsoToPluginWith("name", serializer)` (hybrid) — in **either** the Rust SDK (`.to_plugin(...)` / `.also_to_plugin_with(...)`) or the Java SDK (`.toPlugin(...)` / `.alsoToPluginWith(...)`) — SHALL both route a value through the agent's plugin layer using the channel name as the wire identifier. The numeric `plugin_group: u8` metadata field is removed from the wire format. The replacement metadata field SHALL be `channel: String` — the literal channel name as a string in v1 (see `design.md` Decision 15). A per-channel integer fast-path (sending an interned `u32` instead of the string) MAY be added by a future change as an opt-in alternative, but SHALL NOT be in v1.

For pure-plugin channels, the wire bytes are produced by the channel's `pluginSerializer`/`plugin_serializer` (set by `toPlugin(...)` / `to_plugin(...)`). For hybrid channels, the wire bytes are produced by the channel's `pluginSink.serializer` / `plugin_sink.serializer` (set by `alsoToPluginWith(...)` / `also_to_plugin_with(...)`). In both cases the byte buffer SHALL be sent through the same wire framing path (`channel: String` metadata + payload bytes) so the agent's plugin layer SHALL NOT distinguish between pure and hybrid channels at the wire — both arrive identically and route by channel name. **Furthermore, the agent's plugin layer SHALL NOT distinguish between Rust-SDK and Java-SDK frames** — hybrid frames produced by either SDK are byte-indistinguishable for the same plugin name and serializer-output bytes.

#### Scenario: Pure-plugin emit reaches the agent's plugin layer
- **WHEN** a Operation publishes to a channel declared with `.to_plugin("ohlc_kafka")`
- **THEN** the agent's `ohlc_kafka` plugin SHALL receive the bytes with `channel = <channel name>` in the metadata

#### Scenario: Hybrid emit reaches the agent's plugin layer with identical framing
- **WHEN** a Operation publishes to a channel declared with `.also_to_plugin_with("indicators", sma_to_pb_bytes)` on a channel that also has inter-operation consumers
- **THEN** the agent's `indicators` plugin SHALL receive the serializer-output bytes with `channel = <channel name>` in the metadata
- **AND** the wire frame SHALL be byte-indistinguishable from a pure-plugin channel's frame for the same plugin name and bytes

#### Scenario: Two channels sharing a plugin name fan in at the agent
- **WHEN** a Operative declares two hybrid channels (e.g. `sma_out` with `also_to_plugin_with("indicators", sma_serde)` and `ema_out` with `also_to_plugin_with("indicators", ema_serde)`)
- **THEN** the agent's `indicators` plugin SHALL receive frames from both channels, distinguishable only by the `channel: String` metadata key
- **AND** the SDK's topology builder SHALL NOT reject this declaration as a `MultiProducer` error, because each channel still has exactly one producer Operation

#### Scenario: Plugin subscribes by channel name
- **WHEN** an agent-side plugin is configured to consume outputs from `"ohlc_kafka"`
- **THEN** that plugin SHALL receive every message whose channel name matches `"ohlc_kafka"`, regardless of which Operation produced it

#### Scenario: Hybrid emit reaches the agent's plugin layer with identical framing across SDKs
- **WHEN** a Rust Operation publishes via `.also_to_plugin_with("indicators", sma_to_pb_bytes)` and a Java Operation publishes via `.alsoToPluginWith("indicators", SmaCodec::toBytes)` with identical serializer output for an identical input value
- **THEN** the agent's `indicators` plugin SHALL receive byte-equal wire frames from both SDKs (the only on-wire difference being TCP/UDS interleaving — the framed payload bytes are identical)
- **AND** the SDK's topology builder on either side SHALL NOT reject either declaration as a `MultiProducer` / multi-producer error, because each channel still has exactly one producer Operation within its own SDK

### Requirement: OperationCtx emit semantics

A Operation's `run` method SHALL receive a mutable reference to a `OperationCtx`. The Operation SHALL emit zero, one, or more outputs by calling `ctx.emit::<C>(value)` for each output, where `C` is the channel-payload type (which the compiler resolves to the matching channel via the wiring declaration). For outputs that need a string-keyed user metadata bag attached, the Operation SHALL call `ctx.emit_with_meta::<C>(value, HashMap<String, String>)` instead. The `run` method's return type SHALL be `Result<(), OperationError>` for success / hard-error signaling. Producing no output SHALL be expressed by simply not calling `ctx.emit` (or `ctx.emit_with_meta`); there SHALL be no explicit "no-output" return variant.

`ctx.emit_with_meta` SHALL be valid only for channels declared with `.to_plugin("name")`. Calling `emit_with_meta` on a channel that flows to another Operation inside the same Operative SHALL return a `OperationError` and SHALL NOT propagate the value or the metadata. The error message SHALL name the offending channel and instruct the user to either move the metadata into the typed value or add `.to_plugin(...)` to the channel.

When `emit_with_meta` is used on a plugin-bound channel, the supplied `HashMap<String, String>` SHALL be attached to the output as it crosses the NIF boundary using the existing wire-format `[key_len][val_len][key][value]` block. The channel name (or its interned `u32` equivalent) SHALL be one of the keys; the user-supplied keys SHALL be appended without further validation. Reserved key names (e.g. `channel`) SHALL NOT be overridable via the user-supplied map; an attempt to set one SHALL be silently dropped during framing.

#### Scenario: Fan-out via multiple emit calls
- **WHEN** a ParseOperation's `run` calls `ctx.emit::<ParsedTrade>(p)` followed by `ctx.emit::<RawSummary>(s)` and returns `Ok(())`
- **THEN** both downstream channels SHALL receive their respective values, and the order of emission SHALL match the order of the `emit` calls

#### Scenario: No-output via no-call
- **WHEN** a Operation's `run` returns `Ok(())` without calling `ctx.emit`
- **THEN** the SDK SHALL treat the input as fully processed with no downstream propagation, with no error and no special enum variant required

#### Scenario: Hard error via Err return
- **WHEN** a Operation's `run` returns `Err(e)`
- **THEN** the SDK SHALL log the error against the (operative, operation, segment) triple and SHALL NOT propagate any partial output that may have been emitted before the error

#### Scenario: emit_with_meta on a plugin-bound channel
- **WHEN** a Operation's `run` calls `ctx.emit_with_meta::<Candle>(c, {"trace_id": "abc123"})` on a channel declared with `.to_plugin("ohlc_kafka")`
- **THEN** the message reaching the agent SHALL carry the channel name AND the `trace_id: "abc123"` key in its metadata block, and the agent's plugin SHALL receive both

#### Scenario: emit_with_meta on an inter-operation channel returns error
- **WHEN** a Operation's `run` calls `ctx.emit_with_meta::<ParsedTrade>(p, {"trace_id": "abc123"})` on a channel that flows to another Operation inside the same Operative (no `.to_plugin(...)` clause)
- **THEN** the call SHALL return `Err(OperationError::MetadataNotAllowedOnInterOperationChannel { channel })`, and the value `p` SHALL NOT be propagated to the downstream Operation

### Requirement: Hybrid channel builder method

The Rust SDK SHALL expose a method `also_to_plugin_with<F>(plugin_name: &'static str, serializer: F) -> Self` on `ChannelDeclWithProducer<T>` (in `sdk/rust/src/channel/builder.rs`) where `F: Fn(&T) -> Vec<u8> + Send + Sync + 'static`. Calling this method SHALL leave the channel's `kind = ChannelKind::InterOperation` and SHALL attach a plugin sink (the named plugin and the user-supplied serializer, type-erased to `PluginSerializeFn`) alongside the channel's existing consumer fan-out. After the call, further `.into::<S>()` consumer wiring SHALL chain naturally — the method's return type is `Self`.

The plugin sink SHALL be stored on the channel descriptor as a `plugin_sink: Option<PluginSink>` field where `PluginSink { name: &'static str, serializer: PluginSerializeFn }`. Pure-`PluginBound` channels (produced by `to_plugin(...)`) SHALL continue to populate the existing `plugin_name` and `plugin_serializer` fields and SHALL NOT populate `plugin_sink`. Hybrid channels (`InterOperation` + `also_to_plugin_with`) SHALL populate `plugin_sink` and SHALL NOT populate `plugin_name`/`plugin_serializer`. The two paths SHALL stay textually distinct so existing pattern matches over `ChannelKind` continue to compile unchanged.

#### Scenario: Hybrid wiring is accepted at registration
- **WHEN** a Operative declares `c.channel::<SmaValue>("sma_out").from::<SmaOperation>().also_to_plugin_with("indicators", sma_to_pb_bytes).into::<BollingerOperation>()`
- **THEN** `register_unbounded::<MarketOperative>()` SHALL succeed
- **AND** the resulting `ChannelDescriptor` for `"sma_out"` SHALL have `kind = InterOperation`, exactly one entry in `consumers` (the `BollingerOperation` operation id), and `plugin_sink = Some(PluginSink { name: "indicators", serializer: <fn-ptr> })`

#### Scenario: also_to_plugin_with chains in either order with into::<S>()
- **WHEN** a Operative declares `c.channel::<EmaValue>("ema_out").from::<EmaOperation>().into::<MacdOperation>().also_to_plugin_with("indicators", ema_to_pb_bytes)`
- **THEN** `register_unbounded::<MarketOperative>()` SHALL succeed
- **AND** the resulting descriptor SHALL be identical to the same declaration with the calls swapped (`.also_to_plugin_with(...).into::<MacdOperation>()`)

#### Scenario: Calling also_to_plugin_with twice on the same channel is a topology error
- **WHEN** a Operative calls `c.channel::<T>("foo").from::<S>().also_to_plugin_with("p1", f1).also_to_plugin_with("p2", f2)`
- **THEN** `register_unbounded::<R>()` SHALL panic with `BuildTopologyError::DuplicatePluginSink { channel: "foo" }`
- **AND** the offending channel name SHALL appear verbatim in the error message

#### Scenario: also_to_plugin_with is unreachable after to_plugin
- **WHEN** a Operative attempts `c.channel::<Vec<u8>>("foo").from::<S>().to_plugin("p").also_to_plugin_with(...)`
- **THEN** the code SHALL fail to compile because `to_plugin` returns `Channel<T>` (a finalized handle with no further methods)
- **AND** a trybuild test under `sdk/rust/tests/build_failures/` SHALL pin this as a stable compile error

### Requirement: Hybrid channel emission — consumer fan-out then single serializer call

When a Operation's `Operation::run` calls `ctx.emit(value)` on a hybrid channel (`kind = InterOperation, plugin_sink = Some(_)`), the dispatcher SHALL execute the following sequence on the producer thread:

1. Fan out the typed `Arc<dyn Any + Send + Sync>` value to every consumer mailbox listed in the channel's `consumers` per the channel's `OverflowPolicy`. This step SHALL complete (modulo `OverflowPolicy::Block` parking) before step 2 begins.
2. If `plugin_sink` is `Some(sink)`, invoke `sink.serializer(value.as_ref())` exactly once inside `std::panic::catch_unwind(AssertUnwindSafe(...))`. On `Ok(bytes)`, send the bytes through the existing plugin-wire framing path (`channel: String` wire-metadata key set to the channel name; `[key_len][val_len][key][value]` block format). On `Err(panic)`, return `OperationError::PluginSerializerPanic { channel: <channel name>, message: <formatted panic payload> }` to the producing Operation's `ctx.emit` call without emitting any plugin bytes.

The dispatcher thread SHALL remain alive across a serializer panic. Subsequent emits SHALL proceed normally; only the producing Operation sees the per-emit error.

The serializer SHALL run exactly once per `ctx.emit` invocation regardless of consumer count. The byte buffer produced by the serializer SHALL NOT be cached or reused across emits.

#### Scenario: Hybrid emit fans out typed value to all consumers and bytes to plugin
- **WHEN** a producer Operation's `run` calls `ctx.emit(value)` on a channel wired to `[ConsumerA, ConsumerB]` and with `plugin_sink = Some(("indicators", serializer))`
- **THEN** `ConsumerA::run` and `ConsumerB::run` SHALL each be invoked with the typed `value` (cloned per the existing inter-operation delivery semantics)
- **AND** the agent-side plugin "indicators" SHALL receive a single wire frame with bytes equal to `serializer(&value)` and `channel = <channel name>` in the metadata

#### Scenario: Single serializer call regardless of consumer count
- **WHEN** a hybrid channel has N inter-operation consumers and the producer emits one value
- **THEN** the serializer SHALL be invoked exactly once for that emit
- **AND** N is unconstrained (1, 3, 100 — same single-call guarantee)

#### Scenario: Panicking serializer surfaces as OperationError without crashing the dispatcher
- **WHEN** the serializer panics on a particular input
- **THEN** the inter-operation consumers SHALL still receive the typed value (consumer fan-out completed before serialization)
- **AND** `ctx.emit(...)` SHALL return `Err(OperationError::PluginSerializerPanic { channel, message })`
- **AND** the dispatcher thread SHALL remain alive — a subsequent `ctx.emit` call SHALL execute normally

#### Scenario: Serializer runs after consumer fan-out, not before
- **WHEN** an `OverflowPolicy::Block` consumer mailbox is full and the producer emits on a hybrid channel
- **THEN** the producer SHALL park on the consumer mailbox
- **AND** the plugin sink SHALL NOT receive any bytes until the consumer drains and the producer unparks
- **AND** when the producer unparks, the serializer SHALL be invoked exactly once and the plugin sink SHALL receive the corresponding bytes

### Requirement: emit_with_meta is valid on hybrid channels

`OperationCtx::emit_with_meta(value, meta)` SHALL accept the call when either:

- the channel is `kind = PluginBound` (existing rule), OR
- the channel has `plugin_sink = Some(_)` (new rule for hybrid channels).

On a hybrid channel, the inter-operation consumers SHALL receive the typed value with no metadata (metadata is plugin-framing only and has no place in the typed delivery model). The plugin sink SHALL receive bytes (via the channel's serializer) with the supplied `HashMap<String, String>` attached to the wire frame using the existing `[key_len][val_len][key][value]` block format. The reserved `channel: String` key SHALL be set automatically and SHALL NOT be overridable by user-supplied keys; an attempt to set a reserved key SHALL be silently dropped during framing (matches the existing rule for `PluginBound` channels).

On a pure-`InterOperation` channel with no plugin sink, `emit_with_meta` SHALL continue to return `OperationError::MetadataNotAllowedOnInterOperationChannel { channel }` without delivering the value or the metadata.

#### Scenario: emit_with_meta on a hybrid channel forwards metadata to plugin only
- **WHEN** a producer Operation's `run` calls `ctx.emit_with_meta(value, {"trace_id": "abc123"})` on a hybrid channel wired to one consumer and one plugin sink
- **THEN** the consumer SHALL receive the typed `value` with no `trace_id` visible
- **AND** the plugin wire frame SHALL contain `trace_id = "abc123"` plus the implicit `channel = <channel name>` key

#### Scenario: emit_with_meta on a pure inter-operation channel still rejects metadata
- **WHEN** a producer Operation's `run` calls `ctx.emit_with_meta(value, {"trace_id": "abc123"})` on a channel that has consumers but no `plugin_sink`
- **THEN** `ctx.emit_with_meta` SHALL return `OperationError::MetadataNotAllowedOnInterOperationChannel { channel }`
- **AND** neither the consumers nor any plugin SHALL receive the value or the metadata

#### Scenario: Reserved metadata keys are stripped on hybrid channels
- **WHEN** a producer calls `ctx.emit_with_meta(value, {"channel": "spoof", "trace_id": "abc"})` on a hybrid channel
- **THEN** the wire frame SHALL contain `channel = <real channel name>` (the reserved value, not "spoof") and `trace_id = "abc"`

### Requirement: New BuildTopologyError variant — `DuplicatePluginSink`

`BuildTopologyError` (in `sdk/rust/src/channel/builder.rs`) SHALL gain a `DuplicatePluginSink { channel: &'static str }` variant. The variant SHALL be raised at `register_unbounded::<R>()` time when `also_to_plugin_with` is called more than once on the same channel. The first call registers the sink; the second SHALL push the error onto the builder's error vector and SHALL NOT overwrite the previously-registered sink. The aggregate `register_unbounded::<R>()` validation SHALL fail with the standard "topology rejected" message that lists every accumulated `BuildTopologyError`.

#### Scenario: Second also_to_plugin_with call is rejected at registration
- **WHEN** a Operative's `channels(c)` method calls `also_to_plugin_with("p1", f1)` followed by `also_to_plugin_with("p2", f2)` on the same channel
- **THEN** `register_unbounded::<R>()` SHALL panic with the topology error including `DuplicatePluginSink { channel: "<name>" }`
- **AND** the registered topology SHALL be discarded

### Requirement: New OperationError variant — `PluginSerializerPanic`

`OperationError` (in `sdk/rust/src/operation/error.rs`) SHALL gain a `PluginSerializerPanic { channel: String, message: String }` variant. The variant SHALL be returned from `ctx.emit(...)` (and `ctx.emit_with_meta(...)`) on a hybrid channel when the user-supplied serializer panics. `channel` SHALL be the channel name as declared in `ChannelBuilder`; `message` SHALL be the panic payload formatted via the existing panic-message helper (a string description, not the raw `Box<dyn Any>` payload).

The producing Operation's `Operation::run` SHALL receive the error via the normal `Result<(), OperationError>` return path of `ctx.emit`. Returning `Err` from `Operation::run` continues to discard partial emits per the existing trait contract; ignoring the error is also valid and SHALL leave the inter-operation consumers' delivery intact.

#### Scenario: Producer Operation sees PluginSerializerPanic and chooses how to handle it
- **WHEN** a hybrid channel's serializer panics during `ctx.emit(value)`
- **THEN** `ctx.emit(value)` SHALL return `Err(OperationError::PluginSerializerPanic { channel, message })`
- **AND** the producing Operation MAY ignore, log, or `return Err(...)` — all three are valid handlers
- **AND** the inter-operation consumer fan-out SHALL have already completed before the panic was caught

### Requirement: Hybrid channel builder method (Java SDK)

The Java SDK SHALL expose a method `alsoToPluginWith(String pluginName, Function<T, byte[]> serializer) -> ChannelDeclWithProducer<T>` on `ChannelDeclWithProducer<T>` (in `sdk/java/flezha-sdk-api/src/main/java/com/flezha/sdk/channel/ChannelDeclWithProducer.java`). Calling this method SHALL leave the channel's `kind = ChannelKind.INTER_OPERATION` and SHALL attach a plugin sink (the named plugin and the user-supplied serializer, type-erased to `Function<Object, byte[]>`) alongside the channel's existing consumer fan-out. After the call, further `.into(...)` consumer wiring SHALL chain naturally — the method's return type is `ChannelDeclWithProducer<T>`.

The plugin sink SHALL be stored on the channel descriptor as `Optional<PluginSink> pluginSink` where `PluginSink` is a public record `(String name, Function<Object, byte[]> serializer)`. Pure-`PLUGIN_BOUND` channels (produced by `toPlugin(...)`) SHALL continue to populate the existing `pluginName` and `pluginSerializer` fields and SHALL NOT populate `pluginSink`. Hybrid channels (`INTER_OPERATION` + `alsoToPluginWith`) SHALL populate `pluginSink` and SHALL NOT populate `pluginName`/`pluginSerializer`. The two paths SHALL stay textually distinct so existing pattern matches over `ChannelKind` continue to compile unchanged.

#### Scenario: Hybrid wiring is accepted at registration (Java)
- **WHEN** a Operative declares `c.channel(SmaValue.class, "sma_out").from(SmaOperation.class).alsoToPluginWith("indicators", SmaCodec::toBytes).into(BollingerOperation.class)`
- **THEN** `OperativeRegistry.registerUnbounded(MarketOperative.class)` SHALL succeed
- **AND** the resulting `ChannelDescriptor` for `"sma_out"` SHALL have `kind() == INTER_OPERATION`, exactly one entry in `consumers()` (the `BollingerOperation` operation id), and `pluginSink() == Optional.of(new PluginSink("indicators", <fn>))`

#### Scenario: alsoToPluginWith chains in either order with into(...) (Java)
- **WHEN** a Operative declares `c.channel(EmaValue.class, "ema_out").from(EmaOperation.class).into(MacdOperation.class).alsoToPluginWith("indicators", EmaCodec::toBytes)`
- **THEN** `OperativeRegistry.registerUnbounded(MarketOperative.class)` SHALL succeed
- **AND** the resulting descriptor SHALL be identical to the same declaration with the calls swapped (`.alsoToPluginWith(...).into(MacdOperation.class)`)

#### Scenario: alsoToPluginWith is unreachable after toPlugin (Java)
- **WHEN** a Operative attempts `c.channel(byte[].class, "foo").from(S.class).toPlugin("p").alsoToPluginWith(...)`
- **THEN** the code SHALL fail to compile because `toPlugin` returns `Channel<T>` (a finalized handle that exposes no further methods)

### Requirement: Hybrid channel emission — consumer fan-out then single serializer call (Java SDK)

When a Operation's `Operation.run` calls `ctx.emit(value)` on a hybrid channel (`kind == INTER_OPERATION`, `pluginSink.isPresent()`), the dispatcher SHALL execute the following sequence on the producer thread:

1. Fan out the typed value to every consumer mailbox listed in `descriptor.consumers()`. This step SHALL complete before step 2 begins.
2. If `pluginSink.isPresent()`, invoke `pluginSink.get().serializer().apply(value)` exactly once inside `try { ... } catch (RuntimeException | Error e) { ... }`. On normal return, send the bytes through the existing plugin-wire framing path (`channel: String` wire-metadata key set to `descriptor.name()`; `[key_len][val_len][key][value]` block format; via `FrameEncoder.build(...)` and `NativeTransport.streamWrite(...)`). On caught throw, throw `OperationException.PluginSerializerFailure(channel, message, cause)` to the producing Operation's `ctx.emit` call without emitting any plugin bytes. `Throwable` is intentionally NOT caught — `OutOfMemoryError` and similar JVM-level signals propagate.

The dispatcher thread SHALL remain alive across a serializer-thrown exception. Subsequent emits SHALL proceed normally; only the producing Operation sees the per-emit error.

The serializer SHALL run exactly once per `ctx.emit` invocation regardless of consumer count. The byte buffer produced by the serializer SHALL NOT be cached or reused across emits.

#### Scenario: Hybrid emit fans out typed value to all consumers and bytes to plugin (Java)
- **WHEN** a producer Operation's `run` calls `ctx.emit(value)` on a channel wired to `[ConsumerA.class, ConsumerB.class]` with `pluginSink = Optional.of(new PluginSink("indicators", serializer))`
- **THEN** `ConsumerA.run` and `ConsumerB.run` SHALL each be invoked with `value` (passed by reference per existing inter-operation delivery semantics)
- **AND** the agent-side plugin "indicators" SHALL receive a single wire frame whose body bytes equal `serializer.apply(value)` and whose metadata contains `channel = <channel name>`

#### Scenario: Single serializer call regardless of consumer count (Java)
- **WHEN** a hybrid channel has N inter-operation consumers and the producer emits one value
- **THEN** the serializer SHALL be invoked exactly once for that emit
- **AND** N is unconstrained (1, 3, 100 — same single-call guarantee)

#### Scenario: Throwing serializer surfaces as OperationException without crashing the dispatcher (Java)
- **WHEN** the serializer throws `RuntimeException` on a particular input
- **THEN** the inter-operation consumers SHALL still receive the typed value (consumer fan-out completed before serialization)
- **AND** `ctx.emit(...)` SHALL throw `OperationException.PluginSerializerFailure { channel(), getMessage(), getCause() }`
- **AND** the dispatcher thread SHALL remain alive — a subsequent `ctx.emit` call SHALL execute normally

### Requirement: emitWithMeta is valid on hybrid channels (Java SDK)

`OperationCtx.emitWithMeta(value, meta)` SHALL accept the call when either:

- the channel is `kind == PLUGIN_BOUND` (existing rule), OR
- the channel has `pluginSink.isPresent()` (new rule for hybrid channels).

On a hybrid channel, the inter-operation consumers SHALL receive the typed value with no metadata (metadata is plugin-framing only). The plugin sink SHALL receive bytes (via the channel's serializer) with the supplied `Map<String, String>` attached to the wire frame using the existing `[key_len][val_len][key][value]` block format. The reserved `channel` key SHALL be set automatically and SHALL NOT be overridable by user-supplied keys; an attempt to set it SHALL be silently dropped during framing (matches the existing rule for `PLUGIN_BOUND` channels).

On a pure-`INTER_OPERATION` channel with no plugin sink, `emitWithMeta` SHALL continue to throw `OperationException.MetadataNotAllowedOnInterOperationChannel` without delivering the value or the metadata.

#### Scenario: emitWithMeta on a hybrid channel forwards metadata to plugin only (Java)
- **WHEN** a producer Operation's `run` calls `ctx.emitWithMeta(value, Map.of("trace_id", "abc123"))` on a hybrid channel wired to one consumer and one plugin sink
- **THEN** the consumer SHALL receive `value` with no `trace_id` visible
- **AND** the plugin wire frame SHALL contain `trace_id = "abc123"` plus the implicit `channel = <channel name>` key

#### Scenario: emitWithMeta on a pure inter-operation channel still throws (Java)
- **WHEN** a producer Operation's `run` calls `ctx.emitWithMeta(value, Map.of("trace_id", "abc123"))` on a channel that has consumers but no `pluginSink`
- **THEN** `ctx.emitWithMeta` SHALL throw `OperationException.MetadataNotAllowedOnInterOperationChannel` carrying the channel name
- **AND** neither the consumers nor any plugin SHALL receive the value or the metadata

#### Scenario: Reserved metadata keys are stripped on hybrid channels (Java)
- **WHEN** a producer calls `ctx.emitWithMeta(value, Map.of("channel", "spoof", "trace_id", "abc"))` on a hybrid channel
- **THEN** the wire frame SHALL contain `channel = <real channel name>` (the reserved value, not "spoof") and `trace_id = "abc"`

### Requirement: New OperativeConfigurationException factory — `duplicatePluginSink` (Java SDK)

`OperativeConfigurationException` (in `sdk/java/flezha-sdk-api/src/main/java/com/flezha/sdk/operative/OperativeConfigurationException.java`) SHALL gain a static factory `duplicatePluginSink(String channel)` that produces an exception whose message names the offending channel. The factory SHALL be raised at `OperativeRegistry.registerUnbounded(...)` time when `alsoToPluginWith` is called more than once on the same channel. The first call registers the sink; the second SHALL accumulate the exception into the builder's error vector and SHALL NOT overwrite the previously-registered sink. The aggregate `OperativeRegistry.registerUnbounded(...)` validation SHALL throw a `OperativeConfigurationException` listing every accumulated error with the standard "operative topology validation failed" prefix.

#### Scenario: Second alsoToPluginWith call is rejected at registration (Java)
- **WHEN** a Operative's `channels(c)` method calls `c.channel(T.class, "foo").from(S.class).alsoToPluginWith("p1", f1).alsoToPluginWith("p2", f2)`
- **THEN** `OperativeRegistry.registerUnbounded(R.class)` SHALL throw `OperativeConfigurationException` whose message contains both the verb `duplicatePluginSink` (or equivalent diagnostic phrasing) and the channel name `"foo"`
- **AND** the registered topology SHALL be discarded

### Requirement: New OperationException subclass — `PluginSerializerFailure` (Java SDK)

`OperationException` (in `sdk/java/flezha-sdk-api/src/main/java/com/flezha/sdk/operation/OperationException.java`) SHALL gain a nested `final class PluginSerializerFailure extends OperationException`. Constructor: `(String channel, String message, Throwable cause)`. The class SHALL be thrown from `ctx.emit(...)` (and `ctx.emitWithMeta(...)`) on a hybrid channel when the user-supplied serializer throws `RuntimeException` or `Error`. `channel()` accessor SHALL return the channel name as declared in `ChannelBuilder`; `getMessage()` SHALL include the underlying exception's `toString()` and the channel name; `getCause()` SHALL be the original throwable.

The producing Operation's `Operation.run` SHALL receive the exception via the normal `throws OperationException` declaration on `ctx.emit`. Catching the exception and continuing is valid; rethrowing from `Operation.run` continues to discard partial emits per the existing trait contract.

#### Scenario: Producer Operation sees PluginSerializerFailure and chooses how to handle it (Java)
- **WHEN** a hybrid channel's serializer throws during `ctx.emit(value)`
- **THEN** `ctx.emit(value)` SHALL throw `OperationException.PluginSerializerFailure` carrying the channel name and the underlying cause
- **AND** the producing Operation MAY catch (logging or swallowing) or rethrow — both are valid handlers
- **AND** the inter-operation consumer fan-out SHALL have already completed before the exception was thrown

#### Scenario: Throwable causes other than RuntimeException/Error are not caught (Java)
- **WHEN** the serializer throws `OutOfMemoryError`
- **THEN** the dispatcher SHALL NOT catch it — the JVM's normal escalation path applies (the worker thread terminates, the executor's uncaught-exception handler runs)
- **AND** the consumer fan-out for that emit SHALL have already completed

### Requirement: Entry channel value types limited to bytes or string in v1

Channels declared via `c.input::<T>("name")` (entry channels fed by the agent over the wire) SHALL accept only the following payload types in v1:

- `Channel<Vec<u8>>` — bytes are delivered to the consuming Operation unchanged.
- `Channel<String>` — the SDK SHALL perform UTF-8 decoding at the input edge. An invalid-UTF-8 byte sequence SHALL be logged against the (operative, channel) pair and the input SHALL be dropped without invoking the consuming Operation.

Any other `T` on an entry channel SHALL fail at Operative startup with an error message naming the unsupported type and instructing the user to use `Vec<u8>` or `String` as the entry channel type and add a decoding Operation.

This restriction applies only to entry channels. Inter-operation channels (`c.channel::<T>(...).from::<X>().into::<Y>()`) and plugin-output channels (`.to_plugin(...)`) MAY carry any `T` that satisfies the channel's wiring constraints.

#### Scenario: Entry channel of bytes
- **WHEN** a Operative declares `c.input::<Vec<u8>>("trade.raw")` and an input arrives over the wire
- **THEN** the consuming Operation's `run` SHALL receive the bytes unchanged

#### Scenario: Entry channel of string with valid UTF-8
- **WHEN** a Operative declares `c.input::<String>("trade.raw")` and a UTF-8-valid byte sequence arrives
- **THEN** the consuming Operation's `run` SHALL receive the decoded `String`

#### Scenario: Entry channel of string with invalid UTF-8
- **WHEN** a Operative declares `c.input::<String>("trade.raw")` and a byte sequence that is not valid UTF-8 arrives
- **THEN** the SDK SHALL log the decode failure against the (operative, channel) pair and SHALL NOT invoke the consuming Operation's `run`

#### Scenario: Entry channel of arbitrary type fails at startup
- **WHEN** a Operative declares `c.input::<RawTrade>("trade.raw")` where `RawTrade` is neither `Vec<u8>` nor `String`
- **THEN** the Operative startup SHALL fail with an error message naming the unsupported type and instructing the user to use `Vec<u8>` or `String` as the entry channel type and add a decoding Operation

### Requirement: Worker pool reuses existing dispatch infrastructure

The Runner pool SHALL be the existing Chase-Lev work-stealing pool implementation (`sdk/rust/src/executor/work_stealing_pool.rs`). The `FLEZHA_WORKER_POOL_SIZE` environment variable SHALL continue to control pool size, defaulting to `Runtime.availableOperatives()`. The per-Segment `inflight` CAS guard SHALL be the same mechanism the existing per-`OperativeSlot` guard uses.

#### Scenario: Default pool sizing
- **WHEN** the SDK is started without `FLEZHA_WORKER_POOL_SIZE` set on a host with 8 cores
- **THEN** the runner pool SHALL contain 8 OS threads

#### Scenario: Env override honored
- **WHEN** `FLEZHA_WORKER_POOL_SIZE=4` is set on a host with 8 cores
- **THEN** the runner pool SHALL contain exactly 4 OS threads

### Requirement: No coexistence with OperativeHandler API

The `OperativeHandler` trait, `OperativeResult` enum, `OperativeRunner` enum, `register_factory()` function, `FactoryMetadata` struct, and `InputTransformer` / `OutputTransformer` traits SHALL be removed from the public Rust SDK API in the same change that introduces the `UnboundedOperative` / `Operation` traits. There SHALL be no transitional period in which both APIs are usable simultaneously.

**Reason**: The library is unpublished, the only known executor-app consumer is in this monorepo and migrates atomically as part of the same change, and the user has an explicit "no legacy / backward-compat shims" policy. Coexistence would also force the new abstraction to retain dispatch-by-operative-id baggage that the new model deliberately leaves behind.

**Migration**: executor apps using `OperativeHandler` rewrite to `UnboundedOperative` + one or more `Operation` impls in the same change. Concrete example: `flezha_market/market-operative/src/main.rs` and `market_computation.rs` are rewritten in the `operative-mvp-rust` change. State (formerly `Assets`) is decomposed into per-Operation state types.

#### Scenario: Old API is not importable
- **WHEN** a Rust file attempts to `use flezha_executor::OperativeHandler` after the cutover
- **THEN** the compilation SHALL fail with an unresolved-import error

#### Scenario: Old registration function is removed
- **WHEN** a Rust file attempts to call `register_factory(...)` after the cutover
- **THEN** the compilation SHALL fail with an unresolved-name error

### Requirement: Dynamic-mode lazy segment creation

For dynamic-mode Operations, the runtime SHALL create a `SegmentSlot` for a key on the first input whose `operation_key(&input)` returns `Some(k)` and for which no slot already exists. Slot creation SHALL be synchronous within the dispatch path: allocate the slot, allocate the mailbox at the Operation's declared `mailbox_capacity()`, push the triggering input, submit a runner job. The per-Operation `segments_created_total` counter SHALL be incremented on each create.

Concurrent producers pushing inputs for the same brand-new key SHALL all observe the same `SegmentSlot` instance after creation. The runtime SHALL NOT allocate two distinct slots for the same key under any concurrency pattern.

#### Scenario: First input with a fresh key creates the segment
- **WHEN** a dynamic-mode Operation receives an input whose `operation_key()` returns `Some("user-42")` and no Segment for `"user-42"` exists
- **THEN** the runtime SHALL create a Segment keyed `"user-42"`, push the input onto its mailbox, increment `segments_created_total`, and submit a runner job

#### Scenario: Subsequent inputs with the same key reuse the segment
- **WHEN** a second input arrives with `operation_key()` returning `Some("user-42")` after the first one created the slot
- **THEN** the runtime SHALL push onto the existing Segment's mailbox; `segments_created_total` SHALL NOT increment

#### Scenario: operation_key returning None drops the input
- **WHEN** a dynamic-mode Operation's `operation_key(&input)` returns `None` for a given input
- **THEN** the runtime SHALL drop the input without allocating a Segment and SHALL increment `inputs_dropped_no_key_total`

#### Scenario: Concurrent producers for the same fresh key share one slot
- **WHEN** N producer threads simultaneously push the first inputs for the same brand-new key `"user-99"`
- **THEN** the runtime SHALL allocate exactly one `SegmentSlot` for `"user-99"` and all N inputs SHALL land on its single mailbox in some serial order

### Requirement: Per-Operation idle-TTL eviction is opt-in (no eviction by default)

`Operation::segment_idle_ttl()` SHALL return `Option<Duration>` and SHALL default to `None`. When it returns `None`, the Operation's dynamic Segments SHALL NOT be evicted — they live for the lifetime of the Operative (as static Segments do) and no reaper thread SHALL be spawned for the Operation.

When `segment_idle_ttl()` returns `Some(ttl)`, the SDK SHALL run one reaper thread for the Operation that periodically evicts Segments whose time since last input push exceeds `ttl`. The reaper SHALL acquire the Segment's `inflight` CAS before evicting and SHALL confirm its mailbox is empty; if the CAS is contended, the reaper SHALL skip the Segment and try again on the next sweep. Eviction SHALL increment the per-Operation `segments_evicted_total` counter.

The reaper SHALL sweep on a cadence derived as `min(ttl / 4, 30s)`, overridable globally via the `FLEZHA_REAP_INTERVAL_MS` environment variable for testing.

#### Scenario: No eviction by default
- **WHEN** a dynamic-mode Operation does not override `segment_idle_ttl()` (it returns `None`)
- **THEN** no reaper thread SHALL be spawned and its Segments SHALL never be evicted by idle TTL

#### Scenario: Idle segment is evicted after TTL elapses
- **WHEN** a Operation sets `segment_idle_ttl() = Some(ttl)` and a Segment has had no input push for longer than `ttl` and is not currently running
- **THEN** the next reaper sweep SHALL acquire its inflight CAS, evict it from the registry, and increment `segments_evicted_total`

#### Scenario: Active segment is not evicted mid-run
- **WHEN** a Segment's `Operation::run` is in flight (its inflight CAS is held) when the reaper sweeps
- **THEN** the reaper SHALL skip this Segment for this sweep without evicting; eviction SHALL be reattempted on the next sweep

#### Scenario: Recently active segment is preserved
- **WHEN** a Segment received an input push within the past `ttl` interval
- **THEN** the reaper SHALL NOT evict it on this sweep

### Requirement: Eviction is destructive; recreate is a cold start

Eviction of a dynamic-mode Segment SHALL be destructive. When a Segment is evicted AND persistence is enabled for the Operative class, the SDK SHALL ship a fire-and-forget `delete_segment_state{segment_id}` frame to the controller so its on-disk WAL/snapshot/outbox files are removed. The SDK SHALL NOT block the reaper on a response.

When a dynamic-mode Segment has been evicted and a subsequent input arrives for the same key, the runtime SHALL recreate the `SegmentSlot` synchronously (per "Dynamic-mode lazy segment creation") and SHALL call `Operation::init_state(Some(key), None)` — a fresh cold start. The runtime SHALL NOT restore the evicted Segment's state from disk on recreate. (This is a deliberate decision: it keeps the recreate path off any blocking controller round-trip, so a recreate can never stall a work-stealing pool worker.)

#### Scenario: Recreate after eviction is a cold start
- **WHEN** a dynamic-mode Operation with persistence enabled is reaped and a subsequent input arrives for the same key
- **THEN** the runtime SHALL recreate the slot and call `Operation::init_state(Some(key), None)`; the Operation's `run` SHALL observe freshly-initialized state, not the state from before eviction

#### Scenario: Eviction deletes persisted state
- **WHEN** a persistence-enabled dynamic-mode Segment is evicted
- **THEN** the SDK SHALL ship `delete_segment_state{segment_id}` to the controller, which SHALL remove the Segment's `{key}.wal`, `{key}.snap.{seq}`, and `{key}.outbox.dedup` files

#### Scenario: Recreate without persistence is a cold start
- **WHEN** a dynamic-mode Operation with persistence disabled is reaped and a subsequent input arrives for the same key
- **THEN** the runtime SHALL call `Operation::init_state(Some(key), None)` and the Operation's `run` SHALL observe the freshly-initialized state

### Requirement: Soft cap on dynamic segments per Operation

Each dynamic-mode Operation SHALL declare a maximum segment count via `fn max_segments() -> usize` (default `100_000`). When the per-Operation segment count reaches this cap, the runtime SHALL drop new-key inputs without allocating a Segment and SHALL increment `inputs_dropped_capped_total`. Inputs whose `operation_key()` resolves to a key with an existing Segment SHALL NOT be affected by the cap; they SHALL be processed normally.

The cap is soft: enforcement is best-effort against concurrent inserts. The runtime SHALL NOT propagate an over-cap rejection back to the agent; the input is silently dropped and the counter is incremented.

#### Scenario: Cap reached drops new-key inputs
- **WHEN** a dynamic-mode Operation with `max_segments = 5` already owns 5 Segments and an input arrives whose `operation_key()` returns a 6th unseen key
- **THEN** the runtime SHALL drop the input, SHALL NOT allocate a Segment, and SHALL increment `inputs_dropped_capped_total`

#### Scenario: Cap does not affect inputs for existing keys
- **WHEN** a dynamic-mode Operation is at its cap and an input arrives whose `operation_key()` returns a key that already has a Segment
- **THEN** the runtime SHALL push to that Segment's mailbox normally; `inputs_dropped_capped_total` SHALL NOT increment

#### Scenario: Cap recovers as TTLs elapse
- **WHEN** a dynamic-mode Operation is at its cap, time passes such that some Segments exceed `segment_idle_ttl()`, and the reaper evicts them
- **THEN** subsequent new-key inputs SHALL allocate Segments normally up to the cap

### Requirement: Per-Operation dynamic-mode counters

Each dynamic-mode Operation SHALL expose the following monotonically-increasing counters on its registry, backed by `AtomicU64`:

- `segments_created_total` — incremented once per `SegmentSlot` allocation (including a cold recreate after eviction).
- `segments_evicted_total` — incremented once per eviction.
- `inputs_dropped_no_key_total` — incremented when `operation_key(&input)` returns `None`.
- `inputs_dropped_capped_total` — incremented when an input is dropped due to the soft cap.

Counters SHALL be observable via the same per-Segment / per-Operation observability surface as the bounded-mailbox counters from `operative-operation-segment-model`'s "Per-Segment mailbox is bounded with declared capacity" requirement. Export to a metrics sink is not in scope for this requirement; this requirement only mandates the data is captured.

#### Scenario: Counter shape after a churn cycle
- **GIVEN** a dynamic-mode Operation with a TTL set
- **WHEN** 100 fresh keys are pushed, 30 of those 100 are subsequently reaped, then 10 of the reaped keys see a fresh input each
- **THEN** `segments_created_total = 110` (100 initial + 10 cold recreates), `segments_evicted_total = 30`

