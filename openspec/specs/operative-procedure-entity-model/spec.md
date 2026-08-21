# operative-procedure-entity-model Specification

## Purpose
This capability defines the structural execution model for Operatives: bounded per-Entity mailboxes with a per-channel overflow policy, the `Procedure` stage trait surface (state, change, clock, config, persistence), typed compile-checked channels, and dynamic entity mode (lazy creation, idle-TTL eviction, and soft caps).
It also names the three-level workload vocabulary itself — Operative → Procedure → Entity — and the rule that every declaration, wire name, and on-disk name is named for the level it describes.
## Requirements
### Requirement: Hierarchy and placement
The system SHALL realize the role relationship as follows:

```
Controller (1 per deployment)
  ↓ (runs N ProcessHub instances, one per namespace)
  → Namespace
    ↓ (ProcessHub picks placement by consistent hash)
    → Operative instance (1 per `flz operative start`)
      ↓ (Controller's `alter_child_data` hook picks an `runner_id`)
      → Runner (SDK process) — may host multiple Operatives
        ↓ (SDK-internal entity; not ProcessHub-aware)
        → Procedure (declared per Operative class)
          ↓
          → Entity (per declared key, or default 1)
            ↓
            → Channel (typed I/O port)
```

The Controller SHALL hold the authoritative placement table. Runners SHALL register with the Controller via the control channel and SHALL receive `AddOperative` / `RemOperative` commands. Entity beneath the Operative level (Procedures, Entities, Channels) is SDK-internal and SHALL NOT be ProcessHub-aware.

#### Scenario: Controller distributes Operatives across Runners
- **WHEN** a Controller has two healthy Runners A and B and `flz operative start` is invoked twice
- **THEN** ProcessHub SHALL place each Operative on a Runner according to its consistent-hash policy; the Controller SHALL issue `AddOperative` over the corresponding Runner's control channel

#### Scenario: Each Operative runs on exactly one Runner at a time
- **WHEN** a Operative is placed on Runner A
- **THEN** ProcessHub SHALL ensure no concurrent `AddOperative` for the same `operative_id` is issued to any other Runner; migrations SHALL go through the documented HotSwap-with-handover path

### Requirement: Per-Entity mailbox is bounded with declared capacity
Each EntitySlot's mailbox SHALL have a fixed capacity declared at operative startup. The mailbox SHALL refuse a push that would exceed this capacity; the SDK then applies the source channel's `OverflowPolicy` (see "Overflow policy declared per channel"). Capacity SHALL NOT change at runtime once the operative is started.

The capacity SHALL be the value returned by the consumer Procedure's `mailbox_capacity()` trait method, evaluated once per Procedure at `register_stream::<R>()` time after applying the precedence rules in "Mailbox capacity precedence". Static-mode Procedures SHALL apply the same capacity to every Entity declared by `entity_keys()`.

The mailbox SHALL refuse capacity values of 0 (which would always overflow) and values strictly greater than 1_048_576 (1 MiB items). Operative startup SHALL fail with an error naming the offending Procedure when `mailbox_capacity()` returns an out-of-range value.

#### Scenario: Default capacity is 1024
- **WHEN** a Procedure does not override `mailbox_capacity()` and no `FLEZHA_MAILBOX_CAPACITY` env var is set
- **THEN** the runtime SHALL allocate a mailbox of capacity 1024 for every Entity of that Procedure

#### Scenario: Override on the Procedure takes precedence
- **WHEN** a Procedure overrides `mailbox_capacity() -> usize { 4096 }` and sets `MAILBOX_CAPACITY_OVERRIDDEN = true`
- **THEN** every Entity of that Procedure SHALL have a mailbox of capacity 4096, regardless of any `FLEZHA_MAILBOX_CAPACITY` env value

#### Scenario: Env var override applies when no Procedure override
- **WHEN** `FLEZHA_MAILBOX_CAPACITY=2048` is set and a Procedure does not override `mailbox_capacity()`
- **THEN** every Entity of that Procedure SHALL have a mailbox of capacity 2048

#### Scenario: Static-mode entities share one capacity
- **WHEN** a Procedure declares `entity_keys() = ["1m","1h","1d","1w"]` and `mailbox_capacity() = 4096`
- **THEN** the runtime SHALL create four mailboxes, each of capacity 4096

#### Scenario: Capacity 0 is rejected at startup
- **WHEN** a Procedure returns 0 from `mailbox_capacity()`
- **THEN** Operative startup SHALL fail with an error naming the Procedure

#### Scenario: Capacity above maximum is rejected at startup
- **WHEN** a Procedure returns 2_000_000 from `mailbox_capacity()`
- **THEN** Operative startup SHALL fail with an error naming the Procedure

### Requirement: Mailbox capacity precedence
The SDK SHALL resolve a Procedure's mailbox capacity at `register_stream::<R>()` time using the following ordered rule:

1. If `Procedure::MAILBOX_CAPACITY_OVERRIDDEN` is `true`, use the value returned by `Procedure::mailbox_capacity()`.
2. Otherwise, if the `FLEZHA_MAILBOX_CAPACITY` environment variable is set to a non-empty string parseable as `usize`, use that value.
3. Otherwise, use the built-in default of 1024.

The override-detection const `MAILBOX_CAPACITY_OVERRIDDEN` SHALL exist on the `Procedure` trait alongside the existing `ENTITIES_FOR_OVERRIDDEN` const; users overriding `mailbox_capacity()` MUST also set `MAILBOX_CAPACITY_OVERRIDDEN = true`. `ENTITY_KEY_OVERRIDDEN` SHALL NOT exist, because dynamic keying is no longer a trait override.

#### Scenario: Override-flag false is treated as "not overridden"
- **WHEN** a Procedure overrides `mailbox_capacity() -> usize { 4096 }` but leaves `MAILBOX_CAPACITY_OVERRIDDEN` at its `false` default and `FLEZHA_MAILBOX_CAPACITY=2048` is set
- **THEN** the resolved capacity SHALL be 2048 (env-var precedence applies because the SDK does not see the override flag as true)

#### Scenario: Env var set but unparseable is ignored
- **WHEN** `FLEZHA_MAILBOX_CAPACITY=banana` is set and the Procedure does not override
- **THEN** the resolved capacity SHALL fall through to the default 1024

### Requirement: Overflow policy declared per channel
Each channel declared via `c.input::<T>(...)` (entry) or `c.channel::<T>(...).from::<S>()` (inter-procedure) SHALL carry an `OverflowPolicy` value applied at the producer-side push site when the consumer's mailbox is full. Plugin-bound channels (`.to_plugin(...)`) SHALL NOT carry an `OverflowPolicy` because no SDK-side mailbox sits between the producer Procedure and the wire; declaring `.on_full(...)` on a plugin-bound channel SHALL be a compile error (the chained-builder shape after `.to_plugin(...)` MUST NOT expose `.on_full(...)`).

`OverflowPolicy` SHALL be an enum with exactly three variants:

- `Block` — the producer SHALL park its calling thread until the consumer drains an item, then re-attempt the push.
- `DropNewest` — the incoming item SHALL be discarded; the consumer's `drops_newest_total` counter SHALL be incremented by 1.
- `DropOldest` — the consumer's mailbox SHALL atomically pop its oldest item and push the incoming one (best-effort under MPMC contention); the consumer's `drops_oldest_total` counter SHALL be incremented by 1.

The policy SHALL be declared via the channel-builder method `.on_full(OverflowPolicy)` on `EntryChannel`, `ChannelDecl`, and `ChannelDeclWithProducer`. When `.on_full(...)` is not called the channel SHALL use the kind-specific default (see "Default overflow policy by channel kind").

#### Scenario: Block waits then retries
- **WHEN** an inter-procedure channel declares `.on_full(OverflowPolicy::Block)`, the consumer's mailbox is full, and a producer calls `ctx.emit::<C>(value)`
- **THEN** the producer's thread SHALL park until the consumer pops an item, then the push SHALL retry; on success the producer's `Procedure::run` SHALL return `Ok(())` (assuming no other error)
- **AND** the producer's call to `ctx.emit::<C>(value)` SHALL eventually return `Ok(())` (no `ProcedureError` is produced for the backpressure event)

#### Scenario: DropNewest discards the incoming item
- **WHEN** an entry channel declares `.on_full(OverflowPolicy::DropNewest)` and a wire frame arrives while the consumer's mailbox is full
- **THEN** the SDK SHALL discard the frame's typed payload; the consumer Entity's `drops_newest_total` SHALL increase by 1; no scheduling of a drain task SHALL occur as a result of this push

#### Scenario: DropOldest replaces the head item
- **WHEN** an inter-procedure channel declares `.on_full(OverflowPolicy::DropOldest)`, the consumer's mailbox is full, and a producer emits a value
- **THEN** the consumer's mailbox SHALL drop its oldest item and SHALL accept the new item; the consumer Entity's `drops_oldest_total` SHALL increase by 1; the producer's `ctx.emit::<C>(value)` SHALL return `Ok(())`

#### Scenario: Plugin-bound channel rejects on_full at compile time
- **WHEN** a Operative declares `c.channel::<Vec<u8>>("p").from::<S>().to_plugin("kafka").on_full(OverflowPolicy::Block)`
- **THEN** the Rust compiler SHALL emit a "no method named `on_full`" error and the program MUST NOT build

### Requirement: Default overflow policy by channel kind
When a channel does not call `.on_full(...)`, the runtime SHALL apply a default policy determined by the channel's kind:

- Entry channels: `OverflowPolicy::Block`.
- Inter-procedure channels: `OverflowPolicy::Block`.
- Plugin-bound channels: not applicable (no SDK-side mailbox).

An entry channel SHALL NOT discard a wire frame unless the Operative declared a dropping policy explicitly. Data loss on the ingress path SHALL be a stated intent, never a default: an Operative that wants shedding under saturation declares `.on_full(OverflowPolicy::DropNewest)` or `.on_full(OverflowPolicy::DropOldest)`.

These defaults SHALL be reflected in the `ChannelDescriptor.overflow_policy` field set by `build_topology` for every channel for which the user did not explicitly call `.on_full(...)`.

#### Scenario: Default entry policy is Block
- **WHEN** a Operative declares `c.input::<Vec<u8>>("trade.raw").into::<ParseProcedure>()` without `.on_full(...)` and a frame arrives while the consumer mailbox is full
- **THEN** the pushing thread SHALL park until the consumer drains an item and the push SHALL then retry until success; the frame SHALL NOT be discarded and `drops_newest_total` SHALL NOT change

#### Scenario: An entry channel still drops when it says so
- **WHEN** a Operative declares `c.input::<Vec<u8>>("trade.raw").on_full(OverflowPolicy::DropNewest).into::<ParseProcedure>()` and a frame arrives while the consumer mailbox is full
- **THEN** the frame SHALL be discarded and `drops_newest_total` SHALL increase by 1

#### Scenario: Default inter-procedure policy is Block
- **WHEN** a Operative declares `c.channel::<Candle>("ohlc.candles").from::<OhlcProcedure>().into::<IndicatorProcedure>()` without `.on_full(...)` and IndicatorProcedure's mailbox is full while OhlcProcedure emits
- **THEN** the OhlcProcedure's calling thread SHALL park until IndicatorProcedure drains an item, and then the push SHALL retry until success; OhlcProcedure's `pushes_blocked_total` SHALL increase by at least 1

### Requirement: Per-Entity backpressure counters
Each EntitySlot's mailbox SHALL expose four `AtomicU64` counters readable via an `EntityCounters` accessor:

- `pushes_total` — incremented on every successful push.
- `pushes_blocked_total` — incremented once per `Block` park event (multiple parks for one push due to multiple-producer contention SHALL each increment).
- `drops_newest_total` — incremented per discarded item under `DropNewest`.
- `drops_oldest_total` — incremented per discarded item under `DropOldest`.

The counters SHALL use `Ordering::Relaxed` for both reads and writes. The counters SHALL be visible to in-process code (e.g. via `entity.mailbox.counters()`); export to external metrics systems (Prometheus, StatsD, log) is OUT OF SCOPE for this requirement.

#### Scenario: Counters present at runtime
- **WHEN** an EntitySlot is constructed at operative startup
- **THEN** the slot's mailbox SHALL expose a `counters()` accessor returning a struct with the four named atomics, all initialized to 0

#### Scenario: Successful push increments pushes_total
- **WHEN** an inter-procedure producer emits 10 items into a non-full consumer mailbox
- **THEN** the consumer Entity's `pushes_total` SHALL increase by exactly 10

#### Scenario: DropNewest increments only drops_newest_total
- **WHEN** an entry channel under `DropNewest` policy receives 5 frames while the consumer mailbox is full
- **THEN** `drops_newest_total` SHALL increase by 5; `drops_oldest_total` SHALL be unchanged; `pushes_blocked_total` SHALL be unchanged; `pushes_total` SHALL be unchanged

### Requirement: Block parking does not violate sticky per-entity ordering
When a producer thread is parked on a full consumer mailbox under `OverflowPolicy::Block`, the runtime SHALL preserve the existing "Sticky per-entity ordering" requirement on the *consumer* Entity. Specifically:

- The consumer Entity's drain loop SHALL continue to run on whichever worker thread holds its inflight CAS; no parked producer SHALL block consumer drain.
- Producers parked on the same consumer mailbox SHALL be unparked one at a time as items pop, in the order they parked (FIFO unpark fairness).

This requirement is in addition to and does not modify the "Sticky per-entity ordering" requirement from the umbrella `operative-architecture` capability spec; it constrains how Block must integrate with that invariant.

#### Scenario: Parked producer does not block its consumer's drain
- **WHEN** Producer A's thread is parked because Consumer X's mailbox is full, and the worker pool has one other thread that picks up X's drain
- **THEN** X's drain SHALL run to completion (popping items, calling `Procedure::run`); each pop SHALL unpark one waiting producer (FIFO order); A SHALL eventually be unparked and complete its push

#### Scenario: FIFO unpark order
- **WHEN** Producers A, B, C park in that order on the same full consumer mailbox
- **THEN** the consumer's first three pops SHALL unpark A, B, C respectively (best-effort under MPMC contention; the *intent* is FIFO order via a `VecDeque<Unparker>` implementation)

### Requirement: Procedure declares Change associated type and apply_change function (Rust SDK)
The Rust SDK's `Procedure` trait SHALL declare an associated `type Change: flezha_runner::state::Change` with default `flezha_runner::state::NoChange`. The `Change` trait SHALL bound on `Serialize + DeserializeOwned + Send + Sync + 'static` and SHALL declare `fn coalesce_key(&self) -> Option<CoalesceKey>` defaulting to `None`.

The `Procedure` trait SHALL declare a function `fn apply_change(state: &mut Self::State, change: &Self::Change)` whose default implementation panics with a descriptive error string for any `Change` other than `NoChange`, and is a no-op for `NoChange`. Procedures declaring a non-`NoChange` Change SHALL override `apply_change` to make their state mutation deterministic.

`apply_change` SHALL be deterministic: replay equality is required. The Rust SDK SHALL NOT enforce purity in the type system; instead, `flezha_runner::testing::ProcedureTestHarness::<S>::replay_equals` SHALL be the determinism gate exercised by user tests.

#### Scenario: Default Change is NoChange skips persistence machinery

- **WHEN** a Rust Procedure does not declare its `type Change` and the runtime constructs an Entity for it
- **THEN** `Procedure::Change` SHALL resolve to `NoChange`, the runtime SHALL detect this via `TypeId::of::<S::Change>() == TypeId::of::<NoChange>()` at operative-build time and SHALL NOT register a flush buffer or invoke the per-Entity flusher for this Entity

#### Scenario: User-defined Change opts the Procedure into persistence

- **WHEN** a Rust Procedure declares `type Change = MyChange` (any non-`NoChange` user type implementing the `Change` trait)
- **THEN** the runtime SHALL register a typed flush buffer (`Vec<MyChange>`) on each Entity of that Procedure and SHALL include those Entities in the per-process flusher's drain rotation

#### Scenario: Default apply_change panics on non-NoChange Change without override

- **WHEN** a Rust Procedure declares `type Change = MyChange` but does NOT override `apply_change`
- **THEN** the operative registration step SHALL succeed (the trait default impl exists) but the first `state.apply(MyChange::Foo)` call SHALL panic with a descriptive error pointing the user at the missing override

#### Scenario: Determinism verified by replay_equals

- **WHEN** a test invokes `ProcedureTestHarness::<S>::new().apply_inputs(inputs).replay_equals()` against a deterministic `apply_change`
- **THEN** the returned `Result<(), ReplayMismatch>` SHALL be `Ok(())` and the test SHALL pass; for a non-deterministic `apply_change` (e.g. one that reads `SystemTime::now()`), the result SHALL be `Err(ReplayMismatch { ... })` carrying serialized expected and actual states

### Requirement: Procedure declares Clock associated type with default WallClock (Rust SDK)
The Rust SDK's `Procedure` trait SHALL declare an associated `type Clock: flezha_runner::state::Clock` with default `WallClock`. `Clock` SHALL be a sealed trait with v1 implementations `WallClock`, `LogicalClock`, and `EventTimeFrom<F>`. v1 of the Rust SDK SHALL fully wire only `WallClock`; Procedures declaring a non-default Clock SHALL fail Operative startup with `OperativeStartError::ClockNotImplementedYet { clock_type }` until a follow-up change wires `LogicalClock` and `EventTimeFrom`.

#### Scenario: Default Clock is WallClock

- **WHEN** a Rust Procedure does not declare its `type Clock`
- **THEN** `Procedure::Clock` SHALL resolve to `WallClock` and Operative startup SHALL succeed

#### Scenario: Non-WallClock fails startup in v1

- **WHEN** a Rust Procedure declares `type Clock = LogicalClock`
- **THEN** `register_stream::<R>()` SHALL fail with `OperativeStartError::ClockNotImplementedYet { clock_type: "LogicalClock" }` and the operative SHALL NOT be registered

### Requirement: Procedure declares Config associated type with typed constructor (Rust SDK)
The Rust SDK's `Procedure` trait SHALL declare an associated `type Config: DeserializeOwned + Send + Sync + 'static` with default `()`, and a constructor `fn from_config(cfg: Self::Config) -> Self`. When `Config = ()` (the default), `from_config(())` SHALL invoke `Self::default()` (requiring `Procedure: Default` for unit-Config procedures; this is satisfied by the existing `operative-mvp-rust` requirement).

#### Scenario: Default unit Config

- **WHEN** a Rust Procedure does not declare `type Config`
- **THEN** the runtime SHALL invoke `Procedure::default()` (via the trait default's `from_config(())`) at Entity init

#### Scenario: User-defined typed Config

- **WHEN** a Rust Procedure declares `type Config = MyConfig` and the operative-start metadata carries matching bytes
- **THEN** the runtime SHALL deserialize the bytes into `MyConfig` (postcard) and SHALL invoke `MyProcedure::from_config(cfg)` to construct the Procedure at Entity init; deserialization failure SHALL produce `OperativeStartError::ConfigDeserializeFailed { procedure, source }`

### Requirement: Procedure declares SCHEMA_VERSION const
The Rust SDK's `Procedure` trait SHALL declare `const SCHEMA_VERSION: u32 = 1`. The runtime SHALL stamp this value into the 16-byte header of every persisted Change record for this Procedure. Users SHALL bump `SCHEMA_VERSION` when they make a non-additive Change-schema edit (e.g. removing or renaming a Change variant).

#### Scenario: Default SCHEMA_VERSION is 1

- **WHEN** a Rust Procedure does not declare `SCHEMA_VERSION`
- **THEN** the trait default `1` SHALL be inherited and stamped into headers

### Requirement: Procedure declares compact_after_bytes per-Entity WAL threshold (Rust SDK)
The Rust SDK's `Procedure` trait SHALL declare `fn compact_after_bytes() -> usize { 64 * 1024 * 1024 }` (default 64 MiB). The runtime SHALL report this value to the agent at operative startup in the existing operative-start metadata so the agent's compaction trigger uses the per-Procedure value.

#### Scenario: Default 64 MiB threshold reported

- **WHEN** a Rust Procedure does not override `compact_after_bytes`
- **THEN** the operative-start metadata SHALL carry `compact_after_bytes = 67108864` for this Procedure

#### Scenario: Per-Procedure override propagated to agent

- **WHEN** a Rust Procedure declares `fn compact_after_bytes() -> usize { 16 * 1024 * 1024 }`
- **THEN** the operative-start metadata SHALL carry `compact_after_bytes = 16777216` for this Procedure

### Requirement: Procedure run receives StateHandle, not direct mutable reference (Rust SDK)
The Rust SDK's `Procedure::run` SHALL receive `state: StateHandle<'_, Self>` instead of `state: &mut Self::State`. `StateHandle<'a, S: Procedure>` SHALL implement `std::ops::Deref<Target = S::State>` for read access and SHALL expose a single mutation method `apply(&mut self, change: S::Change)` that runs `S::apply_change(state, &change)` and pushes the Change onto the per-Entity flush buffer. `StateHandle` SHALL NOT implement `DerefMut`.

#### Scenario: Direct mutation does not compile

- **WHEN** a Rust Procedure's `run` body attempts `state.field = value` directly via `StateHandle`
- **THEN** the Rust compiler SHALL fail with a "trait bound `DerefMut` not satisfied" error and the program MUST NOT build

#### Scenario: Mutation via apply succeeds, runs apply_change, and records

- **WHEN** a Rust Procedure's `run` body calls `state.apply(MyChange::Foo)`
- **THEN** `S::apply_change(state, &MyChange::Foo)` SHALL run on the underlying state AND `MyChange::Foo` SHALL be appended to the per-Entity flush buffer (as observed by `ProcedureTestHarness::recorded_changes()` and by the per-process flusher on its next tick)

#### Scenario: Read access via Deref is allocation-free

- **WHEN** a Rust Procedure's `run` body reads `state.some_field`
- **THEN** the read SHALL succeed via `StateHandle`'s `Deref` impl with no Change recording, no buffer mutation, and zero heap allocations

### Requirement: Operative declares PERSISTENCE_ENABLED const, frozen at instance start (Rust SDK)
The Rust SDK's `StreamOperative` trait SHALL declare `const PERSISTENCE_ENABLED: bool = true`. The runtime SHALL resolve a per-Operative-instance `persistence_enabled: bool` at `start_operative(factory_id, instance_id)` time using:

1. If the operative-start metadata from the agent contains `operative_persistence.<OperativeClass>` with a boolean value, use that.
2. Otherwise, use the value of `StreamOperative::PERSISTENCE_ENABLED`.
3. The default of the trait const is `true`.

The resolved value SHALL be bound to the `StreamInstance` and SHALL be propagated to every `EntitySlot` constructed for that instance. Subsequent changes to the runner-side metadata or to the `StreamOperative::PERSISTENCE_ENABLED` const (via SDK redeploy) SHALL NOT affect already-running instances; new values take effect on the next `stop_operative` + `start_operative` cycle for that instance.

#### Scenario: Default const value is true

- **WHEN** a Operative does not declare `PERSISTENCE_ENABLED` and no runner override is present in operative-start metadata
- **THEN** `StreamInstance::persistence_enabled()` SHALL return `true`

#### Scenario: Const-declared opt-out is honored

- **WHEN** `MarketOperative` declares `const PERSISTENCE_ENABLED: bool = false` and no runner override is present
- **THEN** `StreamInstance::persistence_enabled()` SHALL return `false`; the per-Process flusher SHALL drain-and-discard for this instance's Entities; `stub_send_frame` (or its real counterpart) SHALL NOT be invoked for this instance

#### Scenario: Runner override takes precedence over const

- **WHEN** `MarketOperative` declares `const PERSISTENCE_ENABLED: bool = true` AND the operative-start metadata carries `operative_persistence.MarketOperative = false`
- **THEN** `StreamInstance::persistence_enabled()` SHALL return `false`

#### Scenario: Resolved value frozen at instance start

- **WHEN** a `StreamInstance` is constructed with resolved `persistence_enabled = true`, AND the runner subsequently issues a runtime config change setting `operative_persistence.<OperativeClass> = false`
- **THEN** the running `StreamInstance` SHALL continue with `persistence_enabled = true`; the new value SHALL take effect only after `stop_operative(instance_id)` and a subsequent `start_operative(...)` invocation

### Requirement: init_state accepts optional restoration payload (Rust SDK)
The Rust SDK's `Procedure::init_state` signature SHALL be `fn init_state(entity_key: Option<&str>, init_payload: Option<RestoreInitPayload<'_>>) -> Self::State`. When `init_payload = None`, `init_state` SHALL behave as a fresh-start constructor. When `init_payload = Some(payload)`, the user MAY inspect or ignore the payload bytes; the runtime SHALL automatically apply the payload (snapshot decode + change-log fold via `Procedure::apply_change`) AFTER `init_state` returns, replacing the user-constructed state with the restored state.

#### Scenario: Cold start passes None

- **WHEN** a Rust Entity is constructed for the first time with no prior persisted state on the agent
- **THEN** the runtime SHALL invoke `init_state(entity_key, None)`

#### Scenario: Restart passes Some payload

- **WHEN** a Rust Entity is reconnected and the agent has snapshot or WAL bytes for it
- **THEN** the runtime SHALL invoke `init_state(entity_key, Some(payload))` AND, after `init_state` returns, the runtime SHALL decode `payload.snapshot_bytes` into `Self::State` (replacing the user's value) if the snapshot is non-empty, AND the runtime SHALL fold `payload.change_log_bytes` via `Procedure::apply_change` in seq order

#### Scenario: User can inspect payload without applying

- **WHEN** a Rust Procedure's `init_state` reads `payload.change_log_bytes.len()` for instrumentation purposes
- **THEN** the read SHALL succeed; the runtime SHALL still apply the payload after `init_state` returns; the user's instrumentation SHALL not affect the restored state

### Requirement: Per-Entity flush buffer registered for non-NoChange Procedures
The Rust SDK's `EntitySlot` SHALL hold a type-erased flush buffer (`Arc<Mutex<Box<dyn FlushableBuffer>>>`) for any Entity whose Procedure's `Change` type is not `NoChange`. The buffer SHALL be a `Vec<S::Change>` initially empty; `state.apply(...)` SHALL push onto it. The per-process flusher SHALL acquire the Mutex on each linger tick to drain the buffer.

For Entities whose Procedure's `Change == NoChange`, the runtime SHALL NOT allocate a flush buffer or register the Entity with the flusher.

#### Scenario: NoChange Procedure Entities are not registered with the flusher

- **WHEN** the runtime constructs an Entity for a Procedure with `Change = NoChange`
- **THEN** the `EntityFlusher`'s registry SHALL NOT contain an entry for this Entity

#### Scenario: Non-NoChange Procedure Entities are registered

- **WHEN** the runtime constructs an Entity for a Procedure with `Change = MyChange`
- **THEN** the `EntityFlusher`'s registry SHALL contain an entry keyed by `(operative_instance_id, procedure_id, entity_key)` with the typed buffer reference, the resolved `persistence_enabled` flag, the `change_seq: AtomicU64`, and the wire-frame send fn pointer

### Requirement: Four-layer abstraction with defined responsibilities
The SDK SHALL expose a four-layer abstraction comprising Operative, Procedure, Entity, and Runner. Each layer SHALL have the following exclusive responsibilities and no others SHALL be implicitly added:

- **Operative** — a user-defined struct implementing the `StreamOperative` trait. Spawned by the agent once per `flz operative start` invocation. Owns the channel topology declaration and the registered Procedures. Holds no domain state of its own.
- **Procedure** — a user-defined struct implementing the `Procedure` trait. Defines a stage's typed input, typed output, state type, run logic, and entity strategy. Holds no runtime state of its own; acts as the template for Entities.
- **Entity** — a runtime instance of a Procedure, identified by an optional string key. Holds exactly one State value, exactly one mailbox, and exactly one `inflight` flag. Created by the SDK at operative startup; not user-instantiable.
- **Runner** — an OS thread in the SDK's worker pool. Drains Entity mailboxes. Not user-visible at the API surface; pool size is configured via the existing `FLEZHA_WORKER_POOL_SIZE` mechanism.

#### Scenario: Each layer has a distinct programmer-visible role
- **WHEN** a developer reads the public API of the new SDK
- **THEN** they SHALL find a `StreamOperative` trait, a `Procedure` trait, but no `Entity` trait or `Runner` trait — Entities and Runners are runtime artifacts created by the SDK, not types the user implements

#### Scenario: A Operative holds no domain state
- **WHEN** a `StreamOperative` impl is examined
- **THEN** the trait MUST NOT include a state-init or state-handle method; all stateful behavior MUST live on Procedures

### Requirement: Static and single entity modes are supported in v1
A Procedure SHALL run in one of exactly three entity modes (the v1 title is retained for MODIFIED-matcher binding; the body below replaces the v1 wording end-to-end and adds dynamic mode):

- **Single entity (default)**: when the Procedure declares no `entity_keys()` and no consumer edge into it declares a key function. The runtime SHALL create exactly one Entity with `entity_key = None` at operative startup, and SHALL call `Procedure::init_state(None)` to initialize its state.
- **Static entities**: when the Procedure implements `fn entity_keys() -> Vec<&'static str>` returning a non-empty list. The runtime SHALL create one Entity per declared key at operative startup, and SHALL call `Procedure::init_state(Some(key))` for each.
- **Dynamic entities**: when a consumer edge into the Procedure declares a key function (`key_by` / `keyBy` on the wiring) and the Procedure leaves `entity_keys()` empty. The runtime SHALL own zero Entities for the Procedure at operative startup; Entities SHALL be created lazily on the first input for which the edge's key function returns a key with no live Entity (see "Dynamic-mode lazy entity creation"). The key function is total, so there SHALL be no keyless-drop path: discarding an input is expressed with `filter` declared before the keying.

The Operative startup SHALL fail with a clear error message if a Procedure:

- declares a non-empty `entity_keys()` while a consumer edge into it declares a key function (static and dynamic keying are mutually exclusive).

#### Scenario: Single entity for a Procedure with no entity declared
- **WHEN** a Procedure declares no `entity_keys()`, no edge into it is keyed, and the Operative is started
- **THEN** the SDK SHALL create exactly one Entity for that Procedure with `entity_key = None`, and `Procedure::init_state(None)` SHALL be called exactly once during startup

#### Scenario: Static entities created at operative startup
- **WHEN** a Procedure declares `entity_keys() = ["1m", "1h", "1d", "1w"]` and the Operative is started
- **THEN** the SDK SHALL create exactly four Entities for that Procedure at startup, with `entity_key = Some("1m")`, `Some("1h")`, `Some("1d")`, `Some("1w")` respectively; `init_state` SHALL be called exactly once for each

#### Scenario: Dynamic-mode declaration is accepted at operative startup
- **WHEN** a consumer edge declares a key function into a Procedure that leaves `entity_keys()` empty
- **THEN** the Operative startup SHALL succeed and the Procedure SHALL be registered in dynamic mode with zero Entities

#### Scenario: Static keyset plus a keyed edge is rejected
- **WHEN** a Procedure returns a non-empty `entity_keys()` AND a consumer edge into it declares a key function
- **THEN** Operative startup SHALL fail with an error naming the conflicting Procedure

### Requirement: Sticky per-entity ordering
For any single Entity, two invocations of its Procedure's `run(...)` method SHALL NOT execute concurrently, and invocations SHALL observe their inputs in the order those inputs were enqueued onto the Entity's mailbox. This invariant MUST hold across all dispatch modes, all runner pool sizes, and all transport types.

#### Scenario: Same entity is never concurrent
- **WHEN** 10,000 inputs targeting one Entity arrive on its mailbox while the runner pool has 16 threads
- **THEN** at any wall-clock instant, at most one runner thread SHALL be inside that Entity's `run(...)` method

#### Scenario: Different entities of the same procedure run in parallel
- **WHEN** inputs arrive simultaneously for two distinct Entities of the same Procedure (for example `("ohlc","1m")` and `("ohlc","1h")`) and the runner pool has ≥ 2 threads
- **THEN** the two Entities' `run(...)` methods MAY execute concurrently on different threads

#### Scenario: Per-entity input order is preserved
- **WHEN** inputs `a`, `b`, `c` are enqueued onto one Entity's mailbox in that order with an append-to-state Procedure
- **THEN** the Entity's State SHALL end with `[a, b, c]` in exactly that order

### Requirement: Typed channels with compile-time wiring checks
All inter-Procedure and Procedure-to-plugin routing SHALL go through Channels declared on the Operative via the producer-first graph builder (`sdk-graph-authoring`). Each Channel SHALL carry a phantom type parameter `Channel<T>`, where `T` is taken from the producing Procedure's `Output` type — there is no user-supplied type token and no `.from(...)` clause for inter-procedure channels. The wiring API SHALL refuse to build in any of these cases:

- A consumer Procedure whose `Input` associated type is not `T` is wired via `.into::<X>()` / `.into(X.class)` onto a stream of payload type `T`.
- A node is wired as the producer of more than one output channel (the emit surface carries one output channel per procedure, so a second one could never be routed).

In Rust these refusals SHALL be compile-time where the builder shape permits, otherwise registration-time; in Java they SHALL be registration-time with an error carrying the same diagnostic content.

#### Scenario: Type-mismatched consumer fails to build
- **WHEN** a Procedure with `type Input = TradeMsg` is wired via `.into::<That>()` onto a stream whose producer declares `type Output = Candle`
- **THEN** the Rust compiler SHALL emit a trait-bound error (or registration SHALL fail where compile-time enforcement is impossible), and in Java registration SHALL throw naming both procedures and both types

#### Scenario: Channel payload type comes from the producer
- **WHEN** a stream is created by `.into::<OhlcProc>()` where `OhlcProc::Output = Candle`
- **THEN** the resulting channel SHALL carry payload type `Candle` with no type token anywhere in the declaration

#### Scenario: Second output channel for one node fails to build
- **WHEN** a declaration attempts to give one node two output channels
- **THEN** the program MUST fail at compile time (or at registration time if compile-time enforcement is impossible for the chosen builder shape) with an error naming the node

### Requirement: Per-operative channel namespace
Channel names SHALL be scoped to a single Operative instance. Two Operative instances (for example the BTC Operative and the ETH Operative) SHALL each have their own independent channel namespace; a channel named `"trade.raw"` on one Operative SHALL be a distinct logical channel from a channel of the same name on another Operative. Auto-derived channel names (from the producer node's identity, per `sdk-graph-authoring`), `.named(...)` overrides, and explicit entry names SHALL all share this namespace and its duplicate check; a collision SHALL fail registration naming both declarations.

#### Scenario: Same-named channels on different operatives are independent
- **WHEN** the BTC Operative declares `c.input::<Trade>("trade.raw")` and the ETH Operative also declares `c.input::<Trade>("trade.raw")`
- **THEN** an input pushed to the BTC Operative's `"trade.raw"` channel SHALL NOT be observable by the ETH Operative's Procedures, and vice versa

#### Scenario: Derived and explicit names collide in one namespace
- **WHEN** an operative declares an entry channel named `"validate"` and also wires a node whose identity is `validate` without a `.named(...)` override
- **THEN** registration SHALL fail with a duplicate-channel-name error naming both declarations

### Requirement: Hot-path channel dispatch uses interned u32 IDs
At Operative startup, channel names SHALL be interned to `u32` IDs once. Runtime input-routing decisions inside the SDK hot path SHALL use the integer ID, not the string name. Channel-name strings SHALL appear only in: the user-facing API, registration time, and the on-the-wire metadata field crossing the NIF boundary.

#### Scenario: Hot path is allocation-free with respect to channel names
- **WHEN** an input is dispatched from a Operative's input channel through to an Entity's mailbox during steady-state procedure
- **THEN** no per-message allocation of `String` for the channel name SHALL be observable in profiling

### Requirement: Inter-procedure transport is in-process zero-copy
When a Channel connects two Procedures inside the same Operative, the SDK SHALL transport values as `Arc<T>` references between Entities, performing no serialization, no allocation of an output buffer, and no NIF-boundary crossing for that transport.

#### Scenario: Pipeline between two procedures does not serialize
- **WHEN** OhlcProcedure emits a `Candle` to a channel that IndicatorProcedure subscribes to (and the channel has no `to_plugin(...)` clause)
- **THEN** the IndicatorProcedure's `run(...)` SHALL receive the same `Arc<Candle>` instance OhlcProcedure emitted (or an `Arc` to byte-identical content with no intermediate serialization step)

### Requirement: Plugin emission semantics
Channels declared with `.sink("name")` / `.sink_with("name", serializer)` — in **either** the Rust SDK or the Java SDK (`.sink(...)`) — SHALL route a value, whether or not the channel also has inter-procedure consumers, through the agent's plugin layer using the channel name as the wire identifier. The numeric `plugin_group: u8` metadata field is removed from the wire format. The replacement metadata field SHALL be `channel: String` — the literal channel name as a string in v1. A per-channel integer fast-path (sending an interned `u32` instead of the string) MAY be added by a future change as an opt-in alternative, but SHALL NOT be in v1.

The wire bytes are produced by the channel's `pluginSink.serializer` / `plugin_sink.serializer` (set by `sink(...)` / `sink_with(...)`), the one representation of plugin egress for both shapes. The byte buffer SHALL be sent through the same wire framing path (`channel: String` metadata + payload bytes) so the agent's plugin layer SHALL NOT distinguish between pure and hybrid channels at the wire — both arrive identically and route by channel name. **Furthermore, the agent's plugin layer SHALL NOT distinguish between Rust-SDK and Java-SDK frames** — hybrid frames produced by either SDK are byte-indistinguishable for the same plugin name and serializer-output bytes.

#### Scenario: Pure-plugin emit reaches the agent's plugin layer
- **WHEN** a Procedure publishes to a channel declared with `.sink("ohlc_kafka")` and no consumers
- **THEN** the agent's `ohlc_kafka` plugin SHALL receive the bytes with `channel = <channel name>` in the metadata

#### Scenario: Hybrid emit reaches the agent's plugin layer with identical framing
- **WHEN** a Procedure publishes via `.sink_with("indicators", sma_to_pb_bytes)` on a channel that also has inter-procedure consumers
- **THEN** the agent's `indicators` plugin SHALL receive the serializer-output bytes with `channel = <channel name>` in the metadata
- **AND** the wire frame SHALL be byte-indistinguishable from a pure-plugin channel's frame for the same plugin name and bytes

#### Scenario: Two channels sharing a plugin name fan in at the agent
- **WHEN** a Operative declares two hybrid channels (e.g. an `sma_out` stream with `sink_with("indicators", sma_serde)` and an `ema_out` stream with `sink_with("indicators", ema_serde)`)
- **THEN** the agent's `indicators` plugin SHALL receive frames from both channels, distinguishable only by the `channel: String` metadata key
- **AND** the SDK's topology builder SHALL NOT reject this declaration, because each channel still has exactly one producer Procedure

#### Scenario: Plugin subscribes by channel name
- **WHEN** an agent-side plugin is configured to consume outputs from `"ohlc_kafka"`
- **THEN** that plugin SHALL receive every message whose channel name matches `"ohlc_kafka"`, regardless of which Procedure produced it

#### Scenario: Hybrid emit reaches the agent's plugin layer with identical framing across SDKs
- **WHEN** a Rust Procedure publishes via `.sink_with("indicators", sma_to_pb_bytes)` and a Java Procedure publishes via `.sink("indicators", SmaCodec::toBytes)` with identical serializer output for an identical input value
- **THEN** the agent's `indicators` plugin SHALL receive byte-equal wire frames from both SDKs (the only on-wire difference being TCP/UDS interleaving — the framed payload bytes are identical)
- **AND** the SDK's topology builder on either side SHALL NOT reject either declaration, because each channel still has exactly one producer Procedure within its own SDK

### Requirement: ProcedureCtx emit semantics
A Procedure's `run` method SHALL receive a mutable reference to a `ProcedureCtx`. The Procedure SHALL emit zero, one, or more outputs by calling `ctx.emit::<C>(value)` for each output, where `C` is the channel-payload type (which the compiler resolves to the matching channel via the wiring declaration). For outputs that need a string-keyed user metadata bag attached, the Procedure SHALL call `ctx.emit_with_meta::<C>(value, HashMap<String, String>)` instead. The `run` method's return type SHALL be `Result<(), ProcedureError>` for success / hard-error signaling. Producing no output SHALL be expressed by simply not calling `ctx.emit` (or `ctx.emit_with_meta`); there SHALL be no explicit "no-output" return variant.

`ctx.emit_with_meta` SHALL be valid only for channels declared with `.to_plugin("name")`. Calling `emit_with_meta` on a channel that flows to another Procedure inside the same Operative SHALL return a `ProcedureError` and SHALL NOT propagate the value or the metadata. The error message SHALL name the offending channel and instruct the user to either move the metadata into the typed value or add `.to_plugin(...)` to the channel.

When `emit_with_meta` is used on a plugin-bound channel, the supplied `HashMap<String, String>` SHALL be attached to the output as it crosses the NIF boundary using the existing wire-format `[key_len][val_len][key][value]` block. The channel name (or its interned `u32` equivalent) SHALL be one of the keys; the user-supplied keys SHALL be appended without further validation. Reserved key names (e.g. `channel`) SHALL NOT be overridable via the user-supplied map; an attempt to set one SHALL be silently dropped during framing.

#### Scenario: Fan-out via multiple emit calls
- **WHEN** a ParseProcedure's `run` calls `ctx.emit::<ParsedTrade>(p)` followed by `ctx.emit::<RawSummary>(s)` and returns `Ok(())`
- **THEN** both downstream channels SHALL receive their respective values, and the order of emission SHALL match the order of the `emit` calls

#### Scenario: No-output via no-call
- **WHEN** a Procedure's `run` returns `Ok(())` without calling `ctx.emit`
- **THEN** the SDK SHALL treat the input as fully processed with no downstream propagation, with no error and no special enum variant required

#### Scenario: Hard error via Err return
- **WHEN** a Procedure's `run` returns `Err(e)`
- **THEN** the SDK SHALL log the error against the (operative, procedure, entity) triple and SHALL NOT propagate any partial output that may have been emitted before the error

#### Scenario: emit_with_meta on a plugin-bound channel
- **WHEN** a Procedure's `run` calls `ctx.emit_with_meta::<Candle>(c, {"trace_id": "abc123"})` on a channel declared with `.to_plugin("ohlc_kafka")`
- **THEN** the message reaching the agent SHALL carry the channel name AND the `trace_id: "abc123"` key in its metadata block, and the agent's plugin SHALL receive both

#### Scenario: emit_with_meta on an inter-procedure channel returns error
- **WHEN** a Procedure's `run` calls `ctx.emit_with_meta::<ParsedTrade>(p, {"trace_id": "abc123"})` on a channel that flows to another Procedure inside the same Operative (no `.to_plugin(...)` clause)
- **THEN** the call SHALL return `Err(ProcedureError::MetadataNotAllowedOnInterProcedureChannel { channel })`, and the value `p` SHALL NOT be propagated to the downstream Procedure

### Requirement: Hybrid channel builder method
The Rust SDK SHALL expose `sink_with<F>(plugin_name: &'static str, serializer: F)` where `F: Fn(&T) -> Vec<u8> + Send + Sync + 'static`, and `sink(plugin_name: &'static str)` (default serialization: passthrough for `Vec<u8>`, UTF-8 for `String`, the SDK's default JSON codec otherwise), on the graph builder's stream handle. Calling either SHALL attach a plugin sink (the named plugin and the serializer, type-erased to `PluginSerializeFn`) alongside the channel's existing consumer fan-out. After the call, further `.into::<S>()` consumer wiring SHALL chain naturally — the method returns the same stream handle.

The plugin sink SHALL be stored on the channel descriptor as a `plugin_sink: Option<PluginSink>` field where `PluginSink { name: &'static str, serializer: PluginSerializeFn }`. Pure-`PluginBound` channels (produced by `output(...)`) SHALL continue to populate the existing `plugin_name` and `plugin_serializer` fields and SHALL NOT populate `plugin_sink`. Hybrid channels SHALL populate `plugin_sink` and SHALL NOT populate `plugin_name`/`plugin_serializer`. The two paths SHALL stay textually distinct so existing pattern matches over `ChannelKind` continue to compile unchanged.

#### Scenario: Hybrid wiring is accepted at registration
- **WHEN** a Operative declares an `SmaProcedure` output stream with `.sink_with("indicators", sma_to_pb_bytes).into::<BollingerProcedure>()`
- **THEN** registration SHALL succeed
- **AND** the resulting `ChannelDescriptor` SHALL have `kind = InterProcedure`, exactly one entry in `consumers` (the `BollingerProcedure` procedure id), and `plugin_sink = Some(PluginSink { name: "indicators", serializer: <fn-ptr> })`

#### Scenario: sink_with chains in either order with into::<S>()
- **WHEN** a Operative declares `.into::<MacdProcedure>().sink_with("indicators", ema_to_pb_bytes)` on an `EmaProcedure` output stream
- **THEN** registration SHALL succeed
- **AND** the resulting descriptor SHALL be identical to the same declaration with the calls swapped

#### Scenario: sink without a serializer uses the default codec
- **WHEN** a Operative declares `.sink("indicators")` on a stream of a serde-serializable type
- **THEN** the plugin sink SHALL serialize each value with the SDK's default JSON codec, identically to what `output("indicators")` would produce for the same value

#### Scenario: Calling sink_with twice on the same channel is a topology error
- **WHEN** a Operative chains `.sink_with("p1", f1).sink_with("p2", f2)` on one stream
- **THEN** registration SHALL fail with `BuildTopologyError::DuplicatePluginSink { channel }` naming the channel

#### Scenario: nothing chains after sealed
- **WHEN** a Operative attempts `.sink("p").sealed().into::<S>()`
- **THEN** the code SHALL fail to compile because `sealed` returns a finalized handle with no further methods
- **AND** a trybuild test under `sdk/rust/tests/build_failures/` SHALL pin this as a stable compile error

### Requirement: Hybrid channel emission — consumer fan-out then single serializer call
When a Procedure's `Procedure::run` calls `ctx.emit(value)` on a hybrid channel (`kind = InterProcedure, plugin_sink = Some(_)`), the dispatcher SHALL execute the following sequence on the producer thread:

1. Fan out the typed `Arc<dyn Any + Send + Sync>` value to every consumer mailbox listed in the channel's `consumers` per the channel's `OverflowPolicy`. This step SHALL complete (modulo `OverflowPolicy::Block` parking) before step 2 begins.
2. If `plugin_sink` is `Some(sink)`, invoke `sink.serializer(value.as_ref())` exactly once inside `std::panic::catch_unwind(AssertUnwindSafe(...))`. On `Ok(bytes)`, send the bytes through the existing plugin-wire framing path (`channel: String` wire-metadata key set to the channel name; `[key_len][val_len][key][value]` block format). On `Err(panic)`, return `ProcedureError::PluginSerializerPanic { channel: <channel name>, message: <formatted panic payload> }` to the producing Procedure's `ctx.emit` call without emitting any plugin bytes.

The dispatcher thread SHALL remain alive across a serializer panic. Subsequent emits SHALL proceed normally; only the producing Procedure sees the per-emit error.

The serializer SHALL run exactly once per `ctx.emit` invocation regardless of consumer count. The byte buffer produced by the serializer SHALL NOT be cached or reused across emits.

#### Scenario: Hybrid emit fans out typed value to all consumers and bytes to plugin
- **WHEN** a producer Procedure's `run` calls `ctx.emit(value)` on a channel wired to `[ConsumerA, ConsumerB]` and with `plugin_sink = Some(("indicators", serializer))`
- **THEN** `ConsumerA::run` and `ConsumerB::run` SHALL each be invoked with the typed `value` (cloned per the existing inter-procedure delivery semantics)
- **AND** the agent-side plugin "indicators" SHALL receive a single wire frame with bytes equal to `serializer(&value)` and `channel = <channel name>` in the metadata

#### Scenario: Single serializer call regardless of consumer count
- **WHEN** a hybrid channel has N inter-procedure consumers and the producer emits one value
- **THEN** the serializer SHALL be invoked exactly once for that emit
- **AND** N is unconstrained (1, 3, 100 — same single-call guarantee)

#### Scenario: Panicking serializer surfaces as ProcedureError without crashing the dispatcher
- **WHEN** the serializer panics on a particular input
- **THEN** the inter-procedure consumers SHALL still receive the typed value (consumer fan-out completed before serialization)
- **AND** `ctx.emit(...)` SHALL return `Err(ProcedureError::PluginSerializerPanic { channel, message })`
- **AND** the dispatcher thread SHALL remain alive — a subsequent `ctx.emit` call SHALL execute normally

#### Scenario: Serializer runs after consumer fan-out, not before
- **WHEN** an `OverflowPolicy::Block` consumer mailbox is full and the producer emits on a hybrid channel
- **THEN** the producer SHALL park on the consumer mailbox
- **AND** the plugin sink SHALL NOT receive any bytes until the consumer drains and the producer unparks
- **AND** when the producer unparks, the serializer SHALL be invoked exactly once and the plugin sink SHALL receive the corresponding bytes

### Requirement: emit_with_meta is valid on hybrid channels
`ProcedureCtx::emit_with_meta(value, meta)` SHALL accept the call when either:

- the channel is `kind = PluginBound` (existing rule), OR
- the channel has `plugin_sink = Some(_)` (new rule for hybrid channels).

On a hybrid channel, the inter-procedure consumers SHALL receive the typed value with no metadata (metadata is plugin-framing only and has no place in the typed delivery model). The plugin sink SHALL receive bytes (via the channel's serializer) with the supplied `HashMap<String, String>` attached to the wire frame using the existing `[key_len][val_len][key][value]` block format. The reserved `channel: String` key SHALL be set automatically and SHALL NOT be overridable by user-supplied keys; an attempt to set a reserved key SHALL be silently dropped during framing (matches the existing rule for `PluginBound` channels).

On a pure-`InterProcedure` channel with no plugin sink, `emit_with_meta` SHALL continue to return `ProcedureError::MetadataNotAllowedOnInterProcedureChannel { channel }` without delivering the value or the metadata.

#### Scenario: emit_with_meta on a hybrid channel forwards metadata to plugin only
- **WHEN** a producer Procedure's `run` calls `ctx.emit_with_meta(value, {"trace_id": "abc123"})` on a hybrid channel wired to one consumer and one plugin sink
- **THEN** the consumer SHALL receive the typed `value` with no `trace_id` visible
- **AND** the plugin wire frame SHALL contain `trace_id = "abc123"` plus the implicit `channel = <channel name>` key

#### Scenario: emit_with_meta on a pure inter-procedure channel still rejects metadata
- **WHEN** a producer Procedure's `run` calls `ctx.emit_with_meta(value, {"trace_id": "abc123"})` on a channel that has consumers but no `plugin_sink`
- **THEN** `ctx.emit_with_meta` SHALL return `ProcedureError::MetadataNotAllowedOnInterProcedureChannel { channel }`
- **AND** neither the consumers nor any plugin SHALL receive the value or the metadata

#### Scenario: Reserved metadata keys are stripped on hybrid channels
- **WHEN** a producer calls `ctx.emit_with_meta(value, {"channel": "spoof", "trace_id": "abc"})` on a hybrid channel
- **THEN** the wire frame SHALL contain `channel = <real channel name>` (the reserved value, not "spoof") and `trace_id = "abc"`

### Requirement: New BuildTopologyError variant — `DuplicatePluginSink`
`BuildTopologyError` (in `sdk/rust/src/channel/builder.rs`) SHALL gain a `DuplicatePluginSink { channel: &'static str }` variant. The variant SHALL be raised at `register_stream::<R>()` time when `also_to_plugin_with` is called more than once on the same channel. The first call registers the sink; the second SHALL push the error onto the builder's error vector and SHALL NOT overwrite the previously-registered sink. The aggregate `register_stream::<R>()` validation SHALL fail with the standard "topology rejected" message that lists every accumulated `BuildTopologyError`.

#### Scenario: Second also_to_plugin_with call is rejected at registration
- **WHEN** a Operative's `channels(c)` method calls `also_to_plugin_with("p1", f1)` followed by `also_to_plugin_with("p2", f2)` on the same channel
- **THEN** `register_stream::<R>()` SHALL panic with the topology error including `DuplicatePluginSink { channel: "<name>" }`
- **AND** the registered topology SHALL be discarded

### Requirement: New ProcedureError variant — `PluginSerializerPanic`
`ProcedureError` (in `sdk/rust/src/procedure/error.rs`) SHALL gain a `PluginSerializerPanic { channel: String, message: String }` variant. The variant SHALL be returned from `ctx.emit(...)` (and `ctx.emit_with_meta(...)`) on a hybrid channel when the user-supplied serializer panics. `channel` SHALL be the channel name as declared in `ChannelBuilder`; `message` SHALL be the panic payload formatted via the existing panic-message helper (a string description, not the raw `Box<dyn Any>` payload).

The producing Procedure's `Procedure::run` SHALL receive the error via the normal `Result<(), ProcedureError>` return path of `ctx.emit`. Returning `Err` from `Procedure::run` continues to discard partial emits per the existing trait contract; ignoring the error is also valid and SHALL leave the inter-procedure consumers' delivery intact.

#### Scenario: Producer Procedure sees PluginSerializerPanic and chooses how to handle it
- **WHEN** a hybrid channel's serializer panics during `ctx.emit(value)`
- **THEN** `ctx.emit(value)` SHALL return `Err(ProcedureError::PluginSerializerPanic { channel, message })`
- **AND** the producing Procedure MAY ignore, log, or `return Err(...)` — all three are valid handlers
- **AND** the inter-procedure consumer fan-out SHALL have already completed before the panic was caught

### Requirement: Hybrid channel builder method (Java SDK)
The Java SDK SHALL expose `sink(String pluginName, Function<T, byte[]> serializer)` and `sink(String pluginName)` (default serialization: passthrough for `byte[]`, UTF-8 for `String`, Jackson JSON otherwise) on the graph builder's stream handle. Calling either SHALL attach a plugin sink (the named plugin and the serializer, type-erased to `Function<Object, byte[]>`) alongside the channel's existing consumer fan-out. After the call, further `.into(...)` consumer wiring SHALL chain naturally — the method returns the same stream handle.

The plugin sink SHALL be stored on the channel descriptor as `Optional<PluginSink> pluginSink` where `PluginSink` is a public record `(String name, Function<Object, byte[]> serializer)`. Pure-`PLUGIN_BOUND` channels (produced by `output(...)`) SHALL continue to populate the existing `pluginName` and `pluginSerializer` fields and SHALL NOT populate `pluginSink`. Hybrid channels SHALL populate `pluginSink` and SHALL NOT populate `pluginName`/`pluginSerializer`. The two paths SHALL stay textually distinct so existing pattern matches over `ChannelKind` continue to compile unchanged.

#### Scenario: Hybrid wiring is accepted at registration (Java)
- **WHEN** a Operative declares an `SmaProcedure` output stream with `.sink("indicators", SmaCodec::toBytes).into(BollingerProcedure.class)`
- **THEN** registration SHALL succeed
- **AND** the resulting `ChannelDescriptor` SHALL have `kind() == INTER_PROCEDURE`, exactly one entry in `consumers()` (the `BollingerProcedure` procedure id), and `pluginSink() == Optional.of(new PluginSink("indicators", <fn>))`

#### Scenario: sink chains in either order with into(...) (Java)
- **WHEN** a Operative declares `.into(MacdProcedure.class).sink("indicators", EmaCodec::toBytes)` on an `EmaProcedure` output stream
- **THEN** registration SHALL succeed
- **AND** the resulting descriptor SHALL be identical to the same declaration with the calls swapped

#### Scenario: sink without a serializer uses the default codec (Java)
- **WHEN** a Operative declares `.sink("indicators")` on a stream of a Jackson-serializable type
- **THEN** the plugin sink SHALL serialize each value with Jackson JSON, identically to what `output("indicators")` would produce for the same value

#### Scenario: nothing chains after sealed (Java)
- **WHEN** a Operative attempts `.sink("p").sealed().into(S.class)`
- **THEN** the code SHALL fail to type-check because `sealed` returns a finalized `Channel<T>` handle that exposes no further methods

### Requirement: Hybrid channel emission — consumer fan-out then single serializer call (Java SDK)
When a Procedure's `Procedure.run` calls `ctx.emit(value)` on a hybrid channel (`kind == INTER_PROCEDURE`, `pluginSink.isPresent()`), the dispatcher SHALL execute the following sequence on the producer thread:

1. Fan out the typed value to every consumer mailbox listed in `descriptor.consumers()`. This step SHALL complete before step 2 begins.
2. If `pluginSink.isPresent()`, invoke `pluginSink.get().serializer().apply(value)` exactly once inside `try { ... } catch (RuntimeException | Error e) { ... }`. On normal return, send the bytes through the existing plugin-wire framing path (`channel: String` wire-metadata key set to `descriptor.name()`; `[key_len][val_len][key][value]` block format; via `FrameEncoder.build(...)` and `NativeTransport.streamWrite(...)`). On caught throw, throw `ProcedureException.PluginSerializerFailure(channel, message, cause)` to the producing Procedure's `ctx.emit` call without emitting any plugin bytes. `Throwable` is intentionally NOT caught — `OutOfMemoryError` and similar JVM-level signals propagate.

The dispatcher thread SHALL remain alive across a serializer-thrown exception. Subsequent emits SHALL proceed normally; only the producing Procedure sees the per-emit error.

The serializer SHALL run exactly once per `ctx.emit` invocation regardless of consumer count. The byte buffer produced by the serializer SHALL NOT be cached or reused across emits.

#### Scenario: Hybrid emit fans out typed value to all consumers and bytes to plugin (Java)
- **WHEN** a producer Procedure's `run` calls `ctx.emit(value)` on a channel wired to `[ConsumerA.class, ConsumerB.class]` with `pluginSink = Optional.of(new PluginSink("indicators", serializer))`
- **THEN** `ConsumerA.run` and `ConsumerB.run` SHALL each be invoked with `value` (passed by reference per existing inter-procedure delivery semantics)
- **AND** the agent-side plugin "indicators" SHALL receive a single wire frame whose body bytes equal `serializer.apply(value)` and whose metadata contains `channel = <channel name>`

#### Scenario: Single serializer call regardless of consumer count (Java)
- **WHEN** a hybrid channel has N inter-procedure consumers and the producer emits one value
- **THEN** the serializer SHALL be invoked exactly once for that emit
- **AND** N is unconstrained (1, 3, 100 — same single-call guarantee)

#### Scenario: Throwing serializer surfaces as ProcedureException without crashing the dispatcher (Java)
- **WHEN** the serializer throws `RuntimeException` on a particular input
- **THEN** the inter-procedure consumers SHALL still receive the typed value (consumer fan-out completed before serialization)
- **AND** `ctx.emit(...)` SHALL throw `ProcedureException.PluginSerializerFailure { channel(), getMessage(), getCause() }`
- **AND** the dispatcher thread SHALL remain alive — a subsequent `ctx.emit` call SHALL execute normally

### Requirement: emitWithMeta is valid on hybrid channels (Java SDK)
`ProcedureCtx.emitWithMeta(value, meta)` SHALL accept the call when either:

- the channel is `kind == PLUGIN_BOUND` (existing rule), OR
- the channel has `pluginSink.isPresent()` (new rule for hybrid channels).

On a hybrid channel, the inter-procedure consumers SHALL receive the typed value with no metadata (metadata is plugin-framing only). The plugin sink SHALL receive bytes (via the channel's serializer) with the supplied `Map<String, String>` attached to the wire frame using the existing `[key_len][val_len][key][value]` block format. The reserved `channel` key SHALL be set automatically and SHALL NOT be overridable by user-supplied keys; an attempt to set it SHALL be silently dropped during framing (matches the existing rule for `PLUGIN_BOUND` channels).

On a pure-`INTER_PROCEDURE` channel with no plugin sink, `emitWithMeta` SHALL continue to throw `ProcedureException.MetadataNotAllowedOnInterProcedureChannel` without delivering the value or the metadata.

#### Scenario: emitWithMeta on a hybrid channel forwards metadata to plugin only (Java)
- **WHEN** a producer Procedure's `run` calls `ctx.emitWithMeta(value, Map.of("trace_id", "abc123"))` on a hybrid channel wired to one consumer and one plugin sink
- **THEN** the consumer SHALL receive `value` with no `trace_id` visible
- **AND** the plugin wire frame SHALL contain `trace_id = "abc123"` plus the implicit `channel = <channel name>` key

#### Scenario: emitWithMeta on a pure inter-procedure channel still throws (Java)
- **WHEN** a producer Procedure's `run` calls `ctx.emitWithMeta(value, Map.of("trace_id", "abc123"))` on a channel that has consumers but no `pluginSink`
- **THEN** `ctx.emitWithMeta` SHALL throw `ProcedureException.MetadataNotAllowedOnInterProcedureChannel` carrying the channel name
- **AND** neither the consumers nor any plugin SHALL receive the value or the metadata

#### Scenario: Reserved metadata keys are stripped on hybrid channels (Java)
- **WHEN** a producer calls `ctx.emitWithMeta(value, Map.of("channel", "spoof", "trace_id", "abc"))` on a hybrid channel
- **THEN** the wire frame SHALL contain `channel = <real channel name>` (the reserved value, not "spoof") and `trace_id = "abc"`

### Requirement: New OperativeConfigurationException factory — `duplicatePluginSink` (Java SDK)
`OperativeConfigurationException` (in `sdk/java/flezha-sdk-api/src/main/java/com/flezha/sdk/operative/OperativeConfigurationException.java`) SHALL gain a static factory `duplicatePluginSink(String channel)` that produces an exception whose message names the offending channel. The factory SHALL be raised at `OperativeRegistry.registerStream(...)` time when `alsoToPluginWith` is called more than once on the same channel. The first call registers the sink; the second SHALL accumulate the exception into the builder's error vector and SHALL NOT overwrite the previously-registered sink. The aggregate `OperativeRegistry.registerStream(...)` validation SHALL throw a `OperativeConfigurationException` listing every accumulated error with the standard "operative topology validation failed" prefix.

#### Scenario: Second alsoToPluginWith call is rejected at registration (Java)
- **WHEN** a Operative's `channels(c)` method calls `c.channel(T.class, "foo").from(S.class).alsoToPluginWith("p1", f1).alsoToPluginWith("p2", f2)`
- **THEN** `OperativeRegistry.registerStream(R.class)` SHALL throw `OperativeConfigurationException` whose message contains both the verb `duplicatePluginSink` (or equivalent diagnostic phrasing) and the channel name `"foo"`
- **AND** the registered topology SHALL be discarded

### Requirement: New ProcedureException subclass — `PluginSerializerFailure` (Java SDK)
`ProcedureException` (in `sdk/java/flezha-sdk-api/src/main/java/com/flezha/sdk/procedure/ProcedureException.java`) SHALL gain a nested `final class PluginSerializerFailure extends ProcedureException`. Constructor: `(String channel, String message, Throwable cause)`. The class SHALL be thrown from `ctx.emit(...)` (and `ctx.emitWithMeta(...)`) on a hybrid channel when the user-supplied serializer throws `RuntimeException` or `Error`. `channel()` accessor SHALL return the channel name as declared in `ChannelBuilder`; `getMessage()` SHALL include the underlying exception's `toString()` and the channel name; `getCause()` SHALL be the original throwable.

The producing Procedure's `Procedure.run` SHALL receive the exception via the normal `throws ProcedureException` declaration on `ctx.emit`. Catching the exception and continuing is valid; rethrowing from `Procedure.run` continues to discard partial emits per the existing trait contract.

#### Scenario: Producer Procedure sees PluginSerializerFailure and chooses how to handle it (Java)
- **WHEN** a hybrid channel's serializer throws during `ctx.emit(value)`
- **THEN** `ctx.emit(value)` SHALL throw `ProcedureException.PluginSerializerFailure` carrying the channel name and the underlying cause
- **AND** the producing Procedure MAY catch (logging or swallowing) or rethrow — both are valid handlers
- **AND** the inter-procedure consumer fan-out SHALL have already completed before the exception was thrown

#### Scenario: Throwable causes other than RuntimeException/Error are not caught (Java)
- **WHEN** the serializer throws `OutOfMemoryError`
- **THEN** the dispatcher SHALL NOT catch it — the JVM's normal escalation path applies (the worker thread terminates, the runner's uncaught-exception handler runs)
- **AND** the consumer fan-out for that emit SHALL have already completed

### Requirement: Worker pool reuses existing dispatch infrastructure

The Runner pool SHALL be the SDK's poll workers over the shared activation
engine (`flz_core::poll_core`): one base set sized by `FLEZHA_WORKER_POOL_SIZE`
(default: available parallelism), grown under starvation up to
`FLEZHA_WORKER_POOL_MAX`. There SHALL be no second, core-owned pool — workflow
stage work is handed out as step activations the same workers serve — and the
per-Entity `inflight` CAS guard SHALL be the same mechanism the per-`EntitySlot`
guard uses.

#### Scenario: Default pool sizing
- **WHEN** the SDK is started without `FLEZHA_WORKER_POOL_SIZE` set on a host with 8 cores
- **THEN** the runner pool SHALL start 8 OS threads

#### Scenario: Env override honored
- **WHEN** `FLEZHA_WORKER_POOL_SIZE=4` is set on a host with 8 cores
- **THEN** the runner pool SHALL start exactly 4 OS threads

#### Scenario: One worker set serves both operative profiles
- **WHEN** a workflow operative runs a stage while stream entities are being dispatched
- **THEN** both SHALL be served by the same workers, and no core-owned thread SHALL run user code

### Requirement: No coexistence with OperativeHandler API
The `OperativeHandler` trait, `OperativeResult` enum, `OperativeRunner` enum, `register_factory()` function, `FactoryMetadata` struct, and `InputTransformer` / `OutputTransformer` traits SHALL be removed from the public Rust SDK API in the same change that introduces the `StreamOperative` / `Procedure` traits. There SHALL be no transitional period in which both APIs are usable simultaneously.

**Reason**: The library is unpublished, the only known runner-app consumer is in this monorepo and migrates atomically as part of the same change, and the user has an explicit "no legacy / backward-compat shims" policy. Coexistence would also force the new abstraction to retain dispatch-by-operative-id baggage that the new model deliberately leaves behind.

**Migration**: runner apps using `OperativeHandler` rewrite to `StreamOperative` + one or more `Procedure` impls in the same change. Concrete example: `flezha_market/market-operative/src/main.rs` and `market_computation.rs` are rewritten in the `operative-mvp-rust` change. State (formerly `Assets`) is decomposed into per-Procedure state types.

#### Scenario: Old API is not importable
- **WHEN** a Rust file attempts to `use flezha_runner::OperativeHandler` after the cutover
- **THEN** the compilation SHALL fail with an unresolved-import error

#### Scenario: Old registration function is removed
- **WHEN** a Rust file attempts to call `register_factory(...)` after the cutover
- **THEN** the compilation SHALL fail with an unresolved-name error

### Requirement: Dynamic-mode lazy entity creation
For dynamic-mode Procedures, the runtime SHALL create an `EntitySlot` for a key on the first input whose consumer edge's key function returns a key for which no slot already exists. The key function is declared on the wiring via `key_by` / `keyBy` and is total — it returns a `String`, never an absence — so there is no keyless-drop path; discarding an input is expressed with `filter` before the keying. Slot creation SHALL be synchronous within the dispatch path: allocate the slot, allocate the mailbox at the Procedure's declared `mailbox_capacity()`, push the triggering input, submit a runner job. The per-Procedure `entities_created_total` counter SHALL be incremented on each create.

Concurrent producers pushing inputs for the same brand-new key SHALL all observe the same `EntitySlot` instance after creation. The runtime SHALL NOT allocate two distinct slots for the same key under any concurrency pattern.

#### Scenario: First input with a fresh key creates the entity
- **WHEN** a Procedure reached by a keyed edge receives an input whose key function returns `"user-42"` and no Entity for `"user-42"` exists
- **THEN** the runtime SHALL create an Entity keyed `"user-42"`, push the input onto its mailbox, increment `entities_created_total`, and submit a runner job

#### Scenario: Subsequent inputs with the same key reuse the entity
- **WHEN** a second input arrives whose key function returns `"user-42"` after the first one created the slot
- **THEN** the runtime SHALL push onto the existing Entity's mailbox; `entities_created_total` SHALL NOT increment

#### Scenario: Filtering, not a null key, discards an input
- **WHEN** an author wants inputs of a given shape not to reach any Entity
- **THEN** they SHALL declare `filter` before `key_by` on the edge, and the runtime SHALL offer no keyless-drop path

#### Scenario: Concurrent producers for the same fresh key share one slot
- **WHEN** N producer threads simultaneously push the first inputs for the same brand-new key `"user-99"`
- **THEN** the runtime SHALL allocate exactly one `EntitySlot` for `"user-99"` and all N inputs SHALL land on its single mailbox in some serial order

### Requirement: Per-Procedure idle-TTL eviction is opt-in (no eviction by default)
`Procedure::entity_idle_ttl()` SHALL return `Option<Duration>` and SHALL default to `None`. When it returns `None`, the Procedure's dynamic Entities SHALL NOT be evicted — they live for the lifetime of the Operative (as static Entities do) and no reaper thread SHALL be spawned for the Procedure.

When `entity_idle_ttl()` returns `Some(ttl)`, the SDK SHALL run one reaper thread for the Procedure that periodically evicts Entities whose time since last input push exceeds `ttl`. The reaper SHALL acquire the Entity's `inflight` CAS before evicting and SHALL confirm its mailbox is empty; if the CAS is contended, the reaper SHALL skip the Entity and try again on the next sweep. Eviction SHALL increment the per-Procedure `entities_evicted_total` counter.

The reaper SHALL sweep on a cadence derived as `min(ttl / 4, 30s)`, overridable globally via the `FLEZHA_REAP_INTERVAL_MS` environment variable for testing.

#### Scenario: No eviction by default
- **WHEN** a dynamic-mode Procedure does not override `entity_idle_ttl()` (it returns `None`)
- **THEN** no reaper thread SHALL be spawned and its Entities SHALL never be evicted by idle TTL

#### Scenario: Idle entity is evicted after TTL elapses
- **WHEN** a Procedure sets `entity_idle_ttl() = Some(ttl)` and an Entity has had no input push for longer than `ttl` and is not currently running
- **THEN** the next reaper sweep SHALL acquire its inflight CAS, evict it from the registry, and increment `entities_evicted_total`

#### Scenario: Active entity is not evicted mid-run
- **WHEN** an Entity's `Procedure::run` is in flight (its inflight CAS is held) when the reaper sweeps
- **THEN** the reaper SHALL skip this Entity for this sweep without evicting; eviction SHALL be reattempted on the next sweep

#### Scenario: Recently active entity is preserved
- **WHEN** an Entity received an input push within the past `ttl` interval
- **THEN** the reaper SHALL NOT evict it on this sweep

### Requirement: Eviction is destructive; recreate is a cold start
Eviction of a dynamic-mode Entity SHALL be destructive. When an Entity is evicted AND persistence is enabled for the Operative class, the SDK SHALL ship a fire-and-forget `delete_entity_state{entity_id}` frame to the controller so its on-disk WAL/snapshot/outbox files are removed. The SDK SHALL NOT block the reaper on a response.

When a dynamic-mode Entity has been evicted and a subsequent input arrives for the same key, the runtime SHALL recreate the `EntitySlot` synchronously (per "Dynamic-mode lazy entity creation") and SHALL call `Procedure::init_state(Some(key), None)` — a fresh cold start. The runtime SHALL NOT restore the evicted Entity's state from disk on recreate. (This is a deliberate decision: it keeps the recreate path off any blocking controller round-trip, so a recreate can never stall a work-stealing pool worker.)

#### Scenario: Recreate after eviction is a cold start
- **WHEN** a dynamic-mode Procedure with persistence enabled is reaped and a subsequent input arrives for the same key
- **THEN** the runtime SHALL recreate the slot and call `Procedure::init_state(Some(key), None)`; the Procedure's `run` SHALL observe freshly-initialized state, not the state from before eviction

#### Scenario: Eviction deletes persisted state
- **WHEN** a persistence-enabled dynamic-mode Entity is evicted
- **THEN** the SDK SHALL ship `delete_entity_state{entity_id}` to the controller, which SHALL remove the Entity's `{key}.wal`, `{key}.snap.{seq}`, and `{key}.outbox.dedup` files

#### Scenario: Recreate without persistence is a cold start
- **WHEN** a dynamic-mode Procedure with persistence disabled is reaped and a subsequent input arrives for the same key
- **THEN** the runtime SHALL call `Procedure::init_state(Some(key), None)` and the Procedure's `run` SHALL observe the freshly-initialized state

### Requirement: Soft cap on dynamic entities per Procedure
Each dynamic-mode Procedure SHALL declare a maximum entity count via `fn max_entities() -> usize` (default `100_000`). When the per-Procedure entity count reaches this cap, the runtime SHALL drop new-key inputs without allocating an Entity and SHALL increment `inputs_dropped_capped_total`. Inputs whose edge key function resolves to a key with an existing Entity SHALL NOT be affected by the cap; they SHALL be processed normally.

The cap is soft: enforcement is best-effort against concurrent inserts. The runtime SHALL NOT propagate an over-cap rejection back to the agent; the input is silently dropped and the counter is incremented.

#### Scenario: Cap reached drops new-key inputs
- **WHEN** a dynamic-mode Procedure with `max_entities = 5` already owns 5 Entities and an input arrives whose edge key function returns a 6th unseen key
- **THEN** the runtime SHALL drop the input, SHALL NOT allocate an Entity, and SHALL increment `inputs_dropped_capped_total`

#### Scenario: Cap does not affect inputs for existing keys
- **WHEN** a dynamic-mode Procedure is at its cap and an input arrives whose edge key function returns a key that already has an Entity
- **THEN** the runtime SHALL push to that Entity's mailbox normally; `inputs_dropped_capped_total` SHALL NOT increment

#### Scenario: Cap recovers as TTLs elapse
- **WHEN** a dynamic-mode Procedure is at its cap, time passes such that some Entities exceed `entity_idle_ttl()`, and the reaper evicts them
- **THEN** subsequent new-key inputs SHALL allocate Entities normally up to the cap

### Requirement: Per-Procedure dynamic-mode counters
Each dynamic-mode Procedure SHALL expose the following monotonically-increasing counters on its registry, backed by `AtomicU64`:

- `entities_created_total` — incremented once per `EntitySlot` allocation (including a cold recreate after eviction).
- `entities_evicted_total` — incremented once per eviction.
- `inputs_dropped_capped_total` — incremented when an input is dropped due to the soft cap.

Counters SHALL be observable via the same per-Entity / per-Procedure observability surface as the bounded-mailbox counters from `operative-procedure-entity-model`'s "Per-Entity mailbox is bounded with declared capacity" requirement. Export to a metrics sink is not in scope for this requirement; this requirement only mandates the data is captured.

#### Scenario: Counter shape after a churn cycle
- **GIVEN** a dynamic-mode Procedure with a TTL set
- **WHEN** 100 fresh keys are pushed, 30 of those 100 are subsequently reaped, then 10 of the reaped keys see a fresh input each
- **THEN** `entities_created_total = 110` (100 initial + 10 cold recreates), `entities_evicted_total = 30`

### Requirement: The workload model names exactly three levels
The system SHALL name the workload hierarchy **Operative → Procedure → Entity**, and every artifact — source, comment, document, specification, wire name, and on-disk name — SHALL use those names for those levels.

An **Operative** is the deployable unit: it has an id and a namespace, is placed on a Runner, and is started, stopped, migrated, suspended, and crash-resumed. It carries a kind (`workflow`, `stream`) and MUST remain able to accept further kinds as a qualifier.

A **Procedure** is a typed stage inside an Operative. It declares `Input`, `Output`, `State`, `Change`, and `Clock`, implements `run` / `apply_change` / `compensate`, and is wired to other Procedures by Channels.

An **Entity** is one keyed instance of a Procedure. It owns its mailbox, its state, its WAL, its snapshots, and its outbox dedup, and it is the unit of idle eviction and compaction.

#### Scenario: Containment is stated consistently
- **WHEN** any document, module doc, or specification describes the hierarchy
- **THEN** it MUST state that an Operative contains Procedures and a Procedure holds one Entity per key, and MUST NOT name any level with a term this specification retires

#### Scenario: Level 1 accepts a new kind without renaming
- **WHEN** a workload kind beyond `workflow` and `stream` is introduced
- **THEN** it MUST be expressible as a qualifier on `Operative` (for example "scheduled Operative") with no change to the level's name

### Requirement: A Procedure resolves to exactly one entity mode
Each Procedure SHALL resolve, once at registration, to exactly one of three entity modes. Single and Static modes SHALL be derived from the Procedure's own declarations; Dynamic mode SHALL be derived from the wiring — a Procedure reached by a consumer edge carrying a key function is Dynamic.

- **Single** — exactly one Entity, carrying no key.
- **Static** — a fixed keyset declared by the Procedure; one input MAY be routed to several Entities.
- **Dynamic** — a stream keyset; Entities are created on first sight of a key and MAY be idle-evicted.

Wiring a key function onto a Procedure that declares a static keyset SHALL be a build error.

#### Scenario: Single mode carries no key
- **WHEN** a Procedure declares no static keyset and is reached only by edges carrying no key function
- **THEN** it resolves to Single mode and holds exactly one Entity, and every diagnostic, path, and document referring to that Entity MUST remain correct without naming a key

#### Scenario: Static mode may fan one input out
- **WHEN** a Static-mode Procedure's routing declaration returns more than one key for a single input
- **THEN** that input MUST be delivered to the Entity for each returned key

#### Scenario: Dynamic mode creates and evicts
- **WHEN** a Procedure reached by a keyed edge receives an input whose key has no live Entity
- **THEN** an Entity for that key MUST be created before the input is enqueued, and MUST be eligible for idle eviction if the Procedure declares an idle TTL

#### Scenario: Keying a static-mode Procedure is refused
- **WHEN** an edge carrying a key function wires a Procedure declaring a non-empty static keyset
- **THEN** the build SHALL fail naming the Procedure, rather than one declaration silently winning

### Requirement: Declarations are named for the level they describe
A declaration SHALL be named for the level whose values it produces or consumes, not for the level that hosts it.

Entity selection and lifetime declarations SHALL therefore be named for the Entity: `entity_keys()` (the static keyset), `entities_for()` (static routing, plural because one input may fan out), `key_by` (the dynamic key, declared on the wiring), and `entity_idle_ttl()`. A control-plane message addressed to an Operative SHALL be named for the Operative.

#### Scenario: Entity-key declarations name the Entity
- **WHEN** an author declares a Procedure's static keyset or dynamic key hook
- **THEN** the declaration MUST be named `entity_keys` or `entity_key` respectively, in both SDKs, and MUST NOT be named after the Procedure

#### Scenario: Static routing is plural
- **WHEN** an author declares Static-mode routing
- **THEN** the declaration MUST be named `entities_for`, reflecting that its result MAY name several Entities

#### Scenario: An operative-addressed message names the Operative
- **WHEN** a control-plane message carries only an operative identifier and no procedure or entity identifier
- **THEN** it MUST be named for the Operative

### Requirement: Durable and wire artifacts are named for the level that owns them
Wire message names, data-stream payload ids, and on-disk filenames SHALL use the same vocabulary as the source and the documentation, with no separate protocol-level or storage-level spelling.

Per-Entity durable state SHALL live at `state/{operative_class}/{procedure_id}/{entity_key}.*`. Per-Procedure journal and inbox artifacts SHALL be named for the Procedure.

#### Scenario: Payload ids match the documented vocabulary
- **WHEN** a runner ships a state change, a snapshot, a delete, or a compaction request for an Entity
- **THEN** the payload id MUST name the Entity, and MUST match the term used for that concept in `docs/`

#### Scenario: On-disk names match the documented vocabulary
- **WHEN** a workflow Operative writes its journal or its durable inbox
- **THEN** the filename MUST name the Procedure level, and MUST NOT use a retired abbreviation

#### Scenario: State written before the rename is not read after it
- **WHEN** a Controller starts against durable state written under the retired names
- **THEN** that state is not recovered, and the documented upgrade path is a full reset of runtime data

### Requirement: Retired vocabulary does not survive anywhere
The terms `Operation`, `Segment`, and the `Op*` abbreviation family SHALL NOT appear as domain vocabulary in any tracked file — source, comment, test, fixture identifier, document, specification, protobuf definition, payload id, or on-disk name.

No alias, deprecated spelling, or compatibility shim for the retired terms SHALL be introduced. Subsequent work SHALL NOT reintroduce them, including when referring to behaviour that predates this change.

The word "Operator" remains reserved for ProcessHub's ring-node jargon and is unaffected. Ordinary English uses of "operation" that do not name this model's level — for example "no-op" — are unaffected.

#### Scenario: A sweep of tracked files finds nothing
- **WHEN** tracked files are searched for the retired terms as whole words
- **THEN** the search MUST return no domain-vocabulary hit, and any remaining hit MUST be a documented exception

#### Scenario: No compatibility spelling exists
- **WHEN** an author uses a retired name in either SDK
- **THEN** the build MUST fail, rather than resolving through an alias or deprecated path

#### Scenario: Later work does not reintroduce the old terms
- **WHEN** a subsequent change describes behaviour introduced before this rename
- **THEN** it MUST describe that behaviour in the current vocabulary and MUST NOT reference the retired names

### Requirement: The architecture documentation states the model as a diagram
`docs/architecture/README.md` SHALL carry a diagram of the containment model — Controller and Runner above, then Operative, then Procedure, then Entity, with Channels as the edges between Procedures — and SHALL show the three entity modes and what distinguishes them.

#### Scenario: The diagram shows containment
- **WHEN** a reader opens the architecture documentation
- **THEN** the diagram MUST show which level contains which, without requiring the surrounding prose to disambiguate it

#### Scenario: The diagram shows the entity modes
- **WHEN** the diagram describes a Procedure
- **THEN** it MUST show that a Procedure resolves to Single, Static, or Dynamic, and state for each how many Entities result and whether they are keyed

### Requirement: Entry-edge mailbox-full handling and the shared wire reader

The SDK's wire-reader thread (the per-stream loop in `command/streams_connect.rs`) services every Operative sharing one physical connection. Under the default entry-channel policy (`Block`), a push into a full consumer mailbox SHALL park the wire-reader thread until the consumer drains an item; the frame SHALL NOT be discarded. While parked, the reader SHALL NOT run the saturated Operative's procedures itself, and sibling Operatives on the same connection SHALL NOT receive new frames until the parked push completes. The SDK SHALL document this co-tenancy cost in `docs/operative_architecture.md` and `docs/sdk_protocol_spec.md`.

Under a declared drop policy (`.on_full(OverflowPolicy::DropNewest)` or `DropOldest`) the wire-reader thread SHALL NOT block on a full mailbox: the frame's typed payload SHALL be discarded (or the oldest item evicted) per the policy, and the loop SHALL immediately attempt to read the next frame from the wire.

#### Scenario: Default entry Block parks the reader and loses nothing
- **WHEN** Operative A declares `c.input::<Vec<u8>>("x").into::<S>()` without `.on_full(...)`, its entry mailbox saturates, and Operative B shares the physical wire connection
- **THEN** the wire-reader thread SHALL park on the push and B SHALL NOT receive new frames until the parked push completes; once the pool drains A's mailbox every frame addressed to A and B SHALL land, and A's `drops_newest_total` and `drops_oldest_total` SHALL be unchanged

#### Scenario: A declared drop policy keeps the wire reader running
- **WHEN** Operative A's entry channel declares `.on_full(OverflowPolicy::DropNewest)`, its mailbox is full, and Operative B (sharing the physical wire connection) has frames pending
- **THEN** the wire reader SHALL discard the frame addressed to A and continue dispatching the next frame; B's frames SHALL NOT be delayed by A's saturation

