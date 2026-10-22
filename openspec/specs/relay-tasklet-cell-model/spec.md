# relay-tasklet-cell-model Specification

## Purpose
This capability defines the structural execution model for Relays: bounded per-Cell mailboxes with a per-channel overflow policy, the `Tasklet` stage trait surface (state, change, clock, config, persistence), typed compile-checked channels, and dynamic cell mode (lazy creation, idle-TTL eviction, and soft caps).
It also names the three-level workload vocabulary itself — Relay → Tasklet → Cell — and the rule that every declaration, wire name, and on-disk name is named for the level it describes.
## Requirements
### Requirement: Hierarchy and placement
The system SHALL realize the role relationship as follows:

```
Controller (1 per deployment)
  ↓ (runs N ProcessHub instances, one per namespace)
  → Namespace
    ↓ (ProcessHub picks placement by consistent hash)
    → Relay instance (1 per `flz relay start`)
      ↓ (the Relay's Overwatch binds a `runner_id` as it starts)
      → Runner (SDK process) — may host multiple Relays
        ↓ (SDK-internal cell; not ProcessHub-aware)
        → Tasklet (declared per Relay class)
          ↓
          → Cell (per declared key, or default 1)
            ↓
            → Channel (typed I/O port)
```

The Controller SHALL hold the authoritative placement table. Runners SHALL register with the Controller via the control channel and SHALL receive `AddRelay` / `RemRelay` commands. Cell beneath the Relay level (Tasklets, Cells, Channels) is SDK-internal and SHALL NOT be ProcessHub-aware.

#### Scenario: Controller distributes Relays across Runners
- **WHEN** a Controller has two healthy Runners A and B and `flz relay start` is invoked twice
- **THEN** ProcessHub SHALL place each Relay on a Runner according to its consistent-hash policy; the Controller SHALL issue `AddRelay` over the corresponding Runner's control channel

#### Scenario: Each Relay runs on exactly one Runner at a time
- **WHEN** a Relay is placed on Runner A
- **THEN** ProcessHub SHALL ensure no concurrent `AddRelay` for the same `relay_id` is issued to any other Runner; migrations SHALL go through the documented HotSwap-with-handover path

### Requirement: Per-Cell mailbox is bounded with declared capacity
Each CellSlot's mailbox SHALL have a fixed capacity declared at relay startup. The mailbox SHALL refuse a push that would exceed this capacity; the SDK then applies the source channel's `OverflowPolicy` (see "Overflow policy declared per channel"). Capacity SHALL NOT change at runtime once the relay is started.

The capacity SHALL be the value returned by the consumer Tasklet's `mailbox_capacity()` trait method, evaluated once per Tasklet at `register_stream::<R>()` time after applying the precedence rules in "Mailbox capacity precedence". Static-mode Tasklets SHALL apply the same capacity to every Cell declared by `cell_keys()`.

The mailbox SHALL refuse capacity values of 0 (which would always overflow) and values strictly greater than 1_048_576 (1 MiB items). Relay startup SHALL fail with an error naming the offending Tasklet when `mailbox_capacity()` returns an out-of-range value.

#### Scenario: Default capacity is 1024
- **WHEN** a Tasklet does not override `mailbox_capacity()` and no `FLEZHA_MAILBOX_CAPACITY` env var is set
- **THEN** the runtime SHALL allocate a mailbox of capacity 1024 for every Cell of that Tasklet

#### Scenario: Override on the Tasklet takes precedence
- **WHEN** a Tasklet overrides `mailbox_capacity() -> usize { 4096 }` and sets `MAILBOX_CAPACITY_OVERRIDDEN = true`
- **THEN** every Cell of that Tasklet SHALL have a mailbox of capacity 4096, regardless of any `FLEZHA_MAILBOX_CAPACITY` env value

#### Scenario: Env var override applies when no Tasklet override
- **WHEN** `FLEZHA_MAILBOX_CAPACITY=2048` is set and a Tasklet does not override `mailbox_capacity()`
- **THEN** every Cell of that Tasklet SHALL have a mailbox of capacity 2048

#### Scenario: Static-mode cells share one capacity
- **WHEN** a Tasklet declares `cell_keys() = ["1m","1h","1d","1w"]` and `mailbox_capacity() = 4096`
- **THEN** the runtime SHALL create four mailboxes, each of capacity 4096

#### Scenario: Capacity 0 is rejected at startup
- **WHEN** a Tasklet returns 0 from `mailbox_capacity()`
- **THEN** Relay startup SHALL fail with an error naming the Tasklet

#### Scenario: Capacity above maximum is rejected at startup
- **WHEN** a Tasklet returns 2_000_000 from `mailbox_capacity()`
- **THEN** Relay startup SHALL fail with an error naming the Tasklet

### Requirement: Mailbox capacity precedence
The SDK SHALL resolve a Tasklet's mailbox capacity at `register_stream::<R>()` time using the following ordered rule:

1. If `Tasklet::MAILBOX_CAPACITY_OVERRIDDEN` is `true`, use the value returned by `Tasklet::mailbox_capacity()`.
2. Otherwise, if the `FLEZHA_MAILBOX_CAPACITY` environment variable is set to a non-empty string parseable as `usize`, use that value.
3. Otherwise, use the built-in default of 1024.

The override-detection const `MAILBOX_CAPACITY_OVERRIDDEN` SHALL exist on the `Tasklet` trait alongside the existing `CELLS_FOR_OVERRIDDEN` const; users overriding `mailbox_capacity()` MUST also set `MAILBOX_CAPACITY_OVERRIDDEN = true`. `CELL_KEY_OVERRIDDEN` SHALL NOT exist, because dynamic keying is no longer a trait override.

#### Scenario: Override-flag false is treated as "not overridden"
- **WHEN** a Tasklet overrides `mailbox_capacity() -> usize { 4096 }` but leaves `MAILBOX_CAPACITY_OVERRIDDEN` at its `false` default and `FLEZHA_MAILBOX_CAPACITY=2048` is set
- **THEN** the resolved capacity SHALL be 2048 (env-var precedence applies because the SDK does not see the override flag as true)

#### Scenario: Env var set but unparseable is ignored
- **WHEN** `FLEZHA_MAILBOX_CAPACITY=banana` is set and the Tasklet does not override
- **THEN** the resolved capacity SHALL fall through to the default 1024

### Requirement: Overflow policy declared per channel

Each channel declared via `c.input::<T>(...)` (entry) or `c.channel::<T>(...).from::<S>()` (inter-tasklet) SHALL carry an `OverflowPolicy` value applied at the producer-side push site when the consumer's mailbox is full. Plugin-bound channels (`.to_plugin(...)`) SHALL NOT carry an `OverflowPolicy` because no SDK-side mailbox sits between the producer Tasklet and the wire; declaring `.on_full(...)` on a plugin-bound channel SHALL be a compile error (the chained-builder shape after `.to_plugin(...)` MUST NOT expose `.on_full(...)`).

`OverflowPolicy` SHALL be an enum with exactly three variants, each naming what
the policy discards:

- `DropNothing` — nothing is discarded. The producer's rate is coupled to the consumer's, and ordering is preserved. **How** that coupling is realized depends on the channel kind and is not part of the declaration: on an inter-tasklet channel the producing Cell is withheld further work until the push lands; on an entry channel the shared transport reader stalls, which carries the backpressure to the controller. Neither realization discards anything, which is what the author declared.
- `DropNewest` — the incoming item SHALL be discarded; the consumer's `drops_newest_total` counter SHALL be incremented by 1.
- `DropOldest` — the consumer's mailbox SHALL atomically pop its oldest item and push the incoming one (best-effort under MPMC contention); the consumer's `drops_oldest_total` counter SHALL be incremented by 1.

The variant SHALL be named for its guarantee rather than for its implementation, so that one name stays correct on every channel kind whatever mechanism realizes it. `Block` SHALL NOT be a variant name, on any SDK, on the wire, or in the `#[flezha::tasklet]` attribute.

The policy SHALL be declared via the channel-builder method `.on_full(OverflowPolicy)` on `EntryChannel`, `ChannelDecl`, and `ChannelDeclWithProducer`. When `.on_full(...)` is not called the channel SHALL use the kind-specific default (see "Default overflow policy by channel kind").

#### Scenario: DropNothing discards nothing

- **WHEN** an inter-tasklet channel declares `.on_full(OverflowPolicy::DropNothing)`, the consumer's mailbox is full, and a producer calls `ctx.emit::<C>(value)`
- **THEN** the value SHALL be delivered to the consumer's mailbox once room appears, in the order the producer emitted it; no item SHALL be discarded and neither `drops_newest_total` nor `drops_oldest_total` SHALL change
- **AND** the producer's call to `ctx.emit::<C>(value)` SHALL eventually return `Ok(())` (no `TaskletError` is produced for the backpressure event)
- **AND** no host thread SHALL be held for the duration; the producing Cell is withheld further work instead

#### Scenario: An author's declaration does not change with the mechanism

- **WHEN** the non-parking realization of `DropNothing` is enabled or disabled by deployment configuration
- **THEN** no authoring surface, wire literal, declared default or conformance fixture SHALL change, and the guarantee observed by the author — nothing discarded, order preserved — SHALL be identical in both

#### Scenario: DropNewest discards the incoming item
- **WHEN** an entry channel declares `.on_full(OverflowPolicy::DropNewest)` and a wire frame arrives while the consumer's mailbox is full
- **THEN** the SDK SHALL discard the frame's typed payload; the consumer Cell's `drops_newest_total` SHALL increase by 1; no scheduling of a drain task SHALL occur as a result of this push

#### Scenario: DropOldest replaces the head item
- **WHEN** an inter-tasklet channel declares `.on_full(OverflowPolicy::DropOldest)`, the consumer's mailbox is full, and a producer emits a value
- **THEN** the consumer's mailbox SHALL drop its oldest item and SHALL accept the new item; the consumer Cell's `drops_oldest_total` SHALL increase by 1; the producer's `ctx.emit::<C>(value)` SHALL return `Ok(())`

#### Scenario: Plugin-bound channel rejects on_full at compile time
- **WHEN** a Relay declares `c.channel::<Vec<u8>>("p").from::<S>().to_plugin("kafka").on_full(OverflowPolicy::DropNothing)`
- **THEN** the Rust compiler SHALL emit a "no method named `on_full`" error and the program MUST NOT build

### Requirement: Default overflow policy by channel kind
When a channel does not call `.on_full(...)`, the runtime SHALL apply a default policy determined by the channel's kind:

- Entry channels: `OverflowPolicy::DropNothing`.
- Inter-tasklet channels: `OverflowPolicy::DropNothing`.
- Plugin-bound channels: not applicable (no SDK-side mailbox).

An entry channel SHALL NOT discard a wire frame unless the Relay declared a dropping policy explicitly. Data loss on the ingress path SHALL be a stated intent, never a default: a Relay that wants shedding under saturation declares `.on_full(OverflowPolicy::DropNewest)` or `.on_full(OverflowPolicy::DropOldest)`.

The three SDK builders and the FFI host default SHALL stay in lockstep: the `#[default]` variant of `OverflowPolicy` (`crates/flz_core/src/channel.rs`, read at channel creation by `sdk/rust/src/channel/builder/state.rs`), the policy assigned by `declareChannel` (`RelayTopologyBuilder.java`), the `:on_full` fallback in `Flezha.SDK.Relay` and the `#[default]` an absent `overflow` deserializes to (`OverflowPolicy` itself, read by `crates/flz_core/src/host/decl.rs`) SHALL all name `DropNothing`.

These defaults SHALL be reflected in the `ChannelDescriptor.overflow_policy` field set by `build_topology` for every channel for which the user did not explicitly call `.on_full(...)`.

#### Scenario: Default entry policy discards nothing
- **WHEN** a Relay declares `c.input::<Vec<u8>>("trade.raw").into::<ParseTasklet>()` without `.on_full(...)` and a frame arrives while the consumer mailbox is full
- **THEN** the frame SHALL be delivered once room appears; it SHALL NOT be discarded and `drops_newest_total` SHALL NOT change

#### Scenario: An entry channel still drops when it says so
- **WHEN** a Relay declares `c.input::<Vec<u8>>("trade.raw").on_full(OverflowPolicy::DropNewest).into::<ParseTasklet>()` and a frame arrives while the consumer mailbox is full
- **THEN** the frame SHALL be discarded and `drops_newest_total` SHALL increase by 1

#### Scenario: The wire literal follows the variant

- **WHEN** a host class declares a channel with the default policy
- **THEN** its `HostClassDecl` JSON SHALL carry `"overflow": "drop_nothing"`, and a payload carrying `"block"` SHALL be refused at deserialization with an error naming the unknown variant and listing the three accepted spellings

### Requirement: Per-Cell backpressure counters
Each CellSlot's mailbox SHALL expose four `AtomicU64` counters readable via a `CellCounters` accessor:

- `pushes_total` — incremented on every successful push.
- `pushes_blocked_total` — incremented once per `Block` park event (multiple parks for one push due to multiple-producer contention SHALL each increment).
- `drops_newest_total` — incremented per discarded item under `DropNewest`.
- `drops_oldest_total` — incremented per discarded item under `DropOldest`.

The counters SHALL use `Ordering::Relaxed` for both reads and writes. The counters SHALL be visible to in-process code (e.g. via `cell.mailbox.counters()`); export to external metrics systems (Prometheus, StatsD, log) is OUT OF SCOPE for this requirement.

#### Scenario: Counters present at runtime
- **WHEN** a CellSlot is constructed at relay startup
- **THEN** the slot's mailbox SHALL expose a `counters()` accessor returning a struct with the four named atomics, all initialized to 0

#### Scenario: Successful push increments pushes_total
- **WHEN** an inter-tasklet producer emits 10 items into a non-full consumer mailbox
- **THEN** the consumer Cell's `pushes_total` SHALL increase by exactly 10

#### Scenario: DropNewest increments only drops_newest_total
- **WHEN** an entry channel under `DropNewest` policy receives 5 frames while the consumer mailbox is full
- **THEN** `drops_newest_total` SHALL increase by 5; `drops_oldest_total` SHALL be unchanged; `pushes_blocked_total` SHALL be unchanged; `pushes_total` SHALL be unchanged

### Requirement: Tasklet declares SCHEMA_VERSION const
The Rust SDK's `Tasklet` trait SHALL declare `const SCHEMA_VERSION: u32 = 1`. The runtime SHALL stamp this value into the 16-byte header of every persisted Change record for this Tasklet. Users SHALL bump `SCHEMA_VERSION` when they make a non-additive Change-schema edit (e.g. removing or renaming a Change variant).

#### Scenario: Default SCHEMA_VERSION is 1

- **WHEN** a Rust Tasklet does not declare `SCHEMA_VERSION`
- **THEN** the trait default `1` SHALL be inherited and stamped into headers

### Requirement: Tasklet declares compact_after_bytes per-Cell WAL threshold (Rust SDK)
The Rust SDK's `Tasklet` trait SHALL declare `fn compact_after_bytes() -> usize { 64 * 1024 * 1024 }` (default 64 MiB). The runtime SHALL report this value to the agent at relay startup in the existing relay-start metadata so the agent's compaction trigger uses the per-Tasklet value.

#### Scenario: Default 64 MiB threshold reported

- **WHEN** a Rust Tasklet does not override `compact_after_bytes`
- **THEN** the relay-start metadata SHALL carry `compact_after_bytes = 67108864` for this Tasklet

#### Scenario: Per-Tasklet override propagated to agent

- **WHEN** a Rust Tasklet declares `fn compact_after_bytes() -> usize { 16 * 1024 * 1024 }`
- **THEN** the relay-start metadata SHALL carry `compact_after_bytes = 16777216` for this Tasklet

### Requirement: Relay declares its durability in its settings, frozen at instance start (Rust SDK)
The Rust SDK's `Relay` trait SHALL carry the class's durability in `fn settings() -> RelaySettings` as `durable: Durable::Memory | Durable::Local { sleep_after } | Durable::Remote { sleep_after }`, `Durable::Local` with a six-hour `sleep_after` by default. The runtime SHALL resolve a per-instance `keeps_state: bool` (anything but `Durable::Memory`) when the instance is started, for either run, and SHALL declare the durability to the controller in the class's `AddRelay` response (`durable` = `memory`, `local` or `remote`, and `sleep_after_ms` when the class sleeps). A `sleep_after` cannot be written on `Durable::Memory`, which has no such member.

The resolved value SHALL be bound to the `RelayInstance` and SHALL be propagated to every `CellSlot` constructed for that instance. A change to the settings (via SDK redeploy) SHALL NOT affect already-running instances; the new value takes effect on the next `stop_relay` + `start_relay` cycle for that instance.

#### Scenario: The default keeps state

- **WHEN** a Relay of either run does not declare `durable`
- **THEN** `RelayInstance::keeps_state()` SHALL return `true` and the `AddRelay` response SHALL carry `durable = "local"`

#### Scenario: A declared opt-out is honored

- **WHEN** `MarketRelay` declares `durable: Durable::Memory`
- **THEN** `RelayInstance::keeps_state()` SHALL return `false`; the per-Process flusher SHALL drain-and-discard for this instance's Cells; `stub_send_frame` (or its real counterpart) SHALL NOT be invoked for this instance; and the `AddRelay` response SHALL carry `durable = "memory"`

#### Scenario: A steps relay's opt-out is the same setting

- **WHEN** a `Relay` running in steps declares `durable: Durable::Memory`
- **THEN** its Cells SHALL drain-and-discard exactly as a flow relay's do, it SHALL ship no `TaskCommit`, and it SHALL never sleep

#### Scenario: Resolved value frozen at instance start

- **WHEN** a `RelayInstance` is constructed with resolved `keeps_state = true`, AND the class is redeployed with `Durable::Memory` while the instance runs
- **THEN** the running `RelayInstance` SHALL continue with `keeps_state = true`; the new value SHALL take effect only after `stop_relay(instance_id)` and a subsequent `start_relay(...)` invocation

### Requirement: Per-Cell flush buffer registered for non-NoChange Tasklets
The Rust SDK's `CellSlot` SHALL hold a type-erased flush buffer (`Arc<Mutex<Box<dyn FlushableBuffer>>>`) for any Cell whose Tasklet's `Change` type is not `NoChange`. The buffer SHALL be a `Vec<S::Change>` initially empty; `ctx.change(...)` SHALL push onto it. The per-process flusher SHALL acquire the Mutex on each linger tick to drain the buffer.

For Cells whose Tasklet's `Change == NoChange`, the runtime SHALL NOT allocate a flush buffer or register the Cell with the flusher.

#### Scenario: NoChange Tasklet Cells are not registered with the flusher

- **WHEN** the runtime constructs a Cell for a Tasklet with `Change = NoChange`
- **THEN** the `CellFlusher`'s registry SHALL NOT contain an entry for this Cell

#### Scenario: Non-NoChange Tasklet Cells are registered

- **WHEN** the runtime constructs a Cell for a Tasklet with `Change = MyChange`
- **THEN** the `CellFlusher`'s registry SHALL contain an entry keyed by `(relay_instance_id, tasklet_id, cell_key)` with the typed buffer reference, the resolved `keeps_state` flag, the `change_seq: AtomicU64`, and the wire-frame send fn pointer

### Requirement: Four-layer abstraction with defined responsibilities
The SDK SHALL expose a four-layer abstraction comprising Relay, Tasklet, Cell, and Runner. Each layer SHALL have the following exclusive responsibilities and no others SHALL be implicitly added:

- **Relay** — a user-defined struct implementing the `Relay` trait. Spawned by the agent once per `flz relay start` invocation. Owns the channel topology declaration and the registered Tasklets. Holds no domain state of its own.
- **Tasklet** — a user-defined struct implementing the `Tasklet` trait. Defines a stage's typed input, typed output, state type, run logic, and cell strategy. Holds no runtime state of its own; acts as the template for Cells.
- **Cell** — a runtime instance of a Tasklet, identified by an optional string key. Holds exactly one State value, exactly one mailbox, and exactly one `inflight` flag. Created by the SDK at relay startup; not user-instantiable.
- **Runner** — an OS thread in the SDK's worker pool. Drains Cell mailboxes. Not user-visible at the API surface; pool size is configured via the existing `FLEZHA_WORKER_POOL_SIZE` mechanism.

#### Scenario: Each layer has a distinct programmer-visible role
- **WHEN** a developer reads the public API of the new SDK
- **THEN** they SHALL find a `Relay` trait, a `Tasklet` trait, but no `Cell` trait or `Runner` trait — Cells and Runners are runtime artifacts created by the SDK, not types the user implements

#### Scenario: A Relay holds no domain state
- **WHEN** a `Relay` impl is examined
- **THEN** the trait MUST NOT include a state-init or state-handle method; all stateful behavior MUST live on Tasklets

### Requirement: Static and single cell modes are supported in v1
A Tasklet SHALL run in one of exactly three cell modes (the v1 title is retained for MODIFIED-matcher binding; the body below replaces the v1 wording end-to-end and adds dynamic mode):

- **Single cell (default)**: when the Tasklet declares no `cell_keys()` and no consumer edge into it declares a key function. The runtime SHALL create exactly one Cell with `cell_key = None` at relay startup, and SHALL call `Tasklet::init_state(None)` to initialize its state.
- **Static cells**: when the Tasklet implements `fn cell_keys() -> Vec<&'static str>` returning a non-empty list. The runtime SHALL create one Cell per declared key at relay startup, and SHALL call `Tasklet::init_state(Some(key))` for each.
- **Dynamic cells**: when a consumer edge into the Tasklet declares a key function (`key_by` / `keyBy` on the wiring) and the Tasklet leaves `cell_keys()` empty. The runtime SHALL own zero Cells for the Tasklet at relay startup; Cells SHALL be created lazily on the first input for which the edge's key function returns a key with no live Cell (see "Dynamic-mode lazy cell creation"). The key function is total, so there SHALL be no keyless-drop path: discarding an input is expressed with `filter` declared before the keying.

The Relay startup SHALL fail with a clear error message if a Tasklet:

- declares a non-empty `cell_keys()` while a consumer edge into it declares a key function (static and dynamic keying are mutually exclusive).

#### Scenario: Single cell for a Tasklet with no cell declared
- **WHEN** a Tasklet declares no `cell_keys()`, no edge into it is keyed, and the Relay is started
- **THEN** the SDK SHALL create exactly one Cell for that Tasklet with `cell_key = None`, and `Tasklet::init_state(None)` SHALL be called exactly once during startup

#### Scenario: Static cells created at relay startup
- **WHEN** a Tasklet declares `cell_keys() = ["1m", "1h", "1d", "1w"]` and the Relay is started
- **THEN** the SDK SHALL create exactly four Cells for that Tasklet at startup, with `cell_key = Some("1m")`, `Some("1h")`, `Some("1d")`, `Some("1w")` respectively; `init_state` SHALL be called exactly once for each

#### Scenario: Dynamic-mode declaration is accepted at relay startup
- **WHEN** a consumer edge declares a key function into a Tasklet that leaves `cell_keys()` empty
- **THEN** the Relay startup SHALL succeed and the Tasklet SHALL be registered in dynamic mode with zero Cells

#### Scenario: Static keyset plus a keyed edge is rejected
- **WHEN** a Tasklet returns a non-empty `cell_keys()` AND a consumer edge into it declares a key function
- **THEN** Relay startup SHALL fail with an error naming the conflicting Tasklet

### Requirement: Sticky per-cell ordering
For any single Cell, two invocations of its Tasklet's `run(...)` method SHALL NOT execute concurrently, and invocations SHALL observe their inputs in the order those inputs were enqueued onto the Cell's mailbox. This invariant MUST hold across all dispatch modes, all runner pool sizes, and all transport types.

#### Scenario: Same cell is never concurrent
- **WHEN** 10,000 inputs targeting one Cell arrive on its mailbox while the runner pool has 16 threads
- **THEN** at any wall-clock instant, at most one runner thread SHALL be inside that Cell's `run(...)` method

#### Scenario: Different cells of the same tasklet run in parallel
- **WHEN** inputs arrive simultaneously for two distinct Cells of the same Tasklet (for example `("ohlc","1m")` and `("ohlc","1h")`) and the runner pool has ≥ 2 threads
- **THEN** the two Cells' `run(...)` methods MAY execute concurrently on different threads

#### Scenario: Per-cell input order is preserved
- **WHEN** inputs `a`, `b`, `c` are enqueued onto one Cell's mailbox in that order with an append-to-state Tasklet
- **THEN** the Cell's State SHALL end with `[a, b, c]` in exactly that order

### Requirement: Typed channels with compile-time wiring checks
All inter-Tasklet and Tasklet-to-plugin routing SHALL go through Channels declared on the Relay via the producer-first graph builder (`sdk-graph-authoring`). Each Channel SHALL carry its payload type `T`, taken from the producing Tasklet's `Output` type — there is no user-supplied type token and no `.from(...)` clause for inter-tasklet channels. The wiring API SHALL refuse to build in any of these cases:

- A consumer Tasklet whose `Input` associated type is not `T` is wired via `.into::<X>()` / `.into(X.class)` onto a stream of payload type `T`.
- A node is wired as the producer of more than one output channel (the emit surface carries one output channel per tasklet, so a second one could never be routed).

In Rust these refusals SHALL be compile-time where the builder shape permits, otherwise registration-time; in Java they SHALL be registration-time with an error carrying the same diagnostic content.

#### Scenario: Type-mismatched consumer fails to build
- **WHEN** a Tasklet with `type Input = TradeMsg` is wired via `.into::<That>()` onto a stream whose producer declares `type Output = Candle`
- **THEN** the Rust compiler SHALL emit a trait-bound error (or registration SHALL fail where compile-time enforcement is impossible), and in Java registration SHALL throw naming both tasklets and both types

#### Scenario: Channel payload type comes from the producer
- **WHEN** a stream is created by `.into::<OhlcTask>()` where `OhlcTask::Output = Candle`
- **THEN** the resulting channel SHALL carry payload type `Candle` with no type token anywhere in the declaration

#### Scenario: Second output channel for one node fails to build
- **WHEN** a declaration attempts to give one node two output channels
- **THEN** the program MUST fail at compile time (or at registration time if compile-time enforcement is impossible for the chosen builder shape) with an error naming the node

### Requirement: Per-relay channel namespace
Channel names SHALL be scoped to a single Relay instance. Two Relay instances (for example the BTC Relay and the ETH Relay) SHALL each have their own independent channel namespace; a channel named `"trade.raw"` on one Relay SHALL be a distinct logical channel from a channel of the same name on another Relay. Auto-derived channel names (from the producer node's identity, per `sdk-graph-authoring`), `.named(...)` overrides, and explicit entry names SHALL all share this namespace and its duplicate check; a collision SHALL fail registration naming both declarations.

#### Scenario: Same-named channels on different relays are independent
- **WHEN** the BTC Relay declares `c.input::<Trade>("trade.raw")` and the ETH Relay also declares `c.input::<Trade>("trade.raw")`
- **THEN** an input pushed to the BTC Relay's `"trade.raw"` channel SHALL NOT be observable by the ETH Relay's Tasklets, and vice versa

#### Scenario: Derived and explicit names collide in one namespace
- **WHEN** a relay declares an entry channel named `"validate"` and also wires a node whose identity is `validate` without a `.named(...)` override
- **THEN** registration SHALL fail with a duplicate-channel-name error naming both declarations

### Requirement: Hot-path channel dispatch uses interned u32 IDs
At Relay startup, channel names SHALL be interned to `u32` IDs once. Runtime input-routing decisions inside the SDK hot path SHALL use the integer ID, not the string name. Channel-name strings SHALL appear only in: the user-facing API, registration time, and the on-the-wire metadata field crossing the NIF boundary.

#### Scenario: Hot path is allocation-free with respect to channel names
- **WHEN** an input is dispatched from a Relay's input channel through to a Cell's mailbox during steady-state tasklet
- **THEN** no per-message allocation of `String` for the channel name SHALL be observable in profiling

### Requirement: Inter-tasklet transport is in-process zero-copy
When a Channel connects two Tasklets inside the same Relay, the SDK SHALL transport values as `Arc<T>` references between Cells, performing no serialization, no allocation of an output buffer, and no NIF-boundary crossing for that transport.

#### Scenario: Pipeline between two tasklets does not serialize
- **WHEN** OhlcTasklet emits a `Candle` to a channel that IndicatorTasklet subscribes to (and the channel has no `to_plugin(...)` clause)
- **THEN** the IndicatorTasklet's `run(...)` SHALL receive the same `Arc<Candle>` instance OhlcTasklet emitted (or an `Arc` to byte-identical content with no intermediate serialization step)

### Requirement: Plugin emission semantics
Channels declared with `.output("name")` / `.output_with("name", serializer)` — in **either** the Rust SDK or the Java SDK (`.output(...)`) — SHALL route a value, whether or not the channel also has inter-tasklet consumers, through the agent's plugin layer using the channel name as the wire identifier. The numeric `plugin_group: u8` metadata field is removed from the wire format. The replacement metadata field SHALL be `channel: String` — the literal channel name as a string in v1. A per-channel integer fast-path (sending an interned `u32` instead of the string) MAY be added by a future change as an opt-in alternative, but SHALL NOT be in v1.

The wire bytes are produced by the channel's `pluginSink.serializer` / `plugin_sink.serializer` (set by `output(...)` / `output_with(...)`), the one representation of plugin egress for both shapes. The byte buffer SHALL be sent through the same wire framing path (`channel: String` metadata + payload bytes) so the agent's plugin layer SHALL NOT distinguish between pure and hybrid channels at the wire — both arrive identically and route by channel name. **Furthermore, the agent's plugin layer SHALL NOT distinguish between Rust-SDK and Java-SDK frames** — hybrid frames produced by either SDK are byte-indistinguishable for the same plugin name and serializer-output bytes.

#### Scenario: Pure-plugin emit reaches the agent's plugin layer
- **WHEN** a Tasklet publishes to a channel declared with `.output("ohlc_kafka")` and no consumers
- **THEN** the agent's `ohlc_kafka` plugin SHALL receive the bytes with `channel = <channel name>` in the metadata

#### Scenario: Hybrid emit reaches the agent's plugin layer with identical framing
- **WHEN** a Tasklet publishes via `.output_with("indicators", sma_to_pb_bytes)` on a channel that also has inter-tasklet consumers
- **THEN** the agent's `indicators` plugin SHALL receive the serializer-output bytes with `channel = <channel name>` in the metadata
- **AND** the wire frame SHALL be byte-indistinguishable from a pure-plugin channel's frame for the same plugin name and bytes

#### Scenario: Two channels sharing a plugin name fan in at the agent
- **WHEN** a Relay declares two hybrid channels (e.g. an `sma_out` stream with `output_with("indicators", sma_serde)` and an `ema_out` stream with `output_with("indicators", ema_serde)`)
- **THEN** the agent's `indicators` plugin SHALL receive frames from both channels, distinguishable only by the `channel: String` metadata key
- **AND** the SDK's topology builder SHALL NOT reject this declaration, because each channel still has exactly one producer Tasklet

#### Scenario: Plugin subscribes by channel name
- **WHEN** an agent-side plugin is configured to consume outputs from `"ohlc_kafka"`
- **THEN** that plugin SHALL receive every message whose channel name matches `"ohlc_kafka"`, regardless of which Tasklet produced it

#### Scenario: Hybrid emit reaches the agent's plugin layer with identical framing across SDKs
- **WHEN** a Rust Tasklet publishes via `.output_with("indicators", sma_to_pb_bytes)` and a Java Tasklet publishes via `.output("indicators", SmaCodec::toBytes)` with identical serializer output for an identical input value
- **THEN** the agent's `indicators` plugin SHALL receive byte-equal wire frames from both SDKs (the only on-wire difference being TCP/UDS interleaving — the framed payload bytes are identical)
- **AND** the SDK's topology builder on either side SHALL NOT reject either declaration, because each channel still has exactly one producer Tasklet within its own SDK

### Requirement: TaskletCtx emit semantics
A Tasklet's `run` method SHALL receive a mutable reference to its context, `Ctx`. The Tasklet SHALL emit zero, one, or more outputs by calling `ctx.emit(value)` for each output, the value typed as the Tasklet's `Output` (the wiring declaration resolves the matching channel). For outputs that need a string-keyed user metadata bag attached, the Tasklet SHALL call `ctx.emit_with_meta(value, HashMap<String, String>)` instead. The `run` method's return type SHALL be `Result<(), TaskletError>` for success / hard-error signaling. Producing no output SHALL be expressed by simply not calling `ctx.emit` (or `ctx.emit_with_meta`); there SHALL be no explicit "no-output" return variant.

`ctx.emit_with_meta` SHALL be valid only for channels declared with `.to_plugin("name")`. Calling `emit_with_meta` on a channel that flows to another Tasklet inside the same Relay SHALL return a `TaskletError` and SHALL NOT propagate the value or the metadata. The error message SHALL name the offending channel and instruct the user to either move the metadata into the typed value or add `.to_plugin(...)` to the channel.

When `emit_with_meta` is used on a plugin-bound channel, the supplied `HashMap<String, String>` SHALL be attached to the output as it crosses the NIF boundary using the existing wire-format `[key_len][val_len][key][value]` block. The channel name (or its interned `u32` equivalent) SHALL be one of the keys; the user-supplied keys SHALL be appended without further validation. Reserved key names (e.g. `channel`) SHALL NOT be overridable via the user-supplied map; an attempt to set one SHALL be silently dropped during framing.

#### Scenario: Fan-out via multiple emit calls
- **WHEN** a ParseTasklet's `run` calls `ctx.emit::<ParsedTrade>(p)` followed by `ctx.emit::<RawSummary>(s)` and returns `Ok(())`
- **THEN** both downstream channels SHALL receive their respective values, and the order of emission SHALL match the order of the `emit` calls

#### Scenario: No-output via no-call
- **WHEN** a Tasklet's `run` returns `Ok(())` without calling `ctx.emit`
- **THEN** the SDK SHALL treat the input as fully processed with no downstream propagation, with no error and no special enum variant required

#### Scenario: Hard error via Err return
- **WHEN** a Tasklet's `run` returns `Err(e)`
- **THEN** the SDK SHALL log the error against the (relay, tasklet, cell) triple and SHALL NOT propagate any partial output that may have been emitted before the error

#### Scenario: emit_with_meta on a plugin-bound channel
- **WHEN** a Tasklet's `run` calls `ctx.emit_with_meta::<Candle>(c, {"trace_id": "abc123"})` on a channel declared with `.to_plugin("ohlc_kafka")`
- **THEN** the message reaching the agent SHALL carry the channel name AND the `trace_id: "abc123"` key in its metadata block, and the agent's plugin SHALL receive both

#### Scenario: emit_with_meta on an inter-tasklet channel returns error
- **WHEN** a Tasklet's `run` calls `ctx.emit_with_meta::<ParsedTrade>(p, {"trace_id": "abc123"})` on a channel that flows to another Tasklet inside the same Relay (no `.to_plugin(...)` clause)
- **THEN** the call SHALL return `Err(TaskletError::MetadataNotAllowedOnInterTaskletChannel { channel })`, and the value `p` SHALL NOT be propagated to the downstream Tasklet

### Requirement: Hybrid channel builder method
The Rust SDK SHALL expose `output_with<F>(plugin_name: &'static str, serializer: F)` where `F: Fn(&T) -> Vec<u8> + Send + Sync + 'static`, and `output(plugin_name: &'static str)` (default serialization: passthrough for `Vec<u8>`, UTF-8 for `String`, the SDK's default JSON codec otherwise), on the graph builder's stream handle. Calling either SHALL attach a plugin output (the named plugin and the serializer, type-erased to `PluginSerializeFn`) alongside the channel's existing consumer fan-out. After the call, further `.into::<S>()` consumer wiring SHALL chain naturally — the method returns the same stream handle.

The plugin output SHALL be stored on the channel descriptor as a `plugin_sink: Option<PluginSink>` field where `PluginSink { name: &'static str, serializer: PluginSerializeFn }`. Pure-`PluginBound` channels (produced by `output(...)`) SHALL continue to populate the existing `plugin_name` and `plugin_serializer` fields and SHALL NOT populate `plugin_sink`. Hybrid channels SHALL populate `plugin_sink` and SHALL NOT populate `plugin_name`/`plugin_serializer`. The two paths SHALL stay textually distinct so existing pattern matches over `ChannelKind` continue to compile unchanged.

#### Scenario: Hybrid wiring is accepted at registration
- **WHEN** a Relay declares an `SmaTasklet` output stream with `.output_with("indicators", sma_to_pb_bytes).into::<BollingerTasklet>()`
- **THEN** registration SHALL succeed
- **AND** the resulting `ChannelDescriptor` SHALL have `kind = InterTasklet`, exactly one entry in `consumers` (the `BollingerTasklet` tasklet id), and `plugin_sink = Some(PluginSink { name: "indicators", serializer: <fn-ptr> })`

#### Scenario: output_with chains in either order with into::<S>()
- **WHEN** a Relay declares `.into::<MacdTasklet>().output_with("indicators", ema_to_pb_bytes)` on an `EmaTasklet` output stream
- **THEN** registration SHALL succeed
- **AND** the resulting descriptor SHALL be identical to the same declaration with the calls swapped

#### Scenario: output without a serializer uses the default codec
- **WHEN** a Relay declares `.output("indicators")` on a stream of a serde-serializable type
- **THEN** the plugin output SHALL serialize each value with the SDK's default JSON codec, identically to what `output("indicators")` would produce for the same value

#### Scenario: Calling output_with twice on the same channel is a topology error
- **WHEN** a Relay chains `.output_with("p1", f1).output_with("p2", f2)` on one stream
- **THEN** registration SHALL fail with `TopologyIssue::DuplicatePluginOutput { channel }` naming the channel

### Requirement: Hybrid channel emission — consumer fan-out then single serializer call
When a Tasklet's `Tasklet::run` calls `ctx.emit(value)` on a hybrid channel (`kind = InterTasklet, plugin_sink = Some(_)`), the dispatcher SHALL execute the following sequence on the producer thread:

1. Fan out the typed `Arc<dyn Any + Send + Sync>` value to every consumer mailbox listed in the channel's `consumers` per the channel's `OverflowPolicy`. This step SHALL complete (modulo `OverflowPolicy::DropNothing` parking) before step 2 begins.
2. If `plugin_sink` is `Some(sink)`, invoke `sink.serializer(value.as_ref())` exactly once inside `std::panic::catch_unwind(AssertUnwindSafe(...))`. On `Ok(bytes)`, send the bytes through the existing plugin-wire framing path (`channel: String` wire-metadata key set to the channel name; `[key_len][val_len][key][value]` block format). On `Err(panic)`, return `TaskletError::PluginSerializerPanic { channel: <channel name>, message: <formatted panic payload> }` to the producing Tasklet's `ctx.emit` call without emitting any plugin bytes.

The dispatcher thread SHALL remain alive across a serializer panic. Subsequent emits SHALL proceed normally; only the producing Tasklet sees the per-emit error.

The serializer SHALL run exactly once per `ctx.emit` invocation regardless of consumer count. The byte buffer produced by the serializer SHALL NOT be cached or reused across emits.

#### Scenario: Hybrid emit fans out typed value to all consumers and bytes to plugin
- **WHEN** a producer Tasklet's `run` calls `ctx.emit(value)` on a channel wired to `[ConsumerA, ConsumerB]` and with `plugin_sink = Some(("indicators", serializer))`
- **THEN** `ConsumerA::run` and `ConsumerB::run` SHALL each be invoked with the typed `value` (cloned per the existing inter-tasklet delivery semantics)
- **AND** the agent-side plugin "indicators" SHALL receive a single wire frame with bytes equal to `serializer(&value)` and `channel = <channel name>` in the metadata

#### Scenario: Single serializer call regardless of consumer count
- **WHEN** a hybrid channel has N inter-tasklet consumers and the producer emits one value
- **THEN** the serializer SHALL be invoked exactly once for that emit
- **AND** N is unconstrained (1, 3, 100 — same single-call guarantee)

#### Scenario: Panicking serializer surfaces as TaskletError without crashing the dispatcher
- **WHEN** the serializer panics on a particular input
- **THEN** the inter-tasklet consumers SHALL still receive the typed value (consumer fan-out completed before serialization)
- **AND** `ctx.emit(...)` SHALL return `Err(TaskletError::PluginSerializerPanic { channel, message })`
- **AND** the dispatcher thread SHALL remain alive — a subsequent `ctx.emit` call SHALL execute normally

#### Scenario: Serializer runs after consumer fan-out, not before
- **WHEN** an `OverflowPolicy::DropNothing` consumer mailbox is full and the producer emits on a hybrid channel
- **THEN** the producer SHALL park on the consumer mailbox
- **AND** the plugin output SHALL NOT receive any bytes until the consumer drains and the producer unparks
- **AND** when the producer unparks, the serializer SHALL be invoked exactly once and the plugin output SHALL receive the corresponding bytes

### Requirement: emit_with_meta is valid on hybrid channels
`ctx.emit_with_meta(value, meta)` SHALL accept the call when either:

- the channel is `kind = PluginBound` (existing rule), OR
- the channel has `plugin_sink = Some(_)` (new rule for hybrid channels).

On a hybrid channel, the inter-tasklet consumers SHALL receive the typed value with no metadata (metadata is plugin-framing only and has no place in the typed delivery model). The plugin output SHALL receive bytes (via the channel's serializer) with the supplied `HashMap<String, String>` attached to the wire frame using the existing `[key_len][val_len][key][value]` block format. The reserved `channel: String` key SHALL be set automatically and SHALL NOT be overridable by user-supplied keys; an attempt to set a reserved key SHALL be silently dropped during framing (matches the existing rule for `PluginBound` channels).

On a pure-`InterTasklet` channel with no plugin output, `emit_with_meta` SHALL continue to return `TaskletError::MetadataNotAllowedOnInterTaskletChannel { channel }` without delivering the value or the metadata.

#### Scenario: emit_with_meta on a hybrid channel forwards metadata to plugin only
- **WHEN** a producer Tasklet's `run` calls `ctx.emit_with_meta(value, {"trace_id": "abc123"})` on a hybrid channel wired to one consumer and one plugin output
- **THEN** the consumer SHALL receive the typed `value` with no `trace_id` visible
- **AND** the plugin wire frame SHALL contain `trace_id = "abc123"` plus the implicit `channel = <channel name>` key

#### Scenario: emit_with_meta on a pure inter-tasklet channel still rejects metadata
- **WHEN** a producer Tasklet's `run` calls `ctx.emit_with_meta(value, {"trace_id": "abc123"})` on a channel that has consumers but no `plugin_sink`
- **THEN** `ctx.emit_with_meta` SHALL return `TaskletError::MetadataNotAllowedOnInterTaskletChannel { channel }`
- **AND** neither the consumers nor any plugin SHALL receive the value or the metadata

#### Scenario: Reserved metadata keys are stripped on hybrid channels
- **WHEN** a producer calls `ctx.emit_with_meta(value, {"channel": "spoof", "trace_id": "abc"})` on a hybrid channel
- **THEN** the wire frame SHALL contain `channel = <real channel name>` (the reserved value, not "spoof") and `trace_id = "abc"`

### Requirement: New TopologyIssue variant — `DuplicatePluginOutput`
`TopologyIssue` SHALL carry a `DuplicatePluginOutput { channel: String }` variant, and the
type SHALL live in `flz_core::host::issue` with the Rust SDK re-exporting it from
`sdk/rust/src/channel/builder/issue.rs`. The variant SHALL be raised by the reference check
when a channel's record carries more than one `output` link, for every SDK; the Rust SDK's
`register_stream::<R>()` SHALL surface it in the standard "topology rejected" message that
lists every accumulated `TopologyIssue`, and SHALL NOT register the topology.

#### Scenario: Second output_with call is rejected at registration
- **WHEN** a Relay's `channels(c)` method calls `output_with("p1", f1)` followed by `output_with("p2", f2)` on the same channel
- **THEN** `register_stream::<R>()` SHALL panic with the topology error including `DuplicatePluginOutput { channel: "<name>" }`
- **AND** the registered topology SHALL be discarded

#### Scenario: The same shape is refused on an erased host
- **WHEN** a Node, Python, Ruby, PHP, Java or Go builder records two `output` links on one channel
- **THEN** the core's check refuses `topology.duplicate_plugin_output` naming the channel and the class is not registered

### Requirement: New TaskletError variant — `PluginSerializerPanic`
`TaskletError` (in `sdk/rust/src/tasklet/error.rs`) SHALL gain a `PluginSerializerPanic { channel: String, message: String }` variant. The variant SHALL be returned from `ctx.emit(...)` (and `ctx.emit_with_meta(...)`) on a hybrid channel when the user-supplied serializer panics. `channel` SHALL be the channel name as declared in `ChannelBuilder`; `message` SHALL be the panic payload formatted via the existing panic-message helper (a string description, not the raw `Box<dyn Any>` payload).

The producing Tasklet's `Tasklet::run` SHALL receive the error via the normal `Result<(), TaskletError>` return path of `ctx.emit`. Returning `Err` from `Tasklet::run` continues to discard partial emits per the existing trait contract; ignoring the error is also valid and SHALL leave the inter-tasklet consumers' delivery intact.

#### Scenario: Producer Tasklet sees PluginSerializerPanic and chooses how to handle it
- **WHEN** a hybrid channel's serializer panics during `ctx.emit(value)`
- **THEN** `ctx.emit(value)` SHALL return `Err(TaskletError::PluginSerializerPanic { channel, message })`
- **AND** the producing Tasklet MAY ignore, log, or `return Err(...)` — all three are valid handlers
- **AND** the inter-tasklet consumer fan-out SHALL have already completed before the panic was caught

### Requirement: Hybrid channel builder method (Java SDK)
The Java SDK SHALL expose `output(String pluginName, Function<T, byte[]> serializer)` and `output(String pluginName)` (default serialization: passthrough for `byte[]`, UTF-8 for `String`, Jackson JSON otherwise) on the graph builder's stream handle. Calling either SHALL attach a plugin output (the named plugin and the serializer, type-erased to `Function<Object, byte[]>`) alongside the channel's existing consumer fan-out. After the call, further `.into(...)` consumer wiring SHALL chain naturally — the method returns the same stream handle.

The plugin output SHALL be stored on the channel descriptor as `Optional<PluginSink> pluginSink` where `PluginSink` is a public record `(String name, Function<Object, byte[]> serializer)`. Pure-`PLUGIN_BOUND` channels (produced by `output(...)`) SHALL continue to populate the existing `pluginName` and `pluginSerializer` fields and SHALL NOT populate `pluginSink`. Hybrid channels SHALL populate `pluginSink` and SHALL NOT populate `pluginName`/`pluginSerializer`. The two paths SHALL stay textually distinct so existing pattern matches over `ChannelKind` continue to compile unchanged.

#### Scenario: Hybrid wiring is accepted at registration (Java)
- **WHEN** a Relay declares an `SmaTasklet` output stream with `.output("indicators", SmaCodec::toBytes).into(BollingerTasklet.class)`
- **THEN** registration SHALL succeed
- **AND** the resulting `ChannelDescriptor` SHALL have `kind() == INTER_TASKLET`, exactly one entry in `consumers()` (the `BollingerTasklet` tasklet id), and `pluginSink() == Optional.of(new PluginSink("indicators", <fn>))`

#### Scenario: output chains in either order with into(...) (Java)
- **WHEN** a Relay declares `.into(MacdTasklet.class).output("indicators", EmaCodec::toBytes)` on an `EmaTasklet` output stream
- **THEN** registration SHALL succeed
- **AND** the resulting descriptor SHALL be identical to the same declaration with the calls swapped

#### Scenario: output without a serializer uses the default codec (Java)
- **WHEN** a Relay declares `.output("indicators")` on a stream of a Jackson-serializable type
- **THEN** the plugin output SHALL serialize each value with Jackson JSON, identically to what `output("indicators")` would produce for the same value

### Requirement: Hybrid channel emission — consumer fan-out then single serializer call (Java SDK)
When a Tasklet's `Tasklet.run` calls `ctx.emit(value)` on a hybrid channel (`kind == INTER_TASKLET`, `pluginSink.isPresent()`), the dispatcher SHALL execute the following sequence on the producer thread:

1. Fan out the typed value to every consumer mailbox listed in `descriptor.consumers()`. This step SHALL complete before step 2 begins.
2. If `pluginSink.isPresent()`, invoke `pluginSink.get().serializer().apply(value)` exactly once inside `try { ... } catch (RuntimeException | Error e) { ... }`. On normal return, send the bytes through the existing plugin-wire framing path (`channel: String` wire-metadata key set to `descriptor.name()`; `[key_len][val_len][key][value]` block format; via `FrameEncoder.build(...)` and `NativeTransport.streamWrite(...)`). On caught throw, throw `TaskletException.PluginSerializerFailure(channel, message, cause)` to the producing Tasklet's `ctx.emit` call without emitting any plugin bytes. `Throwable` is intentionally NOT caught — `OutOfMemoryError` and similar JVM-level signals propagate.

The dispatcher thread SHALL remain alive across a serializer-thrown exception. Subsequent emits SHALL proceed normally; only the producing Tasklet sees the per-emit error.

The serializer SHALL run exactly once per `ctx.emit` invocation regardless of consumer count. The byte buffer produced by the serializer SHALL NOT be cached or reused across emits.

#### Scenario: Hybrid emit fans out typed value to all consumers and bytes to plugin (Java)
- **WHEN** a producer Tasklet's `run` calls `ctx.emit(value)` on a channel wired to `[ConsumerA.class, ConsumerB.class]` with `pluginSink = Optional.of(new PluginSink("indicators", serializer))`
- **THEN** `ConsumerA.run` and `ConsumerB.run` SHALL each be invoked with `value` (passed by reference per existing inter-tasklet delivery semantics)
- **AND** the agent-side plugin "indicators" SHALL receive a single wire frame whose body bytes equal `serializer.apply(value)` and whose metadata contains `channel = <channel name>`

#### Scenario: Single serializer call regardless of consumer count (Java)
- **WHEN** a hybrid channel has N inter-tasklet consumers and the producer emits one value
- **THEN** the serializer SHALL be invoked exactly once for that emit
- **AND** N is unconstrained (1, 3, 100 — same single-call guarantee)

#### Scenario: Throwing serializer surfaces as TaskletException without crashing the dispatcher (Java)
- **WHEN** the serializer throws `RuntimeException` on a particular input
- **THEN** the inter-tasklet consumers SHALL still receive the typed value (consumer fan-out completed before serialization)
- **AND** `ctx.emit(...)` SHALL throw `TaskletException.PluginSerializerFailure { channel(), getMessage(), getCause() }`
- **AND** the dispatcher thread SHALL remain alive — a subsequent `ctx.emit` call SHALL execute normally

### Requirement: emitWithMeta is valid on hybrid channels (Java SDK)
`TaskletCtx.emitWithMeta(value, meta)` SHALL accept the call when either:

- the channel is `kind == PLUGIN_BOUND` (existing rule), OR
- the channel has `pluginSink.isPresent()` (new rule for hybrid channels).

On a hybrid channel, the inter-tasklet consumers SHALL receive the typed value with no metadata (metadata is plugin-framing only). The plugin output SHALL receive bytes (via the channel's serializer) with the supplied `Map<String, String>` attached to the wire frame using the existing `[key_len][val_len][key][value]` block format. The reserved `channel` key SHALL be set automatically and SHALL NOT be overridable by user-supplied keys; an attempt to set it SHALL be silently dropped during framing (matches the existing rule for `PLUGIN_BOUND` channels).

On a pure-`INTER_TASKLET` channel with no plugin output, `emitWithMeta` SHALL continue to throw `TaskletException.MetadataNotAllowedOnInterTaskletChannel` without delivering the value or the metadata.

#### Scenario: emitWithMeta on a hybrid channel forwards metadata to plugin only (Java)
- **WHEN** a producer Tasklet's `run` calls `ctx.emitWithMeta(value, Map.of("trace_id", "abc123"))` on a hybrid channel wired to one consumer and one plugin output
- **THEN** the consumer SHALL receive `value` with no `trace_id` visible
- **AND** the plugin wire frame SHALL contain `trace_id = "abc123"` plus the implicit `channel = <channel name>` key

#### Scenario: emitWithMeta on a pure inter-tasklet channel still throws (Java)
- **WHEN** a producer Tasklet's `run` calls `ctx.emitWithMeta(value, Map.of("trace_id", "abc123"))` on a channel that has consumers but no `pluginSink`
- **THEN** `ctx.emitWithMeta` SHALL throw `TaskletException.MetadataNotAllowedOnInterTaskletChannel` carrying the channel name
- **AND** neither the consumers nor any plugin SHALL receive the value or the metadata

#### Scenario: Reserved metadata keys are stripped on hybrid channels (Java)
- **WHEN** a producer calls `ctx.emitWithMeta(value, Map.of("channel", "spoof", "trace_id", "abc"))` on a hybrid channel
- **THEN** the wire frame SHALL contain `channel = <real channel name>` (the reserved value, not "spoof") and `trace_id = "abc"`

### Requirement: New TaskletException subclass — `PluginSerializerFailure` (Java SDK)
`TaskletException` (in `sdk/java/flezha-sdk-api/src/main/java/com/flezha/sdk/tasklet/TaskletException.java`) SHALL gain a nested `final class PluginSerializerFailure extends TaskletException`. Constructor: `(String channel, String message, Throwable cause)`. The class SHALL be thrown from `ctx.emit(...)` (and `ctx.emitWithMeta(...)`) on a hybrid channel when the user-supplied serializer throws `RuntimeException` or `Error`. `channel()` accessor SHALL return the channel name as declared in `ChannelBuilder`; `getMessage()` SHALL include the underlying exception's `toString()` and the channel name; `getCause()` SHALL be the original throwable.

The producing Tasklet's `Tasklet.run` SHALL receive the exception via the normal `throws TaskletException` declaration on `ctx.emit`. Catching the exception and continuing is valid; rethrowing from `Tasklet.run` continues to discard partial emits per the existing trait contract.

#### Scenario: Producer Tasklet sees PluginSerializerFailure and chooses how to handle it (Java)
- **WHEN** a hybrid channel's serializer throws during `ctx.emit(value)`
- **THEN** `ctx.emit(value)` SHALL throw `TaskletException.PluginSerializerFailure` carrying the channel name and the underlying cause
- **AND** the producing Tasklet MAY catch (logging or swallowing) or rethrow — both are valid handlers
- **AND** the inter-tasklet consumer fan-out SHALL have already completed before the exception was thrown

#### Scenario: Throwable causes other than RuntimeException/Error are not caught (Java)
- **WHEN** the serializer throws `OutOfMemoryError`
- **THEN** the dispatcher SHALL NOT catch it — the JVM's normal escalation path applies (the worker thread terminates, the runner's uncaught-exception handler runs)
- **AND** the consumer fan-out for that emit SHALL have already completed

### Requirement: Worker pool reuses existing dispatch infrastructure

The Runner pool SHALL be the SDK's poll workers over the shared activation
engine (`flz_core::poll_core`): one base set sized by `FLEZHA_WORKER_POOL_SIZE`
(default: available parallelism), grown under starvation up to
`FLEZHA_WORKER_POOL_MAX`. There SHALL be no second, core-owned pool — steps relay
stage work is handed out as step activations the same workers serve — and the
per-Cell `inflight` CAS guard SHALL be the same mechanism the per-`CellSlot`
guard uses.

#### Scenario: Default pool sizing
- **WHEN** the SDK is started without `FLEZHA_WORKER_POOL_SIZE` set on a host with 8 cores
- **THEN** the runner pool SHALL start 8 OS threads

#### Scenario: Env override honored
- **WHEN** `FLEZHA_WORKER_POOL_SIZE=4` is set on a host with 8 cores
- **THEN** the runner pool SHALL start exactly 4 OS threads

#### Scenario: One worker set serves both relay profiles
- **WHEN** a steps relay runs a stage while flow cells are being dispatched
- **THEN** both SHALL be served by the same workers, and no core-owned thread SHALL run user code

### Requirement: Dynamic-mode lazy cell creation
For dynamic-mode Tasklets, the runtime SHALL create a `CellSlot` for a key on the first input whose consumer edge's key function returns a key for which no slot already exists. The key function is declared on the wiring via `key_by` / `keyBy` and is total — it returns a `String`, never an absence — so there is no keyless-drop path; discarding an input is expressed with `filter` before the keying. Slot creation SHALL be synchronous within the dispatch path: allocate the slot, allocate the mailbox at the Tasklet's declared `mailbox_capacity()`, push the triggering input, submit a runner job. The per-Tasklet `cells_created_total` counter SHALL be incremented on each create.

Concurrent producers pushing inputs for the same brand-new key SHALL all observe the same `CellSlot` instance after creation. The runtime SHALL NOT allocate two distinct slots for the same key under any concurrency pattern.

#### Scenario: First input with a fresh key creates the cell
- **WHEN** a Tasklet reached by a keyed edge receives an input whose key function returns `"user-42"` and no Cell for `"user-42"` exists
- **THEN** the runtime SHALL create a Cell keyed `"user-42"`, push the input onto its mailbox, increment `cells_created_total`, and submit a runner job

#### Scenario: Subsequent inputs with the same key reuse the cell
- **WHEN** a second input arrives whose key function returns `"user-42"` after the first one created the slot
- **THEN** the runtime SHALL push onto the existing Cell's mailbox; `cells_created_total` SHALL NOT increment

#### Scenario: Filtering, not a null key, discards an input
- **WHEN** an author wants inputs of a given shape not to reach any Cell
- **THEN** they SHALL declare `filter` before `key_by` on the edge, and the runtime SHALL offer no keyless-drop path

#### Scenario: Concurrent producers for the same fresh key share one slot
- **WHEN** N producer threads simultaneously push the first inputs for the same brand-new key `"user-99"`
- **THEN** the runtime SHALL allocate exactly one `CellSlot` for `"user-99"` and all N inputs SHALL land on its single mailbox in some serial order

### Requirement: Per-Tasklet idle-TTL eviction is opt-in (no eviction by default)
`Tasklet::idle_ttl()` SHALL return `Option<Duration>` and SHALL default to `None`. When it returns `None`, the Tasklet's dynamic Cells SHALL NOT be evicted: they live for the lifetime of the Relay (as static Cells do) and no idle sweep SHALL be armed for the Tasklet.

When `idle_ttl()` returns `Some(ttl)`, the SDK SHALL arm one idle sweep for the Tasklet as an entry on the poll core's deferred-wake list, re-armed after each sweep and run by the poller that promotes it, that periodically evicts Cells whose time since last input push exceeds `ttl`; no thread SHALL exist per Tasklet to run it. A sweep with a host-run Cell to evict waits for the host to answer the Cell's leave, so that sweep SHALL run on a short-lived thread of its own rather than on the poller. The sweep SHALL acquire the Cell's `inflight` CAS before evicting and SHALL confirm its mailbox is empty; if the CAS is contended, the sweep SHALL skip the Cell and try again on the next sweep. Eviction SHALL increment the per-Tasklet `cells_evicted_total` counter.

The sweep SHALL come due on a cadence derived as `min(ttl / 4, 30s)`, overridable globally via the `FLEZHA_REAP_INTERVAL_MS` environment variable for testing.

#### Scenario: No eviction by default
- **WHEN** a dynamic-mode Tasklet does not override `idle_ttl()` (it returns `None`)
- **THEN** no idle sweep SHALL be armed and its Cells SHALL never be evicted by idle TTL

#### Scenario: Idle cell is evicted after TTL elapses
- **WHEN** a Tasklet sets `idle_ttl() = Some(ttl)` and a Cell has had no input push for longer than `ttl` and is not currently running
- **THEN** the next sweep SHALL acquire its inflight CAS, evict it from the registry, and increment `cells_evicted_total`

#### Scenario: Active cell is not evicted mid-run
- **WHEN** a Cell's `Tasklet::run` is in flight (its inflight CAS is held) when the sweep runs
- **THEN** the sweep SHALL skip this Cell without evicting; eviction SHALL be reattempted on the next sweep

#### Scenario: Recently active cell is preserved
- **WHEN** a Cell received an input push within the past `ttl` interval
- **THEN** the sweep SHALL NOT evict it

### Requirement: Eviction is destructive; recreate is a cold start
Eviction of a dynamic-mode Cell SHALL be destructive. When a Cell is evicted AND persistence is enabled for the Relay class, the SDK SHALL ship a `delete_cell_state{cell_id}` frame to the controller so its on-disk WAL/snapshot/outbox/lineage files are removed. The frame SHALL carry the durable guarantee class, so an eviction that falls inside a reconnect window holds the delete for the next bound stream rather than losing it, and a rebind replays it in write order behind that Cell's own records. The SDK SHALL NOT block the reaper on a response, and the controller writes none.

When a dynamic-mode Cell has been evicted and a subsequent input arrives for the same key, the runtime SHALL recreate the `CellSlot` synchronously (per "Dynamic-mode lazy cell creation") and SHALL call `Tasklet::init_state(Some(key), None)` — a fresh cold start. The runtime SHALL NOT restore the evicted Cell's state from disk on recreate. (This is a deliberate decision: it keeps the recreate path off any blocking controller round-trip, so a recreate can never stall a work-stealing pool worker.)

#### Scenario: Recreate after eviction is a cold start
- **WHEN** a dynamic-mode Tasklet with persistence enabled is reaped and a subsequent input arrives for the same key
- **THEN** the runtime SHALL recreate the slot and call `Tasklet::init_state(Some(key), None)`; the Tasklet's `run` SHALL observe freshly-initialized state, not the state from before eviction

#### Scenario: Eviction deletes persisted state
- **WHEN** a persistence-enabled dynamic-mode Cell is evicted
- **THEN** the SDK SHALL ship `delete_cell_state{cell_id}` to the controller, which SHALL remove the Cell's `{key}.wal`, `{key}.snap.{seq}`, `{key}.outbox.dedup` and `{key}.lineage` files

#### Scenario: An eviction with no stream bound holds its delete
- **WHEN** a persistence-enabled dynamic-mode Cell is evicted while no data stream is bound
- **THEN** the SDK SHALL hold the `delete_cell_state{cell_id}` frame in the class's ring and SHALL deliver it on the next bound stream, framed exactly as the live path would have framed it

#### Scenario: Recreate without persistence is a cold start
- **WHEN** a dynamic-mode Tasklet with persistence disabled is reaped and a subsequent input arrives for the same key
- **THEN** the runtime SHALL call `Tasklet::init_state(Some(key), None)` and the Tasklet's `run` SHALL observe the freshly-initialized state

### Requirement: Soft cap on dynamic cells per Tasklet
Each dynamic-mode Tasklet SHALL declare a maximum cell count via `fn max_cells() -> usize` (default `100_000`). When the per-Tasklet cell count reaches this cap, the runtime SHALL drop new-key inputs without allocating a Cell and SHALL increment `inputs_dropped_capped_total`. Inputs whose edge key function resolves to a key with an existing Cell SHALL NOT be affected by the cap; they SHALL be processed normally.

The cap is soft: enforcement is best-effort against concurrent inserts. The runtime SHALL NOT propagate an over-cap rejection back to the agent; the input is silently dropped and the counter is incremented.

#### Scenario: Cap reached drops new-key inputs
- **WHEN** a dynamic-mode Tasklet with `max_cells = 5` already owns 5 Cells and an input arrives whose edge key function returns a 6th unseen key
- **THEN** the runtime SHALL drop the input, SHALL NOT allocate a Cell, and SHALL increment `inputs_dropped_capped_total`

#### Scenario: Cap does not affect inputs for existing keys
- **WHEN** a dynamic-mode Tasklet is at its cap and an input arrives whose edge key function returns a key that already has a Cell
- **THEN** the runtime SHALL push to that Cell's mailbox normally; `inputs_dropped_capped_total` SHALL NOT increment

#### Scenario: Cap recovers as TTLs elapse
- **WHEN** a dynamic-mode Tasklet is at its cap, time passes such that some Cells exceed `idle_ttl()`, and the idle sweep evicts them
- **THEN** subsequent new-key inputs SHALL allocate Cells normally up to the cap

### Requirement: Per-Tasklet dynamic-mode counters
Each dynamic-mode Tasklet SHALL expose the following monotonically-increasing counters on its registry, backed by `AtomicU64`:

- `cells_created_total` — incremented once per `CellSlot` allocation (including a cold recreate after eviction).
- `cells_evicted_total` — incremented once per eviction.
- `inputs_dropped_capped_total` — incremented when an input is dropped due to the soft cap.

Counters SHALL be observable via the same per-Cell / per-Tasklet observability surface as the bounded-mailbox counters from `relay-tasklet-cell-model`'s "Per-Cell mailbox is bounded with declared capacity" requirement. Export to a metrics output is not in scope for this requirement; this requirement only mandates the data is captured.

#### Scenario: Counter shape after a churn cycle
- **GIVEN** a dynamic-mode Tasklet with a TTL set
- **WHEN** 100 fresh keys are pushed, 30 of those 100 are subsequently reaped, then 10 of the reaped keys see a fresh input each
- **THEN** `cells_created_total = 110` (100 initial + 10 cold recreates), `cells_evicted_total = 30`

### Requirement: The workload model names exactly three levels

The system SHALL name the workload hierarchy **Relay → Tasklet → Cell**, and every artifact — source, comment, document, specification, guide, wire name, and on-disk name — SHALL use those names for those levels.

A **Relay** is the deployable unit: it has an id and a namespace, is placed on a Runner, and is started, stopped, migrated, suspended, and crash-resumed. It carries a kind (`flow`, `steps`) and MUST remain able to accept further kinds as a qualifier.

A **Tasklet** is a typed stage inside a Relay. It declares `Input`, `Output`, `State`, `Change`, and a clock, implements `run` / `compensate` over changes that apply themselves, and is wired to other Tasklets by Channels. A Tasklet stands for the whole life of its Relay; it is not invoked once and completed.

A **Cell** is one keyed instance of a Tasklet. It owns its mailbox, its state, its WAL, its snapshots, and its outbox dedup, and it is the unit of idle eviction and compaction. A Cell exists whether or not its Tasklet declares state, and whether or not it carries a key.

#### Scenario: Containment is stated consistently
- **WHEN** any document, module doc, or specification describes the hierarchy
- **THEN** it MUST state that a Relay contains Tasklets and a Tasklet holds one Cell per key, and MUST NOT name any level with a term this specification retires

#### Scenario: A stateless Tasklet still holds Cells
- **WHEN** a Tasklet declares no state and no Change
- **THEN** it MUST still resolve to at least one Cell, and no name, diagnostic, or document MAY imply that a Cell requires state to exist

#### Scenario: Level 1 accepts a new kind without renaming
- **WHEN** a workload kind beyond `flow` and `steps` is introduced
- **THEN** it MUST be expressible as a qualifier on `Relay` (for example "scheduled Relay") with no change to the level's name

### Requirement: A Tasklet resolves to exactly one cell mode
Each Tasklet SHALL resolve, once at registration, to exactly one of three cell modes. Single and Static modes SHALL be derived from the Tasklet's own declarations; Dynamic mode SHALL be derived from the wiring — a Tasklet reached by a consumer edge carrying a key function is Dynamic.

- **Single** — exactly one Cell, carrying no key.
- **Static** — a fixed keyset declared by the Tasklet; one input MAY be routed to several Cells.
- **Dynamic** — a stream keyset; Cells are created on first sight of a key and MAY be idle-evicted.

Wiring a key function onto a Tasklet that declares a static keyset SHALL be a build error.

#### Scenario: Single mode carries no key
- **WHEN** a Tasklet declares no static keyset and is reached only by edges carrying no key function
- **THEN** it resolves to Single mode and holds exactly one Cell, and every diagnostic, path, and document referring to that Cell MUST remain correct without naming a key

#### Scenario: Static mode may fan one input out
- **WHEN** a Static-mode Tasklet's routing declaration returns more than one key for a single input
- **THEN** that input MUST be delivered to the Cell for each returned key

#### Scenario: Dynamic mode creates and evicts
- **WHEN** a Tasklet reached by a keyed edge receives an input whose key has no live Cell
- **THEN** a Cell for that key MUST be created before the input is enqueued, and MUST be eligible for idle eviction if the Tasklet declares an idle TTL

#### Scenario: Keying a static-mode Tasklet is refused
- **WHEN** an edge carrying a key function wires a Tasklet declaring a non-empty static keyset
- **THEN** the build SHALL fail naming the Tasklet, rather than one declaration silently winning

### Requirement: Declarations are named for the level they describe

A declaration SHALL be named for the level whose values it produces or consumes, not for the level that hosts it.

Cell selection and lifetime declarations SHALL therefore be named for the Cell: `cell_keys()` (the static keyset), `cells_for()` (static routing, plural because one input may fan out), `key_by` (the dynamic key, declared on the wiring), `idle_ttl()` (the Cell eviction window, spelled as the `idle_ttl_seconds` setting every SDK carries), and `ctx.cell_key()` (the running Cell's own key). A control-plane message addressed to a Relay SHALL be named for the Relay.

#### Scenario: Cell-key declarations name the Cell
- **WHEN** an author declares a Tasklet's static keyset
- **THEN** the declaration MUST be named `cell_keys` in every SDK, and MUST NOT be named after the Tasklet

#### Scenario: Static routing is plural
- **WHEN** an author declares Static-mode routing
- **THEN** the declaration MUST be named `cells_for`, reflecting that its result MAY name several Cells

#### Scenario: The running key names the Cell
- **WHEN** a Tasklet body reads the key of the Cell it is running in
- **THEN** the accessor MUST be named `cell_key`, in every SDK

#### Scenario: A relay-addressed message names the Relay
- **WHEN** a control-plane message carries only a relay identifier and no tasklet or cell identifier
- **THEN** it MUST be named for the Relay

### Requirement: Durable and wire artifacts are named for the level that owns them

Wire message names, data-stream payload ids, output metadata keys, and on-disk filenames SHALL use the same vocabulary as the source and the documentation, with no separate protocol-level or storage-level spelling.

Per-Cell durable state SHALL live at `state/{relay_class}/{tasklet_id}/{cell_key}.*`. Per-Tasklet journal and inbox artifacts SHALL be named for the Tasklet.

#### Scenario: Payload ids match the documented vocabulary
- **WHEN** a runner ships a state change, a snapshot, a delete, or a compaction request for a Cell
- **THEN** the payload id MUST name the Cell, and MUST match the term used for that concept in `docs/`

#### Scenario: Output metadata names the Cell
- **WHEN** a released output carries the identity of the Cell that produced it
- **THEN** the metadata key MUST be `flz.cell`

#### Scenario: On-disk names match the documented vocabulary
- **WHEN** a steps Relay writes its journal or its durable inbox
- **THEN** the filename MUST name the Tasklet level

### Requirement: The architecture documentation states the model as a diagram
`docs/public/architecture/overview.md` SHALL carry a diagram of the containment model — Controller and Runner above, then Relay, then Tasklet, then Cell, with Channels as the edges between Tasklets — and SHALL show the three cell modes and what distinguishes them.

#### Scenario: The diagram shows containment
- **WHEN** a reader opens the architecture documentation
- **THEN** the diagram MUST show which level contains which, without requiring the surrounding prose to disambiguate it

#### Scenario: The diagram shows the cell modes
- **WHEN** the diagram describes a Tasklet
- **THEN** it MUST show that a Tasklet resolves to Single, Static, or Dynamic, and state for each how many Cells result and whether they are keyed

### Requirement: Entry-edge mailbox-full handling and the shared wire reader

The SDK's wire-reader thread (the per-stream loop in `command/streams_connect.rs`) services every Relay sharing one physical connection. Under the default entry-channel policy (`Block`), a push into a full consumer mailbox SHALL park the wire-reader thread until the consumer drains an item; the frame SHALL NOT be discarded. While parked, the reader SHALL NOT run the saturated Relay's tasklets itself, and sibling Relays on the same connection SHALL NOT receive new frames until the parked push completes. The SDK SHALL document this co-tenancy cost in `docs/internal/sdk/rust_relay_architecture.md` and `docs/internal/sdk/protocol_spec.md`.

Under a declared drop policy (`.on_full(OverflowPolicy::DropNewest)` or `DropOldest`) the wire-reader thread SHALL NOT block on a full mailbox: the frame's typed payload SHALL be discarded (or the oldest item evicted) per the policy, and the loop SHALL immediately attempt to read the next frame from the wire.

#### Scenario: Default entry Block parks the reader and loses nothing
- **WHEN** Relay A declares `c.input::<Vec<u8>>("x").into::<S>()` without `.on_full(...)`, its entry mailbox saturates, and Relay B shares the physical wire connection
- **THEN** the wire-reader thread SHALL park on the push and B SHALL NOT receive new frames until the parked push completes; once the pool drains A's mailbox every frame addressed to A and B SHALL land, and A's `drops_newest_total` and `drops_oldest_total` SHALL be unchanged

#### Scenario: A declared drop policy keeps the wire reader running
- **WHEN** Relay A's entry channel declares `.on_full(OverflowPolicy::DropNewest)`, its mailbox is full, and Relay B (sharing the physical wire connection) has frames pending
- **THEN** the wire reader SHALL discard the frame addressed to A and continue dispatching the next frame; B's frames SHALL NOT be delayed by A's saturation

### Requirement: DropNothing parking does not violate sticky per-cell ordering
When a producer thread is parked on a full consumer mailbox under `OverflowPolicy::DropNothing`, the runtime SHALL preserve the existing "Sticky per-cell ordering" requirement on the *consumer* Cell. Specifically:

- The consumer Cell's drain loop SHALL continue to run on whichever worker thread holds its inflight CAS; no parked producer SHALL block consumer drain.
- Producers parked on the same consumer mailbox SHALL be unparked one at a time as items pop, in the order they parked (FIFO unpark fairness).

This requirement is in addition to and does not modify the "Sticky per-cell ordering" requirement from the umbrella `relay-architecture` capability spec; it constrains how `DropNothing` must integrate with that invariant.

#### Scenario: Parked producer does not block its consumer's drain
- **WHEN** Producer A's thread is parked because Consumer X's mailbox is full, and the worker pool has one other thread that picks up X's drain
- **THEN** X's drain SHALL run to completion (popping items, calling `Tasklet::run`); each pop SHALL unpark one waiting producer (FIFO order); A SHALL eventually be unparked and complete its push

#### Scenario: FIFO unpark order
- **WHEN** Producers A, B, C park in that order on the same full consumer mailbox
- **THEN** the consumer's first three pops SHALL unpark A, B, C respectively (best-effort under MPMC contention; the *intent* is FIFO order via a `VecDeque<Unparker>` implementation)

### Requirement: A dropping policy is refused where its loss cannot be accounted for

A channel SHALL NOT declare `DropNewest` or `DropOldest` when the loss it permits would break a durability contract the same class declares. Declaration SHALL fail, naming the channel and the conflicting declaration, in the same validation that already refuses `effectively_once` without `persistent`:

- A class declaring `effectively_once` SHALL NOT carry a dropping policy on any channel. Under the tier a dropped input's offset is skipped by a later item's watermark advance, so the input is permanently lost while the tier reports effectively-once. This holds for both dropping policies equally.
- A channel whose consumer declares a correction window SHALL NOT carry a dropping policy, because a correction's answer must not depend on arrival timing.

Independently of the declared policy, a **replay-slice member SHALL always take the `DropNothing` path**. Dropping a slice member leaves the slice's open marker set and its depth short, so the slice closes on the end-of-slice broadcast and folds whatever arrived — the correction then computes a wrong answer with no error surfaced anywhere.

A dropping policy remains fully available to a class that declares neither the tier nor a correction window. Shedding is a legitimate intent; promising effectively-once *and* shedding is a contradiction, and it is refused where the author can still see it.

#### Scenario: The tier refuses a shedding channel at declare time

- **WHEN** a class declares `effectively_once` and any channel declares `.on_full(DropNewest)`
- **THEN** registration SHALL fail with an error naming the channel and both declarations, and no relay process SHALL be created

#### Scenario: A replay slice is never shed

- **WHEN** an entry channel declares `.on_full(DropNewest)`, a correction replay delivers a slice, and the consumer's mailbox is full mid-slice
- **THEN** the member SHALL be delivered once room appears; `drops_newest_total` SHALL NOT change and the slice SHALL close with every member present

### Requirement: Overflow outcomes are observable outside the process

Every overflow outcome SHALL reach a user without attaching a debugger. `pushes_blocked_total`, `drops_newest_total` and `drops_oldest_total` SHALL be aggregated process-wide and carried in the data-channel heartbeat's metadata — as `blocked_parks`, `drops_newest` and `drops_oldest` — beside `dispatches` and `credit_parked`, and the controller SHALL surface them on the same path it surfaces those.

The block counter SHALL advance on **every wait iteration**, not once per contiguous full period; `blocked_parks` names that one counter and there is no second, per-episode one. A counter that advances once per episode leaves a runner in one long episode showing a frozen dispatch count and a frozen block count — byte-for-byte the picture a wedged runner shows — which is the distinction the sender-side credit window already gets right by advancing per iteration.

A `DropOldest` eviction that admits its item SHALL increment `pushes_total`, because it is a successful enqueue by that counter's own definition, and SHALL report the evicted item's identity, which the mailbox's force-push already returns to the caller.

#### Scenario: A parked runner is distinguishable from a wedged one

- **WHEN** a runner's dispatch count stops advancing while producers are held on full mailboxes
- **THEN** the heartbeat's block counter SHALL keep advancing, and the controller's stall classifier SHALL read that as backpressure rather than as a wedge

#### Scenario: A shed input is countable from outside

- **WHEN** a relay declares a dropping policy and sheds inputs under saturation
- **THEN** the drop count SHALL appear in the heartbeat metadata and SHALL be readable through the controller's counter surface

#### Scenario: DropOldest's admitted count is correct

- **WHEN** a mailbox at capacity accepts N items under `DropOldest`, evicting N heads
- **THEN** `pushes_total` SHALL have increased by N and `drops_oldest_total` SHALL have increased by N

### Requirement: A second plugin output is refused by the reference check, in every SDK
A channel's record SHALL carry one `output` link per `output(...)` call, and
`flz_core::host::register::check` SHALL answer `topology.duplicate_plugin_output` naming the
channel when a channel's chains carry more than one. No SDK SHALL carry a rule
or a sentence of its own for the shape:
each SDK SHALL raise its one error type (Java `FlezhaException`), built from the core's
error document: its code SHALL be the first issue's `topology.*` code and its
`details.issues` SHALL list every accumulated issue by code, which is what the veneer
suites assert, never a substring of the message. Registration SHALL be refused and the topology discarded.

#### Scenario: Second output call is rejected at registration (Java)
- **WHEN** a Relay's `channels(c)` method calls `c.channel(T.class, "foo").from(S.class).output("p1", f1).output("p2", f2)`
- **THEN** `FlezhaRunner.registerFactory(R.class)` SHALL throw `FlezhaException` whose code is `topology.duplicate_plugin_output` and whose details name the channel `"foo"`
- **AND** the registered topology SHALL be discarded

### Requirement: A Rust change applies itself through the context
The Rust SDK's `Change<S>` trait SHALL bound on `Serialize + DeserializeOwned + Send + Sync + 'static`, SHALL declare `fn apply_to(&self, state: &mut S)` and SHALL keep `fn coalesce_key(&self) -> Option<CoalesceKey>` defaulting to `None`. The `Tasklet` trait SHALL declare `type Change: Change<Self::State>` and SHALL NOT declare `apply_change`. `NoChange` SHALL implement `Change<S>` for every `S`. `ctx.change(change)` SHALL call `change.apply_to` on the cell's state and push the change onto the cell's flush buffer; the restore fold and `TaskletTestHarness::replay` SHALL call `apply_to` in seq order. A `Change` enum's variants SHALL keep the append-only rule, since postcard encodes the variant index.

#### Scenario: A change is applied and recorded

- **WHEN** a tasklet's `run` calls `ctx.change(Traded { quiet_at: 42, .. })`
- **THEN** `Traded::apply_to` SHALL run on the cell's state, and the change SHALL be in the cell's flush buffer

#### Scenario: A restart folds the log through apply_to

- **WHEN** a cell whose log holds three changes is restored
- **THEN** each change's `apply_to` SHALL run in seq order, and the result SHALL equal the state before the restart

#### Scenario: A stateless tasklet keeps no buffer

- **WHEN** a tasklet leaves `State` and `Change` to the attribute's defaults
- **THEN** its `Change` SHALL be `NoChange`, and no flush buffer SHALL be registered for its cells

### Requirement: A Rust tasklet reads and changes its state through `Ctx`
The Rust SDK's `Tasklet::run` SHALL be `fn run(&mut self, input: Self::Input, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError>`, and `on_timer` and `compensate` SHALL take the same `&mut Ctx<'_, Self>` in place of a state handle and a tasklet context. `Ctx<'_, S>` SHALL expose `state(&self) -> &S::State` and `change(&mut self, S::Change)`, SHALL NOT implement `DerefMut` or hand out `&mut S::State`, and SHALL type `emit`, `emit_with_meta`, `emit_keyed` and `emit_keyed_with_meta` as `S::Output`. It SHALL carry every other method the tasklet context carries today. `StateHandle` and `TaskletCtx` SHALL NOT be public.

#### Scenario: Direct mutation does not compile

- **WHEN** a tasklet's `run` body attempts `ctx.state().field = value`
- **THEN** the Rust compiler SHALL refuse it, and the program MUST NOT build

#### Scenario: An emit of the wrong type does not compile

- **WHEN** a tasklet with `Output = String` calls `ctx.emit(42u64)`
- **THEN** the Rust compiler SHALL refuse it

#### Scenario: Reading the state is allocation-free

- **WHEN** a tasklet's `run` reads `ctx.state().some_field`
- **THEN** the read SHALL record no change and allocate nothing

### Requirement: A Rust tasklet's clock follows from what it declares
The Rust SDK's `Tasklet` trait SHALL declare `const CLOCK: ClockKind`, with `ClockKind` one of `Wall`, `EventTime` and `Logical`, and `fn event_time(input: &Self::Input) -> u64`. The dispatcher SHALL evaluate the clock once per invocation before user code runs:

- **Wall**: epoch milliseconds at dispatch entry.
- **EventTime**: `event_time` applied to the input being dispatched.
- **Logical**: the durable change seq, re-armed to `terminal + 1` on restore.

`#[tasklet]` SHALL set `CLOCK` to `EventTime` when the block writes `event_time`, to `Logical` with the `logical_clock` key, and to `Wall` otherwise, and SHALL fail to compile when both are declared. A `Logical` clock with `Change = NoChange` SHALL fail registration with `topology.unservable_clock`.

#### Scenario: The default clock is the wall clock

- **WHEN** a `#[tasklet]` block writes no `event_time` and no `logical_clock` key
- **THEN** `ctx.now()` SHALL return epoch milliseconds read once at dispatch entry

#### Scenario: Writing event_time makes the clock event time

- **WHEN** a `#[tasklet]` block writes `fn event_time(trade: &Trade) -> u64 { trade.at }` and a trade with `at = 1720000000000` is dispatched
- **THEN** `ctx.now()` inside that invocation SHALL return `1720000000000`, and event-time timers SHALL fire against it

#### Scenario: Two clocks do not compile

- **WHEN** a `#[tasklet(logical_clock)]` block also writes `event_time`
- **THEN** the macro SHALL fail to compile naming both

### Requirement: A Rust tasklet's state starts from `init_state(cell_key)`
The Rust SDK's `Tasklet` trait SHALL declare `fn init_state(cell_key: Option<&str>) -> Self::State` with no restore payload parameter. `#[tasklet]` SHALL write it as `Default::default()` when the block writes none. The runtime SHALL call it for every new cell and SHALL apply any restore payload after it returns.

#### Scenario: A new cell's state is the default

- **WHEN** a `#[tasklet]` block with `State = LastTrade` writes no `init_state` and a cell is created
- **THEN** that cell's `ctx.state()` SHALL equal `LastTrade::default()`

#### Scenario: A written init_state receives the cell key

- **WHEN** a block writes `init_state` and a cell keyed `"1h"` is created
- **THEN** `init_state` SHALL be called with `Some("1h")`

### Requirement: Every SDK SHALL declare compact_after_bytes on the tasklet

`compact_after_bytes` SHALL be a tasklet setting in every SDK (`compact_after_bytes` in Python,
Ruby, Elixir and Rust, `compactAfterBytes` in PHP, Node and Java, `Spec.CompactAfterBytes` in Go),
carried in that tasklet's class declaration; no relay builder SHALL offer it.

#### Scenario: A hosted tasklet declares its own threshold

- **WHEN** a Python tasklet declares `compact_after_bytes=16 * 1024 * 1024` and its relay's other tasklet declares nothing
- **THEN** the controller SHALL resolve 16 MiB for the first tasklet's Cells and the default for the other's

