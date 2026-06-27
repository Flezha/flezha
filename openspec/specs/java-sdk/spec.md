# java-sdk Specification

## Purpose

Defines the Java SDK for Flezha compute engines: the public API, protocol layer, command dispatch, transport selection, reconnection, the Relay / Shard / Partition runtime, and the JNI bridge to `flz_transport`. Conforms to `docs/sdk_protocol_spec.md`.

## Requirements

### Requirement: Java SDK Entry Point

The Java SDK SHALL provide `com.flezha.sdk.Flezha.newInstance(String namespace)` as the single entry point for starting a Flezha compute engine. The call MUST connect to the agent's control socket, send the handshake init message per `docs/sdk_protocol_spec.md` §5.2, wait for `AssignExecutorId`, and return a handle exposing `getId()` and `getRelays()` (where `getRelays()` returns a snapshot of currently-spawned Relay instances keyed by what the wire calls `relay_id` — see the `Relay Registration` requirement below for naming).

#### Scenario: Successful connection returns an assigned id
- **WHEN** an SDK user calls `Flezha.newInstance("my_app")` with the agent running and at least one Relay class registered
- **THEN** the call blocks until the agent responds with `AssignExecutorId`, and the returned handle's `getId()` returns the assigned id (non-null, non-empty)

#### Scenario: Agent unavailable triggers exponential backoff retries
- **WHEN** an SDK user calls `Flezha.newInstance("my_app")` and the agent's control socket is not yet listening
- **THEN** the SDK SHALL retry the connect with exponential backoff starting at 200 ms and capped at 30 s, and succeed once the agent becomes available

#### Scenario: newInstance without any registered relay class fails fast
- **WHEN** `Flezha.newInstance(namespace)` is called before any `RelayRegistry.registerRelay(...)` (or `Flezha.registerRelay(...)`) call
- **THEN** the SDK SHALL throw an `IllegalStateException` describing the missing relay registration

### Requirement: Relay Registration

The Java SDK SHALL provide `RelayRegistry.registerRelay(Class<? extends Relay>)` (and a thin static `Flezha.registerRelay(Class<? extends Relay>)` pass-through) that MUST be called **before** `Flezha.newInstance`. For each registered Relay class the SDK SHALL: (a) instantiate the class via its public no-arg constructor; (b) call its `factoryId()` method to obtain the agent-facing factory id; (c) call its `channels(ChannelBuilder)` method exactly once and freeze the resulting `RelayTopology`; (d) verify wiring constraints (entry-channel-type, type-equality between channel payload and producer/consumer Shard I/O, no unwired channels, no multi-producer, static-mode and dynamic-mode default-impl detection); (e) record `(factoryId → topology)` in the class registry. The registry MUST be frozen at `Flezha.newInstance(...)` time. There SHALL be no `RelayHandler`-style factory registration in the public surface.

#### Scenario: Registering a relay class before newInstance succeeds
- **WHEN** user code calls `Flezha.registerRelay(MarketRelay.class)` and then `Flezha.newInstance("my_app")`
- **THEN** the handshake init JSON SHALL include `"market"` (the value `MarketRelay::factoryId` returns) in `factory_ids`, and subsequent `AddRelay{factory_id:"market"}` commands SHALL succeed by spawning a `RelayInstance`

#### Scenario: Registering a relay class after newInstance is rejected
- **WHEN** user code calls `RelayRegistry.registerRelay(...)` after `Flezha.newInstance(...)` has returned
- **THEN** the SDK SHALL throw an `IllegalStateException`

#### Scenario: Duplicate factoryId across two relay classes is rejected
- **WHEN** two distinct Relay classes both return `"market"` from `factoryId()` and both are registered
- **THEN** the second `registerRelay` call SHALL throw an `IllegalArgumentException` naming the duplicate factoryId

#### Scenario: Wiring mismatch fails registration with a diagnosable error
- **WHEN** a Relay declares `c.channel(Trade.class, "trade.parsed").from(ParseShard.class).into(OhlcShard.class)` but `OhlcShard.inputType()` returns `Candle.class`
- **THEN** `registerRelay` SHALL throw `RelayConfigurationException` whose message names the channel `"trade.parsed"`, the expected payload type (`Trade`), and the actual consumer-input type (`Candle`)

#### Scenario: Entry channel of disallowed type fails registration
- **WHEN** a Relay declares `c.input(Trade.class, "trade.raw").into(ParseShard.class)`
- **THEN** `registerRelay` SHALL throw `RelayConfigurationException` stating that entry channels accept only `byte[]` or `String` in v1

#### Scenario: Unwired channel fails registration
- **WHEN** a Relay declares `c.channel(Candle.class, "ohlc.candles")` and never calls `.from(...)` on the resulting `ChannelDecl`
- **THEN** `registerRelay` SHALL throw `RelayConfigurationException` naming the unwired channel

#### Scenario: Static-mode shard without partitionForOverridden is rejected
- **WHEN** a Shard returns a non-empty list from `shardKeys()` but its `partitionForOverridden()` returns the default `false`
- **THEN** `registerRelay` SHALL throw `RelayConfigurationException` naming the Shard and stating that static-mode shards must override `partitionFor` and `partitionForOverridden`

#### Scenario: Dynamic-mode-only declaration is rejected in v1
- **WHEN** a Shard returns `true` from `shardKeyOverridden()` but returns an empty list from `shardKeys()`
- **THEN** `registerRelay` SHALL throw `RelayConfigurationException` stating that dynamic-mode requires `shardKeys()` until `relay-dynamic-partitioning` ships

### Requirement: Shard Interface and Per-Partition Dispatch

The Java SDK SHALL expose a generic `Shard<Input, State, Output>` interface in `com.flezha.sdk.shard`. Implementers SHALL provide `name()`, `inputType() → Class<Input>`, `outputType() → Class<Output>`, `initState(Optional<String> shardKey) → State`, and `run(Input, State, ShardCtx) throws ShardException`. The default-method overrides `shardKeys()`, `partitionFor(Input)`, `partitionForOverridden()`, `shardKey(Input)`, and `shardKeyOverridden()` declare the partitioning mode (single by default; static when `shardKeys()` non-empty + `partitionForOverridden()` true; dynamic mode reserved for the future change). The runtime SHALL invoke `run` on a per-partition basis with sticky ordering (see the Sticky Per-Partition Ordering requirement) and SHALL pass each input to all partitions returned by `partitionFor(input)` for static-mode shards (single-mode shards skip the call and route to partition index 0).

The `ShardCtx` parameter SHALL provide `emit(value)` for inter-shard or plugin-bound outputs, `emit(channel, value)` for explicit channel selection, and `emitWithMeta(value, meta)` / `emitWithMeta(channel, value, meta)` for plugin-bound outputs with user-attached metadata. `emitWithMeta` on inter-shard channels SHALL throw `ShardException.MetadataNotAllowedOnInterShardChannel` naming the channel.

#### Scenario: Single-mode Shard runs on a single partition
- **WHEN** a Shard whose `shardKeys()` returns an empty list is registered as the consumer of an entry channel, and one input arrives on that channel
- **THEN** the SDK SHALL invoke the Shard's `run` once on the single allocated partition (partition index 0), with `state` argument equal to the value `initState(Optional.empty())` returned at relay startup

#### Scenario: Static-mode Shard creates one partition per shardKey
- **WHEN** a Shard's `shardKeys()` returns `["1m", "1h", "1d", "1w"]` and `partitionForOverridden()` returns `true`
- **THEN** at relay startup the SDK SHALL allocate exactly four `PartitionSlot` instances for that Shard, each constructed with `initState(Optional.of(key))` for its corresponding key

#### Scenario: Static-mode partitionFor fan-out enqueues input on each named partition
- **WHEN** a Shard's `partitionFor(input)` returns `["1m", "1h"]` for a given input
- **THEN** the SDK SHALL enqueue that input on the partitions identified by `"1m"` and `"1h"` (each receives its own `run` invocation), and SHALL NOT enqueue it on `"1d"` or `"1w"`

#### Scenario: Run throwing ShardException logs and continues
- **WHEN** `Shard.run(...)` throws `ShardException` on a given input
- **THEN** the SDK SHALL log the exception with `(relayInstanceId, shardName, partitionKey)` context, SHALL discard any partial `ctx.emit(...)` calls made during this invocation, and SHALL continue dispatching subsequent inputs on the same partition

#### Scenario: Run throwing arbitrary RuntimeException logs and continues
- **WHEN** `Shard.run(...)` throws a non-`ShardException` `RuntimeException`
- **THEN** the SDK SHALL log the exception with the same `(relayInstanceId, shardName, partitionKey)` context, discard partial emits, and continue dispatching subsequent inputs on the same partition

#### Scenario: Inter-shard zero-copy preserves value identity
- **WHEN** Shard A emits a non-null value `v` on an inter-shard channel that Shard B consumes (single-partition fan-out, single JVM)
- **THEN** the value passed to Shard B's `run` SHALL be `==`-equal (reference-equal) to `v` — the runtime SHALL NOT copy or serialize the value between Shards in the same JVM

#### Scenario: emitWithMeta on an inter-shard channel returns error
- **WHEN** a Shard whose declared output is an inter-shard channel calls `ctx.emitWithMeta(value, Map.of("k", "v"))`
- **THEN** the SDK SHALL throw `ShardException.MetadataNotAllowedOnInterShardChannel` whose message names the channel

#### Scenario: Plugin-bound emit attaches the channel name as metadata on the wire
- **WHEN** a Shard whose declared output is a plugin-bound channel `"ohlc.candles"` calls `ctx.emit(candle)`
- **THEN** the SDK SHALL serialize `candle` via the channel's serializer, frame the bytes with `metadata = {"channel": "ohlc.candles"}`, and send the frame on the relay's secondary stream

#### Scenario: Plugin-bound emitWithMeta appends user metadata after the channel key
- **WHEN** a Shard calls `ctx.emitWithMeta(candle, Map.of("trace_id", "abc"))` on a plugin-bound channel `"ohlc.candles"`
- **THEN** the SDK SHALL frame the bytes with `metadata` containing both `"channel" -> "ohlc.candles"` and `"trace_id" -> "abc"`

#### Scenario: Reserved metadata key in user map is silently dropped
- **WHEN** a Shard calls `ctx.emitWithMeta(candle, Map.of("channel", "user_value", "k", "v"))` on a plugin-bound channel `"ohlc.candles"`
- **THEN** the SDK SHALL frame the bytes with `metadata` containing exactly `"channel" -> "ohlc.candles"` and `"k" -> "v"`, dropping the user-supplied `"channel"` entry without raising

### Requirement: Sticky Per-Partition Ordering Under ThreadPool Dispatch

The Java SDK's worker pool (sized by `FLEZHA_WORKER_POOL_SIZE`, default = `Runtime.availableRelays()`) SHALL guarantee that for any single `(relayInstance, shard, partition)` triple, two invocations of that Shard's `run(...)` method on that Partition never execute concurrently, and invocations observe their inputs in the exact order those inputs were enqueued onto the Partition's mailbox. This invariant MUST hold regardless of worker-pool size, number of concurrent Relay instances, number of Partitions per Shard, or transport (UDS/QUIC).

#### Scenario: Same partition never runs concurrently under load
- **WHEN** 10,000 inputs targeting the same `(relayInstance, shard, partitionKey)` triple arrive back-to-back on one or more entry channels while the SDK uses a 16-thread worker pool
- **THEN** at any wall-clock instant, at most one thread SHALL be inside that Partition's `run(...)` method

#### Scenario: Different partitions of same shard may run concurrently
- **WHEN** inputs arrive simultaneously for two distinct partitions of the same Shard (for example `("ohlc", "1m")` and `("ohlc", "1h")`) and the worker pool has ≥ 2 threads
- **THEN** the two Partitions' `run(...)` methods MAY execute concurrently on different threads

#### Scenario: Per-partition inputs delivered in arrival order
- **WHEN** inputs `"a"`, `"b"`, `"c"` are enqueued onto one Partition's mailbox in that order with an append-to-state Shard
- **THEN** the Partition's State SHALL end with `["a", "b", "c"]` in exactly that order

### Requirement: Command Dispatch

The Java SDK SHALL implement handlers for every `CommandRequest` variant defined in `executor_commands.proto`: `AssignExecutorId`, `ConnectSocket`, `AddRelay`, `RemRelay`, `PartitionState`, `MigrationSerialization`, `MigrationDeserialization`, `RelayMessage`, `ExecutorDisconnect`. Each handler MUST produce a `CommandResponse` that echoes the original `reference_id` and sets `status = "success"` or `status = "error"` with populated `errors` per `docs/sdk_protocol_spec.md` §6. `AddRelay` and `RemRelay` SHALL spawn and tear down unit instances respectively (the wire field is named `relay_id` for backward continuity; the SDK maps it to `relayInstanceId`).

`AddRelay` SHALL read the optional `kind` field: an empty or absent value SHALL be interpreted as `:relay` and SHALL spawn a `RelayInstance` exactly as before this change; a value resolving to `:task` SHALL spawn a `TaskInstance`. The per-executor instance registry SHALL hold a sealed `Unit` (provisional name) permitting the relay-instance and task-instance variants, keyed by `relayInstanceId`; the variant SHALL be resolved once per inbound frame at the dispatch boundary, leaving the relay dispatch hot path unchanged save that single per-frame branch. `RemRelay` SHALL remove either variant by instance id. `RelayMessage` SHALL respond with the canonical "unsupported" error until `relay-message-handlers` ships per-Shard `handleMessage`.

#### Scenario: AddRelay spawns a relay instance
- **WHEN** the agent sends `AddRelay{relay_id:"p1", factory_id:"market"}` and Relay class `MarketRelay` is registered with `factoryId() == "market"`
- **THEN** the SDK SHALL allocate a `RelayInstance` keyed by `"p1"`, call `initState(...)` for every Shard×Partition declared by `MarketRelay`, and respond with `status = "success"`

#### Scenario: AddRelay with init_values seeds initial-state for each partition
- **WHEN** the agent sends `AddRelay{relay_id:"p1", factory_id:"market", init_values:"{\"symbol\":\"BTC\"}"}` and `MarketRelay` declares one Shard with single-partition mode
- **THEN** the SDK SHALL pass the JSON string into the Shard's `initState(Optional.empty())` (the runtime makes the JSON available via a per-relay context), invoke `run` only after `initState` returns, and respond with `status = "success"`

#### Scenario: AddRelay with unknown factory_id returns error
- **WHEN** the agent sends `AddRelay{factory_id:"missing"}`
- **THEN** the SDK SHALL respond with `status = "error"` and `errors` containing a string naming the missing factory id

#### Scenario: AddRelay with empty kind spawns a relay (backward-compatible)
- **WHEN** the agent sends `AddRelay{relay_id:"p1", factory_id:"market"}` with the `kind` field empty or absent
- **THEN** the SDK SHALL resolve the kind to `:relay` and spawn a `RelayInstance` exactly as before this change

#### Scenario: AddRelay with kind task spawns a task variant
- **WHEN** the agent sends `AddRelay{relay_id:"t1", factory_id:"greet", kind:"task"}` and `GreetTask` is registered as a task with `factoryId() == "greet"`
- **THEN** the SDK SHALL register a `TaskInstance` keyed by `"t1"` as the task variant of the sealed unit registry and respond with `status = "success"`

#### Scenario: RemRelay removes either unit variant
- **WHEN** the agent sends `RemRelay{relay_id:"t1"}` for a live task instance (and likewise for a relay instance)
- **THEN** the SDK SHALL remove the instance from the sealed unit registry by id and respond with `status = "success"`

#### Scenario: PartitionState returns the per-Shard / per-Partition JSON shape
- **WHEN** the agent sends `PartitionState{relay_id:"p1"}` and `p1` exists with two Shards (`shardA` single-mode, `shardB` static-mode with keys `["1m", "1h"]`)
- **THEN** the SDK SHALL respond with `status = "success"` and `raw_data` containing JSON of shape `{"shardA": {<state>}, "shardB": {"1m": {<state>}, "1h": {<state>}}}` (whichever exact shape the Rust SDK emits per `relay-mvp-rust/design.md` Decision 10 — the Java SDK SHALL match byte-for-byte)

#### Scenario: MigrationSerialization removes the relay instance
- **WHEN** the agent sends `MigrationSerialization{relay_id:"p1"}` and `p1` exists
- **THEN** the SDK SHALL respond with `status = "success"` and `raw_data` containing the per-Shard / per-Partition JSON (same shape as `PartitionState`), AND `p1` SHALL be removed from the relay instance registry

#### Scenario: RelayMessage returns canonical unsupported error pre-relay-message-handlers
- **WHEN** the agent sends `RelayMessage{relay_id:"p1", payload:<...>}` to a v1 SDK
- **THEN** the SDK SHALL respond with `RelayMessageResponse{ payload: "", error: "relay does not support handle_message" }`

#### Scenario: ExecutorDisconnect halts the reconnect loop
- **WHEN** the agent sends `ExecutorDisconnect{}`
- **THEN** the SDK SHALL set status to `Stopped`, respond with `status = "success"`, close all sockets, and NOT attempt to reconnect

### Requirement: Binary Framing and Metadata Codec

The Java SDK SHALL implement a frame codec that produces and parses the binary frame format defined in `docs/sdk_protocol_spec.md` §3 (32-byte padded payload id, big-endian `uint32` payload size and message size, big-endian `uint16` metadata size, flat `(key_len, val_len, key, value)` metadata pairs). The parser MUST be a resumable state machine handling `empty`, `incomplete_header`, and `incomplete_payload` states so that frames split across reads are correctly reassembled.

#### Scenario: Building a handshake init frame matches the spec byte layout
- **WHEN** the SDK builds a frame with `payload_id = "connect_control_channel"`, message = `{"factory_ids":["echo"],"namespace":"my_app"}`, metadata = `{}`
- **THEN** the produced bytes SHALL match the hex dump in `docs/sdk_protocol_spec.md` §9.1 (total 86 bytes)

#### Scenario: Frame split across two reads is reassembled
- **WHEN** a 60-byte frame is delivered as two chunks (40 bytes then 20 bytes) to the parser
- **THEN** the parser SHALL emit exactly one decoded frame after the second chunk, with the original payload id, message, and metadata intact

#### Scenario: Metadata round-trip preserves keys and values
- **WHEN** metadata `{input_type -> 2, trace_id -> abc123}` is serialized and then parsed
- **THEN** the parsed metadata SHALL contain both entries with exact byte equality

### Requirement: Protobuf Codec

The Java SDK SHALL use `protobuf-java` to encode `CommandResponse` and decode `CommandRequest` messages generated from the existing `executor_commands.proto` (no new proto definitions, no hand-written wire-format code).

#### Scenario: Encoding AssignExecutorId response matches protocol
- **WHEN** the SDK encodes `CommandResponse{reference_id:"#Ref<0.1.2.3>", status:"success"}`
- **THEN** the produced bytes SHALL be valid protobuf decodable by the Elixir agent's `CommandResponse` parser

#### Scenario: Decoding AddRelay request extracts all fields
- **WHEN** the SDK receives a protobuf-encoded `CommandRequest` with an `AddRelay` oneof case
- **THEN** the decoder SHALL expose `relay_id`, `factory_id`, and `init_values` as Java strings

### Requirement: Transport Selection

The Java SDK SHALL honor the `FLEZHA_TRANSPORT` environment variable (`uds` or `quic`) for transport selection, defaulting to `uds` when unset. For QUIC, the SDK SHALL also honor `FLEZHA_QUIC_ADDRESS`, `FLEZHA_CA_CERT_PATH`, `FLEZHA_SERVER_CERT_PATH`, `FLEZHA_SERVER_KEY_PATH`. For UDS, the SDK SHALL honor `FLEZHA_SOCKET_FILES_PATH`. All env var names and defaults MUST match `docs/sdk_protocol_spec.md` §2.

#### Scenario: Default transport is UDS
- **WHEN** `FLEZHA_TRANSPORT` is unset and `Flezha.newInstance(...)` is called
- **THEN** the SDK SHALL connect to `{FLEZHA_SOCKET_FILES_PATH or app_data/sockets}/executor_control_channel.sock`

#### Scenario: QUIC transport uses configured address and cert paths
- **WHEN** `FLEZHA_TRANSPORT=quic` and `FLEZHA_QUIC_ADDRESS=127.0.0.1:4433`
- **THEN** the SDK SHALL open a QUIC connection to that address using the CA cert at the configured path, opening the control channel on the first bidi stream per §2 "QUIC Stream Mapping"

### Requirement: Auto-Reconnect on Unexpected Disconnect

The Java SDK SHALL distinguish intentional disconnects (`ExecutorDisconnect` command handled → status `Stopped`) from unexpected ones (BrokenPipe / UnexpectedEof / ConnectionReset / QUIC connection lost). On an unexpected disconnect the SDK MUST clear `io_streams`, the live relay registry, and work queues; reset `executor_id` to `undefined`; and reconnect with exponential backoff (200 ms, 400 ms, 800 ms, …, capped at 30 s) per `docs/sdk_protocol_spec.md` §5 "Auto-Reconnection".

#### Scenario: Agent crash triggers reconnect
- **WHEN** the agent process is killed while the SDK has status `Running`
- **THEN** the SDK SHALL detect the disconnect via the native layer, clear local state, and reconnect once the agent is available again, receiving a fresh `executor_id`

#### Scenario: Intentional disconnect does not reconnect
- **WHEN** the SDK receives `ExecutorDisconnect{}` and responds successfully
- **THEN** the SDK SHALL close its sockets and NOT attempt any reconnection

### Requirement: Worker Pool and Dispatch Throughput Floor

The Java SDK SHALL operate on a single shared bounded executor sized by `FLEZHA_WORKER_POOL_SIZE` (default = `Runtime.availableRelays()`), draining all `PartitionSlot` mailboxes work-steal-style. The SDK SHALL sustain at least the throughput floor (in messages/second) measured against the v1 implementation at merge time on the regression bench (`100 relays × 1 shard × 1 partition × 10K msg`, no-op Shard, UDS transport, default worker-pool size; see `relay-mvp-java/design.md` Decision 7) invoked via `mix bench --runtime java --shape regression -n 100 -m 10000`. The bench-regression-java CI job is informational in v1; the literal floor value is recorded in this requirement once `mix bench --runtime java` has been measured 3× on the reference host (median, rounded down to a clean number) — until then the recorded floor is "TBD pending v1 measurement".

#### Scenario: Reference bench meets the Java throughput floor
- **WHEN** a developer runs `MIX_ENV=prod mix bench --runtime java --shape regression -n 100 -m 10000` on a modern 8-core x86_64 laptop with a warm JVM (≥ 30 s warmup)
- **THEN** the reported `Throughput:` line SHALL be ≥ the recorded Java floor value

#### Scenario: Default worker pool size matches available relays
- **WHEN** the SDK starts with `FLEZHA_WORKER_POOL_SIZE` unset
- **THEN** the worker pool SHALL be sized to `Runtime.availableRelays()`

### Requirement: JNI Bridge to flz_transport

The Java SDK SHALL interact with the transport layer exclusively through a JNI bridge that wraps `flz_transport`'s C FFI. The bridge MUST expose Java-side operations for: opening a transport context with a `FlezhaConfig`, connecting (client mode), opening streams, reading and writing stream data, registering a log callback, and destroying the context. All native buffers returned by the bridge MUST be copied into Java `byte[]` / `String` before the native memory is freed.

#### Scenario: Native buffer freed after copy into Java byte[]
- **WHEN** the JNI bridge receives a data callback carrying a native buffer pointer and length
- **THEN** the bridge SHALL allocate a Java `byte[]` of equal length, copy the native bytes into it, invoke the Java-side handler with the `byte[]`, and ensure the native buffer is freed (or returned to `flz_transport` ownership) before control returns to the native caller

#### Scenario: flezha_version mismatch is logged
- **WHEN** the SDK starts and `flezha_version()` returns a string that does not equal the SDK's bundled expected version
- **THEN** the SDK SHALL log a WARNING naming both versions and continue startup

### Requirement: Shard interface declares Change, Clock, Config generic parameters

The Java SDK's `Shard` interface SHALL be parameterized as `Shard<Input, State, Output, Change extends com.flezha.sdk.state.Change, Clock extends com.flezha.sdk.state.Clock, Config>`. The interface SHALL declare reflection methods `Class<Change> changeType()`, `Class<Clock> clockType()`, `Class<Config> configType()` returning the runtime `Class` for each generic parameter (Java type erasure precludes recovering them otherwise).

The interface SHALL declare lifecycle methods:
- `default void applyChange(State state, Change change)` whose default impl is a no-op for `NoChange` and throws `UnsupportedOperationException` for any other Change type (forcing user override).
- `default Optional<CoalesceKey> coalesceKey(Change change)` whose default impl delegates to `change.coalesceKey()`.
- `default long compactAfterBytes()` returning `64L * 1024 * 1024`.
- `default int schemaVersion()` returning `1`.
- `default List<Class<?>> kryoTypes()` returning `List.of()` — the user-declared list of types to register with Kryo for serialization, in stable-ID order.
- `default Shard<...> fromConfig(Config cfg)` whose default invokes the no-arg constructor when `Config = Void`, and throws `UnsupportedOperationException` for non-Void Config types (forcing user override).

#### Scenario: Default Change is NoChange skips persistence machinery

- **WHEN** a Java Shard does not declare its `Change` parameter (defaults to `NoChange`)
- **THEN** the runtime SHALL detect via `changeType() == NoChange.class` (reference equality) at relay-build time and SHALL NOT register a flush buffer or `PartitionFlusher` entry for this Partition

#### Scenario: User-defined Change opts the Shard into persistence

- **WHEN** a Java Shard declares `Change = MyChange` (any non-`NoChange` user type implementing `com.flezha.sdk.state.Change`)
- **THEN** the runtime SHALL register a typed flush buffer (`List<MyChange>`) on each Partition of that Shard and SHALL include those Partitions in the per-JVM flusher's drain rotation

#### Scenario: Default applyChange throws on non-NoChange Change without override

- **WHEN** a Java Shard declares `Change = MyChange` but does NOT override `applyChange`
- **THEN** the relay registration step SHALL succeed (the default impl exists) but the first `state.apply(MyChange.foo())` call SHALL throw `UnsupportedOperationException` whose message names the missing override

#### Scenario: Default fromConfig invokes no-arg constructor for Void Config

- **WHEN** a Java Shard declares `Config = Void` and does NOT override `fromConfig`
- **THEN** the runtime's `fromConfig(null)` invocation SHALL call the Shard's public no-arg constructor via reflection; absence of a no-arg constructor SHALL throw `IllegalStateException` whose message names the Shard

#### Scenario: User-defined Config ships through Jackson deserialization

- **WHEN** a Java Shard declares `Config = MyConfig` (a Jackson-deserializable POJO/record) and overrides `fromConfig(MyConfig cfg)`
- **THEN** the runtime SHALL deserialize the agent-shipped JSON config bytes via Jackson into `MyConfig` and SHALL invoke `shard.fromConfig(cfg)` to construct the Shard at Partition init; deserialization failure SHALL throw `RelayConfigurationException("Config deserialize failed: " + shardName)`

### Requirement: Shard.run receives StateHandle, not direct State reference

The Java SDK's `Shard.run` signature SHALL be `void run(Input input, StateHandle<State, Change> state, ShardCtx ctx) throws ShardException` (was `void run(Input input, State state, ShardCtx ctx)`). `StateHandle<State, Change>` SHALL provide:

- `State get()` — read access to the underlying state.
- `void apply(Change change)` — runs `Shard::applyChange(state, change)` and pushes the Change onto the per-Partition flush buffer.
- NO setter, no direct-mutation method. Direct `state.field = value` is impossible without going through `apply`.

#### Scenario: Direct field mutation requires apply

- **WHEN** a Java Shard's `run` body wants to mutate `state.someField` from a value `v1` to `v2`
- **THEN** the user MUST declare a Change (e.g. `record SetSomeField(Object newValue) implements Change {}`), implement `applyChange` to write the field, and call `state.apply(new SetSomeField(v2))`; direct `state.get().someField = v2` is technically possible (mutable field on the underlying State) but bypasses persistence and breaks `replayEquals` — flagged as user error in documentation but not structurally prevented in Java (unlike Rust's `Deref` chokepoint)

#### Scenario: Read access via get() is allocation-free

- **WHEN** a Java Shard's `run` body reads `state.get().someField`
- **THEN** the read SHALL succeed via the handle's `get()` method (one method invocation, JIT-inlinable), no Change recording, no buffer mutation

#### Scenario: Mutation via apply runs applyChange and records

- **WHEN** a Java Shard's `run` body calls `state.apply(MyChange.foo())`
- **THEN** `Shard.applyChange(state, &MyChange.foo())` SHALL run on the underlying state AND `MyChange.foo()` SHALL be appended to the per-Partition flush buffer (observable via `ShardTestHarness.recordedChanges()` and via the per-JVM flusher's next tick)

### Requirement: Shard declares Clock generic parameter with default WallClock

The Java SDK's `Shard` interface SHALL declare `Clock extends com.flezha.sdk.state.Clock` as a generic parameter, defaulting to `com.flezha.sdk.state.WallClock`. `Clock` SHALL be a sealed Java interface with `permits WallClock, LogicalClock, EventTimeFrom`. v1 of the Java SDK SHALL fully wire only `WallClock`; Shards declaring `LogicalClock` or `EventTimeFrom` SHALL fail Relay registration with `RelayConfigurationException::ClockNotImplementedYet(String clockTypeName)` until a follow-up change wires the remaining clocks.

#### Scenario: Default Clock is WallClock

- **WHEN** a Java Shard does not declare its `Clock` parameter
- **THEN** `Shard.clockType() == WallClock.class` AND Relay registration SHALL succeed

#### Scenario: Non-WallClock fails registration in v1

- **WHEN** a Java Shard declares `Clock = LogicalClock`
- **THEN** `Flezha.registerRelay(...)` SHALL throw `RelayConfigurationException` whose message contains "Clock not implemented yet: LogicalClock"

### Requirement: Relay declares persistenceEnabled default-method, frozen at instance start

The Java SDK's `Relay` interface SHALL declare `default boolean persistenceEnabled() { return true; }`. The runtime SHALL resolve a per-Relay-instance `persistenceEnabled: boolean` at the moment `AddRelay` is processed using:

1. If the `AddRelay` metadata carries `relay_persistence.<RelayClass>` with a boolean value (executor override), use that.
2. Otherwise, use the value of `Relay.persistenceEnabled()` (the Relay-class default).
3. If neither is overridden, the result is `true`.

The resolved value SHALL be bound to the `RelayInstance` and SHALL be propagated to every `PartitionSlot` constructed for that instance. Subsequent changes to the executor-side metadata or to the SDK's `Relay.persistenceEnabled()` impl (via SDK redeploy) SHALL NOT affect already-running instances.

#### Scenario: Default-on with no override

- **WHEN** a Relay's `persistenceEnabled()` is the default (returns `true`) AND no executor override is present
- **THEN** `RelayInstance.persistenceEnabled()` SHALL return `true`

#### Scenario: Code-declared opt-out is honored

- **WHEN** `MarketRelay.persistenceEnabled()` returns `false` (user override) AND no executor override is present
- **THEN** `RelayInstance.persistenceEnabled()` SHALL return `false`; the `PartitionFlusher` SHALL drain-and-discard for this instance's Partitions; `NativeTransport.sendPartitionStateChange` SHALL NOT be invoked

#### Scenario: Executor override takes precedence

- **WHEN** `MarketRelay.persistenceEnabled()` returns `true` AND `AddRelay` metadata sets `relay_persistence.MarketRelay = false`
- **THEN** `RelayInstance.persistenceEnabled()` SHALL return `false`

#### Scenario: Resolved value frozen at instance start

- **WHEN** a `RelayInstance` is constructed with resolved `persistenceEnabled = true`, AND a subsequent executor config change sets `relay_persistence.<RelayClass> = false`
- **THEN** the running `RelayInstance` SHALL continue with `persistenceEnabled = true`; the new value SHALL take effect only on the next `stop_relay` + `start_relay` cycle for that instance

### Requirement: initState accepts optional restoration payload

The Java SDK's `Shard.initState` signature SHALL be `State initState(Optional<String> shardKey, Optional<RestoreInitPayload> initPayload)`. When `initPayload.isEmpty()`, `initState` SHALL behave as a fresh-start constructor. When `initPayload.isPresent()`, the user MAY inspect or ignore the payload bytes; the runtime SHALL automatically apply the payload AFTER `initState` returns:

1. If `payload.snapshotBytes().length > 0`, deserialize via `KryoCodec` (after header validation) and replace the user-constructed state.
2. If `payload.changeLogBytes().length > 0`, decode each WAL record (frame format from `relay-state-persistence`), validate header, deserialize Change via `KryoCodec`, and invoke `Shard.applyChange(state, change)` in seq order.

#### Scenario: Cold start passes Optional.empty()

- **WHEN** a Java Partition is constructed for the first time with no prior persisted state on the agent
- **THEN** the runtime SHALL invoke `initState(shardKey, Optional.empty())`

#### Scenario: Restart passes Optional.of(payload)

- **WHEN** a Java Partition is reconnected and the agent has snapshot or WAL bytes for it
- **THEN** the runtime SHALL invoke `initState(shardKey, Optional.of(payload))` AND, after `initState` returns, the runtime SHALL deserialize the snapshot (if any) and fold the change log via `applyChange` in seq order

### Requirement: Per-Partition flusher with toggle-aware drain

The Java SDK SHALL spawn at most one `PartitionFlusher` daemon thread per JVM, lazy-initialized via `AtomicReference<PartitionFlusher>` on the first `Flezha.registerRelay(...)` call that registers a Shard with a non-`NoChange` Change. JVMs that register only `NoChange` Shards SHALL NOT spawn a flusher thread.

The flusher SHALL run an infinite loop sleeping `linger_ms` (default 5; configurable via system property `flezha.flush.linger.ms` with env-var fallback `FLEZHA_FLUSH_LINGER_MS`) between iterations. Per iteration: iterate the registered Partitions; for each, acquire its `bufferLock`; call `drainToFrames` (applying coalescing per `relay-state-persistence-rust/design.md` Decision 3); release the lock; for each frame, invoke `sendFrame` if `persistenceEnabled = true`, OR discard if `false`.

#### Scenario: NoChange-only program does not spawn flusher

- **WHEN** a Java program calls `Flezha.registerRelay(MyRelay.class)` for Relays whose Shards all have `Change = NoChange`
- **THEN** no thread named `flezha-flusher` SHALL be spawned (verifiable via `Thread.getAllStackTraces().keySet().stream().anyMatch(t -> "flezha-flusher".equals(t.getName())) == false`)

#### Scenario: First non-NoChange registration starts the flusher

- **WHEN** a Java program calls `Flezha.registerRelay(...)` for a Relay whose Shard has `Change = MyChange`
- **THEN** exactly one daemon thread named `flezha-flusher` SHALL be spawned and remain alive for the JVM's lifetime

#### Scenario: Toggle-off drain discards without wire I/O

- **WHEN** a Relay with `persistenceEnabled = false` records 10 Changes via `state.apply(...)` and the flusher's linger window elapses
- **THEN** the Partition's flush buffer SHALL be empty after the tick AND `NativeTransport.sendPartitionStateChange` SHALL NOT have been invoked AND the Partition's `change_seq` counter SHALL have advanced by the post-coalesce record count

#### Scenario: Toggle-on drain ships frames via JNI

- **WHEN** a Relay with `persistenceEnabled = true` records 10 Changes (no coalescing) via `state.apply(...)` and the flusher's linger elapses
- **THEN** `NativeTransport.sendPartitionStateChange` SHALL have been invoked exactly 10 times with frame bytes built per the umbrella's wire format

### Requirement: Hot-path cost is bounded

The Java SDK's `state.apply(change)` SHALL pay exactly: one `Shard.applyChange(state, change)` invocation (user-supplied; cost varies) plus one `ArrayList.add(change)` operation (amortized O(1)). The hot path SHALL NOT serialize, fsync, network-send, or acquire any lock beyond the existing per-Partition sticky CAS and the runner thread's exclusive ownership of the buffer for the duration of `Shard.run`.

#### Scenario: No serialization on state.apply

- **WHEN** a Shard's `run` calls `state.apply(MyChange.foo())` 100 times in a tight loop
- **THEN** zero `KryoCodec.serialize`, `Kryo.writeClassAndObject`, or any serialization-equivalent call SHALL be issued from the runner thread during these 100 calls (verifiable via JFR profiling or by mocking `KryoCodec` and asserting zero invocations)

#### Scenario: No JNI calls on state.apply

- **WHEN** a Shard's `run` calls `state.apply(MyChange.foo())` 100 times
- **THEN** zero `NativeTransport.*` JNI calls SHALL be issued from the runner thread during these 100 calls

### Requirement: Coalescing applied at drain time, not on the hot path

The Java SDK SHALL apply coalescing inside `FlushableBuffer.drainToFrames`, AFTER the flusher acquires the buffer's lock. The hot path of `state.apply(...)` SHALL NOT touch any coalescing data structure.

The drain-time coalescing algorithm SHALL match the Rust implementation: build a `HashMap<CoalesceKey, Integer>` mapping each key to the index of the latest record carrying that key; build a parallel `boolean[]` mask marking earlier records with the same key as discarded; emit kept records in their original buffer order.

#### Scenario: Same-key records collapse to latest

- **WHEN** a Shard records 50 `BarTick(M1, T)` Changes via `state.apply(...)` within one linger window, all returning `Optional.of(CoalesceKey.fromHash(M1, T))` from `coalesceKey()`
- **THEN** the flusher's drain SHALL emit exactly one frame whose Change is the 50th `BarTick`

#### Scenario: No-key records preserved verbatim

- **WHEN** a Shard records 10 Changes whose `coalesceKey()` returns `Optional.empty()` within one linger window
- **THEN** the flusher's drain SHALL emit 10 frames in the same order they were pushed

### Requirement: 16-byte version header stamped on persisted Changes (Java)

The Java SDK SHALL stamp a 16-byte header on every Change record emitted to the wire path:

- bytes 0:1 — `sdk_lang: u8` SHALL equal `2` (Java).
- bytes 1:5 — `sdk_version: u32 BE` SHALL be parsed from the SDK jar's Manifest.mf `Implementation-Version` field as `(major << 16) | (minor << 8) | patch`.
- bytes 5:9 — `schema_version: u32 BE` SHALL equal `Shard.schemaVersion()` for the Shard owning the Change.
- bytes 9:10 — `format_id: u8` SHALL equal `4` (Kryo with stable registered type IDs) in v1.
- bytes 10:12 — reserved, MUST be zero.
- bytes 12:16 — `crc32: u32 BE` SHALL be `java.util.zip.CRC32.update(change_bytes).getValue()`.

The Java SDK SHALL refuse to deserialize any persisted blob whose header `sdk_lang != 2` (cross-language load) — throws `StateLoadException::CrossLanguageMismatch`. The Java SDK SHALL refuse to deserialize any blob whose `format_id != 4` unless the format is otherwise recognized — throws `StateLoadException::FormatNotSupported`.

#### Scenario: Java header bytes match expectations

- **WHEN** the Java SDK at `version 0.5.3`, Shard `schemaVersion = 1`, stamps a header for change_bytes `[0x01, 0x02, 0x03]`
- **THEN** the resulting 16-byte header SHALL equal `[0x02, 0x00, 0x00, 0x05, 0x03, 0x00, 0x00, 0x00, 0x01, 0x04, 0x00, 0x00, <crc32 BE bytes of [0x01, 0x02, 0x03]>]`

#### Scenario: Cross-language load fails loud

- **WHEN** the Java SDK attempts to decode a blob whose `sdk_lang = 1` (Rust)
- **THEN** the Java SDK SHALL throw `StateLoadException::CrossLanguageMismatch(expected = 2, found = 1)` and SHALL NOT proceed to deserialization

#### Scenario: schema_version mismatch fails loud

- **WHEN** the Java SDK at Shard `schemaVersion = 2` attempts to decode a record whose header carries `schema_version = 1`
- **THEN** the Java SDK SHALL throw `StateLoadException::SchemaMismatch(expected = 2, found = 1)` and SHALL NOT proceed to deserialization

### Requirement: Kryo serialization with stable registered type IDs

The Java SDK SHALL use Kryo for `format_id = 4` serialization with `setRegistrationRequired(true)`. The runtime SHALL register built-in flezha types (NoChange, RestoreInitPayload, CoalesceKey, primitives, common collections) at SDK init in a fixed order (stable IDs 1-N). User types from `Shard.kryoTypes()` SHALL be registered after built-ins in declaration order (stable IDs N+1, N+2, ...).

The Java SDK SHALL refuse to serialize any user-supplied object whose runtime class is not in the registered set — Kryo throws `IllegalArgumentException` per its `registrationRequired = true` semantics; the SDK wraps this as `StateSerializeException::UnregisteredClass(className)` and rethrows.

Reordering of `Shard.kryoTypes()` between SDK versions SHALL be an unsupported breaking change (changes the assigned IDs); appending new types to the end SHALL be safe (preserves prior IDs).

#### Scenario: Built-in types registered with stable IDs

- **WHEN** a JVM starts with the Java SDK loaded
- **THEN** the registered Kryo types SHALL include `NoChange`, `RestoreInitPayload`, `CoalesceKey`, `String`, `Long`, `Integer`, `Double`, `Boolean`, `byte[]`, `List`, `Map`, `Optional` at fixed stable IDs (the exact values are recorded in `KryoCodec` source as a documented constant)

#### Scenario: User types registered after built-ins

- **WHEN** `Shard.kryoTypes()` returns `[OhlcChange.class, OhlcState.class]` and built-ins occupy IDs 1-12
- **THEN** `OhlcChange.class` SHALL register with ID 13 AND `OhlcState.class` SHALL register with ID 14

#### Scenario: Unregistered class fails loud

- **WHEN** a Shard's `Change` references a nested class not in `kryoTypes()` (e.g. a nested `record OhlcChange.BarTick(Interval interval, ...)` where `Interval` is a record not registered)
- **THEN** the first `state.apply(...)` triggering serialization SHALL throw `StateSerializeException::UnregisteredClass("Interval")` and the JVM SHALL NOT silently succeed with auto-IDs

### Requirement: Wire-protocol command handlers (Java)

The Java SDK SHALL register command handlers for `RestoreInitState`, `CompactPartition`, and `PartitionMailboxStatus` per `relay-state-persistence`'s wire-protocol additions:

- `RestoreInitStateHandler`: receives the request, decodes payload, invokes `Shard.initState(shardKey, Optional.of(payload))`, runs the runtime auto-restore (snapshot deserialize + change-log fold), sets the partition state, replies `RestoreInitStateResponse{partition_id, terminal_seq}`.
- `CompactPartitionHandler`: receives the request, acquires the partition's sticky CAS, deep-copies state via Kryo, releases CAS, serializes with header stamping, ships via `NativeTransport.sendPartitionStateSnapshot`, replies `CompactPartitionResponse{partition_id, snapshot_seq, snapshot_bytes_size}`.
- `PartitionMailboxStatusHandler`: receives the request, updates the per-Partition mailbox observer state used by the existing `OverflowPolicy` machinery (`relay-bounded-mailbox`).

#### Scenario: RestoreInitState handler restores state

- **WHEN** the agent ships `RestoreInitState{partition_id, snapshot_bytes, change_log_bytes}` to a Java SDK
- **THEN** the SDK's `RestoreInitStateHandler` SHALL invoke `Shard.initState(shardKey, Optional.of(payload))`, run the auto-restore, set the partition state, AND reply with `RestoreInitStateResponse{partition_id, terminal_seq}` matching the highest seq from the change log

#### Scenario: CompactPartition handler clones state under sticky CAS

- **WHEN** the agent ships `CompactPartition{partition_id}` to a Java SDK
- **THEN** the SDK SHALL acquire the partition's sticky CAS (existing primitive), deep-copy the state, release the CAS, serialize with header, ship via `NativeTransport.sendPartitionStateSnapshot`, AND reply `CompactPartitionResponse{partition_id, snapshot_seq, snapshot_bytes_size}`

#### Scenario: PartitionMailboxStatus :full pauses producer

- **WHEN** the agent ships `PartitionMailboxStatus{partition_id, status: :full}` to a Java SDK
- **THEN** the SDK's `PartitionMailboxStatusHandler` SHALL update the per-Partition observer to "full"; subsequent `ctx.emit(...)` calls on channels feeding this Partition SHALL apply the channel's `OverflowPolicy` (Block parks, DropNewest discards, DropOldest discards-oldest)

### Requirement: Public Java SDK handler classes use canonical Relay vocabulary

The Java SDK SHALL expose handler classes whose names use canonical vocabulary: `AddRelayHandler`, `RemRelayHandler`, `RelayMessageHandler`, `PartitionStateHandler`, `PartitionApplyStateHandler`. Classes previously named `AddProcessorHandler`, `RemProcessorHandler`, `ProcessorMessageHandler`, `ProcessorStateHandler`, `ProcessorApplyStateHandler` SHALL NOT be present in the published API. Java packages SHALL likewise migrate any package-path components that contained `processor`, `operator`, `agent`, or `compute_engine` to the canonical replacements (`relay`, `executor`, `controller`). `FleetManager`-equivalent classes keep their names.

#### Scenario: Old handler classes do not exist
- **WHEN** a downstream Java consumer imports `com.flezha.sdk.handlers.AddProcessorHandler`
- **THEN** the import MUST fail to compile (class not found); the consumer MUST migrate to `com.flezha.sdk.handlers.AddRelayHandler`

#### Scenario: New handlers wire into dispatch
- **WHEN** a Java SDK Executor receives an `AddRelay` protobuf command from the Controller
- **THEN** dispatch MUST route the message to `AddRelayHandler.handle(...)` with the protobuf `relay_id` field exposed under the same name (`getRelayId()`)

### Requirement: Public Relay / Shard / Partition user-facing API names

The Java SDK SHALL expose user-extensible base classes/interfaces under the canonical names `Relay`, `Shard`, `Partition`, and `Channel`. The Executor process startup hook SHALL be `Executor.start(Config)` (or equivalent factory entry point); class names referring to the SDK process itself SHALL use `Executor` as the role term, not `ComputeEngine` or `Operator`.

#### Scenario: User extends Relay
- **WHEN** a user-defined Relay class declares `extends Relay`
- **THEN** the class MUST compile against the published Java SDK; declaring `extends Processor` MUST fail
