# java-sdk Specification

## Purpose

Defines the Java SDK for Flezha compute engines: the public API, protocol layer, command dispatch, transport selection, reconnection, the Operative / Operation / Segment runtime, and the JNI bridge to `flz_transport`. Conforms to `docs/sdk_protocol_spec.md`.

## Requirements

### Requirement: Java SDK Entry Point

The Java SDK SHALL provide `com.flezha.sdk.Flezha.newInstance(String namespace)` as the single entry point for starting a Flezha compute engine. The call MUST connect to the agent's control socket, send the handshake init message per `docs/sdk_protocol_spec.md` §5.2, wait for `AssignExecutorId`, and return a handle exposing `getId()` and `getOperatives()` (where `getOperatives()` returns a snapshot of currently-spawned Operative instances keyed by what the wire calls `operative_id` — see the `Operative Registration` requirement below for naming).

#### Scenario: Successful connection returns an assigned id
- **WHEN** an SDK user calls `Flezha.newInstance("my_app")` with the agent running and at least one Operative class registered
- **THEN** the call blocks until the agent responds with `AssignExecutorId`, and the returned handle's `getId()` returns the assigned id (non-null, non-empty)

#### Scenario: Agent unavailable triggers exponential backoff retries
- **WHEN** an SDK user calls `Flezha.newInstance("my_app")` and the agent's control socket is not yet listening
- **THEN** the SDK SHALL retry the connect with exponential backoff starting at 200 ms and capped at 30 s, and succeed once the agent becomes available

#### Scenario: newInstance without any registered operative class fails fast
- **WHEN** `Flezha.newInstance(namespace)` is called before any `OperativeRegistry.registerContinuous(...)` (or `Flezha.registerContinuous(...)`) call
- **THEN** the SDK SHALL throw an `IllegalStateException` describing the missing operative registration

### Requirement: Operative Registration

The Java SDK SHALL provide `OperativeRegistry.registerContinuous(Class<? extends ContinuousOperative>)` (and a thin static `Flezha.registerContinuous(Class<? extends ContinuousOperative>)` pass-through) that MUST be called **before** `Flezha.newInstance`. For each registered ContinuousOperative class the SDK SHALL: (a) instantiate the class via its public no-arg constructor; (b) call its `factoryId()` method to obtain the agent-facing factory id; (c) call its `channels(ChannelBuilder)` method exactly once and freeze the resulting `OperativeTopology`; (d) verify wiring constraints (entry-channel-type, type-equality between channel payload and producer/consumer Operation I/O, no unwired channels, no multi-producer, static-mode and dynamic-mode default-impl detection); (e) record `(factoryId → topology)` in the class registry. The registry MUST be frozen at `Flezha.newInstance(...)` time. There SHALL be no `OperativeHandler`-style factory registration in the public surface.

#### Scenario: Registering a continuous operative class before newInstance succeeds
- **WHEN** user code calls `Flezha.registerContinuous(MarketOperative.class)` and then `Flezha.newInstance("my_app")`
- **THEN** the handshake init JSON SHALL include `"market"` (the value `MarketOperative::factoryId` returns) in `factory_ids`, and subsequent `AddOperative{factory_id:"market"}` commands SHALL succeed by spawning a `ContinuousOperativeInstance`

#### Scenario: Registering a continuous operative class after newInstance is rejected
- **WHEN** user code calls `OperativeRegistry.registerContinuous(...)` after `Flezha.newInstance(...)` has returned
- **THEN** the SDK SHALL throw an `IllegalStateException`

#### Scenario: Duplicate factoryId across two continuous operative classes is rejected
- **WHEN** two distinct ContinuousOperative classes both return `"market"` from `factoryId()` and both are registered
- **THEN** the second `registerContinuous` call SHALL throw an `IllegalArgumentException` naming the duplicate factoryId

#### Scenario: Wiring mismatch fails registration with a diagnosable error
- **WHEN** an Operative declares `c.channel(Trade.class, "trade.parsed").from(ParseOperation.class).into(OhlcOperation.class)` but `OhlcOperation.inputType()` returns `Candle.class`
- **THEN** `registerContinuous` SHALL throw `OperativeConfigurationException` whose message names the channel `"trade.parsed"`, the expected payload type (`Trade`), and the actual consumer-input type (`Candle`)

#### Scenario: Entry channel of disallowed type fails registration
- **WHEN** an Operative declares `c.input(Trade.class, "trade.raw").into(ParseOperation.class)`
- **THEN** `registerContinuous` SHALL throw `OperativeConfigurationException` stating that entry channels accept only `byte[]` or `String` in v1

#### Scenario: Unwired channel fails registration
- **WHEN** an Operative declares `c.channel(Candle.class, "ohlc.candles")` and never calls `.from(...)` on the resulting `ChannelDecl`
- **THEN** `registerContinuous` SHALL throw `OperativeConfigurationException` naming the unwired channel

#### Scenario: Static-mode operation without segmentForOverridden is rejected
- **WHEN** an Operation returns a non-empty list from `operationKeys()` but its `segmentForOverridden()` returns the default `false`
- **THEN** `registerContinuous` SHALL throw `OperativeConfigurationException` naming the Operation and stating that static-mode operations must override `segmentFor` and `segmentForOverridden`

#### Scenario: Dynamic-mode-only declaration is rejected in v1
- **WHEN** an Operation returns `true` from `operationKeyOverridden()` but returns an empty list from `operationKeys()`
- **THEN** `registerContinuous` SHALL throw `OperativeConfigurationException` stating that dynamic-mode requires `operationKeys()` until `relay-dynamic-partitioning` ships

### Requirement: Operation Interface and Per-Segment Dispatch

The Java SDK SHALL expose a generic `Operation<Input, State, Output>` interface in `com.flezha.sdk.operation`. Implementers SHALL provide `name()`, `inputType() → Class<Input>`, `outputType() → Class<Output>`, `initState(Optional<String> operationKey) → State`, and `run(Input, State, OperationCtx) throws OperationException`. The default-method overrides `operationKeys()`, `segmentFor(Input)`, `segmentForOverridden()`, `operationKey(Input)`, and `operationKeyOverridden()` declare the segmentation mode (single by default; static when `operationKeys()` non-empty + `segmentForOverridden()` true; dynamic mode reserved for the future change). The runtime SHALL invoke `run` on a per-segment basis with sticky ordering (see the Sticky Per-Segment Ordering requirement) and SHALL pass each input to all segments returned by `segmentFor(input)` for static-mode operations (single-mode operations skip the call and route to segment index 0).

The `OperationCtx` parameter SHALL provide `emit(value)` for inter-operation or plugin-bound outputs, `emit(channel, value)` for explicit channel selection, and `emitWithMeta(value, meta)` / `emitWithMeta(channel, value, meta)` for plugin-bound outputs with user-attached metadata. `emitWithMeta` on inter-operation channels SHALL throw `OperationException.MetadataNotAllowedOnInterOperationChannel` naming the channel.

#### Scenario: Single-mode Operation runs on a single segment
- **WHEN** an Operation whose `operationKeys()` returns an empty list is registered as the consumer of an entry channel, and one input arrives on that channel
- **THEN** the SDK SHALL invoke the Operation's `run` once on the single allocated segment (segment index 0), with `state` argument equal to the value `initState(Optional.empty())` returned at operative startup

#### Scenario: Static-mode Operation creates one segment per operationKey
- **WHEN** an Operation's `operationKeys()` returns `["1m", "1h", "1d", "1w"]` and `segmentForOverridden()` returns `true`
- **THEN** at operative startup the SDK SHALL allocate exactly four `SegmentSlot` instances for that Operation, each constructed with `initState(Optional.of(key))` for its corresponding key

#### Scenario: Static-mode segmentFor fan-out enqueues input on each named segment
- **WHEN** an Operation's `segmentFor(input)` returns `["1m", "1h"]` for a given input
- **THEN** the SDK SHALL enqueue that input on the segments identified by `"1m"` and `"1h"` (each receives its own `run` invocation), and SHALL NOT enqueue it on `"1d"` or `"1w"`

#### Scenario: Run throwing OperationException logs and continues
- **WHEN** `Operation.run(...)` throws `OperationException` on a given input
- **THEN** the SDK SHALL log the exception with `(operativeInstanceId, operationName, segmentKey)` context, SHALL discard any partial `ctx.emit(...)` calls made during this invocation, and SHALL continue dispatching subsequent inputs on the same segment

#### Scenario: Run throwing arbitrary RuntimeException logs and continues
- **WHEN** `Operation.run(...)` throws a non-`OperationException` `RuntimeException`
- **THEN** the SDK SHALL log the exception with the same `(operativeInstanceId, operationName, segmentKey)` context, discard partial emits, and continue dispatching subsequent inputs on the same segment

#### Scenario: Inter-operation zero-copy preserves value identity
- **WHEN** Operation A emits a non-null value `v` on an inter-operation channel that Operation B consumes (single-segment fan-out, single JVM)
- **THEN** the value passed to Operation B's `run` SHALL be `==`-equal (reference-equal) to `v` — the runtime SHALL NOT copy or serialize the value between Operations in the same JVM

#### Scenario: emitWithMeta on an inter-operation channel returns error
- **WHEN** an Operation whose declared output is an inter-operation channel calls `ctx.emitWithMeta(value, Map.of("k", "v"))`
- **THEN** the SDK SHALL throw `OperationException.MetadataNotAllowedOnInterOperationChannel` whose message names the channel

#### Scenario: Plugin-bound emit attaches the channel name as metadata on the wire
- **WHEN** an Operation whose declared output is a plugin-bound channel `"ohlc.candles"` calls `ctx.emit(candle)`
- **THEN** the SDK SHALL serialize `candle` via the channel's serializer, frame the bytes with `metadata = {"channel": "ohlc.candles"}`, and send the frame on the operative's secondary stream

#### Scenario: Plugin-bound emitWithMeta appends user metadata after the channel key
- **WHEN** an Operation calls `ctx.emitWithMeta(candle, Map.of("trace_id", "abc"))` on a plugin-bound channel `"ohlc.candles"`
- **THEN** the SDK SHALL frame the bytes with `metadata` containing both `"channel" -> "ohlc.candles"` and `"trace_id" -> "abc"`

#### Scenario: Reserved metadata key in user map is silently dropped
- **WHEN** an Operation calls `ctx.emitWithMeta(candle, Map.of("channel", "user_value", "k", "v"))` on a plugin-bound channel `"ohlc.candles"`
- **THEN** the SDK SHALL frame the bytes with `metadata` containing exactly `"channel" -> "ohlc.candles"` and `"k" -> "v"`, dropping the user-supplied `"channel"` entry without raising

### Requirement: Sticky Per-Segment Ordering Under ThreadPool Dispatch

The Java SDK's worker pool (sized by `FLEZHA_WORKER_POOL_SIZE`, default = `Runtime.availableOperatives()`) SHALL guarantee that for any single `(operativeInstance, operation, segment)` triple, two invocations of that Operation's `run(...)` method on that Segment never execute concurrently, and invocations observe their inputs in the exact order those inputs were enqueued onto the Segment's mailbox. This invariant MUST hold regardless of worker-pool size, number of concurrent Operative instances, number of Segments per Operation, or transport (UDS/QUIC).

#### Scenario: Same segment never runs concurrently under load
- **WHEN** 10,000 inputs targeting the same `(operativeInstance, operation, segmentKey)` triple arrive back-to-back on one or more entry channels while the SDK uses a 16-thread worker pool
- **THEN** at any wall-clock instant, at most one thread SHALL be inside that Segment's `run(...)` method

#### Scenario: Different segments of same operation may run concurrently
- **WHEN** inputs arrive simultaneously for two distinct segments of the same Operation (for example `("ohlc", "1m")` and `("ohlc", "1h")`) and the worker pool has ≥ 2 threads
- **THEN** the two Segments' `run(...)` methods MAY execute concurrently on different threads

#### Scenario: Per-segment inputs delivered in arrival order
- **WHEN** inputs `"a"`, `"b"`, `"c"` are enqueued onto one Segment's mailbox in that order with an append-to-state Operation
- **THEN** the Segment's State SHALL end with `["a", "b", "c"]` in exactly that order

### Requirement: Command Dispatch

The Java SDK SHALL implement handlers for every `CommandRequest` variant defined in `executor_commands.proto`: `AssignExecutorId`, `ConnectSocket`, `AddOperative`, `RemOperative`, `SegmentState`, `MigrationSerialization`, `MigrationDeserialization`, `OperationMessage`, `ExecutorDisconnect`. Each handler MUST produce a `CommandResponse` that echoes the original `reference_id` and sets `status = "success"` or `status = "error"` with populated `errors` per `docs/sdk_protocol_spec.md` §6. `AddOperative` and `RemOperative` SHALL spawn and tear down operative instances respectively (the wire field is named `operative_id`; the SDK maps it to `operativeInstanceId`).

`AddOperative` SHALL read the optional `kind` field: an empty or absent value SHALL be interpreted as `:continuous` and SHALL spawn a `ContinuousOperativeInstance` exactly as before this change; a value resolving to `:bounded` SHALL spawn a `BoundedOperativeInstance`. The per-executor instance registry SHALL hold a sealed `Operative` (provisional name) permitting the continuous-instance and bounded-instance variants, keyed by `operativeInstanceId`; the variant SHALL be resolved once per inbound frame at the dispatch boundary, leaving the operative dispatch hot path unchanged save that single per-frame branch. `RemOperative` SHALL remove either variant by instance id. `OperationMessage` SHALL respond with the canonical "unsupported" error until `relay-message-handlers` ships per-Operation `handleMessage`.

#### Scenario: AddOperative spawns a continuous operative instance
- **WHEN** the agent sends `AddOperative{operative_id:"p1", factory_id:"market"}` and ContinuousOperative class `MarketOperative` is registered with `factoryId() == "market"`
- **THEN** the SDK SHALL allocate a `ContinuousOperativeInstance` keyed by `"p1"`, call `initState(...)` for every Operation×Segment declared by `MarketOperative`, and respond with `status = "success"`

#### Scenario: AddOperative with init_values seeds initial-state for each segment
- **WHEN** the agent sends `AddOperative{operative_id:"p1", factory_id:"market", init_values:"{\"symbol\":\"BTC\"}"}` and `MarketOperative` declares one Operation with single-segment mode
- **THEN** the SDK SHALL pass the JSON string into the Operation's `initState(Optional.empty())` (the runtime makes the JSON available via a per-operative context), invoke `run` only after `initState` returns, and respond with `status = "success"`

#### Scenario: AddOperative with unknown factory_id returns error
- **WHEN** the agent sends `AddOperative{factory_id:"missing"}`
- **THEN** the SDK SHALL respond with `status = "error"` and `errors` containing a string naming the missing factory id

#### Scenario: AddOperative with empty kind spawns a continuous operative (backward-compatible)
- **WHEN** the agent sends `AddOperative{operative_id:"p1", factory_id:"market"}` with the `kind` field empty or absent
- **THEN** the SDK SHALL resolve the kind to `:continuous` and spawn a `ContinuousOperativeInstance` exactly as before this change

#### Scenario: AddOperative with kind bounded spawns a bounded variant
- **WHEN** the agent sends `AddOperative{operative_id:"t1", factory_id:"greet", kind:"bounded"}` and `GreetOperative` is registered as a bounded operative with `factoryId() == "greet"`
- **THEN** the SDK SHALL register a `BoundedOperativeInstance` keyed by `"t1"` as the bounded variant of the sealed operative registry and respond with `status = "success"`

#### Scenario: RemOperative removes either operative variant
- **WHEN** the agent sends `RemOperative{operative_id:"t1"}` for a live bounded operative instance (and likewise for a continuous operative instance)
- **THEN** the SDK SHALL remove the instance from the sealed operative registry by id and respond with `status = "success"`

#### Scenario: SegmentState returns the per-Operation / per-Segment JSON shape
- **WHEN** the agent sends `SegmentState{operative_id:"p1"}` and `p1` exists with two Operations (`operationA` single-mode, `operationB` static-mode with keys `["1m", "1h"]`)
- **THEN** the SDK SHALL respond with `status = "success"` and `raw_data` containing JSON of shape `{"operationA": {<state>}, "operationB": {"1m": {<state>}, "1h": {<state>}}}` (whichever exact shape the Rust SDK emits per `relay-mvp-rust/design.md` Decision 10 — the Java SDK SHALL match byte-for-byte)

#### Scenario: MigrationSerialization removes the operative instance
- **WHEN** the agent sends `MigrationSerialization{operative_id:"p1"}` and `p1` exists
- **THEN** the SDK SHALL respond with `status = "success"` and `raw_data` containing the per-Operation / per-Segment JSON (same shape as `SegmentState`), AND `p1` SHALL be removed from the operative instance registry

#### Scenario: OperationMessage returns canonical unsupported error pre-relay-message-handlers
- **WHEN** the agent sends `OperationMessage{operative_id:"p1", payload:<...>}` to a v1 SDK
- **THEN** the SDK SHALL respond with `OperationMessageResponse{ payload: "", error: "operative does not support handle_message" }`

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

#### Scenario: Decoding AddOperative request extracts all fields
- **WHEN** the SDK receives a protobuf-encoded `CommandRequest` with an `AddOperative` oneof case
- **THEN** the decoder SHALL expose `operative_id`, `factory_id`, and `init_values` as Java strings

### Requirement: Transport Selection

The Java SDK SHALL honor the `FLEZHA_TRANSPORT` environment variable (`uds` or `quic`) for transport selection, defaulting to `uds` when unset. For QUIC, the SDK SHALL also honor `FLEZHA_QUIC_ADDRESS`, `FLEZHA_CA_CERT_PATH`, `FLEZHA_SERVER_CERT_PATH`, `FLEZHA_SERVER_KEY_PATH`. For UDS, the SDK SHALL honor `FLEZHA_SOCKET_FILES_PATH`. All env var names and defaults MUST match `docs/sdk_protocol_spec.md` §2.

#### Scenario: Default transport is UDS
- **WHEN** `FLEZHA_TRANSPORT` is unset and `Flezha.newInstance(...)` is called
- **THEN** the SDK SHALL connect to `{FLEZHA_SOCKET_FILES_PATH or app_data/sockets}/executor_control_channel.sock`

#### Scenario: QUIC transport uses configured address and cert paths
- **WHEN** `FLEZHA_TRANSPORT=quic` and `FLEZHA_QUIC_ADDRESS=127.0.0.1:4433`
- **THEN** the SDK SHALL open a QUIC connection to that address using the CA cert at the configured path, opening the control channel on the first bidi stream per §2 "QUIC Stream Mapping"

### Requirement: Auto-Reconnect on Unexpected Disconnect

The Java SDK SHALL distinguish intentional disconnects (`ExecutorDisconnect` command handled → status `Stopped`) from unexpected ones (BrokenPipe / UnexpectedEof / ConnectionReset / QUIC connection lost). On an unexpected disconnect the SDK MUST clear `io_streams`, the live operative registry, and work queues; reset `executor_id` to `undefined`; and reconnect with exponential backoff (200 ms, 400 ms, 800 ms, …, capped at 30 s) per `docs/sdk_protocol_spec.md` §5 "Auto-Reconnection".

#### Scenario: Agent crash triggers reconnect
- **WHEN** the agent process is killed while the SDK has status `Running`
- **THEN** the SDK SHALL detect the disconnect via the native layer, clear local state, and reconnect once the agent is available again, receiving a fresh `executor_id`

#### Scenario: Intentional disconnect does not reconnect
- **WHEN** the SDK receives `ExecutorDisconnect{}` and responds successfully
- **THEN** the SDK SHALL close its sockets and NOT attempt any reconnection

### Requirement: Worker Pool and Dispatch Throughput Floor

The Java SDK SHALL operate on a single shared bounded executor sized by `FLEZHA_WORKER_POOL_SIZE` (default = `Runtime.availableOperatives()`), draining all `SegmentSlot` mailboxes work-steal-style. The SDK SHALL sustain at least the throughput floor (in messages/second) measured against the v1 implementation at merge time on the regression bench (`100 operatives × 1 operation × 1 segment × 10K msg`, no-op Operation, UDS transport, default worker-pool size; see `relay-mvp-java/design.md` Decision 7) invoked via `mix bench --runtime java --shape regression -n 100 -m 10000`. The bench-regression-java CI job is informational in v1; the literal floor value is recorded in this requirement once `mix bench --runtime java` has been measured 3× on the reference host (median, rounded down to a clean number) — until then the recorded floor is "TBD pending v1 measurement".

#### Scenario: Reference bench meets the Java throughput floor
- **WHEN** a developer runs `MIX_ENV=prod mix bench --runtime java --shape regression -n 100 -m 10000` on a modern 8-core x86_64 laptop with a warm JVM (≥ 30 s warmup)
- **THEN** the reported `Throughput:` line SHALL be ≥ the recorded Java floor value

#### Scenario: Default worker pool size matches available operatives
- **WHEN** the SDK starts with `FLEZHA_WORKER_POOL_SIZE` unset
- **THEN** the worker pool SHALL be sized to `Runtime.availableOperatives()`

### Requirement: JNI Bridge to flz_transport

The Java SDK SHALL interact with the transport layer exclusively through a JNI bridge that wraps `flz_transport`'s C FFI. The bridge MUST expose Java-side operations for: opening a transport context with a `FlezhaConfig`, connecting (client mode), opening streams, reading and writing stream data, registering a log callback, and destroying the context. All native buffers returned by the bridge MUST be copied into Java `byte[]` / `String` before the native memory is freed.

#### Scenario: Native buffer freed after copy into Java byte[]
- **WHEN** the JNI bridge receives a data callback carrying a native buffer pointer and length
- **THEN** the bridge SHALL allocate a Java `byte[]` of equal length, copy the native bytes into it, invoke the Java-side handler with the `byte[]`, and ensure the native buffer is freed (or returned to `flz_transport` ownership) before control returns to the native caller

#### Scenario: flezha_version mismatch is logged
- **WHEN** the SDK starts and `flezha_version()` returns a string that does not equal the SDK's bundled expected version
- **THEN** the SDK SHALL log a WARNING naming both versions and continue startup

### Requirement: Operation interface declares Change, Clock, Config generic parameters

The Java SDK's `Operation` interface SHALL be parameterized as `Operation<Input, State, Output, Change extends com.flezha.sdk.state.Change, Clock extends com.flezha.sdk.state.Clock, Config>`. The interface SHALL declare reflection methods `Class<Change> changeType()`, `Class<Clock> clockType()`, `Class<Config> configType()` returning the runtime `Class` for each generic parameter (Java type erasure precludes recovering them otherwise).

The interface SHALL declare lifecycle methods:
- `default void applyChange(State state, Change change)` whose default impl is a no-op for `NoChange` and throws `UnsupportedOperationException` for any other Change type (forcing user override).
- `default Optional<CoalesceKey> coalesceKey(Change change)` whose default impl delegates to `change.coalesceKey()`.
- `default long compactAfterBytes()` returning `64L * 1024 * 1024`.
- `default int schemaVersion()` returning `1`.
- `default List<Class<?>> kryoTypes()` returning `List.of()` — the user-declared list of types to register with Kryo for serialization, in stable-ID order.
- `default Operation<...> fromConfig(Config cfg)` whose default invokes the no-arg constructor when `Config = Void`, and throws `UnsupportedOperationException` for non-Void Config types (forcing user override).

#### Scenario: Default Change is NoChange skips persistence machinery

- **WHEN** a Java Operation does not declare its `Change` parameter (defaults to `NoChange`)
- **THEN** the runtime SHALL detect via `changeType() == NoChange.class` (reference equality) at operative-build time and SHALL NOT register a flush buffer or `SegmentFlusher` entry for this Segment

#### Scenario: User-defined Change opts the Operation into persistence

- **WHEN** a Java Operation declares `Change = MyChange` (any non-`NoChange` user type implementing `com.flezha.sdk.state.Change`)
- **THEN** the runtime SHALL register a typed flush buffer (`List<MyChange>`) on each Segment of that Operation and SHALL include those Segments in the per-JVM flusher's drain rotation

#### Scenario: Default applyChange throws on non-NoChange Change without override

- **WHEN** a Java Operation declares `Change = MyChange` but does NOT override `applyChange`
- **THEN** the operative registration step SHALL succeed (the default impl exists) but the first `state.apply(MyChange.foo())` call SHALL throw `UnsupportedOperationException` whose message names the missing override

#### Scenario: Default fromConfig invokes no-arg constructor for Void Config

- **WHEN** a Java Operation declares `Config = Void` and does NOT override `fromConfig`
- **THEN** the runtime's `fromConfig(null)` invocation SHALL call the Operation's public no-arg constructor via reflection; absence of a no-arg constructor SHALL throw `IllegalStateException` whose message names the Operation

#### Scenario: User-defined Config ships through Jackson deserialization

- **WHEN** a Java Operation declares `Config = MyConfig` (a Jackson-deserializable POJO/record) and overrides `fromConfig(MyConfig cfg)`
- **THEN** the runtime SHALL deserialize the agent-shipped JSON config bytes via Jackson into `MyConfig` and SHALL invoke `operation.fromConfig(cfg)` to construct the Operation at Segment init; deserialization failure SHALL throw `OperativeConfigurationException("Config deserialize failed: " + operationName)`

### Requirement: Operation.run receives StateHandle, not direct State reference

The Java SDK's `Operation.run` signature SHALL be `void run(Input input, StateHandle<State, Change> state, OperationCtx ctx) throws OperationException` (was `void run(Input input, State state, OperationCtx ctx)`). `StateHandle<State, Change>` SHALL provide:

- `State get()` — read access to the underlying state.
- `void apply(Change change)` — runs `Operation::applyChange(state, change)` and pushes the Change onto the per-Segment flush buffer.
- NO setter, no direct-mutation method. Direct `state.field = value` is impossible without going through `apply`.

#### Scenario: Direct field mutation requires apply

- **WHEN** a Java Operation's `run` body wants to mutate `state.someField` from a value `v1` to `v2`
- **THEN** the user MUST declare a Change (e.g. `record SetSomeField(Object newValue) implements Change {}`), implement `applyChange` to write the field, and call `state.apply(new SetSomeField(v2))`; direct `state.get().someField = v2` is technically possible (mutable field on the underlying State) but bypasses persistence and breaks `replayEquals` — flagged as user error in documentation but not structurally prevented in Java (unlike Rust's `Deref` chokepoint)

#### Scenario: Read access via get() is allocation-free

- **WHEN** a Java Operation's `run` body reads `state.get().someField`
- **THEN** the read SHALL succeed via the handle's `get()` method (one method invocation, JIT-inlinable), no Change recording, no buffer mutation

#### Scenario: Mutation via apply runs applyChange and records

- **WHEN** a Java Operation's `run` body calls `state.apply(MyChange.foo())`
- **THEN** `Operation.applyChange(state, &MyChange.foo())` SHALL run on the underlying state AND `MyChange.foo()` SHALL be appended to the per-Segment flush buffer (observable via `OperationTestHarness.recordedChanges()` and via the per-JVM flusher's next tick)

### Requirement: Operation declares Clock generic parameter with default WallClock

The Java SDK's `Operation` interface SHALL declare `Clock extends com.flezha.sdk.state.Clock` as a generic parameter, defaulting to `com.flezha.sdk.state.WallClock`. `Clock` SHALL be a sealed Java interface with `permits WallClock, LogicalClock, EventTimeFrom`. v1 of the Java SDK SHALL fully wire only `WallClock`; Operations declaring `LogicalClock` or `EventTimeFrom` SHALL fail Operative registration with `OperativeConfigurationException::ClockNotImplementedYet(String clockTypeName)` until a follow-up change wires the remaining clocks.

#### Scenario: Default Clock is WallClock

- **WHEN** a Java Operation does not declare its `Clock` parameter
- **THEN** `Operation.clockType() == WallClock.class` AND Operative registration SHALL succeed

#### Scenario: Non-WallClock fails registration in v1

- **WHEN** a Java Operation declares `Clock = LogicalClock`
- **THEN** `Flezha.registerContinuous(...)` SHALL throw `OperativeConfigurationException` whose message contains "Clock not implemented yet: LogicalClock"

### Requirement: Operative declares persistenceEnabled default-method, frozen at instance start

The Java SDK's `ContinuousOperative` interface SHALL declare `default boolean persistenceEnabled() { return true; }`. The runtime SHALL resolve a per-Operative-instance `persistenceEnabled: boolean` at the moment `AddOperative` is processed using:

1. If the `AddOperative` metadata carries `operative_persistence.<OperativeClass>` with a boolean value (executor override), use that.
2. Otherwise, use the value of `ContinuousOperative.persistenceEnabled()` (the Operative-class default).
3. If neither is overridden, the result is `true`.

The resolved value SHALL be bound to the `ContinuousOperativeInstance` and SHALL be propagated to every `SegmentSlot` constructed for that instance. Subsequent changes to the executor-side metadata or to the SDK's `ContinuousOperative.persistenceEnabled()` impl (via SDK redeploy) SHALL NOT affect already-running instances.

#### Scenario: Default-on with no override

- **WHEN** an Operative's `persistenceEnabled()` is the default (returns `true`) AND no executor override is present
- **THEN** `ContinuousOperativeInstance.persistenceEnabled()` SHALL return `true`

#### Scenario: Code-declared opt-out is honored

- **WHEN** `MarketOperative.persistenceEnabled()` returns `false` (user override) AND no executor override is present
- **THEN** `ContinuousOperativeInstance.persistenceEnabled()` SHALL return `false`; the `SegmentFlusher` SHALL drain-and-discard for this instance's Segments; `NativeTransport.sendSegmentStateChange` SHALL NOT be invoked

#### Scenario: Executor override takes precedence

- **WHEN** `MarketOperative.persistenceEnabled()` returns `true` AND `AddOperative` metadata sets `operative_persistence.MarketOperative = false`
- **THEN** `ContinuousOperativeInstance.persistenceEnabled()` SHALL return `false`

#### Scenario: Resolved value frozen at instance start

- **WHEN** a `ContinuousOperativeInstance` is constructed with resolved `persistenceEnabled = true`, AND a subsequent executor config change sets `operative_persistence.<OperativeClass> = false`
- **THEN** the running `ContinuousOperativeInstance` SHALL continue with `persistenceEnabled = true`; the new value SHALL take effect only on the next `stop_operative` + `start_operative` cycle for that instance

### Requirement: initState accepts optional restoration payload

The Java SDK's `Operation.initState` signature SHALL be `State initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload)`. When `initPayload.isEmpty()`, `initState` SHALL behave as a fresh-start constructor. When `initPayload.isPresent()`, the user MAY inspect or ignore the payload bytes; the runtime SHALL automatically apply the payload AFTER `initState` returns:

1. If `payload.snapshotBytes().length > 0`, deserialize via `KryoCodec` (after header validation) and replace the user-constructed state.
2. If `payload.changeLogBytes().length > 0`, decode each WAL record (frame format from `relay-state-persistence`), validate header, deserialize Change via `KryoCodec`, and invoke `Operation.applyChange(state, change)` in seq order.

#### Scenario: Cold start passes Optional.empty()

- **WHEN** a Java Segment is constructed for the first time with no prior persisted state on the agent
- **THEN** the runtime SHALL invoke `initState(operationKey, Optional.empty())`

#### Scenario: Restart passes Optional.of(payload)

- **WHEN** a Java Segment is reconnected and the agent has snapshot or WAL bytes for it
- **THEN** the runtime SHALL invoke `initState(operationKey, Optional.of(payload))` AND, after `initState` returns, the runtime SHALL deserialize the snapshot (if any) and fold the change log via `applyChange` in seq order

### Requirement: Per-Segment flusher with toggle-aware drain

The Java SDK SHALL spawn at most one `SegmentFlusher` daemon thread per JVM, lazy-initialized via `AtomicReference<SegmentFlusher>` on the first `Flezha.registerContinuous(...)` call that registers an Operation with a non-`NoChange` Change. JVMs that register only `NoChange` Operations SHALL NOT spawn a flusher thread.

The flusher SHALL run an infinite loop sleeping `linger_ms` (default 5; configurable via system property `flezha.flush.linger.ms` with env-var fallback `FLEZHA_FLUSH_LINGER_MS`) between iterations. Per iteration: iterate the registered Segments; for each, acquire its `bufferLock`; call `drainToFrames` (applying coalescing per `relay-state-persistence-rust/design.md` Decision 3); release the lock; for each frame, invoke `sendFrame` if `persistenceEnabled = true`, OR discard if `false`.

#### Scenario: NoChange-only program does not spawn flusher

- **WHEN** a Java program calls `Flezha.registerContinuous(MyOperative.class)` for Operatives whose Operations all have `Change = NoChange`
- **THEN** no thread named `flezha-flusher` SHALL be spawned (verifiable via `Thread.getAllStackTraces().keySet().stream().anyMatch(t -> "flezha-flusher".equals(t.getName())) == false`)

#### Scenario: First non-NoChange registration starts the flusher

- **WHEN** a Java program calls `Flezha.registerContinuous(...)` for an Operative whose Operation has `Change = MyChange`
- **THEN** exactly one daemon thread named `flezha-flusher` SHALL be spawned and remain alive for the JVM's lifetime

#### Scenario: Toggle-off drain discards without wire I/O

- **WHEN** an Operative with `persistenceEnabled = false` records 10 Changes via `state.apply(...)` and the flusher's linger window elapses
- **THEN** the Segment's flush buffer SHALL be empty after the tick AND `NativeTransport.sendSegmentStateChange` SHALL NOT have been invoked AND the Segment's `change_seq` counter SHALL have advanced by the post-coalesce record count

#### Scenario: Toggle-on drain ships frames via JNI

- **WHEN** an Operative with `persistenceEnabled = true` records 10 Changes (no coalescing) via `state.apply(...)` and the flusher's linger elapses
- **THEN** `NativeTransport.sendSegmentStateChange` SHALL have been invoked exactly 10 times with frame bytes built per the umbrella's wire format

### Requirement: Hot-path cost is bounded

The Java SDK's `state.apply(change)` SHALL pay exactly: one `Operation.applyChange(state, change)` invocation (user-supplied; cost varies) plus one `ArrayList.add(change)` operation (amortized O(1)). The hot path SHALL NOT serialize, fsync, network-send, or acquire any lock beyond the existing per-Segment sticky CAS and the runner thread's exclusive ownership of the buffer for the duration of `Operation.run`.

#### Scenario: No serialization on state.apply

- **WHEN** an Operation's `run` calls `state.apply(MyChange.foo())` 100 times in a tight loop
- **THEN** zero `KryoCodec.serialize`, `Kryo.writeClassAndObject`, or any serialization-equivalent call SHALL be issued from the runner thread during these 100 calls (verifiable via JFR profiling or by mocking `KryoCodec` and asserting zero invocations)

#### Scenario: No JNI calls on state.apply

- **WHEN** an Operation's `run` calls `state.apply(MyChange.foo())` 100 times
- **THEN** zero `NativeTransport.*` JNI calls SHALL be issued from the runner thread during these 100 calls

### Requirement: Coalescing applied at drain time, not on the hot path

The Java SDK SHALL apply coalescing inside `FlushableBuffer.drainToFrames`, AFTER the flusher acquires the buffer's lock. The hot path of `state.apply(...)` SHALL NOT touch any coalescing data structure.

The drain-time coalescing algorithm SHALL match the Rust implementation: build a `HashMap<CoalesceKey, Integer>` mapping each key to the index of the latest record carrying that key; build a parallel `boolean[]` mask marking earlier records with the same key as discarded; emit kept records in their original buffer order.

#### Scenario: Same-key records collapse to latest

- **WHEN** an Operation records 50 `BarTick(M1, T)` Changes via `state.apply(...)` within one linger window, all returning `Optional.of(CoalesceKey.fromHash(M1, T))` from `coalesceKey()`
- **THEN** the flusher's drain SHALL emit exactly one frame whose Change is the 50th `BarTick`

#### Scenario: No-key records preserved verbatim

- **WHEN** an Operation records 10 Changes whose `coalesceKey()` returns `Optional.empty()` within one linger window
- **THEN** the flusher's drain SHALL emit 10 frames in the same order they were pushed

### Requirement: 16-byte version header stamped on persisted Changes (Java)

The Java SDK SHALL stamp a 16-byte header on every Change record emitted to the wire path:

- bytes 0:1 — `sdk_lang: u8` SHALL equal `2` (Java).
- bytes 1:5 — `sdk_version: u32 BE` SHALL be parsed from the SDK jar's Manifest.mf `Implementation-Version` field as `(major << 16) | (minor << 8) | patch`.
- bytes 5:9 — `schema_version: u32 BE` SHALL equal `Operation.schemaVersion()` for the Operation owning the Change.
- bytes 9:10 — `format_id: u8` SHALL equal `4` (Kryo with stable registered type IDs) in v1.
- bytes 10:12 — reserved, MUST be zero.
- bytes 12:16 — `crc32: u32 BE` SHALL be `java.util.zip.CRC32.update(change_bytes).getValue()`.

The Java SDK SHALL refuse to deserialize any persisted blob whose header `sdk_lang != 2` (cross-language load) — throws `StateLoadException::CrossLanguageMismatch`. The Java SDK SHALL refuse to deserialize any blob whose `format_id != 4` unless the format is otherwise recognized — throws `StateLoadException::FormatNotSupported`.

#### Scenario: Java header bytes match expectations

- **WHEN** the Java SDK at `version 0.5.3`, Operation `schemaVersion = 1`, stamps a header for change_bytes `[0x01, 0x02, 0x03]`
- **THEN** the resulting 16-byte header SHALL equal `[0x02, 0x00, 0x00, 0x05, 0x03, 0x00, 0x00, 0x00, 0x01, 0x04, 0x00, 0x00, <crc32 BE bytes of [0x01, 0x02, 0x03]>]`

#### Scenario: Cross-language load fails loud

- **WHEN** the Java SDK attempts to decode a blob whose `sdk_lang = 1` (Rust)
- **THEN** the Java SDK SHALL throw `StateLoadException::CrossLanguageMismatch(expected = 2, found = 1)` and SHALL NOT proceed to deserialization

#### Scenario: schema_version mismatch fails loud

- **WHEN** the Java SDK at Operation `schemaVersion = 2` attempts to decode a record whose header carries `schema_version = 1`
- **THEN** the Java SDK SHALL throw `StateLoadException::SchemaMismatch(expected = 2, found = 1)` and SHALL NOT proceed to deserialization

### Requirement: Kryo serialization with stable registered type IDs

The Java SDK SHALL use Kryo for `format_id = 4` serialization with `setRegistrationRequired(true)`. The runtime SHALL register built-in flezha types (NoChange, RestoreInitPayload, CoalesceKey, primitives, common collections) at SDK init in a fixed order (stable IDs 1-N). User types from `Operation.kryoTypes()` SHALL be registered after built-ins in declaration order (stable IDs N+1, N+2, ...).

The Java SDK SHALL refuse to serialize any user-supplied object whose runtime class is not in the registered set — Kryo throws `IllegalArgumentException` per its `registrationRequired = true` semantics; the SDK wraps this as `StateSerializeException::UnregisteredClass(className)` and rethrows.

Reordering of `Operation.kryoTypes()` between SDK versions SHALL be an unsupported breaking change (changes the assigned IDs); appending new types to the end SHALL be safe (preserves prior IDs).

#### Scenario: Built-in types registered with stable IDs

- **WHEN** a JVM starts with the Java SDK loaded
- **THEN** the registered Kryo types SHALL include `NoChange`, `RestoreInitPayload`, `CoalesceKey`, `String`, `Long`, `Integer`, `Double`, `Boolean`, `byte[]`, `List`, `Map`, `Optional` at fixed stable IDs (the exact values are recorded in `KryoCodec` source as a documented constant)

#### Scenario: User types registered after built-ins

- **WHEN** `Operation.kryoTypes()` returns `[OhlcChange.class, OhlcState.class]` and built-ins occupy IDs 1-12
- **THEN** `OhlcChange.class` SHALL register with ID 13 AND `OhlcState.class` SHALL register with ID 14

#### Scenario: Unregistered class fails loud

- **WHEN** an Operation's `Change` references a nested class not in `kryoTypes()` (e.g. a nested `record OhlcChange.BarTick(Interval interval, ...)` where `Interval` is a record not registered)
- **THEN** the first `state.apply(...)` triggering serialization SHALL throw `StateSerializeException::UnregisteredClass("Interval")` and the JVM SHALL NOT silently succeed with auto-IDs

### Requirement: Wire-protocol command handlers (Java)

The Java SDK SHALL register command handlers for `RestoreInitState`, `CompactSegment`, and `SegmentMailboxStatus` per `relay-state-persistence`'s wire-protocol additions:

- `RestoreInitStateHandler`: receives the request, decodes payload, invokes `Operation.initState(operationKey, Optional.of(payload))`, runs the runtime auto-restore (snapshot deserialize + change-log fold), sets the segment state, replies `RestoreInitStateResponse{segment_id, terminal_seq}`.
- `CompactSegmentHandler`: receives the request, acquires the segment's sticky CAS, deep-copies state via Kryo, releases CAS, serializes with header stamping, ships via `NativeTransport.sendSegmentStateSnapshot`, replies `CompactSegmentResponse{segment_id, snapshot_seq, snapshot_bytes_size}`.
- `SegmentMailboxStatusHandler`: receives the request, updates the per-Segment mailbox observer state used by the existing `OverflowPolicy` machinery (`relay-bounded-mailbox`).

#### Scenario: RestoreInitState handler restores state

- **WHEN** the agent ships `RestoreInitState{segment_id, snapshot_bytes, change_log_bytes}` to a Java SDK
- **THEN** the SDK's `RestoreInitStateHandler` SHALL invoke `Operation.initState(operationKey, Optional.of(payload))`, run the auto-restore, set the segment state, AND reply with `RestoreInitStateResponse{segment_id, terminal_seq}` matching the highest seq from the change log

#### Scenario: CompactSegment handler clones state under sticky CAS

- **WHEN** the agent ships `CompactSegment{segment_id}` to a Java SDK
- **THEN** the SDK SHALL acquire the segment's sticky CAS (existing primitive), deep-copy the state, release the CAS, serialize with header, ship via `NativeTransport.sendSegmentStateSnapshot`, AND reply `CompactSegmentResponse{segment_id, snapshot_seq, snapshot_bytes_size}`

#### Scenario: SegmentMailboxStatus :full pauses producer

- **WHEN** the agent ships `SegmentMailboxStatus{segment_id, status: :full}` to a Java SDK
- **THEN** the SDK's `SegmentMailboxStatusHandler` SHALL update the per-Segment observer to "full"; subsequent `ctx.emit(...)` calls on channels feeding this Segment SHALL apply the channel's `OverflowPolicy` (Block parks, DropNewest discards, DropOldest discards-oldest)

### Requirement: Public Java SDK handler classes use canonical Operative vocabulary

The Java SDK SHALL expose handler classes whose names use canonical vocabulary: `AddOperativeHandler`, `RemOperativeHandler`, `OperationMessageHandler`, `SegmentStateHandler`, `SegmentApplyStateHandler`. Classes previously named `AddProcessorHandler`, `RemProcessorHandler`, `ProcessorMessageHandler`, `ProcessorStateHandler`, `ProcessorApplyStateHandler` SHALL NOT be present in the published API. Java packages SHALL likewise migrate any package-path components that contained `processor`, `operator`, `agent`, or `compute_engine` to the canonical replacements (`operative`, `executor`, `controller`). `FleetManager`-equivalent classes keep their names.

#### Scenario: Old handler classes do not exist
- **WHEN** a downstream Java consumer imports `com.flezha.sdk.handlers.AddProcessorHandler`
- **THEN** the import MUST fail to compile (class not found); the consumer MUST migrate to `com.flezha.sdk.handlers.AddOperativeHandler`

#### Scenario: New handlers wire into dispatch
- **WHEN** a Java SDK Executor receives an `AddOperative` protobuf command from the Controller
- **THEN** dispatch MUST route the message to `AddOperativeHandler.handle(...)` with the protobuf `operative_id` field exposed under the same name (`getOperativeId()`)

### Requirement: Public Operative / Operation / Segment user-facing API names

The Java SDK SHALL expose user-extensible base classes/interfaces under the canonical names `ContinuousOperative`, `Operation`, `Segment`, and `Channel`. The Executor process startup hook SHALL be `Executor.start(Config)` (or equivalent factory entry point); class names referring to the SDK process itself SHALL use `Executor` as the role term, not `ComputeEngine` or `Operator`.

#### Scenario: User extends ContinuousOperative
- **WHEN** a user-defined Operative class declares `extends ContinuousOperative`
- **THEN** the class MUST compile against the published Java SDK; declaring `extends Processor` MUST fail
