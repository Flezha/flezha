# java-sdk Specification

## Purpose

Defines the Java SDK for Flezha compute engines: the public API, protocol layer, command dispatch, transport selection, reconnection, the Operative / Operation / Segment runtime, and the JNI bridge to `flz_transport`. Conforms to `docs/sdk_protocol_spec.md`.
## Requirements
### Requirement: Java SDK Entry Point

The Java SDK SHALL provide `com.flezha.sdk.Flezha.createRunner(String namespace)` as the single entry point, returning a concrete `FlezhaRunner` directly — there SHALL be no separate `RunnerBuilder` type. Creating a runner SHALL NOT connect, claim the process singleton slot, or spawn any thread. Calling `.start()` on the runner MUST connect to the controller's control socket, send the handshake init message per `docs/sdk_protocol_spec.md` §5.2 carrying the runner's currently-declared factory ids (possibly empty), wait for `AssignRunnerId`, and return `this` for chaining. The started runner SHALL expose `getNamespace()` (replacing `getId()`) and `getOperatives()` (a snapshot of currently-spawned Operative instances keyed by what the wire calls `operative_id`). `.run()` SHALL be sugar for `.start()` followed by `awaitTermination()`, never an independent implementation.

#### Scenario: Successful connection on a created runner
- **WHEN** an SDK user calls `Flezha.createRunner("my_app").defineUnbounded(MarketOperative.class).start()` with the controller running
- **THEN** the call blocks until the controller responds with `AssignRunnerId`, returns the same `FlezhaRunner`, and `getNamespace()` returns `"my_app"`

#### Scenario: Controller unavailable triggers exponential backoff retries
- **WHEN** an SDK user calls `.start()` on a runner and the controller's control socket is not yet listening
- **THEN** the SDK SHALL retry the connect with exponential backoff starting at 200 ms and capped at 30 s, and succeed once the controller becomes available

#### Scenario: start() with zero defined operative types is legal
- **WHEN** `.start()` is called on a `FlezhaRunner` with no `.defineUnbounded(...)`/`.defineBounded(...)` calls and no auto-wired classes on the classpath
- **THEN** the runner SHALL connect successfully with an empty `factory_ids` list in the handshake init message, and operative types MAY be defined later on the live runner

#### Scenario: RunnerBuilder is not importable
- **WHEN** user code references `com.flezha.sdk.RunnerBuilder`, `com.flezha.sdk.operative.UnboundedOperativeRegistry`, `com.flezha.sdk.bounded.BoundedOperativeRegistry`, or `Flezha.newRunner`
- **THEN** compilation SHALL fail — the builder type, both public registry facades, and the old entry-point name are deleted with no shim

### Requirement: Operation Interface and Per-Segment Dispatch

The Java SDK SHALL expose a generic `Operation<Input, State, Output, Change, Clock, Config>` interface in `com.flezha.sdk.operation`. Implementers SHALL provide `name()`, `inputType() → Class<Input>`, `outputType() → Class<Output>`, `initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) → State`, and `run(Input, StateHandle<State, Change>, OperationCtx) throws OperationException`. The default-method overrides `operationKeys()`, `segmentFor(Input)`, `segmentForOverridden()`, `operationKey(Input)`, and `operationKeyOverridden()` declare the segmentation mode (single by default; static when `operationKeys()` non-empty + `segmentForOverridden()` true; dynamic when `operationKeyOverridden()` true with empty `operationKeys()`). The runtime SHALL invoke `run` on a per-segment basis with sticky ordering (see the Sticky Per-Segment Ordering requirement) and SHALL pass each input to all segments returned by `segmentFor(input)` for static-mode operations (single-mode operations skip the call and route to segment index 0).

The `OperationCtx` parameter SHALL provide:

- `emit(value)` for inter-operation or plugin-bound outputs, `emit(channel, value)` for explicit channel selection, and `emitWithMeta(value, meta)` / `emitWithMeta(channel, value, meta)` for plugin-bound outputs with user-attached metadata. `emitWithMeta` on inter-operation channels SHALL throw `OperationException.MetadataNotAllowedOnInterOperationChannel` naming the channel.
- `emitKeyed(key, value)` and `emitKeyedWithMeta(key, value, meta)` for channels declared `.keyed()`. Emitting **without** a key on a `.keyed()` channel SHALL throw `OperationException.KeyRequiredOnKeyedChannel`; emitting **with** a key on a channel not declared `.keyed()` SHALL throw `OperationException.KeyNotAllowedOnNonKeyedChannel`. Both name the channel.
- `long now()` — the invocation's time value, computed **once** by the runtime from the Operation's declared `Clock` before `run` executes, and returning that same value for every call within the invocation. This is the determinism-contract replacement for `System.currentTimeMillis()`.
- `StableId stableId()` — a deterministic, replay-stable 128-bit identifier derived from the Segment identity, the invocation's durable change seq, and a per-invocation call ordinal. Repeated calls within one invocation differ; the n-th call of a replayed invocation SHALL equal the n-th call of the original. This is the determinism-contract replacement for `UUID.randomUUID()`.
- `void checkpoint()` — see the checkpoint ack-granularity requirement.

The runtime SHALL stamp `flz.segment`, `flz.seq`, and (on keyed emits) `flz.key` onto plugin-bound frames. User-supplied metadata keys in the reserved `flz.` namespace SHALL be **rejected** with `OperationException.ReservedMetadataKey` naming the key, so that identity stamps can neither be spoofed nor clobbered.

#### Scenario: Single-mode Operation runs on a single segment
- **WHEN** an Operation whose `operationKeys()` returns an empty list is registered as the consumer of an entry channel, and one input arrives on that channel
- **THEN** the SDK SHALL invoke the Operation's `run` once on the single allocated segment (segment index 0), with `state` argument equal to the value `initState(Optional.empty(), Optional.empty())` returned at operative startup

#### Scenario: Static-mode Operation creates one segment per operationKey
- **WHEN** an Operation's `operationKeys()` returns `["1m", "1h", "1d", "1w"]` and `segmentForOverridden()` returns `true`
- **THEN** at operative startup the SDK SHALL allocate exactly four `SegmentSlot` instances for that Operation, each constructed with `initState(Optional.of(key), Optional.empty())` for its corresponding key

#### Scenario: Static-mode segmentFor fan-out enqueues input on each named segment
- **WHEN** an Operation's `segmentFor(input)` returns `["1m", "1h"]` for a given input
- **THEN** the SDK SHALL enqueue that input on the segments identified by `"1m"` and `"1h"` (each receives its own `run` invocation), and SHALL NOT enqueue it on `"1d"` or `"1w"`

#### Scenario: Run throwing OperationException logs and continues
- **WHEN** `Operation.run(...)` throws `OperationException` on a given input and the input channel's retry budget is exhausted
- **THEN** the SDK SHALL log the exception with `(operativeInstanceId, operationName, segmentKey)` context, SHALL discard any partial `ctx.emit(...)` calls made during this invocation, SHALL park the payload to the dead-letter queue, and SHALL continue dispatching subsequent inputs on the same segment

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
- **THEN** the SDK SHALL serialize `candle` via the channel's serializer, frame the bytes with `metadata` containing `"channel" -> "ohlc.candles"` plus the runtime's `flz.segment` and `flz.seq` identity stamps, and send the frame on the operative's secondary stream

#### Scenario: Plugin-bound emitWithMeta appends user metadata after the channel key
- **WHEN** an Operation calls `ctx.emitWithMeta(candle, Map.of("trace_id", "abc"))` on a plugin-bound channel `"ohlc.candles"`
- **THEN** the SDK SHALL frame the bytes with `metadata` containing `"channel" -> "ohlc.candles"`, `"trace_id" -> "abc"`, and the runtime identity stamps

#### Scenario: Reserved flz. metadata key in user map is rejected
- **WHEN** an Operation calls `ctx.emitWithMeta(candle, Map.of("flz.key", "spoofed"))` on a plugin-bound channel
- **THEN** the SDK SHALL throw `OperationException.ReservedMetadataKey` naming `"flz.key"`, and SHALL NOT emit the frame

#### Scenario: now() is constant within one invocation
- **WHEN** an Operation's `run` calls `ctx.now()` three times
- **THEN** all three calls SHALL return the identical value — the one the runtime computed from the declared `Clock` before entering `run`

#### Scenario: stableId() is replay-stable and ordinal-distinct
- **WHEN** an Operation's `run` calls `ctx.stableId()` twice for a given input, and the same input is later replayed at the same durable change seq
- **THEN** the two calls within one invocation SHALL return different ids, AND the n-th call of the replayed invocation SHALL return an id byte-equal to the n-th call of the original

#### Scenario: stableId() is byte-identical to the Rust SDK
- **WHEN** the Java SDK derives a `StableId` from segment identity `s`, change seq `q`, and ordinal `n`
- **THEN** the resulting 128-bit value SHALL equal the Rust SDK's `StableId::derive(s, q, n)` (FNV-1a-128 over `s ‖ big-endian q ‖ big-endian n`), and its 32-hex-digit rendering SHALL match Rust's `{:032x}`

#### Scenario: emitKeyed on a non-keyed channel is rejected
- **WHEN** an Operation calls `ctx.emitKeyed("BTC", candle)` on a channel not declared `.keyed()`
- **THEN** the SDK SHALL throw `OperationException.KeyNotAllowedOnNonKeyedChannel` naming the channel

#### Scenario: Keyed emit stamps flz.key on the wire
- **WHEN** an Operation calls `ctx.emitKeyed("BTC", candle)` on a channel declared `.keyed()`
- **THEN** the plugin frame's metadata SHALL carry `flz.key -> "BTC"`, `flz.segment -> "<instance>/<operation>/<segmentKey>"`, and `flz.seq -> "<changeSeq>.<emitOrdinal>"`

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

The Java SDK SHALL implement handlers for every `CommandRequest` variant defined in `runner_commands.proto` that the Rust SDK handles, including `AssignRunnerId`, `ConnectSocket`, `AddOperative`, `RemOperative`, `SegmentState`, `MigrationSerialization`, `MigrationDeserialization`, `OperationMessage`, `RunnerDisconnect`, `RestoreInitState`, `CompactSegment`, `SegmentMailboxStatus`, `Ping`, `BoundedStop`, `CreditWindowInit`, `CreditGrant`, `ParkPayloadAck`, `OpCommitResponse` (tag 29), and `IterCommitResponse` (tag 31). Each handler MUST produce a `CommandResponse` that echoes the original `reference_id` and sets `status = "success"` or `status = "error"` with populated `errors` per `docs/sdk_protocol_spec.md` §6.

The SDK SHALL originate the runner→controller commands `RunnerShutdownNotification`, `BoundedCompleted`, `OpCommit` (tag 28), `IterCommit` (tag 30), and `IterPark` (tag 32).

`AddOperative` SHALL read the optional `kind` field: an empty or absent value SHALL be interpreted as `:unbounded`; a value resolving to `:bounded` SHALL spawn a `BoundedOperativeInstance`. `RemOperative` SHALL remove either variant by instance id.

**`AddOperative` SHALL respond with a populated `AddOperativeResponse` whose `metadata` map declares the registered class's capabilities**, so that the controller's `Overwatch.decode_declarations/1` learns what the class supports. The map SHALL carry, when and only when the class declares them: `effectively_once`, `ack_granularity` (`"checkpoint"`), `correction_window_ms`, `beyond_window_policy`, `compensable_ops` (comma-separated op indices), and `leaf_ops`. A class that declares none of these SHALL respond with an empty map, which the controller reads as "declared nothing" — the pre-change behaviour.

#### Scenario: AddOperative spawns an unbounded operative instance
- **WHEN** the agent sends `AddOperative{operative_id:"p1", factory_id:"market"}` and UnboundedOperative class `MarketOperative` is registered with `factoryId() == "market"`
- **THEN** the SDK SHALL allocate an `UnboundedOperativeInstance` keyed by `"p1"`, call `initState(...)` for every Operation×Segment declared by `MarketOperative`, and respond with `status = "success"`

#### Scenario: AddOperative with unknown factory_id returns error
- **WHEN** the agent sends `AddOperative{factory_id:"missing"}`
- **THEN** the SDK SHALL respond with `status = "error"` and `errors` containing a string naming the missing factory id

#### Scenario: AddOperative with kind bounded spawns a bounded variant
- **WHEN** the agent sends `AddOperative{operative_id:"t1", factory_id:"greet", kind:"bounded"}` and `GreetOperative` is registered as a bounded operative with `factoryId() == "greet"`
- **THEN** the SDK SHALL register a `BoundedOperativeInstance` keyed by `"t1"` and respond with `status = "success"`

#### Scenario: RemOperative removes either operative variant
- **WHEN** the agent sends `RemOperative{operative_id:"t1"}` for a live bounded operative instance (and likewise for an unbounded operative instance)
- **THEN** the SDK SHALL remove the instance from the sealed operative registry by id and respond with `status = "success"`

#### Scenario: AddOperative declaring the effectively-once tier populates the response metadata
- **WHEN** the agent sends `AddOperative{factory_id:"tier"}` for a class whose channel declaration calls `c.effectivelyOnce()`
- **THEN** the SDK's `CommandResponse.raw_data` SHALL decode as an `AddOperativeResponse` whose `metadata` contains `"effectively_once" -> "true"`

#### Scenario: AddOperative declaring a correction window populates window and policy
- **WHEN** a registered class declares `c.correctionWindow(60_000)` and `c.beyondWindowPolicy(DROP)`
- **THEN** the response `metadata` SHALL contain `"correction_window_ms" -> "60000"` and `"beyond_window_policy" -> "drop"`

#### Scenario: AddOperative declaring checkpoint ack granularity populates ack_granularity
- **WHEN** a registered class declares `c.effectivelyOnce()` and `c.checkpointAck()`
- **THEN** the response `metadata` SHALL contain `"ack_granularity" -> "checkpoint"`

#### Scenario: AddOperative for a bounded class declares its compensable ops and leaves
- **WHEN** a bounded class registers a chain whose ops at index 0 and 2 override `compensate` and whose op tree has leaves `[2]`
- **THEN** the response `metadata` SHALL contain `"compensable_ops" -> "0,2"` and `"leaf_ops" -> "2"`

#### Scenario: A class declaring nothing responds with an empty metadata map
- **WHEN** a registered class declares no tier, no correction window, no checkpoint ack, and is unbounded
- **THEN** the response `metadata` SHALL be empty, and the controller SHALL treat the class exactly as it did before this change

#### Scenario: RunnerDisconnect halts the reconnect loop
- **WHEN** the agent sends `RunnerDisconnect{}`
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

The Java SDK SHALL use `protobuf-java` to encode `CommandResponse` and decode `CommandRequest` messages generated from the existing `runner_commands.proto` (no new proto definitions, no hand-written wire-format code).

#### Scenario: Encoding AssignRunnerId response matches protocol
- **WHEN** the SDK encodes `CommandResponse{reference_id:"#Ref<0.1.2.3>", status:"success"}`
- **THEN** the produced bytes SHALL be valid protobuf decodable by the Elixir agent's `CommandResponse` parser

#### Scenario: Decoding AddOperative request extracts all fields
- **WHEN** the SDK receives a protobuf-encoded `CommandRequest` with an `AddOperative` oneof case
- **THEN** the decoder SHALL expose `operative_id`, `factory_id`, and `init_values` as Java strings

### Requirement: Transport Selection

The Java SDK SHALL honor the `FLEZHA_TRANSPORT` environment variable (`uds` or `quic`) for transport selection, defaulting to `uds` when unset. For QUIC, the SDK SHALL also honor `FLEZHA_QUIC_ADDRESS`, `FLEZHA_CA_CERT_PATH`, `FLEZHA_SERVER_CERT_PATH`, `FLEZHA_SERVER_KEY_PATH`. For UDS, the SDK SHALL honor `FLEZHA_SOCKET_FILES_PATH`. All env var names and defaults MUST match `docs/sdk_protocol_spec.md` §2.

#### Scenario: Default transport is UDS
- **WHEN** `FLEZHA_TRANSPORT` is unset and `Flezha.newInstance(...)` is called
- **THEN** the SDK SHALL connect to `{FLEZHA_SOCKET_FILES_PATH or app_data/sockets}/runner_control_channel.sock`

#### Scenario: QUIC transport uses configured address and cert paths
- **WHEN** `FLEZHA_TRANSPORT=quic` and `FLEZHA_QUIC_ADDRESS=127.0.0.1:4433`
- **THEN** the SDK SHALL open a QUIC connection to that address using the CA cert at the configured path, opening the control channel on the first bidi stream per §2 "QUIC Stream Mapping"

### Requirement: Auto-Reconnect on Unexpected Disconnect

The Java SDK SHALL distinguish intentional disconnects (`RunnerDisconnect` command handled → status `Stopped`) from unexpected ones (BrokenPipe / UnexpectedEof / ConnectionReset / QUIC connection lost). On an unexpected disconnect the SDK MUST clear `io_streams`, the live operative registry, and work queues; reset `runner_id` to `undefined`; and reconnect with exponential backoff (200 ms, 400 ms, 800 ms, …, capped at 30 s) per `docs/sdk_protocol_spec.md` §5 "Auto-Reconnection".

#### Scenario: Agent crash triggers reconnect
- **WHEN** the agent process is killed while the SDK has status `Running`
- **THEN** the SDK SHALL detect the disconnect via the native layer, clear local state, and reconnect once the agent is available again, receiving a fresh `runner_id`

#### Scenario: Intentional disconnect does not reconnect
- **WHEN** the SDK receives `RunnerDisconnect{}` and responds successfully
- **THEN** the SDK SHALL close its sockets and NOT attempt any reconnection

### Requirement: Worker Pool and Dispatch Throughput Floor

The Java SDK SHALL operate on a single shared bounded runner sized by `FLEZHA_WORKER_POOL_SIZE` (default = `Runtime.availableOperatives()`), draining all `SegmentSlot` mailboxes work-steal-style. The SDK SHALL sustain at least the throughput floor (in messages/second) measured against the v1 implementation at merge time on the regression bench (`100 operatives × 1 operation × 1 segment × 10K msg`, no-op Operation, UDS transport, default worker-pool size; see `operative-mvp-java/design.md` Decision 7) invoked via `mix bench --runtime java --shape regression -n 100 -m 10000`. The bench-regression-java CI job is informational in v1; the literal floor value is recorded in this requirement once `mix bench --runtime java` has been measured 3× on the reference host (median, rounded down to a clean number) — until then the recorded floor is "TBD pending v1 measurement".

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

The Java SDK's `Operation` interface SHALL declare `Clock extends com.flezha.sdk.state.Clock` as a generic parameter, defaulting to `com.flezha.sdk.state.WallClock`. `Clock` SHALL be a sealed Java interface with `permits WallClock, LogicalClock, EventTimeFrom`. The runtime SHALL evaluate the declared clock **once per invocation**, before entering `run`, and serve that single value from `ctx.now()`:

- `WallClock` — epoch milliseconds at dispatch entry.
- `EventTimeFrom` — the event time extracted from the current input. Because Java's erasure precludes Rust's compile-enforced `TimeExtractor<Input>` bound, an Operation declaring `EventTimeFrom` SHALL override `default OptionalLong eventTime(Input input)`; failing to do so SHALL fail Operative registration with `OperativeConfigurationException` naming the Operation and the missing override.
- `LogicalClock` — the raw durable `change_seq`, re-armed to `terminal + 1` on restore. An Operation declaring `LogicalClock` with `Change = NoChange` SHALL fail registration with `OperativeConfigurationException` carrying the Rust error name `UnservableClock`, since there is no durable seq to serve.

`OperativeConfigurationException::ClockNotImplementedYet` SHALL NOT be thrown for any member of the sealed hierarchy.

#### Scenario: Default Clock is WallClock

- **WHEN** a Java Operation does not declare its `Clock` parameter
- **THEN** `Operation.clockType() == WallClock.class` AND Operative registration SHALL succeed AND `ctx.now()` SHALL return epoch milliseconds sampled once at dispatch entry

#### Scenario: EventTimeFrom serves the extracted input time

- **WHEN** a Java Operation declares `Clock = EventTimeFrom` and overrides `eventTime(input)` to return the input's timestamp field
- **THEN** registration SHALL succeed AND `ctx.now()` SHALL return that extracted timestamp for the invocation

#### Scenario: EventTimeFrom without an eventTime override fails registration

- **WHEN** a Java Operation declares `Clock = EventTimeFrom` but does not override `eventTime(Input)`
- **THEN** `registerUnbounded(...)` SHALL throw `OperativeConfigurationException` naming the Operation and the missing `eventTime` override

#### Scenario: LogicalClock serves the durable change seq and re-arms on restore

- **WHEN** a Java Operation declares `Clock = LogicalClock` and a non-`NoChange` Change, and the Segment is restored from a change log whose terminal seq is 41
- **THEN** registration SHALL succeed AND the first post-restore invocation's `ctx.now()` SHALL return `42`

#### Scenario: LogicalClock with NoChange fails registration

- **WHEN** a Java Operation declares `Clock = LogicalClock` and `Change = NoChange`
- **THEN** `registerUnbounded(...)` SHALL throw `OperativeConfigurationException` whose message carries the error name `UnservableClock`

### Requirement: Operative declares persistenceEnabled default-method, frozen at instance start

The Java SDK's `UnboundedOperative` interface SHALL declare `default boolean persistenceEnabled() { return true; }`. The runtime SHALL resolve a per-Operative-instance `persistenceEnabled: boolean` at the moment `AddOperative` is processed using:

1. If the `AddOperative` metadata carries `operative_persistence.<OperativeClass>` with a boolean value (runner override), use that.
2. Otherwise, use the value of `UnboundedOperative.persistenceEnabled()` (the Operative-class default).
3. If neither is overridden, the result is `true`.

The resolved value SHALL be bound to the `UnboundedOperativeInstance` and SHALL be propagated to every `SegmentSlot` constructed for that instance. Subsequent changes to the runner-side metadata or to the SDK's `UnboundedOperative.persistenceEnabled()` impl (via SDK redeploy) SHALL NOT affect already-running instances.

#### Scenario: Default-on with no override

- **WHEN** an Operative's `persistenceEnabled()` is the default (returns `true`) AND no runner override is present
- **THEN** `UnboundedOperativeInstance.persistenceEnabled()` SHALL return `true`

#### Scenario: Code-declared opt-out is honored

- **WHEN** `MarketOperative.persistenceEnabled()` returns `false` (user override) AND no runner override is present
- **THEN** `UnboundedOperativeInstance.persistenceEnabled()` SHALL return `false`; the `SegmentFlusher` SHALL drain-and-discard for this instance's Segments; `NativeTransport.sendSegmentStateChange` SHALL NOT be invoked

#### Scenario: Runner override takes precedence

- **WHEN** `MarketOperative.persistenceEnabled()` returns `true` AND `AddOperative` metadata sets `operative_persistence.MarketOperative = false`
- **THEN** `UnboundedOperativeInstance.persistenceEnabled()` SHALL return `false`

#### Scenario: Resolved value frozen at instance start

- **WHEN** an `UnboundedOperativeInstance` is constructed with resolved `persistenceEnabled = true`, AND a subsequent runner config change sets `operative_persistence.<OperativeClass> = false`
- **THEN** the running `UnboundedOperativeInstance` SHALL continue with `persistenceEnabled = true`; the new value SHALL take effect only on the next `stop_operative` + `start_operative` cycle for that instance

### Requirement: initState accepts optional restoration payload

The Java SDK's `Operation.initState` signature SHALL be `State initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload)`. When `initPayload.isEmpty()`, `initState` SHALL behave as a fresh-start constructor. When `initPayload.isPresent()`, the user MAY inspect or ignore the payload bytes; the runtime SHALL automatically apply the payload AFTER `initState` returns:

1. If `payload.snapshotBytes().length > 0`, deserialize via `KryoCodec` (after header validation) and replace the user-constructed state.
2. If `payload.changeLogBytes().length > 0`, decode each WAL record (frame format from `operative-state-persistence`), validate header, deserialize Change via `KryoCodec`, and invoke `Operation.applyChange(state, change)` in seq order.

#### Scenario: Cold start passes Optional.empty()

- **WHEN** a Java Segment is constructed for the first time with no prior persisted state on the agent
- **THEN** the runtime SHALL invoke `initState(operationKey, Optional.empty())`

#### Scenario: Restart passes Optional.of(payload)

- **WHEN** a Java Segment is reconnected and the agent has snapshot or WAL bytes for it
- **THEN** the runtime SHALL invoke `initState(operationKey, Optional.of(payload))` AND, after `initState` returns, the runtime SHALL deserialize the snapshot (if any) and fold the change log via `applyChange` in seq order

### Requirement: Per-Segment flusher with toggle-aware drain

The Java SDK SHALL spawn at most one `SegmentFlusher` daemon thread per JVM, lazy-initialized via `AtomicReference<SegmentFlusher>` on the first `Flezha.registerUnbounded(...)` call that registers an Operation with a non-`NoChange` Change. JVMs that register only `NoChange` Operations SHALL NOT spawn a flusher thread.

The flusher SHALL run an infinite loop sleeping `linger_ms` (default 5; configurable via system property `flezha.flush.linger.ms` with env-var fallback `FLEZHA_FLUSH_LINGER_MS`) between iterations. Per iteration: iterate the registered Segments; for each, acquire its `bufferLock`; call `drainToFrames` (applying coalescing per `operative-state-persistence-rust/design.md` Decision 3); release the lock; for each frame, invoke `sendFrame` if `persistenceEnabled = true`, OR discard if `false`.

#### Scenario: NoChange-only program does not spawn flusher

- **WHEN** a Java program calls `Flezha.registerUnbounded(MyOperative.class)` for Operatives whose Operations all have `Change = NoChange`
- **THEN** no thread named `flezha-flusher` SHALL be spawned (verifiable via `Thread.getAllStackTraces().keySet().stream().anyMatch(t -> "flezha-flusher".equals(t.getName())) == false`)

#### Scenario: First non-NoChange registration starts the flusher

- **WHEN** a Java program calls `Flezha.registerUnbounded(...)` for an Operative whose Operation has `Change = MyChange`
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

The Java SDK SHALL register command handlers for `RestoreInitState`, `CompactSegment`, and `SegmentMailboxStatus` per `operative-state-persistence`'s wire-protocol additions:

- `RestoreInitStateHandler`: receives the request, decodes payload, invokes `Operation.initState(operationKey, Optional.of(payload))`, runs the runtime auto-restore (snapshot deserialize + change-log fold), sets the segment state, replies `RestoreInitStateResponse{segment_id, terminal_seq}`.
- `CompactSegmentHandler`: receives the request, acquires the segment's sticky CAS, deep-copies state via Kryo, releases CAS, serializes with header stamping, ships via `NativeTransport.sendSegmentStateSnapshot`, replies `CompactSegmentResponse{segment_id, snapshot_seq, snapshot_bytes_size}`.
- `SegmentMailboxStatusHandler`: receives the request, updates the per-Segment mailbox observer state used by the existing `OverflowPolicy` machinery (`operative-bounded-mailbox`).

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

The Java SDK SHALL expose handler classes whose names use canonical vocabulary: `AddOperativeHandler`, `RemOperativeHandler`, `OperationMessageHandler`, `SegmentStateHandler`, `SegmentApplyStateHandler`. Classes previously named `AddProcessorHandler`, `RemProcessorHandler`, `ProcessorMessageHandler`, `ProcessorStateHandler`, `ProcessorApplyStateHandler` SHALL NOT be present in the published API. Java packages SHALL likewise migrate any package-path components that contained `processor`, `operator`, `agent`, or `compute_engine` to the canonical replacements (`operative`, `runner`, `controller`). `FleetManager`-equivalent classes keep their names.

#### Scenario: Old handler classes do not exist
- **WHEN** a downstream Java consumer imports `com.flezha.sdk.handlers.AddProcessorHandler`
- **THEN** the import MUST fail to compile (class not found); the consumer MUST migrate to `com.flezha.sdk.handlers.AddOperativeHandler`

#### Scenario: New handlers wire into dispatch
- **WHEN** a Java SDK Runner receives an `AddOperative` protobuf command from the Controller
- **THEN** dispatch MUST route the message to `AddOperativeHandler.handle(...)` with the protobuf `operative_id` field exposed under the same name (`getOperativeId()`)

### Requirement: Public Operative / Operation / Segment user-facing API names

The Java SDK SHALL expose user-extensible base classes/interfaces under the canonical names `UnboundedOperative`, `Operation`, `Segment`, and `Channel`. The Runner process startup hook SHALL be `Runner.start(Config)` (or equivalent factory entry point); class names referring to the SDK process itself SHALL use `Runner` as the role term, not `ComputeEngine` or `Operator`.

#### Scenario: User extends UnboundedOperative
- **WHEN** a user-defined Operative class declares `extends UnboundedOperative`
- **THEN** the class MUST compile against the published Java SDK; declaring `extends Processor` MUST fail

### Requirement: Java SDK implements the effectively-once output tier

The Java SDK SHALL implement the `effectively-once-output` capability. A channel declaration SHALL support `c.effectivelyOnce()`; the runtime SHALL then, for each invocation, buffer the invocation's state changes, plugin outputs, consumed input offset, and (on a dead-letter park) its DLQ entry into ONE `segment_effect_record` frame rather than separate change and output frames.

The frame SHALL carry the 16-byte state header with `format_id = 2` and `sdk_lang = 2`, and its body SHALL be the postcard encoding of `EffectRecordBody { changes: Vec<Vec<u8>>, outputs: Vec<OutputRecord>, consumed_offset: Option<u64>, dlq: Option<DlqEntryBody>, event_time: u64 }` in exactly that field order, byte-identical to the Rust SDK. The `changes` elements SHALL be the SDK's own change bytes (Kryo, as a Java plain change record carries) — the controller stores and re-ships them without decoding.

The runtime SHALL read the inbound `flz.offset` metadata key to learn the input's offset, and SHALL re-forward replayed inputs on restore. A dead-letter park under the tier SHALL ride inside the effect record rather than emitting a separate `park_payload` frame, so that the park and the consumed-offset advance are one atomic record.

Declaring `.keyed()` SHALL be required on every plugin-bound channel of a tier-enabled class; a non-keyed plugin channel SHALL fail registration.

#### Scenario: A tier invocation ships one effect record, not separate frames

- **WHEN** a tier-enabled Operation's `run` applies two Changes and emits one keyed output for an input carrying `flz.offset = 7`
- **THEN** the SDK SHALL ship exactly one `segment_effect_record` frame whose decoded body has `changes.length == 2`, `outputs.length == 1`, and `consumed_offset == 7`; and SHALL NOT ship any `segment_state_change` frame for that invocation

#### Scenario: Effect-record bytes are byte-identical to the Rust encoder

- **WHEN** the Java SDK encodes an `EffectRecordBody` with the same field values as a Rust fixture
- **THEN** the produced bytes SHALL equal the Rust `postcard::to_allocvec(EffectRecordBody)` fixture bytes exactly

#### Scenario: A poisoned tier invocation parks inside the effect record

- **WHEN** a tier-enabled Operation exhausts its retry budget on a poison input at offset 9
- **THEN** the SDK SHALL ship one `segment_effect_record` whose body carries `dlq = Some(entry)` AND `consumed_offset == 9`, and SHALL NOT ship a separate `park_payload` frame

#### Scenario: A tier class with a non-keyed plugin channel fails registration

- **WHEN** a class declares `c.effectivelyOnce()` and a plugin-bound channel without `.keyed()`
- **THEN** registration SHALL throw `OperativeConfigurationException` naming the channel

### Requirement: Java SDK implements checkpoint ack granularity

The Java SDK SHALL implement the `effectively-once-checkpoint-granularity` capability. A channel declaration SHALL support `c.checkpointAck()`, valid only together with `c.effectivelyOnce()` — declaring it alone SHALL fail registration with `CheckpointAckWithoutTier`. `OperationCtx.checkpoint()` SHALL mark the invocation as closing the current interval.

The per-Segment flush buffer SHALL carry a third `staged` list holding the open interval's changes. `drainToFrames` and `isEmpty` SHALL deliberately **ignore** `staged` — an open interval is not flushable, and draining it alone would put state ahead of its offset. An invocation SHALL emit an effect record carrying `{staged, consumedOffset}` when it checkpointed or parked to the DLQ; otherwise it SHALL emit `{[], outputs, null}` only if it produced outputs. A dead-letter park SHALL close the interval. A restore SHALL discard the open interval.

#### Scenario: An open interval holds the consumed offset

- **WHEN** a checkpoint-ack Operation processes three inputs without calling `ctx.checkpoint()`
- **THEN** no effect record carrying a non-null `consumed_offset` SHALL be shipped, and the staged changes SHALL remain unflushed

#### Scenario: checkpoint() closes the interval and acknowledges the offset

- **WHEN** the Operation's fourth invocation calls `ctx.checkpoint()` for an input at offset 3
- **THEN** the SDK SHALL ship one effect record whose `changes` are the four invocations' staged changes and whose `consumed_offset == 3`

#### Scenario: checkpointAck without the tier is refused

- **WHEN** a class declares `c.checkpointAck()` without `c.effectivelyOnce()`
- **THEN** registration SHALL throw `OperativeConfigurationException` carrying the error name `CheckpointAckWithoutTier`

#### Scenario: A park closes the open interval

- **WHEN** a checkpoint-ack Operation has staged two invocations' changes and the third input is poison and exhausts its retries
- **THEN** the shipped effect record SHALL carry the staged changes, the DLQ entry, and the poison input's `consumed_offset` — so the replay resumes after the poison offset

#### Scenario: A restore discards the open interval

- **WHEN** a Segment with a non-empty `staged` list is restored via `RestoreInitState`
- **THEN** the staged list SHALL be cleared, since it belongs to a past that no longer exists

### Requirement: Java SDK implements unbounded late-input correction

The Java SDK SHALL implement the `unbounded-correction` capability. A channel declaration SHALL support `c.correctionWindow(millis)` and `c.beyondWindowPolicy(policy)` where policy is `PROCESS_AS_ARRIVAL` (the default, today's behaviour) or `DROP`. Declaring a correction window SHALL require the effectively-once tier, an `EventTimeFrom` clock, and all channels keyed; violating any of these SHALL fail registration.

Every effect record SHALL carry the invocation's `event_time`. The runtime SHALL maintain a per-Segment event-time high-water mark, and SHALL classify an input whose event time is below the high-water by more than the declared window as beyond-window. A replay slice SHALL be recognised via the `flz.replay` metadata key and terminated by a payload-less `flz.replay_end` **broadcast** frame (intercepted before channel lookup, since an input routes to only one segment by key); the buffered slice SHALL be dispatched in **event-time order**, and a closed slice SHALL flush before any live input popped behind it.

A replay SHALL NOT lower the event-time high-water mark.

#### Scenario: A replayed slice dispatches in event-time order

- **WHEN** a replay slice delivers inputs with event times `[30, 10, 20]` followed by an `flz.replay_end` broadcast
- **THEN** the Operation's `run` SHALL be invoked for them in event-time order `10, 20, 30`

#### Scenario: A closed slice flushes before a live input behind it

- **WHEN** a live input is popped from the mailbox behind a just-closed replay slice
- **THEN** the slice SHALL dispatch in full before the live input, so the live input cannot jump ahead of the inputs being re-ordered

#### Scenario: A replay does not lower the event-time high-water

- **WHEN** a Segment restored to an event-time high-water of `500` replays an unconsumed tail whose maximum event time is `300`
- **THEN** the high-water SHALL remain `500`, and a subsequent input at event time `320` SHALL still be classified as late

#### Scenario: A correction window without the tier is refused

- **WHEN** a class declares `c.correctionWindow(60_000)` without `c.effectivelyOnce()`
- **THEN** registration SHALL throw `OperativeConfigurationException` naming the missing tier declaration

#### Scenario: Beyond-window default is process-as-arrival

- **WHEN** a class declares `c.correctionWindow(60_000)` without declaring a policy, and an input arrives whose event time is older than the window
- **THEN** the input SHALL be processed as an arrival (not dropped), since declaring a window must never silently start dropping data

### Requirement: Java SDK implements the bounded op-journal and OpCommit handshake

The Java SDK SHALL implement the `bounded-op-journal` capability. At every op barrier of a durable structured bounded class, the SDK SHALL ship `OpCommit` (tag 28) carrying the ops that ran and the channel-tagged handoff blobs, and SHALL block on the controller's `OpCommitResponse` (tag 29) under the same `reference_id` for up to 30 seconds. A failure or timeout SHALL produce a terminal error and trigger compensation.

Inter-op channels of a durable structured bounded class SHALL declare a codec; a missing codec SHALL fail registration with `MissingInterOpCodec` naming the channel. Classes that opt out of persistence SHALL skip the journal and the handshake entirely, preserving re-run-from-top.

On resume, the SDK SHALL read the journal state shipped on `RestoreInitState.op_journal`, SHALL **skip** ops already recorded as committed, and SHALL seed the next op's batch from the journaled handoff via the channel codec.

#### Scenario: A committed op is skipped on resume, not re-run

- **WHEN** a durable bounded class commits op A, then the runner is SIGKILLed, then the operative is rebound with a journal recording op A as committed
- **THEN** op A's `run` SHALL NOT be invoked again, and op B SHALL run seeded from op A's journaled handoff

#### Scenario: A missing inter-op codec fails registration

- **WHEN** a durable structured bounded class declares an inter-op channel without a codec
- **THEN** registration SHALL throw `BoundedOperativeConfigurationException` carrying the error name `MissingInterOpCodec` and naming the channel

#### Scenario: An opted-out class re-runs from the top

- **WHEN** a bounded class declares `persistenceEnabled() == false` and is resumed after a crash
- **THEN** the SDK SHALL ship no `OpCommit`, and every op SHALL re-run from the top

#### Scenario: A commit timeout triggers compensation

- **WHEN** the controller does not answer an `OpCommit` within 30 seconds
- **THEN** the SDK SHALL treat the op as terminally failed and SHALL run the compensation plan

### Requirement: Java SDK implements bounded staged commit and the transactional outbox

The Java SDK SHALL implement the `bounded-transactional-outbox` capability. Inside a structured op of a durable bounded class, `ctx.emit` SHALL NOT reach the wire: `BoundedCollectSink` SHALL buffer `(channel, bytes)` and the buffered outputs SHALL ride the same `OpCommit` as the journal record, in the `outbox` field (`OpOutboxItem{segment_id, seq_in_op, channel, payload}`). A failing stage SHALL drop its staged bytes together with the error, so nothing leaks. `run`-sugar classes (no barrier to stage against) and transport-less runs SHALL keep the direct-emit path.

Compensation output SHALL ride the same handshake with `compensating` set.

#### Scenario: An op's emits are staged, not shipped, until the barrier

- **WHEN** a structured op of a durable bounded class calls `ctx.emit(v)` twice and then reaches its op barrier
- **THEN** no plugin frame SHALL be sent during the op body, and the `OpCommit` at the barrier SHALL carry both outputs in its `outbox`

#### Scenario: A failing stage leaks no staged output

- **WHEN** a stage emits once and then throws
- **THEN** the staged bytes SHALL be dropped with the error and SHALL NOT appear in any subsequent `OpCommit`

#### Scenario: run-sugar keeps the direct path

- **WHEN** a bounded class authored with `run`-sugar calls `ctx.emit(v)`
- **THEN** the frame SHALL be sent directly, as before this change

### Requirement: Java SDK implements the bounded durable loop and suspension

The Java SDK SHALL implement the `bounded-durable-loop` and `bounded-durable-suspension` capabilities. `BoundedOperative` SHALL declare `loopMode()` returning `ONCE` (the default, byte-identical to the current single pass), `PER_INPUT` (one iteration per inbox item), or `CHAINED` (each iteration seeded by the previous iteration's terminal handoff), plus `wakeAfter()`.

At every iteration boundary the SDK SHALL ship `IterCommit` (tag 30) carrying the iteration index, the consumed inbox offset, and the terminal handoff, and SHALL await `IterCommitResponse` (tag 31). Registration SHALL reject a loop on `run`-sugar (`LoopWithoutChain`) and a durable `CHAINED` loop whose terminal op has no codec'd output channel (`LoopTerminalWithoutCodec`).

When the loop is idle past the park hysteresis, the SDK SHALL ship `IterPark` (tag 32) and exit **without** `BoundedCompleted`, yielding a parked outcome. The SDK SHALL read its inbox offset from the inbound `flz.inbox_offset` metadata key.

#### Scenario: Each loop iteration commits at its boundary

- **WHEN** a `PER_INPUT` bounded class processes three inbox items
- **THEN** the SDK SHALL ship three `IterCommit` commands, each carrying its iteration index and consumed inbox offset, and SHALL await each response before starting the next iteration

#### Scenario: An idle loop parks instead of completing

- **WHEN** a durable looping bounded class has no inbox item for longer than the park hysteresis
- **THEN** the SDK SHALL ship `IterPark` and exit without shipping `BoundedCompleted`

#### Scenario: A resumed loop restarts at the journaled iteration

- **WHEN** a looping bounded class committed iteration 2 and is then rebound after a crash
- **THEN** the SDK SHALL resume at iteration 3, seeded (for `CHAINED`) from iteration 2's journaled terminal handoff

#### Scenario: A loop on run-sugar is refused

- **WHEN** a bounded class authored with `run`-sugar declares a non-`ONCE` `loopMode()`
- **THEN** registration SHALL throw `BoundedOperativeConfigurationException` carrying the error name `LoopWithoutChain`

#### Scenario: A durable chained loop whose terminal op has no codec is refused

- **WHEN** a durable `CHAINED` bounded class's terminal op has no codec'd output channel
- **THEN** registration SHALL throw `BoundedOperativeConfigurationException` carrying the error name `LoopTerminalWithoutCodec`

### Requirement: Java SDK implements the bounded segment retry ladder

The Java SDK SHALL implement the `bounded-segment-retry` capability. `BoundedOperative` SHALL declare `retryAttempts()` (default 3 total attempts) and `retryBackoff()` (default 500 ms base). Only the failing segment SHALL re-run its batch; siblings SHALL stay buffered at the barrier, and each attempt SHALL use a fresh sink so a failed attempt's staged output is dropped rather than double-emitted.

Backoff SHALL reuse the dead-letter ladder's helper (`base × 2^(n-1)` with ±50% jitter, capped at 5 s) and the wait SHALL be cancellation-aware, so a stop or `maxDuration` ends it at once. Retry is strictly pre-commit: a journaled op SHALL never be retried. `retryAttempts() == 0` SHALL collapse to a single attempt; `attempts > 1` with a zero backoff SHALL be rejected at registration with `ZeroRetryBackoff`. A `BoundedException` SHALL carry a `nonRetryable` flag; a thrown `RuntimeException` SHALL stay retryable.

#### Scenario: A transient segment failure recovers on retry

- **WHEN** a bounded segment's batch throws on its first attempt and succeeds on its second
- **THEN** the operative SHALL complete successfully, only the failing segment SHALL have re-run, and its first attempt's staged output SHALL NOT be emitted

#### Scenario: An exhausted ladder compensates

- **WHEN** a bounded segment fails all three attempts
- **THEN** the operative SHALL abort and run the compensation plan

#### Scenario: A non-retryable error skips the ladder

- **WHEN** a segment throws `BoundedException.nonRetryable("bad record")`
- **THEN** the SDK SHALL NOT retry and SHALL proceed directly to the abort path

#### Scenario: attempts > 1 with a zero backoff is refused

- **WHEN** a bounded class declares `retryAttempts() == 3` and `retryBackoff() == Duration.ZERO`
- **THEN** registration SHALL throw `BoundedOperativeConfigurationException` carrying the error name `ZeroRetryBackoff`

### Requirement: Java SDK implements bounded loop rewind

The Java SDK SHALL implement the `bounded-loop-rewind` capability. On restore, the SDK SHALL read `OpJournalState.rewind_undo` (the ops the controller's rewind marker determined are still owed an undo) and, before entering the forward loop, SHALL run those undos in reverse order under a **forced** all-or-nothing rollback mode — whatever the class's declared `rollbackMode()` says — since a rewind unwinds committed iterations by definition.

The SDK SHALL journal exactly one `compensated` record per undone op, **including an empty one for an op with no compensate hook**, since that record is what drains `rewind_undo`. The undo pass SHALL NOT consult the per-segment crash-resume done markers, which are per segment and would wrongly suppress an op that ran in two rewound iterations.

A rewind SHALL NOT replay inputs: the controller truncates the inbox at each iteration commit, so a rewind is undo-and-continue — the loop resumes waiting and the next message becomes the following iteration.

#### Scenario: A rewind undoes committed iterations in reverse order

- **WHEN** the controller rewinds a looping bounded class to iteration 1 and the class committed iterations 2 and 3
- **THEN** on rebind the SDK SHALL run the compensate hooks for iterations 3 and 2's ops in reverse order before resuming the forward loop

#### Scenario: A hookless op still drains the rewind undo

- **WHEN** an op owed a rewind undo declares no compensate hook
- **THEN** the SDK SHALL journal an empty `compensated` record for it, so it is not re-offered on the next restore

#### Scenario: A rewind does not re-deliver consumed inputs

- **WHEN** a looping bounded class is rewound to iteration 1
- **THEN** the inputs consumed by iterations 2 and 3 SHALL NOT be re-delivered; the loop SHALL resume waiting, and the next inbound message SHALL become the next iteration

### Requirement: Java SDK implements bounded branching

The Java SDK SHALL implement the `bounded-branching` capability. `BoundedSpec` SHALL model the op graph as a tree (`order` plus `children`) rather than the current `List<Integer> chain`; a linear chain SHALL be the degenerate single-child case. `BoundedSpec.linearChain()`'s refusal of any non-linear graph SHALL be removed and replaced with validation over the tree.

The SDK SHALL declare the tree's leaf ops to the controller via the `leaf_ops` key of `AddOperativeResponse.metadata`. Fan-in SHALL NOT silently overwrite an edge. Compensation SHALL run in reverse-topological order, which the existing BFS numbering already yields — the compensation planner SHALL NOT be changed.

#### Scenario: A branching chain runs its branches and joins

- **WHEN** a bounded class declares op 0 feeding both op 1 and op 2, which both feed op 3
- **THEN** ops 1 and 2 SHALL each run seeded from op 0's handoff, and op 3 SHALL run once both have committed

#### Scenario: Compensation runs in reverse-topological order

- **WHEN** the branching class above aborts after op 3 commits
- **THEN** the compensate hooks SHALL run in the order 3, then 2 and 1, then 0

#### Scenario: Fan-in does not overwrite an edge

- **WHEN** two ops both feed the same downstream op on the same channel
- **THEN** both edges SHALL be recorded, and neither SHALL silently replace the other

### Requirement: Java SDK data heartbeat reports dispatch progress

The Java SDK's `data_heartbeat` frame SHALL carry the same metrics the Rust SDK ships: `dispatches`, `blocked_parks`, and `credit_parked`. Reporting only `credit_parked` leaves the controller's `StallClassifier` unable to distinguish a live-but-wedged Java runner from a healthy idle one, so the omission is a liveness defect, not a cosmetic gap.

#### Scenario: The heartbeat carries dispatch progress

- **WHEN** a Java runner has dispatched 100 inputs since the previous heartbeat
- **THEN** the next `data_heartbeat` frame's metadata SHALL carry a `dispatches` count reflecting that progress, alongside `blocked_parks` and `credit_parked`

#### Scenario: A wedged Java runner is distinguishable from an idle one

- **WHEN** a Java runner stops making dispatch progress while inputs continue to arrive
- **THEN** the heartbeat's `dispatches` count SHALL stop advancing while the runner keeps sending heartbeats, giving the controller the signal it needs to localize the stall

### Requirement: Operative Type Declaration

The Java SDK SHALL provide `RunnerBuilder.defineUnbounded(Class<? extends UnboundedOperative>)` and `RunnerBuilder.defineBounded(Class<? extends BoundedOperative>)` as chainable builder methods, called **before** `.start()`/`.run()`. For each declared UnboundedOperative class the SDK SHALL: (a) instantiate the class via its public no-arg constructor; (b) call its `factoryId()` method to obtain the controller-facing factory id; (c) call its `channels(ChannelBuilder)` method exactly once — the builder being the producer-first graph builder of `sdk-graph-authoring` — and freeze the resulting `OperativeTopology`; (d) verify wiring constraints (entry-channel deserializability, type-equality between a stream's payload type and its consumer Operations' input types, node reachability from the entry, at most one output channel per node, acyclicity, node-identity and channel-name uniqueness, static-mode and dynamic-mode default-impl detection); (e) record `(factoryId → topology)` in the class registry. The registry MUST be frozen at `.start()` time. There SHALL be no `OperativeHandler`-style factory registration in the public surface.

Dynamic segmentation SHALL be supported: an Operation that overrides `operationKey(Input)` (signalled by `operationKeyOverridden()` returning `true`) and returns an empty list from `operationKeys()` SHALL be declared successfully and create Segments lazily per key at dispatch time, per the `operative-state-persistence` dynamic-segmenting behaviour. Eviction SHALL be off by default (`segmentIdleTtl()` returns `Optional.empty()`); when a TTL is declared, the per-Operation reaper SHALL evict idle Segments destructively by shipping `delete_segment_state`, and a recreated Segment SHALL cold-start fresh rather than restore from disk.

Because Java's type erasure precludes Rust's compile-time topology refusals, every constraint Rust enforces at compile time SHALL be enforced at declaration time, throwing `OperativeConfigurationException` whose message carries the same diagnostic content and the same error name as the corresponding Rust `BuildTopologyError` variant.

#### Scenario: Declaring an unbounded operative class before start() succeeds
- **WHEN** user code calls `Flezha.newRunner("my_app").defineUnbounded(MarketOperative.class).start()`
- **THEN** the handshake init JSON SHALL include `"market"` (the value `MarketOperative::factoryId` returns) in `factory_ids`, and subsequent `AddOperative{factory_id:"market"}` commands SHALL succeed by spawning an `UnboundedOperativeInstance`

#### Scenario: Declaring an operative type after start() is rejected
- **WHEN** user code calls `.defineUnbounded(...)` on a `RunnerBuilder` whose `.start()` has already returned
- **THEN** the SDK SHALL throw an `IllegalStateException`

#### Scenario: Duplicate factoryId across two unbounded operative classes is rejected
- **WHEN** two distinct UnboundedOperative classes both return `"market"` from `factoryId()` and both are declared on the same builder
- **THEN** the second `.defineUnbounded(...)` call SHALL throw an `IllegalArgumentException` naming the duplicate factoryId

#### Scenario: Wiring mismatch fails declaration with a diagnosable error
- **WHEN** an Operative wires `.into(OhlcOperation.class)` onto `ParseOperation`'s output stream but `ParseOperation.outputType()` returns `Trade.class` while `OhlcOperation.inputType()` returns `Candle.class`
- **THEN** `.defineUnbounded(...)` SHALL throw `OperativeConfigurationException` whose message names the channel, the stream's payload type (`Trade`), and the consumer-input type (`Candle`)

#### Scenario: Typed entry channel deserializes at the edge
- **WHEN** an Operative declares `c.input(Trade.class, "trade.raw").into(ParseOperation.class)` and a JSON-encoded Trade arrives
- **THEN** `.defineUnbounded(...)` SHALL succeed and `ParseOperation.run` SHALL receive the Jackson-deserialized `Trade`, per the typed-entry-input contract of `sdk-graph-authoring`

#### Scenario: Unreachable node fails declaration
- **WHEN** an Operative's `channels(c)` wires an Operation that is not reachable from the entry channel
- **THEN** `.defineUnbounded(...)` SHALL throw `OperativeConfigurationException` naming the unreachable Operation

#### Scenario: Static-mode operation without segmentForOverridden is rejected
- **WHEN** an Operation returns a non-empty list from `operationKeys()` but its `segmentForOverridden()` returns the default `false`
- **THEN** `.defineUnbounded(...)` SHALL throw `OperativeConfigurationException` naming the Operation and stating that static-mode operations must override `segmentFor` and `segmentForOverridden`

#### Scenario: Dynamic-mode operation declares and creates segments lazily
- **WHEN** an Operation returns `true` from `operationKeyOverridden()` and an empty list from `operationKeys()`, and two inputs arrive whose `operationKey(input)` returns `"BTC"` and `"ETH"`
- **THEN** `.defineUnbounded(...)` SHALL succeed, and the runtime SHALL lazily allocate exactly two Segments keyed `"BTC"` and `"ETH"`, each constructed with `initState(Optional.of(key), Optional.empty())`

### Requirement: Runner process singleton guard

The Java SDK SHALL allow at most one runner to be **started** per JVM process. `Flezha.createRunner(...)` SHALL NOT claim the singleton slot — any number of unstarted runners may exist. The slot SHALL be claimed by compare-and-set at `.start()`/`.run()`; if a prior runner's `.start()`/`.run()` has already succeeded in this process and not yet closed, the call SHALL throw `IllegalStateException` naming the namespace of the already-running runner, and SHALL NOT connect, mutate any shared registry, or spawn any background thread. Calling `.start()` twice on the same runner SHALL throw `IllegalStateException`.

#### Scenario: Second start() in the same JVM is rejected
- **GIVEN** a JVM process where `Flezha.createRunner("ns1").defineUnbounded(A.class).start()` has already succeeded
- **WHEN** code in the same process calls `Flezha.createRunner("ns2").defineUnbounded(B.class).start()`
- **THEN** the call SHALL throw `IllegalStateException` whose message names `"ns1"` as the already-running namespace
- **AND** no connection SHALL be attempted for `"ns2"`, and the process's stable identity, segment-state sink, and credit window SHALL remain exclusively `"ns1"`'s

#### Scenario: Creating runners never claims the slot
- **GIVEN** a JVM process where a runner for `"ns1"` is already started
- **WHEN** code calls `Flezha.createRunner("ns2")` and mutates it with `define*`/`remove*` without calling `.start()`
- **THEN** no exception SHALL be thrown and `"ns1"` SHALL remain the process's active runner

#### Scenario: A runner may be started again after the first one closes
- **GIVEN** a JVM process where a runner for `"ns1"` was started and then fully closed via `.close()`
- **WHEN** code in the same process calls `Flezha.createRunner("ns2").defineUnbounded(B.class).start()`
- **THEN** the call SHALL succeed and `"ns2"` SHALL become the process's active runner

#### Scenario: Concurrent start() calls — exactly one wins
- **GIVEN** a JVM process with no runner started yet
- **WHEN** two threads each call `.start()` on a distinct `FlezhaRunner` (`"ns1"` and `"ns2"`) at the same time
- **THEN** exactly one call SHALL succeed and become the process's active runner, and the other SHALL throw `IllegalStateException` naming the winner — the guard's compare-and-set SHALL make this outcome deterministic per-run (no scenario where both succeed or both fail)

### Requirement: Runner lifecycle — start, run, awaitTermination, close

`FlezhaRunner.start()` SHALL connect (per the `Java SDK Entry Point` requirement) and return `this` without blocking the calling thread for the runner's ongoing lifetime. `FlezhaRunner.awaitTermination()` SHALL block the calling thread until the runner shuts down (via `.close()`, SIGTERM-triggered graceful shutdown, or a controller-driven stop), replacing any user-authored keep-alive loop; called on a never-started runner it SHALL throw `IllegalStateException` ("runner not started"), as SHALL `getOperatives()`. `FlezhaRunner.run()` SHALL be exactly `.start()` followed by `awaitTermination()`, with no independently-implemented blocking logic. `FlezhaRunner` SHALL implement `AutoCloseable`; `.close()` on a started runner SHALL run the existing graceful-shutdown sequence (ship-fresh-state-on-sdk-shutdown), release the singleton slot, and cause any thread blocked in `awaitTermination()` to return; `.close()` on a never-started runner SHALL be a no-op; `.close()` SHALL be idempotent.

#### Scenario: start() returns immediately after the handshake, before shutdown
- **WHEN** an SDK user calls `.start()` on a runner
- **THEN** the call returns as soon as the runner has an assigned id, without waiting for the runner to ever shut down
- **AND** the runner continues dispatching in background threads exactly as it does today

#### Scenario: run() blocks until shutdown
- **WHEN** an SDK user calls `.run()` on a runner in a standalone `main()`
- **THEN** the call blocks the calling thread until the runner shuts down (SIGTERM or explicit close from another thread), and the JVM does not exit before then even though the runner's background threads are daemon

#### Scenario: close() wakes a thread blocked in awaitTermination()
- **GIVEN** a started `FlezhaRunner` with one thread blocked in `awaitTermination()`
- **WHEN** another thread calls `.close()` on the same runner
- **THEN** the graceful-shutdown sequence runs, and the blocked thread's `awaitTermination()` call returns

#### Scenario: Pre-start lifecycle calls
- **GIVEN** a `FlezhaRunner` created but never started
- **WHEN** code calls `.awaitTermination()` or `.getOperatives()`
- **THEN** each SHALL throw `IllegalStateException` stating the runner is not started
- **AND** calling `.close()` on the same never-started runner SHALL return without effect

#### Scenario: try-with-resources still works for short-lived usage
- **GIVEN** a test that needs a runner for the duration of one test method
- **WHEN** the test writes `try (FlezhaRunner r = Flezha.createRunner("test").defineUnbounded(A.class).start()) { ... }`
- **THEN** the runner SHALL be started for the block and gracefully closed at the end of it, exactly as `AutoCloseable` usage requires

### Requirement: Flezha static accessor for the active runner

`Flezha.currentRunner()` SHALL return an `Optional<FlezhaRunner>` containing the process's active runner if one has been started and not yet closed, or `Optional.empty()` otherwise. It SHALL NOT throw, and SHALL NOT start a runner as a side effect. Runners that were created but never started SHALL never appear in `currentRunner()`.

#### Scenario: currentRunner() reflects the singleton's lifecycle
- **GIVEN** a JVM process with no runner started yet
- **WHEN** code calls `Flezha.currentRunner()`
- **THEN** it SHALL return `Optional.empty()`
- **AND** after `Flezha.createRunner("ns1").defineUnbounded(A.class).start()` succeeds, `Flezha.currentRunner()` SHALL return `Optional.of(<that runner>)`
- **AND** after that runner's `.close()` completes, `Flezha.currentRunner()` SHALL return `Optional.empty()` again
- **AND** a runner created via `Flezha.createRunner(...)` but never started SHALL NOT appear in `currentRunner()`

### Requirement: Each Segment SHALL run on its own Operation instance

Each Segment SHALL invoke `run` on its own Operation instance, as the Rust SDK does by holding one Operation instance per Segment alongside that Segment's State, so that a class authored against either SDK means the same thing.

Today `UnboundedOperativeInstance` builds one Operation per `OperationDispatcher` and `OperationDispatcher` invokes `run` on that single object for every Segment it owns — static-key, keyless, and dynamic alike. The per-Segment instances it also constructs are used only for `initState` and a `schemaVersion()` read. No instance-level lock guards `run`; the per-Segment `stateLock` and `changeBufferLock` are distinct objects per Segment and serialise nothing between Segments, which drain concurrently on the worker pool. Any mutable field on a user's Operation is therefore a shared, unsynchronised channel across Segment keys.

#### Scenario: Instance state does not leak between Segment keys

- **GIVEN** an Operation declaring two Segment keys, whose implementation writes to a mutable instance field during `run`
- **WHEN** input is dispatched to both Segments
- **THEN** neither Segment SHALL observe the value written by the other

#### Scenario: Concurrent Segments do not race on one instance

- **GIVEN** an Operation with multiple Segments draining concurrently on the worker pool
- **WHEN** each Segment's `run` writes to a mutable instance field
- **THEN** the writes SHALL NOT race, because each Segment holds its own instance

#### Scenario: Dynamic Segments each receive an instance

- **GIVEN** a dynamic-mode Operation that creates Segments lazily per key
- **WHEN** a Segment is created for a newly-seen key
- **THEN** that Segment SHALL run on its own Operation instance

#### Scenario: The dispatcher's documentation matches its behaviour

- **WHEN** the dispatcher documents the lifetime of the Operation instance it holds
- **THEN** the documented lifetime SHALL be the implemented lifetime

### Requirement: The Java SDK SHALL document its actual state-mutation enforcement level

The Java SDK SHALL describe the state-mutation enforcement it actually provides, and SHALL document its asymmetry with Rust the way it already documents its other Rust asymmetries.

`StateHandle.get()` returns the live State object, so `state.get().field = x` compiles, runs, and is never journaled as a Change. The Rust SDK forecloses this structurally by implementing `Deref` and deliberately not `DerefMut`. The Java class javadoc and `docs/java_state_persistence.md` both assert that Java's lack of a setter is "the structural enforcement" — which it is not, because `get()` hands back the object.

#### Scenario: Documentation does not claim enforcement that does not exist

- **WHEN** Java SDK documentation describes how state mutation outside `apply` is prevented
- **THEN** it SHALL NOT assert structural enforcement unless a direct field mutation is actually prevented

#### Scenario: The asymmetry with Rust is stated

- **WHEN** a user consults the Java state-persistence documentation
- **THEN** it SHALL state that Rust forecloses direct mutation at compile time and that Java does not, and SHALL name the detection available instead

#### Scenario: A bypassed mutation's actual fate is described

- **WHEN** the documentation describes the consequence of mutating state outside `apply`
- **THEN** it SHALL state that the mutation is captured by graceful shutdown and live migration but lost by any restore, which rebuilds from snapshot plus change log

### Requirement: Factory visibility SHALL match its documentation

A factory or constructor's declared visibility SHALL match the visibility its documentation claims. `StateHandle.create(...)` is declared `public static` while its own javadoc calls it a "package-private factory" and the class javadoc claims a package-private constructor "forecloses user instantiation". No such enforcement exists.

#### Scenario: Declared visibility matches documented visibility

- **WHEN** a factory or constructor documents its visibility
- **THEN** the declared modifier SHALL match, or the documentation SHALL be corrected to describe the real visibility

