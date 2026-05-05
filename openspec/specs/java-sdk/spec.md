# java-sdk Specification

## Purpose

Defines the Java SDK for Flezha compute engines: the public API, protocol layer, command dispatch, transport selection, reconnection, the Relay / Shard / Partition runtime, and the JNI bridge to `flz_transport`. Conforms to `docs/sdk_protocol_spec.md`.

## Requirements

### Requirement: Java SDK Entry Point

The Java SDK SHALL provide `com.flezha.sdk.Flezha.newInstance(String namespace)` as the single entry point for starting a Flezha compute engine. The call MUST connect to the agent's control socket, send the handshake init message per `docs/sdk_protocol_spec.md` §5.2, wait for `AssignComputeEngineId`, and return a handle exposing `getId()` and `getProcessors()` (where `getProcessors()` returns a snapshot of currently-spawned Relay instances keyed by what the wire calls `processor_id` — see the `Relay Registration` requirement below for naming).

#### Scenario: Successful connection returns an assigned id
- **WHEN** an SDK user calls `Flezha.newInstance("my_app")` with the agent running and at least one Relay class registered
- **THEN** the call blocks until the agent responds with `AssignComputeEngineId`, and the returned handle's `getId()` returns the assigned id (non-null, non-empty)

#### Scenario: Agent unavailable triggers exponential backoff retries
- **WHEN** an SDK user calls `Flezha.newInstance("my_app")` and the agent's control socket is not yet listening
- **THEN** the SDK SHALL retry the connect with exponential backoff starting at 200 ms and capped at 30 s, and succeed once the agent becomes available

#### Scenario: newInstance without any registered relay class fails fast
- **WHEN** `Flezha.newInstance(namespace)` is called before any `RelayRegistry.registerRelay(...)` (or `Flezha.registerRelay(...)`) call
- **THEN** the SDK SHALL throw an `IllegalStateException` describing the missing relay registration

### Requirement: Relay Registration

The Java SDK SHALL provide `RelayRegistry.registerRelay(Class<? extends Relay>)` (and a thin static `Flezha.registerRelay(Class<? extends Relay>)` pass-through) that MUST be called **before** `Flezha.newInstance`. For each registered Relay class the SDK SHALL: (a) instantiate the class via its public no-arg constructor; (b) call its `factoryId()` method to obtain the agent-facing factory id; (c) call its `channels(ChannelBuilder)` method exactly once and freeze the resulting `RelayTopology`; (d) verify wiring constraints (entry-channel-type, type-equality between channel payload and producer/consumer Shard I/O, no unwired channels, no multi-producer, static-mode and dynamic-mode default-impl detection); (e) record `(factoryId → topology)` in the class registry. The registry MUST be frozen at `Flezha.newInstance(...)` time. There SHALL be no `ProcessorHandler`-style factory registration in the public surface.

#### Scenario: Registering a relay class before newInstance succeeds
- **WHEN** user code calls `Flezha.registerRelay(MarketRelay.class)` and then `Flezha.newInstance("my_app")`
- **THEN** the handshake init JSON SHALL include `"market"` (the value `MarketRelay::factoryId` returns) in `factory_ids`, and subsequent `AddProcessor{factory_id:"market"}` commands SHALL succeed by spawning a `RelayInstance`

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

The Java SDK's worker pool (sized by `FLEZHA_WORKER_POOL_SIZE`, default = `Runtime.availableProcessors()`) SHALL guarantee that for any single `(relayInstance, shard, partition)` triple, two invocations of that Shard's `run(...)` method on that Partition never execute concurrently, and invocations observe their inputs in the exact order those inputs were enqueued onto the Partition's mailbox. This invariant MUST hold regardless of worker-pool size, number of concurrent Relay instances, number of Partitions per Shard, or transport (UDS/QUIC).

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

The Java SDK SHALL implement handlers for every `CommandRequest` variant defined in `compute_engine_commands.proto`: `AssignComputeEngineId`, `ConnectSocket`, `AddProcessor`, `RemProcessor`, `ProcessorState`, `MigrationSerialization`, `MigrationDeserialization`, `ProcessorMessage`, `ComputeEngineDisconnect`. Each handler MUST produce a `CommandResponse` that echoes the original `reference_id` and sets `status = "success"` or `status = "error"` with populated `errors` per `docs/sdk_protocol_spec.md` §6. `AddProcessor` and `RemProcessor` SHALL spawn and tear down `RelayInstance` objects respectively (the wire field is named `processor_id` for backward continuity; the SDK maps it to `relayInstanceId`). `ProcessorMessage` SHALL respond with the canonical "unsupported" error until `relay-message-handlers` ships per-Shard `handleMessage`.

#### Scenario: AddProcessor spawns a relay instance
- **WHEN** the agent sends `AddProcessor{processor_id:"p1", factory_id:"market"}` and Relay class `MarketRelay` is registered with `factoryId() == "market"`
- **THEN** the SDK SHALL allocate a `RelayInstance` keyed by `"p1"`, call `initState(...)` for every Shard×Partition declared by `MarketRelay`, and respond with `status = "success"`

#### Scenario: AddProcessor with init_values seeds initial-state for each partition
- **WHEN** the agent sends `AddProcessor{processor_id:"p1", factory_id:"market", init_values:"{\"symbol\":\"BTC\"}"}` and `MarketRelay` declares one Shard with single-partition mode
- **THEN** the SDK SHALL pass the JSON string into the Shard's `initState(Optional.empty())` (the runtime makes the JSON available via a per-relay context), invoke `run` only after `initState` returns, and respond with `status = "success"`

#### Scenario: AddProcessor with unknown factory_id returns error
- **WHEN** the agent sends `AddProcessor{factory_id:"missing"}`
- **THEN** the SDK SHALL respond with `status = "error"` and `errors` containing a string naming the missing factory id

#### Scenario: ProcessorState returns the per-Shard / per-Partition JSON shape
- **WHEN** the agent sends `ProcessorState{processor_id:"p1"}` and `p1` exists with two Shards (`shardA` single-mode, `shardB` static-mode with keys `["1m", "1h"]`)
- **THEN** the SDK SHALL respond with `status = "success"` and `raw_data` containing JSON of shape `{"shardA": {<state>}, "shardB": {"1m": {<state>}, "1h": {<state>}}}` (whichever exact shape the Rust SDK emits per `relay-mvp-rust/design.md` Decision 10 — the Java SDK SHALL match byte-for-byte)

#### Scenario: MigrationSerialization removes the relay instance
- **WHEN** the agent sends `MigrationSerialization{processor_id:"p1"}` and `p1` exists
- **THEN** the SDK SHALL respond with `status = "success"` and `raw_data` containing the per-Shard / per-Partition JSON (same shape as `ProcessorState`), AND `p1` SHALL be removed from the relay instance registry

#### Scenario: ProcessorMessage returns canonical unsupported error pre-relay-message-handlers
- **WHEN** the agent sends `ProcessorMessage{processor_id:"p1", payload:<...>}` to a v1 SDK
- **THEN** the SDK SHALL respond with `ProcessorMessageResponse{ payload: "", error: "processor does not support handle_message" }`

#### Scenario: ComputeEngineDisconnect halts the reconnect loop
- **WHEN** the agent sends `ComputeEngineDisconnect{}`
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

The Java SDK SHALL use `protobuf-java` to encode `CommandResponse` and decode `CommandRequest` messages generated from the existing `compute_engine_commands.proto` (no new proto definitions, no hand-written wire-format code).

#### Scenario: Encoding AssignComputeEngineId response matches protocol
- **WHEN** the SDK encodes `CommandResponse{reference_id:"#Ref<0.1.2.3>", status:"success"}`
- **THEN** the produced bytes SHALL be valid protobuf decodable by the Elixir agent's `CommandResponse` parser

#### Scenario: Decoding AddProcessor request extracts all fields
- **WHEN** the SDK receives a protobuf-encoded `CommandRequest` with an `AddProcessor` oneof case
- **THEN** the decoder SHALL expose `processor_id`, `factory_id`, and `init_values` as Java strings

### Requirement: Transport Selection

The Java SDK SHALL honor the `FLEZHA_TRANSPORT` environment variable (`uds` or `quic`) for transport selection, defaulting to `uds` when unset. For QUIC, the SDK SHALL also honor `FLEZHA_QUIC_ADDRESS`, `FLEZHA_CA_CERT_PATH`, `FLEZHA_SERVER_CERT_PATH`, `FLEZHA_SERVER_KEY_PATH`. For UDS, the SDK SHALL honor `FLEZHA_SOCKET_FILES_PATH`. All env var names and defaults MUST match `docs/sdk_protocol_spec.md` §2.

#### Scenario: Default transport is UDS
- **WHEN** `FLEZHA_TRANSPORT` is unset and `Flezha.newInstance(...)` is called
- **THEN** the SDK SHALL connect to `{FLEZHA_SOCKET_FILES_PATH or app_data/sockets}/compute_engine_control_channel.sock`

#### Scenario: QUIC transport uses configured address and cert paths
- **WHEN** `FLEZHA_TRANSPORT=quic` and `FLEZHA_QUIC_ADDRESS=127.0.0.1:4433`
- **THEN** the SDK SHALL open a QUIC connection to that address using the CA cert at the configured path, opening the control channel on the first bidi stream per §2 "QUIC Stream Mapping"

### Requirement: Auto-Reconnect on Unexpected Disconnect

The Java SDK SHALL distinguish intentional disconnects (`ComputeEngineDisconnect` command handled → status `Stopped`) from unexpected ones (BrokenPipe / UnexpectedEof / ConnectionReset / QUIC connection lost). On an unexpected disconnect the SDK MUST clear `io_streams`, the live processor registry, and work queues; reset `compute_engine_id` to `undefined`; and reconnect with exponential backoff (200 ms, 400 ms, 800 ms, …, capped at 30 s) per `docs/sdk_protocol_spec.md` §5 "Auto-Reconnection".

#### Scenario: Agent crash triggers reconnect
- **WHEN** the agent process is killed while the SDK has status `Running`
- **THEN** the SDK SHALL detect the disconnect via the native layer, clear local state, and reconnect once the agent is available again, receiving a fresh `compute_engine_id`

#### Scenario: Intentional disconnect does not reconnect
- **WHEN** the SDK receives `ComputeEngineDisconnect{}` and responds successfully
- **THEN** the SDK SHALL close its sockets and NOT attempt any reconnection

### Requirement: Worker Pool and Dispatch Throughput Floor

The Java SDK SHALL operate on a single shared bounded executor sized by `FLEZHA_WORKER_POOL_SIZE` (default = `Runtime.availableProcessors()`), draining all `PartitionSlot` mailboxes work-steal-style. The SDK SHALL sustain at least the throughput floor (in messages/second) measured against the v1 implementation at merge time on the regression bench (`100 relays × 1 shard × 1 partition × 10K msg`, no-op Shard, UDS transport, default worker-pool size; see `relay-mvp-java/design.md` Decision 7) invoked via `mix bench --runtime java --shape regression -n 100 -m 10000`. The bench-regression-java CI job is informational in v1; the literal floor value is recorded in this requirement once `mix bench --runtime java` has been measured 3× on the reference host (median, rounded down to a clean number) — until then the recorded floor is "TBD pending v1 measurement".

#### Scenario: Reference bench meets the Java throughput floor
- **WHEN** a developer runs `MIX_ENV=prod mix bench --runtime java --shape regression -n 100 -m 10000` on a modern 8-core x86_64 laptop with a warm JVM (≥ 30 s warmup)
- **THEN** the reported `Throughput:` line SHALL be ≥ the recorded Java floor value

#### Scenario: Default worker pool size matches available processors
- **WHEN** the SDK starts with `FLEZHA_WORKER_POOL_SIZE` unset
- **THEN** the worker pool SHALL be sized to `Runtime.availableProcessors()`

### Requirement: JNI Bridge to flz_transport

The Java SDK SHALL interact with the transport layer exclusively through a JNI bridge that wraps `flz_transport`'s C FFI. The bridge MUST expose Java-side operations for: opening a transport context with a `FlezhaConfig`, connecting (client mode), opening streams, reading and writing stream data, registering a log callback, and destroying the context. All native buffers returned by the bridge MUST be copied into Java `byte[]` / `String` before the native memory is freed.

#### Scenario: Native buffer freed after copy into Java byte[]
- **WHEN** the JNI bridge receives a data callback carrying a native buffer pointer and length
- **THEN** the bridge SHALL allocate a Java `byte[]` of equal length, copy the native bytes into it, invoke the Java-side handler with the `byte[]`, and ensure the native buffer is freed (or returned to `flz_transport` ownership) before control returns to the native caller

#### Scenario: flezha_version mismatch is logged
- **WHEN** the SDK starts and `flezha_version()` returns a string that does not equal the SDK's bundled expected version
- **THEN** the SDK SHALL log a WARNING naming both versions and continue startup
