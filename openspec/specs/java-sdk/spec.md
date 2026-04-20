# java-sdk Specification

## Purpose

Defines the Java SDK for Flezha compute engines: the public API, protocol layer, command dispatch, transport selection, reconnection, processor runners, and the JNI bridge to `flz_transport`. Conforms to `docs/sdk_protocol_spec.md`.

## Requirements

### Requirement: Java SDK Entry Point

The Java SDK SHALL provide `com.flezha.sdk.Flezha.newInstance(String namespace)` as the single entry point for starting a Flezha compute engine. The call MUST connect to the agent's control socket, send the handshake init message per `docs/sdk_protocol_spec.md` §5.2, wait for `AssignComputeEngineId`, and return a handle exposing `getId()` and `getProcessors()`.

#### Scenario: Successful connection returns an assigned id
- **WHEN** an SDK user calls `Flezha.newInstance("my_app")` with the agent running and at least one factory registered
- **THEN** the call blocks until the agent responds with `AssignComputeEngineId`, and the returned handle's `getId()` returns the assigned id (non-null, non-empty)

#### Scenario: Agent unavailable triggers exponential backoff retries
- **WHEN** an SDK user calls `Flezha.newInstance("my_app")` and the agent's control socket is not yet listening
- **THEN** the SDK SHALL retry the connect with exponential backoff starting at 200 ms and capped at 30 s, and succeed once the agent becomes available

#### Scenario: newInstance without any registered factory fails fast
- **WHEN** `Flezha.newInstance(namespace)` is called before any `FactoryRegistry.registerFactory(...)` call
- **THEN** the SDK SHALL throw an `IllegalStateException` describing the missing factory registration

### Requirement: Factory Registration

The Java SDK SHALL provide `FactoryRegistry.registerFactory(factoryId, settings, initialState, handlerGenerator)` that MUST be called **before** `Flezha.newInstance`. The registry MUST carry the factory id, a `ProcessorSettings` instance (input transformer, output transformer, runner type), an initial state object, and a `Supplier<ProcessorHandler<S, I, O>>` that produces fresh handler instances.

#### Scenario: Registering a factory before newInstance succeeds
- **WHEN** user code calls `FactoryRegistry.registerFactory("echo", settings, new EchoState(), EchoHandler::new)` and then `Flezha.newInstance("my_app")`
- **THEN** the handshake init JSON SHALL include `"echo"` in `factory_ids`, and subsequent `AddProcessor{factory_id:"echo"}` commands SHALL succeed

#### Scenario: Registering a factory after newInstance is rejected
- **WHEN** user code calls `FactoryRegistry.registerFactory(...)` after `Flezha.newInstance(...)` has returned
- **THEN** the SDK SHALL throw an `IllegalStateException`

#### Scenario: Duplicate factory id is rejected
- **WHEN** `registerFactory("echo", ...)` is called twice
- **THEN** the second call SHALL throw an `IllegalArgumentException` naming the duplicate id

### Requirement: ProcessorHandler Interface

The Java SDK SHALL expose a generic `ProcessorHandler<State, Input, Output>` interface with a required `Output process(Input input, State state) throws ProcessorException` method and an optional `default void postCreate(State state) {}` hook. The SDK SHALL wrap user handlers with the registered input/output transformers to adapt them to `ProcessorInput` / `ProcessorResult` at the protocol boundary.

#### Scenario: Handler returning an output value pipes to agent
- **WHEN** the agent sends an input frame with `payload_id = "p1"` to a processor created from a factory whose `output_transformer` is `pipeToAgentText()` and whose handler returns `"hello"`
- **THEN** the SDK SHALL send a frame with `payload_id = "p1"`, message = UTF-8 bytes of `"hello"`, and empty metadata on the secondary stream

#### Scenario: Handler throwing ProcessorException surfaces as a processor error
- **WHEN** `process()` throws `ProcessorException`
- **THEN** the SDK SHALL log the error and NOT send any secondary-stream output for that input, and the compute engine SHALL remain running and continue processing subsequent inputs

#### Scenario: postCreate is called after initial creation and after migration deserialization
- **WHEN** a processor is created via `AddProcessor` (with or without init_values) or via `MigrationDeserialization`
- **THEN** the SDK SHALL invoke `postCreate(state)` exactly once after the state is assigned and before the processor receives any input

### Requirement: ProcessorInput, ProcessorOutput, ProcessorResult Types

The Java SDK SHALL provide sealed types `ProcessorInput` (permits `Binary`, `Text`), `ProcessorOutput` (permits `Binary`, `Text`), and `ProcessorResult` (permits `None`, `Return`, `PipeToAgent`, `PipeToProcessor`) that mirror the Rust SDK and `docs/sdk_protocol_spec.md` §8.3.

#### Scenario: Input type metadata drives ProcessorInput variant
- **WHEN** a primary-stream frame arrives with metadata `input_type = "2"`
- **THEN** the SDK SHALL construct `ProcessorInput.Text(utf8String(messageBytes))` before invoking the input transformer

#### Scenario: Missing input_type metadata defaults to binary
- **WHEN** a primary-stream frame arrives with empty metadata
- **THEN** the SDK SHALL construct `ProcessorInput.Binary(messageBytes)`

#### Scenario: PipeToProcessor routes locally when target exists
- **WHEN** a processor returns `ProcessorResult.PipeToProcessor(List.of(new Route("p2", output, metadata)))` and processor `"p2"` exists locally
- **THEN** the SDK SHALL invoke `p2`'s input pipeline directly (no write to the secondary stream)

#### Scenario: PipeToProcessor falls back to agent when target is not local
- **WHEN** a processor returns `PipeToProcessor` with a target id that does not exist locally
- **THEN** the SDK SHALL write a frame with `payload_id = targetId` to the secondary stream so the agent can route it

### Requirement: Command Dispatch

The Java SDK SHALL implement handlers for every `CommandRequest` variant defined in `compute_engine_commands.proto`: `AssignComputeEngineId`, `ConnectSocket`, `AddProcessor`, `RemProcessor`, `ProcessorState`, `MigrationSerialization`, `MigrationDeserialization`, `ComputeEngineDisconnect`. Each handler MUST produce a `CommandResponse` that echoes the original `reference_id` and sets `status = "success"` or `status = "error"` with populated `errors` per `docs/sdk_protocol_spec.md` §6.

#### Scenario: AddProcessor with init_values deserializes state
- **WHEN** the agent sends `AddProcessor{processor_id:"p1", factory_id:"echo", init_values:"{\"n\":42}"}`
- **THEN** the SDK SHALL create a processor whose state was deserialized from the JSON, invoke `postCreate(state)`, and respond with `status = "success"`

#### Scenario: AddProcessor with unknown factory_id returns error
- **WHEN** the agent sends `AddProcessor{factory_id:"missing"}`
- **THEN** the SDK SHALL respond with `status = "error"` and `errors` containing `"Factory with id missing does not exist"`

#### Scenario: ProcessorState returns JSON-serialized state
- **WHEN** the agent sends `ProcessorState{processor_id:"p1"}` and `p1` exists
- **THEN** the SDK SHALL respond with `status = "success"` and `raw_data` equal to the Jackson-serialized JSON bytes of `p1`'s state

#### Scenario: MigrationSerialization removes the processor
- **WHEN** the agent sends `MigrationSerialization{processor_id:"p1"}` and `p1` exists
- **THEN** the SDK SHALL respond with `status = "success"` and `raw_data` containing `p1`'s JSON-serialized state, AND `p1` SHALL be removed from the processor registry

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

### Requirement: Worker Pool and Processor Runners

The Java SDK SHALL provide two processor runner strategies, matching the Rust SDK: `ProcessorRunner.THREAD_POOL` (default, shared bounded executor sized by `FLEZHA_WORKER_POOL_SIZE` with default = `Runtime.availableProcessors()`, sticky per-processor ordering) and `ProcessorRunner.THREAD_SPAWN` (one virtual thread per invocation). The selected runner is stored on `ProcessorSettings`.

#### Scenario: THREAD_POOL preserves per-processor input ordering
- **WHEN** three inputs arrive for the same processor back-to-back with the `THREAD_POOL` runner
- **THEN** the processor's `process()` method SHALL be invoked with inputs in the exact order received, and concurrent invocations of the same processor SHALL NOT overlap

#### Scenario: THREAD_SPAWN runs each invocation on a fresh virtual thread
- **WHEN** multiple inputs arrive for a processor configured with `THREAD_SPAWN`
- **THEN** the SDK SHALL invoke `process()` on a newly started virtual thread per invocation

### Requirement: JNI Bridge to flz_transport

The Java SDK SHALL interact with the transport layer exclusively through a JNI bridge that wraps `flz_transport`'s C FFI. The bridge MUST expose Java-side operations for: opening a transport context with a `FlezhaConfig`, connecting (client mode), opening streams, reading and writing stream data, registering a log callback, and destroying the context. All native buffers returned by the bridge MUST be copied into Java `byte[]` / `String` before the native memory is freed.

#### Scenario: Native buffer freed after copy into Java byte[]
- **WHEN** the JNI bridge receives a data callback carrying a native buffer pointer and length
- **THEN** the bridge SHALL allocate a Java `byte[]` of equal length, copy the native bytes into it, invoke the Java-side handler with the `byte[]`, and ensure the native buffer is freed (or returned to `flz_transport` ownership) before control returns to the native caller

#### Scenario: flezha_version mismatch is logged
- **WHEN** the SDK starts and `flezha_version()` returns a string that does not equal the SDK's bundled expected version
- **THEN** the SDK SHALL log a WARNING naming both versions and continue startup
