# Flezha SDK Protocol Specification

**Version:** 1.0
**Status:** Living document

---

## Table of Contents

1. [Overview](#1-overview)
2. [Transport Layer](#2-transport-layer)
3. [Binary Framing Protocol](#3-binary-framing-protocol)
4. [Protobuf Command Protocol](#4-protobuf-command-protocol)
5. [Connection Lifecycle](#5-connection-lifecycle)
6. [Command Reference](#6-command-reference)
7. [Data Plane (Stream I/O)](#7-data-plane-stream-io)
8. [Recommended SDK Public API](#8-recommended-sdk-public-api)
9. [Worked Examples](#9-worked-examples)
10. [Error Handling](#10-error-handling)

---

## 1. Overview

### What is Flezha?

Flezha is a distributed stream-processing platform. The **Agent** (an Elixir/OTP application) orchestrates work across one or more **Compute Engines** (SDK processes). Each SDK process connects to the agent over Unix domain sockets, registers processor factories, and executes data-processing logic on behalf of the agent.

### Architecture

```
+-------------------+         Unix Domain Sockets         +-------------------+
|                   |  Control Channel (Protobuf/binary)  |                   |
|   Flezha Agent    | <=================================> |   SDK Process     |
|   (Elixir/OTP)    |                                     |  (Compute Engine) |
|                   |  Primary Streams  (binary frames)   |                   |
|                   | ─────────────────────────────────> |                   |
|                   |  Secondary Streams (binary frames)  |                   |
|                   | <───────────────────────────────── |                   |
+-------------------+                                     +-------------------+
```

### Two Communication Layers

| Layer | Purpose | Transport | Encoding |
|-------|---------|-----------|----------|
| **Control Plane** | Handshake, command dispatch, lifecycle management | Control socket | Protobuf messages wrapped in binary framing |
| **Data Plane** | Processor input/output streaming | Primary + secondary stream sockets | Raw bytes wrapped in binary framing |

---

## 2. Transport Layer

### Socket Type

All sockets are **Unix Domain Sockets** using `SOCK_STREAM` (TCP-like, reliable, ordered byte streams).

### Socket File Paths

The base path for all socket files is determined by:

| Source | Value |
|--------|-------|
| Environment variable | `FLEZHA_SOCKET_FILES_PATH` |
| Default | `app_data/sockets/` |

Trailing slashes are stripped before path construction.

### Socket Types

There are three socket types. In all cases, the **agent listens** and the **SDK connects**.

#### Control Socket (shared)

```
{base}/compute_engine_control_channel.sock
```

A single control socket serves all SDK processes. Each SDK connects here to initiate the handshake.

#### Primary Stream Sockets (per compute engine)

```
{base}/primary_stream_{compute_engine_id}.sock
```

The agent creates a listening socket for each primary stream after the SDK has been assigned an ID. The SDK connects to these sockets. Data flows **agent -> SDK** (the agent sends processor input to the SDK).

#### Secondary Stream Sockets (per compute engine)

```
{base}/secondary_stream_{compute_engine_id}.sock
```

The agent creates a listening socket for each secondary stream. The SDK connects. Data flows **SDK -> agent** (the SDK sends processor output back to the agent).

---

## 3. Binary Framing Protocol

All messages on all sockets (control and stream) use the same binary framing format. This format wraps both Protobuf-encoded control commands and raw data-plane payloads.

### Frame Layout

```
Offset  Size     Field            Description
──────  ──────   ──────────────   ─────────────────────────────────────────
0       32       Payload ID       UTF-8 string, right-padded with spaces (0x20)
32      4        Payload Size     uint32, big-endian
36      4        Message Size     uint32, big-endian
40      M        Message          {Message Size} bytes of message data
40+M    2        Metadata Size    uint16, big-endian
42+M    D        Metadata         {Metadata Size} bytes of serialized metadata
```

**Total frame size** = 32 + 4 + Payload Size = 36 + Payload Size bytes

### Payload Size Calculation

```
Payload Size = 4 (message_size field)
             + Message Size
             + 2 (metadata_size field)
             + Metadata Size
```

### Payload ID

- Exactly 32 bytes, UTF-8 encoded.
- If the logical ID is shorter than 32 bytes, it is right-padded with ASCII space characters (0x20).
- If longer than 32 bytes, it is truncated to 32 bytes.
- When reading, trim trailing spaces to recover the logical ID.

Well-known payload IDs:

| Payload ID | Context |
|------------|---------|
| `connect_control_channel` | Handshake init message (SDK -> agent) |
| `control_command` | Protobuf command messages (agent -> SDK) |
| `{processor_id}` | Stream I/O data (both directions) |

### Metadata Serialization

Metadata is a key-value map where both keys and values are UTF-8 strings. It is serialized as a flat sequence of entries:

```
For each (key, value) pair:
  [key_length: 1 byte (uint8)] [value_length: 1 byte (uint8)] [key: key_length bytes] [value: value_length bytes]
```

- Maximum key size: 255 bytes (limited by 1-byte length prefix).
- Maximum value size: 255 bytes (limited by 1-byte length prefix).
- An empty metadata map serializes to 0 bytes (metadata_size = 0).
- Iteration order of entries is not guaranteed.

### Frame Buffering

Because Unix domain sockets are stream-oriented, frames may be split across multiple `read()` calls. SDK implementations **must** handle:

1. **Incomplete headers**: fewer than 36 bytes received; buffer and wait for more data.
2. **Incomplete payloads**: header received but fewer than `payload_size` bytes available; buffer and wait.
3. **Multiple frames in one read**: a single `read()` may contain more than one complete frame; parse all of them.

The Elixir agent uses a state-machine buffer with three states:
- `{:empty, <<>>}` -- no partial data buffered
- `{:incomplete_header, buffer}` -- partial header buffered
- `{:incomplete_payload, payload_id, payload_size, buffer}` -- header complete, partial payload buffered

SDK implementations should adopt a similar approach.

---

## 4. Protobuf Command Protocol

### Proto Definition

All control-plane commands use Protocol Buffers v3 (`proto3`). The full `.proto` file:

```protobuf
syntax = "proto3";

package compute_engine_commands;

message CommandRequest {
    string reference_id = 1;

    oneof command {
        AssignComputeEngineId assign_compute_engine_id = 2;
        ConnectSocket connect_socket = 3;
        AddProcessor add_processor = 4;
        RemProcessor rem_processor = 5;
        MigrationSerialization migration_serialization = 6;
        MigrationDeserialization migration_deserialization = 7;
        ProcessorState processor_state = 8;
        ComputeEngineDisconnect compute_engine_disconnect = 9;
    }
}

message CommandResponse {
    string reference_id = 1;
    string status = 2;
    repeated string errors = 3;
    bytes raw_data = 4;
}

message AssignComputeEngineId {
    string compute_engine_id = 1;
}

message ConnectSocket {
    uint32 sockets_count = 1;
}

message AddProcessor {
    string processor_id = 1;
    string factory_id = 2;
    string init_values = 3;
}

message RemProcessor {
    string processor_id = 1;
}

message MigrationSerialization {
    string processor_id = 1;
}

message MigrationDeserialization {
    string processor_id = 1;
    string factory_id = 2;
    bytes serialized_state = 3;
}

message ProcessorState {
    string processor_id = 1;
}

message ComputeEngineDisconnect {}
```

### Request/Response Pattern

- **Direction**: The agent sends `CommandRequest` messages to the SDK.
- **Correlation**: Each `CommandRequest` contains a `reference_id` (an opaque string). The SDK must echo this `reference_id` in the corresponding `CommandResponse`.
- **Framing**: Both `CommandRequest` and `CommandResponse` are Protobuf-encoded, then wrapped in the binary framing format (Section 3).
  - The agent sends commands with payload_id = `"control_command"`.
  - The SDK sends responses using the **same payload_id** that was received (which is `"control_command"`).
- **Empty metadata**: Control-plane messages use empty metadata (`metadata_size = 0`).

### Encoding a Command for Transmission

```
1. Serialize the Protobuf message to bytes
2. Wrap in binary frame:
   - payload_id = "control_command" (or the received payload_id for responses)
   - message = protobuf bytes
   - metadata = {} (empty)
3. Write the complete frame to the control socket
```

---

## 5. Connection Lifecycle

This section describes the complete connection sequence from SDK startup to shutdown.

### Step-by-Step Flow

```
SDK                                        Agent
 │                                           │
 │──── 1. TCP connect to control socket ────>│
 │                                           │
 │──── 2. Send handshake init message ──────>│
 │     payload_id: "connect_control_channel" │
 │     message: JSON                         │
 │                                           │
 │                      3. Agent processes ───│
 │                         handshake          │
 │                                           │
 │<─── 4. AssignComputeEngineId ────────────│
 │     (CommandRequest in binary frame)      │
 │                                           │
 │──── 5. CommandResponse (success) ────────>│
 │                                           │
 │<─── 6. ConnectSocket ────────────────────│
 │     (CommandRequest with sockets_count)   │
 │                                           │
 │     7. SDK connects to stream sockets ────│
 │        primary_stream_{id}.sock (×N)      │
 │        secondary_stream_{id}.sock (×N)    │
 │                                           │
 │──── 8. CommandResponse (success) ─────────>│
 │                                           │
 │         ═══ READY ═══                     │
 │                                           │
 │<─── 9. Commands (AddProcessor, etc.) ────│
 │──── 9. CommandResponse ──────────────────>│
 │                                           │
 │        ... stream I/O active ...          │
 │                                           │
 │<─── 10. ComputeEngineDisconnect ─────────│
 │──── 10. CommandResponse (success) ───────>│
 │                                           │
 │     11. SDK closes socket ────────────────│
 │                                           │
```

### Detailed Steps

#### Step 1: Connect to Control Socket

The SDK opens a Unix domain socket connection to:
```
{FLEZHA_SOCKET_FILES_PATH}/compute_engine_control_channel.sock
```

#### Step 2: Send Handshake Init Message

The SDK sends a single binary frame:

- **Payload ID**: `"connect_control_channel"`
- **Message**: JSON-encoded object:
  ```json
  {
    "namespace": "<namespace_string>",
    "factory_ids": ["<factory_id_1>", "<factory_id_2>", ...]
  }
  ```
  - `namespace`: A string identifying the logical group this compute engine belongs to.
  - `factory_ids`: List of all registered processor factory IDs in this SDK process.
- **Metadata**: empty

#### Step 3: Agent Processes Handshake

The agent:
1. Reads the frame from the control socket (with a 5-second timeout).
2. Parses the JSON to extract `namespace` and `factory_ids`.
3. Generates a unique `compute_engine_id`.
4. Starts supervisor trees for the new compute engine (control unit, stream listeners, etc.).
5. Starts or joins the namespace hub.

#### Step 4: Agent Sends `AssignComputeEngineId`

The agent sends a `CommandRequest` containing:
```protobuf
AssignComputeEngineId {
    compute_engine_id: "<generated_id>"
}
```
This is Protobuf-encoded, then wrapped in a binary frame with payload_id `"control_command"`.

#### Step 5: SDK Responds

The SDK:
1. Stores the assigned `compute_engine_id`.
2. Sets its status to `Running`.
3. Sends a `CommandResponse`:
   ```protobuf
   CommandResponse {
       reference_id: "<echoed_reference_id>",
       status: "success",
       errors: [],
       raw_data: <empty>
   }
   ```

#### Step 6: Agent Sends `ConnectSocket`

The agent sends a `CommandRequest` containing:
```protobuf
ConnectSocket {
    sockets_count: <N>
}
```
Where `N` is the number of concurrent stream channel pairs. By default, `N` equals the agent's online scheduler count (CPU cores), configurable via `FLEZHA_CONCURRENT_CHANNEL_COUNT`.

#### Step 7: SDK Connects to Stream Sockets

For each index `i` in `0..sockets_count`, the SDK:

1. Connects to `{base}/primary_stream_{compute_engine_id}.sock`
   - Sets the socket to **non-blocking** mode.
   - Registers it for read polling (e.g., with `mio`/`epoll`/`kqueue`).
   - This socket receives input data from the agent.

2. Connects to `{base}/secondary_stream_{compute_engine_id}.sock`
   - This socket is used to send output data back to the agent.
   - Stored for later use by processors.

#### Step 8: SDK Responds

The SDK sends a `CommandResponse`:
```protobuf
CommandResponse {
    reference_id: "<echoed_reference_id>",
    status: "success",
    errors: [],
    raw_data: <empty>
}
```

#### Step 9: Command Loop

The control channel enters a steady-state loop. The agent may send any of the commands documented in Section 6. The SDK processes each command and replies with a `CommandResponse`.

#### Step 10: Disconnect

The agent sends a `ComputeEngineDisconnect` command. The SDK:
1. Sets its status to `Stopped`.
2. Sends a `CommandResponse` with status `"success"`.
3. The agent closes the control socket after receiving the response.
4. The SDK should also shut down its stream sockets.

---

## 6. Command Reference

All commands are sent as `CommandRequest` messages from the agent to the SDK. The SDK responds with `CommandResponse`.

### AssignComputeEngineId

| Field | Value |
|-------|-------|
| Direction | Agent -> SDK |
| Proto message | `AssignComputeEngineId { compute_engine_id: string }` |
| Behavior | SDK stores the assigned compute engine ID and sets status to `Running` |
| Response status | `"success"` |
| Response raw_data | empty |

### ConnectSocket

| Field | Value |
|-------|-------|
| Direction | Agent -> SDK |
| Proto message | `ConnectSocket { sockets_count: uint32 }` |
| Behavior | SDK creates `sockets_count` pairs of stream connections (primary + secondary) |
| Response status | `"success"` |
| Response raw_data | empty |

### AddProcessor

| Field | Value |
|-------|-------|
| Direction | Agent -> SDK |
| Proto message | `AddProcessor { processor_id: string, factory_id: string, init_values: string }` |
| Behavior | SDK creates a new processor instance from the named factory. If `init_values` is non-empty, it is parsed as JSON and deserialized into the processor's state via `json_deserialize_state()`. Then `post_alter()` is called. |
| Response status | `"success"` on success, `"error"` on failure |
| Response raw_data | empty |
| Possible errors | Factory not found; JSON parse failure; deserialization failure |

### RemProcessor

| Field | Value |
|-------|-------|
| Direction | Agent -> SDK |
| Proto message | `RemProcessor { processor_id: string }` |
| Behavior | SDK removes the processor with the given ID from its processor registry |
| Response status | `"success"` on success, `"error"` if processor not found |
| Response raw_data | empty |
| Possible errors | `"Processor not found"` |

### ProcessorState

| Field | Value |
|-------|-------|
| Direction | Agent -> SDK |
| Proto message | `ProcessorState { processor_id: string }` |
| Behavior | SDK serializes the processor's current state using `bin_serialize_state()` and returns the JSON bytes |
| Response status | `"success"` on success, `"error"` if processor not found |
| Response raw_data | JSON-serialized processor state bytes |
| Possible errors | `"Processor with id {id} not found"` |

### MigrationSerialization

| Field | Value |
|-------|-------|
| Direction | Agent -> SDK |
| Proto message | `MigrationSerialization { processor_id: string }` |
| Behavior | SDK serializes the processor's state using `json_serialize_state()`, then **removes** the processor from the registry. The serialized state is returned so the agent can migrate it to another compute engine. |
| Response status | `"success"` |
| Response raw_data | JSON-serialized processor state bytes |

### MigrationDeserialization

| Field | Value |
|-------|-------|
| Direction | Agent -> SDK |
| Proto message | `MigrationDeserialization { processor_id: string, factory_id: string, serialized_state: bytes }` |
| Behavior | SDK creates a new processor from the factory, then deserializes the provided `serialized_state` into it via `json_deserialize_state()`. |
| Response status | `"success"` |
| Response raw_data | empty |

### ComputeEngineDisconnect

| Field | Value |
|-------|-------|
| Direction | Agent -> SDK |
| Proto message | `ComputeEngineDisconnect {}` (no fields) |
| Behavior | SDK sets status to `Stopped`. After sending the response, the SDK should close the control socket. |
| Response status | `"success"` |
| Response raw_data | empty |

---

## 7. Data Plane (Stream I/O)

Once stream sockets are established (after `ConnectSocket`), data flows between the agent and SDK:

### Direction

| Socket Type | Direction | Purpose |
|-------------|-----------|---------|
| Primary stream | Agent -> SDK | Agent sends input data TO SDK processors |
| Secondary stream | SDK -> Agent | SDK sends processor output BACK to agent |

### Framing

All stream messages use the same binary framing format described in Section 3.

- **Payload ID**: The `processor_id` of the target processor.
- **Message**: The input/output data bytes.
- **Metadata**: Key-value pairs conveying type information and other attributes.

### Metadata Conventions

| Key | Values | Description |
|-----|--------|-------------|
| `input_type` | `"1"` = binary, `"2"` = text | Indicates the type of the input data. Defaults to `"1"` if absent. |

### Input Processing (Primary Streams)

When the SDK receives a frame on a primary stream:

1. Extract `processor_id` from the payload ID.
2. Read `input_type` from metadata (default `"1"`).
3. Construct a `ProcessorInput`:
   - `"1"` -> `Binary(message_bytes)`
   - `"2"` -> `Text(utf8_string(message_bytes))`
4. Dispatch to the processor's `run()` method via the worker pool.

### Output Processing (Secondary Streams)

When a processor returns a `PipeToAgent` result:

1. For each `(output, metadata)` tuple in the result:
   - Construct a binary frame with payload_id = `processor_id`.
   - Serialize the output:
     - `Text(s)` -> string bytes
     - `Binary(b)` -> raw bytes
   - Write the frame to the secondary stream socket.

### Non-blocking I/O

Primary stream sockets should be set to **non-blocking** mode. The SDK should use an event-driven polling mechanism (e.g., `epoll` on Linux, `kqueue` on macOS) to efficiently wait for input across all primary stream sockets simultaneously.

The Rust SDK uses `mio` with `Poll` for this purpose:
- Each primary stream is registered with a unique `Token(index)`.
- The poll loop calls `extract_payload_non_blocking()` which returns `None` on `WouldBlock`.
- Multiple frames may arrive on a single `poll()` wake-up; drain all available frames before re-polling.

### Backpressure on Secondary Streams

When writing output to a secondary stream, the write may block (or return `WouldBlock` in non-blocking mode). The SDK should:

1. Attempt `write_all()`.
2. If `WouldBlock`, register the socket for writable events and retry when the socket becomes writable.
3. Deregister after successful write.

### PipeToProcessor

When a processor returns `PipeToProcessor`, the SDK routes output to another processor locally:

1. Check if the target processor exists locally.
2. If yes, convert the output to input and invoke the target processor's `run()`.
3. If no, fall back to `pipe_to_agent()` — send it to the agent via the secondary stream so the agent can route it.

---

## 8. Recommended SDK Public API

This section describes the recommended public-facing API surface for SDK implementations. Language-specific idioms may vary, but the semantics should be preserved.

### 8.1 Factory Registration

```
register_factory(factory_id, settings, initial_state, handler_generator)
```

- **`factory_id`**: `string` — A globally unique identifier for this processor type.
- **`settings`**: Configuration object containing:
  - `input_transformer` — Converts raw `ProcessorInput` to the handler's expected input type.
  - `output_transformer` — Converts the handler's output type to `ProcessorResult`.
  - `processor_runner` — Execution strategy: `ThreadPool` (default, sequential per processor) or `ThreadSpawn` (new thread per invocation).
- **`initial_state`**: The default state for new processor instances. Must be serializable to/from JSON.
- **`handler_generator`**: A factory function `() -> ProcessorHandler` that creates new handler instances.

Factories **must** be registered before calling `new()`.

### 8.2 Processor Interface

SDK implementations must support processors that conform to this interface:

```
interface Processor:
    run(input: ProcessorInput) -> Result<ProcessorResult, ProcessorError>
    json_serialize_state() -> bytes
    json_deserialize_state(data: bytes) -> void
    get_factory_id() -> string
    post_alter() -> void
```

| Method | Description |
|--------|-------------|
| `run(input)` | Execute the processor logic on the given input. May mutate internal state. |
| `json_serialize_state()` | Serialize the processor's current state to JSON bytes. Used for migration and state inspection. |
| `json_deserialize_state(data)` | Restore the processor's state from JSON bytes. Used during `AddProcessor` (with init_values) and `MigrationDeserialization`. |
| `get_factory_id()` | Return the factory ID that created this processor. |
| `post_alter()` | Called after state changes (initial creation, init_values application, migration). Use for derived state recomputation. |

### 8.3 Processor Input/Output Types

**ProcessorInput** (received from agent):

| Variant | Description |
|---------|-------------|
| `Binary(bytes)` | Raw binary data |
| `Text(string)` | UTF-8 text data |

**ProcessorOutput** (sent back to agent):

| Variant | Description |
|---------|-------------|
| `Binary(bytes)` | Raw binary data |
| `Text(string)` | UTF-8 text data |

**ProcessorResult** (return value of `run()`):

| Variant | Description |
|---------|-------------|
| `None` | No output; discard silently |
| `Return(output)` | Return output to the caller (local use only, not sent to agent) |
| `PipeToAgent(list<(output, metadata)>)` | Send one or more outputs back to the agent via secondary stream |
| `PipeToProcessor(list<(processor_id, output, metadata)>)` | Route output to another processor (local or via agent) |

### 8.4 ProcessorHandler Interface

The `ProcessorHandler` is the user-facing trait/interface for implementing processing logic:

```
interface ProcessorHandler<State, Input, Output>:
    process(input: Input, state: &mut State) -> Result<Output, ProcessorError>
    post_create(state: &mut State) -> void  // optional, called after creation
```

The SDK wraps this with input/output transformers into a full `Processor` implementation.

### 8.5 SDK Entry Point

```
new(namespace: string) -> SDK
```

1. All factories must be registered before calling `new()`.
2. Connects to the agent's control socket.
3. Sends the handshake init message with the namespace and registered factory IDs.
4. Blocks until `AssignComputeEngineId` is received and processed.
5. Returns the SDK handle.

The returned handle provides:
- `get_id() -> string` — Returns the assigned compute engine ID.
- `get_processors()` — Returns the current set of active processors.

### 8.6 Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `FLEZHA_SOCKET_FILES_PATH` | `app_data/sockets` | Base path for socket files |
| `FLEZHA_CONCURRENT_CHANNEL_COUNT` | `0` (= CPU count) | Number of stream socket pairs (agent-side config) |
| `FLEZHA_WORKER_POOL_SIZE` | CPU count | Number of worker threads in the SDK |

---

## 9. Worked Examples

### 9.1 Handshake Init Message (Hex Dump)

SDK sends the initial handshake for namespace `"my_app"` with one factory `"echo"`.

**JSON message**: `{"factory_ids":["echo"],"namespace":"my_app"}`

**Logical fields**:
- Payload ID: `"connect_control_channel"` (25 chars, padded to 32)
- Message: `{"factory_ids":["echo"],"namespace":"my_app"}` (44 bytes)
- Metadata: empty

**Binary construction**:
```
Payload ID (32 bytes):
  63 6F 6E 6E 65 63 74 5F 63 6F 6E 74 72 6F 6C 5F   connect_control_
  63 68 61 6E 6E 65 6C 20 20 20 20 20 20 20 20 20   channel·········

Payload Size (4 bytes): message_size(4) + message(44) + metadata_size(2) + metadata(0) = 50
  00 00 00 32

Message Size (4 bytes): 44
  00 00 00 2C

Message (44 bytes):
  7B 22 66 61 63 74 6F 72 79 5F 69 64 73 22 3A 5B   {"factory_ids":[
  22 65 63 68 6F 22 5D 2C 22 6E 61 6D 65 73 70 61   "echo"],"namespa
  63 65 22 3A 22 6D 79 5F 61 70 70 22 7D             ce":"my_app"}

Metadata Size (2 bytes): 0
  00 00

Total frame: 32 + 4 + 50 = 86 bytes
```

### 9.2 AssignComputeEngineId Command (Hex Dump)

Agent sends `AssignComputeEngineId` with `compute_engine_id = "ce_001"` and `reference_id = "#Ref<0.1.2.3>"`.

**Protobuf encoding** of `CommandRequest`:
```protobuf
field 1 (reference_id): wire type 2 (length-delimited), value = "#Ref<0.1.2.3>"
field 2 (assign_compute_engine_id): wire type 2 (length-delimited)
  field 1 (compute_engine_id): wire type 2 (length-delimited), value = "ce_001"
```

Protobuf bytes (25 bytes):
```
0A 0D 23 52 65 66 3C 30 2E 31 2E 32 2E 33 3E     reference_id (tag + len + 13 chars)
12 08 0A 06 63 65 5F 30 30 31                       assign_compute_engine_id (tag + len + nested)
```

**Binary frame**:
```
Payload ID (32 bytes):
  63 6F 6E 74 72 6F 6C 5F 63 6F 6D 6D 61 6E 64 20   control_command·
  20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20   ················

Payload Size (4 bytes): 4 + 25 + 2 + 0 = 31
  00 00 00 1F

Message Size (4 bytes): 25
  00 00 00 19

Message (25 bytes): [protobuf bytes above]

Metadata Size (2 bytes): 0
  00 00

Total frame: 32 + 4 + 31 = 67 bytes
```

### 9.3 Stream I/O Message (Hex Dump)

Agent sends text input `"hello"` to processor `"proc_42"` with metadata `{input_type: "2"}`.

**Logical fields**:
- Payload ID: `"proc_42"` (7 chars, padded to 32)
- Message: `"hello"` (5 bytes)
- Metadata: `{"input_type": "2"}`

**Metadata serialization**:
```
key_len=10, val_len=1, key="input_type", value="2"
  0A 01 69 6E 70 75 74 5F 74 79 70 65 32
  (13 bytes total)
```

**Binary frame**:
```
Payload ID (32 bytes):
  70 72 6F 63 5F 34 32 20 20 20 20 20 20 20 20 20   proc_42·········
  20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20   ················

Payload Size (4 bytes): 4 + 5 + 2 + 13 = 24
  00 00 00 18

Message Size (4 bytes): 5
  00 00 00 05

Message (5 bytes):
  68 65 6C 6C 6F                                     hello

Metadata Size (2 bytes): 13
  00 0D

Metadata (13 bytes):
  0A 01 69 6E 70 75 74 5F 74 79 70 65 32

Total frame: 32 + 4 + 24 = 60 bytes
```

### 9.4 Full Processor Lifecycle Sequence

```
Time  Direction    Description
────  ─────────    ───────────────────────────────────────────────
 t0   SDK->Agent   Connect to control socket
 t1   SDK->Agent   Handshake: {"namespace":"my_app","factory_ids":["echo"]}
 t2   Agent->SDK   CommandRequest: AssignComputeEngineId{compute_engine_id:"ce_001"}
 t3   SDK->Agent   CommandResponse: {ref_id:"...", status:"success"}
 t4   Agent->SDK   CommandRequest: ConnectSocket{sockets_count:4}
 t5   SDK          Connect to primary_stream_ce_001.sock (×4, non-blocking)
      SDK          Connect to secondary_stream_ce_001.sock (×4)
 t6   SDK->Agent   CommandResponse: {ref_id:"...", status:"success"}
 t7   Agent->SDK   CommandRequest: AddProcessor{processor_id:"p1", factory_id:"echo", init_values:""}
 t8   SDK          Create processor "p1" from factory "echo"
 t9   SDK->Agent   CommandResponse: {ref_id:"...", status:"success"}
t10   Agent->SDK   [primary stream] payload_id:"p1", message:"hello", metadata:{input_type:"2"}
t11   SDK          Processor "p1" runs: input="hello" -> output="hello"
t12   SDK->Agent   [secondary stream] payload_id:"p1", message:"hello", metadata:{}
t13   Agent->SDK   CommandRequest: RemProcessor{processor_id:"p1"}
t14   SDK          Remove processor "p1"
t15   SDK->Agent   CommandResponse: {ref_id:"...", status:"success"}
t16   Agent->SDK   CommandRequest: ComputeEngineDisconnect{}
t17   SDK->Agent   CommandResponse: {ref_id:"...", status:"success"}
t18   SDK          Close all sockets, shut down
```

---

## 10. Error Handling

### Command Errors

When a command handler encounters an error:

1. The `CommandResponse` status is set to `"error"`.
2. The `errors` field contains a list of human-readable error strings.
3. The `raw_data` field is typically empty on error.

Example:
```protobuf
CommandResponse {
    reference_id: "<echoed_ref>",
    status: "error",
    errors: ["Factory with id unknown_factory does not exist"],
    raw_data: <empty>
}
```

### Connection Close Detection

- **TCP close**: The agent detects a closed control socket via `:tcp_closed` messages. This triggers compute engine disconnect handling (preservation, migration, or rerouting depending on namespace state).
- **SDK side**: Read errors (0 bytes read, `BrokenPipe`) on the control socket indicate the agent has disconnected.

### Incomplete Frame Handling

SDK implementations **must not** discard partial data. If a read returns fewer bytes than expected:

1. Buffer the bytes.
2. Track the parse state (incomplete header vs incomplete payload).
3. On the next read, prepend the buffered bytes and continue parsing.

If a frame fails to parse (corrupt data), the SDK should:
1. Log the error.
2. Discard the corrupt frame.
3. Attempt to continue parsing from the remaining bytes.

### Reconnection

The current protocol does not define automatic reconnection. If the control socket is closed, the SDK process should exit. The agent manages compute engine lifecycle, including keepalive timeouts (configurable via `FLEZHA_OPERATOR_KEEPALIVE`, default 30000ms) to preserve state for potential reconnection by a new SDK process.
