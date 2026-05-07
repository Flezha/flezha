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

Flezha is a distributed stream-processing platform. The **Agent** (an Elixir/OTP application) orchestrates work across one or more **Compute Engines** (SDK processes). Each SDK process connects to the agent over a configurable transport (UDS or QUIC), registers processor factories, and executes data-processing logic on behalf of the agent.

### Architecture

```
+-------------------+      UDS or QUIC Transport          +-------------------+
|                   |  Control Channel (Protobuf/binary)  |                   |
|   Flezha Agent    | <=================================> |   SDK Process     |
|   (Elixir/OTP)    |                                     |  (Compute Engine) |
|                   |  Primary Streams  (binary frames)   |                   |
|                   | ─────────────────────────────────> |                   |
|                   |  Secondary Streams (binary frames)  |                   |
|                   | <───────────────────────────────── |                   |
+-------------------+                                     +-------------------+
```

The agent can run both UDS and QUIC transport servers simultaneously (configured via `FLEZHA_TRANSPORTS`). Each compute engine connects using one transport type, determined by its `FLEZHA_TRANSPORT` environment variable.

### Two Communication Layers

| Layer | Purpose | Transport | Encoding |
|-------|---------|-----------|----------|
| **Control Plane** | Handshake, command dispatch, lifecycle management | Control socket | Protobuf messages wrapped in binary framing |
| **Data Plane** | Processor input/output streaming | Primary + secondary stream sockets | Raw bytes wrapped in binary framing |

---

## 2. Transport Layer

Flezha supports two transport types. Multiple transports can be active simultaneously — each compute engine chooses its transport via the `FLEZHA_TRANSPORT` env var.

### UDS (Unix Domain Sockets)

All sockets are `SOCK_STREAM` (TCP-like, reliable, ordered byte streams).

#### Socket File Paths

The base path for all socket files is determined by:

| Source | Value |
|--------|-------|
| Environment variable | `FLEZHA_SOCKET_FILES_PATH` |
| Default | `app_data/sockets/` |

Trailing slashes are stripped before path construction.

#### Socket Types

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

### QUIC

When `FLEZHA_TRANSPORT=quic`, the SDK connects to the agent via a single QUIC connection (UDP). All communication multiplexes over bidirectional QUIC streams within that connection.

| Setting | Env Variable | Default |
|---------|-------------|---------|
| Server bind address | `FLEZHA_QUIC_ADDRESS` | `0.0.0.0:4433` |
| CA certificate | `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` |
| Server certificate | `FLEZHA_SERVER_CERT_PATH` | `app_data/certs/server.pem` |
| Server key | `FLEZHA_SERVER_KEY_PATH` | `app_data/certs/server.key` |

TLS certificates are auto-generated on first startup if absent.

#### QUIC Stream Mapping

- **First bidi stream** opened by the SDK = control channel (no prefix bytes, same framing as UDS)
- **Subsequent bidi streams** = data streams, prefixed with a 2-byte identifier:
  - Byte 0: stream type (`0x01` = primary, `0x02` = secondary)
  - Byte 1: stream index (round-robin channel selector)

The prefix is consumed by the NIF driver and not visible to the framing layer.

### Multi-Transport Support

The agent can listen on both UDS and QUIC simultaneously by setting `FLEZHA_TRANSPORTS=uds,quic` (or in test mode, both are enabled by default). Each compute engine independently chooses its transport. The transport type is threaded through the supervisor chain so that UDS compute engines get StreamAcceptors and QUIC ones rely on the Transport.Server for data stream routing.

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

#### Reserved metadata keys

| Key | Direction | Description |
|---|---|---|
| `input_type` | Agent → SDK | Wire format of the entry-channel payload — `"1"` for binary (default), `"2"` for UTF-8 text. The SDK uses this to convert the payload bytes to the entry channel's `EntryChannelType` (`Vec<u8>` or `String`). |
| `channel` | Agent → SDK | Names the Relay's entry channel that should receive this input. If absent, the SDK falls back to the Relay's first declared input channel. |
| `channel` | SDK → Agent | Set by `ShardCtx::emit` on plugin-bound channels. Plugin providers (e.g. `Flezha.Plugin.Provider.Kafka.Producer`) match on this key via their `:channel` opt to gate which channel's outputs they consume. |

The `channel` metadata key replaces the pre-`relay-mvp-rust` `plugin_group: u8` integer key. v1 sends the channel name as a UTF-8 string; an integer fast-path is reserved for a future change.

#### Per-Partition mailbox is bounded; on full, the channel's `OverflowPolicy` applies

The SDK's per-Partition mailbox is a fixed-capacity `crossbeam::queue::ArrayQueue` allocated at relay startup. When the agent dispatches a frame to a Relay whose targeted Partition's mailbox is full, the SDK applies the channel's declared `OverflowPolicy`: `Block` (producer parks until the consumer drains an item, then retries), `DropNewest` (frame discarded, counter incremented), or `DropOldest` (consumer's mailbox evicts its oldest item and accepts the new one, counter incremented). The default entry-channel policy is `DropNewest` — **the SDK silently drops frames when a Relay's entry mailbox is saturated**, and the wire-reader thread continues servicing sibling Relays sharing the same physical connection. Operators wanting strict end-to-end ordering should opt into `OverflowPolicy::Block` per channel and accept the documented co-tenancy hazard (a parked wire reader stops delivering frames to every Relay sharing this connection until the saturated Relay drains). See `docs/relay_architecture.md` "Backpressure and bounded mailboxes" for the full contract — capacity declaration, default policies by channel kind, the per-Partition counter set, and worker-pool sizing rules under `Block`.

#### State migration JSON shape

`MigrationSerialization` / `MigrationDeserialization` / `ProcessorState` / `ProcessorApplyState` carry a JSON blob whose shape is:

- Single-mode shards (no `shard_keys()` declared):
  ```json
  {"<shard_name>": <state>}
  ```
- Static-mode shards (`shard_keys()` non-empty):
  ```json
  {"<shard_name>": {"<shard_key>": <state>, ...}}
  ```

This is a wire-format break vs the pre-`relay-mvp-rust` flat `<state>` shape. State portability across the cutover is out of scope (umbrella Non-Goal 2 / `relay-state-migration`); the hard-cutover policy means no production scenario crosses old-SDK and new-SDK state blobs.

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
        ProcessorMessage processor_message = 10;
        ProcessorApplyState processor_apply_state = 11;
        OperatorShutdownNotification operator_shutdown_notification = 12;
        OperatorShutdownAck operator_shutdown_ack = 13;
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

message ProcessorMessage {
    string processor_id = 1;
    bytes payload = 2;
}

message ProcessorMessageResponse {
    bytes payload = 1;
    string error = 2;
}

message ProcessorApplyState {
    string processor_id = 1;
    string factory_id = 2;
    bytes state_binary = 3;
}

message OperatorShutdownNotification {
    string processor_id = 1;
    bytes serialized_state = 2;
}

message OperatorShutdownAck {
    string processor_id = 1;
}
```

### Request/Response Pattern

- **Direction**: Most `CommandRequest` variants flow agent -> SDK. The two
  exceptions — `OperatorShutdownNotification` and its `OperatorShutdownAck`
  — flow SDK -> agent and agent -> SDK respectively, using the same
  `CommandRequest` envelope and the same `"control_command"` payload_id; the
  receiver inspects the oneof tag to dispatch.
- **Correlation**: Each `CommandRequest` contains a `reference_id` (an opaque string). The SDK must echo this `reference_id` in the corresponding `CommandResponse`. For the SDK -> agent shutdown notification, the agent echoes the same `reference_id` in the `OperatorShutdownAck` it sends back.
- **Framing**: Both `CommandRequest` and `CommandResponse` are Protobuf-encoded, then wrapped in the binary framing format (Section 3).
  - The agent sends commands with payload_id = `"control_command"`.
  - The SDK sends responses using the **same payload_id** that was received (which is `"control_command"`).
  - The SDK sends `OperatorShutdownNotification` with payload_id = `"control_command"`; the agent's `OperatorShutdownAck` reuses the same payload_id.
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

This is an **intentional** disconnect — the SDK should NOT attempt to reconnect.

#### Auto-Reconnection (Unexpected Disconnect)

If the control channel drops unexpectedly (agent crash, container restart, network failure), the SDK detects the disconnection and automatically reconnects:

```
SDK                                        Agent
 │                                           │
 │     ══ Agent goes down ══                 ✗
 │
 │  Control listener detects read error
 │  (BrokenPipe / UnexpectedEof)
 │
 │  SDK clears all state:
 │    - io_streams (dead)
 │    - processors (will be re-added)
 │    - work queues (stale stream refs)
 │    - compute_engine_id → "undefined"
 │
 │  Retry with exponential backoff:
 │    200ms, 400ms, 800ms, ... cap 30s
 │
 │     ══ Agent comes back up ══             │
 │                                           │
 │──── 1. Reconnect to control socket ──────>│
 │──── 2. Send handshake init message ──────>│
 │                                           │
 │     (normal lifecycle resumes from Step 3) │
 │                                           │
 │<─── 4. AssignComputeEngineId (new ID) ───│
 │<─── 6. ConnectSocket ────────────────────│
 │<─── 9. AddProcessor (for each) ──────────│
 │                                           │
 │         ═══ READY ═══                     │
```

**Key behaviors:**
- The SDK distinguishes intentional disconnects (`ComputeEngineDisconnect` command sets status to `Stopped`) from unexpected ones (status is `Running`). Only unexpected disconnects trigger reconnection.
- The agent treats the reconnection as a **new connection** — it assigns a fresh `compute_engine_id` and re-sends the full command sequence.
- Processor factories remain registered in the SDK process; the agent re-creates processor instances via `AddProcessor`.
- In-flight worker work completes (writes to dead streams fail silently); workers return to the pool after reconnection.
- Backoff schedule: 200ms, 400ms, 800ms, 1.6s, 3.2s, 6.4s, 12.8s, 25.6s, capped at 30s.

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

### ProcessorMessage

| Field | Value |
|-------|-------|
| Direction | Agent -> SDK |
| Proto message | `ProcessorMessage { processor_id: string, payload: bytes }` |
| Behavior | SDK locates the processor by id and invokes the factory's `handle_message(payload)` hook. The return value (reply bytes on success, error string on failure) is wrapped into a protobuf-encoded `ProcessorMessageResponse` and written as the outer `CommandResponse.raw_data`. Factories that do not override `handle_message` return the default "unsupported" error — mis-routing is loud, not silent. |
| Response status | `"success"` when the processor was found; `"error"` only when the processor id is unknown |
| Response raw_data | Protobuf-encoded `ProcessorMessageResponse { payload: bytes, error: string }` — an empty `error` means success and the agent reads `payload`; a non-empty `error` means the factory reported a failure and the agent discards `payload` |
| Possible errors | `"Processor with id {id} not found"` (CommandResponse.errors); factory-supplied error string (ProcessorMessageResponse.error) |

The agent SHALL treat a non-empty `ProcessorMessageResponse.error` as an
error condition regardless of `payload` contents. Used by the
[processor-message-handlers](../docs/processor_message_handlers.md)
mechanism — specifically the first-party
`Flezha.Plugin.Provider.MessageForward` provider — to bridge
Elixir-side request/reply envelopes to the SDK factory. The caller side
is expected to set its own timeout; there is no server-side deadline
propagation. v1 is open-auth; scoping is the caller's responsibility.
Payloads are opaque bytes and share the control stream with other
commands — keep them small to avoid head-of-line contention.

### OperatorShutdownNotification

| Field | Value |
|-------|-------|
| Direction | SDK -> Agent (only SDK-initiated CommandRequest variant alongside its ack) |
| Proto message | `OperatorShutdownNotification { processor_id: string, serialized_state: bytes }` |
| When emitted | On graceful SDK shutdown only: SIGTERM, `Drop` of the SDK handle, explicit `disconnect()`. Crash paths (SIGKILL, panic) cannot emit and fall back to last periodic snapshot. |
| Behavior | Per running processor, the SDK serializes via the same per-driver serializer used for `MigrationSerialization` and emits one notification. The agent forwards the binary to the corresponding `Overwatch.state.snapshot` and emits an `OperatorShutdownAck` with the same `reference_id`. |
| Ack timeout | The SDK SHALL wait at most **2 s per processor** for `OperatorShutdownAck` (parallelised across processors). After the timeout, the SDK SHALL exit unconditionally and SHALL NOT block its container's shutdown grace period. |
| Per-processor serializer cap | Capped at the agent's `cross_node_request_timeout` (10 s) ceiling; deployments with tighter container grace periods should override the cap via SDK config. |
| Wire format | CommandRequest envelope, payload_id = `"control_command"`, oneof tag 12. |

### OperatorShutdownAck

| Field | Value |
|-------|-------|
| Direction | Agent -> SDK |
| Proto message | `OperatorShutdownAck { processor_id: string }` |
| Behavior | Emitted by the agent's `ControlChannel` immediately after the snapshot cast is enqueued on the `Overwatch` GenServer (NOT after the cast has been applied — the cast is in the GenServer mailbox by then and applies in microseconds). The agent emits the ack regardless of whether the Overwatch was found: a missing Overwatch (already migrated to a peer agent) still gets an ack so the SDK can proceed to exit. |
| Wire format | CommandRequest envelope, payload_id = `"control_command"`, oneof tag 13, `reference_id` matches the originating notification. |

**Forward/back compat.** Both opcodes are additive. Older agents that do
not recognise tag 12 SHALL log and discard the message via Protox's
unknown-field handling — no crash, no protocol downgrade. Older SDKs
that do not emit tag 12 fall back to today's periodic-snapshot freshness
shipped by `heal-connections-on-restart`. No SDK protocol version bump
is required.

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
2. Connects to the agent's control socket. If the agent is not available, retries with exponential backoff (200ms → 30s cap) until the connection succeeds. This allows compute engines to start before the agent.
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

- **Agent side**: The agent detects a closed control socket via `{:flz_closed, ...}` NIF messages. This triggers compute engine disconnect handling (preservation, migration, or rerouting depending on namespace state).
- **SDK side**: The control listener thread distinguishes transient read errors (`WouldBlock`, `Interrupted`) from fatal ones (`BrokenPipe`, `UnexpectedEof`, `ConnectionReset`). Transient errors are retried; fatal errors trigger auto-reconnection (see Section 5, Auto-Reconnection).

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

The SDK automatically reconnects when the control channel drops unexpectedly. See Section 5 (Auto-Reconnection) for the full reconnection flow.

**Intentional vs unexpected disconnect:** When the agent sends `ComputeEngineDisconnect`, the SDK sets its status to `Stopped` and does **not** reconnect. When the connection drops without this command (agent crash, network failure), the SDK's status is still `Running`, and it enters the reconnection loop with exponential backoff.

**Agent-side keepalive:** The agent preserves compute engine state for a configurable window (default 30000ms, via `FLEZHA_OPERATOR_KEEPALIVE`) after detecting a disconnect. If a new SDK process with the same namespace connects within this window, the agent may migrate preserved processors to the new compute engine.


## Per-Partition State Persistence

The persistence subsystem reserves two data-stream payload-ids and five
control commands. Both directions and all wire shapes are settled here
and implemented in `relay-state-persistence-agent`.

### Data-stream payload-id `partition_state_change`

SDK → agent. Carries one Change record destined for the per-Partition
WAL. Frame layout (matches `Flezha.State.Wire.decode_frame/1` and
`flezha_compute_engine::partition::flusher::build_partition_state_change_frame`):

```
[partition_id_len: u16 BE]
[partition_id_bytes]
[seq:              u64 BE]
[header:           16 B  ]    <- HeaderTemplate::stamp output
[body_len:         u32 BE]
[body]                        <- postcard-encoded Change
[crc32:            u32 BE]    <- crc32 over body
```

`partition_id_bytes` is structured so the agent can route the record to
the right per-Partition WAL file without a side-table mapping:

```
[relay_class_len:   u8   ][relay_class]
[shard_id_len:      u8   ][shard_id]
[partition_key_len: u16 BE][partition_key]
```

The 16-byte header carries `sdk_lang = 1` (Rust) / `2` (Java), SDK
version (BE u32), Shard `SCHEMA_VERSION` (BE u32), `format_id = 1`
(postcard), reserved bytes, and a CRC32 over the body.

### Data-stream payload-id `partition_state_snapshot`

SDK → agent. Same wire shape as `partition_state_change`; `body` is the
postcard-encoded `Self::State` (full snapshot) instead of one Change.
The agent writes the body to `<partition_key>.snap.<seq>` via the
`.tmp → fsync → rename → truncate WAL up to seq` sequence.

### Control command `RestoreInitState` (agent → SDK)

```protobuf
message RestoreInitState {
    bytes partition_id      = 1;
    bytes snapshot_bytes    = 2;
    bytes change_log_bytes  = 3;
}
```

Each record inside `change_log_bytes` uses the agent's on-disk WAL
framing:

```
[seq:           u64 BE]
[len:           u32 BE]    <- = 16 + change_bytes.len()
[header:        16 B  ]
[change_bytes]
[crc32:         u32 BE]    <- crc32 over header + change_bytes
```

The SDK's `Shard::init_state(shard_key, Some(payload))` is invoked with
the payload borrowed; the runtime decodes `snapshot_bytes` (when
non-empty) into `Self::State` and folds the change-log records via
`Shard::apply_change`.

### Control command `RestoreInitStateResponse` (SDK → agent)

```protobuf
message RestoreInitStateResponse {
    bytes  partition_id  = 1;
    uint64 terminal_seq  = 2;
}
```

The agent unblocks the Partition's input streaming after the response
arrives. The next `partition_state_change` is expected to carry
`seq = terminal_seq + 1`.

### Control command `CompactPartition` (agent → SDK)

```protobuf
message CompactPartition {
    bytes partition_id = 1;
}
```

Issued when `wal_size_bytes` exceeds `compact_after_bytes` (per-Shard,
default 64 MiB) or when the operator runs `flz state rotate`. The SDK
clones `Self::State` under the partition's sticky CAS, postcard-
serializes it, ships the bytes via `partition_state_snapshot`, then
replies with:

```protobuf
message CompactPartitionResponse {
    bytes  partition_id        = 1;
    uint64 snapshot_seq        = 2;
    uint64 snapshot_bytes_size = 3;
}
```

### Control command `PartitionMailboxStatus` (agent → SDK)

```protobuf
message PartitionMailboxStatus {
    bytes  partition_id = 1;
    string status       = 2;   // "full" | "open"
}
```

One-way notification (no `CommandResponse`). The agent's `WalWriter`
emits `"full"` when `pending_bytes` exceeds `wal_high_watermark_bytes`
(default 16 MiB) and `"open"` when it falls below
`wal_low_watermark_bytes` (default `high / 2`). The SDK's per-Partition
mailbox observer translates the signal into the channel's declared
`OverflowPolicy`.

### Plugin metadata: `flezha.dedup_key`

Reserved metadata key on plugin-bound emit frames. The SDK runtime
stamps `flezha.dedup_key = "<relay_class>/<shard_id>/<partition_key>/<seq>"`
on every emit issued during a `state.apply(...)`-driven `Shard::run`.
Plugin providers (e.g. `Flezha.Plugin.Provider.Kafka`) honor it as the
sink-side dedup key; user-supplied `flezha.dedup_key` is silently
dropped by the SDK runtime.

