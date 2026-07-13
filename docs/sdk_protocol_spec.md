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

Flezha is a distributed stream-processing platform. The **Controller** (an Elixir/OTP application) orchestrates work across one or more **Compute Engines** (SDK processes). Each SDK process connects to the controller over a configurable transport (UDS or QUIC), registers operative factories, and executes data-processing logic on behalf of the controller.

### Architecture

```
+-------------------+      UDS or QUIC Transport          +-------------------+
|                   |  Control Channel (Protobuf/binary)  |                   |
|   Flezha Controller    | <=================================> |   SDK Process     |
|   (Elixir/OTP)    |                                     |  (Compute Engine) |
|                   |  Primary Streams  (binary frames)   |                   |
|                   | ─────────────────────────────────> |                   |
|                   |  Secondary Streams (binary frames)  |                   |
|                   | <───────────────────────────────── |                   |
+-------------------+                                     +-------------------+
```

The controller can run both UDS and QUIC transport servers simultaneously (configured via `FLEZHA_TRANSPORTS`). Each compute engine connects using one transport type, determined by its `FLEZHA_TRANSPORT` environment variable.

### Two Communication Layers

| Layer | Purpose | Transport | Encoding |
|-------|---------|-----------|----------|
| **Control Plane** | Handshake, command dispatch, lifecycle management | Control socket | Protobuf messages wrapped in binary framing |
| **Data Plane** | Operative input/output streaming | Primary + secondary stream sockets | Raw bytes wrapped in binary framing |

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

There are three socket types. In all cases, the **controller listens** and the **SDK connects**.

#### Control Socket (shared)

```
{base}/executor_control_channel.sock
```

A single control socket serves all SDK processes. Each SDK connects here to initiate the handshake.

#### Primary Stream Sockets (per compute engine)

```
{base}/primary_stream_{executor_id}.sock
```

The controller creates a listening socket for each primary stream after the SDK has been assigned an ID. The SDK connects to these sockets. Data flows **controller -> SDK** (the controller sends operative input to the SDK).

#### Secondary Stream Sockets (per compute engine)

```
{base}/secondary_stream_{executor_id}.sock
```

The controller creates a listening socket for each secondary stream. The SDK connects. Data flows **SDK -> controller** (the SDK sends operative output back to the controller).

### QUIC

When `FLEZHA_TRANSPORT=quic`, the SDK connects to the controller via a single QUIC connection (UDP). All communication multiplexes over bidirectional QUIC streams within that connection.

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

The controller can listen on both UDS and QUIC simultaneously by setting `FLEZHA_TRANSPORTS=uds,quic` (or in test mode, both are enabled by default). Each compute engine independently chooses its transport. The transport type is threaded through the supervisor chain so that UDS compute engines get StreamAcceptors and QUIC ones rely on the Transport.Server for data stream routing.

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
| `connect_control_channel` | Handshake init message (SDK -> controller) |
| `control_command` | Protobuf command messages (controller -> SDK) |
| `control_ping` | Control-channel keepalive ping/echo (re-arms the read-level idle watchdog) |
| `data_heartbeat` | Data-channel heartbeat (SDK -> controller; per-channel liveness) |
| `{operative_id}` | Stream I/O data (both directions) |

The `data_heartbeat` frame carries an empty message and empty metadata. The
executor writes one on every secondary stream every `FLEZHA_DATA_HEARTBEAT_MS`
(default 10 s) while connected, so an idle-but-healthy operative keeps the
controller's per-channel liveness clock fresh without producing real output.
The controller treats any inbound frame (heartbeat or operative output) as
liveness; a bound channel silent past `:io_stream_liveness_threshold_ms`
(default 30 s) is flagged as a stall.

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
| `input_type` | Controller → SDK | Wire format of the entry-channel payload — `"1"` for binary (default), `"2"` for UTF-8 text. The SDK uses this to convert the payload bytes to the entry channel's `EntryChannelType` (`Vec<u8>` or `String`). |
| `channel` | Controller → SDK | Names the Operative's entry channel that should receive this input. If absent, the SDK falls back to the Operative's first declared input channel. |
| `channel` | SDK → Controller | Set by `OperationCtx::emit` on plugin-bound and hybrid channels. Plugin providers (e.g. `Flezha.Plugin.Provider.Kafka.Producer`) match on this key via their `:channel` opt to gate which channel's outputs they consume. |
| `flz.segment` | SDK → Controller | Output-identity stamp on `.keyed()` channels: the emitting Segment as `<operative_instance_id>/<operation_name>/<segment_key>` (empty key for single-mode). Replay-stable. |
| `flz.seq` | SDK → Controller | Output-identity stamp on `.keyed()` channels: `<change_seq>.<emit_ordinal>` — the Segment's durable change seq after the flush-before-emit drain, plus the emit's ordinal within the invocation. See "Output identity stamps" below for the exactness boundary. |
| `flz.key` | SDK → Controller | Output-identity stamp on `.keyed()` channels: the caller's natural key from `OperationCtx::emit_keyed(key, value)`. Replay-stable by the determinism contract. |
| `flz.offset` | Controller → SDK | Effectively-once tier: the durable input-log offset assigned to this forwarded input. The SDK threads it through dispatch (including inter-operation emits) so the invocation's effect record carries it back as its consumed-offset. Absent outside the tier. |
| `flz.epoch` | Controller → plugins | Effectively-once tier: the Segment's WAL realign epoch, stamped (with `flz.segment` and the durable `flz.seq` = `outbox_seq`) on every output the fsync-gated outbox flush dispatches. Its presence marks a tier output; `(flz.segment, flz.seq, flz.epoch)` is the exact dedup identity. |

The `channel` metadata key replaces the pre-`operative-mvp-rust` `plugin_group: u8` integer key. v1 sends the channel name as a UTF-8 string; an integer fast-path is reserved for a future change.

The `flz.` prefix is a protected namespace: user-supplied metadata keys
beginning with `flz.` are rejected at emit time with a typed
`OperationError::ReservedMetadataKey`, on every emit variant, so runtime
stamps can never be spoofed or clobbered. Stamps are appended after user
metadata. Channels not declared `.keyed()` carry no stamps and their wire
frames are byte-identical to the pre-identity layout.

Hybrid channels (declared with `also_to_plugin_with(...)` — see `docs/operative_architecture.md` "Hybrid channels: inter-operation fan-out + plugin sink") reuse this exact framing. The plugin-bytes wire frame is byte-indistinguishable from a pure-`to_plugin(...)` frame for the same plugin name and bytes; no new wire payload-id, no new metadata key.

#### Per-Segment mailbox is bounded; on full, the channel's `OverflowPolicy` applies

The SDK's per-Segment mailbox is a fixed-capacity `crossbeam::queue::ArrayQueue` allocated at operative startup. When the controller dispatches a frame to an Operative whose targeted Segment's mailbox is full, the SDK applies the channel's declared `OverflowPolicy`: `Block` (producer drains the full mailbox inline when no drainer is actively running, else parks briefly and retries — no frame is lost), `DropNewest` (frame discarded, counter incremented), or `DropOldest` (consumer's mailbox evicts its oldest item and accepts the new one, counter incremented). The default entry-channel policy is `DropNewest` — **the SDK silently drops frames when an Operative's entry mailbox is saturated**, and the wire-reader thread continues servicing sibling Operatives sharing the same physical connection. Under `Block` a producer never parks indefinitely: when the slot's drain exists only as a queued pool task, the producer becomes the drainer itself (parking there self-deadlocks a fully-blocked worker pool — the 2026-06-10 production freeze). `Block` requires an acyclic operation graph. See `docs/operative_architecture.md` "Backpressure and bounded mailboxes" for the full contract — capacity declaration, default policies by channel kind, and the per-Segment counter set.

#### State migration JSON shape

`MigrationSerialization` / `MigrationDeserialization` / `SegmentState` / `SegmentApplyState` carry a JSON blob whose shape is:

- Single-mode operations (no `operation_keys()` declared):
  ```json
  {"<operation_name>": <state>}
  ```
- Static-mode operations (`operation_keys()` non-empty):
  ```json
  {"<operation_name>": {"<operation_key>": <state>, ...}}
  ```

This is a wire-format break vs the pre-`operative-mvp-rust` flat `<state>` shape. State portability across the cutover is out of scope (umbrella Non-Goal 2 / `operative-state-migration`); the hard-cutover policy means no production scenario crosses old-SDK and new-SDK state blobs.

### Frame Buffering

Because Unix domain sockets are stream-oriented, frames may be split across multiple `read()` calls. SDK implementations **must** handle:

1. **Incomplete headers**: fewer than 36 bytes received; buffer and wait for more data.
2. **Incomplete payloads**: header received but fewer than `payload_size` bytes available; buffer and wait.
3. **Multiple frames in one read**: a single `read()` may contain more than one complete frame; parse all of them.

The Elixir controller uses a state-machine buffer with three states:
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

package executor_commands;

message CommandRequest {
    string reference_id = 1;

    oneof command {
        AssignExecutorId assign_executor_id = 2;
        ConnectSocket connect_socket = 3;
        AddOperative add_operative = 4;
        RemOperative rem_operative = 5;
        MigrationSerialization migration_serialization = 6;
        MigrationDeserialization migration_deserialization = 7;
        SegmentState segment_state = 8;
        ExecutorDisconnect executor_disconnect = 9;
        OperationMessage operation_message = 10;
        SegmentApplyState segment_apply_state = 11;
        ExecutorShutdownNotification executor_shutdown_notification = 12;
        ExecutorShutdownAck executor_shutdown_ack = 13;
    }
}

message CommandResponse {
    string reference_id = 1;
    string status = 2;
    repeated string errors = 3;
    bytes raw_data = 4;
}

message AssignExecutorId {
    string executor_id = 1;
}

message ConnectSocket {
    uint32 sockets_count = 1;
}

message AddOperative {
    string operative_id = 1;
    string factory_id = 2;
    string init_values = 3;
    string kind = 4; // operative kind; empty/absent ⇒ "unbounded"
}

message RemOperative {
    string operative_id = 1;
}

// SDK -> Controller. Emitted by a bounded-kind operative when its run-once
// function returns ("ok" | "error"). The Controller drives the normal
// stop/teardown path (deregister-on-completion). Old Controllers ignore
// the unknown variant.
message BoundedCompleted {
    string operative_id = 1;
    string status = 2;
    string error = 3;
}

message MigrationSerialization {
    string operative_id = 1;
}

message MigrationDeserialization {
    string operative_id = 1;
    string factory_id = 2;
    bytes serialized_state = 3;
}

message SegmentState {
    string operative_id = 1;
}

message ExecutorDisconnect {}

message OperationMessage {
    string operative_id = 1;
    bytes payload = 2;
}

message OperationMessageResponse {
    bytes payload = 1;
    string error = 2;
}

message SegmentApplyState {
    string operative_id = 1;
    string factory_id = 2;
    bytes state_binary = 3;
}

message ExecutorShutdownNotification {
    string operative_id = 1;
    bytes serialized_state = 2;
}

message ExecutorShutdownAck {
    string operative_id = 1;
}
```

### Request/Response Pattern

- **Direction**: Most `CommandRequest` variants flow controller -> SDK. The two
  exceptions — `ExecutorShutdownNotification` and its `ExecutorShutdownAck`
  — flow SDK -> controller and controller -> SDK respectively, using the same
  `CommandRequest` envelope and the same `"control_command"` payload_id; the
  receiver inspects the oneof tag to dispatch.
- **Correlation**: Each `CommandRequest` contains a `reference_id` (an opaque string). The SDK must echo this `reference_id` in the corresponding `CommandResponse`. For the SDK -> controller shutdown notification, the controller echoes the same `reference_id` in the `ExecutorShutdownAck` it sends back.
- **Framing**: Both `CommandRequest` and `CommandResponse` are Protobuf-encoded, then wrapped in the binary framing format (Section 3).
  - The controller sends commands with payload_id = `"control_command"`.
  - The SDK sends responses using the **same payload_id** that was received (which is `"control_command"`).
  - The SDK sends `ExecutorShutdownNotification` with payload_id = `"control_command"`; the controller's `ExecutorShutdownAck` reuses the same payload_id.
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
SDK                                        Controller
 │                                           │
 │──── 1. TCP connect to control socket ────>│
 │                                           │
 │──── 2. Send handshake init message ──────>│
 │     payload_id: "connect_control_channel" │
 │     message: JSON                         │
 │                                           │
 │                      3. Controller processes ───│
 │                         handshake          │
 │                                           │
 │<─── 4. AssignExecutorId ────────────│
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
 │<─── 9. Commands (AddOperative, etc.) ────│
 │──── 9. CommandResponse ──────────────────>│
 │                                           │
 │        ... stream I/O active ...          │
 │                                           │
 │<─── 10. ExecutorDisconnect ─────────│
 │──── 10. CommandResponse (success) ───────>│
 │                                           │
 │     11. SDK closes socket ────────────────│
 │                                           │
```

### Detailed Steps

#### Step 1: Connect to Control Socket

The SDK opens a Unix domain socket connection to:
```
{FLEZHA_SOCKET_FILES_PATH}/executor_control_channel.sock
```

#### Step 2: Send Handshake Init Message

The SDK sends a single binary frame:

- **Payload ID**: `"connect_control_channel"`
- **Message**: JSON-encoded object:
  ```json
  {
    "namespace": "<namespace_string>",
    "factory_ids": ["<factory_id_1>", "<factory_id_2>", ...],
    "factories": [{"id": "<factory_id>", "description": null}, ...],
    "stable_id": "<namespace>-<profile_hash8>-<rand6>"
  }
  ```
  - `namespace`: A string identifying the logical group this compute engine belongs to.
  - `factory_ids`: List of all registered operative factory IDs in this SDK process.
  - `factories`: Per-factory metadata (id + optional description); a superset of `factory_ids`.
  - `stable_id`: The executor's durable, self-owned **stable identity**, present on **every** connect (initial and reconnect). The controller trusts it as the operative-binding key and, on reconnect, swaps the prior session's operatives onto the new session token. See [`resilience/executor_identity.md`](resilience/executor_identity.md).
- **Metadata**: empty

#### Step 3: Controller Processes Handshake

The controller:
1. Reads the frame from the control socket (with a 5-second timeout).
2. Parses the JSON to extract `namespace`, `factory_ids`, and `stable_id`.
3. Mints a fresh per-connection **session token** (the `executor_id` value below) and resolves the presented `stable_id` — on reconnect it swaps the prior session's operative bindings onto the new token instead of minting-and-evicting.
4. Starts supervisor trees for the new compute engine (control unit, stream listeners, etc.).
5. Starts or joins the namespace hub.

#### Step 4: Controller Sends `AssignExecutorId`

The controller sends a `CommandRequest` carrying the minted **session token**:
```protobuf
AssignExecutorId {
    executor_id: "<session_token>"
}
```
This is Protobuf-encoded, then wrapped in a binary frame with payload_id `"control_command"`. The SDK stores it as its session token (it keys the per-connection data sockets) but does **not** persist it — the durable identity is the self-owned `stable_id` it already presented in the handshake.

#### Step 5: SDK Responds

The SDK:
1. Stores the assigned `executor_id`.
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

#### Step 6: Controller Sends `ConnectSocket`

The controller sends a `CommandRequest` containing:
```protobuf
ConnectSocket {
    sockets_count: <N>
}
```
Where `N` is the number of concurrent stream channel pairs. By default, `N` equals the controller's online scheduler count (CPU cores), configurable via `FLEZHA_CONCURRENT_CHANNEL_COUNT`.

#### Step 7: SDK Connects to Stream Sockets

For each index `i` in `0..sockets_count`, the SDK:

1. Connects to `{base}/primary_stream_{executor_id}.sock`
   - Sets the socket to **non-blocking** mode.
   - Registers it for read polling (e.g., with `mio`/`epoll`/`kqueue`).
   - This socket receives input data from the controller.

2. Connects to `{base}/secondary_stream_{executor_id}.sock`
   - This socket is used to send output data back to the controller.
   - Stored for later use by operatives.

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

The control channel enters a steady-state loop. The controller may send any of the commands documented in Section 6. The SDK processes each command and replies with a `CommandResponse`.

#### Step 10: Disconnect

The controller sends a `ExecutorDisconnect` command. The SDK:
1. Sets its status to `Stopped`.
2. Sends a `CommandResponse` with status `"success"`.
3. The controller closes the control socket after receiving the response.
4. The SDK should also shut down its stream sockets.

This is an **intentional** disconnect — the SDK should NOT attempt to reconnect.

#### Auto-Reconnection (Unexpected Disconnect)

If the control channel drops unexpectedly (controller crash, container restart, network failure), the SDK detects the disconnection and automatically reconnects:

```
SDK                                        Controller
 │                                           │
 │     ══ Controller goes down ══                 ✗
 │
 │  Control listener detects read error
 │  (BrokenPipe / UnexpectedEof)
 │
 │  SDK clears all state:
 │    - io_streams (dead)
 │    - operatives (will be re-added)
 │    - work queues (stale stream refs)
 │    - executor_id → "undefined"
 │
 │  Retry with exponential backoff:
 │    200ms, 400ms, 800ms, ... cap 30s
 │
 │     ══ Controller comes back up ══             │
 │                                           │
 │──── 1. Reconnect to control socket ──────>│
 │──── 2. Send handshake init message ──────>│
 │                                           │
 │     (normal lifecycle resumes from Step 3) │
 │                                           │
 │<─── 4. AssignExecutorId (new session token) ───│
 │<─── 6. ConnectSocket ────────────────────│
 │<─── 9. AddOperative (for each) ──────────│
 │                                           │
 │         ═══ READY ═══                     │
```

**Key behaviors:**
- The SDK distinguishes intentional disconnects (`ExecutorDisconnect` command sets status to `Stopped`) from unexpected ones (status is `Running`). Only unexpected disconnects trigger reconnection.
- The controller treats the reconnection as a **new session** — it mints a fresh session token and, because the SDK re-presents its durable `stable_id`, **swaps** the prior session's operative bindings onto the new token rather than minting-and-evicting or re-issuing each operative. See [`resilience/executor_identity.md`](resilience/executor_identity.md).
- Operative factories remain registered in the SDK process; the SDK preserves its in-memory operative registry across reconnect, so the binding swap re-points existing operatives without re-issuing `AddOperative` for each.
- In-flight worker work completes (writes to dead streams fail silently); workers return to the pool after reconnection.
- Backoff schedule: 200ms, 400ms, 800ms, 1.6s, 3.2s, 6.4s, 12.8s, 25.6s, capped at 30s.

---

## 6. Command Reference

All commands are sent as `CommandRequest` messages from the controller to the SDK. The SDK responds with `CommandResponse`.

### AssignExecutorId

| Field | Value |
|-------|-------|
| Direction | Controller -> SDK |
| Proto message | `AssignExecutorId { executor_id: string }` (the value is the controller-minted **session token**) |
| Behavior | SDK stores the session token and sets status to `Running`; the value is NOT persisted (the durable identity is the SDK-owned `stable_id` in the handshake) |
| Response status | `"success"` |
| Response raw_data | empty |

### ConnectSocket

| Field | Value |
|-------|-------|
| Direction | Controller -> SDK |
| Proto message | `ConnectSocket { sockets_count: uint32 }` |
| Behavior | SDK creates `sockets_count` pairs of stream connections (primary + secondary) |
| Response status | `"success"` |
| Response raw_data | empty |

### AddOperative

| Field | Value |
|-------|-------|
| Direction | Controller -> SDK |
| Proto message | `AddOperative { operative_id: string, factory_id: string, init_values: string, kind: string }` |
| Behavior | SDK creates a new operative from the named factory. `kind` (empty ⇒ `"unbounded"`) selects the operative kind. For `"unbounded"`: an unbounded operative instance, deserializing non-empty `init_values` JSON into state via `json_deserialize_state()`, then `post_alter()`. For `"bounded"`: a bounded operative instance whose `run(ctx, args)` is scheduled once on the work pool with `args = init_values`. |
| Response status | `"success"` on success, `"error"` on failure |
| Response raw_data | empty |
| Possible errors | Factory not found (unbounded or bounded class); JSON parse failure; deserialization failure; unknown `kind` |

### BoundedCompleted

| Field | Value |
|-------|-------|
| Direction | SDK -> Controller (same `control_command` envelope as `ExecutorReconnected`) |
| Proto message | `BoundedCompleted { operative_id: string, status: string, error: string }` |
| Behavior | Emitted when a bounded-kind operative's `run` returns: `status` is `"ok"` or `"error"` (with `error`). The Controller drives the normal stop/teardown path (deregister-on-completion) so the finished operative leaves the durable registry; an errored operative is torn down identically (terminal — the platform never re-runs the operative; a structured chain's per-segment retry ladder runs *inside* the run, before this) and surfaced via `[:flezha, :bounded, :failed]`. |
| Response | None (one-way notification; old Controllers ignore the unknown variant) |

Both SDKs are at parity on this contract: the Rust and Java SDKs each read the
optional `AddOperative.kind` field (empty ⇒ `"unbounded"`) and emit `BoundedCompleted`
byte-identically, so the Controller decodes a Java bounded operative's completion exactly as a
Rust one. A bounded operative's plugin output is a frame whose payload id is the operative's
instance id with **empty** metadata (no `channel` key), written on a secondary
stream — identical across both SDKs.

### RemOperative

| Field | Value |
|-------|-------|
| Direction | Controller -> SDK |
| Proto message | `RemOperative { operative_id: string }` |
| Behavior | SDK removes the operative with the given ID from its operative registry |
| Response status | `"success"` on success, `"error"` if operative not found |
| Response raw_data | empty |
| Possible errors | `"Operative not found"` |

### SegmentState

| Field | Value |
|-------|-------|
| Direction | Controller -> SDK |
| Proto message | `SegmentState { operative_id: string }` |
| Behavior | SDK serializes the operative's current state using `bin_serialize_state()` and returns the JSON bytes |
| Response status | `"success"` on success, `"error"` if operative not found |
| Response raw_data | JSON-serialized operative state bytes |
| Possible errors | `"Operative with id {id} not found"` |

### MigrationSerialization

| Field | Value |
|-------|-------|
| Direction | Controller -> SDK |
| Proto message | `MigrationSerialization { operative_id: string }` |
| Behavior | SDK serializes the operative's state using `json_serialize_state()`, then **removes** the operative from the registry. The serialized state is returned so the controller can migrate it to another compute engine. |
| Response status | `"success"` |
| Response raw_data | JSON-serialized operative state bytes |

### MigrationDeserialization

| Field | Value |
|-------|-------|
| Direction | Controller -> SDK |
| Proto message | `MigrationDeserialization { operative_id: string, factory_id: string, serialized_state: bytes }` |
| Behavior | SDK creates a new operative from the factory, then deserializes the provided `serialized_state` into it via `json_deserialize_state()`. |
| Response status | `"success"` |
| Response raw_data | empty |

### ExecutorDisconnect

| Field | Value |
|-------|-------|
| Direction | Controller -> SDK |
| Proto message | `ExecutorDisconnect {}` (no fields) |
| Behavior | SDK sets status to `Stopped`. After sending the response, the SDK should close the control socket. |
| Response status | `"success"` |
| Response raw_data | empty |

### OperationMessage

| Field | Value |
|-------|-------|
| Direction | Controller -> SDK |
| Proto message | `OperationMessage { operative_id: string, payload: bytes }` |
| Behavior | SDK locates the operative by id and invokes the factory's `handle_message(payload)` hook. The return value (reply bytes on success, error string on failure) is wrapped into a protobuf-encoded `OperationMessageResponse` and written as the outer `CommandResponse.raw_data`. Factories that do not override `handle_message` return the default "unsupported" error — mis-routing is loud, not silent. |
| Response status | `"success"` when the operative was found; `"error"` only when the operative id is unknown |
| Response raw_data | Protobuf-encoded `OperationMessageResponse { payload: bytes, error: string }` — an empty `error` means success and the controller reads `payload`; a non-empty `error` means the factory reported a failure and the controller discards `payload` |
| Possible errors | `"Operative with id {id} not found"` (CommandResponse.errors); factory-supplied error string (OperationMessageResponse.error) |

The controller SHALL treat a non-empty `OperationMessageResponse.error` as an
error condition regardless of `payload` contents. Used by the
[operative-message-handlers](../docs/operative_message_handlers.md)
mechanism — specifically the first-party
`Flezha.Plugin.Provider.MessageForward` provider — to bridge
Elixir-side request/reply envelopes to the SDK factory. The caller side
is expected to set its own timeout; there is no server-side deadline
propagation. v1 is open-auth; scoping is the caller's responsibility.
Payloads are opaque bytes and share the control stream with other
commands — keep them small to avoid head-of-line contention.

### ExecutorShutdownNotification

| Field | Value |
|-------|-------|
| Direction | SDK -> Controller (only SDK-initiated CommandRequest variant alongside its ack) |
| Proto message | `ExecutorShutdownNotification { operative_id: string, serialized_state: bytes }` |
| When emitted | On graceful SDK shutdown only: SIGTERM, `Drop` of the SDK handle, explicit `disconnect()`. Crash paths (SIGKILL, panic) cannot emit and fall back to last periodic snapshot. |
| Behavior | Per running operative, the SDK serializes via the same per-driver serializer used for `MigrationSerialization` and emits one notification. The controller forwards the binary to the corresponding `Overwatch.state.snapshot` and emits an `ExecutorShutdownAck` with the same `reference_id`. |
| Ack timeout | The SDK SHALL wait at most **2 s per operative** for `ExecutorShutdownAck` (parallelised across operatives). After the timeout, the SDK SHALL exit unconditionally and SHALL NOT block its container's shutdown grace period. |
| Per-operative serializer cap | Capped at the controller's `cross_node_request_timeout` (10 s) ceiling; deployments with tighter container grace periods should override the cap via SDK config. |
| Wire format | CommandRequest envelope, payload_id = `"control_command"`, oneof tag 12. |

### ExecutorShutdownAck

| Field | Value |
|-------|-------|
| Direction | Controller -> SDK |
| Proto message | `ExecutorShutdownAck { operative_id: string }` |
| Behavior | Emitted by the controller's `ControlChannel` immediately after the snapshot cast is enqueued on the `Overwatch` GenServer (NOT after the cast has been applied — the cast is in the GenServer mailbox by then and applies in microseconds). The controller emits the ack regardless of whether the Overwatch was found: a missing Overwatch (already migrated to a peer controller) still gets an ack so the SDK can proceed to exit. |
| Wire format | CommandRequest envelope, payload_id = `"control_command"`, oneof tag 13, `reference_id` matches the originating notification. |

**Forward/back compat.** Both opcodes are additive. Older controllers that do
not recognise tag 12 SHALL log and discard the message via Protox's
unknown-field handling — no crash, no protocol downgrade. Older SDKs
that do not emit tag 12 fall back to today's periodic-snapshot freshness
shipped by `heal-connections-on-restart`. No SDK protocol version bump
is required.

---

## 7. Data Plane (Stream I/O)

Once stream sockets are established (after `ConnectSocket`), data flows between the controller and SDK:

### Direction

| Socket Type | Direction | Purpose |
|-------------|-----------|---------|
| Primary stream | Controller -> SDK | Controller sends input data TO SDK operatives |
| Secondary stream | SDK -> Controller | SDK sends operative output BACK to controller |

### Framing

All stream messages use the same binary framing format described in Section 3.

- **Payload ID**: The `operative_id` of the target operative.
- **Message**: The input/output data bytes.
- **Metadata**: Key-value pairs conveying type information and other attributes.

### Metadata Conventions

| Key | Values | Description |
|-----|--------|-------------|
| `input_type` | `"1"` = binary, `"2"` = text | Indicates the type of the input data. Defaults to `"1"` if absent. |

### Input Processing (Primary Streams)

When the SDK receives a frame on a primary stream:

1. Extract `operative_id` from the payload ID.
2. Read `input_type` from metadata (default `"1"`).
3. Construct an `OperativeInput`:
   - `"1"` -> `Binary(message_bytes)`
   - `"2"` -> `Text(utf8_string(message_bytes))`
4. Dispatch to the operative's `run()` method via the worker pool.

### Output Processing (Secondary Streams)

When an operative returns a `PipeToAgent` result:

1. For each `(output, metadata)` tuple in the result:
   - Construct a binary frame with payload_id = `operative_id`.
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

### PipeToOperative

When an operative returns `PipeToOperative`, the SDK routes output to another operative locally:

1. Check if the target operative exists locally.
2. If yes, convert the output to input and invoke the target operative's `run()`.
3. If no, fall back to `pipe_to_agent()` — send it to the controller via the secondary stream so the controller can route it.

---

## 8. Recommended SDK Public API

This section describes the recommended public-facing API surface for SDK implementations. Language-specific idioms may vary, but the semantics should be preserved.

### 8.1 Factory Registration

```
register_factory(factory_id, settings, initial_state, handler_generator)
```

- **`factory_id`**: `string` — A globally unique identifier for this operative type.
- **`settings`**: Configuration object containing:
  - `input_transformer` — Converts raw `OperativeInput` to the handler's expected input type.
  - `output_transformer` — Converts the handler's output type to `OperativeResult`.
  - `operative_runner` — Execution strategy: `ThreadPool` (default, sequential per operative) or `ThreadSpawn` (new thread per invocation).
- **`initial_state`**: The default state for new operative instances. Must be serializable to/from JSON.
- **`handler_generator`**: A factory function `() -> OperativeHandler` that creates new handler instances.

Factories **must** be registered before calling `new()`.

### 8.2 UnboundedOperative Interface

SDK implementations must support operatives that conform to this interface:

```
interface UnboundedOperative:
    run(input: OperativeInput) -> Result<OperativeResult, OperativeError>
    json_serialize_state() -> bytes
    json_deserialize_state(data: bytes) -> void
    get_factory_id() -> string
    post_alter() -> void
```

| Method | Description |
|--------|-------------|
| `run(input)` | Execute the operative logic on the given input. May mutate internal state. |
| `json_serialize_state()` | Serialize the operative's current state to JSON bytes. Used for migration and state inspection. |
| `json_deserialize_state(data)` | Restore the operative's state from JSON bytes. Used during `AddOperative` (with init_values) and `MigrationDeserialization`. |
| `get_factory_id()` | Return the factory ID that created this operative. |
| `post_alter()` | Called after state changes (initial creation, init_values application, migration). Use for derived state recomputation. |

### 8.3 Operative Input/Output Types

**OperativeInput** (received from controller):

| Variant | Description |
|---------|-------------|
| `Binary(bytes)` | Raw binary data |
| `Text(string)` | UTF-8 text data |

**OperativeOutput** (sent back to controller):

| Variant | Description |
|---------|-------------|
| `Binary(bytes)` | Raw binary data |
| `Text(string)` | UTF-8 text data |

**OperativeResult** (return value of `run()`):

| Variant | Description |
|---------|-------------|
| `None` | No output; discard silently |
| `Return(output)` | Return output to the caller (local use only, not sent to controller) |
| `PipeToAgent(list<(output, metadata)>)` | Send one or more outputs back to the controller via secondary stream |
| `PipeToOperative(list<(operative_id, output, metadata)>)` | Route output to another operative (local or via controller) |

### 8.4 OperativeHandler Interface

The `OperativeHandler` is the user-facing trait/interface for implementing processing logic:

```
interface OperativeHandler<State, Input, Output>:
    process(input: Input, state: &mut State) -> Result<Output, OperativeError>
    post_create(state: &mut State) -> void  // optional, called after creation
```

The SDK wraps this with input/output transformers into a full `UnboundedOperative` implementation.

### 8.5 SDK Entry Point

```
new(namespace: string) -> SDK
```

1. All factories must be registered before calling `new()`.
2. Connects to the controller's control socket. If the controller is not available, retries with exponential backoff (200ms → 30s cap) until the connection succeeds. This allows compute engines to start before the controller.
3. Sends the handshake init message with the namespace and registered factory IDs.
4. Blocks until `AssignExecutorId` is received and processed.
5. Returns the SDK handle.

The returned handle provides:
- `get_id() -> string` — Returns the assigned compute engine ID.
- `get_operatives()` — Returns the current set of active operatives.

### 8.6 Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `FLEZHA_SOCKET_FILES_PATH` | `app_data/sockets` | Base path for socket files |
| `FLEZHA_CONCURRENT_CHANNEL_COUNT` | `0` (= CPU count) | Number of stream socket pairs (controller-side config) |
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

### 9.2 AssignExecutorId Command (Hex Dump)

Controller sends `AssignExecutorId` with `executor_id = "ce_001"` and `reference_id = "#Ref<0.1.2.3>"`.

**Protobuf encoding** of `CommandRequest`:
```protobuf
field 1 (reference_id): wire type 2 (length-delimited), value = "#Ref<0.1.2.3>"
field 2 (assign_executor_id): wire type 2 (length-delimited)
  field 1 (executor_id): wire type 2 (length-delimited), value = "ce_001"
```

Protobuf bytes (25 bytes):
```
0A 0D 23 52 65 66 3C 30 2E 31 2E 32 2E 33 3E     reference_id (tag + len + 13 chars)
12 08 0A 06 63 65 5F 30 30 31                       assign_executor_id (tag + len + nested)
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

Controller sends text input `"hello"` to operative `"proc_42"` with metadata `{input_type: "2"}`.

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

### 9.4 Full Operative Lifecycle Sequence

```
Time  Direction    Description
────  ─────────    ───────────────────────────────────────────────
 t0   SDK->Controller   Connect to control socket
 t1   SDK->Controller   Handshake: {"namespace":"my_app","factory_ids":["echo"]}
 t2   Controller->SDK   CommandRequest: AssignExecutorId{executor_id:"ce_001"}
 t3   SDK->Controller   CommandResponse: {ref_id:"...", status:"success"}
 t4   Controller->SDK   CommandRequest: ConnectSocket{sockets_count:4}
 t5   SDK          Connect to primary_stream_ce_001.sock (×4, non-blocking)
      SDK          Connect to secondary_stream_ce_001.sock (×4)
 t6   SDK->Controller   CommandResponse: {ref_id:"...", status:"success"}
 t7   Controller->SDK   CommandRequest: AddOperative{operative_id:"p1", factory_id:"echo", init_values:""}
 t8   SDK          Create operative "p1" from factory "echo"
 t9   SDK->Controller   CommandResponse: {ref_id:"...", status:"success"}
t10   Controller->SDK   [primary stream] payload_id:"p1", message:"hello", metadata:{input_type:"2"}
t11   SDK          Operative "p1" runs: input="hello" -> output="hello"
t12   SDK->Controller   [secondary stream] payload_id:"p1", message:"hello", metadata:{}
t13   Controller->SDK   CommandRequest: RemOperative{operative_id:"p1"}
t14   SDK          Remove operative "p1"
t15   SDK->Controller   CommandResponse: {ref_id:"...", status:"success"}
t16   Controller->SDK   CommandRequest: ExecutorDisconnect{}
t17   SDK->Controller   CommandResponse: {ref_id:"...", status:"success"}
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

- **Controller side**: The controller detects a closed control socket via `{:flz_closed, ...}` NIF messages. This triggers compute engine disconnect handling (preservation, migration, or rerouting depending on namespace state).
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

**Intentional vs unexpected disconnect:** When the controller sends `ExecutorDisconnect`, the SDK sets its status to `Stopped` and does **not** reconnect. When the connection drops without this command (controller crash, network failure), the SDK's status is still `Running`, and it enters the reconnection loop with exponential backoff.

**Controller-side keepalive:** The controller preserves compute engine state for a configurable window (default 30000ms, via `FLEZHA_EXECUTOR_KEEPALIVE`) after detecting a disconnect. If a new SDK process with the same namespace connects within this window, the controller may migrate preserved operatives to the new compute engine.


## Per-Segment State Persistence

The persistence subsystem reserves the data-stream payload-ids below and
eight control commands. Both directions and all wire shapes are settled
here and implemented in `operative-state-persistence-controller`
(credit-grant flow control added the three `Credit*` commands;
`effectively-once-output` added `segment_effect_record`).

### Data-stream payload-id `segment_state_change`

SDK → controller. Carries one Change record destined for the per-Segment
WAL. Frame layout (matches `Flezha.State.Wire.decode_frame/1` and
`flezha_executor::segment::flusher::build_segment_state_change_frame`):

```
[segment_id_len: u16 BE]
[segment_id_bytes]
[seq:              u64 BE]
[header:           16 B  ]    <- HeaderTemplate::stamp output
[body_len:         u32 BE]
[body]                        <- postcard-encoded Change
[crc32:            u32 BE]    <- crc32 over body
```

`segment_id_bytes` is structured so the controller can route the record to
the right per-Segment WAL file without a side-table mapping:

```
[operative_class_len: u8   ][operative_class]
[operation_id_len:    u8   ][operation_id]
[segment_key_len:     u16 BE][segment_key]
```

The 16-byte header carries `sdk_lang = 1` (Rust) / `2` (Java), SDK
version (BE u32), Operation `SCHEMA_VERSION` (BE u32), `format_id` (1 =
postcard for Rust, 4 = Kryo for Java), reserved bytes, and a CRC32
over the body.

Cross-language load is rejected loud-and-clear per umbrella Decision 9:
a Java SDK reading a header with `sdk_lang = 1` raises
`StateLoadException.sdkLangMismatch`; the same applies to the Rust SDK
in reverse. Both SDKs reject unknown `format_id` values via the
matching typed error.

### Data-stream payload-id `segment_effect_record`

SDK → controller, effectively-once tier only. Same wire shape as
`segment_state_change` with header `format_id = 2`; `body` is the postcard
`EffectRecordBody` — one invocation's state change(s), plugin outputs, and
consumed input-log offset, persisted by the controller as ONE WAL record
under ONE fsync window (the atomic effect record: state, outputs, and
progress can never tear apart):

```
EffectRecordBody {
    changes:         Vec<Vec<u8>>,       -- postcard S::Change bytes each,
                                            folded on restore exactly like
                                            plain change records, in order
    outputs:         Vec<OutputRecord>,  -- the invocation's plugin emits
    consumed_offset: Option<u64>,        -- flz.offset of the input
    dlq:             Option<DlqEntryBody>,
    event_time:      u64,                -- the invocation's declared clock value
}
OutputRecord {
    channel:  String,
    key:      String,                    -- emit_keyed natural key
    metadata: Vec<(String, String)>,     -- user metadata, key-sorted
    payload:  Vec<u8>,
}
DlqEntryBody {                           -- Some only for a parked poison payload
    channel:  String,                    -- the consumer's input channel
    error:    String,
    attempts: u32,
    payload:  Option<Vec<u8>>,           -- None when not byte-representable
}
```

`event_time` is the value the Operation's declared `Clock` evaluated to for that
invocation — the extracted event time under `EventTimeFrom`, wall-clock ms under
`WallClock`, the change seq under `LogicalClock`. It is what makes the
controller's event-time → seq → input-offset lookup a *read*: late-input
correction resolves its rewind baseline against it, and `TimeTravel`'s
`{:timestamp, ts}` target resolves per-record against it instead of
approximating from the WAL file's mtime.

A tier-on dead-letter park rides the SAME atomic
record as its consumed-offset advance — typically `{no state change, dlq
entry, consumed offset}` — so a crash can never advance the offset while
losing the park. The controller materializes the DLQ store entry from
the record after its fsync (idempotent on `segment_key@consumed_offset`)
and never sends a `ParkPayloadAck` for it: input-log crash-replay
already guarantees the poison payload re-parks.

In tier mode plugin emits never ride the primary stream — the controller
dispatches them to the `:on_output` plugin chain only after the record's
fsync completes, stamped with the durable identity `flz.segment`,
`flz.seq = <record WAL seq>.<emit ordinal>` (the `outbox_seq`), and
`flz.epoch`. Effect records are never coalesced at drain time. Decoded
controller-side by `Flezha.State.Wire.decode_effect_body/1`.

### Data-stream payload-id `segment_state_snapshot`

SDK → controller. Same wire shape as `segment_state_change`; `body` is the
postcard-encoded `Self::State` (full snapshot) instead of one Change.
The controller writes the body to `<segment_key>.snap.<seq>` via the
`.tmp → fsync → rename → truncate WAL up to seq` sequence.

### Data-stream payload-id `park_payload` (SDK → controller)

Dead-letter park for tier-off sessions (`platform-dead-letter-queue`).
When an operation invocation exhausts its input channel's retry budget
(`retry_max`, default 3 total attempts, exponential backoff + jitter) —
or returns a non-retryable error — the SDK ships this frame and holds
the segment until the controller acks the park as durable
(`ParkPayloadAck` below), re-sending on a 2 s cadence: park-then-advance,
never a drop. Same outer frame layout as `segment_state_change` with a
zero header; `body` is the postcard `ParkBody`:

```
ParkBody {
    park_id:       String,          -- SDK-minted ack-correlation id
    origin_offset: Option<u64>,     -- input-log offset when the tier provides one
    entry:         DlqEntryBody,    -- the same {channel, error, attempts, payload}
                                    -- record a tier-on park embeds in its
                                    -- effect record (see above)
}
```

The controller appends the entry to the durable DLQ store
(`app_data/dlq/<operative_class>/<operation_id>/`), fsyncs, and only
then sends the ack. Appends are idempotent on origin identity
(`channel:sha256(payload)` for tier-off parks), so ack-lost re-sends
never duplicate an entry. Decoded controller-side by
`Flezha.State.Wire.decode_park_body/1`. Effectively-once sessions do
not use this frame — their park rides the atomic effect record's `dlq`
field above.

### Control command `RestoreInitState` (controller → SDK)

```protobuf
message RestoreInitState {
    bytes segment_id             = 1;
    bytes snapshot_bytes         = 2;
    bytes change_log_bytes       = 3;
    bool  has_compensation_intent = 4;  // saga crash-resume markers
    bool  has_compensation_done   = 5;
    OpJournalState op_journal    = 6;   // bounded op-journal crash-resume
}
```

`op_journal` (absent unless the instance has a non-empty op-journal)
carries the committed op indexes plus the latest journaled handoff batch
so a resumed durable structured bounded driver skips the committed
operations and seeds the first uncommitted one from the handoff — see
`OpCommit` below.

Each record inside `change_log_bytes` uses the controller's on-disk WAL
framing:

```
[seq:           u64 BE]
[len:           u32 BE]    <- = 16 + change_bytes.len()
[header:        16 B  ]
[change_bytes]
[crc32:         u32 BE]    <- crc32 over header + change_bytes
```

The SDK's `Operation::init_state(operation_key, Some(payload))` is invoked with
the payload borrowed; the runtime decodes `snapshot_bytes` (when
non-empty) into `Self::State` and folds the change-log records via
`Operation::apply_change`.

### Control command `RestoreInitStateResponse` (SDK → controller)

```protobuf
message RestoreInitStateResponse {
    bytes  segment_id    = 1;
    uint64 terminal_seq  = 2;
}
```

The controller unblocks the Segment's input streaming after the response
arrives. The next `segment_state_change` is expected to carry
`seq = terminal_seq + 1`.

### Control command `CompactSegment` (controller → SDK)

```protobuf
message CompactSegment {
    bytes segment_id = 1;
}
```

Issued when `wal_size_bytes` exceeds `compact_after_bytes` (per-Operation,
default 64 MiB) or when the executor runs `flz state rotate`. The SDK
clones `Self::State` under the segment's sticky CAS, postcard-
serializes it, ships the bytes via `segment_state_snapshot`, then
replies with:

```protobuf
message CompactSegmentResponse {
    bytes  segment_id          = 1;
    uint64 snapshot_seq        = 2;
    uint64 snapshot_bytes_size = 3;
}
```

### Control command `SegmentMailboxStatus` (controller → SDK)

```protobuf
message SegmentMailboxStatus {
    bytes  segment_id   = 1;
    string status       = 2;   // "full" | "open"
}
```

One-way notification (no `CommandResponse`). The controller's `WalWriter`
emits `"full"` when `pending_bytes` exceeds `wal_high_watermark_bytes`
(default 16 MiB) and `"open"` when it falls below
`wal_low_watermark_bytes` (default `high / 2`). The SDK's per-Segment
mailbox observer translates the signal into the channel's declared
`OverflowPolicy`.

Shipped only to sessions that are NOT credit-speaking (pre-credit SDKs,
or a controller running with `FLEZHA_CREDIT_FLOW_CONTROL=false`). A
credit-speaking session's back-pressure is carried entirely by the
credit-grant protocol below.

### Control command `CreditWindowInit` (controller → SDK)

```protobuf
message CreditWindowInit {
    string stream_id           = 1;   // opaque credit-session id, fresh per attach
    uint64 window_bytes        = 2;   // un-fsynced byte budget for the stream
    uint64 low_watermark_bytes = 3;   // controller's grant-batching threshold
}
```

Offers credit-grant flow control for the executor's state stream. Emitted
on the first `segment_state_change` frame of a stream attach (so the
offer originates from the channel on the actual state path, and the
window resolves per-Operative from that frame's class —
`:credit_window_bytes`, default `wal_high_watermark_bytes`). The SDK
acknowledges by returning an encoded `CreditWindowAck` as the
`CommandResponse.raw_data`; on that ack the controller marks the session
credit-speaking, stops shipping `SegmentMailboxStatus`, and enforces the
per-stream byte ledger. A pre-credit SDK drops the unknown variant and
never acks — the controller times out and the session keeps the legacy
behaviour byte-for-byte. `FLEZHA_CREDIT_FLOW_CONTROL=false` (controller)
skips the offer entirely.

### Control command `CreditWindowAck` (SDK → controller)

```protobuf
message CreditWindowAck {
    string stream_id = 1;   // echoes the offered credit-session id
}
```

Returned as the `CommandResponse.raw_data` of `CreditWindowInit` (not as
a standalone `CommandRequest`). Arming the window and sending the ack
happen on the SDK's control coordinator; from then on every
`segment_state_change` frame debits its full wire size from the window
before the sink write, and an exhausted window parks the sending thread
— park, never drop.

### Control command `CreditGrant` (controller → SDK)

```protobuf
message CreditGrant {
    string stream_id = 1;   // credit-session id the grant belongs to
    uint64 bytes     = 2;   // replenished send credit
}
```

One-way replenishment (the generic `CommandResponse` it solicits is
ignored). Credit is minted only when appended bytes become durable: on
WAL fsync the controller reports the fsynced wire bytes to the stream's
ledger and sends one batched grant when the un-fsynced buffer falls
below `low_watermark_bytes` — never a grant per record, and a stalled
fsync mints nothing (the sender parks; controller memory stays bounded
by `window_bytes`). Credit never carries across sessions: a reconnect
re-runs `CreditWindowInit` with a fresh `stream_id`, and the SDK discards
any grant whose `stream_id` does not match its current session. A frame
arriving on an exhausted ledger is a protocol violation: the controller
sheds it (`credit_overrun_total`), and the existing seq-gap resync
repairs the gap.

### Control command `ParkPayloadAck` (controller → SDK)

```protobuf
message ParkPayloadAck {
    string park_id = 1;   // echoes the ParkBody.park_id
}
```

One-way durability ack for a `park_payload` data-stream frame (the
generic `CommandResponse` it solicits is ignored). Sent only after the
dead-letter entry is fsynced in the DLQ store; on receipt the SDK wakes
the dispatch thread blocked in the park, drops the poison payload, and
the segment advances to the next input. An SDK that never receives the
ack (controller down, reconnect window) keeps the segment held and
re-sends the park frame — delay, never loss.

### Control command `OpCommit` (SDK → controller)

```protobuf
message OpHandoffItem {
    uint32 channel_id = 1;
    bytes  payload    = 2;   // channel codec bytes of one collected value
}

message OpOutboxItem {
    bytes  segment_id = 1;   // wire identity of the producing segment
    uint32 seq_in_op  = 2;   // that segment's emit order within the operation
    string channel    = 3;   // producing plugin channel's name
    bytes  payload    = 4;   // serialized plugin-bound output
}

message OpCommit {
    string operative_id            = 1;
    uint32 op_idx                  = 2;
    uint32 iter                    = 3;   // 0 for the single-pass chain
    repeated bytes segments_ran    = 4;   // wire segment identities
    repeated OpHandoffItem handoff = 5;
    repeated OpOutboxItem outbox   = 6;   // staged plugin output
    bool compensating              = 7;   // undo record, not forward progress
}
```

Emitted at each operation barrier of a **durable structured** bounded
operative (`bounded-op-journal`): operation `op_idx` completed forward
and released the inter-op handoff batch, serialized through the
producing channel's byte codec (`.codec()` / `.codec_with(...)`,
required at registration for durable structured classes). Direction is
reversed like `ExecutorShutdownNotification`: SDK → controller over the
`"control_command"` payload_id. The driver **blocks** until the
`OpCommitResponse` — the next operation must not race ahead of the
committed record. Opted-out classes (`PERSISTENCE_ENABLED = false`)
skip the handshake entirely.

`outbox` carries the operation's **staged** plugin-bound output
(`bounded-transactional-outbox`). Inside a structured operation
`ctx.emit` does not reach the wire: each segment buffers its serialized
plugin output and it travels here, so a failed or aborted operation
leaks nothing. The controller journals handoff AND outbox in ONE
`op_committed` record (they cannot tear apart), and flushes the outbox
to the bound plugins only after that fsync — see `OpCommitResponse`. The
frame bound applies to the combined payload: an oversized handoff+outbox
is refused with the same typed error. The `run`-sugar form (no
operations, hence no barrier and no journal entry to gate on) keeps the
direct at-least-once wire path, as do opted-out classes.

`compensating = true` marks the undo twin: a `compensate` hook's staged
output for the already-committed operation `op_idx`, journaled as a
`compensated` entry and flushed with comp-marked keys. It is shipped
BEFORE that segment's `compensation_done` marker, so a crash between the
two re-runs the (idempotent) hook rather than losing its undo output. The
per-segment compensation markers remain the sole mechanism deciding
which hooks run on resume.

### Control command `OpCommitResponse` (controller → SDK)

```protobuf
message OpCommitResponse {
    string operative_id = 1;
    uint32 op_idx       = 2;
    string error        = 3;   // empty = journaled durably
}
```

Reply to `OpCommit` under the request's `reference_id`, emitted only
AFTER the `op_committed` entry is fsynced in the instance's
`op.journal` (journal-then-ack). A non-empty `error` (journal write
failure, oversized handoff past the WAL-watermark-scale frame bound)
terminates the run abnormally through the existing terminal-error path,
including saga compensation. A timed-out ack (30 s) is treated as
mid-run channel loss: the run terminates and crash-resume re-drives
from the last committed op (the un-acked op re-runs — at-least-once).

On crash-resume the journal state ships back on
`RestoreInitState.op_journal` (`OpJournalState`): committed operations
are skipped and the first uncommitted operation's input batch is seeded
from the journaled handoff via the channel codec's decode half. Inspect
a journal with `flz state journal <operative/instance>`.

`OpJournalState.rewind_undo` (`repeated OpCommittedOp`, tag 8) carries the
operations a **rewind** unwound that nothing has compensated yet
(`bounded-loop-rewind`). The driver runs them through the ordinary compensation
planner — reverse iteration, reverse chain position — before resuming forward at
`iter`, which the journal has already folded back to the rewound-to iteration.
Each undo journals a `compensated` entry (an empty one for a hookless operation),
so the list shrinks across a crash and re-planning never double-compensates. An
empty list means no rewind is pending. The rewind itself is a `rewound` journal
entry (type 6, target = the entry's `iter`), appended with `epoch + 1`; it never
crosses the wire as a command — the controller writes it and rebinds the
instance, and the folded journal is what the SDK sees.

**Outbox flush.** The response is the durable point past which the
operation's staged output may be seen, so the controller flushes it
immediately AFTER emitting the ack, in journal order, through the same
`:on_output` plugin fan-out live bounded output uses — stamping each
frame with the identity below. A successful flush appends an
`outbox_flushed` entry; a delivery that cannot happen yet (the operative
is not bound) is retried, never dropped. The next operation is free to
run while the flush lands: the chain hands data forward through the
handoff, never through the sink. On restore, every `op_committed` /
`compensated` entry without a matching `outbox_flushed` is re-driven with
its original keys before the resumed run advances — so a crash between
commit and flush duplicates at worst (at-least-once with stable keys) and
loses nothing.

**Bounded output identity.** Every flushed outbox frame carries the same
`flz.*` shape the effectively-once tier stamps, minted from the journal:

| Key | Value |
|---|---|
| `flz.segment` | `<instance_id>/<operation_name>/<segment_key>` of the producing segment |
| `flz.seq` | `<op_idx>.<seq_in_op>` — the operation index and the segment's emit order within it |
| `flz.epoch` | per-instance journal epoch; `0` at first start, carried verbatim across crash re-drives (only a future rewind bumps it) |
| `channel` | the producing plugin channel's name |

The tuple is a pure function of the journal, so a re-drive replays
byte-identical stamps. An undo output's `flz.seq` carries a trailing
`.1` component (`<op_idx>.<seq_in_op>.1`) — the compensation marker —
so it can never collide with the forward output it reverses while
remaining a numeric dotted sequence a keyed sink can order.

### Control command `IterCommit` (SDK → controller)

```protobuf
message IterCommit {
    string operative_id            = 1;
    uint32 iter                    = 2;
    uint64 consumed_offset         = 3;   // durable-inbox offset this iteration consumed
    repeated OpHandoffItem handoff = 4;   // the iteration's terminal batch
}
```

Emitted at each **iteration** boundary of a looping durable structured
bounded operative (`bounded-durable-loop`), after the last operation's
`OpCommit` has been acked. A class opts into looping with
`loop_mode()`: `PerInput` runs one iteration per inbox item (the
chat-turn shape), `Chained` seeds each iteration from the previous one's
terminal handoff (the refinement shape). The single-pass default
(`Once`) never emits this frame and is byte-identical to the
pre-loop protocol.

The controller appends ONE `iter_committed(iter, consumed_offset)`
journal entry — carrying the terminal handoff, so a crash can re-seed
iteration `iter + 1` — fsyncs, and acks. Everything after the ack is
cleanup the commit does not depend on: a boundary `CompactSegment` for
each of the instance's segments (so a rehydrate loads iteration-`iter`
state instead of replaying the WAL tail), the flush of whatever the
iteration's operations staged, and the release of the inbox prefix at or
below `consumed_offset`. A failure in any of them costs work, never
correctness.

Every `OpCommit` inside iteration `N` carries `iter = N`, so redo-vs-skip
works at both the operation and the iteration granularity from one scan.

### Control command `IterCommitResponse` (controller → SDK)

```protobuf
message IterCommitResponse {
    string operative_id = 1;
    uint32 iter         = 2;
    string error        = 3;   // empty = journaled durably
}
```

Reply under the request's `reference_id`, emitted only after the
`iter_committed` entry is fsynced. Same terminal-error and 30 s-timeout
contract as `OpCommitResponse`. The driver enters iteration `iter + 1`
only on success, so a crash re-runs at most the whole in-flight
iteration — never a committed one.

On crash-resume `RestoreInitState.op_journal` carries `iter` (the
iteration to resume at: the one after the highest journaled
`iter_committed`), `committed_op_idxs` (the operations already committed
*inside* that still-in-flight iteration, which are skipped),
`committed_ops` (every forward op commit with its iteration — the
compensation planner's per-iteration `committed` input, which is what
makes `RollbackMode::KeepProgress` keep journaled iterations), and
`consumed_offset`.

### Control command `IterPark` (SDK → controller)

```protobuf
message IterPark {
    string operative_id    = 1;
    uint32 iter            = 2;   // the last COMMITTED iteration
    uint64 wake_deadline_ms = 3;  // 0 = wake on input only
}
```

A `PerInput` loop whose inbox has been empty past the idle threshold
(`FLEZHA_BOUNDED_PARK_IDLE_MS`, default 5 s) at a **committed** iteration
boundary tears its own run down and emits this instead of a
`BoundedCompleted` — suspension is not a terminal outcome
(`bounded-durable-suspension`). Parking is only ever offered at a
committed boundary, so "suspended ⇒ resumable from a clean boundary"
holds; a `recv` mid-iteration blocks in memory as before.

The controller journals `parked(iter[, wake_deadline_ms])` — durable
BEFORE the eviction, so a controller crash in between still rediscovers
the suspension — then evicts the instance from its executor
(`RemOperative`). The registry entry and its controller-side Overwatch
stay: it is the **executor** that holds zero resources. `flz operative
info` reports the instance `suspended`.

Rehydration has one path, whichever wakes it: a new inbox append, or the
journaled wake deadline (which the controller delivers as a synthetic
inbox entry with an empty payload and `flz.wake = timer` metadata — a
class declaring `wake_after()` accepts an empty entry payload as its
tick). Both re-bind the instance through the normal bounded spawn path
on any live executor (existing session fencing applies), restoring
journal + iteration-boundary snapshots and delivering the undelivered
inbox tail. Because both wake sources go through the inbox, two racing
wakes collapse into exactly one rehydration.

### Data frame metadata: durable-inbox offset (`flz.inbox_offset`)

Post-start input for a **durable bounded** instance is appended to the
instance's `inbox.wal` **before** it is forwarded, and the forwarded
frame carries the assigned offset:

| Key | Value |
|---|---|
| `flz.inbox_offset` | 1-based durable-inbox offset of this input (absent ⇒ not journaled: an opted-out class) |

The SDK threads it back through `IterCommit.consumed_offset`, which is
what lets the controller release the consumed prefix and what stops a
rehydrate re-delivering it. Dedup at append is keyed on the sender's
`flz.msg_id` metadata when present, else a payload hash; a duplicate
inside the undelivered window is absorbed, so an at-least-once sender
cannot produce a duplicate turn.

### Data payload `correction_request` (SDK → controller)

Late-input correction (`unbounded-late-input-correction`). A Segment saw an
input whose extracted event time is behind its event-time high-water but inside
the declared `correction_window`. Only the SDK can know that — the event time
comes from the user's `TimeExtractor` — so detection is the SDK's half and
orchestration is the controller's.

Same outer frame as `park_payload` (a zero 16-byte header, a postcard body); the
Segment identity rides the frame envelope.

```rust
struct CorrectionRequestBody {
    target_event_time: u64,
}
```

Nothing else is needed. Inputs are journaled **before** they are forwarded, so
the late input is already in the durable input log at its arrival offset — the
controller re-reads the range and finds it there. There is no offset to send and
no slice to splice.

Fire-and-forget: the controller coalesces concurrent requests per Segment into
one replay, so a duplicate costs nothing, and a lost one is re-raised the moment
the input is re-delivered (it is still in the log, still unconsumed). A
transport-less run has nobody to ask and falls back to processing the input as
an arrival.

### Data frame metadata: replayed slice (`flz.replay`, `flz.replay_end`)

The controller marks every input of a re-forwarded slice so the Segment can hold
it, order it by event time, and only then dispatch — an arrival-ordered replay
would put the late input back after the inputs that overtook it and recompute
the same wrong answer.

| Key | Value |
|---|---|
| `flz.replay` | `"1"` on each input of a replayed slice |
| `flz.replay_end` | `"1"` on a **payload-less broadcast frame** that closes the slice |

`flz.replay_end` is a broadcast, not a flag on the slice's last input, and it has
to be: an input routes to exactly one Segment by key, so a marker riding the last
one would reach one Segment and leave every *other* Segment that buffered slice
members holding them forever. The broadcast frame carries no payload, never
reaches an Operation, and arrives after every frame of the slice.

A `RestoreInitState` clears any half-filled replay buffer, so an abandoned slice
(controller crash mid-replay) cannot strand a Segment — every replay is preceded
by a restore.

### Plugin metadata: output identity stamps (`flz.*`)

Channels declared `.keyed()` (valid on `to_plugin` and
`also_to_plugin_with` wires only — a keyed channel without a plugin wire
is a build error) stamp every plugin-bound emit with three reserved
metadata keys, appended after user metadata:

| Key | Value | Stability |
|---|---|---|
| `flz.segment` | `<operative_instance_id>/<operation_name>/<segment_key>` | replay-stable |
| `flz.seq` | `<change_seq>.<emit_ordinal>` — durable change seq at emit time (post flush-before-emit drain) + emit ordinal within the invocation | replay-stable while state advances |
| `flz.key` | caller's natural key from `OperationCtx::emit_keyed(key, value)` | replay-stable by the determinism contract |

On a keyed channel, plain `emit` / `emit_with_meta` fail with
`KeyRequiredOnKeyedChannel`; on a non-keyed channel `emit_keyed` fails
with `KeyNotAllowedOnNonKeyedChannel` — the channel declaration is the
single source of truth for whether output identity exists. The controller
passes the stamps through the `:on_output` plugin chain unchanged.

**`flz.seq` exactness boundary**: `flz.seq` suffices for changelog-style
consumers that upsert on `flz.key` (candles, indicators). It is NOT a
sufficient handle for exact append-only dedup: consecutive invocations
that apply no state change can produce colliding
`<change_seq>.<emit_ordinal>` pairs. Do not build append-only dedup on
`flz.seq` alone — the `effectively-once-output` capability journals
output identity in a durable outbox and upgrades it to an exact handle.
Sink-side adoption (e.g. the Kafka producer preferring `flz.key` as the
message key over `kafka_msg_key`) lands with that change; today the
producer's framework-key slot (`flezha.dedup_key`) is never stamped.

