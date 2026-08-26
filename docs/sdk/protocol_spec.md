# Flezha SDK Protocol Specification

This is the wire-protocol reference for SDK implementers: transports, binary
framing, the protobuf command set, the connection lifecycle, the data-plane
frames and the per-Entity persistence frames. Operative authors do not need it
— their surface is [`authoring_contract.md`](authoring_contract.md).

The protocol has two implementations: the shared Rust kernel `flz_core`
(linked by the Rust SDK as a crate and by the Java SDK through the
`flz_core_ffi` C ABI and the `FlzCore` JNI binding) and the Elixir SDK
(`Flezha.SDK`, `lib/flezha/sdk/`), its own core that inherits nothing from
`flz_core`. The host-facing Activation contract inside a `flz_core` runner is a
separate boundary: [`poll_core.md`](poll_core.md).

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
11. [Per-Entity State Persistence](#11-per-entity-state-persistence)

## 1. Overview

The **Controller** (Elixir/OTP) orchestrates work across one or more
**Runners** (SDK processes). A runner connects to the controller over one
transport, presents the operative factories it can host, and executes Operative
logic on the controller's behalf over two layers:

| Layer | Purpose | Socket | Encoding |
|-------|---------|--------|----------|
| Control plane | Handshake, command dispatch, lifecycle, durability barriers | control channel (one per runner) | protobuf inside the binary frame |
| Data plane | Operative input/output, heartbeats, per-Entity state records | N primary (controller → runner) + N secondary (runner → controller) streams | raw bytes inside the binary frame |

Of the command surface, only five interactions reach host code in a `flz_core`
runner (as `RestoreFold`, `SnapshotRequest`, `LiveStateRead`, `ApplyState`,
`MigrationSerialize` activations); the kernel answers everything else.

## 2. Transport Layer

A runner chooses its transport with `FLEZHA_TRANSPORT` (`uds`, default;
`quic`; `native`). The controller may listen on several at once via
`FLEZHA_TRANSPORTS=uds,quic` (in the `:test` environment both are on by
default); an unknown value is refused at boot. Everything above the transport
seam — framing, handshake content, command surface, identity, heartbeats — is
identical across transports.

### UDS (Unix domain sockets)

All sockets are `SOCK_STREAM`. The controller listens, the runner connects.

| Socket | Path | Direction |
|--------|------|-----------|
| Control (shared by every runner) | `{base}/runner_control_channel.sock` | both |
| Primary stream (per runner, ×N) | `{base}/primary_stream_{runner_id}.sock` | controller → runner |
| Secondary stream (per runner, ×N) | `{base}/secondary_stream_{runner_id}.sock` | runner → controller |

`{base}` is `FLEZHA_SOCKET_FILES_PATH` (controller default `app_data/sockets/`,
runner default `app_data/sockets`; a trailing slash is tolerated). The
per-runner listeners are created after the runner has its session token and
are served by a `StreamAcceptor` per stream pair.

### QUIC

With `FLEZHA_TRANSPORT=quic` the runner opens one QUIC connection (UDP) and
multiplexes every channel over bidirectional streams inside it.

| Setting | Env var | Default |
|---------|---------|---------|
| Controller listen address | `FLEZHA_QUIC_ADDRESS` | `0.0.0.0:4433` |
| Runner connect address | `FLEZHA_QUIC_ADDRESS` (runner side) | `127.0.0.1:4433` |
| CA certificate | `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` |
| Server certificate / key | `FLEZHA_SERVER_CERT_PATH` / `FLEZHA_SERVER_KEY_PATH` | `app_data/certs/server.pem` / `server.key` |
| Generated client identity (controller side) | `FLEZHA_CLIENT_CERT_PATH` / `FLEZHA_CLIENT_KEY_PATH` | `app_data/certs/client.pem` / `client.key` |
| Client trust anchor (controller side) | `FLEZHA_QUIC_CLIENT_CA` | `app_data/certs/ca.pem` |
| Client certificate / key presented by the runner | `FLEZHA_QUIC_CLIENT_CERT` / `FLEZHA_QUIC_CLIENT_KEY` | `app_data/certs/client.pem` / `client.key` |
| Peer authentication | `FLEZHA_TRANSPORT_AUTH` | required; `off` is the explicit opt-out, warned on every boot |

Certificates are generated on first start if absent (CA, server pair, a client
identity with the `clientAuth` EKU). The server verifies the client certificate
against `FLEZHA_QUIC_CLIENT_CA` and refuses the handshake otherwise — no frame
of an unauthenticated peer is ever parsed. The runner principal
(`runner@<namespace>`, role `runner`) is minted controller-side from the
handshake namespace, never carried on the wire
([`../operations/authn_authz.md`](../operations/authn_authz.md)). QUIC sends a
keep-alive every 5 s, declares the connection dead after
`FLEZHA_CONTROL_IDLE_TIMEOUT_MS` (default 45 s) of silence — the UDS watchdog's
window (§5) — and uses an 8 MiB per-stream receive window.

#### QUIC stream mapping

- The **first** bidirectional stream the runner opens is the control channel:
  no prefix bytes, same framing as UDS.
- Every **subsequent** bidirectional stream is a data stream and starts with a
  2-byte prefix: byte 0 is the stream type (`0x01` primary, `0x02`
  secondary), byte 1 the stream index. The NIF driver consumes the prefix; the
  framing layer never sees it.

### Native (in-node Elixir SDK)

An Elixir SDK hosted inside the controller's own BEAM node speaks `native`:
streams are `Flezha.Transport.Local.Stream` processes and frames are delivered
as `{:flz_data, id, payload_id, message, metadata}` terms — no framing, no
syscalls. Same-node only, opt-in. A standalone Elixir runner uses the binary
protocol over UDS/QUIC and is indistinguishable from a Rust runner. See
[`elixir_sdk.md`](elixir_sdk.md).

## 3. Binary Framing Protocol

Every message on every socket — control commands and data-plane payloads
alike — uses one frame format.

### Frame layout

```text
Offset  Size   Field          Description
------  -----  -------------  ----------------------------------------------
0       32     Payload ID     UTF-8, right-padded with spaces (0x20)
32      4      Payload Size   uint32, big-endian
36      4      Message Size   uint32, big-endian
40      M      Message        {Message Size} bytes
40+M    2      Metadata Size  uint16, big-endian
42+M    D      Metadata       {Metadata Size} bytes (see "Metadata serialization")
```

```text
Payload Size = 4 + Message Size + 2 + Metadata Size
Total frame  = 36 + Payload Size
```

`PAYLOAD_ID_LEN` (32) and `HEADER_LEN` (36) are exported by
`flz_transport::service::payloader`.

### Payload ID

Exactly 32 bytes. A shorter logical id is right-padded with ASCII spaces; a
longer one is truncated to 32 bytes. Readers trim trailing spaces to recover
the logical id.

| Payload ID | Direction | Context |
|------------|-----------|---------|
| `connect_control_channel` | runner → controller | Handshake init (JSON message) |
| `control_command` | both | Protobuf `CommandRequest` / `CommandResponse` |
| `control_ping` | runner → controller → runner | Keep-alive; a bare `Ping` the controller echoes verbatim (§5) |
| `data_heartbeat` | runner → controller | Data-channel liveness, empty message + counters in metadata |
| `{operative_id}` | both | Operative input (primary) and output (secondary) |
| `entity_state_change`, `entity_effect_record`, `entity_state_snapshot` | runner → controller | Per-Entity persistence records (§11) |
| `compensation_intent`, `compensation_done` | runner → controller | Workflow saga markers (§11) |
| `delete_entity_state` | runner → controller | Dynamic-Entity hard delete (§11) |
| `park_payload` | runner → controller | Dead-letter park (§11) |
| `correction_request` | runner → controller | Late-input correction (§11) |

### Frame limits

| Knob | Default | Meaning |
|------|---------|---------|
| `FLEZHA_MAX_FRAME_BYTES` | 64 MiB | Largest declared payload length either parser accepts; read once at boot |
| `FLEZHA_MAX_WRITE_BUFFER_BYTES` | 8 MiB | Per-stream send-buffer cap in the UDS and QUIC drivers |

Both are defined once (`max_frame_bytes()` / `max_write_buffer_bytes()` in
`flz_transport/src/transport/config.rs`) and shared by the runner-side
payloader (`flz_transport/src/service/payloader.rs`) and the controller's NIF
`FrameBuffer` (`flz_nif/src/frame_buffer.rs`). Both parsers reject a declared
length **before allocating for it**, on raw bytes before any authentication
verdict — the only pre-identity defence on an exposed port: an outer
`Payload Size` above `FLEZHA_MAX_FRAME_BYTES` is refused, an inner
`Message Size` above `Payload Size − 6` is refused, and a rejected header
**poisons the stream** — there is no recoverable frame boundary after a bad
header, so there is no resync. The runner surfaces
`io::ErrorKind::InvalidData` (stream-fatal, §10); the controller's
`FrameBuffer` enters `Poisoned` and the driver closes the stream with reason
`frame_too_large`.

### Metadata serialization

Metadata is a map of UTF-8 string keys to UTF-8 string values, serialized as a
flat sequence of entries:

```text
for each (key, value):
  [key_length: u8] [value_length: u8] [key: key_length bytes] [value: value_length bytes]
```

Keys and values are limited to 255 bytes by the 1-byte length prefix (the
serializer does not check this — longer strings corrupt the frame). An empty
map serializes to zero bytes (`Metadata Size = 0`). Entry order is
unspecified.

#### Reserved metadata keys

| Key | Direction | Meaning |
|-----|-----------|---------|
| `input_type` | controller → runner | Stamped by the controller on every entry-channel input from the Operative's `:default_input_type` (`"2"` unless declared; `"1"` binary, `"2"` text). The SDKs do not read it: payload bytes are converted by the entry channel's declared payload type (`Vec<u8>`/`byte[]` passthrough, `String` UTF-8, anything else through the channel's entry parser). |
| `channel` | controller → runner | The Operative's entry channel that receives this input. Absent ⇒ the Operative's first declared input channel. |
| `channel` | runner → controller | Set on every plugin-bound emit. Plugin providers match on it via their `:channel` option to select which channel's outputs they consume. |
| `flz.entity` | runner → controller | Output-identity stamp on `.keyed()` channels: `<operative_instance_id>/<procedure_name>/<entity_key>` (empty key for single-mode). |
| `flz.seq` | runner → controller | Output-identity stamp: `<change_seq>.<emit_ordinal>` (stream) or `<proc_idx>.<seq_in_proc>` (workflow outbox). |
| `flz.key` | runner → controller | Output-identity stamp: the emitting Entity's own key, or the caller's natural key from `emit_keyed(key, value)`. |
| `flz.epoch` | controller → plugins | The Entity's WAL realign epoch (stream tier) or the instance's journal epoch (workflow outbox). `(flz.entity, flz.seq, flz.epoch)` is the exact dedup identity a conforming sink deduplicates on. |
| `flz.ack` | controller → plugins | Delivery-confirmation token minted by a durable outbox flush; the owning writer advances its cursor when the sink confirms or parks it. Absent on live non-durable output. |
| `flz.offset` | controller → runner | Effectively-once tier: the durable input-log offset of this forwarded input. The runner threads it through dispatch so the invocation's effect record carries it back as `consumed_offset`. |
| `flz.inbox_offset` | controller → runner | Durable workflow inbox: the 1-based offset of this post-start input (§11). |
| `flz.msg_id` | sender → controller | Sender-supplied message id used to deduplicate durable-inbox appends (§11). |
| `flz.wake` | controller → runner | `"timer"` on the synthetic empty inbox entry that wakes a parked loop at its journaled deadline (§11). |
| `flz.replay`, `flz.replay_end` | controller → runner | Replayed-slice markers for late-input correction (§11). |
| `flz.change_seq` | runner → controller | On the `EntityState` reply frame: the seq a single selected Entity was serialized at (§6). |

The `flz.` prefix is a protected namespace: a user-supplied metadata key
beginning with `flz.` is rejected at emit time with a typed error
(`ProcedureError::ReservedMetadataKey` in Rust,
`ProcedureException.ReservedMetadataKey` in Java) on every emit variant, so
runtime stamps can never be spoofed or clobbered. Stamps are appended after
user metadata; channels not declared `.keyed()` carry none.

Channel names on the wire are hand-written identity strings (a non-entry
channel's name derives from its producer node unless `.named(...)` overrides
it), so renaming a language class never changes a wire or on-disk name. A
plugin-bound frame is byte-identical whether or not the channel also fans out
to inter-procedure consumers.

### Heartbeat frames

`data_heartbeat` (empty message) is written on **every** secondary stream every
`FLEZHA_DATA_HEARTBEAT_MS` (default 10 s, jittered into `[base/2, base]`)
while connected, so an idle-but-healthy Operative keeps the controller's
per-channel liveness clock fresh. Its metadata is the runner's self-report:
`dispatches` (completed dispatches — what the `runner_pool_wedged` classifier
reads), `blocked_parks`, `drops_newest`, `drops_oldest`, `inflight`,
`inflight_oldest_ms`, `pending_pushes`, `pending_push_oldest_ms` (Entities
withheld work because a consumer will not take their output, and the age of the
oldest such push — a gated producer holds no handout, so `inflight` cannot see
it), `credit_parked`, `restore_discarded_inputs`, `timers_armed`,
`timers_fired`. The controller reads `dispatches` and
`credit_parked`, bumps `hb_count`, and treats any inbound frame as liveness; a
bound channel silent past `:io_stream_liveness_threshold_ms` (default 30 s) is
flagged as a stall.

### Per-Entity mailbox is bounded; on full, the channel's `OverflowPolicy` applies

The runner's per-Entity mailbox is a fixed-capacity queue
(`crossbeam::queue::ArrayQueue`) sized at Operative startup. A frame for a
full mailbox takes the channel's declared `OverflowPolicy`: `DropNothing`
(nothing is discarded — the producer waits for a slot, which on an
inter-procedure channel means the producing Entity is withheld further work and
on an entry channel means the reader stalls; the default for every
channel kind in every SDK), `DropNewest` or `DropOldest` (a counter is
bumped). Under `DropNothing` a saturated entry channel parks the **wire-reader
thread** and sibling Operatives on that connection wait with it; the reader
never runs procedures itself, and `DropNothing` requires an acyclic procedure
graph. Capacity declaration
and counters: [`rust_operative_architecture.md`](rust_operative_architecture.md)
"Backpressure and bounded mailboxes".

### State migration JSON shape

`MigrationSerialization` / `MigrationDeserialization` / `EntityState` carry a
JSON blob whose shape is:

- Single-mode procedures (no Entity keys): `{"<procedure_name>": <state>}`
- Static-mode procedures (`entity_keys()` non-empty):
  `{"<procedure_name>": {"<entity_key>": <state>, ...}}`
- Dynamic-mode procedures (keyed on the wiring with `.key_by` / `.keyBy`): the
  same key-keyed object over the Entities the runner holds live at
  serialization time.

Both SDKs sort object keys, so an equivalent state serializes byte-identically
on either runtime.

### Frame buffering

Sockets are stream-oriented: a frame may arrive split across reads, and one
read may hold several frames. A reader MUST buffer an incomplete header (fewer
than 36 bytes), buffer an incomplete payload (header complete, fewer than
`Payload Size` bytes), and parse every complete frame in a read. The
controller's NIF `FrameBuffer` is a four-state machine — `Empty`,
`PartialHeader`, `PartialPayload`, `Poisoned` — whose fast path splits a full
header straight off the input slice without copying.

## 4. Protobuf Command Protocol

Control-plane commands are Protocol Buffers v3 (`proto3`, package
`runner_commands`). The authoritative definition is
`protobufs/runner_commands.proto`; the generated Rust module is
`flz_core/src/generated/runner_commands.rs` (and a twin in `flz_transport`),
the Elixir decoder is `Flezha.Runner.CommandsProto` (Protox).

### Envelope

```protobuf
message CommandRequest {
    string reference_id = 1;
    oneof command { ... }            // one of the 34 variants below
}

message CommandResponse {
    string reference_id = 1;         // echoes the request
    string status = 2;               // "success" | "error"
    repeated string errors = 3;      // human-readable, first token canonical where stated
    bytes raw_data = 4;              // command-specific reply payload, often empty
}
```

### The `command` oneof

| Tag | Variant | Direction | Reply |
|-----|---------|-----------|-------|
| 2 | `AssignRunnerId` | C → R | `CommandResponse` |
| 3 | `ConnectSocket` | C → R | `CommandResponse` |
| 4 | `AddOperative` | C → R | `CommandResponse` (`raw_data` = `AddOperativeResponse`) |
| 5 | `RemOperative` | C → R | `CommandResponse` |
| 6 | `MigrationSerialization` | C → R | `CommandResponse` (`raw_data` = JSON state) |
| 7 | `MigrationDeserialization` | C → R | `CommandResponse` |
| 8 | `EntityState` | C → R | `CommandResponse` (`raw_data` = JSON state; frame metadata `flz.change_seq`) |
| 9 | `RunnerDisconnect` | C → R | `CommandResponse` |
| 10 | `OperativeMessage` | C → R | `CommandResponse` (`raw_data` = `OperativeMessageResponse`) |
| 11 | `EntityApplyState` | C → R | `CommandResponse` (reserved; see §6) |
| 12 | `RunnerShutdownNotification` | R → C | `RunnerShutdownAck` (tag 13) under the same `reference_id` |
| 13 | `RunnerShutdownAck` | C → R | none |
| 14 | `RestoreInitState` | C → R | `CommandResponse` (`raw_data` = `RestoreInitStateResponse`, tag 15) |
| 16 | `CompactEntity` | C → R | `CommandResponse` (`raw_data` = `CompactEntityResponse`, tag 17) |
| 18 | `EntityMailboxStatus` | C → R | none (one-way) |
| 19 | `Ping` | C → R | `CommandResponse` (`raw_data` = `PingResponse`) |
| 20, 21 | `RunnerReconnected`, `ReassignRunnerId` | — | reserved; never emitted (§6) |
| 22 | `WorkflowCompleted` | R → C | none |
| 23 | `WorkflowStop` | C → R | `CommandResponse` |
| 24 | `CreditWindowInit` | C → R | `CommandResponse` (`raw_data` = `CreditWindowAck`, tag 25) |
| 26 | `CreditGrant` | C → R | generic `CommandResponse`, ignored |
| 27 | `ParkPayloadAck` | C → R | generic `CommandResponse`, ignored |
| 28 | `ProcCommit` | R → C | `ProcCommitResponse` (tag 29) under the same `reference_id` |
| 29 | `ProcCommitResponse` | C → R | none |
| 30 | `IterCommit` | R → C | `IterCommitResponse` (tag 31) under the same `reference_id` |
| 31 | `IterCommitResponse` | C → R | none |
| 32 | `IterPark` | R → C | none |
| 33 | `UpdateFactories` | R → C | none (fire-and-forget) |
| 34 | `SpawnWorkflowRequest` | R → C | `SpawnWorkflowResponse` (tag 35) under the same `reference_id` |
| 35 | `SpawnWorkflowResponse` | C → R | none |

Tags 15, 17 and 25 are message types that only ever ride as a
`CommandResponse.raw_data`, never as a `CommandRequest`.

### Request/response pattern

- **Framing.** Both messages are protobuf-encoded and wrapped in a frame with
  payload id `control_command` and (normally) empty metadata; a runner answers
  under the payload id it received.
- **Correlation.** `reference_id` is opaque; the runner echoes it in its
  `CommandResponse`. A runner-initiated request (tags 12, 28, 30, 34) is
  answered by a controller-initiated `CommandRequest` carrying the same
  `reference_id`.
- **Runner-initiated variants.** Tags 12, 22, 28, 30, 32, 33 and 34 travel
  runner → controller in the same envelope. The controller decodes every
  inbound `control_command` frame as a `CommandRequest` first and dispatches a
  recognised runner-initiated tag there; anything else goes to the
  `ControlUnit` as a `CommandResponse`.
- **Unknown variants.** Both sides skip an unrecognised oneof tag via the
  decoder's unknown-field handling (a `flz_core` runner treats tags 20 and 21
  as `NotificationIgnore`).
- **Reply metadata.** A handler may attach metadata to the reply **frame**
  (`EntityState` → `flz.change_seq`).

## 5. Connection Lifecycle

```text
Runner                                           Controller
  │── 1. connect to the control socket ──────────>│
  │── 2. "connect_control_channel" (JSON) ───────>│  3. mint session token,
  │<─ 4. AssignRunnerId{runner_id} ───────────────│     resolve stable_id
  │── 5. CommandResponse(success) ───────────────>│
  │<─ 6. ConnectSocket{sockets_count: N} ─────────│
  │   7. connect N primary + N secondary streams  │
  │── 8. CommandResponse(success) ───────────────>│
  │<─ 9. Ping / AddOperative / ... (READY) ───────│
  │── 9. CommandResponse ────────────────────────>│
  │<─ 10. RunnerDisconnect ───────────────────────│
  │── 10. CommandResponse(success) ──────────────>│  11. both sides close
```

### Step 2: handshake init message

One frame, payload id `connect_control_channel`, empty metadata, JSON message:

```json
{
  "namespace": "<namespace>",
  "factory_ids": ["<factory_id>", ...],
  "factories": [{"id": "<factory_id>", "description": null}, ...],
  "stable_id": "<namespace>-<profile_hash8>-<rand6>"
}
```

- `namespace` — the logical group the runner belongs to (also the namespace
  of the `runner@<namespace>` principal).
- `factory_ids` / `factories` — every factory the runner currently declares
  (`factories` adds an optional `description`), read at write time so a
  reconnect resyncs post-start defines/removes: the init frame is the resync
  of record.
- `stable_id` — the runner's durable, self-owned identity, present on **every**
  connect (`profile_hash8` hashes the declared factory set order-independently,
  `rand6` is a per-process random suffix). The controller binds operatives to
  it and on a reconnect swaps the prior session's bindings onto the new token.
  See [`../internals/runner_identity.md`](../internals/runner_identity.md).

### Step 3: controller processing

`Flezha.Transport.Server` hands the frame to
`Flezha.Runner.FleetManager.connect_handler/4`, which mints a fresh
**session token** (the `runner_id` below), resolves `stable_id`, stores the
factory set, starts the runner's supervision tree (`ControlChannel`,
`ControlUnit`, stream supervisors) and starts or joins the namespace hub,
bounded by `:cluster_formation_timeout_ms` (15 s) + 5 s.

### Steps 4–5: `AssignRunnerId`

The runner stores the session token (it keys the per-session data sockets),
sets its status to `Running` and replies `success`. The token is **not**
persisted; the durable identity is the `stable_id` the runner presented.
Before the ack the runner's control read deadline is
`FLEZHA_CONTROL_ACK_TIMEOUT_MS` (default 10 s): a controller that accepts the
socket mid-shutdown but never registers the runner is a failed attempt, not a
hang.

### Steps 6–8: `ConnectSocket`

`sockets_count` is `FLEZHA_CONCURRENT_CHANNEL_COUNT` (controller side; `0`,
the default, means `System.schedulers_online()`). For each index the runner
connects one primary stream (set non-blocking, registered for read polling) and
one secondary stream (kept for output), then replies `success`.

### Step 9: steady state

The controller issues a `Ping` round-trip (default timeout
`:overwatch_ping_timeout_ms`, 2 s) before each `AddOperative`, then the
commands of §6. A reply the `ControlUnit` has not seen after
`:control_unit_pending_reply_timeout_ms` (90 s) is failed to its caller.

**Control keep-alive (`control_ping`).** The keep-alive is **runner-initiated**:
every `FLEZHA_CONTROL_KEEPALIVE_MS` (default 15 s, jittered) the runner writes
a bare `Ping{nonce}` under payload id `control_ping`. The controller echoes
the frame verbatim on the same payload id and stamps `last_seen`; the ping
never enters the command dispatcher on either side. The runner re-arms its
idle watchdog on **every** inbound control frame; `FLEZHA_CONTROL_IDLE_TIMEOUT_MS`
(default 45 s) of silence means the controller is wedged or half-open and the
runner drives a reconnect. The controller runs the mirror-image watchdog
(`FLEZHA_CONTROL_LIVENESS_WINDOW_MS` 45 s, checked every
`FLEZHA_CONTROL_LIVENESS_INTERVAL_MS` 15 s) and treats sustained silence as a
close, emitting `[:flezha, :runner, :liveness_timeout]`. Details:
[`../internals/transport_liveness.md`](../internals/transport_liveness.md).

### Step 10: disconnect

`RunnerDisconnect` sets the runner's status to `Stopped`; the runner replies
`success`, the controller closes the control socket, and the runner closes its
streams. This is intentional — the runner MUST NOT reconnect.

### Auto-reconnection

A control read error while the status is `Running` drives a reconnect. The
runner keeps its operative registry and per-Entity state, marks itself
`Stopped` for the duration, re-dials with the same handshake (same
`stable_id`) and returns to `Running` on the new stream. Every connect attempt
retries forever with full-jitter exponential backoff:
`jittered(min(100 ms × 2^min(attempt, 8), 30 s))` — 200 ms, 400 ms, … 25.6 s,
capped at 30 s, each drawn from `[base/2, base]`. Reasons (logged, and carried
by `[:flezha, :runner, :reconnected]`): `control_stream_broken`, `read_error`,
`write_error`, `keepalive_timeout`, `controller_requested`, and
`broken_pipe_flood` from the reliable state sink (§7).

On the controller the reconnect is a **binding swap**: the re-presented
`stable_id` resolves to the prior session, its bindings move to the new token,
and `AddOperative` is re-issued for every instance still bound to the runner;
the runner's handler is idempotent on a duplicate `operative_id`, so existing
Entity state survives. A runner that never returns is removed after
`FLEZHA_RUNNER_KEEPALIVE` (default 30000 ms; `0` disables the grace).

## 6. Command Reference

Every handler answers `status: "success"` unless stated otherwise; an error
reply carries `status: "error"` and one or more strings in `errors`.

### AssignRunnerId

`AssignRunnerId { runner_id: string }` carries the controller-minted session
token. The runner stores it and sets status `Running`; it is never persisted
(the durable identity is `stable_id`).

### ConnectSocket

`ConnectSocket { sockets_count: uint32 }`: connect `sockets_count` primary +
secondary stream pairs (§5).

### Ping

`Ping { nonce: string }` is answered with `raw_data` = `PingResponse { nonce }`
echoing the nonce. Issued before `AddOperative` and by `flz` diagnostics; the
round-trip proves the channel is bound, the runner is listening and the reply
path is intact.

### AddOperative

```protobuf
message AddOperative {
    string operative_id = 1;
    string factory_id = 2;
    string init_values = 3;            // JSON; "" for none
    string kind = 4;                   // "stream" | "workflow"; empty ⇒ "stream"
    uint32 resume_entity_count = 5;    // RestoreInitState frames to expect on this bind
    ProcJournalState proc_journal = 6; // workflow crash-resume state (§11)
    uint32 partition_index = 7;
    uint32 partition_count = 8;        // < 2 ⇒ unpartitioned
    uint32 key_group_from = 9;         // inclusive key-group range, 0..255
    uint32 key_group_to = 10;
    uint32 inbox_tail_count = 11;      // journaled inbox items forwarded after the bind
}
```

| Field | Value |
|-------|-------|
| Behaviour | Create an instance of `factory_id` under `operative_id`. `"stream"`: a stream operative whose non-empty `init_values` JSON is applied as initial state. `"workflow"`: a workflow instance whose run is scheduled once on its driver thread with `init_values` as its argument; a durable driver holds its first procedure until `resume_entity_count` restores have landed, and its park-idle clock starts only once `inbox_tail_count` inbox items have arrived |
| Idempotency | A duplicate `operative_id` (a reconnect re-issue) is answered `success` without re-registering |
| Reply | `raw_data` = `AddOperativeResponse { map<string,string> metadata }` — empty when the class declares nothing |
| Errors | factory not found; JSON parse / deserialization failure; unknown `kind`; `PartitionsWithoutKeyedProcedure: ...` |

`AddOperativeResponse.metadata` carries the class's SDK-side declarations so
the controller resolves per-class capabilities without a parallel config
channel (absent/undecodable ⇒ every default):

| Key | Value | Meaning |
|-----|-------|---------|
| `effectively_once` | `"true"` | the class runs on the effectively-once tier |
| `durability` | `"sync_remote"` | workflow only: withhold barrier acks until the remote backend holds them |
| `keyed` | `"false"` | stream class with no keyed entry-fed procedure (sent only when false) |
| `correction_window_ms` | ms | late-input correction window |
| `beyond_window_policy` | `"process_as_arrival"` / `"drop"` | what a late input beyond the window gets |
| `ack_granularity` | `"checkpoint"` | the class acknowledges input only at checkpoints |
| `compact_after_bytes` | bytes | the smallest per-procedure compaction trigger, when it differs from the default |
| `compensable_procs` | list | workflow tree positions whose procedure declares `compensate` |
| `leaf_procs` | list | the workflow tree's leaf positions |

#### Partition identity

`partition_index` / `partition_count` / `key_group_from` / `key_group_to`
carry one member of a partitioned operative over 256 key groups
([`partitioned_operatives.md`](partitioned_operatives.md)). With
`partition_count < 2` the instance owns the whole space; otherwise the runner
checks that each computed Entity key hashes into
`[key_group_from, key_group_to]` and parks a miss with an error naming the
key, its key group and the owning partition. A migrate-time `AddOperative`
carries the same fields. A class with no keyed entry-fed procedure is refused
with `PartitionsWithoutKeyedProcedure` and answers `keyed: "false"` so the
controller refuses a later partitioned start before touching a runner.

### RemOperative

`RemOperative { operative_id: string }` removes the instance from the registry
(hard teardown). Error: `StreamOperative instance not found`.

### WorkflowStop

`WorkflowStop { operative_id: string }` is the cooperative stop
(`flz operative stop`): the runner trips the instance's cancellation token,
lets in-flight work unwind, runs any saga compensation, then emits the normal
`WorkflowCompleted` with status `"cancelled"`. An unknown id is answered
`success` and ignored.

### WorkflowCompleted

`WorkflowCompleted { operative_id, status, error, result: bytes,
direct_outputs: uint32 }` travels runner → controller, one-way, when a
workflow instance reaches an exit trigger. `status` is `"ok"`, `"error"` (with
`error`) or `"cancelled"`; `result` is the terminal payload. The controller
drives the single stop/teardown path (deregister-on-completion) so a finished
instance leaves the durable registry and is never replayed on boot; an errored
one is torn down identically (terminal — the platform never re-runs it; a
structured chain's per-Entity retry ladder runs *inside* the run) and surfaced
via `[:flezha, :workflow, :failed]`.

`direct_outputs` counts the plugin-bound outputs an opted-out (or `run`-sugar)
instance wrote straight to the data stream before completing: the two frames
travel different sockets, so the controller holds the teardown until its
Overwatch has run that many outputs (60 s drain deadline). A durable class's
outputs ride the journaled outbox and are not counted. A workflow instance's
direct output is a frame whose payload id is the instance id, on a secondary
stream.

### EntityState

| Field | Value |
|-------|-------|
| Message | `EntityState { operative_id, procedure, entity_key }` |
| Behaviour | Serialize live in-memory Entity state as JSON (shape in §3), narrowed by the selector |
| Reply | `raw_data` = JSON; reply-frame metadata `flz.change_seq` when the selector names exactly one Entity |
| Errors | `StreamOperative instance with id {id} not found`, plus the refusal tokens below |

**Selector.** `procedure` and `entity_key` are optional (empty = absent), so a
selector-less command encodes byte-identically to the migration/handover
shape. Absent ⇒ every Procedure; `procedure` ⇒ that Procedure's Entities;
`procedure` + `entity_key` ⇒ one Entity.

**Freshness.** Serialization takes each Entity's state lock and does NOT drain
pending changes: the value is at or ahead of the durable state, and
`flz.change_seq` tells a reader which version it got. A read triggers no
compaction, snapshot or WAL truncation.

| Refusal token (leads the error string) | Meaning |
|---|---|
| `entity_key_without_procedure` | an Entity key is unique only within its Procedure |
| `procedure_not_found` | the Operative declares no Procedure by that name |
| `entity_not_found` | that Procedure holds no Entity by that key |
| `entity_evicted` | the Entity was idle-evicted; eviction is destructive, so there is no durable value either |

The operator surface is `flz state get`
([`../internals/state_persistence.md`](../internals/state_persistence.md)).

### EntityApplyState

`EntityApplyState { operative_id, factory_id, state_binary: bytes }` is
reserved: the controller never sends it. A `flz_core` runner answers an
unknown instance with `StreamOperative instance with id {id} not found` and a
known one with `status: "error"`, `errors: ["apply_state_not_supported"]` —
the canonical token an SDK without apply support MUST return.

### MigrationSerialization

`MigrationSerialization { operative_id }` serializes the instance's whole state
(JSON shape in §3) into `raw_data`, then **removes** the instance from the
registry so the controller can re-create it on another runner.

### MigrationDeserialization

`MigrationDeserialization { operative_id, factory_id, serialized_state: bytes }`
creates the instance from `factory_id`, then applies `serialized_state`.

### RunnerDisconnect

`RunnerDisconnect {}`: set status `Stopped`, reply, close the control socket;
never reconnect.

### OperativeMessage

`OperativeMessage { operative_id, payload: bytes }` is answered with
`status: "success"` and `raw_data` = `OperativeMessageResponse { payload:
bytes, error: string }`. An empty `error` means success and the controller
reads `payload`; a non-empty `error` means the instance reported a failure and
the controller SHALL discard `payload`. A `flz_core` runner always answers
`error: "operative does not support handle_message"` with an empty payload —
the kernel has no per-instance message hook. Sent by
`Flezha.Plugin.Provider.MessageForward` via
`ControlUnit.send_operative_message/3`
([`../plugins/message_handlers.md`](../plugins/message_handlers.md)); the
caller owns the timeout, and payloads share the control stream — keep them
small.

### RunnerShutdownNotification

`RunnerShutdownNotification { operative_id, serialized_state: bytes }` travels
runner → controller in the `CommandRequest` envelope (tag 12) on graceful
shutdown only — SIGTERM, drop of the SDK handle, explicit `disconnect()`;
SIGKILL and panics cannot emit and fall back to the last persisted state. Per
running instance the runner serializes through the same serializer
`MigrationSerialization` uses and emits one notification; the controller
forwards the bytes to the instance's Overwatch snapshot and answers
`RunnerShutdownAck` under the same `reference_id`. The runner waits at most
**2 s per operative** (parallelised), then exits unconditionally and never
blocks its container's grace period; the per-operative serializer is capped at
the controller's `cross_node_request_timeout` ceiling (10 s).

### RunnerShutdownAck

`RunnerShutdownAck { operative_id }` travels controller → runner in the
`CommandRequest` envelope (tag 13), emitted as soon as the snapshot cast is
enqueued on the Overwatch (not after it is applied) and unconditionally — a
missing Overwatch (already migrated to a peer controller) still gets an ack so
the runner can exit.

### RunnerReconnected / ReassignRunnerId

Tags 20 and 21 are defined in the proto but **never emitted**: a reconnect is
resolved entirely by the re-presented `stable_id` in the handshake (§5). A
`flz_core` runner drops either variant as a notification if one ever arrives.

### UpdateFactories

`UpdateFactories { repeated FactoryDecl factories }` (`FactoryDecl { id,
description }`) travels runner → controller, fire-and-forget, after a
post-start define/remove on the runner handle, carrying the runner's **full**
current factory set. Full-set semantics make it idempotent: `FleetManager`
replaces the runner's stored list (remove-then-store), scoped to the sender; an
empty set is legal and leaves the runner connected with zero placeable
factories. No ack: it gates no durability barrier — a stale placement fails at
the runner's `AddOperative` handler and convergence re-places it; a lost frame
is corrected by the next one or by the reconnect init frame. Dropping a factory
that still has operatives bound to the runner logs a `FleetManager` warning
naming namespace, runner, factory id and affected operatives.

### SpawnWorkflowRequest

`SpawnWorkflowRequest { instance_id, factory_id, payload: bytes, if_running:
string }` travels runner → controller (tag 34) when a procedure asks for a
workflow instance (`ctx.spawn_workflow` / `ctx.spawnWorkflow`). The controller
spawns `factory_id` under `instance_id` when free, with `payload` as the first
inbox entry (empty = spawn only); otherwise `if_running` applies: `"deliver"`
(default; any unrecognised value reads as deliver) appends `payload` to the
running instance's inbox, `"error"` refuses.

**The namespace is not carried**: the controller takes it from the control
channel the request arrived on, which makes cross-namespace spawning impossible
by construction. Resolution runs off the control channel (it starts an
operative, which awaits `AddOperative` on the same stream); concurrent requests
for one `(namespace, instance_id)` race-resolve at the registry — one start
wins, every loser converts to a delivery. See
[`procedure_requested_spawn.md`](procedure_requested_spawn.md).

### SpawnWorkflowResponse

`SpawnWorkflowResponse { outcome, error }` returns controller → runner under
the request's `reference_id`; `outcome` ∈ `spawned`, `delivered`,
`already_running`, `factory_unknown`, `refused`, and `error` details a
`refused`. It resolves the requester's result handle and never faults the
requesting procedure (an unobserved handle logs the failure once); against an
older controller the request is ignored and the requester times out.

The persistence commands (`RestoreInitState`, `CompactEntity`,
`EntityMailboxStatus`, `CreditWindowInit` / `CreditWindowAck` / `CreditGrant`,
`ParkPayloadAck`, `ProcCommit`, `IterCommit`, `IterPark`) are specified in
§11.

## 7. Data Plane (Stream I/O)

| Stream | Direction | Carries |
|--------|-----------|---------|
| Primary | controller → runner | Operative input, replay slices, wake entries |
| Secondary | runner → controller | Operative output, `data_heartbeat`, every persistence frame of §11 |

Stream frames use the §3 format with payload id = the target instance's
`operative_id` (or one of the reserved ids of §3).

**Input (primary streams).** The runner resolves the instance by payload id
(an unknown instance is logged and the frame dropped). For a **stream**
instance it resolves the entry channel from `metadata["channel"]` (absent ⇒
the first declared input channel), wraps the payload bytes through that
channel's declared type, and pushes the item onto every consumer Procedure's
Entity mailbox under the mailbox policy of §3; `flz.offset`, `flz.replay` and
`flz.replay_end` ride along as described in §11. For a **workflow** instance
it routes the frame to the running driver's inbox with the `flz.inbox_offset`
it carries (an instance that is not running or has no inbox drops it with a
log line). Dispatch onto host threads, per-Entity ordering and exclusivity are
the poll core's job ([`poll_core.md`](poll_core.md)).

**Output (secondary streams).** Every plugin-bound emit becomes one frame with
payload id = the emitting instance's id, the output bytes as the message, and
metadata = user metadata + `channel` (+ the `flz.*` identity stamps on
`.keyed()` channels). Outputs are batched into one write syscall where possible
(`payloader::build_from_binary_into`). On the effectively-once tier plugin
emits never ride this path: they travel inside the effect record (§11) and the
controller dispatches them only after the record's fsync.

**Reliable state sink.** Persistence frames (§11) go through a reliable sink:
frames produced while no secondary stream is installed, or whose write failed,
are queued in a ring of `FLEZHA_RECONNECT_RING_CAPACITY` (default 1024; the
oldest is dropped on overflow) and replayed under their own payload id once a
stream is back. `FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD` (10) write failures
inside `FLEZHA_BROKEN_PIPE_WINDOW_MS` (5000) request a control reconnect with
reason `broken_pipe_flood`, re-armed after `FLEZHA_RECONNECT_REQUEST_RETRY_MS`
(30000) if unanswered.

**Non-blocking I/O.** Primary sockets are non-blocking and polled through an
event mechanism (the `flz_core` reader uses `mio`;
`extract_payload_non_blocking` returns `None` on `WouldBlock`). A wake may
carry several frames — drain them all before re-polling. A secondary-stream
write that returns `WouldBlock` is retried on the next writable event.

## 8. Recommended SDK Public API

Not part of the wire contract. The authoring surface every SDK must offer —
factories, Procedures, Entities, channels, codecs, the runner handle and its
env knobs (`FLEZHA_WORKER_POOL_SIZE`, `FLEZHA_WORKER_POOL_MAX`,
`FLEZHA_MAX_INFLIGHT_ACTIVATIONS`) — is specified in
[`authoring_contract.md`](authoring_contract.md); the kernel/host split that
decides which of it lives below the poll seam is in
[`../architecture/runner.md`](../architecture/runner.md).

## 9. Worked Examples

### 9.1 Control command frame (`AssignRunnerId`)

`CommandRequest { reference_id: "#Ref<0.1.2.3>", assign_runner_id: { runner_id: "ce_001" } }`:

```text
Protobuf (25 bytes):
  0A 0D 23 52 65 66 3C 30 2E 31 2E 32 2E 33 3E     field 1 reference_id, len 13
  12 08 0A 06 63 65 5F 30 30 31                     field 2 assign_runner_id, len 8
                                                    └ field 1 runner_id, len 6

Payload ID (32 bytes):
  63 6F 6E 74 72 6F 6C 5F 63 6F 6D 6D 61 6E 64 20   control_command.
  20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20   ................

Payload Size (4): 4 + 25 + 2 + 0 = 31          00 00 00 1F
Message Size (4): 25                           00 00 00 19
Message (25 bytes): the protobuf bytes above
Metadata Size (2): 0                           00 00

Total frame: 32 + 4 + 31 = 67 bytes
```

The runner answers `CommandResponse { reference_id: "#Ref<0.1.2.3>",
status: "success" }` under the same payload id. The handshake frame (§5) is
built the same way with payload id `connect_control_channel` and the JSON
bytes as the message.

### 9.2 Data frame (stream input)

Text input `hello` for instance `proc_42` with metadata `{input_type: "2"}`:

```text
Metadata entry: key_len=10, val_len=1, "input_type", "2"
  0A 01 69 6E 70 75 74 5F 74 79 70 65 32             (13 bytes)

Payload ID (32 bytes):
  70 72 6F 63 5F 34 32 20 20 20 20 20 20 20 20 20   proc_42.........
  20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20   ................

Payload Size (4): 4 + 5 + 2 + 13 = 24          00 00 00 18
Message Size (4): 5                            00 00 00 05
Message (5 bytes): 68 65 6C 6C 6F              hello
Metadata Size (2): 13                          00 0D
Metadata (13 bytes): 0A 01 69 6E 70 75 74 5F 74 79 70 65 32

Total frame: 32 + 4 + 24 = 60 bytes
```

## 10. Error Handling

**Command errors.** A failing handler replies `status: "error"` with
human-readable strings in `errors` (e.g.
`"Factory with id unknown_factory does not exist"`) and normally an empty
`raw_data`. Where §6 or §11 names a canonical token
(`apply_state_not_supported`, `entity_key_without_procedure`, …) the token
leads the string so the controller maps it without parsing prose.

**Connection close detection.** On the controller a closed control stream
arrives as `{:flz_closed, ...}` from the NIF driver and the `ControlChannel`
runs the disconnect handling (preservation for `FLEZHA_RUNNER_KEEPALIVE`,
migration or re-placement); the §5 watchdog turns sustained silence into the
same path. On the runner the control reader retries `Interrupted`; a timeout
(`WouldBlock`/`TimedOut`) is fatal in both deadline states, and `BrokenPipe`,
`UnexpectedEof`, `ConnectionReset` and `InvalidData` (a frame-limit violation)
are stream-fatal — every fatal error drives the §5 reconnect unless the status
is `Stopped`.

**Malformed frames.** A reader MUST NOT discard partial data (§3 "Frame
buffering"). A frame whose declared length exceeds the limit poisons the stream
— no resync; the stream is closed and re-established. Any other decode failure
(a bad protobuf body, an undecodable persistence body) is logged and the frame
dropped; the stream stays open because the outer framing was intact.

## 11. Per-Entity State Persistence

This section is the wire contract for persistence. The per-SDK trait surface
(`Procedure::init_state` / `apply_change`, codecs, schema versions) is in
[`state_persistence.md`](state_persistence.md); the controller's on-disk
layout, WAL writer and `flz state` CLI are in
[`../internals/state_persistence.md`](../internals/state_persistence.md).

### Data-stream payload-id `entity_state_change`

Runner → controller. One Change record for the per-Entity WAL. Frame layout
(decoded by `Flezha.State.Wire.decode_frame/1`):

```text
[entity_id_len: u16 BE]
[entity_id_bytes]
[seq:        u64 BE]
[header:     16 B  ]      <- see "The 16-byte record header"
[body_len:   u32 BE]
[body]                    <- postcard-encoded Change
[crc32:      u32 BE]      <- crc32 over body
```

`entity_id_bytes` is structured so the controller routes the record to the
right per-Entity WAL without a side table:

```text
[operative_class_len: u8   ][operative_class]
[procedure_id_len:    u8   ][procedure_id]
[entity_key_len:     u16 BE][entity_key]
```

#### The 16-byte record header

| Offset | Field | Value |
|--------|-------|-------|
| 0 | `sdk_lang: u8` | `1` Rust, `2` Java |
| 1..5 | `sdk_version: u32 BE` | `(major << 16) \| (minor << 8) \| patch` |
| 5..9 | `schema_version: u32 BE` | the Procedure's declared schema version |
| 9 | `format_id: u8` | `1` postcard change, `2` effect record, `3` timer-ops record, `4` Kryo change (Java) |
| 10..12 | reserved | zero |
| 12..16 | `crc32: u32 BE` | over the body |

The controller stores, truncates and re-ships every `format_id` identically
and decodes only `format_id = 2`. A **timer-ops record** (`format_id = 3`)
carries a postcard `Vec<TimerOp>` mutating the Entity's pending-timer set; the
controller never decodes it — the pending set is SDK-owned state
([`entity_timers.md`](entity_timers.md)). On restore the kernel validates the
header for both hosts and refuses a mismatch with a typed error:
`RestoreError::LangMismatch` (a Java runner reading a Rust header or vice
versa), `SchemaMismatch`, `CrcMismatch`, `HeaderInvalid`.

### Data-stream payload-id `entity_effect_record`

Runner → controller, effectively-once tier only. Same frame as
`entity_state_change` with `format_id = 2`; `body` is the postcard
`EffectRecordBody` — one invocation's state change(s), plugin outputs and
consumed input-log offset, persisted by the controller as ONE WAL record under
ONE fsync so state, outputs and progress can never tear apart. Decoded by
`Flezha.State.Wire.decode_effect_body/1`:

```text
EffectRecordBody {
    changes:         Vec<Vec<u8>>,      -- postcard (or Kryo) Change bytes, folded in order
    outputs:         Vec<OutputRecord>, -- the invocation's plugin emits
    consumed_offset: Option<u64>,       -- flz.offset of the input; None for a timer firing
    dlq:             Option<DlqEntryBody>,
    event_time:      u64,               -- the invocation's declared clock value
    timers:          Vec<TimerOp>,      -- this invocation's pending-timer mutations
}
TimerOp      { kind: u32 (0 event time, 1 processing time), scheduled_at: u64 ms, register: bool }
OutputRecord { channel: String, key: String, metadata: Vec<(String, String)> key-sorted, payload: Vec<u8> }
DlqEntryBody { channel: String, error: String, attempts: u32, payload: Option<Vec<u8>> }
```

Postcard encodes `Vec`/`String` as a LEB128 varint length + contents, structs
as concatenated fields and `Option` as `0x00 | 0x01 ++ T`.

- `event_time` is what the Procedure's declared clock evaluated to (extracted
  event time under `EventTimeFrom`, wall-clock ms under `WallClock`, the change
  seq under `LogicalClock`); late-input correction and `{:timestamp, ts}`
  time travel resolve against it.
- `timers` makes a timer **firing** exactly-once: the Changes `on_timer`
  applied, its emits and the fired timer's removal are one record, which
  carries no `consumed_offset`.
- A tier-on dead-letter park rides the SAME record as its consumed-offset
  advance, so a crash can never advance the offset while losing the park; the
  controller materializes the DLQ entry after the fsync (idempotent on
  `entity_key@consumed_offset`) and sends no `ParkPayloadAck` for it.
- On the tier plugin emits never ride the primary stream: the controller
  dispatches them to the `:on_output` chain only after the record's fsync,
  stamped with `flz.entity`, `flz.seq = <record WAL seq>.<emit ordinal>` and
  `flz.epoch`. Effect records are never coalesced at drain time.

### Data-stream payload-id `entity_state_snapshot`

Runner → controller. Same frame as `entity_state_change`; `body` is the full
serialized `State` (postcard / Kryo) captured at `seq`. The controller writes
it to `<entity_key>.snap.<seq>` (`.tmp → fsync → rename`), truncates the WAL
up to `seq` and clears the in-flight compaction.

### Data-stream payload-ids `compensation_intent` / `compensation_done`

Runner → controller, workflow saga change-log, same frame as
`entity_state_change`. `compensation_intent` records a procedure's forward
completion (so a crash mid-compensation knows what to undo),
`compensation_done` a compensation hook's completion (so a resume skips it);
the controller hands them back as the `has_compensation_*` flags of
`RestoreInitState`.

### Data-stream payload-id `delete_entity_state`

Runner → controller, dynamic-mode hard delete. The message is the bare
structured `entity_id_bytes` — no seq, header or crc. Sent fire-and-forget
when the runner evicts a persistence-enabled dynamic Entity; the controller
drops the Entity's WAL, snapshot and outbox files so a later recreate
cold-starts, ignores the frame for a non-dynamic class, and writes no
response frame.

### Data-stream payload-id `park_payload`

Runner → controller, dead-letter park for sessions **off** the
effectively-once tier. When an invocation exhausts its input channel's retry
budget (`max_attempts` default 3, exponential backoff ±50 % jitter capped at
5 s) or returns a non-retryable error, the runner ships this frame and holds
the Entity until the controller acks the park as durable (`ParkPayloadAck`),
re-sending every 2 s — park-then-advance, never a drop. Same outer frame as
`entity_state_change` with a zero header; `body` is the postcard `ParkBody`
(`Flezha.State.Wire.decode_park_body/1`):

```text
ParkBody {
    park_id:       String,        -- SDK-minted ack-correlation id
    origin_offset: Option<u64>,   -- input-log offset when the tier provides one
    entry:         DlqEntryBody,  -- the same {channel, error, attempts, payload} record
}
```

The controller appends the entry to the durable DLQ store
(`app_data/dlq/<operative_class>/<procedure_id>/`), fsyncs, then acks.
Appends are idempotent on origin identity (`channel:sha256(payload)`), so an
ack-lost re-send never duplicates an entry.

### Data-stream payload-id `correction_request`

Runner → controller, late-input correction. An Entity saw an input whose
extracted event time is behind its event-time high-water but inside the
declared `correction_window`; only the runner can know that, so detection is
the runner's half and orchestration the controller's. Same outer frame as
`park_payload` (zero header, postcard body); the Entity identity rides the
envelope:

```text
CorrectionRequestBody { target_event_time: u64 }
```

Inputs are journaled before they are forwarded, so the late input is already
in the input log at its arrival offset and the controller re-reads the range.
Fire-and-forget: concurrent requests per Entity coalesce into one replay, a
duplicate costs nothing, and a lost one is re-raised when the input is
re-delivered. A transport-less run processes the input as an arrival.

The controller marks every input of the re-forwarded slice with
`flz.replay = "1"` and closes the slice with a **payload-less broadcast
frame** carrying `flz.replay_end = "1"`. The Entity holds the slice, orders it
by event time and only then dispatches — an arrival-ordered replay would
recompute the same wrong answer. The end marker is a broadcast because an
input routes to exactly one Entity by key; a flag on the last input would
strand every other Entity holding slice members. A `RestoreInitState` clears
any half-filled replay buffer, and every replay is preceded by a restore.

### Control command `RestoreInitState` (controller → runner)

```protobuf
message RestoreInitState {
    bytes  entity_id = 1;                // structured entity_id_bytes
    bytes  snapshot_bytes = 2;           // full State, empty when none
    bytes  change_log_bytes = 3;         // WAL tail, on-disk framing below
    bool   has_compensation_intent = 4;  // saga crash-resume markers
    bool   has_compensation_done = 5;
    uint64 terminal_seq = 6;             // highest seq the shipped state covers
    bytes  snapshot_header = 7;          // the snapshot's stored 16-byte header
}
message RestoreInitStateResponse {
    bytes  entity_id = 1;
    uint64 terminal_seq = 2;
    bool   migrated = 3;                 // a record was read at an older schema version
}
```

Each record inside `change_log_bytes` uses the controller's on-disk WAL
framing, back to back:

```text
[seq:    u64 BE]
[len:    u32 BE]    <- 16 + change_bytes.len()
[header: 16 B  ]
[change_bytes]
[crc32:  u32 BE]    <- crc32 over header + change_bytes
```

`snapshot_header` is the snapshot file's stored header, shipped rather than
stripped: it carries the `schema_version` the snapshot was written at, and
without it every compacted Entity is unverifiable. It is empty exactly when
`snapshot_bytes` is empty.

The runner resolves every record's header — the snapshot included — against
the running Procedure's `schema_version` and `min_readable_schema_version`
before decoding anything (see "Schema evolution" below), decodes
`snapshot_bytes` (when non-empty) into `State`, folds the
change-log records through the Procedure's `apply_change`, and re-arms the
Entity's change seq to `max(change-log terminal, terminal_seq) + 1` — a
snapshot-only restore (empty WAL tail after a compaction) has no record to
derive the seq from, and a seq below the snapshot's would have its next
Changes refused as duplicates. `RestoreInitStateResponse` returns as
`raw_data`; the controller blocks the Entity's input until it lands (the
restore rides the control channel, the replay the data channel) and expects
the next `entity_state_change` at `seq = terminal_seq + 1`. The proc-journal is
**not** here — it is instance-scoped and rides `AddOperative.proc_journal`; a
durable workflow class with no per-Entity state receives no `RestoreInitState`
at all.

#### Schema evolution

A record's `schema_version` (header bytes 5..9) is the version of the
Procedure that wrote it. Per record, against the running Procedure's
`SCHEMA_VERSION` (`V`) and `MIN_READABLE_SCHEMA_VERSION` (`M`, defaulting to
`V`):

| stamped | outcome |
|---|---|
| `== V` | decode with the current codec |
| `>= M` and `< V` | hand the bytes to the Procedure's migration for that version |
| `< M` | refuse — the record predates what this build declares it reads |
| `> V` | refuse — older code cannot read newer state; a downgrade, not an evolution |

The decision is per record because one WAL legitimately holds both: records
written before a deploy sit under records written after it. It is made in
`flz_core` for every SDK, so a host-run Procedure receives only bodies it can
decode at its own version or migrate from a named one, and a refusal never
crosses to the host.

When any record was migrated, the runner sets `migrated` on the response and
the controller answers with the `CompactEntity` it already drives on WAL size
and age: the next snapshot is written by the running version and the migration
is paid once rather than on every restore.

`schema_version` and `min_readable_schema_version` are declared **per
procedure** in the class declaration (`procedures[].schema_version`), never
class-wide — a class-wide value re-stamps a Procedure's records with a version
it never declared.

### Control command `CompactEntity` (controller → runner)

```protobuf
message CompactEntity          { bytes entity_id = 1; }
message CompactEntityResponse  { bytes entity_id = 1; uint64 snapshot_seq = 2; uint64 snapshot_bytes_size = 3; }
```

Issued when the Entity's un-snapshotted WAL exceeds its compaction trigger
(`FLEZHA_COMPACT_AFTER_BYTES`, default 8 MiB, overridable per class through
`AddOperativeResponse.compact_after_bytes` and per operator), when its oldest
un-snapshotted record is older than `FLEZHA_COMPACT_AFTER_AGE_MS` (default
300000; `0` disables), or on `flz state rotate`. The runner drains the
Entity's pending changes, clones `State` under the Entity's sticky CAS,
serializes it, ships it as `entity_state_snapshot`, then returns
`CompactEntityResponse` as `raw_data`. `snapshot_seq` MUST be the seq the
drained state stands at — the writer treats it as a truncation point.

### Control command `EntityMailboxStatus` (controller → runner)

```protobuf
message EntityMailboxStatus { bytes entity_id = 1; string status = 2; }   // "full" | "open"
```

One-way back-pressure signal for sessions that are **not** credit-speaking
(an SDK that never acked `CreditWindowInit`, or a controller with
`FLEZHA_CREDIT_FLOW_CONTROL=false`): the `WalWriter` emits `"full"` when its
pending bytes reach `:wal_high_watermark_bytes` (default 16 MiB) and `"open"`
when they fall to `:wal_low_watermark_bytes`. A `flz_core` runner tolerates
and drops it — its send path is already bounded by the mailbox policies.

### Control command `CreditWindowInit` (controller → runner)

```protobuf
message CreditWindowInit {
    string stream_id = 1;            // opaque credit-session id, fresh per attach
    uint64 window_bytes = 2;         // un-fsynced byte budget for the stream
    uint64 low_watermark_bytes = 3;  // the controller's grant-batching threshold
}
message CreditWindowAck  { string stream_id = 1; }     // runner → controller, as raw_data
message CreditGrant      { string stream_id = 1; uint64 bytes = 2; }
```

Offered on the **first** `entity_state_change` frame of a stream attach, so
the window resolves per Operative from that frame's class
(`:credit_window_bytes`, default `:wal_high_watermark_bytes`;
`:credit_low_watermark_bytes`, default half the window). The runner arms the
window and returns `CreditWindowAck` echoing `stream_id` as `raw_data`; on
that ack the controller marks the session credit-speaking, stops sending
`EntityMailboxStatus` and enforces the per-stream byte ledger. An SDK that
does not recognise the variant never acks; the offer times out after 10 s and
the session keeps the legacy behaviour. `FLEZHA_CREDIT_FLOW_CONTROL=false`
skips the offer entirely.

From the ack on, every `entity_state_change` frame debits its full wire size
before the write; an exhausted window parks the sending thread — park, never
drop (`credit_parked` in the heartbeat). `CreditGrant` is one-way and minted
only when appended bytes become durable: on WAL fsync the controller sends one
batched grant when the un-fsynced buffer falls below `low_watermark_bytes` —
never per record, and a stalled fsync mints nothing. Credit never crosses
sessions: a reconnect re-runs `CreditWindowInit` with a fresh `stream_id`, and
a grant for another `stream_id` is discarded. A frame arriving on an exhausted
ledger is a protocol violation: the controller sheds it
(`credit_overrun_total`) and the seq-gap resync repairs the gap.

### Control command `ParkPayloadAck` (controller → runner)

```protobuf
message ParkPayloadAck { string park_id = 1; }   // echoes ParkBody.park_id
```

One-way durability ack for a `park_payload` frame, sent only after the DLQ
entry is fsynced. On receipt the runner wakes the dispatch thread blocked in
the park, drops the payload and advances the Entity. A runner that never
receives it keeps the Entity held and re-sends the park — delay, never loss.

### Control command `AddOperative.proc_journal` (controller → runner)

```protobuf
message ProcHandoffItem   { uint32 channel_id = 1; bytes payload = 2; }   // channel codec bytes of one value
message ProcCommittedProc { uint32 proc_idx = 1; uint32 iter = 2; repeated bytes entities_ran = 3; }
message ProcEdgeHandoff   { uint32 proc_idx = 1; repeated ProcHandoffItem handoff = 2; }

message ProcJournalState {
    repeated uint32 committed_proc_idxs = 1;       // committed inside the in-flight iteration — skipped
    uint32 iter = 2;                               // the iteration to resume at
    repeated ProcHandoffItem handoff = 3;          // previous iteration's terminal batch
    repeated ProcCommittedProc committed_procs = 4;// every forward commit, for compensation planning
    uint64 consumed_offset = 5;                    // inbox offset the last committed iteration consumed
    bool   parked = 6;                             // suspension record
    uint64 wake_deadline_ms = 7;
    repeated ProcCommittedProc rewind_undo = 8;    // unwound by a rewind, not yet compensated
    repeated ProcEdgeHandoff proc_handoffs = 9;    // in-flight iteration, per committed proc
}
```

Present only when the instance has a non-empty proc-journal. A resumed driver
skips `committed_proc_idxs`, re-seeds each uncommitted child branch from its
own parent's edge in `proc_handoffs` (filtered to the channel the child
consumes — the latest commit's handoff alone would strand a sibling), re-seeds
iteration N+1 of a handoff-chained loop from `handoff`, and rebuilds the
compensation planner's input from `committed_procs`. `rewind_undo` lists the
procedures a rewind unwound that nothing has compensated yet: the driver runs
them through the ordinary compensation planner (reverse iteration, reverse
chain position) before resuming forward at `iter`, each undo journaling a
`compensated` entry so the list shrinks across a crash. The rewind itself
never crosses the wire — the controller writes the `rewound` entry and rebinds
the instance.

### Control command `ProcCommit` (runner → controller)

```protobuf
message ProcOutboxItem {
    bytes  entity_id = 1;     // wire identity of the producing Entity
    uint32 seq_in_proc = 2;   // that Entity's emit order within the procedure
    string channel = 3;       // producing plugin channel's name
    bytes  payload = 4;       // serialized plugin-bound output
}

message ProcCommit {
    string operative_id = 1;
    uint32 proc_idx = 2;
    uint32 iter = 3;                        // 0 for the single-pass chain
    repeated bytes entities_ran = 4;        // wire Entity identities
    repeated ProcHandoffItem handoff = 5;
    repeated ProcOutboxItem outbox = 6;     // staged plugin output
    bool compensating = 7;                  // undo record, not forward progress
}

message ProcCommitResponse { string operative_id = 1; uint32 proc_idx = 2; string error = 3; }
```

Emitted at each procedure barrier of a **durable structured** workflow
instance: procedure `proc_idx` completed forward on `entities_ran` and released
the inter-proc handoff batch, serialized through the producing channel's byte
codec (`.codec()` / `.codec_with(...)`, required at registration for durable
structured classes). The driver **blocks** until `ProcCommitResponse` — the
next procedure must not race ahead of the committed record. Opted-out classes
(`PERSISTENCE_ENABLED = false`) and the `run`-sugar form skip the barrier and
keep the direct at-least-once wire path.

`outbox` is the procedure's **staged** plugin-bound output: inside a structured
procedure `ctx.emit` never reaches the wire; each Entity buffers its serialized
output and it travels here, so a failed or aborted procedure leaks nothing. The
controller journals handoff AND outbox in ONE `proc_committed` record and
flushes the outbox only after that fsync. The frame bound applies to the
combined payload; an oversized handoff + outbox is refused with a non-empty
`error`.

`compensating = true` marks the undo twin: a `compensate` hook's staged output
for the already-committed procedure `proc_idx`, journaled as a `compensated`
entry and flushed with comp-marked keys. It is shipped BEFORE that Entity's
`compensation_done` marker, so a crash between the two re-runs the
(idempotent) hook rather than losing its undo output.

**Reply and durability.** `ProcCommitResponse` rides a controller-initiated
`CommandRequest` under the request's `reference_id`, emitted only AFTER the
`proc_committed` entry is fsynced in the instance's `proc.journal`
(journal-then-ack). The fsync is batched by the `FsyncBatcher` tick and the
acks of every barrier in the tick — across every instance on the runner — are
released after that one sync. The control-channel reader never journals: it
hands the frame to the per-runner commit worker (`ControlChannel.Commits`), so
a commit storm never queues a ping, stop, park or compaction behind disk I/O.
A non-empty `error` (journal write failure, oversized payload) terminates the
run abnormally through the terminal-error path, including saga compensation.

**Idempotent re-send.** An ack that does not arrive within the driver's wait
(`FLEZHA_WORKFLOW_ACK_TIMEOUT_MS`, default 30 s) is re-sent: the same frame
goes out under the **same `reference_id`**, after the class's retry backoff, up
to `barrier_retry_attempts()` / `barrierRetryAttempts()` sends (default 3). The
controller journals a barrier once by its identity `{proc_idx, iter, epoch}` —
the journal writer refuses an append whose 16-byte record header
(`[epoch u32][type u8][proc_idx u16][iter u32]`, zero-padded) is already in the
file (`dedup: true`) — and answers a re-send from the existing entry. Only the
exhausted ladder is mid-run channel loss: the run terminates and crash-resume
re-drives from the last committed proc (the un-acked proc re-runs —
at-least-once).

**Outbox flush.** The response is the durable point past which the staged
output may be seen; the controller flushes it right after the ack, in journal
order, through the same `:on_output` fan-out live workflow output uses,
stamping each frame with the identity below, and appends `outbox_flushed`. A
delivery that cannot happen yet is retried, never dropped; on restore every
`proc_committed` / `compensated` entry without a matching `outbox_flushed` is
re-driven with its original keys before the resumed run advances — a crash
between commit and flush duplicates at worst (at-least-once with stable keys)
and loses nothing.

**Workflow output identity.** Every flushed outbox frame carries the same
`flz.*` shape the effectively-once tier stamps, minted from the journal:

| Key | Value |
|-----|-------|
| `flz.entity` | `<instance_id>/<procedure_name>/<entity_key>` of the producing Entity |
| `flz.seq` | `<proc_idx>.<seq_in_proc>`; an undo output carries a trailing `.1` (`<proc_idx>.<seq_in_proc>.1`) so it never collides with the forward output it reverses |
| `flz.epoch` | the instance's journal epoch: `0` at first start, carried verbatim across crash re-drives, bumped only by a rewind |
| `channel` | the producing plugin channel's name |

The tuple is a pure function of the journal, so a re-drive replays
byte-identical stamps.

### Control command `IterCommit` (runner → controller)

```protobuf
message IterCommit {
    string operative_id = 1;
    uint32 iter = 2;
    uint64 consumed_offset = 3;             // durable-inbox offset this iteration consumed
    repeated ProcHandoffItem handoff = 4;   // the iteration's terminal batch
}
message IterCommitResponse { string operative_id = 1; uint32 iter = 2; string error = 3; }
```

Emitted at each **iteration** boundary of a looping durable structured
instance, after the last procedure's `ProcCommit` is acked. A class opts into
looping with `loop_mode()`: `PerInput` runs one iteration per inbox item,
`Chained` seeds each iteration from the previous terminal handoff; the default
`Once` never emits this frame. Every `ProcCommit` inside iteration `N` carries
`iter = N`, so redo-vs-skip works at both granularities from one scan.

The controller appends ONE `iter_committed(iter, consumed_offset)` entry
carrying the terminal handoff, fsyncs, and acks under the same batching,
terminal-error and bounded re-send contract as `ProcCommitResponse` (a re-sent
`IterCommit` is journaled once by `{iter, epoch}`). Everything after the ack is
cleanup: a boundary `CompactEntity` per Entity of the instance, the flush of
what the iteration staged, the release of the inbox prefix at or below
`consumed_offset`, and a cut of the proc-journal below the oldest entry its
fold still needs (the latest `iter_committed`, entries whose outbox is still
owed, the latest `rewound` marker with its pending undos), deferred until the
settled prefix exceeds `:record_log_cut_after_bytes` (64 KiB) and skipped for
a class whose procedures declare `compensate`. The driver enters iteration
`iter + 1` only on success, so a crash re-runs at most the in-flight
iteration.

### Control command `IterPark` (runner → controller)

```protobuf
message IterPark {
    string operative_id = 1;
    uint32 iter = 2;               // the last COMMITTED iteration
    uint64 wake_deadline_ms = 3;   // 0 = wake on input only
}
```

A `PerInput` loop whose inbox has been empty past `FLEZHA_WORKFLOW_PARK_IDLE_MS`
(default 5 s) at a **committed** iteration boundary tears its own run down and
emits this instead of `WorkflowCompleted` — suspension is not a terminal
outcome, and it is only ever offered at a committed boundary. A rehydrated loop
waits up to 60 s for the inbox tail the controller owes it
(`AddOperative.inbox_tail_count`) before it may park again.

The controller journals `parked(iter[, wake_deadline_ms])` — durable BEFORE
the eviction — then evicts the instance from its runner (`RemOperative`). The
registry entry and the controller-side Overwatch stay: it is the **runner**
that holds zero resources, and `flz operative info` reports the instance
`suspended`. Rehydration has one path, whichever wakes it — a new inbox
append, or the journaled deadline, which the controller delivers as a synthetic
inbox entry with an empty payload and `flz.wake = timer` — and both re-bind
the instance through the normal workflow spawn path on any live runner,
restoring journal + boundary snapshots and delivering the undelivered inbox
tail. Two racing wakes collapse into exactly one rehydration.

### Data frame metadata: durable-inbox offset (`flz.inbox_offset`)

Post-start input for a **durable workflow** instance is appended to the
instance's `inbox.wal` **before** it is forwarded, and the forwarded frame
carries the assigned 1-based offset as `flz.inbox_offset` (absent ⇒ not
journaled: an opted-out class). The runner threads it back through
`IterCommit.consumed_offset`, which lets the controller release the consumed
prefix and stops a rehydrate re-delivering it. Dedup at append is keyed on the
sender's `flz.msg_id` metadata when present, else a payload hash, over the
window of the file above its last release: a duplicate inside the undelivered
window is absorbed, so an at-least-once sender cannot produce a duplicate
turn. The dedup identity is the first 16 bytes of `sha256(dedup_id)` in the
record header, so an append reads no file and folds no journal.

### Plugin metadata: output identity stamps (`flz.*`)

Channels declared `.keyed()` (valid on `output` and `sink` / `sink_with`
wires only — a keyed channel without a plugin wire is a build error) stamp
every plugin-bound emit with three reserved keys, appended after user
metadata:

| Key | Value | Stability |
|-----|-------|-----------|
| `flz.entity` | `<operative_instance_id>/<procedure_name>/<entity_key>` | replay-stable |
| `flz.seq` | `<change_seq>.<emit_ordinal>` — the Entity's durable change seq after the flush-before-emit drain, plus the emit's ordinal within the invocation | replay-stable while state advances |
| `flz.key` | the emitting Entity's own key, or the caller's natural key from `ProcedureCtx::emit_keyed(key, value)` | replay-stable by the determinism contract |

On a keyed channel a plain `emit` / `emit_with_meta` carries the emitting
Entity's own key — equal to the key its input routed by — and `emit_keyed` is
for the output whose natural key differs. A single-Entity Procedure has no such
key and its plain emit fails with `KeyRequiredOnKeyedChannel`; on a non-keyed
channel `emit_keyed` fails with `KeyNotAllowedOnNonKeyedChannel` — the channel
declaration is the single source of truth for whether output identity exists.
The controller passes the stamps through the `:on_output` plugin chain
unchanged.

**`flz.seq` exactness boundary.** `flz.seq` suffices for changelog-style
consumers that upsert on `flz.key`. It is NOT a sufficient handle for exact
append-only dedup: consecutive invocations that apply no state change can
produce colliding `<change_seq>.<emit_ordinal>` pairs. Exact dedup needs a
durable outbox — the effectively-once tier and the workflow outbox both stamp
`flz.epoch`, and `(flz.entity, flz.seq, flz.epoch)` is the identity a
conforming sink deduplicates on (the Kafka producer uses `flz.key` as the
record key and ships `<entity>/<outbox_seq>/<epoch>` in its
`flezha.dedup_key` header; the Postgres sink guards on `(epoch, seq)`). See
[`../resilience/data_durability.md`](../resilience/data_durability.md).

## See also

- [`poll_core.md`](poll_core.md) — the Activation/ActivationResult boundary inside a `flz_core` runner
- [`state_persistence.md`](state_persistence.md) — the per-SDK persistence trait surface (Rust and Java)
- [`../internals/state_persistence.md`](../internals/state_persistence.md) — the controller's on-disk layout, WAL writer and `flz state` CLI
- [`../internals/transport_liveness.md`](../internals/transport_liveness.md) — keep-alives, watchdogs and the stall classifier
- [`../internals/nif_transport.md`](../internals/nif_transport.md) — the controller-side UDS/QUIC drivers
- [`../architecture/workflow_profile.md`](../architecture/workflow_profile.md) — the workflow barrier, journal and outbox model
