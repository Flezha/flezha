# Flezha SDK Protocol Specification

This is the wire-protocol reference for SDK implementers: transports, binary
framing, the protobuf command set, the connection lifecycle, the data-plane
frames and the per-Cell persistence frames. Relay authors do not need it
— their surface is [`authoring_contract.md`](authoring_contract.md).

The runner side of the protocol has one implementation, the shared Rust
kernel `flz_core`, linked by the Rust SDK as a crate and by every other SDK
through the `flz_core_ffi` C ABI. The host-facing Activation contract inside a `flz_core` runner is a
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
11. [Per-Cell State Persistence](#11-per-cell-state-persistence)

## 1. Overview

The **Controller** (Elixir/OTP) orchestrates work across one or more
**Runners** (SDK processes). A runner connects to the controller over one
transport, presents the relay factories it can host, and executes Relay
logic on the controller's behalf over two layers:

| Layer | Purpose | Socket | Encoding |
|-------|---------|--------|----------|
| Control plane | Handshake, command dispatch, lifecycle, durability barriers | control channel (one per runner) | protobuf inside the binary frame |
| Data plane | Relay input/output, heartbeats, per-Cell state records | N primary (controller → runner) + N secondary (runner → controller) streams | raw bytes inside the binary frame |

Of the command surface, only four interactions reach host code in a `flz_core`
runner (as `RestoreFold`, `SnapshotRequest`, `LiveStateRead` and `ApplyState`
activations; a migration reads the state as a `LiveStateRead`); the kernel
answers everything else. A relay
leaving the runner and an evicted Cell reach it too, as a `Leave` activation
that nothing waits on, so the host drops the state it holds for them.

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

`{base}` is the socket directory both sides resolve from one ladder —
`FLEZHA_SOCKET_FILES_PATH` when set, then `$XDG_RUNTIME_DIR/flezha`,
`/run/flezha`, `storage/app/sockets` — always reported absolutely, and never
re-derived per SDK (see [Files and sockets](../../public/configuration/files_and_sockets.md#the-socket-directory-ladder)). It is
at most 60 bytes, so the longest data socket path fits the 107 bytes a Unix
domain socket address holds, and a controller refuses to boot on a longer one.
The
per-runner listeners are created after the runner has its session token and
are served by a `StreamAcceptor` per stream pair.

### QUIC

The QUIC transport is [experimental: quic](../../public/publishing/maturity.md#quic).

With `FLEZHA_TRANSPORT=quic` the runner opens one QUIC connection (UDP) and
multiplexes every channel over bidirectional streams inside it.

| Setting | Env var | Default |
|---------|---------|---------|
| Controller listen address | `FLEZHA_QUIC_LISTEN_ADDRESS` | `0.0.0.0:4433` |
| Runner connect addresses | `FLEZHA_QUIC_ADDRESSES` — a comma list of names or literals, re-resolved every attempt | `127.0.0.1:4433` |
| CA certificate | `FLEZHA_CA_CERT_PATH` | `storage/app/certs/ca.pem` |
| Server certificate / key | `FLEZHA_SERVER_CERT_PATH` / `FLEZHA_SERVER_KEY_PATH` | `storage/app/certs/server.pem` / `server.key` |
| Generated client identity (controller side) | `FLEZHA_CLIENT_CERT_PATH` / `FLEZHA_CLIENT_KEY_PATH` | `storage/app/certs/client.pem` / `client.key` |
| Client trust anchor (controller side) | `FLEZHA_QUIC_CLIENT_CA` | `storage/app/certs/ca.pem` |
| Client certificate / key presented by the runner | `FLEZHA_QUIC_CLIENT_CERT` / `FLEZHA_QUIC_CLIENT_KEY` | `storage/app/certs/client.pem` / `client.key` |
| Peer authentication | `FLEZHA_TRANSPORT_AUTH` | required; `off` is the explicit opt-out, warned on every boot |

Certificates are generated on first start if absent (CA, server pair, a client
identity with the `clientAuth` EKU). The server verifies the client certificate
against `FLEZHA_QUIC_CLIENT_CA` and refuses the handshake otherwise — no frame
of an unauthenticated peer is ever parsed. That verification proves the peer was
issued a certificate; **which namespace it may serve is a separate question**,
and the certificate answers it only when it carries a SPIFFE URI SAN with an
`/ns/<name>` cell. The pair generated on first start does not, so a default
QUIC deployment proves its namespace with the credential of §5 step 2, exactly as
UDS does. The runner principal (`runner@<namespace>`, role `runner`) is minted
controller-side from the namespace the connection PROVED, never from the value
the frame claimed
([`../../public/topics/core/authorization.md`](../../public/topics/core/authorization.md)). QUIC sends a
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
`flz_transport::service::payloader`, and so is the one body decoder,
`parse_payload_body`, which the runner's reader and the controller's NIF
`FrameBuffer` both call: the two sides cannot read one body two ways.

### Payload ID

Exactly 32 bytes. A shorter logical id is right-padded with ASCII spaces; a
longer one is truncated to 32 bytes. Readers trim trailing spaces to recover
the logical id.

| Payload ID | Direction | Context |
|------------|-----------|---------|
| `connect_control_channel` | runner → controller | Handshake init (JSON message) |
| `control_command` | both | Protobuf `ControlFrame`: a `CommandRequest` or a `CommandResponse` |
| `control_ping` | runner → controller → runner | Keep-alive; a bare `Ping` the controller echoes verbatim (§5) |
| `data_heartbeat` | runner → controller | Data-channel liveness, empty message + counters in metadata |
| `{relay_id}` | both | Relay input (primary) and output (secondary) |
| `cell_state_change`, `cell_effect_record`, `cell_state_snapshot` | runner → controller | Per-Cell persistence records (§11) |
| `compensation_intent`, `compensation_done` | runner → controller | Steps saga markers (§11) |
| `delete_cell_state` | runner → controller | Dynamic-Cell hard delete (§11) |
| `park_payload` | runner → controller | Dead-letter park (§11) |
| `correction_request` | runner → controller | Late-input correction (§11) |
| `output_part` | runner → controller | A part: a preview of an invocation's output, for listeners only (§11) |
| `processed_prefix` | runner → controller | An instance's processed input prefix (§11) |

### Frame limits

- `FLEZHA_MAX_FRAME_BYTES`: Largest declared payload length either parser accepts; read once at boot.
- `FLEZHA_MAX_WRITE_BUFFER_BYTES`: Per-stream send-buffer cap in the UDS and QUIC drivers.

Their types and defaults are in [Runner environment variables](../../public/configuration/runner_environment.md#every-variable).

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

A header within the limit whose body does not decode is as fatal: an inner
`Message Size` overrunning the payload, or a metadata table cut inside a pair.
The runner's reader ends on the decoder's error; the controller's
`FrameBuffer` poisons, the driver closes the stream with reason
`malformed_frame` and counts it in `driver_stats` as `malformed_frames`. No
frame is skipped and no partial metadata table is kept.

### Metadata serialization

Metadata is a map of UTF-8 string keys to UTF-8 string values, serialized as a
flat sequence of entries:

```text
for each (key, value):
  [key_length: u8] [value_length: u8] [key: key_length bytes] [value: value_length bytes]
```

Keys and values are limited to 255 bytes by the 1-byte length prefix, and the
whole serialized table to 65535 by `Metadata Size`. The encoder refuses a frame
that exceeds any of the three, naming the offending key and the length; it never
writes a truncated prefix, because a truncated one produces a frame that parses
cleanly and yields pairs the sender never wrote. An empty map serializes to zero
bytes (`Metadata Size = 0`). Entry order is unspecified.

A channel name rides every frame its channel produces, as the `channel` value,
so an over-long one is refused at registration with `topology.channel_name_too_long` rather
than at emit.

#### Reserved metadata keys

| Key | Direction | Meaning |
|-----|-----------|---------|
| `input_type` | controller → runner | Stamped by the controller on every entry-channel input from the Relay's `:default_input_type` (`"2"` unless declared; `"1"` binary, `"2"` text). The SDKs do not read it: payload bytes are converted by the entry channel's declared payload type (`Vec<u8>`/`byte[]` passthrough, `String` UTF-8, anything else through the channel's entry parser). |
| `channel` | controller → runner | The Relay's entry channel that receives this input. Absent ⇒ the Relay's first declared input channel. A name that is not one of the Relay's entry channels — undeclared, or declared as a channel of another kind — is parked to the dead-letter store under that name, never dropped and never routed to another channel. |
| `channel` | runner → controller | Set on every plugin-bound emit. Plugin providers match on it via their `:channel` option to select which channel's outputs they consume. |
| `flz.cell` | runner → controller | Output-identity stamp on `.keyed()` channels: `<relay_instance_id>/<tasklet_name>/<cell_key>` (empty key for single-mode). |
| `flz.seq` | runner → controller | Output-identity stamp: `<change_seq>.<emit_ordinal>`; for a Cell that records effects (a flow relay on the effectively-once tier, or a durable steps relay Cell) the controller stamps `<record seq>.<emit_ordinal>` from the effect record. |
| `flz.key` | runner → controller | Output-identity stamp: the emitting Cell's own key, or the caller's natural key from `emit_keyed(key, value)`. |
| `flz.epoch` | controller → plugins | The Cell's WAL realign epoch, for a flow relay on the effectively-once tier and a durable steps relay Cell alike; a steps relay rewind does not move it. `(flz.cell, flz.seq, flz.epoch)` is the exact dedup identity a conforming output deduplicates on. |
| `flz.ack` | controller → plugins | Delivery-confirmation token minted by a durable outbox flush; the owning writer advances its cursor when the output confirms or parks it. Absent on live non-durable output. |
| `flz.offset` | controller → runner | The durable input-log offset of this forwarded input, for an instance with durable input: a flow relay on the effectively-once tier or a durable steps relay (§11). A flow relay's invocation carries it back as its effect record's `consumed_offset`, a steps relay's iteration as `IterCommit.consumed_offset`. |
| `flz.msg_id` | sender → controller | Sender-supplied message id: an append whose id the instance's input log still retains is absorbed (§11). |
| `flz.replay`, `flz.replay_end` | controller → runner | Replayed-slice markers for late-input correction (§11). |
| `flz.change_seq` | runner → controller | On the `CellState` reply frame: the seq a single selected Cell was serialized at (§6). |
| `flz.part`, `flz.part_attempt`, `flz.relay` | runner → controller | On an `output_part` frame: the part's number within its attempt, from `0`; the attempt, from `1`; and the instance it belongs to (§11). |

The `flz.` prefix is a protected namespace: a user-supplied metadata key
beginning with `flz.` is rejected at emit time with a typed error
(`TaskletError::ReservedMetadataKey` in Rust,
`TaskletException.ReservedMetadataKey` in Java) on every emit variant, so
runtime stamps can never be spoofed or clobbered. Stamps are appended after
user metadata; channels not declared `.keyed()` carry none.

Channel names on the wire are hand-written identity strings (a non-entry
channel's name derives from its producer node unless an `output("...")` name or a
`.named(...)` override supplies one; declaring both is refused), so renaming a
language class never changes a wire or on-disk name. A
plugin-bound frame is byte-identical whether or not the channel also fans out
to inter-tasklet consumers.

### Heartbeat frames

`data_heartbeat` (empty message) is written on **every** secondary stream every
`FLEZHA_DATA_HEARTBEAT_MS` (default 10 s, jittered into `[base/2 + 1, base/2 + base]` ms)
while connected, so an idle-but-healthy Relay keeps the controller's
per-channel liveness clock fresh. Its metadata is the runner's self-report, and
the key set is **one declaration**: every row of
`Flezha.Transport.LivenessCounters` that names a `heartbeat:`, answered by
`LivenessCounters.heartbeat_keys/0`. It is headed by `dispatches` (completed
dispatches) and `handouts` (activations issued, which a handler still running or
a retry in its backoff moves while `dispatches` stays put); the
`runner_pool_wedged` classifier reads both. It covers the
overflow outcomes, the handout and deferred-push reports, the credit park, the
rebind discard and the two timer counters; read the rows for the current list
rather than a copy of it here.

The controller mirrors each key into its `hb_<key>` liveness slot, bumps
`hb_count`, and treats any inbound frame as liveness; a bound channel silent
past `:io_stream_liveness_threshold_ms` (default 30 s) is flagged as a stall.
Because the mirror and the exporter are both derived from those rows, a key
added to the layout arrives with its slot and its surface already in place, and
a key with no row cannot be mirrored at all — the cross-SDK gate compares the
declared set against `flz_core`'s builder on every default-suite run.

### Per-Cell mailbox is bounded; on full, the channel's `OverflowPolicy` applies

The runner's per-Cell mailbox is a fixed-capacity queue
(`crossbeam::queue::ArrayQueue`) sized at Relay startup. A frame for a
full mailbox takes the channel's declared `OverflowPolicy`: `DropNothing`
(nothing is discarded — the producer waits for a slot, which on an
inter-tasklet channel means the producing Cell is withheld further work and
on an entry channel means the reader stalls; the default for every
channel kind in every SDK), `DropNewest` or `DropOldest` (a counter is
bumped). Under `DropNothing` a saturated entry channel parks the **wire-reader
thread** and sibling Relays on that connection wait with it; the reader
never runs tasklets itself, and `DropNothing` requires an acyclic tasklet
graph. Capacity declaration
and counters: [`rust_relay_architecture.md`](rust_relay_architecture.md)
"Backpressure and bounded mailboxes".

### Relay state JSON shape

`MigrationSerialization` / `MigrationDeserialization` / `CellState` and a
relay's `AddRelay.init_values`, for both kinds, all carry a JSON blob
whose shape is:

- Single-mode tasklets (no Cell keys): `{"<tasklet_name>": <state>}`
- Static-mode tasklets (`cell_keys()` non-empty):
  `{"<tasklet_name>": {"<cell_key>": <state>, ...}}`
- Dynamic-mode tasklets (keyed on the wiring with `.key_by` / `.keyBy`): the
  same key-keyed object over the Cells the runner holds live at
  serialization time.

Both SDKs sort object keys, so an equivalent state serializes byte-identically
on either runtime.

#### Applying the shape

A document need not cover the relay. Every top-level entry is applied to
the tasklet it names and every inner entry to the Cell its key names; a
tasklet the document omits keeps the state its class builds. Within one
entry there is no partial application: the value is handed to the tasklet's
state deserializer whole, so it must be a complete state unless the class
declares per-field defaults.

An entry that names nothing live is **ignored, not refused**. The runner writes
one line to its stderr (`ignoring unknown tasklet`, `ignoring unknown
cell_key`) and the command still succeeds; a value that fails to deserialize
is ignored the same way. Only the document as a whole can fail `AddRelay`:
JSON that does not parse, and JSON that parses to anything but an object.

A dynamic-mode tasklet cannot be seeded by `init_values`: its Cells are
minted on first sight of a key and none exists at the bind, so the entry
reaches nothing. Migration is unaffected, because a dynamic tasklet's
Cells are restored from their own records rather than through this path.

A steps relay takes this shape exactly as a flow relay does: its
`init_values` are applied as its tasklets' initial state before the
`AddRelay` acknowledgement, so before any message can reach it. Nothing
seeds an entry channel from the document; a steps relay's input is the messages
sent to it.

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
`runner_commands`). The authoritative definition, with every message and
field this document names, is
[`protobufs/runner_commands.proto`](../../../protobufs/runner_commands.proto); the generated Rust module is
`flz_core/src/generated/runner_commands.rs`, written by `protobufs/regen.sh`
(the `proto-fresh` gate fails when it is stale), and the controller's Elixir
decoder is `Flezha.Runner.CommandsProto` (Protox).

### Envelope

Every `control_command` payload, in either direction, is one `ControlFrame`
whose `frame` oneof says its direction: `request` (field 1, a `CommandRequest`:
a command its sender initiated, or a typed reply to one) or `response` (field
2, a `CommandResponse` to a request the reader sent). Each side decodes an
inbound payload once and dispatches on that oneof; neither keeps a list of the
commands the other side initiates.

A `CommandRequest` carries an opaque `reference_id` and one variant of its
`command` oneof (below). A `CommandResponse` echoes the request's
`reference_id` and carries a `status` (`"success"` or `"error"`), the
`errors` that say why when the status is `"error"` (§10), and `raw_data`, the
command-specific reply payload, often empty. An `Error` carries a stable
`code` spelled `family.snake` ([`docs/public/errors.md`](../../public/errors.md)), its `class`
(`internal`, `invalid`, `unreachable`, `denied`, `not_found`, `conflict` or
`unavailable`), one plain `message`, `details_json` (a JSON object of the
facts the message names) and a `hint` naming the next command, empty when
there is none.

### The `command` oneof

Tags 2 to 43 are the 42 tag numbers of the oneof; four of them are reserved
and never reused. Direction is the side that sends the variant, always in a
`request` frame; a `CommandResponse` in the last column travels back in a
`response` frame.

| Tag | Variant | Direction | Reply |
|-----|---------|-----------|-------|
| 2 | `AssignRunnerId` | C → R | `CommandResponse` |
| 3 | `ConnectSocket` | C → R | `CommandResponse` |
| 4 | `AddRelay` | C → R | `CommandResponse` (`raw_data` = `AddRelayResponse`) |
| 5 | `RemRelay` | C → R | `CommandResponse` |
| 6 | `MigrationSerialization` | C → R | `CommandResponse` (`raw_data` = JSON state) |
| 7 | `MigrationDeserialization` | C → R | `CommandResponse` |
| 8 | `CellState` | C → R | `CommandResponse` (`raw_data` = JSON state; frame metadata `flz.change_seq`) |
| 9 | `RunnerDisconnect` | C → R | `CommandResponse` |
| 10 | `RelayMessage` | C → R | `CommandResponse` (`raw_data` = `RelayMessageResponse`) |
| 11 | reserved (was `CellApplyState`) | — | — |
| 12 | `RunnerShutdownNotification` | R → C | `RunnerShutdownAck` (tag 13) under the same `reference_id` |
| 13 | `RunnerShutdownAck` | C → R | none |
| 14 | `RestoreInitState` | C → R | `CommandResponse` (`raw_data` = `RestoreInitStateResponse`, tag 15) |
| 15 | `RestoreInitStateResponse` | R → C | rides only as `CommandResponse.raw_data` |
| 16 | `CompactCell` | C → R | `CommandResponse` (`raw_data` = `CompactCellResponse`, tag 17) |
| 17 | `CompactCellResponse` | R → C | rides only as `CommandResponse.raw_data` |
| 18 | `CellMailboxStatus` | C → R | none (one-way) |
| 19 | `Ping` | C → R | `CommandResponse` (`raw_data` = `PingResponse`) |
| 20 | reserved (was `RunnerReconnected`) | — | — |
| 21 | reserved (was `ReassignRunnerId`) | — | — |
| 22 | `RelayCompleted` | R → C | none |
| 23 | `StepsStop` | C → R | `CommandResponse` |
| 24 | `CreditWindowInit` | C → R | `CommandResponse` (`raw_data` = `CreditWindowAck`, tag 25) |
| 25 | `CreditWindowAck` | R → C | rides only as `CommandResponse.raw_data` |
| 26 | `CreditGrant` | C → R | generic `CommandResponse`, ignored |
| 27 | `ParkPayloadAck` | C → R | generic `CommandResponse`, ignored |
| 28 | `TaskCommit` | R → C | `TaskCommitResponse` (tag 29) under the same `reference_id` |
| 29 | `TaskCommitResponse` | C → R | none |
| 30 | `IterCommit` | R → C | `IterCommitResponse` (tag 31) under the same `reference_id` |
| 31 | `IterCommitResponse` | C → R | none |
| 32 | `IterPark` | R → C | none |
| 33 | `UpdateFactories` | R → C | none (fire-and-forget) |
| 34 | `SpawnStepsRequest` | R → C | `SpawnStepsResponse` (tag 35) under the same `reference_id` |
| 35 | `SpawnStepsResponse` | C → R | none |
| 36 | `AdminRequest` | R → C | `AdminResponse` (tag 37) under the same `reference_id` |
| 37 | `AdminResponse` | C → R | none |
| 38 | reserved (was `RelayExited`) | — | — |
| 39 | `ConnectRefused` | C → R | none; the controller closes the stream after it |
| 40 | `EffectResult` | R → C | `EffectResultAck` (tag 41) under the same `reference_id` |
| 41 | `EffectResultAck` | C → R | none |
| 42 | `EffectResults` | C → R | `CommandResponse` once the batch is stored |
| 43 | `TaskletActivity` | C → R | `CommandResponse` (`raw_data` = `TaskletActivityResponse`) |

### Request/response pattern

- **Framing.** The `ControlFrame` is protobuf-encoded and wrapped in a frame
  with payload id `control_command` and (normally) empty metadata; a runner
  answers under the payload id it received.
- **Correlation.** `reference_id` is opaque; the runner echoes it in its
  `CommandResponse`. A runner-initiated request (tags 12, 28, 30, 34, 36,
  40) is answered by a controller-initiated `CommandRequest` carrying the
  same `reference_id`.
- **Runner-initiated variants.** Tags 12, 22, 28, 30, 32, 33, 34, 36 and 40
  travel runner → controller as `request` frames. The controller dispatches a
  `request` by its tag and hands a `response` to the `ControlUnit` that issued
  the request it answers.
- **Unknown variants.** Both sides skip an unrecognised oneof tag via the
  decoder's unknown-field handling. A tag a side never receives is logged and
  dropped, never answered, and a `response` reaching a runner is dropped with a
  notice.
- **Reply metadata.** A handler may attach metadata to the reply **frame**
  (`CellState` → `flz.change_seq`).

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
  │<─ 9. Ping / AddRelay / ... (READY) ───────│
  │── 9. CommandResponse ────────────────────────>│
  │<─ 10. RunnerDisconnect ───────────────────────│
  │── 10. CommandResponse(success) ──────────────>│  11. both sides close
```

### Step 2: handshake init message

One frame, payload id `connect_control_channel`, empty metadata, JSON message:

```json
{
  "namespace": "<namespace>",
  "factories": [{"id": "<factory_id>", "description": "", "sdk_lang": 1, "run": "steps",
                 "each": "once", "on_failure": "undo_all", "api": [], "graph": null}, ...],
  "stable_id": "<namespace>-<profile_hash8>-<rand6>",
  "incarnation": "<opaque, minted once per process>",
  "namespace_secret": "flzt_<base32>"
}
```

- `namespace` — the logical group the runner belongs to (also the namespace
  of the `runner@<namespace>` principal). It is what the runner ASKS for: the
  controller checks it against what the connection proved and refuses a frame
  naming a namespace the credential does not entitle, naming both namespaces in
  the refusal. `_system` is refused on every transport. A refused connect is
  answered with one `ConnectRefused { error }` frame (tag 39) before the
  controller closes the stream, so the runner reports the code (for example
  `runner.namespace_not_proven`) instead of retrying in silence.
- `factories` — every factory the runner currently declares, each a
  [`FactoryDecl`](#updatefactories) serialized under its field names: `id`,
  `description` (`""` for none), the class's `sdk_lang`, its `run` (`"flow"`
  or `"steps"`), a steps class's `each` and `on_failure` (`""` for a flow),
  `api` (`[]` for none) and `graph` (`null` for none). It is read at write
  time so a reconnect resyncs post-start defines/removes: the init frame is
  the resync of record. The controller reads these entries and
  `UpdateFactories` with one reader, which takes `""` and an absent value
  alike, so a blank `description` never replaces another runner's.
  The controller establishes each factory's language, run, `each` and
  `on_failure` from the first announcement and places only on a runner
  announcing them as established; an entry with no run never places. A start names only the factory: the runner
  runs it as its class declares.
- `stable_id` — the runner's durable, self-owned identity, present on **every**
  connect (`profile_hash8` hashes the declared factory set order-independently,
  `rand6` is a per-process random suffix). The controller binds relays to
  it and on a reconnect swaps the prior session's bindings onto the new token.
- `incarnation` — an opaque value the runner process mints once and presents
  on every connect it makes. Under a `stable_id` the controller already holds,
  the same incarnation is a reconnect of the process that still runs the
  relays, so only the bindings move; a new one is a restarted process that
  runs none of them, so each relay is bound onto it afresh and resumes from
  its durable state.
- `namespace_secret` — the credential the claimed namespace is checked against,
  from `FLEZHA_NAMESPACE_SECRET`. **Omitted entirely** when the runner holds
  none, which is what a deployment authenticating with an `/ns/`-bearing
  certificate, and one whose controller enforces nothing, both send. Presented on
  the control channel only: a data stream is bound to a session the control
  channel already minted, so a data stream on a connection that was refused
  reaches no channel at all.

### Step 3: controller processing

`Flezha.Transport.Server` hands the frame to
`Flezha.Runner.FleetManager.connect_handler/4`, which first resolves the
namespace the connection proved (`Flezha.AuthZ.RunnerProof`) and refuses the
connect if the frame named another one. Only then does it mint a fresh **session
token** (the `runner_id` below), resolve `stable_id`, store the factory set,
start the runner's supervision tree (`ControlChannel`, `ControlUnit`, stream
supervisors) and start or join the namespace hub, bounded by
`:cluster_formation_timeout_ms` (15 s) + 5 s. The session remembers the
credential that admitted it, so revoking that credential closes this channel.

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

A step the runner cannot take is a failed connect, not a failed runner, on
either transport. Over UDS a socket directory that will not resolve, a data
socket the controller has not bound (after a dial ladder of about 3 s), a
descriptor that will not clone and a poisoned stream table each reply
`status: "error"`; over QUIC an absent client, a stream the connection will not
open and a stream-type byte that will not write do the same. The stream
refusals name the half and the step (`"secondary stream open failed: ..."`),
the absent client is `"QUIC client not available"`.

The controller records the session only once the runner has answered both
`AssignRunnerId` and `ConnectSocket`. A refusal, no answer within
`control_unit_reply_timeout_ms` (5 s), or a disconnect first ends the session
and closes the control stream, and the runner counts it as a failed attempt.

**The pair set is all or nothing.** A connect that refuses at any step
installs no stream and leaves the runner's stream map as it found it. A
partly installed set would be worse than none: its secondaries answer the
data heartbeat, so the runner reports a live data plane while the primaries
beside them have no reader and every input on them is lost.

### Step 9: steady state

The controller issues a `Ping` round-trip (default timeout
`:overwatch_ping_timeout_ms`, 2 s) before each `AddRelay`, then the
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
close, emitting `[:flezha, :runner, :liveness_timeout]`.

### Step 10: disconnect

`RunnerDisconnect` sets the runner's status to `Stopped`; the runner replies
`success`, the controller closes the control socket, and the runner closes its
streams. This is intentional — the runner MUST NOT reconnect.

### Auto-reconnection

A control read error while the status is `Running` drives a reconnect. The
runner keeps its relay registry and per-Cell state, marks itself
`Stopped` for the duration, re-dials with the same handshake (same
`stable_id`) and returns to `Running` on the new stream. Every connect attempt
retries forever with jittered exponential backoff, the `flz_transport::backoff::Ladder`
`min(100 ms × 2^min(attempt, 8), cap)` — 200 ms, 400 ms, … 25.6 s — each drawn from
`[base/2 + 1, base/2 + base]` ms and held at the cap, which is
`FLEZHA_RECONNECT_MAX_BACKOFF_MS` (default 30 s). Reasons (logged, and carried
by `[:flezha, :runner, :reconnected]`): `control_stream_broken`, `read_error`,
`write_error`, `keepalive_timeout`, `controller_requested`, and
`broken_pipe_flood` from the reliable state output (§7).

The attempt count belongs to the runner, not to one connect: it resets only
when a handshake completed, meaning the runner answered `ConnectSocket` with its
pair set up and the controller then sent it another frame. A session that ends
before that, including one whose first frame arrived, counts as a failed
attempt. The ack deadline above still switches on the first frame; the two
are separate facts.

A data stream that closes while the control channel is alive is not a reason
to reconnect. The runner lets its whole data plane go, and the controller,
which sees its own end of each stream close, releases every data channel of the
session and re-issues `ConnectSocket` on it. A session gets three such rebinds;
the data-plane loss after them ends the session.

On the controller the reconnect is a **binding swap**: the re-presented
`stable_id` resolves to the prior session, its bindings move to the new token,
and `AddRelay` is re-issued for every instance still bound to the runner;
the runner's handler is idempotent on a duplicate `relay_id`, so existing
Cell state survives. A runner that never returns is removed after
`FLEZHA_RUNNER_REMOVAL_GRACE_MS` (default 30000 ms; `0` disables the grace).

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
echoing the nonce. Issued before `AddRelay` and by `flz` diagnostics; the
round-trip proves the channel is bound, the runner is listening and the reply
path is intact.

### AddRelay

`AddRelay` carries `relay_id`, `factory_id`, `init_values` (JSON; empty for
none), `resume_cell_count` (the `RestoreInitState` frames to expect on this
bind), `task_journal` (steps crash-resume state, §11), the partition identity
(below), `inbox_tail_count` (the input-log records the bind re-forwards) and
`effect_result_count` (the effect results shipped as `EffectResults` after the
bind) and `max_duration_remaining_ms` (what is left of the run's window, which
the controller counts from the run's first bind on its own clock). Fields 4 and
12 are reserved: the runner runs a class as the class declares, and every
controller parks a failed turn sent as `IterCommit.dead_letter`.

| Field | Value |
|-------|-------|
| Behaviour | Create an instance of `factory_id` under `relay_id`. A flow class: a flow relay whose non-empty `init_values` JSON is applied as initial state in the tasklet-keyed shape of "Relay state JSON shape" above. A steps class: a steps instance, whose run is a pass sheet its events advance, its non-empty `init_values` applied as initial state in the same shape, its input the messages that arrive on its entry channels; a durable instance holds its first tasklet until `resume_cell_count` restores and `effect_result_count` effect results have landed. For either run, while any of its `inbox_tail_count` owed input-log records has not arrived its idle deadline is 60 s instead of its class's `sleep_after`; a class with `max_duration_ms` arms its stop `max_duration_remaining_ms` out, or its whole window when the field is absent, and a 0 is due at once |
| Idempotency | A duplicate `relay_id` (a reconnect re-issue) is answered `success` without re-registering |
| Reply | `raw_data` = `AddRelayResponse { map<string,string> metadata }` — empty when the class declares nothing |
| Errors | factory not found; JSON parse / deserialization failure; `PartitionsWithoutPerInputLoop: ...`; `PartitionsWithoutKeyedTasklet: ...` |

`AddRelayResponse.metadata` carries the class's SDK-side declarations so
the controller resolves per-class capabilities without a parallel config
channel (absent/undecodable ⇒ every default). How the class runs, and a steps
class's `each` and `on_failure`, are not among them: they ride the factory's
announcement (`FactoryDecl`), and the controller reads them as placement
does: the factory's established record, else its runners' unanimous
announcements:

| Key | Value | Meaning |
|-----|-------|---------|
| `durable` | `"memory"` / `"local"` / `"remote"` | where an instance keeps its work; `remote` withholds barrier acks until the remote backend holds them |
| `sleep_after_ms` | ms | how long an idle instance waits before it sleeps (absent: it never sleeps) |
| `max_duration_ms` | ms | the wall-clock stop, when the class declares one; the controller counts it from the run's first bind and sends what is left as `AddRelay.max_duration_remaining_ms` |
| `exactly` | `"effectively_once"` | flow only: the class runs on the effectively-once tier |
| `keyed` | `"false"` | flow class with no keyed entry-fed tasklet (sent only when false) |
| `correction_ms` | ms | late-input correction window |
| `beyond_window` | `"process_as_arrival"` / `"drop"` | what a late input beyond the window gets |
| `ack` | `"checkpoint"` | the class acknowledges input only at checkpoints |
| `compact_after_bytes` | bytes | the smallest per-tasklet compaction trigger, when it differs from the default |
| `compensable_tasks` | list | steps relay tree positions whose tasklet declares `compensate` |

#### Partition identity

`partition_index` / `partition_count` / `key_group_from` / `key_group_to`
carry one member of a partitioned relay over 256 key groups
([`../../public/topics/core/partitioning.md`](../../public/topics/core/partitioning.md)). With
`partition_count < 2` the instance owns the whole space; otherwise the runner
checks that each computed Cell key hashes into
`[key_group_from, key_group_to]` and parks a miss with an error naming the
key, its key group and the owning partition. A migrate-time `AddRelay`
carries the same fields, for either kind. A steps class whose loop mode is
`once` or `chained` is refused first, with `PartitionsWithoutPerInputLoop`:
its one message would reach one member while the others waited. A class with
no keyed entry-fed tasklet is refused with `PartitionsWithoutKeyedTasklet`.
A flow class answers `keyed: "false"` and a steps class its `each`,
so the controller refuses a later partitioned start of a known class before
touching a runner. On a steps relay member a refused entry key parks the message
under its entry channel and opens no pass.

### RemRelay

`RemRelay { relay_id: string }` removes the instance from the registry
(hard teardown). Error: `runtime.relay_not_found`.

### StepsStop

`StepsStop { relay_id: string }` is the cooperative stop
(`flz relay stop`): the runner trips the instance's cancellation token,
lets in-flight work unwind, runs any saga compensation, then emits the normal
`RelayCompleted` with status `"cancelled"`. An unknown id is answered
`success` and ignored.

### RelayCompleted

`RelayCompleted { relay_id, status, error, result: bytes }` travels
runner → controller, one-way, when an instance reaches an exit
trigger. `status` is `"ok"` (a steps run's clean end), `"completed"` (a flow
instance's `complete`), `"error"` (with `error`) or `"cancelled"`; `result` is the terminal payload. The controller
drives the single stop/teardown path (deregister-on-completion) so a finished
instance leaves the durable registry and is never replayed on boot; an errored
one is torn down identically (terminal — the platform never re-runs it; a
structured chain's per-Cell retry ladder runs *inside* the run) and surfaced
via `[:flezha, :steps, :failed]`.

An opted-out instance writes its plugin-bound outputs straight
to the data stream — a frame whose payload id is the instance id, on a
secondary stream — so those outputs and this completion travel different
sockets. Before deregistering the instance the controller **flushes** the
runner's stream channels: one bounded synchronous call per channel, which is
answered only after every frame already in that channel's mailbox has been
routed onward. Nothing is counted, because a count of what the runner wrote is
decidable only while nothing is ever lost. A durable class's outputs ride its
Cells' effect records instead, and that debt (exact and durable) is waited for
under a 60 s bound, until every Cell writer of the instance owes nothing; an
entry still held for a stage marker is owed nothing. Field 5 was a `direct_outputs` count and is
reserved; a runner that still sets it is simply not read.

### CellState

| Field | Value |
|-------|-------|
| Message | `CellState { relay_id, tasklet, cell_key }` |
| Behaviour | Serialize live in-memory Cell state as JSON (shape in §3), narrowed by the selector |
| Reply | `raw_data` = JSON; reply-frame metadata `flz.change_seq` when the selector names exactly one Cell |
| Errors | `Relay instance with id {id} not found`, plus the refusal tokens below |

**Selector.** `tasklet` and `cell_key` are optional (empty = absent), so a
selector-less command encodes byte-identically to the migration/handover
shape. Absent ⇒ every Tasklet; `tasklet` ⇒ that Tasklet's Cells;
`tasklet` + `cell_key` ⇒ one Cell.

**Freshness.** Serialization takes each Cell's state lock and does NOT drain
pending changes: the value is at or ahead of the durable state, and
`flz.change_seq` tells a reader which version it got. A read triggers no
compaction, snapshot or WAL truncation.

| Refusal code (the `Error.code`) | Meaning |
|---|---|
| `state.cell_key_without_tasklet` | a Cell key is unique only within its Tasklet |
| `state.tasklet_not_found` | the Relay declares no Tasklet by that name |
| `state.cell_not_found` | that Tasklet holds no Cell by that key |
| `state.cell_evicted` | the Cell was idle-evicted; eviction is destructive, so there is no durable value either |

The operations surface is `flz state get`
([`../state_persistence.md`](../state_persistence.md)).

### TaskletActivity

| Field | Value |
|-------|-------|
| Message | `TaskletActivity { relay_id, cells_of?, limit? }` |
| Behaviour | One row per tasklet of the instance, summed over its Cells from figures the data path already keeps; with `cells_of`, that tasklet's Cells as well |
| Reply | `raw_data` = `TaskletActivityResponse { rows, read_at_ms, instance_seq, cells? }` |
| Errors | `runtime.relay_not_found` naming the relay; `state.tasklet_not_found` naming a `cells_of` the class does not declare |

Each `TaskletActivityRow` carries the tasklet's name and live `cells`, then
`queued` (in total) and `queued_max` (in the deepest Cell), and the totals
`admitted_total`, `blocked_total`, `dropped_newest_total` and
`dropped_oldest_total`. A dynamic tasklet adds `dropped_capped_total`,
`cells_created_total` and `cells_evicted_total`. Every figure but `tasklet` and
`cells` is `optional`: a runtime that does not keep one leaves it absent, never
zero.

**Totals.** Totals rise for the life of one instance, an evicted Cell's
included. `admitted_total` counts mailbox items, timer firings and interval
closes included; `blocked_total` counts wait rounds, not blocked pushes.
`queued` is approximate while items arrive and leaves out items held outside
the mailbox.

**Instance.** `read_at_ms` is the runner's monotonic clock at the read.
`instance_seq` names the instance that answered: a rebind, a migration, a wake
or a runner restart builds a new one, and every total starts again from zero.
A caller computing rates restarts them when `instance_seq` or the runner
changes.

**Cells.** `cells_of` names one tasklet whose Cells are answered too, as
`cells = TaskletCells { tasklet, cells_total, rows }`. `rows` holds at most
`limit` Cells (default 50, clamped to 500), the most recently admitting first,
and `cells_total` counts every live Cell of the tasklet. Each
`CellActivityRow` carries the Cell's `key` (absent for an unkeyed Cell), its
`queued` items, its `admitted_total`, `blocked_total`, `dropped_newest_total`
and `dropped_oldest_total`, and `last_admitted_ms` on the clock of
`read_at_ms`; a Cell that has admitted nothing yet carries the time it was
created. A runtime that keeps no per-Cell counters leaves `cells` absent, never
empty, so absent also means "this runner does not report Cells".

**Cost.** The read adds nothing to the per-message path. A `flz_core` runner
resolves the relay, and the tasklet `cells_of` names, on the control reader
and walks its Cells on a thread of its own, so a relay with many live Cells
does not hold up the commands behind it. The Cells of `cells_of` are
picked during that same walk, keeping the newest `limit` in a bounded heap.
An older runner that cannot decode the command drops it without a reply, and
one that predates `cells_of` answers without `cells`.

### MigrationSerialization

`MigrationSerialization { relay_id }` serializes the instance's whole state
(JSON shape in §3) into `raw_data`, then **removes** the instance from the
registry so the controller can re-create it on another runner.

### MigrationDeserialization

`MigrationDeserialization { relay_id, factory_id, serialized_state: bytes }`
creates the instance from `factory_id`, then applies `serialized_state`.

### RunnerDisconnect

`RunnerDisconnect {}`: set status `Stopped`, reply, close the control socket;
never reconnect.

### RelayMessage

`RelayMessage { relay_id, payload: bytes }` is answered with
`status: "success"` and `raw_data` = `RelayMessageResponse { payload:
bytes, error: Error }`. An absent `error` means success and the controller
reads `payload`; a present one means the instance reported a failure and the
controller SHALL discard `payload`. A `flz_core` runner always answers the
code `runtime.relay_message_unsupported` with an empty payload — the kernel
has no per-instance message hook. Sent by
`Flezha.Plugin.Provider.MessageForward` via
`ControlUnit.send_relay_message/3`; the
caller owns the timeout, and payloads share the control stream — keep them
small.

### RunnerShutdownNotification

`RunnerShutdownNotification { relay_id, serialized_state: bytes }` travels
runner → controller in a `request` frame (tag 12) on graceful
shutdown only — SIGTERM, drop of the SDK handle, explicit `disconnect()`;
SIGKILL and panics cannot emit and fall back to the last persisted state. Per
running instance the runner serializes through the same serializer
`MigrationSerialization` uses and emits one notification; the controller
forwards the bytes to the instance's Overwatch snapshot and answers
`RunnerShutdownAck` under the same `reference_id`. The runner waits at most
**2 s per relay** (parallelised), then exits unconditionally and never
blocks its container's grace period; the per-relay serializer is capped at
the controller's `cross_node_request_timeout` ceiling (14 s).

### RunnerShutdownAck

`RunnerShutdownAck { relay_id }` travels controller → runner in a `request`
frame (tag 13), emitted as soon as the snapshot cast is
enqueued on the Overwatch (not after it is applied) and unconditionally — a
missing Overwatch (already migrated to a peer controller) still gets an ack so
the runner can exit.

### UpdateFactories

`UpdateFactories { repeated FactoryDecl factories }` (`FactoryDecl { id,
description, sdk_lang, run, each, on_failure, api, graph }`, the struct whose serde form is each init frame entry)
travels runner → controller, fire-and-forget, after a
post-start define/remove in any SDK (the core's session sends it), carrying the runner's **full**
current factory set. Full-set semantics make it idempotent: `FleetManager`
replaces the runner's stored list (remove-then-store), scoped to the sender; an
empty set is legal and leaves the runner connected with zero placeable
factories. No ack: it gates no durability barrier — a stale placement fails at
the runner's `AddRelay` handler and convergence re-places it; a lost frame
is corrected by the next one or by the reconnect init frame. Dropping a factory
that still has relays bound to the runner logs a `FleetManager` warning
naming namespace, runner, factory id and affected relays.

### ApiEntry

`FactoryDecl.api` (field 7) is the factory's API, one `ApiEntry { channel, codec,
schema_json, description, answer_codec, answer_schema_json }` per entry channel of the class
that is not a call end's response channel. `codec` is `json`, `text`, `bytes`, `msgpack` or
`custom`; `schema_json` and `description` are empty when not declared. `answer_codec` is set
when an output that answers is reachable from the channel along its consumer edges, with that
output's codec and schema; reachable outputs that disagree give `bytes` and no schema. The init
frame's factory entries carry the same six keys under `"api"`. `flz_core` builds the entries
from the host's channel facts (`advertised_for_factory`). A
runner that sends none leaves its factory with no API; the controller keeps each runner's latest
announcement and `Relay.api` reads it.

### RelayGraph

`FactoryDecl.graph` (field 8) is the class's static data flow, `RelayGraph { channels,
tasklets }`, built when the factory is announced and never per message. Each `GraphChannel
{ name, kind, producer, consumers, plugin_end, call_target, overflow }` names a channel of the
class: `kind` is `entry`, `inter_tasklet` or `plugin_bound`; `producer` is the producing
tasklet's name, empty for an entry; `consumers` are the consuming tasklets' names; `plugin_end`
is `call` for a `call(...)` end, `output` for a plugin output, otherwise `none`; `call_target`
is a call end's `id:<relay>` or `factory:<id>`, empty otherwise; `overflow` is `drop_nothing`,
`drop_newest` or `drop_oldest`. Each `GraphTasklet { name, mode, cell_keys, mailbox_capacity }`
names a tasklet in declaration order, with `mode` `single`, `static` or `dynamic`. The init
frame's factory entries carry the same structure as a `"graph"` object with these keys, omitted
when there is none. `flz_core` builds it from the class's topology (`advertised_for_factory`).
A runner that sends none leaves its factory with no graph, and
it is still placed. The controller keeps each runner's latest announcement beside its API, out
of the factory rows a call reads, and `Relay.graph` reads it.

### SpawnStepsRequest

`SpawnStepsRequest { instance_id, factory_id, payload: bytes, if_running:
string, identity: string }` travels runner → controller (tag 34) when a tasklet asks for a
steps instance (`ctx.spawn_steps` / `ctx.spawnSteps`). The controller
spawns `factory_id` under `instance_id` when free, with `payload` as its first
message (empty = spawn only): an ordinary entry message naming no channel, so
it enters the first declared entry channel, encoded by the SDK's no-codec output
rule (bytes as they are, a string as UTF-8, anything else as JSON); otherwise
`if_running` applies: `"deliver"` (default; any unrecognised value reads as
deliver) sends `payload` to the running instance as one more message,
`"error"` refuses. An empty `instance_id` asks the
controller to generate one; such a request spawns and `if_running` is
ignored.

`identity` names a tasklet's request across a re-run of its input: FNV-1a-128,
rendered as 32 lowercase hex digits, over the requesting Cell's identity, the
input-log offset its invocation took (8 bytes big-endian), the call's position
in the invocation (4 bytes), the factory id and the payload, each part
preceded by its length as 8 bytes big-endian. It is empty for an invocation
that took no offset and for a spawn from outside a tasklet. A generated id is
derived from it, and a delivered payload carries it as its `flz.msg_id`, so a
request whose identity was already resolved answers the relay the first one
started, running or completed, and delivers nothing twice.

**The namespace is not carried**: the controller takes it from the control
channel the request arrived on, which makes cross-namespace spawning impossible
by construction. Resolution runs off the control channel (it starts a
relay, which awaits `AddRelay` on the same stream); concurrent requests
for one `(namespace, instance_id)` race-resolve at the registry — one start
wins, every loser converts to a delivery. Each act is judged as the
`runner@<namespace>` principal the connection proved, before it is performed: a
start against the `relay_start` row, which records it, and a delivery as
`relay.send`, recorded only when refused because it happens once per event.
With authorization enforced, an unbound principal is `runner`, and a start or a
delivery answers `refused`. See
[`workflows.md`](../../public/topics/core/workflows.md#starting-one-from-a-tasklet).

### SpawnStepsResponse

`SpawnStepsResponse { outcome, error, instance_id }` returns controller →
runner under the request's `reference_id`; `outcome` is a success word,
`spawned` or `delivered`, and is empty when `error` carries the refusal under
the controller's own code: `relay.already_running`,
`relay.factory_unknown`, `relay.factory_not_steps` (a factory its
runners announced as a flow relay, decided before anything is placed),
an `authz.*` or `quota.exceeded` code, or any other. `instance_id` is the id
the request resolved to: the requester's own, or
the generated one (empty when a request that carried none started nothing). It resolves the requester's result handle and never faults the
requesting tasklet (an unobserved handle logs the failure once); against an
older controller the request is ignored and the requester times out.

### AdminRequest

`AdminRequest { verb, args }` goes runner → controller when application code
administers the namespace its connection proved. One frame serves every verb,
because the verb is data: `verb` names a function of the controller's published
consumer surface as `"<Module>.<function>"` (`"Relay.start"`), and `args`
is a JSON array of that call's arguments other than the namespace.

**There is no namespace field and no principal field**, and that is the safety
property rather than an omission: the controller fills both from the control
channel the request arrived on, and fills the namespace slot each verb's own
row declares, so naming another namespace is unexpressible rather than
validated. What the caller may reach follows from the role its
`runner@<namespace>` principal is bound to, which the controller reads on every
call. Unbound, that is `runner`, and every administration verb is refused. An
operation the catalogue marks as not `remote`, or as streaming, is refused as
`control.unknown_function`: a request gets one reply, so a stream has nowhere to go.

Every host reaches it through one verb-keyed C ABI entry,
`flz_core_admin_call`. A runner's send rides the same frame: `flz_core_send_message` makes the
`Relay.send` request, or, local, hands the message to a relay bound on
that runner and sends no frame at all.

### AdminResponse

`AdminResponse { result, error }` returns controller → runner under the
request's `reference_id`. `result` is the published function's own answer
rendered as JSON; `error` carries a refusal (policy, quota, a name that
resolves to nothing, or the effect's own failure) under the code a `flz` user
sees for the same refusal, and is absent otherwise.
Against an older controller the request is ignored and the requester times out.

The persistence commands (`RestoreInitState`, `CompactCell`,
`CellMailboxStatus`, `CreditWindowInit` / `CreditWindowAck` / `CreditGrant`,
`ParkPayloadAck`, `TaskCommit`, `IterCommit`, `IterPark`) are specified in
§11.

## 7. Data Plane (Stream I/O)

| Stream | Direction | Carries |
|--------|-----------|---------|
| Primary | controller → runner | Relay input, replay slices, wake entries |
| Secondary | runner → controller | Relay output, `data_heartbeat`, every persistence frame of §11 |

Stream frames use the §3 format with payload id = the target instance's
`relay_id` (or one of the reserved ids of §3).

**Input (primary streams).** The runner resolves the instance by payload id
(an unknown instance is logged and the frame dropped). For a **stream**
instance it resolves the entry channel from `metadata["channel"]` (absent ⇒
the first declared input channel; a name that is not one of its entry channels
parks to the dead-letter queue under that name, with one attempt), wraps the payload
bytes through that
channel's declared type, and pushes the item onto every consumer Tasklet's
Cell mailbox under the mailbox policy of §3; `flz.offset`, `flz.replay` and
`flz.replay_end` ride along as described in §11. A **steps relay** instance takes
the same entrance: the channel is resolved the same way (an undeclared name
parks under that name), the payload is wrapped through the channel's declared
type or parser, a parse failure rides the entry channel's retry ladder to the
dead-letter queue, and the entry edge's transforms address the typed value to
the roots of that channel's chain on the reader. The message, with the channel
it entered on and the `flz.offset` it carries, starts a pass when the instance
is waiting and otherwise waits in the instance's queue for the next one. Dispatch onto host threads, per-Cell ordering and exclusivity are
the poll core's job ([`poll_core.md`](poll_core.md)).

**Output (secondary streams).** Every plugin-bound emit becomes one frame with
payload id = the emitting instance's id, the output bytes as the message, and
metadata = user metadata + `channel` (+ the `flz.*` identity stamps on
`.keyed()` channels). Each output is framed by `payloader::build_from_binary` and
written through `flz_core::state_sink::write`. An output whose write the stream refuses,
or that finds no stream at all, is held for the next bound one rather than
dropped — its class says so, and the ring is the one below. On the effectively-once tier
plugin emits never ride this path: they travel inside the effect record (§11) and
the controller dispatches them only after the record's fsync.

**What the wire owes a frame, per class.** Every frame an SDK writes carries a
declared **guarantee class**, and the class — not the write site — decides what
happens when the wire refuses it. There are three, the same in every runtime:

- **control** — a control-channel command, a reply, a keepalive ping, a channel
  handshake. Refused to its caller, or counted where the caller is a cast, and
  never held: a reply re-driven after its waiter timed out is a stale answer,
  and the reconnect re-issues the request it belonged to.
- **durable** — a change record, an effect record, a compaction snapshot, an
  eviction's delete, a saga marker, a park payload, a shutdown state ship. Held
  in the ring below and replayed in write order on the next bound stream.
- **output** — a plugin or steps relay emit. Held **already framed**, so a keyed
  output's `channel` and its `flz.*` identity stamps survive the replay; a
  re-framed record would carry empty metadata and strip them.

The `data_heartbeat` is owed nothing at all and is never held: it reports a
moment that has passed, so replaying it after a rebind is worse than losing it.
Its write verdict feeds the reconnect ladder instead. A payload id no class
table names is refused at the call rather than given a default, which is what
makes a write site with a policy of its own impossible.

**Reliable state output.** The held classes queue in a ring of
`FLEZHA_RECONNECT_RING_CAPACITY` (default 1024; the oldest is dropped on
overflow, counted as a drop-oldest shed) and are replayed once a stream is back
— a record re-framed under its own payload id, a frame a writer built itself
replayed byte for byte. `FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD` (10) write failures
inside `FLEZHA_BROKEN_PIPE_WINDOW_MS` (5000) request a control reconnect with
reason `broken_pipe_flood`, re-armed after `FLEZHA_RECONNECT_REQUEST_RETRY_MS`
(30000) if unanswered. The entry point is `flz_core::state_sink::write`, shared by
every SDK.

**Non-blocking I/O.** Primary sockets are non-blocking and polled through an
event mechanism (the `flz_core` reader uses `mio`). A wake may
carry several frames — drain them all before re-polling. A secondary-stream
write that returns `WouldBlock` is retried on the next writable event.

## 8. Recommended SDK Public API

Not part of the wire contract. The authoring surface every SDK must offer —
factories, Tasklets, Cells, channels, codecs, the runner handle and its
env knobs (`FLEZHA_WORKER_POOL_SIZE`, `FLEZHA_WORKER_POOL_MAX`,
`FLEZHA_MAX_INFLIGHT_ACTIVATIONS`) — is specified in
[`authoring_contract.md`](authoring_contract.md); the kernel/host split that
decides which of it lives below the poll seam is in
[`../../public/architecture/runner.md`](../../public/architecture/runner.md).

## 9. Worked Examples

### 9.1 Control command frame (`AssignRunnerId`)

`ControlFrame { request: CommandRequest { reference_id: "#Ref<0.1.2.3>", assign_runner_id: { runner_id: "ce_001" } } }`:

```text
Protobuf (27 bytes):
  0A 19                                             field 1 request, len 25
  0A 0D 23 52 65 66 3C 30 2E 31 2E 32 2E 33 3E     └ field 1 reference_id, len 13
  12 08 0A 06 63 65 5F 30 30 31                     └ field 2 assign_runner_id, len 8
                                                      └ field 1 runner_id, len 6

Payload ID (32 bytes):
  63 6F 6E 74 72 6F 6C 5F 63 6F 6D 6D 61 6E 64 20   control_command.
  20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20   ................

Payload Size (4): 4 + 27 + 2 + 0 = 33          00 00 00 21
Message Size (4): 27                           00 00 00 1B
Message (27 bytes): the protobuf bytes above
Metadata Size (2): 0                           00 00

Total frame: 32 + 4 + 33 = 69 bytes
```

The runner answers `ControlFrame { response: CommandResponse { reference_id:
"#Ref<0.1.2.3>", status: "success" } }` under the same payload id. The handshake frame (§5) is
built the same way with payload id `connect_control_channel` and the JSON
bytes as the message.

### 9.2 Data frame (stream input)

Text input `hello` for instance `task_42` with metadata `{input_type: "2"}`:

```text
Metadata entry: key_len=10, val_len=1, "input_type", "2"
  0A 01 69 6E 70 75 74 5F 74 79 70 65 32             (13 bytes)

Payload ID (32 bytes):
  70 72 6F 63 5F 34 32 20 20 20 20 20 20 20 20 20   task_42.........
  20 20 20 20 20 20 20 20 20 20 20 20 20 20 20 20   ................

Payload Size (4): 4 + 5 + 2 + 13 = 24          00 00 00 18
Message Size (4): 5                            00 00 00 05
Message (5 bytes): 68 65 6C 6C 6F              hello
Metadata Size (2): 13                          00 0D
Metadata (13 bytes): 0A 01 69 6E 70 75 74 5F 74 79 70 65 32

Total frame: 32 + 4 + 24 = 60 bytes
```

## 10. Error Handling

**Command errors.** A failing handler replies `status: "error"` with one
`Error` per failure in `errors` and normally an empty `raw_data`. Every failure
on the wire is that one message: a command reply, `TaskCommitResponse`,
`IterCommitResponse`, `RelayMessageResponse`, `SpawnStepsResponse`,
`AdminResponse` and `ConnectRefused`. The receiver branches on `code` (for
example `runtime.class_not_registered`, `state.cell_key_without_tasklet`),
never on `message`, which may be reworded.
`class` tells a caller what it can do about it, and `details_json` carries the
facts the message names. Every code, its class and its message are listed in
[`../../public/errors.md`](../../public/errors.md).

**Connection close detection.** On the controller a closed control stream
arrives as `{:flz_closed, ...}` from the NIF driver and the `ControlChannel`
runs the disconnect handling (preservation for `FLEZHA_RUNNER_REMOVAL_GRACE_MS`,
migration or re-placement); the §5 watchdog turns sustained silence into the
same path. On the runner the control reader retries `Interrupted`; a timeout
(`WouldBlock`/`TimedOut`) is fatal in both deadline states, and `BrokenPipe`,
`UnexpectedEof`, `ConnectionReset` and `InvalidData` (a frame-limit violation
or a malformed body) are stream-fatal — every fatal error drives the §5 reconnect unless the status
is `Stopped`.

**Malformed frames.** A reader MUST NOT discard partial data (§3 "Frame
buffering"). A frame whose declared length exceeds the limit, or whose body's
inner lengths do not fit it (§3 "Frame limits"), poisons the stream — no
resync; the stream is closed and re-established. Any other decode failure
(a bad protobuf body, an undecodable persistence body) is logged and the frame
dropped; the stream stays open because the outer framing was intact.

## 11. Per-Cell State Persistence

This section is the wire contract for persistence. The per-SDK trait surface
(`Tasklet::init_state` / `Change::apply_to`, codecs, schema versions) is in
[`state_persistence.md`](state_persistence.md); the controller's on-disk
layout, WAL writer and `flz state` CLI are in
[`../state_persistence.md`](../state_persistence.md).

### Data-stream payload-id `cell_state_change`

Runner → controller. One Change record for the per-Cell WAL. Frame layout
(decoded by `Flezha.State.Wire.decode_frame/1`):

```text
[cell_id_len: u16 BE]
[cell_id_bytes]
[seq:        u64 BE]
[header:     16 B  ]      <- see "The 16-byte record header"
[body_len:   u32 BE]
[body]                    <- postcard-encoded Change
[crc32:      u32 BE]      <- crc32 over body
```

`cell_id_bytes` is structured so the controller routes the record to the
right per-Cell WAL without a side table:

```text
[relay_class_len: u8   ][relay_class]
[tasklet_id_len:    u8   ][tasklet_id]
[cell_key_len:     u16 BE][cell_key]
```

#### The 16-byte record header

| Offset | Field | Value |
|--------|-------|-------|
| 0 | `sdk_lang: u8` | `1` Rust, `2` Java, `3` Elixir, `4` Node, `5` Ruby, `6` PHP, `7` Python — the allocation table in `flz_core/src/header.rs` |
| 1..5 | `sdk_version: u32 BE` | `(major << 16) \| (minor << 8) \| patch` of the **writing SDK's** own released version, each component clamped at 255; `0` means the SDK did not say. Advisory: no reader decides how a body is read from it |
| 5..9 | `schema_version: u32 BE` | the Tasklet's declared schema version |
| 9 | `format_id: u8` | `1` postcard change, `2` effect record, `3` timer-ops record, `4` Kryo change (Java), `5` MessagePack change (Node, Ruby, Python), `6` JSON change (PHP), `7` External Term Format change (Elixir) |
| 10..12 | reserved | zero |
| 12..16 | `crc32: u32 BE` | over the body — written, and read back by nothing: the record frame's own trailing crc is the one that is checked |

The controller stores, truncates and re-ships every `format_id` identically
and decodes only `format_id = 2`. A **timer-ops record** (`format_id = 3`)
carries a postcard `Vec<TimerOp>` mutating the Cell's pending-timer set; the
controller never decodes it — the pending set is SDK-owned state
([`../../public/topics/core/timers.md`](../../public/topics/core/timers.md)). On restore the kernel validates the
header for every host it serves and refuses a mismatch by name, the language before anything else:
`RestoreError::LangMismatch` (one SDK reading another's header),
`SchemaFromTheFuture` / `SchemaTooOld`, `CrcMismatch`, `HeaderInvalid`.

### Data-stream payload-id `cell_effect_record`

Runner → controller, from a Cell that records effects: a flow Cell on the
effectively-once tier, and every Cell of a durable steps class (which does
not declare the tier; the record is its durability). A persistent Cell off the
tier writes one too for the inputs it took with a `flz.offset`: the changes of
a drain fold into one record carrying the highest offset folded, so the offset
rides the record of the state it produced. Same frame as
`cell_state_change` with `format_id = 2`; `body` is the postcard
`EffectRecordBody`: one invocation's state change(s), plugin outputs and
consumed input-log offset, persisted by the controller as ONE WAL record under
ONE fsync so state, outputs and progress can never tear apart. A steps relay
Cell's record carries no consumed offset (the iteration's `IterCommit` does)
and no dead-letter park, and is sent as the item settles, ahead of the stage's
`TaskCommit`, never on the flush linger. Decoded by
`Flezha.State.Wire.decode_effect_body/1`:

```text
EffectRecordBody {
    changes:         Vec<Vec<u8>>,      -- Change bytes in the writing SDK's own codec
                                        -- (postcard, Kryo or ETF), folded in order
    outputs:         Vec<OutputRecord>, -- the invocation's plugin emits
    consumed_offset: Option<u64>,       -- flz.offset of the input; None for a timer firing
    dlq:             Option<DlqEntryBody>,
    event_time:      u64,               -- the invocation's declared clock value
    timers:          Vec<TimerOp>,      -- this invocation's pending-timer mutations
    edges:           Vec<EdgeRecord>,   -- inter-tasklet values, where the record carries the offset
}
EdgeRecord   { channel: u32, consumer: u32, origin: Option<u64>, payload: Vec<u8> }
TimerOp      { kind: u32 (0 event time, 1 processing time), scheduled_at: u64 ms, register: bool }
OutputRecord { channel: String, key: String, metadata: Vec<(String, String)> key-sorted, payload: Vec<u8> }
DlqEntryBody { channel: String, error: String, attempts: u32, payload: Option<Vec<u8>> }
```

Postcard encodes `Vec`/`String` as a LEB128 varint length + contents, structs
as concatenated fields and `Option` as `0x00 | 0x01 ++ T`.

- `edges` keeps each inter-tasklet value the input produced: the channel and
  consuming tasklet as the writing SDK numbers them, the input-log offset it
  derives from, and the value through the channel's codec. A bind delivers
  every kept value whose origin is above the instance's floor again, ahead of
  the re-forwarded tail (`flz.edge`, below); a consumer whose own records took
  that offset drops it.
- `event_time` is what the Tasklet's declared clock evaluated to (the input's
  event time under an event-time clock, wall-clock ms under the wall clock, the
  change seq under the logical clock); late-input correction and `{:timestamp, ts}`
  time travel resolve against it.
- `timers` makes a timer **firing** exactly-once: the Changes `on_timer`
  applied, its emits and the fired timer's removal are one record, which
  carries no `consumed_offset`.
- A tier-on dead-letter park rides the SAME record as its consumed-offset
  advance, so a crash can never advance the offset while losing the park; the
  controller materializes the DLQ entry after the fsync (idempotent on
  `cell_key@consumed_offset`) and sends no `ParkPayloadAck` for it.
- A recorded plugin emit never rides the primary stream: the controller
  dispatches it to the `:on_output` chain only after the record's fsync (for a
  steps relay Cell, only once the durable `task_committed` or `compensated` entry
  naming the record has been acked), stamped with `flz.cell`,
  `flz.seq = <record WAL seq>.<emit ordinal>`, `flz.epoch`, `channel`, `flz.key`
  when keyed, and the author's metadata. Effect records are never coalesced at
  drain time.

### Data-stream payload-id `cell_state_snapshot`

Runner → controller. Same frame as `cell_state_change`; `body` is the full
serialized `State` (postcard / Kryo) captured at `seq`. The controller writes
it to `<cell_key>.snap.<seq>` (`.tmp → fsync → rename`), truncates the WAL
up to `seq` and clears the in-flight compaction.

### Data-stream payload-ids `compensation_intent` / `compensation_done`

Runner → controller, steps saga change-log, same frame as
`cell_state_change`. `compensation_intent` records a tasklet's forward
completion (so a crash mid-compensation knows what to undo),
`compensation_done` a compensation hook's completion (so a resume skips it);
the controller hands them back as the `has_compensation_*` flags of
`RestoreInitState`.

### Data-stream payload-id `delete_cell_state`

Runner → controller, dynamic-mode hard delete. The message is the bare
structured `cell_id_bytes` — no seq, header or crc, so it is **not** the
`cell_state_change` frame the saga markers reuse. Sent when the runner
evicts a persistence-enabled dynamic Cell, under the **durable** class (§7):
a refused write is held and replayed in write order behind that Cell's own
records, never dropped at the write site. The controller drops the Cell's
WAL, snapshot and outbox files so a later recreate cold-starts, ignores the
frame for a non-dynamic class, and writes no response frame. The sweep waits
for none.

### Data-stream payload-id `park_payload`

Runner → controller, dead-letter park for sessions **off** the
effectively-once tier. When an invocation exhausts its input channel's retry
budget (`max_attempts` default 3, exponential backoff ±50 % jitter capped at
5 s) or returns a non-retryable error, the runner ships this frame and holds
the Cell until the controller acks the park as durable (`ParkPayloadAck`),
re-sending every 2 s — park-then-advance, never a drop. A payload the
routing seam could not deliver (a frame naming an undeclared channel, an
unreadable entry, an edge that unwound) parks the same way, held by the
runner's one park owner rather than a thread of its own. The owner holds at
most 4 096 such payloads; past that, the thread handing over the next one (a
frame's reader, or a Cell's worker whose output could not be delivered) waits
until one is acked, so the runner slows down and nothing is dropped. Same outer frame as
`cell_state_change` with a zero header; `body` is the postcard `ParkBody`
(`Flezha.State.Wire.decode_park_body/1`):

```text
ParkBody {
    park_id:       String,        -- SDK-minted ack-correlation id
    origin_offset: Option<u64>,   -- input-log offset when the tier provides one
    entry:         DlqEntryBody,  -- the same {channel, error, attempts, payload} record
}
```

The controller appends the entry to the durable DLQ store
(`storage/app/dlq/<relay_class>/<tasklet_id>/`), fsyncs, then acks.
Appends are idempotent on origin identity (`channel:sha256(payload)`), so an
ack-lost re-send never duplicates an entry.

### Data-stream payload-id `correction_request`

Runner → controller, late-input correction. A Cell saw an input whose
extracted event time is behind its event-time high-water but inside the
declared `correction_ms`; only the runner can know that, so detection is
the runner's half and orchestration the controller's. Same outer frame as
`park_payload` (zero header, postcard body); the Cell identity rides the
envelope:

```text
CorrectionRequestBody { target_event_time: u64 }
```

Inputs are journaled before they are forwarded, so the late input is already
in the input log at its arrival offset and the controller re-reads the range.
Fire-and-forget: concurrent requests per Cell coalesce into one replay, a
duplicate costs nothing, and a lost one is re-raised when the input is
re-delivered. A transport-less run processes the input as an arrival.

The controller marks every input of the re-forwarded slice with
`flz.replay = "1"` and closes the slice with a **payload-less broadcast
frame** carrying `flz.replay_end = "1"`. The Cell holds the slice, orders it
by event time and only then dispatches — an arrival-ordered replay would
recompute the same wrong answer. The end marker is a broadcast because an
input routes to exactly one Cell by key; a flag on the last input would
strand every other Cell holding slice members. A `RestoreInitState` clears
any half-filled replay buffer, and every replay is preceded by a restore.

### Data-stream payload-id `output_part`

Runner → controller, a **part**: a preview of the output an invocation is
producing, sent from inside the invocation (`ctx.part(value)`) rather than
when it returns. The frame body is an output frame's (`build_from_binary`):
the payload is the value encoded as an emit of it on the tasklet's output
channel would be, and the metadata is:

| Key | Value |
|-----|-------|
| `channel` | the tasklet's output channel, which must end in an output |
| `flz.part` | the part's number within the attempt, from `0` |
| `flz.part_attempt` | the invocation's attempt, from `1` |
| `flz.cell` | the Cell's label, `<instance>/<tasklet>/<key>` |
| `flz.key` | the Cell's key, when it has one |
| `flz.relay` | the instance the part belongs to |

The payload id is reserved rather than the instance id, so a controller that
does not know parts routes the frame to an instance named `output_part`, finds
no route and drops it; it never reaches that controller's outputs. The
controller routes a part by `flz.relay` through the session's own route, so
a part from a session that no longer owns the instance is dropped, and hands it
to the Overwatch's `on_preview` hook, which only a listener implements.

A part is never durable and never waited on. The runner writes it only if the
stream takes it at once (a non-blocking write on UDS, a write the QUIC driver
answers without buffering) and drops it otherwise: when no stream is bound, in
a local run, when the write is refused or would wait, and when the frame would
pass `FLEZHA_MAX_FRAME_BYTES`. An invocation of a replayed input
(`flz.replay`) sends none. Every part sent or dropped is counted by reason in
the heartbeat's `parts_sent`, `parts_dropped_no_stream`,
`parts_dropped_refused` and `parts_dropped_too_large`. A retried attempt numbers
its parts from `0` again.

### Control command `RestoreInitState` (controller → runner)

`RestoreInitState` and its reply `RestoreInitStateResponse` are defined in
[`runner_commands.proto`](../../../protobufs/runner_commands.proto). The request names the Cell by its structured
identity bytes (`cell_id`) and ships its full `snapshot_bytes` (empty when
none) with the snapshot's stored 16-byte `snapshot_header`, its WAL tail as
`change_log_bytes`, the saga crash-resume markers `has_compensation_intent` and
`has_compensation_done`, and `terminal_seq`, the highest seq the shipped state
covers. The reply echoes `cell_id` and `terminal_seq` and sets `migrated` when
a record was read at an older schema version.

Each record inside `change_log_bytes` uses the controller's on-disk WAL
framing, back to back:

```text
[seq:    u64 BE]
[len:    u32 BE]    <- 16 + change_bytes.len()
[header: 16 B  ]
[change_bytes]
[crc32:  u32 BE]    <- crc32 over header + change_bytes
```

One frame, one crc, two runtimes. `Flezha.State.RecordFrame` is the only module
on the controller side that cuts this framing or computes that crc, and
`flz_core::restore::fold_change_log` is the kernel's own reader for every host
it serves. The two share no code and are held together by bytes instead:
`test/fixtures/durable_frame.txt` carries golden records, headers, the
`sdk_lang` allocation table and the schema-decision cases, and both runtimes'
tests fold it, so a change to the framing on either side fails the other side's
suite. Note that the trailing crc32 here covers `header <> change_bytes`, while
the wire frame above crcs the **body alone** — two contracts that both say
"frame".

`snapshot_header` is the snapshot file's stored header, shipped rather than
stripped: it carries the `schema_version` the snapshot was written at, and
without it every compacted Cell is unverifiable. It is empty exactly when
`snapshot_bytes` is empty.

The runner resolves every record's header — the snapshot included — against
the running Tasklet's `schema_version` and `min_readable_schema_version`
before decoding anything (see "Schema evolution" below), decodes
`snapshot_bytes` (when non-empty) into `State`, folds the
change-log records through each change's `apply_to`, and re-arms the
Cell's change seq to `max(change-log terminal, terminal_seq) + 1` — a
snapshot-only restore (empty WAL tail after a compaction) has no record to
derive the seq from, and a seq below the snapshot's would have its next
Changes refused as duplicates. `RestoreInitStateResponse` returns as
`raw_data`; the controller blocks the Cell's input until it lands (the
restore rides the control channel, the replay the data channel) and expects
the next `cell_state_change` at `seq = terminal_seq + 1`. The task-journal is
**not** here — it is instance-scoped and rides `AddRelay.task_journal`; a
durable steps class with no per-Cell state receives no `RestoreInitState`
at all.

#### Schema evolution

A record's `schema_version` (header bytes 5..9) is the version of the
Tasklet that wrote it. Per record, against the running Tasklet's
`SCHEMA_VERSION` (`V`) and `MIN_READABLE_SCHEMA_VERSION` (`M`, defaulting to
`V`):

| stamped | outcome |
|---|---|
| `== V` | decode with the current codec |
| `>= M` and `< V` | hand the bytes to the Tasklet's migration for that version |
| `< M` | refuse — the record predates what this build declares it reads |
| `> V` | refuse — older code cannot read newer state; a downgrade, not an evolution |

The decision is per record because one WAL legitimately holds both: records
written before a deploy sit under records written after it. It is made in
`flz_core` for every SDK, so a host-run Tasklet receives only bodies it can
decode at its own version or migrate from a named one, and a refusal never
crosses to the host.

When any record was migrated, the runner sets `migrated` on the response and
the controller answers with the `CompactCell` it already drives on WAL size
and age: the next snapshot is written by the running version and the migration
is paid once rather than on every restore.

`schema_version` and `min_readable_schema_version` are declared **per
tasklet** in the class declaration (`tasklets[].schema_version`), never
class-wide — a class-wide value re-stamps a Tasklet's records with a version
it never declared.

### Control command `CompactCell` (controller → runner)

`CompactCell { cell_id }` is answered with `CompactCellResponse { cell_id,
snapshot_seq, snapshot_bytes_size }` ([`runner_commands.proto`](../../../protobufs/runner_commands.proto)).

Issued when the Cell's un-snapshotted WAL exceeds its compaction trigger
(`FLEZHA_COMPACT_AFTER_BYTES`, default 8 MiB, overridden per class through
`AddRelayResponse.compact_after_bytes`), when its oldest
un-snapshotted record is older than `FLEZHA_COMPACT_AFTER_AGE_MS` (default
300000; `0` disables), or on `flz state rotate`. The runner drains the
Cell's pending changes, clones `State` under the Cell's sticky CAS,
serializes it, ships it as `cell_state_snapshot`, then returns
`CompactCellResponse` as `raw_data`. `snapshot_seq` MUST be the seq the
drained state stands at — the writer treats it as a truncation point.

### Control command `CellMailboxStatus` (controller → runner)

`CellMailboxStatus { cell_id, status }` carries `status` `"full"` or
`"open"` ([`runner_commands.proto`](../../../protobufs/runner_commands.proto)).

One-way back-pressure signal for sessions that are **not** credit-speaking
(an SDK that never acked `CreditWindowInit`, or a controller with
`FLEZHA_CREDIT_FLOW_CONTROL=false`): the `WalWriter` emits `"full"` when its
pending bytes reach `:wal_high_watermark_bytes` (default 16 MiB) and `"open"`
when they fall to `:wal_low_watermark_bytes`. A `flz_core` runner tolerates
and drops it — its send path is already bounded by the mailbox policies.

### Control command `CreditWindowInit` (controller → runner)

`CreditWindowInit { stream_id, window_bytes }` offers an opaque
credit-session id, fresh per attach, and the stream's un-fsynced byte budget;
the runner answers `CreditWindowAck { stream_id }` as `raw_data`, and the
controller then returns credit as `CreditGrant { stream_id, bytes }`
([`runner_commands.proto`](../../../protobufs/runner_commands.proto)). Field 3 of `CreditWindowInit` is reserved:
the low watermark grants are batched on is the controller's own.

Offered on the **first** `cell_state_change` frame of a stream attach, with
the window `:wal_high_watermark_bytes` (capped by the namespace's mailbox
quota while authorization is enforced; `:credit_low_watermark_bytes`, default
half the window). The runner arms the
window and returns `CreditWindowAck` echoing `stream_id` as `raw_data`; on
that ack the controller marks the session credit-speaking, stops sending
`CellMailboxStatus` and enforces the per-stream byte ledger. An SDK that
does not recognise the variant never acks; the offer times out after 10 s and
the session keeps the legacy behaviour. `FLEZHA_CREDIT_FLOW_CONTROL=false`
skips the offer entirely.

From the ack on, every `cell_state_change` frame debits its full wire size
before the write; an exhausted window parks the sending thread — park, never
drop (`credit_parked` in the heartbeat). `CreditGrant` is one-way and minted
only when appended bytes become durable: on WAL fsync the controller sends one
batched grant when the un-fsynced buffer falls below that low watermark —
never per record, and a stalled fsync mints nothing. Credit never crosses
sessions: a reconnect re-runs `CreditWindowInit` with a fresh `stream_id`, and
a grant for another `stream_id` is discarded. The runner applies
`CreditWindowInit` and `CreditGrant` as it reads them, ahead of any command
still running or queued, since a command can wait on a Cell whose sender is
parked on the credit that grant returns. A frame arriving on an exhausted
ledger is a protocol violation: the controller sheds it
(`credit_overrun_total`) and the seq-gap resync repairs the gap.

### Control command `ParkPayloadAck` (controller → runner)

`ParkPayloadAck { park_id }` echoes `ParkBody.park_id` ([`runner_commands.proto`](../../../protobufs/runner_commands.proto)).

One-way durability ack for a `park_payload` frame, sent only after the DLQ
entry is fsynced. On receipt the runner wakes the dispatch thread blocked in
the park, drops the payload and advances the Cell. A runner that never
receives it keeps the Cell held and re-sends the park — delay, never loss.

### Control command `AddRelay.task_journal` (controller → runner)

`AddRelay.task_journal` is a `TaskJournalState` ([`runner_commands.proto`](../../../protobufs/runner_commands.proto)):
`committed_task_idxs`, the tasks committed inside the in-flight iteration,
which are skipped; `iter`, the iteration to resume at; `handoff`, the previous
iteration's terminal batch, each `TaskHandoffItem` a channel id and the
channel codec bytes of one value; `committed_tasks`, every forward commit, for
compensation planning; `consumed_offset`, the input-log offset the last
committed iteration consumed (absent: none); `rewind_undo`, the tasks a rewind
unwound and nothing has compensated yet; `task_handoffs`, the handoff of each
committed task of the in-flight iteration; `completed` and `result`, set when
the last committed iteration ended the instance, with that result;
`entry_channel`, the chain the last committed iteration ran; and
`released_offset`, the highest offset released by a commit or an unread dead
letter. Fields 6 and 7 are reserved: where an instance sleeps is not the
journal's. Each `TaskCommittedTask` carries `task_idx`, `iter`, `cells_ran`,
`completes` and `result` (the tasklet's own `ctx.complete`) and `trigger`,
the timer firing whose pass the task was the root of.

Present only when the instance has a non-empty task-journal. A resumed instance
skips `committed_task_idxs`, re-seeds each uncommitted child branch from its
own parent's edge in `task_handoffs` (filtered to the channel the child
consumes — the latest commit's handoff alone would strand a sibling), re-seeds
iteration N+1 of a handoff-chained loop from `handoff`, and rebuilds the
compensation planner's input from `committed_tasks`. When `completed` is set
the instance ends the run at once with `result`, and a skipped tasklet of the
in-flight iteration whose `completes` is set supplies that iteration's
completion, since it never runs again to ask. A committed task of the
in-flight iteration that carries a `trigger` was the root of a pass a timer
firing opened: the instance resumes that tasklet's chain rather than waiting
for a message, and drops the named timer from its Cell's restored pending set
before re-arming, so a firing whose stage committed never runs twice.
`rewind_undo` lists the
tasklets a rewind unwound that nothing has compensated yet: the instance runs
them through the ordinary compensation planner (reverse iteration, reverse
chain position) before resuming forward at `iter`, each undo journaling a
`compensated` entry so the list shrinks across a crash. The rewind itself
never crosses the wire — the controller writes the `rewound` entry and rebinds
the instance.

### Control command `TaskCommit` (runner → controller)

`TaskCommit`, `CommittedCell`, `TimerTrigger` and `TaskCommitResponse` are
defined in [`runner_commands.proto`](../../../protobufs/runner_commands.proto). A `TaskCommit` names `relay_id`,
`task_idx` and `iter` (0 for the single-pass chain), the `cells_ran`, the
`handoff`, `compensating` (an undo record, not forward progress), `completes`
and `result` (the tasklet called `ctx.complete`, with this result) and, on the
root stage of a timer-fed pass only, the `trigger`: the `cell_key` of the
stage tasklet's Cell whose timer fired, whether it is an `event_time` timer,
and its `scheduled_at` timestamp. Field 6 is reserved. The reply
`TaskCommitResponse` carries `relay_id`, `task_idx` and an `error`, absent on
success.

Emitted at each tasklet barrier of a **durable structured** steps relay
instance: tasklet `task_idx` completed forward on `cells_ran` and released
the inter-task handoff batch, serialized through the producing channel's byte
codec, which the runtime derives and no SDK declares. The tasklet's outputs reach its children
only on `TaskCommitResponse` — the next tasklet must not race ahead of the committed record —
and nothing waits for it: the ack is an event that releases them. Classes kept in memory
(`durable: false`) skip the barrier and keep the direct
at-least-once wire path.

`cells_ran` names each Cell the stage ran, with the range `first_seq..seq` of
effect records it wrote in the stage (both absent when it wrote none) and the
plugin channels those records wrote on. The barrier carries no output: a
stage's plugin output is in its Cells' `cell_effect_record`s, each sent before
the barrier. The frame bound applies to the handoff; an oversized handoff is
refused with a non-empty `error`.

`compensating = true` marks the undo twin: `cells_ran` names the records a
`compensate` hook wrote for the already-committed tasklet `task_idx`,
journaled as a `compensated` entry under the same wait and release as a
forward stage. It is shipped BEFORE that Cell's `compensation_done` marker, so
a crash between the two re-runs the (idempotent) hook rather than losing its
undo output.

**Reply and durability.** Before it appends, the controller waits until every
record `cells_ran` names is durable on its Cell's writer; a record the writer
refuses, or one not durable within `FLEZHA_STEPS_ACK_TIMEOUT_MS`, fails the
barrier with `record_not_durable: <cell> seq=<n>`, and a re-sent barrier whose
first copy still waits joins that wait. `TaskCommitResponse` rides a
controller-initiated `CommandRequest` under the request's `reference_id`,
emitted only AFTER the `task_committed` entry is fsynced in the instance's
`task.journal` (records, then journal, then ack). The fsync is batched by the `FsyncBatcher` tick and the
acks of every barrier in the tick — across every instance on the runner — are
released after that one sync. The control-channel reader never journals: it
hands the frame to the per-runner commit worker (`ControlChannel.Commits`), so
a commit storm never queues a ping, stop, park or compaction behind disk I/O.
A non-empty `error` (journal write failure, oversized payload, a record not
durable) terminates the run abnormally through the terminal-error path, including saga compensation.

**Idempotent re-send.** An ack that does not arrive within
`FLEZHA_STEPS_ACK_TIMEOUT_MS` (default 30 s) of a send is re-sent: the same
frame goes out under the **same `reference_id`**, up to three sends in all, a constant of the connection that no
class sets. The
controller journals a barrier once by its identity `{task_idx, iter, epoch}` —
the journal writer refuses an append whose 16-byte record header
(`[epoch u32][type u8][task_idx u16][iter u32]`, zero-padded) is already in the
file (`dedup: true`) — and answers a re-send from the existing entry. Only the
exhausted ladder is mid-run channel loss: the run terminates and crash-resume
re-drives from the last committed task (the un-acked task re-runs —
at-least-once).

**Release.** The response is the durable point past which the named records'
outputs may be seen: right after the ack the controller tells each named
Cell's writer that its range is committed, and the writer's outbox, the same
one a tier flow relay Cell uses, dispatches those outputs through the
`:on_output` chain. A held entry of that Cell below the range belongs to an
attempt that never committed and is dropped. On restart the writer re-queues
every record above its flush cursor and releases what a live journal entry
names; a crash between commit and flush duplicates at worst (at-least-once
with stable keys) and loses nothing.

**Steps output identity.** Every released frame carries the identity a tier
stream's output carries, minted by the Cell's writer from the effect record:

| Key | Value |
|-----|-------|
| `flz.cell` | `<instance_id>/<tasklet_name>/<cell_key>` of the producing Cell |
| `flz.seq` | `<record seq>.<emit ordinal>`; an undo output is a newer record of the compensating Cell, so its seq sorts above the forward output's |
| `flz.epoch` | the Cell's realign epoch, carried verbatim across crash re-drives; a rewind does not move it (a rewind bumps only the journal epoch, which keys barrier identity) |
| `channel` | the producing plugin channel's name |
| `flz.key` | the emit's key, when the channel is keyed |
| author metadata | every pair the emit supplied |

The tuple is a pure function of the record, so a re-drive replays
byte-identical stamps. An opted-out class writes no record: its outputs go
straight to the wire with `channel`, `flz.key` and metadata, as a tier-off
flow relay's do.

### Control command `IterCommit` (runner → controller)

`IterCommit`, `DeadLetter` and `IterCommitResponse` are defined in
[`runner_commands.proto`](../../../protobufs/runner_commands.proto). An `IterCommit` names `relay_id` and `iter`,
the `consumed_offset` (the `flz.offset` of the input the iteration consumed;
absent: none), the iteration's terminal `handoff`, `completes` and `result` (a
tasklet of the iteration called `ctx.complete`; the resolved result the
instance ends with), the `entry_channel` whose chain the iteration ran, and
`dead_letter`, set when the iteration's turn failed: the `tasklet` whose Cell
failed, its `attempts`, the author's `error` and the Cell's `cell_key` (empty
for a declared Cell). The reply `IterCommitResponse` carries `relay_id`,
`iter` and an `error`, absent on success.

Emitted at each **iteration** boundary of a looping durable structured
instance, after the last tasklet's `TaskCommit` is acked. A class opts into
looping with `each()`: `PerInput` runs one iteration per message,
`Chained` takes the first message and seeds each later iteration from the
previous terminal handoff into the chain that message started; the default
`Once` takes one message, runs one pass and never emits this frame. Every
message-fed pass takes its message from the inbox the same way, and every
iteration names in `entry_channel` the entry channel whose chain it ran. Every `TaskCommit` inside iteration `N` carries
`iter = N`, so redo-vs-skip works at both granularities from one scan.

The controller appends ONE `iter_committed(iter, consumed_offset, entry_channel)`
entry carrying the terminal handoff, fsyncs, and acks under the same batching,
terminal-error and bounded re-send contract as `TaskCommitResponse` (a re-sent
`IterCommit` is journaled once by `{iter, epoch}`). Everything after the ack is
cleanup: the report of `consumed_offset` into the input log's watermark
(whose retention then reclaims the prefix below it), and a cut of the
task-journal below the oldest entry its fold still needs (the latest
`iter_committed`, a stage entry naming a record above its Cell's flush cursor,
the latest `rewound` marker with its pending undos), deferred until the
settled prefix exceeds `:record_log_cut_after_bytes` (64 KiB) and skipped for
a class whose tasklets declare `compensate`. The instance enters iteration
`iter + 1` only on success, so a crash re-runs at most the in-flight
iteration.

`dead_letter` marks an iteration whose turn failed, in a `per_input` loop
declaring keep-progress. The
controller first parks the message at `consumed_offset`, read from the
instance's input log, under the `_turn` slot, then appends ONE
`dead_lettered` entry naming the DLQ entry instead of `iter_committed`, then
acks; a park that fails or does not answer in time is refused as
`commit.turn_not_parked` and nothing is journaled. After the ack the driver
undoes the turn's committed tasklets (one `TaskCommit` with `compensating`
set per tasklet, no done markers) and takes the next message as `iter + 1`.

### Control command `IterPark` (runner → controller)

`IterPark` ([`runner_commands.proto`](../../../protobufs/runner_commands.proto)) names `relay_id`, `iter` (the last
committed iteration; 0 for a flow relay), `wake_deadline_ms` (the earliest
pending processing-time timer; 0 wakes on input only) and `consumed_offset`
(the last message parked unread; absent: none).

An instance of either run whose class sleeps (`durable` `local` or `remote`
with a `sleep_after`) asks to sleep with this once it has been idle past its
class's `sleep_after` (by default never for a flow class, six hours for a steps class) with every Cell holding nothing
queued, in flight, retrying or unflushed. A pending timer keeps a flow relay awake; a steps relay sleeps with its
timers pending and names the earliest in `wake_deadline_ms`. A steps relay of any loop mode does so only at a **committed** iteration
boundary, the boundary before its first pass included, and emits it instead of
`RelayCompleted`; a flow relay sends `iter` `0`. Suspension is not a terminal
outcome. A rehydrated instance waits up to 60 s for the input-log tail the
controller owes it (`AddRelay.inbox_tail_count`) before it may sleep again.

A message the run parked to the dead-letter queue unread (an undeclared
channel, a parse the entry's ladder never got past, a host decoder's unreadable
verdict) is consumed without a pass; `consumed_offset` names the highest such
input-log offset. The controller journals it as released (an `input_released`
task-journal entry) before it records the park, as an `IterCommit` releases its
input, so a suspended instance is not rehydrated to receive it again, across a
restart too; the driver drops a re-forwarded message at or below the journal's
`released_offset`.

The controller writes the instance's park record (its wake deadline) beside
the class's declarations, durable BEFORE the eviction, then evicts the instance from its runner (`RemRelay`); the
bind that wakes it clears the record. The
registry entry and the controller-side Overwatch stay: it is the **runner**
that holds zero resources, and `flz relay info` reports the instance
`suspended`. Rehydration has one path, whichever wakes it — a new input-log
append, or the journaled deadline elapsing — and both re-bind the instance
through the normal spawn path on any live runner, restoring its Cells (and a
steps relay's journal) and delivering the input log's undelivered tail. The
deadline appends nothing: `wake_deadline_ms` is the earliest processing-time
timer pending on any of the instance's Cells when it parked, so the restored
Cell re-arms that timer and its firing opens the pass. An elapsed deadline
counts as pending work, like undelivered input, so a wake that found no live
runner is retried and a controller booting past it rehydrates at once. Two
racing wakes collapse into exactly one rehydration.

### Data frame metadata: the durable input log (`flz.offset`, `flz.msg_id`)

An instance has durable input when it is a flow relay on the effectively-once tier,
a durable steps relay, or an instance of a class that sleeps. Its input is appended to the instance's one input log
(`<state key>/input.log`) **before** it is forwarded, answered only once the
record is synced, and the forwarded frame carries the assigned offset as
`flz.offset`. Offsets are 0-based; absent means the input was not journaled (an
instance without durable input), never offset `0`. A flow relay threads it into
its Cells' effect records' `consumed_offset`; a steps relay into
`IterCommit.consumed_offset`, or `IterPark.consumed_offset` for a message it
parked unread, and an iteration that consumed no journaled input (a carry-fed
pass) carries none. A Cell drops an input stamped at or below the highest
offset it took, restored from its records and raised as it takes each forwarded
input, so a tail forwarded twice is taken once. The controller reports each
offset into the log's watermark, retention reclaims below the instance's floor,
and every bind re-forwards exactly the retained records above it, each with its
original `flz.offset`. A bind first re-delivers the inter-tasklet values the
Cells' records kept for inputs above the floor, each stamped `flz.edge`
(`"<channel>:<consumer>"`, the record's numbers) and `flz.offset` (its origin);
the runner hands such a frame to that one consumer, never to an entry channel.

### Data-stream payload-id `processed_prefix`

Runner → controller, on the data stream of a flow instance whose input
is journaled: a Cell frame whose identity names the instance (empty tasklet and
key), whose seq is the offset `P` and whose 16 header bytes are zero. It says
every input at or below `P` has been processed by every Cell it reached and
every record it produced is on this stream ahead of the marker. The body is
empty, or two bytes big-endian naming how many streams the runner sent the
same marker on when it spreads the instance's records over several; the
controller then counts `P` once each of them has. The runner never holds a
marker for a reconnect, and sends none covering a record it lost.

The controller confirms `P` once every Cell writer of the instance has fsynced
past the bytes this stream had handed it when the marker arrived; records an
earlier stream handed over are made durable by one flush the stream's first
marker asks for, and under `durable: remote` shipped, before any of its markers
counts. A confirmed `P` is the instance's floor.
Before one is confirmed, and after a controller restart, the floor is the
minimum durable consumed offset over the instance's reporters.

Dedup at append is keyed on the sender's `flz.msg_id` metadata only: an append
whose id the log still retains is absorbed and nothing is forwarded, so an
at-least-once sender cannot produce a duplicate input, and a message without an
id is always appended. The log keeps the index in memory, built from the file
on the first append that carries an id, so an append reads no file and folds no
journal.

### Plugin metadata: output identity stamps (`flz.*`)

Channels declared `.keyed()` (valid on `output` and `output` / `output_with`
wires only — a keyed channel without a plugin wire is a build error) stamp
every plugin-bound emit with three reserved keys, appended after user
metadata:

| Key | Value | Stability |
|-----|-------|-----------|
| `flz.cell` | `<relay_instance_id>/<tasklet_name>/<cell_key>` | replay-stable |
| `flz.seq` | `<change_seq>.<emit_ordinal>` — the Cell's durable change seq after the flush-before-emit drain, plus the emit's ordinal within the invocation | replay-stable while state advances |
| `flz.key` | the emitting Cell's own key, or the caller's natural key from `Ctx::emit_keyed(key, value)` | replay-stable by the determinism contract |

On a keyed channel a plain `emit` / `emit_with_meta` carries the emitting
Cell's own key — equal to the key its input routed by — and `emit_keyed` is
for the output whose natural key differs. A single-Cell Tasklet has no such
key and its plain emit fails with `runtime.key_required_on_keyed_channel`; on a non-keyed
channel `emit_keyed` fails with `runtime.key_not_allowed_on_non_keyed_channel` — the channel
declaration is the single source of truth for whether output identity exists.
The controller passes the stamps through the `:on_output` plugin chain
unchanged.

**`flz.seq` exactness boundary.** `flz.seq` suffices for changelog-style
consumers that upsert on `flz.key`. It is NOT a sufficient handle for exact
append-only dedup: consecutive invocations that apply no state change can
produce colliding `<change_seq>.<emit_ordinal>` pairs. Exact dedup needs a
durable outbox: the Cell writer's outbox stamps `flz.epoch` for the
effectively-once tier and for a durable steps relay alike, and `(flz.cell, flz.seq, flz.epoch)` is the identity a
conforming output deduplicates on (the Kafka producer uses `flz.key` as the
record key and ships `<cell>/<outbox_seq>/<epoch>` in its
`flezha.dedup_key` header; the Postgres output guards on `(epoch, seq)`). See
[`../../public/topics/core/durability.md`](../../public/topics/core/durability.md).

## See also

- [`poll_core.md`](poll_core.md) — the Activation/ActivationResult boundary inside a `flz_core` runner
- [`state_persistence.md`](state_persistence.md) — the per-SDK persistence trait surface (Rust and Java)
- [`../state_persistence.md`](../state_persistence.md) — the controller's on-disk layout, WAL writer and `flz state` CLI
- [`../../public/architecture/steps_profile.md`](../../public/architecture/steps_profile.md) — the steps relay barrier, journal and outbox model
