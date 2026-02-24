# NIF Transport Architecture (`flz_nif`)

## Overview

The `flz_nif` crate is a Rustler NIF that wraps the `flz_transport` shared library, providing server-side transport to the Elixir agent. It replaces both `:quicer` (MsQuic NIF for QUIC) and `:gen_tcp` (for UDS), unifying all transport behind a single NIF boundary.

**NIF boundary**: Rust handles transport I/O + binary frame parsing. The NIF delivers complete `{payload_id, message, metadata}` tuples to Elixir. Elixir handles protobuf decoding and command dispatch.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│              Elixir Agent (BEAM)                        │
│                                                         │
│  Transport.Server ← {:flz_connection, stream_ref}      │
│       │            ← {:flz_data, id, pid, msg, meta}   │
│       │            ← {:flz_stream, ref, type, index}   │
│       ▼              (QUIC data streams only)           │
│  FleetManager.connect_handler/3                         │
│       │                                                 │
│       ▼                                                 │
│  ComputeEngineSupervisor                                │
│    ├── ControlChannel ← {:flz_data, ...}               │
│    ├── StreamSup :primary                               │
│    │     └── StreamChannel(s) ← {:flz_data, ...}       │
│    ├── StreamSup :secondary                             │
│    │     └── StreamChannel(s) ← {:flz_data, ...}       │
│    ├── StreamAcceptor :primary  (UDS only)              │
│    └── StreamAcceptor :secondary (UDS only)             │
│                                                         │
│  ──── NIF Boundary ────────────────────────────────     │
│                                                         │
│  Flezha.Transport.Native (Rustler NIF module)           │
│    start_uds_server/2, start_quic_server/4              │
│    stop_server/1                                        │
│    send_data/2, send_frame/4                            │
│    set_active/1, set_passive/1, close/1                 │
│    controlling_process/2                                 │
└─────────────────────────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────┐
│              flz_nif (Rust cdylib)                      │
│                                                         │
│  NIF Functions ──crossbeam channel──▶ Driver Thread(s)  │
│    (server.rs, stream.rs)            (uds_driver.rs     │
│                                       quic_driver.rs)   │
│                                                         │
│  UDS Driver (mio event loop):                           │
│    ├── Token(0): UnixListener (accept connections)      │
│    ├── Token(1): Waker (NIF commands)                   │
│    └── Token(N): Connected streams (per-stream read)    │
│                                                         │
│  QUIC Driver (mio event loop):                          │
│    ├── Token(0): UDP socket (QUIC datagrams)            │
│    ├── Token(1): Waker (NIF commands)                   │
│    └── quinn-proto (connection + stream state machine)  │
│                                                         │
│  Per-stream:                                            │
│    raw bytes ──▶ FrameBuffer ──▶ ParsedFrame            │
│    ParsedFrame ──enif_send──▶ {:flz_data, ...}         │
│                                                         │
│  Reuses from flz_transport:                             │
│    - payloader::build_from_binary (frame building)      │
│    - payloader metadata format (serialize/deserialize)  │
│    - quinn_proto (QUIC protocol state machine)          │
└─────────────────────────────────────────────────────────┘
```

## Crate Structure

```
flz_nif/
├── Cargo.toml           # cdylib, deps: flz_transport, rustler, crossbeam, mio, quinn-proto, rustls
└── src/
    ├── lib.rs           # rustler::init!, resource registration, module declarations
    ├── atoms.rs         # Atoms: ok, error, flz_data, flz_closed, flz_connection, flz_stream
    ├── resources.rs     # ResourceArc types: TransportServer, StreamHandle
    ├── frame_buffer.rs  # Stateful frame parser (port of Elixir Payloader.parse_frame)
    ├── server.rs        # NIF functions: start_uds_server, start_quic_server, stop_server
    ├── stream.rs        # NIF functions: send_data, send_frame, set_active, etc.
    ├── uds_driver.rs    # mio-based UDS server driver thread
    └── quic_driver.rs   # mio+quinn-proto QUIC server driver thread
```

## Resource Types

### TransportServer

Opaque handle to a running transport server (UDS or QUIC), held by Elixir as a NIF resource reference.

```rust
pub struct TransportServer {
    cmd_tx: Sender<ServerCommand>,    // Send commands to driver thread
    waker: Arc<Waker>,                // Wake driver's mio poll
    driver_handle: Mutex<Option<JoinHandle<()>>>,  // Driver thread handle
}
```

When dropped, sends `Shutdown` command to the driver thread and joins it.

### StreamHandle

Opaque handle to a single stream on the server. For UDS this represents a Unix socket connection. For QUIC this represents a single bidirectional QUIC stream.

```rust
pub struct StreamHandle {
    stream_id: u64,                   // Unique stream identifier
    cmd_tx: Sender<ServerCommand>,    // Send commands to driver thread
    waker: Arc<Waker>,                // Wake driver's mio poll
    owner: Arc<Mutex<LocalPid>>,      // Current owning Elixir process
}
```

When dropped, sends `CloseStream` command to the driver thread.

## FrameBuffer (frame_buffer.rs)

Port of Elixir's `Payloader.parse_frame/3` state machine. Accumulates bytes and handles partial headers/payloads across segment boundaries.

### States

- **Empty** — waiting for first byte of a new frame
- **PartialHeader** — have < 36 bytes of header (payload_id + payload_size)
- **PartialPayload** — have complete header, waiting for payload_size bytes

### API

```rust
let mut fb = FrameBuffer::new();
let frames: Vec<ParsedFrame> = fb.feed(incoming_bytes);
// frames contains 0+ complete frames
```

Each `ParsedFrame` contains:
- `payload_id: String` — trimmed 32-byte payload ID
- `message: Vec<u8>` — raw message bytes
- `metadata: HashMap<String, String>` — deserialized key-value pairs

### Frame Format

```
[payload_id (32 bytes, space-padded)]
[payload_size (4 bytes, big-endian u32)]
  └─ [message_size (4 bytes, big-endian u32)]
     [message (variable bytes)]
     [metadata_size (2 bytes, big-endian u16)]
     [metadata (variable bytes)]
```

Metadata format: `[key_len(1)] [val_len(1)] [key_bytes] [val_bytes] ...` (repeating pairs)

## UDS Driver Thread (uds_driver.rs)

Single background thread per server, driven by mio. Created when `start_uds_server/2` is called.

### Event Loop

```
loop {
    poll(timeout: 50ms)

    // 1. Accept new connections from listener
    //    → Register stream with mio for READABLE
    //    → Create StreamHandle ResourceArc
    //    → enif_send {:flz_connection, stream_ref} to acceptor

    // 2. Flush writable streams (backpressure relief)
    //    → Write buffered data when socket becomes writable

    // 3. Read from readable streams
    //    → Feed bytes into per-stream FrameBuffer
    //    → For each complete frame: enif_send {:flz_data, stream_id, payload_id, message, metadata}
    //    → On EOF: enif_send {:flz_closed, stream_id, reason}

    // 4. Process commands from NIF (crossbeam channel)
    //    → SendData, SendFrame, SetActive, SetPassive, CloseStream, UpdateOwner, Shutdown

    // 5. Flush pending writes for all streams
}
```

### Write Backpressure

Per-stream write buffering handles non-blocking socket backpressure:
- Data is appended to `write_buffer` and an immediate flush is attempted
- On `WouldBlock`, the stream is registered for `WRITABLE` events
- When writable, buffered data is flushed
- Once fully flushed, the stream reverts to `READABLE` only

## QUIC Driver Thread (quic_driver.rs)

Single background thread per QUIC server, driven by mio + quinn-proto. Created when `start_quic_server/4` is called.

### Architecture

The QUIC driver handles multiple concurrent connections, each with multiple bidirectional streams:

```
quinn-proto::Endpoint (stateless QUIC protocol handling)
    └── HashMap<ConnectionHandle, ConnectionState>
            └── HashMap<StreamId, QuicStreamState>
                    ├── FrameBuffer (per-stream frame parser)
                    ├── TypeByteState (data stream identification)
                    ├── write_buffer (backpressure handling)
                    └── active/passive mode
```

### TLS Configuration

- Server certificates loaded from PEM files (generated by `CertGenerator` in Elixir)
- ALPN protocol: `"flezha"`
- rustls ServerConfig with no client auth
- Transport config: keep_alive 5s, 8MB stream window, 16MB connection window, 128 max bidi streams

### Stream Type Detection (QUIC only)

In QUIC mode, a single connection carries both the control channel and data streams:

- **First bidi stream per connection** = control channel (no prefix bytes)
- **Subsequent bidi streams** = data streams (prefixed with `[type_byte, index_byte]`)

The driver tracks per-connection stream count and per-stream `TypeByteState`:
- `ControlChannel` — data goes directly to FrameBuffer
- `AwaitingPrefix` — accumulating the 2-byte prefix
- `DataStream { type, index }` — prefix consumed, data goes to FrameBuffer

When the prefix is read:
- Control streams: `{:flz_connection, stream_ref}` sent immediately
- Data streams: `{:flz_stream, stream_ref, type, index}` sent after prefix read

### Event Loop

```
loop {
    // Compute poll timeout from all active connection timers
    poll(timeout: min(connection_timeouts, 50ms))

    // 1. Handle connection timers
    // 2. Read UDP datagrams → route to quinn-proto endpoint
    //    → ConnectionEvent: route to connection
    //    → Response: send reply datagram
    //    → NewConnection: accept, create ConnectionState
    // 3. Pump endpoint ↔ connection events
    // 4. Poll connection events:
    //    → StreamEvent::Opened{Bi}: accept stream, create StreamHandle
    //    → StreamEvent::Readable: drain data → FrameBuffer → deliver frames
    //    → StreamEvent::Writable: flush write buffer
    //    → StreamEvent::Finished: close stream, notify owner
    //    → ConnectionLost: close all streams, notify owners
    // 5. Process NIF commands (same as UDS: SendData, SendFrame, etc.)
    // 6. Flush pending writes and outgoing packets
}
```

### Write Backpressure (QUIC)

Uses quinn-proto's flow control:
- Write attempt via `connection.send_stream(id).write(data)`
- On `WriteError::Blocked`, buffer data and wait for `StreamEvent::Writable`
- On writable, retry flushing buffered data

## Message Format (Rust → Elixir)

| Message | Meaning |
|---------|---------|
| `{:flz_connection, stream_ref}` | New connection accepted (UDS) or control channel stream (QUIC) |
| `{:flz_stream, stream_ref, type, index}` | New QUIC data stream identified (type: 1=primary, 2=secondary) |
| `{:flz_data, stream_id, payload_id, message, metadata}` | Complete parsed frame |
| `{:flz_closed, stream_id, reason}` | Stream/connection closed |

Messages are sent via `OwnedEnv::send_and_clear()` (Rustler wrapper for `enif_send`).

### Active/Passive Mode

Each stream has an `active` flag:
- **Active**: parsed frames are immediately sent as Erlang messages
- **Passive** (default): frames are buffered in `passive_queue` until `set_active` is called, at which point all queued frames are flushed

## NIF Functions

### Server Lifecycle

| Function | Signature | Description |
|----------|-----------|-------------|
| `start_uds_server` | `(socket_path, acceptor_pid) → {:ok, server_ref}` | Start UDS server |
| `start_quic_server` | `(bind_address, cert_path, key_path, acceptor_pid) → {:ok, server_ref}` | Start QUIC server |
| `stop_server` | `(server_ref) → :ok` | Stop server, close all streams |

### Stream Operations

| Function | Signature | Description |
|----------|-----------|-------------|
| `send_data` | `(stream_ref, binary) → :ok` | Send raw bytes |
| `send_frame` | `(stream_ref, payload_id, message, metadata) → :ok` | Build frame in Rust + send |
| `set_active` | `(stream_ref) → :ok` | Enable active mode |
| `set_passive` | `(stream_ref) → :ok` | Disable active mode |
| `close` | `(stream_ref) → :ok` | Close stream |
| `controlling_process` | `(stream_ref, new_owner_pid) → :ok` | Transfer ownership |

`send_frame/4` builds the binary frame via `payloader::build_from_binary` in Rust, avoiding a round-trip to Elixir for frame construction.

## Elixir Module Integration

### Transport.Server

GenServer managing the NIF server lifecycle. Starts UDS or QUIC based on `FLEZHA_TRANSPORT` config.

**UDS mode:**
- Calls `Native.start_uds_server(path, self())`
- Receives `{:flz_connection, stream_ref}` for each new SDK connection
- After control channel handshake, StreamAcceptors handle data stream routing

**QUIC mode:**
- Calls `CertGenerator.ensure_certs()` then `Native.start_quic_server(addr, cert, key, self())`
- Control channels arrive as `{:flz_connection, stream_ref}` (first bidi stream per connection)
- Data streams arrive as `{:flz_stream, stream_ref, type, index}` (subsequent bidi streams)
- Routes data streams to StreamChannels using stream type + index

### ComputeEngineSupervisor

**UDS mode:** Starts StreamAcceptors (primary + secondary) that create per-stream UDS sockets.
**QUIC mode:** No StreamAcceptors — data streams are routed by Transport.Server.

### StreamAcceptor (UDS only)

Starts a NIF UDS server on per-compute-engine stream socket paths and routes incoming SDK connections to StreamChannel processes in round-robin order.

## Connection Lifecycle

### UDS

```
SDK Process                    NIF UDS Driver                 Elixir Agent
    │                               │                              │
    │── connect(control.sock) ─────▶│                              │
    │                               │── {:flz_connection, ref} ──▶│ Transport.Server
    │── send(connect_control_ch) ──▶│                              │
    │                               │── {:flz_data, ...,           │
    │                               │   "connect_control_channel") │
    │                               │                         ────▶│ Transport.Server
    │                               │                              │── FleetManager
    │                               │                              │   └── start ComputeEngineSup
    │◀─ AssignComputeEngineId ──────│◀─ send_frame ────────────────│ ControlChannel
    │◀─ ConnectSocket ──────────────│◀─ send_frame ────────────────│ ControlChannel
    │                               │                              │
    │── connect(primary.sock) ─────▶│ StreamAcceptor UDS driver    │
    │                               │── route to StreamChannel ───▶│
```

### QUIC

```
SDK Process                    NIF QUIC Driver                Elixir Agent
    │                               │                              │
    │── QUIC connect ──────────────▶│                              │
    │   (UDP handshake via          │                              │
    │    quinn-proto)               │                              │
    │                               │                              │
    │── open bidi stream (control)─▶│                              │
    │                               │── {:flz_connection, ref} ──▶│ Transport.Server
    │── send(connect_control_ch) ──▶│                              │
    │                               │── {:flz_data, ...,           │
    │                               │   "connect_control_channel") │
    │                               │                         ────▶│ Transport.Server
    │                               │                              │── FleetManager
    │◀─ AssignComputeEngineId ──────│◀─ send_frame ────────────────│ ControlChannel
    │◀─ ConnectSocket ──────────────│◀─ send_frame ────────────────│ ControlChannel
    │                               │                              │
    │── open bidi stream (primary)─▶│                              │
    │── write [0x01, index] ───────▶│                              │
    │                               │── {:flz_stream, ref, 1, i)─▶│ Transport.Server
    │                               │                              │── route to StreamChannel
    │── open bidi stream (secondary)│                              │
    │── write [0x02, index] ───────▶│                              │
    │                               │── {:flz_stream, ref, 2, i)─▶│ Transport.Server
    │                               │                              │── route to StreamChannel
```

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| NIF boundary | Transport + framing | Clean seam. NIF delivers parsed frames, Elixir handles protobuf |
| Crate location | Separate `flz_nif/` | Avoids conflating cdylib targets (C FFI vs Rustler) |
| Server logic | In `flz_nif`, not `flz_transport` | Server I/O model (enif_send) is fundamentally different from client |
| Driver threading | Single mio thread per server | Matches existing QuicClient pattern, proven in codebase |
| QUIC library | quinn-proto (same as SDK client) | Consistent protocol handling, no second QUIC library needed |
| Stream type detection | 2-byte prefix consumed in Rust driver | Prevents FrameBuffer corruption from non-frame bytes |
| QUIC data routing | Transport.Server routes by type+index | Simpler than per-connection acceptors; matches existing UDS pattern |
| Message delivery | OwnedEnv + enif_send | Standard Rustler pattern for async message delivery to BEAM |
| Outbound framing | `send_frame` NIF builds frame in Rust | Avoids round-trip; one NIF call instead of build+send |
| Poll timeout | 50ms (UDS), dynamic (QUIC) | QUIC uses connection timer deadlines for lower latency |

## Testing

### Rust Unit Tests

```bash
cargo test -p flz_nif
```

Tests the FrameBuffer with:
- Single complete frame
- Partial header (< 36 bytes)
- Partial payload
- Multiple frames in one feed
- Empty feed
- Frames with metadata

### Integration

```bash
# UDS mode (default)
mix test

# QUIC mode
FLEZHA_TRANSPORT=quic mix test test/quic_test.exs

# Benchmarks
./scripts/bench.sh          # UDS
FLEZHA_TRANSPORT=quic ./scripts/bench.sh  # QUIC
```

## Building

### Prerequisites

- Rust toolchain (stable)
- Elixir ~> 1.17, Erlang/OTP 27+
- `rustler` Hex package (automatically compiles the NIF crate during `mix compile`)

### Build Commands

```bash
# Build Rust workspace (all crates including flz_nif)
cargo build --workspace

# Run Rust tests (FrameBuffer unit tests)
cargo test -p flz_nif

# Build Elixir + NIF (Rustler compiles flz_nif in release mode automatically)
mix deps.get
mix compile

# Run all workspace Rust tests
cargo test --workspace
```

## Dependency Graph

```
flz_nif (cdylib, Rustler NIF)
  ├── flz_transport (rlib)
  │     ├── payloader (frame building/parsing)
  │     ├── protocol (stream type constants)
  │     └── config (env var reading)
  ├── quinn-proto (QUIC protocol state machine)
  ├── rustls (TLS for QUIC)
  ├── mio (non-blocking I/O)
  └── crossbeam (NIF ↔ driver channels)

sdk/rust (Compute Engine)
  └── flz_transport (rlib)
        ├── quic (QuicClient, mio+quinn-proto)
        ├── payloader
        └── transport (FlezhaStream enum)

Elixir Agent
  └── flz_nif (via Rustler)
        └── Transport.Native (NIF stubs)
              └── Transport.Server (GenServer)
```

## Future Work (Phase 3 — Cleanup)

- Delete dead modules: `Transport.Quic`, `QuicListener`, `QuicStreamAcceptor`, `Socket.Listener`, `Socket.Dispatcher`, `Transport.Uds`
- Remove all `:quicer` references
- Simplify `Resolver` (currently already returns `Transport.Native` only)
- Unify remaining `FleetManager` code paths
