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
│       ▼                                                 │
│  FleetManager.connect_handler/3                         │
│       │                                                 │
│       ▼                                                 │
│  ComputeEngineSupervisor                                │
│    ├── ControlChannel ← {:flz_data, ...}               │
│    ├── StreamSup :primary                               │
│    │     └── StreamChannel(s) ← {:flz_data, ...}       │
│    └── StreamSup :secondary                             │
│          └── StreamChannel(s) ← {:flz_data, ...}       │
│                                                         │
│  ──── NIF Boundary ────────────────────────────────     │
│                                                         │
│  Flezha.Transport.Native (Rustler NIF module)           │
│    start_uds_server/2, stop_server/1                    │
│    send_data/2, send_frame/4                            │
│    set_active/1, set_passive/1, close/1                 │
│    controlling_process/2                                 │
└─────────────────────────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────┐
│              flz_nif (Rust cdylib)                      │
│                                                         │
│  NIF Functions ──crossbeam channel──▶ Driver Thread     │
│    (server.rs, stream.rs)             (uds_driver.rs)   │
│                                                         │
│  Driver Thread (mio event loop):                        │
│    ├── Token(0): UnixListener (accept new connections)  │
│    ├── Token(1): Waker (NIF command notification)       │
│    └── Token(N): Connected streams (per-stream read)    │
│                                                         │
│  Per-stream:                                            │
│    raw bytes ──▶ FrameBuffer ──▶ ParsedFrame            │
│    ParsedFrame ──enif_send──▶ {:flz_data, ...}         │
│                                                         │
│  Reuses from flz_transport:                             │
│    - payloader::build_from_binary (frame building)      │
│    - payloader metadata format (serialize/deserialize)  │
└─────────────────────────────────────────────────────────┘
```

## Crate Structure

```
flz_nif/
├── Cargo.toml           # cdylib, deps: flz_transport, rustler, crossbeam, mio
└── src/
    ├── lib.rs           # rustler::init!, resource registration, module declarations
    ├── atoms.rs         # Atoms: ok, error, flz_data, flz_closed, flz_connection
    ├── resources.rs     # ResourceArc types: TransportServer, StreamHandle
    ├── frame_buffer.rs  # Stateful frame parser (port of Elixir Payloader.parse_frame)
    ├── server.rs        # NIF functions: start_uds_server, stop_server
    ├── stream.rs        # NIF functions: send_data, send_frame, set_active, etc.
    └── uds_driver.rs    # mio-based UDS server driver thread
```

## Resource Types

### TransportServer

Opaque handle to a running transport server, held by Elixir as a NIF resource reference.

```rust
pub struct TransportServer {
    cmd_tx: Sender<ServerCommand>,    // Send commands to driver thread
    waker: Arc<Waker>,                // Wake driver's mio poll
    driver_handle: Mutex<Option<JoinHandle<()>>>,  // Driver thread handle
}
```

When dropped, sends `Shutdown` command to the driver thread and joins it.

### StreamHandle

Opaque handle to a single stream (connection) on the server.

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

Port of Elixir's `Payloader.parse_frame/3` state machine. Accumulates bytes and handles partial headers/payloads across TCP segment boundaries.

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

### Initialization

1. Driver thread creates its own `mio::Poll` and `mio::Waker`
2. Binds a `UnixListener` on the specified socket path
3. Sends `DriverReady { waker }` back to the NIF via a oneshot channel
4. Enters the event loop

### Event Loop

```
loop {
    poll(timeout: 50ms)

    // 1. Accept new connections from listener
    //    → Register stream with mio for READABLE
    //    → Create StreamHandle ResourceArc
    //    → enif_send {:flz_connection, stream_ref} to acceptor

    // 2. Read from readable streams
    //    → Feed bytes into per-stream FrameBuffer
    //    → For each complete frame: enif_send {:flz_data, stream_id, payload_id, message, metadata}
    //    → On EOF: enif_send {:flz_closed, stream_id, reason}

    // 3. Process commands from NIF (crossbeam channel)
    //    → SendData, SendFrame, SetActive, SetPassive, CloseStream, UpdateOwner, Shutdown
}
```

### Message Format (Rust → Elixir)

| Message | Meaning |
|---------|---------|
| `{:flz_connection, stream_ref}` | New connection accepted |
| `{:flz_data, stream_id, payload_id, message, metadata}` | Complete parsed frame |
| `{:flz_closed, stream_id, reason}` | Stream/connection closed |

Messages are sent via `OwnedEnv::send_and_clear()` (Rustler wrapper for `enif_send`).

### Active/Passive Mode

Each stream has an `active` flag:
- **Active** (default): parsed frames are immediately sent as Erlang messages
- **Passive**: frames are buffered in `passive_queue` until `set_active` is called, at which point all queued frames are flushed

## NIF Functions

### Server Lifecycle

| Function | Signature | Description |
|----------|-----------|-------------|
| `start_uds_server` | `(socket_path, acceptor_pid) → {:ok, server_ref}` | Start UDS server |
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

## Elixir Module Changes

### New Modules

| Module | Purpose |
|--------|---------|
| `Flezha.Transport.Native` | Rustler NIF module, implements `Transport.Behaviour` |
| `Flezha.Transport.Server` | GenServer managing NIF server lifecycle |

### Modified Modules

| Module | Changes |
|--------|---------|
| `Application` | `transport_children/0` returns single `Transport.Server` for all transports |
| `FleetManager` | Unified `connect_handler/3` replacing separate UDS/QUIC handlers |
| `ControlChannel` | Handles `{:flz_data, ...}` instead of `{:tcp, ...}` / `{:quic, ...}`. No buffer state. |
| `StreamChannel` | Handles `{:flz_data, ...}`. No UDS accept or QUIC stream_attached logic. |
| `ComputeEngineSupervisor` | Single `init/1` clause. No socket listeners or QuicStreamAcceptor. |
| `Resolver` | Always returns `Transport.Native` |
| `Payloader` | Removed `parse_frame/3` (moved to Rust FrameBuffer). Retains `build_from_bin/3`. |

### Modules to Delete (after full migration)

| Module | Reason |
|--------|--------|
| `Flezha.Transport.Quic` | Replaced by `Transport.Native` |
| `Flezha.Transport.QuicListener` | Replaced by NIF server |
| `Flezha.Transport.QuicStreamAcceptor` | Stream routing handled in NIF |
| `Flezha.Socket.Listener` | Replaced by NIF server |
| `Flezha.Socket.Dispatcher` | Replaced by NIF server |
| `Flezha.Transport.Uds` | Replaced by `Transport.Native` |

## Connection Lifecycle (UDS)

```
SDK Process                    NIF Driver Thread              Elixir Agent
    │                               │                              │
    │── connect(UDS) ──────────────▶│                              │
    │                               │── {:flz_connection, ref} ──▶│ Transport.Server
    │                               │                              │
    │── send(connect_control_ch) ──▶│                              │
    │                               │── {:flz_data, ...,           │
    │                               │   "connect_control_channel", │
    │                               │   message, metadata) ──────▶│ Transport.Server
    │                               │                              │
    │                               │                              │── FleetManager.connect_handler/3
    │                               │                              │   ├── start_compute_engine_sup
    │                               │                              │   ├── Native.controlling_process
    │                               │                              │   │   (→ ControlChannel now owns stream)
    │                               │                              │   ├── Native.set_active
    │                               │                              │   ├── command_assign_compute_engine_id
    │                               │                              │   └── command_sockets_connect
    │                               │                              │
    │◀─ AssignComputeEngineId ──────│◀─ Native.send_data ─────────│ ControlChannel
    │◀─ ConnectSocket ──────────────│◀─ Native.send_data ─────────│ ControlChannel
    │                               │                              │
    │── open data streams ─────────▶│                              │
    │   (new UDS connections)       │── {:flz_connection, ref} ──▶│ Transport.Server
    │                               │                              │   └── route to StreamChannel
```

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| NIF boundary | Transport + framing | Clean seam. NIF delivers parsed frames, Elixir handles protobuf |
| Crate location | Separate `flz_nif/` | Avoids conflating cdylib targets (C FFI vs Rustler) |
| Server logic | In `flz_nif`, not `flz_transport` | Server I/O model (enif_send) is fundamentally different from client |
| Driver threading | Single mio thread per server | Matches existing QuicClient pattern, proven in codebase |
| Message delivery | OwnedEnv + enif_send | Standard Rustler pattern for async message delivery to BEAM |
| Outbound framing | `send_frame` NIF builds frame in Rust | Avoids round-trip; one NIF call instead of build+send |
| Poll timeout | 50ms | Balance between latency and CPU usage for command processing |

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

1. NIF loading: `Flezha.Transport.Native` loads in `iex`
2. UDS: `mix test` — existing test suite with NIF transport
3. Cross-SDK: Rust SDK connects to agent via NIF server

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

### Build Artifacts

| Artifact | Location | Purpose |
|----------|----------|---------|
| `libflz_nif.so` | `priv/native/flz_nif.so` | NIF shared object loaded by BEAM |
| `libflz_transport.rlib` | `target/release/deps/` | Rust library linked into flz_nif |

### Known Warnings

- `non_local_definitions` in `flz_nif/src/lib.rs` — from `rustler::resource!` macro, harmless
- `:quicer` undefined warnings in old transport modules — these modules are pending deletion (Phase 3)
- `flz_transport` dead code warnings — FFI fields prepared for future server mode

## Dependency Graph

```
flz_nif (cdylib, Rustler NIF)
  └── flz_transport (rlib)
        ├── payloader (frame building/parsing)
        ├── protocol (stream type constants)
        └── config (env var reading)

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

## Future Work (Phase 2)

- `quic_driver.rs`: QUIC server driver using quinn-proto (mirrors UDS driver architecture)
- `start_quic_server/4` NIF function
- `{:flz_stream, conn_ref, stream_ref, type_byte, index}` message for QUIC data streams
- `Transport.Server` handles both QUIC and UDS based on config

## Future Work (Phase 3 — Cleanup)

- Delete dead modules: `Transport.Quic`, `QuicListener`, `QuicStreamAcceptor`, `Socket.Listener`, `Socket.Dispatcher`, `Transport.Uds`
- Remove all `:quicer` references
- Simplify `Resolver` (currently already returns `Transport.Native` only)
- Unify remaining `FleetManager` code paths
