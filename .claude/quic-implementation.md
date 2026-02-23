# QUIC Transport Implementation Reference

## Architecture Overview

Flezha uses a transport abstraction layer to support multiple communication backends:
- **Behaviour** (`lib/flezha/transport/behaviour.ex`) — Defines transport callbacks (`send_data`, `set_active`, `close`, `controlling_process`)
- **Resolver** (`lib/flezha/transport/resolver.ex`) — Runtime transport selection based on `FLEZHA_TRANSPORT` env var
- Transports: UDS (default, Unix domain sockets) or QUIC
- Supervision: `Application` → conditionally starts `QuicListener` when transport is `:quic`

## File Map

### Elixir (Agent-side)

| File | Purpose |
|------|---------|
| `lib/flezha/transport/quic.ex` | Transport behaviour impl — `send_data`, `set_active`, `close`, `controlling_process` via `:quicer` |
| `lib/flezha/transport/quic_listener.ex` | GenServer accept loop, TLS handshake, routes connections to FleetManager |
| `lib/flezha/transport/quic_stream_acceptor.ex` | Reads type byte from streams (0x01=primary, 0x02=secondary), routes to StreamChannels |
| `lib/flezha/transport/cert_generator.ex` | Self-signed CA + server cert generation (RSA 2048, 10yr validity) |
| `lib/flezha/transport/protocol.ex` | Stream type constants |
| `lib/flezha/transport/behaviour.ex` | Transport callback definitions |
| `lib/flezha/transport/resolver.ex` | Runtime transport selection |
| `lib/flezha/application.ex` | Conditional supervisor children based on transport |
| `lib/flezha/compute_engine/fleet_manager.ex` | `connect_handler_quic/2` entry point for QUIC connections |
| `lib/flezha/compute_engine/control_channel.ex` | Handles `{:quic, :data, stream, frame}` messages |
| `lib/flezha/compute_engine/stream_channel.ex` | Handles QUIC stream data and `{:quic_stream_attached, stream}` |

### Rust (SDK-side)

| File | Purpose |
|------|---------|
| `sdk/rust/src/transport/quic.rs` | `QuicClient` (quinn-based), `QuicBidiStream` with Read/Write traits |
| `sdk/rust/src/transport/mod.rs` | `FlezhaStream` enum (Unix \| Quic variants) |
| `sdk/rust/src/transport/protocol.rs` | Stream type constants (`STREAM_TYPE_PRIMARY=0x01`, `STREAM_TYPE_SECONDARY=0x02`) |
| `sdk/rust/src/transport/config.rs` | Transport configuration |
| `sdk/rust/src/command/streams_connect.rs` | `handle_quic()`: opens bidi streams, sends type byte, spawns read threads |
| `sdk/rust/src/compute_engine/control_unit.rs` | Control channel open/communication |

### Config & Tests

| File | Purpose |
|------|---------|
| `config/runtime.exs` | Transport env vars, cert paths, QUIC listen address |
| `test/quic_test.exs` | 5 integration tests (connect, create processor, e2e message, state, sync IO) |
| `scripts/bench.sh` | Bench script with `--transport quic` flag |

## Connection Lifecycle

1. Agent starts `QuicListener` on `0.0.0.0:4433` (ALPN: `"flezha"`, idle timeout: 30s)
2. SDK `QuicClient` connects (ALPN: `"flezha"`, keep-alive: 5s, CA cert verification)
3. First bidi stream becomes the **control channel**
4. SDK sends init message (namespace + factory_ids) on control stream
5. Agent's `FleetManager` creates compute engine supervisor
6. Agent sends `AssignComputeEngineId` command
7. Agent sends `ConnectSocket{sockets_count: N}` command
8. SDK opens 2×N bidi streams (N primary + N secondary), sends type byte on each
9. Agent's `QuicStreamAcceptor` reads type byte, routes to `StreamChannel`s
10. System ready for processor operations

## Stream Types & Data Flow

- **Primary (0x01):** Agent → SDK (processor input)
- **Secondary (0x02):** SDK → Agent (processor output)
- **Control:** Bidirectional protobuf commands

Binary framing (all streams): `[payload_id(32)] [payload_size(4)] [message_size(4)] [message] [metadata_size(2)] [metadata]`

## Key Implementation Details

- **Rust QUIC:** Blocking reads on tokio runtime (thread-per-stream model)
- **Elixir QUIC:** `:quicer` NIF (MsQuic) with active mode for message delivery
- QUIC messages arrive as `{:quic, :data, stream, frame}` vs UDS `{:tcp, port, frame}`
- Stream closure signal: `{:quic, :stream_closed, stream, flags}`
- Certs auto-generated at `app_data/certs/` (ca.pem, server.pem, server.key)

## Dependencies

- **Elixir:** `quicer ~> 0.2.15` (MsQuic NIF binding)
- **Rust:** `quinn 0.11`, `rustls 0.23`, `tokio 1`

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FLEZHA_TRANSPORT` | `"uds"` | Transport type: `"uds"` or `"quic"` |
| `FLEZHA_QUIC_ADDRESS` | `0.0.0.0:4433` (server) / `127.0.0.1:4433` (client) | Listen/connect address |
| `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` | CA certificate path |
| `FLEZHA_SERVER_CERT_PATH` | `app_data/certs/server.pem` | Server certificate path |
| `FLEZHA_SERVER_KEY_PATH` | `app_data/certs/server.key` | Server key path |

## Patterns & Differences from UDS

- UDS uses mio-based polling; QUIC uses thread-per-stream blocking reads
- UDS has separate socket listeners per stream; QUIC waits for `QuicStreamAcceptor` attachment
- `ControlChannel` handles both `{:tcp, ...}` and `{:quic, ...}` message formats via pattern matching
- `StreamChannel` handles both transport types with pattern matching
- QUIC streams share a single connection; UDS has independent socket pairs
