# QUIC Implementation — Diagnostic Report

**Date:** 2026-02-22
**Branch:** `quic-impl-1`
**Status:** 5/5 tests passing

---

## Test Command

The `FLEZHA_TRANSPORT=quic` env var is **required** — the test's `setup_all` guard raises without it.
`FLEZHA_CONCURRENT_CHANNEL_COUNT=1` prevents `runtime.exs` from defaulting to `schedulers_online`.

```bash
docker exec flezha_agent_dev bash -c \
  '. "$HOME/.cargo/env" && cd /flezha && \
  FLEZHA_TRANSPORT=quic FLEZHA_CONCURRENT_CHANNEL_COUNT=1 \
  mix test test/quic_test.exs'
```

Running `mix test test/quic_test.exs` **without** `FLEZHA_TRANSPORT=quic` will show:
```
RuntimeError: QUIC tests require FLEZHA_TRANSPORT=quic
5 tests, 0 failures, 5 invalid
```

This is intentional — the guard is in `test/quic_test.exs:18`.

---

## Architecture Overview

```
[Rust SDK (Quinn/rustls)] ──QUIC──> [Elixir Agent (quicer/MsQuic)]

SDK per connection (control_unit.rs):
  1. Opens QUIC connection to server (127.0.0.1:4433)
  2. Opens bidi stream #0 as control channel
  3. Sends "connect_control_channel" JSON: {namespace, factory_ids}
  4. Reads protobuf commands: AssignComputeEngineId, ConnectSocket, ...
  5. On ConnectSocket (streams_connect.rs):
     Opens N primary + N secondary bidi streams
     Each stream sends 1-byte type identifier: 0x01 (primary) or 0x02 (secondary)

Server per connection:
  1. QuicListener.handle_info({:quic, :new_conn, conn, _})
     ├── Starts TLS handshake (async_handshake)
     └── Spawns per-connection handler process BEFORE handshake completes
  2. Handler calls async_accept_stream → registers in MsQuic acceptor queue
  3. TLS handshake completes → SDK opens control stream → handler receives it
  4. Handler reads control payload via quicer.recv (passive mode)
  5. Handler calls FleetManager.connect_handler_quic(conn, stream)
  6. FleetManager starts supervisor tree (ComputeEngineSupervisor)
  7. QuicStreamAcceptor (child) spawns Tasks that accept N*2 data streams
  8. FleetManager sends AssignComputeEngineId + ConnectSocket commands to SDK
  9. SDK opens data streams → QuicStreamAcceptor routes them to StreamChannels
```

### Connection Lifecycle Sequence Diagram

```
   Rust SDK                     QuicListener              Handler Process           FleetManager
      │                              │                          │                       │
      │── QUIC connect ────────────>│                          │                       │
      │                              │ {:quic, :new_conn}      │                       │
      │                              │── async_handshake ──>   │                       │
      │                              │── spawn ────────────────>│                      │
      │                              │                          │ async_accept_stream   │
      │<── TLS handshake ──────────>│                          │   (NIF, ~µs)         │
      │                              │ {:quic, :connected}      │                       │
      │── open_bi (control) ───────>│                          │                       │
      │                              │          {:quic, :new_stream}                    │
      │                              │                          │<──────────────────────│
      │<── quicer.recv ────────────────────────────────────────>│                      │
      │                              │                          │── connect_handler ───>│
      │                              │                          │                       │ start supervisor
      │<── AssignComputeEngineId ──────────────────────────────────────────────────────│
      │<── ConnectSocket ──────────────────────────────────────────────────────────────│
      │── open_bi (primary 0x01) ──>│                          │                       │
      │── open_bi (secondary 0x02) >│                          │                       │
      │                              │   (QuicStreamAcceptor routes to StreamChannels)  │
```

---

## Process Ownership Model

Understanding **who owns what** is critical for QUIC debugging:

| Handle                | Initial Owner                  | Final Owner                  | Transfer Point                        |
|-----------------------|--------------------------------|------------------------------|---------------------------------------|
| Connection            | QuicListener (via async_accept)| QuicListener (unchanged)     | N/A                                   |
| Control stream        | Handler process (via async_accept_stream) | ControlChannel GenServer | `fleet_manager.ex:50` controlling_process |
| Data streams (N*2)    | QuicStreamAcceptor Tasks (via accept_stream) | StreamChannel GenServers | `quic_stream_acceptor.ex:95` controlling_process |

**Rules:**
- `quicer.recv(stream, N)` delivers to the **stream owner's** mailbox, not the caller's
- `controlling_process(stream, pid)` can only be called by the **current owner**
- Active mode messages (`{:quic, binary, stream, props}`) go to the **current owner**

---

## Elixir Side — File-by-File Reference

### `lib/flezha/transport/quic_listener.ex`
QUIC connection listener GenServer. Uses `async_accept` pattern to avoid double-accept issue.

**Key design: Pre-handshake stream acceptor registration.**
In `handle_info({:quic, :new_conn, ...})`, spawns a handler process that calls `async_accept_stream`
before the TLS handshake completes. This eliminates the orphan stream race condition (see Issue #9).

- `init/1`: Starts quicer, generates certs, opens listener on `0.0.0.0:4433`
- `handle_info({:quic, :new_conn, ...})`: async_handshake + spawn handler + re-accept
- `handle_info({:quic, :connected, ...})`: Removes from pending set (informational only)
- `handle_info({:quic, :new_stream, ...})`: Orphan stream fallback (should not fire in normal operation)
- `handle_connection/1`: Runs in spawned process. Calls `async_accept_stream`, waits for control stream, delegates to FleetManager

**Listen options:**
```elixir
%{
  certfile: ~c"app_data/certs/server.pem",
  keyfile: ~c"app_data/certs/server.key",
  alpn: [~c"flezha"],
  peer_bidi_stream_count: 100,  # Must be >= 1 + 2*N (control + data streams)
  peer_unidi_stream_count: 10,
  idle_timeout_ms: 30_000
}
```

### `lib/flezha/compute_engine/fleet_manager.ex`
Connection setup orchestrator. Two code paths: UDS (`compute_engine_connect`) and QUIC (`compute_engine_connect_quic`).

**QUIC-specific functions:**
- `connect_handler_quic/2` (line 32): Called from handler process (stream owner). Reads control info via `quicer.recv`, calls GenServer for supervisor setup, transfers stream ownership to ControlChannel, sends setup commands.
- `extract_control_info_quic/1` (line 520): Reads and parses the `connect_control_channel` payload from the control stream using `quicer.recv(stream, 0)`.

**Critical ordering in `connect_handler_quic`:**
1. `extract_control_info_quic(stream)` — must run in stream owner (handler process)
2. `GenServer.call(FleetManager, {:compute_engine_connect_quic, args})` — starts supervisor tree
3. `controlling_process(stream, control_channel_pid)` — transfers ownership
4. `setopt(stream, :active, true)` — enables active mode AFTER ownership transfer
5. `GenServer.call(control_unit, :command_assign_compute_engine_id)` — sends to SDK
6. `GenServer.call(control_unit, :command_sockets_connect)` — triggers data stream opening

### `lib/flezha/compute_engine/control_channel.ex`
Control stream GenServer. Handles bidirectional protobuf command/response communication.

**QUIC adaptations:**
- `init/1` (line 84): Skips `set_active` for QUIC (stream not owned yet)
- `handle_info({:quic, data, ...}) when is_binary(data)` (line 141): QUIC data reception
- `handle_info({:quic, :stream_closed, ...})` (line 146): QUIC stream close event

### `lib/flezha/compute_engine/stream_channel.ex`
Data stream GenServer. Handles primary (agent→SDK) and secondary (SDK→agent) I/O streams.

**QUIC adaptations:**
- `init/1` (line 18): In QUIC mode, starts with `sock: nil` (no socket listeners)
- `handle_call({:quic_stream_attached, stream})` (line 99): QuicStreamAcceptor attaches the stream
- `handle_info({:quic, data, ...}) when is_binary(data)` (line 120): QUIC data reception
- `handle_info({:quic, :stream_closed, ...})` (line 125): Sets sock to nil

### `lib/flezha/transport/quic_stream_acceptor.ex`
Accepts QUIC data streams after ConnectSocket command. Spawns N*2 concurrent Tasks.

**Flow per Task:**
1. `accept_stream(conn, %{active: false}, 30_000)` — blocks until SDK opens a stream
2. `quicer.recv(stream, 1)` — reads 1 byte: `0x01` (primary) or `0x02` (secondary)
3. Looks up the StreamChannel GenServer by name
4. `controlling_process(stream, channel_pid)` — transfers ownership
5. `setopt(stream, :active, true)` — enables active mode
6. `GenServer.call(pid, {:quic_stream_attached, stream})` — attaches to channel

**Why no race condition here:** QuicStreamAcceptor is a child of ComputeEngineSupervisor. It starts during `FleetManager.compute_engine_connect_quic`. The SDK doesn't open data streams until it receives the `ConnectSocket` command, which is sent AFTER the supervisor tree is fully started.

**Limitation:** Currently hardcodes `index = 1` for stream routing (line 82). Only supports `concurrent_channel_count = 1`.

### `lib/flezha/compute_engine/compute_engine_supervisor.ex`
Per-engine supervisor with two `init/3` clauses: QUIC and UDS.

**QUIC children (line 19):**
1. ControlUnit — command processing
2. ControlChannel — control stream I/O (starts with control_stream handle)
3. Primary StreamSup — manages primary StreamChannel workers
4. Secondary StreamSup — manages secondary StreamChannel workers
5. QuicStreamAcceptor — accepts data streams from SDK

**UDS children (line 51):** Same minus QuicStreamAcceptor, plus socket Listeners for primary/secondary.

### `lib/flezha/service/port_pilot.ex`
Manages external Rust SDK processes via `Port.open`.

- `@port_down_counter 10` — allows up to 10 restarts per 30s window
- `open_port/1` (line 12): Passes env vars `FLEZHA_SOCKET_FILES_PATH`, `FLEZHA_TRANSPORT`, `FLEZHA_CA_CERT_PATH`

### `lib/flezha/transport/cert_generator.ex`
Auto-generates self-signed TLS certificates for QUIC.

- Creates CA cert + server cert signed by CA + server private key
- Stored at `app_data/certs/{ca.pem, server.pem, server.key}`
- Server cert has SAN for `localhost` and `127.0.0.1`
- 10-year validity, RSA 2048-bit keys

### `lib/flezha/transport/quic.ex`
Transport behaviour implementation for QUIC. Delegates to `quicer` functions.

### `lib/flezha/transport/protocol.ex`
Stream type constants: `0x01` (primary), `0x02` (secondary).

### `lib/flezha/transport/resolver.ex`
Reads `Application.get_env(:flezha, :transport)` and returns the transport module.

---

## Rust SDK Side — File-by-File Reference

### `sdk/rust/src/transport/quic.rs`
QUIC client using Quinn (built on rustls).

- `QuicClient::connect(address, ca_cert_path)`: Creates tokio runtime, loads CA cert, configures ALPN `"flezha"`, connects to server
- `QuicClient::open_bidi_stream()`: Opens a bidirectional stream, returns `QuicBidiStream`
- `QuicBidiStream`: Wraps `Arc<Mutex<SendStream>>` + `Arc<Mutex<RecvStream>>` + `Arc<Runtime>`
- Implements `std::io::Read` and `std::io::Write` by blocking on the tokio runtime
- Also implements `Read`/`Write` for `&QuicBidiStream` (needed for `Arc<FlezhaStream>` usage)
- `try_clone()`: Clones by sharing Arc references (mutex-protected)

### `sdk/rust/src/transport/config.rs`
Transport configuration from environment variables.

- `FLEZHA_TRANSPORT`: `"quic"` → `TransportType::Quic`, else `TransportType::Uds`
- `FLEZHA_QUIC_ADDRESS`: SDK connect address, default `"127.0.0.1:4433"`
- `FLEZHA_CA_CERT_PATH`: CA cert for TLS verification, default `"app_data/certs/ca.pem"`

**Note:** The server listens on `0.0.0.0:4433` (from `FLEZHA_QUIC_ADDRESS` in runtime.exs), but the SDK connects to `127.0.0.1:4433` (from the Rust default). Do NOT set `FLEZHA_QUIC_ADDRESS` for SDK processes — they need the loopback address.

### `sdk/rust/src/transport/protocol.rs`
Stream type constants matching the Elixir side:
- `STREAM_TYPE_PRIMARY = 0x01`
- `STREAM_TYPE_SECONDARY = 0x02`

### `sdk/rust/src/compute_engine/control_unit.rs`
Opens the control channel connection. In QUIC mode (`open_quic`):
1. `QuicClient::connect(address, ca_cert_path)` — establishes QUIC connection
2. `client.open_bidi_stream()` — opens first bidi stream as control channel
3. Wraps as `FlezhaStream::Quic(stream)` for unified interface
4. Sends `{"namespace": "...", "factory_ids": [...]}` as `connect_control_channel` payload
5. Returns `(handle, stream, Some(client))` — client is kept for data stream opening

### `sdk/rust/src/command/streams_connect.rs`
Handles the `ConnectSocket` command. In QUIC mode (`handle_quic`):
1. For each socket (0..sockets_count):
   - Opens primary bidi stream, writes `0x01` type byte
   - Opens secondary bidi stream, writes `0x02` type byte
   - Stores secondary in `io_streams` (for SDK writes → agent reads)
   - Collects primary streams for listening thread
2. Spawns one thread per primary stream for blocking reads
3. Each reader thread calls `payloader::extract_payload` in a loop

---

## Configuration

### Environment Variables

| Variable | Server Default | SDK Default | Purpose |
|----------|---------------|-------------|---------|
| `FLEZHA_TRANSPORT` | `"uds"` | `"uds"` | Transport selection |
| `FLEZHA_QUIC_ADDRESS` | `"0.0.0.0:4433"` | `"127.0.0.1:4433"` | Listen/connect address |
| `FLEZHA_CA_CERT_PATH` | `"app_data/certs/ca.pem"` | `"app_data/certs/ca.pem"` | CA cert for TLS |
| `FLEZHA_CONCURRENT_CHANNEL_COUNT` | `"0"` (→schedulers_online) | N/A | Streams per connection |
| `FLEZHA_SERVER_CERT_PATH` | `"app_data/certs/server.pem"` | N/A | Server TLS cert |
| `FLEZHA_SERVER_KEY_PATH` | `"app_data/certs/server.key"` | N/A | Server TLS key |

### Config Priority (Elixir)

`runtime.exs` env vars override `config/test.exs` static values. For tests, always pass env vars:
```bash
FLEZHA_TRANSPORT=quic FLEZHA_CONCURRENT_CHANNEL_COUNT=1 mix test test/quic_test.exs
```

---

## All Issues Fixed (Chronological)

### 1. quicer expects maps, not keyword lists
**Symptom:** Dialyzer type errors
**Fix:** Changed all quicer option args from `[key: val]` to `%{key: val}`
**Files:** `quic_listener.ex`, `quic_stream_acceptor.ex`

### 2. Wrong QUIC message format in active mode
**Symptom:** ControlChannel and StreamChannel never received data from SDK
**Root cause:** Handlers matched `{:quic, :data, stream, frame}` but quicer sends `{:quic, binary_data, stream, props}` where `binary_data` is the actual bytes
**Fix:** Changed patterns to `{:quic, data, _stream, _props} when is_binary(data)`
**Files:** `control_channel.ex`, `stream_channel.ex`

### 3. `controlling_process` returns `{:error, :not_owner}`
**Symptom:** Stream ownership transfer fails, `send_data` returns `{:error, :closed}`
**Root cause:** `controlling_process` can only be called by the current stream owner. The Task that called `accept_stream` is the owner, not FleetManager.
**Fix:** Moved `controlling_process` call to `connect_handler_quic` (runs in the Task/handler that owns the stream)
**Files:** `fleet_manager.ex`

### 4. `set_active` called before `controlling_process` in QUIC mode
**Symptom:** ControlChannel calls `set_active` in init, but it doesn't own the stream yet
**Fix:** Skip `set_active` in ControlChannel init for QUIC; FleetManager calls it after ownership transfer
**Files:** `control_channel.ex`, `fleet_manager.ex`

### 5. `quicer.recv` ownership race
**Symptom:** `extract_control_info_quic` blocks forever in FleetManager's `handle_call`
**Root cause:** `quicer.recv` does a `receive` in the calling process's mailbox. When called from FleetManager, but the stream owner is the Task, messages go to the Task's mailbox.
**Fix:** Moved `extract_control_info_quic` to `connect_handler_quic` (handler process side, where the stream owner runs)
**Files:** `fleet_manager.ex`

### 6. `peer_bidi_stream_count` defaults to 1
**Symptom:** Only control stream succeeds; data stream opens fail
**Root cause:** quicer defaults `peer_bidi_stream_count` to 1 (confirmed in `deps/quicer/src/quicer.erl:1377`)
**Fix:** Set `peer_bidi_stream_count: 100` in `listen_opts`
**Files:** `quic_listener.ex`

### 7. `runtime.exs` overrides `test.exs` config
**Symptom:** `concurrent_channel_count` set to 1 in test.exs but runtime.exs reads env var with default `"0"` → `schedulers_online` (8)
**Fix:** Pass `FLEZHA_CONCURRENT_CHANNEL_COUNT=1` as env var in test command
**Files:** N/A (env var)

### 8. PortPilot missing QUIC env vars
**Symptom:** Rust SDK processes don't know to use QUIC transport
**Fix:** Pass `FLEZHA_TRANSPORT` and `FLEZHA_CA_CERT_PATH` in `open_port` env
**Files:** `port_pilot.ex`

### 9. Stream acceptor race condition (THE CRITICAL BUG)
**Symptom:** Second SDK connection's control stream always times out (20s). Only 1 of 2 connections succeeds. Sometimes neither succeeds.

**Root cause (confirmed via quicer C source `deps/quicer/c_src/quicer_connection.c:1355-1376`):**

When the SDK opens a bidi stream, MsQuic fires `PEER_STREAM_STARTED`. The quicer NIF handler:

```c
ACCEPTOR *acc = AcceptorDequeue(c_ctx->acceptor_queue);
if (!acc) {
    // No acceptor registered — fallback to connection owner
    is_orphan = TRUE;
    acc = AcceptorAlloc();
    CxPlatCopyMemory(acc, c_ctx->owner, sizeof(ACCEPTOR));
    acc->active = ACCEPTOR_RECV_MODE_PASSIVE;
}
// Send {quic, new_stream, Stream, #{is_orphan => true/false}} to acc->Pid
```

**Key behavior:** When no stream acceptor is registered, the stream is NOT rejected. Instead, it's delivered to the **connection owner** (QuicListener GenServer) as an "orphan" stream with `is_orphan: true` in props.

**The race:**
1. MsQuic completes TLS handshake → fires CONNECTED callback → sends `{quic, :connected, conn, _}` to QuicListener → callback returns
2. MsQuic immediately processes next event: SDK's control stream → fires PEER_STREAM_STARTED
3. No stream acceptor registered (the Erlang Task hasn't been spawned yet)
4. Stream delivered to QuicListener as orphan → `{quic, :new_stream, stream, #{is_orphan: true}}`
5. QuicListener's catch-all handler discards it
6. Task eventually calls `accept_stream` → no stream arrives → 20s timeout

**Why non-deterministic:** On loopback, MsQuic events are serialized per connection. After CONNECTED returns, PEER_STREAM_STARTED fires within microseconds. The Erlang GenServer hasn't had time to process `:connected`, spawn the Task, and have the Task call `accept_stream`.

**Fix:** Spawn a per-connection handler process during `:new_conn` (before handshake completes). The handler calls `async_accept_stream` to register itself as the stream acceptor. Since TLS handshake takes ~1-10ms but `async_accept_stream` is a NIF call (~microseconds), the acceptor is guaranteed to be registered before the stream arrives.

```elixir
# In handle_info({:quic, :new_conn, conn, _}), after async_handshake:
spawn(fn ->
  :quicer.async_accept_stream(conn, %{active: false})
  receive do
    {:quic, :new_stream, stream, _props} ->
      FleetManager.connect_handler_quic(conn, stream)
  after
    20_000 -> :quicer.close_connection(conn)
  end
end)
```

Each handler process is independent and only receives streams from its own connection (because `async_accept_stream` registers on the per-connection acceptor queue).

---

## Key quicer API Behaviors

1. **`accept_stream(conn, opts, timeout)`** — Calls `async_accept_stream` then blocks on `receive {quic, new_stream, ...}`. The calling process becomes the stream owner.

2. **`async_accept_stream(conn, opts)`** — Registers the calling process as a stream acceptor on the connection's acceptor queue. Returns `{ok, conn}`. This is a NIF call (~microseconds).

3. **`controlling_process(handle, pid)`** — Works for both connection and stream handles. Only the current owner can call it. Changes the owner and updates the NIF monitor.

4. **`quicer.recv(stream, N)`** — Reads N bytes (0 = read available). Does a `receive` in the **calling process's mailbox**. The stream must be in passive mode. Only works if the caller IS the stream owner (messages route to owner's mailbox).

5. **Orphan streams** — When `PEER_STREAM_STARTED` fires and no acceptor is queued, the stream is delivered to the connection owner with `is_orphan: true` in props. The stream is set to passive mode.

6. **Message format** — Active mode: `{quic, binary_data, stream_handle, props}`. Stream events: `{quic, :stream_closed, stream, flags}`, `{quic, :send_shutdown_complete, stream, status}`.

7. **Connection events** — `{quic, :new_conn, conn, props}`, `{quic, :connected, conn, props}`, `{quic, :transport_shutdown, conn, props}`, `{quic, :closed, conn, flags}`.

---

## Known Limitations

1. **`concurrent_channel_count > 1` not supported for QUIC.** `QuicStreamAcceptor.route_to_channel` hardcodes `index = 1` (line 82). Supporting multiple channels requires tracking which streams have been assigned per type.

2. **SDK debug logging.** `streams_connect.rs` has `eprintln!` debug statements (lines 94-124) that should be removed or guarded behind a flag for production.

3. **Dialyzer warning in `fleet_manager.ex:206`.** Pattern `{:error, reason}` can never match the return type of `compute_engine_connect_quic`. The function always returns `{:ok, namespace, compute_engine_id}` — no error path exists in the QUIC version.
