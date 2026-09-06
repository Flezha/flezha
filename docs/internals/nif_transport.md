# NIF transport

For contributors working on the controller's wire edge. The `flz_nif` crate is a Rustler NIF
that wraps the `flz_transport` library and gives the Elixir controller its UDS and QUIC servers,
plus the client entry points the standalone Elixir SDK uses. This page covers the NIF boundary,
the resource types, the frame parser, the two driver threads, the messages delivered to Elixir,
the NIF surface, and how `Flezha.Transport.*` and the runner process tree consume it. The byte
format itself is specified in [the protocol spec](../sdk/protocol_spec.md).

## The boundary

Rust owns transport I/O and binary framing; the NIF delivers complete
`{payload_id, message, metadata}` frames as Erlang messages; Elixir owns protobuf decoding and
command dispatch. The crate is loaded as `Flezha.Transport.Native`
(`use Rustler, otp_app: :flezha, crate: "flz_nif", path: "flz_nif"`; `RUSTLER_SKIP_COMPILATION=true`
compiles the module to `:not_loaded` stubs). `lib.rs` registers three resource types:
`TransportServer`, `StreamHandle` and `QuicClientConn`.

```text
Elixir                                          flz_nif (cdylib)
Transport.Server  <- {:flz_connection, handle}  NIF fns --crossbeam channel--> driver thread
                  <- {:flz_stream, ...} (QUIC)    server.rs / stream.rs        driver/mod.rs
                  <- {:flz_refused, reason}                              driver/uds.rs, driver/quic.rs
ControlChannel    <- {:flz_data, ...}           per stream: bytes -> FrameBuffer -> ParsedFrame
                  <- {:flz_closed, id, reason}  client.rs: reader + writer threads per stream
```

| File | Role |
|---|---|
| `lib.rs`, `atoms.rs` | `rustler::init!`, resource registration; atoms `ok error undefined flz_data flz_closed flz_connection flz_stream flz_refused` |
| `resources.rs` | `TransportServer`, `StreamHandle`, `StreamWaker` |
| `frame_buffer.rs` | stateful frame parser, limit-checked before allocation |
| `server.rs` | `start_uds_server`, `start_quic_server`, `stop_server`, `driver_stats` |
| `stream.rs` | per-stream NIFs (`send_frame`, `set_active`, `close`, `stream_id`, ...) |
| `driver_msg.rs` | the `{:flz_*, ...}` tuple builders the loop and the client send through |
| `driver_stats.rs` | the driver loop's self-reported liveness |
| `driver/mod.rs` | the one `DriverLoop<T: Transport>`, the `Transport` contract and the `CloseReason` vocabulary |
| `driver/uds.rs`, `driver/quic.rs` | one `impl Transport` per wire; one mio event-loop thread per server |
| `client.rs` | standalone-SDK client connects returning the same `StreamHandle` |
| `instance_claim.rs` | what one controller instance owns alone on a host: the socket directory it listens in, the state root it writes |

From `flz_transport` it reuses `service::payloader`, `transport::config` (`max_frame_bytes`,
`max_write_buffer_bytes`), `transport::liveness`, `transport::tls`, `transport::protocol` and `transport::quic`.

## Resources

```rust
pub struct TransportServer {            // one per running server
    cmd_tx: Sender<ServerCommand>,      // NIF -> driver commands
    waker: Arc<mio::Waker>,             // wakes the driver's poll
    driver_handle: Mutex<Option<JoinHandle<()>>>,
    stats: Arc<DriverStats>,            // the one self-report, whichever wire ran
}
pub struct StreamHandle {               // one per UDS connection / QUIC stream / client stream
    stream_id: u64,
    cmd_tx: Sender<ServerCommand>,
    waker: StreamWaker,                 // Mio(Arc<Waker>) for server streams, Channel for client
    owner: Arc<Mutex<LocalPid>>,        // current owning Elixir process
}
pub struct QuicClientConn { client: QuicClient }
```

Dropping a `TransportServer` sends `Shutdown` and joins the driver thread; dropping a
`StreamHandle` sends `CloseStream`. `StreamWaker` exists so one handle type serves the mio
drivers and the channel-driven client writer (`wake` is a no-op for `Channel`).

`ServerCommand` is the whole NIF-to-driver vocabulary: `SendData`, `SendFrame`, `SetActive`,
`SetPassive`, `CloseStream`, `UpdateOwner`, `Shutdown`.

## FrameBuffer

A frame is a 36-byte header (`payloader::HEADER_LEN`: 32-byte space-padded payload id + u32 BE
payload size) followed by the body: u32 BE message size, message bytes, u16 BE metadata size,
metadata as repeated `[key_len u8][val_len u8][key][value]`.

States: `Empty`, `PartialHeader` (< 36 bytes), `PartialPayload` (header known, waiting for
`payload_size` bytes) and `Poisoned`. `feed(&[u8]) -> Result<Vec<ParsedFrame>, FrameError>`
returns zero or more complete frames. A header whose declared size exceeds
`FLEZHA_MAX_FRAME_BYTES` (default 64 MiB) fails before any allocation and poisons the buffer:
every later feed repeats the error, there is no resync, and the driver closes the stream with
reason `"frame_too_large"`. `ParsedFrame` is `{payload_id: String, message: Vec<u8>,
metadata: HashMap<String, String>}`.

## The driver loop (`driver/mod.rs`)

One thread per server — `flz-uds-driver` or `flz-quic-driver` — running one
`DriverLoop<T: Transport>`. The loop owns every job that is the same whichever wire is under
it, and a transport is one `impl Transport` supplying wire operations only. A capability added
to the loop reaches every transport at once; a capability one transport has and another lacks
is a defect of the loop, not a backlog item for the transport that lacks it.

Each iteration: wait on the wire, stamp the liveness heartbeat, take the pass the wire found
(streams to read, to flush, and ones it ended), flush and read those, drain the NIF command
channel, flush whatever is still buffered, and publish the per-stream snapshot. The loop owns
the iteration clock, so no wire operation takes a timestamp of its own.

The six `ServerCommand` arms are the loop's; only `SendFrame` and `Shutdown` reach `T` at all.
Streams start **passive**: frames queue in `passive_queue` until `SetActive`, which flushes the
queue in order.

Write backpressure is per stream and lives in `StreamState::offer` / `StreamState::observe`:
the frame's reservation is released, the buffer is offered the bytes, and only a frame the
ceiling took counts as written. The buffer is capped by `FLEZHA_MAX_WRITE_BUFFER_BYTES`
(default 8 MiB): an offer that would exceed the cap on a non-empty buffer is refused and
increments `write_buffer_dropped` (an empty buffer always accepts, so a single oversized frame
is never dropped). Crossing a watermark sends `{:flz_send_gate, stream_id, raised?}` to the
owner.

A stream close is one `CloseReason`, spelled once and rendered once, so the same wire event
reads the same whichever transport observed it:

| reason | what it means | produced by |
|---|---|---|
| `connection_closed` | the peer ended the stream cleanly | every transport, and `client.rs` |
| `read_error` | a read failed | every transport, and `client.rs` |
| `write_error` | a write failed | every transport |
| `frame_too_large` | a header declared a payload over the limit | every transport, and `client.rs` |
| `stream_reset` | the peer reset the stream | QUIC only — a UDS peer cannot say it |
| `connection_lost: <e>` | the whole connection died | QUIC only — a UDS connection IS its stream |

`DriverStats` is the loop's self-report: lock-free `loop_iterations`, `last_loop_ms`,
`cmd_queue_depth`, plus a per-stream snapshot (`write_buffer_len`, `write_buffer_dropped`,
`frames_written`, `frames_read`, `last_write_ms`, `last_read_ms`) rebuilt once per iteration.
`driver_stats/1` returns it as a map with `now_ms` so Elixir can age the timestamps. One
producer means a field is present for one transport exactly when it is present for the other.
The `:flezha_transport` observability source reads it about once a second; the stall classifier
derives `:driver_loop_frozen` and `:nif_queue_backing_up` from it
(see [transport liveness](transport_liveness.md)).

`driver/conformance_tests.rs` runs one table of cases over both `impl Transport`s, so a
difference between the wires is a failing build. `test/flezha/transport/driver_parity_test.exs`
drives the same parity through the real NIF over a real socket.

## The UDS wire (`driver/uds.rs`)

Started by `start_uds_server/2`. mio tokens: 0 the `UnixListener`, 1 the waker, 2+ one per
accepted stream; the poll timeout is a flat 50 ms. Accepting registers the socket READABLE,
builds a `StreamHandle` and sends `{:flz_connection, handle}` to the acceptor pid. `WouldBlock`
on a write registers `READABLE | WRITABLE` and the buffer drains on the next writable event;
once empty the interest reverts to `READABLE`. A read of zero bytes is the peer's clean end.

The listening socket's parent directory is created 0700 and the socket itself 0600, because
directory traversal is the UDS authentication boundary: a local same-uid peer IS the node
owner.

## The QUIC wire (`driver/quic.rs`)

Started by `start_quic_server/5`, driving a `quinn_proto::Endpoint` over one UDP socket
(token 0; waker token 1). Per connection: `ConnectionState { connection, streams:
HashMap<StreamId, u64>, stream_count, control_nif_id }` — the stream map is a routing table
only, because a stream's state belongs to the loop. Per stream the wire keeps its connection
handle, its `StreamId` and its `TypeByteState`. The poll timeout is the nearest connection
timer, capped at 50 ms. One pass handles timers, routes datagrams through the endpoint
(`NewConnection` creates a `ConnectionState`) and pumps connection events: `Opened{Bi}` accepts
streams, `Readable` marks them readable, `Writable` marks them writable, and `ConnectionLost`
ends every stream on the connection with `connection_lost: <reason>`. Reading answers the
peer's clean end from `Chunks::next` — `Ok(None)` is the FIN with every byte read, and
`Err(Blocked)` is only "nothing more right now".

TLS (`build_quic_server_config/3`): server cert/key from PEM; a non-empty `client_ca_path`
installs a `WebPkiClientVerifier`, so every peer must present a certificate chaining to that
CA; an empty path disables client auth (the `FLEZHA_TRANSPORT_AUTH=off` opt-out). ALPN is
`flezha`. Transport config: keep-alive 5 s, `max_idle_timeout` =
`FLEZHA_CONTROL_IDLE_TIMEOUT_MS` (default 45 s, the same window UDS liveness uses), 8 MiB stream
receive window, 16 MiB connection receive window, 8 MiB send window, 128 concurrent bidi streams.
A connection that dies with a transport error before opening any stream is a refused handshake
(absent or untrusted client certificate) and is reported as `{:flz_refused, reason}`.

Stream typing: the **first** bidi stream of a connection is the control channel (no prefix;
`{:flz_connection, handle}` is sent immediately and its nif id becomes the connection's
`control_nif_id`). Every later stream starts in `AwaitingPrefix` and must send a 2-byte
`[type, index]` preamble (type 1 primary, 2 secondary); once consumed the driver sends
`{:flz_stream, handle, type, index, conn_id}` where `conn_id` is the control stream's id, and
the rest of the bytes go to the frame buffer. Writes go through quinn's flow control: on
`WriteError::Blocked` the data is buffered and retried on `Writable`. The QUIC write buffer is
bounded by the same send gate the UDS one is, because both go through the loop's one ledger.

## Client entry points (`client.rs`)

The standalone Elixir SDK dials the controller through three `DirtyIo` NIFs that return the
**same** `StreamHandle` resource the servers produce, so every per-stream NIF and the
`Flezha.Transport.Stream` seam already cover a client handle:

| NIF | Behaviour |
|---|---|
| `client_connect_uds(path, owner_pid)` | `UnixStream::connect` with retry (10 ms exponential, 500 ms cap, ~3 s deadline) |
| `client_connect_quic(address, ca_cert_path)` | `QuicClient::connect`; returns `{:ok, conn}` (`QuicClientConn`) |
| `client_open_stream(conn, type, index, owner_pid)` | opens a bidi stream; type 1/2 writes the `[type, index]` preamble, type 0 (control) writes none |

Each client stream runs a blocking reader thread (`payloader::extract_payload` -> `{:flz_data,
...}`; EOF `"connection_closed"`, over-limit `"frame_too_large"`, else `"read_error"`) and a
writer thread draining the handle's command channel (`SendFrame` via `build_from_binary_into`,
`SendData`, `CloseStream`). Client streams are active from birth and route by the shared
`owner`, so `SetActive`, `SetPassive` and `UpdateOwner` are inert for them; `controlling_process/2`
still works because it updates the shared `Arc<Mutex<LocalPid>>`. Client stream ids come from a
private counter and never mix with server ids.

## Messages delivered to Elixir

| Message | Meaning |
|---|---|
| `{:flz_connection, handle}` | UDS connection accepted, or QUIC control stream opened; sent to the acceptor pid |
| `{:flz_stream, handle, type, index, conn_id}` | QUIC data stream identified after its preamble (type 1 primary, 2 secondary); `conn_id` is the owning control stream's id |
| `{:flz_data, stream_id, payload_id, message, metadata}` | one complete frame; `metadata` is a map of binaries |
| `{:flz_closed, stream_id, reason}` | stream or connection closed; `reason` is a binary |
| `{:flz_refused, reason}` | QUIC peer refused at the handshake before any stream; `Transport.Server` logs it and counts it on `[:flezha, :transport, :connect_rejected]` with `stage: :handshake` |

Messages are built in `driver_msg.rs` with `OwnedEnv::send_and_clear` and go to the stream's
**current** owner (`controlling_process/2` moves it). `stream_id/1` returns the id that
`{:flz_data, ...}` and `{:flz_closed, ...}` carry for a handle, which is how an owner correlates
an opaque handle with its later events.

## NIF functions

| Function | Signature | Notes |
|---|---|---|
| `start_uds_server` | `(socket_path, acceptor_pid) -> {:ok, server}` | spawns the UDS driver |
| `start_quic_server` | `(bind_address, cert_path, key_path, client_ca_path, acceptor_pid) -> {:ok, server}` | `client_ca_path == ""` disables client auth |
| `stop_server` | `(server) -> :ok` | `Shutdown` to the driver |
| `driver_stats` | `(server) -> map` | driver self-report, one shape for the UDS and the QUIC driver |
| `send_frame` | `(handle, payload_id, message, metadata) -> `:ok` \| `{:error, :send_buffer_full \| :driver_gone}` | frame built in Rust by `payloader::build_from_binary`; refuses on a raised send gate, and answers rather than discarding when the driver thread is gone |
| `set_active` / `set_passive` | `(handle) -> :ok` | deliver frames as messages / queue them |
| `close` | `(handle) -> :ok` | |
| `controlling_process` | `(handle, pid) -> :ok` | transfers ownership |
| `stream_id` | `(handle) -> integer` | driver-assigned id |
| `client_connect_uds` / `client_connect_quic` / `client_open_stream` | see above | standalone SDK only |
| `socket_dir_live` | `(socket_dir) -> boolean` | `socket_probe::probe` from the controller's side: a live peer's directory is not this instance's to clear |
| `lock_state_root` | `(state_root) -> {:ok, lock_file} \| {:error, holder}` | `fcntl` record lock held for the OS process lifetime; `F_GETLK` names the holder |
| `held_state_root` | `() -> string` | the lock file this process holds, `""` before it has claimed one |

Every stream NIF enqueues a `ServerCommand` and wakes the driver; none blocks.

## Elixir integration

**`Flezha.Transport.Stream`** is the per-handle dispatch seam: `send_frame/4`, `set_active/1`,
`close/1`, `controlling_process/2`, `stream_id/1`. A `%Flezha.Transport.Local.Stream{}` (the
in-node `:native` transport used by the in-BEAM Elixir SDK) takes the Elixir arm; any other term
is a NIF resource and goes to `Flezha.Transport.Native`. There is one send: the control channel
frames once, in Rust, through `send_frame(ref, "control_command" | "control_ping", bytes, %{})`.
The invariant is grep-simple: no per-stream `Native.` call outside `native.ex` and the seam.
`Flezha.Transport.Native` implements `Flezha.Transport.Behaviour` (`set_active/1`, `close/1`,
`controlling_process/2`).

**`Flezha.Transport.Server`** runs one GenServer per entry in the `:transports` app env
(`FLEZHA_TRANSPORTS`, default `[:uds]`, `[:uds, :quic]` in the test env; `:native` is opt-in),
named `Flezha.Transport.Server.Uds | .Quic | .Native`:

- UDS: `Native.start_uds_server(path, self())` on
  `<socket dir>/runner_control_channel.sock`, the directory resolved by the
  shared ladder (`FLEZHA_SOCKET_FILES_PATH`, then `$XDG_RUNTIME_DIR/flezha`,
  `/run/flezha`, `app_data/sockets` — see [`../configuration.md`](../configuration.md)).
- QUIC: `Flezha.Transport.CertGenerator.ensure_certs()`, then `Native.start_quic_server/5` on
  `FLEZHA_QUIC_LISTEN_ADDRESS` (default `0.0.0.0:4433`) with `FLEZHA_SERVER_CERT_PATH` /
  `FLEZHA_SERVER_KEY_PATH` and the client CA `FLEZHA_QUIC_CLIENT_CA` (default
  `app_data/certs/ca.pem`), or `""` when `FLEZHA_TRANSPORT_AUTH=off`.

Handshake: `{:flz_connection, handle}` is activated and parked in `pending_streams` under its
stream id; the `{:flz_data, id, "connect_control_channel", json, _}` frame is matched back to
that exact handle and dispatched to `FleetManager.connect_handler/4`, which starts the runner's
`RunnerSupervisor` and hands stream ownership to its `ControlChannel`. A rejected connect closes
the stream and emits `[:flezha, :transport, :connect_rejected]` with `stage: :authorization`.
For QUIC the server keeps `quic_conn_runners` (`conn_id -> runner_id`, dropped when the
ControlChannel goes down) and routes each `{:flz_stream, ...}` to that runner's channels via
`StreamPool.bind_stream_to_first_nil/2`.

**`Flezha.Runner.RunnerSupervisor`** starts `ControlUnit`, `ControlChannel.Commits`,
`ControlChannel` and the `:primary` / `:secondary` `StreamSup`s for every transport; for UDS it
adds a `StreamAcceptor` per stream type, each running its own NIF UDS server on
`<type>_stream_<runner_id>.sock` and binding accepted connections round-robin to the first
unbound `StreamChannel` (the atomic `{:bind, ref}` call; a live channel answers
`:already_bound`). QUIC and native add no acceptors. `ControlChannel` echoes every
`control_ping` frame on the same payload id.

```text
UDS                                     QUIC
SDK connects control socket             SDK QUIC-connects, opens bidi stream #1
  -> {:flz_connection, h}                 -> {:flz_connection, h}
SDK sends connect_control_channel       SDK sends connect_control_channel
  -> {:flz_data, id, "connect_...", ..}   -> {:flz_data, id, "connect_...", ..}
  -> FleetManager -> RunnerSupervisor     -> FleetManager -> RunnerSupervisor; conn_id bound
  <- AssignRunnerId, ConnectSocket        <- AssignRunnerId, ConnectSocket
SDK connects primary/secondary sockets  SDK opens streams, writes [1,i] / [2,i]
  -> StreamAcceptor binds a channel       -> {:flz_stream, h, t, i, conn_id} -> bind a channel
```

## Build and test

```bash
make nif                 # cargo build -p flz_nif
make nif-release         # cargo build --release -p flz_nif
cargo test -p flz_nif    # FrameBuffer, oversized-header, write-cap, DriverStats, client tests
mix compile              # Rustler (~> 0.35, Elixir ~> 1.17) builds flz_nif in release mode
mix test                 # test env enables both UDS and QUIC
./scripts/bench.sh -t quic   # wire transport for the bench (default uds)
```

## See also

- [Protocol spec](../sdk/protocol_spec.md) — frame and protobuf command formats
- [Transport liveness](transport_liveness.md) — driver stats, stall classifier, send-buffer cap
- [Runner identity](runner_identity.md) — sessions, reconnect and per-connection routing
- [Elixir SDK](../sdk/elixir_sdk.md) — the `:native` transport and the standalone client
- [Authn/authz](../operations/authn_authz.md) — transport peer authentication
