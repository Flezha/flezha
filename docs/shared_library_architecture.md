# Shared Library Architecture

## Overview

Flezha uses a **shared Rust library** for all transport-level communication between the Elixir controller and language SDKs. The library handles everything needed to move bytes between the two sides — QUIC, UDS, binary framing, protobuf encoding, and TLS — but **nothing more**.

Each language SDK is responsible for its own runner: running operatives, managing state, worker pools, and command dispatch. A Python SDK runs operatives in Python. A Go SDK runs them in Go. The shared library is the communication layer only.

## Why

Without a shared library, every new language SDK must implement:
- QUIC client (quinn-proto, mio, crossbeam or equivalent)
- UDS client (platform-specific)
- Binary framing protocol (payloader)
- Protobuf command encoding/decoding
- TLS certificate handling

This is ~1000 lines of non-trivial, protocol-specific code that must be identical across all SDKs. By extracting it into a single Rust shared library (`.so`/`.dylib`/`.dll`), new SDKs only need FFI bindings to call into it.

On the controller side, the same library replaces `:quicer` (MsQuic NIF) with a Rustler NIF, giving the Elixir controller the same transport implementation as all SDKs.

## Architecture Layers

```
┌─────────────────────────────────────────────────────────┐
│                    Language SDK                          │
│  (Rust, Python, Go, Java, etc.)                         │
│                                                         │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐ │
│  │  Compute     │  │  Operative   │  │  Command       │ │
│  │  Engine      │  │  Execution   │  │  Handlers      │ │
│  │  Lifecycle   │  │  & State     │  │  & Dispatch    │ │
│  └──────┬───────┘  └──────┬───────┘  └───────┬────────┘ │
│         │                 │                  │          │
│         └─────────────────┼──────────────────┘          │
│                           │ (native language calls)     │
├───────────────────────────┼─────────────────────────────┤
│                           │                             │
│          Shared Library (Rust .so/.dylib/.dll)           │
│                                                         │
│  ┌──────────────┐  ┌────────────┐  ┌─────────────────┐ │
│  │  Transport   │  │  Payloader  │  │  Protobuf       │ │
│  │  QUIC / UDS  │  │  Binary     │  │  Command        │ │
│  │  Client +    │  │  Framing    │  │  Encode/Decode  │ │
│  │  Server      │  │             │  │                 │ │
│  └──────────────┘  └────────────┘  └─────────────────┘ │
│  ┌──────────────┐  ┌────────────┐                       │
│  │  TLS Cert    │  │  Path      │                       │
│  │  Generation  │  │  Helpers   │                       │
│  └──────────────┘  └────────────┘                       │
│                                                         │
│  C FFI API  ·  Rustler NIF (for Elixir controller)           │
└─────────────────────────────────────────────────────────┘
```

## What goes where

### Shared Library (transport-only)

| Module | Purpose |
|---|---|
| `transport/quic.rs` | QUIC client (quinn-proto, mio, crossbeam) + server mode |
| `transport/mod.rs` | `FlezhaStream` enum (Unix / Quic) with Read/Write impls |
| `transport/config.rs` | Transport type detection, address/cert path resolution |
| `transport/protocol.rs` | Stream type constants (PRIMARY `0x01`, SECONDARY `0x02`) |
| `service/payloader.rs` | Binary frame build/extract per SDK protocol spec |
| `utility/path_helper.rs` | Socket file path construction |
| `generated/` | Protobuf types (CommandRequest, CommandResponse, etc.) |
| `ffi/` | C API: context, config, error handling, logging, frame ops |

### Language SDK (per-language, not shared)

| Module | Purpose |
|---|---|
| `runner/mod.rs` | RunnerNode, message loop, worker dispatch |
| `runner/control_unit.rs` | Control channel open (UDS/QUIC), init handshake |
| `runner/worker_pool.rs` | Thread pool for operative execution |
| `command/mod.rs` | Command routing and response building |
| `command/*.rs` | Individual command handlers (assign, connect, add/rem operative, etc.) |
| `operative/mod.rs` | Operative trait surface, types, factory registration |
| `operative/runner.rs` | Operative execution and output piping |
| `operative/factory_registry.rs` | Global factory registry, OperativeHandle, `OperativeRegistry` (DashMap-backed live instance map under fast-dispatch) |
| `lib.rs` | `Flezha` struct, public API (`new`, `get_operatives`) |

## Rust SDK dispatch

The Rust SDK (`sdk/rust/`) has two dispatch backends selected at compile
time via the `fast-dispatch` cargo feature. Both preserve the same public
API and sticky per-operative ordering semantics — the feature flag only
changes the internal fast path.

### Legacy (default: `fast-dispatch` off)

`runner/legacy.rs`. Every inbound message crosses the coordinator
channel (`crossbeam::channel`) into `RunnerNode`'s message loop,
which performs a lock-heavy check-out: `RwLock<HashMap<operative>>` read
→ operative `Mutex` acquire for runner type → `operative_workers` /
`available_workers` / `operative_queues` / `pending_operatives`
bookkeeping → dispatch to a fixed worker over `Sender<WorkerPoolMessage>`.
The worker runs the operative and checks back in, which triggers the next
queued item in FIFO order. Sticky ordering comes from the `operative_workers`
map that pins one operative to one worker at a time.

### Fast (opt-in: `fast-dispatch` on — per-operative mailboxes)

`runner/fast.rs` + `runner/mailbox.rs` +
`runner/work_stealing_pool.rs`. Every operative owns:
  - an `Arc<Mutex<OperativeBox>>` — same inner lock as before (the
    operative trait uses `&mut self`, so a lock is required).
  - a `Mailbox<OperativePayload>` — a `crossbeam::queue::SegQueue` plus
    an `AtomicBool inflight` flag.

Stream-reader threads (one per primary stream) push directly into the
target operative's mailbox and CAS `inflight false→true`. The CAS winner
submits a drain closure into a **Chase-Lev work-stealing pool** built on
`crossbeam-deque::{Injector, Worker, Stealer}`; N worker threads
(default `num_cpus::get()`, override via `FLEZHA_WORKER_POOL_SIZE`) poll
their own deque, the shared injector, and steal from siblings.

The drain closure loops: pop one item, take the operative lock, run, pipe
the `OperativeResult` (PipeToAgent writes the secondary stream, PipeToOperative
pushes into the target operative's mailbox, preserving downstream
ordering). On an observed-empty queue, the drain stores `inflight = false`
and re-checks the queue — this is the classic "re-CAS on wake miss" that
closes the race where a concurrent push beat the store (see
`runner/mailbox.rs` docstring for the worked case).

Sticky per-operative ordering is preserved: only the CAS winner drains,
and the inflight flag guarantees at most one drainer at a time per
mailbox. Work-stealing provides load balance across operatives; the
inflight flag is what provides serialization within an operative.

The coordinator thread still exists under fast-dispatch but handles only
control-plane traffic (`HandleCommandAction`, `ReturnOperatives`,
`Reconnect`). Under fast-dispatch, the `RunnerMessage` enum has
the data-plane variants (`WorkerCheckOut` / `WorkerCheckIn` /
`WorkerInitCheckIn`) removed at the type level — attempting to send one
is a compile error.

### Feature-flag rollout

The flag defaults OFF on the published `flezha_runner` crate
during rollout (to give a single-cycle A/B against the legacy path) and
ON for in-repo consumers (`test/apps/rust_runner`). See
`openspec/changes/optimize-rust-sdk-dispatch/` for the full proposal,
design, and tasks. The Criterion suite at `sdk/rust/benches/` pins
dispatch throughput and contention-read metrics so CI catches future
regressions.

## C API Surface

The shared library exposes a C API following established patterns from libcurl, gRPC-core, and ZeroMQ.

### Design Principles

1. **Context/Handle Pattern** — All state behind opaque handles. No global state.
2. **Error Handling** — Every function returns `FlezhaStatus`. Thread-local last-error string.
3. **Memory Ownership** — Whoever allocates, frees. Library buffers freed via `flezha_*_free()`.
4. **Configuration via Structs** — Typed config structs, env vars are convenience wrappers.
5. **Explicit Lifecycle** — `new → configure → connect → use → close → destroy`.
6. **Logging Callbacks** — Each language routes logs to its own logger.
7. **Thread Safety** — Context creation/destruction: single-threaded. All other procedures: thread-safe.

### Functions

```c
// Lifecycle
FlezhaContext*  flezha_context_new(FlezhaConfig config);
FlezhaStatus    flezha_context_destroy(FlezhaContext* ctx);
FlezhaConfig    flezha_config_default(void);
const char*     flezha_last_error(void);
const char*     flezha_version(void);

// Logging
FlezhaStatus    flezha_set_log_callback(FlezhaContext* ctx, FlezhaLogFn fn, void* user_data);

// Transport — Client (SDK side)
FlezhaStatus    flezha_connect(FlezhaContext* ctx);
FlezhaStatus    flezha_open_stream(FlezhaContext* ctx, uint8_t stream_type, uint8_t index,
                    uint64_t* stream_id);
FlezhaStatus    flezha_stream_write(FlezhaContext* ctx, uint64_t stream_id,
                    const uint8_t* data, size_t len);
FlezhaStatus    flezha_stream_read(FlezhaContext* ctx, uint64_t stream_id,
                    uint8_t* buf, size_t buf_len, size_t* bytes_read);
FlezhaStatus    flezha_close(FlezhaContext* ctx);

// Transport — Server (controller / Elixir NIF side)
FlezhaStatus    flezha_server_listen(FlezhaContext* ctx, FlezhaServerCallbacks cbs, void* user_data);
FlezhaStatus    flezha_server_send(FlezhaContext* ctx, uint64_t conn_id, uint64_t stream_id,
                    const uint8_t* data, size_t len);
FlezhaStatus    flezha_server_close_stream(FlezhaContext* ctx, uint64_t conn_id, uint64_t stream_id);
FlezhaStatus    flezha_server_stop(FlezhaContext* ctx);

// Payloader (standalone, no context needed)
FlezhaStatus    flezha_frame_build(const char* payload_id, const uint8_t* message, size_t msg_len,
                    const char* metadata_json, uint8_t** out, size_t* out_len);
FlezhaStatus    flezha_frame_free(uint8_t* data, size_t len);

// TLS
FlezhaStatus    flezha_ensure_certs(const char* ca_path, const char* cert_path, const char* key_path);
```

## Configuration

```c
FlezhaConfig config = flezha_config_default();
config.transport = FLEZHA_TRANSPORT_QUIC;        // or FLEZHA_TRANSPORT_UDS
config.quic_address = "127.0.0.1:4433";
config.ca_cert_path = "app_data/certs/ca.pem";
config.socket_base_path = "app_data/sockets";
config.worker_pool_size = 4;
```

Environment variables (`FLEZHA_TRANSPORT`, `FLEZHA_QUIC_ADDRESS`, etc.) remain as convenience — the library reads them as defaults when config fields are null/zero.

## File Layout

```
flezha/
├── Cargo.toml                    # Workspace root
├── flz_transport/                # Shared transport library (crate: flz_transport)
│   ├── Cargo.toml                # rlib + cdylib targets
│   ├── cbindgen.toml             # C header generation
│   └── src/
│       ├── lib.rs
│       ├── transport/            # QUIC + UDS
│       ├── service/              # Payloader
│       ├── utility/              # Path helpers
│       ├── generated/            # Protobuf types
│       └── ffi/                  # C API layer
├── sdk/rust/                     # Rust SDK
│   ├── Cargo.toml                # depends on shared lib
│   └── src/
│       ├── lib.rs                # Flezha struct, public API
│       ├── runner/       # Engine lifecycle
│       ├── command/              # Command handlers
│       └── operative/        # Operative trait + execution
├── sdk/java/                     # Java SDK (Maven multi-module)
├── sdk/python/                   # Python SDK (future)
├── flz_jni/                      # JNI bindings (cdylib, re-exports flz_transport symbols)
└── lib/                          # Elixir controller (uses shared lib via NIF)
```

## Java Bindings (`flz_jni`)

The Java SDK binds to `flz_transport` through a thin `flz_jni` crate
(`crate-type = ["cdylib"]`) that exports `Java_com_flezha_sdk_native_1_NativeTransport_*`
symbols and delegates to `flz_transport`'s Rust + C FFI APIs. The cdylib is
named `libflz_jni` so a single shared library lives on the Java
classpath per platform.

### Layout

```
flezha/
├── flz_jni/                                   # Rust — JNI entry points + reader threads
│   ├── Cargo.toml                             # cdylib, depends on flz_transport
│   └── src/
│       ├── lib.rs                             # module root + throw_native helper
│       ├── context.rs                         # JniContext + contextNew/destroy/open/write/close
│       ├── callbacks.rs                       # on_data / on_stream_closed / on_connection_lost
│       ├── frame.rs                           # buildFrame — calls flezha_frame_build, copies bytes, frees
│       ├── logging.rs                         # setLogCallback — routes FFI logs into Java
│       └── version.rs                         # version() — thin wrapper over flezha_version
└── sdk/java/
    ├── pom.xml                                # Maven parent (6 modules)
    ├── flezha-sdk-api/                        # Public types — OperativeHandler, sealed Input/Output/Result
    ├── flezha-sdk-protobuf/                   # Generated Java classes from protobufs/*.proto
    ├── flezha-sdk-runtime/                    # Control plane, data plane, worker pool, reconnect
    ├── flezha-sdk-native/                     # NativeTransport + NativeLibraryLoader + bundled binaries
    ├── flezha-sdk/                            # Fat-jar aggregator (the published artifact)
    └── flezha-sdk-integration-tests/          # End-to-end tests driven by the Elixir harness
```

### Buffer ownership at the JNI boundary

1. Frames built via `buildFrame(payloadId, message, metadataJson)` call
   `flezha_frame_build` in `flz_transport`, copy the returned native buffer
   into a fresh Java `byte[]`, then call `flezha_frame_free`. No native
   buffer escapes the JNI method.
2. Incoming stream data (`on_data` callback) reaches Java as a fresh
   `byte[]` allocated by the JVM; the native reader thread drops its local
   buffer immediately after the callback returns.
3. Java-owned arrays handed into native methods (`streamWrite`, etc.) are
   copied into Rust-side buffers before the call returns; the JVM retains
   ownership.

### Native binary packaging

The `flezha-sdk-native` module's `native-build` Maven profile drives
`scripts/build-native.sh` (cargo-zigbuild) to cross-compile
`libflz_jni.{so,dylib,dll}` for five target triples. The binaries
land under `src/main/resources/native/<os>-<arch>/` and are extracted +
`System.load`-ed at SDK init time by `NativeLibraryLoader`.
