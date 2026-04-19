# Shared Library Architecture

## Overview

Flezha uses a **shared Rust library** for all transport-level communication between the Elixir agent and language SDKs. The library handles everything needed to move bytes between the two sides — QUIC, UDS, binary framing, protobuf encoding, and TLS — but **nothing more**.

Each language SDK is responsible for its own compute engine: running processors, managing state, worker pools, and command dispatch. A Python SDK runs processors in Python. A Go SDK runs them in Go. The shared library is the communication layer only.

## Why

Without a shared library, every new language SDK must implement:
- QUIC client (quinn-proto, mio, crossbeam or equivalent)
- UDS client (platform-specific)
- Binary framing protocol (payloader)
- Protobuf command encoding/decoding
- TLS certificate handling

This is ~1000 lines of non-trivial, protocol-specific code that must be identical across all SDKs. By extracting it into a single Rust shared library (`.so`/`.dylib`/`.dll`), new SDKs only need FFI bindings to call into it.

On the agent side, the same library replaces `:quicer` (MsQuic NIF) with a Rustler NIF, giving the Elixir agent the same transport implementation as all SDKs.

## Architecture Layers

```
┌─────────────────────────────────────────────────────────┐
│                    Language SDK                          │
│  (Rust, Python, Go, Java, etc.)                         │
│                                                         │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐ │
│  │  Compute     │  │  Processor   │  │  Command       │ │
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
│  C FFI API  ·  Rustler NIF (for Elixir agent)           │
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
| `compute_engine/mod.rs` | ComputeEngineNode, message loop, worker dispatch |
| `compute_engine/control_unit.rs` | Control channel open (UDS/QUIC), init handshake |
| `compute_engine/worker_pool.rs` | Thread pool for processor execution |
| `command/mod.rs` | Command routing and response building |
| `command/*.rs` | Individual command handlers (assign, connect, add/rem processor, etc.) |
| `processor/mod.rs` | Processor trait, types, factory registration |
| `processor/executor.rs` | Processor execution and output piping |
| `processor/factory_registry.rs` | Global factory registry, ProcessorHandle |
| `lib.rs` | `Flezha` struct, public API (`new`, `get_processors`) |

## C API Surface

The shared library exposes a C API following established patterns from libcurl, gRPC-core, and ZeroMQ.

### Design Principles

1. **Context/Handle Pattern** — All state behind opaque handles. No global state.
2. **Error Handling** — Every function returns `FlezhaStatus`. Thread-local last-error string.
3. **Memory Ownership** — Whoever allocates, frees. Library buffers freed via `flezha_*_free()`.
4. **Configuration via Structs** — Typed config structs, env vars are convenience wrappers.
5. **Explicit Lifecycle** — `new → configure → connect → use → close → destroy`.
6. **Logging Callbacks** — Each language routes logs to its own logger.
7. **Thread Safety** — Context creation/destruction: single-threaded. All other operations: thread-safe.

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

// Transport — Server (agent / Elixir NIF side)
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
│       ├── compute_engine/       # Engine lifecycle
│       ├── command/              # Command handlers
│       └── processor/            # Processor trait + execution
├── sdk/java/                     # Java SDK (Maven multi-module)
├── sdk/python/                   # Python SDK (future)
├── flz_jni/                      # JNI bindings (cdylib, re-exports flz_transport symbols)
└── lib/                          # Elixir agent (uses shared lib via NIF)
```

## Java Bindings (`flz_jni`)

The Java SDK binds to `flz_transport` through a thin `flz_jni` crate
(`crate-type = ["cdylib"]`) that exports `Java_com_flezha_sdk_native_1_NativeTransport_*`
symbols and delegates to `flz_transport`'s Rust + C FFI APIs. The cdylib is
named `libflz_transport` so a single shared library lives on the Java
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
    ├── flezha-sdk-api/                        # Public types — ProcessorHandler, sealed Input/Output/Result
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
`libflz_transport.{so,dylib,dll}` for five target triples. The binaries
land under `src/main/resources/native/<os>-<arch>/` and are extracted +
`System.load`-ed at SDK init time by `NativeLibraryLoader`.
