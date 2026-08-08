# Shared Library Architecture

## Overview

> **`extract-sdk-poll-core`**: the SDK kernel described below now lives in
> the `flz_core` workspace crate, consumed by `sdk/rust` as a crate and by
> non-Rust hosts through `flz_core_ffi`'s poll/complete C ABI. The
> boundary contract and host loop are documented in
> `docs/sdk_poll_core.md`; the layers below describe the transport tier,
> which is unchanged.

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

### The poll core (`flz_core`, shared kernel) and the language SDK

Since `extract-sdk-poll-core`, the runner kernel is one Rust crate consumed
by every SDK:

| Module (`flz_core/src/`) | Purpose |
|---|---|
| `poll_core.rs` | The activation engine: per-entity handout exclusivity, batch assembly (admission, replay-slice ordering, due-timer generation), the retry→park ladder, `poll`/`complete` |
| `schema.rs` | The versioned Activation/ActivationResult boundary contract (postcard; hex-pinned fixtures) |
| `host.rs` | Erased host classes: data-declared topologies, `BytesBuffer`, completion application — what the C ABI drives |
| `entity.rs`, `mailbox.rs` | `EntitySlot`, bounded mailbox + inflight CAS, `BlockParker`, shared slot assembly |
| `routing.rs` | `enqueue` + overflow policies, consumer-edge delivery, entry-parse and edge ladders, end-of-slice broadcast |
| `flusher.rs`, `state_sink.rs`, `credit.rs`, `park.rs`, `correction.rs`, `timer.rs`, `restore.rs`, `header.rs` | Durability: WAL framing + linger drain, the reliable sink, credit windows, DLQ parks, late-input machinery, timers, restore payloads, record headers |
| `fast.rs`, `control_unit.rs`, `runner.rs`, `command/` | RunnerNode, wire readers, control channel + reconnect, the 19 command handlers |
| `bounded/` | The bounded driver, saga machinery, spec/DAG kernel |
| `work_stealing_pool.rs` | Chase-Lev pool — bounded-driver stage tasks only |

`flz_core_ffi` wraps the same engine in a C ABI (poll/complete, versioned
schema negotiation) for non-Rust hosts. What stays per-language: the
authoring surface (traits, graph DSL, codecs), the poll loop's integration
with the host's concurrency primitives, idiomatic errors, and the test
harness. See `docs/sdk_poll_core.md`.

## Rust SDK dispatch

The Rust SDK's typed layer is the first poll host. Stream-reader threads
push directly into the target Entity's bounded mailbox
(`crossbeam::queue::ArrayQueue` + `AtomicBool inflight`); the CAS winner
schedules an engine round, and the typed layer's **poll workers** (N =
`FLEZHA_WORKER_POOL_SIZE`, default available parallelism) serve it:
`poll() → run one attempt per item through the typed dispatcher →
complete` (or hand a failed head back to the engine's ladder).

Sticky per-Entity ordering is preserved: batch assembly runs under the
single-active-drainer gates, an entity with an outstanding activation is
never handed out again until its completion, and the inflight flag closes
the classic push-vs-empty race (see `flz_core/src/mailbox.rs`). The
work-stealing pool still exists for bounded-driver stage tasks; unbounded
dispatch no longer runs on it.

The coordinator thread handles only control-plane traffic
(`HandleCommandAction`, `ReturnOperatives`, `Reconnect`); data-plane
frames never cross it.

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
├── flz_core/                     # Poll-based SDK core (shared runner kernel)
│   └── src/                      # Engine, schema, host layer, durability, commands
├── flz_core_ffi/                 # C ABI over flz_core (cdylib + staticlib)
├── sdk/rust/                     # Rust SDK — the typed layer over flz_core
│   ├── Cargo.toml                # depends on flz_core + flz_transport
│   └── src/
│       ├── lib.rs                # Flezha struct, public API
│       ├── procedure/            # Procedure trait, ctx, clocks, errors
│       ├── channel/              # Channel<T> + the builder DSL
│       ├── operative/            # UnboundedOperative trait, builder, typed dispatcher
│       ├── entity/               # Typed halves: TypedBuffer, header/timer wrappers, restore fold
│       └── bounded/              # BoundedOperative trait + spec build/validation
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
