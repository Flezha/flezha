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
| `workflow/` | The workflow driver (lifecycle, DAG walk, commits, saga), `step.rs` (a stage's step activations + its retry barrier), the spec/DAG kernel |

`flz_core_ffi` wraps the same engine in a C ABI (poll/complete, versioned
schema negotiation) for non-Rust hosts. What stays per-language: the
authoring surface (traits, graph DSL, codecs), the poll loop's integration
with the host's concurrency primitives, idiomatic errors, and the test
harness. See `docs/sdk_poll_core.md`.

## Rust SDK dispatch

The Rust SDK's typed layer is the first poll host. Stream-reader threads
push directly into the target Entity's bounded mailbox
(`crossbeam::queue::ArrayQueue` + `AtomicBool inflight`); the CAS winner
schedules an engine round, and the typed layer's **poll workers** (base N =
`FLEZHA_WORKER_POOL_SIZE`, default available parallelism, grown under
starvation up to `FLEZHA_WORKER_POOL_MAX`) serve it: `poll() → run one
attempt per item through the typed dispatcher → complete` (or hand a failed
head back to the engine's ladder).

Sticky per-Entity ordering is preserved: batch assembly runs under the
single-active-drainer gates, an entity with an outstanding activation is
never handed out again until its completion, and the inflight flag closes
the classic push-vs-empty race (see `flz_core/src/mailbox.rs`). Workflow
stage work rides the same workers: the driver hands each entity partition
out as a step activation and waits at its barrier, so the core owns no
thread that runs user code.

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
│       ├── operative/            # StreamOperative trait, builder, typed dispatcher
│       ├── entity/               # Typed halves: TypedBuffer, header/timer wrappers, restore fold
│       └── workflow/              # WorkflowOperative trait + spec build/validation
├── sdk/java/                     # Java SDK (Maven multi-module)
├── sdk/python/                   # Python SDK (future)
└── lib/                          # Elixir controller (uses shared lib via NIF)
```

## Java Bindings

The Java SDK is a **poll host over `flz_core`**: its runtime binds to
`flz_core_ffi` (built with the `jni` feature, shipped as `libflz_core_ffi`)
through the one-to-one `FlzCore` class — context creation with schema
negotiation, class declaration (JSON), start, blocking poll, complete, the
workflow session calls, spawn, shutdown. JVM poller threads loop
`poll → decode Activation → run user code → encode ActivationResult →
complete`; the kernel (transport, ordering, durability, retries, timers,
credit, routing, the workflow driver, reconnect) lives entirely in the core.
See `docs/sdk_poll_core.md`.

### The shipped binary

`flz_core_ffi` is built with `crate-type = ["cdylib"]` and the `jni`
feature, exporting both the C ABI (`flz_core_*`) and the JNI symbols
(`Java_com_flezha_sdk_native_1_FlzCore_*`) from one binary, so a single
shared library lives on the Java classpath per platform:
`libflz_core_ffi.{so,dylib,dll}`.

### Layout

```
flezha/
├── flz_core/                                  # The language-agnostic runner kernel
├── flz_core_ffi/                              # C ABI + JNI exports (feature "jni")
│   └── src/
│       ├── lib.rs                             # flz_core_* entry points
│       └── jni_glue.rs                        # Java_com_flezha_sdk_native_1_FlzCore_*
└── sdk/java/
    ├── pom.xml                                # Maven parent
    ├── flezha-sdk-api/                        # Public authoring surface (no runtime deps)
    ├── flezha-sdk-processor/                  # Annotation processor
    ├── flezha-sdk-runtime/                    # ActivationDispatcher + codec + FlezhaImpl pollers
    ├── flezha-sdk-native/                     # FlzCore + NativeLibraryLoader + bundled binaries
    ├── flezha-sdk-testing/                    # ProcedureTestHarness
    ├── flezha-sdk-examples/                   # Worked examples
    └── flezha-sdk/                            # Fat-jar aggregator (the published artifact)
```

### Buffer ownership at the JNI boundary

1. Every `byte[]` the binding returns (a polled activation, an inbox
   payload, a spawn outcome) is copied from the core's buffer into a fresh
   Java array, and the native buffer is freed through `flz_core_buf_free`
   before the JNI method returns. No native buffer escapes.
2. Java-owned arrays handed into native methods (`declareClass`,
   `complete`, `workflowEmit`, …) are copied into Rust-side buffers before
   the call returns; the JVM retains ownership.

### Native binary packaging

The `flezha-sdk-native` module's `native-build` Maven profile drives
`scripts/build-native.sh` (cargo-zigbuild) to cross-compile
`libflz_core_ffi.{so,dylib,dll}` (feature `jni`) for five target triples.
The binaries land under `src/main/resources/native/<os>-<arch>/` and are
extracted + `System.load`-ed at SDK init by `NativeLibraryLoader`; schema
negotiation at `FlzCore.contextNew` refuses a shim/core version mismatch
before any controller connection.
