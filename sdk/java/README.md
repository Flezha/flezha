# Flezha Java SDK

Java 21 SDK for authoring Flezha compute engines. Feature parity with the
Rust SDK (`sdk/rust/`), talks to the existing Elixir controller through JNI
bindings over `flz_transport`. Supports both UDS and QUIC transports.

## Quickstart

The Java SDK uses the same Operative / Operation / Channel model as the Rust SDK
(see `docs/operative_architecture.md`). User code declares one or more
ContinuousOperative classes, registers them via
`Flezha.registerContinuous(MyOperative.class)`, then
boots the SDK with `Flezha.newInstance(namespace)`.

```java
public final class EchoOperation
        implements Operation<String, EchoOperation.State, byte[], NoChange, WallClock, Void> {
    public record State() {}

    @Override public String name() { return "echo_operation"; }
    @Override public Class<String> inputType() { return String.class; }
    @Override public Class<byte[]> outputType() { return byte[].class; }
    @Override public Class<NoChange> changeType() { return NoChange.class; }
    @Override public Class<WallClock> clockType() { return WallClock.class; }
    @Override public Class<Void> configType() { return Void.class; }

    @Override public State initState(Optional<String> operationKey, Optional<RestoreInitPayload> p) {
        return new State();
    }

    @Override public void run(String input, StateHandle<State, NoChange> state, OperationCtx ctx)
            throws OperationException {
        ctx.emit(input.getBytes(StandardCharsets.UTF_8));
    }
}

public final class EchoOperative implements ContinuousOperative {
    @Override public String factoryId() { return "echo"; }
    @Override public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(EchoOperation.class);
        c.channel(byte[].class, "out").from(EchoOperation.class).toPlugin("echo_kafka");
    }
}

public final class App {
    public static void main(String[] args) {
        Flezha.registerContinuous(EchoOperative.class);
        Flezha sdk = Flezha.newInstance(args[0]);
        System.out.println("assigned id: " + sdk.getId());
    }
}
```

## Build Prerequisites

- **Java 21** or newer (JDK with `javac`). The build is pinned to Java 21 via
  `maven-compiler-plugin`'s `<release>21</release>` and enforced by
  `maven-enforcer-plugin`.
- **Rust stable toolchain** (`cargo`, `rustc`). Needed to build the `flz_jni`
  bridge crate that produces `libflz_transport.{so,dylib,dll}`.
- **[cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild)** for the
  native cross-build. Install via `cargo install cargo-zigbuild`. The Zig
  toolchain is pulled in by `cargo-zigbuild` itself.

## Entry point: `mvn package`

Portable build (bundles all five per-OS native binaries into the fat jar):

```
mvn -f sdk/java/pom.xml -P native-build package
```

Fast local iteration (builds only the host triple — **non-portable**):

```
mvn -f sdk/java/pom.xml -P host-only package
```

## Modules

| Module | Purpose |
|---|---|
| `flezha-sdk-api` | Public API: `Flezha`, `ContinuousOperative`, `ContinuousOperativeRegistry`, `Operation`, `OperationCtx`, `OperationException`, channel builder types. No native dependency. |
| `flezha-sdk-protobuf` | Java classes generated from `protobufs/executor_commands.proto`. Pure generated code; no hand-written sources. |
| `flezha-sdk-runtime` | Control plane, data plane, framing, worker pool, reconnect, command dispatch. |
| `flezha-sdk-native` | JNI bindings (`NativeTransport`) + bundled per-OS `libflz_transport` native binaries. |
| `flezha-sdk-testing` | Test fixtures — `OperationTestHarness` for Operation authors. Add as `<scope>test</scope>`. |
| `flezha-sdk` | Fat-jar aggregator — the published artifact. |
| `flezha-sdk-integration-tests` | End-to-end tests against a running controller (typically driven by the Elixir harness via `mix test.java_sdk`). |

## State persistence

The SDK persists per-Segment state via a `Change`-trait machinery
mirroring the Rust SDK. Operations opt in by declaring a non-`NoChange`
Change type and recording mutations through
`StateHandle.apply(change)` instead of direct field writes. The
per-Segment flusher batches Changes on a 5 ms linger window and ships
them on the controller's control channel using the umbrella's wire format
(`sdk_lang = 2`, `format_id = 4` = Kryo).

See `docs/java_state_persistence.md` for the full API surface, code
examples, and Kryo registration guidance. `flezha-sdk-testing`'s
`OperationTestHarness` provides the `replayEquals()` determinism gate.

## Running tests

Unit tests (no controller required):

```
mvn -f sdk/java/pom.xml test
```

With coverage report (JaCoCo):

```
mvn -f sdk/java/pom.xml -P coverage verify
```

Integration tests (against a live controller):

```
mix test.java_sdk  # from the repo root; runs the full Java-runtime matrix
```

## Environment variables

All SDK configuration is read from environment variables matching the
Rust SDK and documented in `docs/sdk_protocol_spec.md` §2:

| Variable | Default | Notes |
|---|---|---|
| `FLEZHA_TRANSPORT` | `uds` | `uds` or `quic`. |
| `FLEZHA_QUIC_ADDRESS` | `127.0.0.1:4433` | QUIC only. |
| `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` | QUIC only. |
| `FLEZHA_SERVER_CERT_PATH` | `app_data/certs/cert.pem` | Server mode only. |
| `FLEZHA_SERVER_KEY_PATH` | `app_data/certs/key.pem` | Server mode only. |
| `FLEZHA_SOCKET_FILES_PATH` | `app_data/sockets` | UDS only. |
| `FLEZHA_WORKER_POOL_SIZE` | `Runtime.availableProcessors()` | Size of the shared executor that drains every `SegmentSlot` mailbox work-steal-style. |
| `FLEZHA_NATIVE_LIB_DIR` | `$TMPDIR` | Override the directory the SDK extracts `libflz_transport` into. Use this on hardened containers where `/tmp` is noexec. |
