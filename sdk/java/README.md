# Flezha Java SDK

Java 21 SDK for authoring Flezha compute engines. Feature parity with the
Rust SDK (`sdk/rust/`), talks to the existing Elixir agent through JNI
bindings over `flz_transport`. Supports both UDS and QUIC transports.

## Quickstart

```java
public class EchoHandler implements ProcessorHandler<EchoState, String, String> {
    @Override
    public String process(String input, EchoState state) {
        return input;
    }
}

public static void main(String[] args) {
    ProcessorSettings<EchoState, String, String> settings =
        ProcessorSettings.<EchoState, String, String>builder()
            .inputTransformer(InputTransformers.text())
            .outputTransformer(OutputTransformers.pipeToAgentText())
            .runner(ProcessorRunner.THREAD_POOL)
            .build();

    FactoryRegistry.registerFactory("echo", settings, new EchoState(), EchoHandler::new);

    Flezha sdk = Flezha.newInstance(args[0]);
    System.out.println("assigned id: " + sdk.getId());
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
| `flezha-sdk-api` | Public API: `Flezha`, `FactoryRegistry`, `ProcessorHandler`, sealed input/output/result types, transformers. No native dependency. |
| `flezha-sdk-protobuf` | Java classes generated from `protobufs/compute_engine_commands.proto`. Pure generated code; no hand-written sources. |
| `flezha-sdk-runtime` | Control plane, data plane, framing, worker pool, reconnect, command dispatch. |
| `flezha-sdk-native` | JNI bindings (`NativeTransport`) + bundled per-OS `libflz_transport` native binaries. |
| `flezha-sdk` | Fat-jar aggregator — the published artifact. |
| `flezha-sdk-integration-tests` | End-to-end tests against a running agent (typically driven by the Elixir harness via `mix test.java_sdk`). |

## Running tests

Unit tests (no agent required):

```
mvn -f sdk/java/pom.xml test
```

With coverage report (JaCoCo):

```
mvn -f sdk/java/pom.xml -P coverage verify
```

Integration tests (against a live agent):

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
| `FLEZHA_WORKER_POOL_SIZE` | `Runtime.availableProcessors()` | Size of the shared executor for `THREAD_POOL` runners. |
| `FLEZHA_NATIVE_LIB_DIR` | `$TMPDIR` | Override the directory the SDK extracts `libflz_transport` into. Use this on hardened containers where `/tmp` is noexec. |
