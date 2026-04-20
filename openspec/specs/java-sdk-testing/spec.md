# java-sdk-testing Specification

## Purpose

Defines the testing strategy for the Java SDK: unit test coverage of the protocol layer, JNI bridge tests, the reference Java compute engine test app, the Elixir integration harness that launches the Java runtime, the full runtime×transport integration matrix, the one-command local entry point, and CI enforcement.

## Requirements

### Requirement: Unit Test Coverage for Protocol Layer

The Java SDK SHALL include JUnit 5 unit tests that cover every component of the protocol and runtime layer: binary framing (frame build / parse, partial reads, incomplete headers, incomplete payloads, multiple frames per read, oversized payload ids, empty metadata), metadata serialization (round-trip, boundary lengths of 0 and 255, missing metadata), protobuf encode/decode for every `CommandRequest` oneof and `CommandResponse` shape, command dispatch for every command variant (success and error paths), factory registration (duplicate id, missing id lookup, before/after `newInstance` gating), processor lifecycle (add, remove, state serialize, migrate serialize, migrate deserialize), and the reconnect state machine (intentional vs unexpected disconnect, backoff schedule).

#### Scenario: Running mvn test passes the unit suite
- **WHEN** a developer runs `mvn -f sdk/java/pom.xml test`
- **THEN** all unit tests SHALL pass and the JaCoCo line coverage report SHALL show ≥ 85% coverage for `flezha-sdk-runtime` and `flezha-sdk-api` modules

#### Scenario: Each SDK protocol requirement has at least one unit test
- **WHEN** the unit test suite is enumerated
- **THEN** for every requirement under the `java-sdk` spec there SHALL be at least one JUnit test whose `@DisplayName` or class name references the requirement's scenario

### Requirement: JNI Bridge Tests

The `flezha-sdk-native` module SHALL include dedicated JNI bridge tests that exercise the native layer in isolation: context create/destroy, config defaulting, log callback registration and invocation, frame build and free, version query. These tests MUST run without a live agent and MUST use only the host-triple native binary.

#### Scenario: Round-trip frame build through JNI matches pure-Java build
- **WHEN** the same `(payload_id, message, metadata)` triple is built by both the pure-Java framing code and by `NativeTransport.buildFrame(...)` (which delegates to `flezha_frame_build`)
- **THEN** the two produced byte arrays SHALL be bytewise equal

#### Scenario: Log callback invoked from native code
- **WHEN** a Java-side `FlezhaLogCallback` is registered via the JNI bridge, and native code invokes the log callback with level INFO and message `"test"`
- **THEN** the Java callback SHALL receive one invocation with `level = INFO` and `message = "test"`

#### Scenario: Context destroy is idempotent
- **WHEN** `NativeTransport.destroyContext(handle)` is called twice on the same handle
- **THEN** the first call SHALL succeed and the second SHALL return an `INVALID_STATE` status without crashing the JVM

### Requirement: Reference Java Compute Engine Test App

The change SHALL introduce `test/apps/java_compute_engine/` — a Maven project that mirrors `test/apps/rust_compute_engine/` and implements the same three reference processors (`DoNothingProcessor`, `StoreInputProcessor`, `CompareFilterProcessor`) using the Java SDK. The built jar MUST accept the namespace as its first command-line argument and keep the process alive until stdin closes, matching `rust_compute_engine`'s behavior.

#### Scenario: Java test app builds and runs
- **WHEN** `mvn -f test/apps/java_compute_engine/pom.xml package` produces `target/java_compute_engine.jar` and `java -jar target/java_compute_engine.jar my_namespace` is run while the agent is up
- **THEN** the process SHALL register its three factories, connect to the agent, and receive an `AssignComputeEngineId` without errors

#### Scenario: Java test app mirrors Rust test app processor set
- **WHEN** the list of factory ids registered by the Java test app is compared to those registered by `test/apps/rust_compute_engine/`
- **THEN** the two lists SHALL be identical (same factory ids, same corresponding processor semantics)

### Requirement: Elixir Integration Harness Supports Java Runtime

`test/helpers/node_test_helper.ex` SHALL accept a `:runtime` option (`:rust | :java`, default `:rust`) in `start_compute_engine/2`. When `:java` is passed, the helper MUST launch `java -jar test/apps/java_compute_engine/target/java_compute_engine.jar <namespace>` via `Flezha.Service.PortPilot` with the same event handling and timeout behavior used for the Rust path. All existing callers that pass no runtime option MUST continue to work unchanged.

#### Scenario: Default runtime remains Rust
- **WHEN** existing tests call `start_compute_engine("my_app")` with no runtime option
- **THEN** the helper SHALL launch the Rust test app exactly as it does today, and those tests SHALL continue to pass

#### Scenario: Passing :java launches the Java test app
- **WHEN** a test calls `start_compute_engine("my_app", runtime: :java, transport: :uds)`
- **THEN** the helper SHALL invoke the `java -jar ...` command and return the assigned `compute_engine_id` after the `:compute_engine_connected_event` fires

#### Scenario: Java runtime works over both transports
- **WHEN** tests call `start_compute_engine(ns, runtime: :java, transport: :uds)` and `start_compute_engine(ns, runtime: :java, transport: :quic)`
- **THEN** both calls SHALL succeed and return valid compute engine ids, exercising the full handshake, `AssignComputeEngineId`, and `ConnectSocket` flow on each transport

### Requirement: Integration Test Matrix

At least one ExUnit integration suite SHALL exercise the Java SDK against the unchanged Elixir agent across every combination of `{runtime: [:rust, :java]} × {transport: [:uds, :quic]}`. The matrix SHALL cover: handshake + `AssignComputeEngineId`, `ConnectSocket`, add-remove processor lifecycle, end-to-end data flow through a processor (`PipeToAgent`), `PipeToProcessor` routing, migration (serialize + deserialize on a second node), and unexpected-disconnect auto-reconnect.

#### Scenario: Matrix suite runs under mix test
- **WHEN** a developer runs `mix test --only java_sdk`
- **THEN** all four combinations of the runtime×transport matrix SHALL run, and each matrix point SHALL assert that processor output received by the agent matches the expected value

#### Scenario: Java-runtime reconnect under agent restart
- **WHEN** the integration test restarts the agent while a Java test compute engine is running
- **THEN** the Java SDK SHALL reconnect automatically, receive a fresh compute engine id, be re-issued processor add commands, and resume correct stream I/O

#### Scenario: Java-runtime migration preserves processor state
- **WHEN** a processor holding non-trivial state is migrated between two compute engines via `MigrationSerialization` + `MigrationDeserialization`
- **THEN** the processor's JSON-serialized state on the destination SHALL be bytewise equal to the serialized state on the source

### Requirement: One-Command Local Test Entry Point

The repository SHALL expose a single Mix alias `mix test.java_sdk` that (a) builds the `flz_transport` host-triple native binary, (b) builds the Java SDK and the Java test compute engine via Maven, (c) runs `mix test --only java_sdk` to execute the integration matrix. A developer SHOULD NOT need to run Maven manually before `mix test`.

#### Scenario: Single command from a clean clone runs the Java integration tests
- **WHEN** a developer with Java 21 and a Rust toolchain installed runs `mix test.java_sdk` on a fresh clone
- **THEN** the command SHALL build the native binary, build both the Java SDK and the test jar via Maven, and run the Java integration test matrix to completion

#### Scenario: Stale Java test jar is rebuilt before tests
- **WHEN** a developer edits a Java source file in `test/apps/java_compute_engine/` and then runs `mix test.java_sdk`
- **THEN** Maven SHALL rebuild the test jar before `mix test --only java_sdk` launches any compute engine

### Requirement: CI Enforces the Full Java SDK Test Suite

The project CI workflow SHALL run the Java SDK unit tests, the JNI bridge tests, and the full Java-runtime integration matrix on every pull request that touches `sdk/java/**`, `flz_transport/**`, `test/apps/java_compute_engine/**`, or `test/helpers/node_test_helper.ex`. Failures in any of these SHALL block the PR.

#### Scenario: PR touching flz_transport runs the full Java suite
- **WHEN** a pull request modifies a file under `flz_transport/src/`
- **THEN** CI SHALL run both the Rust SDK tests and the full Java SDK test suite (unit + JNI + integration matrix)

#### Scenario: Failing Java integration test blocks merge
- **WHEN** any integration scenario under `--only java_sdk` fails
- **THEN** the CI check for that workflow SHALL report a failure and the PR merge SHALL be blocked until the scenario passes
