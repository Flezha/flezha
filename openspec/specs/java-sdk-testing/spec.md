# java-sdk-testing Specification

## Purpose

Defines the testing strategy for the Java SDK: unit test coverage of the protocol layer, JNI bridge tests, the reference Java compute engine test app, the Elixir integration harness that launches the Java runtime, the full runtime×transport integration matrix, the one-command local entry point, and CI enforcement.

## Requirements

### Requirement: Unit Test Coverage for Protocol Layer

The Java SDK SHALL include JUnit 5 unit tests that cover every component of the protocol and runtime layer: binary framing (frame build / parse, partial reads, incomplete headers, incomplete payloads, multiple frames per read, oversized payload ids, empty metadata), metadata serialization (round-trip, boundary lengths of 0 and 255, missing metadata, the reserved `channel` key), protobuf encode/decode for every `CommandRequest` oneof and `CommandResponse` shape, command dispatch for every command variant (success and error paths, including the `RelayMessage` canonical-unsupported response), Relay registration (duplicate factoryId, missing factoryId lookup, before/after `newInstance` gating, wiring-mismatch rejection, entry-channel-type rejection, unwired-channel rejection, static-mode and dynamic-mode default-impl detection), Shard dispatch (single-mode partition allocation, static-mode partition fan-out, sticky per-partition ordering, parallel partition execution, inter-shard reference-equality, plugin-bound serialization + channel-name metadata attachment, `emitWithMeta` rejection on inter-shard channels, reserved-key drop), relay-instance lifecycle (add via `AddRelay`, remove via `RemRelay`, state serialize via `PartitionState`, migrate serialize via `MigrationSerialization`, migrate deserialize via `MigrationDeserialization`), and the reconnect state machine (intentional vs unexpected disconnect, backoff schedule).

#### Scenario: Running mvn test passes the unit suite
- **WHEN** a developer runs `mvn -f sdk/java/pom.xml test`
- **THEN** all unit tests SHALL pass and the JaCoCo line coverage report SHALL show ≥ 85% coverage for `flezha-sdk-runtime` and `flezha-sdk-api` modules

#### Scenario: Each java-sdk requirement has at least one unit test
- **WHEN** the unit test suite is enumerated
- **THEN** for every requirement under the `java-sdk` spec there SHALL be at least one JUnit test whose `@DisplayName` or class name references the requirement's scenario, including the new `Relay Registration`, `Shard Interface and Per-Partition Dispatch`, and `Sticky Per-Partition Ordering Under ThreadPool Dispatch` requirements

#### Scenario: Old API tests removed
- **WHEN** the unit test suite is enumerated for tests referencing `RelayHandler`, `RelayRunner.THREAD_SPAWN`, `FactoryRegistry.registerFactory`, `FactoryMetadata`, `InputTransformer`, or `OutputTransformer`
- **THEN** the count SHALL be zero — every such test is either deleted or rewritten against the new `Relay` / `Shard` surface

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

The Java reference test app at `test/apps/java_executor/` SHALL implement three reference Relay classes (`DoNothingRelay`, `StoreInputRelay`, `CompareFilterRelay`) using the Java SDK's `Relay` / `Shard` API. The built jar MUST accept the namespace as its first command-line argument and keep the process alive until stdin closes, matching `rust_executor`'s behavior. The factory ids registered by these Relay classes (via `Relay::factoryId`) SHALL be `"do_nothing"`, `"store_input"`, `"compare_filter"` — exactly the surviving subset that `test/apps/rust_executor/` exposes. The legacy `EchoMessageRelay` (and any other `relay-message-handlers`-driven factory) SHALL NOT be present; its inclusion is deferred until `relay-message-handlers` ships per-Shard `handleMessage`.

#### Scenario: Java test app builds and runs
- **WHEN** `mvn -f test/apps/java_executor/pom.xml package` produces `target/java_executor.jar` and `java -jar target/java_executor.jar my_namespace` is run while the agent is up
- **THEN** the process SHALL register its three Relay classes via `Flezha.registerRelay(...)`, connect to the agent, and receive an `AssignExecutorId` without errors

#### Scenario: Java test app mirrors Rust test app factory set
- **WHEN** the list of factory ids registered by the Java test app is compared to those registered by `test/apps/rust_executor/`
- **THEN** the two lists SHALL be equal sets (`"do_nothing"`, `"store_input"`, `"compare_filter"`); message-handler factories SHALL be absent from both

#### Scenario: Java test app uses the new Shard / ShardCtx API exclusively
- **WHEN** the Java test app's source files are scanned for `import com.flezha.sdk.RelayHandler`, `com.flezha.sdk.FactoryRegistry`, `com.flezha.sdk.RelaySettings`, `com.flezha.sdk.RelayResult`, `com.flezha.sdk.RelayRunner`, `com.flezha.sdk.InputTransformer`, or `com.flezha.sdk.OutputTransformer`
- **THEN** zero matches SHALL be found — the test app's classes use only `Relay`, `Shard`, `ShardCtx`, `ChannelBuilder`, and `Flezha.registerRelay`

### Requirement: Elixir Integration Harness Supports Java Runtime

`test/helpers/node_test_helper.ex` SHALL accept a `:runtime` option (`:rust | :java`, default `:rust`) in `start_executor/2`. When `:java` is passed, the helper MUST launch `java -jar test/apps/java_executor/target/java_executor.jar <namespace>` via `Flezha.Service.PortPilot` with the same event handling and timeout behavior used for the Rust path. All existing callers that pass no runtime option MUST continue to work unchanged.

#### Scenario: Default runtime remains Rust
- **WHEN** existing tests call `start_executor("my_app")` with no runtime option
- **THEN** the helper SHALL launch the Rust test app exactly as it does today, and those tests SHALL continue to pass

#### Scenario: Passing :java launches the Java test app
- **WHEN** a test calls `start_executor("my_app", runtime: :java, transport: :uds)`
- **THEN** the helper SHALL invoke the `java -jar ...` command and return the assigned `executor_id` after the `:executor_connected_event` fires

#### Scenario: Java runtime works over both transports
- **WHEN** tests call `start_executor(ns, runtime: :java, transport: :uds)` and `start_executor(ns, runtime: :java, transport: :quic)`
- **THEN** both calls SHALL succeed and return valid compute engine ids, exercising the full handshake, `AssignExecutorId`, and `ConnectSocket` flow on each transport

### Requirement: Integration Test Matrix

At least one ExUnit integration suite SHALL exercise the Java SDK against the unchanged Elixir agent across every combination of `{runtime: [:rust, :java]} × {transport: [:uds, :quic]}`. The matrix SHALL cover: handshake + `AssignExecutorId`, `ConnectSocket`, add-remove relay lifecycle, end-to-end data flow through a relay (`PipeToAgent`), `PipeToRelay` routing, migration (serialize + deserialize on a second node), and unexpected-disconnect auto-reconnect.

#### Scenario: Matrix suite runs under mix test
- **WHEN** a developer runs `mix test --only java_sdk`
- **THEN** all four combinations of the runtime×transport matrix SHALL run, and each matrix point SHALL assert that relay output received by the agent matches the expected value

#### Scenario: Java-runtime reconnect under agent restart
- **WHEN** the integration test restarts the agent while a Java test compute engine is running
- **THEN** the Java SDK SHALL reconnect automatically, receive a fresh compute engine id, be re-issued relay add commands, and resume correct stream I/O

#### Scenario: Java-runtime migration preserves relay state
- **WHEN** a relay holding non-trivial state is migrated between two compute engines via `MigrationSerialization` + `MigrationDeserialization`
- **THEN** the relay's JSON-serialized state on the destination SHALL be bytewise equal to the serialized state on the source

### Requirement: One-Command Local Test Entry Point

The repository SHALL expose a single Mix alias `mix test.java_sdk` that (a) builds the `flz_transport` host-triple native binary, (b) builds the Java SDK and the Java test compute engine via Maven, (c) runs `mix test --only java_sdk` to execute the integration matrix. A developer SHOULD NOT need to run Maven manually before `mix test`.

#### Scenario: Single command from a clean clone runs the Java integration tests
- **WHEN** a developer with Java 21 and a Rust toolchain installed runs `mix test.java_sdk` on a fresh clone
- **THEN** the command SHALL build the native binary, build both the Java SDK and the test jar via Maven, and run the Java integration test matrix to completion

#### Scenario: Stale Java test jar is rebuilt before tests
- **WHEN** a developer edits a Java source file in `test/apps/java_executor/` and then runs `mix test.java_sdk`
- **THEN** Maven SHALL rebuild the test jar before `mix test --only java_sdk` launches any compute engine

### Requirement: CI Enforces the Full Java SDK Test Suite

The project CI workflow SHALL run the Java SDK unit tests, the JNI bridge tests, and the full Java-runtime integration matrix on every pull request that touches `sdk/java/**`, `flz_transport/**`, `test/apps/java_executor/**`, or `test/helpers/node_test_helper.ex`. Failures in any of these SHALL block the PR. After this change merges, every CI matrix entry, integration test, and `mix test --only java_sdk` invocation previously tagged `RELAY_MVP_JAVA_PENDING` SHALL be re-enabled (the tag is grep-and-untagged in one pass; see `relay-mvp-java/design.md` Decision 11). Tests tagged `RELAY_MESSAGE_HANDLERS_PENDING` SHALL remain disabled until `relay-message-handlers` ships.

#### Scenario: PR touching flz_transport runs the full Java suite
- **WHEN** a pull request modifies a file under `flz_transport/src/`
- **THEN** CI SHALL run both the Rust SDK tests and the full Java SDK test suite (unit + JNI + integration matrix) — neither suite SHALL be skipped on the basis of `RELAY_MVP_JAVA_PENDING`

#### Scenario: Failing Java integration test blocks merge
- **WHEN** any integration scenario under `--only java_sdk` fails
- **THEN** the CI check for that workflow SHALL report a failure and the PR merge SHALL be blocked until the scenario passes

#### Scenario: No RELAY_MVP_JAVA_PENDING tags remain post-merge
- **WHEN** the repository is grep-searched for the literal string `RELAY_MVP_JAVA_PENDING` after this change merges
- **THEN** zero matches SHALL be found across `.github/`, `test/`, `lib/mix/`, `mix.exs`, and `sdk/java/**`

#### Scenario: RELAY_MESSAGE_HANDLERS_PENDING tags persist post-merge
- **WHEN** the repository is grep-searched for the literal string `RELAY_MESSAGE_HANDLERS_PENDING` after this change merges
- **THEN** the matches SHALL match the pre-change set — this change does NOT re-enable message-handler tests

### Requirement: ShardTestHarness in flezha-sdk-testing artifact

The `flezha-sdk-testing` Maven artifact SHALL export `com.flezha.sdk.testing.ShardTestHarness<Input, State, Output, Change extends com.flezha.sdk.state.Change>` as a public type. The harness SHALL support:

- `static <I, S, O, C extends Change> ShardTestHarness<I, S, O, C> of(Shard<I, S, O, C, ?, ?> shard)` — constructs a harness with state from `shard.initState(Optional.empty(), Optional.empty())`.
- `withInitialState(State state)` — explicit initial state.
- `withPayload(Optional<String> shardKey, RestoreInitPayload payload)` — replicates the runtime's snapshot+changelog restoration flow.
- `applyInputs(Iterable<Input> inputs)` — invokes `Shard.run` for each input on the harness's owned state, recording emits and Changes; returns `this` for chaining.
- `expectChanges(List<Change> expected)` — asserts equality, throws `AssertionError` with diagnostic on mismatch, returns `this`.
- `expectEmits(Class<C> type, List<C> expected)` — asserts emits of type `C` equal expected, returns `this`.
- `expectState(State expected)` — asserts state equality (via Kryo bytes), returns `this`.
- `replay(State initial, List<Change> log) -> State` — folds the log via `Shard.applyChange`, returns terminal state.
- `replayEquals() -> Optional<ReplayMismatch>` — replays self.recordedChanges against a re-constructed initial state and asserts byte-equality (via Kryo round-trip) with self.state. Empty optional = pass; non-empty carries `expectedBytes` and `actualBytes` for the test failure message.

#### Scenario: Harness asserts on recorded Changes

- **WHEN** a JUnit test runs `ShardTestHarness.of(new CounterShard()).applyInputs(List.of(new Input.Add(5), new Input.Add(3))).expectChanges(List.of(new CounterChange(5), new CounterChange(3)))`
- **THEN** the chain SHALL succeed with no `AssertionError` thrown; the harness's `recordedChanges` SHALL equal the expected list

#### Scenario: Harness replays for determinism check

- **WHEN** a JUnit test runs `ShardTestHarness.of(new CounterShard()).applyInputs(inputs).replayEquals()` against a deterministic `applyChange`
- **THEN** the result SHALL be `Optional.empty()` and the test SHALL pass

#### Scenario: Harness detects non-deterministic applyChange

- **WHEN** a Shard's `applyChange` reads `System.currentTimeMillis()` and uses it to seed a state field, AND the harness runs `replayEquals()`
- **THEN** the result SHALL be `Optional.of(new ReplayMismatch(expectedBytes, actualBytes))` whose `toString()` renders a hex-diff identifying the divergent bytes

#### Scenario: Harness compiles only for testing-scope dependencies

- **WHEN** a downstream Java module depends on `flezha-sdk-testing` with `<scope>test</scope>` AND attempts to use `ShardTestHarness` from `src/main/java/`
- **THEN** Maven SHALL fail the compile with a "package not visible at compile scope" error; the harness SHALL be visible only from `src/test/java/`

### Requirement: ShardTestHarness JUnit assertions are AssertJ-style chainable

The `ShardTestHarness` API SHALL be method-chainable (each `expect*` method returns `this`), matching the AssertJ idiom that JUnit-using Java developers expect. Failures SHALL throw `AssertionError` (or a subclass) with a descriptive message including: the expected value, the actual value, and a per-element diff for collection assertions.

#### Scenario: Chained assertions are evaluated in order

- **WHEN** a test runs `harness.applyInputs(inputs).expectChanges(c).expectEmits(C.class, e).expectState(s)`
- **THEN** each assertion SHALL evaluate in chain order; the FIRST failing assertion SHALL throw with its own diagnostic; subsequent assertions SHALL NOT run

#### Scenario: Collection-mismatch shows per-element diff

- **WHEN** `expectChanges(List.of(new CounterChange(5), new CounterChange(3)))` is called and the actual recorded Changes are `List.of(new CounterChange(5), new CounterChange(7))`
- **THEN** the thrown `AssertionError`'s message SHALL include the expected list, the actual list, AND a marker pointing at index 1 (the differing element)

### Requirement: ShardTestHarness uses KryoCodec for byte-equality state comparison

`ShardTestHarness.replayEquals()` and `ShardTestHarness.expectState()` SHALL serialize both the actual and expected `State` values via `KryoCodec` (constructed from the Shard's `kryoTypes()`) and compare the resulting `byte[]` arrays via `Arrays.equals`. This avoids requiring `State` to override `equals` correctly (Java's default `equals` is reference equality; record `equals` works but user POJO `equals` is often broken).

The Shard's `kryoTypes()` SHALL include all classes reachable from `State` for `replayEquals` and `expectState` to work; missing types SHALL throw `StateSerializeException::UnregisteredClass(className)` from the Kryo round-trip, with a diagnostic message naming the missing class.

#### Scenario: Record State works without registration of inner types

- **WHEN** a Shard declares `State = OhlcState` (a record carrying primitives only) AND `kryoTypes() = List.of(OhlcState.class)`
- **THEN** `replayEquals()` SHALL succeed via Kryo bytes equality

#### Scenario: State referencing un-registered class throws diagnostic

- **WHEN** a Shard declares `State = OrderBookState` containing a `Map<Long, Level>` where `Level` is not in `kryoTypes()`
- **THEN** `replayEquals()` SHALL throw `StateSerializeException::UnregisteredClass("Level")` with a clear pointer to the missing registration

### Requirement: ShardTestHarness records emits without invoking the wire path

The harness SHALL provide a `TestShardCtx` implementation of `ShardCtx` that records `emit` and `emitWithMeta` calls into a `List<EmitRecord>` accessible via `harness.recordedEmits()`. The test ctx SHALL NOT invoke `NativeTransport.*` or any JNI bridge; the emits exist only in-memory.

#### Scenario: emit calls recorded but not shipped

- **WHEN** a test runs `harness.applyInputs(inputs)` against a Shard that emits a `Candle` per input
- **THEN** for each input that produced an emit, `harness.recordedEmits()` SHALL contain one `EmitRecord(channel: <name>, value: Candle, metadata: <map>)`; `NativeTransport.*` SHALL NOT be invoked

#### Scenario: emitWithMeta records the metadata

- **WHEN** a Shard calls `ctx.emitWithMeta(value, Map.of("trace_id", "abc"))` on a plugin-bound channel
- **THEN** the recorded `EmitRecord.metadata` SHALL contain `{"trace_id": "abc"}` (the SDK's auto-attached `flezha.dedup_key` SHALL also be present in the metadata, because the test ctx mirrors the runtime's metadata-stamping behavior)

### Requirement: At least one harness test per java-sdk requirement scenario

The Java SDK test suite (`mvn -f sdk/java test`) SHALL include at least one harness-based test per scenario in the `java-sdk` capability's persistence-related requirements added by `relay-state-persistence-java`. Scenarios SHALL be enumerated from the spec's `#### Scenario:` blocks and verified at the test level.

#### Scenario: Coverage report enumerates per-scenario harness tests

- **WHEN** `mvn -f sdk/java test` runs and produces JaCoCo + JUnit reports
- **THEN** the JUnit report SHALL include test methods named after each scenario from the persistence requirements (e.g. `testDefaultChangeIsNoChange_skipsPersistence`, `testStateApplyRunsApplyChangeAndRecords`, `testCoalesceCollapsesSameKey`, ...) — at least one test method per scenario

#### Scenario: Coverage gap fails CI pre-merge

- **WHEN** a new persistence-related scenario is added to a `java-sdk` requirement and no corresponding harness test exists
- **THEN** the spec/test sync check (run as part of `mvn -f sdk/java verify`) SHALL fail and block merge with a message naming the missing scenario; this matches the existing pattern from `java-sdk-testing` "Each java-sdk requirement has at least one unit test"
