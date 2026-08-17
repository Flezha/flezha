# java-sdk-testing Specification

## Purpose

Defines the testing strategy for the Java SDK: unit test coverage of the protocol layer, JNI bridge tests, the reference Java compute engine test app, the Elixir integration harness that launches the Java runtime, the full runtime×transport integration matrix, the one-command local entry point, and CI enforcement.
## Requirements
### Requirement: Unit Test Coverage for Protocol Layer

The Java SDK SHALL include JUnit 5 unit tests that cover every component of the protocol and runtime layer: binary framing (frame build / parse, partial reads, incomplete headers, incomplete payloads, multiple frames per read, oversized payload ids, empty metadata), metadata serialization (round-trip, boundary lengths of 0 and 255, missing metadata, the reserved `channel` key), protobuf encode/decode for every `CommandRequest` oneof and `CommandResponse` shape, command dispatch for every command variant (success and error paths, including the `OperativeMessage` canonical-unsupported response), Operative registration (duplicate factoryId, missing factoryId lookup, before/after `newInstance` gating, wiring-mismatch rejection, entry-channel-type rejection, unwired-channel rejection, static-mode and dynamic-mode default-impl detection), Procedure dispatch (single-mode entity allocation, static-mode entity fan-out, sticky per-entity ordering, parallel entity execution, inter-procedure reference-equality, plugin-bound serialization + channel-name metadata attachment, `emitWithMeta` rejection on inter-procedure channels, reserved-key drop), operative-instance lifecycle (add via `AddOperative`, remove via `RemOperative`, state serialize via `EntityState`, migrate serialize via `MigrationSerialization`, migrate deserialize via `MigrationDeserialization`), and the reconnect state machine (intentional vs unexpected disconnect, backoff schedule).

#### Scenario: Running mvn test passes the unit suite
- **WHEN** a developer runs `mvn -f sdk/java/pom.xml test`
- **THEN** all unit tests SHALL pass and the JaCoCo line coverage report SHALL show ≥ 85% coverage for `flezha-sdk-runtime` and `flezha-sdk-api` modules

#### Scenario: Each java-sdk requirement has at least one unit test
- **WHEN** the unit test suite is enumerated
- **THEN** for every requirement under the `java-sdk` spec there SHALL be at least one JUnit test whose `@DisplayName` or class name references the requirement's scenario, including the new `Operative Registration`, `Procedure Interface and Per-Entity Dispatch`, and `Sticky Per-Entity Ordering Under ThreadPool Dispatch` requirements

#### Scenario: Old API tests removed
- **WHEN** the unit test suite is enumerated for tests referencing `OperativeHandler`, `OperativeRunner.THREAD_SPAWN`, `FactoryRegistry.registerFactory`, `FactoryMetadata`, `InputTransformer`, or `OutputTransformer`
- **THEN** the count SHALL be zero — every such test is either deleted or rewritten against the new `StreamOperative` / `Procedure` surface

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

The Java reference test app at `test/apps/java_runner/` SHALL implement three reference Operative classes (`DoNothingOperative`, `StoreInputOperative`, `CompareFilterOperative`) using the Java SDK's `StreamOperative` / `Procedure` API. The built jar MUST accept the namespace as its first command-line argument and keep the process alive until stdin closes, matching `rust_runner`'s behavior. The factory ids registered by these Operative classes (via `StreamOperative::factoryId`) SHALL be `"do_nothing"`, `"store_input"`, `"compare_filter"` — exactly the surviving subset that `test/apps/rust_runner/` exposes. The legacy `EchoMessageOperative` (and any other `operative-message-handlers`-driven factory) SHALL NOT be present; its inclusion is deferred until `operative-message-handlers` ships per-Procedure `handleMessage`.

#### Scenario: Java test app builds and runs
- **WHEN** `mvn -f test/apps/java_runner/pom.xml package` produces `target/java_runner.jar` and `java -jar target/java_runner.jar my_namespace` is run while the agent is up
- **THEN** the process SHALL register its three Operative classes via `Flezha.registerStream(...)`, connect to the agent, and receive an `AssignRunnerId` without errors

#### Scenario: Java test app mirrors Rust test app factory set
- **WHEN** the list of factory ids registered by the Java test app is compared to those registered by `test/apps/rust_runner/`
- **THEN** the two lists SHALL be equal sets (`"do_nothing"`, `"store_input"`, `"compare_filter"`); message-handler factories SHALL be absent from both

#### Scenario: Java test app uses the new Procedure / ProcedureCtx API exclusively
- **WHEN** the Java test app's source files are scanned for `import com.flezha.sdk.OperativeHandler`, `com.flezha.sdk.FactoryRegistry`, `com.flezha.sdk.OperativeSettings`, `com.flezha.sdk.OperativeResult`, `com.flezha.sdk.OperativeRunner`, `com.flezha.sdk.InputTransformer`, or `com.flezha.sdk.OutputTransformer`
- **THEN** zero matches SHALL be found — the test app's classes use only `StreamOperative`, `Procedure`, `ProcedureCtx`, `ChannelBuilder`, and `Flezha.registerStream`

### Requirement: Elixir Integration Harness Supports Java Runtime

`test/helpers/node_test_helper.ex` SHALL accept a `:runtime` option (`:rust | :java`, default `:rust`) in `start_runner/2`. When `:java` is passed, the helper MUST launch `java -jar test/apps/java_runner/target/java_runner.jar <namespace>` via `Flezha.Service.PortPilot` with the same event handling and timeout behavior used for the Rust path. All existing callers that pass no runtime option MUST continue to work unchanged.

#### Scenario: Default runtime remains Rust
- **WHEN** existing tests call `start_runner("my_app")` with no runtime option
- **THEN** the helper SHALL launch the Rust test app exactly as it does today, and those tests SHALL continue to pass

#### Scenario: Passing :java launches the Java test app
- **WHEN** a test calls `start_runner("my_app", runtime: :java, transport: :uds)`
- **THEN** the helper SHALL invoke the `java -jar ...` command and return the assigned `runner_id` after the `:runner_connected_event` fires

#### Scenario: Java runtime works over both transports
- **WHEN** tests call `start_runner(ns, runtime: :java, transport: :uds)` and `start_runner(ns, runtime: :java, transport: :quic)`
- **THEN** both calls SHALL succeed and return valid compute engine ids, exercising the full handshake, `AssignRunnerId`, and `ConnectSocket` flow on each transport

### Requirement: Integration Test Matrix

At least one ExUnit integration suite SHALL exercise the Java SDK against the unchanged Elixir agent across every combination of `{runtime: [:rust, :java]} × {transport: [:uds, :quic]}`. The matrix SHALL cover: handshake + `AssignRunnerId`, `ConnectSocket`, add-remove operative lifecycle, end-to-end data flow through an operative (`PipeToAgent`), `PipeToOperative` routing, migration (serialize + deserialize on a second node), and unexpected-disconnect auto-reconnect.

#### Scenario: Matrix suite runs under mix test
- **WHEN** a developer runs `mix test --only java_sdk`
- **THEN** all four combinations of the runtime×transport matrix SHALL run, and each matrix point SHALL assert that operative output received by the agent matches the expected value

#### Scenario: Java-runtime reconnect under agent restart
- **WHEN** the integration test restarts the agent while a Java test compute engine is running
- **THEN** the Java SDK SHALL reconnect automatically, receive a fresh compute engine id, be re-issued operative add commands, and resume correct stream I/O

#### Scenario: Java-runtime migration preserves operative state
- **WHEN** an operative holding non-trivial state is migrated between two compute engines via `MigrationSerialization` + `MigrationDeserialization`
- **THEN** the operative's JSON-serialized state on the destination SHALL be bytewise equal to the serialized state on the source

### Requirement: One-Command Local Test Entry Point

The repository SHALL expose a single Mix alias `mix test.java_sdk` that (a) builds the `flz_transport` host-triple native binary, (b) builds the Java SDK and the Java test compute engine via Maven, (c) runs `mix test --only java_sdk` to execute the integration matrix. A developer SHOULD NOT need to run Maven manually before `mix test`.

#### Scenario: Single command from a clean clone runs the Java integration tests
- **WHEN** a developer with Java 21 and a Rust toolchain installed runs `mix test.java_sdk` on a fresh clone
- **THEN** the command SHALL build the native binary, build both the Java SDK and the test jar via Maven, and run the Java integration test matrix to completion

#### Scenario: Stale Java test jar is rebuilt before tests
- **WHEN** a developer edits a Java source file in `test/apps/java_runner/` and then runs `mix test.java_sdk`
- **THEN** Maven SHALL rebuild the test jar before `mix test --only java_sdk` launches any compute engine

### Requirement: CI Enforces the Full Java SDK Test Suite

The project CI workflow SHALL run the Java SDK unit tests, the JNI bridge tests, and the full Java-runtime integration matrix on every pull request that touches `sdk/java/**`, `flz_transport/**`, `test/apps/java_runner/**`, or `test/helpers/node_test_helper.ex`. Failures in any of these SHALL block the PR. After this change merges, every CI matrix entry, integration test, and `mix test --only java_sdk` invocation previously tagged `OPERATIVE_MVP_JAVA_PENDING` SHALL be re-enabled (the tag is grep-and-untagged in one pass; see `operative-mvp-java/design.md` Decision 11). Tests tagged `OPERATIVE_MESSAGE_HANDLERS_PENDING` SHALL remain disabled until `operative-message-handlers` ships.

#### Scenario: PR touching flz_transport runs the full Java suite
- **WHEN** a pull request modifies a file under `flz_transport/src/`
- **THEN** CI SHALL run both the Rust SDK tests and the full Java SDK test suite (unit + JNI + integration matrix) — neither suite SHALL be skipped on the basis of `OPERATIVE_MVP_JAVA_PENDING`

#### Scenario: Failing Java integration test blocks merge
- **WHEN** any integration scenario under `--only java_sdk` fails
- **THEN** the CI check for that workflow SHALL report a failure and the PR merge SHALL be blocked until the scenario passes

#### Scenario: No OPERATIVE_MVP_JAVA_PENDING tags remain post-merge
- **WHEN** the repository is grep-searched for the literal string `OPERATIVE_MVP_JAVA_PENDING` after this change merges
- **THEN** zero matches SHALL be found across `.github/`, `test/`, `lib/mix/`, `mix.exs`, and `sdk/java/**`

#### Scenario: OPERATIVE_MESSAGE_HANDLERS_PENDING tags persist post-merge
- **WHEN** the repository is grep-searched for the literal string `OPERATIVE_MESSAGE_HANDLERS_PENDING` after this change merges
- **THEN** the matches SHALL match the pre-change set — this change does NOT re-enable message-handler tests

### Requirement: ProcedureTestHarness in flezha-sdk-testing artifact

The `flezha-sdk-testing` Maven artifact SHALL export `com.flezha.sdk.testing.ProcedureTestHarness<Input, State, Output, Change extends com.flezha.sdk.state.Change>` as a public type. The harness SHALL support:

- `static <I, S, O, C extends Change> ProcedureTestHarness<I, S, O, C> of(Procedure<I, S, O, C, ?, ?> procedure)` — constructs a harness with state from `procedure.initState(Optional.empty(), Optional.empty())`.
- `withInitialState(State state)` — explicit initial state.
- `withPayload(Optional<String> entityKey, RestoreInitPayload payload)` — replicates the runtime's snapshot+changelog restoration flow.
- `applyInputs(Iterable<Input> inputs)` — invokes `Procedure.run` for each input on the harness's owned state, recording emits and Changes; returns `this` for chaining.
- `expectChanges(List<Change> expected)` — asserts equality, throws `AssertionError` with diagnostic on mismatch, returns `this`.
- `expectEmits(Class<C> type, List<C> expected)` — asserts emits of type `C` equal expected, returns `this`.
- `expectState(State expected)` — asserts state equality (via Kryo bytes), returns `this`.
- `replay(State initial, List<Change> log) -> State` — folds the log via `Procedure.applyChange`, returns terminal state.
- `replayEquals() -> Optional<ReplayMismatch>` — replays self.recordedChanges against a re-constructed initial state and asserts byte-equality (via Kryo round-trip) with self.state. Empty optional = pass; non-empty carries `expectedBytes` and `actualBytes` for the test failure message.

#### Scenario: Harness asserts on recorded Changes

- **WHEN** a JUnit test runs `ProcedureTestHarness.of(new CounterProcedure()).applyInputs(List.of(new Input.Add(5), new Input.Add(3))).expectChanges(List.of(new CounterChange(5), new CounterChange(3)))`
- **THEN** the chain SHALL succeed with no `AssertionError` thrown; the harness's `recordedChanges` SHALL equal the expected list

#### Scenario: Harness replays for determinism check

- **WHEN** a JUnit test runs `ProcedureTestHarness.of(new CounterProcedure()).applyInputs(inputs).replayEquals()` against a deterministic `applyChange`
- **THEN** the result SHALL be `Optional.empty()` and the test SHALL pass

#### Scenario: Harness detects non-deterministic applyChange

- **WHEN** a Procedure's `applyChange` reads `System.currentTimeMillis()` and uses it to seed a state field, AND the harness runs `replayEquals()`
- **THEN** the result SHALL be `Optional.of(new ReplayMismatch(expectedBytes, actualBytes))` whose `toString()` renders a hex-diff identifying the divergent bytes

#### Scenario: Harness compiles only for testing-scope dependencies

- **WHEN** a downstream Java module depends on `flezha-sdk-testing` with `<scope>test</scope>` AND attempts to use `ProcedureTestHarness` from `src/main/java/`
- **THEN** Maven SHALL fail the compile with a "package not visible at compile scope" error; the harness SHALL be visible only from `src/test/java/`

### Requirement: ProcedureTestHarness JUnit assertions are AssertJ-style chainable

The `ProcedureTestHarness` API SHALL be method-chainable (each `expect*` method returns `this`), matching the AssertJ idiom that JUnit-using Java developers expect. Failures SHALL throw `AssertionError` (or a subclass) with a descriptive message including: the expected value, the actual value, and a per-element diff for collection assertions.

#### Scenario: Chained assertions are evaluated in order

- **WHEN** a test runs `harness.applyInputs(inputs).expectChanges(c).expectEmits(C.class, e).expectState(s)`
- **THEN** each assertion SHALL evaluate in chain order; the FIRST failing assertion SHALL throw with its own diagnostic; subsequent assertions SHALL NOT run

#### Scenario: Collection-mismatch shows per-element diff

- **WHEN** `expectChanges(List.of(new CounterChange(5), new CounterChange(3)))` is called and the actual recorded Changes are `List.of(new CounterChange(5), new CounterChange(7))`
- **THEN** the thrown `AssertionError`'s message SHALL include the expected list, the actual list, AND a marker pointing at index 1 (the differing element)

### Requirement: ProcedureTestHarness uses KryoCodec for byte-equality state comparison

`ProcedureTestHarness.replayEquals()` and `ProcedureTestHarness.expectState()` SHALL serialize both the actual and expected `State` values via `KryoCodec` (constructed from the Procedure's `kryoTypes()`) and compare the resulting `byte[]` arrays via `Arrays.equals`. This avoids requiring `State` to override `equals` correctly (Java's default `equals` is reference equality; record `equals` works but user POJO `equals` is often broken).

The Procedure's `kryoTypes()` SHALL include all classes reachable from `State` for `replayEquals` and `expectState` to work; missing types SHALL throw `StateSerializeException::UnregisteredClass(className)` from the Kryo round-trip, with a diagnostic message naming the missing class.

#### Scenario: Record State works without registration of inner types

- **WHEN** a Procedure declares `State = OhlcState` (a record carrying primitives only) AND `kryoTypes() = List.of(OhlcState.class)`
- **THEN** `replayEquals()` SHALL succeed via Kryo bytes equality

#### Scenario: State referencing un-registered class throws diagnostic

- **WHEN** a Procedure declares `State = OrderBookState` containing a `Map<Long, Level>` where `Level` is not in `kryoTypes()`
- **THEN** `replayEquals()` SHALL throw `StateSerializeException::UnregisteredClass("Level")` with a clear pointer to the missing registration

### Requirement: ProcedureTestHarness records emits without invoking the wire path

The harness SHALL provide a `TestProcedureCtx` implementation of `ProcedureCtx` that records `emit` and `emitWithMeta` calls into a `List<EmitRecord>` accessible via `harness.recordedEmits()`. The test ctx SHALL NOT invoke `NativeTransport.*` or any JNI bridge; the emits exist only in-memory.

#### Scenario: emit calls recorded but not shipped

- **WHEN** a test runs `harness.applyInputs(inputs)` against a Procedure that emits a `Candle` per input
- **THEN** for each input that produced an emit, `harness.recordedEmits()` SHALL contain one `EmitRecord(channel: <name>, value: Candle, metadata: <map>)`; `NativeTransport.*` SHALL NOT be invoked

#### Scenario: emitWithMeta records the metadata

- **WHEN** a Procedure calls `ctx.emitWithMeta(value, Map.of("trace_id", "abc"))` on a plugin-bound channel
- **THEN** the recorded `EmitRecord.metadata` SHALL contain `{"trace_id": "abc"}` (the SDK's auto-attached `flezha.dedup_key` SHALL also be present in the metadata, because the test ctx mirrors the runtime's metadata-stamping behavior)

### Requirement: At least one harness test per java-sdk requirement scenario

The Java SDK test suite (`mvn -f sdk/java test`) SHALL include at least one harness-based test per scenario in the `java-sdk` capability's persistence-related requirements added by `operative-state-persistence-java`. Scenarios SHALL be enumerated from the spec's `#### Scenario:` blocks and verified at the test level.

#### Scenario: Coverage report enumerates per-scenario harness tests

- **WHEN** `mvn -f sdk/java test` runs and produces JaCoCo + JUnit reports
- **THEN** the JUnit report SHALL include test methods named after each scenario from the persistence requirements (e.g. `testDefaultChangeIsNoChange_skipsPersistence`, `testStateApplyRunsApplyChangeAndRecords`, `testCoalesceCollapsesSameKey`, ...) — at least one test method per scenario

#### Scenario: Coverage gap fails CI pre-merge

- **WHEN** a new persistence-related scenario is added to a `java-sdk` requirement and no corresponding harness test exists
- **THEN** the spec/test sync check (run as part of `mvn -f sdk/java verify`) SHALL fail and block merge with a message naming the missing scenario; this matches the existing pattern from `java-sdk-testing` "Each java-sdk requirement has at least one unit test"

### Requirement: Java test app directory uses canonical Runner naming

The Java test compute-engine application SHALL live at `test/apps/java_runner/` (formerly `test/apps/java_compute_engine/`). All CI matrix entries that reference the test app's path SHALL be updated. Maven `pom.xml` artifact ID for the test app SHALL include `runner` (not `compute_engine`).

#### Scenario: CI matrix references new path
- **WHEN** the project's CI configuration is inspected for the Java SDK integration matrix
- **THEN** test-app working-directory entries SHALL reference `test/apps/java_runner/`; entries referencing the legacy path MUST NOT exist

#### Scenario: Integration test runs from new path
- **WHEN** the Java SDK integration matrix is run
- **THEN** the matrix MUST locate the test app at `test/apps/java_runner/`, build it, and exercise the Controller↔Runner flow including Operative registration, Procedure execution, Entity state persistence, and survive-restart behavior

### Requirement: Test naming reflects canonical role vocabulary

Java SDK integration test class names and test descriptions SHALL use canonical vocabulary (Controller, Runner, Operative, Procedure, Entity). Existing test classes whose names contain `Processor`, `Operator`, or `ComputeEngine` SHALL be renamed accordingly.

#### Scenario: Test class names compile after rename
- **WHEN** `mvn -f sdk/java test` is run
- **THEN** all integration test classes SHALL compile and run; no class name SHALL contain `Processor`, `Operator`, or `ComputeEngine`

### Requirement: Reference Java test app registers a Workflow Operative

The Java reference test app at `test/apps/java_runner/` SHALL register at least one `WorkflowOperative` class via `Flezha.registerWorkflow(...)`, mirroring the workflow operative that `test/apps/rust_runner/` registers (an `EchoOperative` that emits its `args` as plugin output and returns success). The workflow operative SHALL be a hand-written `WorkflowOperative` implementation (no annotation-processor sugar in v1). Registering the workflow operative SHALL NOT disturb the test app's stream operative registrations or its run-until-stdin-closes behavior.

#### Scenario: Java test app registers a workflow operative factory

- **WHEN** the Java test app starts and registers its operatives
- **THEN** it SHALL call `Flezha.registerWorkflow(EchoOperative.class)`, and the handshake `factory_ids` SHALL include the workflow operative's factory id alongside the stream operative factory ids

#### Scenario: Java and Rust test apps expose the same workflow operative factory

- **WHEN** the workflow operative factory ids registered by the Java test app are compared to those registered by `test/apps/rust_runner/`
- **THEN** both SHALL expose the mirrored echo workflow-operative factory id, so the same `flz operative start … --kind workflow` exercises either runtime

### Requirement: Integration matrix exercises a run-once Java workflow operative over both transports

The Elixir↔Java integration suite SHALL exercise a run-once Java workflow operative against the unchanged Elixir controller across both transports (`{transport: [:uds, :quic]}`). The suite SHALL assert, per transport: the workflow operative's `run` executes exactly once with the supplied `args`; emitted output reaches a bound plugin; a `WorkflowCompleted{status:"ok"}` is received and the workflow operative is torn down and absent from the durable registry; and a deliberately-failing workflow operative emits `WorkflowCompleted{status:"error"}` with its message and is likewise deregistered without retry.

#### Scenario: Run-once Java workflow operative completes and is torn down over UDS

- **WHEN** the suite starts an `EchoOperative` on a Java runner over UDS with `args="hi"` and a bound output plugin
- **THEN** the workflow operative SHALL run once, its emitted output SHALL reach the plugin, the controller SHALL receive `WorkflowCompleted{status:"ok"}`, and the workflow operative SHALL be removed from the durable registry

#### Scenario: Run-once Java workflow operative completes and is torn down over QUIC

- **WHEN** the same scenario runs over QUIC
- **THEN** the same assertions SHALL hold, exercising the full handshake + workflow operative spawn + run-once + emit + completion path on QUIC

#### Scenario: A failing Java workflow operative reports a terminal error

- **WHEN** the suite starts a workflow operative whose `run` throws over either transport
- **THEN** the controller SHALL receive `WorkflowCompleted{status:"error"}` carrying the throwable's message, the workflow operative SHALL be deregistered without retry, and no operative process SHALL remain

### Requirement: ProcedureTestHarness replayEquals replays against the captured initial snapshot

`ProcedureTestHarness.replayEquals()` SHALL reconstruct its replay baseline from the initial `State` captured (Kryo-serialized) at construction — whether the harness was built by `of(...)`, `withInitialState(...)`, or `withPayload(...)` — and fold the recorded Changes onto a fresh deserialization of that snapshot, matching the Rust `ProcedureTestHarness`. It SHALL NOT rebuild the baseline by calling `initState(Optional.empty(), Optional.empty())` afresh at replay time. This makes the harness honor a `withInitialState`/`withPayload` baseline whose `State` carries init-derived, non-journaled fields (e.g. a per-entity window length supplied via the `@InitState` hook), which the prior `initState(empty)` reconstruction would report as a false replay divergence.

A stateless Procedure (`Change = NoChange`) SHALL keep no replay baseline: `replayEquals()` SHALL return `Optional.empty()` without serializing the state, since nothing is journaled and the `Stateless` state is not Kryo-registered.

#### Scenario: withInitialState baseline is honored

- **WHEN** a harness is built via `withInitialState(state)` for a Procedure whose `State` has a key-derived field set outside the journaled Change log, inputs are applied, and `replayEquals()` runs
- **THEN** the recorded Changes are folded onto the captured `state` snapshot rather than a fresh `initState(Optional.empty(), Optional.empty())`, so the key-derived field matches and the result is `Optional.empty()`

#### Scenario: Stateless Procedure keeps no replay baseline

- **WHEN** `replayEquals()` runs on a harness for a `NoChange` Procedure
- **THEN** it returns `Optional.empty()` without serializing the stateless state, since no Changes are journaled

### Requirement: ProcedureTestHarness verifies the determinism contract on the output side

The Java `ProcedureTestHarness` SHALL provide `replayEmitsEqual()` — the output-side sibling of `replayEquals()` — which re-runs the recorded inputs against a fresh baseline and compares the resulting emit sequence (channel, key, metadata, and serialized payload) to the original. It SHALL return `Optional.empty()` when the replayed emits match, and a diagnostic describing the first divergence otherwise.

This is what lets a Java author verify the determinism contract (`ctx.now()` / `ctx.stableId()` instead of `System.currentTimeMillis()` / `UUID.randomUUID()`) the same way a Rust author does with `ProcedureTestHarness::replay_emits_equal`.

#### Scenario: A deterministic Procedure replays identical emits

- **WHEN** a Procedure derives its emitted key from `ctx.stableId()` and its timestamp from `ctx.now()`, and `replayEmitsEqual()` runs after applying inputs
- **THEN** it SHALL return `Optional.empty()`

#### Scenario: A Procedure reading ambient time is caught

- **WHEN** a Procedure emits a value derived from `System.currentTimeMillis()` rather than `ctx.now()`, and `replayEmitsEqual()` runs
- **THEN** it SHALL return a non-empty diagnostic naming the diverging emit

### Requirement: ProcedureTestHarness supports injectable clocks

The `ProcedureTestHarness` SHALL support driving the Procedure's declared `Clock` from the test rather than from the ambient system clock: `withWallClock(long fixedMillis)` SHALL pin `ctx.now()` to a constant, and `withWallClockScript(long... millis)` SHALL serve one value per successive invocation. For an `EventTimeFrom` Procedure the harness SHALL serve the value the Procedure's `eventTime(input)` extractor returns; for a `LogicalClock` Procedure it SHALL serve the harness's durable change seq.

#### Scenario: A fixed wall clock makes now() constant across invocations

- **WHEN** a harness built with `withWallClock(1_700_000_000_000L)` applies three inputs
- **THEN** every invocation's `ctx.now()` SHALL return `1700000000000`

#### Scenario: A scripted wall clock advances per invocation

- **WHEN** a harness built with `withWallClockScript(1000L, 2000L, 3000L)` applies three inputs
- **THEN** the invocations' `ctx.now()` SHALL return `1000`, `2000`, and `3000` respectively

### Requirement: ProcedureTestHarness asserts keyed emits and identity stamps

The harness's `EmitRecord` SHALL expose the emit's key (empty for an emit carrying none) and its user metadata, so tests can assert keyed-emit behaviour without invoking the wire path. The harness SHALL support declaring a channel keyed (`withKeyedChannel()`) and standing in for the Entity a key routed to (`withEntityKey(key)`), so that the keyed-channel contract — the Entity-key default and the refusals (`KeyRequiredOnKeyedChannel`, `KeyNotAllowedOnNonKeyedChannel`) — is exercisable in a unit test. The `flz.*` identity stamps themselves are minted by the runtime's frame writer and are NOT reproduced by the harness; assert them against the dispatcher.

#### Scenario: A keyed emit records its key

- **WHEN** a Procedure calls `ctx.emitKeyed("BTC", candle)` on a channel the harness declared keyed
- **THEN** `harness.emitRecords()` SHALL contain one record whose key is `"BTC"`

#### Scenario: A key-less emit in a keyed Entity records that Entity's key

- **WHEN** a harness declared keyed and standing in for the Entity `"BTC/USD"` drives a Procedure that calls `ctx.emit(candle)`
- **THEN** `harness.emitRecords()` SHALL contain one record whose key is `"BTC/USD"`, and `ctx.entityKey()` SHALL have served `"BTC/USD"`

#### Scenario: A key-less emit on a keyed channel is refused without an Entity key

- **WHEN** a harness declared keyed with no Entity key drives a Procedure that calls `ctx.emit(candle)`
- **THEN** the harness ctx SHALL throw `ProcedureException.KeyRequiredOnKeyedChannel` and SHALL record nothing

#### Scenario: A reserved metadata key is rejected in the harness too

- **WHEN** a Procedure calls `ctx.emitWithMeta(value, Map.of("flz.seq", "1.0"))` under the harness
- **THEN** the harness ctx SHALL throw `ProcedureException.ReservedMetadataKey`, matching the runtime's behaviour

