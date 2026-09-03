# java-sdk Specification

## Purpose

Defines the Java SDK for Flezha compute engines: the public authoring API and the Operative / Procedure / Entity runtime, hosted on the shared poll core through the `FlzCore` JNI binding over `flz_core_ffi`. Conforms to `docs/sdk/protocol_spec.md` and `docs/sdk/poll_core.md`.
## Requirements
### Requirement: Java SDK Entry Point

The Java SDK SHALL provide `com.flezha.sdk.Flezha.createRunner(String namespace)` as the single entry point, returning a concrete `FlezhaRunner` directly — there SHALL be no separate `RunnerBuilder` type. Creating a runner SHALL NOT connect, claim the process singleton slot, or spawn any thread. Calling `.start()` on the runner MUST connect to the controller's control socket, send the handshake init message per `docs/sdk/protocol_spec.md` §5.2 carrying the runner's currently-declared factory ids (possibly empty), wait for `AssignRunnerId`, and return `this` for chaining. The started runner SHALL expose `getNamespace()` (replacing `getId()`) and `getOperatives()` (a snapshot of currently-spawned Operative instances keyed by what the wire calls `operative_id`). `.run()` SHALL be sugar for `.start()` followed by `awaitTermination()`, never an independent implementation.

#### Scenario: Successful connection on a created runner
- **WHEN** an SDK user calls `Flezha.createRunner("my_app").defineStream(MarketOperative.class).start()` with the controller running
- **THEN** the call blocks until the controller responds with `AssignRunnerId`, returns the same `FlezhaRunner`, and `getNamespace()` returns `"my_app"`

#### Scenario: Controller unavailable triggers exponential backoff retries
- **WHEN** an SDK user calls `.start()` on a runner and the controller's control socket is not yet listening
- **THEN** the SDK SHALL retry the connect with exponential backoff starting at 200 ms and capped at 30 s, and succeed once the controller becomes available

#### Scenario: start() with zero defined operative types is legal
- **WHEN** `.start()` is called on a `FlezhaRunner` with no `.defineStream(...)`/`.defineWorkflow(...)` calls and no auto-wired classes on the classpath
- **THEN** the runner SHALL connect successfully with an empty `factory_ids` list in the handshake init message, and operative types MAY be defined later on the live runner

#### Scenario: RunnerBuilder is not importable
- **WHEN** user code references `com.flezha.sdk.RunnerBuilder`, `com.flezha.sdk.operative.StreamOperativeRegistry`, `com.flezha.sdk.workflow.WorkflowOperativeRegistry`, or `Flezha.newRunner`
- **THEN** compilation SHALL fail — the builder type, both public registry facades, and the old entry-point name are deleted with no shim

### Requirement: Procedure Interface and Per-Entity Dispatch

The Java SDK SHALL expose a generic `Procedure<Input, State, Output, Change, Clock, Config>` interface in `com.flezha.sdk.procedure`. Implementers SHALL provide `name()`, `inputType() → Class<Input>`, `outputType() → Class<Output>`, `initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) → State`, and `run(Input, StateHandle<State, Change>, ProcedureCtx) throws ProcedureException`. The default-method overrides `entityKeys()`, `entitiesFor(Input)` and `entitiesForOverridden()` declare static keying; dynamic keying is declared on the WIRING (`keyBy` on the consumer edge), not on the Procedure. The entity mode is therefore single by default, static when `entityKeys()` is non-empty with `entitiesForOverridden()` true, and dynamic when a consumer edge into the Procedure carries a key function. The runtime SHALL invoke `run` on a per-entity basis with sticky ordering (see the Sticky Per-Entity Ordering requirement) and SHALL pass each input to all entities returned by `entitiesFor(input)` for static-mode procedures (single-mode procedures skip the call and route to entity index 0).

The `ProcedureCtx` parameter SHALL provide:

- `emit(value)` for inter-procedure or plugin-bound outputs, `emit(channel, value)` for explicit channel selection, and `emitWithMeta(value, meta)` / `emitWithMeta(channel, value, meta)` for plugin-bound outputs with user-attached metadata. `emitWithMeta` on inter-procedure channels SHALL throw `ProcedureException.MetadataNotAllowedOnInterProcedureChannel` naming the channel.
- `emitKeyed(key, value)` and `emitKeyedWithMeta(key, value, meta)` for channels declared `.keyed()`, supplying an output key that differs from the Entity's own. Emitting **without** a key on a `.keyed()` channel SHALL carry the invoking Entity's key when it has one, and SHALL throw `ProcedureException.KeyRequiredOnKeyedChannel` when it does not (a single-entity Procedure); emitting **with** a key on a channel not declared `.keyed()` SHALL throw `ProcedureException.KeyNotAllowedOnNonKeyedChannel`. Both name the channel.
- `Optional<String> entityKey()` — the key this Entity was routed by, empty for a single-entity Procedure. It is part of the invocation's identity, so it is replay-stable.
- `long now()` — the invocation's time value, computed **once** by the runtime from the Procedure's declared `Clock` before `run` executes, and returning that same value for every call within the invocation. This is the determinism-contract replacement for `System.currentTimeMillis()`.
- `StableId stableId()` — a deterministic, replay-stable 128-bit identifier derived from the Entity identity, the invocation's durable change seq, and a per-invocation call ordinal. Repeated calls within one invocation differ; the n-th call of a replayed invocation SHALL equal the n-th call of the original. This is the determinism-contract replacement for `UUID.randomUUID()`.
- `void checkpoint()` — see the checkpoint ack-granularity requirement.

The runtime SHALL stamp `flz.entity`, `flz.seq`, and (on keyed emits) `flz.key` onto plugin-bound frames. User-supplied metadata keys in the reserved `flz.` namespace SHALL be **rejected** with `ProcedureException.ReservedMetadataKey` naming the key, so that identity stamps can neither be spoofed nor clobbered.

#### Scenario: Single-mode Procedure runs on a single entity
- **WHEN** a Procedure whose `entityKeys()` returns an empty list is registered as the consumer of an entry channel, and one input arrives on that channel
- **THEN** the SDK SHALL invoke the Procedure's `run` once on the single allocated entity (entity index 0), with `state` argument equal to the value `initState(Optional.empty(), Optional.empty())` returned at operative startup

#### Scenario: Static-mode Procedure creates one entity per entityKey
- **WHEN** a Procedure's `entityKeys()` returns `["1m", "1h", "1d", "1w"]` and `entitiesForOverridden()` returns `true`
- **THEN** at operative startup the SDK SHALL allocate exactly four `EntitySlot` instances for that Procedure, each constructed with `initState(Optional.of(key), Optional.empty())` for its corresponding key

#### Scenario: Static-mode entitiesFor fan-out enqueues input on each named entity
- **WHEN** a Procedure's `entitiesFor(input)` returns `["1m", "1h"]` for a given input
- **THEN** the SDK SHALL enqueue that input on the entities identified by `"1m"` and `"1h"` (each receives its own `run` invocation), and SHALL NOT enqueue it on `"1d"` or `"1w"`

#### Scenario: Run throwing ProcedureException logs and continues
- **WHEN** `Procedure.run(...)` throws `ProcedureException` on a given input and the input channel's retry budget is exhausted
- **THEN** the SDK SHALL log the exception with `(operativeInstanceId, procedureName, entityKey)` context, SHALL discard any partial `ctx.emit(...)` calls made during this invocation, SHALL park the payload to the dead-letter queue, and SHALL continue dispatching subsequent inputs on the same entity

#### Scenario: Run throwing arbitrary RuntimeException logs and continues
- **WHEN** `Procedure.run(...)` throws a non-`ProcedureException` `RuntimeException`
- **THEN** the SDK SHALL log the exception with the same `(operativeInstanceId, procedureName, entityKey)` context, discard partial emits, and continue dispatching subsequent inputs on the same entity

#### Scenario: Inter-procedure zero-copy preserves value identity
- **WHEN** Procedure A emits a non-null value `v` on an inter-procedure channel that Procedure B consumes (single-entity fan-out, single JVM)
- **THEN** the value passed to Procedure B's `run` SHALL be `==`-equal (reference-equal) to `v` — the runtime SHALL NOT copy or serialize the value between Procedures in the same JVM

#### Scenario: emitWithMeta on an inter-procedure channel returns error
- **WHEN** a Procedure whose declared output is an inter-procedure channel calls `ctx.emitWithMeta(value, Map.of("k", "v"))`
- **THEN** the SDK SHALL throw `ProcedureException.MetadataNotAllowedOnInterProcedureChannel` whose message names the channel

#### Scenario: Plugin-bound emit attaches the channel name as metadata on the wire
- **WHEN** a Procedure whose declared output is a plugin-bound channel `"ohlc.candles"` calls `ctx.emit(candle)`
- **THEN** the SDK SHALL serialize `candle` via the channel's serializer, frame the bytes with `metadata` containing `"channel" -> "ohlc.candles"` plus the runtime's `flz.entity` and `flz.seq` identity stamps, and send the frame on the operative's secondary stream

#### Scenario: Plugin-bound emitWithMeta appends user metadata after the channel key
- **WHEN** a Procedure calls `ctx.emitWithMeta(candle, Map.of("trace_id", "abc"))` on a plugin-bound channel `"ohlc.candles"`
- **THEN** the SDK SHALL frame the bytes with `metadata` containing `"channel" -> "ohlc.candles"`, `"trace_id" -> "abc"`, and the runtime identity stamps

#### Scenario: Reserved flz. metadata key in user map is rejected
- **WHEN** a Procedure calls `ctx.emitWithMeta(candle, Map.of("flz.key", "spoofed"))` on a plugin-bound channel
- **THEN** the SDK SHALL throw `ProcedureException.ReservedMetadataKey` naming `"flz.key"`, and SHALL NOT emit the frame

#### Scenario: now() is constant within one invocation
- **WHEN** a Procedure's `run` calls `ctx.now()` three times
- **THEN** all three calls SHALL return the identical value — the one the runtime computed from the declared `Clock` before entering `run`

#### Scenario: stableId() is replay-stable and ordinal-distinct
- **WHEN** a Procedure's `run` calls `ctx.stableId()` twice for a given input, and the same input is later replayed at the same durable change seq
- **THEN** the two calls within one invocation SHALL return different ids, AND the n-th call of the replayed invocation SHALL return an id byte-equal to the n-th call of the original

#### Scenario: stableId() is byte-identical to the Rust SDK
- **WHEN** the Java SDK derives a `StableId` from entity identity `s`, change seq `q`, and ordinal `n`
- **THEN** the resulting 128-bit value SHALL equal the Rust SDK's `StableId::derive(s, q, n)` (FNV-1a-128 over `s ‖ big-endian q ‖ big-endian n`), and its 32-hex-digit rendering SHALL match Rust's `{:032x}`

#### Scenario: emitKeyed on a non-keyed channel is rejected
- **WHEN** a Procedure calls `ctx.emitKeyed("BTC", candle)` on a channel not declared `.keyed()`
- **THEN** the SDK SHALL throw `ProcedureException.KeyNotAllowedOnNonKeyedChannel` naming the channel

#### Scenario: Keyed emit stamps flz.key on the wire
- **WHEN** a Procedure calls `ctx.emitKeyed("BTC", candle)` on a channel declared `.keyed()`
- **THEN** the plugin frame's metadata SHALL carry `flz.key -> "BTC"`, `flz.entity -> "<instance>/<procedure>/<entityKey>"`, and `flz.seq -> "<changeSeq>.<emitOrdinal>"`

#### Scenario: A key-less emit in a keyed Entity carries that Entity's key
- **WHEN** the Entity routed by `"BTC/USD"` calls `ctx.emit(candle)` on a channel declared `.keyed()`
- **THEN** the plugin frame SHALL carry `flz.key -> "BTC/USD"`, identically to `ctx.emitKeyed("BTC/USD", candle)`

#### Scenario: A key-less emit on a keyed channel is refused where the Entity has no key
- **WHEN** a single-entity Procedure calls `ctx.emit(candle)` on a channel declared `.keyed()`
- **THEN** the SDK SHALL throw `ProcedureException.KeyRequiredOnKeyedChannel` naming the channel, and nothing SHALL be emitted

### Requirement: Sticky Per-Entity Ordering Under ThreadPool Dispatch

The runtime SHALL guarantee that for any single `(operativeInstance,
procedure, entity)` triple, two invocations of that Procedure's `run(...)`
never execute concurrently, and that invocations observe their inputs in
enqueue order.
This invariant is supplied by the core's per-entity handout exclusivity —
an entity with an outstanding activation is never handed out again until
its completion — and MUST hold regardless of poller count, Operative count,
Entity count, or transport.

#### Scenario: Same entity never runs concurrently under load
- **WHEN** 10,000 inputs targeting one entity arrive back-to-back while 16 Java pollers poll
- **THEN** at any wall-clock instant at most one thread SHALL be inside that Entity's `run(...)`

#### Scenario: Different entities of same procedure may run concurrently
- **WHEN** inputs arrive simultaneously for two distinct entities of the same Procedure and ≥ 2 pollers are polling
- **THEN** the two Entities' `run(...)` methods MAY execute concurrently on different threads

#### Scenario: Per-entity inputs delivered in arrival order
- **WHEN** inputs `"a"`, `"b"`, `"c"` are enqueued for one Entity in that order with an append-to-state Procedure
- **THEN** the Entity's State SHALL end with `["a", "b", "c"]` in exactly that order

### Requirement: Command Dispatch

Control commands SHALL be handled by the core. Of the command surface, only
the five host-participating interactions SHALL reach Java — as activation
kinds (`RestoreFold`, `SnapshotRequest`, `LiveStateRead`, `ApplyState`,
`MigrationSerialize`), subject to per-entity exclusivity; every other
command (including `Ping`, credit frames, acks, `AssignRunnerId`,
`ConnectSocket`, `RemOperative`, `WorkflowStop`) SHALL be answered by the
core without surfacing to Java. Class capability declarations
(`effectively_once`, `ack_granularity`, `correction_window_ms`,
`beyond_window_policy`, `compensable_procs`, `leaf_procs`,
`compact_after_bytes`) SHALL ride the declaration the Java layer registers;
the core SHALL populate `AddOperativeResponse.metadata` from it — a class
declaring nothing yields an empty map, byte-identical to today.

#### Scenario: AddOperative spawns a Java-hosted operative instance
- **WHEN** the controller sends `AddOperative{operative_id:"p1", factory_id:"market"}` for a declared Java class
- **THEN** the core SHALL instantiate the class's topology, respond `status = "success"` with its declared capability metadata, and subsequent entry inputs SHALL surface to Java as work activations for `"p1"`

#### Scenario: Ping never reaches Java
- **WHEN** the controller sends `Ping` while Java pollers are polling
- **THEN** the core SHALL answer it and no activation SHALL be handed to any poller

#### Scenario: Restore is a fold handed to Java
- **WHEN** the controller ships `RestoreInitState` for an entity with a persisted change log
- **THEN** Java SHALL receive a `RestoreFold` activation, fold the snapshot and Kryo change records through its existing decode paths, and no input activation for that entity SHALL be handed out before the fold completes

### Requirement: Auto-Reconnect on Unexpected Disconnect

Reconnection SHALL be owned by the core: on an unexpected control-channel
disconnect the core retries with capped exponential backoff and
re-presents the runner's stable identity, exactly as for the Rust host.
The Java layer SHALL NOT implement a reconnect loop; its pollers simply
continue polling across the reconnect (work resumes when the controller
re-binds and re-forwards).

#### Scenario: Controller restart is invisible to Java pollers
- **WHEN** the controller restarts while a Java runner is idle-polling
- **THEN** the core SHALL reconnect with backoff and re-handshake, no Java-side reconnect code runs, and the next dispatched input surfaces as an ordinary activation

### Requirement: Worker Pool and Dispatch Throughput Floor

The Java SDK SHALL fetch work with a small fixed set of platform fetcher
threads (default 2; `FLEZHA_WORKER_POOL_SIZE` overrides the count) and SHALL
run each polled activation on a virtual thread; the count of jobs underway is
owned by the core, not by any Java pool size. The SDK SHALL sustain at least
the throughput floor (in messages/second) measured against the v1
implementation at merge time on the regression bench (`100 operatives × 1
procedure × 1 entity × 10K msg`, no-op Procedure, UDS transport, default
worker-pool size; see `operative-mvp-java/design.md` Decision 7) invoked via
`mix bench --runtime java --shape regression -n 100 -m 10000`. The
bench-regression-java CI job is informational in v1; the literal floor value
is recorded in this requirement once `mix bench --runtime java` has been
measured 3× on the reference host (median, rounded down to a clean number) —
until then the recorded floor is "TBD pending v1 measurement".

A fetcher that exits for any reason other than shutdown SHALL be replaced, so
the configured fetcher count is maintained for the life of the runner; each
replacement SHALL be logged with the throwable that ended the previous thread.
A runner SHALL NOT be left with zero fetchers while it is still connected.

Every activation dispatched onto a virtual thread SHALL be answered exactly
once. The dispatch SHALL be wrapped so that a throwable escaping the dispatcher
— including one thrown while encoding the result or while calling `complete` —
abandons the activation rather than returning without answering it; logging the
throwable is not sufficient, because an unanswered activation retires its entity
for the life of the process.

#### Scenario: Reference bench meets the Java throughput floor
- **WHEN** a developer runs `MIX_ENV=prod mix bench --runtime java --shape regression -n 100 -m 10000` on a modern 8-core x86_64 laptop with a warm JVM (≥ 30 s warmup)
- **THEN** the reported `Throughput:` line SHALL be ≥ the recorded Java floor value

#### Scenario: Default fetcher count is small and fixed
- **WHEN** the SDK starts with `FLEZHA_WORKER_POOL_SIZE` unset
- **THEN** exactly 2 platform fetcher threads SHALL poll, and every activation
  SHALL be dispatched on a virtual thread

#### Scenario: A dead fetcher is replaced
- **WHEN** a fetcher thread ends on a throwable while the runner is connected and not closing
- **THEN** a replacement fetcher SHALL be started, the configured fetcher count SHALL be restored, and the throwable SHALL be logged

#### Scenario: A throwing dispatch abandons instead of dropping the activation
- **WHEN** a throwable escapes the dispatcher, the result encoder, or `complete` for an activation on entity E
- **THEN** the SDK SHALL abandon the activation, E SHALL be pollable again with its items intact, and the throwable SHALL be logged

### Requirement: JNI Bridge to flz_core_ffi

The Java SDK SHALL interact with the runner kernel exclusively through a
JNI bridge that wraps `flz_core_ffi`'s C ABI one-to-one: context creation
with schema-version negotiation, class declaration (JSON), start, blocking
poll with timeout, complete, shutdown, last-error, and the log callback.
There SHALL be no other native surface: a capability reachable through the
ABI but not through the bridge is a defect of the bridge. `poll` SHALL be
issued as a plain blocking native call on a host thread (the core wakes it
on shutdown). All native buffers returned by the bridge MUST be copied into
Java `byte[]` before being freed through the core-provided free function.

#### Scenario: Version mismatch refuses at context creation
- **WHEN** the Java shim was built against schema version N and the bundled core speaks N+1
- **THEN** context creation SHALL fail before any controller connection with an error naming both N and N+1

#### Scenario: Native buffer freed after copy into Java byte[]
- **WHEN** `poll` returns an activation buffer pointer and length
- **THEN** the bridge SHALL copy the bytes into a Java `byte[]`, free the native buffer through the core's free function exactly once, and hand the `byte[]` to the poller

#### Scenario: Shutdown wakes blocked Java pollers
- **WHEN** poller threads are blocked in the native `poll` call and the runner shuts down
- **THEN** every blocked call SHALL return promptly with the distinguishable shutting-down status and the poller threads SHALL exit their loops

### Requirement: Procedure interface declares Change, Clock, Config generic parameters

The Java SDK's `Procedure` interface SHALL be parameterized as `Procedure<Input, State, Output, Change extends com.flezha.sdk.state.Change, Clock extends com.flezha.sdk.state.Clock, Config>`. The interface SHALL declare reflection methods `Class<Change> changeType()`, `Class<Clock> clockType()`, `Class<Config> configType()` returning the runtime `Class` for each generic parameter (Java type erasure precludes recovering them otherwise).

The interface SHALL declare lifecycle methods:
- `default void applyChange(State state, Change change)` whose default impl is a no-op for `NoChange` and throws `UnsupportedOperationException` for any other Change type (forcing user override).
- `default Optional<CoalesceKey> coalesceKey(Change change)` whose default impl delegates to `change.coalesceKey()`.
- `default long compactAfterBytes()` returning `64L * 1024 * 1024`.
- `default int schemaVersion()` returning `1`.
- `default List<Class<?>> kryoTypes()` returning `List.of()` — the user-declared list of types to register with Kryo for serialization, in stable-ID order.
- `default Procedure<...> fromConfig(Config cfg)` whose default invokes the no-arg constructor when `Config = Void`, and throws `UnsupportedOperationException` for non-Void Config types (forcing user override).

#### Scenario: Default Change is NoChange skips persistence machinery

- **WHEN** a Java Procedure does not declare its `Change` parameter (defaults to `NoChange`)
- **THEN** the runtime SHALL detect via `changeType() == NoChange.class` (reference equality) and its completions SHALL carry no change bytes, so the core registers no flusher work for this Entity

#### Scenario: User-defined Change opts the Procedure into persistence

- **WHEN** a Java Procedure declares `Change = MyChange` (any non-`NoChange` user type implementing `com.flezha.sdk.state.Change`)
- **THEN** the runtime SHALL register a typed flush buffer (`List<MyChange>`) on each Entity of that Procedure and SHALL include those Entities in the per-JVM flusher's drain rotation

#### Scenario: Default applyChange throws on non-NoChange Change without override

- **WHEN** a Java Procedure declares `Change = MyChange` but does NOT override `applyChange`
- **THEN** the operative registration step SHALL succeed (the default impl exists) but the first `state.apply(MyChange.foo())` call SHALL throw `UnsupportedOperationException` whose message names the missing override

#### Scenario: Default fromConfig invokes no-arg constructor for Void Config

- **WHEN** a Java Procedure declares `Config = Void` and does NOT override `fromConfig`
- **THEN** the runtime's `fromConfig(null)` invocation SHALL call the Procedure's public no-arg constructor via reflection; absence of a no-arg constructor SHALL throw `IllegalStateException` whose message names the Procedure

#### Scenario: User-defined Config ships through Jackson deserialization

- **WHEN** a Java Procedure declares `Config = MyConfig` (a Jackson-deserializable POJO/record) and overrides `fromConfig(MyConfig cfg)`
- **THEN** the runtime SHALL deserialize the agent-shipped JSON config bytes via Jackson into `MyConfig` and SHALL invoke `procedure.fromConfig(cfg)` to construct the Procedure at Entity init; deserialization failure SHALL throw `StreamOperativeConfigurationException("Config deserialize failed: " + procedureName)`

### Requirement: Procedure.run receives StateHandle, not direct State reference

The Java SDK's `Procedure.run` signature SHALL be `void run(Input input, StateHandle<State, Change> state, ProcedureCtx ctx) throws ProcedureException` (was `void run(Input input, State state, ProcedureCtx ctx)`). `StateHandle<State, Change>` SHALL provide:

- `State get()` — read access to the underlying state.
- `void apply(Change change)` — runs `Procedure::applyChange(state, change)` and pushes the Change onto the per-Entity flush buffer.
- NO setter, no direct-mutation method. Direct `state.field = value` is impossible without going through `apply`.

#### Scenario: Direct field mutation requires apply

- **WHEN** a Java Procedure's `run` body wants to mutate `state.someField` from a value `v1` to `v2`
- **THEN** the user MUST declare a Change (e.g. `record SetSomeField(Object newValue) implements Change {}`), implement `applyChange` to write the field, and call `state.apply(new SetSomeField(v2))`; direct `state.get().someField = v2` is technically possible (mutable field on the underlying State) but bypasses persistence and breaks `replayEquals` — flagged as user error in documentation but not structurally prevented in Java (unlike Rust's `Deref` chokepoint)

#### Scenario: Read access via get() is allocation-free

- **WHEN** a Java Procedure's `run` body reads `state.get().someField`
- **THEN** the read SHALL succeed via the handle's `get()` method (one method invocation, JIT-inlinable), no Change recording, no buffer mutation

#### Scenario: Mutation via apply runs applyChange and records

- **WHEN** a Java Procedure's `run` body calls `state.apply(MyChange.foo())`
- **THEN** `Procedure.applyChange(state, &MyChange.foo())` SHALL run on the underlying state AND `MyChange.foo()` SHALL be appended to the per-Entity flush buffer (observable via `ProcedureTestHarness.recordedChanges()` and via the per-JVM flusher's next tick)

### Requirement: Procedure declares Clock generic parameter with default WallClock

The Java SDK's `Procedure` interface SHALL declare `Clock extends com.flezha.sdk.state.Clock` as a generic parameter, defaulting to `com.flezha.sdk.state.WallClock`. `Clock` SHALL be a sealed Java interface with `permits WallClock, LogicalClock, EventTimeFrom`. The runtime SHALL evaluate the declared clock **once per invocation**, before entering `run`, and serve that single value from `ctx.now()`:

- `WallClock` — epoch milliseconds at dispatch entry.
- `EventTimeFrom` — the event time extracted from the current input. Because Java's erasure precludes Rust's compile-enforced `TimeExtractor<Input>` bound, a Procedure declaring `EventTimeFrom` SHALL override `default OptionalLong eventTime(Input input)`; failing to do so SHALL fail Operative registration with `StreamOperativeConfigurationException` naming the Procedure and the missing override.
- `LogicalClock` — the raw durable `change_seq`, re-armed to `terminal + 1` on restore. A Procedure declaring `LogicalClock` with `Change = NoChange` SHALL fail registration with `StreamOperativeConfigurationException` carrying the Rust error name `UnservableClock`, since there is no durable seq to serve.

`StreamOperativeConfigurationException::ClockNotImplementedYet` SHALL NOT be thrown for any member of the sealed hierarchy.

#### Scenario: Default Clock is WallClock

- **WHEN** a Java Procedure does not declare its `Clock` parameter
- **THEN** `Procedure.clockType() == WallClock.class` AND Operative registration SHALL succeed AND `ctx.now()` SHALL return epoch milliseconds sampled once at dispatch entry

#### Scenario: EventTimeFrom serves the extracted input time

- **WHEN** a Java Procedure declares `Clock = EventTimeFrom` and overrides `eventTime(input)` to return the input's timestamp field
- **THEN** registration SHALL succeed AND `ctx.now()` SHALL return that extracted timestamp for the invocation

#### Scenario: EventTimeFrom without an eventTime override fails registration

- **WHEN** a Java Procedure declares `Clock = EventTimeFrom` but does not override `eventTime(Input)`
- **THEN** `.defineStream(...)` SHALL throw `StreamOperativeConfigurationException` naming the Procedure and the missing `eventTime` override

#### Scenario: LogicalClock serves the durable change seq and re-arms on restore

- **WHEN** a Java Procedure declares `Clock = LogicalClock` and a non-`NoChange` Change, and the Entity is restored from a change log whose terminal seq is 41
- **THEN** registration SHALL succeed AND the first post-restore invocation's `ctx.now()` SHALL return `42`

#### Scenario: LogicalClock with NoChange fails registration

- **WHEN** a Java Procedure declares `Clock = LogicalClock` and `Change = NoChange`
- **THEN** `.defineStream(...)` SHALL throw `StreamOperativeConfigurationException` whose message carries the error name `UnservableClock`

### Requirement: Operative declares persistenceEnabled default-method, frozen at instance start

The Java SDK's `StreamOperative` interface SHALL declare `default boolean persistenceEnabled() { return true; }`. The runtime SHALL resolve a per-Operative-instance `persistenceEnabled: boolean` at the moment `AddOperative` is processed using:

1. If the `AddOperative` metadata carries `operative_persistence.<OperativeClass>` with a boolean value (runner override), use that.
2. Otherwise, use the value of `StreamOperative.persistenceEnabled()` (the Operative-class default).
3. If neither is overridden, the result is `true`.

The resolved value SHALL be bound to the `StreamOperativeInstance` and SHALL be propagated to every `EntitySlot` constructed for that instance. Subsequent changes to the runner-side metadata or to the SDK's `StreamOperative.persistenceEnabled()` impl (via SDK redeploy) SHALL NOT affect already-running instances.

#### Scenario: Default-on with no override

- **WHEN** an Operative's `persistenceEnabled()` is the default (returns `true`) AND no runner override is present
- **THEN** `StreamOperativeInstance.persistenceEnabled()` SHALL return `true`

#### Scenario: Code-declared opt-out is honored

- **WHEN** `MarketOperative.persistenceEnabled()` returns `false` (user override) AND no runner override is present
- **THEN** the class declaration SHALL announce `persistent: false` and the core SHALL drain-and-discard this instance's change bytes — no `entity_state_change` frame reaches the wire

#### Scenario: Runner override takes precedence

- **WHEN** `MarketOperative.persistenceEnabled()` returns `true` AND `AddOperative` metadata sets `operative_persistence.MarketOperative = false`
- **THEN** `StreamOperativeInstance.persistenceEnabled()` SHALL return `false`

#### Scenario: Resolved value frozen at instance start

- **WHEN** an `StreamOperativeInstance` is constructed with resolved `persistenceEnabled = true`, AND a subsequent runner config change sets `operative_persistence.<OperativeClass> = false`
- **THEN** the running `StreamOperativeInstance` SHALL continue with `persistenceEnabled = true`; the new value SHALL take effect only on the next `stop_operative` + `start_operative` cycle for that instance

### Requirement: initState accepts optional restoration payload

The Java SDK's `Procedure.initState` signature SHALL be `State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload)`. When `initPayload.isEmpty()`, `initState` SHALL behave as a fresh-start constructor. When `initPayload.isPresent()`, the user MAY inspect or ignore the payload bytes; the runtime SHALL automatically apply the payload AFTER `initState` returns:

1. If `payload.snapshotBytes().length > 0`, deserialize via `KryoCodec` (after header validation) and replace the user-constructed state.
2. If `payload.changeLogBytes().length > 0`, decode each WAL record (frame format from `operative-state-persistence`), validate header, deserialize Change via `KryoCodec`, and invoke `Procedure.applyChange(state, change)` in seq order.

#### Scenario: Cold start passes Optional.empty()

- **WHEN** a Java Entity is constructed for the first time with no prior persisted state on the agent
- **THEN** the runtime SHALL invoke `initState(entityKey, Optional.empty())`

#### Scenario: Restart passes Optional.of(payload)

- **WHEN** a Java Entity is reconnected and the agent has snapshot or WAL bytes for it
- **THEN** the runtime SHALL invoke `initState(entityKey, Optional.of(payload))` AND, after `initState` returns, the runtime SHALL deserialize the snapshot (if any) and fold the change log via `applyChange` in seq order

### Requirement: Per-Entity flusher with toggle-aware drain

Durable-change flushing SHALL be owned by the core: Kryo-encoded change
bytes returned in completions are buffered per entity core-side and
drained to framed WAL records on the core's linger cadence, honoring the
persistence toggle (drain-and-discard when disabled). The Java layer's
obligation is exactly its completion contract: hand the invocation's
change bytes (and timer ops, offsets, outputs) back in the
`ActivationResult` — it SHALL NOT run a flusher thread of its own.

#### Scenario: Changes returned in a completion reach the WAL
- **WHEN** a Java invocation applies two Changes and its completion carries their Kryo bytes with persistence enabled
- **THEN** the core SHALL frame and ship them as `entity_state_change` records with contiguous seqs, and a subsequent restore SHALL fold them back through Java's `RestoreFold`

#### Scenario: Persistence disabled drains and discards
- **WHEN** the same completion arrives for a class with persistence disabled
- **THEN** no WAL record SHALL ship and the entity's in-memory state SHALL still reflect the applied Changes

### Requirement: Hot-path cost is bounded

The Java SDK's `state.apply(change)` SHALL pay exactly: one `Procedure.applyChange(state, change)` invocation (user-supplied; cost varies) plus one `ArrayList.add(change)` procedure (amortized O(1)). The hot path SHALL NOT serialize, fsync, network-send, or acquire any lock beyond the existing per-Entity sticky CAS and the runner thread's exclusive ownership of the buffer for the duration of `Procedure.run`.

#### Scenario: No serialization on state.apply

- **WHEN** a Procedure's `run` calls `state.apply(MyChange.foo())` 100 times in a tight loop
- **THEN** zero `KryoCodec.serialize`, `Kryo.writeClassAndObject`, or any serialization-equivalent call SHALL be issued from the runner thread during these 100 calls (verifiable via JFR profiling or by mocking `KryoCodec` and asserting zero invocations)

#### Scenario: No JNI calls on state.apply

- **WHEN** a Procedure's `run` calls `state.apply(MyChange.foo())` 100 times
- **THEN** zero `NativeTransport.*` JNI calls SHALL be issued from the runner thread during these 100 calls

### Requirement: Kryo serialization with stable registered type IDs

The Java SDK SHALL use Kryo for `format_id = 4` serialization with `setRegistrationRequired(true)`. The runtime SHALL register built-in flezha types (NoChange, RestoreInitPayload, CoalesceKey, primitives, common collections) at SDK init in a fixed order (stable IDs 1-N). User types from `Procedure.kryoTypes()` SHALL be registered after built-ins in declaration order (stable IDs N+1, N+2, ...).

The Java SDK SHALL refuse to serialize any user-supplied object whose runtime class is not in the registered set — Kryo throws `IllegalArgumentException` per its `registrationRequired = true` semantics; the SDK wraps this as `StateSerializeException::UnregisteredClass(className)` and rethrows.

Reordering of `Procedure.kryoTypes()` between SDK versions SHALL be an unsupported breaking change (changes the assigned IDs); appending new types to the end SHALL be safe (preserves prior IDs).

#### Scenario: Built-in types registered with stable IDs

- **WHEN** a JVM starts with the Java SDK loaded
- **THEN** the registered Kryo types SHALL include `NoChange`, `RestoreInitPayload`, `CoalesceKey`, `String`, `Long`, `Integer`, `Double`, `Boolean`, `byte[]`, `List`, `Map`, `Optional` at fixed stable IDs (the exact values are recorded in `KryoCodec` source as a documented constant)

#### Scenario: User types registered after built-ins

- **WHEN** `Procedure.kryoTypes()` returns `[OhlcChange.class, OhlcState.class]` and built-ins occupy IDs 1-12
- **THEN** `OhlcChange.class` SHALL register with ID 13 AND `OhlcState.class` SHALL register with ID 14

#### Scenario: Unregistered class fails loud

- **WHEN** a Procedure's `Change` references a nested class not in `kryoTypes()` (e.g. a nested `record OhlcChange.BarTick(Interval interval, ...)` where `Interval` is a record not registered)
- **THEN** the first `state.apply(...)` triggering serialization SHALL throw `StateSerializeException::UnregisteredClass("Interval")` and the JVM SHALL NOT silently succeed with auto-IDs

### Requirement: Public Java SDK handler classes use canonical Operative vocabulary

The Java SDK SHALL expose handler classes whose names use canonical vocabulary: `AddOperativeHandler`, `RemOperativeHandler`, `OperativeMessageHandler`, `EntityStateHandler`, `EntityApplyStateHandler`. Classes previously named `AddProcessorHandler`, `RemProcessorHandler`, `ProcessorMessageHandler`, `ProcessorStateHandler`, `ProcessorApplyStateHandler` SHALL NOT be present in the published API. Java packages SHALL likewise migrate any package-path components that contained `processor`, `operator`, `agent`, or `compute_engine` to the canonical replacements (`operative`, `runner`, `controller`). `FleetManager`-equivalent classes keep their names.

#### Scenario: Old handler classes do not exist
- **WHEN** a downstream Java consumer imports `com.flezha.sdk.handlers.AddProcessorHandler`
- **THEN** the import MUST fail to compile (class not found); the consumer MUST migrate to `com.flezha.sdk.handlers.AddOperativeHandler`

#### Scenario: New handlers wire into dispatch
- **WHEN** a Java SDK Runner receives an `AddOperative` protobuf command from the Controller
- **THEN** dispatch MUST route the message to `AddOperativeHandler.handle(...)` with the protobuf `operative_id` field exposed under the same name (`getOperativeId()`)

### Requirement: Public Operative / Procedure / Entity user-facing API names

The Java SDK SHALL expose user-extensible base classes/interfaces under the canonical names `StreamOperative`, `Procedure`, `Entity`, and `Channel`. The Runner process startup hook SHALL be `Runner.start(Config)` (or equivalent factory entry point); class names referring to the SDK process itself SHALL use `Runner` as the role term, not `ComputeEngine` or `Operator`.

#### Scenario: User extends StreamOperative
- **WHEN** a user-defined Operative class declares `extends StreamOperative`
- **THEN** the class MUST compile against the published Java SDK; declaring `extends Processor` MUST fail

### Requirement: Java SDK implements the effectively-once output tier

The Java SDK SHALL implement the `effectively-once-output` capability. A channel declaration SHALL support `c.effectivelyOnce()`; the runtime SHALL then, for each invocation, buffer the invocation's state changes, plugin outputs, consumed input offset, and (on a dead-letter park) its DLQ entry into ONE `entity_effect_record` frame rather than separate change and output frames.

The frame SHALL carry the 16-byte state header with `format_id = 2` and `sdk_lang = 2`, and its body SHALL be the postcard encoding of `EffectRecordBody { changes: Vec<Vec<u8>>, outputs: Vec<OutputRecord>, consumed_offset: Option<u64>, dlq: Option<DlqEntryBody>, event_time: u64 }` in exactly that field order, byte-identical to the Rust SDK. The `changes` elements SHALL be the SDK's own change bytes (Kryo, as a Java plain change record carries) — the controller stores and re-ships them without decoding.

The runtime SHALL read the inbound `flz.offset` metadata key to learn the input's offset, and SHALL re-forward replayed inputs on restore. A dead-letter park under the tier SHALL ride inside the effect record rather than emitting a separate `park_payload` frame, so that the park and the consumed-offset advance are one atomic record.

Declaring `.keyed()` SHALL be required on every plugin-bound channel of a tier-enabled class; a non-keyed plugin channel SHALL fail registration.

#### Scenario: A tier invocation ships one effect record, not separate frames

- **WHEN** a tier-enabled Procedure's `run` applies two Changes and emits one keyed output for an input carrying `flz.offset = 7`
- **THEN** the SDK SHALL ship exactly one `entity_effect_record` frame whose decoded body has `changes.length == 2`, `outputs.length == 1`, and `consumed_offset == 7`; and SHALL NOT ship any `entity_state_change` frame for that invocation

#### Scenario: Effect-record bytes are byte-identical to the Rust encoder

- **WHEN** the Java SDK encodes an `EffectRecordBody` with the same field values as a Rust fixture
- **THEN** the produced bytes SHALL equal the Rust `postcard::to_allocvec(EffectRecordBody)` fixture bytes exactly

#### Scenario: A poisoned tier invocation parks inside the effect record

- **WHEN** a tier-enabled Procedure exhausts its retry budget on a poison input at offset 9
- **THEN** the SDK SHALL ship one `entity_effect_record` whose body carries `dlq = Some(entry)` AND `consumed_offset == 9`, and SHALL NOT ship a separate `park_payload` frame

#### Scenario: A tier class with a non-keyed plugin channel fails registration

- **WHEN** a class declares `c.effectivelyOnce()` and a plugin-bound channel without `.keyed()`
- **THEN** registration SHALL throw `StreamOperativeConfigurationException` naming the channel

### Requirement: Java SDK implements checkpoint ack granularity

The Java SDK SHALL implement the `effectively-once-checkpoint-granularity` capability. A channel declaration SHALL support `c.checkpointAck()`, valid only together with `c.effectivelyOnce()` — declaring it alone SHALL fail registration with `CheckpointAckWithoutTier`. `ProcedureCtx.checkpoint()` SHALL mark the invocation as closing the current interval.

The per-Entity flush buffer SHALL carry a third `staged` list holding the open interval's changes. `drainToFrames` and `isEmpty` SHALL deliberately **ignore** `staged` — an open interval is not flushable, and draining it alone would put state ahead of its offset. An invocation SHALL emit an effect record carrying `{staged, consumedOffset}` when it checkpointed or parked to the DLQ; otherwise it SHALL emit `{[], outputs, null}` only if it produced outputs. A dead-letter park SHALL close the interval. A restore SHALL discard the open interval.

#### Scenario: An open interval holds the consumed offset

- **WHEN** a checkpoint-ack Procedure processes three inputs without calling `ctx.checkpoint()`
- **THEN** no effect record carrying a non-null `consumed_offset` SHALL be shipped, and the staged changes SHALL remain unflushed

#### Scenario: checkpoint() closes the interval and acknowledges the offset

- **WHEN** the Procedure's fourth invocation calls `ctx.checkpoint()` for an input at offset 3
- **THEN** the SDK SHALL ship one effect record whose `changes` are the four invocations' staged changes and whose `consumed_offset == 3`

#### Scenario: checkpointAck without the tier is refused

- **WHEN** a class declares `c.checkpointAck()` without `c.effectivelyOnce()`
- **THEN** registration SHALL throw `StreamOperativeConfigurationException` carrying the error name `CheckpointAckWithoutTier`

#### Scenario: A park closes the open interval

- **WHEN** a checkpoint-ack Procedure has staged two invocations' changes and the third input is poison and exhausts its retries
- **THEN** the shipped effect record SHALL carry the staged changes, the DLQ entry, and the poison input's `consumed_offset` — so the replay resumes after the poison offset

#### Scenario: A restore discards the open interval

- **WHEN** an Entity with a non-empty `staged` list is restored via `RestoreInitState`
- **THEN** the staged list SHALL be cleared, since it belongs to a past that no longer exists

### Requirement: Java SDK implements stream late-input correction

The Java SDK SHALL implement the `stream-correction` capability. A channel declaration SHALL support `c.correctionWindow(millis)` and `c.beyondWindowPolicy(policy)` where policy is `PROCESS_AS_ARRIVAL` (the default, today's behaviour) or `DROP`. Declaring a correction window SHALL require the effectively-once tier, an `EventTimeFrom` clock, and all channels keyed; violating any of these SHALL fail registration.

Every effect record SHALL carry the invocation's `event_time`. The runtime SHALL maintain a per-Entity event-time high-water mark, and SHALL classify an input whose event time is below the high-water by more than the declared window as beyond-window. A replay slice SHALL be recognised via the `flz.replay` metadata key and terminated by a payload-less `flz.replay_end` **broadcast** frame (intercepted before channel lookup, since an input routes to only one entity by key); the buffered slice SHALL be dispatched in **event-time order**, and a closed slice SHALL flush before any live input popped behind it.

A replay SHALL NOT lower the event-time high-water mark.

#### Scenario: A replayed slice dispatches in event-time order

- **WHEN** a replay slice delivers inputs with event times `[30, 10, 20]` followed by an `flz.replay_end` broadcast
- **THEN** the Procedure's `run` SHALL be invoked for them in event-time order `10, 20, 30`

#### Scenario: A closed slice flushes before a live input behind it

- **WHEN** a live input is popped from the mailbox behind a just-closed replay slice
- **THEN** the slice SHALL dispatch in full before the live input, so the live input cannot jump ahead of the inputs being re-ordered

#### Scenario: A replay does not lower the event-time high-water

- **WHEN** an Entity restored to an event-time high-water of `500` replays an unconsumed tail whose maximum event time is `300`
- **THEN** the high-water SHALL remain `500`, and a subsequent input at event time `320` SHALL still be classified as late

#### Scenario: A correction window without the tier is refused

- **WHEN** a class declares `c.correctionWindow(60_000)` without `c.effectivelyOnce()`
- **THEN** registration SHALL throw `StreamOperativeConfigurationException` naming the missing tier declaration

#### Scenario: Beyond-window default is process-as-arrival

- **WHEN** a class declares `c.correctionWindow(60_000)` without declaring a policy, and an input arrives whose event time is older than the window
- **THEN** the input SHALL be processed as an arrival (not dropped), since declaring a window must never silently start dropping data

### Requirement: Java SDK implements the workflow proc-journal and ProcCommit handshake

The Java SDK SHALL implement the `workflow-proc-journal` capability. At every proc barrier of a durable structured workflow class, the SDK SHALL ship `ProcCommit` (tag 28) carrying the procs that ran and the channel-tagged handoff blobs, and SHALL block on the controller's `ProcCommitResponse` (tag 29) under the same `reference_id` for up to 30 seconds. A failure or timeout SHALL produce a terminal error and trigger compensation.

Inter-proc channels of a durable structured workflow class SHALL declare a codec; a missing codec SHALL fail registration with `MissingInterProcCodec` naming the channel. Classes that opt out of persistence SHALL skip the journal and the handshake entirely, preserving re-run-from-top.

On resume, the SDK SHALL read the journal state shipped on `AddOperative.proc_journal`, SHALL **skip** procs already recorded as committed, and SHALL seed the next proc's batch from the journaled handoff via the channel codec.

#### Scenario: A committed proc is skipped on resume, not re-run

- **WHEN** a durable workflow class commits proc A, then the runner is SIGKILLed, then the operative is rebound with a journal recording proc A as committed
- **THEN** proc A's `run` SHALL NOT be invoked again, and proc B SHALL run seeded from proc A's journaled handoff

#### Scenario: A missing inter-proc codec fails registration

- **WHEN** a durable structured workflow class declares an inter-proc channel without a codec
- **THEN** registration SHALL throw `WorkflowOperativeConfigurationException` carrying the error name `MissingInterProcCodec` and naming the channel

#### Scenario: An opted-out class re-runs from the top

- **WHEN** a workflow class declares `persistenceEnabled() == false` and is resumed after a crash
- **THEN** the SDK SHALL ship no `ProcCommit`, and every proc SHALL re-run from the top

#### Scenario: A commit timeout triggers compensation

- **WHEN** the controller does not answer a `ProcCommit` within 30 seconds
- **THEN** the SDK SHALL treat the proc as terminally failed and SHALL run the compensation plan

### Requirement: Java SDK implements workflow staged commit and the transactional outbox

The Java SDK SHALL implement the `workflow-transactional-outbox` capability. Inside a structured proc of a durable workflow class, `ctx.emit` SHALL NOT reach the wire: `WorkflowCollectSink` SHALL buffer `(channel, bytes)` and the buffered outputs SHALL ride the same `ProcCommit` as the journal record, in the `outbox` field (`ProcOutboxItem{entity_id, seq_in_proc, channel, payload}`). A failing stage SHALL drop its staged bytes together with the error, so nothing leaks. `run`-sugar classes (no barrier to stage against) and transport-less runs SHALL keep the direct-emit path.

Compensation output SHALL ride the same handshake with `compensating` set.

#### Scenario: A proc's emits are staged, not shipped, until the barrier

- **WHEN** a structured proc of a durable workflow class calls `ctx.emit(v)` twice and then reaches its proc barrier
- **THEN** no plugin frame SHALL be sent during the proc body, and the `ProcCommit` at the barrier SHALL carry both outputs in its `outbox`

#### Scenario: A failing stage leaks no staged output

- **WHEN** a stage emits once and then throws
- **THEN** the staged bytes SHALL be dropped with the error and SHALL NOT appear in any subsequent `ProcCommit`

#### Scenario: run-sugar keeps the direct path

- **WHEN** a workflow class authored with `run`-sugar calls `ctx.emit(v)`
- **THEN** the frame SHALL be sent directly, as before this change

### Requirement: Java SDK implements the workflow durable loop and suspension

The Java SDK SHALL implement the `workflow-durable-loop` and `workflow-durable-suspension` capabilities. `WorkflowOperative` SHALL declare `loopMode()` returning `ONCE` (the default, byte-identical to the current single pass), `PER_INPUT` (one iteration per inbox item), or `CHAINED` (each iteration seeded by the previous iteration's terminal handoff), plus `wakeAfter()`.

At every iteration boundary the SDK SHALL ship `IterCommit` (tag 30) carrying the iteration index, the consumed inbox offset, and the terminal handoff, and SHALL await `IterCommitResponse` (tag 31). Registration SHALL reject a loop on `run`-sugar (`LoopWithoutChain`) and a durable `CHAINED` loop whose terminal proc has no codec'd output channel (`LoopTerminalWithoutCodec`).

When the loop is idle past the park hysteresis, the SDK SHALL ship `IterPark` (tag 32) and exit **without** `WorkflowCompleted`, yielding a parked outcome. The SDK SHALL read its inbox offset from the inbound `flz.inbox_offset` metadata key.

#### Scenario: Each loop iteration commits at its boundary

- **WHEN** a `PER_INPUT` workflow class processes three inbox items
- **THEN** the SDK SHALL ship three `IterCommit` commands, each carrying its iteration index and consumed inbox offset, and SHALL await each response before starting the next iteration

#### Scenario: An idle loop parks instead of completing

- **WHEN** a durable looping workflow class has no inbox item for longer than the park hysteresis
- **THEN** the SDK SHALL ship `IterPark` and exit without shipping `WorkflowCompleted`

#### Scenario: A resumed loop restarts at the journaled iteration

- **WHEN** a looping workflow class committed iteration 2 and is then rebound after a crash
- **THEN** the SDK SHALL resume at iteration 3, seeded (for `CHAINED`) from iteration 2's journaled terminal handoff

#### Scenario: A loop on run-sugar is refused

- **WHEN** a workflow class authored with `run`-sugar declares a non-`ONCE` `loopMode()`
- **THEN** registration SHALL throw `WorkflowOperativeConfigurationException` carrying the error name `LoopWithoutChain`

#### Scenario: A durable chained loop whose terminal proc has no codec is refused

- **WHEN** a durable `CHAINED` workflow class's terminal proc has no codec'd output channel
- **THEN** registration SHALL throw `WorkflowOperativeConfigurationException` carrying the error name `LoopTerminalWithoutCodec`

### Requirement: Java SDK implements the workflow entity retry ladder

The Java SDK SHALL implement the `workflow-entity-retry` capability. `WorkflowOperative` SHALL declare `retryAttempts()` (default 3 total attempts) and `retryBackoff()` (default 500 ms base). Only the failing entity SHALL re-run its batch; siblings SHALL stay buffered at the barrier, and each attempt SHALL use a fresh sink so a failed attempt's staged output is dropped rather than double-emitted.

Backoff SHALL reuse the dead-letter ladder's helper (`base × 2^(n-1)` with ±50% jitter, capped at 5 s) and the wait SHALL be cancellation-aware, so a stop or `maxDuration` ends it at once. Retry is strictly pre-commit: a journaled proc SHALL never be retried. `retryAttempts() == 0` SHALL collapse to a single attempt; `attempts > 1` with a zero backoff SHALL be rejected at registration with `ZeroRetryBackoff`. A `WorkflowException` SHALL carry a `nonRetryable` flag; a thrown `RuntimeException` SHALL stay retryable.

#### Scenario: A transient entity failure recovers on retry

- **WHEN** a workflow entity's batch throws on its first attempt and succeeds on its second
- **THEN** the operative SHALL complete successfully, only the failing entity SHALL have re-run, and its first attempt's staged output SHALL NOT be emitted

#### Scenario: An exhausted ladder compensates

- **WHEN** a workflow entity fails all three attempts
- **THEN** the operative SHALL abort and run the compensation plan

#### Scenario: A non-retryable error skips the ladder

- **WHEN** an entity throws `WorkflowException.nonRetryable("bad record")`
- **THEN** the SDK SHALL NOT retry and SHALL proceed directly to the abort path

#### Scenario: attempts > 1 with a zero backoff is refused

- **WHEN** a workflow class declares `retryAttempts() == 3` and `retryBackoff() == Duration.ZERO`
- **THEN** registration SHALL throw `WorkflowOperativeConfigurationException` carrying the error name `ZeroRetryBackoff`

### Requirement: Java SDK implements workflow loop rewind

The Java SDK SHALL implement the `workflow-loop-rewind` capability. On restore, the SDK SHALL read `ProcJournalState.rewind_undo` (the procs the controller's rewind marker determined are still owed an undo) and, before entering the forward loop, SHALL run those undos in reverse order under a **forced** all-or-nothing rollback mode — whatever the class's declared `rollbackMode()` says — since a rewind unwinds committed iterations by definition.

The SDK SHALL journal exactly one `compensated` record per undone proc, **including an empty one for a proc with no compensate hook**, since that record is what drains `rewind_undo`. The undo pass SHALL NOT consult the per-entity crash-resume done markers, which are per entity and would wrongly suppress a proc that ran in two rewound iterations.

A rewind SHALL NOT replay inputs: the controller truncates the inbox at each iteration commit, so a rewind is undo-and-continue — the loop resumes waiting and the next message becomes the following iteration.

#### Scenario: A rewind undoes committed iterations in reverse order

- **WHEN** the controller rewinds a looping workflow class to iteration 1 and the class committed iterations 2 and 3
- **THEN** on rebind the SDK SHALL run the compensate hooks for iterations 3 and 2's procs in reverse order before resuming the forward loop

#### Scenario: A hookless proc still drains the rewind undo

- **WHEN** a proc owed a rewind undo declares no compensate hook
- **THEN** the SDK SHALL journal an empty `compensated` record for it, so it is not re-offered on the next restore

#### Scenario: A rewind does not re-deliver consumed inputs

- **WHEN** a looping workflow class is rewound to iteration 1
- **THEN** the inputs consumed by iterations 2 and 3 SHALL NOT be re-delivered; the loop SHALL resume waiting, and the next inbound message SHALL become the next iteration

### Requirement: Java SDK implements workflow branching

The Java SDK SHALL implement the `workflow-branching` capability. `WorkflowSpec` SHALL model the proc graph as a tree (`order` plus `children`) rather than the current `List<Integer> chain`; a linear chain SHALL be the degenerate single-child case. `WorkflowSpec.linearChain()`'s refusal of any non-linear graph SHALL be removed and replaced with validation over the tree.

The SDK SHALL declare the tree's leaf procs to the controller via the `leaf_procs` key of `AddOperativeResponse.metadata`. Fan-in SHALL NOT silently overwrite an edge. Compensation SHALL run in reverse-topological order, which the existing BFS numbering already yields — the compensation planner SHALL NOT be changed.

#### Scenario: A branching chain runs its branches and joins

- **WHEN** a workflow class declares proc 0 feeding both proc 1 and proc 2, which both feed proc 3
- **THEN** procs 1 and 2 SHALL each run seeded from proc 0's handoff, and proc 3 SHALL run once both have committed

#### Scenario: Compensation runs in reverse-topological order

- **WHEN** the branching class above aborts after proc 3 commits
- **THEN** the compensate hooks SHALL run in the order 3, then 2 and 1, then 0

#### Scenario: Fan-in does not overwrite an edge

- **WHEN** two procs both feed the same downstream proc on the same channel
- **THEN** both edges SHALL be recorded, and neither SHALL silently replace the other

### Requirement: Java SDK data heartbeat reports dispatch progress

The Java SDK's `data_heartbeat` frame SHALL carry the same metrics the Rust SDK ships: `dispatches`, `blocked_parks`, and `credit_parked`. Reporting only `credit_parked` leaves the controller's `StallClassifier` unable to distinguish a live-but-wedged Java runner from a healthy idle one, so the omission is a liveness defect, not a cosmetic gap.

#### Scenario: The heartbeat carries dispatch progress

- **WHEN** a Java runner has dispatched 100 inputs since the previous heartbeat
- **THEN** the next `data_heartbeat` frame's metadata SHALL carry a `dispatches` count reflecting that progress, alongside `blocked_parks` and `credit_parked`

#### Scenario: A wedged Java runner is distinguishable from an idle one

- **WHEN** a Java runner stops making dispatch progress while inputs continue to arrive
- **THEN** the heartbeat's `dispatches` count SHALL stop advancing while the runner keeps sending heartbeats, giving the controller the signal it needs to localize the stall

### Requirement: Operative Type Declaration

The Java SDK SHALL provide `FlezhaRunner.defineStream(Class<? extends StreamOperative>)` and `FlezhaRunner.defineWorkflow(Class<? extends WorkflowOperative>)` as chainable builder methods, callable **before and after** `.start()`/`.run()`. For each declared StreamOperative class the SDK SHALL: (a) instantiate the class via its public no-arg constructor; (b) call its `factoryId()` method to obtain the controller-facing factory id; (c) call its `channels(ChannelBuilder)` method exactly once — the builder being the producer-first graph builder of `sdk-graph-authoring` — and freeze the resulting `OperativeTopology`; (d) verify wiring constraints (entry-channel deserializability, type-equality between a stream's payload type and its consumer Procedures' input types, node reachability from the entry, at most one output channel per node, acyclicity, node-identity and channel-name uniqueness, static-mode and dynamic-mode default-impl detection); (e) record `(factoryId → topology)` in the class registry. The registry SHALL stay mutable after `.start()`: `defineStream`/`defineWorkflow` and their `removeStream`/`removeWorkflow` inverses are legal on a started runner, and every post-start change SHALL re-sync the runner's full current factory set rather than a delta — the set the next connect/reconnect init frame carries to the controller. There SHALL be no `OperativeHandler`-style factory registration in the public surface.

Dynamic entity mode SHALL be supported: a Procedure reached by a consumer edge declaring `keyBy`, returning an empty list from `entityKeys()`, SHALL be declared successfully and create Entities lazily per key at dispatch time, per the `operative-state-persistence` dynamic-entity behaviour. Eviction SHALL be off by default (`entityIdleTtl()` returns `Optional.empty()`); when a TTL is declared, the per-Procedure reaper SHALL evict idle Entities destructively by shipping `delete_entity_state`, and a recreated Entity SHALL cold-start fresh rather than restore from disk.

Because Java's type erasure precludes Rust's compile-time topology refusals, every constraint Rust enforces at compile time SHALL be enforced at declaration time, throwing `StreamOperativeConfigurationException` whose message carries the same diagnostic content and the same error name as the corresponding Rust `TopologyIssue` variant.

#### Scenario: Declaring a stream operative class before start() succeeds
- **WHEN** user code calls `Flezha.createRunner("my_app").defineStream(MarketOperative.class).start()`
- **THEN** the handshake init JSON SHALL include `"market"` (the value `MarketOperative::factoryId` returns) in `factory_ids`, and subsequent `AddOperative{factory_id:"market"}` commands SHALL succeed by spawning an `StreamOperativeInstance`

#### Scenario: Declaring an operative type after start() is accepted
- **WHEN** user code calls `.defineStream(...)` on a started `FlezhaRunner` already declaring `"market"`, for a class whose `factoryId()` returns `"orders"`
- **THEN** the call SHALL succeed and return the runner, the new class SHALL be declared to the core immediately, and the runner's declared set SHALL become the full set `{"market", "orders"}` the next connect/reconnect init frame carries

#### Scenario: Duplicate factoryId across two stream operative classes is rejected
- **WHEN** two distinct StreamOperative classes both return `"market"` from `factoryId()` and both are declared on the same builder
- **THEN** the second `.defineStream(...)` call SHALL throw an `IllegalArgumentException` naming the duplicate factoryId

#### Scenario: Wiring mismatch fails declaration with a diagnosable error
- **WHEN** an Operative wires `.into(OhlcProcedure.class)` onto `ParseProcedure`'s output stream but `ParseProcedure.outputType()` returns `Trade.class` while `OhlcProcedure.inputType()` returns `Candle.class`
- **THEN** `.defineStream(...)` SHALL throw `StreamOperativeConfigurationException` whose message names the channel, the stream's payload type (`Trade`), and the consumer-input type (`Candle`)

#### Scenario: Typed entry channel deserializes at the edge
- **WHEN** an Operative declares `c.input(Trade.class, "trade.raw").into(ParseProcedure.class)` and a JSON-encoded Trade arrives
- **THEN** `.defineStream(...)` SHALL succeed and `ParseProcedure.run` SHALL receive the Jackson-deserialized `Trade`, per the typed-entry-input contract of `sdk-graph-authoring`

#### Scenario: Unreachable node fails declaration
- **WHEN** an Operative's `channels(c)` wires a Procedure that is not reachable from the entry channel
- **THEN** `.defineStream(...)` SHALL throw `StreamOperativeConfigurationException` naming the unreachable Procedure

#### Scenario: Static-mode procedure without entitiesForOverridden is rejected
- **WHEN** a Procedure returns a non-empty list from `entityKeys()` but its `entitiesForOverridden()` returns the default `false`
- **THEN** `.defineStream(...)` SHALL throw `StreamOperativeConfigurationException` naming the Procedure and stating that static-mode procedures must override `entitiesFor` and `entitiesForOverridden`

#### Scenario: Dynamic-mode procedure declares and creates entities lazily
- **WHEN** a Procedure returning an empty `entityKeys()` is wired through an edge declaring `keyBy`, and two inputs arrive whose key function returns `"BTC"` and `"ETH"`
- **THEN** `.defineStream(...)` SHALL succeed, and the runtime SHALL lazily allocate exactly two Entities keyed `"BTC"` and `"ETH"`, each constructed with `initState(Optional.of(key), Optional.empty())`

### Requirement: Runner process singleton guard

The Java SDK SHALL allow at most one runner to be **started** per JVM process. `Flezha.createRunner(...)` SHALL NOT claim the singleton slot — any number of unstarted runners may exist. The slot SHALL be claimed by compare-and-set at `.start()`/`.run()`; if a prior runner's `.start()`/`.run()` has already succeeded in this process and not yet closed, the call SHALL throw `IllegalStateException` naming the namespace of the already-running runner, and SHALL NOT connect, mutate any shared registry, or spawn any background thread. Calling `.start()` twice on the same runner SHALL throw `IllegalStateException`.

#### Scenario: Second start() in the same JVM is rejected
- **GIVEN** a JVM process where `Flezha.createRunner("ns1").defineStream(A.class).start()` has already succeeded
- **WHEN** code in the same process calls `Flezha.createRunner("ns2").defineStream(B.class).start()`
- **THEN** the call SHALL throw `IllegalStateException` whose message names `"ns1"` as the already-running namespace
- **AND** no connection SHALL be attempted for `"ns2"`, and the process's stable identity, entity-state sink, and credit window SHALL remain exclusively `"ns1"`'s

#### Scenario: Creating runners never claims the slot
- **GIVEN** a JVM process where a runner for `"ns1"` is already started
- **WHEN** code calls `Flezha.createRunner("ns2")` and mutates it with `define*`/`remove*` without calling `.start()`
- **THEN** no exception SHALL be thrown and `"ns1"` SHALL remain the process's active runner

#### Scenario: A runner may be started again after the first one closes
- **GIVEN** a JVM process where a runner for `"ns1"` was started and then fully closed via `.close()`
- **WHEN** code in the same process calls `Flezha.createRunner("ns2").defineStream(B.class).start()`
- **THEN** the call SHALL succeed and `"ns2"` SHALL become the process's active runner

#### Scenario: Concurrent start() calls — exactly one wins
- **GIVEN** a JVM process with no runner started yet
- **WHEN** two threads each call `.start()` on a distinct `FlezhaRunner` (`"ns1"` and `"ns2"`) at the same time
- **THEN** exactly one call SHALL succeed and become the process's active runner, and the other SHALL throw `IllegalStateException` naming the winner — the guard's compare-and-set SHALL make this outcome deterministic per-run (no scenario where both succeed or both fail)

### Requirement: Runner lifecycle — start, run, awaitTermination, close

`FlezhaRunner.start()` SHALL connect (per the `Java SDK Entry Point` requirement) and return `this` without blocking the calling thread for the runner's ongoing lifetime. `FlezhaRunner.awaitTermination()` SHALL block the calling thread until the runner shuts down (via `.close()`, SIGTERM-triggered graceful shutdown, or a controller-driven stop), replacing any user-authored keep-alive loop; called on a never-started runner it SHALL throw `IllegalStateException` ("runner not started"), as SHALL `getOperatives()`. `FlezhaRunner.run()` SHALL be exactly `.start()` followed by `awaitTermination()`, with no independently-implemented blocking logic. `FlezhaRunner` SHALL implement `AutoCloseable`; `.close()` on a started runner SHALL run the existing graceful-shutdown sequence (ship-fresh-state-on-sdk-shutdown), release the singleton slot, and cause any thread blocked in `awaitTermination()` to return; `.close()` on a never-started runner SHALL be a no-op; `.close()` SHALL be idempotent.

#### Scenario: start() returns immediately after the handshake, before shutdown
- **WHEN** an SDK user calls `.start()` on a runner
- **THEN** the call returns as soon as the runner has an assigned id, without waiting for the runner to ever shut down
- **AND** the runner continues dispatching in background threads exactly as it does today

#### Scenario: run() blocks until shutdown
- **WHEN** an SDK user calls `.run()` on a runner in a standalone `main()`
- **THEN** the call blocks the calling thread until the runner shuts down (SIGTERM or explicit close from another thread), and the JVM does not exit before then even though the runner's background threads are daemon

#### Scenario: close() wakes a thread blocked in awaitTermination()
- **GIVEN** a started `FlezhaRunner` with one thread blocked in `awaitTermination()`
- **WHEN** another thread calls `.close()` on the same runner
- **THEN** the graceful-shutdown sequence runs, and the blocked thread's `awaitTermination()` call returns

#### Scenario: Pre-start lifecycle calls
- **GIVEN** a `FlezhaRunner` created but never started
- **WHEN** code calls `.awaitTermination()` or `.getOperatives()`
- **THEN** each SHALL throw `IllegalStateException` stating the runner is not started
- **AND** calling `.close()` on the same never-started runner SHALL return without effect

#### Scenario: try-with-resources still works for short-lived usage
- **GIVEN** a test that needs a runner for the duration of one test method
- **WHEN** the test writes `try (FlezhaRunner r = Flezha.createRunner("test").defineStream(A.class).start()) { ... }`
- **THEN** the runner SHALL be started for the block and gracefully closed at the end of it, exactly as `AutoCloseable` usage requires

### Requirement: Flezha static accessor for the active runner

`Flezha.currentRunner()` SHALL return an `Optional<FlezhaRunner>` containing the process's active runner if one has been started and not yet closed, or `Optional.empty()` otherwise. It SHALL NOT throw, and SHALL NOT start a runner as a side effect. Runners that were created but never started SHALL never appear in `currentRunner()`.

#### Scenario: currentRunner() reflects the singleton's lifecycle
- **GIVEN** a JVM process with no runner started yet
- **WHEN** code calls `Flezha.currentRunner()`
- **THEN** it SHALL return `Optional.empty()`
- **AND** after `Flezha.createRunner("ns1").defineStream(A.class).start()` succeeds, `Flezha.currentRunner()` SHALL return `Optional.of(<that runner>)`
- **AND** after that runner's `.close()` completes, `Flezha.currentRunner()` SHALL return `Optional.empty()` again
- **AND** a runner created via `Flezha.createRunner(...)` but never started SHALL NOT appear in `currentRunner()`

### Requirement: Each Entity SHALL run on its own Procedure instance

Each Entity SHALL invoke `run` on its own Procedure instance, as the Rust SDK does by holding one Procedure instance per Entity alongside that Entity's State, so that a class authored against either SDK means the same thing.

Today `StreamOperativeInstance` builds one Procedure per `ProcedureDispatcher` and `ProcedureDispatcher` invokes `run` on that single object for every Entity it owns — static-key, keyless, and dynamic alike. The per-Entity instances it also constructs are used only for `initState` and a `schemaVersion()` read. No instance-level lock guards `run`; the per-Entity `stateLock` and `changeBufferLock` are distinct objects per Entity and serialise nothing between Entities, which drain concurrently on the worker pool. Any mutable field on a user's Procedure is therefore a shared, unsynchronised channel across Entity keys.

#### Scenario: Instance state does not leak between Entity keys

- **GIVEN** a Procedure declaring two Entity keys, whose implementation writes to a mutable instance field during `run`
- **WHEN** input is dispatched to both Entities
- **THEN** neither Entity SHALL observe the value written by the other

#### Scenario: Concurrent Entities do not race on one instance

- **GIVEN** a Procedure with multiple Entities draining concurrently on the worker pool
- **WHEN** each Entity's `run` writes to a mutable instance field
- **THEN** the writes SHALL NOT race, because each Entity holds its own instance

#### Scenario: Dynamic Entities each receive an instance

- **GIVEN** a dynamic-mode Procedure that creates Entities lazily per key
- **WHEN** an Entity is created for a newly-seen key
- **THEN** that Entity SHALL run on its own Procedure instance

#### Scenario: The dispatcher's documentation matches its behaviour

- **WHEN** the dispatcher documents the lifetime of the Procedure instance it holds
- **THEN** the documented lifetime SHALL be the implemented lifetime

### Requirement: The Java SDK SHALL document its actual state-mutation enforcement level

The Java SDK SHALL describe the state-mutation enforcement it actually provides, and SHALL document its asymmetry with Rust the way it already documents its other Rust asymmetries.

`StateHandle.get()` returns the live State object, so `state.get().field = x` compiles, runs, and is never journaled as a Change. The Rust SDK forecloses this structurally by implementing `Deref` and deliberately not `DerefMut`. The Java class javadoc and `docs/sdk/state_persistence.md` both assert that Java's lack of a setter is "the structural enforcement" — which it is not, because `get()` hands back the object.

#### Scenario: Documentation does not claim enforcement that does not exist

- **WHEN** Java SDK documentation describes how state mutation outside `apply` is prevented
- **THEN** it SHALL NOT assert structural enforcement unless a direct field mutation is actually prevented

#### Scenario: The asymmetry with Rust is stated

- **WHEN** a user consults the Java state-persistence documentation
- **THEN** it SHALL state that Rust forecloses direct mutation at compile time and that Java does not, and SHALL name the detection available instead

#### Scenario: A bypassed mutation's actual fate is described

- **WHEN** the documentation describes the consequence of mutating state outside `apply`
- **THEN** it SHALL state that the mutation is captured by graceful shutdown and live migration but lost by any restore, which rebuilds from snapshot plus change log

### Requirement: Factory visibility SHALL match its documentation

A factory or constructor's declared visibility SHALL match the visibility its documentation claims. `StateHandle.create(...)` is declared `public static` while its own javadoc calls it a "package-private factory" and the class javadoc claims a package-private constructor "forecloses user instantiation". No such enforcement exists.

#### Scenario: Declared visibility matches documented visibility

- **WHEN** a factory or constructor documents its visibility
- **THEN** the declared modifier SHALL match, or the documentation SHALL be corrected to describe the real visibility

### Requirement: The Java SDK SHALL emit an entity_state_snapshot on CompactEntity

`CompactEntityHandler` currently returns `new byte[0]` on its success path — a zero-length body where the Controller expects a `CompactEntityResponse` — with a comment describing unwritten work. Its "not yet implemented" error is reachable only from a `catch (ReflectiveOperationException)` branch that does not fire in normal procedure. The handler SHALL locate the addressed Entity, capture its state, ship an `entity_state_snapshot` data frame, and answer a well-formed `CompactEntityResponse` reporting the captured seq and byte size.

The snapshot body SHALL be encoded with the same codec the restore path decodes — Kryo, per `EntityStateRestore.rebuild`. The Jackson JSON serializer used for graceful shutdown and live migration SHALL NOT be used for the snapshot body.

#### Scenario: A live Entity produces a snapshot at its current seq

- **GIVEN** a live Entity with persisted Changes and a current change-seq
- **WHEN** the SDK receives `CompactEntity` addressing that Entity
- **THEN** the SDK SHALL ship one `entity_state_snapshot` frame carrying the Entity's serialized state stamped with a seq the Entity has reached
- **AND** SHALL answer a well-formed `CompactEntityResponse` reporting that seq and the body's byte size

#### Scenario: The response is never a zero-length body

- **WHEN** the Java `CompactEntity` handler completes, successfully or otherwise
- **THEN** it SHALL answer either a well-formed `CompactEntityResponse` or a reported error
- **AND** SHALL NOT answer a zero-length byte array on the success path

#### Scenario: The snapshot body round-trips through the restore path

- **GIVEN** a snapshot produced by the Java SDK for an Entity
- **WHEN** that snapshot is later shipped back as `RestoreInitState.snapshot_bytes`
- **THEN** `EntityStateRestore` SHALL decode it without error and SHALL reconstruct the state the Entity held when the snapshot was taken

### Requirement: The snapshot frame SHALL be stamped with its payload-id

A snapshot answered through the `SnapshotRequest` activation SHALL reach the wire framed by the core under the `entity_state_snapshot` payload-id the Controller routes on — the host returns naked state bytes and never frames.

#### Scenario: The frame is routable by the Controller

- **WHEN** the Java SDK ships a snapshot frame
- **THEN** the frame SHALL carry the `entity_state_snapshot` payload-id
- **AND** the Controller SHALL route it to its snapshot-write path

#### Scenario: No transport helper claims stamping it does not perform

- **WHEN** a transport helper's documentation states that it stamps a payload-id
- **THEN** it SHALL stamp that payload-id, or the documentation SHALL be corrected and the unused helper removed

### Requirement: The Java SDK SHALL ship its declared compact_after_bytes to the Controller

The Java SDK SHALL place its Procedure's declared `compactAfterBytes` value into the `AddOperative` metadata the Controller reads, so the per-Procedure threshold is resolvable. Today the value reaches `ProcedureDescriptor` and stops — `compact_after_bytes` appears nowhere in the Java SDK's metadata construction, so a user's override changes nothing.

The **default value** itself is owned by `java-state-guard-and-dead-knobs`, which aligns it with Rust and the Controller and runs first. This requirement covers only the transport of whatever value is declared.

#### Scenario: A declared override reaches the Controller

- **WHEN** a Java Procedure declares a `compactAfterBytes` different from the default
- **THEN** the SDK SHALL ship that value in the `AddOperative` metadata
- **AND** the Controller SHALL resolve it as that Procedure's size threshold

### Requirement: The Java runtime is a poll host over the shared core

The Java SDK's runtime SHALL be hosted on `flz_core` through the C ABI: a
small fixed set of platform **fetcher** threads (default 2;
`FLEZHA_WORKER_POOL_SIZE` overrides the count) SHALL loop `poll → decode
Activation → hand off to a virtual thread → poll again`, and the virtual
thread SHALL run the typed dispatch, encode the ActivationResult, and call
`complete` itself — completion from a thread other than the poller is the
core's documented contract. Fetchers SHALL be platform threads, never virtual
ones: `FlzCore.poll` blocks in native code, which would pin a virtual
thread's carrier. The runtime SHALL NOT reimplement kernel mechanisms the
core owns — command handling, flushing, credit, DLQ parks, timer wheels, the
workflow driver, reconnect, wire framing — and the classes that did
(`runtime/control` handlers, `EntityFlusher`, `CreditWindow`, `DlqPark`,
`TimerWheel`, `WorkflowOperativeRunner`, the reconnect loop, `AckRegistry`)
SHALL be deleted, not shimmed. The Java SDK SHALL encode and decode the
Activation/ActivationResult schema byte-identically to the core's hex-pinned
fixtures, reproduced in a Java fixture suite.

#### Scenario: Kernel classes are gone
- **WHEN** user or test code references the deleted kernel classes (for example `com.flezha.sdk.runtime.state.EntityFlusher` or a `runtime/control/handlers` class)
- **THEN** compilation SHALL fail — the parallel mechanism is removed with no compatibility layer

#### Scenario: Java reproduces the schema fixtures byte-identically
- **WHEN** the Java fixture suite encodes the canonical work activation, result, and all command-kind fixtures from `flz_core::schema`
- **THEN** the produced bytes SHALL equal the core's pinned hex literals exactly, and a mismatch SHALL fail the build

#### Scenario: Per-entity state is host-held without host locking
- **WHEN** two activations for two different entities of one Procedure run concurrently on virtual threads
- **THEN** each SHALL touch only its own entity's materialized state — per-entity exclusivity is the core's handout guarantee, and the Java layer performs no entity-level locking of its own

#### Scenario: Blocking user code does not consume a fetcher
- **WHEN** activations that block for tens of seconds (e.g., an LLM call) are
  in flight for many entities at once
- **THEN** the fetchers SHALL keep polling and handing out further
  activations, and work for unblocked entities SHALL proceed — concurrency is
  bounded by the core's jobs-underway limit, not by the fetcher count

### Requirement: The Java class declaration carries per-Procedure schema versions

The Java SDK's host class declaration SHALL carry each Procedure's own
`schema_version` and `min_readable_schema_version`. It SHALL NOT reduce the
class's Procedures to a single class-wide value.

**BREAKING** for any Java class whose Procedures declare differing schema
versions: their records were previously stamped with the class maximum.

#### Scenario: two Procedures with different versions declare both

- **GIVEN** a Java class whose Procedure A declares schema version 2 and Procedure B declares schema version 1
- **WHEN** the class declaration is built
- **THEN** the declaration SHALL carry 2 for A and 1 for B, and SHALL NOT carry 2 for both

### Requirement: The Java Procedure surface declares migrations

The Java `Procedure` surface SHALL expose the same migration declaration the
Rust surface does: a readable-from floor and entry points that convert a state
snapshot and a change record from a named older schema version, both defaulting
to a refusal.

#### Scenario: a Java Procedure restores state written at an older version

- **GIVEN** a Java Procedure at schema version 2 declaring a readable-from floor of 1 and a state migration
- **WHEN** an Entity whose snapshot was written at version 1 restores
- **THEN** the migration SHALL be invoked with `from_version = 1` and the snapshot bytes, and the Entity SHALL resume with the value it returned

