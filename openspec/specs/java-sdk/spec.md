# java-sdk Specification

## Purpose

Defines the Java SDK for Flezha compute engines: the public authoring API and the Operative / Tasklet / Cell runtime, hosted on the shared poll core through the `FlzCore` JNI binding over `flz_core_ffi`. Conforms to `docs/sdk/protocol_spec.md` and `docs/sdk/poll_core.md`.
## Requirements
### Requirement: Java SDK Entry Point

The Java SDK SHALL provide `com.flezha.sdk.Flezha.createRunner(String namespace)` as the single entry point, returning a concrete `FlezhaRunner` directly; there SHALL be no separate `RunnerBuilder` type. Creating a runner SHALL NOT connect, claim the process singleton slot, or spawn any thread. Calling `.start()` on the runner MUST connect to the controller's control socket, send the handshake init message per `docs/sdk/protocol_spec.md` §5.2 carrying the runner's currently-declared factory ids (possibly empty), wait for `AssignRunnerId`, and return `this` for chaining. The started runner SHALL expose `getNamespace()` (replacing `getId()`) and `getOperatives()` (a snapshot of currently-spawned Operative instances keyed by what the wire calls `operative_id`). `.run()` SHALL be sugar for `.start()` followed by `awaitTermination()`, never an independent implementation.

#### Scenario: Successful connection on a created runner
- **WHEN** an SDK user calls `Flezha.createRunner("my_app").registerFactory(MarketOperative.class).start()` with the controller running
- **THEN** the call blocks until the controller responds with `AssignRunnerId`, returns the same `FlezhaRunner`, and `getNamespace()` returns `"my_app"`

#### Scenario: Controller unavailable triggers exponential backoff retries
- **WHEN** an SDK user calls `.start()` on a runner and the controller's control socket is not yet listening
- **THEN** the SDK SHALL retry the connect with exponential backoff starting at 200 ms and capped at 30 s, and succeed once the controller becomes available

#### Scenario: start() with zero defined operative types is legal
- **WHEN** `.start()` is called on a `FlezhaRunner` with no `.registerFactory(...)` calls, whatever short-form classes are on the classpath
- **THEN** the runner SHALL connect successfully with an empty `factory_ids` list in the handshake init message, and operative types MAY be defined later on the live runner

#### Scenario: RunnerBuilder is not importable
- **WHEN** user code references `com.flezha.sdk.RunnerBuilder`, `com.flezha.sdk.operative.StreamOperativeRegistry`, `com.flezha.sdk.operative.WorkflowOperativeRegistry`, or `Flezha.newRunner`
- **THEN** compilation SHALL fail: the builder type, both public registry facades, and the old entry-point name are deleted with no shim

### Requirement: Tasklet Interface and Per-Cell Dispatch

The Java SDK SHALL expose a generic `Tasklet<Input, State, Output, Change, Clock, Config>` interface in `com.flezha.sdk.tasklet`. Implementers SHALL provide `name()`, `inputType() → Class<Input>`, `outputType() → Class<Output>`, `initState(Optional<String> cellKey, Optional<RestoreInitPayload> initPayload) → State`, and `run(Input, StateHandle<State, Change>, TaskletCtx) throws TaskletException`. The default-method overrides `cellKeys()` and `cellsFor(Input)` declare static keying; dynamic keying is declared on the WIRING (`keyBy` on the consumer edge), not on the Tasklet. The cell mode is therefore single by default, static when `cellKeys()` is non-empty and the implementing class declares `cellsFor`, and dynamic when a consumer edge into the Tasklet carries a key function. The interface SHALL carry no `*Overridden()` companion for any optional hook: whether a hook is declared SHALL be read at registration from the implementing class, by comparing the declaring class of the hook's erased method against `Tasklet` itself, and a hook declared on an intermediate class in the hierarchy SHALL count as declared. This covers `compensate`, `cellsFor`, `eventTime`, `mailboxCapacity` and `maxCells`, and it is the same derivation the authoring contract already applies to the type tokens and `kryoTypes()`. The runtime SHALL invoke `run` on a per-cell basis with sticky ordering (see the Sticky Per-Cell Ordering requirement) and SHALL pass each input to all cells returned by `cellsFor(input)` for static-mode tasklets (single-mode tasklets skip the call and route to cell index 0).

The `TaskletCtx` parameter SHALL provide:

- `emit(value)` for inter-tasklet or plugin-bound outputs, `emit(channel, value)` for explicit channel selection, and `emitWithMeta(value, meta)` / `emitWithMeta(channel, value, meta)` for plugin-bound outputs with user-attached metadata. `emitWithMeta` on inter-tasklet channels SHALL throw `TaskletException.MetadataNotAllowedOnInterTaskletChannel` naming the channel.
- `emitKeyed(key, value)` and `emitKeyedWithMeta(key, value, meta)` for channels declared `.keyed()`, supplying an output key that differs from the Cell's own. Emitting **without** a key on a `.keyed()` channel SHALL carry the invoking Cell's key when it has one, and SHALL throw `TaskletException.KeyRequiredOnKeyedChannel` when it does not (a single-cell Tasklet); emitting **with** a key on a channel not declared `.keyed()` SHALL throw `TaskletException.KeyNotAllowedOnNonKeyedChannel`. Both name the channel.
- `Optional<String> cellKey()` — the key this Cell was routed by, empty for a single-cell Tasklet. It is part of the invocation's identity, so it is replay-stable.
- `long now()` — the invocation's time value, computed **once** by the runtime from the Tasklet's declared `Clock` before `run` executes, and returning that same value for every call within the invocation. This is the determinism-contract replacement for `System.currentTimeMillis()`.
- `StableId stableId()` — a deterministic, replay-stable 128-bit identifier derived from the Cell identity, the invocation's durable change seq, and a per-invocation call ordinal. Repeated calls within one invocation differ; the n-th call of a replayed invocation SHALL equal the n-th call of the original. This is the determinism-contract replacement for `UUID.randomUUID()`.
- `void checkpoint()` — see the checkpoint ack-granularity requirement.

The runtime SHALL stamp `flz.cell`, `flz.seq`, and (on keyed emits) `flz.key` onto plugin-bound frames. User-supplied metadata keys in the reserved `flz.` namespace SHALL be **rejected** with `TaskletException.ReservedMetadataKey` naming the key, so that identity stamps can neither be spoofed nor clobbered.

#### Scenario: Single-mode Tasklet runs on a single cell
- **WHEN** a Tasklet whose `cellKeys()` returns an empty list is registered as the consumer of an entry channel, and one input arrives on that channel
- **THEN** the SDK SHALL invoke the Tasklet's `run` once on the single allocated cell (cell index 0), with `state` argument equal to the value `initState(Optional.empty(), Optional.empty())` returned at operative startup

#### Scenario: Static-mode Tasklet creates one cell per cellKey
- **WHEN** a Tasklet's `cellKeys()` returns `["1m", "1h", "1d", "1w"]` and its class declares `cellsFor`
- **THEN** at operative startup the SDK SHALL allocate exactly four `CellSlot` instances for that Tasklet, each constructed with `initState(Optional.of(key), Optional.empty())` for its corresponding key

#### Scenario: Static-mode cellsFor fan-out enqueues input on each named cell
- **WHEN** a Tasklet's `cellsFor(input)` returns `["1m", "1h"]` for a given input
- **THEN** the SDK SHALL enqueue that input on the cells identified by `"1m"` and `"1h"` (each receives its own `run` invocation), and SHALL NOT enqueue it on `"1d"` or `"1w"`

#### Scenario: Run throwing TaskletException logs and continues
- **WHEN** `Tasklet.run(...)` throws `TaskletException` on a given input and the input channel's retry budget is exhausted
- **THEN** the SDK SHALL log the exception with `(operativeInstanceId, taskletName, cellKey)` context, SHALL discard any partial `ctx.emit(...)` calls made during this invocation, SHALL park the payload to the dead-letter queue, and SHALL continue dispatching subsequent inputs on the same cell

#### Scenario: Run throwing arbitrary RuntimeException logs and continues
- **WHEN** `Tasklet.run(...)` throws a non-`TaskletException` `RuntimeException`
- **THEN** the SDK SHALL log the exception with the same `(operativeInstanceId, taskletName, cellKey)` context, discard partial emits, and continue dispatching subsequent inputs on the same cell

#### Scenario: Inter-tasklet zero-copy preserves value identity
- **WHEN** Tasklet A emits a non-null value `v` on an inter-tasklet channel that Tasklet B consumes (single-cell fan-out, single JVM)
- **THEN** the value passed to Tasklet B's `run` SHALL be `==`-equal (reference-equal) to `v` — the runtime SHALL NOT copy or serialize the value between Tasklets in the same JVM

#### Scenario: emitWithMeta on an inter-tasklet channel returns error
- **WHEN** a Tasklet whose declared output is an inter-tasklet channel calls `ctx.emitWithMeta(value, Map.of("k", "v"))`
- **THEN** the SDK SHALL throw `TaskletException.MetadataNotAllowedOnInterTaskletChannel` whose message names the channel

#### Scenario: Plugin-bound emit attaches the channel name as metadata on the wire
- **WHEN** a Tasklet whose declared output is a plugin-bound channel `"ohlc.candles"` calls `ctx.emit(candle)`
- **THEN** the SDK SHALL serialize `candle` via the channel's serializer, frame the bytes with `metadata` containing `"channel" -> "ohlc.candles"` plus the runtime's `flz.cell` and `flz.seq` identity stamps, and send the frame on the operative's secondary stream

#### Scenario: Plugin-bound emitWithMeta appends user metadata after the channel key
- **WHEN** a Tasklet calls `ctx.emitWithMeta(candle, Map.of("trace_id", "abc"))` on a plugin-bound channel `"ohlc.candles"`
- **THEN** the SDK SHALL frame the bytes with `metadata` containing `"channel" -> "ohlc.candles"`, `"trace_id" -> "abc"`, and the runtime identity stamps

#### Scenario: Reserved flz. metadata key in user map is rejected
- **WHEN** a Tasklet calls `ctx.emitWithMeta(candle, Map.of("flz.key", "spoofed"))` on a plugin-bound channel
- **THEN** the SDK SHALL throw `TaskletException.ReservedMetadataKey` naming `"flz.key"`, and SHALL NOT emit the frame

#### Scenario: now() is constant within one invocation
- **WHEN** a Tasklet's `run` calls `ctx.now()` three times
- **THEN** all three calls SHALL return the identical value — the one the runtime computed from the declared `Clock` before entering `run`

#### Scenario: stableId() is replay-stable and ordinal-distinct
- **WHEN** a Tasklet's `run` calls `ctx.stableId()` twice for a given input, and the same input is later replayed at the same durable change seq
- **THEN** the two calls within one invocation SHALL return different ids, AND the n-th call of the replayed invocation SHALL return an id byte-equal to the n-th call of the original

#### Scenario: stableId() is byte-identical to the Rust SDK
- **WHEN** the Java SDK derives a `StableId` from cell identity `s`, change seq `q`, and ordinal `n`
- **THEN** the resulting 128-bit value SHALL equal the Rust SDK's `StableId::derive(s, q, n)` (FNV-1a-128 over `s ‖ big-endian q ‖ big-endian n`), and its 32-hex-digit rendering SHALL match Rust's `{:032x}`

#### Scenario: emitKeyed on a non-keyed channel is rejected
- **WHEN** a Tasklet calls `ctx.emitKeyed("BTC", candle)` on a channel not declared `.keyed()`
- **THEN** the SDK SHALL throw `TaskletException.KeyNotAllowedOnNonKeyedChannel` naming the channel

#### Scenario: Keyed emit stamps flz.key on the wire
- **WHEN** a Tasklet calls `ctx.emitKeyed("BTC", candle)` on a channel declared `.keyed()`
- **THEN** the plugin frame's metadata SHALL carry `flz.key -> "BTC"`, `flz.cell -> "<instance>/<tasklet>/<cellKey>"`, and `flz.seq -> "<changeSeq>.<emitOrdinal>"`

#### Scenario: A key-less emit in a keyed Cell carries that Cell's key
- **WHEN** the Cell routed by `"BTC/USD"` calls `ctx.emit(candle)` on a channel declared `.keyed()`
- **THEN** the plugin frame SHALL carry `flz.key -> "BTC/USD"`, identically to `ctx.emitKeyed("BTC/USD", candle)`

#### Scenario: A key-less emit on a keyed channel is refused where the Cell has no key
- **WHEN** a single-cell Tasklet calls `ctx.emit(candle)` on a channel declared `.keyed()`
- **THEN** the SDK SHALL throw `TaskletException.KeyRequiredOnKeyedChannel` naming the channel, and nothing SHALL be emitted

### Requirement: Sticky Per-Cell Ordering Under ThreadPool Dispatch

The runtime SHALL guarantee that for any single `(operativeInstance,
tasklet, cell)` triple, two invocations of that Tasklet's `run(...)`
never execute concurrently, and that invocations observe their inputs in
enqueue order.
This invariant is supplied by the core's per-cell handout exclusivity —
a cell with an outstanding activation is never handed out again until
its completion — and MUST hold regardless of poller count, Operative count,
Cell count, or transport.

#### Scenario: Same cell never runs concurrently under load
- **WHEN** 10,000 inputs targeting one cell arrive back-to-back while 16 Java pollers poll
- **THEN** at any wall-clock instant at most one thread SHALL be inside that Cell's `run(...)`

#### Scenario: Different cells of same tasklet may run concurrently
- **WHEN** inputs arrive simultaneously for two distinct cells of the same Tasklet and ≥ 2 pollers are polling
- **THEN** the two Cells' `run(...)` methods MAY execute concurrently on different threads

#### Scenario: Per-cell inputs delivered in arrival order
- **WHEN** inputs `"a"`, `"b"`, `"c"` are enqueued for one Cell in that order with an append-to-state Tasklet
- **THEN** the Cell's State SHALL end with `["a", "b", "c"]` in exactly that order

### Requirement: Command Dispatch

Control commands SHALL be handled by the core. Of the command surface, only
the five host-participating interactions SHALL reach Java — as activation
kinds (`RestoreFold`, `SnapshotRequest`, `LiveStateRead`, `ApplyState`,
`MigrationSerialize`), subject to per-cell exclusivity; every other
command (including `Ping`, credit frames, acks, `AssignRunnerId`,
`ConnectSocket`, `RemOperative`, `WorkflowStop`) SHALL be answered by the
core without surfacing to Java. Class capability declarations
(`effectively_once`, `ack_granularity`, `correction_window_ms`,
`beyond_window_policy`, `compensable_tasks`, `leaf_tasks`,
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
- **WHEN** the controller ships `RestoreInitState` for a cell with a persisted change log
- **THEN** Java SHALL receive a `RestoreFold` activation, fold the snapshot and Kryo change records through its existing decode paths, and no input activation for that cell SHALL be handed out before the fold completes

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
tasklet × 1 cell × 10K msg`, no-op Tasklet, UDS transport, default
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
throwable is not sufficient, because an unanswered activation retires its cell
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
- **WHEN** a throwable escapes the dispatcher, the result encoder, or `complete` for an activation on cell E
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

### Requirement: Tasklet interface declares Change, Clock, Config generic parameters

The Java SDK's `Tasklet` interface SHALL be parameterized as `Tasklet<Input, State, Output, Change extends com.flezha.sdk.state.Change, Clock extends com.flezha.sdk.state.Clock, Config>`. The interface SHALL declare reflection methods `Class<Change> changeType()`, `Class<Clock> clockType()`, `Class<Config> configType()` returning the runtime `Class` for each generic parameter (Java type erasure precludes recovering them otherwise).

The interface SHALL declare lifecycle methods:
- `default void applyChange(State state, Change change)` whose default impl is a no-op for `NoChange` and throws `UnsupportedOperationException` for any other Change type (forcing user override).
- `default Optional<CoalesceKey> coalesceKey(Change change)` whose default impl delegates to `change.coalesceKey()`.
- `default long compactAfterBytes()` returning `64L * 1024 * 1024`.
- `default int schemaVersion()` returning `1`.
- `default List<Class<?>> kryoTypes()` returning `List.of()` — the user-declared list of types to register with Kryo for serialization, in stable-ID order.
- `default Tasklet<...> fromConfig(Config cfg)` whose default invokes the no-arg constructor when `Config = Void`, and throws `UnsupportedOperationException` for non-Void Config types (forcing user override).

#### Scenario: Default Change is NoChange skips persistence machinery

- **WHEN** a Java Tasklet does not declare its `Change` parameter (defaults to `NoChange`)
- **THEN** the runtime SHALL detect via `changeType() == NoChange.class` (reference equality) and its completions SHALL carry no change bytes, so the core registers no flusher work for this Cell

#### Scenario: User-defined Change opts the Tasklet into persistence

- **WHEN** a Java Tasklet declares `Change = MyChange` (any non-`NoChange` user type implementing `com.flezha.sdk.state.Change`)
- **THEN** the runtime SHALL register a typed flush buffer (`List<MyChange>`) on each Cell of that Tasklet and SHALL include those Cells in the per-JVM flusher's drain rotation

#### Scenario: Default applyChange throws on non-NoChange Change without override

- **WHEN** a Java Tasklet declares `Change = MyChange` but does NOT override `applyChange`
- **THEN** the operative registration step SHALL succeed (the default impl exists) but the first `state.apply(MyChange.foo())` call SHALL throw `UnsupportedOperationException` whose message names the missing override

#### Scenario: Default fromConfig invokes no-arg constructor for Void Config

- **WHEN** a Java Tasklet declares `Config = Void` and does NOT override `fromConfig`
- **THEN** the runtime's `fromConfig(null)` invocation SHALL call the Tasklet's public no-arg constructor via reflection; absence of a no-arg constructor SHALL throw `IllegalStateException` whose message names the Tasklet

#### Scenario: User-defined Config ships through Jackson deserialization

- **WHEN** a Java Tasklet declares `Config = MyConfig` (a Jackson-deserializable POJO/record) and overrides `fromConfig(MyConfig cfg)`
- **THEN** the runtime SHALL deserialize the agent-shipped JSON config bytes via Jackson into `MyConfig` and SHALL invoke `tasklet.fromConfig(cfg)` to construct the Tasklet at Cell init; deserialization failure SHALL throw `StreamOperativeConfigurationException("Config deserialize failed: " + taskletName)`

### Requirement: Tasklet.run receives StateHandle, not direct State reference

The Java SDK's `Tasklet.run` signature SHALL be `void run(Input input, StateHandle<State, Change> state, TaskletCtx ctx) throws TaskletException` (was `void run(Input input, State state, TaskletCtx ctx)`). `StateHandle<State, Change>` SHALL provide:

- `State get()` — read access to the underlying state.
- `void apply(Change change)` — runs `Tasklet::applyChange(state, change)` and pushes the Change onto the per-Cell flush buffer.
- NO setter, no direct-mutation method. Direct `state.field = value` is impossible without going through `apply`.

#### Scenario: Direct field mutation requires apply

- **WHEN** a Java Tasklet's `run` body wants to mutate `state.someField` from a value `v1` to `v2`
- **THEN** the user MUST declare a Change (e.g. `record SetSomeField(Object newValue) implements Change {}`), implement `applyChange` to write the field, and call `state.apply(new SetSomeField(v2))`; direct `state.get().someField = v2` is technically possible (mutable field on the underlying State) but bypasses persistence and breaks `replayEquals` — flagged as user error in documentation but not structurally prevented in Java (unlike Rust's `Deref` chokepoint)

#### Scenario: Read access via get() is allocation-free

- **WHEN** a Java Tasklet's `run` body reads `state.get().someField`
- **THEN** the read SHALL succeed via the handle's `get()` method (one method invocation, JIT-inlinable), no Change recording, no buffer mutation

#### Scenario: Mutation via apply runs applyChange and records

- **WHEN** a Java Tasklet's `run` body calls `state.apply(MyChange.foo())`
- **THEN** `Tasklet.applyChange(state, &MyChange.foo())` SHALL run on the underlying state AND `MyChange.foo()` SHALL be appended to the per-Cell flush buffer (observable via `TaskletTestHarness.recordedChanges()` and via the per-JVM flusher's next tick)

### Requirement: Tasklet declares Clock generic parameter with default WallClock

The Java SDK's `Tasklet` interface SHALL declare `Clock extends com.flezha.sdk.state.Clock` as a generic parameter, defaulting to `com.flezha.sdk.state.WallClock`. `Clock` SHALL be a sealed Java interface with `permits WallClock, LogicalClock, EventTimeFrom`. The runtime SHALL evaluate the declared clock **once per invocation**, before entering `run`, and serve that single value from `ctx.now()`:

- `WallClock` — epoch milliseconds at dispatch entry.
- `EventTimeFrom` — the event time extracted from the current input. Because Java's erasure precludes Rust's compile-enforced `TimeExtractor<Input>` bound, a Tasklet declaring `EventTimeFrom` SHALL override `default OptionalLong eventTime(Input input)`; failing to do so SHALL fail Operative registration with `StreamOperativeConfigurationException` naming the Tasklet and the missing override.
- `LogicalClock` — the raw durable `change_seq`, re-armed to `terminal + 1` on restore. A Tasklet declaring `LogicalClock` with `Change = NoChange` SHALL fail registration with `StreamOperativeConfigurationException` carrying the Rust error name `UnservableClock`, since there is no durable seq to serve.

`StreamOperativeConfigurationException::ClockNotImplementedYet` SHALL NOT be thrown for any member of the sealed hierarchy.

#### Scenario: Default Clock is WallClock

- **WHEN** a Java Tasklet does not declare its `Clock` parameter
- **THEN** `Tasklet.clockType() == WallClock.class` AND Operative registration SHALL succeed AND `ctx.now()` SHALL return epoch milliseconds sampled once at dispatch entry

#### Scenario: EventTimeFrom serves the extracted input time

- **WHEN** a Java Tasklet declares `Clock = EventTimeFrom` and overrides `eventTime(input)` to return the input's timestamp field
- **THEN** registration SHALL succeed AND `ctx.now()` SHALL return that extracted timestamp for the invocation

#### Scenario: EventTimeFrom without an eventTime override fails registration

- **WHEN** a Java Tasklet declares `Clock = EventTimeFrom` but does not override `eventTime(Input)`
- **THEN** `.registerFactory(...)` SHALL throw `StreamOperativeConfigurationException` naming the Tasklet and the missing `eventTime` override

#### Scenario: LogicalClock serves the durable change seq and re-arms on restore

- **WHEN** a Java Tasklet declares `Clock = LogicalClock` and a non-`NoChange` Change, and the Cell is restored from a change log whose terminal seq is 41
- **THEN** registration SHALL succeed AND the first post-restore invocation's `ctx.now()` SHALL return `42`

#### Scenario: LogicalClock with NoChange fails registration

- **WHEN** a Java Tasklet declares `Clock = LogicalClock` and `Change = NoChange`
- **THEN** `.registerFactory(...)` SHALL throw `StreamOperativeConfigurationException` whose message carries the error name `UnservableClock`

### Requirement: Operative declares persistenceEnabled default-method, frozen at instance start

The Java SDK's `StreamOperative` and `WorkflowOperative` interfaces SHALL each declare `default boolean persistenceEnabled() { return true; }` and `default boolean sleepWhenIdle()` (`false` on `StreamOperative`, `persistenceEnabled()` on `WorkflowOperative`). The runtime SHALL read `persistenceEnabled()` from the class for either kind when it builds the class declaration, SHALL write it as the declaration's `persistent` and SHALL NOT derive `persistent` from whether any Tasklet declares a Change other than `NoChange`; it SHALL write `sleepWhenIdle()` as `sleep_when_idle`. The core resolves the per-instance value from the declaration when `AddOperative` is processed and announces both to the controller.

The resolved value SHALL be bound to the instance and SHALL be propagated to every `CellSlot` constructed for that instance. A change to the class's method (via SDK redeploy) SHALL NOT affect already-running instances.

#### Scenario: Default-on with no override

- **WHEN** an Operative's `persistenceEnabled()` is the default (returns `true`)
- **THEN** the class declaration SHALL announce `persistent: true` and the instance SHALL resolve `persistenceEnabled = true`

#### Scenario: Code-declared opt-out is honored

- **WHEN** `MarketOperative.persistenceEnabled()` returns `false` (user override)
- **THEN** the class declaration SHALL announce `persistent: false` and the core SHALL drain-and-discard this instance's change bytes — no `cell_state_change` frame reaches the wire

#### Scenario: A stream's opt-out is read from the class, not its Tasklets

- **WHEN** a `StreamOperative` whose Tasklets declare Changes other than `NoChange` returns `false` from `persistenceEnabled()`
- **THEN** the class declaration SHALL announce `persistent: false`, no `cell_state_change` frame SHALL reach the wire, and a rebind after the runner dies SHALL restore nothing

#### Scenario: Sleeping without persistence is refused

- **WHEN** a class of either kind returns `true` from `sleepWhenIdle()` and `false` from `persistenceEnabled()`
- **THEN** `registerFactory` SHALL throw a configuration exception whose message contains `SleepWithoutPersistence`

#### Scenario: Resolved value frozen at instance start

- **WHEN** an instance is constructed with resolved `persistenceEnabled = true`, AND the class is redeployed returning `false` while the instance runs
- **THEN** the running instance SHALL continue with `persistenceEnabled = true`; the new value SHALL take effect only on the next `stop_operative` + `start_operative` cycle for that instance

### Requirement: initState accepts optional restoration payload

The Java SDK's `Tasklet.initState` signature SHALL be `State initState(Optional<String> cellKey, Optional<RestoreInitPayload> initPayload)`. When `initPayload.isEmpty()`, `initState` SHALL behave as a fresh-start constructor. When `initPayload.isPresent()`, the user MAY inspect or ignore the payload bytes; the runtime SHALL automatically apply the payload AFTER `initState` returns:

1. If `payload.snapshotBytes().length > 0`, deserialize via `KryoCodec` (after header validation) and replace the user-constructed state.
2. If `payload.changeLogBytes().length > 0`, decode each WAL record (frame format from `operative-state-persistence`), validate header, deserialize Change via `KryoCodec`, and invoke `Tasklet.applyChange(state, change)` in seq order.

#### Scenario: Cold start passes Optional.empty()

- **WHEN** a Java Cell is constructed for the first time with no prior persisted state on the agent
- **THEN** the runtime SHALL invoke `initState(cellKey, Optional.empty())`

#### Scenario: Restart passes Optional.of(payload)

- **WHEN** a Java Cell is reconnected and the agent has snapshot or WAL bytes for it
- **THEN** the runtime SHALL invoke `initState(cellKey, Optional.of(payload))` AND, after `initState` returns, the runtime SHALL deserialize the snapshot (if any) and fold the change log via `applyChange` in seq order

### Requirement: Per-Cell flusher with toggle-aware drain

Durable-change flushing SHALL be owned by the core: Kryo-encoded change
bytes returned in completions are buffered per cell core-side and
drained to framed WAL records on the core's linger cadence, honoring the
persistence toggle (drain-and-discard when disabled). The Java layer's
obligation is exactly its completion contract: hand the invocation's
change bytes (and timer ops, offsets, outputs) back in the
`ActivationResult` — it SHALL NOT run a flusher thread of its own.

#### Scenario: Changes returned in a completion reach the WAL
- **WHEN** a Java invocation applies two Changes and its completion carries their Kryo bytes with persistence enabled
- **THEN** the core SHALL frame and ship them as `cell_state_change` records with contiguous seqs, and a subsequent restore SHALL fold them back through Java's `RestoreFold`

#### Scenario: Persistence disabled drains and discards
- **WHEN** the same completion arrives for a class with persistence disabled
- **THEN** no WAL record SHALL ship and the cell's in-memory state SHALL still reflect the applied Changes

### Requirement: Hot-path cost is bounded

The Java SDK's `state.apply(change)` SHALL pay exactly: one `Tasklet.applyChange(state, change)` invocation (user-supplied; cost varies) plus one `ArrayList.add(change)` tasklet (amortized O(1)). The hot path SHALL NOT serialize, fsync, network-send, or acquire any lock beyond the existing per-Cell sticky CAS and the runner thread's exclusive ownership of the buffer for the duration of `Tasklet.run`.

#### Scenario: No serialization on state.apply

- **WHEN** a Tasklet's `run` calls `state.apply(MyChange.foo())` 100 times in a tight loop
- **THEN** zero `KryoCodec.serialize`, `Kryo.writeClassAndObject`, or any serialization-equivalent call SHALL be issued from the runner thread during these 100 calls (verifiable via JFR profiling or by mocking `KryoCodec` and asserting zero invocations)

#### Scenario: No JNI calls on state.apply

- **WHEN** a Tasklet's `run` calls `state.apply(MyChange.foo())` 100 times
- **THEN** zero `NativeTransport.*` JNI calls SHALL be issued from the runner thread during these 100 calls

### Requirement: Kryo serialization with stable registered type IDs

The Java SDK SHALL use Kryo for `format_id = 4` serialization with `setRegistrationRequired(true)`. The runtime SHALL register built-in flezha types (NoChange, RestoreInitPayload, CoalesceKey, primitives, common collections) at SDK init in a fixed order (stable IDs 1-N). User types from `Tasklet.kryoTypes()` SHALL be registered after built-ins in declaration order (stable IDs N+1, N+2, ...).

The Java SDK SHALL refuse to serialize any user-supplied object whose runtime class is not in the registered set — Kryo throws `IllegalArgumentException` per its `registrationRequired = true` semantics; the SDK wraps this as `StateSerializeException::UnregisteredClass(className)` and rethrows.

Reordering of `Tasklet.kryoTypes()` between SDK versions SHALL be an unsupported breaking change (changes the assigned IDs); appending new types to the end SHALL be safe (preserves prior IDs).

#### Scenario: Built-in types registered with stable IDs

- **WHEN** a JVM starts with the Java SDK loaded
- **THEN** the registered Kryo types SHALL include `NoChange`, `RestoreInitPayload`, `CoalesceKey`, `String`, `Long`, `Integer`, `Double`, `Boolean`, `byte[]`, `List`, `Map`, `Optional` at fixed stable IDs (the exact values are recorded in `KryoCodec` source as a documented constant)

#### Scenario: User types registered after built-ins

- **WHEN** `Tasklet.kryoTypes()` returns `[OhlcChange.class, OhlcState.class]` and built-ins occupy IDs 1-12
- **THEN** `OhlcChange.class` SHALL register with ID 13 AND `OhlcState.class` SHALL register with ID 14

#### Scenario: Unregistered class fails loud

- **WHEN** a Tasklet's `Change` references a nested class not in `kryoTypes()` (e.g. a nested `record OhlcChange.BarTick(Interval interval, ...)` where `Interval` is a record not registered)
- **THEN** the first `state.apply(...)` triggering serialization SHALL throw `StateSerializeException::UnregisteredClass("Interval")` and the JVM SHALL NOT silently succeed with auto-IDs

### Requirement: Public Java SDK handler classes use canonical Operative vocabulary

The Java SDK SHALL expose handler classes whose names use canonical vocabulary: `AddOperativeHandler`, `RemOperativeHandler`, `OperativeMessageHandler`, `CellStateHandler`, `CellApplyStateHandler`. Classes previously named `AddProcessorHandler`, `RemProcessorHandler`, `ProcessorMessageHandler`, `ProcessorStateHandler`, `ProcessorApplyStateHandler` SHALL NOT be present in the published API. Java packages SHALL likewise migrate any package-path components that contained `processor`, `operator`, `agent`, or `compute_engine` to the canonical replacements (`operative`, `runner`, `controller`). `FleetManager`-equivalent classes keep their names.

#### Scenario: Old handler classes do not exist
- **WHEN** a downstream Java consumer imports `com.flezha.sdk.handlers.AddProcessorHandler`
- **THEN** the import MUST fail to compile (class not found); the consumer MUST migrate to `com.flezha.sdk.handlers.AddOperativeHandler`

#### Scenario: New handlers wire into dispatch
- **WHEN** a Java SDK Runner receives an `AddOperative` protobuf command from the Controller
- **THEN** dispatch MUST route the message to `AddOperativeHandler.handle(...)` with the protobuf `operative_id` field exposed under the same name (`getOperativeId()`)

### Requirement: Public Operative / Tasklet / Cell user-facing API names

The Java SDK SHALL expose user-extensible base classes/interfaces under the canonical names `StreamOperative`, `Tasklet`, `Cell`, and `Channel`. The Runner process startup hook SHALL be `Runner.start(Config)` (or equivalent factory entry point); class names referring to the SDK process itself SHALL use `Runner` as the role term, not `ComputeEngine` or `Operator`.

#### Scenario: User extends StreamOperative
- **WHEN** a user-defined Operative class declares `extends StreamOperative`
- **THEN** the class MUST compile against the published Java SDK; declaring `extends Processor` MUST fail

### Requirement: Java SDK implements the effectively-once output tier

The Java SDK SHALL implement the `effectively-once-output` capability. A channel declaration SHALL support `c.effectivelyOnce()`; the runtime SHALL then, for each invocation, buffer the invocation's state changes, plugin outputs, consumed input offset, and (on a dead-letter park) its DLQ entry into ONE `cell_effect_record` frame rather than separate change and output frames.

The frame SHALL carry the 16-byte state header with `format_id = 2` and `sdk_lang = 2`, and its body SHALL be the postcard encoding of `EffectRecordBody { changes: Vec<Vec<u8>>, outputs: Vec<OutputRecord>, consumed_offset: Option<u64>, dlq: Option<DlqEntryBody>, event_time: u64 }` in exactly that field order, byte-identical to the Rust SDK. The `changes` elements SHALL be the SDK's own change bytes (Kryo, as a Java plain change record carries) — the controller stores and re-ships them without decoding.

The runtime SHALL read the inbound `flz.offset` metadata key to learn the input's offset, and SHALL re-forward replayed inputs on restore. A dead-letter park under the tier SHALL ride inside the effect record rather than emitting a separate `park_payload` frame, so that the park and the consumed-offset advance are one atomic record.

Declaring `.keyed()` SHALL be required on every plugin-bound channel of a tier-enabled class; a non-keyed plugin channel SHALL fail registration.

#### Scenario: A tier invocation ships one effect record, not separate frames

- **WHEN** a tier-enabled Tasklet's `run` applies two Changes and emits one keyed output for an input carrying `flz.offset = 7`
- **THEN** the SDK SHALL ship exactly one `cell_effect_record` frame whose decoded body has `changes.length == 2`, `outputs.length == 1`, and `consumed_offset == 7`; and SHALL NOT ship any `cell_state_change` frame for that invocation

#### Scenario: Effect-record bytes are byte-identical to the Rust encoder

- **WHEN** the Java SDK encodes an `EffectRecordBody` with the same field values as a Rust fixture
- **THEN** the produced bytes SHALL equal the Rust `postcard::to_allocvec(EffectRecordBody)` fixture bytes exactly

#### Scenario: A poisoned tier invocation parks inside the effect record

- **WHEN** a tier-enabled Tasklet exhausts its retry budget on a poison input at offset 9
- **THEN** the SDK SHALL ship one `cell_effect_record` whose body carries `dlq = Some(entry)` AND `consumed_offset == 9`, and SHALL NOT ship a separate `park_payload` frame

#### Scenario: A tier class with a non-keyed plugin channel fails registration

- **WHEN** a class declares `c.effectivelyOnce()` and a plugin-bound channel without `.keyed()`
- **THEN** registration SHALL throw `StreamOperativeConfigurationException` naming the channel

### Requirement: Java SDK implements checkpoint ack granularity

The Java SDK SHALL implement the `effectively-once-checkpoint-granularity` capability. A channel declaration SHALL support `c.checkpointAck()`, valid only together with `c.effectivelyOnce()` — declaring it alone SHALL fail registration with `CheckpointAckWithoutTier`. `TaskletCtx.checkpoint()` SHALL mark the invocation as closing the current interval.

The per-Cell flush buffer SHALL carry a third `staged` list holding the open interval's changes. `drainToFrames` and `isEmpty` SHALL deliberately **ignore** `staged` — an open interval is not flushable, and draining it alone would put state ahead of its offset. An invocation SHALL emit an effect record carrying `{staged, consumedOffset}` when it checkpointed or parked to the DLQ; otherwise it SHALL emit `{[], outputs, null}` only if it produced outputs. A dead-letter park SHALL close the interval. A restore SHALL discard the open interval.

#### Scenario: An open interval holds the consumed offset

- **WHEN** a checkpoint-ack Tasklet processes three inputs without calling `ctx.checkpoint()`
- **THEN** no effect record carrying a non-null `consumed_offset` SHALL be shipped, and the staged changes SHALL remain unflushed

#### Scenario: checkpoint() closes the interval and acknowledges the offset

- **WHEN** the Tasklet's fourth invocation calls `ctx.checkpoint()` for an input at offset 3
- **THEN** the SDK SHALL ship one effect record whose `changes` are the four invocations' staged changes and whose `consumed_offset == 3`

#### Scenario: checkpointAck without the tier is refused

- **WHEN** a class declares `c.checkpointAck()` without `c.effectivelyOnce()`
- **THEN** registration SHALL throw `StreamOperativeConfigurationException` carrying the error name `CheckpointAckWithoutTier`

#### Scenario: A park closes the open interval

- **WHEN** a checkpoint-ack Tasklet has staged two invocations' changes and the third input is poison and exhausts its retries
- **THEN** the shipped effect record SHALL carry the staged changes, the DLQ entry, and the poison input's `consumed_offset` — so the replay resumes after the poison offset

#### Scenario: A restore discards the open interval

- **WHEN** a Cell with a non-empty `staged` list is restored via `RestoreInitState`
- **THEN** the staged list SHALL be cleared, since it belongs to a past that no longer exists

### Requirement: Java SDK implements stream late-input correction

The Java SDK SHALL implement the `stream-correction` capability. A channel declaration SHALL support `c.correctionWindow(millis)` and `c.beyondWindowPolicy(policy)` where policy is `PROCESS_AS_ARRIVAL` (the default, today's behaviour) or `DROP`. Declaring a correction window SHALL require the effectively-once tier, an `EventTimeFrom` clock, and all channels keyed; violating any of these SHALL fail registration.

Every effect record SHALL carry the invocation's `event_time`. The runtime SHALL maintain a per-Cell event-time high-water mark, and SHALL classify an input whose event time is below the high-water by more than the declared window as beyond-window. A replay slice SHALL be recognised via the `flz.replay` metadata key and terminated by a payload-less `flz.replay_end` **broadcast** frame (intercepted before channel lookup, since an input routes to only one cell by key); the buffered slice SHALL be dispatched in **event-time order**, and a closed slice SHALL flush before any live input popped behind it.

A replay SHALL NOT lower the event-time high-water mark.

#### Scenario: A replayed slice dispatches in event-time order

- **WHEN** a replay slice delivers inputs with event times `[30, 10, 20]` followed by an `flz.replay_end` broadcast
- **THEN** the Tasklet's `run` SHALL be invoked for them in event-time order `10, 20, 30`

#### Scenario: A closed slice flushes before a live input behind it

- **WHEN** a live input is popped from the mailbox behind a just-closed replay slice
- **THEN** the slice SHALL dispatch in full before the live input, so the live input cannot jump ahead of the inputs being re-ordered

#### Scenario: A replay does not lower the event-time high-water

- **WHEN** a Cell restored to an event-time high-water of `500` replays an unconsumed tail whose maximum event time is `300`
- **THEN** the high-water SHALL remain `500`, and a subsequent input at event time `320` SHALL still be classified as late

#### Scenario: A correction window without the tier is refused

- **WHEN** a class declares `c.correctionWindow(60_000)` without `c.effectivelyOnce()`
- **THEN** registration SHALL throw `StreamOperativeConfigurationException` naming the missing tier declaration

#### Scenario: Beyond-window default is process-as-arrival

- **WHEN** a class declares `c.correctionWindow(60_000)` without declaring a policy, and an input arrives whose event time is older than the window
- **THEN** the input SHALL be processed as an arrival (not dropped), since declaring a window must never silently start dropping data

### Requirement: Java SDK implements the workflow task-journal and TaskCommit handshake

The Java SDK SHALL implement the `workflow-task-journal` capability. At every task barrier of a durable structured workflow class, the SDK SHALL ship `TaskCommit` (tag 28) carrying the tasks that ran and the channel-tagged handoff blobs, and SHALL block on the controller's `TaskCommitResponse` (tag 29) under the same `reference_id` for up to 30 seconds. A failure or timeout SHALL produce a terminal error and trigger compensation.

The SDK SHALL declare no inter-tasklet codec and SHALL offer no method to declare one: it hands the core bytes, so the core derives the journal codec from the channel's kind. Classes that opt out of persistence SHALL skip the journal and the handshake entirely, preserving re-run-from-top.

On resume, the SDK SHALL read the journal state shipped on `AddOperative.task_journal`, SHALL **skip** tasks already recorded as committed, and SHALL seed the next task's batch from the journaled handoff via the channel codec.

#### Scenario: A committed task is skipped on resume, not re-run

- **WHEN** a durable workflow class commits task A, then the runner is SIGKILLed, then the operative is rebound with a journal recording task A as committed
- **THEN** task A's `run` SHALL NOT be invoked again, and task B SHALL run seeded from task A's journaled handoff

#### Scenario: A handoff is journaled with nothing declared

- **WHEN** a durable structured workflow class wires task A into task B and declares nothing about how the value between them is encoded
- **THEN** registration SHALL succeed, and A's handoff SHALL be journaled at the commit barrier and re-seeded on resume

#### Scenario: An opted-out class re-runs from the top

- **WHEN** a workflow class declares `persistenceEnabled() == false` and is resumed after a crash
- **THEN** the SDK SHALL ship no `TaskCommit`, and every task SHALL re-run from the top

#### Scenario: A commit timeout triggers compensation

- **WHEN** the controller does not answer a `TaskCommit` within 30 seconds
- **THEN** the SDK SHALL treat the task as terminally failed and SHALL run the compensation plan

### Requirement: Java SDK implements workflow staged commit and the transactional outbox

The Java SDK SHALL implement the `workflow-transactional-outbox` capability. Inside a structured task of a durable workflow class, `ctx.emit` SHALL NOT reach the wire: `WorkflowCollectSink` SHALL buffer `(channel, bytes)` and the buffered outputs SHALL ride the same `TaskCommit` as the journal record, in the `outbox` field (`TaskOutboxItem{cell_id, seq_in_task, channel, payload}`). A failing stage SHALL drop its staged bytes together with the error, so nothing leaks. A transport-less run SHALL keep the direct-emit path.

Compensation output SHALL ride the same handshake with `compensating` set.

#### Scenario: A task's emits are staged, not shipped, until the barrier

- **WHEN** a structured task of a durable workflow class calls `ctx.emit(v)` twice and then reaches its task barrier
- **THEN** no plugin frame SHALL be sent during the task body, and the `TaskCommit` at the barrier SHALL carry both outputs in its `outbox`

#### Scenario: A failing stage leaks no staged output

- **WHEN** a stage emits once and then throws
- **THEN** the staged bytes SHALL be dropped with the error and SHALL NOT appear in any subsequent `TaskCommit`

### Requirement: Java SDK implements the workflow durable loop and suspension

The Java SDK SHALL implement the `workflow-durable-loop` and `workflow-durable-suspension` capabilities. `WorkflowOperative` SHALL declare `loopMode()` returning `ONCE` (the default, byte-identical to the current single pass), `PER_INPUT` (one iteration per inbox item), or `CHAINED` (each iteration seeded by the previous iteration's terminal handoff), and no wake setting: a sleeping instance's wake deadline is the earliest pending processing-time timer its tasklets registered.

At every iteration boundary the SDK SHALL ship `IterCommit` (tag 30) carrying the iteration index, the consumed input-log offset when the iteration's input carried one and no offset otherwise, the terminal handoff, and a tasklet's completion request with its result when one was made, and SHALL await `IterCommitResponse` (tag 31). A durable `CHAINED` loop whose terminal task releases no inter-tasklet channel, whether it owns no output channel at all or only a plugin-bound one, which the runtime gives no journal codec, SHALL be rejected by the core when the class is declared, and SHALL NOT be re-judged by this SDK, exactly as the branching-chained-loop refusal already is: the rule belongs to the registration every host reaches, not to one SDK's builder.

When the loop is idle past the park hysteresis, the SDK SHALL ship `IterPark` (tag 32) carrying the earliest pending processing-time timer across the instance's Cells as its wake deadline (`0` when none) and exit **without** `WorkflowCompleted`, yielding a parked outcome. The offset an input carries SHALL be read from the inbound `flz.offset` metadata key by the shared core on the SDK's behalf; the SDK SHALL read no offset key of its own.

#### Scenario: Each loop iteration commits at its boundary

- **WHEN** a `PER_INPUT` workflow class processes three inbox items
- **THEN** the SDK SHALL ship three `IterCommit` commands, each carrying its iteration index and the input-log offset its item carried, and SHALL await each response before starting the next iteration

#### Scenario: An idle loop parks instead of completing

- **WHEN** a durable looping workflow class has no inbox item for longer than the park hysteresis
- **THEN** the SDK SHALL ship `IterPark` and exit without shipping `WorkflowCompleted`

#### Scenario: A resumed loop restarts at the journaled iteration

- **WHEN** a looping workflow class committed iteration 2 and is then rebound after a crash
- **THEN** the SDK SHALL resume at iteration 3, seeded (for `CHAINED`) from iteration 2's journaled terminal handoff

#### Scenario: A durable chained loop whose terminal task releases nothing is refused

- **WHEN** a durable `CHAINED` workflow class's terminal task has no output channel, or has one whose only wiring is a plugin sink
- **THEN** declaring the class to the core SHALL fail, with an error naming the terminal tasklet and stating that it releases no inter-tasklet handoff

### Requirement: Java SDK implements the workflow cell retry ladder

The Java SDK SHALL implement the `workflow-cell-retry` capability. The ladder's budget SHALL be the consuming edge's `retryMax()` / `retryBackoffMs()` (default 3 total attempts, 50 ms base), the same builder calls and the same `RetryPolicy` a stream's edge declares; `WorkflowOperative` SHALL declare no `retryAttempts()` or `retryBackoff()`. Only the failing cell SHALL re-run its batch; siblings SHALL stay buffered at the barrier, and each attempt SHALL use a fresh sink so a failed attempt's staged output is dropped rather than double-emitted.

Backoff SHALL reuse the dead-letter ladder's helper (`base × 2^(n-1)` with ±50% jitter, capped at 5 s) and the wait SHALL be cancellation-aware, so a stop or `maxDuration` ends it at once. Retry is strictly pre-commit: a journaled task SHALL never be retried. `retryMax(1)` SHALL be a single attempt and `retryMax(0)` SHALL be refused at registration with `RetryMaxZero`, as on a stream. A `WorkflowException` SHALL carry a `nonRetryable` flag; a thrown `RuntimeException` SHALL stay retryable.

#### Scenario: A transient cell failure recovers on retry

- **WHEN** a workflow cell's batch throws on its first attempt and succeeds on its second
- **THEN** the operative SHALL complete successfully, only the failing cell SHALL have re-run, and its first attempt's staged output SHALL NOT be emitted

#### Scenario: An exhausted ladder compensates

- **WHEN** a workflow cell fails all three attempts
- **THEN** the operative SHALL abort and run the compensation plan

#### Scenario: A non-retryable error skips the ladder

- **WHEN** a cell throws `WorkflowException.nonRetryable("bad record")`
- **THEN** the SDK SHALL NOT retry and SHALL proceed directly to the abort path

#### Scenario: The edge's budget bounds the ladder

- **WHEN** a workflow wires `c.input(String.class, "in").retryMax(2).into(Flaky.class)` and `Flaky` throws on every attempt
- **THEN** the cell SHALL run exactly twice before the operative aborts, and `retryMax(0)` on that edge SHALL be refused at `registerFactory` naming `RetryMaxZero`

### Requirement: Java SDK implements workflow loop rewind

The Java SDK SHALL implement the `workflow-loop-rewind` capability. On restore, the SDK SHALL read `TaskJournalState.rewind_undo` (the tasks the controller's rewind marker determined are still owed an undo) and, before entering the forward loop, SHALL run those undos in reverse order under a **forced** all-or-nothing rollback mode — whatever the class's declared `rollbackMode()` says — since a rewind unwinds committed iterations by definition.

The SDK SHALL journal exactly one `compensated` record per undone task, **including an empty one for a task with no compensate hook**, since that record is what drains `rewind_undo`. The undo pass SHALL NOT consult the per-cell crash-resume done markers, which are per cell and would wrongly suppress a task that ran in two rewound iterations.

A rewind SHALL NOT replay inputs: the controller truncates the inbox at each iteration commit, so a rewind is undo-and-continue — the loop resumes waiting and the next message becomes the following iteration.

#### Scenario: A rewind undoes committed iterations in reverse order

- **WHEN** the controller rewinds a looping workflow class to iteration 1 and the class committed iterations 2 and 3
- **THEN** on rebind the SDK SHALL run the compensate hooks for iterations 3 and 2's tasks in reverse order before resuming the forward loop

#### Scenario: A hookless task still drains the rewind undo

- **WHEN** a task owed a rewind undo declares no compensate hook
- **THEN** the SDK SHALL journal an empty `compensated` record for it, so it is not re-offered on the next restore

#### Scenario: A rewind does not re-deliver consumed inputs

- **WHEN** a looping workflow class is rewound to iteration 1
- **THEN** the inputs consumed by iterations 2 and 3 SHALL NOT be re-delivered; the loop SHALL resume waiting, and the next inbound message SHALL become the next iteration

### Requirement: Java SDK implements workflow branching

The Java SDK SHALL implement the `workflow-branching` capability. `WorkflowSpec` SHALL model the task graph as a tree (`order` plus `children`) rather than the current `List<Integer> chain`; a linear chain SHALL be the degenerate single-child case. `WorkflowSpec.linearChain()`'s refusal of any non-linear graph SHALL be removed and replaced with validation over the tree.

The SDK SHALL declare the tree's leaf tasks to the controller via the `leaf_tasks` key of `AddOperativeResponse.metadata`. Fan-in SHALL NOT silently overwrite an edge. Compensation SHALL run in reverse-topological order, which the existing BFS numbering already yields — the compensation planner SHALL NOT be changed.

#### Scenario: A branching chain runs its branches and joins

- **WHEN** a workflow class declares task 0 feeding both task 1 and task 2, which both feed task 3
- **THEN** tasks 1 and 2 SHALL each run seeded from task 0's handoff, and task 3 SHALL run once both have committed

#### Scenario: Compensation runs in reverse-topological order

- **WHEN** the branching class above aborts after task 3 commits
- **THEN** the compensate hooks SHALL run in the order 3, then 2 and 1, then 0

#### Scenario: Fan-in does not overwrite an edge

- **WHEN** two tasks both feed the same downstream task on the same channel
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

The Java SDK SHALL provide `FlezhaRunner.registerFactory(Class<? extends Operative>)` and its inverse `FlezhaRunner.unregisterFactory(Class<? extends Operative>)` as chainable builder methods, `registerFactory` reading the kind from the kind interface the class implements and refusing a class that implements both `StreamOperative` and `WorkflowOperative` with an `IllegalArgumentException` whose message begins `AmbiguousOperativeKind`, or neither with one beginning `UnknownOperativeKind`, callable **before and after** `.start()`/`.run()`. For each declared StreamOperative class the SDK SHALL: (a) instantiate the class via its public no-arg constructor; (b) call its `factoryId()` method to obtain the controller-facing factory id; (c) call its `channels(ChannelBuilder)` method exactly once — the builder being the producer-first graph builder of `sdk-graph-authoring` — and freeze the resulting `OperativeTopology`; (d) verify wiring constraints (entry-channel deserializability, type-equality between a stream's payload type and its consumer Tasklets' input types, node reachability from the entry, at most one output channel per node, acyclicity, node-identity and channel-name uniqueness, static-mode and dynamic-mode default-impl detection); (e) record `(factoryId → topology)` in the class registry. The registry SHALL stay mutable after `.start()` for as long as the runner is open: `registerFactory` and its `unregisterFactory` inverse are legal on a started runner, and every post-start change SHALL re-sync the runner's full current factory set rather than a delta — the set the next connect/reconnect init frame carries to the controller. A started runner that has been closed SHALL refuse every `registerFactory`/`unregisterFactory` call with an `IllegalStateException` naming the closed runner, and the refused call SHALL NOT reach the class registries or the core — `.close()` destroys the core context, so a declaration made afterwards would otherwise carry a freed handle back across the JNI boundary. A never-started runner has no context to free, so `.close()` on one leaves it mutable, as the runner-lifecycle requirement already states. `unregisterFactory` SHALL throw `IllegalArgumentException` naming the factory id when that id is not defined on this runner, and `OperativeTypeInUseException` — an `IllegalStateException` subclass — naming the factory id when the runner is started and live operative instances of it exist, leaving the type defined. There SHALL be no `OperativeHandler`-style factory registration in the public surface.

Dynamic cell mode SHALL be supported: a Tasklet reached by a consumer edge declaring `keyBy`, returning an empty list from `cellKeys()`, SHALL be declared successfully and create Cells lazily per key at dispatch time, per the `operative-state-persistence` dynamic-cell behaviour. Eviction SHALL be off by default (`cellIdleTtl()` returns `Optional.empty()`); when a TTL is declared, the per-Tasklet reaper SHALL evict idle Cells destructively by shipping `delete_cell_state`, and a recreated Cell SHALL cold-start fresh rather than restore from disk.

Because Java's type erasure precludes Rust's compile-time topology refusals, every constraint Rust enforces at compile time SHALL be enforced at declaration time, throwing `StreamOperativeConfigurationException` whose message carries the same diagnostic content and the same error name as the corresponding Rust `TopologyIssue` variant.

#### Scenario: Declaring a stream operative class before start() succeeds
- **WHEN** user code calls `Flezha.createRunner("my_app").registerFactory(MarketOperative.class).start()`
- **THEN** the handshake init JSON SHALL include `"market"` (the value `MarketOperative::factoryId` returns) in `factory_ids`, and subsequent `AddOperative{factory_id:"market"}` commands SHALL succeed by spawning an `StreamOperativeInstance`

#### Scenario: Declaring an operative type after start() is accepted
- **WHEN** user code calls `.registerFactory(...)` on a started `FlezhaRunner` already declaring `"market"`, for a class whose `factoryId()` returns `"orders"`
- **THEN** the call SHALL succeed and return the runner, the new class SHALL be declared to the core immediately, and the runner's declared set SHALL become the full set `{"market", "orders"}` the next connect/reconnect init frame carries

#### Scenario: Duplicate factoryId across two stream operative classes is rejected
- **WHEN** two distinct StreamOperative classes both return `"market"` from `factoryId()` and both are declared on the same builder
- **THEN** the second `.registerFactory(...)` call SHALL throw an `IllegalArgumentException` naming the duplicate factoryId

#### Scenario: Wiring mismatch fails declaration with a diagnosable error
- **WHEN** an Operative wires `.into(OhlcTasklet.class)` onto `ParseTasklet`'s output stream but `ParseTasklet.outputType()` returns `Trade.class` while `OhlcTasklet.inputType()` returns `Candle.class`
- **THEN** `.registerFactory(...)` SHALL throw `StreamOperativeConfigurationException` whose message names the channel, the stream's payload type (`Trade`), and the consumer-input type (`Candle`)

#### Scenario: Typed entry channel deserializes at the edge
- **WHEN** an Operative declares `c.input(Trade.class, "trade.raw").into(ParseTasklet.class)` and a JSON-encoded Trade arrives
- **THEN** `.registerFactory(...)` SHALL succeed and `ParseTasklet.run` SHALL receive the Jackson-deserialized `Trade`, per the typed-entry-input contract of `sdk-graph-authoring`

#### Scenario: Unreachable node fails declaration
- **WHEN** an Operative's `channels(c)` wires a Tasklet that is not reachable from the entry channel
- **THEN** `.registerFactory(...)` SHALL throw `StreamOperativeConfigurationException` naming the unreachable Tasklet

#### Scenario: Static-mode tasklet that declares no cellsFor is rejected
- **WHEN** a Tasklet returns a non-empty list from `cellKeys()` but neither it nor any class in its hierarchy declares `cellsFor`
- **THEN** `.registerFactory(...)` SHALL throw `StreamOperativeConfigurationException` naming the Tasklet and stating that a static-mode tasklet must declare `cellsFor`

#### Scenario: A declared hook needs no second declaration
- **WHEN** a workflow Tasklet declares `compensate` and nothing else about it
- **THEN** `.registerFactory(...)` SHALL record it as compensable and the class declaration shipped to the controller SHALL carry `compensate_overridden` for it, with no further member written by the author

#### Scenario: Dynamic-mode tasklet declares and creates cells lazily
- **WHEN** a Tasklet returning an empty `cellKeys()` is wired through an edge declaring `keyBy`, and two inputs arrive whose key function returns `"BTC"` and `"ETH"`
- **THEN** `.registerFactory(...)` SHALL succeed, and the runtime SHALL lazily allocate exactly two Cells keyed `"BTC"` and `"ETH"`, each constructed with `initState(Optional.of(key), Optional.empty())`

#### Scenario: Declaring or removing a type on a closed runner is refused
- **GIVEN** a `FlezhaRunner` that was started and then closed via `.close()`
- **WHEN** code calls `.registerFactory(...)` or `.unregisterFactory(...)` on it
- **THEN** each call SHALL throw `IllegalStateException` naming the closed runner's namespace, no class SHALL be registered or unregistered in the runtime class registries, and no factory set SHALL be shipped to the runtime — the freed core context SHALL NOT be reached

#### Scenario: Removing a type that is not defined is rejected
- **WHEN** code calls `.unregisterFactory(...)` for a class that was never defined on this runner, or that was already removed from it
- **THEN** the call SHALL throw `IllegalArgumentException` naming the class's factory id, and the runner's declared set SHALL be unchanged

#### Scenario: Removing a type with live instances is refused and keeps it defined
- **GIVEN** a started runner declaring `"market"` with at least one live operative instance of that factory
- **WHEN** code calls `.unregisterFactory(MarketOperative.class)`
- **THEN** the call SHALL throw `OperativeTypeInUseException` naming `"market"`, the type SHALL remain defined and registered, and no factory set SHALL be shipped

### Requirement: Runner process singleton guard

The Java SDK SHALL allow at most one runner to be **started** per JVM process. `Flezha.createRunner(...)` SHALL NOT claim the singleton slot — any number of unstarted runners may exist. The slot SHALL be claimed by compare-and-set at `.start()`/`.run()`; if a prior runner's `.start()`/`.run()` has already succeeded in this process and not yet closed, the call SHALL throw `IllegalStateException` naming the namespace of the already-running runner, and SHALL NOT connect, mutate any shared registry, or spawn any background thread. Calling `.start()` twice on the same runner SHALL throw `IllegalStateException`.

#### Scenario: Second start() in the same JVM is rejected
- **GIVEN** a JVM process where `Flezha.createRunner("ns1").registerFactory(A.class).start()` has already succeeded
- **WHEN** code in the same process calls `Flezha.createRunner("ns2").registerFactory(B.class).start()`
- **THEN** the call SHALL throw `IllegalStateException` whose message names `"ns1"` as the already-running namespace
- **AND** no connection SHALL be attempted for `"ns2"`, and the process's stable identity, cell-state sink, and credit window SHALL remain exclusively `"ns1"`'s

#### Scenario: Creating runners never claims the slot
- **GIVEN** a JVM process where a runner for `"ns1"` is already started
- **WHEN** code calls `Flezha.createRunner("ns2")` and mutates it with `define*`/`remove*` without calling `.start()`
- **THEN** no exception SHALL be thrown and `"ns1"` SHALL remain the process's active runner

#### Scenario: A runner may be started again after the first one closes
- **GIVEN** a JVM process where a runner for `"ns1"` was started and then fully closed via `.close()`
- **WHEN** code in the same process calls `Flezha.createRunner("ns2").registerFactory(B.class).start()`
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
- **WHEN** the test writes `try (FlezhaRunner r = Flezha.createRunner("test").registerFactory(A.class).start()) { ... }`
- **THEN** the runner SHALL be started for the block and gracefully closed at the end of it, exactly as `AutoCloseable` usage requires

### Requirement: Flezha static accessor for the active runner

`Flezha.currentRunner()` SHALL return an `Optional<FlezhaRunner>` containing the process's active runner if one has been started and not yet closed, or `Optional.empty()` otherwise. It SHALL NOT throw, and SHALL NOT start a runner as a side effect. Runners that were created but never started SHALL never appear in `currentRunner()`.

#### Scenario: currentRunner() reflects the singleton's lifecycle
- **GIVEN** a JVM process with no runner started yet
- **WHEN** code calls `Flezha.currentRunner()`
- **THEN** it SHALL return `Optional.empty()`
- **AND** after `Flezha.createRunner("ns1").registerFactory(A.class).start()` succeeds, `Flezha.currentRunner()` SHALL return `Optional.of(<that runner>)`
- **AND** after that runner's `.close()` completes, `Flezha.currentRunner()` SHALL return `Optional.empty()` again
- **AND** a runner created via `Flezha.createRunner(...)` but never started SHALL NOT appear in `currentRunner()`

### Requirement: Each Cell SHALL run on its own Tasklet instance

Each Cell SHALL invoke `run` on its own Tasklet instance, as the Rust SDK does by holding one Tasklet instance per Cell alongside that Cell's State, so that a class authored against either SDK means the same thing.

Today `StreamOperativeInstance` builds one Tasklet per `TaskletDispatcher` and `TaskletDispatcher` invokes `run` on that single object for every Cell it owns — static-key, keyless, and dynamic alike. The per-Cell instances it also constructs are used only for `initState` and a `schemaVersion()` read. No instance-level lock guards `run`; the per-Cell `stateLock` and `changeBufferLock` are distinct objects per Cell and serialise nothing between Cells, which drain concurrently on the worker pool. Any mutable field on a user's Tasklet is therefore a shared, unsynchronised channel across Cell keys.

#### Scenario: Instance state does not leak between Cell keys

- **GIVEN** a Tasklet declaring two Cell keys, whose implementation writes to a mutable instance field during `run`
- **WHEN** input is dispatched to both Cells
- **THEN** neither Cell SHALL observe the value written by the other

#### Scenario: Concurrent Cells do not race on one instance

- **GIVEN** a Tasklet with multiple Cells draining concurrently on the worker pool
- **WHEN** each Cell's `run` writes to a mutable instance field
- **THEN** the writes SHALL NOT race, because each Cell holds its own instance

#### Scenario: Dynamic Cells each receive an instance

- **GIVEN** a dynamic-mode Tasklet that creates Cells lazily per key
- **WHEN** a Cell is created for a newly-seen key
- **THEN** that Cell SHALL run on its own Tasklet instance

#### Scenario: The dispatcher's documentation matches its behaviour

- **WHEN** the dispatcher documents the lifetime of the Tasklet instance it holds
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

### Requirement: The Java SDK SHALL emit a cell_state_snapshot on CompactCell

`CompactCellHandler` currently returns `new byte[0]` on its success path — a zero-length body where the Controller expects a `CompactCellResponse` — with a comment describing unwritten work. Its "not yet implemented" error is reachable only from a `catch (ReflectiveOperationException)` branch that does not fire in normal tasklet. The handler SHALL locate the addressed Cell, capture its state, ship a `cell_state_snapshot` data frame, and answer a well-formed `CompactCellResponse` reporting the captured seq and byte size.

The snapshot body SHALL be encoded with the same codec the restore path decodes — Kryo, per `CellStateRestore.rebuild`. The Jackson JSON serializer used for graceful shutdown and live migration SHALL NOT be used for the snapshot body.

#### Scenario: A live Cell produces a snapshot at its current seq

- **GIVEN** a live Cell with persisted Changes and a current change-seq
- **WHEN** the SDK receives `CompactCell` addressing that Cell
- **THEN** the SDK SHALL ship one `cell_state_snapshot` frame carrying the Cell's serialized state stamped with a seq the Cell has reached
- **AND** SHALL answer a well-formed `CompactCellResponse` reporting that seq and the body's byte size

#### Scenario: The response is never a zero-length body

- **WHEN** the Java `CompactCell` handler completes, successfully or otherwise
- **THEN** it SHALL answer either a well-formed `CompactCellResponse` or a reported error
- **AND** SHALL NOT answer a zero-length byte array on the success path

#### Scenario: The snapshot body round-trips through the restore path

- **GIVEN** a snapshot produced by the Java SDK for a Cell
- **WHEN** that snapshot is later shipped back as `RestoreInitState.snapshot_bytes`
- **THEN** `CellStateRestore` SHALL decode it without error and SHALL reconstruct the state the Cell held when the snapshot was taken

### Requirement: The snapshot frame SHALL be stamped with its payload-id

A snapshot answered through the `SnapshotRequest` activation SHALL reach the wire framed by the core under the `cell_state_snapshot` payload-id the Controller routes on — the host returns naked state bytes and never frames.

#### Scenario: The frame is routable by the Controller

- **WHEN** the Java SDK ships a snapshot frame
- **THEN** the frame SHALL carry the `cell_state_snapshot` payload-id
- **AND** the Controller SHALL route it to its snapshot-write path

#### Scenario: No transport helper claims stamping it does not perform

- **WHEN** a transport helper's documentation states that it stamps a payload-id
- **THEN** it SHALL stamp that payload-id, or the documentation SHALL be corrected and the unused helper removed

### Requirement: The Java SDK SHALL ship its declared compact_after_bytes to the Controller

The Java SDK SHALL place its Tasklet's declared `compactAfterBytes` value into the `AddOperative` metadata the Controller reads, so the per-Tasklet threshold is resolvable. Today the value reaches `TaskletDescriptor` and stops — `compact_after_bytes` appears nowhere in the Java SDK's metadata construction, so a user's override changes nothing.

The **default value** itself is owned by `java-state-guard-and-dead-knobs`, which aligns it with Rust and the Controller and runs first. This requirement covers only the transport of whatever value is declared.

#### Scenario: A declared override reaches the Controller

- **WHEN** a Java Tasklet declares a `compactAfterBytes` different from the default
- **THEN** the SDK SHALL ship that value in the `AddOperative` metadata
- **AND** the Controller SHALL resolve it as that Tasklet's size threshold

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
(`runtime/control` handlers, `CellFlusher`, `CreditWindow`, `DlqPark`,
`TimerWheel`, `WorkflowOperativeRunner`, the reconnect loop, `AckRegistry`)
SHALL be deleted, not shimmed. The Java SDK SHALL encode and decode the
Activation/ActivationResult schema byte-identically to the core's hex-pinned
fixtures, reproduced in a Java fixture suite.

#### Scenario: Kernel classes are gone
- **WHEN** user or test code references the deleted kernel classes (for example `com.flezha.sdk.runtime.state.CellFlusher` or a `runtime/control/handlers` class)
- **THEN** compilation SHALL fail — the parallel mechanism is removed with no compatibility layer

#### Scenario: Java reproduces the schema fixtures byte-identically
- **WHEN** the Java fixture suite encodes the canonical work activation, result, and all command-kind fixtures from `flz_core::schema`
- **THEN** the produced bytes SHALL equal the core's pinned hex literals exactly, and a mismatch SHALL fail the build

#### Scenario: Per-cell state is host-held without host locking
- **WHEN** two activations for two different cells of one Tasklet run concurrently on virtual threads
- **THEN** each SHALL touch only its own cell's materialized state — per-cell exclusivity is the core's handout guarantee, and the Java layer performs no cell-level locking of its own

#### Scenario: Blocking user code does not consume a fetcher
- **WHEN** activations that block for tens of seconds (e.g., an LLM call) are
  in flight for many cells at once
- **THEN** the fetchers SHALL keep polling and handing out further
  activations, and work for unblocked cells SHALL proceed — concurrency is
  bounded by the core's jobs-underway limit, not by the fetcher count

### Requirement: The Java class declaration carries per-Tasklet schema versions

The Java SDK's host class declaration SHALL carry each Tasklet's own
`schema_version` and `min_readable_schema_version`. It SHALL NOT reduce the
class's Tasklets to a single class-wide value.

**BREAKING** for any Java class whose Tasklets declare differing schema
versions: their records were previously stamped with the class maximum.

#### Scenario: two Tasklets with different versions declare both

- **GIVEN** a Java class whose Tasklet A declares schema version 2 and Tasklet B declares schema version 1
- **WHEN** the class declaration is built
- **THEN** the declaration SHALL carry 2 for A and 1 for B, and SHALL NOT carry 2 for both

### Requirement: The Java Tasklet surface declares migrations

The Java `Tasklet` surface SHALL expose the same migration declaration the
Rust surface does: a readable-from floor and entry points that convert a state
snapshot and a change record from a named older schema version, both defaulting
to a refusal.

#### Scenario: a Java Tasklet restores state written at an older version

- **GIVEN** a Java Tasklet at schema version 2 declaring a readable-from floor of 1 and a state migration
- **WHEN** a Cell whose snapshot was written at version 1 restores
- **THEN** the migration SHALL be invoked with `from_version = 1` and the snapshot bytes, and the Cell SHALL resume with the value it returned

### Requirement: The Java builder submits its topology to the reference check

The Java SDK's builder SHALL record the authoring calls it receives — channels, the
ordered links of each chain, merges, branches, class flags and tasklet facts — into the
declaration record and SHALL take its verdict from the reference check, so that every name
of the `TopologyIssue` vocabulary is refused with the reference sentence and no rule is
carried in the builder. The fluent, generically typed surface SHALL stay as it is; only the judgement moves.
The SDK SHALL prove the set by one conformance run over every row of
`test/fixtures/reference_topologies.txt` through the row interpreter in its test app.

#### Scenario: A malformed row refuses with the reference name

- **WHEN** the Java interpreter authors a refusal row through the SDK's own builder
- **THEN** registration refuses with the row's name and the reference sentence

#### Scenario: A well-formed row derives to the fixture's bytes

- **WHEN** the Java interpreter authors an `ok` row
- **THEN** the derived declaration equals the row's lines

### Requirement: Diagnostics Reach a Stream Without a Third-Party Logging Dependency

The Java SDK SHALL emit its diagnostics through a logger it owns, with a working default that requires no dependency the application has to add, and SHALL NOT declare a compile-scope dependency on a third-party logging facade or provider.

A library that declares a logging facade without a provider is silent by
construction, and one that bundles a provider collides with whatever the host
already uses. Owning a small logger with a default sink is how the Node, Ruby
and PHP SDKs avoid both, and the failure paths this SDK logs on (a dead fetcher
thread, a failed core poll, a failed activation decode, a drain that timed out)
are the ones an operator most needs to see.

#### Scenario: A fresh application gets diagnostics with no extra dependency

- **WHEN** an application depends only on the SDK and starts a runner that emits a `warn`-level diagnostic
- **THEN** the line SHALL be written to standard error, and no warning about a missing logging provider SHALL be printed

#### Scenario: No third-party logging facade on the compile classpath

- **WHEN** the SDK's published modules are resolved
- **THEN** no third-party logging facade or provider SHALL appear as a compile-scope dependency

#### Scenario: A failed write never takes the process down

- **GIVEN** a runner whose standard error has been closed
- **WHEN** the SDK emits a diagnostic during its drain
- **THEN** the write SHALL fail silently and the drain SHALL complete normally

### Requirement: The Host Supplies Its Own Logger Through the Runner Builder

The Java SDK SHALL let the application replace the logger through a builder method on the runner, so diagnostics can be routed into whatever logging pipeline the host already runs.

This is the same authoring surface Node (`runner.logger(l)`), PHP
(`Runner->logger()`) and Ruby (`Flezha.log =`) already carry. A host that wants
SLF4J, Log4j, structured fields or spans supplies them; the SDK depends on none
of them.

#### Scenario: Diagnostics reach the host's logger

- **WHEN** a logger is supplied through the runner builder before start
- **THEN** every SDK diagnostic at or above the active level SHALL be emitted through it, and none SHALL be written to the default sink

#### Scenario: A supplied logger survives the runner's close

- **GIVEN** a runner whose logger was supplied through the builder
- **WHEN** the runner is closed and the JVM-wide runner slot is released
- **THEN** diagnostics emitted during and after the close SHALL still reach the supplied logger

### Requirement: The Java SDK Honours FLEZHA_LOG_LEVEL

The Java SDK SHALL read its default diagnostic level from `FLEZHA_LOG_LEVEL`, with the same level names and the same `warn` default the other SDKs use.

`FLEZHA_LOG_LEVEL` is read by the Node, Ruby, PHP and Python SDKs and by the
controller's own configuration. An operator setting one variable across a fleet
of mixed-language runners SHALL not have to except the Java ones.

#### Scenario: The variable sets the level

- **WHEN** a runner starts with `FLEZHA_LOG_LEVEL=debug`
- **THEN** the SDK SHALL emit diagnostics at `debug` and above

#### Scenario: Unset, blank or unrecognised all mean warn

- **WHEN** a runner starts with `FLEZHA_LOG_LEVEL` unset, empty, or set to a name that is not a level
- **THEN** the SDK SHALL emit diagnostics at `warn` and above

#### Scenario: A supplied logger governs its own level

- **GIVEN** `FLEZHA_LOG_LEVEL` is set
- **WHEN** the application supplies its own logger through the runner builder
- **THEN** the variable SHALL NOT filter what reaches that logger, which decides for itself what it writes

### Requirement: Both operative kinds live in one package

The Java SDK SHALL place the shared authoring base and both operative kinds in
`com.flezha.sdk.operative`. The base SHALL be named `Operative`, and
`StreamOperative` and `WorkflowOperative` SHALL both extend it from that package.
No operative kind SHALL be reachable only from a package that the other kind is
absent from.

#### Scenario: The workflow kind is not in a package of its own

- **WHEN** user code imports `com.flezha.sdk.workflow.WorkflowOperative`
- **THEN** compilation SHALL fail, the package having been folded into
  `com.flezha.sdk.operative` with no shim

#### Scenario: Both kinds and the base share one package

- **WHEN** the authoring surface is enumerated
- **THEN** `Operative`, `StreamOperative` and `WorkflowOperative` SHALL all resolve
  from `com.flezha.sdk.operative`, and the workflow supporting types
  (`WorkflowOperativeCtx`, `LoopMode`, `RollbackMode`, `WorkflowLoopBody` and the
  workflow exception types) SHALL resolve from the same package

### Requirement: Both kinds declare their graph in `channels` on the `Operative` base

`channels(ChannelBuilder)` SHALL be declared once, abstract, on `com.flezha.sdk.operative.Operative`; `StreamOperative` and `WorkflowOperative` SHALL inherit it and SHALL NOT redeclare it, and `WorkflowOperative` SHALL carry no `tasklets` method. A workflow class SHALL be built through the same `StreamOperativeTopologyBuilder` entry a stream is, which reads the kind from the class's interface, and SHALL be registered as the same `StreamOperativeTopology` a stream is; the `@Tasklet` processor SHALL emit `channels` for a generated operative of either kind. `WorkflowOperative` SHALL keep only the settings that are a workflow's own: `maxDuration()`, `selfTerminating()`, `loopMode()`, `rollbackMode()`, `persistenceEnabled()` and `syncDurability()`; `retryAttempts()`, `retryBackoff()`, `barrierRetryAttempts()` and `wakeAfter()` SHALL NOT exist, and `WorkflowOperativeConfigurationException` SHALL carry no `ZeroRetryBackoff`. The javadoc of `Operative`, `StreamOperative`, `WorkflowOperative`, `com.flezha.sdk.operative.package-info` and `com.flezha.sdk.channel.package-info` SHALL describe a workflow as a graph of tasklets committed stage by stage and resumed at the stage, and SHALL NOT describe it as ordered steps, a step sequence, or resuming at a step.

#### Scenario: A workflow class writes channels

- **WHEN** a class implementing `WorkflowOperative` overrides `channels(ChannelBuilder c)` and wires `c.input(OrderTicket.class, "order").into(ReserveFunds.class)`
- **THEN** `registerFactory` SHALL declare a workflow whose graph is that wiring, and a class that instead declares `tasklets(ChannelBuilder)` SHALL compile only as an unrelated method the runtime never calls, its `channels` being abstract and unimplemented

#### Scenario: A class of both kinds is refused

- **WHEN** a class implements both `StreamOperative` and `WorkflowOperative`
- **THEN** it SHALL fail to compile unless it resolves the `persistenceEnabled()` default both kinds declare, a class that does SHALL be refused at `registerFactory` with `AmbiguousOperativeKind`, and the base's single abstract `channels` SHALL give it no second body to write

#### Scenario: The retry knobs are gone from the interface

- **WHEN** a class overrides `retryAttempts()` on `WorkflowOperative`
- **THEN** compilation SHALL fail, because the interface declares no such method, and the edge's `retryMax` / `retryBackoffMs` SHALL be the only retry declaration a workflow can write

#### Scenario: The javadoc speaks in the graph vocabulary

- **WHEN** `sdk/java/flezha-sdk-api/src/main/java` is searched for "ordered steps", "step sequence", "resumes at the step", "between two steps" or "step boundary"
- **THEN** no javadoc or comment carries any of them, and `sdk/java/flezha-sdk-runtime/src/main/java` carries none either

### Requirement: A Java workflow class carries no stream-only setting and no wake

`HostClassDeclaration.forWorkflow` SHALL put on the wire every key the class's builder recorded, tier and checkpoint keys included, and SHALL run the core's check on a record that carries the `workflow` key, so a stream-only setting on a workflow class is refused by the core under its name (`EffectivelyOnceOnWorkflow`, `CheckpointOnWorkflow`, `CorrectionWindowOnWorkflow`) at `registerFactory`, wrapped in `WorkflowOperativeConfigurationException` with the name in its message, and never stripped, never honoured in process while withheld from the controller. `WorkflowOperative` SHALL declare no `wakeAfter()`, and the declaration SHALL carry no `wake_after_ms`. Inside a workflow instance, `OperativeInstance.runItem` SHALL attach a `TimerSession` to the `EmitContext` exactly as it does for a stream instance, for `run` and for `onTimer`, so `timerAfter`, `timerAtEventTime`, `cancelProcessingTimeTimer` and `cancelEventTimeTimer` work in both kinds; `compensate` SHALL attach none.

#### Scenario: The tier on a workflow class is refused by the core's name

- **WHEN** a `WorkflowOperative`'s `channels` calls `c.effectivelyOnce()`
- **THEN** `registerFactory` SHALL throw `WorkflowOperativeConfigurationException` whose message contains `EffectivelyOnceOnWorkflow`, and the class SHALL NOT be registered

#### Scenario: A checkpoint setting on a workflow class is refused by its own name

- **WHEN** a `WorkflowOperative`'s `channels` calls `c.checkpointEvery(100)`
- **THEN** `registerFactory` SHALL throw with a message containing `CheckpointOnWorkflow`

#### Scenario: A timer inside a workflow tasklet fires

- **WHEN** a tasklet of a running `PER_INPUT` workflow calls `ctx.timerAfter(Duration.ofMillis(300))` in `run`
- **THEN** the call SHALL answer the scheduled timestamp, the timer op SHALL reach the effect, and `onTimer` SHALL run on that Cell at that timestamp with `firedAt` equal to it, opening a pass at that tasklet

#### Scenario: The retired wake method does not compile

- **WHEN** a class implementing `WorkflowOperative` writes `@Override public Optional<Duration> wakeAfter()`
- **THEN** compilation fails, the interface declaring no such method

