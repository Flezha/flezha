# java-sdk-task-kind Specification

## Purpose

Defines the Bounded operative kind in the Java SDK: a distinct `BoundedOperative` authoring interface (run-once `run(ctx, args)`) registered via `Flezha.registerBounded`; a `BoundedClassRegistry`; a sealed `Operative` instance registry resolved once per inbound frame at the dispatch boundary; spawning a bounded operative from `AddOperative` whose `kind` resolves to `:bounded`; running it once on start with `init_values` args and optional plugin output via `BoundedCtx.emit`; a `BoundedCompleted` SDK->Controller signal byte-matching `relay-task-kind`; terminal failure semantics; and bounded operative factory advertisement on connect. Conforms to `docs/sdk_protocol_spec.md` and mirrors the Rust `task-compute-unit` capability.

## Requirements

### Requirement: A Java bounded operative is authored with a distinct run-once interface

The Java SDK SHALL expose a `BoundedOperative` authoring interface (`com.flezha.sdk.bounded.BoundedOperative`) distinct from `ContinuousOperative`/`Operation`: `String factoryId()` and a single run-once method `run(BoundedCtx ctx, String args) throws BoundedException`, with no channel declaration, no segment keys, and no `StateHandle`. A bounded operative SHALL be registered through `Flezha.registerBounded(Class<? extends BoundedOperative>)` — a sibling of `Flezha.registerContinuous` — making the bounded operative class spawnable by its `factoryId`. Registering a bounded operative SHALL NOT require the author to enumerate capability flags or declare any state/change/clock type; choosing the bounded kind is sufficient.

#### Scenario: A registered bounded operative is spawnable by factory id

- **WHEN** a `BoundedOperative` class is registered via `Flezha.registerBounded(GreetOperative.class)` before `Flezha.newInstance(...)`
- **THEN** `GreetOperative::factoryId` SHALL be resolvable by the executor, and an `AddOperative` naming that `factoryId` with the bounded kind SHALL spawn a bounded operative instance

#### Scenario: The bounded operative interface carries no continuous-operative machinery

- **WHEN** a bounded operative is authored
- **THEN** its authoring surface SHALL expose only `factoryId()` and `run(ctx, args)` and SHALL NOT require declaring operations, channels, segment keys, or a state handle

#### Scenario: Registering a bounded operative after newInstance is rejected

- **WHEN** user code calls `Flezha.registerBounded(...)` after `Flezha.newInstance(...)` has returned
- **THEN** the SDK SHALL throw an `IllegalStateException`

#### Scenario: Duplicate factoryId across a bounded operative and a continuous operative is rejected

- **WHEN** a `BoundedOperative` and a `ContinuousOperative` (or two bounded operatives) both return `"job"` from their factory-id method and both are registered
- **THEN** the second registration SHALL throw an `IllegalArgumentException` naming the duplicate factoryId

### Requirement: Registered bounded operative factories are advertised to the controller on connect

The Java SDK's connect handshake SHALL advertise the factory ids of every registered bounded operative alongside the registered continuous operative factory ids in `factory_ids` (and `factories`), so the controller validates `AddOperative` with the bounded kind for a bounded operative `factory_id`. The advertisement SHALL be a single combined list — no separate bounded-only handshake field.

#### Scenario: Bounded operative factory ids appear in the handshake

- **WHEN** `GreetOperative` (factoryId `"greet"`) is registered via `Flezha.registerBounded` and `MarketOperative` (factoryId `"market"`) via `Flezha.registerContinuous`, then `Flezha.newInstance(...)` connects
- **THEN** the handshake init JSON `factory_ids` SHALL include both `"greet"` and `"market"`

### Requirement: Starting an operative with the bounded kind spawns a bounded operative instance

When the controller issues `AddOperative` whose `kind` resolves to `:bounded`, the Java SDK SHALL spawn a **bounded** operative instance for the named `factory_id`; when `kind` is empty or resolves to `:continuous`, it SHALL spawn a continuous operative instance exactly as before this change. Both instance variants SHALL be held in one sealed per-executor operative registry whose entry is resolved once per inbound frame at the routing boundary. Spawning SHALL be idempotent on a duplicate instance id, as for continuous operatives. An `AddOperative` naming a bounded operative `factory_id` that is not registered as a bounded operative SHALL be reported as an error with no instance created.

#### Scenario: Bounded kind spawns a bounded operative

- **WHEN** the SDK handles `AddOperative` whose `kind` resolves to `:bounded` for a registered bounded operative `factory_id`
- **THEN** a `BoundedOperativeInstance` SHALL be created and registered under the instance id as the bounded variant of the sealed operative registry

#### Scenario: A bounded operative is not a continuous operative in the registry

- **WHEN** a bounded operative instance has been registered under an instance id
- **THEN** the continuous-only registry accessor SHALL return empty for that id, while the sealed-operative accessor SHALL return the bounded variant

#### Scenario: Unknown bounded operative factory is reported

- **WHEN** `AddOperative` names a `factory_id` with the bounded kind that is not registered as a bounded operative
- **THEN** the SDK SHALL append an error naming the missing bounded operative factory and SHALL NOT create an instance

### Requirement: A Java bounded operative runs its function exactly once on start

On spawning a bounded operative instance, the Java SDK SHALL invoke the bounded operative's `run` exactly once, scheduled on the executor's worker pool, supplying `args` taken from the `init_values` shipped with `AddOperative`. A duplicate `AddOperative` for a live bounded operative instance id SHALL NOT re-invoke `run`. The SDK SHALL NOT route inbound wire data frames into a bounded operative: a bounded operative has no input channel, so a data frame addressed to a bounded operative instance id SHALL be a logged no-op and SHALL NOT invoke `run`.

#### Scenario: run is invoked once with init_values args

- **WHEN** a bounded operative instance is spawned from `AddOperative{init_values:"hello", kind:"bounded"}`
- **THEN** the bounded operative's `run` SHALL be called exactly once with `"hello"` as `args`

#### Scenario: Duplicate AddOperative does not re-run the bounded operative

- **WHEN** a second `AddOperative` arrives for an already-live bounded operative instance id
- **THEN** the SDK SHALL treat it as idempotent and SHALL NOT invoke `run` a second time

#### Scenario: An inbound data frame to a bounded operative is dropped

- **WHEN** a data frame is delivered whose payload id is a bounded operative instance id
- **THEN** the SDK SHALL log and drop it at the dispatch boundary and SHALL NOT invoke `run` again

#### Scenario: A bounded operative writes no per-segment state

- **WHEN** a bounded operative runs to completion
- **THEN** no write-ahead log or snapshot files SHALL be written for it, because it declares no segments and no persistent state

### Requirement: A Java bounded operative may emit output to bound plugins

A bounded operative's `run` SHALL be able to emit output through `BoundedCtx.emit`, which writes a frame whose payload id is the bounded operative's instance id and whose metadata is empty, on a secondary stream — mirroring the Rust SDK's bounded-operative-output frame so the controller fans it out to the plugins bound to the operative by instance id. A bounded operative that emits nothing SHALL be a valid pure side-effect job. The bounded-operative-output frame SHALL NOT carry a `"channel"` metadata key (that is a continuous-operative concept).

#### Scenario: Bounded operative output reaches bound plugins

- **WHEN** a bounded operative started with a bound output plugin calls `ctx.emit(value)` in its `run`
- **THEN** the SDK SHALL frame `value` with payload id equal to the bounded operative instance id and empty metadata, write it on a secondary stream, and the value SHALL be delivered to the bound plugin

#### Scenario: A pure side-effect bounded operative is valid

- **WHEN** a bounded operative's `run` emits nothing and returns normally
- **THEN** the bounded operative SHALL complete with `status="ok"` and no error

### Requirement: A Java bounded operative signals completion to the controller

When a bounded operative's `run` returns, the Java SDK SHALL emit a `BoundedCompleted` control message (an SDK→Controller `CommandRequest` with the `bounded_completed` oneof set) naming the bounded operative's instance id and carrying `status = "ok"` on normal return, or `status = "error"` with the throwable's message when `run` throws a `BoundedException` or any unchecked exception. The message SHALL be framed as a `control_command` and written on the control stream via the same SDK-initiated control-write path used by `ExecutorShutdownNotification`, and its byte form SHALL match the `relay-task-kind` (#2) contract so the controller decodes Java and Rust bounded-operative completions identically.

#### Scenario: Successful completion emits status ok

- **WHEN** a bounded operative's `run` returns normally
- **THEN** the SDK SHALL emit `BoundedCompleted` with `operative_id` = the instance id and `status = "ok"`

#### Scenario: Failed completion emits status error with the message

- **WHEN** a bounded operative's `run` throws a `BoundedException("boom")`
- **THEN** the SDK SHALL emit `BoundedCompleted` with `status = "error"` and `error` containing `"boom"`, and SHALL NOT retry `run`

#### Scenario: An unchecked exception is reported as a terminal error

- **WHEN** a bounded operative's `run` throws an unchecked `RuntimeException`
- **THEN** the SDK SHALL catch it, emit `BoundedCompleted` with `status = "error"` and the exception's message, and SHALL NOT crash the executor or re-invoke `run`

### Requirement: The Java bounded-operative-output and completion frames are byte-compatible with the Rust SDK

The `BoundedCompleted` `CommandRequest` and the bounded-operative-output frame produced by the Java SDK SHALL be byte-compatible with those produced by the Rust SDK for an equivalent bounded operative, so the unchanged Elixir controller drives a Java bounded operative identically to a Rust one. The controller-side completion handling, teardown, and deregistration are defined by `relay-task-kind` and are reused unchanged — this change adds no controller code.

#### Scenario: Controller tears down a completed Java bounded operative

- **WHEN** a Java bounded operative completes and emits `BoundedCompleted{status:"ok"}` to the unchanged controller
- **THEN** the controller SHALL drive its existing stop/teardown path so the bounded operative is removed from the durable registry and is not replayed on a controller restart — identically to a Rust bounded operative

#### Scenario: Controller surfaces a Java bounded operative failure without retry

- **WHEN** a Java bounded operative emits `BoundedCompleted{status:"error", error:"boom"}`
- **THEN** the controller SHALL tear the bounded operative down exactly as on success, issue no retry, and surface the failure via its existing telemetry/log path
