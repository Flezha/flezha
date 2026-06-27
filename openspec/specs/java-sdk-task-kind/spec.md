# java-sdk-task-kind Specification

## Purpose

Defines the Task compute-unit kind in the Java SDK: a distinct `Task` authoring interface (run-once `run(ctx, args)`) registered via `Flezha.registerTask`; a `TaskClassRegistry`; a sealed `Unit` instance registry resolved once per inbound frame at the dispatch boundary; spawning a task from `AddRelay` whose `kind` resolves to `:task`; running it once on start with `init_values` args and optional plugin output via `TaskCtx.emit`; a `TaskCompleted` SDK->Controller signal byte-matching `relay-task-kind`; terminal failure semantics; and task factory advertisement on connect. Conforms to `docs/sdk_protocol_spec.md` and mirrors the Rust `task-compute-unit` capability.

## Requirements

### Requirement: A Java task is authored with a distinct run-once interface

The Java SDK SHALL expose a `Task` authoring interface (`com.flezha.sdk.task.Task`) distinct from `Relay`/`Shard`: `String factoryId()` and a single run-once method `run(TaskCtx ctx, String args) throws TaskException`, with no channel declaration, no partition keys, and no `StateHandle`. A task SHALL be registered through `Flezha.registerTask(Class<? extends Task>)` — a sibling of `Flezha.registerRelay` — making the task class spawnable by its `factoryId`. Registering a task SHALL NOT require the author to enumerate capability flags or declare any state/change/clock type; choosing the task kind is sufficient.

#### Scenario: A registered task is spawnable by factory id

- **WHEN** a `Task` class is registered via `Flezha.registerTask(GreetTask.class)` before `Flezha.newInstance(...)`
- **THEN** `GreetTask::factoryId` SHALL be resolvable by the executor, and an `AddRelay` naming that `factoryId` with the task kind SHALL spawn a task instance

#### Scenario: The task interface carries no relay machinery

- **WHEN** a task is authored
- **THEN** its authoring surface SHALL expose only `factoryId()` and `run(ctx, args)` and SHALL NOT require declaring shards, channels, partition keys, or a state handle

#### Scenario: Registering a task after newInstance is rejected

- **WHEN** user code calls `Flezha.registerTask(...)` after `Flezha.newInstance(...)` has returned
- **THEN** the SDK SHALL throw an `IllegalStateException`

#### Scenario: Duplicate factoryId across a task and a relay is rejected

- **WHEN** a `Task` and a `Relay` (or two tasks) both return `"job"` from their factory-id method and both are registered
- **THEN** the second registration SHALL throw an `IllegalArgumentException` naming the duplicate factoryId

### Requirement: Registered task factories are advertised to the controller on connect

The Java SDK's connect handshake SHALL advertise the factory ids of every registered task alongside the registered relay factory ids in `factory_ids` (and `factories`), so the controller validates `AddRelay` with the task kind for a task `factory_id`. The advertisement SHALL be a single combined list — no separate task-only handshake field.

#### Scenario: Task factory ids appear in the handshake

- **WHEN** `GreetTask` (factoryId `"greet"`) is registered via `Flezha.registerTask` and `MarketRelay` (factoryId `"market"`) via `Flezha.registerRelay`, then `Flezha.newInstance(...)` connects
- **THEN** the handshake init JSON `factory_ids` SHALL include both `"greet"` and `"market"`

### Requirement: Starting a unit with the task kind spawns a task instance

When the controller issues `AddRelay` whose `kind` resolves to `:task`, the Java SDK SHALL spawn a **task** instance for the named `factory_id`; when `kind` is empty or resolves to `:relay`, it SHALL spawn a relay instance exactly as before this change. Both instance variants SHALL be held in one sealed per-executor unit registry whose entry is resolved once per inbound frame at the routing boundary. Spawning SHALL be idempotent on a duplicate instance id, as for relays. An `AddRelay` naming a task `factory_id` that is not registered as a task SHALL be reported as an error with no instance created.

#### Scenario: Task kind spawns a task

- **WHEN** the SDK handles `AddRelay` whose `kind` resolves to `:task` for a registered task `factory_id`
- **THEN** a `TaskInstance` SHALL be created and registered under the instance id as the task variant of the sealed unit registry

#### Scenario: A task is not a relay in the registry

- **WHEN** a task instance has been registered under an instance id
- **THEN** the relay-only registry accessor SHALL return empty for that id, while the sealed-unit accessor SHALL return the task variant

#### Scenario: Unknown task factory is reported

- **WHEN** `AddRelay` names a `factory_id` with the task kind that is not registered as a task
- **THEN** the SDK SHALL append an error naming the missing task factory and SHALL NOT create an instance

### Requirement: A Java task runs its function exactly once on start

On spawning a task instance, the Java SDK SHALL invoke the task's `run` exactly once, scheduled on the executor's worker pool, supplying `args` taken from the `init_values` shipped with `AddRelay`. A duplicate `AddRelay` for a live task instance id SHALL NOT re-invoke `run`. The SDK SHALL NOT route inbound wire data frames into a task: a task has no input channel, so a data frame addressed to a task instance id SHALL be a logged no-op and SHALL NOT invoke `run`.

#### Scenario: run is invoked once with init_values args

- **WHEN** a task instance is spawned from `AddRelay{init_values:"hello", kind:"task"}`
- **THEN** the task's `run` SHALL be called exactly once with `"hello"` as `args`

#### Scenario: Duplicate AddRelay does not re-run the task

- **WHEN** a second `AddRelay` arrives for an already-live task instance id
- **THEN** the SDK SHALL treat it as idempotent and SHALL NOT invoke `run` a second time

#### Scenario: An inbound data frame to a task is dropped

- **WHEN** a data frame is delivered whose payload id is a task instance id
- **THEN** the SDK SHALL log and drop it at the dispatch boundary and SHALL NOT invoke `run` again

#### Scenario: A task writes no per-partition state

- **WHEN** a task runs to completion
- **THEN** no write-ahead log or snapshot files SHALL be written for it, because it declares no partitions and no persistent state

### Requirement: A Java task may emit output to bound plugins

A task's `run` SHALL be able to emit output through `TaskCtx.emit`, which writes a frame whose payload id is the task's instance id and whose metadata is empty, on a secondary stream — mirroring the Rust SDK's task-output frame so the controller fans it out to the plugins bound to the unit by instance id. A task that emits nothing SHALL be a valid pure side-effect job. The task-output frame SHALL NOT carry a `"channel"` metadata key (that is a relay concept).

#### Scenario: Task output reaches bound plugins

- **WHEN** a task started with a bound output plugin calls `ctx.emit(value)` in its `run`
- **THEN** the SDK SHALL frame `value` with payload id equal to the task instance id and empty metadata, write it on a secondary stream, and the value SHALL be delivered to the bound plugin

#### Scenario: A pure side-effect task is valid

- **WHEN** a task's `run` emits nothing and returns normally
- **THEN** the task SHALL complete with `status="ok"` and no error

### Requirement: A Java task signals completion to the controller

When a task's `run` returns, the Java SDK SHALL emit a `TaskCompleted` control message (an SDK→Controller `CommandRequest` with the `task_completed` oneof set) naming the task's instance id and carrying `status = "ok"` on normal return, or `status = "error"` with the throwable's message when `run` throws a `TaskException` or any unchecked exception. The message SHALL be framed as a `control_command` and written on the control stream via the same SDK-initiated control-write path used by `ExecutorShutdownNotification`, and its byte form SHALL match the `relay-task-kind` (#2) contract so the controller decodes Java and Rust task completions identically.

#### Scenario: Successful completion emits status ok

- **WHEN** a task's `run` returns normally
- **THEN** the SDK SHALL emit `TaskCompleted` with `relay_id` = the instance id and `status = "ok"`

#### Scenario: Failed completion emits status error with the message

- **WHEN** a task's `run` throws a `TaskException("boom")`
- **THEN** the SDK SHALL emit `TaskCompleted` with `status = "error"` and `error` containing `"boom"`, and SHALL NOT retry `run`

#### Scenario: An unchecked exception is reported as a terminal error

- **WHEN** a task's `run` throws an unchecked `RuntimeException`
- **THEN** the SDK SHALL catch it, emit `TaskCompleted` with `status = "error"` and the exception's message, and SHALL NOT crash the executor or re-invoke `run`

### Requirement: The Java task-output and completion frames are byte-compatible with the Rust SDK

The `TaskCompleted` `CommandRequest` and the task-output frame produced by the Java SDK SHALL be byte-compatible with those produced by the Rust SDK for an equivalent task, so the unchanged Elixir controller drives a Java task identically to a Rust one. The controller-side completion handling, teardown, and deregistration are defined by `relay-task-kind` and are reused unchanged — this change adds no controller code.

#### Scenario: Controller tears down a completed Java task

- **WHEN** a Java task completes and emits `TaskCompleted{status:"ok"}` to the unchanged controller
- **THEN** the controller SHALL drive its existing stop/teardown path so the task is removed from the durable registry and is not replayed on a controller restart — identically to a Rust task

#### Scenario: Controller surfaces a Java task failure without retry

- **WHEN** a Java task emits `TaskCompleted{status:"error", error:"boom"}`
- **THEN** the controller SHALL tear the task down exactly as on success, issue no retry, and surface the failure via its existing telemetry/log path
