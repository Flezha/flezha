# task-compute-unit Specification

## Purpose

The task-compute-unit capability adds a `:task` compute-unit kind: a stateless, run-once job that reuses the generic relay control plane instead of forking it. A task is authored through a distinct `Task` trait — a single `run(ctx, args) -> Result<Output>` function with no channels, partition keys, or per-partition state handle — and registered as a sibling of relay registration so it is spawnable by `factory_id`. The controller spawns a task by issuing `AddRelay --kind task`; the SDK invokes `run` exactly once on the work pool with `init_values` as `args`, may emit output to bound plugins via the existing fan-out, and ignores inbound data frames. On `run` returning, the SDK emits a `TaskCompleted` control message (status `"ok"` or `"error"`) over the existing SDK→Controller control path; the controller then drives the standard relay teardown so the task is deregistered from the durable registry, its plugins disabled, and its quota released — terminal on success and on failure, with no retry and convergence never acting on it. Because the task kind does not declare `persistent_state?`, per-partition state queries against a task are rejected through the existing kind-unsupported gate. The kind is selectable from the CLI via `flz relay start --kind task`, defaulting to `relay`.

## Requirements

### Requirement: A task is authored with a distinct run-once trait

The SDK SHALL expose a `Task` authoring surface distinct from the relay/shard surface: a single run-once function `run(ctx, args) -> Result<Output>` with no channels, no partition keys, and no per-partition state handle. A task SHALL be registered through a registration entrypoint that is a sibling of relay registration (`register_task`, alongside `register_relay`), making the task class spawnable by its `factory_id`. Registering a task SHALL NOT require the author to enumerate capability flags — choosing the task kind is sufficient.

#### Scenario: A registered task is spawnable by factory id

- **WHEN** a task class is registered with the SDK before the executor starts
- **THEN** its `factory_id` SHALL be resolvable by the executor, and an `AddRelay` naming that `factory_id` with the task kind SHALL spawn a task instance

#### Scenario: The task trait carries no relay machinery

- **WHEN** a task is authored
- **THEN** its authoring surface SHALL expose only `run(ctx, args)` and SHALL NOT require declaring shards, channels, partition keys, or a state handle

### Requirement: Starting a relay with the task kind spawns a task instance

When the controller issues `AddRelay` with `kind` resolving to `:task`, the SDK SHALL spawn a **task** instance for the named `factory_id`; when `kind` resolves to `:relay` (the default), it SHALL spawn a relay instance exactly as today. The two instance variants SHALL be held in one sealed per-executor unit registry whose entry is resolved once per inbound frame at the routing boundary. Spawning SHALL be idempotent on a duplicate instance id, as for relays.

#### Scenario: Task kind spawns a task

- **WHEN** the SDK handles `AddRelay` whose `kind` resolves to `:task` for a registered task `factory_id`
- **THEN** a task instance SHALL be created and registered under the instance id

#### Scenario: Default kind still spawns a relay

- **WHEN** the SDK handles `AddRelay` whose `kind` is empty or `:relay`
- **THEN** a relay instance SHALL be created exactly as before this change

#### Scenario: Unknown task factory is reported

- **WHEN** `AddRelay` names a `factory_id` with task kind that is not registered as a task
- **THEN** the SDK SHALL report an error and SHALL NOT create an instance

### Requirement: A task runs its function exactly once on start

On spawning a task instance, the SDK SHALL invoke the task's `run` exactly once, scheduled on the executor's work pool, supplying `args` taken from the `init_values` shipped with `AddRelay`. The SDK SHALL NOT route inbound wire data frames into a task: a task has no input channel, so a data frame addressed to a task instance SHALL be a logged no-op and SHALL NOT re-invoke `run`.

#### Scenario: run is invoked once with init_values args

- **WHEN** a task instance is spawned with non-empty `init_values`
- **THEN** `run` SHALL be called exactly once with those values as `args`

#### Scenario: An inbound data frame to a task is dropped

- **WHEN** a data frame is delivered for a task instance id
- **THEN** the SDK SHALL log and drop it, and SHALL NOT invoke `run` again

#### Scenario: A task writes no per-partition state

- **WHEN** a task runs to completion
- **THEN** no write-ahead log or snapshot files SHALL be written for it, because it declares no partitions and no `persistent_state?` capability

### Requirement: A task may emit output to plugins

A task's `run` SHALL be able to emit output through the same channel→plugin fan-out relays use, so a task can publish results to bound plugins (e.g. Kafka, stdout). A task that emits nothing SHALL be a valid pure side-effect job. Plugin binding and activation for a task SHALL work identically to a relay's — plugins are orthogonal to state and partitions.

#### Scenario: Task output reaches bound plugins

- **WHEN** a task is started with a bound output plugin and its `run` emits a value
- **THEN** the value SHALL be delivered to the bound plugin via the existing fan-out

#### Scenario: A pure side-effect task is valid

- **WHEN** a task's `run` emits nothing and returns success
- **THEN** the task SHALL complete normally with no error

### Requirement: A task signals completion to the controller

When a task's `run` returns, the SDK SHALL emit a `TaskCompleted` control message (an SDK→Controller `CommandRequest` variant) naming the instance id and carrying a status of `"ok"` on success or `"error"` (with an error description) on failure. The message SHALL follow the existing SDK-initiated control-message path used by `ExecutorShutdownNotification` / `ExecutorReconnected`. A controller that does not recognise the variant SHALL ignore it (existing decoder behaviour), so an old controller degrades gracefully.

#### Scenario: Successful completion emits status ok

- **WHEN** a task's `run` returns success
- **THEN** the SDK SHALL emit `TaskCompleted` with `status = "ok"` for the task's instance id

#### Scenario: Failed completion emits status error

- **WHEN** a task's `run` returns an error
- **THEN** the SDK SHALL emit `TaskCompleted` with `status = "error"` and an error description

### Requirement: The controller deregisters a completed task

On receiving `TaskCompleted`, the controller SHALL drive the existing relay stop/teardown path so the task is removed from the durable process registry, its plugins are disabled, and its allocation quota is released — the same teardown as an explicit stop. Deregistration SHALL remove the task's entry from the durable (DETS) registry so that boot-recovery never re-runs it and convergence never sees it. A task whose `run` errored SHALL be deregistered identically (terminal failure): the controller SHALL NOT retry it, and SHALL surface the failure via telemetry/log.

#### Scenario: A completed task is removed from the durable registry

- **WHEN** the controller receives `TaskCompleted` with `status = "ok"` for a running task
- **THEN** the task SHALL be torn down and its entry SHALL no longer be present in the durable process registry

#### Scenario: A finished task is not replayed on boot

- **WHEN** the controller restarts after a task has completed and been deregistered
- **THEN** boot-recovery SHALL NOT re-spawn the task

#### Scenario: An errored task is deregistered without retry

- **WHEN** the controller receives `TaskCompleted` with `status = "error"`
- **THEN** the task SHALL be torn down exactly as on success, no retry SHALL be issued, and the failure SHALL be surfaced via telemetry/log

#### Scenario: Convergence does not act on a finished task

- **WHEN** a task has completed and been deregistered
- **THEN** the convergence health loop SHALL NOT classify it as degraded or force a rebind, because it is no longer in the registry

### Requirement: Per-partition state queries on a task are rejected

A request for a task's per-partition state SHALL be rejected with the structured kind-unsupported error, because the task kind does not declare `persistent_state?`. The rejection SHALL reuse the existing state-introspection gate (no task-specific code path) and SHALL surface through the CLI as a clear, non-zero outcome distinct from "relay not found".

#### Scenario: State query on a task is unsupported

- **WHEN** `flz relay state <task>` is run for a running task
- **THEN** the result SHALL be `{:error, {:unsupported_for_kind, :persistent_state}}`, rendered as a clear non-zero outcome, and the task SHALL NOT be contacted for state

### Requirement: The task kind is selectable from the CLI

`flz relay start` SHALL accept a `--kind` option selecting the compute-unit kind, defaulting to `relay` when omitted. The selected kind SHALL be carried through to the relay spec so the controller sets the relay's `:kind` and populates the `AddRelay` wire field. An unknown `--kind` value SHALL fail at start with a clear error naming the kind, and no relay process SHALL be created.

#### Scenario: Starting a task from the CLI

- **WHEN** `flz relay start <id> <factory> --kind task` is run
- **THEN** the relay spec's kind SHALL be `:task` and a task instance SHALL be spawned

#### Scenario: Default CLI kind is relay

- **WHEN** `flz relay start <id> <factory>` is run without `--kind`
- **THEN** the relay's kind SHALL be `:relay`
