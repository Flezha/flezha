# task-compute-unit Specification

## Purpose

The task-compute-unit capability adds a `:bounded` operative kind: a stateless, run-once job that reuses the generic operative control plane instead of forking it. A bounded operative is authored through a distinct `BoundedOperative` trait — a single `run(ctx, args) -> Result<Output>` function with no channels, segment keys, or per-segment state handle — and registered as a sibling of continuous operative registration so it is spawnable by `factory_id`. The controller spawns a bounded operative by issuing `AddOperative --kind bounded`; the SDK invokes `run` exactly once on the work pool with `init_values` as `args`, may emit output to bound plugins via the existing fan-out, and ignores inbound data frames. On `run` returning, the SDK emits a `BoundedCompleted` control message (status `"ok"` or `"error"`) over the existing SDK→Controller control path; the controller then drives the standard operative teardown so the bounded operative is deregistered from the durable registry, its plugins disabled, and its quota released — terminal on success and on failure, with no retry and convergence never acting on it. Because the bounded kind does not declare `persistent_state?`, per-segment state queries against a bounded operative are rejected through the existing kind-unsupported gate. The kind is selectable from the CLI via `flz operative start --kind bounded`, defaulting to `continuous`.

## Requirements

### Requirement: A bounded operative is authored with a distinct run-once trait

The SDK SHALL expose a `BoundedOperative` authoring surface distinct from the continuous-operative/operation surface: a single run-once function `run(ctx, args) -> Result<Output>` with no channels, no segment keys, and no per-segment state handle. A bounded operative SHALL be registered through a registration entrypoint that is a sibling of continuous operative registration (`register_bounded`, alongside `register_continuous`), making the bounded operative class spawnable by its `factory_id`. Registering a bounded operative SHALL NOT require the author to enumerate capability flags — choosing the bounded kind is sufficient.

#### Scenario: A registered bounded operative is spawnable by factory id

- **WHEN** a bounded operative class is registered with the SDK before the executor starts
- **THEN** its `factory_id` SHALL be resolvable by the executor, and an `AddOperative` naming that `factory_id` with the bounded kind SHALL spawn a bounded operative instance

#### Scenario: The bounded operative trait carries no continuous-operative machinery

- **WHEN** a bounded operative is authored
- **THEN** its authoring surface SHALL expose only `run(ctx, args)` and SHALL NOT require declaring operations, channels, segment keys, or a state handle

### Requirement: Starting an operative with the bounded kind spawns a bounded operative instance

When the controller issues `AddOperative` with `kind` resolving to `:bounded`, the SDK SHALL spawn a **bounded** operative instance for the named `factory_id`; when `kind` resolves to `:continuous` (the default), it SHALL spawn a continuous operative instance exactly as today. The two instance variants SHALL be held in one sealed per-executor operative registry whose entry is resolved once per inbound frame at the routing boundary. Spawning SHALL be idempotent on a duplicate instance id, as for continuous operatives.

#### Scenario: Bounded kind spawns a bounded operative

- **WHEN** the SDK handles `AddOperative` whose `kind` resolves to `:bounded` for a registered bounded operative `factory_id`
- **THEN** a bounded operative instance SHALL be created and registered under the instance id

#### Scenario: Default kind still spawns a continuous operative

- **WHEN** the SDK handles `AddOperative` whose `kind` is empty or `:continuous`
- **THEN** a continuous operative instance SHALL be created exactly as before this change

#### Scenario: Unknown bounded operative factory is reported

- **WHEN** `AddOperative` names a `factory_id` with bounded kind that is not registered as a bounded operative
- **THEN** the SDK SHALL report an error and SHALL NOT create an instance

### Requirement: A bounded operative runs its function exactly once on start

On spawning a bounded operative instance, the SDK SHALL invoke the bounded operative's `run` exactly once, scheduled on the executor's work pool, supplying `args` taken from the `init_values` shipped with `AddOperative`. The SDK SHALL NOT route inbound wire data frames into a bounded operative: a bounded operative has no input channel, so a data frame addressed to a bounded operative instance SHALL be a logged no-op and SHALL NOT re-invoke `run`.

#### Scenario: run is invoked once with init_values args

- **WHEN** a bounded operative instance is spawned with non-empty `init_values`
- **THEN** `run` SHALL be called exactly once with those values as `args`

#### Scenario: An inbound data frame to a bounded operative is dropped

- **WHEN** a data frame is delivered for a bounded operative instance id
- **THEN** the SDK SHALL log and drop it, and SHALL NOT invoke `run` again

#### Scenario: A bounded operative writes no per-segment state

- **WHEN** a bounded operative runs to completion
- **THEN** no write-ahead log or snapshot files SHALL be written for it, because it declares no segments and no `persistent_state?` capability

### Requirement: A bounded operative may emit output to plugins

A bounded operative's `run` SHALL be able to emit output through the same channel→plugin fan-out continuous operatives use, so a bounded operative can publish results to bound plugins (e.g. Kafka, stdout). A bounded operative that emits nothing SHALL be a valid pure side-effect job. Plugin binding and activation for a bounded operative SHALL work identically to a continuous operative's — plugins are orthogonal to state and segments.

#### Scenario: Bounded operative output reaches bound plugins

- **WHEN** a bounded operative is started with a bound output plugin and its `run` emits a value
- **THEN** the value SHALL be delivered to the bound plugin via the existing fan-out

#### Scenario: A pure side-effect bounded operative is valid

- **WHEN** a bounded operative's `run` emits nothing and returns success
- **THEN** the bounded operative SHALL complete normally with no error

### Requirement: A bounded operative signals completion to the controller

When a bounded operative's `run` returns, the SDK SHALL emit a `BoundedCompleted` control message (an SDK→Controller `CommandRequest` variant) naming the instance id and carrying a status of `"ok"` on success or `"error"` (with an error description) on failure. The message SHALL follow the existing SDK-initiated control-message path used by `ExecutorShutdownNotification` / `ExecutorReconnected`. A controller that does not recognise the variant SHALL ignore it (existing decoder behaviour), so an old controller degrades gracefully.

#### Scenario: Successful completion emits status ok

- **WHEN** a bounded operative's `run` returns success
- **THEN** the SDK SHALL emit `BoundedCompleted` with `status = "ok"` for the bounded operative's instance id

#### Scenario: Failed completion emits status error

- **WHEN** a bounded operative's `run` returns an error
- **THEN** the SDK SHALL emit `BoundedCompleted` with `status = "error"` and an error description

### Requirement: The controller deregisters a completed bounded operative

On receiving `BoundedCompleted`, the controller SHALL drive the existing operative stop/teardown path so the bounded operative is removed from the durable process registry, its plugins are disabled, and its allocation quota is released — the same teardown as an explicit stop. Deregistration SHALL remove the bounded operative's entry from the durable (DETS) registry so that boot-recovery never re-runs it and convergence never sees it. A bounded operative whose `run` errored SHALL be deregistered identically (terminal failure): the controller SHALL NOT retry it, and SHALL surface the failure via telemetry/log.

#### Scenario: A completed bounded operative is removed from the durable registry

- **WHEN** the controller receives `BoundedCompleted` with `status = "ok"` for a running bounded operative
- **THEN** the bounded operative SHALL be torn down and its entry SHALL no longer be present in the durable process registry

#### Scenario: A finished bounded operative is not replayed on boot

- **WHEN** the controller restarts after a bounded operative has completed and been deregistered
- **THEN** boot-recovery SHALL NOT re-spawn the bounded operative

#### Scenario: An errored bounded operative is deregistered without retry

- **WHEN** the controller receives `BoundedCompleted` with `status = "error"`
- **THEN** the bounded operative SHALL be torn down exactly as on success, no retry SHALL be issued, and the failure SHALL be surfaced via telemetry/log

#### Scenario: Convergence does not act on a finished bounded operative

- **WHEN** a bounded operative has completed and been deregistered
- **THEN** the convergence health loop SHALL NOT classify it as degraded or force a rebind, because it is no longer in the registry

### Requirement: Per-segment state queries on a bounded operative are rejected

A request for a bounded operative's per-segment state SHALL be rejected with the structured kind-unsupported error, because the bounded kind does not declare `persistent_state?`. The rejection SHALL reuse the existing state-introspection gate (no bounded-specific code path) and SHALL surface through the CLI as a clear, non-zero outcome distinct from "operative not found".

#### Scenario: State query on a bounded operative is unsupported

- **WHEN** `flz operative state <bounded operative>` is run for a running bounded operative
- **THEN** the result SHALL be `{:error, {:unsupported_for_kind, :persistent_state}}`, rendered as a clear non-zero outcome, and the bounded operative SHALL NOT be contacted for state

### Requirement: The bounded kind is selectable from the CLI

`flz operative start` SHALL accept a `--kind` option selecting the operative kind, defaulting to `continuous` when omitted. The selected kind SHALL be carried through to the operative spec so the controller sets the operative's `:kind` and populates the `AddOperative` wire field. An unknown `--kind` value SHALL fail at start with a clear error naming the kind, and no operative process SHALL be created.

#### Scenario: Starting a bounded operative from the CLI

- **WHEN** `flz operative start <id> <factory> --kind bounded` is run
- **THEN** the operative spec's kind SHALL be `:bounded` and a bounded operative instance SHALL be spawned

#### Scenario: Default CLI kind is continuous

- **WHEN** `flz operative start <id> <factory>` is run without `--kind`
- **THEN** the operative's kind SHALL be `:continuous`
