# bounded-operative Specification

## Purpose

The bounded-operative capability is the contract for the **Bounded Operative** kind — the operative that runs once to completion and finishes (formerly "Task"). It defines the `BoundedOperative` authoring surface (a single run-once `run(ctx, args)` with no channels, segment keys, or per-segment state), the `register_bounded` entrypoint sibling to `register_continuous`, the bounded-kind spawn path through one sealed per-executor operative registry, and the terminal `BoundedCompleted` control message that drives standard teardown with no retry and no convergence action. Behaviour is identical to the former task kind; only the contract's names change. Because the bounded kind does not declare `persistent_state?`, per-Segment state queries against a bounded operative are rejected through the kind-unsupported gate. Duration bounds, self/external stop, internal loops, and retry/replay are out of scope.

## Requirements

### Requirement: A bounded operative is authored with a distinct run-once trait

The SDK SHALL expose a `BoundedOperative` authoring surface distinct from the continuous (Operation/Channel) surface: a single run-once function `run(ctx, args) -> Result<Output>` with no channels, no segment keys, and no per-segment state handle. A bounded operative SHALL be registered through an entrypoint that is a sibling of continuous registration (`register_bounded`, alongside `register_continuous`), making the bounded class spawnable by its `factory_id`. Registering a bounded operative SHALL NOT require the author to enumerate capability flags — choosing the bounded kind is sufficient. This requirement renames the former `Task` trait and `register_task` entrypoint without changing their behaviour.

#### Scenario: A registered bounded operative is spawnable by factory id

- **WHEN** a bounded class is registered with the SDK before the executor starts
- **THEN** its `factory_id` SHALL be resolvable, and an `AddOperative` naming that `factory_id` with the bounded kind SHALL spawn a bounded instance

#### Scenario: The bounded trait carries no continuous machinery

- **WHEN** a bounded operative is authored
- **THEN** its surface SHALL expose only `run(ctx, args)` and SHALL NOT require declaring operations, channels, segment keys, or a state handle

### Requirement: Starting an operative with the bounded kind spawns a bounded instance

When the Controller issues `AddOperative` with `kind` resolving to `:bounded`, the SDK SHALL spawn a **bounded** instance for the named `factory_id`; when `kind` resolves to `:continuous` (the default), it SHALL spawn a continuous instance. The two variants SHALL be held in one sealed per-executor operative registry whose entry is resolved once per inbound frame at the routing boundary. Spawning SHALL be idempotent on a duplicate instance id.

#### Scenario: Bounded kind spawns a bounded instance

- **WHEN** the SDK handles `AddOperative` whose `kind` resolves to `:bounded` for a registered bounded `factory_id`
- **THEN** a bounded instance SHALL be created and registered under the instance id

#### Scenario: Default kind still spawns a continuous instance

- **WHEN** the SDK handles `AddOperative` whose `kind` is empty or `:continuous`
- **THEN** a continuous instance SHALL be created

#### Scenario: Unknown bounded factory is reported

- **WHEN** `AddOperative` names a `factory_id` with bounded kind that is not registered as bounded
- **THEN** the SDK SHALL report an error and SHALL NOT create an instance

### Requirement: A bounded operative runs once to completion and deregisters terminally

The SDK SHALL invoke `run` exactly once on the work pool with `init_values` as `args`. The bounded operative MAY emit output to bound plugins via the existing fan-out and SHALL ignore inbound data frames. On `run` returning, the SDK SHALL emit a `BoundedCompleted` control message (status `"ok"` or `"error"`) over the existing SDK→Controller control path; the Controller SHALL then drive the standard operative teardown so the bounded operative is deregistered from the durable registry, its plugins disabled, and its quota released — terminal on success and on failure, with no retry, and convergence never acting on it. This requirement renames the former `TaskCompleted` message to `BoundedCompleted` and preserves the run-once behaviour exactly; duration bounds, self/external stop, internal loops, and retry/replay are out of scope for this change.

#### Scenario: Completion deregisters the bounded operative

- **WHEN** a bounded operative's `run` returns successfully
- **THEN** the SDK SHALL emit `BoundedCompleted` with status `"ok"` and the Controller SHALL deregister it, disable its plugins, and release its quota

#### Scenario: Failure is terminal with no retry

- **WHEN** a bounded operative's `run` returns an error
- **THEN** the SDK SHALL emit `BoundedCompleted` with status `"error"`, the Controller SHALL tear it down terminally, and convergence SHALL NOT attempt to restart or rebind it

### Requirement: Bounded operatives reject per-segment state queries

Because the bounded kind does not declare `persistent_state?`, a per-Segment state query against a bounded operative SHALL be rejected through the kind-unsupported gate with `{:error, {:unsupported_for_kind, :persistent_state}}` rather than contacting the computation.

#### Scenario: Segment-state query against a bounded operative is rejected

- **WHEN** a per-Segment state query targets a bounded operative
- **THEN** it SHALL return `{:error, {:unsupported_for_kind, :persistent_state}}`
