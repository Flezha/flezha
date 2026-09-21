# workflow-operative Specification

## Purpose

The workflow-operative capability is the contract for the **Workflow Operative** kind, the operative that runs to an exit trigger and finishes. It defines the `WorkflowOperative` authoring surface (a graph of tasklets declared in `tasklets` on the builder a stream uses; a class declaring none is refused), registration through the runner's `register_factory`, the workflow-kind spawn path through one sealed per-runner operative registry, and the terminal `WorkflowCompleted` control message that drives standard teardown with no retry and no convergence action. Because the workflow kind does not declare `persistent_state?`, per-Cell state queries against a workflow operative are rejected through the kind-unsupported gate. Loops, exit triggers and a tasklet's `ctx.complete` are in `workflow-operative-lifecycle` and `workflow-durable-loop`.
## Requirements
### Requirement: A workflow operative is authored with a distinct run-once trait

The SDK SHALL expose a `WorkflowOperative` authoring surface distinct from the stream one, registered through the runner's `register_factory`, making the workflow class spawnable by its `factory_id`. A workflow operative SHALL declare its tasklets, Channels and Cells in a `tasklets` body on the same graph builder a stream uses; there SHALL be no one-function `run(ctx, args)` form, and a workflow class declaring no tasklets SHALL be refused at registration. Registering a workflow operative SHALL NOT require the author to enumerate capability flags; choosing the workflow kind is sufficient.

#### Scenario: A registered workflow operative is spawnable by factory id

- **WHEN** a workflow class is registered with the SDK before the runner starts
- **THEN** its `factory_id` SHALL be resolvable, and an `AddOperative` naming that `factory_id` with the workflow kind SHALL spawn a workflow instance

#### Scenario: A workflow with no tasklets is refused

- **WHEN** a workflow class whose `tasklets` body declares no tasklet is handed to `register_factory`
- **THEN** registration SHALL fail naming the class and the missing tasklets, and nothing SHALL be declared

### Requirement: Starting an operative with the workflow kind spawns a workflow instance

When the Controller issues `AddOperative` with `kind` resolving to `:workflow`, the SDK SHALL spawn a **workflow** instance for the named `factory_id`; when `kind` resolves to `:stream` (the default), it SHALL spawn a stream instance. The two variants SHALL be held in one sealed per-runner operative registry whose entry is resolved once per inbound frame at the routing boundary. Spawning SHALL be idempotent on a duplicate instance id.

#### Scenario: Workflow kind spawns a workflow instance

- **WHEN** the SDK handles `AddOperative` whose `kind` resolves to `:workflow` for a registered workflow `factory_id`
- **THEN** a workflow instance SHALL be created and registered under the instance id

#### Scenario: Default kind still spawns a stream instance

- **WHEN** the SDK handles `AddOperative` whose `kind` is empty or `:stream`
- **THEN** a stream instance SHALL be created

#### Scenario: Unknown workflow factory is reported

- **WHEN** `AddOperative` names a `factory_id` with workflow kind that is not registered as workflow
- **THEN** the SDK SHALL report an error and SHALL NOT create an instance

### Requirement: A workflow operative runs to an exit trigger and deregisters terminally

A workflow operative SHALL be spawned on demand with `args` (`init_values`), representing **one job/session**. It SHALL be **structured**: Tasklets, Channels, and Cells on the same per-cell engine as a stream operative, run level by level, each tasklet **done when all its cells finish** (see `workflow-operative-structure`). A workflow operative MAY **receive input after it starts** (an inbox), **loop** by its declared loop mode, be bounded by a declared maximum duration, be stopped externally, declare per-tasklet compensation hooks with an author-selected rollback mode (see `workflow-operative-saga`), and, being **durable by default** (opt-out per class), resume from durable state across a crash (see `workflow-operative-durability`). It SHALL terminate on the first exit trigger (all tasklets done with no loop active, a tasklet's `ctx.complete`, a loop running out of work, duration, or external stop). On termination, the SDK SHALL emit a `WorkflowCompleted` control message carrying the terminal result/error payload and a status (`ok | error | cancelled`); the Controller SHALL then drive the **single** standard operative teardown, running compensation first when terminating abnormally, so the workflow operative is deregistered, its plugins disabled, and its quota released, terminal in every case, with convergence never acting on it, and its data retained for the finished-job window. A tasklet failing after its retry ladder SHALL be terminal with no automatic platform retry.

#### Scenario: Clean completion deregisters the workflow operative

- **WHEN** a workflow operative reaches an exit trigger with all work successful
- **THEN** the SDK SHALL emit `WorkflowCompleted` with status `ok` and the result payload, and the Controller SHALL deregister it, disable its plugins, release its quota, and retain its data for the finished-job window

#### Scenario: Failure compensates then terminates with no auto-retry

- **WHEN** a workflow operative's tasklet returns an error after partial progress
- **THEN** the SDK SHALL fire compensation for completed tasklets in reverse order (per the operative's rollback mode), emit `WorkflowCompleted` with status `error` and the error payload, and the Controller SHALL tear it down terminally without convergence rebinding it

#### Scenario: External stop is terminal

- **WHEN** a running workflow operative is stopped via `flz operative stop`
- **THEN** the SDK SHALL cooperatively cancel in-flight work, run compensation if partial progress was made, emit `WorkflowCompleted` with status `cancelled`, and the Controller SHALL tear it down terminally

#### Scenario: A tasklet's completion carries the result

- **WHEN** a tasklet of a running workflow calls `ctx.complete(result)` and its iteration commits
- **THEN** the SDK SHALL emit `WorkflowCompleted` with status `ok` and that result, and `flz operative info` SHALL show it

