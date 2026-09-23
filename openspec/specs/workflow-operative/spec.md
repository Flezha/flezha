# workflow-operative Specification

## Purpose

The workflow-operative capability is the contract for the **Workflow Operative** kind, the operative that runs to an exit trigger and finishes. It defines the `WorkflowOperative` authoring surface (a graph of tasklets declared in `tasklets` on the builder a stream uses; a class declaring none is refused), registration through the runner's `register_factory`, the workflow-kind spawn path through one sealed per-runner operative registry, and the terminal `WorkflowCompleted` control message that drives standard teardown with no retry and no convergence action. Loops, exit triggers and a tasklet's `ctx.complete` are in `workflow-operative-lifecycle` and `workflow-durable-loop`.
## Requirements
### Requirement: A workflow operative is authored with a distinct run-once trait

The SDK SHALL expose a `WorkflowOperative` authoring surface distinct from the stream one, registered through the runner's `register_factory`, making the workflow class spawnable by its `factory_id`. A workflow operative SHALL declare its tasklets, Channels and Cells in a `channels` body on the same graph builder a stream uses, the body carrying the one name both kinds give it in every SDK; there SHALL be no `tasklets` body and no one-function `run(ctx, args)` form, and a workflow class declaring no tasklets SHALL be refused at registration. Registering a workflow operative SHALL NOT require the author to enumerate capability flags; choosing the workflow kind is sufficient, and the runner SHALL announce that kind to the controller with the factory id.

#### Scenario: A registered workflow operative is spawnable by factory id

- **WHEN** a workflow class is registered with the SDK before the runner starts
- **THEN** its `factory_id` SHALL be resolvable, the runner SHALL announce it with kind `workflow`, and an `AddOperative` naming that `factory_id` with the workflow kind SHALL spawn a workflow instance

#### Scenario: A workflow with no tasklets is refused

- **WHEN** a workflow class whose `channels` body declares no tasklet is handed to `register_factory`
- **THEN** registration SHALL fail naming the class and the missing tasklets, and nothing SHALL be declared

#### Scenario: The workflow body is the stream's body

- **WHEN** a workflow class in Rust, Java or Elixir is written with `channels` and the same graph is written in a stream class's `channels`
- **THEN** both SHALL register, the two declarations SHALL differ only in the kind and the workflow's own settings, and a `tasklets` member SHALL exist on no operative type in any SDK

### Requirement: Starting an operative with the workflow kind spawns a workflow instance

The SDK SHALL spawn a **workflow** instance when the Controller issues `AddOperative` for a
factory the runner registered as a workflow class, and a stream instance for a stream class,
under the named `factory_id`. There SHALL be one instance type for both kinds: a
workflow instance is the operative instance a stream is (its cells built by the same builder,
registered with the flusher the same way, reachable by the same live-state, restore, compaction
and migration commands) plus a pass sheet carrying the run. There SHALL be one class registry
carrying each class's kind, and one sealed per-runner operative registry whose entry is
resolved once per inbound frame at the routing boundary. Spawning SHALL be idempotent on a
duplicate instance id. A workflow instance's bind SHALL create no thread: the sheet is built
from the bind's resume state (empty for a fresh instance) and the first pass starts on the
event that seeds it.

#### Scenario: A workflow class spawns a workflow instance

- **WHEN** the SDK handles `AddOperative` for a registered workflow `factory_id`
- **THEN** a workflow instance SHALL be created and registered under the instance id, with no thread spawned for it

#### Scenario: A stream class still spawns a stream instance

- **WHEN** the SDK handles `AddOperative` for a registered stream `factory_id`
- **THEN** a stream instance SHALL be created

#### Scenario: Unknown workflow factory is reported

- **WHEN** `AddOperative` names a `factory_id` no registered class carries
- **THEN** the SDK SHALL report an error and SHALL NOT create an instance

#### Scenario: A durable workflow's cells answer a live state read

- **WHEN** `flz cell state <instance>` names a running durable workflow instance
- **THEN** the SDK's `CellState` handler SHALL find its cells through the same lookup a stream's
  are found by and answer their state in the per-tasklet / per-cell JSON shape, with no
  "StreamOperative instance not found" refusal

#### Scenario: One registry serves both kinds

- **WHEN** a runner registers a stream class and a workflow class
- **THEN** both SHALL be held in one class registry, each with its kind, and the factory ids
  advertised on connect SHALL come from that one registry

### Requirement: A workflow operative runs to an exit trigger and deregisters terminally

A workflow operative SHALL be spawned on demand with `init_values` meaning initial state per tasklet name, exactly as a stream operative's `init_values` do, and SHALL take its input as messages on its entry channels, representing **one job/session**. It SHALL be **structured**: Tasklets, Channels, and Cells on the same per-cell engine as a stream operative, run level by level, each tasklet **done when all its cells finish** (see `workflow-operative-structure`). A workflow operative SHALL **receive its input after it starts** through the same entrance a stream operative uses, **loop** by its declared loop mode, MAY be bounded by a declared maximum duration, be stopped externally, declare per-tasklet compensation hooks with an author-selected rollback mode (see `workflow-operative-saga`), and, being **durable by default** (opt-out per class), resume from durable state across a crash (see `workflow-operative-durability`). It SHALL terminate on the first exit trigger (all tasklets done with no loop active, a tasklet's `ctx.complete`, a loop running out of work, duration, or external stop). On termination, the SDK SHALL emit a `WorkflowCompleted` control message carrying the terminal result/error payload and a status (`ok | error | cancelled`); the Controller SHALL then drive the **single** standard operative teardown, running compensation first when terminating abnormally, so the workflow operative is deregistered, its plugins disabled, and its quota released, terminal in every case, with convergence never acting on it, and its data retained for the finished-job window. A tasklet failing after its retry ladder SHALL be terminal with no automatic platform retry.

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

#### Scenario: Init values seed a workflow tasklet's state

- **WHEN** a workflow operative whose tasklet `reserve_funds` holds state is started with `init_values` `{"reserve_funds": {"budget": 100}}`
- **THEN** that tasklet's Cell SHALL start with the state `{"budget": 100}` before the first message is read, and no entry channel SHALL receive the document

### Requirement: A workflow operative is fed by messages on its entry channels in every loop mode

A workflow operative's input SHALL be the messages sent to it: a tasklet's `ctx.spawn_workflow` payload, `flz operative send`, a schedule's stored first message and `flz operative start --text`. Every loop mode SHALL read them: `ONCE` SHALL take exactly one message, run one pass and end; `PER_INPUT` SHALL take one message per pass; `CHAINED` SHALL take the first message, then seed each further pass from the previous pass's terminal handoff into the chain the first message started. An instance with no message to read SHALL wait for one and SHALL NOT end: a durable instance SHALL park after the idle window as a `per_input` loop does and the next message SHALL wake it, and an opted-out instance SHALL wait until a message or a stop arrives. A `max_duration` SHALL still end a waiting instance. A second message to a `ONCE` instance, and a message on another chain's channel while a `CHAINED` loop runs, SHALL be accepted and never read by that instance; no refusal or guard SHALL be built for either.

#### Scenario: A spawned once workflow runs on its payload

- **WHEN** a tasklet calls `ctx.spawn_workflow("paper_order", ticket)` and `paper_order` is a `once` workflow whose entry channel `order` decodes the ticket's type
- **THEN** the spawned instance's first tasklet SHALL run over the decoded ticket, the pass SHALL complete and the instance SHALL end with status `ok`

#### Scenario: A once workflow started without a message waits for it

- **WHEN** `flz operative start paper_order --kind workflow` is run with no `--text` and `flz operative send <id> --text '<ticket>'` follows
- **THEN** the instance SHALL run its one pass over the sent ticket, and a durable instance that had parked in between SHALL be woken by the send

#### Scenario: A chained loop feeds the chain its first message picked

- **WHEN** a `chained` workflow with entry channels `a` and `b`, each feeding its own chain, receives its first message on `b`
- **THEN** pass 0 SHALL run `b`'s chain over the message, every later pass SHALL feed the previous terminal batch into `b`'s chain, and `a`'s chain SHALL never run in that instance

#### Scenario: A second message to a once instance is not read

- **WHEN** two messages are sent to a `once` instance before it ends
- **THEN** the instance SHALL run its one pass over the first, the second SHALL stay unread, and no refusal SHALL be answered to the sender

### Requirement: A timer firing is an input to the run beside a message, and pending timers end with the instance

A workflow instance SHALL take two kinds of trigger: a message on one of its entry channels, which starts that channel's chain, and a Cell timer's firing, which starts the chain of the tasklet that set it. Both SHALL obey the one-pass-at-a-time rule and the loop mode: a `PER_INPUT` instance SHALL run one pass per trigger in arrival order; a `CHAINED` instance SHALL serve a queued firing before its carry; a `ONCE` instance SHALL end on its one pass, so a firing that comes due after that pass's last stage has committed SHALL never run. When an instance ends, on any terminal outcome, its Cells' pending timers and any firing still queued SHALL be dropped with the Cells, exactly as a stream's are on removal; the runner SHALL log one debug-level line naming the instance and the count dropped, and the completion record SHALL be unchanged.

#### Scenario: A firing in a per_input loop is one iteration

- **WHEN** a `per_input` instance has consumed two messages and a timer set in the second fires
- **THEN** iteration 2 SHALL run from the tasklet that set it, `iter_committed(2)` SHALL be journaled, and the instance SHALL wait for the next trigger

#### Scenario: A once instance ends with its timers

- **WHEN** a `once` instance's tasklet registers a timer 30 s out and the pass completes in under a second
- **THEN** the instance SHALL end with status `ok` at its last stage's commit, the timer SHALL be dropped with the Cells, `on_timer` SHALL never run, and the completion record SHALL carry nothing about the timer

#### Scenario: A stop drops the timers with the cells

- **WHEN** a `per_input` instance with pending timers is stopped and its compensation has run
- **THEN** the instance SHALL end `cancelled`, and no timer of it SHALL fire afterwards

