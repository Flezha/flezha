# steps-operator Specification

## Purpose

The steps-operator capability is the contract for an operator whose `run` is **steps**, the operator that runs to an exit trigger and finishes. It defines its authoring surface (a graph of tasklets declared in `channels` on the builder a flow operator uses; a steps class declaring none is refused), registration through the runner's `register_factory`, the steps spawn path through one sealed per-runner operator registry, and the terminal `OperatorCompleted` control message that drives standard teardown with no retry and no convergence action. Loops, exit triggers and a tasklet's `ctx.complete` are in `steps-operator-lifecycle` and `steps-durable-loop`.
## Requirements
### Requirement: A steps operator is authored with a distinct run-once trait

The SDK SHALL expose a `Operator` authoring surface distinct from the flow one, registered through the runner's `register_factory`, making the steps class spawnable by its `factory_id`. A steps operator SHALL declare its tasklets, Channels and Cells in a `channels` body on the same graph builder a stream uses, the body carrying the one name both kinds give it in every SDK; there SHALL be no `tasklets` body and no one-function `run(ctx, args)` form, and a steps class declaring no tasklets SHALL be refused at registration. Registering a steps operator SHALL NOT require the author to enumerate capability flags; choosing the steps run is sufficient, and the runner SHALL announce that kind to the controller with the factory id.

#### Scenario: A registered steps operator is spawnable by factory id

- **WHEN** a steps class is registered with the SDK before the runner starts
- **THEN** its `factory_id` SHALL be resolvable, the runner SHALL announce it with kind `steps`, and an `AddOperator` naming that `factory_id` with the steps run SHALL spawn a steps instance

#### Scenario: A steps operator with no tasklets is refused

- **WHEN** a steps class whose `channels` body declares no tasklet is handed to `register_factory`
- **THEN** registration SHALL fail naming the class and the missing tasklets, and nothing SHALL be declared

#### Scenario: The steps body is the flow body

- **WHEN** a steps class in Rust, Java or Elixir is written with `channels` and the same graph is written in a flow class's `channels`
- **THEN** both SHALL register, the two declarations SHALL differ only in the kind and the steps operator's own settings, and a `tasklets` member SHALL exist on no operator type in any SDK

### Requirement: Starting an operator with the steps run spawns a steps instance

The SDK SHALL spawn a **steps operator** instance when the Controller issues `AddOperator` for a
factory the runner registered as a steps class, and a flow instance for a flow class,
under the named `factory_id`. There SHALL be one instance type for both kinds: a
steps instance is the operator instance a flow operator is (its cells built by the same builder,
registered with the flusher the same way, reachable by the same live-state, restore, compaction
and migration commands) plus a pass sheet carrying the run. There SHALL be one class registry
carrying each class's kind, and one sealed per-runner operator registry whose entry is
resolved once per inbound frame at the routing boundary. Spawning SHALL be idempotent on a
duplicate instance id. A steps instance's bind SHALL create no thread: the sheet is built
from the bind's resume state (empty for a fresh instance) and the first pass starts on the
event that seeds it.

#### Scenario: A steps class spawns a steps instance

- **WHEN** the SDK handles `AddOperator` for a registered steps operator `factory_id`
- **THEN** a steps instance SHALL be created and registered under the instance id, with no thread spawned for it

#### Scenario: A flow class still spawns a flow instance

- **WHEN** the SDK handles `AddOperator` for a registered flow `factory_id`
- **THEN** a flow instance SHALL be created

#### Scenario: Unknown steps factory is reported

- **WHEN** `AddOperator` names a `factory_id` no registered class carries
- **THEN** the SDK SHALL report an error and SHALL NOT create an instance

#### Scenario: A durable steps operator's cells answer a live state read

- **WHEN** `flz cell state <instance>` names a running durable steps instance
- **THEN** the SDK's `CellState` handler SHALL find its cells through the same lookup a flow operator's
  are found by and answer their state in the per-tasklet / per-cell JSON shape, with no
  "Operator instance not found" refusal

#### Scenario: One registry serves both kinds

- **WHEN** a runner registers a flow class and a steps class
- **THEN** both SHALL be held in one class registry, each with its kind, and the factory ids
  advertised on connect SHALL come from that one registry

### Requirement: A steps operator runs to an exit trigger and deregisters terminally

A steps operator SHALL be spawned on demand with `init_values` meaning initial state per tasklet name, exactly as a flow operator's `init_values` do, and SHALL take its input as messages on its entry channels, representing **one job/session**. It SHALL be **structured**: Tasklets, Channels, and Cells on the same per-cell engine as a flow operator, each tasklet run once every producer feeding it has committed and **done when all its cells finish**, then committed without waiting for its siblings (see `steps-operator-structure`). A steps operator SHALL **receive its input after it starts** through the same entrance a flow operator uses, **loop** by its declared loop mode, MAY be bounded by a declared maximum duration, be stopped externally, declare per-tasklet compensation hooks with an author-selected rollback mode (see `steps-operator-saga`), and, being **durable by default** (opt-out per class), resume from durable state across a crash (see `steps-operator-durability`). It SHALL terminate on the first exit trigger (all tasklets done with no loop active, a tasklet's `ctx.complete`, a loop running out of work, duration, or external stop). On termination, the SDK SHALL emit a `OperatorCompleted` control message carrying the terminal result/error payload and a status (`ok | error | cancelled`); the Controller SHALL then drive the **single** standard operator teardown, running compensation first when terminating abnormally, so the steps operator is deregistered, its plugins disabled, and its quota released, terminal in every case, with convergence never acting on it, and its data retained for the finished-job window. A tasklet failing after its retry ladder SHALL be terminal with no automatic platform retry, except in a `per_input` loop declaring keep-progress, where it fails only the turn it ran in: the turn is dead-lettered and undone, and the loop takes its next message (see `steps-durable-loop`).

#### Scenario: Clean completion deregisters the steps operator

- **WHEN** a steps operator reaches an exit trigger with all work successful
- **THEN** the SDK SHALL emit `OperatorCompleted` with status `ok` and the result payload, and the Controller SHALL deregister it, disable its plugins, release its quota, and retain its data for the finished-job window

#### Scenario: Failure compensates then terminates with no auto-retry

- **WHEN** a steps operator's tasklet returns an error after partial progress, and the operator is not a `per_input` loop declaring keep-progress
- **THEN** the SDK SHALL fire compensation for completed tasklets in reverse order (per the operator's rollback mode), emit `OperatorCompleted` with status `error` and the error payload, and the Controller SHALL tear it down terminally without convergence rebinding it

#### Scenario: External stop is terminal

- **WHEN** a running steps operator is stopped via `flz operator stop`
- **THEN** the SDK SHALL cooperatively cancel in-flight work, run compensation if partial progress was made, emit `OperatorCompleted` with status `cancelled`, and the Controller SHALL tear it down terminally

#### Scenario: A tasklet's completion carries the result

- **WHEN** a tasklet of a running steps operator calls `ctx.complete(result)` and its iteration commits
- **THEN** the SDK SHALL emit `OperatorCompleted` with status `ok` and that result, and `flz operator info` SHALL show it

#### Scenario: Init values seed a steps tasklet's state

- **WHEN** a steps operator whose tasklet `reserve_funds` holds state is started with `init_values` `{"reserve_funds": {"budget": 100}}`
- **THEN** that tasklet's Cell SHALL start with the state `{"budget": 100}` before the first message is read, and no entry channel SHALL receive the document

#### Scenario: A per-input keep-progress turn's failure is not terminal

- **WHEN** a tasklet of a `per_input` keep-progress operator fails after its retry ladder
- **THEN** the SDK SHALL NOT emit `OperatorCompleted`, the turn SHALL be dead-lettered and
  undone, and the operator SHALL stay registered and take its next message

### Requirement: A steps operator is fed by messages on its entry channels in every loop mode

A steps operator's input SHALL be the messages sent to it: a tasklet's `ctx.spawn_steps` payload, `flz operator send`, a schedule's stored first message and `flz operator start --text`. Every loop mode SHALL read them: `ONCE` SHALL take exactly one message, run one pass and end; `PER_INPUT` SHALL take one message per pass; `CHAINED` SHALL take the first message, then seed each further pass from the previous pass's terminal handoff into the chain the first message started. An instance with no message to read SHALL wait for one and SHALL NOT end: a durable instance SHALL park after the idle window as a `per_input` loop does and the next message SHALL wake it, and an opted-out instance SHALL wait until a message or a stop arrives. A `max_duration` SHALL still end a waiting instance. A second message to a `ONCE` instance, and a message on another chain's channel while a `CHAINED` loop runs, SHALL be accepted and never read by that instance; no refusal or guard SHALL be built for either.

#### Scenario: A spawned once steps runs on its payload

- **WHEN** a tasklet calls `ctx.spawn_steps("paper_order", ticket)` and `paper_order` is a `once` steps operator whose entry channel `order` decodes the ticket's type
- **THEN** the spawned instance's first tasklet SHALL run over the decoded ticket, the pass SHALL complete and the instance SHALL end with status `ok`

#### Scenario: A once steps operator started without a message waits for it

- **WHEN** `flz operator start paper_order --kind steps` is run with no `--text` and `flz operator send <id> --text '<ticket>'` follows
- **THEN** the instance SHALL run its one pass over the sent ticket, and a durable instance that had parked in between SHALL be woken by the send

#### Scenario: A chained loop feeds the chain its first message picked

- **WHEN** a `chained` steps operator with entry channels `a` and `b`, each feeding its own chain, receives its first message on `b`
- **THEN** pass 0 SHALL run `b`'s chain over the message, every later pass SHALL feed the previous terminal batch into `b`'s chain, and `a`'s chain SHALL never run in that instance

#### Scenario: A second message to a once instance is not read

- **WHEN** two messages are sent to a `once` instance before it ends
- **THEN** the instance SHALL run its one pass over the first, the second SHALL stay unread, and no refusal SHALL be answered to the sender

### Requirement: A timer firing is an input to the run beside a message, and pending timers end with the instance

A steps instance SHALL take two kinds of trigger: a message on one of its entry channels, which starts that channel's chain, and a Cell timer's firing, which starts the chain of the tasklet that set it. Both SHALL obey the one-pass-at-a-time rule and the loop mode: a `PER_INPUT` instance SHALL run one pass per trigger in arrival order; a `CHAINED` instance SHALL serve a queued firing before its carry; a `ONCE` instance SHALL end on its one pass, so a firing that comes due after that pass's last stage has committed SHALL never run. When an instance ends, on any terminal outcome, its Cells' pending timers and any firing still queued SHALL be dropped with the Cells, exactly as a flow operator's are on removal; the runner SHALL log one debug-level line naming the instance and the count dropped, and the completion record SHALL be unchanged.

#### Scenario: A firing in a per_input loop is one iteration

- **WHEN** a `per_input` instance has consumed two messages and a timer set in the second fires
- **THEN** iteration 2 SHALL run from the tasklet that set it, `iter_committed(2)` SHALL be journaled, and the instance SHALL wait for the next trigger

#### Scenario: A once instance ends with its timers

- **WHEN** a `once` instance's tasklet registers a timer 30 s out and the pass completes in under a second
- **THEN** the instance SHALL end with status `ok` at its last stage's commit, the timer SHALL be dropped with the Cells, `on_timer` SHALL never run, and the completion record SHALL carry nothing about the timer

#### Scenario: A stop drops the timers with the cells

- **WHEN** a `per_input` instance with pending timers is stopped and its compensation has run
- **THEN** the instance SHALL end `cancelled`, and no timer of it SHALL fire afterwards

### Requirement: A per-input steps operator SHALL run as a partition group of independent instances

A steps operator whose loop mode is `per_input` SHALL be startable with `--partitions N`,
and the group SHALL be N ordinary steps instances `<id>#p0` .. `<id>#p{N-1}`, each with its own
key-group range, journal, durable input log, pass sheet and `max_duration` clock, started,
converged, read and rescaled through the same group module a flow operator's group is, with no group
mechanism of the steps operator's own. A message SHALL reach its member by key as a flow operator's does, and
the SDK's key-group guard SHALL apply to a steps operator member unchanged. Each member SHALL end and
compensate itself only: a member's exit trigger (`ctx.complete` in one of its passes, its
`max_duration`, a stop, a terminal failure) SHALL end that member alone, emitting its own
`OperatorCompleted`, driving its own teardown and writing its own completion record under its
member id, while the other members run on; a member's rollback, in either mode, SHALL undo that
member's passes and no other member's. There SHALL be no group-wide end, no group `complete`
result and no group rollback. The member count SHALL change only between passes, when every
member is quiet, and a rescale SHALL be a history boundary for each surviving member. A `once`
or `chained` steps operator SHALL be refused `--partitions` with a reason naming that its one message
would reach one member while the others waited for nothing.

#### Scenario: Messages by key reach their member and each member commits its own passes

- **WHEN** a `per_input` steps operator started with `--partitions 2` receives three messages whose
  keys hash into `#p0`'s range and two whose keys hash into `#p1`'s, through a keyed forward
- **THEN** `#p0` commits three iterations and `#p1` two, each in its own journal, each member's
  Cells hold only its own keys, and neither member's journal names the other's Cells

#### Scenario: One member ends itself and the other runs on

- **WHEN** a tasklet of `orders#p1` calls `ctx.complete(result)` and the iteration commits
- **THEN** `orders#p1` emits `OperatorCompleted` with status `ok` and `result`, is torn down and
  keeps its completion record under `orders#p1`, and `orders#p0` receives and runs its next
  message unaffected

#### Scenario: One member fails and compensates itself only

- **WHEN** a pass of `orders#p0` fails terminally after partial progress
- **THEN** `orders#p0` compensates its own completed tasklets per its rollback mode and ends with
  status `error`, no tasklet of `orders#p1` is compensated, and `orders#p1` keeps running

#### Scenario: A once steps operator is refused partitions

- **WHEN** `flz operator start <factory> --partitions 2` names a `once` steps operator
- **THEN** the start is refused with `topology.partitions_without_per_input_loop`, naming that one message
  reaches one member, and no instance runs

