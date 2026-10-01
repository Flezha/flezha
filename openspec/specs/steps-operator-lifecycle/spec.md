# steps-operator-lifecycle Specification

## Purpose
TBD - created by archiving change steps-operator-functionality. Update Purpose after archive.
## Requirements
### Requirement: A steps operator is spawned on demand and terminates on an exit trigger

A steps operator SHALL be spawned on demand with `init_values` as initial state per tasklet and SHALL take its input as messages on its entry channels, each spawn independent and representing **one job/session** (cells are internal parallelism, never multiple sessions). It SHALL terminate on the first of these exit triggers: (a) **all tasklets done**: its chain of tasklets has run to the final tasklet's completion with no loop active; (b) a tasklet calling `ctx.complete(result)`, which ends the instance once the iteration it ran in has committed; (c) a loop running out of work (a chained loop releasing an empty batch); (d) a declared maximum wall-clock duration elapsing; (e) an external `flz operator stop`. Waiting for a message SHALL NOT be an exit trigger. A flow operator SHALL never reach a terminal trigger, and a tasklet of a flow operator calling `ctx.complete` SHALL fail that input with an error naming `runtime.complete_outside_steps`.

#### Scenario: The chain running to its end terminates the operator

- **WHEN** a steps operator's last tasklet completes (all its cells finished) and no loop is active
- **THEN** the operator SHALL proceed to terminal teardown with status `ok`

#### Scenario: A tasklet's completion terminates the operator

- **WHEN** a tasklet of a steps operator calls `ctx.complete(result)`
- **THEN** the rest of its iteration SHALL run and commit, the operator SHALL accept no further input, tear down terminally, and report `result` in its completion

#### Scenario: A tasklet of a flow operator cannot complete it

- **WHEN** a tasklet of a flow operator calls `ctx.complete(result)`
- **THEN** that input SHALL fail with an error naming `runtime.complete_outside_steps`, handled as any tasklet error, and the flow operator SHALL keep running

#### Scenario: Duration elapse terminates the operator

- **WHEN** a steps operator declares `max_duration` and is still running when it elapses
- **THEN** the SDK SHALL cooperatively cancel in-flight work and tear the operator down

#### Scenario: Waiting for a message does not end the instance

- **WHEN** a `once` steps instance is started without a message and none arrives within the idle window
- **THEN** it SHALL NOT complete; a durable instance SHALL park and an opted-out one SHALL keep waiting, until a message, a stop or its `max_duration`

### Requirement: Internal loop with an intrinsic terminal condition

A steps operator SHALL loop by declaring its loop mode (`PerInput`, one iteration per message, or `Chained`, each iteration seeded by the previous one's terminal output into the chain the first message started), never by a loop written inside a tasklet body. Waiting for the next message SHALL happen at a committed iteration boundary, observe the cancellation token (duration or external stop), and block or park while idle. A loop SHALL have at least one **non-timeout** exit: a tasklet's `ctx.complete`, a chained loop's empty batch, or external stop. Authoring a steps operator whose **only** possible exit is external cancellation SHALL be rejected at registration with a clear error directing the author to add a terminal condition. A declared `max_duration` MAY additionally bound any loop as a backstop but SHALL NOT be required when a non-timeout exit exists.

#### Scenario: A loop ends on a tasklet's completion

- **WHEN** a `PerInput` steps operator's tasklet calls `ctx.complete(result)` while handling a message
- **THEN** the loop SHALL NOT wait for another message after that iteration commits, and the operator SHALL proceed toward terminal teardown with status `ok`

#### Scenario: Loop exits on cancellation

- **WHEN** a steps operator is looping and the duration elapses or an external stop arrives
- **THEN** the loop SHALL observe the cancellation token and stop iterating

#### Scenario: External-stop-only loop authoring is rejected

- **WHEN** a steps operator declares a loop and marks itself as having no terminal condition and no duration bound, so its only exit is an external kill
- **THEN** registration SHALL reject it, naming the missing terminal condition

### Requirement: A steps operator can be stopped externally

`flz operator stop <id>` (and the equivalent controller-issued path) SHALL **cooperatively** cancel a steps operator (distinct from a hard removal) and drive the standard terminal teardown with completion status `cancelled`, whether the instance is awake or asleep. A sleeping (suspended) instance SHALL first be woken through the ordinary bind on a live runner, with the stop carried by that wake so `StepsStop` reaches the runner as soon as the binding is published; the manager SHALL answer `{:ok, :cooperative}` once the wake is under way. The woken instance, waiting at a committed boundary with nothing owed, SHALL end `cancelled` with nothing to roll back, exactly as an awake instance stopped at the same boundary; a message that starts a pass before the stop lands SHALL be cancelled with that pass's compensation. Its completion record SHALL be written and its state directory kept under the finished-instance retention rule. Only when no runner can take the wake, or an awake instance's runner cannot be reached, SHALL the stop fall back to the hard removal, and it SHALL do so the same way for both. A flow operator's stop SHALL remain a hard removal, awake or asleep.

#### Scenario: User stops a running steps operator

- **WHEN** the user runs `flz operator stop <id>` for a running steps operator
- **THEN** the SDK SHALL cancel in-flight work, run compensation if partial progress was made, emit completion with status `cancelled`, and the Controller SHALL tear it down terminally

#### Scenario: User stops a sleeping steps operator

- **WHEN** the user runs `flz operator stop <id>` for a durable loop that is suspended and a runner is available
- **THEN** the instance SHALL be rehydrated onto that runner, `StepsStop` SHALL follow the bind, the instance SHALL end with a `cancelled` completion record, its state directory SHALL be kept, and the stop SHALL answer `{:ok, :cooperative}`, exactly as for the same loop stopped awake at its boundary

#### Scenario: A sleeping instance with no runner to wake on is removed hard

- **WHEN** the user stops a suspended steps instance while no runner in its namespace can take it
- **THEN** the stop SHALL take the hard removal, as it does for an awake instance whose runner cannot be reached, and SHALL NOT leave the instance suspended

#### Scenario: A sleeping flow operator is removed hard

- **WHEN** the user runs `flz operator stop <id>` for a flow instance that is suspended
- **THEN** the instance SHALL be removed and its state directory deleted, as for an awake flow operator, with no runner woken

### Requirement: All exit triggers converge on one terminal teardown

Every exit trigger SHALL drive the single existing `OperatorCompleted` teardown, in order: cancel in-flight work → run compensation if terminating abnormally (see `steps-operator-saga`) → emit completion (status `ok | error | cancelled` plus result/error payload) → deregister, disable plugins, release quota, and start the finished-job retention timer (see `steps-operator-durability`). There SHALL NOT be a second teardown codepath, and convergence SHALL never restart or rebind a terminal steps operator. A `OperatorCompleted` from a runner whose stable identity the instance's ownership record does not name is not an exit trigger: it SHALL write no completion record and drive no teardown, and SHALL be refused as `runner-identity-and-session` prescribes. Before a durable instance is deregistered, the teardown SHALL re-drive and wait (steps) for every outbox entry its journal still owes, so the last tasklet's output — flushed after its commit ack — is never torn down under; an entry still owed past the bound stays journaled. An opted-out instance's outputs travel the data stream while its completion travels the control stream; before deregistering it the teardown SHALL flush the runner's stream channels synchronously, so every output frame the transport has already delivered is routed and run first, and it SHALL NOT hold the teardown against a count of outputs the runner reports having written.

#### Scenario: Teardown is identical across triggers

- **WHEN** a steps operator terminates by chain completion, `ctx.complete`, loop exit, duration, or external stop
- **THEN** the Controller SHALL run the same teardown and convergence SHALL NOT act on it

#### Scenario: The last output is delivered before the teardown

- **WHEN** a durable chain completes with its final tasklet's outbox journaled but not yet flushed
- **THEN** the Controller SHALL flush it to the sink before the instance is deregistered

#### Scenario: An opted-out instance's last output is delivered before the teardown

- **WHEN** an opted-out chain writes its final output to the data stream and completes immediately after
- **THEN** the Controller SHALL run that output's plugin chain before the instance is deregistered, however the two frames interleave on arrival

#### Scenario: A teardown ends without waiting out a bound for an output that cannot arrive

- **WHEN** an opted-out instance completes after an output of its run was lost rather than delivered
- **THEN** the teardown SHALL still complete promptly once the stream channels report themselves flushed and the journal owes nothing, rather than waiting out a bound that only expiry can end

#### Scenario: An output that beats the stream's routing is still delivered

- **WHEN** a chain emits within its first milliseconds, before the owning session's stream channel holds a route for it
- **THEN** the stream SHALL route the frame through the Overwatch the ownership record names, which the class registered before its SDK start, and cache the route; a frame for an operator neither the channel nor the record knows SHALL be counted as a drop, never discarded silently

#### Scenario: An output for a replaced Overwatch is still delivered

- **WHEN** an operator's Overwatch is replaced — by a crash-resume, a rebind, or a supervisor restart — while its runner is still writing that operator's outputs to the data stream
- **THEN** the stream SHALL route those frames to the live Overwatch rather than to the replaced one, and SHALL NOT deliver a frame to a process that is no longer alive

#### Scenario: Completion carries a result/error payload

- **WHEN** a steps operator terminates
- **THEN** its `OperatorCompleted` SHALL include the terminal result (or error) payload and the Controller SHALL make it available to bound consumers and to `flz operator info`

#### Scenario: A completion from a runner that does not own the instance tears nothing down

- **GIVEN** a steps instance bound to runner A's stable identity and a second session B in the namespace
- **WHEN** B emits `OperatorCompleted` naming the instance
- **THEN** the instance SHALL keep running on A, no completion record SHALL be written, `flz operator info` SHALL not report it finished, and B SHALL be sent `RemOperator` for it once

### Requirement: A steps operator receives its input through the flow operator's entrance

A steps operator SHALL declare one or more entry channels and SHALL receive every message through the same entrance a flow operator's frames take: the channel the message names SHALL be resolved among the declared entry channels, a message naming none SHALL take the first declared entry channel, a message naming a channel the operator does not declare SHALL be parked under that name rather than dropped or rerouted, the entry channel's parser (`input_with(name, parser)` or its codec) SHALL run, and the entry edge's `map`, `filter` and `flat_map` SHALL run before the first tasklet sees the value. A message the entrance cannot read SHALL ride the entry channel's retry ladder and then be parked to the dead-letter queue under the channel's identity, and SHALL NOT count as a message: a `PER_INPUT` loop SHALL take the next one and a `ONCE` or `CHAINED` instance SHALL keep waiting for its message; the run SHALL NOT end or compensate for it, in any loop mode. For a **durable** steps instance, every message SHALL additionally be journaled to the instance's durable inbox before delivery, so a message sent while the instance is suspended, crashed, or migrating SHALL NOT be lost and SHALL be delivered on rehydrate. An operator waiting for a message SHALL block or sleep while idle and SHALL NOT busy-spin; a **durable** operator waiting at a committed iteration boundary, the boundary before its first pass included, SHALL be suspended (evicted from runner memory) per the `durable-suspension` capability instead of holding live resources.

#### Scenario: A message is delivered through the entry channel it names

- **WHEN** a steps operator declaring entry channels `orders` and `cancels` receives a frame with `channel = "cancels"`
- **THEN** the frame SHALL be parsed by `cancels`'s parser, mapped by `cancels`'s edge transforms and delivered to `cancels`'s consumer tasklets, and `orders`'s chain SHALL NOT run in that pass

#### Scenario: A message naming no channel takes the first entry channel

- **WHEN** a spawn payload with no `channel` metadata reaches a steps operator declaring `orders` first
- **THEN** it SHALL enter `orders`

#### Scenario: A message naming an undeclared channel is parked

- **WHEN** a frame with `channel = "refunds"` reaches a steps operator that declares no such entry channel
- **THEN** the payload SHALL be parked under `refunds` with the declared entry set in its error, no tasklet SHALL run, and the instance SHALL keep waiting

#### Scenario: An unreadable message rides the ladder and does not count

- **WHEN** a `once` steps operator whose entry channel decodes JSON receives a message that is not JSON
- **THEN** the message SHALL ride the entry channel's retry ladder and be parked to the dead-letter queue, the instance SHALL NOT complete or compensate, and the next readable message SHALL run its pass

#### Scenario: An unreadable message on a host SDK is told apart from a tasklet failure

- **WHEN** a Java, Node, Python, Ruby or PHP steps operator's entry decoder rejects a message
- **THEN** the SDK SHALL report the activation as unreadable, the core SHALL park the message to the dead-letter queue under the entry channel after the ladder, and the run SHALL NOT end, whereas a readable message on which the tasklet throws SHALL end the run as a tasklet failure does

#### Scenario: Input sent while the instance is down is not lost

- **WHEN** a data frame is sent to a durable steps instance that is suspended or crashed
- **THEN** the frame SHALL be durably journaled and delivered when the instance rehydrates

#### Scenario: An idle instance does not busy-spin

- **WHEN** a steps operator is waiting for the next message with none available
- **THEN** it SHALL sleep/block until a message arrives or a cancellation (duration/external stop) trips, without consuming CPU in a spin loop, and a durable operator at a committed iteration boundary SHALL be evicted from memory rather than occupying the runner

