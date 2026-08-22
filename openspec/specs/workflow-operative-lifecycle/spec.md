# workflow-operative-lifecycle Specification

## Purpose
TBD - created by archiving change workflow-operative-functionality. Update Purpose after archive.
## Requirements
### Requirement: A workflow operative is spawned on demand and terminates on an exit trigger

A workflow operative SHALL be spawned on demand with `args` (`init_values`), each spawn independent and representing **one job/session** (entities are internal parallelism, never multiple sessions). It SHALL terminate on the first of these exit triggers: (a) **all procedures done** — its linear chain of procedures has run to the final procedure's completion with no loop active; (b) an explicit `ctx.complete(result)` call; (c) an internal loop's terminal predicate; (d) a declared maximum wall-clock duration elapsing; (e) an external `flz operative stop`. A stream operative SHALL never reach a terminal trigger.

#### Scenario: The chain running to its end terminates the operative

- **WHEN** a workflow operative's last procedure completes (all its entities finished) and no loop is active
- **THEN** the operative SHALL proceed to terminal teardown with status `ok`

#### Scenario: Explicit completion terminates the operative

- **WHEN** a workflow operative calls `ctx.complete(result)`
- **THEN** it SHALL stop accepting new work, tear down terminally, and report `result` in its completion

#### Scenario: Duration elapse terminates the operative

- **WHEN** a workflow operative declares `max_duration` and is still running when it elapses
- **THEN** the SDK SHALL cooperatively cancel in-flight work and tear the operative down

### Requirement: A workflow operative may receive input after it starts

A workflow operative MAY declare an entry channel and **receive input after it has started** (an inbox). The SDK dispatch path SHALL route a data frame addressed to a running workflow instance to its current procedure's entry channel/entity, rather than dropping it. For a **durable** workflow instance, post-start input SHALL additionally be journaled to the instance's durable inbox before delivery, so input sent while the instance is suspended, crashed, or migrating SHALL NOT be lost and SHALL be delivered on rehydrate. An operative waiting on its inbox SHALL block/sleep while idle and SHALL NOT busy-spin; a **looping durable** operative waiting at a committed iteration boundary SHALL be suspended (evicted from runner memory) per the `workflow-durable-suspension` capability instead of holding live resources.

#### Scenario: Input delivered after start reaches the procedure

- **WHEN** a workflow operative has started and a data frame is sent to it
- **THEN** the frame SHALL be delivered to the running procedure's entry channel and processed, not dropped

#### Scenario: Input sent while the instance is down is not lost

- **WHEN** a data frame is sent to a durable workflow instance that is suspended or crashed
- **THEN** the frame SHALL be durably journaled and delivered when the instance rehydrates

#### Scenario: An idle interactive operative does not busy-spin

- **WHEN** a workflow operative is waiting for the next input with none available
- **THEN** it SHALL sleep/block until input arrives or a cancellation (duration/external stop) trips, without consuming CPU in a spin loop — and, for a looping durable operative at a committed iteration boundary, it SHALL be evicted from memory rather than occupying the runner

### Requirement: Internal loop with an intrinsic terminal condition

The SDK SHALL provide a loop construct usable inside a Procedure/`run` that repeats a body until the author's terminal predicate is satisfied OR a cancellation token (duration or external stop) trips. The construct SHALL observe the cancellation token automatically and SHALL block/sleep while idle. A loop SHALL have at least one **non-timeout** exit (a terminal predicate, an exhausted input source, or external stop); authoring a workflow operative whose **only** possible exit is external cancellation SHALL be rejected at registration with a clear error directing the author to add a terminal condition. A declared `max_duration` MAY additionally bound any loop as a backstop but SHALL NOT be required when a non-timeout exit exists.

#### Scenario: Loop exits on the terminal predicate

- **WHEN** a workflow operative loops with a predicate that becomes true
- **THEN** the loop SHALL exit and the operative SHALL proceed toward terminal teardown with status `ok`

#### Scenario: Loop exits on cancellation

- **WHEN** a workflow operative is looping and the duration elapses or an external stop arrives
- **THEN** the loop SHALL observe the cancellation token and stop iterating

#### Scenario: External-stop-only loop authoring is rejected

- **WHEN** a workflow operative uses the loop construct with no terminal predicate, no exhausted-input exit, and no duration bound — so its only exit is an external kill
- **THEN** registration SHALL reject it, naming the missing terminal condition

### Requirement: A workflow operative can be stopped externally

`flz operative stop <id>` (and the equivalent controller-issued path) SHALL **cooperatively** cancel a running workflow operative — distinct from a hard removal — and drive the standard terminal teardown with completion status `cancelled`.

#### Scenario: Operator stops a running workflow operative

- **WHEN** the operator runs `flz operative stop <id>` for a running workflow operative
- **THEN** the SDK SHALL cancel in-flight work, run compensation if partial progress was made, emit completion with status `cancelled`, and the Controller SHALL tear it down terminally

### Requirement: All exit triggers converge on one terminal teardown

Every exit trigger SHALL drive the single existing `WorkflowCompleted` teardown, in order: cancel in-flight work → run compensation if terminating abnormally (see `workflow-operative-saga`) → emit completion (status `ok | error | cancelled` plus result/error payload) → deregister, disable plugins, release quota, and start the finished-job retention timer (see `workflow-operative-durability`). There SHALL NOT be a second teardown codepath, and convergence SHALL never restart or rebind a terminal workflow operative. Before a durable instance is deregistered, the teardown SHALL re-drive and wait (bounded) for every outbox entry its journal still owes, so the last procedure's output — flushed after its commit ack — is never torn down under; an entry still owed past the bound stays journaled.

#### Scenario: Teardown is identical across triggers

- **WHEN** a workflow operative terminates by chain completion, `ctx.complete`, loop exit, duration, or external stop
- **THEN** the Controller SHALL run the same teardown and convergence SHALL NOT act on it

#### Scenario: The last output is delivered before the teardown

- **WHEN** a durable chain completes with its final procedure's outbox journaled but not yet flushed
- **THEN** the Controller SHALL flush it to the sink before the instance is deregistered

#### Scenario: Completion carries a result/error payload

- **WHEN** a workflow operative terminates
- **THEN** its `WorkflowCompleted` SHALL include the terminal result (or error) payload and the Controller SHALL make it available to bound consumers and to `flz operative info`

