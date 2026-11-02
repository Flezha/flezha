# steps-relay Specification

## Purpose

The contract for a relay whose `run` is steps: a relay that runs to an exit trigger and finishes. It
is authored with the same tasklets, channels and Cells as a flow relay and is fed through the same
entrance, but its tasklets run sequentially along the graph, a tasklet completing when all its Cells
finish. This capability covers how a steps relay is spawned on demand and registered, the tree or
static DAG its chain may form and the order branches run and complete in, internal loops, a
per-input relay as a group of independent instances, timers as inputs to the run, external stops,
and the one `RelayCompleted` teardown every exit trigger converges on.

## Requirements

### Requirement: A steps relay is authored with a distinct run-once trait

The SDK SHALL expose a `Relay` authoring surface distinct from the flow one, registered through the runner's `register_factory`, making the steps class spawnable by its `factory_id`. A steps relay SHALL declare its tasklets, Channels and Cells in a `channels` body on the same graph builder a stream uses, the body carrying the one name both kinds give it in every SDK; there SHALL be no `tasklets` body and no one-function `run(ctx, args)` form, and a steps class declaring no tasklets SHALL be refused at registration. Registering a steps relay SHALL NOT require the author to enumerate capability flags; choosing the steps run is sufficient, and the runner SHALL announce that kind to the controller with the factory id.

#### Scenario: A registered steps relay is spawnable by factory id

- **WHEN** a steps class is registered with the SDK before the runner starts
- **THEN** its `factory_id` SHALL be resolvable, the runner SHALL announce it with kind `steps`, and an `AddRelay` naming that `factory_id` with the steps run SHALL spawn a steps instance

#### Scenario: A steps relay with no tasklets is refused

- **WHEN** a steps class whose `channels` body declares no tasklet is handed to `register_factory`
- **THEN** registration SHALL fail naming the class and the missing tasklets, and nothing SHALL be declared

#### Scenario: The steps body is the flow body

- **WHEN** a steps class in Rust, Java, Go or Elixir is written with `channels` and the same graph is written in a flow class's `channels`
- **THEN** both SHALL register, the two declarations SHALL differ only in the kind and the steps relay's own settings, and a `tasklets` member SHALL exist on no relay type in any SDK

### Requirement: Starting a relay with the steps run spawns a steps instance

The SDK SHALL spawn a **steps relay** instance when the Controller issues `AddRelay` for a
factory the runner registered as a steps class, and a flow instance for a flow class,
under the named `factory_id`. There SHALL be one instance type for both kinds: a
steps instance is the relay instance a flow relay is (its cells built by the same builder,
registered with the flusher the same way, reachable by the same live-state, restore, compaction
and migration commands) plus a pass sheet carrying the run. There SHALL be one class registry
carrying each class's kind, and one sealed per-runner relay registry whose entry is
resolved once per inbound frame at the routing boundary. Spawning SHALL be idempotent on a
duplicate instance id. A steps instance's bind SHALL create no thread: the sheet is built
from the bind's resume state (empty for a fresh instance) and the first pass starts on the
event that seeds it.

#### Scenario: A steps class spawns a steps instance

- **WHEN** the SDK handles `AddRelay` for a registered steps relay `factory_id`
- **THEN** a steps instance SHALL be created and registered under the instance id, with no thread spawned for it

#### Scenario: A flow class still spawns a flow instance

- **WHEN** the SDK handles `AddRelay` for a registered flow `factory_id`
- **THEN** a flow instance SHALL be created

#### Scenario: Unknown steps factory is reported

- **WHEN** `AddRelay` names a `factory_id` no registered class carries
- **THEN** the SDK SHALL report an error and SHALL NOT create an instance

#### Scenario: A durable steps relay's cells answer a live state read

- **WHEN** `flz cell state <instance>` names a running durable steps instance
- **THEN** the SDK's `CellState` handler SHALL find its cells through the same lookup a flow relay's
  are found by and answer their state in the per-tasklet / per-cell JSON shape, with no
  "Relay instance not found" refusal

#### Scenario: One registry serves both kinds

- **WHEN** a runner registers a flow class and a steps class
- **THEN** both SHALL be held in one class registry, each with its kind, and the factory ids
  advertised on connect SHALL come from that one registry

### Requirement: A steps relay runs to an exit trigger and deregisters terminally

A steps relay SHALL be spawned on demand with `init_values` meaning initial state per tasklet name, exactly as a flow relay's `init_values` do, and SHALL take its input as messages on its entry channels, representing **one job/session**. It SHALL be **structured**: Tasklets, Channels, and Cells on the same per-cell engine as a flow relay, each tasklet run once every producer feeding it has committed and **done when all its cells finish**, then committed without waiting for its siblings (see `steps-relay`). A steps relay SHALL **receive its input after it starts** through the same entrance a flow relay uses, **loop** by its declared loop mode, MAY be bounded by a declared maximum duration, be stopped externally, declare per-tasklet compensation hooks with an author-selected rollback mode (see `steps-compensation`), and, being **durable by default** (opt-out per class), resume from durable state across a crash (see `steps-durability`). It SHALL terminate on the first exit trigger (all tasklets done with no loop active, a tasklet's `ctx.complete`, a loop running out of work, duration, or external stop). On termination, the SDK SHALL emit a `RelayCompleted` control message carrying the terminal result/error payload and a status (`ok | error | cancelled`); the Controller SHALL then drive the **single** standard relay teardown, running compensation first when terminating abnormally, so the steps relay is deregistered, its plugins disabled, and its quota released, terminal in every case, with convergence never acting on it, and its data retained for the finished-job window. A tasklet failing after its retry ladder SHALL be terminal with no automatic platform retry, except in a `per_input` loop declaring keep-progress, where it fails only the turn it ran in: the turn is dead-lettered and undone, and the loop takes its next message (see `steps-durable-loop`).

#### Scenario: Clean completion deregisters the steps relay

- **WHEN** a steps relay reaches an exit trigger with all work successful
- **THEN** the SDK SHALL emit `RelayCompleted` with status `ok` and the result payload, and the Controller SHALL deregister it, disable its plugins, release its quota, and retain its data for the finished-job window

#### Scenario: Failure compensates then terminates with no auto-retry

- **WHEN** a steps relay's tasklet returns an error after partial progress, and the relay is not a `per_input` loop declaring keep-progress
- **THEN** the SDK SHALL fire compensation for completed tasklets in reverse order (per the relay's rollback mode), emit `RelayCompleted` with status `error` and the error payload, and the Controller SHALL tear it down terminally without convergence rebinding it

#### Scenario: External stop is terminal

- **WHEN** a running steps relay is stopped via `flz relay stop`
- **THEN** the SDK SHALL cooperatively cancel in-flight work, run compensation if partial progress was made, emit `RelayCompleted` with status `cancelled`, and the Controller SHALL tear it down terminally

#### Scenario: A tasklet's completion carries the result

- **WHEN** a tasklet of a running steps relay calls `ctx.complete(result)` and its iteration commits
- **THEN** the SDK SHALL emit `RelayCompleted` with status `ok` and that result, and `flz relay info` SHALL show it

#### Scenario: Init values seed a steps tasklet's state

- **WHEN** a steps relay whose tasklet `reserve_funds` holds state is started with `init_values` `{"reserve_funds": {"budget": 100}}`
- **THEN** that tasklet's Cell SHALL start with the state `{"budget": 100}` before the first message is read, and no entry channel SHALL receive the document

#### Scenario: A per-input keep-progress turn's failure is not terminal

- **WHEN** a tasklet of a `per_input` keep-progress relay fails after its retry ladder
- **THEN** the SDK SHALL NOT emit `RelayCompleted`, the turn SHALL be dead-lettered and
  undone, and the relay SHALL stay registered and take its next message

### Requirement: A steps relay is fed by messages on its entry channels in every loop mode

A steps relay's input SHALL be the messages sent to it: a tasklet's `ctx.spawn_steps` payload, `flz relay send`, a schedule's stored first message and `flz relay start --text`. Every loop mode SHALL read them: `ONCE` SHALL take exactly one message, run one pass and end; `PER_INPUT` SHALL take one message per pass; `CHAINED` SHALL take the first message, then seed each further pass from the previous pass's terminal handoff into the chain the first message started. An instance with no message to read SHALL wait for one and SHALL NOT end: a durable instance SHALL park after the idle window as a `per_input` loop does and the next message SHALL wake it, and an opted-out instance SHALL wait until a message or a stop arrives. A `max_duration` SHALL still end a waiting instance. A second message to a `ONCE` instance, and a message on another chain's channel while a `CHAINED` loop runs, SHALL be accepted and never read by that instance; no refusal or guard SHALL be built for either.

#### Scenario: A spawned once steps runs on its payload

- **WHEN** a tasklet calls `ctx.spawn_steps("paper_order", ticket)` and `paper_order` is a `once` steps relay whose entry channel `order` decodes the ticket's type
- **THEN** the spawned instance's first tasklet SHALL run over the decoded ticket, the pass SHALL complete and the instance SHALL end with status `ok`

#### Scenario: A once steps relay started without a message waits for it

- **WHEN** `flz relay start paper_order --kind steps` is run with no `--text` and `flz relay send <id> --text '<ticket>'` follows
- **THEN** the instance SHALL run its one pass over the sent ticket, and a durable instance that had parked in between SHALL be woken by the send

#### Scenario: A chained loop feeds the chain its first message picked

- **WHEN** a `chained` steps relay with entry channels `a` and `b`, each feeding its own chain, receives its first message on `b`
- **THEN** pass 0 SHALL run `b`'s chain over the message, every later pass SHALL feed the previous terminal batch into `b`'s chain, and `a`'s chain SHALL never run in that instance

#### Scenario: A second message to a once instance is not read

- **WHEN** two messages are sent to a `once` instance before it ends
- **THEN** the instance SHALL run its one pass over the first, the second SHALL stay unread, and no refusal SHALL be answered to the sender

### Requirement: A timer firing is an input to the run beside a message, and pending timers end with the instance

A steps instance SHALL take two kinds of trigger: a message on one of its entry channels, which starts that channel's chain, and a Cell timer's firing, which starts the chain of the tasklet that set it. Both SHALL obey the one-pass-at-a-time rule and the loop mode: a `PER_INPUT` instance SHALL run one pass per trigger in arrival order; a `CHAINED` instance SHALL serve a queued firing before its carry; a `ONCE` instance SHALL end on its one pass, so a firing that comes due after that pass's last stage has committed SHALL never run. When an instance ends, on any terminal outcome, its Cells' pending timers and any firing still queued SHALL be dropped with the Cells, exactly as a flow relay's are on removal; the runner SHALL log one debug-level line naming the instance and the count dropped, and the completion record SHALL be unchanged.

#### Scenario: A firing in a per_input loop is one iteration

- **WHEN** a `per_input` instance has consumed two messages and a timer set in the second fires
- **THEN** iteration 2 SHALL run from the tasklet that set it, `iter_committed(2)` SHALL be journaled, and the instance SHALL wait for the next trigger

#### Scenario: A once instance ends with its timers

- **WHEN** a `once` instance's tasklet registers a timer 30 s out and the pass completes in under a second
- **THEN** the instance SHALL end with status `ok` at its last stage's commit, the timer SHALL be dropped with the Cells, `on_timer` SHALL never run, and the completion record SHALL carry nothing about the timer

#### Scenario: A stop drops the timers with the cells

- **WHEN** a `per_input` instance with pending timers is stopped and its compensation has run
- **THEN** the instance SHALL end `cancelled`, and no timer of it SHALL fire afterwards

### Requirement: A per-input steps relay SHALL run as a partition group of independent instances

A steps relay whose loop mode is `per_input` SHALL be startable with `--partitions N`,
and the group SHALL be N ordinary steps instances `<id>#p0` .. `<id>#p{N-1}`, each with its own
key-group range, journal, durable input log, pass sheet and `max_duration` clock, started,
converged, read and rescaled through the same group module a flow relay's group is, with no group
mechanism of the steps relay's own. A message SHALL reach its member by key as a flow relay's does, and
the SDK's key-group guard SHALL apply to a steps relay member unchanged. Each member SHALL end and
compensate itself only: a member's exit trigger (`ctx.complete` in one of its passes, its
`max_duration`, a stop, a terminal failure) SHALL end that member alone, emitting its own
`RelayCompleted`, driving its own teardown and recording its own outcome under its member id
(a completion record when its class keeps finished state, the relay history's `ended` record
always), while the other members run on; a member's rollback, in either mode, SHALL undo that
member's passes and no other member's. There SHALL be no group-wide end, no group `complete`
result and no group rollback. The member count SHALL change only between passes, when every
member is quiet, and a rescale SHALL be a history boundary for each surviving member. A `once`
or `chained` steps relay SHALL be refused `--partitions` with a reason naming that its one message
would reach one member while the others waited for nothing.

#### Scenario: Messages by key reach their member and each member commits its own passes

- **WHEN** a `per_input` steps relay started with `--partitions 2` receives three messages whose
  keys hash into `#p0`'s range and two whose keys hash into `#p1`'s, through a keyed forward
- **THEN** `#p0` commits three iterations and `#p1` two, each in its own journal, each member's
  Cells hold only its own keys, and neither member's journal names the other's Cells

#### Scenario: One member ends itself and the other runs on

- **WHEN** a tasklet of `orders#p1` calls `ctx.complete(result)` and the iteration commits
- **THEN** `orders#p1` emits `RelayCompleted` with status `ok` and `result`, is torn down,
  its outcome is read under `orders#p1`, and `orders#p0` receives and runs its next message
  unaffected

#### Scenario: One member fails and compensates itself only

- **WHEN** a pass of `orders#p0` fails terminally after partial progress
- **THEN** `orders#p0` compensates its own completed tasklets per its rollback mode and ends with
  status `error`, no tasklet of `orders#p1` is compensated, and `orders#p1` keeps running

#### Scenario: A once steps relay is refused partitions

- **WHEN** `flz relay start <factory> --partitions 2` names a `once` steps relay
- **THEN** the start is refused with `topology.partitions_without_per_input_loop`, naming that one message
  reaches one member, and no instance runs

### Requirement: A steps relay is structured but runs its tasklets sequentially

A steps relay SHALL be authorable with the same Tasklet, Channel, and Cell surface as a flow relay, on the same per-cell state engine. The kind difference SHALL be **scheduling**: a steps relay's tasklets SHALL run sequentially **along each path** of a static **tree-shaped chain** — the linear chain (`Op1 → Op2 → … → TaskN`) as the single-branch case, and fan-out into independent branches per the `steps-relay` capability (fan-in remains rejected) — where each tasklet's per-channel output is its downstream tasklet's input and a child runs only after its parent commits. **Within a single tasklet, cells SHALL run in parallel** exactly as in a flow relay (per-cell mailbox, worker, and state). The identity of a steps relay's durable state SHALL use the same `{relay_class, tasklet_id, cell_key}` form — no synthetic tasklet/cell placeholders.

#### Scenario: A steps relay declares tasklets and cells

- **WHEN** a steps relay is authored with multiple Tasklets chained by Channels and a Cell key on a stateful Tasklet
- **THEN** each Tasklet SHALL run on the same per-cell engine as a flow relay, with per-Cell state behaving identically, but the Tasklets SHALL execute in topological order along their path rather than concurrently with their ancestors

#### Scenario: Cells within one tasklet run in parallel

- **WHEN** a Tasklet in a steps relay fans its input across multiple cell keys
- **THEN** those cells SHALL be processed in parallel, each with its own state, and the Tasklet SHALL be considered finished only when every one of its cells has finished

#### Scenario: Steps state identity has no sentinels

- **WHEN** a durable steps relay writes Cell state
- **THEN** the on-disk/on-wire identity SHALL be `{relay_class, tasklet_id, cell_key}` with real tasklet and cell values, not reserved placeholders

### Requirement: A tasklet completes when all its cells finish

A steps relay SHALL advance from one tasklet to the next via a **barrier**: tasklet `Task(i+1)` SHALL NOT begin until tasklet `Task(i)` is **complete**, defined as every item of `Task(i)`'s input having been processed across all of its cells, with no cell mid-run and no pending work. The previous tasklet's collected output SHALL be released to the next tasklet only at this barrier. The first tasklet's input SHALL be seeded from the spawn `args` (`init_values`).

#### Scenario: The next tasklet waits for the barrier

- **WHEN** tasklet A in a steps relay fans work across cells and tasklet B consumes A's output
- **THEN** B SHALL NOT start processing until all of A's cells have finished, and B SHALL then run on A's complete output

#### Scenario: Completion is countable, not a timed guess

- **WHEN** a tasklet processes a finite, known batch of items across its cells
- **THEN** "tasklet complete" SHALL be determined by all dispatched items having been processed (a count), not by an idle-timeout heuristic

### Requirement: A keyed edge into a steps tasklet runs one Cell per key

A steps tasklet reached by a keyed consumer edge SHALL run in dynamic cell mode exactly as
a flow tasklet reached by `key_by` / `keyBy` does: registration SHALL accept the edge in
every SDK, the instance SHALL own no Cell for the tasklet before a pass,
and a Cell SHALL be created on the first item of a pass whose key has no live Cell, by the
same routing that delivers a flow relay's items, at the moment the item is addressed to the Cell.
A tasklet's `max_cells` SHALL bind as it binds on a flow relay: an item whose key would mint a
Cell past the cap SHALL be dropped and counted, and the pass SHALL run without it, a Cell that
never ran being recorded nowhere. A stage SHALL be complete when every item it addressed has
been processed by the Cell it was addressed to, whether that Cell was declared or minted, and
its commit SHALL record the Cells that ran by their `{relay_class, tasklet_id, cell_key}`
identity, never by a position in a declared list.

A keyed Cell of a steps relay SHALL be evicted by its tasklet's idle TTL only while no pass of
the instance runs, and never while it is named by a completed stage that the instance's
rollback mode would still undo and whose tasklet declares a compensation hook: under
all-or-nothing every such Cell of every committed iteration SHALL be kept until the instance
ends; under keep-progress a committed iteration's Cells SHALL be free once the next pass has
not started. A flow relay's eviction rules are unchanged.

#### Scenario: Keys mint Cells during a pass

- **WHEN** a steps relay whose second tasklet is reached by `.key_by(f)` starts a pass whose
  first tasklet releases values keyed `a`, `b` and `c`
- **THEN** three Cells of the second tasklet exist by the time the stage runs, each processes
  its own values, the stage commits once all three have processed theirs, and the commit
  names the three identities

#### Scenario: The cap drops a new key and the pass completes without it

- **WHEN** the keyed tasklet declares `max_cells = 2` and a pass releases three distinct keys
- **THEN** two Cells are minted, the third key's items are dropped and counted as capped, the
  stage commits naming two Cells, and the instance completes

#### Scenario: A Cell is not evicted mid-pass

- **WHEN** a keyed steps relay Cell with a one-second idle TTL finished its items and the stage
  is still waiting on its commit acknowledgement for longer than the TTL
- **THEN** the sweep leaves the Cell in place, and it is swept only once the instance is
  waiting for its next message

#### Scenario: All-or-nothing keeps a compensable Cell across passes

- **WHEN** an all-or-nothing `per_input` steps relay committed an iteration in which a keyed
  compensable tasklet ran for key `a`, and the instance is then idle past the TTL
- **THEN** `a`'s Cell is kept, and a later failure compensates it, while the same class under
  keep-progress evicts `a`'s Cell on that sweep

#### Scenario: Every SDK accepts the edge

- **WHEN** a steps class in any SDK declares `input(...).into(Split).key_by(f)
  .into(Keyed)` with `Keyed` declaring no static keys
- **THEN** registration succeeds, the declaration reaching the controller carries `Keyed` in
  dynamic mode, and no SDK reports the tasklet as refused for its mode

### Requirement: A steps relay is spawned on demand and terminates on an exit trigger

A steps relay SHALL be spawned on demand with `init_values` as initial state per tasklet and SHALL take its input as messages on its entry channels, each spawn independent and representing **one job/session** (cells are internal parallelism, never multiple sessions). It SHALL terminate on the first of these exit triggers: (a) **all tasklets done**: its chain of tasklets has run to the final tasklet's completion with no loop active; (b) a tasklet calling `ctx.complete(result)`, which ends the instance once the iteration it ran in has committed; (c) a loop running out of work (a chained loop releasing an empty batch); (d) a declared maximum wall-clock duration elapsing; (e) an external `flz relay stop`. Waiting for a message SHALL NOT be an exit trigger. A flow relay SHALL never reach a terminal trigger, and a tasklet of a flow relay calling `ctx.complete` SHALL fail that input with an error naming `runtime.complete_outside_steps`.

#### Scenario: The chain running to its end terminates the relay

- **WHEN** a steps relay's last tasklet completes (all its cells finished) and no loop is active
- **THEN** the relay SHALL proceed to terminal teardown with status `ok`

#### Scenario: A tasklet's completion terminates the relay

- **WHEN** a tasklet of a steps relay calls `ctx.complete(result)`
- **THEN** the rest of its iteration SHALL run and commit, the relay SHALL accept no further input, tear down terminally, and report `result` in its completion

#### Scenario: A tasklet of a flow relay cannot complete it

- **WHEN** a tasklet of a flow relay calls `ctx.complete(result)`
- **THEN** that input SHALL fail with an error naming `runtime.complete_outside_steps`, handled as any tasklet error, and the flow relay SHALL keep running

#### Scenario: Duration elapse terminates the relay

- **WHEN** a steps relay declares `max_duration` and is still running when it elapses
- **THEN** the SDK SHALL cooperatively cancel in-flight work and tear the relay down

#### Scenario: Waiting for a message does not end the instance

- **WHEN** a `once` steps instance is started without a message and none arrives within the idle window
- **THEN** it SHALL NOT complete; a durable instance SHALL park and an opted-out one SHALL keep waiting, until a message, a stop or its `max_duration`

### Requirement: Internal loop with an intrinsic terminal condition

A steps relay SHALL loop by declaring its loop mode (`PerInput`, one iteration per message, or `Chained`, each iteration seeded by the previous one's terminal output into the chain the first message started), never by a loop written inside a tasklet body. Waiting for the next message SHALL happen at a committed iteration boundary, observe the cancellation token (duration or external stop), and block or park while idle. A loop SHALL have at least one **non-timeout** exit: a tasklet's `ctx.complete`, a chained loop's empty batch, or external stop. Authoring a steps relay whose **only** possible exit is external cancellation SHALL be rejected at registration with a clear error directing the author to add a terminal condition. A declared `max_duration` MAY additionally bound any loop as a backstop but SHALL NOT be required when a non-timeout exit exists.

#### Scenario: A loop ends on a tasklet's completion

- **WHEN** a `PerInput` steps relay's tasklet calls `ctx.complete(result)` while handling a message
- **THEN** the loop SHALL NOT wait for another message after that iteration commits, and the relay SHALL proceed toward terminal teardown with status `ok`

#### Scenario: Loop exits on cancellation

- **WHEN** a steps relay is looping and the duration elapses or an external stop arrives
- **THEN** the loop SHALL observe the cancellation token and stop iterating

#### Scenario: External-stop-only loop authoring is rejected

- **WHEN** a steps relay declares a loop and marks itself as having no terminal condition and no duration bound, so its only exit is an external kill
- **THEN** registration SHALL reject it, naming the missing terminal condition

### Requirement: A steps relay can be stopped externally

`flz relay stop <id>` (and the equivalent controller-issued path) SHALL **cooperatively** cancel a steps relay (distinct from a hard removal) and drive the standard terminal teardown with completion status `cancelled`, whether the instance is awake or asleep. Before it acts, the stop SHALL record on the instance who requested it (principal and source), so the instance's end is told apart from one its `max_duration` caused. A sleeping (suspended) instance SHALL first be woken through the ordinary bind on a live runner, with the stop carried by that wake so `StepsStop` reaches the runner as soon as the binding is published; the manager SHALL answer `{:ok, :cooperative}` once the wake is under way. The woken instance, waiting at a committed boundary with nothing owed, SHALL end `cancelled` with nothing to roll back, exactly as an awake instance stopped at the same boundary; a message that starts a pass before the stop lands SHALL be cancelled with that pass's compensation. Its state directory and completion record SHALL be kept or deleted as its class's `keep_finished` resolves (see `durability-cell-state`). A halted instance SHALL take the hard removal at once, because no runner runs it and no completion would answer a cooperative stop. Only when no runner can take the wake, or an awake instance's runner cannot be reached, SHALL the stop fall back to the hard removal, and it SHALL do so the same way for both. A flow relay's stop SHALL remain a hard removal, awake or asleep.

#### Scenario: User stops a running steps relay

- **WHEN** the user runs `flz relay stop <id>` for a running steps relay
- **THEN** the SDK SHALL cancel in-flight work, run compensation if partial progress was made, emit completion with status `cancelled`, and the Controller SHALL tear it down terminally

#### Scenario: User stops a sleeping steps relay

- **WHEN** the user runs `flz relay stop <id>` for a durable loop that is suspended and a runner is available
- **THEN** the instance SHALL be rehydrated onto that runner, `StepsStop` SHALL follow the bind, the instance SHALL end with status `cancelled`, its state directory SHALL be kept only when its class keeps finished state, and the stop SHALL answer `{:ok, :cooperative}`, exactly as for the same loop stopped awake at its boundary

#### Scenario: A sleeping instance with no runner to wake on is removed hard

- **WHEN** the user stops a suspended steps instance while no runner in its namespace can take it
- **THEN** the stop SHALL take the hard removal, as it does for an awake instance whose runner cannot be reached, and SHALL NOT leave the instance suspended

#### Scenario: A halted steps instance is removed hard

- **WHEN** the user stops a steps instance that is halted while its runner stays connected
- **THEN** the stop SHALL take the hard removal at once and the instance SHALL end, rather than wait for a `cancelled` completion its runner will never send

#### Scenario: A stop records who asked for it

- **WHEN** principal `alice` stops a running steps instance and it ends `cancelled`
- **THEN** the instance's end SHALL be attributed to a requested stop by `alice`, not to its time limit

#### Scenario: A sleeping flow relay is removed hard

- **WHEN** the user runs `flz relay stop <id>` for a flow instance that is suspended
- **THEN** the instance SHALL be removed and its state directory deleted, as for an awake flow relay, with no runner woken

### Requirement: All exit triggers converge on one terminal teardown

Every exit trigger SHALL drive the single existing `RelayCompleted` teardown, in order: cancel in-flight work → run compensation if terminating abnormally (see `steps-compensation`) → emit completion (status `ok | error | cancelled` plus result/error payload) → hand the terminal outcome to the instance's Overwatch with the teardown's cause → deregister, disable plugins, release quota, and keep or delete the state directory as the class's `keep_finished` resolves (see `durability-cell-state`). There SHALL NOT be a second teardown codepath, and convergence SHALL never restart or rebind a terminal steps relay. A `RelayCompleted` from a runner whose stable identity the instance's ownership record does not name is not an exit trigger: it SHALL write no completion record, hand no outcome to the Overwatch and drive no teardown, and SHALL be refused as `runner-identity-and-session` prescribes. Before a durable instance is deregistered, the teardown SHALL re-drive and wait (steps) for every outbox entry its journal still owes, so the last tasklet's output — flushed after its commit ack — is never torn down under; an entry still owed past the bound stays journaled. An opted-out instance's outputs travel the data stream while its completion travels the control stream; before deregistering it the teardown SHALL flush the runner's stream channels synchronously, so every output frame the transport has already delivered is routed and run first, and it SHALL NOT hold the teardown against a count of outputs the runner reports having written.

#### Scenario: Teardown is identical across triggers

- **WHEN** a steps relay terminates by chain completion, `ctx.complete`, loop exit, duration, or external stop
- **THEN** the Controller SHALL run the same teardown and convergence SHALL NOT act on it

#### Scenario: The last output is delivered before the teardown

- **WHEN** a durable chain completes with its final tasklet's outbox journaled but not yet flushed
- **THEN** the Controller SHALL flush it to the output before the instance is deregistered

#### Scenario: An opted-out instance's last output is delivered before the teardown

- **WHEN** an opted-out chain writes its final output to the data stream and completes immediately after
- **THEN** the Controller SHALL run that output's plugin chain before the instance is deregistered, however the two frames interleave on arrival

#### Scenario: A teardown ends without waiting out a bound for an output that cannot arrive

- **WHEN** an opted-out instance completes after an output of its run was lost rather than delivered
- **THEN** the teardown SHALL still complete promptly once the stream channels report themselves flushed and the journal owes nothing, rather than waiting out a bound that only expiry can end

#### Scenario: An output that beats the stream's routing is still delivered

- **WHEN** a chain emits within its first milliseconds, before the owning session's stream channel holds a route for it
- **THEN** the stream SHALL route the frame through the Overwatch the ownership record names, which the class registered before its SDK start, and cache the route; a frame for a relay of the session's namespace neither the channel nor the record knows SHALL be counted as a drop, never discarded silently

#### Scenario: An output for a replaced Overwatch is still delivered

- **WHEN** a relay's Overwatch is replaced — by a crash-resume, a rebind, or a supervisor restart — while its runner is still writing that relay's outputs to the data stream
- **THEN** the stream SHALL route those frames to the live Overwatch rather than to the replaced one, and SHALL NOT deliver a frame to a process that is no longer alive

#### Scenario: Completion carries a result/error payload

- **WHEN** a steps relay terminates
- **THEN** its `RelayCompleted` SHALL include the terminal result (or error) payload, the Controller SHALL make it available to bound consumers, and `flz relay info` SHALL report it while the instance's state is kept and report the status and error from the relay history once it is not

#### Scenario: A completion from a runner that does not own the instance tears nothing down

- **GIVEN** a steps instance bound to runner A's stable identity and a second session B in the namespace
- **WHEN** B emits `RelayCompleted` naming the instance
- **THEN** the instance SHALL keep running on A, no completion record SHALL be written, `flz relay info` SHALL not report it finished, and B SHALL be sent `RemRelay` for it once

### Requirement: A steps relay receives its input through the flow relay's entrance

A steps relay SHALL declare one or more entry channels and SHALL receive every message through the same entrance a flow relay's frames take: the channel the message names SHALL be resolved among the declared entry channels, a message naming none SHALL take the first declared entry channel, a message naming a channel the relay does not declare SHALL be parked under that name rather than dropped or rerouted, the entry channel's parser (`input_with(name, parser)` or its codec) SHALL run, and the entry edge's `map`, `filter` and `flat_map` SHALL run before the first tasklet sees the value. A message the entrance cannot read SHALL ride the entry channel's retry ladder and then be parked to the dead-letter queue under the channel's identity, and SHALL NOT count as a message: a `PER_INPUT` loop SHALL take the next one and a `ONCE` or `CHAINED` instance SHALL keep waiting for its message; the run SHALL NOT end or compensate for it, in any loop mode. For a **durable** steps instance, every message SHALL additionally be journaled to the instance's durable inbox before delivery, so a message sent while the instance is suspended, crashed, or migrating SHALL NOT be lost and SHALL be delivered on rehydrate. A relay waiting for a message SHALL block or sleep while idle and SHALL NOT busy-spin; a **durable** relay waiting at a committed iteration boundary, the boundary before its first pass included, SHALL be suspended (evicted from runner memory) per the `steps-durability` capability instead of holding live resources.

#### Scenario: A message is delivered through the entry channel it names

- **WHEN** a steps relay declaring entry channels `orders` and `cancels` receives a frame with `channel = "cancels"`
- **THEN** the frame SHALL be parsed by `cancels`'s parser, mapped by `cancels`'s edge transforms and delivered to `cancels`'s consumer tasklets, and `orders`'s chain SHALL NOT run in that pass

#### Scenario: A message naming no channel takes the first entry channel

- **WHEN** a spawn payload with no `channel` metadata reaches a steps relay declaring `orders` first
- **THEN** it SHALL enter `orders`

#### Scenario: A message naming an undeclared channel is parked

- **WHEN** a frame with `channel = "refunds"` reaches a steps relay that declares no such entry channel
- **THEN** the payload SHALL be parked under `refunds` with the declared entry set in its error, no tasklet SHALL run, and the instance SHALL keep waiting

#### Scenario: An unreadable message rides the ladder and does not count

- **WHEN** a `once` steps relay whose entry channel decodes JSON receives a message that is not JSON
- **THEN** the message SHALL ride the entry channel's retry ladder and be parked to the dead-letter queue, the instance SHALL NOT complete or compensate, and the next readable message SHALL run its pass

#### Scenario: An unreadable message on a host SDK is told apart from a tasklet failure

- **WHEN** a Java, Node, Python, Ruby, PHP or Go steps relay's entry decoder rejects a message
- **THEN** the SDK SHALL report the activation as unreadable, the core SHALL park the message to the dead-letter queue under the entry channel after the ladder, and the run SHALL NOT end, whereas a readable message on which the tasklet throws SHALL end the run as a tasklet failure does

#### Scenario: Input sent while the instance is down is not lost

- **WHEN** a data frame is sent to a durable steps instance that is suspended or crashed
- **THEN** the frame SHALL be durably journaled and delivered when the instance rehydrates

#### Scenario: An idle instance does not busy-spin

- **WHEN** a steps relay is waiting for the next message with none available
- **THEN** it SHALL sleep/block until a message arrives or a cancellation (duration/external stop) trips, without consuming CPU in a spin loop, and a durable relay at a committed iteration boundary SHALL be evicted from memory rather than occupying the runner

### Requirement: Branches run after their parent commits; the relay completes when all leaves commit

A child tasklet SHALL become runnable only after **every** parent tasklet
feeding it has had its commit acknowledged — for a single-parent child this is
the existing rule unchanged; a join node waits for all of its producers.
Sibling branches SHALL be independent and MAY interleave. Each tasklet SHALL
keep the existing per-task barrier, commit gating, staged outbox, and
cell-retry semantics unchanged. The relay SHALL terminate with status
`ok` exactly when every **leaf** tasklet has committed.

#### Scenario: Parent commit releases both branches

- **WHEN** tasklet A commits in an A→(B,C) tree
- **THEN** B and C SHALL both become runnable, each proceeding through its own barrier and commit independently

#### Scenario: A join waits for every producer

- **WHEN** in an A→(B,C)→D graph, B has committed but C has not
- **THEN** D SHALL NOT be dispatched; it SHALL become runnable only once C's commit is also acknowledged, and SHALL then run exactly once

#### Scenario: DONE requires every leaf

- **WHEN** leaf B has committed but leaf C has not
- **THEN** the relay SHALL NOT be complete; it SHALL complete only once C also commits

### Requirement: Compensation over a tree is reverse topological

On abnormal termination, completed tasklets SHALL be compensated in reverse
topological order over the DAG: a tasklet SHALL NOT be compensated before
every compensated descendant of it has been compensated — in particular a join
node SHALL be compensated before any of its producers — and the relative order
among independent sibling subtrees is unspecified. For a linear chain this
SHALL degenerate to the existing exact reverse order. Per-cell compensation
semantics and the intent/done marker machinery SHALL be unchanged.

#### Scenario: Parent is undone last

- **WHEN** an A→(B,C) tree fails after A, B, and C completed
- **THEN** A SHALL be compensated only after both B and C have been compensated

#### Scenario: A join is undone before its producers

- **WHEN** an A→(B,C)→D graph fails after all four completed
- **THEN** D SHALL be compensated before either B or C, and A SHALL be compensated last

#### Scenario: Linear chain order is preserved

- **WHEN** a linear chain A→B→C fails after all three completed
- **THEN** compensation SHALL run C, then B, then A — identical to pre-change behavior

### Requirement: Crash-resume re-runs exactly the uncommitted subtrees

On respawn, the driver SHALL derive committed nodes from the journal and SHALL
schedule only the uncommitted subgraphs, seeding each uncommitted node from
its parents' journaled per-edge handoffs. A join node with one committed and
one uncommitted producer SHALL run only after the uncommitted producer
completes on the resumed run, with an input batch accumulating the committed
producer's **journaled** handoff and the resumed producer's fresh output.
Committed branches SHALL NOT re-run.

#### Scenario: One committed branch survives a crash

- **WHEN** in an A→(B,C) tree the journal holds `task_committed` for A and B when the runner crashes
- **THEN** on respawn only C (seeded from A's journaled handoff on the channel C consumes) SHALL run; A and B SHALL NOT re-run

#### Scenario: A join resumes with one journaled and one fresh contribution

- **WHEN** in an A→(B,C)→D graph the journal holds `task_committed` for A and B when the runner crashes
- **THEN** on respawn C SHALL re-run and D SHALL then run exactly once with an input batch containing B's journaled handoff and C's fresh output; B SHALL NOT re-run

### Requirement: A handoff-chained loop requires a single terminal tasklet

A class declaring a handoff-chained loop over a branching tree SHALL be
rejected at registration: a chained loop seeds iteration N+1 from iteration N's
terminal batch, which a tree with several leaves does not define. Classes whose
loop is per-input, or which do not loop at all, MAY branch freely.

#### Scenario: A branching chained loop is refused

- **WHEN** a class declares `Each::Chained` over an A→(B,C) tree
- **THEN** registration SHALL fail with an error naming the branching chained loop

### Requirement: A steps chain may fan out and join into a static DAG

Registration SHALL accept a steps tasklet topology shaped as a static
acyclic graph (DAG). An **edge** is a tasklet's output channel paired with
one of that channel's consumers, so a tasklet MAY feed several downstream
tasklets by declaring more than one consumer on the channel it emits on
(fan-out), and a tasklet MAY consume more than one incoming channel
(fan-in / join). Handoff routing SHALL be per-channel: a downstream tasklet
receives exactly the values released on the channels it consumes, and a join
node's input batch SHALL **accumulate** every producer's released output — no
producer's contribution may be overwritten or dropped.

A channel's consumer count SHALL NOT change what its producer releases. One
emit SHALL release one value on the producing channel whether that channel has
one consumer or several; the fan-out happens once, where the driver hands the
released batch to each child. A topology SHALL therefore produce the same
per-tasklet invocation count, the same journaled handoff and the same plugin
output whatever its fan-out width, in every SDK.

A tasklet declaring more than one **outgoing** channel SHALL be rejected at
registration with an error naming the node (the emit surface carries one
output channel per tasklet, so a second one could never be routed), as SHALL
a cycle (with an error identifying a node on it). Validation SHALL order
tasklets topologically — every producer of a node precedes it — replacing
the former BFS-from-entry walk. A linear chain SHALL remain valid as the
single-branch case with unchanged behavior.

#### Scenario: Fan-out registers and routes per channel

- **WHEN** tasklet A emits on a channel declaring both B and C as consumers
- **THEN** registration SHALL succeed and both B and C SHALL receive the values A released on that channel

#### Scenario: Join registers and accumulates

- **WHEN** a class declares tasklets B and C each feeding tasklet D on its own channel
- **THEN** registration SHALL succeed in both SDKs, and D's input batch SHALL contain both B's and C's released outputs

#### Scenario: Cycle is rejected

- **WHEN** a class declares a topology in which a tasklet's output reaches itself transitively
- **THEN** registration SHALL fail with an error identifying the cycle

#### Scenario: Fan-out does not multiply the released batch

- **WHEN** the root of a diamond A→(B1→B2, C)→Join emits once on a channel with two consumers
- **THEN** its journaled handoff SHALL hold ONE value, each of B1, C, B2 and the join SHALL run once per contribution, and the output SHALL receive exactly the five payloads a single-consumer chain would produce — on every SDK that carries the `steps` capability
