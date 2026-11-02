# steps-durability Specification

## Purpose

How a steps relay survives a crash and costs nothing while it waits. Durability is on by default and
opt-out per class. Each durable instance keeps an append-only task journal on the shared state
store: at every task barrier the SDK commits the tasklet and its handoff through a gated handshake,
and a resumed instance skips the journaled tasklets and re-seeds the first uncommitted one from the
journaled handoff. Post-start input is journaled in the durable input log before it is delivered, so
nothing sent to a suspended, crashed or moving instance is lost; an idle instance sleeps after its
class's `sleep_after`, is evicted from memory, and is rehydrated by new input or its durable timer.

## Requirements

### Requirement: WAL persistence for a steps relay is durable by default, opt-out per class

A steps relay class SHALL be **durable by default**; an author MAY **opt out** per class (fire-and-forget) through the same class setting a flow class carries, `durable: false`, announced by the runner in the `AddRelay` response and recorded by the controller with the class's other declarations, with no configuration override. When durable, its tasklets/cells SHALL be WAL-backed on the **same substrate** (WalWriter framing, snapshots, fsync batching, restore) and the **same `{relay_class, tasklet_id, cell_key}` identity** as a flow relay's, with no parallel persistence machinery, and each invocation SHALL be one effect record carrying its state changes and its plugin outputs together, the record a flow Cell on the effectively-once tier writes; its outputs SHALL leave through that Cell's outbox once the stage naming the record has committed. An opted-out class SHALL produce no WAL, snapshot, journal or input log on the controller, SHALL ship no state frames from the runner, SHALL be given no restore on a rebind, and SHALL NOT sleep, having no `sleep_after` to declare, and a `once` or `per_input` instance of it waits in memory for its message or a stop. An opted-out class SHALL send its plugin outputs straight to the wire with their channel, key and metadata, as an opted-out flow relay does.

#### Scenario: Durable steps relay is WAL-backed by default

- **WHEN** a steps relay class does not opt out of persistence and writes Cell state
- **THEN** its Cell state changes SHALL be appended to the WAL on the shared substrate exactly as a flow relay's are, inside effect records that also carry the invocation's outputs

#### Scenario: Opted-out steps relay writes nothing to disk

- **WHEN** a steps relay class opts out of persistence and runs to completion
- **THEN** no WAL, snapshot, journal, input log or state files SHALL be created for it, in any SDK

#### Scenario: The opt-out is honoured on both sides

- **WHEN** a steps class opts out of persistence in Java, Node, Python, Ruby, PHP, Go, Rust or Elixir and an instance of it is rebound after its runner died
- **THEN** the runner SHALL have shipped no `cell_state_change` frame for it, the controller SHALL ship no `RestoreInitState` and no `task_journal`, and every tasklet SHALL re-run from the top

#### Scenario: An opted-out steps relay's output keeps its identity

- **WHEN** a steps relay class that opted out of persistence emits with a key and metadata on a keyed output
- **THEN** the output receives the frame with `channel`, `flz.key` and the metadata, exactly as from an opted-out flow relay

### Requirement: A durable steps relay resumes from the WAL on crash, at-least-once

When a durable steps relay's runner or controller restarts mid-run, the relay SHALL be respawned and its Tasklet/Cell state restored from the WAL via the bind-time restore path, then execution SHALL **resume at the current tasklet/iteration** rather than restarting from the beginning; the controller-side **task-journal** decides redo-vs-skip — tasklets recorded `task_committed` SHALL be skipped and the first uncommitted tasklet SHALL be seeded from the journaled handoff batch. The Cells restored SHALL be the persisted Cell set the controller enumerates on disk for the instance, a Cell a keyed edge created included: the controller SHALL count every such Cell into `AddRelay.resume_cell_count`, and the runner SHALL materialise a keyed Cell the restore names before folding its state into it, so the resumed pass finds it as the crashed one left it. The restore gate SHALL count restore frames, not declared Cells: a frame that finds no Cell, or names a keyed Cell the tasklet's cap refuses, SHALL count as landed, so the gate never waits on it. A journaled `task_committed` entry SHALL be read back onto the Cells it names by identity, a keyed identity being resolved on the instance the same way a restore resolves it. The bind SHALL re-derive, on each Cell writer, which records are committed from the ranges the live `task_committed` and `compensated` entries name for that Cell, so the outputs of a committed stage that a crash left undelivered are re-driven and the outputs of an uncommitted one are dropped; the bind SHALL start the writer of every persisted Cell it restores, since a Cell the resumed run never writes to again still owes what its committed records hold. An instance with no journal (opted-out class, or created before the journal existed) SHALL re-run from the top. Execution SHALL therefore be **at-least-once** — a step whose completion was not journaled before the crash MAY run again on resume — so steps and compensations SHALL be idempotent by author contract; a re-run's records are newer than the interrupted attempt's and supersede its outputs. A cell error inside a structured tasklet SHALL first pass through the per-cell retry ladder (`steps-durable-loop`); an error that is non-retryable or has exhausted the ladder SHALL be terminal — the relay compensates and terminates with status `error`, with no further platform re-run of the relay (author loops own their own semantic retry); this is distinct from crash-resume. A crash during a retry ladder SHALL converge on normal crash-resume of the uncommitted tasklet (the in-memory attempt counter resets).

#### Scenario: Crash mid-run resumes from durable state

- **WHEN** a durable steps relay has committed Cell state and journaled `task_committed` entries for several completed tasklets, then its runner crashes and it is respawned
- **THEN** it SHALL restore the committed state, skip the journaled tasklets, and continue from the first uncommitted tasklet seeded with the journaled handoff — not from the beginning

#### Scenario: Crash mid-run restores the keyed Cells

- **WHEN** a durable steps relay whose keyed tasklet committed state for keys `a`, `b` and `c` crashes while a later tasklet blocks, and it is respawned
- **THEN** `resume_cell_count` counts the three keyed Cells with the rest, three restore frames materialise and fold them before the first tasklet of the resumed pass runs, each keyed Cell's WAL holds exactly its pre-crash records afterwards, and the instance completes

#### Scenario: A refused restore does not hold the gate

- **WHEN** a resumed instance's keyed tasklet declares a cap below the number of keyed Cells the controller ships
- **THEN** the frames past the cap are acknowledged without a Cell, the gate opens once every shipped frame has landed, and the pass resumes rather than waiting for the resume timeout

#### Scenario: A crash between a record and its marker re-runs the stage over its own record

- **WHEN** a durable steps relay Cell's effect record for tasklet B is durable, its `task_committed` entry is not, and the instance is respawned
- **THEN** B re-runs over the restored state, its new record supersedes the interrupted one, the marker names the new record, and only the new record's output reaches the output

#### Scenario: Exhausted or non-retryable error is terminal

- **WHEN** a steps relay's cell error is non-retryable or has exhausted the retry ladder
- **THEN** the relay SHALL compensate completed tasklets and terminate with status `error`, and the platform SHALL NOT automatically re-run the relay

### Requirement: A restore gate that times out SHALL fail the run

A resumed steps run SHALL NOT start a pass while any restore its bind announced
(`resume_cell_count` Cell restores and `effect_result_count` effect results) has not landed. When
the restore gate's bound (30 s) passes first, the run SHALL end with status `error` and an error
naming how many of the announced restores landed; it SHALL run no tasklet and no compensation,
since neither may read Cells whose state never arrived. The controller SHALL treat it as every
run error: no automatic re-run, the outcome recorded, the instance's state, journal and input log
retained. A stop that arrives while the gate is closed SHALL end the run with status
`cancelled`, SHALL NOT open the gate, and SHALL run no tasklet and no compensation.

#### Scenario: A lost restore ends the run instead of running partially restored

- **WHEN** a durable steps run is rebound with `resume_cell_count` 2 and only one restore lands within the gate's bound
- **THEN** the run SHALL end `error` naming 1 of 2 restores, no tasklet SHALL have run in that bind, and the message the controller holds for the instance SHALL remain in its input log

#### Scenario: A stop before the restores land cancels without opening the gate

- **WHEN** a resumed steps run is stopped (externally, or by a `max_duration` already spent at bind) while one of its two announced restores has not landed
- **THEN** the run SHALL end `cancelled`, no tasklet and no compensation hook SHALL have run in that bind, and its journal and state SHALL be retained

### Requirement: Each durable steps instance has an append-only task-journal on the shared state substrate

The controller SHALL maintain one append-only task-journal file per durable steps relay
relay instance at `storage/app/state/{relay_class}/{instance_id}/task.journal`,
written through the existing WalWriter framing and FsyncBatcher path (no parallel
persistence machinery). Journal entries SHALL be typed, length-prefixed frames
carrying `{seq, type, task_idx, iter, payload}`. The entry types SHALL be
`task_committed`, `iter_committed`, `compensated` and `rewound`, and no other;
the journal SHALL hold markers about the instance's progress and SHALL carry no Cell state
and no plugin output: a `task_committed` or `compensated` entry names, for each Cell the stage
ran, its identity, the range of effect records it wrote in that stage (first and last seq,
both absent when it wrote none) and the plugin channels those records wrote on, and the entry's own payload is the stage's
inter-task handoff and its completion. A
torn trailing frame SHALL be truncated on open. An opted-out class
(`durable: false`) SHALL produce no journal file.

#### Scenario: Journal file lives with the instance's state files

- **WHEN** a durable structured steps relay commits its first tasklet
- **THEN** `task.journal` SHALL exist in the instance's state directory alongside its cell WAL files, and its first decoded entry SHALL be `task_committed` for task index 0

#### Scenario: A stage entry names its records, not their contents

- **WHEN** a stage ran two Cells, one of which changed state and emitted on channel `out` and the other did nothing durable
- **THEN** the `task_committed` entry names both Cells, the first with the range of its effect records and `out`, the second with no seq and no channel, and the entry carries no output payload

#### Scenario: Torn tail is recovered

- **WHEN** the controller crashes mid-append leaving a partial final frame and the journal is reopened
- **THEN** the partial frame SHALL be discarded and every complete prior entry SHALL remain readable

#### Scenario: Opted-out class writes no journal

- **WHEN** a steps class with `durable: false` runs a structured chain to completion
- **THEN** no `task.journal` file SHALL be created for the instance

### Requirement: Task-boundary commit is a gated handshake

The SDK SHALL send, at each tasklet barrier of a durable structured steps relay
relay, a `TaskCommit` control-channel request carrying the tasklet index,
the set of Cells that ran, each with the range of effect records it wrote in the stage and the
plugin channels they wrote on, and the serialized inter-task handoff batch. Every effect record a
barrier names SHALL have been sent on the Cell's state stream before the barrier is sent. The
controller SHALL, before appending, wait until each named record is durable on its Cell's writer,
then append the `task_committed` entry, make it durable, and only
then reply `TaskCommitResponse`; the ack therefore implies that every record the entry names is
durable. A named record that the writer refuses, or that is not durable within the barrier's
deadline, SHALL be reported in the response as an error naming the Cell and the seq. Durability SHALL be batched: the append
marks the journal writer dirty and the `FsyncBatcher` tick syncs every dirty
writer with one fsync per file, after which the acks of every barrier that
landed in the tick are released together. The control-channel reader process
SHALL NOT perform the append, the fsync or the wait itself; a per-runner
commit worker SHALL, so that pings, stops, parks and compactions on the same
channel are never queued behind disk I/O. The driver SHALL NOT start the next
tasklet before receiving the successful response. A failed journal write
SHALL be reported in the response and SHALL terminate the run abnormally
(existing terminal-error path, including compensation). A barrier whose
response does not arrive within the driver's wait SHALL be re-sent under the
same `reference_id` up to a bounded number of attempts before the run fails;
the controller SHALL treat a re-sent barrier whose entry is already journaled
as acked, and a re-sent barrier whose first copy is still waiting on its records SHALL join that
wait rather than start a second. A barrier (`TaskCommit` or `IterCommit`) from a runner whose stable
identity the instance's ownership record does not name SHALL NOT be appended:
the commit worker SHALL answer it with a non-empty error, so the sending
driver ends its run through the same terminal-error path, and the refusal
SHALL be counted and reported as `runner-identity-and-session` prescribes.
After a successful ack the commit worker SHALL tell each named Cell's writer that the records
in the range the entry names are committed, which is what releases their outputs; a held record
of that Cell below the range belongs to an attempt that never committed and SHALL be dropped. Opted-out classes
SHALL skip the handshake entirely.

#### Scenario: Next tasklet waits for the durable ack

- **WHEN** tasklet A's cells all quiesce at the barrier
- **THEN** tasklet B SHALL start only after the controller has durably journaled `task_committed(A)` and acked it

#### Scenario: The marker waits for the records it names

- **WHEN** a `TaskCommit` names a Cell's record at seq 5 and that record's batch has not synced when the barrier arrives
- **THEN** the `task_committed` entry SHALL NOT be appended until that batch has synced, and the ack SHALL follow the entry's own sync

#### Scenario: A record the writer never got fails the barrier

- **WHEN** a `TaskCommit` names a record at seq 5 of a Cell whose writer holds no record at that seq within the barrier's deadline
- **THEN** the response SHALL carry an error naming the Cell and seq 5, no entry SHALL be appended, and the run SHALL end through the terminal-error path

#### Scenario: Barriers landing in one tick share one fsync

- **WHEN** ten instances on one runner reach a barrier within one `FsyncBatcher` tick
- **THEN** their ten `task_committed` entries SHALL be made durable by that tick's sync and all ten `TaskCommitResponse`s SHALL be sent after it, none before

#### Scenario: A commit storm does not delay the control channel's other commands

- **WHEN** a runner has hundreds of barriers in flight
- **THEN** a `:ping_runner` round trip on that runner's control channel SHALL complete within its timeout, because the reader forwards commits to the commit worker instead of journaling them inline

#### Scenario: An unacked barrier is re-sent, not failed

- **WHEN** a `TaskCommitResponse` does not arrive within the driver's wait
- **THEN** the driver SHALL re-send the same `TaskCommit` under the same `reference_id`, and a controller that had already journaled the entry SHALL ack it without a second append

#### Scenario: Journal write failure is a terminal run error

- **WHEN** the controller cannot append the `task_committed` entry (e.g. disk error)
- **THEN** the `TaskCommitResponse` SHALL carry the error and the relay SHALL terminate abnormally, compensating completed tasklets per the existing saga contract

#### Scenario: A barrier from a runner that does not own the instance is refused

- **GIVEN** a durable instance bound to runner A's stable identity and a second session B in the namespace
- **WHEN** B sends `TaskCommit` or `IterCommit` naming the instance
- **THEN** the response SHALL carry a non-empty error, the journal SHALL hold no entry for it, no inbox prefix SHALL be released and no journal prefix cut, and B SHALL be sent `RemRelay` for the instance once

### Requirement: Inter-task channels of a durable structured class carry a byte codec

Inter-tasklet channels SHALL carry a symmetric byte codec (typed→bytes and bytes→typed) so the
handoff batch can be journaled and re-seeded, and the runtime SHALL derive that codec rather than
accept a declaration of it. No SDK SHALL offer a way to declare one, so a channel of a durable
structured class cannot lack it and there is no registration refusal for a missing codec.

The codec SHALL ride an **inter-tasklet** channel and no other kind. A channel whose only
wiring is a plugin output is the class's terminal egress: nothing re-seeds from it, and journaling
every plugin payload at every barrier would be journal volume no reader asks for. Registration of
a durable handoff-chained loop SHALL therefore fail when its terminal tasklet releases no
inter-tasklet channel — whether it owns no output channel at all or only a plugin-bound one —
because the iteration commit would otherwise ship an empty handoff and the crash-resumed loop
would read that as convergence and stop.

An oversized handoff batch (above the configured journal frame bound) SHALL fail the commit with
a typed error directing the author to keep bulk data in Cell state.

#### Scenario: A handoff is journaled with nothing declared

- **WHEN** a durable structured steps class wires tasklet A into tasklet B and declares nothing about how the value between them is encoded
- **THEN** registration SHALL succeed, the A→B channel SHALL carry the codec the runtime derived, and A's handoff SHALL be journaled at the commit barrier and re-seeded on crash-resume

#### Scenario: A plugin-bound terminal carries no journal codec

- **WHEN** a tasklet's only output wiring is a plugin output, so the channel derives plugin-bound
- **THEN** that channel SHALL carry no journal codec, and the commit SHALL ship an empty handoff for it

#### Scenario: A durable chained loop whose terminal releases no inter-task handoff is refused

- **WHEN** a durable `Chained` steps class's terminal tasklet owns no output channel, or owns one whose only wiring is a plugin output
- **THEN** registration SHALL fail with an error naming the terminal tasklet

#### Scenario: Oversized handoff fails the commit with a typed error

- **WHEN** a tasklet's serialized handoff batch exceeds the journal frame bound
- **THEN** the task commit SHALL fail with an error identifying the oversized channel/batch, and the run SHALL terminate abnormally

### Requirement: Crash-resume skips journaled tasklets and re-seeds from the journaled handoff

On respawn of a durable structured steps instance, the controller SHALL ship
the journal state (the resume iteration, the entry channel of the last committed
iteration, committed task indexes, and the latest journaled handoff) to the SDK
**on the bind** (`AddRelay.task_journal`), which is instance-scoped and therefore
reaches a class that has no per-Cell state and so receives no `RestoreInitState`.
Every way the instance comes back onto a runner SHALL be that bind: a fresh bind after a
controller restart, an un-park, a rehydrate, a rewind, the convergence loop's rebind after
its runner died, and the takeover by a restarted runner presenting the same stable
identity; none of them SHALL send an `AddRelay` without the journal.
When no compensation is in flight, the driver SHALL skip every tasklet recorded
`task_committed` and SHALL seed each uncommitted tasklet that has a committed producer
from that producer's journaled handoff instead of the entry channel; an uncommitted root
of a message-fed iteration SHALL be seeded from the message the controller re-forwards
for that iteration, and a join SHALL wait only for the producers of the chain that
iteration runs. Compensation-in-flight resume (existing behavior) SHALL take precedence
unchanged. An instance with no journal (opted-out) SHALL keep the re-run-from-top behavior.

#### Scenario: Mid-chain crash resumes at the first uncommitted tasklet

- **WHEN** a chain A→B→C crashes after `task_committed(A)` and `task_committed(B)` were journaled
- **THEN** on respawn the driver SHALL NOT re-run A or B and SHALL run C seeded with B's journaled handoff batch

#### Scenario: Crash before the first commit re-runs from the top

- **WHEN** a chain crashes while tasklet A is still running (no `task_committed` entry)
- **THEN** on respawn the chain SHALL re-run from tasklet A seeded from the re-forwarded message on its entry channel (at-least-once)

#### Scenario: A join on a resume counts only this pass's producers

- **WHEN** a steps relay declares entry `a` feeding A→J and entry `b` feeding B→J, a pass on `a` crashes after `task_committed(A)`, and the instance resumes
- **THEN** J SHALL run seeded from A's handoff without waiting for B, which is not a producer of `a`'s pass

#### Scenario: An instance with no per-Cell state still resumes at its iteration

- **WHEN** a durable looping instance whose tasklets hold no per-Cell state (no Cell WAL, so no `RestoreInitState` is shipped) parks and is rehydrated
- **THEN** the driver SHALL enter the iteration after the last committed one, and that iteration's staged output SHALL be delivered rather than colliding with an earlier iteration's flush marker

#### Scenario: Compensation-in-flight still wins

- **WHEN** a respawned instance has outstanding compensation-intent markers
- **THEN** the driver SHALL run the remaining compensation (existing behavior) and SHALL NOT resume forward execution from the journal

#### Scenario: A rebind onto another runner carries the journal

- **GIVEN** a chain A→B whose runner is killed after `task_committed(A)` was journaled
- **WHEN** the instance is bound onto a replacement runner, by the convergence loop or by the takeover of a restarted runner presenting the same stable identity
- **THEN** the `AddRelay` SHALL carry the folded journal, the replacement's driver SHALL NOT run A, and the run of A observable on the replacement SHALL be absent, not merely deduplicated in the journal and the WAL

### Requirement: The task-journal shares the instance lifecycle and is inspectable

The task-journal SHALL be deleted with the instance's state files by the
finished-instance retention sweeper, `flz state delete`, and `flz reset`. A
read-only CLI verb `flz state journal <relay/instance>` SHALL render the
decoded journal entries.

#### Scenario: Retention removes the journal with the instance data

- **WHEN** a finished steps instance's retention window elapses
- **THEN** `task.journal` SHALL be deleted together with the instance's other state files

#### Scenario: User inspects task progress

- **WHEN** a user runs `flz state journal <relay/instance>` for an instance with journaled commits
- **THEN** the CLI SHALL list the decoded entries (seq, type, task index, iteration) in order

### Requirement: The crash-resume state ships every in-flight task's handoff

The task-journal restore state SHALL carry the journaled handoff of **every**
tasklet committed in the in-flight iteration, keyed by its `task_idx`, so that
each uncommitted child branch re-seeds from exactly its own parent's edge —
filtered to the channel that child consumes. The restore state's iteration-level
handoff SHALL carry the previous iteration's terminal batch, which re-seeds a
handoff-chained loop. The existing frame-size guard SHALL apply unchanged to
each journaled entry.

Whether a run is done SHALL be the engine's answer alone, reported by its `RelayCompleted` and
recorded by the controller as the run's outcome; the controller SHALL NOT derive completion from
the journal, and the journal report SHALL show the recorded outcome, or that the run is still
running.

#### Scenario: Each branch re-seeds from its own parent

- **WHEN** tasklet A with children B and C commits, and the instance later resumes with only B committed
- **THEN** C SHALL be seeded from A's journaled handoff, and B SHALL NOT re-run

#### Scenario: The journal report shows a finished loop as finished

- **WHEN** a `per_input` loop commits two iterations and a tasklet's `complete` ends it in the second
- **THEN** `flz state journal` SHALL show the outcome `ok` for the instance, and before the completion it SHALL show the instance as running

### Requirement: The iteration entry type is active and the journal records no park

The task-journal codec SHALL write and interpret the `iter_committed` and `input_released`
entry types:
`iter_committed` carries the iteration number, the consumed input-log offset (the `flz.offset`
of the input that seeded the iteration) or the fact that the iteration consumed no journaled
input, and, when a tasklet of that iteration called `ctx.complete(result)`, the completion and
its result. The absence of a consumed offset SHALL be recorded as absence, never as offset `0`,
since `0` is the log's first offset. Per-task `task_committed` entries SHALL carry the
enclosing iteration in the `iter` field (0 for a non-looping chain) and, when that tasklet
called `ctx.complete(result)`, its completion and result. An `iter_committed` or
`task_committed` entry written before completions existed SHALL read as carrying none. An
`input_released` entry SHALL carry the input-log offset of an input the instance consumed without
an iteration commit (an input dead-lettered unread before the instance slept); it SHALL be
written before the park that follows it, and the journal's released offset SHALL be the highest
offset any `iter_committed` or `input_released` entry carries. The journal SHALL carry no
`parked` entry type and the codec SHALL know none: where an instance sleeps is the class
registry's per-instance park record, for either kind. The restore state
shipped on a rebind SHALL carry the latest committed iteration's completion, its consumed
offset or its absence, the journal's released offset, and each committed tasklet's own, and
SHALL carry no park flag or wake deadline. The `flz state journal` verb SHALL render the entry types, SHALL show a completing
entry as completing, and SHALL show an iteration that consumed nothing as such rather than as
offset `0`.

#### Scenario: Journal reflects loop progress

- **WHEN** a looping instance has committed two iterations and then slept
- **THEN** `flz state journal` SHALL show the per-task entries tagged with their iteration and two `iter_committed` entries with their consumed input-log offsets, and no `parked` entry, while `flz relay info` reports the instance `suspended`

#### Scenario: An iteration that consumed nothing is journaled as such

- **WHEN** a chained iteration seeded from the previous handoff commits
- **THEN** its `iter_committed` entry SHALL carry no consumed offset, the restore state SHALL ship none for it, and `flz state journal` SHALL render it as having consumed no input

#### Scenario: The first offset is an offset

- **WHEN** the input at `flz.offset` `0` seeds iteration 1 and the iteration commits
- **THEN** its `iter_committed` entry SHALL carry consumed offset `0` and a rehydrate SHALL NOT re-deliver that input

#### Scenario: A completing iteration is journaled as completing

- **WHEN** a tasklet of iteration 3 calls `ctx.complete(result)` and iteration 3 commits
- **THEN** the journal's `iter_committed(3)` entry and the completing tasklet's `task_committed` entry SHALL each carry the completion and `result`, and `flz state journal` SHALL render both as completing

#### Scenario: An input dead-lettered unread is released in the journal

- **WHEN** a durable instance waiting for a message dead-letters the message at offset 0 unread and then sleeps
- **THEN** the journal SHALL hold an `input_released` entry for offset 0 before the park is recorded, `flz state journal` SHALL render it, and the journal's released offset SHALL be 0

### Requirement: The root stage of a timer-fed pass records its trigger, and a resume reads the chain and the consumed firing from it

The `TaskCommit` of the stage that a timer firing opened SHALL carry the trigger: the Cell key, the timer kind and the scheduled timestamp. The controller SHALL write it as a trailing segment of that `task_committed` entry, beside the completion segment, absent on every other entry, and SHALL hand it back on the corresponding committed task in the restore state (`TaskJournalState.committed_tasks`). No `IterCommit` SHALL carry a trigger. On a rebind, the SDK SHALL read the in-flight iteration's chain from the trigger when the iteration's root committed with one, and from the re-forwarded message otherwise; a join SHALL count only the producers of that chain. Before any restored Cell's processing-time timers are re-armed, the SDK SHALL drop from the Cell's restored pending set every timer whose firing's stage the journal records as committed, writing the removal as a timer-ops record, so a committed firing never fires twice. `flz state journal` SHALL render a `task_committed` entry that carries a trigger as opened by that timer, naming the tasklet, the Cell, the kind and the timestamp.

#### Scenario: The root's commit names the firing

- **WHEN** a timer scheduled at 31000 on Cell `a` of tasklet `tick` fires and `tick`'s stage commits
- **THEN** the journal's `task_committed` entry for `tick` SHALL carry the trigger `(a, processing_time, 31000)`, and `flz state journal` SHALL render it as opened by that timer

#### Scenario: A resume after the root committed follows the timer's chain

- **WHEN** a steps relay declares entry `a` feeding `A -> J` and entry `b` feeding `B -> J`, a timer set by `B` fires, `B` commits with the trigger, and the runner crashes before `J` commits
- **THEN** on rebind `J` SHALL run seeded from `B`'s journaled handoff without waiting for `A`, and the timer SHALL NOT be pending on `B`'s Cell

#### Scenario: A resume before the root committed re-arms the timer

- **WHEN** a timer fires on `B`, `on_timer` completes, and the runner crashes before `B`'s `TaskCommit` is acknowledged
- **THEN** on rebind the journal SHALL show no committed task for that iteration, `B`'s Cell SHALL restore the timer pending, it SHALL be re-armed, and its firing SHALL open the pass again

#### Scenario: A non-root stage carries no trigger

- **WHEN** `J` commits in a timer-fed iteration
- **THEN** its `task_committed` entry SHALL carry no trigger segment, and its restore-state counterpart SHALL carry none

### Requirement: Post-start input is durably journaled before delivery

The controller SHALL append every post-start input for an instance whose input is journaled
(a durable steps relay, a flow relay on the effectively-once tier, or a class of either run
that sleeps) to the instance's durable input log (`durability-input-log`, the same
module, file and offset space for every such instance) with a monotonic offset **before**
forwarding it to the runner, stamped `flz.offset`. An input carrying a `flz.msg_id` already
retained in the log SHALL be absorbed at append; an input carrying no id SHALL always be
appended. The log's floor SHALL be the durable consumption the instance's own records carry
(`durability-input-log`): for a flow relay, whether it declares the tier or only sleeps,
its Cells' durable consumed offsets and the processed prefix the controller confirms; for a
steps relay, the journal's iteration commits and released inputs. What the controller has
forwarded SHALL NOT move the floor. Entries at or below the floor
SHALL be released to the log's retention. Message payloads SHALL live only in the input log,
never in `task.journal`. A class that declares none of the three SHALL have no durable input
and SHALL pay no append.

#### Scenario: Input survives the instance being down

- **WHEN** a message arrives for a suspended (or crashed, not-yet-respawned) instance whose input is journaled
- **THEN** it SHALL be durably appended to the instance's input log and delivered when the
  instance next runs, not dropped

#### Scenario: A sleeping flow relay's message is held and wakes it

- **WHEN** a message arrives for a flow class that sleeps while the instance is suspended
- **THEN** it SHALL be appended to the instance's input log, the instance SHALL be rehydrated, and the message SHALL be delivered by the bind's re-forward, once

#### Scenario: A plain sleeping flow relay folds nothing twice after a controller restart

- **WHEN** a flow relay that sleeps and is not on the tier has folded `a:100`, slept, woken on `a:1` and `a:2`, and the controller restarts while it is awake, then `a:10` is sent
- **THEN** its total for `a` SHALL be 113, and none of the three earlier inputs SHALL be folded again

#### Scenario: A forward alone does not release input

- **WHEN** the controller forwards the record at offset 9 to the runner of a plain sleeping flow relay and the runner dies before the record's change is durable
- **THEN** offset 9 SHALL NOT be released and the rebind SHALL re-forward it

#### Scenario: Duplicate send is absorbed

- **WHEN** the same message, carrying the same `flz.msg_id`, is sent twice before the instance
  consumes it
- **THEN** exactly one log record SHALL exist and the relay SHALL receive it once

#### Scenario: A message without an id is never absorbed

- **WHEN** the same payload is sent twice with no `flz.msg_id`
- **THEN** two records SHALL exist and the relay SHALL receive both

#### Scenario: Consumed entries are released

- **WHEN** iteration N commits with consumed offset 7
- **THEN** records at or below 7 SHALL be eligible for the log's retention and later rehydrates
  SHALL NOT re-deliver them

#### Scenario: A class that does not sleep pays nothing

- **WHEN** a flow class declares `sleep_after: false` and not the effectively-once tier
- **THEN** no input log SHALL be written for its instances and every input SHALL be forwarded as today

### Requirement: A suspended instance rehydrates on new input or its durable timer

The controller SHALL rehydrate a `suspended` instance of either run on a new input-log append
or on the elapse of its recorded wake deadline, which for a steps relay is the earliest pending
processing-time timer its Cells held when it parked: respawn through the normal bind path on any
live runner (existing session fencing applies), restore its Cells from their snapshots and WAL
tails and, for a steps relay, its journal state, deliver the log's retained tail above the
instance's floor through the one re-forward every bind runs, and continue: a steps relay enters
iteration N+1, a flow relay resumes folding input. A wake on the deadline SHALL append nothing to the input log:
the restored Cells re-arm their pending timers, the due timer fires on the runner, and its
firing opens the pass at the tasklet that set it. An elapsed wake deadline SHALL count as
pending work wherever undelivered input does, so a wake that found no runner to rehydrate onto
SHALL be retried on the next convergence tick. Rehydration SHALL survive controller
restart/redeploy: at boot an instance whose park record exists, whose log owes nothing and
whose wake deadline has not elapsed SHALL enter `suspended` without binding, and any other
SHALL bind. Concurrent
wake sources SHALL resolve to exactly one rehydration. Whether a suspended instance still owes
input SHALL be read from the log's tail and the instance's floor, not from a decode of the file.

#### Scenario: New message wakes a suspended instance

- **WHEN** a message arrives for an instance suspended at iteration N
- **THEN** the instance SHALL be respawned (possibly on a different runner), restored from
  journal + iter-N snapshots, and SHALL process the message as iteration N+1

#### Scenario: A sleeping flow relay wakes with its state

- **WHEN** a message arrives for a flow instance that slept with a keyed Cell holding a running total
- **THEN** the instance SHALL be respawned on a live runner, that Cell SHALL be restored to the total it held before the sleep, and the message SHALL be folded onto it

#### Scenario: Suspension survives controller redeploy

- **WHEN** the controller restarts while an instance of either run is suspended and a message then arrives
- **THEN** the instance SHALL rehydrate exactly as if no restart had happened

#### Scenario: Durable timer fires without input

- **WHEN** a steps instance parked with a wake deadline, the earliest pending processing-time timer its Cells held, reaches that deadline with no input
- **THEN** it SHALL be rehydrated, nothing SHALL be appended to its input log, and the tasklet that set the timer SHALL run `on_timer` on its Cell as iteration N+1

#### Scenario: A wake with no runner is retried

- **WHEN** an instance's wake deadline elapses while no runner is active, and a runner then connects
- **THEN** the instance SHALL be rehydrated on the next convergence tick and its due timer SHALL fire

#### Scenario: A park landing during an un-park keeps the suspension

- **WHEN** the rehydrated instance parks again before the un-park task has reported its bound state
- **THEN** the Overwatch SHALL stay suspended (the runner has already torn the instance down),
  the park record written by that park SHALL stand, and the Overwatch SHALL rehydrate again only
  if the log still holds records above the instance's floor or its wake deadline has elapsed; the task's stale bound state SHALL
  NOT be adopted

#### Scenario: Racing wake sources rehydrate once

- **WHEN** an input-log append and a timer deadline fire for the same suspended instance
  concurrently
- **THEN** exactly one rehydration SHALL occur

### Requirement: An idle instance sleeps durably and is evicted from memory

Sleeping when idle SHALL be a class setting of every class that keeps its work, its
`durable` tree's `sleep_after`, declared in every SDK, announced by the runner in the
`AddRelay` response (`sleep_after_ms`) and recorded by the controller beside the class's
other declarations. The idle threshold SHALL be that interval, for a flow instance and a
steps instance alike; no knob SHALL carry a run in its name.

The SDK SHALL, when an instance of a class that sleeps has been idle past the threshold, send
`IterPark` (the iteration a steps relay sleeps at, `0` for a flow relay, and, for a steps relay, a wake
deadline: the earliest pending processing-time timer across the instance's Cells, none when no
Cell holds one) and remove the instance from runner memory; the controller SHALL write the park
record durably before the eviction, then tear the instance down without a terminal completion:
registry state `suspended`, zero live runner resources. An instance is idle when no cell of it
holds a queued, in-flight, held-back, retrying or unflushed item, no
input tail is owed, for a flow relay no cell timer is pending (a steps relay sleeps with its timers
pending, its wake deadline naming the earliest) and, for a steps relay, its pass sheet is `Waiting` with an empty queue at a
committed iteration boundary, the boundary before its first pass included. The threshold SHALL
be timed as one deadline on the core's deferred-wake list, armed once and re-armed from the
cells' latest activity when it comes due early, promoted by a thread already waiting in `poll`,
and SHALL hold no thread and touch no input path; an instance found busy when it comes due
SHALL NOT sleep and the deadline SHALL re-arm. A steps relay's processing-time timer that comes
due inside the idle window SHALL fire and open a pass, as a message does, so the instance is
not idle when the deadline comes due. No class-level wake setting SHALL exist: a steps relay's
wake deadline is what its tasklets' timers say. An iteration boundary SHALL be the only place a
steps relay waits for a message. An eviction-hysteresis knob SHALL avoid park/rehydrate thrash
under trickling input.

The park record SHALL be a per-instance fact in the class registry's durable sidecar for either
run, holding the wake deadline when one was given and nothing else; it SHALL NOT be a
task-journal entry and SHALL hold no consumed offset. The bind that wakes the instance SHALL
clear it before the runner is told to start the instance; clearing it SHALL release nothing and
lower no floor.

The suspended Overwatch SHALL hibernate: a sleeper holds nothing of its bind on
its heap (the restore frames and plugin setup are garbage once it parks).

#### Scenario: A sleeper costs the controller a minimal heap

- **WHEN** an instance parks
- **THEN** its Overwatch SHALL be hibernated, and a resting population SHALL cost the controller on the order of tens of KiB per sleeper, not the bind's working set

#### Scenario: Idle chat relay frees the runner

- **WHEN** a looping instance commits iteration N and nothing arrives past the idle threshold
- **THEN** the park record SHALL be written, the instance SHALL be removed from runner memory, and the registry SHALL report it `suspended`

#### Scenario: An idle flow relay that declares sleeping frees the runner

- **WHEN** an instance of a flow class that sleeps receives nothing for its class's `sleep_after` with every cell drained and flushed and no timer pending
- **THEN** the runner SHALL send `IterPark` with iteration `0`, the park record SHALL be written, the instance SHALL be removed from runner memory, and the registry SHALL report it `suspended`

#### Scenario: A flow relay declaring `sleep_after: false` never sleeps

- **WHEN** a flow class declares `sleep_after: false` and an instance of it receives nothing for hours
- **THEN** no `IterPark` SHALL be sent and the instance SHALL stay bound

#### Scenario: A busy instance does not sleep at the deadline

- **WHEN** the idle deadline comes due while a cell of the instance still holds an item in a retry backoff, an unflushed buffer or, on a flow relay, a pending timer
- **THEN** the instance SHALL NOT sleep and the deadline SHALL be re-armed

#### Scenario: A park records the earliest pending timer as its deadline

- **WHEN** a looping steps instance's Cells hold processing-time timers at 90 000 and 60 000 when it sleeps
- **THEN** its `IterPark` SHALL carry 60 000 as its wake deadline and the park record SHALL hold it

#### Scenario: A timer due inside the idle window fires instead of parking

- **WHEN** a looping steps instance commits iteration N holding a timer due before the idle threshold elapses
- **THEN** the timer SHALL fire and open iteration N+1, and no park record SHALL be written for the boundary

#### Scenario: A once instance waiting for its first message parks

- **WHEN** a durable `once` instance is started without a message and none arrives within the idle threshold
- **THEN** the park record SHALL be written for iteration `0`, the instance SHALL be removed from runner memory, the registry SHALL report it `suspended`, and the next message SHALL rehydrate it into pass 0

#### Scenario: A message arriving mid-iteration waits for the boundary

- **WHEN** a message reaches a `PerInput` instance while an iteration is still running
- **THEN** it SHALL be journaled to the input log, the runner SHALL hold it in the instance's queue, it SHALL seed the next iteration after the current one commits, and no tasklet SHALL receive it mid-iteration

#### Scenario: A waiting population holds no thread

- **WHEN** ten thousand instances wait inside the idle threshold
- **THEN** no thread SHALL exist on their behalf and the runner SHALL consume no measurable CPU
  for the wait

#### Scenario: The park record is not in the journal

- **WHEN** a durable loop has parked and `flz state journal` is run for it
- **THEN** the journal SHALL show its committed entries and no `parked` entry, and the instance SHALL still be reported `suspended`

#### Scenario: A message parked unread does not wake the instance again

- **WHEN** a durable instance waiting for a message receives one it cannot read, parks it to the dead-letter queue, and then waits past the idle threshold
- **THEN** the journal SHALL record that message's offset as released, the message SHALL no longer be owed, and the instance SHALL stay `suspended` rather than be rehydrated to receive it again; a rehydrated instance that was owed the message SHALL count it as received and park on the idle threshold

#### Scenario: A woken instance keeps its release across a controller restart

- **WHEN** a durable instance that dead-lettered a message unread and slept is woken, and the controller restarts before its next iteration commits
- **THEN** the message SHALL NOT be owed again and SHALL NOT be dead-lettered a second time

### Requirement: A rehydrated instance waits for the input tail it is owed before it sleeps

The controller SHALL tell a rehydrating bind of either run how many journaled, undelivered
input records it will forward after the bind (`AddRelay.inbox_tail_count`, the log's tail
above the instance's floor), and SHALL forward them after that bind, on every bind the
instance takes: a rehydrate from suspension, a rewind, the convergence loop's rebind after its
runner died, and the takeover by a restarted runner presenting the same stable identity. The
SDK SHALL NOT start the instance's idle clock until that many records have arrived (bounded by
an owed-tail wait of 60 s, after which it sleeps and the controller's rehydrate-if-pending
forwards the tail again). A tail forwarded after the bind under load would otherwise reach an
instance that has already slept and be dropped at the socket. The controller SHALL NOT forward
the tail to a process that still runs the instance: a reconnect of the same process moves its
session and forwards nothing.

#### Scenario: The tail arrives after the park idle would have expired
- **WHEN** a suspended instance with one undelivered record is rehydrated and the controller's forward of that record reaches the runner later than the idle window after the bind
- **THEN** the instance SHALL still be awake, consume the record, and SHALL NOT have slept in between

#### Scenario: A crashed instance rebound elsewhere is told and sent its tail
- **GIVEN** a durable per-input steps relay whose runner is killed with two journaled input records not yet consumed
- **WHEN** the instance is bound onto a replacement runner by the convergence loop or by a same-identity takeover
- **THEN** the `AddRelay` SHALL carry `inbox_tail_count = 2`, both records SHALL be forwarded after the bind with their original offsets, and the instance SHALL run them as its next iterations without sleeping first

#### Scenario: Nothing owed
- **WHEN** an instance binds with `inbox_tail_count = 0`
- **THEN** its idle clock runs from the bind as before

### Requirement: The sleep interval is the class's `sleep_after`, on by default for a steps class

An instance of a class whose `durable` is `local` or `remote` SHALL sleep once it has been
idle at a committed boundary for the class's `sleep_after`. A class that declares no
`sleep_after` SHALL sleep after six hours when it runs in steps and SHALL never sleep when it
runs as a flow, because a sleeping flow instance has its input journaled before it is
forwarded. A class that declares `sleep_after: false` SHALL never sleep. No environment variable SHALL set or override the interval;
<!-- docs-citation-record:begin -->`FLEZHA_PARK_IDLE_MS` SHALL not exist<!-- docs-citation-record:end -->. A class whose `durable` is `false` SHALL have no
`sleep_after` to declare. The sleep, the eviction, the wake on input or on a durable timer
and the input tail owed on wake SHALL be what the other requirements of this capability
describe, for a flow instance and a steps instance alike.

#### Scenario: A steps class that says nothing sleeps after six hours

- **WHEN** a durable steps class declares no `sleep_after` and an instance is idle at a committed boundary
- **THEN** the instance SHALL ship its park at six hours of idleness and no sooner, whatever the environment carries

#### Scenario: A flow class that says nothing never sleeps

- **WHEN** a durable flow class declares no `sleep_after` and an instance is idle
- **THEN** the class SHALL announce no `sleep_after_ms` and the instance SHALL stay bound, its input forwarded without the input log

#### Scenario: A short interval on the class

- **WHEN** a class declares `durable: {local, sleep_after: 2 seconds}` and an instance goes idle
- **THEN** the instance SHALL be shown `suspended` by `flz relay info` within a few seconds and SHALL answer the next message with its cells restored

#### Scenario: A flow instance sleeps like a steps instance

- **WHEN** a flow class declares `sleep_after: 2 seconds` and its instance goes idle
- **THEN** it SHALL park, be evicted and wake on its next input exactly as a steps instance does
