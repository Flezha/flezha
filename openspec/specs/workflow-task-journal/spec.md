# workflow-task-journal Specification

## Purpose

The workflow-task-journal capability is the controller-side durable task-progress store for durable structured workflow operatives: one append-only `task.journal` per instance on the shared WalWriter/FsyncBatcher substrate, recording each completed tasklet and its serialized inter-task handoff batch. At every tasklet barrier the SDK ships a `TaskCommit` control-channel request and blocks until the controller has fsynced the `task_committed` entry and replied — the durable point later changes gate staged output flushes and iteration commits on. On crash-resume the journal state rides the bind itself (`AddOperative.task_journal`) — it is instance-scoped, and a class with no per-Cell state receives no per-Cell restore at all — so committed tasklets are skipped (not redone) and the first uncommitted tasklet is re-seeded from the journaled handoff; compensation-in-flight resume takes precedence unchanged. Inter-tasklet channels of durable structured classes carry a symmetric byte codec so the handoff is journal-able; the journal shares the instance's file lifecycle (retention, delete, reset) and is inspectable via `flz state journal`. Opted-out classes (`PERSISTENCE_ENABLED = false`) write no journal and keep the re-run-from-top contract.
## Requirements
### Requirement: Each durable workflow instance has an append-only task-journal on the shared state substrate

The controller SHALL maintain one append-only task-journal file per durable workflow
operative instance at `app_data/state/{operative_class}/{instance_id}/task.journal`,
written through the existing WalWriter framing and FsyncBatcher path (no parallel
persistence machinery). Journal entries SHALL be typed, length-prefixed frames
carrying `{seq, type, task_idx, iter, payload}`. This capability ships the
`task_committed` entry type; the codec SHALL tolerate the reserved entry types
(`outbox_flushed`, `iter_committed`, `parked`, `compensated`) without crashing. A
torn trailing frame SHALL be truncated on open. An opted-out class
(`PERSISTENCE_ENABLED = false`) SHALL produce no journal file.

#### Scenario: Journal file lives with the instance's state files

- **WHEN** a durable structured workflow operative commits its first tasklet
- **THEN** `task.journal` SHALL exist in the instance's state directory alongside its cell WAL files, and its first decoded entry SHALL be `task_committed` for task index 0

#### Scenario: Torn tail is recovered

- **WHEN** the controller crashes mid-append leaving a partial final frame and the journal is reopened
- **THEN** the partial frame SHALL be discarded and every complete prior entry SHALL remain readable

#### Scenario: Opted-out class writes no journal

- **WHEN** a workflow class with `PERSISTENCE_ENABLED = false` runs a structured chain to completion
- **THEN** no `task.journal` file SHALL be created for the instance

### Requirement: Task-boundary commit is a gated handshake

The SDK SHALL send, at each tasklet barrier of a durable structured workflow
operative, a `TaskCommit` control-channel request carrying the tasklet index,
the set of cells that ran, and the serialized inter-task handoff batch. The
controller SHALL append the `task_committed` entry, make it durable, and only
then reply `TaskCommitResponse`. Durability SHALL be batched: the append
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
as acked. A barrier (`TaskCommit` or `IterCommit`) from a runner whose stable
identity the instance's ownership record does not name SHALL NOT be appended:
the commit worker SHALL answer it with a non-empty error, so the sending
driver ends its run through the same terminal-error path, and the refusal
SHALL be counted and reported as `runner-identity-and-session` prescribes.
Opted-out classes SHALL skip the handshake entirely.

#### Scenario: Next tasklet waits for the durable ack

- **WHEN** tasklet A's cells all quiesce at the barrier
- **THEN** tasklet B SHALL start only after the controller has durably journaled `task_committed(A)` and acked it

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
- **THEN** the `TaskCommitResponse` SHALL carry the error and the operative SHALL terminate abnormally, compensating completed tasklets per the existing saga contract

#### Scenario: A barrier from a runner that does not own the instance is refused

- **GIVEN** a durable instance bound to runner A's stable identity and a second session B in the namespace
- **WHEN** B sends `TaskCommit` or `IterCommit` naming the instance
- **THEN** the response SHALL carry a non-empty error, the journal SHALL hold no entry for it, no inbox prefix SHALL be released and no journal prefix cut, and B SHALL be sent `RemOperative` for the instance once

### Requirement: Inter-task channels of a durable structured class carry a byte codec

Inter-tasklet channels SHALL carry a symmetric byte codec (typed→bytes and bytes→typed) so the
handoff batch can be journaled and re-seeded, and the runtime SHALL derive that codec rather than
accept a declaration of it. No SDK SHALL offer a way to declare one, so a channel of a durable
structured class cannot lack it and there is no registration refusal for a missing codec.

The codec SHALL ride an **inter-tasklet** channel and no other kind. A channel whose only
wiring is a plugin sink is the class's terminal egress: nothing re-seeds from it, and journaling
every plugin payload at every barrier would be journal volume no reader asks for. Registration of
a durable handoff-chained loop SHALL therefore fail when its terminal tasklet releases no
inter-tasklet channel — whether it owns no output channel at all or only a plugin-bound one —
because the iteration commit would otherwise ship an empty handoff and the crash-resumed loop
would read that as convergence and stop.

An oversized handoff batch (above the configured journal frame bound) SHALL fail the commit with
a typed error directing the author to keep bulk data in Cell state.

#### Scenario: A handoff is journaled with nothing declared

- **WHEN** a durable structured workflow class wires tasklet A into tasklet B and declares nothing about how the value between them is encoded
- **THEN** registration SHALL succeed, the A→B channel SHALL carry the codec the runtime derived, and A's handoff SHALL be journaled at the commit barrier and re-seeded on crash-resume

#### Scenario: A plugin-bound terminal carries no journal codec

- **WHEN** a tasklet's only output wiring is a plugin sink, so the channel derives plugin-bound
- **THEN** that channel SHALL carry no journal codec, and the commit SHALL ship an empty handoff for it

#### Scenario: A durable chained loop whose terminal releases no inter-task handoff is refused

- **WHEN** a durable `Chained` workflow class's terminal tasklet owns no output channel, or owns one whose only wiring is a plugin sink
- **THEN** registration SHALL fail with an error naming the terminal tasklet

#### Scenario: Oversized handoff fails the commit with a typed error

- **WHEN** a tasklet's serialized handoff batch exceeds the journal frame bound
- **THEN** the task commit SHALL fail with an error identifying the oversized channel/batch, and the run SHALL terminate abnormally

### Requirement: Crash-resume skips journaled tasklets and re-seeds from the journaled handoff

On respawn of a durable structured workflow instance, the controller SHALL ship
the journal state (the resume iteration, the entry channel of the last committed
iteration, committed task indexes, and the latest journaled handoff) to the SDK
**on the bind** (`AddOperative.task_journal`), which is instance-scoped and therefore
reaches a class that has no per-Cell state and so receives no `RestoreInitState`.
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

- **WHEN** a workflow declares entry `a` feeding A→J and entry `b` feeding B→J, a pass on `a` crashes after `task_committed(A)`, and the instance resumes
- **THEN** J SHALL run seeded from A's handoff without waiting for B, which is not a producer of `a`'s pass

#### Scenario: An instance with no per-Cell state still resumes at its iteration

- **WHEN** a durable looping instance whose tasklets hold no per-Cell state (no Cell WAL, so no `RestoreInitState` is shipped) parks and is rehydrated
- **THEN** the driver SHALL enter the iteration after the last committed one, and that iteration's staged output SHALL be delivered rather than colliding with an earlier iteration's flush marker

#### Scenario: Compensation-in-flight still wins

- **WHEN** a respawned instance has outstanding compensation-intent markers
- **THEN** the driver SHALL run the remaining compensation (existing behavior) and SHALL NOT resume forward execution from the journal

### Requirement: The task-journal shares the instance lifecycle and is inspectable

The task-journal SHALL be deleted with the instance's state files by the
finished-instance retention sweeper, `flz state delete`, and `flz reset`. A
read-only CLI verb `flz state journal <operative/instance>` SHALL render the
decoded journal entries.

#### Scenario: Retention removes the journal with the instance data

- **WHEN** a finished workflow instance's retention window elapses
- **THEN** `task.journal` SHALL be deleted together with the instance's other state files

#### Scenario: Operator inspects task progress

- **WHEN** an operator runs `flz state journal <operative/instance>` for an instance with journaled commits
- **THEN** the CLI SHALL list the decoded entries (seq, type, task index, iteration) in order

### Requirement: The task-commit payload carries staged outbox entries

The `TaskCommit` request and the `task_committed` journal entry SHALL carry the
tasklet's staged outbox entries (`{cell_key, seq_in_task, payload,
plugin_route}`) in addition to the cells-ran set and the handoff batch. The
journal frame size guard SHALL apply to the combined payload, with the same
typed oversized error. The `outbox_flushed` and `compensated` entry types
(previously reserved) SHALL be written and interpreted as defined by the
`workflow-transactional-outbox` capability.

#### Scenario: Commit and outbox arrive as one request

- **WHEN** a structured tasklet with staged outputs reaches its barrier
- **THEN** a single `TaskCommit` request SHALL carry both the handoff batch and the outbox entries, journaled in one `task_committed` entry

#### Scenario: Oversized combined payload is rejected with a typed error

- **WHEN** a tasklet's handoff plus staged outbox exceeds the journal frame bound
- **THEN** the commit SHALL fail with the typed oversized error and the run SHALL terminate abnormally

### Requirement: Iteration and park entry types are active

The task-journal codec SHALL write and interpret the `iter_committed` and
`parked` entry types (previously reserved): `iter_committed` carries the
iteration number, the consumed inbox offset, and, when a tasklet of that
iteration called `ctx.complete(result)`, the completion and its result;
`parked` carries the iteration number and an optional wake deadline. Per-task
`task_committed` entries SHALL carry the enclosing iteration in the `iter`
field (0 for a non-looping chain) and, when that tasklet called
`ctx.complete(result)`, its completion and result. An `iter_committed` or
`task_committed` entry written before completions existed SHALL read as
carrying none. The restore state shipped on a rebind SHALL carry the latest
committed iteration's completion and each committed tasklet's own. The
`flz state journal` verb SHALL render both entry types, and SHALL show a
completing entry as completing.

#### Scenario: Journal reflects loop progress

- **WHEN** a looping instance has committed two iterations and parked
- **THEN** `flz state journal` SHALL show the per-task entries tagged with their iteration, two `iter_committed` entries with consumed offsets, and a final `parked` entry

#### Scenario: A completing iteration is journaled as completing

- **WHEN** a tasklet of iteration 3 calls `ctx.complete(result)` and iteration 3 commits
- **THEN** the journal's `iter_committed(3)` entry and the completing tasklet's `task_committed` entry SHALL each carry the completion and `result`, and `flz state journal` SHALL render both as completing

### Requirement: The crash-resume state ships every in-flight task's handoff

The task-journal restore state SHALL carry the journaled handoff of **every**
tasklet committed in the in-flight iteration, keyed by its `task_idx`, so that
each uncommitted child branch re-seeds from exactly its own parent's edge —
filtered to the channel that child consumes. The restore state's iteration-level
handoff SHALL carry the previous iteration's terminal batch, which re-seeds a
handoff-chained loop. The existing frame-size guard SHALL apply unchanged to
each journaled entry.

The SDK SHALL declare its leaf tasklets in the `AddOperative` response
metadata, and the controller SHALL derive operative completion (every leaf
tasklet committed in the in-flight iteration) from the journal alone.

#### Scenario: Each branch re-seeds from its own parent

- **WHEN** tasklet A with children B and C commits, and the instance later resumes with only B committed
- **THEN** C SHALL be seeded from A's journaled handoff, and B SHALL NOT re-run

#### Scenario: Controller derives DONE from the journal

- **WHEN** the journal holds `task_committed` entries covering every declared leaf tasklet of the in-flight iteration
- **THEN** the controller SHALL report the operative complete from the journal alone

