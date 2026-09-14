# workflow-proc-journal Specification

## Purpose

The workflow-proc-journal capability is the controller-side durable proc-progress store for durable structured workflow operatives: one append-only `proc.journal` per instance on the shared WalWriter/FsyncBatcher substrate, recording each completed procedure and its serialized inter-proc handoff batch. At every procedure barrier the SDK ships a `ProcCommit` control-channel request and blocks until the controller has fsynced the `proc_committed` entry and replied — the durable point later changes gate staged output flushes and iteration commits on. On crash-resume the journal state rides the bind itself (`AddOperative.proc_journal`) — it is instance-scoped, and a class with no per-Entity state receives no per-Entity restore at all — so committed procedures are skipped (not redone) and the first uncommitted procedure is re-seeded from the journaled handoff; compensation-in-flight resume takes precedence unchanged. Inter-procedure channels of durable structured classes carry a symmetric byte codec so the handoff is journal-able; the journal shares the instance's file lifecycle (retention, delete, reset) and is inspectable via `flz state journal`. Opted-out classes (`PERSISTENCE_ENABLED = false`) write no journal and keep the re-run-from-top contract.
## Requirements
### Requirement: Each durable workflow instance has an append-only proc-journal on the shared state substrate

The controller SHALL maintain one append-only proc-journal file per durable workflow
operative instance at `app_data/state/{operative_class}/{instance_id}/proc.journal`,
written through the existing WalWriter framing and FsyncBatcher path (no parallel
persistence machinery). Journal entries SHALL be typed, length-prefixed frames
carrying `{seq, type, proc_idx, iter, payload}`. This capability ships the
`proc_committed` entry type; the codec SHALL tolerate the reserved entry types
(`outbox_flushed`, `iter_committed`, `parked`, `compensated`) without crashing. A
torn trailing frame SHALL be truncated on open. An opted-out class
(`PERSISTENCE_ENABLED = false`) SHALL produce no journal file.

#### Scenario: Journal file lives with the instance's state files

- **WHEN** a durable structured workflow operative commits its first procedure
- **THEN** `proc.journal` SHALL exist in the instance's state directory alongside its entity WAL files, and its first decoded entry SHALL be `proc_committed` for proc index 0

#### Scenario: Torn tail is recovered

- **WHEN** the controller crashes mid-append leaving a partial final frame and the journal is reopened
- **THEN** the partial frame SHALL be discarded and every complete prior entry SHALL remain readable

#### Scenario: Opted-out class writes no journal

- **WHEN** a workflow class with `PERSISTENCE_ENABLED = false` runs a structured chain to completion
- **THEN** no `proc.journal` file SHALL be created for the instance

### Requirement: Proc-boundary commit is a gated handshake

The SDK SHALL send, at each procedure barrier of a durable structured workflow
operative, a `ProcCommit` control-channel request carrying the procedure index,
the set of entities that ran, and the serialized inter-proc handoff batch. The
controller SHALL append the `proc_committed` entry, make it durable, and only
then reply `ProcCommitResponse`. Durability SHALL be batched: the append
marks the journal writer dirty and the `FsyncBatcher` tick syncs every dirty
writer with one fsync per file, after which the acks of every barrier that
landed in the tick are released together. The control-channel reader process
SHALL NOT perform the append, the fsync or the wait itself; a per-runner
commit worker SHALL, so that pings, stops, parks and compactions on the same
channel are never queued behind disk I/O. The driver SHALL NOT start the next
procedure before receiving the successful response. A failed journal write
SHALL be reported in the response and SHALL terminate the run abnormally
(existing terminal-error path, including compensation). A barrier whose
response does not arrive within the driver's wait SHALL be re-sent under the
same `reference_id` up to a bounded number of attempts before the run fails;
the controller SHALL treat a re-sent barrier whose entry is already journaled
as acked. A barrier (`ProcCommit` or `IterCommit`) from a runner whose stable
identity the instance's ownership record does not name SHALL NOT be appended:
the commit worker SHALL answer it with a non-empty error, so the sending
driver ends its run through the same terminal-error path, and the refusal
SHALL be counted and reported as `runner-identity-and-session` prescribes.
Opted-out classes SHALL skip the handshake entirely.

#### Scenario: Next procedure waits for the durable ack

- **WHEN** procedure A's entities all quiesce at the barrier
- **THEN** procedure B SHALL start only after the controller has durably journaled `proc_committed(A)` and acked it

#### Scenario: Barriers landing in one tick share one fsync

- **WHEN** ten instances on one runner reach a barrier within one `FsyncBatcher` tick
- **THEN** their ten `proc_committed` entries SHALL be made durable by that tick's sync and all ten `ProcCommitResponse`s SHALL be sent after it, none before

#### Scenario: A commit storm does not delay the control channel's other commands

- **WHEN** a runner has hundreds of barriers in flight
- **THEN** a `:ping_runner` round trip on that runner's control channel SHALL complete within its timeout, because the reader forwards commits to the commit worker instead of journaling them inline

#### Scenario: An unacked barrier is re-sent, not failed

- **WHEN** a `ProcCommitResponse` does not arrive within the driver's wait
- **THEN** the driver SHALL re-send the same `ProcCommit` under the same `reference_id`, and a controller that had already journaled the entry SHALL ack it without a second append

#### Scenario: Journal write failure is a terminal run error

- **WHEN** the controller cannot append the `proc_committed` entry (e.g. disk error)
- **THEN** the `ProcCommitResponse` SHALL carry the error and the operative SHALL terminate abnormally, compensating completed procedures per the existing saga contract

#### Scenario: A barrier from a runner that does not own the instance is refused

- **GIVEN** a durable instance bound to runner A's stable identity and a second session B in the namespace
- **WHEN** B sends `ProcCommit` or `IterCommit` naming the instance
- **THEN** the response SHALL carry a non-empty error, the journal SHALL hold no entry for it, no inbox prefix SHALL be released and no journal prefix cut, and B SHALL be sent `RemOperative` for the instance once

### Requirement: Inter-proc channels of a durable structured class carry a byte codec

Inter-procedure channels SHALL carry a symmetric byte codec (typed→bytes and bytes→typed) so the
handoff batch can be journaled and re-seeded, and the runtime SHALL derive that codec rather than
accept a declaration of it. No SDK SHALL offer a way to declare one, so a channel of a durable
structured class cannot lack it and there is no registration refusal for a missing codec.

The codec SHALL ride an **inter-procedure** channel and no other kind. A channel whose only
wiring is a plugin sink is the class's terminal egress: nothing re-seeds from it, and journaling
every plugin payload at every barrier would be journal volume no reader asks for. Registration of
a durable handoff-chained loop SHALL therefore fail when its terminal procedure releases no
inter-procedure channel — whether it owns no output channel at all or only a plugin-bound one —
because the iteration commit would otherwise ship an empty handoff and the crash-resumed loop
would read that as convergence and stop.

An oversized handoff batch (above the configured journal frame bound) SHALL fail the commit with
a typed error directing the author to keep bulk data in Entity state.

#### Scenario: A handoff is journaled with nothing declared

- **WHEN** a durable structured workflow class wires procedure A into procedure B and declares nothing about how the value between them is encoded
- **THEN** registration SHALL succeed, the A→B channel SHALL carry the codec the runtime derived, and A's handoff SHALL be journaled at the commit barrier and re-seeded on crash-resume

#### Scenario: A plugin-bound terminal carries no journal codec

- **WHEN** a procedure's only output wiring is a plugin sink, so the channel derives plugin-bound
- **THEN** that channel SHALL carry no journal codec, and the commit SHALL ship an empty handoff for it

#### Scenario: A durable chained loop whose terminal releases no inter-proc handoff is refused

- **WHEN** a durable `Chained` workflow class's terminal procedure owns no output channel, or owns one whose only wiring is a plugin sink
- **THEN** registration SHALL fail with an error naming the terminal procedure

#### Scenario: Oversized handoff fails the commit with a typed error

- **WHEN** a procedure's serialized handoff batch exceeds the journal frame bound
- **THEN** the proc commit SHALL fail with an error identifying the oversized channel/batch, and the run SHALL terminate abnormally

### Requirement: Crash-resume skips journaled procedures and re-seeds from the journaled handoff

On respawn of a durable structured workflow instance, the controller SHALL ship
the journal state (the resume iteration, committed proc indexes, and the latest
journaled handoff) to the SDK **on the bind** (`AddOperative.proc_journal`), which
is instance-scoped and therefore reaches a class that has no per-Entity state
and so receives no `RestoreInitState`. When no compensation is in flight, the
driver SHALL skip every procedure recorded `proc_committed` and SHALL seed the
first uncommitted procedure's input batch from the journaled handoff instead of
the entry channel. Compensation-in-flight resume (existing behavior) SHALL take
precedence unchanged. An instance with no journal (pre-upgrade or opted-out)
SHALL keep the re-run-from-top behavior.

#### Scenario: Mid-chain crash resumes at the first uncommitted procedure

- **WHEN** a chain A→B→C crashes after `proc_committed(A)` and `proc_committed(B)` were journaled
- **THEN** on respawn the driver SHALL NOT re-run A or B and SHALL run C seeded with B's journaled handoff batch

#### Scenario: Crash before the first commit re-runs from the top

- **WHEN** a chain crashes while procedure A is still running (no `proc_committed` entry)
- **THEN** on respawn the chain SHALL re-run from procedure A seeded from the entry channel (at-least-once)

#### Scenario: An instance with no per-Entity state still resumes at its iteration

- **WHEN** a durable looping instance whose procedures hold no per-Entity state (no Entity WAL, so no `RestoreInitState` is shipped) parks and is rehydrated
- **THEN** the driver SHALL enter the iteration after the last committed one, and that iteration's staged output SHALL be delivered rather than colliding with an earlier iteration's flush marker

#### Scenario: Compensation-in-flight still wins

- **WHEN** a respawned instance has outstanding compensation-intent markers
- **THEN** the driver SHALL run the remaining compensation (existing behavior) and SHALL NOT resume forward execution from the journal

### Requirement: The proc-journal shares the instance lifecycle and is inspectable

The proc-journal SHALL be deleted with the instance's state files by the
finished-instance retention sweeper, `flz state delete`, and `flz reset`. A
read-only CLI verb `flz state journal <operative/instance>` SHALL render the
decoded journal entries.

#### Scenario: Retention removes the journal with the instance data

- **WHEN** a finished workflow instance's retention window elapses
- **THEN** `proc.journal` SHALL be deleted together with the instance's other state files

#### Scenario: Operator inspects proc progress

- **WHEN** an operator runs `flz state journal <operative/instance>` for an instance with journaled commits
- **THEN** the CLI SHALL list the decoded entries (seq, type, proc index, iteration) in order

### Requirement: The proc-commit payload carries staged outbox entries

The `ProcCommit` request and the `proc_committed` journal entry SHALL carry the
procedure's staged outbox entries (`{entity_key, seq_in_proc, payload,
plugin_route}`) in addition to the entities-ran set and the handoff batch. The
journal frame size guard SHALL apply to the combined payload, with the same
typed oversized error. The `outbox_flushed` and `compensated` entry types
(previously reserved) SHALL be written and interpreted as defined by the
`workflow-transactional-outbox` capability.

#### Scenario: Commit and outbox arrive as one request

- **WHEN** a structured procedure with staged outputs reaches its barrier
- **THEN** a single `ProcCommit` request SHALL carry both the handoff batch and the outbox entries, journaled in one `proc_committed` entry

#### Scenario: Oversized combined payload is rejected with a typed error

- **WHEN** a procedure's handoff plus staged outbox exceeds the journal frame bound
- **THEN** the commit SHALL fail with the typed oversized error and the run SHALL terminate abnormally

### Requirement: Iteration and park entry types are active

The proc-journal codec SHALL write and interpret the `iter_committed` and
`parked` entry types (previously reserved): `iter_committed` carries the
iteration number and the consumed inbox offset; `parked` carries the iteration
number and an optional wake deadline. Per-proc `proc_committed` entries SHALL carry
the enclosing iteration in the `iter` field (0 for a non-looping chain). The
`flz state journal` verb SHALL render both entry types.

#### Scenario: Journal reflects loop progress

- **WHEN** a looping instance has committed two iterations and parked
- **THEN** `flz state journal` SHALL show the per-proc entries tagged with their iteration, two `iter_committed` entries with consumed offsets, and a final `parked` entry

### Requirement: The crash-resume state ships every in-flight proc's handoff

The proc-journal restore state SHALL carry the journaled handoff of **every**
procedure committed in the in-flight iteration, keyed by its `proc_idx`, so that
each uncommitted child branch re-seeds from exactly its own parent's edge —
filtered to the channel that child consumes. The restore state's iteration-level
handoff SHALL carry the previous iteration's terminal batch, which re-seeds a
handoff-chained loop. The existing frame-size guard SHALL apply unchanged to
each journaled entry.

The SDK SHALL declare its leaf procedures in the `AddOperative` response
metadata, and the controller SHALL derive operative completion (every leaf
procedure committed in the in-flight iteration) from the journal alone.

#### Scenario: Each branch re-seeds from its own parent

- **WHEN** procedure A with children B and C commits, and the instance later resumes with only B committed
- **THEN** C SHALL be seeded from A's journaled handoff, and B SHALL NOT re-run

#### Scenario: Controller derives DONE from the journal

- **WHEN** the journal holds `proc_committed` entries covering every declared leaf procedure of the in-flight iteration
- **THEN** the controller SHALL report the operative complete from the journal alone

