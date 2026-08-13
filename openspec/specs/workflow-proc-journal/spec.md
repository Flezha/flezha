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
controller SHALL append the `proc_committed` entry, make it durable (fsync), and
only then reply `ProcCommitResponse`. The driver SHALL NOT start the next
procedure before receiving the successful response. A failed journal write SHALL
be reported in the response and SHALL terminate the run abnormally (existing
terminal-error path, including compensation). Opted-out classes SHALL skip the
handshake entirely.

#### Scenario: Next procedure waits for the durable ack

- **WHEN** procedure A's entities all quiesce at the barrier
- **THEN** procedure B SHALL start only after the controller has durably journaled `proc_committed(A)` and acked it

#### Scenario: Journal write failure is a terminal run error

- **WHEN** the controller cannot append the `proc_committed` entry (e.g. disk error)
- **THEN** the `ProcCommitResponse` SHALL carry the error and the operative SHALL terminate abnormally, compensating completed procedures per the existing saga contract

### Requirement: Inter-proc channels of a durable structured class carry a byte codec

Inter-procedure channels SHALL support a symmetric byte codec (typed→bytes and
bytes→typed) so the handoff batch can be journaled and re-seeded. Registering a
**durable** structured workflow class with an inter-proc channel that lacks the
codec SHALL fail at registration with an error naming the channel. An oversized
handoff batch (above the configured journal frame bound) SHALL fail the commit
with a typed error directing the author to keep bulk data in Entity state.

#### Scenario: Missing codec is rejected at registration

- **WHEN** a durable structured workflow class declares an inter-proc channel without a byte codec
- **THEN** registration SHALL fail with an error identifying that channel

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
