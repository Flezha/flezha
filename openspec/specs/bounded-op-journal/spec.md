# bounded-op-journal Specification

## Purpose

The bounded-op-journal capability is the controller-side durable op-progress store for durable structured bounded operatives: one append-only `op.journal` per instance on the shared WalWriter/FsyncBatcher substrate, recording each completed operation and its serialized inter-op handoff batch. At every operation barrier the SDK ships an `OpCommit` control-channel request and blocks until the controller has fsynced the `op_committed` entry and replied — the durable point later changes gate staged output flushes and iteration commits on. On crash-resume the journal state rides the bind itself (`AddOperative.op_journal`) — it is instance-scoped, and a class with no per-Segment state receives no per-Segment restore at all — so committed operations are skipped (not redone) and the first uncommitted operation is re-seeded from the journaled handoff; compensation-in-flight resume takes precedence unchanged. Inter-operation channels of durable structured classes carry a symmetric byte codec so the handoff is journal-able; the journal shares the instance's file lifecycle (retention, delete, reset) and is inspectable via `flz state journal`. Opted-out classes (`PERSISTENCE_ENABLED = false`) write no journal and keep the re-run-from-top contract.

## Requirements

### Requirement: Each durable bounded instance has an append-only op-journal on the shared state substrate

The controller SHALL maintain one append-only op-journal file per durable bounded
operative instance at `app_data/state/{operative_class}/{instance_id}/op.journal`,
written through the existing WalWriter framing and FsyncBatcher path (no parallel
persistence machinery). Journal entries SHALL be typed, length-prefixed frames
carrying `{seq, type, op_idx, iter, payload}`. This capability ships the
`op_committed` entry type; the codec SHALL tolerate the reserved entry types
(`outbox_flushed`, `iter_committed`, `parked`, `compensated`) without crashing. A
torn trailing frame SHALL be truncated on open. An opted-out class
(`PERSISTENCE_ENABLED = false`) SHALL produce no journal file.

#### Scenario: Journal file lives with the instance's state files

- **WHEN** a durable structured bounded operative commits its first operation
- **THEN** `op.journal` SHALL exist in the instance's state directory alongside its segment WAL files, and its first decoded entry SHALL be `op_committed` for op index 0

#### Scenario: Torn tail is recovered

- **WHEN** the controller crashes mid-append leaving a partial final frame and the journal is reopened
- **THEN** the partial frame SHALL be discarded and every complete prior entry SHALL remain readable

#### Scenario: Opted-out class writes no journal

- **WHEN** a bounded class with `PERSISTENCE_ENABLED = false` runs a structured chain to completion
- **THEN** no `op.journal` file SHALL be created for the instance

### Requirement: Op-boundary commit is a gated handshake

The SDK SHALL send, at each operation barrier of a durable structured bounded
operative, an `OpCommit` control-channel request carrying the operation index,
the set of segments that ran, and the serialized inter-op handoff batch. The
controller SHALL append the `op_committed` entry, make it durable (fsync), and
only then reply `OpCommitResponse`. The driver SHALL NOT start the next
operation before receiving the successful response. A failed journal write SHALL
be reported in the response and SHALL terminate the run abnormally (existing
terminal-error path, including compensation). Opted-out classes SHALL skip the
handshake entirely.

#### Scenario: Next operation waits for the durable ack

- **WHEN** operation A's segments all quiesce at the barrier
- **THEN** operation B SHALL start only after the controller has durably journaled `op_committed(A)` and acked it

#### Scenario: Journal write failure is a terminal run error

- **WHEN** the controller cannot append the `op_committed` entry (e.g. disk error)
- **THEN** the `OpCommitResponse` SHALL carry the error and the operative SHALL terminate abnormally, compensating completed operations per the existing saga contract

### Requirement: Inter-op channels of a durable structured class carry a byte codec

Inter-operation channels SHALL support a symmetric byte codec (typed→bytes and
bytes→typed) so the handoff batch can be journaled and re-seeded. Registering a
**durable** structured bounded class with an inter-op channel that lacks the
codec SHALL fail at registration with an error naming the channel. An oversized
handoff batch (above the configured journal frame bound) SHALL fail the commit
with a typed error directing the author to keep bulk data in Segment state.

#### Scenario: Missing codec is rejected at registration

- **WHEN** a durable structured bounded class declares an inter-op channel without a byte codec
- **THEN** registration SHALL fail with an error identifying that channel

#### Scenario: Oversized handoff fails the commit with a typed error

- **WHEN** an operation's serialized handoff batch exceeds the journal frame bound
- **THEN** the op commit SHALL fail with an error identifying the oversized channel/batch, and the run SHALL terminate abnormally

### Requirement: Crash-resume skips journaled operations and re-seeds from the journaled handoff

On respawn of a durable structured bounded instance, the controller SHALL ship
the journal state (the resume iteration, committed op indexes, and the latest
journaled handoff) to the SDK **on the bind** (`AddOperative.op_journal`), which
is instance-scoped and therefore reaches a class that has no per-Segment state
and so receives no `RestoreInitState`. When no compensation is in flight, the
driver SHALL skip every operation recorded `op_committed` and SHALL seed the
first uncommitted operation's input batch from the journaled handoff instead of
the entry channel. Compensation-in-flight resume (existing behavior) SHALL take
precedence unchanged. An instance with no journal (pre-upgrade or opted-out)
SHALL keep the re-run-from-top behavior.

#### Scenario: Mid-chain crash resumes at the first uncommitted operation

- **WHEN** a chain A→B→C crashes after `op_committed(A)` and `op_committed(B)` were journaled
- **THEN** on respawn the driver SHALL NOT re-run A or B and SHALL run C seeded with B's journaled handoff batch

#### Scenario: Crash before the first commit re-runs from the top

- **WHEN** a chain crashes while operation A is still running (no `op_committed` entry)
- **THEN** on respawn the chain SHALL re-run from operation A seeded from the entry channel (at-least-once)

#### Scenario: An instance with no per-Segment state still resumes at its iteration

- **WHEN** a durable looping instance whose operations hold no per-Segment state (no Segment WAL, so no `RestoreInitState` is shipped) parks and is rehydrated
- **THEN** the driver SHALL enter the iteration after the last committed one, and that iteration's staged output SHALL be delivered rather than colliding with an earlier iteration's flush marker

#### Scenario: Compensation-in-flight still wins

- **WHEN** a respawned instance has outstanding compensation-intent markers
- **THEN** the driver SHALL run the remaining compensation (existing behavior) and SHALL NOT resume forward execution from the journal

### Requirement: The op-journal shares the instance lifecycle and is inspectable

The op-journal SHALL be deleted with the instance's state files by the
finished-instance retention sweeper, `flz state delete`, and `flz reset`. A
read-only CLI verb `flz state journal <operative/instance>` SHALL render the
decoded journal entries.

#### Scenario: Retention removes the journal with the instance data

- **WHEN** a finished bounded instance's retention window elapses
- **THEN** `op.journal` SHALL be deleted together with the instance's other state files

#### Scenario: Operator inspects op progress

- **WHEN** an operator runs `flz state journal <operative/instance>` for an instance with journaled commits
- **THEN** the CLI SHALL list the decoded entries (seq, type, op index, iteration) in order

### Requirement: The op-commit payload carries staged outbox entries

The `OpCommit` request and the `op_committed` journal entry SHALL carry the
operation's staged outbox entries (`{segment_key, seq_in_op, payload,
plugin_route}`) in addition to the segments-ran set and the handoff batch. The
journal frame size guard SHALL apply to the combined payload, with the same
typed oversized error. The `outbox_flushed` and `compensated` entry types
(previously reserved) SHALL be written and interpreted as defined by the
`bounded-transactional-outbox` capability.

#### Scenario: Commit and outbox arrive as one request

- **WHEN** a structured operation with staged outputs reaches its barrier
- **THEN** a single `OpCommit` request SHALL carry both the handoff batch and the outbox entries, journaled in one `op_committed` entry

#### Scenario: Oversized combined payload is rejected with a typed error

- **WHEN** an operation's handoff plus staged outbox exceeds the journal frame bound
- **THEN** the commit SHALL fail with the typed oversized error and the run SHALL terminate abnormally

### Requirement: Iteration and park entry types are active

The op-journal codec SHALL write and interpret the `iter_committed` and
`parked` entry types (previously reserved): `iter_committed` carries the
iteration number and the consumed inbox offset; `parked` carries the iteration
number and an optional wake deadline. Per-op `op_committed` entries SHALL carry
the enclosing iteration in the `iter` field (0 for a non-looping chain). The
`flz state journal` verb SHALL render both entry types.

#### Scenario: Journal reflects loop progress

- **WHEN** a looping instance has committed two iterations and parked
- **THEN** `flz state journal` SHALL show the per-op entries tagged with their iteration, two `iter_committed` entries with consumed offsets, and a final `parked` entry

### Requirement: The crash-resume state ships every in-flight op's handoff

The op-journal restore state SHALL carry the journaled handoff of **every**
operation committed in the in-flight iteration, keyed by its `op_idx`, so that
each uncommitted child branch re-seeds from exactly its own parent's edge —
filtered to the channel that child consumes. The restore state's iteration-level
handoff SHALL carry the previous iteration's terminal batch, which re-seeds a
handoff-chained loop. The existing frame-size guard SHALL apply unchanged to
each journaled entry.

The SDK SHALL declare its leaf operations in the `AddOperative` response
metadata, and the controller SHALL derive operative completion (every leaf
operation committed in the in-flight iteration) from the journal alone.

#### Scenario: Each branch re-seeds from its own parent

- **WHEN** operation A with children B and C commits, and the instance later resumes with only B committed
- **THEN** C SHALL be seeded from A's journaled handoff, and B SHALL NOT re-run

#### Scenario: Controller derives DONE from the journal

- **WHEN** the journal holds `op_committed` entries covering every declared leaf operation of the in-flight iteration
- **THEN** the controller SHALL report the operative complete from the journal alone
