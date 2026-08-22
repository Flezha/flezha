# workflow-durable-loop Specification

## Purpose

The workflow-durable-loop capability turns the structured workflow chain into a durable loop: a class may declare its chain to repeat, one iteration seeded from the next durable inbox item or from the previous iteration's handoff, with each iteration running under the existing per-proc commit gating (`workflow-proc-journal`) and ending in an iteration commit. The commit tuple is one durable point plus its consequences — the controller appends `iter_committed(N, consumed_inbox_offset)` to the instance's `proc.journal`, requests iteration-boundary snapshot compaction of the instance's entities through the existing CompactEntity machinery, and flushes the iteration's staged outbox (`workflow-transactional-outbox`). Per-proc journal entries carry the enclosing iteration so a resumed instance re-enters at iteration `N+1` — never at the loop start — restored from the boundary snapshots (or WAL-tail replay) and re-seeded from the recorded consumed offset; only an uncommitted in-flight iteration may re-run (at-least-once, unchanged author contract). Because committed iterations are journal-backed, the saga planner's `committed` input is finally real: `KeepProgress` keeps every journaled iteration and undoes only the in-flight one, while all-or-nothing unwinds the in-flight iteration and then every journaled iteration in reverse, reusing `plan_compensation`'s ordering contract unchanged. Loop termination (terminal predicate, cancellation, `complete`) is preserved from the existing loop contract.

## Requirements

### Requirement: A structured workflow chain can loop with a durable per-iteration commit

A structured workflow class SHALL be able to declare its chain as a loop: each
iteration seeds from the next inbox item or the previous iteration's handoff,
runs the chain under the existing per-proc commit gating, and ends with an
iteration commit — the controller SHALL append `iter_committed(N,
consumed_inbox_offset)` to the proc-journal, trigger boundary snapshot compaction
for the instance's entities (existing CompactEntity machinery), and flush the
iteration's staged outbox. Per-proc journal entries within iteration N SHALL
carry `iter = N`. Loop termination semantics (terminal predicate, cancellation,
`complete`) SHALL be preserved from the existing loop contract.

#### Scenario: Turn ends with journal + snapshot + flush

- **WHEN** a looping workflow operative finishes iteration N
- **THEN** the journal SHALL hold `iter_committed(N)` with the consumed inbox offset, the entities SHALL have a boundary snapshot requested, and iteration N's outbox SHALL be flushed

#### Scenario: The boundary snapshot lands for a workflow Entity

- **WHEN** two iterations have committed for a looping instance whose procedure holds Entity state
- **THEN** a snapshot file SHALL exist for that Entity and the WAL SHALL hold only the records above it

#### Scenario: Snapshot failure does not lose the commit

- **WHEN** the boundary snapshot compaction fails after `iter_committed(N)` is durable
- **THEN** the iteration SHALL remain committed and a later rehydrate SHALL fall back to WAL-tail replay for the affected entities

### Requirement: Crash-resume enters the iteration after the last committed one

On respawn of a looping durable workflow instance, the driver SHALL resume at
iteration `N+1` where `N` is the highest journaled `iter_committed` — never at
the loop start — with entity state restored from the iteration-boundary
snapshots (or WAL replay) and input delivered from the recorded consumed
offset. Work of an uncommitted in-flight iteration MAY re-run (at-least-once,
existing author contract).

#### Scenario: Resume from the last committed iteration

- **WHEN** a looping instance crashes during iteration 3 after `iter_committed(2)` was journaled
- **THEN** on respawn it SHALL resume iteration 3 (re-seeded from inbox offset recorded at iteration 2's commit), and iterations 1–2 SHALL NOT re-run

### Requirement: KeepProgress rollback is journal-backed

The compensation planner's `committed` input SHALL, for an abnormal
termination of a looping durable instance, be built from journaled `iter_committed`
markers: **keep-progress** SHALL keep every journaled iteration and undo only
the in-flight one; **all-or-nothing** SHALL undo the in-flight iteration and
then every journaled iteration in reverse. The existing `plan_compensation`
ordering contract SHALL be reused unchanged.

#### Scenario: Keep-progress keeps journaled iterations across a crash

- **WHEN** a keep-progress looping instance has journaled iterations 1–4, crashes, resumes, and iteration 5 then fails terminally
- **THEN** only iteration 5's completed procedures SHALL be compensated and iterations 1–4 SHALL be kept

#### Scenario: All-or-nothing unwinds journaled iterations too

- **WHEN** an all-or-nothing looping instance with journaled iterations 1–2 fails in iteration 3
- **THEN** compensation SHALL undo iteration 3's completed procedures, then iterations 2 and 1, in reverse completion order
