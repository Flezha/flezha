# bounded-operative-saga Specification

## Purpose
TBD - created by archiving change bounded-operative-functionality. Update Purpose after archive.
## Requirements
### Requirement: An operation may declare a compensation hook

An operation in a bounded operative MAY declare a compensation hook (`compensate(ctx, completed_output, state)` in Rust/Java). The hook expresses how to undo that operation's committed effect and SHALL receive the operation's own completed output and its segment state, so it can undo precisely. Declaring a compensation hook SHALL be optional per operation; operations without one SHALL be skipped during compensation.

#### Scenario: An operation declares compensation

- **WHEN** an operation that performs a reversible side effect declares a `compensate` hook
- **THEN** the hook SHALL be registered for that operation and invoked only during abnormal-termination compensation, receiving that operation's completed output and state

### Requirement: Compensation fires in reverse order, per segment

When a bounded operative terminates abnormally — an operation returns an error, or it is force-stopped after partial progress — the SDK SHALL fire the compensation hooks of already-**completed** operations in **reverse order**, back to the last commit point (see modes below). For an operation that ran across multiple segments, its hook SHALL fire **once per segment** that accumulated an effect; the order among those segments is unspecified (they are independent keys). Undo SHALL be strictly top-down: an earlier step SHALL NOT be undone while a later step that depended on it is kept. Operations that had not completed SHALL NOT be compensated, and a clean termination (`ok`) SHALL NOT trigger compensation.

#### Scenario: Reverse-order compensation on a failed step

- **WHEN** operations A then B then C completed and operation D then failed
- **THEN** the SDK SHALL compensate C, then B, then A (D is not compensated, having not completed), and the operative SHALL complete with status `error`

#### Scenario: Per-segment compensation for a parallel operation

- **WHEN** a completed operation that ran across multiple segments is compensated
- **THEN** its `compensate` hook SHALL fire once per segment that has an effect, each receiving that segment's own output and state

#### Scenario: Clean completion does not compensate

- **WHEN** a bounded operative completes all work successfully
- **THEN** no compensation hook SHALL fire

### Requirement: Rollback scope is the author's choice per operative

A bounded operative SHALL declare one of two rollback modes; the default SHALL be **all-or-nothing**:

- **all-or-nothing (default):** any failure SHALL undo **all** completed operations in reverse, back to the start.
- **keep-progress:** each successfully finished **loop iteration** SHALL commit (be kept); a failure SHALL undo only the **in-flight** iteration's completed operations, in reverse. Within a single iteration — or in a no-loop pass — rollback SHALL be all-or-nothing.

The commit boundary SHALL be the loop iteration (or the end of a no-loop pass), NEVER an individual operation, so that a classic single-pass saga (`reserve → charge → ship`) still undoes earlier steps when a later step fails.

#### Scenario: All-or-nothing undoes the whole job

- **WHEN** an all-or-nothing bounded operative completes operations Op1 and several loop iterations of Op2, then fails
- **THEN** every completed operation including all finished Op2 iterations SHALL be undone in reverse, back to Op1

#### Scenario: Keep-progress keeps finished iterations

- **WHEN** a keep-progress bounded operative finishes loop iterations 1–9 of an operation and iteration 10 fails
- **THEN** only iteration 10's completed steps SHALL be undone, and iterations 1–9 (and any operation before the loop) SHALL be kept

### Requirement: Compensation is durably recorded and crash-safe

For a durable bounded operative, the completion of each operation SHALL be durably recorded (a compensation-intent record) and each compensation's completion SHALL likewise be recorded (a compensation-done record). A crash during compensation SHALL resume the remaining hooks on respawn rather than re-running already-completed compensations. Compensation hooks SHALL be idempotent by author contract.

#### Scenario: Crash mid-compensation resumes the remainder

- **WHEN** a durable bounded operative is compensating C → B → A, crashes after C's compensation is recorded, and is respawned
- **THEN** it SHALL resume by compensating B then A, and SHALL NOT re-run C's compensation

### Requirement: Compensation undo-effects are delivered durably and keyed

Plugin-bound output emitted by a `compensate` hook SHALL NOT be delivered
best-effort from the executor for a durable bounded operative; it SHALL be
staged and delivered through the journal-gated transactional outbox
(`bounded-transactional-outbox`), so that a crash between a compensation's
completion and its undo-output delivery re-drives the delivery with stable,
comp-marked keys. The existing per-segment compensation intent/done markers
SHALL remain the sole mechanism deciding which hooks run on resume.

#### Scenario: Undo delivery survives a crash

- **WHEN** a compensation hook completes and the process crashes before its undo output reaches the plugin
- **THEN** after respawn the undo output SHALL be delivered (not lost), with the same comp-marked key, and the hook itself SHALL NOT re-run

#### Scenario: Markers still gate hook execution

- **WHEN** a respawned instance resumes a partially-complete compensation pass
- **THEN** which hooks run SHALL be decided by the per-segment intent/done markers exactly as before this change

