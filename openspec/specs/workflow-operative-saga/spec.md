# workflow-operative-saga Specification

## Purpose
TBD - created by archiving change workflow-operative-functionality. Update Purpose after archive.
## Requirements
### Requirement: A tasklet may declare a compensation hook

A tasklet in a workflow operative MAY declare a compensation hook (`compensate(ctx, completed_output, state)` in Rust/Java). The hook expresses how to undo that tasklet's committed effect and SHALL receive the tasklet's own completed output and its cell state, so it can undo precisely. Declaring a compensation hook SHALL be optional per tasklet; tasklets without one SHALL be skipped during compensation.

#### Scenario: A tasklet declares compensation

- **WHEN** a tasklet that performs a reversible side effect declares a `compensate` hook
- **THEN** the hook SHALL be registered for that tasklet and invoked only during abnormal-termination compensation, receiving that tasklet's completed output and state

### Requirement: Compensation fires in reverse order, per cell

When a workflow operative terminates abnormally — a tasklet returns an error, or it is force-stopped after partial progress — the SDK SHALL fire the compensation hooks of already-**completed** tasklets in **reverse order**, back to the last commit point (see modes below). For a tasklet that ran across multiple cells, its hook SHALL fire **once per cell** that accumulated an effect; the order among those cells is unspecified (they are independent keys). Undo SHALL be strictly top-down: an earlier step SHALL NOT be undone while a later step that depended on it is kept. Tasklets that had not completed SHALL NOT be compensated, and a clean termination (`ok`) SHALL NOT trigger compensation.

#### Scenario: Reverse-order compensation on a failed step

- **WHEN** tasklets A then B then C completed and tasklet D then failed
- **THEN** the SDK SHALL compensate C, then B, then A (D is not compensated, having not completed), and the operative SHALL complete with status `error`

#### Scenario: Per-cell compensation for a parallel tasklet

- **WHEN** a completed tasklet that ran across multiple cells is compensated
- **THEN** its `compensate` hook SHALL fire once per cell that has an effect, each receiving that cell's own output and state

#### Scenario: Clean completion does not compensate

- **WHEN** a workflow operative completes all work successfully
- **THEN** no compensation hook SHALL fire

### Requirement: Rollback scope is the author's choice per operative

A workflow operative SHALL declare one of two rollback modes; the default SHALL be **all-or-nothing**:

- **all-or-nothing (default):** any failure SHALL undo **all** completed tasklets in reverse, back to the start.
- **keep-progress:** each successfully finished **loop iteration** SHALL commit (be kept); a failure SHALL undo only the **in-flight** iteration's completed tasklets, in reverse. Within a single iteration — or in a no-loop pass — rollback SHALL be all-or-nothing.

The commit boundary SHALL be the loop iteration (or the end of a no-loop pass), NEVER an individual tasklet, so that a classic single-pass saga (`reserve → charge → ship`) still undoes earlier steps when a later step fails.

#### Scenario: All-or-nothing undoes the whole job

- **WHEN** an all-or-nothing workflow operative completes tasklets Op1 and several loop iterations of Op2, then fails
- **THEN** every completed tasklet including all finished Op2 iterations SHALL be undone in reverse, back to Op1

#### Scenario: Keep-progress keeps finished iterations

- **WHEN** a keep-progress workflow operative finishes loop iterations 1–9 of a tasklet and iteration 10 fails
- **THEN** only iteration 10's completed steps SHALL be undone, and iterations 1–9 (and any tasklet before the loop) SHALL be kept

### Requirement: Compensation is durably recorded and crash-safe

For a durable workflow operative, the completion of each tasklet SHALL be durably recorded (a compensation-intent record) and each compensation's completion SHALL likewise be recorded (a compensation-done record). A crash during compensation SHALL resume the remaining hooks on respawn rather than re-running already-completed compensations. Compensation hooks SHALL be idempotent by author contract.

#### Scenario: Crash mid-compensation resumes the remainder

- **WHEN** a durable workflow operative is compensating C → B → A, crashes after C's compensation is recorded, and is respawned
- **THEN** it SHALL resume by compensating B then A, and SHALL NOT re-run C's compensation

### Requirement: Compensation undo-effects are delivered durably and keyed

Plugin-bound output emitted by a `compensate` hook SHALL NOT be delivered
best-effort from the runner for a durable workflow operative; it SHALL be
staged and delivered through the journal-gated transactional outbox
(`workflow-transactional-outbox`), so that a crash between a compensation's
completion and its undo-output delivery re-drives the delivery with stable,
comp-marked keys. The existing per-cell compensation intent/done markers
SHALL remain the sole mechanism deciding which hooks run on resume.

#### Scenario: Undo delivery survives a crash

- **WHEN** a compensation hook completes and the process crashes before its undo output reaches the plugin
- **THEN** after respawn the undo output SHALL be delivered (not lost), with the same comp-marked key, and the hook itself SHALL NOT re-run

#### Scenario: Markers still gate hook execution

- **WHEN** a respawned instance resumes a partially-complete compensation pass
- **THEN** which hooks run SHALL be decided by the per-cell intent/done markers exactly as before this change

