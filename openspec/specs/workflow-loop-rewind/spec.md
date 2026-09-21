# workflow-loop-rewind Specification

## Purpose

The workflow-loop-rewind capability lets an operator take a looping workflow operative back to a committed iteration: `flz operative rewind <instance> --to-iteration N` compensates the tasklets of every iteration above N in reverse order, restores the inter-task handoff and consumed inbox offset to the iteration-N boundary, bumps the instance's epoch so re-run output supersedes the unwound output at conforming sinks, and resumes the loop at iteration N+1. It is undo-and-continue, not replay — the unwound iterations' inputs are gone with the truncated inbox. The rewind is refused, with no durable change, when the target iteration never committed or when an unwound tasklet staged plugin output it declares no `compensate` hook to undo.

## Requirements

### Requirement: Rewind to a committed iteration

`flz operative rewind <instance> --to-iteration N` SHALL: compensate the
tasklets of iterations above N in reverse order using the existing per-cell
compensation machinery, restore the instance's inter-task handoff and consumed inbox
offset to the iteration-N boundary, bump the epoch, and resume the loop at
iteration N+1. Iteration N SHALL remain committed. Rewinding to an iteration that
has not committed SHALL be refused with a typed error reporting the committed
range, and SHALL make no durable change.

Cell **state** SHALL be returned to the boundary by the unwound tasklets'
own `compensate` hooks — the existing saga contract, in which a tasklet that
needs its effects undone declares one and reverses its own State change. The
engine SHALL NOT truncate the per-Cell state WAL to do it. A tasklet that
declares no hook therefore leaves its State change in place, exactly as it does
under an `AllOrNothing` crash rollback today.

A rewind is undo-and-continue, not replay: an iteration's **input** is not
retained past its commit (the durable inbox is truncated to the consumed offset
at each iteration boundary), so the unwound iterations are not re-run from their
original inputs. The loop resumes waiting, and the next input it receives is
processed as iteration N+1.

The rewind point SHALL be made durable before any compensation runs, so that a
crash mid-rewind resumes as a rewind rather than as a partially unwound loop.

#### Scenario: Chat rewound to an earlier turn

- **WHEN** a chat operative has committed iterations 1..5 and the operator rewinds
  to iteration 3
- **THEN** the tasklets of iterations 5 and 4 are compensated in that order,
  the handoff equals the iteration-3 commit, and the next input is processed as
  iteration 4

#### Scenario: Crash mid-rewind resumes without double compensation

- **WHEN** the process crashes after compensating iteration 5's tasklets but
  before iteration 4's
- **THEN** the resumed rewind compensates only iteration 4

#### Scenario: Rewind target must be committed

- **WHEN** the operator requests `--to-iteration 7` but only 5 iterations have
  committed
- **THEN** the verb fails with a typed error listing the committed range and the
  journal is unchanged

### Requirement: A rewound iteration's work is neither resumed nor flushed

After a rewind to iteration N, the journal SHALL fold as though the tasklets of
iterations above N had not been committed: they SHALL NOT be skipped as
already-done on resume, and any outbox entries they staged but had not yet flushed
SHALL NOT be flushed. Tasklets journaled after the rewind SHALL fold normally,
including a re-run of an iteration that was unwound.

#### Scenario: Unflushed output of an unwound iteration is not owed

- **WHEN** iteration 5 committed a tasklet whose outbox had not yet flushed, and
  the instance is rewound to iteration 3
- **THEN** that outbox is never flushed, and the re-run of iteration 4 flushes only
  its own outputs

#### Scenario: Re-run of an unwound iteration commits normally

- **WHEN** iteration 4 re-runs after a rewind to iteration 3 and commits
- **THEN** the journal folds to iteration 5 as the next iteration, and the re-run's
  tasklets are the ones resumed from

### Requirement: Rewound outputs republish under a bumped epoch

A rewind SHALL bump the instance's epoch. Outputs emitted after a rewind SHALL
carry the bumped epoch so that conforming sinks accept them as newer than the
unwound iterations' outputs. Crash-retry re-emits within an un-rewound run SHALL
keep their epoch and be deduplicated. This is the same epoch rule as stream
correction.

#### Scenario: Re-run turn's output supersedes the unwound turn's

- **WHEN** iteration 4 re-runs after a rewind and emits on a channel that iteration
  4's original run had already written to a guarded sink
- **THEN** the re-run's output carries a higher epoch than the original and the
  sink's final visible value for the affected identity is the re-run's

#### Scenario: Crash re-emit does not bump

- **WHEN** a crash re-drives an un-rewound instance's unflushed outbox
- **THEN** the re-driven outputs carry their original epoch

### Requirement: Compensation-coverage guard

A rewind SHALL be refused with a typed error when any tasklet being unwound
staged plugin output and its tasklet declares no `compensate` hook. A workflow
operative's staged plugin output carries no natural key, so a re-run's output
cannot be relied on to overwrite the original at every sink; a semantic undo is
therefore the only coverage the engine can guarantee. The error SHALL name the
uncovered channels, and no compensation and no epoch bump SHALL occur.

A tasklet that staged no plugin output SHALL require no coverage.

#### Scenario: Uncovered emitting tasklet blocks rewind

- **WHEN** iteration 5 ran a tasklet that staged output on a channel and that
  tasklet declares no `compensate`
- **THEN** the rewind is refused before any compensation runs, naming that channel,
  and the journal is unchanged

#### Scenario: Pure state iterations rewind freely

- **WHEN** the iterations being unwound staged no plugin output
- **THEN** the rewind proceeds with no coverage requirement
