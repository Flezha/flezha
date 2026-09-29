# bounded-loop-rewind Specification

## Purpose

The bounded-loop-rewind capability lets a user take a looping bounded operator back to a committed iteration: `flz operator rewind <instance> --to-iteration N` compensates the tasklets of every iteration above N in reverse order, restores the inter-task handoff and consumed inbox offset to the iteration-N boundary, bumps the instance's epoch so re-run output supersedes the unwound output at conforming sinks, and resumes the loop at iteration N+1. It is undo-and-continue, not replay — the unwound iterations' inputs are gone with the truncated inbox. The rewind is refused, with no durable change, when the target iteration never committed or when an unwound tasklet staged plugin output it declares no `compensate` hook to undo.
## Requirements
### Requirement: Rewind to a committed iteration

`flz operator rewind <instance> --to-iteration N` SHALL: compensate the
tasklets of iterations above N in reverse order using the existing per-cell
compensation machinery, restore the instance's inter-task handoff and consumed
input-log offset to the iteration-N boundary, bump the epoch, and resume the loop at
iteration N+1. Iteration N SHALL remain committed. Rewinding to an iteration that
has not committed SHALL be refused with a typed error reporting the committed
range, and SHALL make no durable change.

Cell **state** SHALL be returned to the boundary by the unwound tasklets'
own `compensate` hooks, the existing saga contract, in which a tasklet that
needs its effects undone declares one and reverses its own State change. The
engine SHALL NOT truncate the per-Cell state WAL to do it. A tasklet that
declares no hook therefore leaves its State change in place, exactly as it does
under an `AllOrNothing` crash rollback today.

A rewind is undo-and-continue, not replay: an iteration's **input** is released
to the input log's retention at its commit, and the instance's floor in that
log, the highest offset any committed iteration consumed, SHALL NOT be lowered
by a rewind, so the unwound iterations are not re-run from their original
inputs even while the log still retains them. The loop resumes waiting, and the
next input it receives is processed as iteration N+1.

The rewind point SHALL be made durable before any compensation runs, so that a
crash mid-rewind resumes as a rewind rather than as a partially unwound loop.

#### Scenario: Chat rewound to an earlier turn

- **WHEN** a chat operator has committed iterations 1..5 and the user rewinds
  to iteration 3
- **THEN** the tasklets of iterations 5 and 4 are compensated in that order,
  the handoff equals the iteration-3 commit, and the next input is processed as
  iteration 4

#### Scenario: Retained input is not re-delivered by a rewind

- **WHEN** the user rewinds to iteration 3 while the input log still retains
  the inputs iterations 4 and 5 consumed
- **THEN** the instance's floor stays at the offset iteration 5 consumed and
  neither input is forwarded again

#### Scenario: Crash mid-rewind resumes without double compensation

- **WHEN** the process crashes after compensating iteration 5's tasklets but
  before iteration 4's
- **THEN** the resumed rewind compensates only iteration 4

#### Scenario: Rewind target must be committed

- **WHEN** the user requests `--to-iteration 7` but only 5 iterations have
  committed
- **THEN** the verb fails with a typed error listing the committed range and the
  journal is unchanged

### Requirement: A rewound iteration's work is neither resumed nor flushed

After a rewind to iteration N, the journal SHALL fold as though the tasklets of
iterations above N had not been committed: they SHALL NOT be skipped as
already-done on resume, and the effect records those tasklets wrote whose outputs had not yet
been handed to the plugin chain SHALL NOT be released: the rebind that follows the rewind
SHALL re-derive each Cell writer's committed records from the folded journal's live entries
and drop the queued entries no live entry names. An output already handed to the chain before the rewind SHALL be left to its
confirmation, and the unwound tasklet's `compensate` hook is what reverses it. Tasklets
journaled after the rewind SHALL fold normally, including a re-run of an iteration that was
unwound, and their records SHALL be released as any committed stage's are.

#### Scenario: Unreleased output of an unwound iteration is dropped

- **WHEN** iteration 5 committed a tasklet whose Cell record's output had not yet reached the
  plugin chain, and the instance is rewound to iteration 3
- **THEN** that output is never released, the rebind drops it from the writer's queue, and the
  re-run of iteration 4 releases only its own records' outputs

#### Scenario: Re-run of an unwound iteration commits normally

- **WHEN** iteration 4 re-runs after a rewind to iteration 3 and commits
- **THEN** the journal folds to iteration 5 as the next iteration, and the re-run's
  tasklets are the ones resumed from

### Requirement: Rewound outputs republish under a bumped epoch

A rewind SHALL bump the instance's journal epoch, which keys every later barrier's identity so
a re-run's `TaskCommit` for a `(task_idx, iter)` the rewind unwound is a new entry and not a
re-send of the old one. Outputs carry no journal epoch: an output's `flz.epoch` is its Cell's
realign epoch, unchanged by a rewind, and an output emitted after a rewind is a newer record of
the same Cell, so its `flz.seq` sorts above every output of the unwound iterations and a
conforming sink accepts it as newer. Crash-retry re-emits within an un-rewound run SHALL keep
their identity and be deduplicated. This is the same ordering rule an unbounded operator's outputs follow,
and a stream correction's epoch bump stays the one thing that moves a Cell's `flz.epoch`.

#### Scenario: A re-run turn's output sorts above the unwound turn's

- **WHEN** iteration 4 re-runs after a rewind and emits on a channel that iteration
  4's original run had already written to a guarded sink
- **THEN** the re-run's output carries the same `flz.epoch` and a higher `flz.seq` than the original, and the
  sink's final visible value for the affected identity is the re-run's

#### Scenario: A re-sent barrier after a rewind is a new entry

- **WHEN** iteration 4 re-runs after a rewind to 3 and its first tasklet commits
- **THEN** its `task_committed(0, iter 4)` entry carries the bumped journal epoch and is appended, not answered from the unwound entry

#### Scenario: Crash re-emit keeps its identity

- **WHEN** a crash re-drives an un-rewound instance's undelivered outputs
- **THEN** the re-driven outputs carry their original `flz.cell`, `flz.seq` and `flz.epoch`

### Requirement: Compensation-coverage guard

A rewind SHALL be refused with a typed error when any tasklet being unwound
wrote plugin output and its tasklet declares no `compensate` hook. A rewind is undo-and-continue:
nothing re-runs over the unwound iterations' inputs, so no later output overwrites the original
at a sink by its key, and a semantic undo is
therefore the only coverage the engine can guarantee. The channels a tasklet wrote on SHALL be
read from the journal's stage entries, which name them per Cell, never from the Cell WALs,
which compaction may have truncated. The error SHALL name
the uncovered channels, and no compensation and no epoch bump SHALL occur.

A tasklet that wrote no plugin output SHALL require no coverage.

#### Scenario: Uncovered emitting tasklet blocks rewind

- **WHEN** iteration 5 ran a tasklet that wrote output on a channel and that
  tasklet declares no `compensate`
- **THEN** the rewind is refused before any compensation runs, naming that channel,
  and the journal is unchanged

#### Scenario: Pure state iterations rewind freely

- **WHEN** the iterations being unwound wrote no plugin output
- **THEN** the rewind proceeds with no coverage requirement

#### Scenario: The guard reads the journal after the Cell WAL was compacted

- **WHEN** a compensable class's Cell was compacted past the records iteration 5 wrote, and the user rewinds to iteration 3
- **THEN** the guard still names the channels iteration 5 wrote on, from the `task_committed` entries alone

### Requirement: A rewind SHALL treat a dead-lettered iteration as committed and already undone

A `dead_lettered` iteration SHALL count as a committed iteration for `flz operator rewind`:
it SHALL be a valid `--to-iteration` target, it SHALL fall inside the committed range a
refusal reports, and the loop resumed after a rewind to it SHALL take its next input as the
following iteration. A rewind to an iteration below it SHALL NOT compensate any of its
tasklets a second time, because each undo the failure ran is a journaled `compensated`
entry, and an undo the failure still owes when the rewind is asked SHALL run once. The
compensation-coverage guard SHALL NOT refuse a rewind over a dead-lettered iteration for a
tasklet without a hook, because that iteration's outputs were settled when it failed. A
rewind SHALL NOT touch the dead-letter queue.

#### Scenario: Rewinding past a failed turn undoes nothing twice

- **GIVEN** a keep-progress session with iterations 1 and 2 committed, 3 dead-lettered after
  its first tasklet's undo ran, and 4 committed
- **WHEN** the user rewinds to iteration 1
- **THEN** iterations 4 and 2 SHALL be compensated, iteration 3's first tasklet SHALL NOT be
  compensated again, the DLQ entry of iteration 3 SHALL stay, and the next input SHALL be
  iteration 2

#### Scenario: A failed turn is a valid target

- **WHEN** the user rewinds to a dead-lettered iteration 3 of a session that has committed 5
- **THEN** iterations 5 and 4 SHALL be compensated and the next input SHALL be iteration 4

