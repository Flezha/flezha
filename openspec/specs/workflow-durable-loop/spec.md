# workflow-durable-loop Specification

## Purpose

The workflow-durable-loop capability turns the structured workflow chain into a durable loop: a class may declare its chain to repeat, one iteration seeded from the next durable inbox item or from the previous iteration's handoff, with each iteration running under the existing per-task commit gating (`workflow-task-journal`) and ending in an iteration commit. The commit tuple is one durable point plus its consequences — the controller appends `iter_committed(N, consumed_inbox_offset)` to the instance's `task.journal`, requests iteration-boundary snapshot compaction of the instance's cells through the existing CompactCell machinery, and flushes the iteration's staged outbox (`workflow-transactional-outbox`). Per-task journal entries carry the enclosing iteration so a resumed instance re-enters at iteration `N+1` — never at the loop start — restored from the boundary snapshots (or WAL-tail replay) and re-seeded from the recorded consumed offset; only an uncommitted in-flight iteration may re-run (at-least-once, unchanged author contract). Because committed iterations are journal-backed, the saga planner's `committed` input is finally real: `KeepProgress` keeps every journaled iteration and undoes only the in-flight one, while all-or-nothing unwinds the in-flight iteration and then every journaled iteration in reverse, reusing `plan_compensation`'s ordering contract unchanged. Loop termination (terminal predicate, cancellation, `complete`) is preserved from the existing loop contract.
## Requirements
### Requirement: A structured workflow chain can loop with a durable per-iteration commit

A structured workflow class SHALL be able to declare its chain as a loop: each
iteration seeds from the next inbox item or the previous iteration's handoff,
runs the chain under the existing per-task commit gating, and ends with an
iteration commit — the controller SHALL append `iter_committed(N,
consumed_inbox_offset)` to the task-journal, trigger boundary snapshot compaction
for the instance's cells (existing CompactCell machinery), and flush the
iteration's staged outbox. The `iter_committed` append SHALL be made durable
by the `FsyncBatcher` tick like a task commit, and its ack SHALL follow the
sync. Once `iter_committed(N)` is durable the controller SHALL cut the
task-journal below the oldest entry its fold still needs — the pending outbox
entries, the latest `rewound` marker with the commits it has yet to undo, and
the latest `iter_committed` that carries the resume point and the epoch — so
the journal a loop keeps is bounded by one iteration plus a settled prefix
below the cut threshold (64 KiB by default), never by its history. The cut
SHALL be the journal writer's own work, off the process that acks, and a
class whose tasklets declare compensation SHALL keep its history (the saga
and a rewind replan from it). The durable inbox append that precedes a turn
SHALL NOT re-read the inbox file or fold the journal: the inbox writer SHALL
keep the dedup window in memory — the entries still in the file, rebuilt from
the file when the writer opens and released at every truncate — and the
inbox release at an iteration commit SHALL rewrite the file only once the
released prefix exceeds the cut threshold. Per-task journal entries within
iteration N SHALL carry `iter = N`. Loop termination semantics (terminal
predicate, cancellation, `complete`) SHALL be preserved from the existing
loop contract.

#### Scenario: Turn ends with journal + snapshot + flush

- **WHEN** a looping workflow operative finishes iteration N
- **THEN** the journal SHALL hold `iter_committed(N)` with the consumed inbox offset, the cells SHALL have a boundary snapshot requested, and iteration N's outbox SHALL be flushed

#### Scenario: The journal does not grow with the turns

- **WHEN** a looping workflow operative with no compensable tasklet has committed 1 000 iterations whose outboxes were all flushed
- **THEN** its task-journal SHALL hold the entries of the current iteration plus at most the cut threshold of settled prefix, and a rehydrate SHALL restore from them the resume point, consumed offset, handoff and epoch a full history would have given

#### Scenario: A class with compensable tasklets keeps its history

- **WHEN** a looping workflow operative whose tasklets declare `compensate` commits an iteration
- **THEN** its task-journal SHALL NOT be cut, so an all-or-nothing rollback and a rewind still replan from every committed iteration

#### Scenario: The inbox append costs the same at turn 1 and turn 10 000

- **WHEN** a turn is ingested for a loop that has consumed 10 000 turns
- **THEN** the append SHALL consult the writer's dedup window and SHALL NOT read the inbox file or fold the journal, and the ingest SHALL be answered only once the append's batch has synced

#### Scenario: A re-sent message is absorbed across a writer restart

- **WHEN** the inbox writer is reopened and a message already in the undelivered window is sent again
- **THEN** the append SHALL answer the existing entry's offset from the window rebuilt off the file, and the operative SHALL receive the message once

#### Scenario: Snapshot failure does not lose the commit

- **WHEN** the boundary snapshot compaction fails after `iter_committed(N)` is durable
- **THEN** the iteration SHALL remain committed and a later rehydrate SHALL fall back to WAL-tail replay for the affected cells

### Requirement: Crash-resume enters the iteration after the last committed one

On respawn of a looping durable workflow instance, the driver SHALL resume at
iteration `N+1` where `N` is the highest journaled `iter_committed` — never at
the loop start — with cell state restored from the iteration-boundary
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
- **THEN** only iteration 5's completed tasklets SHALL be compensated and iterations 1–4 SHALL be kept

#### Scenario: All-or-nothing unwinds journaled iterations too

- **WHEN** an all-or-nothing looping instance with journaled iterations 1–2 fails in iteration 3
- **THEN** compensation SHALL undo iteration 3's completed tasklets, then iterations 2 and 1, in reverse completion order

### Requirement: A tasklet ends its workflow after the iteration commits

A tasklet running inside a workflow operative SHALL be able to call `ctx.complete(result)` on its own tasklet context, in every SDK. The call SHALL NOT stop the iteration: every remaining stage of the iteration SHALL run, its outputs SHALL be staged and committed as usual, and once the iteration's commit is durable the instance SHALL end with status `ok` and `result`, instead of waiting for the next inbox item, re-seeding a chained loop, or parking. The completion SHALL travel with the iteration commit (`IterCommit`) and be journaled in its `iter_committed` entry, so an instance that crashes after that commit is durable and before its `WorkflowCompleted` is sent SHALL, on resume, end with the same result rather than wait for input. The completing tasklet's own completion SHALL also travel with its task commit (`TaskCommit`) and be journaled in its `task_committed` entry, and a resume that skips that committed tasklet SHALL end the resumed iteration with it, so a crash between the two commits loses nothing. In a `once` workflow the call SHALL set the result the single pass ends with.

When more than one cell calls `ctx.complete` in one iteration, the instance SHALL end once, with the result of the call whose tasklet is latest in the chain's commit order and, within that tasklet, whose cell key sorts last, so the result does not depend on scheduling. A tasklet that calls `ctx.complete` and then fails SHALL fail its iteration as any failure does, and the completion SHALL be discarded with the iteration.

#### Scenario: A chat agent ends itself

- **WHEN** a `PerInput` workflow whose one tasklet calls `ctx.complete({"turns": n})` on a message whose kind is `end` receives three messages and then that message
- **THEN** four iterations SHALL commit, the instance SHALL end with status `ok` and result `{"turns": n}`, and it SHALL NOT park after the fourth

#### Scenario: A completion survives a crash after the commit

- **WHEN** an instance crashes after `iter_committed(N)` carrying a completion is durable and before `WorkflowCompleted` reaches the controller
- **THEN** the rehydrated instance SHALL end with the journaled result and SHALL NOT wait for input

#### Scenario: A completion survives a crash between the two commits

- **WHEN** an instance crashes after the completing tasklet's `task_committed` entry is durable and before its iteration's `iter_committed`
- **THEN** the resumed iteration SHALL skip that tasklet, run the tasklets still uncommitted, commit with the journaled completion, and end with its result without waiting for input

#### Scenario: A failed iteration discards its completion

- **WHEN** a tasklet calls `ctx.complete(result)` and a later stage of the same iteration fails after its retry ladder
- **THEN** the iteration SHALL fail and compensate per the rollback mode, and the instance SHALL end with status `error`, not with `result`

#### Scenario: Two completions in one iteration end the instance once

- **WHEN** two cells of one tasklet call `ctx.complete` with different results in one iteration
- **THEN** the instance SHALL end once, with the result of the cell whose key sorts last

