# bounded-durable-loop Specification

## Purpose

The bounded-durable-loop capability turns the structured bounded chain into a durable loop: a class may declare its chain to repeat, one iteration seeded from the next durable inbox item or from the previous iteration's handoff, with each iteration running under the existing per-task commit gating (`bounded-task-journal`) and ending in an iteration commit. The commit tuple is one durable point plus its consequences — the controller appends `iter_committed(N, consumed_inbox_offset)` to the instance's `task.journal`, and then reports the consumed offset into the input log's watermark; the iteration's stage outputs leave through their Cells' own outboxes as each stage commits, and the Watchdog's size and age triggers compact the instance's Cells. Per-task journal entries carry the enclosing iteration so a resumed instance re-enters at iteration `N+1` — never at the loop start — restored from the boundary snapshots (or WAL-tail replay) and re-seeded from the recorded consumed offset; only an uncommitted in-flight iteration may re-run (at-least-once, unchanged author contract). Because committed iterations are journal-backed, the saga planner's `committed` input is finally real: `KeepProgress` keeps every journaled iteration and undoes only the in-flight one, while all-or-nothing unwinds the in-flight iteration and then every journaled iteration in reverse, reusing `plan_compensation`'s ordering contract unchanged. Loop termination (terminal predicate, cancellation, `complete`) is preserved from the existing loop contract.
## Requirements
### Requirement: A structured bounded chain can loop with a durable per-iteration commit

A structured bounded class SHALL be able to declare its chain as a loop: each
iteration seeds from the next input-log record or the previous iteration's handoff,
runs the chain under the existing per-task commit gating, and ends with an
iteration commit. The controller SHALL append `iter_committed(N, consumed_offset)` to the
task-journal, where `consumed_offset` is the `flz.offset` of the input that seeded the
iteration or is absent when nothing journaled seeded it. The `iter_committed` append SHALL be made durable
by the `FsyncBatcher` tick like a task commit, and its ack SHALL follow the
sync. The iteration commit SHALL request no boundary snapshot and SHALL flush no outbox of its
own: the iteration's outputs left through the Cells' own outboxes as each stage committed, and
the instance's Cells are compacted by the Watchdog's size and age triggers as an unbounded operator's are, a
rehydrate that finds no snapshot replaying the WAL tail. Once `iter_committed(N)` is durable the controller SHALL cut the
task-journal below the oldest entry its fold still needs, the
latest `rewound` marker with the commits it has yet to undo, and
the latest `iter_committed` that carries the resume point and the epoch, so
the journal a loop keeps is bounded by one iteration plus a settled prefix
below the cut threshold (64 KiB by default), never by its history. The cut
SHALL be the journal writer's own work, off the process that acks, and a
class whose tasklets declare compensation SHALL keep its history (the saga
and a rewind replan from it). The input-log append that precedes a turn
SHALL NOT re-read the log file or fold the journal: the log SHALL keep its
message-id index in memory, over the records it retains, rebuilt from the file
once and trimmed at every truncation. Once `iter_committed(N)` is durable the
controller SHALL report its consumed offset into the input log's watermark under
the journal's identity; the release of the consumed prefix is then the log's
retention, which SHALL rewrite the file only once the released prefix exceeds
the cut threshold. Per-task journal entries within
iteration N SHALL carry `iter = N`. Loop termination semantics (terminal
predicate, cancellation, `complete`) SHALL be preserved from the existing
loop contract.

#### Scenario: Turn ends with the journal entry and the watermark

- **WHEN** a looping bounded operator finishes iteration N
- **THEN** the journal SHALL hold `iter_committed(N)` with the consumed input-log offset, the instance's watermark row SHALL be at that offset, and no `CompactCell` SHALL have been requested for the boundary

#### Scenario: The journal does not grow with the turns

- **WHEN** a looping bounded operator with no compensable tasklet has committed 1 000 iterations
- **THEN** its task-journal SHALL hold the entries of the current iteration plus at most the cut threshold of settled prefix, and a rehydrate SHALL restore from them the resume point, consumed offset, handoff and epoch a full history would have given

#### Scenario: A class with compensable tasklets keeps its history

- **WHEN** a looping bounded operator whose tasklets declare `compensate` commits an iteration
- **THEN** its task-journal SHALL NOT be cut, so an all-or-nothing rollback and a rewind still replan from every committed iteration

#### Scenario: The inbox append costs the same at turn 1 and turn 10 000

- **WHEN** a turn is ingested for a loop that has consumed 10 000 turns
- **THEN** the append SHALL consult the log's in-memory id index and SHALL NOT read the log file or fold the journal, and the ingest SHALL be answered only once the record is durable

#### Scenario: A re-sent message is absorbed across a writer restart

- **WHEN** the input log is reopened and a message whose `flz.msg_id` is still retained is sent again
- **THEN** the append SHALL answer the existing record's offset from the index rebuilt off the file, and the operator SHALL receive the message once

#### Scenario: The commit moves the watermark, the sweep cuts the file

- **WHEN** iteration N commits with consumed offset 7 and fewer released bytes than the cut threshold lie below it
- **THEN** the log's watermark for the instance SHALL be 7 at once, and the file SHALL be rewritten only by a later sweep once the released prefix exceeds the threshold

#### Scenario: A loop's Cells compact on the Watchdog's triggers

- **WHEN** a looping bounded operator's Cell WAL grows past `compact_after_bytes`, or its oldest unsnapshotted record ages past `compact_after_age_ms`
- **THEN** the Watchdog requests the Cell's compaction exactly as it does for an unbounded operator Cell, and a rehydrate before that restores from the WAL tail

### Requirement: Crash-resume enters the iteration after the last committed one

On respawn of a looping durable bounded instance, the driver SHALL resume at
iteration `N+1` where `N` is the highest journaled `iter_committed`, never at
the loop start, with cell state restored from the iteration-boundary
snapshots (or WAL replay) and input delivered from the input log's retained
tail above the instance's floor. A message-fed iteration (`ONCE` pass 0, every
`PER_INPUT` pass, `CHAINED` pass 0) that was in flight at the crash SHALL be
resumed over the message the controller re-forwards for it: that message's
channel is the pass's channel, its tasklets that the journal records committed
SHALL be skipped and re-seed their children from the journaled handoff, its
roots not yet committed SHALL re-run over the message, and the iteration SHALL
commit that message's offset, so the message drives one iteration and never
seeds a second. A carry-fed `CHAINED` iteration SHALL resume into the chain the
journaled entry channel names. Work of an uncommitted in-flight iteration MAY
re-run (at-least-once, existing author contract).

#### Scenario: Resume from the last committed iteration

- **WHEN** a looping instance crashes during iteration 3 after `iter_committed(2)` was journaled
- **THEN** on respawn it SHALL resume iteration 3 (re-seeded from the input-log offset recorded at iteration 2's commit), and iterations 1 and 2 SHALL NOT re-run

#### Scenario: A mid-iteration resume consumes its message once

- **WHEN** a `per_input` instance crashes in iteration 3 after its root tasklet committed and before `iter_committed(3)`, and the controller re-forwards the message iteration 3 had taken
- **THEN** the resumed iteration 3 SHALL skip the committed root, run the rest from its journaled handoff, commit with that message's offset, and iteration 4 SHALL wait for a new message rather than run over the same one

#### Scenario: A resumed chained iteration re-enters its chain

- **WHEN** a `chained` instance with two entry channels crashes in iteration 2 after `iter_committed(1)` named entry channel `b`
- **THEN** the resumed iteration 2 SHALL feed the journaled handoff into `b`'s chain

### Requirement: KeepProgress rollback is journal-backed

The compensation planner's `committed` input SHALL, for an abnormal
termination of a looping durable instance, be built from journaled `iter_committed`
markers: **keep-progress** SHALL keep every journaled iteration and undo only
the in-flight one; **all-or-nothing** SHALL undo the in-flight iteration and
then every journaled iteration in reverse. The existing `plan_compensation`
ordering contract SHALL be reused unchanged. For a member of a partition group the
journaled iterations are those committed since the member's last rescale, because a rescale
seals the member's journal at its resume point: an all-or-nothing rollback SHALL reach back
to that seal and no further, and a member's rollback SHALL never touch another member's
iterations.

#### Scenario: Keep-progress keeps journaled iterations across a crash

- **WHEN** a keep-progress looping instance has journaled iterations 1–4, crashes, resumes, and iteration 5 then fails terminally
- **THEN** only iteration 5's completed tasklets SHALL be compensated and iterations 1–4 SHALL be kept

#### Scenario: All-or-nothing unwinds journaled iterations too

- **WHEN** an all-or-nothing looping instance with journaled iterations 1–2 fails in iteration 3
- **THEN** compensation SHALL undo iteration 3's completed tasklets, then iterations 2 and 1, in reverse completion order

#### Scenario: All-or-nothing on a partition member stops at its seal

- **WHEN** an all-or-nothing member sealed at iteration 6 by a rescale commits iterations 7 and 8 and fails in iteration 9
- **THEN** compensation SHALL undo iteration 9's completed tasklets, then 8 and 7, and SHALL NOT compensate any iteration at or below 6

### Requirement: A tasklet ends its bounded operator after the iteration commits

A tasklet running inside a bounded operator SHALL be able to call `ctx.complete(result)` on its own tasklet context, in every SDK. The call SHALL NOT stop the iteration: every remaining stage of the iteration SHALL run, its outputs SHALL be recorded and released as usual, and once the iteration's commit is durable the instance SHALL end with status `ok` and `result`, instead of waiting for the next inbox item, re-seeding a chained loop, or parking. The completion SHALL travel with the iteration commit (`IterCommit`) and be journaled in its `iter_committed` entry, so an instance that crashes after that commit is durable and before its `BoundedCompleted` is sent SHALL, on resume, end with the same result rather than wait for input. The completing tasklet's own completion SHALL also travel with its task commit (`TaskCommit`) and be journaled in its `task_committed` entry, and a resume that skips that committed tasklet SHALL end the resumed iteration with it, so a crash between the two commits loses nothing. In a `once` bounded operator the call SHALL set the result the single pass ends with.

When more than one cell calls `ctx.complete` in one iteration, the instance SHALL end once, with the result of the call whose tasklet is latest in the chain's commit order and, within that tasklet, whose cell key sorts last, so the result does not depend on scheduling. A tasklet that calls `ctx.complete` and then fails SHALL fail its iteration as any failure does, and the completion SHALL be discarded with the iteration.

#### Scenario: A chat agent ends itself

- **WHEN** a `PerInput` bounded operator whose one tasklet calls `ctx.complete({"turns": n})` on a message whose kind is `end` receives three messages and then that message
- **THEN** four iterations SHALL commit, the instance SHALL end with status `ok` and result `{"turns": n}`, and it SHALL NOT park after the fourth

#### Scenario: A completion survives a crash after the commit

- **WHEN** an instance crashes after `iter_committed(N)` carrying a completion is durable and before `BoundedCompleted` reaches the controller
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

#### Scenario: A completing tasklet's output is released at its own commit

- **WHEN** a tasklet emits plugin-bound output and calls `ctx.complete(result)` in the same run
- **THEN** its output is written in its Cell's effect record, released when that stage's `task_committed` entry is acked, and the instance ends only after the iteration commits

### Requirement: A timer-fed iteration commits like any iteration

An iteration opened by a timer firing SHALL take the next iteration number and SHALL end with an `IterCommit` as a message-fed iteration does, under the same journal-then-ack gating, boundary snapshot compaction and outbox flush. Its commit SHALL carry no new consumed offset, a firing consuming no input: the offset the previous iteration recorded, or its absence where the journal records absence. It SHALL name the entry channel the previous iteration named, which is the chain a `CHAINED` resume re-enters. For a `CHAINED` loop its handoff SHALL be the iteration's leaf batch when the firing's chain reached the loop's leaf and released something, else the carry as it stood, so a timer-fed iteration that released nothing at the leaf SHALL NOT end the loop as an empty terminal batch would. A `PER_INPUT` loop's timer-fed iteration SHALL record its leaf batch as today.

#### Scenario: A timer-fed iteration keeps the consumed offset where it was

- **WHEN** a `per_input` loop committed iteration 3 with consumed offset 7 and a timer firing runs iteration 4
- **THEN** `iter_committed(4)` SHALL carry consumed offset 7, or the absence of one where the journal records absence, and no input-log record SHALL be released by it

#### Scenario: A chained loop keeps its carry across a timer-fed iteration that reached no leaf

- **WHEN** a `chained` loop's leaf released batch `B` in iteration 2 and a timer firing runs iteration 3 through a chain that does not reach the leaf
- **THEN** `iter_committed(3)` SHALL carry `B` as its handoff and iteration 4 SHALL be seeded from `B` into the loop's chain

#### Scenario: A chained loop takes a timer-fed iteration's leaf batch as its carry

- **WHEN** a timer firing's chain in a `chained` loop reaches the loop's leaf and the leaf releases batch `C`
- **THEN** `iter_committed` for that iteration SHALL carry `C` and the next carry-fed iteration SHALL be seeded from `C`

