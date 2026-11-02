# steps-durable-loop Specification

## Purpose

A steps chain that repeats with a durable commit per iteration. Each iteration is seeded from the
next inbox item or the previous iteration's handoff and ends in an iteration commit; a resumed
instance enters the iteration after the last committed one. A per-input keep-progress loop
dead-letters a failed turn, answers a caller waiting on it with an error, and takes the next input.
Inside an iteration a failing Cell is retried in place with backoff before its tasklet fails,
without holding a worker thread, and authors can mark errors non-retryable and tune the policy.

## Requirements

### Requirement: A structured steps chain can loop with a durable per-iteration commit

A structured steps class SHALL be able to declare its chain as a loop: each
iteration seeds from the next input-log record or the previous iteration's handoff,
runs the chain under the existing per-task commit gating, and ends with an
iteration commit. The controller SHALL append `iter_committed(N, consumed_offset)` to the
task-journal, where `consumed_offset` is the `flz.offset` of the input that seeded the
iteration or is absent when nothing journaled seeded it. The `iter_committed` append SHALL be made durable
by the `FsyncBatcher` tick like a task commit, and its ack SHALL follow the
sync. The iteration commit SHALL request no boundary snapshot and SHALL flush no outbox of its
own: the iteration's outputs left through the Cells' own outboxes as each stage committed, and
the instance's Cells are compacted by the Watchdog's size and age triggers as a flow relay's are, a
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

- **WHEN** a looping steps relay finishes iteration N
- **THEN** the journal SHALL hold `iter_committed(N)` with the consumed input-log offset, the instance's watermark row SHALL be at that offset, and no `CompactCell` SHALL have been requested for the boundary

#### Scenario: The journal does not grow with the turns

- **WHEN** a looping steps relay with no compensable tasklet has committed 1 000 iterations
- **THEN** its task-journal SHALL hold the entries of the current iteration plus at most the cut threshold of settled prefix, and a rehydrate SHALL restore from them the resume point, consumed offset, handoff and epoch a full history would have given

#### Scenario: A class with compensable tasklets keeps its history

- **WHEN** a looping steps relay whose tasklets declare `compensate` commits an iteration
- **THEN** its task-journal SHALL NOT be cut, so an all-or-nothing rollback and a rewind still replan from every committed iteration

#### Scenario: The inbox append costs the same at turn 1 and turn 10 000

- **WHEN** a turn is ingested for a loop that has consumed 10 000 turns
- **THEN** the append SHALL consult the log's in-memory id index and SHALL NOT read the log file or fold the journal, and the ingest SHALL be answered only once the record is durable

#### Scenario: A re-sent message is absorbed across a writer restart

- **WHEN** the input log is reopened and a message whose `flz.msg_id` is still retained is sent again
- **THEN** the append SHALL answer the existing record's offset from the index rebuilt off the file, and the relay SHALL receive the message once

#### Scenario: The commit moves the watermark, the sweep cuts the file

- **WHEN** iteration N commits with consumed offset 7 and fewer released bytes than the cut threshold lie below it
- **THEN** the log's watermark for the instance SHALL be 7 at once, and the file SHALL be rewritten only by a later sweep once the released prefix exceeds the threshold

#### Scenario: A loop's Cells compact on the Watchdog's triggers

- **WHEN** a looping steps relay's Cell WAL grows past `compact_after_bytes`, or its oldest unsnapshotted record ages past `compact_after_age_ms`
- **THEN** the Watchdog requests the Cell's compaction exactly as it does for a flow relay Cell, and a rehydrate before that restores from the WAL tail

### Requirement: Crash-resume enters the iteration after the last committed one

On respawn of a looping durable steps instance, the driver SHALL resume at
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

A failed turn of a `per_input` keep-progress loop SHALL NOT be an abnormal termination: it
SHALL be dead-lettered and undone as "A per-input keep-progress loop SHALL dead-letter a
failed turn and take the next input" states, keeping every journaled iteration as
keep-progress always does, and the loop SHALL go on.

#### Scenario: Keep-progress keeps journaled iterations across a crash

- **WHEN** a keep-progress `chained` looping instance has journaled iterations 1–4, crashes, resumes, and iteration 5 then fails terminally
- **THEN** only iteration 5's completed tasklets SHALL be compensated and iterations 1–4 SHALL be kept

#### Scenario: All-or-nothing unwinds journaled iterations too

- **WHEN** an all-or-nothing looping instance with journaled iterations 1–2 fails in iteration 3
- **THEN** compensation SHALL undo iteration 3's completed tasklets, then iterations 2 and 1, in reverse completion order

#### Scenario: All-or-nothing on a partition member stops at its seal

- **WHEN** an all-or-nothing member sealed at iteration 6 by a rescale commits iterations 7 and 8 and fails in iteration 9
- **THEN** compensation SHALL undo iteration 9's completed tasklets, then 8 and 7, and SHALL NOT compensate any iteration at or below 6

#### Scenario: A per-input keep-progress failure keeps the loop

- **WHEN** a keep-progress `per_input` instance has journaled iterations 1–4 and iteration 5's turn fails after its retries
- **THEN** only iteration 5's completed tasklets SHALL be undone, iterations 1–4 SHALL be kept, and the instance SHALL wait for its next message as iteration 6

### Requirement: A tasklet ends its steps relay after the iteration commits

A tasklet running inside a steps relay SHALL be able to call `ctx.complete(result)` on its own tasklet context, in every SDK. The call SHALL NOT stop the iteration: every remaining stage of the iteration SHALL run, its outputs SHALL be recorded and released as usual, and once the iteration's commit is durable the instance SHALL end with status `ok` and `result`, instead of waiting for the next inbox item, re-seeding a chained loop, or parking. The completion SHALL travel with the iteration commit (`IterCommit`) and be journaled in its `iter_committed` entry, so an instance that crashes after that commit is durable and before its `RelayCompleted` is sent SHALL, on resume, end with the same result rather than wait for input. The completing tasklet's own completion SHALL also travel with its task commit (`TaskCommit`) and be journaled in its `task_committed` entry, and a resume that skips that committed tasklet SHALL end the resumed iteration with it, so a crash between the two commits loses nothing. In a `once` steps relay the call SHALL set the result the single pass ends with.

When more than one cell calls `ctx.complete` in one iteration, the instance SHALL end once, with the result of the call whose tasklet is latest in the chain's commit order and, within that tasklet, whose cell key sorts last, so the result does not depend on scheduling. A tasklet that calls `ctx.complete` and then fails SHALL fail its iteration as any failure does, and the completion SHALL be discarded with the iteration.

#### Scenario: A chat agent ends itself

- **WHEN** a `PerInput` steps relay whose one tasklet calls `ctx.complete({"turns": n})` on a message whose kind is `end` receives three messages and then that message
- **THEN** four iterations SHALL commit, the instance SHALL end with status `ok` and result `{"turns": n}`, and it SHALL NOT park after the fourth

#### Scenario: A completion survives a crash after the commit

- **WHEN** an instance crashes after `iter_committed(N)` carrying a completion is durable and before `RelayCompleted` reaches the controller
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

### Requirement: A per-input keep-progress loop SHALL dead-letter a failed turn and take the next input

A steps instance SHALL NOT end when its loop mode is `per_input`, its rollback mode is
keep-progress, and a pass seeded by a message fails because a tasklet's Cell exhausted its
retry ladder or raised a non-retryable error. The driver SHALL instead, in this order:

1. stop the pass, as today: no Cell of the pass runs again and nothing it had not committed
   is kept;
2. send the iteration's commit marked dead-lettered, carrying the iteration number, the
   message's input-log offset, its entry channel, the failing tasklet, its attempts and its
   error text, and wait for the controller's durable answer;
3. undo the iteration's committed tasklets the way a rewind undoes an unwound iteration:
   each tasklet's `compensate` hook runs over what it recorded, each undo is journaled as a
   `compensated` entry, and no compensation-done marker is written;
4. wait for the next message as the next iteration.

On the dead-lettered commit the controller SHALL first park the message in the
dead-letter queue (see `relay-dead-letters`) and wait for the entry to be durable, then
journal the record `dead_lettered` (the iteration, the consumed offset, the entry channel,
the tasklet, the attempts and the running count of dead-lettered turns), wait for it to be
durable, answer, and then release the offset into the input log's watermark and cut the
journal as an iteration commit does. The task-journal fold SHALL treat a `dead_lettered`
record as an iteration commit for the resume point, the consumed offset, the floor and the
journal cut, and SHALL list the iteration's committed tasklets that no `compensated` entry
covers as owed undo, in the same `rewind_undo` a rewind fills. The epoch SHALL NOT change.

A class that opted out of persistence journals nothing and keeps no input log: its driver
SHALL park the failed turn through the ordinary park path, with no payload bytes and no
offset, undo in memory, and wait for the next message.

Everything else SHALL end the instance as it does today: a failure under all-or-nothing; a
failure in a `once` or a `chained` loop; a failure in a pass a timer firing opened; a stage
or iteration commit the controller refuses; a stop; `max_duration`. An unreadable message
SHALL keep being parked without a pass, as today. Both steps drivers SHALL behave alike:
the shared core's, which runs the steps relays of the Rust, Java, Node, Python, Ruby,
PHP and Go SDKs, and the Elixir SDK's.

#### Scenario: A model error does not end the session

- **GIVEN** a durable `per_input` keep-progress session whose tasklet fails every attempt on
  the message `boom`
- **WHEN** it is sent `hello`, then `boom`, then `again`
- **THEN** `hello` and `again` SHALL each be answered, `boom` SHALL be one entry in the DLQ,
  the instance SHALL still be running, and its journal SHALL hold, in order, an
  `iter_committed` for the iteration that took `hello`, a `dead_lettered` record for the one
  that took `boom` and an `iter_committed` for the one that took `again`

#### Scenario: A turn's committed tasklets are undone

- **GIVEN** a two-tasklet keep-progress session whose first tasklet declares `compensate`
  and whose second fails on `boom`
- **WHEN** it is sent `boom`
- **THEN** the first tasklet's `compensate` SHALL run once over what it recorded for that
  turn, a `compensated` entry SHALL follow the `dead_lettered` record, and the next message
  SHALL be answered

#### Scenario: A crash during the undo resumes the undo, not the turn

- **WHEN** the runner is killed after the `dead_lettered` record is durable and before the
  first tasklet's undo is journaled, and the instance is rebound
- **THEN** the resumed instance SHALL run that undo once, SHALL NOT run the turn again, SHALL
  NOT end, and SHALL answer the next message

#### Scenario: A crash before the dead-letter commit re-runs the turn

- **WHEN** the runner is killed after the message was parked and before the
  `dead_lettered` record was journaled, and the instance is rebound
- **THEN** the turn SHALL run again over the re-forwarded message; if it fails again the DLQ
  SHALL hold one entry for it, and if it succeeds the DLQ SHALL hold none

#### Scenario: All-or-nothing still ends the session

- **WHEN** a `per_input` all-or-nothing instance's turn fails after its retries
- **THEN** it SHALL compensate and end with status `error`, exactly as before this change,
  and nothing SHALL be written to the DLQ

#### Scenario: Other loop modes still end

- **WHEN** a keep-progress `chained` instance's pass fails after its retries
- **THEN** it SHALL end with status `error`, and nothing SHALL be written to the DLQ

#### Scenario: A stop during a turn still ends the session

- **WHEN** a keep-progress `per_input` instance is stopped while a turn runs
- **THEN** it SHALL end with status `cancelled` and nothing SHALL be written to the DLQ

#### Scenario: A session that opted out of persistence goes on too

- **WHEN** a `per_input` keep-progress class with persistence off fails a turn
- **THEN** a DLQ entry without payload bytes SHALL be written and the instance SHALL answer
  its next message

### Requirement: A dead-lettered turn SHALL be visible

A dead-lettered turn SHALL be visible wherever a person or a program looks for it:
`flz dlq list <relay>` SHALL show its entry under the `_turn` slot with the entry
channel, the offset, the error and the attempts; `flz relay info <relay>` SHALL show
`Failed turns: <count> (last: iteration <n>)` for an instance that has dead-lettered any,
read from the latest iteration record's running count so it survives a journal cut and a
restart; `flz state journal <relay>` SHALL show the `dead_lettered` row with its iteration,
offset and tasklet; the controller SHALL execute
`[:flezha, :steps, :turn_dead_lettered]` with a count of 1 and the namespace, relay,
iteration, offset, tasklet and attempts, bridged to the metric
`flezha_steps_turns_dead_lettered_total`; and it SHALL log one warning line naming the
relay, the iteration, the tasklet and the DLQ entry. Once `send-a-part-while-it-runs` has
landed, the park SHALL reach the relay's listeners as its `input.dead_lettered` notice,
released once the `dead_lettered` record is durable; whichever of the two changes lands
second SHALL wire the turn's park to that notice.

#### Scenario: Every surface shows the failed turn

- **WHEN** a session dead-letters the turn of its iteration `n`
- **THEN** `flz dlq list` SHALL show one `_turn` entry, `flz relay info` SHALL print
  `Failed turns: 1 (last: iteration <n>)`, `flz state journal` SHALL show a `dead_lettered`
  row for iteration `n`, and one `turn_dead_lettered` event SHALL have been executed

### Requirement: A caller waiting on a dead-lettered turn SHALL be answered with an error

A caller SHALL be answered with an error in the error model's shape when the message of a
dead-lettered turn was a call awaiting an answer (the reply of a
call, or an outside caller's answer),
once the `dead_lettered` record is durable, and SHALL NOT be left to its deadline. The durable
`dead_lettered` record SHALL be that answer's release point: the controller SHALL hand the
turn's reply token, read from the input-log record it already reads for the park, to the
instance's Overwatch once the record is durable, as the other park routes hand theirs. The
error SHALL be the one those changes define for a dead-lettered call input (`call.failed` in
their drafts), its details naming the DLQ entry and the author's error text. A class that
opted out of persistence has no `dead_lettered` record: its turn's park frame SHALL carry the
pass's reply token, and its caller SHALL be answered as that park route answers, once the
DLQ append is durable. This change dead-letters the turn and the call changes own the
answer; the requirement SHALL hold in both directions once both have landed, whichever lands
second wiring the hand-off, and a replay of the entry SHALL answer no caller.

#### Scenario: A called session's failed turn answers its caller

- **GIVEN** a relay that calls a `per_input` keep-progress session whose turn fails on
  `boom`
- **WHEN** the caller sends `boom`
- **THEN** the caller SHALL receive an error answer naming the DLQ entry before its deadline,
  and the session SHALL answer its next call

### Requirement: A failing cell is retried with backoff before the tasklet fails

A cell whose work errors inside a structured steps tasklet SHALL be retried in place on the
core's per-item retry ladder, the one a flow cell climbs, under the retry policy of the edge
that feeds the tasklet, the same `retry_max` / `retry_backoff_ms` a flow relay's edge declares:
default 3 attempts (the first run plus its retries), with the platform's shared jittered
exponential backoff (`base × 2^(n-1)` ±50%, capped at `RETRY_BACKOFF_MAX_MS` = 5 s; default base
50 ms, the edge's default, computed by the same helper the flow relay's retry-then-park ladder uses).
The failed item and the untouched tail of its batch are held at the head of the cell's line and
re-handed after the backoff; sibling cells' settled results stay held on the instance's pass
sheet and are not re-run. There SHALL be no class-level retry policy on a steps relay: the policy
the ladder reads SHALL be the one the shared core already derives for the tasklet from its
input channel, and a steps relay SHALL carry no `retry_attempts`, `retry_backoff` or
`barrier_retry_attempts` declaration in any SDK or on the wire. A retry that succeeds SHALL let
the stage reach its count and commit normally. A cancellation (external stop / `max_duration`)
SHALL end a pending backoff at once by disarming its deferred wake and releasing the cell's
gates. The retried item re-runs under the same at-least-once idempotency contract crash-resume
imposes.

#### Scenario: Transient failure recovers without failing the task

- **WHEN** cell k2 of a parallel stage errors once with a retryable error and succeeds on retry, while k1 and k3 settled
- **THEN** the stage SHALL reach its count and commit normally, k1/k3 SHALL NOT re-run, and no compensation SHALL fire

#### Scenario: The edge's budget is the ladder's

- **WHEN** a steps relay wires `c.input("order").retry_max(5).retry_backoff_ms(20).into(Reserve)` and `Reserve`'s cell errors retryably on every attempt
- **THEN** the cell SHALL run five times, each wait SHALL grow from a 20 ms base, and the tasklet SHALL fail after the fifth attempt

#### Scenario: Only the failed item and its tail re-run

- **WHEN** a cell's stage share is three items and the second fails retryably
- **THEN** the first item SHALL NOT run again, and the retry SHALL re-hand the second item and then the third, in that order

#### Scenario: Stop interrupts a waiting retry

- **WHEN** an external stop arrives while a cell is in a backoff wait
- **THEN** the wait SHALL end at once, the cell's held items SHALL be dropped, and the relay SHALL take the normal abnormal-exit path (compensation of completed tasklets)

### Requirement: Retry exhaustion fails the tasklet exactly as today

A cell SHALL fail its tasklet after the configured attempts are exhausted, surfacing
the original error into the existing abnormal-termination path: the tasklet
does not commit, AbortAll/compensation runs per the existing saga contract, and
the relay terminates with status `error`. No new terminal state SHALL be
introduced. In a `per_input` loop declaring keep-progress the exhausted Cell SHALL fail
only the turn: the pass stops, the turn is dead-lettered and undone, and the relay does
not terminate (see `steps-durable-loop`); the same holds for a non-retryable error.
Each retry attempt SHALL emit telemetry (instance, tasklet, cell, attempt, delay).

#### Scenario: Persistent failure aborts after the ladder

- **WHEN** a cell errors retryably on every one of its 3 attempts, in a relay that is not a `per_input` loop declaring keep-progress
- **THEN** the tasklet SHALL fail with the original error, completed tasklets SHALL be compensated, and the run SHALL terminate with status `error`

#### Scenario: Retries are observable

- **WHEN** a cell is retried
- **THEN** a telemetry event with the instance, tasklet, cell, attempt number, and applied delay SHALL be emitted

#### Scenario: Exhaustion in a per-input keep-progress loop fails the turn

- **WHEN** a cell of a `per_input` keep-progress relay errors retryably on every one of
  its 3 attempts
- **THEN** the tasklet SHALL fail with the original error, the turn SHALL be dead-lettered
  with `attempts` 3 and that error, and the relay SHALL NOT terminate

### Requirement: Authors can classify errors as non-retryable and tune the policy

`StepsError` SHALL support a non-retryable marker; an error so marked SHALL
skip the ladder and fail the tasklet immediately. Errors produced by the
existing string conversions SHALL remain retryable by default. The policy knobs SHALL be the
edge's, `retry_max` and `retry_backoff_ms` on the channel builder of every SDK, exactly as a
stream tunes them; the `Relay` trait SHALL expose no per-class knob for attempt
count or backoff base, and `retry_max(1)` SHALL be fail-fast, `retry_max(0)` being refused as
`topology.retry_max_zero` for both kinds. Retry SHALL apply to
every steps relay's tasklet cells, since every steps relay runs tasklets, and
SHALL only ever occur before the tasklet's commit (a committed tasklet is
never retried).

#### Scenario: Non-retryable error aborts immediately

- **WHEN** a cell returns an error marked non-retryable on its first attempt
- **THEN** no retry SHALL occur and the tasklet SHALL fail immediately

#### Scenario: retry_max(1) is fail-fast

- **WHEN** a steps relay's entry edge declares `retry_max(1)` and a cell errors
- **THEN** the tasklet SHALL fail on the first error, and the same declaration with `retry_max(0)` SHALL be refused at registration naming `topology.retry_max_zero`

#### Scenario: The class-level knobs are gone from every SDK

- **WHEN** every SDK's steps relay surface and `test/fixtures/host_class_decl_keys.txt` are searched for `retry_attempts`, `retryAttempts`, `RetryAttempts`, `retry_backoff`, `retryBackoff`, `RetryBackoff`, `barrier_retry_attempts`, `barrierRetryAttempts` or `BarrierRetryAttempts`
- **THEN** no trait, interface, definition type, decorator keyword, class-body setting, attribute argument, `use` option or declaration key carries one, and `ZeroRetryBackoff` names no refusal in any SDK or in the core

### Requirement: A backoff wait holds no worker thread

A retrying cell's backoff SHALL NOT occupy any thread: the attempt runs to completion on an SDK
worker, the worker is released, and the re-hand is timed by the core's deferred-wake list, the
same entry a flow cell's backoff is. There SHALL be no steps-owned ladder and no barrier
that times it. The retry policy (attempts, shared jittered exponential backoff), error
classification, exhaustion behaviour (the stage fails into compensation, never into the
dead-letter queue) and per-attempt telemetry SHALL be the flow relay's ladder's, with the terminal
action the only difference.

#### Scenario: Sibling stages proceed while a cell backs off

- **WHEN** cell k2 of a parallel stage is waiting out a retry backoff while
  unrelated activations are queued
- **THEN** no thread SHALL be sleeping on k2's behalf, and the queued
  activations SHALL be served by the workers that ran k2's failed attempt

#### Scenario: Exhaustion compensates rather than parks

- **WHEN** a steps cell's item fails on its final attempt, or with a non-retryable error
- **THEN** no dead-letter entry SHALL be minted; the cell's stage SHALL be marked failed on the
  instance's pass sheet, and once no stage of the pass owes an item the relay SHALL
  compensate and end with status `error`

#### Scenario: Stop disarms a pending backoff

- **WHEN** an external stop arrives while the deferred-wake list holds a cell's retry entry
- **THEN** the entry SHALL be removed, the cell's gates released and its held items dropped on
  the thread that brings the stop, and the relay SHALL take the normal abnormal-exit path

### Requirement: A commit barrier is re-sent a fixed number of times

The re-send of a task-commit, iteration-commit or compensation barrier whose acknowledgement timed out (`FLEZHA_STEPS_ACK_TIMEOUT_MS`) SHALL be governed by the runner's controller connection, not by the class: a constant of three sends, each waited out with the shared ladder's default backoff base, declared beside the acknowledgement timeout in the shared core and read by the Elixir SDK's driver from its knob table. The waiter SHALL stay registered across the re-sends so a late acknowledgement continues the chain, and only the exhausted count SHALL be a terminal error, as today. A barrier the controller refuses as `commit.record_not_durable` SHALL be sent again under the same reference id after the shared ladder's backoff from a 1 s base, within the same three sends; any other refusal of a forward task-commit or an iteration-commit barrier SHALL fail it at once.

#### Scenario: A stalled acknowledgement is re-sent three times

- **WHEN** a controller withholds every `TaskCommitResponse`
- **THEN** the runner SHALL send the same barrier under the same reference id three times in total and then end the run with a terminal error, and no class setting SHALL change that count

#### Scenario: A barrier refused as not durable in time is re-sent

- **WHEN** a controller refuses a task-commit barrier with `commit.record_not_durable`
- **THEN** the runner SHALL send the same barrier again under the same reference id after a backoff, up to three sends in total, and fail the stage only when the last send is refused
- **AND** a refusal with any other code SHALL fail the stage without a re-send
