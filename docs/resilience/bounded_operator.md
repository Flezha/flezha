# Bounded Operator

The mechanism-level and user-level reference for the bounded operator kind: what the
controller and the shared runner core do at each tasklet barrier, iteration boundary, park,
rewind and crash, and the knobs, files, CLI verbs and telemetry a user has — for users
and for contributors to `flz_core/src/bounded/` and `lib/flezha/operator/`. The concepts (the
graph, the journal layout, the barrier, the outbox, loop modes, suspension, rewind) are described
once in [`../architecture/bounded_profile.md`](../architecture/bounded_profile.md) and not
repeated here; [`data_durability.md`](data_durability.md) says what is lost when.

## Structure: a DAG of tasklets, parallel cells

- **Numbering.** Validation orders the tasklets in Kahn topological order from the entry; the
  position is the wire `task_idx`. A producer's index is always below its consumers', so
  completion order is topological and compensation simply reverses it.
- **Scheduling.** A stage's items reach its cells the way an unbounded operator's do: through the routing
  seam into each cell's mailbox, handed out by the poll core under per-cell exclusivity,
  fairness and the in-flight cap (`FLEZHA_MAX_INFLIGHT_ACTIVATIONS`), with no queue of their
  own. A stage commits when its own last item settles, and what it emitted waits on the
  instance until that commit is acknowledged, then enters its children's mailboxes. A node runs
  once every producer feeding it in this pass has committed, a join with every producer's
  items. Sibling stages run and commit independently. Where a stage runs:
  [`../architecture/runner.md`](../architecture/runner.md#a-bounded-stage).
- **Done.** The operator is `ok` when every leaf tasklet has committed. The SDK declares its
  leaf set (`leaf_tasks`) and compensable set (`compensable_tasks`) in the `AddOperator` response
  metadata, so the controller derives the same fact from the journal. A join with one committed
  and one uncommitted producer resumes with the committed handoff banked, never re-run.
- **Self-termination.** `self_terminating()` / `selfTerminating()` defaults to `true`; `false`
  with no `max_duration` is refused at registration.

## Lifecycle: exit triggers, inbox, one teardown

An instance is spawned with `init_values` as its tasklets' initial state, as an unbounded operator is,
from the CLI (`flz operator start`), a schedule (`flz schedule`) or a running tasklet
(`ctx.spawn_bounded`, [`../sdk/tasklet_requested_spawn.md`](../sdk/tasklet_requested_spawn.md)),
takes its input as messages on its entry channels (the first may ride the start:
`flz operator start --text`, a schedule's stored `--text`, a spawn's payload) and terminates
on the first of: chain quiescence (every leaf committed); a tasklet's
`ctx.complete(result)`, honoured once its iteration commits; a loop that runs out of work (a
chained loop's empty batch); `max_duration()` / `maxDuration()`
elapsing (a cooperative cancellation token); `flz operator stop` (a cooperative `BoundedStop`
that trips the same token — an instance not bound to a runner takes the hard `RemOperator`
teardown, `OperatorManager.request_stop/3`).

Every trigger converges on one `BoundedCompleted` (`status` `ok | error | cancelled`, `error`,
`result`). The controller writes the completion record, then tears down off the control-channel
reader, **settling both output debts first** (`ControlChannel.drain_owed_outputs/2`). They are
settled differently. The data stream is *flushed*: `StreamPool.flush_channels/1` issues one
bounded call per stream channel of the runner, and a channel answers only after every frame
already in its mailbox has been routed to an Overwatch — so an opted-out class's
last direct output is run before its Overwatch is stopped, however the two sockets interleave on
arrival, and even when an output of that run was lost rather than delivered. The Cells'
outboxes are *waited for*: up to `drain_owed_ms` (60 s), polling every 50 ms, until every live
Cell writer of the instance reports nothing pending and nothing awaiting a sink's confirmation
(`WalWriter.outbox_status/2`). An entry still held for a stage marker belongs to a stage that
never committed and is owed nothing. A deadline hit is logged (`... still owed`) and the
retained WAL keeps the debt. `status = "error"` tears down identically: there is no
platform retry of a failed tasklet. A frame addressed to a running instance takes the
stream's entrance (channel resolved, parsed, transformed, parked when it cannot enter), never
dropped: it starts a pass when the instance is waiting at a committed boundary, and otherwise
waits in the instance's queue for the next one. Every loop mode reads it: `Once` its one
message, `PerInput` one per iteration, `Chained` the first and then its own carry. An instance
with no message waits, and a second message to a `Once` instance is accepted and never read.
A due Cell timer waits in the same queue, in arrival order with messages, and a `Chained`
loop's carry is fed only once nothing is queued, so a firing that came due during a pass runs
before the loop's next carry-fed pass.

## Durable loop and suspension

Loop modes are declared with `loop_mode()` / `loopMode()`. Registration refuses a class that
declares no tasklet (`topology.no_tasklets`) and a durable `Chained` loop whose terminal tasklet
releases no inter-tasklet channel (`LoopTerminalWithoutHandoff`). Two shapes fail that: a
terminal nothing consumes and that names no sink owns no output channel at all, and one whose only
wiring is a plugin sink gets no journal codec, because the runtime attaches one to inter-tasklet
channels and nothing else. Either way a crash would re-seed empty, read as converged.

**The iteration boundary.** `IterCommit` rides the same commit worker as `TaskCommit` (below).
After the ack the worker reports the iteration's consumed offset into the input log's watermark
(`TaskJournal.release_input/2`) and cuts the journal (`TaskJournal.cut/1`). The boundary takes
no snapshot and flushes nothing: the stages' outputs were released at their own markers, and
the Watchdog's size and age compaction triggers serve a bounded operator's Cells as they serve a
stream's, so a rehydrate replays whatever WAL tail lies above the last snapshot. An iteration
that consumed no journaled input (a carry-fed pass) reports nothing. Both files are cut by their
own writers (the journal on this cast, the input log by its retention sweep below the
watermark), so the worker never waits on a file rewrite,
and a prefix cut below `:record_log_cut_after_bytes` (64 KiB) is deferred until that many
released bytes accumulate.
The journal is cut only for a class whose `compensable_tasks` is `[]` (a saga or a rewind
replans from the committed history); a cuttable loop can only be rewound to its current
iteration.

**The durable input log.** `Ingest` holds the one append-before-forward seam, and a durable
bounded operator uses the input log an unbounded operator on the effectively-once tier uses: input goes to
`<state_root>/<ns>/<inst>/input.log` (`Flezha.State.InputLog`) before it is forwarded,
answered once synced and stamped `flz.offset` (0-based; absent means not journaled, never
`0`); a failed append is not forwarded. A repeat of a `flz.msg_id` the log still retains is
absorbed, from an in-memory index the log builds from the file on the first append that
carries an id; a message without an id is always appended. The instance's floor is the log's
watermark under the journal's identity, seeded at every bind from the journal's **released**
offset (the maximum consumed offset of any iteration commit over the raw journal, which no rewind
lowers) and from the park record while the instance sleeps, and raised by every commit and park,
so a consumed message is never re-delivered. The appends debit the source credit window like an unbounded operator's, so a sender with
16 MiB owed by one instance parks until it commits.

**Suspension.** Sleeping when idle is a class setting of both kinds, `sleep_when_idle`, on by
default for a bounded operator whose persistence is on; a class that declares it with persistence off is
refused at registration as `topology.sleep_without_persistence`. An instance of any loop mode whose class
sleeps and whose inbox stays empty past `FLEZHA_PARK_IDLE_MS` (default 600 000; read in the
shared core, so every host SDK agrees, and by the Elixir SDK's own driver) at a committed
boundary, with every Cell drained, asks to sleep without a completion and ships `IterPark`,
naming the last message it parked to the dead-letter queue unread. The controller writes the
instance's park record (`ClassRegistry.put_parked/2`, an ETS entry and a durable sidecar in the
class directory, never a journal entry) and reports that offset into the input log's watermark,
releasing the message as an iteration commit releases its input, **before**
`Migration.suspend/3` evicts the instance (`RemOperator`, runner tag `nil`); a park whose record
fails leaves the instance bound and logged. The Overwatch and the registry entry stay, and the
Overwatch **hibernates** (`:hibernate` on both suspension entries, releasing its bind-time heap).
An unbounded operator that sleeps takes the same path at iteration `0`
([`data_durability.md`](data_durability.md#sleeping-instances)).

| Surface | While suspended |
| --- | --- |
| `flz operator info` | phase `suspended` |
| convergence loop | skipped (`suspended?`), plugin diff included — its plugins left with the runner and the rehydrating bind replays them |
| namespace hub | outlives its runner: `HubManager.stop_hub_if_vacant/1` stops a hub only with no operator registered, so a parked fleet survives its sole runner dying |
| `flz state get` | refused — no live Cell |
| `flz operator stop` | wakes it onto a runner with the stop carried: `BoundedStop` follows the bind, and it ends `cancelled` with its record and directory kept, as it would stopped awake. With no runner to wake on, the hard removal |
| input-silence sweep | never flags it: the sweep skips a class whose park record exists |
| removing its factory from the runner | the SDK's live-instance check cannot see a suspended instance, so the removal succeeds and the later rehydrate fails placement until a runner re-defines the factory; `FleetManager` warns `dropped factory ... that still has controller-known operators` on the `UpdateFactories` frame. A blocking policy is deferred |

**Rehydrate = bind.** A new input-log append, the wake deadline, a crash and a migration all go
through `Lifecycle.complete_init/5`: respawn on any live runner, `AddOperator.task_journal`
with the folded journal, `resume_cell_count` Cell restores, and the one re-forward every bind
of either kind runs (`Replay.reforward_undelivered_tail/1`): the log's records above the floor,
each with its original `flz.offset`, before the plugins come up.
`AddOperator.inbox_tail_count` (the log's last offset minus the floor) tells the instance how
many items it is owed; while any is still owed its park deadline is 60 s instead of the park
idle, so a tail forwarded under load never meets an instance that already parked. An instance with
undelivered input is never left suspended (`rehydrate_if_pending/1` runs at every park and at
boot, and asks the log's tail rather than reading its records), and a park that lands while an
un-park is still binding wins (`parked_in_flight` discards the task's stale bound state; the
pending tail wakes it again). A bounded operator's journal, Cells and input log share one key, its
instance id, so a bind whose `:operator_class` names anything else is refused.

**The wake timer** is the instance's earliest pending processing-time Cell timer, which the
runner names in the park (`IterPark.wake_deadline_ms`). The controller keeps it in the park
record and re-arms it with `Process.send_after` from it at boot, so it survives a controller
redeploy. It fires by rehydrating the instance, nothing appended to its input log: the restored
Cells re-arm their timers and the due one opens a pass. A racing message and the deadline
collapse into one rehydration, and an elapsed deadline counts as pending work beside
undelivered input, so a wake that found no runner is retried on the next convergence tick and a
controller that boots past it rehydrates at once. At boot an instance with a park record and nothing undelivered
(`Migration.suspended_on_disk?/1`) enters suspended without binding; undelivered input outranks
the park. The bind that wakes it clears the record before the runner is told to start the
instance, so a park landing after the start keeps its own record. Inspect with
`flz state journal <operator/instance>` (entries tagged with their iteration, `iter_committed`
with its consumed offset or "consumed no journaled input"; the journal records no park) and
`flz state inbox <operator/instance>` (the input log's
retained records, and which are still above the floor). `flz operator replay` refuses a
bounded operator Cell, since its baseline is a committed iteration across every Cell of the instance and
its effect records carry no consumed offset; `flz operator rewind` is a loop's way back.

A bounded tasklet's Cell timers fire as passes of their own
([`cell_timers.md`](../sdk/cell_timers.md#timers-in-a-bounded-operator)). A served firing leaves its
Cell's pending set when the stage that ran it commits, and the root's `TaskCommit` names the
firing, so a resume knows the pass it opened: a crash before that commit fires the restored
timer again into a fresh pass, and a crash after it drops the restored timer and resumes the
pass from the journal.

## Cell retry: the rung before the abort

A failing item inside a **structured** tasklet is retried in place on the poll core's
per-item ladder, the one every unbounded cell climbs, while what its finished siblings emitted
waits on the instance for the stage's commit. The policy is the tasklet's input edge's, as on
an unbounded operator; a bounded class declares none of its own.

| Knob | On the edge | Default |
| --- | --- | --- |
| total attempts, first run included | `retry_max(n)` / `retryMax(n)` | 3; `1` is fail-fast, `0` is refused (`topology.retry_max_zero`) |
| backoff base | `retry_backoff_ms(ms)` / `retryBackoffMs(ms)` | 50 ms; attempt `n` waits `base × 2^(n-1)` ±50 % jitter, capped at 5 s (`RETRY_BACKOFF_MAX_MS`) |

- `TaskletError::non_retryable(..)` skips the ladder; every other error — a panic included,
  which the author never got to classify — is retryable. Where an unbounded operator parks an exhausted
  input to the dead-letter queue, a bounded operator fails the tasklet and compensates.
- A retry re-hands the failed item and the items queued behind it, never one that already
  settled, over the state its failed attempt already applied — the at-least-once contract
  crash-resume imposes. A failed attempt's outputs are dropped, never double-emitted. The
  ladder is strictly pre-commit: a journaled tasklet is never retried, and a crash mid-ladder
  resumes as an ordinary uncommitted-tasklet re-run with the counter reset.
- The backoff holds no worker and no thread: it is an entry on the poll core's deferred-wake
  list, and the cell's gates stay held until it comes due. A stop, `max_duration` or a sibling's
  failure disarms every pending backoff of the pass at once, and whatever the pass's cells still
  have queued is consumed without running. A failure while the cancel token is set reports
  `cancelled`. Each retry is logged (`instance / tasklet / cell / attempt / delay`) and counted in
  the retry counter the unbounded operator's ladder feeds, shipped in `data_heartbeat` metadata.

An exhausted ladder fails the stage where an unbounded operator would park the item: no commit, nothing
leaked, compensation, status `error`. Nothing bounded operator is ever written to the DLQ store — abort-and-compensate is the escape
where an unbounded operator would [park the payload](data_durability.md#dead-letter-queue-poison-payloads-park-never-drop).

## Rewind: taking a loop back to a committed iteration

```text
flz operator rewind <instance> --to-iteration N
```

`Migration.rewind_to/2` runs, in order:

1. **The guard**, before anything durable changes. `N` must be a committed iteration (the refusal
   names the committed range), and every tasklet above `N` that wrote plugin output must be in
   `compensable_tasks` (`TaskJournal.staged_channels_above/2`, which reads the channels each
   Cell wrote on from the stage entries' `cells_ran`, never from the Cell WALs compaction may
   have truncated; the refusal names the channels). A rewind is undo-and-continue: nothing
   re-runs over the unwound inputs, so no later output overwrites the original by its key, and
   the hook is the only undo the engine can guarantee.
   A class that declared nothing (`nil`) is not a chain with nothing compensable (`[]`). Neither
   refusal touches the journal.
2. **The marker.** `TaskJournal.append_rewound/2` appends `rewound(N)` at `current_epoch + 1` with
   a synchronous fsync. The fold purges accumulated entries with `iter > N` and keeps everything
   appended after the marker, so a rewound iteration's records are no longer named by a live
   entry, and the rebind drops those whose outputs were not yet handed to the plugin chain. The
   marker's epoch keys the barrier identity of the re-run; an output's `flz.epoch` is its Cell's
   and does not move.
3. **Evict and rehydrate unconditionally**: a rebind, not a park: no park record, no wake
   timer, and the loop comes back with no input waiting because it must run the undo.

The undo list rides `TaskJournalState.rewind_undo`: the tasklets committed before the marker at
an iteration above `N`, minus those with a `compensated` entry appended after their own commit.
The instance runs them through the ordinary planner under a **forced** `AllOrNothing` (reverse
iteration, reverse position) and journals one `compensated` entry per tasklet — an empty one
for a hookless tasklet, which is what drains the list. It does not consult the per-cell done
markers: a cell that ran in two rewound iterations is undone once per iteration. A crash
mid-rewind resumes with exactly the outstanding work; there is no done marker.

A rewind **does not replay inputs**: every iteration commit raises the instance's floor in its
input log and a rewind never lowers it, so a committed turn's message is not delivered again
even while the log still retains it. Rewind is undo-and-continue — the loop resumes
waiting and the next message becomes iteration `N + 1`. Cell state is unwound by the hooks,
not by truncating the state WAL; a hookless tasklet leaves its state change in place.
Telemetry `[:flezha, :bounded, :rewind]` (measurement `epoch`); `flz state journal` renders `rewound`.

## Saga: compensation on abnormal termination

A tasklet declares `compensate(ctx, completed_output, state)`; `rollback_mode()` /
`rollbackMode()` selects `AllOrNothing` (default) or `KeepProgress`. Both are journal-backed: the
planner's `committed` input is the journaled tasklets grouped by iteration, so `KeepProgress`
keeps every committed iteration and `AllOrNothing` unwinds them after the in-flight one. For a
single-pass chain the two coincide.

**A partitioned loop.** Each member of a partitioned `per_input` bounded operator rolls back its own
passes only. A rescale seals every surviving member's journal at its latest committed iteration,
because a Cell moved to another member takes its earlier passes out of this member's reach: after
a rescale, `AllOrNothing` unwinds only the iterations committed since, and `flz operator rewind`
refuses an iteration below the seal with the committed range. The scale command names the
members it sealed. A loop whose passes must stay undoable across the rescale must not be
rescaled.

- The plan is the Kahn order reversed — a join before any of its producers, a shared root last,
  independent subtrees in unspecified relative order. Hooks run once per cell that had an
  effect, inside the single teardown. A hookless tasklet is skipped; a failing hook is logged
  and the rest of the plan still runs. A clean `ok` never compensates.
- Each stage commits on its own, so a sibling can be committed, its outputs released, before the
  stage beside it fails; it is then undone like any committed stage, in reverse completion
  order. A stage still running when the pass fails is recorded as having run, so it is
  compensated, and is not committed. A cooperative stop takes the same path, and the undo is
  never interrupted by the cancel token, because rollback is the response to that cancel.

**Crash-safe compensation** uses per-Cell markers on the state substrate: `compensation_intent`
(`<cell>.comp_intent`) when a compensable tasklet completes forward, `compensation_done`
(`<cell>.comp_done`) when its undo finishes. They ride `RestoreInitState.has_compensation_intent`
/ `has_compensation_done` on the rebind, and the instance skips every cell whose done marker
landed. A shipped **done** marker proves an undo pass was in flight and wins over journal-gated
forward resume; intent markers alone do not, because they also accumulate during forward
progress. Tasklets the journal skipped still roll back on a later abnormal exit, folded into
the plan from their intent markers. The undo record (`TaskCommit` with `compensating = true`,
journaled as `compensated`) is written **before** the done marker, so a crash between the two
re-runs the idempotent hook rather than losing its undo output. Author contract: a
journal-rebuilt tasklet's `completed_output` is empty on resume (its inter-tasklet output
was never durable), so a durable compensable tasklet keeps its undo handle in Cell state,
and hooks are idempotent.

## Durability: effect records, task.journal, outbox, crash-resume

A class is durable by default and opts out with `PERSISTENCE_ENABLED = false` /
`persistenceEnabled()`, the switch an unbounded class carries too; the runner announces it
(`persistent = "false"` in the `AddOperator` response) and the controller resolves it in
`PersistenceResolver.persistence_enabled?/1` (user override available). Opted out, the
per-Cell flush drains and discards (no WAL, snapshot, journal or handshake); a crash re-runs
from the top, a live Cell read answers `{:error, :persistence_off}`, and the class cannot sleep.

**The commit path.** Every item a bounded operator Cell settles is one `cell_effect_record` (its
state changes and its plugin outputs with channel, key and metadata), flushed inline by the
worker that settled it rather than on the flush linger. When a stage's last item settles, that
worker ships `TaskCommit` (`task_idx`, `iter`, `cells_ran` naming each Cell's record range in the
stage and the plugin channels those records wrote on, the handoff through the channel's codec;
no output); nothing waits for the ack, which is an event on the control-channel reader
that releases the stage's outputs into its children.

| Step | Where | Detail |
| --- | --- | --- |
| decode, hand over | control-channel reader | casts to `ControlChannel.Commits` and returns — no disk I/O on the reader, so a commit storm never delays a ping, stop, park or compaction |
| records | `Commits` (one per runner) | before a `task_committed` or `compensated` entry, `WalWriter.await_durable_async/4` for every named `{cell, seq}` on its Cell's writer, answered once the record's batch has synced; a refusal, an unreachable writer or `FLEZHA_BOUNDED_ACK_TIMEOUT_MS` elapsing fails the barrier with `record_not_durable: <cell> seq=<n>`, and a re-sent barrier whose first copy still waits joins that wait |
| append | `Commits` | once every named record is durable, `TaskJournal.append_async/4`, `sync: :batched`: the reply is deferred until the `FsyncBatcher` tick, a prefix cut or a clean close syncs the batch; the worker's request collection is the set of acks owed and it never waits on a writer |
| identity | journal writer | the 16-byte header `[epoch u32][type u8][task_idx u16][iter u32]` is the dedup key: a re-sent barrier is answered from the existing entry, never journaled twice |
| ack | written by the channel | `TaskCommitResponse` under the request's `reference_id`; a non-empty `error` (write failure, `{:oversized_handoff, size, max}`, `record_not_durable`) terminates the run |
| sync remote | a task, before the ack | `SYNC_DURABILITY = true` / `syncDurability()` (`durability: sync_remote` in the declaration) has `DurabilityTier.commit_sync/1` ship the journal suffix to the [remote tier](data_durability.md) first — RPO 0, bounded classes only |
| release | after the ack, on the worker | `WalWriter.commit_records/3` for each named range: the Cell's writer releases those records' outputs to the plugin chain and drops a held entry of that Cell below the range (an attempt that never committed) |

Each send arms a deadline of `FLEZHA_BOUNDED_ACK_TIMEOUT_MS` (default 30 000) on the poll core's
deferred-wake list. One that comes due with the ack still owed re-sends the same frame under the
same `reference_id`, up to three sends in all, a constant of the runner's controller connection
rather than a class setting; the ack stays registered across the re-sends, so a late one
continues the chain, and only the third expiry is a terminal error. No channel declares how its handoff is encoded — the runtime derives the
journal codec, in Rust from the producing tasklet's declared `Output` type — and a journal entry
is bounded by `:wal_high_watermark_bytes` (16 MiB): keep bulk data in Cell state and hand off
references. Every released output carries the identity an unbounded operator's does: `flz.cell`,
`flz.seq = <record seq>.<emit ordinal>`, `flz.epoch` (the Cell's realign epoch, which a rewind
does not move), `channel`, `flz.key` when the channel is keyed, and the author's metadata. An
undo output is a newer record of the compensating Cell, so its seq sorts above the forward
output's. An opted-out class sends its outputs straight to the wire with the same channel, key
and metadata. Flush contract: [`data_durability.md`](data_durability.md#bounded-operators-one-record-released-at-the-stage-commit).

**Crash-resume.** Journal state rides `AddOperator.task_journal` on the bind — not a per-Cell
restore, because a class whose tasklets hold no per-Cell state receives no
`RestoreInitState` at all. Committed tasklets are skipped, the first uncommitted one is seeded
from the journaled handoff, and only the tasklet mid-flight at the crash re-runs. A
restore-before-run gate (`AddOperator.resume_cell_count`) holds the first tasklet until
that many `RestoreInitState` frames land. The persisted Cell set includes keyed Cells, and a
frame naming one mints it before it folds; the gate counts frames, not Cells, so a frame the
`max_cells` cap refuses, or one naming a tasklet the class no longer declares, still counts.
The last frame opens the gate on the reader that brings it,
and a 30 s deadline on the deferred-wake list degrades a restore that never lands to a
best-effort re-run. A restored Cell re-arms its change seq from `RestoreInitState.terminal_seq`, so
post-resume appends are contiguous and the unbounded-only seq-gap resync never applies. Execution
is at-least-once: steps and hooks are idempotent by author contract. A crashed instance reaches
a new runner in one of two ways: a restarted runner presents the same stable identity
(`FLEZHA_RUNNER_ID_PATH`) with a new `incarnation`, and the Overwatch binds onto it at the
connect; or the convergence loop's `rebind_operator` moves it onto another runner once the dead
session is gone. Both are the full bind, `Lifecycle.complete_init/5`, which stamps
`resume_cell_count` and the journal from durable state, ships the persisted Cells with their
compensation markers and re-forwards the input log's undelivered tail, and the instance resumes
past the journaled tasklets — or resumes the compensation — to a terminal `BoundedCompleted`.
A reconnect of the same process (the same `incarnation`) only moves the binding, because that
process still runs the instance.
A live instance and a resumed one are the same data: the pass the journal describes is the
pass the instance runs.

Telemetry: `[:flezha, :state, :task_commit]` (measurements `count`, `handoff_items`, `cells`),
`[:flezha, :state, :iter_commit]`,
`[:flezha, :bounded, :completed | :failed | :parked | :suspended | :rehydrating]`.

## Finished-job retention

On terminal teardown the controller writes `Flezha.State.Completion`
(`<state_root>/<namespace>/<operator_class>/completion.record` — `status`, `error`, `result`,
`finished_at`)
before the teardown, closes the instance's writer processes, and **retains** the state instead
of deleting it. `Flezha.State.Retention` reclaims state and record once `finished_at + window`
has passed. During the window `flz operator info <id>` reports phase `finished` with the status,
result and finish time, and the convergence loop does not read the retained state as an intent
mismatch. An opted-out class leaves no state to retain; its record is best-effort.

| Knob | Default |
| --- | --- |
| `:default_bounded_completed_retention_hours`, env `FLEZHA_BOUNDED_RETENTION_HOURS` | 72 |
| `:bounded_completed_retention_hours` — `%{<class> => hours}` per-class override | none |

## What decides each behaviour

No behaviour is attached to a kind. Each follows from what the class declares or from the
instance's own state:

| Behaviour | Decided by |
| --- | --- |
| per-Cell WAL, bind-time restore, live Cell reads (`flz state get`) | the class's persistence switch |
| sleeping when idle, and the durable input a sleeping unbounded operator keeps | the class's sleep setting |
| point-in-time queries and history retention (`flz state at`, `time_travel_retention_hours`) | nothing: a bounded operator Cell keeps history like an unbounded operator Cell, then finished-job retention reclaims the directory |
| dynamic Cells and their destructive idle eviction | nothing: a bounded operator evicts only between passes and never within a rollback's reach, and the controller deletes the evicted Cell's state |
| output-stall watch and the forced rebind it drives | nothing: a wedged bounded operator is rebound and resumes at its stage |
| input-silence sweep | the instance's state: never a sleeping instance, never a checkpoint-ack class between checkpoints |

## How many instances a runner holds

An instance holds no thread. Its run is a small pass sheet on the instance (iteration, stages
in flight and done, held outputs, completion, phase) under one lock, advanced by whichever
thread brings the next event: a worker settling a stage's last item, the control-channel reader
bringing a commit ack or a stop, the data-channel reader bringing a message, a poller promoting a
deadline. None of them waits and none runs author code while it holds the lock. Measured on
8 cores / 32 GiB, release build, with the standing probe:

```text
FLEZHA_SCALE_SHAPE=step FLEZHA_SCALE_N=10000 \
  cargo bench -p flezha_runner --features bench-internals,testing \
    --bench bounded_scaling
```

| 10 000 instances | blocked mid-step (`step`) | waiting for a message (`idle`) |
| --- | --- | --- |
| resident per instance | ~61 KiB | ~61 KiB |
| threads per instance | 0 (the workers running the steps are the pool's) | 0 |
| CPU while blocked | 0 % | 0 % |
| start / teardown | 0.35 s / 0.06 s | 0.35 s / 0.03 s |
| an unrelated instance's p99 beside them | 0.8x its unloaded latency | 0.9x |

**Size a runner by what its turns hold, not by instances.** A waiting instance costs its heap
and nothing else. A turn blocked on a slow call holds the SDK worker running it — a pool thread
in Rust, a virtual thread in Java — and one in-flight slot: `FLEZHA_MAX_INFLIGHT_ACTIVATIONS`
(default 1 024) bounds how many cells are handed out at once, for both kinds, so a runner meant
to hold more blocked turns than that raises it with its worker pool. `max_duration()` is an entry
on the deferred-wake list, not a thread. Edge transforms (`map`/`filter`/`flat_map`/merge
mapping) run on the worker that produced the value, as on an unbounded operator.

## Operations notes

- **Crash-resume is at-least-once.** A non-idempotent fire-and-forget job can double-act on
  resume: write it idempotently, or opt out of durability so a crash re-runs from a clean slate.
- **Stop runners gracefully** ([`../operations/graceful_shutdown.md`](../operations/graceful_shutdown.md)):
  a graceful stop lets a running instance migrate; a SIGKILL forces crash-resume.
- **Retention grows disk for many small jobs.** Opt out for fire-and-forget, or lower
  `FLEZHA_BOUNDED_RETENTION_HOURS`. Bounded operator is not the high-throughput path.
- **A sleeper costs a bind to stop.** Stopping a suspended bounded operator restores it onto a runner
  first, so it can end `cancelled` exactly as it would awake; with no runner it is removed hard.
- **History grows with the retention window.** A bounded operator Cell keeps point-in-time history for
  `time_travel_retention_hours`; set it to `0` for a class that wants only its latest snapshot.
- **Proof:** `test/system/bounded_crash_resume_e2e_test.exs` and
  `test/system/bounded_control_flow_e2e_test.exs` — SIGKILL mid-run and mid-compensation, the
  fan-in diamond, the outbox re-drive, cell retry, park and rehydrate, stopping a sleeper,
  rewind; `test/system/sleep_when_idle_e2e_test.exs` for a sleeping unbounded operator and the persistence
  switch; on every SDK runtime, UDS and QUIC.

## See also

- [`../architecture/bounded_profile.md`](../architecture/bounded_profile.md) — the concepts and the wire table
- [`data_durability.md`](data_durability.md) — the scenario matrix, the durable input log and the outbox
- [`../internals/state_persistence.md`](../internals/state_persistence.md) — the WAL substrate, `FsyncBatcher`, retention
- [`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) — field-level `TaskCommit` / `IterCommit` / `IterPark`
- [`data_durability.md`](data_durability.md) — `sync_remote`
