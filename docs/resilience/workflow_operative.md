# Workflow Operative

The mechanism-level and operator-level reference for the workflow operative kind: what the
controller and the shared runner core do at each tasklet barrier, iteration boundary, park,
rewind and crash, and the knobs, files, CLI verbs and telemetry an operator has — for operators
and for contributors to `flz_core/src/workflow/` and `lib/flezha/operative/`. The concepts (the
graph, the journal layout, the barrier, the outbox, loop modes, suspension, rewind) are described
once in [`../architecture/workflow_profile.md`](../architecture/workflow_profile.md) and not
repeated here; [`data_durability.md`](data_durability.md) says what is lost when.

## Structure: a DAG of tasklets, parallel cells

- **Numbering.** Validation orders the tasklets in Kahn topological order from the entry; the
  position is the wire `task_idx`. A producer's index is always below its consumers', so
  completion order is topological and compensation simply reverses it.
- **Scheduling.** A stage's items reach its cells the way a stream's do: through the routing
  seam into each cell's mailbox, handed out by the poll core under per-cell exclusivity,
  fairness and the in-flight cap (`FLEZHA_MAX_INFLIGHT_ACTIVATIONS`), with no queue of their
  own. A stage commits when its own last item settles, and what it emitted waits on the
  instance until that commit is acknowledged, then enters its children's mailboxes. A node runs
  once every producer feeding it in this pass has committed, a join with every producer's
  items. Sibling stages run and commit independently. Where a stage runs:
  [`../architecture/runner.md`](../architecture/runner.md#a-workflow-stage).
- **Done.** The operative is `ok` when every leaf tasklet has committed. The SDK declares its
  leaf set (`leaf_tasks`) and compensable set (`compensable_tasks`) in the `AddOperative` response
  metadata, so the controller derives the same fact from the journal. A join with one committed
  and one uncommitted producer resumes with the committed handoff banked, never re-run.
- **Self-termination.** `self_terminating()` / `selfTerminating()` defaults to `true`; `false`
  with no `max_duration` is refused at registration.

## Lifecycle: exit triggers, inbox, one teardown

An instance is spawned with `init_values` as its tasklets' initial state, as a stream is,
from the CLI (`flz operative start`), a schedule (`flz schedule`) or a running tasklet
(`ctx.spawn_workflow`, [`../sdk/tasklet_requested_spawn.md`](../sdk/tasklet_requested_spawn.md)),
takes its input as messages on its entry channels (the first may ride the start:
`flz operative start --text`, a schedule's stored `--text`, a spawn's payload) and terminates
on the first of: chain quiescence (every leaf committed); a tasklet's
`ctx.complete(result)`, honoured once its iteration commits; a loop that runs out of work (a
chained loop's empty batch); `max_duration()` / `maxDuration()`
elapsing (a cooperative cancellation token); `flz operative stop` (a cooperative `WorkflowStop`
that trips the same token — an instance not bound to a runner takes the hard `RemOperative`
teardown, `OperativeManager.request_stop/3`).

Every trigger converges on one `WorkflowCompleted` (`status` `ok | error | cancelled`, `error`,
`result`). The controller writes the completion record, then tears down off the control-channel
reader, **settling both output debts first** (`ControlChannel.drain_owed_outputs/2`). They are
settled differently. The data stream is *flushed*: `Placement.flush_stream_channels/1` issues one
bounded call per stream channel of the runner, and a channel answers only after every frame
already in its mailbox has been routed to an Overwatch — so an opted-out class's
last direct output is run before its Overwatch is stopped, however the two sockets interleave on
arrival, and even when an output of that run was lost rather than delivered. The journal is
*waited for*: up to 60 s, re-driving `TaskJournal.flush_pending/1`, until
`TaskJournal.pending_entries/1` is empty. A deadline hit is logged (`... still owed`) and the
retained journal keeps the debt. `status = "error"` tears down identically — there is no
platform retry of a failed tasklet. A frame addressed to a running instance takes the
stream's entrance (channel resolved, parsed, transformed, parked when it cannot enter), never
dropped: it starts a pass when the instance is waiting at a committed boundary, and otherwise
waits in the instance's queue for the next one. Every loop mode reads it: `Once` its one
message, `PerInput` one per iteration, `Chained` the first and then its own carry. An instance
with no message waits, and a second message to a `Once` instance is accepted and never read.

## Durable loop and suspension

Loop modes are declared with `loop_mode()` / `loopMode()`. Registration refuses a class that
declares no tasklet (`NoTasklets`) and a durable `Chained` loop whose terminal tasklet
releases no inter-tasklet channel (`LoopTerminalWithoutHandoff`). Two shapes fail that: a
terminal nothing consumes and that names no sink owns no output channel at all, and one whose only
wiring is a plugin sink gets no journal codec, because the runtime attaches one to inter-tasklet
channels and nothing else. Either way a crash would re-seed empty, read as converged.

**The iteration boundary.** `IterCommit` rides the same commit worker as `TaskCommit` (below).
After the ack the worker runs the post-ack cleanup in order: a boundary snapshot
(`CompactCell` per Cell, in a task, best-effort — a failed compaction means the rehydrate
replays the WAL tail), the outbox flush in journal order, the report of the iteration's
consumed offset into the input log's watermark (`TaskJournal.release_input/2`) and the journal
cut (`TaskJournal.cut/1`). An iteration that consumed no journaled input (a carry-fed pass)
reports nothing. Both files are cut by their own writers — the journal on this cast, the input
log by its retention sweep below the watermark — so the worker never waits on a file rewrite,
and a prefix cut below `:record_log_cut_after_bytes` (64 KiB) is deferred until that many
released bytes accumulate.
The journal is cut only for a class whose `compensable_tasks` is `[]` (a saga or a rewind
replans from the committed history); a cuttable loop can only be rewound to its current
iteration.

**The durable input log.** `Ingest` holds the one append-before-forward seam, and a durable
workflow uses the input log a stream on the effectively-once tier uses: input goes to
`<state_root>/<ns>/<inst>/input.log` (`Flezha.State.InputLog`) before it is forwarded,
answered once synced and stamped `flz.offset` (0-based; absent means not journaled, never
`0`); a failed append is not forwarded. A repeat of a `flz.msg_id` the log still retains is
absorbed, from an in-memory index the log builds from the file on the first append that
carries an id; a message without an id is always appended. The instance's floor is the log's
watermark under the journal's identity, seeded at every bind from the journal's **released**
offset (the maximum consumed offset of any iteration commit or park over the raw journal, which
no rewind lowers) and raised by every commit and park, so a consumed message is never
re-delivered. The appends debit the source credit window like a stream's, so a sender with
16 MiB owed by one instance parks until it commits.

**Suspension.** An instance of any loop mode whose inbox stays empty past `FLEZHA_WORKFLOW_PARK_IDLE_MS`
(default 5 000; read in the shared core, so Rust and Java agree, and by the Elixir SDK's own driver) at a committed boundary tears
its run down without a completion and ships `IterPark`, naming the last message it parked to
the dead-letter queue unread. The controller journals
`parked(iter[, wake_deadline_ms], consumed_offset)` and reports that offset into the input
log's watermark, releasing the message as an iteration commit releases its input, **before**
`Migration.suspend/3` evicts the instance
(`RemOperative`, runner tag `nil`); a park whose append fails leaves the instance bound and
logged. The Overwatch and the registry entry stay, and the Overwatch **hibernates**
(`:hibernate` on both suspension entries, releasing its bind-time heap).

| Surface | While suspended |
| --- | --- |
| `flz operative info` | phase `suspended` |
| convergence loop | skipped (`suspended?`), plugin diff included — its plugins left with the runner and the rehydrating bind replays them |
| namespace hub | outlives its runner: `HubManager.stop_hub_if_vacant/1` stops a hub only with no operative registered, so a parked fleet survives its sole runner dying |
| `flz state get` | refused — no live Cell |
| removing its factory from the runner | the SDK's live-instance check cannot see a suspended instance, so the removal succeeds and the later rehydrate fails placement until a runner re-defines the factory; `FleetManager` warns `dropped factory ... that still has controller-known operatives` on the `UpdateFactories` frame. A blocking policy is deferred |

**Rehydrate = bind.** A new input-log append, the wake deadline, a crash and a migration all go
through `Lifecycle.complete_init/4`: respawn on any live runner, `AddOperative.task_journal`
with the folded journal, `resume_cell_count` Cell restores, and the one re-forward every bind
of either kind runs (`Replay.reforward_undelivered_tail/1`): the log's records above the floor,
each with its original `flz.offset`, before the plugins come up.
`AddOperative.inbox_tail_count` (the log's last offset minus the floor) tells the instance how
many items it is owed; while any is still owed its park deadline is 60 s instead of the park
idle, so a tail forwarded under load never meets an instance that already parked. An instance with
undelivered input is never left suspended (`rehydrate_if_pending/1` runs at every park and at
boot, and asks the log's tail rather than reading its records), and a park that lands while an
un-park is still binding wins (`parked_in_flight` discards the task's stale bound state; the
pending tail wakes it again). A workflow's journal, Cells and input log share one key, its
instance id, so a bind whose `:operative_class` names anything else is refused.

**The wake timer** (`wake_after()` / `wakeAfter()`) is journaled with the park record and
re-armed with `Process.send_after` from the journal at boot, so it survives a controller
redeploy. It fires by appending a synthetic empty input-log record (`flz.wake = timer`), so a racing
message and timer collapse into one rehydration. At boot an instance whose journal ends in
`parked` with nothing undelivered (`Migration.suspended_on_disk?/1`) enters suspended without
binding; undelivered input outranks the park. Inspect with `flz state journal <operative/instance>`
(entries tagged with their iteration, `iter_committed` with its consumed offset or "consumed no
journaled input", the final `parked`) and `flz state inbox <operative/instance>` (the input log's
retained records, and which are still above the floor). `flz operative replay` refuses a
workflow Cell, since its baseline is a journal iteration, not an effect record; `flz operative
rewind` is a loop's way back.

The wake timer is the one timer a workflow has. Cell timers do not fire inside a workflow yet,
so a timer a workflow tasklet registers is refused rather than dropped: every SDK's ctx answers
`TimersUnavailable`, and the shared core fails an item whose effect still carries a timer op
with a non-retryable error of that name, so the run compensates and ends in `error`.

## Cell retry: the rung before the abort

A failing item inside a **structured** tasklet is retried in place on the poll core's
per-item ladder, the one every stream cell climbs, while what its finished siblings emitted
waits on the instance for the stage's commit. The policy is the tasklet's input edge's, as on
a stream; a workflow class declares none of its own.

| Knob | On the edge | Default |
| --- | --- | --- |
| total attempts, first run included | `retry_max(n)` / `retryMax(n)` | 3; `1` is fail-fast, `0` is refused (`RetryMaxZero`) |
| backoff base | `retry_backoff_ms(ms)` / `retryBackoffMs(ms)` | 50 ms; attempt `n` waits `base × 2^(n-1)` ±50 % jitter, capped at 5 s (`RETRY_BACKOFF_MAX_MS`) |

- `TaskletError::non_retryable(..)` skips the ladder; every other error — a panic included,
  which the author never got to classify — is retryable. Where a stream parks an exhausted
  input to the dead-letter queue, a workflow fails the tasklet and compensates.
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
  the retry counter the stream ladder feeds, shipped in `data_heartbeat` metadata.

An exhausted ladder fails the stage where a stream would park the item: no commit, nothing
leaked, compensation, status `error`. Nothing workflow is ever written to the DLQ store — abort-and-compensate is the escape
where a stream operative would [park the payload](data_durability.md#dead-letter-queue-poison-payloads-park-never-drop).

## Rewind: taking a loop back to a committed iteration

```text
flz operative rewind <instance> --to-iteration N
```

`Migration.rewind_to/2` runs, in order:

1. **The guard**, before anything durable changes. `N` must be a committed iteration (the refusal
   names the committed range), and every tasklet above `N` that staged plugin output must be in
   `compensable_tasks` (`TaskJournal.staged_channels_above/2`; the refusal names the channels) —
   a workflow emit carries no `flz.key`, so the hook is the only undo the engine can guarantee.
   A class that declared nothing (`nil`) is not a chain with nothing compensable (`[]`). Neither
   refusal touches the journal.
2. **The marker.** `TaskJournal.append_rewound/2` appends `rewound(N)` at `current_epoch + 1` with
   a synchronous fsync. The fold purges accumulated entries with `iter > N` and keeps everything
   appended after the marker, so a rewound iteration's unflushed outbox ceases to be owed.
3. **Evict and rehydrate unconditionally** — a rebind, not a park: no `parked` entry, no wake
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
Telemetry `[:flezha, :workflow, :rewind]` (measurement `epoch`); `flz state journal` renders `rewound`.

## Saga: compensation on abnormal termination

A tasklet declares `compensate(ctx, completed_output, state)`; `rollback_mode()` /
`rollbackMode()` selects `AllOrNothing` (default) or `KeepProgress`. Both are journal-backed: the
planner's `committed` input is the journaled tasklets grouped by iteration, so `KeepProgress`
keeps every committed iteration and `AllOrNothing` unwinds them after the in-flight one. For a
single-pass chain the two coincide.

- The plan is the Kahn order reversed — a join before any of its producers, a shared root last,
  independent subtrees in unspecified relative order. Hooks run once per cell that had an
  effect, inside the single teardown. A hookless tasklet is skipped; a failing hook is logged
  and the rest of the plan still runs. A clean `ok` never compensates.
- Each stage commits on its own, so a sibling can be committed, its outbox flushed, before the
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

## Durability: staged commit, task.journal, outbox, crash-resume

A class is durable by default and opts out with `PERSISTENCE_ENABLED = false` /
`persistenceEnabled()`; the controller resolves it in `PersistenceResolver.persistent_state?/3`
(operator override available). Opted out, the per-Cell flush drains and discards — no WAL,
snapshot, journal or handshake — and a crash re-runs from the top.

**The commit path.** When a stage's last item settles, the worker that settled it ships
`TaskCommit` (`task_idx`, `iter`, `cells_ran`, the handoff through the channel's codec, the
staged `outbox`); nothing waits for the ack, which is an event on the control-channel reader
that releases the stage's outputs into its children.

| Step | Where | Detail |
| --- | --- | --- |
| decode, hand over | control-channel reader | casts to `ControlChannel.Commits` and returns — no disk I/O on the reader, so a commit storm never delays a ping, stop, park or compaction |
| append | `Commits` (one per runner) | `TaskJournal.append_async/4`, `sync: :batched`: the reply is deferred until the `FsyncBatcher` tick, a prefix cut or a clean close syncs the batch; the worker's request collection is the set of acks owed and it never waits on a writer |
| identity | journal writer | the 16-byte header `[epoch u32][type u8][task_idx u16][iter u32]` is the dedup key: a re-sent barrier is answered from the existing entry, never journaled twice |
| ack | written by the channel | `TaskCommitResponse` under the request's `reference_id`; a non-empty `error` (write failure, `{:oversized_handoff, size, max}`) terminates the run |
| sync remote | a task, before the ack | `SYNC_DURABILITY = true` / `syncDurability()` (`durability: sync_remote` in the declaration) has `DurabilityTier.commit_sync/1` ship the journal suffix to the [remote tier](../operations/remote_durability_tier.md) first — RPO 0, workflow classes only |
| flush | after the ack, on the worker | `TaskJournal.flush_pending/1` delivers in journal order and writes `outbox_flushed`; an unbound operative retries every 1 s |

Each send arms a deadline of `FLEZHA_WORKFLOW_ACK_TIMEOUT_MS` (default 30 000) on the poll core's
deferred-wake list. One that comes due with the ack still owed re-sends the same frame under the
same `reference_id`, up to three sends in all, a constant of the runner's controller connection
rather than a class setting; the ack stays registered across the re-sends, so a late one
continues the chain, and only the third expiry is a terminal error. No channel declares how its handoff is encoded — the runtime derives the
journal codec, in Rust from the producing tasklet's declared `Output` type — and a journal entry
is bounded by `:wal_high_watermark_bytes` (16 MiB): keep bulk data in Cell state and hand off
references. Every flushed item is stamped
`flz.cell`, `flz.seq = <task_idx>.<seq_in_task>` (an undo's seq carries a trailing `.1`,
numeric so the Postgres sink's guard still orders it), `flz.epoch` and `channel`; there is no
`flz.key`. Flush contract: [`data_durability.md`](data_durability.md#workflow-operatives-staged-commit-and-the-transactional-outbox).

**Crash-resume.** Journal state rides `AddOperative.task_journal` on the bind — not a per-Cell
restore, because a class whose tasklets hold no per-Cell state receives no
`RestoreInitState` at all. Committed tasklets are skipped, the first uncommitted one is seeded
from the journaled handoff, and only the tasklet mid-flight at the crash re-runs. A
restore-before-run gate (`AddOperative.resume_cell_count`) holds the first tasklet until
that many `RestoreInitState` frames land. The persisted Cell set includes keyed Cells, and a
frame naming one mints it before it folds; the gate counts frames, not Cells, so a frame the
`max_cells` cap refuses, or one naming a tasklet the class no longer declares, still counts.
The last frame opens the gate on the reader that brings it,
and a 30 s deadline on the deferred-wake list degrades a restore that never lands to a
best-effort re-run. A restored Cell re-arms its change seq from `RestoreInitState.terminal_seq`, so
post-resume appends are contiguous and the stream-only seq-gap resync never applies. Execution
is at-least-once: steps and hooks are idempotent by author contract. A crashed instance reaches
a new runner when a replacement presenting the same stable identity (`FLEZHA_RUNNER_ID_PATH`)
has the dead session's bindings swapped onto it — promptly on UDS (socket EOF), after the 45 s
control idle timeout on QUIC; the Overwatch re-binds with `Lifecycle.complete_init/4`, which
stamps `resume_cell_count` from the on-disk WAL and ships the persisted Cells with their
compensation markers (in production the convergence loop drives this), and the instance resumes
past the journaled tasklets — or resumes the compensation — to a terminal `WorkflowCompleted`.
A live instance and a resumed one are the same data: the pass the journal describes is the
pass the instance runs.

Telemetry: `[:flezha, :state, :task_commit | :iter_commit | :task_outbox_flush]`,
`[:flezha, :workflow, :completed | :failed | :parked | :suspended | :rehydrating]`.

## Finished-job retention

On terminal teardown the controller writes `Flezha.State.Completion`
(`<state_root>/<namespace>/<operative_class>/completion.record` — `status`, `error`, `result`,
`finished_at`)
before the teardown, closes the instance's writer processes, and **retains** the state instead
of deleting it. `Flezha.State.Retention` reclaims state and record once `finished_at + window`
has passed. During the window `flz operative info <id>` reports phase `finished` with the status,
result and finish time, and the convergence loop does not read the retained state as an intent
mismatch. An opted-out class leaves no state to retain; its record is best-effort.

| Knob | Default |
| --- | --- |
| `:default_workflow_completed_retention_hours`, env `FLEZHA_WORKFLOW_RETENTION_HOURS` | 72 |
| `:workflow_completed_retention_hours` — `%{<class> => hours}` per-class override | none |

## Capability split

`Flezha.Operative.Kind` declares per-kind capabilities; an undeclared flag reads `false`, and an
unknown class defaults to `:stream`, so every gate is subtractive.

| Flag | Meaning | `:stream` | `:workflow` |
| --- | --- | --- | --- |
| `persistent_state?` | per-Cell WAL + restore, state introspection | always | durable by default, per-class opt-out (`PersistenceResolver.persistent_state?/3`) |
| `time_travel?` | point-in-time queries, retention beyond the latest snapshot (`flz state at`) | yes | no |
| `dynamic_cells?` | lazy per-key Cells, destructive idle eviction | yes | runner side yes (evicted only between passes and never within a rollback's reach); the controller refuses the evicted Cell's state delete and keeps the state, a gate that goes with this table |
| `output_stall_watch?` | per-Cell output-staleness watchdog → forced rebind | yes | no |

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
    --bench workflow_scaling
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
mapping) run on the worker that produced the value, as on a stream.

## Operator notes

- **Crash-resume is at-least-once.** A non-idempotent fire-and-forget job can double-act on
  resume: write it idempotently, or opt out of durability so a crash re-runs from a clean slate.
- **Stop runners gracefully** ([`../operations/graceful_shutdown.md`](../operations/graceful_shutdown.md)):
  a graceful stop lets a running instance migrate; a SIGKILL forces crash-resume.
- **Retention grows disk for many small jobs.** Opt out for fire-and-forget, or lower
  `FLEZHA_WORKFLOW_RETENTION_HOURS`. Workflow is not the high-throughput path.
- **Proof:** `test/system/workflow_crash_resume_e2e_test.exs` and
  `test/system/workflow_control_flow_e2e_test.exs` — SIGKILL mid-run and mid-compensation, the
  fan-in diamond, the outbox re-drive, cell retry, park and rehydrate, rewind; on Rust and
  Java, UDS and QUIC.

## See also

- [`../architecture/workflow_profile.md`](../architecture/workflow_profile.md) — the concepts and the wire table
- [`data_durability.md`](data_durability.md) — the scenario matrix, the durable input log and the outbox
- [`../internals/state_persistence.md`](../internals/state_persistence.md) — the WAL substrate, `FsyncBatcher`, retention
- [`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) — field-level `TaskCommit` / `IterCommit` / `IterPark`
- [`../operations/remote_durability_tier.md`](../operations/remote_durability_tier.md) — `sync_remote`
