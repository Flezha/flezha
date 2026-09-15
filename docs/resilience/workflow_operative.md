# Workflow Operative

The mechanism-level and operator-level reference for the workflow operative kind: what the
controller and the shared runner core do at each procedure barrier, iteration boundary, park,
rewind and crash, and the knobs, files, CLI verbs and telemetry an operator has — for operators
and for contributors to `flz_core/src/workflow/` and `lib/flezha/operative/`. The concepts (the
graph, the journal layout, the barrier, the outbox, loop modes, suspension, rewind) are described
once in [`../architecture/workflow_profile.md`](../architecture/workflow_profile.md) and not
repeated here; [`data_durability.md`](data_durability.md) says what is lost when.

## Structure: a DAG of procedures, parallel entities

- **Numbering.** Validation orders the procedures in Kahn topological order from the entry; the
  position is the wire `proc_idx`. A producer's index is always below its consumers', so
  completion order is topological and compensation simply reverses it.
- **Scheduling.** A node is runnable once every producer feeding it has its commit acknowledged.
  Every runnable node is submitted together — one step activation per entity, on the poll core's
  direct queue, exempt from the in-flight cap — and joined on one barrier before the level
  commits. A join is dispatched once, with every producer's batch appended to its input. Where a
  step runs: [`../architecture/runner.md`](../architecture/runner.md#a-workflow-step).
- **Done.** The operative is `ok` when every leaf procedure has committed. The SDK declares its
  leaf set (`leaf_procs`) and compensable set (`compensable_procs`) in the `AddOperative` response
  metadata, so the controller derives the same fact from the journal. A join with one committed
  and one uncommitted producer resumes with the committed handoff banked, never re-run.
- **Self-termination.** `self_terminating()` / `selfTerminating()` defaults to `true`; `false`
  with no `max_duration` is refused at registration.

## Lifecycle: exit triggers, inbox, one teardown

An instance is spawned with `args` (`init_values` seeds the root) from the CLI
(`flz operative start`), a schedule (`flz schedule`) or a running procedure
(`ctx.spawn_workflow`, [`../sdk/procedure_requested_spawn.md`](../sdk/procedure_requested_spawn.md))
and terminates on the first of: chain quiescence (every leaf committed); `ctx.complete(result)`;
a loop terminal predicate (`ctx.loop_until(pred, body)` on a `run`-sugar body, which blocks on
its inbox with a 200 ms wake to observe cancellation); `max_duration()` / `maxDuration()`
elapsing (a cooperative cancellation token); `flz operative stop` (a cooperative `WorkflowStop`
that trips the same token — an instance not bound to a runner takes the hard `RemOperative`
teardown, `OperativeManager.request_stop/3`).

Every trigger converges on one `WorkflowCompleted` (`status` `ok | error | cancelled`, `error`,
`result`). The controller writes the completion record, then tears down off the control-channel
reader, **settling both output debts first** (`ControlChannel.drain_owed_outputs/2`). They are
settled differently. The data stream is *flushed*: `Placement.flush_stream_channels/1` issues one
bounded call per stream channel of the runner, and a channel answers only after every frame
already in its mailbox has been routed to an Overwatch — so an opted-out or `run`-sugar class's
last direct output is run before its Overwatch is stopped, however the two sockets interleave on
arrival, and even when an output of that run was lost rather than delivered. The journal is
*waited for*: up to 60 s, re-driving `ProcJournal.flush_pending/1`, until
`ProcJournal.pending_entries/1` is empty. A deadline hit is logged (`... still owed`) and the
retained journal keeps the debt. `status = "error"` tears down identically — there is no
platform retry of a failed procedure. A `run`-sugar body may `recv()` input after it starts; a
frame addressed to a running instance is routed to its inbox, never dropped.

## Durable loop and suspension

Loop modes are declared with `loop_mode()` / `loopMode()`. Registration refuses a loop on a
`run`-sugar class (`LoopWithoutChain`) and a durable `Chained` loop whose terminal procedure
releases no inter-procedure channel (`LoopTerminalWithoutHandoff`). Two shapes fail that: a
terminal nothing consumes and that names no sink owns no output channel at all, and one whose only
wiring is a plugin sink gets no journal codec, because the runtime attaches one to inter-procedure
channels and nothing else. Either way a crash would re-seed empty, read as converged.

**The iteration boundary.** `IterCommit` rides the same commit worker as `ProcCommit` (below).
After the ack the worker runs the post-ack cleanup in order: a boundary snapshot
(`CompactEntity` per Entity, in a task, best-effort — a failed compaction means the rehydrate
replays the WAL tail), the outbox flush in journal order, the inbox release
(`ProcInbox.truncate/2`) and the journal cut (`ProcJournal.cut/1`). The two cuts are the
writers' own casts — the worker never waits on a file rewrite — and a prefix cut below
`:record_log_cut_after_bytes` (64 KiB) is deferred until that many released bytes accumulate.
The journal is cut only for a class whose `compensable_procs` is `[]` (a saga or a rewind
replans from the committed history); a cuttable loop can only be rewound to its current
iteration.

**The durable inbox.** `Ingest` holds the one append-before-forward seam: post-start
input goes to `<state_root>/<ns>/<inst>/<inst>/inbox.wal` before it is forwarded, stamped
`flz.inbox_offset` (WAL seq + 1; offset 0 means nothing consumed); a failed append is not
forwarded. The record header is the first 16 bytes of `sha256(dedup_id)` — `flz.msg_id` when the
sender stamps one, else a hash of the payload — and the writer's `dedup: true` index refuses a
duplicate inside the window, the file above its last release. Delivery is filtered by the
journal's **released** offset (the maximum consumed offset over the raw journal, which no rewind
lowers), so a re-sent consumed message is absorbed rather than re-delivered.

**Suspension.** A `PerInput` loop whose inbox stays empty past `FLEZHA_WORKFLOW_PARK_IDLE_MS`
(default 5 000; read in the shared core, so Rust and Java agree, and by the Elixir SDK's own driver) at a committed boundary tears
its run down without a completion and ships `IterPark`. The controller journals
`parked(iter[, wake_deadline_ms])` **before** `Migration.suspend/3` evicts the instance
(`RemOperative`, runner tag `nil`); a park whose append fails leaves the instance bound and
logged. The Overwatch and the registry entry stay, and the Overwatch **hibernates**
(`:hibernate` on both suspension entries, releasing its bind-time heap).

| Surface | While suspended |
| --- | --- |
| `flz operative info` | phase `suspended` |
| convergence loop | skipped (`suspended?`), plugin diff included — its plugins left with the runner and the rehydrating bind replays them |
| namespace hub | outlives its runner: `HubManager.stop_hub_if_vacant/1` stops a hub only with no operative registered, so a parked fleet survives its sole runner dying |
| `flz state get` | refused — no live Entity |
| removing its factory from the runner | the SDK's live-instance check cannot see a suspended instance, so the removal succeeds and the later rehydrate fails placement until a runner re-defines the factory; `FleetManager` warns `dropped factory ... that still has controller-known operatives` on the `UpdateFactories` frame. A blocking policy is deferred |

**Rehydrate = bind.** A new inbox append, the wake deadline, a crash and a migration all go
through `Lifecycle.complete_init/4`: respawn on any live runner, `AddOperative.proc_journal`
with the folded journal, `resume_entity_count` Entity restores, then the undelivered tail
(`Replay.deliver_inbox_tail/1`). `AddOperative.inbox_tail_count` tells the driver how many items
it is owed; it waits up to 60 s for them before its park-idle clock starts, so a tail forwarded
under load never hits a driver that already parked. An instance with undelivered input is never
left suspended (`rehydrate_if_pending/1` runs at every park and at boot), and a park that lands
while an un-park is still binding wins (`parked_in_flight` discards the task's stale bound state;
the pending tail wakes it again).

**The wake timer** (`wake_after()` / `wakeAfter()`) is journaled with the park record and
re-armed with `Process.send_after` from the journal at boot, so it survives a controller
redeploy. It fires by appending a synthetic empty inbox item (`flz.wake = timer`), so a racing
message and timer collapse into one rehydration. At boot an instance whose journal ends in
`parked` with nothing undelivered (`Migration.suspended_on_disk?/1`) enters suspended without
binding; undelivered input outranks the park. Inspect with `flz state journal <operative/instance>`
(entries tagged with their iteration, `iter_committed` with its consumed offset, the final
`parked`) and `flz state inbox <operative/instance>` (offsets, and which are still undelivered).

## Entity retry: the rung before the abort

A failing entity inside a **structured** procedure is retried in place while its finished
siblings stay buffered at the barrier; a `run`-sugar body has no entity to scope a retry to and
is terminal on its first error.

| Knob | Rust / Java | Default |
| --- | --- | --- |
| total attempts, first run included | `retry_attempts()` / `retryAttempts()` | 3; `0` or `1` is fail-fast |
| backoff base | `retry_backoff()` / `retryBackoff()` | 500 ms; attempt `n` waits `base × 2^(n-1)` ±50 % jitter, capped at 5 s (`RETRY_BACKOFF_MAX_MS`) |

- More than one attempt with a zero backoff base is refused at registration (`ZeroRetryBackoff`).
  `ProcedureError::non_retryable(..)` skips the ladder; every other error — a panic included,
  which the author never got to classify — is retryable.
- A retried entity re-runs its whole batch over the state its failed attempt already applied —
  the at-least-once contract crash-resume imposes. Each attempt collects into a fresh sink, so a
  failed attempt's staged output is dropped, never double-emitted. The ladder is strictly
  pre-commit: a journaled procedure is never retried, and a crash mid-ladder resumes as an
  ordinary uncommitted-procedure re-run with the counter reset.
- The backoff holds no worker: an activation runs one attempt and releases its thread; the
  ladder (attempt, due time, last error) lives on the driver, which re-submits at the barrier it
  is blocked on and re-checks cancellation every 200 ms, so a stop or `max_duration` cuts the
  wait short. A stage error while the cancel token is set reports `cancelled`. Each retry is
  logged (`instance / procedure / entity / attempt / delay`) and counted in the retry counter the
  stream ladder feeds, shipped in `data_heartbeat` metadata.

An exhausted ladder fails the procedure: no commit, nothing leaked, compensation, status
`error`. Nothing workflow is ever written to the DLQ store — abort-and-compensate is the escape
where a stream operative would [park the payload](data_durability.md#dead-letter-queue-poison-payloads-park-never-drop).

## Rewind: taking a loop back to a committed iteration

```text
flz operative rewind <instance> --to-iteration N
```

`Migration.rewind_to/2` runs, in order:

1. **The guard**, before anything durable changes. `N` must be a committed iteration (the refusal
   names the committed range), and every procedure above `N` that staged plugin output must be in
   `compensable_procs` (`ProcJournal.staged_channels_above/2`; the refusal names the channels) —
   a workflow emit carries no `flz.key`, so the hook is the only undo the engine can guarantee.
   A class that declared nothing (`nil`) is not a chain with nothing compensable (`[]`). Neither
   refusal touches the journal.
2. **The marker.** `ProcJournal.append_rewound/2` appends `rewound(N)` at `current_epoch + 1` with
   a synchronous fsync. The fold purges accumulated entries with `iter > N` and keeps everything
   appended after the marker, so a rewound iteration's unflushed outbox ceases to be owed.
3. **Evict and rehydrate unconditionally** — a rebind, not a park: no `parked` entry, no wake
   timer, and the loop comes back with no input waiting because it must run the undo.

The undo list rides `ProcJournalState.rewind_undo`: the procedures committed before the marker at
an iteration above `N`, minus those with a `compensated` entry appended after their own commit.
The driver runs them through the ordinary planner under a **forced** `AllOrNothing` (reverse
iteration, reverse position) and journals one `compensated` entry per procedure — an empty one
for a hookless procedure, which is what drains the list. It does not consult the per-entity done
markers: an entity that ran in two rewound iterations is undone once per iteration. A crash
mid-rewind resumes with exactly the outstanding work; there is no done marker.

A rewind **does not replay inputs**: the inbox is released at every iteration commit, so a
committed turn's message is gone from disk. Rewind is undo-and-continue — the loop resumes
waiting and the next message becomes iteration `N + 1`. Entity state is unwound by the hooks,
not by truncating the state WAL; a hookless procedure leaves its state change in place.
Telemetry `[:flezha, :workflow, :rewind]` (measurement `epoch`); `flz state journal` renders `rewound`.

## Saga: compensation on abnormal termination

A procedure declares `compensate(ctx, completed_output, state)`; `rollback_mode()` /
`rollbackMode()` selects `AllOrNothing` (default) or `KeepProgress`. Both are journal-backed: the
planner's `committed` input is the journaled procedures grouped by iteration, so `KeepProgress`
keeps every committed iteration and `AllOrNothing` unwinds them after the in-flight one. For a
single-pass chain the two coincide.

- The plan is the Kahn order reversed — a join before any of its producers, a shared root last,
  independent subtrees in unspecified relative order. Hooks run once per entity that had an
  effect, inside the single teardown. A hookless procedure is skipped; a failing hook is logged
  and the rest of the plan still runs. A clean `ok` never compensates.
- A failing node poisons its level: sibling nodes that completed are recorded as having run (so
  they are compensated) but not committed, since a commit would flush an outbox to be undone. A
  cooperative stop after a completed stage takes the same path; the undo record's commit wait
  ignores the cancel token, because rollback is the response to that cancel.

**Crash-safe compensation** uses per-Entity markers on the state substrate: `compensation_intent`
(`<entity>.comp_intent`) when a compensable procedure completes forward, `compensation_done`
(`<entity>.comp_done`) when its undo finishes. They ride `RestoreInitState.has_compensation_intent`
/ `has_compensation_done` on the rebind, and the driver skips every entity whose done marker
landed. A shipped **done** marker proves an undo pass was in flight and wins over journal-gated
forward resume; intent markers alone do not, because they also accumulate during forward
progress. Procedures the journal skipped still roll back on a later abnormal exit, folded into
the plan from their intent markers. The undo record (`ProcCommit` with `compensating = true`,
journaled as `compensated`) is written **before** the done marker, so a crash between the two
re-runs the idempotent hook rather than losing its undo output. Author contract: a
journal-rebuilt procedure's `completed_output` is empty on resume (its inter-procedure output
was never durable), so a durable compensable procedure keeps its undo handle in Entity state,
and hooks are idempotent.

## Durability: staged commit, proc.journal, outbox, crash-resume

A class is durable by default and opts out with `PERSISTENCE_ENABLED = false` /
`persistenceEnabled()`; the controller resolves it in `PersistenceResolver.persistent_state?/3`
(operator override available). Opted out, the per-Entity flush drains and discards — no WAL,
snapshot, journal or handshake — and a crash re-runs from the top; so does `run`-sugar.

**The commit path.** At every barrier the driver ships `ProcCommit` (`proc_idx`, `iter`,
`entities_ran`, the handoff through the channel's codec, the staged `outbox`) and blocks.

| Step | Where | Detail |
| --- | --- | --- |
| decode, hand over | control-channel reader | casts to `ControlChannel.Commits` and returns — no disk I/O on the reader, so a commit storm never delays a ping, stop, park or compaction |
| append | `Commits` (one per runner) | `ProcJournal.append_async/4`, `sync: :batched`: the reply is deferred until the `FsyncBatcher` tick, a prefix cut or a clean close syncs the batch; the worker's request collection is the set of acks owed and it never waits on a writer |
| identity | journal writer | the 16-byte header `[epoch u32][type u8][proc_idx u16][iter u32]` is the dedup key: a re-sent barrier is answered from the existing entry, never journaled twice |
| ack | written by the channel | `ProcCommitResponse` under the request's `reference_id`; a non-empty `error` (write failure, `{:oversized_handoff, size, max}`) terminates the run |
| sync remote | a task, before the ack | `SYNC_DURABILITY = true` / `syncDurability()` (`durability: sync_remote` in the declaration) has `DurabilityTier.commit_sync/1` ship the journal suffix to the [remote tier](../operations/remote_durability_tier.md) first — RPO 0, workflow classes only |
| flush | after the ack, on the worker | `ProcJournal.flush_pending/1` delivers in journal order and writes `outbox_flushed`; an unbound operative retries every 1 s |

The driver's wait is `FLEZHA_WORKFLOW_ACK_TIMEOUT_MS` (default 30 000). An expired wait re-sends
the same frame under the same `reference_id`, after the class's retry backoff, up to
`barrier_retry_attempts()` / `barrierRetryAttempts()` sends (default 3); the waiter stays
registered across the ladder, so a late ack continues the chain, and only the exhausted ladder
is a terminal error. No channel declares how its handoff is encoded — the runtime derives the
journal codec, in Rust from the producing procedure's declared `Output` type — and a journal entry
is bounded by `:wal_high_watermark_bytes` (16 MiB): keep bulk data in Entity state and hand off
references. Every flushed item is stamped
`flz.entity`, `flz.seq = <proc_idx>.<seq_in_proc>` (an undo's seq carries a trailing `.1`,
numeric so the Postgres sink's guard still orders it), `flz.epoch` and `channel`; there is no
`flz.key`. Flush contract: [`data_durability.md`](data_durability.md#workflow-operatives-staged-commit-and-the-transactional-outbox).

**Crash-resume.** Journal state rides `AddOperative.proc_journal` on the bind — not a per-Entity
restore, because a class whose procedures hold no per-Entity state receives no
`RestoreInitState` at all. Committed procedures are skipped, the first uncommitted one is seeded
from the journaled handoff, and only the procedure mid-flight at the crash re-runs. A
restore-before-run gate (`AddOperative.resume_entity_count`) holds the first procedure until
that many `RestoreInitState` frames land, with a 30 s safety cap that degrades to a best-effort
re-run. A restored Entity re-arms its change seq from `RestoreInitState.terminal_seq`, so
post-resume appends are contiguous and the stream-only seq-gap resync never applies. Execution
is at-least-once: steps and hooks are idempotent by author contract. A crashed instance reaches
a new runner when a replacement presenting the same stable identity (`FLEZHA_RUNNER_ID_PATH`)
has the dead session's bindings swapped onto it — promptly on UDS (socket EOF), after the 45 s
control idle timeout on QUIC; the Overwatch re-binds with `Lifecycle.complete_init/4`, which
stamps `resume_entity_count` from the on-disk WAL and ships the persisted Entities with their
compensation markers (in production the convergence loop drives this), and the driver resumes
past the journaled procedures — or resumes the compensation — to a terminal `WorkflowCompleted`.

Telemetry: `[:flezha, :state, :proc_commit | :iter_commit | :proc_outbox_flush]`,
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
| `persistent_state?` | per-Entity WAL + restore, state introspection | always | durable by default, per-class opt-out (`PersistenceResolver.persistent_state?/3`) |
| `time_travel?` | point-in-time queries, retention beyond the latest snapshot (`flz state at`) | yes | no |
| `dynamic_entities?` | lazy per-key Entities, destructive idle eviction | yes | no |
| `output_stall_watch?` | per-Entity output-staleness watchdog → forced rebind | yes | no |

## How many instances a runner holds

Every actively-running instance owns one OS driver thread (`flezha-workflow-<id>`) that holds
the chain's position as a call stack and never runs user code. Measured on 8 cores / 32 GiB,
release build, with the standing probe:

```text
FLEZHA_SCALE_SHAPE=step FLEZHA_SCALE_N=10000 \
  cargo test -p flezha_runner --features bench-internals --release \
    --test workflow_scaling -- --ignored --nocapture
```

| | measured |
| --- | --- |
| resident per blocked instance | ~73 KiB, flat from 1 000 to 10 000 instances |
| …of which the driver thread | ~1.4 KiB — the rest is per-instance heap |
| 10 000 blocked instances | ~734–884 MiB resident, start 0.65 s, teardown 0.44 s |
| CPU, 10 000 blocked mid-step | 0.5 % of one core |
| CPU, 10 000 parked in the inbox wait | ~1.6 of 8 cores |

**Size a runner by threads, not instances.** A turn blocked on a slow call holds two OS threads
in Rust (driver + pool worker) and one in Java (the worker is a virtual thread); a class with
`max_duration()` pays a third for its watcher. At ~10 000 threads an unrelated instance's
start-to-completion roughly doubles at the median; at ~20 000 throughput drops to a quarter.
**A driver blocked at a barrier is free; a driver polling is not.** The inbox wait, the
commit-ack wait and the `max_duration` watcher re-check every 200 ms, so a population in any of
them costs CPU. A durable `PerInput` loop parks after `FLEZHA_WORKFLOW_PARK_IDLE_MS`; an
opted-out one has no park to take and polls for as long as it lives.
`FLEZHA_WORKFLOW_DRIVER_STACK_KIB` sets the driver stack (unset = platform default) — an
address-space lever only, since a blocked driver's stack is not resident; an edge transform
(`map`/`filter`/`flat_map`/merge mapping) runs on this thread and a stack overflow there aborts.

## Operator notes

- **Crash-resume is at-least-once.** A non-idempotent fire-and-forget job can double-act on
  resume: write it idempotently, or opt out of durability so a crash re-runs from a clean slate.
- **Stop runners gracefully** ([`../operations/graceful_shutdown.md`](../operations/graceful_shutdown.md)):
  a graceful stop lets a running instance migrate; a SIGKILL forces crash-resume.
- **Retention grows disk for many small jobs.** Opt out for fire-and-forget, or lower
  `FLEZHA_WORKFLOW_RETENTION_HOURS`. Workflow is not the high-throughput path.
- **Proof:** `test/system/workflow_crash_resume_e2e_test.exs` — SIGKILL mid-run and
  mid-compensation, the fan-in diamond, the outbox re-drive, entity retry, park and rehydrate,
  rewind; Rust and Java, UDS and QUIC.

## See also

- [`../architecture/workflow_profile.md`](../architecture/workflow_profile.md) — the concepts and the wire table
- [`data_durability.md`](data_durability.md) — the scenario matrix, the outbox and inbox contracts
- [`../internals/state_persistence.md`](../internals/state_persistence.md) — the WAL substrate, `FsyncBatcher`, retention
- [`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) — field-level `ProcCommit` / `IterCommit` / `IterPark`
- [`../operations/remote_durability_tier.md`](../operations/remote_durability_tier.md) — `sync_remote`
