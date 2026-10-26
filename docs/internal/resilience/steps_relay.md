# Steps Relay

What must stay true in a relay whose class sets `run: steps`, where each rule is enforced, and
what a crash in each window leaves behind. Written for contributors to `crates/flz_core/src/steps/`
and to the controller's steps path. The concepts (the graph, the journal, the stage commit, the
outbox, retry, undo, the loop, sleeping, rewind, how an instance ends) are described once in
[`../../public/architecture/steps_profile.md`](../../public/architecture/steps_profile.md) and
not repeated here. The user's view is
[`../../public/topics/core/workflows.md`](../../public/topics/core/workflows.md); how the
controller binds, sleeps and wakes an instance is
[`../architecture/relay_lifecycle.md`](../architecture/relay_lifecycle.md).

**Contents**

1. [Where the code is](#where-the-code-is)
   1. [The shared runner core](#the-shared-runner-core)
   2. [The controller](#the-controller)
   3. [Extension points](#extension-points)
2. [Invariants of the runner core](#invariants-of-the-runner-core)
   1. [Decide under the lock, act after it](#decide-under-the-lock-act-after-it)
   2. [The order inside `act`](#the-order-inside-act)
   3. [Kahn order is the wire order](#kahn-order-is-the-wire-order)
   4. [A barrier keeps one identity](#a-barrier-keeps-one-identity)
   5. [Record, then Mark](#record-then-mark)
   6. [Nothing runs before the restore gate opens](#nothing-runs-before-the-restore-gate-opens)
3. [Invariants of the controller](#invariants-of-the-controller)
   1. [Records, then journal, then ack](#records-then-journal-then-ack)
   2. [The floor only rises](#the-floor-only-rises)
   3. [A cut never shortens a history someone replans from](#a-cut-never-shortens-a-history-someone-replans-from)
   4. [Every resume is a bind](#every-resume-is-a-bind)
   5. [Owed outputs settle before the teardown](#owed-outputs-settle-before-the-teardown)
   6. [No behaviour follows from the run alone](#no-behaviour-follows-from-the-run-alone)
4. [Crash and failure windows](#crash-and-failure-windows)
5. [How many instances a runner holds](#how-many-instances-a-runner-holds)
6. [Related pages](#related-pages)

## Where the code is

### The shared runner core

The runner side of a steps relay is `flz_core::steps`, whichever SDK hosts it. The instance
is a `RelayInstance` that also carries a `StepsRun`; the module's files split by concern:

| File | What it holds |
| --- | --- |
| `crates/flz_core/src/steps/mod.rs` | the module's doc, its re-exports (`StepsRun`, `Resume`, the event functions) and `StepsError` |
| `crates/flz_core/src/steps/spec.rs` | `StepsSpec::of` and `TaskDag::of`: the frozen DAG in Kahn order, one `Chain` per entry channel, the class's `each` and `on_failure` |
| `crates/flz_core/src/steps/instance.rs` | the pass sheet (`PassSheet`, `Phase`, `Pass`), `BarrierFor` with its `impl`, `Barrier`, `Undo` and `UndoStep`, and `JournalResume`, the journal state a bind carries |
| `crates/flz_core/src/steps/events.rs` | the entry points each thread calls with what it brings: `open`, `restore_landed`, `message`, `stop`, `item_settled`, `complete`, `barrier_acked`, `deadline_due` and the timer hooks |
| `crates/flz_core/src/steps/run.rs` | `Run` and `Run::decide`, addressing values to Cells off the lock, `begin_pass`, and the decisions on stops, deadlines, barriers and their re-sends |
| `crates/flz_core/src/steps/pass.rs` | the pass decisions under the lock: messages and passes, settled items, stage commits, `settle_pass`, a failed turn's undo, the iteration ack |
| `crates/flz_core/src/steps/resume.rs` | what the journal says about where a run resumes (`seed_from_journal`, `resumed_seed`), the restore gate (`landed`, `open_gate`) and `resume` |
| `crates/flz_core/src/steps/compensation.rs` | `plan_compensation`, the saga, rewind and failed-turn undo plans, and the undo steps a run takes one event at a time |
| `crates/flz_core/src/steps/effects.rs` | `Effects` and `act`: frames sent, deadlines armed, items admitted and author code run, after the lock |
| `crates/flz_core/src/steps/commit.rs` | the barrier frames (`task_frame`, `iteration_frame`, `undo_frame`) and `ack_timeout` |
| `crates/flz_core/src/steps/run_tests.rs` | deadline and restore-gate tests |
| `crates/flz_core/src/steps/driver_tests.rs`, `crates/flz_core/src/steps/driver_tests/` | the host path as a C-ABI host drives it, on the fake controller in `crates/flz_core/src/test_support/steps.rs`; `driver_tests/undo.rs` holds the unacknowledged-compensation tests |
| `crates/flz_core/src/steps/spec_tests.rs` | the DAG order a class registers with |

The loop shape and failure mode live beside the other settings, in
`crates/flz_core/src/relay/settings.rs`, and an instance's deadlines (`Deadline`) in
`crates/flz_core/src/relay/mod.rs`. The Rust SDK's driver suite (`sdk/rust/src/steps/tests/`)
runs on the same fake controller.

### The controller

| Module | What it holds |
| --- | --- |
| `Flezha.Runner.ControlChannel.Commits` | one process per runner that journals each `TaskCommit` and `IterCommit` and acks it |
| `Flezha.Runner.ControlChannel.Requests` | `RelayCompleted` (the completion record and the teardown), `IterPark` |
| `Flezha.State.TaskJournal` | the per-instance journal: append, fold, cut, the rewind marker, what is committed and owed |
| `Flezha.State.TaskJournal.Codec` | the entry types and their bytes, laid out in [`../state_persistence.md`](../state_persistence.md) |
| `Flezha.State.TaskJournal.Records` | the effect records a stage entry names: the wait for them, their release, what the writers still owe |
| `Flezha.State.TaskJournal.Turns` | a failed turn's dead letter: the park, its announcement, an unrecorded park dropped at bind |
| `Flezha.State.InputLog` | the durable input a steps relay shares with the effectively-once tier |
| `Flezha.State.WalWriter.Settlement` | a Cell writer's held records: released by a stage marker, dropped when no marker will name them |
| `Flezha.Relay.Lifecycle`, `Flezha.Relay.Replay` | the bind: journal state, Cell restores, the floor and the re-forwarded tail |
| `Flezha.Relay.Suspend` | the park record, the wake timer, rewind |
| `Flezha.Relay.ClassRegistry` | what a class declared: `each`, its compensable tasklets, `single_input?/1` |
| `Flezha.State.Completion`, `Flezha.State.Retention` | the finished-job record and its reclamation |
| `Flezha.Service.Steps` | the service API behind `flz state journal`, `flz state inbox`, rewind and rescale |

### Extension points

A new rule or kind goes into the type that owns it; no trait sits in front of these:

- **A loop-shape or failure-mode rule** is one inherent method on `Each` or `OnFailure` in
  `crates/flz_core/src/relay/settings.rs`, one `match` each. Today `Each` answers `as_str`,
  `message_fed(iter)`, `ends_after_first`, `carries_leaf`, `calls_allowed` and `partitionable`,
  and `OnFailure` answers `as_str`, `keeps_committed` and `fails_turn(each)`. A site outside that file calls a method rather than matching a variant.
- **A barrier step** is one method on `impl BarrierFor` in `crates/flz_core/src/steps/instance.rs`,
  beside the enum (`Stage(node)`, `Iteration`, `DeadLetter`, `Undo`): today `on_ack`,
  `on_refusal`, `on_timeout`, `failed`, `label` and `commits_iteration`. Outside that
  file a `BarrierFor` is only constructed, never matched.
- **The graph order** is `TaskDag::of` in `crates/flz_core/src/steps/spec.rs`, over `graph_order` in
  `crates/flz_core/src/relay/topology.rs`. A change to the walk changes every journal's `task_idx`, so
  `spec_tests.rs` pins it.
- **The controller's loop-shape question** is `ClassRegistry.single_input?/1`, over the `each`
  the class registered with; partitioning and the one-input rule call it.
- **A journal entry type** is one encode and one decode clause in
  `Flezha.State.TaskJournal.Codec`, the other end of the format the core writes.

## Invariants of the runner core

### Decide under the lock, act after it

An instance holds no thread. Every event (a settled item on the worker that ran it, an ack or a
stop on the control reader, a message on the data reader, a deadline on the poller) enters
through `Run::decide`: the closure decides under the sheet's one `Mutex` and only records what
to do in an `Effects`; `act` carries it out after the lock is released. No frame is sent, no
item admitted and no author code run while the lock is held, and nothing waits on it. Edge
transforms and `cells_for` run on the producing worker before an output is held. A retry's
backoff is an entry on the poll core's deferred-wake list, so a stop, `max_duration` or a
sibling's failure disarms every backoff of the pass (`Effects::cancel_retries`) and what the
pass's Cells still have queued is consumed without running.

### The order inside `act`

`act` runs one fixed order, and two of its steps depend on it:

1. **Sent to the controller** (`send`): compensation markers, then barrier frames, then frames
   sent as they stand (a re-send), then timer removals, then a park, then dropped acks. An intent
   marker precedes its commit, so a crash between them resolves as "not committed, run again"
   (window C9).
2. **Handed to the poller** (`schedule`): deadlines disarmed, then armed, then held timer arms,
   then items admitted, then Cells made ready, then retries cancelled.
3. A message parked to the dead-letter queue, then the completion, then an undo hook's run, then
   the start of a new pass.

A barrier's command is stored on the sheet before it is sent (`send_barrier`), because a refusal
can answer before the sending thread takes the lock again, and its re-send needs the command.

### Kahn order is the wire order

`TaskDag::of` numbers the tasklets in Kahn order from the entry channels; a node's index is its
stage index and its wire `task_idx`. A producer's index is always below its consumers', so
commits arrive in topological order, `plan_compensation` reverses them without a second walk,
and a join resumes with each committed producer's hand-off banked rather than run again. The
relay is `ok` when every leaf tasklet has committed; the SDK declares the compensable set
(`compensable_tasks`) in the `AddRelay` response metadata, so a rewind is checked against it.

### A barrier keeps one identity

A barrier (`Barrier`) keeps its reference id for its whole life. An ack not received within
`FLEZHA_STEPS_ACK_TIMEOUT_MS` (30 s) re-sends the same command under the same id, up to
`BARRIER_SENDS` (three) sends in all, a constant of the controller connection rather than a class
setting; the ack stays registered across the re-sends, so a late one continues the chain. The
controller journals a barrier once by the entry's identity, so a re-sent barrier is answered from
the existing entry. Every re-send goes out as a new frame (`Decide::resend`), whose send registers
the ack's waiter again, in place of the one a timeout left or a refusal consumed, and re-arms the
ack wait in place, so a refusal answered in the send's own millisecond cannot drop the new wait.
An `Undo` barrier is re-sent on any refusal at once. Any barrier refused as
`commit.record_not_durable` is re-sent after a back-off: the controller journals nothing when a
named record's wait times out, its Cell's writer cannot be reached, or the writer refuses the
wait, and the reason on the wire does not tell these apart, so all three are retried
(`resend_after`: `REFUSAL_RETRY`, the shared ladder from a 1 s base, counted and logged by
`retry::note_failed_attempt`), the back-off waited out on its `Ack` deadline. Both count
against the same `BARRIER_SENDS`; every other refusal, and one with no send left, fails the
barrier at once.

### Record, then Mark

An undo of a saga Cell is three steps on `Undo.steps`: the hook runs (`UndoStep::Cell`), the
record it wrote is journaled as `compensated` (`UndoStep::Record`), then the Cell's
compensation-done marker is shipped (`UndoStep::Mark`). The markers live beside the Cell's
state (`<cell>.comp_intent` when a compensable tasklet completes forward, `<cell>.comp_done`
when its undo finishes) and ride the rebind as `RestoreInitState.has_compensation_intent` and
`has_compensation_done`. The done marker stops the hook running again
on resume, so a crash between the two must fall on the side that runs the hook again, and the
marker follows only an ack (`undo_journaled`). When the record is never acknowledged the Cell's
marker step is dropped with it, the rest of the plan still runs, and the run ends `error` naming
the tasklet (window C7). A rewind's or a failed turn's undo (`unwind_steps`) earns no marker: it
journals one `compensated` entry per tasklet, even an empty one, so the journal alone says what
is outstanding.

### Nothing runs before the restore gate opens

A bind enters `Phase::Gated` and expects `AddRelay.resume_cell_count` restores. The gate counts
frames, not Cells, so a frame the `max_cells` cap refuses, or one naming a tasklet the class no
longer declares, still counts; the last frame opens it (`open_gate`) on the reader that brings
it. While `Gated`, no tasklet and no undo hook runs: `Deadline::RestoreGate` (30 s) ends the run
`error`, naming how many restores landed, and a stop or `max_duration` ends it `cancelled`
(window C11). With a journal, intent markers alone never start a compensation at the gate; only
a shipped done marker proves an undo was in flight, because intent markers also accumulate
during forward progress.

## Invariants of the controller

### Records, then journal, then ack

The control-channel reader decodes a barrier and casts it to `Commits`, so a commit storm never
queues a ping, stop, park or compaction behind disk I/O. Each step of the path:

| Step | Where | Rule |
| --- | --- | --- |
| records | `Commits` | before a `task_committed` or `compensated` entry, `WalWriter.await_durable_async/4` for every named `{cell, seq}` on its Cell's writer; a refusal, an unreachable writer or the ack timeout fails the barrier with `record_not_durable: <cell> seq=<n>`, and a re-sent barrier whose first copy still waits joins that wait |
| append | `Commits` | once every named record is durable, `TaskJournal.append_async/4` with `sync: :batched`, answered when the `FsyncBatcher` tick, a prefix cut or a clean close syncs the batch; no wait blocks the process |
| identity | journal writer | the 16-byte header `[epoch u32][type u8][task_idx u16][iter u32]` is the dedup key |
| remote | before the ack | under `durable: remote`, `DurabilityTier.commit_sync/1` ships the journal suffix to the remote tier first |
| ack | the channel | `TaskCommitResponse` or `IterCommitResponse` under the request's `reference_id`; a non-empty `error` (a write failure, `{:oversized_handoff, size, max}`) ends the run; `record_not_durable` is re-sent after a back-off while sends remain |
| release | after the ack | `WalWriter.commit_records/3` per named range releases those records' outputs and drops a held entry of that Cell below the range, an attempt that never committed |

A dead-lettered iteration's record is its turn's message: it is parked in the dead-letter queue,
read from the input log, before the `dead_lettered` entry naming it is appended. An iteration's
ack gates the input-log release (`TaskJournal.release_input/2`) and the journal cut. A journal
entry is bounded by `:wal_high_watermark_bytes` (16 MiB).

### The floor only rises

The instance's floor in its input log is the log's watermark under the journal's identity. It is
seeded at every bind from the journal's released offset (`Replay.seed_floor/1`: the highest
offset an iteration commit or an input released unread consumed, which no rewind lowers) and
from the park record while the instance sleeps, and raised by every iteration commit and park. A consumed
message is therefore never delivered again, and a rewind does not replay inputs.

### A cut never shortens a history someone replans from

`TaskJournal.cut/2` cuts the journal only for a class whose `compensable_tasks` is `[]`: a saga
and a rewind both replan from the committed history, and a class that declared nothing (`nil`)
keeps its history too. A cut below `:record_log_cut_after_bytes` (64 KiB) waits until that many
released bytes accumulate. A partition rescale seals each surviving member at its latest
committed iteration (`cut(id, seal: true)`) whatever the class declares, because a moved Cell's
earlier passes belong to a member that may no longer own it.

### Every resume is a bind

A crash, a migration, a wake and a rewind all go through `Lifecycle.complete_init/5`: the folded
journal in `AddRelay.task_journal`, `resume_cell_count` Cell restores carrying their compensation
markers, and the one re-forward (`Replay.reforward_undelivered_tail/1`) of the input log's
records above the floor, after it removes a dead-letter entry whose `dead_lettered` record never
landed (`TaskJournal.Turns.drop_unrecorded_turns/1`). At a writer's boot, `Settlement.seed_commits/3`
re-derives from the journal's live entries which held records are committed. A live instance
and a resumed one are the same data: the pass the journal describes is the pass the instance
runs. A reconnect of the same runner process (the same `incarnation`) only moves the binding.

### Owed outputs settle before the teardown

Every exit converges on one `RelayCompleted`. The controller writes the completion record, then
tears down under `Flezha.Process.DurableTasks`, off the control-channel reader, after settling
two debts. The data stream is flushed (`StreamPool.flush_channels/2`, one bounded call per stream
channel), so a class kept in memory has its last direct output routed before its Overwatch stops.
The Cells' outboxes are waited for, up to `drain_owed_ms` (60 s), polling every 50 ms, until
`TaskJournal.Records.owed_outputs/1` is zero; a deadline hit is logged (`... still owed`) and the
retained WAL keeps the debt (window C10). An `error` ends the same way: there is no automatic
re-run of a failed tasklet.

### No behaviour follows from the run alone

Each behaviour follows from what the class declares or from the instance's own state:

| Behaviour | Decided by |
| --- | --- |
| per-Cell WAL, bind-time restore, live Cell reads (`flz state get`) | the class's `durable` setting |
| sleeping when idle, and the durable input a sleeping flow relay keeps | the class's sleep setting |
| point-in-time queries and history retention (`flz state at`, `time_travel_retention_hours`) | nothing: a steps relay Cell keeps history like a flow relay Cell, then finished-job retention reclaims the directory |
| dynamic Cells and their idle eviction | nothing: a steps relay evicts only between passes and never within a rollback's reach |
| output-stall watch and the forced rebind it drives | nothing: a wedged steps relay is rebound and resumes at its stage |
| input-silence sweep | the instance's state: never a sleeping instance, never a checkpoint-ack class between checkpoints |

## Crash and failure windows

Each window, what the next bind does, the code that makes it so, and the test that pins it.
Paths in the Test column are under `test/system/` unless they name a crate.

| # | Window | Outcome | Guard | Test |
| --- | --- | --- | --- | --- |
| C1 | a Cell's record shipped at settle, crash before its stage's `TaskCommit` | no entry names the record, so it is dropped and reaches no output | `Settlement.seed_commits/3` at the writer's boot | `steps_crash_resume_e2e_test.exs` "a failed stage delivers nothing, though its Cell wrote a record", "the record precedes the entry naming it under a long flush linger" |
| C2 | stage entry durable, crash before the release | the boot re-derives the release and the output goes out with identical stamps | `Settlement.seed_commits/3` | `steps_crash_resume_e2e_test.exs` "a crash before the flush re-drives the output with identical stamps" |
| C3 | `TaskCommit` durable, crash before the next stage | committed stages are skipped and their children seeded from the journaled hand-off | `open_gate`, `resumed_seed` | `steps_crash_resume_e2e_test.exs` "a fan-in diamond joins once, and a crash re-runs only the uncommitted producer"; `sdk/rust/src/steps/tests/driver/fan_in.rs` `branching_resume` |
| C4 | `IterCommit` durable, crash before the input release or the cut | the bind seeds the floor from the journal's released offset, so the message drives one iteration | `Replay.seed_floor/1` | `steps_crash_resume_e2e_test.exs` "the re-forwarded message drives one iteration after the resume, never two", "an Overwatch killed mid-bind leaves the message one iteration" |
| C5 | a completing iteration journaled, crash before `RelayCompleted` | the resumed run ends `ok` with the journaled result and runs nothing | `open_gate` (`JournalResume.completion`) | `sdk/rust/src/steps/tests/driver/durable_loop.rs` `loop_completion` |
| C6 | a timer firing's stage committed, the firing's removal unwritten | the restored timer is not fired again | `committed_firings` (the root's `TaskCommit` names the firing) | `steps_crash_resume_e2e_test.exs` "a crash after the firing's root committed does not fire it again"; `sdk/rust/src/steps/tests/driver/timers.rs` `a_crash_after_the_roots_ack_does_not_fire_it_again` |
| C7 | an undo's `compensated` record refused or never acknowledged | re-sent under its reference id within the three sends; the done marker follows only an ack; once the sends run out the Cell stays unmarked, the rest of the plan runs and the run ends `error` naming the tasklet | `BarrierFor::on_refusal`, `Decide::resend`, `undo_journaled` | `crates/flz_core/src/steps/driver_tests/undo.rs` `a_compensation_the_controller_always_refuses_leaves_its_cell_unmarked_and_ends_error`, `a_compensation_refused_once_is_sent_again_and_marked_after_its_ack`, `a_compensation_never_acked_ends_error_and_the_other_cell_is_still_marked` |
| C8 | a failed turn parked, crash before its `dead_lettered` entry | the bind removes the unrecorded entry and the turn runs again, so the message is parked at most once | `TaskJournal.Turns.drop_unrecorded_turns/1` | `steps_failed_turn_e2e_test.exs` "after the park and before the record re-runs the turn, and parks it once", "after the record and before the undo is journaled undoes the turn once, on the rebind" |
| C9 | intent markers sent, crash before the barrier frame they precede | with a journal, intents start no compensation without a done marker; the stage runs again | the order inside `act`; `open_gate` | `steps_crash_resume_e2e_test.exs` "durable saga steps resumes compensation after a SIGKILL mid-rollback" |
| C10 | `RelayCompleted` written, the drain of owed outboxes times out | the teardown goes on, the warning names what is still owed, the retained WAL keeps the debt; a finished relay is not started again | `drain_owed_ms`, `DurableTasks` | `test/flezha/runner/control_channel_steps_test.exs` "the teardown flushes the runner's stream channels before it deregisters" (the flush); none for the timeout |
| C11 | a bind's Cell restores do not all land | the run ends `error` after 30 s naming how many landed; a stop or `max_duration` meanwhile ends it `cancelled`; no tasklet or hook runs, and the journal and state stay | `Deadline::RestoreGate` and `stop` in `Phase::Gated` | `crates/flz_core/src/steps/run_tests.rs` `a_restore_gate_timeout_ends_the_run_error`, `a_stop_at_the_restore_gate_ends_the_run_cancelled`; the teardown that follows: `test/flezha/runner/control_channel_steps_test.exs` "RelayCompleted error → failure telemetry + identical teardown" |

Every window also runs end to end on each SDK runtime, over UDS and QUIC, in
`steps_crash_resume_e2e_test.exs`, `steps_control_flow_e2e_test.exs`,
`steps_failed_turn_e2e_test.exs` and `steps_timers_e2e_test.exs`.

## How many instances a runner holds

An instance costs its pass sheet (iteration, stages in flight and done, held outputs,
completion, phase) and nothing else; it holds no thread. This is the envelope the
`sdk-poll-core` specification holds the runner to, measured on 8 cores and 32 GiB, release
build, with the standing probe:

```text
FLEZHA_SCALE_SHAPE=step FLEZHA_SCALE_N=10000 \
  cargo bench -p flezha_runner --features bench-internals,testing \
    --bench steps_scaling
```

| 10 000 instances | blocked mid-step (`step`) | waiting for a message (`idle`) |
| --- | --- | --- |
| resident per instance | about 61 KiB | about 61 KiB |
| threads per instance | 0 (the workers running the steps are the pool's) | 0 |
| CPU while blocked | 0 % | 0 % |
| start / teardown | 0.35 s / 0.06 s | 0.35 s / 0.03 s |
| an unrelated instance's p99 beside them | 0.8x its unloaded latency | 0.9x |

A turn blocked on a slow call holds the SDK worker running it (a pool thread in Rust, a virtual
thread in Java) and one in-flight slot: `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` (1 024 by default)
bounds how many Cells are handed out at once, so a runner meant to hold more blocked turns than
that raises it with its worker pool.

## Related pages

- [`../../public/architecture/steps_profile.md`](../../public/architecture/steps_profile.md): the concepts, the journal layout and the wire table
- [`../../public/topics/core/durability.md`](../../public/topics/core/durability.md): what is lost when, the durable input log and the outbox
- [`../architecture/relay_lifecycle.md`](../architecture/relay_lifecycle.md): bind, sleep, wake and stop on the controller
- [`../state_persistence.md`](../state_persistence.md): the WAL substrate, `FsyncBatcher`, the journal entry layouts
- [`../sdk/protocol_spec.md`](../sdk/protocol_spec.md): field-level `TaskCommit`, `IterCommit` and `IterPark`
- [`../sdk/poll_core.md`](../sdk/poll_core.md): the poll core and the deferred-wake list
