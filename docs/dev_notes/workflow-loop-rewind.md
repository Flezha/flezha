# Workflow loop rewind (`workflow-loop-rewind`)

- **Marker, not truncation**: `rewound` = proc-journal entry type 6, target =
  the entry's own `iter` field (empty payload), appended with
  `current_epoch + 1` — the FIRST non-zero epoch anything writes. The journal
  stays append-only; `ProcJournal.live_entries/1` is the one fold that applies
  markers (**on `rewound(N)`, purge accumulated entries with `iter > N`; keep
  everything appended after it**). `restore_state/1`, `pending_entries/1`,
  `consumed_offset/1`, `committed_iterations/1` all run over it — so a rewound
  iteration's unflushed outbox **ceases to be owed**, not merely gets skipped.
- **Undo is derived, not marked**: `restore_state/1` also returns `rewind_undo`
  = proc_committed entries before the marker with `iter > N`, minus those with a
  `compensated` entry appended *after* their own commit (that "after its own
  commit" test is what keeps a re-run distinguishable from the original it
  replaced, and survives a second rewind). No done-marker, no completeness
  oracle → crash-mid-rewind resumes with exactly the outstanding work.
- **Wire**: ONE additive field, `ProcJournalState.rewind_undo = 8`
  (`repeated ProcCommittedProc`). No new control message — the controller writes the
  marker and rebinds; the folded journal is all the SDK sees.
- **SDK**: `drive_chain` gains one branch before the forward loop →
  `run_rewind_undo` reuses `plan_compensation(AllOrNothing, groups, &[])`
  verbatim (reverse iteration, reverse stage) — **forced** AllOrNothing whatever
  the class's `rollback_mode` says, since a rewind unwinds committed iterations
  by definition. `StageRecord` gained `iter` (the `compensated` entry's `iter`
  was hardcoded `0` before). Journals ONE `compensated` per proc — **even an empty
  one for a hookless proc**, which is what drains `rewind_undo`; and it does NOT
  consult the per-entity done markers (they are per *entity*, and an entity
  that ran in two rewound iterations must be undone once per iteration).
  The two undo passes deliberately stay separate — one is **marker**-backed
  (crash resume, per entity), the other **journal**-backed (rewind, per proc) —
  but share `plan_compensation` (the plan), `compensate_entity` (the effect) and
  `journal_undo` (the `compensated` record).
- **Resume = rebind, not park**: `Overwatch.handle_call({:rewind, N})` → guard →
  `append_rewound` → evict (`evict_runner/2`, extracted from `enter_suspended`)
  → `rehydrate/1`. No `parked` entry, no wake timer.
- **Coverage guard** (`rewind_guard/2`, controller-side, before anything durable
  changes): an unwound proc that staged plugin output MUST declare `compensate`.
  The keyed-channel half of the original spec is **unreachable for workflow** —
  `WorkflowEntitySink` stages `(channel, bytes)` and `outbox_items` drops the
  key, so a workflow emit carries no `flz.key`. Facts ride the EXISTING
  `AddOperativeResponse.metadata` map (`"compensable_procs" => "0,2"`, no proto
  change) → `ClassRegistry.put_compensable_ops/2` (same sidecar shape as the
  tier). The guard asks `ProcJournal.staged_channels_above/2` — the journal owns
  its fold, Overwatch owns the policy (`live_entries/1` is private).
- **`ClassRegistry.decode_declarations/1`** is the ONE place the `AddOperative`
  response is decoded → `%{effectively_once:, compensable_procs:}`. A new
  SDK-declared capability is added there and nowhere else. (`compensable_procs`
  `nil` = the class declared nothing — Java/stream/run-sugar — vs `[]` = a
  chain with nothing compensable; a rewind treats those differently.)
- **GOTCHA — a rewind does NOT replay inputs.** `ControlChannel` truncates the
  inbox to the consumed offset at every iteration commit
  (`control_channel.ex:629`), so a committed turn's input is GONE. Moving
  `consumed_offset` back cannot resurrect it. Rewind is **undo-and-continue**:
  the loop resumes waiting and the NEXT message becomes iteration N+1. (The e2e
  caught this — an earlier draft asserted re-delivery and failed.)
- **Entity State** is unwound by the `compensate` hooks (the saga contract), NOT
  by truncating the state WAL. A hookless proc leaves its State change in place.
- **CLI**: `flz operative rewind <instance> --to-iteration N`
  (`CliManager.operative_rewind/3`); `flz state journal` renders `rewound`.
  Telemetry `[:flezha, :workflow, :rewind]`.
- **Tests**: `proc_journal_test.exs` (epoch bump, fold purge, outbox no longer
  owed, `rewind_undo` drain, re-run + second rewind),
  `overwatch_suspension_test.exs` (rewind + both refusals),
  `cli_manager_rewind_test.exs`, `workflow_driver_scenarios` (reverse-order undo,
  hookless drain), e2e "loop rewind (rust/uds)" in
  `workflow_crash_resume_e2e_test.exs` (fixture `chat_rewind_workflow` —
  `ChatReplyUndoable` keeps its undo handle in State and retracts in reverse;
  asserts `flz.epoch` 0 before / 1 after).
- **Split**: stream late-input correction + `flz operative replay` were carved
  out into the `stream-late-input-correction` openspec — its design needs four
  primitives that do NOT exist (event-time→seq map, non-destructive WAL rebase,
  event-time-ordered replay splice, an actually-enforced input-log `hold_window`;
  `realign_epoch/4` wipes the WAL and every snapshot — it cannot be "scoped").

