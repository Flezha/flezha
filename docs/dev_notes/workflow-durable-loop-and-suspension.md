# Workflow durable loop + suspension (`workflow-durable-loop-and-suspension`)

- **Loop (SDK)**: `WorkflowOperative::loop_mode()` → `Once` (default, the
  old single pass, byte-identical) | `PerInput` (one iteration per inbox
  item — chat turns) | `Chained` (seeded by the previous iteration's
  terminal handoff). `drive_chain` is now an iteration loop around the
  SAME stage machinery (`run_iteration` = the old for-loop body).
  Registration rejects a loop on run-sugar (`LoopWithoutChain`) and a
  durable `Chained` whose terminal proc has no codec'd output channel
  (`LoopTerminalWithoutCodec` — else a crash re-seeds empty and reads as
  "converged").
- **Iteration commit**: `IterCommit` (proto tag 30) / `IterCommitResponse`
  (31) at every iteration boundary → ONE `iter_committed(iter,
  consumed_offset)` journal entry (type 3, ACTIVE) carrying the terminal
  handoff; ack; THEN boundary `CompactEntity` per entity + outbox flush
  + inbox truncate (post-ack cleanup — a failure there costs work, not
  correctness). Every `proc_committed` is stamped with its enclosing `iter`.
- **KeepProgress is real**: `plan_compensation`'s `committed` is rebuilt
  from journaled `committed_procs` grouped by iter (`journaled_iterations`).
  Planner UNCHANGED. A journal-rebuilt iteration's `EntityRun.outputs` is
  empty (the at-least-once undo-handle-in-State contract) — the tests key
  on that (`undo-loop:1` in-flight vs `undo-loop:0` journaled).
- **Durable inbox**: `lib/flezha/state/proc_inbox.ex` — per-instance
  `inbox.wal` beside `proc.journal` on the SAME WalWriter substrate,
  identity `Registry.inbox_id/1` (`{inst, inst, "_proc_inbox"}`).
  **Offset = WAL seq + 1** (the substrate REQUIRES seq 0 first — an
  empty WAL receiving any other seq sheds the frame and pulls a baseline
  — while offset 0 must mean "nothing consumed"/"unjournaled").
  `Registry.@reserved_files` keeps `inbox.wal` out of `scan_dir` (it
  would otherwise decode as an Entity named "inbox").
- **Append-before-forward**: `Ingest.journal_input/2` — the ONE durable
  ingest seam (eff-once InputLog clause + durable-workflow ProcInbox clause).
  Stamps `flz.inbox_offset`; a duplicate (`flz.msg_id`, else payload hash,
  inside the undelivered window) returns nil = absorbed; a failed append
  is NOT forwarded.
- **Suspension**: SDK ships `IterPark` (tag 32) and exits WITHOUT
  `WorkflowCompleted` (`RunOutcome::Parked`). Controller journals
  `parked(N[, deadline])` (type 4, ACTIVE) THEN
  `Lifecycle.suspend/3` → `RemOperative` + `operative_set_runner(nil)`;
  Overwatch + registry entry STAY (zero *runner* resources).
  `Convergence.Diff`/`Loop` skip `suspended?` (else they'd rebind it and
  undo the eviction). `flz operative info` → phase `suspended`.
- **Rehydrate = bind**: `complete_init` always delivers the undelivered
  inbox tail (`deliver_inbox_tail`), so crash-resume / migrate / wake all
  use ONE path; `rehydrate/1` reuses the existing `{:unpark, ...}` migrate
  task. The wake deadline is an Overwatch `Process.send_after` re-armed
  from the journal at boot (NOT a Watchdog sweep — fires exactly, no
  scheduler); it wakes by appending a synthetic empty inbox item
  (`flz.wake=timer`), so racing wake sources collapse to one rehydration
  by construction. Boot: `suspended_on_disk?` (journal's last entry is
  `parked` AND nothing undelivered) → enter suspended, never bind.
- **Knobs**: `FLEZHA_WORKFLOW_PARK_IDLE_MS` (SDK-side, default 5 s
  hysteresis), `WorkflowOperative::wake_after()`.
- **CLI**: `flz state inbox <instance>`; `flz state journal` renders
  `iter_committed`/`parked`.
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `loopMode()` / `wakeAfter()` / `IterCommit` / `IterPark`.
- **Tests**: `workflow_driver_scenarios` (loop turns → iteration commits +
  park + resume-at-iteration; journal-backed KeepProgress/AllOrNothing),
  `loop_authoring_is_validated`, `test/flezha/state/proc_inbox_test.exs`,
  `proc_journal_test.exs` (iter/park entries),
  `control_channel_proc_commit_test.exs` (IterCommit journal→ack→truncate,
  boundary snapshot, IterPark→suspend),
  `test/flezha/operative/overwatch_suspension_test.exs` (park/rehydrate/
  timer/redeploy).
- **Docs**: `docs/resilience/workflow_operative.md` "Durable loop and
  suspension", `docs/resilience/data_durability.md` "the durable inbox",
  `docs/sdk_protocol_spec.md` `IterCommit`/`IterCommitResponse`/`IterPark`
  + `flz.inbox_offset`.
- **Unblocks**: `operative-rewind-and-correction` → `workflow-loop-rewind`.

