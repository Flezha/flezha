# Workflow performance campaign (2026-08-22)

Report: `WORKFLOW-PERF.md`. Harness: `mix bench.workflow` on the shared
`Mix.Tasks.Bench.Fleet` + `Mix.Tasks.Bench.Tally` (the roundtrip tally,
extracted); fixtures `bench_chain_{1,4,16}`, `bench_chain_optout_4`,
`bench_loop_workflow` in `test/apps/rust_runner/src/main.rs`; matrix scripts
`bench/workflow/batch*.sh`, logs `bench/workflow/reports/`.

- **A ping TIMEOUT never resets the live control channel** —
  `Lifecycle.attempt_ping_and_start/3` resets only on a non-timeout ping
  error or on the retry ladder's last attempt (`last_attempt?:` from
  `do_start_with_retry/4`). Before: one busy instance's timed-out ping
  nil-ed the `stream_ref` every other instance on that runner used →
  `{:error, :no_connection_available}` everywhere, ack timeouts, refused
  starts, terminal loop failures. The migrate path (`:runner_migrate`) is a
  one-shot and therefore never resets on a timeout. Tests 5.2/5.3 in
  `overwatch_retry_test.exs`; spec `runner-controller-connection-healing`.
- **`CompactEntity` resolves a workflow Entity** — `compact.rs` falls back to
  `runner.find_workflow_entity` like `RestoreInitState`; before, every
  boundary snapshot of a loop logged `no live entity matches` and no workflow
  Entity was ever compacted. e2e `snapshot_count` assertion in the loop test.
- **`migrate_operative/1` carries `kind:`** — a convergence reroute of a
  workflow re-added it as a stream class (`StreamOperative class … is not
  registered`) and never healed. `control_unit_partition_test.exs`.
- **Retained finished-job state is not intent-mismatch evidence** —
  `Loop.intent_mismatch/2` rejects ids with a `Completion` record; before,
  every completion joined the warning list and the tick diffed the whole
  retained set (126 ms at 1 000 retained). `convergence_test.exs`.
- **A stopped operative's writers are closed** — `State.Supervisor.close_writers/1`
  from `OperativeManager.delete_operative_state_dir/1`, retained or deleted;
  before, K+2 `WalWriter` processes per completed durable workflow lived for
  the 72 h retention window (9 867 after 789 16-proc chains), and stream
  writers outlived their operative too. `wal_writer_test.exs`.
- **A completed workflow drains its owed outbox before teardown** —
  `ControlChannel.drain_owed_outputs/1` waits (≤ 60 s, re-driving
  `flush_pending`) until `ProcJournal.pending_entries/1` is empty; before,
  the last proc's post-ack flush raced `stop_operative` and 3/1000 durable
  chains lost their terminal output with `status: "ok"`. The opted-out
  class's terminal emit still races the completion on the data stream
  (open, `WORKFLOW-PERF.md` §6).
- **A restore re-arms the change seq from the controller's terminal** —
  `RestoreInitState.terminal_seq` (proto field 6, from
  `Restore.assemble_payload/1`'s `terminal_seq_hint`) and
  `restore_into_slot` takes `max(change-log terminal, cmd.terminal_seq)`.
  Before, `restore_state` derived the terminal from the change-log records
  only, so a snapshot-only restore (empty WAL tail — the normal state after a
  compaction) restarted `change_seq` at 1 and the writer silently dropped the
  next `snapshot_seq` changes as duplicates (`dedup_drops_total`). Masked for
  workflows by the compaction defect; latent for streams. The loop e2e
  counts distinct Changes compaction-proof (`change_count/2`).
- **A park that lands during an un-park wins** — `Overwatch.parked_in_flight`
  is set by `suspend_at/3` while a `migrate_task` is binding;
  `apply_migrate_result/2` then keeps the suspension and re-runs
  `rehydrate_if_pending/1` instead of adopting the task's stale bound state
  (which left the controller believing an instance the runner had already
  torn down was bound: 1000 sleepers wedged with their turn journaled and
  never delivered). The task `:DOWN` path re-checks too.
- **A rehydrated loop waits for the inbox tail it is owed** —
  `AddOperative.inbox_tail_count` (proto field 11, `length(ProcInbox.undelivered/1)`
  at bind in `with_resume_state/3`) → `WorkflowInstance.owed_inbox` →
  `ChainCtx.owed_inbox`; `wait_for_input` waits `OWED_INBOX_WAIT` (60 s)
  instead of the park idle while items are owed. Before, the driver's idle
  clock started at `AddOperative` while the controller forwarded the tail
  only after `complete_init` — under load later than a 500 ms idle — so the
  driver parked, the tail hit a closed inbox and was dropped, and the park's
  rehydrate-if-pending replayed the race (1 000 wakes → 2 000 rehydrations,
  2 000 drops, 0 replies). Spec requirement in `workflow-durable-suspension`.
- **A suspended Overwatch hibernates** — both suspension entries return
  `:hibernate`; before, a sleeper kept its bind-time garbage (restore frames,
  plugin setup) on the heap: 2 499 KiB → 5 KiB after a forced GC, which was
  the whole "470–600 KiB per sleeper" cost of a resting population.
- **process_hub reconcile** — `Recovery.Round.stop_undeclared/3` applies the
  two-consecutive-rounds memory (`StorageKey.rup`); commit bb49842 on the
  process-hub `dev` checkout (not pushed; bump the pin after pushing).
- **Measure, don't assume**: the proc-commit ceiling (~350/s on two runners,
  flat across concurrency) is `handle_proc_commit/2` fsync-ing inline on the
  control-channel reader; the per-turn O(history) is `ProcInbox.append/3`
  reading the inbox + folding a never-cut journal. Both are the OpenSpec
  change `workflow-durable-path-throughput`, not a patch.
- Gotchas for the harness: a run-once chain takes its entry from the spawn
  args (`init_values` → JSON), a loop from its inbox; the `suspended`
  telemetry fires from `enter_suspended` (boot restores included); an
  unpaced turn pacer's in-flight cap is global, so a loop whose next turn
  lands after the park idle legitimately parks; a stall verdict must be
  phase-local.
- The teardown drain's bound is what loses outputs under a start storm
  (7/1 000 at 10 s: `completed with N outbox entries still owed`); the flush
  queues behind the same serialized channel as everything else.
- Catching the ControlUnit's 5 s `send_control` call timeout instead of
  letting it crash was tried and REVERTED: a wedged channel then serialized
  every caller at 5 s each (37 parks in 3 min). The crash is bad; the
  serialization is worse; the cure is the OpenSpec change.
- **A suspended instance is not plugin-diffed** — `Diff.plugin_divergence/2`
  skips `suspended?` instances; before, every declared plugin of a parked
  loop read as missing (its plugins left with the runner), so the loop logged
  a thousand sleepers degraded and healed them; the rehydrating bind replays
  the cspec plugins anyway. Open question of the same shape: a parked
  (`init_error`) instance has a pid and no binding too.

