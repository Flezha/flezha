# Workflow durable-path throughput (`workflow-durable-path-throughput`)

Read before touching the commit barrier (`ProcCommit` / `IterCommit`), the
proc-journal, the durable inbox, `WalWriter.append_next`, or the SDK's
`ship_and_await`.

- **The batched append is a deferred reply, the token is the request id.**
  `WalWriter.append_next(pid, header, body, sync: :batched)` writes, marks the
  writer dirty and defers the `GenServer` reply until the batch has synced —
  the `FsyncBatcher` tick, a prefix cut (`after_prefix_cut`), or a clean close
  (`terminate/2` on `:normal` / `:shutdown`); a crash answers nobody, so the
  waiter fails. There is no token type: the caller that must not wait issues
  the append with `:gen_server.send_request` (`WalWriter.append_next_async/6`,
  `RecordLog.append_async/6`, `ProcJournal.append_async/4`,
  `ProcInbox.append_async/5`) and collects the reply with
  `check_response/3` — the seam `InputLog.append_async` already used.
  `Metrics` gained `:fsync_total` (the count a batch shares).
- **One generic writer facility serves the re-send AND the inbox window:**
  `dedup: true` refuses a record whose 16-byte header is already in the file
  (`{:duplicate, seq}`), from a `header => seq` index built on the first keyed
  append, trimmed by every `truncate_up_to`, dropped by a rewind/realign. The
  journal puts its identity in the header (`[epoch u32][type u8][proc_idx
  u16][iter u32][0 ×40]`), so a re-sent barrier is answered from the existing
  entry without a second append; the inbox puts the first 16 bytes of
  `sha256(dedup_id)` there, so an append reads no file and folds no journal.
  A duplicate against a dirty writer is deferred too — it names a record that
  may not be synced yet.
- **The inbox dedup window is the file above its last release**, not "above
  the journal's consumed offset": `ProcInbox.append` no longer consults
  `ProcJournal.consumed_offset/1` at all, which is how task 3.2 ("answer from
  the writer's folded head on the ingest path") was met — the ingest path
  folds no journal. A release whose rewrite is deferred or lost widens the
  window until the next one (a re-sent consumed message is absorbed rather
  than re-delivered). Inbox files written before this change carry zero
  headers and are outside the window once.
- **The delivery floor is the journal's RELEASED offset, not its consumed
  one.** `undelivered/1` used to filter the file by `consumed_offset/1` (the
  live fold, which a rewind moves back) and relied on the file having been
  physically cut. With the rewrite deferred, the rewind e2e re-delivered the
  rewound turns ("undo-and-continue" silently became "undo-and-replay" for
  inboxes under 64 KiB). `ProcJournal.released_offset/1` — the max consumed
  offset over the RAW journal, which no rewind lowers — is the floor now,
  and the e2e keeps its history assertions at the default threshold (a cut
  journal lists only its head iteration; the physical rewrite is exercised
  by the unit tests with the knob at 0, through the worker too).
- **The commit worker** (`ControlChannel.Commits`, one per runner, a
  `RunnerSupervisor` sibling named `Identificator.commits/1`) owns the barrier
  path; the channel's reader decodes and casts. The worker's request
  collection IS the set of acks owed; per-instance order holds because a
  journal writer answers in append order and the worker handles answers in
  arrival order. The ack is written by the channel
  (`send_control_async/3` now takes the request's `reference_id`). The
  sync-durability ship (`DurabilityTier.commit_sync`) runs in a task and
  reports to the worker; `{:proc_outbox_retry, id}` targets the worker.
- **The worker must never wait on a writer, and a cut is not free.** The first
  cut of this change ran `ProcInbox.truncate` + `ProcJournal.cut` inline
  after every iteration ack: two fsync'd file rewrites (40–97 ms measured) on
  the one worker per runner, a ~25 iter-commits/s ceiling, and a single
  serial loop fell from 13 to 9 turns/s. Now the flush stays on the worker
  (in journal order, ~100–250 µs), both cuts are the writers' own work
  (`WalWriter.truncate_up_to_async/3`, `ProcInbox.truncate/2`,
  `ProcJournal.cut/1` casts), and a prefix cut below
  `PersistenceResolver.record_log_cut_after_bytes/0` (`:record_log_cut_after_bytes`, 64 KiB) is
  deferred — the dedup window releases the records at once, the rewrite is
  paid once per threshold. Tests that read the file after a release set the
  knob to 0.
- **Nothing on the worker may touch the disk or wait on a writer — twice
  more:** the boundary snapshot used `Registry.all_entities()` (a walk of the
  whole state root, p50 138 ms under 100 loops) to find the instance's
  Entities — now `Registry.entities_of/1`, one class dir; and the outbox
  flush registered its entry with `WalWriter.await_outbox/3`, a call that
  waits behind the writer's datasync — now `WalWriter.flush_outbox/5`, a
  cast the writer answers by registering AND dispatching itself (one step in
  the owner of the awaiting set, so a re-entered flush cannot race the
  check). Under 100 loops those two took the worker from a 391 ms p50 ack
  wait to ~70 ms (the disk), and 100 × 20 turns from 12.9 to 229 turns/s.
  A per-commit `Logger.warning` probe left in by mistake cost 3× on its own
  — Logger sync mode blocks whoever logs.
- **What remains is the disk's per-file fsync fan-out:** every instance
  journals in its own file, so N instances' barriers in one tick are N
  datasyncs (10 dirty IO schedulers, ~3 ms per jbd2 commit — fsync p99
  25–30 ms at 100 loops, 5 ms at one). A shared commit log per runner would
  be the true group commit; it is a layout change, not this one.
- **The journal cut keeps what the fold needs** (`ProcJournal.cut_floor/1`):
  the latest live `iter_committed`, every entry whose outbox is still owed, and
  the latest `rewound` marker with the commits it has yet to undo. A cut
  journal folds to the same `restore_state/1` except `committed_procs`, which
  only holds the kept iterations — so **only a class with no compensable
  procedure is cut** (`ClassRegistry.compensable_procs/1 == []`): the saga's
  all-or-nothing rollback and a rewind replan from the committed history,
  and a class that declared nothing is not known to be safe. A cuttable loop
  can only be rewound to its current iteration.
- **The SDK re-send ladder** lives in `ship_and_await` (`flz_core`): on a
  wait timeout (`FLEZHA_WORKFLOW_ACK_TIMEOUT_MS`, default 30 s) the same
  frame goes out under the same `reference_id`, after the class's retry
  backoff (`retry::note_failed_attempt` with a policy of
  `barrier_retry_attempts` attempts), up to `barrier_retry_attempts()` /
  `barrierRetryAttempts()` sends (default 3; Java ships
  `barrier_retry_attempts` in the host declaration, `HostWorkflowDecl`
  defaults it). The waiter stays registered across the ladder, so a late ack
  to an earlier send continues the chain. The Elixir SDK has no workflows —
  nothing to mirror.
- **The fake controller in `workflow_driver_scenarios`** journals by
  `reference_id` (`JOURNALED_REFS`) and can lose a barrier's first ack
  (`lose_acks(id, n)`; `usize::MAX` loses every send): a lost ack still
  records the commit and releases its outbox, exactly as the real
  controller's post-ack flush would. `FLEZHA_WORKFLOW_ACK_TIMEOUT_MS=300`
  keeps the ladder inside the drain's deadline.
- **Measure, and measure the right thing:** the campaign's "~350 proc
  commits/s, flat across concurrency" was 4 × the closed-loop spawn rate,
  never the barrier — `start_operative` was p50 943 ms of a 991 ms
  seed→completed before this change. The barrier's own cost shows in a
  single serial loop (`--scenario turns -n 1 -m 100`) and in the commit
  worker's per-barrier wait (one tick, p50 5.6 ms), not in the chain cell.
- Tests: `wal_writer_test` (batched/dedup), `record_log`-level behaviour via
  `proc_inbox_test` / `proc_journal_test` (cut, deferred cut, restart dedup),
  `control_channel_proc_commit_test` (re-send, ping under a frozen batcher),
  `overwatch_confirming_ingest_test` (the async inbox through a live host),
  `overwatch_suspension_test` (`settle_journal/1`), Rust
  `workflow_driver_scenarios` (resend-1/2/3), Java `HostClassDeclarationTest`.
