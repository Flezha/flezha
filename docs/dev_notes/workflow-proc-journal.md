# Workflow proc-journal (`workflow-proc-journal`)

- **Store**: `lib/flezha/state/proc_journal.ex` — per-instance `proc.journal` at
  `<state_root>/<inst>/<inst>/proc.journal` on the WalWriter+FsyncBatcher
  substrate under reserved identity `Registry.journal_id/1`
  (`{inst, inst, "_proc_journal"}`); entry codec
  `[type u8][proc_idx u16][iter u32][payload]` inside normal WAL framing,
  type 1 = `proc_committed` (entities-ran + channel-tagged handoff blobs);
  types 2–6 (`outbox_flushed`, `iter_committed`, `parked`, `compensated`,
  `rewound`) are all ACTIVE now. Size bound =
  `:wal_high_watermark_bytes` → `{:error, {:oversized_handoff, ..}}`. The
  journal writer skips the outbox dedup side-file so it never registers a
  phantom Entity.
- **Handshake**: SDK→controller `ProcCommit` (proto tag 28) at every proc
  barrier of a durable structured workflow; ControlChannel appends +
  fsyncs, replies `ProcCommitResponse` (tag 29) under the SAME reference_id
  (journal-then-ack); driver blocks ≤30 s (`workflow/driver.rs
  commit_proc_boundary`), failure/timeout → terminal error + compensation.
  Telemetry `[:flezha, :state, :proc_commit]`.
- **Resume**: journal state rides `AddOperative.proc_journal` (field 6,
  `ProcJournalState`) on the bind; `drive_chain` skips committed procs and seeds the next
  batch from the journaled handoff via the channel codec. Compensation
  precedence: a comp-**done** marker wins (intents alone don't — they also
  accumulate during forward progress). Skipped procs still roll back on a
  later abnormal exit (folded from intent markers).
- **Codec**: `.codec()` (postcard/serde) or `.codec_with(enc, dec)` on
  inter-proc channels; REQUIRED for durable structured workflow classes —
  registration fails `MissingInterProcCodec` naming the channel. Opted-out
  classes skip journal + handshake entirely (re-run-from-top preserved).
- **CLI**: `flz state journal <operative/instance>`
  (`CliManager.state_journal/2`).
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `ProcCommit` handshake + journal-gated
  resume-skip. The e2e no longer branches per runtime.
- **Tests**: `test/flezha/state/proc_journal_test.exs`,
  `test/flezha/runner/control_channel_proc_commit_test.exs`, Rust
  `workflow_driver_scenarios` (gating + resume-skip + codec validation),
  e2e `test/system/workflow_crash_resume_e2e_test.exs` (proc A NOT re-run on
  rust; opted-out `optout_commit_block` re-runs from top).
- **Docs**: `docs/sdk_protocol_spec.md` `ProcCommit`/`ProcCommitResponse`,
  `docs/resilience/workflow_operative.md` "Durability" section.

