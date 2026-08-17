# Workflow staged commit + transactional outbox (`workflow-staged-commit-and-outbox`)

- **Staging (SDK)**: inside a structured proc `ctx.emit` NEVER hits the wire —
  `WorkflowEntitySink` (per entity, per stage) buffers `(channel, bytes)`
  when `ChainCtx.journaling()` (durable class AND coordinator installed);
  otherwise (opted-out / transport-less) it forwards direct, as before.
  `WorkflowCollectSink::emit_plugin_bytes(channel, bytes)` carries the
  channel name (`descriptor.name`, same as the eff-once effect record).
  A failing stage drops its staged bytes with the `Err` — nothing leaks.
  `run`-sugar keeps the direct path (no barrier to stage against).
- **Transport**: extends the SAME `ProcCommit` handshake — `outbox`
  (`ProcOutboxItem{entity_id, seq_in_proc, channel, payload}`, tag 6) +
  `compensating` (tag 7). No second protocol. The shared journal-then-await
  helper is now `ship_and_await` (`flz_core/src/workflow/driver.rs` — it
  also carries `IterCommit`), used by the forward barrier and the undo
  record; the undo wait ignores the
  cancel token (rollback IS the response to that cancel).
- **Journal**: `proc_committed` carries entities+handoff+outbox in ONE record;
  `outbox_flushed` (type 2, payload `[for_type u8]` — forward vs undo are
  gated independently) and `compensated` (type 5, outbox only) are now
  ACTIVE. Epoch = u32 in the WAL record **header** (bytes 0–3; byte 9 is the
  effect-record format-id, so no collision), 0 until rewind bumps it.
- **Flush**: `ProcJournal.flush_pending/2` is the ONE path — post-ack
  (`ControlChannel`, retry timer on `:no_overwatch`) and restore re-drive
  (`Flezha.Operative.Replay.replay_persisted_entities/4`, gated
  `kind == :workflow`, passes its own pid). Journal state (not memory) decides what is owed →
  idempotent by construction. Delivery = `GenServer.cast(overwatch,
  {:io_out, payload, stamps})`, same fan-out as `WalWriter`'s eff-once
  outbox. Stamps: `flz.entity`, `flz.seq` = `<proc_idx>.<seq_in_proc>`
  (undo: trailing `.1` — numeric so the Postgres sink's
  `string_to_array(seq)::bigint[]` guard still orders it), `flz.epoch`,
  `channel`. Telemetry `[:flezha, :state, :proc_outbox_flush]`.
- **Ordering gotcha**: the undo record is journaled BEFORE the
  `compensation_done` marker — a crash between them re-runs the (idempotent)
  hook rather than losing its undo output.
- **Tests**: `workflow_driver_scenarios` (staging, leak-nothing, run-sugar
  direct, `UndoSaga` comp staging; the fake controller now FLUSHES each
  commit's outbox on ack, which is why the old output assertions still
  hold), `test/flezha/state/proc_journal_test.exs` (crash-before-flush
  re-drive with identical keys = truncate journal back to pre-flush bytes),
  `control_channel_proc_commit_test.exs` (sync on `:sys.get_state(channel)` —
  the marker append lands after the `:io_out` cast).
- **Java SDK**: AT PARITY (`java-sdk-parity`) — staged commit + `ProcCommit` outbox.
- **Docs**: `docs/sdk_protocol_spec.md` `ProcCommit`/`ProcCommitResponse`,
  `docs/resilience/data_durability.md` "Workflow operatives: staged commit
  and the transactional outbox".

