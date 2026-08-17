# Checkpoint ack-granularity (`effectively-once-checkpoint-granularity`)

- **The kernel was already checkpoint-capable.** `EffectRecordBody.consumed_offset`
  is `Option<u64>` and EVERY controller reader already tolerates `nil`:
  `dispatch_one_effect` advances the watermark only `if is_integer(...)`,
  `max_offset/2` skips nils rebuilding the durable watermark at open,
  `consumed_offset_of/1` folds nil→`-1` for the correction baseline. So the mode
  is a **producer-side** change: no new record type, no new control message, no
  proto change, no new retention rule.
- **Interval replay = the EXISTING crash-resume replay.**
  `Replay.restore_and_replay_journal/1` already re-forwards
  `InputLog.watermark + 1 .. last_offset` on every tier bind. Hold the watermark
  at the last checkpoint and that path IS the interval replay. Same for retention
  (truncates below the min durable consumed-offset = the last checkpoint) and for
  credit (drains on `:watermark_advance` → bursts at checkpoint cadence). All
  three requirements fell out with ZERO controller code.
- **SDK**: `c.checkpoint_ack()` (needs `effectively_once()`, else
  `CheckpointAckWithoutTier`) + `ctx.checkpoint()`. `TypedBuffer` gained a third
  vec `staged` — the open interval's changes. `drain_to_frames`/`is_empty`
  deliberately IGNORE it (an open interval is not flushable; draining it alone
  would put state ahead of its offset — the exact tear the mode prevents).
  Dispatcher emission: `closing = checkpointed || dlq.is_some()` → ship
  `{staged, consumed_offset}`; else ship `{[], outputs, None}` iff outputs exist.
- **Mid-interval emits flush immediately** (open question A/B → **A**). Option B
  (stage until the checkpoint) buys nothing: the interval's outputs are re-emitted
  on replay under either option, because the inputs are re-forwarded. Keyed sinks
  absorb it — the tier already requires them.
- **A park CLOSES the interval** (the DLQ open question). The park record already
  carries `consumed_offset`; letting it also carry the staged changes means the
  poison offset is acknowledged and the replay starts AFTER it. No offset-skip set,
  no DLQ lookup on replay. It is also the only correct choice — otherwise the
  poison is acked while the state before it is not durable.
- **A restore discards the open interval** (`FlushableBuffer::discard_staged`,
  called in `restore_into_slot` beside the mailbox/replay-buffer clears). Same
  rationale: it belongs to a past that no longer exists.
- **The controller needs the declaration for exactly ONE reason**:
  `Watchdog.sweep_catastrophic_dataflow` reads "produced output before, silent
  since" as a dead feed — a lie for a checkpoint class, whose silence is its steady
  state. Rides `AddOperativeResponse.metadata` (`ack_granularity=checkpoint`) →
  `decode_declarations/1` → `ClassRegistry.put_checkpoint_ack/2` (ETS + sidecar,
  same shape as the tier; `put_flag/4`+`flag?/3` now share that shape).
  `sweep_output_staleness` needs NO gate — it fires only when changes are still
  arriving but persistence froze, which is a genuine wedge in checkpoint mode too.
  `StallClassifier.watermark_frozen?` would misfire, but it is currently
  unreachable (`progress_markers/0` never populates the tier cursors).
- **Tests**: `operative_dispatch.rs` (interval holds / park closes / restore
  discards), `builder.rs` (`checkpoint_ack_without_the_tier_is_refused`),
  `wal_writer_outbox_test.exs` (nil-offset record flushes outputs, holds the
  watermark), `class_registry_test.exs`, `add_operative_response_test.exs`,
  `watchdog_test.exs` (with a positive control — the pre-existing workflow
  catastrophic refute was vacuous: `:catastrophic_dataflow_timeout_ms` defaults to
  60 s and no test armed it), e2e `effectively_once_checkpoint_e2e_test.exs`
  (fixture `checkpoint_store`, `commit:` closes the interval).
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `EffectRecord` encoder + `c.checkpointAck()`
  + `ctx.checkpoint()` + the staged interval.

