# Stream late-input correction (`stream-late-input-correction`)

- **THERE IS NO SPLICE.** `Ingest.journal_input/2` appends to the input log
  BEFORE forwarding (`lib/flezha/operative/ingest.ex`); the SDK extracts event time later, in
  `dispatch`. So a late input is already durable in the log at its arrival offset
  by the time anything knows it is late. The slice is an offset range and already
  contains it. The correction = the EXISTING tier restore-and-replay path with
  (i) a rewound baseline and (ii) an event-time-sorted slice. **No new control
  message, no correction journal, no crash marker, no proto change.**
- **Crash-mid-correction needs no marker** — before the rewind commits, nothing
  durable changed (the late input is still unconsumed → re-classified → correction
  re-runs); after it commits, the on-disk state IS the baseline and the ordinary
  resume replay IS the correction. Hence: for a correction-declaring class, EVERY
  journaled replay is marked and sorted.
- **Baseline = scan FORWARD to the first record with `event_time >= T`, keep the
  one before it.** A backward scan for "first earlier event time" is WRONG — it
  stops at an out-of-order straggler (a beyond-window `process_as_arrival` input)
  and leaves the records that overtook the late input in place, correcting
  nothing. Pinned by `wal_rewind_test.exs` "folds an out-of-order straggler".
- **`WalWriter.rewind_to(seq, epoch)`** — keeps records ≤ seq, drops ONLY snapshots
  > seq, re-derives `pending_outbox` from the rewritten file, lowers the input-log
  watermark (`InputLog.rewind_consumed/3`). NOT `realign_epoch/4`, which truncates
  the WAL to zero and `File.rm`s every snapshot (a cold-takeover wipe). Shared tail
  `bump_epoch/2` (outbox cursors + `persist_dedup` = the COMMIT POINT). `epoch` is a
  PARAMETER, not `current+1` — that is what makes a crash-resumed correction
  idempotent instead of double-bumping.
- **`flz.replay_end` is a BROADCAST, not a mark on the last input.** An input routes
  to ONE entity by key, so a marker riding the last one strands the slice every
  OTHER entity buffered. It is a payload-less frame intercepted in `fast.rs` before
  channel lookup → `host::deferred_end_replay_slice` →
  `routing::end_replay_slice` (in `flz_core`).
- **Drain ordering gotcha**: a closed slice must flush BEFORE any live input popped
  behind it (`poll_core.rs flush_closed_slice` runs both mid-pop-loop and
  after it, guarded by `closed_slice_pending`) —
  otherwise the live input jumps ahead of the very inputs being re-ordered. Caught by
  `a_replayed_slice_dispatches_in_event_time_order`.
- **`do_sweep` gates on the FLOOR, not the watermark** — under a hold window the
  floor advances with the clock while the watermark stands still, so the old
  watermark-advanced guard never truncated.
- **Effect record gained `event_time: u64`** (same `format_id = 2`; no legacy shim —
  the library is unpublished). Serves both the correction baseline AND
  `TimeTravel`'s `{:timestamp, ts}` (which used to approximate from the WAL file's
  mtime; `pick_snapshot_by_mtime/2` deleted). Cross-language fixture in
  `flusher.rs` + `wire_test.exs`.
- **No snapshot-header high-water needed**: `compact_after_age_ms >= correction_window`
  guarantees the retained WAL always covers the window, so max `event_time` over the
  restored change log IS the high-water (anything older is beyond-window anyway).
  `compact_after_age_ms` therefore moved onto the per-operative `resolve/5` cascade —
  it is a correctness constraint now, not a tuning knob.
- **Declaration**: `c.correction_window(ms)` + `c.beyond_window_policy(..)`; rides
  `AddOperativeResponse.metadata` → `ClassRegistry.decode_declarations/1`. Refusals: tier
  + `EventTimeFrom` + all-channels-keyed (SDK build); hold-window + snapshot age-floor
  ≥ window (controller, at start). Beyond-window default = `process_as_arrival` (it is
  today's behaviour — declaring a window must never silently start dropping data).
- **Limit worth knowing**: a correction overwrites the keys it re-emits; it does NOT
  retract a key the stale run emitted and the corrected run does not. Safe for
  event-time-bucketed keys (candles); documented in `data_durability.md`.
- **SCOPE DECISION — no `input.idx` side-file.** The hold window now actually bounds
  the log, so a header-only scan is O(retained). A durable index would add
  crash-consistency + truncation-trimming + rebuild for a speculative win.
- **NEVER reset the event-time high-water on a replay.** Every replay is preceded
  by a restore, which already rebuilt it from the change log. Zeroing it first
  (an early draft did) is lossy on a crash-resume whose unconsumed tail carries a
  LOWER max event time than the recovered state — the entity comes back with a
  high-water below the truth and silently reads a genuinely late input as on-time.
  Pinned by `a_replay_does_not_lower_the_event_time_high_water`.
- **`WalWriter.fold_records/3`** is now the ONE place the
  `decode_record → recurse → halt-on-torn-tail` walk lives. There were **11**
  copies across `wal_writer`/`input_log`/`time_travel`/`record_log`/`retention`;
  all but `scan_for_last_valid_seq` (its error branch truncates the torn tail —
  a side effect, not a fold) now go through it. `Wire.decode_effect_record/2` is
  the matching "header check + body decode" pair. `Retention.rewrite_offline/2`
  reuses `WalWriter.rewrite_keep_seq_gt/2` instead of its own byte-copy.
- **CLI**: `flz operative replay <operative/procedure/key> --from <seq|ts>`.
  Telemetry `[:flezha, :correction, :completed|:refused]`, `[:flezha, :state, :wal_rewind]`.
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `c.correctionWindow(ms)` + event-time slice.

