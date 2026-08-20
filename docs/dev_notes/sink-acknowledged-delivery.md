# Delivery confirmation gates the outbox cursor (`sink-acknowledged-delivery`)

- **The one seam**: `Flezha.State.Outbox` — already the module that owns the
  flush cursor's durable form — grew the confirmation seam. `dispatch/3` is
  the ONE place a durable output leaves an outbox: it stamps the identity
  (`Wire.output_stamps`) plus the `flz.ack` token and casts `:io_out`, so the
  token cannot be forgotten by a future outbox. `token/2,3` mints it as
  `(owner, target, key)` — target elided when it equals the owner — and
  `decode/1` reads it back. Four reports route to the owning writer: `hold/1`
  (a sink took the record), `settled/1` (the chain has run one output),
  `confirm/1` (a sink's write is durable), `park/4` (permanent failure → DLQ
  then confirm). A record with no token makes every one of them a no-op, which
  is what keeps plain non-tier output byte-identical.
- **A sink opts in by taking the hold — the plugin TYPE cannot say it.**
  `plugin_type() == :io_binding` also covers `filter`, `std_out`,
  `beam_msg_forward`, `websocket` and `http_poll`, none of which confirm, so
  deriving the hold from the type would wedge every cursor behind a `std_out`
  sink.
- **The owner is always a `WalWriter`.** The stream tier's owner is the
  Entity's writer; the workflow outbox's owner is the instance's *journal*
  writer (`Registry.journal_id/1` is a `WalWriter` under the reserved key).
  So all confirmation state lives in one module. The completion differs:
  `{:cursor, seq}` is handled inside the writer, `{:marker, ...}` is
  `Outbox.complete/2` → `ProcJournal.mark_flushed/5`, **spawned** — appending
  to the journal from inside the journal's own writer is a call to itself. The
  marker entry stays registered until that append reports back
  (`{:outbox_marked, key}`): dropped any sooner, a journal-driven re-flush
  reads the journal, still sees the entry owed, and delivers it twice.
- **The tally, not a flag.** Per entry: `pending` (outputs still in the
  chain), `holds`, `confirms`. Complete when `pending <= 0 and confirms >=
  holds`. Holds and settlements are both sent from the process running the
  chain (the Overwatch), so a hold can never be seen after its own
  settlement; registration always precedes any report (a stream writer
  registers inside the handler that casts, the journal flush registers with a
  synchronous `WalWriter.await_outbox/3`), so a report for an unknown key is
  one a resettle superseded and is dropped.
- **`pending` may go negative**, hence `<= 0`: a superseded chain run can
  settle late. Its outputs carried the same identity, so it is still complete.
- **Cursor order is the `outbox_inflight` queue, not the map.** The cursor
  drains the leading run of entries no longer in `outbox_awaiting`. Taking
  `min(remaining keys)` would be O(n) per confirmation; the queue makes it
  O(1) amortised. `awaiting` in `outbox_status/2` is `map_size(outbox_awaiting)`
  ONLY — the queue holds completed-but-undrained entries too, and handover
  consent must not defer on those.
- **The lost-settlement hole, and why the resettle exists.** If the Overwatch
  dies between the `:io_out` cast and the settlement, nothing will ever
  complete the entry and the cursor wedges for good (the entry is no longer in
  `pending_outbox`). So the tally retains the entry, and the existing
  `:outbox_retry` tick re-dispatches any entry whose `pending > 0` past
  `:outbox_resettle_ms` (default 60 s), resetting the tally — a hold the lost
  run took would otherwise never be answered. The journal side keeps no copy:
  dropping its tally is what lets the next journal-driven `flush_pending/2`
  re-drive it. `resettle_due?/1` checks the inflight head first so a healthy
  writer pays one comparison per tick. A resettle re-dispatches the OUTPUTS
  only (`dispatch_outputs/4`): the record's own side effects — the dead-letter
  park, the consumed-offset advance, the tier's interval cut — belong to the
  record, not to a delivery attempt, and already landed.
- **`RecordLog.append/3` had a latent race** that only concurrent marker
  appends exposed: it read `last_persisted_seq` and then appended, so two
  processes appending to one log picked the same seq and the loser's record
  was refused as a duplicate. Fixed at the source — `WalWriter.append_next/3`
  assigns the seq, writes and fsyncs in one call. `ProcInbox` gets the fix for
  free.
- **Kafka `:queue_overflow` is now derived**, because `Map.has_key?(opts,
  :queue_overflow)` is a real test: the provider struct's defaults never reach
  `init_provider/1` (the generated `build/1` is unused in `lib/`), so an unset
  option means the operator did not choose. Unset + guaranteed record →
  `:block`; unset + plain record → `:drop_oldest`.
- **`:block` on the cast path now blocks the record, not the caller.** The old
  code treated it as `:drop_newest` with a warning because "a cast has no
  caller to wait on" — but the record can wait in the existing `blocked`
  queue, which `process_blocked/1` already drains. No `{:block_timeout, ...}`
  is armed for a cast (its `from` is the atom `:async`, and `drop_blocked/2`
  splits on `from` — one timeout would drop every async record). The queue is
  bounded upstream by `:outbox_inflight_max` per Entity, not by `queue_max`.
- **An explicitly configured drop of a guaranteed record does NOT confirm.**
  Spec-literal: only a confirmed or *parked* range may advance the cursor, and
  a queue-overflow drop is not a delivery attempt. It logs at `error` and tags
  the drop event `guaranteed: true`; the Entity's outbox then stops draining,
  which is the loud outcome. Parking such a drop instead would silently turn
  an operator's "drop" into DLQ growth.
- **`operative_forward` holds only on the routed branch.** An unroutable
  payload parks (existing `park_unroutable/4`), takes no hold, and the
  settlement completes the entry — a park is a durable outcome the cursor may
  pass.
- **Tests**: `test/flezha/state/wal_writer_outbox_test.exs` (hold → confirm,
  out-of-order confirmation, park, in-flight window, resettle),
  `test/flezha/state/proc_journal_test.exs` (marker waits for the sink,
  re-entered flush does not re-deliver), `test/plugin/kafka/producer_worker_test.exs`
  (confirm, park, derived `:block`, alarm-grade drop, `:block` on the cast
  path), `test/plugin/provider/postgres_sink_test.exs` (commit confirms;
  needs `POSTGRES_INTEGRATION=1` + `docker compose -f docker/dev/docker-compose.yml
  up -d postgres-sink`, host port 5433),
  `test/flezha/plugin/provider/operative_forward_test.exs`,
  `test/flezha/convergence/stall_classifier_test.exs`.
  A test process standing in for the Overwatch **must** call
  `Outbox.settled/1`, and must `hold/1` *before* settling.
  `Flezha.State.TestHelpers.stub_outbox_owner/1` + `guaranteed_metadata/3` are
  the shared fixtures for a sink suite with no persistence subsystem behind it.
- **Docs**: `docs/resilience/data_durability.md`, `docs/architecture/kernel.md`,
  `docs/resilience/state_persistence.md`, `docs/kafka-producer.md`.
