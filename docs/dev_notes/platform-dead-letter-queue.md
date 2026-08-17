# Dead-letter queue (`platform-dead-letter-queue`)

- **SDK retry-then-park**: both dispatchers re-run a failing invocation
  (returned error or panic/exception, one policy) up to the input
  channel's `retry_max` (default 3 total attempts, exponential backoff +
  jitter capped 5 s); `.retry_max(n)` / `.retry_backoff_ms(b)` on the
  channel builders, `ProcedureError::NonRetryable` /
  `ProcedureException.NonRetryable` parks immediately. Since the
  poll-core rehost ONE shared ladder in `flz_core` drives both languages:
  `poll_core.rs` + `retry.rs` (counting, backoff, re-hand) + `park.rs`
  (blocking wire park, ack registry); the old per-SDK `dispatcher.rs`
  retry loop, `ProcedureDispatcher.runOnce` and `DlqPark.java` are gone.
- **Protocol**: data-stream payload-id `park_payload` (postcard
  `ParkBody`, same outer frame as `entity_state_change`, zero header)
  acked by control command `ParkPayloadAck` (proto tag 27) AFTER the
  store fsync — park-then-advance, SDK re-sends every 2 s until acked.
  Tier-on parks skip the frame entirely: `EffectRecordBody` gained a
  trailing `dlq: Option<DlqEntryBody>` so the park and the
  consumed-offset advance are ONE atomic WAL record; the WalWriter
  materializes the store entry synchronously before
  `InputLog.record_consumed` (watermark un-freezes after the park).
- **Store**: `lib/flezha/dlq/store.ex` — one GenServer,
  `app_data/dlq/<class>/<procedure>/entries.jsonl` + `<id>.bin` blobs,
  fsync-before-ack, idempotent on origin identity
  (`entity@offset` tier-on, `channel:sha256(payload)` tier-off);
  re-park while `status=replaying` bumps `generation`. Retention age AND
  size (30 d / 4 GiB, more restrictive wins), loud
  `[:flezha, :dlq, :retention_dropped]` per deletion. Config `:dlq`
  block in runtime.exs (`FLEZHA_DLQ_DIR`, `FLEZHA_DLQ_RETENTION_DAYS`).
- **CLI**: `flz dlq list|show|replay|drop` (`cli/src/dlq.rs` →
  `CliManager.dlq_*`). Replay re-enters via
  `GenServer.cast(overwatch, {:io_in, payload, %{"channel" => ...}})` —
  fresh output identity by construction (new input → new WAL seqs, NO
  epoch bump); success detected by absence of a re-park within
  `:dlq[:replay_grace_ms]` (5 s) then entry removed; failing replay
  re-parks generation+1. `drop`/`replay` audited via `:flezha_dlq`
  events (TelemetryBridge mapping + audit-sink allow-list).
- **Observability**: `dlq_parked_total`, `dlq_dropped_by_retention_total`,
  `dlq_usage` metrics bridge to the bus (source `:flezha_dlq`).
- **Workflow profile**: out of scope — AbortAll + saga compensation is the
  equivalent escape; nothing is written to the DLQ store.
- **Tests**: `sdk/rust/tests/dlq_retry.rs` (the Java `DlqRetryTest` was
  removed with the poll-core rehost — Java is covered by the e2e),
  `test/flezha/dlq/store_test.exs`, WalWriter park cases in
  `wal_writer_outbox_test.exs`, CLI `cli_manager_dlq_test.exs`, e2e
  `test/system/dlq_e2e_test.exs` (Rust + Java + tier watermark;
  `--include system [--include java_sdk]`). Poison test procs:
  `poison_probe` (Rust) / `java_poison_probe` (rebuild the committed jar
  with `scripts/build-dist.sh` after SDK changes!) / `tier_store` value
  `"poison"`.
- **Docs**: `docs/resilience/data_durability.md` "Dead-letter queue",
  `docs/sdk_protocol_spec.md` `park_payload`/`ParkPayloadAck`, glossary.

