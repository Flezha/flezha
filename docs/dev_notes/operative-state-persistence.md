# Per-Entity state persistence (`operative-state-persistence`)

- **Modules**: `lib/flezha/state/` — `Registry`, `Wire`, `Metrics`,
  `Outbox`, `PersistenceResolver`, `Supervisor`, `WalWriter`,
  `FsyncBatcher`, `Snapshot`, `Retention`, `Restore`, `TimeTravel`
  (`Flezha.State.TestHelpers` moved to `test/helpers/`).
- **On-disk layout**: `app_data/state/{namespace}/{operative_class}/{procedure_id}/{entity_key}.{wal,snap.{seq},outbox.dedup}` (the `namespace` segment came with `finish-platform-authn-authz` via `Registry.class_dir/1`; `runner_id` was dropped by `auto-restore-on-boot`; `Flezha.Boot.StatePathMigrator` handles upgrade migration).
- **Wire payload-ids**: `entity_state_change`, `entity_state_snapshot`,
  `delete_entity_state` (data stream); `RestoreInitState`, `RestoreInitStateResponse`,
  `CompactEntity`, `CompactEntityResponse`, `EntityMailboxStatus`
  (control channel). See `docs/sdk/protocol_spec.md` "Per-Entity
  State Persistence".
- **Dynamic entity mode**: `flz_core/src/dynamic.rs`
  (`DynamicEntityRegistry` + reaper; re-exported as
  `flezha_runner::entity::dynamic`). Dynamic mode = empty `entity_keys()`
  PLUS a consumer edge declaring `.key_by(...)` — `entity_key()` was
  deleted by `stream-dsl-keyby-and-transforms`. Entities are created
  lazily per key. Eviction is OFF by
  default (`entity_idle_ttl() -> None`); when a TTL is opted in, the
  per-Procedure reaper evicts idle Entities **destructively** — it ships
  `delete_entity_state` (SDK→controller) and the recreate cold-starts
  fresh (no restore-from-disk, by decision 2026-06-16). Controller handler:
  `Flezha.State.Supervisor.delete_entity/1` (stops WalWriter, drops
  FsyncBatcher mark, unlinks files via `Registry.delete_files/1`).
- **Runtime config knobs**: `:state_root` (default `app_data/state`),
  `:agent_fsync_interval_ms` (5 ms), `:wal_high_watermark_bytes`
  (16 MiB), `:wal_low_watermark_bytes` (`high / 2`),
  `:time_travel_retention_check_interval_ms` (60 s),
  `:time_travel_retention_hours` (per-Operative map), `:operative_persistence`
  (per-Operative map), `:default_compact_after_bytes` (8 MiB, env
  `FLEZHA_COMPACT_AFTER_BYTES`; SDK trait default in lockstep),
  `:compact_after_age_ms` (300 s, `0` disables — and is `0` in `:test`;
  env `FLEZHA_COMPACT_AFTER_AGE_MS`).
- **Snapshot baseline durability (`snapshot-baseline-durability`)**: the
  `Watchdog` sweep auto-triggers `CompactEntity` on WAL **size or age**
  (`[:flezha, :state, :compaction_trigger]`); a fresh controller with an
  empty WAL for a live runner's Entity **pulls** a baseline
  (WalWriter awaiting-baseline sheds + one `CompactEntity`,
  `[:flezha, :state, :baseline_seed]`) instead of erroring; the resync
  path never pushes an empty `RestoreInitState`
  (`Restore.has_persisted_state?/1`, zero-byte WAL ≡ no state).
- **Credit-grant flow control (`credit-grant-flow-control`)**:
  runner→controller state-stream back-pressure is credit-based — the
  `StreamChannel` offers `CreditWindowInit` on the first state frame
  (per-Operative `:credit_window_bytes`, default `wal_high_watermark_bytes`;
  kill switch `FLEZHA_CREDIT_FLOW_CONTROL`), debits each frame's wire size,
  and mints one batched `CreditGrant` per fsync-drain below
  `:credit_low_watermark_bytes` (default `window / 2`). ONE shared SDK
  window in `flz_core/src/credit.rs` since the poll-core rehost (the old
  per-SDK `entity/credit.rs` and `CreditWindow.java` are gone;
  park in the flusher under the buffer lock; `credit_parked` in
  `data_heartbeat` metadata). Credit sessions get no `EntityMailboxStatus`;
  shed/overrun on one emits `[:flezha, :state, :credit_anomaly]`. Soak:
  `test/load/credit_soak_test.exs` (`MIX_RUN_CREDIT_SOAK=1`). The
  source-ingestion credit loop is BUILT now: `InputLog` debits a
  per-operative `source:` `CreditLedger`, parks the ingest caller at zero
  credit and unparks on `:watermark_advance`
  (`:source_credit_window_bytes`).
- **CLI**: `flz state list <operative>`, `flz state at <operative/procedure/key> <seq|ts>`,
  `flz state rotate <operative/procedure/key>`, `flz state retention <operative> <hours>`,
  `flz state delete <operative/procedure/key>`. Implemented in `cli/src/state.rs`;
  RPCs into `Flezha.Service.CliManager.state_*/N`.
- **Detailed notes**: `docs/internals/state_persistence.md`.

