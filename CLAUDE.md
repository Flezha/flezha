# Flezha repo notes

## Per-Partition state persistence (`relay-state-persistence-agent`)

- **Modules**: `lib/flezha/state/` — `Registry`, `Wire`, `Metrics`,
  `Outbox`, `PersistenceResolver`, `Supervisor`, `WalWriter`,
  `FsyncBatcher`, `Snapshot`, `Retention`, `Restore`, `TimeTravel`,
  `TestHelpers`.
- **On-disk layout**: `app_data/state/{compute_engine_id}/{relay_class}/{shard_id}/{partition_key}.{wal,snap.{seq},outbox.dedup}`.
- **Wire payload-ids**: `partition_state_change`, `partition_state_snapshot`
  (data stream); `RestoreInitState`, `RestoreInitStateResponse`,
  `CompactPartition`, `CompactPartitionResponse`, `PartitionMailboxStatus`
  (control channel). See `docs/sdk_protocol_spec.md` "Per-Partition
  State Persistence".
- **Runtime config knobs**: `:state_root` (default `app_data/state`),
  `:agent_fsync_interval_ms` (5 ms), `:wal_high_watermark_bytes`
  (16 MiB), `:wal_low_watermark_bytes` (`high / 2`),
  `:time_travel_retention_check_interval_ms` (60 s),
  `:time_travel_retention_hours` (per-Relay map), `:relay_persistence`
  (per-Relay map).
- **CLI**: `flz state list <relay>`, `flz state at <relay/shard/key> <seq|ts>`,
  `flz state rotate <relay/shard/key>`, `flz state retention <relay> <hours>`.
  Implemented in `cli/src/state.rs`; RPCs into
  `Flezha.Service.CliManager.state_*/N`.
- **Detailed notes**: `docs/agent_state_persistence.md`.
