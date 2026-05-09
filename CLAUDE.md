# Flezha repo notes

## Per-Partition state persistence (`relay-state-persistence`)

- **Modules**: `lib/flezha/state/` — `Registry`, `Wire`, `Metrics`,
  `Outbox`, `PersistenceResolver`, `Supervisor`, `WalWriter`,
  `FsyncBatcher`, `Snapshot`, `Retention`, `Restore`, `TimeTravel`,
  `TestHelpers`.
- **On-disk layout**: `app_data/state/{relay_class}/{shard_id}/{partition_key}.{wal,snap.{seq},outbox.dedup}` (flat — `executor_id` was dropped by `auto-restore-on-boot`; `Flezha.Boot.StatePathMigrator` handles upgrade migration).
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
- **Detailed notes**: `docs/controller_state_persistence.md`.

## Auto-restore on boot (`auto-restore-on-boot`)

- **ProcessHub pinning**: `mix.exs` pins `process_hub` to the GitHub
  dev branch — needed for the `durable-registry` and
  `coordinator-bootstrap-recovery` capabilities.
- **Modules**: `lib/flezha/boot/` — `StatePathMigrator`, `RecoveryHooks`,
  `WalTransfer`. The `StatePathMigrator` runs synchronously from
  `Flezha.Application.start/2` before `Flezha.State.Supervisor`.
- **HubManager wiring**: `Flezha.Service.HubManager.process_hub/1`
  passes `registry_backend: {:dets, [path: ...]}` and
  `auto_recovery: [recovery_window_ms, replay_timeout_ms]` to every
  `ProcessHub.child_spec/1`. After each `start_hub/1` it calls
  `Flezha.Boot.RecoveryHooks.register/1` to install
  `pre_recovery_replay` (waits for FleetManager) and
  `post_recovery_replay` (telemetry).
- **Boot CLI**: `flz boot status`, `flz boot await`,
  `flz boot import-legacy-snapshot` — implemented in `cli/src/boot.rs`
  / `Flezha.Service.CliManager.boot_*`.
- **Persistence layers**: ProcessHub durable registry at
  `app_data/process_hub/<hub_id>/registry.dets` (cspec + `opts[:plugins]`)
  + per-Partition WAL at `app_data/state/<relay_class>/<shard_id>/...`
  (change-by-change progress).
- **Plugin enable/disable** uses cspec-write-first ordering via
  `ProcessRegistry.update/3`; `Flezha.Service.HubManager.update_relay_plugins/3`
  was deleted (the persistent registry covers durability).
- **Removed**: `Flezha.Cluster.Snapshot` + `flz cluster snapshot/restore`,
  the `:overwatch_state_snapshot_interval_ms` periodic capture,
  `Builder.opts[:initial_state_binary]`.
- **Detailed notes**: `docs/auto_restore_on_boot.md`.
