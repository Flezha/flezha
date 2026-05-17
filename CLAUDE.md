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
- **Start-fresh workflow**: `flz controller stop && flz reset --yes`
  wipes durable runtime data (`app_data/state/*`, `app_data/process_hub/*`).
  Implemented in `cli/src/reset.rs`; refuses to run while the Controller
  is up. See `docs/operations/reset.md`.
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

## Graceful shutdown (`controller-graceful-shutdown`)

- **Modules**: `lib/flezha/shutdown/` — `Coordinator`, `SignalHandler`.
  Coordinator drives `quiesce → drain → finalise`; SignalHandler
  installs OS handlers via `:os.set_signal/2` so SIGTERM/SIGINT
  trigger graceful shutdown in every BEAM run mode.
- **Supervision tree**: SignalHandler is the **first** child in
  `Flezha.Application.start/2` (handlers installed before any
  service can observe a delivered signal); Coordinator is inserted
  after `Flezha.Service.HubManager` (drain queries the hub).
  `prep_stop/1` calls `Coordinator.shutdown(:prep_stop)`.
- **Hot path gating**: `Coordinator.shutting_down?/0` is an ETS-backed
  flag (no GenServer hop). Read by `RelayManager.start_relay/2` and
  `FleetManager.connect_handler/4`.
- **Runtime config knobs**: `:graceful_shutdown_enabled` (default
  `true`, env `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED`),
  `:graceful_shutdown_timeout_ms` (default `60_000`, env
  `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`). **Pairing rule**: orchestrator
  grace period (`terminationGracePeriodSeconds` /
  `stop_grace_period`) MUST be `(timeout_ms / 1000) + 15s`. Bump in
  lockstep.
- **CLI**: `flz controller stop` (drives the coordinator over the
  CLI socket — works in release/mix/iex), `flz controller status`
  (surfaces `shutdown_in_progress`, `shutdown_phase`,
  `signal_handler_status`). Implemented in `cli/src/controller.rs` /
  `Flezha.Service.CliManager.controller_{stop,status}`.
- **Telemetry**: `[:flezha, :shutdown, :requested|:phase_started|:phase_completed|:drain_progress|:timeout|:complete]`.
- **Test config**: `config/test.exs` sets
  `:graceful_shutdown_enabled` to `false` so `Application.stop/1`
  teardown remains fast.
- **Detailed notes**: `docs/operations/graceful_shutdown.md`.

## Observability foundation (`platform-observability-foundation`)

- **Modules**: `lib/flezha/observability/` — `Signal`, `Source`,
  `Sink` (behaviours), `Registry`, `Bus`, `RingBufferSink`,
  `PubSubSink`, `LoggerHandler`, `TelemetryBridge`, `SinkSupervisor`,
  `SourceSupervisor`, `Supervisor`, `Counters`, `DefaultSinks`. The
  service surface lives at `lib/flezha/service/observability.ex`.
- **Supervision tree**: `Flezha.Observability.Supervisor` is inserted
  in `Flezha.Application.start/2` AFTER `Flezha.Service.HubManager`
  and BEFORE `Flezha.Shutdown.Coordinator`. Children, in start
  order: `Registry` → `Phoenix.PubSub` (server name
  `Flezha.Observability.PubSub`) → `SinkSupervisor` → `SourceSupervisor`
  → `LoggerHandler` → `TelemetryBridge` → `DefaultSinks` (one-shot
  worker that registers `:ring_buffer` and `:pubsub` sinks).
- **Signal envelope**: one struct, four kinds — `:metric`, `:event`,
  `:log`, `:trace_span`. Constructor `Flezha.Observability.Signal.new/1`
  validates kind / atoms / map shapes and auto-fills monotonic `ts`.
- **Bus**: `Flezha.Observability.Bus.emit/1` runs in the caller, never
  blocks under default `:drop` overflow policy, checks per-source
  cardinality on `:metric` signals only, and wraps the dispatch body
  in `try/rescue/catch` so a broken sink cannot crash a producer.
- **Default sinks**: `RingBufferSink` (per-source ETS `:ordered_set`
  with TTL'd retention; queries run in the caller process) and
  `PubSubSink` (per-source topic `flezha.observability.source.<src>`,
  disabled by default in test config).
- **Bridges**: `LoggerHandler` installs an OTP `:logger` handler that
  routes log records into the bus as `:log` signals from the `:logger`
  source. `TelemetryBridge` attaches a single `:telemetry.attach_many/4`
  handler that lifts the known `[:flezha, ...]` events onto the bus
  without modifying any producer call-site.
- **Service API**: `Flezha.Service.Observability` exposes `subscribe/2`,
  `query/2`, `list_sources/1`, `list_sinks/1`, `enable_source/2`,
  `disable_source/2`. Every call takes a `caller_context` (`:source`
  in `[:cli, :ui, :sdk, :internal]`, optional `:principal`, optional
  `:request_id`). Mutating calls emit audit `:event` signals on
  `:observability_service` — the seam `platform-audit-log` will
  consume. Thin CLI wrappers live in `Flezha.Service.CliManager`
  (`observability_list_sources/1`, `_enable_source/2`, etc.).
- **Runtime config knobs**: `:observability` map — `:enabled` (default
  `true`, env `FLEZHA_OBSERVABILITY_ENABLED`), `:ring_buffer_retention_ms`
  (`900_000`), `:ring_buffer_max_entries_per_source` (`10_000`),
  `:overflow_policy` (`:drop`), `:cardinality_budget_per_source`
  (`1_000`), `:pubsub_sink_enabled` (`true` in prod, `false` in test).
- **Counters**: `Flezha.Observability.Counters` holds atomic
  `:dispatcher_failure_count`, `:logger_bridge_failures`,
  `:telemetry_bridge_failures` plus a public ETS table used to
  de-dup "log once per unique error class".
- **Bench**: `mix flezha.bench.observability [iterations]` boots
  just the observability supervisor (no Controller / executors) and
  reports nanoseconds per emit against zero sinks.
- **Detailed notes**: `docs/observability_foundation.md`.
