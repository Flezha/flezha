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

## OTP backend sources (`platform-observability-otp-backend`)

- **Modules**: `lib/flezha/observability/otp_backend/` —
  `Supervisor`, `BeamVmSource`, `HostSource`, `ProcParser`,
  `ProcessHubSource`, `TransportSource`, `BoundedMailboxSource`.
- **Supervision tree**: `Flezha.Observability.OtpBackend.Supervisor`
  is inserted in `Flezha.Observability.Supervisor` AFTER
  `DefaultSinks` and BEFORE `Export.Supervisor`. `:one_for_one` so a
  per-source crash leaves peers running.
- **Shipped sources** — `:beam_vm` (memory / schedulers / GC /
  process+port counts, 1s default), `:host` (Linux `/proc` RSS /
  CPU / FD / loadavg, 5s default; one-shot `:host_source_degraded`
  event on non-Linux), `:processhub` (per-hub registered processes +
  pending recoveries, 1s), `:flezha_transport` (per-transport
  pending-stream count via `Flezha.Transport.Server.get_stats/1`,
  1s), `:flezha_bounded_mailbox` (registered, one-shot
  `:bounded_mailbox_unavailable` event until the Rust→Elixir mailbox
  surface lands).
- **Runtime config knobs**: `:observability, :otp_backend_enabled`
  (default `true` in prod, `false` in test); `:observability,
  :otp_backend, :poll_intervals` map per-source override.
- **CLI**: `flz observe sources`, `flz observe sinks`,
  `flz observe tail <source>`, `flz observe enable <source>`,
  `flz observe disable <source>` — implemented in
  `cli/src/observe.rs` and
  `Flezha.Service.CliManager.observability_render_*`.
- **Detailed notes**: `docs/observability_otp_backend.md`.

## Observability export (`platform-observability-export`)

- **Modules**: `lib/flezha/observability/export/` — `Supervisor`,
  `PrometheusRegistry`, `PrometheusSink`, `PrometheusHttp`,
  `OtlpCodec`, `OtlpClient`, `OtlpSink`.
- **Supervision tree**: `Flezha.Observability.Export.Supervisor` is
  the last child of `Flezha.Observability.Supervisor`, started AFTER
  the OTP backend. Registers the declarative
  `:observability_export` source on init so the exporters'
  operational events participate in bus fan-out.
- **Prometheus**: hand-rolled `:gen_tcp` HTTP/1.1 listener (~250
  LOC, no Cowboy/Plug) at default `127.0.0.1:9568/metrics`. Accepts
  only `GET /metrics` and `GET /`; everything else returns 404 / 405
  / 400. Type inferred from `Signal.measurements` keys (`:count` →
  counter, `:value`/`:gauge` → gauge, `:duration_us`/`:bytes` →
  histogram); `metadata.prometheus_type` overrides. Cardinality
  enforcement aggregates over-budget label values into a synthetic
  `{__truncated__="true"}` series and emits
  `:prometheus_label_truncated` events.
- **OTLP**: OTLP/HTTP via `:mint` (top-level dep, was transitive
  via `:mint_web_socket`). Hand-rolled protobuf encoder in
  `OtlpCodec` matches OpenTelemetry v1 schema for metrics / logs /
  traces. Batched with size + time triggers (`:otlp_batch_max` 512,
  `:otlp_flush_interval_ms` 10s). Overflow policy `:block` —
  producer back-pressure during OTLP outages, bounded by
  `mailbox_watermark × sum-of-backoffs`. Backoff: 250/500/1000/2000/4000
  ms, max 5 attempts.
- **Runtime config knobs**: `:observability_export` map with
  `:prometheus_enabled` (default `false`), `:prometheus_port`
  (`9568`), `:prometheus_bind` (`"127.0.0.1"`), `:otlp_enabled`
  (`false`), `:otlp_endpoint`, `:otlp_headers` (map for bearer
  tokens), `:otlp_flush_interval_ms`, `:otlp_batch_max`. Env-var
  overrides: `FLEZHA_PROMETHEUS_ENABLED`, `FLEZHA_PROMETHEUS_PORT`,
  `FLEZHA_OTLP_ENABLED`, `FLEZHA_OTLP_ENDPOINT`.
- **Registry extension**: `Flezha.Observability.Registry.register_external_sink/1`
  lets exporters register a sink whose pid is owned by a different
  supervisor — required because `Export.Supervisor` owns the
  Prometheus and OTLP sinks, not the foundation `SinkSupervisor`.
- **Detailed notes**: `docs/observability_export.md`.
