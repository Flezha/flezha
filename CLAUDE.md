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
- **Detailed notes**: `docs/resilience/state_persistence.md`.

## Auto-restore on boot (`auto-restore-on-boot` + `consume-recovery-mode-and-self-heal`)

- **ProcessHub pinning**: `mix.exs` pins `process_hub` to the GitHub
  dev branch — needed for the `durable-registry`,
  `coordinator-bootstrap-recovery`, and `recovery-mode-opt-in`
  capabilities.
- **Modules**: `lib/flezha/boot/` — `StatePathMigrator`, `RecoveryHooks`.
  The `StatePathMigrator` runs synchronously from
  `Flezha.Application.start/2` before `Flezha.State.Supervisor`.
  `Flezha.Boot.WalTransfer` was deleted by
  `consume-recovery-mode-and-self-heal` (cross-controller WAL transfer
  is out of scope — cross-node WAL replication is a future-work non-goal).
- **HubManager wiring**: `Flezha.Service.HubManager.process_hub/1`
  passes `registry_backend: {:dets, [path: ...]}`, `auto_recovery:
  [recovery_window_ms, replay_timeout_ms, recovery_timeout_ms]`, and
  `recovery_marker: %{enabled?: true, path: marker_path(hub_id)}` to
  every `ProcessHub.child_spec/1`. After each `start_hub/1` it calls
  `Flezha.Boot.RecoveryHooks.register/1` to install
  `pre_recovery_replay` (thin telemetry forwarder; the upstream
  marker gate queues cluster events for us) and `post_recovery_replay`
  (telemetry). The marker file defaults to
  `<process_hub_registry_root>/<hub_id>/cluster.healthy` on the same
  hostPath as the DETS file so the two never drift.
- **Recovery tracker**:
  `Flezha.Service.HubManager.RecoveryTracker` is a per-supervisor
  GenServer subscribed to `[:process_hub, :recovery, _]` telemetry.
  `HubManager.recovery_state/1` and `await_recovery_complete/1,2`
  expose the per-hub state machine; `Flezha.BootLoader.await_namespace_hubs/0`
  and `Flezha.Executor.FleetManager.init/1` gate registry reads on it.
- **Boot CLI**: `flz boot status`, `flz boot await`,
  `flz boot import-legacy-snapshot` — implemented in `cli/src/boot.rs`
  / `Flezha.Service.CliManager.boot_*`. Cluster prepare-recovery:
  `flz cluster prepare-recovery` (Rust `cli/src/cluster.rs` →
  `Flezha.Service.CliManager.cluster_prepare_recovery/1` →
  `ProcessHub.prepare_recovery_cluster/1`); `flz cluster status`
  now surfaces `recovery_state` and `recovery_armed` per node.
- **Start-fresh workflow**: `flz controller stop && flz reset --yes`
  wipes durable runtime data (`app_data/state/*`, `app_data/process_hub/*`).
  Implemented in `cli/src/reset.rs`; refuses to run while the Controller
  is up. See `docs/operations/reset.md`.
- **Persistence layers**: ProcessHub durable registry at
  `app_data/process_hub/<hub_id>/registry.dets` (cspec + `opts[:plugins]`)
  + per-Partition WAL at `app_data/state/<relay_class>/<shard_id>/...`
  (change-by-change progress) + marker file at
  `app_data/process_hub/<hub_id>/cluster.healthy` (operator gate).
- **Plugin enable/disable** uses cspec-write-first ordering via
  `ProcessRegistry.update/3`; `Flezha.Service.HubManager.update_relay_plugins/3`
  was deleted (the persistent registry covers durability).
- **Runtime config knobs**: `FLEZHA_PROCESS_HUB_RECOVERY_MARKER_PATH`,
  `FLEZHA_PROCESS_HUB_RECOVERY_TIMEOUT_MS` (default 30 s),
  `FLEZHA_AUTO_RESTORE_ENABLED` (default `true`; `false` kills the
  marker gate entirely and reverts to in-memory `:ets`).
- **Removed by `consume-recovery-mode-and-self-heal`**:
  `Flezha.Boot.WalTransfer`; `:pre_replay_fleet_wait_ms` config key;
  `Flezha.Reconciler.{Loop, Actions, Health, Probe, Activity, Leader}`
  (replaced by `Flezha.Convergence.Health`); `Flezha.State.Outbox`;
  `Flezha.Reconciler.Supervisor` (top-level); `FLEZHA_RECONCILER_ENABLED`,
  `FLEZHA_LEADERSHIP_ENABLED`, `FLEZHA_RECONCILE_*` env vars.
- **Detailed notes**: `docs/resilience/auto_restore_on_boot.md`.

## Convergence Health watcher (`consume-recovery-mode-and-self-heal`)

- **Modules**: `lib/flezha/convergence/health.ex` — the single
  controller-side GenServer driving bind/rebind/plugin-restart
  decisions. `lib/flezha/healthz/server.ex` — opt-in `:gen_tcp`
  HTTP/1.1 `/healthz` listener following the
  `Flezha.Observability.Export.PrometheusHttp` pattern.
- **Supervision tree**: `Flezha.Convergence.Health` is inserted in
  `Flezha.Application.start/2` AFTER `Flezha.State.Watchdog`;
  `Flezha.Healthz.Server` is inserted right after, gated on
  `:healthz_enabled` (default `false` — flezha_market opts in via
  its ConfigMap).
- **Behaviour**: one tick per `convergence_tick_ms` (default 5_000).
  On init: `HubManager.await_recovery_complete/1` blocks emissions
  until every per-namespace hub reaches `:normal | :skipped`. On
  every tick: walk `HubManager.get_all_relays/1` per running
  namespace, classify each relay as `:healthy | :degraded | :failed
  | :unbound`, emit at most one action per relay (`:bind`,
  `:rebind`, `:restart_plugin`, `:noop`).
- **Output-stall → forced rebind**: subscribes to
  `[:flezha, :state, :output_stall]` (emitted by
  `Flezha.State.Watchdog`); on receipt, queues a `:rebind` with
  exponential back-off `5s/30s/60s/300s`. Single signal closes the
  silent-freeze failure class.
- **Plugin escalation**: `handle_plugin_failure/2` tracks strikes
  per relay-plugin and escalates to a relay restart at
  `convergence_plugin_strike_threshold` (default 2). Emits
  `[:flezha, :convergence, :plugin_restart]` and
  `[:flezha, :convergence, :plugin_escalated]` telemetry.
- **/healthz contract** (when `FLEZHA_HEALTHZ_ENABLED=true`):
  `GET /` → `pong\n`; `GET /healthz` → `200 healthy`,
  `200 recovering` (kubelet must not restart during planned
  recovery), or `503 unhealthy` with `reason ∈ tick_stale |
  output_stalled | recovery_failed`. Unknown paths → 404; non-GET
  → 405; malformed → 400. Bind defaults to `127.0.0.1`.
- **Runtime config knobs**: `FLEZHA_USE_CONVERGENCE_HEALTH`
  (default `true`), `FLEZHA_CONVERGENCE_TICK_MS` (default 5_000),
  `FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS` (default 60_000),
  `FLEZHA_CONVERGENCE_PLUGIN_STRIKE_THRESHOLD` (default 2),
  `FLEZHA_HEALTHZ_ENABLED` (default `false`),
  `FLEZHA_HEALTHZ_PORT` (default 9569).
- **flezha_market deployment**: `kubernetes/flezha-daemonset.yaml`
  ships a `livenessProbe: httpGet /healthz:9569` block;
  ConfigMap sets `FLEZHA_HEALTHZ_ENABLED=true` and the marker path.
  `kubernetes/prepare-recovery.sh` wraps
  `flz cluster prepare-recovery` for the operator-driven cold-boot
  workflow.
- **Fresh-process executor takeover**: phase 5 of the change keeps
  the takeover on the same self-healing loop instead of bolting on
  a parallel path. `FleetManager.handle_call(:executor_connect, ...)`
  dispatches `:executor_connected_event` and returns; nothing else.
  `Flezha.Convergence.Health` subscribes to that event via
  `EventManager.add_handler/1` and runs an immediate tick. The
  existing `:rebind_if_executor_available` action body — already
  responsible for migrating relays whose `bound_executor` is no
  longer active — flips the registry tag via
  `HubManager.relay_set_executor/3` and seeds the new executor's
  pre-spawned `StreamChannel.relay_mapping` via
  `HubManager.hook_insert_pid/2` (the same fan-out the
  `child_registered` hook uses), then emits
  `[:flezha, :executor, :reconnected_reassigned]` +
  `[:flezha, :executor, :reconnected]` (mode: `:reassigned`,
  reason: `:convergence_rebind`) per reseated relay. The existing
  back-off (5s/30s/60s/300s) carries the retry semantics — a
  failed Overwatch migrate leaves the registry untouched and the
  relay stays `:degraded` for the next tick.

## Data-plane liveness instrumentation (`data-plane-liveness-instrumentation`)

- **Purpose**: localize a silent `io_in`/output stall to one layer.
  PURE instrumentation — zero data-plane behaviour change, no
  remediation. Closes the 22:14 candle incident's "which layer
  wedged?" gap (BEAM channel vs NIF driver thread vs executor).
- **NIF driver self-report**: `flz_nif/src/driver_stats.rs` —
  `DriverStats{loop_iterations, last_loop_ms, cmd_queue_depth:
  AtomicU64; streams: Mutex<HashMap>}` shared `Arc` into the
  flz-uds-driver loop (`uds_driver.rs` bumps the heartbeat once per
  iteration, rebuilds the per-stream snapshot once per iteration —
  never per frame). `driver_stats/1` NIF (in `server.rs`, registered
  via auto-discovery `init!`) returns the map; `:undefined` for QUIC
  (`TransportServer.stats: Option<Arc<DriverStats>>`).
- **BEAM counters**: `lib/flezha/transport/liveness_counters.ex` —
  always-on `:counters`/`:atomics` + public ETS registry (mirrors
  `Observability.Counters`). `StreamChannel` bumps `io_in_received /
  send_frame_calls / nil_drops / flz_data_received / last_send_ms`;
  `Relay.Overwatch` bumps `io_in_received / forwarded / filtered_nil`.
  Dead-owner entries pruned on `snapshot_all/0`.
- **Metric emission**: `Transport.Server.get_stats/1` merges
  `Native.driver_stats`; `OtpBackend.TransportSource` (the
  `:flezha_transport` source) emits per-transport driver metrics +
  flat global BEAM aggregates (bounded cardinality — classifier does
  per-entity localization).
- **Stall classifier**: `lib/flezha/convergence/stall_classifier.ex`
  — pure `classify(current, prev, opts)` returning one of
  `driver_loop_frozen | nif_queue_backing_up | socket_write_blocked |
  executor_not_consuming | channel_not_sending | relay_not_forwarding
  | :ok` (deepest-wedged-layer-wins; each verdict has an "is it being
  fed?" advancing-delta precondition). `Convergence.Loop` builds the
  snapshot per tick and emits `[:flezha, :data_plane,
  :stall_localized]` (with `%{layer: …}`) on verdict change —
  detection only, feeds NO heal; bridged to the bus via
  `TelemetryBridge`.
- **Runtime config knob**: `:observability, :data_plane_liveness`
  (default `true`, env `FLEZHA_DATA_PLANE_LIVENESS`) gates the ~1 Hz
  poll + classifier; counters stay always-on.
- **Ops fallback** (docs, not built): sample
  `/proc/<beam_pid>/task/*/` for `flz-uds-driver` threads in `D`/frozen
  `utime` (`bin/flzvm pid`).
- **Detailed notes**: `docs/resilience/transport_liveness.md`.

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
- **Detailed notes**: `docs/resilience/graceful_shutdown.md`.

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

## Durable audit log (`platform-audit-log`)

- **Modules**: `lib/flezha/observability/audit_log/` — `Codec`,
  `Index`, `Reader`, `Writer`, `Rotation`, `Retention`, `Sink`,
  `Supervisor`. Service surface at
  `lib/flezha/service/audit_log.ex`.
- **Supervision tree**: `Flezha.Observability.AuditLog.Supervisor`
  is the LAST child of `Flezha.Observability.Supervisor` (gated on
  `:audit_log, :enabled`, default `false`). Children, in start
  order: `Writer` → `Rotation` → `Retention` → `Sink`. The Sink is
  last so subscriptions go live only after the Writer is ready to
  accept records.
- **Default allow-list**: `:observability_service`, `:flezha_shutdown`,
  `:flezha_plugin_installer`, `:audit_log` (audit-of-audit). Extend
  via `:audit_log, :extra_subscriptions`.
- **On-disk layout**: `app_data/audit/INDEX` (binary catalog),
  `app_data/audit/audit-current.jsonl` (active file),
  `app_data/audit/audit-YYYYMMDDTHHMMSSZ-N.jsonl[.gz]` (rotated).
  `INDEX` is rebuildable on boot if missing or corrupt.
- **JSON Lines record**: stable field order `ts_us, ts_iso, source,
  name, measurements, metadata, caller`; `caller` is denormalised
  from `metadata.caller_source / :principal / :request_id`.
- **Fsync**: `:per_record` (default, ~1-10 ms/record on local SSD)
  or `:batched` (every `:fsync_interval_ms`, bounded loss window).
  Overflow policy `:block` so audit emits are never silently
  dropped.
- **Rotation**: size (`:max_file_bytes`, 64 MiB) OR age
  (`:max_file_age_seconds`, 1 day), checked every
  `:rotation_check_interval_ms` (60 s) and on every write.
- **Retention**: age (`:retention_seconds`, 30 d) AND total size
  (`:max_total_bytes`, 4 GiB) — more restrictive wins. Active file
  is never deleted by retention.
- **Service API**: `Flezha.Service.AuditLog` —
  `list_files/1`, `read/2`, `tail/2`, `unsubscribe/2`, `stats/1`,
  `rotate/1`. Every public call emits an audit-of-audit event on
  `:audit_log` source. Master flag check returns
  `{:error, :observability_disabled}`; subsystem flag check returns
  `{:error, :audit_log_disabled}`.
- **CLI**: `flz audit list`, `flz audit read <file>`,
  `flz audit tail`, `flz audit stats`, `flz audit rotate` —
  implemented in `cli/src/audit.rs` and
  `Flezha.Service.CliManager.audit_log_render_*`.
- **Shutdown integration**: `Flezha.Shutdown.Coordinator.do_finalise`
  calls `Writer.close/0` BEFORE the `FsyncBatcher` flush, bounded
  by a 5 s timeout. Post-close signals are dropped with a per-sink
  counter increment.
- **Runtime config knobs**: `:audit_log` map (see
  `docs/audit_log.md` for the full list). Env-var overrides:
  `FLEZHA_AUDIT_ENABLED`, `FLEZHA_AUDIT_DIR`,
  `FLEZHA_AUDIT_RETENTION_DAYS`.
- **Detailed notes**: `docs/audit_log.md`.

## Platform authn/authz foundation (`platform-authn-authz`)

- **Modules**: `lib/flezha/authz/` — `Action`, `Role`, `Principal`,
  `Namespace`, `Scope`, `Policy`, `Config`, `Store`, `AuthState`,
  `RoleStore`, `NamespaceStore`, `TokenStore`, `QuotaCounters`,
  `Quotas`, `Bootstrap`, `Bootstrap.OnBoot`, `SourceRegistration`,
  `Supervisor`; top-level `Flezha.AuthZ` exposes `authorize/3`,
  `require_bound_principal/1`, `enabled?/0`. Cert side lives at
  `lib/flezha/authn/cert.ex` (`principal_from_peer/1` +
  `namespace_from_peer/1`, RFC 4514 normalised DN / SPIFFE URI SAN).
- **Supervision tree**: `Flezha.AuthZ.Supervisor` is inserted in
  `Flezha.Application.start/2` AFTER `Flezha.Observability.Supervisor`
  and BEFORE `Flezha.Shutdown.Coordinator`. Children, in order
  (`:rest_for_one`): `AuthState` → `RoleStore` → `NamespaceStore`
  → `TokenStore` → `QuotaCounters` → `SourceRegistration` (one-shot
  `:platform_authz` source registration) → `Bootstrap.OnBoot`
  (boot-time token writer + refuse-to-start gate). Gated by
  `:authn_authz, :application_managed` (default `true`; tests flip
  to `false` so they can bring up isolated stores against a tmp
  `auth_dir`).
- **On-disk layout**: `app_data/auth/` (mode `0700`) —
  `state.dets` (durable `:enabled` flag),
  `roles.dets` (principal → {namespace, role}),
  `namespaces.dets` (catalog + quotas),
  `tokens.dets` (SHA-256-hashed registry),
  `quota_counters.dets` (per-namespace counters),
  `bootstrap.token` (one-time, mode `0600`, unlinked on claim).
  Persistent-state tree under `app_data/state/_system/...` after the
  one-shot namespace migration in `Flezha.Boot.StatePathMigrator`.
- **Policy point**: `Flezha.AuthZ.authorize(caller_context, action,
  scope) :: :ok | {:error, {:forbidden, reason}}`. Four roles:
  `viewer`, `operator`, `admin`, `executor`. Role→action map is
  compile-time in `Flezha.AuthZ.Policy`. Auth-state mutations
  (`:authz.*`) are additionally gated to `admin` on the bound
  namespace; only `admin@_system` may target other namespaces.
- **Tokens**: wire form `flzt_<base32>` (256-bit); storage form
  SHA-256 hash. Raw value returned to caller exactly once at
  `TokenStore.issue/5`. Revocation is instant
  (`TokenStore.revoke/2` sets `revoked_at`; `verify/1` rejects).
  Mutations emit `:token_issued` / `:token_revoked` on
  `:platform_authz`.
- **Quota engine**: `Flezha.AuthZ.Quotas.check_allocation/3` —
  one ETS lookup per call (NamespaceStore cache + QuotaCounters
  cache). `record_allocation/3` / `record_deallocation/3` apply
  async. `RelayManager.start_relay/2` enforces `max_relays` /
  `max_partitions_per_relay` at allocation time and records on
  success; `stop_relay/2` decrements on stop. Mailbox / disk /
  executor enforcement points land in the namespace-state
  follow-up.
- **Bootstrap**: on first boot when no `admin@_system` binding
  exists AND `:enabled` is `true`, `Bootstrap.OnBoot` writes
  `app_data/auth/bootstrap.token` (mode `0600`) after verifying the
  parent dir's mode (aborts with `error` on group/other-write
  bits). `flz auth bootstrap <token>` (Elixir wrapper:
  `Flezha.Service.CliManager.authz_bootstrap/2`) claims it,
  resolves the principal (cert SAN > cert DN > `--principal` >
  freshly-minted admin token), binds `admin@_system`, flips
  `AuthState.enabled` durably, unlinks the token file, emits
  `:bootstrap_completed`.
- **CLI surface (Elixir-side wrappers ready; Rust verbs deferred)**:
  `Flezha.Service.CliManager.authz_{bootstrap,issue,revoke,list,whoami}`,
  `namespace_{create,delete,list,show,set_quota}`,
  `role_{bind,unbind,list}`. Each wraps in `AuthZ.authorize/3` and
  emits a `:platform_authz` audit event on mutating verbs.
- **Audit log integration**: `:platform_authz` in the default
  subscription allow-list (`Flezha.Observability.AuditLog.Sink`);
  JSONL records gain a top-level `namespace` field (defaults to
  `"_system"` when absent on pre-upgrade rows); `caller.principal`
  is the verified principal when `:enabled` is `true`, else the
  caller-source atom (preserving pre-change behaviour).
- **Compatibility mode**: `:authn_authz, :enabled` defaults `false`
  in `config/runtime.exs` and `config/test.exs`. While `false`,
  `AuthZ.authorize/3` returns `:ok` unconditionally and
  `require_bound_principal/1` is a no-op — every existing service
  caller continues to work without producing a verified principal.
  Flips to `true` durably on `Bootstrap.claim/2` success;
  `FLEZHA_AUTHZ_ENABLED=false` is an explicit opt-out that emits a
  `warn` on every boot.
- **Runtime config knobs**: `:authn_authz` map —
  `:enabled` (default `false`, env `FLEZHA_AUTHZ_ENABLED`),
  `:auth_dir` (default `app_data/auth`, env `FLEZHA_AUTHZ_DIR`),
  `:bootstrap_token_path` (default `<auth_dir>/bootstrap.token`,
  env `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH`),
  `:token_ttl_default_seconds` (`86_400`),
  `:sdk_token_ttl_seconds` (`2_592_000`),
  `:namespaces` (map of per-namespace quota overrides),
  `:application_managed` (default `true`; tests set `false`).
- **Deferred to follow-ups**: transport-layer cert wiring + CLI
  socket handshake + Rust CLI verbs
  (`platform-authn-authz-transport`); namespace prefix through
  State subsystem + ProcessHub registry + remaining service-module
  refactors + remaining quota points
  (`platform-authn-authz-namespace-state`); operator guide and
  multi-tenant integration suite
  (`platform-authn-authz-docs-and-e2e`).
