# Application env keys

Read via `Flezha.Config.get/1` by row name; `Read in` names the module that
reads the key. Most keys are filled for you at boot from the
[controller environment variables](environment_variables.md).
`—` means the key is always set and the read site carries no default of its own;
keyword-list keys are shown as `:parent[:child]`.

**No read site carries a default.** Every default below is the row's, declared
once in `lib/flezha/config/keys.ex`, and the millisecond rows live beside it in
`lib/flezha/config/duration_rows.ex` with the derivations that keep one
quantity's multiple of another from drifting; a site reads
`Flezha.Config.Durations.get/1` for those. A retry ladder is the same story one level up: every wait before a
retry is a row of `lib/flezha/config/backoff.ex`, announced on
`[:flezha, :backoff, :wait]`.

## Transport and TLS

| Key | Default | Read in |
|---|---|---|
| `:transport` | `:uds` | `lib/flezha/service/port_pilot.ex` |
| `:transports` | `[:uds]` | `lib/flezha/application.ex`, `lib/flezha/transport/server.ex` |
| `:transport_auth_required` | `true` | `lib/flezha/transport/server.ex` |
| `:controller_enabled` | `true` | `lib/flezha/application.ex` |
| `:quic_listen_address` | `"0.0.0.0:4433"` | `lib/flezha/transport/server.ex`, `lib/flezha/service/port_pilot.ex`; [experimental: quic](../publishing/maturity.md#quic) |
| `:ca_cert_path` | `"app_data/certs/ca.pem"` | `lib/flezha/transport/cert_generator.ex`, `lib/flezha/service/port_pilot.ex` |
| `:server_cert_path` / `:server_key_path` | `"app_data/certs/server.pem"` / `"app_data/certs/server.key"` | `lib/flezha/transport/cert_generator.ex`, `lib/flezha/transport/server.ex` |
| `:client_cert_path` / `:client_key_path` | `"app_data/certs/client.pem"` / `"app_data/certs/client.key"` | `lib/flezha/transport/cert_generator.ex` |
| `:quic_client_ca_path` | `"app_data/certs/ca.pem"` | `lib/flezha/transport/server.ex`; [experimental: quic](../publishing/maturity.md#quic) |
| `:socket_files_path` | — | `lib/flezha/service/port_pilot.ex`, `lib/flezha/transport/socket/support.ex` |
| `:external_processes` | — | `lib/flezha/service/port_pilot.ex` |
| `:concurrent_channel_count` | `0` | `lib/flezha/config.ex` |
| `:send_gate_hold_ms` | `5_000` | `lib/flezha/runner/stream_channel.ex` — how long a sender held on a stream's raised send gate waits for the driver to report the buffer drained. **The gate's own lost-wake fallback**, the counterpart of [`FLEZHA_CREDIT_PARK_INTERVAL_MS`](runner_environment.md): the release arrives on the crossing, and this bounds only a drain report that never came. Running out returns `{:error, :send_buffer_full}` to the caller with the frame still unsent — nothing was discarded on the way, so it is the caller's decision, not a loss |

## Cluster and nodes

| Key | Default | Read in |
|---|---|---|
| `:cluster_discovery` | — | `lib/flezha/cluster/orchestrator.ex` |
| `:initial_nodes` | — | `lib/flezha/cluster/orchestrator.ex` |
| `:static_nodes` | — (set by no config file) | `lib/flezha/cluster/manual_discovery.ex` `validate_settings/1` |
| `:reconnect_interval` / `:max_reconnects` / `:cluster_update_interval` | — | `lib/flezha/cluster/orchestrator.ex` |
| `:reconnect_max_backoff_ms` | `30_000` | `lib/flezha/cluster/orchestrator.ex` |
| `:cluster_formation_timeout_ms` | `15_000` | `lib/flezha/cluster/orchestrator.ex`, `lib/flezha/runner/fleet_manager.ex` |
| `:node_base_name` | — | `lib/flezha/cluster/orchestrator.ex` |
| `:k8s_service_account_path` / `:k8s_cluster_domain` / `:k8s_ip_lookup_mode` | — | `lib/flezha/cluster/k8s_pod_discovery.ex`, `lib/flezha/cluster/orchestrator.ex` |
| `:k8s_cluster_name` / `:k8s_host_identifier` / `:k8s_master_name` / `:k8s_master_port` / `:k8s_namespace` / `:k8s_selector` / `:k8s_service_name` | — | `lib/flezha/cluster/orchestrator.ex` |

## State, WAL and durability

| Key | Default | Read in |
|---|---|---|
| `:state_root` | `FLEZHA_STATE_DIR`, else `"app_data/state"`, expanded to an absolute path on read (derived; set per run in `:test`) | `lib/flezha/state/paths.ex` |
| `:default_compact_after_bytes` | `8 * 1024 * 1024` | `lib/flezha/state/persistence_resolver.ex` |
| `:compact_after_age_ms` | `300_000` | `lib/flezha/state/persistence_resolver.ex` |
| `:compaction_timeout_ms` | `30_000` | `lib/flezha/state/persistence_resolver.ex`; `0` disables the compaction-timeout sweep |
| `:record_log_cut_after_bytes` | `64 * 1024` | `lib/flezha/state/persistence_resolver.ex`; `0` rewrites at every release |
| `:default_time_travel_retention_hours` | `6` | `lib/flezha/state/persistence_resolver.ex` |
| `:default_relay_completed_retention_hours` | `72` | `lib/flezha/state/persistence_resolver.ex` |
| `:default_output_stall_timeout_ms` | `60_000` | `lib/flezha/state/persistence_resolver.ex` |
| `:time_travel_retention_hours` / `:relay_completed_retention_hours` / `:compact_after_bytes` / `:compact_after_age_ms` / `:output_stall_timeout_ms` / `:credit_window_bytes` | `%{}` | `lib/flezha/state/persistence_resolver.ex`: per-class user overrides, `%{"RelayClass" => value}`, winning over the SDK declaration and the instance metadata. Whether a class keeps its work (`durable`) is the class's own and has no override |
| `:wal_high_watermark_bytes` | `16 * 1024 * 1024` | `lib/flezha/state/persistence_resolver.ex` — the one Cell-scale payload bound |
| `:wal_low_watermark_bytes` | half the high watermark (derived) | `lib/flezha/state/wal_writer.ex` |
| `:wal_hard_watermark_bytes` | 4 × the high watermark (derived) | `lib/flezha/state/wal_writer.ex` |
| `:wal_pending_msg_cap` | `5_000` | `lib/flezha/state/wal_writer.ex` |
| `:outbox_inflight_max` | `1_000` | `lib/flezha/state/wal_writer.ex` |
| `:outbox_resettle_ms` | `60_000` | `lib/flezha/state/wal_writer.ex` |
| `:source_credit_window_bytes` | `:wal_high_watermark_bytes` (derived) | `lib/flezha/state/input_log.ex` |
| `:credit_flow_control_enabled` | `true` | `lib/flezha/runner/stream_channel.ex` |
| `:credit_low_watermark_bytes` | half the credit window (derived) | `lib/flezha/runner/stream_channel.ex` |
| `:agent_fsync_interval_ms` | `5` | `lib/flezha/state/fsync_batcher.ex` |
| `:input_log_hold_window_ms` | `0` | `lib/flezha/relay/class_registry.ex`, `lib/flezha/state/input_log.ex` |
| `:time_travel_retention_check_interval_ms` | `60_000` | `lib/flezha/state/retention.ex` |
| `:restore_await_timeout_ms` | `15_000` | `lib/flezha/state/commands.ex` |
| `:state_resync_fun` | unset | `lib/flezha/state/recovery.ex` |
| `:durability_tier[:backend]` | `:none` | `lib/flezha/state/durability_tier.ex`, `lib/flezha/state/durability_tier/backend.ex` |
| `:durability_tier[:ship_interval_ms]` / `[:rpo_target_ms]` / `[:gc_interval_ms]` / `[:gc_grace_ms]` / `[:gc_abandoned_after_ms]` | `30_000` / `120_000` / `3_600_000` / `3_600_000` / `604_800_000` | `lib/flezha/state/durability_tier.ex` (a non-positive value falls back to the default) |
| `:durability_tier[:backend_opts]` | see the `FLEZHA_DURABILITY_PG_*` / `_S3_*` / `_PATH` rows | `lib/flezha/state/durability_tier/backend.ex` |

## Runner and relay lifecycle

| Key | Default | Read in |
|---|---|---|
| `:runner_keepalive` | `"30000"` (string or integer accepted) | `lib/flezha/runner/fleet_manager.ex` |
| `:auto_restore_enabled` | `true` | `lib/flezha/boot/recovery_hooks.ex`, `lib/flezha/service/hub_manager.ex` |
| `:boot_replay_timeout_ms` | `60_000` | `lib/flezha/runner/fleet_manager.ex` |
| `:process_hub_registry_root` | `"app_data/process_hub"` | `lib/flezha/service/hub_manager.ex`, `lib/flezha/state/durability_tier/artifact.ex` |
| `:process_hub_reconcile_grace_ms` / `:process_hub_reconcile_interval_ms` | `45_000` / `30_000` | `lib/flezha/service/hub_manager.ex` |
| `:hub_runner_assign_wait_ms` | `2_000` | `lib/flezha/service/hub_manager.ex` |
| `:migration_consent[:consent_timeout_ms]` / `[:retry_interval_ms]` / `[:max_defer_ms]` | `5_000` / `2_000` / `:graceful_shutdown_timeout_ms` (derived) | `lib/flezha/service/hub_manager.ex` |
| `:overwatch_call_timeout_ms` | `5_000` | `lib/flezha/service/relay_manager.ex` |
| `:call_pending_max` | `10_000` | `lib/flezha/relay/calls.ex`; the answers one relay owes the callers waiting on it |
| `:call_grace_ms` | `1_000` | `lib/flezha/relay/calls.ex`; how long an outside caller waits past its call's deadline, so the relay's own `call.timeout` reaches it first |
| `:call_retention_margin_ms` | `300_000` | `lib/flezha/state/input_log.ex`, `lib/flezha/relay/ingest.ex`; see `FLEZHA_CALL_RETENTION_MARGIN_MS` |
| `:call_worker_idle_ms` | `30_000` | `lib/flezha/service/calls.ex`; how long a relay call's delivery worker (one per caller and target) waits with an empty queue before it stops |
| `:listener_release_check_ms` | `5_000` | `lib/flezha/service/listener/receiver.ex`; how long a listener waits after its plugin was released before it asks the registry whether the relay moved, ended or had the listener disabled, and between asks |
| `:overwatch_ping_enabled` | `true` | `lib/flezha/relay/lifecycle.ex` |
| `:overwatch_ping_timeout_ms` | `2_000` | `lib/flezha/runner/control_unit.ex` |
| `:overwatch_task_warn_ms` | `2_000` | `lib/flezha/relay/overwatch.ex` |
| `:overwatch_runner_wait_ms` | `30_000` | `lib/flezha/relay/lifecycle.ex` |
| `:stream_bind_wait_ms` | `30_000` | `lib/flezha/relay/lifecycle.ex` |
| `:correction_debounce_ms` | `200` | `lib/flezha/relay/replay.ex` |
| `:bind_restore_concurrency` | `16` | `lib/flezha/relay/replay.ex` — how many of a class's Cells a bind restores at once |
| `:placement_load_cap_percent` | `100` | `lib/flezha/runner/fleet_manager/load.ex` — the fairness cap on runner selection, as a percentage of the fair share |
| `:placement_load_refresh_ms` | `1_000` | `lib/flezha/runner/fleet_manager.ex` — how long the per-runner relay count a placement reads may go without being rebuilt from the hub registry |
| `:control_unit_heartbeat_interval_ms` | `15_000` | `lib/flezha/runner/control_unit.ex` |
| `:control_unit_pending_reply_timeout_ms` | `90_000` | `lib/flezha/runner/control_unit.ex` |

## Resilience and convergence

| Key | Default | Read in |
|---|---|---|
| `:control_liveness_window_ms` / `:control_liveness_interval_ms` | `45_000` / `15_000` | `lib/flezha/runner/control_channel.ex` |
| `:io_stream_liveness_interval_ms` / `:io_stream_liveness_threshold_ms` | `10_000` / `30_000` | `lib/flezha/runner/stream_channel.ex` |
| `:graceful_shutdown_enabled` | `true` | `lib/flezha/application.ex`, `lib/flezha/shutdown/coordinator.ex`, `lib/flezha/shutdown/signal_handler.ex` |
| `:graceful_shutdown_timeout_ms` | `60_000` | `lib/flezha/shutdown/coordinator.ex`, `lib/flezha/cli/controller_service.ex` |
| `:convergence_health_enabled` | `true` | `lib/flezha/application.ex` |
| `:convergence_tick_ms` | `5_000` | `lib/flezha/convergence/loop.ex`, `lib/flezha/healthz/server.ex` |
| `:convergence_output_stall_grace_ms` | `60_000` | `lib/flezha/convergence/loop.ex` |
| `:convergence_unmet_intent_grace_ms` | `120_000` | `lib/flezha/convergence/snapshot.ex`: how long a `failed` or `unbound` relay reads `recovering` on `/healthz`, in `flz relay list` and `info`, in `flz cluster status` and in the admin `relay_rows` verb before any of them shows its own word |
| `:convergence_fabric_503_polls` | `3` | `lib/flezha/healthz/server.ex` |
| `:convergence_cell_stall_ms` | `300_000` (the classifier's own default, `lib/flezha/convergence/stall_classifier.ex`) | `lib/flezha/convergence/loop/liveness.ex` — how long a handout may be out before the per-relay classifier calls that Cell stalled; a non-positive value leaves the default in place |
| `:healthz_enabled` | `false` | `lib/flezha/application.ex` |
| `:healthz_port` / `:healthz_bind` | `9569` / `"0.0.0.0"` (`Flezha.Healthz.Server.default_bind/0`, the one default; `config/test.exs` binds loopback) | `lib/flezha/healthz/server.ex` |
| `:output_watchdog_interval_ms` | `10_000` | `lib/flezha/state/watchdog.ex` |
| `:input_silence_timeout_ms` | `0` (off) | `lib/flezha/state/watchdog.ex`: how long a flow relay that produced output earlier may receive no Change before one warning is logged and `[:flezha, :state, :input_silence]` is emitted. Off by default: silence is not a fault, and the sweep cannot tell a dead upstream from a source that reports hourly. Set a positive value on a feed you know to be continuous; `FLEZHA_INPUT_SILENCE_TIMEOUT_MS` sets it from the environment |

## Observability and audit

| Key | Default | Read in |
|---|---|---|
| `:observability[:enabled]` | `true` | `lib/flezha/observability.ex`, `lib/flezha/observability/supervisor.ex` |
| `:observability[:ring_buffer_retention_ms]` / `[:ring_buffer_max_entries_per_source]` | `900_000` / `10_000` | `lib/flezha/observability/ring_buffer_sink.ex` |
| `:observability[:overflow_policy]` | `:drop` | `lib/flezha/observability/registry.ex` |
| `:observability[:cardinality_budget_per_source]` | `1_000` | `lib/flezha/observability/registry.ex` |
| `:observability[:pubsub_sink_enabled]` | `true` (`false` in test) | `lib/flezha/observability/default_sinks.ex`, `lib/flezha/observability/service.ex` |
| `:observability[:data_plane_liveness]` | `true` | `lib/flezha/transport/liveness_counters.ex` |
| `:observability[:otp_backend_enabled]` | `true` (`false` in test) | `lib/flezha/observability/otp_backend/supervisor.ex` |
| `:observability[:otp_backend][:poll_intervals]` | `beam_vm: 1_000, host: 5_000, processhub: 1_000, flezha_transport: 1_000, flezha_bounded_mailbox: 1_000` | `lib/flezha/observability/otp_backend/poller.ex` |
| `:observability_export[:prometheus_enabled]` / `[:prometheus_port]` / `[:prometheus_bind]` | `false` / `9568` / `"127.0.0.1"` | `lib/flezha/observability/export/supervisor.ex`, `lib/flezha/observability/export/prometheus_http.ex` |
| `:observability_export[:otlp_enabled]` / `[:otlp_endpoint]` / `[:otlp_headers]` | `false` / `nil` / `%{}` | `lib/flezha/observability/export/supervisor.ex`, `lib/flezha/observability/export/otlp_client.ex` |
| `:observability_export[:otlp_flush_interval_ms]` / `[:otlp_batch_max]` | `10_000` / `512` | `lib/flezha/observability/export/otlp_sink.ex` |
| `:audit_log[:enabled]` / `[:dir]` | `false` / `"app_data/audit"` | `lib/flezha/observability/audit_log/supervisor.ex`, `lib/flezha/observability/audit_log/writer.ex` |
| `:audit_log[:max_file_bytes]` / `[:max_file_age_seconds]` / `[:rotation_check_interval_ms]` | `67_108_864` / `86_400` / `60_000` | `lib/flezha/observability/audit_log/rotation.ex` |
| `:audit_log[:retention_seconds]` / `[:max_total_bytes]` / `[:retention_check_interval_ms]` | `30 * 86_400` / `4_294_967_296` / `3_600_000` | `lib/flezha/observability/audit_log/retention.ex` |
| `:audit_log[:fsync_mode]` / `[:fsync_interval_ms]` | `:per_record` / `100` | `lib/flezha/observability/audit_log/writer.ex` |
| `:audit_log[:extra_subscriptions]` | `[]` | `lib/flezha/observability/audit_log/sink.ex` |

## Security

| Key | Default | Read in |
|---|---|---|
| `:authn_authz[:enabled]` | `false` (`runtime.exs`: `true` in prod) | `lib/flezha/authz/config.ex`, `lib/flezha/application.ex` |
| `:authn_authz[:application_managed]` | `true` (`false` in test) | `lib/flezha/application.ex` — `false` starts no `Flezha.AuthZ.Supervisor` in the application tree |
| `:authn_authz[:auth_dir]` | `"app_data/auth"` | `lib/flezha/authz/config.ex` |
| `:authn_authz[:bootstrap_token_path]` | `"app_data/auth/bootstrap.token"` | `lib/flezha/authz/config.ex` |
| `:authn_authz[:token_ttl_default_seconds]` | `86_400` | `lib/flezha/authz/config.ex` — every credential with an expiry is one a human holds; a runner credential is refused an expiry outright |
| `:authn_authz[:token_expiry_warning_ms]` / `[:token_expiry_check_interval_ms]` | `86_400_000` / `300_000` | `lib/flezha/authz/token_store.ex` — the warning window and how often the registry looks |
| `:authn_authz[:namespaces]` | `%{}` | `lib/flezha/authz/config.ex` — per-namespace quota overrides |

## Plugins, DLQ and schedules

| Key | Default | Read in |
|---|---|---|
| `:enabled_plugins` | the provider map in `config/config.exs` | `lib/flezha/config.ex` |
| `:plugins[:plugin_root]` | `"app_data/plugins"` | `lib/flezha/plugin/installer/loader.ex`, `lib/flezha/plugin/installer.ex` |
| `:plugins[:trusted_keys]` / `[:require_signature]` / `[:max_package_bytes]` | `[]` / `false` / `50_000_000` | `lib/flezha/plugin/installer/package.ex` |
| `:plugins[:list]` | unset | `lib/flezha/plugin/installer.ex` |
| `:file_plugin_root` | `"app_data/files"` | `lib/flezha/plugin/provider/file.ex`, which resolves every file plugin path inside it and creates only the default folder |
| `:dlq[:dir]` | `"app_data/dlq"` | `lib/flezha/dlq/store.ex` |
| `:dlq[:retention_seconds]` / `[:max_total_bytes]` / `[:retention_check_interval_ms]` | `30 * 86_400` / `4_294_967_296` / `3_600_000` | `lib/flezha/dlq/store.ex` |
| `:dlq[:replay_grace_ms]` | `5_000` | `lib/flezha/dlq/dlq.ex` — how long `flz dlq replay` waits for a re-park |
| `:schedules[:dir]` / `[:misfire_grace_ms]` | `"app_data/schedules"` / `120_000` | `lib/flezha/schedule/store.ex`, `lib/flezha/schedule/job.ex` |
| `:schedules[:application_managed]` | `true` (`false` in test) | `lib/flezha/application.ex` |

## Other

| Key | Default | Read in |
|---|---|---|
| `:env` | `Mix.env()` (`config/config.exs`) | `lib/flezha/relay/handover.ex`, `lib/flezha/runner/control_unit.ex` |
| `:pre_hooks` / `:post_hooks` | `[]` | `lib/flezha/boot/loader.ex` |
| `:testnet_enabled` | `true` in dev/test, `false` in prod (`config/<env>.exs`) | `lib/flezha/application.ex` |
