# Application env keys

Read via `Flezha.Config.get/1` by row name. Most keys are filled for you at boot
from the [controller environment variables](environment_variables.md). Each
area below says what its keys do where that is more than their name; the type
and default of every key is in [Every key](#every-key), generated from the
configuration table, where a keyword-list key is shown as `:parent[:child]`.

**No read site carries a default.** Every default below is the row's, declared
once in `lib/flezha/config/keys.ex`, and the millisecond rows live beside it in
`lib/flezha/config/duration_rows.ex` with the derivations that keep one
quantity's multiple of another from drifting; a site reads
`Flezha.Config.Durations.get/1` for those. A retry ladder is the same story one level up: every wait before a
retry is a row of `lib/flezha/config/backoff.ex`, announced on
`[:flezha, :backoff, :wait]`.

**The boot refuses a key it cannot read.** A `:flezha` key that is no row's key
and no group's root stops the boot naming it and the nearest row, and a value
its row's type refuses stops it naming the key, the type and the value. Each
setting has one layer: there is no per-class map over a class's own value.

## Transport and TLS

- `:transports`: Derived from `:transport`, so `[:uds]` when nothing is set.
- `:quic_listen_address`: QUIC is [experimental: quic](../publishing/maturity.md#quic).
- `:quic_client_ca_path`: QUIC is [experimental: quic](../publishing/maturity.md#quic).
- `:concurrent_channel_count`: `0` when nothing sets it.
- `:send_gate_hold_ms`: How long a sender held on a stream's raised send gate waits for the driver to report the buffer drained. **The gate's own lost-wake fallback**, the counterpart of [`FLEZHA_CREDIT_PARK_INTERVAL_MS`](runner_environment.md): the release arrives on the crossing, and this bounds only a drain report that never came. Running out returns `{:error, :send_buffer_full}` to the caller with the frame still unsent — nothing was discarded on the way, so it is the caller's decision, not a loss.

## Cluster and nodes

- `:static_nodes`: Set by no config file.

## State, WAL and durability

- `:state_root`: Default: `FLEZHA_STATE_DIR`, else `"storage/app/state"`, expanded to an absolute path on read (derived; set per run in `:test`).
- `:compaction_timeout_ms`: `0` disables the compaction-timeout sweep.
- `:record_log_cut_after_bytes`: `0` rewrites at every release.
- `:time_travel_retention_hours`: The `%{"RelayClass" => hours}` store `flz state retention` writes, read ahead of `:default_time_travel_retention_hours`. Default: `%{}`.
- `:keep_finished_by_default`: Set by `FLEZHA_KEEP_FINISHED_BY_DEFAULT`, explained in [Controller environment variables](environment_variables.md).
- `:source_credit_window_bytes`: The bytes a source's input log admits before its writers wait. Default: the high watermark (derived).
- `:wal_high_watermark_bytes`: The one Cell-scale payload bound.
- `:wal_low_watermark_bytes`: Default: half the high watermark (derived).
- `:wal_hard_watermark_bytes`: Default: 4 × the high watermark (derived).
- `:credit_low_watermark_bytes`: Default: half the credit window, which is the high watermark (derived).
- `:durability_tier[:ship_interval_ms]` / `:durability_tier[:rpo_target_ms]` / `:durability_tier[:gc_interval_ms]` / `:durability_tier[:gc_grace_ms]` / `:durability_tier[:gc_abandoned_after_ms]`: `0` is refused at boot.
- `:durability_tier[:backend_opts]`: Filled from the `FLEZHA_DURABILITY_PG_*`, `FLEZHA_DURABILITY_S3_*` and `FLEZHA_DURABILITY_PATH` variables.

## Runner and relay lifecycle

- `:runner_removal_grace_ms`: How long a disconnected runner is kept before removal, `0` removes at once.
- `:migration_consent[:consent_timeout_ms]` / `:migration_consent[:retry_interval_ms]` / `:migration_consent[:max_defer_ms]`: Default: `5_000` / `2_000` / `:graceful_shutdown_timeout_ms` (derived).
- `:call_pending_max`: The answers one relay owes the callers waiting on it.
- `:call_channel_deadline_ms`: The deadline of a relay call whose caller names none, `flz relay call` without `--timeout` included.
- `:call_grace_ms`: How long an outside caller waits past its call's deadline, so the relay's own `call.timeout` reaches it first.
- `:call_retention_margin_ms`: Set by `FLEZHA_CALL_RETENTION_MARGIN_MS`, explained in [Controller environment variables](environment_variables.md).
- `:call_worker_idle_ms`: How long a relay call's delivery worker (one per caller and target) waits with an empty queue before it stops.
- `:listener_release_check_ms`: How long a listener waits after its plugin was released before it asks the registry whether the relay moved, ended or had the listener disabled, and between asks.
- `:bind_restore_concurrency`: How many of a class's Cells a bind restores at once.
- `:placement_load_cap_percent`: The fairness cap on runner selection, as a percentage of the fair share.
- `:stream_channel_max_heap_bytes`: The memory one runner data channel may hold before the runtime kills it; see `FLEZHA_STREAM_CHANNEL_MAX_HEAP_BYTES`.
- `:placement_load_refresh_ms`: How long the per-runner relay count a placement reads may go without being rebuilt from the hub registry.

## Resilience and convergence

- `:control_liveness_window_ms` / `:control_liveness_interval_ms`: Default: `45_000` / `15_000`.
- `:io_stream_liveness_interval_ms` / `:io_stream_liveness_threshold_ms`: Default: `10_000` / `30_000`.
- `:convergence_unmet_intent_grace_ms`: How long a `failed` or `unbound` relay reads `recovering` on `/healthz`, in `flz relay list` and `info`, in `flz cluster status` and in the admin `relay_rows` verb before any of them shows its own word, how long saved state may outlive its relay's intent before `/healthz` names it in `intent_mismatch`, and how long a declared relay may run nowhere before the convergence loop starts it.
- `:convergence_cell_stall_ms`: How long a handout may be out before the per-relay classifier calls that Cell stalled; `0` is refused at boot. Default: `300_000` (the classifier's own default, `lib/flezha/convergence/stall_classifier.ex`).
- `:healthz_port` / `:healthz_bind`: Default: `9569` / `"0.0.0.0"` (`Flezha.Healthz.Server.default_bind/0`, the one default; `config/test.exs` binds loopback).
- `:input_silence_timeout_ms`: How long a flow relay that produced output earlier may receive no Change before one warning is logged and `[:flezha, :state, :input_silence]` is emitted. Off by default: silence is not a fault, and the sweep cannot tell a dead upstream from a source that reports hourly. Set a positive value on a feed you know to be continuous; `FLEZHA_INPUT_SILENCE_TIMEOUT_MS` sets it from the environment. Default: `0` (off).

## Observability, audit and relay history

- `:observability[:pubsub_sink_enabled]`: Default: `true` (`false` in test).
- `:observability[:otp_backend_enabled]`: Default: `true` (`false` in test).
- `:audit_log[:fsync_mode]`: `:per_record` syncs before each record is acknowledged, one sync serving every record queued with it; `:interval` (spelled `:batched` too) syncs every `:fsync_interval_ms`.
- `:relay_history[:max_pending]`: Records queued past this are refused and counted in `flz logs stats relay_history`, never waited for; `:append_ms` is how long a start or stop waits for its record before counting it a timeout.
- `:relay_history[:dedup_window_hours]`, `:relay_history[:dedup_max_entries]`: How far back, and for how many runs, each controller keeps the in-memory index that answers whether a run already happened.

## Security

- `:authn_authz[:enabled]`: Default: `false` (`runtime.exs`: `true` in prod).
- `:authn_authz[:application_managed]`: `false` starts no `Flezha.AuthZ.Supervisor` in the application tree. Default: `true` (`false` in test).
- `:authn_authz[:bootstrap_token_path]`: Default: `"storage/app/auth/bootstrap.token"`.
- `:authn_authz[:token_ttl_default_seconds]`: Every credential with an expiry is one a human holds; a runner credential is refused an expiry outright.
- `:authn_authz[:token_expiry_warning_ms]` / `:authn_authz[:token_expiry_check_interval_ms]`: The warning window and how often the registry looks.
- `:authn_authz[:namespaces]`: Per-namespace quota overrides.

## Plugins, DLQ and schedules

- `:file_plugin_root`: Every file plugin path resolves inside it; the controller creates only the default folder.
- `:dlq[:replay_grace_ms]`: How long `flz dlq replay` waits for a re-park.
- `:schedules[:application_managed]`: Default: `true` (`false` in test).

## Other

- `:env`: Default: `Mix.env()` (`config/config.exs`).
- `:testnet_enabled`: Default: `true` in dev/test, `false` in prod (`config/<env>.exs`).

## Every key

<!-- config-keys:begin application_env -->
| Key | Type | Default |
|---|---|---|
| `:agent_fsync_interval_ms` | ms | `5` |
| `:audit_log[:dir]` | path | `storage/app/audit` |
| `:audit_log[:enabled]` | bool | `false` |
| `:audit_log[:extra_subscriptions]` | term | `[]` |
| `:audit_log[:fsync_interval_ms]` | ms | `100` |
| `:audit_log[:fsync_mode]` | one of `per_record`, `interval`, `batched` | `per_record` |
| `:audit_log[:max_file_age_seconds]` | int | `86400` |
| `:audit_log[:max_file_bytes]` | int | `67108864` |
| `:audit_log[:max_total_bytes]` | int | `4294967296` |
| `:audit_log[:retention_check_interval_ms]` | ms | `3600000` |
| `:audit_log[:retention_seconds]` | seconds | `2592000` |
| `:audit_log[:rotation_check_interval_ms]` | ms | `60000` |
| `:authn_authz[:application_managed]` | bool | `true` |
| `:authn_authz[:auth_dir]` | path | `storage/app/auth` |
| `:authn_authz[:bootstrap_token_path]` | path | derived |
| `:authn_authz[:enabled]` | bool | `false` |
| `:authn_authz[:namespaces]` | term | `%{}` |
| `:authn_authz[:token_expiry_check_interval_ms]` | ms | `300000` |
| `:authn_authz[:token_expiry_warning_ms]` | ms | `86400000` |
| `:authn_authz[:token_ttl_default_seconds]` | int | `86400` |
| `:auto_restore_enabled` | bool | `true` |
| `:bench_durability_owned` | bool | `false` |
| `:bench_skip_build` | bool | `false` |
| `:bind_restore_concurrency` | positive_int | `16` |
| `:boot_replay_timeout_ms` | ms | `60000` |
| `:ca_cert_path` | path | `storage/app/certs/ca.pem` |
| `:call_channel_deadline_ms` | ms | `30000` |
| `:call_grace_ms` | ms | `1000` |
| `:call_pending_max` | positive_int | `10000` |
| `:call_retention_margin_ms` | ms | `300000` |
| `:call_worker_idle_ms` | ms | `30000` |
| `:client_cert_path` | path | `storage/app/certs/client.pem` |
| `:client_key_path` | path | `storage/app/certs/client.key` |
| `:cluster_discovery` | string | `manual_discovery` |
| `:cluster_formation_timeout_ms` | ms | `15000` |
| `:cluster_update_interval` | ms | `5000` |
| `:commit_stall_fn` | term | unset |
| `:compact_after_age_ms` | ms | `300000` |
| `:compaction_timeout_ms` | ms | `30000` |
| `:concurrent_channel_count` | positive_int | derived at boot |
| `:control_liveness_interval_ms` | ms | `15000` |
| `:control_liveness_window_ms` | ms | derived |
| `:control_unit_pending_reply_timeout_ms` | ms | `90000` |
| `:control_unit_reply_sweep_interval_ms` | ms | `15000` |
| `:controller_enabled` | bool | `true` |
| `:convergence_cell_stall_ms` | ms | `300000` |
| `:convergence_fabric_503_polls` | int | `3` |
| `:convergence_health_enabled` | bool | `true` |
| `:convergence_output_stall_grace_ms` | ms | `60000` |
| `:convergence_tick_ms` | ms | `5000` |
| `:convergence_unmet_intent_grace_ms` | ms | `120000` |
| `:correction_debounce_ms` | ms | `200` |
| `:credit_flow_control_enabled` | bool | `true` |
| `:credit_low_watermark_bytes` | int | unset |
| `:default_compact_after_bytes` | int | `8388608` |
| `:default_output_stall_timeout_ms` | ms | `60000` |
| `:default_relay_completed_retention_hours` | int | `72` |
| `:default_time_travel_retention_hours` | int | `6` |
| `:dlq[:dir]` | path | `storage/app/dlq` |
| `:dlq[:max_total_bytes]` | int | `4294967296` |
| `:dlq[:replay_grace_ms]` | ms | `5000` |
| `:dlq[:retention_check_interval_ms]` | ms | `3600000` |
| `:dlq[:retention_seconds]` | seconds | `2592000` |
| `:durability_ship_retry_ms` | ms | `500` |
| `:durability_tier[:backend]` | one of `none`, `postgres`, `s3`, `local_path`, `unset` | `none` |
| `:durability_tier[:backend_opts][:access_key_id]` | string | unset |
| `:durability_tier[:backend_opts][:auto_create]` | bool | `true` |
| `:durability_tier[:backend_opts][:bucket]` | string | unset |
| `:durability_tier[:backend_opts][:database]` | string | `flezha` |
| `:durability_tier[:backend_opts][:endpoint]` | string | unset |
| `:durability_tier[:backend_opts][:hostname]` | string | `localhost` |
| `:durability_tier[:backend_opts][:password_env]` | string | unset |
| `:durability_tier[:backend_opts][:path]` | path | unset |
| `:durability_tier[:backend_opts][:port]` | int | `5432` |
| `:durability_tier[:backend_opts][:prefix]` | string | `flezha` |
| `:durability_tier[:backend_opts][:region]` | string | `us-east-1` |
| `:durability_tier[:backend_opts][:secret_access_key_env]` | string | unset |
| `:durability_tier[:backend_opts][:ssl]` | bool | `false` |
| `:durability_tier[:backend_opts][:table]` | string | `flezha_durability_artifacts` |
| `:durability_tier[:backend_opts][:username]` | string | `flezha` |
| `:durability_tier[:gc_abandoned_after_ms]` | ms | `604800000` |
| `:durability_tier[:gc_grace_ms]` | ms | `3600000` |
| `:durability_tier[:gc_interval_ms]` | ms | `3600000` |
| `:durability_tier[:rpo_target_ms]` | ms | `120000` |
| `:durability_tier[:ship_interval_ms]` | ms | `30000` |
| `:durability_writer_id` | string | derived at boot |
| `:effect_max_bytes` | int | `4194304` |
| `:env` | atom | unset |
| `:external_processes` | list of string | `""` |
| `:file_plugin_root` | path | `storage/app/files` |
| `:file_plugin_s3[:access_key_id]` | string | unset |
| `:file_plugin_s3[:endpoint]` | string | unset |
| `:file_plugin_s3[:region]` | string | `us-east-1` |
| `:file_plugin_s3[:secret_access_key_env]` | string | unset |
| `:graceful_shutdown_enabled` | bool | `true` |
| `:graceful_shutdown_timeout_ms` | ms | `60000` |
| `:group_withdraw_grace_ms` | ms | `5000` |
| `:healthz_bind` | string | `0.0.0.0` |
| `:healthz_enabled` | bool | `false` |
| `:healthz_port` | int | `9569` |
| `:host_node` | string | unset |
| `:http_poll_min_interval_ms` | ms | `1000` |
| `:hub_start_child_ms` | ms | `15000` |
| `:initial_nodes` | list of atom | `""` |
| `:input_log_hold_window_ms` | ms | `0` |
| `:input_silence_timeout_ms` | ms | `0` |
| `:io_stream_liveness_interval_ms` | ms | `10000` |
| `:io_stream_liveness_threshold_ms` | ms | derived |
| `:k8s_cluster_domain` | string | `FLEZHA_K8S_CLUSTER_NAME` |
| `:k8s_cluster_name` | string | `cluster` |
| `:k8s_host_identifier` | one of `ip`, `hostname`, `dns` | `ip` |
| `:k8s_ip_lookup_mode` | one of `endpoints`, `pods` | `pods` |
| `:k8s_master_name` | string | `kubernetes.default.svc` |
| `:k8s_master_port` | int | `6443` |
| `:k8s_namespace` | string | `flezha` |
| `:k8s_selector` | string | `app=flezha` |
| `:k8s_service_account_path` | path | `/var/run/secrets/kubernetes.io/serviceaccount` |
| `:k8s_service_name` | string | `flezha` |
| `:keep_finished_by_default` | bool | `false` |
| `:listener_pending_bytes` | positive_int | `4194304` |
| `:listener_pending_frames` | positive_int | `4096` |
| `:listener_release_check_ms` | ms | `5000` |
| `:max_reconnects` | positive_int | `5` |
| `:migrate_timeout_ms` | ms | `2000` |
| `:migration_consent[:consent_timeout_ms]` | ms | `5000` |
| `:migration_consent[:max_defer_ms]` | ms | unset |
| `:migration_consent[:retry_interval_ms]` | ms | `2000` |
| `:node_base_name` | string | `flezha` |
| `:node_host_name` | string | `127.0.0.1` |
| `:observability[:cardinality_budget_per_source]` | positive_int | `1000` |
| `:observability[:data_plane_liveness]` | bool | `true` |
| `:observability[:enabled]` | bool | `true` |
| `:observability[:log_sources]` | list of atom | `""` |
| `:observability[:otp_backend][:poll_intervals]` | term | `%{beam_vm: 1000, flezha_transport: 1000, host: 5000, processhub: 1000}` |
| `:observability[:otp_backend_enabled]` | bool | `true` |
| `:observability[:pubsub_sink_enabled]` | bool | `true` |
| `:observability[:retention_check_interval_ms]` | ms | `5000` |
| `:observability[:ring_buffer_max_entries_per_source]` | positive_int | `10000` |
| `:observability[:ring_buffer_retention_ms]` | ms | `900000` |
| `:observability_export[:otlp_batch_max]` | positive_int | `512` |
| `:observability_export[:otlp_enabled]` | bool | `false` |
| `:observability_export[:otlp_endpoint]` | string | unset |
| `:observability_export[:otlp_flush_interval_ms]` | ms | `10000` |
| `:observability_export[:otlp_headers]` | term | `%{}` |
| `:observability_export[:prometheus_bind]` | string | `127.0.0.1` |
| `:observability_export[:prometheus_enabled]` | bool | `false` |
| `:observability_export[:prometheus_port]` | int | `9568` |
| `:outbox_inflight_max` | positive_int | `1000` |
| `:outbox_resettle_ms` | ms | `60000` |
| `:output_watchdog_interval_ms` | ms | `10000` |
| `:overwatch_call_timeout_ms` | ms | `5000` |
| `:overwatch_ping_enabled` | bool | `true` |
| `:overwatch_ping_fn` | term | unset |
| `:overwatch_ping_timeout_ms` | ms | `2000` |
| `:overwatch_runner_wait_ms` | ms | `30000` |
| `:overwatch_task_warn_ms` | ms | `2000` |
| `:placement_load_cap_percent` | positive_int | `100` |
| `:placement_load_refresh_ms` | ms | `1000` |
| `:plugin_backlog_bytes` | positive_int | `4194304` |
| `:plugin_backlog_frames` | positive_int | `4096` |
| `:plugin_call_timeout_ms` | ms | `2000` |
| `:plugin_idle_stop_ms` | ms | `60000` |
| `:plugin_ingest_window` | positive_int | `64` |
| `:plugin_stable_uptime_ms` | ms | `60000` |
| `:plugin_successor_poll_ms` | ms | `50` |
| `:plugins[:list]` | term | unset |
| `:plugins[:max_package_bytes]` | int | `50000000` |
| `:plugins[:plugin_root]` | path | `storage/app/plugins` |
| `:plugins[:require_signature]` | bool | `false` |
| `:plugins[:trusted_keys]` | PEM file paths | `""` |
| `:post_hooks` | term | `[]` |
| `:pre_hooks` | term | `[]` |
| `:process_hub_cluster_settle_ms` | ms | `2000` |
| `:process_hub_nodedown_grace_ms` | ms | `30000` |
| `:process_hub_reconcile_grace_ms` | ms | `45000` |
| `:process_hub_reconcile_interval_ms` | ms | `30000` |
| `:process_hub_registry_root` | path | `storage/app/process_hub` |
| `:quic_addresses` | string | `127.0.0.1:4433` |
| `:quic_client_ca_path` | path | `storage/app/certs/ca.pem` |
| `:quic_listen_address` | address | `0.0.0.0:4433` |
| `:quorum_nodes` | positive_int | unset |
| `:quorum_percent` | positive_int | `51` |
| `:quorum_window_ms` | ms | `30000` |
| `:reconnect_interval` | ms | `1000` |
| `:reconnect_max_backoff_ms` | ms | `30000` |
| `:record_log_cut_after_bytes` | int | `65536` |
| `:relay_history[:append_ms]` | ms | `1000` |
| `:relay_history[:dedup_max_entries]` | int | `2000000` |
| `:relay_history[:dedup_window_hours]` | int | `24` |
| `:relay_history[:dir]` | path | `storage/app/relay_history` |
| `:relay_history[:enabled]` | bool | `true` |
| `:relay_history[:max_file_age_seconds]` | int | `86400` |
| `:relay_history[:max_file_bytes]` | int | `16777216` |
| `:relay_history[:max_pending]` | int | `10000` |
| `:relay_history[:max_total_bytes]` | int | `1073741824` |
| `:relay_history[:retention_seconds]` | seconds | `2592000` |
| `:restore_await_timeout_ms` | ms | `15000` |
| `:run_token` | string | unset |
| `:runner_removal_grace_ms` | ms | `30000` |
| `:runner_retiring_mark_ttl_ms` | ms | `30000` |
| `:schedules[:application_managed]` | bool | `true` |
| `:schedules[:dir]` | path | `storage/app/schedules` |
| `:schedules[:misfire_grace_ms]` | ms | `120000` |
| `:sdk_commit_ack_ms` | ms | `30000` |
| `:send_gate_hold_ms` | ms | `5000` |
| `:server_cert_path` | path | `storage/app/certs/server.pem` |
| `:server_key_path` | path | `storage/app/certs/server.key` |
| `:socket_dir_group` | string | unset |
| `:socket_files_path` | path | derived per run |
| `:source_credit_window_bytes` | int | derived |
| `:split_behaviour` | one of `divergence`, `static`, `dynamic` | `divergence` |
| `:state_resync_fun` | term | unset |
| `:state_root` | path | `storage/app/state` |
| `:static_nodes` | list of atom | `""` |
| `:stop_release_timeout_ms` | ms | `15000` |
| `:stream_bind_wait_ms` | ms | `30000` |
| `:stream_channel_max_heap_bytes` | int | `0` |
| `:testnet_enabled` | bool | `false` |
| `:testnet_ws_port` | int | `4500` |
| `:time_travel_retention_check_interval_ms` | ms | `60000` |
| `:time_travel_retention_hours` | term | `%{}` |
| `:transport` | one of `uds`, `quic` | `uds` |
| `:transport_auth_required` | bool | `true` |
| `:transports` | list of one of `uds`, `quic` | derived |
| `:wal_hard_watermark_bytes` | int | derived |
| `:wal_high_watermark_bytes` | int | `16777216` |
| `:wal_low_watermark_bytes` | int | derived |
| `:wal_pending_msg_cap` | positive_int | `5000` |
| `:ws_subscription_min_interval_ms` | ms | `1000` |
<!-- config-keys:end -->
