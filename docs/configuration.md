# Configuration reference

Every knob Flezha exposes, and — just as important — **which layer it lives in**,
because that decides whether changing it costs a restart, a redeploy, or
nothing at all.

Defaults and read sites in the tables below are extracted from the source, so
they are what the code actually does rather than what a guide once said. When a
default is an expression rather than a literal it is marked `(derived)`.

## The four layers

| Layer | Where you set it | Scope | Cost to change |
|---|---|---|---|
| 1. **Environment variables** | the process environment / your deploy manifest | whole node | restart the node |
| 2. **Application env** | `config/config.exs`, `config/runtime.exs`, `config/<env>.exs` | whole node | rebuild or restart (runtime.exs is read at boot) |
| 3. **Operative declarations** | the SDK, in the operative's `channels()` | one operative class | **redeploy the runner** — frozen at registration |
| 4. **Plugin options** | the plugin's `opts` map when it is installed | one plugin instance | re-install the plugin |

Layers 1 and 2 overlap on purpose: most environment variables exist only to set
an application-env key at boot (`config/runtime.exs` is the mapping). Prefer the
environment variable in a container; prefer the config file for a local or
embedded deployment.

Looking for the exactly-once, checkpoint or output-batching options? Those are
**[Layer 3](#layer-3-operative-declarations-sdk)** — they are declared in code,
not configured on the node.

**Layer 3 is the one that surprises people.** Anything an operative *declares* —
its delivery tier, its acknowledgement granularity, its correction window — is
frozen when the class registers with the controller. There is no operator-facing
override, by design: these change the *semantics* of the operative's output, not
just its performance, so they belong with the code that relies on them. Changing
one means editing the operative and redeploying its runner.

## Layer 1: environment variables

Set on the node's process environment; read at boot by `config/runtime.exs`.
Defaults shown are the fallback used when the variable is unset.

#### Transport & TLS

| Variable | Default |
|---|---|
| `FLEZHA_CA_CERT_PATH` | `"app_data/certs/ca.pem"` |
| `FLEZHA_CLIENT_CERT_PATH` | `"app_data/certs/client.pem"` |
| `FLEZHA_CLIENT_KEY_PATH` | `"app_data/certs/client.key"` |
| `FLEZHA_QUIC_ADDRESS` | `"0.0.0.0:4433"` |
| `FLEZHA_QUIC_CLIENT_CA` | `"app_data/certs/ca.pem"` |
| `FLEZHA_SERVER_CERT_PATH` | `"app_data/certs/server.pem"` |
| `FLEZHA_SERVER_KEY_PATH` | `"app_data/certs/server.key"` |
| `FLEZHA_SOCKET_FILES_PATH` | `"app_data/sockets/"` |
| `FLEZHA_TRANSPORT` | `"uds"` (also `"quic"`; `"native"` = in-node Elixir SDK, opt-in only, refused as a server default) |
| `FLEZHA_TRANSPORTS` | `—` (comma list; validated against `uds,quic,native` at boot) |
| `FLEZHA_TRANSPORT_AUTH` | `—` |
| `FLEZHA_CONTROLLER_ENABLED` | `true` — set `false` for a runner-only node (an SDK library dependency boots flezha with no controller tree) |

#### Cluster & nodes

| Variable | Default |
|---|---|
| `FLEZHA_CLUSTER_DISCOVERY` | `"manual_discovery"` |
| `FLEZHA_CLUSTER_FORMATION_TIMEOUT_MS` | `"15000"` |
| `FLEZHA_CLUSTER_UPDATE_INTERVAL` | `"5000"` |
| `FLEZHA_EXTERNAL_PROCESSES` | `""` |
| `FLEZHA_INITIAL_NODES` | `""` |
| `FLEZHA_K8S_CLUSTER_DOMAIN` | `System.get_env("FLEZHA_K8S_CLUSTER_NAME", ""` |
| `FLEZHA_K8S_CLUSTER_NAME` | `"cluster"` |
| `FLEZHA_K8S_HOST_IDENTIFIER` | `"ip"` |
| `FLEZHA_K8S_IP_LOOKUP_MODE` | `"pods"` |
| `FLEZHA_K8S_MASTER_NAME` | `""` |
| `FLEZHA_K8S_MASTER_PORT` | `"6443"` |
| `FLEZHA_K8S_NAMESPACE` | `"flezha"` |
| `FLEZHA_K8S_SELECTOR` | `"app=flezha"` |
| `FLEZHA_K8S_SERVICE_ACCOUNT_PATH` | `"/var/run/secrets/kubernetes.io/serviceaccount"` |
| `FLEZHA_K8S_SERVICE_NAME` | `"flezha"` |
| `FLEZHA_NODE_HOST` | `"127.0.0.1"` |
| `FLEZHA_NODE_NAME` | `"flezha"` |

#### State, WAL & durability

| Variable | Default |
|---|---|
| `FLEZHA_AUDIT_RETENTION_DAYS` | `—` |
| `FLEZHA_COMPACT_AFTER_AGE_MS` | `if(config_env` |
| `FLEZHA_COMPACT_AFTER_BYTES` | `"8388608"` |
| `FLEZHA_CREDIT_FLOW_CONTROL` | `"true"` |
| `FLEZHA_DLQ_RETENTION_DAYS` | `—` |
| `FLEZHA_DURABILITY_BACKEND` | `—` |
| `FLEZHA_DURABILITY_GC_ABANDONED_AFTER_MS` | `"604800000"` |
| `FLEZHA_DURABILITY_GC_GRACE_MS` | `"3600000"` |
| `FLEZHA_DURABILITY_GC_INTERVAL_MS` | `"3600000"` |
| `FLEZHA_DURABILITY_PATH` | `—` |
| `FLEZHA_DURABILITY_PG_AUTO_CREATE` | `"true"` |
| `FLEZHA_DURABILITY_PG_DATABASE` | `"flezha"` |
| `FLEZHA_DURABILITY_PG_HOST` | `"localhost"` |
| `FLEZHA_DURABILITY_PG_PASSWORD_ENV` | `—` |
| `FLEZHA_DURABILITY_PG_PORT` | `"5432"` |
| `FLEZHA_DURABILITY_PG_SSL` | `"false"` |
| `FLEZHA_DURABILITY_PG_TABLE` | `"flezha_durability_artifacts"` |
| `FLEZHA_DURABILITY_PG_USERNAME` | `"flezha"` |
| `FLEZHA_DURABILITY_RPO_TARGET_MS` | `"120000"` |
| `FLEZHA_DURABILITY_S3_ACCESS_KEY_ID` | `—` |
| `FLEZHA_DURABILITY_S3_BUCKET` | `—` |
| `FLEZHA_DURABILITY_S3_ENDPOINT` | `—` |
| `FLEZHA_DURABILITY_S3_PREFIX` | `"flezha"` |
| `FLEZHA_DURABILITY_S3_REGION` | `"us-east-1"` |
| `FLEZHA_DURABILITY_S3_SECRET_ENV` | `—` |
| `FLEZHA_DURABILITY_SHIP_INTERVAL_MS` | `"30000"` |
| `FLEZHA_INPUT_LOG_HOLD_WINDOW_MS` | `"0"` |
| `FLEZHA_RETENTION_CHECK_INTERVAL_MS` | `"60000"` |
| `FLEZHA_WORKFLOW_RETENTION_HOURS` | `"72"` |

#### Runner & operative lifecycle

| Variable | Default |
|---|---|
| `FLEZHA_AUTO_RESTORE_ENABLED` | `"true"` |
| `FLEZHA_BOOT_REPLAY_TIMEOUT_MS` | `"60000"` |
| `FLEZHA_MAX_RECONNECTS` | `"5"` |
| `FLEZHA_MIGRATION_CONSENT_TIMEOUT_MS` | `"5000"` |
| `FLEZHA_MIGRATION_MAX_DEFER_MS` | `—` |
| `FLEZHA_MIGRATION_RETRY_INTERVAL_MS` | `"2000"` |
| `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` | `"45000"` |
| `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS` | `"30000"` |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | `"app_data/process_hub"` |
| `FLEZHA_RECONNECT_INTERVAL` | `"1000"` |
| `FLEZHA_RECONNECT_MAX_BACKOFF_MS` | `"30000"` |
| `FLEZHA_RUNNER_KEEPALIVE` | `"30000"` |

#### Resilience & convergence

| Variable | Default |
|---|---|
| `FLEZHA_CONTROL_LIVENESS_INTERVAL_MS` | `"15000"` |
| `FLEZHA_CONTROL_LIVENESS_WINDOW_MS` | `"45000"` |
| `FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS` | `"60000"` |
| `FLEZHA_CONVERGENCE_PLUGIN_STRIKE_THRESHOLD` | `"2"` |
| `FLEZHA_CONVERGENCE_TICK_LATENCY_THRESHOLD_MS` | `"1000"` |
| `FLEZHA_CONVERGENCE_TICK_MS` | `"5000"` |
| `FLEZHA_CONVERGENCE_UNMET_INTENT_GRACE_MS` | `"120000"` |
| `FLEZHA_DATA_PLANE_LIVENESS` | `"true"` |
| `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` | `—` |
| `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` | `"60000"` |
| `FLEZHA_HEALTHZ_BIND` | `"0.0.0.0"` |
| `FLEZHA_HEALTHZ_ENABLED` | `"false"` |
| `FLEZHA_HEALTHZ_PORT` | `"9569"` |
| `FLEZHA_OUTPUT_WATCHDOG_INTERVAL_MS` | `"10000"` |
| `FLEZHA_USE_CONVERGENCE_HEALTH` | `"true"` |

#### Observability & audit

| Variable | Default |
|---|---|
| `FLEZHA_AUDIT_DIR` | `"app_data/audit"` |
| `FLEZHA_AUDIT_ENABLED` | `—` |
| `FLEZHA_LOG_KEEP_FILES` | `"5"` |
| `FLEZHA_LOG_LEVEL` | `"info"` |
| `FLEZHA_LOG_MAX_BYTES` | `"10485760"` |
| `FLEZHA_LOG_PATH` | `"logs/flezha.log"` |
| `FLEZHA_OBSERVABILITY_ENABLED` | `—` |
| `FLEZHA_OTLP_ENABLED` | `—` |
| `FLEZHA_OTLP_ENDPOINT` | `—` |
| `FLEZHA_PROMETHEUS_BIND` | `"127.0.0.1"` |
| `FLEZHA_PROMETHEUS_ENABLED` | `—` |
| `FLEZHA_PROMETHEUS_PORT` | `"9568"` |

#### Security

| Variable | Default |
|---|---|
| `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH` | `Path.join(System.get_env("FLEZHA_AUTHZ_DIR", "app_data/auth"` |
| `FLEZHA_AUTHZ_DIR` | `"app_data/auth"` |
| `FLEZHA_AUTHZ_ENABLED` | `—` |

#### Plugins, DLQ & schedules

| Variable | Default |
|---|---|
| `FLEZHA_DLQ_DIR` | `"app_data/dlq"` |
| `FLEZHA_SCHEDULE_DIR` | `"app_data/schedules"` |
| `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS` | `"120000"` |

#### Other

| Variable | Default |
|---|---|
| `FLEZHA_CONCURRENT_CHANNEL_COUNT` | `"0"` |
| `FLEZHA_FABRIC_503_POLLS` | `"3"` |

### Runner-process environment (SDK-side)

These are read by the SDK runner *process*, not the controller node — a
standalone Elixir/Rust/Java runner reads them from its own environment. They have
no controller-side application-env key.

| Variable | Default | Read by |
|---|---|---|
| `FLEZHA_RUNNER_ID_PATH` | `/var/lib/flezha-runner/id` | the SDK's durable stable identity (atomic write; degrade-to-fresh + one warn on an unwritable path) |
| `FLEZHA_MAILBOX_CAPACITY` | `1024` (range `1..1_048_576`) | per-Entity mailbox bound |
| `FLEZHA_DATA_HEARTBEAT_MS` | `10000` | `data_heartbeat` interval (jittered into `[base/2, base]`) |
| `FLEZHA_MAX_FRAME_BYTES` | `67108864` (64 MiB) | pre-auth frame-length limit (both frame parsers) |
| `FLEZHA_MAX_WRITE_BUFFER_BYTES` | `8388608` (8 MiB) | driver send-buffer cap |

## Layer 2: application env keys

Read via `Application.get_env(:flezha, ...)`. Most are set for you by
`config/runtime.exs` from the environment variables above; set them directly in
`config/*.exs` for embedded or local deployments. `—` means there is no inline
default at the read site because the key is always set in config.

#### Transport & TLS

| Key | Default | Read in |
|---|---|---|
| `:ca_cert_path` | `"app_data/certs/ca.pem"` | `lib/flezha/service/port_pilot.ex` |
| `:client_cert_path` | `"app_data/certs/client.pem"` | `lib/flezha/transport/cert_generator.ex` |
| `:client_key_path` | `"app_data/certs/client.key"` | `lib/flezha/transport/cert_generator.ex` |
| `:quic_client_ca_path` | `"app_data/certs/ca.pem"` | `lib/flezha/transport/server.ex` |
| `:quic_listen_address` | `"0.0.0.0:4433"` | `lib/flezha/service/port_pilot.ex` |
| `:server_cert_path` | `"app_data/certs/server.pem"` | `lib/flezha/transport/cert_generator.ex` |
| `:server_key_path` | `"app_data/certs/server.key"` | `lib/flezha/transport/cert_generator.ex` |
| `:socket_files_path` | `—` (set in `config/*.exs`) | `lib/flezha/service/port_pilot.ex` |
| `:transport` | `:uds` | `lib/flezha/service/port_pilot.ex` |
| `:transport_auth_required` | `true` | `lib/flezha/transport/server.ex` |
| `:transports` | `[:uds]` | `lib/flezha/application.ex` |

#### Cluster & nodes

| Key | Default | Read in |
|---|---|---|
| `:cluster_discovery` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/orchestrator.ex` |
| `:cluster_formation_timeout_ms` | `15_000` | `lib/flezha/cluster/orchestrator.ex` |
| `:cluster_update_interval` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/orchestrator.ex` |
| `:external_processes` | `—` (set in `config/*.exs`) | `lib/flezha/service/port_pilot.ex` |
| `:initial_nodes` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/orchestrator.ex` |
| `:k8s_cluster_domain` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/k8s_pod_discovery.ex` |
| `:k8s_cluster_name` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/orchestrator.ex` |
| `:k8s_host_identifier` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/orchestrator.ex` |
| `:k8s_ip_lookup_mode` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/k8s_pod_discovery.ex` |
| `:k8s_master_name` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/orchestrator.ex` |
| `:k8s_master_port` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/orchestrator.ex` |
| `:k8s_namespace` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/orchestrator.ex` |
| `:k8s_selector` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/orchestrator.ex` |
| `:k8s_service_account_path` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/k8s_pod_discovery.ex` |
| `:k8s_service_name` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/orchestrator.ex` |
| `:node_base_name` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/orchestrator.ex` |
| `:static_nodes` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/manual_discovery.ex` |

#### State, WAL & durability

| Key | Default | Read in |
|---|---|---|
| `:agent_fsync_interval_ms` | `5` | `lib/flezha/state/fsync_batcher.ex` |
| `:credit_flow_control_enabled` | `true` | `lib/flezha/runner/stream_channel.ex` |
| `:credit_low_watermark_bytes` | `half the credit window` (derived) | `lib/flezha/runner/stream_channel.ex` |
| `:default_compact_after_bytes` | `8 * 1024 * 1024` | `lib/flezha/state/persistence_resolver.ex` |
| `:default_time_travel_retention_hours` | `6` | `lib/flezha/state/persistence_resolver.ex` |
| `:default_workflow_completed_retention_hours` | `72` | `lib/flezha/state/persistence_resolver.ex` |
| `:durability_tier` | `[]` | `lib/flezha/state/durability_tier.ex` |
| `:input_log_hold_window_ms` | `0` | `lib/flezha/operative/class_registry.ex` |
| `:outbox_inflight_max` | `1_000` | `lib/flezha/state/wal_writer.ex` |
| `:outbox_resettle_ms` | `60_000` | `lib/flezha/state/wal_writer.ex` |
| `:source_credit_window_bytes` | `:wal_high_watermark_bytes` (derived) | `lib/flezha/state/input_log.ex` |
| `:state_root` | `—` (set in `config/*.exs`) | `lib/flezha/state/registry.ex` |
| `:time_travel_retention_check_interval_ms` | `60_000` | `lib/flezha/state/retention.ex` |
| `:time_travel_retention_hours` | `%{}` | `lib/flezha/service/cli_manager.ex` |
| `:wal_hard_watermark_bytes` | `2× :wal_high_watermark_bytes` (derived) | `lib/flezha/state/wal_writer.ex` |
| `:wal_high_watermark_bytes` | `16 * 1024 * 1024` | `lib/flezha/state/persistence_resolver.ex` |
| `:wal_low_watermark_bytes` | `half the high watermark` (derived) | `lib/flezha/state/wal_writer.ex` |
| `:wal_pending_msg_cap` | `5_000` | `lib/flezha/state/wal_writer.ex` |
| `:wal_resync_retry_interval_ms` | `1_000` | `lib/flezha/state/wal_writer.ex` |

#### Runner & operative lifecycle

| Key | Default | Read in |
|---|---|---|
| `:auto_restore_enabled` | `true` | `lib/flezha/boot/recovery_hooks.ex` |
| `:boot_replay_timeout_ms` | `60_000` | `lib/flezha/boot_loader.ex` |
| `:hub_runner_assign_wait_ms` | `2_000` | `lib/flezha/service/hub_manager.ex` |
| `:max_reconnects` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/orchestrator.ex` |
| `:migration_consent` | `[]` | `lib/flezha/service/hub_manager.ex` |
| `:overwatch_call_timeout_ms` | `5_000` | `lib/flezha/service/operative_manager.ex` |
| `:overwatch_ping_enabled` | `true` | `lib/flezha/operative/lifecycle.ex` |
| `:overwatch_ping_timeout_ms` | `2_000` | `lib/flezha/runner/control_unit.ex` |
| `:overwatch_proc_warn_ms` | `2_000` | `lib/flezha/operative/overwatch.ex` |
| `:overwatch_retry_backoff_ms` | `[200, 500, 1_000, 2_000, 5_000, 10_000]` | `lib/flezha/operative/lifecycle.ex` |
| `:overwatch_runner_wait_ms` | `30_000` | `lib/flezha/operative/lifecycle.ex` |
| `:plugin_retry_base_delay_ms` | `4_000` | `lib/flezha/plugin/extension/table.ex` |
| `:process_hub_reconcile_grace_ms` | `45_000` | `lib/flezha/service/hub_manager.ex` |
| `:process_hub_reconcile_interval_ms` | `30_000` | `lib/flezha/service/hub_manager.ex` |
| `:process_hub_registry_root` | `"app_data/process_hub"` | `lib/flezha/service/hub_manager.ex` |
| `:reconnect_interval` | `—` (set in `config/*.exs`) | `lib/flezha/cluster/orchestrator.ex` |
| `:reconnect_max_backoff_ms` | `30_000` | `lib/flezha/cluster/orchestrator.ex` |
| `:restore_await_timeout_ms` | `15_000` | `lib/flezha/state/commands.ex` |
| `:runner_keepalive` | `"30000"` | `lib/flezha/runner/fleet_manager.ex` |

#### Resilience & convergence

| Key | Default | Read in |
|---|---|---|
| `:catastrophic_dataflow_timeout_ms` | `60_000` | `lib/flezha/state/watchdog.ex` |
| `:control_liveness_interval_ms` | `15_000` | `lib/flezha/runner/control_channel.ex` |
| `:control_liveness_window_ms` | `45_000` | `lib/flezha/runner/control_channel.ex` |
| `:convergence_fabric_503_polls` | `3` | `lib/flezha/healthz/server.ex` |
| `:convergence_health_enabled` | `true` | `lib/flezha/application.ex` |
| `:convergence_output_stall_grace_ms` | `60_000` | `lib/flezha/convergence/loop.ex` |
| `:convergence_tick_ms` | `5_000 | @default_tick_ms` | `lib/flezha/convergence/loop.ex` |
| `:convergence_unmet_intent_grace_ms` | `120_000` | `lib/flezha/healthz/server.ex` |
| `:correction_debounce_ms` | `200` | `lib/flezha/operative/replay.ex` |
| `:graceful_shutdown_enabled` | `true` | `lib/flezha/application.ex` |
| `:graceful_shutdown_timeout_ms` | `60_000` | `lib/flezha/service/cli_manager.ex` |
| `:healthz_bind` | `"127.0.0.1"` | `lib/flezha/healthz/server.ex` |
| `:healthz_enabled` | `false` | `lib/flezha/application.ex` |
| `:healthz_port` | `9569` | `lib/flezha/healthz/server.ex` |
| `:io_stream_liveness_interval_ms` | `10_000` | `lib/flezha/runner/stream_channel.ex` |
| `:io_stream_liveness_threshold_ms` | `30_000` | `lib/flezha/runner/stream_channel.ex` |
| `:output_watchdog_interval_ms` | `10_000` | `lib/flezha/state/watchdog.ex` |

#### Observability & audit

| Key | Default | Read in |
|---|---|---|
| `:audit_log` | `[]` | `lib/flezha/observability/audit_log/retention.ex` |
| `:observability` | `[]` | `lib/flezha/observability.ex` |
| `:observability_export` | `[]` | `lib/flezha/observability/export/otlp_client.ex` |

#### Security

| Key | Default | Read in |
|---|---|---|
| `:authn_authz` | `... | []` | `lib/flezha/application.ex` |

#### Plugins, DLQ & schedules

| Key | Default | Read in |
|---|---|---|
| `:dlq` | `[]` | `lib/flezha/dlq/store.ex` |
| `:enabled_plugins` | `—` (set in `config/*.exs`) | `lib/flezha/service/config_manager.ex` |
| `:plugin_installer` | `[]` | `lib/flezha/plugin/installer.ex` |
| `:plugin_installer_data_dir` | `—` (set in `config/*.exs`) | `lib/flezha/plugin/installer/store.ex` |
| `:plugin_installer_enabled` | `false` | `lib/flezha/application.ex` |
| `:schedules` | `[]` | `lib/flezha/application.ex` |

#### Other

| Key | Default | Read in |
|---|---|---|
| `:concurrent_channel_count` | `0` | `lib/flezha/service/config_manager.ex` |
| `:control_unit_heartbeat_interval_ms` | `15_000` | `lib/flezha/runner/control_unit.ex` |
| `:control_unit_pending_reply_timeout_ms` | `90_000` | `lib/flezha/runner/control_unit.ex` |
| `:default_output_stall_timeout_ms` | `60_000` | `lib/flezha/state/persistence_resolver.ex` |
| `:env` | `—` (set in `config/*.exs`) | `lib/flezha/operative/handover.ex` |
| `:post_hooks` | `[]` | `lib/flezha/boot_loader.ex` |
| `:pre_hooks` | `[]` | `lib/flezha/boot_loader.ex` |
| `:state_resync_fun` | `—` (set in `config/*.exs`) | `lib/flezha/state/recovery.ex` |
| `:testnet_enabled` | `—` (set in `config/*.exs`) | `lib/flezha/application.ex` |

## Layer 3: operative declarations (SDK)

Declared inside `channels()` on a `StreamOperative`. Rust names first, Java in
parentheses. All are frozen at registration.

### Delivery and acknowledgement

| Declaration | Default if absent | What it does |
|---|---|---|
| `effectively_once()` (`effectivelyOnce`) | off — at-least-once | Every emit rides an atomic effect record through the WAL, and the outbox cursor advances only on a sink's confirmation. Requires every plugin-bound channel to be `.keyed()`. |
| `checkpoint_ack()` (`checkpointAck`) | off — acknowledge per input | Hold the consumed-offset until the interval closes, so state and offset advance together. Requires `effectively_once()`. |
| `checkpoint_every(n)` (`checkpointEvery`) | absent — only `ctx.checkpoint()` closes the interval | Close the interval automatically after `n` inputs. An explicit `ctx.checkpoint()` still closes it early. Implies `checkpoint_ack()`. |
| `outputs_at_checkpoint(max_delay_ms)` (`outputsAtCheckpoint`) | absent — outputs flush as produced | Hold the interval's outputs and ship them **inside** its checkpoint record, flushed as one batch. Implies `checkpoint_ack()`. |
| `correction_window(window_ms)` (`correctionWindow`) | absent — a late input is just an input | A late input inside the window rewinds the Entity to a baseline and replays the affected slice in event-time order. Needs an `EventTimeFrom` clock. |
| `beyond_window_policy(policy)` (`beyondWindowPolicy`) | `ProcessAsArrival` | Fate of an input later than the correction window: process it as an arrival, or drop it. |

#### Tuning the output-latency / throughput trade

`outputs_at_checkpoint` is the one declaration that is a **dial rather than a
switch**, and it is worth understanding before you reach for it.

The interval closes on whichever comes first — `checkpoint_every(n)` inputs, or
`max_delay_ms` milliseconds — so the effective interval is
`min(n / input_rate, max_delay_ms)`, and the output latency you add is bounded
by it. Measured on the round-trip bench (keyed, exactly-once, 4 pipelines,
`n = 500`, `D = 50 ms`, so `D` was binding):

| | throughput | paced p50 | paced p99 | p99 − p50 |
|---|---|---|---|---|
| not declared (`as_produced`) | 20,895 rec/s | 22.3 ms | 71.8 ms | 49.5 ms |
| `outputs_at_checkpoint(50)` | **45,009 rec/s** | 70.7 ms | 89.9 ms | **19.2 ms** |

Three things to take from that:

- **You pay roughly `max_delay_ms` in added median latency, and you pick that
  number.** Want 10 ms of exposure? Declare `outputs_at_checkpoint(10)`. Want
  none? Do not declare it — the default is the latency-optimal setting.
- **The tail tightens.** p99 rose far less than p50, so the spread nearly
  halved: outputs leave in lockstep at interval boundaries instead of queueing
  behind a saturated per-record path. The cost is a predictable shift, not a
  tail risk.
- **Above the knee the trade inverts.** Saturated (unpaced) p99 went 2047 → 867
  ms, because throughput more than doubled and the backlog drains faster. If you
  are near the ceiling, the opt-in wins on both axes.

`max_delay_ms` is required with the option and must be greater than zero — an
unbounded output hold is not declarable, by construction. It is also what stops
a **quiet** Entity from sitting on an output forever: the interval closes on the
delay even if no further input arrives.

### Channel and edge options

Declared on the stream handle returned by `input(...)`.

| Declaration | Default | What it does |
|---|---|---|
| `.on_full(OverflowPolicy)` | `Block` | What a full Entity mailbox does: `Block` (back-pressure), `DropNewest`, `DropOldest`. Nothing drops by default. |
| `.retry_max(n)` | per-channel policy | Attempts before an input is parked to the dead-letter queue. |
| `.retry_backoff_ms(ms)` | per-channel policy | Base delay of the retry ladder. |
| `.keyed()` | off | Stamp `flz.key` on the output; required for every plugin channel under `effectively_once()`. |
| `.key_by(f)` | absent — one Entity | Route by a key function, making the procedure dynamic-entity. |
| `.sink(name)` | — | Wire the channel to a named plugin. |
| `.sealed()` | — | Terminate the chain with no plugin egress. |

### Procedure-level options

Overridden on the `Procedure` trait (Rust) / procedure class (Java).

| Override | Default | What it does |
|---|---|---|
| `compact_after_bytes()` | `:default_compact_after_bytes` (8 MiB) | Per-Entity WAL size that triggers snapshot compaction. |
| `mailbox_capacity()` | 1024 | Bounded mailbox depth per Entity. |
| `entity_idle_ttl()` | none | Evict a dynamic Entity after this idle period. |
| `Clock` associated type | `WallClock` | `WallClock`, `EventTimeFrom<..>`, or `LogicalClock` — decides `ctx.now()` and event-time ordering. |

## Layer 4: plugin options

Passed in the plugin's `opts` map at install time. The Kafka producer carries
the most, and is representative:

| Option | Default | What it does |
|---|---|---|
| `:queue_overflow` | `:block` for a delivery-guaranteed record, `:drop_oldest` otherwise | What a full producer queue does. An explicitly configured drop of a guaranteed record is alarm-grade and does **not** advance the outbox cursor. |
| `:queue_max` | `10_000` | Bounded producer queue depth. |
| `:queue_block_timeout_ms` | `5_000` | How long a synchronous caller parks on a full queue. |
| `:linger_ms` | `10` | Batch linger before a produce request. Lower = lower latency, smaller batches. |
| `:required_acks` | `-1` | Broker acknowledgement level. |
| `:idempotent` | `false` | Producer idempotence (broker sequence numbers). |
| `:max_batch_bytes` | `1_048_576` | Produce-request size ceiling. |
| `:retries` / `:retry_backoff_ms` / `:retry_max_backoff_ms` | `5` / `100` / `5_000` | Retry ladder before a batch parks to the DLQ. |
| `:delivery_timeout_ms` | `30_000` | Total budget for one record's delivery. |
| `:dispatch_timeout_ms` | `10_000` | Per-attempt send budget. |
| `:request_timeout_ms` | `10_000` | Broker-side request timeout. |
| `:auto_create_topic` / `:auto_create_partitions` / `:auto_create_replication_factor` | `true` / `1` / `2` | Topic auto-creation. |

Other providers (`operative_postgres`, `http_poll`, `websocket`,
`operative_forward`, `beam_msg_forward`, `std_out`, `filter`) carry their own
option sets; see the per-plugin pages under `docs/`.

## See also

- `docs/deployment.md` — how to actually run a node, and the deploy-time subset
  of the variables above in narrative form.
- `docs/resilience/data_durability.md` — what the durability knobs mean for
  correctness, including the acknowledgement-granularity options.
- `docs/architecture/kernel.md` — where the WAL, outbox and fsync knobs sit in
  the data path.
- `docs/operations/authn_authz.md` — the security posture behind the authz keys.
- `docs/kafka-producer.md` — the Kafka producer's options in depth.
- `docs/benchmarking.md` — measuring the effect of a change to any of these.
