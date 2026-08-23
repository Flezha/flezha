# Configuration reference

Every knob Flezha exposes, and which of four layers it lives in — because the
layer decides whether changing it costs a restart, a redeploy, or nothing. It
is for operators setting up a node and for operative authors wondering why a
setting is not on the node at all. Defaults are the literal the code falls back
to when nothing is set; `(derived)` marks a default computed from another knob.

## The four layers

| Layer | Where you set it | Scope | Cost to change |
|---|---|---|---|
| 1. Environment variables | the process environment / deploy manifest | whole node | restart the node |
| 2. Application env | `config/config.exs`, `config/runtime.exs`, `config/<env>.exs` | whole node | rebuild or restart (`runtime.exs` is read at boot) |
| 3. Operative declarations | the SDK, in the operative's `channels()` and Procedure/Workflow class | one operative class | redeploy the runner — frozen at registration |
| 4. Plugin options | the plugin's `opts` map when it is installed | one plugin instance | re-install the plugin |

Layers 1 and 2 overlap on purpose: most environment variables exist only to
set an application-env key at boot, and `config/runtime.exs` is the mapping.
Prefer the variable in a container, the config file for an embedded or local
node.

Layer 3 is the one that surprises people. Anything an operative declares —
its delivery tier, its acknowledgement granularity, its correction window — is
frozen when the class registers with the controller, and there is no
operator-facing override: these change the semantics of the operative's
output, not just its performance, so they belong with the code that relies on
them. The effectively-once, checkpoint and output-batching options are all
[Layer 3](#layer-3-operative-declarations-sdk).

## Layer 1: environment variables

Read at boot by `config/runtime.exs` unless the notes say otherwise. Boolean
variables accept `1`/`true`/`yes`/`on` as true unless a narrower list is given.

### Transport and TLS

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_TRANSPORT` | `uds` | `:transport`. `uds`, `quic` or `native` (the in-node Elixir SDK transport, never chosen implicitly); any other value is refused at boot |
| `FLEZHA_TRANSPORTS` | the value of `FLEZHA_TRANSPORT` (`uds,quic` in test) | `:transports`, comma list validated against the same three names |
| `FLEZHA_TRANSPORT_AUTH` | required | `:transport_auth_required`; `off`/`false`/`0`/`no` disables peer authentication and is warned on every boot |
| `FLEZHA_QUIC_ADDRESS` | `0.0.0.0:4433` | `:quic_listen_address` |
| `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` | `:ca_cert_path` |
| `FLEZHA_SERVER_CERT_PATH` | `app_data/certs/server.pem` | `:server_cert_path` |
| `FLEZHA_SERVER_KEY_PATH` | `app_data/certs/server.key` | `:server_key_path` |
| `FLEZHA_CLIENT_CERT_PATH` | `app_data/certs/client.pem` | `:client_cert_path` |
| `FLEZHA_CLIENT_KEY_PATH` | `app_data/certs/client.key` | `:client_key_path` |
| `FLEZHA_QUIC_CLIENT_CA` | `app_data/certs/ca.pem` | `:quic_client_ca_path`, trust anchor for QUIC client certificates |
| `FLEZHA_SOCKET_FILES_PATH` | `app_data/sockets/` | `:socket_files_path` |
| `FLEZHA_CONCURRENT_CHANNEL_COUNT` | `0` | `:concurrent_channel_count` |
| `FLEZHA_EXTERNAL_PROCESSES` | `""` | `:external_processes`, comma list of commands the controller spawns (test mode hard-codes the Rust test runner) |
| `FLEZHA_CONTROLLER_ENABLED` | `true` | `:controller_enabled`; `false`/`0`/`no`/`off` boots a runner-only node with no controller tree |

### Cluster and nodes

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_CLUSTER_DISCOVERY` | `manual_discovery` | `:cluster_discovery`; also `k8s_pod_discovery` |
| `FLEZHA_NODE_NAME` | `flezha` | `:node_base_name`; `rel/env.sh.eex` builds `RELEASE_NODE=<name>@<host>` from it |
| `FLEZHA_NODE_HOST` | `127.0.0.1` | the host half of `RELEASE_NODE` (`rel/env.sh.eex`); also sets `:node_host_name`, which nothing reads |
| `RELEASE_COOKIE` | unset | read by `rel/env.sh.eex`: set → `RELEASE_DISTRIBUTION=name`; unset → `none` (no clustering, `flz` verbs fail) |
| `FLEZHA_DIST_TLS_OPTFILE` | unset | read by `rel/env.sh.eex`: path of an `inet_tls_dist` option file, adds `-proto_dist inet_tls -ssl_dist_optfile` |
| `FLEZHA_INITIAL_NODES` | `""` | `:initial_nodes`, comma list of node names |
| `FLEZHA_RECONNECT_INTERVAL` | `1000` | `:reconnect_interval` |
| `FLEZHA_MAX_RECONNECTS` | `5` | `:max_reconnects` |
| `FLEZHA_RECONNECT_MAX_BACKOFF_MS` | `30000` | `:reconnect_max_backoff_ms` |
| `FLEZHA_CLUSTER_UPDATE_INTERVAL` | `5000` | `:cluster_update_interval` |
| `FLEZHA_CLUSTER_FORMATION_TIMEOUT_MS` | `15000` | `:cluster_formation_timeout_ms` |
| `FLEZHA_K8S_SERVICE_ACCOUNT_PATH` | `/var/run/secrets/kubernetes.io/serviceaccount` | `:k8s_service_account_path` |
| `FLEZHA_K8S_CLUSTER_NAME` | `cluster` | `:k8s_cluster_name` |
| `FLEZHA_K8S_CLUSTER_DOMAIN` | the raw value of `FLEZHA_K8S_CLUSTER_NAME`, else `""` | `:k8s_cluster_domain` |
| `FLEZHA_K8S_HOST_IDENTIFIER` | `ip` | `:k8s_host_identifier`; `ip`, `hostname` or `dns` |
| `FLEZHA_K8S_MASTER_NAME` | `""` | `:k8s_master_name` |
| `FLEZHA_K8S_MASTER_PORT` | `6443` | `:k8s_master_port` |
| `FLEZHA_K8S_NAMESPACE` | `flezha` | `:k8s_namespace` |
| `FLEZHA_K8S_IP_LOOKUP_MODE` | `pods` | `:k8s_ip_lookup_mode`; `pods` or `endpoints` |
| `FLEZHA_K8S_SERVICE_NAME` | `flezha` | `:k8s_service_name` |
| `FLEZHA_K8S_SELECTOR` | `app=flezha` | `:k8s_selector` |

### State, WAL and durability

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_STATE_DIR` | `app_data/state` | the state root, read at runtime by `Flezha.State.Registry.state_root/0` when `:state_root` is unset |
| `FLEZHA_COMPACT_AFTER_BYTES` | `8388608` | `:default_compact_after_bytes` |
| `FLEZHA_COMPACT_AFTER_AGE_MS` | `300000` (`0` in test) | `:compact_after_age_ms`; `0` disables the age trigger |
| `FLEZHA_INPUT_LOG_HOLD_WINDOW_MS` | `0` | `:input_log_hold_window_ms` — a floor; a declared `correction_window` raises its own hold above it |
| `FLEZHA_CREDIT_FLOW_CONTROL` | `true` | `:credit_flow_control_enabled` |
| `FLEZHA_RETENTION_CHECK_INTERVAL_MS` | `60000` | `:time_travel_retention_check_interval_ms` (not in test) |
| `FLEZHA_WORKFLOW_RETENTION_HOURS` | `72` | `:default_workflow_completed_retention_hours` (not in test) |
| `FLEZHA_DURABILITY_BACKEND` | prod: unset refuses to boot; dev/test: `none` | `:durability_tier[:backend]`; `none`, `postgres`, `s3`, `local_path`; any other spelling resolves to `none` |
| `FLEZHA_DURABILITY_WRITER_ID` | `node()` | read at runtime by `Flezha.State.DurabilityTier.writer_id/0`; the stable lineage-writer identity for a controller whose node name changes on reschedule |
| `FLEZHA_DURABILITY_SHIP_INTERVAL_MS` | `30000` | `:durability_tier[:ship_interval_ms]` |
| `FLEZHA_DURABILITY_RPO_TARGET_MS` | `120000` | `:durability_tier[:rpo_target_ms]` |
| `FLEZHA_DURABILITY_GC_INTERVAL_MS` | `3600000` | `:durability_tier[:gc_interval_ms]` |
| `FLEZHA_DURABILITY_GC_GRACE_MS` | `3600000` | `:durability_tier[:gc_grace_ms]` |
| `FLEZHA_DURABILITY_GC_ABANDONED_AFTER_MS` | `604800000` | `:durability_tier[:gc_abandoned_after_ms]` |
| `FLEZHA_DURABILITY_PATH` | unset | `backend_opts[:path]` for `local_path` — a directory that outlives the node's own disk |
| `FLEZHA_DURABILITY_PG_HOST` / `_PORT` / `_DATABASE` / `_USERNAME` | `localhost` / `5432` / `flezha` / `flezha` | `backend_opts` for `postgres` |
| `FLEZHA_DURABILITY_PG_PASSWORD_ENV` | unset | names the variable holding the password — never the password itself |
| `FLEZHA_DURABILITY_PG_SSL` / `_TABLE` / `_AUTO_CREATE` | `false` / `flezha_durability_artifacts` / `true` | `backend_opts` for `postgres` |
| `FLEZHA_DURABILITY_S3_ENDPOINT` / `_BUCKET` / `_ACCESS_KEY_ID` | unset | `backend_opts` for `s3` |
| `FLEZHA_DURABILITY_S3_SECRET_ENV` | unset | names the variable holding the secret key |
| `FLEZHA_DURABILITY_S3_REGION` / `_PREFIX` | `us-east-1` / `flezha` | `backend_opts` for `s3` |

### Runner and operative lifecycle

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_RUNNER_KEEPALIVE` | `30000` | `:runner_keepalive`; how long a disconnected runner is kept before removal, `0` removes at once |
| `FLEZHA_MIGRATION_CONSENT_TIMEOUT_MS` | `5000` | `:migration_consent[:consent_timeout_ms]` |
| `FLEZHA_MIGRATION_RETRY_INTERVAL_MS` | `2000` | `:migration_consent[:retry_interval_ms]` |
| `FLEZHA_MIGRATION_MAX_DEFER_MS` | unset → the graceful-shutdown timeout (derived) | `:migration_consent[:max_defer_ms]` |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | `app_data/process_hub` | `:process_hub_registry_root` (not in test) |
| `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` | `45000` | `:process_hub_reconcile_grace_ms` (not in test) |
| `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS` | `30000` | `:process_hub_reconcile_interval_ms` (not in test) |
| `FLEZHA_BOOT_REPLAY_TIMEOUT_MS` | `60000` | `:boot_replay_timeout_ms` (not in test) |
| `FLEZHA_AUTO_RESTORE_ENABLED` | `true` | `:auto_restore_enabled` (not in test); true only for `true`/`1` |

### Resilience and convergence

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_CONTROL_LIVENESS_WINDOW_MS` | `45000` | `:control_liveness_window_ms`; pair with the runner's `FLEZHA_CONTROL_IDLE_TIMEOUT_MS` |
| `FLEZHA_CONTROL_LIVENESS_INTERVAL_MS` | `15000` | `:control_liveness_interval_ms` |
| `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` | `true` | `:graceful_shutdown_enabled`; when set, anything but `1`/`true`/`yes`/`on` is false |
| `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` | `60000` | `:graceful_shutdown_timeout_ms`; the orchestrator grace period must be this + 15 s |
| `FLEZHA_OUTPUT_WATCHDOG_INTERVAL_MS` | `10000` | `:output_watchdog_interval_ms` (not in test) |
| `FLEZHA_USE_CONVERGENCE_HEALTH` | `true` | `:convergence_health_enabled` (not in test); true only for `true`/`1` |
| `FLEZHA_CONVERGENCE_TICK_MS` | `5000` | `:convergence_tick_ms` (not in test) |
| `FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS` | `60000` | `:convergence_output_stall_grace_ms` (not in test) |
| `FLEZHA_CONVERGENCE_UNMET_INTENT_GRACE_MS` | `120000` | `:convergence_unmet_intent_grace_ms` (not in test) |
| `FLEZHA_CONVERGENCE_PLUGIN_STRIKE_THRESHOLD` | `2` | `:convergence_plugin_strike_threshold` — set by `runtime.exs`, read by nothing in `lib/`; inert |
| `FLEZHA_CONVERGENCE_TICK_LATENCY_THRESHOLD_MS` | `1000` | `:convergence_tick_latency_threshold_ms` — set by `runtime.exs`, read by nothing in `lib/`; inert |
| `FLEZHA_FABRIC_503_POLLS` | `3` | `:convergence_fabric_503_polls` (not in test); consecutive classifier polls reporting `driver_loop_frozen` / `nif_queue_backing_up` before `/healthz` answers 503; `0` disables the fabric 503 |
| `FLEZHA_HEALTHZ_ENABLED` | `false` | `:healthz_enabled` (not in test); true only for `true`/`1` |
| `FLEZHA_HEALTHZ_PORT` | `9569` | `:healthz_port` (not in test) |
| `FLEZHA_HEALTHZ_BIND` | `0.0.0.0` | `:healthz_bind` (not in test) |
| `FLEZHA_DATA_PLANE_LIVENESS` | `true` | `:observability[:data_plane_liveness]`; `false` drops the driver poll and the stall classifier, the counters stay |

### Observability and audit

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_OBSERVABILITY_ENABLED` | `true` | `:observability[:enabled]`; `false` starts no sinks, no logger handler, no telemetry bridge |
| `FLEZHA_PROMETHEUS_ENABLED` | `false` | `:observability_export[:prometheus_enabled]` (not in test) |
| `FLEZHA_PROMETHEUS_PORT` | `9568` | `:observability_export[:prometheus_port]` (not in test) |
| `FLEZHA_PROMETHEUS_BIND` | `127.0.0.1` | `:observability_export[:prometheus_bind]` (not in test) |
| `FLEZHA_OTLP_ENABLED` | `false` | `:observability_export[:otlp_enabled]` (not in test) |
| `FLEZHA_OTLP_ENDPOINT` | unset | `:observability_export[:otlp_endpoint]` (not in test) |
| `FLEZHA_AUDIT_ENABLED` | `false` | `:audit_log[:enabled]` |
| `FLEZHA_AUDIT_DIR` | `app_data/audit` | `:audit_log[:dir]` |
| `FLEZHA_AUDIT_RETENTION_DAYS` | `30` | `:audit_log[:retention_seconds]` (× 86 400) |
| `FLEZHA_LOG_PATH` | `logs/flezha.log` | `:logger, :file_log` path |
| `FLEZHA_LOG_LEVEL` | `info` | `:logger, :file_log` level |
| `FLEZHA_LOG_MAX_BYTES` | `10485760` | `:logger, :file_log` rotation size |
| `FLEZHA_LOG_KEEP_FILES` | `5` | `:logger, :file_log` rotated files kept |

### Security

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_AUTHZ_ENABLED` | `true` in prod, `false` otherwise | `:authn_authz[:enabled]`; `Flezha.AuthZ.Config` also reads the variable live, so it wins over the config file. `false` warns on every boot |
| `FLEZHA_AUTHZ_DIR` | `app_data/auth` | `:authn_authz[:auth_dir]`, also read live by `Flezha.AuthZ.Config` |
| `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH` | `<FLEZHA_AUTHZ_DIR>/bootstrap.token` | `:authn_authz[:bootstrap_token_path]`, also read live |

### Plugins, DLQ and schedules

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_DLQ_DIR` | `app_data/dlq` | `:dlq[:dir]` |
| `FLEZHA_DLQ_RETENTION_DAYS` | `30` | `:dlq[:retention_seconds]` (× 86 400) |
| `FLEZHA_SCHEDULE_DIR` | `app_data/schedules` | `:schedules[:dir]` |
| `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS` | `120000` | `:schedules[:misfire_grace_ms]` |
| `FLEZHA_PLUGIN_INSTALLER_ENABLED` | `false` | `:plugin_installer_enabled`; true only for `true`/`1`/`yes` |
| `FLEZHA_PLUGIN_ROOT` | `app_data/plugins` | `:plugin_installer[:plugin_root]` |
| `FLEZHA_PLUGIN_REQUIRE_SIGNATURE` | `false` | `:plugin_installer[:require_signature]` |
| `FLEZHA_PLUGIN_ALLOW_UNSIGNED` | `true` | `:plugin_installer[:allow_unsigned]` |
| `FLEZHA_PLUGIN_TRUSTED_KEYS` | `""` | `:plugin_installer[:trusted_keys]`, comma-separated PEM file paths read at boot |
| `FLEZHA_PLUGIN_MAX_PACKAGE_BYTES` | `50000000` | `:plugin_installer[:max_package_bytes]` |
| `FLEZHA_PLUGIN_MAX_SOURCE_FILES` | `200` | `:plugin_installer[:max_source_files]` |

The `FLEZHA_PLUGIN_*` family (other than the enabled toggle) is parsed by
`Flezha.Plugin.Installer.RuntimeConfig`, and only when at least one of them is
present; otherwise the `config/config.exs` defaults stand.

### Build-time and image variables

| Variable | Read by | Notes |
|---|---|---|
| `FLEZHA_PROCESS_HUB_PATH` | `mix.exs` | build a release against a local `process_hub` checkout instead of the GitHub `dev` branch |
| `RUSTLER_SKIP_COMPILATION` | Rustler, via `mix compile` | `true` makes the build pick up a pre-built `priv/native/flz_nif.so` instead of invoking cargo (prod image Stage 2) |
| `FLEZHA_SNAPSHOT_ENABLED`, `FLEZHA_SNAPSHOT_DIR`, `FLEZHA_RESTORE_WAIT_TIMEOUT_S`, `FLEZHA_TESTNET_WS_PORT` | `docker/prod/controller-entrypoint.sh` | the image entrypoint's own contract; see [`operations/deployment.md`](operations/deployment.md) |

### Runner-process environment (SDK side)

Read by the runner process, not the controller node. A standalone Rust, Java or
Elixir runner reads them from its own environment; the shared `flz_transport`
crate also reads its rows inside the controller's NIF.

| Variable | Default | Read by |
|---|---|---|
| `FLEZHA_RUNNER_ID_PATH` | `/var/lib/flezha-runner/id` | `flz_core::persistent_id`, `Flezha.SDK.PersistentId` — the durable stable identity; an unwritable path degrades to a fresh per-boot id with one warning, never a refusal |
| `FLEZHA_MAILBOX_CAPACITY` | `1024` (range `1..1_048_576`) | per-Entity mailbox bound in all three SDKs; a Procedure that overrides `mailbox_capacity()` wins over it |
| `FLEZHA_SOCKET_FILES_PATH` | `app_data/sockets` | `flz_transport` path helper, `Flezha.SDK.Transport` |
| `FLEZHA_TRANSPORT` | `uds` | `flz_transport` (`quic` is the only other value) |
| `FLEZHA_QUIC_ADDRESS` | `127.0.0.1:4433` | `flz_transport`, `Flezha.SDK.Transport` — the address to dial |
| `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` | `flz_transport`, `Flezha.SDK.Transport` |
| `FLEZHA_QUIC_CLIENT_CERT` / `FLEZHA_QUIC_CLIENT_KEY` | `app_data/certs/client.pem` / `app_data/certs/client.key` | `flz_transport` — the client pair presented to a QUIC server that requires peer authentication |
| `FLEZHA_MAX_FRAME_BYTES` | `67108864` (64 MiB) | `flz_transport` — pre-auth frame-length limit enforced by both frame parsers, cached on first read |
| `FLEZHA_MAX_WRITE_BUFFER_BYTES` | `8388608` (8 MiB) | `flz_transport` — per-stream send-buffer cap |
| `FLEZHA_CONTROL_ACK_TIMEOUT_MS` | `10000` | `flz_transport` — first-frame ack deadline |
| `FLEZHA_CONTROL_IDLE_TIMEOUT_MS` | `45000` | `flz_transport` — steady-state idle window |
| `FLEZHA_CONTROL_KEEPALIVE_MS` | `15000` | `flz_transport` — control-channel ping interval |
| `FLEZHA_DATA_HEARTBEAT_MS` | `10000` | `flz_transport`, `Flezha.SDK.RunnerServer` — `data_heartbeat` interval, jittered into `[base/2, base]` |
| `FLEZHA_WORKER_POOL_SIZE` | Rust: host parallelism; Java: `2` fetchers | `flezha_runner::operative::poll_pool`, `FlezhaEnv` |
| `FLEZHA_WORKER_POOL_MAX` | `10 × FLEZHA_WORKER_POOL_SIZE` (derived) | Rust — ceiling the starvation supervisor may grow the pool to |
| `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` | `1024` | `flz_core::poll_core` |
| `FLEZHA_FLUSH_LINGER_MS` | `5` | `flz_core::flusher` |
| `FLEZHA_FLUSH_BEFORE_EMIT` | on | Rust dispatcher; `0`/`false`/`off`/`no` reverts to inline emit |
| `FLEZHA_REAP_INTERVAL_MS` | `min(entity_idle_ttl / 4, 30 s)` (derived) | `flz_core::dynamic` — idle-Entity reaper cadence |
| `FLEZHA_RECONNECT_RING_CAPACITY` | `1024` | `flz_core::state_sink` — frames buffered while no sink is installed |
| `FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD` / `FLEZHA_BROKEN_PIPE_WINDOW_MS` | `10` / `5000` | `flz_core::state_sink` — write failures inside the window that request a reconnect |
| `FLEZHA_RECONNECT_REQUEST_RETRY_MS` | `30000` | `flz_core::state_sink` |
| `FLEZHA_WORKFLOW_ACK_TIMEOUT_MS` | `30000` | `flz_core::workflow::driver` — proc-commit ack deadline |
| `FLEZHA_WORKFLOW_PARK_IDLE_MS` | `5000` | `flz_core::workflow::driver` — inbox-loop park hysteresis |
| `FLEZHA_WORKFLOW_DRIVER_STACK_KIB` | platform default | `flz_core::workflow::driver` — driver-thread stack size; address space only |
| `FLEZHA_NATIVE_LIB_DIR` | `java.io.tmpdir` | Java `NativeLibraryLoader` — where the bundled `flz_core_ffi` library is extracted |

## Layer 2: application env keys

Read via `Application.get_env(:flezha, key, default)`; `Read in` names the
module that owns the default. Most keys are set for you by `config/runtime.exs`
from the variables above. `—` means the key is always set in `config/` and the
read site has no inline default (`fetch_env!`); keyword-list keys are shown as
`:parent[:child]`.

### Transport and TLS

| Key | Default | Read in |
|---|---|---|
| `:transport` | `:uds` | `lib/flezha/service/port_pilot.ex` |
| `:transports` | `[:uds]` | `lib/flezha/application.ex`, `lib/flezha/transport/server.ex` |
| `:transport_auth_required` | `true` | `lib/flezha/transport/server.ex` |
| `:controller_enabled` | `true` | `lib/flezha/application.ex` |
| `:quic_listen_address` | `"0.0.0.0:4433"` | `lib/flezha/transport/server.ex`, `lib/flezha/service/port_pilot.ex` |
| `:ca_cert_path` | `"app_data/certs/ca.pem"` | `lib/flezha/transport/cert_generator.ex`, `lib/flezha/service/port_pilot.ex` |
| `:server_cert_path` / `:server_key_path` | `"app_data/certs/server.pem"` / `"app_data/certs/server.key"` | `lib/flezha/transport/cert_generator.ex`, `lib/flezha/transport/server.ex` |
| `:client_cert_path` / `:client_key_path` | `"app_data/certs/client.pem"` / `"app_data/certs/client.key"` | `lib/flezha/transport/cert_generator.ex` |
| `:quic_client_ca_path` | `"app_data/certs/ca.pem"` | `lib/flezha/transport/server.ex` |
| `:socket_files_path` | — | `lib/flezha/service/port_pilot.ex`, `lib/flezha/socket/support.ex` |
| `:external_processes` | — | `lib/flezha/service/port_pilot.ex` |
| `:concurrent_channel_count` | `0` | `lib/flezha/service/config_manager.ex` |

### Cluster and nodes

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

### State, WAL and durability

| Key | Default | Read in |
|---|---|---|
| `:state_root` | `FLEZHA_STATE_DIR`, else `"app_data/state"` (derived) | `lib/flezha/state/registry.ex` |
| `:default_compact_after_bytes` | `8 * 1024 * 1024` | `lib/flezha/state/persistence_resolver.ex` |
| `:compact_after_age_ms` | `300_000` | `lib/flezha/state/persistence_resolver.ex` |
| `:compaction_timeout_ms` | `30_000` | `lib/flezha/state/persistence_resolver.ex`; `0` disables the compaction-timeout sweep |
| `:record_log_cut_after_bytes` | `64 * 1024` | `lib/flezha/state/persistence_resolver.ex`; `0` rewrites at every release |
| `:default_time_travel_retention_hours` | `6` | `lib/flezha/state/persistence_resolver.ex` |
| `:default_workflow_completed_retention_hours` | `72` | `lib/flezha/state/persistence_resolver.ex` |
| `:default_output_stall_timeout_ms` | `60_000` | `lib/flezha/state/persistence_resolver.ex` |
| `:operative_persistence` / `:time_travel_retention_hours` / `:workflow_completed_retention_hours` / `:compact_after_bytes` / `:compact_after_age_ms` / `:output_stall_timeout_ms` / `:sync_durability` / `:credit_window_bytes` | `%{}` | `lib/flezha/state/persistence_resolver.ex` — per-class operator overrides, `%{"OperativeClass" => value}`, winning over the SDK declaration and the instance metadata |
| `:wal_high_watermark_bytes` | `16 * 1024 * 1024` | `lib/flezha/state/persistence_resolver.ex` — the one Entity-scale payload bound |
| `:wal_low_watermark_bytes` | half the high watermark (derived) | `lib/flezha/state/wal_writer.ex` |
| `:wal_hard_watermark_bytes` | 4 × the high watermark (derived) | `lib/flezha/state/wal_writer.ex` |
| `:wal_pending_msg_cap` | `5_000` | `lib/flezha/state/wal_writer.ex` |
| `:wal_resync_retry_interval_ms` | `1_000` | `lib/flezha/state/wal_writer.ex` |
| `:outbox_inflight_max` | `1_000` | `lib/flezha/state/wal_writer.ex` |
| `:outbox_resettle_ms` | `60_000` | `lib/flezha/state/wal_writer.ex` |
| `:source_credit_window_bytes` | `:wal_high_watermark_bytes` (derived) | `lib/flezha/state/input_log.ex` |
| `:credit_flow_control_enabled` | `true` | `lib/flezha/runner/stream_channel.ex` |
| `:credit_low_watermark_bytes` | half the credit window (derived) | `lib/flezha/runner/stream_channel.ex` |
| `:agent_fsync_interval_ms` | `5` | `lib/flezha/state/fsync_batcher.ex` |
| `:input_log_hold_window_ms` | `0` | `lib/flezha/operative/class_registry.ex`, `lib/flezha/state/input_log.ex` |
| `:time_travel_retention_check_interval_ms` | `60_000` | `lib/flezha/state/retention.ex` |
| `:restore_await_timeout_ms` | `15_000` | `lib/flezha/state/commands.ex` |
| `:state_resync_fun` | unset | `lib/flezha/state/recovery.ex` |
| `:durability_tier[:backend]` | `:none` | `lib/flezha/state/durability_tier.ex`, `lib/flezha/state/durability_tier/backend.ex` |
| `:durability_tier[:ship_interval_ms]` / `[:rpo_target_ms]` / `[:gc_interval_ms]` / `[:gc_grace_ms]` / `[:gc_abandoned_after_ms]` | `30_000` / `120_000` / `3_600_000` / `3_600_000` / `604_800_000` | `lib/flezha/state/durability_tier.ex` (a non-positive value falls back to the default) |
| `:durability_tier[:backend_opts]` | see the `FLEZHA_DURABILITY_PG_*` / `_S3_*` / `_PATH` rows | `lib/flezha/state/durability_tier/backend.ex` |

### Runner and operative lifecycle

| Key | Default | Read in |
|---|---|---|
| `:runner_keepalive` | `"30000"` (string or integer accepted) | `lib/flezha/runner/fleet_manager.ex` |
| `:auto_restore_enabled` | `true` | `lib/flezha/boot/recovery_hooks.ex`, `lib/flezha/service/hub_manager.ex` |
| `:boot_replay_timeout_ms` | `60_000` | `lib/flezha/boot_loader.ex`, `lib/flezha/runner/fleet_manager.ex` |
| `:process_hub_registry_root` | `"app_data/process_hub"` | `lib/flezha/service/hub_manager.ex`, `lib/flezha/state/durability_tier/artifact.ex` |
| `:process_hub_reconcile_grace_ms` / `:process_hub_reconcile_interval_ms` | `45_000` / `30_000` | `lib/flezha/service/hub_manager.ex` |
| `:hub_runner_assign_wait_ms` | `2_000` | `lib/flezha/service/hub_manager.ex` |
| `:migration_consent[:consent_timeout_ms]` / `[:retry_interval_ms]` / `[:max_defer_ms]` | `5_000` / `2_000` / `:graceful_shutdown_timeout_ms` (derived) | `lib/flezha/service/hub_manager.ex` |
| `:overwatch_call_timeout_ms` | `5_000` | `lib/flezha/service/operative_manager.ex` |
| `:overwatch_ping_enabled` | `true` | `lib/flezha/operative/lifecycle.ex` |
| `:overwatch_ping_timeout_ms` | `2_000` | `lib/flezha/runner/control_unit.ex` |
| `:overwatch_proc_warn_ms` | `2_000` | `lib/flezha/operative/overwatch.ex` |
| `:overwatch_retry_backoff_ms` | `[200, 500, 1_000, 2_000, 5_000, 10_000]` | `lib/flezha/operative/lifecycle.ex` |
| `:overwatch_runner_wait_ms` | `30_000` | `lib/flezha/operative/lifecycle.ex` |
| `:plugin_retry_base_delay_ms` | `4_000` | `lib/flezha/plugin/extension/table.ex` |
| `:correction_debounce_ms` | `200` | `lib/flezha/operative/replay.ex` |
| `:control_unit_heartbeat_interval_ms` | `15_000` | `lib/flezha/runner/control_unit.ex` |
| `:control_unit_pending_reply_timeout_ms` | `90_000` | `lib/flezha/runner/control_unit.ex` |

### Resilience and convergence

| Key | Default | Read in |
|---|---|---|
| `:control_liveness_window_ms` / `:control_liveness_interval_ms` | `45_000` / `15_000` | `lib/flezha/runner/control_channel.ex` |
| `:io_stream_liveness_interval_ms` / `:io_stream_liveness_threshold_ms` | `10_000` / `30_000` | `lib/flezha/runner/stream_channel.ex` |
| `:graceful_shutdown_enabled` | `true` | `lib/flezha/application.ex`, `lib/flezha/shutdown/coordinator.ex`, `lib/flezha/shutdown/signal_handler.ex` |
| `:graceful_shutdown_timeout_ms` | `60_000` | `lib/flezha/shutdown/coordinator.ex`, `lib/flezha/service/cli_manager.ex` |
| `:convergence_health_enabled` | `true` | `lib/flezha/application.ex` |
| `:convergence_tick_ms` | `5_000` | `lib/flezha/convergence/loop.ex`, `lib/flezha/healthz/server.ex` |
| `:convergence_output_stall_grace_ms` | `60_000` | `lib/flezha/convergence/loop.ex` |
| `:convergence_unmet_intent_grace_ms` | `120_000` | `lib/flezha/healthz/server.ex` |
| `:convergence_fabric_503_polls` | `3` | `lib/flezha/healthz/server.ex` |
| `:healthz_enabled` | `false` | `lib/flezha/application.ex` |
| `:healthz_port` / `:healthz_bind` | `9569` / `"127.0.0.1"` (`runtime.exs` sets `"0.0.0.0"`) | `lib/flezha/healthz/server.ex` |
| `:output_watchdog_interval_ms` | `10_000` | `lib/flezha/state/watchdog.ex` |
| `:catastrophic_dataflow_timeout_ms` | `60_000` | `lib/flezha/state/watchdog.ex` |

### Observability and audit

| Key | Default | Read in |
|---|---|---|
| `:observability[:enabled]` | `true` | `lib/flezha/observability.ex`, `lib/flezha/observability/supervisor.ex` |
| `:observability[:ring_buffer_retention_ms]` / `[:ring_buffer_max_entries_per_source]` | `900_000` / `10_000` | `lib/flezha/observability/ring_buffer_sink.ex` |
| `:observability[:overflow_policy]` | `:drop` | `lib/flezha/observability/registry.ex` |
| `:observability[:cardinality_budget_per_source]` | `1_000` | `lib/flezha/observability/registry.ex` |
| `:observability[:pubsub_sink_enabled]` | `true` (`false` in test) | `lib/flezha/observability/default_sinks.ex`, `lib/flezha/service/observability.ex` |
| `:observability[:data_plane_liveness]` | `true` | `lib/flezha/transport/liveness_counters.ex` |
| `:observability[:otp_backend_enabled]` | `true` (`false` in test) | `lib/flezha/observability/otp_backend/supervisor.ex` |
| `:observability[:otp_backend][:poll_intervals]` | `beam_vm: 1_000, host: 5_000, processhub: 1_000, flezha_transport: 1_000, flezha_bounded_mailbox: 1_000` | `lib/flezha/observability/otp_backend/source.ex` |
| `:observability_export[:prometheus_enabled]` / `[:prometheus_port]` / `[:prometheus_bind]` | `false` / `9568` / `"127.0.0.1"` | `lib/flezha/observability/export/supervisor.ex`, `lib/flezha/observability/export/prometheus_http.ex` |
| `:observability_export[:otlp_enabled]` / `[:otlp_endpoint]` / `[:otlp_headers]` | `false` / `nil` / `%{}` | `lib/flezha/observability/export/supervisor.ex`, `lib/flezha/observability/export/otlp_client.ex` |
| `:observability_export[:otlp_flush_interval_ms]` / `[:otlp_batch_max]` | `10_000` / `512` | `lib/flezha/observability/export/otlp_sink.ex` |
| `:audit_log[:enabled]` / `[:dir]` | `false` / `"app_data/audit"` | `lib/flezha/observability/audit_log/supervisor.ex`, `lib/flezha/observability/audit_log/writer.ex` |
| `:audit_log[:max_file_bytes]` / `[:max_file_age_seconds]` / `[:rotation_check_interval_ms]` | `67_108_864` / `86_400` / `60_000` | `lib/flezha/observability/audit_log/rotation.ex` |
| `:audit_log[:retention_seconds]` / `[:max_total_bytes]` / `[:retention_check_interval_ms]` | `30 * 86_400` / `4_294_967_296` / `3_600_000` | `lib/flezha/observability/audit_log/retention.ex` |
| `:audit_log[:fsync_mode]` / `[:fsync_interval_ms]` | `:per_record` / `100` | `lib/flezha/observability/audit_log/writer.ex` |
| `:audit_log[:extra_subscriptions]` | `[]` | `lib/flezha/observability/audit_log/sink.ex` |

### Security

| Key | Default | Read in |
|---|---|---|
| `:authn_authz[:enabled]` | `false` (`runtime.exs`: `true` in prod) | `lib/flezha/authz/config.ex`, `lib/flezha/application.ex` |
| `:authn_authz[:application_managed]` | `true` (`false` in test) | `lib/flezha/application.ex` — `false` starts no `Flezha.AuthZ.Supervisor` in the application tree |
| `:authn_authz[:auth_dir]` | `"app_data/auth"` | `lib/flezha/authz/config.ex` |
| `:authn_authz[:bootstrap_token_path]` | `"app_data/auth/bootstrap.token"` | `lib/flezha/authz/config.ex` |
| `:authn_authz[:token_ttl_default_seconds]` / `[:sdk_token_ttl_seconds]` | `86_400` / `2_592_000` | `lib/flezha/authz/config.ex` |
| `:authn_authz[:namespaces]` | `%{}` | `lib/flezha/authz/config.ex` — per-namespace quota overrides |

### Plugins, DLQ and schedules

| Key | Default | Read in |
|---|---|---|
| `:enabled_plugins` | the provider map in `config/config.exs` | `lib/flezha/service/config_manager.ex` |
| `:plugin_installer_enabled` | `false` | `lib/flezha/application.ex` |
| `:plugin_installer[:plugin_root]` | `"app_data/plugins"` | `lib/flezha/plugin/installer/store.ex`, `lib/flezha/plugin/installer.ex` |
| `:plugin_installer[:trusted_keys]` / `[:require_signature]` / `[:allow_unsigned]` | `[]` / `false` / `true` | `lib/flezha/plugin/installer.ex`, `lib/flezha/plugin/installer/loader.ex` |
| `:plugin_installer[:max_package_bytes]` / `[:max_source_files]` | `50_000_000` / `200` | `lib/flezha/plugin/installer.ex` |
| `:plugin_installer_data_dir` | unset | `lib/flezha/plugin/installer/store.ex` — overrides `plugin_root` when set |
| `:dlq[:dir]` | `"app_data/dlq"` | `lib/flezha/dlq/store.ex` |
| `:dlq[:retention_seconds]` / `[:max_total_bytes]` / `[:retention_check_interval_ms]` | `30 * 86_400` / `4_294_967_296` / `3_600_000` | `lib/flezha/dlq/store.ex` |
| `:dlq[:replay_grace_ms]` | `5_000` | `lib/flezha/service/cli_manager.ex` — how long `flz dlq replay` waits for a re-park |
| `:schedules[:dir]` / `[:misfire_grace_ms]` | `"app_data/schedules"` / `120_000` | `lib/flezha/schedule/store.ex`, `lib/flezha/schedule/job.ex` |
| `:schedules[:application_managed]` | `true` (`false` in test) | `lib/flezha/application.ex` |

### Other

| Key | Default | Read in |
|---|---|---|
| `:env` | `Mix.env()` (`config/config.exs`) | `lib/flezha/operative/handover.ex`, `lib/flezha/runner/control_unit.ex` |
| `:pre_hooks` / `:post_hooks` | `[]` | `lib/flezha/boot_loader.ex` |
| `:testnet_enabled` | `true` in dev/test, `false` in prod (`config/<env>.exs`) | `lib/flezha/application.ex` |

## Layer 3: operative declarations (SDK)

Declared in code and shipped to the controller at registration: inside
`channels()` on a stream operative, or as trait/interface overrides on a
Procedure or Workflow class. Rust names first, Java in parentheses. The Elixir
SDK carries the channel and edge options and `mailbox_capacity/0`; it has no
delivery-tier or workflow declarations.

### Delivery and acknowledgement (stream operatives)

| Declaration | Default if absent | What it does |
|---|---|---|
| `effectively_once()` (`effectivelyOnce`) | off — at-least-once | Every emit rides an atomic effect record through the WAL, and the outbox cursor advances only on a sink's confirmation. Registration refuses it unless every plugin-bound channel is `.keyed()`. |
| `checkpoint_ack()` (`checkpointAck`) | off — acknowledge per input | Hold the consumed offset until the interval closes, so state and offset advance together. Requires `effectively_once()`. |
| `checkpoint_every(n)` (`checkpointEvery`) | absent — only `ctx.checkpoint()` closes the interval | Close the interval after `n` inputs; an explicit checkpoint still closes it early. Implies `checkpoint_ack()`. |
| `outputs_at_checkpoint(max_delay_ms)` (`outputsAtCheckpoint`) | absent — outputs flush as produced | Hold the interval's outputs and ship them inside its checkpoint record as one batch. `max_delay_ms` is required and bounds how long a quiet Entity holds an output. Implies `checkpoint_ack()`. |
| `correction_window(window_ms)` (`correctionWindow`) | absent — a late input is just an input | A late input inside the window rewinds the Entity to a baseline and replays the affected slice in event-time order. Needs an `EventTimeFrom` clock. |
| `beyond_window_policy(policy)` (`beyondWindowPolicy`) | `ProcessAsArrival` | Fate of an input later than the window: process it as an arrival, or `Drop` it. |

`outputs_at_checkpoint` is a dial, not a switch: the interval closes on
whichever comes first of `checkpoint_every(n)` inputs or `max_delay_ms`, so you
add up to `max_delay_ms` of median output latency and buy one WAL append per
interval instead of one per invocation. Leave it undeclared for the
latency-optimal setting; declare a small delay when throughput near the ceiling
matters more. How the numbers trade: [`operations/benchmarking.md`](operations/benchmarking.md).

### Channel and edge options

Declared on the stream handle returned by `input(...)` or a node's output.

| Declaration | Default | What it does |
|---|---|---|
| `.on_full(OverflowPolicy)` (`onFull`) | `Block` | What a full Entity mailbox does: `Block` (back-pressure), `DropNewest`, `DropOldest`. Nothing drops by default. |
| `.retry_max(n)` (`retryMax`) | `3` attempts | Attempts before a failing payload is parked to the dead-letter queue. The Elixir SDK refuses `0`. |
| `.retry_backoff_ms(ms)` (`retryBackoffMs`) | `50` | Base of the exponential backoff between attempts; the wait holds no thread. |
| `.keyed()` (`keyed`) | off | Every plugin emit must carry a natural key; the runtime stamps `flz.entity` / `flz.seq` / `flz.key`. Requires a `sink`; required on every plugin channel under `effectively_once()`. |
| `.key_by(f)` (`keyBy`) | absent — one Entity | Route by a key function, making the consumer dynamic-entity. |
| `.sink(name)` / `.sink_with(name, f)` (`sink`) | — | Wire the channel to a named plugin; a second sink on one channel is a registration error. |
| `.sealed()` (`sealed`) | — | Type-level statement that nothing follows the channel; touches no builder state. |

### Procedure-level options

Overridden on the `Procedure` trait (Rust) or Procedure class (Java).

| Override | Default | What it does |
|---|---|---|
| `compact_after_bytes()` (`compactAfterBytes`) | 8 MiB, in lockstep with `:default_compact_after_bytes` | Per-Entity WAL size reported to the controller as the snapshot-compaction trigger. |
| `mailbox_capacity()` (`mailboxCapacity`) | `1024`, range `1..=1_048_576` | Bounded mailbox depth per Entity; precedence is the override, then `FLEZHA_MAILBOX_CAPACITY`, then the default. Rust overrides must also set `MAILBOX_CAPACITY_OVERRIDDEN = true`. |
| `entity_idle_ttl()` (`entityIdleTtl`) | `None` — no eviction | Evict a dynamic Entity after this idle period; eviction is destructive (slot, WAL and snapshot dropped). |
| `max_entities()` | `100_000` | Soft cap on live dynamic Entities per Procedure; new keys past it are dropped. |
| `type Clock` | — (must be declared) | `WallClock`, `EventTimeFrom<..>` or `LogicalClock`; decides `ctx.now()` and event-time ordering. |

### Workflow class options

Overridden on the `WorkflowOperative` trait (Rust) or interface (Java).

| Override | Default | What it does |
|---|---|---|
| `max_duration()` (`maxDuration`) | none | Wall-clock bound on one run. |
| `self_terminating()` (`selfTerminating`) | `true` | Whether the run ends on its own or waits for a stop. |
| `loop_mode()` (`loopMode`) | `Once` | `Once`, `PerInput` or `Chained` looping over the durable inbox. |
| `wake_after()` (`wakeAfter`) | none | Deadline at which a parked loop rehydrates without input. |
| `retry_attempts()` / `retry_backoff()` (`retryAttempts` / `retryBackoff`) | `3` / `500 ms` | Pre-commit retry ladder for a failing proc. |
| `barrier_retry_attempts()` (`barrierRetryAttempts`) | `3` | Re-sends of a proc-commit barrier whose ack timed out before the run ends abnormally. |
| `rollback_mode()` (`rollbackMode`) | `AllOrNothing` | How a rewind undoes journaled procs: `AllOrNothing` or `KeepProgress`. |
| `PERSISTENCE_ENABLED` (`persistenceEnabled`) | `true` | `false` keeps the instance in memory only: a crash re-runs from the top. |
| `SYNC_DURABILITY` (`syncDurability`) | `false` | Ack a commit barrier only after its remote ship (RPO 0); inert without a durability backend, workflow classes only. |

## Layer 4: plugin options

Passed in the plugin's `opts` map at install time. The Kafka producer
(`operative_kafka_producer`) carries the most:

| Option | Default | What it does |
|---|---|---|
| `:hosts` / `:topic` | required | Broker list and destination topic. `:partition` defaults to `0`. |
| `:queue_overflow` | `:block` | What a full producer queue does: `:block`, `:drop_oldest`, `:drop_newest`. A configured drop of a delivery-guaranteed record is alarm-grade and does not advance the outbox cursor. |
| `:queue_max` | `10_000` | Bounded producer queue depth. |
| `:queue_block_timeout_ms` | `5_000` | How long a synchronous caller parks on a full queue. |
| `:linger_ms` | `10` | Batch linger before a produce request. |
| `:max_batch_bytes` | `1_048_576` | Produce-request size ceiling. |
| `:required_acks` | `-1` | Broker acknowledgement level (`acks=all`). |
| `:idempotent` | `false` | Producer idempotence (broker sequence numbers). |
| `:max_in_flight` | `5` with `:idempotent`, else `1` | Outstanding produce requests per partition; a width above `1` without `:idempotent` is refused at preparation. |
| `:retries` / `:retry_backoff_ms` / `:retry_max_backoff_ms` | `5` / `100` / `5_000` | Retry ladder before a batch parks to the DLQ. |
| `:delivery_timeout_ms` | `30_000` | Total budget for one record's delivery. |
| `:dispatch_timeout_ms` / `:request_timeout_ms` | `10_000` / `10_000` | Per-attempt send budget and broker-side request timeout. |
| `:metadata_refresh_timeout_ms` | `5_000` | Bound on a metadata refresh. |
| `:auto_create_topic` / `:auto_create_partitions` / `:auto_create_replication_factor` / `:topic_configs` | `true` / `1` / `2` / `[]` | Topic auto-creation. |
| `:auth` / `:sasl_password_env` / `:use_ssl` / `:ssl_options` | unset | Broker authentication; the password is named by variable, never carried in `opts`. |

The other first-party providers and their option sets:

| Provider name | Page |
|---|---|
| `operative_postgres` | `:database` / `:username` required; `:hostname` `localhost`, `:port` `5432`, `:auto_create` `false`, `:linger_ms` `25`, `:max_batch` `500`, `:queue_max` `10_000`, `:retry_backoff_ms` `250`, `:retry_max_backoff_ms` `5_000`, `:ssl` `false`, `:unhealthy_after_ms` `30_000` |
| `operative_http_poll` | [`plugins/http_poll.md`](plugins/http_poll.md) |
| `operative_websocket` | [`plugins/websocket_subscription.md`](plugins/websocket_subscription.md) |
| `operative_forward` | [`plugins/operative_forward.md`](plugins/operative_forward.md) |
| `operative_message_forward` | [`plugins/message_handlers.md`](plugins/message_handlers.md) |
| `operative_beam_msg` (`:include_metadata` `false`), `operative_stdout`, `operative_io_filter` (`:regex_pattern` required, `:inverted` `false`), `monitored_io_forward` (`:recv_pid` required, `:heartbeat_interval` `5000`) | [`plugins/lifecycle.md`](plugins/lifecycle.md) |

## See also

- [`operations/deployment.md`](operations/deployment.md) — the variables a deploy cannot omit, and how to run a node
- [`resilience/data_durability.md`](resilience/data_durability.md) — what the durability knobs mean for correctness
- [`architecture/kernel.md`](architecture/kernel.md) — where the WAL, outbox and fsync knobs sit in the data path
- [`operations/authn_authz.md`](operations/authn_authz.md) — the posture behind the authz keys
- [`plugins/kafka_producer.md`](plugins/kafka_producer.md) — the Kafka producer's options in depth
- [`operations/benchmarking.md`](operations/benchmarking.md) — measuring the effect of a change to any of these
