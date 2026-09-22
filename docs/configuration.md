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
| 3. Operative declarations | the SDK, in the operative's `channels()` and Tasklet/Workflow class | one operative class | redeploy the runner — frozen at registration |
| 4. Plugin options | the plugin's `opts` map when it is installed | one plugin instance | re-install the plugin |

Underneath all four sits the Erlang VM itself, which is configured before the
application starts and so cannot be a row of any table below: its flags are
[their own section](#erlang-vm-arguments) inside Layer 1, because an operator
sets them the same way and pays the same restart to change one.

Layers 1 and 2 overlap on purpose: most environment variables exist only to
set an application-env key at boot. **Both layers are one table**:
`lib/flezha/config/keys.ex` declares every knob once — its name, the
application-env key, the environment variable, the type, the default, the
runtimes that read it and the surface an operator observes it on — and
`config/runtime.exs` is one call over that table rather than a translation of
its own. Prefer the variable in a container, the config file for an embedded or
local node.

Layer 3 is the one that surprises people. Anything an operative declares —
its delivery tier, its acknowledgement granularity, its correction window — is
frozen when the class registers with the controller, and there is no
operator-facing override: these change the semantics of the operative's
output, not just its performance, so they belong with the code that relies on
them. The effectively-once, checkpoint and output-batching options are all
[Layer 3](#layer-3-operative-declarations-sdk).

## Where files land

Ten subtrees are all this product writes, and `lib/flezha/layout/table.ex`
declares them: for each, the knob above that may move it, whether a directory
claim is owed before the first write, and how `flz reset` treats it. Both
runtimes read that declaration — the controller through
`Flezha.Layout.Table.dir/1`, `flz reset` through the matching rows in
`cli/src/reset/plan.rs` — so at the default settings the directory the
controller uses and the one an operator's reset reports are one path. A knob
that moves a subtree moves it for the controller only.

| Subtree | Knob | `flz reset` |
|---|---|---|
| state | `FLEZHA_STATE_DIR` | cleared by default |
| process_hub | `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | cleared by default |
| certs | `FLEZHA_CA_CERT_PATH` and its siblings | `--include-certs` |
| dlq | `FLEZHA_DLQ_DIR` | `--include-dlq` |
| schedules | `FLEZHA_SCHEDULE_DIR` | `--include-schedules` |
| auth | `FLEZHA_AUTHZ_DIR` | `--include-auth` |
| audit | `FLEZHA_AUDIT_DIR` | `--include-audit` |
| plugins | `FLEZHA_PLUGIN_ROOT` | `--include-plugins` |
| sockets | `FLEZHA_SOCKET_FILES_PATH` (the ladder) | `--include-sockets` |
| logs | `FLEZHA_LOG_PATH` | never |

Each knob's default and precedence are its row in the table below; this section
states only which subtrees exist and what reset does with them. `flz reset`
reports every row, the `never` row as kept, and reads none of these knobs but the
ladder's: it clears `<app_data root>/dlq`, not a directory `FLEZHA_DLQ_DIR`
moved it to. `--all` sets every flag in the table, so it also removes every
credential and erases the audit trail; see [`operations/reset.md`](operations/reset.md)
for the window until the fresh bootstrap token is claimed.
Two rows are not subdirectories of the data root. `sockets` is resolved by
the ladder described under
[the socket-directory ladder](#the-socket-directory-ladder), is frequently outside
`app_data/` altogether, and `--app-data-root` does not move it. `logs` defaults
to `logs/`, relative to the working directory, and the data root does not move
it either.

## Layer 1: environment variables

Read at boot by `config/runtime.exs`, which asks the table, unless the notes say
otherwise. Booleans accept `1`/`true`/`yes`/`on` and `0`/`false`/`no`/`off`
case-insensitively; a value that is neither leaves the default standing, as does
a count that is zero, negative or not a number. Every other type — a duration,
an address, a member of an enumeration — refuses the boot naming the variable,
the value and the type.

### Transport and TLS

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_TRANSPORT` | `uds` | `:transport`. `uds`, `quic` or `native` (the in-node Elixir SDK transport, never chosen implicitly); any other value is refused at boot |
| `FLEZHA_TRANSPORTS` | the value of `FLEZHA_TRANSPORT` (`uds,quic` in test) | `:transports`, comma list validated against the same three names |
| `FLEZHA_TRANSPORT_AUTH` | `true` (required) | `:transport_auth_required`; `off`/`false`/`0`/`no` disables peer authentication and is warned on every boot |
| `FLEZHA_QUIC_LISTEN_ADDRESS` | `0.0.0.0:4433` (`:test` derives the port from the run) | `:quic_listen_address`, the address the controller listens on (a runner's dial list is `FLEZHA_QUIC_ADDRESSES`, below) |
| `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` | `:ca_cert_path` |
| `FLEZHA_SERVER_CERT_PATH` | `app_data/certs/server.pem` | `:server_cert_path` |
| `FLEZHA_SERVER_KEY_PATH` | `app_data/certs/server.key` | `:server_key_path` |
| `FLEZHA_CLIENT_CERT_PATH` | `app_data/certs/client.pem` | `:client_cert_path` |
| `FLEZHA_CLIENT_KEY_PATH` | `app_data/certs/client.key` | `:client_key_path` |
| `FLEZHA_QUIC_CLIENT_CA` | `app_data/certs/ca.pem` | `:quic_client_ca_path`, trust anchor for QUIC client certificates |
| `FLEZHA_SOCKET_FILES_PATH` | resolved by the socket ladder (below) | the UDS socket directory. Set, it is used outright; unset, the controller creates the first ladder candidate it can. **At most 60 bytes**: a longer directory refuses the boot, naming the directory, its length and this variable, because a runner's data socket would not fit beside it (see the ladder below) |
| `FLEZHA_SOCKET_DIR_GROUP` | unset (owner-only) | widens UDS traversal to one named group: the directory becomes `0750` owned by that group, warned on every boot. A group the controller's user does not belong to refuses the boot |
| `FLEZHA_CONCURRENT_CHANNEL_COUNT` | `0` | `:concurrent_channel_count` |
| `FLEZHA_EXTERNAL_PROCESSES` | `""` | `:external_processes`, comma list of commands the controller spawns (test mode hard-codes the Rust test runner) |
| `FLEZHA_CONTROLLER_ENABLED` | `true` | `:controller_enabled`; `false`/`0`/`no`/`off` boots a runner-only node with no controller tree |

> **Two suites on one checkout.** In `:test` the state root, the UDS socket
> directory, the QUIC listen port, the testnet daemon's WS port and the
> distribution node name all derive from the OS process running the suite, so a
> second session isolates itself by existing. Setting `FLEZHA_STATE_DIR`,
> `FLEZHA_QUIC_LISTEN_ADDRESS`, `FLEZHA_TESTNET_WS_PORT` or `FLEZHA_NODE_NAME`
> still wins, which is how a scenario that needs a known address or name asks
> for one.

### Cluster and nodes

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_CLUSTER_DISCOVERY` | `manual_discovery` | `:cluster_discovery`; also `k8s_pod_discovery` |
| `FLEZHA_NODE_NAME` | `flezha` (`:test` derives a base per run) | `:node_base_name`; `rel/env.sh.eex` builds `RELEASE_NODE=<name>@<host>` from it, and the test harness starts distribution under the same `<name>@<host>` |
| `FLEZHA_NODE_HOST` | `127.0.0.1` | the host half of `RELEASE_NODE` (`rel/env.sh.eex`); also sets `:node_host_name`, the host half the test harness names its node with |
| `RELEASE_COOKIE` | minted per node | read by `rel/env.sh.eex`: set → `RELEASE_DISTRIBUTION=name`; unset → a node start (`start`, `start_iex`, `daemon`, `daemon_iex`) mints 32 random bytes into `FLEZHA_COOKIE_FILE` (default `<state root>/cookie`, `0600`), reuses it on later boots, and only where neither is possible → `none` (no clustering, `flz` verbs fail). Every other command, each `flz` verb included, only reads the file, and before the first start reports the controller unreachable. A cluster still provisions one value on every node. |
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
| `FLEZHA_STATE_DIR` | `app_data/state` (`:test` derives a root per run) | the state root. Like every variable here it fills its key once, at boot, in `config/runtime.exs`; `Flezha.State.Paths.state_root/0` then reads `:state_root` and resolves it to an absolute path. Setting the variable in a live BEAM changes nothing. A relative value still resolves against the directory the controller was started from, which the boot and `flz controller status` both name |
| `FLEZHA_COMPACT_AFTER_BYTES` | `8388608` | `:default_compact_after_bytes` |
| `FLEZHA_COMPACT_AFTER_AGE_MS` | `300000` (`0` in test) | `:compact_after_age_ms`; `0` disables the age trigger |
| `FLEZHA_INPUT_LOG_HOLD_WINDOW_MS` | `0` | `:input_log_hold_window_ms` — a floor; a declared `correction_window` raises its own hold above it |
| `FLEZHA_CREDIT_FLOW_CONTROL` | `true` | `:credit_flow_control_enabled` |
| `FLEZHA_RETENTION_CHECK_INTERVAL_MS` | `60000` | `:time_travel_retention_check_interval_ms` (not in test) |
| `FLEZHA_WORKFLOW_RETENTION_HOURS` | `72` | `:default_workflow_completed_retention_hours` (not in test) |
| `FLEZHA_DURABILITY_BACKEND` | `none` (prod warns when it was defaulted) | `:durability_tier[:backend]`; `none`, `postgres`, `s3`, `local_path`; any other spelling resolves to `none` and warns. A selected backend missing an option it cannot work without refuses the boot naming it |
| `FLEZHA_DURABILITY_WRITER_ID` | `node()` | read at runtime by `Flezha.State.DurabilityTier.writer_id/0`; the stable lineage-writer identity for a controller whose node name changes on reschedule |
| `FLEZHA_DURABILITY_SHIP_INTERVAL_MS` | `30000` | `:durability_tier[:ship_interval_ms]` |
| `FLEZHA_DURABILITY_RPO_TARGET_MS` | `120000` | `:durability_tier[:rpo_target_ms]` |
| `FLEZHA_DURABILITY_GC_INTERVAL_MS` | `3600000` | `:durability_tier[:gc_interval_ms]` |
| `FLEZHA_DURABILITY_GC_GRACE_MS` | `3600000` | `:durability_tier[:gc_grace_ms]` |
| `FLEZHA_DURABILITY_GC_ABANDONED_AFTER_MS` | `604800000` | `:durability_tier[:gc_abandoned_after_ms]` |
| `FLEZHA_DURABILITY_PATH` | unset | `backend_opts[:path]` for `local_path` — a directory that outlives the node's own disk |
| `FLEZHA_DURABILITY_PG_HOST` | `localhost` | `backend_opts[:hostname]` for `postgres` |
| `FLEZHA_DURABILITY_PG_PORT` | `5432` | `backend_opts[:port]` for `postgres` |
| `FLEZHA_DURABILITY_PG_DATABASE` | `flezha` | `backend_opts[:database]` for `postgres` |
| `FLEZHA_DURABILITY_PG_USERNAME` | `flezha` | `backend_opts[:username]` for `postgres` |
| `FLEZHA_DURABILITY_PG_PASSWORD_ENV` | unset | names the variable holding the password — never the password itself |
| `FLEZHA_DURABILITY_PG_SSL` | `false` | `backend_opts[:ssl]` for `postgres` |
| `FLEZHA_DURABILITY_PG_TABLE` | `flezha_durability_artifacts` | `backend_opts[:table]` for `postgres` |
| `FLEZHA_DURABILITY_PG_AUTO_CREATE` | `true` | `backend_opts[:auto_create]` for `postgres` |
| `FLEZHA_DURABILITY_S3_ENDPOINT` | unset | `backend_opts[:endpoint]` for `s3` |
| `FLEZHA_DURABILITY_S3_BUCKET` | unset | `backend_opts[:bucket]` for `s3` |
| `FLEZHA_DURABILITY_S3_ACCESS_KEY_ID` | unset | `backend_opts[:access_key_id]` for `s3` |
| `FLEZHA_DURABILITY_S3_SECRET_ENV` | unset | names the variable holding the secret key |
| `FLEZHA_DURABILITY_S3_REGION` | `us-east-1` | `backend_opts[:region]` for `s3` |
| `FLEZHA_DURABILITY_S3_PREFIX` | `flezha` | `backend_opts[:prefix]` for `s3` |

### Runner and operative lifecycle

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_RUNNER_KEEPALIVE` | `30000` | `:runner_keepalive`; how long a disconnected runner is kept before removal, `0` removes at once |
| `FLEZHA_RUNNER_RETIRING_MARK_TTL_MS` | `30000` | `:runner_retiring_mark_ttl_ms`; how long a controller-initiated disconnect stays marked, so the close it produces is reported as expected rather than as an incident. A runner id is reused across reconnects, so the mark expires: past this window a close is diagnosed as unexpected again |
| `FLEZHA_GROUP_WITHDRAW_GRACE_MS` | `5000` | `:group_withdraw_grace_ms`; how long this controller keeps declaring a factory to the namespace hub after its last runner for it goes. A runner back inside the grace moves no operative; past it the hub reassigns them to a controller that still runs the factory. A disconnect the controller asked for skips the grace |
| `FLEZHA_PLACEMENT_LOAD_CAP_PERCENT` | `100` | `:placement_load_cap_percent`; the fairness cap on runner selection, as a percentage of the candidate set's fair share. At `100` the cap is exactly the fair share, so a namespace holding fewer operatives than it has runners still uses all of them; a value high enough that no candidate reaches the cap is plain rendezvous hashing |
| `FLEZHA_MIGRATION_CONSENT_TIMEOUT_MS` | `5000` | `:migration_consent[:consent_timeout_ms]` |
| `FLEZHA_MIGRATION_RETRY_INTERVAL_MS` | `2000` | `:migration_consent[:retry_interval_ms]` |
| `FLEZHA_MIGRATION_MAX_DEFER_MS` | unset → the graceful-shutdown timeout (derived) | `:migration_consent[:max_defer_ms]` |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | `app_data/process_hub` | `:process_hub_registry_root` (not in test) |
| `FLEZHA_PROCESS_HUB_CLUSTER_SETTLE_MS` | `2000` | `:process_hub_cluster_settle_ms` (not in test) |
| `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` | `45000` | `:process_hub_reconcile_grace_ms` (not in test) |
| `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS` | `30000` | `:process_hub_reconcile_interval_ms` (not in test) |
| `FLEZHA_BOOT_REPLAY_TIMEOUT_MS` | `60000` | `:boot_replay_timeout_ms` (not in test) |
| `FLEZHA_AUTO_RESTORE_ENABLED` | `true` | `:auto_restore_enabled` (not in test); true only for `true`/`1` |

### Resilience and convergence

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_CONTROL_LIVENESS_WINDOW_MS` | `45000` | `:control_liveness_window_ms` — the `control_liveness_window_ms` row of `Flezha.Config.Durations`, derived as 3 × the keepalive interval, so leaving it unset keeps the pair; it is the same window the runner's `FLEZHA_CONTROL_IDLE_TIMEOUT_MS` declares |
| `FLEZHA_CONTROL_LIVENESS_INTERVAL_MS` | `15000` | `:control_liveness_interval_ms` |
| `FLEZHA_OVERWATCH_PING_TIMEOUT_MS` | `2000` | `:overwatch_ping_timeout_ms`; bounds the pre-start `:ping_runner` round-trip, after which the Overwatch retries rather than cutting the channel |
| `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` | `true` | `:graceful_shutdown_enabled`; when set, anything but `1`/`true`/`yes`/`on` is false |
| `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` | `60000` | `:graceful_shutdown_timeout_ms`; the orchestrator grace period is the `orchestrator_grace_ms` row — this + 15 s — and a test reads it back from the DaemonSet and both compose files |
| `FLEZHA_OUTPUT_WATCHDOG_INTERVAL_MS` | `10000` | `:output_watchdog_interval_ms` (not in test) |
| `FLEZHA_USE_CONVERGENCE_HEALTH` | `true` | `:convergence_health_enabled` (not in test); true only for `true`/`1` |
| `FLEZHA_CONVERGENCE_TICK_MS` | `5000` | `:convergence_tick_ms` (not in test) |
| `FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS` | `60000` | `:convergence_output_stall_grace_ms` (not in test) |
| `FLEZHA_CONVERGENCE_UNMET_INTENT_GRACE_MS` | `120000` | `:convergence_unmet_intent_grace_ms` (not in test) |
| `FLEZHA_CONVERGENCE_CELL_STALL_MS` | `300000` | `:convergence_cell_stall_ms` (not in test) |
| `FLEZHA_FABRIC_503_POLLS` | `3` | `:convergence_fabric_503_polls` (not in test); consecutive classifier polls reporting `driver_loop_frozen` / `nif_queue_backing_up` before `/healthz` answers 503; `0` disables the fabric 503 |
| `FLEZHA_HEALTHZ_ENABLED` | `false` | `:healthz_enabled` (not in test); true only for `true`/`1`. The endpoint folds the convergence loop's verdict records: `degraded` names every reason it folded (`parked`, `dead_runner`, `plugin_unhealthy`, `output_stalled`, `intent_mismatch`, `sensor_stale`, `data_plane_wedged`, joined with `+`), and the body carries `unknown_operatives` |
| `FLEZHA_HEALTHZ_PORT` | `9569` | `:healthz_port` (not in test) |
| `FLEZHA_HEALTHZ_BIND` | `0.0.0.0` | `:healthz_bind` (not in test) |
| `FLEZHA_DATA_PLANE_LIVENESS` | `true` | `:observability[:data_plane_liveness]`; `false` drops the driver poll and the stall classifier, the counters stay |
| `FLEZHA_MEMORY_REPORT_INTERVAL_MS` | unset | `:memory_report_interval_ms`; unset or non-positive starts no reporter. See [`operations/memory_drift.md`](operations/memory_drift.md) |
| `FLEZHA_TESTNET_WS_PORT` | `4500` (`:test` derives the port from the run) | `:testnet_ws_port`, the port `Flezha.Testnet` binds; the prod image entrypoint starts the daemon only when it is set explicitly |

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
| `FLEZHA_LOG_LEVEL` | `info` (controller), `warn` (runner) | `:logger, :file_log` level for the controller; the runner default is the level every SDK starts its own diagnostics at |
| `FLEZHA_LOG_MAX_BYTES` | `10485760` | `:logger, :file_log` rotation size |
| `FLEZHA_LOG_KEEP_FILES` | `5` | `:logger, :file_log` rotated files kept |

### Security

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_AUTHZ_ENABLED` | `true` in prod, `false` otherwise | `:authn_authz[:enabled]`; `Flezha.AuthZ.Config` also reads the variable live, so it wins over the config file. `false` warns on every boot |
| `FLEZHA_AUTHZ_DIR` | `app_data/auth` | `:authn_authz[:auth_dir]`, also read live by `Flezha.AuthZ.Config` |
| `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH` | `<FLEZHA_AUTHZ_DIR>/bootstrap.token` | `:authn_authz[:bootstrap_token_path]`, also read live |
| `FLEZHA_AUTHZ_TOKEN_EXPIRY_WARNING_MS` | `86400000` | `:authn_authz[:token_expiry_warning_ms]` — how far ahead of its expiry an entry is announced as `:token_expiring` |
| `FLEZHA_NAMESPACE_SECRET` | — | The credential a runner presents at connect to prove the namespace it names. Read by every SDK, never by the controller |

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
| `FLEZHA_FILE_PLUGIN_ROOT` | `app_data/files` | `:file_plugin_root`, the one folder, or the one `s3://<bucket>/<prefix>`, every file plugin path must resolve inside. A folder is resolved with symbolic links followed; the first file plugin enable creates the default folder, and a folder set here must already exist, or every enable is refused naming it. An `s3://` root is never created and takes the four settings below |
| `FLEZHA_FILE_PLUGIN_S3_ENDPOINT` | unset | `:file_plugin_s3[:endpoint]`, the S3-compatible endpoint an `s3://` root is on; required under one |
| `FLEZHA_FILE_PLUGIN_S3_REGION` | `us-east-1` | `:file_plugin_s3[:region]` |
| `FLEZHA_FILE_PLUGIN_S3_ACCESS_KEY_ID` | unset | `:file_plugin_s3[:access_key_id]`; required under an `s3://` root |
| `FLEZHA_FILE_PLUGIN_S3_SECRET_ENV` | unset | names the variable holding the secret key; required under an `s3://` root. None of the four falls back to `FLEZHA_DURABILITY_S3_*`, and a root overlapping the durability tier's bucket prefix is refused |

Each `FLEZHA_PLUGIN_*` name is a row of the table like every other knob, so an
unset one leaves the row's own default standing rather than the family being
skipped as a block.

### Erlang VM arguments

The VM the node runs in is configured before the application starts, so none of
these can be a row of `lib/flezha/config/keys.ex` and `config/runtime.exs` never
sees one. They behave like the rest of Layer 1 in the way that matters to an
operator (set them in the process environment or the deploy manifest, restart
to change one) and they are listed here because nothing above can express them.

**What the release sets.** The launcher reads two files: the start commands take
`vm.args`, and `rpc` and `remote` take `remote.vm.args`, which is the node every
`flz` verb starts to make its one call. A gate holds these tables and the two
templates to each other, so a flag cannot ship undocumented.

<!-- vm-args:rel/vm.args.eex:begin -->

| `rel/vm.args.eex` | Why |
|---|---|
| `+sbwt short` | A scheduler with no work stops spinning for it sooner. Measured against the default over paired rounds of the regression bench: **21% less CPU** for the same run, with no throughput difference either window could resolve. It is `short` and not `none` because `none` saves barely more and costs four times as much setup latency; both numbers are in the table below. |
| `+sbwtdcpu short` | The same, for the dirty CPU schedulers. |
| `+sbwtdio short` | The same, for the dirty IO schedulers, which is where every transport NIF call that can block is scheduled. |
| `+Q 65536` | The ceiling on ports: plugin sockets, runner connections, distribution. Pinned because the default moves with whichever ERTS is underneath, 65 536 on the image's OTP 27 and 1 048 576 on OTP 29, so the ceiling is a decision rather than a property of the runtime that happened to be built in. The table is allocated up front at about 16 bytes a slot, so pinning at the image's own value costs nothing there and returns **15 MB of RSS** on the larger default. Raise it if port exhaustion ever appears. |
| `+C multi_time_warp` | Already the default on both OTP 27 and OTP 29, pinned so a future ERTS cannot move it. The keepalive roster compares a monotonic epoch, which is only well defined in this mode. Costs nothing: it is the mode the VM already chooses. |
| `+Bd` | Ctrl-C ends an attached controller at once. With the default break handler SIGINT opens the `BREAK:` menu, which pauses the whole VM: the prompt comes back while the node stays alive holding its name and its socket, and every `flz` verb answers `:noconnection`. It is a hard stop with no drain, recovered on the next boot as a crash would be; SIGTERM and `flz controller stop` drain. |

<!-- vm-args:rel/vm.args.eex:end -->

<!-- vm-args:rel/remote.vm.args.eex:begin -->

| `rel/remote.vm.args.eex` | Why |
|---|---|
| `+S 1:1` | This node connects to the controller, evaluates there and exits. It was being given the host's full scheduler complement to do that. |
| `+SDio 1` | It opens no transport, so the dirty IO pool has nothing to serve. |
| `+Q 65536` | The same ceiling, pinned for the same reason. This node opens two ports and was allocating a table for a million of them: **15 MB of RSS**, and a tenth of the node's startup, on every `flz` verb. |
| `+Bd` | Ctrl-C ends the verb. With the default break handler SIGINT opens the `BREAK:` menu and the node stays up, so a verb that streams, such as `flz operative listen`, could not be interrupted. |

<!-- vm-args:rel/remote.vm.args.eex:end -->

**Which spelling wins.** Three environment variables claim to set VM flags and
only two of them do. Measured against an args file setting `+S 3`:

| What you set | Result |
|---|---|
| nothing | `3`, the args file is read |
| `ERL_FLAGS="+S 5"` | `5`, wins |
| `ERL_ZFLAGS="+S 5"` | `5`, wins |
| `ELIXIR_ERL_OPTIONS="+S 4"` | `3`, **ignored** |
| both `ELIXIR_ERL_OPTIONS` and `ERL_FLAGS` | `ERL_FLAGS` |

One rule explains all of it: the flags are concatenated and the last occurrence
wins, `ELIXIR_ERL_OPTIONS` is placed before the args file and `ERL_FLAGS` after
it. So `ELIXIR_ERL_OPTIONS` still adds a flag the file does not set, which is
how `rel/env.sh.eex` adds TLS distribution, and it can never override one the
file does set. **Use `ERL_FLAGS`.**

**Setting them.**

| Deployment | Route |
|---|---|
| A pulled image | `docker run -e ERL_FLAGS="+S 4:4"`, or the commented `ERL_FLAGS` key in `kubernetes/flezha-configmap.yaml`, which the pod already reads through `envFrom` |
| A read-only root filesystem | Mount an args file and set `RELEASE_VM_ARGS` to its path. The image's own file cannot be edited in place, and the shipped DaemonSet sets `readOnlyRootFilesystem: true` |
| Bare metal | Either, plus editing the assembled `releases/<vsn>/vm.args` directly |

Two things to know before you set one. `ERL_FLAGS` is read by every `flzvm`
invocation, and every `flz` verb starts one, so exporting it in a profile
enlarges those short-lived nodes too. And a flag the VM does not accept refuses
the boot naming itself (`bad scheduler busy wait threshold: banana`), which
under an orchestrator is a crash loop rather than a setting quietly dropped.

**What the VM already handles.** The usual advice to rescue the BEAM from
cgroups does not apply here. Measured on the shipped image on an 8-core host:

| Container | schedulers | online | dirty IO |
|---|---|---|---|
| unconstrained | 8 | 8 | 10 |
| `--cpus=2` | 8 | **2** | 10 |
| `--cpuset-cpus=0,1` | 8 | **2** | 10 |

The VM reads the quota for `schedulers_online` on its own, so a CPU limit alone
does not need a `+S`. What it does not read is the dirty IO pool, which stays at
ten threads whatever the limit.

**Flags this release does not set, and why.** Each was either measured and found
not to matter, or is a decision that belongs to a deployment rather than to a
default.

| Flag | Verdict |
|---|---|
| `+sbwt very_long` and friends | **Measured harmful.** 33% more CPU than the default, and the throughput direction was worse in 8 of 8 paired rounds on both windows. The common advice to spin harder for throughput is wrong on this workload, where the controller shares a machine with the runners it feeds |
| `+sbwt none` and friends | Measured 22% less CPU against the default, one point better than the `short` that ships, and it pays for that point with the whole latency budget: plugin setup +204%, warmup +122%, operative creation +23%, where `short` costs +43%, +28% and +3.5%. Worth setting only where CPU is the scarce resource and nothing waits on a control-path round trip |
| `+sbwtdio none` alone | Measured 4.5% less CPU, consistent across rounds, and free in wall time. Most of the saving is on the normal schedulers, not the dirty IO pool, so this is the conservative fraction of the flag above rather than an alternative to it |
| `+SDio` | Measured flat: `+SDio 4` moved CPU 1.1% and neither throughput window past its own noise. Worth setting only where the dirty IO pool's ten threads are being charged against a small CPU quota |
| `+S` | Only where the pod's CPU limit is not the number you want; the VM already follows the limit |
| `+zdbbl` | The distribution buffer bound, 1 MiB by default. Namespace hubs and WAL transfer ride distribution, so this is the flag to raise if a sending process is seen blocking on a busy dist port. No measurement here supports moving it, and it costs memory per connection |
| `+stbt` | Scheduler binding. Worth trying on a dedicated machine only; wrong anywhere a quota or cpuset can move underneath the VM |
| `+hms`, `+hmbs` | Per-process heap sizes. The cost scales with process count, and this controller runs a process per Cell |
| `-env ERL_FULLSWEEP_AFTER` | A memory-drift remedy chosen against a measured series, not a default. See [`operations/memory_drift.md`](operations/memory_drift.md) |

The measurement protocol and the numbers behind every verdict here are in
[`operations/benchmarking.md`](operations/benchmarking.md).

### Build-time and image variables

| Variable | Read by | Notes |
|---|---|---|
| `RUSTLER_SKIP_COMPILATION` | Rustler, via `mix compile` | `true` makes the build pick up a pre-built `priv/native/flz_nif.so` instead of invoking cargo (prod image Stage 2) |
| `FLEZHA_RESTORE_WAIT_TIMEOUT_S` | `docker/prod/controller-entrypoint.sh` | the image entrypoint's own contract; it bounds the wait for the controller's control socket. See [`operations/deployment/images.md`](operations/deployment/images.md) |
| `FLEZHA_TESTNET_WS_PORT` | `config/runtime.exs` and `docker/prod/controller-entrypoint.sh` | the port the WS testnet daemon binds. `:test` derives it from the run (`4500` outside `:test`); the entrypoint starts the daemon only when it is set explicitly |
| `FLEZHA_NODE_NAME` | `config/runtime.exs` and `rel/env.sh.eex` | the base of the node name `<name>@<host>`. `:test` derives it from the run (`flezha_test_<pid>`; `flezha` outside `:test`) and starts distribution under it; the release script builds `RELEASE_NODE` from it |

### Runner-process environment (SDK side)

Read by the runner process, not the controller node. A standalone Rust, Java or
Elixir runner reads them from its own environment; the shared `flz_transport`
crate also reads its rows inside the controller's NIF.

| Variable | Default | Read by |
|---|---|---|
| `FLEZHA_RUNNER_ID_PATH` | `/var/lib/flezha-runner/id` | `flz_core::persistent_id`, `Flezha.SDK.PersistentId` — the durable stable identity; an unwritable path degrades to a fresh per-boot id with one warning, never a refusal |
| `FLEZHA_MAILBOX_CAPACITY` | `1024` (range `1..1_048_576`) | per-Cell mailbox bound in all three SDKs; a Tasklet that overrides `mailbox_capacity()` wins over it |
| `FLEZHA_SOCKET_FILES_PATH` | resolved by the socket ladder (below) | `flz_transport` path helper — every SDK reaches it, Elixir through `flz_nif` |
| `FLEZHA_TRANSPORT` | `uds` | `flz_transport` (`quic` is the only other value) |
| `FLEZHA_QUIC_ADDRESSES` | `127.0.0.1:4433` | `flz_transport` — a comma list of controller endpoints, each a name or a literal with an optional port (default `4433`), re-resolved on **every** attempt. `FLEZHA_QUIC_ADDRESS` is not honoured and, set alone, refuses startup naming this one |
| `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` | `flz_transport`, `Flezha.SDK.Transport` |
| `FLEZHA_QUIC_CLIENT_CERT` / `FLEZHA_QUIC_CLIENT_KEY` | `app_data/certs/client.pem` / `app_data/certs/client.key` | `flz_transport` — the client pair presented to a QUIC server that requires peer authentication |
| `FLEZHA_MAX_FRAME_BYTES` | `67108864` (64 MiB) | `flz_transport` — pre-auth frame-length limit enforced by both frame parsers, cached on first read |
| `FLEZHA_MAX_WRITE_BUFFER_BYTES` | `8388608` (8 MiB) | `flz_transport` — per-stream send-buffer cap. A sender that honours the send gate never reaches it; what it refuses is answered to the caller, never discarded |
| `FLEZHA_SEND_GATE_HIGH_WATER` / `FLEZHA_SEND_GATE_LOW_WATER` | `0.8` / `0.5` | `flz_transport::transport::send_gate` — the send gate's watermarks, as **fractions** of `FLEZHA_MAX_WRITE_BUFFER_BYTES` so lowering the cap moves them with it. The gate rises when a stream's buffered plus queued bytes cross the high mark and falls when they drop under the low one; the sender holds in between. **The pair is taken whole or refused whole**: anything but two fractions with `0 < low < high < 1` leaves both defaults standing, because one configured mark beside one default is how a low-water mark ends up above its high-water one. Widening the gap costs memory and buys fewer wake-ups; narrowing it does the reverse. Neither is a throughput dial — the frames are sent either way |
| `FLEZHA_CREDIT_PARK_INTERVAL_MS` | `50` | `flz_core::credit` — how long a sender parked on an exhausted credit window waits before re-checking it. **A lost-wake fallback and a diagnostic, not a pacing dial**: `arm` and `grant` both `notify_all`, so the wait normally ends on the event and this bounds only a wake that never came. Lowering it buys no latency in the normal case and costs wakeups; raising it only lengthens recovery from a wake that was lost. `0` or an unparseable value reads as unset. Read inside `flz_core`, so a runner in any SDK satisfies it from its own environment |
| `FLEZHA_CONTROL_ACK_TIMEOUT_MS` | `10000` | `flz_transport` — first-frame ack deadline |
| `FLEZHA_CONTROL_IDLE_TIMEOUT_MS` | `45000` | `flz_transport` — steady-state idle window |
| `FLEZHA_CONTROL_KEEPALIVE_MS` | `15000` | `flz_transport` — control-channel ping interval |
| `FLEZHA_DATA_HEARTBEAT_MS` | `10000` | `flz_transport`, `Flezha.SDK.RunnerServer` — `data_heartbeat` interval, jittered into `[base/2, base]` |
| `FLEZHA_WORKER_POOL_SIZE` | Rust: host parallelism; Java: `2` fetchers | `flezha_runner::operative::poll_pool`, `FlezhaEnv` |
| `FLEZHA_WORKER_POOL_MAX` | `10 × FLEZHA_WORKER_POOL_SIZE` (derived, never below it) | Rust and Java — ceiling the starvation supervisor may grow the worker/fetcher set to (`flezha_runner::operative::poll_pool`, `FlezhaEnv`) |
| `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` | `1024` | `flz_core::poll_core` |
| `FLEZHA_DROP_NOTHING_NONPARKING` | `false` (off) | **Temporary rollout switch** (`flz_core::routing`). On, a `DropNothing` push a producing Cell cannot deliver is held in that Cell's push holdback and the Cell is handed no further work until it lands, instead of parking the calling host thread. Off, the producer parks as before. Reaches Rust and Java (both complete through `flz_core`); the Elixir SDK already defers and reads nothing here. Nothing an author declared changes either way — the guarantee is "nothing discarded, order preserved" in both. **It buys the freed thread and costs throughput under saturation**: free at the default mailbox capacity (p = 0.46), but **30–36% slower than the park** on a mailbox held continuously full (measured at `FLEZHA_MAILBOX_CAPACITY=4`). Turn it on where a host cannot afford to hold a thread — a single-threaded FFI host, a Java carrier pinned by a native frame — not for speed. Flipping the DEFAULT is gated on three things: a ≥ 2 h soak with it on against a genuinely saturated consumer, the Java round-trip bench under both settings (owed), and that 30–36% cost closed or accepted in writing; the flag is deleted in the change that flips it or the next one (`docs/dev_notes/nonblocking-drop-nothing-backpressure.md`) |
| `FLEZHA_PUSH_HOLDBACK_MAX` | `4096` | `flz_core::routing` — how many undelivered pushes one producing Cell may hold before an emit waits for its own earlier ones to land. Only an in-process typed emit loop can reach it; an FFI host's outputs are materialized before the core sees them. Reaching it logs one throttled `flezha::mailbox` warning naming the producer and the consumer |
| `FLEZHA_FLUSH_LINGER_MS` | `5` | `flz_core::flusher` |
| `FLEZHA_FLUSH_BEFORE_EMIT` | `true` (on) | Rust dispatcher; `0`/`false`/`off`/`no` reverts to inline emit |
| `FLEZHA_REAP_INTERVAL_MS` | `min(cell_idle_ttl / 4, 30 s)` (derived) | `flz_core::dynamic` and the Elixir SDK's Operative — idle-Cell reaper cadence |
| `FLEZHA_RECONNECT_RING_CAPACITY` | `1024` | `flz_core::state_sink` and `Flezha.SDK.Hold` — frames held while no stream will take them, replayed when one binds, oldest dropped on overflow and counted as a drop-oldest shed. Only a frame whose guarantee class is held enters it: a durable record or a plugin/workflow output, never a control frame and never the heartbeat. In `flz_core` one ring holds them all; the same knob bounds four holders in the Elixir SDK — the Operative's plugin-bound outputs, the frames a stopping Cell hands to it, each Cell's own change records, and the workflow driver's writes the runner relays — an Operative reads it once and hands it to its own two holds and to the Cells it starts, so those three hold at one number, while the runner's own hold and a workflow stage's Cells read it for themselves; and a rebind replays a stopped Cell's frames first, then a Cell's records, then the outputs of the same dispatch |
| `FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD` / `FLEZHA_BROKEN_PIPE_WINDOW_MS` | `10` / `5000` | `flz_core::state_sink` and `Flezha.SDK.Hold` — write failures inside the window that request a reconnect. The Elixir SDK reads them for its `data_heartbeat`, which is owed no delivery and whose misses are the only signal an idle runner gives |
| `FLEZHA_RECONNECT_REQUEST_RETRY_MS` | `30000` | `flz_core::state_sink` and `Flezha.SDK.Hold` — an unanswered reconnect request re-arms after this, so it cannot disarm the detector for good |
| `FLEZHA_WORKFLOW_ACK_TIMEOUT_MS` | `30000` | `flz_core::workflow::driver` — task-commit ack deadline |
| `FLEZHA_WORKFLOW_PARK_IDLE_MS` | `5000` | `flz_core::workflow::driver` and the Elixir SDK's workflow driver — inbox-loop park hysteresis |
| `FLEZHA_WORKFLOW_DRIVER_STACK_KIB` | platform default | `flz_core::workflow::driver` — driver-thread stack size; address space only |
| `FLEZHA_NATIVE_LIB_DIR` | `java.io.tmpdir` | Java `NativeLibraryLoader` — where the bundled `flz_core_ffi` library is extracted |

### The socket-directory ladder

Where the UDS control and data sockets live is one question with one answer,
resolved by `flz_transport` and shared by every SDK — Rust, Java and Node reach
it through `flz_core`, Elixir through `flz_nif`, and the controller through the
same NIF. The candidates, in order:

| # | Candidate | When |
|---|---|---|
| 1 | `$FLEZHA_SOCKET_FILES_PATH` | whenever it is set — **used outright, with no fallback to any later candidate** |
| 2 | `$XDG_RUNTIME_DIR/flezha` | when `XDG_RUNTIME_DIR` is set (every systemd and desktop session sets it) |
| 3 | `/run/flezha` | a controller running as a system service |
| 4 | `app_data/sockets` | the repository, so the development loop needs no configuration |

The **controller** takes the first candidate it can create, stepping over one
another controller is already listening in — taking that directory would wipe a
live controller's sockets and strand its fleet. An explicit
`FLEZHA_SOCKET_FILES_PATH` is still resolved unconditionally: it is what the
operator named. It is not *prepared* unconditionally, though — a controller
probes the directory it resolved before clearing it, and refuses to start when
a live peer answers there, naming the path and this variable. A socket nobody
answers is a controller that died hard and is cleared as normal. See
[`operations/deployment/bare_metal.md` § Several controllers on one
machine](operations/deployment/bare_metal.md). A **runner** takes
the first holding a live control socket, and when two candidates hold one and
no variable says which is meant, it **refuses to connect and names both** rather
than attaching to a controller the operator did not choose. Every path is
reported absolutely, whichever candidate produced it.

Two consequences worth knowing:

- Sockets are runtime state, so they are no longer under `app_data/` by
  default — that directory holds certificates, credentials and durable state.
- `$XDG_RUNTIME_DIR` is per user, so the zero-configuration case is
  same-user. Across users, set `FLEZHA_SOCKET_FILES_PATH` explicitly on both
  sides and use `FLEZHA_SOCKET_DIR_GROUP` on the controller.

**The socket directory is at most 60 bytes.** A Unix domain socket address holds
107 bytes (`sun_path` less its NUL), and the longest name placed in the
directory is a runner's data socket, `secondary_stream_<id>.sock`, whose id
field is at most 24 bytes (a longer runner id is shortened to 24), so 46 bytes
after the separating `/`. A controller with UDS enabled refuses to boot on a
longer directory, before it removes anything in it, naming the directory,
its length and `FLEZHA_SOCKET_FILES_PATH`; a directory of 61 to 79 bytes would
otherwise hold the control socket and refuse every runner whose id is long at
connect. The ladder's own candidates fit on an ordinary host, and the
repository fallback fits while the controller's working directory is at most
43 bytes, which is another reason to prefer a short runtime directory over a
deep checkout.

`flz runner check` runs this resolution and its probe from a shell, with no
runner code and no running controller; see
[`operations/deployment/bare_metal.md`](operations/deployment/bare_metal.md).

### `FLEZHA_DROP_NOTHING_NONPARKING` — read this before you set it

**What it does.** A `DropNothing` channel never discards anything, so a push
into a full consumer mailbox has to wait. This flag changes *what* waits.

- **Off (the default):** the thread doing the push parks until the consumer
  frees a slot. That thread is stuck. If it happens to be the only thread that
  could have run the consumer, nothing ever frees the slot and the process is
  finished.
- **On:** the value goes onto a per-producer holdback queue and the pushing
  thread returns immediately. The *producing Cell* is handed no further work
  until its queue drains. Nothing is discarded and order is preserved either
  way — only the waiting moves, from the thread to the Cell.

**The default is off. Nothing changes until someone sets it.** Setting it is a
deployment decision, not an authoring one: no channel declaration, wire literal
or conformance fixture differs between the two.

**What it costs.** Measured, not estimated: **30–36% slower under sustained
saturation**. On a mailbox pinned full (`FLEZHA_MAILBOX_CAPACITY=4`, 5 000
messages per iteration) one consumer went 18.92 ms → 25.78 ms and three
consumers 46.10 ms → 59.75 ms. Read that narrowly — it is the cost when a
mailbox is *continuously* full. At the default capacity of 1024 it is within
noise (7.36 ms → 7.19 ms, p = 0.46). It is not a general slowdown.

The reason is structural: parking woke the producer straight back into its
emit, while the holdback re-arm is a scheduling round-trip — consumer pops,
wait list fires, holdback drains, gates go back, the producer's round is
rescheduled, a poller picks it up.

**What it buys.** The producing thread is never held. That is the entire point,
and for some hosts it is the difference between backpressure and a dead
process. A host that cannot grow a thread — PHP's `SyncExecutor`, a Node host on
the event loop — turns the default park into a guaranteed freeze, every time,
not sometimes. The starvation supervisor cannot rescue those: it buys threads,
and a single-threaded runtime has none to sell. Turn this on where a thread
cannot be spared. Do not turn it on for speed.

**Debugging note.** With the flag on, a producer waiting on a full consumer
holds no activation, so `inflight` / `inflight_oldest_ms` cannot see it. Look at
`pending_pushes` and `pending_push_oldest_ms` on the data heartbeat instead, and
at the runner's own `flezha::mailbox` warning, which names both the producer and
the consumer that is behind.

**Before the default is ever flipped**, three things have to be true, and none
of them is true today:

1. the Java round-trip bench run under both settings (owed — the numbers above
   are Rust-only, and one runtime proves one runtime);
2. a soak of at least two hours with the flag on, against a workload that
   genuinely saturates a consumer, at 100% data integrity;
3. the 30–36% saturation cost either closed or accepted in writing.

The flag is deleted in the change that flips it, or the next one. Details and
the deletion checklist.

## Layer 2: application env keys

Read via `Flezha.Config.get/1` by row name; `Read in` names the module that
reads the key. Most keys are filled for you at boot from the variables above.
`—` means the key is always set and the read site carries no default of its own;
keyword-list keys are shown as `:parent[:child]`.

**No read site carries a default.** Every default below is the row's, declared
once in `lib/flezha/config/keys.ex`, and the millisecond rows live beside it in
`lib/flezha/config/duration_rows.ex` with the derivations that keep one
quantity's multiple of another from drifting; a site reads
`Flezha.Config.Durations.get/1` for those. A retry ladder is the same story one level up: every wait before a
retry is a row of `lib/flezha/config/backoff.ex`, announced on
`[:flezha, :backoff, :wait]`.

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
| `:socket_files_path` | — | `lib/flezha/service/port_pilot.ex`, `lib/flezha/transport/socket/support.ex` |
| `:external_processes` | — | `lib/flezha/service/port_pilot.ex` |
| `:concurrent_channel_count` | `0` | `lib/flezha/config.ex` |
| `:send_gate_hold_ms` | `5_000` | `lib/flezha/runner/stream_channel.ex` — how long a sender held on a stream's raised send gate waits for the driver to report the buffer drained. **The gate's own lost-wake fallback**, the counterpart of `FLEZHA_CREDIT_PARK_INTERVAL_MS` above: the release arrives on the crossing, and this bounds only a drain report that never came. Running out returns `{:error, :send_buffer_full}` to the caller with the frame still unsent — nothing was discarded on the way, so it is the caller's decision, not a loss |

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
| `:state_root` | `FLEZHA_STATE_DIR`, else `"app_data/state"`, expanded to an absolute path on read (derived; set per run in `:test`) | `lib/flezha/state/paths.ex` |
| `:default_compact_after_bytes` | `8 * 1024 * 1024` | `lib/flezha/state/persistence_resolver.ex` |
| `:compact_after_age_ms` | `300_000` | `lib/flezha/state/persistence_resolver.ex` |
| `:compaction_timeout_ms` | `30_000` | `lib/flezha/state/persistence_resolver.ex`; `0` disables the compaction-timeout sweep |
| `:record_log_cut_after_bytes` | `64 * 1024` | `lib/flezha/state/persistence_resolver.ex`; `0` rewrites at every release |
| `:default_time_travel_retention_hours` | `6` | `lib/flezha/state/persistence_resolver.ex` |
| `:default_workflow_completed_retention_hours` | `72` | `lib/flezha/state/persistence_resolver.ex` |
| `:default_output_stall_timeout_ms` | `60_000` | `lib/flezha/state/persistence_resolver.ex` |
| `:operative_persistence` / `:time_travel_retention_hours` / `:workflow_completed_retention_hours` / `:compact_after_bytes` / `:compact_after_age_ms` / `:output_stall_timeout_ms` / `:sync_durability` / `:credit_window_bytes` | `%{}` | `lib/flezha/state/persistence_resolver.ex` — per-class operator overrides, `%{"OperativeClass" => value}`, winning over the SDK declaration and the instance metadata |
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
| `:boot_replay_timeout_ms` | `60_000` | `lib/flezha/runner/fleet_manager.ex` |
| `:process_hub_registry_root` | `"app_data/process_hub"` | `lib/flezha/service/hub_manager.ex`, `lib/flezha/state/durability_tier/artifact.ex` |
| `:process_hub_reconcile_grace_ms` / `:process_hub_reconcile_interval_ms` | `45_000` / `30_000` | `lib/flezha/service/hub_manager.ex` |
| `:hub_runner_assign_wait_ms` | `2_000` | `lib/flezha/service/hub_manager.ex` |
| `:migration_consent[:consent_timeout_ms]` / `[:retry_interval_ms]` / `[:max_defer_ms]` | `5_000` / `2_000` / `:graceful_shutdown_timeout_ms` (derived) | `lib/flezha/service/hub_manager.ex` |
| `:overwatch_call_timeout_ms` | `5_000` | `lib/flezha/service/operative_manager.ex` |
| `:overwatch_ping_enabled` | `true` | `lib/flezha/operative/lifecycle.ex` |
| `:overwatch_ping_timeout_ms` | `2_000` | `lib/flezha/runner/control_unit.ex` |
| `:overwatch_task_warn_ms` | `2_000` | `lib/flezha/operative/overwatch.ex` |
| `:overwatch_runner_wait_ms` | `30_000` | `lib/flezha/operative/lifecycle.ex` |
| `:stream_bind_wait_ms` | `30_000` | `lib/flezha/operative/lifecycle.ex` |
| `:correction_debounce_ms` | `200` | `lib/flezha/operative/replay.ex` |
| `:bind_restore_concurrency` | `16` | `lib/flezha/operative/replay.ex` — how many of a class's Cells a bind restores at once |
| `:placement_load_cap_percent` | `100` | `lib/flezha/runner/fleet_manager/load.ex` — the fairness cap on runner selection, as a percentage of the fair share |
| `:placement_load_refresh_ms` | `1_000` | `lib/flezha/runner/fleet_manager.ex` — how long the per-runner operative count a placement reads may go without being rebuilt from the hub registry |
| `:control_unit_heartbeat_interval_ms` | `15_000` | `lib/flezha/runner/control_unit.ex` |
| `:control_unit_pending_reply_timeout_ms` | `90_000` | `lib/flezha/runner/control_unit.ex` |

### Resilience and convergence

| Key | Default | Read in |
|---|---|---|
| `:control_liveness_window_ms` / `:control_liveness_interval_ms` | `45_000` / `15_000` | `lib/flezha/runner/control_channel.ex` |
| `:io_stream_liveness_interval_ms` / `:io_stream_liveness_threshold_ms` | `10_000` / `30_000` | `lib/flezha/runner/stream_channel.ex` |
| `:graceful_shutdown_enabled` | `true` | `lib/flezha/application.ex`, `lib/flezha/shutdown/coordinator.ex`, `lib/flezha/shutdown/signal_handler.ex` |
| `:graceful_shutdown_timeout_ms` | `60_000` | `lib/flezha/shutdown/coordinator.ex`, `lib/flezha/cli/cli_manager.ex` |
| `:convergence_health_enabled` | `true` | `lib/flezha/application.ex` |
| `:convergence_tick_ms` | `5_000` | `lib/flezha/convergence/loop.ex`, `lib/flezha/healthz/server.ex` |
| `:convergence_output_stall_grace_ms` | `60_000` | `lib/flezha/convergence/loop.ex` |
| `:convergence_unmet_intent_grace_ms` | `120_000` | `lib/flezha/healthz/server.ex` |
| `:convergence_fabric_503_polls` | `3` | `lib/flezha/healthz/server.ex` |
| `:convergence_cell_stall_ms` | `300_000` (the classifier's own default, `lib/flezha/convergence/stall_classifier.ex`) | `lib/flezha/convergence/loop/liveness.ex` — how long a handout may be out before the per-operative classifier calls that Cell stalled; a non-positive value leaves the default in place |
| `:healthz_enabled` | `false` | `lib/flezha/application.ex` |
| `:healthz_port` / `:healthz_bind` | `9569` / `"0.0.0.0"` (`Flezha.Healthz.Server.default_bind/0`, the one default; `config/test.exs` binds loopback) | `lib/flezha/healthz/server.ex` |
| `:output_watchdog_interval_ms` | `10_000` | `lib/flezha/state/watchdog.ex` |
| `:input_silence_timeout_ms` | `0` (off) | `lib/flezha/state/watchdog.ex`: how long a stream operative that produced output earlier may receive no Change before one warning is logged and `[:flezha, :state, :input_silence]` is emitted. Off by default: silence is not a fault, and the sweep cannot tell a dead upstream from a source that reports hourly. Set a positive value on a feed you know to be continuous |

### Observability and audit

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

### Security

| Key | Default | Read in |
|---|---|---|
| `:authn_authz[:enabled]` | `false` (`runtime.exs`: `true` in prod) | `lib/flezha/authz/config.ex`, `lib/flezha/application.ex` |
| `:authn_authz[:application_managed]` | `true` (`false` in test) | `lib/flezha/application.ex` — `false` starts no `Flezha.AuthZ.Supervisor` in the application tree |
| `:authn_authz[:auth_dir]` | `"app_data/auth"` | `lib/flezha/authz/config.ex` |
| `:authn_authz[:bootstrap_token_path]` | `"app_data/auth/bootstrap.token"` | `lib/flezha/authz/config.ex` |
| `:authn_authz[:token_ttl_default_seconds]` | `86_400` | `lib/flezha/authz/config.ex` — every credential with an expiry is one a human holds; a runner credential is refused an expiry outright |
| `:authn_authz[:token_expiry_warning_ms]` / `[:token_expiry_check_interval_ms]` | `86_400_000` / `300_000` | `lib/flezha/authz/token_store.ex` — the warning window and how often the registry looks |
| `:authn_authz[:namespaces]` | `%{}` | `lib/flezha/authz/config.ex` — per-namespace quota overrides |

### Plugins, DLQ and schedules

| Key | Default | Read in |
|---|---|---|
| `:enabled_plugins` | the provider map in `config/config.exs` | `lib/flezha/config.ex` |
| `:plugin_installer_enabled` | `false` | `lib/flezha/application.ex` |
| `:plugin_installer[:plugin_root]` | `"app_data/plugins"` | `lib/flezha/plugin/installer/store.ex`, `lib/flezha/plugin/installer.ex` |
| `:plugin_installer[:trusted_keys]` / `[:require_signature]` / `[:allow_unsigned]` | `[]` / `false` / `true` | `lib/flezha/plugin/installer.ex`, `lib/flezha/plugin/installer/loader.ex` |
| `:plugin_installer[:max_package_bytes]` / `[:max_source_files]` | `50_000_000` / `200` | `lib/flezha/plugin/installer.ex` |
| `:plugin_installer_data_dir` | unset | `lib/flezha/plugin/installer/store.ex` — overrides `plugin_root` when set |
| `:file_plugin_root` | `"app_data/files"` | `lib/flezha/plugin/provider/file.ex`, which resolves every file plugin path inside it and creates only the default folder |
| `:dlq[:dir]` | `"app_data/dlq"` | `lib/flezha/dlq/store.ex` |
| `:dlq[:retention_seconds]` / `[:max_total_bytes]` / `[:retention_check_interval_ms]` | `30 * 86_400` / `4_294_967_296` / `3_600_000` | `lib/flezha/dlq/store.ex` |
| `:dlq[:replay_grace_ms]` | `5_000` | `lib/flezha/cli/cli_manager.ex` — how long `flz dlq replay` waits for a re-park |
| `:schedules[:dir]` / `[:misfire_grace_ms]` | `"app_data/schedules"` / `120_000` | `lib/flezha/schedule/store.ex`, `lib/flezha/schedule/job.ex` |
| `:schedules[:application_managed]` | `true` (`false` in test) | `lib/flezha/application.ex` |

### Other

| Key | Default | Read in |
|---|---|---|
| `:env` | `Mix.env()` (`config/config.exs`) | `lib/flezha/operative/handover.ex`, `lib/flezha/runner/control_unit.ex` |
| `:pre_hooks` / `:post_hooks` | `[]` | `lib/flezha/boot/loader.ex` |
| `:testnet_enabled` | `true` in dev/test, `false` in prod (`config/<env>.exs`) | `lib/flezha/application.ex` |

## Layer 3: operative declarations (SDK)

Declared in code and shipped to the controller at registration: inside
`channels()` on a stream operative, or as trait/interface overrides on a
Tasklet or Workflow class. Rust names first, Java in parentheses. The Elixir
SDK carries the channel and edge options and `mailbox_capacity/0`; it has no
delivery-tier or workflow declarations.

### Delivery and acknowledgement (stream operatives)

| Declaration | Default if absent | What it does |
|---|---|---|
| `effectively_once()` (`effectivelyOnce`) | off — at-least-once | Every emit rides an atomic effect record through the WAL, and the outbox cursor advances only on a sink's confirmation. Registration refuses it unless every plugin-bound channel is `.keyed()`. |
| `checkpoint_ack()` (`checkpointAck`) | off — acknowledge per input | Hold the consumed offset until the interval closes, so state and offset advance together. Requires `effectively_once()`. |
| `checkpoint_every(n)` (`checkpointEvery`) | absent — only `ctx.checkpoint()` closes the interval | Close the interval after `n` inputs; an explicit checkpoint still closes it early. Implies `checkpoint_ack()`, and so requires `effectively_once()` too. |
| `outputs_at_checkpoint(max_delay_ms)` (`outputsAtCheckpoint`) | absent — outputs flush as produced | Hold the interval's outputs and ship them inside its checkpoint record as one batch. `max_delay_ms` is required and bounds how long a quiet Cell holds an output. Implies `checkpoint_ack()`, and so requires `effectively_once()` too. |
| `correction_window(window_ms)` (`correctionWindow`) | absent — a late input is just an input | A late input inside the window rewinds the Cell to a baseline and replays the affected slice in event-time order. Needs an `EventTimeFrom` clock. |
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
| `.on_full(OverflowPolicy)` (`onFull`) | `DropNothing` | What a full Cell mailbox discards: `DropNothing`, `DropNewest`, `DropOldest`. Nothing drops by default; `DropNothing` slows the producer to the consumer's rate instead. |
| `.retry_max(n)` (`retryMax`) | `3` attempts | Attempts before a failing payload is parked to the dead-letter queue, or, into a workflow tasklet, before the tasklet fails and the run compensates. `0` is refused (`RetryMaxZero`). |
| `.retry_backoff_ms(ms)` (`retryBackoffMs`) | `50` | Base of the exponential backoff between attempts, for both kinds; the wait holds no thread. |
| `.keyed()` (`keyed`) | off | Every plugin emit must carry a natural key; the runtime stamps `flz.cell` / `flz.seq` / `flz.key`. Requires a `sink`; required on every plugin channel under `effectively_once()`. |
| `.key_by(f)` (`keyBy`) | absent — one Cell | Route by a key function, making the consumer dynamic-cell. |
| `.sink(name)` / `.sink_with(name, f)` (`sink`) | — | Wire the channel to a named plugin; a second sink on one channel is a registration error. |
| `.sealed()` (`sealed`) | — | Type-level statement that nothing follows the channel; touches no builder state. |

### Tasklet-level options

Overridden on the `Tasklet` trait (Rust) or Tasklet class (Java).

| Override | Default | What it does |
|---|---|---|
| `compact_after_bytes()` (`compactAfterBytes`) | 8 MiB, in lockstep with `:default_compact_after_bytes` | Per-Cell WAL size reported to the controller as the snapshot-compaction trigger. |
| `mailbox_capacity()` (`mailboxCapacity`) | `1024`, range `1..=1_048_576` | Bounded mailbox depth per Cell; precedence is the override, then `FLEZHA_MAILBOX_CAPACITY`, then the default. Rust overrides must also set `MAILBOX_CAPACITY_OVERRIDDEN = true`. |
| `cell_idle_ttl()` (`cellIdleTtl`; Elixir `cell_idle_ttl_ms/0`) | `None` — no eviction | Evict a dynamic Cell after this idle period; eviction is destructive (slot, WAL and snapshot dropped). |
| `max_cells()` (Elixir `max_cells/0`) | `100_000` | Soft cap on live dynamic Cells per Tasklet; new keys past it are dropped. |
| `type Clock` | — (must be declared) | `WallClock`, `EventTimeFrom<..>` or `LogicalClock`; decides `ctx.now()` and event-time ordering. |

### Workflow class options

Overridden on the `WorkflowOperative` trait (Rust) or interface (Java). A
tasklet's retry ladder is its input edge's (above), and a commit barrier whose
ack timed out is re-sent three times, a constant, before the run ends
abnormally.

| Override | Default | What it does |
|---|---|---|
| `max_duration()` (`maxDuration`) | none | Wall-clock bound on one run. |
| `self_terminating()` (`selfTerminating`) | `true` | Whether the run ends on its own or waits for a stop. |
| `loop_mode()` (`loopMode`) | `Once` | `Once`, `PerInput` or `Chained` looping over the durable inbox. |
| `wake_after()` (`wakeAfter`) | none | Deadline at which a parked loop rehydrates without input. |
| `rollback_mode()` (`rollbackMode`) | `AllOrNothing` | How a rewind undoes journaled tasks: `AllOrNothing` or `KeepProgress`. |
| `PERSISTENCE_ENABLED` (`persistenceEnabled`) | `true` | `false` keeps the instance in memory only: a crash re-runs from the top. |
| `SYNC_DURABILITY` (`syncDurability`) | `false` | Ack a commit barrier only after its remote ship (RPO 0); inert without a durability backend, workflow classes only. |

## Layer 4: plugin options

Passed in the plugin's `opts` map at enable time. Every option a provider takes
is a row of that provider's own declaration, so the option table on its page is
rendered from the declaration and gated against it by the conformance test:
there is no second list to keep in step, and this page does not carry one.

| Provider name | Options |
|---|---|
| `operative_kafka_producer` | [`plugins/kafka_producer.md`](plugins/kafka_producer.md) |
| `operative_postgres` | [`plugins/postgres_sink.md`](plugins/postgres_sink.md) |
| `operative_http_poll` | [`plugins/http_poll.md`](plugins/http_poll.md) |
| `operative_file_source`, `operative_file_sink` | [`plugins/file.md`](plugins/file.md) |
| `operative_websocket` | [`plugins/websocket_subscription.md`](plugins/websocket_subscription.md) |
| `operative_forward` | [`plugins/operative_forward.md`](plugins/operative_forward.md) |
| `operative_stdout`, `operative_io_filter`, `operative_beam_msg`, `monitored_io_forward`, `operative_message_forward` | [`plugins/lifecycle.md`](plugins/lifecycle.md), which lists every provider and what it declares |

An option a provider does not declare is refused at enable rather than ignored,
on the CLI path and on the `--plugins` spec path alike, so a config asking for a
capability the provider does not offer fails loudly.
| `operative_message_forward` | [`plugins/message_handlers.md`](plugins/message_handlers.md) |
| `operative_beam_msg` (`:include_metadata` `false`), `operative_stdout`, `operative_io_filter` (`:regex_pattern` required, `:inverted` `false`), `monitored_io_forward` (`:recv_pid` required, `:heartbeat_interval` `5000`) | [`plugins/lifecycle.md`](plugins/lifecycle.md) |

## See also

- [`operations/deployment/README.md`](operations/deployment/README.md) — the variables a deploy cannot omit, and how to run a node
- [`resilience/data_durability.md`](resilience/data_durability.md) — what the durability knobs mean for correctness
- [`architecture/kernel.md`](architecture/kernel.md) — where the WAL, outbox and fsync knobs sit in the data path
- [`operations/authn_authz.md`](operations/authn_authz.md) — the posture behind the authz keys
- [`plugins/kafka_producer.md`](plugins/kafka_producer.md) — the Kafka producer's options in depth
- [`operations/benchmarking.md`](operations/benchmarking.md) — measuring the effect of a change to any of these
