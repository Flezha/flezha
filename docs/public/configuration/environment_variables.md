# Controller environment variables

Every variable the controller node reads, grouped by area. A variable a runner
reads is in [Runner environment variables](runner_environment.md), and the VM's
own flags are in [Erlang VM arguments](vm_arguments.md). Changing any of these
takes a restart of the node.

Read at boot by `config/runtime.exs`, which asks the table, unless the notes say
otherwise. Each variable sets the [application env key](application_env.md) its
row names. Booleans accept `1`/`true`/`yes`/`on` and `0`/`false`/`no`/`off`
case-insensitively; a value that is neither leaves the default standing, as does
a count that is zero, negative or not a number. Every other type — a duration,
an address, a member of an enumeration — refuses the boot naming the variable,
the value and the type.

## Transport and TLS

Runners talk to the controller over one of two transports. The default, Unix
domain sockets (`uds`), needs nothing set when the runners run on the
controller's machine: both sides find the socket directory on their own, by the
[socket-directory ladder](files_and_sockets.md#the-socket-directory-ladder).
QUIC lets runners on other machines connect over the network; it is
[experimental: quic](../publishing/maturity.md#quic), and on its first start the controller generates the self-signed
certificates it needs under `app_data/certs`.

Most deployments set nothing here. The settings you are likely to touch:

- `FLEZHA_SOCKET_FILES_PATH`, to fix where the sockets live, for example when
  the controller and the runners run as different users;
- `FLEZHA_TRANSPORT=quic` and `FLEZHA_QUIC_LISTEN_ADDRESS`, to accept runners
  from other machines;
- the `*_CERT_PATH` and `*_KEY_PATH` variables, to use your own certificates
  instead of the generated ones.

```sh
$ FLEZHA_SOCKET_FILES_PATH=/srv/flezha/sockets ./bin/flz controller start
```

Every variable of the area:

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_TRANSPORT` | `uds` | `:transport`. `uds` or `quic` ([experimental: quic](../publishing/maturity.md#quic)); any other value is refused at boot |
| `FLEZHA_TRANSPORTS` | the value of `FLEZHA_TRANSPORT` (`uds,quic` in test) | `:transports`, comma list validated against the same three names; `quic` is [experimental: quic](../publishing/maturity.md#quic) |
| `FLEZHA_TRANSPORT_AUTH` | `true` (required) | `:transport_auth_required`; `off`/`false`/`0`/`no` disables peer authentication and is warned on every boot |
| `FLEZHA_QUIC_LISTEN_ADDRESS` | `0.0.0.0:4433` (`:test` derives the port from the run) | `:quic_listen_address`, the address the controller listens on (a runner's dial list is `FLEZHA_QUIC_ADDRESSES`, in [Runner environment variables](runner_environment.md)); [experimental: quic](../publishing/maturity.md#quic) |
| `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` | `:ca_cert_path` |
| `FLEZHA_SERVER_CERT_PATH` | `app_data/certs/server.pem` | `:server_cert_path` |
| `FLEZHA_SERVER_KEY_PATH` | `app_data/certs/server.key` | `:server_key_path` |
| `FLEZHA_CLIENT_CERT_PATH` | `app_data/certs/client.pem` | `:client_cert_path` |
| `FLEZHA_CLIENT_KEY_PATH` | `app_data/certs/client.key` | `:client_key_path` |
| `FLEZHA_QUIC_CLIENT_CA` | `app_data/certs/ca.pem` | `:quic_client_ca_path`, trust anchor for QUIC client certificates; [experimental: quic](../publishing/maturity.md#quic) |
| `FLEZHA_SOCKET_FILES_PATH` | resolved by the [socket-directory ladder](files_and_sockets.md#the-socket-directory-ladder) | the UDS socket directory. Set, it is used outright; unset, the controller creates the first ladder candidate it can. **At most 60 bytes**: a longer directory refuses the boot, naming the directory, its length and this variable, because a runner's data socket would not fit beside it (see the ladder) |
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

## Cluster and nodes

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

## State, WAL and durability

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_STATE_DIR` | `app_data/state` (`:test` derives a root per run) | the state root. Like every variable here it fills its key once, at boot, in `config/runtime.exs`; `Flezha.State.Paths.state_root/0` then reads `:state_root` and resolves it to an absolute path. Setting the variable in a live BEAM changes nothing. A relative value still resolves against the directory the controller was started from, which the boot and `flz controller status` both name |
| `FLEZHA_COMPACT_AFTER_BYTES` | `8388608` | `:default_compact_after_bytes` |
| `FLEZHA_COMPACT_AFTER_AGE_MS` | `300000` (`0` in test) | `:compact_after_age_ms`; `0` disables the age trigger |
| `FLEZHA_EFFECT_MAX_BYTES` | `4194304` (4 MiB) | `:effect_max_bytes`, the largest `ctx.effect` result the controller keeps; runners read the same variable ([recorded effects](../topics/core/effectively_once.md#recorded-effects)) |
| `FLEZHA_INPUT_LOG_HOLD_WINDOW_MS` | `0` | `:input_log_hold_window_ms`, a floor; a declared `correction_ms` raises its own hold above it |
| `FLEZHA_INPUT_SILENCE_TIMEOUT_MS` | `0` (off) | `:input_silence_timeout_ms`; a flow or awake steps relay that produced output and then receives no Change for this long logs one warning and emits `[:flezha, :state, :input_silence]`. Set it on a controller whose feeds are continuous |
| `FLEZHA_CREDIT_FLOW_CONTROL` | `true` | `:credit_flow_control_enabled` |
| `FLEZHA_RETENTION_CHECK_INTERVAL_MS` | `60000` | `:time_travel_retention_check_interval_ms` (not in test) |
| `FLEZHA_STEPS_RETENTION_HOURS` | `72` | `:default_relay_completed_retention_hours` (not in test); how long the record and state of a finished instance are kept, a completed flow instance's as well as a steps one's |
| `FLEZHA_DURABILITY_BACKEND` | `none` (prod warns when it was defaulted) | `:durability_tier[:backend]`; `none`, `postgres`, `s3`, `local_path`; any other spelling resolves to `none` and warns. A selected backend missing an option it cannot work without refuses the boot naming it. A class at `durable: local` ships to it asynchronously; one at `durable: remote` needs it and waits on it before each acknowledgement |
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

## Runner and relay lifecycle

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_RUNNER_KEEPALIVE` | `30000` | `:runner_keepalive`; how long a disconnected runner is kept before removal, `0` removes at once |
| `FLEZHA_RUNNER_RETIRING_MARK_TTL_MS` | `30000` | `:runner_retiring_mark_ttl_ms`; how long a controller-initiated disconnect stays marked, so the close it produces is reported as expected rather than as an incident. A runner id is reused across reconnects, so the mark expires: past this window a close is diagnosed as unexpected again |
| `FLEZHA_GROUP_WITHDRAW_GRACE_MS` | `5000` | `:group_withdraw_grace_ms`; how long this controller keeps declaring a factory to the namespace hub after its last runner for it goes. A runner back inside the grace moves no relay; past it the hub reassigns them to a controller that still runs the factory. A disconnect the controller asked for skips the grace |
| `FLEZHA_PLACEMENT_LOAD_CAP_PERCENT` | `100` | `:placement_load_cap_percent`; the fairness cap on runner selection, as a percentage of the candidate set's fair share. At `100` the cap is exactly the fair share, so a namespace holding fewer relays than it has runners still uses all of them; a value high enough that no candidate reaches the cap is plain rendezvous hashing |
| `FLEZHA_MIGRATION_CONSENT_TIMEOUT_MS` | `5000` | `:migration_consent[:consent_timeout_ms]` |
| `FLEZHA_MIGRATION_RETRY_INTERVAL_MS` | `2000` | `:migration_consent[:retry_interval_ms]` |
| `FLEZHA_MIGRATION_MAX_DEFER_MS` | unset → the graceful-shutdown timeout (derived) | `:migration_consent[:max_defer_ms]` |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | `app_data/process_hub` | `:process_hub_registry_root` (not in test) |
| `FLEZHA_PROCESS_HUB_CLUSTER_SETTLE_MS` | `2000` | `:process_hub_cluster_settle_ms` (not in test) |
| `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` | `45000` | `:process_hub_reconcile_grace_ms` (not in test) |
| `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS` | `30000` | `:process_hub_reconcile_interval_ms` (not in test) |
| `FLEZHA_BOOT_REPLAY_TIMEOUT_MS` | `60000` | `:boot_replay_timeout_ms` (not in test) |
| `FLEZHA_AUTO_RESTORE_ENABLED` | `true` | `:auto_restore_enabled` (not in test); true only for `true`/`1` |
| `FLEZHA_CALL_PENDING_MAX` | `10000` | `:call_pending_max`; the answers one callee relay owes to the callers waiting on it, outside callers and relays alike. A call past it is refused `call.busy` before anything is ingested; a relay's own outbound calls never count against it, its own or a callee's |
| `FLEZHA_CALL_RETENTION_MARGIN_MS` | `300000` | `:call_retention_margin_ms`; how long an input log keeps a record carrying a relay's call (the request in the callee's log, the `Response` in the caller's) past the call's deadline, so a request sent again is absorbed and a recovered caller finds its `Response`. A call whose caller was down past its deadline plus this margin is settled with no `Response` (`[:flezha, :call, :recovered_unanswered]`) |
| `FLEZHA_LISTENER_PENDING_BYTES` | `4194304` | `:listener_pending_bytes`; the payload bytes one listener (`flz relay listen`, or a plugin through `Flezha.Service.Listener.listen/4`) holds that its handler has not taken. Parts are dropped oldest first to stay inside it; an output or a dead-letter notice that still does not fit ends the listener `listener.overrun` |
| `FLEZHA_LISTENER_PENDING_FRAMES` | `4096` | `:listener_pending_frames`; the frames one listener holds that its handler has not taken, with the same drops and the same end as `FLEZHA_LISTENER_PENDING_BYTES` |
| `FLEZHA_PLUGIN_BACKLOG_BYTES` | `4194304` | `:plugin_backlog_bytes`; the bytes of the hook events one copy of a controller plugin has not run. An event that does not fit drops the oldest waiting ones, counted, the copy's health says `plugin_lagging`, and `flz plugin list` shows the controllers it lags on |
| `FLEZHA_PLUGIN_BACKLOG_FRAMES` | `4096` | `:plugin_backlog_frames`; the hook events one copy of a controller plugin has not run, with the same drops as `FLEZHA_PLUGIN_BACKLOG_BYTES` |
| `FLEZHA_PLUGIN_INGEST_WINDOW` | `64` | `:plugin_ingest_window`; the records of one batch a source hands over with `Flezha.Plugin.Host.inject_many/4` that wait for their verdicts at once; the next is sent as each answer arrives. Also the default window of `RelayManager.send_io_confirming_many/5` |
| `FLEZHA_PLUGIN_IDLE_STOP_MS` | `60000` | `:plugin_idle_stop_ms`; how long a plugin's running copy whose last relay on that controller went down or moved away waits for one to come back before it stops. A deliberate leave (an unbind, a relay stop) stops it at once; the plugin stays enabled either way |

## Resilience and convergence

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
| `FLEZHA_HEALTHZ_ENABLED` | `false` | `:healthz_enabled` (not in test); true only for `true`/`1`. The endpoint folds the convergence loop's verdict records: `degraded` names every reason it folded (`parked`, `dead_runner`, `plugin_unhealthy`, `output_stalled`, `intent_mismatch`, `sensor_stale`, `data_plane_wedged`, joined with `+`), and the body carries `unknown_relays` |
| `FLEZHA_HEALTHZ_PORT` | `9569` | `:healthz_port` (not in test) |
| `FLEZHA_HEALTHZ_BIND` | `0.0.0.0` | `:healthz_bind` (not in test) |
| `FLEZHA_DATA_PLANE_LIVENESS` | `true` | `:observability[:data_plane_liveness]`; `false` drops the driver poll and the stall classifier, the counters stay |
| `FLEZHA_MEMORY_REPORT_INTERVAL_MS` | unset | `:memory_report_interval_ms`; unset or non-positive starts no reporter |
| `FLEZHA_TESTNET_WS_PORT` | `4500` (`:test` derives the port from the run) | `:testnet_ws_port`, the port `Flezha.Testnet` binds; the prod image entrypoint starts the daemon only when it is set explicitly |

## Observability and audit

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

## Security

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_AUTHZ_ENABLED` | `true` in prod, `false` otherwise | `:authn_authz[:enabled]`; `Flezha.AuthZ.Config` also reads the variable live, so it wins over the config file. `false` warns on every boot |
| `FLEZHA_AUTHZ_DIR` | `app_data/auth` | `:authn_authz[:auth_dir]`, also read live by `Flezha.AuthZ.Config` |
| `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH` | `<FLEZHA_AUTHZ_DIR>/bootstrap.token` | `:authn_authz[:bootstrap_token_path]`, also read live |
| `FLEZHA_AUTHZ_TOKEN_EXPIRY_WARNING_MS` | `86400000` | `:authn_authz[:token_expiry_warning_ms]` — how far ahead of its expiry an entry is announced as `:token_expiring` |
| `FLEZHA_NAMESPACE_SECRET` | — | The credential a runner presents at connect to prove the namespace it names. Read by every SDK, never by the controller |

## Plugins, DLQ and schedules

| Variable | Default | Sets / notes |
|---|---|---|
| `FLEZHA_DLQ_DIR` | `app_data/dlq` | `:dlq[:dir]` |
| `FLEZHA_DLQ_RETENTION_DAYS` | `30` | `:dlq[:retention_seconds]` (× 86 400) |
| `FLEZHA_SCHEDULE_DIR` | `app_data/schedules` | `:schedules[:dir]` |
| `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS` | `120000` | `:schedules[:misfire_grace_ms]` |
| `FLEZHA_PLUGIN_ROOT` | `app_data/plugins` | `:plugins[:plugin_root]`, the folder of installed `*.fpz` packages, loaded at every boot. An archive there is the opt-in: no setting gates loading. The boot writes `.loaded/` inside it, so it must be writable |
| `FLEZHA_PLUGIN_REQUIRE_SIGNATURE` | `false` | `:plugins[:require_signature]`; with it set and no trusted key, every package is refused and the controller still boots |
| `FLEZHA_PLUGIN_TRUSTED_KEYS` | `""` | `:plugins[:trusted_keys]`, comma-separated PEM file paths read at boot |
| `FLEZHA_PLUGIN_MAX_PACKAGE_BYTES` | `50000000` | `:plugins[:max_package_bytes]`, a package's unpacked size and a download alike |
| `FLEZHA_PLUGINS` | unset | `:plugins[:list]`, the packages this node declares: paths or `https://` URLs separated by whitespace or commas, each optionally pinned with `#sha256=<hex>`. `flz plugin sync`, which the image entrypoint runs before start, makes the plugin root hold exactly these; while it is set, even empty, `flz plugin install` and `uninstall` are refused |
| `FLEZHA_FILE_PLUGIN_ROOT` | `app_data/files` | `:file_plugin_root`, the one folder, or the one `s3://<bucket>/<prefix>`, every file plugin path must resolve inside. A folder is resolved with symbolic links followed; the first file plugin enable creates the default folder, and a folder set here must already exist, or every enable is refused naming it. An `s3://` root is never created and takes the four settings below |
| `FLEZHA_FILE_PLUGIN_S3_ENDPOINT` | unset | `:file_plugin_s3[:endpoint]`, the S3-compatible endpoint an `s3://` root is on; required under one |
| `FLEZHA_FILE_PLUGIN_S3_REGION` | `us-east-1` | `:file_plugin_s3[:region]` |
| `FLEZHA_FILE_PLUGIN_S3_ACCESS_KEY_ID` | unset | `:file_plugin_s3[:access_key_id]`; required under an `s3://` root |
| `FLEZHA_FILE_PLUGIN_S3_SECRET_ENV` | unset | names the variable holding the secret key; required under an `s3://` root. None of the four falls back to `FLEZHA_DURABILITY_S3_*`, and a root overlapping the durability tier's bucket prefix is refused |

Each `FLEZHA_PLUGIN_*` name is a row of the table like every other knob, so an
unset one leaves the row's own default standing rather than the family being
skipped as a block.

## Build-time and image variables

| Variable | Read by | Notes |
|---|---|---|
| `RUSTLER_SKIP_COMPILATION` | Rustler, via `mix compile` | `true` makes the build pick up a pre-built `priv/native/flz_nif.so` instead of invoking cargo (prod image Stage 2) |
| `FLEZHA_RESTORE_WAIT_TIMEOUT_S` | `docker/prod/controller-entrypoint.sh` | the image entrypoint's own contract; it bounds the wait for the controller's control socket |
| `FLEZHA_SHUTDOWN_TOKEN_FILE` | `docker/prod/controller-entrypoint.sh` | a file holding an `admin@_system` token, presented by the SIGTERM trap's `flz controller stop`; without it an enforcing controller refuses the graceful stop and the orchestrator kills the BEAM at its grace period |
| `FLEZHA_TESTNET_WS_PORT` | `config/runtime.exs` and `docker/prod/controller-entrypoint.sh` | the port the WS testnet daemon binds. `:test` derives it from the run (`4500` outside `:test`); the entrypoint starts the daemon only when it is set explicitly |
| `FLEZHA_NODE_NAME` | `config/runtime.exs` and `rel/env.sh.eex` | the base of the node name `<name>@<host>`. `:test` derives it from the run (`flezha_test_<pid>`; `flezha` outside `:test`) and starts distribution under it; the release script builds `RELEASE_NODE` from it |
