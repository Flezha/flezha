# Controller environment variables

Every variable the controller node reads, grouped by area. A variable a runner
reads is in [Runner environment variables](runner_environment.md), and the VM's
own flags are in [Erlang VM arguments](vm_arguments.md). Changing any of these
takes a restart of the node.

Read at boot by `config/runtime.exs`, which asks the table, unless the notes say
otherwise. Each variable sets an [application env key](application_env.md).
Booleans accept `1`/`true`/`yes`/`on` and `0`/`false`/`no`/`off`
case-insensitively; a value that is neither leaves the default standing, as does
a count that is zero, negative or not a number. Every other type — a duration,
an address, a member of an enumeration — refuses the boot naming the variable,
the value and the type.

Each area below says what its variables do; the type and default of every one
is in [Every variable](#every-variable), generated from the configuration table.

## Transport and TLS

Runners talk to the controller over one of two transports. The default, Unix
domain sockets (`uds`), needs nothing set when the runners run on the
controller's machine: both sides find the socket directory on their own, by the
[socket-directory ladder](files_and_sockets.md#the-socket-directory-ladder).
QUIC lets runners on other machines connect over the network; it is
[experimental: quic](../publishing/maturity.md#quic), and on its first start the controller generates the self-signed
certificates it needs under `storage/app/certs`.

Most deployments set nothing here. The settings you are likely to touch:

  the controller and the runners run as different users;
  from other machines;
- the `*_CERT_PATH` and `*_KEY_PATH` variables, to use your own certificates
  instead of the generated ones.

```sh
$ FLEZHA_SOCKET_FILES_PATH=/srv/flezha/sockets ./bin/flz controller start
```

Notes on the area's variables:

- `FLEZHA_TRANSPORT`: `uds` or `quic` ([experimental: quic](../publishing/maturity.md#quic)); any other value is refused at boot.
- `FLEZHA_TRANSPORTS`: Comma list validated against the same three names; `quic` is [experimental: quic](../publishing/maturity.md#quic). Default: the value of `FLEZHA_TRANSPORT` (`uds,quic` in test).
- `FLEZHA_TRANSPORT_AUTH`: `off`/`false`/`0`/`no` disables peer authentication and is warned on every boot. Default: `true` (required).
- `FLEZHA_QUIC_LISTEN_ADDRESS`: The address the controller listens on (a runner's dial list is `FLEZHA_QUIC_ADDRESSES`, in [Runner environment variables](runner_environment.md)); [experimental: quic](../publishing/maturity.md#quic). Default: `0.0.0.0:4433` (`:test` derives the port from the run).
- `FLEZHA_QUIC_CLIENT_CA`: Trust anchor for QUIC client certificates; [experimental: quic](../publishing/maturity.md#quic).
- `FLEZHA_SOCKET_FILES_PATH`: The UDS socket directory. Set, it is used outright; unset, the controller creates the first ladder candidate it can. **At most 60 bytes**: a longer directory refuses the boot, naming the directory, its length and this variable, because a runner's data socket would not fit beside it (see the ladder). Default: resolved by the [socket-directory ladder](files_and_sockets.md#the-socket-directory-ladder).
- `FLEZHA_SOCKET_DIR_GROUP`: Widens UDS traversal to one named group: the directory becomes `0750` owned by that group, warned on every boot. A group the controller's user does not belong to refuses the boot. Default: unset (owner-only).
- `FLEZHA_CONCURRENT_CHANNEL_COUNT`: `0` when nothing sets it.
- `FLEZHA_EXTERNAL_PROCESSES`: Comma list of commands the controller spawns (test mode hard-codes the Rust test runner).
- `FLEZHA_CONTROLLER_ENABLED`: `false`/`0`/`no`/`off` boots a runner-only node with no controller tree.

> **Two suites on one checkout.** In `:test` the state root, the UDS socket
> directory, the QUIC listen port, the testnet daemon's WS port and the
> distribution node name all derive from the OS process running the suite, so a
> second session isolates itself by existing. Setting `FLEZHA_STATE_DIR`,
> `FLEZHA_QUIC_LISTEN_ADDRESS`, `FLEZHA_TESTNET_WS_PORT` or `FLEZHA_NODE_NAME`
> still wins, which is how a scenario that needs a known address or name asks
> for one.

## Cluster and nodes

- `FLEZHA_CLUSTER_DISCOVERY`: `manual_discovery` or `k8s_pod_discovery`.
- `FLEZHA_NODE_NAME`: `rel/env.sh.eex` builds `RELEASE_NODE=<name>@<host>` from it, and the test harness starts distribution under the same `<name>@<host>`. Default: `flezha` (`:test` derives a base per run).
- `FLEZHA_NODE_HOST`: The host half of `RELEASE_NODE` (`rel/env.sh.eex`); also sets `:node_host_name`, the host half the test harness names its node with.
- `RELEASE_COOKIE`: Read by `rel/env.sh.eex`: set → `RELEASE_DISTRIBUTION=name`; unset → a node start (`start`, `start_iex`, `daemon`, `daemon_iex`) mints 32 random bytes into `FLEZHA_COOKIE_FILE` (default `<state root>/cookie`, `0600`), reuses it on later boots, and only where neither is possible → `none` (no clustering, and `flzvm remote` cannot connect). Every other command, `flzvm remote` and the `flzvm eval` behind `flz`'s local verbs included, only reads the file. `flz` itself needs no cookie: it reaches the controller over its control socket. A cluster still provisions one value on every node. No command line carries it: each VM that joins distribution reads it from `.erlang.cookie` under the release's `tmp/home/<node name>/` (`RELEASE_TMP`), which the release writes `0600`. Default: minted per node.
- `FLEZHA_DIST_TLS_OPTFILE`: Read by `rel/env.sh.eex`: path of an `inet_tls_dist` option file, adds `-proto_dist inet_tls -ssl_dist_optfile`.
- `FLEZHA_INITIAL_NODES`: Comma list of node names.
- `FLEZHA_K8S_CLUSTER_DOMAIN`: Default: the raw value of `FLEZHA_K8S_CLUSTER_NAME`, else `""`.
- `FLEZHA_K8S_HOST_IDENTIFIER`: `ip`, `hostname` or `dns`.
- `FLEZHA_K8S_IP_LOOKUP_MODE`: `pods` or `endpoints`.
- `FLEZHA_SPLIT_BEHAVIOUR`: What every hub does in a network split: `divergence` keeps both sides running, `static` and `dynamic` stop the side without quorum ([choosing a split behaviour](../topics/core/running_and_testing.md#choosing-a-split-behaviour)). A setting that cannot work stops the boot, naming the variable.
- `FLEZHA_QUORUM_NODES`: `static`: the quorum, in controllers (required). `dynamic`: how many controllers a controller that knows no cluster waits for before it runs relays.
- `FLEZHA_QUORUM_PERCENT`: `dynamic`: the share of the cluster, 51 to 100, that must survive a sudden loss.
- `FLEZHA_QUORUM_WINDOW_MS`: `dynamic`: how long a lost controller counts as a sudden loss; rounded up to whole seconds.
- `FLEZHA_PROCESS_HUB_NODEDOWN_GRACE_MS`: How long a controller that failed (killed, crashed, cut off) keeps its relays before its peers take them over; a graceful stop or drain never waits ([a controller that fails](../topics/core/durability.md#a-controller-that-fails)). `0` moves them as soon as the loss is confirmed. Default: `30000` (`0` in test).

## State, WAL and durability

- `FLEZHA_STATE_DIR`: The state root. Like every variable here it fills its key once, at boot, in `config/runtime.exs`; `Flezha.State.Paths.state_root/0` then reads `:state_root` and resolves it to an absolute path. Setting the variable in a live BEAM changes nothing. A relative value still resolves against the directory the controller was started from, which the boot and `flz controller status` both name. Default: `storage/app/state` (`:test` derives a root per run).
- `FLEZHA_COMPACT_AFTER_AGE_MS`: `0` disables the age trigger. Default: `300000` (`0` in test).
- `FLEZHA_EFFECT_MAX_BYTES`: The largest `ctx.effect` result the controller keeps; runners read the same variable ([recorded effects](../topics/core/effectively_once.md#recorded-effects)). Default: `4194304` (4 MiB).
- `FLEZHA_INPUT_LOG_HOLD_WINDOW_MS`: A floor; a declared `correction_ms` raises its own hold above it.
- `FLEZHA_INPUT_SILENCE_TIMEOUT_MS`: A flow or awake steps relay that produced output and then receives no Change for this long logs one warning and emits `[:flezha, :state, :input_silence]`. Set it on a controller whose feeds are continuous. Default: `0` (off).
- `FLEZHA_STEPS_RETENTION_HOURS`: How long the record and state of a finished instance are kept, a completed flow instance's as well as a steps one's.
- `FLEZHA_DURABILITY_BACKEND`: `none`, `postgres`, `s3`, `local_path`; any other spelling resolves to `none` and warns. A selected backend missing an option it cannot work without refuses the boot naming it. A class at `durable: local` ships to it asynchronously; one at `durable: remote` needs it and waits on it before each acknowledgement. Default: `none` (prod warns when it was defaulted).
- `FLEZHA_DURABILITY_WRITER_ID`: Read at runtime by `Flezha.State.DurabilityTier.writer_id/0`; the stable lineage-writer identity for a controller whose node name changes on reschedule. Default: `node()`.
- `FLEZHA_DURABILITY_PATH`: `backend_opts[:path]` for `local_path` — a directory that outlives the node's own disk.
- `FLEZHA_DURABILITY_PG_HOST`: `backend_opts[:hostname]` for `postgres`.
- `FLEZHA_DURABILITY_PG_PORT`: `backend_opts[:port]` for `postgres`.
- `FLEZHA_DURABILITY_PG_DATABASE`: `backend_opts[:database]` for `postgres`.
- `FLEZHA_DURABILITY_PG_USERNAME`: `backend_opts[:username]` for `postgres`.
- `FLEZHA_DURABILITY_PG_PASSWORD_ENV`: Names the variable holding the password — never the password itself.
- `FLEZHA_DURABILITY_PG_SSL`: `backend_opts[:ssl]` for `postgres`.
- `FLEZHA_DURABILITY_PG_TABLE`: `backend_opts[:table]` for `postgres`.
- `FLEZHA_DURABILITY_PG_AUTO_CREATE`: `backend_opts[:auto_create]` for `postgres`.
- `FLEZHA_DURABILITY_S3_ENDPOINT`: `backend_opts[:endpoint]` for `s3`.
- `FLEZHA_DURABILITY_S3_BUCKET`: `backend_opts[:bucket]` for `s3`.
- `FLEZHA_DURABILITY_S3_ACCESS_KEY_ID`: `backend_opts[:access_key_id]` for `s3`.
- `FLEZHA_DURABILITY_S3_SECRET_ENV`: Names the variable holding the secret key.
- `FLEZHA_DURABILITY_S3_REGION`: `backend_opts[:region]` for `s3`.
- `FLEZHA_DURABILITY_S3_PREFIX`: `backend_opts[:prefix]` for `s3`.

## Runner and relay lifecycle

- `FLEZHA_RUNNER_REMOVAL_GRACE_MS`: How long a disconnected runner is kept before removal, `0` removes at once.
- `FLEZHA_RUNNER_RETIRING_MARK_TTL_MS`: How long a controller-initiated disconnect stays marked, so the close it produces is reported as expected rather than as an incident. A runner id is reused across reconnects, so the mark expires: past this window a close is diagnosed as unexpected again.
- `FLEZHA_GROUP_WITHDRAW_GRACE_MS`: How long this controller keeps declaring a factory to the namespace hub after its last runner for it goes. A runner back inside the grace moves no relay; past it the hub reassigns them to a controller that still runs the factory. A disconnect the controller asked for skips the grace.
- `FLEZHA_PLACEMENT_LOAD_CAP_PERCENT`: The fairness cap on runner selection, as a percentage of the candidate set's fair share. At `100` the cap is exactly the fair share, so a namespace holding fewer relays than it has runners still uses all of them; a value high enough that no candidate reaches the cap is plain rendezvous hashing.
- `FLEZHA_STREAM_CHANNEL_MAX_HEAP_BYTES`: A last-resort valve for a runner that sends faster than the controller takes its frames in: the memory one runner data channel may hold, the messages waiting in its mailbox included, before the runtime kills it. The killed channel's replacement is repaired as a closed stream is: the runner is asked for fresh streams (in practice it often reconnects its whole session), the relays on it are flagged for the convergence loop, and inputs not yet applied are sent again from the input log. What it costs: every frame waiting in the channel is dropped and recovered only by that replay, which can apply an input twice, and it bounds the channel alone, not the relays and logs behind it, so a sustained flood still fills the controller's memory. The kill is logged in one line and emits `[:flezha, :stream, :channel_down]`. Default: `0` (off).
- `FLEZHA_MIGRATION_MAX_DEFER_MS`: Default: unset → the graceful-shutdown timeout (derived).
- `FLEZHA_AUTO_RESTORE_ENABLED`: True only for `true`/`1`.
- `FLEZHA_CALL_PENDING_MAX`: The answers one callee relay owes to the callers waiting on it, outside callers and relays alike. A call past it is refused `call.busy` before anything is ingested; a relay's own outbound calls never count against it, its own or a callee's.
- `FLEZHA_CALL_RETENTION_MARGIN_MS`: How long an input log keeps a record carrying a relay's call (the request in the callee's log, the `Response` in the caller's) past the call's deadline, so a request sent again is absorbed and a recovered caller finds its `Response`. A call whose caller was down past its deadline plus this margin is settled with no `Response` (`[:flezha, :call, :recovered_unanswered]`).
- `FLEZHA_LISTENER_PENDING_BYTES`: The payload bytes one listener (`flz relay listen`, or a plugin through `Flezha.Service.Listener.listen/4`) holds that its handler has not taken. Parts are dropped oldest first to stay inside it; an output or a dead-letter notice that still does not fit ends the listener `listener.overrun`.
- `FLEZHA_LISTENER_PENDING_FRAMES`: The frames one listener holds that its handler has not taken, with the same drops and the same end as `FLEZHA_LISTENER_PENDING_BYTES`.
- `FLEZHA_PLUGIN_BACKLOG_BYTES`: The bytes of the hook events one copy of a controller plugin has not run. An event that does not fit drops the oldest waiting ones, counted, the copy's health says `plugin_lagging`, and `flz plugin list` shows the controllers it lags on.
- `FLEZHA_PLUGIN_BACKLOG_FRAMES`: The hook events one copy of a controller plugin has not run, with the same drops as `FLEZHA_PLUGIN_BACKLOG_BYTES`.
- `FLEZHA_PLUGIN_INGEST_WINDOW`: The records of one batch a source hands over with `Flezha.Plugin.Host.inject_many/4` that wait for their verdicts at once; the next is sent as each answer arrives. Also the default window of `RelayManager.send_io_confirming_many/5`.
- `FLEZHA_PLUGIN_IDLE_STOP_MS`: How long a plugin's running copy whose last relay on that controller went down or moved away waits for one to come back before it stops. A deliberate leave (an unbind, a relay stop) stops it at once; the plugin stays enabled either way.

## Resilience and convergence

- `FLEZHA_CONTROL_LIVENESS_WINDOW_MS`: The `control_liveness_window_ms` row of `Flezha.Config.Durations`, derived as 3 × the keepalive interval, so leaving it unset keeps the pair; it is the same window the runner's `FLEZHA_CONTROL_IDLE_TIMEOUT_MS` declares. Default: `45000`.
- `FLEZHA_OVERWATCH_PING_TIMEOUT_MS`: Bounds the pre-start `:ping_runner` round-trip, after which the Overwatch retries rather than cutting the channel.
- `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED`: When set, anything but `1`/`true`/`yes`/`on` is false.
- `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`: The orchestrator grace period is the `orchestrator_grace_ms` row — this + 15 s — and a test reads it back from the DaemonSet and both compose files.
- `FLEZHA_USE_CONVERGENCE_HEALTH`: True only for `true`/`1`.
- `FLEZHA_FABRIC_503_POLLS`: Consecutive classifier polls reporting `driver_loop_frozen` / `nif_queue_backing_up` before `/healthz` answers 503; `0` disables the fabric 503.
- `FLEZHA_HEALTHZ_ENABLED`: True only for `true`/`1`. The endpoint folds the convergence loop's verdict records: `degraded` names every reason it folded (`halted`, `dead_runner`, `relays_unbound`, `plugin_unhealthy`, `plugin_lagging`, `output_stalled`, `intent_mismatch`, `sensor_stale`, `data_plane_wedged`, joined with `+`), and the body carries `unknown_relays`.
- `FLEZHA_DATA_PLANE_LIVENESS`: `false` drops the driver poll and the stall classifier, the counters stay.
- `FLEZHA_TESTNET_WS_PORT`: The port `Flezha.Testnet` binds; the prod image entrypoint starts the daemon only when it is set explicitly. Default: `4500` (`:test` derives the port from the run).

## Observability and audit

- `FLEZHA_OBSERVABILITY_ENABLED`: `false` starts no sinks and no logger handler; the telemetry bridge only writes the events that are also log lines.
- `FLEZHA_OBSERVABILITY_LOG_SOURCES`: The sources whose signals are also written to the log, one `[<source>] <name> key=value` info line each; `beam_vm` gives the controller's memory and load every second. Unset writes none.
- `FLEZHA_AUDIT_RETENTION_DAYS`: In days; the key it fills holds seconds.
- `FLEZHA_LOG_PATH`: The file the `:logger` file handler writes.
- `FLEZHA_LOG_LEVEL`: Level for the controller; the runner default is the level every SDK starts its own diagnostics at. Default: `info` (controller), `warn` (runner).
- `FLEZHA_LOG_MAX_BYTES`: The size at which that file rotates.
- `FLEZHA_LOG_KEEP_FILES`: How many rotated files are kept.

## Security

- `FLEZHA_AUTHZ_ENABLED`: `Flezha.AuthZ.Config` also reads the variable live, so it wins over the config file. `false` warns on every boot. Default: `true` in prod, `false` otherwise.
- `FLEZHA_AUTHZ_DIR`: Also read live by `Flezha.AuthZ.Config`.
- `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH`: Also read live. Default: `<FLEZHA_AUTHZ_DIR>/bootstrap.token`.
- `FLEZHA_AUTHZ_TOKEN_EXPIRY_WARNING_MS`: How far ahead of its expiry an entry is announced as `:token_expiring`.
- `FLEZHA_NAMESPACE_SECRET`: The credential a runner presents at connect to prove the namespace it names. Read by every SDK, never by the controller.

## Plugins, DLQ and schedules

- `FLEZHA_DLQ_RETENTION_DAYS`: In days; the key it fills holds seconds.
- `FLEZHA_PLUGIN_ROOT`: The folder of installed `*.fpz` packages, loaded at every boot. An archive there is the opt-in: no setting gates loading. The boot writes `.loaded/` inside it, so it must be writable.
- `FLEZHA_PLUGIN_REQUIRE_SIGNATURE`: With it set and no trusted key, every package is refused and the controller still boots.
- `FLEZHA_PLUGIN_TRUSTED_KEYS`: Comma-separated PEM file paths read at boot.
- `FLEZHA_PLUGIN_MAX_PACKAGE_BYTES`: A package's unpacked size and a download alike.
- `FLEZHA_PLUGINS`: The packages this node declares: paths or `https://` URLs separated by whitespace or commas, each optionally pinned with `#sha256=<hex>`. `flz plugin sync`, which the image entrypoint runs before start, makes the plugin root hold exactly these; while it is set, even empty, `flz plugin install` and `uninstall` are refused.
- `FLEZHA_FILE_PLUGIN_ROOT`: The one folder, or the one `s3://<bucket>/<prefix>`, every file plugin path must resolve inside. A folder is resolved with symbolic links followed; the first file plugin enable creates the default folder, and a folder set here must already exist, or every enable is refused naming it. An `s3://` root is never created and takes the four settings below.
- `FLEZHA_FILE_PLUGIN_S3_ENDPOINT`: The S3-compatible endpoint an `s3://` root is on; required under one.
- `FLEZHA_FILE_PLUGIN_S3_ACCESS_KEY_ID`: Required under an `s3://` root.
- `FLEZHA_FILE_PLUGIN_S3_SECRET_ENV`: Names the variable holding the secret key; required under an `s3://` root. None of the four falls back to `FLEZHA_DURABILITY_S3_*`, and a root overlapping the durability tier's bucket prefix is refused.

Each `FLEZHA_PLUGIN_*` name is a row of the table like every other knob, so an
unset one leaves the row's own default standing rather than the family being
skipped as a block.

## Every variable

<!-- config-keys:begin environment -->
| Variable | Type | Default |
|---|---|---|
| `FLEZHA_AUDIT_DIR` | path | `storage/app/audit` |
| `FLEZHA_AUDIT_ENABLED` | bool | `false` |
| `FLEZHA_AUDIT_RETENTION_DAYS` | days | `30` |
| `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH` | path | derived |
| `FLEZHA_AUTHZ_DIR` | path | `storage/app/auth` |
| `FLEZHA_AUTHZ_ENABLED` | bool | `false` |
| `FLEZHA_AUTHZ_TOKEN_EXPIRY_WARNING_MS` | ms | `86400000` |
| `FLEZHA_AUTO_RESTORE_ENABLED` | bool | `true` |
| `FLEZHA_BOOT_REPLAY_TIMEOUT_MS` | ms | `60000` |
| `FLEZHA_CALL_PENDING_MAX` | positive_int | `10000` |
| `FLEZHA_CALL_RETENTION_MARGIN_MS` | ms | `300000` |
| `FLEZHA_CA_CERT_PATH` | path | `storage/app/certs/ca.pem` |
| `FLEZHA_CLIENT_CERT_PATH` | path | `storage/app/certs/client.pem` |
| `FLEZHA_CLIENT_KEY_PATH` | path | `storage/app/certs/client.key` |
| `FLEZHA_CLUSTER_DISCOVERY` | string | `manual_discovery` |
| `FLEZHA_CLUSTER_FORMATION_TIMEOUT_MS` | ms | `15000` |
| `FLEZHA_CLUSTER_UPDATE_INTERVAL` | ms | `5000` |
| `FLEZHA_COMPACT_AFTER_AGE_MS` | ms | `300000` |
| `FLEZHA_COMPACT_AFTER_BYTES` | int | `8388608` |
| `FLEZHA_CONCURRENT_CHANNEL_COUNT` | positive_int | derived at boot |
| `FLEZHA_CONTROLLER_ENABLED` | bool | `true` |
| `FLEZHA_CONTROL_KEEPALIVE_MS` | ms | `15000` |
| `FLEZHA_CONTROL_LIVENESS_INTERVAL_MS` | ms | `15000` |
| `FLEZHA_CONTROL_LIVENESS_WINDOW_MS` | ms | derived |
| `FLEZHA_CONVERGENCE_CELL_STALL_MS` | ms | `300000` |
| `FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS` | ms | `60000` |
| `FLEZHA_CONVERGENCE_TICK_MS` | ms | `5000` |
| `FLEZHA_CONVERGENCE_UNMET_INTENT_GRACE_MS` | ms | `120000` |
| `FLEZHA_CREDIT_FLOW_CONTROL` | bool | `true` |
| `FLEZHA_DATA_HEARTBEAT_MS` | ms | `10000` |
| `FLEZHA_DATA_PLANE_LIVENESS` | bool | `true` |
| `FLEZHA_DLQ_DIR` | path | `storage/app/dlq` |
| `FLEZHA_DLQ_RETENTION_DAYS` | days | `30` |
| `FLEZHA_DURABILITY_BACKEND` | one of `none`, `postgres`, `s3`, `local_path`, `unset` | `none` |
| `FLEZHA_DURABILITY_GC_ABANDONED_AFTER_MS` | ms | `604800000` |
| `FLEZHA_DURABILITY_GC_GRACE_MS` | ms | `3600000` |
| `FLEZHA_DURABILITY_GC_INTERVAL_MS` | ms | `3600000` |
| `FLEZHA_DURABILITY_PATH` | path | unset |
| `FLEZHA_DURABILITY_PG_AUTO_CREATE` | bool | `true` |
| `FLEZHA_DURABILITY_PG_DATABASE` | string | `flezha` |
| `FLEZHA_DURABILITY_PG_HOST` | string | `localhost` |
| `FLEZHA_DURABILITY_PG_PASSWORD_ENV` | string | unset |
| `FLEZHA_DURABILITY_PG_PORT` | int | `5432` |
| `FLEZHA_DURABILITY_PG_SSL` | bool | `false` |
| `FLEZHA_DURABILITY_PG_TABLE` | string | `flezha_durability_artifacts` |
| `FLEZHA_DURABILITY_PG_USERNAME` | string | `flezha` |
| `FLEZHA_DURABILITY_RPO_TARGET_MS` | ms | `120000` |
| `FLEZHA_DURABILITY_S3_ACCESS_KEY_ID` | string | unset |
| `FLEZHA_DURABILITY_S3_BUCKET` | string | unset |
| `FLEZHA_DURABILITY_S3_ENDPOINT` | string | unset |
| `FLEZHA_DURABILITY_S3_PREFIX` | string | `flezha` |
| `FLEZHA_DURABILITY_S3_REGION` | string | `us-east-1` |
| `FLEZHA_DURABILITY_S3_SECRET_ENV` | string | unset |
| `FLEZHA_DURABILITY_SHIP_INTERVAL_MS` | ms | `30000` |
| `FLEZHA_DURABILITY_WRITER_ID` | string | derived at boot |
| `FLEZHA_EFFECT_MAX_BYTES` | int | `4194304` |
| `FLEZHA_EXTERNAL_PROCESSES` | list of string | `""` |
| `FLEZHA_FABRIC_503_POLLS` | int | `3` |
| `FLEZHA_FILE_PLUGIN_ROOT` | path | `storage/app/files` |
| `FLEZHA_FILE_PLUGIN_S3_ACCESS_KEY_ID` | string | unset |
| `FLEZHA_FILE_PLUGIN_S3_ENDPOINT` | string | unset |
| `FLEZHA_FILE_PLUGIN_S3_REGION` | string | `us-east-1` |
| `FLEZHA_FILE_PLUGIN_S3_SECRET_ENV` | string | unset |
| `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` | bool | `true` |
| `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` | ms | `60000` |
| `FLEZHA_GROUP_WITHDRAW_GRACE_MS` | ms | `5000` |
| `FLEZHA_HEALTHZ_BIND` | string | `0.0.0.0` |
| `FLEZHA_HEALTHZ_ENABLED` | bool | `false` |
| `FLEZHA_HEALTHZ_PORT` | int | `9569` |
| `FLEZHA_INITIAL_NODES` | list of atom | `""` |
| `FLEZHA_INPUT_LOG_HOLD_WINDOW_MS` | ms | `0` |
| `FLEZHA_INPUT_SILENCE_TIMEOUT_MS` | ms | `0` |
| `FLEZHA_K8S_CLUSTER_DOMAIN` | string | `FLEZHA_K8S_CLUSTER_NAME` |
| `FLEZHA_K8S_CLUSTER_NAME` | string | `cluster` |
| `FLEZHA_K8S_HOST_IDENTIFIER` | one of `ip`, `hostname`, `dns` | `ip` |
| `FLEZHA_K8S_IP_LOOKUP_MODE` | one of `endpoints`, `pods` | `pods` |
| `FLEZHA_K8S_MASTER_NAME` | string | `""` |
| `FLEZHA_K8S_MASTER_PORT` | int | `6443` |
| `FLEZHA_K8S_NAMESPACE` | string | `flezha` |
| `FLEZHA_K8S_SELECTOR` | string | `app=flezha` |
| `FLEZHA_K8S_SERVICE_ACCOUNT_PATH` | path | `/var/run/secrets/kubernetes.io/serviceaccount` |
| `FLEZHA_K8S_SERVICE_NAME` | string | `flezha` |
| `FLEZHA_LISTENER_PENDING_BYTES` | positive_int | `4194304` |
| `FLEZHA_LISTENER_PENDING_FRAMES` | positive_int | `4096` |
| `FLEZHA_LOG_KEEP_FILES` | positive_int | `5` |
| `FLEZHA_LOG_LEVEL` | one of `debug`, `info`, `warn`, `error` | `info (controller), warn (runner)` |
| `FLEZHA_LOG_MAX_BYTES` | int | `10485760` |
| `FLEZHA_LOG_PATH` | path | `storage/logs/flezha.log` |
| `FLEZHA_MAX_RECONNECTS` | positive_int | `5` |
| `FLEZHA_MIGRATION_CONSENT_TIMEOUT_MS` | ms | `5000` |
| `FLEZHA_MIGRATION_MAX_DEFER_MS` | ms | unset |
| `FLEZHA_MIGRATION_RETRY_INTERVAL_MS` | ms | `2000` |
| `FLEZHA_NODE_NAME` | string | `flezha` |
| `FLEZHA_OBSERVABILITY_ENABLED` | bool | `true` |
| `FLEZHA_OBSERVABILITY_LOG_SOURCES` | list of atom | `""` |
| `FLEZHA_OTLP_ENABLED` | bool | `false` |
| `FLEZHA_OTLP_ENDPOINT` | string | unset |
| `FLEZHA_OUTPUT_WATCHDOG_INTERVAL_MS` | ms | `10000` |
| `FLEZHA_OVERWATCH_PING_TIMEOUT_MS` | ms | `2000` |
| `FLEZHA_PLACEMENT_LOAD_CAP_PERCENT` | positive_int | `100` |
| `FLEZHA_PLUGINS` | term | unset |
| `FLEZHA_PLUGIN_BACKLOG_BYTES` | positive_int | `4194304` |
| `FLEZHA_PLUGIN_BACKLOG_FRAMES` | positive_int | `4096` |
| `FLEZHA_PLUGIN_IDLE_STOP_MS` | ms | `60000` |
| `FLEZHA_PLUGIN_INGEST_WINDOW` | positive_int | `64` |
| `FLEZHA_PLUGIN_MAX_PACKAGE_BYTES` | int | `50000000` |
| `FLEZHA_PLUGIN_REQUIRE_SIGNATURE` | bool | `false` |
| `FLEZHA_PLUGIN_ROOT` | path | `storage/app/plugins` |
| `FLEZHA_PLUGIN_TRUSTED_KEYS` | PEM file paths | `""` |
| `FLEZHA_PROCESS_HUB_CLUSTER_SETTLE_MS` | ms | `2000` |
| `FLEZHA_PROCESS_HUB_NODEDOWN_GRACE_MS` | ms | `30000` |
| `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` | ms | `45000` |
| `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS` | ms | `30000` |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | path | `storage/app/process_hub` |
| `FLEZHA_PROMETHEUS_BIND` | string | `127.0.0.1` |
| `FLEZHA_PROMETHEUS_ENABLED` | bool | `false` |
| `FLEZHA_PROMETHEUS_PORT` | int | `9568` |
| `FLEZHA_QUIC_CLIENT_CA` | path | `storage/app/certs/ca.pem` |
| `FLEZHA_QUIC_LISTEN_ADDRESS` | address | `0.0.0.0:4433` |
| `FLEZHA_QUORUM_NODES` | positive_int | unset |
| `FLEZHA_QUORUM_PERCENT` | positive_int | `51` |
| `FLEZHA_QUORUM_WINDOW_MS` | ms | `30000` |
| `FLEZHA_RECONNECT_INTERVAL` | ms | `1000` |
| `FLEZHA_RECONNECT_MAX_BACKOFF_MS` | ms | `30000` |
| `FLEZHA_RETENTION_CHECK_INTERVAL_MS` | ms | `60000` |
| `FLEZHA_RUNNER_REMOVAL_GRACE_MS` | ms | `30000` |
| `FLEZHA_RUNNER_RETIRING_MARK_TTL_MS` | ms | `30000` |
| `FLEZHA_SCHEDULE_DIR` | path | `storage/app/schedules` |
| `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS` | ms | `120000` |
| `FLEZHA_SERVER_CERT_PATH` | path | `storage/app/certs/server.pem` |
| `FLEZHA_SERVER_KEY_PATH` | path | `storage/app/certs/server.key` |
| `FLEZHA_SOCKET_DIR_GROUP` | string | unset |
| `FLEZHA_SOCKET_FILES_PATH` | path | derived per run |
| `FLEZHA_SPLIT_BEHAVIOUR` | one of `divergence`, `static`, `dynamic` | `divergence` |
| `FLEZHA_STATE_DIR` | path | `storage/app/state` |
| `FLEZHA_STEPS_ACK_TIMEOUT_MS` | ms | `30000` |
| `FLEZHA_STEPS_RETENTION_HOURS` | int | `72` |
| `FLEZHA_STREAM_CHANNEL_MAX_HEAP_BYTES` | int | `0` |
| `FLEZHA_TRANSPORT` | one of `uds`, `quic` | `uds` |
| `FLEZHA_TRANSPORTS` | list of one of `uds`, `quic` | derived |
| `FLEZHA_TRANSPORT_AUTH` | bool | `true` |
| `FLEZHA_USE_CONVERGENCE_HEALTH` | bool | `true` |
<!-- config-keys:end -->

## Build-time and image variables

Read by the build, the image or the shell wrappers rather than the controller.

- `RUSTLER_SKIP_COMPILATION`: `true` makes the build pick up a pre-built `priv/native/flz_nif.so` instead of invoking cargo (prod image Stage 2).
- `FLEZHA_RESTORE_WAIT_TIMEOUT_S`: The image entrypoint's own contract; it bounds the wait for the controller's control socket.
- `FLEZHA_SHUTDOWN_TOKEN_FILE`: A file holding an `admin@_system` token, presented by the SIGTERM trap's `flz controller stop`; without it an enforcing controller refuses the graceful stop and the orchestrator kills the BEAM at its grace period.
- `FLEZHA_TESTNET_WS_PORT`: The port the WS testnet daemon binds. `:test` derives it from the run (`4500` outside `:test`); the entrypoint starts the daemon only when it is set explicitly.
- `FLEZHA_NODE_NAME`: The base of the node name `<name>@<host>`. `:test` derives it from the run (`flezha_test_<pid>`; `flezha` outside `:test`) and starts distribution under it; the release script builds `RELEASE_NODE` from it.

<!-- config-keys:begin build -->
| Variable | Type | Default |
|---|---|---|
| `FLEZHA_CONSOLE_PATH` | path | unset |
| `FLEZHA_CONTAINER_USER` | string | unset |
| `FLEZHA_COOKIE_FILE` | path | unset |
| `FLEZHA_DIST_TLS_OPTFILE` | path | unset |
| `FLEZHA_GIT_MIRROR` | path | unset |
| `FLEZHA_IMAGE` | string | unset |
| `FLEZHA_NODE_HOST` | string | `127.0.0.1` |
| `FLEZHA_NODE_NAME` | string | `flezha` |
| `FLEZHA_REBUILD_IMAGE` | bool | `false` |
| `FLEZHA_RESTORE_WAIT_TIMEOUT_S` | int | `60` |
| `FLEZHA_SHUTDOWN_TOKEN_FILE` | path | unset |
| `FLEZHA_STATE_DIR` | path | `storage/app/state` |
| `FLEZHA_STORAGE_ROOT` | path | `storage` |
| `FLEZHA_TESTNET_WS_PORT` | int | `4500` |
<!-- config-keys:end -->
