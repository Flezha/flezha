# Cluster Snapshot & Restore

`flz cluster snapshot` and `flz cluster restore` are the supported way to
survive a planned restart of one or more nodes in a Flezha cluster. The
executor runs the command on any node; the controller on that node fans out RPC
calls to its reachable peers, gathers their hosted state, and writes one
file. Restore is the inverse.

## Commands

```sh
flz cluster snapshot <file> [--peer-timeout-ms N] [--strict] [--namespace NS]
flz cluster restore  <file> [--warn-if-older-than DURATION] [--namespace NS] [--peer-timeout-ms N]
```

`<file>` is an absolute path on the node executing the command. The same
file must be reachable from the node performing the restore — this is
typically a shared volume in containerised deployments.

### snapshot flags

| Flag | Default | Behaviour |
|------|---------|-----------|
| `--peer-timeout-ms` | `5000` | Per-peer RPC timeout. Peers that do not respond within this window are logged at WARN and skipped. |
| `--strict` | best-effort | When set, any peer failure aborts the snapshot and writes nothing. Use this for backups; omit for rolling deploys. |
| `--namespace` | cluster-wide | Capture only entries belonging to `NS`. Other namespaces are pruned from the file (relays, plugins, captured states). Mirrors restore's `--namespace`; pass it on both ends for symmetric per-namespace backups. |

### restore flags

| Flag | Default | Behaviour |
|------|---------|-----------|
| `--warn-if-older-than` | infinity | Logs WARN if the snapshot's age exceeds the threshold; restore still proceeds. Accepts `Ns`, `Nm`, `Nh`, `Nd` suffixes (e.g. `24h`, `7d`). |
| `--namespace` | all | Restrict apply to entries in `NS`. Other namespaces are listed at INFO and skipped. |
| `--peer-timeout-ms` | `5000` | Per-peer apply RPC timeout. |

## File format (high level)

The on-disk file is the binary produced by
`:erlang.term_to_binary({:flezha_snapshot, version, payload}, [:compressed])`.
`version` starts at `1`. Reading code pattern-matches the outer tuple — a
mismatched version is a hard error (no auto-migration). The `payload` map
carries the gather time, the cluster's reachable nodes, the per-relay
builders + `last_observed_node`, the per-plugin entries, and the
per-relay captured state binaries (subject to the size cap, see below).

Executors do not need to read the file directly. To inspect contents
ad-hoc:

```sh
erl -noshell -eval '
  {ok, Bin} = file:read_file("/tmp/snap.flz"),
  {flezha_snapshot, 1, Payload} = erlang:binary_to_term(Bin),
  io:format("~p relays, ~p plugins, taken_at=~p~n",
            [length(maps:get(relays, Payload)),
             length(maps:get(plugins, Payload)),
             maps:get(taken_at_ms, Payload)]),
  halt().
'
```

## Executor flow

### Graceful shutdown

```sh
flz cluster snapshot /var/lib/flezha/snapshots/pre-shutdown.flz
# stop your controller / restart hosts
flz cluster restore  /var/lib/flezha/snapshots/pre-shutdown.flz
```

### Backups

```sh
flz cluster snapshot /backups/$(date +%Y%m%d-%H%M%S).flz --strict
```

`--strict` aborts on any peer failure. The previous successful snapshot
file is left untouched.

### Rolling deploys

Use the default best-effort mode. Peers that are mid-restart are logged at
WARN; the snapshot is written with what was reachable.

## Restart-survival model

`flz cluster snapshot/restore` is the **only** restart-survival
mechanism. The BEAM itself does not auto-replay — an controller process that
comes back up after a crash starts with no relays unless an
executor (or the image-level entrypoint, see below) runs `flz cluster
restore`. The supported flow is:

- **Planned shutdowns:** snapshot before, restore after.
- **Crash recovery:** maintain a recent snapshot file (cron, pre-stop
  hooks, etc.) and run `restore` against it after the controller is back.
- **Cold start:** delete the snapshot file (or just don't run restore).
  Relays that subsequently get started come up with their SDK's
  default empty state.

### Image-level entrypoint

The published `:latest` and `:latest-dev` images ship an entrypoint
(`/usr/local/bin/controller-entrypoint.sh`) and a pre-stop hook
(`/usr/local/bin/snapshot-on-stop.sh`) that wire the snapshot/restore
flow into the container lifecycle:

- **On start**: if `${FLEZHA_SNAPSHOT_DIR}/latest.flz` exists, the
  entrypoint runs `flz cluster restore` after the controller is up. If
  `FLEZHA_RESTORE_WAIT_SOCKET_GLOB` is set, it first waits up to
  `FLEZHA_RESTORE_WAIT_TIMEOUT_S` for a matching SDK socket — required
  when an external compute engine must attach before relays can be
  re-spawned.
- **On SIGTERM / Kubernetes preStop**: the pre-stop hook calls
  `flz cluster snapshot` and atomically writes `latest.flz`.
- **Disabling**: set `FLEZHA_SNAPSHOT_ENABLED=false` to skip both ends
  (image runs as if no entrypoint plumbing existed).

See `docs/deployment.md` for the full env-var contract.

Plugins whose state is bound to a caller pid that died with the previous
BEAM (`relay_beam_msg`, `monitored_io_forward`) are excluded from
the snapshot and must be re-enabled by the executor or SDK reconnect
path after restore.

## Configuration

| Application env key | Env var | Default | Notes |
|---------------------|---------|---------|-------|
| `:snapshot_state_cap_bytes` | `FLEZHA_SNAPSHOT_STATE_CAP_BYTES` | `8388608` (8 MB) | Per-relay cap on captured-state size in the snapshot. Relays above the cap are written without state and a WARN names them. |

## Schema versioning

The file's outer tuple carries a single integer version (`1` today).
Reading code in `Flezha.Cluster.Snapshot.Encoding.decode/1` is the only
place that pattern-matches the version. A future controller that supports a new
version is expected to reject older versions cleanly with an explicit
`{:error, {:unsupported_version, got, [supported]}}` — executors are
expected to delete the file (cold start) rather than rely on automatic
migration.
