# Wiping Controller runtime data (`flz reset`)

`flz reset` clears the Controller's durable on-disk state under
`app_data/` so the next boot starts from a clean slate. It is the
operator-facing inverse of "first boot ever" — useful when an operator
wants to throw away every Partition's WAL/snapshot, every ProcessHub
durable registry, and (optionally) the on-disk TLS material and UDS
sockets.

It refuses to run while the Controller is up. Stop it first with
`flz controller stop` (see
[`../resilience/graceful_shutdown.md`](../resilience/graceful_shutdown.md)).

## Command surface

```
flz reset [OPTIONS]
```

By default clears the contents of:

| Directory                | Source                                    |
| ------------------------ | ----------------------------------------- |
| `app_data/state/`        | per-Partition WAL/snapshot/outbox files   |
| `app_data/process_hub/`  | ProcessHub durable registry (`.dets`)     |

Opt-in (off by default):

| Directory                | Flag                  |
| ------------------------ | --------------------- |
| `app_data/certs/`        | `--include-certs`     |
| `app_data/sockets/`      | `--include-sockets`   |

`--all` is a shorthand for `--include-certs --include-sockets`.

### Flags

| Flag                       | Meaning                                                                 |
| -------------------------- | ----------------------------------------------------------------------- |
| `--yes`                    | REQUIRED to apply. Without it, the command runs as a non-zero preview.  |
| `--dry-run`                | Print the plan without touching the filesystem; exits 0.                |
| `--include-certs`          | Also clear `app_data/certs/*`.                                          |
| `--include-sockets`        | Also clear `app_data/sockets/*`.                                        |
| `--all`                    | `--include-certs --include-sockets`.                                    |
| `--app-data-root <PATH>`   | Override the resolved `app_data/` root (mirrors `FLEZHA_APP_DATA_ROOT`). |
| `--probe-timeout-ms <N>`   | Live-node probe timeout in ms (default `1000`).                         |
| `--json`                   | Machine-readable single-object output.                                  |

`--yes` and `--dry-run` together is a usage error.

## Live-node guard

`flz reset` refuses to modify the filesystem while the Controller is
up. Detection runs `flzvm rpc 'Flezha.Service.CliManager.controller_status("text")'`
against the configured node; an exit-0 with parseable output is "up"
(refused, exit 4), any other outcome is "not up" (proceed).

There is no `--force-while-live` escape hatch. Operators with a wedged
Controller stop it first via `flz controller stop --force`, then run
`flz reset --yes`.

## Filesystem semantics

`flz reset` never deletes `app_data/` itself — only the contents of
its subdirectories. The parent inode is preserved so volume mounts,
file watchers, and docker bind-mounts remain valid. A path-equality
guard refuses to unlink the configured root or a cleared root itself
(e.g. if `--app-data-root` is malformed or aliased via symlink).

Missing directories are success, not error: running `flz reset --yes`
on a freshly-checked-out repo with no `app_data/` exits 0.

## Exit codes

| Code | Meaning                                                                             |
| ---- | ----------------------------------------------------------------------------------- |
| `0`  | Success (applied, or `--dry-run` preview).                                          |
| `1`  | Invalid usage (missing `--yes` without `--dry-run`, conflicting flags, bad path).   |
| `2`  | Partial success — some roots cleared, then a filesystem error stopped the walk.     |
| `3`  | I/O error before any action (e.g. `app_data/` exists but is unreadable).            |
| `4`  | Refused: Controller is up. Run `flz controller stop` first.                         |

## Output

Human (default):

```
cleared     app_data/state/                                  (12 files, 4.3 MiB)
cleared     app_data/process_hub/                            (2 files, 16.0 KiB)
kept        app_data/certs/                                  (use --include-certs to clear)
kept        app_data/sockets/                                (use --include-sockets to clear)
reset complete in 41 ms
```

`--json`:

```json
{"cleared":[{"root":"app_data/state","files":12,"bytes":4509876},{"root":"app_data/process_hub","files":2,"bytes":16384}],"kept":[{"root":"app_data/certs","reason":"use --include-certs to clear"},{"root":"app_data/sockets","reason":"use --include-sockets to clear"}],"duration_ms":41,"dry_run":false}
```

`--dry-run` (and absent `--yes`) substitute `would clear` / `would
keep`; the JSON `dry_run` flag flips to `true`. The walk that computes
counts is the same in dry-run and apply modes, so the preview matches
the action exactly.

## Recipes

### Local restart from a clean slate

```bash
flz controller stop
flz reset --yes
flz controller start
```

### docker-compose

```bash
docker compose exec controller flz controller stop
docker compose exec controller flz reset --yes
docker compose restart controller
```

Bind-mounting `app_data/` into the container (`-v
./app_data:/opt/flezha/app_data`) survives this command because we
never `rm` the mount point itself — only its contents.

### Kubernetes

```bash
kubectl exec -n flezha deploy/controller -- flz controller stop
kubectl exec -n flezha deploy/controller -- flz reset --yes
kubectl rollout restart -n flezha deploy/controller
```

If the Controller is fronted by a PVC, the PVC's filesystem is
preserved (we wipe contents, not the mount point). To dispose of the
PVC entirely, use `kubectl delete pvc <name>` instead.

### CI / integration tests

`flz reset --dry-run --json` is safe to run at any time — it never
modifies the filesystem and never requires the Controller to be down.
Use it as a starting-state assertion that the test harness has put
the right files in place.

## Out of scope

- **Scoped reset while the Controller is up** (e.g. "wipe just relay
  X"). Different safety model; deferred to a follow-up change.
- **Backups.** `tar -czf backup.tgz app_data/` before running.
- **Time-travel-aware trimming.** Use `flz state retention <relay> <hours>` instead.
- **Cluster-wide reset.** Single-node operation; run `flz reset` per
  node.
