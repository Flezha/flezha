# Resetting Controller runtime data

For operators who need a Controller to boot from a clean slate. `flz reset`
clears the durable on-disk state under `app_data/` — every Entity's
WAL/snapshot/outbox files and the ProcessHub durable registry, optionally the
TLS material and UDS sockets — without deleting `app_data/` itself. It refuses
to run while the Controller is up and never reaches into a remote durability
backend.

## Scope

`flz reset` resolves every root as `<app_data root>/<name>`. The root is
`--app-data-root`, else `FLEZHA_APP_DATA_ROOT`, else `./app_data`. Per-store
path overrides such as `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` or
`FLEZHA_SOCKET_FILES_PATH` are not consulted: a store relocated that way is not
found and not cleared.

| Directory               | Contents                               | Cleared             |
| ----------------------- | -------------------------------------- | ------------------- |
| `app_data/state/`       | per-Entity WAL, snapshot, outbox files | by default          |
| `app_data/process_hub/` | ProcessHub durable registry (`.dets`)  | by default          |
| `app_data/certs/`       | TLS material                           | `--include-certs`   |
| `app_data/sockets/`     | UDS socket files                       | `--include-sockets` |

`--all` is `--include-certs --include-sockets`. Every other `app_data/`
subdirectory (`auth/`, `audit/`, `dlq/`, `schedules/`, `plugins/`) is left
alone.

## Command surface

```text
flz reset [OPTIONS]
```

| Flag                     | Meaning                                                                        |
| ------------------------ | ------------------------------------------------------------------------------ |
| `--yes`                  | Required to apply. Without it the command prints the plan and exits 1.        |
| `--dry-run`              | Print the plan without touching the filesystem; exits 0.                      |
| `--include-certs`        | Also clear `app_data/certs/*`.                                                 |
| `--include-sockets`      | Also clear `app_data/sockets/*`.                                               |
| `--all`                  | `--include-certs --include-sockets`.                                           |
| `--app-data-root <PATH>` | Override the `app_data/` root (precedence over `FLEZHA_APP_DATA_ROOT`).        |
| `--probe-timeout-ms <N>` | Live-node probe timeout in ms (default `1000`).                                |
| `--json`                 | Machine-readable single-object output.                                         |

`--yes` together with `--dry-run` is a usage error; so is any unknown argument.

## Live-node guard

The command probes the configured node with
`flzvm rpc 'Flezha.Service.CliManager.controller_status("text")'`. Exit 0 with
non-empty output means "up" — the reset is refused with exit 4. Any other
outcome (no node, timeout, distribution failure) means "not up" and the reset
proceeds. There is no override flag: stop a wedged Controller with
`flz controller stop --force` first. `flz auth rebootstrap` runs behind the
same probe.

## Filesystem semantics

- Only the **contents** of each root are removed; the root directory and
  `app_data/` itself are never unlinked, so bind mounts, PVCs and file watchers
  stay valid.
- A path-equality guard (after symlink canonicalisation) refuses to clear the
  configured root itself or the same path twice — a malformed or aliased
  `--app-data-root` exits 1 before anything is touched.
- A missing root is success: `flz reset --yes` on a checkout with no
  `app_data/` exits 0.
- The file/byte counts are computed by the same walk in dry-run and apply
  mode, so the preview matches the action.

## Exit codes

| Code | Meaning                                                                          |
| ---- | -------------------------------------------------------------------------------- |
| `0`  | Applied, or `--dry-run` preview.                                                 |
| `1`  | Usage: no `--yes` and no `--dry-run`, conflicting flags, unknown argument, root guard refusal. |
| `2`  | Partial: at least one root was cleared, then a filesystem error stopped the walk. |
| `3`  | I/O error before anything was removed.                                           |
| `4`  | Refused: the Controller is up.                                                   |

On error the remaining roots are reported as `kept (not reached: earlier error)`
and the failing path is printed to stderr.

## Output

Human (default; `would clear` / `would keep` in plan-only mode):

```text
cleared     ./app_data/state/                                (12 files, 4.3 MiB)
cleared     ./app_data/process_hub/                          (2 files, 16.0 KiB)
kept        ./app_data/certs/                                (use --include-certs to clear)
kept        ./app_data/sockets/                              (use --include-sockets to clear)
reset complete in 41 ms
```

`--json`:

```json
{"cleared":[{"root":"./app_data/state","files":12,"bytes":4509876},{"root":"./app_data/process_hub","files":2,"bytes":16384}],"kept":[{"root":"./app_data/certs","reason":"use --include-certs to clear"},{"root":"./app_data/sockets","reason":"use --include-sockets to clear"}],"duration_ms":41,"dry_run":false}
```

`dry_run` is `true` for both `--dry-run` and a bare invocation.

## A clean slate needs `FLEZHA_DURABILITY_BACKEND=none`

With a remote durability backend configured, the next boot hydrates the
control-plane stores and the registry cspec from the remote store before the
hub starts, and the operatives it names re-hydrate their state at bind — the
wiped state, and the record of what exists, comes straight back. To start
fresh, set the backend to `none` for that boot or purge the remote
prefix/table first. See
[`remote_durability_tier.md`](remote_durability_tier.md).

## Recipes

Local:

```bash
flz controller stop
flz reset --yes
flz controller start
```

docker compose (a bind-mounted `app_data/` survives — only its contents go):

```bash
docker compose exec controller flz controller stop
docker compose exec controller flz reset --yes
docker compose restart controller
```

Kubernetes (the PVC filesystem is preserved; `kubectl delete pvc` disposes of
it entirely):

```bash
kubectl exec -n flezha deploy/controller -- flz controller stop
kubectl exec -n flezha deploy/controller -- flz reset --yes
kubectl rollout restart -n flezha deploy/controller
```

CI / integration tests: `flz reset --dry-run --json` never modifies anything
and does not need the Controller down — use it as a starting-state assertion.

Back up first when in doubt: `tar -czf backup.tgz app_data/`.

## Out of scope

- Scoped reset while the Controller is up ("wipe just operative X").
- Trimming time-travel history: `flz state retention <operative_class> <hours>`.
- Cluster-wide reset: run `flz reset` on each node.
- Purging the remote durability backend: shipped artifacts and manifests stay
  and re-hydrate on the next boot unless the backend is disabled or purged out
  of band.

## See also

- [`graceful_shutdown.md`](graceful_shutdown.md) — `flz controller stop` and its exit codes.
- [`remote_durability_tier.md`](remote_durability_tier.md) — what re-hydrates after a reset.
- [`auto_restore_on_boot.md`](auto_restore_on_boot.md) — what the next boot does with whatever is left on disk.
- [`../internals/state_persistence.md`](../internals/state_persistence.md) — the `app_data/state/` layout.
