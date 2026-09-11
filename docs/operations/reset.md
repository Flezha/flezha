# Resetting Controller runtime data

For operators who need a Controller to boot from a clean slate. `flz reset`
clears the durable on-disk state under `app_data/` — every Entity's
WAL/snapshot/outbox files and the ProcessHub durable registry, optionally the
TLS material, parked DLQ records, schedules and UDS sockets — without deleting
`app_data/` itself. It refuses to run while the Controller is up and never
reaches into a remote durability backend.

## Scope

`flz reset` resolves every `app_data/` root as `<app_data root>/<name>`.
The root is `--app-data-root`, else `FLEZHA_APP_DATA_ROOT`, else `./app_data`.
Per-store path overrides such as `FLEZHA_PROCESS_HUB_REGISTRY_ROOT`,
`FLEZHA_DLQ_DIR` and `FLEZHA_SCHEDULE_DIR` are not consulted: a store
relocated that way is not found and not cleared, `--include-dlq` and
`--include-schedules` included.

Which subtrees exist and how each is treated is one declaration, read by both
runtimes: `lib/flezha/layout/table.ex` on the controller's side and
`cli/src/reset/plan.rs` on this one. Every row is reported on every run,
cleared or kept, so the summary names everything a reset left behind.

The socket directory is the exception, because the Controller resolves it from
a different source. `flz reset` walks the same ladder the Controller, every
runner and `flz runner check` walk, and clears the directory that is actually
in use.

| Directory                     | Contents                               | Cleared               |
| ----------------------------- | -------------------------------------- | --------------------- |
| `app_data/state/`             | per-Entity WAL, snapshot, outbox files | by default            |
| `app_data/process_hub/`       | ProcessHub durable registry (`.dets`)  | by default            |
| `app_data/certs/`             | TLS material                           | `--include-certs`     |
| `app_data/dlq/`               | parked dead-letter records             | `--include-dlq`       |
| `app_data/schedules/`         | every schedule                         | `--include-schedules` |
| the resolved socket directory | Flezha's own UDS socket files          | `--include-sockets`   |
| `app_data/auth/`              | tokens, role bindings, namespaces      | never                 |
| `app_data/audit/`             | the audit trail                        | never                 |
| `app_data/plugins/`           | installed plugins                      | never                 |
| `logs/`                       | the controller's log file              | never                 |

`--all` is every `--include-` flag above. A `never` row is reported as kept,
`not reset's to clear`, whatever the flags: clearing `auth/` would destroy an
operator's tokens, and an audit trail one command can erase is a weaker audit
trail. `logs/` is named where the log file's default (`logs/flezha.log`) puts
it, relative to the working directory: it is not under the data root, and
`--app-data-root` does not move it.

## The socket directory

The ladder's rungs, in order — the first that answers wins:

1. `$FLEZHA_SOCKET_FILES_PATH`, used outright if set.
2. `$XDG_RUNTIME_DIR/flezha` — set in every desktop and systemd session, so
   this is the usual answer.
3. `/run/flezha` — a Controller running as a system service.
4. `app_data/sockets` — the repository fallback.

Three consequences an operator should expect:

- **`--app-data-root` does not move the socket directory.** The two are
  resolved from different sources because the Controller resolves them from
  different sources. A run that cleared `/run/user/1000/flezha/` while
  `--app-data-root` pointed elsewhere is the ladder answering, not a bug — the
  summary names the rung beside the path, so you can see which one did.
- **A Controller still serving in that directory keeps it.** Several
  Controllers may share a machine, and therefore a runtime directory. Before
  unlinking anything, `flz reset` runs the same probe as `flz runner check`; a
  control socket that answers means someone is serving there, so the socket
  root is reported `kept` and the rest of the reset proceeds. The live-node
  guard below cannot cover this: it asks about *this* Controller's BEAM node,
  and a colocated peer is a different node. A socket file nothing is listening
  on is the stale socket this command exists to clear, and is cleared. A
  directory this user may not enter, or a socket that refuses for another
  reason, is reported and skipped rather than forced.
- **Only Flezha's own socket files are unlinked** — the control socket and the
  per-runner stream sockets. The other roots are subdirectories of an
  `app_data/` root this command owns, so clearing their whole contents is safe;
  a directory you named through `$FLEZHA_SOCKET_FILES_PATH` may hold things
  that are not Flezha's, and those are left in place, as is the directory
  itself.

If the ladder cannot resolve a directory at all, the socket root is reported
`kept` with the reason and the remaining roots are still cleared. Not knowing
which directory holds the sockets is not an I/O error on a path the command was
asked to clear, and does not change the exit code.

Asking the ladder establishes a directory the way a Controller boot does, so
`flz reset` consults it only when `--include-sockets` (or `--all`) is in
effect. A run that was not asked about sockets does not resolve one.

## Command surface

```text
flz reset [OPTIONS]
```

| Flag                     | Meaning                                                                        |
| ------------------------ | ------------------------------------------------------------------------------ |
| `--yes`                  | Required to apply. Without it the command prints the plan and exits 1.        |
| `--dry-run`              | Print the plan without touching the filesystem; exits 0.                      |
| `--include-certs`        | Also clear `app_data/certs/*`.                                                 |
| `--include-dlq`          | Also clear `app_data/dlq/*`: every parked dead-letter record.                 |
| `--include-schedules`    | Also clear `app_data/schedules/*`: every schedule.                            |
| `--include-sockets`      | Also unlink Flezha's socket files in the resolved socket directory (see above). |
| `--all`                  | `--include-certs --include-dlq --include-schedules --include-sockets`.         |
| `--app-data-root <PATH>` | Override the `app_data/` root (precedence over `FLEZHA_APP_DATA_ROOT`).        |
| `--probe-timeout-ms <N>` | Live-node probe timeout in ms (default `1000`).                                |
| `--json`                 | Machine-readable single-object output (`--output json` is the same request).   |

`--yes` together with `--dry-run` is a usage error; so is any unknown argument —
`--output` with a value other than `text` or `json` included.

## Live-node guard

The command probes the configured node with
`flzvm rpc 'Flezha.Service.CliManager.controller_status("text")'`. Exit 0 with
non-empty output means "up" — the reset is refused with exit 4. Any other
outcome (no node, timeout, distribution failure) means "not up" and the reset
proceeds. There is no override flag: stop a wedged Controller with
`flz controller stop --force` first. `flz auth rebootstrap` runs behind the
same probe.

This guard is deliberately not widened to cover a Controller colocated on the
same machine: a peer is a different node with its own durable state, and that
state is not this reset's to protect. What a peer shares is the socket
directory, and that is guarded where it is touched.

## Filesystem semantics

- Only the **contents** of each root are removed; the root directory and
  `app_data/` itself are never unlinked, so bind mounts, PVCs and file watchers
  stay valid. In the socket directory, only Flezha's own socket files are
  removed.
- A path-equality guard (after symlink canonicalisation) refuses to clear the
  configured root itself or the same path twice — a malformed or aliased
  `--app-data-root` exits 1 before anything is touched.
- A missing root is success: `flz reset --yes` on a checkout with no
  `app_data/` exits 0.
- The file/byte counts are computed over the same entry list in dry-run and
  apply mode, so the preview matches the action.

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
kept        ./app_data/dlq/                                  (use --include-dlq to clear)
kept        ./app_data/schedules/                            (use --include-schedules to clear)
kept        socket directory                                 (use --include-sockets to clear)
kept        ./app_data/auth/                                 (not reset's to clear)
kept        ./app_data/audit/                                (not reset's to clear)
kept        ./app_data/plugins/                              (not reset's to clear)
kept        logs/                                            (not reset's to clear)
reset complete in 41 ms
```

`--json`:

```json
{"cleared":[{"root":"./app_data/state","files":12,"bytes":4509876},{"root":"./app_data/process_hub","files":2,"bytes":16384}],"kept":[{"root":"./app_data/certs","reason":"use --include-certs to clear"},{"root":"./app_data/dlq","reason":"use --include-dlq to clear"},{"root":"./app_data/schedules","reason":"use --include-schedules to clear"},{"root":"socket directory","reason":"use --include-sockets to clear"},{"root":"./app_data/auth","reason":"not reset's to clear"},{"root":"./app_data/audit","reason":"not reset's to clear"},{"root":"./app_data/plugins","reason":"not reset's to clear"},{"root":"logs","reason":"not reset's to clear"}],"duration_ms":41,"dry_run":false}
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
