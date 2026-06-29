# Docker

Three image stacks, one per concern. Don't mix them up.

| Directory  | Purpose                                                              | Used by                                       |
|------------|----------------------------------------------------------------------|-----------------------------------------------|
| `dev/`     | Local dev/runtime: controller + Rust compute engine + Kafka UI + testnet  | `docker compose -f docker/dev/docker-compose.yml up` |
| `prod/`    | Production controller image                                               | `docker compose -f docker/prod/docker-compose.yml up` |
| `build/`   | Pinned cross-compile toolchain for `scripts/build-dist.sh --docker`  | one-shot artifact refresh; never long-running |

The committed distribution artifacts (5× native binaries + 2 Java fat
jars) are produced exclusively by `build/Dockerfile.dist` (or the host
toolchain) — never by `dev/` or `prod/`. See
[`build/README.md`](build/README.md) for the build flow and
[`dev/README.md`](dev/README.md) for the runtime flow.

## Failure recovery volume

The controller persists the child specs of locally hosted operatives **and**
the recoverable plugins attached to them under `FLEZHA_DATA_DIR` (default
`/flezha/app_data/recovery` in the provided compose file). Two DETS files
live side-by-side in that directory:

- `recovery.dets` — operative child specs, keyed by `child_id`.
- `recovery_plugins.dets` — recoverable plugin bindings, keyed by
  `plugin_id`.

The prod `docker-compose.yml` declares a named volume `flezha_recovery`
mounted at `/flezha/app_data/recovery` — a single volume carries both
files. If you override the volume or data dir, make sure the new location
is (a) persistent across container restarts, and (b) owned by the
non-root `flezha` user (uid 1000) that the image runs as. Without a
persistent volume, recovery is a no-op and the cluster reverts to today's
behaviour (waiting for SDK clients to reissue `AddOperative` and
`enable_plugin`).

Set `FLEZHA_RECOVERY_ENABLED=false` in the environment to disable the
subsystem entirely.

Note that only a subset of plugins is recoverable — plugins whose
provider is bound to a caller pid (`operative_beam_msg`,
`monitored_io_forward`) are deliberately excluded and must be
re-established by the caller on reconnect. See the top-level
[`README.md`](../README.md#failure-recovery) for the per-provider table.