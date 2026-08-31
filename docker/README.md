# Docker

Three image stacks, one per concern. Don't mix them up.

| Directory  | Purpose                                                              | Used by                                       |
|------------|----------------------------------------------------------------------|-----------------------------------------------|
| `dev/`     | Local dev/runtime: controller + Rust compute engine + Kafka UI + testnet  | `docker compose -f docker/dev/docker-compose.yml up` |
| `prod/`    | Production controller image                                               | `docker compose -f docker/prod/docker-compose.yml up` |
| `build/`   | Pinned cross-compile toolchain for `scripts/build-dist.sh --docker`  | release artifact build; never long-running    |

The distribution artifacts a release publishes (5× native binaries + 2 Java fat
jars — build output, tracked nowhere) are produced exclusively by
`build/Dockerfile.dist` (or the host toolchain) — never by `dev/` or `prod/`.
Day-to-day work needs only the host triple, from `scripts/build-native.sh`. See
[`build/README.md`](build/README.md) for the build flow and
[`dev/README.md`](dev/README.md) for the runtime flow.

## Failure recovery volume

The controller recovers its own operatives and their plugins on boot from
ProcessHub's durable registry — the enable is written into the operative's
child spec (`Flezha.Service.PluginManager.enable_plugin/2`), so one record
carries both. Per-Entity state comes from the WAL and snapshot files beside it.

Two directories must therefore survive a container restart, and the prod
`docker-compose.yml` gives each a named volume:

- `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` (`/flezha/app_data/process_hub`,
  volume `flezha_process_hub`) — ProcessHub's persistent registry.
- `FLEZHA_STATE_DIR` (`/flezha/app_data/state`, volume `flezha_state`) —
  per-Entity WAL and snapshot files.

If you override either path, make sure the new location is (a) persistent
across container restarts and (b) owned by the non-root `flezha` user
(uid 1000) that the image runs as. Without persistence the controller comes
up empty and waits for SDK clients to reissue `AddOperative` and
`enable_plugin`.

Set `FLEZHA_AUTO_RESTORE_ENABLED=false` to disable the boot-time replay while
still persisting registry mutations. See
[`../docs/operations/auto_restore_on_boot.md`](../docs/operations/auto_restore_on_boot.md)
for the recovery contract and its one tolerated loss, and
[`../docs/internals/state_persistence.md`](../docs/internals/state_persistence.md)
for the on-disk layout.
