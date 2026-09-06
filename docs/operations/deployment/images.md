# Container images

Building the production controller image, what it promises at runtime,
and publishing it.

## Images

| Tag | Built from | Purpose |
|---|---|---|
| `alfetahe/flezha:<version>` | `docker/prod/Dockerfile.controller` | Prod runtime, immutable. The version is `version:` in `mix.exs`. This is what a cluster pins. |
| `alfetahe/flezha:latest` | same build | Mutable convenience tag. Never pin a cluster to it. |
| `alfetahe/flezha:latest-dev` | `docker/dev/Dockerfile.runtime` | `FROM :latest` + Node.js + the pre-built testnet WS server, started only when `FLEZHA_TESTNET_WS_PORT` is set. |
| `alfetahe/flezha:latest-toolchain` | `docker/dev/Dockerfile.toolchain` | Elixir + Rust + Node + protoc shell over bind-mounted source, for hacking on Flezha itself. |

The prod Dockerfile has three stages:

```text
Stage 1  rust:1.87.0-slim-bookworm                   cargo build --release -p flz_nif -p flz
Stage 2  hexpm/elixir:1.17.3-erlang-27.2-…-slim      RUSTLER_SKIP_COMPILATION=true, mix release (ERTS included)
Stage 3  debian:bookworm-slim                        runtime: tini, libssl3, non-root flezha:flezha (uid/gid 1000)
```

The NIF is built once in Stage 1 and dropped into `priv/native/flz_nif.so`, so
Stage 2 never runs cargo. `bin/flzvm` is the release binary (renamed from
`bin/flezha` by `mix.exs`); `bin/flz` is the CLI, copied from
`target/release/flz` (`target/debug/flz` in a dev release).

### Runtime contract of the image

`ENTRYPOINT` is `tini`; `CMD` is `/usr/local/bin/controller-entrypoint.sh`,
which removes a stale control socket left on a reused volume, starts
`bin/flzvm start`, waits up to `FLEZHA_RESTORE_WAIT_TIMEOUT_S` (60) for
`runner_control_channel.sock` to appear — the transport being bound, and the
precondition for issuing any control RPC — then spends what is left of the same
budget on `flz boot await`, which returns only when the readiness verdict is
`:ready`. It exits 1 if the BEAM dies first, if the socket never appears, or if
the controller never reports ready — which is how a boot refusal, such as an
unset `FLEZHA_DURABILITY_BACKEND`, surfaces to the orchestrator. On
SIGTERM or SIGINT it runs
`flz controller stop --timeout-ms $FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`
(quiesce, drain, finalise); when `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` is
`false`/`0`/`no`/`off` it instead SIGTERMs the BEAM directly, bypassing the
coordinator. A `HEALTHCHECK` runs `flz boot await --timeout 0` every 10 s after a 120 s
start period.

The entrypoint writes and reads no cluster-wide snapshot file. Restart survival
comes from the durable ProcessHub registry and the per-Entity WAL — see
[`../auto_restore_on_boot.md`](../auto_restore_on_boot.md) — plus the remote
durability tier where one is configured.

Attach to a running container through the release's own entry points:

```bash
docker exec <container>     bin/flzvm rpc 'IO.inspect(node())'
docker exec -it <container> bin/flzvm remote
docker exec <container>     bin/flz controller status
```

## Production image build

```bash
VERSION=0.2.0 make docker-build
# = docker build -t alfetahe/flezha:0.2.0 -f docker/prod/Dockerfile.controller .
```

`process_hub` comes from hex at the version `mix.lock` pins; a plain clone is
all the build context needs. Set `FLEZHA_PROCESS_HUB_PATH` at build time to
build from a local checkout instead (`mix.exs`).

### Publishing

Images are built natively on each architecture's own hardware — no buildx, no
QEMU. `scripts/publish-image.sh` (also `make docker-publish`, `make
docker-publish ARGS=--dev`) builds for the host CPU, pushes
`:<version>-<arch>` and `:latest-<arch>` (`:latest-dev-<arch>` with `--dev`),
then rebuilds the `:<version>`, `:latest` or `:latest-dev` manifest list from
whichever per-arch tags exist on Docker Hub, so an amd64 publish never
overwrites the arm64 variant. Run it once per architecture.

```bash
docker login -u alfetahe
scripts/publish-image.sh            # prod: :<version> + :latest
scripts/publish-image.sh --dev      # :latest-dev
NO_CACHE=1 scripts/publish-image.sh # clean rebuild after a base-image bump
REPUBLISH=1 scripts/publish-image.sh # overwrite an already-published version tag
```

A prod publish refuses to overwrite a version tag that already exists —
"changed the controller, forgot to bump `mix.exs`" fails in seconds. On every
controller release: bump `version:` in `mix.exs`, publish per architecture,
then bump `image:` and the `app.kubernetes.io/version` labels in
`kubernetes/flezha-daemonset.yaml`.

### CI size gate

`.github/workflows/docker.yml` runs on pull requests that touch
`docker/prod/**`, `docker/dev/Dockerfile.runtime`, `test/apps/testnet/**`,
`mix.exs`, `mix.lock`, `Cargo.toml`, `Cargo.lock`, `flz_nif/**`,
`.dockerignore` or the workflow itself. It builds `:latest` and `:latest-dev`,
prints both sizes to the step summary and fails if the prod image exceeds
300 MiB (`PROD_IMAGE_SIZE_LIMIT_BYTES=314572800`) or the runtime-dev image
exceeds 450 MiB (`RUNTIME_DEV_IMAGE_SIZE_LIMIT_BYTES=471859200`). It never
pushes.

### Export to a file

```bash
docker save -o flezha-0.2.0.tar alfetahe/flezha:0.2.0
```
