# Flezha Controller - Production Docker Build

Run the following commands from the **flezha project root** directory.

## Published tags

| Tag                                | Built from                          | Notes |
|------------------------------------|-------------------------------------|-------|
| `alfetahe/flezha:<version>`        | `docker/prod/Dockerfile.controller` | Prod runtime, immutable. Version comes from `mix.exs`. **This is what clusters run.** |
| `alfetahe/flezha:latest`           | `docker/prod/Dockerfile.controller` | Same build, mutable convenience tag. Never pin a cluster to it. |
| `alfetahe/flezha:latest-dev`       | `docker/dev/Dockerfile.runtime`     | `FROM :latest` + Node.js + pre-built testnet WS server. |
| `alfetahe/flezha:latest-toolchain` | `docker/dev/Dockerfile.toolchain`   | Dev toolchain (Elixir + Rust + Node + protoc, source-mounted). |

**On every controller release**: bump `version:` in `mix.exs`, run
`scripts/publish-image.sh` on each architecture, then bump the pinned tag in
`kubernetes/flezha-daemonset.yaml` (`image:` and the
`app.kubernetes.io/version` labels) to match. The reference manifests must never
point at `:latest` — with `imagePullPolicy: IfNotPresent`, a mutable tag leaves nodes
running whatever they happened to cache.

## Image layout

`docker/prod/Dockerfile.controller` is a three-stage build:

- **Stage 1** — `rust:1.87.0-slim-bookworm`: builds `flz_nif.so` and the
  `flz` CLI binary with `--release`. `protoc` lives here because
  `prost-build` needs it.
- **Stage 2** — `hexpm/elixir:1.17.3-erlang-27.2-debian-bookworm-...-slim`:
  assembles the Mix release with `include_erts: true`, consuming the
  Stage 1 artefacts via `RUSTLER_SKIP_COMPILATION=true`.
- **Stage 3** — `debian:bookworm-slim`: runtime. Apt-installs `libssl3`,
  `libstdc++6`, `libtinfo6`, `locales`, `ca-certificates`, `tini`. The
  release bundles its own ERTS — there is no system Elixir or Erlang in
  the runtime image, and `:wx` / `:observer` are not packaged.

## Default runtime contract

The image's CMD is `/usr/local/bin/controller-entrypoint.sh` (under tini). The
script starts `bin/flzvm start`, optionally runs `flz cluster restore` if
a snapshot file is present, traps SIGTERM/SIGINT to write a snapshot via
`/usr/local/bin/snapshot-on-stop.sh`, then `wait`s. All behaviour is
configured via env vars — see `docs/deployment.md` for the contract. To
run the controller without the snapshot lifecycle, set
`FLEZHA_SNAPSHOT_ENABLED=false`.

A HEALTHCHECK is declared on the control socket
(`${FLEZHA_SOCKET_FILES_PATH}/executor_control_channel.sock`).

Attach a shell against a running container via the release's dist-node
entry points:

```bash
docker exec <container>     bin/flzvm rpc 'IO.puts("ok")'
docker exec -it <container> bin/flzvm remote
```

## Image size budget

`.github/workflows/docker.yml` runs `docker-size-gate` on PRs that touch
`docker/prod/**`, `mix.exs`, `mix.lock`, `Cargo.toml`, `Cargo.lock`, or
`flz_nif/**`. The job builds the prod image, prints its size to the
GitHub step summary, and fails if the image exceeds the budget.

Budget: **300 MiB** (`IMAGE_SIZE_LIMIT_BYTES=314572800`). Adjust with a
one-line edit if a legitimate size change ships in the same PR.


## process_hub source

The image fetches `process_hub` from its GitHub `dev` branch at build time
(`mix.exs::process_hub_dep/0` default). Nothing is baked in from a local
checkout — a plain git clone is all the build context needs.

## Publishing to Docker Hub

`:latest` (prod) and `:latest-dev` (dev) are multi-arch manifest lists, built
**natively on each architecture's own hardware** (no QEMU, no buildx
`--platform`, per the project policy). Use `scripts/publish-image.sh`:

```bash
docker login -u alfetahe

# Run on each architecture host (e.g. an arm64 Pi, and amd64 moon):
scripts/publish-image.sh           # prod  → pushes :<version>-<arch> + :latest-<arch>, rebuilds both manifests
scripts/publish-image.sh --dev     # dev   → pushes :latest-dev-<arch>, rebuilds :latest-dev
```

Each run pushes the **host arch** tag (`:latest-amd64` / `:latest-arm64`), then
rebuilds the `:latest` manifest from whichever per-arch tags currently exist on
Docker Hub. So building on amd64 updates the amd64 variant and building on arm64
updates the arm64 variant — **they never overwrite each other**. Run the script
on one host per architecture you want represented in the manifest.

Builds reuse the BuildKit layer cache and the Dockerfile's `--mount=type=cache`
mounts (fast incremental Rust/Elixir) and `--pull` to refresh base images. Pass
`NO_CACHE=1 scripts/publish-image.sh` to force a clean rebuild when bumping the
base image or toolchain.

## Run Docker Image

```bash
docker run -d \
  --name flezha_container \
  -e FLEZHA_MAX_RECONNECTS=3 \
  alfetahe/flezha:latest
```

## Export Docker Image

```bash
# Export latest
docker save -o flezha-latest.tar alfetahe/flezha:latest

# Export specific version
docker save -o flezha-0.2.0.tar alfetahe/flezha:0.2.0
```

## Docker Compose Deployment

```bash
# Start production containers
docker compose -f docker/prod/docker-compose.yml up -d
```

