# Flezha Controller - Production Docker Build

Run the following commands from the **flezha project root** directory.

## Published tags

| Tag                                | Built from                          | Notes |
|------------------------------------|-------------------------------------|-------|
| `flezha/flezha:<version>`        | `docker/prod/Dockerfile.controller` | Prod runtime, immutable. Version comes from `mix.exs`. **This is what clusters run.** |
| `flezha/flezha:latest`           | `docker/prod/Dockerfile.controller` | Same build, mutable convenience tag. Never pin a cluster to it. |
| `flezha/flezha:latest-dev`       | `docker/dev/Dockerfile.runtime`     | `FROM :latest` + Node.js + pre-built testnet WS server. |
| `flezha/flezha:latest-toolchain` | `docker/dev/Dockerfile.toolchain`   | Dev toolchain (Elixir + Rust + Node + protoc, source-mounted). |
| `flezha/plugin-builder:<version>` | `docker/prod/Dockerfile.controller`, `--target plugin-builder` | The release's toolchain, for building plugin packages this image loads. Built locally, not published before go-live. |

**On every controller release**: bump `version:` in `mix.exs`, run
`scripts/publish-image.sh` on each architecture, then bump the pinned tag in
`kubernetes/flezha-daemonset.yaml` (`image:` and the
`app.kubernetes.io/version` labels) to match. The reference manifests must never
point at `:latest` — with `imagePullPolicy: IfNotPresent`, a mutable tag leaves nodes
running whatever they happened to cache.

## Image layout

`docker/prod/Dockerfile.controller` is a three-stage build, plus a
`plugin-builder` target the runtime image never includes. The Elixir/OTP image
tag is written once, in the `ELIXIR_IMAGE` argument, and read by both the
release stage and the builder:

- **Stage 1** — `rust:1.87.0-slim-bookworm`: builds `flz_nif.so` and the
  `flz` CLI binary with `--release`. It needs no `protoc`: the prost output
  is committed.
- **Stage 2**, `${ELIXIR_IMAGE}`: assembles the Mix release with
  `include_erts: true`, consuming the Stage 1 artefacts via
  `RUSTLER_SKIP_COMPILATION=true`.
- **`plugin-builder`**: `${ELIXIR_IMAGE}` with Hex, rebar and the tools
  compiling `flezha` as a dependency needs, `MIX_ENV=prod`, workdir `/src`.
  A package built here carries the image's OTP and Elixir versions, which is
  what the controller checks before loading it.
- **Stage 3** — `debian:bookworm-slim`: runtime. Apt-installs `libssl3`,
  `libstdc++6`, `libtinfo6`, `locales`, `ca-certificates`, `tini`. The
  release bundles its own ERTS — there is no system Elixir or Erlang in
  the runtime image, and `:wx` / `:observer` are not packaged.

## Running the image

What the container does when it starts and stops, what to mount, Compose, `flz` in
the container, and VM flags:
[`docs/public/topics/deploying/docker.md`](../../docs/public/topics/deploying/docker.md).

## Image size budget

`.github/workflows/docker.yml` runs `docker-size-gate` on PRs that touch
`docker/prod/**`, `mix.exs`, `mix.lock`, `Cargo.toml`, `Cargo.lock`, or
`flz_nif/**`. The job builds the prod and runtime-dev images, then runs the
gate row `image-size` (`python3 scripts/naming_gate.py --run @docker`), which
prints their sizes to the GitHub step summary and fails if either exceeds its
budget.

Budget: **300 MiB** for the prod image, **450 MiB** for runtime-dev, both in
`IMAGE_SIZE` in `scripts/gate_rows.py`. Adjust with a one-line edit if a
legitimate size change ships in the same PR.


## process_hub source

`process_hub` comes from its `dev` branch on GitHub at the commit `mix.lock`
pins, not from hex: 0.7.0 is the newest published version and it predates two
fixes this controller needs. The build resolves it like any other dependency, so
a plain git clone plus network access is all the build context needs, and every
build gets the same commit. It goes back to a hex requirement once 0.7.1 is
published.

## Publishing to Docker Hub

`:latest` (prod) and `:latest-dev` (dev) are multi-arch manifest lists, built
**natively on each architecture's own hardware** (no QEMU, no buildx
`--platform`, per the project policy). Use `scripts/publish-image.sh`:

```bash
docker login -u flezha

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

## Export Docker Image

```bash
# Export latest
docker save -o flezha-latest.tar flezha/flezha:latest

# Export specific version
docker save -o flezha-0.1.0.tar flezha/flezha:0.1.0
```

## Plugins

A plugin package (`.fpz`) is data beside the immutable image: the controller
loads every archive in `/flezha/app_data/plugins` when it starts, and a change
takes effect at the next start. Build a package for this image in the builder:

```bash
docker build --target plugin-builder -t flezha/plugin-builder:0.1.0 \
  -f docker/prod/Dockerfile.controller .
docker run --rm -v "$PWD/my_plugin:/src" flezha/plugin-builder:0.1.0 \
  sh -c 'mix deps.get && mix flezha.plugin.package'
```

Three ways to give a container its packages:

- **Install into the volume**: `docker exec <c> /flezha/bin/flz plugin install
  <path-or-https-url>`, then `docker restart <c>`. The compose file's
  `flezha_plugins` volume keeps the archives.
- **Declare the list**: set `FLEZHA_PLUGINS` (paths or `https://` URLs, each
  optionally `#sha256=<hex>`). The entrypoint runs `flz plugin sync` before it
  starts the controller and exits non-zero, starting nothing, when the sync
  fails. Install and uninstall are refused while the list is set.
- **Bake a derived image**: `FROM flezha/flezha:<version>` and `COPY *.fpz
  /flezha/app_data/plugins/`, run with no volume over that directory.

[`docs/public/topics/plugins/installing_plugins.md`](../../docs/public/topics/plugins/installing_plugins.md) covers statuses, errors and signing.
