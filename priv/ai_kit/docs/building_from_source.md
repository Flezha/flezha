# Building from source

The [release archives](deploying_a_controller.md) are built for Linux x86_64
and for macOS on Apple Silicon and Intel. Build a release yourself on any other
platform, or to run a controller with changes of your own.

## What you need

- Elixir 1.17 or later, with its Erlang/OTP.
- A stable Rust toolchain.
- A C compiler, cmake (the Kafka plugin's checksum library builds with it) and
  protoc (the controller's protobuf modules are generated with it at compile
  time).
- A checkout of the repository with the Elixir and Rust SDK submodules, which
  the build compiles:

```sh
$ git clone https://github.com/flezha/flezha.git
$ cd flezha
$ git submodule update --init sdk/elixir sdk/rust
```

## Build a release

```sh
$ make build-release
```

That writes `_build/prod/rel/flezha/`, the same directory the release archive
unpacks to: `bin/flz`, the CLI, and `bin/flzvm`, the release script that
starts the controller. The release carries its own Erlang runtime, so the machine it runs on
needs no Elixir and no Erlang, but it runs only on the platform it was built
on. From here, [Deploying a controller](deploying_a_controller.md)
applies as it does to a downloaded release.

> #### State inside a build directory {: .warning}
>
> Running from `_build/prod/rel/flezha` puts `storage/app/` inside build output.
> `make clean` deletes it, and with it the controller's durable state and its
> cookie. For anything you want to keep, point `FLEZHA_STATE_DIR` at an
> absolute path outside the build.

To pack a release of your own as an archive, the way a GitHub release does:

```sh
$ make release-archive
```

It writes `_build/.dist/controller/flezha-<version>-<platform>.tar.gz` and
leaves out the release's `tmp/` and `storage/`, which hold the logs and the
node cookie of every local run. `scripts/release-archive.sh smoke <archive>
<runner>` runs the check a release runs on it: a controller starts from the
unpacked archive, a runner connects, and the controller stops.

## On macOS

The release archives for macOS are built and started on Macs before each
release, so on a Mac they are the simpler route. A build from source on a Mac
of your own has not been tested yet. Install the C compiler with
`xcode-select --install`, the other tools with Homebrew, and Rust with
[rustup](https://rustup.rs):

```sh
$ brew install elixir cmake protobuf
```

Then build as above.

## Make targets

Every target below runs as `make <target>` from the project root. `build` is
the first target in the Makefile, so a bare `make` does the same as
`make build`.

| Target | Does |
|---|---|
| `build` | `cargo build --workspace`, `mix deps.get`, `mix compile` |
| `build-release` | `cargo build --release -p flz_nif -p flz`, then `MIX_ENV=prod mix deps.get --only prod && mix release --overwrite`, into `$(REL_DIR)/flezha` (`REL_DIR` defaults to `_build/prod/rel`) |
| `release-archive` | `build-release`, then the release packed as this platform's archive in `_build/.dist/controller/`, as a GitHub release publishes it |
| `nif` / `nif-release` | Build only the `flz_nif` crate (debug / release) |
| `test` | `mix test` |
| `fmt` | `scripts/format-apply.sh`, the formatter for every language in the tree |
| `refresh-process-hub` | `mix deps.unlock process_hub && mix deps.get` — re-resolve `process_hub` within the requirement `mix.exs` declares, taking a newer matching release |
| `clean` | `cargo clean`, `mix clean`, remove `_build`, `deps`, `priv/native/*.so` |
| `docker-build` | Prod image for the host architecture; requires `VERSION=x.y.z` |
| `docker-publish` | `scripts/publish-image.sh $(ARGS)` — native per-arch build, push, manifest rebuild |
| `docker-build-dev` | `flezha/flezha:latest-dev` from `deploy/docker/dev/Dockerfile.runtime` (needs `:latest` in the local cache) |
| `docker-build-toolchain` | `flezha/flezha:latest-toolchain` from `deploy/docker/dev/Dockerfile.toolchain` |
| `k8s-apply` | Apply the six manifests in dependency order |
| `k8s-delete` | Delete the DaemonSet and ConfigMap (`--ignore-not-found`) |
