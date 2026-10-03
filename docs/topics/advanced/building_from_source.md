# Building from source

The [release archive](../basics/deploying_a_controller.md) is built for Linux
x86_64. Build a release yourself on any other platform, or to run a controller
with changes of your own.

## What you need

- Elixir 1.17 or later, with its Erlang/OTP.
- A stable Rust toolchain; `rust-toolchain.toml` selects it.
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
unpacks to: `bin/flz`, the CLI, and `bin/flzvm`, the release script the CLI
calls. The release carries its own Erlang runtime, so the machine it runs on
needs no Elixir and no Erlang, but it runs only on the platform it was built
on. From here, [Deploying a controller](../basics/deploying_a_controller.md)
applies as it does to a downloaded release.

> #### State inside a build directory {: .warning}
>
> Running from `_build/prod/rel/flezha` puts `app_data/` inside build output.
> `make clean` deletes it, and with it the controller's durable state and its
> cookie. For anything you want to keep, point `FLEZHA_STATE_DIR` at an
> absolute path outside the build.

To pack a release of your own as an archive:

```sh
$ tar -czf <name>.tar.gz --exclude='flezha/tmp' --exclude='flezha/app_data' -C _build/prod/rel flezha
```

A plain `tar` of the directory carries the logs and the node cookie of every
local run.

## Make targets

Every target below runs as `make <target>` from the project root. `build` is
the first target in the Makefile, so a bare `make` does the same as
`make build`.

| Target | Does |
|---|---|
| `build` | `cargo build --workspace`, `mix deps.get`, `mix compile` |
| `build-release` | `cargo build --release -p flz_nif -p flz`, then `MIX_ENV=prod mix deps.get --only prod && mix release --overwrite` |
| `nif` / `nif-release` | Build only the `flz_nif` crate (debug / release) |
| `test` | `mix test` |
| `fmt` | `scripts/format-apply.sh`, the formatter for every language in the tree |
| `stabilize-test-check` | `mix test` twice, `cargo test --workspace --features flezha_runner/bench-internals`, `mvn -f sdk/java verify` (`SKIP_JAVA=1` skips the last) |
| `refresh-process-hub` | `mix deps.unlock process_hub && mix deps.get` — re-resolve `process_hub` within the requirement `mix.exs` declares, taking a newer matching release |
| `clean` | `cargo clean`, `mix clean`, remove `_build`, `deps`, `priv/native/*.so` |
| `docker-build` | Prod image for the host architecture; requires `VERSION=x.y.z` |
| `docker-publish` | `scripts/publish-image.sh $(ARGS)` — native per-arch build, push, manifest rebuild |
| `docker-build-dev` | `flezha/flezha:latest-dev` from `docker/dev/Dockerfile.runtime` (needs `:latest` in the local cache) |
| `docker-build-toolchain` | `flezha/flezha:latest-toolchain` from `docker/dev/Dockerfile.toolchain` |
| `k8s-apply` | Apply the six manifests in dependency order |
| `k8s-delete` | Delete the DaemonSet and ConfigMap (`--ignore-not-found`) |
