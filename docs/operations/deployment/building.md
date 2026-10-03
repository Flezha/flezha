# Building

Every target below runs as `make <target>` from the project root. `build` is the
first target in the Makefile, so a bare `make` does the same as `make build`. How to
build and run a controller is on [Deploying a controller](../../topics/basics/deploying_a_controller.md).

## Targets

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
