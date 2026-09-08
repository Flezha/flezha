# Building

Every target below runs as `make <target>` from the project root. `build` is the
first target in the Makefile, so a bare `make` does the same as `make build`.

## Examples

Build after cloning, and after most edits:

```bash
make build
```

The Rust workspace first (the NIF, the `flz` CLI, the SDK and the test
runners), then `mix deps.get` and `mix compile`. Debug profile.

Build a production release:

```bash
make build-release
```

Output is `_build/prod/rel/flezha/`, holding two binaries: `bin/flzvm` is the
controller and `bin/flz` is the CLI, copied in from `target/release/flz`. The
release carries its own ERTS, so it runs on a machine with no Elixir installed,
and it is the same artifact the production image runs. Cargo has to run first
because the release step copies the CLI out of `target/release/`, which is why
this is one target rather than two commands.

Rebuild only the NIF, when nothing but Rust changed:

```bash
make nif
```

Run the suite:

```bash
make test                            # mix test
SKIP_JAVA=1 make stabilize-test-check # the fuller gate, without the Maven step
```

Build the production image. The variable goes **before** `make`; with no
`VERSION` the tag expands to `alfetahe/flezha:` and docker refuses it:

```bash
VERSION=0.2.0 make docker-build
```

Start over. This deletes `_build` and `deps` and runs `cargo clean`, so the next
build is a full one and will not be quick:

```bash
make clean
```

## Makefile reference

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
| `docker-build-dev` | `alfetahe/flezha:latest-dev` from `docker/dev/Dockerfile.runtime` (needs `:latest` in the local cache) |
| `docker-build-toolchain` | `alfetahe/flezha:latest-toolchain` from `docker/dev/Dockerfile.toolchain` |
| `k8s-apply` | Apply the six manifests in dependency order |
| `k8s-delete` | Delete the DaemonSet and ConfigMap (`--ignore-not-found`) |
