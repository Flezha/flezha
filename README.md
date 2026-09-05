# Flezha

Distributed stateful operatives: a compute graph of procedures, each with its
own durable per-Entity state, placed and migrated across a cluster by a
controller, with static and dynamic creation of operatives.

## Try it without any of that

You do not need a controller, a container or a socket to see one run. **Local
mode** takes the operative class you would deploy, runs its declared graph in
your own process, and hands you the output:

```rust
let run = LocalOperative::start::<WordCount>()?;
run.on_sink("totals", |out| println!("{}", String::from_utf8_lossy(&out.payload)))?;
run.feed("lines", "the quick brown fox")?;
run.run_until_idle(Duration::from_secs(5))?;
```

```bash
cargo run -p flezha_runner --example local_word_count
```

**Local mode holds state in memory and nothing is durable** — no write-ahead
log, no snapshots, no dead-letter queue, and nothing survives the process. It
runs the deployed machinery (entry decode, edge transforms and keying, one
Entity per key with its own mailbox, the retry ladder, inter-procedure
delivery); durability, plugins, placement and migration, the dead-letter queue,
observability export, authz and multi-runner scale need a controller. It ships
in the Rust, Java and Elixir SDKs; Node, Python, Ruby and PHP do not carry it
yet.

**[`docs/quickstart.md`](docs/quickstart.md)** walks through it in five
minutes.

## Reading further

[`docs/architecture/`](docs/architecture/README.md) — the durable-compute
kernel, the two operative profiles, and the diagrams. Vocabulary:
[`docs/glossary.md`](docs/glossary.md). Every knob, by layer:
[`docs/configuration.md`](docs/configuration.md).

## Features
- Run a whole graph in process, with no controller ([`docs/quickstart.md`](docs/quickstart.md))
- Clustered, hot-swap-migratable operatives
- SDK runtimes for Rust, Java, and others over a UDS or QUIC transport
- Per-Entity durable state — WAL + snapshot, crash-resume, point-in-time query ([`docs/internals/state_persistence.md`](docs/internals/state_persistence.md))
- Opt-in effectively-once delivery — durable input log, atomic effect record, fsync-gated outbox ([`docs/architecture/kernel.md`](docs/architecture/kernel.md))
- Workflow operatives — a procedure tree with saga compensation, a durable loop, and suspension ([`docs/architecture/workflow_profile.md`](docs/architecture/workflow_profile.md))
- Installable plugin subsystem ([`docs/plugins/installation.md`](docs/plugins/installation.md))
- First-party Kafka producer with retries, idempotence, batching, and bounded queueing ([`docs/plugins/kafka_producer.md`](docs/plugins/kafka_producer.md))
- Transactional Postgres sink — upsert + progress watermark in one transaction ([`docs/resilience/data_durability.md`](docs/resilience/data_durability.md))

## Image size and supported architectures

The prod controller image runs on `debian:bookworm-slim` with a bundled-ERTS
Mix release (see [`docker/prod/README.md`](docker/prod/README.md)) under
a **300 MiB** budget enforced by the `docker-size-gate` job in
[`.github/workflows/docker.yml`](.github/workflows/docker.yml). Supported
platforms: `linux/amd64` and `linux/arm64`; the manifest list is assembled by
`make docker-publish`, run once on a host of each architecture.
The `protoc` install layer rejects any other `uname -m` with
`Unsupported architecture: <arch>`.

## Building
- Copy `.env.example` and rename to `.env`
- Install testnet dependencies: `cd test/apps/testnet && npm ci`
- Build the testnet: `npm run build` (from `test/apps/testnet`)
- Build the `rust_runner` application.
- Build the controller `mix compile`

### Distribution artifacts (built, never committed)

Three classes of build output — none of it tracked in git:

- 5× native binaries — `sdk/java/flezha-sdk-native/src/main/resources/native/<platform>/libflz_core_ffi.{so,dylib,dll}`
- SDK fat jar — `sdk/java/flezha-sdk/dist/flezha-sdk.jar`
- Test runner fat jar — `test/apps/java_runner/dist/java_runner.jar`

**To develop and test you need one native binary: the host triple.** Build it
once after cloning, and again after touching `flz_core_ffi/`, `flz_core/` or
`flz_transport/`:

```bash
scripts/build-native.sh --output sdk/java/flezha-sdk-native/src/main/resources/native
```

`mvn verify`, `FLEZHA_E2E_RUNTIME=java mix test` and the conformance suite all
read that one file, offline. The Maven `native-verify` profile asserts it is
there and names this command when it is not.

**All five triples are a release concern.** A version tag runs the
`release-artifacts` job in `.github/workflows/java-sdk.yml`, which cross-builds
the five, assembles both fat jars, and publishes them with a SHA-256 manifest as
GitHub Release assets. To assemble a portable jar yourself:

```bash
scripts/build-dist.sh             # host toolchain (auto-installs zig if missing)
scripts/build-dist.sh --docker    # pinned container; needs only docker
mvn -P native-build -f sdk/java/pom.xml package   # or: fetch the published five
```

`native-build` downloads any triple missing from your tree from the release
matching the pinned version, verifies it against the manifest, and leaves
anything you cross-built alone. It is the only path that touches the network,
and no test uses it.

See [`docker/build/README.md`](docker/build/README.md) for the build
image; [`docker/dev/README.md`](docker/dev/README.md) for the runtime
dev container.

### Running CI locally before pushing

`scripts/ci-local.sh` runs the **verbatim** command sequence from the
GitHub Actions workflows, in the same order. A green local run = a
green CI run; a red local run reproduces the failure on your machine
in seconds instead of waiting 10+ minutes for GitHub.

```bash
scripts/ci-local.sh                # both sections (default)
scripts/ci-local.sh elixir         # only the Elixir CI job
scripts/ci-local.sh java           # only the Java SDK CI jobs
scripts/ci-local.sh elixir java    # any combination, any order
```

The `elixir` section mirrors `.github/workflows/elixir.yml`; the `java` section
mirrors both jobs of `.github/workflows/java-sdk.yml`. What each one runs, in
order, is listed in the script's own header — one list, one owner, so the two
cannot drift.

The `java` section builds the host triple and both jars first, exactly as the
workflow does — nothing it needs is tracked in git. The script uses an isolated
`CARGO_TARGET_DIR=target-ci-local/` so it can't be blocked by stale local cargo
state.

Exit code is non-zero on the first failing section; otherwise the script
ends with `ALL CI CHECKS PASSED`.

## Graceful shutdown

The controller runs a three-phase orderly shutdown (quiesce → drain →
finalise) on SIGTERM, `Application.prep_stop/1` and `flz controller stop`, in
release, `mix run --no-halt` and `iex -S mix` modes. Triggers per run mode, the
two env knobs and their defaults, the orchestrator grace-period pairing rule,
telemetry, exit-code semantics and the third-SIGTERM escape hatch:
[`docs/operations/graceful_shutdown.md`](docs/operations/graceful_shutdown.md).

## Wiping runtime data

`flz reset` clears the Controller's durable on-disk state so the next boot
starts from a clean slate, and refuses to run while the Controller is up. Which
directories are in scope (the socket directory is resolved, not fixed), the
flags, the exit codes and the operator recipes for docker-compose, k8s and CI:
[`docs/operations/reset.md`](docs/operations/reset.md).

## Code style

The repo enforces one formatter per language, plus the Python linter, via the
[`format`](.github/workflows/format.yml) workflow on every PR — drift
blocks merge. The workflow runs the same script you do, so the two cannot
cover different languages:

```bash
scripts/format-check.sh   # every formatter in check mode, plus the licensing checks
scripts/format-apply.sh   # auto-fix the formatting the check reports
```

Both report every language they can and name any formatter that is not
installed, so a missing toolchain removes one language from the report and
never the rest.

`ruff check` is the one linter in that list, and it never rewrites source —
not even in the apply pass, which has to stay reproducible by re-running the
formatters alone. So the apply script can report a Python finding it will not
fix for you; [`ruff.toml`](ruff.toml) is where each rule is settled, and the
rules refused there carry the reason they were refused. Its enabled set is the
default of the version pinned in
[`sdk/python/pyproject.toml`](sdk/python/pyproject.toml), so raising that pin
settles whatever the new version reports.

Style sources of truth: [`rustfmt.toml`](rustfmt.toml) for Rust (nightly
rustfmt — pinned date in `format.yml` and `scripts/format-apply.sh`),
[`.formatter.exs`](.formatter.exs) for Elixir, the Spotless block in
[`sdk/java/pom.xml`](sdk/java/pom.xml) (palantir-java-format) for Java, and
each veneer SDK's own pinned development dependency for Node (prettier),
Python (ruff), Ruby (rubocop) and PHP (php-cs-fixer). The format-pass
commits are listed in
[`.git-blame-ignore-revs`](.git-blame-ignore-revs) so `git blame` skips
them.

## Examples

### Start controller (started automatically for dev mode)
Starting the node and specifying the external processes to be used by the node mynode.
PROD:
```bash
RELEASE_NODE=mynode FLEZHA_EXTERNAL_PROCESSES="cargo run --manifest-path=test/apps/rust_runner/Cargo.toml --target-dir=./target" _build/prod/rel/flezha/bin/flz controller start --mode=attached_iex
```

DEV:
```bash
RELEASE_NODE=mynode _build/dev/rel/flezha/bin/flz controller start --mode=attached_iex
```

### Stop controller
Stopping the node.
```bash
_build/dev/rel/flezha/bin/flz controller stop
```

### Starting an operative.
```bash
_build/dev/rel/flezha/bin/flz operative start my_operative compare_filter --init_values='{
  "comparable": "hello world", 
  "inverted": false
}'
```

```bash
_build/dev/rel/flezha/bin/flz operative start my_operative do_nothing
```

### Stop an operative.
```bash
_build/dev/rel/flezha/bin/flz operative stop my_operative
```

### Installable plugins
```bash
# Full lifecycle reference (install / upgrade / uninstall / enable / disable)
# lives in docs/plugins/installation.md — including package layout, manifest
# schema, checksum generation, and Ed25519 signing.
_build/dev/rel/flezha/bin/flz plugin install /path/to/my_plugin-1.0.0.fpz
_build/dev/rel/flezha/bin/flz plugin enable my_plugin
```

See [`docs/plugins/installation.md`](docs/plugins/installation.md) for the
full authoring guide and [`examples/plugins/hello_world/`](examples/plugins/hello_world/)
for a minimal copy-paste starting point.

### Enabling websocket plugin for operative.

`flz plugin enable <plugin_id> <provider_id> …`. Provider options carry the
`--X` prefix and reach `init_provider/1`; attachment (`--operative_id`) and
subscription (`--trigger`) are plain options the CLI consumes first — a
provider with a strict allow-list refuses every `--X` key it does not own.

```bash
_build/dev/rel/flezha/bin/flz plugin enable my_ws operative_websocket \
  --operative_id=my_operative \
  --trigger=proactive \
  --Xuri="ws://localhost:4500" \
  --Xpost_connect='{"payload":{"type":"text","message":"{\"event_type\":\"subscribe\",\"event_data\":{\"default_message\":\"hello world\",\"interval\":1000}}"}}'
```

```bash
_build/dev/rel/flezha/bin/flz plugin enable my_stdout operative_stdout \
  --operative_id=my_operative \
  --trigger=on_output
```

### Get operative state.
```bash
_build/dev/rel/flezha/bin/flz operative state my_operative
```


### Performance Testing

Use `./scripts/bench.sh` to measure throughput. Switching axes are independent:
runtime (`rust`/`java`), transport (`uds`/`quic`), shape (`regression`/`entity`).

```bash
./scripts/bench.sh                                # rust + uds, defaults
./scripts/bench.sh -r java -t quic -n 100 -m 1000 # any combination
./scripts/bench.sh --shape entity --pool-sizes 1,8
./scripts/bench.sh --help
```

Full reference (flags, shapes, CI gates, floors, triage):
[`docs/operations/benchmarking.md`](docs/operations/benchmarking.md).

### Restart survival

Flezha auto-restores after any restart — single-controller crash, rolling
deploy, or full cluster cold boot. Two persistence layers back it: the
ProcessHub durable registry, which holds every operative's child-spec including
`opts[:plugins]`, and the per-Entity WAL/snapshot, which holds change-by-change
progress and is shipped back to the SDK when the Entity's stream reattaches.
What is written where, the boot reconcile, the `flz boot status` and
`flz boot await` surface, the `FLEZHA_AUTO_RESTORE_ENABLED` kill-switch and the
safety matrix per scenario — including the one tolerated data-loss case:
[`docs/operations/auto_restore_on_boot.md`](docs/operations/auto_restore_on_boot.md).

### Development
Run the application inside the docker containers.
See `docker/dev/README.md` for more information.

### Running the SDK test suites

One end-to-end suite serves every SDK; `FLEZHA_E2E_RUNTIME` picks which one a
run exercises, and nothing under `test/system/` names a runtime.

```bash
mix test                                             # unit + the conformance gate, no runner
FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke # the smoke tier on the Java SDK
FLEZHA_E2E_RUNTIME=java mix test --include system    # the whole suite on the Java SDK
mvn -f sdk/java/pom.xml -P coverage,native-verify verify   # the Java SDK's own unit tests
```

A capability an SDK does not carry is declared in its manifest
(`test/apps/<sdk>_runner/conformance.json`) and skipped by name; everything else
must pass. `docs/sdk/conformance.md` has the whole contract, including what a
new SDK owes.

The jars and native binary the Java runs consume are build output — produce them
with `scripts/build-native.sh` plus `mvn package` (Java 21 and Rust stable; see
"Distribution artifacts" above). Only a portable, five-triple jar needs
`cargo-zigbuild` and a Zig toolchain.

On hardened containers where `/tmp` is mounted `noexec`, set
`FLEZHA_NATIVE_LIB_DIR=/path/to/writable` so `NativeLibraryLoader` can
extract `libflz_core_ffi` somewhere it can be loaded. See
`sdk/java/README.md` for the full env-var reference.

## License, trademark and warranty

| Path | License |
|---|---|
| Everything not listed below — the controller (`lib/`), `cli/`, `flz_nif/`, `config/`, `rel/`, `docker/`, `kubernetes/`, `test/`, `docs/`, `scripts/`, `bench/` | [MPL-2.0](LICENSE) |
| The SDK side — `sdk/`, `flz_core/`, `flz_core_ffi/`, `flz_transport/`, `flezha-macros/`, `lib/flezha/sdk/`, `protobufs/` | [Apache-2.0](LICENSES/Apache-2.0.txt) |

The per-path map is machine-readable in [`REUSE.toml`](REUSE.toml) and checked
by CI (`reuse lint`). In short: build on the SDKs freely; if you modify and
distribute the controller's files, publish those modifications. Your own files
never inherit either license.

**Status and warranty.** Flezha is pre-1.0 software provided "AS IS", without
warranty of any kind; liability is disclaimed to the extent the licenses allow
(MPL-2.0 §6–7, Apache-2.0 §7–8). It is not certified for any regulated use.
Every durability and delivery default is documented in
[`docs/configuration.md`](docs/configuration.md) and
[`docs/resilience/data_durability.md`](docs/resilience/data_durability.md) —
read them before trusting the system with data.

**Name.** "Flezha" names this project and its releases. The licenses grant no
trademark rights (MPL-2.0 §2.3, Apache-2.0 §6): a fork, derived work or hosted
service must not use the name or a confusingly similar one.

Third-party components bundled in binary distributions are listed in
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md). Contributions:
[`CONTRIBUTING.md`](CONTRIBUTING.md) (CLA required). Security reports:
[`SECURITY.md`](SECURITY.md).
