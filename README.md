# Flezha

## Description
Distributed stateful relays. Provides static and dynamic creation of relays.

## Features
- Clustered, hot-swap-migratable relays
- SDK runtimes for Rust, Java, and others over a UDS or QUIC transport
- Installable plugin subsystem ([`docs/plugin_installation.md`](docs/plugin_installation.md))
- First-party Kafka producer with retries, idempotence, batching, and bounded queueing ([`docs/kafka-producer.md`](docs/kafka-producer.md))
- Cluster-wide snapshot & restore via `flz cluster` ([`docs/cluster-snapshot.md`](docs/cluster-snapshot.md))

## Image size and supported architectures

The prod controller image runs on `debian:bookworm-slim` with a bundled-ERTS
Mix release (see [`docker/prod/README.md`](docker/prod/README.md)) under
a **300 MiB** budget enforced by the `docker-size-gate` job in
[`.github/workflows/docker.yml`](.github/workflows/docker.yml). Supported
platforms: `linux/amd64` and `linux/arm64` (`make docker-build-multiarch`).
The `protoc` install layer rejects any other `uname -m` with
`Unsupported architecture: <arch>`.

## Building
- Copy `.env.example` and rename to `.env`
- Install testnet dependencies: `cd test/apps/testnet && npm install`
- Build the testnet: `npm run build` (from `test/apps/testnet`)
- Build the `rust_executor` application.
- Build the controller `mix compile`

### Distribution artifacts (committed to git, never built by CI)

Three classes of artifacts ride along in the repo so CI never rebuilds
them:

- 5× native binaries — `sdk/java/flezha-sdk-native/src/main/resources/native/<platform>/libflz_transport.{so,dylib,dll}`
- SDK fat jar — `sdk/java/flezha-sdk/dist/flezha-sdk.jar`
- Test executor fat jar — `test/apps/java_executor/dist/java_executor.jar`

Refresh all three with one command after touching anything under
`sdk/java/`, `flz_jni/`, `flz_transport/`, or `test/apps/java_executor/`:

```bash
scripts/build-dist.sh             # host toolchain (auto-installs zig if missing)
scripts/build-dist.sh --docker    # pinned container; needs only docker
```

Then `git add` the updated `dist/` directories and commit. The
`scripts/install-hooks.sh` pre-commit hook will refuse a commit that
edits source under those paths without restaging the refreshed jars.

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

What each section runs (each line below is the literal command):

- **`elixir`** — mirrors `.github/workflows/elixir.yml`:
  - `mix deps.get`
  - `mix compile --warnings-as-errors`
  - `mix test` (excludes `:system` and `:java_sdk`)
- **`java`** — mirrors `.github/workflows/java-sdk.yml` (both jobs):
  - `mvn -B -f sdk/java/pom.xml -P coverage,native-verify verify` (Java unit tests + JaCoCo)
  - `mix test.java_sdk` (Elixir↔Java integration matrix against the committed test executor jar)

The `java` section refuses to run if any committed distribution artifact
is missing — it'll point you at `scripts/build-dist.sh`. The script uses
an isolated `CARGO_TARGET_DIR=target-ci-local/` so it can't be blocked
by stale local cargo state.

Exit code is non-zero on the first failing section; otherwise the script
ends with `ALL CI CHECKS PASSED`.

## Graceful shutdown

The controller runs a three-phase orderly shutdown (quiesce → drain →
finalise) on SIGTERM, SIGINT, `Application.prep_stop/1`, and
`flz controller stop` — works in release, `mix run --no-halt`, and
`iex -S mix` modes. Bounded by `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`
(default 60 s); pair the orchestrator's grace period
(`terminationGracePeriodSeconds` / `stop_grace_period`) at
`timeout_ms / 1000 + 15s`. Default-on; set
`FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false` to fall back to today's
direct-stop behaviour.

| Env var                                | Default | Description                           |
| -------------------------------------- | ------- | ------------------------------------- |
| `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED`     | `true`  | Kill-switch (truthy: 1/true/yes/on)   |
| `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`  | `60000` | Coordinator drain timeout             |

Full operator playbook (run modes, telemetry, exit-code semantics,
the third-SIGTERM escape hatch): [`docs/operations/graceful_shutdown.md`](docs/operations/graceful_shutdown.md).

## Code style

The repo enforces three formatters via the
[`format`](.github/workflows/format.yml) workflow on every PR — drift
blocks merge. Run them locally before pushing:

```bash
scripts/format-check.sh   # cargo fmt --check, mix format --check-formatted, mvn spotless:check
scripts/format-apply.sh   # auto-fix the same three
```

Style sources of truth: [`rustfmt.toml`](rustfmt.toml) for Rust (nightly
rustfmt — pinned date in `format.yml`), [`.formatter.exs`](.formatter.exs)
for Elixir, the Spotless block in [`sdk/java/pom.xml`](sdk/java/pom.xml)
(palantir-java-format) for Java. The format-pass commits are listed in
[`.git-blame-ignore-revs`](.git-blame-ignore-revs) so `git blame` skips
them.

## Examples

### Start controller (started automatically for dev mode)
Starting the node and specifying the external processes to be used by the node mynode.
PROD:
```bash
RELEASE_NODE=mynode FLEZHA_EXTERNAL_PROCESSES="cargo run --manifest-path=/home/anuar/Projects/personal/flezha/test/apps/rust_executor/Cargo.toml --target-dir=./target" _build/prod/rel/flezha/bin/flz controller start --mode=attached_iex
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

### Starting a relay.
```bash
_build/dev/rel/flezha/bin/flz relay start my_relay compare_filter --init_values='{
  "comparable": "hello world", 
  "inverted": false
}'
```

```bash
_build/dev/rel/flezha/bin/flzrelay start my_relay do_nothing
```

### Stop a relay.
```bash
_build/dev/rel/flezha/bin/flz relay stop my_relay
```

### Installable plugins
```bash
# Full lifecycle reference (install / upgrade / uninstall / enable / disable)
# lives in docs/plugin_installation.md — including package layout, manifest
# schema, checksum generation, and Ed25519 signing.
_build/dev/rel/flezha/bin/flz plugin install /path/to/my_plugin-1.0.0.fpz
_build/dev/rel/flezha/bin/flz plugin enable my_plugin
```

See [`docs/plugin_installation.md`](docs/plugin_installation.md) for the
full authoring guide and [`examples/plugins/hello_world/`](examples/plugins/hello_world/)
for a minimal copy-paste starting point.

### Enabling websocket plugin for relay. 
```bash
_build/dev/rel/flezha/bin/flz plugin enable relay_websocket  --settings='{
  "relay_id": "my_relay",
  "trigger": "proactive",
  "uri": "ws://localhost:4500",
  "post_connect": {
    "payload": {
      "type": "text",
      "message": "{\\"event_type\\": \\"subscribe\\",\\"event_data\\": {\\"default_message\\": \\"hello world\\",\\"interval\\": 1000}}"
    }
  }
}'
```

```bash
_build/dev/rel/flezha/bin/flz plugin enable relay_stdout  --settings='{
  "relay_id": "my_relay",
  "trigger": "on_out",
}'
```

### Get relay state.
```bash
_build/dev/rel/flezha/bin/flz relay state my_relay
```


### Performance Testing
Run performance tests to measure relay throughput and message processing capabilities:

```bash
# Run with default settings (100 relays, 100 messages per relay)
MIX_ENV=prod mix bench

# Run with custom settings
MIX_ENV=prod mix bench --num-relays 100 --messages-per-relay 100

# Short form aliases
MIX_ENV=prod mix bench -n 100 -m 50

# Bench the Rust SDK (default) vs the Java SDK — same controller, same protocol,
# same measurement code, so the Throughput numbers are directly comparable.
MIX_ENV=prod mix bench --runtime rust -n 100 -m 1000 | tee bench-rust.txt
MIX_ENV=prod mix bench --runtime java -n 100 -m 1000 | tee bench-java.txt
diff bench-rust.txt bench-java.txt

# scripts/bench.sh is a thin wrapper that also passes FLEZHA_TRANSPORT:
./scripts/bench.sh --runtime java --transport quic -n 100 -m 1000
```

The Rust SDK's dispatch core was rewritten to use per-relay mailboxes
and a work-stealing pool — see
[`openspec/changes/optimize-rust-sdk-dispatch/`](openspec/changes/optimize-rust-sdk-dispatch/)
and [`docs/shared_library_architecture.md`](docs/shared_library_architecture.md)
for the design, and [`sdk/rust/CHANGELOG.md`](sdk/rust/CHANGELOG.md) for
the summary. The `--runtime rust` bench is now competitive with Java on
modern x86_64 hardware; lower-spec laptops will see a smaller but still
meaningful speedup. Criterion dispatch micro-benches live at
`sdk/rust/benches/` and run via:

```bash
cargo bench -p flezha_executor --features bench-internals --bench dispatch
```


**Options:**
- `--num-relays` / `-n`: Number of relays to create (default: 100)
- `--messages-per-relay` / `-m`: Number of messages to send per relay (default: 100)
- `--runtime` / `-r`: Which SDK to benchmark — `rust` (default) or `java`. The
  `java` runtime requires the test jar at
  `test/apps/java_executor/target/java_executor.jar` (build with
  `mix test.java_sdk` or `mvn -f test/apps/java_executor/pom.xml package`).

The performance test creates multiple relays, enables plugins, sends messages, and measures:
- Relay creation time
- Plugin setup time  
- Message sending time
- Message processing time
- Overall throughput (messages/second)

### Restart survival

Flezha auto-restores after any restart — single-controller crash,
rolling deploy, or full cluster cold boot. Two persistence layers
back this:

1. **ProcessHub durable registry** (`app_data/process_hub/<hub_id>/registry.dets`)
   — every relay child-spec, including `opts[:plugins]`, is persisted
   to disk through ProcessHub's `registry_backend: {:dets, ...}`. On
   boot, ProcessHub's coordinator runs the `:recovery_pending →
   :recovering | :normal` state machine: it exchanges modes with peers,
   skips replay if any peer is already `:normal` (avoiding stale-trample),
   otherwise replays the persisted registry under
   `:flezha, :recovery_window_ms` (default 10 s).
2. **Per-Partition WAL/snapshot** (`app_data/state/<relay_class>/<shard_id>/`)
   — change-by-change progress for every Partition. When a relay's
   first Partition stream reattaches after restart, the controller
   ships the assembled WAL+snapshot via `RestoreInitState` so the SDK
   resumes at the exact pre-shutdown sequence.

The path layout drops `executor_id`, so executor pod restarts that
rotate `executor_id` do not orphan state.

Operator surface:

```bash
flz boot status                              # per-namespace recovery_state + registry rows
flz boot await --timeout 60                  # block until every hub reports :normal
flz boot import-legacy-snapshot snap.flz     # one-shot migration from the removed `flz cluster snapshot` format
```

Disable the auto-replay state machine (registry mutations still
persist) with `FLEZHA_AUTO_RESTORE_ENABLED=false`. The single tolerated
data-loss case: cluster shut down, then only a strict subset of nodes
returns — rows held only on the missing nodes are lost. See
[`docs/auto_restore_on_boot.md`](docs/auto_restore_on_boot.md) for the
full safety table.

### Development
Run the application inside the docker containers.
See `docker/dev/README.md` for more information.

### Running Java SDK tests

Flezha ships a Java 21 SDK under `sdk/java/`. Its integration matrix runs
against the unchanged Elixir controller over both UDS and QUIC transports, and
covers both the Rust and Java runtimes. The single entry point is:

```bash
mix test.java_sdk
```

This alias (defined in `mix.exs`):
1. Cross-builds the host-triple `flz_jni` cdylib via
   `scripts/build-native.sh` and stages it at
   `sdk/java/flezha-sdk-native/src/main/resources/native/<os>-<arch>/libflz_transport.<ext>`.
2. Builds the Java SDK fat jar (`mvn -f sdk/java/pom.xml -P host-only package`).
3. Builds the Java test compute engine
   (`mvn -f test/apps/java_executor/pom.xml package`).
4. Runs `mix test --only java_sdk`.

Prerequisites: Java 21, Rust stable, `cargo-zigbuild`
(`cargo install cargo-zigbuild`), and a Zig toolchain (pulled in by
`cargo-zigbuild`).

On hardened containers where `/tmp` is mounted `noexec`, set
`FLEZHA_NATIVE_LIB_DIR=/path/to/writable` so `NativeLibraryLoader` can
extract `libflz_transport` somewhere it can be loaded. See
`sdk/java/README.md` for the full env-var reference.