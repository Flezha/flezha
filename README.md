# Flezha

## Description
Distributed stateful processors. Provides static and dynamic creation of processors.

## Features
- Clustered, hot-swap-migratable processors
- SDK runtimes for Rust, Java, and others over a UDS or QUIC transport
- Installable plugin subsystem ([`docs/plugin_installation.md`](docs/plugin_installation.md))
- Cluster-wide snapshot & restore via `flz cluster` ([`docs/cluster-snapshot.md`](docs/cluster-snapshot.md))

## Building
- Copy `.env.example` and rename to `.env`
- Install testnet dependencies: `cd test/apps/testnet && npm install`
- Build the testnet: `npm run build` (from `test/apps/testnet`)
- Build the `rust_compute_engine` application.
- Build the agent `mix compile`

### Test prerequisites

`mix test` exercises the Java SDK matrix and the cluster-system suite,
which spawn the test Java compute engine from
`test/apps/java_compute_engine/target/java_compute_engine.jar`. That jar
is no longer committed (the previous convention bloated history with a
5.8 MB binary on every rebuild). Build it once before the first
`mix test` run, and again after any change under `sdk/java/` or
`test/apps/java_compute_engine/`:

```bash
mvn -q -f test/apps/java_compute_engine/pom.xml -DskipTests package
```

`mix test.java_sdk` does this for you (it also rebuilds the host-target
JNI cdylib). Skipping the build leaves the jar missing; the Java-SDK
matrix tests then fail fast with a pointer to this command.

## Examples

### Start agent (started automatically for dev mode)
Starting the node and specifying the external processes to be used by the node mynode.
PROD:
```bash
RELEASE_NODE=mynode FLEZHA_EXTERNAL_PROCESSES="cargo run --manifest-path=/home/anuar/Projects/personal/flezha/test/apps/rust_compute_engine/Cargo.toml --target-dir=./target" _build/prod/rel/flezha/bin/flz agent start --mode=attached_iex
```

DEV:
```bash
RELEASE_NODE=mynode _build/dev/rel/flezha/bin/flz agent start --mode=attached_iex
```

### Stop agent
Stopping the node.
```bash
_build/dev/rel/flezha/bin/flz agent stop
```

### Starting a processor.
```bash
_build/dev/rel/flezha/bin/flz processor start my_processor compare_filter --init_values='{
  "comparable": "hello world", 
  "inverted": false
}'
```

```bash
_build/dev/rel/flezha/bin/flzprocessor start my_processor do_nothing
```

### Stop a processor.
```bash
_build/dev/rel/flezha/bin/flz processor stop my_processor
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

### Enabling websocket plugin for processor. 
```bash
_build/dev/rel/flezha/bin/flz plugin enable processor_websocket  --settings='{
  "processor_id": "my_processor",
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
_build/dev/rel/flezha/bin/flz plugin enable processor_stdout  --settings='{
  "processor_id": "my_processor",
  "trigger": "on_out",
}'
```

### Get processor state.
```bash
_build/dev/rel/flezha/bin/flz processor state my_processor
```


### Performance Testing
Run performance tests to measure processor throughput and message processing capabilities:

```bash
# Run with default settings (100 processors, 100 messages per processor)
MIX_ENV=prod mix bench

# Run with custom settings
MIX_ENV=prod mix bench --num-processors 100 --messages-per-processor 100

# Short form aliases
MIX_ENV=prod mix bench -n 100 -m 50

# Bench the Rust SDK (default) vs the Java SDK — same agent, same protocol,
# same measurement code, so the Throughput numbers are directly comparable.
MIX_ENV=prod mix bench --runtime rust -n 100 -m 1000 | tee bench-rust.txt
MIX_ENV=prod mix bench --runtime java -n 100 -m 1000 | tee bench-java.txt
diff bench-rust.txt bench-java.txt

# scripts/bench.sh is a thin wrapper that also passes FLEZHA_TRANSPORT:
./scripts/bench.sh --runtime java --transport quic -n 100 -m 1000
```

The Rust SDK's dispatch core was rewritten to use per-processor mailboxes
and a work-stealing pool — see
[`openspec/changes/optimize-rust-sdk-dispatch/`](openspec/changes/optimize-rust-sdk-dispatch/)
and [`docs/shared_library_architecture.md`](docs/shared_library_architecture.md)
for the design, and [`sdk/rust/CHANGELOG.md`](sdk/rust/CHANGELOG.md) for
the summary. The `--runtime rust` bench is now competitive with Java on
modern x86_64 hardware; lower-spec laptops will see a smaller but still
meaningful speedup. Criterion dispatch micro-benches live at
`sdk/rust/benches/` and run via:

```bash
cargo bench -p flezha_compute_engine --features bench-internals --bench dispatch
```


**Options:**
- `--num-processors` / `-n`: Number of processors to create (default: 100)
- `--messages-per-processor` / `-m`: Number of messages to send per processor (default: 100)
- `--runtime` / `-r`: Which SDK to benchmark — `rust` (default) or `java`. The
  `java` runtime requires the test jar at
  `test/apps/java_compute_engine/target/java_compute_engine.jar` (build with
  `mix test.java_sdk` or `mvn -f test/apps/java_compute_engine/pom.xml package`).

The performance test creates multiple processors, enables plugins, sends messages, and measures:
- Processor creation time
- Plugin setup time  
- Message sending time
- Message processing time
- Overall throughput (messages/second)

### Restart survival

Restart-survival is operator-driven via `flz cluster snapshot` and
`flz cluster restore` — see
[`docs/cluster-snapshot.md`](docs/cluster-snapshot.md). There is no
automatic at-boot replay: an agent that comes back up after an
unattended crash starts with no processors. The supported flow for both
planned shutdowns and crash-recovery is:

```bash
# Before a planned restart (or on a routine cadence):
flz cluster snapshot /flezha/app_data/snapshots/latest.flz

# After the restart (or to recover from a crash, against the most recent
# snapshot file you have):
flz cluster restore  /flezha/app_data/snapshots/latest.flz
```

Plugins whose state is bound to a caller pid that died with the previous
BEAM (`processor_beam_msg`, `monitored_io_forward`) are excluded from
the snapshot and must be re-enabled by the operator or SDK reconnect
path after restore.

### Development
Run the application inside the docker containers.
See `docker/dev/README.md` for more information.

### Running Java SDK tests

Flezha ships a Java 21 SDK under `sdk/java/`. Its integration matrix runs
against the unchanged Elixir agent over both UDS and QUIC transports, and
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
   (`mvn -f test/apps/java_compute_engine/pom.xml package`).
4. Runs `mix test --only java_sdk`.

Prerequisites: Java 21, Rust stable, `cargo-zigbuild`
(`cargo install cargo-zigbuild`), and a Zig toolchain (pulled in by
`cargo-zigbuild`).

On hardened containers where `/tmp` is mounted `noexec`, set
`FLEZHA_NATIVE_LIB_DIR=/path/to/writable` so `NativeLibraryLoader` can
extract `libflz_transport` somewhere it can be loaded. See
`sdk/java/README.md` for the full env-var reference.