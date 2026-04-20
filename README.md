# Flezha

## Description
Distributed stateful processors. Provdes a static and dynamic creation of processors.

## Building
- Copy `.env.example` and rename to `.env`
- Install testnet dependencies: `cd test/apps/testnet && npm install`
- Build the testnet: `npm run build` (from `test/apps/testnet`)
- Build the `rust_compute_engine` application.
- Build the agent `mix compile`

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