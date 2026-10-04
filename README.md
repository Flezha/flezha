# Flezha

Distributed stateful operators: a compute graph of tasklets, each with its
own durable per-Cell state, placed and migrated across a cluster by a
controller, with static and dynamic creation of operators. A runner hosts
operators; an operator is the unit you start, and it runs as a flow (until you
stop it or it completes itself) or in steps (a run with a start and an end).

It is built for ETL pipelines: extract from files, HTTP endpoints, sockets or
another operator, transform with per-key state that survives a crash, and load
into Postgres, Kafka or files, as a continuous stream or as a scheduled batch
job. [ETL pipelines](#etl-pipelines) maps each stage to what carries it.

## Try it without any of that

You do not need a controller, a container or a socket to see one run. **Local
mode** takes the operator class you would deploy, runs its declared graph in
your own process, and hands you the output:

```rust
let run = LocalOperator::start::<WordCount>()?;
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
Cell per key with its own mailbox, the retry ladder, inter-tasklet
delivery); durability, plugins, placement and migration, the dead-letter queue,
observability export, authz and multi-runner scale need a controller. It ships
in every SDK.

**[`docs/topics/basics/local_mode.md`](docs/topics/basics/local_mode.md)** walks through it in five
minutes.

## ETL pipelines

Extract, transform, load is the job Flezha is shaped for. An operator declares
where records come in, what happens to them and where they go, and plugins attach
the real endpoints, so the same code can read a file today and an HTTP feed
tomorrow with no rebuild.

| Stage | What carries it |
| --- | --- |
| Extract | A file read line by line, resuming from its last committed offset ([`file.md`](docs/plugins/file.md)); an HTTP resource, injected only when it changes ([`http_poll.md`](docs/plugins/http_poll.md)); a WebSocket feed ([`websocket_subscription.md`](docs/plugins/websocket_subscription.md)); another operator's output ([`operator_forward.md`](docs/plugins/operator_forward.md)); or a plugin of your own ([`docs/topics/plugins/installing_plugins.md`](docs/topics/plugins/installing_plugins.md)) |
| Transform | `map`, `filter`, `flat_map` and `key_by` declared on the edges ([`docs/topics/basics/streams.md`](docs/topics/basics/streams.md)); tasklets with durable per-key state for aggregation and deduplication; joins and enrichment by merging channels ([`docs/topics/advanced/merged_inputs.md`](docs/topics/advanced/merged_inputs.md)); windows and timeouts from per-key timers ([`docs/topics/advanced/timers.md`](docs/topics/advanced/timers.md)) |
| Load | Postgres, each batch writing its rows and its progress watermark in one transaction ([`postgres_sink.md`](docs/plugins/postgres_sink.md)); Kafka, with retries, batching and an idempotent producer ([`kafka_producer.md`](docs/plugins/kafka_producer.md)); a file; or the next operator |

What a pipeline usually has to build around itself is already there:

- **A crash resumes instead of starting over.** Per-key state is journaled and
  snapshotted on the controller, and an operator picks up where it stopped
  after a runner crash or a controller restart
  ([`docs/topics/basics/running_operators.md`](docs/topics/basics/running_operators.md)).
- **A bad record neither stops the stream nor vanishes.** It is retried, then
  parked in a dead-letter queue; `flz dlq list` shows it and `flz dlq replay`
  sends it back through the normal input path
  ([`docs/topics/basics/durability.md`](docs/topics/basics/durability.md)).
- **Effectively-once when you need it.** Delivery is at-least-once by default.
  Opt a channel into effectively-once and a Postgres load commits each record
  once, retries included
  ([`docs/topics/basics/durability.md`](docs/topics/basics/durability.md)).
- **Late data corrects the past.** With late-input correction declared on top of
  effectively-once, a record that arrives late rewinds its key and replays the
  input in event-time order, and a keyed sink overwrites the stale rows
  ([`docs/topics/basics/durability.md`](docs/topics/basics/durability.md)).
- **Batch jobs as well as streams.** A steps operator is a run with a start
  and an end: it commits progress at every step, retries a failing step before
  it aborts, and can undo finished steps with saga compensation
  ([`steps_profile.md`](docs/architecture/steps_profile.md)).
  `flz schedule` starts one on a cron schedule, with no external scheduler.
- **Scale by key.** `--partitions N` splits one flow operator, or a
  per-input steps operator, into N instances across its key space
  ([`docs/topics/advanced/partitioning.md`](docs/topics/advanced/partitioning.md)).

## Flezha and AI

Flezha is built to work with AI from both sides: AI coding tools can drive it and
build on it, and it can run the services AI applications are made of.

### For AI tools working with Flezha

- **Errors a program can act on.** Every failure has one shape on every surface,
  the `flz` CLI, the consumer API and every SDK: a stable `code`, a `class`, a
  one-sentence `message`, the `details` it names and a `hint` with the next command
  to run. The `flz` exit code follows the class, so a tool can tell "fix the
  arguments" from "retry later" without reading the text. Every code is listed in
  [`docs/errors.md`](docs/errors.md).
- **JSON from every command.** Each `flz` verb takes `--json`, and a failure is then
  one line of JSON ([`docs/topics/cli/cli.md`](docs/topics/cli/cli.md)).
- **Documentation for language models.** The generated documentation carries an
  `llms.txt` index and a Markdown copy of every page next to the HTML one.
- **Fast feedback.** Local mode runs your processing graph inside your own process,
  with no controller, so a change can be tried in seconds
  ([`docs/topics/basics/local_mode.md`](docs/topics/basics/local_mode.md)). A graph
  that cannot work is refused when the runner starts, with an error code that names
  why.

<!-- TODO: Flezha MCP server, so an AI assistant can inspect and operate a running
deployment through MCP: list namespaces, operators and runners, read state, send
messages and calls, read and replay dead letters, and get coded errors back. -->
<!-- TODO: an AGENTS.md template for projects built on Flezha, pointing an AI
assistant at the documentation, llms.txt, the error reference and the CLI's JSON
output. -->
<!-- TODO: a Claude Code skill or plugin that knows the flz CLI and each SDK's
current form, so an assistant writes and runs Flezha code without guessing. -->

### For AI services running on Flezha

Flezha has no AI framework of its own. The code it runs is ordinary code in your
language, so it calls whichever model SDK or agent framework you already use. What
Flezha adds is everything around that code:

- **One session per conversation.** Each conversation runs as its own instance with
  its own state, many run at once, and one that nobody is talking to sleeps
  ([`docs/topics/advanced/sessions.md`](docs/topics/advanced/sessions.md)).
- **Answers as they are written.** A reply can be streamed to the reader while the
  model is still producing it
  ([`docs/topics/advanced/streamed_answers.md`](docs/topics/advanced/streamed_answers.md)).
- **Turns taken back.** A conversation can go back to an earlier turn, and a failed
  turn can fail alone without losing the conversation
  ([`docs/topics/advanced/rewinding_a_turn.md`](docs/topics/advanced/rewinding_a_turn.md)).
- **Behind HTTP, or called by another service.** The HTTP server plugin puts a
  session behind an endpoint, and a call waits for its answer
  ([`docs/plugins/http_server.md`](docs/plugins/http_server.md),
  [`docs/topics/advanced/operator_calls.md`](docs/topics/advanced/operator_calls.md)).
- **A crash does not end the conversation.** A session keeps a journal, so when its
  runner dies mid-turn the conversation carries on from where it stopped, and with
  effectively-once delivery the turn's effects are counted once.

[The agentdesk tutorial](docs/tutorials/agentdesk/agentdesk.md) builds all of this,
with a demo model standing in for a real one.

<!-- TODO: AI plugins, for example a model-call plugin (one place for provider keys,
retries, rate limits and token accounting) and a vector-store source and sink.
Decide which ones, then specify each in OpenSpec. -->

## Reading further

[`docs/architecture/`](docs/architecture/README.md) — the durable-compute
kernel, the two operator profiles, and the diagrams. Vocabulary:
[`docs/glossary.md`](docs/glossary.md). Every knob, by layer:
[`docs/configuration.md`](docs/configuration.md).

## Features
- ETL pipelines, run continuously or as scheduled batch jobs ([ETL pipelines](#etl-pipelines))
- Run a whole graph in process, with no controller ([`docs/topics/basics/local_mode.md`](docs/topics/basics/local_mode.md))
- Clustered, hot-swap-migratable operators
- SDK runtimes for Rust, Java, and others over a UDS transport, the default and stable, or over
  QUIC, which is [experimental: quic](docs/publishing/maturity.md#quic)
- Per-Cell durable state — WAL + snapshot, crash-resume, point-in-time query ([`docs/internals/state_persistence.md`](docs/internals/state_persistence.md))
- Opt-in effectively-once delivery — durable input log, atomic effect record, fsync-gated outbox ([`docs/architecture/kernel.md`](docs/architecture/kernel.md))
- Steps operators — a tasklet tree with saga compensation, a durable loop, and suspension ([`docs/architecture/steps_profile.md`](docs/architecture/steps_profile.md))
- Installable plugin subsystem ([`docs/topics/plugins/installing_plugins.md`](docs/topics/plugins/installing_plugins.md))
- A Kafka producer, as a plugin package, with key partitioning, retries, idempotence, batching, and bounded queueing ([`docs/plugins/kafka_producer.md`](docs/plugins/kafka_producer.md))
- Transactional Postgres sink — upsert + progress watermark in one transaction ([`docs/topics/basics/durability.md`](docs/topics/basics/durability.md))

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
- Check out the testnet, a git submodule: `git submodule update --init test/apps/testnet`
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
[`docs/topics/basics/deploying_a_controller.md`](docs/topics/basics/deploying_a_controller.md).

## Wiping runtime data

`flz reset` clears the Controller's durable on-disk state so the next boot
starts from a clean slate, and clears nothing while the Controller is up. Which
directories are in scope (the socket directory is resolved, not fixed), the
flags, the exit codes and the user recipes for docker-compose, k8s and CI:
[`docs/topics/basics/running_operators.md`](docs/topics/basics/running_operators.md).

## Code style

The repo enforces one formatter per language, plus the Python linter, via the
[`format`](.github/workflows/format.yml) workflow on every PR — drift
blocks merge. The workflow runs the same script you do, so the two cannot
cover different languages:

```bash
python3 scripts/naming_gate.py --run @format   # every formatter in check mode,
                                              # plus the licensing checks
scripts/format-apply.sh                       # auto-fix what the check reports
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
RELEASE_NODE=mynode FLEZHA_EXTERNAL_PROCESSES="cargo run --manifest-path=sdk/rust/test_app/Cargo.toml --target-dir=./target" _build/prod/rel/flezha/bin/flz controller start --mode=attached_iex
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

### Starting an operator.
```bash
_build/dev/rel/flezha/bin/flz operator start do_nothing --id my_operator
```

`--init-values` seeds Tasklet state at start. Its top level is a map of
**Tasklet name** to that Tasklet's starting state, so one operator seeds
several Tasklets from one document:

```bash
_build/dev/rel/flezha/bin/flz operator start compare_filter --id my_operator --init-values='{
  "compare_filter_tasklet": {"comparable": "hello world", "inverted": false}
}'
```

Name only the Tasklets you want to seed; one you leave out starts from the
state its class builds. A Tasklet with declared Cell keys takes one state
per key instead: `{"<tasklet>": {"<cell_key>": <state>}}`. A name the class
does not carry is ignored with a line on the runner's stderr and the start
still succeeds, so check the spelling. The full rules, including what a
workflow does with the same flag, are in
[`docs/sdk/protocol_spec.md`](docs/sdk/protocol_spec.md) "Operator state JSON
shape".

### Stop an operator.
```bash
_build/dev/rel/flezha/bin/flz operator stop my_operator
```

### Installable plugins
```bash
# Full lifecycle reference (install / upgrade / uninstall / enable / disable)
# lives in docs/topics/plugins/installing_plugins.md — including package layout, manifest
# schema, checksum generation, and Ed25519 signing.
_build/dev/rel/flezha/bin/flz plugin install /path/to/my_plugin-1.0.0.fpz
_build/dev/rel/flezha/bin/flz plugin enable my_plugin
```

See [`docs/topics/plugins/installing_plugins.md`](docs/topics/plugins/installing_plugins.md) for the
full authoring guide and [`examples/plugins/hello_world/`](examples/plugins/hello_world/)
for a minimal copy-paste starting point.

### Enabling websocket plugin for operator.

`flz plugin enable <plugin_id> <provider_id> …`. Provider options carry the
`--X` prefix and reach `init_provider/1`, and a provider with a strict
allow-list refuses every `--X` key it does not own. The enable itself reads
three more options: `--namespace` (or `-n`), `--operator_id`, the operator the
plugin attaches to, and `--hook`. Any other plain option is refused, naming it.

A hook is a point at which the operator calls the plugin, named by the
provider callback it runs. `--hook=on_output` sends the plugin every payload the
operator emits, `--hook=on_input` every payload it ingests, and
`--hook=on_interval --on_interval.interval_ms=5000` calls it on a timer whose
interval has no default. `--hook` repeats, and a hook's own options are written
`--<name>.<key>=<value>`. Naming no hook means no hooks, which is what a plugin
that only receives wants: the websocket below holds its own socket and injects
every frame into the operator's input.

```bash
_build/dev/rel/flezha/bin/flz plugin enable my_ws operator_websocket \
  --operator_id=my_operator \
  --Xuri="ws://localhost:4500" \
  --Xpost_connect='{"payload":{"type":"text","message":"{\"event_type\":\"subscribe\",\"event_data\":{\"default_message\":\"hello world\",\"interval\":1000}}"}}'
```

```bash
_build/dev/rel/flezha/bin/flz plugin enable my_stdout operator_stdout \
  --operator_id=my_operator \
  --hook=on_output
```

### Get operator state.
```bash
_build/dev/rel/flezha/bin/flz operator state my_operator
```


### Performance Testing

Use `./scripts/bench.sh` to measure throughput. Switching axes are independent:
runtime (`rust`/`java`), transport (`uds`/`quic`), shape (`regression`/`cell`).

```bash
./scripts/bench.sh                                # rust + uds, defaults
./scripts/bench.sh -r java -t quic -n 100 -m 1000 # any combination
./scripts/bench.sh --shape cell --pool-sizes 1,8
./scripts/bench.sh --help
```

Full reference (flags, shapes, CI gates, floors, triage):
[`docs/operations/benchmarking.md`](docs/operations/benchmarking.md).

### Restart survival

Flezha auto-restores after any restart — single-controller crash, rolling
deploy, or full cluster cold boot. Two persistence layers back it: the
ProcessHub durable registry, which holds every operator's child-spec including
`opts[:plugins]`, and the per-Cell WAL/snapshot, which holds change-by-change
progress and is shipped back to the SDK when the Cell's stream reattaches.
What is written where, the boot reconcile, the `flz boot status` and
`flz boot await` surface, the `FLEZHA_AUTO_RESTORE_ENABLED` kill-switch and the
safety matrix per scenario — including the one tolerated data-loss case:
[`docs/topics/basics/running_operators.md`](docs/topics/basics/running_operators.md).

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
| The SDK side — `sdk/`, `flz_core/`, `flz_core_ffi/`, `flz_transport/`, `protobufs/` | [Apache-2.0](LICENSES/Apache-2.0.txt) |

The per-path map is machine-readable in [`REUSE.toml`](REUSE.toml) and checked
by CI (`reuse lint`). In short: build on the SDKs freely; if you modify and
distribute the controller's files, publish those modifications. Your own files
never inherit either license.

**Status and warranty.** Flezha is pre-1.0 software provided "AS IS", without
warranty of any kind; liability is disclaimed to the extent the licenses allow
(MPL-2.0 §6–7, Apache-2.0 §7–8). It is not certified for any regulated use.
Every durability and delivery default is documented in
[`docs/configuration.md`](docs/configuration.md) and
[`docs/topics/basics/durability.md`](docs/topics/basics/durability.md) —
read them before trusting the system with data.

**Name.** "Flezha" names this project and its releases. The licenses grant no
trademark rights (MPL-2.0 §2.3, Apache-2.0 §6): a fork, derived work or hosted
service must not use the name or a confusingly similar one.

Third-party components bundled in binary distributions are listed in
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md). Contributions:
[`CONTRIBUTING.md`](CONTRIBUTING.md) (CLA required). Security reports:
[`SECURITY.md`](SECURITY.md).
