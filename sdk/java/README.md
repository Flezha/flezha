# Flezha Java SDK

Java 21 SDK for authoring Flezha compute engines. Talks to the Elixir controller
through JNI bindings over `flz_transport`; UDS and QUIC both supported.

Functionally at parity with the Rust SDK (`sdk/rust/`) — every operative shape,
durability tier, and delivery guarantee available there is available here.

**New here? Read [`flezha-sdk-examples/`](flezha-sdk-examples/README.md).** Seven
worked, compiled examples from "hello world" to a durable suspended agent.

## Quickstart

Three pieces: an **Operation** (the unit of work), an **Operative** (the topology
wiring Operations to Channels), and **registration** before boot.

```java
public final class EchoOperation
        implements Operation<String, EchoOperation.Empty, String, NoChange, WallClock, Void> {
    public record Empty() {}

    @Override public String name() { return "echo"; }

    // Erasure means the runtime needs these Class handles to wire the topology.
    @Override public Class<String>   inputType()  { return String.class; }
    @Override public Class<String>   outputType() { return String.class; }
    @Override public Class<NoChange> changeType() { return NoChange.class; }
    @Override public Class<WallClock> clockType() { return WallClock.class; }
    @Override public Class<Void>     configType() { return Void.class; }

    @Override public Empty initState(Optional<String> key, Optional<RestoreInitPayload> p) {
        return new Empty();
    }

    @Override public void run(String input, StateHandle<Empty, NoChange> state, OperationCtx ctx)
            throws OperationException {
        ctx.emit("echo: " + input);
    }
}

public final class EchoOperative implements UnboundedOperative {
    @Override public String factoryId() { return "echo"; }

    @Override public void channels(ChannelBuilder c) {
        c.input(String.class, "in").into(EchoOperation.class);
        c.channel(String.class, "out").from(EchoOperation.class).toPlugin("my_sink");
    }
}

public static void main(String[] args) throws Exception {
    Flezha.registerUnbounded(EchoOperative.class);   // must precede newInstance
    try (Flezha sdk = Flezha.newInstance(args[0])) {
        Thread.currentThread().join();
    }
}
```

## Running a runner

The controller owns placement, durability and restarts — but it is Elixir, and
your Operations are Java. It cannot run your code inside its own process. So your
process is where the compute lives, and `newInstance` is what turns your JVM into
it: the call dials the controller, advertises the factory ids you registered, and
then waits.

From there **control is inverted**. You never call Flezha to handle a message.
The controller decides which operatives run where, ships them to your process,
and calls your `run`. Your `main` is a host, not a client — which is why it does
nothing but stay alive.

Everything else follows from that. Registering a class only says what this
runner *can* serve; the controller decides what it *does* serve. A runner
with nothing placed on it is idle, not broken.

A **bounded** operative is the other shape: started with arguments, runs to
completion, torn down. The smallest one is a single `run`, no topology at all:

```java
public final class GreetBounded implements BoundedOperative {
    @Override public String factoryId() { return "greet"; }

    @Override public void run(BoundedOperativeCtx ctx, String args)
            throws BoundedOperativeException {
        ctx.emit("hello " + args);
    }
}

public static void main(String[] args) throws Exception {
    Flezha.registerUnbounded(EchoOperative.class);
    Flezha.registerBounded(GreetBounded.class);
    try (Flezha sdk = Flezha.newInstance(args[0])) {   // args[0] = namespace
        Thread.currentThread().join();
    }
}
```

**Start the controller first.** `newInstance` connects during the call and throws
if the control socket is not there; the reconnect loop only covers a controller
that drops *after* a successful connect.

```bash
java -jar my-runner.jar my_app          # my_app = the namespace

flz operative start echo1 echo -n my_app
flz operative start greet1 greet -n my_app --kind bounded --init_values '{"name":"world"}'
```

The argument to `newInstance` is the **namespace**, and it is the join key: the
`-n` above must match it, or the controller has no runner to place the
operative on. It is a logical grouping, not a transport address — every runner
on a node dials the same control socket regardless of namespace.

In `flz operative start <operative_id> <factory_id>`, the factory id (`echo`,
`greet`) is what `factoryId()` returns; the operative id (`echo1`, `greet1`)
names this one instance. `--init_values` is handed to a bounded `run` verbatim as
its `args` string — parse it yourself if it is JSON. The unbounded `echo1` then
runs until stopped; the bounded `greet1` emits once, completes, and disappears.

### Identity across restarts

On every connect the SDK presents a durable **stable id**, and the controller
replies with a per-connection session token. The stable id is the binding key —
it is why a restarted runner picks its operatives back up instead of losing
them. It is persisted at `FLEZHA_RUNNER_ID_PATH`, derived as
`<namespace>-<factory_hash>-<random>`.

The default path is `/var/lib/flezha-runner/id`, which a non-root JVM usually
cannot write. That degrades quietly: the runner still starts, but mints a fresh
id every boot and logs one `warn`, so its old bindings age out instead of being
reclaimed. Point it somewhere writable on any runner you intend to restart.
Full model: [`docs/resilience/runner_identity.md`](../../docs/resilience/runner_identity.md).

## The rule that matters most

Everything durable rests on **replay**: state is rebuilt by re-applying a change
log, a correction re-runs `run`, a crashed step is retried. So `run` must be a
**pure function of its inputs**.

| Instead of | Use |
|---|---|
| `System.currentTimeMillis()` | `ctx.now()` |
| `UUID.randomUUID()` | `ctx.stableId()` |

An Operation that reads ambient time or randomness rebuilds *differently* than it
ran, and nothing warns you — the state quietly diverges after a crash. Guard it
with `OperationTestHarness.replayEquals()` (state) **and** `replayEmitsEqual()`
(output); the second catches what the first cannot.

## Capabilities

| Capability | Declare | Gives you |
|---|---|---|
| Durable state | `Change` type + `state.apply(...)` | State survives restart, rebuilt from a change log. |
| Effectively-once output | `c.effectivelyOnce()` + `.keyed()` channels | State, output and consumed offset become durable in ONE record. A crash cannot advance the offset while losing the output. |
| Checkpointed intervals | `c.checkpointAck()` + `ctx.checkpoint()` | The offset is withheld until the state is coherent, so a half-built window is never persisted. |
| Late-input correction | `c.correctionWindow(ms)` | A late input rewinds the Segment and replays the slice in event-time order, repairing the earlier result. |
| Dead-letter queue | `.retryMax(n)` on the channel | A poison payload is retried, then parked durably — never silently dropped. |
| Dynamic segmentation | `operationKey(input)` | One Segment per key the stream actually carries, created on first sight. |
| Bounded transaction | `implements BoundedOperative` + `operations(...)` | Resumable chain: a committed step is skipped on resume, not re-run. Failure unwinds via `compensate`. |
| Durable loop / suspension | `loopMode()` + `wakeAfter()` | One durable iteration per message; parks when idle, holding its journal. |
| Segment retry | `retryAttempts()` / `retryBackoff()` | Only the failing segment re-runs; a failed attempt's output is dropped, not double-emitted. |

Each is a declaration the controller must honour, so declare what you need rather
than what sounds reassuring — a `correctionWindow` sizes the controller's input-log
retention, and a tier without keyed sinks is refused at registration.

## Modules

| Module | Purpose |
|---|---|
| `flezha-sdk-api` | Public API: `Flezha`, `UnboundedOperative`, `BoundedOperative`, `Operation`, `OperationCtx`, `StableId`, channel builders, clocks. No native dependency. |
| `flezha-sdk-protobuf` | Generated from `protobufs/runner_commands.proto`. |
| `flezha-sdk-runtime` | Control plane, data plane, framing, worker pool, reconnect, state persistence, bounded driver. |
| `flezha-sdk-native` | JNI bindings + bundled per-OS `libflz_transport`. |
| `flezha-sdk-testing` | `OperationTestHarness` — add with `<scope>test</scope>`. |
| `flezha-sdk-examples` | Worked examples. Compiled by the build, so they cannot rot. |
| `flezha-sdk` | Fat-jar aggregator — the published artifact. |

## Build

```bash
mvn -f sdk/java/pom.xml -P native-build package   # portable: bundles all 5 native targets
mvn -f sdk/java/pom.xml -P host-only  package     # fast local iteration (NOT portable)
```

Prerequisites: **JDK 21+**, a **Rust stable toolchain** (for the `flz_jni` bridge),
and **[cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild)** for the
native cross-build.

> Refreshing the committed dist jars? Run `mvn clean install` in `sdk/java` first.
> `scripts/build-dist.sh` will otherwise happily shade a **stale** artifact from
> your local `~/.m2` and report success — the jar builds, it is just not the code
> you wrote.

## Test

```bash
mvn -f sdk/java/pom.xml test                    # unit suite, no controller needed
mvn -f sdk/java/pom.xml -P coverage verify      # + JaCoCo
mix test.java_sdk                               # from repo root: full Java matrix vs a live controller
```

## Environment

| Variable | Default | Notes |
|---|---|---|
| `FLEZHA_TRANSPORT` | `uds` | `uds` or `quic`. |
| `FLEZHA_QUIC_ADDRESS` | `127.0.0.1:4433` | QUIC only. |
| `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` | QUIC only. |
| `FLEZHA_SERVER_CERT_PATH` | `app_data/certs/cert.pem` | Server mode only. |
| `FLEZHA_SERVER_KEY_PATH` | `app_data/certs/key.pem` | Server mode only. |
| `FLEZHA_SOCKET_FILES_PATH` | `app_data/sockets` | UDS only. |
| `FLEZHA_RUNNER_ID_PATH` | `/var/lib/flezha-runner/id` | Durable stable identity. Unwritable ⇒ a fresh id per boot and lost bindings. |
| `FLEZHA_WORKER_POOL_SIZE` | `availableProcessors()` | Shared runner draining every Segment mailbox. |
| `FLEZHA_NATIVE_LIB_DIR` | `$TMPDIR` | Where `libflz_transport` is extracted. Set it on containers where `/tmp` is `noexec`. |
| `FLEZHA_BOUNDED_PARK_IDLE_MS` | `5000` | Idle hysteresis before a looping bounded operative parks. |

## Further reading

- [`flezha-sdk-examples/`](flezha-sdk-examples/README.md) — the seven examples.
- `docs/sdk_protocol_spec.md` — wire protocol, framing, output identity stamps.
- `docs/resilience/data_durability.md` — where state and output are lost vs not.
- `docs/java_state_persistence.md` — Kryo registration and the Change machinery.
