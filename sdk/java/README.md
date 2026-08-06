# Flezha Java SDK

Java 21 SDK for authoring Flezha compute engines. Talks to the Elixir controller
through JNI bindings over `flz_transport`; UDS and QUIC both supported.

Functionally at parity with the Rust SDK (`sdk/rust/`) — every operative shape,
durability tier, and delivery guarantee available there is available here.

**New here? Read [`flezha-sdk-examples/`](flezha-sdk-examples/README.md).** Seven
worked, compiled examples from "hello world" to a durable suspended agent.

## Quickstart

Three pieces: a **Procedure** (the unit of work), an **Operative** (the topology
wiring Procedures to Channels), and **registration** before boot.

```java
public final class EchoProcedure
        implements Procedure<String, EchoProcedure.Empty, String, NoChange, WallClock, Void> {
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

    @Override public void run(String input, StateHandle<Empty, NoChange> state, ProcedureCtx ctx)
            throws ProcedureException {
        ctx.emit("echo: " + input);
    }
}

public final class EchoOperative implements UnboundedOperative {
    @Override public String factoryId() { return "echo"; }

    @Override public void channels(ChannelBuilder c) {
        // Producer-first: `.into` wires a consumer and returns THAT
        // procedure's output stream; `output` closes it plugin-bound.
        c.input(String.class, "in").into(EchoProcedure.class).named("out").output("my_sink");
    }
}

public static void main(String[] args) throws Exception {
    Flezha.createRunner(args[0])                     // args[0] = the namespace
            .defineUnbounded(EchoOperative.class)    // legal before AND after start()
            .run();                                  // connect + block until shutdown
}
```

`run()` is `start()` + `awaitTermination()` — the one-liner for a standalone
binary. Embedding into a larger app? `start()` returns a `FlezhaRunner`
handle (`AutoCloseable`) and keeps your thread:

```java
try (FlezhaRunner runner = Flezha.createRunner(ns).defineUnbounded(EchoOperative.class).start()) {
    // ... your app; runner.awaitTermination() to block, Flezha.currentRunner() from anywhere
}
```

At most one runner runs per JVM: a second `start()` throws
`IllegalStateException` naming the running namespace (see
`docs/sdk_naming_conventions.md` for the lifecycle vocabulary).

## Authoring a topology

Graphs read in flow order (`sdk-graph-authoring`): the channel between two
procedures carries the producer's declared output type — no type token, no
channel name unless you want one (`.named(...)` overrides the name derived
from the producer's identity; entry names are explicit — they are the
operative's external address). A typed entry decodes at the edge (Jackson
by default, or a custom `bytes -> T` parser overload); a failing parse rides
the channel's retry→DLQ ladder:

```java
c.input(OrderEvent.class, "orders")      // JSON-decoded at the entry edge
        .into(Validate.class)
        .into(Alert.class)
        .output("alerts");               // plugin-bound terminal
```

Branching is handle reuse — hold the stream in a variable and wire it twice —
or the equivalent `branch(...)` sugar; fan-in is two streams naming one
(unnamed) class, whose node accumulates both contributions. One class can sit
at several graph positions via named nodes (`into(Filter.class, "spam_filter")`),
each with its own state identity:

**Variable spelling** — hold the handle, wire it as many times as you like.
This is the general form: the handle can cross helper methods, loops, or
conditionals, and each arm keeps its own chain:

```java
var validated = c.input(OrderEvent.class, "orders").into(Validate.class);

// Two consumers on one channel: both receive every value Validate
// releases. Each arm is an ordinary statement that keeps chaining.
validated.into(FraudCheck.class).into(Merge.class);
validated.into(Receipt.class).into(Merge.class);   // same class ⇒ same node: a join
```

**`branch(...)` spelling** — pure sugar over the same mechanism: each arm
lambda receives the same handle, and the frozen topology is *identical* to
the variable form (pinned by a descriptor-equality test). Use it when the
fan-out is the last thing you do with the stream and a variable would only
add a name:

```java
c.input(OrderEvent.class, "orders").into(Validate.class).branch(
        b -> b.into(FraudCheck.class).into(Merge.class),
        b -> b.into(Receipt.class).into(Merge.class));
```

The two spellings mix freely — `branch` returns the same handle, so you can
tap it, name it, or wire a third consumer after the call. Fewer than two arms
is refused at registration.

Graphs must be acyclic; per-channel config (`codec`, `keyed`, `onFull`,
`retryMax`, `retryBackoffMs`) chains on the handle; the hybrid tap is
`alsoOutput(name[, serializer])`.

### A fuller graph: named nodes, a hybrid tap, a keyed sink

Everything composes on the same handles. Here one `Filter` class serves two
positions with independent state, the validated stream is tapped to an audit
plugin *and* consumed downstream, the join accumulates both filters'
verdicts, and the terminal channel carries output identity for a
changelog-style sink:

```java
public final class RiskOperative implements UnboundedOperative {
    @Override public String factoryId() { return "risk"; }

    @Override public void channels(ChannelBuilder c) {
        // Entry-side config chains BEFORE .into: this channel retries a
        // failing consumer (or a failing typed parse) 5 times, 20 ms base.
        var validated = c.input(OrderEvent.class, "orders")
                .retryMax(5)
                .retryBackoffMs(20)
                .into(Validate.class)
                .named("orders.validated")   // overrides the derived name
                .alsoOutput("audit");        // tap: default JSON to `audit`

        // One class, two nodes: each keeps its own per-entity state under
        // its node identity (state dirs, journals, diagnostics). Both arms
        // name the same (unnamed) Merge class, so their verdicts fan IN —
        // either arm's `.into(Merge.class)` returns the one join's handle.
        var verdicts = validated.into(Filter.class, "fraud_filter").into(Merge.class);
        validated.into(Filter.class, "spam_filter").into(Merge.class);

        // The join's output closes plugin-bound and keyed: every emit calls
        // ctx.emitKeyed(key, value) and frames carry flz.* identity stamps,
        // so a changelog sink upserts instead of appending.
        verdicts.named("risk.verdicts").keyed().output("verdict_sink");
    }
}
```

Reading order is flow order: every statement starts at a handle you already
hold, and the only names in the graph are the ones something external needs —
the entry (`orders`), the tap (`audit`), the sink (`verdict_sink`), and any
channel you chose to `.named(...)`.

### A structured bounded DAG

Bounded operatives author on the same builder. A durable class journals each
proc barrier, so inter-proc channels declare `.codec()`; a fan-in node runs once,
after **all** its producers committed, with the accumulated batch — and on an
abnormal exit the saga unwinds reverse-topologically (the join before either
producer, the shared root last):

```java
public final class SettleBounded implements BoundedOperative {
    @Override public String factoryId() { return "settle"; }

    @Override public int retryAttempts() { return 3; } // entity ladder before the saga

    @Override public void procedures(ChannelBuilder c) {
        // The diamond: Reserve fans out to Charge and Ledger, whose outputs
        // accumulate into one Reconcile join.
        var reserved = c.input(SettleJob.class, "in")
                .into(Reserve.class)
                .named("reserved")
                .codec();
        reserved.into(Charge.class).named("charged").codec().into(Reconcile.class);
        reserved.into(Ledger.class)
                .named("booked")
                .codec()
                .into(Reconcile.class)
                .named("reconciled")
                .output("settlement_sink");
    }
}
```

A crash between the two producers resumes with the committed producer's
**journaled** handoff banked and only the uncommitted arm re-run; `Reconcile`
still runs exactly once. Cycles are refused at registration; a chained loop
(`LoopMode.CHAINED`) additionally requires the graph to converge on a single
codec'd terminal.

Parameterized procedures use the `Config` type parameter with `fromConfig` —
the sanctioned path for one class serving several configurations:

```java
public record SmaConfig(int window) {}

public final class Sma implements Procedure<Double, SmaState, Double, NoChange, WallClock, SmaConfig> {
    private int window;

    @Override public Class<SmaConfig> configType() { return SmaConfig.class; }

    @Override public Sma fromConfig(SmaConfig cfg) {
        Sma proc = new Sma();
        proc.window = cfg.window();
        return proc;
    }
    // ... name()/inputType()/outputType()/initState()/run() as usual
}
```

## Running a runner

The controller owns placement, durability and restarts — but it is Elixir, and
your Procedures are Java. It cannot run your code inside its own process. So your
process is where the compute lives, and `start()`/`run()` is what turns your JVM
into it: the call dials the controller, advertises the factory ids you declared,
and then waits.

From there **control is inverted**. You never call Flezha to handle a message.
The controller decides which operatives run where, ships them to your process,
and calls your `run`. Your `main` is a host, not a client — which is why it does
nothing but stay alive.

Everything else follows from that. Declaring a class only says what this
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
    Flezha.createRunner(args[0])                       // args[0] = the namespace
            .defineUnbounded(EchoOperative.class)
            .defineBounded(GreetBounded.class)
            .run();
}
```

**Start the controller first.** `start()` connects during the call and throws
if the control socket is not there; the reconnect loop only covers a controller
that drops *after* a successful connect.

```bash
java -jar my-runner.jar my_app          # my_app = the namespace

flz operative start echo1 echo -n my_app
flz operative start greet1 greet -n my_app --kind bounded --init_values '{"name":"world"}'
```

The argument to `createRunner` is the **namespace**, and it is the join key: the
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

A Procedure that reads ambient time or randomness rebuilds *differently* than it
ran, and nothing warns you — the state quietly diverges after a crash. Guard it
with `ProcedureTestHarness.replayEquals()` (state) **and** `replayEmitsEqual()`
(output); the second catches what the first cannot.

## Capabilities

| Capability | Declare | Gives you |
|---|---|---|
| Durable state | `Change` type + `state.apply(...)` | State survives restart, rebuilt from a change log. |
| Effectively-once output | `c.effectivelyOnce()` + `.keyed()` channels | State, output and consumed offset become durable in ONE record. A crash cannot advance the offset while losing the output. |
| Checkpointed intervals | `c.checkpointAck()` + `ctx.checkpoint()` | The offset is withheld until the state is coherent, so a half-built window is never persisted. |
| Late-input correction | `c.correctionWindow(ms)` | A late input rewinds the Entity and replays the slice in event-time order, repairing the earlier result. |
| Dead-letter queue | `.retryMax(n)` on the channel | A poison payload is retried, then parked durably — never silently dropped. |
| Edge transforms | `.map(T.class, f)` / `.filter(p)` / `.flatMap(T.class, f)` on the wiring | Reshape, drop or fan out values without a Procedure — no Entity, no mailbox, no dispatch hop. |
| Dynamic entity mode | `.keyBy(f)` on the wiring | One Entity per key the stream actually carries, created on first sight — and visible where the graph is declared. |
| Bounded transaction | `implements BoundedOperative` + `procedures(...)` | Resumable chain: a committed step is skipped on resume, not re-run. Failure unwinds via `compensate`. |
| Durable loop / suspension | `loopMode()` + `wakeAfter()` | One durable iteration per message; parks when idle, holding its journal. |
| Entity retry | `retryAttempts()` / `retryBackoff()` | Only the failing entity re-runs; a failed attempt's output is dropped, not double-emitted. |

Each is a declaration the controller must honour, so declare what you need rather
than what sounds reassuring — a `correctionWindow` sizes the controller's input-log
retention, and a tier without keyed sinks is refused at registration.

## Modules

| Module | Purpose |
|---|---|
| `flezha-sdk-api` | Public API: `Flezha`, `UnboundedOperative`, `BoundedOperative`, `Procedure`, `ProcedureCtx`, `StableId`, channel builders, clocks. No native dependency. |
| `flezha-sdk-protobuf` | Generated from `protobufs/runner_commands.proto`. |
| `flezha-sdk-runtime` | Control plane, data plane, framing, worker pool, reconnect, state persistence, bounded driver. |
| `flezha-sdk-native` | JNI bindings + bundled per-OS `libflz_transport`. |
| `flezha-sdk-testing` | `ProcedureTestHarness` — add with `<scope>test</scope>`. |
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
| `FLEZHA_WORKER_POOL_SIZE` | `availableProcessors()` | Shared runner draining every Entity mailbox. |
| `FLEZHA_NATIVE_LIB_DIR` | `$TMPDIR` | Where `libflz_transport` is extracted. Set it on containers where `/tmp` is `noexec`. |
| `FLEZHA_BOUNDED_PARK_IDLE_MS` | `5000` | Idle hysteresis before a looping bounded operative parks. |

## Further reading

- [`flezha-sdk-examples/`](flezha-sdk-examples/README.md) — the seven examples.
- `docs/sdk_protocol_spec.md` — wire protocol, framing, output identity stamps.
- `docs/resilience/data_durability.md` — where state and output are lost vs not.
- `docs/java_state_persistence.md` — Kryo registration and the Change machinery.
