# Flezha Java SDK

Java 21 SDK for authoring Flezha compute engines.

You write procedures that handle one message at a time, and operatives that
declare how those procedures are connected. A controller then owns everything
around them: placing each operative on a host, persisting per-entity state,
enforcing the delivery guarantee you declared, retrying failures, relocating work
when a host is withdrawn, and reconnecting when a connection drops. Recovery
resumes an entity at the point it reached rather than restarting it, which is the
property the rest of this document keeps returning to.

Functionally at parity with the Rust SDK: every operative shape, durability tier
and delivery guarantee available there is available here.

## Add it

```xml
<dependency>
    <groupId>com.flezha</groupId>
    <artifactId>flezha-sdk</artifactId>
    <version>0.1.0</version>
</dependency>
```

Java 21 or later. `flezha-sdk-testing` is a separate artifact, with `<scope>test</scope>`.

## A complete example

Three files, and this structure holds however large the application becomes.

**The procedure** contains the code invoked once per message. Invocation is
serialised per entity, so two messages for the same key are never handled
concurrently and a procedure needs no internal locking around its own state;
messages for different keys are handled concurrently, possibly on different
hosts. This procedure declares no state, so it is a pure function of its input.

```java
package com.example.echo;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.*;
import java.util.Optional;

public final class EchoProcedure
        implements Procedure<String, EchoProcedure.None, String, NoChange, WallClock, Void> {

    /** Nothing is remembered between messages, so the state type is empty. */
    public record None() {}

    /** Durable: it names this procedure in saved state. Choose it once. */
    @Override
    public String name() {
        return "echo";
    }

    @Override
    public None initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new None();
    }

    @Override
    public void run(String input, StateHandle<None, NoChange> state, ProcedureCtx ctx)
            throws ProcedureException {
        ctx.emit("echo: " + input);
    }
}
```

The six type arguments on `Procedure` are, in declaration order: the input value,
the per-entity state, the emitted output, the change type used to record state
mutations, the clock the procedure reads, and its configuration type. The runtime
recovers all six as `Class` handles from the `implements` clause, so they must be
concrete there. `NoChange` opts out of the persistence machinery entirely, and
`Void` declares that the procedure takes no configuration.

**The operative** declares the graph and holds no logic of its own. A procedure
cannot be deployed alone, because nothing in the system knows where its messages
originate or where its output belongs. The operative supplies that binding, which
makes it the unit the controller schedules onto a host, restarts after a failure,
relocates when a host is withdrawn, and scales across several hosts.

```java
package com.example.echo;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;

public final class EchoOperative implements StreamOperative {

    /** Also durable. Renaming it makes the cluster forget what was running. */
    @Override
    public String factoryId() {
        return "echo";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "in").into(EchoProcedure.class).named("out").sink("my_sink");
    }
}
```

The wiring expression reads left to right: declare an entry channel named `in`
carrying strings, route it to the echo procedure, label that procedure's output
`out`, and deliver the labelled channel to a plugin sink named `my_sink`. Every
call returns the next handle in the chain, so a graph of any size is expressed as
one fluent expression, and the whole declaration is validated at registration
rather than failing later during processing.

The label supplied to `named` is an identity rather than documentation. It is
what a sink registers against and what a test feeds or reads; unlabelled, a
channel takes the producing procedure's name.

**The main** starts a runner: the process that connects to a controller and serves
the operatives declared on it.

```java
package com.example.echo;

import com.flezha.sdk.Flezha;

public final class Main {
    public static void main(String[] args) throws Exception {
        Flezha.createRunner("my_app")
                .defineStream(EchoOperative.class)
                .run();
    }
}
```

`run()` connects and blocks until shutdown, which is either a SIGTERM or a
`close()` from another thread. `start()` returns to the caller instead and leaves
the runner to be closed explicitly.

Every operative must be declared on the builder before the runner starts. One
runner can serve several, and `defineWorkflow(...)` is the equivalent call for
the workflow kind.

To run this without a controller, replace the main with `LocalOperative`, which
executes the same declared wiring inside the calling JVM and delivers sink output
to a callback. Nothing is durable in that mode, and declarations requiring a
controller are refused at startup with the responsible declaration named.

## Where to go

- **Learn the SDK in order:** [`flezha-sdk-examples/`](flezha-sdk-examples/README.md).
  Twelve compiled examples, six forming a ladder from hello world to a tested
  graph, and six covering specific problems you may or may not have. They are
  built by the reactor and most are covered by a test, so an example that drifts
  from the SDK breaks the build rather than misleading you.
- **Look up a type or method:** the generated javadoc. Every concept is
  documented on the type it concerns, so the explanation for why a field cannot
  be assigned directly sits on `StateHandle` and `Change`, where a reader
  encounters the problem.

      mvn -f sdk/java/pom.xml -pl flezha-sdk-api javadoc:javadoc

  The output is `flezha-sdk-api/target/site/apidocs/index.html`. The plugin skips
  when its output appears current, so delete that directory if an edit does not
  appear.
- **Run a graph without a controller:** `LocalOperative` executes a declared
  graph in the calling JVM. See
  [`local/`](flezha-sdk-examples/src/main/java/com/flezha/examples/local).
- **Threading model, packaging and cross-SDK comparison:** `docs/sdk/java_sdk.md`.
- **Wire protocol and framing:** `docs/sdk/protocol_spec.md`.

## Capabilities

| Capability | Declare | Gives you |
|---|---|---|
| Durable state | `Change` type + `state.apply(...)` | State survives restart, rebuilt from a change log. |
| Effectively-once output | `c.effectivelyOnce()` + `.keyed()` channels | State, output and consumed offset become durable in ONE record. A crash cannot advance the offset while losing the output. |
| Checkpointed intervals | `c.checkpointAck()` + `ctx.checkpoint()` | The offset is withheld until the state is coherent, so a half-built window is never persisted. |
| Late-input correction | `c.correctionWindow(ms)` | A late input rewinds the Entity and replays the slice in event-time order, repairing the earlier result. |
| Dead-letter queue | `.retryMax(n)` on the channel | A poison payload is retried, then parked durably, never silently dropped. |
| Edge transforms | `.map(T.class, f)` / `.filter(p)` / `.flatMap(T.class, f)` on the wiring | Reshape, drop or fan out values without a Procedure, with no entity, no mailbox and no dispatch hop. |
| Dynamic entity mode | `.keyBy(f)` on the wiring | One Entity per key the stream actually carries, created on first sight, and visible where the graph is declared. |
| Operative-wide document | `c.routeLatest(Doc.class, documents, traffic, keyFn)` | Every key seen on the traffic stream receives each distinct document exactly once, on that key's own next traffic item, the one delivery a keyed procedure cannot get from a broadcast. One tick of latency per key is the price of only ever emitting keys this member owns. |
| Workflow transaction | `implements WorkflowOperative` + `procedures(...)` | Resumable chain: a committed step is skipped on resume, not re-run. Failure unwinds via `compensate`. |
| Durable loop / suspension | `loopMode()` + `wakeAfter()` | One durable iteration per message; parks when idle, holding its journal. |
| Entity retry | `retryAttempts()` / `retryBackoff()` | Only the failing entity re-runs; a failed attempt's output is dropped, not double-emitted. |

Each is a declaration the controller must honour, so declare what you need rather
than what sounds reassuring: a `correctionWindow` sizes the controller's input-log
retention, and a tier without keyed sinks is refused at registration.

## Modules

| Module | Purpose |
|---|---|
| `flezha-sdk-api` | Public API: `Flezha`, `StreamOperative`, `WorkflowOperative`, `Procedure`, `ProcedureCtx`, `StableId`, channel builders, clocks. No native dependency. |
| `flezha-sdk-runtime` | Control plane, data plane, framing, worker pool, reconnect, state persistence, workflow driver. |
| `flezha-sdk-native` | JNI bindings + bundled per-OS `libflz_core_ffi`. |
| `flezha-sdk-testing` | `ProcedureTestHarness`, added with `<scope>test</scope>`. |
| `flezha-sdk-examples` | Worked examples. Compiled by the build, so they cannot rot. |
| `flezha-sdk` | Fat-jar aggregator, the published artifact. |

## Build

```bash
mvn -f sdk/java/pom.xml -P native-build package   # portable: bundles all 5 native targets
mvn -f sdk/java/pom.xml -P host-only  package     # fast local iteration (NOT portable)
```

Prerequisites: **JDK 21+**, a **Rust stable toolchain** (for the `flz_core_ffi` binding),
and **[cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild)** for the
native cross-build.

## Test

```bash
mvn -f sdk/java/pom.xml test                    # unit suite, no controller needed
mvn -f sdk/java/pom.xml -P coverage verify      # + JaCoCo
FLEZHA_E2E_RUNTIME=java mix test --include system   # from repo root: the shared conformance suite
```

## Environment

| Variable | Default | Notes |
|---|---|---|
| `FLEZHA_TRANSPORT` | `uds` | `uds` or `quic`. |
| `FLEZHA_QUIC_ADDRESSES` | `127.0.0.1:4433` | QUIC only. Comma list of names or literals, re-resolved every attempt. |
| `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` | QUIC only. |
| `FLEZHA_SERVER_CERT_PATH` | `app_data/certs/cert.pem` | Server mode only. |
| `FLEZHA_SERVER_KEY_PATH` | `app_data/certs/key.pem` | Server mode only. |
| `FLEZHA_SOCKET_FILES_PATH` | resolved by the socket ladder | UDS only. Set, it wins outright; unset, the ladder walks `$XDG_RUNTIME_DIR/flezha`, `/run/flezha`, `app_data/sockets`. |
| `FLEZHA_RUNNER_ID_PATH` | `/var/lib/flezha-runner/id` | Durable stable identity. Unwritable means a fresh id per boot and lost bindings. |
| `FLEZHA_WORKER_POOL_SIZE` | `availableProcessors()` | Shared runner draining every Entity mailbox. |
| `FLEZHA_NATIVE_LIB_DIR` | `$TMPDIR` | Where `libflz_core_ffi` is extracted. Set it on containers where `/tmp` is `noexec`. |
| `FLEZHA_WORKFLOW_PARK_IDLE_MS` | `5000` | Idle hysteresis before a looping workflow operative parks. |

## Further reading

- `docs/sdk/protocol_spec.md`: wire protocol, framing, output identity stamps.
- `docs/resilience/data_durability.md`: where state and output are lost vs not.
- `docs/sdk/state_persistence.md`: Kryo registration and the Change machinery.

## License

Apache-2.0, [`LICENSES/Apache-2.0.txt`](../../LICENSES/Apache-2.0.txt). The
per-path map for the whole repository is [`REUSE.toml`](../../REUSE.toml).
