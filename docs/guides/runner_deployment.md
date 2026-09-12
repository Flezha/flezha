# Deploy a runner

**Before you start:** finish [`controller_deployment.md`](controller_deployment.md).
A runner is useless on its own: it connects to a controller, so you need one
running and reachable before anything here works.

TODO: this is a draft of the Java tab only, for review. The section list is
provisional and the other six SDKs are not written yet.

## The hello world

Every SDK ships the same example, so you can read it in one language and
recognise it in another. It is three parts, and every Flezha application has
these same three, whatever it grows into later.

**The procedure is your code.** The runtime hands it one message and it emits
one message. It carries a stable name, and that name is durable: per-entity
state is filed under it, so renaming a procedure later does not rename its
state, it declares a different procedure with none. Two things it does not do
are worth noticing, because they are what makes the rest possible. It does not
say where its output goes, so the graph can be rewired without touching it. And
it does not lock anything, because the runtime never runs two messages for the
same key at the same time. Different keys do run at the same time, possibly on
different machines.

**The operative is the graph, and it holds no logic at all.** It says which
channel feeds which procedure and where the result is delivered. A procedure on
its own cannot be deployed, because nothing knows where its messages come from
or where its output belongs; the operative supplies exactly that. It is
therefore the unit the whole system works in: what the controller places on a
runner, restarts after a crash, moves when a runner disappears, and splits
across several runners when one is not enough. It carries its own durable
identity, and that is the name you type when you start one.

**The runner is a process you run.** It connects to the controller, tells it
which operatives it is able to serve, and then waits. It decides nothing: it
never picks what runs or where, it only offers capacity and does what the
controller asks. One runner can serve many operatives, and many runners can
offer the same one, which is how a deployment grows.

None of the three knows about the others at runtime. You start a runner, the
controller learns the operative exists and can be served, you ask for one by
name, and the controller binds it to a runner that offered it. Messages then
arrive on the channel the operative declared, and your procedure is called.

<!-- tabs-open -->

### Java

The whole example:
[`flezha-sdk-examples/.../examples/echo`](https://github.com/alfetahe/flezha/tree/main/sdk/java/flezha-sdk-examples/src/main/java/com/flezha/examples/echo).

**The procedure.** Your code. It is handed one message and emits one message.

```java
public final class EchoProcedure
        implements Procedure<String, EchoProcedure.None, String, NoChange, WallClock, Void> {

    public record None() {}

    @Override
    public String name() {
        return "echo";
    }

    @Override
    public None initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new None();
    }

    @Override
    public void run(String input, StateHandle<None, NoChange> state, ProcedureCtx ctx) {
        ctx.emit("echo: " + input);
    }
}
```

**The six type arguments** are, in order: the input, the per-entity state, the
output, the change type, the clock, and the configuration. The runtime reads
all six back off the `implements` clause at registration, which is why each one
has to be a concrete class there rather than a variable.

Two of the six are opted out of. `NoChange` declares that this procedure records
no state changes, which switches the persistence machinery off entirely: nothing
is journaled, no snapshot is written, and a restart begins with no history.
`Void` declares that it takes no configuration. A procedure that needs to
remember something replaces both, and that is the next example rather than this
one.

**`name()` is durable, not cosmetic.** Per-entity state is filed under it. Change
it in a later release and you have not renamed this procedure, you have declared
a different one, and everything the old name accumulated is no longer associated
with it. Choose it deliberately the first time.

**`initState` builds the state for one entity,** called the first time a key is
seen. Its two arguments are that key, and a payload present only when the entity
is arriving from another runner rather than starting cold. On a resume the
runtime restores the newest snapshot and folds the recorded changes over
whatever this returns, so it only has to be right for the cold case. This
procedure keeps nothing, so it returns an empty record.

**`run` is the whole of the work.** `ctx` is the runtime handle: `emit` releases
output, `now()` reads the declared clock, and `stableId()` gives an identifier
that is the same again on a replay. Note what is missing: no destination, no
channel name, no sink. `emit` says what, never where.

**The operative.** The graph, and the thing the cluster actually schedules.

```java
public final class EchoOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "echo";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "in")
            .into(EchoProcedure.class)
            .sink("out");
    }
}
```

**One expression declares the whole application.** An entry channel called `in`
carrying strings, routed into the procedure, whose output is delivered to a
plugin sink called `out`. Read it left to right and you have the entire data
path.

**Channels are never constructed directly.** Each call on the builder returns
the next handle in the chain, and the finished declaration is validated as a
whole before a single message is processed. A graph that cannot work is refused
at startup rather than halfway through a run.

**The names are identities, not labels.** `in` is what `flz operative send
--channel` addresses, and `out` is what a sink registers against. Changing
either changes an address that something outside this file is already using.

**`factoryId()` is durable in the same way `name()` is.** The controller's
registry and the durable state both key on it. It is also the second argument
to `flz operative start <your-id> <factory-id>`, which is how you ask for one of
these to be running.

**`channels` runs once, at registration,** not per message. It describes the
graph; it never touches data.

**The runner.** A process that connects and serves.

```java
public static void main(String[] args) throws Exception {
    Flezha.createRunner(args.length > 0 ? args[0] : "my_app")
            .defineStream(EchoOperative.class)
            .run();
}
```

**`createRunner` takes the namespace,** which is the tenant this process belongs
to. The controller admits it only if that namespace exists, and, with
authorization enforced, only if the runner can prove it is entitled to it.

**`defineStream` declares a class this process can serve.** Call it once per
operative; one runner can offer as many as you like, and `defineWorkflow` is the
same call for the workflow kind. Declaring a class does not start anything: it
tells the controller this runner is capable of it, and the controller decides
whether and where to use that.

**`run()` connects and then blocks** until shutdown, which is either a SIGTERM
or a `close()` from another thread. Use `start()` instead when the calling
thread has something else to do. Either way the connection is the runner's own
responsibility to keep. If it breaks, the runner re-dials by itself, retrying
forever on a backoff that tops out at 30 seconds, and it keeps its operatives
and their state across the gap rather than starting over.

### Rust

TODO.

### Node

TODO.

### Python

TODO.

### Ruby

TODO.

### PHP

TODO.

### Elixir

TODO.

<!-- tabs-close -->

## Get it

TODO.

## Start it

TODO.

## Check it

TODO.

## Stop it

TODO.

## Next

TODO.
