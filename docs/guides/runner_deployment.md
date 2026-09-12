# Deploy a runner

**Before you start:** finish [`controller_deployment.md`](controller_deployment.md).
A runner is useless on its own: it connects to a controller, so you need one
running and reachable before anything here works.

TODO: this is a draft of the Java tab only, for review. The section list is
provisional and the other six SDKs are not written yet.

## The hello world

Every SDK ships the same example, so you can read it in one language and
recognise it in another. It is three parts, and every Flezha application has
these same three:

| Part | What it is |
|---|---|
| The procedure | your code, invoked once per message |
| The operative | the graph that wires the procedure to an input and an output. This is the unit the controller schedules, restarts and moves |
| The runner | a process that connects to the controller and serves the operative |

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

The six type arguments are, in order: the input, the per-entity state, the
output, the change type, the clock, and the configuration. Two are opted out of
here. `NoChange` says this procedure persists nothing, which switches the whole
persistence machinery off, and `Void` says it takes no configuration.

`name()` is durable. Durable state is keyed on it, so changing it later does not
rename this procedure, it declares a different one.

`ctx.emit` does not say where the value goes. The operative decides that, which
is why the graph can be rewired without touching this file.

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

One expression declares the whole application: an entry channel carrying
strings, routed into the procedure, whose output is delivered to a plugin sink
called `out`. Those names are identities rather than documentation. `out` is
what a sink registers against, and `in` is what `flz operative send --channel`
names.

`factoryId()` is durable in the same way `name()` is. It is what you pass to
`flz operative start`.

**The runner.** A process that connects and serves.

```java
public static void main(String[] args) throws Exception {
    Flezha.createRunner(args.length > 0 ? args[0] : "my_app")
            .defineStream(EchoOperative.class)
            .run();
}
```

`createRunner` takes the namespace. Declare every operative this process should
serve before calling `run()`, which connects and then blocks until shutdown.
Use `start()` instead when you want to keep the calling thread.

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
