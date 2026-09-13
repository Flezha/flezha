# Echo: one message in, one message out

The Java hello world, as a standalone project. Nothing here depends on the
Flezha source tree: the `pom.xml` names one released coordinate, the way your
own project would. Open this folder in an IDE and it resolves.

Three files, and every Flezha application has these same three parts:

| File | Part | What it does |
|---|---|---|
| `EchoProcedure.java` | the procedure | your code: one message in, one message out |
| `EchoOperative.java` | the operative | the wiring: `in` feeds the procedure, output lands on `out` |
| `Main.java` | the runner | this process: offers the operative and waits |

## Run it

You need a controller running and reachable first
([`docs/guides/controller_deployment.md`](../../../../docs/guides/controller_deployment.md)).

```sh
mvn exec:java
```

Then, from another terminal:

```sh
flz operative start my_echo echo
flz operative send my_echo --text hello --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello
```

`mvn exec:java` blocks until shutdown, which is a SIGTERM or a `close()` from
another thread. The namespace is the first argument and defaults to `default`:
`mvn exec:java -Dexec.args=my_app`.

## Building from a checkout

The SDK is not on Maven Central yet, so `com.flezha:flezha-sdk:0.1.0` has to be
in your local Maven repository before this resolves. From the repository root:

```sh
scripts/build-native.sh --output sdk/java/flezha-sdk-native/src/main/resources/native
mvn -f sdk/java/pom.xml -DskipTests install
```

That is a one-time step. Once the SDK is published, it goes away and `mvn
exec:java` is all you need.

## Where to go next

[`docs/guides/runner_deployment.md`](../../../../docs/guides/runner_deployment.md)
walks through what each of the three parts is and why the split is the way it
is. The other worked examples, covering state, keys, timers, workflows and the
rest, stay inside the SDK's own build at
[`sdk/java/flezha-sdk-examples/`](../../flezha-sdk-examples/README.md).
