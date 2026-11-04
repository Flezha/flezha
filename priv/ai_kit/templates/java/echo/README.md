# Echo: the Java hello world

The Java hello world, as a standalone project. Nothing here depends on the
Flezha source tree: the `pom.xml` names one released coordinate, the way your
own project would. Open this folder in an IDE and it resolves.

Three files, one for each of the [building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md):

| File | Part | What it does |
|---|---|---|
| `Echo.java` | the tasklet | the processing stage: prefixes each message |
| `EchoRelay.java` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `Main.java` | the runner | this process: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([Up and running](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md)).

```sh
mvn -q package && java -jar target/echo.jar
```

Then, from another terminal:

```sh
flz relay start echo --id my_echo
flz relay send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

The jar carries the SDK, so it runs with nothing else on the classpath. It blocks
until shutdown, which is a SIGTERM or a `close()` from another thread. The
namespace is the first argument and defaults to `default`:
`java -jar target/echo.jar my_app`.

## Building from a checkout

To run the example against a Flezha checkout instead of the published SDK, install
the checkout's `com.flezha:flezha-sdk:0.1.0` into your local Maven repository, which
Maven reads before Maven Central. From the repository root:

```sh
scripts/build-native.sh --output sdk/java/flezha-sdk-native/src/main/resources/native
mvn -f sdk/java/pom.xml -DskipTests install
```

## Where to go next

[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what each of the three parts is and why the split is the way it
is.
