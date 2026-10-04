# The java SDK's echo example

Every file of `sdk/java/examples/echo`, the runner the echo tutorial runs. Its dependency file names the SDK package and the version this release pins.

## examples/echo/README.md

````markdown
# Echo: the Java hello world

The Java hello world, as a standalone project. Nothing here depends on the
Flezha source tree: the `pom.xml` names one released coordinate, the way your
own project would. Open this folder in an IDE and it resolves.

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `Echo.java` | the tasklet | the processing stage: prefixes each message |
| `EchoOperator.java` | the operator | the graph: `in` feeds the tasklet, output lands on `out` |
| `Main.java` | the runner | this process: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([`docs/guides/controller_deployment.md`](../../../../docs/guides/controller_deployment.md)).

```sh
mvn -q package && java -jar target/echo.jar
```

Then, from another terminal:

```sh
flz operator start echo --id my_echo
flz operator send my_echo --text 'hello world' --await 2000
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

The SDK is not on Maven Central yet, so `com.flezha:flezha-sdk:0.1.0` has to be
in your local Maven repository before this resolves. From the repository root:

```sh
scripts/build-native.sh --output sdk/java/flezha-sdk-native/src/main/resources/native
mvn -f sdk/java/pom.xml -DskipTests install
```

That is a one-time step. Once the SDK is published, it goes away and the command
above is all you need.

## Where to go next

[`docs/guides/runner_deployment.md`](../../../../docs/guides/runner_deployment.md)
walks through what each of the three parts is and why the split is the way it
is. The other worked examples, covering state, keys, timers, steps operators and the
rest, stay inside the SDK's own build at
[`sdk/java/flezha-sdk-examples/`](../../flezha-sdk-examples/README.md).
````

## examples/echo/pom.xml

````xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <!-- Standalone on purpose: no parent, and one dependency on a released
         coordinate, so this builds the way your own project would rather than
         the way a module inside the SDK's reactor does. Open this file in an
         IDE and it resolves. -->
    <groupId>com.flezha.examples</groupId>
    <artifactId>echo</artifactId>
    <version>0.1.0</version>
    <packaging>jar</packaging>

    <name>Flezha echo example</name>
    <description>One message in, one message out: a tasklet, an operator and a runner.</description>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.release>21</maven.compiler.release>
        <flezha.sdk.version>0.1.0</flezha.sdk.version>
    </properties>

    <dependencies>
        <!-- The aggregate jar: the authoring surface, the runtime and the
             native binaries for every supported platform, in one coordinate. -->
        <dependency>
            <groupId>com.flezha</groupId>
            <artifactId>flezha-sdk</artifactId>
            <version>${flezha.sdk.version}</version>
        </dependency>
        <dependency>
            <groupId>com.google.code.findbugs</groupId>
            <artifactId>jsr305</artifactId>
            <version>3.0.2</version>
        </dependency>
    </dependencies>

    <build>
        <finalName>echo</finalName>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.13.0</version>
            </plugin>
            <!-- One runnable jar with the SDK inside: java -jar target/echo.jar -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.6.0</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <createDependencyReducedPom>false</createDependencyReducedPom>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.flezha.examples.echo.Main</mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
````

## examples/echo/src/main/java/com/flezha/examples/echo/Echo.java

````java
package com.flezha.examples.echo;

import com.flezha.sdk.tasklet.Ctx;
import com.flezha.sdk.tasklet.Tasklet;
import com.flezha.sdk.tasklet.TaskletException;

/**
 * A processing stage of the operator's graph, invoked once per message. This one prefixes the
 * input and emits the result.
 *
 * <p>{@code Tasklet<String, Void, String>} reads input, state, output: a string in, no state, a
 * string out. With {@code Void} state nothing is kept between messages and nothing is journaled.
 *
 * <p>{@code name} is the tasklet's stable identity. State is filed under it, so changing it later
 * declares a different tasklet rather than renaming this one.
 *
 * <p>Where the result goes is not decided here. The operator decides that, so the graph can be
 * rewired without touching this class.
 */
@Tasklet.Spec(name = "echo")                 // state is saved under this name, so choose it once
public final class Echo implements Tasklet<String, Void, String> {

    @Override
    public void run(String input, Ctx<Void, String> ctx) throws TaskletException {
        ctx.emit("echo: " + input);          // where it goes is the operator's business
    }
}
````

## examples/echo/src/main/java/com/flezha/examples/echo/EchoOperator.java

````java
package com.flezha.examples.echo;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operator.Operator;
import javax.annotation.Nonnull;

/**
 * The deployable unit. An operator declares the graph a message travels: the entry channel it
 * arrives on, the steps it passes through, and the channel its result leaves on.
 *
 * <p>The operator is what the controller schedules onto a host, restarts after a failure,
 * relocates when a host is withdrawn, and scales across several hosts. A tasklet is one stage
 * inside that graph.
 */
public final class EchoOperator implements Operator {

    /**
     * The operator's stable identity, durable in the same way the tasklet's name is. The
     * controller's registry and the durable state both key on this value, so changing it declares a
     * new operator rather than renaming an existing one.
     */
    @Override
    @Nonnull
    public String factoryId() {
        return "echo";                       // the name `flz operator start` takes
    }

    /**
     * Invoked once at registration to declare the graph. Channels are never constructed directly;
     * each call on the builder returns the next handle in the chain, and the completed declaration
     * is validated as a whole before any message is processed.
     *
     * <p>The expression below declares an entry channel named {@code in} carrying strings, routes
     * it to the echo tasklet, and terminates that tasklet's output on the plugin layer under
     * the name {@code out}. The name a sink carries is the channel's own: an identity rather than
     * documentation, and what a plugin filter matches or a test reads.
     */
    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.input(String.class, "in")          // messages arrive on the entry channel "in"
                .into(Echo.class)            // each one runs through the echo tasklet
                .sink("out");                // the result leaves on "out", for a plugin to pick up
    }
}
````

## examples/echo/src/main/java/com/flezha/examples/echo/Main.java

````java
package com.flezha.examples.echo;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.FlezhaRunner;
import com.flezha.sdk.SendOpts;
import com.flezha.sdk.control.Control;
import com.flezha.sdk.control.OperatorSpec;
import com.flezha.sdk.control.PluginSpec;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Starts a runner: a process that connects to the controller and serves your operators.
 *
 * <p>Declare every operator you want served before calling {@code run()}. One runner can serve
 * several, and {@code registerFactory(...)} is the same call for the steps run.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        // run() is start() followed by awaitTermination(). Splitting them keeps the handle,
        // which is the only way to reach control() and send(), and still blocks until shutdown.
        FlezhaRunner runner = Flezha.createRunner(args.length > 0 ? args[0] : "default")
                .registerFactory(EchoOperator.class)     // the operators this runner serves
                .start();                                // connects to the controller

        // Everything `flz operator` and `flz plugin` do is also a method of control(), and
        // send() hands a message to an operator. Uncomment to drive this namespace from the
        // application instead of from a terminal:
        //
        // Control control = runner.control();
        // control.startOperator(OperatorSpec.of("echo").id("my_echo"));
        //
        // ObjectNode opts = JsonNodeFactory.instance.objectNode().put("path", "echo-out.txt");
        // control.enablePlugin("writer", PluginSpec.of("operator_file_sink").opts(opts));
        // ArrayNode hooks = JsonNodeFactory.instance.arrayNode();
        // hooks.addArray().add("on_output").addObject();
        // control.bindPlugin("writer", "my_echo", hooks, null);
        //
        // runner.send("my_echo", "hello world", "in", SendOpts.of().msgId("hello-1"));
        //
        // docs/topics/advanced/sending_from_runner_code.md covers sending. An enforced controller
        // refuses all of it until `flz role bind runner@default app default`.

        runner.awaitTermination();
    }

    private Main() {}
}
````

## examples/echo/src/main/java/com/flezha/examples/echo/package-info.java

````java
/**
 * The minimal complete application: an operator that declares the graph, a stateless tasklet it
 * routes each message into, and a main that serves it.
 *
 * <p>This example deliberately introduces nothing else. A line of text arrives on an entry channel,
 * the tasklet prefixes it, and the result is delivered to a plugin sink.
 *
 * <p>{@link com.flezha.examples.echo.EchoOperator} declares the graph and is the unit the cluster
 * actually schedules. {@link com.flezha.examples.echo.Echo} is the processing stage invoked once per
 * message, and the class the operator routes into. {@link com.flezha.examples.echo.Main} starts a
 * runner and declares the operator on it. Read the operator first: its single wiring expression is
 * the shortest complete description of what this application does.
 *
 * <p>The tasklet declares {@code Void} state, so it persists nothing. Nothing is
 * journaled, no snapshot is written, and a restart begins with no history. The {@code state}
 * example is the next step, and covers what changes when that is no longer acceptable.
 */
package com.flezha.examples.echo;
````
