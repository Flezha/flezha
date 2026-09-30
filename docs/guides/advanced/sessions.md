# Sessions

On this page agentdesk runs for the first time: a controller and a runner in Docker
containers, and one session per conversation. You send a session messages from the CLI,
leave it idle until it sleeps, and wake it again.

A conversation has a beginning, it has turns, and it remembers what was said. A bounded
operator gives it all three, the kind [`building_a_workflow.md`](../tradedesk/building_a_workflow.md)
introduced: each conversation is an instance of its own, with its own state. Where an order
ran its steps once, a session runs them once per message, for as long as it lasts. Each run
is a **turn**.

## Answer a message

`reply` answers a message with the demo model, and keeps the conversation in its state:
the list of messages so far, one more each turn. The model sees the conversation, so its
answer can say which message this is.

> Start a new runner project, copy `DemoModel` into it from the example, and write the
> tasklet that answers.
> {: .step}

<!-- tabs-open -->

### Java

The whole example:
[`sdk/java/examples/agentdesk`](https://github.com/flezha/flezha_java_sdk/tree/main/examples/agentdesk).

```java
// Reply.java: answers a message with the demo model, and keeps the conversation.
@Tasklet.Spec(name = "reply")
public final class Reply implements Tasklet<String, Reply.Conversation, Reply.Answer> {

    public record Answer(String text, int tokens) {}

    public static class Conversation {                       // what the session remembers
        public List<String> messages = new ArrayList<>();
    }

    public record Said(String message) implements Change<Conversation> {
        @Override
        public void applyTo(Conversation conversation) {
            conversation.messages.add(message);
        }
    }

    @Override
    public void run(String message, Ctx<Conversation, Answer> ctx) throws TaskletException {
        ctx.emit(DemoModel.answer(ctx.state().messages, message, part -> {}));
        ctx.change(new Said(message));
    }
}
```

An answer is its text and the number of tokens the model used for it. `DemoModel` stands in
for a model's client: it answers every message with one canned sentence, so nothing outside
the example is needed. Its third argument receives each token as it is written, which
[`streamed_answers.md`](streamed_answers.md) uses; for now the tokens are dropped.

<!-- tabs-close -->

## A session is a bounded operator

The operator is called `agent`, and each session is an instance of it, named after its
user. A message arrives on the entry channel `turn`, `reply` answers it, and the
`response()` end sends the answer back to whoever sent the message.

> Write the session's operator.
> {: .step}

<!-- tabs-open -->

### Java

```java
// AgentOperator.java: one conversation, one turn per message.
public final class AgentOperator implements BoundedOperator {

    @Override
    public String factoryId() {
        return "agent";
    }

    @Override
    public LoopMode loopMode() {
        return LoopMode.PER_INPUT;                       // one turn per message
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "turn")
                .into(Reply.class)
                .response();                             // to whoever called
    }
}
```

`LoopMode.PER_INPUT` runs the chain once per message, instead of once in all as an order
did.

<!-- tabs-close -->

## The runner

The runner joins the namespace `agents` and registers the `agent` factory, as
[`namespaces.md`](../tradedesk/namespaces.md) showed for the tradedesk.

> Write the runner and its image.
> {: .step}

<!-- tabs-open -->

### Java

```java
// Main.java: the runner.
public final class Main {

    public static void main(String[] args) throws Exception {
        FlezhaRunner runner = Flezha.createRunner("agents")
                .registerFactory(AgentOperator.class)
                .start();

        runner.awaitTermination();
    }

    private Main() {}
}
```

```dockerfile
# The runner's image: the jar `mvn -q package` builds, on a Java 21 runtime.
FROM eclipse-temurin:21-jre
COPY target/agentdesk.jar /agentdesk/agentdesk.jar
USER 1000
ENTRYPOINT ["java", "-jar", "/agentdesk/agentdesk.jar"]
```

<!-- tabs-close -->

## The containers

The controller's image is Flezha's own, which
[`controller_deployment.md`](../controller_deployment.md#docker) runs, plus the plugin
packages in the project's `controller/plugins/` folder, empty until
[`custom_plugins.md`](custom_plugins.md) puts one there.

> Copy `controller/Dockerfile` and `compose.yaml` from the example.
> {: .step}

```dockerfile
# The controller's image: Flezha's own, with the plugin packages in plugins/.
FROM flezha/flezha:0.1.0
COPY --chown=flezha:flezha plugins/ /flezha/plugins/
ENV FLEZHA_PLUGIN_ROOT=/flezha/plugins
```

```yaml
# agentdesk in Docker Compose: one controller and one runner.
services:
  controller:
    build: controller
    environment:
      FLEZHA_AUTHZ_ENABLED: "false"
      FLEZHA_DURABILITY_BACKEND: none
    ports:
      - "127.0.0.1:8080:8080"              # the gateway's HTTP port, on this machine only
    volumes:
      - app_data:/flezha/app_data
      - sockets:/flezha/app_data/sockets

  runner:
    build: .
    environment:
      FLEZHA_SOCKET_FILES_PATH: /flezha/app_data/sockets
      FLEZHA_PARK_IDLE_MS: "10000"         # an idle session sleeps after 10 s
    volumes:
      - sockets:/flezha/app_data/sockets
    depends_on:
      controller:
        condition: service_healthy
    restart: on-failure

volumes:
  app_data:
  sockets:
```

- **`app_data`** holds everything the controller keeps, the sessions included, so a new
  controller container finds it.
- **`sockets`** is shared: the runner reaches the controller through the Unix sockets in it.
- **Authorization is off**, to keep the setup short;
  [`authorization.md`](../tradedesk/authorization.md) shows how to keep it on.
- **`FLEZHA_DURABILITY_BACKEND: none`** keeps the state on the controller's volume only.
  The image refuses to start without the setting.
- **Port 8080** is for [`http_gateway.md`](http_gateway.md); nothing listens on it until then.

## Start it

> Build the runner, then start both containers, from the project's folder.
> {: .step}

<!-- tabs-open -->

### Java

```sh
$ mvn -q package
```

<!-- tabs-close -->

```sh
$ docker compose up -d --build --wait
```

`--wait` returns once the controller's healthcheck says it is ready.

> Make `flz` the CLI inside the controller's container, for this shell, and ask the
> controller for the runner.
> {: .step}

```sh
$ alias flz='docker compose exec controller bin/flz'
$ flz runner list --namespace agents
```

```output
Fetching runners for namespace 'agents'...
Runners in namespace 'agents':
  - agents_33173e7dbe13
```

Every `flz` command on the Part 3 pages runs this way, from the project's folder.

## Talk to a session

> Start a session for Anna, and send it two messages.
> {: .step}

```sh
$ flz operator start agent --id anna --namespace agents
```

```output
Started anna
```

```sh
$ flz operator call anna --namespace agents --text 'Hello'
$ flz operator call anna --namespace agents --text 'What can you do?'
```

```output
{"text":"Message 1: you said \"Hello\". I am a demo model.","tokens":10}
{"text":"Message 2: you said \"What can you do?\". I am a demo model.","tokens":13}
```

`flz operator call` sends one message and waits for the answer the `response()` end
produces from it; [`calls.md`](../../sdk/calls.md) is its contract. The session remembered
the first message.

> Start a session for Bob, and send it a message.
> {: .step}

```sh
$ flz operator start agent --id bob --namespace agents
$ flz operator call bob --namespace agents --text 'Hi, I am Bob'
```

```output
Started bob
{"text":"Message 1: you said \"Hi, I am Bob\". I am a demo model.","tokens":13}
```

Bob's session has a conversation of its own.

## An idle session sleeps

A session waiting for its next message still holds a place on its runner, and with many
sessions that adds up. So a bounded operator that keeps its state sleeps when it is idle:
it leaves its runner, and its next message wakes it on whichever runner is there. `agent`
declares nothing for this. How long "idle" is comes from `FLEZHA_PARK_IDLE_MS` on the
runner, ten minutes by default and ten seconds in `compose.yaml`.

> Leave Anna alone for ten seconds, then ask the controller about her session.
> {: .step}

```sh
$ flz operator info anna --namespace agents
```

```output
...
Operator: anna
  Kind: bounded
  Phase: suspended
  Health: suspended
  Runner ID: 
...
```

`suspended` is asleep: the controller keeps the session, and no runner holds it.

> Restart the runner, so the one that wakes Anna is not the one she fell asleep on, and
> send her a message.
> {: .step}

```sh
$ docker compose restart runner
$ flz operator call anna --namespace agents --text 'Are you still there?'
```

```output
{"text":"Message 3: you said \"Are you still there?\". I am a demo model.","tokens":13}
```

The message woke the session on the new runner, with its conversation. The time a wake
takes counts against the call's deadline. Leave both sessions running: the next pages use
them.

## Next

[`rewinding_a_turn.md`](rewinding_a_turn.md) takes turns back, and lets a turn fail without
ending the session.
