# An agent session

On this page agentdesk runs for the first time: a controller and a runner, each in a
Docker container, and one session per conversation. You start a session for a user,
send it a message from the CLI, and get its answer back.

## A session is a bounded operator

A conversation has a beginning, it has turns, and it needs to remember what was said.
A bounded operator gives it all three, the kind
[`building_a_workflow.md`](../tradedesk/building_a_workflow.md) introduced: every
conversation is an instance of its own, started when the conversation starts, with its
own state. Where the order workflow ran its steps once, a session runs them once per
message it is sent, for as long as the session lasts. Each run is a **turn**.

The operator is called `agent`, and each session is an instance of it, named after its
user.

> Write the session's operator. It goes into a new runner project; the finished project
> is linked at the top of your language's tab.
> {: .step}

<!-- tabs-open -->

### Java

The whole example:
[`sdk/java/examples/agentdesk`](https://github.com/flezha/flezha_java_sdk/tree/main/examples/agentdesk).

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
    public RollbackMode rollbackMode() {
        return RollbackMode.KEEP_PROGRESS;               // a failed turn fails alone
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "turn")
                .into(Reply.class)
                .sink("answer")                          // to whoever listens
                .into(Deliver.class)
                .response();                             // to whoever called
        c.input(Message.class, "message")                // from the gateway
                .map(String.class, Message::text)
                .into(Reply.class);
    }
}
```

`LoopMode.PER_INPUT` runs the chain once per message instead of once in all.
`RollbackMode.KEEP_PROGRESS` is what a failing turn needs, and
[`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails) shows it.

<!-- tabs-close -->

A message arrives on the entry channel `turn`, and `reply` answers it. The answer
leaves twice. The sink `answer` sends it to whoever listens to the session, which
[`streamed_answers.md`](streamed_answers.md) uses. The `response()` end sends it back to
whoever sent the message and is waiting for the answer. A channel ends in one sink or
one `response()`, never both, so `deliver` passes the answer on to a channel of its
own. The second entry channel, `message`, is for the gateway of
[`http_gateway.md`](http_gateway.md#a-second-way-into-a-session).

## Answer a message

`reply` answers a message with the demo model and keeps the conversation: its state is
the list of messages so far, and every turn adds one. The model sees the conversation,
so the answer can say which message of the conversation this is.

> Write the tasklet that answers, and the one that passes the answer on. Copy the demo
> model, `DemoModel`, from the example: [`streamed_answers.md`](streamed_answers.md)
> goes through it.
> {: .step}

<!-- tabs-open -->

### Java

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

    public record Unsaid() implements Change<Conversation> {
        @Override
        public void applyTo(Conversation conversation) {
            conversation.messages.removeLast();
        }
    }

    @Override
    public void run(String message, Ctx<Conversation, Answer> ctx) throws TaskletException {
        ctx.emit(DemoModel.answer(ctx.state().messages, message, ctx::part));
        ctx.change(new Said(message));
    }

    // The undo, when a turn is taken back: the conversation forgets its message.
    @Override
    public void compensate(List<Answer> sent, Ctx<Conversation, Answer> ctx) {
        System.out.println("[undo] forgot \"" + ctx.state().messages.getLast() + "\"");
        ctx.change(new Unsaid());
    }
}
```

An answer is its text and the number of tokens the model used for it. `ctx::part` hands
the model a way to send the answer while it is still being written, which
[`streamed_answers.md`](streamed_answers.md) explains, and `compensate` is the undo that
[`rewinding_a_turn.md`](rewinding_a_turn.md) uses.

```java
// Deliver.java: hands the finished answer to the caller.
@Tasklet.Spec(name = "deliver")
public final class Deliver implements Tasklet<Answer, Void, Answer> {

    @Override
    public void run(Answer answer, Ctx<Void, Answer> ctx) throws TaskletException {
        ctx.emit(answer);
    }

    // The undo, when a turn is taken back: an answer already given has nothing to take back.
    @Override
    public void compensate(List<Answer> sent, Ctx<Void, Answer> ctx) {}
}
```

<!-- tabs-close -->

## The runner

The runner joins the namespace `agents` and registers the `agent` factory, as
[`namespaces.md`](../tradedesk/namespaces.md) showed for the tradedesk. The `gateway`
factory is [`http_gateway.md`](http_gateway.md)'s.

<!-- tabs-open -->

### Java

```java
// Main.java: the runner.
public final class Main {

    public static void main(String[] args) throws Exception {
        FlezhaRunner runner = Flezha.createRunner("agents")
                .registerFactory(AgentOperator.class)
                .registerFactory(GatewayOperator.class)
                .start();

        runner.awaitTermination();
    }

    private Main() {}
}
```

The runner's image is the project's jar on a Java runtime:

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
[`controller_deployment.md`](../controller_deployment.md#docker) runs, with one addition:
the plugin packages in the project's `controller/plugins/` folder, which stays empty
until [`custom_plugins.md`](custom_plugins.md) puts one there.

```dockerfile
# The controller's image: Flezha's own, with the plugin packages in plugins/.
FROM flezha/flezha:0.1.0
COPY --chown=flezha:flezha plugins/ /flezha/plugins/
ENV FLEZHA_PLUGIN_ROOT=/flezha/plugins
```

`compose.yaml` starts one of each:

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

- **`app_data`** holds everything the controller keeps, the sessions' state included, so
  a new controller container finds it.
- **`sockets`** is shared: the runner reaches the controller through the Unix sockets in
  it, as a runner on the same machine would.
- **Authorization is off**, to keep the setup short.
  [`authorization.md`](../tradedesk/authorization.md) shows how to keep it on.
- **`FLEZHA_DURABILITY_BACKEND: none`** keeps the state on the controller's own volume
  and nowhere else. The image refuses to start without the setting.
- **`FLEZHA_PARK_IDLE_MS`** is for [`sleeping_sessions.md`](sleeping_sessions.md).
- **Port 8080** is for [`http_gateway.md`](http_gateway.md#start-the-gateway); nothing
  listens on it until then.

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
$ docker compose ps --format '{{.Service}}\t{{.Status}}'
```

```output
controller	Up 6 seconds (healthy)
runner	Up Less than a second
```

`--wait` returns once the controller's healthcheck says it is ready, and the runner starts
after it.

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
  - agents_c45940de94d4
```

Every `flz` command on the Part 3 pages runs this way, from the project's folder.

## Talk to a session

> Start a session for Anna, and send it a message.
> {: .step}

```sh
$ flz operator start agent --id anna --namespace agents
```

```output
Started anna
```

```sh
$ flz operator call anna --namespace agents --text 'Hello'
```

```output
{"text":"Message 1: you said \"Hello\". I am a demo model.","tokens":10}
```

`flz operator call` sends one message and waits for the answer the `response()` end
produces from it. [`calls.md`](../../sdk/calls.md) is its contract: the deadline, and the
error it prints when no answer can come.

> Send Anna a second message.
> {: .step}

```sh
$ flz operator call anna --namespace agents --text 'What can you do?'
```

```output
{"text":"Message 2: you said \"What can you do?\". I am a demo model.","tokens":13}
```

The session remembered the first message.

> Start a session for Bob, and send it a message.
> {: .step}

```sh
$ flz operator start agent --id bob --namespace agents
```

```output
Started bob
```

```sh
$ flz operator call bob --namespace agents --text 'Hi, I am Bob'
```

```output
{"text":"Message 1: you said \"Hi, I am Bob\". I am a demo model.","tokens":13}
```

Bob's session has a conversation of its own. Leave both running: the next pages use
them.

## Next

[`streamed_answers.md`](streamed_answers.md) sends the answer while the model is still
writing it.
