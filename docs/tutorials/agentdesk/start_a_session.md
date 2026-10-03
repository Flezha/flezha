# Start a session

On this page you build the runner, start the agentdesk in Docker Compose, talk
to two sessions, and watch an idle one sleep and wake on another runner.

A session is an operator that runs in steps, one pass per message: `each:
per_input`. It sleeps after ten seconds without a message and wakes with its
conversation when the next one arrives.
[Sessions](../../topics/advanced/sessions.md) explains the shape, and
[Configuring an operator](../../topics/basics/configuring_an_operator.md) the settings.

<!-- tabs-open -->

### Java

```java
// AgentOperator.java: the session: one pass per message
public final class AgentOperator implements Operator {
    @Override
    public String factoryId() {
        return "agent";
    }

    @Override
    public Run run() {
        return new Run.Steps(
                Each.PER_INPUT,                          // one turn per message
                OnFailure.KEEP_PROGRESS);                // a failed turn fails alone
    }

    @Override
    public Durable durable() {
        return new Durable.Local(Duration.ofSeconds(10)); // an idle session sleeps after 10 s
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

[`AgentOperator.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/AgentOperator.java)

### Rust

TODO: this page is not yet written for Rust.

### Node

TODO: this page is not yet written for Node.

### Python

TODO: this page is not yet written for Python.

### Ruby

TODO: this page is not yet written for Ruby.

### PHP

TODO: this page is not yet written for PHP.

### Elixir

```elixir
# lib/agentdesk/agent_operator.ex: the session: one pass per message
defmodule Agentdesk.AgentOperator do
  use Flezha.SDK.Operator,
    factory_id: "agent",
    run: {:steps, each: :per_input, on_failure: :keep_progress},
    durable: {:local, sleep_after: 10_000}

  @impl true
  def channels(c) do
    c
    |> input("turn")
    |> into(Reply)
    |> sink("answer")
    |> into(Deliver)
    |> response()

    c
    |> input_with("message", &Jason.decode/1)
    |> map(& &1["text"])
    |> into(Reply)
  end
end
```

[`lib/agentdesk/agent_operator.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/agentdesk/lib/agentdesk/agent_operator.ex)

<!-- tabs-close -->

> Build the runner, then start both containers, from the agentdesk folder.
> {: .step}

<!-- tabs-open -->

### Java

```sh
$ mvn -q package
```

### Rust

TODO: this page is not yet written for Rust.

### Node

TODO: this page is not yet written for Node.

### Python

TODO: this page is not yet written for Python.

### Ruby

TODO: this page is not yet written for Ruby.

### PHP

TODO: this page is not yet written for PHP.

### Elixir

```sh
$ mix deps.get
$ MIX_ENV=prod mix release
```

<!-- tabs-close -->

```sh
$ docker compose up -d --build --wait
```

> Make `flz` the CLI inside the controller's container, for this shell, and ask for the runner.
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

> Start a session for Anna, and send it two messages.
> {: .step}

```sh
$ flz operator start agent --id anna --namespace agents
$ flz operator call anna --namespace agents --text 'Hello'
$ flz operator call anna --namespace agents --text 'What can you do?'
```

```output
Started anna
{"text":"Message 1: you said \"Hello\". I am a demo model.","tokens":10}
{"text":"Message 2: you said \"What can you do?\". I am a demo model.","tokens":13}
```

`flz operator call` sends one message and waits for the answer. Bob's session is
another instance of the same operator, with a conversation of its own:

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

> Leave Anna alone for ten seconds, then ask the controller about her session.
> {: .step}

```sh
$ flz operator info anna --namespace agents
```

```output
...
Operator: anna
  Run: steps (each: per_input, on_failure: keep_progress)
  Durable: local (sleep_after: 10s)
  Phase: suspended
  Health: suspended
  Runner ID:
...
```

> Restart the runner, so the one that wakes Anna is not the one she fell asleep on, and send her a message.
> {: .step}

```sh
$ docker compose restart runner
$ flz operator call anna --namespace agents --text 'Are you still there?'
```

```output
{"text":"Message 3: you said \"Are you still there?\". I am a demo model.","tokens":13}
```

She remembers the conversation: this is her third message.

## Next

[Take a turn back](take_a_turn_back.md) rewinds Anna's session.
