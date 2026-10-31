# Start a session

On this page you build the runner, start the agentdesk in Docker Compose, talk
to two sessions, and watch an idle one sleep and wake on another runner.

A session is a relay that runs in steps, one pass per message: `each:
per_input`. It sleeps after ten seconds without a message and wakes with its
conversation when the next one arrives.
[Sessions](../sessions.html) explains the shape, and
[Relay settings](../relay_settings.html) the settings.

<!-- tabs-open -->

### Java

```java
// AgentRelay.java: the session: one pass per message
public final class AgentRelay implements Relay {
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
        c.input(String.class, "turn")                    // entry 1: `flz relay call`
                .into(Reply.class)
                .output("answer");                       // to whoever called, and whoever listens
        c.input(Message.class, "message")                // entry 2: the gateway, into the same Reply
                .map(String.class, Message::text)
                .into(Reply.class);
    }
}
```

[`AgentRelay.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/AgentRelay.java)

### Rust

```rust
// src/agent.rs: the session: one pass per message
pub struct AgentRelay;

impl Relay for AgentRelay {
    fn factory_id() -> &'static str {
        "agent"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("turn") // entry 1: `flz relay call`
            .into::<Reply>()
            .output_named("answer"); // to whoever called, and whoever listens
        c.input::<Message>("message") // entry 2: the gateway, into the same Reply
            .map(|message| message.text)
            .into::<Reply>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,                // one turn per message
                on_failure: OnFailure::KeepProgress, // a failed turn fails alone
            },
            durable: Durable::Local {
                sleep_after: SleepAfter::After(Duration::from_secs(10)), // an idle session sleeps after 10 s
                keep_finished: KeepFinished::Nothing,
            },
            ..RelaySettings::default()
        }
    }
}
```

[`src/agent.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/agentdesk/src/agent.rs)

### Node

```javascript
// agent_relay.mjs: the session: one pass per message
export default defineRelay({
  factoryId: 'agent',
  // one turn per message; a failed turn fails alone
  steps: { each: 'per_input', onFailure: 'keep_progress' },
  durable: { local: { sleepAfter: 10_000 } }, // an idle session sleeps after 10 s
  channels: (c) => {
    c.input('turn', { codec: 'text' }) // entry 1: `flz relay call`
      .into(Reply)
      .output('answer'); // to whoever called, and whoever listens
    c.input('message') // entry 2: the gateway, into the same Reply
      .map((message) => message.text)
      .into(Reply);
  },
});
```

[`agent_relay.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/agentdesk/agent_relay.mjs)

### Python

```python
# agent.py: the session: one pass per message
@flezha.relay(
    "agent",
    # one turn per message; a failed turn fails alone
    run={"steps": {"each": "per_input", "on_failure": "keep_progress"}},
    durable={"local": {"sleep_after": 10_000}},  # an idle session sleeps after 10 s
)
def agent(c: flezha.ChannelBuilder) -> None:
    (
        c.input("turn", codec="text")  # entry 1: `flz relay call`
        .into(Reply)
        .output("answer")  # to whoever called, and whoever listens
    )
    (
        c.input("message")  # entry 2: the gateway, into the same Reply
        .map(lambda message: message["text"])
        .into(Reply)
    )
```

[`agent.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/agentdesk/agent.py)

### Ruby

```ruby
# agent_relay.rb: the session: one pass per message
class AgentRelay < Flezha::Relay
  factory_id "agent"
  run steps: { each: :per_input, on_failure: :keep_progress } # one turn per message; a failed turn fails alone
  durable local: { sleep_after: 10_000 } # an idle session sleeps after 10 s

  channels do |c|
    c.input("turn", codec: :text)                            # entry 1: `flz relay call`
     .into(Reply)
     .output("answer")                                       # to whoever called, and whoever listens
    c.input("message")                                       # entry 2: the gateway, into the same Reply
     .map { |message| message[:text] }
     .into(Reply)
  end
end
```

[`agent_relay.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/agentdesk/agent_relay.rb)

### PHP

```php
// src/AgentRelay.php: the session: one pass per message
#[Relay(
    factoryId: 'agent',
    run: ['steps' => [
        'each' => 'per_input',                       // one turn per message
        'on_failure' => 'keep_progress',             // a failed turn fails alone
    ]],
    durable: ['local' => ['sleep_after' => 10_000]], // an idle session sleeps after 10 s
)]
final class AgentRelay extends \Flezha\Relay
{
    public static function channels(ChannelBuilder $c): void
    {
        $c->input('turn', codec: ChannelCodec::Text) // entry 1: `flz relay call`
            ->into(Reply::class)
            ->output('answer');                      // to whoever called, and whoever listens
        $c->input('message')                         // entry 2: the gateway, into the same Reply
            ->map(fn (array $message) => $message['text'])
            ->into(Reply::class);
    }
}
```

[`src/AgentRelay.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/agentdesk/src/AgentRelay.php)

### Elixir

```elixir
# lib/agentdesk/agent_relay.ex: the session: one pass per message
defmodule Agentdesk.AgentRelay do
  use Flezha.SDK.Relay,
    factory_id: "agent",
    # one turn per message; a failed turn fails alone
    run: {:steps, each: :per_input, on_failure: :keep_progress},
    # an idle session sleeps after 10 s
    durable: {:local, sleep_after: 10_000}

  @impl true
  def channels(c) do
    c
    # entry 1: `flz relay call`
    |> input("turn", codec: :text)
    |> into(Reply)
    # to whoever called, and whoever listens
    |> output("answer")

    # entry 2: the gateway, into the same Reply
    c
    |> input("message")
    |> map(& &1["text"])
    |> into(Reply)
  end
end
```

[`lib/agentdesk/agent_relay.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/agentdesk/lib/agentdesk/agent_relay.ex)

### Go

```go
// agent.go: the session: one pass per message
type AgentRelay struct{}

func (AgentRelay) Spec() flezha.RelaySpec {
	return flezha.RelaySpec{
		FactoryID: "agent",
		Run: map[string]any{"steps": map[string]any{
			"each":       "per_input",     // one turn per message
			"on_failure": "keep_progress", // a failed turn fails alone
		}},
		Durable: map[string]any{"local": map[string]any{
			"sleep_after": 10_000, // an idle session sleeps after 10 s
		}},
	}
}

func (AgentRelay) Channels(c *flezha.ChannelBuilder) {
	// entry 1: `flz relay call`
	turn := flezha.Input[string](c, "turn", flezha.InputOptions{Codec: flezha.Text})
	flezha.Into(turn, Reply{}).
		Output("answer") // to whoever called, and whoever listens

	// entry 2: the gateway, into the same Reply
	message := flezha.Input[Message](c, "message")
	flezha.Into(flezha.Map(message, func(m Message) string { return m.Text }), Reply{})
}
```

[`agent.go`](https://github.com/flezha/flezha_go_sdk/blob/main/examples/agentdesk/agent.go)

<!-- tabs-close -->

> Build the runner, then start both containers, from the agentdesk folder.
> {: .step}

<!-- tabs-open -->

### Java

```sh
$ mvn -q package
```

### Rust

```sh
$ cargo build --release --target-dir target
```

### Node

```sh
$ npm install
```

### Python

```sh
$ python3 -m pip install --target vendor -r requirements.txt
```

### Ruby

```sh
$ bundle cache
```

### PHP

```sh
$ composer install
```

### Elixir

```sh
$ mix deps.get
$ MIX_ENV=prod mix release
```

### Go

```sh
$ go build
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

  RUNNER                            PID      NODE                  CONNECTED AT          LATE
  agents_33173e7dbe13               -        flezha@controller.agentdesk2026-10-18T09:20:41Z  no
```

> Start a session for Anna, and send it two messages.
> {: .step}

```sh
$ flz relay start agent --id anna --namespace agents
$ flz relay call anna --namespace agents --text 'Hello'
$ flz relay call anna --namespace agents --text 'What can you do?'
```

```output
Started anna
{"text":"Message 1: you said \"Hello\". I am a demo model.","tokens":10}
{"text":"Message 2: you said \"What can you do?\". I am a demo model.","tokens":13}
```

`flz relay call` sends one message and waits for the answer. Bob's session is
another instance of the same relay, with a conversation of its own:

> Start a session for Bob, and send it a message.
> {: .step}

```sh
$ flz relay start agent --id bob --namespace agents
$ flz relay call bob --namespace agents --text 'Hi, I am Bob'
```

```output
Started bob
{"text":"Message 1: you said \"Hi, I am Bob\". I am a demo model.","tokens":13}
```

> Leave Anna alone for ten seconds, then ask the controller about her session.
> {: .step}

```sh
$ flz relay info --namespace agents
```

```output
...
Relay: anna
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
$ flz relay call anna --namespace agents --text 'Are you still there?'
```

```output
{"text":"Message 3: you said \"Are you still there?\". I am a demo model.","tokens":13}
```

She remembers the conversation: this is her third message.

## Next

[Take a turn back](take_a_turn_back.md) rewinds Anna's session.
