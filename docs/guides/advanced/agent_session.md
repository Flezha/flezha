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

### Rust

The whole example:
[`sdk/rust/examples/agentdesk`](https://github.com/flezha/flezha_rust_sdk/tree/main/examples/agentdesk).

```rust
// src/agent.rs: one conversation, one turn per message.
pub struct AgentOperator;

impl BoundedOperator for AgentOperator {
    fn factory_id() -> &'static str {
        "agent"
    }

    fn loop_mode() -> LoopMode {
        LoopMode::PerInput // one turn per message
    }

    fn rollback_mode() -> RollbackMode {
        RollbackMode::KeepProgress // a failed turn fails alone
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("turn")
            .into::<Reply>()
            .sink_named("answer") // to whoever listens
            .into::<Deliver>()
            .response(); // to whoever called
        c.input::<Message>("message") // from the gateway
            .map(|message| message.text)
            .into::<Reply>();
    }
}
```

`LoopMode::PerInput` runs the chain once per message instead of once in all.
`RollbackMode::KeepProgress` is what a failing turn needs, and
[`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails) shows it.

### Node

The whole example:
[`sdk/node/examples/agentdesk`](https://github.com/flezha/flezha_node_sdk/tree/main/examples/agentdesk).

```javascript
// agent_operator.mjs: one conversation, one turn per message.
export default defineBounded({
  factoryId: 'agent',
  loopMode: 'per_input', // one turn per message
  rollbackMode: 'keep_progress', // a failed turn fails alone
  channels: (c) => {
    c.input('turn', { codec: 'text' })
      .into(Reply)
      .sink('answer') // to whoever listens
      .into(Deliver)
      .response(); // to whoever called
    c.input('message') // from the gateway
      .map((message) => message.text)
      .into(Reply);
  },
});
```

`loopMode: 'per_input'` runs the chain once per message instead of once in all.
`rollbackMode: 'keep_progress'` is what a failing turn needs, and
[`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails) shows it. `turn` reads its
message as text; `message` names no codec, so its JSON arrives as an object.

### Python

The whole example:
[`sdk/python/examples/agentdesk`](https://github.com/flezha/flezha_python_sdk/tree/main/examples/agentdesk).

```python
# agent.py: one conversation, one turn per message.
@flezha.bounded_operator(
    "agent",
    loop_mode="per_input",  # one turn per message
    rollback_mode="keep_progress",  # a failed turn fails alone
)
def agent(c: flezha.Channels) -> None:
    (
        c.input("turn", codec="text")
        .into(Reply)
        .sink("answer")  # to whoever listens
        .into(Deliver)
        .response()  # to whoever called
    )
    (
        c.input("message")  # from the gateway
        .map(lambda message: message["text"])
        .into(Reply)
    )
```

`loop_mode="per_input"` runs the chain once per message instead of once in all.
`rollback_mode="keep_progress"` is what a failing turn needs, and
[`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails) shows it. `turn` reads its
message as text; `message` names no codec, so its JSON arrives as a dict.

### Ruby

The whole example:
[`sdk/ruby/examples/agentdesk`](https://github.com/flezha/flezha_ruby_sdk/tree/main/examples/agentdesk).

```ruby
# agent_operator.rb: one conversation, one turn per message.
class AgentOperator < Flezha::BoundedOperator
  factory_id "agent"
  loop_mode :per_input                                       # one turn per message
  rollback_mode :keep_progress                               # a failed turn fails alone

  channels do |c|
    c.input("turn", codec: :text)
     .into(Reply)
     .sink("answer")                                         # to whoever listens
     .into(Deliver)
     .response                                               # to whoever called
    c.input("message")                                       # from the gateway
     .map { |message| message[:text] }
     .into(Reply)
  end
end
```

`loop_mode :per_input` runs the chain once per message instead of once in all.
`rollback_mode :keep_progress` is what a failing turn needs, and
[`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails) shows it. `turn` reads its
message as text; `message` names no codec, so its JSON arrives as a Hash with Symbol keys.

### PHP

The whole example:
[`sdk/php/examples/agentdesk`](https://github.com/flezha/flezha_php_sdk/tree/main/examples/agentdesk).

```php
// src/AgentOperator.php: one conversation, one turn per message.
#[Operator(factoryId: 'agent')]
#[Bounded(
    loopMode: LoopMode::PerInput,                    // one turn per message
    rollbackMode: RollbackMode::KeepProgress,        // a failed turn fails alone
)]
final class AgentOperator extends BoundedOperator
{
    public static function channels(Builder $c): void
    {
        $c->input('turn', codec: ChannelCodec::Text)
            ->into(Reply::class)
            ->sink('answer')                         // to whoever listens
            ->into(Deliver::class)
            ->response();                            // to whoever called
        $c->input('message')                         // from the gateway
            ->map(fn (array $message) => $message['text'])
            ->into(Reply::class);
    }
}
```

`LoopMode::PerInput` runs the chain once per message instead of once in all.
`RollbackMode::KeepProgress` is what a failing turn needs, and
[`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails) shows it. `turn` reads its
message as text; `message` names no codec, so its JSON arrives as an array.

### Elixir

The whole example:
[`sdk/elixir/examples/agentdesk`](https://github.com/flezha/flezha_elixir_sdk/tree/main/examples/agentdesk).

```elixir
# lib/agentdesk/agent_operator.ex: one conversation, one turn per message.
defmodule Agentdesk.AgentOperator do
  use Flezha.SDK.BoundedOperator,
    factory_id: "agent",
    # one turn per message
    loop_mode: :per_input,
    # a failed turn fails alone
    rollback_mode: :keep_progress

  @impl true
  def channels(c) do
    c
    |> input("turn")
    |> into(Reply)
    # to whoever listens
    |> sink("answer")
    |> into(Deliver)
    # to whoever called
    |> response()

    # from the gateway
    c
    |> input_with("message", &Jason.decode/1)
    |> map(& &1["text"])
    |> into(Reply)
  end
end
```

`loop_mode: :per_input` runs the chain once per message instead of once in all.
`rollback_mode: :keep_progress` is what a failing turn needs, and
[`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails) shows it. An entry channel
passes bytes, so `turn`'s message arrives as text, and `message` decodes its JSON with
`Jason.decode/1` into a map with string keys. `sink/2` answers the handle it was given, so
the chain goes on after it into `Deliver`.

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
> model from the example: [`streamed_answers.md`](streamed_answers.md) goes through it.
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

### Rust

```rust
// src/reply.rs: answers a message with the demo model, and keeps the conversation.
#[derive(Clone, Serialize, Deserialize)]
pub struct Answer {
    pub text: String,
    pub tokens: u32,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Conversation {
    messages: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub enum Edit {
    Said(String),
    Unsaid,
}

impl Change<Conversation> for Edit {
    fn apply_to(&self, conversation: &mut Conversation) {
        match self {
            Edit::Said(message) => conversation.messages.push(message.clone()),
            Edit::Unsaid => {
                conversation.messages.pop();
            }
        }
    }
}

#[derive(Default)]
pub struct Reply;

#[tasklet(name = "reply")]
impl Tasklet for Reply {
    type Input = String;
    type Output = Answer;
    type State = Conversation;
    type Change = Edit;

    fn run(&mut self, message: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(demo_model::answer(
            &ctx.state().messages,
            &message,
            |part| ctx.part(part),
        )?)?;
        ctx.change(Edit::Said(message));
        Ok(())
    }

    // The undo, when a turn is taken back: the conversation forgets its message.
    fn compensate(
        &mut self,
        _: Vec<Arc<Answer>>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        println!(
            "[undo] forgot \"{}\"",
            ctx.state().messages.last().map_or("", String::as_str)
        );
        ctx.change(Edit::Unsaid);
        Ok(())
    }
}
```

An answer is its text and the number of tokens the model used for it. `Edit` is the
one change type `reply` records, a message said or the last one taken back.
`|part| ctx.part(part)` hands the model a way to send the answer while it is still being
written, which [`streamed_answers.md`](streamed_answers.md) explains, and `compensate` is
the undo that [`rewinding_a_turn.md`](rewinding_a_turn.md) uses.

```rust
// src/deliver.rs: hands the finished answer to the caller.
#[derive(Default)]
pub struct Deliver;

#[tasklet(name = "deliver")]
impl Tasklet for Deliver {
    type Input = Answer;
    type Output = Answer;

    fn run(&mut self, answer: Answer, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(answer)
    }

    // The undo, when a turn is taken back: an answer already given has nothing to take back.
    fn compensate(
        &mut self,
        _: Vec<Arc<Answer>>,
        _: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }
}
```

### Node

```javascript
// reply.mjs: answers a message with the demo model, and keeps the conversation.
export const Reply = defineTasklet({
  name: 'reply',

  initState: () => ({ messages: [] }), // what the session remembers

  applyChange: (conversation, change) => {
    if (change.kind === 'said') conversation.messages.push(change.message);
    else conversation.messages.pop(); // 'unsaid'
  },

  run: async (message, ctx) => {
    ctx.emit(await answer(ctx.state.messages, message, (part) => ctx.part(part)));
    ctx.change({ kind: 'said', message });
  },

  // The undo, when a turn is taken back: the conversation forgets its message.
  compensate: (_answers, ctx) => {
    console.log(`[undo] forgot "${ctx.state.messages.at(-1)}"`);
    ctx.change({ kind: 'unsaid' });
  },
});
```

An answer is an object of its text and the number of tokens the model used for it. A
change is an object too, a message said or the last one taken back, and `applyChange`
folds it into the conversation. `run` awaits the model and hands it a way to send the
answer through `ctx.part` while it is still being written, which
[`streamed_answers.md`](streamed_answers.md) explains, and `compensate` is the undo that
[`rewinding_a_turn.md`](rewinding_a_turn.md) uses.

```javascript
// deliver.mjs: hands the finished answer to the caller.
export const Deliver = defineTasklet({
  name: 'deliver',

  run: (answer, ctx) => ctx.emit(answer),

  // The undo, when a turn is taken back: an answer already given has nothing to take back.
  compensate: () => {},
});
```

### Python

```python
# reply.py: answers a message with the demo model, and keeps the conversation.
@flezha.state
@dataclass
class Conversation:  # what the session remembers
    messages: list[str] = field(default_factory=list)


class Edit(TypedDict):  # a message said, or the last one taken back
    kind: Literal["said", "unsaid"]
    message: NotRequired[str]


@flezha.tasklet(name="reply")
class Reply:
    def run(self, message: str, ctx: flezha.Ctx[Conversation, Answer]) -> None:
        ctx.emit(demo_model.answer(ctx.state.messages, message, ctx.part))
        ctx.change(Edit(kind="said", message=message))

    def apply_change(self, conversation: Conversation, edit: Edit) -> None:
        match edit:
            case {"kind": "said", "message": message}:
                conversation.messages.append(message)
            case _:
                conversation.messages.pop()

    # The undo, when a turn is taken back: the conversation forgets its message.
    def compensate(self, outputs: list[Answer], ctx: flezha.Ctx[Conversation, Answer]) -> None:
        print(f'[undo] forgot "{ctx.state.messages[-1]}"', flush=True)
        ctx.change(Edit(kind="unsaid"))
```

An answer, the `Answer` in `demo_model.py`, is a dict of its text and the number of tokens
the model used for it. `Edit` is the one change `reply` records, a message said or the
last one taken back, and `apply_change` folds it into the conversation. `ctx.part` hands
the model a way to send the answer while it is still being written, which
[`streamed_answers.md`](streamed_answers.md) explains, and `compensate` is the undo that
[`rewinding_a_turn.md`](rewinding_a_turn.md) uses.

```python
# deliver.py: hands the finished answer to the caller.
@flezha.tasklet(name="deliver")
class Deliver:
    def run(self, answer: Answer, ctx: flezha.Ctx[None, Answer]) -> None:
        ctx.emit(answer)

    # The undo, when a turn is taken back: an answer already given has nothing to take back.
    def compensate(self, outputs: list[Answer], ctx: flezha.Ctx[None, Answer]) -> None:
        pass
```

### Ruby

```ruby
# reply.rb: answers a message with the demo model, and keeps the conversation.
class Reply < Flezha::Tasklet
  tasklet_name "reply"

  def init_state(_session) = { messages: [] } # what the session remembers

  def apply_change(conversation, change)
    case change
    in { kind: "said", message: } then conversation[:messages] << message
    in { kind: "unsaid" } then conversation[:messages].pop
    end
  end

  def run(message, ctx)
    ctx.emit(DemoModel.answer(ctx.state[:messages], message) { |part| ctx.part(part) })
    ctx.change({ kind: "said", message: message })
  end

  # The undo, when a turn is taken back: the conversation forgets its message.
  def compensate(_answers, ctx)
    puts "[undo] forgot \"#{ctx.state[:messages].last}\""
    ctx.change({ kind: "unsaid" })
  end
end
```

An answer is a Hash of its text and the number of tokens the model used for it. A
change is a Hash too, a message said or the last one taken back, and `apply_change` folds
it into the conversation. The block hands the model a way to send the answer while it is
still being written, which [`streamed_answers.md`](streamed_answers.md) explains, and
`compensate` is the undo that [`rewinding_a_turn.md`](rewinding_a_turn.md) uses.

```ruby
# deliver.rb: hands the finished answer to the caller.
class Deliver < Flezha::Tasklet
  tasklet_name "deliver"

  def run(answer, ctx) = ctx.emit(answer)

  # The undo, when a turn is taken back: an answer already given has nothing to take back.
  def compensate(_answers, _ctx); end
end
```

### PHP

```php
// src/Reply.php: answers a message with the demo model, and keeps the conversation.
#[Task(name: 'reply')]
final class Reply extends Tasklet
{
    public function initState(?string $cellKey): mixed
    {
        return ['messages' => []];                   // what the session remembers
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        match ($change['kind']) {
            'said' => $state['messages'][] = $change['message'],
            'unsaid' => array_pop($state['messages']),
        };
    }

    public function run(mixed $input, Ctx $ctx): void
    {
        $ctx->emit(DemoModel::answer($ctx->state['messages'], $input, $ctx->part(...)));
        $ctx->change(['kind' => 'said', 'message' => $input]);
    }

    // The undo, when a turn is taken back: the conversation forgets its message.
    public function compensate(array $outputs, Ctx $ctx): void
    {
        echo '[undo] forgot "' . end($ctx->state['messages']) . "\"\n";
        $ctx->change(['kind' => 'unsaid']);
    }
}
```

An answer is an array of its text and the number of tokens the model used for it. A
change is plain data, a message said or the last one taken back, and `applyChange` folds
it into the conversation. `$ctx->part(...)` hands the model a way to send the answer
while it is still being written, which [`streamed_answers.md`](streamed_answers.md)
explains, and `compensate` is the undo that [`rewinding_a_turn.md`](rewinding_a_turn.md)
uses.

```php
// src/Deliver.php: hands the finished answer to the caller.
#[Task(name: 'deliver')]
final class Deliver extends Tasklet
{
    public function run(mixed $input, Ctx $ctx): void
    {
        $ctx->emit($input);
    }

    // The undo, when a turn is taken back: an answer already given has nothing to take back.
    public function compensate(array $outputs, Ctx $ctx): void
    {
    }
}
```

### Elixir

```elixir
# lib/agentdesk/reply.ex: answers a message with the demo model, and keeps the conversation.
defmodule Agentdesk.Reply do
  use Flezha.SDK.Tasklet, name: "reply"

  # What the session remembers.
  state do
    field(:messages, :list, default: [])
  end

  defchange said(conversation, message) do
    %{conversation | messages: conversation.messages ++ [message]}
  end

  defchange unsaid(conversation) do
    %{conversation | messages: Enum.drop(conversation.messages, -1)}
  end

  @impl true
  def run(message, conversation, ctx) do
    with {:ok, answer} <- DemoModel.answer(conversation.messages, message, &part(ctx, &1)) do
      emit(ctx, answer)
      {:ok, said(conversation, ctx, message)}
    end
  end

  # The undo, when a turn is taken back: the conversation forgets its message.
  @impl true
  def compensate(_answers, conversation, ctx) do
    IO.puts(~s([undo] forgot "#{List.last(conversation.messages)}"))
    {:ok, unsaid(conversation, ctx)}
  end
end
```

The `state` block is what the session remembers, and each `defchange` is one change to
it, a message said or the last one taken back. An answer is the demo model's `Answer`,
its text and the number of tokens the model used for it. `run/3` hands the model a
function that sends the answer through `part/2` while it is still being written, which
[`streamed_answers.md`](streamed_answers.md) explains, and `compensate/3` is the undo that
[`rewinding_a_turn.md`](rewinding_a_turn.md) uses.

```elixir
# lib/agentdesk/deliver.ex: hands the finished answer to the caller.
defmodule Agentdesk.Deliver do
  use Flezha.SDK.Tasklet, name: "deliver"

  @impl true
  def run(answer, state, ctx) do
    emit(ctx, answer)
    {:ok, state}
  end

  # The undo, when a turn is taken back: an answer already given has nothing to take back.
  @impl true
  def compensate(_answers, state, _ctx), do: {:ok, state}
end
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

### Rust

```rust
// src/main.rs: the runner.
mod admit;
mod agent;
mod budget;
mod deliver;
mod demo_model;
mod gateway;
mod http;
mod reply;
mod request;
mod watch;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runner = Flezha::create_runner("agents")?;
    runner
        .register_factory(agent::AgentOperator)?
        .register_factory(gateway::GatewayOperator)?
        .start()?;

    runner.await_termination()?;
    Ok(())
}
```

The runner's image is the binary Cargo builds, on a Debian runtime:

```dockerfile
# The runner's image: the binary `cargo build --release` builds, on a Debian 12 runtime.
FROM debian:bookworm-slim
COPY target/release/agentdesk /agentdesk/agentdesk
USER 1000
ENTRYPOINT ["/agentdesk/agentdesk"]
```

The image copies the binary built on your machine, so one built against a C library
newer than Debian 12's may not start in it. The project's `.dockerignore` sends Docker
that binary alone, not the rest of `target/`.

### Node

```javascript
// runner.mjs: the runner.
const runner = createRunner('agents');
await runner.registerFactory('./agent_operator.mjs');
await runner.registerFactory('./gateway_operator.mjs');

await runner.run();
```

An operator is registered by the path of its module, which resolves against the working
directory, so the runner starts from the project's folder.

The runner's image is the project, with its `node_modules/`, on a Node image:

```dockerfile
# The runner's image: the project and the packages `npm install` put in node_modules/, on Node 24.
FROM node:24-bookworm-slim
COPY . /agentdesk
WORKDIR /agentdesk
# The runner stops when its standard input closes, and a container's is closed: it reads a pipe
# that stays open instead.
RUN mkfifo -m 600 stdin && chown 1000 stdin
USER 1000
ENTRYPOINT ["sh", "-c", "exec node runner.mjs <> stdin"]
```

The image copies `node_modules/` as `npm install` left it on your machine, and the
project's `.dockerignore` sends Docker only the JavaScript files, `package.json` and
`node_modules/`. `npm install` fetches the SDK's native part for the machine it runs on,
so run it on Linux, on the processor the image runs on. A Node runner stops when its
standard input closes, and a container starts with it closed, so the image gives the
runner a pipe that stays open.

### Python

```python
# runner.py: the runner.
def main() -> None:
    runner = flezha.create_runner("agents").register_factory(agent).register_factory(gateway)

    runner.run()


if __name__ == "__main__":
    main()
```

The runner's image is the project, with its `vendor/`, on a Python image:

```dockerfile
# The runner's image: the project and the packages `pip install --target vendor` put in vendor/,
# on Python 3.12.
FROM python:3.12-slim
COPY . /agentdesk
WORKDIR /agentdesk
ENV PYTHONPATH=/agentdesk/vendor
# The runner stops when its standard input closes, and a container's is closed: it reads a pipe
# that stays open instead.
RUN mkfifo -m 600 stdin && chown 1000 stdin
USER 1000
ENTRYPOINT ["sh", "-c", "exec python runner.py <> stdin"]
```

The image copies `vendor/` as `pip install --target vendor` left it on your machine, and
the project's `.dockerignore` sends Docker only the Python files and `vendor/`. A Python
runner stops when its standard input closes, and a container starts with it closed, so
the image gives the runner a pipe that stays open.

### Ruby

```ruby
# runner.rb: the runner.
# The sessions print their undos; written at once, they reach the log in the
# order they happened.
$stdout.sync = true

runner = Flezha.create_runner("agents")
               .register_factory(AgentOperator)
               .register_factory(GatewayOperator)
               .boot

runner.await_termination
```

The runner's image installs the project's gems on a Ruby image:

```dockerfile
# The runner's image: the project, and the gems `bundle cache` put in vendor/cache, on Ruby 4.0.
# msgpack builds a C extension when it is installed, so the gems are installed in the full Ruby
# image, which has a compiler, and copied onto the slim one.
FROM ruby:4.0 AS gems
WORKDIR /agentdesk
COPY Gemfile Gemfile.lock ./
COPY vendor/cache vendor/cache
RUN bundle install --local

FROM ruby:4.0-slim
COPY --from=gems /usr/local/bundle /usr/local/bundle
COPY . /agentdesk
WORKDIR /agentdesk
# The runner stops when its standard input closes, and a container's is closed: it reads a pipe
# that stays open instead.
RUN mkfifo -m 600 stdin && chown 1000 stdin
USER 1000
ENTRYPOINT ["sh", "-c", "exec ruby runner.rb <> stdin"]
```

The image installs the gems `bundle cache` put in `vendor/cache` on your machine, and the
project's `.dockerignore` sends Docker only the Ruby files, the `Gemfile` with its lock
and `vendor/cache`. A Ruby runner stops when its standard input closes, and a container
starts with it closed, so the image gives the runner a pipe that stays open.

### PHP

```php
// runner.php: the runner.
require __DIR__ . '/vendor/autoload.php';

$runner = Flezha::createRunner('agents')
    ->registerFactory(AgentOperator::class)
    ->registerFactory(GatewayOperator::class)
    ->start();

$runner->awaitTermination();
```

The runner's image is the project, with its `vendor/`, on a PHP image with FFI:

```dockerfile
# The runner's image: the project and the packages `composer install` put in vendor/, on PHP 8.4
# with FFI, which the SDK loads its native library through.
FROM php:8.4-cli
RUN apt-get update && apt-get install -y --no-install-recommends libffi-dev \
    && rm -rf /var/lib/apt/lists/* && docker-php-ext-install ffi
COPY . /agentdesk
WORKDIR /agentdesk
# The runner stops when its standard input closes, and a container's is closed: it reads a pipe
# that stays open instead.
RUN mkfifo -m 600 stdin && chown 1000 stdin
USER 1000
ENTRYPOINT ["sh", "-c", "exec php runner.php <> stdin"]
```

The image copies `vendor/` as `composer install` left it on your machine, and the
project's `.dockerignore` sends Docker only `runner.php`, `src/` and `vendor/`. A PHP
runner stops when its standard input closes, and a container starts with it closed, so
the image gives the runner a pipe that stays open.

### Elixir

```elixir
# lib/agentdesk/application.ex: the runner.
defmodule Agentdesk.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # `:uds` because this node is not the controller's.
      {Flezha.SDK,
       namespace: "agents",
       operators: [Agentdesk.AgentOperator, Agentdesk.GatewayOperator],
       transport: :uds}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
```

The runner is the project's application: `mix.exs` names `Agentdesk.Application` as the
module that starts it, and `Flezha.SDK` under its supervisor connects to the controller.

The runner's image is the release Mix builds, on a Debian runtime:

```dockerfile
# The runner's image: the release `MIX_ENV=prod mix release` builds, on a Debian 13 runtime.
FROM debian:trixie-slim
COPY _build/prod/rel/agentdesk /agentdesk
# The runner is not an Erlang cluster node, and writes its temporary files where it may.
ENV LANG=C.UTF-8 RELEASE_DISTRIBUTION=none RELEASE_TMP=/tmp
USER 1000
ENTRYPOINT ["/agentdesk/bin/agentdesk", "start"]
```

The image copies the release built on your machine, with the Erlang runtime it carries,
so one built against a C library newer than Debian 13's may not start in it. The
project's `.dockerignore` sends Docker the release alone, not the rest of `_build/`.

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

### Rust

```sh
$ cargo build --release
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
