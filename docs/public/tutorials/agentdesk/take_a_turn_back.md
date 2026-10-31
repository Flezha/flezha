# Take a turn back

On this page you rewind Anna's session to an earlier turn, and send a message
that fails without ending the session.

Every turn is one iteration of the session. A rewind undoes the iterations
after the one you name, last first, through each tasklet's `compensate`;
`reply` forgets the last message. `on_failure: keep_progress` makes a failed
turn fail alone, so the session goes on.
[Rewinding a turn](../sessions.html#rewinding-a-turn) and
[Workflows](../workflows.html) explain both.

<!-- tabs-open -->

### Java

```java
// Reply.java: answers, and forgets a message on an undo
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
        // Each piece streams out as it is made; then the whole answer.
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

[`Reply.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/Reply.java)

### Rust

```rust
// src/reply.rs: answers, and forgets a message on an undo
#[derive(Clone, Serialize, Deserialize)]
pub struct Answer {
    pub text: String,
    pub tokens: u32,
}

/// What the session remembers.
#[derive(Default, Serialize, Deserialize)]
pub struct Conversation {
    messages: Vec<String>,
}

/// How the conversation changes: a message said, or the last one taken back.
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
        // Each piece streams out as it is made; then the whole answer.
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

[`src/reply.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/agentdesk/src/reply.rs)

### Node

```javascript
// reply.mjs: answers, and forgets a message on an undo
export const Reply = defineTasklet({
  name: 'reply',

  initState: () => ({ messages: [] }), // what the session remembers

  applyChange: (conversation, change) => {
    if (change.kind === 'said') conversation.messages.push(change.message);
    else conversation.messages.pop(); // 'unsaid'
  },

  run: async (message, ctx) => {
    // Each piece streams out as it is made; then the whole answer.
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

[`reply.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/agentdesk/reply.mjs)

### Python

```python
# reply.py: answers, and forgets a message on an undo
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
        # Each piece streams out as it is made; then the whole answer.
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

[`reply.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/agentdesk/reply.py)

### Ruby

```ruby
# reply.rb: answers, and forgets a message on an undo
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
    # Each piece streams out as it is made; then the whole answer.
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

[`reply.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/agentdesk/reply.rb)

### PHP

```php
// src/Reply.php: answers, and forgets a message on an undo
#[Tasklet\Spec(name: 'reply')]
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
        // Each piece streams out as it is made; then the whole answer.
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

[`src/Reply.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/agentdesk/src/Reply.php)

### Elixir

```elixir
# lib/agentdesk/reply.ex: answers, and forgets a message on an undo
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
    # Each piece streams out as it is made; then the whole answer.
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

[`lib/agentdesk/reply.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/agentdesk/lib/agentdesk/reply.ex)

### Go

```go
// reply.go: answers, and forgets a message on an undo
type Answer struct {
	Text   string `json:"text"`
	Tokens int    `json:"tokens"`
}

// Conversation is what the session remembers.
type Conversation struct {
	Messages []string `json:"messages"`
}

type Said struct {
	Message string `json:"message"`
}

func (s Said) ApplyTo(c *Conversation) { c.Messages = append(c.Messages, s.Message) }

type Unsaid struct{}

func (Unsaid) ApplyTo(c *Conversation) {
	if n := len(c.Messages); n > 0 {
		c.Messages = c.Messages[:n-1]
	}
}

type Reply struct{}

func (Reply) Spec() flezha.Spec { return flezha.Spec{Name: "reply"} }

func (Reply) Changes() flezha.Changes[Conversation] {
	return flezha.Changes[Conversation]{"said": Said{}, "unsaid": Unsaid{}}
}

func (Reply) Run(message string, ctx *flezha.Ctx[Conversation, Answer]) error {
	// Each piece streams out as it is made; then the whole answer.
	answer, err := askDemoModel(ctx.State().Messages, message, ctx.Part)
	if err != nil {
		return err
	}
	if err := ctx.Emit(answer); err != nil {
		return err
	}
	return ctx.Change(Said{Message: message})
}

// Compensate is the undo, when a turn is taken back: the conversation forgets
// its message.
func (Reply) Compensate(_ []Answer, ctx *flezha.Ctx[Conversation, Answer]) error {
	if messages := ctx.State().Messages; len(messages) > 0 {
		fmt.Printf("[undo] forgot \"%s\"\n", messages[len(messages)-1])
	}
	return ctx.Change(Unsaid{})
}
```

[`reply.go`](https://github.com/flezha/flezha_go_sdk/blob/main/examples/agentdesk/reply.go)

<!-- tabs-close -->

> Read Anna's conversation.
> {: .step}

```sh
$ flz state get anna/reply --namespace agents
```

```output
state get anna/reply:
  change_seq: 3
  state:      {"reply":{"messages":["Hello","What can you do?","Are you still there?"]}}
```

> Take Anna's session back to iteration 0, keeping her first message.
> {: .step}

```sh
$ flz relay rewind anna --to-iteration 0 --namespace agents
```

```output
Relay 'anna' rewound to iteration 0. Iterations above it were compensated; the loop resumes at 1.
```

```sh
$ docker compose logs runner | grep undo
```

```output
runner-1  | [undo] forgot "Are you still there?"
runner-1  | [undo] forgot "What can you do?"
```

> Send Anna a message.
> {: .step}

```sh
$ flz relay call anna --namespace agents --text 'Where were we?'
```

```output
{"text":"Message 2: you said \"Where were we?\". I am a demo model.","tokens":12}
```

The demo model reads at most 200 characters, so a longer message fails.

> Send Anna a message of 250 characters, then another one.
> {: .step}

```sh
$ flz relay call anna --namespace agents --text "$(printf '%0250d' 0)"
```

```output
error: relay 'anna' dead-lettered the call's input as 'anna/_turn/fd471f7e0b872ddf': tasklet `reply` failed after 1 attempt(s): non-retryable tasklet error: the demo model reads at most 200 characters, and this message has 250 (call.failed)
hint: flz dlq list anna
```

```sh
$ flz relay call anna --namespace agents --text 'Sorry, go on'
```

```output
{"text":"Message 3: you said \"Sorry, go on\". I am a demo model.","tokens":12}
```

The failed turn was set aside in the dead-letter queue, and the session went on.

## Next

[Stream an answer](stream_an_answer.md) watches an answer arrive token by token.
