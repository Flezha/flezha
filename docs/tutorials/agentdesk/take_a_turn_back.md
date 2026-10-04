# Take a turn back

On this page you rewind Anna's session to an earlier turn, and send a message
that fails without ending the session.

Every turn is one iteration of the session. A rewind undoes the iterations
after the one you name, last first, through each tasklet's `compensate`;
`reply` forgets the last message. `on_failure: keep_progress` makes a failed
turn fail alone, so the session goes on.
[Rewinding a turn](../../topics/advanced/rewinding_a_turn.md) and
[Workflows](../../topics/basics/workflows.md) explain both.

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
$ flz operator rewind anna --to-iteration 0 --namespace agents
```

```output
Operator 'anna' rewound to iteration 0. Iterations above it were compensated; the loop resumes at 1.
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
$ flz operator call anna --namespace agents --text 'Where were we?'
```

```output
{"text":"Message 2: you said \"Where were we?\". I am a demo model.","tokens":12}
```

The demo model reads at most 200 characters, so a longer message fails.

> Send Anna a message of 250 characters, then another one.
> {: .step}

```sh
$ flz operator call anna --namespace agents --text "$(printf '%0250d' 0)"
```

```output
error: operator 'anna' dead-lettered the call's input as 'anna/_turn/fd471f7e0b872ddf': tasklet `reply` failed after 1 attempt(s): non-retryable tasklet error: the demo model reads at most 200 characters, and this message has 250 (call.failed)
hint: flz dlq list anna
```

```sh
$ flz operator call anna --namespace agents --text 'Sorry, go on'
```

```output
{"text":"Message 3: you said \"Sorry, go on\". I am a demo model.","tokens":12}
```

The failed turn was set aside in the dead-letter queue, and the session went on.

## Next

[Stream an answer](stream_an_answer.md) watches an answer arrive token by token.
