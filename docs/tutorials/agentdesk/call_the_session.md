# Call the session

On this page you send messages to sessions over HTTP. The gateway calls the
user's session with `call`, which starts the session if it does not run yet,
and keeps each user's daily token budget in a cell per user, fed by the tokens
each answer used. [Operator calls](../../topics/advanced/operator_calls.md) explains
`call`, and [State](../../topics/basics/state.md) the cell per key.

<!-- tabs-open -->

### Java

```java
// Budget.java: each user's tokens today, and the verdict
@Tasklet.Spec(name = "budget")
public final class Budget implements Tasklet<Budget.Event, Budget.Spent, Budget.Verdict> {
    static final int DAILY_TOKENS = 40;

    public sealed interface Event {                          // what reaches a user's cell
        String user();
    }

    public record Sent(Message message) implements Event {  // a message to admit
        @Override
        public String user() {
            return message.user();
        }
    }

    public record Used(String user, int tokens) implements Event {    // the tokens an answer used
        static Used of(Response<Message, Answer> response) {          // a failed turn used none
            return new Used(response.request().user(), response.ok() ? response.answer().tokens() : 0);
        }
    }

    public record Verdict(Message message, int spent) {
        boolean admitted() {
            return spent < DAILY_TOKENS;
        }
    }

    public static class Spent {                              // what each user's cell remembers
        public String day = "";
        public int tokens;

        int on(String today) {
            return today.equals(day) ? tokens : 0;
        }
    }

    public record Counted(String day, int tokens) implements Change<Spent> {
        @Override
        public void applyTo(Spent spent) {
            spent.tokens = spent.on(day) + tokens;
            spent.day = day;
        }
    }

    @Override
    public void run(Event event, Ctx<Spent, Verdict> ctx) throws TaskletException {
        String today = LocalDate.ofInstant(Instant.ofEpochMilli(ctx.now()), ZoneOffset.UTC).toString();
        switch (event) {
            case Sent sent -> ctx.emit(new Verdict(sent.message(), ctx.state().on(today)));
            case Used used -> ctx.change(new Counted(today, used.tokens()));
        }
    }
}
```

[`Budget.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/Budget.java)

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
# lib/agentdesk/budget.ex: each user's tokens today, and the verdict
defmodule Agentdesk.Budget do
  use Flezha.SDK.Tasklet, name: "budget"

  @daily_tokens 40

  def daily_tokens, do: @daily_tokens

  def admitted?(verdict), do: verdict.spent < @daily_tokens

  def used(%Response{request: request, result: result}) do
    %{user: Jason.decode!(request)["user"], tokens: tokens(result)}
  end

  defp tokens({:ok, answer}), do: answer["tokens"]
  defp tokens({:error, _error}), do: 0

  state do
    field(:day, :string, default: "")
    field(:tokens, :integer, default: 0)
  end

  defchange counted(spent, day, tokens) do
    %{spent | day: day, tokens: on(spent, day) + tokens}
  end

  @impl true
  def run(%{tokens: tokens}, spent, ctx), do: {:ok, counted(spent, ctx, today(ctx), tokens)}

  def run(message, spent, ctx) do
    emit(ctx, %{message: message, spent: on(spent, today(ctx))})
    {:ok, spent}
  end

  defp today(ctx), do: ctx |> now() |> DateTime.from_unix!(:millisecond) |> Date.to_iso8601()

  defp on(%{day: day, tokens: tokens}, day), do: tokens
  defp on(_spent, _today), do: 0
end
```

[`lib/agentdesk/budget.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/agentdesk/lib/agentdesk/budget.ex)

<!-- tabs-close -->

> Send Anna a message over HTTP.
> {: .step}

```sh
$ curl -si -d 'Hello from the web' localhost:8080/sessions/anna/messages
```

```output
HTTP/1.1 202 Accepted
...
sent to anna
```

> Send Carol a message. She has no session yet.
> {: .step}

```sh
$ curl -s -d 'Hi, I am Carol' localhost:8080/sessions/carol/messages
$ flz state get carol/reply --namespace agents
```

```output
sent to carol
state get carol/reply:
  change_seq: 1
  state:      {"reply":{"messages":["Hi, I am Carol"]}}
```

The budget is 40 tokens a day.

> Send Dave five messages, two seconds apart, and print each status.
> {: .step}

```sh
$ for text in One Two Three Four Five; do curl -s -o /dev/null -w '%{http_code}\n' -d "$text" localhost:8080/sessions/dave/messages; sleep 2; done
```

```output
202
202
202
202
429
```

```sh
$ flz state get gateway/budget/dave --namespace agents
```

```output
state get gateway/budget/dave:
  change_seq: 4
  state:      {"budget":{"dave":{"day":"2026-09-30","tokens":40}}}
```

## Next

[Run a cluster](run_a_cluster.md) adds a second controller.
