# Call the session

On this page you send messages to sessions over HTTP. The gateway calls the
user's session with `call`, which starts the session if it does not run yet,
and keeps each user's daily token budget in a cell per user, fed by the tokens
each answer used. [Relay calls](../sending_and_calling.html#calls) explains
`call`, and [State](../state.html) the cell per key.

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
            // A message to admit: hand on its verdict.
            case Sent sent -> ctx.emit(new Verdict(sent.message(), ctx.state().on(today)));
            // The tokens an answer used: count them.
            case Used used -> ctx.change(new Counted(today, used.tokens()));
        }
    }
}
```

[`Budget.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/Budget.java)

### Rust

```rust
// src/budget.rs: each user's tokens today, and the verdict
pub const DAILY_TOKENS: u32 = 40;
const DAY_MS: u64 = 86_400_000;

/// What reaches a user's cell: a message to admit, or the tokens an answer used.
#[derive(Clone)]
pub enum Event {
    Sent(Message),
    Used { user: String, tokens: u32 },
}

impl Event {
    pub fn user(&self) -> &str {
        match self {
            Event::Sent(message) => &message.user,
            Event::Used { user, .. } => user,
        }
    }

    // A failed turn used no tokens.
    pub fn used(response: Response<Message, Answer>) -> Self {
        Event::Used {
            user: response.request.user,
            tokens: response.result.map_or(0, |answer| answer.tokens),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Verdict {
    pub message: Message,
    pub spent: u32,
}

impl Verdict {
    pub fn admitted(&self) -> bool {
        self.spent < DAILY_TOKENS
    }
}

/// What each user's cell remembers: the day, counted in days since 1970 in UTC, and its tokens.
#[derive(Default, Serialize, Deserialize)]
pub struct Spent {
    day: u64,
    tokens: u32,
}

impl Spent {
    fn on(&self, today: u64) -> u32 {
        if today == self.day {
            self.tokens
        } else {
            0
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Counted {
    day: u64,
    tokens: u32,
}

impl Change<Spent> for Counted {
    fn apply_to(&self, spent: &mut Spent) {
        spent.tokens = spent.on(self.day) + self.tokens;
        spent.day = self.day;
    }
}

#[derive(Default)]
pub struct Budget;

#[tasklet(name = "budget")]
impl Tasklet for Budget {
    type Input = Event;
    type Output = Verdict;
    type State = Spent;
    type Change = Counted;

    fn run(&mut self, event: Event, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let today = ctx.now() / DAY_MS;
        match event {
            // A message to admit: hand on its verdict.
            Event::Sent(message) => ctx.emit(Verdict {
                message,
                spent: ctx.state().on(today),
            }),
            // The tokens an answer used: count them.
            Event::Used { tokens, .. } => {
                ctx.change(Counted { day: today, tokens });
                Ok(())
            }
        }
    }
}
```

[`src/budget.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/agentdesk/src/budget.rs)

### Node

```javascript
// budget.mjs: each user's tokens today, and the verdict
export const DAILY_TOKENS = 40;

export const admitted = (verdict) => verdict.spent < DAILY_TOKENS;

// The tokens an answer used; a failed turn used none.
export const used = (response) => ({
  user: response.request.user,
  tokens: response.ok ? response.value.tokens : 0,
});

const on = (spent, today) => (spent.day === today ? spent.tokens : 0);

export const Budget = defineTasklet({
  name: 'budget',

  initState: () => ({ day: '', tokens: 0 }), // what each user's cell remembers

  applyChange: (spent, counted) => {
    spent.tokens = on(spent, counted.day) + counted.tokens;
    spent.day = counted.day;
  },

  run: (event, ctx) => {
    const today = new Date(Number(ctx.now())).toISOString().slice(0, 10);
    if ('tokens' in event) {
      // The tokens an answer used: count them.
      ctx.change({ day: today, tokens: event.tokens });
    } else {
      // A message to admit: hand on its verdict.
      ctx.emit({ message: event, spent: on(ctx.state, today) });
    }
  },
});
```

[`budget.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/agentdesk/budget.mjs)

### Python

```python
# budget.py: each user's tokens today, and the verdict
DAILY_TOKENS = 40


class Used(TypedDict):  # the tokens an answer used
    user: str
    tokens: int


Event = Message | Used  # what reaches a user's cell


class Verdict(TypedDict):
    message: Message
    spent: int


@flezha.state
@dataclass
class Spent:  # what each user's cell remembers
    day: str = ""
    tokens: int = 0

    def on(self, today: str) -> int:
        return self.tokens if today == self.day else 0


class Counted(TypedDict):  # how an answer's tokens are written down
    day: str
    tokens: int


def admitted(verdict: Verdict) -> bool:
    return verdict["spent"] < DAILY_TOKENS


def sent(request: Request) -> Event:  # a message to admit
    return request.message()


def used(response: flezha.Response[Message, Answer]) -> Event:  # a failed turn used none
    tokens = 0 if response.value is None else response.value["tokens"]
    return Used(user=response.request["user"], tokens=tokens)


@flezha.tasklet(name="budget")
class Budget:
    def run(self, event: Event, ctx: flezha.Ctx[Spent, Verdict]) -> None:
        today = time.strftime("%Y-%m-%d", time.gmtime(ctx.now() // 1000))
        if "tokens" in event:  # the tokens an answer used: count them
            ctx.change(Counted(day=today, tokens=event["tokens"]))
        else:  # a message to admit: hand on its verdict
            ctx.emit(Verdict(message=event, spent=ctx.state.on(today)))

    def apply_change(self, spent: Spent, counted: Counted) -> None:
        spent.tokens = spent.on(counted["day"]) + counted["tokens"]
        spent.day = counted["day"]
```

[`budget.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/agentdesk/budget.py)

### Ruby

```ruby
# budget.rb: each user's tokens today, and the verdict
class Budget < Flezha::Tasklet
  tasklet_name "budget"

  DAILY_TOKENS = 40

  def self.admitted?(verdict) = verdict[:spent] < DAILY_TOKENS

  def self.used(response)                                    # a failed turn used none
    { user: response.request[:user], tokens: response.ok? ? response.value[:tokens] : 0 }
  end

  def init_state(_user) = { day: "", tokens: 0 }             # what each user's cell remembers

  def apply_change(spent, counted)
    spent[:tokens] = on(spent, counted[:day]) + counted[:tokens]
    spent[:day] = counted[:day]
  end

  def run(event, ctx)
    today = Time.at(ctx.now / 1000, in: "UTC").strftime("%F")
    if event.key?(:tokens)                                   # the tokens an answer used: count them
      ctx.change({ day: today, tokens: event[:tokens] })
    else                                                     # a message to admit: hand on its verdict
      ctx.emit({ message: event, spent: on(ctx.state, today) })
    end
  end

  private

  def on(spent, today) = spent[:day] == today ? spent[:tokens] : 0
end
```

[`budget.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/agentdesk/budget.rb)

### PHP

```php
// src/Budget.php: each user's tokens today, and the verdict
#[Tasklet\Spec(name: 'budget')]
final class Budget extends Tasklet
{
    public const DAILY_TOKENS = 40;

    /** @param array{message: array{user: string, text: string}, spent: int} $verdict */
    public static function admitted(array $verdict): bool
    {
        return $verdict['spent'] < self::DAILY_TOKENS;
    }

    /** @return array{user: string, tokens: int} */
    public static function used(Response $response): array    // a failed turn used none
    {
        return [
            'user' => $response->request['user'],
            'tokens' => $response->ok() ? $response->value['tokens'] : 0,
        ];
    }

    public function initState(?string $cellKey): mixed
    {
        return ['day' => '', 'tokens' => 0];         // what each user's cell remembers
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state = ['day' => $change['day'], 'tokens' => self::on($state, $change['day']) + $change['tokens']];
    }

    public function run(mixed $input, Ctx $ctx): void
    {
        $today = gmdate('Y-m-d', intdiv($ctx->now(), 1000));
        if (isset($input['tokens'])) {               // the tokens an answer used: count them
            $ctx->change(['day' => $today, 'tokens' => $input['tokens']]);
        } else {                                     // a message to admit: hand on its verdict
            $ctx->emit(['message' => $input, 'spent' => self::on($ctx->state, $today)]);
        }
    }

    /** @param array{day: string, tokens: int} $spent */
    private static function on(array $spent, string $today): int
    {
        return $spent['day'] === $today ? $spent['tokens'] : 0;
    }
}
```

[`src/Budget.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/agentdesk/src/Budget.php)

### Elixir

```elixir
# lib/agentdesk/budget.ex: each user's tokens today, and the verdict
defmodule Agentdesk.Budget do
  use Flezha.SDK.Tasklet, name: "budget"

  @daily_tokens 40

  def daily_tokens, do: @daily_tokens

  def admitted?(verdict), do: verdict.spent < @daily_tokens

  # The tokens an answer used; a failed turn used none.
  def used(%Response{request: request, result: result}) do
    %{user: Jason.decode!(request)["user"], tokens: tokens(result)}
  end

  defp tokens({:ok, answer}), do: answer["tokens"]
  defp tokens({:error, _error}), do: 0

  # What each user's cell remembers.
  state do
    field(:day, :string, default: "")
    field(:tokens, :integer, default: 0)
  end

  defchange counted(spent, day, tokens) do
    %{spent | day: day, tokens: on(spent, day) + tokens}
  end

  # The tokens an answer used: count them.
  @impl true
  def run(%{tokens: tokens}, spent, ctx), do: {:ok, counted(spent, ctx, today(ctx), tokens)}

  # A message to admit: hand on its verdict.
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

### Go

```go
// budget.go: each user's tokens today, and the verdict
const dailyTokens = 40

// Event is what reaches a user's cell: a message to admit, or the tokens an
// answer used.
type Event struct {
	User    string   `json:"user"`
	Message *Message `json:"message,omitempty"`
	Tokens  int      `json:"tokens,omitempty"`
}

// Sent is a message to admit.
func Sent(request Request) Event {
	message := request.Message()
	return Event{User: message.User, Message: &message}
}

// Used is the tokens a session's answer used; a failed turn used none.
func Used(response flezha.Response[Message, Answer]) Event {
	event := Event{User: response.Request.User}
	if response.Ok() {
		event.Tokens = response.Value.Tokens
	}
	return event
}

type Verdict struct {
	Message Message `json:"message"`
	Spent   int     `json:"spent"`
}

func (v Verdict) Admitted() bool { return v.Spent < dailyTokens }

// Spent is what each user's cell remembers: the day, in UTC, and its tokens.
type Spent struct {
	Day    string `json:"day"`
	Tokens int    `json:"tokens"`
}

func (s Spent) on(today string) int {
	if today == s.Day {
		return s.Tokens
	}
	return 0
}

type Counted struct {
	Day    string `json:"day"`
	Tokens int    `json:"tokens"`
}

func (c Counted) ApplyTo(s *Spent) {
	s.Tokens = s.on(c.Day) + c.Tokens
	s.Day = c.Day
}

type Budget struct{}

func (Budget) Spec() flezha.Spec { return flezha.Spec{Name: "budget"} }

func (Budget) Changes() flezha.Changes[Spent] {
	return flezha.Changes[Spent]{"counted": Counted{}}
}

func (Budget) Run(event Event, ctx *flezha.Ctx[Spent, Verdict]) error {
	today := time.UnixMilli(int64(ctx.Now())).UTC().Format(time.DateOnly)
	if event.Message != nil {
		// A message to admit: hand on its verdict.
		return ctx.Emit(Verdict{Message: *event.Message, Spent: ctx.State().on(today)})
	}
	// The tokens an answer used: count them.
	return ctx.Change(Counted{Day: today, Tokens: event.Tokens})
}
```

[`budget.go`](https://github.com/flezha/flezha_go_sdk/blob/main/examples/agentdesk/budget.go)

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
