# A token budget

A model is paid for by the token, so a chat usually gives each user a budget. On this
page the gateway keeps each user's tokens for the day, from what each answer reports,
and refuses a message with 429 once the day's budget is used.

## Count the tokens

Every answer says how many tokens it used, and the gateway hears of every answer: its
call to the session, from [`http_gateway.md`](http_gateway.md#the-gateway), comes back
as a `Response` carrying the session's answer. So the tokens are counted where the
messages are admitted, in `budget`, one cell per user.

> Write the tasklet that keeps the budget.
> {: .step}

<!-- tabs-open -->

### Java

```java
// Budget.java: each user's tokens for the day, and whether a message is admitted.
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

A tasklet takes one type of input, and `budget` reads two things: the messages, and the
tokens their answers used. So its input is `Event`, which is either, and the gateway's
graph maps each into it on the way in and keys both by user, so a user's messages and
answers reach the same cell. A `Response` carries the request it answers, which is
where the user comes from, and a turn that failed, or took too long, is counted as using
no tokens.

### Rust

```rust
// src/budget.rs: each user's tokens for the day, and whether a message is admitted.
pub const DAILY_TOKENS: u32 = 40;
const DAY_MS: u64 = 86_400_000;

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
            Event::Sent(message) => ctx.emit(Verdict {
                message,
                spent: ctx.state().on(today),
            }),
            Event::Used { tokens, .. } => {
                ctx.change(Counted { day: today, tokens });
                Ok(())
            }
        }
    }
}
```

A tasklet takes one type of input, and `budget` reads two things: the messages, and the
tokens their answers used. So its input is `Event`, an enum which is either, and the
gateway's graph maps each into it on the way in and keys both by user, so a user's
messages and answers reach the same cell. A `Response` carries the request it answers,
which is where the user comes from, and a turn that failed, or took too long, has an
error for its `result` and is counted as using no tokens. The day is counted in days
since 1970, as the standard library has no calendar, so `flz state get` prints it as a
number.

### Python

```python
# budget.py: each user's tokens for the day, and whether a message is admitted.
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
        if "tokens" in event:  # the tokens an answer used
            ctx.change(Counted(day=today, tokens=event["tokens"]))
        else:  # a message to admit
            ctx.emit(Verdict(message=event, spent=ctx.state.on(today)))

    def apply_change(self, spent: Spent, counted: Counted) -> None:
        spent.tokens = spent.on(counted["day"]) + counted["tokens"]
        spent.day = counted["day"]
```

A tasklet takes one type of input, and `budget` reads two things: the messages, and the
tokens their answers used. So its input is `Event`, which is either, told apart by
whether it carries `tokens`, and the gateway's graph lifts each into it on the way in,
with `sent` and `used`, and keys both by user, so a user's messages and answers reach
the same cell. `used` makes the tokens out of the call's `Response`, which carries the
request it answers, where the user comes from; a turn that failed, or took too long,
has no `value` and is counted as using no tokens. `admitted` is the check the graph's
filter and `admit` share.

### PHP

```php
// src/Budget.php: each user's tokens for the day, and whether a message is admitted.
#[Task(name: 'budget')]
final class Budget extends Tasklet
{
    public const DAILY_TOKENS = 40;

    public static function admitted(array $verdict): bool
    {
        return $verdict['spent'] < self::DAILY_TOKENS;
    }

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
        if (isset($input['tokens'])) {               // the tokens an answer used
            $ctx->change(['day' => $today, 'tokens' => $input['tokens']]);
        } else {                                     // a message to admit
            $ctx->emit(['message' => $input, 'spent' => self::on($ctx->state, $today)]);
        }
    }

    private static function on(array $spent, string $today): int
    {
        return $spent['day'] === $today ? $spent['tokens'] : 0;
    }
}
```

A tasklet takes one type of input, and `budget` reads two things: the messages, and the
tokens their answers used. Both are arrays, told apart by whether they carry `tokens`,
and the gateway's graph keys both by user, so a user's messages and answers reach the
same cell. `used` makes the tokens out of the call's `Response`, which carries the
request it answers, where the user comes from; a turn that failed, or took too long,
has an `error` and is counted as using no tokens. `admitted` is the check the graph's
filter and `admit` share.

<!-- tabs-close -->

The state is the day, in UTC, and the tokens used on it; the first answer of a new day
starts again from zero. A message is admitted while the user is under the budget,
checked when the message arrives. Tokens are counted when an answer arrives, so a
message sent while the previous answer is still being written is admitted on the count
before it: the budget can be overrun by the answers in flight, never by more.

## Use up the budget

The budget is 40 tokens a day, and an answer to a one-word message uses 10.

> Send Dave four messages over HTTP, two seconds apart, and print each status.
> {: .step}

```sh
$ for text in One Two Three Four; do curl -s -o /dev/null -w '%{http_code}\n' -d "$text" localhost:8080/sessions/dave/messages; sleep 2; done
```

```output
202
202
202
202
```

> Send him a fifth.
> {: .step}

```sh
$ curl -si -d 'Five' localhost:8080/sessions/dave/messages
```

```output
HTTP/1.1 429 Too Many Requests
date: Wed, 30 Sep 2026 05:22:23 GMT
content-length: 37
vary: accept-encoding
cache-control: max-age=0, private, must-revalidate
content-type: text/plain; charset=utf-8

dave has used 40 of today's 40 tokens
```

The message did not reach Dave's session. Dave's cell of `budget` holds the count:

```sh
$ flz state get gateway/budget/dave --namespace agents
```

```output
state get gateway/budget/dave:
  change_seq: 9
  state:      {"budget":{"dave":{"day":"2026-09-30","tokens":40}}}
```

> Send Dave's session a message from the CLI.
> {: .step}

```sh
$ flz operator call dave --namespace agents --text 'Still there?'
```

```output
{"text":"Message 5: you said \"Still there?\". I am a demo model.","tokens":11}
```

The budget is the gateway's: a message that does not come through the gateway is not
counted, and not refused. The session itself is untouched, and a message over HTTP will
be admitted again tomorrow.

The count is the gateway's state, kept by the controller. With
`FLEZHA_DURABILITY_BACKEND: none`, as agentdesk runs, it stays on this controller's
volume; [`http_server.md`](../../plugins/http_server.md#deployment) says what that means
once there are several controllers, which is the next page.

## Next

[`clustering.md`](clustering.md) runs several controllers, stops one, and watches its
sessions move.
