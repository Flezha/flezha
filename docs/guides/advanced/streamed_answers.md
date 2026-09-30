# Streamed answers

A model writes its answer a token at a time, and a long answer takes a while. Rather
than wait for the whole of it, a reader can watch it grow. On this page the session
sends every token as the model writes it, and you watch a turn stream in from a second
terminal.

## The demo model

The demo model stands in for a model: it builds one canned sentence around the message,
and hands it over a token at a time, with a short pause for each, the way a model's
client streams its tokens. It is the file you copied from the example on
[`agent_session.md`](agent_session.md#answer-a-message).

<!-- tabs-open -->

### Java

```java
// DemoModel.java: a canned answer, one token at a time.
final class DemoModel {

    private static final int MAX_MESSAGE = 200;
    private static final Duration PER_TOKEN = Duration.ofMillis(50);

    static Answer answer(List<String> earlier, String message, Consumer<Answer> part) throws TaskletException {
        if (message.length() > MAX_MESSAGE) {
            throw new TaskletException.NonRetryable(
                    "the demo model reads at most " + MAX_MESSAGE + " characters, and this message has " + message.length());
        }
        String text = "Message " + (earlier.size() + 1) + ": you said \"" + message + "\". I am a demo model.";
        String[] tokens = text.split("(?<= )");
        for (int i = 0; i < tokens.length; i++) {
            part.accept(new Answer(tokens[i], i + 1));
            LockSupport.parkNanos(PER_TOKEN.toNanos());
        }
        return new Answer(text, tokens.length);
    }

    private DemoModel() {}
}
```

`reply` passes `ctx::part` as `part`, so every token leaves as a part while `run` is
still running, and the whole answer is returned at the end. A real model's client would
be called here instead, handing each token it streams to `part` the same way. The
message limit is for [`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails).
[`java_sdk.md`](https://github.com/flezha/flezha/blob/main/docs/sdk/java_sdk.md#tasklets) has the details of parts.

### Rust

```rust
// src/demo_model.rs: a canned answer, one token at a time.
const MAX_MESSAGE: usize = 200;
const PER_TOKEN: Duration = Duration::from_millis(50);

pub fn answer(
    earlier: &[String],
    message: &str,
    part: impl Fn(Answer) -> Result<(), TaskletError>,
) -> Result<Answer, TaskletError> {
    let length = message.chars().count();
    if length > MAX_MESSAGE {
        return Err(TaskletError::NonRetryable(format!(
            "the demo model reads at most {MAX_MESSAGE} characters, and this message has {length}"
        )));
    }
    let text = format!(
        "Message {}: you said \"{message}\". I am a demo model.",
        earlier.len() + 1
    );
    let tokens: Vec<&str> = text.split_inclusive(' ').collect();
    for (i, token) in tokens.iter().enumerate() {
        part(Answer {
            text: token.to_string(),
            tokens: i as u32 + 1,
        })?;
        std::thread::sleep(PER_TOKEN);
    }
    Ok(Answer {
        tokens: tokens.len() as u32,
        text,
    })
}
```

`reply` passes `|part| ctx.part(part)` as `part`, so every token leaves as a part while
`run` is still running, and the whole answer is returned at the end. A real model's
client would be called here instead, handing each token it streams to `part` the same
way. The message limit is for [`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails).
The Rust SDK's [README](https://github.com/flezha/flezha_rust_sdk/blob/main/README.md#writing-a-tasklet)
has the details of parts.

### Node

```javascript
// demo_model.mjs: a canned answer, one token at a time.
const MAX_MESSAGE = 200;
const PER_TOKEN_MS = 50;

export async function answer(earlier, message, part) {
  if (message.length > MAX_MESSAGE) {
    const refusal = `the demo model reads at most ${MAX_MESSAGE} characters, and this message has ${message.length}`;
    throw Object.assign(new Error(refusal), { nonRetryable: true });
  }
  const text = `Message ${earlier.length + 1}: you said "${message}". I am a demo model.`;
  const tokens = text.split(/(?<= )/);
  for (const [i, token] of tokens.entries()) {
    part({ text: token, tokens: i + 1 });
    await sleep(PER_TOKEN_MS);
  }
  return { text, tokens: tokens.length };
}
```

`reply` hands the model a function that passes each token to `ctx.part`, so every token
leaves as a part while `run` is still running, and the whole answer is returned at the
end. A real model's client would be called here instead, handing each token it streams
to `part` the same way. The pause is awaited, so it holds nothing: the runner's event
loop serves every other session meanwhile. The message limit is for
[`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails), and its error carries
`nonRetryable: true`, so it is not tried again.
[`node_sdk.md`](https://github.com/flezha/flezha/blob/main/docs/sdk/node_sdk.md#writing-an-operator) has the details of parts.

### Python

```python
# demo_model.py: a canned answer, one token at a time.
MAX_MESSAGE = 200
PER_TOKEN_S = 0.05


class Answer(TypedDict):  # an answer, or a part of one
    text: str
    tokens: int


def answer(earlier: list[str], message: str, part: Callable[[Answer], None]) -> Answer:
    if len(message) > MAX_MESSAGE:
        raise flezha.NonRetryableError(
            f"the demo model reads at most {MAX_MESSAGE} characters, "
            f"and this message has {len(message)}"
        )
    text = f'Message {len(earlier) + 1}: you said "{message}". I am a demo model.'
    tokens = re.split(r"(?<= )", text)
    for i, token in enumerate(tokens, start=1):
        part(Answer(text=token, tokens=i))
        time.sleep(PER_TOKEN_S)
    return Answer(text=text, tokens=len(tokens))
```

`reply` passes `ctx.part` as `part`, so every token leaves as a part while `run` is still
running, and the whole answer is returned at the end. A real model's client would be
called here instead, handing each token it streams to `part` the same way. The pause
holds one of the runner's worker threads for the length of the answer. The message limit
is for [`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails).
[`python_sdk.md`](https://github.com/flezha/flezha/blob/main/docs/sdk/python_sdk.md#authoring) has the details of parts.

### Ruby

```ruby
# demo_model.rb: a canned answer, one token at a time.
module DemoModel
  MAX_MESSAGE = 200
  PER_TOKEN_S = 0.05

  def self.answer(earlier, message)
    if message.length > MAX_MESSAGE
      raise Flezha::NonRetryableError,
            "the demo model reads at most #{MAX_MESSAGE} characters, and this message has #{message.length}"
    end

    text = "Message #{earlier.size + 1}: you said \"#{message}\". I am a demo model."
    tokens = text.split(/(?<= )/)
    tokens.each.with_index(1) do |token, i|
      yield({ text: token, tokens: i })
      sleep PER_TOKEN_S
    end
    { text: text, tokens: tokens.size }
  end
end
```

`reply`'s block passes each token to `ctx.part`, so every token leaves as a part while
`run` is still running, and the whole answer is returned at the end. A real model's
client would be called here instead, handing each token it streams to the block the same
way. The pause holds one of the runner's worker threads for the length of the answer.
The message limit is for [`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails).
[`ruby_sdk.md`](https://github.com/flezha/flezha/blob/main/docs/sdk/ruby_sdk.md#authoring) has the details of parts.

### PHP

```php
// src/DemoModel.php: a canned answer, one token at a time.
final class DemoModel
{
    private const MAX_MESSAGE = 200;
    private const PER_TOKEN_US = 50_000;

    public static function answer(array $earlier, string $message, callable $part): array
    {
        $length = mb_strlen($message);
        if ($length > self::MAX_MESSAGE) {
            throw new NonRetryableException(
                'the demo model reads at most ' . self::MAX_MESSAGE . " characters, and this message has $length",
            );
        }
        $text = 'Message ' . (count($earlier) + 1) . ": you said \"$message\". I am a demo model.";
        $tokens = preg_split('/(?<= )/', $text);
        foreach ($tokens as $i => $token) {
            $part(['text' => $token, 'tokens' => $i + 1]);
            usleep(self::PER_TOKEN_US);
        }

        return ['text' => $text, 'tokens' => count($tokens)];
    }
}
```

`reply` passes `$ctx->part(...)` as `$part`, so every token leaves as a part while `run`
is still running, and the whole answer is returned at the end. A real model's client
would be called here instead, handing each token it streams to `$part` the same way. The
pause holds the runner, which serves every session on one thread, for the length of the
answer. The message limit is for [`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails).
[`php_sdk.md`](https://github.com/flezha/flezha/blob/main/docs/sdk/php_sdk.md#authoring) has the details of parts.

### Elixir

```elixir
# lib/agentdesk/demo_model.ex: a canned answer, one token at a time.
defmodule Agentdesk.DemoModel do
  defmodule Answer do
    # Written as JSON with its text first.
    @derive {Jason.Encoder, only: [:text, :tokens]}
    defstruct [:text, :tokens]
  end

  @max_message 200
  @per_token_ms 50

  def answer(earlier, message, part) do
    characters = String.length(message)

    if characters > @max_message do
      {:error,
       {:non_retryable,
        "the demo model reads at most #{@max_message} characters, and this message has #{characters}"}}
    else
      text = ~s(Message #{length(earlier) + 1}: you said "#{message}". I am a demo model.)
      tokens = Regex.split(~r/(?<= )/, text)

      for {token, i} <- Enum.with_index(tokens, 1) do
        part.(%Answer{text: token, tokens: i})
        Process.sleep(@per_token_ms)
      end

      {:ok, %Answer{text: text, tokens: length(tokens)}}
    end
  end
end
```

`reply` passes `&part(ctx, &1)` as `part`, so every token leaves as a part while `run/3`
is still running, and the whole answer is returned at the end. A real model's client
would be called here instead, handing each token it streams to `part` the same way. Each
session runs in a process of its own, so the pause holds only this one. The message
limit is for [`rewinding_a_turn.md`](rewinding_a_turn.md#a-turn-that-fails), and its
refusal is `{:error, {:non_retryable, reason}}`, so it is not tried again. `Answer`
derives its JSON encoder with its fields in order, so the text comes first.
[`elixir_sdk.md`](https://github.com/flezha/flezha/blob/main/docs/sdk/elixir_sdk.md#the-behaviours-beneath) has the details of parts.

<!-- tabs-close -->

## What a part is

A part is a preview of the output a tasklet is producing. It leaves at once, on the
tasklet's output channel, and reaches only the session's listeners: not the sink, not the
caller, and nothing about it is kept. With nobody listening, it is dropped. Parts are
numbered from 0 in each attempt, so a turn that is tried again starts its preview again.

A tasklet can send parts only when its output channel ends in a sink, which is why
`reply`'s channel is `answer`. The finished answer then leaves on that sink as usual.
Your SDK's reference page, linked at the end of your language's tab above, has the
details.

## Watch a turn stream

> In a second terminal, from the project's folder with the `flz` alias of
> [`agent_session.md`](agent_session.md#start-it), listen to Anna's session for five
> seconds.
> {: .step}

```sh
$ flz operator listen anna --namespace agents --duration-ms 5000
```

> In the first terminal, send Anna a message.
> {: .step}

```sh
$ flz operator call anna --namespace agents --text 'Tell me a story'
```

```output
{"text":"Message 3: you said \"Tell me a story\". I am a demo model.","tokens":13}
```

The caller gets the finished answer, once. The listener sees every token as it was sent,
then the answer on the sink:

```output
answer	{"text":"Message ","tokens":1}	part=0 attempt=1 cell=anna/reply/
answer	{"text":"3: ","tokens":2}	part=1 attempt=1 cell=anna/reply/
answer	{"text":"you ","tokens":3}	part=2 attempt=1 cell=anna/reply/
answer	{"text":"said ","tokens":4}	part=3 attempt=1 cell=anna/reply/
answer	{"text":"\"Tell ","tokens":5}	part=4 attempt=1 cell=anna/reply/
answer	{"text":"me ","tokens":6}	part=5 attempt=1 cell=anna/reply/
answer	{"text":"a ","tokens":7}	part=6 attempt=1 cell=anna/reply/
answer	{"text":"story\". ","tokens":8}	part=7 attempt=1 cell=anna/reply/
answer	{"text":"I ","tokens":9}	part=8 attempt=1 cell=anna/reply/
answer	{"text":"am ","tokens":10}	part=9 attempt=1 cell=anna/reply/
answer	{"text":"a ","tokens":11}	part=10 attempt=1 cell=anna/reply/
answer	{"text":"demo ","tokens":12}	part=11 attempt=1 cell=anna/reply/
answer	{"text":"model.","tokens":13}	part=12 attempt=1 cell=anna/reply/
answer	{"text":"Message 3: you said \"Tell me a story\". I am a demo model.","tokens":13}	cell=anna/reply/ seq=2.0
```

Each part line is the channel, the part (one token, and the tokens so far), and
`part=<n> attempt=<a>`. The last line is the output, which carries its place in the
session's record, `seq`, instead. [`http_gateway.md`](http_gateway.md) and
[`streaming_to_the_browser.md`](streaming_to_the_browser.md) send the same frames to a
browser.

## Next

[`sleeping_sessions.md`](sleeping_sessions.md) lets a session that nobody talks to sleep.
