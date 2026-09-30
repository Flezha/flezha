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

<!-- tabs-close -->

## What a part is

A part is a preview of the output a tasklet is producing. It leaves at once, on the
tasklet's output channel, and reaches only the session's listeners: not the sink, not the
caller, and nothing about it is kept. With nobody listening, it is dropped. Parts are
numbered from 0 in each attempt, so a turn that is tried again starts its preview again.

A tasklet can send parts only when its output channel ends in a sink, which is why
`reply`'s channel is `answer`. The finished answer then leaves on that sink as usual.
The Java SDK's reference, `docs/sdk/java_sdk.md`, has the details.

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
