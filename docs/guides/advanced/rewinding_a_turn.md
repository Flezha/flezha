# Rewinding a turn

A user edits a message they sent a while ago, or an answer went wrong and should be
forgotten: the conversation goes back to an earlier turn and carries on from there. And
sometimes a turn fails, because the model refused or timed out, and only that turn should
be lost, not the conversation. This page does both to Anna's session.

## Every turn is an iteration

Each message a session runs is one **iteration**, numbered from 0. Anna's session has run
four, and `reply`'s state holds their messages:

> Read Anna's conversation.
> {: .step}

```sh
$ flz state get anna/reply --namespace agents
```

```output
state get anna/reply:
  change_seq: 4
  state:      {"reply":{"messages":["Hello","What can you do?","Tell me a story","Are you still there?"]}}
```

## Take turns back

`flz operator rewind` takes a session back to an iteration: it keeps that iteration and
every earlier one, and undoes every later one, newest first, through each tasklet's
`compensate`. The messages it undoes are not run again; the session waits for its next
message.

<!-- tabs-open -->

### Java

`reply`'s undo, from [`agent_session.md`](agent_session.md#answer-a-message), forgets the
last message of the conversation:

```java
    // The undo, when a turn is taken back: the conversation forgets its message.
    @Override
    public void compensate(List<Answer> sent, Ctx<Conversation, Answer> ctx) {
        System.out.println("[undo] forgot \"" + ctx.state().messages.getLast() + "\"");
        ctx.change(new Unsaid());
    }
```

`deliver`'s is empty: an answer already given has nothing to take back.

### Rust

`reply`'s undo, from [`agent_session.md`](agent_session.md#answer-a-message), forgets the
last message of the conversation:

```rust
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
```

`deliver`'s is empty: an answer already given has nothing to take back.

### Node

`reply`'s undo, from [`agent_session.md`](agent_session.md#answer-a-message), forgets the
last message of the conversation:

```javascript
  // The undo, when a turn is taken back: the conversation forgets its message.
  compensate: (_answers, ctx) => {
    console.log(`[undo] forgot "${ctx.state.messages.at(-1)}"`);
    ctx.change({ kind: 'unsaid' });
  },
```

`deliver`'s is empty: an answer already given has nothing to take back.

### Python

`reply`'s undo, from [`agent_session.md`](agent_session.md#answer-a-message), forgets the
last message of the conversation:

```python
    # The undo, when a turn is taken back: the conversation forgets its message.
    def compensate(self, outputs: list[Answer], ctx: flezha.Ctx[Conversation, Answer]) -> None:
        print(f'[undo] forgot "{ctx.state.messages[-1]}"', flush=True)
        ctx.change(Edit(kind="unsaid"))
```

`deliver`'s does nothing: an answer already given has nothing to take back.

### Ruby

`reply`'s undo, from [`agent_session.md`](agent_session.md#answer-a-message), forgets the
last message of the conversation:

```ruby
  # The undo, when a turn is taken back: the conversation forgets its message.
  def compensate(_answers, ctx)
    puts "[undo] forgot \"#{ctx.state[:messages].last}\""
    ctx.change({ kind: "unsaid" })
  end
```

`deliver`'s is empty: an answer already given has nothing to take back.

### PHP

`reply`'s undo, from [`agent_session.md`](agent_session.md#answer-a-message), forgets the
last message of the conversation:

```php
    // The undo, when a turn is taken back: the conversation forgets its message.
    public function compensate(array $outputs, Ctx $ctx): void
    {
        echo '[undo] forgot "' . end($ctx->state['messages']) . "\"\n";
        $ctx->change(['kind' => 'unsaid']);
    }
```

`deliver`'s is empty: an answer already given has nothing to take back.

<!-- tabs-close -->

A rewind asks every tasklet whose output left the session, through a sink or a
`response()` end, for its undo, and is refused with `operator.rewind_uncovered` when one
has none. That is why `deliver` declares an empty one.

> Take Anna's session back to iteration 1, keeping her first two messages.
> {: .step}

```sh
$ flz operator rewind anna --to-iteration 1 --namespace agents
```

```output
Operator 'anna' rewound to iteration 1. Iterations above it were compensated; the loop resumes at 2.
```

The runner printed the two undos, newest first:

```sh
$ docker compose logs runner | grep undo
```

```output
runner-1  | [undo] forgot "Are you still there?"
runner-1  | [undo] forgot "Tell me a story"
```

```sh
$ flz state get anna/reply --namespace agents
```

```output
state get anna/reply:
  change_seq: 6
  state:      {"reply":{"messages":["Hello","What can you do?"]}}
```

> Send Anna a message.
> {: .step}

```sh
$ flz operator call anna --namespace agents --text 'Where were we?'
```

```output
{"text":"Message 3: you said \"Where were we?\". I am a demo model.","tokens":12}
```

## A turn that fails

`agent` declares the rollback mode that keeps progress
([`agent_session.md`](agent_session.md#a-session-is-a-bounded-operator)). With it, a turn
whose tasklet fails, once its retries are spent or at once for an error that is not worth
retrying, fails alone: its message goes to the dead-letter queue, the turn's tasklets are
undone, and the session takes its next message. Without it, a failed turn ends the whole
session and undoes every turn, as a failed step ended an order in
[`building_a_workflow.md`](../tradedesk/building_a_workflow.md).

The demo model reads at most 200 characters, and refuses a longer message with an error
that is not worth retrying.

> Send Anna a message of 250 characters.
> {: .step}

```sh
$ flz operator call anna --namespace agents --text "$(printf '%0250d' 0)"
```

```output
error: operator 'anna' dead-lettered the call's input as 'anna/_turn/b7b60e19e485759b': tasklet `reply` failed after 1 attempt(s): non-retryable tasklet error: the demo model reads at most 200 characters, and this message has 250 (call.failed)
hint: flz dlq list anna
```

```sh
$ flz operator info anna --namespace agents
```

```output
...
Operator: anna
  Kind: bounded
  Phase: running
  Health: healthy
  Failed turns: 1 (last: iteration 3)
  Runner ID: agents_b58101d77c4c
  Group ID: agents
  Uptime (ms): 29266
  Default Input Type: 2
  Waiting callers: 0
  Plugins:
    (none)
```

```sh
$ flz dlq list anna --namespace agents
```

```output
ENTRY                                                       CELL            CHANNEL     GEN  AGE       ERROR
anna/_turn/b7b60e19e485759b                                 _turn           turn        1    0s        tasklet `reply` failed after 1 attempt(s): non-retryable tas
```

The message is kept, under the session's `_turn` slot, and
[`durability.md`](../tradedesk/durability.md#the-dead-letter-queue) shows how to read,
replay or drop it. A replay sends it to the session again as a new message.

> Send Anna another message.
> {: .step}

```sh
$ flz operator call anna --namespace agents --text 'Sorry, go on'
```

```output
{"text":"Message 4: you said \"Sorry, go on\". I am a demo model.","tokens":12}
```

The session went on, and the failed message is not part of the conversation.

## Next

[`custom_plugins.md`](custom_plugins.md) installs the HTTP plugin into the controller's
image.
