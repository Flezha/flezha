# Rewinding a turn

A user edits a message sent a while ago, or an answer went wrong: the conversation goes back
to an earlier turn and carries on from there. And sometimes a turn fails, because the model
refused or timed out, and only that turn should be lost, not the conversation. This page
does both to Anna's session.

## Every turn is an iteration

Each message a session runs is one **iteration**, numbered from 0. Anna's session has run
three, and `reply`'s state holds their messages.

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

## Add an undo

`flz operator rewind` takes a session back to an iteration: it keeps that iteration and every
earlier one, and undoes every later one, newest first, through each tasklet's `compensate`.
The messages it undoes are not run again. A rewind is refused with
`operator.rewind_uncovered` when a tasklet whose output left the session has no undo, and
`reply`'s output leaves through the `response()` end.

> Give `reply` an undo that forgets the last message of the conversation.
> {: .step}

<!-- tabs-open -->

### Java

In `Reply`, a second change beside `Said`, and the undo after `run`:

```java
    public record Unsaid() implements Change<Conversation> {
        @Override
        public void applyTo(Conversation conversation) {
            conversation.messages.removeLast();
        }
    }

    // The undo, when a turn is taken back: the conversation forgets its message.
    @Override
    public void compensate(List<Answer> sent, Ctx<Conversation, Answer> ctx) {
        System.out.println("[undo] forgot \"" + ctx.state().messages.getLast() + "\"");
        ctx.change(new Unsaid());
    }
```

<!-- tabs-close -->

## A turn that fails

By default a failed turn ends the whole session and undoes every turn, as a failed step
ended an order in [`building_a_workflow.md`](../tradedesk/building_a_workflow.md). A
conversation should lose only the turn that failed: with the failure mode that keeps
progress, a failed turn's message goes to the dead-letter queue, its tasklets are undone,
and the session takes its next message.

> Declare the failure mode that keeps progress.
> {: .step}

<!-- tabs-open -->

### Java

In `AgentOperator`, `run` gains its failure mode:

```java
    @Override
    public Run run() {
        return new Run.Steps(
                Each.PER_INPUT,                          // one turn per message
                OnFailure.KEEP_PROGRESS);                // a failed turn fails alone
    }
```

<!-- tabs-close -->

> Build the runner again and replace its container.
> {: .step}

<!-- tabs-open -->

### Java

```sh
$ mvn -q package
```

<!-- tabs-close -->

```sh
$ docker compose up -d --build --wait runner
```

## Take turns back

> Take Anna's session back to iteration 0, keeping her first message.
> {: .step}

```sh
$ flz operator rewind anna --to-iteration 0 --namespace agents
```

```output
Operator 'anna' rewound to iteration 0. Iterations above it were compensated; the loop resumes at 1.
```

The runner printed the two undos, newest first, and the conversation lost two messages:

```sh
$ docker compose logs runner | grep undo
```

```output
runner-1  | [undo] forgot "Are you still there?"
runner-1  | [undo] forgot "What can you do?"
```

```sh
$ flz state get anna/reply --namespace agents
```

```output
state get anna/reply:
  change_seq: 5
  state:      {"reply":{"messages":["Hello"]}}
```

> Send Anna a message.
> {: .step}

```sh
$ flz operator call anna --namespace agents --text 'Where were we?'
```

```output
{"text":"Message 2: you said \"Where were we?\". I am a demo model.","tokens":12}
```

## Fail a turn

The demo model reads at most 200 characters, and refuses a longer message with an error that
is not worth retrying.

> Send Anna a message of 250 characters.
> {: .step}

```sh
$ flz operator call anna --namespace agents --text "$(printf '%0250d' 0)"
```

```output
error: operator 'anna' dead-lettered the call's input as 'anna/_turn/fd471f7e0b872ddf': tasklet `reply` failed after 1 attempt(s): non-retryable tasklet error: the demo model reads at most 200 characters, and this message has 250 (call.failed)
hint: flz dlq list anna
```

The message is kept in the dead-letter queue, and
[`durability.md`](../tradedesk/durability.md#the-dead-letter-queue) shows how to read, replay
or drop it.

> Send Anna another message.
> {: .step}

```sh
$ flz operator call anna --namespace agents --text 'Sorry, go on'
```

```output
{"text":"Message 3: you said \"Sorry, go on\". I am a demo model.","tokens":12}
```

The session went on, and the failed message is not part of the conversation.

## Next

[`streamed_answers.md`](streamed_answers.md) sends the answer while the model is still
writing it.
