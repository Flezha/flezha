# Effectively once

A message that is lost is a question nobody answers. A message that is handled twice is
an answer given twice, and tokens counted twice. The delivery guarantee is that neither
happens when a runner or a controller dies at the wrong moment: a message may be
delivered again after a crash, but what handling it changes and sends counts once.
Flezha calls it effectively once. On this page you look at where agentdesk keeps what
the guarantee needs, then kill the runner in the middle of a message and count.

## The sessions already have it

`agent` is a bounded operator that keeps its state, and such an operator writes every
message to its input log on the controller's disk before the turn runs, and writes the
end of each turn to its journal. A turn runs to its end once. A crash before the end runs
it again from the log, and the answer leaves only once the turn's end is written.
Nothing in the session's operator declares this.

> Look at Anna's input log and journal.
> {: .step}

```sh
$ flz state inbox anna --namespace agents
```

```output
durable input log for anna (consumed through 11):

  offset=0 consumed 5 byte(s)
  offset=1 consumed 16 byte(s)
  offset=2 consumed 20 byte(s)
  offset=3 consumed 14 byte(s)
  offset=4 consumed 250 byte(s)
  offset=5 consumed 12 byte(s)
  offset=6 consumed 15 byte(s)
  offset=7 consumed 24 byte(s)
  offset=8 consumed 12 byte(s)
  offset=9 consumed 43 byte(s) channel=message
  offset=10 consumed 20 byte(s)
  offset=11 consumed 29 byte(s) channel=message
```

Every message she was sent is there, from the CLI or, on the `message` channel, from the
gateway. The 250-byte one is the message that failed in
[`rewinding_a_turn.md`](rewinding_a_turn.md): it went to the dead-letter queue, which
consumed it too.

```sh
$ flz state journal anna --namespace agents
```

```output
task-journal for instance anna:
...
  seq=17 task_committed task_idx=0 iter=5 epoch=1
    ran: anna/reply/ record=8 channels=answer
    handoff: 1 item(s), 116 byte(s)
      edge: channel_id=1 1 item(s), 116 byte(s)

  seq=18 task_committed task_idx=1 iter=5 epoch=1
    ran: anna/deliver/ record=1 channels=deliver
    handoff: 0 item(s), 0 byte(s)

  seq=19 iter_committed task_idx=0 iter=5 epoch=1
    handoff: 0 item(s), 0 byte(s)
    consumed input-log offset: 7
...
```

A turn is `reply`, then `deliver`, then the turn's end, `iter_committed`, which names the
message it consumed. The early turns, from before [`streamed_answers.md`](streamed_answers.md)
added `deliver`, are `reply` alone, and the rewind and the failed turn are there too.

## The gateway declares it

An unbounded operator keeps no input log unless it asks for one, and without one its
guarantee is at least once: after a crash an output can leave twice, and a message in flight
can be lost. The gateway counts tokens, so it asks.

> Declare the delivery guarantee on the gateway.
> {: .step}

<!-- tabs-open -->

### Java

In `GatewayOperator.channels`, as its first line:

```java
        c.effectivelyOnce();                                 // each request and answer on disk before it runs
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

The gateway now writes each HTTP request to its input log before it runs, and each answer
its calls bring back. A call is effectively once only when both ends keep an input log, the
caller for the answer and the session for the message, and here both do.

> Send Anna a message over HTTP, and look at the gateway's input log.
> {: .step}

```sh
$ curl -s -d 'Are you on disk?' localhost:8080/sessions/anna/messages
```

```output
sent to anna
```

```sh
$ flz state inbox gateway --namespace agents
```

```output
durable input log for gateway (consumed through 2):

  offset=0 consumed 61 byte(s) channel=http
  offset=1 consumed 100 byte(s) channel=http
  offset=2 consumed 257 byte(s) channel=budget.response
```

`http` is a request, and `budget.response` is the answer of a call, on its way to
`budget`, which counts its tokens.

## Kill the runner

The runner runs both Anna's session and the gateway.

> Read what Anna has used today.
> {: .step}

```sh
$ flz state get gateway/budget/anna --namespace agents
```

```output
state get gateway/budget/anna:
  change_seq: 4
  state:      {"budget":{"anna":{"day":"2026-09-30","tokens":36}}}
```

> Send Anna a message, and kill the runner a second later.
> {: .step}

```sh
$ curl -s -d 'Tell me everything' localhost:8080/sessions/anna/messages && sleep 1 && docker compose kill runner
```

The gateway answered `sent to anna`, and then the runner was gone. Whatever was on its way
when the runner died, Anna's turn, the call's answer or the count, is in an input log.

> Start the runner again, and read the budget once Anna has answered.
> {: .step}

```sh
$ docker compose start runner
```

```sh
$ flz state get gateway/budget/anna --namespace agents
```

```output
state get gateway/budget/anna:
  change_seq: 6
  state:      {"budget":{"anna":{"day":"2026-09-30","tokens":48}}}
```

The answer used 12 tokens, and they were counted once.

```sh
$ flz operator call anna --namespace agents --text 'What did I ask?'
```

```output
{"text":"Message 12: you said \"What did I ask?\". I am a demo model.","tokens":13}
```

Her eleventh message was the one sent before the kill, and it is in her conversation once.
The gateway's log shows that request and its answer once each:

```sh
$ flz state inbox gateway --namespace agents
```

```output
...
  offset=2 consumed 257 byte(s) channel=budget.response
  offset=3 consumed 102 byte(s) channel=http
  offset=4 consumed 265 byte(s) channel=budget.response
```

## What it requires

- **An input log on each end** of whatever crosses between operators: a bounded operator
  that keeps its state has one, an unbounded operator declares the guarantee.
  [`calls.md`](../../sdk/calls.md#guarantees) has the table for calls.
- **Tasklets that give the same result for the same input.** A message replayed after a
  crash runs its tasklet again, so a tasklet reads the time from `ctx.now()`, which a
  replay serves again, and never from the clock: `budget` takes the day from it.
- **Keyed sinks.** On an operator that declares it, a channel that ends in a sink plugin
  must be `.keyed()`, so the sink can drop a record it has already written. The gateway
  answers requests and calls, which need no key.
- **The controller's disk.** The logs are on it. The database copy from
  [`clustering.md`](clustering.md) is what carries them past losing that disk, a little
  behind.

## What it costs

Every request and every answer is written and synced to disk before it runs, and the
gateway's answers, the 202 and the 429, leave only once the record behind them is on
disk. So each message waits for a disk write it would not wait for otherwise, and the
input log takes disk space until what it holds is consumed. A slow disk makes a slow
gateway. The sessions pay the same for their own log. An operator that can lose a
message or handle one twice, say one that only shows a preview, does not need to pay it.
[`calls.md`](../../sdk/calls.md#where-an-answer-goes) says when an answer leaves, for each
kind of operator.

## Next

[`kubernetes_deployment.md`](kubernetes_deployment.md) runs the same images on k3s.
