# Sleeping sessions

Most conversations are quiet most of the time: a user reads, thinks, leaves for lunch.
A session that waits for its next message still holds a place on its runner, and with
many sessions that adds up. So an idle session sleeps: it leaves its runner, holding
nothing there, and its next message wakes it, on whichever runner is there, with its
conversation restored.

## Why a session sleeps

A bounded operator that keeps its state sleeps when it is idle, and `agent` keeps its
state, as every operator does unless it opts out. So it declares nothing for this.

How long "idle" is comes from `FLEZHA_PARK_IDLE_MS`, ten minutes by default. The runner
reads it, which is why agentdesk's `compose.yaml` sets it on the runner, to ten seconds
so the pages need not wait. [`configuration.md`](../../configuration.md) lists it.

## Watch a session sleep

> Leave Anna alone for ten seconds, then ask the controller about her session.
> {: .step}

```sh
$ flz operator info anna --namespace agents
```

```output
...
Operator: anna
  Kind: bounded
  Phase: suspended
  Health: suspended
  Runner ID: 
  Group ID: agents
  Uptime (ms): 19510
  Default Input Type: 2
  Waiting callers: 0
  Plugins:
    (none)
```

`suspended` is asleep, and there is no runner: the controller keeps the session, and the
runner keeps nothing of it. Bob's session, idle as long, sleeps too.

## Wake it on another runner

> Restart the runner, so the one that wakes Anna is not the one she fell asleep on.
> {: .step}

```sh
$ docker compose restart runner
```

> Send Anna a message.
> {: .step}

```sh
$ flz operator call anna --namespace agents --text 'Are you still there?'
```

```output
{"text":"Message 4: you said \"Are you still there?\". I am a demo model.","tokens":13}
```

The message woke the session on the new runner, with its three earlier messages, and it
answered as if it had never slept. The time a wake takes counts against the call's
deadline.

```sh
$ flz operator info anna --namespace agents
```

```output
...
Operator: anna
  Kind: bounded
  Phase: running
  Health: healthy
  Runner ID: agents_b58101d77c4c
  Group ID: agents
  Uptime (ms): 26284
  Default Input Type: 2
  Waiting callers: 0
  Plugins:
    (none)
```

Ten seconds from now she sleeps again.

## Next

[`rewinding_a_turn.md`](rewinding_a_turn.md) takes turns back, and lets a turn fail
without ending the session.
