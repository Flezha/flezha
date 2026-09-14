# Introduction

Flezha runs your data processing code continuously, on one machine or across
many. Records come in from somewhere, your code handles each one, and the results
go out somewhere else. You write that code in Java, Rust, Node, Python, Ruby, PHP
or Elixir. Flezha decides which machine runs it, saves what it needs to remember,
and when a process crashes or a machine goes away, carries on from where it
stopped.

For example: an exchange publishes its trades on a WebSocket, and you want each
symbol's total traded volume in a Postgres table, always up to date.

What you write is an **operative**, a short declaration of the path the data
takes: trades enter on a channel, are grouped by symbol, pass through a
**procedure**, and leave on a second channel. The procedure is the only real
logic, a few lines that add each trade to its symbol's total and emit the new
total. The rest is already there:

- **Trades in, totals out.** Two plugins that ship with Flezha,
  [WebSocket](../plugins/websocket_subscription.md) and
  [Postgres](../plugins/postgres_sink.md), attached to those two channels with
  one command each. Writing the totals to [Kafka](../plugins/kafka_producer.md)
  instead is a different command, not a code change.
- **The totals survive a crash.** Each symbol's total is saved as it changes, so
  after a crash it continues from its last value, not from zero.
- **A bad trade does not stop the stream.** A trade your procedure keeps failing
  on is retried, then set aside in a dead-letter queue, where you can inspect it
  and send it through again.

That operative runs until you stop it, which makes it a **stream**. The other
kind is a **workflow**: a job with a start and an end, such as a nightly import,
which `flz schedule` can start on a timetable.

Two kinds of process make that work:

- A **controller** is the brain. It owns the durable state, decides placement,
  heals what breaks, and serves the operator surface.
- A **runner** is a process of yours that executes operative code. It offers
  capacity to a controller and does what it is told.

The guides below build that up in order. **Part 1 gets something running.**
Parts 2 and 3 take it apart one subject at a time, and you can read those in any
order once Part 1 works.

## Part 1: getting started

The shortest path from nothing to data flowing. Read these in order: each one
assumes the one before it, and **uses what that one started, still running**.
Run in the foreground, the controller and your runner each keep a terminal, so
keep a third for `flz`. The last page stops everything.

### First, watch one run

Before building anything, bring up a controller and a runner that is already
offering an operative, and send a message through it:

```sh
cd examples/python/echo
docker compose up -d --wait
docker compose exec controller /flezha/bin/flz operative start my_echo echo
docker compose exec controller /flezha/bin/flz operative send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

[`examples/python/echo`](../../examples/python/echo) is that deployment, and it
runs the same `echo` the guides below build by hand. Seeing the pieces move
first is worth it: the rest of Part 1 is then about what each piece is, rather
than about getting anything to start at all.

**It is a demo and not a deployment.** It makes every choice for you, and the
first thing you change will be one of them, which is what the guides below are
for. It keeps running until you take it down, and the guides start a controller
of their own:

```sh
docker compose down
```

| Guide | What you end up with |
|---|---|
| [`controller_deployment.md`](controller_deployment.md) | A controller running, four ways to choose from, and a check that it is up |
| [`runner_deployment.md`](runner_deployment.md) | Your own runner connected to it, offering an operative class |
| [`running_operatives.md`](running_operatives.md) | An operative started on that runner, taking messages and answering them |
| [`sources_and_sinks.md`](sources_and_sinks.md) | Data arriving from somewhere real, and results going somewhere real |
| [`stop_everything.md`](stop_everything.md) | Everything above stopped, and what comes back when you start again |

**Part 1 turns authorization off**, to keep things simple while you get a first
run working. That is a real feature switched off, not a default, and
[`authorization.md`](authorization.md) turns it back on.

## Part 2: the basics

One subject each. Read what you need.

| Guide | Covers |
|---|---|
| [`cli.md`](cli.md) | `flz` itself: how it reaches a controller, its output formats, and how a gated verb is identified |
| [`namespaces.md`](namespaces.md) | The boundary every runner, operative and credential sits inside |
| [`authorization.md`](authorization.md) | Turning enforcement on, claiming the admin, tokens, roles, and what a runner has to prove |
| [`using_plugins.md`](using_plugins.md) | The plugin system, the ones that ship, and what each costs |
| [`clustering.md`](clustering.md) | More than one machine: how controllers find each other and what that buys |
| [`durability.md`](durability.md) | What survives a crash, and what it costs |
| [`stream_basics.md`](stream_basics.md) | The stream profile: channels, keys, entities, and state |
| [`workflow_basics.md`](workflow_basics.md) | The workflow profile: steps, retries, and compensation |

## Part 3: advanced topics

| Guide | Covers |
|---|---|
| [`custom_plugins.md`](custom_plugins.md) | Writing and installing a plugin of your own |
| [`effectively_once.md`](effectively_once.md) | The delivery guarantee, what it requires, and what it costs |
| [`erlang_vm_tuning.md`](erlang_vm_tuning.md) | Scheduler, memory and IO settings for a controller under load |

## When something is wrong

[`../troubleshooting/`](../troubleshooting/runner_cannot_connect.md) is symptom
to fix: what a message means and what to do about it.

## Trying it without deploying anything

Every SDK can run an operative's declared graph in your own process, with no
controller, no socket and no deployment. It is the fastest way to see one work
and the fastest way to test one. [`../quickstart.md`](../quickstart.md).
