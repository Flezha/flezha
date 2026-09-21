# What is Flezha

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
**tasklet**, and leave on a second channel. The tasklet is the only real
logic, a few lines that add each trade to its symbol's total and emit the new
total. The rest is already there:

- **Trades in, totals out.** Two plugins that ship with Flezha,
  [WebSocket](../plugins/websocket_subscription.md) and
  [Postgres](../plugins/postgres_sink.md), attached to those two channels with
  one command each. Writing the totals to [Kafka](../plugins/kafka_producer.md)
  instead is a different command, not a code change.
- **The totals survive a crash.** Each symbol's total is saved as it changes, so
  after a crash it continues from its last value, not from zero.
- **A bad trade does not stop the stream.** A trade your tasklet keeps failing
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

## Watch one run

Bring up a controller and a runner that is already offering an operative, and
send a message through it:

```sh
$ cd sdk/python/examples/echo_deployment
$ docker compose up -d --wait
$ docker compose exec controller /flezha/bin/flz operative start echo --id my_echo
$ docker compose exec controller /flezha/bin/flz operative send my_echo --text 'hello world' --await 2000
```

```output
Delivered to 'my_echo'.
out	echo: hello world
```

[`sdk/python/examples/echo_deployment`](../../sdk/python/examples/echo_deployment)
is that deployment. Part 1 builds the same `echo` by hand, one piece at a time.

It is a demo, not a deployment: it makes every choice for you. The guides start
a controller of their own, so take it down first:

```sh
$ docker compose down
```

## Next

[`guides.md`](guides.md) lays out the guides.
