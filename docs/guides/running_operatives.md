# Running operatives

**Before you start:** finish [`runner_deployment.md`](runner_deployment.md). The
controller and your runner should both still be running, each in its own
terminal. From a third, check both:

```sh
flz boot await     # boot await: ready
flz runner list    # lists your runner
```

If either is missing, start it again the way its guide showed.

## What an operative is

What you wrote is a **declaration**: a graph from an input to an output, with the
steps in between. It runs nothing by itself. Your runner registers it
under a **factory id**, and an operative is one running instance created from
that declaration when you ask the controller for one. If the analogy helps, the
declaration is the definition and an operative is a thing made from it.

One declaration backs as many operatives as you need, each under a name you
choose and each independent of the rest: its own entities, its own state, its
own placement in the cluster. Ten exchanges to watch is one declaration started
ten times. What the declaration itself is, and why it is the unit the controller
schedules, is in [the runner guide](runner_deployment.md#the-operative).

## The two kinds

An operative's **kind** is fixed when you start it, and it decides when the
thing is over.

**Stream operatives are always on.** An endless input, no completion, recovery
per entity. `echo` is one, and everything below uses this kind. It runs until
you stop it.

**Workflow operatives terminate.** A graph of procedures run level by level
behind a commit barrier, ending by itself on chain completion, on
`ctx.complete`, on a durable loop's terminal predicate, or at `max_duration`.
Stopping one by hand ends it early; it is not how one is meant to finish.
[`workflow_basics.md`](workflow_basics.md) is the kind's own guide.

## Start one

The runner is connected and offering `echo`, and nothing is running yet.
Declaring an operative on a runner says that process is *able* to serve one.
Starting it is a separate step, and it is the one that creates a running
instance.

```bash
flz operative start my_echo echo
```

The command takes two ids, and they are not the same kind of thing.

**`my_echo` is the operative id.** You choose it when you start the operative,
and it identifies this one operative for as long as it exists. It is unique
within its namespace, and every later command finds the operative by it:
sending it a message, listening to it, stopping it, or attaching a plugin to it
with `--operative_id`.

**`echo` is the factory id.** Every operative class declares exactly one, and
`factoryId()` returns it in your code. A runner offers its classes to the
controller by their factory ids, and `flz operative start` uses the one you give
it to find a runner that can run the class. It is recorded in the controller's
registry and in durable state, so changing it later declares a new class rather
than renaming the old one.

One factory id can back many operatives: `flz operative start eu_echo echo` and
`flz operative start us_echo echo` would be two independent operatives of the
same class, each with its own state. Everything after this uses the operative
id.

## Confirm it is bound

```bash
flz operative list
```

```
Fetching operatives for namespace 'default'...
Operatives in namespace 'default':

  OPERATIVE                         HEALTH    PARTS   NODE                  RUNNER
  my_echo                           healthy   -       flezha@127.0.0.1      default_23fc85126379
```

`RUNNER` is the column that matters. A name there means the controller found a
runner offering `echo` and bound the instance to it. `(none)`, with
`(unplaced)` under `NODE`, means the instance exists but no runner has been
found to serve it. That almost always means the factory id is spelled one way
in your code and another way on the command line.

## Send one message and see what comes back

```bash
flz operative send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

The first line is the delivery verdict, and it only says the payload was
accepted. It is answered as soon as the input is durably accepted, before your
procedure runs, so it tells you nothing about what happened next.

The second line is the output: the channel name, a tab, and the payload your
procedure emitted. `out` is the name the operative gave that channel with
`.sink("out")`.

`--await 2000` attaches an output listener, delivers the payload, and prints
for two seconds. Read it as **everything this operative emitted during the
window**, not as the answer to your message. A graph is a stream and not a
function: a `filter` can drop your input and print nothing at all, a `flat_map`
or a `key_by` can turn one input into many lines, and an aggregation may emit
minutes later mixed in with earlier inputs. For the hello world those are the
same thing. For a real graph they are not, and the window is the honest
description of both.

## Or watch it from another terminal

```bash
flz operative listen my_echo
```

It shows every message the operative sends out, one per line, for 30 seconds.
`--duration-ms` sets a different time, and Ctrl-C stops it sooner. Send from a
second terminal and the lines appear as they arrive:

```
out	echo: hello world
```

Each line is the channel the message left on, a tab, and the message itself. A
message from a keyed channel, one declared with `keyed()`, gets a third column
saying which key it came from and its number in that key's sequence:
`entity=... seq=... key=...`.

Two options help when something looks wrong:

- **`--input`** shows what goes into the operative instead of what comes out. If
  your message appears there but nothing comes out, it arrived and the operative
  produced nothing for it. If it does not appear, it never arrived. A message
  sent without `--channel` shows `-` where the channel would be.
- **`--json`** prints each message as one line of JSON, with the channel, the
  message and everything else attached to it. Use it when a script reads the
  output instead of you.

Listening leaves nothing behind. It stops by itself when the time is up, when
you press Ctrl-C, or when your terminal closes.

## Stopping it, later

`echo` is a stream operative, so nothing ends it but you. **Leave it running for
now:** the next guide attaches plugins to it, and
[`stop_everything.md`](stop_everything.md) stops it at the end of Part 1. The
command is:

```bash
flz operative stop my_echo
```

Two things about the lifetime surprise people.

**You start an operative once, not once per session.** The controller keeps the
operative list in a durable registry and rehydrates it on boot, so after a
controller restart `my_echo` comes back on its own. Running `start` again is
not how you bring it back.

**Running `start` again on a live id changes nothing, quietly.** It reports
that the existing instance was left untouched and that the options were not
compared. So editing the command to add `--partitions 3` and re-running it
looks like it worked and did nothing at all. Stop the instance first, then
start it with the options you want.

## Or from your own code

Every verb above is also a method on the runner, so an application can start and
stop its own operatives without anyone typing `flz`. `runner.admin()` reaches
them, and the controller judges each call as the identity the connection already
proved, so nothing holds a credential and nothing expires.

<!-- tabs-open -->

### Java

```java
FlezhaRunner runner = Flezha.createRunner("default")
        .defineStream(EchoOperative.class)
        .start();

Admin admin = runner.admin();
admin.startOperative(OperativeSpec.of("my_echo", "echo"));
admin.send("my_echo", "hello", "in");
admin.stopOperative("my_echo");

runner.awaitTermination();
```

`start()` rather than `run()`, because `run()` is `start()` followed by
`awaitTermination()` and never hands the handle back. Split the two and the
admin calls go between them: nothing extra keeps the process alive, the wait
does, exactly as before.

The echo example carries these lines commented out in its `Main`,
[ready to uncomment](https://github.com/alfetahe/flezha/tree/main/sdk/java/examples/echo).

### Rust

```rust
let runner = Flezha::create_runner("default")?;
runner.define_stream::<EchoOperative>()?.start()?;

let admin = runner.admin();
admin.start_operative(&OperativeSpec::new("my_echo", "echo"))?;
admin.send("my_echo", "hello", "in")?;
admin.stop_operative("my_echo")?;

runner.await_termination()?;
```

`start()` rather than `run()`, for the same reason as in Java. Each call answers
the controller's own result as a `serde_json::Value`, and a refusal as an
`AdminError`.

The echo example carries the calls commented out in its `main.rs`,
[ready to uncomment](https://github.com/alfetahe/flezha/tree/main/sdk/rust/examples/echo).

### Node

```javascript
const runner = createRunner('default').defineStream('./echo_operative.mjs');
await runner.run();

const admin = runner.admin();
await admin.startOperative({ id: 'my_echo', factory_id: 'echo' });
await admin.send('my_echo', 'hello', 'in');
await admin.stopOperative('my_echo');
```

`run()` resolves once the runner is connected and serving, so the calls go after
it and the process keeps serving without a wait of its own. Each call is a promise
of the controller's result.

The echo example carries the calls commented out in its `runner.mjs`,
[ready to uncomment](https://github.com/alfetahe/flezha/tree/main/sdk/node/examples/echo).

### Python

```python
runner = flezha.create_runner("default")
runner.define_stream(echo_operative)
runner.start()

admin = runner.admin()
admin.start_operative("my_echo", "echo")
admin.send("my_echo", "hello", "in")
admin.stop_operative("my_echo")

runner.await_termination()
```

`start()` rather than `run()`, for the same reason as in Java. Each call blocks
for the controller's answer.

The echo example carries the calls commented out in its `runner.py`,
[ready to uncomment](https://github.com/alfetahe/flezha/tree/main/sdk/python/examples/echo).

### Ruby

```ruby
runner = Flezha.create_runner("default").define_stream(EchoOperative)
runner.boot

admin = runner.admin
admin.start_operative("my_echo", "echo")
admin.send_to("my_echo", "hello", channel: "in")
admin.stop_operative("my_echo")

runner.await_termination
```

`boot` rather than `run`, because `run` is `boot` followed by
`await_termination`. It is `send_to` rather than `send`, which every Ruby object
already has.

The echo example carries the calls commented out in its `runner.rb`,
[ready to uncomment](https://github.com/alfetahe/flezha/tree/main/sdk/ruby/examples/echo).

### PHP

```php
$runner = Flezha::createRunner('default')
    ->defineStream(EchoOperative::class)
    ->start();

$admin = $runner->admin();
$admin->startOperative('my_echo', 'echo');
$admin->send('my_echo', 'hello', 'in');
$admin->stopOperative('my_echo');

$runner->awaitTermination();
```

`start()` rather than `run()`, for the same reason as in Java. The calls are
answered at once, but PHP has no threads: a message they send is processed once
`awaitTermination()` starts serving.

The echo example carries the calls commented out in its `runner.php`,
[ready to uncomment](https://github.com/alfetahe/flezha/tree/main/sdk/php/examples/echo).

### Elixir

```elixir
children = [{Flezha.SDK, namespace: "default", streams: [Echo.Operative], transport: :uds}]
{:ok, _supervisor} = Supervisor.start_link(children, strategy: :one_for_one)

alias Flezha.SDK.Admin
runner = Flezha.SDK.RunnerServer

{:ok, _} = Admin.start_operative(runner, "my_echo", "echo")
{:ok, _} = Admin.send_to(runner, "my_echo", "hello", "in")
{:ok, _} = Admin.stop_operative(runner, "my_echo")
```

Functions rather than methods: each takes the runner, here its registered process,
and answers `{:ok, result}` or `{:error, reason}`. The runner is connected once its
child has started, so the calls can follow `Supervisor.start_link/2`.

The echo example carries the calls commented out in its `application.ex`,
[ready to uncomment](https://github.com/alfetahe/flezha/tree/main/examples/elixir/echo).

<!-- tabs-close -->

An operative started this way is the cluster's, not the connection's: it
survives the runner disconnecting, and an operator sees it in
`flz operative list` and can stop it there.

**This works here because Part 1 turned authorization off**, which allows every
call. With enforcement on, the connection is `runner@<namespace>` bound to the
`runner` role, which grants the connect and nothing else, so every one of these
is refused until an operator grants it once:

```sh
flz role bind runner@default app default
```

The whole surface, what it deliberately cannot do, and the spelling in each of
the seven SDKs: [`../sdk/admin.md`](../sdk/admin.md).

## Next

An operative with nothing feeding it does nothing.
[`sources_and_sinks.md`](sources_and_sinks.md) attaches a real input and a real
output to it.
