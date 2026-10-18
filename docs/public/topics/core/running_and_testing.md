# Running relays

A runner that registered a factory runs nothing yet. A relay is one
running instance of a factory, with its own id, its own state and its own place
in the cluster, and it exists once something starts it: `flz relay start`,
a schedule, a running tasklet, or application code. This page covers starting,
watching and stopping relays, testing one in local mode, what comes back
after a controller restart, and how to clear a controller back to a clean
slate.

The relation between a factory and its relays is a template and its
instances ([`building_blocks.md`](building_blocks.md#factory-and-relay)):
every start makes another independent instance with its own state, so one
factory can run as many relays as you need.

**Contents**

1. [Start a relay](#start-a-relay)
2. [Check that it is bound](#check-that-it-is-bound)
3. [Send a message](#send-a-message)
4. [Watch it](#watch-it)
5. [Diagnose a relay](#diagnose-a-relay)
6. [Stop it](#stop-it)
7. [From application code](#from-application-code)
8. [Local mode](#local-mode)
   1. [Nothing in local mode is durable](#nothing-in-local-mode-is-durable)
   2. [How it is used](#how-it-is-used)
   3. [Example: counting words](#example-counting-words)
   4. [What you run is what you deploy](#what-you-run-is-what-you-deploy)
9. [After a controller restart](#after-a-controller-restart)
10. [Start from a clean slate: `flz reset`](#start-from-a-clean-slate-flz-reset)
11. [Limits and gotchas](#limits-and-gotchas)
12. [See also](#see-also)

## Start a relay

```sh
$ flz relay start echo --id my_echo
```

```output
Started my_echo
```

`echo` is the factory id, which the runner registered; `my_echo` is the
relay id, which identifies this one relay for as long as it exists. It
is unique within its namespace, and later commands find the relay by it:
sending it a message, listening to it, stopping it, or attaching a plugin to
it. Without `--id` the controller generates one from the factory id, such as
`echo-k7m2qzxw4pd3fn2a`, and prints it.

> #### A start that may be retried needs an id {: .warning}
>
> A start without `--id` that is run again because its answer was lost starts
> a second relay. A second flow relay runs until it is stopped. Passing
> `--id` prevents it: a start of an id that is already running changes nothing
> and prints `my_echo is already running, left untouched`. An id that is not
> running but kept its durable state starts again, and a `--text` it carries is
> taken again: that message has no identity of its own, as a relay call's
> request and a schedule's message have, to tell a retry from a new one.

**Starting a live id again changes nothing, quietly.** The options are not
compared, so editing the command to add `--partitions 3` and running it again
looks like it worked and did nothing. Stop the instance first, then start it
with the options you want.

**You start a relay once, not once per session.** The controller keeps the
relay list in a durable registry and brings it back on boot, so after a
controller restart `my_echo` comes back on its own
([below](#after-a-controller-restart)).

The options of `flz relay start <factory_id>`:

| Option | What it does |
| --- | --- |
| `--id <id>`, `-i` | the id to start under; generated when absent |
| `--namespace <ns>` | the namespace to start in; `default` when absent |
| `--init-values <json>` | state to seed at start, keyed by tasklet name |
| `--text <payload>`, `-t` | the first message, delivered as `send` delivers one |
| `--channel <ch>`, `-c` | the entry channel the first message goes to; needs `--text` |
| `--plugins <json-array>` | plugins to bind at start, enabling each one named with a provider when it is not enabled yet |
| `--partitions <N>` | start `N` partition members as one group ([`partitioning.md`](partitioning.md)) |
| `--output <text\|json>`, `--json` | output format; JSON answers `{"id": ..., "outcome": "started" \| "already_running"}` |

None of them changes how the relay behaves: that is its class's settings
tree, and the runner announces it, so a start names only the factory
([`relay_settings.md`](../../configuration/relay_settings.md#three-layers-one-place-for-each-setting)).

**`--init-values`** is keyed by tasklet name, one entry per tasklet to seed;
a tasklet left out keeps the state its class builds. A keyed tasklet takes one
state per declared key:

```sh
$ flz relay start candles --init-values '{"ohlc": {"1m": {"high": 0}}}'
```

A name the class does not carry is ignored, not refused: the runner logs it
and the start succeeds, so check the spelling.

**`--text`** is the instance's first input, on the entry channel `--channel`
names or the first declared one. A workflow started without it waits for
`flz relay send`.

```sh
$ flz relay start order_executor --text '{"symbol":"EUR/USD","quantity":10}' --channel order
```

## Check that it is bound

```sh
$ flz relay list
```

```output
Fetching relays for namespace 'default'...
Relays in namespace 'default':

  RELAY                          HEALTH      PARTS   NODE                  RUNNER
  my_echo                           healthy     -       flezha@127.0.0.1      default_23fc85126379
```

A name under `RUNNER` means the controller found a runner that registered the
factory and bound the instance to it. `(none)`, with `(unplaced)` under
`NODE`, means the instance exists but no runner serves it yet. That almost
always means the factory id is spelled one way in the code and another way on
the command line.

`flz relay info` shows every relay in detail, including how it runs
(`Run: ...`, `Durable: ...`), its phase (`suspended` while it sleeps,
`finished` after a workflow ended) and, for a workflow, its result.
`flz relay info <id>` shows one relay's API: each entry channel with its
format, whether it answers, its description and schema, and an example call
([`sending_and_calling.md`](sending_and_calling.md#read-it)).
`flz relay graph <id>` prints what the relay is made of: each tasklet with the
channels it reads and writes, and which channels end at a plugin output or a
call ([`relay_commands.md`](../cli/relay_commands.md#flz-relay)).
`flz relay activity <id>` asks its runner what each tasklet is doing: its live
Cells, the items queued, and the items admitted, blocked and dropped so far;
`--cells <tasklet>` adds that tasklet's most recently active Cells, one line each.
`flz relay listen <id> --tally` counts what leaves on each output channel.

## Send a message

```sh
$ flz relay send my_echo --text 'hello world' --await 2000
```

```output
Delivered to 'my_echo'.
out	echo: hello world
```

The first line is the delivery verdict. It says only that the payload was
accepted: it is answered as soon as the input is durably accepted, before any
tasklet runs. The next lines are output: the channel name, a tab, and the
payload.

`--await 2000` attaches an output listener, delivers the payload, and prints
for two seconds. Read it as **everything this relay emitted during the
window**, not as the answer to your message. A graph is a stream, not a
function: a `filter` can drop the input and print nothing, a `flatMap` or a
`keyBy` can turn one input into many lines, and an aggregation may emit
minutes later, mixed with earlier inputs. For a relay that answers each
input, `flz relay call` returns that answer
([`sending_and_calling.md`](sending_and_calling.md#calls)), and it can call a
channel by its fields, typed by the channel's schema:

```sh
$ flz relay call orders-eu --channel place --arg pair=EUR/USD --arg amount=1000
```

```output
{"amount":1000,"pair":"EUR/USD","status":"placed"}
```

## Watch it

```sh
$ flz relay listen my_echo
```

```output
out	echo: hello world
```

It prints each message the relay sends out, one per line, for 30 seconds.
`--duration-ms` sets another time, and Ctrl-C stops it sooner. Each line is
the channel, a tab and the payload. A message from a keyed channel (declared
with `keyed()`) adds a third field, `cell=... seq=... key=...`.

- **`--input`** shows what goes into the relay instead of what comes out.
  A message that appears there with nothing coming out arrived, and the
  relay produced nothing for it; one that does not appear never arrived. A
  message sent without `--channel` shows `-` in place of the channel.
- **`--json`** prints each message as one line of JSON, with the channel, the
  payload and its metadata, for a script to read.

Listening leaves nothing behind. It stops when the time is up, on Ctrl-C, when
the terminal closes, or when the relay ends. A partition group is listened
to by its group id.

## Diagnose a relay

When a relay misbehaves, `flz relay diagnose` says what is wrong and what to do
next, from what the controller already knows about it: the convergence loop's
verdict, the runners that announce its factory, its dead letters, its bound
plugins and the inputs waiting in its input log.

```sh
$ flz relay diagnose my_echo
```

```output
OK       relay 'my_echo' is healthy, bound to runner 'default_23fc85126379' on controller flezha@127.0.0.1
```

A relay with something wrong prints one block per finding, problems first,
each with its code and the hint to act on:

```output
WARNING  diagnose.dead_letters  relay 'turns' has dead letters: 3, the newest 42s old
         hint: flz dlq list turns
NOTE     diagnose.asleep  relay 'turns' is suspended at a committed boundary; the next message wakes it on a runner
```

- **`PROBLEM`** needs action: no runner announces the factory, the runner is
  gone, a bound plugin is unhealthy or missing, the outputs are not read.
- **`WARNING`** is worth a look: dead letters, a parked relay, inputs that
  wait while nothing moves, a relay that did not answer for its own state.
- **`NOTE`** is information: the relay sleeps, it is being bound again, it
  finished.

Every code is in the [error reference](../../errors.md). `--json` prints the
`verdict` (`ok`, `warning` or `problem`) and the findings, for a script to read;
for a caller whose role may list dead letters, the dead-letter finding also
carries their most common errors. The command exits 0 whenever the diagnosis
itself ran. A relay that is neither running nor kept as finished answers
`relay.not_found`, exit 5.

A running relay that does not answer, because it is wedged or its process is
gone, is diagnosed from the convergence loop's verdict alone, with a
`diagnose.unread` warning in place of any `OK`: `--json` adds `live_read` with
the code of the failed read, such as `controller.timeout`, and its runner and
plugins are the ones its hub recorded.

## Stop it

```sh
$ flz relay stop my_echo
```

A stream runs until it is stopped. A workflow ends by itself; stopping one ends
it early, as `cancelled`, and undoes what its finished steps did
([`workflows.md`](workflows.md)). A sleeping workflow is woken onto a runner to
be stopped, so it ends as it would awake.

**Stopping is what forgets.** The controller keeps started relays and
enabled plugins in a durable registry, so whatever you do not stop or disable
comes back the next time the controller and its runners start. To shut
everything down so the next start is empty, go in reverse order:

1. Disable the plugins: `flz plugin disable <name>` ([`plugins.md`](../plugins/plugins.md)).
2. Stop the relays: `flz relay stop <id>`.
3. Stop the runners: SIGTERM, or Ctrl-C in a terminal. An Elixir runner in
   the controller's own BEAM stops with the controller.
4. Stop the controller: `flz controller stop`, `docker stop`, or, embedded in
   an Elixir application, with the application.

> #### Stop the controller rather than kill it {: .info}
>
> `flz controller stop` stops taking new work, hands its relays to another
> controller while they keep running, if there is one, and writes its last
> changes to disk before it exits
> ([`deploying_a_controller.md`](../deploying/deploying_a_controller.md#graceful-shutdown)).
> It prints `[shutdown] complete duration_ms=8 outcome=clean` when done.
>
> Killing it, or Ctrl-C in `attached` mode, is not a disaster: the controller
> writes each change to disk as it goes, syncing every few milliseconds, and
> the next start recovers from there. What a kill can cost is those last
> milliseconds: a relay may come back a step behind and send a few outputs
> a second time. A relay on the effectively-once tier does not have that
> gap, because its outputs wait until the change behind them is on disk.

## From application code

Every verb above is also a method of the runner's `control()` handle, and the
send is a method of the runner itself, so an application can start, stop and
feed its own relays without anyone typing `flz`. The calls run as the
identity the runner's connection proved, so nothing holds a credential and
nothing expires; with authorization on, the namespace must first be bound to a
role that allows them ([`namespaces.md`](namespaces.md#administering-from-application-code)).
Here the echo runner starts `my_echo` and sends it a message:

<!-- tabs-open -->

### Java

```java
// Main.java: the runner's control and send calls
Control control = runner.control();
control.startRelay(RelaySpec.of("echo").id("my_echo"));
runner.send("my_echo", "hello world", "in", SendOpts.of().msgId("hello-1"));
```

Full file: [`Main.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/echo/src/main/java/com/flezha/examples/echo/Main.java).
Use `start()` rather than `run()`: `run()` is `start()` followed by
`awaitTermination()` and does not hand the runner back, so the calls go
between the two.

### Rust

```rust
// src/main.rs: the runner's control and send calls
let control = runner.control();
control.start_relay(&flezha_runner::RelaySpec::new("echo").id("my_echo"))?;
let opts = flezha_runner::SendOpts::new().msg_id("hello-1");
runner.send("my_echo", "hello world", "in", &opts)?;
```

Full file: [`src/main.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/echo/src/main.rs).
Each call answers the controller's own result as a `serde_json::Value`, and a
refusal as a `ControlError`.

### Node

```javascript
// runner.mjs: the runner's control and send calls
const control = runner.control();
await control.startRelay('echo', { id: 'my_echo' });
await runner.send('my_echo', 'hello world', 'in', { msgId: 'hello-1' });
```

Full file: [`runner.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/echo/runner.mjs).
`run()` resolves once the runner is connected and serving, so the calls go
after it. Each call is a promise of the controller's result.

### Python

```python
# runner.py: the runner's control and send calls
control = runner.control()
control.start_relay("echo", id="my_echo")
runner.send("my_echo", "hello world", "in", msg_id="hello-1")
```

Full file: [`runner.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/echo/runner.py).
Each call blocks for the controller's answer.

### Ruby

```ruby
# runner.rb: the runner's control and send calls
control = runner.control
control.start_relay("echo", id: "my_echo")
runner.send_to("my_echo", "hello world", "in", msg_id: "hello-1")
```

Full file: [`runner.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/echo/runner.rb).
`boot` rather than `run`, because `run` is `boot` followed by
`await_termination`. It is `send_to` rather than `send`, which every Ruby
object already has.

### PHP

```php
// runner.php: the runner's control and send calls
$control = $runner->control();
$control->startRelay('echo', id: 'my_echo');
$runner->send('my_echo', 'hello world', 'in', msgId: 'hello-1');
```

Full file: [`runner.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/echo/runner.php).
The calls are answered at once, but PHP has no threads: a message they send is
processed once `awaitTermination()` starts serving.

### Elixir

```elixir
# lib/echo/application.ex: the runner's control and send calls
alias Flezha.SDK.Control
runner = Flezha.SDK.Runner
{:ok, %{id: "my_echo"}} = Control.start_relay(runner, "echo", id: "my_echo")
{:ok, _} = Flezha.SDK.send(runner, "my_echo", "hello world", "in", msg_id: "hello-1")
```

Full file: [`lib/echo/application.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/echo/lib/echo/application.ex).
Functions rather than methods: each takes the runner and answers
`{:ok, result}` or `{:error, reason}`. The runner is connected once its child
has started, so the calls can follow `Supervisor.start_link/2`.

<!-- tabs-close -->

The same retry rule applies here: a start without an id that is retried after
its answer was lost starts a second relay, and a second flow relay runs
until it is stopped. Pass an id when a start may be retried. A relay
started this way is the cluster's, not the connection's: it survives the
runner disconnecting, shows in `flz relay list`, and can be stopped there.
On a relay with an input log, the message id `hello-1` makes a repeat of
the send absorbed rather than handled twice;
[`sending_and_calling.md`](sending_and_calling.md#sending-from-runner-code) covers
the send's options.

## Local mode

Local mode runs a relay inside your own process, with no controller, no
Docker and no socket. It takes the relay class you would deploy, runs its
declared graph, and hands you the output. It is the way to try a relay, to
develop one and to test its graph.

### Nothing in local mode is durable

Local mode holds state in memory. There is no write-ahead log, no snapshot, no
dead-letter queue, and nothing survives the process. Every run prints that on
stderr before it does any work, and the notice cannot be turned off.

Local mode is not a deployment. These need a controller:

| Needs a controller | Why |
| --- | --- |
| Durability: the write-ahead log, snapshots, resuming after a crash, point-in-time queries | The controller owns the durable store |
| Effectively-once delivery | An output rides its effect record and is released once that record is durable |
| Plugins: Kafka, Postgres, HTTP, WebSocket | Plugins run on the controller |
| Placement, migration, rescale, partitioning | A member's key-group range arrives with `AddRelay` |
| The dead-letter queue | A park is durable, and the controller acknowledges it; a local run logs a payload that would park and drops it |
| Observability export, authorization, several runners | Controller surfaces |

Everything else runs as it does deployed: the entry channel's decode and parse
ladder, edge transforms (`map`, `filter`, `flat_map`) and keying (`key_by`),
one cell per resolved key with its own mailbox and overflow policy, the retry
ladder, and the delivery between tasklets.

### How it is used

Every SDK has the same four calls: start the relay, register a callback on an
output, feed an input channel, and run until the graph is idle.

- **`feed(channel, bytes)`** enters where a frame off the wire enters, so the
  channel's declared decoder runs. It blocks while the consumer's mailbox is
  full, which is the deployed entry path's own backpressure.
- **`on_output(channel, callback)`** registers against the output's channel name. An
  output names the channel it ends, which is why the Rust example below says
  `.output_named("totals")` and registers `"totals"`. An output without a name is
  named after the tasklet that feeds it.
- A channel name the declaration does not carry is refused, naming the channel,
  the relay and the channels that are declared. A value released on an output
  with no callback is reported rather than discarded. A declaration that needs
  a controller is refused at startup, naming the capability.
- **`run_until_idle(budget)`** blocks until the graph has nothing left to do. A
  budget that runs out first is an error, never a silent success.

| SDK | Entry point | Worked example |
| --- | --- | --- |
| Rust | `LocalRelay::start` | [`sdk/rust/examples/local_word_count.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/local_word_count.rs) |
| Java | `LocalRelay.start` | [`sdk/java/flezha-sdk-examples/.../local/`](https://github.com/flezha/flezha_java_sdk/tree/main/flezha-sdk-examples/src/main/java/com/flezha/examples/local) |
| Node | `LocalRelay.start` | [`examples/node/local_word_count.mjs`](https://github.com/flezha/flezha/blob/main/examples/node/local_word_count.mjs) |
| Python | `LocalRelay.start` | [`sdk/python/examples/local_word_count.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/local_word_count.py) |
| Ruby | `Flezha::LocalRelay.start` | [`sdk/ruby/examples/local_word_count.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/local_word_count.rb) |
| PHP | `LocalRelay::start` | [`sdk/php/examples/E07_LocalMode.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/E07_LocalMode.php) |
| Elixir | `Flezha.SDK.Local.start` | the snippet below |

### Example: counting words

A graph that splits lines into words, keys each word to a cell of its own and
counts it. In Rust, the declaration and the run:

```rust
impl Relay for WordCount {
    fn factory_id() -> &'static str {
        "local_word_count"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("lines")
            .into::<Split>()                       // one line -> its words
            .key_by(|word: &String| word.clone())  // one Cell per word
            .into::<Tally>()                       // counts what it is handed
            .keyed()
            .output_named("totals");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let run = LocalRelay::start::<WordCount>()?;
    run.on_output("totals", |out| {
        println!("{}", String::from_utf8_lossy(&out.payload));
    })?;
    run.feed("lines", "the quick brown fox")?;
    run.feed("lines", "the lazy dog")?;
    run.run_until_idle(Duration::from_secs(5))?;
    Ok(())
}
```

`Split` and `Tally` are ordinary tasklets with nothing local about them. Flezha
is not on crates.io yet, so the crate is a path dependency on a checkout
(`flezha_runner = { path = "../flezha/sdk/rust" }`). From a checkout:

```sh
$ cargo run -p flezha_runner --example local_word_count
```

It prints the notice, then one line per word occurrence, counted per word:
`the 1`, `quick 1`, `brown 1`, `the 2`, and so on.

The same run in Java:

```java
try (LocalRelay run = LocalRelay.start(WordCount.class)) {
    run.onOutput("totals", out -> System.out.println(new String(out.payload(), UTF_8)));
    run.feed("lines", "the quick brown fox");
    run.runUntilIdle(Duration.ofSeconds(5));
}
```

Build the host native binary once, install the reactor, then run it:

```sh
$ scripts/build-native.sh --output sdk/java/flezha-sdk-native/src/main/resources/native
$ mvn -f sdk/java/pom.xml -DskipTests install
$ mvn -f sdk/java/flezha-sdk-examples/pom.xml exec:java -Dexec.mainClass=com.flezha.examples.local.Main
```

And in Elixir:

```elixir
{:ok, run} = Flezha.SDK.Local.start(WordCount)
:ok = Flezha.SDK.Local.on_output(run, "counts", &IO.puts(&1.payload))
:ok = Flezha.SDK.Local.feed(run, "lines", "the quick brown fox")
:ok = Flezha.SDK.Local.run_until_idle(run, 5_000)
:ok = Flezha.SDK.Local.stop(run)
```

Every SDK runs local mode on the same core and is held to one recorded
expectation, `test/fixtures/local_mode_word_count.txt`, which every SDK's
local-mode suite reads.

### What you run is what you deploy

The class carries no local-mode member. Registered with a runner, the same type
deploys unchanged:

```rust
Flezha::create_runner("default")?.register_factory(WordCount)?.run()?;
```

## After a controller restart

A controller whose disk survived a restart brings its relays back by
itself: it reads the relay list from its durable registry, starts each
relay that is running nowhere, rebinds it to a runner that registered its
factory, and restores each cell from its log and snapshots. Plugins attached
to a relay come back with it. There is no marker file and no boot mode to
arm. With a remote durability backend configured, both the registry and the
state are first fetched from it ([`durability.md`](durability.md)).

What is kept on disk:

| What | Where |
| --- | --- |
| the registry: each relay's start, with its plugins | `app_data/process_hub/<hub_id>/registry.dets` |
| each cell's log and snapshots | `app_data/state/<namespace>/<relay_class>/<tasklet_id>/<cell_key>.{wal,snap.<seq>,outbox.dedup}` |
| a remote copy of both, unless opted out | Postgres, S3 or a path, by `FLEZHA_DURABILITY_BACKEND` |

**The first recovery round is the boot gate.** Each namespace's hub waits for
its peers (about two seconds on a node alone), then compares the declared list
with what runs: it starts declared relays running nowhere, stops running
ones whose entry was removed, and settles duplicates. Until that first round,
the controller reports `recovering`: `/healthz` says so, and `flz boot await`,
`/readyz` and the image's healthcheck answer not ready. They turn ready by
themselves, with no restart. The round waits for peers, not for runners: a
relay started before its runner connects waits unbound, and is bound when a
runner offering its factory arrives.

A namespace whose hub was torn down is given one again on the next start
while the namespace exists; `flz namespace delete <name>` is what makes a stop
permanent. A clean state root holds one `_system` hub after the first boot.

| Scenario | Restored? | What happens |
| --- | --- | --- |
| the whole cluster starts cold | yes | each node finds the whole list; a duplicate start of one id is harmless |
| one controller restarts into a healthy cluster, as in a rolling update | no | nothing is missing, so nothing is started |
| a single-node deployment | yes | the first round starts the whole durable list |
| a runner restarts with a new identity | not needed | the controller never went down; the new runner gets the same cells restored |
| a relay moves while its old runner is alive | not needed | its state travels with it |
| a relay moves after its old runner died | yes | the new place pulls the cell files and replays them, once |
| a network partition during boot | yes, with care | a short window where one relay may start twice, settled when the network heals; set `FLEZHA_AUTO_RESTORE_ENABLED=false` in fragile environments |
| three nodes, two die | only what is missing | returning nodes start only what nothing in the cluster holds |
| the cluster goes down and only some nodes return | yes | **the one tolerated loss**: relays recorded only on the nodes that did not return are gone, unless the remote tier is on |
| the registry file is damaged | empty | the file is renamed `registry.dets.corrupt-<n>`, and the boot continues empty |
| an old `registry.dets` restored from a backup | per entry | its entries are intent again, and the next round starts them |

**Running the cold-start script again is safe.** Each start resolves through
one claim on the relay id: `flz relay start` of a live instance exits 0
and does nothing, `--partitions N` over an existing group starts only the
missing members, and `flz plugin enable` of an enabled plugin with the same
provider reports `already_enabled`. So re-running the script that started
everything is the supported way to fill gaps after a warm restart. A claim
refused with no live winner (a stale registry entry) stays a loud error.

```sh
$ flz boot status                # readiness verdict and its conditions, then each namespace you may see
$ flz boot await --timeout 60    # wait until ready (exit 7, boot.not_ready, otherwise)
$ flz boot await --timeout 0     # evaluate once: what the image healthcheck runs
$ flz cluster status             # recovery state per node
```

A verb that refuses writes its reason to your shell's stderr, so
`flz boot await` names the condition still unmet:

```sh
$ flz boot await --timeout 0
```

```output
boot await: recovering; unmet: durable_registry_loaded
```

- `FLEZHA_AUTO_RESTORE_ENABLED`: Only `true` or `1` enable it; off, nothing is written to the registry and relays must be started again after each boot.
- `FLEZHA_PROCESS_HUB_CLUSTER_SETTLE_MS`: How long a starting hub waits for peers before treating itself as alone; `0` when the cluster forms before the application starts.
- `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS`: The longest a peer that never answers can hold a hub in `recovering`.
- `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS`: At most one later recovery round per this interval.
- `FLEZHA_BOOT_REPLAY_TIMEOUT_MS`: How long a connecting runner is given while the cluster forms.
- `FLEZHA_PROCESS_HUB_REGISTRY_ROOT`: Where the registry lives; must be persistent storage.

Their types and defaults are in [Controller environment variables](../../configuration/environment_variables.md#every-variable).

Turning auto-restore off takes effect on the next restart; an existing
`app_data/process_hub/` is then ignored, and `flz reset` deletes it.

The boot emits telemetry under `[:flezha, :boot, ...]`, each event with
`hub_id` and `namespace`:

| Event | Fires | Measurements |
| --- | --- | --- |
| `:pre_recovery_replay` | before the first round | `cspec_count`, `system_time` |
| `:recovery_complete` | after the first round | `child_count`, `succeeded`, `failed`; `replay_outcome` metadata |
| `:reconcile_round` | every round | `candidates`, `orphans`, `started`, `duplicates`, `elapsed_ms`; `first_round` metadata |
| `:reconcile_duplicate` | one relay seen on several nodes | `instance_count`; `relay_id`, `kept_node`, `stopped_nodes` metadata |

`:recovery_complete` and `:reconcile_duplicate` also reach the observability
bus under the `:flezha_boot` source. `:reconcile_duplicate` is an alarm, not a
metric: the duplicate is already resolved, keeping one. It is expected only
while recovering from a partition between nodes; at any other time a start
bypassed the id claim, and it logs at `error`.

## Start from a clean slate: `flz reset`

`flz reset` clears a controller's durable data under `app_data/` so that it
boots empty: every cell's log, snapshot and outbox files and the registry by
default, and optionally the TLS material, the dead-letter queue, schedules,
the auth store, the audit trail, installed plugins and socket files. It
removes only the contents of each directory, never `app_data/` itself, so bind
mounts, volumes and file watchers stay valid. It clears nothing while the
controller is up, and does not touch a remote durability backend.

```sh
$ flz controller stop
$ flz reset --yes
$ flz controller start
```

| Directory | Contents | Cleared |
| --- | --- | --- |
| `app_data/state/` | each cell's log, snapshot and outbox files | by default |
| `app_data/process_hub/` | the durable registry | by default |
| `app_data/certs/` | TLS material | `--include-certs` |
| `app_data/dlq/` | parked dead-letter records | `--include-dlq` |
| `app_data/schedules/` | each schedule | `--include-schedules` |
| `app_data/auth/` | tokens, role bindings, namespaces | `--include-auth` |
| `app_data/audit/` | the audit trail | `--include-audit` |
| `app_data/plugins/` | installed plugins | `--include-plugins` |
| the resolved socket directory | Flezha's own socket files | `--include-sockets` |
| `logs/` | the controller's log file | never |

| Flag | Meaning |
| --- | --- |
| `--yes` | required to apply; without it the command prints the plan and exits 2 |
| `--dry-run` | print the plan without touching anything; exits 0 |
| `--include-certs`, `--include-dlq`, `--include-schedules`, `--include-auth`, `--include-audit`, `--include-plugins`, `--include-sockets` | also clear that directory, as in the table above |
| `--all` | every `--include-` flag |
| `--app-data-root <PATH>` | the `app_data/` root; otherwise `FLEZHA_APP_DATA_ROOT`, otherwise `./app_data` |
| `--probe-timeout-ms <N>` | how long to probe for a running controller; 1000 by default |
| `--json` | one JSON object as output (`--output json` is the same) |

`--yes` with `--dry-run` is a usage error, and so is any unknown argument
(`--output` with a value other than `text` or `json` included) and a switch
written with a value: `--all=no` is refused as
`error: --all takes no value (usage.option_takes_no_value)`, because reading
it as written would clear what you asked to keep.

**Every directory is reported on every run**, cleared or kept, so the summary
names what a reset left behind:

```output
cleared     ./app_data/state/                                (12 files, 4.3 MiB)
cleared     ./app_data/process_hub/                          (2 files, 16.0 KiB)
kept        ./app_data/certs/                                (use --include-certs to clear)
kept        ./app_data/dlq/                                  (use --include-dlq to clear)
kept        ./app_data/schedules/                            (use --include-schedules to clear)
kept        ./app_data/auth/                                 (use --include-auth to clear)
kept        ./app_data/audit/                                (use --include-audit to clear)
kept        ./app_data/plugins/                              (use --include-plugins to clear)
kept        socket directory                                 (use --include-sockets to clear)
kept        logs/                                            (not reset's to clear)
reset complete in 41 ms
```

In plan-only mode the words are `would clear` and `would keep`. With `--json`
the same report is one object with `cleared` (each `root`, `files`, `bytes`),
`kept` (each `root`, `reason`), `duration_ms` and `dry_run`, which is `true`
for `--dry-run` and for a bare invocation. The counts are computed over the
same entries in a preview and in a run, so the preview matches the action.

**It refuses while the controller is up.** The command probes the configured
node; a node that answers means the reset is refused as
`reset.controller_running` (exit 6). There is no override: stop a wedged
controller with `flz controller stop --force` first. `--dry-run` skips the
probe, since it changes nothing. A controller co-located on the same machine is
a different node, and is not what this check protects; what a peer shares is
the socket directory, guarded below.

**What it does not find.** `flz reset` resolves each directory as
`<app_data root>/<name>` and does not read per-store variables such as
`FLEZHA_PROCESS_HUB_REGISTRY_ROOT`, `FLEZHA_DLQ_DIR`, `FLEZHA_SCHEDULE_DIR` or
`FLEZHA_AUTHZ_DIR`: a store moved that way is neither found nor cleared. `logs/`
sits where the log file's default puts it, relative to the working directory,
so a controller started elsewhere or with `FLEZHA_LOG_PATH` logs where reset
cannot see, and reset never empties it.

**The socket directory** is resolved the way the controller and each runner
resolve it ([`connecting_a_runner.md`](../deploying/connecting_a_runner.md#the-socket-directory)),
and only with `--include-sockets` or `--all`, so `--app-data-root` does not
move it; the summary names which rule chose the path. A controller still
serving in that directory keeps it: reset probes the socket first, reports the
directory `kept` and goes on with the rest. A socket nothing listens on is
stale and is cleared. Only Flezha's own socket files are removed, never other
files in a directory named by `FLEZHA_SOCKET_FILES_PATH`, nor the directory. A
directory reset may not enter, or a socket that refuses for another reason, is
reported and skipped. When no directory can be resolved, the socket row is
reported `kept` with the reason and the exit code does not change.

**The data you keep on purpose** has its own flag:

- **`--include-auth` removes every credential**: every token, runner
  credentials included, every role binding and every namespace but `_system`.
  With authorization on, the next start writes a fresh one-time token to
  `<auth_dir>/bootstrap.token` and names the path in its log. Until it is
  claimed, administration verbs are refused and every runner is admitted
  without a credential into any namespace but `_system`. Claim it at once with
  `flz auth bootstrap <token>`, above all when the reset was meant to revoke a
  leaked secret, then bind roles and issue runner credentials again
  ([`authorization.md`](authorization.md)).
- **`--include-audit` erases the audit trail.** Copy `app_data/audit/` first if
  it must be kept.
- **`--include-plugins` removes every installed plugin.** Install them again
  after the next start.

> #### A clean slate needs `FLEZHA_DURABILITY_BACKEND=none` {: .warning}
>
> With a remote durability backend, the next boot fetches the registry, the
> auth store, the schedules and the cells' state from the remote store, so
> what was wiped comes straight back. To start fresh, set the backend to `none`
> for that boot, or purge the remote prefix or table first.

| Exit | Meaning |
| --- | --- |
| 0 | applied, or a `--dry-run` preview |
| 1 | `reset.partial` (a directory was cleared, then a filesystem error stopped the walk) or `reset.path_unreadable` (nothing was removed) |
| 2 | `reset.confirmation_required` (neither `--yes` nor `--dry-run`), `reset.path_refused` (an `--app-data-root` that is the root itself, or names one path twice after symlinks are resolved), or a `usage.*` refusal |
| 6 | `reset.controller_running` |

On an error, the directories not reached are reported as
`kept (not reached: earlier error)`, and the error, naming the failing path,
goes to stderr:

```output
error: flz reset printed its plan and cleared nothing: pass --yes to apply it, or --dry-run to preview without an error (reset.confirmation_required)
hint: flz reset --yes
```

A missing root is success: `flz reset --yes` with no `app_data/` exits 0. In
CI, `flz reset --dry-run --json` modifies nothing and does not need the
controller down, so it serves as a starting-state check. Back up first when in
doubt: `tar -czf backup.tgz app_data/`.

In Docker Compose, a bind-mounted `app_data/` survives and only its contents
go:

```sh
$ docker compose exec controller flz controller stop
$ docker compose exec controller flz reset --yes
$ docker compose restart controller
```

In Kubernetes, the volume's file system is kept; `kubectl delete pvc` disposes
of it entirely:

```sh
$ kubectl exec -n flezha deploy/controller -- flz controller stop
$ kubectl exec -n flezha deploy/controller -- flz reset --yes
$ kubectl rollout restart -n flezha deploy/controller
```

`flz auth rebootstrap` runs behind the same running-controller check. What
`flz reset` does not do: reset one relay while the controller is up, trim
point-in-time history (that is `flz state retention <relay_class> <hours>`),
reset a whole cluster (run it on each node), or purge a remote backend.

## Limits and gotchas

- **A start without `--id` is not safe to retry**, as the box above says.
- **Starting an existing id ignores new options.** Stop it first.
- **Stopping is the only way to forget.** Relays and plugins left running
  come back after a restart.
- **A namespace deleted is gone for good**; a hub stopped comes back while its
  namespace exists.
- **`flz reset` and relocated stores**: per-store path variables are not read.

## See also

- [`building_blocks.md`](building_blocks.md): factories and relays.
- [`relay_settings.md`](../../configuration/relay_settings.md): what the class decides, and what a start may not.
- [`namespaces.md`](namespaces.md): namespaces, quotas and the `control()` surface from code.
- [`deploying_a_controller.md`](../deploying/deploying_a_controller.md#graceful-shutdown): stopping a controller.
- [`durability.md`](durability.md): what survives a crash, and the remote tier.
- [`cli.md`](../cli/cli.md): the whole `flz` command line.
- [`partitioning.md`](partitioning.md), [`sending_and_calling.md`](sending_and_calling.md).
- Getting started: [Run the echo](../../getting_started/up_and_running.md#run-the-echo) and [Stop and clean up](../../getting_started/stop_and_clean_up.md).
