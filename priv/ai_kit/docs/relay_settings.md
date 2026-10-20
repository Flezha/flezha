# Relay settings

A relay is configured by one settings tree, declared in its class. The tree
says how an instance runs, where its work is kept, and how long it may run.
There is one relay class in every SDK and no relay kinds: an ETL stream
and a workflow are the same class with different settings.

The tree lives in the code because it is what the relay promises: that a
finished step is not run again, that an output is not duplicated, that an idle
instance leaves its runner. Nothing given at start time and nothing in the
environment changes those promises, so the class alone tells you how every
instance of it behaves.

The settings are frozen when the class registers with the controller and
have no node-level override, because they change what the relay's output
means, not just how fast it comes; to change one, redeploy the runner.

**Contents**

1. [The settings tree](#the-settings-tree)
2. [Flow or steps](#flow-or-steps)
3. [Three layers, one place for each setting](#three-layers-one-place-for-each-setting)
4. [How each SDK writes the tree](#how-each-sdk-writes-the-tree)
5. [How the tree is checked](#how-the-tree-is-checked)
6. [Seeing what an instance runs with](#seeing-what-an-instance-runs-with)
7. [Example: a workflow, in steps](#example-a-workflow-in-steps)
8. [Example: sleeping and effectively once](#example-sleeping-and-effectively-once)
9. [Channel and edge options](#channel-and-edge-options)
10. [Tasklet-level options](#tasklet-level-options)
11. [Limits and gotchas](#limits-and-gotchas)
12. [See also](#see-also)

## The settings tree

Every setting is optional except `factory_id`. A setting exists only under the
parent that carries it, so a combination that makes no sense cannot be
written.

```text
factory_id                 required: the factory's name
description                free text
max_duration_ms            none: a wall-clock stop for one run, from its first bind
run                        flow (default) | steps
  flow
    exactly                at_least_once (default) | effectively_once
      effectively_once
        ack                per_input (default) | checkpoint
          checkpoint
            every          close the interval after n inputs
            max_delay_ms   close it after this long
            outputs        at_checkpoint: hold the interval's outputs
        correction_ms      none: the late-input correction window
        beyond_window      process_as_arrival (default) | drop
  steps
    each                   once (default) | per_input | chained
    on_failure             undo_all (default) | keep_progress
durable                    false | local (default) | remote
  local, remote
    sleep_after            a duration | false
```

A class that declares only its factory id and its graph runs in flow, with
`exactly: at_least_once`, kept on the controller's disk (`durable: local`),
never sleeping, with no `max_duration_ms`. A class that declares `run: steps`
and nothing else is kept locally too, and sleeps after six idle hours.

| Setting | Default | What it does |
| --- | --- | --- |
| `factory_id` | required | The factory's stable identity, the name `flz relay start` resolves. It is recorded in the controller's registry and in durable state, so changing it declares a new factory rather than renaming the old one. |
| `description` | none | Free text. |
| `max_duration_ms` | none | A wall-clock stop for either run, counted from the run's first bind: a sleep, a runner restart or a move to another runner or controller does not restart it, and a run that comes back past it ends at once. A steps instance still running then unwinds and ends `cancelled`; a flow instance ends `cancelled` once its cells hold nothing. A stop and a new start of the same id open a new window. |
| `run` | `flow` | How an instance runs. `flow` takes inputs for as long as it lives, and its commit point is one input (or one checkpoint). `steps` runs a chain of tasklets, and its commit point is a finished step, written to a journal. |
| `run.flow.exactly` | `at_least_once` | The flow run's duplicate rule. `effectively_once` puts the relay on the effectively-once tier: every emit rides an atomic effect record, and an output leaves only on an output plugin's confirmation ([`effectively_once.md`](../topics/core/effectively_once.md)). |
| `exactly.effectively_once.ack` | `per_input` | When the consumed input is acknowledged. `checkpoint` holds it until the interval closes, so state and offset advance together. Its members: `every` (close after `n` inputs; an explicit `ctx.checkpoint()` still closes it early), `max_delay_ms` (close after this long) and `outputs: at_checkpoint` (hold the interval's outputs and ship them inside its checkpoint record; needs `max_delay_ms`). |
| `exactly.effectively_once.correction_ms` | none: a late input is just an input | A late input inside the window rewinds the cell to a baseline and replays the affected inputs in event-time order. Needs an event-time clock on the tasklet. |
| `exactly.effectively_once.beyond_window` | `process_as_arrival` | What an input later than the window becomes: processed as an arrival, or `drop`ped. |
| `run.steps.each` | `once` | The loop shape. `once` runs the chain over the first message, and the instance is then over. `per_input` runs the chain once per message, for as long as the instance lives. `chained` feeds each pass's last handoff into the next pass and ends on an empty one. |
| `run.steps.on_failure` | `undo_all` | What a step that fails for good does. `undo_all` undoes every finished step, last one first. `keep_progress` keeps every committed pass and undoes only the one in flight; in a `per_input` loop the failed turn is set aside and the loop goes on. |
| `durable` | `local` | Where an instance keeps its work. `false`: in memory only, and a crash starts over. `local`: on the controller's disk. `remote`: each commit point (a step, a flow input, a flow checkpoint) is acknowledged only once the remote durability backend holds it. |
| `durable.<local\|remote>.sleep_after` | `false` for a flow class, six hours for a steps class | An instance idle this long is written out, leaves its runner, and wakes on its next message with its cells restored. `false` never sleeps. A sleeping flow instance has its input written to disk before it is forwarded, so a flow class pays for that only when it sets an interval. |

`ack.checkpoint` with `outputs: at_checkpoint` trades latency for throughput:
the interval closes on whichever comes first of `every` inputs or
`max_delay_ms`, so an output waits up to `max_delay_ms` longer, and the
relay writes one log record per interval instead of one per input. Leave it
out for the lowest latency
([the benchmarking guide](https://github.com/flezha/flezha/blob/main/docs/internal/operations/benchmarking.md)
shows how the numbers trade).

## Flow or steps

The one question the tree asks first is where the commit point is.

| | `run: flow` | `run: steps` |
| --- | --- | --- |
| Commit point | one input, or one checkpoint | one finished step |
| Input | each message is handled the moment it arrives | the chain runs over its message, step by step |
| A step sees a value | as soon as the step before it emits it | once the step before it has finished and that is on disk |
| Ends | when stopped, when a tasklet calls `complete`, or at `max_duration_ms` | when the chain has run, when a tasklet calls `complete`, when a `chained` loop runs out of work, at `max_duration_ms`, or when stopped |
| A failure that does not go away | the input is set aside in the dead-letter queue, and the stream goes on | the steps that finished are undone (`undo_all`), or the turn is set aside (`keep_progress` in a `per_input` loop) |
| Sleeps when idle | not unless the class sets `sleep_after` | after six hours, unless the class says otherwise |
| Typical use | an ETL stream: [`streams.md`](../topics/core/streams.md) | a workflow: [`workflows.md`](../topics/core/workflows.md) |

Both runs use the same graph builder, the same tasklets, the same cells and the
same retry settings, and the controller places, restarts and moves both the
same way. A flow instance that ends on `complete` first runs the work its cells
already hold to its outputs, as an external stop does.

## Three layers, one place for each setting

Every setting lives in exactly one layer:

| Layer | What it holds | Examples |
| --- | --- | --- |
| The class (code) | everything the relay promises: the settings tree, the graph, each edge's options, each tasklet's options | `run`, `durable`, `retry_max`, `key_by`, `mailbox_capacity` |
| The start (`flz relay start`) | only what makes an instance this instance | `--id`, `--partitions`, `--init-values`, the first message (`--text`, `--channel`), the plugins attached at start (`--plugins`), the namespace, the caller's credential |
| The environment (`FLEZHA_*`) | only where a relay runs and what it costs | `FLEZHA_DURABILITY_BACKEND`, `FLEZHA_STEPS_RETENTION_HOURS`, `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` |

No configuration file overrides a class's value per class: the controller has
no `%{class => value}` map, and a `:flezha` key no setting reads stops the
boot. A tasklet's mailbox capacity is the one class option with an
environment default: `FLEZHA_MAILBOX_CAPACITY` applies only to a tasklet that
declares none.

`flz relay start` takes no option that changes how the class behaves. An
option such as `--snapshot-interval` is refused as unknown, and
`flz relay start --help` lists none. The start names only the factory: the
run is read from the class and announced by the runner when it registers the
factory, so `flz relay start` and `flz schedule add` take no run.

There is one check across the layers: a class that declares `durable: remote`,
started on a controller whose `FLEZHA_DURABILITY_BACKEND` is `none`, is
refused at start with a message naming `FLEZHA_DURABILITY_BACKEND`, and no
instance is created.

Beside the tree, the code declares two smaller sets of options, listed in
full [below](#channel-and-edge-options):

- **Edge options**, on the channel that feeds a tasklet: `retry_max` (3
  attempts by default), `retry_backoff_ms` (a 50 ms base), `on_full` (what a
  full mailbox does), `key_by` (one cell per key) and `keyed` (every plugin
  output carries a natural key). The retry settings belong to the edge in both
  runs; a class has none of its own.
- **Tasklet options**, on the tasklet: its `name`, fixed cell keys, the
  mailbox capacity, the cap on cells, how long an idle cell lives, the state's
  schema version and its clock.

## How each SDK writes the tree

Each SDK spells the tree in its own idiom. Each "one of" is a sum type of its
language, and each nested setting is a member of its parent only.

The same tree in each SDK: a relay that runs in steps, one pass per message,
keeps a failed pass's progress, sleeps after ten idle seconds, stops after ten
minutes, and says what it is for.

<!-- tabs-open -->

### Java

The settings are methods of `Relay`:

```java
public final class OrdersRelay implements Relay {
    ...
    @Override
    public Run run() {
        return new Run.Steps(Each.PER_INPUT, OnFailure.KEEP_PROGRESS);
    }

    @Override
    public Durable durable() {
        return new Durable.Local(Duration.ofSeconds(10));
    }

    @Override
    public Optional<Duration> maxDuration() {
        return Optional.of(Duration.ofMinutes(10));
    }

    @Override
    public String description() {
        return "one order per message";
    }
}
```

### Rust

The settings are one `RelaySettings` value, from `fn settings()`:

```rust
impl Relay for OrdersRelay {
    ...
    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps { each: Each::PerInput, on_failure: OnFailure::KeepProgress },
            durable: Durable::Local { sleep_after: SleepAfter::After(Duration::from_secs(10)) },
            max_duration: Some(Duration::from_secs(600)),
            description: "one order per message",
        }
    }
}
```

### Node

The settings are fields of `defineRelay`:

```javascript
export default defineRelay({
  factoryId: 'orders',
  steps: { each: 'per_input', onFailure: 'keep_progress' },
  durable: { local: { sleepAfter: 10_000 } },
  maxDurationMs: 600_000,
  description: 'one order per message',
  channels: (c) => {
    ...
  },
});
```

### Python

The settings are keywords of `@flezha.relay`:

```python
@flezha.relay(
    "orders",
    run={"steps": {"each": "per_input", "on_failure": "keep_progress"}},
    durable={"local": {"sleep_after": 10_000}},
    max_duration_ms=600_000,
    description="one order per message",
)
def orders(c: flezha.Channels) -> None:
    ...
```

### Ruby

The settings are calls in the class body:

```ruby
class Orders < Flezha::Relay
  factory_id "orders"
  run steps: { each: :per_input, on_failure: :keep_progress }
  durable local: { sleep_after: 10_000 }
  max_duration_ms 600_000
  description "one order per message"
  ...
end
```

### PHP

The settings are arguments of `#[Relay]`:

```php
#[Relay(
    factoryId: 'orders',
    run: ['steps' => ['each' => 'per_input', 'on_failure' => 'keep_progress']],
    durable: ['local' => ['sleep_after' => 10_000]],
    maxDurationMs: 600_000,
    description: 'one order per message',
)]
final class Orders extends \Flezha\Relay
{
    ...
}
```

### Elixir

The settings are options of `use Flezha.SDK.Relay`:

```elixir
defmodule Orders do
  use Flezha.SDK.Relay,
    factory_id: "orders",
    run: {:steps, each: :per_input, on_failure: :keep_progress},
    durable: {:local, sleep_after: 10_000},
    max_duration_ms: 600_000,
    description: "one order per message"
  ...
end
```

<!-- tabs-close -->

A few spellings differ from the tree's names:

- **Java**: `durable()` takes `Durable.Memory()` for `false`. `sleepAfter` is an
  `Optional<Duration>`, and empty never sleeps. `EffectivelyOnce` and
  `Ack.Checkpoint` take `withAck`, `withCorrectionMs`, `withBeyondWindow`,
  `withEvery`, `withMaxDelayMs` and `withOutputs`.
- **Node**: the run is written as its branch, `flow: {...}` or `steps: {...}`,
  never both, because a short form's `run` is its tasklet body. The tree
  reaches the core as JSON, members spelled as the core reads them
  (`sleepAfter` is `sleep_after`).
- **Python**, **Ruby**, **PHP**: `run` and `durable` are written bare
  (`"steps"`, `:local`) or as a one-key map holding the branch's members.
  Python's `Run` and `Durable` type aliases spell the tree with `Literal`
  unions for a type checker. A PHP attribute argument is a constant
  expression, so a tree the environment chooses is a constant the program
  defines before it registers the class.
- **Elixir**: a branch with members is a tuple, such as
  `{:effectively_once, ack: ack, correction_ms: ms, beyond_window: :drop}` or
  `{:checkpoint, every: n, max_delay_ms: ms, outputs: :at_checkpoint}`.

The SDK reference pages list each SDK's whole spelling:
[Java](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/java_sdk.md#the-settings-tree),
[Node](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/node_sdk.md), [Python](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/python_sdk.md#class-settings),
[Ruby](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/ruby_sdk.md#class-settings),
[PHP](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/php_sdk.md#class-settings) and
[Elixir](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/elixir_sdk.md#the-settings-tree).

## How the tree is checked

What an author writes always reaches a judgement; no SDK accepts a setting and
ignores it.

- **A setting under the wrong parent** (`each` under `flow`, `ack` or
  `correction_ms` under `steps`, `sleep_after` under `durable: false`) does not
  compile in Rust and Java, because the type under that parent has no such
  member. Elixir, Node, Python, Ruby and PHP pass the tree as written, and it
  is refused when the runner registers the class, as `topology.unknown_field`
  naming the setting and its parent.
- **A value outside its choices** is refused as `topology.invalid_setting`,
  naming the setting and the value. In a Java short form, two branches of a
  "one of" written at once are refused the same way.
- **One combination is refused**: `exactly: effectively_once` with
  `durable: false`, as `topology.effectively_once_without_persistence`, because
  the tier is durable state.
- **Value checks on one setting** stay: `topology.checkpoint_every_zero`,
  `topology.checkpoint_max_delay_zero`, `topology.at_checkpoint_without_max_delay`,
  `topology.correction_window_zero` (a `beyond_window` with no `correction_ms`
  is a zero window) and `topology.correction_without_event_time`.
- **The tier's graph rules**: under `effectively_once`, every channel with a
  plugin output must be declared `keyed`
  (`topology.unkeyed_channel_under_effectively_once`), and no channel may
  declare a dropping overflow policy, `DropNewest` or `DropOldest`
  (`topology.dropping_policy_under_effectively_once`); the same holds for a
  class that declares a correction window.

Every code is listed in the [error catalogue](../errors.md).

## Seeing what an instance runs with

`flz relay info` shows the run and the durability of an instance, as its
runner announced them:

```sh
$ flz relay info <relay_id>
```

```output
  Run: steps (each: per_input, on_failure: keep_progress)
  Durable: local (sleep_after: 6h)
```

A flow instance shows `Run: flow`, or `Run: flow (exactly: effectively_once)`
on the tier. A class kept in memory shows `Durable: false`, since it has
nothing to sleep on, and one that never sleeps shows `sleep_after: false`.

## Example: a workflow, in steps

In the tradedesk example, `order_executor` places one order: reserve the
money, place the order, then confirm the fill and book it. An order has a start
and an end, each finished step must be on disk before the next one starts, and
a step that cannot succeed must undo the ones before it. So the class runs in
steps. `each: once` runs the chain over the instance's first message, the
order, and the instance is then over. `on_failure: undo_all` undoes every
finished step when one fails for good. Both are the defaults; the class writes
them out so it says what it does.

<!-- tabs-open -->

### Java

```java
// OrderExecutorRelay.java: the workflow. It runs in steps, once, undoing everything on a failure.
public final class OrderExecutorRelay implements Relay {
    ...
    @Override
    public Run run() {
        return new Run.Steps(Each.ONCE, OnFailure.UNDO_ALL);
    }
    ...
}
```

Full file: [`OrderExecutorRelay.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/tradedesk/src/main/java/com/flezha/examples/tradedesk/OrderExecutorRelay.java).
`run()` answers one branch of the sealed `Run`; `Each` and `OnFailure` exist
only as members of `Run.Steps`.

### Rust

```rust
// src/order_executor.rs: the workflow. It runs in steps, once, undoing everything on a failure.
impl Relay for OrderExecutorRelay {
    ...
    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps { each: Each::Once, on_failure: OnFailure::UndoAll },
            ..RelaySettings::default()
        }
    }
}
```

Full file: [`src/order_executor.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/tradedesk/src/order_executor.rs).
`..RelaySettings::default()` keeps the defaults for `durable`,
`max_duration` and `description`.

### Node

```javascript
// order_executor_relay.mjs: the workflow. It runs in steps, once, undoing everything on a failure.
export default defineRelay({
  factoryId: 'order_executor',
  steps: { each: 'once', onFailure: 'undo_all' },
  channels: (c) => {
    ...
  },
});
```

Full file: [`order_executor_relay.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/tradedesk/order_executor_relay.mjs).
The `steps` field is the run's branch, and its members sit inside it.

### Python

```python
# order_executor.py: the workflow. It runs in steps, once, undoing everything on a failure.
@flezha.relay("order_executor", run={"steps": {"each": "once", "on_failure": "undo_all"}})
def order_executor(c: flezha.Channels) -> None:
    ...
```

Full file: [`order_executor.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/tradedesk/order_executor.py).
`run` is a one-key dict: the branch, and its members.

### Ruby

```ruby
# order_executor_relay.rb: the workflow. It runs in steps, once, undoing everything on a failure.
class OrderExecutorRelay < Flezha::Relay
  factory_id "order_executor"
  run steps: { each: :once, on_failure: :undo_all }
  ...
end
```

Full file: [`order_executor_relay.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/tradedesk/order_executor_relay.rb).
`run` takes the branch as its one keyword, and its members as a Hash.

### PHP

```php
// src/OrderExecutorRelay.php: the workflow. It runs in steps, once per message, undoing everything on a failure.
#[Relay(factoryId: 'order_executor', run: ['steps' => ['each' => 'per_input', 'on_failure' => 'undo_all']])]
final class OrderExecutorRelay extends \Flezha\Relay
{
    ...
}
```

Full file: [`src/OrderExecutorRelay.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/tradedesk/src/OrderExecutorRelay.php).
The PHP example departs from the others with `'each' => 'per_input'`. A PHP
runner serves every relay on one thread, so its `confirm_fill` waits for
the exchange with a cell timer rather than a sleep, and a timer outlives its
step only while the instance does. A `once` instance would end, timer and all,
as soon as its last step committed; a `per_input` one stays until the timer's
`complete` ends it. Nothing sends it a second message.

### Elixir

```elixir
# lib/tradedesk/order_executor.ex: the workflow. It runs in steps, once, undoing everything on a failure.
defmodule Tradedesk.OrderExecutor do
  use Flezha.SDK.Relay,
    factory_id: "order_executor",
    run: {:steps, each: :once, on_failure: :undo_all}
  ...
end
```

Full file: [`lib/tradedesk/order_executor.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/tradedesk/lib/tradedesk/order_executor.ex).
`run:` is a tuple: the branch, then its members as a keyword list.

<!-- tabs-close -->

Neither class writes `durable`, so both are kept on the controller's disk. An
idle `order_executor` instance would sleep after six hours, though a `once`
order ends long before that.

## Example: sleeping and effectively once

The agentdesk example uses the rest of the tree. Its `agent` relay is one
conversation: it runs in steps, one turn per message (`each: per_input`), a
failed turn fails alone (`on_failure: keep_progress`), and an idle session
sleeps after ten seconds. Its `gateway` relay, which answers HTTP requests,
runs in flow on the effectively-once tier.

<!-- tabs-open -->

### Java

```java
// AgentRelay.java: one conversation, one turn per message, asleep when idle.
@Override
public Run run() {
    return new Run.Steps(
            Each.PER_INPUT,                          // one turn per message
            OnFailure.KEEP_PROGRESS);                // a failed turn fails alone
}

@Override
public Durable durable() {
    return new Durable.Local(Duration.ofSeconds(10)); // an idle session sleeps after 10 s
}
```

Full file: [`AgentRelay.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/AgentRelay.java).

```java
// GatewayRelay.java: the HTTP side, in flow, effectively once.
@Override
public Run run() {
    return new Run.Flow(new Exactly.EffectivelyOnce());
}
```

Full file: [`GatewayRelay.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/GatewayRelay.java).

### Rust

```rust
// src/agent.rs: one conversation, one turn per message, asleep when idle.
impl Relay for AgentRelay {
    ...
    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,                // one turn per message
                on_failure: OnFailure::KeepProgress, // a failed turn fails alone
            },
            durable: Durable::Local {
                sleep_after: SleepAfter::After(Duration::from_secs(10)), // an idle session sleeps after 10 s
            },
            ..RelaySettings::default()
        }
    }
}
```

Full file: [`src/agent.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/agentdesk/src/agent.rs).

```rust
// src/gateway.rs: the HTTP side, in flow, effectively once.
fn settings() -> RelaySettings {
    RelaySettings {
        // each request and answer on disk before it runs
        run: Run::Flow {
            exactly: Exactly::effectively_once(),
        },
        ..RelaySettings::default()
    }
}
```

Full file: [`src/gateway.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/agentdesk/src/gateway.rs).

### Node

```javascript
// agent_relay.mjs: one conversation, one turn per message, asleep when idle.
export default defineRelay({
  factoryId: 'agent',
  // one turn per message; a failed turn fails alone
  steps: { each: 'per_input', onFailure: 'keep_progress' },
  durable: { local: { sleepAfter: 10_000 } }, // an idle session sleeps after 10 s
  channels: (c) => {
    ...
  },
});
```

Full file: [`agent_relay.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/agentdesk/agent_relay.mjs).

```javascript
// gateway_relay.mjs: the HTTP side, in flow, effectively once.
export default defineRelay({
  factoryId: 'gateway',
  flow: { exactly: 'effectively_once' }, // each request and answer on disk before it runs
  channels: (c) => {
    ...
  },
});
```

Full file: [`gateway_relay.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/agentdesk/gateway_relay.mjs).

### Python

```python
# agent.py: one conversation, one turn per message, asleep when idle.
@flezha.relay(
    "agent",
    # one turn per message; a failed turn fails alone
    run={"steps": {"each": "per_input", "on_failure": "keep_progress"}},
    durable={"local": {"sleep_after": 10_000}},  # an idle session sleeps after 10 s
)
def agent(c: flezha.Channels) -> None:
    ...
```

Full file: [`agent.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/agentdesk/agent.py).

```python
# gateway.py: the HTTP side, in flow, effectively once.
# each request and answer on disk before it runs
@flezha.relay("gateway", run={"flow": {"exactly": "effectively_once"}})
def gateway(c: flezha.Channels) -> None:
    ...
```

Full file: [`gateway.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/agentdesk/gateway.py).

### Ruby

```ruby
# agent_relay.rb: one conversation, one turn per message, asleep when idle.
class AgentRelay < Flezha::Relay
  factory_id "agent"
  run steps: { each: :per_input, on_failure: :keep_progress }   # one turn per message; a failed turn fails alone
  durable local: { sleep_after: 10_000 }                       # an idle session sleeps after 10 s
  ...
end
```

Full file: [`agent_relay.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/agentdesk/agent_relay.rb).

```ruby
# gateway_relay.rb: the HTTP side, in flow, effectively once.
class GatewayRelay < Flezha::Relay
  factory_id "gateway"
  run flow: { exactly: :effectively_once } # each request and answer on disk before it runs
  ...
end
```

Full file: [`gateway_relay.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/agentdesk/gateway_relay.rb).

### PHP

```php
// src/AgentRelay.php: one conversation, one turn per message, asleep when idle.
#[Relay(
    factoryId: 'agent',
    run: ['steps' => [
        'each' => 'per_input',                       // one turn per message
        'on_failure' => 'keep_progress',             // a failed turn fails alone
    ]],
    durable: ['local' => ['sleep_after' => 10_000]], // an idle session sleeps after 10 s
)]
final class AgentRelay extends \Flezha\Relay
{
    ...
}
```

Full file: [`src/AgentRelay.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/agentdesk/src/AgentRelay.php).

```php
// src/GatewayRelay.php: the HTTP side, in flow, effectively once.
#[Relay(
    factoryId: 'gateway',
    run: ['flow' => ['exactly' => 'effectively_once']], // each request and answer on disk before it runs
)]
final class GatewayRelay extends \Flezha\Relay
{
    ...
}
```

Full file: [`src/GatewayRelay.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/agentdesk/src/GatewayRelay.php).

### Elixir

```elixir
# lib/agentdesk/agent_relay.ex: one conversation, one turn per message, asleep when idle.
use Flezha.SDK.Relay,
  factory_id: "agent",
  # one turn per message; a failed turn fails alone
  run: {:steps, each: :per_input, on_failure: :keep_progress},
  # an idle session sleeps after 10 s
  durable: {:local, sleep_after: 10_000}
```

Full file: [`lib/agentdesk/agent_relay.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/agentdesk/lib/agentdesk/agent_relay.ex).

```elixir
# lib/agentdesk/gateway_relay.ex: the HTTP side, in flow, effectively once.
use Flezha.SDK.Relay,
  factory_id: "gateway",
  # each request and answer on disk before it runs
  run: {:flow, exactly: :effectively_once}
```

Full file: [`lib/agentdesk/gateway_relay.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/agentdesk/lib/agentdesk/gateway_relay.ex).

<!-- tabs-close -->

Effectively once written with no members, as every gateway above writes it,
takes each member at its default: `ack: per_input`, no correction window. How a
sleeping instance leaves its runner and wakes: [the relay lifecycle](https://github.com/flezha/flezha/blob/main/docs/internal/architecture/relay_lifecycle.md).

## Channel and edge options

Declared on the stream handle returned by `input(...)` or a node's output.

| Declaration | Default | What it does |
|---|---|---|
| `.on_full(OverflowPolicy)` (`onFull`) | `DropNothing` | What a full Cell mailbox discards: `DropNothing`, `DropNewest`, `DropOldest`. Nothing drops by default; `DropNothing` slows the producer to the consumer's rate instead. |
| `.retry_max(n)` (`retryMax`) | `3` attempts | Attempts before a failing payload is parked to the dead-letter queue, or, into a steps tasklet, before the tasklet fails and the run compensates (a `per_input` keep-progress session parks the turn instead and goes on). `0` is refused (`topology.retry_max_zero`). |
| `.retry_backoff_ms(ms)` (`retryBackoffMs`) | `50` | Base of the exponential backoff between attempts, for both runs; the wait holds no thread. |
| `.keyed()` (`keyed`) | off | Every plugin emit must carry a natural key; the runtime stamps `flz.cell` / `flz.seq` / `flz.key`. Requires an `output`; required on every plugin channel under `effectively_once`. |
| `.key_by(f)` (`keyBy`) | absent — one Cell | Route by a key function, making the consumer dynamic-cell. |
| `.output(name)` / `.output_with(name, f)` (`output`) | — | Wire the channel to a named plugin; a second output on one channel is a registration error. |

## Tasklet-level options

Overridden on the `Tasklet` trait (Rust) or Tasklet class (Java).

| Override | Default | What it does |
|---|---|---|
| `compact_after_bytes()` (`compactAfterBytes`) | 8 MiB, in lockstep with `:default_compact_after_bytes` | Per-Cell WAL size reported to the controller as the snapshot-compaction trigger. |
| `mailbox_capacity()` (`mailboxCapacity`) | `1024`, range `1..=1_048_576` | Bounded mailbox depth per Cell: the declared value, else `FLEZHA_MAILBOX_CAPACITY`, else the default. Rust sets it with the `capacity` key on `#[flezha::tasklet]`. |
| `cell_idle_ttl()` (`cellIdleTtl`; Elixir `idle_ttl_seconds/0`) | `None` — no eviction | Evict a dynamic Cell after this idle period; eviction is destructive (slot, WAL and snapshot dropped). |
| `max_cells()` (Elixir `max_cells/0`) | `100_000` | Soft cap on live dynamic Cells per Tasklet; new keys past it are dropped. |
| `CLOCK` (set by a written `event_time`, or the `logical_clock` key) | wall clock | Wall clock, event time or logical clock; decides `ctx.now()` and event-time ordering. |

A steps tasklet's retry ladder is its input edge's (above), and a commit
barrier whose ack timed out is re-sent three times, a constant, before the run
ends abnormally.

## Limits and gotchas

- **The run belongs to the factory.** The controller learns a factory's run
  from the first runner that announces it. A runner that announces the other
  run for the same factory id, or none, is never chosen to place it. A
  schedule of a factory announced as flow is refused at `flz schedule add`,
  and a tasklet's spawn naming one answers `factory_not_steps`.
- **A second message to a `once` instance** is accepted and never read. Use
  `each: per_input` for an instance that takes one message after another.
- **A `per_input` steps instance has no natural end**: it runs until it is
  stopped, a tasklet calls `complete`, or `max_duration_ms` passes. That is
  allowed, not refused.
- **`sleep_after` on a flow class has a cost**: the instance's input is written
  to disk before it is forwarded, so that a sleeping instance loses nothing.
  A flow class that never sleeps does not pay it.
- **`durable: false` keeps nothing**: a crash starts the instance over, its
  cells cannot be read live (`flz state get`), it cannot sleep, and it cannot
  run on the effectively-once tier.
- **`durable: remote` needs a backend** on the controller
  ([`durability.md`](../topics/core/durability.md)); without one, the start is refused.
- **The defaults differ per run** only for `sleep_after`: six hours for a
  steps class, never for a flow class.

## See also

- [Streams](../topics/core/streams.md): what a relay in flow does with each message.
- [Workflows](../topics/core/workflows.md): steps, undo, retries and loop shapes.
- [Sessions](../topics/core/sessions.md): a relay that runs in steps once per message.
- [Durability and delivery](../topics/core/durability.md): what each `durable` level keeps through a crash.
- [Effectively once](../topics/core/effectively_once.md): the tier, checkpoints and late-input correction.
- [Running and testing](../topics/core/running_and_testing.md): starting, stopping and inspecting instances.
- [Timers](../topics/core/timers.md): cell timers in either run.
- [`../errors.md`](../errors.md): every refusal code.
- Tutorials: [`watch_the_trades.md`](tutorials/watch_the_trades.html)
  builds `trade_watcher`, and [`place_orders.md`](tutorials/place_orders.html)
  builds `order_executor`.
