# Operator commands

The groups that work with operators: starting and stopping them, sending them
input, reading their state, the inputs they gave up on, and the workflows
started on a schedule.

Each section shows what the group's help prints and, for the verbs used most,
a command with its answer. Unless a section names another page, the answers
come from the echo of the [tutorials](../../tutorials/echo/run_the_echo.md).
How `flz` reaches the controller, its options and its exit codes are on the
[CLI](cli.md) page, and every verb in one list is the
[command reference](command_reference.md).

## `flz operator`

Starts and stops operators, sends them input, and shows what they answer and emit. Explained in [`running_operators.md`](../basics/running_operators.md), [`workflows.md`](../basics/workflows.md), [`partitioning.md`](../advanced/partitioning.md), [`operator_calls.md`](../advanced/operator_calls.md), [`rewinding_a_turn.md`](../advanced/rewinding_a_turn.md), tutorial [Run the echo](../../tutorials/echo/run_the_echo.md).

```sh
$ flz operator help
```

```output
flz operator - Operator management commands

USAGE:
    flz operator <COMMAND>

COMMANDS:
    start       Start an operator
    scale       Rescale a partition group to N partitions
    stop        Stop an operator
    state       Get operator state
    list        List operators
    info        Get operator info
    listen      Print the frames an operator emits or ingests
    send        Send a payload to an operator's input stream
    call        Send one input and print the operator's answer
    rewind      Rewind a steps loop to a committed iteration
    replay      Re-run a Cell's retained inputs from a point in the past
    help        Show this help message

EXAMPLES:
    flz operator start candles --id ohlc --partitions 3
    flz operator scale ohlc 5
    flz operator send orchestrator --text '{"task":"refactor"}'
    flz operator send my_echo --text 'hello world' --await 2000
    flz operator send prices --key EURUSD --text x
    flz operator send orders --text x --msg-id orders/0/41
    flz operator call echo --text hello
    flz operator listen my_echo --duration-ms 10000
    flz operator rewind chat-1 --to-iteration 3
    flz operator replay candles/aggregate/BTC --from 1720000000000

Use 'flz operator <COMMAND> --help' for more information on a specific command.

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var.
```

Each verb's `--help` starts with its usage line:

```output
flz operator stop <operator_id> [--namespace | -n <namespace>]
flz operator state <operator_id> [--namespace | -n <namespace>]
flz operator list [--namespace | -n <namespace>]
flz operator info [--namespace | -n <namespace>]
flz operator send <operator_id> --text | -t <payload> [--channel | -c <ch>] [--key <key>] [--msg-id <id>] [--await <ms>] [--namespace | -n <namespace>]
flz operator scale <group> <partitions> [--namespace | -n <namespace>] [--timeout | -t <ms>]
flz operator rewind <operator_id> --to-iteration <N> [--namespace | -n <namespace>]
flz operator replay <operator/tasklet/key> --from | -f <seq|timestamp> [--namespace | -n <namespace>]
```

`start`, `listen` and `call` print a page of their own. `start`'s:

```sh
$ flz operator start --help
```

```output
flz operator start - Start an operator

USAGE:
    flz operator start <factory_id> [OPTIONS]

OPTIONS:
    --id <id>, -i <id>          Id to start under (default: generated)
    --namespace <ns>            Namespace to start in (default: default)
    --init-values <json>        Seed state at start
    --text <payload>, -t        First message, delivered as `send` does
    --channel <ch>, -c          Entry channel the first message names
    --plugins <json-array>      Plugins to bind at start, enabling each named
                                with a provider when it is not enabled yet
    --partitions <N>            Start N partition members as one group
    --output <text|json>        Output format (default: text)
    --json                      The same as --output json

RUN:
    The factory's own: its class declares how it runs (flow or steps) and
    where its work is kept, and its runner announces it, so a start names only
    the factory.

ID:
    Without --id the controller generates one, <factory>-<16 characters>, and
    prints it: Started echo-k7m2qzxw4pd3fn2a. With --output json the answer is
    {"id": ..., "outcome": "started" | "already_running"}.

    A start without --id that is run again after its answer was lost starts a
    second operator, and a flow one runs until it is stopped. Pass --id
    when the start may be retried: a start of an id that is already running
    prints "<id> is already running, left untouched" and starts nothing.

INIT VALUES:
    A flow operator's document is keyed by Tasklet name, one entry per
    Tasklet to seed. Name only what you want seeded; a Tasklet you leave
    out keeps the state its class builds.

        --init-values '{"compare_filter_tasklet": {"comparable": "hi"}}'
        --init-values '{"ohlc": {"1m": {"high": 0}}}'   one state per declared key

    A name the class does not carry is ignored, not refused: the runner logs
    it and the start still succeeds, so check the spelling. A steps
    operator's tasklets take the document as state the same way.

FIRST MESSAGE:
    --text is the instance's first input, on the entry channel --channel
    names or the first declared one, whichever way it runs; a steps one started
    without it waits for `flz operator send`. --channel needs --text.

EXAMPLES:
    flz operator start do_nothing
    flz operator start do_nothing --id my_operator
    flz operator start candles --id ohlc --partitions 3
    flz operator start order_aggregator -i orders --namespace prod -o json
    flz operator start order_executor --text '{"symbol":"EUR/USD","quantity":10}' --channel order
```

The echo from the tutorials, started, listed, sent a message and stopped:

```sh
$ flz operator start echo --id my_echo
```

```output
Started my_echo
```

```sh
$ flz operator list
```

```output
Fetching operators for namespace 'default'...
Operators in namespace 'default':

  OPERATOR                          HEALTH      PARTS   NODE                  RUNNER
  my_echo                           healthy     -       flezha@127.0.0.1      default_23fc85126379
```

`--await` waits that many milliseconds for what the operator emits:

```sh
$ flz operator send my_echo --text 'hello world' --await 2000
```

```output
Delivered to 'my_echo'.
out	echo: hello world
```

`--msg-id` gives the message an id. An operator with an input log (effectively
once, durable steps, a class that sleeps) absorbs a repeat of an id it still
holds, so a script that may run twice sends each message once:

```sh
$ flz operator send orders --text x --msg-id orders/0/41
```

```output
Absorbed (duplicate) by 'orders'.
```

```sh
$ flz operator info
```

```output
Fetching operator information for namespace 'default'...
Operator information for namespace 'default':

Operator: my_echo
  Run: flow
  Durable: local (sleep_after: false)
  Phase: running
  Health: healthy
  Runner ID: default_06355a1b5a63
  Group ID: default
  Uptime (ms): 11819
  Default Input Type: 2
  Waiting callers: 0
  Plugins:
    - reader: Flezha.Plugin.Provider.File.Source
        path: /opt/flezha/app_data/files/echo-in.txt
        health: :healthy
    - writer: Flezha.Plugin.Provider.File.Sink
        hook: on_output []
        path: /opt/flezha/app_data/files/echo-out.txt
        health: :healthy
```

A name that is not running fails with its code, exit 5:

```sh
$ flz operator send ghost --text hi
```

```output
error: operator 'ghost' is not running in namespace 'default' (operator.not_found)
hint: flz operator list --namespace default
```

```sh
$ flz operator stop my_echo
```

```output
Stopping operator...
Operator has been stopped
Operator stopped successfully.
```

`call` waits for the answer and prints only that, so a script can use it as
it is. The echo of [`operator_calls.md`](../advanced/operator_calls.md#example)
answers through a `response()` end:

```sh
$ flz operator call echo --text hello
```

```output
hello
```

## `flz state`

Reads an operator's stored state, now or at a point in the past, and manages how long it is kept. Explained in [`state.md`](../basics/state.md#reading-a-cell-s-state), [`workflows.md`](../basics/workflows.md), [`durability.md`](../basics/durability.md).

```sh
$ flz state help
```

```output
flz state - Per-Cell state inspection and time-travel queries

USAGE:
    flz state <SUBCOMMAND>

SUBCOMMANDS:
    get <operator/tasklet[/key]>             Current in-memory state of a live Cell, with its change_seq
    list <operator>                          List Cells under an Operator class
    at <operator/tasklet/key> <seq|ts>       Query state at a point in time
    rotate <operator/tasklet/key>            Force a compaction snapshot
    retention <operator> <hours>             Set time-travel retention
    delete <operator/tasklet/key>            Hard-delete a Cell's persisted state
    outbox <operator/tasklet/key>            Effectively-once outbox status (lag, cursor, watermark)
    journal <operator/instance>              Steps instance task-journal (committed tasks, iterations, park)
    inbox <operator/instance>                Steps instance durable inbox (offsets, undelivered tail)
    help                                     Show this help message

EXAMPLES:
    flz state get MarketOperator/OhlcTasklet/M1
    flz state get MarketOperator/OhlcTasklet --output json
    flz state list MarketOperator
    flz state at MarketOperator/OhlcTasklet/M1 12345
    flz state at MarketOperator/OhlcTasklet/M1 2026-05-01T12:00:00Z
    flz state rotate MarketOperator/OhlcTasklet/M1
    flz state retention MarketOperator 6
    flz state delete MarketOperator/OhlcTasklet/M1
    flz state outbox MarketOperator/OhlcTasklet/M1
    flz state journal etl-job-42
    flz state inbox chat-session-7
```

A cell's state is read by `<operator>/<tasklet>`, here from
[`state.md`](../basics/state.md#reading-a-cell-s-state):

```sh
$ flz state get anna/reply --namespace agents
```

```output
state get anna/reply:
  change_seq: 3
  state:      {"reply":{"messages":["Hello","What can you do?","Are you still there?"]}}
```

## `flz dlq`

Lists the inputs an operator gave up on after its retries, and replays or drops them. Explained in [`durability.md`](../basics/durability.md#the-dead-letter-queue).

```sh
$ flz dlq help
```

```output
Dead-letter queue: parked poison payloads (retry budget exhausted).

Usage:
  flz dlq list [operator]                        Parked entries, nearest-to-expiry first
  flz dlq show <operator/tasklet/entry-id>       Entry metadata + payload preview
  flz dlq replay <operator/tasklet/entry-id>     Re-dispatch through the normal input path
  flz dlq drop <operator/tasklet/entry-id>       Discard the entry (audited)

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var.
```

An input that failed every retry, from
[`durability.md`](../basics/durability.md#the-dead-letter-queue):

```sh
$ flz dlq list 'trade_watcher#p1' --namespace trading
```

```output
ENTRY                                                       CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/track_prices/307bcbd6439a080f              EUR/USD         trades      1    8s        TypeError: the price is not a number: "one euro"
```

## `flz schedule`

Starts a workflow on a cron schedule. Explained in [`workflows.md`](../basics/workflows.md#one-instance-per-piece-of-work), [`configuring_an_operator.md`](../basics/configuring_an_operator.md).

```sh
$ flz schedule help
```

```output
flz schedule - cron-scheduled steps operator spawns

USAGE:
    flz schedule <SUBCOMMAND>

SUBCOMMANDS:
    add <name> --cron "<expr>" --tz <iana> --factory <factory_id>
        [--namespace <ns>] [--init-values <json>] [--text <payload>] [--channel <ch>]
        [--plugins <json>]
                 Add a durable schedule of a steps factory; a factory a
                 connected runner announced as flow is refused.
                 --init-values is the state every fired instance starts with;
                 --text is the first message it receives, on the entry channel
                 --channel names or the first declared one.
                 --plugins takes the same array as `operator start --plugins`;
                 each firing binds its instance to the plugins it names.
    list         List schedules with their next fire time
    remove <name>  Remove a schedule
    run <name>     Fire one instance immediately with the stored config;
                   judged as operator.start in the schedule's namespace
    help         Show this help message

EXAMPLES:
    flz schedule add digest --cron "0 8 * * *" --tz Europe/Tallinn --factory ai_digest
    flz schedule add nightly --cron "0 2 * * *" --tz UTC --factory report --text '{"day":"today"}' --channel requests
    flz schedule list
    flz schedule run digest

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var.
```
