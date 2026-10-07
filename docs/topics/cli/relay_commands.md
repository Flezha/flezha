# Relay commands

The groups that work with relays: starting and stopping them, sending them
input, reading their state, the inputs they gave up on, and the workflows
started on a schedule.

Each section shows what the group's help prints and, for the verbs used most,
a command with its answer. Unless a section names another page, the answers
come from the echo of [Up and running](../../getting_started/up_and_running.md#run-the-echo).
How `flz` reaches the controller, its options and its exit codes are on the
[CLI](cli.md) page, and every verb in one list is the
[command reference](command_reference.md).

## `flz relay`

Starts and stops relays, sends them input, and shows what they answer and emit. Explained in [`running_and_testing.md`](../core/running_and_testing.md), [`workflows.md`](../core/workflows.md), [`partitioning.md`](../core/partitioning.md), [`sending_and_calling.md`](../core/sending_and_calling.md#calls), [`sessions.md`](../core/sessions.md#rewinding-a-turn), [Run the echo](../../getting_started/up_and_running.md#run-the-echo).

```sh
$ flz relay help
```

```output
flz relay - Relay management commands

USAGE:
    flz relay <COMMAND>

COMMANDS:
    start       Start a relay
    scale       Rescale a partition group to N partitions
    stop        Stop a relay
    state       Get relay state
    list        List relays
    info        Get relay info, or one relay's API
    diagnose    Say what is wrong with a relay and what to do
    listen      Print the frames a relay emits or ingests
    send        Send a payload to a relay's input stream
    call        Send one input and print the relay's answer
    rewind      Rewind a steps loop to a committed iteration
    replay      Re-run a Cell's retained inputs from a point in the past
    help        Show this help message

EXAMPLES:
    flz relay start candles --id ohlc --partitions 3
    flz relay scale ohlc 5
    flz relay send orchestrator --text '{"task":"refactor"}'
    flz relay send my_echo --text 'hello world' --await 2000
    flz relay send prices --key EURUSD --text x
    flz relay send orders --text x --msg-id orders/0/41
    flz relay call echo --text hello
    flz relay diagnose orders-eu
    flz relay listen my_echo --duration-ms 10000
    flz relay rewind chat-1 --to-iteration 3
    flz relay replay candles/aggregate/BTC --from 1720000000000

Use 'flz relay <COMMAND> --help' for more information on a specific command.

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var.
```

Each verb's `--help` starts with its usage line:

```output
flz relay stop <relay_id> [--namespace | -n <namespace>]
flz relay state <relay_id> [--namespace | -n <namespace>]
flz relay list [--namespace | -n <namespace>]
flz relay info [<relay_id>] [--namespace | -n <namespace>]
flz relay diagnose <relay_id> [--namespace | -n <namespace>]
flz relay send <relay_id> --text | -t <payload> [--channel | -c <ch>] [--key <key>] [--msg-id <id>] [--await <ms>] [--namespace | -n <namespace>]
flz relay scale <group> <partitions> [--namespace | -n <namespace>] [--timeout | -t <ms>]
flz relay rewind <relay_id> --to-iteration <N> [--namespace | -n <namespace>]
flz relay replay <relay/tasklet/key> --from | -f <seq|timestamp> [--namespace | -n <namespace>]
```

`start`, `listen` and `call` print a page of their own. `start`'s:

```sh
$ flz relay start --help
```

```output
flz relay start - Start a relay

USAGE:
    flz relay start <factory_id> [OPTIONS]

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
    second relay, and a flow one runs until it is stopped. Pass --id
    when the start may be retried: a start of an id that is already running
    prints "<id> is already running, left untouched" and starts nothing.

INIT VALUES:
    A flow relay's document is keyed by Tasklet name, one entry per
    Tasklet to seed. Name only what you want seeded; a Tasklet you leave
    out keeps the state its class builds.

        --init-values '{"compare_filter_tasklet": {"comparable": "hi"}}'
        --init-values '{"ohlc": {"1m": {"high": 0}}}'   one state per declared key

    A name the class does not carry is ignored, not refused: the runner logs
    it and the start still succeeds, so check the spelling. A steps
    relay's tasklets take the document as state the same way.

FIRST MESSAGE:
    --text is the instance's first input, on the entry channel --channel
    names or the first declared one, whichever way it runs; a steps one started
    without it waits for `flz relay send`. --channel needs --text.

EXAMPLES:
    flz relay start do_nothing
    flz relay start do_nothing --id my_relay
    flz relay start candles --id ohlc --partitions 3
    flz relay start order_aggregator -i orders --namespace prod -o json
    flz relay start order_executor --text '{"symbol":"EUR/USD","quantity":10}' --channel order
```

The echo from the getting-started pages, started, listed, sent a message and stopped:

```sh
$ flz relay start echo --id my_echo
```

```output
Started my_echo
```

```sh
$ flz relay list
```

```output
Fetching relays for namespace 'default'...
Relays in namespace 'default':

  RELAY                          HEALTH      PARTS   NODE                  RUNNER
  my_echo                           healthy     -       flezha@127.0.0.1      default_23fc85126379
```

`--await` waits that many milliseconds for what the relay emits:

```sh
$ flz relay send my_echo --text 'hello world' --await 2000
```

```output
Delivered to 'my_echo'.
out	echo: hello world
```

`--msg-id` gives the message an id. A relay with an input log (effectively
once, durable steps, a class that sleeps) absorbs a repeat of an id it still
holds, so a script that may run twice sends each message once:

```sh
$ flz relay send orders --text x --msg-id orders/0/41
```

```output
Absorbed (duplicate) by 'orders'.
```

```sh
$ flz relay info
```

```output
Fetching relay information for namespace 'default'...
Relay information for namespace 'default':

Relay: my_echo
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

`diagnose` says what is wrong with one relay and what to do, one block per
finding, and exits 0 whatever it found
([`running_and_testing.md`](../core/running_and_testing.md#diagnose-a-relay)):

```sh
$ flz relay diagnose orders-eu
```

```output
PROBLEM  diagnose.no_runner  no connected runner announces factory 'do_nothing', so relay 'orders-eu' has nowhere to run
         hint: start a runner that defines 'do_nothing', then check it with flz runner list --namespace default
```

A name that is not running fails with its code, exit 5:

```sh
$ flz relay send ghost --text hi
```

```output
error: relay 'ghost' is not running in namespace 'default' (relay.not_found)
hint: flz relay list --namespace default
```

```sh
$ flz relay stop my_echo
```

```output
Stopping relay...
Relay has been stopped
Relay stopped successfully.
```

`call` waits for the answer, the first output the input causes on an output, and
prints only that, so a script can use it as it is. `--answer-channel <ch>` waits
for the output on that channel instead
([`sending_and_calling.md`](../core/sending_and_calling.md#calls)). An echo relay whose tasklet
ends in an output answers with its input:

```sh
$ flz relay call echo --text hello
```

```output
hello
```

`call` goes through the relay's API ([`sending_and_calling.md`](../core/sending_and_calling.md#a-relay-s-api)).
`--text` sends the payload as written; `--arg name=value`, repeated, gives one
field of a JSON input each, typed by the channel's schema. A value that does not
fit is refused before anything is sent, and a channel that answers nothing
prints `accepted`:

```sh
$ flz relay call orders-eu --channel place --arg pair=EUR/USD --arg amount=1000
```

```output
{"amount":1000,"pair":"EUR/USD","status":"placed"}
```

```sh
$ flz relay call orders-eu --channel place --arg pair=EUR/USD --arg amount=lots
```

```output
error: the value for channel 'place' is not a number: amount: "lots" (api.input_invalid)
hint: pass the payload as it is with --text
```

`flz relay info <relay_id>` shows that relay's API: each entry channel with its
format, whether it answers, its description and schema, its MCP tool and an
example call. With `--json` it is the entries as data:

```sh
$ flz relay info orders-eu
```

```output
API of relay 'orders-eu' (factory 'orders'):
  place  json → answer json
    Places an order
    schema: {"properties":{"amount":{"type":"number"},"pair":{"type":"string"}},"required":["pair","amount"],"type":"object"}
    MCP tool: orders_place
    flz relay call orders-eu --channel place --arg pair=... --arg amount=...
  audit  text, no answer
    flz relay call orders-eu --channel audit --text <payload>
```

## `flz state`

Reads a relay's stored state, now or at a point in the past, and manages how long it is kept. Explained in [`state.md`](../core/state.md#reading-a-cell-s-state), [`workflows.md`](../core/workflows.md), [`durability.md`](../core/durability.md).

```sh
$ flz state help
```

```output
flz state - Per-Cell state inspection and time-travel queries

USAGE:
    flz state <SUBCOMMAND>

SUBCOMMANDS:
    get <relay/tasklet[/key]>             Current in-memory state of a live Cell, with its change_seq
    list <relay>                          List Cells under a Relay class
    at <relay/tasklet/key> <seq|ts>       Query state at a point in time
    rotate <relay/tasklet/key>            Force a compaction snapshot
    retention <relay> <hours>             Set time-travel retention
    delete <relay/tasklet/key>            Hard-delete a Cell's persisted state
    outbox <relay/tasklet/key>            Effectively-once outbox status (lag, cursor, watermark)
    journal <relay/instance>              Steps instance task-journal (committed tasks, iterations, park)
    inbox <relay/instance>                Steps instance durable inbox (offsets, undelivered tail)
    help                                     Show this help message

EXAMPLES:
    flz state get MarketRelay/OhlcTasklet/M1
    flz state get MarketRelay/OhlcTasklet --output json
    flz state list MarketRelay
    flz state at MarketRelay/OhlcTasklet/M1 12345
    flz state at MarketRelay/OhlcTasklet/M1 2026-05-01T12:00:00Z
    flz state rotate MarketRelay/OhlcTasklet/M1
    flz state retention MarketRelay 6
    flz state delete MarketRelay/OhlcTasklet/M1
    flz state outbox MarketRelay/OhlcTasklet/M1
    flz state journal etl-job-42
    flz state inbox chat-session-7
```

A cell's state is read by `<relay>/<tasklet>`, here from
[`state.md`](../core/state.md#reading-a-cell-s-state):

```sh
$ flz state get anna/reply --namespace agents
```

```output
state get anna/reply:
  change_seq: 3
  state:      {"reply":{"messages":["Hello","What can you do?","Are you still there?"]}}
```

## `flz dlq`

Lists the inputs a relay gave up on after its retries, and replays or drops them. Explained in [`durability.md`](../core/durability.md#the-dead-letter-queue).

```sh
$ flz dlq help
```

```output
Dead-letter queue: parked poison payloads (retry budget exhausted).

Usage:
  flz dlq list [relay]                        Parked entries, nearest-to-expiry first
  flz dlq show <relay/tasklet/entry-id>       Entry metadata + payload preview
  flz dlq replay <relay/tasklet/entry-id>     Re-dispatch through the normal input path
  flz dlq drop <relay/tasklet/entry-id>       Discard the entry (audited)

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var.
```

An input that failed every retry, from
[`durability.md`](../core/durability.md#the-dead-letter-queue):

```sh
$ flz dlq list 'trade_watcher#p1' --namespace trading
```

```output
ENTRY                                                       CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/track_prices/307bcbd6439a080f              EUR/USD         trades      1    8s        TypeError: the price is not a number: "one euro"
```

## `flz schedule`

Starts a workflow on a cron schedule. Explained in [`workflows.md`](../core/workflows.md#one-instance-per-piece-of-work), [`relay_settings.md`](../../configuration/relay_settings.md).

```sh
$ flz schedule help
```

```output
flz schedule - cron-scheduled steps relay spawns

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
                 --plugins takes the same array as `relay start --plugins`;
                 each firing binds its instance to the plugins it names.
    list         List schedules with their next fire time
    remove <name>  Remove a schedule
    run <name>     Fire one instance immediately with the stored config;
                   judged as relay.start in the schedule's namespace
    help         Show this help message

EXAMPLES:
    flz schedule add digest --cron "0 8 * * *" --tz Europe/Tallinn --factory ai_digest
    flz schedule add nightly --cron "0 2 * * *" --tz UTC --factory report --text '{"day":"today"}' --channel requests
    flz schedule list
    flz schedule run digest

Identity for gated verbs rides `--token <token>` or the FLEZHA_TOKEN env var.
```
