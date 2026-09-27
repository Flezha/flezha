# Durability

Durability is what keeps the desk's work when something stops: the runner is
stopped or crashes, or the controller is restarted. Flezha writes down every
change a tasklet makes to what it remembers, and every step an order finishes,
on the controller's disk, as it happens. When the runner comes back, the work
carries on from where it was instead of starting again. Nothing in the desk's
code saves or loads anything.

On this page you break the desk on purpose, one piece at a time, and check
each time that nothing was lost. First you pause the demo exchange, so the
prices hold still while you look. Then you stop the runner, crash it, and
crash it again in the middle of an order, and each time you read back what the
desk remembered. You restart the controller too, and watch everything come
back.

Last, you see what happens when something goes wrong for good: a message the
desk cannot read is set aside instead of blocking the others, and an order the
exchange refuses is undone, step by step.

## What the desk remembers

The desk remembers three things, and each one comes back after a stop:

| What | Where it lives | Written in |
|---|---|---|
| each symbol's last ten prices | the cell of `track_prices` for that symbol | [`building_an_etl_stream.md`](building_an_etl_stream.md#keep-each-symbol-s-prices) |
| which steps of an order have finished | the order's journal, one per order | [`building_a_workflow.md`](building_a_workflow.md#several-tasklets-step-by-step) |
| the reservation and order ids an undo needs | the state of `reserve_funds` and `place_order` | [`building_a_workflow.md`](building_a_workflow.md#a-step-that-can-be-undone) |

## Pause the exchange

While the exchange is connected, new prices keep changing the windows, so a
window read before a stop and one read after it would differ anyway. Pause the
feed first, and every window stays exactly as it is.

> Type `/pause` in the testnet's terminal. The exchange stops sending trades,
> the windows keep what they have, and no new orders start.
> {: .step}

```text
/pause
```

```output
market paused: nothing is generated until /resume
```

The plugin stays connected; the exchange just has nothing to send.

> Read every symbol's window, and keep the output to compare with later.
> {: .step}

```sh
$ flz state get trade_watcher/track_prices --namespace trading
```

```output
state get trade_watcher/track_prices:
  change_seq: -
  state:      {"track_prices":{"EUR/USD":{"prices":[1.13378,1.14873,1.1487,1.13987,1.13993,1.14014,1.14012,1.14368,1.1327,1.13267]},"GBP/USD":{"prices":[1.37453,1.37442,1.37448,1.3887,1.38872,1.40792,1.40786,1.40481,1.40493,1.40494]},"USD/JPY":{"prices":[161.741,161.744,160.871,159.473,159.473,161.233,162.522,162.519,162.525,162.162]}}}
```

## Stop the runner

A runner can stop in two ways, and they differ in what reaches the controller:

- **A clean stop**, with Ctrl-C or a SIGTERM from whatever runs it. The runner
  hands its last changes to the controller before it exits, so nothing is lost.
- **A crash**, such as `kill -9`, a power cut or the machine dying. There is no
  goodbye, and the controller keeps every change the runner had already sent.

> Stop the runner with Ctrl-C in its terminal.
> {: .step}

With no runner, the partitions have nowhere to run. The controller marks them
`failed`, and they wait:

```sh
$ flz operator info trade_watcher --namespace trading
```

```output
Fetching operator information for namespace 'trading'...
Operator information for namespace 'trading':

Partition group: trade_watcher (3/3)
  p0  trade_watcher#p0               key groups 0-84  trading_90cb7c5b6846  failed
  p1  trade_watcher#p1               key groups 85-169  trading_90cb7c5b6846  failed
  p2  trade_watcher#p2               key groups 170-255  trading_90cb7c5b6846  failed
  coverage: all 256 key groups owned exactly once
...
```

A window cannot be read now either, because `flz state get` asks the runner
that holds it, and there is none:

```sh
$ flz state get trade_watcher/track_prices --namespace trading
```

```output
error: state get: no running operator `trade_watcher#p0` in any namespace
```

> Start the runner again, with the same command as in
> [`partitioning.md`](partitioning.md#start-the-desk). Within a few seconds the
> partitions are back on it, `healthy`.
> {: .step}

```sh
$ flz operator info trade_watcher --namespace trading
```

```output
...
Partition group: trade_watcher (3/3)
  p0  trade_watcher#p0               key groups 0-84  trading_884227a6e4a6  healthy
  p1  trade_watcher#p1               key groups 85-169  trading_884227a6e4a6  healthy
  p2  trade_watcher#p2               key groups 170-255  trading_884227a6e4a6  healthy
...
```

The runner has a new id, because every run of it is a new session, and the
partitions moved to it. Their windows came with them:

```sh
$ flz state get trade_watcher/track_prices --namespace trading
```

```output
state get trade_watcher/track_prices:
  change_seq: -
  state:      {"track_prices":{"EUR/USD":{"prices":[1.13378,1.14873,1.1487,1.13987,1.13993,1.14014,1.14012,1.14368,1.1327,1.13267]},"GBP/USD":{"prices":[1.37453,1.37442,1.37448,1.3887,1.38872,1.40792,1.40786,1.40481,1.40493,1.40494]},"USD/JPY":{"prices":[161.741,161.744,160.871,159.473,159.473,161.233,162.522,162.519,162.525,162.162]}}}
```

Every price is where it was.

## Crash the runner

> Crash the runner. `flz runner list` shows its process id, and `kill -9` with
> that id, from another terminal, ends it at once. The controller sees it gone
> and marks the partitions `failed`, as before.
> {: .step}

```sh
$ flz runner list --namespace trading
```

```output
Fetching runners for namespace 'trading'...
Runners in namespace 'trading':
  - trading_1194c54f876f  pid 2534837
```

```sh
$ kill -9 2534837
```

> Start the runner again, and read the windows once more.
> {: .step}

```sh
$ flz state get trade_watcher/track_prices --namespace trading
```

```output
state get trade_watcher/track_prices:
  change_seq: -
  state:      {"track_prices":{"EUR/USD":{"prices":[1.13378,1.14873,1.1487,1.13987,1.13993,1.14014,1.14012,1.14368,1.1327,1.13267]},"GBP/USD":{"prices":[1.37453,1.37442,1.37448,1.3887,1.38872,1.40792,1.40786,1.40481,1.40493,1.40494]},"USD/JPY":{"prices":[161.741,161.744,160.871,159.473,159.473,161.233,162.522,162.519,162.525,162.162]}}}
```

The same windows again. A crash had no chance to hand anything over, and it
did not need to: every change had reached the controller as it was made.

## An order in the middle of a crash

An order writes down each step the moment it finishes, in its journal. When the
runner crashes in the middle of an order, the order continues at the first step
that had not finished. The steps before it do not run again, so the money is
not put aside twice and the order is not placed twice.

`confirm_fill` waits ten seconds for the paper exchange to answer, which is
the time you have to crash the runner in the middle of an order.

> Start an order by hand, and read its journal right away, within the ten
> seconds.
> {: .step}

```sh
$ flz operator start order_executor --id order_1 --namespace trading --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order
```

```output
Started order_1
```

```sh
$ flz state journal order_1 --namespace trading
```

```output
task-journal for instance order_1:

  seq=0 task_committed task_idx=0 iter=0 epoch=0
    ran: order_1/reserve_funds/ record=0
    handoff: 1 item(s), 102 byte(s)
      edge: channel_id=1 1 item(s), 102 byte(s)

  seq=1 task_committed task_idx=1 iter=0 epoch=0
    ran: order_1/place_order/ record=0
    handoff: 1 item(s), 103 byte(s)
      edge: channel_id=2 1 item(s), 103 byte(s)

  seq=2 task_committed task_idx=3 iter=0 epoch=0
    ran: order_1/record_order/
    handoff: 0 item(s), 0 byte(s)


  leaves: 2,3  complete: false
```

Each `task_committed` is a step that finished: the money is reserved, the order
is placed and booked. `confirm_fill` is still waiting for the exchange, so the
order is not `complete`. The runner's terminal shows the booking:

```output
[book] EUR/USD 10 as ord-6b2e13f521b6781afd9f710152cd2f25
```

> Crash the runner while `confirm_fill` waits, the same way: `kill -9` with the
> process id `flz runner list` shows now, then start it again.
> {: .step}

```sh
$ kill -9 <pid>
```

About ten seconds after the runner is back, the order is done:

```sh
$ flz state journal order_1 --namespace trading
```

```output
task-journal for instance order_1:
...
  seq=2 task_committed task_idx=3 iter=0 epoch=0
    ran: order_1/record_order/
    handoff: 0 item(s), 0 byte(s)

  seq=3 task_committed task_idx=2 iter=0 epoch=0
    ran: order_1/confirm_fill/
    handoff: 0 item(s), 0 byte(s)
    completes the instance: result 57 byte(s)


  leaves: 2,3  complete: true
```

Only `confirm_fill` ran after the crash. The runner's terminal still shows the
one booking from before it, and no second one, because `record_order` had
finished and was not run again.

```sh
$ flz operator info order_1 --namespace trading
```

```output
...
Operator: order_1
  Kind: bounded
  Phase: finished
  Status: ok
  Finished At (unix s): 1790449731
```

## Restart the controller

The runner is not the only thing that can stop. The controller keeps its own
records on disk, and reads them back when it starts.

> Stop the controller, then start it again the way
> [`authorization.md`](authorization.md#claim-the-admin) did. Leave the runner
> running.
> {: .step}

```sh
$ flz controller stop
```

```output
...
[shutdown] complete duration_ms=9 outcome=clean
```

While the controller is gone, the runner keeps trying to reach it, and connects
by itself once it is back:

```output
...
[flezha] Control channel attempt 7 in 3.265s...
[flezha] Connected after 6 attempts
```

Your admin token still works, and `trading` is still there:

```sh
$ flz auth whoami
```

```output
principal: admin-c5257dbf521257b2
namespace: _system
role:      admin
```

```sh
$ flz namespace list
```

```output
...
  NAMESPACE                          RUNNING
  _system                            yes
  trading                            yes
```

The partitions take up to a minute to be placed on the runner again. Then they
are `healthy`, and every window is where it was:

```sh
$ flz state get trade_watcher/track_prices --namespace trading
```

```output
state get trade_watcher/track_prices:
  change_seq: -
  state:      {"track_prices":{"EUR/USD":{"prices":[1.13378,1.14873,1.1487,1.13987,1.13993,1.14014,1.14012,1.14368,1.1327,1.13267]},"GBP/USD":{"prices":[1.37453,1.37442,1.37448,1.3887,1.38872,1.40792,1.40786,1.40481,1.40493,1.40494]},"USD/JPY":{"prices":[161.741,161.744,160.871,159.473,159.473,161.233,162.522,162.519,162.525,162.162]}}}
```

## The dead-letter queue

Some messages cannot be handled at all, however often they are tried. Such a
message is tried three times, then set aside in the **dead-letter queue**, so
the messages behind it are not held up. It is kept there, on the controller's
disk, until you decide what to do with it.

> Send `trade_watcher#p1` a trade whose price is not a number.
> {: .step}

```sh
$ flz operator send 'trade_watcher#p1' --namespace trading --channel trades --text '{"channel": "trade", "data": [{"symbol": "EUR/USD", "price": "one euro", "ord_type": "market"}]}'
```

```output
Delivered to 'trade_watcher#p1'.
```

The runner's terminal shows the three tries, the last one ending:

```output
[flezha] prepare for channel `trades` parking payload to DLQ after 3 attempt(s): java.lang.IllegalArgumentException: entry channel: JSON deserialization failed: Cannot deserialize value of type `double` from String "one euro": not a valid `double` value (as String to convert)
```

`flz dlq list` shows what a partition has set aside:

```sh
$ flz dlq list 'trade_watcher#p1' --namespace trading
```

```output
ENTRY                                                       CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/trades/307bcbd6439a080f                           _entry          trades      1    3s        java.lang.IllegalArgumentException: entry channel: JSON dese
```

`flz dlq show` reads one entry, with the message itself:

```sh
$ flz dlq show 'trade_watcher#p1/trades/307bcbd6439a080f' --namespace trading
```

```output
id                "307bcbd6439a080f"
operator_class    "trade_watcher#p1"
tasklet_id        "trades"
cell_key          "_entry"
channel           "trades"
origin_offset     nil
error             "java.lang.IllegalArgumentException: entry channel: JSON deserialization failed: ..."
attempts          3
generation        1
status            "parked"
ts_ms             1790449871673
payload_bytes     96
payload            {"channel": "trade", "data": [{"symbol": "EUR/USD", "price": "one euro", "ord_type": "market"}]}
```

An entry is either sent through again or thrown away. `flz dlq replay` sends it
through again, which is for a message that failed because of something since
fixed, such as a bug in the operator. This one will never be readable, so drop
it:

> Drop the entry.
> {: .step}

```sh
$ flz dlq drop 'trade_watcher#p1/trades/307bcbd6439a080f' --namespace trading
```

```output
dropped trade_watcher#p1/trades/307bcbd6439a080f
```

```sh
$ flz dlq list 'trade_watcher#p1' --namespace trading
```

```output
dead-letter queue is empty
```

A partition group with entries in its dead-letter queue cannot be rescaled, as
[`partitioning.md`](partitioning.md#change-the-number-of-partitions) says, so
empty it before `flz operator scale`.

## Resume the exchange

> Type `/resume` in the testnet's terminal. The desk picks up the prices from
> where the windows stopped.
> {: .step}

```text
/resume
```

```output
market resumed
```

## Undo an order

The paper exchange fills at most 100 at a time, and the desk buys 10 for each
percent a price is above its average, so a price more than 10 % above it asks
for too much. That order fails at `confirm_fill`, is tried three times, and then
everything that finished is undone, last step first.

The testnet can make such a move on purpose: `/jump` sends one trade at a price
that far from the current one, and the price then carries on as before, so
exactly one order is too big.

> Type `/jump EUR/USD 12` in the testnet's terminal, and watch the runner's
> terminal. With ten seconds per try, the undo comes about half a minute later.
> {: .step}

```text
/jump EUR/USD 12
```

```output
EUR/USD moves 12% on its next trade (1 socket(s) subscribed)
```

Between the other symbols' bookings, the runner's terminal shows the one order:

```output
[book] EUR/USD 136 as ord-be4f7a593756acada0924d10da8d31c6
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 63 ms): the exchange fills at most 100 a time, and this is 136
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 66 ms): the exchange fills at most 100 a time, and this is 136
[undo] cancelled ord-be4f7a593756acada0924d10da8d31c6
[undo] released res-935e21608cc9ea9844b537d8c0c97517
```

The order was cancelled, then the money released. Both undos read their id from
the step's state, which is what makes them work after a crash too: the state
comes back, while what a step handed on does not. The booking is not undone,
because `record_order` has no `compensate`.

## Where it is kept

The controller keeps everything it saves on its own disk, in the folder it was
started in: each namespace's state under `app_data/state/<namespace>/`, apart
from every other's, and the namespaces, tokens and keys under `app_data/auth/`.
That includes the write-ahead log, the running record of every change, which is
always on the local disk, whatever else is configured.

The local disk is enough as long as it survives: a crashed runner or a restarted
controller gets everything back from it. A controller that loses its disk loses
what was on it. For that case the controller can also send a copy of everything
it saves to storage that lives somewhere else, chosen with
`FLEZHA_DURABILITY_BACKEND`:

| Backend | Where the copy goes |
|---|---|
| `none` | nowhere: the local disk is the only copy. This is the default |
| `postgres` | a table in a PostgreSQL database |
| `s3` | a bucket on Amazon S3 or any S3-compatible store |
| `local_path` | a folder that outlives the controller's own disk, such as a network mount |

The two copies are kept on different schedules:

- **The controller's disk** gets every change within a few milliseconds, always,
  whichever backend is set.
- **The extra copy** follows in the background: a workflow's after every
  finished step, a stream's at least every 30 seconds
  (`FLEZHA_DURABILITY_SHIP_INTERVAL_MS`). The 30 seconds applies to this copy
  only, and only matters if the controller loses its disk.

A controller that starts on an empty disk fetches the copy and carries on from
it. Part 2 keeps the default, the local disk, and
[`configuration.md`](../../configuration.md#state-wal-and-durability) lists the
settings for each backend.

> #### Fast in memory, saved in the background {: .info}
>
> A cell's state lives in the memory of the runner that runs it, so a tasklet
> reads and changes it at memory speed, with no disk in the way. Every change is
> also sent to the controller, which adds it to the write-ahead log, and neither
> step holds the tasklet up: the runner sends its changes in small batches about
> every 5 milliseconds, and the controller writes its log to disk (an `fsync`)
> in batches, also about every 5 milliseconds, rather than once per change. The
> copy to Postgres or S3, when one is set, is sent in the background as well.
>
> The cost is a small window. A runner that crashes, rather than stopping
> cleanly, can lose its last few milliseconds of changes. A clean stop, with
> Ctrl-C or a SIGTERM, hands everything to the controller first and loses
> nothing.
>
> A workflow is the exception, on purpose: a bounded operator waits until each
> finished step is on the controller's disk before the next step starts, so a
> finished step is kept even when the runner crashes.

## What this page does not cover

Delivery. After a crash, a value the desk sent out just before it can reach a
sink a second time, so `signals.txt` can hold a line twice. Making every value
arrive exactly once is Part 3's
[`effectively_once.md`](../advanced/effectively_once.md).

## Next

That is the end of Part 2: the desk trades on its own, remembers what it needs,
and survives a stop or a crash of its runner or its controller.
[Part 3](../guides.md#part-3-advanced-topics) goes deeper.
