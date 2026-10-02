# Durability

Flezha writes down every change a tasklet makes to its state, and every step an
order finishes, on the controller's disk as it happens. When a runner or the
controller comes back after a stop or a crash, the desk carries on from where
it was. Nothing in the desk's code saves or loads anything.

On this page you stop and crash the desk on purpose and check that nothing was
lost. Then you see what happens to a message the desk cannot read, and to an
order the exchange refuses.

## What the desk remembers

| What | Where it lives | Written in |
|---|---|---|
| each symbol's last ten prices | the cell of `track_prices` for that symbol | [`building_an_etl_stream.md`](building_an_etl_stream.md#keep-each-symbol-s-prices) |
| which steps of an order have finished | the order's journal, one per order | [`building_a_workflow.md`](building_a_workflow.md#several-tasklets-step-by-step) |
| the reservation and order ids an undo needs | the state of `reserve_funds` and `place_order` | [`building_a_workflow.md`](building_a_workflow.md#a-step-that-can-be-undone) |

## Pause the exchange

> Type `/pause` in the testnet's terminal, so the prices hold still, then read
> every symbol's window and keep the output to compare with later.
> {: .step}

```text
/pause
```

```output
market paused: nothing is generated until /resume
```

```sh
$ flz state get trade_watcher/track_prices --namespace trading
```

```output
state get trade_watcher/track_prices:
  change_seq: -
  state:      {"track_prices":{"EUR/USD":{"prices":[1.13378,1.14873,1.1487,1.13987,1.13993,1.14014,1.14012,1.14368,1.1327,1.13267]},"GBP/USD":{"prices":[1.37453,1.37442,1.37448,1.3887,1.38872,1.40792,1.40786,1.40481,1.40493,1.40494]},"USD/JPY":{"prices":[161.741,161.744,160.871,159.473,159.473,161.233,162.522,162.519,162.525,162.162]}}}
```

## Stop and crash the runner

A clean stop (Ctrl-C, or SIGTERM) hands the runner's last changes to the
controller before it exits. A crash (`kill -9`, a power cut) hands over
nothing, and does not need to: every change reached the controller as it was
made.

> Stop the runner with Ctrl-C (twice for an Elixir runner), then start it again
> with the same command as in
> [`partitioning.md`](partitioning.md#start-the-desk).
> {: .step}

While the runner is gone, its partitions show `failed`. Within a few seconds of
the restart they are back, `healthy`, on the new runner:

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

> Now crash it: find its process id, `kill -9` it from another terminal, and
> start it again.
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

After either restart, `flz state get` prints the same windows as before.

## An order in the middle of a crash

An order writes each step to its journal the moment the step finishes. After a
crash it continues at the first unfinished step, so the money is not reserved
twice and the order is not placed twice. `confirm_fill` waits ten seconds for
the exchange, which is your window to crash the runner.

> Start an order, and read its journal within the ten seconds.
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
    ...
  seq=1 task_committed task_idx=1 iter=0 epoch=0
    ran: order_1/place_order/ record=0
    ...
  seq=2 task_committed task_idx=3 iter=0 epoch=0
    ran: order_1/record_order/
    ...

  leaves: 2,3  complete: false
```

Three steps are done: the money is reserved, and the order is placed and
booked. `confirm_fill` is still waiting.

> Crash the runner while `confirm_fill` waits, and start it again.
> {: .step}

About ten seconds later the journal ends with `confirm_fill`, and the order is
`complete: true`:

```output
...
  seq=3 task_committed task_idx=2 iter=0 epoch=0
    ran: order_1/confirm_fill/
    handoff: 0 item(s), 0 byte(s)
    completes the instance: result 57 byte(s)


  leaves: 2,3  complete: true
```

Only `confirm_fill` ran after the crash. The runner's terminal shows one
`[book]` line for the order, not two.

A PHP runner's `confirm_fill` waits on a timer set for a fixed moment, so it
finishes at the original deadline, or at once if the runner is back later than
that, rather than ten seconds after the restart.

## Restart the controller

> Stop the controller with `flz controller stop`, and start it again the way
> [`authorization.md`](authorization.md#claim-the-admin) did. Leave the runner
> running.
> {: .step}

The runner reconnects by itself once the controller is back:

```output
[flezha] Connected after 6 attempts
```

Your admin token and the `trading` namespace are still there. Within a minute
the partitions are placed on the runner again, and `flz state get` prints the
same windows as before.

## The dead-letter queue

A message that fails however often it is tried is tried three times, then set
aside in the **dead-letter queue** on the controller's disk, so the messages
behind it are not held up.

> Send `trade_watcher#p1` a trade whose price is not a number.
> {: .step}

```sh
$ flz operator send 'trade_watcher#p1' --namespace trading --channel trades --text '{"channel": "trade", "data": [{"symbol": "EUR/USD", "price": "one euro", "ord_type": "market"}]}'
```

```output
Delivered to 'trade_watcher#p1'.
```

`flz dlq list` shows what the partition has set aside:

```sh
$ flz dlq list 'trade_watcher#p1' --namespace trading
```

<!-- tabs-open -->

### Java

The typed `Trade` record cannot be read, so the entry channel refuses the
trade before any tasklet sees it:

```output
ENTRY                                                       CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/trades/307bcbd6439a080f                           _entry          trades      1    3s        java.lang.IllegalArgumentException: entry channel: JSON dese
```

### Rust

The typed `Trade` struct cannot be read, so the entry channel refuses the
trade before any tasklet sees it. The entry is `trade_watcher#p1/trades/...`
in cell `_entry`, and its error is serde's reason, as the runner logs it:

```output
[flezha] entry channel `trades` parking payload to DLQ after 3 attempt(s): entry channel: JSON deserialization failed: invalid type: string "one euro", expected f64 at line 1 column 71
```

### Node

`track_prices` reads the trade as a plain object and refuses the price itself:

```output
ENTRY                                                       CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/track_prices/307bcbd6439a080f              EUR/USD         trades      1    8s        TypeError: the price is not a number: "one euro"
```

### Python

`track_prices` reads the trade as a dict and refuses the price itself:

```output
ENTRY                                                       CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/track_prices/307bcbd6439a080f              EUR/USD         trades      1    5s        TypeError: the price is not a number: 'one euro'
```

### Ruby

`track_prices` reads the trade as a Hash and refuses the price itself:

```output
ENTRY                                                       CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/track_prices/307bcbd6439a080f              EUR/USD         trades      1    6s        ArgumentError: the price is not a number: "one euro"
```

### PHP

`track_prices` reads the trade as an array and refuses the price itself:

```output
ENTRY                                                       CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/track_prices/307bcbd6439a080f              EUR/USD         trades      1    3s        InvalidArgumentException: the price is not a number: "one eu
```

### Elixir

`track_prices` reads the trade as a map and refuses the price itself. The
retries are logged at the `debug` level:

```output
ENTRY                                                       CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/track_prices/239d0c721a1951a6              EUR/USD         trades      1    0s        the price is not a number: "one euro"
```

The tasklet was handed a decoded map rather than bytes, so this entry can be
dropped but not replayed.

<!-- tabs-close -->

`flz dlq show` prints one entry, with the message itself. Use the `ENTRY` your
list printed:

```sh
$ flz dlq show '<entry>' --namespace trading
```

```output
...
attempts          3
status            "parked"
payload            {"channel": "trade", "data": [{"symbol": "EUR/USD", "price": "one euro", "ord_type": "market"}]}
```

`flz dlq replay` sends an entry through again, for a message that failed
because of a bug since fixed. This one will never be readable.

> Drop the entry.
> {: .step}

```sh
$ flz dlq drop '<entry>' --namespace trading
```

```output
dropped <entry>
```

A partition group cannot be rescaled while its dead-letter queue holds entries
([`partitioning.md`](partitioning.md#change-the-number-of-partitions)).

## Undo an order

The exchange fills at most 100 at a time, so an order bigger than that fails at
`confirm_fill`, is tried three times, and then every finished step is undone,
last step first. The testnet's `/jump` makes one such order on purpose: it
moves the next trade's price far enough to ask for more than 100.

> Type `/resume` and then `/jump EUR/USD 12` in the testnet's terminal, and
> watch the runner's terminal.
> {: .step}

```text
/resume
/jump EUR/USD 12
```

About half a minute later, after three tries of ten seconds each, the order is
undone:

<!-- tabs-open -->

### Java

```output
[book] EUR/USD 136 as ord-be4f7a593756acada0924d10da8d31c6
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 63 ms): the exchange fills at most 100 a time, and this is 136
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 66 ms): the exchange fills at most 100 a time, and this is 136
[undo] cancelled ord-be4f7a593756acada0924d10da8d31c6
[undo] released res-935e21608cc9ea9844b537d8c0c97517
```

### Rust

```output
[book] EUR/USD 120 as ord-26188e006a7df8374b811618b39c770a
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 45 ms): tasklet error: the exchange fills at most 100 a time, and this is 120
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 120 ms): tasklet error: the exchange fills at most 100 a time, and this is 120
[undo] cancelled ord-26188e006a7df8374b811618b39c770a
[undo] released res-9282a7ff442753844305a0393c154b6b
```

### Node

Another order's booking can land between the tries, because the wait holds
nothing else up:

```output
[book] EUR/USD 114 as ord-1583d35b849ecc1e5be0c67eb19126b1
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 41 ms): Error: the exchange fills at most 100 a time, and this is 114
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 94 ms): Error: the exchange fills at most 100 a time, and this is 114
[book] EUR/USD 26 as ord-e2b95dca4b993989b1bf0ae12b069bfd
[undo] cancelled ord-1583d35b849ecc1e5be0c67eb19126b1
[undo] released res-765f48effc6403ca7079785ffc7b8820
```

### Python

```output
[book] EUR/USD 130 as ord-147cb3cb7525c45f615cf36ae92ed0be
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 31 ms): RuntimeError: the exchange fills at most 100 a time, and this is 130
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 120 ms): RuntimeError: the exchange fills at most 100 a time, and this is 130
[undo] cancelled ord-147cb3cb7525c45f615cf36ae92ed0be
[undo] released res-494fc55d64e4cc925b295bd07fa3b41f
```

### Ruby

```output
[book] EUR/USD 124 as ord-a40ba575b8a83599f33dad116255f5c3
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 40 ms): RuntimeError: the exchange fills at most 100 a time, and this is 124
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 62 ms): RuntimeError: the exchange fills at most 100 a time, and this is 124
[undo] cancelled ord-a40ba575b8a83599f33dad116255f5c3
[undo] released res-b159dff9228e6788657db153453c4bda
```

### PHP

`confirm_fill` waits the ten seconds once, on its timer, and retries straight
away, so the undo comes about ten seconds after the booking:

```output
[book] EUR/USD 120 as ord-601b85a308803b3d10634c4833c8b7de
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 29 ms): RuntimeException: the exchange fills at most 100 a time, and this is 120
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 108 ms): RuntimeException: the exchange fills at most 100 a time, and this is 120
[undo] cancelled ord-601b85a308803b3d10634c4833c8b7de
[undo] released res-0a63b28bebbc54f887a0ac0c6891cc3f
```

### Elixir

The tries are logged at the `debug` level:

```output
[book] EUR/USD 500 as ord-e015c2df9402f4625d9cef4def51f243

20:04:04.357 [debug] [Flezha.SDK] order_too_big/node/single attempt 1/3 failed: the exchange fills at most 100 a time, and this is 500; retrying

20:04:14.417 [debug] [Flezha.SDK] order_too_big/node/single attempt 2/3 failed: the exchange fills at most 100 a time, and this is 500; retrying
[undo] cancelled ord-e015c2df9402f4625d9cef4def51f243

20:04:24.547 [debug] [Flezha.SDK] order_too_big compensating task 1
[undo] released res-e0d09a3a6a33661df98dd0a4768bb45a
```

<!-- tabs-close -->

The order is cancelled, then the money released. Each undo reads its id from
its step's state, which survives a crash. The booking stays, because
`record_order` has no `compensate`.

## Where it is kept

How far an operator's work is kept is its class's `durable` setting. The desk's
two classes do not set it, so both keep their work on the controller's disk,
under `app_data/` in the folder the controller was started in.

| `durable` | Where the work is kept | After a crash | After the controller's disk is lost |
|---|---|---|---|
| `false` | the runner's memory only | the work starts over | lost |
| `local`, the default | the controller's disk | carries on | lost, unless a copy was shipped |
| `remote` | the controller's disk and a remote copy; each finished step or input counts as done only once the copy holds it | carries on | carries on from the copy |

A class that runs in flow also says how often a value may reach a sink, with
`exactly`: `at_least_once`, the default, can send a value twice after a crash,
and `effectively_once` sends it once.

The remote copy goes where `FLEZHA_DURABILITY_BACKEND` says:

| Backend | Where the copy goes |
|---|---|
| `none` | nowhere; the local disk is the only copy (the default) |
| `postgres` | a table in a PostgreSQL database |
| `s3` | a bucket on Amazon S3 or any S3-compatible store |
| `local_path` | a folder that outlives the controller's disk, such as a network mount |

With a backend set, a `local` class's copy follows in the background: a
workflow's after every finished step, a stream's at least every 30 seconds
(`FLEZHA_DURABILITY_SHIP_INTERVAL_MS`). A controller that starts on an empty
disk fetches the copy and carries on from it.
[`configuration.md`](../../configuration.md#state-wal-and-durability) lists the
settings.

> #### The cost of saving in the background {: .info}
>
> A tasklet changes its state in the runner's memory, and the runner sends its
> changes to the controller in batches about every 5 milliseconds. So a runner
> that crashes can lose its last few milliseconds of changes; a clean stop loses
> nothing. A workflow is the exception: each finished step is on the
> controller's disk before the next one starts.

## Next

After a crash, a value the desk sent just before it can reach a sink twice, so
`signals.txt` can hold a line twice. Part 3's
[`effectively_once.md`](../advanced/effectively_once.md) makes every value
arrive exactly once.

That is the end of Part 2.
[Part 3](../advanced/advanced_topics.md) goes deeper.
