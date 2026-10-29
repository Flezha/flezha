# Crash the desk

On this page you stop and crash the runner and the controller while the desk
works, and see that nothing it kept is lost: the price windows, an order in the
middle of its steps, and a message that cannot be read.
[Durability](../durability.html) explains what survives what,
and why.

> Type `/pause` in the demo exchange's terminal, so the prices hold still, then
> read every symbol's window and keep the output to compare with later.
> {: .step}

```sh
$ ./bin/flz state get trade_watcher/track_prices --namespace trading
```

```output
state get trade_watcher/track_prices:
  change_seq: -
  state:      {"track_prices":{"EUR/USD":{"prices":[1.13378,1.14873,1.1487,1.13987,1.13993,1.14014,1.14012,1.14368,1.1327,1.13267]},"GBP/USD":{"prices":[1.37453,1.37442,1.37448,1.3887,1.38872,1.40792,1.40786,1.40481,1.40493,1.40494]},"USD/JPY":{"prices":[161.741,161.744,160.871,159.473,159.473,161.233,162.522,162.519,162.525,162.162]}}}
```

> Stop the runner with Ctrl-C (twice for an Elixir runner), then start it again
> with the same command as in [Open the desk](open_the_desk.md).
> {: .step}

While the runner is gone its partitions show `failed`; a few seconds after the
restart they are `healthy` again:

```sh
$ ./bin/flz relay info trade_watcher --namespace trading
```

```output
...
Partition group: trade_watcher (3/3)
  p0  trade_watcher#p0               key groups 0-84  trading_884227a6e4a6  healthy
  p1  trade_watcher#p1               key groups 85-169  trading_884227a6e4a6  healthy
  p2  trade_watcher#p2               key groups 170-255  trading_884227a6e4a6  healthy
...
```

> Now crash it: find its process id, `kill -9` it, and start it again.
> {: .step}

```sh
$ ./bin/flz runner list --namespace trading
```

```output
Fetching runners for namespace 'trading'...
Runners in namespace 'trading':

  RUNNER                            PID      NODE                  CONNECTED AT          LATE
  trading_1194c54f876f              2534837  flezha@127.0.0.1      2026-10-18T09:47:30Z  no
```

```sh
$ kill -9 2534837
```

After either restart, `flz state get` prints the same windows as before.

## An order in the middle of a crash

`confirm_fill` waits ten seconds for the exchange, which is your window.

> Start an order, crash the runner while `confirm_fill` waits, and start the
> runner again.
> {: .step}

```sh
$ ./bin/flz relay start order_executor --id order_2 --namespace trading --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order
```

About ten seconds after the restart, `flz state journal order_2 --namespace
trading` ends with `confirm_fill` and `outcome: ok`. Only `confirm_fill` ran
after the crash: the runner's terminal shows one `[book]` line for the order,
not two. A PHP runner's `confirm_fill` waits on a timer set for a fixed moment,
so it finishes at the original deadline instead.

## Restart the controller

> Stop the controller with `flz controller stop`, and start it again the way
> [Open the desk](open_the_desk.md) did. Leave the runner running.
> {: .step}

The runner reconnects by itself:

```output
[flezha] Connected after 6 attempts
```

Your admin token and `trading` are still there, the partitions are placed on
the runner again within a minute, and `flz state get` prints the same windows.

## A message that cannot be read

A message that keeps failing is tried three times, then set aside in the
dead-letter queue, so the messages behind it are not held up.

> Send `trade_watcher#p1` a trade whose price is not a number.
> {: .step}

```sh
$ ./bin/flz relay send 'trade_watcher#p1' --namespace trading --channel trades --text '{"channel": "trade", "data": [{"symbol": "EUR/USD", "price": "one euro", "ord_type": "market"}]}'
```

```sh
$ ./bin/flz dlq list 'trade_watcher#p1'
```

<!-- tabs-open -->

### Java

The typed `Trade` record cannot be read, so the entry channel refuses the
trade before any tasklet sees it:

```output
ENTRY                                                       NODE                        CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/trades/307bcbd6439a080f                    flezha@127.0.0.1                   _entry          trades      1    3s        java.lang.IllegalArgumentException: entry channel: JSON dese
```

### Rust

The typed `Trade` struct cannot be read, so the entry channel refuses the
trade before any tasklet sees it. The entry is `trade_watcher#p1/trades/...`
in cell `_entry`, and its error is serde's reason, as the runner logs it:

```output
[flezha] warning: entry channel `trades` parking payload to DLQ after 3 attempt(s): entry channel: JSON deserialization failed: invalid type: string "one euro", expected f64 at line 1 column 71
```

### Node

`track_prices` reads the trade as a plain object and refuses the price itself:

```output
ENTRY                                                       NODE                        CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/track_prices/307bcbd6439a080f              flezha@127.0.0.1            EUR/USD         trades      1    8s        TypeError: the price is not a number: "one euro"
```

### Python

`track_prices` reads the trade as a dict and refuses the price itself:

```output
ENTRY                                                       NODE                        CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/track_prices/307bcbd6439a080f              flezha@127.0.0.1            EUR/USD         trades      1    5s        TypeError: the price is not a number: 'one euro'
```

### Ruby

`track_prices` reads the trade as a Hash and refuses the price itself:

```output
ENTRY                                                       NODE                        CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/track_prices/307bcbd6439a080f              flezha@127.0.0.1            EUR/USD         trades      1    6s        ArgumentError: the price is not a number: "one euro"
```

### PHP

`track_prices` reads the trade as an array and refuses the price itself:

```output
ENTRY                                                       NODE                        CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/track_prices/307bcbd6439a080f              flezha@127.0.0.1            EUR/USD         trades      1    3s        InvalidArgumentException: the price is not a number: "one eu
```

### Elixir

`track_prices` reads the trade as a map and refuses the price itself. The
runner logs each retry as a warning:

```output
ENTRY                                                       NODE                        CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/track_prices/239d0c721a1951a6              flezha@127.0.0.1            EUR/USD         trades      1    0s        the price is not a number: "one euro"
```

The tasklet was handed a decoded map rather than bytes, so this entry can be
dropped but not replayed.

### Go

The typed `Trade` struct cannot be read, so the entry channel refuses the
trade before any tasklet sees it. The error is `encoding/json`'s reason:

```output
ENTRY                                                       NODE                        CELL            CHANNEL     GEN  AGE       ERROR
trade_watcher#p1/trades/307bcbd6439a080f                    flezha@127.0.0.1            _entry          trades      1    0s        json: cannot unmarshal string into Go struct field Trade.dat
```

<!-- tabs-close -->

> Drop the entry, using the `ENTRY` your list printed. It will never be
> readable; `flz dlq replay` is for a message that failed on a bug since fixed.
> {: .step}

```sh
$ ./bin/flz dlq drop '<entry>'
```

```output
dropped <entry>
```

That is the tradedesk. [The agentdesk](../agentdesk/agentdesk.md) is the next
tutorial.
