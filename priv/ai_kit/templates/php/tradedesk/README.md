# Tradedesk: the PHP project of the tradedesk tutorial

The toy trader the [tradedesk tutorial](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/tradedesk/tradedesk.md) builds, as a
standalone project. Nothing here depends on the Flezha source tree:
`composer.json` names one released version of `flezha/sdk`, the way your own
project would. It needs PHP 8.4 or later with `ext-ffi`.

It holds the desk's two relays and the runner that serves them. The tutorial
writes them one tasklet at a time.

| File | What is in it |
|---|---|
| `runner.php` | the runner: this process joins the `trading` namespace and registers both factories |
| `src/TradeWatcherRelay.php` | the ETL stream, a flow relay: the exchange's trade messages in, a cell per symbol, buy signals out |
| `src/TrackPrices.php` | keeps each symbol's last ten prices, and compares a new price with their average |
| `src/BuyAboveAverage.php` | starts an order when a price is 2 % or more above its average |
| `src/OrderExecutorRelay.php` | the workflow, a steps relay: two steps in order, then two side by side |
| `src/ReserveFunds.php` | step one: puts the money aside, and releases it again if the order is undone |
| `src/PlaceOrder.php` | step two: places the paper order, and cancels it if the order is undone |
| `src/ConfirmFill.php` | asks for a timer ten seconds out, then confirms the fill and ends the order with its result, or refuses an order above the exchange's limit |
| `src/RecordOrder.php` | books the placed order, and has nothing to undo |

Every tasklet is a class extending `Flezha\Tasklet`, with `#[Tasklet\Spec(name: '...')]`
naming it. What a tasklet remembers and the changes that record it are plain
arrays, folded by the class's own `applyChange`, and the values it takes and
hands on are the arrays their JSON decodes to.

## Run it

You need a controller running, the `trading` namespace created and a key for it
([Open the desk](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/tradedesk/open_the_desk.md) does both).

```sh
composer install
FLEZHA_NAMESPACE_SECRET=<trading key> php runner.php
```

It keeps running until Ctrl-C, or its standard input closing, so do not start
it with standard input closed. The namespace is the first argument and defaults
to `trading`: `php runner.php my_desk`.

From another terminal, the controller lists it:

```sh
flz runner list --namespace trading
```

## Start an order by hand

One instance of `order_executor` is one order. `--text` carries the order on the
`order` channel:

```sh
flz relay start order_executor --id order_1 --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order
```

The runner's terminal shows the order being booked. The ids come from
`$ctx->stableId()`, so yours differ:

```
[book] EUR/USD 10 as ord-6b2e13f521b6781afd9f710152cd2f25
```

About ten seconds later, once the exchange's answer arrives on `confirm_fill`'s
timer, the instance ends, and `flz relay info` shows how:

```sh
flz relay info order_1 --namespace trading
```

```
...
Relay: order_1
  Run: steps
  Phase: finished
  Status: ok
  Finished At (unix s): 1790588842
```

## Watch it undo itself

The paper exchange fills at most 100 at a time, so `confirm_fill` refuses a
larger order. Start one the same way, with a quantity above the limit:

```sh
flz relay start order_executor --id order_2 --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 500}' --channel order
```

Ten seconds later `confirm_fill` refuses it, is tried three times, and then the
order ends in failure. Every step that had finished is undone, in reverse: the
order is cancelled, then the money is released. `record_order` has no undo, so
the booking stays:

```
[book] EUR/USD 500 as ord-0e7ee9553cfe5eac606fd232fd7ab33c
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 29 ms): RuntimeException: the exchange fills at most 100 a time, and this is 500
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 130 ms): RuntimeException: the exchange fills at most 100 a time, and this is 500
[undo] cancelled ord-0e7ee9553cfe5eac606fd232fd7ab33c
[undo] released res-54e4641a002cc27f8d030507d8d5aea5
```

```sh
flz relay info order_2 --namespace trading
```

```
...
Relay: order_2
  Run: steps
  Phase: finished
  Status: error
  Error: RuntimeException: the exchange fills at most 100 a time, and this is 500
  Finished At (unix s): 1790588862
```

## Building from a checkout

To run the project against a Flezha checkout instead of the published package,
stage the native library the package loads, from the repository root:

```sh
scripts/build-php-sdk.sh --release
```

It needs a Rust toolchain. Then tell Composer where the package is, once, naming
your checkout's absolute path:

```sh
composer config --global repositories.flezha-sdk \
  '{"type": "path", "url": "/path/to/flezha/sdk/php", "options": {"versions": {"flezha/sdk": "0.1.1"}}}'
```

After that, `composer install` in this folder links the checkout's package. The
entry lives in Composer's global configuration rather than in `composer.json`,
so this folder stays as it is; remove the entry to use the published package
again.

## Where to go next

The echo in [`../echo`](../echo/README.md) is the project of the getting-started pages, and
[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what a tasklet, a relay and a runner each are.
