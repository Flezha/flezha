# Tradedesk: the Node project of the tradedesk tutorial

The toy trader the [tradedesk tutorial](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/tradedesk/tradedesk.md) builds, as a
standalone project. Nothing here depends on the Flezha source tree:
`package.json` names one released version of `@flezha/sdk`, the way your own
project would. It needs Node 20 or later.

It holds the desk's two relays and the runner that serves them. The tutorial
writes them one tasklet at a time.

| File | What is in it |
|---|---|
| `runner.mjs` | the runner: this process joins the `trading` namespace and registers both factories |
| `trade_watcher_relay.mjs` | the ETL stream, a flow relay: the exchange's trade messages in, a cell per symbol, buy signals out |
| `track_prices.mjs` | keeps each symbol's last ten prices, and compares a new price with their average |
| `buy_above_average.mjs` | starts an order when a price is 2 % or more above its average |
| `order_executor_relay.mjs` | the workflow, a steps relay: two steps in order, then two side by side |
| `reserve_funds.mjs` | step one: puts the money aside, and releases it again if the order is undone |
| `place_order.mjs` | step two: places the paper order, and cancels it if the order is undone |
| `confirm_fill.mjs` | waits ten seconds for the exchange, confirms the fill and ends the order with its result, and refuses an order above the exchange's limit |
| `record_order.mjs` | books the placed order, and has nothing to undo |

Every tasklet is one object handed to `defineTasklet`, with a required `name`
and a `run` that takes the value and the context and emits through
`ctx.emit`; what `run` returns is not output. What a tasklet remembers and the
changes that record it are plain objects, folded by its own `applyChange`, and
the values it takes and hands on are the objects their JSON decodes to. Each
relay module's default export is what the runner registers.

## Run it

You need a controller running, the `trading` namespace created and a key for it
([Open the desk](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/tradedesk/open_the_desk.md) does both). From this
folder:

```sh
npm install
FLEZHA_NAMESPACE_SECRET=<trading key> node runner.mjs
```

Start it from this folder: the runner registers its relays by module
specifier, `./trade_watcher_relay.mjs`, and that resolves against the
working directory. It keeps running until a SIGTERM, Ctrl-C, or its standard
input closing, so do not start it with standard input closed. The namespace is
the first argument and defaults to `trading`: `node runner.mjs my_desk`.

From another terminal, the controller lists it:

```sh
flz runner list --namespace trading
```

## Start an order by hand

One instance of `order_executor` is one order, and each needs an id of its own. `--text` carries the order on the
`order` channel:

```sh
flz relay start order_executor --id order_3 --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order
```

The runner's terminal shows the order being booked. The ids come from
`ctx.stableId()`, so yours differ:

```
[book] EUR/USD 10 as ord-452676c06be3d48c837a005ff62b5567
```

About ten seconds later, once `confirm_fill` has its answer from the exchange,
the instance ends, and `flz relay info` shows how:

```sh
flz relay info order_3 --namespace trading
```

```
...
Relay: order_3
  Run: steps
  Phase: finished
  Status: ok
  Finished At (unix s): 1790607251
```

## Watch it undo itself

The paper exchange fills at most 100 at a time, so `confirm_fill` refuses a
larger order. Start one the same way, with a quantity above the limit:

```sh
flz relay start order_executor --id order_4 --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 500}' --channel order
```

`confirm_fill` refuses it after ten seconds, is tried three times, ten seconds
each, and then the order ends in failure. Every step that had finished is
undone, in reverse: the order is cancelled, then the money is released.
`record_order` has no undo, so the booking stays:

```
[book] EUR/USD 500 as ord-401645df395aad3f855cda29b2260d0e
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 34 ms): Error: the exchange fills at most 100 a time, and this is 500
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 105 ms): Error: the exchange fills at most 100 a time, and this is 500
[undo] cancelled ord-401645df395aad3f855cda29b2260d0e
[undo] released res-02ea05d4f24f6cb7d992bc45d67aa38f
```

```sh
flz relay info order_4 --namespace trading
```

```
...
Relay: order_4
  Run: steps
  Phase: finished
  Status: error
  Error: Error: the exchange fills at most 100 a time, and this is 500
  Finished At (unix s): 1790607285
```

## Building from a checkout

To run the project against a Flezha checkout instead of the published package, build
the SDK from the repository root and link it into this folder once:

```sh
scripts/build-node-sdk.sh --release
(cd sdk/node && npm link)
(cd sdk/node/examples/tradedesk && npm link @flezha/sdk)
```

`npm install` in this folder then keeps the link, and `package.json` stays as it
is.

## Where to go next

The echo in [`../echo`](../echo/README.md) is the project of the getting-started pages, and
[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what a tasklet, a relay and a runner each are.
