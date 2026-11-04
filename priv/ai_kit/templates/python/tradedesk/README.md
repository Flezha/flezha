# Tradedesk: the Python project of the tradedesk tutorial

The toy trader the [tradedesk tutorial](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/tradedesk/tradedesk.md) builds, as a
standalone project. Nothing here depends on the Flezha source tree:
`requirements.txt` names one released version of `flezha`, the way your own
project would. It needs Python 3.11 or later.

It holds the desk's two relays and the runner that serves them. The tutorial
writes them one tasklet at a time.

| File | What is in it |
|---|---|
| `runner.py` | the runner: this process joins the `trading` namespace and registers both factories |
| `trade_watcher.py` | the ETL stream, a flow relay: the exchange's trade messages in, a cell per symbol, buy signals out |
| `track_prices.py` | keeps each symbol's last ten prices, and compares a new price with their average |
| `buy_above_average.py` | starts an order when a price is 2 % or more above its average |
| `order_executor.py` | the workflow, a steps relay: two steps in order, then two side by side |
| `reserve_funds.py` | step one: puts the money aside, and releases it again if the order is undone. The order an instance is started with is the `OrderTicket` here |
| `place_order.py` | step two: places the paper order, and cancels it if the order is undone |
| `confirm_fill.py` | waits ten seconds for the exchange, confirms the fill and ends the order with its result, and refuses an order above the exchange's limit |
| `record_order.py` | books the placed order, and has nothing to undo |

Every tasklet is a class with `@flezha.tasklet(name="...")`, whose `run` takes
the value and a `flezha.Ctx[State, Output]`. What a tasklet remembers is a
`@flezha.state` dataclass, and the changes that record it and the values it
takes and hands on are dicts, typed as `TypedDict`s, beside it in its own file.

## Run it

You need a controller running, the `trading` namespace created and a key for it
([Open the desk](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/tradedesk/open_the_desk.md) does both).

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
FLEZHA_NAMESPACE_SECRET=<trading key> .venv/bin/python runner.py
```

It keeps running until a SIGTERM, Ctrl-C, or its standard input closing, so do
not start it with standard input closed. The namespace is the first argument
and defaults to `trading`: `.venv/bin/python runner.py my_desk`.

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
`ctx.stable_id()`, so yours differ:

```
[book] EUR/USD 10 as ord-6b2e13f521b6781afd9f710152cd2f25
```

About ten seconds later, once `confirm_fill` has its answer from the exchange,
the instance ends, and `flz relay info` shows how:

```sh
flz relay info order_1 --namespace trading
```

```
...
Relay: order_1
  Run: steps
  Phase: finished
  Status: ok
  Finished At (unix s): 1790594741
```

## Watch it undo itself

The paper exchange fills at most 100 at a time, so `confirm_fill` refuses a
larger order. Start one the same way, with a quantity above the limit:

```sh
flz relay start order_executor --id order_2 --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 500}' --channel order
```

`confirm_fill` refuses it after ten seconds, is tried three times, ten seconds
each, and then the order ends in failure. Every step that had finished is
undone, in reverse: the order is cancelled, then the money is released.
`record_order` has no undo, so the booking stays:

```
[book] EUR/USD 500 as ord-0e7ee9553cfe5eac606fd232fd7ab33c
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 67 ms): RuntimeError: the exchange fills at most 100 a time, and this is 500
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 144 ms): RuntimeError: the exchange fills at most 100 a time, and this is 500
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
  Error: RuntimeError: the exchange fills at most 100 a time, and this is 500
  Finished At (unix s): 1790595063
```

## Building from a checkout

To run the project against a Flezha checkout instead of the published package,
build a wheel for this platform once, from the package root:

```sh
pip install "maturin>=1.9,<2.0"
maturin build --release --out dist --features python-extension,python-abi3
```

`maturin build` compiles the SDK's native library, so it needs a Rust
toolchain. Then install the wheel into this folder's environment instead of
the `requirements.txt` line:

```sh
cd examples/tradedesk
python3 -m venv .venv
.venv/bin/pip install ../../dist/flezha-0.1.0-*.whl
```

## Where to go next

The echo in [`../echo`](../echo/README.md) is the project of the getting-started pages, and
[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what a tasklet, a relay and a runner each are.
