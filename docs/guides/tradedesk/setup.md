# Setup

Part 2.

The tradedesk stack in Docker Compose: a controller, and the testnet standing in
for an exchange.

## What Part 2 builds

`tradedesk` is a small paper-trading desk. It reads trades, turns them into
candles, and when a candle gives a signal it places an order with paper money,
so nothing ever reaches a real exchange. Every page shows the same code in each
SDK language.

It runs entirely in Docker Compose:

| Container | What it is |
|---|---|
| `controller` | The Flezha controller, as in Part 1 |
| `testnet` | A stand-in exchange streaming Kraken v2 trades for four demo symbols |
| `runner` | Your tradedesk code, joining once there is code to run |

It has two operatives, one of each kind:

- **A stream operative, `candles`**, runs for as long as the desk does. Trades
  come in keyed by symbol and become one-minute candles, and a closed candle that
  gives a signal starts an order. [`stream_basics.md`](stream_basics.md) writes
  it by hand: procedures chained one into the next, `map`, `filter` and
  `key_by`, a merge of two channels into one procedure, and state per symbol.
- **A workflow operative, `paper_order`**, runs once per order and then ends. It
  reserves funds, places the paper order and records the fill, and if a step
  fails it releases the funds again.
  [`workflow_basics.md`](workflow_basics.md) writes it by hand: steps, retries
  and compensation.

Until [`plugins.md`](plugins.md), you send trades by hand. The other pages change
how the same desk runs, not what it does: its own namespace, authorization,
surviving a crash, real trades from the testnet, partitions, several controllers
and Kubernetes. [`introduction.md`](../introduction.md)
lists them in order.

## Start the stack

TODO: not written yet.
