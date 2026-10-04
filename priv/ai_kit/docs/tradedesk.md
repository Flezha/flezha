# The tradedesk

This tutorial runs `tradedesk`, a toy trader, and uses it to show what a
deployment needs beyond the echo: authorization, a namespace of its own, state,
partitions, plugins, and recovery from a crash.

The desk is two operators. `trade_watcher` is an ETL stream that runs in flow:
it reads trades from a demo exchange, keeps each symbol's recent prices, and
sends out a buy signal when a price is far enough above their average. Each buy
also starts an `order_executor`, a workflow that runs in steps: it takes one
order through its steps with paper money and ends when the order is filled or
refused.

```mermaid
flowchart LR
    X(["demo exchange"]) -->|trades| W["trade_watcher<br/>runs in flow"]
    W -->|buy signals| F(["signals.txt"])
    W -->|one per buy| O["order_executor<br/>runs in steps"]
```

> #### What you need {: .tip}
>
> - What [the echo tutorial](../echo/first_steps.md) needed: a controller
>   release or Docker, and one language's toolchain for the runner.
> - **Node.js, with npm**, to run the demo exchange.
> - **git**, to get the example and the demo exchange.

> Get the example: clone the repository. The tradedesk of each language is in
> `sdk/<language>/examples/tradedesk`.
> {: .step}

```sh
$ git clone --recurse-submodules https://github.com/flezha/flezha.git
```

| Page | What you do |
|---|---|
| [Open the desk](open_the_desk.md) | Start a controller with authorization on, give the desk its namespace and key, and connect its runner |
| [Watch the trades](watch_the_trades.md) | Start `trade_watcher` in two partitions, send it trades, read its state, and scale it |
| [Connect the exchange](connect_the_exchange.md) | Feed it the demo exchange's live trades and write the buy signals to a file |
| [Place orders](place_orders.md) | Start an order by hand, read its journal, and watch a failed order undo itself |
| [Crash the desk](crash_the_desk.md) | Stop and crash the runner and the controller, and handle a message that cannot be read |
