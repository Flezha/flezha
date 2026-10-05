# Connect the exchange

On this page you start the demo exchange, feed its live trades into
`trade_watcher` with the WebSocket plugin, and write the buy signals to a file.
[Plugins](../../topics/plugins/plugins.md) explains how a plugin is enabled and
bound, and [the WebSocket plugin](../../plugins/websocket_subscription.md) lists
its options.

> Clone the demo exchange and start it in a terminal of its own. It runs until
> you press Ctrl-C.
> {: .step}

```sh
$ git clone https://github.com/flezha/flezha_testnet.git
$ cd flezha_testnet
$ npm ci
$ npm run build
$ npm run start
```

```output
Loaded testnet config from testnet-config.json: channels [trade, book], 7 tuned symbol(s)
Testnet webSocket server is listening on ws://localhost:4500
```

> Enable the WebSocket plugin as `exchange` and bind `trade_watcher` to it,
> reading three currency pairs onto the `trades` channel. The desk has to be
> running as [Watch the trades](watch_the_trades.md) left it.
> {: .step}

```sh
$ ./bin/flz plugin enable exchange relay_websocket \
  --relay_id=trade_watcher --namespace trading \
  --Xuri=ws://localhost:4500/ \
  --Xchannel=trades \
  --Xsubscription_items='["EUR/USD", "GBP/USD", "USD/JPY"]' \
  --Xsubscribe_template='{"method": "subscribe", "params": {"channel": "trade", "symbol": {{items}}}, "req_id": {{req_id}}}' \
  --Xunsubscribe_template='{"method": "unsubscribe", "params": {"channel": "trade", "symbol": {{items}}}, "req_id": {{req_id}}}'
```

```output
Enabling plugin...
Plugin exchange enabled.
Plugin exchange bound to trade_watcher.
```

Options with an `X` prefix are the plugin's own settings. Each of the three
partitions runs its own connection and subscribes only to the symbols it owns,
which the exchange's terminal shows:

```output
Testnet client connected on /
Testnet client connected on /
Testnet client connected on /
Subscribing trade/GBP/USD (trade every 1500ms)
Subscribing trade/EUR/USD (trade every 1500ms)
Subscribing trade/USD/JPY (trade every 1500ms)
```

Trades now arrive on their own. Whenever one is far enough above its average,
an order is booked, and the runner's terminal shows it:

```output
[book] EUR/USD 32 as ord-8e35b354af95a17c48f5a31db79f5f12
[book] USD/JPY 21 as ord-3d5fdddd0094fb3152e57ef05b02be57
```

> Enable a file output as `signals`, bound to `trade_watcher`, that writes every
> signal to `signals.txt`.
> {: .step}

```sh
$ ./bin/flz plugin enable signals relay_file_output \
  --relay_id=trade_watcher --namespace trading \
  --hook=on_output \
  --Xpath=signals.txt \
  --Xchannel_out=signals
```

```output
Enabling plugin...
Plugin signals enabled.
Plugin signals bound to trade_watcher.
```

From the directory the controller was started in:

```sh
$ head -4 app_data/files/signals.txt
```

```output
EUR/USD 1.0651 is 2.41% above its average of 1.0400, buying 24
EUR/USD 1.0701 is 2.56% above its average of 1.0435, buying 26
EUR/USD 1.0774 is 2.86% above its average of 1.0474, buying 29
EUR/USD 1.0773 is 2.46% above its average of 1.0514, buying 25
```

## Next

[Place orders](place_orders.md) looks at the orders the desk is starting.
