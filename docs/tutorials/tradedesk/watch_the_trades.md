# Watch the trades

On this page you start the desk's ETL stream, `trade_watcher`, in two
partitions, send it trades by hand, read the prices it keeps, and scale it to
three partitions while it runs.

`trade_watcher` reads trade messages, keeps only market trades, keys each one by
its symbol so every symbol gets a cell with its own window of prices, and emits
a buy signal when a price is more than 2 % above the window's average.
[Streams](../../topics/basics/streams.md) explains the graph,
[State](../../topics/basics/state.md) the window kept per cell, and
[Configuring a relay](../../topics/basics/configuring_a_relay.md) why it
runs in flow, the default.

<!-- tabs-open -->

### Java

```java
// TradeWatcherRelay.java: the ETL stream
public final class TradeWatcherRelay implements Relay {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TradeMessage(String channel, List<Trade> data) {}

    @Override
    @Nonnull
    public String factoryId() {
        return "trade_watcher";
    }

    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.input(TradeMessage.class, "trades")                        // JSON from the exchange
                .filter(message -> "trade".equals(message.channel())) // trade messages only
                .flatMap(Trade.class, TradeMessage::data)            // a message into its trades
                .filter(trade -> "market".equals(trade.ord_type()))  // market orders only
                .keyBy(Trade::symbol)                                // one cell per symbol
                .into(TrackPrices.class)
                .into(BuyAboveAverage.class)                         // fed by the tasklet above
                .output("signals");                                  // out to the file plugin
    }
}
```

[`TradeWatcherRelay.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/tradedesk/src/main/java/com/flezha/examples/tradedesk/TradeWatcherRelay.java)

### Rust

```rust
// src/trade_watcher.rs: the ETL stream
#[derive(Clone, Deserialize)]
pub struct TradeMessage {
    #[serde(default)]
    channel: String,
    #[serde(default)]
    data: Vec<Trade>,
}

pub struct TradeWatcherRelay;

impl Relay for TradeWatcherRelay {
    fn factory_id() -> &'static str {
        "trade_watcher"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<TradeMessage>("trades") // JSON from the exchange
            .filter(|message| message.channel == "trade") // trade messages only
            .flat_map(|message| message.data) // a message into its trades
            .filter(|trade| trade.ord_type == "market") // market orders only
            .key_by(|trade| trade.symbol.clone()) // one cell per symbol
            .into::<TrackPrices>()
            .into::<BuyAboveAverage>() // fed by the tasklet above
            .output_named("signals"); // out to the file plugin
    }
}
```

[`src/trade_watcher.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/tradedesk/src/trade_watcher.rs)

### Node

```javascript
// trade_watcher_relay.mjs: the ETL stream
export default defineRelay({
  factoryId: 'trade_watcher',
  channels: (c) => {
    c.input('trades') // JSON from the exchange
      .filter((message) => message.channel === 'trade') // trade messages only
      .flatMap((message) => message.data) // a message into its trades
      .filter((trade) => trade.ord_type === 'market') // market orders only
      .keyBy((trade) => trade.symbol) // one cell per symbol
      .into(TrackPrices)
      .into(BuyAboveAverage) // fed by the tasklet above
      .output('signals'); // out to the file plugin
  },
});
```

[`trade_watcher_relay.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/tradedesk/trade_watcher_relay.mjs)

### Python

```python
# trade_watcher.py: the ETL stream
@flezha.relay("trade_watcher")
def trade_watcher(c: flezha.Channels) -> None:
    (
        c.input("trades")  # JSON from the exchange
        .filter(lambda message: message.get("channel") == "trade")  # trade messages only
        .flat_map(lambda message: message["data"])  # a message into its trades
        .filter(lambda trade: trade["ord_type"] == "market")  # market orders only
        .key_by(lambda trade: trade["symbol"])  # one cell per symbol
        .into(TrackPrices)
        .into(BuyAboveAverage)  # fed by the tasklet above
        .output("signals")  # out to the file plugin
    )
```

[`trade_watcher.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/tradedesk/trade_watcher.py)

### Ruby

```ruby
# trade_watcher_relay.rb: the ETL stream
class TradeWatcherRelay < Flezha::Relay
  factory_id "trade_watcher"

  channels do |c|
    c.input("trades")                                        # JSON from the exchange
     .filter { |message| message[:channel] == "trade" }      # trade messages only
     .flat_map { |message| message[:data] }                  # a message into its trades
     .filter { |trade| trade[:ord_type] == "market" }        # market orders only
     .key_by { |trade| trade[:symbol] }                      # one cell per symbol
     .into(TrackPrices)
     .into(BuyAboveAverage)                                  # fed by the tasklet above
     .output("signals")   # out to the file plugin
  end
end
```

[`trade_watcher_relay.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/tradedesk/trade_watcher_relay.rb)

### PHP

```php
// src/TradeWatcherRelay.php: the ETL stream
#[Relay(factoryId: 'trade_watcher')]
final class TradeWatcherRelay extends \Flezha\Relay
{
    public static function channels(Builder $c): void
    {
        $c->input('trades')                                                            // JSON from the exchange
            ->filter(fn (array $message) => ($message['channel'] ?? null) === 'trade') // trade messages only
            ->flatMap(fn (array $message) => $message['data'])                         // a message into its trades
            ->filter(fn (array $trade) => $trade['ord_type'] === 'market')             // market orders only
            ->keyBy(fn (array $trade) => $trade['symbol'])                             // one cell per symbol
            ->into(TrackPrices::class)
            ->into(BuyAboveAverage::class)                                             // fed by the tasklet above
            ->output('signals');                                                       // out to the file plugin
    }
}
```

[`src/TradeWatcherRelay.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/tradedesk/src/TradeWatcherRelay.php)

### Elixir

```elixir
# lib/tradedesk/trade_watcher.ex: the ETL stream
defmodule Tradedesk.TradeWatcher do
  use Flezha.SDK.Relay, factory_id: "trade_watcher"

  @impl true
  def channels(c) do
    c
    # JSON from the exchange
    |> input_with("trades", &Jason.decode/1)
    # trade messages only
    |> filter(&(&1["channel"] == "trade"))
    # a message into its trades
    |> flat_map(& &1["data"])
    # market orders only
    |> filter(&(&1["ord_type"] == "market"))
    # one cell per symbol
    |> key_by(& &1["symbol"])
    |> into(Tradedesk.TrackPrices)
    # fed by the tasklet above
    |> into(Tradedesk.BuyAboveAverage)
    # out to the file plugin
    |> output("signals")
  end
end
```

[`lib/tradedesk/trade_watcher.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/tradedesk/lib/tradedesk/trade_watcher.ex)

<!-- tabs-close -->

> Start `trade_watcher` as two partitions. The runner from
> [Open the desk](open_the_desk.md) has to be running.
> {: .step}

```sh
$ ./bin/flz relay start trade_watcher --id trade_watcher --partitions 2 --namespace trading
```

```output
Relay trade_watcher started with 2 partitions:
  trade_watcher#p0
  trade_watcher#p1
```

Each partition owns a share of the 256 key groups, and a symbol always lands in
the same one. [Partitioning](../../topics/advanced/partitioning.md) covers how.

> Send EUR/USD three trades. The third is more than 2 % above the first two,
> and `--await` waits three seconds for what comes out.
> {: .step}

```sh
$ ./bin/flz relay send 'trade_watcher#p0' --namespace trading --channel trades --text '{"channel": "trade", "data": [{"symbol": "EUR/USD", "price": 1.0850, "ord_type": "market"}]}'
$ ./bin/flz relay send 'trade_watcher#p0' --namespace trading --channel trades --text '{"channel": "trade", "data": [{"symbol": "EUR/USD", "price": 1.0853, "ord_type": "market"}]}'
$ ./bin/flz relay send 'trade_watcher#p0' --namespace trading --channel trades --text '{"channel": "trade", "data": [{"symbol": "EUR/USD", "price": 1.1100, "ord_type": "market"}]}' --await 3000
```

```output
Delivered to 'trade_watcher#p0'.
Delivered to 'trade_watcher#p0'.
Delivered to 'trade_watcher#p0'.
signals	EUR/USD 1.1100 is 2.29% above its average of 1.0852, buying 23
```

By hand you name the partition that owns the symbol, or pass the symbol as
`--key`. The buy also started an order, and the runner's terminal shows it
booked:

```output
[book] EUR/USD 23 as ord-993c88bfb1bc7474a70cd3db8312b447
```

> Send GBP/USD a trade, to the partition that owns it.
> {: .step}

```sh
$ ./bin/flz relay send 'trade_watcher#p1' --namespace trading --channel trades --text '{"channel": "trade", "data": [{"symbol": "GBP/USD", "price": 1.3400, "ord_type": "market"}]}'
```

Read every symbol's window, from every partition:

```sh
$ ./bin/flz state get trade_watcher/track_prices --namespace trading
```

```output
state get trade_watcher/track_prices:
  change_seq: 3
  state:      {"track_prices":{"EUR/USD":{"prices":[1.085,1.0853,1.11]},"GBP/USD":{"prices":[1.34]}}}
```

> Scale `trade_watcher` to three partitions.
> {: .step}

```sh
$ ./bin/flz relay scale trade_watcher 3 --namespace trading
```

```output
Scaling relay group trade_watcher to 3...
Relay trade_watcher rescaled 2 -> 3 (2 cells moved, 0 trees removed):
  trade_watcher#p0
  trade_watcher#p1
  trade_watcher#p2
```

The two cells whose symbols now belong to another partition moved there with
their state. EUR/USD's window came along:

```sh
$ ./bin/flz state get trade_watcher/track_prices/EUR/USD --namespace trading
```

```output
state get trade_watcher/track_prices/EUR/USD:
  change_seq: 3
  state:      {"track_prices":{"EUR/USD":{"prices":[1.085,1.0853,1.11]}}}
```

## Next

[Connect the exchange](connect_the_exchange.md) replaces the trades you typed
with the demo exchange's live feed.
