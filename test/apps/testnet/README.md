# Flezha Testnet

# Install dependencies
```bash
npm install
```

## Build the testnet
```bash
npm run build
```

## Run the tests
```bash
npm test
```

## Start the testnet server
```bash
npm run start
```

## Subscriptions

The server speaks two subscription dialects on the same socket.

**Kraken v2** — what the `operative_websocket` subscription source sends:

```json
{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/DEMO1"]},"req_id":7}
```

Each request is acked once per symbol, echoing its `req_id` (the source commits
its applied set on that echo). Generators run **per connection per symbol**, so
a partitioned operative's members each receive only what they subscribed.

**Legacy** — `{"event_type":"subscribe","event_data":{"generators":[…]}}`,
which carries its own intervals and generator config, unchanged.

## Configuration

| Env | Meaning |
| --- | --- |
| `FLEZHA_TESTNET_WS_PORT` | Listen port (default `4500`) |
| `GENERATORS_PATH` | Directory of generator modules (default `./generators`) |
| `FLEZHA_TESTNET_CONFIG` | JSON file mapping Kraken channels to generators, plus per-symbol tuning |

A v2 subscribe carries no room for a base price or a tick interval, so that
tuning is server-side:

```json
{
  "channels": {
    "trade": { "generator": "trade", "interval_ms": 1500, "config": { "volatility": 0.002 } },
    "book":  { "generator": "order_book", "interval_ms": 1000, "config": { "levels": 100 } }
  },
  "symbol_defaults": { "basePrice": 42000 },
  "symbols": { "ETH/DEMO1": { "basePrice": 2500 } }
}
```

A symbol with no entry streams on `symbol_defaults`, so subscribing a new pair
needs no server restart.