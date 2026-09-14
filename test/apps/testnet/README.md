# Flezha Testnet

# Install dependencies
```bash
npm ci
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

## Manual input

Run the testnet in a terminal and it reads typed lines, sending each one to
every connected socket. This is the hand-driven half of an end-to-end test: the
`operative_websocket` source connects to the testnet as a client, so what you
type here is what the operative ingests.

```
$ npm run start
Manual input enabled. Type /help for the commands.
Testnet client connected on /in
{"symbol":"BTC/DEMO1","price":42000}
sent to 1 socket(s)
/bulk 500 30s {"n":{{n}},"at":"{{ts}}"}
bulk: 500 message(s), one every 60ms
```

| Line | Effect |
| --- | --- |
| any text | sent verbatim to every input socket |
| `/bulk <count> <duration> <text>` | `<count>` copies spread evenly over `<duration>` |
| `/stop` | cancel a running bulk |
| `/peers` | list the connected sockets and their paths |
| `/help` | the command list |

Durations are `250ms`, `30s` or `2m`; a bare number is milliseconds. In a bulk
template `{{n}}` becomes the 1-based index and `{{ts}}` an ISO timestamp, which
is what makes a burst checkable for ordering and for loss.

A window shorter than the message count collapses to one send per millisecond
rather than a burst the timer cannot deliver, so `/bulk 500 10ms` takes half a
second, not ten milliseconds.

**The `/out` path is never fed.** A socket that connects on `/out` is excluded
from every broadcast, so a sink attached there cannot have manual input echoed
onto it. Every other path, `/in` and the pathless `/` included, receives.

Manual input is on when stdin is a terminal and off otherwise, because the
controller starts the testnet as an Erlang Port in `:dev` and `:test` and
stdin is the shutdown channel there, not a keyboard. Force it either way with
`FLEZHA_TESTNET_MANUAL=1` or `=0`.

## Output

A socket connecting on `/out` is treated as a Flezha sink reporting what an
operative emitted. Its frames are printed verbatim and nothing is sent back: no
acks, no generators, and none of the manual input above. That is what lets one
testnet process be both ends of a hand-driven run.

```
Testnet client connected on /out
out < {"total":3}
```

What arrives is whatever the websocket sink was configured to send. With no
`sink_template` the operative's payload goes out on its own, which is the line
above. A template composes the frame instead: `{{payload}}` is required, and
`{{req_id}}`, `{{entity}}` and `{{seq}}` are available.

A websocket text frame has no header slot, so a template is the only way the
identity stamps reach the other end. To see them here, give the sink one:

```
--plugins '[{"id":"out","provider":"operative_websocket","opts":{
  "uri":"ws://localhost:4500/out",
  "sink_template":"{\"payload\":{{payload}},\"entity\":\"{{entity}}\",\"seq\":{{seq}}}"
},"hooks":[["on_output",{}]]}]'
```

```
out < {"payload":{"total":3},"entity":"order-7","seq":41}
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
| `FLEZHA_TESTNET_MANUAL` | `1` forces manual input on, `0` off; unset follows whether stdin is a terminal |

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