# Aflecha

## Description
Distributed stateful computations. Provdes a static and dynamic creation of computations.

## Building
- Copy `.env.example` and rename to `.env`
- Navigate to the testnet app(`test/apps/testnet`) and follow the instruction from the `README.md` file.
- Build the `rust_operator` application.
- Build the agent `mix compile`

## Examples

### Start agent
Starting the node and specifying the external processes to be used by the node mynode.
```bash
RELEASE_NODE=mynode AFLECHA_EXTERNAL_PROCESSES="cargo run --manifest-path=/home/anuar/Projects/personal/aflecha/test/apps/rust_operator/Cargo.toml --target-dir=./target" _build/dev/rel/aflecha/bin/agent start --mode=attached_iex
```

### Stop agent
Stopping the node.
```bash
_build/dev/rel/aflecha/bin/agent stop
```

### Starting a computation.
```bash
_build/dev/rel/aflecha/bin/computation start my_computation compare_filter --variables='{
  "comparable": "hello world", 
  "inverted": false
}'
```

```bash
_build/dev/rel/aflecha/bin/computation start my_computation forward_orig
```

### Stop a computation.
```bash
_build/dev/rel/aflecha/bin/computation stop my_computation
```

### Adding IO to the computation.
```bash
_build/dev/rel/aflecha/bin/io add input my_computation --parameters='{
  "io_type": "websocket",
  "uri": "wss://ws.kraken.com",
  "post_connect": {
    "payload": [
      "text",
      {
        "event": "subscribe",
        "subscription": { "name": "trade" },
        "pair": ["BTC/EUR", "ETH/EUR"]
      }
    ]
  }
}'
```

### Development
Run the application inside the docker containers.
See `docker/dev/README.md` for more information.