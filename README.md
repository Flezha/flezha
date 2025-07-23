# Flezha

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
PROD:
```bash
RELEASE_NODE=mynode FLEZHA_EXTERNAL_PROCESSES="cargo run --manifest-path=/home/anuar/Projects/personal/flezha/test/apps/rust_operator/Cargo.toml --target-dir=./target" _build/prod/rel/flezha/bin/agent start --mode=attached_iex
```

DEV:
```bash
RELEASE_NODE=mynode _build/dev/rel/flezha/bin/agent start --mode=attached_iex
```

### Stop agent
Stopping the node.
```bash
_build/dev/rel/flezha/bin/agent stop
```

### Starting a computation.
```bash
_build/dev/rel/flezha/bin/computation start my_computation compare_filter --init_values='{
  "comparable": "hello world", 
  "inverted": false
}'
```

```bash
_build/dev/rel/flezha/bin/computation start my_computation do_nothing
```

### Stop a computation.
```bash
_build/dev/rel/flezha/bin/computation stop my_computation
```

### Enabling websocket plugin for computation. 
```bash
_build/dev/rel/flezha/bin/plugin enable computation_websocket  --settings='{
  "computation_id": "my_computation",
  "trigger": "proactive",
  "uri": "ws://localhost:4500",
  "post_connect": {
    "payload": {
      "type": "text",
      "message": "{\\"event_type\\": \\"subscribe\\",\\"event_data\\": {\\"default_message\\": \\"hello world\\",\\"interval\\": 1000}}"
    }
  }
}'
```

```bash
_build/dev/rel/flezha/bin/plugin enable computation_stdout  --settings='{
  "computation_id": "my_computation",
  "trigger": "on_out",
}'
```

### Get computation state.
```bash
_build/dev/rel/flezha/bin/computation state my_computation
```


### Development
Run the application inside the docker containers.
See `docker/dev/README.md` for more information.