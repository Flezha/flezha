# Aflecha

## Description
Distributed stateful pipelines. Provdes a static and dynamic creation of pipelines.

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
RELEASE_NODE=mynode AFLECHA_EXTERNAL_PROCESSES="cargo run --manifest-path=/home/anuar/Projects/personal/aflecha/test/apps/rust_operator/Cargo.toml --target-dir=./target" _build/prod/rel/aflecha/bin/agent start --mode=attached_iex
```

DEV:
```bash
RELEASE_NODE=mynode _build/dev/rel/aflecha/bin/agent start --mode=attached_iex
```

### Stop agent
Stopping the node.
```bash
_build/dev/rel/aflecha/bin/agent stop
```

### Starting a pipeline.
```bash
_build/dev/rel/aflecha/bin/pipeline start my_pipeline compare_filter --init_values='{
  "comparable": "hello world", 
  "inverted": false
}'
```

```bash
_build/dev/rel/aflecha/bin/pipeline start my_pipeline do_nothing
```

### Stop a pipeline.
```bash
_build/dev/rel/aflecha/bin/pipeline stop my_pipeline
```

### Enabling websocket plugin for pipeline.
```bash
_build/dev/rel/aflecha/bin/plugin enable pipeline_websocket  --settings='{
  "pipeline_id": "my_pipeline",
  "trigger": "proactive",
  "uri": "wss://ws.kraken.com",
  "post_connect": {
    "payload": {
      "type": "text",
      "message": "{\\"event\\": \\"subscribe\\", \\"subscription\\": {\\"name\\": \\"trade\\" }, \\"pair\\": [\\"BTC/EUR\\", \\"ETH/EUR\\"]}"
    }
  }
}'
```

### Get pipeline state.
```bash
_build/dev/rel/aflecha/bin/pipeline state my_pipeline
```


### Development
Run the application inside the docker containers.
See `docker/dev/README.md` for more information.