# Flezha

## Description
Distributed stateful processors. Provdes a static and dynamic creation of processors.

## Building
- Copy `.env.example` and rename to `.env`
- Navigate to the testnet app(`test/apps/testnet`) and follow the instruction from the `README.md` file.
- Build the `rust_compute_engine` application.
- Build the agent `mix compile`

## Examples

### Start agent (started automatically for dev mode)
Starting the node and specifying the external processes to be used by the node mynode.
PROD:
```bash
RELEASE_NODE=mynode FLEZHA_EXTERNAL_PROCESSES="cargo run --manifest-path=/home/anuar/Projects/personal/flezha/test/apps/rust_compute_engine/Cargo.toml --target-dir=./target" _build/prod/rel/flezha/bin/flz agent start --mode=attached_iex
```

DEV:
```bash
RELEASE_NODE=mynode _build/dev/rel/flezha/bin/flz agent start --mode=attached_iex
```

### Stop agent
Stopping the node.
```bash
_build/dev/rel/flezha/bin/flz agent stop
```

### Starting a processor.
```bash
_build/dev/rel/flezha/bin/flz processor start my_processor compare_filter --init_values='{
  "comparable": "hello world", 
  "inverted": false
}'
```

```bash
_build/dev/rel/flezha/bin/flzprocessor start my_processor do_nothing
```

### Stop a processor.
```bash
_build/dev/rel/flezha/bin/flz processor stop my_processor
```

### Enabling websocket plugin for processor. 
```bash
_build/dev/rel/flezha/bin/flz plugin enable processor_websocket  --settings='{
  "processor_id": "my_processor",
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
_build/dev/rel/flezha/bin/flz plugin enable processor_stdout  --settings='{
  "processor_id": "my_processor",
  "trigger": "on_out",
}'
```

### Get processor state.
```bash
_build/dev/rel/flezha/bin/flz processor state my_processor
```


### Performance Testing
Run performance tests to measure processor throughput and message processing capabilities:

```bash
# Run with default settings (1000 processors, 1000 messages per processor)
MIX_ENV=prod mix bench

# Run with custom settings
MIX_ENV=prod mix bench --num-processors 100 --messages-per-processor 100

# Short form aliases
MIX_ENV=prod mix bench -n 100 -m 50
MIX_ENV=prod mix bench -n 100 -m 50
```

**Options:**
- `--num-processors` / `-n`: Number of processors to create (default: 1000)
- `--messages-per-processor` / `-m`: Number of messages to send per processor (default: 1000)

The performance test creates multiple processors, enables plugins, sends messages, and measures:
- Processor creation time
- Plugin setup time  
- Message sending time
- Message processing time
- Overall throughput (messages/second)

### Development
Run the application inside the docker containers.
See `docker/dev/README.md` for more information.