# Docker DEV

Run the following commands from the project *root* directory.

# Build, run and manage docker image
- Create docker network (if not exists)
```bash
docker network create flezha_network
```

- Build the images and start the containers:
```bash
docker compose -f docker/dev/docker-compose.yml up
```

- Start the agent inside the container, build rust cli commands and start iex:
```bash
docker exec -it flezha_agent_dev bash
mix release 
_build/dev/rel/flezha/bin/flz agent start --mode=attached_iex

# Or use directly IEX (you wont be able to use the CLI commands then) by default:
iex --name flezha@127.0.0.1 --cookie flezha_dev_cookie -S mix
```

- Start another terminal on agent container to start the compute engine (you can start multiple instances of compute engines):
```bash
docker exec -it flezha_agent_dev bash
cd test/apps/rust_compute_engine
cargo run --target-dir=./target
```


- Start another terminal on agent container:
```bash
docker exec -it flezha_agent_dev bash


# Start processor, websocket plugin and kafka output plugins:
_build/dev/rel/flezha/bin/flz processor start my_processor store_input --init_values '{"binary_inputs": [], "text_inputs": ["FIRST"]}' 


_build/dev/rel/flezha/bin/flz plugin enable test_plugin_ws processor_websocket \
      --Xprocessor_id="my_processor" \
      --Xtrigger="proactive" \
      --Xuri="ws://localhost:4500" \
      --Xpost_connect='{"payload":{"type":"text","message":"{\"event_type\":\"subscribe\",\"event_data\":{\"default_message\":\"hello world\",\"interval\":1000}}"}}' \
      --Xposition=0


_build/dev/rel/flezha/bin/flz plugin enable test_plugin_kafka processor_kafka_producer \
  --Xprocessor_id=my_processor \
  --Xtrigger=on_output \
  --Xhosts=kafka:9092 \
  --Xtopic=test_topic
  
  
# Listen to processor IO:  
_build/dev/rel/flezha/bin/flz processor io_listen input my_processor
_build/dev/rel/flezha/bin/flz processor io_listen output my_processor
```

# Kafka UI:
Kafka UI can be seen on: `http://localhost:8080/`
