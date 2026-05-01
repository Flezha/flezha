# Docker DEV

Toolchain image for hacking on the Flezha agent and Rust compute engine —
bundles Elixir+OTP, Rust, Node.js, protoc, etc. so `docker compose up`
gives a ready-to-go shell against bind-mounted source. This is distinct
from the published runtime images:

| Tag                              | Purpose                                                  | Dockerfile                          |
|----------------------------------|----------------------------------------------------------|-------------------------------------|
| `alfetahe/flezha:latest`         | Prod runtime — Mix release + graceful-restart plumbing   | `docker/prod/Dockerfile.agent`      |
| `alfetahe/flezha:latest-dev`     | Runtime + Node.js + pre-built testnet WS server          | `docker/dev/Dockerfile.runtime`     |
| `alfetahe/flezha:latest-toolchain` | Dev toolchain (Elixir + Rust + Node + protoc, source-mounted) | `docker/dev/Dockerfile.toolchain` |

Build it, exec into it, run the agent, attach plugins, etc. Run all
commands below from the project *root* directory.

> **Not** the image for producing distribution artifacts. The native
> binaries committed under
> `sdk/java/flezha-sdk-native/src/main/resources/native/` and the two
> committed fat jars (`sdk/java/flezha-sdk/dist/flezha-sdk.jar`,
> `test/apps/java_compute_engine/dist/java_compute_engine.jar`) are
> produced by a separate, build-only image. See
> [`../build/README.md`](../build/README.md) — the one-shot wrapper is
> `scripts/build-dist.sh --docker`. CI never builds those artifacts; it
> consumes the committed copies, so any change under `sdk/java/`,
> `flz_jni/`, `flz_transport/`, or `test/apps/java_compute_engine/`
> needs an artifact refresh + commit before it merges.

## Quick reference

| You want to…                              | Run                              | Image                      |
|-------------------------------------------|----------------------------------|----------------------------|
| Run the agent + plugins for dev           | the steps in this file           | `docker/dev/Dockerfile.toolchain`  |
| Refresh committed dist artifacts          | `scripts/build-dist.sh --docker` | `docker/build/Dockerfile.dist` |
| Mirror what GitHub Actions does locally   | `scripts/ci-local.sh`            | (none — runs on host)      |
| Production agent                          | see `../prod/README.md`          | `docker/prod/Dockerfile.agent` |

# ADD step to copy the .env.example to .env file if you don't have it yet:
```bash
cp .env.example .env
# TODO: check the default values inside .env and maybe create separate for dev and prod and testing.
```

# Build, run and manage docker image

The toolchain image (`alfetahe/flezha:latest-toolchain`) bundles the full
toolchain — Elixir+OTP, Rust, Node.js+npm+TypeScript, protoc — so
`docker compose up` gives a ready-to-go shell against bind-mounted source.
This is the image used for hacking on Flezha itself; downstream consumers
should use `:latest` (prod runtime) or `:latest-dev` (runtime + Node + testnet).

## Build and push the toolchain image

```bash
# Build for the current host arch (run natively per arch — no QEMU/buildx)
docker build -f docker/dev/Dockerfile.toolchain -t alfetahe/flezha:latest-toolchain .
docker push alfetahe/flezha:latest-toolchain
```

## Run via docker compose

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

- Build the testnet TS app inside the container (one-time setup):
```bash
docker exec -it flezha_agent_dev bash -c "cd test/apps/testnet && npm install && npm run build"
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
