# Docker DEV

Toolchain image for hacking on the Flezha controller and Rust compute engine —
bundles Elixir+OTP, Rust, Node.js, protoc, etc. so `docker compose up`
gives a ready-to-go shell against bind-mounted source. This is distinct
from the published runtime images:

| Tag                              | Purpose                                                  | Dockerfile                          |
|----------------------------------|----------------------------------------------------------|-------------------------------------|
| `alfetahe/flezha:latest`         | Prod runtime — Mix release + graceful-restart plumbing   | `docker/prod/Dockerfile.controller`      |
| `alfetahe/flezha:latest-dev`     | Runtime + Node.js + pre-built testnet WS server          | `docker/dev/Dockerfile.runtime`     |
| `alfetahe/flezha:latest-toolchain` | Dev toolchain (Elixir + Rust + Node + protoc, source-mounted) | `docker/dev/Dockerfile.toolchain` |

Build it, exec into it, run the controller, attach plugins, etc. Run all
commands below from the project *root* directory.

> **Not** the image for producing distribution artifacts. The native
> binaries committed under
> `sdk/java/flezha-sdk-native/src/main/resources/native/` and the two
> committed fat jars (`sdk/java/flezha-sdk/dist/flezha-sdk.jar`,
> `test/apps/java_runner/dist/java_runner.jar`) are
> produced by a separate, build-only image. See
> [`../build/README.md`](../build/README.md) — the one-shot wrapper is
> `scripts/build-dist.sh --docker`. CI never builds those artifacts; it
> consumes the committed copies, so any change under `sdk/java/`,
> `flz_core_ffi/`, `flz_core/`, `flz_transport/`, or `test/apps/java_runner/`
> needs an artifact refresh + commit before it merges.

## Quick reference

| You want to…                              | Run                              | Image                      |
|-------------------------------------------|----------------------------------|----------------------------|
| Run the controller + plugins for dev           | the steps in this file           | `docker/dev/Dockerfile.toolchain`  |
| Refresh committed dist artifacts          | `scripts/build-dist.sh --docker` | `docker/build/Dockerfile.dist` |
| Mirror what GitHub Actions does locally   | `scripts/ci-local.sh`            | (none — runs on host)      |
| Production controller                          | see `../prod/README.md`          | `docker/prod/Dockerfile.controller` |

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

The image also creates every path a compose file mounts a named volume onto
(`/flezha/_build`, `/flezha/deps`, `/flezha/target`, `/socket_data`, and the
container-local `/home/flezha/_build` + `/home/flezha/deps` the chaos and
healing stacks use) owned by uid 1000. Docker initialises a fresh named
volume from the image directory, ownership included, so **no harness
container needs root**. A mountpoint missing from that list is provisioned
root-owned instead — if you add one to a compose file, add it to
`Dockerfile.toolchain` in the same change.

`scripts/harness-boot.sh` is the shared entrypoint for the chaos and healing
stacks: it guards those mountpoints, fetches Elixir deps only when
`mix.lock` moved, waits for the controller's control socket, then execs the
service command. Because it is a plain script it can be exercised outside
compose:

```bash
docker run --rm -v "$PWD/scripts/harness-boot.sh:/boot.sh:ro" \
  -e FLEZHA_HARNESS_PATHS=/home/flezha/_build \
  alfetahe/flezha:latest-toolchain /boot.sh sh -c 'echo uid=$(id -u) pid=$$'
```

On a host where the image-supplied ownership does not apply (userns-remap),
start a harness service with `FLEZHA_CONTAINER_USER=0:0`: the script then
chowns only the mountpoints whose top-level owner is wrong and drops
privileges with `setpriv`. It never uses `su` — see
[graceful shutdown](../../docs/operations/graceful_shutdown.md).

## Build and push the toolchain image

```bash
# Build for the current host arch (run natively per arch — no QEMU/buildx)
make docker-build-toolchain        # or: docker build -f docker/dev/Dockerfile.toolchain -t alfetahe/flezha:latest-toolchain .
docker push alfetahe/flezha:latest-toolchain
```

## Build and push the runtime-dev image (`:latest-dev`)

`Dockerfile.runtime` is `FROM alfetahe/flezha:latest`, so the prod runtime
must be available locally (built per `../prod/README.md`, or pulled with
`docker pull alfetahe/flezha:latest`) before this step.

```bash
# Ensure :latest exists locally first (skip if you already pulled it)
make docker-build VERSION=latest

# Build for the current host arch (run natively per arch — no QEMU/buildx)
# or: docker build -f docker/dev/Dockerfile.runtime -t alfetahe/flezha:latest-dev .
make docker-build-dev              
docker push alfetahe/flezha:latest-dev
```

For a multi-arch manifest, mirror the two-native-machines flow in
`../prod/README.md` — build `:latest-dev-amd64` / `:latest-dev-arm64` on
each host, then `docker manifest create alfetahe/flezha:latest-dev …` and
`docker manifest push`.

## Run via docker compose

- Create docker network (if not exists)
```bash
docker network create flezha_network
```

- Build the images and start the containers:
```bash
docker compose -f docker/dev/docker-compose.yml up
```

- Start the controller inside the container, build rust cli commands and start iex:
```bash
docker exec -it flezha_controller_dev bash
mix release 
_build/dev/rel/flezha/bin/flz controller start --mode=attached_iex

# Or use directly IEX (you wont be able to use the CLI commands then) by default:
iex --name flezha@127.0.0.1 --cookie flezha_dev_cookie -S mix
```

- Start another terminal on controller container to start the compute engine (you can start multiple instances of compute engines):
```bash
docker exec -it flezha_controller_dev bash
cd test/apps/rust_runner
cargo run --target-dir=./target
```

- Build the testnet TS app inside the container (one-time setup):
```bash
docker exec -it flezha_controller_dev bash -c "cd test/apps/testnet && npm install && npm run build"
```


- Start another terminal on controller container:
```bash
docker exec -it flezha_controller_dev bash


# Start operative, websocket plugin and kafka output plugins:
_build/dev/rel/flezha/bin/flz operative start my_operative store_input --init_values '{"binary_inputs": [], "text_inputs": ["FIRST"]}' 


# `--X…` flags are PROVIDER options; attachment (`--operative_id`) and
# subscription (`--trigger`) are plain CLI options, consumed before the
# provider sees the map. A provider with a strict allow-list refuses every
# `--X` key it does not own.
_build/dev/rel/flezha/bin/flz plugin enable test_plugin_ws operative_websocket \
      --operative_id="my_operative" \
      --trigger="proactive" \
      --Xuri="ws://localhost:4500" \
      --Xpost_connect='{"payload":{"type":"text","message":"{\"event_type\":\"subscribe\",\"event_data\":{\"default_message\":\"hello world\",\"interval\":1000}}"}}'


_build/dev/rel/flezha/bin/flz plugin enable test_plugin_kafka operative_kafka_producer \
  --operative_id=my_operative \
  --trigger=on_output \
  --Xhosts=kafka:9092 \
  --Xtopic=test_topic
  
  
# Listen to operative IO:  
_build/dev/rel/flezha/bin/flz operative io_listen input my_operative
_build/dev/rel/flezha/bin/flz operative io_listen output my_operative
```

# Kafka UI:
Kafka UI can be seen on: `http://localhost:8080/`
