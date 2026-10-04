# Setting up a checkout

This page gets a checkout of the repository built and ready for the tests,
either on your machine or inside the development containers. To build a
release of the controller instead, see
[Building from source](../topics/deploying/building_from_source.md).

## What you need

- Elixir 1.17 or later, with its Erlang/OTP.
- A stable Rust toolchain. `rust-toolchain.toml` selects it.
- Node.js with npm, for the WebSocket test server some tests connect to.
- For work on the Java SDK, Java 21 and Maven.
- Docker, only for the development containers below.

## Get the code

```sh
$ git clone --recurse-submodules https://github.com/flezha/flezha.git
$ cd flezha
```

The SDKs (`sdk/<language>`), the plugin packages (`plugins/`) and the WebSocket
test server (`test/apps/testnet`) are git submodules, each a repository of its
own. A clone made without `--recurse-submodules` leaves them empty; fill them in
with:

```sh
$ git submodule update --init
```

## Build

```sh
$ make build
$ mix test.setup
```

`make build` runs `cargo build --workspace`, `mix deps.get` and `mix compile`.
`mix test.setup` builds the WebSocket test server. The tests that need it skip
with a message when it is missing.

Every `make` target is listed in
[Building from source](../topics/deploying/building_from_source.md#make-targets).

### The native library for Java

The Java SDK calls the shared Rust core through a native library,
`libflz_core_ffi`. It is build output and is not in git, so build it once after
cloning, and again after changing `flz_core_ffi/`, `flz_core/` or
`flz_transport/`:

```sh
$ scripts/build-native.sh --output sdk/java/flezha-sdk-native/src/main/resources/native
```

That builds it for your own platform, which is all the tests read. The other
platforms are a release concern, covered in [Releasing](releasing.md).

## In the development containers

`docker/dev/docker-compose.yml` runs a container with the whole toolchain
(Elixir, Rust, Node.js and protoc) against your checkout, mounted at `/flezha`,
next to the services the plugin packages are tested against. `scripts/dev.sh`
drives it from the project root:

```sh
$ scripts/dev.sh build     # build the images
$ scripts/dev.sh up        # start the containers
$ scripts/dev.sh shell     # a shell in the controller container
$ scripts/dev.sh iex       # the controller in an IEx session
$ scripts/dev.sh down      # stop and remove the containers
$ scripts/dev.sh help      # every command
```

On its first run it creates `.env` from `.env.example` and the
`flezha_network` Docker network.

| Service | On the host |
|---|---|
| `rust`, a Rust test runner that connects to the controller | |
| `kafka` | `localhost:19092` (`kafka:9092` inside the network) |
| `kafka_ui` | `http://localhost:8081/` |
| `postgres-sink` | `localhost:5433` |
| `postgres-cdc`, with `wal_level=logical` | `localhost:5434`, database `flezha_test`, user `flezha`, password `flezha_test` |
| `rabbitmq` | AMQP on `localhost:5672`, management on `http://localhost:15672/` (`guest` / `guest`) |
| `minio` | `localhost:9100` |

To start one service alone:

```sh
$ docker compose -f docker/dev/docker-compose.yml up -d rabbitmq
```

### The images

| Image | Dockerfile | Built with |
|---|---|---|
| `flezha/flezha:latest-toolchain`, the development container | `docker/dev/Dockerfile.toolchain` | `make docker-build-toolchain` |
| `flezha/flezha:latest-dev`, the release plus Node.js and the WebSocket test server | `docker/dev/Dockerfile.runtime` | `make docker-build-dev` |

`Dockerfile.runtime` starts from `flezha/flezha:latest`, so build that first
with `make docker-build VERSION=latest`, or pull it.

The toolchain image creates every directory a compose file mounts a named
volume onto, owned by uid 1000, so no container needs root. A compose file that
mounts a volume somewhere new needs that directory added to
`Dockerfile.toolchain` in the same change.
