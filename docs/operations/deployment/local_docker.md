# Running locally in Docker

Two stacks: the toolchain container a contributor develops in, and the
built production image under docker compose.

## Local development (Docker)

`scripts/dev.sh` drives `docker/dev/docker-compose.yml`. It copies
`.env.example` to `.env` when missing (the file feeds the compose file's
`${VAR:-default}` substitutions, e.g. `FLEZHA_TRANSPORT`) and creates the
external network `flezha_network`.

```bash
./scripts/dev.sh build     # build the toolchain image and start the stack
./scripts/dev.sh up        # start without rebuilding
./scripts/dev.sh shell     # bash in flezha_controller_dev
./scripts/dev.sh iex       # iex --name flezha@127.0.0.1 --cookie flezha_dev_cookie -S mix
./scripts/dev.sh logs      # follow the controller service
./scripts/dev.sh status    # docker compose ps
./scripts/dev.sh release   # mix release --overwrite inside the container
./scripts/dev.sh cargo build --workspace   # any cargo command inside the container
./scripts/dev.sh down      # stop containers
./scripts/dev.sh clean     # down -v: drop the build-cache volumes
./scripts/dev.sh rebuild   # down -v, build, up
```

The stack: `controller` (toolchain image, source bind-mounted at `/flezha`,
`FLEZHA_SOCKET_FILES_PATH=/flezha/app_data/sockets/`), `zookeeper` (2181),
`kafka` (9092), `kafka_ui` (host port **8081**), `postgres-sink` (host port
5433, for `POSTGRES_INTEGRATION=1 mix test --include postgres_integration`),
`minio` (host port 9100, for `S3_INTEGRATION=1 mix test --include s3_integration`)
and `rust`, which waits for the control socket and runs the test runner against
`FLEZHA_DEMO_NAMESPACE` (default `default`). Named volumes `flezha_build`,
`flezha_deps` and `flezha_cargo_target` cache build output across restarts;
the single bind mount also carries `app_data/` (ProcessHub registry, WAL
files, plugin root). The toolchain image creates each of those volume
mountpoints owned by uid 1000, so the stack runs unprivileged; the chaos and
healing stacks share `scripts/harness-boot.sh`, which guards the mountpoints,
skips `mix deps.get` when `mix.lock` is unchanged, and execs the workload in
place so it keeps PID 1.

Inside the container, first time:

```bash
cargo build --workspace          # NIF, CLI, SDK, test runner
mix deps.get && mix compile      # Rustler reuses the built NIF
```

Run the controller either in IEx (`./scripts/dev.sh iex`; no `flz` CLI) or as a
release:

```bash
mix release --overwrite
_build/dev/rel/flezha/bin/flzvm start
```

Run a runner from a second shell (this is what the `rust` service does):

```bash
cargo run --quiet --manifest-path=test/apps/rust_runner/Cargo.toml -- default
```

## Local production smoke test

`docker/prod/docker-compose.yml` runs the built image (container
`flezha_agent_prod`) with `zookeeper_prod` and `kafka_prod`, named volumes
`flezha_sockets`, `flezha_data`, `flezha_certs`, `flezha_logs`,
`flezha_state`, `flezha_process_hub` and `flezha_plugins`, and
`stop_grace_period: 75s`. It sets `MIX_ENV=prod`, `FLEZHA_TRANSPORT`,
`FLEZHA_SOCKET_FILES_PATH=/flezha/app_data/sockets/`,
`FLEZHA_LOG_PATH=/flezha/app_data/logs/flezha.log`, `FLEZHA_LOG_LEVEL` and the
two graceful-shutdown variables. It sets neither `FLEZHA_DURABILITY_BACKEND`
nor `RELEASE_COOKIE`: add both to the controller's `environment:` block before
the first `up`, or the release refuses to boot and `flz` cannot reach it.

```bash
VERSION=0.2.0 make docker-build
VERSION=0.2.0 docker compose -f docker/prod/docker-compose.yml up -d
docker logs -f flezha_agent_prod
docker exec flezha_agent_prod bin/flz controller status
docker compose -f docker/prod/docker-compose.yml down
```

`FLEZHA_TRANSPORT=quic VERSION=0.2.0 docker compose … up -d` starts the stack
on QUIC. The full prod-image drill — the boot refusal with the backend unset,
the `none` opt-out, and recovery of the DLQ, authz and schedule stores through a
`local_path` and a `postgres` backend — is `test/scripts/durability_posture.sh`
(`BACKENDS=local_path` to restrict it, `FLEZHA_KEEP_STACK=1` to keep the
containers). It builds `alfetahe/flezha:durability-validation` when absent.
