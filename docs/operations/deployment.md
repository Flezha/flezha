# Deployment

This guide is for whoever runs a Flezha Controller: local development in
Docker, building and publishing the production image, smoke-testing that image
with docker compose, and deploying it to Kubernetes. It names only the
variables a deploy cannot omit; every other knob, with its default and the file
that reads it, is in [`../configuration.md`](../configuration.md). All commands
run from the project root.

## Variables you must set

| Variable | Why a deploy cannot omit it |
|---|---|
| `FLEZHA_DURABILITY_BACKEND` | A prod release refuses to boot with it unset (`Flezha.State.DurabilityTier.children/0`). `none` is the explicit local-only opt-out — byte-identical to a build without the remote tier, reported as `durability: local_only` on `/healthz`. `postgres`, `s3` and `local_path` enable the tier and need their `FLEZHA_DURABILITY_PG_*`, `FLEZHA_DURABILITY_S3_*` or `FLEZHA_DURABILITY_PATH` family. Any other spelling resolves to `none`. See [`remote_durability_tier.md`](remote_durability_tier.md). |
| `RELEASE_COOKIE` | Without it `rel/env.sh.eex` starts the release with `RELEASE_DISTRIBUTION=none`: the node cannot cluster and every `flz` verb fails, because the CLI reaches the BEAM over distribution. There is no source-committed fallback. |
| `FLEZHA_SOCKET_FILES_PATH` | The directory runners attach through; share it with every runner container on the node. The image's readiness check and the entrypoint both key off `<path>/runner_control_channel.sock`. |
| `FLEZHA_NODE_NAME`, `FLEZHA_NODE_HOST` | Form the release node name `RELEASE_NODE=<name>@<host>` (`rel/env.sh.eex`); in Kubernetes the host is the pod IP. |
| `FLEZHA_STATE_DIR`, `FLEZHA_PROCESS_HUB_REGISTRY_ROOT`, `FLEZHA_DLQ_DIR`, `FLEZHA_AUTHZ_DIR`, `FLEZHA_SCHEDULE_DIR`, `FLEZHA_AUDIT_DIR`, `FLEZHA_PLUGIN_ROOT`, `FLEZHA_LOG_PATH` | Every durable path defaults to `app_data/*` under the release root — ephemeral in a container and unwritable under a read-only root filesystem. Point each at a mounted volume owned by uid 1000. |
| `FLEZHA_HEALTHZ_ENABLED` | `/healthz` is off by default; an orchestrator probe needs it on (`FLEZHA_HEALTHZ_PORT` 9569, `FLEZHA_HEALTHZ_BIND` `0.0.0.0`). |
| `FLEZHA_CLUSTER_DISCOVERY` | `manual_discovery` by default; `k8s_pod_discovery` in Kubernetes. |
| `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` | Defaults to `60000`, but the orchestrator's stop grace period must be this plus 15 s (`stop_grace_period: 75s`, `terminationGracePeriodSeconds: 75` in the shipped files). Bump them in lockstep. See [`graceful_shutdown.md`](graceful_shutdown.md). |
| `FLEZHA_AUTHZ_ENABLED`, `FLEZHA_TRANSPORT_AUTH`, `FLEZHA_DIST_TLS_OPTFILE`, `FLEZHA_CA_CERT_PATH`, `FLEZHA_SERVER_CERT_PATH`, `FLEZHA_SERVER_KEY_PATH`, `FLEZHA_QUIC_CLIENT_CA` | Authz enforcement is on by default in prod and peer authentication is required by default; turning either off warns on every boot. The TLS material under the cert paths must persist across restarts. Provisioning the cookie, the TLS distribution option file, the QUIC certificates and the bootstrap claim: [`authn_authz.md`](authn_authz.md). |

## Bare metal: two ordinary processes

Neither the controller nor a runner needs configuration to find the other on
one machine, as the same user. Start the controller, then start the runner;
the controller prints the line that attaches one:

```
[flezha] runners attach with FLEZHA_SOCKET_FILES_PATH=/run/user/1000/flezha
```

Both sides resolve the socket directory from the same ladder —
`$FLEZHA_SOCKET_FILES_PATH`, then `$XDG_RUNTIME_DIR/flezha`, `/run/flezha`,
`app_data/sockets` — so two processes started from two different working
directories land on the same place. The controller creates the first candidate
it can; a runner attaches to the first holding a live control socket, and
refuses rather than guessing when two do. The full table, and why the default
moved off `app_data/`, is in
[`../configuration.md` § The socket-directory ladder](../configuration.md).

### Two users on one machine

Directory traversal is the UDS authentication boundary, so a runner run by a
different user cannot reach a controller's socket directory. Two arrangements
work:

- **Same user.** Run the runner as the controller's user. Nothing to configure.
- **A shared group.** Start the controller with
  `FLEZHA_SOCKET_DIR_GROUP=<group>`, a group both users belong to. The socket
  directory becomes `0750` owned by that group, and the controller logs a
  warning naming it on every boot. The boundary is then "that group" instead of
  "one user"; no setting makes the directory reachable by anyone else. A group
  the controller's user does not belong to refuses the boot naming the group.

Point both sides at an explicit `FLEZHA_SOCKET_FILES_PATH` in this arrangement:
`$XDG_RUNTIME_DIR` differs per user, so the zero-configuration path is
same-user only.

### When a runner cannot connect

`flz runner check` runs exactly the probe a runner runs — no runner code, no
running runner, no controller needed — and prints the resolved directory, the
candidates walked, the verdict and the fix. It exits zero only when a
controller answered. A runner prints the same diagnosis once, before it starts
retrying; the terse line that repeats every backoff is the retry notice, not
the diagnosis.

| Verdict | What it means | Fix |
|---|---|---|
| `socket directory absent` | no controller has ever used this path | start the controller, or set `FLEZHA_SOCKET_FILES_PATH` to the directory it established |
| `control socket absent` | the directory is there, the controller is not | start it, or wait for it to finish booting |
| `access denied` | the socket directory is the access boundary and this user is outside it — the diagnosis names the directory's owning uid and mode and the caller's uid | run the runner as the owning user, or use the group mode above |
| `stale control socket` | a socket file nobody is listening on: a controller that died hard | clears on the next controller start |
| `more than one controller is listening` | two ladder candidates hold a live socket and none was named | set `FLEZHA_SOCKET_FILES_PATH` to the one this runner should attach to |

### A remote runner over QUIC

`FLEZHA_QUIC_ADDRESSES` is a comma list; each entry is a name or a literal
address with an optional port (default `4433`). Every entry is resolved through
DNS on **every** connection attempt, so a controller that moves behind its name
is followed with no reconfiguration, and an entry that fails to resolve is one
failed candidate rather than a fatal error. A runner that has connected once
dials that address first on every later attempt — reconnecting to the same
controller is a binding swap, reconnecting elsewhere strands the previous
controller's operatives until its own convergence loop heals them — and orders
the remaining candidates randomly so a fleet-wide restart does not converge on
one node.

`FLEZHA_QUIC_ADDRESS` is retired and not aliased: set alone it refuses startup
naming the replacement. The controller's own listen address is
`FLEZHA_QUIC_LISTEN_ADDRESS`.

### Several controllers on one machine

A machine may host more than one controller. Every resource a controller owns
is already selected by a variable, so two instances coexist with no code change
and no filename suffix: give each its own values, and each owns a directory
tree and a node name that no other instance touches.

| Variable | What the instance owns |
|---|---|
| `FLEZHA_SOCKET_FILES_PATH` | The socket directory. Also the UDS access boundary, so separate directories give separate permission scopes. |
| `FLEZHA_STATE_DIR` | The per-Entity WAL, snapshot and outbox trees. Claimed exclusively at boot — see below. |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | The durable hub registry. `<root>/<hub_id>/registry.dets` is not keyed by node, so two instances over one root open one file. |
| `FLEZHA_DLQ_DIR` | The parked-record store. |
| `FLEZHA_NODE_NAME` (with `FLEZHA_NODE_HOST`) | The node name. Two BEAMs on one host cannot share one. |
| `FLEZHA_HEALTHZ_PORT` | The probe listener, when `FLEZHA_HEALTHZ_ENABLED` is on. |
| `FLEZHA_QUIC_LISTEN_ADDRESS` | The QUIC listener, when QUIC is enabled. |
| `FLEZHA_AUTHZ_DIR`, `FLEZHA_SCHEDULE_DIR`, `FLEZHA_AUDIT_DIR`, `FLEZHA_PLUGIN_ROOT`, `FLEZHA_LOG_PATH` | The remaining durable trees, where their subsystems are enabled. |

`flz reset` resolves its roots as `<app_data root>/<name>` and does not consult
these per-store overrides, so on a co-located controller it clears nothing that
controller owns. Clear a relocated tree by path, with the controller down.

The unit of separation is the **directory**, not a filename inside a shared
one. A suffix on `runner_control_channel.sock` would let two controllers share
a directory, and the directory is what carries the permission boundary
(`0700`, or `0750` for one named group): sharing it would put both instances
inside one scope and leave each controller's `rm_rf` reaching the other's
sockets. A runner also keeps naming a directory rather than learning an
instance identifier.

Two guards make a missed variable a refusal rather than damage:

- **A live peer's socket directory is refused.** Before preparing its
  directory a controller probes any control socket there — the same probe
  `flz runner check` runs. A peer that answers is live, so the controller
  refuses without removing anything, naming the path, the socket that answered
  and `FLEZHA_SOCKET_FILES_PATH`. A socket nobody answers is a controller that
  died hard, and is cleared as before, so a crash still needs no cleanup.
- **The state root is claimed exclusively.** A controller takes an advisory
  lock on `<state root>/.instance.lock` at boot and holds it for its lifetime.
  Two controllers over one state root are two unsynchronised writers on one
  entity WAL tree, which corrupts it silently; the refusal names the state
  root, the process holding it and `FLEZHA_STATE_DIR`. The kernel releases the
  lock when the holder exits, so a controller that was killed leaves nothing
  to clear.

#### A peer controller is capacity, not durability

A second controller buys capacity and isolation. It is **not** a failover
target, and adding one is not a substitute for the remote durability tier:

- The hub's redundancy strategy is `Singularity` — each operative runs on
  exactly one node, with no replicas — so no peer shadows another's
  operatives.
- Cross-controller state transfer (`Flezha.State.WalTransfer`) is
  destination-driven over `:erpc` against the source node, so a controller that
  died with its disk answers nothing and the adopting controller starts cold.

A controller that dies with its disk takes its operatives' state, and the
registry record of their existence, with it. The mechanism that survives that
is the remote durability tier: see
[`remote_durability_tier.md`](remote_durability_tier.md). An operator running
two controllers believing they cover each other is worse off than one running a
single controller with the tier on.

### When a controller refuses to start

| Refusal | What it means | Fix |
|---|---|---|
| `… is another controller's socket directory` | a live controller answered on the control socket in the directory this one was told to prepare | give this controller its own `FLEZHA_SOCKET_FILES_PATH`, or stop the controller listening there. Nothing was removed |
| `state root … is already claimed (held by pid N)` | another controller holds the advisory lock on that state root | give this controller its own `FLEZHA_STATE_DIR`, or stop pid N. If the holder is gone, the lock is gone with it — the message names a live process |
| `no socket directory could be created` | no ladder candidate was writable | set `FLEZHA_SOCKET_FILES_PATH` to a directory this user can write |
| `FLEZHA_SOCKET_DIR_GROUP=<group>: no such group` | the group does not exist, or this controller's user is not in it | use a group both users belong to, or drop the variable for owner-only |

## Images

| Tag | Built from | Purpose |
|---|---|---|
| `alfetahe/flezha:<version>` | `docker/prod/Dockerfile.controller` | Prod runtime, immutable. The version is `version:` in `mix.exs`. This is what a cluster pins. |
| `alfetahe/flezha:latest` | same build | Mutable convenience tag. Never pin a cluster to it. |
| `alfetahe/flezha:latest-dev` | `docker/dev/Dockerfile.runtime` | `FROM :latest` + Node.js + the pre-built testnet WS server, started only when `FLEZHA_TESTNET_WS_PORT` is set. |
| `alfetahe/flezha:latest-toolchain` | `docker/dev/Dockerfile.toolchain` | Elixir + Rust + Node + protoc shell over bind-mounted source, for hacking on Flezha itself. |

The prod Dockerfile has three stages:

```text
Stage 1  rust:1.87.0-slim-bookworm                   cargo build --release -p flz_nif -p flz
Stage 2  hexpm/elixir:1.17.3-erlang-27.2-…-slim      RUSTLER_SKIP_COMPILATION=true, mix release (ERTS included)
Stage 3  debian:bookworm-slim                        runtime: tini, libssl3, non-root flezha:flezha (uid/gid 1000)
```

The NIF is built once in Stage 1 and dropped into `priv/native/flz_nif.so`, so
Stage 2 never runs cargo. `bin/flzvm` is the release binary (renamed from
`bin/flezha` by `mix.exs`); `bin/flz` is the CLI, copied from
`target/release/flz` (`target/debug/flz` in a dev release).

### Runtime contract of the image

`ENTRYPOINT` is `tini`; `CMD` is `/usr/local/bin/controller-entrypoint.sh`,
which removes a stale control socket left on a reused volume, starts
`bin/flzvm start`, waits up to `FLEZHA_RESTORE_WAIT_TIMEOUT_S` (60) for
`runner_control_channel.sock` to appear, then waits on the BEAM. On SIGTERM or
SIGINT it runs `flz controller stop --timeout-ms $FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`
(quiesce, drain, finalise); when `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` is
`false`/`0`/`no`/`off` it instead runs `snapshot-on-stop.sh` and SIGTERMs the
BEAM. A `HEALTHCHECK` probes the control socket every 10 s after a 120 s start
period.

The entrypoint also carries a snapshot/restore branch (`FLEZHA_SNAPSHOT_*`,
`FLEZHA_RESTORE_WAIT_SOCKET_GLOB`, `FLEZHA_SNAPSHOT_PEER_TIMEOUT_MS`,
`FLEZHA_SNAPSHOT_STALENESS_THRESHOLD_S`) that invokes `flz cluster snapshot`
and `flz cluster restore`. The CLI implements neither verb (`flz cluster`
answers only `status`), so the branch cannot succeed: with graceful shutdown on
no snapshot is ever written, and a `latest.flz` found under
`FLEZHA_SNAPSHOT_DIR` (`/flezha/app_data/snapshots`) at boot makes the
container exit 1. Set `FLEZHA_SNAPSHOT_ENABLED=false` to skip the branch
entirely. Restart survival comes from the durable ProcessHub registry and
per-Entity WAL instead — see [`auto_restore_on_boot.md`](auto_restore_on_boot.md).

Attach to a running container through the release's own entry points:

```bash
docker exec <container>     bin/flzvm rpc 'IO.inspect(node())'
docker exec -it <container> bin/flzvm remote
docker exec <container>     bin/flz controller status
```

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

## Production image build

```bash
VERSION=0.2.0 make docker-build
# = docker build -t alfetahe/flezha:0.2.0 -f docker/prod/Dockerfile.controller .
```

`process_hub` is fetched from its GitHub `dev` branch at build time
(`mix deps.unlock process_hub` in Stage 2); a plain clone is all the build
context needs. Set `FLEZHA_PROCESS_HUB_PATH` at build time to build from a
local checkout instead (`mix.exs`).

### Publishing

Images are built natively on each architecture's own hardware — no buildx, no
QEMU. `scripts/publish-image.sh` (also `make docker-publish`, `make
docker-publish ARGS=--dev`) builds for the host CPU, pushes
`:<version>-<arch>` and `:latest-<arch>` (`:latest-dev-<arch>` with `--dev`),
then rebuilds the `:<version>`, `:latest` or `:latest-dev` manifest list from
whichever per-arch tags exist on Docker Hub, so an amd64 publish never
overwrites the arm64 variant. Run it once per architecture.

```bash
docker login -u alfetahe
scripts/publish-image.sh            # prod: :<version> + :latest
scripts/publish-image.sh --dev      # :latest-dev
NO_CACHE=1 scripts/publish-image.sh # clean rebuild after a base-image bump
REPUBLISH=1 scripts/publish-image.sh # overwrite an already-published version tag
```

A prod publish refuses to overwrite a version tag that already exists —
"changed the controller, forgot to bump `mix.exs`" fails in seconds. On every
controller release: bump `version:` in `mix.exs`, publish per architecture,
then bump `image:` and the `app.kubernetes.io/version` labels in
`kubernetes/flezha-daemonset.yaml`.

### CI size gate

`.github/workflows/docker.yml` runs on pull requests that touch
`docker/prod/**`, `docker/dev/Dockerfile.runtime`, `test/apps/testnet/**`,
`mix.exs`, `mix.lock`, `Cargo.toml`, `Cargo.lock`, `flz_nif/**`,
`.dockerignore` or the workflow itself. It builds `:latest` and `:latest-dev`,
prints both sizes to the step summary and fails if the prod image exceeds
300 MiB (`PROD_IMAGE_SIZE_LIMIT_BYTES=314572800`) or the runtime-dev image
exceeds 450 MiB (`RUNTIME_DEV_IMAGE_SIZE_LIMIT_BYTES=471859200`). It never
pushes.

### Export to a file

```bash
docker save -o flezha-0.2.0.tar alfetahe/flezha:0.2.0
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

## Kubernetes deployment

Manifests live in `kubernetes/`. The DaemonSet runs one controller pod per
node; `kubernetes/README.md` covers resources, the security profile and the
hostPath-versus-PVC trade-off.

| File | Resource |
|---|---|
| `flezha-namespace.yaml` | Namespace `flezha` |
| `flezha-serviceaccount.yaml` | ServiceAccount `flezha-reader` |
| `flezha-role.yaml` | Role `pod-reader` (`pods`: get, list, watch — pod discovery) |
| `flezha-rolebinding.yaml` | RoleBinding `read-pods` |
| `flezha-configmap.yaml` | ConfigMap `flezha-config` (non-secret env) |
| `flezha-daemonset.yaml` | DaemonSet `flezha-daemonset`, image `alfetahe/flezha:0.2.0`, `imagePullPolicy: IfNotPresent` |
| `service-template.yaml` | Reference Deployment for a runner service sharing the socket hostPath |
| `node-hardening.sh` | Run once per node as root: persistent journald (200 M cap) and `hung_task_panic` |

### Apply

```bash
make k8s-apply      # namespace, serviceaccount, role, rolebinding, configmap, daemonset — in that order
kubectl get pods -n flezha -l app=flezha
```

The image is public; `imagePullSecrets` is commented out in the DaemonSet.
For a private mirror create `dockerhub-secret` in the namespace
(`kubectl create secret docker-registry dockerhub-secret --docker-server=docker.io …`)
and uncomment the block.

### Update the image

Edit `image:` and the `app.kubernetes.io/version` labels in
`kubernetes/flezha-daemonset.yaml`, then `kubectl apply -f kubernetes/flezha-daemonset.yaml`.
Never point the manifest at `:latest` — with `IfNotPresent` a mutable tag
leaves nodes running whatever they cached.

### ConfigMap

The DaemonSet loads `flezha-config` via `envFrom`, then sets per-pod values:
`FLEZHA_NODE_HOST` (`status.podIP`), `FLEZHA_NODE_NAME` (`flezha`),
`FLEZHA_K8S_NAMESPACE`, `FLEZHA_K8S_SERVICE_NAME` (the `app` label),
`FLEZHA_K8S_SELECTOR` (`app=flezha`) and `FLEZHA_SOCKET_FILES_PATH`
(`/flezha/sockets`). The `app: flezha` pod label is load-bearing for discovery.

| ConfigMap key | Value |
|---|---|
| `FLEZHA_TRANSPORT` | `uds` |
| `FLEZHA_CLUSTER_DISCOVERY` | `k8s_pod_discovery` |
| `FLEZHA_K8S_IP_LOOKUP_MODE` / `FLEZHA_K8S_MASTER_NAME` / `FLEZHA_K8S_MASTER_PORT` | `pods` / `kubernetes.default.svc` / `443` |
| `FLEZHA_RECONNECT_INTERVAL` / `FLEZHA_MAX_RECONNECTS` / `FLEZHA_RUNNER_KEEPALIVE` | `2000` / `10` / `30000` |
| `FLEZHA_QUIC_LISTEN_ADDRESS` | `0.0.0.0:4433` |
| `FLEZHA_CA_CERT_PATH` / `FLEZHA_SERVER_CERT_PATH` / `FLEZHA_SERVER_KEY_PATH` | `/flezha/app_data/certs/{ca.pem,server.pem,server.key}` — persisted on the `flezha-certs` hostPath |
| `FLEZHA_LOG_PATH` | `/var/log/flezha/flezha.log` |
| `FLEZHA_AUTO_RESTORE_ENABLED` / `FLEZHA_BOOT_REPLAY_TIMEOUT_MS` | `true` / `60000` |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` / `FLEZHA_STATE_DIR` / `FLEZHA_DLQ_DIR` / `FLEZHA_SNAPSHOT_DIR` | `/var/lib/flezha/{process_hub,state,dlq,snapshots}` — on the `flezha-recovery` hostPath |
| `FLEZHA_HEALTHZ_ENABLED` / `FLEZHA_HEALTHZ_PORT` / `FLEZHA_HEALTHZ_BIND` | `true` / `9569` / `0.0.0.0` (the kubelet probes the pod IP, not loopback) |
| `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` | `60000`, paired with `terminationGracePeriodSeconds: 75` |

Not in the shipped ConfigMap and needed for a prod pod: `FLEZHA_DURABILITY_BACKEND`
(the pod does not boot without it), `RELEASE_COOKIE` (from a Secret), and
`FLEZHA_AUTHZ_DIR`, `FLEZHA_SCHEDULE_DIR`, `FLEZHA_AUDIT_DIR`,
`FLEZHA_PLUGIN_ROOT` (their defaults sit under the read-only `/flezha`; point
them under `/var/lib/flezha`). To enable the remote durability tier add the
backend family to the ConfigMap; the `_PASSWORD_ENV` / `_SECRET_ENV`
indirection names a Secret-sourced variable rather than carrying the secret.

After editing the ConfigMap:

```bash
kubectl apply -f kubernetes/flezha-configmap.yaml
kubectl rollout restart daemonset/flezha-daemonset -n flezha
```

### Volumes (per-node persistence)

| Volume | Host path | Mount path | Holds |
|---|---|---|---|
| `flezha-sockets` | `/opt/flezha-sockets` | `/flezha/sockets` | UDS socket files, shared with runner pods |
| `flezha-data` | `/opt/flezha-data` | `/flezha/data` | spare data directory |
| `flezha-certs` | `/opt/flezha-certs` | `/flezha/app_data/certs` | TLS material, generated on first boot and reused |
| `flezha-logs` | `/opt/flezha-logs` | `/var/log/flezha` | log file |
| `flezha-recovery` | `/opt/flezha-recovery` | `/var/lib/flezha` | ProcessHub registry, per-Entity WAL and snapshots, DLQ — losing it reverts the node to an empty recovery |
| `release-tmp`, `tmp` | emptyDir | `/flezha/tmp`, `/tmp` | the two non-persistent paths the BEAM writes under a read-only root (`RELEASE_TMP` holds the merged runtime config) |

All five hostPaths are `DirectoryOrCreate`. Because hostPath ignores
`fsGroup`, a root `fix-permissions` init container chowns them to 1000:1000;
swapping them for an fsGroup-honouring volume (a PVC) removes that container
and makes the pod admissible under the `restricted` Pod Security Standard. The
pod itself runs as uid/gid 1000, `readOnlyRootFilesystem: true`, all
capabilities dropped, seccomp `RuntimeDefault`.

### Ports and probes

The container exposes `epmd` 4369/TCP, `quic` 4433/UDP and `healthz`
9569/TCP. Startup (period 10 s, 30 failures), liveness and readiness (period
10 s, timeout 5 s, 3 failures) probes all `GET /healthz`; the endpoint answers
503 only on a genuine wedge, and a planned recovery reports `recovering` with
200 so the kubelet does not restart mid-replay. `preStop` runs
`/flezha/bin/flz controller stop --timeout-ms $(FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS)`
before SIGTERM. Exposing QUIC outside the node needs a Service with
`protocol: UDP` (a UDP-capable load balancer or a NodePort).

### Status and logs

```bash
kubectl get pods -n flezha -l app=flezha
kubectl describe pod -n flezha <pod-name>
kubectl logs -f -n flezha -l app=flezha --all-containers=true
kubectl exec -it -n flezha <pod-name> -- tail -f /var/log/flezha/flezha.log
sudo tail -f /opt/flezha-logs/flezha.log        # on the node
```

### Delete

```bash
make k8s-delete                    # DaemonSet and ConfigMap only
kubectl delete namespace flezha    # everything, including RBAC
```

## Makefile reference

| Target | Does |
|---|---|
| `build` | `cargo build --workspace`, `mix deps.get`, `mix compile` |
| `build-release` | `cargo build --release -p flz_nif -p flz`, then `MIX_ENV=prod mix deps.get --only prod && mix release --overwrite` |
| `nif` / `nif-release` | Build only the `flz_nif` crate (debug / release) |
| `test` | `mix test` |
| `stabilize-test-check` | `mix test` twice, `cargo test --workspace --features flezha_runner/bench-internals`, `mvn -f sdk/java verify` (`SKIP_JAVA=1` skips the last) |
| `refresh-process-hub` | `mix deps.unlock process_hub && mix deps.get` — move to the branch tip while `mix.exs` tracks `process_hub`'s `dev` branch |
| `clean` | `cargo clean`, `mix clean`, remove `_build`, `deps`, `priv/native/*.so` |
| `docker-build` | Prod image for the host architecture; requires `VERSION=x.y.z` |
| `docker-publish` | `scripts/publish-image.sh $(ARGS)` — native per-arch build, push, manifest rebuild |
| `docker-build-dev` | `alfetahe/flezha:latest-dev` from `docker/dev/Dockerfile.runtime` (needs `:latest` in the local cache) |
| `docker-build-toolchain` | `alfetahe/flezha:latest-toolchain` from `docker/dev/Dockerfile.toolchain` |
| `k8s-apply` | Apply the six manifests in dependency order |
| `k8s-delete` | Delete the DaemonSet and ConfigMap (`--ignore-not-found`) |

## See also

- [`../configuration.md`](../configuration.md) — every knob, its default and the file that reads it
- [`authn_authz.md`](authn_authz.md) — cookie, TLS distribution, QUIC certificates, bootstrap claim
- [`graceful_shutdown.md`](graceful_shutdown.md) — what `flz controller stop` does inside the grace period
- [`auto_restore_on_boot.md`](auto_restore_on_boot.md) — what a restarted controller brings back from the recovery volume
- [`remote_durability_tier.md`](remote_durability_tier.md) — choosing and configuring a durability backend
- [`reset.md`](reset.md) — wiping a node's runtime data
