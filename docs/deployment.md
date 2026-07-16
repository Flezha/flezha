# Flezha Deployment Guide

This document covers the full deployment lifecycle: local dev, production image builds, and Kubernetes.

All commands are run from the **project root** unless stated otherwise.

---

## Table of Contents

1. [Environment variables](#1-environment-variables)
2. [Local development (Docker)](#2-local-development-docker)
3. [Production image build](#3-production-image-build)
4. [Local production smoke-test](#4-local-production-smoke-test)
5. [Kubernetes deployment](#5-kubernetes-deployment)
6. [Makefile reference](#6-makefile-reference)

---

## 1. Environment variables

Copy the example file before first use:

```bash
cp .env.example .env
```

Key variables:

| Variable | Default | Description |
|---|---|---|
| `FLEZHA_TRANSPORT` | `uds` | Default transport layer: `uds` or `quic` |
| `FLEZHA_TRANSPORTS` | (derived) | Comma-separated list of transports to enable simultaneously (e.g. `uds,quic`). Defaults to the value of `FLEZHA_TRANSPORT`. In test mode defaults to `uds,quic`. |
| `FLEZHA_SOCKET_FILES_PATH` | `/flezha/socket_data/` | UDS socket directory |
| `FLEZHA_QUIC_ADDRESS` | `0.0.0.0:4433` | QUIC bind address |
| `FLEZHA_CA_CERT_PATH` | `app_data/certs/ca.pem` | TLS CA certificate (auto-generated if absent) |
| `FLEZHA_SERVER_CERT_PATH` | `app_data/certs/server.pem` | TLS server certificate |
| `FLEZHA_SERVER_KEY_PATH` | `app_data/certs/server.key` | TLS server private key |
| `FLEZHA_LOG_PATH` | `logs/flezha.log` | Log file path |
| `FLEZHA_LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warning`, `error` |
| `FLEZHA_LOG_MAX_BYTES` | `10485760` | Max log file size before rotation (bytes) |
| `FLEZHA_LOG_KEEP_FILES` | `5` | Number of rotated log files to keep |
| `FLEZHA_CLUSTER_DISCOVERY` | — | Discovery strategy (`k8s_pod_discovery` in K8s) |
| `FLEZHA_RECONNECT_INTERVAL` | `2000` | Reconnect interval in ms |
| `FLEZHA_MAX_RECONNECTS` | `10` | Max reconnect attempts |
| `RUSTLER_SKIP_COMPILATION` | — | Set to `true` to skip NIF cargo build (used in prod image Stage 2) |

### Graceful-restart entrypoint (image-level)

The published `:latest` and `:latest-dev` images replace the historical `bin/flzvm start` CMD with `/usr/local/bin/controller-entrypoint.sh`. The script wraps the controller in a snapshot/restore lifecycle. All knobs are configured by env vars; defaults are tuned for the upstream `docker/prod/docker-compose.yml` volume layout.

| Variable | Default | Description |
|---|---|---|
| `FLEZHA_SNAPSHOT_ENABLED` | `true` | Master toggle. Set to `false`/`0`/`no`/`off` to disable both pre-stop snapshot and on-boot restore. |
| `FLEZHA_SNAPSHOT_DIR` | `/flezha/app_data/snapshots` | Directory holding `latest.flz`. |
| `FLEZHA_SNAPSHOT_PEER_TIMEOUT_MS` | `5000` | Per-peer RPC timeout passed to `flz cluster snapshot/restore`. |
| `FLEZHA_SNAPSHOT_STALENESS_THRESHOLD_S` | `86400` | Restore logs WARN if the snapshot is older than this. |
| `FLEZHA_RESTORE_WAIT_TIMEOUT_S` | `60` | Wait window for `FLEZHA_RESTORE_WAIT_SOCKET_GLOB` before giving up. |
| `FLEZHA_RESTORE_WAIT_SOCKET_GLOB` | (empty) | Glob inside `FLEZHA_SOCKET_FILES_PATH`. When set, the entrypoint waits for at least one matching SDK socket before invoking `flz cluster restore`. Empty = restore immediately. |
| `FLEZHA_TESTNET_WS_PORT` | (unset) | When set on `:latest-dev`, the entrypoint also launches the pre-built testnet WS server on this port. No-op on `:latest`. |

### Published image taxonomy

| Tag                                | Purpose                                                     |
|------------------------------------|-------------------------------------------------------------|
| `alfetahe/flezha:latest`           | Prod runtime — Mix release + graceful-restart entrypoint    |
| `alfetahe/flezha:latest-dev`       | Runtime + Node.js + pre-built testnet WS server             |
| `alfetahe/flezha:latest-toolchain` | Dev toolchain (Elixir + Rust + Node + protoc, source-mounted) |

---

## 2. Local development (Docker)

The dev environment mounts the source tree into the container so source changes are visible immediately without rebuilding the image. Named volumes (`flezha_build`, `flezha_deps`, `flezha_cargo_target`) cache build artifacts across restarts.

### Quick start

```bash
# One-time: build image and start all services
./scripts/dev.sh build
./scripts/dev.sh up

# Open an IEX session (controller starts, Transport.Server comes up)
./scripts/dev.sh iex

# Or open a plain shell
./scripts/dev.sh shell
```

### Inside the container — first-time setup

After opening a shell, compile Rust and Elixir:

```bash
# Compile entire Rust workspace (NIF, CLI, SDK, test runner)
cargo build --workspace

# Compile Elixir (Rustler reuses the already-built NIF)
mix deps.get && mix compile
```

### Running the controller

```bash
# Option A — IEX (no CLI commands)
iex --name flezha@127.0.0.1 --cookie flezha_dev_cookie -S mix

# Option B — Elixir release with IEX
mix release --overwrite
_build/dev/rel/flezha/bin/flzvm start
```

`bin/flzvm` is the Elixir release binary (renamed from `bin/flezha` by `mix.exs`).
`bin/flz` is the CLI binary (copied from `cli/target/debug/flz`).

### Running a runner (separate shell)

```bash
./scripts/dev.sh shell   # second terminal
cd test/apps/rust_runner
cargo run --target-dir=./target
```

### Useful dev.sh commands

```bash
./scripts/dev.sh build          # build image
./scripts/dev.sh up             # start containers
./scripts/dev.sh down           # stop containers
./scripts/dev.sh shell          # bash inside controller container
./scripts/dev.sh iex            # IEX session inside container
./scripts/dev.sh logs           # follow controller logs
./scripts/dev.sh status         # show container status
./scripts/dev.sh rebuild        # full clean rebuild (down + clear volumes + build + up)
./scripts/dev.sh release        # run mix release inside container
./scripts/dev.sh cargo build --workspace   # arbitrary cargo commands inside container
./scripts/dev.sh clean          # remove named build volumes
```

### Kafka UI

Available at `http://localhost:8080/` while the dev environment is running.

---

## 3. Production image build

The prod Dockerfile (`docker/prod/Dockerfile.controller`) uses three stages:

```
Stage 1  rust:1.87.0        — cargo build --release → libflz_nif.so + flz (CLI)
Stage 2  elixir:1.17-otp-27 — RUSTLER_SKIP_COMPILATION=true, mix release
Stage 3  elixir:1.17-otp-27 — runtime, non-root user flezha:flezha (uid 1000)
```

The NIF `.so` is compiled once in Stage 1 and copied directly into Stage 2's `priv/native/`. This avoids installing Rust in the Elixir build stage and keeps the final image lean.

### Build for current architecture

```bash
VERSION=0.2.0 make docker-build
# equivalent to:
docker build -t alfetahe/flezha:0.2.0 -f docker/prod/Dockerfile.controller .
```

### Build multi-arch and push to Docker Hub

Requires [Docker Buildx](https://docs.docker.com/buildx/working-with-buildx/).

```bash
# One-time: create a buildx builder
docker buildx create --name multiarch --use
docker buildx inspect --bootstrap

# Build for amd64 + arm64 and push
VERSION=0.2.0 make docker-build-multiarch

# Or manually with extra tags
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f docker/prod/Dockerfile.controller \
  -t alfetahe/flezha:0.2.0 \
  -t alfetahe/flezha:latest \
  --push .
```

### Export image to file

```bash
docker save -o flezha-0.2.0.tar alfetahe/flezha:0.2.0
```

---

## 4. Local production smoke-test

Use the prod docker-compose to test the built image locally without K8s:

```bash
# Build the image first
VERSION=0.2.0 make docker-build

# Start prod stack (controller + kafka + zookeeper)
VERSION=0.2.0 docker compose -f docker/prod/docker-compose.yml up -d

# Check controller logs
docker logs -f flezha_agent_prod

# Stop
docker compose -f docker/prod/docker-compose.yml down
```

The controller writes to `/flezha/app_data/logs/flezha.log` inside the container, which maps to the `flezha_logs` named volume.

### Run with QUIC transport

```bash
FLEZHA_TRANSPORT=quic VERSION=0.2.0 \
  docker compose -f docker/prod/docker-compose.yml up -d
```

---

## 5. Kubernetes deployment

All manifests live in `kubernetes/`. The DaemonSet runs one controller pod per node.

### Prerequisites

- A running K8s cluster (minikube, GKE, EKS, etc.)
- `kubectl` configured with cluster access
- A Docker Hub pull secret if using a private image

### Image pull secret (if needed)

```bash
kubectl create secret docker-registry dockerhub-secret \
  --docker-server=docker.io \
  --docker-username=<your-username> \
  --docker-password=<your-password> \
  -n flezha
```

The DaemonSet already references `dockerhub-secret` via `imagePullSecrets`.

### Apply all resources

```bash
make k8s-apply
# equivalent to applying, in order:
#   flezha-namespace.yaml
#   flezha-serviceaccount.yaml
#   flezha-role.yaml
#   flezha-rolebinding.yaml
#   flezha-configmap.yaml
#   flezha-daemonset.yaml
```

### Update the image version

Edit `kubernetes/flezha-daemonset.yaml` and change the image tag, then re-apply:

```yaml
image: alfetahe/flezha:0.2.0   # ← update this
```

```bash
kubectl apply -f kubernetes/flezha-daemonset.yaml
```

### ConfigMap

`kubernetes/flezha-configmap.yaml` holds all non-secret env vars. The DaemonSet loads them via `envFrom: configMapRef`. Edit the ConfigMap to change transport, cert paths, or cluster discovery settings, then apply:

```bash
kubectl apply -f kubernetes/flezha-configmap.yaml
# Restart pods to pick up changes
kubectl rollout restart daemonset/flezha-daemonset -n flezha
```

Key ConfigMap values:

| Key | Default | Notes |
|---|---|---|
| `FLEZHA_TRANSPORT` | `uds` | Default transport for auto-started runners |
| `FLEZHA_TRANSPORTS` | (derived) | Comma-separated list to enable multiple transports simultaneously (e.g. `uds,quic`) |
| `FLEZHA_QUIC_ADDRESS` | `0.0.0.0:4433` | QUIC listen address (UDP port 4433 exposed) |
| `FLEZHA_CA_CERT_PATH` | `/flezha/app_data/certs/ca.pem` | Auto-generated on first run |
| `FLEZHA_SERVER_CERT_PATH` | `/flezha/app_data/certs/server.pem` | Auto-generated on first run |
| `FLEZHA_SERVER_KEY_PATH` | `/flezha/app_data/certs/server.key` | Auto-generated on first run |
| `FLEZHA_CLUSTER_DISCOVERY` | `k8s_pod_discovery` | K8s pod-based cluster formation |

### Host paths (per-node persistence)

| Volume | Host path | Mount path | Purpose |
|---|---|---|---|
| `flezha-sockets` | `/opt/flezha-sockets` | `/flezha/sockets` | UDS socket files |
| `flezha-data` | `/opt/flezha-data` | `/flezha/data` | Controller state |
| `flezha-certs` | `/opt/flezha-certs` | `/flezha/app_data/certs` | TLS certs (auto-generated, persisted) |
| `flezha-logs` | `/opt/flezha-logs` | `/var/log/flezha` | Log files |

Certs are generated automatically on first startup and persist on the host node. Subsequent pod restarts reuse the same certs without regenerating them.

### QUIC port

When QUIC is enabled (via `FLEZHA_TRANSPORT=quic` or `FLEZHA_TRANSPORTS=uds,quic`), the controller listens on UDP port 4433. The DaemonSet exposes this port:

```yaml
ports:
  - name: quic
    containerPort: 4433
    protocol: UDP
```

To expose QUIC externally you will need a K8s Service with `protocol: UDP` and a cloud load balancer that supports UDP, or NodePort.

### Check pod status

```bash
kubectl get pods -n flezha -l app=flezha
kubectl describe pod -n flezha <pod-name>
```

### Logs

```bash
# Stream logs from all controller pods
kubectl logs -f -n flezha -l app=flezha --all-containers=true

# Stream logs from a specific pod
kubectl logs -f -n flezha <pod-name>

# Read the persistent log file from inside a pod
kubectl exec -it -n flezha <pod-name> -- tail -f /var/log/flezha/flezha.log

# Read directly on the node
sudo tail -f /opt/flezha-logs/flezha.log
```

### Delete all resources

```bash
make k8s-delete
# Deletes DaemonSet and ConfigMap (namespace and RBAC left intact)
```

To remove everything including the namespace:

```bash
kubectl delete namespace flezha
```

---

## 6. Makefile reference

Run `make <target>` from the project root.

| Target | Description |
|---|---|
| `build` | Compile Rust workspace (debug) + `mix deps.get && mix compile` |
| `build-release` | Compile NIF + CLI (release) + `MIX_ENV=prod mix release` |
| `nif` | Compile only the NIF crate (debug) |
| `nif-release` | Compile only the NIF crate (release) |
| `test` | Run `mix test` |
| `clean` | Remove `_build`, `deps`, `target`, `priv/native/*.so` |
| `docker-build` | Build prod image for current arch — requires `VERSION=x.y.z` |
| `docker-build-multiarch` | Build + push multi-arch image — requires `VERSION=x.y.z` |
| `k8s-apply` | Apply all K8s manifests in dependency order |
| `k8s-delete` | Delete DaemonSet and ConfigMap |

Examples:

```bash
make nif                          # fast NIF rebuild during development
make build-release                # full prod build locally
VERSION=0.2.0 make docker-build   # build prod image tagged 0.2.0
make k8s-apply                    # deploy to cluster
```
