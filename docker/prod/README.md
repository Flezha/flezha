# Flezha Agent - Production Docker Build

Run the following commands from the **flezha project root** directory.

## Build Local Image (Single Architecture)

```bash
# Build and push on amd64
docker build -f docker/prod/Dockerfile.agent -t alfetahe/flezha:latest-amd64 .
docker push alfetahe/flezha:latest-amd64

# Build and push on arm64
docker build -f docker/prod/Dockerfile.agent -t alfetahe/flezha:latest-arm64 .
docker push alfetahe/flezha:latest-arm64

# Push to the latest
docker buildx imagetools create -t alfetahe/flezha:latest \
  alfetahe/flezha:latest-amd64 alfetahe/flezha:latest-arm64

# Build and push for current architecture.
docker build -f docker/prod/Dockerfile.agent -t alfetahe/flezha:latest .
docker push alfetahe/flezha:latest
```

## Build Multi-Architecture Image and Push to Docker Hub

The result is a single manifest list under one tag (e.g. `latest`) that serves both
`linux/amd64` and `linux/arm64`. Docker automatically pulls the correct variant per host.

### Option A — Single machine (QEMU emulation)

Simplest setup. The non-native platform (usually arm64) is emulated and compiles slowly

```bash
# Setup buildx (one-time)
docker buildx create --name multiarch --use
docker buildx inspect --bootstrap

# Build and push with both version and latest tags
docker buildx build --platform linux/amd64,linux/arm64 \
  -f docker/prod/Dockerfile.agent \
  -t alfetahe/flezha:0.0.1 \
  -t alfetahe/flezha:latest \
  . --push
```

### Option B — Two native machines (faster, recommended)

Build each architecture on its own machine, push them as separate tags, then combine
them into one manifest list. All commands use standard `docker` — no SSH magic required.

**On your amd64 machine:**

```bash
docker build -f docker/prod/Dockerfile.agent \
  -t alfetahe/flezha:amd64 .
docker push alfetahe/flezha:amd64
```

**On your arm64 machine (SSH in normally, then run):**

```bash
docker build -f docker/prod/Dockerfile.agent \
  -t alfetahe/flezha:arm64 .
docker push alfetahe/flezha:arm64
```

**Back on either machine — combine into one manifest and push:**

```bash
docker manifest create alfetahe/flezha:latest \
  alfetahe/flezha:amd64 \
  alfetahe/flezha:arm64

docker manifest push alfetahe/flezha:latest

# Optionally tag a version as well
docker manifest create alfetahe/flezha:0.0.1 \
  alfetahe/flezha:amd64 \
  alfetahe/flezha:arm64

docker manifest push alfetahe/flezha:0.0.1
```

Both options produce the same single `latest` tag on Docker Hub — a manifest list
containing one image digest per architecture.

## Run Docker Image

```bash
docker run -d \
  --name flezha_container \
  -e FLEZHA_MAX_RECONNECTS=3 \
  alfetahe/flezha:latest
```

## Export Docker Image

```bash
# Export latest
docker save -o flezha-latest.tar alfetahe/flezha:latest

# Export specific version
docker save -o flezha-0.0.1.tar alfetahe/flezha:0.0.1
```

## Docker Compose Deployment

```bash
# Start production containers
docker compose -f docker/prod/docker-compose.yml up -d
```

