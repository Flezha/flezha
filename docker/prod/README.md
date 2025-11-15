# Flezha Agent - Production Docker Build

Run the following commands from the **flezha project root** directory.

## Build Local Image (Single Architecture)

```bash
docker build -t flezha:latest -f docker/prod/Dockerfile.agent .
```

## Build Multi-Architecture Image and Push to Docker Hub

**Recommended for production deployment (supports both amd64 and arm64 platforms):**

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

