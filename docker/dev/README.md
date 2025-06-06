# Docker DEV

Run the following commands from the project *root* directory.

# TODO: update names
## Build docker image
```bash
docker build -t flezha:latest -f docker/dev/Dockerfile.agent .
```

## Build docker image for multiple platforms and push to docker hub
```bash
docker buildx build --platform linux/amd64,linux/arm64 -t alfetahe/flezha:0.1.4 . --push
```

## Run docker image
```bash
docker run -d \
  --name flezha_container \
  -e AFLECHA_MAX_RECONNECTS=3 \
  alfetahe/flezha:0.1.4  
```

## Export docker image
```bash
docker save -o flezha.tar alfetahe/flezha:0.1.4
```

## Start the containers:
```bash
docker compose -f docker/dev/docker-compose.yml up
```

# Kafka UI:
Kafka UI can be seen on: `http://localhost:8080/`
