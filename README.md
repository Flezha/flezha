# Aflecha

## Description
Distributed stateful computations. Provdes a static and dynamic creation of computations.

## Example usage
Starting the node and specifying the external processes to be used by the node mynode.
```bash
RELEASE_NODE=mynode AFLECHA_EXTERNAL_PROCESSES="cargo run --manifest-path=/home/anuar/Projects/personal/aflecha/test/apps/rust_app/Cargo.toml --target-dir=./target" aflecha/bin/aflecha start_iex
```

Adding a computation to the node.
```bash
aflecha/bin/add_computation comp_id increment_state -v '{\"value\": 500}' -n "mynode"
```

Removing a computation from the node.
```bash
aflecha/bin/rem_computation comp_id -n "mynode"
```

## Build docker image for multiple platforms and push to docker hub
```bash
docker buildx build --platform linux/amd64,linux/arm64 -t alfetahe/aflecha:0.1.4 . --push
```

## Run docker image
```bash
docker run -d \
  --name aflecha_container \
  -e AFLECHA_MAX_RECONNECTS=3 \
  alfetahe/aflecha:0.1.4
```

## Export docker image
```bash
docker save -o aflecha.tar alfetahe/aflecha:0.1.4
```