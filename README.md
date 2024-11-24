# Aflecha

## Description
Distributed stateful computations. Provdes a static and dynamic creation of computations.

## Examples

### Start agent
Starting the node and specifying the external processes to be used by the node mynode.
```bash
RELEASE_NODE=mynode AFLECHA_EXTERNAL_PROCESSES="cargo run --manifest-path=/home/anuar/Projects/personal/aflecha/test/apps/rust_app/Cargo.toml --target-dir=./target" _build/dev/rel/aflecha/bin/agent start
```

### Stop agent
Stopping the node.
```bash
_build/dev/rel/aflecha/bin/agent stop
```

Adding a computation to the node.
```bash
_build/dev/rel/aflecha/bin/computation add my_computation compare_filter --variables='{"comparable": "hello world", "inverted": false}'
```

Removing a computation from the node.
```bash
_build/dev/rel/aflecha/bin/computation rem my_computation
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