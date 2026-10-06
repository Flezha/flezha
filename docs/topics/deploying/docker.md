# Docker

The image `flezha/flezha` holds the same release as the binaries, on Debian, and runs it
as the user `flezha` (uid 1000). [Deploying a controller](deploying_a_controller.md#ways-to-run-it)
has the commands to pull it, run it and build it. This page covers what the container does
when it starts and stops, what to mount, and a controller with its runner in one Compose
file.

**Contents**

1. [What the container runs](#what-the-container-runs)
2. [What to mount](#what-to-mount)
3. [A controller and a runner in Compose](#a-controller-and-a-runner-in-compose)
4. [flz in the container](#flz-in-the-container)
5. [Tune the VM](#tune-the-vm)
6. [Plugins](#plugins)
7. [Limits and gotchas](#limits-and-gotchas)
8. [See also](#see-also)

## What the container runs

`tini` is the first process and passes signals on to the entrypoint script. On start the
script:

1. makes the plugin root hold exactly the packages `FLEZHA_PLUGINS` declares, when it is
   set. A failed sync exits 1 and starts nothing.
2. starts the controller with `bin/flzvm start`.
3. waits for the control socket, then for `flz boot await` to report ready. Both waits share
   `FLEZHA_RESTORE_WAIT_TIMEOUT_S`, 60 seconds by default. When it runs out, the script
   exits 1 and the container stops, for its restart policy to start it again.

On SIGTERM, which `docker stop` and `docker compose stop` send, the script runs
`flz controller stop --timeout-ms ${FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS}`: the drain
[Graceful shutdown](deploying_a_controller.md#graceful-shutdown) describes. Docker kills a
container 10 seconds after SIGTERM unless told otherwise, so give it the paired grace
period, 75 seconds with the default timeout:

```sh
$ docker stop -t 75 flezha-controller
```

In a Compose file the same pair is `stop_grace_period: 75s`.

The healthcheck runs `flz boot await --timeout 0` every 10 seconds. Failures in the first
120 seconds do not count, so a long replay is not reported unhealthy, and ten failures in
a row after that are. Plain Docker only reports the status; it does not restart an
unhealthy container. A Compose service waits for it with
`depends_on: { controller: { condition: service_healthy } }`.

## What to mount

One volume at `/flezha/app_data` holds every durable tree the controller owns. A volume
per tree works too, but then every tree needs one: a tree left without a volume is written
into the container and removed with it. The socket directory gets a volume of its own
when a runner container shares it
([Connecting a runner](connecting_a_runner.md#the-socket-directory)).

The image creates `/flezha/app_data` and the directories under it owned by uid 1000, and a
new named volume takes over that owner. Two cases do not:

- **A named volume over a path the image does not have** is created owned by root, and
  the controller cannot write to it. Mount under the directories the image creates, or
  set the matching `FLEZHA_*_DIR` variable to a path inside one.
- **A bind mount** keeps the host directory's owner, which has to be uid 1000 or a group
  that uid belongs to.

With `/flezha/app_data` on a volume, the container also runs with a read-only root
filesystem (`--read-only`, or `read_only: true` in Compose).

## A controller and a runner in Compose

The agentdesk example runs one controller and one runner, each from its own image:

```yaml
# agentdesk in Docker Compose: one controller and one runner.
services:
  controller:
    build: controller
    environment:
      FLEZHA_AUTHZ_ENABLED: "false"
      FLEZHA_DURABILITY_BACKEND: none
    ports:
      - "127.0.0.1:8080:8080"              # the gateway's HTTP port, on this machine only
    volumes:
      - app_data:/flezha/app_data
      - sockets:/flezha/app_data/sockets

  runner:
    build: .
    environment:
      FLEZHA_SOCKET_FILES_PATH: /flezha/app_data/sockets
    volumes:
      - sockets:/flezha/app_data/sockets
    depends_on:
      controller:
        condition: service_healthy
    restart: on-failure

volumes:
  app_data:
  sockets:
```

[Full file](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/compose.yaml)

- **`build: controller`** is an image derived from `flezha/flezha` that copies the
  example's plugin packages in
  ([Installing plugins](../plugins/installing_plugins.md#example)).
- **The `sockets` volume** is mounted in both containers, and the runner is told the path
  with `FLEZHA_SOCKET_FILES_PATH`. The runner image runs as uid 1000 too, so it can enter
  the directory.
- **The runner waits for the controller's healthcheck,** and `restart: on-failure` starts
  it again when it exits, as a runner the controller tells to leave does
  ([Run it as a service](connecting_a_runner.md#run-it-as-a-service)). A Node, Python,
  Ruby or PHP runner also needs `stdin_open: true`.
- **Authorization off and no durability backend** suit the example only.
  [Variables a deployment sets](deploying_a_controller.md#variables-a-deployment-sets) has
  what a real deployment sets instead.

For a Ruby runner, `docker/prod/Dockerfile.ruby-runner` is a reference image: it preloads
jemalloc, which keeps a long-running Ruby process from growing through fragmentation.

## flz in the container

The image carries the CLI. Run it with `docker exec`; it reads the controller's own cookie
and state directory, and the container's environment, so it needs nothing more:

```sh
$ docker exec flezha-controller bin/flz boot status
```

```output
readiness: ready
  met    boot_sequence_complete
  met    hubs_recovered
  met    durable_registry_loaded
  met    declared_hubs_started
```

Under Compose it is `docker compose exec controller bin/flz`, and an alias saves typing it:

```sh
$ alias flz='docker compose exec controller bin/flz'
```

A shell on the running node, for inspecting it:

```sh
$ docker exec -it flezha-controller bin/flzvm remote
```

## Tune the VM

The release's VM flags are in the image. `ERL_FLAGS` overrides them without a rebuild:

```sh
$ docker run -d --name flezha-controller \
  -e ERL_FLAGS="+S 4:4" \
  -v flezha_data:/flezha/app_data \
  flezha/flezha:0.1.0
```

A flag the VM does not accept stops the boot, so the container exits instead of running
with the flag ignored. To replace the file as a whole, mount one and point
`RELEASE_VM_ARGS` at it. [Tune the VM](deploying_a_controller.md#tune-the-vm) has when a
flag is worth setting, and [VM arguments](../../configuration/vm_arguments.md) the full
list.

## Plugins

A container gets its plugin packages in one of three ways: installed into a volume, listed
in `FLEZHA_PLUGINS`, or copied into a derived image.
[Where the packages come from](../plugins/installing_plugins.md#where-the-packages-come-from)
has the commands, and [Build the package](../plugins/writing_a_plugin.md#build-the-package)
the builder image a package for this image is built in.

## Limits and gotchas

- **Pin the version.** `latest` moves, so two hosts that pulled it at different times run
  different controllers.
- **`docker/prod/docker-compose.yml` is a smoke test,** not a template: it starts Kafka
  beside the controller and turns the durability backend off.
- **A container is one machine's worth of state.** Without a durability backend, losing
  the volume loses the state ([Durability](../basics/durability.md)).

## See also

- [Deploying a controller](deploying_a_controller.md): the four ways to run it, the
  variables a deployment sets, graceful shutdown.
- [Connecting a runner](connecting_a_runner.md): a runner in a container or on the host.
- [Clustering](clustering.md), [Kubernetes](kubernetes.md).
- Tutorial: [Start a session](tutorials/start_a_session.html), the agentdesk
  in Compose.
