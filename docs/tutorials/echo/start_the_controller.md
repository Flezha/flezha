# Start the controller

On this page you start one controller and leave it running for the rest of the
tutorial. The controller places operators on runners, keeps their state durable
and answers `flz`; [Deploying a controller](../../topics/basics/deploying_a_controller.md)
covers it in full, including Kubernetes and running it inside an Elixir
application.

> Start a controller: pick one of the two tabs and follow it from top to
> bottom. Leave it running until [Stop everything](stop_everything.md).
> {: .step}

<!-- tabs-open -->

### Binaries

Build a release from your checkout of the repository:

```sh
$ make build-release
```

That writes `_build/prod/rel/flezha/`, with `bin/flz`, the CLI. The release
carries its own Erlang runtime. Put its `bin` on your path, in every terminal
you use:

```sh
$ cd _build/prod/rel/flezha
$ export PATH="$PWD/bin:$PATH"
```

Start the controller from that directory, every time:

```sh
$ FLEZHA_AUTHZ_ENABLED=false flz controller start
```

It runs in the foreground of this terminal. Leave it open, and run everything
after this from a second terminal. Check that it is ready:

```sh
$ flz boot await
```

```output
boot await: ready
```

And that a runner can reach it, which needs no credential:

```sh
$ flz runner check
```

```output
controller reachable
  directory  /run/user/1000/flezha (from $XDG_RUNTIME_DIR/flezha)
  checked    /run/user/1000/flezha
             /run/flezha
             /srv/flezha/app_data/sockets
```

### Docker

```sh
$ docker run -d --name flezha-controller \
  -e FLEZHA_AUTHZ_ENABLED=false \
  -v flezha_data:/flezha/app_data \
  -v /tmp/flz_sock:/flezha/app_data/sockets \
  flezha/flezha:0.1.0
```

The first volume holds everything the controller keeps. The second puts its
socket in `/tmp/flz_sock` on the host, where your runner finds it; the image
runs as uid 1000, so your host user needs that uid to reach it. Check that it
is ready:

```sh
$ docker exec flezha-controller /flezha/bin/flz boot await
```

```output
boot await: ready
```

<!-- tabs-close -->

`FLEZHA_AUTHZ_ENABLED=false` turns [authorization](../../topics/basics/authorization.md)
off, so no command here needs a token. The tradedesk tutorial turns it on.

## Next

A controller with no runner does nothing.
[Write the echo runner](write_the_echo_runner.md) connects one to it.
