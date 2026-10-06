# Start the controller

On this page you start one controller and leave it running for the rest of Up
and running. The controller places relays on runners, keeps their state durable
and answers `flz`; [Deploying a controller](../../topics/deploying/deploying_a_controller.md)
covers it in full, including Kubernetes and running it inside an Elixir
application.

> Start a controller: pick one of the two tabs and follow it from top to
> bottom. Leave it running until [Stop everything](stop_everything.md).
> {: .step}

<!-- tabs-open -->

### Binaries

Download the release and unpack it:

```sh
$ curl -fLO https://github.com/flezha/flezha/releases/download/v0.1.0-alpha.1/flezha-0.1.0-linux-x86_64.tar.gz
$ tar -xzf flezha-0.1.0-linux-x86_64.tar.gz
```

That gives you a `flezha/` directory, with `bin/flz`, the CLI. The release
carries its own Erlang runtime, so nothing else needs installing. It is built
for Linux x86_64; on another platform,
[build it from source](../../topics/deploying/building_from_source.md). Every
`./bin/flz` command in the tutorials runs from that directory, in whichever
terminal you use, so go there and start the controller:

```sh
$ cd flezha
$ FLEZHA_AUTHZ_ENABLED=false ./bin/flz controller start
```

It runs in the foreground of this terminal. Leave it open, and run everything
after this from a second terminal, in the `flezha` directory. Check that it is
ready:

```sh
$ ./bin/flz boot await
```

```output
boot await: ready
```

And that a runner can reach it, which needs no credential:

```sh
$ ./bin/flz runner check
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
