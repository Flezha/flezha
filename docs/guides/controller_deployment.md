# Deploy a controller

The controller is the brain of a Flezha deployment. Runners execute operative
code and nothing else; every decision about that code's life, and every record
of it that has to survive a crash, belongs to the controller. Five things are
its alone:

- **Placement and distribution.** Which runner each operative binds to, how a
  partitioned operative's key groups are divided between its instances, and
  where that work goes when a runner disconnects or a node leaves the cluster.
- **Healing.** A convergence loop compares what should be running against what
  is, on a tick, and closes the gap: rebinding an operative whose runner went
  away, retrying on a backoff ladder, and parking what cannot proceed instead
  of losing it.
- **Durable state.** The per-Entity write-ahead log, the snapshots and the dead
  letter queue. This is what turns a restart into a resume, and what
  effectively-once delivery is built on.
- **The registry.** The record of which operatives exist and what they were
  doing, held so that it outlives any single process, and replayed on every
  boot before the controller calls itself ready.
- **The operator surface.** Namespaces, authorization, tokens and roles,
  plugins, schedules, the audit log and observability export: everything `flz`
  and any consumer program talk to.

## Choose a way

**Do one of these four, not all of them.** They give you the same controller.
Each is self contained: get it, start it, check it, stop it. They are the four
tabs in [Start a controller](#start-a-controller).

| Way | Pick it when |
|---|---|
| Binaries | you deploy to a machine and want no container runtime |
| Docker | you want one command and no toolchain on the host |
| Kubernetes | you run a cluster. Not written yet |
| Inside an Elixir application | your own Elixir app should be the controller |

## Start a controller

<!-- tabs-open -->

### Binaries

#### Get them

TODO: download links, one archive per platform.

Until then, build a release from a checkout:

```sh
make build-release
```

Output is `_build/prod/rel/flezha/`, holding `bin/flz` (the operator
CLI) and `bin/flzvm` (the release script the CLI calls). The release carries
its own Erlang runtime, so the target machine needs no Elixir and no Erlang.

A prebuilt archive for x86-64 Linux, built that way and kept out of git, is at
`local_dev/dist/flezha-0.1.0-linux-x86_64.tar.gz`. Pack your own with
`tar -czf <name>.tar.gz --exclude='flezha/tmp' --exclude='flezha/app_data' -C _build/prod/rel flezha`:
a plain `tar` of that directory carries the logs and the node cookie of every
local run.

Unpack it where the controller will live, and work from inside it:

```sh
tar -xzf flezha-0.1.0-linux-x86_64.tar.gz
cd flezha
export PATH="$PWD/bin:$PATH"
```

Start it from this directory every time. The state root is relative to the
working directory and the minted cookie is relative to the release root, so
starting from somewhere else puts them in two different trees. An absolute
`FLEZHA_STATE_DIR` settles it for good.

#### Start it

```sh
$ FLEZHA_AUTHZ_ENABLED=false flz controller start
```

`--mode daemon` returns once the controller is launched. There are four modes,
and `-m` is the short form:

| Mode | What you get |
|---|---|
| `attached` | the default. Runs in the foreground of this terminal and logs to your screen |
| `daemon` | detached. Returns at once, and logs to `logs/flezha.log` instead |
| `attached_iex` | attached, with an IEx shell on the running node |
| `daemon_iex` | detached, with an IEx shell under the daemon |

#### Check it

Two different questions, two commands. Is it ready:

```sh
$ flz boot await
boot await: ready

$ flz boot status
readiness: ready
  met    boot_sequence_complete
  met    hubs_recovered
  met    durable_registry_loaded
  met    declared_hubs_started
```

And can a runner reach it, which needs no controller and no credential:

```sh
$ flz runner check
controller reachable
  directory  /run/user/1000/flezha (from $XDG_RUNTIME_DIR/flezha)
  checked    /run/user/1000/flezha
             /run/flezha
             /srv/flezha/app_data/sockets
```

Both exit zero only on success.

#### Stop it

```sh
flz controller stop
```

It quiesces, drains and finalises, and says so:

```
[shutdown] complete duration_ms=8 outcome=clean
```

Ctrl-C in `attached` mode stops it too, but at once: nothing is drained, and the
next start recovers from disk as it would after a crash.

That verb is authorization gated, so with enforcement on it refuses until you
have claimed an admin. `flzvm stop` needs only the cookie and works either way,
which is what to reach for on a controller you cannot authorize against.

### Docker

#### Get the image

```sh
docker pull alfetahe/flezha:0.1.0
```

TODO: amd64 only for now, other architectures to follow.

#### Start it

```sh
docker run -d --name flezha-controller \
  -e FLEZHA_AUTHZ_ENABLED=false \
  -v flezha_data:/flezha/app_data \
  alfetahe/flezha:0.1.0
```

One volume is enough: every durable tree the controller owns (state, auth,
plugins, schedules, the hub registry, the dead letter queue, logs and the
socket directory) lives under `/flezha/app_data`. Mount it, or the controller
starts cold after every `docker rm`.

#### Check it

```sh
$ docker ps --format '{{.Names}}\t{{.Image}}\t{{.Status}}'
flezha-controller	alfetahe/flezha:0.1.0	Up 20 seconds (healthy)
```

`healthy` is the controller's own verdict: the image's healthcheck runs
`flz boot await --timeout 0`, which exits zero only on `ready`. Expect
`health: starting` for the first few seconds while the hubs replay.

Ask it directly for the same answer:

```sh
$ docker exec flezha-controller /flezha/bin/flz boot await
boot await: ready
```

#### Stop it

```sh
docker stop flezha-controller
```

### Kubernetes

TODO. The reference manifests are in
[`kubernetes/`](https://github.com/alfetahe/flezha/tree/main/kubernetes) and `make k8s-apply` applies them in
dependency order, but this section has not been walked through end to end yet.

### Inside an Elixir application

TODO: this section is not confirmed yet.

Your application depends on flezha and becomes the controller. There is nothing
to start: flezha is an OTP application, and its controller tree comes up with
yours.

Working example: [`examples/elixir/embedded_controller`](https://github.com/alfetahe/flezha/tree/main/examples/elixir/embedded_controller).

#### Add the dependency

```elixir
# mix.exs
defp deps do
  [{:flezha, github: "alfetahe/flezha", branch: "dev"}]   # TODO: hex package
end
```

#### Configure it

```elixir
# config/config.exs
config :kafka_ex, disable_default_worker: true, kafka_version: "kayrock"
config :flezha, healthz_enabled: true
```

The `:kafka_ex` line is **required, not optional**. Flezha depends on kafka_ex
unconditionally, and kafka_ex starts a default worker when its application
starts. With no brokers configured that worker raises and takes your whole boot
with it:

```
** (Mix) Could not start application kafka_ex: KafkaEx.start(:normal, []) returned an error:
   ** (Protocol.UndefinedError) protocol Enumerable not implemented for Atom
```

Flezha disables that worker in its own `config/config.exs`, which your
application does not inherit.

**`FLEZHA_*` environment variables do nothing here.** They are read by flezha's
own `config/runtime.exs`, and a dependency's config files are never evaluated.
Configure through application env instead, one key per variable:

```elixir
config :flezha,
  state_root: "/var/lib/flezha/state",
  socket_files_path: "/run/flezha"
```

[`../configuration.md`](../configuration.md) lists every key.

**Authorization is off by default here.** Enforcement is the release's default,
set for `MIX_ENV=prod` by flezha's own config, so a library host gets the open
posture and a warning on every boot until it sets
`config :flezha, authn_authz: [enabled: true]`.

#### Start it

```sh
mix run --no-halt
```

Or `iex -S mix`, or your release. The boot ends with the transport listening:

```
[warning] [authz] authn/authz is OFF; every caller is trusted.
[info] [cluster] formed and ready; members [:nonode@nohost]
[info] NIF transport server (UDS) started on /run/user/1000/flezha/runner_control_channel.sock
```

#### Check it

In the node:

```elixir
iex> Flezha.Boot.Readiness.verdict()
{:ready,
 [
   %{key: :boot_sequence_complete, met: true},
   %{key: :hubs_recovered, met: true},
   %{key: :durable_registry_loaded, met: true},
   %{key: :declared_hubs_started, met: true}
 ]}
```

A controller answers `recovering` for a moment after start, while its hubs
replay, so poll this rather than reading it once.

From outside, with `healthz_enabled: true`:

```sh
$ curl localhost:9569/healthz
{"status":"healthy","durability":"local_only","tick_age_ms":3460,"bound_operatives":0,"producing_operatives":0,"unknown_operatives":0}
```

`flz` cannot reach this controller. The CLI runs the `flzvm` script beside it,
which only exists in a release.

#### Stop it

The controller stops with your application.

<!-- tabs-close -->

## Left for later

To keep things simple, this guide turned off a feature that is normally on and
passed over some of what the boot told you. Neither matters for a first run,
and both are picked up later.

**Authorization is off.** Every command above sets `FLEZHA_AUTHZ_ENABLED=false`.
With it on, operator verbs refuse until you claim an admin, and a runner has to
prove the namespace it connects to.
[`authorization.md`](authorization.md) turns it on.

**The boot prints three decisions you can ignore for now:** the node cookie it
minted, the state root it resolved, and that no durability backend is set. They
start to matter once there is a second machine or a second disk, which is
[`clustering.md`](clustering.md) and [`durability.md`](durability.md).

## Next

A controller with no runner does nothing.
[`runner_deployment.md`](runner_deployment.md) connects one and starts an operative
on it.
