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

## Contents

**Do one of these four, not all of them.** They give you the same controller.
Each is self contained: get it, start it, check it, stop it.

| Way | Pick it when |
|---|---|
| [Docker](#docker) | you want one command and no toolchain on the host |
| [Binaries](#binaries) | you deploy to a machine and want no container runtime |
| [Inside an Elixir application](#inside-an-elixir-application) | your own Elixir app should be the controller |
| [Kubernetes](#kubernetes) | you run a cluster. Not written yet |

**Everything after them is optional.**

| Section | Read it when |
|---|---|
| [Three lines in the boot worth reading](#three-lines-in-the-boot-worth-reading) | before you run a controller you care about |
| [Authorization](#authorization) | you are ready to claim an admin, or you want authorization out of the way |

## Start a controller

<!-- tabs-open -->

### Docker

#### Get the image

```sh
docker pull alfetahe/flezha:0.1.0
```

TODO: amd64 only for now, other architectures to follow.

#### Start it

```sh
docker run -d --name flezha-controller \
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
$ flz controller start
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
controller reachable at /run/user/1000/flezha (from $XDG_RUNTIME_DIR/flezha); walked
/run/user/1000/flezha, /run/flezha, /srv/flezha/app_data/sockets. Fix: nothing to
fix - the controller answered
```

Both exit zero only on success.

#### Stop it

```sh
flzvm stop
```

`flz controller stop` is the graceful verb, and it is authorization gated: it
refuses until the bootstrap admin is claimed. `flzvm stop` needs only the
cookie, so it is what works on a controller you have not claimed yet.

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

### Kubernetes

TODO. The reference manifests are in
[`kubernetes/`](https://github.com/alfetahe/flezha/tree/main/kubernetes) and `make k8s-apply` applies them in
dependency order, but this section has not been walked through end to end yet.

<!-- tabs-close -->

## Three lines in the boot worth reading

Every start prints what it decided. These are the three that bite later.

| Line | What it means |
|---|---|
| `no RELEASE_COOKIE provisioned; using the node-local cookie in <path>` | This node minted its own cookie. Enough for one node. A cluster needs one `RELEASE_COOKIE` you set on every node, because only a shared value forms one. |
| `durable state under <path>` | The state root. It defaults to a **relative** `app_data/state`, so starting from a different directory opens a different tree and the controller comes up cold. Set `FLEZHA_STATE_DIR` to an absolute path, or always start from the same place. |
| `FLEZHA_DURABILITY_BACKEND is not set` | This machine's disk holds the only copy of durable state. Set the variable to `none` to accept that and silence the warning, or to `postgres`, `s3` or `local_path` to keep a copy off this machine. |

## Authorization

A fresh controller enforces authorization and has no admin yet, so every gated
verb refuses and names the state it is in:

```
authz.namespace_create: refused: authorization is enforced but the bootstrap has not been claimed
```

It goes on to name `flz auth bootstrap` and the file holding the token.

Data keeps flowing while that is true. It is the operator surface that is shut,
not the controller.

### Claim the admin

The first enforced boot writes a one-time token to
`app_data/auth/bootstrap.token`, under the working directory the controller was
started from, or `/flezha/app_data/auth/bootstrap.token` in the container. Read
the file rather than the log: the path is logged only on the boot that creates
it.

```sh
$ cat app_data/auth/bootstrap.token
g5dy7mfj3z3ccnmgdzjzn74wuw6iqhqthz3gvht4clobnuaghwwa

$ flz auth bootstrap g5dy7mfj3z3ccnmgdzjzn74wuw6iqhqthz3gvht4clobnuaghwwa
bootstrap claimed; admin@_system bound to admin-ad133413363b2d3c
admin token (shown once, store it now): flzt_kpkt3cgsped2eqyei25nowypv6nu4d45o4ig37ikvyomwbbszwga
```

The claim unlinks the file, and "shown once" means it: nothing can print that
token again.

### Use it

A gated verb takes the token from `--token` or from `FLEZHA_TOKEN`:

```sh
$ export FLEZHA_TOKEN=flzt_kpkt3cgsped2eqyei25nowypv6nu4d45o4ig37ikvyomwbbszwga
$ flz auth whoami
principal: admin-ad133413363b2d3c
namespace: _system
role:      admin
```

That variable belongs in the shell that runs `flz`, not in the controller's
environment. The container is the one exception: its entrypoint runs
`flz controller stop` on SIGTERM, so a container with no `FLEZHA_TOKEN` is
forced down instead of drained.

### The first token lasts a day

The minted admin token carries the default lifetime of one day. Issue a longer
one before it lapses:

```sh
$ flz token issue admin _system admin --ttl 2592000
token issued for admin (admin@_system)
id:    60384fcaea2a
token (shown once, store it now): flzt_qngtidkyk7ljhvz7ohg4hjnhviqfkhepyvohqxkbvfomkrhb4mea
```

The positionals are `<principal> <namespace> <role>`, where the role is
`viewer`, `operator`, `admin` or `runner`.

### If you lose the admin

No token file to read means the bootstrap was already claimed. Mint a fresh one
without stopping the controller:

```sh
$ flz auth rebootstrap
Bootstrap token written to ./app_data/auth/bootstrap.token.
Claim it against the RUNNING controller:
    flz auth bootstrap 5ce1782ca518d067b7bbfa9dd81dd26a6c8df1af9f71425c4d5415d9a3da6a85
```

That claim unbinds every `admin@_system` and revokes its `_system` admin
tokens. Operatives, runner connections and runner credentials carry on.

Two refusals tell the two states apart. One naming `flz auth bootstrap` and a
file path means not claimed, and the token is in that file. `no verifiable
identity` means claimed, and you need a token.

### Turning it off

While you are learning the system, or running it on your own machine, one
variable removes all of the above:

| Way | Where it goes |
|---|---|
| Docker | `-e FLEZHA_AUTHZ_ENABLED=false` on the `docker run` |
| Binaries | `FLEZHA_AUTHZ_ENABLED=false flz controller start`, or exported in that shell |
| Elixir application | nothing to do. It is already off, because enforcement is set for `MIX_ENV=prod` by flezha's own config and your application does not inherit that |

The posture becomes open. Every caller is authorized, no verb needs a token, no
runner is asked for a credential, and `flz controller stop` drains instead of
refusing. The boot says so on every start:

```
[warning] [authz] authn/authz is OFF; every caller is trusted.
```

Turn it back on for anything that matters.
[`../operations/authn_authz.md`](../operations/authn_authz.md) covers roles,
namespaces and certificates.

## Next

A controller with no runner does nothing.
[`runner_deployment.md`](runner_deployment.md) connects one and starts an operative
on it.
