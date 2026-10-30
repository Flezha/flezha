# Deploying a controller

The controller is the part of Flezha that decides and remembers. Runners run
relay code and nothing else; every decision about that code's life, and
every record of it that has to survive a crash, belongs to the controller:

- **Placement and distribution.** Which runner each relay binds to, how a
  partitioned relay's key groups are divided, and where that work goes when
  a runner disconnects or a node leaves the cluster.
- **Healing.** A convergence loop compares what should be running with what
  is, on a tick, and closes the gap: it rebinds a relay whose runner went
  away, retries on a backoff ladder, and parks what cannot proceed instead of
  losing it.
- **Durable state.** Each cell's write-ahead log, the snapshots and the
  dead-letter queue. This is what turns a restart into a resume, and what
  effectively-once delivery is built on.
- **The registry.** The record of which relays exist and what they were
  doing, kept so that it outlives any single process, and replayed on every
  boot before the controller calls itself ready.
- **The operations surface.** Namespaces, authorization, tokens and roles,
  plugins, schedules, the audit log and observability export: everything
  `flz` and any consumer program talk to.

Each machine that runs runners runs a controller, and the runners attach to
it through a local socket ([`connecting_a_runner.md`](connecting_a_runner.md)).
Several controllers form one cluster ([`clustering.md`](clustering.md)).

## Ways to run it

The four ways give you the same controller:

| Way | Pick it when |
| --- | --- |
| Binaries (a release) | you deploy to a machine and want no container runtime |
| Docker | you want one command and no toolchain on the host |
| Kubernetes | you run a cluster: one controller per node, as a DaemonSet ([`kubernetes.md`](kubernetes.md)) |
| Inside an Elixir application | your own Elixir application should be the controller |

<!-- tabs-open -->

### Binaries

**Get the release.** Each release has an archive per platform:

| Platform | Archive |
|---|---|
| Linux x86_64 | `flezha-<version>-linux-x86_64.tar.gz` |
| macOS, Apple Silicon | `flezha-<version>-macos-arm64.tar.gz` |
| macOS, Intel | `flezha-<version>-macos-x86_64.tar.gz` |

Download yours with its checksum file, check it, and unpack it:

```sh
$ curl -fLO https://github.com/flezha/flezha/releases/download/v0.1.0-alpha.1/flezha-0.1.0-linux-x86_64.tar.gz
$ curl -fLO https://github.com/flezha/flezha/releases/download/v0.1.0-alpha.1/SHA256SUMS.txt
$ sha256sum --check --ignore-missing SHA256SUMS.txt
$ tar -xzf flezha-0.1.0-linux-x86_64.tar.gz
$ cd flezha
```

On macOS, `shasum -a 256 --check --ignore-missing SHA256SUMS.txt` does the
check. Download with `curl`, as above: a browser marks the file as downloaded
from the internet, and macOS then refuses to run the unsigned binaries in it.

The `flezha/` directory holds `bin/flz` (the CLI) and `bin/flzvm` (the release
script the CLI calls). The release carries its own Erlang runtime, so the
machine it runs on needs no Elixir and no Erlang. The Linux archive is the same
artifact the production image runs. Each archive started a controller and
served a runner on its own platform before it was published. On another
platform, or with changes of your own,
[build it from source](building_from_source.md).

The commands below call it as `flz`; [CLI](../cli/cli.md) shows how
to put it on your path.

**Start the controller from the same directory every time**, or set
`FLEZHA_STATE_DIR` to an absolute path. The state root is relative to the
working directory and the minted cookie is relative to the release root, so
starting from somewhere else opens two different trees.

> #### State inside the release directory {: .warning}
>
> Started from the release directory, the controller keeps its durable state
> and its cookie in `storage/app/` beside `bin/`. Deleting the directory to put a
> newer release in its place deletes them too. For anything you want to keep,
> point `FLEZHA_STATE_DIR` at an absolute path outside it.

**Start it:**

```sh
$ flz controller start
```

`flz controller start` runs the release's own boot script, and takes a mode
(`-m` for short):

| Mode | What you get |
| --- | --- |
| `attached` | the default. Runs in the foreground of this terminal and logs to it |
| `daemon` | detached. Returns at once, and logs to `storage/logs/flezha.log` |
| `attached_iex` | attached, with an IEx shell on the running node |
| `daemon_iex` | detached, with an IEx shell under the daemon |

### Docker

```sh
$ docker pull flezha/flezha:0.1.0
```

```sh
$ docker run -d --name flezha-controller \
  -v flezha_data:/flezha/storage/app \
  flezha/flezha:0.1.0
```

The image is amd64 only for now. One volume is enough: every durable tree the
controller owns (state, authorization, plugins, schedules, the hub registry,
the dead-letter queue, logs and the socket directory) lives under
`/flezha/storage/app`. Mount it, or the controller starts cold after every
`docker rm`.

The image's healthcheck runs `flz boot await --timeout 0`, which exits zero
only when the controller is ready:

```sh
$ docker ps --format '{{.Names}}\t{{.Image}}\t{{.Status}}'
```

```output
flezha-controller	flezha/flezha:0.1.0	Up 20 seconds (healthy)
```

Expect `health: starting` for the first seconds while the hubs replay. To
build the image yourself, the version goes before `make`; without it the tag
is `flezha/flezha:` and Docker refuses it:

```sh
$ VERSION=0.1.0 make docker-build
```

That builds the production image for the host's architecture.

### Kubernetes

The reference manifests are in
[`deploy/kubernetes/`](https://github.com/flezha/flezha/tree/main/deploy/kubernetes):
`make k8s-apply` applies the six manifests in dependency order, and
`make k8s-delete` deletes the DaemonSet and ConfigMap. The walk-through is
[`kubernetes.md`](kubernetes.md).

### Inside an Elixir application

Your application depends on `flezha` and becomes the controller. There is
nothing to start: Flezha is an OTP application, and its controller tree comes
up with yours. This way is not confirmed end to end yet.

```elixir
# mix.exs
defp deps do
  [{:flezha, github: "flezha/flezha", branch: "dev"}]   # TODO: hex package
end
```

```elixir
# config/config.exs
config :flezha, healthz_enabled: true
```

**`FLEZHA_*` environment variables do nothing here.** They are read by
Flezha's own `config/runtime.exs`, and a dependency's config files are not
evaluated. Configure through application env, one key per variable:

```elixir
config :flezha,
  state_root: "/var/lib/flezha/state",
  socket_files_path: "/run/flezha"
```

**Authorization is off by default here.** Enforcement is the release's default,
set for `MIX_ENV=prod` by Flezha's own config, so a library host gets the open
posture and a warning on every boot until it sets
`config :flezha, authn_authz: [enabled: true]`.

Start it with `mix run --no-halt`, `iex -S mix` or your release. In the node,
`Flezha.Boot.Readiness.verdict()` answers `{:ready, [...]}` with the same
conditions `flz boot status` lists; it answers `recovering` for a moment after
start, so poll it. From outside, with `healthz_enabled: true`,
`curl localhost:9569/healthz` answers the health JSON
([Health and liveness](../../architecture/kernel.md#health-and-liveness)). `flz` cannot reach this
controller: the CLI runs the `flzvm` script, which only exists in a release.

An Elixir runner can run in the same BEAM; it connects over the
controller's socket like any other runner
([`connecting_a_runner.md`](connecting_a_runner.md)).

<!-- tabs-close -->

## What the boot tells you

A healthy boot is a handful of lines, ending with the transport listening:

```output
[flezha] no RELEASE_COOKIE provisioned; using the node-local cookie in /home/you/flezha/storage/app/state/cookie.
[flezha] That value is this node's alone. A cluster provisions RELEASE_COOKIE with the same value on every node.
[shutdown] OS signal handlers installed (SIGTERM)
[flezha] durable state under /home/you/flezha/storage/app/state (set with FLEZHA_STATE_DIR)
[flezha] that directory holds no durable state: this controller is starting cold
[flezha] runners attach with FLEZHA_SOCKET_FILES_PATH=/run/user/1000/flezha
[cluster] formed and ready; members [:"flezha@127.0.0.1"]
NIF transport server (UDS) started on /run/user/1000/flezha/runner_control_channel.sock
```

The first lines say what the boot chose, and print on every boot, for whoever
inherits the node later. The cold-start line goes away once the directory
holds something. If it comes back on a controller that had state, you are
almost certainly starting from a different directory, and the data is still in
the old one; `flz controller status` answers the same question at any time.
Without a durability backend the boot also warns that this disk holds the only
copy ([`durability.md`](../core/durability.md)).

## Check that it is up

Two questions, two commands, and both exit zero only on success. Is the
controller ready:

```sh
$ flz boot await
```

```output
boot await: ready
```

```sh
$ flz boot status
```

```output
controller flezha@127.0.0.1
readiness: ready
  met    boot_sequence_complete
  met    hubs_recovered
  met    durable_registry_loaded
  met    declared_hubs_started
```

A controller still coming up answers `recovering` and names what it waits
for. The socket is bound early in the boot, well before the controller can
serve anything, so the second question is separate: can a runner reach it.
`flz runner check` needs no controller and no credential
([`connecting_a_runner.md`](connecting_a_runner.md#the-socket-directory)).

## Point the CLI at it

`flz` sends each verb to the controller over its control socket, which it
finds the way a runner does: with the same `FLEZHA_SOCKET_FILES_PATH` as the
controller, or neither, it needs nothing more. Every controller listens on that
socket, whatever `FLEZHA_TRANSPORT` its runners use.

A verb run before the controller has started, or from a shell that looks in
another socket directory, reports the controller unreachable and names the
socket it looked for:

```output
error: the controller could not be reached at its control socket /run/user/1000/flezha/runner_control_channel.sock: it is not up yet, it stopped after it was reached, or it listens in another socket directory (FLEZHA_SOCKET_FILES_PATH) (controller.unreachable)
hint: flz controller start
```

## Variables a deployment sets

On one machine every variable has a default that works, and the boot says
which defaults it took. A real deployment sets these:

| Variable | Why it matters |
| --- | --- |
| `FLEZHA_DURABILITY_BACKEND` | Unset, it is `none`, and every boot warns that this node's disk holds the only copy of its durable state. Setting `none` explicitly is the opt-out, reported as `durability: local_only` on `/healthz`. `postgres`, `s3` and `local_path` enable the remote tier and need their `FLEZHA_DURABILITY_PG_*`, `FLEZHA_DURABILITY_S3_*` or `FLEZHA_DURABILITY_PATH` settings; a backend selected without one refuses the boot, naming the option. Any other spelling resolves to `none` and warns ([`durability.md`](../core/durability.md)). |
| `RELEASE_COOKIE` | Unset, the first start mints a random node-local cookie, keeps it `0600` at `${FLEZHA_STATE_DIR}/cookie` (moved with `FLEZHA_COOKIE_FILE`) and reuses it. That is enough for one node. **A cluster has to set it**: two nodes connect only under one cookie you gave them both. Where the release can neither read nor mint one, it starts with `RELEASE_DISTRIBUTION=none`, and every `flz` verb then fails, because the CLI reaches the node over distribution. |
| `FLEZHA_SOCKET_FILES_PATH` | The directory runners attach through; share it with every runner container on the node. The container entrypoint waits for `<path>/runner_control_channel.sock` before it asks for readiness. At most 60 bytes, or the controller refuses to boot. |
| `FLEZHA_NODE_NAME`, `FLEZHA_NODE_HOST` | Form the node name `<name>@<host>` the CLI reaches. They default to the release name and `127.0.0.1`; in Kubernetes the host is the pod IP, or the pod's stable hostname with `FLEZHA_K8S_HOST_IDENTIFIER=hostname`. |
| `FLEZHA_STATE_DIR`, `FLEZHA_PROCESS_HUB_REGISTRY_ROOT`, `FLEZHA_DLQ_DIR`, `FLEZHA_AUTHZ_DIR`, `FLEZHA_SCHEDULE_DIR`, `FLEZHA_AUDIT_DIR`, `FLEZHA_PLUGIN_ROOT`, `FLEZHA_LOG_PATH` | Every durable path defaults to a **relative** `storage/app/*`, resolved against the directory the controller was started from, so it is lost with a container and unwritable under a read-only root. Point each at a mounted volume owned by uid 1000. The boot prints the absolute state root, and says when it is empty. |
| `FLEZHA_HEALTHZ_ENABLED` | `/healthz` is off by default; an orchestrator probe needs it on (`FLEZHA_HEALTHZ_PORT` 9569, `FLEZHA_HEALTHZ_BIND` `0.0.0.0`). What it and `/readyz` answer is in [Health and liveness](../../architecture/kernel.md#health-and-liveness). |
| `FLEZHA_CLUSTER_DISCOVERY` | `manual_discovery` by default; `k8s_pod_discovery` in Kubernetes. |
| `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` | 60 000 by default, paired with the orchestrator's grace period ([below](#graceful-shutdown)). |
| `FLEZHA_AUTHZ_ENABLED`, `FLEZHA_TRANSPORT_AUTH`, `FLEZHA_DIST_TLS_OPTFILE`, `FLEZHA_CA_CERT_PATH`, `FLEZHA_SERVER_CERT_PATH`, `FLEZHA_SERVER_KEY_PATH`, `FLEZHA_QUIC_CLIENT_CA` | Authorization is enforced by default in a release and peer authentication is required by default; turning either off warns on every boot. The TLS material must persist across restarts ([`authorization.md`](../core/authorization.md)). |

Every variable is in [Controller environment variables](../../configuration/environment_variables.md).

## Where a daemon's output goes

A daemon writes nothing to your terminal:

| What | Where |
| --- | --- |
| everything the controller logs | `FLEZHA_LOG_PATH`, default `storage/logs/flezha.log` |
| the boot, and anything printed before the logger is up | `$RELEASE_TMP/log/`, default `<release>/tmp/log/` |

The second is inside the release directory, so replacing the release takes it
with it. Point `RELEASE_TMP` somewhere durable on a machine you care about.

## Tune the VM

The release ships its own VM flags in
`flezha/releases/<vsn>/vm.args`, and on bare metal that file
is yours to edit. For one node or one experiment, `ERL_FLAGS` is less trouble
and wins over the file:

```sh
$ ERL_FLAGS="+S 4:4" flz controller start --mode daemon
```

`ERL_FLAGS` is read by every `flzvm` call, and every `flz` verb starts one, so
exporting it in a shell profile enlarges those short-lived nodes too. A
dedicated machine is the one place scheduler binding (`+stbt`) is worth
trying; it is wrong wherever a CPU quota or a cpuset can move underneath the
VM. The full list, including the flags deliberately left unset, is in the
[configuration reference](../../configuration/vm_arguments.md).

## Several controllers on one machine

A machine may host more than one controller. Each resource a controller owns
is selected by a variable, so two instances coexist when each has its own
values:

| Variable | What the instance owns |
| --- | --- |
| `FLEZHA_SOCKET_FILES_PATH` | the socket directory, which is also the access boundary |
| `FLEZHA_STATE_DIR` | the cells' logs, snapshots and outboxes; claimed exclusively at boot |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | the durable hub registry, which is not keyed by node |
| `FLEZHA_DLQ_DIR` | the dead-letter queue |
| `FLEZHA_NODE_NAME` (with `FLEZHA_NODE_HOST`) | the node name; two nodes on one host cannot share one |
| `FLEZHA_HEALTHZ_PORT` | the probe listener, when enabled |
| `FLEZHA_QUIC_LISTEN_ADDRESS` | the QUIC listener ([experimental: quic](../../publishing/maturity.md#quic)), when enabled |
| `FLEZHA_AUTHZ_DIR`, `FLEZHA_SCHEDULE_DIR`, `FLEZHA_AUDIT_DIR`, `FLEZHA_PLUGIN_ROOT`, `FLEZHA_LOG_PATH` | the remaining durable trees |

The unit of separation is the directory, not a file name inside a shared one,
because the directory carries the permission boundary. Two guards make a
missed variable a refusal rather than damage:

- **A live peer's socket directory is refused.** Before preparing its
  directory, a controller probes any control socket there. A peer that answers
  is live, so the controller refuses without removing anything, naming the
  path, the socket that answered and `FLEZHA_SOCKET_FILES_PATH`. A socket no
  one answers belongs to a controller that died hard, and is cleared.
- **The state root is claimed exclusively**, with a lock on
  `<state root>/.instance.lock` held for the controller's lifetime. Two
  controllers writing one state root would corrupt it silently; the refusal
  names the state root, the process holding it and `FLEZHA_STATE_DIR`. The
  lock goes away with its holder, so a killed controller leaves nothing to
  clear.

`flz reset` resolves its roots under the storage root and does not read
these per-store variables, so on a co-located controller it clears nothing
that controller owns. Clear a relocated tree by path, with the controller down.

> #### A second controller is capacity, not a backup {: .warning}
>
> Each relay runs on exactly one node, with no replicas, and moving state
> between controllers reads it from the source node, so a controller that died
> with its disk answers nothing and the one adopting its work starts cold. A
> controller that dies with its disk takes its relays' state, and the record
> of their existence, with it. The remote durability tier is what survives
> that ([`durability.md`](../core/durability.md)). Two controllers believed to cover
> each other are worse off than one controller with the tier on.

## Graceful shutdown

Graceful shutdown is on by default. A controller that is told to stop runs
three phases:

1. **Quiesce.** The node plans nothing new onto itself: from here until the
   node stops, a relay start there answers `shutting_down` and new runner
   connections are refused. Running relays and connected runners are not
   touched.
2. **Drain.** Each relay it hosts moves to a peer controller, namespace by
   namespace, concurrently, the way its namespace moves relays (by
   [cold swap](../core/namespaces.md#how-its-relays-move) unless it chooses
   hot), and the drain ends once each moved relay has arrived on its peer and
   copied its saved state from this controller. A relay in the
   middle of critical work can defer its move (below); whatever still defers at
   the deadline is moved by force. With no peer to move to, nothing moves, and
   the next boot restores the relays from disk
   ([`running_and_testing.md`](../core/running_and_testing.md#after-a-controller-restart)).
3. **Finalise.** Pending disk writes are flushed, the durable logs are closed,
   then the hubs stop with the relays they still host, and the transports
   stop; both stay stopped until the node ends. The flush and the close are
   bounded to 5 s each, and each hub stops its relays inside its own
   shutdown bound. A step that fails is logged as
   `[shutdown] step node/<name> failed: ...` and the next one still runs.
   Finalise has its own budget, the 15 s of headroom below, so a drain that
   used the whole timeout still leaves it whole; at its end the node's own
   shutdown finishes whatever is left.

Quiesce and drain are bounded by `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`. On
expiry the controller logs an error, finalises, which stops the remaining
relays, and reports which relays were still local. It does not hang on a
drain.

**What starts it:**

| Run mode | Trigger |
| --- | --- |
| any | SIGTERM to the controller's process |
| a release | `bin/flzvm stop` |
| any | `flz controller stop` |
| `docker stop`, compose stop | SIGTERM through tini; the entrypoint runs `flz controller stop --timeout-ms ${FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS}` |
| `kubectl delete pod` | the `preStop` hook runs the same `flz controller stop`, then SIGTERM |

SIGINT is not among them: the Erlang runtime reserves it, so Ctrl-C keeps its
default meaning. Each of these needs the controller's process to *receive*
SIGTERM. A container that starts as root and drops privileges must do it with
something that replaces the process (`exec setpriv`), so the controller stays
PID 1. Wrapping it in `su` breaks the drain: `su` answers SIGTERM by killing
its child about two seconds later, whatever the timeouts say.

> #### Pair the timeout with the orchestrator {: .warning}
>
> The orchestrator's stop grace period must be
> `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS / 1000 + 15` seconds: 60 000 ms pairs
> with 75 s, 120 000 ms with 135 s. The 15 s are headroom for finalise and
> container teardown. Change both together, or the controller is killed in the
> middle of its drain. In the shipped files the pair is
> `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` in `deploy/kubernetes/flezha-configmap.yaml`
> with `terminationGracePeriodSeconds: 75` in
> `deploy/kubernetes/flezha-daemonset.yaml`, and `stop_grace_period: 75s` in
> `docker/{prod,dev}/docker-compose.yml` and
> `deploy/docker/dev/docker-compose.healing.yml`.

- `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED`: `1`, `true`, `yes` or `on` (any case) is true; anything else turns graceful shutdown off.
- `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`: Bounds the whole sequence.

Their types and defaults are in [Controller environment variables](../../configuration/environment_variables.md#every-variable).

**Relays that defer their move.** When any move is planned (a node joining,
a rebind, a drain), each relay is asked whether it may move now, and
answers "not yet" while a move would tear critical work:

| Busy with | Ready to move when |
| --- | --- |
| a late-input correction | the correction completes |
| a durable steps instance that is not asleep | it reaches a committed iteration boundary and sleeps |
| an effectively-once outbox not yet flushed | it is asked again on the next retry |

- `FLEZHA_MIGRATION_CONSENT_TIMEOUT_MS`: No answer within this counts as "not yet".
- `FLEZHA_MIGRATION_RETRY_INTERVAL_MS`: How often a deferring relay is asked again.
- `FLEZHA_MIGRATION_MAX_DEFER_MS`: After this, the relay is moved by force and recovers from its journal and log on the target; a deferring relay does not outlive the shutdown window.

Their types and defaults are in [Controller environment variables](../../configuration/environment_variables.md#every-variable).

**`flz controller stop`** drives the shutdown in each run mode, streams drain
progress to stderr, and needs the `controller.stop` permission. Its options:
`--timeout-ms <N>` overrides the drain timeout for this call; `--no-wait`
starts the shutdown and exits 0 without waiting; `--force` skips quiesce and
drain and only finalises.

| Exit | Meaning |
| --- | --- |
| 0 | clean shutdown: each relay moved to a peer, or `--force` finalised cleanly. A stop issued while one is in flight follows it and exits with its outcome |
| 3 | `controller.unreachable`: the controller is not running, or the socket path is wrong |
| 4 | `authz.*`: refused for want of a token or a role |
| 7 | `shutdown.partial_drain`: the timeout passed with relays still local at finalise |

Before an admin is claimed, or when the controller is wedged, the release
script stops it with the cookie alone:

```sh
$ flzvm stop
```

A clean stop removes the socket directory's contents. A controller that died
hard leaves the socket behind, and the next start clears it.

**`flz controller status`** reports `shutdown_in_progress`, `shutdown_phase`
(`quiesce`, `drain`, `finalise` or none), `signal_handler_status` (`degraded`
when the platform refused the SIGTERM handler), `graceful_shutdown_enabled` and
`graceful_shutdown_timeout_ms`. A readiness probe can poll it to drain traffic
early.

**A drain that does not progress** can be cut short: the first SIGTERM starts
the graceful shutdown, a second is logged and ignored, and a third logs a
warning and exits the controller at once with status 143. The orchestrator's
grace period is the wall-clock backstop.

**Turning it off.** `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false` makes SIGTERM
stop the controller directly, with no drain; the container entrypoint then
sends SIGTERM instead of calling `flz controller stop`.

**Telemetry.** The shutdown emits events under `[:flezha, :shutdown, ...]`, and
each phase boundary also logs a `[shutdown] ...` line:

| Event | Measurements | Metadata |
| --- | --- | --- |
| `:requested` | none | `reason`, `timestamp_ms`; emitted once |
| `:phase_started` | none | `phase`: `:quiesce`, `:drain` or `:finalise` |
| `:phase_completed` | `duration_ms` | `phase` |
| `:drain_progress` | `migrated`, `forced`, `remaining_hubs`, `elapsed_ms` | `namespace`, one per finished namespace |
| `:timeout` | none | `phase`, `still_local` (relay ids) |
| `:complete` | `duration_ms` | `outcome`: `:clean`, `:partial` or `:forced`; `reason` |

## Limits and gotchas

- **A relative state root moves with you.** Start from one directory, or set
  `FLEZHA_STATE_DIR` to an absolute path.
- **`make clean` deletes a release's `storage/app/`** when the controller runs
  from the build directory.
- **The CLI cannot reach an embedded controller**: there is no `flzvm` beside
  it.
- **One disk is one copy.** Without a durability backend, losing the disk
  loses the state ([`durability.md`](../core/durability.md)).
- **Upgrade every controller together** to the release that moves relays by
  cold swap by default: an older controller neither learns where a moved relay
  came from nor takes a cold move's handover, so a relay moved onto it arrives
  without its state.
- **Every boot refusal**, what it means and its fix, plus a controller that
  will not stop:
  [`../../troubleshooting/controller_will_not_start.md`](../../troubleshooting/controller_will_not_start.md).

## See also

- [`connecting_a_runner.md`](connecting_a_runner.md): runners attaching to the controller.
- [`authorization.md`](../core/authorization.md): claiming an admin, tokens, TLS.
- [`durability.md`](../core/durability.md): the remote durability tier.
- [`running_and_testing.md`](../core/running_and_testing.md): what the controller restores at boot, and `flz reset`.
- [`clustering.md`](clustering.md), [`kubernetes.md`](kubernetes.md).
- [`../../configuration.md`](../../configuration.md): every variable and application key.
- Up and running: [Start the controller](../../getting_started/up_and_running.md#start-the-controller).
