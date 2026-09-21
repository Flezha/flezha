# Bare metal: two ordinary processes

A controller and a runner, started by hand on one machine. Nothing here needs a
container, and on one machine as one user nothing needs configuring to make the
two find each other.

## Before you start

Build the release first. [`building.md`](building.md) has the targets; the one
you want is:

```bash
make build-release
```

That produces `_build/prod/rel/flezha/`, holding `bin/flzvm` (the controller)
and `bin/flz` (the CLI). Every command below runs from the project root.

[`variables.md`](variables.md) lists what a deploy cannot omit. On one machine
nothing does: every variable has a default that works here, and the boot says
which defaults it took. Three are worth knowing before the first start:

| Variable | Default here | When to set it |
|---|---|---|
| `RELEASE_COOKIE` | A random 32-byte value minted on the first boot, kept `0600` in `<state root>/cookie` and reused afterwards. | Whenever two nodes must cluster: they can only do that under one value you gave them both. `FLEZHA_COOKIE_FILE` moves the file. |
| `FLEZHA_DURABILITY_BACKEND` | `none`: the local disk holds the only copy of durable state. The boot warns, on every boot, until you decide. | When durable state must survive losing this machine's disk. Setting `none` explicitly is the other valid answer and silences the warning. |
| `FLEZHA_STATE_DIR` | `app_data/state`, **relative to the directory you start from**. The boot prints the absolute path it resolved to. | Whenever the controller may be started from more than one directory: a relative root opens a different tree from each one. An absolute path here ends the question. |

The node name and host, which together form the node the CLI reaches, default
to the release name and `127.0.0.1` in `rel/env.sh.eex`; set `FLEZHA_NODE_NAME`
and `FLEZHA_NODE_HOST` when you want something else. The socket directory
resolves itself, as below.

## Start the controller

`flz controller start` is the front door. It runs the release's own boot script
underneath, so the variables above apply either way, and it takes the mode:

```bash
export PATH="$PWD/_build/prod/rel/flezha/bin:$PATH"

flz controller start                    # attached, logs to this terminal
flz controller start --mode daemon      # detached, returns immediately
flz controller start --mode attached_iex   # attached with an IEx shell
```

Attached is the one to use the first time: the boot is short and you see it.

A healthy boot is a handful of lines, ending with the transport listening:

```
[flezha] no RELEASE_COOKIE provisioned; using the node-local cookie in /home/you/flezha/app_data/state/cookie.
[flezha] That value is this node's alone. A cluster provisions RELEASE_COOKIE with the same value on every node.
[shutdown] OS signal handlers installed (SIGTERM)
[flezha] durable state under /home/you/flezha/app_data/state (set with FLEZHA_STATE_DIR)
[flezha] that directory holds no durable state: this controller is starting cold
[flezha] runners attach with FLEZHA_SOCKET_FILES_PATH=/run/user/1000/flezha
[cluster] formed and ready; members [:"flezha@127.0.0.1"]
NIF transport server (UDS) started on /run/user/1000/flezha/runner_control_channel.sock
```

The first four lines are the boot saying what it chose. They print on every
boot, not only the first: whoever inherits this node later is who they are for.
The cold-start line goes away once the directory holds something. If it comes
back on a controller that had state, read the line above it: you are almost
certainly starting from a different directory, and the data is still in the old
one. `flz controller status` answers the same question at any time.

You do not need to copy the socket path out of it. On one machine as one user a
runner resolves the same directory the controller did, and `flz runner check`
answers the question on demand whenever you do want it.

## Point the CLI at it

`flz` runs the controller's own `flzvm` underneath, so it resolves the node the
same way the controller did and reads the same cookie file: with the same
`FLEZHA_STATE_DIR` and `FLEZHA_COOKIE_FILE` as the controller, or neither, it
needs nothing more. A controller started with `RELEASE_COOKIE` set needs the
same value exported wherever `flz` runs.

Only the controller's start creates that file. A verb run before the first
start, or with a cookie that is not the controller's, reports the controller
unreachable:

```
--rpc-eval : RPC failed with reason :noconnection
```

## Check it came up

Two questions, two commands. The first asks whether the transport is there, and
needs no cookie and no controller:

```bash
$ flz runner check
controller reachable
  directory  /run/user/1000/flezha (from $XDG_RUNTIME_DIR/flezha)
  checked    /run/user/1000/flezha
             /run/flezha
             /srv/flezha/app_data/sockets
```

The second asks whether the controller is actually ready, which is a different
question: the socket is bound early in the boot, well before the controller can
serve anything.

```bash
$ flz boot await
boot await: ready
```

Both exit zero only on success. A controller that is still coming up answers
`recovering` and names what it is waiting for.

## Where a daemon's output goes

A daemon writes nothing to your terminal. Two places carry it:

| What | Where |
|---|---|
| everything the controller logs | `FLEZHA_LOG_PATH`, default `logs/flezha.log` |
| the boot, and anything printed before the logger is up | `$RELEASE_TMP/log/`, default `<release>/tmp/log/` |

The second is inside the build output, so `make build-release` can take it with
it. Point `RELEASE_TMP` somewhere durable on a machine you care about.

## Stop it

```bash
flz controller stop        # graceful: drains, then exits
```

That verb is authorization-gated, so it needs a claimed bootstrap and a token
(see [`../authn_authz.md`](../authn_authz.md)). Before you have one, or if the
controller is wedged, the release script stops it with the cookie alone:

```bash
_build/prod/rel/flezha/bin/flzvm stop
```

A clean stop removes the socket directory's contents. A controller that died
hard leaves the socket behind, and the next start clears it.

## Tune the VM

The release ships its own flags in `_build/prod/rel/flezha/releases/<vsn>/vm.args`,
and on bare metal that file is yours to edit. For one node or one experiment,
setting `ERL_FLAGS` is less trouble and wins over the file either way:

```bash
ERL_FLAGS="+S 4:4" flz controller start --mode daemon
```

Two bare-metal caveats the container pages do not have. `ERL_FLAGS` is read by
every `flzvm` invocation, and every `flz` verb starts one to make its call, so
exporting it in a shell profile enlarges those short-lived nodes as well as the
controller. And a dedicated machine is the one place scheduler binding
(`+stbt`) is worth trying, because it is actively wrong anywhere a CPU quota or
a cpuset can move underneath the VM.

The full list, including the flags deliberately left unset and why, is in
[`../../configuration.md`](../../configuration.md#erlang-vm-arguments).

## Two users on one machine

Directory traversal is the UDS authentication boundary, so a runner run by a
different user cannot reach a controller's socket directory. Two arrangements
work:

- **Same user.** Run the runner as the controller's user. Nothing to configure.
- **A shared group.** Start the controller with
  `FLEZHA_SOCKET_DIR_GROUP=<group>`, a group both users belong to. The socket
  directory becomes `0750` owned by that group, and the controller logs a
  warning naming it on every boot. The boundary is then "that group" instead of
  "one user"; no setting makes the directory reachable by anyone else. A group
  the controller's user does not belong to refuses the boot naming the group.

Point both sides at an explicit `FLEZHA_SOCKET_FILES_PATH` in this arrangement:
`$XDG_RUNTIME_DIR` differs per user, so the zero-configuration path is
same-user only.

## When a runner cannot connect

`flz runner check` runs exactly the probe a runner runs, and names the verdict
and the fix. Every verdict, and the two failures that are not verdicts:
[`../../troubleshooting/runner_cannot_connect.md`](../../troubleshooting/runner_cannot_connect.md).

## A remote runner over QUIC

`FLEZHA_QUIC_ADDRESSES` is a comma list; each entry is a name or a literal
address with an optional port (default `4433`). Every entry is resolved through
DNS on **every** connection attempt, so a controller that moves behind its name
is followed with no reconfiguration, and an entry that fails to resolve is one
failed candidate rather than a fatal error. A runner that has connected once
dials that address first on every later attempt — reconnecting to the same
controller is a binding swap, reconnecting elsewhere strands the previous
controller's operatives until its own convergence loop heals them — and orders
the remaining candidates randomly so a fleet-wide restart does not converge on
one node.

`FLEZHA_QUIC_ADDRESS` is retired and not aliased: set alone it refuses startup
naming the replacement. The controller's own listen address is
`FLEZHA_QUIC_LISTEN_ADDRESS`.

## Several controllers on one machine

A machine may host more than one controller. Every resource a controller owns
is already selected by a variable, so two instances coexist with no code change
and no filename suffix: give each its own values, and each owns a directory
tree and a node name that no other instance touches.

| Variable | What the instance owns |
|---|---|
| `FLEZHA_SOCKET_FILES_PATH` | The socket directory. Also the UDS access boundary, so separate directories give separate permission scopes. |
| `FLEZHA_STATE_DIR` | The per-Cell WAL, snapshot and outbox trees. Claimed exclusively at boot — see below. |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | The durable hub registry. `<root>/<hub_id>/registry.dets` is not keyed by node, so two instances over one root open one file. |
| `FLEZHA_DLQ_DIR` | The parked-record store. |
| `FLEZHA_NODE_NAME` (with `FLEZHA_NODE_HOST`) | The node name. Two BEAMs on one host cannot share one. |
| `FLEZHA_HEALTHZ_PORT` | The probe listener, when `FLEZHA_HEALTHZ_ENABLED` is on. |
| `FLEZHA_QUIC_LISTEN_ADDRESS` | The QUIC listener, when QUIC is enabled. |
| `FLEZHA_AUTHZ_DIR`, `FLEZHA_SCHEDULE_DIR`, `FLEZHA_AUDIT_DIR`, `FLEZHA_PLUGIN_ROOT`, `FLEZHA_LOG_PATH` | The remaining durable trees, where their subsystems are enabled. |

`flz reset` resolves its roots as `<app_data root>/<name>` and does not consult
these per-store overrides, so on a co-located controller it clears nothing that
controller owns. Clear a relocated tree by path, with the controller down.

The unit of separation is the **directory**, not a filename inside a shared
one. A suffix on `runner_control_channel.sock` would let two controllers share
a directory, and the directory is what carries the permission boundary
(`0700`, or `0750` for one named group): sharing it would put both instances
inside one scope and leave each controller's `rm_rf` reaching the other's
sockets. A runner also keeps naming a directory rather than learning an
instance identifier.

Two guards make a missed variable a refusal rather than damage:

- **A live peer's socket directory is refused.** Before preparing its
  directory a controller probes any control socket there — the same probe
  `flz runner check` runs. A peer that answers is live, so the controller
  refuses without removing anything, naming the path, the socket that answered
  and `FLEZHA_SOCKET_FILES_PATH`. A socket nobody answers is a controller that
  died hard, and is cleared as before, so a crash still needs no cleanup.
- **The state root is claimed exclusively.** A controller takes an advisory
  lock on `<state root>/.instance.lock` at boot and holds it for its lifetime.
  Two controllers over one state root are two unsynchronised writers on one
  cell WAL tree, which corrupts it silently; the refusal names the state
  root, the process holding it and `FLEZHA_STATE_DIR`. The kernel releases the
  lock when the holder exits, so a controller that was killed leaves nothing
  to clear.

#### A peer controller is capacity, not durability

A second controller buys capacity and isolation. It is **not** a failover
target, and adding one is not a substitute for the remote durability tier:

- The hub's redundancy strategy is `Singularity` — each operative runs on
  exactly one node, with no replicas — so no peer shadows another's
  operatives.
- Cross-controller state transfer (`Flezha.State.WalTransfer`) is
  destination-driven over `:erpc` against the source node, so a controller that
  died with its disk answers nothing and the adopting controller starts cold.

A controller that dies with its disk takes its operatives' state, and the
registry record of their existence, with it. The mechanism that survives that
is the remote durability tier: see
[`../remote_durability_tier.md`](../remote_durability_tier.md). An operator running
two controllers believing they cover each other is worse off than one running a
single controller with the tier on.

## When a controller refuses to start

Every boot refusal, what it means and its fix, plus a controller that will not
stop:
[`../../troubleshooting/controller_will_not_start.md`](../../troubleshooting/controller_will_not_start.md).
