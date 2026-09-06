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
exactly two of them have no usable default:

| Variable | Why it is not optional here |
|---|---|
| `FLEZHA_DURABILITY_BACKEND` | A prod release refuses to boot with it unset. `none` is the local-only value. |
| `RELEASE_COOKIE` | Without it the release starts with distribution off, and every `flz` verb fails. There is deliberately no committed fallback. |

Everything else has a default that works here. The node name and host, which
together form the node the CLI reaches, default to the release name and
`127.0.0.1` in `rel/env.sh.eex`; set `FLEZHA_NODE_NAME` and `FLEZHA_NODE_HOST`
when you want something else. The socket directory resolves itself, as below.

## Start the controller

```bash
FLEZHA_DURABILITY_BACKEND=none RELEASE_COOKIE=flezha_local \
  _build/prod/rel/flezha/bin/flzvm start
```

It runs in the foreground and logs its boot. The line that tells a runner where
to attach is in there:

```
[flezha] runners attach with FLEZHA_SOCKET_FILES_PATH=/run/user/1000/flezha
```

## Point the CLI at it

`flz` runs the controller's own `flzvm` underneath, so it resolves the node the
same way the controller did and needs only the cookie:

```bash
export RELEASE_COOKIE=flezha_local
```

Without the cookie every verb stops before it starts, and says so:

```
[flezha] RELEASE_COOKIE is not set; refusing to start Erlang distribution.
```

## Check it came up

Two questions, two commands. The first asks whether the transport is there, and
needs no cookie and no controller:

```bash
$ _build/prod/rel/flezha/bin/flz runner check
controller reachable at /run/user/1000/flezha (from $XDG_RUNTIME_DIR/flezha); walked
/run/user/1000/flezha, /run/flezha, ./app_data/sockets. Fix: nothing to fix - the
controller answered
```

The second asks whether the controller is actually ready, which is a different
question: the socket is bound early in the boot, well before the controller can
serve anything.

```bash
$ _build/prod/rel/flezha/bin/flz boot await
boot await: ready
```

Both exit zero only on success. A controller that is still coming up answers
`recovering` and names what it is waiting for.

## Stop it

```bash
_build/prod/rel/flezha/bin/flzvm stop
```

A clean stop removes the socket directory's contents. A controller that died
hard leaves the socket behind, and the next start clears it.

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

`flz runner check` runs exactly the probe a runner runs — no runner code, no
running runner, no controller needed — and prints the resolved directory, the
candidates walked, the verdict and the fix. It exits zero only when a
controller answered. A runner prints the same diagnosis once, before it starts
retrying; the terse line that repeats every backoff is the retry notice, not
the diagnosis.

| Verdict | What it means | Fix |
|---|---|---|
| `socket directory absent` | no controller has ever used this path | start the controller, or set `FLEZHA_SOCKET_FILES_PATH` to the directory it established |
| `control socket absent` | the directory is there, the controller is not | start it, or wait for it to finish booting |
| `access denied` | the socket directory is the access boundary and this user is outside it — the diagnosis names the directory's owning uid and mode and the caller's uid | run the runner as the owning user, or use the group mode above |
| `stale control socket` | a socket file nobody is listening on: a controller that died hard | clears on the next controller start |
| `more than one controller is listening` | two ladder candidates hold a live socket and none was named | set `FLEZHA_SOCKET_FILES_PATH` to the one this runner should attach to |

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
| `FLEZHA_STATE_DIR` | The per-Entity WAL, snapshot and outbox trees. Claimed exclusively at boot — see below. |
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
  entity WAL tree, which corrupts it silently; the refusal names the state
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

| Refusal | What it means | Fix |
|---|---|---|
| `… is another controller's socket directory` | a live controller answered on the control socket in the directory this one was told to prepare | give this controller its own `FLEZHA_SOCKET_FILES_PATH`, or stop the controller listening there. Nothing was removed |
| `state root … is already claimed (held by pid N)` | another controller holds the advisory lock on that state root | give this controller its own `FLEZHA_STATE_DIR`, or stop pid N. If the holder is gone, the lock is gone with it — the message names a live process |
| `no socket directory could be created` | no ladder candidate was writable | set `FLEZHA_SOCKET_FILES_PATH` to a directory this user can write |
| `FLEZHA_SOCKET_DIR_GROUP=<group>: no such group` | the group does not exist, or this controller's user is not in it | use a group both users belong to, or drop the variable for owner-only |
