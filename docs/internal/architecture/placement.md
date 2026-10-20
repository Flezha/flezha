# Placement

Who decides where a relay, a partition member, a controller plugin's home and a namespace hub
run, the evidence each decision reads, and how two deciders that can act on one child at once
are kept apart. Written for contributors who change one of them. What a user sees is in
`docs/public/architecture/overview.md` and `docs/public/topics/core/partitioning.md`; what a
relay does once placed is [`relay_lifecycle.md`](relay_lifecycle.md).

**Contents**

1. [Two choices](#two-choices)
2. [The deciders](#the-deciders)
3. [Hub membership](#hub-membership)
4. [Deciders that can act on one child at once](#deciders-that-can-act-on-one-child-at-once)
5. [Open edges](#open-edges)

## Two choices

A relay is placed twice. ProcessHub picks the **controller node** its Overwatch runs on, with
`Flezha.Cluster.GroupedHashing` as the hub's distribution strategy. The Overwatch then binds a
**runner** on that node, which `Flezha.Runner.FleetManager.resolve_runner/4` picks. The two
choices meet in one table: the nodes that serve a factory, which the fleet manager declares into
the hub and both the hub and the convergence loop read.

## The deciders

### Which controllers may host a factory

`Flezha.Runner.FleetManager.Announce` keeps the served table. `Announce.sync/2` declares into
each hub the factories this controller can place, read by
`Flezha.Runner.FleetManager.Factories.placeable_factories/2`: its active runners advertising the
factory, narrowed by the attributes a placement must match (`placeable_runners/3`). An addition
is declared at once. A factory the controller stops serving is withdrawn after
`group_withdraw_grace_ms`, or at once when the controller asked for the disconnect
(`Announce.withdraw/3`). A hub that starts is declared from scratch.

### Which controller runs a relay: the hub

`GroupedHashing.belongs_to/4` answers the nodes for a child. A relay's row carries its factory
as its group (`Flezha.Service.HubManager.Placement`'s `stamp_group/2`, written with `put_new` so
a placement decision is never rewritten); the child is placed on a hash ring over
`GroupedHashing.group_nodes/2`, the nodes serving that factory. When no node serves the group,
the child stays on the nodes it is registered on. A child without a group, a plugin home, is
placed on a ring over every node of the hub. A start naming a group nobody serves is refused
with `{:no_group_members, _}` (`children_init/4`). A change of the table re-runs the hub's
node-up redistribution (`recheck/1`).

### Which runner a relay binds

The hub's alter hook (`Flezha.Service.HubManager.Placement.alter_child_data/2`) keeps the
recorded runner on a placed row and waits for nothing. The Overwatch's own bind resolves the
runner: `Flezha.Relay.Lifecycle.bind/3` calls
`Flezha.Service.RelayManager.Placement.await_runner/5`, which asks
`FleetManager.resolve_runner/4` until a ready runner answers or `overwatch_runner_wait_ms` runs
out, and then holds the relay unbound. A stop from the Overwatch's supervisor that arrives
between polls ends the wait as that stop, so the relay's teardown still runs. The fleet manager filters its runners with
`placeable_runners/3`, ranks them by a rendezvous hash of the relay id and caps each runner's
share by load, in one call (`Flezha.Runner.FleetManager.Load.place/3`). A wake
(`Flezha.Relay.Suspend.wake/2`) and the loop's rebind (`Flezha.Convergence.Heal`) take a runner
from `RelayManager.Placement.assign_runner/3`.

### Which controller runs a call

`Flezha.Service.RelayManager` runs a stop, a pid lookup or a start on this node when a hub of
the namespace is alive here. Otherwise it forwards the call to a member of the hub
(`on_hub_member/5`) or, for a start, to a node whose runners advertise the factory
(`route_start/3`), picked by `RelayManager.Placement.pick_runner/2`. The node lists come from the
`:pg` groups of `Flezha.Cluster.Directory`, which are eventually consistent; the node that
receives the call checks again. A start no node can place is refused with the reason
`Flezha.Service.RelayManager.Admission.cluster_wide_refusal/2` gives.

### The convergence loop

`Flezha.Convergence.Probe` reads, per namespace and factory, the local runners and the other
nodes serving the group (`GroupedHashing.group_nodes/2`). `Flezha.Convergence.Diff` does not plan
a bind for an unbound or failed relay whose factory no local runner serves while another node
does (`classify_bindable/4` answers `:skip`): the hub's move is the remedy there. Otherwise it
plans a rebind, which `Flezha.Convergence.Heal` sends as `{:rebind, target}`. While the node
drains, a `{:node_draining, ...}` fact empties every placing field of the plan
(`suppress_placing/1`).

### Partition groups

`Flezha.Relay.PartitionGroup` (`lib/flezha/service/partition_group.ex`) starts and rescales a
group's members, each through `RelayManager.start_relay/3`, under one cluster-wide lock per group
(`:global.trans/4` in `under_group_lock/5`). A rescale takes the lock and never waits; a start retries on the
`:no_response` backoff row until its deadline and is then refused with
`{:rescale_running, namespace, group}`.

### Controller plugins

A plugin's home is a hub child without a group (`Flezha.Plugin.Home`). The home asks every node
of the hub to place the plugin there (`Flezha.Plugin.Shared.place/2` decides what runs: a copy
per node, or the one copy of a `single` plugin) and stops copies on nodes that left the hub. A copy watches its home; when
the home goes down it reads the home's row again and follows a live home other than the one
that went, and stops only when none runs (`Flezha.Plugin.Shared`'s `moved_home/2`).

### Draining a node

The `:node` walk's drain row moves every local relay through ProcessHub's migration with the
consent gate (`Flezha.Relay.Handover.consent/1`), then waits for the local children to exit
(`Flezha.Service.HubManager.Lifecycle.drain_hub/2`).

## Hub membership

A node starts a namespace's hub when a runner of the namespace connects
(`Flezha.Runner.FleetManager.Session` through `HubManager.Lifecycle.ensure_hub/1`), and when a
plugin is enabled in a namespace with no hub here (`Flezha.Service.PluginManager`'s `hub_for/1`).

It leaves the hub through `Flezha.Service.HubManager.stop_hub_if_vacant/1`, asked by the loop
for a namespace with no active runner (`Flezha.Convergence.Loop`'s `reclaim_vacant_hubs/1`) and
by the fleet manager's runner cleanup; the fleet manager decides
(`Flezha.Runner.FleetManager.reclaim_vacant_hub/1`), so no runner connect lands between the
check and the stop. A vacancy leave keeps the registry directory. `HubManager.stop_hub/1`, the
namespace stop's row, is unguarded and wipes it.

`Flezha.Service.HubManager.holds_anything?/1` decides whether the member stays. It stays while it
runs a relay here or while the hub's recovery has not settled. It also stays while the hub
declares anything or it runs any child here, a plugin home included, unless another member
holds the hub instead: another live member runs a child of it, or no member runs one and this
node is not the leader of the declared list.

Each hub's orphan reconcile (ProcessHub's recovery rounds, configured by
`HubManager.Lifecycle.auto_recovery_config/0`) starts a declared child that runs nowhere and stops
a running child whose declared entry was removed; an undeclared child is stopped only after two
rounds agree.

## Deciders that can act on one child at once

| # | Pair | Child | Guard | Where |
|---|---|---|---|---|
| 1 | Loop rebind × session swap | relay | `Flezha.Relay.Rebind.rebind/2` answers `:busy` while a bind task runs, and the heal counts it unmet. A swap to a restarted runner leaves the binding to the bind under way; a reconnect re-points the binding whatever runs (see open edges) | `Rebind.rebind/2`, `Rebind.session_replaced/4` |
| 2 | Loop rebind × the Overwatch's own bind or wake | relay | serialized in the Overwatch: the first bind runs in `handle_continue`, so a rebind call waits behind it; a wake during a bind joins it; a bound result is not adopted when the phase changed or a halt or park voided the bind (`Rebind.void_bind/1`) | `Rebind.apply_bind_result/2`, `Suspend.wake/2` |
| 3 | Loop rebind × hub move | relay | partial: the plan skips only the withdrawal case (no local runner, another node serves). A table change that adds a server while local runners exist starts the hub's redistribution with no loop suppression | `Diff`'s `classify_bindable/4`, `GroupedHashing`'s `recheck/1` |
| 4 | Loop placing × drain | relay | `{:node_draining, ...}` empties the placing fields; a tick already running finishes its plan, and binds the Overwatch starts itself are not suppressed | `Diff`'s `suppress_placing/1` |
| 5 | Orphan reconcile × user stop | relay, home | the stop's precommit removes the declared entry before anything terminates, so the reconcile cannot restart it | `Flezha.Service.RelayManager.Teardown.hub_stop_children/3` |
| 6 | Orphan reconcile × rescale | partition member | the rescale's stop (`cause: :rescale`) removes the entries before it restarts the same ids | `PartitionGroup` |
| 7 | Group start × rescale | partition group | one `:global` lock per group; a start waits for a running rescale within its bound | `PartitionGroup`'s `under_group_lock/5` |
| 8 | Rescale quiesce × loop plugin heal | member bindings | the rescale removes a binding from the child spec before unsubscribing, and the loop's intent reads bindings from the child spec, so there is nothing to heal | `Flezha.Service.PluginManager`, `Flezha.Convergence.Intent` |
| 9 | Reclaim × runner connect | hub | both decided inside the fleet manager, which counts handshakes in progress as occupants | `FleetManager.reclaim_vacant_hub/1` |
| 10 | Reclaim × plugin enable in a namespace with no hub | hub | partial: once the entry is declared, `holds_anything?/1` keeps the hub; between the hub's start and the declare only an unsettled recovery keeps it, since the enable runs in its caller | `PluginManager`'s `hub_for/1` |
| 11 | Reclaim × the hub placing a home here | home | `holds_anything?/1` counts homes; a node holding only a home still leaves when another member runs a child, and a home start in flight is not counted | `HubManager.holds_anything?/1` |
| 12 | Old home × new home after a move | plugin copy | the copy follows the live home the row names when its home goes down, and stops only when none runs | `Shared`'s `moved_home/2` |
| 13 | User stop or disable × a peer leaving | relay, home | a stop that got no answer is asked again within the caller's budget; a child a retry no longer finds counts as stopped | `Teardown.hub_stop_children/3`, `Teardown.hub_stop_child/3` |
| 14 | Session swap × hub move | relay | no guard of its own: the swap casts to the old Overwatch, and the loop's stale-tag and misbound heals repair the binding | `Flezha.Runner.FleetManager.Session` |
| 15 | Concurrent starts × quota | namespace | the `:start` walk's `:reserve` row checks and counts in one call to this controller's `Flezha.AuthZ.QuotaCounters`, held until `Admission.settle_start/2` or the caller's exit | `Admission`'s `reserve_step/2` |

## Open edges

Read from the code, not reproduced by a test:

- Pair 1: a reconnect re-points the binding (`Lifecycle.commit_binding/2`) without looking at a
  bind task, and the task's result then publishes over it.
- Pair 15: the reservation lives in the controller that runs the start. Two starts of one
  namespace on two controllers are not serialized by it; the relay count each reads is refreshed
  from the hub's relays (`Admission.check_relay_quota/3`).
- Hub membership: with no member running a child, only the declared list's leader keeps the hub;
  two members that see different node sets could name different leaders.
