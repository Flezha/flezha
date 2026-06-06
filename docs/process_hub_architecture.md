# ProcessHub in Flezha

How the controller uses the [`process_hub`](https://hex.pm/packages/process_hub) library to
distribute relays across nodes, plus how this layer interacts with the
executor (Rust/Java SDK) connection model.

This document is a reference for anyone designing features that touch relay
placement, restart, recovery, migration, or the executor/controller connection
lifecycle. Read it before proposing changes in those areas — most of the
behaviour described here is already automatic, and the most common mistake is
to assume Flezha owns logic that ProcessHub already provides.

> **READ FIRST: `docs/resilience/process_hub_recovery_model.md`.** The live, cluster-synced
> ProcessHub registry is ALWAYS the source of truth for which relays run where;
> DETS is a disk backup used ONLY to cold-start a fully-downed cluster. A node
> rejoining a live cluster syncs from peers and MUST NOT trust its local DETS.
> Treating DETS as the authority causes split-brain, stale-state crashes, and
> dishonest health reporting.

## Two-level distribution

Flezha distributes relays at two independent levels:

1. **Cluster level — ProcessHub.** ProcessHub picks which controller **node** owns
   each relay, using a per-namespace consistent hash ring. Controller nodes
   running the same namespace cluster together via Erlang distribution +
   ProcessHub's coordinator.
2. **Within-controller level — executor selection.** Once an controller owns a relay,
   it picks one of its locally-connected executors (Rust/Java SDK clients) to
   spawn the relay on. Today this pick is `Enum.random` filtered by factory
   support (`lib/flezha/service/relay_manager.ex`).

The two levels are decoupled. ProcessHub knows nothing about executors; the
controller's `alter_child_data` hook handles executor selection at relay start
time.

> **Note (relay-mvp-rust).** The Rust SDK now ships work as
> `Relay` / `Shard` / `Partition` (see `docs/relay_architecture.md`).
> Partitioning is **SDK-internal**: a single Relay instance can have many
> Shards, each with many Partitions running in parallel inside the
> executor process. ProcessHub continues per-relay placement at the
> cluster level — one Relay instance is one ProcessHub child — and is
> not aware of partitions. The two-level distribution above is unchanged.

## Per-namespace ProcessHub instances

Each controller runs **one ProcessHub instance per namespace** it serves. The hub
id is derived from the namespace via `Flezha.Identificator.hub/1`. Controllers
serving namespace A and controllers serving namespace B are in completely
independent ProcessHub clusters — work is never migrated between unrelated
namespaces.

This means:

- An controller may serve multiple namespaces; it has multiple ProcessHub instances.
- "Peer controllers" in any reasoning about migration / failover always means
  **other controllers serving the same namespace**, not just other controllers in the
  Erlang cluster.
- A namespace dies entirely when the last controller serving it leaves — there is
  nowhere to migrate to.

The hub spec lives in `lib/flezha/service/hub_manager.ex`:

```elixir
ProcessHub.child_spec(%ProcessHub{
  hub_id: Identificator.hub(namespace),
  synchronization_strategy: %ProcessHub.Strategy.Synchronization.PubSub{},
  redundancy_strategy: %ProcessHub.Strategy.Redundancy.Singularity{},
  partition_tolerance_strategy: %ProcessHub.Strategy.PartitionTolerance.Divergence{},
  distribution_strategy: %ProcessHub.Strategy.Distribution.ConsistentHashing{},
  migration_strategy: %ProcessHub.Strategy.Migration.HotSwap{handover: true},
  cross_node_request_timeout: 10_000,
  hooks: %{...}
})
```

## Strategies in use

| Concern | Strategy | Effect |
|---|---|---|
| Distribution | `ConsistentHashing` | `relay_id` hashes to a node on the ring. Adding/removing a node reshuffles only the affected slice. |
| Synchronization | `PubSub` | Registry changes (process starts/stops) are broadcast to all nodes. Each node has the same view of "which relay lives where". |
| Migration | `HotSwap{handover: true}` | On graceful node leave, relay state is captured on the source, shipped to the target, and only then is the source process terminated. See [HotSwap with handover](#hotswap-with-handover) below. |
| Redundancy | `Singularity` | Each relay runs on exactly one node. No replicas. |
| Partition tolerance | `Divergence` | Each network partition independently redistributes. On heal, the larger partition's state wins. |

## Hooks Flezha registers

Flezha plugs into ProcessHub via two hooks (`lib/flezha/service/hub_manager.ex`):

- `child_data_alter` — fires when ProcessHub is about to start a relay on
  this node. Flezha rewrites the child_spec to bake in the assigned
  `executor_id`, picked by `RelayManager.assign_executor(namespace,
  factory_id)`. Raises if no executor declaring that factory is currently
  connected. **Net effect:** every relay restart re-runs executor selection.
- `child_registered` → `hook_insert_pid` — fires after a relay has started.
  Flezha pushes the new local PID to all stream channels of the assigned
  `executor_id` so subsequent I/O routes to the right process.

## Lifecycle scenarios

### Graceful node leave (rolling DaemonSet update, normal shutdown)

1. ProcessHub's coordinator on the leaving node observes its own departure
   intent.
2. For each relay on this node, `HotSwap` queries `prepare_handover_state/1`
   (Flezha overrides this in `Overwatch`) — the live process returns its
   current state, including any captured rust/java state binary.
3. The state is stashed in ETS with a TTL on the source node.
4. ProcessHub picks a destination node (consistent-hash recompute on the new
   ring without this node) and fires the start request there.
5. The destination node starts the relay; before it accepts I/O, ETS
   handover delivers the saved state via `alter_handover_state/2`.
6. Only after handover completes does the source process terminate.
7. The destination's `alter_child_data` hook picks a local executor — possibly
   different from the one that hosted the relay on the source node.

Net effect: graceful failover preserves relay + plugin state. Executors on
the leaving node lose their work (it moved). Executors on the destination node
gain the work.

### Ungraceful node loss (crash, OOM, kill -9, network partition that holds)

1. Other coordinators detect `:nodedown` via Erlang's `:net_kernel.monitor_nodes(true)`.
2. ProcessHub batches the departure (small window to absorb flaps), then
   redistributes affected relays across surviving nodes.
3. No state handover occurs — the source died before any capture.
4. Destination nodes start relays fresh; `alter_child_data` picks executors.
5. Per-relay periodic state snapshots (see [State snapshots](#state-snapshots-for-handover-and-restart))
   provide a "last-known good" state if the destination's relay restart
   path consults them.

Net effect: state is lost (or rolled back to the last periodic snapshot,
depending on the relay's logic), but the relays continue to run.

### Node returns to the cluster

When a previously-down node rejoins, ProcessHub's cluster-join handler
recalculates the consistent hash. Depending on the resulting ring shape,
relays **may or may not** move back to the rejoining node. There is no
explicit "snap back" — placement is purely a function of the hash ring.

When ProcessHub does decide to move a relay to the rejoining node, it goes
through the standard `child_data_alter` path on that node, which picks a
locally-connected executor.

### Executor restart (SDK process dies and comes back)

ProcessHub doesn't see this — it's invisible at the cluster layer. Handling
lives in `FleetManager` (`lib/flezha/executor/fleet_manager.ex`):

- Branch A — other executors in this namespace on this controller: reroute the dead
  executor's relays to a peer executor (via `executor_reroute` →
  `Overwatch.executor_migrate` → `start_relay_sdk` → `AddRelay`
  protobuf to the surviving executor).
- Branch B — no executors here, but other controllers serve this namespace:
  `hub_migrate` tears down the local namespace hub. ProcessHub redistributes
  its slice to peer controllers. Returning executor gets new work next time
  ProcessHub places something on it.
- Branch C — last executor in the namespace cluster: mark `:inactive` with a
  timestamp. If the executor reconnects within `executor_keepalive`
  (default 30 s, env var `FLEZHA_OPERATOR_KEEPALIVE`),
  `try_migrate_executor` reroutes the preserved work to the new
  `executor_id`.

## Connection-healing contracts (post 0.2.x)

Two contracts make executor ↔ controller connections heal automatically across
either-side restarts:

1. **Stream rebind on `{:flz_closed, ...}`.** `StreamChannel` clears its
   `stream_ref` to `nil` synchronously inside its close-handler clause, and
   refuses outbound frames while `stream_ref == nil` (returns
   `{:error, :no_stream}` on the call path; logs and drops on the cast path).
   On the next incoming data stream for the same `executor_id`,
   `StreamAcceptor` (UDS) and `Transport.Server` (QUIC) walk the existing
   `StreamSup` children and bind the new ref to the first nil-ref channel via
   an atomic `GenServer.call(channel, {:bind, new_ref})`. A channel with a
   live ref returns `:already_bound`, so the acceptor moves to the next
   candidate without ever silently overwriting an in-flight binding.
2. **AddRelay reissue on reconnect.** After the existing
   `try_migrate_executor` step, `FleetManager.executor_connect/3`
   calls `reissue_local_orphans/5`, which queries
   `HubManager.get_registry/1`, filters to relays hosted on `node()` whose
   `:tag` metadata names a no-longer-active `executor_id` and whose
   factory the connecting executor advertises, and re-issues `AddRelay` to
   the new executor via `Overwatch.executor_migrate` →
   `start_relay_sdk`. Relays already handled by the migrate step are
   skipped via the `claimed_relay_ids` set so no SDK ever receives a
   duplicate `AddRelay` it would have to handle as a no-op.

The Rust and Java SDK `AddRelay` handlers are idempotent on duplicate
`relay_id`: they log
`"AddRelay: relay <id> already registered, skipping"` and return,
preserving the existing instance's state.

## HotSwap with handover

The migration strategy is the most subtle piece. `HotSwap{handover: true}`
implements stateful migration in a strict order:

1. Source node receives a "process X moving to node Y" instruction.
2. Source sends `{:process_hub, :query_hot_handover_state, ...}` to the live process.
3. Process replies `{:process_hub, :hotswap_state, child_id, prepared_state}`.
4. Source stashes the state in an ETS table with a short TTL.
5. Source sends a normal start request to the target node.
6. Target node starts the process. Before the new process is added to the
   registry, a post-action callback fires that asks the source for the saved
   state.
7. Source delivers `{:process_hub, :hotswap_handover, child_id, state}` to the
   new process.
8. New process consumes the state via `alter_handover_state/2`.
9. Only now does the source terminate the old process.

Flezha relays `use ProcessHub.Strategy.Migration.HotSwap` in
`lib/flezha/relay/overwatch.ex` and override the handover callbacks to
ship rust/java state binaries alongside the Elixir state.

If handover fails (target unreachable, timeout, callback error), the source
process is NOT terminated — the existing process keeps running. The migration
may be retried later.

## State snapshots for handover and restart

State persistence is split between two layers:

* **HotSwap migration** uses an in-memory snapshot captured by
  `prepare_handover_state` and consumed by `alter_handover_state/2` on
  the destination — see `lib/flezha/relay/overwatch.ex`.
* **Cold-boot restart** is covered by the per-Partition WAL/snapshot
  files in `app_data/state/{relay_class}/{shard_id}/...`
  (`docs/resilience/state_persistence.md`) plus ProcessHub's persistent
  registry under `app_data/process_hub/{hub_id}/registry.dets`. There is
  no periodic in-memory snapshot loop and no `flz cluster snapshot` CLI.

## Executor → controller discovery and connection

(Documented here because it's adjacent to ProcessHub-driven assignment.)

- Each executor (Rust/Java SDK) connects to **exactly one controller**. There is no
  multi-controller connection.
- Discovery is filesystem co-location, not service discovery. In Kubernetes
  the `flezha-sockets` hostPath volume is mounted into BOTH the controller pod and
  the executor pod, so the executor opens the local controller's UDS socket via
  `FLEZHA_SOCKET_FILES_PATH` (default `/flezha/sockets`).
- For QUIC transport: the executor dials `FLEZHA_QUIC_ADDRESS`.
- Reconnect loop: exponential backoff `100ms * 2^n` capped at 30 s; retries
  forever; never tries a different controller.
- Stream architecture: one bidirectional control channel for handshake +
  protobuf commands, plus N data streams opened on demand for relay I/O.
  Data streams are routed by `StreamAcceptor` round-robin to `StreamChannel`
  processes.

## What does NOT exist (anti-patterns to avoid in design)

These are common assumptions to double-check against the actual code:

- **Auto-restore-on-boot exists** via ProcessHub's durable registry +
  per-Partition WAL/snapshot files. See `docs/resilience/auto_restore_on_boot.md`
  for the full safety analysis. Anything beyond that boundary
  (out-of-cluster portable backups, point-in-time-to-arbitrary-past)
  is out of scope.
- **No persistent FleetManager state.** `executor_namespaces` and
  `namespace_factories` live only in the FleetManager GenServer. Restarting
  the controller process loses them entirely; ProcessHub's
  `pre_recovery_replay` hook waits for FleetManager to repopulate
  before replay dispatches `start_children`.
- **No persistent executor → relay assignment.** Assignment is recomputed
  every time `child_data_alter` fires; nothing is stored.
- **No multi-controller discovery in the SDK.** An executor does not failover to
  another controller if its local one is gone — it waits forever for the local one
  to return.

## Where the code lives

| Concern | File |
|---|---|
| Hub spec, hooks, alter_child_data | `lib/flezha/service/hub_manager.ex` |
| Executor selection (random within namespace+factory) | `lib/flezha/service/relay_manager.ex` |
| FleetManager (tracks executors, factories, disconnect branches) | `lib/flezha/executor/fleet_manager.ex` |
| Relay wrapper, HotSwap callbacks | `lib/flezha/relay/overwatch.ex` |
| Control channel (executor handshake, EOF detection) | `lib/flezha/executor/control_channel.ex` |
| Stream channels, attach/detach, frame routing | `lib/flezha/executor/stream_channel.ex` |
| UDS stream acceptance and round-robin | `lib/flezha/socket/stream_acceptor.ex` |
| Rust SDK reconnect loop | `sdk/rust/src/executor/control_unit.rs` |
| Rust SDK reconnect handler | `sdk/rust/src/executor/{fast,legacy}.rs` `handle_reconnect` |

## Plugin subscriptions persistence

The ProcessHub child-spec is the source of truth for a relay's
plugin subscriptions, and ProcessHub's **persistent registry** (the
`durable-registry` capability with `registry_backend: {:dets, ...}`)
makes that source of truth survive any controller restart.
`relay.opts[:plugins]` (embedded inside the cspec start args) carries
the authoritative list; the runtime `Overwatch.state.extensions`
table is a derived projection rebuilt from it on every `init/1`.

`Flezha.Service.PluginManager.enable_plugin/2` and `disable_plugin/1`
follow a registry-write-first ordering: the cspec is rewritten via
`ProcessHub.Service.ProcessRegistry.update/3` (durably persisted to
DETS), and only after that succeeds is the runtime
`:plugin_subscribe` / `:plugin_unsubscribe` message dispatched to the
Overwatch. If the registry write fails, the call returns
`{:error, :registry_unavailable}` without mutating the runtime — the
caller can retry. As a result every Overwatch (re)start path — cold
start, ProcessHub redistribution, in-place restart from
`alter_child_data` rewriting start args, controller process restart
(or full cluster cold boot, since the persistent registry survives),
and HotSwap migration — restores the same plugin set without executor
intervention.

This persistence layer is independent of the per-Partition WAL +
snapshot persistence in `lib/flezha/state/` (`docs/resilience/state_persistence.md`):
the cspec persists *which* plugins are subscribed; the per-Partition
layer persists *how much progress* a Partition has made through its
input stream. Both layers can be present, absent, or partially
populated for a given relay without affecting one another.

## RestoreInitState vs executor_migrate

The per-Partition `RestoreInitState` handshake (`relay-state-persistence-controller`)
and the existing `executor_migrate` flow are **mutually exclusive
per Partition**. A reconnecting Partition with `persistence_enabled =
true` and on-disk WAL/snapshot is restored via `RestoreInitState` only.
A migrating Partition (HotSwap moving the Overwatch to a peer controller
host) carries its own snapshot bytes through the migration path; the
new controller does NOT additionally fire `RestoreInitState` for the same
Partition. Doing both would double-apply state.

The persistence module (`lib/flezha/state/`) is per-Partition and
ProcessHub-agnostic. ProcessHub picks the controller host; the persistence
module owns the per-Partition WAL on whichever host the Overwatch is
currently running on. Migrating an Overwatch to a peer host moves the
in-memory state via `executor_migrate`; the new host's
persistence module starts a fresh WAL (no replication is in scope for
v1).

## Related docs

- `docs/sdk_protocol_spec.md` — wire-level binary framing and protobuf commands.
- `docs/resilience/state_persistence.md` — per-Partition WAL, snapshot, and time-travel.
- `docs/resilience/auto_restore_on_boot.md` — the single auto-restore mechanism (ProcessHub durable registry + per-Partition WAL).
- `docs/nif_architecture.md` — transport NIF design (UDS + QUIC drivers).
- `docs/deployment.md` — k8s DaemonSet layout, env vars, hostPath volumes.
