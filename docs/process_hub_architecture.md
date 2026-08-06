# ProcessHub in Flezha

How the controller uses the [`process_hub`](https://hex.pm/packages/process_hub) library to
distribute operatives across nodes, plus how this layer interacts with the
runner (Rust/Java SDK) connection model.

This document is a reference for anyone designing features that touch operative
placement, restart, recovery, migration, or the runner/controller connection
lifecycle. Read it before proposing changes in those areas — most of the
behaviour described here is already automatic, and the most common mistake is
to assume Flezha owns logic that ProcessHub already provides.

> **READ FIRST: `docs/resilience/process_hub_recovery_model.md`.** The live, cluster-synced
> ProcessHub registry is ALWAYS the source of truth for which operatives run where;
> DETS is a disk backup used ONLY to cold-start a fully-downed cluster. A node
> rejoining a live cluster syncs from peers and MUST NOT trust its local DETS.
> Treating DETS as the authority causes split-brain, stale-state crashes, and
> dishonest health reporting.

## Two-level distribution

Flezha distributes operatives at two independent levels:

1. **Cluster level — ProcessHub.** ProcessHub picks which controller **node** owns
   each operative, using a per-namespace consistent hash ring. Controller nodes
   running the same namespace cluster together via Erlang distribution +
   ProcessHub's coordinator.
2. **Within-controller level — runner selection.** Once an controller owns an operative,
   it picks one of its locally-connected runners (Rust/Java SDK clients) to
   spawn the operative on. Today this pick is `Enum.random` filtered by factory
   support (`lib/flezha/service/operative_manager.ex`).

The two levels are decoupled. ProcessHub knows nothing about runners; the
controller's `alter_child_data` hook handles runner selection at operative start
time.

> **Note (operative-mvp-rust).** The Rust SDK now ships work as
> `Unbounded Operative` / `Procedure` / `Entity` (see `docs/operative_architecture.md`).
> Entity is **SDK-internal**: a single Unbounded Operative instance can have many
> Procedures, each with many Entities running in parallel inside the
> runner process. ProcessHub continues per-operative placement at the
> cluster level — one Unbounded Operative instance is one ProcessHub child — and is
> not aware of entities. The two-level distribution above is unchanged.

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
| Distribution | `ConsistentHashing` | `operative_id` hashes to a node on the ring. Adding/removing a node reshuffles only the affected slice. |
| Synchronization | `PubSub` | Registry changes (process starts/stops) are broadcast to all nodes. Each node has the same view of "which operative lives where". |
| Migration | `HotSwap{handover: true}` | On graceful node leave, operative state is captured on the source, shipped to the target, and only then is the source process terminated. See [HotSwap with handover](#hotswap-with-handover) below. |
| Redundancy | `Singularity` | Each operative runs on exactly one node. No replicas. |
| Partition tolerance | `Divergence` | Each network partition independently redistributes. On heal, the larger partition's state wins. |

## Hooks Flezha registers

Flezha plugs into ProcessHub via two hooks (`lib/flezha/service/hub_manager.ex`):

- `child_data_alter` — fires when ProcessHub is about to start an operative on
  this node. Flezha rewrites the child_spec to bake in the assigned
  `runner_id`, picked by `OperativeManager.assign_runner(namespace,
  factory_id)`. Raises if no runner declaring that factory is currently
  connected. **Net effect:** every operative restart re-runs runner selection.
- `child_registered` → `hook_insert_pid` — fires after an operative has started.
  Flezha pushes the new local PID to all stream channels of the assigned
  `runner_id` so subsequent I/O routes to the right process.

## Lifecycle scenarios

### Graceful node leave (rolling DaemonSet update, normal shutdown)

1. ProcessHub's coordinator on the leaving node observes its own departure
   intent.
2. For each operative on this node, `HotSwap` queries `prepare_handover_state/1`
   (Flezha overrides this in `Overwatch`) — the live process returns its
   current state, including any captured rust/java state binary.
3. The state is stashed in ETS with a TTL on the source node.
4. ProcessHub picks a destination node (consistent-hash recompute on the new
   ring without this node) and fires the start request there.
5. The destination node starts the operative; before it accepts I/O, ETS
   handover delivers the saved state via `alter_handover_state/2`.
6. Only after handover completes does the source process terminate.
7. The destination's `alter_child_data` hook picks a local runner — possibly
   different from the one that hosted the operative on the source node.

Net effect: graceful failover preserves operative + plugin state. Runners on
the leaving node lose their work (it moved). Runners on the destination node
gain the work.

### Ungraceful node loss (crash, OOM, kill -9, network partition that holds)

1. Other coordinators detect `:nodedown` via Erlang's `:net_kernel.monitor_nodes(true)`.
2. ProcessHub batches the departure (small window to absorb flaps), then
   redistributes affected operatives across surviving nodes.
3. No state handover occurs — the source died before any capture.
4. Destination nodes start operatives fresh; `alter_child_data` picks runners.
5. Per-operative periodic state snapshots (see [State snapshots](#state-snapshots-for-handover-and-restart))
   provide a "last-known good" state if the destination's operative restart
   path consults them.

Net effect: state is lost (or rolled back to the last periodic snapshot,
depending on the operative's logic), but the operatives continue to run.

### Node returns to the cluster

When a previously-down node rejoins, ProcessHub's cluster-join handler
recalculates the consistent hash. Depending on the resulting ring shape,
operatives **may or may not** move back to the rejoining node. There is no
explicit "snap back" — placement is purely a function of the hash ring.

When ProcessHub does decide to move an operative to the rejoining node, it goes
through the standard `child_data_alter` path on that node, which picks a
locally-connected runner.

### Runner restart (SDK process dies and comes back)

ProcessHub doesn't see this — it's invisible at the cluster layer. Handling
lives in `FleetManager` (`lib/flezha/runner/fleet_manager.ex`):

- Branch A — other runners in this namespace on this controller: reroute the dead
  runner's operatives to a peer runner (via `runner_reroute` →
  `Overwatch.runner_migrate` → `start_operative_sdk` → `AddOperative`
  protobuf to the surviving runner).
- Branch B — no runners here, but other controllers serve this namespace:
  `hub_migrate` **drains** the local namespace hub
  (`HubManager.drain_hub/2` → `ProcessHub.Service.Migration.drain/2`), so each
  operative leaves through the migration-consent gate with a live HotSwap
  handover rather than being killed with the hub. Because the runner that would
  have serialized the state is the one that just died, the handover is empty and
  the destination pulls the Entity files from this node
  (`Flezha.State.WalTransfer`) — `state_root` is node-local and never assumed
  shared. The hub is reclaimed afterwards only if it ends up vacant. The drain
  runs off the FleetManager loop, since it blocks to its deadline, on a
  deliberately **short** budget (5 s, against graceful shutdown's 60 s): a drain
  holds this node out of the namespace's distribution for its whole duration, so
  a long one would leave a runner that reconnects seconds later facing its own
  node still drained. Whatever does not migrate inside the window simply stays,
  visible and misbound, for the convergence loop.
- Branch C — last runner in the namespace cluster: mark `:inactive` with a
  timestamp. If the runner reconnects within `runner_keepalive`
  (default 30 s, env var `FLEZHA_OPERATOR_KEEPALIVE`),
  `try_migrate_runner` reroutes the preserved work to the new
  `runner_id`.

## Connection-healing contracts (post 0.2.x)

Two contracts make runner ↔ controller connections heal automatically across
either-side restarts:

1. **Stream rebind on `{:flz_closed, ...}`.** `StreamChannel` clears its
   `stream_ref` to `nil` synchronously inside its close-handler clause, and
   refuses outbound frames while `stream_ref == nil` (returns
   `{:error, :no_stream}` on the call path; logs and drops on the cast path).
   On the next incoming data stream for the same `runner_id`,
   `StreamAcceptor` (UDS) and `Transport.Server` (QUIC) walk the existing
   `StreamSup` children and bind the new ref to the first nil-ref channel via
   an atomic `GenServer.call(channel, {:bind, new_ref})`. A channel with a
   live ref returns `:already_bound`, so the acceptor moves to the next
   candidate without ever silently overwriting an in-flight binding.
2. **AddOperative reissue on reconnect.** After the existing
   `try_migrate_runner` step, `FleetManager.runner_connect/3`
   calls `reissue_local_orphans/5`, which queries
   `HubManager.get_registry/1`, filters to operatives hosted on `node()` whose
   `:tag` metadata names a no-longer-active `runner_id` and whose
   factory the connecting runner advertises, and re-issues `AddOperative` to
   the new runner via `Overwatch.runner_migrate` →
   `start_operative_sdk`. Operatives already handled by the migrate step are
   skipped via the `claimed_operative_ids` set so no SDK ever receives a
   duplicate `AddOperative` it would have to handle as a no-op.

The Rust and Java SDK `AddOperative` handlers are idempotent on duplicate
`operative_id`: they log
`"AddOperative: operative <id> already registered, skipping"` and return,
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

Flezha operatives `use ProcessHub.Strategy.Migration.HotSwap` in
`lib/flezha/operative/overwatch.ex` and override the handover callbacks to
ship rust/java state binaries alongside the Elixir state.

If handover fails (target unreachable, timeout, callback error), the source
process is NOT terminated — the existing process keeps running. The migration
may be retried later.

## State snapshots for handover and restart

State persistence is split between two layers:

* **HotSwap migration** uses an in-memory snapshot captured by
  `prepare_handover_state` and consumed by `alter_handover_state/2` on
  the destination — see `lib/flezha/operative/overwatch.ex`.
* **Cold-boot restart** is covered by the per-Entity WAL/snapshot
  files in `app_data/state/{operative_class}/{procedure_id}/...`
  (`docs/resilience/state_persistence.md`) plus ProcessHub's persistent
  registry under `app_data/process_hub/{hub_id}/registry.dets`. There is
  no periodic in-memory snapshot loop and no `flz cluster snapshot` CLI.

## Runner → controller discovery and connection

(Documented here because it's adjacent to ProcessHub-driven assignment.)

- Each runner (Rust/Java SDK) connects to **exactly one controller**. There is no
  multi-controller connection.
- Discovery is filesystem co-location, not service discovery. In Kubernetes
  the `flezha-sockets` hostPath volume is mounted into BOTH the controller pod and
  the runner pod, so the runner opens the local controller's UDS socket via
  `FLEZHA_SOCKET_FILES_PATH` (default `/flezha/sockets`).
- For QUIC transport: the runner dials `FLEZHA_QUIC_ADDRESS`.
- Reconnect loop: exponential backoff `100ms * 2^n` capped at 30 s; retries
  forever; never tries a different controller.
- Stream architecture: one bidirectional control channel for handshake +
  protobuf commands, plus N data streams opened on demand for operative I/O.
  Data streams are routed by `StreamAcceptor` round-robin to `StreamChannel`
  processes.

## What does NOT exist (anti-patterns to avoid in design)

These are common assumptions to double-check against the actual code:

- **Auto-restore-on-boot exists** via ProcessHub's durable registry +
  per-Entity WAL/snapshot files. See `docs/resilience/auto_restore_on_boot.md`
  for the full safety analysis. Anything beyond that boundary
  (out-of-cluster portable backups, point-in-time-to-arbitrary-past)
  is out of scope.
- **No persistent FleetManager state.** `runner_namespaces` and
  `namespace_factories` live only in the FleetManager GenServer. Restarting
  the controller process loses them entirely; ProcessHub's
  `pre_recovery_replay` hook waits for FleetManager to repopulate
  before replay dispatches `start_children`.
- **No persistent runner → operative assignment.** Assignment is recomputed
  every time `child_data_alter` fires; nothing is stored.
- **No multi-controller discovery in the SDK.** A runner does not failover to
  another controller if its local one is gone — it waits forever for the local one
  to return.

## Where the code lives

| Concern | File |
|---|---|
| Hub spec, hooks, alter_child_data | `lib/flezha/service/hub_manager.ex` |
| Runner selection (random within namespace+factory) | `lib/flezha/service/operative_manager.ex` |
| FleetManager (tracks runners, factories, disconnect branches) | `lib/flezha/runner/fleet_manager.ex` |
| Operative wrapper, HotSwap callbacks | `lib/flezha/operative/overwatch.ex` |
| Control channel (runner handshake, EOF detection) | `lib/flezha/runner/control_channel.ex` |
| Stream channels, attach/detach, frame routing | `lib/flezha/runner/stream_channel.ex` |
| UDS stream acceptance and round-robin | `lib/flezha/socket/stream_acceptor.ex` |
| Rust SDK reconnect loop | `sdk/rust/src/runner/control_unit.rs` |
| Rust SDK reconnect handler | `sdk/rust/src/runner/{fast,legacy}.rs` `handle_reconnect` |

## Plugin subscriptions persistence

The ProcessHub child-spec is the source of truth for an operative's
plugin subscriptions, and ProcessHub's **persistent registry** (the
`durable-registry` capability with `registry_backend: {:dets, ...}`)
makes that source of truth survive any controller restart.
`operative.opts[:plugins]` (embedded inside the cspec start args) carries
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
and HotSwap migration — restores the same plugin set without runner
intervention.

This persistence layer is independent of the per-Entity WAL +
snapshot persistence in `lib/flezha/state/` (`docs/resilience/state_persistence.md`):
the cspec persists *which* plugins are subscribed; the per-Entity
layer persists *how much progress* an Entity has made through its
input stream. Both layers can be present, absent, or partially
populated for a given operative without affecting one another.

## RestoreInitState vs runner_migrate

The per-Entity `RestoreInitState` handshake (`operative-state-persistence`)
and the existing `runner_migrate` flow are **mutually exclusive
per Entity**. A reconnecting Entity with `persistence_enabled =
true` and on-disk WAL/snapshot is restored via `RestoreInitState` only.
A migrating Entity (HotSwap moving the Overwatch to a peer controller
host) carries its own snapshot bytes through the migration path; the
new controller does NOT additionally fire `RestoreInitState` for the same
Entity. Doing both would double-apply state.

The persistence module (`lib/flezha/state/`) is per-Entity and
ProcessHub-agnostic. ProcessHub picks the controller host; the persistence
module owns the per-Entity WAL on whichever host the Overwatch is
currently running on. Migrating an Overwatch to a peer host moves the
in-memory state via `runner_migrate`; the new host's
persistence module starts a fresh WAL (no replication is in scope for
v1).

## Related docs

- `docs/sdk_protocol_spec.md` — wire-level binary framing and protobuf commands.
- `docs/resilience/state_persistence.md` — per-Entity WAL, snapshot, and time-travel.
- `docs/resilience/auto_restore_on_boot.md` — the single auto-restore mechanism (ProcessHub durable registry + per-Entity WAL).
- `docs/nif_architecture.md` — transport NIF design (UDS + QUIC drivers).
- `docs/deployment.md` — k8s DaemonSet layout, env vars, hostPath volumes.
