# ProcessHub in the controller

For contributors touching operative placement, restart, recovery, migration or the runner
connection lifecycle. The controller distributes operatives across nodes with the
[`process_hub`](https://github.com/alfetahe/process-hub) library; most of the behaviour on this
page is automatic; the usual mistake is to assume Flezha owns logic the hub already provides.
It covers the two invariants, two-level distribution, per-namespace hubs and strategies, the
hooks Flezha registers, recovery as a reconcile, lifecycle scenarios, connection healing,
HotSwap handover, what restores state after a move, plugin persistence and runner discovery.

## The two invariants

1. **Intent lives in the hub's declared set.** Every durable operative start
   (`durable: HubManager.durable_starts?()`, set by `OperativeManager` on every start path:
   operator starts, scheduled and procedure-requested spawns, partition members) adds an entry;
   every deliberate stop or workflow completion removes it through the hub stop path. The set
   is versioned, leader-written, persisted beside the registry on every node and optionally
   shipped off-cluster. It answers *what SHOULD exist*, even when nothing runs.
   `Convergence.Intent.declared/0` and `HubManager.declared_operatives/1` read it; nothing
   outside the start/stop authority writes it.
2. **The live, cluster-synced registry is the source of truth for what IS running and where.**
   Not DETS, not controller caches. DETS is a disk backup of runtime rows used only to cold-start
   a fully-downed cluster; a node rejoining a live cluster syncs from peers and must not trust
   its own DETS. `HubManager.get_registry/1` reads the live rows (unbound and stopped rows
   excluded).

Losing runtime rows loses no intent: the reconcile re-derives them from the declared set.
Losing the declared set parks the reconcile (with the `declared_parked` hook) rather than
opening empty; `ProcessHub.Service.DeclaredChildren.clear/1` is the operator override.

## Two-level distribution

1. **Cluster level — ProcessHub** picks the controller **node** that owns each operative on a
   per-namespace consistent-hash ring. One stream or workflow operative instance is one hub
   child; Entities are SDK-internal and invisible to the hub. A partitioned operative
   (`flz operative start <id> --partitions N`) is N children, `<id>#p0..#p{N-1}`, which may land
   on N nodes (see [partitioned operatives](../sdk/partitioned_operatives.md)).
2. **Within a controller — runner selection.** The owning controller picks one locally
   connected runner advertising the operative's factory: `OperativeManager.assign_runner/3` ->
   `pick_runner/2`, a rendezvous hash (`max_by :erlang.phash2({operative_id, runner_id})`), so
   a partitioned group's members spread statistically across runners.

The levels are decoupled: the hub knows nothing about runners; the `child_data_alter` hook does
runner selection at start time.

## Per-namespace hubs

Each controller runs one hub per namespace it serves, id `Identificator.hub(namespace)`
(a `<namespace>_hub` atom; `Identificator.hub_id?/1`). Controllers serving namespace A and
namespace B form independent clusters; "peer controllers" always means *peers serving the same
namespace*, and a namespace dies when its last controller leaves. The cron-schedule hub
`:flz_schedules` is separate: `ColdSwap`, `:ets` registry, no auto-recovery, no hooks.

`HubManager.process_hub/1` builds the spec:

```elixir
ProcessHub.child_spec(%ProcessHub{
  hub_id: Identificator.hub(namespace),
  synchronization_strategy: %ProcessHub.Strategy.Synchronization.PubSub{},
  redundancy_strategy: %ProcessHub.Strategy.Redundancy.Singularity{},
  partition_tolerance_strategy: %ProcessHub.Strategy.PartitionTolerance.Divergence{},
  distribution_strategy: %ProcessHub.Strategy.Distribution.ConsistentHashing{},
  migration_strategy: %ProcessHub.Strategy.Migration.HotSwap{handover: true, consent_settings: ...},
  registry_backend: {:durable_ets, [path: "<registry_root>/<hub_id>/registry.dets"]},
  auto_recovery: [reconcile_grace_ms: ..., reconcile_interval_ms: ..., remote_manifest: ...],
  cross_node_request_timeout: 10_000,
  hooks: %{...}
})
```

| Concern | Strategy | Effect |
|---|---|---|
| Distribution | `ConsistentHashing` | `operative_id` hashes to a node on the ring; a membership change reshuffles only the affected slice |
| Synchronization | `PubSub` | registry starts/stops broadcast to every node (`:blockade`); interval sync every 30 s |
| Migration | `HotSwap{handover: true, consent_settings}` | the source is terminated only after the target has started and received the handover; busy operatives may defer |
| Redundancy | `Singularity` | each instance runs on exactly one node, no replicas |
| Partition tolerance | `Divergence` | a network partition is not handled: both sides keep running independently |

Consent settings (app env `:migration_consent`): `consent_timeout_ms` 5 000, `retry_interval_ms`
2 000, `max_defer_ms` defaults to the graceful-shutdown budget so a deferring operative never
outlives the shutdown window. The registry backend is `:ets` and `auto_recovery` is `false`
when auto-restore is off.

## Hooks Flezha registers

| Hook | Handler | Purpose |
|---|---|---|
| `child_data_alter` | `HubManager.alter_child_data/2` | rewrite the cspec start args with the assigned runner and set `metadata.tag` |
| `child_registered` | `HubManager.hook_registry_written/2` (hub-wide) | ship the cspec to the durability tier; declare `{:registry_written, …}` for each local operative, so the loop reads the row beside the binding and re-publishes it when they differ |
| `child_registered` | `HubManager.hook_insert_pid/2` (one per runner, registered by `register_handler/2` on connect, cancelled on disconnect) | push the new local pid into that runner's stream channels so I/O routes |
| `recovery_state_changed` | `Convergence.Snapshot.handle_recovery_transition/2` | cache the hub's recovery state for `/healthz` and the boot gate |
| `pre_recovery_replay`, `post_recovery_replay`, `reconcile_round`, `reconcile_duplicate` | `Flezha.Boot.RecoveryHooks` | forward to telemetry; `reconcile_round` also triggers an immediate convergence tick; `reconcile_duplicate` is an alarm. Not registered when auto-restore is off |

`alter_child_data` placement: it waits for a placeable runner with a budget shared across the
start batch (`OperativeManager.await_runner/5`, bounded by `cross_node_request_timeout`). A first
start with no runner for the factory raises (`No active compute engines available ...`). A
replayed cspec that already records a runner starts on that recorded runner pending rebind, so
its row gets a live pid and the convergence loop can heal it instead of the whole batch dying.

## Recovery is a reconcile toward the declared set

There is no recovery marker, boot mode or arming step. Every node runs an **orphan reconcile**
`reconcile_grace_ms` after its coordinator starts and then at most once per
`reconcile_interval_ms`: it starts declared children observed running nowhere and stops running
durable children whose declared entry was removed. One rule covers every case:

- **Cold-starting a dead cluster** — nothing runs, the whole declared set is started, including
  spawned workflow instances.
- **Rejoining a live cluster** — peers run everything, the difference is empty. The declared
  set syncs by version (adoption replaces the whole list, higher wins), so the rejoining node
  adopts stops it was absent for, however long the absence.
- **Wiped runtime registry** — rows are gone, the declared set is not; the reconcile restarts
  through the same idempotent start authority (the registry's duplicate-id claim prevents a
  double bind).

A deliberate stop is **absence** from the declared set and never expires: `OperativeManager.stop_operative/3`
rides `ProcessHub.stop_children(hub_id, ids, awaitable: true, timeout: 15_000)`, which removes
the entry *before* the child terminates, so a crashed stop converges to stopped. A transient
normal exit deletes the row but not the entry (the cspec stays `restart: :transient`), which is
why every deliberate stop must go through the hub stop. A hub without auto-recovery refuses
durable starts (`:durable_requires_auto_recovery`).

The first round is what moves a hub to `:normal`, so the grace **is the boot gate**:
`Convergence.RecoveryGate` and `Boot.Loader.await_namespace_hubs/0` block on it, and it is set
above the runner handshake window because an orphan start needs a runner to place against.
Two loops start operatives — the reconcile and `Convergence.Loop` (5 s tick, rebind/replace
heals); `reconcile_duplicate` telemetry is the tripwire if they contend.

| Knob | Default | Effect |
|---|---|---|
| `FLEZHA_AUTO_RESTORE_ENABLED` | `true` | `false` disables the reconcile, refuses durable starts and reverts the registry to in-memory `:ets` |
| `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` | `45000` | first round after coordinator start; also the boot gate |
| `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS` | `30000` | minimum spacing of later rounds |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | `app_data/process_hub` | `<root>/<hub_id>/registry.dets` plus `registry.declared.dets` beside it |

### Registry writes must be cluster-wide

Interval sync propagates *which children run where*, not edits to a row: `Synchronizer.append_data/2`
rewrites a peer row only when its pid changed, with the sender's cspec. A plain
`ProcessRegistry.update/3` is node-local. Manifests embedded in the cspec — the plugin list and
the runner tag — are written with `ProcessRegistry.update/4` `propagate: true`, which applies
the same pure transform on every hub node (best effort; a node missed while down converges on
rejoin). The transforms live in `Flezha.Service.RegistryUpdate` so the shipped closure resolves
on peers; all hub nodes are assumed to run the same release.

### Remote manifest

With the durability tier enabled, every declared-list version ships asynchronously to
`Flezha.State.DurabilityTier.DeclaredManifest`, the `ProcessHub.Storage.RemoteManifest`
adapter over the tier's backend, under key `declared/<hub_id>` (outside the tier GC's `blob/`
scope). Boot adopts the highest version among the local and remote copies. The ship is
leader-only, retried, never a synchronous dependency of a start or stop. See
[remote durability tier](../operations/remote_durability_tier.md).

### Restart playbook

- **Rolling update:** bring the new instance up and confirm it runs before stopping the old
  one; it syncs live state from the surviving peer.
- **Sole instance, cluster fully down:** plain restart; the first round restores the declared
  set within `reconcile_grace_ms`.
- **Total disk loss:** with the tier configured, boot fetches the remote manifest. Without one,
  the lost list parks the reconcile and `DeclaredChildren.clear/1` is the override.
- **Start fresh:** `flz controller stop && flz reset --yes` (see [reset](../operations/reset.md)).

## Lifecycle scenarios

**Graceful node leave** (rolling update, `flz controller stop`). The hub is drained
(`HubManager.drain_hub/2`, default ceiling 60 s): every local child asks its Overwatch for
consent, `prepare_handover_state/1` captures the state, the target node starts the child, the
handover is delivered, and only then is the source terminated. The target's `child_data_alter`
picks a local runner. Whatever still defers at the deadline is force-migrated. See
[graceful shutdown](../operations/graceful_shutdown.md).

**Ungraceful node loss** (crash, OOM, SIGKILL, a partition that holds). Coordinators see
`:nodedown`, batch it to absorb flaps, and redistribute. No handover happens and the dead node's
`state_root` cannot be pulled; Entity state returns only through the remote durability tier when
enabled, otherwise it stays on that node's disk until it returns.

**Node returns.** The ring is recomputed; operatives may or may not move back — there is no
snap-back. A move goes through `child_data_alter` on the rejoining node.

**Runner restart** (invisible to the hub; `FleetManager.Session.handle_runner_disconnect/3`): the
session drops the runner from the active set, declares `{:runner_disconnected, ns, runner_id}` to
the convergence loop and migrates nothing itself. The loop chooses per operative on its next tick,
which the declaration triggers at once:

- *Other runners in this namespace on this controller* — the Diff marks each of the runner's operatives misbound with reason `:runner_disconnected` (not streak-gated: a closed session is not a probe miss) and the Planner plans `rebind_operative`, keyed `{:runner_disconnected, operative_id}`, at most `max_ops` per tick with the plan re-ticking until the set is walked.
- *No runner here, peers serve the namespace* — the Diff's `replace_operatives` (no local runner for the factory, a peer fleet has one) plans `replace_operative` per child, through `Migration.migrate_child/3` and the migration-consent gate with live handover. Nothing is forced and no drain runs: a child that defers stays put, visible and misbound, for the next tick, and the namespace hub stays addressable so a runner reconnecting seconds later finds its node in the distribution. The dead runner cannot serialize state, so the handover is empty and the destination pulls the Entity files (`Flezha.State.WalTransfer`).
- *Last runner in the namespace cluster* — `Session.runner_preserve/3` marks it `:inactive` with a
  timestamp; the cleanup tick (`FLEZHA_RUNNER_KEEPALIVE` + 10 s) reaps it once
  `FLEZHA_RUNNER_KEEPALIVE` (default 30 000 ms) has elapsed and reclaims the hub if vacant.

On reconnect `FleetManager.Session.runner_connect/3` starts the hub if needed (after
`Orchestrator.await_cluster_ready/0`), registers the runner's `child_registered` handler, starts
its `RunnerSupervisor`, transfers the control stream and sends `AssignRunnerId` then
`ConnectSocket`. Orphaned operatives are rebound by the convergence loop, not by FleetManager;
session continuity is covered in [runner identity](runner_identity.md).

## Connection-healing contracts

1. **Stream rebind on `{:flz_closed, ...}`.** `StreamChannel` clears `stream_ref` to `nil`
   inside its close handler; `StreamChannel.io_in/3` answers `{:error, :no_stream}` (emitted as
   `[:flezha, :stream, :drop]`) while unbound. The next data stream for the runner is bound by
   `StreamAcceptor` (UDS) or `Transport.Server` (QUIC) through
   `StreamPool.bind_stream_to_first_nil/2`: an atomic `{:bind, ref}` call that a live channel
   answers with `:already_bound`, so an in-flight binding is never overwritten.
2. **`AddOperative` is idempotent.** The shared `flz_core` handler (Rust and Java) answers a
   duplicate `operative_id` with the existing instance's metadata when `runner.has_operative`
   is true, keeping its Entity state intact.

## HotSwap with handover

`Flezha.Operative.Overwatch` does `use ProcessHub.Strategy.Migration.HotSwap` and
`use ProcessHub.Strategy.Migration.MigrationConsent`:

1. The source is told "child X moves to node Y" and, if consent is configured, asks the
   process `{:process_hub, :migration_consent, ...}`; `Overwatch.migration_consent/1`
   (`Handover.consent/1`) answers `:defer` while a correction is pending, a durable workflow is
   off an iteration boundary (suspension is the barrier), or the effectively-once outbox still
   owes output; no reply within `consent_timeout` also defers.
2. `{:process_hub, :query_hot_handover_state, ...}` -> `prepare_handover_state/1`
   (`Handover.prepare/1`): `%{id, namespace, factory_id, init_values, plugins, source_node}`
   plus the SDK's serialized bytes (`MigrationSerialization`; `nil` if the runner is gone).
3. The state is stashed in ETS (`state_ttl` 30 s; `state_query_timeout` 5 s).
4. The target starts the child; before it is registered, the source delivers
   `{:process_hub, :hotswap_handover, child_id, state}` and the new process applies it via
   `alter_handover_state/2` (`Handover.absorb/2`: ships `MigrationDeserialization`, rebuilds
   the plugin table).
5. Only after the target start succeeded is the source terminated; a failed target start leaves
   the source running for a later retry.

`ProcessHub.Service.Migration.migrate_child/3` is the per-child variant the convergence
`:replace_operative` heal uses: explicit target node, no consent query, never touches the
declared set (a stop+start would lose the operative on a crash in between). The ring may later
migrate the child back, and the loop re-applies.

## What restores state after a move

- **HotSwap with state** — the Overwatch arrives with `arrived_via_hotswap?: true` and ignores
  `{:kick_restore, _}`: the WAL is not replayed on top of the in-band state.
- **Empty handover** (source runner already dead) — `WalTransfer.pull_operative/2` copies the
  Entity files from `source_node` (`state_root` is node-local, never assumed shared), then
  `Replay.kick_restore_for_bound_runner/2` issues `RestoreInitState` per persisted Entity.
- **Same-node rebind** (`{:runner_migrate, runner_id}`) — the binding commits only once the new
  runner has answered; the `rebind_operative` primitive then runs
  `State.Recovery.resync_for_overwatch/2`, which replays the per-Entity WAL into the new binding.
  A parked or suspended operative un-parks through the same call
  (`Operative.Migration.spawn_migrate_task/1`).
- **Cold boot** — the durable registry plus the per-Entity WAL and snapshots under
  `app_data/state/...` ([state persistence](state_persistence.md)). There is no periodic
  in-memory snapshot loop and no cluster-wide snapshot CLI.

## Plugin subscriptions

The cspec's `operative.opts[:plugins]` is the authoritative plugin list; the runtime
`Overwatch.state.extensions` table is rebuilt from it on every `init/1`.
`PluginManager.enable_plugin/2` and `disable_plugin/1` write the registry first
(`ProcessRegistry.update/4`, `propagate: true`) and only then send
`{:plugin_subscribe, ...}` / `{:plugin_unsubscribe, ...}` to the Overwatch; a failed write returns
`{:error, :registry_unavailable}` without touching the runtime. Every restart path — cold start,
redistribution, in-place restart, HotSwap — restores the same set without runner involvement.
The declared entry keeps the start-time manifest, so a restore that has only the declared set
replays the plugins as they were at start. This layer is independent of the per-Entity WAL: the
cspec persists *which* plugins, the WAL persists *how far* an Entity has progressed.

## Runner to controller discovery

- A runner connects to exactly one controller and never fails over to another.
- UDS discovery is filesystem co-location: the runner opens the controller's control socket under
  `FLEZHA_SOCKET_FILES_PATH` (unset, the socket ladder resolves it; the Kubernetes DaemonSet
  mounts the `flezha-sockets` hostPath at `/flezha/sockets`). QUIC runners dial
  `FLEZHA_QUIC_ADDRESSES` (SDK default `127.0.0.1:4433`).
- The reconnect loop (`flz_core/src/control_unit.rs`, shared by Rust and Java) retries forever
  with full-jitter exponential backoff `100 ms * 2^min(n, 8)`, capped at 30 s.
- One control channel carries the handshake and protobuf commands; primary and secondary data
  streams carry operative I/O (see [NIF transport](nif_transport.md)).

## What does not exist

- **No persistent FleetManager state.** Runner sessions and advertised factories live in the
  FleetManager process; after a controller restart the reconcile grace and `alter_child_data`'s
  batch wait give runners time to re-announce.
- **No persistent runner assignment beyond the registry row.** `metadata.tag` and the cspec
  record the last binding so a replay can start pending rebind; placement is recomputed on
  every `child_data_alter`.
- **No cross-node WAL replication.** `WalTransfer` is a one-shot pull at migration time; the
  remote durability tier is the cross-node artifact answer.

## Where the code lives

| Concern | Location |
|---|---|
| Hub spec, hooks, `alter_child_data`, `declared_operatives/1`, `drain_hub/2` | `lib/flezha/service/hub_manager.ex` |
| Runner selection, start/stop authority | `lib/flezha/service/operative_manager.ex` |
| Registry transforms shipped with `propagate: true` | `lib/flezha/service/registry_update.ex` |
| Plugin enable/disable ordering | `lib/flezha/service/plugin_manager.ex` |
| Runner sessions, disconnect branches | `lib/flezha/runner/fleet_manager.ex` |
| Overwatch HotSwap/consent callbacks, `runner_migrate` | `lib/flezha/operative/overwatch.ex`, `lib/flezha/operative/handover.ex` |
| Rebind / replace heals, intent reader | `lib/flezha/convergence/heal.ex`, `lib/flezha/convergence/intent.ex` |
| Recovery hook forwarding | `lib/flezha/boot/recovery_hooks.ex` |
| Remote manifest adapter, WAL transfer | `lib/flezha/state/durability_tier/declared_manifest.ex`, `lib/flezha/state/wal_transfer.ex` |
| Library: cluster, sync, registry, declared set, recovery, migration | `deps/process_hub/lib/process_hub/service/{cluster,synchronizer,process_registry,declared_children,recovery,migration}.ex`, `storage/remote_manifest.ex`, `strategy/migration/hot_swap.ex` |

The dependency is a hex release at the requirement `mix.exs` declares; `mix.lock` pins
the exact version.
`mix deps.get` follows the lock; `make refresh-process-hub` re-resolves within the requirement
and rewrites it; `FLEZHA_PROCESS_HUB_PATH` builds from a local checkout.

## See also

- [Convergence loop](convergence_loop.md) — Intent, Probe, Diff, Planner, Heal
- [Auto-restore on boot](../operations/auto_restore_on_boot.md) — operator view of the reconcile
- [Graceful shutdown](../operations/graceful_shutdown.md) — drain and migration consent
- [State persistence](state_persistence.md) — per-Entity WAL and snapshots
- [Runner identity](runner_identity.md) — sessions, reconnect and takeover
- [Partitioned operatives](../sdk/partitioned_operatives.md)
