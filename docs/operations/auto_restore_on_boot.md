# Auto-restore on boot

For users running a Controller whose disk survived a restart. Covers the on-disk layers a
restart recovers from, the boot sequence, what the orphan reconcile starts and stops, the
safety matrix per scenario, the `flz boot` surface, start authority, the knobs and the disable
flag. Losing the disk itself is the [remote durability tier](../resilience/data_durability.md)'s
problem; with a backend configured, both layers below hydrate from it first.

On boot the controller rehydrates the operator list from the durable ProcessHub registry and
ships the on-disk WAL and snapshots to each runner, so Operators resume from their last
persisted state. Recovery is an **orphan reconcile**: no marker file, no boot mode, no arming.

## Persistence layers

| Layer | Path | Owner |
|---|---|---|
| ProcessHub durable registry (cspecs incl. `opts[:plugins]`) | `app_data/process_hub/<hub_id>/registry.dets` | `Flezha.Service.HubManager` (`registry_backend: {:durable_ets, …}`) |
| Per-Cell WAL + snapshots | `app_data/state/<namespace>/<operator_class>/<tasklet_id>/<cell_key>.{wal,snap.<seq>,outbox.dedup}` | `Flezha.State.Supervisor` |
| Remote manifest + artifacts backing both (unless opted out) | Postgres / S3 / file, `FLEZHA_DURABILITY_BACKEND` | `Flezha.State.DurabilityTier` (`Shipper` out, `Hydrator` back) |

## Boot sequence

1. `Flezha.Application.start/2` runs `Flezha.Boot.StatePathMigrator.migrate/0` synchronously
   before `Flezha.State.Supervisor`: a one-shot, idempotent migration of older state layouts
   into the namespaced one, skipped on later boots by the `.layout` marker.
2. With a remote backend, `Flezha.State.DurabilityTier.Hydrator.hydrate_on_boot/0` runs as a
   one-shot child **ahead of `HubManager`** (control-plane stores, then every registry cspec):
   a hub opens `registry.dets` at start, so a cspec landing later is invisible to the reconcile.
3. `Flezha.Service.HubManager.Starter` gives a hub to every namespace this node already has
   evidence for — a registry directory under the registry root, or an entry in the namespace
   catalog — so a restarted controller replays its durable registry without waiting for a
   runner to connect. It runs beside the boot sequence rather than inside it, because it waits
   for cluster formation first. Each hub starts through `Flezha.Service.HubManager` with
   `registry_backend: {:durable_ets, [path: …]}` (reads from ETS, writes mirrored to disk
   synchronously) and `auto_recovery: [reconcile_grace_ms:, reconcile_interval_ms:,
   cluster_settle_ms:, remote_manifest:]`, then `Flezha.Boot.RecoveryHooks.register/1`
   forwards the recovery hooks. A namespace in neither source still gets its hub when a runner
   connects for it.
4. ProcessHub runs the first reconcile round on evidence: once `cluster_settle_ms` has passed
   since coordinator start and every connected peer has sent its registry data. On a node with
   no peers that is the settle window alone, about two seconds. `reconcile_grace_ms` caps the
   wait, so a connected peer that never answers cannot hold the hub for longer. Later rounds run
   at most one per `reconcile_interval_ms`. A round takes the hub's **declared list** as the
   candidate set: it starts declared children observed running nowhere, stops running durable
   children whose entry was removed, removes stale registry rows and resolves duplicates. A
   dead cluster's difference is its whole list; a node rejoining a live cluster starts nothing.
   **The first round moves the hub `:recovering` → `:normal`, so it is the boot gate.** It
   waits for peers, never for runners: an orphan it starts before the orphan's runner connects
   is held unbound by its Overwatch and bound by the convergence rebind when a runner
   advertising its factory arrives.
5. `Overwatch.init/1` rebuilds each Operator's plugin table from the cspec's `opts[:plugins]`.
   On bind, `Flezha.Operator.Replay.restore_on_bind/2` enumerates the Cells on disk
   (hydrating from the remote tier first when local state is absent) and ships a
   `RestoreInitState` for each — unless the Operator arrived via HotSwap with in-band state.
6. `Flezha.Boot.Loader` finishes its sequence and the application records that fact; it does
   not wait for recovery, because readiness is not latched at boot. `Flezha.Boot.Readiness`
   folds the verdict live from its precondition table, so `/healthz` reports
   `recovering` / `restoring_registry` until the registry is loaded and its Operators bound,
   and `/readyz`, `flz boot await` and the image `HEALTHCHECK` all answer not-ready for that
   whole window — then flip on their own once the hub reaches `:normal`, with no restart. The
   `declared_hubs_started` row holds the same window open until step 3 has finished, so a node
   whose hubs do not exist yet is never reported ready over an empty hub set.

## A stopped namespace comes back

The catalog is user intent, so a namespace whose hub was torn down is given one again on
the next controller start while its catalog entry stands. `flz namespace delete <name>` is what
makes a stop permanent. A clean state root therefore holds one `_system` hub and its registry
directory after the first boot.

The reverse does not happen mid-life: a hub this controller reclaims because it serves none of
that namespace's operators stays down until the next start, and does not hold readiness. The
reclaim will not take a hub that is still recovering or that was declared to hold operators —
it deletes the registry directory, and boot-time creation puts hubs exactly where no runner has
placed anything yet.

## Safety by scenario

| Scenario | Replay? | Result |
|---|---|---|
| Cluster-wide cold boot | Yes, automatic | Each node's first round finds the whole difference; the hash ring picks the owner, a duplicate `child_id` start is idempotent. Plugins re-attach in `Overwatch.init/1`, Cell state ships on bind. |
| One controller restarts into a healthy cluster (incl. rolling rollouts) | No | The difference against the live cluster is empty; the node reaches `:normal` after the grace and tramples nothing. |
| Single-node deployment | Yes, automatic | No peer can hand rows back; the first round starts the whole durable list. |
| Runner pod restart with `runner_id` rotation | N/A | The controller never went down; the state layout has no `runner_id` component, so the new runner receives `RestoreInitState` from the same files. |
| HotSwap migration, source runner alive | N/A | State travels in-band; `restore_on_bind/2` does not fire. Source files are left for the retention sweeper. |
| HotSwap migration, source runner already dead | N/A | The handover is empty, so the destination pulls the Cell files over `:erpc` (`Flezha.State.WalTransfer.stage_operator/3`, switched in whole by `Flezha.State.Adoption`) and replays them — one-shot, not replication. A destination crash mid-flight leaves the source files intact and ProcessHub retries. |
| Network partition during boot | Yes (suspect) | A brief duplicate-start window while healing; the hub's `PartitionTolerance.Divergence` strategy and the round's duplicate resolution settle it. Fragile environments can set `FLEZHA_AUTO_RESTORE_ENABLED=false`. |
| Quorum loss (3 nodes, 2 die) | Only the survivor's gaps | Returning nodes start only what nothing in the cluster holds; the survivor stays authoritative for what it runs. |
| Subset rejoin (cluster down, some nodes return) | Yes | **The one tolerated loss**: rows held only on non-returning nodes are gone — unless the remote tier is on, in which case their cspecs and state hydrate from the backend. |
| Registry corruption | Empty replay | The file is rotated to `registry.dets.corrupt-<monotonic>`; boot continues empty. |
| User restores an old `registry.dets` from backup | Per row | Its rows are declared intent again: the next round starts them. |

## Operations surface

```bash
flz boot status                # readiness verdict + preconditions, then per-namespace detail
flz boot await --timeout 60    # block until the verdict is :ready (exit 7, boot.not_ready, otherwise)
flz boot await --timeout 0     # evaluate once — what the image HEALTHCHECK runs
flz cluster status             # recovery_state per node
```

A verb that refuses writes its reason to the stderr of the shell you ran it in,
not to the Controller's log, so `flz boot await` names the precondition that is
still unmet:

```
$ flz boot await --timeout 0
boot await: recovering; unmet: durable_registry_loaded
```

Telemetry under `[:flezha, :boot, …]`, each with `%{hub_id, namespace}` metadata:

| Event | Fires | Measurements |
|---|---|---|
| `:pre_recovery_replay` | Before the first round | `%{cspec_count, system_time}` |
| `:recovery_complete` | After the first round | `%{child_count, succeeded, failed}`; `replay_outcome` metadata |
| `:reconcile_round` | Every round | `%{candidates, orphans, started, duplicates, elapsed_ms}`; `first_round` metadata |
| `:reconcile_duplicate` | One Operator seen on several nodes | `%{instance_count}`; `operator_id`, `kept_node`, `stopped_nodes` metadata |

`:recovery_complete` and `:reconcile_duplicate` reach the observability bus under the
`:flezha_boot` source. `reconcile_duplicate` is an **alarm, not a metric** (ProcessHub already
resolved it keep-one/stop-others): expected only while recovering from a cross-node partition,
at any other time a start bypassed the id claim, and it logs at `error`.

## Start authority

The reconcile and the convergence Loop both start Operators; neither is gated on the other
because every start resolves through one claim, `ProcessHub.Service.Distributor.compose_start_operation/3`
(the reconcile from its round, flezha from `OperatorManager.start_operator/3` →
`ProcessHub.start_child/3`): the registry id check, backstopped by the local
`DistributedSupervisor`'s `{:already_started, pid}`. A start that loses the claim answers
`:duplicate` after verifying the winner is live and does nothing to it. The CLI renders that
as success — `flz operator start` against a live instance exits 0 as a no-op, `--partitions N`
over an existing group starts only the missing members, `flz plugin enable` on an attached
plugin reports `already_enabled` — so re-running a cold-start script is the supported heal
after a warm restart. A refused claim with **no** live winner (a stale registry row) stays a
loud error. Boot is held by `Flezha.Convergence.RecoveryGate` until hubs are `:normal`; later
a round reporting `started > 0` triggers one convergence tick that adopts what it started.

## Configuration

| Env var | Application key | Default | Purpose |
|---|---|---|---|
| `FLEZHA_AUTO_RESTORE_ENABLED` | `:flezha, :auto_restore_enabled` | `true` | Disable flag; only `true` / `1` enable (see below) |
| `FLEZHA_PROCESS_HUB_CLUSTER_SETTLE_MS` | `:flezha, :process_hub_cluster_settle_ms` | `2000` | How long a starting hub waits for peers before it treats itself as alone; on a node with no peers, how long a hub stays `:recovering`. `0` when the cluster forms before the application starts |
| `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` | `:flezha, :process_hub_reconcile_grace_ms` | `45000` | Cap on the first round: the longest a connected peer that never sends its registry data can hold a hub in `:recovering` |
| `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS` | `:flezha, :process_hub_reconcile_interval_ms` | `30000` | Rate limit on later rounds |
| `FLEZHA_BOOT_REPLAY_TIMEOUT_MS` | `:flezha, :boot_replay_timeout_ms` | `60000` | How long a connecting runner is given during cluster formation |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | `:flezha, :process_hub_registry_root` | `app_data/process_hub` | Parent of each `<hub_id>/registry.dets`; must be persistent storage. Not read in the test environment, where `config/test.exs` wins |

## Disable flag and rollback

`FLEZHA_AUTO_RESTORE_ENABLED=false` flips one predicate (`HubManager.durable_starts?/0`) that
every auto-restore surface reads together: hubs start with `registry_backend: :ets` (nothing
written to disk) and `auto_recovery: false` (no reconcile, straight to `:normal`), the hooks
are not registered, and starts no longer declare durable intent — Operators must be
re-registered after every boot. Roll back by flipping the variable and restarting; an existing
`app_data/process_hub/` is ignored ([`flz reset`](reset.md) deletes it).

## See also

- [Remote durability tier](../resilience/data_durability.md) — surviving the loss of the disk
- [Graceful shutdown](graceful_shutdown.md) — the solo-node drain path that relies on this
- [ProcessHub internals](../internals/process_hub.md) — the declared list and the reconcile
