# Auto-restore on boot

For operators running a Controller whose disk survived a restart. Covers the on-disk layers a
restart recovers from, the boot sequence, what the orphan reconcile starts and stops, the
safety matrix per scenario, the `flz boot` surface, start authority, the knobs and the disable
flag. Losing the disk itself is the [remote durability tier](remote_durability_tier.md)'s
problem; with a backend configured, both layers below hydrate from it first.

On boot the controller rehydrates the operative list from the durable ProcessHub registry and
ships the on-disk WAL and snapshots to each runner, so Operatives resume from their last
persisted state. Recovery is an **orphan reconcile**: no marker file, no boot mode, no arming.

## Persistence layers

| Layer | Path | Owner |
|---|---|---|
| ProcessHub durable registry (cspecs incl. `opts[:plugins]`) | `app_data/process_hub/<hub_id>/registry.dets` | `Flezha.Service.HubManager` (`registry_backend: {:durable_ets, …}`) |
| Per-Entity WAL + snapshots | `app_data/state/<namespace>/<operative_class>/<procedure_id>/<entity_key>.{wal,snap.<seq>,outbox.dedup}` | `Flezha.State.Supervisor` |
| Remote manifest + artifacts backing both (unless opted out) | Postgres / S3 / file, `FLEZHA_DURABILITY_BACKEND` | `Flezha.State.DurabilityTier` (`Shipper` out, `Hydrator` back) |

## Boot sequence

1. `Flezha.Application.start/2` runs `Flezha.Boot.StatePathMigrator.migrate/0` synchronously
   before `Flezha.State.Supervisor`: a one-shot, idempotent migration of older state layouts
   into the namespaced one, skipped on later boots by the `.layout` marker.
2. With a remote backend, `Flezha.State.DurabilityTier.Hydrator.hydrate_on_boot/0` runs as a
   one-shot child **ahead of `HubManager`** (control-plane stores, then every registry cspec):
   a hub opens `registry.dets` at start, so a cspec landing later is invisible to the reconcile.
3. `Flezha.Service.HubManager` starts each namespace hub with
   `registry_backend: {:durable_ets, [path: …]}` (reads from ETS, writes mirrored to disk
   synchronously) and `auto_recovery: [reconcile_grace_ms:, reconcile_interval_ms:,
   remote_manifest:]`, then `Flezha.Boot.RecoveryHooks.register/1` forwards the recovery hooks.
4. `reconcile_grace_ms` after coordinator start ProcessHub runs the first reconcile round, then
   at most one per `reconcile_interval_ms`. A round takes the hub's **declared list** as the
   candidate set: it starts declared children observed running nowhere, stops running durable
   children whose entry was removed, removes stale registry rows and resolves duplicates. A
   dead cluster's difference is its whole list; a node rejoining a live cluster starts nothing.
   **The first round moves the hub `:recovering` → `:normal`, so the grace is the boot gate**;
   it sits above the runner handshake window because an orphan start needs a runner.
5. `Overwatch.init/1` rebuilds each Operative's plugin table from the cspec's `opts[:plugins]`.
   On bind, `Flezha.Operative.Replay.restore_on_bind/2` enumerates the Entities on disk
   (hydrating from the remote tier first when local state is absent) and ships a
   `RestoreInitState` for each — unless the Operative arrived via HotSwap with in-band state.
6. `Flezha.Boot.Loader` waits on `HubManager.await_recovery_complete/1` (every hub `:normal`,
   bounded by `boot_replay_timeout_ms`) before flipping `boot_ready?`. `/healthz` reports
   `recovering` / `restoring_registry` until the registry is loaded and its Operatives bound,
   and `/readyz` answers 503 for that whole window — including the case where the bounded
   wait elapses, which lets boot continue but is not readiness.

## Safety by scenario

| Scenario | Replay? | Result |
|---|---|---|
| Cluster-wide cold boot | Yes, automatic | Each node's first round finds the whole difference; the hash ring picks the owner, a duplicate `child_id` start is idempotent. Plugins re-attach in `Overwatch.init/1`, Entity state ships on bind. |
| One controller restarts into a healthy cluster (incl. rolling rollouts) | No | The difference against the live cluster is empty; the node reaches `:normal` after the grace and tramples nothing. |
| Single-node deployment | Yes, automatic | No peer can hand rows back; the first round starts the whole durable list. |
| Runner pod restart with `runner_id` rotation | N/A | The controller never went down; the state layout has no `runner_id` component, so the new runner receives `RestoreInitState` from the same files. |
| HotSwap migration, source runner alive | N/A | State travels in-band; `restore_on_bind/2` does not fire. Source files are left for the retention sweeper. |
| HotSwap migration, source runner already dead | N/A | The handover is empty, so the destination pulls the Entity files over `:erpc` (`Flezha.State.WalTransfer.pull_operative/3`) and replays them — one-shot, not replication. A destination crash mid-flight leaves the source files intact and ProcessHub retries. |
| Network partition during boot | Yes (suspect) | A brief duplicate-start window while healing; the hub's `PartitionTolerance.Divergence` strategy and the round's duplicate resolution settle it. Fragile environments can set `FLEZHA_AUTO_RESTORE_ENABLED=false`. |
| Quorum loss (3 nodes, 2 die) | Only the survivor's gaps | Returning nodes start only what nothing in the cluster holds; the survivor stays authoritative for what it runs. |
| Subset rejoin (cluster down, some nodes return) | Yes | **The one tolerated loss**: rows held only on non-returning nodes are gone — unless the remote tier is on, in which case their cspecs and state hydrate from the backend. |
| Registry corruption | Empty replay | The file is rotated to `registry.dets.corrupt-<monotonic>`; boot continues empty. |
| Operator restores an old `registry.dets` from backup | Per row | Its rows are declared intent again: the next round starts them. |

## Operator surface

```bash
flz boot status                # per-namespace recovery_state + registry row count
flz boot await --timeout 60    # block until every hub reports :normal (exit 1 otherwise)
flz cluster status             # recovery_state per node
```

Telemetry under `[:flezha, :boot, …]`, each with `%{hub_id, namespace}` metadata:

| Event | Fires | Measurements |
|---|---|---|
| `:pre_recovery_replay` | Before the first round | `%{cspec_count, system_time}` |
| `:recovery_complete` | After the first round | `%{child_count, succeeded, failed}`; `replay_outcome` metadata |
| `:reconcile_round` | Every round | `%{candidates, orphans, started, duplicates, elapsed_ms}`; `first_round` metadata |
| `:reconcile_duplicate` | One Operative seen on several nodes | `%{instance_count}`; `operative_id`, `kept_node`, `stopped_nodes` metadata |

`:recovery_complete` and `:reconcile_duplicate` reach the observability bus under the
`:flezha_boot` source. `reconcile_duplicate` is an **alarm, not a metric** (ProcessHub already
resolved it keep-one/stop-others): expected only while recovering from a cross-node partition,
at any other time a start bypassed the id claim, and it logs at `error`.

## Start authority

The reconcile and the convergence Loop both start Operatives; neither is gated on the other
because every start resolves through one claim, `ProcessHub.Service.Distributor.compose_start_operation/3`
(the reconcile from its round, flezha from `OperativeManager.start_operative/3` →
`ProcessHub.start_child/3`): the registry id check, backstopped by the local
`DistributedSupervisor`'s `{:already_started, pid}`. A start that loses the claim answers
`:duplicate` after verifying the winner is live and does nothing to it. The CLI renders that
as success — `flz operative start` against a live instance exits 0 as a no-op, `--partitions N`
over an existing group starts only the missing members, `flz plugin enable` on an attached
plugin reports `already_enabled` — so re-running a cold-start script is the supported heal
after a warm restart. A refused claim with **no** live winner (a stale registry row) stays a
loud error. Boot is held by `Flezha.Convergence.RecoveryGate` until hubs are `:normal`; later
a round reporting `started > 0` triggers one convergence tick that adopts what it started.

## Configuration

| Env var | Application key | Default | Purpose |
|---|---|---|---|
| `FLEZHA_AUTO_RESTORE_ENABLED` | `:flezha, :auto_restore_enabled` | `true` | Disable flag; only `true` / `1` enable (see below) |
| `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` | `:flezha, :process_hub_reconcile_grace_ms` | `45000` | Delay before the first round, and how long a hub stays `:recovering`; keep above the runner handshake window |
| `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS` | `:flezha, :process_hub_reconcile_interval_ms` | `30000` | Rate limit on later rounds |
| `FLEZHA_BOOT_REPLAY_TIMEOUT_MS` | `:flezha, :boot_replay_timeout_ms` | `60000` | How long `Boot.Loader` waits for every hub to reach `:normal`, and a connecting runner during cluster formation |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | `:flezha, :process_hub_registry_root` | `app_data/process_hub` | Parent of each `<hub_id>/registry.dets`; must be persistent storage. Not read in the test environment, where `config/test.exs` wins |

## Disable flag and rollback

`FLEZHA_AUTO_RESTORE_ENABLED=false` flips one predicate (`HubManager.durable_starts?/0`) that
every auto-restore surface reads together: hubs start with `registry_backend: :ets` (nothing
written to disk) and `auto_recovery: false` (no reconcile, straight to `:normal`), the hooks
are not registered, and starts no longer declare durable intent — Operatives must be
re-registered after every boot. Roll back by flipping the variable and restarting; an existing
`app_data/process_hub/` is ignored ([`flz reset`](reset.md) deletes it).

## See also

- [Remote durability tier](remote_durability_tier.md) — surviving the loss of the disk
- [Graceful shutdown](graceful_shutdown.md) — the solo-node drain path that relies on this
- [ProcessHub internals](../internals/process_hub.md) — the declared list and the reconcile
