# Auto-restore on boot

This is the single restart-survival mechanism in Flezha. On boot, the
controller rehydrates the operative list from the durable ProcessHub registry
and ships the on-disk WAL + snapshot to each runner so operatives resume
from their last persisted state.

## Two persistence layers

| Layer | Path | Lifecycle owner |
|---|---|---|
| ProcessHub durable registry (operative specs incl. `opts[:plugins]`) | `app_data/process_hub/<hub_id>/registry.dets` | `Flezha.Service.HubManager` (configures `registry_backend: {:durable_ets, ...}`) |
| Per-Entity WAL + snapshots (change-by-change progress) | `app_data/state/<operative_class>/<procedure_id>/<entity_key>.{wal,snap.N,outbox.dedup}` | `Flezha.State.Supervisor` and friends |

The path layout is **flat in the operative_class direction** — there is
no `runner_id` component. A runner pod restart that rotates
`runner_id` finds the same persisted state on disk and the
controller ships it to the new runner on Entity stream attach.

## Lifecycle

1. `Flezha.Application.start/2` runs
   `Flezha.Boot.StatePathMigrator.migrate/0` synchronously **before**
   `Flezha.State.Supervisor` starts. It is idempotent: a one-shot
   migration from the legacy runner-keyed layout to the flat
   layout, no-op on subsequent runs.
2. `Flezha.Service.HubManager` starts each per-namespace ProcessHub
   with `registry_backend: {:durable_ets, [path: ...]}` and
   `auto_recovery: [reconcile_grace_ms:, reconcile_interval_ms:,
   stopped_row_ttl_ms:]`. `:durable_ets` serves reads from ETS — which
   is what keeps the reconcile's per-round registry reads cheap — and
   mirrors writes to disk synchronously.
3. `Flezha.Boot.RecoveryHooks.register/1` installs thin telemetry
   forwarders for ProcessHub's `pre_recovery_replay`,
   `post_recovery_replay`, `reconcile_round` and `reconcile_duplicate`
   hooks so the recovery lifecycle is visible on the observability bus.
4. ProcessHub's coordinator runs the recovery state machine as an
   **orphan reconcile**: `reconcile_grace_ms` after start it runs a
   round that starts the difference between this node's durable
   registry and what the cluster is observed to hold, then repeats at
   most once per `reconcile_interval_ms`. That first round is what
   moves the hub `:recovering` → `:normal`, so the grace is what the
   boot gate waits on — it is set above the runner handshake window
   because an orphan start needs a runner to place against. There is
   no marker and no operator arming step: a cold cluster recovers
   itself, and a node rejoining a live cluster starts nothing because
   the difference is empty.
5. Each Operative's `Overwatch.init/1` reads `operative.opts[:plugins]`
   straight from the cspec (the persistent registry already restored
   it) and rebuilds `state.extensions`. Then `start_operative_sdk` and
   `maybe_dispatch_restore_init_state/2` ship the on-disk WAL +
   snapshot to the SDK for each Entity belonging to this operative
   class — unless the operative arrived via HotSwap, in which case the
   in-band handover state stands.
6. `Flezha.BootLoader` waits for `ProcessHub.await_normal/2` per
   namespace hub before flipping `boot_ready?`.

## Safety analysis

| Scenario | Replay? | Result |
|---|---|---|
| Cluster-wide cold boot (all controllers down, all return) | Only where the marker was cleared first | Each such node calls `start_children` for its persistent rows. ProcessHub's hash ring picks the canonical owner; idempotent on duplicate `child_id`. Plugins re-subscribed via `Overwatch.init/1`; Entity state shipped via `dispatch_restore_init_state/2` on stream-attach. A cold boot with markers intact comes back empty until an operator arms replay. |
| Single-controller restart into healthy cluster | No | The marker survived on disk, so the returning node goes straight to `:normal`. ProcessHub has already redistributed its work; the durable registry on disk does not trample peer state. |
| Rolling k8s rollout | No | Same as single-controller, repeated per pod. No stampede. |
| Single-node deployment | Only with the marker cleared | No peer can hand the rows back, so a restart is empty until the marker is removed while the node is down; then `:recovering`; replay; `:normal`. |
| Runner pod restart with `runner_id` rotation | N/A | Controller never went down. New `runner_id` is assigned via the existing flow; flat WAL layout means state is found and shipped via `RestoreInitState`. Zero data loss. |
| HotSwap migration, source runner alive (operative moves A → B) | N/A | `prepare_handover_state` serializes from the runner and ships the state in-band; `dispatch_restore_init_state/2` does NOT fire (handover delivers state). State files on A are not deleted (retention sweeper handles long-term orphans). |
| HotSwap migration, source runner already dead (A lost its last runner) | N/A | The runner is the state source, so the handover is **empty** and `arrived_via_hotswap?` stays `false`. B pulls A's Entity files over `:erpc` (`Flezha.State.WalTransfer`) into its own `state_root` and replays them. One-shot at migration time, not replication. |
| HotSwap mid-flight crash on destination | N/A | Source state files preserved. ProcessHub retries the migration. |
| Network partition during boot | Yes (suspect) | Brief duplicate-start window during heal; ProcessHub's `Divergence` strategy resolves. Documented limitation. Operators in fragile environments can `FLEZHA_AUTO_RESTORE_ENABLED=false`. |
| Quorum loss (3-node cluster, 2 die) | No (returning) | Returning nodes keep their markers, so they skip replay; survivor authoritative and they reconcile against ProcessHub placement. |
| Subset-rejoin (cluster down, only some nodes return) | Yes | **Single tolerated data-loss case** — rows held only on the non-rejoining nodes are lost. |
| Mixed-version rolling upgrade | Per-node | Forward+backward-compatible row schema. |
| Registry corruption | Empty replay | DETS rotates to `corrupt-<monotonic>`; boot continues empty. |
| Operator restored old `operatives.dets` from backup | Per-row | Eager reconcile drops rows ProcessHub doesn't claim. |

## Operator surface

```bash
# Per-namespace recovery state and registry counts.
flz boot status

# Block until every hub reports :normal.
flz boot await --timeout 60

# One-shot migration from the removed `flz cluster snapshot` format.
flz boot import-legacy-snapshot /path/to/snap.flz [--force]
```

## Configuration

| Env var | Application key | Default | Purpose |
|---|---|---|---|
| `FLEZHA_AUTO_RESTORE_ENABLED` | `:flezha, :auto_restore_enabled` | `true` | Kill-switch. When `false`, the auto-recovery hooks do not register; durable registry mutations still happen. |
| `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` | `:flezha, :process_hub_reconcile_grace_ms` | `45_000` | How long after coordinator start the first reconcile round runs. That round is what reaches `:normal`, so this is also how long a hub stays `:recovering`. Set above the runner handshake window: an orphan start needs a runner to place against. |
| `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS` | `:flezha, :process_hub_reconcile_interval_ms` | `30_000` | Rate limit on subsequent rounds. `Flezha.Convergence.Health` binds on its own 5 s tick, so both loops can act on the same missing operative; `reconcile_duplicate` telemetry is what reports it when they do. |
| `FLEZHA_PROCESS_HUB_STOPPED_ROW_TTL_MS` | `:flezha, :process_hub_stopped_row_ttl_ms` | `2_592_000_000` (30 d) | How long a deliberately stopped operative keeps its durable row. The row is what makes the stop survive a node's absence instead of being resurrected by a peer, so this is the window in which `flz operative stop` stays honoured. |
| `FLEZHA_BOOT_REPLAY_TIMEOUT_MS` | `:flezha, :boot_replay_timeout_ms` | `60_000` | How long boot waits for every hub to leave `:recovery_pending`, and how long a connecting runner waits for that during cluster formation. Not passed to ProcessHub — its own gate is `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS`. |
| `FLEZHA_PROCESS_HUB_REGISTRY_ROOT` | `:flezha, :process_hub_registry_root` | `app_data/process_hub` | Parent dir for each per-namespace `<hub_id>/registry.dets`. |

## Kill-switch and rollback

`FLEZHA_AUTO_RESTORE_ENABLED=false` disables the
`pre_recovery_replay` and `post_recovery_replay` hook registration —
ProcessHub's auto-recovery remains structurally configured but the
state machine short-circuits to `:normal`. Durable registry writes
still happen; mutators continue to persist operative specs and plugin
subscriptions to DETS. Rollback path: flip the env var, optionally
delete `app_data/process_hub/`, and the controller behaves like the
pre-change version (operators must re-register operatives after every
boot).

## Related modules

| Module | Role |
|---|---|
| `Flezha.Service.HubManager` | Configures `registry_backend` + `auto_recovery`; calls `Flezha.Boot.RecoveryHooks.register/1` on each `start_hub/1`. |
| `Flezha.Boot.RecoveryHooks` | Telemetry forwarders for `pre_recovery_replay` / `post_recovery_replay` lifecycle events. |
| `Flezha.Boot.StatePathMigrator` | One-shot legacy-layout → flat-layout migration. |
| `Flezha.Operative.Overwatch` | `arrived_via_hotswap?` gate; `maybe_dispatch_restore_init_state/2` ships `RestoreInitState` to the SDK. |
| `Flezha.State.Registry` | Flat path resolution (`{operative_class, procedure_id, entity_key}`). |
| `Flezha.Service.OperativeManager.stop_operative/2` | Only operator-driven path that deletes state directories. |
| `Flezha.Service.PluginManager` | Cspec-write-first ordering: `ProcessRegistry.update/3` then runtime subscribe. |
