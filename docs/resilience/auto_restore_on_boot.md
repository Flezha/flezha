# Auto-restore on boot

This is the single restart-survival mechanism in Flezha. On boot, the
controller rehydrates the operative list from the durable ProcessHub registry
and ships the on-disk WAL + snapshot to each executor so operatives resume
from their last persisted state.

## Two persistence layers

| Layer | Path | Lifecycle owner |
|---|---|---|
| ProcessHub durable registry (operative specs incl. `opts[:plugins]`) | `app_data/process_hub/<hub_id>/registry.dets` | `Flezha.Service.HubManager` (configures `registry_backend: {:dets, ...}`) |
| Per-Segment WAL + snapshots (change-by-change progress) | `app_data/state/<operative_class>/<operation_id>/<segment_key>.{wal,snap.N,outbox.dedup}` | `Flezha.State.Supervisor` and friends |

The path layout is **flat in the operative_class direction** — there is
no `executor_id` component. An executor pod restart that rotates
`executor_id` finds the same persisted state on disk and the
controller ships it to the new executor on Segment stream attach.

## Lifecycle

1. `Flezha.Application.start/2` runs
   `Flezha.Boot.StatePathMigrator.migrate/0` synchronously **before**
   `Flezha.State.Supervisor` starts. It is idempotent: a one-shot
   migration from the legacy executor-keyed layout to the flat
   layout, no-op on subsequent runs.
2. `Flezha.Service.HubManager` starts each per-namespace ProcessHub
   with `registry_backend: {:dets, [path: ...]}` and
   `auto_recovery: [recovery_window_ms: ..., replay_timeout_ms: ...]`.
3. `Flezha.Boot.RecoveryHooks.register/1` installs thin telemetry
   forwarders for ProcessHub's `pre_recovery_replay` and
   `post_recovery_replay` hooks so the recovery lifecycle is visible
   on the observability bus.
4. ProcessHub's coordinator runs the recovery state machine
   (`:recovery_pending → :recovering | :normal`). If any peer
   transitions to `:normal` within `recovery_window_ms`, this node
   skips replay; otherwise it dispatches `start_children` for the
   persistent-registry rows.
5. Each Operative's `Overwatch.init/1` reads `operative.opts[:plugins]`
   straight from the cspec (the persistent registry already restored
   it) and rebuilds `state.extensions`. Then `start_operative_sdk` and
   `maybe_dispatch_restore_init_state/2` ship the on-disk WAL +
   snapshot to the SDK for each Segment belonging to this operative
   class — unless the operative arrived via HotSwap, in which case the
   in-band handover state stands.
6. `Flezha.BootLoader` waits for `ProcessHub.await_normal/2` per
   namespace hub before flipping `boot_ready?`.

## Safety analysis

| Scenario | Replay? | Result |
|---|---|---|
| Cluster-wide cold boot (all controllers down, all return) | Yes (every node) | Each node calls `start_children` for its persistent rows. ProcessHub's hash ring picks the canonical owner; idempotent on duplicate `child_id`. Plugins re-subscribed via `Overwatch.init/1`; Segment state shipped via `dispatch_restore_init_state/2` on stream-attach. |
| Single-controller restart into healthy cluster | No (returning node) | Returning node sees a `:normal` peer within the window, skips replay. ProcessHub has already redistributed its work; the durable registry on disk does not trample peer state. |
| Rolling k8s rollout | No | Same as single-controller, repeated per pod. No stampede. |
| Single-node deployment | Yes | Window elapses with no peer; `:recovering`; replay; `:normal`. |
| Executor pod restart with `executor_id` rotation | N/A | Controller never went down. New `executor_id` is assigned via the existing flow; flat WAL layout means state is found and shipped via `RestoreInitState`. Zero data loss. |
| HotSwap migration (operative moves A → B) | N/A | `prepare_handover_state` ships in-memory state in-band; `dispatch_restore_init_state/2` does NOT fire (handover delivers state). State files on A are not deleted (retention sweeper handles long-term orphans). |
| HotSwap mid-flight crash on destination | N/A | Source state files preserved. ProcessHub retries the migration. |
| Network partition during boot | Yes (suspect) | Brief duplicate-start window during heal; ProcessHub's `Divergence` strategy resolves. Documented limitation. Operators in fragile environments can `FLEZHA_AUTO_RESTORE_ENABLED=false`. |
| Quorum loss (3-node cluster, 2 die) | No (returning) | Survivor authoritative; returning nodes reconcile against ProcessHub placement. |
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
| `FLEZHA_RECOVERY_WINDOW_MS` | `:flezha, :recovery_window_ms` | `10_000` | How long a returning node waits for a `:normal` peer before replaying. |
| `FLEZHA_BOOT_REPLAY_TIMEOUT_MS` | `:flezha, :boot_replay_timeout_ms` | `60_000` | Cap on how long ProcessHub takes to drain the replay before declaring `:normal`. |
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
| `Flezha.State.Registry` | Flat path resolution (`{operative_class, operation_id, segment_key}`). |
| `Flezha.Service.OperativeManager.stop_operative/2` | Only operator-driven path that deletes state directories. |
| `Flezha.Service.PluginManager` | Cspec-write-first ordering: `ProcessRegistry.update/3` then runtime subscribe. |
