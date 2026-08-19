# Auto-restore on boot

This is the restart-survival mechanism in Flezha for a controller whose disk
survived. On boot, the controller rehydrates the operative list from the
durable ProcessHub registry and ships the on-disk WAL + snapshot to each runner
so operatives resume from their last persisted state. (Losing the disk itself
is the [remote durability tier](../remote_durability_tier.md)'s problem — when
a backend is configured, both layers below can be rehydrated from it first.)

## Two persistence layers

| Layer | Path | Lifecycle owner |
|---|---|---|
| ProcessHub durable registry (operative specs incl. `opts[:plugins]`) | `app_data/process_hub/<hub_id>/registry.dets` | `Flezha.Service.HubManager` (configures `registry_backend: {:durable_ets, ...}`) |
| Per-Entity WAL + snapshots (change-by-change progress) | `app_data/state/<operative_class>/<procedure_id>/<entity_key>.{wal,snap.N,outbox.dedup}` | `Flezha.State.Supervisor` and friends |
| *(unless opted out)* remote manifest + artifacts backing both layers above, plus the DLQ and the control-plane stores | Postgres / S3 / file, `FLEZHA_DURABILITY_BACKEND` | `Flezha.State.DurabilityTier` (`Shipper` out, `Hydrator` back) |

The path layout is **flat in the operative_class direction** — there is
no `runner_id` component. A runner pod restart that rotates
`runner_id` finds the same persisted state on disk and the
controller ships it to the new runner on Entity stream attach.

## Lifecycle

1. `Flezha.Application.start/2` runs
   `Flezha.Boot.StatePathMigrator.migrate/0` synchronously **before**
   `Flezha.State.Supervisor` starts. It is idempotent: a one-shot
   migration from the legacy runner-keyed layout to the flat
   layout, no-op on subsequent runs. When a remote durability backend is
   configured, `DurabilityTier.Hydrator.hydrate_cspecs_on_boot/0` also runs
   here — a one-shot child ahead of `HubManager`, because the registry cspec
   must be on disk *before* any hub opens `registry.dets`, or a late-landing
   cspec is invisible to the orphan reconcile.
2. `Flezha.Service.HubManager` starts each per-namespace ProcessHub
   with `registry_backend: {:durable_ets, [path: ...]}` and
   `auto_recovery: [reconcile_grace_ms:, reconcile_interval_ms:,
   remote_manifest:]`. `:durable_ets` serves reads from ETS — which
   is what keeps the reconcile's per-round registry reads cheap — and
   mirrors writes to disk synchronously.
3. `Flezha.Boot.RecoveryHooks.register/1` installs thin telemetry
   forwarders for ProcessHub's `pre_recovery_replay`,
   `post_recovery_replay`, `reconcile_round` and `reconcile_duplicate`
   hooks so the recovery lifecycle is visible on the observability bus.
4. ProcessHub's coordinator runs the recovery state machine as an
   **orphan reconcile**: `reconcile_grace_ms` after start it runs a
   round that converges the cluster toward the hub's declared set —
   starting declared children observed running nowhere, stopping
   running durable children whose entry was removed — then repeats at
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
| Cluster-wide cold boot (all controllers down, all return) | Yes — automatic | Each node's first reconcile round finds the whole difference between its durable registry and the (empty) observed cluster, and calls `start_children` for its persistent rows. ProcessHub's hash ring picks the canonical owner; idempotent on duplicate `child_id`. Plugins re-subscribed via `Overwatch.init/1`; Entity state shipped via `dispatch_restore_init_state/2` on stream-attach. No operator step. |
| Single-controller restart into healthy cluster | No | The reconcile's difference against the live cluster is empty, so the returning node starts nothing and reaches `:normal` after the grace. ProcessHub has already redistributed its work; the durable registry on disk does not trample peer state. |
| Rolling k8s rollout | No | Same as single-controller, repeated per pod. No stampede. |
| Single-node deployment | Yes — automatic | No peer can hand the rows back, so the first reconcile round's difference is the whole durable list: `:recovering` → replay → `:normal`, with no operator step. |
| Runner pod restart with `runner_id` rotation | N/A | Controller never went down. New `runner_id` is assigned via the existing flow; flat WAL layout means state is found and shipped via `RestoreInitState`. Zero data loss. |
| HotSwap migration, source runner alive (operative moves A → B) | N/A | `prepare_handover_state` serializes from the runner and ships the state in-band; `dispatch_restore_init_state/2` does NOT fire (handover delivers state). State files on A are not deleted (retention sweeper handles long-term orphans). |
| HotSwap migration, source runner already dead (A lost its last runner) | N/A | The runner is the state source, so the handover is **empty** and `arrived_via_hotswap?` stays `false`. B pulls A's Entity files over `:erpc` (`Flezha.State.WalTransfer`) into its own `state_root` and replays them. One-shot at migration time, not replication (unless the remote durability tier is enabled, which ships artifacts continuously). |
| HotSwap mid-flight crash on destination | N/A | Source state files preserved. ProcessHub retries the migration. |
| Network partition during boot | Yes (suspect) | Brief duplicate-start window during heal; ProcessHub's `Divergence` strategy resolves. Documented limitation. Operators in fragile environments can `FLEZHA_AUTO_RESTORE_ENABLED=false`. |
| Quorum loss (3-node cluster, 2 die) | Only the survivor's gaps | Returning nodes observe the survivor's live rows and start only what nothing in the cluster holds; the survivor stays authoritative for what it already runs. |
| Subset-rejoin (cluster down, only some nodes return) | Yes | **Single tolerated data-loss case** — rows held only on the non-rejoining nodes are lost (with the remote tier enabled, the missing nodes' cspecs and state hydrate from the backend instead). |
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

## Start authority

The orphan reconcile and the convergence Loop are not gated on each other —
they do not need to be, because **every** start resolves through one claim and
they cooperate rather than race.

**One claim.** Both paths reach the same seam:
`Distributor.compose_start_operation(hub, specs, check_existing: true)` — the
reconcile via `Recovery.start_orphans/4`, flezha via
`OperativeManager.start_operative/3` → `ProcessHub.start_child/3`, which is a
call into the hub coordinator. The claim itself is
`Distributor.init_registry_check/3`, with the local `DistributedSupervisor`'s
`{:already_started, pid}` as the backstop for the registry-propagation window.
A start that loses the claim answers `:duplicate` after verifying the winner is
live, and does **nothing** to the winner — never stops, restarts, or rebinds it.

The CLI renders that concession as success: `flz operative start` against a
live instance exits 0 as an explicit no-op, a `--partitions N` re-run over an
existing group starts only the missing members (converge), and
`flz plugin enable` on an already-attached plugin exits 0 untouched. Together
they make re-running a cold-start script the supported heal after a warm
restart — the script attaches whatever is missing and leaves the rest alone. A
claim refused with **no** live winner behind it (a stale registry row) stays a
loud error.

**Cooperation, not a hold.** ProcessHub's `reconcile_round` hook fires at round
*completion*, so an in-flight hold is not buildable from it — and the window
where contention concentrates, boot, is already held (the Loop blocks on
`RecoveryGate` until hubs reach `:normal`, and the first reconcile round is what
reaches `:normal`). For steady-state rounds: a round reporting `started > 0`
triggers one immediate convergence tick, whose plan *adopts* the started
operatives — they probe as bound, so no op targets them. A round reporting
`started = 0` triggers nothing.

**`reconcile_duplicate` is an alarm, not a metric.** Upstream it reports one
operative observed on more than one node, already resolved keep-one/stop-others
(`kept_node` / `stopped_nodes` ride the event, and the keeper is the node the
distribution strategy assigns). That is inherent while recovering from a
cross-node partition. Fired at any other time it means a start bypassed the
claim, so it is logged at `error` and bridged to the observability bus as
`:flezha_boot / :reconcile_duplicate`.

## Configuration

| Env var | Application key | Default | Purpose |
|---|---|---|---|
| `FLEZHA_AUTO_RESTORE_ENABLED` | `:flezha, :auto_restore_enabled` | `true` | Kill-switch. When `false`, the auto-recovery hooks do not register; durable registry mutations still happen. |
| `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS` | `:flezha, :process_hub_reconcile_grace_ms` | `45_000` | How long after coordinator start the first reconcile round runs. That round is what reaches `:normal`, so this is also how long a hub stays `:recovering`. Set above the runner handshake window: an orphan start needs a runner to place against. |
| `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS` | `:flezha, :process_hub_reconcile_interval_ms` | `30_000` | Rate limit on subsequent rounds. See "Start authority" below for how the reconcile and the convergence Loop stay out of each other's way. |
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
