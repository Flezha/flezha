# Auto-restore on boot (`auto-restore-on-boot` + `consume-recovery-mode-and-self-heal`)

- **ProcessHub dependency — a hex release.** The work this note describes
  shipped in 0.7.0. `mix.exs` declares the requirement, `mix.lock` pins the
  version, and every build path honours the lock — the controller image
  included. `make refresh-process-hub` re-resolves within the requirement
  (commit the resulting lock); `FLEZHA_PROCESS_HUB_PATH` still overrides with
  a local checkout.
- **Modules**: `lib/flezha/boot/` — `StatePathMigrator`, `RecoveryHooks`.
  The `StatePathMigrator` runs synchronously from
  `Flezha.Application.start/2` before `Flezha.State.Supervisor`.
  Cross-controller state movement lives in `Flezha.State.WalTransfer`
  (see `fix-last-runner-migration-state-loss`): a destination-pulled,
  one-shot copy at MIGRATION time. Ongoing cross-node WAL replication
  remains a non-goal — a controller that dies with its disk takes its
  operatives' state with it.
- **THE MARKER IS GONE — recovery is an ORPHAN RECONCILE.** There is no
  `cluster.healthy`, no boot mode, no arming step. Every node runs a
  round `reconcile_grace_ms` after coordinator start (then at most once
  per `reconcile_interval_ms`) that STARTS THE DIFFERENCE between its
  durable registry and what the cluster is observed to hold: a dead
  cluster's difference is the whole list (it restores itself), a live
  cluster's is empty (it starts nothing). **That first round is what
  reaches `:normal`, so the grace IS the boot gate** —
  `RecoveryGate`/`BootLoader.await_namespace_hubs` block on it, which is
  why it is 45 s (above the runner handshake window; an orphan start
  needs a runner to place against) and why every recovery test shrinks
  it to upstream's 1 s floor.
- **HubManager wiring**: `Flezha.Service.HubManager.process_hub/1`
  passes `registry_backend: {:durable_ets, [path: ...]}` and
  `auto_recovery: [reconcile_grace_ms:, reconcile_interval_ms:,
  remote_manifest:]` (from `auto_recovery_config/0`) to every
  `ProcessHub.child_spec/1`. After each `start_hub/1` it calls
  `Flezha.Boot.RecoveryHooks.register/1`, which forwards four hooks to
  telemetry: `pre_recovery_replay`, `post_recovery_replay`,
  `reconcile_round` and `reconcile_duplicate`.
- **GOTCHA — a deprecated `:auto_recovery` key is accepted with a WARN
  and silently ignored**, which is indistinguishable from working.
  `hub_manager_test`'s `auto_recovery_config/0` case round-trips the
  config through the real `Recovery.parse_config/1` for exactly that
  reason — it catches both an unread key and an out-of-range value.
- **Superseded by `durable-operator-intent`**: stopped rows and their
  TTL are gone — a deliberate stop deletes the registry row and removes
  the entry from the hub's declared set, which is what now survives the
  stopping node's absence (forever, no window). Readers meaning "what
  should be running" go through `HubManager.declared_operatives/1`;
  readers meaning "what is running with a live binding" go through
  `HubManager.live_registry_dump/1`.
- **GOTCHA — two loops now start operatives.** The reconcile starts
  orphans AND `Convergence.Loop` binds/rebinds on its own 5 s tick;
  neither is gated on the other. `reconcile_duplicate` telemetry is the
  tripwire for whether they actually contend in prod.
- **Recovery state**: there is no `RecoveryTracker` GenServer any
  more — per-hub recovery state lives in `Flezha.Convergence.Snapshot`'s
  ETS table behind `Flezha.Convergence.RecoveryGate` (no GenServer of
  its own); hooks are forwarded by `Flezha.Boot.RecoveryHooks.register/1`.
  `HubManager.recovery_state/1` and `await_recovery_complete/1,2`
  delegate there; `Flezha.BootLoader.await_namespace_hubs/0`
  and `Flezha.Runner.FleetManager.init/1` gate registry reads on it.
- **Boot CLI**: `flz boot status`, `flz boot await`,
  `flz boot import-legacy-snapshot` — implemented in `cli/src/boot.rs`
  / `Flezha.Service.CliManager.boot_*`. `flz cluster status` surfaces
  `recovery_state` per node. **DELETED with the marker**: the
  `flz cluster prepare-recovery` verb, `CliManager.cluster_prepare_recovery/1`
  and its table printer, and the `recovery_armed` column.
- **Start-fresh workflow**: `flz controller stop && flz reset --yes`
  wipes durable runtime data (`app_data/state/*`, `app_data/process_hub/*`).
  Implemented in `cli/src/reset.rs`; refuses to run while the Controller
  is up. See `docs/operations/reset.md`.
- **Persistence layers**: ProcessHub durable registry at
  `app_data/process_hub/<hub_id>/registry.dets` (cspec + `opts[:plugins]`)
  + per-Entity WAL at `app_data/state/<operative_class>/<procedure_id>/...`
  (change-by-change progress).
- **Plugin enable/disable** uses cspec-write-first ordering via
  `ProcessRegistry.update/3`; `Flezha.Service.HubManager.update_operative_plugins/3`
  was deleted (the persistent registry covers durability).
- **Runtime config knobs**: `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS`
  (45 s), `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS` (30 s),
  `FLEZHA_AUTO_RESTORE_ENABLED` (default `true`; `false` disables the
  reconcile entirely and reverts to in-memory `:ets`).
  `FLEZHA_PROCESS_HUB_STOPPED_ROW_TTL_MS` was removed with the
  tombstone model (`durable-operator-intent`); a deployment setting it
  keeps booting, the value is simply no longer read.
- **Removed by `consume-recovery-mode-and-self-heal`**:
  `Flezha.Boot.WalTransfer`; `:pre_replay_fleet_wait_ms` config key;
  `Flezha.Reconciler.{Loop, Actions, Health, Probe, Activity, Leader}`
  (replaced by what is now `Flezha.Convergence.Loop`);
  `Flezha.Reconciler.Supervisor` (top-level); `FLEZHA_RECONCILER_ENABLED`,
  `FLEZHA_LEADERSHIP_ENABLED`, `FLEZHA_RECONCILE_*` env vars.
  (`Flezha.State.Outbox` was NOT removed — it still exists and is live.)
- **Detailed notes**: `docs/operations/auto_restore_on_boot.md`.

