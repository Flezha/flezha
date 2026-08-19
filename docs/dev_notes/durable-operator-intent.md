# durable-operator-intent — implementation notes

Pairs with process_hub's `declared-children-and-remote-manifest` (landed
upstream as `5ceb6cc`, plus two follow-ups this change required: `d0d41cd`
accepts `:transient` specs for durable children, `e0f112d` exposes
`Migration.migrate_child/3`). Flezha pin: `e0f112d`.

## The model

- **Intent = the hub's declared set.** `durable: true` at the ONE start seam
  (`OperativeManager.init_operative/3` — operator starts, scheduled spawns,
  procedure spawns, partition members all funnel there; the legacy snapshot
  import passes it too). The flag is `HubManager.durable_starts?()` — off in
  lockstep with `:auto_restore_enabled`, because a hub without auto-recovery
  REFUSES durable starts (`:durable_requires_auto_recovery`).
- **Every deliberate stop rides `ProcessHub.stop_children`**
  (`OperativeManager.stop_via_hub/2`): the declared entry is removed BEFORE
  the child terminates (remove-before-terminate), so a stop can never be
  resurrected. The Overwatch's `handle_call(:stop_operative)` and
  `Lifecycle.stop/1` are GONE — deliberate teardown now runs in
  `terminate/2` under the supervisor's shutdown, which also kills any
  in-flight un-park task first (it would otherwise bind a runner for a dying
  operative). The runner drop is now async (`stop_operative_async`); the
  plugin `:on_terminate` reason is `:shutdown`, not `:stopped` — nothing
  in-tree reads the reason.
- **A transient normal exit deletes the row but NOT the declared entry** —
  that is exactly why every deliberate stop must go through the hub stop. Do
  not reintroduce a `{:stop, :normal}` deliberate-stop path.

## Load-bearing decisions and gotchas

- **cspec stays `restart: :transient`.** Upstream originally required
  `:permanent` for durable children; `:permanent` restarts on EVERY exit
  reason, which would turn the namespace-conflict claim-loser stop
  (`overwatch.ex` `{:shutdown, {:namespace_conflict, ...}}`) into a
  supervisor restart loop. Upstream was loosened instead (`d0d41cd`):
  `:transient` is durable-sound because a normal exit keeps the declared
  entry and the reconcile restarts at round cadence — visible, bounded.
- **`Intent.declared/0` reads `HubManager.declared_operatives/1`** (declared
  set parsed through the same cspec start-args shapes as the registry
  readers). Plugins still come from the REGISTRY row (`operative_plugins/2`):
  `flz plugin enable/disable` updates the row cspec, not the declared entry,
  so declared-entry plugins go stale the moment an operator touches them. A
  restore that has ONLY the declared set (rows lost) replays the start-time
  plugin manifest — accepted RPO for that catastrophe; documented in
  `docs/resilience/process_hub_recovery_model.md`.
- **Diff skips declared-but-unobserved refs.** Starting an absent declared
  child is the hub reconcile's job; a heal op needs a live Overwatch pid and
  would churn `:error` under Backoff forever. `/healthz` still reports the
  full declared count as `intended` (that is the point — a wiped runtime
  reads as unmet intent, not zero intent).
- **Remote manifest = `Flezha.State.DurabilityTier.DeclaredManifest`** over
  `Backend.resolve()` — key `declared/<hub_id>`, OUTSIDE the tier GC's
  `blob/` listing scope, so collection can never eat it. The
  version-monotonic store contract maps onto `put_if(key, blob,
  {:extend, version})`; a `{:claim_held, _}` refusal is a superseded write
  and answers `:ok`. New tier backend `Backend.LocalPath` (`:local_path`)
  gives file storage (NFS-mount grade; put_if is read-then-write, no CAS —
  documented); `:s3` and `:postgres` unchanged; k8s-style stores plug in via
  the `{module, opts}` extension point on either seam.
- **Reroute = per-operative `Heal.rebind/2`** (public wrapper over the
  primitive's body): factory-filtered HRW pick, registry flip ONLY on a
  confirmed `{:runner_migrate, _}`, `hook_insert_local_pid` + WAL resync on
  commit. `runner_reroute` spawns the rebinds off the FleetManager process —
  `assign_runner/3` calls back INTO FleetManager and would deadlock inline;
  the spawn's calls queue behind the disconnect handler, so they see the
  post-`drop_runner` active set. `HubManager.runner_switch/3` is DELETED —
  one migrate-commit contract, one placement function.
- **Re-place is `Migration.migrate_child/3`** (upstream, per-child drain-style
  handover to an explicit node), NOT stop+start: a stop would remove the
  declared entry and a crash between stop and re-add would silently LOSE the
  operative. `migrate_child` never touches the declared set. Heal executes
  `:replace_operative` only on the node hosting the Overwatch (consent —
  `Overwatch.migration_consent/1`, the drain's gate — must read the right
  disk), picks the target node HRW-deterministically, and tolerates the ring
  migrating the child back on a later topology event (the loop re-applies).
  Placement facts ride `Probe` (`placement:` — local factory runners +
  `Directory.factory_nodes`); `Diff` classifies `replace_operatives` only on
  a definite `local: []` with a non-empty remote — `:unknown` stays an
  ordinary rebind.
- **Recovery-replay tests changed meaning.** They inject rows and expect a
  replay; under the new model that is the UPGRADE path (rows predating the
  declared list), so `replay_hub`/`restart_hub(drop_declared: true)` delete
  `*.declared.dets*` between stop and start to trigger the first-enablement
  seed. Deleting those files while the hub runs is useless — the store holds
  them open and recreates on close.
- **`FakeStartRegistry`'s stop stub** now mirrors the hub stop (row removed
  before instance terminate, returns `%StopResult{}`); the Instance's
  `:stop_operative` handler is gone with the real one.

## Test map

- `test/flezha/state/durability_tier_local_path_test.exs` — LocalPath blob
  contract (always runs, unlike s3/postgres) + DeclaredManifest version
  monotonicity, per-hub keys, GC-scope isolation, tier-off refusal.
- `test/flezha/service/hub_manager_recovery_test.exs` "declared intent" —
  cold start restores after row loss; deliberate stop is permanent across
  boots; total disk loss restores via the tier (LocalPath, end-to-end through
  upstream ship/boot-adopt).
- `test/flezha/convergence/heal_rebind_contract_test.exs` — confirmed flip /
  timed-out / refused / no-advertiser, against a scriptable fake Overwatch.
- `test/flezha/convergence/convergence_test.exs` — Diff skip-unobserved,
  replace classification (+ `:unknown` and no-remote fallbacks), Planner
  replace op ordering/backoff key, seven-primitive surface.
- Real-runner e2e (`test/flezha_test.exs` `:runner_disconnect_*`) rides the
  new reroute; `hub_manager_test.exs` round-trips the new `auto_recovery`
  shape through upstream's real parser (tier on and off).
- Upstream: `test/service/migration_test.exs` (`migrate_child/3`),
  `test/process_hub_recovery_test.exs` (restartable gate).
