# Convergence self-heal loop (`consume-recovery-mode-and-self-heal`)

- **Modules**: `lib/flezha/convergence/loop.ex` — `Flezha.Convergence.Loop`,
  the single controller-side convergence actor (this change built it as
  `Convergence.Health`; `close-control-plane-silent-wedges` restructured
  it into the Intent → Probe → Diff → Planner → Heal loop).
  `lib/flezha/healthz/server.ex` — opt-in HTTP/1.1 `/healthz` listener,
  now on the shared `Flezha.Http.Listener`.
- **Supervision tree**: `Flezha.Convergence.Loop` is inserted in
  `Flezha.Application.start/2` AFTER `Flezha.State.Watchdog`;
  `Flezha.Healthz.Server` after it, gated on
  `:healthz_enabled` (default `false` — flezha_market opts in via
  its ConfigMap).
- **Behaviour**: one tick per `convergence_tick_ms` (default 5_000).
  `init/1` never blocks on recovery — each tick filters the intent to
  namespaces whose hubs are `:normal | :skipped | :timeout` via
  `RecoveryGate.ready_namespaces/1`, so a mid-replay namespace is
  skipped while the others heal. Diff/Planner mint backoff-filtered
  primitives per tick — `:rebind_operative | :resync_runner |
  :start_plugin | :stop_plugin | :restart_plugin | :evict_runner` —
  executed by `Heal`.
- **Output-stall → forced rebind**: subscribes to
  `[:flezha, :state, :output_stall]` (emitted by
  `Flezha.State.Watchdog`); on receipt, queues a `:rebind_operative`
  with exponential back-off `5s/30s/60s/300s`. Single signal closes the
  silent-freeze failure class.
- **Plugin healing**: a plugin divergence is a per-tick
  `:restart_plugin` primitive from the Planner. The old
  `handle_plugin_failure/2` strike counter, the escalation to an
  operative restart and the `:plugin_restart`/`:plugin_escalated`
  telemetry are GONE.
- **/healthz contract** (when `FLEZHA_HEALTHZ_ENABLED=true`):
  `GET /` → `pong\n`; `GET /healthz` → `200 healthy`,
  `200 recovering` (kubelet must not restart during planned
  recovery), `200 degraded` (output-stalled / stuck operatives), or
  `503 unhealthy` ONLY when the Loop itself is wedged — `reason ∈
  tick_stale | recovery_failed | data_plane_fabric_wedged`. Unknown
  paths → 404; non-GET → 405; malformed → 400. Bind defaults to
  `0.0.0.0` (`FLEZHA_HEALTHZ_BIND`).
- **Runtime config knobs**: `FLEZHA_CONVERGENCE_TICK_MS`
  (default 5_000), `FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS`
  (default 60_000), `FLEZHA_HEALTHZ_ENABLED` (default `false`),
  `FLEZHA_HEALTHZ_PORT` (default 9569), `FLEZHA_HEALTHZ_BIND`
  (default `0.0.0.0`). `FLEZHA_USE_CONVERGENCE_HEALTH` and
  `FLEZHA_CONVERGENCE_PLUGIN_STRIKE_THRESHOLD` are still SET in
  `runtime.exs` but read nowhere in `lib/` — inert.
- **flezha_market deployment**: `kubernetes/flezha-daemonset.yaml`
  ships a `livenessProbe: httpGet /healthz:9569` block;
  ConfigMap sets `FLEZHA_HEALTHZ_ENABLED=true` and
  `FLEZHA_PROCESS_HUB_REGISTRY_ROOT`. There is no cold-boot operator
  workflow any more — the reconcile is the recovery.
- **Fresh-process runner takeover**: the takeover rides the same
  loop, not a parallel path — but the binding swap itself is now
  SYNCHRONOUS in `FleetManager.handle_call(:runner_connect, ...)`
  (`resolve_stable_identity/4` is the primary path); the
  `:runner_connected_event` it then dispatches is only a safety-net
  trigger. `Flezha.Convergence.Loop` subscribes to it via
  `EventManager.add_handler/1` and runs an immediate tick. The
  `:rebind_operative` primitive (`Heal.rebind_operative/2`) — already
  responsible for migrating operatives whose `bound_runner` is no
  longer active — flips the registry tag via
  `HubManager.operative_set_runner/3` and seeds the new runner's
  pre-spawned `StreamChannel.operative_mapping` via
  `HubManager.hook_insert_pid/2` (the same fan-out the
  `child_registered` hook uses), then emits
  `[:flezha, :runner, :reconnected_reassigned]` +
  `[:flezha, :runner, :reconnected]` per reseated operative. The
  back-off (5s/30s/60s/300s) carries the retry semantics — a
  failed Overwatch migrate leaves the registry untouched for the
  next tick.

