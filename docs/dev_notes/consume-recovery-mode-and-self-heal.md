# Convergence Health watcher (`consume-recovery-mode-and-self-heal`)

- **Modules**: `lib/flezha/convergence/health.ex` — the single
  controller-side GenServer driving bind/rebind/plugin-restart
  decisions. `lib/flezha/healthz/server.ex` — opt-in `:gen_tcp`
  HTTP/1.1 `/healthz` listener following the
  `Flezha.Observability.Export.PrometheusHttp` pattern.
- **Supervision tree**: `Flezha.Convergence.Health` is inserted in
  `Flezha.Application.start/2` AFTER `Flezha.State.Watchdog`;
  `Flezha.Healthz.Server` is inserted right after, gated on
  `:healthz_enabled` (default `false` — flezha_market opts in via
  its ConfigMap).
- **Behaviour**: one tick per `convergence_tick_ms` (default 5_000).
  On init: `HubManager.await_recovery_complete/1` blocks emissions
  until every per-namespace hub reaches `:normal | :skipped`. On
  every tick: walk `HubManager.get_all_operatives/1` per running
  namespace, classify each operative as `:healthy | :degraded | :failed
  | :unbound`, emit at most one action per operative (`:bind`,
  `:rebind`, `:restart_plugin`, `:noop`).
- **Output-stall → forced rebind**: subscribes to
  `[:flezha, :state, :output_stall]` (emitted by
  `Flezha.State.Watchdog`); on receipt, queues a `:rebind` with
  exponential back-off `5s/30s/60s/300s`. Single signal closes the
  silent-freeze failure class.
- **Plugin escalation**: `handle_plugin_failure/2` tracks strikes
  per operative-plugin and escalates to an operative restart at
  `convergence_plugin_strike_threshold` (default 2). Emits
  `[:flezha, :convergence, :plugin_restart]` and
  `[:flezha, :convergence, :plugin_escalated]` telemetry.
- **/healthz contract** (when `FLEZHA_HEALTHZ_ENABLED=true`):
  `GET /` → `pong\n`; `GET /healthz` → `200 healthy`,
  `200 recovering` (kubelet must not restart during planned
  recovery), or `503 unhealthy` with `reason ∈ tick_stale |
  output_stalled | recovery_failed`. Unknown paths → 404; non-GET
  → 405; malformed → 400. Bind defaults to `127.0.0.1`.
- **Runtime config knobs**: `FLEZHA_USE_CONVERGENCE_HEALTH`
  (default `true`), `FLEZHA_CONVERGENCE_TICK_MS` (default 5_000),
  `FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS` (default 60_000),
  `FLEZHA_CONVERGENCE_PLUGIN_STRIKE_THRESHOLD` (default 2),
  `FLEZHA_HEALTHZ_ENABLED` (default `false`),
  `FLEZHA_HEALTHZ_PORT` (default 9569).
- **flezha_market deployment**: `kubernetes/flezha-daemonset.yaml`
  ships a `livenessProbe: httpGet /healthz:9569` block;
  ConfigMap sets `FLEZHA_HEALTHZ_ENABLED=true` and
  `FLEZHA_PROCESS_HUB_REGISTRY_ROOT`. There is no cold-boot operator
  workflow any more — the reconcile is the recovery.
- **Fresh-process runner takeover**: phase 5 of the change keeps
  the takeover on the same self-healing loop instead of bolting on
  a parallel path. `FleetManager.handle_call(:runner_connect, ...)`
  dispatches `:runner_connected_event` and returns; nothing else.
  `Flezha.Convergence.Health` subscribes to that event via
  `EventManager.add_handler/1` and runs an immediate tick. The
  existing `:rebind_if_runner_available` action body — already
  responsible for migrating operatives whose `bound_runner` is no
  longer active — flips the registry tag via
  `HubManager.operative_set_runner/3` and seeds the new runner's
  pre-spawned `StreamChannel.operative_mapping` via
  `HubManager.hook_insert_pid/2` (the same fan-out the
  `child_registered` hook uses), then emits
  `[:flezha, :runner, :reconnected_reassigned]` +
  `[:flezha, :runner, :reconnected]` (mode: `:reassigned`,
  reason: `:convergence_rebind`) per reseated operative. The existing
  back-off (5s/30s/60s/300s) carries the retry semantics — a
  failed Overwatch migrate leaves the registry untouched and the
  operative stays `:degraded` for the next tick.

