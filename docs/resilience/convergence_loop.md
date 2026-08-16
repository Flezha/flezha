# Convergence Loop

The convergence loop (`lib/flezha/convergence/`) is the controller's single
self-healing engine. It runs a declarative pipeline on every tick and on
declared events to reconcile the desired operative/plugin state against observed
reality.

---

## Architecture

```
Intent.declared()
      │
      ▼
Probe.snapshot()          ← reality
StallClassifier.classify()   ← sensor signals (verdict, stalled ids)
      │
      ▼
Diff.compute(intent, reality, signals)    ← pure function
      │
      ▼
Planner.plan(delta, backoff)     ← pure function
      │
      ▼
Heal.execute(op)                  ← side effects, returns :ok | :error
      │
      ▼
Backoff.update(key, outcome)
      │
      ▼
Snapshot.publish(reality, health)   ← healthz / CLI / telemetry
```

`Convergence.Loop` owns the tick timer and the event subscriptions; it
orchestrates the pipeline by calling the pure modules in sequence. All state
mutation is in `Heal`; every other module is side-effect-free.

**One chain of custody.** Sensors sense, exactly one actor acts, healthz reads
the actor's snapshot, the orchestrator restarts on healthz — each level watches
the level below. `State.Watchdog`, `StallClassifier`, `LivenessCounters` and
`RecoveryTracker` emit facts and invoke no remedy of their own; the Loop is the
only module that heals. That is why the Watchdog no longer calls
`State.Recovery.resync/2` for an output stall: two uncoordinated remediation
paths for one signal is the failure this design exists to remove.

---

## Modules

| Module | Role |
|--------|------|
| `Convergence.Loop` | GenServer owning the tick timer, misbound-streak table, event subscriptions, and try/rescue guard |
| `Convergence.Intent` | Reads the ProcessHub registry: declared operatives, their declared plugins, and factory ids |
| `Convergence.Probe` | Reads live reality: active runners per namespace, per-operative Overwatch state (incl. provider plugin health via `Extension.Table.health/1`), hub recovery phase |
| `Convergence.Diff` | Pure function: intent + reality + this tick's sensor signals (classifier verdict, stalled ids) → `%Diff{unbound_operatives, misbound_operatives, stalled_operatives, missing_plugins, extra_plugins, unhealthy_plugins, data_plane_wedge, data_plane_layer}` |
| `Convergence.Planner` | Pure function: `%Diff{}` + `%Backoff{}` → ordered op list, filtered by cooldown, capped at `max_ops` (default 20). The ONE place a divergence becomes an op |
| `Convergence.Heal` | Six idempotent primitives: `rebind_operative`, `resync_runner`, `start_plugin`, `stop_plugin`, `restart_plugin`, `evict_runner` |
| `Convergence.Backoff` | Per-`{primitive, target}` cooldown table: `5 s → 30 s → 60 s → 300 s`; reset on success |
| `Convergence.Snapshot` | Shared ETS table read by `/healthz`, CLI, and telemetry; also caches per-hub recovery state |
| `Convergence.RecoveryGate` | Single query point for hub recovery state: classifies which namespaces are ready for healing; exposes `await_recovery_complete/2` |
| `Convergence.SafeCall` | `GenServer.call` wrapper returning `{:error, reason}` on timeout / `:noproc` / any exit |

---

## Tick pipeline

One tick per `convergence_tick_ms` (default 5 000 ms). The tick also fires
immediately when:

- `[:flezha, :state, :output_stall]` is emitted (by `State.Watchdog`, or by
  `Runner.StreamChannel` when a bound data channel goes silent past
  `:io_stream_liveness_threshold_ms` — i.e. several missed data-channel
  heartbeats, not merely absent operative output)
- `:runner_connected_event` fires (a runner reconnected — an immediate tick
  detects and rebinds the operative rather than waiting up to 5 s)
- a `[:flezha, :boot, :reconcile_round]` round completes with `started > 0`
  (the ProcessHub orphan reconcile started operatives — the tick adopts them
  rather than a later tick finding them half-bound). A quiet round triggers
  nothing.

An output stall already recorded only refreshes its timestamp: the Watchdog
re-flags a frozen Entity every sweep, and the ladder for that operative is
already walking on the periodic tick, so re-flagging must not drive the whole
loop at sweep cadence.

**Recovery gate:** `RecoveryGate.ready_namespaces/1` is called on every tick to
determine which namespaces are eligible for healing. A namespace's hub must be in
`:normal` or `:skipped` to be included. Hubs in `:recovering` or `:recovery_pending`
are excluded — healing against a partially-replayed registry would produce false
divergence. Hubs in `:timeout` (DETS replay window expired; live cluster ring still
healthy) are **included** with a `Logger.warning` so one stuck hub does not silently
disable healing for every other namespace. `await_recovery_complete/2` (used by
`HubManager` and `BootLoader`) is also on `RecoveryGate`.

**Tick isolation:** the entire tick body is wrapped in `try/rescue`. An uncaught
exception logs an error and emits `[:flezha, :convergence, :tick_error]`
telemetry; the loop survives and schedules the next tick normally.

---

## Stability guards

### Misbound-streak gate

An operative must appear misbound in **≥ 2 consecutive ticks** before a
`rebind_operative` op is issued. The `Loop` maintains a `misbound_streaks` map
`%{{ns, operative_id} => count}`. Operatives that are no longer misbound on the
current tick are removed from the map.

This means a single runner blip (brief GC pause, transient netsplit) cannot
trigger a full-namespace WAL resync. The backoff table further throttles the
second and subsequent rebinds.

### Sanity gate on empty runners

If `Probe.snapshot/0` returns `active_runners: :unknown` for a namespace (a
read failure against `FleetManager`), the Diff suppresses misbound classification
for that namespace entirely. `:unknown` ≠ "no runners running" — the loop does
not act destructively on a partial observation.

### Per-target backoff

Each `{primitive, target}` key has its own cooldown entry. A failed
`rebind_operative` for operative A does not block `rebind_operative` for operative
B. Backoff resets to zero on a successful execution.

---

## Heal primitives

### `rebind_operative`

Migrates an operative from a dead or wrong runner to a live one:

1. Flip the ProcessHub registry tag via `HubManager.operative_set_runner/3`.
2. Fan the operative's pid into the new runner's pre-spawned
   `StreamChannel.operative_mapping` via `HubManager.hook_insert_pid/2`.
3. Call `State.Recovery.resync/2` to trigger `RestoreInitState` handshake so the
   runner picks up the on-disk WAL state.

Idempotent — if the operative is already correctly bound, the call is a no-op.

`Heal.skip_rebind?/4` normally skips a rebind whose bound runner is back in the
live, factory-owning active set — a stale snapshot must not churn a healthy
operative. It does NOT skip when the op carries `reason: :output_stall` or
`reason: :data_plane_wedge`: both prove the binding is not producing, which is
exactly what the guard is not meant to protect.

### `resync_runner`

Re-issues the `RestoreInitState` handshake for every persisted Entity of an
operative against the runner it is **already** bound to — the same replay a
rebind performs after migrating, without the migration. It is the rung below
`rebind_operative`: no placement decision, no handover, no WAL replay through a
new binding. An operative with no live pid or no binding has nothing to resync;
that is a failed heal, and the next tick reaches it through `rebind_operative`.

### `start_plugin` / `stop_plugin`

Reconcile an operative's running plugin set against its declared cspec manifest.
Both are safe to retry; `stop_plugin` on an already-stopped plugin is a no-op;
`start_plugin` on an already-running plugin is a no-op.

The declared manifest is cluster-wide: `flz plugin enable/disable` persists
through `ProcessRegistry.update/4` with `propagate: true`, which re-applies the
cspec rewrite on every hub node (2 s per-peer budget, best-effort; an
unreachable peer converges on rejoin via peer sync). The same propagation
applies to the runner tag written by `operative_set_runner/3`. Without it a
restart driven from another node's registry copy yields a running-but-pluginless
operative that the loop cannot heal — the 2026-06-12 production gap.

### `restart_plugin`

Tears down and re-subscribes a declared plugin whose provider self-reports
unhealthy, so `activate/1` reopens its upstream resources (websocket reconnect,
Kafka producer worker rebuild). Routed through Overwatch
`{:plugin_restart, id, plugin}` → `Extension.Table` unsubscribe + subscribe.
If the re-subscribe fails the plugin stays unsubscribed and the next tick
retries it as a missing plugin under backoff.

### `evict_runner`

Forcibly drops a dead runner binding from the FleetManager table. Called once
per dead runner (keyed per-runner in backoff, not per-operative) before the
associated operatives are rebound.

`FleetManager.evict_runner/2` normally refuses to evict a runner that is in the
active set, for the same reason `skip_rebind?` exists. An op carrying a proved
wedge passes `proved_wedged: true` and waives it: a pool-wedged runner is still
connected, and being connected is precisely what stops anything else from
dislodging it.

---

## The output-stall ladder

`State.Watchdog` emits `[:flezha, :state, :output_stall]` and does nothing
else. The Loop records the instance in `stalled_operatives` (aged out after
`:convergence_output_stall_grace_ms`) and passes the flagged ids into
`Diff.compute/3` as a signal; the Diff resolves each to a declared
`{namespace, operative_id}` (which is also what drops a flag for an operative
this tick is not healing), and the Planner emits one op per stalled operative
the plan does not already cover:

| Ladder attempt | Op |
|---|---|
| first eligible | `resync_runner` — cheap, connection-level, no migration |
| subsequent | `rebind_operative` — the stall survived the resync |

Both rungs carry `args: %{reason: :output_stall}` and share ONE cooldown key
(`Planner.ladder_key/1`), so escalating walks a single 5 s → 30 s → 60 s →
300 s ladder instead of restarting the throttle under a fresh key. The rung is
read off that entry's attempt count — no second per-operative counter.

A ladder op's `:ok` means the remedy was *applied*, not that the stall cleared:
only the Watchdog going quiet proves that. The cooldown therefore advances on
either outcome (which is what reaches the next rung), and the entry is reset
when the operative ages out of `stalled_operatives` — a operative that resumed
producing starts again from the cheap rung.

The ladder is appended past `max_ops`: a operative that has stopped producing
must not be starved by a large structural divergence filling the tick's budget.

---

## Plugin health (production-liveness detection)

Channel heartbeats prove the runner link is alive, not that production is.
The authoritative production signal is the plugin provider's own `health/1`
self-report, collected per tick by `Probe` through `Extension.Table.health/1`:

- `operative_kafka_producer` → `{:unhealthy, :produce_stalled | :swap_exhausted |
  :worker_dead | :no_producer}` on a sustained produce stall or dead worker.
- `operative_websocket` → `{:unhealthy, :no_connection | :not_alive |
  :connection_idle | :data_idle}` on a dead or silent upstream connection.
- `operative_http_poll` → `{:unhealthy, :no_poller | :poller_dead |
  {:poll_failing, reason}}` after `failure_threshold` consecutive failed polls.
  Requests run off the poller process, so the verdict is available for exactly
  the hanging endpoint that would otherwise starve the probe.
- An uncertain probe (timeout, raise, no `health/1` export, Overwatch on a
  different node) reports healthy — the loop never escalates on uncertainty.

A declared, subscribed, unhealthy plugin diffs into `unhealthy_plugins` and is
healed by `restart_plugin` under the standard backoff. Operatives with missing or
unhealthy declared plugins are published as `plugin_degraded_operatives`;
`/healthz` reports `degraded` (HTTP 200, reason `plugin_unhealthy`) while the set
is non-empty, and the loop logs set transitions only.

---

## Health publishing

After each tick `Loop` calls `Snapshot.publish/2` with the probed reality and a
health summary. `Healthz.Server` reads `Snapshot.current/0` — it holds no state
of its own. The published map includes:

- `bound_operatives`, `producing_operatives` (for the `degraded` vs `healthy` split)
- `intended_operatives` (declared count); `bound < intended` ⇒ `/healthz` is
  `recovering` (post-restart rebind window), never `healthy`
- `stalled_operatives`, `stuck_operatives`, and `plugin_degraded_operatives`
  (all subtract from `producing_operatives`; `/healthz` composes the degraded
  reason from `output_stalled` / `plugin_unhealthy` / `intent_mismatch`)
- `stuck_operatives` — the producing-liveness verdicts
  (`StallClassifier.classify_producing/3` over per-Entity durable progress
  markers read from `Flezha.State.Watchdog.entity_statuses/0` at tick
  cadence). Every verdict carries the fed precondition: an operative whose
  markers are flat with no input arriving is **IDLE** and counts as
  producing (`/healthz` stays healthy); one that is fed but whose markers
  stay flat past `:convergence_output_stall_grace_ms` is **STUCK** and does
  not. STUCK detail is `wal_frozen` (persisted seq frozen while Changes
  arrive), or — once the effectively-once tier lands — `watermark_frozen`
  (input-log tail grows, min-watermark flat) / `outbox_flat` (output-bearing
  records accumulate, flush cursor flat); absent tier cursors degrade the
  classification to WAL-seq-only. One root cause freezing several markers
  yields the single deepest verdict per operative. Heartbeats and channel
  state are never producing-liveness inputs. Report-only: healing stays on
  the output-stall ladder.
- `data_plane_layer` / `data_plane_streak` — this tick's data-plane
  classifier verdict and how many consecutive polls have reported that same
  layer (a different layer, or `:ok`, restarts the count). A layer the
  convergence pipeline can heal surfaces as `200 degraded` with reason
  `data_plane_wedged`; a controller-fabric layer (`driver_loop_frozen`,
  `nif_queue_backing_up`) sustained for `:convergence_fabric_503_polls` polls
  is `503 data_plane_fabric_wedged`, because no BEAM-level heal reaches a
  wedged NIF driver thread — every heal this controller could issue travels
  through it. See `docs/resilience/transport_liveness.md`.
- **Sensor freshness.** Periodic sensors stamp their own last completed pass
  into `Snapshot.observe_sensor/3` (`{:sensor, name} => {at_ms, period_ms}`);
  `State.Watchdog` does so after each full sweep. `/healthz` reports a sensor
  older than 3× its own cadence as `200 degraded, reason: sensor_stale`
  naming it. A sensor that has never stamped is not stale (boot is not a
  wedge), and neither is one whose cadence is `0` (deliberately disabled).
  The sensor writes the mark rather than being polled for it, so reading it
  never blocks on the very process that wedged — timestamps, not a
  watchdog-for-the-watchdog loop.
- `intent_mismatch` — operatives with durable evidence of prior intent
  (first-level `app_data/state/` directories, mtime-cached, plus
  durable-registry rows of running hubs) that are absent from the live
  intended set. A wiped registry cannot be detected from the live registry
  alone — the durable evidence is the witness. Non-empty ⇒ `/healthz` is
  `200 degraded` with reason `intent_mismatch` naming the operatives, never
  a vacuous `200 healthy` and never 503 (a kubelet restart cannot repair
  missing intent). A deliberate decommission clears the entry by deleting
  the state (`flz state delete` / `flz reset`); recovery states take
  precedence while a hub is mid-replay.
- `unmet_intent_ms` — how long `bound < intended` has held while not
  recovering (nil otherwise). Once it exceeds
  `:convergence_unmet_intent_grace_ms`, `/healthz` escalates `recovering →
  degraded` (HTTP 200, reason `operatives_unbound`) so an operative stuck unbound
  alarms instead of sitting silently `recovering` forever. Not 503: a
  controller restart cannot re-bind a runner-orphaned operative. The clock
  resets while any hub is recovering.
- `operative_health` — a `{namespace, operative_id} => :healthy | :degraded | :failed |
  :unbound | :unknown` map, classified per tick from the same probed reality the
  `Diff` uses (bound + live-runner + stall/plugin signals). This is the
  authoritative source for the `HEALTH` column in `flz operative list` and the
  `Operative health:` rollup in `flz cluster status` — `HubManager.operative_rows/1`
  reads `Snapshot.operative_health/2`, not registry metadata. An operative reports
  `:unknown` only before the first tick observes it (or on a failed read that
  tick); it is a transient startup state, never permanent.
- Per-hub recovery state
- Tick timestamp (used to detect a stale loop for the `tick_stale` 503 case)

---

## Configuration

| Env var | App key | Default | Meaning |
|---------|---------|---------|---------|
| `FLEZHA_USE_CONVERGENCE_HEALTH` | `:convergence_health_enabled` | `true` | Master kill-switch; `false` removes `Loop` from the supervision tree. `Convergence.Snapshot` always starts so recovery-state reads work even with the loop disabled. |
| `FLEZHA_CONVERGENCE_TICK_MS` | `:convergence_tick_ms` | `5000` | Tick interval. |
| `FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS` | `:convergence_output_stall_grace_ms` | `60000` | How long a recorded stall stays live in `stalled_operatives` (the age-out window / STUCK grace). The immediate tick fires on the *first* flag; this knob does not delay it. Separate from the watchdog's own `output_stall_timeout_ms`. |
| `FLEZHA_CONVERGENCE_UNMET_INTENT_GRACE_MS` | `:convergence_unmet_intent_grace_ms` | `120000` | How long `bound < intended` may hold (while not recovering) before `/healthz` escalates `recovering → degraded` and the `:unmet_intent` alarm fires. ~10 ticks at the default cadence. |
| `FLEZHA_FABRIC_503_POLLS` | `:convergence_fabric_503_polls` | `3` | Consecutive classifier polls a controller-fabric wedge must hold before `/healthz` answers 503. `0` disables the fabric 503 — the knob and its escape hatch are the same setting. |

---

## Telemetry

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:flezha, :convergence, :tick]` | `%{operatives_total, actions_issued, duration_ms}` | `%{state: :ticking \| :error}` |
| `[:flezha, :convergence, :heal]` | `%{count}` | `%{primitive, namespace, target}` |
| `[:flezha, :convergence, :safe_call]` | `%{count}` | `%{result, server, …probe fields}` |
| `[:flezha, :convergence, :operative_stuck]` | `%{count: 1}` | `%{operative_id, detail, entity}` — emitted once per fresh STUCK verdict, not on steady state |
| `[:flezha, :convergence, :intent_mismatch]` | `%{count}` | `%{operatives}` — emitted when the mismatch list changes to a non-empty value |

Both new events are bridged onto the observability bus as `:flezha_convergence`
signals by `TelemetryBridge`.

A healthy cluster emits `actions_issued: 0` on most ticks. Sustained non-zero
`actions_issued` in steady state indicates a structural problem (read race,
runner instability, or an operative permanently failing to bind).

---

## Supervision

`Convergence.Loop` is a child of `Flezha.Application` under `:one_for_one`,
started after `Flezha.State.Watchdog`. `Convergence.Snapshot` is started
unconditionally before it (the snapshot table must exist before the loop
publishes to it).

The kill-switch (`FLEZHA_USE_CONVERGENCE_HEALTH=false`) removes only `Loop` from
the child list; `Snapshot` stays running so `flz boot status` and recovery-state
queries continue to work.
