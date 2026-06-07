# Convergence Loop

The convergence loop (`lib/flezha/convergence/`) is the controller's single
self-healing engine. It runs a declarative pipeline on every tick and on
declared events to reconcile the desired relay/plugin state against observed
reality.

---

## Architecture

```
Intent.declared()
      │
      ▼
Probe.snapshot()          ← reality
      │
      ▼
Diff.compute(intent, reality)    ← pure function
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

---

## Modules

| Module | Role |
|--------|------|
| `Convergence.Loop` | GenServer owning the tick timer, misbound-streak table, event subscriptions, and try/rescue guard |
| `Convergence.Intent` | Reads the ProcessHub registry: declared relays, their declared plugins, and factory ids |
| `Convergence.Probe` | Reads live reality: active executors per namespace, per-relay Overwatch state, hub recovery phase |
| `Convergence.Diff` | Pure function: intent vs reality → `%Diff{unbound_relays, misbound_relays, missing_plugins, extra_plugins}` |
| `Convergence.Planner` | Pure function: `%Diff{}` + `%Backoff{}` → ordered op list, filtered by cooldown, capped at `max_ops` (default 20) |
| `Convergence.Heal` | Four idempotent primitives: `rebind_relay`, `start_plugin`, `stop_plugin`, `evict_executor` |
| `Convergence.Backoff` | Per-`{primitive, target}` cooldown table: `5 s → 30 s → 60 s → 300 s`; reset on success |
| `Convergence.Snapshot` | Shared ETS table read by `/healthz`, CLI, and telemetry; also caches per-hub recovery state |
| `Convergence.RecoveryGate` | Single query point for hub recovery state: classifies which namespaces are ready for healing; exposes `await_recovery_complete/2` |
| `Convergence.SafeCall` | `GenServer.call` wrapper returning `{:error, reason}` on timeout / `:noproc` / any exit |

---

## Tick pipeline

One tick per `convergence_tick_ms` (default 5 000 ms). The tick also fires
immediately when:

- `[:flezha, :state, :output_stall]` is emitted by `State.Watchdog`
- `:executor_connected_event` fires (an executor reconnected — an immediate tick
  detects and rebinds the relay rather than waiting up to 5 s)

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

A relay must appear misbound in **≥ 2 consecutive ticks** before a `rebind_relay`
op is issued. The `Loop` maintains a `misbound_streaks` map
`%{{ns, relay_id} => count}`. Relays that are no longer misbound on the current
tick are removed from the map.

This means a single executor blip (brief GC pause, transient netsplit) cannot
trigger a full-namespace WAL resync. The backoff table further throttles the
second and subsequent rebinds.

### Sanity gate on empty executors

If `Probe.snapshot/0` returns `active_executors: :unknown` for a namespace (a
read failure against `FleetManager`), the Diff suppresses misbound classification
for that namespace entirely. `:unknown` ≠ "no executors running" — the loop does
not act destructively on a partial observation.

### Per-target backoff

Each `{primitive, target}` key has its own cooldown entry. A failed `rebind_relay`
for relay A does not block `rebind_relay` for relay B. Backoff resets to zero on a
successful execution.

---

## Heal primitives

### `rebind_relay`

Migrates a relay from a dead or wrong executor to a live one:

1. Flip the ProcessHub registry tag via `HubManager.relay_set_executor/3`.
2. Fan the relay's pid into the new executor's pre-spawned
   `StreamChannel.relay_mapping` via `HubManager.hook_insert_pid/2`.
3. Call `State.Recovery.resync/2` to trigger `RestoreInitState` handshake so the
   executor picks up the on-disk WAL state.

Idempotent — if the relay is already correctly bound, the call is a no-op.

### `start_plugin` / `stop_plugin`

Reconcile a relay's running plugin set against its declared cspec manifest.
Both are safe to retry; `stop_plugin` on an already-stopped plugin is a no-op;
`start_plugin` on an already-running plugin is a no-op.

### `evict_executor`

Forcibly drops a dead executor binding from the FleetManager table. Called once
per dead executor (keyed per-executor in backoff, not per-relay) before the
associated relays are rebound.

---

## Plugin strike escalation

`Convergence.Loop` tracks per-relay-plugin failure strikes. When a plugin fails
heal (`start_plugin` returns `:error`) across `convergence_plugin_strike_threshold`
(default 2) consecutive attempts, the loop escalates to a full relay restart.
Emits `[:flezha, :convergence, :plugin_restart]` and
`[:flezha, :convergence, :plugin_escalated]` telemetry.

---

## Health publishing

After each tick `Loop` calls `Snapshot.publish/2` with the probed reality and a
health summary. `Healthz.Server` reads `Snapshot.current/0` — it holds no state
of its own. The published map includes:

- `bound_relays`, `producing_relays` (for the `degraded` vs `healthy` split)
- Per-hub recovery state
- Tick timestamp (used to detect a stale loop for the `tick_stale` 503 case)

---

## Configuration

| Env var | App key | Default | Meaning |
|---------|---------|---------|---------|
| `FLEZHA_USE_CONVERGENCE_HEALTH` | `:convergence_health_enabled` | `true` | Master kill-switch; `false` removes `Loop` from the supervision tree. `Convergence.Snapshot` always starts so recovery-state reads work even with the loop disabled. |
| `FLEZHA_CONVERGENCE_TICK_MS` | `:convergence_tick_ms` | `5000` | Tick interval. |
| `FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS` | `:convergence_output_stall_grace_ms` | `60000` | How long an output stall must persist before an immediate tick is triggered (separate from the watchdog's own `output_stall_timeout_ms`). |
| `FLEZHA_CONVERGENCE_PLUGIN_STRIKE_THRESHOLD` | `:convergence_plugin_strike_threshold` | `2` | Consecutive plugin-heal failures before relay restart escalation. |
| `FLEZHA_CONVERGENCE_TICK_LATENCY_THRESHOLD_MS` | `:convergence_tick_latency_threshold_ms` | `1000` | Tick-duration threshold above which `[:flezha, :convergence, :tick_slow]` is emitted. |

---

## Telemetry

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:flezha, :convergence, :tick]` | `%{duration_ms, actions_issued}` | `%{namespace}` |
| `[:flezha, :convergence, :tick_slow]` | `%{duration_ms}` | `%{}` |
| `[:flezha, :convergence, :tick_error]` | `%{}` | `%{error: inspect(e)}` |
| `[:flezha, :convergence, :heal]` | `%{}` | `%{primitive, namespace, target}` |
| `[:flezha, :convergence, :safe_call]` | `%{}` | `%{reason: :timeout \| :noproc \| :error}` |
| `[:flezha, :convergence, :plugin_restart]` | `%{}` | `%{namespace, relay_id, plugin_id}` |
| `[:flezha, :convergence, :plugin_escalated]` | `%{}` | `%{namespace, relay_id, plugin_id}` |

A healthy cluster emits `actions_issued: 0` on most ticks. Sustained non-zero
`actions_issued` in steady state indicates a structural problem (read race,
executor instability, or a relay permanently failing to bind).

---

## Supervision

`Convergence.Loop` is a child of `Flezha.Application` under `:one_for_one`,
started after `Flezha.State.Watchdog`. `Convergence.Snapshot` is started
unconditionally before it (the snapshot table must exist before the loop
publishes to it).

The kill-switch (`FLEZHA_USE_CONVERGENCE_HEALTH=false`) removes only `Loop` from
the child list; `Snapshot` stays running so `flz boot status` and recovery-state
queries continue to work.
