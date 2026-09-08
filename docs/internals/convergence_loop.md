# Convergence Loop

For contributors working on the controller's self-healing path. The convergence loop
(`lib/flezha/convergence/`) is the controller's single self-healing engine: on every tick, and on
a few declared events, it reconciles the declared operative/plugin set against observed reality
and issues bounded, idempotent heal operations. This document covers the pipeline and its
modules, the stability guards, the heal primitives, the output-stall ladder, plugin health, what
`/healthz` publishes, and the configuration, telemetry and supervision surface. The high-level
picture is in [`../architecture/kernel.md`](../architecture/kernel.md#health-and-liveness).

## Architecture

```text
Facts.declare(fact)                      ← the ONE intake, from every site
      │
Intent.declared()                        ← the hub's declared set
Probe.snapshot()                         ← reality
StallClassifier.classify()               ← the data-plane verdict
      ▼
Diff.compute(intent, reality, signals)   ← pure, over reality AND the held facts
      ▼
Planner.plan(delta, backoff)             ← pure
      ▼
Heal.execute(op) → Backoff.update(key, outcome)   ← the only side effects
      ▼
Snapshot.publish(summary)                ← /healthz, CLI, telemetry
```

`Convergence.Loop` owns the tick timer and the fact map and calls the pure modules in
sequence; all state mutation happens in `Heal`. **One chain of custody:** sensors sense, exactly
one actor acts, `/healthz` reads the actor's snapshot, the orchestrator restarts on `/healthz`.
`State.Watchdog`, `StallClassifier`, `LivenessCounters`, the WAL admission, the extension table,
the fleet manager, the bind's park funnel and the registry hooks each record a fact and declare
it through `Flezha.Convergence.Facts.declare/1`; none of them decides what to fix, none carries a
retry ladder, counter or clock of its own, and none invokes a remedy entry point directly. The
Loop is the only module that heals.

The one remedy-shaped action outside the primitives is the node drain of a graceful shutdown: an
operator verb the loop is told about (`{:node_draining, deadline}`) and never chooses, reached
through `HubManager.drain_hub/2` from the shutdown plan only.

## Modules

| Module | Role |
|---|---|
| `Convergence.Loop` | GenServer owning the tick timer, the declared-fact map, the misbound-streak map, the stuck/plugin-degraded sets and the try/rescue guard |
| `Convergence.Facts` | The one intake (`declare/1`) and the table of what may be declared: per kind, the Diff field that consumes it, the reason its op keys on, whether its key resets inline or on evidence, its grace and whether a new key ticks at once. Names no other module, so a declaring site depends on an abstraction that names no remedy |
| `Convergence.Intent` | Reads the hub's declared set (`HubManager.declared_operatives/1`) plus each operative's declared plugins from the durable cspec. With durable starts off (`HubManager.durable_starts?/0`) intent degrades to the live registry |
| `Convergence.Probe` | Reads live reality: per-operative Overwatch state (bound runner, factory, parked/suspended, running plugins, provider health via `Extension.Table.health/1`), active runners per namespace, factory placement (local runners vs other nodes' fleets), hub recovery states |
| `Convergence.Verdict` | The health record and the one health vocabulary: `%Verdict{subject, state, reason, phase, since_ms, threshold, facts}` with `state` one of `healthy \| degraded \| failed \| unbound \| parked \| suspended \| unknown`, `rank/1` (the one order every listing sorts by) and `fold/2` (the one `/healthz` table) |
| `Convergence.Probe.Table` | Every detector declared once: its fact reader, the subject kind it speaks about, the thresholds it cites and the `[:flezha, …]` events it executes. A new detector is one entry — no surface and no bridge line changes |
| `Convergence.Probe.Readers` | One fact reader per table entry, each returning `[%Probe.Fact{}]`. A reader reads; it decides nothing |
| `Convergence.Probe.Judge` | The one place a fact becomes a state: folds the readers' facts per subject in one fixed rule order into one `%Verdict{}` (`Probe.judge/2`) |
| `Convergence.Diff` | Pure: intent + reality + verdict records → `%Diff{unbound_operatives, misbound_operatives, parked_operatives, replace_operatives, stalled_operatives, resync_entities, missing_plugins, extra_plugins, unhealthy_plugins, stale_tags, data_plane_wedge, data_plane_layer, draining}` |
| `Convergence.Planner` | Pure: `%Diff{}` + `%Backoff{}` → ordered, cooldown-filtered op list capped at `max_ops` (default 20). The one place a divergence becomes an op |
| `Convergence.Heal` | The primitive table: ten idempotent primitives, each naming its executor and the evidence that resets its key — `rebind_operative`, `replace_operative`, `resync_runner`, `start_plugin`, `stop_plugin`, `restart_plugin`, `evict_runner`, `restart_runner`, `reset_control_channel`, `publish_binding` |
| `Config.Backoff` | The ladder table for the whole tree, at the foundation layer so every directory may read it: one row per key — rungs or a geometric base/cap, a jitter flag, a reset rule and a give-up bound — read by every site that waits before trying again, in the controller and in the Elixir SDK. The loop's own row is `:heal` (5 s → 30 s → 60 s → 300 s, held at 300 s), keyed `{primitive, target}`, `{primitive, {operative, plugin}}` for a plugin op, or `{reason, target}` for one carrying a declared reason. Every wait and every reset is announced on `[:flezha, :backoff, :wait]` / `[:flezha, :backoff, :reset]` |
| `Convergence.Snapshot` | Public ETS table read by `/healthz`, the CLI and telemetry: `:current` (per-tick summary), `{:recovery, hub_id}`, `{:sensor, name}` |
| `Convergence.RecoveryGate` | Hub recovery queries: `ready_namespaces/1`; `await_recovery_complete/2` behind `HubManager.await_recovery_complete/1` (the `Boot.Loader` and `FleetManager` boot gates) |
| `Flezha.SafeCall` | Not a convergence module — the control plane's bounded-call wrapper, at the root and named by five trees. Returns `{:error, reason}` on timeout / `:noproc` / any exit; probes a timed-out local target's mailbox and `current_function` |
| `Convergence.StallClassifier` | Pure data-plane localizer (`classify/3`) and producing-liveness classifier (`classify_producing/3`) — [`transport_liveness.md`](transport_liveness.md) |

## Tick pipeline

One tick per `:convergence_tick_ms` (default 5 000 ms), the first immediately on `init/1`. A tick
also runs at once when a declared fact opens a key the loop does not hold; a re-declaration of a
key it holds refreshes the time and costs no tick, so a Watchdog re-flagging a frozen Entity every
sweep, or thirty operatives losing one runner, drives one tick and not thirty. Within a tick:

1. Classify the data plane (`StallClassifier.classify/3`) when `LivenessCounters.poll_enabled?/0`, carrying the verdict, its consecutive-poll streak and the per-runner liveness facts; then the producing-liveness pass (`StallClassifier.classify_producing/3`). Both run BEFORE the plan, because the judge folds them into the records the Diff consumes.
2. `Intent.declared/0`, `Probe.snapshot/0`.
3. **Settle the facts** against the probe: drop the ones reality shows repaired, the ones nothing still names, and the ones past `:convergence_output_stall_grace_ms`, releasing the ladder each one walked; adopt a park the probe observed that nobody declared.
4. **Recovery gate:** `RecoveryGate.ready_namespaces/1` keeps namespaces whose hub is `:normal` or `:skipped`; `:recovering` / `:recovery_pending` hubs are excluded (no heal against a half-replayed registry); `:timeout` hubs (replay window expired, live ring healthy) are included with a `Logger.warning`, so one stuck hub does not disable healing elsewhere.
5. **Judge**: `Probe.judge/2` folds every detector's facts into one `%Verdict{}` per subject and executes `[:flezha, :health, :verdict]` for each state that changed. Then `Diff.compute/3` over the ready subset, whose third argument is that record map; apply the misbound-streak gate; drop `missing_plugins` whose enable is in flight (`PluginManager.pending_ids/0` — cspec first, subscribe second, so a heal would subscribe twice).
6. `Planner.plan/2`, then `Heal.execute/1` per op, each outcome recorded in `Backoff`. A plan that filled `max_ops` re-ticks at once, so the cap bounds one tick's blast radius and never becomes a rate.
7. Report-only input: the intent-mismatch check.
8. `Snapshot.publish/1` over the **full** intent, so `/healthz` sees the true intended count.

## The intake

`Flezha.Convergence.Facts.declare({kind, target, args})` is the one way a site tells the loop
something. The loop holds each fact in one map keyed `{kind, target}` with the time of its last
declaration. A fact leaves when its counter-fact arrives, when the probe shows it repaired, or
when its grace expires — and the key its remedy walked is released with it.

| Kind | Declared by | Diff field | Remedy, keyed | Cleared by |
|---|---|---|---|---|
| `output_stall` | `State.Watchdog` / `StreamChannel` telemetry | `stalled_operatives` | the ladder, `{:output_stall, id}` | the flag stopping, past the grace |
| `runner_disconnected` | `FleetManager.Session` on a closed session | `misbound_operatives` (reason `:runner_disconnected`) | `rebind_operative`, `{:runner_disconnected, id}` | the runner active again, or no operative naming it |
| `operative_parked` | `Operative.Lifecycle.park/2`, the one park funnel | `parked_operatives` | `rebind_operative`, `{:parked, id}` | the operative observed bound |
| `plugin_down` | `Extension.Table.handle_down/5` via the Overwatch | `unhealthy_plugins` | `restart_plugin`, `{:restart_plugin, {id, plugin}}` | `:running` for the stable window |
| `compaction_gave_up` | `State.Watchdog`'s strike table | `resync_entities` | `resync_runner`, `{:compaction_gave_up, entity}` | `compaction_completed` |
| `seq_gap` | `WalWriter.Admission` on the first gap of an episode | `resync_entities` | `resync_runner`, `{:seq_gap, entity}` | `seq_gap_closed` (a contiguous append) |
| `registry_written` | `HubManager`'s `child_registered` hook | `stale_tags` | `publish_binding`, `{:publish_binding, id}` | the row naming the bound runner |
| `node_draining` | `Shutdown.Coordinator` at quiesce | `draining` | — (it suppresses, never plans) | `node_drained`, at finalise |

`runner_connected` and `reconcile_started` are declared too and held by nothing: they only shorten
detection latency by ticking at once. A runner connecting also releases every `{:parked, _}` key in
its namespace — the world changed, so the parked operatives are retried at once rather than at
their next rung.

**Probe concurrency.** Every Overwatch is read through `Task.async_stream` (`max_concurrency:
schedulers_online × 4`, per-operative budget 1 000 ms + 3 × the 500 ms plugin probe budget,
`:kill_task` on timeout); an unreadable operative is recorded as `bound_runner: :unknown`.

**Tick isolation.** The tick body is wrapped in `try/rescue` + `catch`: an uncaught error logs,
emits `[:flezha, :convergence, :tick]` with `%{state: :error}`, keeps the prior state and
schedules the next tick.

**Not this pipeline's job.** A declared operative not observed running at all is started by the
hub's own reconcile through the idempotent start authority; a heal op acts only on a live Overwatch.

## Stability guards

- **Misbound-streak gate.** An operative the PROBE observed misbound must be seen so in ≥ 2 consecutive ticks before `rebind_operative` is planned; a tick on which it is not misbound clears its streak. A runner blip cannot trigger a namespace-wide migration + WAL replay. A DECLARED disconnect is not streak-gated: it is a closed session the fleet manager observed, not a probe miss.
- **No action on a failed observation.** `:unknown` — a failed `FleetManager.active_runners/1` read (namespace-wide), a failed Overwatch read (per operative) or a failed local `runners_for_factory/2` read (never `[]`) — suppresses misbound/replace classification. `:unknown` is not "no runners".
- **Suspended operatives are not healed.** A workflow instance evicted at a committed iteration boundary is unbound by design; its own wake sources bring it back.
- **Plugins diff only on a running operative** (live pid, not suspended, not parked), and `extra_plugins` only against a non-empty declared set — an empty declaration is indistinguishable from a failed cspec read.
- **Per-target backoff.** Each `{primitive, target}` cools down independently.

## Heal primitives

`Heal.execute/1` emits `[:flezha, :convergence, :heal]` and dispatches. Every operative-scoped
primitive first resolves the live pid via `HubManager.get_pid/2`; no pid is a failed heal.

**`rebind_operative`** — migrate an operative bound to a dead/wrong runner, or not yet bound
(including a parked one), onto an available runner: `OperativeManager.assign_runner/3` picks a
factory-capable runner; `{:runner_migrate, target}` (2 s bound) makes the Overwatch ping the runner
and start the operative there **before** committing the binding (a binding committed first would
name a live runner the operative cannot reach, and the Diff would stop seeing it as misbound); on
success only, `HubManager.operative_set_runner/3` flips the registry tag,
`HubManager.hook_insert_local_pid/3` seeds the new runner's pre-spawned
`StreamChannel.operative_mapping`, and `State.Recovery.resync_for_overwatch/2` replays the
per-Entity WAL through `RestoreInitState` (the HotSwap handover carries state only when the
previous runner is alive to serialise it). Emits `[:flezha, :runner, :reconnected_reassigned]` and
`[:flezha, :runner, :reconnected]` with `reason: :convergence_rebind`. `skip_rebind?/4` makes a
misbound rebind a safe no-op when the bound runner is back in the live, factory-owning set — never
for an op whose reason proves the target is not producing (`:output_stall`, `:data_plane_wedge`).
It is reached only from the plan: a runner disconnect is declared, never rerouted at the fleet manager.

**`replace_operative`** — an operative whose factory no local runner advertises while another
node's fleet does. Runs only on the node hosting the Overwatch, asks
`Overwatch.migration_consent/1`, picks the destination through the same HRW the start path uses,
and moves the child with `ProcessHub.Service.Migration.migrate_child/3`. The declared entry never
changes, so a crash mid-move heals through the hub reconcile.

Each tick also reclaims a hub this node cannot serve: a namespace whose local fleet is empty and
none of whose operatives this node hosts has its hub stopped here
(`HubManager.stop_hub_if_vacant/1`), so the distribution stops handing children to a controller
with no runners. It is not a primitive and is not planned — it names no target, takes no backoff
and repairs nothing. The guard is node-local: a peer's operatives never keep this node's hub
alive, this node's own always do, and a later runner connect starts the hub again.

**`resync_runner`** — re-issue `RestoreInitState` for every persisted Entity against the runner
the operative is **already** bound to: the rung below `rebind_operative`, no placement, no
handover. No live pid or no binding is a failed heal; the next tick reaches it through
`rebind_operative`.

**`start_plugin` / `stop_plugin`** — reconcile the running plugin set against the declared cspec
manifest through the same `{:plugin_subscribe, ...}` / `{:plugin_unsubscribe, ...}` Overwatch
calls `flz plugin enable/disable` use (2 s bound); no-ops on an already-converged plugin. The
manifest is cluster-wide: `flz plugin enable/disable` persists through `ProcessRegistry.update/4`
with `propagate: true`, as does the runner tag written by `operative_set_runner/3`.

**`restart_plugin`** — `{:plugin_restart, id, plugin}`: unsubscribe + subscribe so the provider's
`activate/1` reopens its upstream resources. If the re-subscribe fails the plugin stays
unsubscribed, diffs as missing next tick and is healed by `start_plugin` under backoff.

**`evict_runner`** — `FleetManager.evict_runner/2` drops a dead runner binding so its operatives
rebind; keyed per runner, planned before the rebinds. FleetManager refuses to evict a runner still
in the active set unless the op carries `proved_wedged: true` (a classifier-proved wedge): a
pool-wedged runner is still connected, which is what stops anything else from dislodging it.

**`reset_control_channel`** — the controller-side reconnect REQUEST, and the only remedy that
sends nothing: `FleetManager.reset_control_channel/1` clears the runner's `ControlChannel`
`stream_ref` so the SDK's next data stream claims it through the existing rebind path. Planned
from a park whose bind reported a dead transport, before the rebind that would otherwise be placed
on it; a park whose ping merely TIMED OUT is a busy runner whose live channel carries every other
instance on it, so it is retried and the channel cut only once the `{:parked, id}` key has walked
two rungs. The bind that failed classifies its own reason, and the verdict rides the fact.

**`publish_binding`** — asks the Overwatch to re-write its durable row and the runner's route from
the binding it holds. Planned last, from `stale_tags`: a start's registry write can land after a
rebind and leave `metadata.tag` naming the old runner, which the Probe reads beside the live
binding.

**Planner order:** evictions and channel resets → the data-plane wedge op (at most one) → rebinds
(misbound, then parked, then unbound) → replacements → Entity resyncs → plugin starts, stops,
restarts → row re-publishes; cooldown-filtered and capped at `max_ops`, with the output-stall
ladder appended past the cap. A rule is one `%Diff{}` field and the primitives it may plan
(`Planner.rules/0`), so a new remedy is one primitive entry plus the rule that plans it and edits
no site.

## The key and the evidence rule

One `%Backoff{}` per node over the `:heal` row, `5 s → 30 s → 60 s → 300 s`. A key is `{primitive, target}`, or
`{primitive, {operative, plugin}}` for a plugin op — per plugin, so one crashing plugin never
throttles its siblings' starts — or `{reason, target}` for an op carrying a declared reason, so
every rung of that reason's ladder walks ONE cooldown and the attempt count IS the rung.

Two reset rules exist and both are THE rule:

- a primitive whose repair is observable inline resets its key on its own `:ok`;
- a primitive whose repair is only visible later — a **ladder** — records `:error` on every application and resets only when its declared evidence arrives.

Which is which is read from the declared kinds (`Facts.ladder_reason?/1`, `ladder_primitive?/1`),
never from a case in the loop. A `restart_plugin` that reached `:running` and crashed again a
second later therefore stays on its rung: only `:running` observed for the whole 60 s stable
window, read from the extension table's own `started_at`, resets it.

## The output-stall ladder

Two sensors emit `[:flezha, :state, :output_stall]` with `operative_instance` and do nothing else:

- `State.Watchdog` (sweep every `:output_watchdog_interval_ms`, default 10 000 ms): an Entity whose Changes keep arriving but whose persisted seq has not advanced for the class's `output_stall_timeout_ms` (default 60 000 ms via `:default_output_stall_timeout_ms`). Only kinds with the `output_stall_watch?` capability (`:stream`) are swept.
- `Runner.StreamChannel`: a bound **secondary** data channel silent past `:io_stream_liveness_threshold_ms` (default 30 000 ms, checked every `:io_stream_liveness_interval_ms`, default 10 000 ms) — several missed heartbeats — and immediately on `{:flz_closed, ...}`, for every mapped operative of a stall-watched kind. Primary channels are send-only and never checked.

The Loop records the instance in `stalled_operatives`; the Diff resolves each id to a declared,
recovery-ready `{namespace, operative_id}` (which also drops a flag this tick is not healing); the
Planner emits one op per stalled operative the plan does not already cover:

| Ladder attempt | Op |
|---|---|
| first | `resync_runner` — connection-level, no migration |
| subsequent | `rebind_operative` — the stall survived the resync |

Both rungs carry `args: %{reason: :output_stall}` and share one cooldown key
(`Planner.ladder_key/1`), so escalation walks a single 5 s → 30 s → 60 s → 300 s ladder; the rung
is `Backoff.attempt/2` on that key. A ladder op's `:ok` means the remedy was *applied*, not that
the stall cleared — only the Watchdog going quiet proves that — so the Loop records `:error` for
every ladder op (which is what reaches the next rung) and resets the key when the fact ages out
after `:convergence_output_stall_grace_ms`.

## Plugin health

Channel heartbeats prove the runner link is alive, not that production is. The authoritative
production signal is the provider's own `health/1`, collected per tick through
`Extension.Table.health/1` — only on the Overwatch's node, only for `:running` plugins (a
`:prepared` one is healthy), each probe bounded by the 500 ms `ProviderBehaviour.probe_budget_ms/0`
and run off the caller. A provider without `health/1` is healthy while its owned pid is alive or
absent and `{:unhealthy, :owned_pid_dead}` otherwise. A timed-out, raised or remote probe reports
healthy — the loop never escalates on uncertainty.

| Provider | Unhealthy reasons |
|---|---|
| `operative_kafka_producer` | `:no_producer`, `:worker_dead`, `{:swap_exhausted, reason}`, `:produce_stalled` |
| `operative_websocket` | `:no_connection`, `:not_alive`, `:connection_idle`, `:data_idle` |
| `operative_http_poll` | `:no_poller`, `:poller_dead`, `{:poll_failing, reason}` after `failure_threshold` (default 3) consecutive failed polls |

The provider's answer travels **unjudged**: `Extension.Table.health/1` returns `:healthy`,
`{:unhealthy, reason}` or `{:fail_open, reason}` — one shape, normalised by
`ProviderBehaviour.normalise/1`, the only normaliser — and the loop's judge is what turns it into a
state. A plugin gets its own verdict record: `degraded` when it is not subscribed, when its owned
pid crashed, or when its provider self-reports unhealthy; `healthy` with reason `prepared` when it
is subscribed and has not activated (its PHASE, not its health, is the authority on whether it is
running); `healthy` with the fail-open reason when the probe could not answer; `unknown` when it
was never probed. An operative whose declared plugin is not running carries that as its own
`degraded` reason, so the two surfaces agree without either re-reading the other.

A declared, subscribed, unhealthy plugin diffs into `unhealthy_plugins` and is healed by
`restart_plugin` under the standard backoff. A subscriber that CRASHES is the same divergence
declared from the other side: the extension table records its phase as `{:down, reason}`, declares
`{:plugin_down, …}`, arms no timer and holds no counter, and the loop restarts it through the same
primitive. Operatives with missing or unhealthy declared plugins
are published as `plugin_degraded_operatives`; the loop logs set transitions only.

## Health publishing

After each tick the Loop calls `Snapshot.publish/1`; `Healthz.Server` reads `Snapshot.current/0`
and holds no state of its own. The published map:

- `operative_health` — the tick's verdict records, `{kind, id} => %Verdict{}`, written from exactly one site (`Loop.Summary.publish/3`) and read by every surface: `/healthz`, `flz operative info`, `flz operative list`, `flz cluster status` and the published `Flezha.Service.Operative.operative_rows/2`. `Snapshot.operative_health/2` answers a record's state, `Snapshot.verdict/2` the record, `Snapshot.record/1` a record for any subject.
- `intended_operatives`, `bound_operatives`, `producing_operatives` and `unknown_operatives` are DERIVED from those records (`Verdict.counts/1`): bound counts records that are neither `unbound`, `failed` nor `unknown` and not suspended — so a dead binding is never counted as bound — and producing counts the `healthy` ones. `plugin_degraded_operatives`, `last_tick_at`.
- `stuck_operatives` — producing-liveness verdicts (`StallClassifier.classify_producing/3` over per-Entity progress markers from `Watchdog.entity_statuses/0` joined with `InputLog.cursors/1`, one input-log read per operative per tick). Every verdict carries the fed precondition: flat markers with no input is **IDLE** and counts as producing; fed but flat past `:convergence_output_stall_grace_ms` is **STUCK** with detail `wal_frozen` (persisted seq frozen while Changes arrive), `watermark_frozen` (input-log tail grew, this Entity's consumed offset did not) or `outbox_flat` (output-bearing records accumulate while the flush cursor is flat, or the oldest dispatched output is unconfirmed past the grace). Cursor checks compare against a baseline at least one grace window old and are skipped when absent, so a tier-off operative degrades to WAL-seq-only. One deepest verdict per operative. Report-only: healing stays on the ladder.
- `data_plane_layer` / `data_plane_streak` — this tick's classifier verdict and how many consecutive polls reported that same layer.
- `intent_mismatch` — operative ids with durable evidence of intent (operative-class directories under the namespaced state root, mtime-cached, plus durable-registry rows of running hubs) absent from the live intended set, excluding finished workflows retained for inspection. A wiped registry is invisible to the live registry alone; the durable evidence is the witness. A deliberate decommission clears it by deleting the state (`flz state delete` / `flz reset`).
- `unmet_intent_ms` — how long `bound < intended` has held while no hub is recovering (`nil` otherwise); `pending_restore` — `HubManager.durable_registry_pending?/0`: durable rows exist for a hub not yet loaded, which `bound < intended` cannot catch.

### The verdict record

One record per subject per tick, and every state names ONE owner of the next step:

| State | Owner of the next step |
|---|---|
| `healthy` | nobody |
| `degraded` | the loop, in place (`restart_plugin`, `resync_runner`, a rebind on a stall) |
| `failed` | the loop, by rebinding — the runner is gone |
| `unbound` | the loop, by binding |
| `parked` | an operator or a plugin fix; the loop does not touch it |
| `suspended` | nobody until the committed boundary — planned, not a fault |
| `unknown` | the next tick; an uncertain probe escalates nothing |

`phase` is the subject's own lifecycle word and is never derived from `state`, but the two derive in
the same order — suspended, then unbound, then parked — so they cannot describe one operative
differently. A parked operative is `phase: parked, state: parked`; a suspended one that also carries
a park reason stays `phase: suspended, state: suspended`; and one held after a bind that found no
runner carries a park reason with no binding, so it is `phase: unbound, state: unbound`. `facts` carries what the readers read — for a runner the
control silence, the data silence, whether heartbeats and dispatches are advancing, and its roster
membership, side by side, so a live session over a dead data plane is two fields of one row.

The rosters cover the running namespaces only, so a runner whose namespace hub is not running is
held by none of them while its session is open. A runner no roster the tick read holds is `failed`
only once its control session has been silent for `control_liveness_window_ms`, the window the
control channel's own watchdog closes on, or was never measured. Still speaking inside that window
it is `unknown`, which escalates nothing. Its channel records carry the same two facts and are
judged by the same rule, so the runner row and its channel rows never disagree.

The states a kind can reach are declared and tested: a plugin and a driver reach `healthy`,
`degraded` and `unknown`; a channel and a runner add `failed`; an operative reaches all seven. Which
reader a verdict goes to is its subject KIND: a `driver` record is read by `/healthz` alone and the
Diff plans nothing for it; every other kind is read by the Diff and reaches `/healthz` only as
`degraded`.

**Sensor freshness.** Periodic sensors stamp their own completed pass with
`Snapshot.observe_sensor/3` (`{:sensor, name} => {at_ms, period_ms}`); `State.Watchdog` stamps
`:watchdog` after each full sweep. A sensor older than 3× its own cadence is `sensor_stale`; one
that never stamped, or whose cadence is 0, is not. **Per-hub recovery state** arrives through
ProcessHub's `recovery_state_changed` hook: `:recovering`, `:timeout` (normal via
`:recovery_timeout`), `:skipped` (normal via `:marker_present`), `:normal`.

### `/healthz` contract

Opt-in (`FLEZHA_HEALTHZ_ENABLED=true`). `GET /` → `pong\n`; unknown path 404, non-GET 405,
malformed 400. `GET /healthz` answers JSON with a `status` and, in every response, `durability` =
`remote_tier` | `local_only`. Verdicts in precedence order:

| HTTP | `status` | `reason` | When |
|---|---|---|---|
| 200 | `recovering` | — (`recovery_state`) | any hub `:recovery_pending`, `:recovering` or `:timeout` |
| 503 | `unhealthy` | `tick_stale` | no tick for > 24 × `:convergence_tick_ms` |
| 503 | `unhealthy` | `data_plane_fabric_wedged` | a `:fabric` layer (`driver_loop_frozen`, `nif_queue_backing_up`) held for ≥ `:convergence_fabric_503_polls` polls — no BEAM-level heal reaches a wedged NIF driver thread |
| 200 | `recovering` | `tick_resuming` | tick gap > 3 × tick (BEAM suspended, not wedged) |
| 200 | `recovering` | `operatives_rebinding` | any operative record `unbound` or `failed` within `:convergence_unmet_intent_grace_ms` |
| 200 | `recovering` | `restoring_registry` | `pending_restore` |
| 200 | `degraded` | `durability_rpo_exceeded` | a remote-tier RPO breach (`DurabilityTier.breaches/0`) |
| 200 | `degraded` | `output_stalled`, `plugin_unhealthy`, `parked`, `dead_runner`, `operatives_unbound`, `intent_mismatch`, `sensor_stale`, `data_plane_wedged`, joined with `+` | any operative record `degraded` or `parked`, any `unbound`/`failed` past the grace, any plugin record `degraded`, a non-empty intent mismatch, a stale sensor, or a pipeline-scoped data-plane layer |
| 200 | `healthy` | — | otherwise: every record `healthy`, `suspended` or `unknown`. The body carries `unknown_operatives` — an uncertain probe is counted, never escalated |

The fold is one ordered table in `Verdict.fold/2`, so the endpoint renders and never judges: the
words, their order and the joined reasons are the record module's, and `flz operative list` sorts a
group's worst member by the same `Verdict.rank/1`. 503 is reserved for what only an orchestrator restart can clear; a restart cannot re-bind a
runner-orphaned operative, repair missing intent or restart a supervisor-owned sensor, so those
are never 503.

## Configuration

| Env var | App key | Default | Meaning |
|---|---|---|---|
| `FLEZHA_USE_CONVERGENCE_HEALTH` | `:convergence_health_enabled` | `true` | Off switch; `false` removes `Loop` from the supervision tree. `Snapshot` always starts |
| `FLEZHA_CONVERGENCE_TICK_MS` | `:convergence_tick_ms` | `5000` | Tick interval; also the unit of the `/healthz` tick-age bounds |
| `FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS` | `:convergence_output_stall_grace_ms` | `60000` | Age-out window of a declared fact nothing re-declares, and the STUCK grace. The immediate tick fires on the first declaration regardless. Separate from the Watchdog's `output_stall_timeout_ms` |
| `FLEZHA_CONVERGENCE_UNMET_INTENT_GRACE_MS` | `:convergence_unmet_intent_grace_ms` | `120000` | How long `bound < intended` may hold before `/healthz` escalates `recovering → degraded` |
| `FLEZHA_FABRIC_503_POLLS` | `:convergence_fabric_503_polls` | `3` | Consecutive polls a fabric wedge must hold before 503; `0` disables the fabric 503 |
| `FLEZHA_HEALTHZ_ENABLED` / `_PORT` / `_BIND` | `:healthz_enabled` / `:healthz_port` / `:healthz_bind` | `false` / `9569` / `0.0.0.0` | `/healthz` listener |
| `FLEZHA_DATA_PLANE_LIVENESS` | `:observability, :data_plane_liveness` | `true` | Gates the per-tick data-plane classifier ([`transport_liveness.md`](transport_liveness.md)) |

## Telemetry

| Event | Measurements | Metadata |
|---|---|---|
| `[:flezha, :convergence, :tick]` | `%{operatives_total, actions_issued, duration_ms}` (zeros on error) | `%{state: :ticking \| :error}` |
| `[:flezha, :convergence, :heal]` | `%{count: 1}` | `%{primitive, namespace, target}` |
| `[:flezha, :convergence, :safe_call]` | `%{count: 1}` | `%{result, server}` + probe fields on timeout |
| `[:flezha, :convergence, :operative_stuck]` | `%{count: 1}` | `%{operative_id, detail, entity}` — once per fresh STUCK verdict |
| `[:flezha, :convergence, :intent_mismatch]` | `%{count}` | `%{operatives}` — when the list changes to a non-empty value |
| `[:flezha, :data_plane, :stall_localized]` | `%{count: 1}` | verdict context + `%{layer}` — on verdict change |

`[:flezha, :convergence, :safe_call]` is the seam's own event, not the loop's: every caller of
`Flezha.SafeCall` emits it, and since the Elixir SDK reaches the same seam a runner's refused
call arrives under this name too. The name is a published surface and is deliberately not
derived from the module's.

`TelemetryBridge` puts `operative_stuck` and `intent_mismatch` on the observability bus as
`:flezha_convergence` signals and `stall_localized` as `:flezha_data_plane`. A healthy cluster
emits `actions_issued: 0` on most ticks; sustained non-zero `actions_issued` in steady state is a
structural problem (read race, runner instability, an operative that never binds).

## Supervision

All children sit under `Flezha.Supervisor` (`:one_for_one`). `Convergence.Snapshot` starts early,
before `HubManager`, so the table exists before any hub fires recovery telemetry.
`Convergence.Loop` starts after the state subsystem (`Flezha.State.Watchdog` among it) that it
heals, gated on `:convergence_health_enabled`; `Healthz.Server` after it, gated on
`:healthz_enabled`. The Loop detaches and re-attaches its telemetry handler on `init/1`, so a
supervisor restart never leaves it deaf to sensor events.

## See also

- [`transport_liveness.md`](transport_liveness.md) — the data-plane classifier and the producing-liveness markers the Loop consumes
- [`runner_identity.md`](runner_identity.md) — the binding-swap primitives `rebind_operative` shares with reconnect
- [`process_hub.md`](process_hub.md) — hub recovery states and the orphan reconcile
- [`../operations/auto_restore_on_boot.md`](../operations/auto_restore_on_boot.md) — the boot-time recovery gate
- [`../resilience/overview.md`](../resilience/overview.md) — what survives what
