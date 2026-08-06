# convergence-self-healing Specification

## Purpose
TBD - created by archiving change unified-convergence-loop. Update Purpose after archive.
## Requirements
### Requirement: Single convergence loop owns all self-healing decisions

The system MUST run exactly one self-healing decision loop (`Flezha.Convergence.Loop`). No module other than the Loop emits heal actions (rebind, restart-plugin, reconnect-stream, evict-runner, etc.). Watchdog, FleetManager, HubManager, Healthz, StreamChannel — each emits *events* or *facts*; none of them decide what to fix.

#### Scenario: stall is observed in one place, decision happens in another
- **GIVEN** the State Watchdog detects output stall for operative `btc_kraken` procedure 0
- **WHEN** the Watchdog flags the stall
- **THEN** the Watchdog emits a `[:flezha, :state, :output_stall]` event with `%{operative_class, operative_instance, procedure, key}`
- **AND** the Watchdog performs no heal action of its own
- **AND** the Convergence Loop records the stalled instance, and on its next tick the Diff classifies the divergence, the Planner emits a `rebind_operative` op (with `reason: :output_stall`), and the Heal executes it under per-target backoff.

### Requirement: The reconciler is a pure-function pipeline, not scattered handlers

The self-healing decision logic MUST be expressed as a pure-function pipeline under `lib/flezha/convergence/`: `Probe.snapshot/0` (observe reality) → `Intent` (declared state) → `Diff.compute/2` (divergence delta) → `Planner` (ordered, backoff-filtered ops) → `Heal.execute/1` (bounded primitives). Adding a failure mode means extending the Diff/Planner/Heal seams, NOT adding a new self-healing module or scattering decision branches across services. The heal surface is a fixed, small set of idempotent primitives.

Recovery-gating queries (namespace readiness, await-recovery-complete) SHALL be issued via `Flezha.Convergence.RecoveryGate`. `Probe` SHALL NOT expose `recovery_state/1`, `recovery_states/0`, or `await_recovery_complete/2` as its own functions; callers MUST use `RecoveryGate` directly.

#### Scenario: a new divergence reuses the existing seams
- **GIVEN** a new failure mode "wedged stream channel" must be healed
- **WHEN** it is added
- **THEN** the Probe surfaces the fact (per-pid strike counters), the Diff classifies it (`wedged_channels`), the Planner emits an existing primitive (`reconnect_stream`), and the Heal executes it
- **AND** no new top-level self-healing GenServer is introduced.

#### Scenario: recovery-gate queries go through RecoveryGate

- **WHEN** the Loop needs to determine which namespaces are ready for healing
- **THEN** it calls `Flezha.Convergence.RecoveryGate.ready_namespaces/1`
- **AND** there is no equivalent function exposed on `Probe` or implemented inline in `Loop`

### Requirement: A hub stuck in `:recovery_timeout` does not permanently block healing for its namespace

The convergence loop MUST include hubs in `:recovery_timeout` state in the ready set returned by `RecoveryGate.ready_namespaces/1`. Heal procedures for operatives in a timed-out hub's namespace MUST proceed using the live cluster state (the authoritative source), not be gated indefinitely.

A `Logger.warning` MUST be emitted for each `:recovery_timeout` namespace included in the ready set so operators are alerted to the stuck hub.

#### Scenario: timed-out hub does not block healthy operatives

- **GIVEN** a controller hosting two namespaces: `ns_ok` (hub `:normal`) and `ns_timeout` (hub `:recovery_timeout`)
- **WHEN** a convergence tick runs
- **THEN** the Loop heals misbound operatives in both `ns_ok` and `ns_timeout`
- **AND** a `Logger.warning` is emitted naming `ns_timeout` as timed-out

#### Scenario: normal procedure is unchanged for healthy hubs

- **GIVEN** all hubs are in `:normal` or `:skipped`
- **WHEN** a convergence tick runs
- **THEN** no warning is emitted and healing proceeds as before

### Requirement: Every heal primitive is a safe no-op on a healthy target

Each heal primitive MUST re-read the authoritative live state at execution time and do nothing if the target is no longer broken. A detection slip (a reconnect or recovery between the snapshot and the heal) MUST NOT disrupt a working operative, runner, or producer.

#### Scenario: a reconnected runner is not evicted
- **GIVEN** the Diff snapshot marked runner `default_X` for eviction
- **AND** `default_X` re-established its transport connection before the heal ran
- **WHEN** `Heal.execute(:evict_runner)` runs for `default_X`
- **THEN** FleetManager observes `default_X` is currently `:active` and performs no eviction
- **AND** the operatives bound to `default_X` keep producing.

#### Scenario: an output-stalled operative is always re-wired
- **GIVEN** operative `btc_kraken` is bound to an active runner but has not produced output past the stall grace
- **WHEN** the Planner emits a `rebind_operative` op with `reason: :output_stall`
- **THEN** the Heal migrates the operative to an available runner even though its bound runner is active (the silent-freeze self-heal MUST NOT be skipped by the healthy-target guard).

### Requirement: Bounded calls and backoff prevent heal-induced churn

Every cross-process call inside the Loop MUST be wrapped in `Convergence.SafeCall.bounded/3` (timeout + `:exit` catch + telemetry). Repeated heals of the same target MUST be suppressed by per-target exponential backoff so a persistent fault is not re-healed every tick.

#### Scenario: a flapping target is backed off
- **GIVEN** operative `r1` was just rebound by the Loop
- **WHEN** the same divergence is observed on the next tick
- **THEN** the Planner filters the repeat `rebind_operative` op under backoff and does not re-issue it until the backoff window elapses.

### Requirement: Producing-liveness derives from durable progress markers with a fed precondition

The convergence snapshot SHALL classify each bound operative's producing-liveness from durable progress markers compared between ticks — per-Entity WAL persisted seq always, and, when the operative runs the effectively-once tier, per-Entity durable consumed-offset advance and outbox `flushed_seq` drain. Channel state and heartbeats SHALL NOT be producing-liveness inputs.

The tier cursors SHALL be read per Entity. An Entity's durable consumed offset is the offset the controller has fsynced for it, and the minimum across an operative's Entities is that operative's watermark — so a single frozen Entity freezes the operative's verdict while the verdict still names the Entity responsible.

Every flat-marker verdict SHALL apply the fed precondition over the grace window: input-side evidence advancing while the progress marker is flat past the grace period classifies the operative STUCK (not producing); no input-side advance with a flat marker classifies it IDLE (producing-healthy). For tier-on operatives, an input-log tail growing while an Entity's durable consumed offset is flat is a STUCK verdict, and output-bearing WAL records accumulating while `outbox_flushed_seq` is flat is a STUCK verdict. When one root cause freezes multiple markers, the snapshot SHALL report the single deepest verdict for that operative.

Cursor reads SHALL NOT create or resurrect durable state for an operative that has none: an operative without a live input log SHALL yield no cursor fields, and its classification SHALL degrade to WAL-seq-only rather than fail or default.

All marker reads SHALL be off-path at the convergence tick cadence, reusing counters and cursors that already advance; no per-message instrumentation is added.

#### Scenario: Idle operative stays healthy

- **WHEN** an operative receives no input and all its progress markers are flat across ticks
- **THEN** it classifies IDLE and counts as producing; `/healthz` remains `200 healthy`

#### Scenario: Fed but frozen operative is STUCK

- **WHEN** input-side evidence for an operative advances across ticks while its WAL persisted seq stays flat past the grace period
- **THEN** the snapshot classifies it STUCK, `producing_operatives` excludes it, and `/healthz` reports `200 degraded`

#### Scenario: Frozen consumed offset with growing input-log is STUCK (tier on)

- **WHEN** a tier-on operative's input-log tail grows across ticks while one of its Entities' durable consumed offset does not advance past the grace period
- **THEN** the operative classifies STUCK with a watermark-frozen detail naming that Entity

#### Scenario: A checkpoint-ack operative that stops consuming is STUCK

- **WHEN** a checkpoint-ack operative's input-log tail grows across ticks while its Entities' durable consumed offsets stay flat past the grace period
- **THEN** the operative classifies STUCK, even though its WAL persisted seq is legitimately flat between checkpoints and it is exempt from the catastrophic-dataflow sweep

#### Scenario: Undrained outbox with accumulating output records is STUCK (tier on)

- **WHEN** output-bearing WAL records accumulate for an Entity across ticks while its `outbox_flushed_seq` stays flat past the grace period
- **THEN** the operative classifies STUCK with an outbox-flat detail naming that Entity

#### Scenario: A tier-off operative is classified on the WAL seq alone

- **WHEN** an operative that has not declared the effectively-once tier is classified
- **THEN** no cursor fields are present on its markers, no input log is created for it, and its verdict is derived from WAL fed/advance evidence only

### Requirement: An empty intended set with durable evidence of intent is never vacuously healthy

On each tick the convergence snapshot SHALL compare the live intended set against durable evidence of prior intent (per-Operative state directories under the state root and durable-registry entries). Any operative with durable evidence but absent from the live intended set SHALL be listed as `intent_mismatch`. When the mismatch list is non-empty — including the case where the live intended set is entirely empty — `/healthz` SHALL report `200 degraded` with `reason: intent_mismatch` naming the operatives, and SHALL NOT report `200 healthy`. The condition SHALL NOT produce `503 unhealthy` (a kubelet restart cannot repair missing intent). Deleting the operative's durable state (`flz state delete`, `flz reset`) clears the mismatch.

#### Scenario: Registry wipe degrades health instead of reporting healthy

- **WHEN** the durable registry is wiped so the live intended set is empty, while per-Operative state directories from prior procedure exist on disk
- **THEN** `/healthz` reports `200 degraded` with `reason: intent_mismatch` listing the evidenced operatives, not `200 healthy`

#### Scenario: Deliberate decommission clears the mismatch

- **WHEN** an operator stops an operative and deletes its durable state
- **THEN** the operative no longer appears in `intent_mismatch` and health returns to `200 healthy`

#### Scenario: Fresh install with no evidence is healthy

- **WHEN** a controller boots with an empty intended set and no durable state directories or registry entries
- **THEN** `/healthz` reports `200 healthy`

### Requirement: /healthz never lies

The `/healthz` HTTP endpoint MUST derive `bound_operatives`, `producing_operatives`, recovery state, and the intent-mismatch list from a single `Convergence.Snapshot.current/0` reader. It MUST NOT compute these from independent paths. `producing_operatives` MUST be derived from the producing-liveness classification (durable progress markers with the fed precondition) — an IDLE operative counts as producing, a STUCK operative does not. It returns `200 healthy`, `200 degraded` (bound ≠ producing, plugin-degraded, or non-empty intent mismatch), `200 recovering`/`restoring_registry` (planned recovery — kubelet MUST NOT restart), or `503 unhealthy` ONLY on an authoritative dead-loop (`tick_stale`) or `recovery_failed`. An empty intended set MUST NOT short-circuit to `200 healthy` while durable evidence of intent exists.

#### Scenario: healthz reflects reality even when bound and producing diverge

- **GIVEN** 10 operatives are bound and 3 classify STUCK (fed but progress markers flat past `output_stall_grace_ms`)
- **WHEN** `/healthz` is queried
- **THEN** the body reports `bound_operatives=10`, `producing_operatives=7`, and the status is `200 degraded` (not 200 healthy, not 503).

#### Scenario: healthz does not 503 on a slow-but-live loop

- **GIVEN** the Loop completes ticks slowly under CPU pressure but is not dead
- **WHEN** `/healthz` is queried
- **THEN** the status is `200` (healthy/degraded), and `503` fires only when the loop has not ticked within the authoritative tick-age bound.

#### Scenario: a recovery timeout surfaces as 200 recovering, not 503

- **GIVEN** a hub has recovery state `:timeout` (DETS replay window expired but live cluster ring is healthy)
- **WHEN** `/healthz` is queried
- **THEN** the response is `200 recovering` with `recovery_state: "recovery_timeout"` in the body
- **AND** the response is NOT `503 unhealthy`
- **AND** kubelet does not restart the pod

#### Scenario: idle fleet is healthy, wiped fleet is not

- **GIVEN** two controllers: one with 5 intended operatives all IDLE (no input), one with an empty intended set but durable state directories for 5 operatives
- **WHEN** `/healthz` is queried on each
- **THEN** the idle controller reports `200 healthy` with `producing_operatives=5`, and the wiped controller reports `200 degraded` with `reason: intent_mismatch`

### Requirement: Output-stalled operatives SHALL be actively rebound, not only reported as degraded

When `Flezha.Convergence.Loop` receives an `{:output_stall, operative_id}` message from the Watchdog telemetry handler, it SHALL:

1. Record the stall timestamp in `stalled_operatives` (existing behaviour — for healthz reporting).
2. Trigger an immediate convergence tick (same as `runner_connected_event`).

On each tick, after `Planner.plan/2` produces its plan, the Loop SHALL inject additional `rebind_operative` ops for every operative whose `operative_id` is in `stalled_operatives` and which is not already covered by the plan, using `args: %{reason: :output_stall}`. These injected ops SHALL be filtered through the standard per-target `Backoff` so a persistently stalled operative follows the 5s → 30s → 60s → 300s cooldown, not a per-tick storm.

`Heal.skip_rebind?/4` SHALL NOT skip a rebind when `args.reason == :output_stall`, even if the operative's currently-bound runner is in the live active set. The output-stall condition proves the binding is not producing; the active-runner guard exists only to avoid disrupting a healthy operative, which an output-stalled operative is not.

#### Scenario: output-stalled operative is rebound on the next tick

- **GIVEN** operative `btc_kraken` is bound to active runner `ex1` and has not produced output past the output-stall grace period
- **WHEN** the Watchdog flags it and the convergence Loop receives the stall event
- **THEN** the Loop triggers an immediate tick
- **AND** a `rebind_operative` op with `args: %{reason: :output_stall}` is issued for `btc_kraken`
- **AND** `skip_rebind?` returns `false` even though `ex1` is still active
- **AND** the operative is migrated to a new runner and begins producing again

#### Scenario: repeated stalls are backoff-throttled

- **GIVEN** operative `btc_kraken` was just rebound due to an output stall 2 seconds ago
- **WHEN** the Watchdog flags another output stall for `btc_kraken`
- **THEN** the injected rebind op is filtered out by Backoff (5s cooldown not yet elapsed)
- **AND** no rebind is issued on this tick

### Requirement: Rolling upgrade preserves producing-operatives count

An operator-initiated rolling restart of the controller or runner workload MUST complete with no data loss, measured by kafka offset advance per declared operative. The runner presents its persisted `previous_runner_id` on reconnect so the controller evicts the stale binding and rebinds atomically.

#### Scenario: rolling restart of the runner workload
- **GIVEN** the cluster is producing `producing_operatives = N`
- **WHEN** the runner pods are rolled one at a time (graceful SIGTERM, peer stays up)
- **THEN** the new runner presents its `previous_runner_id`, the controller evicts the old binding and rebinds within a tick, and kafka offsets advance with no data loss.

### Requirement: Host sleep / wake recovers automatically

The system MUST detect a wall-clock vs monotonic-clock divergence indicating sleep/hibernate and, on wake, kick reconnects across SDK control streams and producers without operator action.

#### Scenario: host sleep then wake
- **GIVEN** the system is producing `producing_operatives = N`
- **WHEN** the host sleeps for ≥ 30 s and wakes
- **THEN** the Loop observes the wake, reconnects affected streams/producers, and kafka offsets per operative resume advancing without an operator command.

### Requirement: Plugins are durable across controller restart

When `flz plugin enable <operative> <plugin>` succeeds, the plugin MUST be persisted into the operative's ProcessHub cspec start-args **on every node of the hub cluster**, regardless of which node served the CLI call. The same cluster-wide propagation MUST apply to plugin disable and to the registry runner-tag write issued by Heal rebinds. On restart (recovery mode, plain restart, or node-death redistribution from any node), every restored operative MUST re-instantiate its declared plugins idempotently, and the convergence I2 backstop MUST re-add a declared-but-missing plugin to a running operative. A peer node that is unreachable at write time MUST converge on rejoin via the peer-sync recovery path; propagation failures MUST be logged, never silently dropped.

#### Scenario: plugin survives recovery-mode restart
- **GIVEN** `flz plugin enable btc_kraken kafka-producer` was executed
- **WHEN** the controller is restarted in recovery mode
- **THEN** `btc_kraken` shows kafka-producer and the producer functions within a few ticks of boot.

#### Scenario: a dropped plugin is re-instantiated on a running operative
- **GIVEN** a declared plugin is force-unsubscribed at runtime while its operative stays up
- **WHEN** the Loop ticks
- **THEN** the Diff detects the missing plugin and the Heal re-subscribes it from the declared cspec.

#### Scenario: plugin enabled via one node is declared on every node
- **GIVEN** a two-node hub cluster with a operative registered on both nodes' registries
- **WHEN** `flz plugin enable` is served by node B
- **THEN** node A's registry row for the operative contains the plugin in its cspec start-args, and a operative restart driven from node A's cspec re-instantiates the plugin.

#### Scenario: redistribution after node death restores a producing operative
- **GIVEN** a operative whose plugins were enabled through any node of the cluster
- **WHEN** the node hosting the operative's Overwatch dies and the hub redistributes the operative to a surviving node
- **THEN** the restarted operative re-attaches its full declared plugin set without operator action.

### Requirement: Unhealthy declared plugins are detected and restarted by the loop

The convergence loop SHALL judge production liveness of a operative's plugins by the plugin provider's own `health/1` self-report (`Extension.Table.health/1`), never by traffic-volume proxies. A plugin that is declared, currently subscribed, and self-reports `{:unhealthy, reason}` SHALL produce a `restart_plugin` heal op routed through the extension table's existing restart machinery, subject to the loop's per-target exponential backoff. An uncertain health probe (timeout, raise, missing provider callback) SHALL be treated as healthy so the loop never escalates on uncertainty.

#### Scenario: wedged kafka producer is restarted after broker recovery
- **GIVEN** a operative with a declared kafka producer plugin and an unreachable broker long enough for the provider to self-report `{:unhealthy, :produce_stalled}`
- **WHEN** the Loop ticks
- **THEN** a `restart_plugin` op is executed for that plugin under backoff, and once the broker is reachable a restart attempt succeeds and the verdict clears to healthy.

#### Scenario: healthy plugins are never restarted
- **GIVEN** a operative whose declared plugins all self-report `:healthy`
- **WHEN** the Loop ticks
- **THEN** no `restart_plugin` op is planned for that operative.

#### Scenario: uncertain probe does not escalate
- **GIVEN** a plugin whose `health/1` raises or times out
- **WHEN** the Loop ticks
- **THEN** the plugin is treated as healthy and no heal op is issued.

### Requirement: Plugin-degraded operatives surface on /healthz and telemetry

A operative with at least one declared plugin that is missing or self-reported unhealthy SHALL be published to the convergence snapshot as plugin-degraded. While any operative is plugin-degraded, `/healthz` SHALL report `degraded` with HTTP 200 and a `plugin_unhealthy` reason — degraded MUST NOT trigger a kubelet restart. The loop SHALL log plugin-health verdict transitions (degraded set becomes non-empty, changes membership, or clears), not steady-state.

#### Scenario: pluginless running operative is degraded, not healthy
- **GIVEN** a running operative whose declared cspec lists plugins that are not subscribed
- **WHEN** `/healthz` is queried before the heal completes
- **THEN** the response is HTTP 200 with status `degraded` and reason `plugin_unhealthy`.

#### Scenario: verdict clears after heal
- **GIVEN** a plugin-degraded operative
- **WHEN** the loop re-attaches or successfully restarts the affected plugins
- **THEN** the snapshot's plugin-degraded set empties and `/healthz` returns to `healthy` with a single transition log line.

### Requirement: Fresh-process runner takeover is atomic

When an SDK runner process restarts and presents `previous_runner_id`, the controller MUST evict the previous binding and bind the new id atomically, with no window where a operative is bound to both ids or to a corpse.

#### Scenario: SDK process restart preserves operative binding
- **GIVEN** operative `btc_kraken` is bound to runner `default_X` and producing
- **WHEN** the SDK process is killed and restarts, reading its persisted `runner_id = default_X` and announcing it as `previous_runner_id`
- **THEN** the controller evicts the stale `default_X` binding inside the connect handler before the new bind, re-attaches the operative, and kafka offsets resume within a couple of ticks
- **AND** no entry in the active runner set references a dead `default_X`.

## Removed Requirements

### Requirement: `Backoff.schedule_ms/0` is part of the public API

**Reason**: The function has zero callers in application code. It was added as a test-convenience export but is not referenced anywhere. Removing it trims the public API surface.

**Migration**: Any test or external code that calls `Backoff.schedule_ms/0` should inline the value `[5_000, 30_000, 60_000, 300_000]` or read the module attribute `@schedule_ms` if needed for compile-time assertions.

