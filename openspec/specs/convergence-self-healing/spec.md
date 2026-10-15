# convergence-self-healing Specification

## Purpose
TBD - created by archiving change unified-convergence-loop. Update Purpose after archive.
## Requirements
### Requirement: Single convergence loop owns all self-healing decisions

The system MUST run exactly one self-healing decision loop (`Flezha.Convergence.Loop`), and every remedy MUST be one of its declared primitives — `rebind_relay`, `resync_runner`, `start_plugin`, `stop_plugin`, `restart_plugin`, `evict_runner`, `restart_runner`, `reset_control_channel`, `publish_binding` — planned by the Planner and executed by `Heal.execute/1`. No module other than the Loop emits heal actions. Watchdog, FleetManager, HubManager, Healthz, StreamChannel, the Overwatch's bind, the WAL admission, the extension table and the registry hooks each record a fact and declare it through `Loop.declare/1`; none of them decide what to fix, none carry a retry ladder, counter or clock of its own, and none invoke a remedy entry point directly. In particular the Watchdog SHALL NOT call `Recovery.resync` (or any other remedy) itself — neither for an output stall nor for a compaction give-up — and the WAL admission SHALL NOT resync a seq gap itself. The one remedy-shaped action outside the primitives is the node drain of a graceful shutdown: an administration verb the loop is told about (`{:node_draining, deadline}`) and never chooses, reached through `HubManager.drain_hub/2` from `lib/flezha/shutdown/` only.

#### Scenario: stall is observed in one place, decision happens in another
- **GIVEN** the State Watchdog detects output stall for relay `btc_feed` tasklet 0
- **WHEN** the Watchdog flags the stall
- **THEN** the Watchdog emits a `[:flezha, :state, :output_stall]` event with `%{relay_class, relay_instance, tasklet, key}`
- **AND** the Watchdog performs no heal action of its own — no direct `Recovery.resync`, no dedup bookkeeping
- **AND** the Convergence Loop holds the declared stall as a fact, and on its next tick the Diff classifies the divergence and the Planner emits the output-stall ladder op (`resync_runner` first, escalating to `rebind_relay`) under per-target backoff.

#### Scenario: a compaction give-up is declared, not resynced
- **GIVEN** the Watchdog's strike table reaches its give-up count for a Cell
- **WHEN** the sweep gives up
- **THEN** the Watchdog SHALL declare `{:compaction_gave_up, cell_id, runner_id}` and call nothing, and the loop SHALL plan `resync_runner` for that Cell on the `{:compaction_gave_up, cell_id}` key

#### Scenario: a seq gap is one latched fact per episode
- **GIVEN** the WAL admission observes a seq gap for a Cell, then more gaps before the next contiguous append
- **WHEN** the gaps arrive
- **THEN** the first SHALL declare `{:seq_gap, cell_id, runner_id}` and the rest SHALL refresh the same key, the loop SHALL plan `resync_runner` for the Cell on its table, and the next contiguous append SHALL declare `{:seq_gap_closed, cell_id}`, which resets the key

#### Scenario: a runner disconnect is a declared fact planned per relay
- **GIVEN** a runner with thirty bound relays closes its session while another runner advertising their factory is in the fleet
- **WHEN** the fleet manager observes the close
- **THEN** it SHALL declare `{:runner_disconnected, ns, runner_id}` and reroute nothing; the loop SHALL tick at once, plan `rebind_relay` for at most twenty of them keyed on the reason, and re-tick without waiting for the rest

#### Scenario: a stale registry tag is re-asserted by the planner
- **GIVEN** a registry write lands after a rebind naming the old runner
- **WHEN** the registry hook observes the row
- **THEN** it SHALL declare `{:registry_written, ns, id}` and re-write nothing; the Diff SHALL report the stale tag and the plan SHALL carry `publish_binding` last

### Requirement: The reconciler is a pure-function pipeline, not scattered handlers

The self-healing decision logic MUST be expressed as a pure-function pipeline under `lib/flezha/convergence/`: `Loop.declare/1` (the one intake — a fact `{kind, target, args}` held in one map keyed `{kind, target}` with the time of its last declaration, refreshed by a re-declaration, cleared by its counter-fact or by its grace expiry, ticking the loop at once when the key is new) → `Probe.snapshot/0` (observe reality) → `Intent` (declared state) → `Diff.compute/3` (divergence delta, over probed reality AND this tick's facts) → `Planner` (ordered, backoff-filtered ops) → `Heal.execute/1` (steps primitives). The two telemetry subscriptions and the runner-connected handler SHALL be declarations too, so a site tells the loop in one shape. Adding a failure mode means one `declare/1` kind consumed by one Diff field, or one primitive entry with the Diff rule that plans it — NOT a new self-healing module, a site-local ladder, or decision branches scattered across services. The heal surface is a fixed, small set of idempotent primitives, each an entry naming its key rule, its executor and the evidence that resets it. The Planner SHALL be the only place a divergence becomes an op — no caller may inject ops into or around a produced plan. That rule governs remedies: everything that acts on a relay, a runner, a plugin or a Cell. The loop carries one duty outside it — on each tick it reclaims this node's hub for any namespace whose local fleet is empty and none of whose relays this node hosts. That reclaim names no target, takes no backoff and repairs nothing: it retires a node-local resource the fleet no longer justifies, so it is neither a primitive nor planned.

#### Scenario: a new signal enters through the Diff, not around the plan
- **WHEN** a sensor produces a fact the Loop must heal from (the data-plane classifier verdict, the Watchdog's stalled instances)
- **THEN** it is passed into `Diff.compute/3` as a signal alongside the Probe snapshot, surfaces as a `%Diff{}` field, and becomes an op inside `Planner.plan/3`
- **AND** no module appends, filters, or rewrites ops after the Planner has produced them

#### Scenario: a re-declaration costs no tick
- **GIVEN** a fact key the loop already holds
- **WHEN** the same site declares it again
- **THEN** the loop SHALL refresh the key's time and SHALL NOT tick for it; a key it does not hold SHALL tick at once

#### Scenario: a fact is cleared by its counter-fact or its expiry
- **WHEN** a relay held as parked is observed bound, a plugin held as down is observed `:running` past the stable window, a disconnected runner is observed active or no relay still names it, or a fact outlives its grace
- **THEN** the loop SHALL drop the fact and the next tick SHALL plan nothing from it

### Requirement: A hub stuck in `:recovery_timeout` does not permanently block healing for its namespace

The convergence loop MUST include hubs in `:recovery_timeout` state in the ready set returned by `RecoveryGate.ready_namespaces/1`. Heal tasklets for relays in a timed-out hub's namespace MUST proceed using the live cluster state (the authoritative source), not be gated indefinitely.

A `Logger.warning` MUST be emitted for each `:recovery_timeout` namespace included in the ready set so users are alerted to the stuck hub.

#### Scenario: timed-out hub does not block healthy relays

- **GIVEN** a controller hosting two namespaces: `ns_ok` (hub `:normal`) and `ns_timeout` (hub `:recovery_timeout`)
- **WHEN** a convergence tick runs
- **THEN** the Loop heals misbound relays in both `ns_ok` and `ns_timeout`
- **AND** a `Logger.warning` is emitted naming `ns_timeout` as timed-out

#### Scenario: normal tasklet is unchanged for healthy hubs

- **GIVEN** all hubs are in `:normal` or `:skipped`
- **WHEN** a convergence tick runs
- **THEN** no warning is emitted and healing proceeds as before

### Requirement: Every heal primitive is a safe no-op on a healthy target

Each heal primitive MUST re-read the authoritative live state at execution time and do nothing if the target is no longer broken. A detection slip (a reconnect or recovery between the snapshot and the heal) MUST NOT disrupt a working relay, runner, or producer.

The guard protects a target that is merely *observed* healthy. It SHALL NOT protect a target whose op carries a reason that PROVES it is not producing (`:output_stall`, `:data_plane_wedge`): neither `Heal.skip_rebind?/4`'s active-runner check nor `FleetManager.evict_runner/2`'s "never evict a live runner" rule applies to one. A pool-wedged runner is still connected, and being connected is exactly what stops anything else from dislodging it.

#### Scenario: a reconnected runner is not evicted
- **GIVEN** the Diff snapshot marked runner `default_X` for eviction
- **AND** `default_X` re-established its transport connection before the heal ran
- **WHEN** `Heal.execute(:evict_runner)` runs for `default_X`
- **THEN** FleetManager observes `default_X` is currently `:active` and performs no eviction
- **AND** the relays bound to `default_X` keep producing.

#### Scenario: a classifier-proved wedged runner IS evicted while connected
- **GIVEN** runner `default_X` is in the live active set, heartbeating, and the classifier reported `runner_pool_wedged` for it
- **WHEN** `Heal.execute(:evict_runner)` runs with `args.reason == :data_plane_wedge`
- **THEN** FleetManager evicts it despite the open connection, and its relays rebind onto a live runner

#### Scenario: an output-stalled relay is always re-wired
- **GIVEN** relay `btc_feed` is bound to an active runner but has not produced output past the stall grace
- **WHEN** the Planner emits the output-stall ladder op (`resync_runner`, then `rebind_relay` on persistence) with `reason: :output_stall`
- **THEN** the Heal applies it even though its bound runner is active (the silent-freeze self-heal MUST NOT be skipped by the healthy-target guard).

### Requirement: A failed heal SHALL NOT erase the divergence it failed to fix

`Overwatch.handle_call({:runner_migrate, new_runner_id})` SHALL commit the new binding only after `attempt_ping_and_start/2` reports the runner answered; on failure it SHALL return the state untouched. Runner ownership is node-local — `ControlUnit` and the `StreamChannel` registry live in the per-node `Identificator` registry and `FleetManager` is a per-node GenServer — so a heal handing a local runner to an Overwatch on another node fails by construction, and that failure MUST leave the relay honestly misbound.

Committing the binding first records `bound_runner ∈ active_runners` for a runner the relay cannot reach: `Convergence.Probe` reads it as placed, `Convergence.Diff` stops classifying it misbound, and the delta goes permanently empty. The relay then reports healthy while every frame drops.

#### Scenario: a failed cross-node migrate stays visible to the next tick
- **GIVEN** relay `btc_feed` is misbound and the Heal targets a runner on another controller's node
- **WHEN** `runner_migrate` cannot reach that runner (`{:ping_failed, :no_control_unit}`)
- **THEN** the relay's `runner_id` and `channel` SHALL be unchanged
- **AND** the next Diff SHALL still classify it misbound and re-plan the heal under backoff.

### Requirement: Bounded calls and backoff prevent heal-induced churn

Every cross-process call inside the Loop MUST be wrapped in `Flezha.SDK.SafeCall.bounded/3` (timeout + `:exit` catch + telemetry). Repeated heals of the same target MUST be suppressed by per-target exponential backoff so a persistent fault is not re-healed every tick; that backoff is the `:heal` row of the ladder table, whose rungs SHALL be `2 s → 5 s → 10 s → 15 s → 20 s → 30 s → 45 s → 60 s → 90 s → 120 s → 180 s → 300 s`, held at 300 s, reset on `:ok`. The early rungs are dense because the common fault is a runner restart measured in seconds, and a coarse first step punishes it as though it were permanent.

The Watchdog's compaction handshake SHALL ride a `:compaction` row of its own with rungs `5 s → 30 s → 60 s → 300 s` and its existing give-up bound of four attempts. It MUST NOT share the heal row's rungs: it gives up by attempt count, so a denser ladder would shorten the wall-clock it waits before abandoning a compaction, which is a change to durability behaviour and not to healing.

The wrapper is control-plane-wide, not a convergence concept: it is named by `convergence`, `schedule`, `runner` and `service`, so it sits with the pure modules the controller imports from the Elixir SDK. Its telemetry event stays `[:flezha, :convergence, :safe_call]`, which is a published surface and MUST NOT be renamed with the module.

#### Scenario: a flapping target is backed off
- **GIVEN** relay `r1` was just rebound by the Loop
- **WHEN** the same divergence is observed on the next tick
- **THEN** the Planner filters the repeat `rebind_relay` op under backoff and does not re-issue it until the backoff window elapses.

#### Scenario: the loop's rungs are the table's
- **WHEN** a heal of one target fails on twelve consecutive eligible ticks
- **THEN** the twelve waits emitted on `[:flezha, :backoff, :wait]` for ladder `:heal` SHALL be `2_000`, `5_000`, `10_000`, `15_000`, `20_000`, `30_000`, `45_000`, `60_000`, `90_000`, `120_000`, `180_000`, `300_000`, and a thirteenth failure SHALL wait `300_000` again

#### Scenario: compaction keeps its own wall-clock before giving up
- **WHEN** a compaction of one Cell fails on four consecutive eligible attempts
- **THEN** the waits emitted for ladder `:compaction` SHALL be `5_000`, `30_000`, `60_000`, `300_000` and the Watchdog SHALL give up only after the fourth
- **AND** a change to the `:heal` rungs SHALL NOT move any of those four values

### Requirement: Producing-liveness derives from durable progress markers with a fed precondition

The convergence snapshot SHALL classify each bound relay's producing-liveness from durable progress markers compared between ticks — per-Cell WAL persisted seq always, and, when the relay runs the effectively-once tier, per-Cell durable consumed-offset advance and outbox `flushed_seq` drain. Channel state and heartbeats SHALL NOT be producing-liveness inputs.

The tier cursors SHALL be read per Cell. A Cell's durable consumed offset is the offset the controller has fsynced for it, and the minimum across a relay's Cells is that relay's watermark — so a single frozen Cell freezes the relay's verdict while the verdict still names the Cell responsible.

Every flat-marker verdict SHALL apply the fed precondition over the grace window: input-side evidence advancing while the progress marker is flat past the grace period classifies the relay STUCK (not producing); no input-side advance with a flat marker classifies it IDLE (producing-healthy). For tier-on relays, an input-log tail growing while a Cell's durable consumed offset is flat is a STUCK verdict, and output-bearing WAL records accumulating while `outbox_flushed_seq` is flat is a STUCK verdict. When one root cause freezes multiple markers, the snapshot SHALL report the single deepest verdict for that relay.

Cursor reads SHALL NOT create or resurrect durable state for a relay that has none: a relay without a live input log SHALL yield no cursor fields, and its classification SHALL degrade to WAL-seq-only rather than fail or default.

All marker reads SHALL be off-path at the convergence tick cadence, reusing counters and cursors that already advance; no per-message instrumentation is added.

#### Scenario: Idle relay stays healthy

- **WHEN** a relay receives no input and all its progress markers are flat across ticks
- **THEN** it classifies IDLE and counts as producing; `/healthz` remains `200 healthy`

#### Scenario: Fed but frozen relay is STUCK

- **WHEN** input-side evidence for a relay advances across ticks while its WAL persisted seq stays flat past the grace period
- **THEN** the snapshot classifies it STUCK, `producing_relays` excludes it, and `/healthz` reports `200 degraded`

#### Scenario: Frozen consumed offset with growing input-log is STUCK (tier on)

- **WHEN** a tier-on relay's input-log tail grows across ticks while one of its Cells' durable consumed offset does not advance past the grace period
- **THEN** the relay classifies STUCK with a watermark-frozen detail naming that Cell

#### Scenario: A checkpoint-ack relay that stops consuming is STUCK

- **WHEN** a checkpoint-ack relay's input-log tail grows across ticks while its Cells' durable consumed offsets stay flat past the grace period
- **THEN** the relay classifies STUCK, even though its WAL persisted seq is legitimately flat between checkpoints and it is exempt from the input-silence sweep

#### Scenario: Undrained outbox with accumulating output records is STUCK (tier on)

- **WHEN** output-bearing WAL records accumulate for a Cell across ticks while its `outbox_flushed_seq` stays flat past the grace period
- **THEN** the relay classifies STUCK with an outbox-flat detail naming that Cell

#### Scenario: A tier-off relay is classified on the WAL seq alone

- **WHEN** a relay that has not declared the effectively-once tier is classified
- **THEN** no cursor fields are present on its markers, no input log is created for it, and its verdict is derived from WAL fed/advance evidence only

### Requirement: An empty intended set with durable evidence of intent is never vacuously healthy

On each tick the convergence snapshot SHALL compare the live intended set against durable evidence of prior intent (per-Relay state directories under the state root and durable-registry entries). Any relay with durable evidence but absent from the live intended set SHALL be listed as `intent_mismatch`. When the mismatch list is non-empty — including the case where the live intended set is entirely empty — `/healthz` SHALL report `200 degraded` with `reason: intent_mismatch` naming the relays, and SHALL NOT report `200 healthy`. The condition SHALL NOT produce `503 unhealthy` (a kubelet restart cannot repair missing intent). Deleting the relay's durable state (`flz state delete`, `flz reset`) clears the mismatch. A state directory that carries a finished-job `Completion` record is retained for inspection on purpose and SHALL NOT count as evidence of lost intent.

#### Scenario: Registry wipe degrades health instead of reporting healthy

- **WHEN** the durable registry is wiped so the live intended set is empty, while per-Relay state directories from prior tasklet exist on disk
- **THEN** `/healthz` reports `200 degraded` with `reason: intent_mismatch` listing the evidenced relays, not `200 healthy`

#### Scenario: Deliberate decommission clears the mismatch

- **WHEN** a user stops a relay and deletes its durable state
- **THEN** the relay no longer appears in `intent_mismatch` and health returns to `200 healthy`

#### Scenario: A retained finished steps relay is not a mismatch

- **WHEN** a steps relay completes and its state directory is retained with its `Completion` record while it is no longer intended
- **THEN** it SHALL NOT appear in `intent_mismatch` and health SHALL NOT degrade for it

#### Scenario: Fresh install with no evidence is healthy

- **WHEN** a controller boots with an empty intended set and no durable state directories or registry entries
- **THEN** `/healthz` reports `200 healthy`

### Requirement: /healthz never lies

The `/healthz` HTTP endpoint MUST derive `bound_relays`, `producing_relays`, `unknown_relays`, recovery state, the intent-mismatch list, the current data-plane fabric verdict, and sensor freshness from a single `Convergence.Snapshot.current/0` reader, and the per-relay and per-plugin words it folds MUST be the verdict records that snapshot carries. It MUST NOT compute these from independent paths, and it MUST NOT judge: the fold from records to one endpoint word is one ordered table owned by the verdict record module, and the endpoint renders its result. `producing_relays` MUST be the count of relay records whose state is `healthy` — an IDLE relay counts as producing, a STUCK relay does not — and `bound_relays` the count of records that are neither `unbound`, `failed` nor `unknown` and not suspended, so a dead binding is never counted as bound. It returns `200 healthy` (every relay record `healthy`, `suspended` or `unknown`; the body carries `unknown_relays`), `200 degraded` (any relay record `degraded` or `parked`, any relay `unbound` or `failed` past the unmet-intent grace, any plugin record `degraded`, non-empty intent mismatch, `sensor_stale`, or an RPO breach — the body's reason joins the records' reasons, e.g. `dead_runner+parked+plugin_unhealthy`), `200 recovering`/`restoring_registry` (planned recovery, a gapped tick, a relay `unbound` or `failed` inside the unmet-intent grace, or a pending restore — kubelet MUST NOT restart), or `503 unhealthy` ONLY on an authoritative dead-loop (`tick_stale`), `recovery_failed`, or a persistent controller-fabric data-plane wedge (a `driver` record `degraded` with a wedged layer — `driver_loop_frozen` or `nif_queue_backing_up` — sustained for `FLEZHA_FABRIC_503_POLLS` consecutive classifier polls — default 3, `0` disables the fabric 503 — reason `data_plane_fabric_wedged` naming the layer). BEAM-remediable verdicts MUST NOT produce `503` — they are healed by the convergence pipeline and surface as `degraded`. An `unknown` record MUST NOT degrade the endpoint: an uncertain probe never escalates. An empty intended set MUST NOT short-circuit to `200 healthy` while durable evidence of intent exists.

The fabric arm of that guarantee needs a NIF driver to judge, and **the scope MUST be stated wherever the guarantee is**. Both server drivers, UDS and QUIC, produce the self-report the fabric verdicts are computed from, so a controller on either transport is covered and its `/healthz` is evidence about the data plane. The endpoint's own documentation and the classifier's driver clauses MUST state which transports carry a driver.

#### Scenario: healthz reflects reality even when bound and producing diverge

- **GIVEN** 10 relays are bound and 3 classify STUCK (fed but progress markers flat past `output_stall_grace_ms`)
- **WHEN** `/healthz` is queried
- **THEN** the body reports `bound_relays=10`, `producing_relays=7`, and the status is `200 degraded` (not 200 healthy, not 503).

#### Scenario: healthz does not 503 on a slow-but-live loop

- **GIVEN** the Loop completes ticks slowly under CPU pressure but is not dead
- **WHEN** `/healthz` is queried
- **THEN** the status is `200` (healthy/degraded); loop slowness alone reaches `503` only once the loop has not ticked within the authoritative tick-age bound.

#### Scenario: a recovery timeout surfaces as 200 recovering, not 503

- **GIVEN** a hub has recovery state `:timeout` (DETS replay window expired but live cluster ring is healthy)
- **WHEN** `/healthz` is queried
- **THEN** the response is `200 recovering` with `recovery_state: "recovery_timeout"` in the body
- **AND** the response is NOT `503 unhealthy`
- **AND** kubelet does not restart the pod

#### Scenario: idle fleet is healthy, wiped fleet is not

- **GIVEN** two controllers: one with 5 intended relays all IDLE (no input), one with an empty intended set but durable state directories for 5 relays
- **WHEN** `/healthz` is queried on each
- **THEN** the idle controller reports `200 healthy` with `producing_relays=5`, and the wiped controller reports `200 degraded` with `reason: intent_mismatch`

#### Scenario: a frozen NIF driver reaches 503

- **GIVEN** the classifier reports `driver_loop_frozen` (heartbeat flat while frames are queued) for `FLEZHA_FABRIC_503_POLLS` consecutive polls
- **WHEN** `/healthz` is queried
- **THEN** the status is `503 unhealthy` with `reason: data_plane_fabric_wedged` and `layer: driver_loop_frozen` in the body
- **AND** the orchestrator restarts the controller — the only remedy that reaches a wedged NIF thread

#### Scenario: a transient fabric verdict does not flap the pod

- **GIVEN** `driver_loop_frozen` is reported for a single poll and clears on the next
- **WHEN** `/healthz` is queried during and after
- **THEN** the status never reaches 503, and a busy-but-healthy controller under sustained load reports 200 throughout

#### Scenario: a runner-side wedge stays 200

- **GIVEN** the classifier reports a persistent `runner_not_consuming` verdict
- **WHEN** `/healthz` is queried
- **THEN** the status is `200 degraded` (the convergence pipeline owns the remedy) and never `503`

#### Scenario: the fabric arm covers a QUIC controller

- **GIVEN** a controller whose transport is QUIC, whose driver produces the self-report and whose loop heartbeat has stopped advancing past the freeze threshold
- **WHEN** the data-plane classifier runs and `/healthz` is queried
- **THEN** driver measurements are emitted for it, `driver_loop_frozen` is minted naming the QUIC transport, and the endpoint moves toward the fabric `503` on the same sustained-poll rule a UDS controller follows
- **AND** the `/healthz` endpoint documentation and the classifier's driver clauses state which transports carry a driver

#### Scenario: a parked relay is degraded at once

- **GIVEN** a relay the Controller has parked, whatever the park reason
- **WHEN** `/healthz` is queried
- **THEN** the status is `200 degraded` with `parked` among the joined reasons, and never `200 healthy`

#### Scenario: a relay whose runner is absent is recovering, then degraded

- **GIVEN** a bound relay whose runner process has died and left the roster
- **WHEN** `/healthz` is queried inside the unmet-intent grace and again after it
- **THEN** the first response is `200 recovering`, the second is `200 degraded` with `dead_runner` among the joined reasons, and neither counts the dead binding in `bound_relays`

#### Scenario: an unobserved relay is counted, not judged

- **GIVEN** a relay whose fact reader timed out on the last tick
- **WHEN** `/healthz` is queried
- **THEN** the status is `200 healthy` when nothing else degrades it, the body's `unknown_relays` is 1, and no `503` and no `degraded` follows from the uncertainty alone

### Requirement: Output-stalled relays SHALL be actively rebound, not only reported as degraded

An output stall SHALL reach the loop as a declared fact: the Watchdog telemetry handler declares `{:output_stall, relay_id}`, the loop holds it as any fact (a re-declaration refreshes it, a new key ticks at once), the Diff carries it, and `Planner.plan/3` plans the ladder — `resync_runner` on the first eligible attempt (cheap connection-level resync, no migration), escalating to `rebind_relay` on subsequent eligible attempts while the stall persists — with `args: %{reason: :output_stall}` on the `{:output_stall, relay_id}` key of the one backoff table, so a persistently stalled relay follows the `:heal` row's cooldown and never a per-tick storm. Escalation is counted in attempts, not in elapsed time, so the denser early rungs reach `rebind_relay` sooner in wall-clock terms while the order of remedies is unchanged. No op SHALL be injected after the plan is produced. The stall's timestamp stays readable for healthz reporting.

`Heal.skip_rebind?/4` SHALL NOT skip a rebind when `args.reason == :output_stall`, even if the relay's currently-bound runner is in the live active set. The output-stall condition proves the binding is not producing; the active-runner guard exists only to avoid disrupting a healthy relay, which an output-stalled relay is not.

#### Scenario: output-stalled relay walks the resync-then-rebind ladder

- **GIVEN** relay `btc_feed` is bound to active runner `ex1` and has not produced output past the output-stall grace period
- **WHEN** the Watchdog flags it and the convergence Loop receives the stall event
- **THEN** the Loop triggers an immediate tick
- **AND** the first eligible attempt issues a `resync_runner` op with `args: %{reason: :output_stall}`
- **AND** if the stall persists past the next backoff window, the following attempt issues `rebind_relay`, `skip_rebind?` returns `false` even though `ex1` is still active, and the relay is migrated and begins producing again

#### Scenario: repeated stalls are backoff-throttled

- **GIVEN** relay `btc_feed` was just resynced due to an output stall 1 second ago
- **WHEN** the Watchdog flags another output stall for `btc_feed`
- **THEN** the planned op is filtered out by Backoff (the first rung's cooldown has not yet elapsed)
- **AND** no op is issued on this tick

### Requirement: Rolling upgrade preserves producing-relays count

A user-initiated rolling restart of the controller or runner workload MUST complete with no data loss, measured by kafka offset advance per declared relay. The runner presents its persisted `previous_runner_id` on reconnect so the controller evicts the stale binding and rebinds atomically.

#### Scenario: rolling restart of the runner workload
- **GIVEN** the cluster is producing `producing_relays = N`
- **WHEN** the runner pods are rolled one at a time (graceful SIGTERM, peer stays up)
- **THEN** the new runner presents its `previous_runner_id`, the controller evicts the old binding and rebinds within a tick, and kafka offsets advance with no data loss.

### Requirement: Host sleep / wake recovers automatically

The system MUST detect a wall-clock vs monotonic-clock divergence indicating sleep/hibernate and, on wake, kick reconnects across SDK control streams and producers without user action.

#### Scenario: host sleep then wake
- **GIVEN** the system is producing `producing_relays = N`
- **WHEN** the host sleeps for ≥ 30 s and wakes
- **THEN** the Loop observes the wake, reconnects affected streams/producers, and kafka offsets per relay resume advancing without a user command.

### Requirement: Plugins are durable across controller restart

When `flz plugin enable <relay> <plugin>` succeeds, the plugin MUST be persisted into the hub's **declared-children set**, on every node of the hub cluster, regardless of which node served the CLI call. The declared set is named deliberately: the hub keeps two records of the same child, and an auto-recovery hub never replays its process registry from disk, so a plugin held only in the registry row is not read back by the restart it was written to survive. The registry row MUST continue to carry the plugin as the live view, written from the declared spec by every start, so the two records cannot disagree about a running relay. On restart (recovery mode, plain restart, or node-death redistribution from any node), every restored relay MUST re-instantiate its declared plugins idempotently, and the convergence I2 backstop MUST re-add a declared-but-missing plugin to a running relay. The same cluster-wide propagation MUST apply to plugin disable, to provider state persisted against a plugin, and to the registry runner-tag write issued by Heal rebinds. A peer node that is unreachable at write time MUST converge on rejoin via the peer-sync recovery path; propagation failures MUST be logged, never silently dropped.

#### Scenario: plugin survives recovery-mode restart
- **GIVEN** `flz plugin enable btc_feed kafka-producer` was executed
- **WHEN** the controller is restarted in recovery mode
- **THEN** `btc_feed` shows kafka-producer and the producer functions within a few ticks of boot.

#### Scenario: a restart-driven start SHALL NOT erase an enabled plugin
- **GIVEN** a relay declared by `flz relay start` with no plugins, and a plugin enabled on it afterwards
- **WHEN** the controller restarts and the reconcile round starts the relay from its declared entry
- **THEN** that declared entry carries the enabled plugin, so the started relay's plugin table carries it
- **AND** the registry row the start writes carries it too, so a later divergence is healable rather than invisible

#### Scenario: a disabled plugin is not resurrected by a restart
- **GIVEN** a plugin that was enabled and then disabled on a running relay
- **WHEN** the controller restarts
- **THEN** the declared entry no longer names it and the restored relay does not subscribe it

#### Scenario: the relay reports the plugins it actually has
- **GIVEN** a relay restored after a restart whose enabled plugins failed to re-instantiate
- **WHEN** a user runs `flz relay info`
- **THEN** the relay SHALL NOT report `Health: healthy` with an empty plugin table while plugins are declared for it

#### Scenario: a dropped plugin is re-instantiated on a running relay
- **GIVEN** a declared plugin is force-unsubscribed at runtime while its relay stays up
- **WHEN** the Loop ticks
- **THEN** the Diff detects the missing plugin and the Heal re-subscribes it from the cspec the relay was started with.

#### Scenario: plugin enabled via one node is declared on every node
- **GIVEN** a two-node hub cluster with a relay registered on both nodes' registries
- **WHEN** `flz plugin enable` is served by node B
- **THEN** the declared entry the leader wrote is visible to node A, and a relay restart driven from node A re-instantiates the plugin.

#### Scenario: redistribution after node death restores a producing relay
- **GIVEN** a relay whose plugins were enabled through any node of the cluster
- **WHEN** the node hosting the relay's Overwatch dies and the hub redistributes the relay to a surviving node
- **THEN** the restarted relay re-attaches its full declared plugin set without user action.

### Requirement: Unhealthy declared plugins are detected and restarted by the loop

The convergence loop SHALL judge production liveness of a relay's plugins by the plugin provider's own `health/1` self-report (`Extension.Table.health/1`), never by traffic-volume proxies. A plugin that is declared, currently subscribed, and self-reports `{:unhealthy, reason}` SHALL produce a `restart_plugin` heal op routed through the extension table's existing restart machinery, subject to the loop's per-target exponential backoff. An uncertain health probe (timeout, raise, missing provider callback) SHALL be treated as healthy so the loop never escalates on uncertainty.

A subscriber that crashes is the same divergence declared from the other side: the extension table records its phase as `{:down, reason}` and declares `{:plugin_down, namespace, relay_id, plugin_id}`; the table restarts nothing and holds no counter or timer. The loop plans `restart_plugin` keyed `{restart_plugin, {relay_id, plugin_id}}` — per plugin, so one crashing plugin never throttles its siblings' starts — and the key resets only on `:running` observed for the stable window, read from the table's `started_at` by the Probe, so a fast-failing provider accumulates backoff instead of resetting on every restart. The denser early rungs of the `:heal` row mean such a provider is retried more often in its first minute; the stable-window reset is what still carries it to the top rung rather than resetting on every restart.

The health self-report is the authority on whether a RUNNING plugin is producing;
it is not the authority on whether a plugin is running at all. A declared plugin
that is not in the `:running` phase SHALL be healed as missing rather than judged
by its health verdict, per "A declared plugin that is not running is a divergence".

An uncertain probe treated as healthy SHALL be emitted as telemetry by the
extension table, so that fail-open is observable rather than silent.

#### Scenario: wedged kafka producer is restarted after broker recovery
- **GIVEN** a relay with a declared kafka producer plugin and an unreachable broker long enough for the provider to self-report `{:unhealthy, :produce_stalled}`
- **WHEN** the Loop ticks
- **THEN** a `restart_plugin` op is executed for that plugin under backoff, and once the broker is reachable a restart attempt succeeds and the verdict clears to healthy.

#### Scenario: healthy plugins are never restarted
- **GIVEN** a relay whose declared plugins all self-report `:healthy`
- **WHEN** the Loop ticks
- **THEN** no `restart_plugin` op is planned for that relay.

#### Scenario: uncertain probe does not escalate
- **GIVEN** a plugin whose `health/1` raises or times out
- **WHEN** the Loop ticks
- **THEN** the plugin is treated as healthy and no heal op is issued.
- **AND** telemetry SHALL record that the verdict was a fail-open, naming the plugin and the reason.

#### Scenario: a crashed subscriber is restarted by the loop, not the table
- **GIVEN** a declared plugin whose subscriber process is killed
- **WHEN** the table handles the `DOWN`
- **THEN** it SHALL record `{:down, reason}`, declare `{:plugin_down, …}` and arm no timer; the loop's declaring tick SHALL plan `restart_plugin` for that plugin and no other

#### Scenario: a fast-failing provider accumulates backoff
- **GIVEN** a plugin that reaches `:running` and crashes again within the stable window on every restart
- **WHEN** the loop plans across ticks
- **THEN** its key SHALL walk the `:heal` row's rungs to the top and SHALL NOT reset until `:running` is observed for the whole window

### Requirement: Plugin-degraded relays surface on /healthz and telemetry

A declared plugin SHALL carry its own verdict record: `degraded` when it is not subscribed at all, when its owning process crashed, or when its provider self-reports unhealthy. A relay with at least one declared plugin that is not RUNNING — whichever of those it is, and including a plugin subscribed but left `prepared` by a failed activation — SHALL carry that divergence as its own `degraded` reason naming the plugin, so the relay surface and the plugin surface agree without either re-reading the other, and the snapshot's plugin-degraded set SHALL be derived from those records rather than kept as a second list. While any plugin record is `degraded`, `/healthz` SHALL report `degraded` with HTTP 200 and a reason that names `plugin_unhealthy` — degraded MUST NOT trigger a kubelet restart. The loop SHALL log plugin-health verdict transitions (a record's state changes, the degraded set changes membership, or it clears), not steady-state. A suspended steps instance — alive, unbound, its plugins released with its runner — SHALL NOT be plugin-diffed: the rehydrating bind replays its declared plugins, and until then there is nothing to heal.

#### Scenario: pluginless running relay is degraded, not healthy
- **GIVEN** a running relay whose declared cspec lists plugins that are not subscribed
- **WHEN** `/healthz` is queried before the heal completes
- **THEN** the response is HTTP 200 with status `degraded` and a reason naming `plugin_unhealthy`.

#### Scenario: a suspended instance is not degraded
- **GIVEN** a parked steps instance whose cspec declares plugins and whose extension table runs none
- **WHEN** the Loop ticks
- **THEN** no `missing_plugins` entry, no heal op and no degraded verdict SHALL be produced for it

#### Scenario: verdict clears after heal
- **GIVEN** a plugin-degraded relay
- **WHEN** the loop re-attaches or successfully restarts the affected plugins
- **THEN** every plugin record for it returns to `healthy` and `/healthz` returns to `healthy` with a single transition log line.

### Requirement: Fresh-process runner takeover is atomic

When an SDK runner process restarts and presents its stable identity with a new incarnation while the identity's previous session is still held, the controller MUST fence and evict the previous session and bind each of the identity's relays onto the new session through the one bind path, ordered unbind-old before bind-new, with no window where a relay is bound to both sessions or to a corpse, and with no `AddRelay` sent to a process for a relay it still runs.

#### Scenario: SDK process restart preserves relay binding
- **GIVEN** relay `btc_feed` is bound to session `default_X` of stable identity `S` and producing
- **WHEN** the SDK process is killed and restarts, presenting `S` with a new incarnation
- **THEN** the controller fences and evicts `default_X` inside the connect handler, each affected Overwatch binds its relay onto the new session through `Lifecycle.complete_init/4`, `btc_feed` is re-attached with its persisted state, and kafka offsets resume within a couple of ticks
- **AND** no entry in the active runner set references a dead `default_X`.

### Requirement: Data-plane stall verdicts feed the convergence pipeline

The classifier verdict the Loop already computes each tick SHALL be consumed by the Diff → Planner → Heal seams — passed in-process alongside the Probe snapshot, with NO new telemetry subscription or side channel — and mapped by remedy: runner-scoped verdicts (`runner_not_consuming`, `runner_pool_wedged`) to `restart_runner` first and `evict_runner` only on a later rung (the process the verdict named is ended on its own session — the one moment it can be identified — and the existing disconnect reroute moves its relays; see *A runner the classifier proves wedged is terminally restarted, not only evicted*); connection-scoped verdicts (`socket_write_blocked`, `channel_not_sending`) to `resync_runner`; relay-scoped verdicts (`relay_not_forwarding`, `cell_dispatch_stalled`) to `rebind_relay` for the relay named by the verdict metadata. Ops SHALL ride the standard per-target backoff, and the active-runner rebind guard SHALL NOT protect a target the classifier proved wedged. The classifier returns one deepest-wins verdict per tick; the pipeline SHALL heal that layer and let the next tick surface any remaining layer. No new self-healing module SHALL be introduced.

`cell_dispatch_stalled` SHALL be minted from the runner's self-reported handout liveness — a non-zero outstanding count whose oldest-age exceeds the configured threshold — under the same "is it actually being fed?" precondition every other check carries, and SHALL rank *shallower* than every runner-scoped verdict, since a wedged runner explains a wedged cell and not the reverse. Its detail SHALL carry the reported age so a false positive is diagnosable from the log alone.

`runner_pool_wedged` SHALL be minted only across a heartbeat that landed after a frame was sent, and only while the runner's self-reported completed dispatches, issued handouts and block counter are all flat between the two heartbeats. A handler still running when the heartbeat lands, or a retry waiting out its backoff, completes nothing but has moved the handout count, so it SHALL NOT read as a wedge. A runner that reports no handout count SHALL be judged on its dispatches alone.

**The precondition is universal, and the two driver-global checks SHALL carry it in their own form.** A verdict is a statement that something which was working has stopped, so a layer SHALL NOT be judged from a baseline that never showed it working:

* `driver_loop_frozen` SHALL be minted only when the *previous* sample shows an established driver — a non-zero loop-iteration count and a stamped last-loop time — and the current sample's iteration count has not gone backwards. A driver that has never completed a loop iteration SHALL NOT be reported frozen, and a counter that decreased SHALL be read as a re-created driver at the same transport key rather than as a frozen one. In consequence a reported `driver_loop_frozen` can never carry an infinite loop age.
* `nif_queue_backing_up` SHALL be minted only when the driver has demonstrably written before — a non-zero written count in the previous sample — so that "nothing was written since" means the driver stopped draining rather than that there was nothing to drain. This is the same exemption `runner_not_consuming` already applies to a stream that has never been read.

A close the controller itself initiated SHALL NOT mint an `output_stall` for the relays mapped to the closing stream, so a planned teardown SHALL NOT enter the loop as a divergence to heal. An unexpected close SHALL continue to emit it unchanged.

#### Scenario: A wedged runner pool is restarted from a classifier verdict
- **WHEN** the classifier reports `runner_pool_wedged` (heartbeats flowing, dispatch and handout counters frozen) for a runner that remains in the active set
- **THEN** the same tick's plan issues a `restart_runner` op with the verdict as reason, the runner exits non-zero and its relays are rerouted by the existing disconnect path and planner ordering, and repeats are backoff-throttled on the standard ladder

#### Scenario: A handler running across a heartbeat is not a wedge
- **GIVEN** a fed runner that took a frame up after one heartbeat and still runs its handler when the next lands
- **WHEN** the two heartbeats report the same dispatch count and a handout count that moved
- **THEN** the classifier SHALL NOT report `runner_pool_wedged` for that runner

#### Scenario: A retry waiting out its backoff is not a wedge
- **GIVEN** a fed runner whose frame failed and waits out its retry backoff, with no activation outstanding
- **WHEN** the two heartbeats report the same dispatch count and a handout count that moved
- **THEN** the classifier SHALL NOT report `runner_pool_wedged` for that runner

#### Scenario: A wedged channel is resynced, a wedged relay is rebound
- **WHEN** the classifier reports `channel_not_sending` for a fed channel, and on a later tick `relay_not_forwarding` for a fed relay
- **THEN** the plan issues `resync_runner` for the first and `rebind_relay` for the second, each under per-target backoff, and `skip_rebind?` does not skip the rebind despite the bound runner being active

#### Scenario: One verdict per tick, deepest first
- **WHEN** both a channel and the runner beneath it are wedged
- **THEN** the classifier reports only the deeper runner-scoped verdict, that layer is healed first, and any surviving channel wedge surfaces on a later tick

#### Scenario: A single stalled cell is rebound while the runner keeps working
- **GIVEN** a runner whose dispatch counter is still advancing and whose relays are being fed
- **WHEN** its heartbeat reports an outstanding activation older than the threshold for one relay
- **THEN** the classifier SHALL report `cell_dispatch_stalled` scoped to that relay, the plan SHALL issue `rebind_relay` for it, and no `evict_runner` SHALL be issued for the runner

#### Scenario: A wedged runner outranks a stalled cell
- **WHEN** a runner reports frozen dispatch and handout counters and an aged outstanding activation
- **THEN** the classifier SHALL report only `runner_pool_wedged`, and the cell-scoped verdict SHALL surface on a later tick only if it survives the runner-level remedy

#### Scenario: A driver that has not started yet is not frozen
- **GIVEN** a data-stream driver registered moments ago, whose previous sample carries a zero loop-iteration count and no stamped last-loop time
- **WHEN** the classifier runs against that pair of samples
- **THEN** the verdict SHALL be `:ok`, no `driver_loop_frozen` SHALL be logged or emitted, and `/healthz` SHALL NOT move toward a fabric `503`

#### Scenario: A re-created driver at the same transport key is not frozen
- **GIVEN** a runner that reconnects, registering a fresh driver under the same transport key, whose iteration count restarts below the previous sample's
- **WHEN** the classifier runs against that pair of samples
- **THEN** the decreased counter SHALL be read as a new driver and the verdict SHALL be `:ok`

#### Scenario: A bring-up queue burst is not a backed-up queue
- **GIVEN** a driver whose previous sample shows no frames written at all, and whose command-queue depth has risen past the high-water mark during fleet bring-up
- **WHEN** the classifier runs against that pair of samples
- **THEN** no `nif_queue_backing_up` SHALL be minted, because the driver has never been observed writing

#### Scenario: A genuinely frozen driver is still caught
- **GIVEN** a driver established across earlier samples, with frames written and a stamped last-loop time
- **WHEN** its iteration count stops advancing and its last loop ages past the freeze threshold
- **THEN** `driver_loop_frozen` SHALL be minted exactly as before, with a finite loop age in its detail

#### Scenario: A planned teardown is not a divergence
- **GIVEN** a runner the controller has marked retiring because it initiated the disconnect
- **WHEN** the runner's data streams close and the close reaches the stream channel
- **THEN** no `output_stall` SHALL be emitted for the relays mapped to that stream, and the convergence loop SHALL plan no rebind on account of the close

### Requirement: Sensor freshness is visible

Periodic sensors SHALL stamp their last completed pass into the convergence Snapshot — initially the Watchdog's sweep timestamp (in-tick computations are already covered by the tick-age bound) — and `/healthz` SHALL report a sensor whose stamp is older than 3× its period as `200 degraded` with `reason: sensor_stale` naming the sensor. Sensor watching SHALL be implemented as Snapshot fields read by healthz, not as a new monitoring process.

#### Scenario: A wedged Watchdog is visible within one poll
- **WHEN** the Watchdog stops completing sweeps without crashing
- **THEN** `/healthz` reports `200 degraded` with `reason: sensor_stale` and `sensor: watchdog` once the stamp exceeds 3× the sweep period

#### Scenario: A crashed-and-restarted sensor recovers silently
- **WHEN** the Watchdog crashes, is restarted by its supervisor, and completes a sweep
- **THEN** the stamp is fresh again and healthz drops the `sensor_stale` reason with no user action

### Requirement: Relay starts have a single idempotent authority

Every path that starts or binds a relay — the ProcessHub orphan reconcile, the Convergence planner, manager/CLI starts, scheduled spawns, and tasklet-requested spawns — SHALL resolve through the registry's duplicate-id claim. On a collision the loser SHALL observe the duplicate verdict, verify the winner is live, and no-op; it SHALL NOT stop, restart, or rebind the winner's instance. The `reconcile_duplicate` telemetry event (upstream: one relay observed on multiple nodes, resolved keep-one/stop-others) SHALL be reclassified alarm-grade carrying the kept/stopped nodes: a firing outside a cross-node partition-recovery window indicates a start-authority violation.

#### Scenario: Concurrent reconcile and planner starts collapse to one instance
- **WHEN** the orphan reconcile and the Convergence planner both attempt to start the same relay in the same window on one node
- **THEN** exactly one instance runs, the losing path no-ops after verifying the winner is live, and no stop/restart churn is observed on the winner

#### Scenario: Duplicate telemetry distinguishes its two cases
- **WHEN** `reconcile_duplicate` fires
- **THEN** it is emitted alarm-grade on the observability bus with kept/stopped node metadata, so a partition-recovery resolution and a start-authority violation are distinguishable at a glance

### Requirement: Convergence cooperates with reconcile rounds instead of racing them

`Flezha.Convergence.Loop` SHALL consume `reconcile_round` completion events (the hook fires at round completion, carrying `started`/`duplicates`/`elapsed_ms`): a round reporting `started > 0` SHALL trigger an immediate convergence tick whose plan adopts the reconcile-started relays — no rebind, stop, or duplicate-start op is emitted against a relay the round just started and that probes as live-bound. A round reporting `started = 0` SHALL trigger nothing. The boot/recovery window remains gated by the existing RecoveryGate hold, unchanged.

#### Scenario: A reconcile-started relay is adopted, not fought
- **WHEN** a steady-state reconcile round starts two orphaned relays and the completion event reaches the Loop
- **THEN** the Loop ticks immediately, the Diff observes both relays bound, and the plan contains no op targeting them

#### Scenario: Quiet rounds are free
- **WHEN** a reconcile round completes with `started = 0`
- **THEN** no extra tick is triggered and the Loop's cadence is unchanged

### Requirement: Rebind is node-placement-aware with cross-node re-placement

The heal/rebind path SHALL only bind a relay to a runner reachable from
the node hosting that relay's Overwatch, and every rebind path SHALL pick
its target through the same factory-filtered placement function as the start
path.

The convergence loop SHALL NOT move a relay to another node. Which
controller hosts a relay is decided by the namespace hub's distribution
strategy alone (see `relay-placement`), so a relay with a factory is
never assigned to a node whose fleet cannot run it, and there SHALL be no
`replace_relay` primitive, no `replace_relays` Diff field and no call to
`ProcessHub.Service.Migration.migrate_child/3` in `lib/`.

A relay whose node has no eligible runner while a peer's fleet has one is
in that state only during the group withdrawal grace or while the hub's own
move is in flight. The Diff SHALL classify it as waiting: no op is planned
against it and no backoff key advances.

#### Scenario: No local runner plans nothing

- **WHEN** a relay's Overwatch is on a node whose local fleet has no
  runner advertising its factory, while another node's fleet does
- **THEN** the plan contains no op targeting the relay
- **AND** the relay reaches the other node through the hub's
  redistribution once this node withdraws the factory's group

#### Scenario: No eligible fleet anywhere degrades loudly, without churn

- **WHEN** no node in the cluster has a runner advertising the relay's
  factory
- **THEN** the relay stays where it is, visibly degraded under backoff, and
  no move is attempted

### Requirement: Registry rebinds commit only on a confirmed migrate

Every rebind path SHALL commit the registry binding only after the relay
confirms the bind — the heal rebind and the runner-disconnect reroute
alike. The Overwatch's `runner_migrate` SHALL take the one bind path for every
state it is in: it starts the bind task an un-park starts, linked to the Overwatch, which runs
`Lifecycle.complete_init/4` against the target and reports its outcome; the task and every
process it starts SHALL stop when the Overwatch stops, killed or not, so no bind outlives the
Overwatch that owns it; the
Overwatch publishes the registry tag and the route when the task reports a bound
state, and parks the relay with the attempt's reason when it reports a
failure, so the failure is declared to the loop and rebound on the `{:parked, id}`
ladder. There SHALL be no synchronous running-migrate path beside the task, no
snapshot-seeded `AddRelay`, and no restore dispatched by the heal after the
bind: the bind restores the persisted Cells and delivers the inbox tail itself.
A failed, timed-out, or unconfirmable bind SHALL leave the durable registry
binding unchanged, and the relay SHALL remain eligible for the next
convergence tick.

#### Scenario: Timed-out migrate does not switch the registry

- **WHEN** a `runner_migrate` call times out, or the bind task it spawned reports a failure
- **THEN** the durable registry binding is not switched to the new runner, the relay is parked with the reason and declared to the loop, and the parked ladder rebinds it

#### Scenario: A steps instance migrates as a steps relay

- **WHEN** a `runner_migrate` re-adds a durable steps relay on its new runner
- **THEN** the `AddRelay` SHALL carry the relay's kind, its folded `task_journal`, a `resume_cell_count` equal to its persisted Cells and its undelivered `inbox_tail_count`, so the runner starts it as a steps relay, holds its driver until those Cells are restored, skips every committed tasklet and receives the tail it is owed

#### Scenario: A rebind performs one restore

- **WHEN** the heal's `rebind_relay` succeeds for a relay with persisted Cells
- **THEN** each Cell SHALL receive exactly one `RestoreInitState` for that rebind, sent by the bind, and the heal SHALL send none of its own

#### Scenario: Reroute picks by placement, not at random

- **WHEN** a runner disconnects and its relays are rerouted to the
  remaining fleet
- **THEN** each replacement target is chosen by the factory-filtered placement
  function, and no relay is bound to a runner that does not advertise its
  factory

#### Scenario: A killed Overwatch leaves no bind running

- **WHEN** an Overwatch is killed while its bind task is restoring Cells and re-forwarding the tail
- **THEN** the task and its restore processes SHALL stop with it, and the restarted Overwatch's bind SHALL be the only one that completes

### Requirement: A runner the classifier proves wedged is terminally restarted, not only evicted

`restart_runner` SHALL be the FIRST rung of the runner-scoped wedge ladder on the existing
per-target backoff, and `evict_runner` the rung above it. The terminal disconnect is ordered
on the session the verdict named, because eviction ends that session: the SDK reconnects the
same process under a fresh session token, a ladder keyed on the token never reaches a later
rung for it, and the returned process is a placement candidate again. Eviction SHALL be
planned only when a restart has already been applied to that session and the classifier
still proves it wedged — a process that did not exit — and a core that has acknowledged a
terminal disconnect SHALL NOT reconnect, so an evicted one cannot return. Both rungs SHALL be
gated on the same `proved_wedged?/1` evidence that waives the healthy-target guards. A runner
that is merely slow, idle, or unfed SHALL never reach either rung.

The primitive SHALL send a terminal disconnect reason over the control channel.
On receiving it the SDK SHALL run its graceful shutdown and then exit the
process with a **non-zero** status, so the orchestrator that owns process
lifecycle restarts it; a zero exit would read as a clean stop and the runner
would not come back. The controller SHALL NOT attempt to restart the process
itself.

Every issued restart SHALL be logged and emitted as telemetry naming the
namespace, runner, the verdict that proved the wedge, and the ladder rung, so a
runner restarting repeatedly is visible rather than mysterious.

#### Scenario: Restart first, eviction only if the process survives it
- **GIVEN** a runner the classifier reports `runner_pool_wedged` for
- **WHEN** the first tick plans a remedy
- **THEN** it SHALL plan `restart_runner`; and only if a later tick still proves the same session wedged — the process did not exit — SHALL it plan `evict_runner`

#### Scenario: The process the verdict named cannot come back
- **GIVEN** a runner that acknowledged a terminal disconnect
- **WHEN** its session closes, or it is evicted on the later rung
- **THEN** it SHALL open no new control connection, so no placement can choose it; the orchestrator's fresh process presents the same stable identity and is bound as any reconnect is

#### Scenario: The restarted runner exits non-zero
- **WHEN** a runner receives the terminal disconnect reason
- **THEN** it SHALL run its graceful shutdown and exit with a non-zero status, and its relays SHALL be rerouted by the existing disconnect path

#### Scenario: A healthy runner is never restarted
- **WHEN** a runner is idle, or is connected but unfed, or is slow without a wedge verdict
- **THEN** no `restart_runner` op SHALL be planned for it

#### Scenario: Repeats are throttled and visible
- **WHEN** a restarted runner's fresh process wedges again after the orchestrator brings it back
- **THEN** the next restart SHALL be delayed by the standard per-target backoff, and each issued restart SHALL be logged and emitted with the verdict and rung

### Requirement: A park is an outcome declared to the loop, never a primitive

A park SHALL be declared to the convergence loop by the one park funnel (`Flezha.Relay.Lifecycle.park/2`) as the fact `{:relay_parked, namespace, relay_id, reason}`, and SHALL NOT be a heal primitive: nothing is repaired by a park, it is what a failed repair leaves behind. The Diff SHALL report parked relays apart from unbound ones, with the reason, and the Planner SHALL answer with `rebind_relay` on the `{:parked, relay_id}` key — eligible at the declaring tick, then on the loop's table — planning `reset_control_channel` for the runner first when the reason is a dead-transport ping failure, and only once the parked key has walked two rungs when the reason is a timeout. A bind SHALL be one attempt; the un-park task reports its outcome to the Overwatch and decides nothing.

A runner connecting in a namespace SHALL release the cooldown keys of every relay in that namespace which is waiting for a runner, whatever phase it is in: parked, unbound, and misbound to a runner that is gone. The released set SHALL be **derived** from the same `Planner.key/1` that produced the op, never from a hand-written list of key shapes, so a rebind planned under a new reason cannot silently escape the release.

The release SHALL be guarded so that a runner which cannot serve a relay does not wake it: a runner SHALL release only the keys of relays whose factory it advertises. Because that advertisement can change between the release and the plan, the guard SHALL NOT be the only protection — a rebind attempt that finds no runner able to serve the relay SHALL be treated as an unmet precondition and SHALL neither advance nor reset the ladder, so a wake that turns out to be useless costs a plan and not a rung. A failed bind that did have a capable runner is a failed repair and advances the ladder as before. No separate release counter, per-runner release cooldown or other site-local ladder SHALL be introduced: the `:on_success` reset of the one table is what bounds a flapping runner.

#### Scenario: A failed bind parks once and is retried by the loop
- **GIVEN** a relay whose one bind attempt fails with a timeout-class reason
- **WHEN** the Overwatch parks it
- **THEN** `{:relay_parked, ns, id, reason}` SHALL reach the loop, the loop SHALL tick at once and plan `rebind_relay` for it under the `{:parked, id}` key, and no retry SHALL run inside the Overwatch

#### Scenario: A dead transport is cut before the relay is placed on it
- **GIVEN** a relay parked because its bind's ping reported a dead transport
- **WHEN** the loop plans the declaring tick
- **THEN** it SHALL plan `reset_control_channel` for the runner before `rebind_relay` for the relay

#### Scenario: A busy runner is retried, then cut
- **GIVEN** a relay parked with a timeout reason whose `{:parked, id}` key has walked two rungs
- **WHEN** the fault persists into the next eligible tick
- **THEN** the plan SHALL carry `reset_control_channel` for its runner; a first timeout SHALL NOT cut the channel

#### Scenario: A runner arriving mid-cooldown is used at once
- **GIVEN** three relays parked in a namespace, their keys on a mid-ladder rung
- **WHEN** a runner advertising their factory connects to that namespace
- **THEN** every one of their cooldown keys SHALL be released and the immediate tick SHALL plan their rebinds

#### Scenario: A relay whose runner died is released too
- **GIVEN** a relay judged misbound to a runner that is gone, cooling on the `{:dead_runner, id}` key its reasoned rebind derives
- **WHEN** a runner advertising its factory connects to the namespace
- **THEN** that key SHALL be released and the relay SHALL bind on the immediate tick rather than waiting out its rung

#### Scenario: A runner that cannot serve the relay wakes nobody
- **GIVEN** a relay waiting for a runner in a namespace
- **WHEN** a runner connects to that namespace advertising no factory the relay needs
- **THEN** the relay's cooldown key SHALL NOT be released
- **AND** if it is woken anyway, the resulting attempt SHALL neither advance nor reset its ladder

#### Scenario: A flapping runner does not defeat the ladder
- **GIVEN** a relay waiting on a mid-ladder rung whose bind fails for its own reasons
- **WHEN** a runner connects and disconnects repeatedly in its namespace
- **THEN** each failed repair SHALL advance the ladder, and the rung SHALL be cleared only by a bind that succeeds

### Requirement: A bypass of the remedy seam SHALL be impossible, and every primitive SHALL pass one conformance test

One table-driven test SHALL walk `Heal.primitives/0` and the declared fact kinds and
assert: every primitive has a key rule, an executor and an evidence rule and is a no-op
on a healthy target; every primitive advances its key on `:error` and resets on its
declared evidence, a ladder op resetting on evidence only; every declared kind is
consumed by exactly one Diff field and planned by exactly one Planner rule; and a source
walk over `lib/**/*.ex` finds no call to a remedy entry point — `Recovery.resync/2`,
`Recovery.resync_for_overwatch/2`, any `Heal` function, `FleetManager.evict_runner/2`,
`FleetManager.reset_control_channel/1`, `Overwatch.plugin_restart/3`,
`Extension.Table.attempt_restart/3`,
`Lifecycle.publish_binding/1`, a terminal `ControlUnit.disconnect/2` — outside
`lib/flezha/convergence/`, nor to `HubManager.drain_hub/2` outside
`lib/flezha/shutdown/`. One end-to-end drill in the system tier SHALL walk the same
table and, per primitive, inject its fault through the real transport and assert the
loop plans and executes that primitive, advances its key while the fault persists and
resets it on repair. The one exception is a rebind whose fault leaves no runner able to
serve the relay: that attempt is an unmet precondition and neither advances nor
resets the ladder, so for it the drill SHALL assert instead that the rebind is planned
again with no waiting key of the relay armed, and that no key is left armed once
the relay binds to a runner that arrives. A new primitive or fact kind SHALL be
covered by both with no test edit.

#### Scenario: A direct resync outside the loop fails the walk
- **WHEN** a call to `Recovery.resync/2` is added to a module outside `lib/flezha/convergence/`
- **THEN** the seam test SHALL fail naming the file, the line and the called function

#### Scenario: A primitive nobody plans fails the table
- **WHEN** a primitive is added to the primitive table with no Diff rule that plans it
- **THEN** the seam test SHALL fail naming the primitive

#### Scenario: A ladder op does not reset on its own success
- **GIVEN** a primitive whose repair is only visible later, with its fact standing
- **WHEN** a stub executor answers `:ok`
- **THEN** its key SHALL stay armed until the declared evidence arrives

#### Scenario: The drill plans each primitive from its own fault
- **WHEN** the system-tier drill injects, for every primitive, the fault the Diff plans it for
- **THEN** `[:flezha, :convergence, :heal]` SHALL fire naming that primitive and target, the tick summary SHALL report the advanced rung while the fault is held, the unmet rebind below excepted, and the tick after repair SHALL plan nothing for the target

#### Scenario: A rebind with no runner to place on arms no rung
- **GIVEN** the drill's rebind row, whose runner is killed with no other runner able to serve the relay
- **WHEN** the loop plans the rebind
- **THEN** it SHALL plan it again, with no waiting key of the relay armed
- **AND** once a runner arrives and the relay binds to it, no waiting key SHALL be left armed and the tick after SHALL plan nothing for the relay

### Requirement: One verdict record per subject SHALL be judged once and read everywhere

The convergence loop SHALL mint, on every tick, exactly one verdict record per health
subject — a runner, a relay, a plugin, a channel or a driver — carrying the subject,
one state of a closed vocabulary (`healthy`, `degraded`, `failed`, `unbound`, `parked`,
`suspended`, `unknown`), the reason, the subject's own lifecycle phase as a fact never
derived from the state, the monotonic instant the state was first minted, the named
threshold it was judged by, and the raw facts it was judged from. Every state SHALL
name one owner of the next step and no two states SHALL share an owner: `healthy`
nobody, `degraded` the loop in place, `failed` the loop by rebinding, `unbound` the loop
by binding, `parked` a user or a plugin fix, `suspended` nobody until the committed
boundary, `unknown` the next tick. The states a subject kind can reach SHALL be declared
and tested: a plugin and a driver reach `healthy`, `degraded` and `unknown`; a channel
and a runner add `failed`; a relay reaches all seven.

Every detector SHALL be one entry of a declaration table naming its fact reader, the
subject kind it speaks about, the threshold names it cites and the `[:flezha, …]` events
it executes; a detector SHALL read facts and SHALL NOT decide. One judge SHALL fold the
facts per subject in one fixed rule order — an unobserved reader wins, then suspension,
then no binding, then a binding to a runner the roster does not hold, then a park, then
the deepest of a stall, a stuck marker or a degraded plugin, otherwise healthy — and the
three fail-open rules SHALL be the judge's: a `:prepared` plugin is `healthy`, a probe
that answered uncertainly is `healthy`, and a relay whose markers are flat with no
input-side advance is `healthy` with reason `idle`. A reader that raises or times out
SHALL yield `unknown` for its subjects, `unknown` SHALL escalate nothing, and the tick
SHALL complete.

The judged records SHALL be the Diff's input in place of an open signals map, SHALL be
published to the snapshot from exactly one site, and `bound_relays`,
`producing_relays` and `unknown_relays` SHALL be derived from them. A classifier
verdict SHALL reach exactly one reader by its subject kind: a `driver` record is read by
`/healthz` alone and the Diff SHALL plan nothing for it; every other kind is read by the
Diff alone and reaches `/healthz` only as `degraded`. The five facts — session open, data
plane draining, WAL advancing, process alive, probe answered — SHALL stay five readers
with five thresholds; the fold is of the vocabulary and the record, never of the facts,
so a session that is alive over a data plane that is dead is two fields of one record.

#### Scenario: A detector that raises yields unknown and the tick completes

- **GIVEN** a detector entry whose fact reader raises on this tick
- **WHEN** the judge runs
- **THEN** every subject that entry speaks about SHALL be `unknown` with a reason naming
  the failure, no heal SHALL be planned on its account, and the tick SHALL publish a
  snapshot

#### Scenario: An idle relay is healthy, not stalled

- **GIVEN** a bound relay whose progress markers are flat and whose input side has
  not advanced
- **WHEN** the judge runs
- **THEN** its record SHALL be `healthy` with reason `idle` and it SHALL count as
  producing

#### Scenario: A suspended instance with a park reason is suspended

- **GIVEN** a durable steps instance evicted at a committed boundary that also carries
  a park reason
- **WHEN** the judge runs
- **THEN** its record SHALL be `state: suspended, phase: suspended`, and a parked
  relay that is not suspended SHALL be `state: parked, phase: parked`

#### Scenario: A driver verdict plans nothing

- **GIVEN** the classifier reports a fabric-scoped layer
- **WHEN** the judge and the Diff run
- **THEN** a `driver` record SHALL be minted `degraded` with the wedged layer as its
  reason, the Diff SHALL issue no op for it, and no runner, channel or relay record
  SHALL reach the fabric arm of `/healthz`

#### Scenario: A live session over a dead data plane is one record with two facts

- **GIVEN** a runner whose keepalive pings keep arriving while its data-plane counters
  have stopped advancing past the stall window
- **WHEN** the judge runs
- **THEN** the runner's record SHALL carry a small control silence and a large data
  silence side by side, and its state SHALL be `degraded` with a wedged-layer reason

#### Scenario: A new detector is one table entry

- **WHEN** a detector entry is added to the table with a fact reader and its declared
  events
- **THEN** its subjects SHALL be judged, published and rendered on every surface, and its
  events SHALL be bridged, with no change to the Diff, the snapshot, `/healthz`, the CLI
  readers or the bridge

### Requirement: Every retry ladder is one declared row of the backoff table

Every wait-before-retry in the controller SHALL be one row of
`Flezha.Config.Backoff`'s ladder table — rungs or a geometric base and cap, a jitter
flag, a reset rule (`:on_success`, or `{:stable_for, duration}` where success means an
owned process stayed up) and a give-up bound — and every site SHALL obtain its delay
from that row through `Backoff.delay/3`, `Backoff.wait/3` or the `%Backoff{}` struct,
never from its own arithmetic, list or literal. Jitter SHALL be one rule, uniform in
`[d/2, 3d/2)` and then capped at the row's cap. Every wait and every reset SHALL be
emitted on `[:flezha, :backoff, :wait]` (`delay_ms`, `attempt`; `ladder`, `target`) and
`[:flezha, :backoff, :reset]`, so a ladder's behaviour is observable without reading its
site. A successful attempt SHALL reset the row's attempt count, under the row's own
definition of success. A row's values SHALL be stated once, by the requirement that owns
the behaviour they pace: the `:heal` and `:compaction` rows by "Steps calls and
backoff prevent heal-induced churn", and the rest here — the OTLP post's
`250 ms → 4 s` in five attempts, the cluster reconnect's `1 s` doubling to its
`:reconnect_max_backoff_ms` knob, the hub-call retry's `150 ms → 5 s` in eight attempts,
the schedule's `1 s → 60 s`, the shipper's `500 ms → 30 s`, the websocket socket
reconnect's `250 ms × 1.5 → 30 s`, the websocket output's ack retry `1 s` doubling to
`30 s` and given up after six unacknowledged sends, and the SDK's `50 ms → 5 s` and
`100 ms → 30 s`. A row
retuned later SHALL be retuned in the one requirement that states it, so no second copy
of a ladder's values can go stale.

#### Scenario: A site beside the table fails the seam test

- **WHEN** a module under `lib/flezha` outside `convergence/backoff.ex` gains a retry
  arithmetic — a `Process.sleep` over a doubling expression, a rung list, a `2^attempt`
  — or its own jitter
- **THEN** `test/flezha/config/backoff_seam_test.exs` SHALL fail naming the file
  and line

#### Scenario: Every row is observable through its real entry point

- **GIVEN** a ladder row and the failure that walks it induced through the real
  transport or process (a stopped listener, a failing tasklet, a 429 on a websocket
  upgrade, an unanswered call)
- **WHEN** the site retries
- **THEN** one `[:flezha, :backoff, :wait]` per attempt SHALL carry the row's declared
  delay for that attempt, attempts SHALL ascend, and the first success SHALL emit
  `[:flezha, :backoff, :reset]` after which the next wait is attempt 1 again

#### Scenario: A row that no site drives fails the drill by name

- **GIVEN** a ladder declared in the table
- **WHEN** `test/system/backoff_table_drill_test.exs` walks `Backoff.ladders/0`
- **THEN** a row that neither a device drives nor an entry accounts for — naming the
  suite that drives its site — SHALL fail the drill by name, so a new ladder is covered
  with no test edit

### Requirement: A watched process's death SHALL reach the loop on its own tick, never through a monitor the loop holds

The convergence Loop SHALL hold no process monitors. It SHALL learn of a death the way it learns of everything else — by polling on its tick — and no owner of a monitor SHALL call into it.

A monitor exists so its OWNER can maintain its own bookkeeping — drop a route, forget a subscriber, settle an in-flight request. The watch seam SHALL hand that owner the tags it keyed on the dead pid and SHALL decide nothing further: whether the death is a divergence worth repairing is the Loop's judgement, made on the tick it plans on, so no site gains a second, faster clock than the plan.

#### Scenario: An owner reacts at once and the Loop plans on its own tick

- **GIVEN** a route table that watches the process serving a relay
- **WHEN** that process dies
- **THEN** the route table SHALL drop its routes at once, and the Loop SHALL reach the same verdict on its next tick rather than being called into directly

#### Scenario: The Loop holds no monitors

- **WHEN** the convergence tree is read
- **THEN** no module under it SHALL call `Process.monitor/1` and no `:DOWN` clause SHALL exist there

### Requirement: A runner's report that a relay instance exited SHALL park it for the one rebind

The controller SHALL read `RelayExited { relay_id, reason }` on a runner's control stream as
a runner-initiated frame, admit it through the ownership record, flush the reporting runner's
stream channels off the control channel's process, and then tell the instance's Overwatch. The
Overwatch SHALL act on a report only when it is current: the instance is bound to the reporting
session, no bind task is in flight, it is not suspended, it is not already parked, and no
completion is recorded for it. A report that lands while a bind task is in flight SHALL be held
and judged by the same rules against the binding that task's result adopts, because the
incarnation the bind started can exit before the bind reports. Any other report SHALL change
nothing. A current report SHALL be
logged, SHALL emit `[:flezha, :relay, :exited]` naming the namespace, the relay, the runner
and the reason, and SHALL park the instance through the one park funnel with the reason
`{:relay_exited, reason}`.

That park SHALL be answered as every park is: `rebind_relay` on the `{:parked, relay_id}` key,
whose bind task sends `AddRelay` with the resume state, restores and awaits every persisted Cell
of an instance whose input is durable, and re-forwards the input log above the confirmed floor
before the instance takes new input. No other rebind path SHALL be added.

A park whose reason is `{:relay_exited, _}` SHALL count as repaired only once the probe sees the
instance unparked and bound for at least the stable window a restarted plugin must stay up for,
read from the same duration row, so that an instance which exits again inside that window is
rebound when its key's rung allows and not at once. No new ladder or knob SHALL be introduced.

#### Scenario: A killed instance is parked and rebound

- **GIVEN** a flow instance `X` bound to a live Elixir runner and a running convergence loop
- **WHEN** the runner reports `RelayExited` for `X`
- **THEN** `X` SHALL be parked with `{:relay_exited, reason}`, `[:flezha, :relay, :exited]`
  SHALL be emitted, the loop SHALL plan `rebind_relay` for `X` on the declaring tick, an
  `AddRelay` for `X` SHALL reach a runner advertising its factory, and `X` SHALL be observed
  bound and unparked afterwards

#### Scenario: A stale report changes nothing

- **GIVEN** an instance `X` bound to session B
- **WHEN** a `RelayExited` for `X` is admitted from session A, which shares B's stable identity
- **THEN** `X` SHALL NOT be parked and no rebind SHALL be planned for it

#### Scenario: A report while a bind is in flight is judged against the binding it adopts

- **GIVEN** an instance `X` whose bind task is running
- **WHEN** a `RelayExited` for `X` arrives, and the bind task then reports `X` bound
- **THEN** `X` SHALL NOT be parked while the task runs, the task's result SHALL be adopted, and `X`
  SHALL then be parked with `{:relay_exited, reason}` when the report came from the session the
  adopted binding names, and left bound otherwise

#### Scenario: A report for a suspended or completed instance changes nothing

- **WHEN** a `RelayExited` arrives for an instance that is suspended, or for a steps instance
  whose completion is recorded
- **THEN** the instance SHALL NOT be parked or rebound, and a completed one's teardown SHALL proceed

#### Scenario: A second report for a parked instance changes nothing

- **GIVEN** an instance `X` already parked by a report
- **WHEN** the runner sends the same report again
- **THEN** `X` SHALL stay parked with its first reason and one rebind SHALL be planned

#### Scenario: An instance that dies again inside the stable window waits its rung

- **GIVEN** an instance `X` rebound after a report, whose park fact is still held
- **WHEN** `X` is reported exited again before it has been bound for the stable window
- **THEN** the report SHALL cause no immediate tick, and the next `rebind_relay` for `X` SHALL be
  planned only once its `{:parked, X}` key's rung has elapsed

#### Scenario: An instance that stays up clears its ladder

- **GIVEN** an instance `X` rebound after a report
- **WHEN** the probe has seen `X` bound and unparked for the stable window
- **THEN** its park fact SHALL be dropped and its `{:parked, X}` key reset

### Requirement: A declared plugin that is not running is a divergence

`Convergence.Probe` SHALL report a relay's RUNNING plugins as the set whose
lifecycle phase is `:running`, not the set of subscribed plugin ids. A declared
plugin that is subscribed but stuck in `:prepared` — because its `activate/1`
failed — SHALL therefore appear in the Diff's `missing_plugins`, and SHALL be
healed by the existing `start_plugin` primitive under the existing per-target
backoff.

The `extra_plugins` check SHALL continue to read the full SUBSCRIBED set, so an
undeclared plugin is still torn down whatever its phase; the two checks read two
different sets deliberately, and the Probe SHALL report both.

Without this, a plugin that never acquired its upstream resources is counted as
running, is reported healthy because `:prepared` is healthy by rule, and yields
an empty delta on every tick — the relay produces nothing and no verdict,
op, or alarm is ever raised.

#### Scenario: a plugin stuck in prepared is diffed as missing
- **GIVEN** a relay with one declared plugin whose `activate/1` failed, leaving it `:prepared` and subscribed
- **WHEN** the Loop ticks
- **THEN** the Diff SHALL list that plugin in `missing_plugins`
- **AND** the Planner SHALL plan a `start_plugin` op for it under backoff

#### Scenario: a running plugin is not diffed as missing
- **GIVEN** a relay whose declared plugins are all `:running`
- **WHEN** the Loop ticks
- **THEN** `missing_plugins` SHALL be empty and no `start_plugin` op SHALL be planned

#### Scenario: an undeclared plugin is still extra whatever its phase
- **GIVEN** a relay holding a subscribed plugin that its declaration does not name, in either phase
- **WHEN** the Loop ticks
- **THEN** the Diff SHALL list it in `extra_plugins` and a `stop_plugin` op SHALL be planned

### Requirement: A heal that did not bring the plugin up SHALL report failure

The `start_plugin` and `restart_plugin` primitives SHALL report `:error` when the
plugin did not reach the `:running` phase, even when the underlying subscribe
succeeded. A heal that reports `:ok` for a plugin that acquired no upstream
resources advances the per-target backoff as though the divergence were repaired,
and — for `restart_plugin` — converts a plugin the loop HAD detected as unhealthy
into one it can no longer detect, because the plugin is now `:prepared` and
`:prepared` is healthy. The remedy must never be able to hide the fault.

The result SHALL be judged by the binding's phase after the call, read in the same
call: `:running` is `:ok`; `:prepared` or `{:down, _}` is `:error`; and
`{:not_started, reason}`, a binding that cannot start until something outside the
loop changes, such as its plugin not being enabled or its provider not being loaded
on this controller, is `:unmet`. An `:unmet` plugin heal SHALL neither advance nor
reset the target's backoff and SHALL NOT count toward the per-tick cap, so any
number of such bindings never makes the loop re-tick without waiting `tick_ms`.
A binding the probe sees recorded not started SHALL be planned after every other
op of the tick, so such bindings take only the plan slots no repair needs.

#### Scenario: a restart whose activation fails is recorded as a failed heal
- **GIVEN** a declared plugin self-reporting `{:unhealthy, :upstream_down}` whose upstream is still down
- **WHEN** the Loop plans and executes `restart_plugin`
- **THEN** the primitive SHALL report `:error`
- **AND** the next tick SHALL still see a divergence for that plugin and re-plan the heal under backoff

#### Scenario: upstream recovering lets the ladder succeed
- **GIVEN** a plugin whose repeated `restart_plugin` heals have failed while upstream was down
- **WHEN** upstream recovers and the next backoff window opens
- **THEN** the heal SHALL bring the plugin to `:running` and report `:ok`
- **AND** the following tick SHALL show no divergence for it

#### Scenario: a successful heal still reports success
- **GIVEN** a declared plugin that is missing and whose `activate/1` will succeed
- **WHEN** the Loop executes `start_plugin`
- **THEN** the primitive SHALL report `:ok` and the plugin SHALL be `:running`

#### Scenario: a binding whose plugin is disabled is an unmet heal
- **GIVEN** a running relay with a binding whose plugin was disabled, so the binding is `{:not_started, {:plugin_not_found, name}}`
- **WHEN** the Loop plans and executes `start_plugin` for it
- **THEN** the primitive SHALL report `:unmet`
- **AND** the `:heal` key for that target SHALL be neither advanced nor reset

#### Scenario: many disabled bindings do not make the loop spin
- **GIVEN** more disabled bindings across running relays than `Planner.max_ops()`
- **WHEN** the Loop ticks
- **THEN** no tick SHALL re-tick without waiting `tick_ms` on their account
- **AND** the Loop SHALL receive no more than one scheduled tick per `tick_ms` while nothing else is declared

#### Scenario: disabled bindings never crowd out a repair
- **GIVEN** more disabled bindings on a running relay than `Planner.max_ops()`, and a declared plugin of the same relay that is missing and an undeclared one that is subscribed
- **WHEN** the Loop plans a tick
- **THEN** the plan SHALL start the missing plugin and stop the undeclared one
- **AND** the disabled bindings' `start_plugin` ops SHALL fill only the slots left

#### Scenario: enabling the plugin again brings the binding up without the loop
- **GIVEN** a relay with a binding whose plugin is disabled
- **WHEN** a plugin of that name is enabled again
- **THEN** the binding SHALL reach `:running` through the enable's own reconvergence
- **AND** the next `start_plugin` the Loop would plan for it SHALL not be planned, because it is no longer missing

### Requirement: Every NIF server driver SHALL produce the driver self-report

Every transport server driver started through the NIF SHALL produce the driver self-report
`driver_stats/1` returns, in one shape, so that no reader above the NIF carries a per-transport
branch. The shape is `%{now_ms, loop_iterations, last_loop_ms, cmd_queue_depth, streams: [...]}`
with each stream reporting `{stream_id, write_buffer_len, write_buffer_dropped, frames_written,
frames_read, last_write_ms, last_read_ms}`.

A field SHALL carry a measurement of the driver's own behaviour or SHALL be absent. A value that
means "not measured" SHALL NOT be reported as zero, because the classifier reads zero as healthy
and would then treat an uninstrumented driver as a working one.

The observability source that emits these values and the stall classifier that judges them SHALL
require no transport-specific branch: they read one map, and a transport that has no driver thread
at all carries no `:driver` key rather than a differently-shaped one.

#### Scenario: Both server drivers answer the same map
- **WHEN** `driver_stats/1` is called on a running UDS server and on a running QUIC server
- **THEN** both answer a map with the same keys, neither answers `:undefined`, and each loop heartbeat advances while its driver thread is alive

#### Scenario: A driver's per-stream line reports what it actually did
- **WHEN** a stream on either server driver is offered frames and its peer takes some of them
- **THEN** every offered frame is counted either as written or as refused at the ceiling, a write the peer accepted stamps `last_write_ms`, and the reported `write_buffer_len` is the depth of that stream's own send buffer

### Requirement: A tick's liveness read SHALL NOT walk the state tree

The producing-liveness pass of every tick SHALL learn which Cells have a live writer from the
writer registry, not by listing directories under the state root: `Watchdog.cell_statuses/0`
SHALL answer the stall status of every Cell whose writer is registered, read from
`WriterRegistry`, and SHALL read no directory. The intent-mismatch pass SHALL check a class
with durable state and no live relay for its completion record in the class directory the
cached walk found, at the cost of one file check, without re-resolving the class's namespace.
A tick's file-server work SHALL therefore grow by no more than one file check per relay class
directory on disk with no live relay, including directories left behind by relays that no
longer run.

The state walk that the sweeps and the retention pass still make (`Paths.all_cells/0`,
`relay_classes/0`, `cells_of/1`) SHALL resolve each class's namespace once per walk, from the
namespace directory it found the class under, and SHALL NOT re-resolve it per class or per
tasklet. `Paths.state_root/0` SHALL return the absolute root without a file-server call per
use: a relative configured root SHALL be expanded once per configured value. The walk's
results SHALL be unchanged.

#### Scenario: leftover class directories do not slow the tick
- **GIVEN** a controller with 200 empty relay class directories under a namespace and no relay running
- **WHEN** the Loop ticks
- **THEN** `Watchdog.cell_statuses/0` SHALL read no directory
- **AND** the tick SHALL make at most one file-server call per such directory, its completion check, and no cwd call

#### Scenario: the liveness read covers every live writer
- **GIVEN** relays running with Cells whose writers are registered, one of them before its first file was written
- **WHEN** `Watchdog.cell_statuses/0` is read
- **THEN** it SHALL answer a stall status for each of those Cells and for no Cell without a live writer

#### Scenario: the walk keeps the namespace it found
- **GIVEN** classes under two namespaces with tasklets and Cells on disk
- **WHEN** `Paths.all_cells/0` runs
- **THEN** it SHALL answer the same Cell ids as before the change
- **AND** it SHALL list each namespace directory once and SHALL make no cwd call to the file server

## Removed Requirements

### Requirement: `Backoff.schedule_ms/0` is part of the public API

**Reason**: The function has zero callers in application code. It was added as a test-convenience export but is not referenced anywhere. Removing it trims the public API surface.

**Migration**: Any test or external code that calls `Backoff.schedule_ms/0` should inline the value `[5_000, 30_000, 60_000, 300_000]` or read the module attribute `@schedule_ms` if needed for compile-time assertions.
