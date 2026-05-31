# convergence-self-healing Specification

## Purpose
TBD - created by archiving change unified-convergence-loop. Update Purpose after archive.
## Requirements
### Requirement: Single convergence loop owns all self-healing decisions

The system MUST run exactly one self-healing decision loop (`Flezha.Convergence.Loop`). No module other than the Loop emits heal actions (rebind, restart-plugin, reconnect-stream, evict-executor, etc.). Watchdog, FleetManager, HubManager, Healthz, StreamChannel — each emits *events* or *facts*; none of them decide what to fix.

#### Scenario: stall is observed in one place, decision happens in another
- **GIVEN** the State Watchdog detects output stall for relay `btc_kraken` shard 0
- **WHEN** the Watchdog flags the stall
- **THEN** the Watchdog emits a `[:flezha, :state, :output_stall]` event with `%{relay_class, relay_instance, shard, key}`
- **AND** the Watchdog performs no heal action of its own
- **AND** the Convergence Loop records the stalled instance, and on its next tick the Diff classifies the divergence, the Planner emits a `rebind_relay` op (with `reason: :output_stall`), and the Heal executes it under per-target backoff.

### Requirement: The reconciler is a pure-function pipeline, not scattered handlers

The self-healing decision logic MUST be expressed as a pure-function pipeline under `lib/flezha/convergence/`: `Probe.snapshot/0` (observe reality) → `Intent` (declared state) → `Diff.compute/2` (divergence delta) → `Planner` (ordered, backoff-filtered ops) → `Heal.execute/1` (bounded primitives). Adding a failure mode means extending the Diff/Planner/Heal seams, NOT adding a new self-healing module or scattering decision branches across services. The heal surface is a fixed, small set of idempotent primitives.

#### Scenario: a new divergence reuses the existing seams
- **GIVEN** a new failure mode "wedged stream channel" must be healed
- **WHEN** it is added
- **THEN** the Probe surfaces the fact (per-pid strike counters), the Diff classifies it (`wedged_channels`), the Planner emits an existing primitive (`reconnect_stream`), and the Heal executes it
- **AND** no new top-level self-healing GenServer is introduced.

### Requirement: Every heal primitive is a safe no-op on a healthy target

Each heal primitive MUST re-read the authoritative live state at execution time and do nothing if the target is no longer broken. A detection slip (a reconnect or recovery between the snapshot and the heal) MUST NOT disrupt a working relay, executor, or producer.

#### Scenario: a reconnected executor is not evicted
- **GIVEN** the Diff snapshot marked executor `default_X` for eviction
- **AND** `default_X` re-established its transport connection before the heal ran
- **WHEN** `Heal.execute(:evict_executor)` runs for `default_X`
- **THEN** FleetManager observes `default_X` is currently `:active` and performs no eviction
- **AND** the relays bound to `default_X` keep producing.

#### Scenario: an output-stalled relay is always re-wired
- **GIVEN** relay `btc_kraken` is bound to an active executor but has not produced output past the stall grace
- **WHEN** the Planner emits a `rebind_relay` op with `reason: :output_stall`
- **THEN** the Heal migrates the relay to an available executor even though its bound executor is active (the silent-freeze self-heal MUST NOT be skipped by the healthy-target guard).

### Requirement: Bounded calls and backoff prevent heal-induced churn

Every cross-process call inside the Loop MUST be wrapped in `Convergence.SafeCall.bounded/3` (timeout + `:exit` catch + telemetry). Repeated heals of the same target MUST be suppressed by per-target exponential backoff so a persistent fault is not re-healed every tick.

#### Scenario: a flapping target is backed off
- **GIVEN** relay `r1` was just rebound by the Loop
- **WHEN** the same divergence is observed on the next tick
- **THEN** the Planner filters the repeat `rebind_relay` op under backoff and does not re-issue it until the backoff window elapses.

### Requirement: /healthz never lies

The `/healthz` HTTP endpoint MUST derive `bound_relays`, `producing_relays`, and recovery state from a single `Convergence.Snapshot.current/0` reader. It MUST NOT compute these from independent paths. It returns `200 healthy`, `200 degraded` (bound ≠ producing), `200 recovering`/`restoring_registry` (planned recovery — kubelet MUST NOT restart), or `503 unhealthy` ONLY on an authoritative dead-loop (`tick_stale`) or `recovery_failed`.

#### Scenario: healthz reflects reality even when bound and producing diverge
- **GIVEN** 10 relays are bound and 3 have not advanced output past `output_stall_grace_ms`
- **WHEN** `/healthz` is queried
- **THEN** the body reports `bound_relays=10`, `producing_relays=7`, and the status is `200 degraded` (not 200 healthy, not 503).

#### Scenario: healthz does not 503 on a slow-but-live loop
- **GIVEN** the Loop completes ticks slowly under CPU pressure but is not dead
- **WHEN** `/healthz` is queried
- **THEN** the status is `200` (healthy/degraded), and `503` fires only when the loop has not ticked within the authoritative tick-age bound.

#### Scenario: a recovery timeout surfaces as 503 recovery_failed
- **GIVEN** a hub's recovery is observed as `:timeout`
- **WHEN** `/healthz` is queried
- **THEN** the status is `503 unhealthy` with reason `recovery_failed`, and it returns to `200 healthy` once the hub reaches `:normal`.

### Requirement: Rolling upgrade preserves producing-relays count

An operator-initiated rolling restart of the controller or executor workload MUST complete with no data loss, measured by kafka offset advance per declared relay. The executor presents its persisted `previous_executor_id` on reconnect so the controller evicts the stale binding and rebinds atomically.

#### Scenario: rolling restart of the executor workload
- **GIVEN** the cluster is producing `producing_relays = N`
- **WHEN** the executor pods are rolled one at a time (graceful SIGTERM, peer stays up)
- **THEN** the new executor presents its `previous_executor_id`, the controller evicts the old binding and rebinds within a tick, and kafka offsets advance with no data loss.

### Requirement: Host sleep / wake recovers automatically

The system MUST detect a wall-clock vs monotonic-clock divergence indicating sleep/hibernate and, on wake, kick reconnects across SDK control streams and producers without operator action.

#### Scenario: host sleep then wake
- **GIVEN** the system is producing `producing_relays = N`
- **WHEN** the host sleeps for ≥ 30 s and wakes
- **THEN** the Loop observes the wake, reconnects affected streams/producers, and kafka offsets per relay resume advancing without an operator command.

### Requirement: Plugins are durable across controller restart

When `flz plugin enable <relay> <plugin>` succeeds, the plugin MUST be persisted into the relay's ProcessHub cspec start-args. On restart (recovery mode), every restored relay MUST re-instantiate its declared plugins idempotently, and the convergence I2 backstop MUST re-add a declared-but-missing plugin to a running relay.

#### Scenario: plugin survives recovery-mode restart
- **GIVEN** `flz plugin enable btc_kraken kafka-producer` was executed
- **WHEN** the controller is restarted in recovery mode
- **THEN** `btc_kraken` shows kafka-producer and the producer functions within a few ticks of boot.

#### Scenario: a dropped plugin is re-instantiated on a running relay
- **GIVEN** a declared plugin is force-unsubscribed at runtime while its relay stays up
- **WHEN** the Loop ticks
- **THEN** the Diff detects the missing plugin and the Heal re-subscribes it from the declared cspec.

### Requirement: Fresh-process executor takeover is atomic

When an SDK executor process restarts and presents `previous_executor_id`, the controller MUST evict the previous binding and bind the new id atomically, with no window where a relay is bound to both ids or to a corpse.

#### Scenario: SDK process restart preserves relay binding
- **GIVEN** relay `btc_kraken` is bound to executor `default_X` and producing
- **WHEN** the SDK process is killed and restarts, reading its persisted `executor_id = default_X` and announcing it as `previous_executor_id`
- **THEN** the controller evicts the stale `default_X` binding inside the connect handler before the new bind, re-attaches the relay, and kafka offsets resume within a couple of ticks
- **AND** no entry in the active executor set references a dead `default_X`.

