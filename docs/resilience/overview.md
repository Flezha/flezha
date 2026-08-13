# Fault Tolerance, Self-Healing & Recovery — Overview

This document maps every mechanism in Flezha that prevents data loss, detects
failures, and restores the system to a healthy state. It is the entry point;
each section links to the dedicated doc for its subsystem.

For the architectural model these mechanisms compose into — the durable-compute
kernel, the two profiles, and the diagrams — start at
[`../architecture/`](../architecture/README.md).

The system has three layers:

- **Prevention** — bound mailboxes, WAL durability, graceful drain
- **Detection** — watchdog sweeps, control-channel liveness, seq-gap detection,
  convergence probes
- **Recovery** — convergence loop rebind/plugin-restart/evict, Overwatch retry,
  SDK reconnect, ProcessHub cold-boot recovery, entity resync

---

## 1. Convergence Loop (primary self-healer)

`lib/flezha/convergence/`

The controller's single self-healing engine. Every 5 s (and immediately on
declared events) it runs a declarative pipeline:

```
Intent → Probe → Diff → Planner → Heal
```

| Module | Role |
|--------|------|
| `Convergence.Loop` | GenServer driving the tick; owns misbound-streak state and event subscriptions |
| `Convergence.Intent` | Reads the ProcessHub registry to know what operatives and plugins _should_ exist |
| `Convergence.Probe` | Reads live reality (active runners, operative states, hub recovery); pure observer |
| `Convergence.Diff` | Pure function comparing intent vs reality → delta (unbound/misbound operatives, missing/extra plugins) |
| `Convergence.Planner` | Turns the delta into an ordered, backoff-filtered list of procs (cap: 20/tick) |
| `Convergence.Heal` | Executes procs via four idempotent primitives: `rebind_operative`, `start_plugin`, `stop_plugin`, `evict_runner` |
| `Convergence.Backoff` | Per-`{primitive, target}` exponential cooldown: 5 s → 30 s → 60 s → 300 s |
| `Convergence.Snapshot` | ETS table holding the latest published health summary; read by `/healthz`, CLI, and telemetry |
| `Convergence.RecoveryGate` | Classifies which namespaces are ready for healing based on per-hub recovery state; `:timeout` hubs are included rather than blocking all healing |
| `Convergence.SafeCall` | Bounded `GenServer.call` that returns `{:error, reason}` instead of crashing when a process is wedged or gone |

**Stability guarantees built into the loop:**

- An operative must be misbound for **≥ 2 consecutive ticks** before a rebind is
  issued — a single blip cannot trigger a full-namespace migration.
- The entire tick body is wrapped in `try/rescue`; an uncaught error becomes a
  skipped tick, not a Loop crash.
- If `active_runners` returns `:unknown` (a read failure), the diff suppresses
  destructive procs for that namespace — it is not treated as "all runners gone."
- Tick events (`:output_stall`, `:runner_connected`) shorten detection latency
  by triggering an immediate tick; they do not bypass the pipeline.

See [`convergence_loop.md`](convergence_loop.md) for the full architecture and
configuration reference.

---

## 2. Per-Operative Overwatch

`lib/flezha/operative/overwatch.ex`

A per-operative GenServer that does the actual bind work: ping a runner,
negotiate operative start, install plugins, and ship persisted WAL state on
attach. On any failure it retries with jittered exponential backoff
(`200 ms → 500 ms → 1 s → 2 s → 5 s → 10 s`). The convergence loop's
`rebind_operative` primitive calls into Overwatch; Overwatch never calls back
into the loop.

---

## 3. State Persistence — WAL + Snapshots

`lib/flezha/state/`

Every operative state change is durable before the runner's output plugin
fires, so a hard restart restores state from disk rather than losing it.

| Module | Role |
|--------|------|
| `State.WalWriter` | Appends every state change to a per-entity WAL file |
| `State.FsyncBatcher` | Coalesces all WAL writers' fsync calls into one batch every 5 ms |
| `State.Snapshot` | Crash-safe atomic snapshots: write `.tmp` → fsync → rename |
| `State.Outbox` | Persists the last-sent seq per entity as a dedup marker |
| `State.Restore` | Assembles snapshot + tail-WAL bytes to send to a reconnecting runner |
| `State.Recovery` | Re-runs the `RestoreInitState` handshake when an entity is detected as desync'd |
| `State.Retention` | Sweeper pruning old snapshots and WAL records beyond the retention window |
| `State.TimeTravel` | Point-in-time query: replay WAL up to a target seq or timestamp |
| `State.Watchdog` | Detects an entity still receiving changes but whose seq stopped advancing |
| `State.PersistenceResolver` | Resolves per-operative persistence config from runner override → operative metadata → global default |

**Back-pressure:** when a WAL writer's pending bytes exceed the high-watermark
(default 16 MiB), it sends an `EntityMailboxStatus{status: "full"}` command to
the SDK. The SDK's overflow policy (block/drop/replace) activates until the
low-watermark is crossed.

See [`state_persistence.md`](state_persistence.md) for the WAL record format,
snapshot lifecycle, crash-recovery semantics, and all configuration knobs.

An operative may upgrade this to **effectively-once** delivery — a durable input
log, one atomic record per invocation, and an fsync-gated outbox flush. That tier
and the two mechanisms it unlocks (the dead-letter queue and late-input
correction) are covered in §14.

---

## 4. Output-Stall Detection

`lib/flezha/state/watchdog.ex`

Sweeps every 10 s. For each persistence-enabled entity it checks:

1. **Output staleness** — still receiving changes but seq has not advanced within
   `output_stall_timeout_ms` (default 60 s) → emits
   `[:flezha, :state, :output_stall]` and triggers `State.Recovery.resync/2`.
   A genuinely idle entity (no changes arriving) is never flagged.
2. **Compaction timeout** — a `CompactEntity` whose snapshot has not arrived
   within `compaction_timeout_ms` (default 30 s) → emits
   `[:flezha, :state, :compaction_timeout]` and triggers resync.

The convergence loop subscribes to `[:flezha, :state, :output_stall]` and fires
an immediate tick on receipt.

---

## 5. Seq-Gap Self-Heal

`lib/flezha/runner/stream_channel.ex`

When `StreamChannel` receives a change frame with
`seq > last_persisted_seq + 1`, it emits `[:flezha, :state, :seq_gap]` and
calls `State.Recovery.resync/2` so the SDK rewinds and resends from
`terminal_seq + 1`. This is silent and automatic — no operator action required.

---

## 6. Boot Recovery

`lib/flezha/boot/`

| Module | Role |
|--------|------|
| `Boot.StatePathMigrator` | One-shot, idempotent rename of legacy runner-keyed WAL paths to the flat layout; runs before any WalWriter opens |
| `Boot.RecoveryHooks` | Installs telemetry forwarders for ProcessHub's `pre_recovery_replay` / `post_recovery_replay` lifecycle events |
| `BootLoader` | Runs pre-boot hooks (sockets, certs) and gates hub creation on cluster readiness |

**ProcessHub recovery gate:** `HubManager` starts each namespace hub with a
durable registry backend and an orphan reconcile. Every node periodically
starts the difference between its durable registry and what the cluster is
observed to hold: on a fully-downed cluster that difference is the whole
operative list, so it restores itself with no operator step; joining a live
cluster the difference is empty, so it starts nothing. The durable copy is
never wiped to force either outcome. See
[`process_hub_recovery_model.md`](process_hub_recovery_model.md).

**Bounded-kind operatives (durable crash-resume).** A `:bounded` operative is
**durable by default** (opt-out per class) and crash-resumes on the shared WAL
substrate: on a runner/controller restart it restores its committed
per-Entity state via the bind-time restore path and **re-runs its procedure
chain** (Model A idempotent re-run — a step may repeat, so steps are idempotent
by author contract; the `run`-sugar form re-runs from the top). A restore-before-run
gate (`AddOperative.resume_entity_count`) holds the first procedure until the
persisted Entities are restored, so it never runs on empty state. Saga
compensation is likewise crash-safe — compensation-intent / -done markers ride
the restore, so a crash mid-rollback resumes the remaining hooks and skips the
already-done ones. A completed operative is deregistered, but its final state + a
completion record are **retained for a window** (default 3 days) for post-hoc
inspection via `flz operative info`. Execution is **at-least-once**: a
non-idempotent fire-and-forget job should opt out of durability so a crash
re-runs from a clean slate rather than a partial one. See
[`bounded_operative.md`](bounded_operative.md). (A near-instant bounded operative
completing during the start RPC's confirmation window is handled — a clean exit
there is reported as a successful start.)

---

## 7. Graceful Shutdown & Drain

`lib/flezha/shutdown/`

| Module | Role |
|--------|------|
| `Shutdown.Coordinator` | Three-phase shutdown: quiesce (block new operatives/connections) → drain (wait for operatives to migrate to peers) → finalise (flush WAL, close transports) |
| `Shutdown.SignalHandler` | Installs SIGTERM/SIGINT OS handlers in every BEAM run mode; third signal triggers `erlang.halt(143)` |

The drain phase polls peer registry dumps until every locally-hosted operative is
visible on at least one peer. Solo nodes skip the wait. On graceful stop the SDK
ships final operative state before exiting, so the WAL is consistent at the next
boot.

See [`graceful_shutdown.md`](graceful_shutdown.md).

---

## 8. Health Endpoint

`lib/flezha/healthz/server.ex`

HTTP `/healthz` listener (opt-in, `FLEZHA_HEALTHZ_ENABLED=true`). Reads only
`Convergence.Snapshot.current/0` — holds no state of its own.

| Status | HTTP | Condition |
|--------|------|-----------|
| `healthy` | 200 | All hubs `:normal` and `producing_operatives == bound_operatives` |
| `degraded` | 200 | One or more operatives output-stalled (kubelet must not restart — the loop is healing) |
| `recovering` | 200 | Any hub in `:recovery_pending \| :recovering` |
| `unhealthy` | 503 | Convergence loop itself is wedged (`tick_stale \| recovery_failed`) |

---

## 9. Cluster Formation & Reconnect

`lib/flezha/cluster/orchestrator.ex`

Keeps the BEAM cluster connected: discovers peers on startup, reconnects with
jittered backoff on disconnect, and gates namespace hub creation on
`cluster_ready?/0`. A single-node deployment degrades to solo once the bounded
formation timeout elapses, so it still boots without hanging.

---

## 10. Runner Connection Resilience

`lib/flezha/runner/`

| Module | Role |
|--------|------|
| `FleetManager` | Accepts new runner connections, mints a per-connection **session token**, runs the reconnect **binding swap** keyed on the runner's durable **stable identity**, rejects connects during shutdown, and dispatches `:runner_connected_event` (immediate convergence tick) |
| `StreamChannel` | Per-stream GenServer forwarding operative output to the runner socket; detects seq gaps and triggers entity resync |
| `ControlChannel` | Reads protobuf commands from the runner's control socket; stamps `last_seen` on every inbound frame and runs the **control-plane session-liveness watchdog** (sustained silence → connection close) |

Runner identity is two layers — a durable, SDK-owned **stable identity** (the
binding key) and an ephemeral controller-minted **session token** (the
routing/Registry key). Reconnect re-points the stable identity's operatives onto
the new session token rather than minting-and-evicting, giving binding continuity
across restart. See [`runner_identity.md`](runner_identity.md) and the
control-plane watchdog in [`transport_liveness.md`](transport_liveness.md).

---

## 11. Rust SDK Resilience

`sdk/rust/src/`

| Module | Role |
|--------|------|
| `shutdown.rs` | On SIGTERM/drop, serializes every running operative's final state and sends it to the controller so the next runner starts from the true last state |
| `flz_core/src/control_unit.rs` | Opens the control channel with full-jitter exponential backoff retry (cap 30 s) and reconnects automatically on disconnect or ack-timeout |
| `flz_core/src/persistent_id.rs` | Creates + persists the runner's durable **stable identity** (`<namespace>-<profile_hash8>-<rand6>`) so a restarted process re-presents it and the controller swaps its bindings onto the new session; degrades to a fresh per-boot id when the path is unwritable |
| `flz_core/src/flusher.rs` | Background thread draining each entity's change buffer on a 5 ms linger and sending framed WAL records to the controller |
| `flz_core/src/restore.rs` + the typed fold in `sdk/rust/src/entity/restore.rs` | Applies a `RestoreInitPayload` (snapshot + change-log) so a reconnected entity resumes from exactly its last state |
| `flz_core/src/mailbox.rs` | Bounded lock-free per-operative mailbox with configurable overflow policy (block/drop/replace) so a slow operative cannot back-pressure the whole runner |

**Control-channel liveness** (`flz_transport/src/transport/liveness.rs`, shared
by Rust and Java SDKs):

- **First-frame ack deadline** (`FLEZHA_CONTROL_ACK_TIMEOUT_MS`, default 10 s):
  a freshly opened channel that never gets an ack (controller mid-shutdown)
  reconnects instead of hanging forever.
- **Steady-state idle timeout** (`FLEZHA_CONTROL_IDLE_TIMEOUT_MS`, default 45 s):
  a half-open / wedged controller is detected and reconnected.
- **Keepalive ping** (`FLEZHA_CONTROL_KEEPALIVE_MS`, default 15 s): jittered
  round-trip prevents the idle timeout from firing on a healthy-but-quiet channel.

**Data-channel heartbeat** (`FLEZHA_DATA_HEARTBEAT_MS`, default 10 s, shared by
Rust and Java SDKs): the runner writes a bare `data_heartbeat` frame on every
secondary stream while connected. The control channel staying alive does not
prove the data plane is — the data plane is what froze in the 21 h candle
incident — so liveness rides the data channel directly. The controller's
`StreamChannel` treats any inbound frame (heartbeat or operative output) as
liveness and flags `[:flezha, :state, :output_stall]` only when a bound channel
is silent past `:io_stream_liveness_threshold_ms` (default 30 s, ≈3 beats). An
idle-but-healthy operative is therefore never mistaken for a stall.

---

## 12. Observability Feeding into Healing

| Module | Role |
|--------|------|
| `State.Watchdog` | Emits `[:flezha, :state, :output_stall]` telemetry consumed by the convergence loop |
| `Observability.TelemetryBridge` | Lifts all `[:flezha, ...]` telemetry onto the observability bus for Prometheus/OTLP/ring-buffer sinks |
| `Observability.Bus` | Fan-out dispatcher; a broken sink never crashes the producer |

---

## 13. Data-Plane Liveness Instrumentation (stall localization)

`flz_nif/src/driver_stats.rs`, `lib/flezha/transport/liveness_counters.ex`,
`lib/flezha/convergence/stall_classifier.ex`

Detection-only instrumentation that localizes a silent `io_in`/output stall to
the exact layer that wedged. The output-stall watchdog (§4) tells you a stream
went silent; this tells you **why** — was it the operative, the channel, the NIF
command queue, the driver thread, the socket, or the runner? It adds **zero**
data-plane behaviour and no remediation. See
[`transport_liveness.md`](transport_liveness.md) for the full signal set,
verdict decision tree, and the zero-code `/proc` fallback. Production
data-freeze incidents are tracked one-entry-per-occurrence in
`incident_log.md` at the repo root (local-only, gitignored).

---

## 14. Delivery Semantics, Dead-Letter Queue, and Correction

`lib/flezha/state/input_log.ex`, `lib/flezha/dlq/store.ex`

The default is **at-least-once**: output leaves as it is produced, and a
crash-replay can re-emit it. An operative declaring `effectively_once()` gets a
durable input log (every inbound payload journaled with an offset *before* it is
forwarded), one **atomic effect record** per invocation carrying `{state changes,
outputs, consumed offset, dead-letter entry, event time}` under one fsync, and an
**fsync-gated** outbox flush stamped with a replay-stable identity — so a
conforming sink (the built-in Postgres and Kafka bindings) deduplicates the
repeats a crash produces.

| Mechanism | Role |
|--------|------|
| Durable input log | Offset-addressable journal per operative class. Retention truncates below the minimum durable consumed-offset across its Entities. |
| `Flezha.Dlq.Store` | A payload that fails every retry is **parked durably** and its Entity advances — the minimum watermark keeps moving, and nothing is silently dropped. `flz dlq list \| show \| replay \| drop`. |
| Late-input correction | An event arriving behind the high-water mark but inside a declared window rewinds the Entity to a baseline, replays the slice in event-time order, and republishes under a bumped epoch — the keyed sink overwrites the stale rows. |
| Checkpoint ack-granularity | An operative that folds many inputs into one coherent state push acknowledges at `ctx.checkpoint()` rather than per input. |

Bounded operatives do not use the DLQ — their escape is AbortAll + saga
compensation (§6).

The same machinery carries **operative-to-operative** delivery
(`lib/flezha/plugin/provider/operative_forward.ex`), which adds no new identity
scheme, dedup store, or durable ledger. The sender's outbox already stamps
identity; the plugin's worker retries until the target confirms; the target's
`journal_input/2` already absorbs duplicates. The guarantee is therefore
**derived, not configured** — a source with a transactional outbox gets
effectively-once across the hop, one without gets at-least-once — and there is
deliberately no delivery-mode option to misconfigure. In-flight queued items do
not survive a controller restart, identically to the Kafka and Postgres sink
buffers; see `data_durability.md` "Operative to operative".

See [`data_durability.md`](data_durability.md) for the semantics and the operator
playbook, and [`../architecture/unbounded_profile.md`](../architecture/unbounded_profile.md)
for the model and diagrams.

---

## Durability guarantee summary

| Scenario | State loss | Output |
|----------|-----------|--------|
| Graceful runner stop (SIGTERM / `disconnect()`) | None — SDK ships final state | At-least-once |
| Runner SIGKILL | Best-effort (async WAL lag) | At-least-once |
| Controller graceful restart with live peer | None — peer sync | At-least-once |
| Controller recovery-mode restart, sole instance | None — DETS replay | At-least-once |
| Whole cluster cold boot | Recovered from DETS | n/a |
| Bounded operative runner SIGKILL (durable) | None past last WAL fsync — restores + re-runs the chain | At-least-once |
| Bounded operative runner SIGKILL (persistence opted out) | Full — re-runs from the top | At-least-once |
| Operative-to-operative hop, target down or parked | None — retained and retried until confirmed | Effectively-once if the source has an outbox, else at-least-once |
| Operative-to-operative hop, controller dies mid-queue | n/a | Queued-but-unconfirmed items lost (same as the Kafka / Postgres sink buffers) |

For the full scenario matrix and operator playbook see
[`data_durability.md`](data_durability.md); for the bounded operative model
(structure, lifecycle, saga, crash-resume, retention) see
[`bounded_operative.md`](bounded_operative.md).
