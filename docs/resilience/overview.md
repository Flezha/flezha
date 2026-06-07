# Fault Tolerance, Self-Healing & Recovery — Overview

This document maps every mechanism in Flezha that prevents data loss, detects
failures, and restores the system to a healthy state. It is the entry point;
each section links to the dedicated doc for its subsystem.

The system has three layers:

- **Prevention** — bound mailboxes, WAL durability, graceful drain
- **Detection** — watchdog sweeps, control-channel liveness, seq-gap detection,
  convergence probes
- **Recovery** — convergence loop rebind/plugin-restart/evict, Overwatch retry,
  SDK reconnect, ProcessHub cold-boot recovery, partition resync

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
| `Convergence.Intent` | Reads the ProcessHub registry to know what relays and plugins _should_ exist |
| `Convergence.Probe` | Reads live reality (active executors, relay states, hub recovery); pure observer |
| `Convergence.Diff` | Pure function comparing intent vs reality → delta (unbound/misbound relays, missing/extra plugins) |
| `Convergence.Planner` | Turns the delta into an ordered, backoff-filtered list of ops (cap: 20/tick) |
| `Convergence.Heal` | Executes ops via four idempotent primitives: `rebind_relay`, `start_plugin`, `stop_plugin`, `evict_executor` |
| `Convergence.Backoff` | Per-`{primitive, target}` exponential cooldown: 5 s → 30 s → 60 s → 300 s |
| `Convergence.Snapshot` | ETS table holding the latest published health summary; read by `/healthz`, CLI, and telemetry |
| `Convergence.RecoveryGate` | Classifies which namespaces are ready for healing based on per-hub recovery state; `:timeout` hubs are included rather than blocking all healing |
| `Convergence.SafeCall` | Bounded `GenServer.call` that returns `{:error, reason}` instead of crashing when a process is wedged or gone |

**Stability guarantees built into the loop:**

- A relay must be misbound for **≥ 2 consecutive ticks** before a rebind is
  issued — a single blip cannot trigger a full-namespace migration.
- The entire tick body is wrapped in `try/rescue`; an uncaught error becomes a
  skipped tick, not a Loop crash.
- If `active_executors` returns `:unknown` (a read failure), the diff suppresses
  destructive ops for that namespace — it is not treated as "all executors gone."
- Tick events (`:output_stall`, `:executor_connected`) shorten detection latency
  by triggering an immediate tick; they do not bypass the pipeline.

See [`convergence_loop.md`](convergence_loop.md) for the full architecture and
configuration reference.

---

## 2. Per-Relay Overwatch

`lib/flezha/relay/overwatch.ex`

A per-relay GenServer that does the actual bind work: ping an executor, negotiate
relay start, install plugins, and ship persisted WAL state on attach. On any
failure it retries with jittered exponential backoff
(`200 ms → 500 ms → 1 s → 2 s → 5 s → 10 s`). The convergence loop's
`rebind_relay` primitive calls into Overwatch; Overwatch never calls back into
the loop.

---

## 3. State Persistence — WAL + Snapshots

`lib/flezha/state/`

Every relay state change is durable before the executor's output plugin fires,
so a hard restart restores state from disk rather than losing it.

| Module | Role |
|--------|------|
| `State.WalWriter` | Appends every state change to a per-partition WAL file |
| `State.FsyncBatcher` | Coalesces all WAL writers' fsync calls into one batch every 5 ms |
| `State.Snapshot` | Crash-safe atomic snapshots: write `.tmp` → fsync → rename |
| `State.Outbox` | Persists the last-sent seq per partition as a dedup marker |
| `State.Restore` | Assembles snapshot + tail-WAL bytes to send to a reconnecting executor |
| `State.Recovery` | Re-runs the `RestoreInitState` handshake when a partition is detected as desync'd |
| `State.Retention` | Sweeper pruning old snapshots and WAL records beyond the retention window |
| `State.TimeTravel` | Point-in-time query: replay WAL up to a target seq or timestamp |
| `State.Watchdog` | Detects a partition still receiving changes but whose seq stopped advancing |
| `State.PersistenceResolver` | Resolves per-relay persistence config from executor override → relay metadata → global default |

**Back-pressure:** when a WAL writer's pending bytes exceed the high-watermark
(default 16 MiB), it sends a `PartitionMailboxStatus{status: "full"}` command to
the SDK. The SDK's overflow policy (block/drop/replace) activates until the
low-watermark is crossed.

See [`state_persistence.md`](state_persistence.md) for the WAL record format,
snapshot lifecycle, crash-recovery semantics, and all configuration knobs.

---

## 4. Output-Stall Detection

`lib/flezha/state/watchdog.ex`

Sweeps every 10 s. For each persistence-enabled partition it checks:

1. **Output staleness** — still receiving changes but seq has not advanced within
   `output_stall_timeout_ms` (default 60 s) → emits
   `[:flezha, :state, :output_stall]` and triggers `State.Recovery.resync/2`.
   A genuinely idle partition (no changes arriving) is never flagged.
2. **Compaction timeout** — a `CompactPartition` whose snapshot has not arrived
   within `compaction_timeout_ms` (default 30 s) → emits
   `[:flezha, :state, :compaction_timeout]` and triggers resync.

The convergence loop subscribes to `[:flezha, :state, :output_stall]` and fires
an immediate tick on receipt.

---

## 5. Seq-Gap Self-Heal

`lib/flezha/executor/stream_channel.ex`

When `StreamChannel` receives a change frame with
`seq > last_persisted_seq + 1`, it emits `[:flezha, :state, :seq_gap]` and
calls `State.Recovery.resync/2` so the SDK rewinds and resends from
`terminal_seq + 1`. This is silent and automatic — no operator action required.

---

## 6. Boot Recovery

`lib/flezha/boot/`

| Module | Role |
|--------|------|
| `Boot.StatePathMigrator` | One-shot, idempotent rename of legacy executor-keyed WAL paths to the flat layout; runs before any WalWriter opens |
| `Boot.RecoveryHooks` | Installs telemetry forwarders for ProcessHub's `pre_recovery_replay` / `post_recovery_replay` lifecycle events |
| `BootLoader` | Runs pre-boot hooks (sockets, certs) and gates hub creation on cluster readiness |

**ProcessHub recovery gate:** `HubManager` starts each namespace hub with a
durable DETS registry backend and an auto-recovery window. When a hub starts on
a fully-downed cluster, it replays the relay list from DETS (opt-in via the
`cluster.healthy` marker / `flz cluster prepare-recovery`). When joining a live
cluster it syncs from peers and discards the local DETS copy — peers are always
authoritative. See [`process_hub_recovery_model.md`](process_hub_recovery_model.md).

---

## 7. Graceful Shutdown & Drain

`lib/flezha/shutdown/`

| Module | Role |
|--------|------|
| `Shutdown.Coordinator` | Three-phase shutdown: quiesce (block new relays/connections) → drain (wait for relays to migrate to peers) → finalise (flush WAL, close transports) |
| `Shutdown.SignalHandler` | Installs SIGTERM/SIGINT OS handlers in every BEAM run mode; third signal triggers `erlang.halt(143)` |

The drain phase polls peer registry dumps until every locally-hosted relay is
visible on at least one peer. Solo nodes skip the wait. On graceful stop the SDK
ships final relay state before exiting, so the WAL is consistent at the next boot.

See [`graceful_shutdown.md`](graceful_shutdown.md).

---

## 8. Health Endpoint

`lib/flezha/healthz/server.ex`

HTTP `/healthz` listener (opt-in, `FLEZHA_HEALTHZ_ENABLED=true`). Reads only
`Convergence.Snapshot.current/0` — holds no state of its own.

| Status | HTTP | Condition |
|--------|------|-----------|
| `healthy` | 200 | All hubs `:normal` and `producing_relays == bound_relays` |
| `degraded` | 200 | One or more relays output-stalled (kubelet must not restart — the loop is healing) |
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

## 10. Executor Connection Resilience

`lib/flezha/executor/`

| Module | Role |
|--------|------|
| `FleetManager` | Accepts new executor connections, rejects them during shutdown, and dispatches `:executor_connected_event` which triggers an immediate convergence tick |
| `StreamChannel` | Per-stream GenServer forwarding relay output to the executor socket; detects seq gaps and triggers partition resync |
| `ControlChannel` | Reads protobuf commands from the executor's control socket and dispatches them |

---

## 11. Rust SDK Resilience

`sdk/rust/src/`

| Module | Role |
|--------|------|
| `shutdown.rs` | On SIGTERM/drop, serializes every running relay's final state and sends it to the controller so the next executor starts from the true last state |
| `executor/control_unit.rs` | Opens the control channel with full-jitter exponential backoff retry (cap 30 s) and reconnects automatically on disconnect or ack-timeout |
| `executor/persistent_id.rs` | Persists the controller-assigned executor ID to disk so a restarted process can identify itself and get a clean eviction rather than leaving a ghost binding |
| `partition/flusher.rs` | Background thread draining each partition's change buffer on a 5 ms linger and sending framed WAL records to the controller |
| `partition/restore.rs` | Applies a `RestoreInitPayload` (snapshot + change-log) so a reconnected partition resumes from exactly its last state |
| `executor/mailbox.rs` | Bounded lock-free per-relay mailbox with configurable overflow policy (block/drop/replace) so a slow relay cannot back-pressure the whole executor |

**Control-channel liveness** (`flz_transport/src/transport/liveness.rs`, shared
by Rust and Java SDKs):

- **First-frame ack deadline** (`FLEZHA_CONTROL_ACK_TIMEOUT_MS`, default 10 s):
  a freshly opened channel that never gets an ack (controller mid-shutdown)
  reconnects instead of hanging forever.
- **Steady-state idle timeout** (`FLEZHA_CONTROL_IDLE_TIMEOUT_MS`, default 45 s):
  a half-open / wedged controller is detected and reconnected.
- **Keepalive ping** (`FLEZHA_CONTROL_KEEPALIVE_MS`, default 15 s): jittered
  round-trip prevents the idle timeout from firing on a healthy-but-quiet channel.

---

## 12. Observability Feeding into Healing

| Module | Role |
|--------|------|
| `State.Watchdog` | Emits `[:flezha, :state, :output_stall]` telemetry consumed by the convergence loop |
| `Observability.TelemetryBridge` | Lifts all `[:flezha, ...]` telemetry onto the observability bus for Prometheus/OTLP/ring-buffer sinks |
| `Observability.Bus` | Fan-out dispatcher; a broken sink never crashes the producer |

---

## Durability guarantee summary

| Scenario | State loss | Output |
|----------|-----------|--------|
| Graceful executor stop (SIGTERM / `disconnect()`) | None — SDK ships final state | At-least-once |
| Executor SIGKILL | Best-effort (async WAL lag) | At-least-once |
| Controller graceful restart with live peer | None — peer sync | At-least-once |
| Controller recovery-mode restart, sole instance | None — DETS replay | At-least-once |
| Whole cluster cold boot | Recovered from DETS | n/a |

For the full scenario matrix and operator playbook see
[`data_durability.md`](data_durability.md).
