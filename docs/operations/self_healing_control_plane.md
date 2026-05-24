# Self-healing control plane

The Controller continuously reconciles **desired state** (the durable ProcessHub
registry: each relay's child-spec, plugins, `factory_id`, assigned executor, and
placement) against **actual state** (cheap liveness probes). One authoritative
control-loop — `Flezha.Reconciler` — is the single orchestrator of healing; it
subsumed the scattered self-timers that used to heal independently (the Overwatch
park-retry timer, the `State.Watchdog`/`State.Retention` ticks, and
`FleetManager.reissue_local_orphans`). The providers' fast in-process detectors
(WebSocket idle/resubscribe, Kafka producer retry/swap, `MonitoredIOForward`
heartbeat) are retained — the loop **consumes** their signals, it does not replace
them.

## What the loop does

On events (executor connect/disconnect, ProcessHub `child_registered` /
`child_unregistered` / `post_node_join` / `post_node_leave` /
`post_recovery_replay`) — each debounced and targeted — plus a bounded periodic
backstop sweep, the loop probes every relay, classifies its health
(`HEALTHY` / `DEGRADED` / `FAILED`), and applies idempotent convergence actions:

| Action            | When                                                          | Scope          |
|-------------------|--------------------------------------------------------------|----------------|
| `purge_dead_node` | registry entry references a pid on a node not in the cluster | leader-only    |
| `replace`         | relay's only placement was a dead node, executor available   | leader-only    |
| `unpark`          | parked Overwatch + an executor for its factory is available  | node-local     |
| `reissue`         | running relay whose assigned executor went away              | node-local     |
| `rebind_stream`   | running relay with an unbound data `StreamChannel`           | node-local     |
| `restart_plugin`  | a plugin reported unhealthy across the escalation threshold  | node-local     |

The periodic sweep also drives the two per-Partition persistence sweeps it
subsumed: `State.Watchdog` (output-staleness + compaction-timeout detection, which
self-heals via `State.Recovery.resync`) and `State.Retention` (time-travel WAL
prune), each clocked at its own configured cadence.

## Leader topology

Leadership is opt-in and elector-backed (via `ProcessHub.start_leadership/0`). One
controller is elected leader and **serializes** the cluster-wide, registry-mutating
reconcile (`purge_dead_node`, `replace`) so peers never race the same entries.
Placement itself stays ProcessHub's `ConsistentHashing`; the leader only ensures
the CH-decided placement is realized and producing. **Every node always runs its
local reconcile** (un-park, reissue, rebind, plugin restart), so the data path is
never blocked waiting on an election. A `Process.monitor` fallback covers the
window around an election.

`flz cluster status` shows the elected leader, per-node relay counts, and the
health rollup; `flz relay list` shows a per-relay HEALTH column.

## Configuration

### Controller (`config/runtime.exs`, env-driven)

| Env var | Default | Meaning |
|---|---|---|
| `FLEZHA_RECONCILER_ENABLED` | `true` | Master kill-switch. `false` reverts to the pre-reconciler code paths (see Rollback). |
| `FLEZHA_LEADERSHIP_ENABLED` | `true` | Opt into elector-backed cluster-wide orchestration leadership. |
| `FLEZHA_RECONCILE_SWEEP_INTERVAL_MS` | `20000` | Backstop full-sweep interval. `0` disables the sweep (events only). |
| `FLEZHA_RECONCILE_DEBOUNCE_MS` | `250` | Window coalescing a burst of events into one targeted reconcile. |
| `FLEZHA_THROUGHPUT_STALL_WINDOW_MS` | `30000` | Egress "data in, nothing out" stall window. |
| `FLEZHA_RECONCILE_PLUGIN_ESCALATION_THRESHOLD` | `2` | Consecutive unhealthy probes before the loop restarts a plugin (the provider's own recovery gets that long first). |
| `FLEZHA_OUTPUT_WATCHDOG_INTERVAL_MS` | `10000` | Cadence the sweep drives the persistence output-staleness/compaction watchdog (floored by the sweep interval). |
| `FLEZHA_RETENTION_CHECK_INTERVAL_MS` | `60000` | Cadence the sweep drives the time-travel retention prune (floored by the sweep interval). |
| `FLEZHA_MAX_RECONNECTS` | `5` | Jittered inter-controller reconnect attempts before falling back to the periodic discovery sweep (raised from `1`). |
| `FLEZHA_RECONNECT_MAX_BACKOFF_MS` | `30000` | Upper bound for the exponential, jittered reconnect backoff. |
| `FLEZHA_CLUSTER_FORMATION_TIMEOUT_MS` | `15000` | Bounded "degrade to solo" window: how long boot waits for discovered peers before declaring the cluster ready (so a hub is never created on a not-yet-clustered node). |

### Executor SDK — control-channel liveness (Rust + Java)

Read by the shared `flz_transport` liveness layer (and Java `FlezhaEnv`); applies to
both SDKs.

| Env var | Default | Meaning |
|---|---|---|
| `FLEZHA_CONTROL_ACK_TIMEOUT_MS` | `10000` | Deadline for the first control frame after connect; on expiry the executor reconnects (closes the accept-but-never-ack strand). |
| `FLEZHA_CONTROL_IDLE_TIMEOUT_MS` | `45000` | Steady-state idle deadline: a wedged/half-open controller after the handshake is detected and reconnected. |
| `FLEZHA_CONTROL_KEEPALIVE_MS` | `15000` | Interval for the jittered `control_ping` keepalive the executor sends; the controller echoes it, re-arming the idle deadline so a healthy-but-quiet channel never false-fires. |

## Observability

Telemetry (bridged onto the observability bus via the canonical mapping):

- `[:flezha, :reconcile, :action]` — `metadata.action ∈ {rebind_stream, reissue,
  restart_plugin, purge_dead_node, replace, unpark}` plus `namespace`, `relay_id`, `node`.
- `[:flezha, :reconcile, :health]` — a relay's health transition (`previous` → `health`).
- `[:flezha, :leadership, :transition]` — leadership gained/lost.
- `[:flezha, :stream, :drop]` — an `io_in` frame dropped because the data stream was unbound.
- `[:flezha, :state, :output_stall]` / `[:flezha, :state, :compaction_timeout]` — persistence-path self-heal.

## Rollback

Setting `FLEZHA_RECONCILER_ENABLED=false` stops the reconciliation loop; the
Controller boots and serves relays on the pre-reconciler code paths and the kept
provider in-process detectors. Note: with the loop disabled the subsumed
healers (park-retry, persistence output-staleness, retention prune, orphan
reissue) do **not** run — rollback trades self-healing for the prior behavior, so
treat it as an incident lever rather than a steady-state mode. The SDK
control-channel ack/idle deadlines are independently env-gated and unaffected.

## Deploying (proposed; the operator runs — prod is read-only here)

Prerequisites (one-time for this change):

1. **Land the ProcessHub `dev` changes.** Leadership / dead-node purge / oracle /
   migration live on `../process-hub` `dev`. During development Flezha pins it by
   path (`mix.exs`: `{:process_hub, path: "../process-hub"}`); before pushing,
   switch that back to the git dep at the pushed `dev` SHA
   (`{:process_hub, git: "https://github.com/alfetahe/process-hub", branch: "dev"}`)
   so the build is reproducible off GitHub.
2. **Refresh the cross-triple SDK natives.** Only `linux-x64`'s `libflz_jni.so`
   was rebuilt locally. Before deploying the Java SDK, cross-refresh the other
   four triples (`scripts/build-native.sh --targets all`, needs `zig`) so the
   committed `libflz_jni.{so,dylib,dll}` carry the flz_jni/flz_transport liveness
   changes.

Release sequence:

1. Push Flezha to GitHub `main` (controller + SDK).
2. In `flezha_market/market-processor`, pull the new executor SDK:
   `cargo update -p flezha_executor` (the dep tracks `flezha.git` `main`).
3. Roll the cluster: `./kubernetes/update.sh market-executor flezha-controller`
   (rebuilds + restarts the executor DaemonSet and the controller).

## Verifying the deployment

- `./kubernetes/smoke-warm-restart.sh` exits `0` (graceful leg: every pre-rollout
  relay PID returns). Also run `./kubernetes/smoke-warm-restart.sh --simulate-oom`
  (SIGKILL leg: relays return **and** Kafka topic offsets advance).
- Kafka end-offsets advance on all `btc_kraken.*` topics with no manual
  `./kubernetes/start-asset.sh` — the reconciler resumes production on its own.
- `flz cluster status` shows exactly **one** leader and **both** nodes, with the
  per-node relay counts and a `HEALTHY` rollup; `flz relay list` shows each relay
  `HEALTHY`.
- **Rollback lever:** set `FLEZHA_RECONCILER_ENABLED=false` in
  `kubernetes/flezha-configmap.yaml` and `./kubernetes/update.sh flezha-controller`
  to revert to the pre-reconciler code paths.
