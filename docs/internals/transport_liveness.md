# Transport Liveness

For contributors working on the controller↔runner transport and the signals the convergence loop
heals on. Two complementary signals cover two wedge classes: a **control-plane session watchdog**
(is the runner session alive at all?) and **data-plane stall instrumentation** (which layer wedged
when output went silent?). This document covers both, the send-buffer cap, the stall classifier
and its remedies, the producing-liveness markers, the application-facing entry-idle signal,
configuration, and the operator fallback when the BEAM itself is unresponsive.

| Wedge class | Caught by |
|---|---|
| Process / control-channel wedge — the runner stops sending entirely (the UDS half-open blind spot) | the control-plane `last_seen` watchdog |
| Dispatch-pool wedge — the work pool is frozen but the keepalive thread still pings | data-plane `output_stall` + the `:runner_pool_wedged` verdict |

## Control-plane session liveness

`Flezha.Runner.ControlChannel` treats the inbound control-frame stream as the single liveness
signal for a runner session. It stamps a monotonic `last_seen` on **every** inbound control frame,
including the SDK's keepalive ping, which it also echoes back verbatim on the dedicated
`control_ping` payload so the SDK's own idle watchdog re-arms (the ping never enters the command
path on either side). A jittered watchdog (`:liveness_tick`, every
`:control_liveness_interval_ms` jittered into `[interval/2, interval]`; `0` disables it) treats
silence past `:control_liveness_window_ms` (3 × the keepalive) as a **connection close**: it emits
`[:flezha, :runner, :liveness_timeout]` (`%{count: 1, silent_ms}`, `%{runner_id, namespace}`) and
routes through the same `handle_connection_closed/1` → `:runner_disconnected` path a real socket
close uses. There is no parallel eviction path.

QUIC declares death in the same window by construction: both `flz_transport/src/transport/quic.rs` and
`flz_nif/src/quic_driver.rs` set `keep_alive_interval` to 5 s and `max_idle_timeout` to
`liveness::control_idle_timeout()` instead of the quinn default. When tuning, pair
`FLEZHA_CONTROL_LIVENESS_WINDOW_MS` (the controller watchdog) with `FLEZHA_CONTROL_IDLE_TIMEOUT_MS` (the
QUIC idle and the Rust/Java SDK idle watchdog in `flz_transport/src/transport/liveness.rs`; the Elixir
SDK pings every 15 s from `Flezha.SDK.RunnerServer`).

**Calibrated, not trigger-happy.** The window is multiple missed pings, jittered, and the action
is a connection close — a cheap reconnect through the binding swap in
[`runner_identity.md`](runner_identity.md), not a redistribute sweep; the SDK keepalive runs on its
own timer thread, so a busy runner keeps proving liveness and a brief GC pause cannot trip it.
`ControlUnit` failing unacked control replies (`fail_stuck_refs`) only unblocks callers; it is not
a liveness signal.

## The instrumented path

```text
source plugin
  → Operative.Overwatch ingest            [io_in_received, forwarded, filtered_nil]
  → StreamChannel.io_in/3                 [io_in_received, send_frame_calls, nil_drops, last_send_ms]
  → Transport.Stream.send_frame
  → [command channel]                     [cmd_queue_depth]
  → flz-uds-driver loop (uds_driver.rs)   [loop_iterations, last_loop_ms]
  → per-stream write buffer → UnixStream  [write_buffer_len, write_buffer_dropped, frames_written, last_write_ms]
Inbound mirror: driver read → StreamChannel({:flz_data}) [flz_data_received] + [frames_read, last_read_ms]
```

## Signals

The counters are passive and the classifier is pure — nothing here changes data-plane behaviour.

**NIF driver self-report** (`Flezha.Transport.Native.driver_stats/1`,
`flz_nif/src/driver_stats.rs`): per UDS server — `loop_iterations` (bumped once per loop
iteration), `last_loop_ms` and `now_ms` (driver-elapsed ms), `cmd_queue_depth`, and per stream
`{write_buffer_len, write_buffer_dropped, frames_written, frames_read, last_write_ms,
last_read_ms}`. The loop counters are lock-free `AtomicU64`; the per-stream snapshot is rebuilt
under a `Mutex` once per loop iteration, never per frame. A frozen `loop_iterations` with a stale
`last_loop_ms` unambiguously means the thread is wedged. QUIC has no per-stream stats
(`driver_stats/1` returns `:undefined`); the `native` in-node transport has no driver, so
`Transport.Server.get_stats(:native)` carries no `:driver` key (the QUIC shape — no native-specific
branch). `StreamAcceptor` registers each per-runner data-stream server
(`LivenessCounters.register_driver/3`) so the classifier polls the drivers carrying operative traffic.

**BEAM counters** (`Flezha.Transport.LivenessCounters`): always-on `:atomics`-backed `:counters`, one
array per channel (`{runner_id, name}`) and per operative, registered in a public ETS table and pruned
of dead owners on `snapshot_all/0`. Channel slots: `io_in_received`, `send_frame_calls`, `nil_drops`,
`flz_data_received`, `last_send_ms`, `hb_count`, `hb_dispatches`, `hb_credit_parked`, `credit_active`,
`credit_ledger_level`, `credit_overrun_total`, `stale_writer_refused`. Operative slots: `io_in_received`,
`forwarded`, `filtered_nil`.

**Runner dispatch self-report** (all three SDKs): every `data_heartbeat` frame on a secondary stream
carries `dispatches` (process-global payloads dispatched through the poll engine), `blocked_parks`
(Block-policy full-mailbox episodes) and, from `flz_core`, `credit_parked`. The heartbeat thread
outlives a wedged dispatch pool, so without the counter a runner can look alive while processing
nothing. The secondary `StreamChannel` mirrors `dispatches` into `hb_dispatches` and `credit_parked`
into `hb_credit_parked` and bumps `hb_count`; the per-stream `output_stall` heartbeat check runs only
on secondary channels — primary streams are send-only.

**Metric emission** (`Observability.OtpBackend.TransportSource`, ~1 Hz): driver stats per transport
(`:driver_loop_iterations`, `:driver_loop_age_ms`, `:driver_cmd_queue_depth`, `:driver_write_buffer_bytes`,
`:driver_streams`) and the BEAM counters as flat global aggregates (`:channel_*`, `:operative_*`). No
per-runner or per-stream labels — per-entity localisation is the classifier's job.

## Send-buffer cap

The per-stream send buffer is bounded so a slow or non-reading runner cannot grow controller memory
without limit. When the buffer is non-empty and appending a frame would exceed `max_write_buffer_bytes`
(`flz_transport/src/transport/config.rs`), `enqueue_write` drops the newest frame — a `DropNewest`
policy mirroring the SDK bounded mailbox — and bumps the per-stream `write_buffer_dropped` counter. An
empty buffer always accepts, so a single large frame on an idle stream is never dropped. The data
stream is at-least-once with downstream dedup, so a dropped frame is recoverable; the driver never
blocks its command loop on a slow peer. The QUIC driver mirrors the cap and counter on its write buffer.

## The stall classifier

`Flezha.Convergence.StallClassifier.classify/3` is a pure function over the current and previous
liveness snapshot (control driver `get_stats` + registered data-stream `driver_stats` +
`LivenessCounters.snapshot_all/0`), built by the convergence Loop each tick; the first tick has no
previous snapshot and is always `:ok`. On a verdict change the Loop logs `[convergence] data-plane stall
localized: layer=…` (an info line on clear) and emits one `[:flezha, :data_plane, :stall_localized]`
event carrying `%{layer}`. Any error in the step is swallowed; the classifier can never break the tick it feeds.

Verdicts are evaluated top-down and the **deepest wedged layer wins** (it explains every layer
above it), so there is one verdict per tick and no remedy fan-out. Each has a built-in "is it being
fed?" precondition — a count that must be advancing between ticks — so an idle operative never
reads as a stall. Defaults: `driver_freeze_ms` 5 000, `stall_ms` 30 000, `queue_high` 256.

| Verdict | Signature |
|---|---|
| `:driver_loop_frozen` | `loop_iterations` unchanged and `now_ms − last_loop_ms` > `driver_freeze_ms` |
| `:nif_queue_backing_up` | `cmd_queue_depth` > `queue_high` and rising, no stream's `frames_written` advancing |
| `:socket_write_blocked` | a stream's `write_buffer_len` rising, `frames_written` advancing, last write older than `stall_ms` |
| `:runner_not_consuming` | `frames_written` advancing, `frames_read` flat and previously > 0 (exempts send-only primaries), last read older than `stall_ms` |
| `:runner_pool_wedged` | per runner: `send_frame_calls` and `hb_count` advancing, self-reported `hb_dispatches` > 0 and frozen |
| `:channel_not_sending` | channel `io_in_received` advancing, `send_frame_calls` and `nil_drops` flat |
| `:operative_not_forwarding` | operative `io_in_received` advancing, `forwarded` flat, the gap not explained by `filtered_nil` |

Each layer declares its **scope** (`StallClassifier.layer_scope/1`) beside the check that mints it, so
every verdict has exactly one consumer. The verdict is passed into `Diff.compute/3` in-process on the
tick that computed it; the Diff resolves the scope against the same probed reality every other invariant
uses (a verdict naming a runner or operative the probe did not observe heals nothing and is dropped),
and the Planner picks the remedy:

| Scope (verdicts) | Consumer | Remedy |
|---|---|---|
| `:runner` (`runner_not_consuming`, `runner_pool_wedged`) | Diff → Planner | `evict_runner` with `proved_wedged: true`; the planner's ordering rebinds that runner's operatives |
| `:connection` (`socket_write_blocked`, `channel_not_sending`) | Diff → Planner | `resync_runner` for every operative bound to that runner — in place, no migration |
| `:operative` (`operative_not_forwarding`) | Diff → Planner | `rebind_operative` for the named operative |
| `:fabric` (`driver_loop_frozen`, `nif_queue_backing_up`) | Snapshot → `/healthz` | `503 data_plane_fabric_wedged` after `:convergence_fabric_503_polls` consecutive polls — no BEAM-level heal reaches a wedged NIF thread |

Only the per-runner data transports (`{:uds_data, runner_id, type}`) identify a runner; a
control-transport verdict heals nothing. Wedge ops carry `args: %{reason: :data_plane_wedge, layer}`,
ride the standard per-target backoff, and waive the guards that spare a *healthy* target
(`Heal.skip_rebind?/4`, FleetManager's "never evict a live runner") — a pool-wedged runner is still
connected, which is exactly why nothing else can dislodge it.

> **Dormant verdicts.** `channel_not_sending` and `operative_not_forwarding` cannot currently fire:
> `StreamChannel` bumps `io_in_received` together with exactly one of `send_frame_calls` / `nil_drops`,
> and `Operative.Ingest` bumps `io_in_received` together with exactly one of `forwarded` /
> `filtered_nil`, so each "flat while fed" condition is unsatisfiable by construction (the counters
> sit at the branch that chose a path). Their heal mappings are unit-tested and activate when the counters move.

## Producing-liveness (progress markers)

`StallClassifier.classify_producing/3` is the per-operative producing-liveness classifier the Loop
runs at tick cadence. `Convergence.Loop.progress_markers/0` builds the markers from
`Flezha.State.Watchdog.entity_statuses/0` (`last_change_seen_at` / `last_advance_at`, the
`out_pending_seq` / `outbox_flushed_seq` outbox cursors — `out_pending_seq` counts only output-bearing
records, so a state-only Entity never reads as a frozen outbox — and `outbox_unconfirmed_ms`) joined
with `Flezha.State.InputLog.cursors/1` (the log tail and each Entity's durable consumed offset): one
input-log read per operative per tick, not per Entity. A tier-off operative yields no cursor fields and
degrades to WAL-seq-only. It shares the deepest-verdict-wins convention and the fed precondition (IDLE
counts as producing; STUCK degrades `/healthz` with detail `wal_frozen | watermark_frozen | outbox_flat`)
and is report-only — healing stays on the output-stall ladder. Verdict definitions and the `/healthz`
contract: [`convergence_loop.md`](convergence_loop.md#health-publishing).

## The application's own signal (entry-input idle)

Everything above answers an operator's question — which layer wedged — and says nothing to the
application, which cannot see the data stream (the SDK owns the IO threads). So the SDK exposes, per
entry channel, when a frame last arrived, and accepts an idle observer carrying the application's own
threshold: `runner.on_entry_idle(channel, threshold, observer)` (Rust) / `runner.onEntryIdle(...)`
(Java). It fires once per idle episode, never before the channel's first frame, re-arms on resume, and
ships no default threshold — the signal is SDK knowledge, the policy (is this silence anomalous) is
domain knowledge. Contract: [`../sdk/entry_input_idle.md`](../sdk/entry_input_idle.md).

## Configuration

| Env var | App key | Default | Meaning |
|---|---|---|---|
| `FLEZHA_CONTROL_LIVENESS_WINDOW_MS` | `:control_liveness_window_ms` | `45000` | Controller-side silence window before a session counts as closed |
| `FLEZHA_CONTROL_LIVENESS_INTERVAL_MS` | `:control_liveness_interval_ms` | `15000` | Watchdog check interval (jittered); `0` disables the watchdog |
| `FLEZHA_CONTROL_IDLE_TIMEOUT_MS` | (Rust, `flz_transport`) | `45000` | QUIC `max_idle_timeout` and the Rust/Java SDK idle watchdog |
| `FLEZHA_CONTROL_KEEPALIVE_MS` / `FLEZHA_CONTROL_ACK_TIMEOUT_MS` / `FLEZHA_DATA_HEARTBEAT_MS` | (Rust, `flz_transport`) | `15000` / `10000` / `10000` | SDK keepalive ping interval, first-frame ack deadline, data-heartbeat interval |
| `FLEZHA_MAX_WRITE_BUFFER_BYTES` | (Rust, `flz_transport`) | `8388608` | Per-stream send-buffer cap, UDS and QUIC |
| `FLEZHA_DATA_PLANE_LIVENESS` | `:observability, :data_plane_liveness` | `true` | Gates the ~1 Hz driver poll and the per-tick classifier; the counters are always on |
| — | `:io_stream_liveness_threshold_ms` / `:io_stream_liveness_interval_ms` | `30000` / `10000` | Secondary-channel heartbeat silence that flags `output_stall`, and its check interval |

The classifier runs only when the convergence Loop runs (`FLEZHA_USE_CONVERGENCE_HEALTH`, default
on); the `:flezha_transport` metric source only when `:observability, :otp_backend_enabled` is on.

## Zero-code operator fallback

If the BEAM is up but unresponsive, sample the OS threads directly: `bin/flzvm pid` gives the BEAM
pid; read `/proc/<pid>/task/*/comm` for threads named `flz-uds-driver` and check
`/proc/<pid>/task/<tid>/stat` — a thread in `D` (uninterruptible sleep) or with a frozen `utime` is a
wedged driver, matching a frozen `loop_iterations`. No controller code path is involved, so this works
even if the NIF itself is what wedged.

## See also

- [`convergence_loop.md`](convergence_loop.md) — the consumer of every verdict here
- [`runner_identity.md`](runner_identity.md) — the binding swap a liveness-driven close leads to
- [`nif_transport.md`](nif_transport.md) — the driver threads being instrumented
- [`../sdk/entry_input_idle.md`](../sdk/entry_input_idle.md) — the application-facing idle signal
- [`../operations/observability.md`](../operations/observability.md) — where the `:flezha_transport` metrics land
