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
`flz_nif/src/driver/quic.rs` set `keep_alive_interval` to 5 s and `max_idle_timeout` to
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

The runner's own heartbeat is owed no delivery: it reports a moment that has passed, so it is
never held for a replay. Its write verdict is evidence instead — a heartbeat counts as delivered
when ANY bound secondary took it, and a run of ticks that reached none crosses
`FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD` misses inside `FLEZHA_BROKEN_PIPE_WINDOW_MS` and asks
for a reconnect, re-armed after `FLEZHA_RECONNECT_REQUEST_RETRY_MS` so an unanswered request
cannot disarm the detector. Both runtimes run that ladder: `flz_core`'s `heartbeat_tick` through
`state_sink::note_transport_failure`, and the Elixir SDK's through its own guarantee class. It is
the only signal an idle runner gives that its outbound wire is gone.

A wire that has gone for good is a different fact, and it is detected once per runtime rather
than once per host: `flz_core::shutdown` owns both the standard-input end-of-file watch and the
SIGINT/SIGTERM handler, on a thread no host's virtual machine owns, and every erased host reaches
the watch through one ABI entry. Both lead to the same exit — ship every operative's final state,
stop the poll core so the host's own workers drain, then end — so a runner cannot drain
differently for one than for the other, and a compute-bound handler holding the host's loop
cannot delay either.

## The instrumented path

```text
source plugin
  → Operative.Overwatch ingest            [io_in_received, forwarded, filtered_nil]
  → StreamChannel.io_in/3                 [io_in_received, send_frame_calls, nil_drops, last_send_ms]
  → Transport.Stream.send_frame
  → [command channel]                     [cmd_queue_depth]
  → driver loop (driver/mod.rs)           [loop_iterations, last_loop_ms]
  → per-stream write buffer → socket      [write_buffer_len, write_buffer_dropped, frames_written, last_write_ms]
Inbound mirror: driver read → StreamChannel({:flz_data}) [flz_data_received] + [frames_read, last_read_ms]
```

## Signals

The counters are passive and the classifier is pure — nothing here changes data-plane behaviour.

**NIF driver self-report** (`Flezha.Transport.Native.driver_stats/1`,
`flz_nif/src/driver_stats.rs`): per server — `loop_iterations` (bumped once per loop
iteration), `last_loop_ms` and `now_ms` (driver-elapsed ms), `cmd_queue_depth`, and per stream
`{write_buffer_len, write_buffer_dropped, frames_written, frames_read, last_write_ms,
last_read_ms}`. The loop counters are lock-free `AtomicU64`; the per-stream snapshot is rebuilt
under a `Mutex` once per loop iteration, never per frame. A frozen `loop_iterations` with a stale
`last_loop_ms` unambiguously means the thread is wedged. The UDS and the QUIC driver both produce
this map, in one shape: each keeps its own per-stream write buffer and send gate before the bytes
reach the socket, so every field has one answer on either transport, and the classifier needs no
per-transport branch. The `native` in-node transport has no driver, so
`Transport.Server.get_stats(:native)` carries no `:driver` key and its data-plane evidence is the
BEAM counters alone. `StreamAcceptor` registers each per-runner data-stream server
(`LivenessCounters.register_driver/3`) so the classifier polls the drivers carrying operative traffic.

**BEAM counters** (`Flezha.Transport.LivenessCounters`): always-on `:atomics`-backed `:counters`, one
array per channel (`{runner_id, name}`) and per operative, registered in a public ETS table and pruned
of dead owners on `snapshot_all/0`. Channel slots: `io_in_received`, `send_frame_calls`, `nil_drops`,
`flz_data_received`, `last_send_ms`, `hb_count`, `hb_dispatches`, `hb_credit_parked`, `hb_inflight`,
`hb_inflight_oldest_ms`, `hb_blocked_parks`, `hb_drops_newest`, `hb_drops_oldest`, `hb_pending_pushes`,
`hb_pending_push_oldest_ms`, `hb_restore_discarded_inputs`, `hb_timers_armed`, `hb_timers_fired`,
`credit_active`, `credit_ledger_level`, `credit_overrun_total`, `stale_writer_refused`,
`send_gate_held`. Operative slots: `io_in_received`, `forwarded`, `filtered_nil`, `parked_drops`.

**Runner dispatch self-report** (all three SDKs): every `data_heartbeat` frame on a secondary stream
carries `dispatches` (process-global payloads dispatched through the poll engine), `blocked_parks`,
`drops_newest`, `drops_oldest`, `inflight`, `inflight_oldest_ms`, `pending_pushes`,
`pending_push_oldest_ms`, `credit_parked`, `timers_armed`, `timers_fired` and, from `flz_core`,
`restore_discarded_inputs`. The heartbeat thread
outlives a wedged dispatch pool, so without the counter a runner can look alive while processing
nothing. The secondary `StreamChannel` mirrors each into its `hb_*` slot and bumps `hb_count`; the
per-stream `output_stall` heartbeat check runs only on secondary channels — primary streams are
send-only.

`blocked_parks` counts every `DropNothing` admission that found the mailbox full, not contiguous
episodes — the same choice `credit_parked` makes on the sender side, and for the same reason: a
runner held in one long episode would show a frozen dispatch count beside a frozen block count,
byte for byte the picture a wedged runner shows. That is exactly the pair `runner_pool_wedged`
reads, so the two are only separable while the block counter keeps moving. It is the **entry
path** that parks a thread for those iterations; a producing Entity does not park at all — its
push is held and it is handed no further work — and each attempt on that path counts here too,
so the counter keeps moving under either mechanism.

`pending_pushes` and `pending_push_oldest_ms` are the deferred-push half of that picture:
Entities currently holding an undelivered inter-procedure output, and the age of the oldest. A
gated producer holds no handout, so `hb_inflight` is blind to it — under the parking mechanism it
was at least visible as a growing handout age. The two drop counters are the only way an operator
can see what a shedding operative discarded; all of these were written by the kernel and read by
nothing before this.

**Metric emission** (`Observability.OtpBackend.TransportSource`, ~1 Hz): driver stats per transport
(`:driver_loop_iterations`, `:driver_loop_age_ms`, `:driver_cmd_queue_depth`, `:driver_write_buffer_bytes`,
`:driver_streams`) and the BEAM counters as flat global aggregates (`:channel_*`, `:operative_*`). No
per-runner or per-stream labels — per-entity localisation is the classifier's job.

## Send-buffer cap

The per-stream send buffer is bounded by `max_write_buffer_bytes`
(`flz_transport/src/transport/config.rs`) so a slow or non-reading runner cannot grow controller memory
without limit, and the bound is reached through a **send gate** rather than by discarding
(`flz_transport/src/transport/send_gate.rs`). A sender counts its own bytes into the gate as it queues
them and raises the congestion signal itself once queued plus buffered crosses the high-water mark — the
half it would otherwise be racing is the half it writes — and the driver lowers the signal and wakes the
sender once the buffered bytes fall below the low-water mark. `Flezha.Runner.StreamChannel.Gate` holds
a refused frame for `:send_gate_hold_ms` and offers it again on the drain; running that budget out
answers `{:error, :send_buffer_full}` to the caller **with the frame still unsent**, so congestion is
reported as the caller's decision and never as a silent loss. The driver never blocks its command loop
on a slow peer, and both server drivers keep their own send buffer and their own gate.

`would_overflow_write_buffer/3` is the last-resort ceiling underneath that gate, and a sender honouring
the gate never reaches it. An empty buffer always accepts, so a single large frame on an idle stream is
never dropped. When the ceiling does refuse, the refusal is answered to its caller rather than hidden
under an `:ok`, and the per-stream `write_buffer_dropped` counter moves. That counter is an **invariant,
not a metric**: nothing is discarded for congestion, so any increment is transport data loss and a
defect to be surfaced — the stall classifier reads it as `:send_buffer_dropping`.

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
| `:runner_pool_wedged` | per runner: `send_frame_calls` and `hb_count` advancing, self-reported `hb_dispatches` > 0 and frozen, **and `hb_blocked_parks` flat** — a frozen dispatch count with the block counter climbing is backpressure, not a wedge |
| `:channel_not_sending` | channel `io_in_received` advancing, `send_frame_calls` and `nil_drops` flat |
| `:operative_not_forwarding` | operative `io_in_received` advancing, `forwarded` flat, the gap not explained by `filtered_nil` |
| `:producer_push_stalled` | per runner: `hb_pending_pushes` > 0 and `hb_pending_push_oldest_ms` > `entity_stall_ms` — an Entity withheld work because a consumer will not take its output. **Declares no scope**: the thing that is behind is the CONSUMER, and rebinding the producer would move the congestion rather than clear it, so this is a report and mints no op. The runner names both ends in its own `flezha::mailbox` log; the heartbeat carries counters, not identities |

Each layer declares its **scope** (`StallClassifier.layer_scope/1`) beside the check that mints it.
The scope is what gives a verdict exactly one consumer, and it says so through the SUBJECT KIND of
the health record the convergence loop mints from it (`Convergence.Verdict`): `:fabric` becomes a
`driver` record, which `/healthz` reads and the Diff plans nothing for; `:runner`, `:connection` and
`:operative` become `runner`, `channel` and `operative` records, which the Diff reads and which reach
`/healthz` only as `degraded`. The verdict is judged and passed into `Diff.compute/3` in-process on
the tick that computed it; the Diff resolves the scope against the same probed reality every other invariant
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
runs at tick cadence. `Convergence.Loop.Progress.progress_markers/0` builds the markers from
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
pid; read `/proc/<pid>/task/*/comm` for threads named `flz-uds-driver` or `flz-quic-driver` and check
`/proc/<pid>/task/<tid>/stat` — a thread in `D` (uninterruptible sleep) or with a frozen `utime` is a
wedged driver, matching a frozen `loop_iterations`. No controller code path is involved, so this works
even if the NIF itself is what wedged.

## See also

- [`convergence_loop.md`](convergence_loop.md) — the consumer of every verdict here
- [`runner_identity.md`](runner_identity.md) — the binding swap a liveness-driven close leads to
- [`nif_transport.md`](nif_transport.md) — the driver threads being instrumented
- [`../sdk/entry_input_idle.md`](../sdk/entry_input_idle.md) — the application-facing idle signal
- [`../operations/observability.md`](../operations/observability.md) — where the `:flezha_transport` metrics land
