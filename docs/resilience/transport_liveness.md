# Transport Liveness

Two complementary liveness signals on the controller↔runner transport: a
**control-plane session watchdog** (is the runner session alive at all?) and
**data-plane stall instrumentation** (which layer wedged when output went
silent?). They cover different wedge classes — see the table below.

## Control-plane session liveness (unify-executor-liveness-and-identity)

`Flezha.Runner.ControlChannel` treats the inbound control-frame stream as the
single liveness signal for the runner session. It stamps a monotonic
`last_seen` on **every** inbound control frame — including the SDK's 15 s
keepalive ping (formerly echoed and discarded) — and runs one jittered watchdog
(`:liveness_tick`). Sustained silence past `control_liveness_window_ms`
(default 45 s = 3× the keepalive; env `FLEZHA_CONTROL_LIVENESS_WINDOW_MS`, check
interval `FLEZHA_CONTROL_LIVENESS_INTERVAL_MS`) is treated as a **connection
close**: it routes through the existing `handle_connection_closed/1` →
`{:runner_disconnected}` path — the same path a real socket close uses, NOT a
parallel eviction path — and emits `[:flezha, :runner, :liveness_timeout]`.

This closes the **UDS wedged-but-connected blind spot**: UDS surfaces only a hard
socket EOF, never a half-open peer, so before this watchdog a wedged-but-
connected runner was invisible to the control plane on the default transport.
QUIC sets an explicit `max_idle_timeout` equal to the same window (`flz_transport`
`quic.rs` + `flz_nif` `quic_driver.rs`, via `liveness::control_idle_timeout()` /
`FLEZHA_CONTROL_IDLE_TIMEOUT_MS`, default 45 s; keep-alive stays 5 s) instead of
inheriting the quinn default — so **both transports declare death in the same
window by construction**. When tuning, pair the two window knobs
(`FLEZHA_CONTROL_LIVENESS_WINDOW_MS` for the UDS watchdog,
`FLEZHA_CONTROL_IDLE_TIMEOUT_MS` for the QUIC idle + the SDK-side idle watchdog).

**Calibrated, not trigger-happy.** The window is multiple missed pings, jittered,
and the action is connection-close (a cheap reconnect via the binding swap — see
[`runner_identity.md`](runner_identity.md)), not a redistribute sweep. The
SDK keepalive runs on an independent timer thread, so a *busy* runner keeps
proving liveness; a single missed ping or a brief GC/host-contention pause cannot
trip it.

**Retired mechanisms** (coverage subsumed before deletion):

| Retired / changed | What it caught | Subsumed by |
|---|---|---|
| `ControlUnit.synthesize_disconnect` (90 s stuck-reply → disconnect) | a wedged runner *with a control request in flight* | the session watchdog (fires on silence in ≤45 s, request or not). `ControlUnit.fail_stuck_refs` stays, purely to unblock stuck callers |
| Controller echo-and-discard of the keepalive ping | nothing controller-side | the watchdog now *consumes* the same ping as `last_seen` |
| QUIC implicit idle (quinn default) | a wedged QUIC peer | explicit `max_idle_timeout` aligned to the window |

**What it catches — and what it does not.** A control-channel watchdog detects a
process / control-channel wedge (the runner stops sending entirely). It does
**not** detect a dispatch-pool wedge where the work pool is frozen but the
keepalive timer thread keeps pinging (the 06-10 / 06-19 incidents) — that class
is covered by the *data-plane* `output_stall` + the runner's
`:runner_pool_wedged` self-report, below. Both signals are needed:

| Wedge class | Caught by |
| --- | --- |
| Process / control-channel wedge (the UDS blind spot) | the control-plane `last_seen` watchdog (this section) |
| Dispatch-pool wedge, keepalive thread still ticking | data-plane `output_stall` + `:runner_pool_wedged` (below) |

## Data-plane stall localization

Detection-only instrumentation that localizes a silent data-plane stall to the
exact layer that wedged. **It changes no data-plane behaviour and triggers no
remediation** — it is pure observability layered over the existing
`Flezha.Observability` foundation and the convergence Loop.

### Why this exists

On 2026-06-07 22:14 a runner's data plane went silent for ~21 h: inbound
`io_in` stopped reaching the runner *and* the runner produced no output —
both directions silent, sockets open, no error, no OOM, no crash, BEAM memory
flat. It recovered only when the operative migrated and the streams were rebuilt.
Forensics cleared the operative GenServer, Kafka `on_output`, the WAL writers, and
the controller NIF writes, but could not separate three suspects from read-only
post-recovery state: the BEAM `StreamChannel` stopped calling `send_frame`, vs.
the NIF `flz-uds-driver` thread stalled, vs. the runner stopped consuming. The
offending threads were gone by investigation time. This instrumentation makes
the **next** occurrence self-localize.

## The instrumented path

```
Kraken WS plugin
  → Operative.Overwatch.handle_cast/handle_call({:io_in})   [io_in_received, forwarded, filtered_nil]
  → StreamChannel.handle_cast/handle_call({:io_in})      [io_in_received, send_frame_calls, nil_drops, last_send_ms]
  → Native.send_frame
  → [crossbeam unbounded command channel]                [cmd_queue_depth]
  → flz-uds-driver loop (uds_driver.rs)                  [loop_iterations, last_loop_ms]
  → per-stream write_buffer (capped) → UnixStream write   [write_buffer_len, write_buffer_dropped, last_write_ms, frames_written]
Inbound mirror: driver read → StreamChannel({:flz_data}) [flz_data_received] + [last_read_ms, frames_read]
```

## Signals

**NIF driver self-report** (`driver_stats/1`, `flz_nif`): per UDS server —
`loop_iterations` (heartbeat, bumped once per loop iteration), `last_loop_ms`
(driver-elapsed ms at the last iteration), `now_ms`, `cmd_queue_depth`
(`cmd_rx.len()`), and per stream `{write_buffer_len, write_buffer_dropped,
frames_written, frames_read, last_write_ms, last_read_ms}`. The loop counters are
lock-free `AtomicU64`; the
per-stream snapshot is rebuilt under a `Mutex` once per loop iteration — never on
the per-frame path. A frozen `loop_iterations` with a stale `last_loop_ms`
unambiguously means the thread is wedged. QUIC has no equivalent yet —
`driver_stats/1` returns `:undefined`.

**BEAM counters** (`Flezha.Transport.LivenessCounters`): always-on
`:atomics`-backed `:counters`, one array per channel and per operative, registered in
a public ETS table. Channels bump `io_in_received`, `send_frame_calls`,
`nil_drops`, `flz_data_received`, `hb_count` and set `last_send_ms`,
`hb_dispatches`; operatives bump `io_in_received`, `forwarded`, `filtered_nil`.
Sub-100 ns increments, no hot-path locks or allocations. Dead-owner entries are
pruned on the next `snapshot_all/0`. `StreamAcceptor` additionally registers its
per-runner data-stream NIF server refs (`register_driver/3`) so the classifier
polls the drivers that actually carry operative traffic, not just the control
channel's.

**Runner dispatch self-report** (Rust SDK): every `data_heartbeat` frame
carries `dispatches` (process-global count of payloads dispatched through
`drain_segment`) and `blocked_parks` (Block-policy full-mailbox episodes) in
its metadata. The heartbeat thread outlives a wedged dispatch pool, so without
this counter a runner can look alive while processing nothing — exactly the
2026-06-10 freeze. The secondary `StreamChannel` mirrors the values into
`hb_count` / `hb_dispatches`.

The per-stream `output_stall` heartbeat check runs only on SECONDARY channels —
primary streams are send-only (the runner never writes them), so the check
there produced a permanent false positive (one warning every 10 s).

**Metric emission** (`Observability.OtpBackend.TransportSource`, ~1 Hz): the
driver stats are surfaced per transport (`:driver_loop_iterations`,
`:driver_loop_age_ms`, `:driver_cmd_queue_depth`, `:driver_write_buffer_bytes`,
`:driver_streams`) and the BEAM counters as flat global aggregates
(`:channel_*`, `:operative_*`). Cardinality stays bounded — no per-runner or
per-stream label values — because the per-entity localization is the
classifier's job, not the metrics'.

## Send-buffer cap

The per-stream send buffer is bounded so a slow or non-reading runner cannot
grow controller memory without limit (a contributor to the 2026-06-19 OOM). When
the socket returns `WouldBlock` and the buffered bytes are already at
`max_write_buffer_bytes` (default 8 MiB, env `FLEZHA_MAX_WRITE_BUFFER_BYTES`),
`enqueue_write` drops the newest frame instead of appending it — a `DropNewest`
policy mirroring the SDK bounded mailbox — and bumps the per-stream
`write_buffer_dropped` counter (surfaced in `driver_stats/1`). An empty buffer
always accepts, so a single large frame on an idle stream is never dropped. The
data stream is at-least-once with downstream dedup, so a dropped frame is
recoverable; unbounded growth → OOM is strictly worse. The driver never blocks
its command loop on a slow peer. The QUIC driver
(`flz_transport/src/transport/quic.rs`) mirrors the same cap + drop counter on
its `write_buf`.

## The stall classifier

`Flezha.Convergence.StallClassifier.classify/3` is a **pure** function over the
current and previous liveness snapshot. The convergence Loop builds a snapshot
each tick (control driver `get_stats` + registered data-stream `driver_stats` +
`LivenessCounters.snapshot_all`), classifies it, and on verdict change logs a
`[convergence] data-plane stall localized: layer=…` warning (and an info line
on clear) plus one `[:flezha, :data_plane, :stall_localized]` event (carrying
`%{layer: …}`). The classifier feeds **no** heal
decision — the existing `:output_stall → rebind` path is untouched — and any
error in the step is swallowed so instrumentation can never break convergence.

Verdicts, evaluated top-down (the deepest wedged layer wins, since it explains
every layer above it). Each has a built-in "is it actually being fed?"
precondition (a count that must be *advancing* between ticks), so an idle operative
never reads as a stall:

| Verdict | Signature |
|---------|-----------|
| `:driver_loop_frozen` | `loop_iterations` unchanged **and** `now_ms − last_loop_ms` > `driver_freeze_ms` (5 s) |
| `:nif_queue_backing_up` | `cmd_queue_depth` high (> 256) and rising, no stream's `frames_written` advancing |
| `:socket_write_blocked` | a stream's `write_buffer_len` rising, `frames_written` advancing, last write stale (> 30 s) |
| `:runner_not_consuming` | `frames_written` advancing but `frames_read` flat, last read stale (> 30 s) |
| `:runner_pool_wedged` | per runner: `send_frame_calls` advancing, `hb_count` advancing, self-reported `hb_dispatches` > 0 and frozen — alive but processing nothing |
| `:channel_not_sending` | channel `io_in_received` advancing, `send_frame_calls` flat, `nil_drops` flat (stream bound) |
| `:operative_not_forwarding` | operative `io_in_received` advancing, `forwarded` flat, the gap not explained by `filtered_nil` |

## Producing-liveness (progress markers)

The same module carries `StallClassifier.classify_producing/3` — the
per-operative producing-liveness classifier the Loop runs at tick cadence over
durable progress markers. `Flezha.Convergence.Loop.progress_markers/0` builds
them from `Flezha.State.Watchdog.segment_statuses/0` (WAL
`last_change_seen_at` / `last_advance_at`, plus the `out_pending_seq` /
`outbox_flushed_seq` outbox cursors) joined with `Flezha.State.InputLog.cursors/1`
(the log tail and each Segment's durable consumed offset) — one input-log read
per operative per tick, not per Segment. A tier-off operative has no input log,
yields no cursor fields, and degrades to WAL-seq-only.

It shares the deepest-verdict-wins convention and the fed precondition:
**IDLE** (no input, flat markers) counts as producing and stays healthy;
**STUCK** (fed but flat past the grace) degrades `/healthz` with detail
`wal_frozen | watermark_frozen | outbox_flat`.

Each detail carries its own fed precondition, so an idle operative cannot trip
one: `watermark_frozen` requires the **input-log tail to have grown** while the
Segment's consumed offset stood still, and `outbox_flat` requires
**output-bearing records to have accumulated** while the flush cursor stood
still. `out_pending_seq` deliberately counts only output-bearing records —
`outbox_durable_seq` advances on every fsynced record, so reusing it would read a
Segment that persists state but emits nothing as a permanently frozen outbox.

Like the data-plane classifier it is report-only — healing stays on the
`:output_stall → rebind` path. See `docs/resilience/convergence_loop.md`
"Health publishing" for the `/healthz` contract (including the
`intent_mismatch` reason).

## Configuration

`:observability, :data_plane_liveness` (default `true`, env
`FLEZHA_DATA_PLANE_LIVENESS`). The atomic counters are always-on (cost is
negligible); this flag gates the ~1 Hz driver poll + per-tick classifier. The
classifier additionally runs only when the convergence Loop runs
(`FLEZHA_USE_CONVERGENCE_HEALTH`, default on) and the source only when
`:otp_backend_enabled` is on.

`FLEZHA_MAX_WRITE_BUFFER_BYTES` (default `8 * 1024 * 1024`) caps the per-stream
send buffer in both the UDS and QUIC drivers; past it the newest frame is dropped
(`write_buffer_dropped`).

## Zero-code operator fallback

If the BEAM is up but unresponsive, sample the OS threads directly:
`bin/flzvm pid` gives the BEAM pid, then read
`/proc/<pid>/task/*/comm` for threads named `flz-uds-driver` and check
`/proc/<pid>/task/<tid>/stat` — a thread stuck in `D` (uninterruptible sleep) or
with a frozen `utime` indicates a wedged driver, matching a frozen
`loop_iterations`. This needs no controller code path and works even if the NIF
itself is the thing that wedged.
