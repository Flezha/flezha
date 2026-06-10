# Data-Plane Liveness Instrumentation

Detection-only instrumentation that localizes a silent data-plane stall to the
exact layer that wedged. **It changes no data-plane behaviour and triggers no
remediation** — it is pure observability layered over the existing
`Flezha.Observability` foundation and the convergence Loop.

## Why this exists

On 2026-06-07 22:14 an executor's data plane went silent for ~21 h: inbound
`io_in` stopped reaching the executor *and* the executor produced no output —
both directions silent, sockets open, no error, no OOM, no crash, BEAM memory
flat. It recovered only when the relay migrated and the streams were rebuilt.
Forensics cleared the relay GenServer, Kafka `on_output`, the WAL writers, and
the controller NIF writes, but could not separate three suspects from read-only
post-recovery state: the BEAM `StreamChannel` stopped calling `send_frame`, vs.
the NIF `flz-uds-driver` thread stalled, vs. the executor stopped consuming. The
offending threads were gone by investigation time. This instrumentation makes
the **next** occurrence self-localize.

## The instrumented path

```
Kraken WS plugin
  → Relay.Overwatch.handle_cast/handle_call({:io_in})   [io_in_received, forwarded, filtered_nil]
  → StreamChannel.handle_cast/handle_call({:io_in})      [io_in_received, send_frame_calls, nil_drops, last_send_ms]
  → Native.send_frame
  → [crossbeam unbounded command channel]                [cmd_queue_depth]
  → flz-uds-driver loop (uds_driver.rs)                  [loop_iterations, last_loop_ms]
  → per-stream write_buffer → UnixStream write           [write_buffer_len, last_write_ms, frames_written]
Inbound mirror: driver read → StreamChannel({:flz_data}) [flz_data_received] + [last_read_ms, frames_read]
```

## Signals

**NIF driver self-report** (`driver_stats/1`, `flz_nif`): per UDS server —
`loop_iterations` (heartbeat, bumped once per loop iteration), `last_loop_ms`
(driver-elapsed ms at the last iteration), `now_ms`, `cmd_queue_depth`
(`cmd_rx.len()`), and per stream `{write_buffer_len, frames_written, frames_read,
last_write_ms, last_read_ms}`. The loop counters are lock-free `AtomicU64`; the
per-stream snapshot is rebuilt under a `Mutex` once per loop iteration — never on
the per-frame path. A frozen `loop_iterations` with a stale `last_loop_ms`
unambiguously means the thread is wedged. QUIC has no equivalent yet —
`driver_stats/1` returns `:undefined`.

**BEAM counters** (`Flezha.Transport.LivenessCounters`): always-on
`:atomics`-backed `:counters`, one array per channel and per relay, registered in
a public ETS table. Channels bump `io_in_received`, `send_frame_calls`,
`nil_drops`, `flz_data_received`, `hb_count` and set `last_send_ms`,
`hb_dispatches`; relays bump `io_in_received`, `forwarded`, `filtered_nil`.
Sub-100 ns increments, no hot-path locks or allocations. Dead-owner entries are
pruned on the next `snapshot_all/0`. `StreamAcceptor` additionally registers its
per-executor data-stream NIF server refs (`register_driver/3`) so the classifier
polls the drivers that actually carry relay traffic, not just the control
channel's.

**Executor dispatch self-report** (Rust SDK): every `data_heartbeat` frame
carries `dispatches` (process-global count of payloads dispatched through
`drain_partition`) and `blocked_parks` (Block-policy full-mailbox episodes) in
its metadata. The heartbeat thread outlives a wedged dispatch pool, so without
this counter an executor can look alive while processing nothing — exactly the
2026-06-10 freeze. The secondary `StreamChannel` mirrors the values into
`hb_count` / `hb_dispatches`.

The per-stream `output_stall` heartbeat check runs only on SECONDARY channels —
primary streams are send-only (the executor never writes them), so the check
there produced a permanent false positive (one warning every 10 s).

**Metric emission** (`Observability.OtpBackend.TransportSource`, ~1 Hz): the
driver stats are surfaced per transport (`:driver_loop_iterations`,
`:driver_loop_age_ms`, `:driver_cmd_queue_depth`, `:driver_write_buffer_bytes`,
`:driver_streams`) and the BEAM counters as flat global aggregates
(`:channel_*`, `:relay_*`). Cardinality stays bounded — no per-executor or
per-stream label values — because the per-entity localization is the
classifier's job, not the metrics'.

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
precondition (a count that must be *advancing* between ticks), so an idle relay
never reads as a stall:

| Verdict | Signature |
|---------|-----------|
| `:driver_loop_frozen` | `loop_iterations` unchanged **and** `now_ms − last_loop_ms` > `driver_freeze_ms` (5 s) |
| `:nif_queue_backing_up` | `cmd_queue_depth` high (> 256) and rising, no stream's `frames_written` advancing |
| `:socket_write_blocked` | a stream's `write_buffer_len` rising, `frames_written` advancing, last write stale (> 30 s) |
| `:executor_not_consuming` | `frames_written` advancing but `frames_read` flat, last read stale (> 30 s) |
| `:executor_pool_wedged` | per executor: `send_frame_calls` advancing, `hb_count` advancing, self-reported `hb_dispatches` > 0 and frozen — alive but processing nothing |
| `:channel_not_sending` | channel `io_in_received` advancing, `send_frame_calls` flat, `nil_drops` flat (stream bound) |
| `:relay_not_forwarding` | relay `io_in_received` advancing, `forwarded` flat, the gap not explained by `filtered_nil` |

## Configuration

`:observability, :data_plane_liveness` (default `true`, env
`FLEZHA_DATA_PLANE_LIVENESS`). The atomic counters are always-on (cost is
negligible); this flag gates the ~1 Hz driver poll + per-tick classifier. The
classifier additionally runs only when the convergence Loop runs
(`FLEZHA_USE_CONVERGENCE_HEALTH`, default on) and the source only when
`:otp_backend_enabled` is on.

## Zero-code operator fallback

If the BEAM is up but unresponsive, sample the OS threads directly:
`bin/flzvm pid` gives the BEAM pid, then read
`/proc/<pid>/task/*/comm` for threads named `flz-uds-driver` and check
`/proc/<pid>/task/<tid>/stat` — a thread stuck in `D` (uninterruptible sleep) or
with a frozen `utime` indicates a wedged driver, matching a frozen
`loop_iterations`. This needs no controller code path and works even if the NIF
itself is the thing that wedged.
