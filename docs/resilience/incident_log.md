# Production incident log

Running log of flezha_market production data-freeze incidents
(https://192.168.1.49:30443/). One entry per incident, newest first.
Keep entries short: timestamps, observed evidence, verdict. Detail goes
in the linked analysis docs / commits. **Every freeze gets an entry here
the day it happens** — the occurrence table is how we see recurrence.

## Occurrence summary

| Date (UTC) | Duration | Root cause | Status |
|---|---|---|---|
| 2026-06-10 13:46 | ~3.5 h (manual restart) | SDK BlockParker self-deadlock (confirmed live, thread-level evidence) | Fixed: inline drain in `enqueue` + regression test; deployed 2026-06-10 |
| ≤ 2026-06-02, ~5+/day | minutes–hours each | Convergence loop SIGKILLed its own busy StreamChannel (proven) | Fixed `336f960`, deployed |
| 2026-05-09 → 2026-06-10, sporadic | hours each | Unattributed individually; signature consistent with the BlockParker deadlock (possible from 2026-05-09, the day Block-default inter-shard channels reached prod) | Class closed by the 2026-06-10 fix; do not retro-attribute specific incidents |

Detection gaps closed on 2026-06-10 (so the NEXT freeze self-localizes):
executor `dispatches` self-report in heartbeat metadata + `:executor_pool_wedged`
classifier verdict + stall verdicts now LOGGED (were telemetry-only) +
data-stream NIF drivers added to the classifier snapshot + `output_stall`
false positive on primary channels removed.

Format: `## YYYY-MM-DD HH:MM UTC — <one-line verdict>`

---

## 2026-06-10 13:46 UTC — executor SDK self-deadlock: single pool worker parked forever on a full Block-policy mailbox

- **Onset**: last candle `period_start` 13:46:00, last order-book row
  13:46:55 (postgres). Freeze still live at 17:00 when diagnosed.
- **Executor evidence** (`market-executor-947c6`, raspberrypi4):
  - 13:47:25 executor logged `CATASTROPHIC data-flow stop: input absent
    past threshold; process alive but all shards starved` — its only log
    line since boot.
  - Thread `flezha-ws-0` (the ONLY work-stealing pool worker): state S,
    `wchan=futex_wait_queue`, utime frozen across samples → parked
    indefinitely inside a task.
  - Thread `flezha-flusher`: also futex-blocked (waits on the partition
    buffer mutex the worker holds across `dispatch`).
  - Data-reader thread still alive, draining the UDS socket (~103 msg/s)
    and dropping/queueing frames nobody consumes.
- **Controller evidence** (`flezha-daemonset-cth2z`):
  - Websocket plugin healthy (~105 msg/s in), Overwatch forwarding,
    NIF primary driver `frames_written` advancing, `write_buffer_len=0`,
    loop alive → entire controller-side path healthy throughout.
  - `data_heartbeat` still arriving every 10 s → channel `last_seen`
    fresh → no real stall signal; StallClassifier verdict `:ok` during
    the freeze (blind spot: it never sees inside the executor).
  - Zero log lines at onset — the freeze was controller-invisible.
- **Mechanism** (code-confirmed, `sdk/rust`):
  `enqueue` with `OverflowPolicy::Block` parks the producer on
  `BlockParker::wait_one` → `Parker::park()` (no timeout,
  `relay/dispatcher.rs:492-499`, `partition/mod.rs:126-137`). The unpark
  comes only from `drain_partition` popping that mailbox — which can only
  run on the work-stealing pool. Executor pod has `limits.cpu=500m` →
  `available_parallelism()=1` → pool size 1. The single worker, mid-
  dispatch, emitted into a full consumer mailbox and parked itself; the
  drain task that would free it sits in the injector forever. Permanent
  self-deadlock; heartbeats (separate thread) keep the channel green.
- **Noise**: controller logged `output_stall` every 10 s since boot
  (7,570×) for the PRIMARY (send-only) stream channel — a structural
  false positive (primary never receives frames/heartbeats), unrelated
  to the freeze.
- **Verdict confirmation (5 adversarial cycles, 2026-06-10)**: frozen
  worker showed 0 voluntary ctx-switches/10 s with `futex(…, timeout=NULL)`
  vs a healthy control worker's +200/10 s with a timeout pointer; all
  MarketRelay inter-shard channels use the `Block` default; shards hold no
  blocking primitives; `BlockParker::park()` is the only indefinite condvar
  wait reachable from dispatch; Block default landed 2026-05-05 and reached
  prod 2026-05-09 — the onset of this failure class.
- **Fix (same day)**: `enqueue`'s Block arm drains the full mailbox inline
  (`drain_active` single-active-drainer gate in `drain_partition`) and any
  residual park is bounded (50 ms) with a >5 s warning. Regression test
  `block_policy_emit_on_single_worker_pool_does_not_deadlock` reproduces
  the exact scenario. Note: the Java SDK's bounded mailbox has the same
  Block semantics and needs the parity fix (follow-up; prod runs Rust).

## ~2026-06-10 and earlier — ~30–50 historical freezes, same user-visible shape

- Same symptom (candles + order book stop after hours/days of uptime;
  restart heals). At least one distinct root cause was proven and fixed
  earlier (convergence loop SIGKILLing its own busy StreamChannel,
  fixed 2026-06-02, commit `336f960`). The 2026-06-10 deadlock above is
  consistent with the "executor alive but silent, controller all green"
  signature of most unexplained recurrences, but only incidents
  diagnosed live count as confirmed. Prior incidents lacked the thread-
  level evidence; do not retroactively attribute them.
