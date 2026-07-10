# Data & state durability: what survives, what doesn't, and how to avoid loss

This is the single reference for **where flezha can lose operative state or output, where
it cannot, and how to operate so you never lose data on a planned action.** Read it
with `docs/resilience/process_hub_recovery_model.md` (which covers *operative placement* recovery; this
doc covers *operative state + output* durability).

## The two things that can be "lost"

1. **Operative state** — an operative's accumulated in-memory state (e.g. a running total). The
   authoritative live copy lives on the operative's **one bound executor**; it is backed up
   **asynchronously** to a per-Segment **WAL on the controller** (the executor streams
   `segment_state_change` frames; the controller batches the fsync, ~5 ms).
2. **Output** — what an operative emits on `on_output`. Output is routed **through the
   controller** (the operative sends an output frame; the controller runs the `on_output`
   plugin chain → Kafka/HTTP/operative/etc.). Output is **at-least-once**.

The core durability invariant we provide: **on a *graceful* stop, no state is lost; on a
*hard kill*, a small best-effort window can lose the most-recent un-persisted state.**

A third durable artifact is the operative's **plugin manifest** (the `on_output`/feed
plugin chain): it lives in the operative's registry cspec and is written cluster-wide
(`propagate: true`), so an operative restarted from any node re-attaches its full plugin
set and the convergence loop re-adds anything missing. See
`process_hub_recovery_model.md` "Registry writes must be cluster-wide".

## Scenario matrix

| Scenario | Operative state | Output | Why / how |
|---|---|---|---|
| **Graceful executor stop** (SIGTERM / rolling deploy / `disconnect()` / `Drop`) | **No loss** | At-least-once | SDK `shutdown.rs` serializes each operative's *final* state, ships it to the controller, and waits for `ExecutorShutdownAck` before exiting. |
| **Single executor SIGKILL** (hard) under load | **Brief best-effort loss** | At-least-once | No graceful drain; the operative rebinds to a peer/itself and restores from the controller WAL, which may lag the last few changes emitted under load. |
| **All executors SIGKILL'd at once** under load | **Brief best-effort loss** | At-least-once | Same WAL-restore path, with no live copy anywhere — reliably exposes the lag (this is what `stateful-resume` provokes). |
| **Executor OOM / container crash / node death** | **Brief best-effort loss** | At-least-once | Same as SIGKILL — no drain runs. |
| **Controller graceful restart / rolling deploy with a live peer** | **No loss** | At-least-once | The new instance syncs the registry from the surviving peer; executors stay connected. |
| **Controller plain restart, sole instance** (marker present) | State in WAL preserved; **operative *list* briefly unknown (~6 s)** | At-least-once | Skip-replay boot wipes local DETS by design; the registry is rebuilt from executors re-announcing. `/healthz` is `healthy` (vacuously) during the gap — safe for kubelet, but not "recovering". Prefer recovery mode (below). |
| **Controller recovery-mode restart, sole instance** (marker removed / `flz cluster prepare-recovery`) | **No loss** | At-least-once | Replays the registry from DETS; `/healthz` honestly reports `recovering` until rebound. The correct way to restart the *only* instance. |
| **Whole cluster down → cold start** | Recovered from DETS | n/a | The ONE case DETS replay is for: no peer survives, disk is the only record. |
| **Node rejoins a live cluster** | Synced from peers; local DETS discarded | n/a | Correct — local DETS is stale; peers are authoritative. |
| **Kafka/broker down, executors alive** | No loss | Buffered, resumes | Producer reconnects (see kafka producer recovery); offset resumes when the broker returns. |
| **Sustained overload** (state changes outpace fsync — the 06-21 OOM shape) | **No loss** (credit-speaking SDK) | At-least-once | Credit-grant flow control: the sender's byte window (default 16 MiB) is minted back only on WAL fsync, so an exhausted window *parks* the SDK's flush path instead of growing controller memory — park, never drop. Shedding is unreachable for a conformant sender; any shed/overrun on a credit session emits `[:flezha, :state, :credit_anomaly]`. Pre-credit SDKs keep the legacy status-flag + shed/resync path. See [`state_persistence.md`](state_persistence.md) "Flow control". |
| **Bounded operative executor SIGKILL** (durable, the default) | **Brief best-effort loss** (async WAL lag) | At-least-once | Respawns on a new executor, restores committed Segment state via the restore-before-run gate, and **re-runs the operation chain** (Model A — completed ops are idempotent no-ops). See [`bounded_operative.md`](bounded_operative.md). |
| **Bounded operative executor SIGKILL** (persistence opted out) | **Full — re-runs from the top** | At-least-once | No WAL to restore from; the operative re-runs its `run`/chain from `args`. Idempotent by author contract, or don't opt out. |
| **Bounded operative SIGKILL mid-compensation** (durable) | Compensation resumes | n/a | Compensation-intent / -done markers ride the restore; the respawn finishes the outstanding undo hooks and **skips** the already-done ones (no forward re-run). |

## Why output can briefly get ahead of state (the hard-kill gap)

The executor streams **output frames** and **state-change frames** to the controller
independently, and the controller emits output before the matching state is durably
fsync'd. So on a hard kill, output (e.g. total=135 in Kafka) can outlive the durable
state (WAL=128), and the restored operative comes back slightly stale. We deliberately do
**not** close this with per-record acking (it would add a network round-trip to every
message and cripple throughput). Instead:

- **Planned stops drain gracefully → zero loss** (the SDK ships final state with ack).
- **Hard kills accept the small window** — rare, and full protection requires the
  expensive checkpoint/2-phase-commit machinery (this is what Apache Flink's end-to-end
  exactly-once buys, at a real cost). flezha is "durable state + at-least-once output."

**FsyncBatcher and the lag window.** The WAL writer does not fsync after every record.
`State.FsyncBatcher` coalesces dirty-mark casts from all per-segment `WalWriter`
processes and issues one `:file.datasync/1` call per writer per cadence tick (default
5 ms). On a hard kill, records written within the last ~5 ms may not have been
fsynced and can be lost. This is the mechanical source of the "async WAL lag" in the
scenario matrix above. The fsync cadence is configurable via
`:flezha, :agent_fsync_interval_ms`; lowering it reduces the lag window at the cost
of higher I/O pressure. Compare: the audit-log writer has a separate `:per_record` vs
`:batched` fsync mode (`:per_record` is its default); WAL state persistence always
uses the batched FsyncBatcher — per-record WAL fsync is not supported.

## How to avoid loss — operator playbook

- **Deploy / upgrade executors with a graceful stop, never a kill.** Send SIGTERM (or
  `flz`-driven `disconnect`) and set the orchestrator grace period
  (`terminationGracePeriodSeconds` / `stop_grace_period`) **longer than the drain**
  (serialize + ack; default ack budget 2 s/operative, serialize cap 10 s). Roll one node at
  a time so a peer is always live.
- **Restart the controller correctly:**
  - Multi-node / rolling: bring the new instance up *before* killing the old → peer sync,
    zero loss.
  - Sole instance that must keep its operatives: use **recovery mode** (remove the
    `cluster.healthy` marker / `flz cluster prepare-recovery`) → replays from DETS.
  - Plain restart of a sole instance is "accept the operative-list rebuild window" — only do
    it when that's acceptable.
- **Make sinks idempotent where exactly-once matters.** Output is at-least-once and can
  be re-emitted after a rebind/restore — use Kafka keys/transactions, HTTP idempotency
  keys, or DB upserts downstream.
- **Never treat DETS as the source of truth** in code or ops — it is a cold-start backup
  only (see `docs/resilience/process_hub_recovery_model.md`).
- **Bounded operatives are durable + at-least-once by default.** A crash re-runs the
  operation chain over restored state, so author each operation (and compensation)
  **idempotently**, or **opt the class out of persistence** for a fire-and-forget job that
  must re-run from a clean slate. See [`bounded_operative.md`](bounded_operative.md).

## Common pitfalls

- `docker kill` / `kill -9` / `terminationGracePeriodSeconds` too short on an executor →
  drain is cut off → best-effort loss. Use graceful stop + adequate grace.
- **The executor binary must receive SIGTERM directly.** Run it as PID 1 (exec-form
  `CMD`/entrypoint, or `setpriv`/`gosu` to drop privileges by `exec`). A wrapper that
  forks the binary and force-kills it on SIGTERM — notably `su` ("Session terminated,
  killing shell") — terminates the SDK before its graceful drain ships final state, so
  a "graceful" stop silently degrades to a hard kill. The prod `market-executor` is
  correct (exec-form `CMD ["./market_analyzer"]`, `runAsUser: 1000`, no `su`).
- Plain-restarting the sole controller and expecting the operative list back instantly /
  `/healthz` to read `recovering` → use recovery mode instead.
- Assuming output is exactly-once → it is at-least-once; dedup downstream.
- Forcing a node to recover from its own DETS while peers are alive → split-brain /
  stale-state crash (see the recovery-model doc).
