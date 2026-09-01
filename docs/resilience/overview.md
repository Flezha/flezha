# Resilience Overview

A map of every mechanism in Flezha that prevents data loss, detects failure and restores a
healthy system, for operators and contributors who need to know **which guarantee lives
where**. Each entry says what the mechanism guarantees and links to the document that owns
it; the model they compose into is in [`../architecture/`](../architecture/README.md).

## Detection and self-healing

**Convergence loop** — `lib/flezha/convergence/`. The controller's single self-healer runs
`Intent → Probe → Diff → Planner → Heal` every 5 s and immediately on a declared event, through
eight idempotent primitives (`rebind_operative`, `replace_operative`, `resync_runner`,
`start_plugin`, `stop_plugin`, `restart_plugin`, `evict_runner`, `restart_runner`), at most 20
procs per tick, each `{primitive, target}` backed off 5 s → 30 s → 60 s → 300 s. A rebind needs
two consecutive misbound ticks, a failed read is `:unknown` rather than "gone", and an uncaught
error is a skipped tick, never a crashed loop.
Owner: [`../internals/convergence_loop.md`](../internals/convergence_loop.md); model:
[`../architecture/kernel.md` "Health and liveness"](../architecture/kernel.md#health-and-liveness).

**Per-operative Overwatch** — `lib/flezha/operative/`. Does the bind work the loop asks for and
retries on failure with jittered backoff 200 ms → 500 ms → 1 s → 2 s → 5 s → 10 s. The loop
calls Overwatch; Overwatch never calls back.
Owner: [`../internals/convergence_loop.md`](../internals/convergence_loop.md).

**Plugin liveness** — `lib/flezha/plugin/extension/table.ex`,
`lib/flezha/convergence/{probe,diff}.ex`. A plugin's lifecycle PHASE, not its subscription, is
what "running" means: a plugin whose `activate/1` failed stays subscribed in `:prepared`, so the
Probe reports it as not running, the Diff lists it under `missing_plugins`, and the existing
`start_plugin` primitive re-activates it under the standard backoff until upstream returns. The
`extra_plugins` check reads the full SUBSCRIBED set instead, so an undeclared plugin is torn
down whatever its phase. Bringing a plugin up is idempotent — a plugin already on the table is
re-activated rather than refused as a duplicate — which is what lets the heal reach a failed
`activate/1` at all. A heal that did not reach `:running` reports `:error`, so the ladder walks
instead of the loop recording a repair it did not achieve.

Three fail-open rules compose here and each is deliberate: `:prepared` is healthy (it holds no
upstream resources), an uncertain probe is healthy (the loop must never escalate on a probe it
could not finish), and an idle operative is not stalled. A probe that cannot be completed is
reported upward as `{:fail_open, reason}` and emitted once, by the extension table, as
`[:flezha, :plugin, :health, :fail_open]` — fail-open stays, but stops being invisible. A source
provider that has stopped completing work self-reports: `http_poll` is unhealthy once its last
SETTLED poll ages past its idle window, mirroring `websocket`'s data-liveness watchdog.
Owner: [`../internals/convergence_loop.md`](../internals/convergence_loop.md).

**Output-stall watchdog** — `lib/flezha/state/watchdog.ex`. Sweeps every 10 s; an Entity still
receiving changes whose seq has not advanced in `output_stall_timeout_ms` (60 s) emits
`[:flezha, :state, :output_stall]` and nothing else — the remedy is the loop's. A `CompactEntity`
unanswered past `compaction_timeout_ms` (30 s) triggers a resync. A sweep that stops completing
reads `sensor_stale` on `/healthz`.
Owner: [`../internals/state_persistence.md`](../internals/state_persistence.md).

**Stall localization** — `flz_nif/src/driver_stats.rs`, `lib/flezha/convergence/stall_classifier.ex`.
Names the layer that wedged (operative, channel, NIF queue, driver thread, socket, runner) and
hands each verdict to exactly one consumer: a heal primitive where the BEAM has a remedy, a
`/healthz` 503 where only a process restart does.
Owner: [`../internals/transport_liveness.md`](../internals/transport_liveness.md).

**Runner-side self-healing** — `sdk/rust/src/operative/poll_pool.rs`,
`sdk/java/.../PollHost.java`, `lib/flezha/sdk/operative.ex`. A host that loses a worker gets it
back without operator action: the pool's live count is released by a guard that runs on an
unwind (so a departed worker never occupies ceiling headroom), the starvation supervisor tops
the pool back up to its base size, Java replaces a fetcher that ends on a throwable, and the
Elixir Operative recreates a crashed Entity on that key's next input. Every activation a host
takes is answered exactly once — complete, fail, or **abandon** — through a structural guard,
because per-entity exclusivity means an unanswered activation retires its entity for the life
of the process.
Owner: [`../sdk/poll_core.md` "The host answers every activation exactly once"](../sdk/poll_core.md).

**The wedged-runner ladder** — `lib/flezha/convergence/`. A runner the classifier proves wedged
rides two rungs on one per-target cooldown: `evict_runner` moves its operatives, and if a later
tick still proves the same runner wedged, `restart_runner` sends a terminal disconnect. The SDK
answers it by running its graceful shutdown and exiting **non-zero** (`70`), so the orchestrator
that owns process lifecycle restarts it — a zero exit would read as a clean stop and the runner
would not come back. Eviction alone leaves the wedged process connected and placeable, which is
the flap the rung exists to end; every restart is logged and emitted as
`[:flezha, :runner, :restarted]` with the verdict and the rung. The controller never restarts a
process itself.
Owner: [`../internals/convergence_loop.md`](../internals/convergence_loop.md).

**Handout liveness** — `flz_core/src/poll_core/`, `lib/flezha/sdk/counters.ex`. Every SDK
reports how many activations are underway (`inflight`) and how long the oldest has been out
(`inflight_oldest_ms`) in the data heartbeat's metadata. One wedged Entity is invisible to the
runner-wide dispatch counter — it keeps climbing on every other Entity — so these two are what
mint `entity_dispatch_stalled`, whose remedy is an operative rebind from durable state. The
core never reclaims a handout on a timer: a ten-minute Procedure and a lost handout are
indistinguishable from it, and re-handing on a deadline would re-execute exactly the slowest
work in the system. `FLEZHA_CONVERGENCE_ENTITY_STALL_MS` (default 300 s) is the threshold.
Owner: [`../sdk/poll_core.md`](../sdk/poll_core.md).

**Control- and data-channel liveness** — `flz_transport/src/transport/liveness.rs`, shared by
the Rust and Java SDKs. A fresh control channel reconnects with no ack inside
`FLEZHA_CONTROL_ACK_TIMEOUT_MS` (10 s), a quiet one after `FLEZHA_CONTROL_IDLE_TIMEOUT_MS`
(45 s); `FLEZHA_CONTROL_KEEPALIVE_MS` (15 s) keeps a healthy channel from tripping it. The data
plane proves itself separately — a `data_heartbeat` every `FLEZHA_DATA_HEARTBEAT_MS` (10 s) —
and a bound stream silent past `:io_stream_liveness_threshold_ms` (30 s) is an output stall.
Owner: [`../internals/transport_liveness.md`](../internals/transport_liveness.md).

**Seq-gap self-heal** — `lib/flezha/state/wal_writer.ex`. A change frame with
`seq > last_persisted_seq + 1` emits `[:flezha, :state, :seq_gap]` and drives a rate-limited
resync; an empty WAL pulls a baseline snapshot instead. Silent and automatic.
Owner: [`../internals/state_persistence.md`](../internals/state_persistence.md).

**Health endpoint** — `lib/flezha/healthz/server.ex`, opt-in via `FLEZHA_HEALTHZ_ENABLED`; reads
only the published convergence snapshot. `degraded` means the loop is healing or an operator
decision is owed; 503 is reserved for a wedged convergence loop or a wedged controller-fabric
driver thread (`FLEZHA_FABRIC_503_POLLS`, default 3, `0` disables).
Owner: [`../architecture/kernel.md` "The `/healthz` contract"](../architecture/kernel.md#the-healthz-contract).

## Durability

**Per-Entity state: WAL + snapshots** — `lib/flezha/state/`. Every change is appended to a
per-Entity WAL and made durable by the `FsyncBatcher` tick (`:agent_fsync_interval_ms`, 5 ms);
snapshots are written atomically; restore ships snapshot + WAL tail to the reconnecting runner.
A graceful stop loses nothing; a hard kill can lose the records inside the last fsync window.
Owner: [`../internals/state_persistence.md`](../internals/state_persistence.md).

**Flow control** — credit windows on both loops: the runner→controller window
(`:wal_high_watermark_bytes`, 16 MiB) is minted back only on WAL fsync, the source→controller
window (`:source_credit_window_bytes`, same default) only on the consumed-offset advance. An
exhausted window parks the sender; nothing is shed. `Block` is the default overflow policy for
every channel kind in both SDKs and the FFI host.
Owner: [`../architecture/kernel.md` "Flow control: the three loops"](../architecture/kernel.md#flow-control-the-three-loops).

**Delivery semantics** — at-least-once by default with replay-stable identity stamps; the
opt-in effectively-once tier adds a durable input log, one atomic effect record per invocation
and a confirmation-gated outbox. The dead-letter queue and late-input correction ride the tier.
Owner: [`data_durability.md`](data_durability.md).

**Workflow operatives** — durable by default: a crash restores committed Entity state and
resumes at the first un-journaled procedure, output is staged and flushed only after the
barrier is durable, and post-start input is journaled before it is forwarded.
Owner: [`workflow_operative.md`](workflow_operative.md).

**Per-Entity timers** — folded state in the Entity's own change log, so the pending set is
rebuilt by the ordinary restore and nothing new has to be made crash-consistent.
Owner: [`../sdk/entity_timers.md`](../sdk/entity_timers.md).

**Remote durability tier** — a production boot must choose `FLEZHA_DURABILITY_BACKEND`; when
enabled, immutable artifacts ship at existing boundaries so a controller that dies with its
disk is rehydrated on a fresh node.
Owner: [`../operations/remote_durability_tier.md`](../operations/remote_durability_tier.md).

## Placement, identity and lifecycle

**Orphan reconcile at boot** — `lib/flezha/service/hub_manager.ex`. Every node starts the
difference between its durable registry (`registry.dets`) and what the cluster holds: on a
cold cluster that is the whole operative list, joining a live cluster it is empty. The durable
copy is never wiped to force either outcome; with a remote backend the registry cspec is
hydrated before any hub opens.
Owner: [`../operations/auto_restore_on_boot.md`](../operations/auto_restore_on_boot.md),
[`../internals/process_hub.md`](../internals/process_hub.md).

**Runner identity and reconnect** — `lib/flezha/runner/`, `flz_core/src/persistent_id.rs`. A
durable SDK-owned stable identity is the binding key, a controller-minted session token the
routing key; a reconnecting runner has its bindings swapped onto the new session instead of
being evicted and re-placed.
Owner: [`../internals/runner_identity.md`](../internals/runner_identity.md); fencing:
[`../architecture/kernel.md` "Ownership and fencing"](../architecture/kernel.md#ownership-and-fencing).

**Graceful shutdown** — `lib/flezha/shutdown/`, `flz_core/src/shutdown.rs`. The controller
quiesces, drains operatives to peers and finalises (a third signal escapes with
`:erlang.halt(143)`); the SDK ships every operative's final state and waits for
`RunnerShutdownAck` (2 s per operative, in parallel) before exiting.
Owner: [`../operations/graceful_shutdown.md`](../operations/graceful_shutdown.md).

**Observability feeding healing** — every `[:flezha, ...]` telemetry event is lifted onto the
observability bus; a broken sink never crashes the producer. Owner: [`../operations/observability.md`](../operations/observability.md).

## Durability guarantee summary

| Scenario | State loss | Output |
| --- | --- | --- |
| Graceful runner stop (SIGTERM / `disconnect()`) | None — the SDK ships final state and waits for the ack | At-least-once |
| Runner SIGKILL / OOM / node death | Best-effort: records inside the last fsync window (5 ms default) | At-least-once |
| Controller graceful restart with a live peer | None — peer sync, runners stay connected | At-least-once |
| Controller plain restart, sole instance | None — the orphan reconcile replays the durable registry; `/healthz` reads `recovering` meanwhile | At-least-once |
| Whole cluster cold boot | Recovered from the durable registry | n/a |
| Controller dies with its disk | Full, unless the remote tier is on: then at most one `ship_interval_ms` (30 s) of stream work or the last unshipped workflow barrier | n/a |
| Effectively-once stream, any crash | None past the last fsynced effect record | Effectively-once at a conforming sink — re-driven under identical identity |
| Workflow operative runner SIGKILL (durable, the default) | None past the last fsync — restores and resumes at the first un-journaled procedure | At-least-once |
| Workflow operative runner SIGKILL (persistence opted out) | Full — re-runs from the top | At-least-once |
| Operative-to-operative hop, target down or parked | None — retained and retried until confirmed | Effectively-once if the source has an outbox, else at-least-once |
| Operative-to-operative hop, controller dies mid-delivery | None for an outbox source — unconfirmed entries are re-driven on restart; a plain stream's in-flight items are lost | As above |
| Poison payload (fails every retry) | None — parked durably to the DLQ, the Entity advances | Entity continues |

## See also

- [`data_durability.md`](data_durability.md) — the scenario matrix, delivery semantics and the operator playbook
- [`workflow_operative.md`](workflow_operative.md) — structure, lifecycle, saga, crash-resume, retention
- [`../architecture/kernel.md`](../architecture/kernel.md) — the durable-compute kernel these mechanisms implement
- [`../internals/convergence_loop.md`](../internals/convergence_loop.md) — the self-healer in full
- [`../operations/remote_durability_tier.md`](../operations/remote_durability_tier.md) — surviving the loss of a disk
