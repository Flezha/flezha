# Flezha workflow operative performance — 2026-08-22

Companion to `PERF-TEST.md` (stream operatives). A stream pays per record
through one long-lived operative; a workflow pays per **instance lifecycle**
and per **durable barrier**. This campaign built a bench for those paths —
spawn → chain → completion, the `ProcCommit` barrier, the chat turn
(`IterCommit`), suspension and the wake paths — measured them, found thirteen
defects (plus one found on the boot re-take), fixed eleven in this tree (one of them in the `process_hub` checkout),
re-measured, and wrote the remaining architectural item up as the OpenSpec
change `workflow-durable-path-throughput`. One 8-core developer box; numbers
are indicative, not publishable.

## 1. Summary

| Question | Answer |
|---|---|
| Could it run workflows under load? | Before the fixes, no: every cell with more than ~100 durable instances in flight collapsed through one defect — a single timed-out ping reset the live control channel for every instance on that runner (§6.4). With the fixes the same cells complete; what remains is a throughput ceiling and a storm limit, not a collapse. |
| How fast is a durable chain? | A 4-proc durable chain: **seed → completed p50 20 ms / p99 73 ms at 50 spawns/s** (before: p50 638 ms with losses). The controller sustains **~350 proc commits/s on two runners** whatever the concurrency (10 → 100 in flight only moves latency 105 ms → 1 s). The ceiling is one synchronous fsync per barrier on the runner's single control-channel process — the OpenSpec change. |
| How fast is a chat turn? | ~75 ms serial for one loop (13 turns/s); **25–33 turns/s for 100 loops**. A turn costs five fsyncs and a file rename, plus two O(history) file reads because the loop's journal is never cut — same OpenSpec change. Compaction, which never ran before (§6.1), costs a further 25 % at the current snapshot-every-iteration policy. |
| What does a storm do? | A wake, rebind or spawn storm of a few hundred instances saturates the serialized control channel: the per-runner `ControlUnit` crashes on its 5 s call timeout, commit acks miss the SDK's 30 s wait and **the loop fails terminally** (588 of 1 000 in a 1 000-wide wake storm; 58 of 100 at 200 wakes/s; 56 of 3 000 at 50 wakes/s). The OpenSpec change makes the barrier batched and the timeout a retry. Below that limit — a 100-wide storm, 50 spawns/s, 100 instances in flight — the paths are clean. |
| Was anything silently lost? | Yes, five ways, all fixed: a restore from a snapshot restarted the Entity's change seq and the writer dropped the next changes as duplicates (§6.9 — latent for streams too); a rehydrated loop parked before the controller's inbox-tail forward arrived and the tail was dropped (§6.12); the terminal output of a durable chain raced its teardown (§6.6); a park landing during an un-park left the controller believing a torn-down instance was bound (§6.10); the hub reconcile stopped live children during a start storm (§6.7). |
| What does a sleeper cost? | Two controller processes (Overwatch + journal writer), **~53 KiB of BEAM** once suspended Overwatches hibernate (it was 470–600 KiB of uncollected bind-time garbage, §6.13), 0.1 KiB on disk, and a place in every convergence tick (~0.15 ms each: 20 ms at 100, 1.2–1.8 s at 10 000). A controller restart brings 1 000 sleepers back as suspended in 45 s (§5.3). |
| Did the sleepers survive a controller restart? | Yes: 1 000 kept sleepers restored as suspended 45 s after the runners connected (§5.3). |

## 2. Setup

| | |
|---|---|
| Box | 8 cores, 31 GB, one consumer NVMe; controller, two Rust release runners and the bench in one BEAM |
| Config | dev: durability backend `:none` (local fsync), convergence loop ON (5 s tick, 1 s probe), hub reconcile every 15 s, authz off; `FLEZHA_WORKFLOW_PARK_IDLE_MS` per cell (SDK default 5 s) |
| Harness | `mix bench.workflow` (`lib/mix/tasks/bench/workflow.ex`) on the shared `Mix.Tasks.Bench.Fleet`; the roundtrip tally (bit per seq for loss/dup + 10 µs histogram) extracted into `Mix.Tasks.Bench.Tally` and used by both benches; `Fleet.start_runners` keeps the runner ports so a cell can SIGKILL one |
| Fixtures | `test/apps/rust_runner/src/main.rs`: `bench_chain_{1,4,16}` (durable K-proc chains of the e2e's `durable_step!`, every edge codec'd, sink at the end), `bench_chain_optout_4` (persistence off), `bench_loop_workflow` (the `chat_loop_workflow` shape — BenchTurn → ChatReply, `PerInput`, bounded state, 24 h backstop, `wake_after` from `FLEZHA_BENCH_WAKE_AFTER_MS`) |
| Measurement | seq + send timestamp in the payload (spawn args for a chain, the turn for a loop); `BeamMsgForward` sink into the bench process; telemetry for `completed` / `suspended` / `rehydrating` / `proc_commit` / `iter_commit`; every cell reports loss, duplicates, state dir, BEAM memory, process kinds; a stalled cell prints the journal / inbox / Overwatch state of the instances that never answered and the control processes' queues |
| SDKs | Rust only. The cross-SDK rule owes Java and Elixir fixtures to the follow-up that lands `workflow-durable-path-throughput`; the kernel fixes (§6.1, §6.9, §6.12) are in `flz_core` and serve Java through it |

Scenarios: `chain` (spawn N run-once chains: burst / closed loop `-c` / open loop
`--rate`), `turns` (L loops × T turns, paced or unpaced under an in-flight cap,
`--kill-runner-at`, `--duration`), `sleep` (rest cost, convergence tick,
message wake storm or `--wake-rate`, timer-wake storm, `--keep`), `sleep-boot`
(controller restart over the kept sleepers). Matrix: `bench/workflow/batch1.sh`
(before), `batch3.sh` + `batch5.sh` (after), `batch2.sh` (sleep family, soak,
repro), `batch4.sh` (proof cells with the local `process_hub`); per-cell logs in
`bench/workflow/reports/` (`fixed_*`, `fixed2_*`, `final_*` = after).

## 3. Lifecycle and the proc barrier (`chain`)

### 3.1 Before the fixes (batch 1)

| Cell | Result |
|---|---|
| 1000 × 1-proc, burst | **205 workflows/s**, 205 commits/s, 0 loss; start call p50 3.1 s (queueing in the burst) |
| 1000 × 4-proc, burst | **collapsed**: 511 completed, 540 outputs missing, 47 runs failed `commit N ack timed out after 30s`, 515 starts refused — §6.4 |
| 1000 × 16-proc, burst | collapsed the same way: 769 completed, 545 missing, 209 failed; 9 867 `WalWriter` processes left behind — §6.5 |
| 1000 × 4-proc, persistence off, burst | all 1000 complete in 2.24 s (≈450 workflows/s, the start path alone), **24 terminal outputs lost** with every completion `ok` — §6.6 |
| 4-proc, 10 in flight, n=1000 | **91 workflows/s, 364 commits/s**, seed → completed p50 105 / p99 191 ms, clean |
| 4-proc, 100 in flight, n=2000 | **88 workflows/s, 351 commits/s**, p50 992 / p99 1 767 ms, clean — same throughput, 10× the latency |
| 4-proc, 1000 in flight, n=5000 | crawled (317/5020 in 100 s); the hub reconcile stopped 201 running children in one round — §6.7; cell killed |
| 4-proc, 50 spawns/s open loop | 17 starts refused, 18 outputs lost, 5 runs failed; three reconcile rounds stopped live chains — §6.4, §6.7 |
| 4-proc, 200 spawns/s open loop | 1 182 of 2 020 starts refused, 55 outputs lost, 22 failed |

The closed-loop pair is the number that matters: **~350 proc commits/s on two
runners, flat from 10 to 100 in flight.** `ControlChannel.handle_proc_commit/2`
runs on the runner's one control-channel process and calls
`ProcJournal.append_proc_committed` → `RecordLog.append` → `WalWriter.append_next`
→ `do_fsync` synchronously before replying: one fsync per barrier, serialized per
runner (≈5.7 ms per commit-and-reply per runner on this NVMe). Concurrency buys
nothing, and every other control command on that channel — pings, stops, parks,
compactions — queues behind the fsyncs.

### 3.2 After the fixes (batch 3 / batch 4, same cells)

| Cell | Before | After |
|---|---|---|
| 1000 × 4-proc, burst | collapsed (511/1000, 540 missing) | **1000/1000 complete, 85–98 workflows/s, 340–390 commits/s**, no channel reset, no reroute, no run failed; 3–7 outputs lost to the teardown race (§6.6: the drain's 10 s bound expired under the burst — raised to 60 s after the last cell); 569–795 of the 1 020 start *calls* timed out at the caller while the instances went on to complete |
| 4-proc, 100 in flight, n=2000 | 88 workflows/s, 351 commits/s | 81 workflows/s, 324 commits/s, p50 1.04 s, clean — the ceiling, unchanged by the fixes |
| 4-proc, 50 spawns/s open loop | 17 refused / 18 lost / 5 failed, p50 638 ms | **clean: 50.0 workflows/s, seed → completed p50 20 / p95 48 / p99 73 ms, start call p50 17 ms** — the service latency of a 4-proc durable chain |
| 4-proc, 1000 in flight, n=5000 (local `process_hub` fix) | 317/5020 in 100 s, 201 children stopped by one reconcile round | **4 901/5 000 completed, 22.8 workflows/s**; reconcile stops 2 (the two-rounds rule holds); 18 230 `ControlUnit` crash reports on the 5 s call timeout, 236 last-attempt channel resets, 19 failed, 341 outputs lost to the drain bound — the storm limit of §6.8 |

## 4. The chat turn (`turns`)

### 4.1 Before the fixes

| Cell | turns/s | turn → reply p50 / p99 | Result |
|---|---|---|---|
| 1 loop × 500, unpaced | 13 (serial, ≈75 ms a turn) | 192 / 439 ms under the cap | **wedged at turn 440**, 61 missing, nothing logged |
| 100 × 100, unpaced, cap 400 in flight | **33** (67 commits/s) | 8.2 s / 31.5 s (queueing at the cap) | clean; 43 loops parked mid-run (the cap is global, so a starved loop idles past 5 s — harness starvation, but every park costs a bind); `125 control reply(s) unacked >= 90000ms` at cleanup |
| 1000 × 20, unpaced | — | — | the 1 000 warm-up turns ran at 66/s; then the per-runner `ControlUnit` crashed 18× on a 5 s `send_control` call timeout, 175 commit acks arrived after the SDK's own timeout, and 3 992 inbox frames were dropped at the SDK because the controller kept forwarding to drivers that had already parked |
| 100 × 50 at 200/s (every loop woken at once) | 0.6 | — | 100 rehydrations → 100 pings → timeouts → channel reset → `iteration commit 5 ack timed out after 30s` → **39 loops failed terminally** — §6.4 |
| 200 × 20 at 100/s, park idle 500 ms (every turn a wake) | 1.2 | — | 28 failed, 3 778 missing — §6.4 |
| 100 × 60 at 200/s, runner SIGKILL at +10 s | 1.1 | — | **no reply after the kill**: 21 reroutes, 65 × `StreamOperative class … is not registered` — §6.2 |

What a turn costs on the durable path, from the code: `ProcInbox.append/3` reads
the WHOLE inbox file (`undelivered/1` → `read/1`) and folds the WHOLE proc journal
(`ProcJournal.consumed_offset/1` → `live/1`), appends and fsyncs; two
`ProcCommit`s (one fsync each, serialized on the control channel); one
`IterCommit` (fsync); the boundary `CompactEntity` round trip; the outbox flush;
the inbox cut — tail copied to `.rewriting`, fsynced, renamed. Five fsyncs and a
rename per turn plus two O(history) reads, and the journal of a loop is never
cut, so the fold is O(turns): quadratic over a session.

### 4.2 After the fixes (batch 3 / batch 5)

| Cell | Before | After |
|---|---|---|
| 100 × 100, unpaced | 33.3 turns/s, 0 snapshots | **24.8 turns/s, 436 snapshots, clean** — compaction now runs at every iteration boundary and costs ~25 % of turn throughput at this policy (§6.11) |
| 100 × 50 at 200/s (wake storm) | 39 failed, channel reset | no channel reset, 100 snapshots, **58 failed** `ack timed out after 30s` — the storm now saturates the channel instead of cutting it (§6.8); complete fix set: 171 replies of 5 000, **31 failed**, 345 ping timeouts, 12 `ControlUnit` crash reports, 0 tail drops (the §6.12 gate holds), wake → reply p50 4.1 s — fewer failures than before the gate (58), same limit |
| 200 × 20 at 100/s, every turn a wake | 28 failed | complete fix set: 247 replies of 4 000, **101 failed**, 136 ack timeouts, 444 ping timeouts, 99 `ControlUnit` crash reports, 96 tail forwards dropped at parking drivers and re-delivered by the park — 100 wakes/s over 200 loops is above the channel's limit (§6.8) |
| 100 × 60 at 200/s, runner SIGKILL at +10 s | no reply after the kill | **first reply 17 ms after the kill**, 0 refusals, 22 reroutes, 122 rehydrations — the migrate fix works; then the 100-wide rebind storm saturates the surviving runner's channel: 42 failed, 22 duplicate replies (the at-least-once re-run of uncommitted turns); complete fix set: 47 reroutes, 0 refusals, 36 ack timeouts (the rebind storm on the surviving runner); the cell was stopped after 10 min of trickling progress |
| 1000 × 20, unpaced | wedged (see above) | complete fix set: the 1 000 warm-up wakes all answered (65/s, 0 ping failures, 0 drops — §6.4 and §6.12 hold), then the measured turns saturate the channel: 750 replies of 20 000, **850 loops failed terminally** on ack timeouts, 636 `ControlUnit` crash reports, 4 035 turns dropped at drivers already parking (re-delivered by the park only where the loop survived) — §6.8 |
| 1 loop × 500, park impossible (60 s idle) | — | wedged at ~turn 440 unpaced — NOT its own bug: paced at 50/s the same loop completes 500/500 clean (§6.8). The journal is never cut, so each turn folds the whole journal; under an unpaced burst the per-turn latency blows up until one inter-turn gap exceeds the harness stall timeout, and even paced the reply p99 climbs to 21 s by turn 500 |
| 1 loop × 500, default park | wedged at 440 | wedged at 440 again (reproducible) |

### 4.3 Churn soak — 100 loops, 20 turns/s, every turn a wake (15 min)

Livelock: 1 359 replies of 18 000 in 15 minutes, **17 872 parks** — each turn
woke its loop, the turn was answered or dropped at a parking driver, the loop
parked again within 500 ms, the next turn woke it again; the channel spent the
run on binds and parks rather than turns. Two verification suites ran on the
box during this soak (their ping timeouts are in its log), so the absolute
numbers are tainted; the shape — a wake per turn at 20/s does not sustain on
the serialized channel — matches the paced cells. A soak at a sustainable wake
rate is owed with the OpenSpec change.

## 5. Sleeping instances (`sleep`, `sleep-boot`)

### 5.1 Resting cost and wake paths (after the fixes)

| Sleepers | BEAM per sleeper | Processes per sleeper | State dir | Convergence tick | Message wake | Timer wakes |
|---|---|---|---|---|---|---|
| 100 | +69 KiB (processes heap +66 KiB) | 2 (`Overwatch` + journal `WalWriter`) | 0.1 KiB | 20–29 ms | all 100 woken at once: last reply 627 ms after the first cast (160 wakes/s), wake → reply p50 499 / p95 610 / p99 622 ms, 100 rehydrations, 0 missing | 100 of 100 fired, answered within 0.49 s of the deadline (p99), 100 rehydrations |
| 1 000 | +495–530 KiB before §6.13; **+53 KiB after** (Overwatch heap 5 KiB, journal writer 10 KiB) | 2 | 0.1 KiB | 103–119 ms (230–670 ms on the re-takes) | all at once, before §6.12: 2 000 rehydrations, 2 000 drops, 0 replies. Complete fix set, all at once: **1 000/1 000 replies, 0 drops, 0 ping failures**, last reply 7.8 s after the first cast (128 wakes/s), wake → reply p50 7.0 / p99 7.75 s; the re-park behind it stalled on iteration commits (3 of 1 000 acked in 60 s, 151 `ControlUnit` crashes). Paced at 20/s: 149 replies of 1 000, 4 463 ping timeouts, 449 `ControlUnit` crashes — paced binds compete with the earlier wakes' commits and parks | (cell design fault: 20 s timers fired into the park storm) |
| 3 000 | +471 KiB (Overwatch heap 66 KiB; the rest per-instance writers) | 2 | 0.1 KiB | 536–548 ms | paced at 50/s: 203 replies of 3 000 in 5 min — 3 250 rehydrations, 1 913 `ControlUnit` crashes on the 5 s `send_control` call timeout, 14 988 ping failures, 56 loops failed terminally; wake → reply p50 6.1 s | — |
| 10 000 | +598 KiB (BEAM 6.1 GB; Overwatch heap 147 KiB; largest single processes: hub event queue 56 MiB, hub 27 MiB, `Convergence.Loop` 27 MiB) | 2 | 0.1 KiB | 1.2–1.8 s | paced at 100/s: livelock — 145 259 rehydrations for 10 000 wakes, 116 647 `ControlUnit` crash reports, 112 101 ping failures, 105 loops failed, 132 replies in 11 min | — |

Spawning 10 000 loops at ≤ 50 starts in flight took 176 s (57/s, start call
p50 907 ms / p99 1.8 s); 3 000 took 21.6 s (139/s); 1 000 took 2.5 s at 1 000 in
flight. Per-sleeper controller memory looked super-linear — 69 KiB at 100, 495 KiB
at 1 000, 471 KiB at 3 000, 598 KiB at 10 000 — until the GC probe showed an
Overwatch heap of 2.5 MiB collapsing to 5 KiB: uncollected bind-time garbage
on Overwatches that never hibernated (§6.13, fixed — 53 KiB per sleeper at
1 000 afterwards). The convergence tick walks every sleeper: ~0.12–0.18 ms
each, 1.2–1.8 s at 10 000.

### 5.2 Storms

A 100-wide storm is answered in 0.6 s. A 1 000-wide storm, or a paced 50/s wake
over 3 000 sleepers, saturates the channel: binds, restores and parks queue on
one process per runner, the `ControlUnit` crashes on its 5 s call timeout and
takes every in-flight request with it, the commit acks miss the SDK's 30 s wait
and the loops fail terminally. Two harness mistakes inflated the first storm
cells and are recorded so nobody repeats them: parks were counted from after
the replies instead of from the wake (the "366 / 37 / 108 parks" stalls), and a
20 s timer storm was fired into a still-draining park storm.

### 5.3 Controller restart over sleepers

The first boot cell (3 000 kept sleepers) restored nothing in 5 minutes: the
namespace hub — and with it the recovery replay of its declared children —
starts with the first runner connect, which the cell had not done. The second
attempt (1 000 kept sleepers, runners connected first: 626 ms after app start)
crashed the harness on a reply carrying the previous VM's monotonic clock
(fixed: such a latency counts as 0). Third attempt, clean (1 000 sleepers
kept straight after the resting report, nothing owed, suspended Overwatches
hibernating): **all 1 000 restored as suspended 45.3 s after the runners
connected** (runners connected 0.6 s after app start) — ~22 restores/s
through the hub's recovery replay; after the boot the population costs
+181 KiB per sleeper in total BEAM (processes heap +34 KiB, journal writers
not yet started: 1.07 processes per sleeper), convergence tick 210–630 ms.
The convergence loop logged every restored sleeper as "plugin-degraded" and
healed it — a suspended instance released its plugins with its runner, and
the rehydrating bind replays them; fixed (§6.14).

## 6. Defects

| # | Defect | State |
|---|---|---|
| 6.1 | `CompactEntity` never resolves a workflow Entity | **fixed** (`flz_core/src/command/compact.rs`) + e2e assertion |
| 6.2 | `migrate_operative/1` drops the workflow kind | **fixed** (`lifecycle.ex`) + test |
| 6.3 | Retained finished-job state reported as a wiped-registry witness | **fixed** (`convergence/loop.ex`) + test |
| 6.4 | A timed-out ping resets the live control channel | **fixed** (`lifecycle.ex`; spec + docs synced) + tests |
| 6.5 | A stopped operative's `WalWriter` processes never close | **fixed** (`state/supervisor.ex`, `operative_manager.ex`) + test |
| 6.6 | The terminal emit races the completion teardown | **fixed** — durable classes: the teardown drains the journaled outbox; opted-out classes: `WorkflowCompleted.direct_outputs` counts the data-stream outputs written before it and the teardown waits until the Overwatch has run them (60 s bound) |
| 6.7 | The hub reconcile stops live children during a start storm | **fixed in process_hub** (commit `bb49842` on the local `dev` checkout, two-consecutive-rounds rule + test) — push and bump the pin |
| 6.8 | One fsync per barrier on the control-channel reader; O(history) per turn; ack timeout is terminal; `ControlUnit` crashes on a busy channel | OpenSpec `workflow-durable-path-throughput` |
| 6.9 | A restore from a snapshot restarts the Entity's change seq; the writer drops the next changes as duplicates | **fixed** (`RestoreInitState.terminal_seq`, `restore.rs`, `commands.ex`; protocol doc) + compaction-proof e2e count |
| 6.10 | A park landing during an un-park is overwritten by the task's stale bound state | **fixed** (`overwatch.ex`, `lifecycle.ex`) |
| 6.11 | Boundary compaction at every iteration costs ~25 % of turn throughput | policy question in the OpenSpec design |
| 6.12 | A rehydrated loop parks before the controller's inbox-tail forward arrives; the tail is dropped | **fixed** (`AddOperative.inbox_tail_count`, `WorkflowInstance.owed_inbox`, `wait_for_input`; spec `workflow-durable-suspension`) |
| 6.13 | Per-sleeper controller memory grows with the population (69 KiB → 598 KiB per sleeper from 100 to 10 000) | **fixed** — uncollected garbage on never-hibernating Overwatches (2.5 MiB → 5 KiB after GC); suspended Overwatches now hibernate |
| 6.14 | The convergence loop diffs a suspended instance's plugins, finds every declared one "missing", logs it degraded and heals it | **fixed** (`convergence/diff.ex`: a suspended instance is not diffed — the rehydrating bind replays its plugins) + test |

### 6.1 A workflow Entity is never compacted
`compact.rs` resolved the addressed Entity with `restore::find_entity_slot`,
which walks `runner.get_operatives()` = `snapshot_stream()` — stream instances
only — and then the dynamic-entity path (also stream). `RestoreInitState` has the
workflow fallback `runner.find_workflow_entity`; `CompactEntity` did not. Every
iteration-boundary `CompactEntity` and every Watchdog compaction of a workflow
Entity answered `compact_entity: no live entity matches the addressed
entity_id` (logged at `[error]` and `[warning]`, twice per turn per Entity);
every before-cell showed `0 snap`. Consequence: a loop's Entity WAL grew one
record per turn forever and every wake replayed all of it. Fix: the same
fallback restore uses; the loop e2e asserts a `chat_turn` snapshot after two
committed iterations. Fixing it exposed §6.9.

### 6.2 A workflow instance cannot be migrated by the convergence loop
`Lifecycle.migrate_operative/1` rebuilt the `%Builder{}` for `:runner_migrate`
without `kind:` (so `:stream`). The runner answered `StreamOperative class with
factory_id … is not registered`, `Heal.rebind/2` reported `:error`, FleetManager
logged `reroute … did not confirm; binding left unchanged, heals on next
convergence tick` — and never did. The crash-resume e2e proves the other path
(Overwatch `:kill` → fresh `init` with the original builder); the migrate path
had never run with a workflow. Before: `no reply after the kill`; after: first
reply 17 ms after the kill.

### 6.3 Finished-job retention reported as a wiped-registry witness
`Convergence.Loop.intent_mismatch/2` took every state dir as "durable state
evidence"; a completed workflow's state is retained 72 h by design, so every
completion joined the mismatch set, the loop logged the whole list at
`[warning]` every tick and re-diffed it: 4–17 ms with 20 retained instances,
126 ms with 1 020, 398 ms with 2 020. Fix: a dir with a `Completion` record is
intent fulfilled, not lost.

### 6.4 A slow ping cut the live control channel
Under a burst `start_operative` took ~2.03 s per instance against the 2 s
`overwatch_ping_timeout_ms`, so the Overwatch's end-to-end ping timed out.
`Lifecycle.attempt_ping_and_start/2` treated ANY ping error as a stale
transport and called `FleetManager.reset_control_channel/1` →
`ControlChannel.reset_stream_ref/1`, nil-ing the `stream_ref` of a healthy
channel. From then on every control command to that runner answered
`{:error, :no_connection_available}`: `{:ping_failed, :no_connection_available}`
×1 121 across retry attempts, 515 starts refused, `commit N ack timed out after
30s` ×47 (the journal append landed, the ack could not be written), `no active
runner … within 30000 ms` ×30, Overwatches stopping with `:runner_not_ready`
×38; the convergence probe's 1 s `:get_overwatch_state` timed out on the busy
Overwatches (×441) and rerouted (§6.2). The 100-wide wake storm did the same and
failed 39 loops. A second shape: the per-runner `ControlUnit` crashed when its
5 s `send_control` call into the busy channel timed out, and the ping then failed
with `{:control_unit_exit, {{:timeout, …}}}`, which also reset the channel. Fix:
a timeout-class ping error (`:timeout`, or a ControlUnit exit on a timeout)
resets only on the retry ladder's last attempt; a dead-transport error still
resets at once. A/B: the 1000 × 4-proc burst goes from 511/1000 to 1000/1000.
(Catching the ControlUnit's call timeout instead of letting it crash was tried
and reverted: a wedged channel then serialized every caller at 5 s each.)

### 6.5 A stopped operative's WAL writers lived on
`OperativeManager.delete_operative_state_dir/1` retained a finished workflow's
files (correct) and stopped nothing; the deleting branch removed files under
live writers. 9 867 `WalWriter` processes after 789 completed 16-proc chains —
K+2 per durable workflow, for the 72 h retention window; stream operatives
leaked theirs too. Fix: `State.Supervisor.close_writers/1` from the teardown,
retained or deleted. After: 309 processes after 1 020 completed 4-proc chains.

### 6.6 The terminal emit races the completion teardown
Durable class: the last proc's outbox is flushed after its commit ack; the
`WorkflowCompleted` that follows tore the Overwatch down under that flush
(3/1000 outputs lost with `status: "ok"` and no `outbox_flushed` in the
journal). Fix: `ControlChannel.drain_owed_outputs/1` re-drives `flush_pending`
until `ProcJournal.pending_entries/1` is empty before `stop_operative`. Under a
1000-instance burst the flush itself queues behind the saturated channel: the
10 s bound expired for 7 of 1 000 (`completed with 1 outbox entries still
owed; they stay journaled`), so the bound is now 60 s; the retained journal
keeps the debt in any case. Opted-out class: the emit rides the data stream and
the completion the control stream; the teardown could overtake the frame
(24/1000 lost). The SDK already writes both in order from one coordinator
loop, so the loss was on the controller — and the count gate built first
(`WorkflowCompleted.direct_outputs`, proto field 5: the direct outputs written
before the completion; the Overwatch counts the `:io_out` it has run and the
teardown waits for the difference) turned it from a silent loss into a
waiting teardown, which named the real site: the `StreamChannel` dropped a
frame silently when the operative was not yet in its routing map, which the
registry hook seeds only after the bind publishes — a 1-proc chain emits
before that. Fix: the Overwatch registers itself for its class before the
SDK is started, and a routing miss resolves through that registry; a frame
neither knows is counted as `[:flezha, :stream, :drop]`. Proof: the same
burst now delivers 1000/1000 at 467 workflows/s.

### 6.7 The hub reconcile stops live children during a start storm
process_hub `Recovery.Round.stop_undeclared/3` terminated every live row marked
durable whose id was not in the declared set, every 15 s. A durable start
registers the hub row first and group-commits the declared entry on the
`ProcessHub.Service.Batch` core (`operative-startup-latency`); a round inside
that window saw "live + durable + undeclared". 201 running children stopped in
one round at 1000 in flight; at 50 spawns/s the five ids stopped (wf_1013–1018)
were the run's missing completions. Fix (process-hub `bb49842`, not pushed): the
undeclared stop uses the round's existing two-consecutive-rounds memory
(`StorageKey.rup`), reports `deferred_undeclared`; its recovery test asserts the
deferral (30/30 pass). Batch 4 ran flezha against it
(`FLEZHA_PROCESS_HUB_PATH=~/Code/process-hub`): 2 reconcile stops across 5 000
starts at 1 000 in flight, against 201 in one round before. The pinned dep needs
the push and a `mix.lock` bump.

### 6.8 The barrier path (OpenSpec `workflow-durable-path-throughput`)
One fsync per barrier, serialized on the control-channel reader; an inbox append
that reads the inbox file and folds a never-cut journal (this is the whole of the "single-loop wedge at ~turn 440": unpaced, the per-turn latency blows up until the harness stall timeout trips; paced, the same loop finishes with reply p99 21 s by turn 500); an ack timeout that
ends the run; a control channel whose queue under a storm exceeds every other
command's 5 s call timeout, so the `ControlUnit` crashes and takes its in-flight
requests with it. The change batches the journal/inbox appends on the
`FsyncBatcher` tick with acks per batch off the reader, keeps the undelivered
set in the inbox writer, cuts the journal at every iteration commit, and
re-sends an unacked barrier before failing. Acceptance: this report's c100 and
100 × 100 cells, and `turns -n 1000 -m 20` completing with no `ControlUnit`
crash. Every storm result in §3–§5 is this defect's blast radius.

### 6.9 A restore from a snapshot restarted the Entity's change seq
`restore_state` (`sdk/rust/src/operative/dispatcher.rs`) derived the terminal
seq from the change-log records only. After a compaction the WAL tail is empty,
so a rehydrate restored the snapshot and re-armed `change_seq` at 1; the writer
then dropped the next `snapshot_seq` changes as duplicates
(`dedup_drops_total`, no warning) — the loop e2e's "wake-up" turn was answered
and never persisted. The controller already computed
`terminal_seq_hint = max(snapshot_seq, wal_terminal)` in
`Restore.assemble_payload/1` but never shipped it. Fix: `RestoreInitState.terminal_seq`
(proto field 6, additive) and `restore_into_slot` re-arms from
`max(change-log terminal, terminal_seq)` — in `flz_core`, so Rust and Java
alike. Masked for workflows by §6.1; latent for a stream Entity migrated after
a compaction. The loop e2e now counts distinct Changes compaction-proof.

### 6.10 A park landing during an un-park was overwritten
`apply_migrate_result/2` replaced the Overwatch state with the un-park task's
bound snapshot. When the driver found its inbox empty and parked before the
task reported, the controller ended up believing an instance the runner had
torn down was bound; the journaled turn waited forever. Fix: `suspend_at/3`
marks `parked_in_flight` while a task is binding, and the result handler keeps
the suspension and re-runs `rehydrate_if_pending/1` (the task `:DOWN` path too).

### 6.14 Suspended instances reported plugin-degraded
`Diff.plugin_divergence/2` diffed every declared operative with a live pid. A
suspended workflow instance keeps its Overwatch alive but released its
plugins with the runner, so every declared plugin read as missing: 1 000
sleepers logged as degraded and a plugin heal issued against instances with
no binding. A suspended instance is now skipped like an unbound one; the
rehydrating bind replays the cspec plugin list. The same hole existed for a
parked instance (init failed, plugins prepared but not running — the un-park
activates them): the plugin diff now runs only for a running operative.

### 6.11 Boundary compaction at every iteration
With §6.1 fixed, the 100 × 100 cell dropped from 33.3 to 24.8 turns/s while
producing 436 snapshots: a snapshot of every Entity at every iteration for a
loop whose state changes by bytes per turn. Snapshot on the Watchdog's size/age
triggers or every N iterations — decided with the journal cut in the OpenSpec
design.

### 6.12 The rehydrate tail race
The rehydrated driver's park-idle clock started at `AddOperative`, while the
controller forwarded the journaled inbox tail only after the bind completed
(`complete_init` → `deliver_inbox_tail`). Under load that took longer than the
500 ms park idle: the driver parked, the tail hit a closed inbox and was
dropped (`inbox frame … dropped (not running / no inbox)`), the park's
rehydrate-if-pending forwarded it again, and the race repeated — 1 000 wakes
became 2 000 rehydrations, 2 000 drops and no reply. Fix, the
`resume_entity_count` pattern: `AddOperative.inbox_tail_count` (proto field
11) carries `length(ProcInbox.undelivered/1)` at bind, `WorkflowInstance.owed_inbox`
counts it down, and `wait_for_input` waits an owed-tail bound (60 s) instead of
the park idle while items are owed. A turn that lands while a driver is already
parking is still dropped at the socket and re-delivered by the park — that
path is unchanged and correct.

### 6.13 Per-sleeper memory was uncollected garbage
Measured in §5.1 as 470–600 KiB per sleeper at 1 000–10 000. The resting
report's GC probe settled it: **an Overwatch heap of 2 499 KiB drops to 5 KiB
after a forced collection** (journal writer 10 KiB). A suspended Overwatch
never hibernated, so the bind's restore frames and plugin setup stayed on its
heap until the next wake. Fix: both entries into the suspended state
(`handle_cast({:suspend, …})`, the boot-restore continue) return `:hibernate`.
The clean keep/boot re-take (§5.3) is the measurement after the fix.

## 7. Follow-ups

Open:

1. Implement `workflow-durable-path-throughput` (proposal, design, specs,
   tasks in `openspec/changes/`; validates `--strict`; 0/17 tasks). Every
   storm number in this report is its acceptance bar. This is the one large
   architectural item.
2. The crash-resume redo-vs-skip failures (`workflow_crash_resume_e2e_test.exs`
   `:89`/`:125`) — pre-existing and unowned (the 12-failure set at HEAD on
   2026-08-20), not caused by this campaign; its own session.
3. Java and Elixir bench fixtures (cross-SDK rule) with the OpenSpec change;
   the kernel fixes are already in `flz_core`, so Java inherits them.
4. A churn soak at a sustainable wake rate once the barrier path is batched.

Done this campaign (were follow-ups): process-hub `bb49842`/`8e60760`/`a95a222`
pushed and the pin bumped (`d097029b`); §6.13 (hibernating sleepers, `2f8ea418`
era); the opted-out terminal emit (§6.6, `33c13fe9`); the single-loop "wedge"
was diagnosed as §6.8, not a distinct defect.

## 8. Where things are

| | |
|---|---|
| Harness | `lib/mix/tasks/bench/workflow.ex`, `lib/mix/tasks/bench/tally.ex`, `lib/mix/tasks/bench/fleet.ex` (ports kept) |
| Fixtures | `test/apps/rust_runner/src/main.rs` — "Bench workflow fixtures" |
| Matrix + logs | `bench/workflow/batch{1..5}.sh`, `bench/workflow/reports/*.log` |
| Fixes | `flz_core/src/command/{compact,restore,operative_start}.rs`, `flz_core/src/{fast.rs,workflow/{driver,instance}.rs}`, `flz_core/src/generated/runner_commands.rs`, `protobufs/runner_commands.proto`, `lib/flezha/operative/{lifecycle,overwatch}.ex` (incl. `:hibernate` on suspension), `lib/flezha/convergence/loop.ex`, `lib/flezha/state/{supervisor,commands}.ex`, `lib/flezha/service/operative_manager.ex`, `lib/flezha/runner/{control_channel,control_unit}.ex`; tests beside them; `docs/operative_architecture.md`, `docs/sdk_protocol_spec.md`, base specs synced directly (no OpenSpec change): `operative-state-persistence`, `workflow-durable-loop`, `convergence-self-healing`, `workflow-durable-suspension`, `workflow-operative-lifecycle`, `runner-controller-connection-healing`; process-hub `bb49842`/`8e60760` + its `registry-convergence` spec |
| Notes | `docs/dev_notes/workflow-perf-campaign.md` (indexed in `CLAUDE.md`) |
| Proposal | `openspec/changes/workflow-durable-path-throughput/` |

## 9. Verification

| Suite | Result |
|---|---|
| `mix test` | 1651/1652 passed while two other suites and a bench cell ran; the one failure passes alone (`mix test --failed`: 1 passed) |
| `cargo test -p flz_core` | 110 passed |
| `cargo test -p flezha_runner --features bench-internals,testing --lib --tests` | all green (130 lib + feature suites) |
| process-hub `mix test test/process_hub_recovery_test.exs` | 30 passed |
| Loop e2e (`workflow_crash_resume_e2e_test.exs:558`, `--include system`) | passes with the new compaction-proof assertions (one failure recorded while a debug rebuild and the 10k spawn ran concurrently; passes on re-run) |
| Workflow crash-resume / saga e2e cells | fail in this tree on a quiet box, all runtime/transport cells, with `journaled proc A was re-run on resume (redo-vs-skip broken)` and `operative never reached its durable mid-run point` — the identical 12-failure set recorded at HEAD on 2026-08-20 (proven then with `lib/` restored to HEAD: an open, unowned bug at HEAD, not introduced here); the loop/suspension cell of the same file passes |
| Overwatch/inbox tests after the hibernate fix | 18 passed |
| Harness after two `cx-refine` cycles (shared process sample, one diagnostic core, one spawn prologue, one verdict line, tolerant fleet cleanup) | `chain` and `sleep` smoke cells clean; `mix format --check-formatted` clean on every touched Elixir file |
| `openspec validate --specs --strict` | 79/79 |
| `openspec validate --changes --strict` | `workflow-durable-path-throughput` ✓ (the pre-existing `platform-productionization-umbrella` fails) |
| Compile | `mix compile --warnings-as-errors` clean; `cargo build -p rust_runner --release` 0 warnings; touched Rust files `rustfmt +nightly` clean |
