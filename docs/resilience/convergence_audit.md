# Convergence Subsystem — Reliability & Performance Audit

> **Fix status:** All five items in the prioritized fix list (section 5) have
> been implemented: C1 (sanity gate on empty executors), C2 (tick try/rescue),
> C3 (concurrent probe via `Task.async_stream`), C4 (misbound streak ≥ 2 ticks),
> and H4 (output-stall grace period). The "Not implemented" annotations below
> are historical — the current code reflects these fixes.

## Context

The convergence loop (`lib/flezha/convergence/`) is the controller's single self-healing
mechanism. The working theory under test: **the loop that is supposed to heal the system is
itself a primary cause of outages ("breakouts"), churns healthy relays, and degrades
performance.** This document is the evidence-backed verdict.
Changes are scoped to be **minimal and deletion-funded** (consistent with the
repo's LOC budget), so the actionable fix list below leads with the smallest surgical changes and
marks the larger redesign as optional.

### Strongest prior evidence (git history = active firefighting)

The recent commit log on these files is itself the smoking gun — the loop has *already* caused
at least two confirmed outages, each fixed reactively:

- `336f960 fix(convergence): stop self-healing from SIGKILLing its own live data channel` — the
  recurring flezha_market freeze was the loop killing a **busy-but-healthy** StreamChannel because
  it couldn't answer a 1s proxy probe within budget while doing inline disk I/O. Fixed by
  **deleting** the kill machinery (net −239 lib).
- `77330f2 convergence: report remote pids in SafeCall.probe_target instead of crashing` —
  `Process.info/2` raised `ArgumentError` (an `:error`, not the `:exit` that `SafeCall` catches)
  on a remote pid; the raise **propagated out of the Loop and took the whole node down.**
- Also: `a163a49 heal primitives are safe no-ops on healthy targets`, `83accee authoritative
  signals over proxies`, `59d8153 recover wedged Kafka producer` — a continuous stream of
  "the healer hurt us, patch it" commits.

Both confirmed outages share one shape: **the loop acted destructively on a proxy/partial
observation against something that was actually fine.** That is the pattern this audit traces to
its structural root, not just the two already-patched instances.

---

## 1. Tick cost model (worst-case & steady-state wall-clock)

One tick runs **entirely inline in the single `Loop` GenServer** (`loop.ex:151 run_tick`), so the
sum below is also the time the Loop cannot service `:output_stall`, `:executor_connected_event`,
or `:tick_now`. Let **R** = relays, **W** = wedged/slow-to-answer relays, **Nₛ** = namespaces,
**C** = stream channels per executor, **P** = planned ops (capped at 20, `planner.ex:20`).

| Phase | Call | Per-unit cost | Serial count | File |
|---|---|---|---|---|
| Intent | `relay_plugins/2` registry read | ~ε (DETS: ms) | R | `intent.ex:55` |
| Probe.relays | `SafeCall.bounded(pid, :get_overwatch_state, 1000)` | **≤1000 ms if wedged** | **R (serial)** | `probe.ex:43-51` |
| Probe.active_executors | `FleetManager.active_executors/1` = `GenServer.call` | **≤5000 ms (default)** | Nₛ (serial) | `probe.ex:81`, `fleet_manager.ex:56` |
| Heal.rebind (per op) | overwatch_snapshot 1000 + skip_rebind? `executors_for_factory` 5000 + assign `executors_for_factory` 5000 + `executor_migrate` 2000 + `hook_insert_pid` C×1000 | **≈13 s + C s** | up to 20 | `heal.ex:136-150`, `fleet_manager.ex:83` |
| Heal.evict (per op) | `FleetManager.evict_executor/1` = `GenServer.call` | ≤5000 ms | up to 20 | `heal.ex:98`, `fleet_manager.ex:71` |

**Steady-state (healthy cluster):** every `SafeCall` returns in <1 ms, `active_executors` is
fast, `Diff` is empty → **0 ops**. Tick ≈ a few ms + R fast registry/Overwatch reads. Acceptable.
**A healthy cluster should issue ~0 heal ops/tick** — `[:flezha, :convergence, :heal]` firing
every tick in steady state is the smoking gun to look for in telemetry/logs.

**Worst-case (the regimes that actually occur):**
- **All Overwatches slow** (e.g. inline disk I/O on slow storage — exactly the `336f960`
  condition): `Probe.relays` = **R × 1 s**. At R=100 that's **100 s for the probe phase alone**,
  during which the Loop is deaf to every event and ticks serialize.
- **FleetManager slow/backed-up:** `active_executors` blocks Nₛ × 5 s, AND every rebind calls
  `executors_for_factory` **twice** (`heal.ex:141` via `assign_executor` + `heal.ex:184` via
  `skip_rebind?`), each a 5 s `GenServer.call` to that same wedged process.
- **Mass-misbound tick:** 20 rebinds × ≈13 s ≈ minutes (many calls fail faster on `:noproc`, but
  the floor is still tens of seconds).

The `30_000 ms` timeout on `tick_now` (`loop.ex:46`) is a tell that the authors already know a
tick can take tens of seconds.

---

## 2. Ranked table of REAL issues

Severity (S1 critical → S3 minor) × likelihood × blast radius. "Amplifies" = the loop makes the
original fault worse.

| # | Issue | File:line | Trigger (concrete) | Sev | Likelihood | Blast radius | Amp/Damp |
|---|---|---|---|---|---|---|---|
| **C1** | **Mass self-inflicted migration on a transient FleetManager read failure.** `Probe.active_executors` catches `:exit` and returns an **empty MapSet** (`probe.ex:82-84`); `Diff` then marks **every** relay whose `bound_executor` is set but not in the (empty) live set as **misbound** (`diff.ex:50`); Planner emits `evict_executor` + `rebind_relay` for all of them. **No sanity/quorum gate exists** comparing reality-emptiness against Intent. | `probe.ex:77-88`, `diff.ex:41-53`, `planner.ex:44-67` | FleetManager mailbox backs up under connect-flood/load → `active_executors` 5 s timeout → empty set → all relays "misbound" in one tick. | **S1** | **High** (FleetManager is a single GenServer on the hot connect path) | Whole namespace re-migrates + WAL resync; positive-feedback against FleetManager | **Amplifies** |
| **C2** | **Single GenServer = SPOF with default restart intensity.** Loop runs the whole tick inline; any uncaught error (not `:exit`) crashes it. Started under `:one_for_one` with **no `max_restarts` override** (`application.ex:77,153`) → BEAM default 3-in-5 s → repeated Loop crash takes down `Flezha.Supervisor` = the controller. Already happened (`77330f2`). On restart `recovery_awaited?` resets (`loop.ex:31`). | `loop.ex:88-92,151`, `application.ex:77,151-157` | Any un-`:exit` raise in tick body (the `ArgumentError` class, a malformed registry row, etc.). | **S1** | Medium (one class already shipped to prod) | Entire controller down | **Amplifies** |
| **C3** | **Serial probe storm / head-of-line blocking.** `Probe.relays` does a sequential `SafeCall.bounded(pid, :get_overwatch_state, 1000)` to every relay in the Loop process (`probe.ex:43-51`); rebind adds more serial 1–5 s calls. While blocked the Loop cannot service `:output_stall` / `:executor_connected_event` / `:tick_now` (they queue behind the tick). | `probe.ex:42-51`, `loop.ex:160` | Any N relays slow to answer (slow disk, wedged Overwatch). | **S1** | **High** (recurs whenever storage is slow — the `336f960` regime) | Detection latency → tens of seconds; events starve; ticks serialize | **Amplifies** |
| **C4** | **Executor flap amplification.** One brief executor disconnect makes every relay on it misbound for a tick → `evict_executor` (one, keyed per-executor — OK) **plus** N `rebind_relay` (keyed per-relay, `planner.ex:34`), each doing `executor_migrate` + `hook_insert_pid` + **WAL `resync_for_overwatch`** (`heal.ex:142-159`). Backoff does **not** contain the first occurrence — initial burst is always eligible (`backoff.ex:37-39`). | `heal.ex:136-159`, `backoff.ex:36-41`, `planner.ex:51-56` | TCP blip / executor GC pause / brief netsplit. | **S1** | **High** | All relays on the executor migrate + replay WAL; flap-back if it returns | **Amplifies** |
| **H4** | **output_stall rebind always acts, even on upstream quiet.** `skip_rebind?(_,_,_, %{reason: :output_stall})` returns `false` unconditionally (`heal.ex:175`) — a relay bound to a healthy executor but quiet (idle market / the WS idle-watchdog false stall this lineage just fixed) is migrated + WAL-resynced for no fault of its own. | `heal.ex:172-175`, `loop.ex:98-101`, `planner.ex:62-65` | Quiet upstream feed for > `output_stall_grace_ms` (60 s). | **S2** | Medium | Pointless migration + resync churn on idle-but-healthy relays | **Amplifies** |
| **H5** | **Recovery gate is a permanent single wedge point.** `run_or_await` never flips `recovery_awaited?` to `true` until `Probe.await_recovery_complete(0)` returns `:ok` (`loop.ex:135-138`). If **any** hub never reaches `:normal`/`:skipped`, every tick re-polls with timeout 0, fails, emits `awaiting_recovery`, and **runs no real heal or health publish — indefinitely**. | `loop.ex:133-149`, `probe.ex:116-166` | One hub stuck non-normal (recovery timeout, missing marker). | **S2** | Low–Med | All self-healing silently disabled; `/healthz` never gets a real publish | Neither (fails dark) |
| **H8** | **Double-heal vs ProcessHub's own migration.** ProcessHub `HotSwap.handle_topology_contraction` auto-redistributes on `:nodedown` ~500 ms after a disconnect; convergence evict+rebind acts ~5 s later off the stale `metadata.tag` (ProcessHub clears `node_pids` but not the tag convergence reads). Mostly converges to the same target (benign), but is redundant work and opens a churn window if the executor reconnects between the two. | `hub_manager.ex` child_spec (HotSwap), `diff.ex:45`, `heal.ex:143` | Executor node leaves then rejoins within ~5 s. | **S3** | Medium | Redundant migration; transient churn | Mild amplify |
| **H7** | **Intent ≈ Reality by construction.** `Intent.declared` derives relays from the same `HubManager.get_all_relays` registry that `Probe` reads, and hard-codes `factory_id: nil` (`intent.ex:34-44`). The **only** genuine divergence the loop detects is `bound_executor` tag vs FleetManager's `active_executors` — i.e. the loop's real job reduces to *executor-liveness reconciliation*, and a read race between two views of one source (C1) is the dominant "divergence" source, not real drift. | `intent.ex:26-47`, `diff.ex:41-53` | Always (design property). | **S3** | — | Most "healing" is reacting to read races, not true drift | Context for C1 |

**Refuted / contained:** `evict_executor` is keyed per-executor in Backoff so it fires once, not
per-relay (H3 partially dampened); `evict_executor` no-ops if the executor is actually live
(`a163a49`, dampens H3); the empty-declared-plugin teardown storm was already fixed
(`diff.ex:114-127`); `resync_for_overwatch` is an async `cast` so it doesn't block the Loop
itself (`recovery.ex`) — but it still triggers real WAL disk I/O on the target Overwatch.

---

## 3. Root-cause verdict: does convergence cause breakouts?

**Yes.** Not as a hypothetical — it has demonstrably done so twice (`336f960`, `77330f2`), and
the structural conditions for more remain. The specific live paths:

1. **C1 → mass migration.** A single transient FleetManager timeout collapses `active_executors`
   to `∅`; with no sanity gate, the entire namespace is classified misbound and mass
   evicted/rebound + WAL-resynced. The cure (the loop) manufactures the outage from a read race.
2. **C3 → freeze ride-through failure.** Serial 1 s probes of N relays mean a slow-storage event
   (the original freeze condition) blocks the Loop for ~N seconds, so the very mechanism meant to
   detect and heal the freeze is itself frozen and deaf to `:output_stall`.
3. **C2 → whole-controller crash.** The tick runs inline in one un-isolated GenServer with default
   restart intensity; one un-`:exit` raise (proven possible) cascades to the top supervisor.
4. **C4 → flap storms.** Brief executor blips trigger full-namespace migration with WAL replay,
   uncontained on first occurrence.

The common root is **acting destructively on partial/proxy observations with no staleness budget,
no sanity gate, and no isolation** — the loop trusts a single tick's read of an unreliable source
and immediately mutates state on it.

---

## 4. Honest design critique

**What's structurally wrong:**

- **Synchronous, serial probing inside the decision process.** Reading reality, deciding, and
  mutating all happen inline in one GenServer. Read cadence is welded to heal cadence, so slow
  reads = slow (and deaf) healing. (C3)
- **Acts on a single partial observation.** `:exit`/empty fallbacks (`probe.ex:82,172,178`) are
  indistinguishable from "the world is genuinely empty." There is **no quorum/sanity gate** and
  **no requirement that a divergence persist K ticks** before a destructive op. One bad read = one
  mass mutation. (C1, H7)
- **Single-process gate + SPOF.** `recovery_awaited?` and the whole tick live in one un-isolated
  GenServer with default restart intensity. (C2, H5)
- **Intent ≈ Reality.** The loop compares two derivations of the same registry; the only real
  signal is executor liveness, so most "healing" is chasing read races. (H7)
- **Output-stall coupling.** Binding decisions are coupled to upstream traffic volume. (H4)
- **Redundant with ProcessHub.** Two independent recovery engines act on the same node-down
  event. (H8)

**What a better design looks like (north-star, not all required for minimal fix):**

- **Separate read cadence from heal cadence**; probe relays **concurrently** (`Task.async_stream`
  with a staleness budget) so one wedged relay can't block the rest or the event loop.
- **Sanity/quorum gate before any destructive op:** if reality looks implausibly empty vs Intent
  (e.g. `active_executors == ∅` while Intent has relays, or `> X%` of relays flip misbound in one
  tick), **skip the destructive plan and re-observe** — log it, don't act.
- **Divergence must persist K consecutive ticks** before a mutating heal (debounce in the Diff/
  Planner, not just per-target Backoff which doesn't gate the first occurrence).
- **Circuit-breaker on mass-op ticks:** if a tick would issue more than a small threshold of
  evict/rebind ops, trip the breaker and alert instead of executing.
- **Isolate the mutating heals** from the read/decide path (or at minimum raise restart intensity
  and wrap the whole tick body) so a heal failure can't crash the controller.
- **Don't couple binding to traffic** — treat output-stall as a softer signal (resume streams in
  place, not migrate) unless binding is independently confirmed broken.

---

## 5. Prioritized fix list (minimal, deletion-funded first)

Each item below is scoped to the smallest diff that removes the biggest real risk. **Per repo
workflow: each fix gets a failing test first that reproduces the bug, then the fix, then proof the
test bites by reverting.** Not implemented.

1. **[C1 — biggest risk, small change] Sanity gate in `Diff.compute`/`Loop.run_tick`.** When
   `active_executors` for a namespace is empty (or implausibly small) while Intent declares relays,
   **suppress the misbound→evict/rebind plan for that namespace this tick** and emit a telemetry
   skip. ~10–15 lines in `diff.ex`/`loop.ex`, no new module. Test: empty `active_executors` +
   non-empty Intent ⇒ `delta.misbound_relays == []` (fails today: it lists every relay).

2. **[C4 — high value] Require misbound to persist K ticks before rebind.** Track a per-relay
   misbound-streak in Loop state; only plan `rebind_relay`/`evict_executor` after the relay has
   been misbound for ≥2 consecutive ticks. Contains flap storms; tiny state addition. Test: one
   tick misbound ⇒ no rebind op; two ⇒ rebind op.

3. **[C2 — cheap hardening] Isolate the tick / raise restart intensity.** Wrap the `run_tick` body
   so any raise (not just `:exit`) is caught, logged, and converted to a skipped tick instead of a
   Loop crash; and/or set explicit `max_restarts` on the Loop child. ~5 lines. Test: a heal op that
   raises ⇒ Loop survives and next tick runs.

4. **[C3 — larger, optional] Concurrent probe with staleness budget.** Replace the serial
   `Enum.reduce` in `Probe.relays` with `Task.async_stream(..., timeout: 1000, on_timeout: :kill_task)`
   so N slow relays cost ~1 s total, not N s, and never block the event loop. Net-neutral LOC.
   Test: K wedged relays ⇒ probe wall-clock ≈ one timeout, not K×timeout.

5. **[H4 — small] Soften output-stall rebind.** Make `:output_stall` resume streams in place /
   require corroboration before a full migrate, so quiet feeds don't churn. Reuses existing
   stream-resume path. Test: stalled relay on a healthy executor ⇒ no migrate op.

6. **[H5 — small] Don't let one stuck hub disable all healing.** Gate per-hub, not globally: heal
   the namespaces whose hubs are normal even if one hub is stuck. Test: one hub stuck +
   one normal ⇒ normal namespace still heals.

Items 1–3 are the **minimal high-impact set** and the recommended first batch. 4–6 are follow-ups.
None require SDK changes (convergence is controller-side; confirmed — `sdk/rust` and `sdk/java`
are untouched by these).

---

## Verification (for when fixes are approved)

- Run targeted unit tests: `mix test test/flezha/convergence/convergence_test.exs` (native; the
  Docker rule is flezha_market-only). Each fix's new test must fail on clean HEAD and pass after.
- The existing `executor disconnect preserve` integration test covers live healing — re-run it
  after C1/C4 to confirm no regression in legitimate rebind.
- Mine `[:flezha, :convergence, :tick]` (`actions_issued`) on a healthy cluster: target is
  ~0 ops/tick. Any steady-state non-zero is a live repro of C1/H7.
