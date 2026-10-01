# Steps operator bench acceptance harness

Repeatable, configurable runs of the steps operator bench
(`mix bench.steps`), with progress streamed as each run lands and a
median summary at the end that flags regressions against a saved baseline.

Everything here drives `mix bench.steps`; the cases only pick scenarios and
sizes. Transport, runner count and SDK are run-time flags, so one case set
sweeps across configurations without editing.

## Quick start

```bash
# fast smoke — does every path run clean? (~3 min)
bench/acceptance/run.sh --profile quick --repeat 1

# the acceptance gate — the numbers to trust (~15–25 min)
bench/acceptance/run.sh --profile standard

# record the current medians as the baseline to compare future runs against
bench/acceptance/run.sh --profile standard --save-baseline

# endurance — sustained integrity over time (~1–2 h)
bench/acceptance/run.sh --profile soak
```

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--profile quick\|standard\|soak` | `standard` | Which case set (`cases.<profile>.tsv`) to run. |
| `--repeat N` | 3 (soak: 1) | Runs per case. The summary takes the **median**, so odd N ≥ 3 tames the box's ±20 % drift. |
| `--runners N` | 2 | Runner (SDK) processes the controller drives. |
| `--transport uds\|quic` | `uds` | Wire transport (sets `FLEZHA_TRANSPORT` for the whole run). QUIC first-connects are slower — the harness widens the connect budget automatically. |
| `--runtime rust\|java` | `rust` | Default SDK. A case can pin its own (the `runtime` column); |
| `--save-baseline` | off | Write the run's medians as the new `baseline.<profile>.tsv` instead of comparing. |

Examples:

```bash
bench/acceptance/run.sh --profile standard --runners 4 --transport quic
bench/acceptance/run.sh --profile quick --runtime java --repeat 1
bench/acceptance/run.sh --profile standard --repeat 5 --save-baseline
```

## What you get

- **While it runs:** one readable line per run —
  `[chain_4 2/3 rust] 90.1 steps/s · p50 194 ms · peak 267 MiB · fsync 17 ms · clean`.
- **At the end:** a median table (throughput, p50, **peak MiB**, **fsync p99
  ms**, integrity, vs baseline) printed and written to `summary.<profile>.md`,
  plus a one-line verdict: `all cases within tolerance` or
  `REGRESSION / INTEGRITY FAILURE`.
- **Resource columns are context, not a gate.** `peak MiB` is the controller
  BEAM's peak memory during the run (a tiny 250 ms sampler process — no
  `/proc`, no observer effect), the real capacity signal; `fsync p99 ms` is
  the slowest Cell's WAL fsync p99 — it tells you when the **disk**, not the
  engine, is the ceiling. Only throughput and integrity gate a regression;
  peak and fsync are reported so you can watch their variance before deciding
  to gate them. Neither covers the runner processes' RAM or any CPU — see the
  limits below.
- **Regression rule:** a case fails if its median throughput is **>15 % below**
  the baseline, or its integrity is not `clean`. 5–15 % shows as ⚠, a gain as ✅.
- **Integrity** is `clean` / `loss` (a missing or duplicated record — always a
  bug) / `stall` (the case did not finish). It is not a throughput judgement;
  a `loss` fails regardless of speed.

Raw per-run rows land in `results.<profile>.tsv` (git-ignored).

## Profiles and how to change them

Each profile is a TSV, one case per line:

```
name<TAB>runtime<TAB>scenario args
chain_4_c50	default	--scenario chain -n 300 --tasks 4 -c 50 --stall-timeout 120
chain_4_java	java	--scenario chain -n 100 --tasks 4 -c 20 --stall-timeout 180
```

- `runtime` is `default` (use `--runtime`) or a pin (`rust` / `java`).
- The args are passed straight to `mix bench.steps`; `--runners` and the
  transport come from the run flags, so don't put them here.
- Add a case by adding a line. If you baseline it, re-run `--save-baseline`.

The three profiles:

- **quick** — small cases, one repeat. A smoke that every scenario (chain,
  turns, sleep) runs clean. Not for numbers.
- **standard** — the acceptance set: the durable chain at several widths, the
  opt-out chain, a Java case, a chat-turn case, and a sleeper case. This is what
  you baseline and gate on.
- **soak** — long-running (a 20 000-instance chain, an hour of chat turns, a
  1 000-sleeper wake soak). Endurance and integrity over time, not throughput.

## Scenarios (from `mix bench.steps`)

- `chain` — spawn N run-once durable chains (`--tasks 1|4|16`, `--tier optout`);
  `-c N` keeps N in flight, `--rate R` spawns R/s. Reports **steps/s** and
  **task commits/s**.
- `turns` — L loops × T turns (`-n L -m T`, `--rate`); reports **turns/s** and
  turn→reply latency. `--kill-runner-at S` SIGKILLs a runner mid-run.
- `sleep` — spawn N loops, park them, then a wake storm; reports the resting
  cost per sleeper and **wakes/s**. `--wake-rate R` paces the storm;
  `--keep` + the `sleep-boot` scenario measures a controller restart.

Run `mix bench.steps` with no matching case to see the full flag list.

## Limits (read before trusting a number)

- **One controller.** This harness runs a single controller (its own BEAM).
  Multiple controllers — the multi-node / partition scale-out story — is a
  different harness (a real cluster); it is the largest untested dimension and
  is not covered here.
- **Shared box.** The controller, runners and any Kafka broker share one
  machine, so numbers drift ±20 %. Always compare **medians** (`--repeat`),
  never single runs, and keep the box quiet during a gate run.
- **The barrier ceiling.** Until `workflow-durable-path-throughput` lands, the
  task-commit path is one fsync per barrier on the runner's control channel
  (~350 commits/s on two runners) and the loop journal is never cut, so
  high-rate `turns` cases can stall on the O(history) per-turn cost. The
  profiles keep rates modest for that reason; a storm case is expected to be
  slow, not a regression, until that change is in.
- **QUIC** works but first-connects are slower than UDS; the harness gives it a
  wider connect budget, but a cold run may still need a retry under load.
- **Resources are partial.** `peak MiB` is the controller BEAM only (not the
  Rust/Java runner processes' RSS), and there is **no CPU** metric — both were
  deliberate: runner RSS and CPU on a shared box are noisy and would need a
  `/proc` sampler with an observer effect. Peak BEAM memory and fsync p99 are
  in-VM, stable, and near-free, so they were worth adding; the rest was not.

## Files

| | |
|---|---|
| `run.sh` | the runner (flags above) |
| `summarize.exs` | median + baseline compare + regression verdict |
| `cases.{quick,standard,soak}.tsv` | the case sets |
| `baseline.<profile>.tsv` | saved medians (commit this to gate against it) |
| `results.<profile>.tsv`, `summary.<profile>.md` | per-run output (git-ignored) |
