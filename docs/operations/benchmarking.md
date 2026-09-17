# Benchmarking

For contributors running and interpreting Flezha benches: the `mix bench`
harness and its `./scripts/bench.sh` wrapper, the `mix bench.workflow`
workflow bench, the Rust SDK Criterion microbenches, the controller-side
microbenches, the recorded floors, the CI jobs that run benches, and how to
triage a failed `bench-regression` job.

- Single entry point: `./scripts/bench.sh` (wraps `mix bench`, sets
  `FLEZHA_TRANSPORT`, defaults `MIX_ENV=prod` and
  `FLEZHA_DURABILITY_BACKEND=none`).
- Independent axes: **runtime** (rust/java/elixir) × **transport**
  (uds/quic/native) × **shape** (regression/entity/dynamic).
- No CI job fails on an absolute throughput number; see [CI gates](#ci-gates).
- Two different kinds of floor live here: the **absolute** reference figures
  under [Recorded floors](#recorded-floors), which are informational, and the
  **measured run-to-run spreads** that decide whether a row can resolve an
  effect at all, under [What a row can resolve](#what-a-row-can-resolve).

## Quick start

```sh
./scripts/bench.sh                                # rust + uds, 100 operatives x 100 msg
./scripts/bench.sh -n 100 -m 10000                # the CI job's dimensions
./scripts/bench.sh -r java                        # java SDK
./scripts/bench.sh -t quic                        # quic transport
./scripts/bench.sh -r java -t quic -n 100 -m 1000 # any combination
./scripts/bench.sh --shape entity --pool-sizes 1,8
./scripts/bench.sh --help
```

Compare two runs — the controller, protocol and measurement code are identical,
only the launched SDK process and the wire change, so the throughput rows are
directly comparable:

```sh
./scripts/bench.sh -r rust -n 100 -m 1000 | tee bench-rust.txt
./scripts/bench.sh -r java -n 100 -m 1000 | tee bench-java.txt
diff bench-rust.txt bench-java.txt
```

Direct `mix bench …` takes the same flags; the script only sets the
environment and forwards them.

## Axes

| Flag                               | Values                                      | Effect                                                                                                                                                                                                                                                                      |
| ---------------------------------- | ------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-r` / `--runtime`                 | `rust` (default), `java`, `elixir`          | Which SDK runner is launched. Each runner is launched from its test app's manifest, `bench` profile (`test/apps/<runtime>_runner/conformance.json`), which also names the build command for a missing artifact — `java` wants `mvn -f test/apps/java_runner/pom.xml package`, `elixir` on `uds`/`quic` wants `cd test/apps/elixir_runner && mix deps.get && MIX_ENV=prod mix compile`. On `native` the Elixir SDK starts in-process, with no spawn. |
| `-t` / `--transport`               | `uds` (default), `quic`, `native`           | Sets `FLEZHA_TRANSPORT`. QUIC generates self-signed certs at `app_data/certs/` on first run. `native` is the in-process Elixir SDK and requires `--runtime elixir`.                                                                                                          |
| `-s` / `--shape`                   | `regression` (default), `entity`, `dynamic` | The workload (below). `entity` ignores `-n`/`-m`.                                                                                                                                                                                                                           |
| `-n` / `--num-operatives`          | int (default 100)                           | Operative count, `regression` and `dynamic`.                                                                                                                                                                                                                                |
| `-m` / `--messages-per-operative`  | int (default 100)                           | Messages per operative, `regression` and `dynamic`.                                                                                                                                                                                                                         |
| `--pool-sizes`                     | comma list (default `1,8`)                  | Worker-pool sizes to sweep, `entity` only; each spawns fresh runners with `FLEZHA_WORKER_POOL_SIZE=<N>`.                                                                                                                                                                   |
| `-w` / `--warmup`                  | int (default 10, `0` disables)              | Untimed messages per operative drained before the measured phase, so runner cold start (class loading, JIT, lazy codec init) stays out of the numbers.                                                                                                                      |

Runner placement: the Rust runtime reuses runners already connected in the
`default` namespace, otherwise (and always for `java`/`elixir`) two runners
are spawned into the scratch namespace `bench`, which the task wipes
(`app_data/state/bench`, `app_data/process_hub/bench_hub`) before the
controller boots. A Rust runner binary is built on demand; build
`cargo build -p rust_runner --release` first for representative numbers.

## What a run prints

Every run opens with three header blocks, then any preflight warning, then the
result tables.

- **run** — the runtime and its dispatch model (read from the SDK's
  `conformance.json` `dispatch` key, with the current value of every
  environment variable that selects the topology), whether the runner is
  embedded or spawned, the transport, shape, durability backend, dimensions
  and `MIX_ENV`.
- **system** — CPU model, logical cores and governor; memory; 1-minute load
  average; OTP/Elixir and the launched runtime's own toolchain version; the
  git SHA and whether the tree is dirty. A field the host cannot supply reads
  `unavailable`; nothing here can fail a run.
- **artifacts** — the artifact each runtime actually launched, its size and
  mtime, whether it was the profile's first choice or a fallback, and whether
  the manifest `build` command ran or was skipped.

### Preflight warnings

Printed above the numbers and carried in the report. **None of them abort the
run** — a number produced on an unfit box is still that box's number, and the
report says what was wrong with it.

| Warning                   | Why it matters                                                       |
| ------------------------- | -------------------------------------------------------------------- |
| CPU governor not `performance` | Clock scaling makes the number irreproducible run to run.        |
| Load average above cores  | The box is contended; the bench is not the only thing running.        |
| `MIX_ENV` not `prod`      | The controller is not running the code it ships.                      |
| Dirty working tree        | The number cannot be attributed to a commit.                          |
| `FLEZHA_BENCH_SKIP_BUILD` | The artifact measured is whatever was on disk, not necessarily this tree. |
| Fallback artifact         | The profile's own artifact is missing; a different build was launched. |
| Debug-profile artifact    | Not a bench artifact.                                                 |
| Durability backend off    | The number excludes the cost of persisting anything (the wrapper's default). |

### Throughput windows

Two rates are reported and each row names the window it measures. They differ
by the send window and are **not** interchangeable:

- **end-to-end (first input → last response)** — the **headline**. Responses
  stream in while the send loop is still casting, so this is the whole cost of
  moving a message through the system.
- **receive only (send loop returned → last response)** — the drain rate. It
  credits work the send window paid for, so it reads several times higher; it
  is the window closest to *SDK dispatch*, because the send window it excludes
  is the controller's own cast overhead.

There is no `Average time per operative`: it divided total elapsed time across
four unrelated phases by a count of concurrently-running operatives and
measured nothing.

### The structured report

Every run writes `bench/reports/bench-<runtime>-<shape>-<run_id>.json`.
It carries the `run`, `system`,
`artifacts` and `warnings` blocks plus a `metrics` block with every measured
value.

**The report is the machine contract.** CI reads its named fields rather than
matching printed prose, so the rendering above can change without touching a
CI job: `metrics.throughput_msg_per_sec` (the end-to-end headline) and
`metrics.receive_window_msg_per_sec` for the regression run,
`metrics.scaling_ratio` for the entity sweep.
`bench/reports/` is gitignored.

`FLEZHA_BENCH_LOG` sets the logger level for a run (default `warning`).
Nothing in the harness filters or rewrites a log line: a warning a bench
provokes is either a real condition to fix at its source, or evidence that a
classifier is wrong.

## Shapes

### `regression` (default)

`-n` operatives × 1 procedure × 1 entity × `-m` msg, factory `do_nothing`,
end-to-end through controller + NIF + SDK. The `bench-regression` CI job runs
it at `-n 100 -m 10000` and reports both windows; no rate fails the job.

### `entity` (scaling check, informational)

8 operatives × 20K msg against factory `entity_stress` (4 procedures, 8 entity
keys, fan-out then fan-in). Sweeps `--pool-sizes` and prints a `scaling` table
ending with `ratio (max/min)`; the same ratio is the report's
`metrics.scaling_ratio`, which the informational CI job reads.

A small worker pool cannot drain this shape's 8× fan-out: on a 4-core box
`--pool-sizes 1` and `2` stall partway through the drain and the run times out,
while `8` completes. Sweep sizes the host can actually serve.

```sh
./scripts/bench.sh --shape entity --pool-sizes 1,8
```

### `dynamic` (keyed steady state)

The `regression` harness with factory `dynamic_entity` and keyed inputs
(`k<i>:<n>`, 8 keys per operative); each input reaches one lazily created
Entity per key.

```sh
./scripts/bench.sh --shape dynamic -n 100 -m 10000
```

### Elixir runtime

The `elixir` runtime runs all three shapes over all three transports, which
isolates two axes:

- **elixir × native vs elixir × uds** — a same-runtime transport measurement
  (in-node message passing vs the socket stack).
- **elixir × uds vs rust × uds** — a same-transport runtime measurement (BEAM
  dispatch vs the Rust engine).

No CI job benches the elixir runtime. Use `-n 100 -m 10000` after
pre-building the standalone app; small runs are setup-dominated.

## Workflow bench (`mix bench.workflow`)

Measures the paths a stream never takes: instance lifecycle (spawn → chain →
completion), the durable proc barrier, the chat turn (iteration commit),
suspension and the wake paths. Runs in the scratch namespace `bench_wf`
against `--runtime rust|java` (the Elixir SDK has no workflow operatives).

```sh
mix bench.workflow --scenario chain -n 1000 --procs 4 -c 100
mix bench.workflow --scenario chain -n 2000 --procs 4 --rate 200
mix bench.workflow --scenario turns -n 100 -m 100 --rate 1000
mix bench.workflow --scenario turns -n 100 --duration 600 --rate 500 --kill-runner-at 120
mix bench.workflow --scenario sleep -n 10000 --wake-after-ms 15000
mix bench.workflow --scenario sleep -n 10000 --keep
mix bench.workflow --scenario sleep-boot -n 10000
```

| Scenario     | What it measures                                                                                                                                                                                                                |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `chain`      | Spawn `-n` run-once workflows of `bench_chain_<procs>` (`--procs 1\|4\|16`; `--tier optout` picks `bench_chain_optout_4`, the persistence-off twin), each seeded with `"<seq>,<send_ts_ns>"` passed through to the sink. `-c N` keeps N in flight (closed loop), `--rate R` spawns R/s (open loop); neither = one burst. |
| `turns`      | Spawn `-n` `bench_loop_workflow` instances, send `-m` turns each round-robin (`--rate` total turns/s, else unpaced under an in-flight cap) and measure the reply. `--duration S` sizes `-m` from the rate; `--kill-runner-at S` SIGKILLs one runner S seconds in. |
| `sleep`      | Spawn `-n` loops, one turn each, wait until every instance has parked; report resting cost per sleeper and the convergence tick time, then a message wake storm (`--wake-rate R` paces it, `--no-wake` skips it) and, with `--wake-after-ms`, a timer wake storm. `--keep` leaves the namespace for `sleep-boot`. |
| `sleep-boot` | Boot over the namespace `sleep --keep` left: time until every sleeper is back as a suspended Overwatch, then the same wake storm.                                                                                               |

Other switches: `--runners` (2), `--park-idle-ms` (500 for the sleep
scenarios, else the SDK's 5000), `--warmup` (chain: `min(20, n)`, else 0),
`--spawn-concurrency` (1000 starts in flight for a burst),
`--status-every S`, `--stall-timeout S` (60). `BENCH_LOG_LEVEL` (default
`warning`) sets the controller log level.

## SDK-internal microbenches (Criterion, no controller, no NIF)

All but `state` need `--features bench-internals`.

| Bench                         | Ids                                                                                                                 | Measures                                                                                          |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `correctness`                 | `throughput_single_entity`, `throughput_static_fanout`, `throughput_effectively_once_tiers/{tier_off,tier_on}`      | Per-message dispatch budget on a pinned producer thread; 4-key static fan-out; the effectively-once tier's cost. Invariant violations fail the run. |
| `dispatch`                    | `operative_dispatch/regression_100x1x1x10k`, `entity_stress_8x4x8x20k`                                              | Criterion mirrors of the `mix bench` shapes. The worker pool is process-global and sized once — sweep by re-running under different `FLEZHA_WORKER_POOL_SIZE` values. |
| `dynamic_entity_steady_state` | `dynamic_entity/steady_state_1k_entities`                                                                           | 1,000 active dynamic Entities, no eviction, 200K inputs round-robined: one `entity_key` call plus one `DashMap` lookup per input. |
| `hybrid_channel_throughput`   | `pure_inter_procedure_1c`, `pure_inter_procedure_3c`, `pure_plugin_bound`, `hybrid_1c`, `hybrid_3c`                 | Per-emit cost of hybrid (`sink_with`) channels vs pure inter-procedure and pure plugin-bound ones. |
| `state`                       | `state_apply/{small,large}_{no_key,keyed}_{no_prealloc,prealloc_1024}`                                              | The `state.apply` hot path — one `apply_change` plus one `Vec::push`, no serialization or I/O.   |

```sh
cargo bench --bench correctness -p flezha_runner --features bench-internals -- throughput_single_entity
cargo bench --bench dispatch -p flezha_runner --features bench-internals
FLEZHA_WORKER_POOL_SIZE=1 cargo bench --bench dispatch -p flezha_runner --features bench-internals
cargo bench --bench dynamic_entity_steady_state -p flezha_runner --features bench-internals
cargo bench --bench state -p flezha_runner
```

Use them to localise a regression: a `mix bench` rate that dropped while
Criterion `regression_100x1x1x10k` held puts the regression in the
transport/controller path, not the SDK.

State-apply budget ceilings (median per call, reference host):

| Workload                      | Median   |
| ----------------------------- | -------- |
| small, no key, no prealloc    | < 200 ns |
| small, keyed, prealloc 1024   | < 100 ns |
| large, any key, any prealloc  | < 500 ns |

No CI workflow runs the `state` bench; the ceilings are checked by hand.

## Controller-side microbenches (Elixir, no runner)

```sh
# Plugin hook hot path (`Extension.run/3` at 0/1/5 hooked plugins) plus the
# Kafka chain mailbox-depth check (depth must stay <= 1). Default 200_000 iterations.
mix bench.extension [--iterations N]

# Observability bus emit against zero interested sinks (ns per emit, default 1_000_000).
mix flezha.bench.observability [iterations]

# Credit-grant back-pressure soak (long-running, opt-in).
MIX_RUN_CREDIT_SOAK=1 mix test test/load/credit_soak_test.exs
```

Each task's `@moduledoc` carries the workload details and pass criteria.

## Recorded floors

Reference figures for triage. No CI job reads them or fails on them.

| Constant                          | Value           | What it measures                                                                                       |
| --------------------------------- | --------------- | ------------------------------------------------------------------------------------------------------ |
| `SINGLE_ENTITY_FLOOR_MSG_PER_SEC` | **700000** msg/s | `cargo bench … throughput_single_entity`. Informational; used to triage where a regression sits.       |

`SINGLE_ENTITY_FLOOR_MSG_PER_SEC = 700_000`

Recording method: median of 3 runs on the reference host class (modern x86_64
desktop, 8+ cores): 714K elem/s for the single-entity microbench, recorded as
700 000.

The dynamic-entity steady-state floor (`steady_state_1k_entities`) is
**~520K inputs/s** (median ~383 ms for 200K inputs) on the reference host. It
is a trend signal, not a CI gate.

## CI gates

| Workflow                    | Trigger                                                                                                                                                            | What it runs                                                                                                                                                                                                                                                                                  |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bench.yml`                 | PRs touching `sdk/rust/**`, `flz_transport/**`, `flz_nif/**`, `lib/flezha/**`, `lib/mix/tasks/bench**`, `protobufs/**`, `test/apps/rust_runner/**`, the workflow | **`bench-regression`**: `mix bench --runtime rust --shape regression -n 100 -m 10000`; reads `metrics.throughput_msg_per_sec` and `metrics.receive_window_msg_per_sec` from the run's JSON report (not from printed output) and writes both to the job summary, re-emits every preflight warning as a job annotation, and uploads `bench/reports/` with the log. No throughput number fails it: it fails only when the bench run fails, writes no report, or writes one with no rate. **`bench-entity-scaling`** (informational, `continue-on-error`): `--shape entity --pool-sizes 1,8`, reads `metrics.scaling_ratio`; GitHub runners do not reliably expose 8 physical cores, so the 4× target is validated on real hardware. |
| `rust-sdk-perf.yml`         | PRs and `main` pushes touching `sdk/rust/**`, `flz_transport/**`, `Cargo.toml`, `Cargo.lock`                                                                         | Runs the `correctness` bench (any invariant violation fails) and the `dispatch` bench with `--save-baseline`; on PRs compares against the cached `main` baseline and fails if any bench's estimated change exceeds 10%.                                                                      |
| `rust-sdk-bench-cron.yml`   | Mondays 03:00 UTC, `workflow_dispatch`                                                                                                                              | Non-blocking drift detector: `mix bench -n 100 -m 10000` across rust/java × uds/quic, plus Criterion `dispatch` (`regression_` group only) and `dynamic_entity_steady_state`; posts a step summary.                                                                                           |

## Settling a series

**The first run of a series is not a result on every instrument — and which
instruments it is not a result on was measured, not assumed.** A figure taken
from a series that contains its own warm-up is not a floor. On
`mix bench.workflow` the first run is roughly four times the plateau, so six
consecutive runs of one identical arm spread 316 % while the four plateau
samples spread 1.3 %.
On `mix bench.extension`, on the same box in the same week, the first run sits
inside the plateau. So this is a property of the driver, not of the machine, and
it is stated per instrument.

Every recorded number, on any instrument:

1. **Discard until that instrument's plateau.** The table says how many runs.
2. **Interleave the arms of a comparison** — one round of A, one of B, repeated —
   never all of A and then all of B. A series whose arms are not interleaved can
   read a warm-up as an effect.
3. **Report how many plateau samples the figure came from.** A number with no
   sample count cannot be told from a single run.
4. **Say whether the row can gate at all.** Settling removes the warm-up; it does
   not shrink the noise floor. A row whose floor exceeds the effect being claimed
   is a *direction* and blocks nothing — the floors are below.
5. **If the comparison crosses runtimes, size its warm-up for the slowest-warming
   one** and use that value on every arm — see *Comparing two runtimes*.

| Instrument | First run | Discard | The measurement behind it |
| --- | --- | --- | --- |
| `mix bench.workflow --scenario chain --procs 4 -n 1000 -c 100` | ~4x the plateau, and run 2 is still ~1.4x | first **2** | Six runs of one identical arm on a quiet box: 94.5 / 31.8 / 23.0 / 22.9 / 23.0 / 22.7 workflows/s. **316 %** over the six; **1.3 %** over the four plateau samples. An earlier five-run series read 93.2 / 33.3 / 23.1 / 24.2 / 23.1 — the same shape. |
| `mix bench.extension` | inside the plateau | **none** | Five runs: run 1 within **3.6 %** of the plateau on the four rows above 100 ns. The 60 ns row spreads 16.7 % and resolves nothing. |
| `./scripts/bench.sh -n 100 -m 10000` (the `bench-regression` workload) | not separated from the noise | first **1** | Five runs: receive window spread **12.1 %**, end-to-end **5.1 %**. |

**Two runs, not one, is what the workflow measurement establishes.** Run 1 sits
at 4.1x the plateau and run 2 at 1.4x, so a series that discards only its first
sample still carries a 39 % warm-up into its floor. The four plateau samples
above spread 1.3 %, taken with the 1-minute load average climbing 1.2 to 9.4
across the series — the plateau is the *loaded* steady state, and it is what a
comparison on this driver must be taken against, on both arms.

**Two explanations for the workflow transient were refuted, which is what makes
the number usable.** A cold run of the same arm after 4.5 minutes idle came back
at 91.1 workflows/s with an identical state directory, so nothing accumulates on
disk or in the namespace. `package_throttle_count` was flat before the cooldown,
after the idle and after the cold run, so package thermal throttling is not it.
What remains is the box's sustained-load clock behaviour under the `powersave`
governor, which is not separated — and does not need to be, because the operating
rule is the same either way.

### What a row can resolve

Every floor below is a measured run-to-run spread on the reference box
(i7-8650U, `powersave`, on mains), and it is the number that decides whether a
result from that row means anything. A row whose floor exceeds the effect being
claimed is a direction, not a gate: do not run it as one, and do not report its
single-run number as a verdict.

| Row | Floor | Verdict | Runs / measured |
| --- | --- | --- | --- |
| Rust typed dispatch | 0.33 % | gate, on a cold box only | run count unrecorded, 2026-08-28 |
| Rust entity stress | 0.95 % | gate, on a cold box only | run count unrecorded, 2026-08-28 |
| `throughput_effectively_once_tiers`, tier off / on | 9.04 % / 6.01 % | direction only | unrecorded |
| `host_loop` (erased) | 4.13 % | guard | unrecorded |
| whole-system round trip | ~18 % | direction only | 5 s runs within one arm, no settled floor |
| veneer round trip (node) | 11.89 % | paired rounds only | single-run floor |
| `bench-regression` receive window / end-to-end | 12.1 % / 5.1 % | see below | 5 runs, 2026-08-30; confirmed 2026-09-03 |

**The two gate floors hold on a cold box and not on a working one.** Both were
taken on an idle machine, and they did not survive a session that had just run
two workspace test passes and a system suite: two arms carrying *identical*
production code separated **1.93 %** on the typed-dispatch row in interleaved
rounds, and one arm spanned 5.6 % within itself. So a 0.33 % floor is not a
licence to act on a 1 % difference measured mid-session. Either measure on a
cold box, or carry a same-code control arm and read the effect against that.

**That last row is the one to read carefully.** The **receive** window spreads
12.1 % run-to-run here and the end-to-end window less than half that on the same
workload, so on this workload the end-to-end window is the one that can resolve
a difference. Neither is a dependable absolute figure: this box read 518 872 on
the receive window at its slowest on 2026-08-30, and every one of fourteen runs
on 2026-09-03 came in more than 16 % below that, so an absolute rate from this
row is a statement about the host it ran on and not about the tree.

**The last row was re-derived once and held.** Five `-w 1000` runs of the rust
arm on 2026-09-03, taken while the box was working rather than idle, spread
**11.21 %** on the receive window and **4.18 %** on the end-to-end window —
inside the floors recorded three days earlier from an independent five-run
series.

**A blank in the last column is a gap, not a small number.** Those floors are
inherited from the campaigns that measured them; the run count and date were
never written down, so nobody can tell a five-run floor from a two-run one.
Re-deriving each needs its own paired campaign, and a row is re-recorded here
with its runs and its date when one is taken.

### Comparing two runtimes

**Report every window and let the resolvable one decide.** The floor rule above
applies to an arm-to-arm difference exactly as it does to a gate, and on this
workload that means the receive window is the one that cannot decide. Which window
is quieter is a measured property of the workload, not of what the window means,
so read it off the table rather than guessing.

**Warm up for the slowest-warming runtime, and use the same value on both
arms.** The `-w` / `--warmup` row of *Axes* above says warm-up exists so "runner
cold start (class loading, JIT, lazy codec init) stays out of the numbers", and
the default is ten untimed messages per operative
(`lib/mix/tasks/bench.ex:93`). Ten activations do that for a natively compiled
runner and cannot do it for a JVM, whose hottest tier does not engage until
thousands of invocations — so the default warms a `rust` arm fully and a `java`
arm barely, and reports the difference as though it were the effect. Pass
`-w 1000` to both arms.

The dimensions carry the same asymmetry. At `-n 100 -m 10000` an un-warmed
prefix is a small share of a million measured messages; at the CI default of
`-m 100` it dominates. **A cross-runtime comparison is never taken at the CI
default dimensions.**


### Two ways an A/B harness reports its own shape as an effect

Both were measured on `bench-regression` while comparing two controller trees, and
both produced a *consistent, statistically strong* reading of an effect that could
not exist.

**Keep the build out of the timed run.** A harness that switches arms by editing a
source file and then calls `./scripts/bench.sh` puts `mix compile` inside the run's
wall clock. Under the `powersave` governor sustained load *raises* the clock, so the
arm whose source file is larger warms the CPU more immediately before measuring and
reads faster. One arm 66 source lines larger measured **+11.0 % in 6 of 6 rounds**
(sign test p ≈ 0.0014) for a change that adds no work to the measured path. Compiling
before the run and settling a fixed idle first dropped the same comparison to +5.8 %,
and swapping the arms' *whole* file set rather than one file dropped it to **+0.1 %**.

**A three-run round gives its majority arm two of the three positions.** Position
dominates this workload — the runs inside a round are not exchangeable — so pooling
an `A B A` / `B A B` design credits the majority arm with the position effect.
Compare position-matched, or use two-run rounds with strict alternation.

**Carry a same-code control inside each round**, per *What a row can resolve* above.
Two runs of byte-identical code inside one round spread **0.6 %–7.3 %** at
`-n 100 -m 2000` and **1.3 %–2.4 %** at `-n 1000` (2026-09-03, on battery under
`powersave`). Any arm-to-arm gap smaller than that band is not an effect, whatever
its sign or consistency.

### Measuring a VM flag

An arm is one value of `ERL_FLAGS` on the `mix bench` process, which **is** the
controller BEAM. Nothing else differs: the runner is a separate Rust binary that
ignores the variable, so the arms share one tree and one runner and the harness
cannot report its own shape as an effect. Round structure is the arms forward
then reversed, so every arm takes an early and a late position inside the same
round, and a round's arm-minus-control delta carries the same drift in both
terms.

**Measure CPU, not only throughput. On this workload it is the instrument that
can see.** Wrapping the run in `/usr/bin/time` and reading user + sys costs
nothing and, measured over the control arm, is three to five times quieter than
either throughput window:

| Metric | Control-arm CV | Runs |
|---|---|---|
| end-to-end window | 5.1 % | 16, 2026-09-08 |
| receive window | 14.9 % | 16, 2026-09-08 |
| CPU seconds, whole process tree | **3.2 %** | 16, 2026-09-08 |

Taken on battery under `powersave`. A mains series has not been run; what would
move is the absolute rate and, with it, how much of each arm's difference is
wake-up latency rather than work.

The first two reproduce the floors recorded above from an independent series
three days earlier, which is the check that the third is worth trusting.

**Results, `-n 100 -m 2000 -w 100`, rust on uds, 8 and 6 paired rounds
(2026-09-08, **on battery** under `powersave`).** Deltas are per-round paired
means against the control arm. The power state is stated because the absolute
floors above were recorded on mains and this box boosts differently on each; the
arms were interleaved inside every round, so a comparison here is protected
against drift but the absolute rates are not comparable with a mains series.

| Arm | CPU | end-to-end | receive | wall clock |
|---|---|---|---|---|
| `+sbwt short` ×3 | **−20.9 %** (6/6) | −1.1 % | +7.7 % | +0.96 s |
| `+sbwt none` ×3 | **−21.9 %** (6/6) | +0.1 % | +9.5 % | +2.17 s |
| `+sbwtdio none` alone | −4.5 % (6/6) | −0.4 % | +2.4 % | +0.07 s |
| `+sbwt very_long` ×3 | **+32.7 %** (8/8) | −3.6 % (8/8) | −7.6 % (8/8) | +0.21 s |
| `+SDio 4` | +1.1 % | +0.1 % | −2.8 % | +0.09 s |

Only the bold figures clear their metric's floor. **Neither throughput window
resolved anything**, including for the arm that burns a third more CPU, which is
the clearest statement available of what these rows can and cannot decide.
`very_long`'s throughput direction agreed in 8 of 8 rounds on both windows and
is still inside the floor: a direction, not a number.

**The wall-clock column is where the trade-off lives**, and it is invisible in
both throughput windows because it falls outside them. Sleeping schedulers cost
wake-up latency on sparse, request-response work and nothing on saturated work.
Per-phase, against control:

| Phase | `+sbwt short` | `+sbwt none` |
|---|---|---|
| operative creation | +3.5 % | +23 % |
| plugin setup | +43 % | +204 % |
| warmup | +28 % | +122 % |
| receive (saturated) | −7 % | −8 % |

That table is why the release ships `short`: one point of CPU is not worth four
times the setup latency. A bench that had reported only its two throughput
windows would have called all four arms a tie.

**`+swt` was not measured, and the series above is the reason.** The wake-up
threshold is worth a run when a workload is wake-up bound, and the candidate for
that was `+sbwt very_long`: it spins the schedulers hardest, so if wake-up
latency were costing throughput it is the arm that would buy it back. It bought
nothing on either window while burning a third more CPU, and the flat arms
either side of it agree. A flag whose only lever is the one already shown to
move nothing here does not get a series; it gets this paragraph.

**A capacity flag is measured in memory, not in throughput.** `+Q` sizes the
port table, which is allocated at startup and does not depend on load, so a
bench cannot decide it and does not need to. Peak RSS of a bare node, five runs
each on OTP 29 (default port limit 1 048 576):

| Port limit | Peak RSS | Against the default |
|---|---|---|
| default (1 048 576) | 69.9 MB | |
| `+Q 262144` | 58.1 MB | −11.8 MB |
| `+Q 65536` | 55.0 MB | **−14.9 MB** |

That is about 16 bytes a slot, and it is why the release pins the ceiling at the
value the image's own ERTS already defaults to rather than at the larger one: on
the image the pin costs nothing, and on a newer ERTS it returns the difference.
The same measurement on the CLI's node, which the release sizes separately, is
64.9 MB down to 49.6 MB with a startup 11% shorter (5 runs each).

## Triage when `bench-regression` fails

No throughput number fails the job. It fails in three ways, and the uploaded
artifact (`bench-regression.log` and `bench/reports/`) carries the evidence for
each:

1. **The bench run failed.** A build that failed, a runner that never
   connected, every operative start refused, or `Timeout waiting for messages`.
   The reason is in `bench-regression.log`, and the preflight warnings at the
   top of the run say what was unusual about the host.
2. **No report was written.** The step's `ls` of `bench/reports/` fails: the
   run ended before it wrote its report. Read the log for why.
3. **The report carries no rate.** The job names the report and prints the last
   50 lines of the log.

### Localising a throughput drop

A rate that moved fails nothing, but when the job summary shows one drop:

1. **Transport / NIF changes?** If the PR touches `flz_transport/`, `flz_nif/`,
   `flz_core/src/command/streams_connect.rs` or the framing path, compare
   `cargo bench --bench dispatch -- regression_100x1x1x10k` to the `mix bench`
   number. Criterion steady while `mix bench` dropped → transport/controller.
2. **SDK dispatch changes?** If the PR touches
   `sdk/rust/src/operative/dispatcher.rs`, `flz_core/src/routing.rs` or
   `flz_core/src/entity.rs`, run `throughput_single_entity` against
   `SINGLE_ENTITY_FLOOR_MSG_PER_SEC`. A regression here is the dispatch hot
   path itself.
3. **Runner drift?** Rerun the job on `main`. If `main` reads the same on the
   same runner image, the runner pool shifted, not the tree.
4. **Entity ratio dropped?** If `bench-entity-scaling`'s `ratio (max/min)` fell
   from ~4× to below 2× on the same PR, the regression is in the entity
   fan-out, not single-entity dispatch.

## Round-trip bench

`mix bench.roundtrip` — the Kafka round trip that measures generator → `in`
topic → engine → `out` topic → collector — is development material kept out of
this repository, so a fresh clone carries neither the task nor the compose stack
it drives.

## See also

- [`../sdk/rust_operative_architecture.md`](../sdk/rust_operative_architecture.md) — dispatch hot path, entity mailboxes, poll workers.
- [`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) — the wire format `mix bench` exercises.
- [`../sdk/elixir_sdk.md`](../sdk/elixir_sdk.md) — the in-process `native` transport the elixir runtime measures.
- [`../sdk/conformance.md`](../sdk/conformance.md) — the manifests both bench profiles and the e2e harness launch from.
