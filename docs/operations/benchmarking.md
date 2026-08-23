# Benchmarking

For contributors running and interpreting Flezha benches: the `mix bench`
harness and its `./scripts/bench.sh` wrapper, the `mix bench.workflow`
workflow bench, the Rust SDK Criterion microbenches, the controller-side
microbenches, the recorded floors the CI gates parse, and how to triage a red
gate.

- Single entry point: `./scripts/bench.sh` (wraps `mix bench`, sets
  `FLEZHA_TRANSPORT`, defaults `MIX_ENV=prod` and
  `FLEZHA_DURABILITY_BACKEND=none`).
- Independent axes: **runtime** (rust/java/elixir) × **transport**
  (uds/quic/native) × **shape** (regression/entity/dynamic).
- The recorded floors are the two literal lines under
  [Recorded floors](#recorded-floors); `mix bench.floor` and CI parse them
  from this file.

## Quick start

```sh
./scripts/bench.sh                                # rust + uds, 100 operatives x 100 msg
./scripts/bench.sh -n 100 -m 10000                # the CI gate's dimensions
./scripts/bench.sh -r java                        # java SDK
./scripts/bench.sh -t quic                        # quic transport
./scripts/bench.sh -r java -t quic -n 100 -m 1000 # any combination
./scripts/bench.sh --shape entity --pool-sizes 1,8
./scripts/bench.sh --help
```

Compare two runs — the controller, protocol and measurement code are identical,
only the launched SDK process and the wire change, so the `Throughput:` line
is directly comparable:

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
| `-r` / `--runtime`                 | `rust` (default), `java`, `elixir`          | Which SDK runner is launched. `java` needs `test/apps/java_runner/target/java_runner.jar` (`mix test.java_sdk` or `mvn -f test/apps/java_runner/pom.xml package`). `elixir` on `uds`/`quic` spawns `test/apps/elixir_runner` — pre-build it: `cd test/apps/elixir_runner && MIX_ENV=prod mix compile`; on `native` it starts the SDK in-process. |
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

## Shapes

Every shape prints `Throughput:` (responses over the receive window — the
gate metric) and `End-to-end (first send -> last response)` (the honest
number, since responses stream in while the send loop is still casting).

### `regression` (default, merge-blocking)

`-n` operatives × 1 procedure × 1 entity × `-m` msg, factory `do_nothing`,
end-to-end through controller + NIF + SDK. The `bench-regression` gate runs it
at `-n 100 -m 10000`.

### `entity` (scaling check, informational)

8 operatives × 20K msg against factory `entity_stress` (4 procedures, 8 entity
keys, fan-out then fan-in). Sweeps `--pool-sizes` and prints a
`=== Scaling Summary ===` block ending with `ratio (max/min)`.

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

There is no CI floor for the elixir runtime. Use `-n 100 -m 10000` after
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

Use them to localise a regression: `mix bench` red with Criterion
`regression_100x1x1x10k` green puts the regression in the transport/controller
path, not the SDK.

State-apply budget ceilings (median per call, reference host):

| Workload                      | Median   |
| ----------------------------- | -------- |
| small, no key, no prealloc    | < 200 ns |
| small, keyed, prealloc 1024   | < 100 ns |
| large, any key, any prealloc  | < 500 ns |

No CI workflow runs the `state` bench; the ceilings are checked by hand.

## Controller-side microbenches (Elixir, no runner)

```sh
# Plugin extension hot path (`Extension.run/3` at 0/1/5 subscribers) plus the
# Kafka chain mailbox-depth check (depth must stay <= 1). Default 200_000 iterations.
mix bench.extension [--iterations N]

# Observability bus emit against zero interested sinks (ns per emit, default 1_000_000).
mix flezha.bench.observability [iterations]

# Credit-grant back-pressure soak (long-running, opt-in).
MIX_RUN_CREDIT_SOAK=1 mix test test/load/credit_soak_test.exs
```

Each task's `@moduledoc` carries the workload details and pass criteria.

## Recorded floors

| Constant                          | Value           | What it gates                                                                                          |
| --------------------------------- | --------------- | ------------------------------------------------------------------------------------------------------ |
| `BENCH_FLOOR_MSG_PER_SEC`         | **480000** msg/s | The `Throughput:` line of `mix bench --runtime rust --shape regression -n 100 -m 10000`. Merge-blocking via `bench-regression`. |
| `SINGLE_ENTITY_FLOOR_MSG_PER_SEC` | **700000** msg/s | `cargo bench … throughput_single_entity`. Informational; used to triage where a regression sits.       |

`BENCH_FLOOR_MSG_PER_SEC = 480_000`

`SINGLE_ENTITY_FLOOR_MSG_PER_SEC = 700_000`

Recording method: median of 3 runs on the reference host class (modern x86_64
desktop, 8+ cores), rounded down to the nearest 5K — 482 550 msg/s (range
477K–513K) for the regression floor, 714K elem/s for the single-entity
microbench. `mix bench.floor` prints `BENCH_FLOOR_MSG_PER_SEC` from this file
to stdout; the `bench-regression` job's sync step parses it together with the
`at least **N messages/second**` clause of the dispatch spec file it names and
fails on disagreement.

The dynamic-entity steady-state floor (`steady_state_1k_entities`) is
**~520K inputs/s** (median ~383 ms for 200K inputs) on the reference host. It
is a trend signal, not a CI gate.

## CI gates

| Workflow                    | Trigger                                                                                                                                                            | What it runs                                                                                                                                                                                                                                                                                  |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bench.yml`                 | PRs touching `sdk/rust/**`, `flz_transport/**`, `flz_nif/**`, `lib/flezha/**`, `lib/mix/tasks/bench**`, `protobufs/**`, `test/apps/rust_runner/**`, this doc, the spec, the workflow | **`bench-regression`** (merge-blocking): spec/doc floor sync check, then `mix bench --runtime rust --shape regression -n 100 -m 10000`; fails below `0.9 × BENCH_FLOOR_MSG_PER_SEC`, warns when below the floor but inside the band. **`bench-entity-scaling`** (informational, `continue-on-error`): `--shape entity --pool-sizes 1,8`, reports `ratio (max/min)`; GitHub runners do not reliably expose 8 physical cores, so the 4× target is validated on real hardware. |
| `rust-sdk-perf.yml`         | PRs and `main` pushes touching `sdk/rust/**`, `flz_transport/**`, `Cargo.toml`, `Cargo.lock`                                                                         | Runs the `correctness` bench (any invariant violation fails) and the `dispatch` bench with `--save-baseline`; on PRs compares against the cached `main` baseline and fails if any bench's estimated change exceeds 10%.                                                                      |
| `rust-sdk-bench-cron.yml`   | Mondays 03:00 UTC, `workflow_dispatch`                                                                                                                              | Non-blocking drift detector: `mix bench -n 100 -m 10000` across rust/java × uds/quic, plus Criterion `dispatch` (`regression_` group only) and `dynamic_entity_steady_state`; posts a step summary.                                                                                           |

## Floor-update protocol

When a change intentionally moves the floor:

1. Run `./scripts/bench.sh -n 100 -m 10000` three times after a warm cache
   (discard the first) on the reference host class.
2. Take the median, round down to the nearest 5K msg/s.
3. In the **same PR**, update both the `BENCH_FLOOR_MSG_PER_SEC` literal line
   in this doc and the `at least **N messages/second**` clause in the spec
   file `bench.yml`'s sync step names; CI fails on divergence.
4. If lowering the floor, justify the trade-off in the proposal.

The 10% tolerance band absorbs runner-image variance. If CI variance is
consistently above 10%, widen the band in the workflow YAML rather than
lowering the floor.

## Triage when `bench-regression` fails

1. **Read the failure message** — it names the floor, the measured throughput
   and the shortfall. A shortfall inside the band warns but passes.
2. **Transport / NIF changes?** If the PR touches `flz_transport/`, `flz_nif/`,
   `flz_core/src/command/streams_connect.rs` or the framing path, compare
   `cargo bench --bench dispatch -- regression_100x1x1x10k` to the failing
   `mix bench` number. Criterion green + mix red → transport/controller.
3. **SDK dispatch changes?** If the PR touches
   `sdk/rust/src/operative/dispatcher.rs`, `flz_core/src/fast.rs` or
   `flz_core/src/entity.rs`, run `throughput_single_entity` against
   `SINGLE_ENTITY_FLOOR_MSG_PER_SEC`. A regression here is the dispatch hot
   path itself.
4. **Runner drift?** Rerun the job on `main`. If `main` also underperforms on
   the same runner image, the runner pool shifted — rerecord the floor in its
   own change; do not casually lower it.
5. **Entity ratio dropped?** If `bench-entity-scaling`'s `ratio (max/min)` fell
   from ~4× to below 2× on the same PR, the regression is in the entity
   fan-out, not single-entity dispatch.

If none of these explain it, widen the tolerance band rather than lowering the
floor.

## See also

- [`../sdk/rust_operative_architecture.md`](../sdk/rust_operative_architecture.md) — dispatch hot path, entity mailboxes, poll workers.
- [`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) — the wire format `mix bench` exercises.
- [`../sdk/elixir_sdk.md`](../sdk/elixir_sdk.md) — the in-process `native` transport the elixir runtime measures.
