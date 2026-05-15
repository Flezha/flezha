# Benchmarking

One place for everything about running and interpreting Flezha benches.

- Single entry point: `./scripts/bench.sh` (wraps `mix bench` and sets `FLEZHA_TRANSPORT`).
- Switching axes are independent: **runtime** (rust/java) × **transport** (uds/quic) × **shape** (regression/partitioning).
- Floor numbers: `BENCH_FLOOR_MSG_PER_SEC = 480_000`, `SINGLE_PARTITION_FLOOR_MSG_PER_SEC = 700_000`.

## Quick start

```sh
./scripts/bench.sh                                # rust + uds, 100 relays x 100 msg
./scripts/bench.sh -n 100 -m 1000                 # bigger run
./scripts/bench.sh -r java                        # java SDK
./scripts/bench.sh -t quic                        # quic transport
./scripts/bench.sh -r java -t quic -n 100 -m 1000 # any combination
./scripts/bench.sh --shape partitioning --pool-sizes 1,8

./scripts/bench.sh --help                         # full options
```

Compare two runs:

```sh
./scripts/bench.sh -r rust -n 100 -m 1000 | tee bench-rust.txt
./scripts/bench.sh -r java -n 100 -m 1000 | tee bench-java.txt
diff bench-rust.txt bench-java.txt
```

The controller, protocol, and measurement code are identical across runs — only the launched SDK process and the wire change — so the `Throughput:` line is directly comparable.

## Switching axes

| Flag | Values | Effect |
|---|---|---|
| `-r` / `--runtime` | `rust` (default), `java` | Picks which SDK executor is launched. `java` requires `test/apps/java_executor/target/java_executor.jar` — build with `mix test.java_sdk` or `mvn -f test/apps/java_executor/pom.xml package`. |
| `-t` / `--transport` | `uds` (default), `quic` | Sets `FLEZHA_TRANSPORT`. QUIC auto-generates self-signed certs at `app_data/certs/` on first run. |
| `-s` / `--shape` | `regression` (default), `partitioning` | Picks the workload. `regression` honors `-n`/`-m`; `partitioning` is fixed at 8×4×8×20K and honors `--pool-sizes`. |
| `-n` / `--num-relays` | int (default 100) | Relay count, `regression` shape only. |
| `-m` / `--messages-per-relay` | int (default 100) | Messages per relay, `regression` shape only. |
| `--pool-sizes` | comma list (default `1,8`) | Worker-pool sizes to sweep, `partitioning` shape only. |

Direct `mix bench …` works identically; the script just sets `FLEZHA_TRANSPORT` and forwards the rest.

## Bench shapes

### `regression` (default, merge-blocking)

100 relays × 1 shard × 1 partition × 10K msg, factory `do_nothing`, end-to-end through controller + NIF + SDK. One `Throughput:` line out. This is what the `bench-regression` CI gate runs.

```sh
./scripts/bench.sh -n 100 -m 10000
```

### `partitioning` (scaling check, informational)

8 relays × 4 shards × 8 partitions × 20K msg, factory `partitioning_stress`. Each input fans out 8 plugin responses. Sweeps the values in `--pool-sizes`, prints a `=== Scaling Summary ===` block ending with `ratio (max/min)`.

```sh
./scripts/bench.sh --shape partitioning --pool-sizes 1,8
```

### SDK-internal microbenches (no controller, no NIF)

```sh
# Single-partition dispatch hot path.
cargo bench --bench correctness -p flezha_executor \
  --features bench-internals -- throughput_single_partition

# Static fan-out cost (4 partitions per input).
cargo bench --bench correctness -p flezha_executor \
  --features bench-internals -- throughput_static_fanout

# Criterion mirrors of the mix-bench shapes (host-detected pool size).
cargo bench --bench dispatch -p flezha_executor --features bench-internals
FLEZHA_BENCH_POOL_SIZES=1,8 cargo bench --bench dispatch \
  -p flezha_executor --features bench-internals
```

Use these to localize a regression: if `mix bench` is red but Criterion `regression_100x1x1x10k` is green, the regression is in the transport/controller path, not the SDK.

### State-apply hot path

```sh
cargo bench --bench state -p flezha_executor
```

Budget ceilings (per call, on the reference host):

| Workload | Median |
|---|---|
| small, no key, no prealloc | < 200 ns |
| small, keyed, prealloc 1024 | < 100 ns |
| large, any key, any prealloc | < 500 ns |

CI fails on > 50% regression from the recorded baseline.

## Recorded floors

| Constant | Value | What it gates |
|---|---|---|
| `BENCH_FLOOR_MSG_PER_SEC` | **480000** msg/s | `mix bench --shape regression` end-to-end. Merge-blocking via `bench-regression`. |
| `SINGLE_PARTITION_FLOOR_MSG_PER_SEC` | **700000** msg/s | `cargo bench … throughput_single_partition`. Informational; used to triage where a regression sits. |

`BENCH_FLOOR_MSG_PER_SEC = 480_000`

`SINGLE_PARTITION_FLOOR_MSG_PER_SEC = 700_000`

Recording metadata (2026-05-04, modern x86_64 desktop, 8+ cores): median 482550 msg/s across 3 runs (range 477K–513K), rounded down to nearest 5K. Single-partition microbench median 714K elem/s.

The CI sync check parses these literals from this doc and from `openspec/specs/rust-sdk-dispatch/spec.md`; build fails on disagreement. `mix bench.floor` prints `BENCH_FLOOR_MSG_PER_SEC` to stdout.

## CI gates

Two jobs in `.github/workflows/bench.yml`:

- **`bench-regression`** (merge-blocking). Runs `mix bench --shape regression -n 100 -m 10000`, fails if measured < `0.9 × BENCH_FLOOR_MSG_PER_SEC` (10% tolerance band). Pre-step verifies spec/doc floor agreement.
- **`bench-partitioning-scaling`** (informational). Runs `mix bench --shape partitioning --pool-sizes 1,8`, reports the `ratio (max/min)`. Never fails the build — GitHub runners don't reliably expose 8 physical cores, so the 4× scaling target is validated manually on real hardware.

Both trigger on changes under `sdk/rust/src/**`, `flz_transport/src/**`, `flz_nif/src/**`, `lib/flezha/**`, `protobufs/**`.

## Floor-update protocol

When a change intentionally moves the floor:

1. Run `./scripts/bench.sh -n 100 -m 10000` three times after a warm cache (discard the first), on the reference host class.
2. Take the median, round down to the nearest 5K msg/s.
3. In the **same PR**, update both:
   - the `BENCH_FLOOR_MSG_PER_SEC` literal in this doc, and
   - the `at least **N messages/second**` clause in `openspec/specs/rust-sdk-dispatch/spec.md`.
4. CI parses both and fails on divergence.
5. If lowering the floor, justify the trade-off in the proposal `Why` section.

The 10% tolerance band absorbs runner-image variance. If CI variance is consistently > 10%, widen to 15% in the workflow YAML rather than lowering the floor.

## Triage when `bench-regression` fails

1. **Read the failure message** — it logs floor, measured throughput, and shortfall. A < 10% shortfall warns but passes.
2. **Transport / NIF changes?** If the PR touches `flz_transport/`, `flz_nif/`, `streams_connect.rs`, or the framing path: compare `cargo bench --bench dispatch -- regression_100x1x1x10k` to the failing `mix bench` number. Criterion green + mix red → regression is in transport/controller, not SDK.
3. **SDK dispatch changes?** If the PR touches `dispatcher.rs`, `executor/fast.rs`, or `partition/mod.rs`: run `cargo bench … throughput_single_partition` and compare to `SINGLE_PARTITION_FLOOR_MSG_PER_SEC`. A regression here is the dispatch hot path itself.
4. **Runner drift?** Rerun the job on `main`. If `main` also underperforms on the same runner image, the runner pool shifted — open a separate change to rerecord the floor; do not casually lower it.
5. **Partitioning ratio dropped?** If `bench-partitioning-scaling`'s `ratio (max/min)` fell from ~4× to <2× on the same PR, the regression is in the partitioning fan-out, not single-partition dispatch.

If none of the above explain it, widen the tolerance band rather than lowering the floor.

## See also

- `docs/relay_architecture.md` — dispatch hot path, partition mailboxes, work-stealing pool.
- `docs/sdk_protocol_spec.md` — wire format that `mix bench` exercises.
- `openspec/specs/rust-sdk-dispatch/spec.md` — the spec the floor closes.
