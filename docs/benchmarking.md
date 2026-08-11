# Benchmarking

One place for everything about running and interpreting Flezha benches.

- Single entry point: `./scripts/bench.sh` (wraps `mix bench` and sets `FLEZHA_TRANSPORT`).
- Switching axes are independent: **runtime** (rust/java) × **transport** (uds/quic) × **shape** (regression/entity).
- Floor numbers: `BENCH_FLOOR_MSG_PER_SEC = 480_000`, `SINGLE_ENTITY_FLOOR_MSG_PER_SEC = 700_000`.

## Quick start

```sh
./scripts/bench.sh                                # rust + uds, 100 operatives x 100 msg
./scripts/bench.sh -n 100 -m 1000                 # bigger run
./scripts/bench.sh -r java                        # java SDK
./scripts/bench.sh -t quic                        # quic transport
./scripts/bench.sh -r java -t quic -n 100 -m 1000 # any combination
./scripts/bench.sh --shape entity --pool-sizes 1,8

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
| `-r` / `--runtime` | `rust` (default), `java` | Picks which SDK runner is launched. `java` requires `test/apps/java_runner/target/java_runner.jar` — build with `mix test.java_sdk` or `mvn -f test/apps/java_runner/pom.xml package`. |
| `-t` / `--transport` | `uds` (default), `quic` | Sets `FLEZHA_TRANSPORT`. QUIC auto-generates self-signed certs at `app_data/certs/` on first run. |
| `-s` / `--shape` | `regression` (default), `entity` | Picks the workload. `regression` honors `-n`/`-m`; `entity` is fixed at 8×4×8×20K and honors `--pool-sizes`. |
| `-n` / `--num-operatives` | int (default 100) | Operative count, `regression` shape only. |
| `-m` / `--messages-per-operative` | int (default 100) | Messages per operative, `regression` shape only. |
| `--pool-sizes` | comma list (default `1,8`) | Worker-pool sizes to sweep, `entity` shape only. |

Direct `mix bench …` works identically; the script just sets `FLEZHA_TRANSPORT` and forwards the rest.

## Bench shapes

### `regression` (default, merge-blocking)

100 operatives × 1 procedure × 1 entity × 10K msg, factory `do_nothing`, end-to-end through controller + NIF + SDK. One `Throughput:` line out. This is what the `bench-regression` CI gate runs.

```sh
./scripts/bench.sh -n 100 -m 10000
```

### `entity` (scaling check, informational)

8 operatives × 4 procedures × 8 entities × 20K msg, factory `entity_stress`. Each input fans out 8 plugin responses. Sweeps the values in `--pool-sizes`, prints a `=== Scaling Summary ===` block ending with `ratio (max/min)`.

```sh
./scripts/bench.sh --shape entity --pool-sizes 1,8
```

### SDK-internal microbenches (no controller, no NIF)

```sh
# Single-entity dispatch hot path.
cargo bench --bench correctness -p flezha_runner \
  --features bench-internals -- throughput_single_entity

# Static fan-out cost (4 entities per input).
cargo bench --bench correctness -p flezha_runner \
  --features bench-internals -- throughput_static_fanout

# Criterion mirrors of the mix-bench shapes. The SDK's worker pool is
# process-global and sized once, so a run measures one size — sweep by
# re-running under different values.
cargo bench --bench dispatch -p flezha_runner --features bench-internals
FLEZHA_WORKER_POOL_SIZE=1 cargo bench --bench dispatch \
  -p flezha_runner --features bench-internals
```

Use these to localize a regression: if `mix bench` is red but Criterion `regression_100x1x1x10k` is green, the regression is in the transport/controller path, not the SDK.

### State-apply hot path

```sh
cargo bench --bench state -p flezha_runner
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
| `SINGLE_ENTITY_FLOOR_MSG_PER_SEC` | **700000** msg/s | `cargo bench … throughput_single_entity`. Informational; used to triage where a regression sits. |

`BENCH_FLOOR_MSG_PER_SEC = 480_000`

`SINGLE_ENTITY_FLOOR_MSG_PER_SEC = 700_000`

Recording metadata (2026-05-04, modern x86_64 desktop, 8+ cores): median 482550 msg/s across 3 runs (range 477K–513K), rounded down to nearest 5K. Single-entity microbench median 714K elem/s.

The CI sync check parses these literals from this doc and from `openspec/specs/rust-sdk-dispatch/spec.md`; build fails on disagreement. `mix bench.floor` prints `BENCH_FLOOR_MSG_PER_SEC` to stdout.

## CI gates

Two jobs in `.github/workflows/bench.yml`:

- **`bench-regression`** (merge-blocking). Runs `mix bench --shape regression -n 100 -m 10000`, fails if measured < `0.9 × BENCH_FLOOR_MSG_PER_SEC` (10% tolerance band). Pre-step verifies spec/doc floor agreement.
- **`bench-entity-scaling`** (informational). Runs `mix bench --shape entity --pool-sizes 1,8`, reports the `ratio (max/min)`. Never fails the build — GitHub runners don't reliably expose 8 physical cores, so the 4× scaling target is validated manually on real hardware.

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
3. **SDK dispatch changes?** If the PR touches `dispatcher.rs`, `runner/fast.rs`, or `entity/mod.rs`: run `cargo bench … throughput_single_entity` and compare to `SINGLE_ENTITY_FLOOR_MSG_PER_SEC`. A regression here is the dispatch hot path itself.
4. **Runner drift?** Rerun the job on `main`. If `main` also underperforms on the same runner image, the runner pool shifted — open a separate change to rerecord the floor; do not casually lower it.
5. **Entity ratio dropped?** If `bench-entity-scaling`'s `ratio (max/min)` fell from ~4× to <2× on the same PR, the regression is in the entity fan-out, not single-entity dispatch.

If none of the above explain it, widen the tolerance band rather than lowering the floor.

## Dynamic-entity steady-state floor

`cargo bench --features bench-internals --bench dynamic_entity_steady_state`
(`steady_state_1k_entities`) measures the dynamic-mode hot path: 1,000
active dynamic Entities, no eviction, 200K inputs round-robined across
the keys. It isolates the dynamic routing cost — one `entity_key` call plus
one `DashMap` lookup per input — over the static path.

Recorded floor (reference host, `cargo bench`, 2026-06-16):
**~520 K inputs/s** (median ~383 ms for 200K inputs). This is a steady-
state floor for dynamic routing, not a CI gate; the `bench-regression`
gate continues to track single/static dispatch via `dispatch.rs`. The
dynamic code path is isolated behind `EntityMode::Dynamic`, so the
single- and static-mode floors are unaffected (the only added cost on
those paths is one Relaxed `last_activity_at` store per push).

## See also

- `docs/operative_architecture.md` — dispatch hot path, entity mailboxes, poll workers.
- `docs/sdk_protocol_spec.md` — wire format that `mix bench` exercises.
- `openspec/specs/rust-sdk-dispatch/spec.md` — the spec the floor closes.
