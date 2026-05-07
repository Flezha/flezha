# Benchmarking

How the Flezha Rust SDK's dispatch throughput is measured, what the recorded
floors mean, and what to do when CI's `bench-regression` gate fails. Closes
relay-mvp-rust §10 by giving the `rust-sdk-dispatch` capability spec a literal
floor value and the matching tooling/CI gates.

## Recorded floors

| Name | Value | What it gates | Where |
|---|---|---|---|
| `BENCH_FLOOR_MSG_PER_SEC` | **480000** msg/s | `mix bench --shape regression` end-to-end (agent + NIF + SDK round-trip). Merge-blocking via the `bench-regression` GitHub Actions job. | `rust-sdk-dispatch` requirement "Dispatch Throughput Floor on Reference Workload"; this doc |
| `SINGLE_PARTITION_FLOOR_MSG_PER_SEC` | 700000 msg/s | `cargo bench --bench correctness -- throughput_single_partition` (SDK-internal microbench). Informational only — used for triaging where a regression sits when the gate fires. | `rust-sdk-dispatch` requirement "Per-Message Dispatch-Overhead Budget"; this doc |

`BENCH_FLOOR_MSG_PER_SEC = 480_000`

`SINGLE_PARTITION_FLOOR_MSG_PER_SEC = 700_000`

CI sync check: the `bench-regression` job parses the literal `480_000` from
both the spec and this doc; the build fails if they disagree (per
`relay-bench-harness/design.md` Decision 6's floor-update protocol).

### Recording metadata

| Field | Value |
|---|---|
| Date measured | 2026-05-04 |
| Host class | Modern x86_64 desktop / 8+ cores, non-constrained CPU |
| Median of 3 runs (regression bench) | 482_550 msg/s |
| Range observed across 3 runs | 477K — 513K msg/s |
| Median rounded down to nearest 5K msg/s | 480_000 |
| Single-partition microbench median | 714_000 elem/s |

#### Re-measurement note (`relay-bounded-mailbox`)

`relay-bounded-mailbox` adds one Relaxed atomic op per successful push
(the `pushes_total` counter) on the dispatch hot path, raising the
analytical sync-op budget from 3 to 4 (Decision 7 of that change's
`design.md`). Per the floor-update protocol below, the floor was
re-measured against the post-bounded-mailbox SDK on the same reference
host class; the measured-at-merge values replace the prior `480_000` /
`700_000` numbers above. The pre-bounded-mailbox values are recorded in
that change's spec as the historical baseline; they SHALL NOT be carried
forward as the gate.

The pre-cutover floor (350K msg/s, on the same workload before relay-mvp-rust)
is intentionally NOT carried forward — it was measured against a different
SDK architecture (the `Processor` trait), so it isn't apples-to-apples with
the post-cutover Relay/Shard/Partition model.

## Bench shapes

There are four bench shapes. They serve different purposes; running all four
is rarely necessary day-to-day.

### Regression — `mix bench --shape regression`

The merge-blocking gate. Mirrors today's `mix bench --runtime rust -n 100 -m
10000` invocation: 100 relay instances × 1 shard × 1 partition × 10K msg
each, factory `do_nothing`, end-to-end through the agent's Overwatch
GenServer + UDS NIF + SDK + plugin layer. Reports a single `Throughput:`
line as input messages/second; `BeamMsgForward(:on_output)` echoes one
plugin response per dispatched input.

```sh
MIX_ENV=prod mix bench --runtime rust --shape regression -n 100 -m 10000
# or equivalently (no --shape flag — `regression` is the default):
MIX_ENV=prod mix bench --runtime rust -n 100 -m 10000
```

### Partitioning — `mix bench --shape partitioning`

Exercises the partitioning code path: 8 relay instances × 4 shards × 8
partitions × 20K msg/instance, factory `partitioning_stress`. The entry
shard fans out every input to all 8 partitions of itself; each pipeline
stage (B/C/D) runs once per upstream emit on its `p0` partition. The final
shard emits each invocation on a plugin-bound channel, so each input
produces 8 plugin responses.

```sh
# Sweep pool sizes; CI uses 1,8 to verify the 4× scaling spec scenario.
MIX_ENV=prod mix bench --runtime rust --shape partitioning --pool-sizes 1,8
```

The output ends with a `=== Scaling Summary ===` block listing throughput
per pool size and the `ratio (max/min)` line that
`bench-partitioning-scaling` parses (informational; non-blocking).

### Single-partition microbench — `cargo bench --bench correctness -- throughput_single_partition`

SDK-internal: one relay × one shard × one partition × no transport. Drives
`RelayDispatchHandle::dispatch` directly, bypassing the agent + NIF.
Reports msg/s for the dispatch hot path (registry lookup + mailbox enqueue
+ `pushes_total` Relaxed counter + inflight CAS — the 4-sync-op budget from
`rust-sdk-dispatch::Per-Message Dispatch-Overhead Budget`, raised from 3
to 4 by `relay-bounded-mailbox` Decision 7 to honestly account for the
success counter the bounded-mailbox observability story requires). The
bench's entry channel opts into `OverflowPolicy::Block` so the bursty
producer never trips the default `DropNewest` mid-measurement.

```sh
cargo bench --bench correctness -p flezha_compute_engine \
  --features bench-internals -- throughput_single_partition
```

### Static-fanout microbench — `cargo bench --bench correctness -- throughput_static_fanout`

Same as the single-partition microbench except the shard has 4 partition
keys and `partition_for` returns all 4 keys per input — exercises the
fan-out cost (`4 + N × 3` sync ops per input where N is the partition
count, per `relay-bounded-mailbox/design.md` Decision 7). Informational;
characterizes the "static-mode partition fan-out cost is proportional to
partition count" spec scenario.

```sh
cargo bench --bench correctness -p flezha_compute_engine \
  --features bench-internals -- throughput_static_fanout
```

### Criterion dispatch benches — `cargo bench --bench dispatch`

Mirror the `mix bench` shapes but bypass the agent. `regression_100x1x1x10k`
is the SDK-internal counterpart to `mix bench --shape regression`;
`partitioning_stress_8x4x8x20k/<pool>` is the SDK-internal counterpart to
`mix bench --shape partitioning`. Both use `RelayDispatchHandle::dispatch`
directly. The `partitioning_stress_*` group reads
`FLEZHA_BENCH_POOL_SIZES` (default: a single pool size from
`std::thread::available_parallelism()`).

```sh
# Default pool size (host-detected).
cargo bench --bench dispatch -p flezha_compute_engine \
  --features bench-internals

# Sweep pool sizes 1 and 8 (matches CI's bench-partitioning-scaling job).
FLEZHA_BENCH_POOL_SIZES=1,8 cargo bench --bench dispatch \
  -p flezha_compute_engine --features bench-internals
```

## CI gates

Two GitHub Actions jobs in `.github/workflows/bench.yml`:

- **`bench-regression`** (merge-blocking, paths: `sdk/rust/src/**`,
  `flz_transport/src/**`, `flz_nif/src/**`, `lib/flezha/**`,
  `protobufs/**`). Runs `mix bench --shape regression -n 100 -m 10000`,
  parses the `Throughput:` line, fails if measured < `0.9 ×
  BENCH_FLOOR_MSG_PER_SEC` (10% tolerance band per
  `relay-bench-harness/design.md` Decision 6). Pre-step: spec/doc sync
  check (parses the literal from both and aborts on divergence).
- **`bench-partitioning-scaling`** (informational, same triggers). Runs
  `mix bench --shape partitioning --pool-sizes 1,8`, parses the
  `ratio (max/min)` line, reports it to the PR check output. Does NOT
  fail the build on any threshold — GitHub Actions runners are
  virtualized and don't reliably expose 8 physical cores, so the 4×
  scaling scenario is checked manually on real hardware before merging
  cross-cutting changes.

`mix bench.floor` prints `BENCH_FLOOR_MSG_PER_SEC` (parsed from this doc)
to stdout. Used by the CI sync-check step instead of hard-coding the value
in workflow YAML.

## Floor-update protocol

When a future change intentionally raises or lowers the floor (an
optimization that beats the existing number consistently, or an
architectural change that intentionally trades dispatch throughput for a
different invariant):

1. Run `mix bench --shape regression -n 100 -m 10000` three times after a
   warm cache (one pre-run discarded), on the reference host class.
2. Take the median; round down to the nearest 5K msg/s.
3. Update **both** the spec literal in
   `openspec/changes/relay-bench-harness/specs/rust-sdk-dispatch/spec.md`
   (the `Dispatch Throughput Floor on Reference Workload` requirement and
   the `Reference bench meets the floor` scenario) **and** the
   `BENCH_FLOOR_MSG_PER_SEC` value in this doc — in the **same PR** as the
   change.
4. The `bench-regression` CI sync check enforces this: parsing the literal
   from both files and failing the build if they disagree.
5. If the floor is being intentionally lowered, the PR's `proposal.md`
   `Why` section must justify the trade-off (e.g. "trades dispatch
   throughput for state-restoration correctness in `relay-state-migration`").

The 10% CI tolerance band absorbs runner-image variance. Tighter gates
produce false-positive flakes; looser bands miss real regressions. If
practical CI variance is observed to exceed 10%, the band can be widened
to 15% in a follow-up — it's a single constant in the workflow YAML.

## Triage when `bench-regression` fails

When the gate fires, work down the list. Most regressions are in step 2 or
3.

1. **Read the failure message.** The job logs the recorded floor, the
   measured throughput, and the percentage shortfall. A 5% shortfall
   inside the 10% band passes (with a warning); a > 10% shortfall blocks.
2. **Check the diff for transport changes.** If the PR touches
   `flz_transport/`, `flz_nif/`, `sdk/rust/src/command/streams_connect.rs`,
   or the NIF ↔ Elixir framing path, the regression is likely there.
   Compare `cargo bench --bench dispatch -- regression_100x1x1x10k`
   (SDK-internal) to the failing `mix bench` number — if Criterion is
   green but `mix bench` is red, the regression is in the transport
   layer or agent dispatch path, not the SDK.
3. **Check the diff for SDK dispatch changes.** If the PR touches
   `sdk/rust/src/relay/dispatcher.rs`, `compute_engine/fast.rs`, or
   `partition/mod.rs`, run the Criterion `throughput_single_partition`
   bench and compare to `SINGLE_PARTITION_FLOOR_MSG_PER_SEC`. A regression
   there is the dispatch hot path itself.
4. **Check the runner pool.** Run the failing PR's `bench-regression` job
   on a different commit (e.g. main) on the same runner image. If main is
   ALSO measuring under the floor, the runner pool itself shifted and the
   floor may need to be widened or rerecorded — open a separate change to
   re-record with the new runner-image baseline.
5. **Look at the partitioning scaling check** (informational). If the
   `bench-partitioning-scaling` job's ratio dropped from ~4× to <2× on
   the same PR, the regression is in the partitioning fan-out path, not
   single-partition dispatch.

If none of the above explain the regression, the floor may be too tight
for current CI variance. Open a follow-up that widens the tolerance to
15% and notes the symptom in the PR description; do NOT casually lower
the floor itself unless the architecture has changed (per the
floor-update protocol above).

## See also

- `docs/relay_architecture.md` — SDK-side architecture (dispatch hot
  path, partition mailboxes, work-stealing pool).
- `docs/sdk_protocol_spec.md` — wire format that `mix bench` exercises.
- `openspec/changes/relay-bench-harness/specs/rust-sdk-dispatch/spec.md`
  — the spec requirement this doc closes.
- `openspec/changes/relay-mvp-rust/tasks.md §10` — the deferred work this
  change completes.

## State persistence hot path

`sdk/rust/benches/state.rs::state_apply_throughput` measures the
per-`state.apply(...)` call cost. The umbrella commits to "one
`Vec::push` plus the user's `apply_change` body" on the hot path;
serialization, coalescing, and wire I/O happen off the hot path on
the per-process flusher thread.

**Reference baseline (recorded on the merge-time host):**

| Dimension | Median |
|---|---|
| `small_no_key_no_prealloc` | < 200 ns/op |
| `small_no_key_prealloc_1024` | < 100 ns/op |
| `small_keyed_no_prealloc` | < 200 ns/op |
| `small_keyed_prealloc_1024` | < 100 ns/op |
| `large_no_key_no_prealloc` | < 500 ns/op |
| `large_no_key_prealloc_1024` | < 500 ns/op |
| `large_keyed_no_prealloc` | < 500 ns/op |
| `large_keyed_prealloc_1024` | < 500 ns/op |

(Values above are budget ceilings, not hard floors. Run `cargo bench
--bench state` on your reference host to record the actual median;
the umbrella's "Vec::push only" promise puts small-no-key at ~50 ns.)

**Regression policy:** CI fails the build if any dimension regresses
> 50% from the recorded baseline. Treat the floor like other regression
floors in this doc — re-record only when the architecture changes
(e.g. coalescing moves on-path, header layout grows, etc.).
