# improve-bench-report-and-signals — implementation notes

## The two driver-global checks had no precondition, and it reached `/healthz`

`StallClassifier.driver_loop_frozen/3` and `nif_queue_backing_up/3` paired each
driver against its previous sample by transport key and judged it with no check
that the driver had ever been *established*. A driver observed in its first
moments reports `last_loop_ms == 0` (`age/2` → `:infinity`) and an empty stream
set (`total_written` → `0 == 0`), which satisfies both checks' "no progress"
clauses **vacuously**. Every other check in the file already carried the
precondition (`find_runner` requires `send_frame_calls` advancing;
`runner_not_consuming` requires `prev_s.frames_read > 0`), the moduledoc claimed
it universally, and the `convergence-self-healing` requirement already worded the
verdict as *"heartbeat flat while frames are queued"*. The implementation was the
outlier.

This is not cosmetic. `driver_loop_frozen` is scoped `:fabric`, and
`healthz/server.ex:188` turns a fabric verdict held for `FLEZHA_FABRIC_503_POLLS`
polls into a **503**. A controller whose fleet had just connected could be
restarted by its orchestrator while perfectly healthy.

`established?/2` is the fix, and it holds on **both** samples:

```
prev.loop_iterations > 0 and prev.last_loop_ms > 0 and
  d.last_loop_ms > 0 and d.loop_iterations >= prev.loop_iterations
```

The `d.last_loop_ms > 0` clause was **not** in the design. The design argued it
was derivable — an established baseline plus a non-decreasing counter implies the
same driver instance, and `mark_loop` stamps the clock every iteration. The
sweep test (`prev_iters × prev_stamp × curr_iters`) refuted that in one run: a
*reported* current sample of `{loop_iterations: 500, last_loop_ms: 0}` against a
`{500, @now}` baseline still minted `:infinity`. The age a verdict reports comes
from the current sample, so the rule has to hold there too. Keep the sweep test —
it is what caught the hole in the reasoning.

The monotonic clause is not decoration either: `find_driver` pairs by transport
key `{:uds_data, runner_id, type}`, and a **reconnect registers a new driver
under the same key** whose counters restart.

Neither verdict ever minted a heal op (`:fabric` verdicts reach `/healthz`, not
the Diff), so removing the false positives lost no remediation. Checked
deliberately before shipping.

The old test built a freeze by zeroing the *current* `last_loop_ms`, which now
correctly reads as "never looped". A real freeze holds an **old** stamp while the
driver clock runs on: `last_loop_ms: @now - 20_000`.

## The controller could not tell "I asked for this close" from "it died"

`FleetManager.disconnect_runner/2` and `stop_namespace/1` ended in the same
`{:flz_closed, …}` clause as a crash: a `Logger.warning` per socket **plus**
`emit_output_stall/1` for every mapped operative, waking the convergence loop to
plan rebinds for operatives deliberately stopped a moment earlier.

The mark lives in `FleetManager`, beside `mark_session_superseded/1`. That was
not the first design: it started on `Shutdown.Coordinator`'s ETS table (which
already held `shutting_down?/0`, the same kind of "this is expected" fact) and
had to widen that table `:protected` → `:public` to let `FleetManager` write it.
The refine pass found that `FleetManager` **already had the whole mechanism** —
a public, TTL-bounded ETS marks table on a runner identity, with a prune on its
cleanup tick — so the second one was a parallel mechanism, and moving the fact
to where its three producers already live let the `:public` widening be reverted
entirely. The two tables stay separate on purpose: the fence decides
correctness, the retiring mark decides only what gets reported.

**Do not route these writes through a `GenServer.call`.** They are bare ETS
writes because the readers are the `{:flz_closed, …}` clauses, and a channel
asking a GenServer whether its own close was expected would be asking the
process that is busy tearing it down. (On the Coordinator that had a second
edge: the drain phase calls Coordinator → migration → FleetManager, so calling
back the other way inside a `handle_call` was a deadlock waiting for the wrong
day.)

Two things are load-bearing:

- **The mark is written before the close is issued.** `mark_runner_retiring/1`
  runs ahead of `ControlUnit.disconnect/1` and `FleetSupervisor.stop_runner_sup/1`,
  or the channels see the close before the fact exists.
- **The mark expires** (`FLEZHA_RUNNER_RETIRING_MARK_TTL_MS`, 30 s). `runner_id`
  is the *durable stable identity* and is reused across reconnects, so a mark
  that outlived its disconnect would silence the diagnosis of a later, genuine
  close of the same runner.

Only the **reporting** is inside the branch. `runner-controller-connection-healing`
requires that a real socket close drop the runner immediately and
unconditionally, so `stream_ref: nil`, `reset_credit`, `publish_stream` and the
`{:runner_disconnected}` call all sit outside it. The test asserts exactly that
pairing — a marked and an unmarked close on the same runner id must differ in
nothing but log severity and the absence of the `output_stall` emit.

Test-suite trap: the primary logger runs at `:error`, so
`capture_log([level: :warning], …)` returns `""`. The `:level` option sets the
capture handler's level, not the primary one. `Logger.configure(level: :info)` in
`setup` with an `on_exit` restore is what actually works.

## Bench reporting

`Mix.Tasks.Bench.Report` owns the whole run lifecycle — `open/2` (title, header
rows, run id, log level) → `record_artifact/1` (the fleet, once it has built one)
→ `emit/1` (header + warnings, idempotent, after the fleet is up) → `finish/2`
(merge and write). The three bench tasks own their workloads and nothing else.

That lifecycle is the refine pass's doing: the first implementation left each
task to sequence it, which meant three copies of the process-dictionary keys,
three `emit_header` functions and the payload default literal written out three
times — and it hid a real bug, `bench.workflow` reading `BENCH_LOG_LEVEL`
instead of the `FLEZHA_BENCH_LOG` this change's own requirement mandates.
`Report` now holds one `:bench_report` key and nothing outside it knows the
shape. Tables are plain aligned columns with a single rule —
**no box drawing**, because the documented comparison workflow is
`… | tee a.txt` then `diff`, and border glyphs make that diff noisy. A test
asserts no box glyph survives.

Ordering that matters: **the header prints after the fleet is up**, in all three
tasks, because the artifacts block reports what the run will actually launch and
`Fleet.ensure_built/1` has only just produced it. Each task stashes its run rows
in the process dictionary at parse time and calls its `emit_header` after setup.

No per-runtime clause table survives in the bench layer. `dispatch_note/1` and
`toolchain/1` both read the conformance manifest, which is the rule this change
introduced for dispatch and then promptly broke for toolchain — the first
implementation shipped a `toolchain_command(:rust)` clause list, the same shape
task 3.4 had just deleted. A per-runtime clause is a list a new SDK has to
remember to join.

`PortPilot.resolve_artifact/2` became public and now returns
`{path, :first | {:fallback, first_choice}}`; `launch_command/3` uses it for the
`{artifact}` variable. One resolution, two readers — a re-derivation for the
provenance block would be a second answer that can disagree with the first.

The **headline** is the end-to-end window. `Throughput:` used to be the
receive-window rate, which credits work the send window paid for: on the run that
prompted this change it read 351,899 msg/s against an end-to-end 95,178.

The **gate** deliberately stays on the receive window
(`metrics.receive_window_msg_per_sec`, floor unchanged at 480_000). The headline
a human reads and the number a gate measures are allowed to differ: the
`rust-sdk-dispatch` floor is about *dispatch* throughput, and the send window it
excludes is the controller's own cast overhead, not anything the SDK's dispatch
path controls. The design originally conflated the two and would have
invalidated a floor measured on faster hardware for no gain.

**Watch the first CI run after this merges.** The `bench-regression` job has
never actually executed (its sync step pointed at an archived path and could not
pass), so fixing that step turns on a merge-blocking gate for the first time,
against a floor recorded on a "modern x86_64 desktop, 8+ cores". For scale: this
4-core i7-8650U laptop on `powersave` measures a receive-window median of
309,172 msg/s against a `0.9 × 480_000 = 432_000` threshold — it would fail. If
GitHub's `ubuntu-latest` runners are in that class, the floor needs re-recording
against runner hardware rather than the gate being weakened.

`bench.roundtrip`'s report keeps its path (`bench/flink/reports/`) because
`bench/flink/lib/summarize.py` and `bench/flink/bench.sh` read it. Its fields
moved under `config`/`metrics`, and `summarize.py` gained a six-line `flatten()`
at its load point rather than a hundred edited accessors. `flatten()` is a no-op
on an old flat report, so a developer's existing local reports still summarize.

`bench.workflow` keeps its `BENCH_RESULT k=v` line — `bench/acceptance/run.sh`
parses it — and writes a JSON report beside it.

## Pre-existing, NOT introduced here, and not fixed here

- **`bench.yml`'s spec/doc sync check pointed at a path that was archived.** It
  read `openspec/changes/operative-bench-harness/specs/rust-sdk-dispatch/spec.md`,
  archived to `openspec/changes/archive/2026-05-04-operative-bench-harness/`.
  Under `set -euo pipefail` the step could not pass, so the merge-blocking job
  was broken. Repointed at the live capability spec, which is the file a future
  change actually edits.
- **The entity shape stalls at small worker-pool sizes.** On a 4-core box
  `--pool-sizes 1` stalls at ~89 of 1,280,000 responses and `2` at ~539; `4`
  reached 320,388 before timing out; `8` completes in 13.2 s. Not a linear
  slowdown — a stall. The documented default sweep is `1,8`, so it cannot
  complete there. This is SDK dispatch, not the reporting layer; no dispatch code
  changed in this work.
- **The Elixir runner dies under `-n 1000 -m 1000`.** The classifier reports
  `:runner_pool_wedged` (dispatches frozen at 271,057) and a runner's control
  socket closes *before* teardown, which then floods `Connection closed for IO
  socket` — correctly, because the controller did not ask for those closes. At
  `-n 50 -m 100` the same runtime tears down with **zero** warnings. The
  remaining flood is the signal working, not noise.

## `/proc` parsing is one function

`ProcParser` gained `parse_cpuinfo/1` and `parse_meminfo/1`, and both initially
hand-rolled "split the line on `:`, match a key" — which the pre-existing
`parse_status/1` had already been doing. Three copies is the extraction
trigger: `fields/1` builds the `%{key => value}` map, `int_field/2` and
`kib_field/2` read it, and `parse_status/1` is now three lines instead of
twenty-eight.

`fields/1` is **first-wins** (`Map.put_new`), not last-wins. `/proc/cpuinfo`
repeats every field once per core, and a reader asking for `model name` expects
the first. `/proc/self/status` has unique keys, so `parse_status/1`'s behaviour
is unchanged either way. `parse_cpuinfo/1` still counts `processor` lines
separately, because a map collapses exactly the repetition it needs to count.
