# `refactor-5d-observability` — the instrument tree, measured rather than planned

`lib/flezha/observability` + `lib/mix`. Four defect commits and three refactor
commits. Everything below is a number this change took, not one it inherited.

## What the area finished at

| metric | `lib/flezha/observability` | `lib/mix` |
|---|---|---|
| `prod_loc` | 4 148 → **3 933** (−215) | 3 360 → **3 474** (+114) |
| `files` | 37 | 10 → **14** |
| `files_over_800` | 0 | 2 → **0** |
| `functions_over_50` | 2 → **0** | 13 → **0** |
| `narrative_markers` | 3 → **0** | 1 → **0** |
| `cycles` / `max_cycle_files` | 1 / 2 → **0 / 0** | 0 / 0 |
| `duplicate_pairs` (**full scan**, the one `baseline.json` holds) | 27 → **13** | 3 → **2** |
| `duplicate_pairs` (**area scan**, these two areas only) | 26 → **9** | 1 → **0** |
| `credo_warnings` | 32 → **31** | 20 → **15** |
| `doc_lines` | 877 → **705** | 352 → **395** |

**`duplicate_pairs` is a pair count over whatever the run scanned**, which is why
both rows are above and both are labelled. The full-scan figures are the ones
`baseline.json` holds and the only ones `--check` compares; the area-scan figures
answer a different question and are larger in their movement. Quoting one against
the other is the mistake a supervisor had to catch on area 5b. Both directions
agree here, which is luck, not a reason to stop labelling them.

`lib/mix` **+114 is ceiling-forced and counted by cause**: four new module
headers with their moduledocs (≈ 80) and ~15 function extractions, each a
signature and a brace (≈ 45), against the deletions. §1's rule — when the
ceiling and the total pull against each other the ceiling wins — is the reason,
and the two files over 800 are now four files, none over 660.

## The edge ledger — one edge, and what it bought

The whole `Registry → Bus` direction was **one call**: `registry.ex:480
Bus.emit(signal)` inside `emit_budget_exceeded_event/1`. Deleting that function
removed the area's only back-edge and its only dependency cycle
(`bus.ex ↔ registry.ex`), taking `cycles` 1 → 0 and `max_cycle_files` 2 → 0.

It is worth recording that the metric move was the **smaller** half. The same
edge was a P2 defect: the emit ran inside the Registry's own `handle_call`, into
sinks registered `overflow: :block, block_timeout_ms: 30_000`, and `bus.ex:136`'s
`pid == self()` self-deadlock guard cannot fire because `self()` there is the
Registry. Cutting the edge is what stops every `register_source/1` queueing
behind a slow sink on a 5 s default.

## Four defects, each red first

**F1 — the registry blocked on a sink while emitting its own event.** Driven
with `TestSinks.Blocking` extended to *hold* one named signal until released,
not with a socket that accepts and never replies: a silent socket only makes a
sink slow, a held `GenServer.call` makes the block deterministic and bounds the
test at 500 ms instead of 5 s. Red: `result :registry_blocked`. The reply widened
to `{:dropped, :cardinality_budget_exceeded, :first | :repeat}` — the published
spec documents the *behaviour*, never the tuple, so no clause moved for it.

**Two separated negative controls, each reddening exactly one test**: the emit
put back inside `handle_call` reddens only F1's; `emit_system_event/2` bypassing
the source status reddens only the asymmetry test. Both files restored and
`sha256sum -c` verified.

**The asymmetry.** `emit_kind_violation/2` reached `fan_out/1` directly while the
budget event went through `Bus.emit/1`, so disabling `:observability_registry`
suppressed one system event and not the other. Both now cross one
`emit_system_event/2`: status-checked, and subject to **no** kind check. That
last part is deliberate — routing a kind violation through `do_emit/1` recurses
without bound the moment `:observability_registry` is registered without
`:event` in its kinds, and no `:event` on that source can fail the check anyway.

**F2 / KI-99 — a callback with no invoker.** `Sink.handle_signal/2` was declared,
implemented eight times (four no-op stubs, and `prometheus_sink.ex` duplicating
its own live `handle_info/2` body) and invoked nowhere; four spec clauses said it
SHALL be invoked. Deleted, and the spec corrected to the `{:signal, signal}`
contract that ships. Checking the pair found `Source.handle_poll/2` in the
identical state, and then the larger fact: **`Flezha.Observability.Source` has
zero implementers in the whole tree** — the only `@behaviour` line for it is
inside its own moduledoc example, the five real sources `use` a different module,
and `do_register_source/1` never names a module at all. Filed as KI-100.

**F3 — a bench task deciding its runtime set from a clause list.** The gate row
that catches it reads each task's **source** for a quoted runtime name;
`bench.ex` and `roundtrip.ex` carry zero, `workflow.ex` carried three, so the
rule is exact rather than approximate. Three private copies collapsed into
`Fleet.parse_runtime/3` — three arguments, not the design's two, because the
third names *what asked* for the capability and keeps `roundtrip`'s refusal text
byte for byte.

**KI-97 — the plugin bench dead since a false-positive rename.** The entry named
two sites; there are **three**, and its description of one was wrong: `:120`'s
`entity: 0` is inside the Kayrock *response* shape, not the producer opts, and
`:118`'s `entity_responses:` is a site it does not mention. Fixing only the two
named trades a `KeyError` for a `CaseClauseError`.

## Two silent defects the designed splits would have shipped

**`roundtrip.ex`'s designed partition is CYCLIC against the tree as it is.** The
edge is `Load.maybe_status/1 → Main.wal_signal_summary/1`, which design §4
reports as `Load → Main 0` because its **span**-based partition split
`wal_signal_summary` (642–660) across the 648/649 boundary and attributed it by
its start line. A name-based projection sees it.

Cutting it by moving the WAL-telemetry trio to `Load` is not convenience, it is
**forced**: `wal_signal_summary/0`, `attach_wal_telemetry/0` and
`report_wal_telemetry/0` all key `:persistent_term` on
`{__MODULE__, :wal_counters}`. Split across two modules the writer and one reader
use different keys, the `--status-every` line quietly loses its WAL summary, and
**nothing fails** — no test covers that output. That is the difference between a
refactor and a regression, and the only way to find it was to look before
splitting.

Second obstacle the design does not price: `@namespace` is named by all three
groups of `roundtrip.ex` and `@namespace`/`@waiters` by all three of
`workflow.ex`. A module attribute cannot cross a module boundary, so the first
travels on `cfg` (the same decision in both bench tasks) and the second pair sits
on `Workflow.Run`, the leaf, behind `namespace/0` and `waiters/0` — one
definition, never a duplicated constant.

## Acyclicity, verified against the tree as it is

| partition | verdict | cross-group edges |
|---|---|---|
| `Roundtrip` / `.Load` / `.Engines` | **ACYCLIC** | `Main → Load` 13, `Main → Engines` 2, everything else **0** |
| `Workflow` / `.Scenarios` / `.Run` | **ACYCLIC** | `Task → Run` 5, `Task → Scenarios` 4, `Scenarios → Run` 101, everything else **0** |

Checked again **as landed**, on the real files rather than the plan: `Load` and
`Engines` contain the string `Roundtrip` only in their own `defmodule` line;
`Run` names neither `Scenarios` nor the task; `Scenarios` names only `Run`.

## N1–N4, the four proofs that replace a benchmark

**N1** — `mix help` for all six tasks, captured from the HEAD tree **before the
first hunk**. Four are byte-identical at the close. Two changed, **deliberately
and once**: `bench` and `bench.floor` both cited
`operative-bench-harness/design.md Decision 6`, a design-decision label pointing
at an archived change, which §5 forbids and which an operator cannot follow. The
golden was re-captured with the diff recorded, never silently.

**N2** — three machine contracts, not the design's two. `write_report/7`'s 27
literal keys and all three `emit_result/1` call-site key lists are unchanged, and
so is a third the design does not name: **`bench.ex`'s `metrics` map**, which
`.github/workflows/bench.yml` reads as `.metrics.receive_window_msg_per_sec`.

**N3** — every timed window's expression sequence, extracted from the area-start
commit and from the landed files with comments and blanks stripped: roundtrip 15
expressions, chain 10, turns 6, wake 12, **all identical**, the only difference
being the sibling-module qualifier on four once-per-run calls.

**N4 — every promotion, with its frequency.**

| | promotions | per-item crossings |
|---|---|---|
| `Roundtrip.Load` | 12 | none |
| `Roundtrip.Engines` | 2 | none |
| `Workflow.Run` | 28 clauses + `namespace/0`, `waiters/0` | `Run.now_ns/0`, 22 sites |
| `Workflow.Scenarios` | 4 | none |

`produce_retry_counter/0` is called per batch, but that call stays **inside**
`Load`; only its two report-time callers cross. The design predicted `now_ms/0`
would also cross and **it does not** — its only caller is `maybe_status/1`,
intra-`Run`.

## The hot path was never touched, and the proof is a byte diff

Design §7 named the Prometheus record funnel a no-touch zone. Over the whole area,
`git diff -U0` on `prometheus_registry.ex` shows hunks at old lines 6, 64, 70, 106
and 441+, and **nothing between 109 and 441** — the entire funnel, `do_record/1`
through `histogram_sample/1`. `signal.ex` has three hunks and all three are inside
the moduledoc: everything from `defstruct` down, `metadata_text/1` included, is
**byte-identical** to the area start.

The one render-path edit, C5, folded three renderers that share a skeleton (the
design proposed two; measured, all three do). Proved by rendering a 65-line
exposition fixture — counters, gauges, histograms, labelled and unlabelled, two
sources, custom buckets — through both arms to the **same sha256**.

## C13 is refused, four ways, and the measurement is the reason

The proposal predicted ≈ −10 lines from collapsing four ns→ms renderers. Measured,
they do not collapse, and each refusal has a different price:

- **`Run.fmt_ms/1` → `Report.ms_from_ns/1`** — 2 011 of 2 016 vector inputs agree;
  **5 disagree**, and every disagreement is `Float.round`'s scientific notation
  for a round value: `Float.round(1000.0, 1)` interpolates as `"1.0e3"` where
  `Report.ms_from_ns` gives `"1000.0"`. Task 5.2's own rule — *a disagreement is a
  behaviour change and stops the fold* — so it is refused. The disagreement is
  itself a live display defect: the workflow bench prints `in 1.0e3 ms` whenever a
  window lands on a round millisecond, which is exactly what
  `bench_report_test.exs`'s "never as a raw float" test exists to prevent.
- **`roundtrip.fmt_ms/1`** — 8 of its 9 call sites are inside `write_report/7`,
  the JSON `summarize.py` and `bench.yml` read as **numbers**. `Report.ms/1`
  returns a string.
- **`Run.rate/2`** — feeds `throughput:` and `commits_s:` in `emit_result`, both a
  JSON and a `BENCH_RESULT` contract. `Report.rate/1` is a display formatter that
  puts thousands separators on a count: the same name, a different question.
- **`Run.mib/1`** — an integer with the unit written in the surrounding text
  (`"#{mib(a)} → #{mib(b)} MiB"`). `Report.bytes/1` auto-scales and returns
  `"31.2 GiB"`.

## The narrative markers: the gate was right six times in seven

Six real, one false positive. Real: `report.ex`'s *"The three tasks **used to**
keep that sequence"* (pure history), `otlp_client.ex`'s *"per **proposal** D8"*,
`prometheus_registry.ex`'s *"per **proposal** D6"*, and **three** sites citing
`operative-bench-harness/design.md Decision 6` — `bench.ex:30`, `bench.ex:301`
and `floor.ex:8`, of which the survey named only the last. False positive:
`counters.ex`'s *"**Used to** log dispatch failures"*, the employed-to sense —
reworded anyway, because *"A caller logs …"* is both clearer and true.

## O1: the floor exists now, and the transient does not

`mix flezha.bench.observability 1000000`, seven identical arms:
**1941 / 1913 / 1896 / 1906 / 1938 / 1921 / 1901 ns per emit.**

| | |
|---|---|
| floor, all 7 | **2.37 %** |
| floor, 6 plateau samples | **2.22 %** |
| run 1 against the plateau median | **+1.46 %**, inside the floor |

So the row does **not** reclassify to `direction` on its floor. It is still not
run, for two reasons that are stronger than noise: its only comparable arm is a
pre-C3 build, which is a `git worktree`, and §11c excludes a worktree arm from
measurement; and C3's change is on the **first-breach** branch, which fires once
per source per boot and cannot be put in a loop — the bench's steady state
returns `:ok` from an ETS read and never reaches it. **That branch's evidence is
batch 0's regression test, an assertion about which process blocks, not a
number.**

**KI-92's cold-start transient is a property of `bench.workflow`'s driver, not of
this box** — and this is the second driver to show it. `mix bench.extension`
(KI-97's own floor: ≤ 4.0 % on the four rows above 100 ns, 16.7 % on a 60 ns row
that can resolve nothing) put run 1 within +3.6 % of its plateau, and this task
puts it within +1.46 %. Against `bench.workflow`'s 303 %.

**O2 was not built, and here is what it would be.** The Prometheus label funnel
is the area's one real hot path and `mix flezha.bench.observability` provably
cannot reach it — it emits against zero interested sinks, so the dispatcher exits
before the registry. A row would need a task that starts `PrometheusRegistry`
alone, builds one `%Signal{}` with a fixed four-label metadata map, calls the
record path N times and reports ns/record, plus a second arm for the
already-legal versus rewritten label split — about 40 lines. It is not built
because this change does not touch the funnel and a benchmark for a path nobody
edited is a new capability funded by nobody. **Owed to whichever change next
edits `do_record/1` or `metadata_text/1`.** The fixture that proves C5 is
one-third of that harness and is in the scratch record.

## The `test` area, and what is and is not mine

`test` is peer-movable (§11d) and area 5c was live in it throughout. Two rows
moved for this change and are attributable: `files_over_800` **7 → 6** — the
951-line `export_test.exs` split three ways, none over 660 — and
`async_false_share` **0.807 → 0.799**, because the pure `describe "OtlpCodec"`
block starts no process, sets no application env and touches no named table, so
its file is `async: true` honestly (verified alone in 0.03 s and under
`--seed 0` beside the rest). The **true** share with the three still-untracked
files counted is **0.797 over 227 files**.

`test_loc` −401, `duplicate_pairs` 112 → 111, `narrative_markers` 37 → 36 and
`refutes` 16 → 15 moved in the same window and are **not this change's** — the
split is line-neutral and deleted no `refute_receive`. They are 5c's.

## Things a successor should not re-derive

- **`refactor_metrics.py`'s function length is per CLAUSE**, from the `def` to
  where the indent closes — not the span from one function name to the next. A
  hand-rolled "next def minus one" rule over-counts a multi-clause function and
  invents ceiling breaches that are not there. Import the script and call
  `measure_file/1` on the paths instead.
- **A `use Mix.Task` moduledoc is `mix help` output** (charter §11g), so §5's
  15-line ceiling does not bind it. Four of this area's seventeen over-ceiling
  docs are Mix tasks and they stay; the other thirteen were trimmed and what had
  to survive moved to `docs/operations/observability.md` — which already carried
  every fact except one: **the driver self-report is UDS-only, so on a QUIC
  controller `driver_write_buffer_dropped` is emitted zero times and a green
  `/healthz` is not evidence about the transport** (KI-72). That lived only in a
  moduledoc and would have been deleted with it.
- **`reuse lint` fails on a tracked file deleted but not staged**, which is what
  a test split looks like before the supervisor commits. It is not a licence
  problem.
