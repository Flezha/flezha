# bench-reporting Specification

## Purpose
What a bench run reports: the header that situates a number, the preflight
warnings that qualify it, the tables that present it, and the structured
report that is the machine contract CI reads.
## Requirements
### Requirement: Every bench run opens with a header that situates its numbers

Every bench task (`mix bench`, `mix bench.roundtrip`, `mix bench.workflow`) SHALL print, before any measurement, a header composed of three blocks rendered by one shared module:

* **run** — the SDK runtime and its dispatch model (from the manifest's `dispatch` key, with the current value of each env var it names), the transport, whether the runner is embedded or spawned (derived from the transport, not declared), the workload shape, the durability backend, the dimensions (operatives, messages per operative, warmup), and `MIX_ENV`.
* **system** — CPU model and logical core count, total memory, 1-minute load average, CPU governor, OTP release, Elixir version, the launched runtime's own toolchain version, and the git SHA with a dirty flag.
* **artifacts** — for each launched runtime: the resolved artifact path, its mtime and size, whether that path was the profile's first choice or a fallback, and whether the manifest `build` command ran or was skipped.

The blocks SHALL be derived from the same sources the run itself uses; no field SHALL be re-derived by a second path that can disagree with the first. On a platform where a field cannot be read the field SHALL be reported as unavailable and the run SHALL proceed.

#### Scenario: A header names the machine, the build and the topology
- **WHEN** `./scripts/bench.sh -r node -n 100 -m 100` is run with `FLEZHA_NODE_WORKERS=4`
- **THEN** the header names the node runtime with its `event-loop` dispatch model and `FLEZHA_NODE_WORKERS=4`, the uds transport, the CPU model, core count and governor, the OTP/Elixir/node versions, the git SHA, and the resolved `index.mjs` path with its mtime

#### Scenario: A field that cannot be read does not stop the run
- **WHEN** a bench runs on a host with no `/proc/cpuinfo`
- **THEN** the CPU model and governor rows read `unavailable`, the BEAM-derived rows are still populated, and the measurement proceeds

#### Scenario: The artifact reported is the artifact launched
- **WHEN** a runtime's first-choice artifact is absent and a later candidate is used
- **THEN** the header's artifact row names the path actually launched, marks it a fallback, and names the first choice that was missing

### Requirement: A bench warns about an unfit box or build without refusing to run

Before the measurement, the shared module SHALL evaluate a fixed set of preflight conditions and print any that hold as a warning block above the numbers, each naming the condition and why it affects the measurement. The conditions SHALL be: a CPU governor other than `performance`; a 1-minute load average above the logical core count; `MIX_ENV` other than `prod`; a debug-profile artifact resolved for the `bench` profile; a skipped build (`FLEZHA_BENCH_SKIP_BUILD`); a fallback artifact; a dirty git tree; and a disabled durability backend.

The run SHALL proceed in every case. No condition SHALL abort a bench, because a number produced on an unfit box is still that box's number and the report SHALL say what was wrong with it.

#### Scenario: A powersave laptop still produces a number, with the reason attached
- **WHEN** a bench is run with the CPU governor set to `powersave` and a dirty working tree
- **THEN** the warning block names both conditions before the results, the run completes normally, and both appear in the report's `warnings` array

#### Scenario: A clean run prints no warning block
- **WHEN** a bench is run on an idle `performance`-governor host with `MIX_ENV=prod`, a clean tree, and a build that ran
- **THEN** no warning block is printed and the report's `warnings` array is empty

### Requirement: Results render as aligned tables with fixed precision

Bench results SHALL be rendered as aligned plain-text tables by the shared module: a header row, a single horizontal rule, and space-aligned columns. Tables SHALL NOT use box-drawing borders or colour, because the documented comparison workflow pipes bench output through `tee` and `diff` and the output is read on terminals that are not reliably UTF-8.

Durations SHALL be rendered in milliseconds to one decimal place and rates as integers. No raw float SHALL reach the output.

#### Scenario: Two runs diff cleanly
- **WHEN** two bench runs are captured with `tee` and compared with `diff`
- **THEN** the differing lines are the metric rows alone, and no line differs only in float precision

#### Scenario: Durations are readable
- **WHEN** a phase takes 12281.683999999998 ms of wall time
- **THEN** the table renders `12281.7 ms`

### Requirement: The headline throughput names the window it measures

A bench SHALL report the **end-to-end** rate — total messages over the window from the first input dispatched to the last response received — as its headline throughput, and SHALL report the receive-window rate beside it. Each row SHALL name its window in the row label, because the two differ by the send window and are not interchangeable.

The metric `Average time per operative` SHALL be removed. It divides total elapsed time across four unrelated phases by a count of concurrently-running operatives and does not measure a per-operative cost.

#### Scenario: Both rates are reported and distinguishable
- **WHEN** a regression bench sends 1,000,000 messages, spending 7664.9 ms sending and 2841.7 ms receiving
- **THEN** the table reports the end-to-end rate over the 10506.6 ms window as the headline, reports the receive-window rate over 2841.7 ms beside it, and each row names its window

#### Scenario: The meaningless average is gone
- **WHEN** any bench shape completes
- **THEN** no output line and no report field reports total elapsed time divided by operative count

### Requirement: Every bench run writes a structured report, and CI reads that report

Every bench run SHALL write a JSON report through one shared writer, carrying the `run`, `system`, `artifacts` and `warnings` blocks of the header plus a `metrics` block holding every measured value, and SHALL print the report path. The destination path SHALL be supplied by the calling task.


The report SHALL be the only machine-readable contract for a bench number. The `bench-regression` CI job SHALL read the recorded throughput from the report's named field rather than matching a line of printed prose, so that the printed rendering can change without breaking a merge-blocking gate.

#### Scenario: CI reads a field, not a sentence
- **WHEN** the `bench-regression` job runs the regression bench
- **THEN** it reads the end-to-end throughput from the report's `metrics` block, and no step greps the printed output for a throughput value

#### Scenario: A report is written even when the output is redirected
- **WHEN** a bench is run with its stdout piped to a file
- **THEN** the JSON report is still written to its path and the path is named in the captured output

### Requirement: The bench log level is a deliberate choice

Every bench task SHALL take its logger level from `FLEZHA_BENCH_LOG`, defaulting to `:warning`. No bench task SHALL hardcode a level, and no bench task SHALL filter, allowlist or rewrite log lines: a warning a bench provokes is either a real condition to be fixed at its source or evidence that a classifier is wrong, and neither is served by suppression.

#### Scenario: A run's diagnostics can be turned up without editing code
- **WHEN** `FLEZHA_BENCH_LOG=info ./scripts/bench.sh` is run
- **THEN** controller info-level diagnostics appear in the output for every bench task

#### Scenario: No suppression layer exists
- **WHEN** the bench harness is inspected
- **THEN** no module filters, drops or rewrites log messages emitted by the controller during a bench

### Requirement: A bench task derives its accepted runtime set and states no list of its own

Every bench task SHALL decide which `--runtime` values it accepts by reading the conformance manifests (`test/apps/<runtime>_runner/conformance.json`) at run time, and SHALL refuse a runtime the task cannot measure by naming the capability that runtime's manifest defers and the path of the manifest that says so. No bench task SHALL enumerate runtimes in a clause list, a literal collection, or a documentation line; a per-runtime clause is a list a new SDK has to remember to join, and a task carrying one silently refuses a runtime whose test app already exists.

#### Scenario: A task is asked for a runtime whose manifest defers the capability it measures

- **WHEN** `mix bench.workflow --runtime node` is run and `test/apps/node_runner/conformance.json` lists `workflow` under `deferred`
- **THEN** the task SHALL refuse by naming `workflow` and the manifest path, and SHALL NOT name a hard-coded set of runtimes in the refusal

#### Scenario: A new SDK gains a test app

- **WHEN** a new `test/apps/<runtime>_runner/conformance.json` is added with an empty `deferred` list
- **THEN** every bench task SHALL accept that runtime with no edit to any task, because each derives the set from the manifests

#### Scenario: A task carries a runtime list in code

- **WHEN** a module under `lib/mix/tasks/bench` decides an accepted runtime set from a clause list or literal collection rather than from `Flezha.Service.PortPilot.runtimes/0` and the manifest's `deferred` key
- **THEN** the conformance gate SHALL fail naming that task, the set it enumerates and the set the manifests advertise

