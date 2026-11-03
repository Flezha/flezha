# repo-bench Specification

## Purpose

What a bench run takes and reports, so its figure can be set beside a published one. Every run opens
with a header that situates its numbers, warns about an unfit box or build, renders aligned tables
with fixed precision, names the window its headline throughput measures, states how each series
settled, and writes a structured report CI reads. Its options measure latency percentiles from
stamped inputs, pace the send rate, set and state the payload size, choose the durability backend
and measure what it persisted, and hold the CPU governor for the run; a run without them is
unchanged. Bench tasks derive their runtime set, avoid collisions between sessions on one checkout,
and are documented only where the repository ships them.

## Requirements

### Requirement: Every bench run opens with a header that situates its numbers

Every bench task (`mix bench`, `mix bench.steps`) SHALL print, before any measurement, a header composed of three blocks rendered by one shared module:

* **run**: the SDK runtime and its dispatch model (from the manifest's `dispatch` key, with the current value of each env var it names), the transport, whether the runner is embedded or spawned (derived from the transport, not declared), the workload shape, the durability backend, the dimensions (relays, messages per relay, warmup), and `MIX_ENV`. A `mix bench` run SHALL also name the input payload size in bytes as a `min–max` range, the send mode (`burst`, or the paced rate in inputs per second), whether latency is measured, and, when the durability backend is `local_path`, its root path.
* **system**: CPU model and logical core count, total memory, 1-minute load average, CPU governor, OTP release, Elixir version, the launched runtime's own toolchain version, and the git SHA with a dirty flag.
* **artifacts**: for each launched runtime, the resolved artifact path, its mtime and size, whether that path was the profile's first choice or a fallback, and whether the manifest `build` command ran or was skipped.

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

#### Scenario: A default run states its payload size and send mode
- **WHEN** `./scripts/bench.sh -n 100 -m 100` is run with no new option
- **THEN** the run block names a payload range in bytes derived from the inputs the run sends, the send mode `burst`, and latency as off, and the report's `metrics` block carries the payload range as numbers

### Requirement: A bench warns about an unfit box or build without refusing to run

Before the measurement, the shared module SHALL evaluate a fixed set of preflight conditions and print any that hold as a warning block above the numbers, each naming the condition and why it affects the measurement. Each condition SHALL be of one of two kinds.

* **box**: a CPU governor other than `performance`; a 1-minute load average above the logical core count; `MIX_ENV` other than `prod`; a debug-profile artifact resolved for the `bench` profile; a skipped build (`FLEZHA_BENCH_SKIP_BUILD`); a fallback artifact; a dirty git tree.
* **workload**: the remote durability tier off; a durability root the bench did not clear, whose earlier lineages may be restored; a paced send that fell behind its requested rate.

The durability warning SHALL say that the remote durability tier is off and that the number excludes that tier's cost. It SHALL NOT claim that nothing is persisted, because a shape that keeps state still persists to its local WAL with the tier off.

The run SHALL proceed in every case unless it was started with `--strict`. Without `--strict`, no condition SHALL abort a bench, because a number produced on an unfit box is still that box's number and the report SHALL say what was wrong with it. With `--strict`, a run SHALL refuse to start its measurement when any box condition holds, naming every one that holds; workload conditions SHALL NOT refuse a run under `--strict`, because they describe what was asked for.

#### Scenario: A powersave laptop still produces a number, with the reason attached
- **WHEN** a bench is run with the CPU governor set to `powersave` and a dirty working tree, without `--strict`
- **THEN** the warning block names both conditions before the results, the run completes normally, and both appear in the report's `warnings` array

#### Scenario: A clean run prints no warning block
- **WHEN** a bench is run on an idle `performance`-governor host with `MIX_ENV=prod`, a clean tree, a build that ran, and a durability backend other than `none`
- **THEN** no warning block is printed and the report's `warnings` array is empty

#### Scenario: Strict refuses an unfit box before measuring
- **WHEN** `./scripts/bench.sh --strict` is run with the CPU governor set to `powersave` and a dirty working tree
- **THEN** the warning block is printed, the run stops with an error naming both conditions, no input is sent, and the bench's runners and relays are cleaned up

#### Scenario: Strict does not refuse a workload choice
- **WHEN** `./scripts/bench.sh --strict` is run on a fit box with the durability backend `none`
- **THEN** the durability warning is printed and the run completes

#### Scenario: The durability warning tells the truth about a stateful shape
- **WHEN** `./scripts/bench.sh --shape tier` is run with the durability backend `none`
- **THEN** the warning says the remote durability tier is off, and the persisted section reports the local changes and fsyncs the run made

### Requirement: Results render as aligned tables with fixed precision

Bench results SHALL be rendered as aligned plain-text tables by the shared module: a header row, a single horizontal rule, and space-aligned columns. Tables SHALL NOT use box-drawing borders or colour, because the documented comparison steps relay pipes bench output through `tee` and `diff` and the output is read on terminals that are not reliably UTF-8.

Durations SHALL be rendered in milliseconds by the shared module's formatter and rates as integers. No float SHALL reach output text by interpolation: Elixir renders a float of magnitude 1000 or more that lands on a round value in scientific notation, so a window that measures a whole number of milliseconds prints `1.0e3` where a reader and a `diff` expect `1000.0`. One decimal place is the default and what every table uses; a row whose own resolution needs more SHALL ask the shared formatter for the decimals it needs, and SHALL NOT keep a rounding rule of its own.

A value that a bench ships as a **number** SHALL stay a number. The JSON report's `metrics` block is read by summarisers that take medians and compute ratios over its fields, so a formatter returning a string is not the answer there. The shared formatter SHALL therefore be applied where text is rendered rather than where a value is measured, and a task that both prints a value and ships it SHALL render for the one and round for the other.

#### Scenario: Two runs diff cleanly
- **WHEN** two bench runs are captured with `tee` and compared with `diff`
- **THEN** the differing lines are the metric rows alone, and no line differs only in float precision

#### Scenario: Durations are readable
- **WHEN** a phase takes 12281.683999999998 ms of wall time
- **THEN** the table renders `12281.7 ms`

#### Scenario: A window that lands on a round millisecond
- **WHEN** any bench task prints a duration of exactly 1000.0 ms
- **THEN** the output reads `1000.0 ms`, and no bench task prints `1.0e3`

#### Scenario: A printed value and a shipped value part company
- **WHEN** a task prints a measured float and writes the same measurement into the JSON report
- **THEN** the printed one is a fixed-decimal string and the reported one is a JSON number

### Requirement: The headline throughput names the window it measures

A bench SHALL report the **end-to-end** rate — total messages over the window from the first input dispatched to the last response received — as its headline throughput, and SHALL report the receive-window rate beside it; under a paced send, whose drain is only the tail after its last input, it SHALL report the rate the sender held there instead. Each row SHALL name its window in the row label, because the two differ by the send window and are not interchangeable.

The metric `Average time per relay` SHALL be removed. It divides total elapsed time across four unrelated phases by a count of concurrently-running relays and does not measure a per-relay cost.

#### Scenario: Both rates are reported and distinguishable
- **WHEN** a regression bench sends 1,000,000 messages, spending 7664.9 ms sending and 2841.7 ms receiving
- **THEN** the table reports the end-to-end rate over the 10506.6 ms window as the headline, reports the receive-window rate over 2841.7 ms beside it, and each row names its window

#### Scenario: The meaningless average is gone
- **WHEN** any bench shape completes
- **THEN** no output line and no report field reports total elapsed time divided by relay count

### Requirement: Every bench run writes a structured report

Every bench run SHALL write a JSON report through one shared writer, carrying the `run`, `system`, `artifacts` and `warnings` blocks of the header plus a `metrics` block holding every measured value, and SHALL print the report path. The destination path SHALL be supplied by the calling task.

The report SHALL be the only machine-readable contract for a bench number: a tool that reads a bench number SHALL read the report's named field rather than a line of printed prose, so that the printed rendering can change freely.

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

- **WHEN** `mix bench.steps --runtime node` is run and `test/apps/node_runner/conformance.json` lists `steps` under `deferred`
- **THEN** the task SHALL refuse by naming `steps` and the manifest path, and SHALL NOT name a hard-coded set of runtimes in the refusal

#### Scenario: A new SDK gains a test app

- **WHEN** a new `test/apps/<runtime>_runner/conformance.json` is added with an empty `deferred` list
- **THEN** every bench task SHALL accept that runtime with no edit to any task, because each derives the set from the manifests

#### Scenario: A task carries a runtime list in code

- **WHEN** a module under `lib/mix/tasks/bench` decides an accepted runtime set from a clause list or literal collection rather than from `Flezha.Service.PortPilot.runtimes/0` and the manifest's `deferred` key
- **THEN** the conformance gate SHALL fail naming that task, the set it enumerates and the set the manifests advertise

### Requirement: A series' settling SHALL be stated per instrument, and a floor SHALL NOT be computed from a series containing its own warm-up

`docs/internal/operations/benchmarking.md` SHALL state, for each bench instrument, whether that instrument's first run of a series is a result, and SHALL record the measurement that establishes it. A floor, a comparison or a regression verdict SHALL be computed from plateau samples only, and the number of plateau samples SHALL be reported beside the figure, as SHALL the number of runs discarded before the plateau.

The rule SHALL be stated per instrument rather than per machine, because it was measured to differ between instruments on one box: five consecutive runs of one identical arm of `mix bench.steps --scenario chain` read a 303 % spread with run 1 at roughly four times the plateau, while the same five-run series on `mix bench.extension` put run 1 within 3.6 % of its plateau.

Discarding a warm-up SHALL NOT be read as making a row comparable. A row whose measured noise floor exceeds the effect being claimed is a direction and SHALL NOT be run as a gate, and a document that states a settling protocol SHALL say so beside it.

Every measured noise floor SHALL be recorded in tracked repository documentation beside the absolute gate floors, each naming the box it was taken on, the number of runs behind it and the date. A row whose run count or date was never recorded SHALL say so rather than appear beside a fully attributed row as though it were one. A floor that lives only in an untracked working file cannot be read by anyone the repository ships to, and a reader who cannot find it will act on a row that resolves nothing.

#### Scenario: A worker records a floor for an instrument

- **WHEN** a floor is recorded for any bench instrument
- **THEN** the series discards samples until the instrument's plateau, the arms are interleaved, and the recorded figure names how many plateau samples it came from and how many runs were discarded

#### Scenario: An instrument with no warm-up transient

- **GIVEN** an instrument whose first run is measured to sit inside its plateau
- **WHEN** its settling is stated
- **THEN** the document says so for that instrument, and does not generalise either finding to the box

#### Scenario: A settled row that still cannot gate

- **GIVEN** a row whose measured noise floor exceeds the effect a change is claiming
- **WHEN** its result is reported
- **THEN** it is reported as a direction, and no merge or verdict rests on it

#### Scenario: The spread is findable in the shipped repository

- **WHEN** a reader looks for whether a bench row can resolve a given effect
- **THEN** the row's measured spread SHALL be present in tracked documentation beside the absolute floors, with its box, its run count and its date, or with those last two marked unrecorded

### Requirement: A cross-runtime comparison SHALL state its warm-up and decide on a window that resolves it

A comparison between two runtimes SHALL use one untimed warm-up value on every arm, sized so the slowest-warming runtime in the comparison reaches steady state, and SHALL state that value and its dimensions in the report. A warm-up sized for a runtime that starts cold in microseconds measures a just-in-time-compiled runtime before its hottest tier engages and reports that difference as though it were the effect being claimed — a failure mode a same-runtime A/B cannot have, because there the warm-up is symmetric whatever its value.

The harness default is ten untimed messages per relay (`lib/mix/tasks/bench.ex`), sized for a natively compiled runner, and the merge gate's dimensions are chosen to gate rather than to compare. A cross-runtime comparison SHALL therefore state its own warm-up and dimensions rather than inherit either.

Where a workload carries more than one throughput window, the comparison SHALL report every window and SHALL take its verdict only from a window whose measured spread is smaller than the difference claimed — including when that is not the window CI gates. Which window is the quieter one is a measured property of the workload, not a property of what the window means.

#### Scenario: A cross-runtime comparison warms both arms for the slower one

- **WHEN** a comparison is taken between a natively compiled runtime and a just-in-time compiled one
- **THEN** both arms SHALL run with the same untimed warm-up, sized so the just-in-time runtime reaches steady state, and the report SHALL state the value used and the dimensions it was taken at

#### Scenario: The gated window cannot resolve the difference

- **GIVEN** a workload whose gated window spreads wider run-to-run than the arm-to-arm difference being claimed, while another of its windows spreads narrower
- **WHEN** the comparison is reported
- **THEN** both windows SHALL be reported, the verdict SHALL rest on the narrower window only, and the gated window SHALL be marked as deciding nothing

### Requirement: A number a bench prints SHALL be readable, whatever its magnitude

Every float a bench report interpolates SHALL be formatted by the shared helper that
keeps a value out of scientific notation, not only the values that happen to be
durations. Rates, sizes and per-unit counts SHALL be as readable as the timings
beside them.

#### Scenario: A rate is small enough to reach scientific notation

- **WHEN** a bench prints a rate, a per-sleeper size or a per-steps count whose
  float would render as `1.0e3`
- **THEN** it SHALL be formatted by the same helper the duration fields use

### Requirement: A shipped document SHALL NOT document a task the repository excludes

A tracked user document SHALL only document commands whose source ships. Where a
task is deliberately kept out of the repository, the shipped documentation SHALL NOT
instruct a reader to run it.

#### Scenario: A bench task is excluded from the repository

- **WHEN** a Mix task's source is excluded by `.git/info/exclude` or `.gitignore`
- **THEN** no tracked document SHALL present it as a command the reader can run,
  either by shipping the task or by removing the section

### Requirement: Two sessions on one checkout SHALL NOT collide on a fixed port or state root

A test or bench run SHALL derive its controller state root, its socket directory, its
listening ports, its node name and its ProcessHub registry root from something unique to
the run, through one derivation entry per resource over one run token, applied only when
the resource's variable is unset. Two sessions working one checkout SHALL be able to run
their suites at the same time without one failing on `EADDRINUSE`, reading the other's
durable state, or meeting the other's sockets. An explicit variable SHALL win for every
resource in every environment.

#### Scenario: Two suites run at once on one checkout

- **WHEN** two sessions start a suite that binds the data-plane port
- **THEN** neither SHALL fail to bind, and neither SHALL read state the other wrote

#### Scenario: Two suites do not meet on the socket directory or the registry root

- **WHEN** two sessions start a suite on one checkout with no Flezha variables set
- **THEN** each creates its control socket in its own directory and its ProcessHub registry
  under its own state root, and neither boot removes anything the other wrote

#### Scenario: An explicit variable pins a resource in a test run

- **WHEN** a session runs a suite with `FLEZHA_SOCKET_FILES_PATH` set
- **THEN** the controller creates its sockets in that directory, exactly as it would outside
  `mix test`

### Requirement: Published user documentation SHALL NOT drive material the repository excludes

A tracked user document SHALL NOT instruct a reader to run a task, script or stack whose source this repository deliberately keeps out of version control, without saying so at the point of instruction. A fresh clone does not have that material, so the instruction fails with an unknown task and the reader cannot tell a missing dependency from a documentation error.

Where such a harness is described because its results are quoted elsewhere, the document SHALL mark the section as describing development material the repository does not carry, and SHALL state any published fact it depends on rather than sending the reader to the excluded file for it.

#### Scenario: A documented bench task is not in a fresh clone

- **WHEN** a tracked operations document names a Mix task whose source is excluded from version control
- **THEN** the document SHALL say the repository does not carry that task, at the section that names it

#### Scenario: A published number does not depend on excluded material

- **WHEN** a published document quotes a figure produced by an excluded harness
- **THEN** the figure SHALL be stated in the published document rather than cited from the excluded file

### Requirement: A run without the new options is unchanged

`scripts/bench.sh` and `mix bench` SHALL accept `--latency`, `--rate`, `--payload-bytes` and `--strict`, and `scripts/bench.sh` SHALL also accept `--durability` and `--governor`. A run given none of them SHALL send each relay its inputs in turn, as one burst from the bench process, and SHALL report the throughput windows and metric fields the CI gate reads, plus the payload range and the persisted figures. The durability backend SHALL default to `none` in `scripts/bench.sh`.

#### Scenario: The CI gate's invocation keeps its numbers' meaning
- **WHEN** `mix bench --runtime rust --shape regression -n 100 -m 10000` is run
- **THEN** inputs are sent as one burst from the bench process, `metrics.throughput_msg_per_sec` is the end-to-end rate over the same window as before, and no latency or persisted section is computed from stamped inputs

### Requirement: Latency percentiles are measured from stamped inputs

With `--latency`, every input body SHALL begin with its sequence number and its send time in monotonic nanoseconds. The bench SHALL take the receive time when it takes each forwarded output, and SHALL report p50, p95, p99, p99.9 and max in milliseconds over the measured pass, with the count of measured outputs, of outputs that carried no stamp, and of duplicate outputs for one input. Warm-up inputs SHALL be stamped and SHALL NOT enter the percentiles.

Stamping, timing and parsing SHALL happen in the bench only. No product module SHALL gain work per message for latency to be measured.

The unstamped and duplicate counts SHALL cover the measured pass only, like `n`. A shape whose outputs do not carry their input's stamp SHALL report zero measured outputs and its unstamped count, not an error and not a figure from another source. Under `--latency` the inputs SHALL be sent by a process other than the one receiving the outputs, so that an output's latency does not include time spent waiting behind the bench's own send loop.

The latency section SHALL name its send mode: paced at a stated rate, or burst, whose latency includes queueing behind every input sent before it.

#### Scenario: Latency under a paced regression run
- **WHEN** `./scripts/bench.sh --latency --rate 50000 -n 100 -m 1000` is run
- **THEN** the output has a latency section reading `p50 / p95 / p99 / p99.9 / max` in milliseconds with `n=100000`, labelled as paced at 50,000/s, and the report's `metrics.latency_ms` carries the same values as numbers

#### Scenario: Warm-up stays out of the percentiles
- **WHEN** a latency run uses `-w 10` over 100 relays
- **THEN** the 1,000 warm-up outputs are counted as seen and the reported `n` equals the measured inputs only

#### Scenario: A shape that does not echo its input
- **WHEN** `./scripts/bench.sh --latency --shape sleeping` is run
- **THEN** the latency section reports `n=0` with the unstamped count and the run completes

#### Scenario: A burst latency run says what it measured
- **WHEN** `./scripts/bench.sh --latency` is run without `--rate`
- **THEN** the latency section is labelled as burst, including queueing

### Requirement: The send loop can be paced to a fixed total rate

With `--rate <R>`, R a positive integer, the bench SHALL send inputs round-robin over its relays at R inputs per second in total, from a sender process separate from the receiving one. The rule that decides how many inputs are due SHALL be the one function the `bench.steps` pacers also use. If the run's end-to-end window ends later than the schedule's own length plus 5 % of it, and at least 5 ms, the run SHALL warn that it fell behind the rate, in its sender or in the drain, and that its latency includes queueing at saturation. The bench's own receive loop SHALL take a stamp at a cost small enough that a box which keeps up with a rate without `--latency` also keeps up with it with `--latency`. `--rate` SHALL be refused with `--via host_inject` and with `--shape cell`, naming the reason.

#### Scenario: A paced run holds its rate
- **WHEN** `./scripts/bench.sh --rate 20000 -n 10 -m 2000` is run on a box that sustains it
- **THEN** the send phase takes about one second, the header names the rate, and no rate warning is printed

#### Scenario: A rate the box cannot reach is reported, not hidden
- **WHEN** `--rate` is set above what the send loop achieves
- **THEN** the run completes, and a warning naming the requested and the achieved end-to-end rates is printed and carried in the report

#### Scenario: A sender that holds the rate while the drain falls behind
- **WHEN** a paced run's sender holds `--rate` but its outputs drain slower
- **THEN** the same warning is printed, since the latency then measures queueing

#### Scenario: An unsupported combination is refused up front
- **WHEN** `./scripts/bench.sh --rate 1000 --via host_inject` is run
- **THEN** the run stops before the controller boots with an error naming why the two cannot combine

### Requirement: The input payload size can be set and is always stated

With `--payload-bytes <N>`, every input SHALL be padded to exactly N bytes, counting any key prefix and stamp. An N below the unpadded input length SHALL be refused, naming the minimum. A shape whose input is not free text (`sleeping`) SHALL refuse the option. Every run, with or without the option, SHALL report the input size range in bytes in its header and as numbers in its report, computed from the input generator and not sampled in the send loop.

#### Scenario: A fixed payload
- **WHEN** `./scripts/bench.sh --payload-bytes 1024 --latency --rate 10000` is run
- **THEN** every input is 1024 bytes, the header reads `1024–1024 B`, and the latency stamp still parses from each output

#### Scenario: A payload too small for its stamp
- **WHEN** `./scripts/bench.sh --payload-bytes 8 --latency` is run
- **THEN** the run stops before the controller boots with an error naming the minimum size

### Requirement: The durability backend can be chosen, and what was persisted is measured

`scripts/bench.sh --durability <none|local_path|postgres|s3>` SHALL set the controller's durability backend for the run and SHALL refuse any other name. For `local_path`, the script SHALL default the root to a bench-owned directory under `storage/app/bench/` unless `FLEZHA_DURABILITY_PATH` is set, and SHALL mark that store as the bench's own through the `bench_durability_owned` configuration row. The bench SHALL clear a store so marked, whole, before boot, since the tier's keys carry no namespace, and SHALL NOT clear a user-supplied path, a Postgres database or an S3 bucket, warning instead that lineages an earlier run left there may be restored.

Every `mix bench` run SHALL report a persisted section: the growth over the measured window of persisted changes, persisted bytes and fsyncs, summed over the cells of the run's relays and read from the state layer's existing counters before the measured send and after the drain. A run that persisted nothing SHALL say so.

#### Scenario: A stateful shape with the local tier
- **WHEN** `./scripts/bench.sh --durability local_path --shape tier -n 10 -m 1000` is run
- **THEN** the header names `local_path` and its root, no durability warning is printed, and the persisted section reports non-zero changes, bytes and fsyncs

#### Scenario: A stateless shape with durability on
- **WHEN** `./scripts/bench.sh --durability local_path` is run with the default `regression` shape
- **THEN** the persisted section reports that nothing was persisted

#### Scenario: A misspelled backend
- **WHEN** `./scripts/bench.sh --durability postgress` is run
- **THEN** the script stops before running `mix`, naming the four accepted backends

### Requirement: The CPU governor can be set for the run and is restored afterwards

`scripts/bench.sh --governor <name>` SHALL validate the name against the governors the CPU offers, record every CPU's current governor, switch all CPUs to the named one with a non-interactive `sudo`, and restore each CPU's recorded governor when the script exits, whether the bench succeeded, failed or was interrupted. The script SHALL keep the `sudo` credential fresh until that restore, so a build and a run longer than sudo's cache still restore. The script SHALL exit with the bench's own status. If `sudo` would prompt, or `cpupower` or the governor files are missing, the script SHALL stop before the bench starts, naming what is missing and how to provide it. Before the run, the script SHALL print the governor it changed from and the commands that restore each CPU's own governor by hand.

#### Scenario: A powersave laptop runs at performance and goes back
- **WHEN** `./scripts/bench.sh --governor performance` is run on a `powersave` box with a cached `sudo` credential
- **THEN** the header reports governor `performance`, no governor warning is printed, and after the script exits every CPU reads `powersave` again

#### Scenario: An interrupted run still restores the governor
- **WHEN** a `--governor performance` run is interrupted with Ctrl-C during the measurement
- **THEN** every CPU's governor is restored before the script exits

#### Scenario: No cached sudo credential
- **WHEN** `./scripts/bench.sh --governor performance` is run without a cached `sudo` credential
- **THEN** the script stops before the bench starts, telling the user to run `sudo -v` first, and no governor is changed
