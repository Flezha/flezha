# bench-run-controls Specification

## Purpose
The options a bench run takes so that its figure can be set beside a published one: latency percentiles from stamped inputs, a paced send rate, a stated and settable payload size, a chosen durability backend with what it persisted, a refusal to measure on an unfit box, and a CPU governor held for the run.
## Requirements
### Requirement: A run without the new options is unchanged

`scripts/bench.sh` and `mix bench` SHALL accept `--latency`, `--rate`, `--payload-bytes` and `--strict`, and `scripts/bench.sh` SHALL also accept `--durability` and `--governor`. A run given none of them SHALL send each relay its inputs in turn, as one burst from the bench process, and SHALL report the throughput windows and metric fields the CI gate reads, plus the payload range and the persisted figures. The durability backend SHALL default to `none` in `scripts/bench.sh`.

#### Scenario: The CI gate's invocation keeps its numbers' meaning
- **WHEN** `mix bench --runtime rust --shape regression -n 100 -m 10000` is run
- **THEN** inputs are sent as one burst from the bench process, `metrics.throughput_msg_per_sec` is the end-to-end rate over the same window as before, and no latency or persisted section is computed from stamped inputs

### Requirement: Latency percentiles are measured from stamped inputs

With `--latency`, every input body SHALL begin with its sequence number and its send time in monotonic nanoseconds. The bench SHALL take the receive time when it takes each forwarded output, and SHALL report p50, p95, p99, p99.9 and max in milliseconds over the measured pass, with the count of measured outputs, of outputs that carried no stamp, and of duplicate outputs for one input. Warm-up inputs SHALL be stamped and SHALL NOT enter the percentiles.

Stamping, timing and parsing SHALL happen in the bench only. No product module SHALL gain work per message for latency to be measured.

A shape whose outputs do not carry their input's stamp SHALL report zero measured outputs and its unstamped count, not an error and not a figure from another source. Under `--latency` the inputs SHALL be sent by a process other than the one receiving the outputs, so that an output's latency does not include time spent waiting behind the bench's own send loop.

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

With `--rate <R>`, R a positive integer, the bench SHALL send inputs round-robin over its relays at R inputs per second in total, from a sender process separate from the receiving one. The rule that decides how many inputs are due SHALL be the one function the `bench.steps` pacers also use. If the achieved send rate ends below 95 % of R, the run SHALL warn that the sender fell behind and that the latency was taken at saturation. `--rate` SHALL be refused with `--via host_inject` and with `--shape cell`, naming the reason.

#### Scenario: A paced run holds its rate
- **WHEN** `./scripts/bench.sh --rate 20000 -n 10 -m 2000` is run on a box that sustains it
- **THEN** the send phase takes about one second, the header names the rate, and no rate warning is printed

#### Scenario: A rate the box cannot reach is reported, not hidden
- **WHEN** `--rate` is set above what the send loop achieves
- **THEN** the run completes, and a warning naming the requested and achieved rates is printed and carried in the report

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

`scripts/bench.sh --governor <name>` SHALL validate the name against the governors the CPU offers, record every CPU's current governor, switch all CPUs to the named one with a non-interactive `sudo`, and restore each CPU's recorded governor when the script exits, whether the bench succeeded, failed or was interrupted. The script SHALL exit with the bench's own status. If `sudo` would prompt, or `cpupower` or the governor files are missing, the script SHALL stop before the bench starts, naming what is missing and how to provide it. Before the run, the script SHALL print the governor it changed from and the command that restores it by hand.

#### Scenario: A powersave laptop runs at performance and goes back
- **WHEN** `./scripts/bench.sh --governor performance` is run on a `powersave` box with a cached `sudo` credential
- **THEN** the header reports governor `performance`, no governor warning is printed, and after the script exits every CPU reads `powersave` again

#### Scenario: An interrupted run still restores the governor
- **WHEN** a `--governor performance` run is interrupted with Ctrl-C during the measurement
- **THEN** every CPU's governor is restored before the script exits

#### Scenario: No cached sudo credential
- **WHEN** `./scripts/bench.sh --governor performance` is run without a cached `sudo` credential
- **THEN** the script stops before the bench starts, telling the user to run `sudo -v` first, and no governor is changed

