# refactor-gates Specification

## Purpose
TBD - created by archiving change refactor-0-baseline. Update Purpose after archive.
## Requirements
### Requirement: The repository measures its own structure and ratchets it

The repository SHALL carry one measuring script, `scripts/refactor_metrics.py`, that reports per area: production and test line counts, comment density split into doc and plain comments, the count of history-narrating comment markers, files over 800 lines, functions over 50 lines, production names over 32 characters, duplicate-code clusters, Elixir dependency cycles, the share of `async: false` ExUnit files, sleep and `refute_receive` counts in tests, lint warning counts, and suite wall-clock when a log is supplied. It SHALL write a baseline (`docs/refactor/baseline.json`), render a target list (`docs/refactor/targets.md`), and check the current tree against the baseline.

#### Scenario: A ratcheted metric regresses in a blocking area

- **WHEN** `scripts/refactor_metrics.py --check` runs on a tree where an area listed as `blocking` in the baseline has a higher value for any ratcheted metric than the baseline records
- **THEN** the script SHALL exit non-zero and name the area, the metric, and the file that caused the regression

#### Scenario: A measuring-only area regresses

- **WHEN** `--check` runs and the regressing area is not listed as `blocking`
- **THEN** the script SHALL print the regression and exit zero

#### Scenario: The baseline records where it was measured

- **WHEN** `--write-baseline` runs
- **THEN** the baseline SHALL carry the hostname, CPU model and governor beside every timing or bench number, and a later `--check` SHALL compare timings only against a baseline from the same hostname

### Requirement: Tests wait for signals, not for time

A test in the repository SHALL NOT sleep to wait for a condition, and SHALL NOT assert silence with a bare `refute_receive`: a wait SHALL observe a positive settle signal the code emits — an existing message, an existing telemetry event, or a state read through an existing public function — and a `refute_receive` SHALL follow such a signal with a timeout of at most 100 ms. A state read that must be repeated SHALL go through the suite's one polling primitive (`NodeTestHelper.wait_until/2,3`), whose interval is the suite's only deliberate sleep. An ExUnit module SHALL be `async: true` unless its `use` line states the global it depends on. A `test/system` module SHALL boot its fleet once per module unless it states why each test needs a fresh one.

#### Scenario: A sleep is introduced into a test

- **WHEN** `scripts/refactor_metrics.py --check` runs on a tree where a file under `test/` contains `Process.sleep` or `:timer.sleep`, or a Rust test contains `thread::sleep`
- **THEN** the check SHALL report the file and, once `test` is a blocking area, exit non-zero

#### Scenario: A flaky test is met

- **WHEN** a test fails in a full run and passes alone
- **THEN** the session that met it SHALL fix its root cause in the test's observation or in the code, and SHALL NOT tag it skipped or flaky

### Requirement: A hot-path change is measured by an interleaved A/B on one box

A change that touches a hot path (the `flz_core` poll core and host, the dispatchers, `flz_transport`, `wal_writer`, `input_log`, the Kafka producer worker, the plugin routing seam) SHALL be measured by an interleaved A/B on the same box against the tag `refactor-baseline-<area>` set at the start of the area: two arms built from the same tree in sequence, the arm artifacts recorded by content hash, at least five rounds with the arm order alternating by round, the baseline arm's run-to-run spread measured and recorded before the changed arm exists, and the hostname, CPU model, governor and power state recorded beside every number. A loss of more than 3 % on any row whose recorded spread is at most 3 % SHALL block the change; a row whose spread exceeds 3 % SHALL be decided by paired rounds (the changed arm below 97 % of the baseline arm in at least four of five rounds). The numbers SHALL be recorded in the change's `design.md` and the verdict as one line in `docs/refactor/STATE.md`; the dispatcher unification SHALL be measured this way as a spike with no other output before any of its code moves.

#### Scenario: A spike measures the dispatcher seam

- **WHEN** the dispatcher-unification spike runs its A/B on this box
- **THEN** every row of its table SHALL carry the baseline arm's pre-recorded spread, five baseline and five changed-arm medians, the artifact hashes of both arms and the governor and power state, and the change SHALL be recorded as "converge" only when every row's loss is at most 3 % under the paired-round rule

### Requirement: The kernel areas ratchet as blocking and the erased path has a bench

Once `refactor-1-kernel` lands, the areas `flz_core`, `flz_transport` and `flz_nif` SHALL be listed under `blocking` in `docs/refactor/baseline.json`, re-baselined from the landed tree, so that any later rise in a ratcheted metric for those areas fails `scripts/refactor_metrics.py --check`. The repository SHALL carry a bench that drives the erased host path through the real `flz_core_poll` / `flz_core_complete` exports (`flz_core_ffi/benches/host_loop.rs`) so that a hot-path A/B on the kernel has a row for the path no typed bench reaches, and every kernel A/B after this change SHALL include that row at a measurement window whose recorded noise floor is at most 3 %.

#### Scenario: A kernel metric regresses after the flip

- **WHEN** `scripts/refactor_metrics.py --check` runs on a tree where `flz_core`, `flz_transport` or `flz_nif` has more files over 800 lines, more functions over 50 lines, more narrative markers, more production names over 32 characters, more duplicate pairs, or more production lines than the re-baselined value
- **THEN** the check SHALL exit non-zero naming the area, the metric and the file

#### Scenario: A kernel change measures the erased path

- **WHEN** a change touches `flz_core`'s poll core, host, flusher, entity or routing modules and runs the charter's interleaved A/B
- **THEN** its table SHALL carry the `host_loop` row with its pre-recorded floor, both arms' medians and allocations per element, and a loss beyond the floor on that row SHALL block the change like any other row

### Requirement: The FFI and veneer areas ratchet as blocking and the veneer shape has one implementation

Once `refactor-2-ffi-veneers` lands, the areas `flz_core_ffi`, `sdk/node`, `sdk/ruby`, `sdk/php` and `sdk/python` SHALL be listed under `blocking` in `docs/refactor/baseline.json`, re-baselined from the landed tree, so that any later rise in a ratcheted metric for those areas fails `scripts/refactor_metrics.py --check`. The crate `flz_core_ffi` SHALL carry exactly one implementation of the activation and result shapes its language bindings present (`flz_core_ffi/src/veneer/shape.rs`), parameterised over how a language builds and reads a value, and one implementation of the language-neutral half of a binding (`flz_core_ffi/src/veneer/mod.rs`: context creation, the last-error read, the out-buffer hand-off, the typed calls and the refusal shape), so that a binding for a new language adds only its value primitives and its exception mapping.

#### Scenario: An FFI or veneer metric regresses after the flip

- **WHEN** `scripts/refactor_metrics.py --check` runs on a tree where `flz_core_ffi`, `sdk/node`, `sdk/ruby`, `sdk/php` or `sdk/python` has more files over 800 lines, more functions over 50 lines, more narrative markers, more production names over 32 characters, more duplicate pairs, more test sleeps, or more production lines than the re-baselined value
- **THEN** the check SHALL exit non-zero naming the area, the metric and the file

#### Scenario: A binding maps an activation through the one shape

- **WHEN** `cargo test -p flz_core_ffi` runs under `--features napi`, `--features python` or `--features ruby-embed`
- **THEN** the ABI scenario SHALL prove the shape once in Rust against a recording sink and source, and each enabled binding SHALL prove only that its own value primitives round-trip through its runtime, with no per-language copy of the activation vocabulary in the crate's source or tests

### Requirement: The Rust SDK areas ratchet as blocking and the typed path settles through the shared seam

Once `refactor-3-rust-sdk` lands, the areas `sdk/rust` and `flezha-macros` SHALL be listed under `blocking` in `docs/refactor/baseline.json`, re-baselined from the landed tree, so that any later rise in a ratcheted metric for those areas fails `scripts/refactor_metrics.py --check`. The Rust SDK SHALL settle an invocation's effects through `flz_core::effect::settle_effects` and buffer them in `flz_core::effect::EffectBuffer`, carrying no second implementation of the settlement tail, of the per-Entity flush buffer or of the interval close, so that the typed and erased paths cannot drift; and the SDK's benches SHALL take their scaffolding from `flz_core::bench_support` rather than re-implementing it, so that a bench row measures the code and not a per-file harness.

#### Scenario: A Rust SDK metric regresses after the flip

- **WHEN** `scripts/refactor_metrics.py --check` runs on a tree where `sdk/rust` or `flezha-macros` has more files over 800 lines, more functions over 50 lines, more narrative markers, more production names over 32 characters, more duplicate pairs, more test sleeps, or more production lines than the re-baselined value
- **THEN** the check SHALL exit non-zero naming the area, the metric and the file

#### Scenario: The typed dispatcher settles an invocation

- **WHEN** `ProcedureDispatcherImpl` completes one invocation, inside or outside the effectively-once tier
- **THEN** the changes, outputs, consumed offset, timer ops and park SHALL be settled by the same `settle_effects` call the C-ABI host uses, into an `EffectBuffer` whose change type is supplied by a `Bodies` marker, with the tier-off park handed back to the caller

#### Scenario: A Rust SDK bench declares its scaffolding

- **WHEN** `cargo bench -p flezha_runner --features bench-internals` builds any bench target
- **THEN** the stream pair, the dispatch fan-in, the settle wait, the measurement window and the allocation counter SHALL come from `flz_core::bench_support`, and the crate's `bench-internals` feature SHALL enable `flz_core/bench-internals` so that a bench built through `cargo bench -p flezha_runner` alone reaches them

#### Scenario: The workspace test run needs no feature flag

- **WHEN** `cargo test --workspace` runs with no `--features` argument
- **THEN** every `flezha_runner` test target that imports `flezha_runner::__dispatch_internals` SHALL declare `required-features = ["bench-internals"]`, so the run compiles and the targets that need the feature are skipped rather than failing to build

### Requirement: A recorded metric SHALL name the scan that produced it

A metric whose value depends on which files were scanned SHALL record that scan
alongside the value. A comparison between two recordings made under different
scans SHALL be refused, not reported — the numbers describe different questions,
and a gate that compares them reports a change nobody made.

Refusal SHALL be an error, not a warning. A warning in a gate is a number nobody
reads, which is the same defect the requirement exists to prevent.

#### Scenario: A cross-scan comparison is refused

- **GIVEN** a recorded baseline produced under one set of scanned areas
- **WHEN** the check runs over a different set
- **THEN** it SHALL refuse the comparison and name the difference, rather than report a delta

#### Scenario: A same-scan comparison proceeds

- **GIVEN** a baseline and a run over the same set of areas
- **WHEN** the check runs
- **THEN** it SHALL compare them normally

### Requirement: The Java SDK area ratchets as blocking and its host splits by owner

Once `refactor-4-java-sdk` lands, the area `sdk/java` SHALL be listed under `blocking` in `docs/refactor/baseline.json`, re-baselined from the landed tree over the full scan set, so that any later rise in a ratcheted metric for that area fails `scripts/refactor_metrics.py --check`. The Java host SHALL carry no source file over 800 lines and no production function over 50 lines; its activation dispatcher SHALL be split by the state it mutates — one file for the instance map, one for a live operative, and one each for the emit context, the entity routing, the state exchange and the workflow arm — rather than by activation kind; and a consumer edge's fused transform closure SHALL be applied through a single shared function, so that the producer-side and delivery-side applications cannot drift from one another.

#### Scenario: A Java SDK metric regresses after the flip

- **WHEN** `scripts/refactor_metrics.py --check` runs on a tree where `sdk/java` has more files over 800 lines, more functions over 50 lines, more narrative markers, more production names over 32 characters, more duplicate pairs, more test sleeps, or more production lines than the re-baselined value
- **THEN** the check SHALL exit non-zero naming the area, the metric and the file

#### Scenario: The edge transform has one implementation

- **WHEN** the Java host applies a consumer edge's transform chain, either when a producing activation completes or when a consumer receives the value
- **THEN** both SHALL call the same shared application function, so that changing which site is authoritative is a change at one place

#### Scenario: A split file carries only the imports it uses

- **WHEN** a batch of this change creates a new Java source file by extracting from an existing one
- **THEN** `mvn -f sdk/java/pom.xml -DskipTests spotless:apply` SHALL run before the batch reports its metrics, so that a copied import block cannot raise `duplicate_pairs` for source that did not change

#### Scenario: The Java host is measured by a row that can resolve the claim

- **WHEN** this change reports its interleaved A/B for the Java host
- **THEN** each row SHALL be declared `gate`, `guard` or `direction` with its measured floor, the whole-system receive window SHALL be reported as `direction` only, and a row whose floor cannot resolve the effect being claimed SHALL be recorded as such rather than run as a gate

### Requirement: The CLI area ratchets as blocking and every operator verb runs one RPC through one seam

Once `refactor-6-cli` lands, the area `cli` SHALL be listed under `blocking` in `docs/refactor/baseline.json`, re-baselined from the landed tree over the full scan set, so that any later rise in a ratcheted metric for that area fails `scripts/refactor_metrics.py --check`. The `flz` binary SHALL carry no source file over 800 lines and no production function over 50 lines; every invocation of `flzvm rpc` SHALL go through a single shared function that owns spawning, failure reporting and the process exit code, so that no operator verb can reach the controller by a path that reports failure differently from its siblings; and the help text for every command SHALL be rendered from one declarative table by one renderer, so that two screens describing the same command grammar cannot drift apart.

#### Scenario: A CLI metric regresses after the flip

- **WHEN** `scripts/refactor_metrics.py --check` runs on a tree where `cli` has more files over 800 lines, more production functions over 50 lines, more narrative markers, more production names over 32 characters, more duplicate pairs, or more production lines than the re-baselined value
- **THEN** the check SHALL exit non-zero naming the area, the metric and the file

#### Scenario: A verb reaches the controller without the shared seam

- **WHEN** a `cli` source file spawns the controller launcher with an `rpc` argument other than through the shared run function
- **THEN** that SHALL be treated as a regression of this requirement, because the exit code a verb returns is then decided in a second place

#### Scenario: An RPC that fails is never reported as success

- **WHEN** the controller launcher exits non-zero for any `flz` verb that issues an RPC
- **THEN** `flz` SHALL exit non-zero, and a test driving a stand-in launcher that always fails SHALL assert this for at least one verb of every failure policy the seam offers

#### Scenario: The help text is regenerated from the table

- **WHEN** a batch of this change converts a hand-written help printer to a table entry
- **THEN** the rendered output SHALL equal, byte for byte, the output captured from the binary before the change, and any correction to that text SHALL land in a separate commit that also updates the captured file

#### Scenario: The CLI is measured by rows that can resolve the claim

- **WHEN** this change reports its interleaved A/B
- **THEN** each row SHALL be declared `gate`, `guard` or `direction` with its measured floor before it is run, and a row whose measured floor exceeds the effect being claimed SHALL be recorded as `direction` with the binary's checksum as its evidence rather than run

