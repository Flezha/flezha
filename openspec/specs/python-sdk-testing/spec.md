# python-sdk-testing Specification

## Purpose
TBD - created by archiving change python-sdk. Update Purpose after archive.
## Requirements
### Requirement: The Python Test App Is Admitted by the Conformance Suite

The SDK SHALL ship `test/apps/python_runner/` with a conformance manifest declaring its runtime, command template, `test` and `bench` artifact profiles, build command, connect timeout, transports, deferred capabilities and fixtures, and SHALL be dispatchable as a runtime option in the nightly end-to-end workflow. The suite SHALL pass under `FLEZHA_E2E_RUNTIME=python` for every capability the manifest does not defer, over every transport the manifest lists.

Admitting the runtime SHALL require no change to the shared test helpers, the system suites, the test helper configuration or the conformance gate — a new SDK adds an app, not harness clauses.

#### Scenario: The suite admits the runtime
- **WHEN** `FLEZHA_E2E_RUNTIME=python mix test --include system` is run
- **THEN** every non-deferred capability SHALL pass, and the smoke tier's controller-side declaration snapshot SHALL match the reference

#### Scenario: The manifest is the scope matrix
- **WHEN** the SDK's documentation names the capabilities it carries
- **THEN** it SHALL point at the manifest rather than restate it, and a discrepancy SHALL fail the gate

#### Scenario: The pool-wedge capability is decided by the suite, not by argument
- **WHEN** the wedge drill is run against the Python runner
- **THEN** the capability SHALL be claimed if the fetch-and-run pool produces the wedge signature, and otherwise deferred by name in the manifest with a follow-up recorded

### Requirement: The Python Runner Honours the Harness Teardown Contract

The Python test app SHALL terminate when its standard input reaches end of file, so the harness can stop runners without signals, and SHALL drain in-flight activations before exiting.

#### Scenario: Closing stdin stops the runner
- **WHEN** the harness closes the runner's standard input
- **THEN** the runner SHALL drain its in-flight activations and exit without requiring a signal

### Requirement: A Procedure Test Harness at Parity With the Other SDKs

The SDK SHALL ship a procedure test harness at parity with those the Rust and Java SDKs already provide: construction from an initial state or from a restore payload, application of inputs, timer firing, event-time advance, inspection of recorded changes and emits, and a replay-equality gate that folds the recorded change log against a freshly constructed initial state and reports a mismatch with both states for inspection.

A handler SHALL remain an ordinary callable that a plain unit test can invoke with no runtime present and no controller running.

#### Scenario: Replay equality is gated in a unit test
- **WHEN** a handler whose change log does not reproduce its terminal state is exercised through the harness
- **THEN** the replay-equality gate SHALL report a mismatch carrying the expected and actual states

#### Scenario: A handler is testable with no runtime
- **WHEN** a decorated handler is called directly from a unit test with no runner started
- **THEN** it SHALL execute as an ordinary function

### Requirement: Python Is a Measured Runtime, Not an Assumed One

The round-trip benchmark SHALL accept the Python runtime as an arm, and the SDK SHALL land with per-runtime numbers recorded for it rather than inheriting another runtime's. Measurements SHALL interleave arms inside a round, and SHALL report the wheel tier and executor under which they were taken.

#### Scenario: The bench carries the runtime
- **WHEN** the round-trip benchmark is run with the Python runtime selected
- **THEN** it SHALL launch the Python runner from its manifest and report that runtime's numbers

#### Scenario: A number names its conditions
- **WHEN** a Python performance number is recorded
- **THEN** it SHALL name the wheel tier and the executor, and SHALL NOT be reported as another runtime's number

