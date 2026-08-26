# node-sdk-testing Specification

## Purpose
TBD - created by archiving change node-sdk. Update Purpose after archive.
## Requirements
### Requirement: The Node Test App Is Admitted by the Conformance Suite

The SDK SHALL ship a Node test app with a conformance manifest declaring its runtime, command
template, test and bench artifact profiles, build command, connect timeout, transports, deferred
capabilities and fixtures, and SHALL be dispatchable as a runtime option in the nightly end-to-end
workflow. The suite SHALL pass for every capability the manifest does not defer, over every
transport the manifest lists.

Admitting the runtime SHALL require no change to the shared test helpers, the system suites or the
test helper configuration — a new SDK adds an app, not harness clauses. The conformance gate SHALL
take one row of data: where that SDK spells its language byte, so the manifest is checked against
the constant rather than trusted.

#### Scenario: The suite admits the runtime
- **WHEN** the end-to-end suite is run against the Node runtime
- **THEN** every non-deferred capability SHALL pass, and the smoke tier's controller-side
  declaration snapshot SHALL match the reference

#### Scenario: The manifest is the scope matrix
- **WHEN** the SDK's documentation names the capabilities it carries
- **THEN** it SHALL point at the manifest rather than restate it, and a discrepancy SHALL fail the
  gate

### Requirement: Both Dispatch Configurations Are an Axis of the Run

The dispatch loop count SHALL be an axis of the end-to-end run rather than a property of a fixture
file. One test app SHALL be run under both the single-loop and multi-loop configurations, and there
SHALL be no per-configuration fixture set.

#### Scenario: One app proves both configurations
- **WHEN** the suite is run under each dispatch configuration
- **THEN** the same fixtures SHALL execute, and results SHALL be identical for every capability that
  does not concern dispatch topology

### Requirement: The Pool-Wedge Capability Is Decided by the Drill

The manifest SHALL claim the pool-wedge capability if the multi-loop configuration produces the
wedge signature under the drill, and SHALL otherwise defer it by name with a recorded follow-up. The
claim SHALL NOT be asserted from the design.

#### Scenario: The drill decides the claim
- **WHEN** the wedge drill is run against the Node runner
- **THEN** the capability SHALL be claimed or deferred according to the drill's result, and the
  manifest SHALL record which

### Requirement: The Node Runner Honours the Harness Teardown Contract

The Node test app SHALL terminate when its standard input reaches end of file, so the harness can
stop runners without signals, and SHALL drain in-flight activations before exiting.

#### Scenario: Closing stdin stops the runner
- **WHEN** the harness closes the runner's standard input
- **THEN** the runner SHALL drain its in-flight activations and exit without requiring a signal

### Requirement: A Procedure Test Harness at Parity With the Other SDKs

The SDK SHALL ship a procedure test harness at parity with those the Rust and Java SDKs provide:
construction from an initial state or from a restore payload, application of inputs, timer firing,
event-time advance, inspection of recorded changes and emits, and a replay-equality gate that folds
the recorded change log against a freshly constructed initial state and reports a mismatch with both
states for inspection.

A handler SHALL remain an ordinary function that a plain unit test can invoke with no runtime present
and no controller running.

#### Scenario: Replay equality is gated in a unit test
- **WHEN** a handler whose change log does not reproduce its terminal state is exercised through the
  harness
- **THEN** the replay-equality gate SHALL report a mismatch carrying the expected and actual states

#### Scenario: A handler is testable with no runtime
- **WHEN** a defined handler is called directly from a unit test with no runner started
- **THEN** it SHALL execute as an ordinary function

### Requirement: Node Is a Measured Runtime, Not an Assumed One

The round-trip benchmark SHALL accept the Node runtime as an arm, and the SDK SHALL land with
per-runtime numbers recorded for it rather than inheriting another runtime's. Measurements SHALL
interleave arms inside a round, and SHALL report the dispatch configuration under which they were
taken.

#### Scenario: Numbers are attributed to the runtime and configuration
- **WHEN** the round-trip benchmark is run for the Node runtime
- **THEN** the recorded results SHALL name the runtime and the dispatch configuration, and SHALL NOT
  be reported as another runtime's

### Requirement: The Package Has Its Own Test and Build Workflow

The SDK SHALL ship a continuous-integration workflow running its unit tests, its type-check
fixtures including those expected to fail compilation, and its native binary builds for every
supported platform.

#### Scenario: A broken refusal fails the build
- **WHEN** a compile-time refusal stops working
- **THEN** the fixture asserting that refusal SHALL fail the workflow

