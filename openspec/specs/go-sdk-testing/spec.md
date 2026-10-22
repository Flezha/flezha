# go-sdk-testing Specification

## Purpose
How the Go SDK is proven: its admission to the conformance suite, the harness teardown contract, the tasklet test harness and Go as a measured runtime.
## Requirements
### Requirement: The Go test app is admitted by the conformance suite
`test/apps/go_runner/conformance.json` SHALL declare runtime `go`, `sdk_lang` 8, test and bench profiles, its transports, its dispatch model `thread-pool`, its topology surfaces, and every capability of the vocabulary, with no capability deferred. `go` SHALL be a `workflow_dispatch` option of `.github/workflows/nightly-e2e.yml`, and the manifest's `docs` page SHALL be `docs/internal/sdk/go_sdk.md`. The gate's source-of-truth tables SHALL record where Go spells its language byte and its version constant, and that Go has no package metadata to compare the version against.

#### Scenario: The gate admits the Go app
- **WHEN** `mix test` runs the conformance gate with the Go manifest present
- **THEN** the gate SHALL pass with no change to the harness

#### Scenario: The smoke and full tiers pass on Go
- **WHEN** `FLEZHA_E2E_RUNTIME=go` runs the smoke tier and then the full tier
- **THEN** every carried capability's scenarios SHALL pass and none SHALL be skipped as deferred

### Requirement: The Go runner honours the harness teardown contract
The Go runner SHALL exit when the harness closes its standard input, within the core's grace, and SHALL leave no process, socket file or temporary directory of its own behind.

#### Scenario: Teardown leaves nothing behind
- **WHEN** a conformance scenario ends and the harness closes the runner's port
- **THEN** the runner process SHALL be gone within the grace and its socket files SHALL be removed

### Requirement: A Go tasklet test harness at parity with the other SDKs
The package `flezhatest` SHALL provide `TaskletTestHarness` with the one harness shape every SDK carries: a drive verb that feeds an input and returns the outcome, the cell's state, timers fired on demand, and the replay check that folds the recorded changes again and compares the result with the live state. It SHALL run tasklets without a controller and without the core's network session.

#### Scenario: A harness test drives a tasklet
- **WHEN** a test feeds two inputs to a stateful tasklet through the harness
- **THEN** it SHALL see each outcome's emits and the state after each, and the replay check SHALL pass

#### Scenario: A timer fires on demand
- **WHEN** a test registers a timer through a fed input and fires it through the harness
- **THEN** the tasklet's `OnTimer` SHALL run with the firing's kind and scheduled time

### Requirement: Go is a measured runtime
The acceptance bench SHALL carry a `chain_4_go` case in the quick and the standard profiles, run through the Go manifest's bench profile, with its baseline kept per case and runtime. The stripped size of the Go test app's runner SHALL be measured and stated in the SDK guide beside the Rust runner's size for the same app.

#### Scenario: The quick profile measures Go
- **WHEN** the acceptance bench's quick profile runs
- **THEN** it SHALL report a `chain_4_go` result against Go's own baseline, never against another runtime's

#### Scenario: The guide states a measured size
- **WHEN** the Go SDK guide's packaging section is read
- **THEN** it SHALL state the measured stripped runner size, the date it was measured and the Rust runner's size for the same app

