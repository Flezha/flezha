# python-sdk-packaging Specification

## Purpose
TBD - created by archiving change python-sdk. Update Purpose after archive.
## Requirements
### Requirement: The Python Binding Is a Feature of the ABI Crate, Not a New One

The PyO3 binding SHALL be built from `flz_core_ffi` under a `python` feature, as a one-to-one veneer over the same C entry points in the same binary, mirroring the existing `jni` feature. It SHALL NOT be a separate crate, SHALL NOT reimplement schema negotiation, buffer ownership or error mapping, and SHALL NOT introduce a transport client, framing or reconnect implementation of its own.

#### Scenario: The veneer is a consumer, not a fork
- **WHEN** the Python veneer's exported functions are compared against the ABI's exported symbol set
- **THEN** each SHALL map onto a `flz_core_ffi` entry point, and any Python-reachable kernel capability that bypasses the ABI SHALL be a defect

#### Scenario: The veneer is provable without Python
- **WHEN** the crate's own test suite runs with the `python` feature enabled
- **THEN** a full scenario SHALL be driven through the veneer with no external Python process involved

### Requirement: Schema Version Negotiation Precedes Any Connection

The binding SHALL negotiate the core's schema version at context creation, and a mismatch SHALL raise an error naming both versions before any controller connection is attempted. A wheel built against a different core schema SHALL NOT be able to reach the wire.

#### Scenario: A mismatched build fails before connecting
- **WHEN** a wheel whose embedded core reports a different schema version is loaded
- **THEN** context creation SHALL raise naming both versions, and no socket SHALL be opened

### Requirement: Two Wheel Tiers, One Source and One API

The SDK SHALL declare a supported Python floor no lower than 3.11, and SHALL publish a GIL tier as one `abi3` wheel per supported platform targeting that floor, plus a free-threaded tier as per-version wheels for each supported free-threaded interpreter. Both tiers SHALL be built from one source with one public API and one set of semantics; they SHALL differ only in the interpreter build they target. Prebuilt wheels SHALL be published for the supported platform triples so that installation requires no Rust toolchain; a source distribution SHALL remain as the fallback for platforms without one.

The free-threaded tier SHALL be published from the SDK's first release rather than deferred, because it is the only change that lifts the SDK's single documented concurrency ceiling.

#### Scenario: The floor is what the compatibility wheel targets
- **WHEN** the `abi3` wheel is inspected
- **THEN** it SHALL target the declared floor, so one wheel serves every supported interpreter at or above it

#### Scenario: Installation needs no toolchain
- **WHEN** the SDK is installed on a supported platform in an image with no Rust toolchain, under either tier
- **THEN** installation SHALL succeed from a prebuilt wheel and the runner SHALL start

#### Scenario: The source distribution still builds
- **WHEN** the SDK is installed on a platform with no prebuilt wheel, in an image with a Rust toolchain
- **THEN** the source distribution SHALL build and the runner SHALL start

#### Scenario: Both tiers pass the same suite
- **WHEN** the SDK's unit suite runs under the GIL tier and again under a free-threaded interpreter
- **THEN** both SHALL pass with identical semantics

### Requirement: Payloads Cross the Boundary Without an Intermediate Copy

The binding SHALL expose an activation's user payload to the codec as a borrowed view over the core's buffer, so that the common decode path allocates no intermediate byte object. A user who requests raw bytes SHALL receive a copy. The borrowed view's validity SHALL end with the invocation, and every core buffer SHALL be released through the ABI's free entry point before the binding returns, so no native buffer escapes into Python.

#### Scenario: The common path copies nothing
- **WHEN** an activation is decoded through a built-in codec adapter
- **THEN** no intermediate byte object SHALL be allocated for the payload

#### Scenario: No native buffer escapes
- **WHEN** an activation completes, including one whose handler raised
- **THEN** every core buffer handed out for it SHALL have been freed through the ABI

### Requirement: Errors and Panics Cross the Boundary Intact

An exception raised by a Python handler SHALL be surfaced to the engine as a procedure error carrying the exception type, message and traceback, and SHALL be subject to the same retry policy as a Rust handler error; an exception marked non-retryable SHALL skip the remaining retry budget and park exactly as its Rust counterpart does. An error originating in the engine and surfaced to Python SHALL preserve the engine's specific detail rather than a generic failure. The boundary SHALL NOT allow a Python exception to unwind into Rust as a panic, nor a Rust panic to reach Python as an opaque failure.

#### Scenario: A handler exception is retried then parked
- **WHEN** a Python handler raises on every attempt and the channel's retry budget is three
- **THEN** the SDK SHALL invoke it three times and then park the input to the DLQ with the Python traceback recorded in the entry

#### Scenario: A non-retryable exception parks immediately
- **WHEN** a Python handler raises an exception marked non-retryable
- **THEN** the SDK SHALL park after a single attempt without consuming the remaining budget

#### Scenario: An engine error reaches Python intact
- **WHEN** a topology build fails inside the engine
- **THEN** the Python-side exception SHALL carry the engine's specific issue, not a generic failure

### Requirement: The wheels are built from the wrapper crate

maturin SHALL build both wheel tiers from the wrapper crate in the Python
tree, whose one dependency is `flz_core_ffi` at the pinned core tag with the
`python-extension` feature and, for the compatibility tier, `python-abi3`.
`pyproject.toml` SHALL name no path outside the Python tree.

#### Scenario: A wheel builds from the Python tree alone
- **WHEN** `maturin build --release` runs in the Python repository with no core tree beside it
- **THEN** it SHALL fetch the pinned tag and produce a wheel whose module exports `PyInit_flz_core_ffi`

#### Scenario: Both tiers ship
- **WHEN** a release completes
- **THEN** the assets SHALL include one `abi3` wheel per triple and one free-threaded wheel per triple per supported interpreter

