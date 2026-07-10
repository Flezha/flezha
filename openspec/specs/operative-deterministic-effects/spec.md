# operative-deterministic-effects Specification

## Purpose
The determinism contract for operative code: ctx-mediated time (`ctx.now()` served from the declared Clock, including wired `EventTimeFrom`/`LogicalClock`), ctx-mediated stable identifiers (`ctx.stable_id()`), build-time clock validation, and harness verification that replay reproduces identical emits. Binds all current and future SDK languages.

## Requirements

### Requirement: Determinism contract for operative code

The SDK SHALL define a determinism contract binding `Operation::run`,
`apply_change`, and `compensate`: given the same Segment state and the same input
sequence, the code SHALL produce identical state changes and identical emitted
outputs. Ambient non-determinism (wall-clock reads, random number / UUID
generation, reads of external mutable state) inside these functions is a contract
violation; time and identifiers SHALL be obtained through `OperationCtx`. The
contract SHALL bind all current and future SDK languages.

#### Scenario: Contract is documented on the authoring surface

- **WHEN** an author reads the `Operation` trait / `run` documentation
- **THEN** the determinism contract is stated, including the ctx-mediated
  alternatives for time (`ctx.now()`) and identifiers (`ctx.stable_id()`)

#### Scenario: Violation surfaces as a replay difference in the harness

- **WHEN** an Operation whose `run` calls `SystemTime::now()` to build its output
  is checked with the harness replay-emit verification
- **THEN** the check fails because the two replay passes emit different bytes

### Requirement: Runtime-evaluated Clock served via `ctx.now()`

The dispatcher SHALL evaluate the Operation's declared `Clock` exactly once per
`run` invocation, before user code executes, and `OperationCtx::now()` SHALL return
that single value for the entire invocation. The static `Clock::now()` entry point
SHALL NOT exist on the trait surface.

#### Scenario: One value per invocation

- **WHEN** `run` calls `ctx.now()` multiple times during one invocation
- **THEN** every call returns the same value

#### Scenario: WallClock reads dispatch-entry time

- **WHEN** an Operation declares `type Clock = WallClock` and `run` calls
  `ctx.now()`
- **THEN** the value is the system time read at dispatch entry for that invocation

### Requirement: EventTimeFrom extracts time from the current input

`EventTimeFrom<F>` SHALL be fully wired: `F` implements a `TimeExtractor<Input>`
supplying `fn extract(&Input) -> u64`, the dispatcher SHALL apply it to the input
being dispatched, and `ctx.now()` SHALL return the extracted value. The result
SHALL depend only on the input.

#### Scenario: Event time is replay-stable

- **WHEN** the same input is dispatched twice (original run and replay) to an
  Operation declaring `EventTimeFrom<F>`
- **THEN** `ctx.now()` returns the identical value in both invocations

#### Scenario: Extractor sees the dispatched input

- **WHEN** an input carrying event timestamp `1720000000000` is dispatched
- **THEN** `ctx.now()` inside that invocation returns `1720000000000`

### Requirement: LogicalClock serves the durable change sequence

`LogicalClock` SHALL be fully wired: `ctx.now()` returns the Segment's current
durable change sequence (the same counter that orders WAL records and is re-armed
from the WAL on restore). The semantics (version-like, advances with applied
changes, replay-stable across crash-restore) SHALL be stated in the spec-level
documentation.

#### Scenario: Logical time survives crash-restore

- **WHEN** a Segment with `LogicalClock` has applied changes up to seq 42, the
  executor restarts, and state is restored via the restore handshake
- **THEN** the next invocation's `ctx.now()` reflects seq 42, not a reset counter

#### Scenario: Logical time advances with applied changes

- **WHEN** an invocation applies one change via `state.apply(...)` and a subsequent
  invocation runs
- **THEN** the subsequent invocation's `ctx.now()` is greater than the prior one's

### Requirement: Deterministic stable identifiers via `ctx.stable_id()`

`OperationCtx::stable_id()` SHALL return a 128-bit identifier derived by a frozen,
library-independent algorithm from the Segment identity, the current change
sequence, and a per-invocation call ordinal. Repeated calls within one invocation
SHALL return distinct values; the n-th call in a replayed invocation SHALL return
the identical value as the n-th call in the original invocation.

#### Scenario: Replay reproduces identifiers

- **WHEN** an invocation calls `stable_id()` twice, then the same input is replayed
  over the same restored state
- **THEN** the replayed invocation's two identifiers equal the originals, in order

#### Scenario: Distinct within an invocation

- **WHEN** `stable_id()` is called twice in one invocation
- **THEN** the two identifiers differ

### Requirement: Build-time clock validation in the Rust SDK

Operative build SHALL validate the declared Clock configuration and fail fast with
a typed start error for any configuration the runtime cannot serve, instead of
panicking at first use. This brings the Rust SDK to parity with the Java SDK's
build-time clock rejection.

#### Scenario: Unservable clock configuration fails at build

- **WHEN** an operative is built whose declared Clock configuration cannot be
  served by the runtime
- **THEN** registration fails with a typed error naming the clock type, and no
  Segment is started

### Requirement: Harness verifies replay-equal emits with an injectable clock

`OperationTestHarness` SHALL provide a replay-emit verification that runs the same
input sequence twice from equivalent starting state and asserts the two serialized
emit streams are byte-identical. The harness SHALL be able to inject a fixed or
scripted wall-clock source so WallClock operatives are testable deterministically.

#### Scenario: Deterministic operative passes

- **WHEN** an Operation using only ctx-mediated time and identifiers is checked
- **THEN** the replay-emit verification passes

#### Scenario: Injected clock makes WallClock testable

- **WHEN** the harness injects a fixed wall-clock value and a WallClock Operation
  is checked
- **THEN** both passes observe the same `ctx.now()` value and the verification
  compares emits meaningfully
