# relay-deterministic-effects Specification

## Purpose
The determinism contract for relay code: ctx-mediated time (`ctx.now()` served from the declared clock, including event time and the logical clock), ctx-mediated stable identifiers (`ctx.stable_id()`), build-time clock validation, and harness verification that replay reproduces identical emits. Binds all current and future SDK languages.
## Requirements
### Requirement: Determinism contract for relay code

The SDK SHALL define a determinism contract binding `Tasklet::run`,
`Change::apply_to`, and `compensate`: given the same Cell state and the same input
sequence, the code SHALL produce identical state changes and identical emitted
outputs. Ambient non-determinism (wall-clock reads, random number / UUID
generation, reads of external mutable state) inside these functions is a contract
violation; time and identifiers SHALL be obtained through the tasklet context (`Ctx`). A call
to anything outside the instance (a model, a tool, a service) inside `run` or `compensate`
SHALL go through `ctx.effect(name, fn)`, which keeps its result so a re-run reads the same
value; `Change::apply_to` SHALL make no outside call at all. The contract SHALL bind all
current and future SDK languages.

#### Scenario: Contract is documented on the authoring surface

- **WHEN** an author reads the `Tasklet` trait / `run` documentation
- **THEN** the determinism contract is stated, including the ctx-mediated
  alternatives for time (`ctx.now()`), identifiers (`ctx.stable_id()`) and outside calls
  (`ctx.effect`)

#### Scenario: Violation surfaces as a replay difference in the harness

- **WHEN** a Tasklet whose `run` calls `SystemTime::now()` to build its output
  is checked with the harness replay-emit verification
- **THEN** the check fails because the two replay passes emit different bytes

#### Scenario: An outside call through `effect` replays equal

- **WHEN** a Tasklet whose `run` builds its output from `ctx.effect("model", fn)` where `fn`
  answers differently on each call is checked with the harness replay-emit verification
- **THEN** the check passes, because the replay pass is served the live pass's result

### Requirement: Runtime-evaluated Clock served via `ctx.now()`

The dispatcher SHALL evaluate the Tasklet's declared clock exactly once per
`run` invocation, before user code executes, and `ctx.now()` SHALL return
that single value for the entire invocation. No static clock read SHALL exist on
the trait surface.

#### Scenario: One value per invocation

- **WHEN** `run` calls `ctx.now()` multiple times during one invocation
- **THEN** every call returns the same value

#### Scenario: WallClock reads dispatch-entry time

- **WHEN** a Tasklet declares the wall clock (in Rust, writes no `event_time`
  and no `logical_clock`) and `run` calls
  `ctx.now()`
- **THEN** the value is the system time read at dispatch entry for that invocation

### Requirement: LogicalClock serves the durable change sequence

The logical clock (`ClockKind::Logical` in Rust) SHALL be fully wired: `ctx.now()` returns the Cell's current
durable change sequence (the same counter that orders WAL records and is re-armed
from the WAL on restore). The semantics (version-like, advances with applied
changes, replay-stable across crash-restore) SHALL be stated in the spec-level
documentation.

#### Scenario: Logical time survives crash-restore

- **WHEN** a Cell on the logical clock has applied changes up to seq 42, the
  runner restarts, and state is restored via the restore handshake
- **THEN** the next invocation's `ctx.now()` reflects seq 42, not a reset counter

#### Scenario: Logical time advances with applied changes

- **WHEN** an invocation applies one change via `ctx.change(...)` and a subsequent
  invocation runs
- **THEN** the subsequent invocation's `ctx.now()` is greater than the prior one's

### Requirement: Deterministic stable identifiers via `ctx.stable_id()`

`ctx.stable_id()` SHALL return a 128-bit identifier derived by a frozen,
library-independent algorithm from the Cell identity, the current change
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

Relay build SHALL validate the declared Clock configuration and fail fast with
a typed start error for any configuration the runtime cannot serve, instead of
panicking at first use. This brings the Rust SDK to parity with the Java SDK's
build-time clock rejection.

#### Scenario: Unservable clock configuration fails at build

- **WHEN** a relay is built whose declared Clock configuration cannot be
  served by the runtime
- **THEN** registration fails with a typed error naming the clock type, and no
  Cell is started

### Requirement: Harness verifies replay-equal emits with an injectable clock

`TaskletTestHarness` SHALL provide a replay-emit verification that runs the same
input sequence twice from equivalent starting state and asserts the two serialized
emit streams are byte-identical. The harness SHALL be able to inject a fixed or
scripted wall-clock source so wall-clock relays are testable deterministically.

#### Scenario: Deterministic relay passes

- **WHEN** a Tasklet using only ctx-mediated time and identifiers is checked
- **THEN** the replay-emit verification passes

#### Scenario: Injected clock makes WallClock testable

- **WHEN** the harness injects a fixed wall-clock value and a wall-clock Tasklet
  is checked
- **THEN** both passes observe the same `ctx.now()` value and the verification
  compares emits meaningfully

### Requirement: Event time comes from the tasklet's `event_time`
A tasklet that declares event time (Rust: writes `event_time` in a `#[tasklet]` block; Java: overrides `eventTime`) SHALL have its extractor applied by the dispatcher to the input being dispatched, and `ctx.now()` SHALL return the extracted value. The result SHALL depend only on the input.

#### Scenario: Event time is replay-stable

- **WHEN** the same input is dispatched twice (original run and replay) to a tasklet that declares event time
- **THEN** `ctx.now()` returns the identical value in both invocations

#### Scenario: The extractor sees the dispatched input

- **WHEN** an input carrying event timestamp `1720000000000` is dispatched
- **THEN** `ctx.now()` inside that invocation returns `1720000000000`

