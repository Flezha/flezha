# relay-determinism Specification

## Purpose

The contract that makes a relay's re-run produce the same result. Relay code reads time through
`ctx.now()` from the class's declared clock (processing, event or logical time) and makes
identifiers with `ctx.stable_id()`, and the testing harness checks that a replay reproduces
identical emits. A call to anything outside the instance goes through `ctx.effect(name, fn)`: the
result is kept beside the instance's input log for as long as that input, shipped back on every
bind, and read instead of calling out again on a crash re-run, a replay, a wake or a move.

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

### Requirement: Every SDK SHALL offer `ctx.effect(name, fn)`

Each SDK (Rust, Java, Node, Python, Ruby, PHP, Elixir, Go) SHALL offer an `effect`
call on the tasklet context (in Go the package function `flezha.Effect(ctx, name, fn)`, a Go
method taking no type parameter) that takes a name and a function, runs the function at most once
per invocation identity while its result is kept, and returns the function's result. The name
SHALL be a non-empty string of at most 255 bytes, refused otherwise with a non-retryable
error. The result SHALL be encoded as a sink naming no codec encodes a value (bytes as they
are, a string as UTF-8, anything else as JSON), or by the codec the caller passes, and SHALL
be decoded the same way when it is returned from a kept result. In Python and Node a function
that returns an awaitable SHALL make `effect` awaitable.

#### Scenario: A first run runs the function and returns its result

- **WHEN** a tasklet of a durable steps relay calls `ctx.effect("model", fn)` for the first
  time for an input
- **THEN** `fn` runs once, its result is returned, and the controller keeps it under the
  invocation's identity

#### Scenario: A re-run returns the kept result without running the function

- **WHEN** the runner is killed after `ctx.effect("model", fn)` returned and before the
  invocation committed, and the input is run again on a new runner
- **THEN** `ctx.effect("model", fn)` returns the kept result and `fn` is not called

#### Scenario: A bad name is refused

- **WHEN** a tasklet calls `ctx.effect("", fn)`
- **THEN** the call raises a non-retryable error naming the rule and `fn` does not run

#### Scenario: Same behaviour in every SDK

- **WHEN** the e2e crash scenario above is run with a test tasklet in each SDK
- **THEN** each one calls its function exactly once across the crash

### Requirement: An effect result SHALL be named by where it came from

The identity of an effect result SHALL be derived from the cell, the input-log offset the
invocation is taking, the phase (`run` or `compensate`), the name, and the number of earlier
`effect` calls with that name at that offset in the activation. An invocation that took no
input-log offset SHALL have no identity.

#### Scenario: Two names in one invocation are two results

- **WHEN** one invocation calls `ctx.effect("model", f)` then `ctx.effect("tool", g)`
- **THEN** a re-run returns `f`'s result to the first call and `g`'s to the second

#### Scenario: Repeated name counts in order

- **WHEN** one invocation calls `ctx.effect("model", f)` twice with different results
- **THEN** a re-run returns the first result to the first call and the second to the second

#### Scenario: An added call with another name does not shift identities

- **WHEN** the result of `ctx.effect("model", f)` is kept, and the re-run first calls
  `ctx.effect("lookup", h)` before `ctx.effect("model", f)`
- **THEN** `h` runs, and `ctx.effect("model", f)` still returns the kept result

#### Scenario: Two items of one cell in one step are two results

- **WHEN** a step tasklet receives two handoff items for the same cell key from one input and
  each calls `ctx.effect("model", f)`
- **THEN** each item keeps its own result and a re-run returns each its own

### Requirement: An effect result SHALL be kept before `effect` returns and as long as its input

The controller SHALL write an effect result to the instance's effect log and answer the
runner only once a sync covers it, and at `durable: remote` only once it is also shipped.
`effect` SHALL return only after that answer. The controller SHALL drop an effect result
when the input log truncates below its offset, and not before.

#### Scenario: A controller restart keeps the results

- **WHEN** an effect result is kept, the controller is restarted before the invocation
  commits, and the instance is bound again
- **THEN** the re-run invocation's `effect` returns the kept result without running `fn`

#### Scenario: A committed input's results are dropped with it

- **WHEN** an invocation that kept effect results commits and the input log truncates below
  its offset
- **THEN** the effect log holds no result for that offset after the truncation

#### Scenario: A sleeping session wakes with its results

- **WHEN** a session sleeps between a kept effect and the commit of its turn (the turn was
  interrupted by a crash) and is then woken
- **THEN** the re-run turn's `effect` returns the kept result

### Requirement: A rebind SHALL ship the kept results before the first tasklet runs

On every bind of an instance that has kept effect results, the controller SHALL put their
count on `AddRelay` and send them after the bind, and the runner SHALL hold the first
tasklet until that many have arrived. The runner SHALL drop its copy of a Cell's results for
an offset once that Cell calls `effect` at a later offset, and all of an instance's results
when the instance is removed, so nothing is added to the per-message path.

#### Scenario: A moved instance finds its results

- **WHEN** an instance with kept effect results is moved to another runner
- **THEN** the new runner receives every kept result before the first re-run invocation
  starts, and that invocation's `effect` calls do not run their functions

### Requirement: Where nothing can re-run, `effect` SHALL only run the function

An `effect` call in an invocation with no identity SHALL run the function, return its result
and send nothing to the controller.

#### Scenario: Local mode runs the function every time

- **WHEN** a tasklet calling `ctx.effect("model", fn)` runs twice in local mode on the same
  input
- **THEN** `fn` runs twice and no frame is sent

#### Scenario: A local input records nothing

- **WHEN** a tasklet of an effectively-once relay calls `ctx.effect("model", fn)` while
  handling an input sent with `runner.send`'s `local` option
- **THEN** `fn` runs, its result is returned, and no effect result is sent to the controller

### Requirement: `effect` SHALL refuse what it cannot keep

`effect` SHALL raise `effect.outside_invocation` when called through a context whose
invocation has returned, `effect.nested` when called inside another `effect`'s function,
`effect.too_large` (non-retryable) when the encoded result exceeds `FLEZHA_EFFECT_MAX_BYTES`,
and `effect.not_recorded` (retryable) when the controller does not confirm the result before
the deadline or answers a failure. A function's own error SHALL propagate unchanged and
nothing SHALL be kept.

#### Scenario: A result over the cap is refused

- **WHEN** `fn` returns a value whose encoding exceeds `FLEZHA_EFFECT_MAX_BYTES`
- **THEN** `effect` raises `effect.too_large` and nothing is kept

#### Scenario: A failing function keeps nothing

- **WHEN** `fn` raises
- **THEN** the error reaches the tasklet unchanged, and a re-run runs `fn` again

#### Scenario: Nested effect is refused

- **WHEN** a function passed to `effect` calls `ctx.effect` itself
- **THEN** the inner call raises `effect.nested`

### Requirement: A second result for a kept identity SHALL resolve to the first

When the controller receives a result for an identity it already keeps, it SHALL keep the
first, answer with it, and the runner SHALL return the first result.

#### Scenario: A duplicate result resolves to the first

- **WHEN** two `EffectResult` frames for one identity arrive with different payloads
- **THEN** the controller keeps the first, and `effect` returns the first payload for both

### Requirement: The testing harness SHALL serve kept results on replay

Each SDK harness with a replay pass SHALL keep the live pass's effect results, serve them on
the replay pass, and fail the replay with a message naming the effect when a function runs
on the replay pass.

#### Scenario: A replay that runs the function fails

- **WHEN** a tasklet calls `fn` directly on the replay pass instead of through `effect`, and
  the same call goes through `effect` on the live pass
- **THEN** the harness replay check fails naming the effect
