# go-sdk Specification

## Purpose
The Go SDK's authoring surface and runner: tasklets as types with a Spec and a typed Run, relays wired with typed streams, the function short form, JSON state with named changes, deterministic effects, local mode and diagnostics.
## Requirements
### Requirement: Go SDK entry point and runner lifecycle
The Go SDK SHALL expose a runner created by `flezha.CreateRunner(namespace string)`, carrying declaration and lifecycle on one value: `RegisterFactory` registers a relay type of either run, `UnregisterFactory` is its inverse, `Start` connects, `AwaitTermination` blocks until the runner stops, `Close` is the one public shutdown verb, and `Run` is `Start` followed by `AwaitTermination`. Every fallible call SHALL return an `error` carrying the core's error document (`code`, `class`, `message`, `details`, `hint`), and a caller SHALL be able to branch on the code with `errors.As`.

The runner SHALL map onto the ABI lifecycle: context creation with the SDK's compiled `SCHEMA_VERSION`, one declaration per class, start, poll and complete, shutdown, context free.

#### Scenario: A declared flow relay connects and is placed
- **WHEN** a program calls `flezha.CreateRunner("trading")`, registers a relay type and calls `Run` against a live controller
- **THEN** the runner SHALL connect, the controller SHALL place an instance of that relay on it, and inputs routed to it SHALL reach the declared tasklet

#### Scenario: Zero declared relay types is legal
- **WHEN** a runner is started having registered no relay type
- **THEN** it SHALL connect and stay connected, as the other SDKs do

#### Scenario: A schema mismatch is refused before connecting
- **WHEN** the SDK's compiled `SCHEMA_VERSION` differs from the linked core's
- **THEN** `CreateRunner` SHALL return an error naming both versions, and no controller connection SHALL be attempted

#### Scenario: A refusal carries its code
- **WHEN** `RegisterFactory` is refused by the core
- **THEN** the returned error SHALL expose the core's code through `errors.As`, and its text SHALL be the core's rendering

### Requirement: A Go tasklet is a type with a Spec and a Run
A Go tasklet SHALL be a type implementing `Spec() flezha.Spec` and `Run(in I, ctx *flezha.Ctx[S, O]) error`. `Spec` SHALL carry the required `Name` and the scalar settings, and SHALL be read once, at registration. A hook (`OnTimer`, `EventTime`, `Compensate`, `MigrateState`, `MigrateChange`, `InitState`) SHALL be declared by implementing it; a tasklet that does not implement a hook SHALL declare no such hook. A value SHALL leave a tasklet only through `ctx.Emit` and its keyed and named-channel forms.

`Run` returning `nil` SHALL complete the activation; any other error SHALL be retryable and climb the channel's retry ladder; an error wrapped by `flezha.NonRetryable` SHALL park without further attempts; a panic SHALL be recovered and treated as a retryable error carrying its text, and SHALL NOT end the process.

The initial state of a cell SHALL be what `InitState` returns when the tasklet implements it, and the zero value of `S` otherwise.

#### Scenario: A tasklet without a name is refused
- **WHEN** a tasklet whose `Spec()` leaves `Name` empty is registered
- **THEN** `RegisterFactory` SHALL return the core's refusal naming the tasklet type, and nothing SHALL be declared

#### Scenario: Implementing a hook declares it
- **WHEN** one tasklet implements `OnTimer` and another does not
- **THEN** the first SHALL be declared with a timer hook and the second without one

#### Scenario: A tasklet that emits nothing
- **WHEN** `Run` returns `nil` without calling `ctx.Emit` for an input
- **THEN** nothing SHALL be emitted for that input

#### Scenario: A panic is a retryable failure
- **WHEN** `Run` panics on its first attempt and returns `nil` on its second
- **THEN** the activation SHALL be retried once, the second attempt SHALL complete, and the runner SHALL keep serving

#### Scenario: A non-retryable error parks at once
- **WHEN** `Run` returns `flezha.NonRetryable(err)`
- **THEN** the input SHALL park to the dead-letter queue without a further attempt

### Requirement: A Go relay is wired with typed streams
The relay builder SHALL hand out `Stream[T]` values, and every step that changes the element type SHALL be a package function taking and returning typed streams (`Map`, `KeyBy`, `Into`, `Merge` and the other steps the shared builder carries). Wiring a stream into a tasklet whose input type differs from the stream's element type SHALL fail `go build`. Each such refusal SHALL be pinned by a fixture under `testdata/` that a test builds, asserting both that the build fails and that the compiler's message names both sides of the mismatch: the stream's element type, and the tasklet or function that cannot take it.

The builder SHALL submit its topology to the reference check at registration, as every other builder does, so a refusal static typing cannot see is still made, under the same name.

#### Scenario: A mismatched edge does not compile
- **WHEN** a `Stream[string]` is passed to `Into` with a tasklet type `Totals` whose input type is `Order`
- **THEN** `go build` SHALL fail, and the pinned fixture's test SHALL find `string` and `Totals` in the compiler's message

#### Scenario: The reference refusals are reached
- **WHEN** the Go test app drives every row of `test/fixtures/reference_topologies.txt` through its builder
- **THEN** each refusal SHALL carry the code the reference names, and each accepted topology SHALL equal the reference

### Requirement: The Go short form is a function that may skip
`flezha.ShortRelay(factoryID, fn)` SHALL declare a one-tasklet relay from a function `func(in I, ctx *flezha.Ctx[S, O]) (O, bool)`. The relay SHALL emit the returned value when the boolean is `true` and SHALL emit nothing when it is `false`. A stateless short form SHALL use `flezha.NoState` as `S`. The tasklet's name SHALL be the factory id.

#### Scenario: True emits, false skips
- **WHEN** a short form returns `("", false)` for one input and `("hi", true)` for the next
- **THEN** the relay SHALL emit nothing for the first input and `hi` for the second

### Requirement: Go state and changes are named JSON records
State and change values SHALL be encoded with the Go standard library's `encoding/json`, and durable records SHALL be stamped `sdk_lang = SDK_LANG_GO` (8) with format id `FORMAT_ID_JSON` (6). A stateful tasklet SHALL list its change types under explicit names through `Changes() flezha.Changes[S]`, a change type SHALL implement `ApplyTo(*S)`, and a change record SHALL be the JSON object `{"c": <name>, "v": <value>}`. Recording a change whose type is not listed SHALL be refused at the call with the error code `runtime.change_not_registered`.

#### Scenario: State survives a restart
- **WHEN** a cell accumulates state, the runner is stopped, and a new runner binds the same relay
- **THEN** the restored state SHALL equal the state at shutdown, folded from the change log

#### Scenario: A renamed change type still restores
- **WHEN** a change type is renamed in Go between two runs and listed under the same name
- **THEN** restore SHALL succeed, because the record carries the listed name and no Go type name

#### Scenario: An unlisted change is refused
- **WHEN** `ctx.Change` is called with a type the tasklet's `Changes()` does not list
- **THEN** the call SHALL fail with `runtime.change_not_registered` naming the type and the tasklet

#### Scenario: A record of another language is refused before decoding
- **WHEN** a Go-hosted class is sent records stamped with another SDK's `sdk_lang`
- **THEN** the core's restore fold SHALL refuse before any body reaches the Go codec

### Requirement: The Go SDK names its state-mutation enforcement level
`ctx.State()` SHALL return a copy of the cell's state, so assigning a field of the returned value SHALL NOT change durable state. Maps, slices and pointers inside the state are shared and SHALL be documented as not guarded. The detection offered SHALL be the test harness's replay check, opt-in, and the authoring contract's per-SDK table SHALL record this level for Go.

#### Scenario: Assigning a field of the copy changes nothing
- **WHEN** `Run` assigns a field of `ctx.State()` without calling `ctx.Change`
- **THEN** the next input for that cell SHALL see the state unchanged

#### Scenario: The replay check finds an unrecorded mutation
- **WHEN** a tasklet appends to a slice inside its state without a change and the harness's replay check runs
- **THEN** the check SHALL fail naming the cell and the differing state

### Requirement: Deterministic effects through Ctx
`ctx.Now()` SHALL return the time the core evaluated for the invocation, `ctx.StableID()` SHALL equal the Rust and Java values for the same cell identity, change sequence and ordinal, pinned by the shared fixture, and `ctx.Effect(name, fn)` SHALL run through the core's effect entries, so a kept result is returned without running `fn` again.

#### Scenario: Now is constant within one invocation
- **WHEN** `ctx.Now()` is called twice inside one `Run`
- **THEN** both calls SHALL return the same value

#### Scenario: A recorded effect does not run twice
- **WHEN** an invocation that ran `ctx.Effect("charge", fn)` is retried
- **THEN** the retry SHALL receive the kept result and `fn` SHALL NOT run again

### Requirement: The Go activation codec is pinned to the shared fixtures
The Go SDK SHALL encode and decode the postcard `Activation` and `ActivationResult` schema in Go, pinned against the hex fixtures in `flz_core/src/schema.rs`, at the schema version the SDK is compiled against. A `u64` field SHALL decode into a `uint64`.

#### Scenario: Every schema fixture round-trips
- **WHEN** each hex fixture is decoded and re-encoded by the Go codec
- **THEN** the re-encoded bytes SHALL be identical to the fixture

### Requirement: Full declaration and every command activation
The Go SDK SHALL build the complete class declaration `flz_core_declare_class` accepts, with every key of `test/fixtures/host_class_decl_keys.txt`, and SHALL handle every command activation kind the core hands out at its schema version, including the restore fold and the notice that a cell or relay left.

#### Scenario: The declaration keys are exactly the fixture's
- **WHEN** the Go declaration builder's keys are compared with `host_class_decl_keys.txt`
- **THEN** they SHALL be equal, and a key either side lacks SHALL fail the test naming it

#### Scenario: A restore fold rebuilds state before inputs flow
- **WHEN** a cell with a persisted change log is bound and the core hands out a restore fold
- **THEN** the SDK SHALL fold the change records into state, and the cell's next input SHALL observe the folded state

### Requirement: Go local mode runs the shared example
The Go SDK SHALL run a relay graph with no controller through the core's local-mode entries, and its local-mode test SHALL run the example in `test/fixtures/local_mode_word_count.txt` and produce its expected output.

#### Scenario: The word-count example matches the fixture
- **WHEN** the Go local-mode test feeds the fixture's input
- **THEN** the outputs SHALL equal the fixture's expected output

### Requirement: Go diagnostics use the standard library logger
The Go SDK SHALL log through `log/slog` at `FLEZHA_LOG_LEVEL` (default `warn`) unless the application handed it a logger, and SHALL drain the core's notices into that logger, so no core line reaches standard error past it.

#### Scenario: Core notices reach the application's logger
- **WHEN** an application hands the runner its own `*slog.Logger` and the core writes a notice
- **THEN** the notice SHALL arrive at that logger with its level, and nothing SHALL be written to standard error

