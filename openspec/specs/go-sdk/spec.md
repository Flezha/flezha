# go-sdk Specification

## Purpose

The Go SDK. Its authoring surface makes a tasklet a type with a Spec and a typed Run, wires relays
with typed streams, offers a function short form, keeps state and changes as named JSON records, and
serves deterministic effects through `Ctx`, local mode and standard-library diagnostics. Its runner
serves activations from a pool of poll goroutines grown by a starvation supervisor that cannot be
configured away, copies core buffers before use, and owns the termination signals. It reaches the
core through the plain C ABI over cgo, linked statically into one executable with the standard
library only, and is proven by its admission to the conformance suite, the harness teardown
contract, a tasklet test harness at parity with the other SDKs, and Go as a measured runtime.

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

### Requirement: The Go host is a poll goroutine pool with a starvation supervisor
The Go SDK SHALL run a base pool of goroutines, each looping `poll -> dispatch -> complete`, and one supervisor goroutine answering the core's pool governor through `flz_core_pool_supervise`. Each spawn answer SHALL start one more worker, up to the governor's ceiling; a grown worker SHALL retire after the governor's retire time of polls without work, and give its seat back through `flz_core_pool_left`. The pool SHALL be opened with `flz_core_pool_open` over a default base of `runtime.NumCPU()`, which `FLEZHA_WORKER_POOL_SIZE` overrides; the ceiling SHALL be the governor's, ten times the base by default and set by `FLEZHA_WORKER_POOL_MAX`. The SDK SHALL add no pool knob of its own.

#### Scenario: A workload that never starves stays on the base pool
- **WHEN** handlers return promptly and the runner is saturated with work
- **THEN** the pool SHALL stay at the base size

#### Scenario: Blocked handlers buy workers
- **WHEN** every worker is inside a handler that blocks for seconds and work is queued in the core
- **THEN** the supervisor SHALL start workers up to the ceiling and the queued work SHALL be served

#### Scenario: Grown workers retire
- **WHEN** the blocking workload ends and the grown workers idle past the retirement streak
- **THEN** the pool SHALL return to the base size

### Requirement: The Go starvation supervisor cannot be configured away
The SDK SHALL NOT offer a configuration without the supervisor, including a single-worker mode, because a completion can block on a full `DropNothing` consumer and only a new poller drains it.

#### Scenario: A DropNothing wedge recovers on a base pool of one
- **WHEN** `FLEZHA_WORKER_POOL_SIZE=1` and a handler emits onto a full `DropNothing` consumer mailbox
- **THEN** the supervisor SHALL start a worker, that worker SHALL drain the consumer, and the blocked completion SHALL return

### Requirement: Per-cell ordering is the core's, not the Go SDK's
The Go SDK SHALL NOT implement mailboxes, drain gates or ordering of its own. No goroutine SHALL stay locked to its OS thread beyond one core call: a call is pinned only until the error document the core keeps for that thread is read. An activation MAY be completed from a thread other than the one that polled it, as the core allows.

#### Scenario: Different cells run concurrently
- **WHEN** inputs for two distinct cell keys arrive together on a pool of two or more
- **THEN** both activations MAY be in flight at once, and the SDK SHALL add no serialization

#### Scenario: One cell never runs concurrently with itself
- **WHEN** many inputs for one cell key arrive
- **THEN** the SDK SHALL never have two activations for that cell in flight, and `Run` SHALL observe the inputs in arrival order

### Requirement: Core buffers never outlive the call that handed them out
The Go SDK SHALL copy each buffer the core hands out into Go memory and SHALL free the core's buffer through `flz_core_buf_free` before dispatching, so no Go value can refer to memory the core frees. Bytes handed to the core SHALL be Go byte slices that hold no Go pointers, passed for the duration of the call only.

#### Scenario: The pointer checks pass under load
- **WHEN** the Go SDK's concurrency tests run with `GODEBUG=cgocheck=1` and the race detector
- **THEN** no cgo pointer violation and no data race SHALL be reported

### Requirement: Go owns the termination signals and hands them to the core
The Go runner SHALL call `flz_core_host_owns_signals` before starting the core, SHALL receive SIGTERM and SIGINT through `os/signal`, and on the first SHALL call `flz_core_terminate`, so the core drains and exits as it does for every host. The runner SHALL also start the core's standard-input watch when standard input is not a terminal.

#### Scenario: SIGTERM drains instead of crashing
- **WHEN** a started Go runner with activations in flight receives SIGTERM
- **THEN** the in-flight activations SHALL complete within the core's grace and the process SHALL exit with the status the core's graceful end gives, with no Go runtime crash report

#### Scenario: Closing standard input stops the runner
- **WHEN** the harness closes the runner's standard input
- **THEN** the runner SHALL drain and exit within the core's grace

### Requirement: The Go binding is the plain C ABI over cgo
The Go SDK SHALL call the `flz_core_ffi` C entry points through cgo, with no Rust glue feature, no new Rust crate and no Go implementation of the protocol. The entry points it declares SHALL be asserted against `test/fixtures/abi_surface.txt` by a test in the SDK's own suite, so a symbol the core removes or renames fails that test.

#### Scenario: The declared surface matches the fixture
- **WHEN** the Go SDK's declared C entry points are compared with `abi_surface.txt`
- **THEN** every declared entry SHALL be in the fixture, and a declared entry the fixture lacks SHALL fail the test naming it

### Requirement: The Go runner links the core statically into one executable
The Go SDK SHALL link the crate's `staticlib`, `libflz_core_ffi.a`, with the library directory supplied by the build through `CGO_LDFLAGS`, and SHALL name in its cgo directives the system libraries the core needs, taken from the Rust toolchain's report for that target. A built Go runner SHALL NOT need `libflz_core_ffi` at run time.

#### Scenario: The runner needs no core library at run time
- **WHEN** the Go test app is built by `scripts/build-go-sdk.sh` and its dynamic dependencies are listed
- **THEN** no `libflz_core_ffi` SHALL be among them, and the runner SHALL start from a directory holding only itself

#### Scenario: A missing library names the setting
- **WHEN** the SDK is built without a directory holding `libflz_core_ffi.a`
- **THEN** the SDK guide's build section SHALL name `CGO_LDFLAGS` and the build script that sets it, and the link error SHALL name `flz_core_ffi`

### Requirement: The Go SDK depends on the standard library only
The Go SDK's module SHALL require no module beyond the Go standard library, and its `go.mod` SHALL declare the toolchain floor: the older of the two Go releases the Go project supports on the day the change is applied.

#### Scenario: The module requires nothing
- **WHEN** `go.mod` is read
- **THEN** it SHALL carry a `go` directive at the floor and no `require` line

### Requirement: The Go SDK is developed and tested from its own tree
The SDK SHALL live in its own repository, `flezha/flezha_go_sdk`, pinned as a submodule at `sdk/go` with `test/apps/go_runner` a symlink to its test app, and SHALL carry the three tiers an SDK repository runs without a controller, resolving the contract set through `$FLEZHA_CONTRACT_DIR`, then `contract/`, then the tree the module sits in, and failing rather than skipping when none resolves.

#### Scenario: The suite runs in the SDK tree without a controller
- **WHEN** `go test ./...` runs in `sdk/go` with the core library built
- **THEN** the unit, fixture and fake-controller tiers SHALL run and no controller SHALL be started

#### Scenario: A missing contract set fails
- **WHEN** the suite runs with no contract set reachable
- **THEN** the fixture tests SHALL fail naming the three places searched

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
