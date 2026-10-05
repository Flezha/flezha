# php-sdk-executors Specification

## Purpose
TBD - created by archiving change add-php-sdk. Update Purpose after archive.
## Requirements
### Requirement: The Executor seam owns loop multiplicity, not work distribution
The PHP SDK SHALL define an `Executor` interface of exactly two methods — `run(Runner $runner): void`, which drives the runner's poll loop to completion and MAY drive it on more than one thread, and `stop(): void`, which asks every loop to finish its current activation and return.

The seam SHALL NOT be expressed as per-activation submission of a callable, because `Swoole\Thread` bootstraps a fresh interpreter from a script path and scalar arguments and cannot carry a closure, an object or a resource across a thread boundary. Loop multiplicity is expressible under every PHP threading model; work distribution is not.

The loop SHALL take its work from a blocking `flz_core_poll(ctx, timeout_ms)` call and complete through `flz_core_complete`, which the ABI accepts from any thread including one that never polled — so no executor requires a notification descriptor or any other ABI addition.

#### Scenario: The poll loop is executor-agnostic
- **WHEN** the SDK is run under each shipped executor against an identical workload
- **THEN** the same decode, dispatch and completion code paths SHALL execute, differing only in how many threads run them

#### Scenario: An activation completes on the thread that polled it
- **WHEN** any executor runs the loop
- **THEN** each thread SHALL poll, dispatch and complete within itself, so no activation crosses a thread boundary

#### Scenario: Stop does not race in-flight activations
- **WHEN** the runner stops, whether by signal or by reaching its activation budget
- **THEN** `stop()` SHALL return only once every loop has left user code, and `flz_core_shutdown` SHALL NOT be called before it does

### Requirement: Concurrency cannot affect per-cell correctness
An executor SHALL NOT be able to change ordering, exclusivity, or state consistency, because those are held by the core: `PollCore` never hands out two activations for the same cell concurrently, regardless of how many threads poll it.

The SDK SHALL NOT add locking, queueing, or ordering of its own on top of that guarantee.

#### Scenario: Concurrent activations never share a cell
- **WHEN** many threads poll under sustained load
- **THEN** no two in-flight activations SHALL name the same cell

#### Scenario: Switching executors does not change results
- **WHEN** the same deterministic workload is replayed under `SyncExecutor` and under `SwooleExecutor`
- **THEN** the resulting cell states and emitted outputs SHALL be equal

### Requirement: SyncExecutor is the zero-dependency default
The SDK SHALL ship a `SyncExecutor` as the default, requiring no Composer dependency and no PHP extension beyond `ext-ffi`: `run()` drives the loop on the calling thread and `stop()` sets the flag the loop checks between polls.

It SHALL process one activation at a time, which is correct for tasklet bodies that are CPU-bound or use blocking I/O, and it SHALL work on a non-thread-safe (NTS) PHP build, which is what distributions ship by default.

#### Scenario: A runner starts with no dependencies installed
- **WHEN** a project installs only `flezha/sdk` on an NTS PHP build and starts a runner without selecting an executor
- **THEN** the runner SHALL start and process activations under `SyncExecutor`

#### Scenario: A blocking tasklet body is correct, not exceptional
- **WHEN** a tasklet body performs blocking I/O under `SyncExecutor`
- **THEN** the activation SHALL complete normally and the runner SHALL process the next one afterwards, with no warning and no degraded mode

### Requirement: SwooleExecutor gives one runner every core
The SDK SHALL ship a `SwooleExecutor` that runs the poll loop on a pool of `Swoole\Thread`s over one shared `FlzContext`, so a single runner — and therefore a single relay's tasklet class — uses more than one core.

Each thread SHALL bootstrap the runner's entry script, rebuild the PHP-side tasklet registry, and attach to the existing context by its address, without re-declaring classes to the core: `flz_core_declare_class` is per context, not per thread. The entry script is therefore re-entered, and the SDK SHALL document that it must be safe to `require` — in particular that it carries no `#!` line, which is stripped only from the script PHP was invoked with.

The pool SHALL grow on the core's own signal rather than a guess: a supervisor SHALL block in `flz_core_wait_starvation` and add a thread when starvation is observed and the configured maximum is not reached, which is the case the ABI documents as "hosts whose fetch threads can be occupied by user code".

`SwooleExecutor` SHALL be selected explicitly and SHALL NOT be the default, because it requires a ZTS PHP build and an extension that most distributions do not ship.

#### Scenario: Threads process activations in parallel
- **WHEN** a CPU-bound workload runs under `SwooleExecutor` with a thread maximum above one
- **THEN** total throughput SHALL exceed the same workload's throughput under `SyncExecutor` on the same host

#### Scenario: The pool grows when user code occupies the fetch threads
- **WHEN** every running thread is inside a tasklet body while further work is queued
- **THEN** the supervisor SHALL observe starvation and start another thread, up to the configured maximum

#### Scenario: One relay uses more than one core
- **WHEN** a single dynamic-cell relay receives inputs across many keys under `SwooleExecutor`
- **THEN** activations for distinct cells SHALL execute simultaneously on distinct threads

#### Scenario: A missing runtime produces an instruction
- **WHEN** `SwooleExecutor` is selected on a non-ZTS build, without the extension that provides `Swoole\Thread`, or on a build whose `ffi.enable` is not `true`
- **THEN** the SDK SHALL raise an error naming what is missing and how to obtain it, before the runner connects

#### Scenario: FFI must be enabled outright, not only for preload
- **WHEN** the threaded executor is selected on a build whose `ffi.enable` is PHP's shipped default `preload`
- **THEN** it SHALL be refused with an instruction, because that setting permits FFI from the CLI SAPI and a worker thread is a fresh interpreter that is not it — every thread would otherwise start and be refused its first core call

#### Scenario: A worker that ends before shutdown is replaced
- **WHEN** a worker thread ends for any reason other than shutdown
- **THEN** the pool SHALL report it and SHALL NOT count it against the maximum, so a pool that lost every worker starts another rather than reporting starvation forever

#### Scenario: Threads exit cleanly at shutdown
- **WHEN** the runner stops while several threads hold in-flight activations
- **THEN** every thread SHALL finish its activation, leave the loop and be joined, and the context SHALL be freed exactly once, by the thread that created it

### Requirement: The FFI binding resolves per thread
The SDK SHALL resolve its FFI handle through a per-thread accessor rather than a process-wide singleton, because `FFI::cdef` is per-interpreter and each `Swoole\Thread` is a separate interpreter.

The `FlzContext` pointer SHALL be carried as an integer, so it crosses a thread boundary as a bootstrap argument with no lifetime of its own. A shim that casts the integer back to a pointer type through its language's FFI SHALL keep whatever that cast borrows alive for as long as the pointer is used; carrying the integer to the ABI directly is the shape with nothing to keep alive.

#### Scenario: Each interpreter gets its own handle
- **WHEN** the binding is accessed from a thread that has not used it before
- **THEN** it SHALL create that thread's handle lazily and reach the same context

#### Scenario: An attached handle answers a call that dereferences the context
- **WHEN** a handle attached by address is used for a call the core answers from the context itself, rather than one that ignores it
- **THEN** the core's own answer SHALL come back — a handle that passes a context-free call and faults on the first real one is the failure this pins

#### Scenario: The context outlives every worker thread
- **WHEN** worker threads attach to a context they did not create
- **THEN** they SHALL NOT free it, and the creating thread SHALL free it only after every worker has been joined

### Requirement: Both executors are gated by the same suites
Every suite that gates the PHP SDK — unit, e2e and benchmark — SHALL be runnable under either executor without editing a test, a fixture or a manifest, selecting it by one environment variable of the run, defaulting to `SyncExecutor`.

The selection SHALL be an axis of the run, never a property of a file, so no test, fixture or benchmark exists in a per-executor variant.

#### Scenario: The e2e suite runs under either executor
- **WHEN** the `php_sdk`-tagged suites are run with the executor variable unset, and again set to the threaded executor
- **THEN** both runs SHALL execute the same tests and both SHALL pass

#### Scenario: The benchmark runs under either executor
- **WHEN** the round-trip benchmark is run against the PHP runtime under each executor
- **THEN** each run SHALL report which executor produced it, so the two numbers are comparable and attributable

#### Scenario: No fixture names an executor
- **WHEN** the PHP test app and the system suites are inspected
- **THEN** no fixture, test name or manifest entry SHALL be specific to one executor

### Requirement: Swoole is optional and never named in shared code
The SDK's Composer manifest SHALL require only `php >= 8.4` and `ext-ffi`, declaring `ext-swoole` under `suggest` with the minimum version that provides `Swoole\Thread`.

No Swoole type SHALL be named outside the threaded adapter's own files and its own tests — not in the `Executor` interface, not as a parameter or return type, and not in a `use` statement in shared code — so that PHP's per-class autoloading leaves the adapter entirely inert when it is not selected. The adapter is more than one file because a `Swoole\Thread` is constructed from a **script path**: its worker bootstrap is necessarily a script, not a class. The check SHALL read the parsed source rather than the raw text, so a comment explaining the seam is not an offence.

Shared code SHALL likewise not name the adapter class itself: the factory that constructs it is the one door, so nothing else autoloads the adapter.

#### Scenario: An unselected adapter is never loaded
- **WHEN** a runner runs under `SyncExecutor` on an NTS build with no Swoole extension present
- **THEN** no error SHALL occur, because the threaded adapter's file is never autoloaded

#### Scenario: Shared code names no runtime
- **WHEN** the SDK's sources outside the adapter file are inspected
- **THEN** no Swoole symbol SHALL appear in any of them

