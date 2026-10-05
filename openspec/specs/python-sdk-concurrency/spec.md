# python-sdk-concurrency Specification

## Purpose
TBD - created by archiving change python-sdk. Update Purpose after archive.
## Requirements
### Requirement: The default host is a fetch-and-run thread pool with a starvation supervisor

The SDK's default executor SHALL run a base pool of Python threads, each looping `poll → dispatch → complete` on itself, and one supervisor thread parked in `flz_core_wait_starvation`. Each starvation observation SHALL start one additional worker, up to a ceiling; workers above the base SHALL retire after an idle streak. The base SHALL default to the machine's processor count and SHALL be configured by `FLEZHA_WORKER_POOL_SIZE`; the ceiling SHALL default to ten times the base, never below it, and SHALL be configured by `FLEZHA_WORKER_POOL_MAX`. The SDK SHALL introduce no additional pool knob.

The process's main thread SHALL NOT be a poll worker, because CPython runs signal handlers only on the main thread between bytecodes and a main thread inside a handler would delay them by the length of that handler.

#### Scenario: A workload that never starves runs on the base pool
- **WHEN** handlers return promptly and the runner is saturated with work
- **THEN** the pool SHALL remain at the base size and no worker SHALL be started by the supervisor

#### Scenario: Blocked handlers buy threads
- **WHEN** every worker is inside a handler that blocks for seconds and work is queued in the core
- **THEN** the supervisor SHALL observe starvation and start workers up to the ceiling, and the queued work SHALL be served

#### Scenario: Grown workers retire
- **WHEN** the blocking workload ends and the grown workers idle past the retirement streak
- **THEN** the pool SHALL return to the base size

#### Scenario: Concurrency is bounded by the core, not the pool
- **WHEN** the pool is larger than `FLEZHA_MAX_INFLIGHT_ACTIVATIONS`
- **THEN** work activations underway SHALL NOT exceed the core's cap

#### Scenario: The main thread stays available to signal handling
- **WHEN** the runner is serving under full load
- **THEN** no activation SHALL be dispatched on the process's main thread

### Requirement: The starvation supervisor is required, not an optimisation

The SDK SHALL NOT offer a pool configuration that omits the starvation supervisor, including a single-threaded mode. A completion can block: an emit onto a full `DropNothing`-policy consumer mailbox parks the completing thread, and the inline-drain hook that rescues the in-process Rust host is not reachable through the C ABI. A host whose only thread is parked in a completion is therefore not entering `poll`, which is what the core reports as starvation, and the worker the supervisor starts is what drains the consumer and releases the producer.

#### Scenario: A DropNothing-policy wedge recovers on a base pool of one
- **WHEN** the pool base is configured to one worker and a handler emits onto a full `DropNothing`-policy consumer mailbox
- **THEN** the supervisor SHALL observe starvation, start a worker, and that worker SHALL drain the consumer so the blocked completion returns

#### Scenario: The supervisor cannot be configured away
- **WHEN** a configuration attempts to run the pool without the supervisor
- **THEN** the SDK SHALL refuse it rather than start a host that can wedge

### Requirement: Every blocking core call releases the GIL

The binding SHALL release the global interpreter lock for the duration of every blocking core call — `poll`, `wait_starvation`, `complete` and the steps relay inbox receive — so that no blocking core call prevents another Python thread from running. The binding SHALL NOT hold a Rust lock across arbitrary Python execution.

#### Scenario: A blocked steps relay receive does not freeze the interpreter
- **WHEN** a steps stage waits on the inbox for several seconds
- **THEN** other Python threads, including fetchers, SHALL continue to execute Python code throughout the wait

#### Scenario: A blocked poll does not freeze the interpreter
- **WHEN** every worker is parked in `poll` with no work available
- **THEN** an unrelated Python thread SHALL run at full speed

#### Scenario: A blocked completion does not freeze the interpreter
- **WHEN** a completion parks behind a full `DropNothing`-policy consumer mailbox
- **THEN** other Python threads SHALL continue to execute, including the worker that drains that consumer

### Requirement: The poll timeout is the documented interrupt granularity

Because shutdown wakes every poller and the ABI exposes no per-thread wake, worker threads SHALL bound each `poll` by a timeout, and that timeout SHALL be the documented upper bound on how quickly `KeyboardInterrupt`, a termination signal, and runner close are observed. The value SHALL be documented alongside the guarantee.

#### Scenario: Interrupt is observed within the bound
- **WHEN** the process receives an interrupt while all workers are parked in `poll` with no work
- **THEN** shutdown SHALL begin within the documented poll timeout rather than waiting for work to arrive

### Requirement: The executor seam may never affect per-cell ordering

The SDK SHALL expose an executor seam with at least a thread executor (default) and an asyncio executor. Under every executor the thread that fetches SHALL be a real Python thread — never a coroutine, and never borrowed from an event loop's default executor or any other pool the application also uses — and an activation SHALL be completed only after its dispatch has returned. An executor SHALL NOT observe, reorder, batch or coalesce activations.

The executor SHALL be selected by `FLEZHA_PYTHON_EXECUTOR` (default the thread executor) as an axis of the run, never encoded per test, per fixture or per manifest entry.

#### Scenario: Ordering holds under either executor
- **WHEN** the conformance suite runs under the thread executor and again under the asyncio executor
- **THEN** per-cell input ordering and output identity SHALL be identical in both runs

#### Scenario: The asyncio executor does not fetch on the event loop
- **WHEN** the asyncio executor is active
- **THEN** `poll` SHALL be called from a real thread and activations SHALL reach the loop through a thread-safe hand-off

#### Scenario: Completing from a different thread than the one that polled is supported
- **WHEN** an executor dispatches on a thread or loop other than the fetcher's and completes there
- **THEN** the completion SHALL be accepted and the cell released identically to same-thread completion

#### Scenario: One test app serves both executors
- **WHEN** the suite is run under each executor
- **THEN** the same test app and manifest SHALL serve both, with no per-executor fixture, test name or manifest entry

### Requirement: Synchronous and asynchronous handlers are both first class

Handlers SHALL be ordinary synchronous callables by default. The SDK SHALL detect a coroutine-function handler and drive it to completion under whichever executor is active, without requiring the user to declare which style they are using. Both styles SHALL observe identical ordering, retry and durability semantics, and a synchronous user SHALL NOT be required to install or import an event loop library.

#### Scenario: A synchronous handler runs unchanged
- **WHEN** a handler performs a blocking HTTP call
- **THEN** the SDK SHALL invoke it directly and SHALL NOT require any async adaptation from the user

#### Scenario: An async handler is driven under both executors
- **WHEN** a coroutine-function handler is served under the thread executor and again under the asyncio executor
- **THEN** it SHALL be driven to completion in both, with identical ordering and durability semantics

### Requirement: The SDK spawns no processes and refuses use after a fork

The SDK SHALL NOT fork, spawn or supervise processes; process count SHALL be the orchestrator's decision, and each process SHALL be one runner. The SDK SHALL record the process that started the core and SHALL raise, naming the rule, on any core call made from a different process. The documented entrypoint order SHALL be: declare classes, fork if the user forks at all, then start in the child.

#### Scenario: A core call after a fork is refused loudly
- **WHEN** a process forks after the core has been started and the child calls into the core
- **THEN** the SDK SHALL raise an error naming the fork rule, rather than wedging or silently failing to serve

#### Scenario: Fork-then-start is supported
- **WHEN** a user declares classes in the parent, forks, and starts the core in each child
- **THEN** each child SHALL serve as an independent runner

### Requirement: A free-threaded build tier ships, and per-interpreter state keeps sub-interpreters reachable

The SDK SHALL ship a free-threaded wheel tier alongside the GIL tier from its first release, with one source and one API; the free-threaded tier SHALL be documented as the build under which handler bodies execute in parallel. The runner SHALL report which tier it is running in its startup line.

All per-interpreter SDK state — the binding handle, the class registry view and the active executor — SHALL be resolved per interpreter rather than through a process-wide module global, so that a sub-interpreter executor can be added without restructuring. The sub-interpreter executor SHALL be recorded as deferred by name, with the follow-up change that owes it, in the SDK's documentation.

#### Scenario: Handler bodies run in parallel on the free-threaded tier
- **WHEN** two CPU-bound handlers of different Cells are served concurrently under the free-threaded wheel
- **THEN** their bodies SHALL execute in parallel, and the run SHALL be observably faster than the same workload on the GIL tier

#### Scenario: The build tier is never ambiguous
- **WHEN** a benchmark or bug report includes the runner's startup line
- **THEN** that line SHALL name the build tier, the active executor and the pool sizing

#### Scenario: No process-wide global holds the binding
- **WHEN** the SDK's state accessors are inspected
- **THEN** each SHALL resolve per interpreter, and none SHALL be a bare module-level mutable handle

#### Scenario: The deferral is named, not silent
- **WHEN** the SDK's documentation page is read
- **THEN** it SHALL name the sub-interpreter executor as deferred and name the follow-up that owes it

### Requirement: The parallelism model is documented as arithmetic

The SDK documentation SHALL state, without hedging: on the GIL tier, Python-level handler execution is bounded to one core per process; on the free-threaded tier it is not; everything below the poll seam runs with the GIL released and is genuinely parallel under either tier; work underway is capped by `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` regardless of pool size; throughput per loop is approximately the reciprocal of handler latency; and the levers for CPU-bound handlers are, in order, the free-threaded tier, process count, and native libraries that release the GIL.

#### Scenario: The ceiling is stated, not implied
- **WHEN** a reader consults the Python SDK page for how to scale CPU-bound handlers
- **THEN** it SHALL name the free-threaded tier and process count as the levers and SHALL state the one-core-per-process bound of the GIL tier explicitly

#### Scenario: A worked sizing example accompanies the model
- **WHEN** the page states the model
- **THEN** it SHALL work one example through from handler latency and desired in-flight concurrency to a loop and process count

