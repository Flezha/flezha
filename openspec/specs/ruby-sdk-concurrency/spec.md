# ruby-sdk-concurrency Specification

## Purpose
TBD - created by archiving change ruby-sdk. Update Purpose after archive.
## Requirements
### Requirement: The default host is a fetch-and-run thread pool with a starvation supervisor

The SDK's default executor SHALL run a base pool of Ruby threads, each looping `poll → dispatch → complete` on itself, and one supervisor thread parked in `flz_core_wait_starvation`. Each starvation observation SHALL start one additional worker, up to a ceiling; workers above the base SHALL retire after an idle streak. The base SHALL default to the machine's processor count and SHALL be configured by `FLEZHA_WORKER_POOL_SIZE`; the ceiling SHALL default to ten times the base, never below it, and SHALL be configured by `FLEZHA_WORKER_POOL_MAX`. The SDK SHALL introduce no additional pool knob.

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

### Requirement: Every blocking core call releases the GVL

The binding SHALL release the global VM lock for the duration of every blocking core call — `poll`, `wait_starvation`, `complete` and the steps relay inbox receive — so that no blocking core call prevents another Ruby thread from running.

#### Scenario: A blocked steps relay receive does not freeze the VM
- **WHEN** a steps stage waits on the inbox for several seconds
- **THEN** other Ruby threads, including fetchers, SHALL continue to execute Ruby code throughout the wait

#### Scenario: A blocked poll does not freeze the VM
- **WHEN** every worker is parked in `poll` with no work available
- **THEN** an unrelated Ruby thread SHALL run at full speed

### Requirement: The poll timeout is the documented interrupt granularity

Because shutdown wakes every poller and the ABI exposes no per-thread wake, worker threads SHALL bound each `poll` by a timeout, and that timeout SHALL be the documented upper bound on how quickly thread interruption, `Interrupt`, and runner close are observed. The value SHALL be documented alongside the guarantee.

#### Scenario: Interrupt is observed within the bound
- **WHEN** the process receives an interrupt while all workers are parked in `poll` with no work
- **THEN** shutdown SHALL begin within the documented poll timeout rather than waiting for work to arrive

### Requirement: The executor seam may never affect per-cell ordering

The SDK SHALL expose an executor seam with at least a thread executor (default) and a `Fiber::Scheduler` executor. Under every executor the thread that fetches SHALL be a real Ruby thread — never a fiber, and never borrowed from a shared I/O pool the application also uses — and an activation SHALL be completed only after its dispatch has returned. An executor SHALL NOT observe, reorder, batch or coalesce activations.

#### Scenario: Ordering holds under either executor
- **WHEN** the conformance suite runs under the thread executor and again under the fiber executor
- **THEN** per-cell input ordering and output identity SHALL be identical in both runs

#### Scenario: The fiber executor does not fetch on a fiber
- **WHEN** the fiber executor is active
- **THEN** `poll` SHALL be called from a real thread and activations SHALL reach fibers through a hand-off queue

#### Scenario: Completing from a different thread than the one that polled is supported
- **WHEN** an executor dispatches on a thread or fiber other than the fetcher and completes there
- **THEN** the completion SHALL be accepted and the cell released identically to same-thread completion

### Requirement: The SDK spawns no processes and refuses use after a fork

The SDK SHALL NOT fork, spawn or supervise processes; process count SHALL be the orchestrator's decision, and each process SHALL be one runner. The SDK SHALL record the process that started the core and SHALL raise, naming the rule, on any core call made from a different process. The documented entrypoint order SHALL be: declare classes, warm up, fork if the user forks at all, then start in the child.

#### Scenario: A core call after a fork is refused loudly
- **WHEN** a process forks after the core has been started and the child calls into the core
- **THEN** the SDK SHALL raise an error naming the fork rule, rather than wedging or silently failing to serve

#### Scenario: Fork-then-start is supported
- **WHEN** a user declares classes in the parent, forks, and starts the core in each child
- **THEN** each child SHALL serve as an independent runner

### Requirement: Per-VM SDK state is Ractor-local, and the Ractor executor is deferred by name

All per-VM SDK state — the binding handle, the class registry view and the active executor — SHALL be resolved per Ractor rather than through a process-wide singleton, so that a Ractor executor can be added without restructuring. The Ractor executor SHALL be recorded as deferred by name, with the follow-up change that owes it, in the SDK's documentation.

#### Scenario: No process-wide singleton holds the binding
- **WHEN** the SDK's state accessors are inspected
- **THEN** each SHALL resolve per Ractor, and none SHALL be a bare constant or global holding a mutable handle

#### Scenario: The deferral is named, not silent
- **WHEN** the SDK's documentation page is read
- **THEN** it SHALL name the Ractor executor as deferred and name the follow-up that owes it

### Requirement: The parallelism model is documented as arithmetic

The SDK documentation SHALL state, without hedging: Ruby-level handler execution is bounded to one core per process; everything below the poll seam runs with the GVL released and is genuinely parallel; work underway is capped by `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` regardless of pool size; and the lever for CPU-bound handlers is process count, not thread count.

#### Scenario: The ceiling is stated, not implied
- **WHEN** a reader consults the Ruby SDK page for how to scale CPU-bound handlers
- **THEN** it SHALL name process count as the lever and SHALL state the one-core-per-process bound explicitly

