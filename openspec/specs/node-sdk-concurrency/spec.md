# node-sdk-concurrency Specification

## Purpose
TBD - created by archiving change node-sdk. Update Purpose after archive.
## Requirements
### Requirement: No Blocking Core Call Is Synchronously Reachable From JavaScript

The SDK SHALL NOT expose any blocking core entry point as a synchronous call from JavaScript.
`flz_core_poll` SHALL NOT be exported to JavaScript at all. `flz_core_complete`,
`flz_core_spawn_result` read with a nonzero timeout, and `flz_core_start`
SHALL be reachable only as asynchronous operations whose blocking work executes on a thread that is
not an event-loop thread.

This requirement exists to close a process-wide deadlock. Under the default `DropNothing` overflow
policy a completion's routing push waits until the consumer's mailbox drains, and the consumer
drains only when its handler runs. If the waiting thread is the event loop, the loop is the only
thread that could run that handler, and no other thread can free it.

Non-blocking entry points (abandon, buffer free, monotonic clock, last-error, `flz_core_spawn_request`,
and `flz_core_spawn_result` read with a timeout of zero) MAY be synchronous. `ctx.spawnSteps` SHALL
therefore return its handle synchronously, the handle's `tryResult()` SHALL be synchronous, and only
its `wait()` SHALL be asynchronous.

#### Scenario: A blocking entry point cannot be called from the event loop
- **WHEN** the veneer's exported surface is enumerated in a test
- **THEN** every blocking core entry point SHALL be absent or asynchronous, and the absence SHALL be
  asserted rather than reviewed

#### Scenario: A full no-drop consumer does not wedge the process
- **WHEN** a tasklet emits onto a full consumer mailbox declared `DropNothing`, with the
  consumer's handler runnable only on an event loop
- **THEN** the completion SHALL wait off-loop, the consumer's activation SHALL be dispatched and
  drained, and the completion SHALL then return

#### Scenario: Spawn returns on the event loop and waits off it
- **WHEN** a tasklet calls `ctx.spawnSteps('worker', payload)` on an event loop and then
  awaits `handle.wait()` while the controller is slow to answer
- **THEN** the spawn call SHALL return the handle without blocking the loop, `tryResult()` SHALL
  answer pending without blocking the loop, and the waiting read SHALL run on a thread that is not
  an event-loop thread

### Requirement: Fetch Threads Are the SDK's Own and Never the Runtime's Shared Pool

The SDK SHALL poll on native threads it owns, and SHALL NOT poll on the JavaScript runtime's shared
I/O threadpool. A fetch thread SHALL NOT execute user code, so that it is always available to enter
`poll`. The fetcher count SHALL be configurable and SHALL default to the value the Java host uses.

#### Scenario: Polling does not consume shared I/O capacity
- **WHEN** the runner is polling with every fetcher blocked in the core
- **THEN** the runtime's shared I/O threadpool SHALL remain fully available to unrelated file,
  name-resolution and cryptographic work

### Requirement: The Completion Pool Grows So a Blocked Push Cannot Exhaust It

Completions SHALL execute on a pool of native threads whose size grows when a completion is queued
and every thread is occupied, up to a configured ceiling. The pool SHALL be sized through the
existing worker-pool base and ceiling configuration rather than through new configuration
vocabulary, and threads above the base SHALL retire after an idle streak.

A fixed pool is refused: if every thread parks pushing to one consumer, the result that would drain
that consumer has no thread to run on, and the deadlock re-forms one level out from the one the
event-loop rule closes.

Growth SHALL be driven by the host's own observation of a queued completion with no free thread,
because the core cannot observe the host's completion queue.

#### Scenario: A blocked push does not exhaust the pool
- **WHEN** every completion thread is parked pushing to a full consumer mailbox and a further
  completion is queued
- **THEN** the pool SHALL grow, the queued completion SHALL execute, and the runner SHALL make
  progress

#### Scenario: The pool returns to base
- **WHEN** the condition that grew the pool has cleared and the added threads have been idle for the
  retirement streak
- **THEN** the pool SHALL return to its base size

### Requirement: A Starvation Supervisor Covers Queued Direct Activations

The SDK SHALL park one thread in the core's starvation wait, and SHALL respond to an observation by
making a thread available to enter `poll`. The supervisor SHALL end when the runner shuts down,
observing the same three-way shape as `poll`.

This covers the condition the core can observe — a queued direct activation with no thread inside
`poll` — which is distinct from, and not a substitute for, the completion-pool growth rule.

#### Scenario: A queued direct activation is promoted
- **WHEN** a restore fold, snapshot request or steps relay step is queued with no thread inside `poll`
  for the core's dwell
- **THEN** the observation SHALL result in a thread entering `poll` and the activation SHALL be
  handed out

### Requirement: Dispatch Targets One of N Event Loops Over a Single Code Path

An activation SHALL be handed to one event loop selected by the host, and the selection SHALL be the
only difference between the single-loop and multi-loop configurations. The number of dispatch loops
SHALL be configurable both in code and by environment variable, SHALL default to the single-loop
configuration, and SHALL NOT require a different authoring surface, registration form, or dispatch
path.

The selection SHALL send every activation addressed to a cell to the same loop, and SHALL spread
only the prepare crossing — the one activation that names no cell, because the key is what it is
computing — across loops. A dispatch loop is a separate isolate holding the state its restore fold
materialized, so an activation for that cell sent elsewhere would find nothing to fold into.
Exclusivity makes either selection safe for concurrency; only one of them is safe for state.

A cell that carries no key is still a cell: a single-cell tasklet holds state exactly as a
keyed one does, and an absent key SHALL NOT be read as an absent cell.

#### Scenario: A cell's activations reach the loop holding its state
- **WHEN** a cell's restore fold has run on one dispatch loop under a multi-loop configuration
- **THEN** its later activations SHALL be dispatched to that same loop

#### Scenario: A single-cell tasklet is not spread across loops
- **WHEN** successive activations reach a tasklet that declares no keys under a multi-loop
  configuration
- **THEN** they SHALL all be dispatched to the loop holding that tasklet's one cell

#### Scenario: The tiers share one dispatch path
- **WHEN** the same handler module is run under the single-loop and multi-loop configurations
- **THEN** dispatch, completion, ordering and results SHALL be identical, and the handler source
  SHALL be unchanged

#### Scenario: Per-cell ordering holds across loops
- **WHEN** successive inputs for one cell are dispatched under a multi-loop configuration
- **THEN** they SHALL be processed in enqueue order regardless of which loop takes each activation

### Requirement: Handlers Are Registered by Module Specifier

The runner entry point SHALL accept handler modules by specifier rather than as already-imported
values, in every dispatch configuration. A dispatch loop SHALL import the module itself.

Accepting an imported value is refused because a live value cannot cross into a separate isolate, so
it would make enabling additional dispatch loops a breaking change to the authoring surface.

#### Scenario: Enabling more loops is a flag, not a rewrite
- **WHEN** a runner written against the single-loop default is reconfigured for multiple loops
- **THEN** no change to the handler module or its registration SHALL be required

#### Scenario: An unimportable module is named
- **WHEN** a registered specifier cannot be imported
- **THEN** startup SHALL fail naming the specifier and chaining the original import error

### Requirement: Module-Scope State Is Per Dispatch Loop, and the SDK Says So

The SDK SHALL document that state held at module scope is instantiated once per dispatch loop, and
SHALL prove both configurations in its test app. Durable state SHALL be unaffected, being owned by
the core through the declared change fold.

#### Scenario: Isolation is observable and specified
- **WHEN** a handler module holds module-scope state and runs under a multi-loop configuration
- **THEN** each loop SHALL hold its own instance, and this SHALL be asserted by a test rather than
  described only in prose

### Requirement: Every Activation Is Answered Exactly Once Through a Structural Guard

Every dispatched activation SHALL be answered exactly once — completed, failed, or abandoned —
through a guard that runs on any exit path, including a thrown error, an unhandled promise
rejection, and the termination of the dispatch loop that held it. The guard SHALL NOT be a
convention applied at each return site.

An unanswered activation retires its cell for the life of the process, so this is a correctness
requirement rather than a hygiene one.

#### Scenario: A throwing handler abandons rather than retiring its cell
- **WHEN** a handler throws, or its returned promise rejects
- **THEN** the activation SHALL be answered, and the cell SHALL receive subsequent activations

#### Scenario: A terminating dispatch loop abandons what it held
- **WHEN** a dispatch loop terminates with activations outstanding
- **THEN** every outstanding activation SHALL be abandoned before the replacement loop begins
  dispatching

### Requirement: A Dispatch Loop That Ends Is Replaced

A dispatch loop that ends for any reason other than shutdown SHALL be replaced without user
action, and its departure SHALL NOT permanently reduce the host's dispatch capacity. A fetch thread
that ends on an error SHALL likewise be replaced.

The single-loop configuration SHALL document that the main event loop cannot be replaced, and that
this is the reason a multi-loop configuration is the recommended production posture.

#### Scenario: A dead worker is replaced
- **WHEN** a dispatch worker terminates unexpectedly
- **THEN** a replacement SHALL start, the configured loop count SHALL be restored, and the failure
  SHALL be reported

### Requirement: Event-Loop Delay Is Observed and Reported

The SDK SHALL observe its dispatch loops' event-loop delay and SHALL report a warning, naming the
tasklet most recently dispatched on that loop, when delay crosses a documented threshold.

A synchronous compute-bound handler is the one failure this SDK cannot prevent, so it SHALL be
diagnosable at its source rather than only as a controller-side stall verdict.

#### Scenario: A blocking handler is diagnosed at the host
- **WHEN** a handler occupies its dispatch loop past the delay threshold
- **THEN** the SDK SHALL warn naming the tasklet and the observed delay, and SHALL point at the
  multi-loop configuration as the remedy

### Requirement: Shutdown Drains In-Flight Activations

Shutdown SHALL be observed from a signal, from end of file on standard input, and from a
controller-requested stop, and SHALL drain in-flight activations before exit. The poll timeout SHALL
bound how quickly a shutdown request is observed, and that bound SHALL be documented.

#### Scenario: Every dispatch loop is drained
- **WHEN** shutdown is requested with activations in flight across several dispatch loops
- **THEN** every loop SHALL be drained or its activations abandoned, and the process SHALL exit
  without requiring a further signal

#### Scenario: A held loop does not prevent the runner from being stopped
- **WHEN** end of file is reached on standard input while a handler is holding its dispatch loop
- **THEN** the end of file SHALL be observed off that loop, the remaining loops SHALL be given the
  documented grace period to drain, and the process SHALL end without a signal

### Requirement: The Parallelism Model Is Stated as Arithmetic

The SDK's documentation SHALL state its concurrency limits as numbers rather than adjectives: that
concurrent activations are bounded by the core's in-flight cap and not by the fetcher or loop count,
that compute parallelism equals the dispatch loop count, and that the single-loop default has a
compute parallelism of one.

#### Scenario: The documentation states the bound
- **WHEN** a user reads the concurrency documentation
- **THEN** it SHALL name the in-flight cap, the fetcher default, the loop count's role, and which of
  those bounds which kind of work

