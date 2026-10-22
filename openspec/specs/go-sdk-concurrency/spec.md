# go-sdk-concurrency Specification

## Purpose
How the Go runner serves activations: a pool of poll goroutines grown by the core's governor, core buffers copied before use, and termination signals owned by Go and handed to the core.
## Requirements
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

