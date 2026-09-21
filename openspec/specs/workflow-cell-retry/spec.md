# workflow-cell-retry Specification

## Purpose

The workflow-cell-retry capability is the retry rung between a cell failure and the abort of a workflow tasklet: a cell whose work errors inside a structured tasklet is retried in place — default 3 attempts with the platform's shared jittered exponential backoff — while sibling cells' completed results stay buffered at the barrier. A retry that succeeds lets the stage quiesce and commit normally; exhaustion (or an author-declared non-retryable error) fails the tasklet into the existing AbortAll + saga path with status `error`, introducing no new terminal state. Retry is strictly pre-commit, so a committed tasklet is never retried and a crash mid-ladder converges on normal crash-resume of the uncommitted tasklet. Authors classify permanent failures via a non-retryable marker on `WorkflowError` and tune attempts/backoff per class (attempts = 0 restores fail-fast).
## Requirements
### Requirement: A failing cell is retried with backoff before the tasklet fails

A cell whose work errors inside a structured workflow tasklet SHALL be
retried in place — default 3 attempts (the first run plus its retries), with the
platform's shared jittered exponential backoff (`base × 2^(n-1)` ±50%, capped at
`RETRY_BACKOFF_MAX_MS` = 5 s; default base 500 ms — the same helper the stream
retry-then-park ladder uses) — while sibling cells' completed results stay
buffered at the barrier and are not re-run. A retry that succeeds SHALL let the stage quiesce and commit
normally. The backoff wait SHALL observe cancellation (external stop /
`max_duration`) at the existing wake granularity. The retried cell body
re-runs under the same at-least-once idempotency contract crash-resume imposes.

#### Scenario: Transient failure recovers without failing the task

- **WHEN** cell k2 of a parallel stage errors once with a retryable error and succeeds on retry, while k1 and k3 completed
- **THEN** the stage SHALL quiesce and commit normally, k1/k3 SHALL NOT re-run, and no compensation SHALL fire

#### Scenario: Stop interrupts a waiting retry

- **WHEN** an external stop arrives while a cell is in a backoff wait
- **THEN** the wait SHALL end promptly and the operative SHALL take the normal abnormal-exit path (compensation of completed tasklets)

### Requirement: Retry exhaustion fails the tasklet exactly as today

A cell SHALL fail its tasklet after the configured attempts are exhausted, surfacing
the original error into the existing abnormal-termination path: the tasklet
does not commit, AbortAll/compensation runs per the existing saga contract, and
the operative terminates with status `error`. No new terminal state SHALL be
introduced. Each retry attempt SHALL emit telemetry (instance, tasklet,
cell, attempt, delay).

#### Scenario: Persistent failure aborts after the ladder

- **WHEN** a cell errors retryably on every one of its 3 attempts
- **THEN** the tasklet SHALL fail with the original error, completed tasklets SHALL be compensated, and the run SHALL terminate with status `error`

#### Scenario: Retries are observable

- **WHEN** a cell is retried
- **THEN** a telemetry event with the instance, tasklet, cell, attempt number, and applied delay SHALL be emitted

### Requirement: Authors can classify errors as non-retryable and tune the policy

`WorkflowError` SHALL support a non-retryable marker; an error so marked SHALL
skip the ladder and fail the tasklet immediately. Errors produced by the
existing string conversions SHALL remain retryable by default. The
`WorkflowOperative` trait SHALL expose per-class policy knobs for attempt count
and backoff base; attempts = 0 SHALL restore fail-fast. Retry SHALL apply to
every workflow's tasklet cells, since every workflow runs tasklets, and
SHALL only ever occur before the tasklet's commit (a committed tasklet is
never retried).

#### Scenario: Non-retryable error aborts immediately

- **WHEN** a cell returns an error marked non-retryable on its first attempt
- **THEN** no retry SHALL occur and the tasklet SHALL fail immediately

#### Scenario: attempts = 0 restores fail-fast

- **WHEN** a class sets its retry attempts to 0 and a cell errors
- **THEN** the tasklet SHALL fail on the first error, matching pre-change behavior

### Requirement: A backoff wait holds no worker thread

A retrying cell's backoff SHALL NOT occupy a worker thread: the step
attempt runs to completion on an SDK worker, the worker is released, and the
driver SHALL own the re-submission timing at its stage barrier. The retry
policy (attempts, shared jittered exponential backoff), error classification,
exhaustion behavior, per-attempt telemetry, and cancellation responsiveness
(external stop / `max_duration` observed at the existing wake granularity)
SHALL be unchanged.

#### Scenario: Sibling stages proceed while a cell backs off

- **WHEN** cell k2 of a parallel stage is waiting out a retry backoff while
  unrelated activations are queued
- **THEN** no worker thread SHALL be sleeping on k2's behalf, and the queued
  activations SHALL be served by the workers that ran k2's failed attempt

#### Scenario: Stop interrupts a driver-timed backoff

- **WHEN** an external stop arrives while the driver is timing a cell's
  backoff at its barrier
- **THEN** the wait SHALL end promptly and the operative SHALL take the normal
  abnormal-exit path, exactly as the pre-existing stop-interrupts-backoff
  scenario pins

