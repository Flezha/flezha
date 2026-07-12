# bounded-segment-retry Specification

## Purpose

The bounded-segment-retry capability is the retry rung between a segment failure and the abort of a bounded operation: a segment whose work errors inside a structured operation is retried in place — default 3 attempts with the platform's shared jittered exponential backoff — while sibling segments' completed results stay buffered at the barrier. A retry that succeeds lets the stage quiesce and commit normally; exhaustion (or an author-declared non-retryable error) fails the operation into the existing AbortAll + saga path with status `error`, introducing no new terminal state. Retry is strictly pre-commit, so a committed operation is never retried and a crash mid-ladder converges on normal crash-resume of the uncommitted operation. Authors classify permanent failures via a non-retryable marker on `BoundedError` and tune attempts/backoff per class (attempts = 0 restores fail-fast); `run`-sugar bodies keep terminal-on-error.

## Requirements

### Requirement: A failing segment is retried with backoff before the operation fails

A segment whose work errors inside a structured bounded operation SHALL be
retried in place — default 3 attempts (the first run plus its retries), with the
platform's shared jittered exponential backoff (`base × 2^(n-1)` ±50%, capped at
`RETRY_BACKOFF_MAX_MS` = 5 s; default base 500 ms — the same helper the unbounded
retry-then-park ladder uses) — while sibling segments' completed results stay
buffered at the barrier and are not re-run. A retry that succeeds SHALL let the stage quiesce and commit
normally. The backoff wait SHALL observe cancellation (external stop /
`max_duration`) at the existing wake granularity. The retried segment body
re-runs under the same at-least-once idempotency contract crash-resume imposes.

#### Scenario: Transient failure recovers without failing the op

- **WHEN** segment k2 of a parallel stage errors once with a retryable error and succeeds on retry, while k1 and k3 completed
- **THEN** the stage SHALL quiesce and commit normally, k1/k3 SHALL NOT re-run, and no compensation SHALL fire

#### Scenario: Stop interrupts a waiting retry

- **WHEN** an external stop arrives while a segment is in a backoff wait
- **THEN** the wait SHALL end promptly and the operative SHALL take the normal abnormal-exit path (compensation of completed operations)

### Requirement: Retry exhaustion fails the operation exactly as today

A segment SHALL fail its operation after the configured attempts are exhausted, surfacing
the original error into the existing abnormal-termination path: the operation
does not commit, AbortAll/compensation runs per the existing saga contract, and
the operative terminates with status `error`. No new terminal state SHALL be
introduced. Each retry attempt SHALL emit telemetry (instance, operation,
segment, attempt, delay).

#### Scenario: Persistent failure aborts after the ladder

- **WHEN** a segment errors retryably on every one of its 3 attempts
- **THEN** the operation SHALL fail with the original error, completed operations SHALL be compensated, and the run SHALL terminate with status `error`

#### Scenario: Retries are observable

- **WHEN** a segment is retried
- **THEN** a telemetry event with the instance, operation, segment, attempt number, and applied delay SHALL be emitted

### Requirement: Authors can classify errors as non-retryable and tune the policy

`BoundedError` SHALL support a non-retryable marker; an error so marked SHALL
skip the ladder and fail the operation immediately. Errors produced by the
existing string conversions SHALL remain retryable by default. The
`BoundedOperative` trait SHALL expose per-class policy knobs for attempt count
and backoff base; attempts = 0 SHALL restore fail-fast. Retry SHALL apply only
to structured-operation segments — `run`-sugar bodies keep terminal-on-error —
and SHALL only ever occur before the operation's commit (a committed operation
is never retried).

#### Scenario: Non-retryable error aborts immediately

- **WHEN** a segment returns an error marked non-retryable on its first attempt
- **THEN** no retry SHALL occur and the operation SHALL fail immediately

#### Scenario: attempts = 0 restores fail-fast

- **WHEN** a class sets its retry attempts to 0 and a segment errors
- **THEN** the operation SHALL fail on the first error, matching pre-change behavior

#### Scenario: run-sugar stays terminal on error

- **WHEN** a `run`-sugar bounded operative's body returns an error
- **THEN** the failure SHALL be terminal with no platform retry
</content>
</invoke>
