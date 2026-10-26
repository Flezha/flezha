# steps-cell-retry Specification

## Purpose

The steps-cell-retry capability is the retry rung between a cell failure and the abort of a steps tasklet: a cell whose work errors inside a structured tasklet is retried in place — default 3 attempts with the platform's shared jittered exponential backoff — while sibling cells' completed results stay buffered at the barrier. A retry that succeeds lets the stage quiesce and commit normally; exhaustion (or an author-declared non-retryable error) fails the tasklet into the existing AbortAll + saga path with status `error`, introducing no new terminal state. Retry is strictly pre-commit, so a committed tasklet is never retried and a crash mid-ladder converges on normal crash-resume of the uncommitted tasklet. Authors classify permanent failures via a non-retryable marker on `StepsError` and tune attempts/backoff per class (attempts = 0 restores fail-fast).
## Requirements
### Requirement: A failing cell is retried with backoff before the tasklet fails

A cell whose work errors inside a structured steps tasklet SHALL be retried in place on the
core's per-item retry ladder, the one a flow cell climbs, under the retry policy of the edge
that feeds the tasklet, the same `retry_max` / `retry_backoff_ms` a flow relay's edge declares:
default 3 attempts (the first run plus its retries), with the platform's shared jittered
exponential backoff (`base × 2^(n-1)` ±50%, capped at `RETRY_BACKOFF_MAX_MS` = 5 s; default base
50 ms, the edge's default, computed by the same helper the flow relay's retry-then-park ladder uses).
The failed item and the untouched tail of its batch are held at the head of the cell's line and
re-handed after the backoff; sibling cells' settled results stay held on the instance's pass
sheet and are not re-run. There SHALL be no class-level retry policy on a steps relay: the policy
the ladder reads SHALL be the one the shared core already derives for the tasklet from its
input channel, and a steps relay SHALL carry no `retry_attempts`, `retry_backoff` or
`barrier_retry_attempts` declaration in any SDK or on the wire. A retry that succeeds SHALL let
the stage reach its count and commit normally. A cancellation (external stop / `max_duration`)
SHALL end a pending backoff at once by disarming its deferred wake and releasing the cell's
gates. The retried item re-runs under the same at-least-once idempotency contract crash-resume
imposes.

#### Scenario: Transient failure recovers without failing the task

- **WHEN** cell k2 of a parallel stage errors once with a retryable error and succeeds on retry, while k1 and k3 settled
- **THEN** the stage SHALL reach its count and commit normally, k1/k3 SHALL NOT re-run, and no compensation SHALL fire

#### Scenario: The edge's budget is the ladder's

- **WHEN** a steps relay wires `c.input("order").retry_max(5).retry_backoff_ms(20).into(Reserve)` and `Reserve`'s cell errors retryably on every attempt
- **THEN** the cell SHALL run five times, each wait SHALL grow from a 20 ms base, and the tasklet SHALL fail after the fifth attempt

#### Scenario: Only the failed item and its tail re-run

- **WHEN** a cell's stage share is three items and the second fails retryably
- **THEN** the first item SHALL NOT run again, and the retry SHALL re-hand the second item and then the third, in that order

#### Scenario: Stop interrupts a waiting retry

- **WHEN** an external stop arrives while a cell is in a backoff wait
- **THEN** the wait SHALL end at once, the cell's held items SHALL be dropped, and the relay SHALL take the normal abnormal-exit path (compensation of completed tasklets)

### Requirement: Retry exhaustion fails the tasklet exactly as today

A cell SHALL fail its tasklet after the configured attempts are exhausted, surfacing
the original error into the existing abnormal-termination path: the tasklet
does not commit, AbortAll/compensation runs per the existing saga contract, and
the relay terminates with status `error`. No new terminal state SHALL be
introduced. In a `per_input` loop declaring keep-progress the exhausted Cell SHALL fail
only the turn: the pass stops, the turn is dead-lettered and undone, and the relay does
not terminate (see `steps-durable-loop`); the same holds for a non-retryable error.
Each retry attempt SHALL emit telemetry (instance, tasklet, cell, attempt, delay).

#### Scenario: Persistent failure aborts after the ladder

- **WHEN** a cell errors retryably on every one of its 3 attempts, in a relay that is not a `per_input` loop declaring keep-progress
- **THEN** the tasklet SHALL fail with the original error, completed tasklets SHALL be compensated, and the run SHALL terminate with status `error`

#### Scenario: Retries are observable

- **WHEN** a cell is retried
- **THEN** a telemetry event with the instance, tasklet, cell, attempt number, and applied delay SHALL be emitted

#### Scenario: Exhaustion in a per-input keep-progress loop fails the turn

- **WHEN** a cell of a `per_input` keep-progress relay errors retryably on every one of
  its 3 attempts
- **THEN** the tasklet SHALL fail with the original error, the turn SHALL be dead-lettered
  with `attempts` 3 and that error, and the relay SHALL NOT terminate

### Requirement: Authors can classify errors as non-retryable and tune the policy

`StepsError` SHALL support a non-retryable marker; an error so marked SHALL
skip the ladder and fail the tasklet immediately. Errors produced by the
existing string conversions SHALL remain retryable by default. The policy knobs SHALL be the
edge's, `retry_max` and `retry_backoff_ms` on the channel builder of every SDK, exactly as a
stream tunes them; the `Relay` trait SHALL expose no per-class knob for attempt
count or backoff base, and `retry_max(1)` SHALL be fail-fast, `retry_max(0)` being refused as
`topology.retry_max_zero` for both kinds. Retry SHALL apply to
every steps relay's tasklet cells, since every steps relay runs tasklets, and
SHALL only ever occur before the tasklet's commit (a committed tasklet is
never retried).

#### Scenario: Non-retryable error aborts immediately

- **WHEN** a cell returns an error marked non-retryable on its first attempt
- **THEN** no retry SHALL occur and the tasklet SHALL fail immediately

#### Scenario: retry_max(1) is fail-fast

- **WHEN** a steps relay's entry edge declares `retry_max(1)` and a cell errors
- **THEN** the tasklet SHALL fail on the first error, and the same declaration with `retry_max(0)` SHALL be refused at registration naming `topology.retry_max_zero`

#### Scenario: The class-level knobs are gone from every SDK

- **WHEN** every SDK's steps relay surface and `test/fixtures/host_class_decl_keys.txt` are searched for `retry_attempts`, `retryAttempts`, `RetryAttempts`, `retry_backoff`, `retryBackoff`, `RetryBackoff`, `barrier_retry_attempts`, `barrierRetryAttempts` or `BarrierRetryAttempts`
- **THEN** no trait, interface, definition type, decorator keyword, class-body setting, attribute argument, `use` option or declaration key carries one, and `ZeroRetryBackoff` names no refusal in any SDK or in the core

### Requirement: A backoff wait holds no worker thread

A retrying cell's backoff SHALL NOT occupy any thread: the attempt runs to completion on an SDK
worker, the worker is released, and the re-hand is timed by the core's deferred-wake list, the
same entry a flow cell's backoff is. There SHALL be no steps-owned ladder and no barrier
that times it. The retry policy (attempts, shared jittered exponential backoff), error
classification, exhaustion behaviour (the stage fails into compensation, never into the
dead-letter queue) and per-attempt telemetry SHALL be the flow relay's ladder's, with the terminal
action the only difference.

#### Scenario: Sibling stages proceed while a cell backs off

- **WHEN** cell k2 of a parallel stage is waiting out a retry backoff while
  unrelated activations are queued
- **THEN** no thread SHALL be sleeping on k2's behalf, and the queued
  activations SHALL be served by the workers that ran k2's failed attempt

#### Scenario: Exhaustion compensates rather than parks

- **WHEN** a steps cell's item fails on its final attempt, or with a non-retryable error
- **THEN** no dead-letter entry SHALL be minted; the cell's stage SHALL be marked failed on the
  instance's pass sheet, and once no stage of the pass owes an item the relay SHALL
  compensate and end with status `error`

#### Scenario: Stop disarms a pending backoff

- **WHEN** an external stop arrives while the deferred-wake list holds a cell's retry entry
- **THEN** the entry SHALL be removed, the cell's gates released and its held items dropped on
  the thread that brings the stop, and the relay SHALL take the normal abnormal-exit path

### Requirement: A commit barrier is re-sent a fixed number of times

The re-send of a task-commit, iteration-commit or compensation barrier whose acknowledgement timed out (`FLEZHA_STEPS_ACK_TIMEOUT_MS`) SHALL be governed by the runner's controller connection, not by the class: a constant of three sends, each waited out with the shared ladder's default backoff base, declared beside the acknowledgement timeout in the shared core and read by the Elixir SDK's driver from its knob table. The waiter SHALL stay registered across the re-sends so a late acknowledgement continues the chain, and only the exhausted count SHALL be a terminal error, as today. A barrier the controller refuses as `commit.record_not_durable` SHALL be sent again under the same reference id after the shared ladder's backoff from a 1 s base, within the same three sends; any other refusal of a forward task-commit or an iteration-commit barrier SHALL fail it at once.

#### Scenario: A stalled acknowledgement is re-sent three times

- **WHEN** a controller withholds every `TaskCommitResponse`
- **THEN** the runner SHALL send the same barrier under the same reference id three times in total and then end the run with a terminal error, and no class setting SHALL change that count

#### Scenario: A barrier refused as not durable in time is re-sent

- **WHEN** a controller refuses a task-commit barrier with `commit.record_not_durable`
- **THEN** the runner SHALL send the same barrier again under the same reference id after a backoff, up to three sends in total, and fail the stage only when the last send is refused
- **AND** a refusal with any other code SHALL fail the stage without a re-send

