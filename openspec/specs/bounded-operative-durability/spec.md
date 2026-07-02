# bounded-operative-durability Specification

## Purpose
TBD - created by archiving change bounded-operative-functionality. Update Purpose after archive.
## Requirements
### Requirement: WAL persistence for a bounded operative is durable by default, opt-out per class

A bounded operative class SHALL be **durable by default**; an author MAY **opt out** per class (fire-and-forget) via a flag on the operative (mirroring the continuous persistence flag), resolved at start through the existing persistence resolver, with an operator override still available. When durable, its operations/segments SHALL be WAL-backed on the **same substrate** (WalWriter framing, snapshots, fsync batching, retention) and the **same `{operative_class, operation_id, segment_key}` identity** a continuous operative uses. When opted out, a bounded operative SHALL be pure in-memory and SHALL leave no on-disk state artifacts.

#### Scenario: Durable bounded operative is WAL-backed by default

- **WHEN** a bounded operative class does not opt out of persistence and writes Segment state
- **THEN** its Segment state changes SHALL be appended to the WAL on the shared substrate exactly as a continuous operative's are

#### Scenario: Opted-out bounded operative writes nothing to disk

- **WHEN** a bounded operative class opts out of persistence and runs to completion
- **THEN** no WAL, snapshot, or state files SHALL be created for it

### Requirement: A durable bounded operative resumes from the WAL on crash, at-least-once

When a durable bounded operative's executor or controller restarts mid-run, the operative SHALL be respawned and its Operation/Segment state restored from the WAL via the bind-time restore path, then execution SHALL **resume at the current operation/iteration** rather than restarting from the beginning; a per-iteration commit marker SHALL decide redo-vs-skip. For the `run`-sugar form (no segments, no checkpoints) "resume" SHALL mean re-running `run` from the top. Execution SHALL therefore be **at-least-once** — a step whose completion was not committed before the crash MAY run again on resume — so steps and compensations SHALL be idempotent by author contract. An operation returning an error SHALL remain terminal with **no automatic platform retry** (author loops own their own retry); this is distinct from crash-resume.

#### Scenario: Crash mid-run resumes from durable state

- **WHEN** a durable bounded operative has committed Segment state and several completed operations, then its executor crashes and it is respawned
- **THEN** it SHALL restore the committed state and continue from the next uncompleted operation, not from the beginning

#### Scenario: `run`-sugar re-runs from the top on crash

- **WHEN** a durable `run`-sugar bounded operative (no segments) crashes mid-run and is respawned
- **THEN** `run` SHALL be re-run from the top, relying on author idempotency, rather than reporting a partial result

#### Scenario: Error is terminal with no auto-retry

- **WHEN** a bounded operative's operation returns an error
- **THEN** the operative SHALL compensate completed operations and terminate with status `error`, and the platform SHALL NOT automatically re-run it

### Requirement: A finished bounded operative's data is retained for a window

On terminal teardown (status `ok | error | cancelled`), a durable bounded operative's data SHALL NOT be deleted immediately. The controller SHALL record a completion record (status, result/error payload, finished-at) alongside the retained final state and SHALL retain them for a configurable window — default **3 days** (`:bounded_completed_retention_hours = 72`, per-class override, env `FLEZHA_BOUNDED_RETENTION_HOURS`) — after which the existing retention sweeper SHALL delete them. During the window, `flz operative info <id>` SHALL report the finished operative's terminal status, result/error, and finish time.

#### Scenario: Finished-job data survives until the window elapses

- **WHEN** a durable bounded operative terminates and the retention window has not elapsed
- **THEN** its final state and completion record SHALL remain on disk and `flz operative info <id>` SHALL report its terminal status and result

#### Scenario: Retention sweeper cleans up after the window

- **WHEN** a finished bounded operative's `finished_at` plus the retention window is in the past
- **THEN** the retention sweeper SHALL delete its retained state and completion record

