# bounded-operative-durability Specification

## Purpose
TBD - created by archiving change bounded-operative-functionality. Update Purpose after archive.
## Requirements
### Requirement: WAL persistence for a bounded operative is durable by default, opt-out per class

A bounded operative class SHALL be **durable by default**; an author MAY **opt out** per class (fire-and-forget) via a flag on the operative (mirroring the unbounded persistence flag), resolved at start through the existing persistence resolver, with an operator override still available. When durable, its operations/segments SHALL be WAL-backed on the **same substrate** (WalWriter framing, snapshots, fsync batching, retention) and the **same `{operative_class, operation_id, segment_key}` identity** an unbounded operative uses. When opted out, a bounded operative SHALL be pure in-memory and SHALL leave no on-disk state artifacts.

#### Scenario: Durable bounded operative is WAL-backed by default

- **WHEN** a bounded operative class does not opt out of persistence and writes Segment state
- **THEN** its Segment state changes SHALL be appended to the WAL on the shared substrate exactly as an unbounded operative's are

#### Scenario: Opted-out bounded operative writes nothing to disk

- **WHEN** a bounded operative class opts out of persistence and runs to completion
- **THEN** no WAL, snapshot, or state files SHALL be created for it

### Requirement: A durable bounded operative resumes from the WAL on crash, at-least-once

When a durable bounded operative's executor or controller restarts mid-run, the operative SHALL be respawned and its Operation/Segment state restored from the WAL via the bind-time restore path, then execution SHALL **resume at the current operation/iteration** rather than restarting from the beginning; the controller-side **op-journal** decides redo-vs-skip — operations recorded `op_committed` SHALL be skipped and the first uncommitted operation SHALL be seeded from the journaled handoff batch. For the `run`-sugar form (no segments, no checkpoints) "resume" SHALL mean re-running `run` from the top; an instance with no journal (opted-out class, or created before the journal existed) SHALL also re-run from the top. Execution SHALL therefore be **at-least-once** — a step whose completion was not journaled before the crash MAY run again on resume — so steps and compensations SHALL be idempotent by author contract. A segment error inside a structured operation SHALL first pass through the per-segment retry ladder (`bounded-segment-retry`); an error that is non-retryable or has exhausted the ladder SHALL be terminal — the operative compensates and terminates with status `error`, with no further platform re-run of the operative (author loops own their own semantic retry); this is distinct from crash-resume. A crash during a retry ladder SHALL converge on normal crash-resume of the uncommitted operation (the in-memory attempt counter resets).

#### Scenario: Crash mid-run resumes from durable state

- **WHEN** a durable bounded operative has committed Segment state and journaled `op_committed` entries for several completed operations, then its executor crashes and it is respawned
- **THEN** it SHALL restore the committed state, skip the journaled operations, and continue from the first uncommitted operation seeded with the journaled handoff — not from the beginning

#### Scenario: `run`-sugar re-runs from the top on crash

- **WHEN** a durable `run`-sugar bounded operative (no segments) crashes mid-run and is respawned
- **THEN** `run` SHALL be re-run from the top, relying on author idempotency, rather than reporting a partial result

#### Scenario: Exhausted or non-retryable error is terminal

- **WHEN** a bounded operative's segment error is non-retryable or has exhausted the retry ladder
- **THEN** the operative SHALL compensate completed operations and terminate with status `error`, and the platform SHALL NOT automatically re-run the operative

### Requirement: A finished bounded operative's data is retained for a window

On terminal teardown (status `ok | error | cancelled`), a durable bounded operative's data SHALL NOT be deleted immediately. The controller SHALL record a completion record (status, result/error payload, finished-at) alongside the retained final state and SHALL retain them for a configurable window — default **3 days** (`:bounded_completed_retention_hours = 72`, per-class override, env `FLEZHA_BOUNDED_RETENTION_HOURS`) — after which the existing retention sweeper SHALL delete them. During the window, `flz operative info <id>` SHALL report the finished operative's terminal status, result/error, and finish time.

#### Scenario: Finished-job data survives until the window elapses

- **WHEN** a durable bounded operative terminates and the retention window has not elapsed
- **THEN** its final state and completion record SHALL remain on disk and `flz operative info <id>` SHALL report its terminal status and result

#### Scenario: Retention sweeper cleans up after the window

- **WHEN** a finished bounded operative's `finished_at` plus the retention window is in the past
- **THEN** the retention sweeper SHALL delete its retained state and completion record

