# workflow-operative-durability Specification

## Purpose
TBD - created by archiving change workflow-operative-functionality. Update Purpose after archive.
## Requirements
### Requirement: WAL persistence for a workflow operative is durable by default, opt-out per class

A workflow operative class SHALL be **durable by default**; an author MAY **opt out** per class (fire-and-forget) via a flag on the operative (mirroring the stream persistence flag), resolved at start through the existing persistence resolver, with an operator override still available. When durable, its procedures/entities SHALL be WAL-backed on the **same substrate** (WalWriter framing, snapshots, fsync batching, retention) and the **same `{operative_class, procedure_id, entity_key}` identity** a stream operative uses. When opted out, a workflow operative SHALL be pure in-memory and SHALL leave no on-disk state artifacts.

#### Scenario: Durable workflow operative is WAL-backed by default

- **WHEN** a workflow operative class does not opt out of persistence and writes Entity state
- **THEN** its Entity state changes SHALL be appended to the WAL on the shared substrate exactly as a stream operative's are

#### Scenario: Opted-out workflow operative writes nothing to disk

- **WHEN** a workflow operative class opts out of persistence and runs to completion
- **THEN** no WAL, snapshot, or state files SHALL be created for it

### Requirement: A durable workflow operative resumes from the WAL on crash, at-least-once

When a durable workflow operative's runner or controller restarts mid-run, the operative SHALL be respawned and its Procedure/Entity state restored from the WAL via the bind-time restore path, then execution SHALL **resume at the current procedure/iteration** rather than restarting from the beginning; the controller-side **proc-journal** decides redo-vs-skip — procedures recorded `proc_committed` SHALL be skipped and the first uncommitted procedure SHALL be seeded from the journaled handoff batch. An instance with no journal (opted-out class, or created before the journal existed) SHALL re-run from the top. Execution SHALL therefore be **at-least-once** — a step whose completion was not journaled before the crash MAY run again on resume — so steps and compensations SHALL be idempotent by author contract. An entity error inside a structured procedure SHALL first pass through the per-entity retry ladder (`workflow-entity-retry`); an error that is non-retryable or has exhausted the ladder SHALL be terminal — the operative compensates and terminates with status `error`, with no further platform re-run of the operative (author loops own their own semantic retry); this is distinct from crash-resume. A crash during a retry ladder SHALL converge on normal crash-resume of the uncommitted procedure (the in-memory attempt counter resets).

#### Scenario: Crash mid-run resumes from durable state

- **WHEN** a durable workflow operative has committed Entity state and journaled `proc_committed` entries for several completed procedures, then its runner crashes and it is respawned
- **THEN** it SHALL restore the committed state, skip the journaled procedures, and continue from the first uncommitted procedure seeded with the journaled handoff — not from the beginning

#### Scenario: Exhausted or non-retryable error is terminal

- **WHEN** a workflow operative's entity error is non-retryable or has exhausted the retry ladder
- **THEN** the operative SHALL compensate completed procedures and terminate with status `error`, and the platform SHALL NOT automatically re-run the operative

### Requirement: A finished workflow operative's data is retained for a window

On terminal teardown (status `ok | error | cancelled`), a durable workflow operative's data SHALL NOT be deleted immediately. The controller SHALL record a completion record (status, result/error payload, finished-at) alongside the retained final state and SHALL retain them for a configurable window — default **3 days** (`:workflow_completed_retention_hours = 72`, per-class override, env `FLEZHA_WORKFLOW_RETENTION_HOURS`) — after which the existing retention sweeper SHALL delete them. During the window, `flz operative info <id>` SHALL report the finished operative's terminal status, result/error, and finish time.

#### Scenario: Finished-job data survives until the window elapses

- **WHEN** a durable workflow operative terminates and the retention window has not elapsed
- **THEN** its final state and completion record SHALL remain on disk and `flz operative info <id>` SHALL report its terminal status and result

#### Scenario: Retention sweeper cleans up after the window

- **WHEN** a finished workflow operative's `finished_at` plus the retention window is in the past
- **THEN** the retention sweeper SHALL delete its retained state and completion record

