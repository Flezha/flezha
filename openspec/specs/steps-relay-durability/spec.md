# steps-relay-durability Specification

## Purpose
TBD - created by archiving change steps-relay-functionality. Update Purpose after archive.
## Requirements
### Requirement: WAL persistence for a steps relay is durable by default, opt-out per class

A steps relay class SHALL be **durable by default**; an author MAY **opt out** per class (fire-and-forget) through the same class setting a flow class carries, `durable: false`, announced by the runner in the `AddRelay` response and recorded by the controller with the class's other declarations, with no configuration override. When durable, its tasklets/cells SHALL be WAL-backed on the **same substrate** (WalWriter framing, snapshots, fsync batching, restore) and the **same `{relay_class, tasklet_id, cell_key}` identity** as a flow relay's, with no parallel persistence machinery, and each invocation SHALL be one effect record carrying its state changes and its plugin outputs together, the record a flow Cell on the effectively-once tier writes; its outputs SHALL leave through that Cell's outbox once the stage naming the record has committed. An opted-out class SHALL produce no WAL, snapshot, journal or input log on the controller, SHALL ship no state frames from the runner, SHALL be given no restore on a rebind, and SHALL NOT sleep, having no `sleep_after` to declare, and a `once` or `per_input` instance of it waits in memory for its message or a stop. An opted-out class SHALL send its plugin outputs straight to the wire with their channel, key and metadata, as an opted-out flow relay does.

#### Scenario: Durable steps relay is WAL-backed by default

- **WHEN** a steps relay class does not opt out of persistence and writes Cell state
- **THEN** its Cell state changes SHALL be appended to the WAL on the shared substrate exactly as a flow relay's are, inside effect records that also carry the invocation's outputs

#### Scenario: Opted-out steps relay writes nothing to disk

- **WHEN** a steps relay class opts out of persistence and runs to completion
- **THEN** no WAL, snapshot, journal, input log or state files SHALL be created for it, in any SDK

#### Scenario: The opt-out is honoured on both sides

- **WHEN** a steps class opts out of persistence in Java, Node, Python, Ruby, PHP, Go, Rust or Elixir and an instance of it is rebound after its runner died
- **THEN** the runner SHALL have shipped no `cell_state_change` frame for it, the controller SHALL ship no `RestoreInitState` and no `task_journal`, and every tasklet SHALL re-run from the top

#### Scenario: An opted-out steps relay's output keeps its identity

- **WHEN** a steps relay class that opted out of persistence emits with a key and metadata on a keyed output
- **THEN** the output receives the frame with `channel`, `flz.key` and the metadata, exactly as from an opted-out flow relay

### Requirement: A durable steps relay resumes from the WAL on crash, at-least-once

When a durable steps relay's runner or controller restarts mid-run, the relay SHALL be respawned and its Tasklet/Cell state restored from the WAL via the bind-time restore path, then execution SHALL **resume at the current tasklet/iteration** rather than restarting from the beginning; the controller-side **task-journal** decides redo-vs-skip — tasklets recorded `task_committed` SHALL be skipped and the first uncommitted tasklet SHALL be seeded from the journaled handoff batch. The Cells restored SHALL be the persisted Cell set the controller enumerates on disk for the instance, a Cell a keyed edge created included: the controller SHALL count every such Cell into `AddRelay.resume_cell_count`, and the runner SHALL materialise a keyed Cell the restore names before folding its state into it, so the resumed pass finds it as the crashed one left it. The restore gate SHALL count restore frames, not declared Cells: a frame that finds no Cell, or names a keyed Cell the tasklet's cap refuses, SHALL count as landed, so the gate never waits on it. A journaled `task_committed` entry SHALL be read back onto the Cells it names by identity, a keyed identity being resolved on the instance the same way a restore resolves it. The bind SHALL re-derive, on each Cell writer, which records are committed from the ranges the live `task_committed` and `compensated` entries name for that Cell, so the outputs of a committed stage that a crash left undelivered are re-driven and the outputs of an uncommitted one are dropped; the bind SHALL start the writer of every persisted Cell it restores, since a Cell the resumed run never writes to again still owes what its committed records hold. An instance with no journal (opted-out class, or created before the journal existed) SHALL re-run from the top. Execution SHALL therefore be **at-least-once** — a step whose completion was not journaled before the crash MAY run again on resume — so steps and compensations SHALL be idempotent by author contract; a re-run's records are newer than the interrupted attempt's and supersede its outputs. A cell error inside a structured tasklet SHALL first pass through the per-cell retry ladder (`steps-cell-retry`); an error that is non-retryable or has exhausted the ladder SHALL be terminal — the relay compensates and terminates with status `error`, with no further platform re-run of the relay (author loops own their own semantic retry); this is distinct from crash-resume. A crash during a retry ladder SHALL converge on normal crash-resume of the uncommitted tasklet (the in-memory attempt counter resets).

#### Scenario: Crash mid-run resumes from durable state

- **WHEN** a durable steps relay has committed Cell state and journaled `task_committed` entries for several completed tasklets, then its runner crashes and it is respawned
- **THEN** it SHALL restore the committed state, skip the journaled tasklets, and continue from the first uncommitted tasklet seeded with the journaled handoff — not from the beginning

#### Scenario: Crash mid-run restores the keyed Cells

- **WHEN** a durable steps relay whose keyed tasklet committed state for keys `a`, `b` and `c` crashes while a later tasklet blocks, and it is respawned
- **THEN** `resume_cell_count` counts the three keyed Cells with the rest, three restore frames materialise and fold them before the first tasklet of the resumed pass runs, each keyed Cell's WAL holds exactly its pre-crash records afterwards, and the instance completes

#### Scenario: A refused restore does not hold the gate

- **WHEN** a resumed instance's keyed tasklet declares a cap below the number of keyed Cells the controller ships
- **THEN** the frames past the cap are acknowledged without a Cell, the gate opens once every shipped frame has landed, and the pass resumes rather than waiting for the resume timeout

#### Scenario: A crash between a record and its marker re-runs the stage over its own record

- **WHEN** a durable steps relay Cell's effect record for tasklet B is durable, its `task_committed` entry is not, and the instance is respawned
- **THEN** B re-runs over the restored state, its new record supersedes the interrupted one, the marker names the new record, and only the new record's output reaches the output

#### Scenario: Exhausted or non-retryable error is terminal

- **WHEN** a steps relay's cell error is non-retryable or has exhausted the retry ladder
- **THEN** the relay SHALL compensate completed tasklets and terminate with status `error`, and the platform SHALL NOT automatically re-run the relay

### Requirement: A finished steps relay's data is retained for a window

On terminal teardown (status `ok | error | cancelled`), a durable steps relay's data SHALL NOT be deleted immediately. The controller SHALL record a completion record (status, result/error payload, finished-at) alongside the retained final state and SHALL retain them for a configurable window — default **3 days** (`:default_relay_completed_retention_hours = 72`, env `FLEZHA_STEPS_RETENTION_HOURS`, or the value the start's instance metadata carries; no per-class map) — after which the existing retention sweeper SHALL delete them. During the window, `flz relay info <id>` SHALL report the finished relay's terminal status, result/error, and finish time.

#### Scenario: Finished-job data survives until the window elapses

- **WHEN** a durable steps relay terminates and the retention window has not elapsed
- **THEN** its final state and completion record SHALL remain on disk and `flz relay info <id>` SHALL report its terminal status and result

#### Scenario: Retention sweeper cleans up after the window

- **WHEN** a finished steps relay's `finished_at` plus the retention window is in the past
- **THEN** the retention sweeper SHALL delete its retained state and completion record

### Requirement: A restore gate that times out SHALL fail the run

A resumed steps run SHALL NOT start a pass while any restore its bind announced
(`resume_cell_count` Cell restores and `effect_result_count` effect results) has not landed. When
the restore gate's bound (30 s) passes first, the run SHALL end with status `error` and an error
naming how many of the announced restores landed; it SHALL run no tasklet and no compensation,
since neither may read Cells whose state never arrived. The controller SHALL treat it as every
run error: no automatic re-run, the outcome recorded, the instance's state, journal and input log
retained. A stop that arrives while the gate is closed SHALL end the run with status
`cancelled`, SHALL NOT open the gate, and SHALL run no tasklet and no compensation.

#### Scenario: A lost restore ends the run instead of running partially restored

- **WHEN** a durable steps run is rebound with `resume_cell_count` 2 and only one restore lands within the gate's bound
- **THEN** the run SHALL end `error` naming 1 of 2 restores, no tasklet SHALL have run in that bind, and the message the controller holds for the instance SHALL remain in its input log

#### Scenario: A stop before the restores land cancels without opening the gate

- **WHEN** a resumed steps run is stopped (externally, or by a `max_duration` already spent at bind) while one of its two announced restores has not landed
- **THEN** the run SHALL end `cancelled`, no tasklet and no compensation hook SHALL have run in that bind, and its journal and state SHALL be retained

