# workflow-transactional-outbox Specification

## Purpose

The workflow-transactional-outbox capability makes a durable structured workflow operative's plugin-bound output transactional with its task progress. Plugin-bound emits inside a structured tasklet are staged per cell instead of routed straight to the wire; at the tasklet barrier they travel to the controller inside the `TaskCommit` request and are persisted as outbox entries in the same `task_committed` journal entry (one durable write + fsync) that records the tasklet's completion. Only after that durable point are the entries flushed to the unit's bound plugins, stamped with the deterministic key `(cell_key, task_idx, seq_in_task, epoch)` — aligned with the effectively-once `(seg, N, epoch)` output-identity shape — and an `outbox_flushed` entry is appended. A crash between commit and flush re-drives the outbox on restore with the original keys; a failed, aborted, or cancelled tasklet leaks nothing. Compensation undo-effects ride the same journal-gated path via the `compensated` entry type, with comp-marked keys that can never collide with the forward output they reverse. An opted-out class, which journals nothing, keeps the direct at-least-once emit path.
## Requirements
### Requirement: Plugin-bound output of a structured tasklet is staged until the task's durable commit

Plugin-bound output emitted inside a structured workflow tasklet SHALL be
buffered per cell and SHALL NOT reach the wire, plugins, or sinks before the
producing tasklet's `task_committed` journal entry is durable. The staged
outputs SHALL travel to the controller inside the `TaskCommit` barrier request. A
failed, aborted, or cancelled tasklet SHALL leak none of its staged output.
Every workflow's output SHALL take this path: no workflow form emits straight
to the wire from a durable class.

#### Scenario: Mid-task emit is not visible before commit

- **WHEN** a cell of tasklet B emits plugin-bound output and tasklet B has not yet committed
- **THEN** no plugin SHALL have received that output

#### Scenario: Failed tasklet leaks nothing

- **WHEN** tasklet B's cells emitted output and another B cell then fails terminally (task does not commit)
- **THEN** none of B's staged output SHALL ever be delivered to a plugin or sink

#### Scenario: A completing tasklet's output is still staged

- **WHEN** a tasklet emits plugin-bound output and calls `ctx.complete(result)` in the same run
- **THEN** its output SHALL be staged and flushed after its commit like any other, and the instance SHALL end only after that commit

### Requirement: Outbox entries are part of the single durable task-commit write

The controller SHALL persist a tasklet's staged outputs as outbox entries
inside the same `task_committed` journal entry (one durable write + fsync) that
records the tasklet's completion — outbox and progress SHALL NOT be able to
tear apart. Only after that durable point SHALL the controller flush the
entries to the unit's bound plugins, and after successful delivery it SHALL
append an `outbox_flushed` journal entry for that tasklet.

#### Scenario: One atomic durable write at the boundary

- **WHEN** tasklet B commits with staged outputs
- **THEN** the `task_committed(B)` journal entry SHALL contain B's outbox entries and SHALL be fsynced before any of them is delivered

#### Scenario: Flush is recorded

- **WHEN** all of tasklet B's outbox entries have been delivered to the bound plugins
- **THEN** an `outbox_flushed(B)` journal entry SHALL be appended

### Requirement: Every flushed output carries a deterministic dedup key

Each flushed outbox entry SHALL be stamped with the deterministic key
`(cell_key, task_idx, seq_in_task, epoch)` as frame metadata, where `epoch` is a
per-instance journal-held value (0 at first start) that is stable across crash
re-drives and reserved for rewind to bump. The stamp format SHALL be aligned
with the effectively-once `(seg, N, epoch)` output-identity shape so keyed sink
adapters apply unchanged. Without a dedup-capable sink, delivery SHALL be
at-least-once with keys present (documented).

#### Scenario: Re-drive produces identical keys

- **WHEN** tasklet B's outbox is flushed, and later re-driven after a crash
- **THEN** every re-driven output SHALL carry a key identical to its original delivery

#### Scenario: Epoch is stable across respawn

- **WHEN** an instance crashes and resumes several times without a rewind
- **THEN** all its flushed outputs SHALL carry the same epoch value

### Requirement: Crash before flush re-drives the outbox

On instance restore, the controller SHALL re-flush every `task_committed`
entry's outbox that has no matching `outbox_flushed` entry, with the original
keys, before reporting restore complete for the outbox. The `outbox_flushed`
marker SHALL be journaled only after the external sink confirms the delivery;
an in-memory hand-off to the operative's `:on_output` chain SHALL NOT be
treated as delivery. Tasklets whose `outbox_flushed` entry exists SHALL NOT
be re-flushed. A plugin-delivery failure or missing confirmation SHALL leave
`outbox_flushed` unwritten and SHALL be retried — outbox entries are never
silently dropped.

#### Scenario: Crash between commit and flush loses nothing

- **WHEN** the controller crashes after journaling `task_committed(B)` but before delivering B's outbox
- **THEN** on restore B's outbox SHALL be delivered with the original keys

#### Scenario: Dispatched but unconfirmed output re-drives

- **WHEN** the controller crashes after casting B's outbox into the plugin
  chain but before the sink confirms it
- **THEN** the journal holds no `outbox_flushed(B)` and restore re-delivers
  B's outbox with the original keys; a conforming sink deduplicates

#### Scenario: Already-confirmed task is not re-delivered by restore

- **WHEN** the journal holds `task_committed(A)` + `outbox_flushed(A)` —
  written on confirmed delivery — and the instance restores
- **THEN** the restore path SHALL NOT re-deliver A's outbox

### Requirement: Compensation undo-effects ride the transactional outbox

Plugin-bound output emitted by `compensate` hooks SHALL be staged per cell
and delivered through the same journal-gated outbox path: persisted in the
`compensated` journal entry, flushed only after that entry is durable, and
stamped with the forward key tuple plus a compensation marker so an undo output
can never collide with the forward output it reverses.

#### Scenario: Undo output is keyed and durable before delivery

- **WHEN** tasklet B is compensated and its hook emits an undo record
- **THEN** the undo record SHALL be journaled in the `compensated` entry before delivery and SHALL carry a comp-marked key distinct from B's forward keys

#### Scenario: Crash before undo flush re-drives it

- **WHEN** the controller crashes after journaling a `compensated` entry but before delivering its undo outputs
- **THEN** on restore the undo outputs SHALL be delivered with the original comp-marked keys

