# workflow-transactional-outbox Specification

## Purpose

The workflow-transactional-outbox capability makes a durable structured workflow operative's plugin-bound output transactional with its proc progress. Plugin-bound emits inside a structured procedure are staged per entity instead of routed straight to the wire; at the procedure barrier they travel to the controller inside the `ProcCommit` request and are persisted as outbox entries in the same `proc_committed` journal entry (one durable write + fsync) that records the procedure's completion. Only after that durable point are the entries flushed to the unit's bound plugins, stamped with the deterministic key `(entity_key, proc_idx, seq_in_proc, epoch)` — aligned with the effectively-once `(seg, N, epoch)` output-identity shape — and an `outbox_flushed` entry is appended. A crash between commit and flush re-drives the outbox on restore with the original keys; a failed, aborted, or cancelled procedure leaks nothing. Compensation undo-effects ride the same journal-gated path via the `compensated` entry type, with comp-marked keys that can never collide with the forward output they reverse. The `run`-sugar form (no procedures/barriers) keeps the direct at-least-once emit path.

## Requirements

### Requirement: Plugin-bound output of a structured procedure is staged until the proc's durable commit

Plugin-bound output emitted inside a structured workflow procedure SHALL be
buffered per entity and SHALL NOT reach the wire, plugins, or sinks before the
producing procedure's `proc_committed` journal entry is durable. The staged
outputs SHALL travel to the controller inside the `ProcCommit` barrier request. A
failed, aborted, or cancelled procedure SHALL leak none of its staged output.
The `run`-sugar form (no procedures/barriers) SHALL keep the direct
at-least-once emit path.

#### Scenario: Mid-proc emit is not visible before commit

- **WHEN** an entity of procedure B emits plugin-bound output and procedure B has not yet committed
- **THEN** no plugin SHALL have received that output

#### Scenario: Failed procedure leaks nothing

- **WHEN** procedure B's entities emitted output and another B entity then fails terminally (proc does not commit)
- **THEN** none of B's staged output SHALL ever be delivered to a plugin or sink

#### Scenario: run-sugar keeps the direct path

- **WHEN** a `run`-sugar workflow operative calls `ctx.emit`
- **THEN** the output SHALL be routed to the wire immediately (at-least-once), as today

### Requirement: Outbox entries are part of the single durable proc-commit write

The controller SHALL persist a procedure's staged outputs as outbox entries
inside the same `proc_committed` journal entry (one durable write + fsync) that
records the procedure's completion — outbox and progress SHALL NOT be able to
tear apart. Only after that durable point SHALL the controller flush the
entries to the unit's bound plugins, and after successful delivery it SHALL
append an `outbox_flushed` journal entry for that procedure.

#### Scenario: One atomic durable write at the boundary

- **WHEN** procedure B commits with staged outputs
- **THEN** the `proc_committed(B)` journal entry SHALL contain B's outbox entries and SHALL be fsynced before any of them is delivered

#### Scenario: Flush is recorded

- **WHEN** all of procedure B's outbox entries have been delivered to the bound plugins
- **THEN** an `outbox_flushed(B)` journal entry SHALL be appended

### Requirement: Every flushed output carries a deterministic dedup key

Each flushed outbox entry SHALL be stamped with the deterministic key
`(entity_key, proc_idx, seq_in_proc, epoch)` as frame metadata, where `epoch` is a
per-instance journal-held value (0 at first start) that is stable across crash
re-drives and reserved for rewind to bump. The stamp format SHALL be aligned
with the effectively-once `(seg, N, epoch)` output-identity shape so keyed sink
adapters apply unchanged. Without a dedup-capable sink, delivery SHALL be
at-least-once with keys present (documented).

#### Scenario: Re-drive produces identical keys

- **WHEN** procedure B's outbox is flushed, and later re-driven after a crash
- **THEN** every re-driven output SHALL carry a key identical to its original delivery

#### Scenario: Epoch is stable across respawn

- **WHEN** an instance crashes and resumes several times without a rewind
- **THEN** all its flushed outputs SHALL carry the same epoch value

### Requirement: Crash before flush re-drives the outbox

On instance restore, the controller SHALL re-flush every `proc_committed` entry's
outbox that has no matching `outbox_flushed` entry, with the original keys,
before reporting restore complete for the outbox. Procedures whose
`outbox_flushed` entry exists SHALL NOT be re-flushed. A plugin-delivery
failure SHALL leave `outbox_flushed` unwritten and SHALL be retried — outbox
entries are never silently dropped.

#### Scenario: Crash between commit and flush loses nothing

- **WHEN** the controller crashes after journaling `proc_committed(B)` but before delivering B's outbox
- **THEN** on restore B's outbox SHALL be delivered with the original keys

#### Scenario: Already-flushed proc is not re-delivered by restore

- **WHEN** the journal holds `proc_committed(A)` + `outbox_flushed(A)` and the instance restores
- **THEN** the restore path SHALL NOT re-deliver A's outbox

### Requirement: Compensation undo-effects ride the transactional outbox

Plugin-bound output emitted by `compensate` hooks SHALL be staged per entity
and delivered through the same journal-gated outbox path: persisted in the
`compensated` journal entry, flushed only after that entry is durable, and
stamped with the forward key tuple plus a compensation marker so an undo output
can never collide with the forward output it reverses.

#### Scenario: Undo output is keyed and durable before delivery

- **WHEN** procedure B is compensated and its hook emits an undo record
- **THEN** the undo record SHALL be journaled in the `compensated` entry before delivery and SHALL carry a comp-marked key distinct from B's forward keys

#### Scenario: Crash before undo flush re-drives it

- **WHEN** the controller crashes after journaling a `compensated` entry but before delivering its undo outputs
- **THEN** on restore the undo outputs SHALL be delivered with the original comp-marked keys
