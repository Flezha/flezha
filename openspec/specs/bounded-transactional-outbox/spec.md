# bounded-transactional-outbox Specification

## Purpose

The bounded-transactional-outbox capability makes a durable structured bounded operative's plugin-bound output transactional with its op progress. Plugin-bound emits inside a structured operation are staged per segment instead of routed straight to the wire; at the operation barrier they travel to the controller inside the `OpCommit` request and are persisted as outbox entries in the same `op_committed` journal entry (one durable write + fsync) that records the operation's completion. Only after that durable point are the entries flushed to the unit's bound plugins, stamped with the deterministic key `(segment_key, op_idx, seq_in_op, epoch)` — aligned with the effectively-once `(seg, N, epoch)` output-identity shape — and an `outbox_flushed` entry is appended. A crash between commit and flush re-drives the outbox on restore with the original keys; a failed, aborted, or cancelled operation leaks nothing. Compensation undo-effects ride the same journal-gated path via the `compensated` entry type, with comp-marked keys that can never collide with the forward output they reverse. The `run`-sugar form (no operations/barriers) keeps the direct at-least-once emit path.

## Requirements

### Requirement: Plugin-bound output of a structured operation is staged until the op's durable commit

Plugin-bound output emitted inside a structured bounded operation SHALL be
buffered per segment and SHALL NOT reach the wire, plugins, or sinks before the
producing operation's `op_committed` journal entry is durable. The staged
outputs SHALL travel to the controller inside the `OpCommit` barrier request. A
failed, aborted, or cancelled operation SHALL leak none of its staged output.
The `run`-sugar form (no operations/barriers) SHALL keep the direct
at-least-once emit path.

#### Scenario: Mid-op emit is not visible before commit

- **WHEN** a segment of operation B emits plugin-bound output and operation B has not yet committed
- **THEN** no plugin SHALL have received that output

#### Scenario: Failed operation leaks nothing

- **WHEN** operation B's segments emitted output and another B segment then fails terminally (op does not commit)
- **THEN** none of B's staged output SHALL ever be delivered to a plugin or sink

#### Scenario: run-sugar keeps the direct path

- **WHEN** a `run`-sugar bounded operative calls `ctx.emit`
- **THEN** the output SHALL be routed to the wire immediately (at-least-once), as today

### Requirement: Outbox entries are part of the single durable op-commit write

The controller SHALL persist an operation's staged outputs as outbox entries
inside the same `op_committed` journal entry (one durable write + fsync) that
records the operation's completion — outbox and progress SHALL NOT be able to
tear apart. Only after that durable point SHALL the controller flush the
entries to the unit's bound plugins, and after successful delivery it SHALL
append an `outbox_flushed` journal entry for that operation.

#### Scenario: One atomic durable write at the boundary

- **WHEN** operation B commits with staged outputs
- **THEN** the `op_committed(B)` journal entry SHALL contain B's outbox entries and SHALL be fsynced before any of them is delivered

#### Scenario: Flush is recorded

- **WHEN** all of operation B's outbox entries have been delivered to the bound plugins
- **THEN** an `outbox_flushed(B)` journal entry SHALL be appended

### Requirement: Every flushed output carries a deterministic dedup key

Each flushed outbox entry SHALL be stamped with the deterministic key
`(segment_key, op_idx, seq_in_op, epoch)` as frame metadata, where `epoch` is a
per-instance journal-held value (0 at first start) that is stable across crash
re-drives and reserved for rewind to bump. The stamp format SHALL be aligned
with the effectively-once `(seg, N, epoch)` output-identity shape so keyed sink
adapters apply unchanged. Without a dedup-capable sink, delivery SHALL be
at-least-once with keys present (documented).

#### Scenario: Re-drive produces identical keys

- **WHEN** operation B's outbox is flushed, and later re-driven after a crash
- **THEN** every re-driven output SHALL carry a key identical to its original delivery

#### Scenario: Epoch is stable across respawn

- **WHEN** an instance crashes and resumes several times without a rewind
- **THEN** all its flushed outputs SHALL carry the same epoch value

### Requirement: Crash before flush re-drives the outbox

On instance restore, the controller SHALL re-flush every `op_committed` entry's
outbox that has no matching `outbox_flushed` entry, with the original keys,
before reporting restore complete for the outbox. Operations whose
`outbox_flushed` entry exists SHALL NOT be re-flushed. A plugin-delivery
failure SHALL leave `outbox_flushed` unwritten and SHALL be retried — outbox
entries are never silently dropped.

#### Scenario: Crash between commit and flush loses nothing

- **WHEN** the controller crashes after journaling `op_committed(B)` but before delivering B's outbox
- **THEN** on restore B's outbox SHALL be delivered with the original keys

#### Scenario: Already-flushed op is not re-delivered by restore

- **WHEN** the journal holds `op_committed(A)` + `outbox_flushed(A)` and the instance restores
- **THEN** the restore path SHALL NOT re-deliver A's outbox

### Requirement: Compensation undo-effects ride the transactional outbox

Plugin-bound output emitted by `compensate` hooks SHALL be staged per segment
and delivered through the same journal-gated outbox path: persisted in the
`compensated` journal entry, flushed only after that entry is durable, and
stamped with the forward key tuple plus a compensation marker so an undo output
can never collide with the forward output it reverses.

#### Scenario: Undo output is keyed and durable before delivery

- **WHEN** operation B is compensated and its hook emits an undo record
- **THEN** the undo record SHALL be journaled in the `compensated` entry before delivery and SHALL carry a comp-marked key distinct from B's forward keys

#### Scenario: Crash before undo flush re-drives it

- **WHEN** the controller crashes after journaling a `compensated` entry but before delivering its undo outputs
- **THEN** on restore the undo outputs SHALL be delivered with the original comp-marked keys
