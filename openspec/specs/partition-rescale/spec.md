# partition-rescale Specification

## Purpose
Change a partition group's member count without losing a key's history. A
rescale quiesces the group (sources off, input logs consumed, DLQs empty),
recomputes contiguous key-group ranges, moves every Cell whose owner changes
into its new member's state tree, and only then starts the new member set. Any
refusal leaves the group exactly as it was, and a crash mid-move is completed
by re-running the command.

## Requirements

### Requirement: `flz operative scale` SHALL rescale a group only under an explicit quiesce
`flz operative scale <group> M` SHALL proceed only after: (1) the members' source plugins are disabled, (2) every member's input-log watermark has reached its last offset, and (3) the group's DLQ trees are empty. The wait SHALL be bounded by a timeout; on expiry the command SHALL abort with nothing changed, naming the lagging member and cell (or the non-empty DLQ). Checkpoint-ack classes advance their watermark only at checkpoint close; if the quiesce cannot be reached for that reason, the refusal SHALL say so.

#### Scenario: Quiesce reached, rescale proceeds
- **WHEN** sources are disabled and every member's watermark equals its last offset and the group DLQ is empty
- **THEN** the rescale proceeds to state movement

#### Scenario: Quiesce timeout aborts with a named laggard
- **WHEN** a member's watermark has not reached its last offset within the timeout
- **THEN** the command aborts, no member is stopped and no file is moved, and the error names the lagging member and cell

#### Scenario: Non-empty DLQ refuses the rescale
- **WHEN** any member has parked DLQ entries
- **THEN** the rescale is refused naming the member and entry count, directing the operator to replay or drop them first

### Requirement: Rescale SHALL preflight the new member set before changing anything
Before stopping any member, the command SHALL recompute contiguous key-group ranges for M members and run the same preflights as a partitioned start: id budget for `<group>#p{M-1}` and `max_operatives` headroom for the net member delta.

#### Scenario: Preflight failure changes nothing
- **WHEN** the id budget or quota preflight for M fails
- **THEN** the rescale is refused and every existing member keeps running untouched

### Requirement: Every cell whose group changes owner SHALL move with its full durable file set
After all members are stopped, each cell whose key group maps to a different owner under the new ranges SHALL have its complete durable file set (WAL, snapshots, outbox dedup sidecar, compensation markers) moved into the new owner's state tree: a same-controller move SHALL be an atomic rename; a cross-controller move SHALL use the existing `WalTransfer` chunked pull (write-then-atomic-rename). Cell identity (class/tasklet/key) is unchanged by the move.

#### Scenario: Same-controller move
- **WHEN** a cell's new owner lives on the same controller node
- **THEN** its files are renamed into the new owner's tree and a subsequent restore serves the same state and seq

#### Scenario: Cross-controller move
- **WHEN** a cell's new owner lives on a different controller node
- **THEN** its files arrive via the WalTransfer pull with fsync-then-rename, and the source copy is removed only after the destination rename completes

#### Scenario: Crash mid-move resumes idempotently
- **WHEN** the controller crashes after some cells have moved and others have not
- **THEN** re-running `flz operative scale <group> M` re-derives the remaining moves from what is on disk and completes them, moving no cell twice

### Requirement: New members SHALL start only after movement completes, and vacated trees SHALL be cleaned up
The M members SHALL be started with the new ranges only after every owed move has completed; each restores its cells through the ordinary bind path (cold working-set rebuild). Vacated instance trees — holding only a fully-consumed input log after their cells moved out — SHALL be removed, and the registry rows of members beyond M SHALL be gone as soon as their stop commits: a stop removes the member from the hub's declared set and deletes its row, so a vacated member cannot be resurrected and no expiry window has to elapse first.

#### Scenario: Rescale 3 to 5 preserves every key's history
- **WHEN** a group is rescaled from 3 to 5 and inputs resume
- **THEN** every key's cell state continues from its pre-rescale seq on its new owner, coverage validation shows all 256 groups owned exactly once, and no key restarts from empty state

#### Scenario: Rescale 3 to 2 removes vacated members
- **WHEN** a group is rescaled from 3 to 2
- **THEN** `<group>#p2`'s cells are absorbed by the remaining members, its vacated state tree is removed, and its registry row is deleted with the stop rather than lingering until an expiry sweep

#### Scenario: A vacated member is not resurrected by a returning node
- **WHEN** a node that was absent for the whole rescale rejoins, however long it was away
- **THEN** the vacated member is absent from the declared set and is not restarted, because membership — not a timed tombstone — is what records the stop
