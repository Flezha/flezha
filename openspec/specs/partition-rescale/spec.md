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
`flz operative scale <group> M` SHALL proceed only after: (1) the members' source plugins are disabled, (2) every member's input-log watermark has reached its last offset, and (3) the group's DLQ trees are empty. The wait SHALL be bounded by a timeout; on expiry the command SHALL abort with nothing changed, naming the lagging member and cell (or the non-empty DLQ). Checkpoint-ack classes advance their watermark only at checkpoint close; if the quiesce cannot be reached for that reason, the refusal SHALL say so. For a workflow member the watermark is its journal's row, advanced at each iteration commit to the offset that pass consumed, and every message is in the log above the floor until its pass commits; condition (2) therefore holds exactly when no pass is in flight, no message is queued for the next pass and no parked member owes a tail, which is the only "quiet" a rescale accepts, read from the log with no call into the runner. A pass in flight SHALL be waited out under the same timeout and refused past it with nothing stopped or moved; the refusal SHALL say that a workflow member's watermark advances at each pass commit, so the wait rides the pass in flight. A member of a class that declares persistence off has no log and no journal: it is quiet at once, nothing durable moves for it, and a pass in flight on it is lost as any stop of an opted-out instance loses it.

#### Scenario: Quiesce reached, rescale proceeds
- **WHEN** sources are disabled and every member's watermark equals its last offset and the group DLQ is empty
- **THEN** the rescale proceeds to state movement

#### Scenario: Quiesce timeout aborts with a named laggard
- **WHEN** a member's watermark has not reached its last offset within the timeout
- **THEN** the command aborts, no member is stopped and no file is moved, and the error names the lagging member and cell

#### Scenario: Non-empty DLQ refuses the rescale
- **WHEN** any member has parked DLQ entries
- **THEN** the rescale is refused naming the member and entry count, directing the operator to replay or drop them first

#### Scenario: A workflow pass in flight is waited out
- **WHEN** `flz operative scale orders 3` is issued while `orders#p1` is mid-pass over the message at offset 12 and its journal's row stands at 11
- **THEN** the command waits, proceeds once iteration commits with consumed offset 12, and stops no member before that

#### Scenario: A parked workflow member is quiet
- **WHEN** every member of a `per_input` workflow group is parked with its log's last offset at or below its journal's row
- **THEN** the quiesce is reached at once and the rescale proceeds

#### Scenario: A pass that outlives the timeout refuses the rescale
- **WHEN** a member's pass has not committed within `--timeout`
- **THEN** the command aborts naming that member, that its watermark advances at each pass commit, and that nothing was stopped or moved

### Requirement: Rescale SHALL preflight the new member set before changing anything
Before stopping any member, the command SHALL recompute contiguous key-group ranges for M members and run the same preflights as a partitioned start: id budget for `<group>#p{M-1}` and `max_operatives` headroom for the net member delta.

#### Scenario: Preflight failure changes nothing
- **WHEN** the id budget or quota preflight for M fails
- **THEN** the rescale is refused and every existing member keeps running untouched

### Requirement: Every cell whose group changes owner SHALL move with its full durable file set
After all members are stopped, each cell whose key group maps to a different owner under the new ranges SHALL have its complete durable file set (WAL, snapshots, outbox dedup sidecar, compensation markers) moved into the new owner's state tree: a same-controller move SHALL be an atomic rename; a cross-controller move SHALL use the existing `WalTransfer` chunked pull (write-then-atomic-rename). Cell identity (class/tasklet/key) is unchanged by the move. A stop that preserves state for a move SHALL close the member's durable writers, its Cell writers, its journal writer and its input-log process, before any file moves, so a member restarted under the same id opens the file at its path rather than the descriptor of a file that was moved or removed. A member's instance-level files, its journal and its input log, SHALL move as one unit with its Cells under the member's own id: a surviving member on another controller node has both gathered to the node running the rescale by the same mover, a vacated member's go with its tree, and a new member has neither. A same-id move from another node SHALL be a real pull, the source removed only after the destination rename completes, never a no-op.

#### Scenario: Same-controller move
- **WHEN** a cell's new owner lives on the same controller node
- **THEN** its files are renamed into the new owner's tree and a subsequent restore serves the same state and seq

#### Scenario: Cross-controller move
- **WHEN** a cell's new owner lives on a different controller node
- **THEN** its files arrive via the WalTransfer pull with fsync-then-rename, and the source copy is removed only after the destination rename completes

#### Scenario: Crash mid-move resumes idempotently
- **WHEN** the controller crashes after some cells have moved and others have not
- **THEN** re-running `flz operative scale <group> M` re-derives the remaining moves from what is on disk and completes them, moving no cell twice

#### Scenario: A surviving workflow member's journal and log are gathered with its Cells
- **WHEN** `orders#p1` lives on controller B and the rescale runs on controller A
- **THEN** `orders#p1`'s Cells, its `task.journal` and its `input.log` arrive on A under `orders#p1`, B's copies are removed after each lands, and the restarted `orders#p1` resumes at its journal's iteration with its log's floor

#### Scenario: A same-id move from a peer is not a no-op
- **WHEN** a Cell whose owner does not change sits on another controller node at the rescale
- **THEN** it is pulled to the rescaling node under the same identity and the peer's copy is removed after the rename

#### Scenario: Writers are closed before the move
- **WHEN** a member is stopped for a rescale and then restarted under the same id after its files moved
- **THEN** its journal and Cell writers open the files at their new paths, and no record is written through a descriptor of the pre-move file

### Requirement: New members SHALL start only after movement completes, and vacated trees SHALL be cleaned up
The M members SHALL be started with the new ranges only after every owed move has completed; each restores its cells through the ordinary bind path (cold working-set rebuild). A surviving workflow member SHALL resume from its sealed journal at its next iteration, and whatever its input log retains above its floor SHALL be re-forwarded on that bind by the one re-forward every bind runs; a new workflow member SHALL start at iteration 0 with an empty log and no journal. Vacated instance trees — holding only a fully-consumed input log, and for a workflow its journal, after their cells moved out — SHALL be removed, and the registry rows of members beyond M SHALL be gone as soon as their stop commits: a stop removes the member from the hub's declared set and deletes its row, so a vacated member cannot be resurrected and no expiry window has to elapse first.

#### Scenario: Rescale 3 to 5 preserves every key's history
- **WHEN** a group is rescaled from 3 to 5 and inputs resume
- **THEN** every key's cell state continues from its pre-rescale seq on its new owner, coverage validation shows all 256 groups owned exactly once, and no key restarts from empty state

#### Scenario: Rescale 3 to 2 removes vacated members
- **WHEN** a group is rescaled from 3 to 2
- **THEN** `<group>#p2`'s cells are absorbed by the remaining members, its vacated state tree is removed, and its registry row is deleted with the stop rather than lingering until an expiry sweep

#### Scenario: A vacated member is not resurrected by a returning node
- **WHEN** a node that was absent for the whole rescale rejoins, however long it was away
- **THEN** the vacated member is absent from the declared set and is not restarted, because membership — not a timed tombstone — is what records the stop

#### Scenario: A workflow group rescaled 2 to 3 continues each key on its new owner
- **WHEN** a `per_input` workflow group with per-key counters is rescaled from 2 to 3 between passes and messages for the same keys resume
- **THEN** each key's next pass runs on the member that now owns it, its counter continues from its pre-rescale value, the two survivors commit their next pass as their next iteration, and `<group>#p2` commits its first pass as iteration 0

#### Scenario: A message that arrived after the quiesce check is not lost
- **WHEN** a message reaches a surviving member's log after the quiesce check and before its stop
- **THEN** the member's bind re-forwards it stamped with its offset; if its key still belongs to the member it runs a pass, and if the key moved the guard parks it under the entry channel where the dead-letter listing shows it

### Requirement: A rescale SHALL seal every surviving workflow member's history

A rescale SHALL be a history boundary for every surviving workflow member. A member's journal
records, per committed iteration, the Cells that ran and their outputs; a rewind compensates
those Cells and an all-or-nothing rollback compensates every journaled iteration; after a
rescale a Cell may belong to another member, so an undo over an earlier iteration would run on
the wrong instance. At its stop for the rescale, on the node it ran on and before its files move, each surviving member's journal SHALL be cut
to its resume point, keeping the latest committed iteration (its number, its consumed input-log
offset and its epoch), every entry whose outbox is still owed and the latest rewind marker with
its undo entries, whether or not the class declares compensable tasklets and whatever the
journal's size. The member SHALL resume at the next iteration with its input-log floor
unchanged; `flz operative rewind` SHALL refuse an iteration below the seal with the committed
range, as it refuses any uncommitted iteration; and an all-or-nothing rollback SHALL undo the
iterations committed since the seal and no earlier one. The scale command SHALL report the seal
for a workflow group. A moved Cell's compensation markers move with its files and SHALL drive no
compensation on their own: a resume compensates from intent markers only beside a done marker
or with no journal, and a quiet member has neither in flight.

#### Scenario: A rewind below the seal is refused

- **WHEN** `orders#p0` has committed iterations 1..6, the group is rescaled, and the operator
  runs `flz operative rewind orders#p0 --to-iteration 4`
- **THEN** the verb refuses with the committed range `6..6`, the journal is unchanged, and the
  member's next message runs as iteration 7

#### Scenario: A rollback after a rescale reaches the seal and stops

- **WHEN** an all-or-nothing `per_input` member sealed at iteration 6 commits iterations 7 and 8
  and iteration 9 fails terminally
- **THEN** compensation undoes iteration 9's completed tasklets, then 8 and 7 in reverse, and
  never touches iterations 1..6

#### Scenario: The seal keeps what a resume and an outbox re-drive need

- **WHEN** a member is sealed while a committed iteration's outbox output is still owed to a
  plugin
- **THEN** that entry survives the cut, the member re-drives it on its next bind, and the member
  resumes with the consumed offset and epoch of its latest committed iteration

