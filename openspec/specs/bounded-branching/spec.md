# bounded-branching Specification

## Purpose
Tree-shaped bounded operation chains: fan-out registration, per-channel handoff
routing, branch scheduling, the leaf-completion rule, reverse-topological
compensation, and subtree-precise crash-resume.

## Requirements
### Requirement: A bounded chain may fan out into a static tree

Registration SHALL accept a bounded operation topology shaped as a static
tree. An **edge** is an operation's output channel paired with one of that
channel's consumers, so an operation MAY feed several downstream operations by
declaring more than one consumer on the channel it emits on. Handoff routing
SHALL be per-channel: a downstream operation receives exactly the values
released on the channel it consumes.

An operation with more than one **incoming** channel (fan-in / join) SHALL be
rejected at registration with an error naming the node, as SHALL an operation
declaring more than one **outgoing** channel (the emit surface carries one
output channel per operation, so a second one could never be routed). A linear
chain SHALL remain valid as the single-branch case with unchanged behavior.

#### Scenario: Fan-out registers and routes per channel

- **WHEN** operation A emits on a channel declaring both B and C as consumers
- **THEN** registration SHALL succeed and both B and C SHALL receive the values A released on that channel

#### Scenario: Join is rejected

- **WHEN** a class declares operations B and C each feeding operation D on its own channel
- **THEN** registration SHALL fail with an error identifying D as a fan-in node

### Requirement: Branches run after their parent commits; the operative completes when all leaves commit

A child operation SHALL become runnable only after its parent operation's
commit is acknowledged; sibling branches SHALL be independent and MAY
interleave. Each operation SHALL keep the existing per-op barrier, commit
gating, staged outbox, and segment-retry semantics unchanged. The operative
SHALL terminate with status `ok` exactly when every **leaf** operation has
committed.

#### Scenario: Parent commit releases both branches

- **WHEN** operation A commits in an A→(B,C) tree
- **THEN** B and C SHALL both become runnable, each proceeding through its own barrier and commit independently

#### Scenario: DONE requires every leaf

- **WHEN** leaf B has committed but leaf C has not
- **THEN** the operative SHALL NOT be complete; it SHALL complete only once C also commits

### Requirement: Compensation over a tree is reverse topological

On abnormal termination, completed operations SHALL be compensated in reverse
topological order: an operation SHALL NOT be compensated before every
compensated descendant of it has been compensated; the relative order among
independent sibling subtrees is unspecified. For a linear chain this SHALL
degenerate to the existing exact reverse order. Per-segment compensation
semantics and the intent/done marker machinery SHALL be unchanged.

#### Scenario: Parent is undone last

- **WHEN** an A→(B,C) tree fails after A, B, and C completed
- **THEN** A SHALL be compensated only after both B and C have been compensated

#### Scenario: Linear chain order is preserved

- **WHEN** a linear chain A→B→C fails after all three completed
- **THEN** compensation SHALL run C, then B, then A — identical to pre-change behavior

### Requirement: Crash-resume re-runs exactly the uncommitted subtrees

On respawn, the driver SHALL derive committed nodes from the journal and SHALL
schedule only the uncommitted subtrees, seeding each subtree root from its
parent's journaled per-edge handoff. Committed branches SHALL NOT re-run.

#### Scenario: One committed branch survives a crash

- **WHEN** in an A→(B,C) tree the journal holds `op_committed` for A and B when the executor crashes
- **THEN** on respawn only C (seeded from A's journaled handoff on the channel C consumes) SHALL run; A and B SHALL NOT re-run

### Requirement: A handoff-chained loop requires a single terminal operation

A class declaring a handoff-chained loop over a branching tree SHALL be
rejected at registration: a chained loop seeds iteration N+1 from iteration N's
terminal batch, which a tree with several leaves does not define. Classes whose
loop is per-input, or which do not loop at all, MAY branch freely.

#### Scenario: A branching chained loop is refused

- **WHEN** a class declares `LoopMode::Chained` over an A→(B,C) tree
- **THEN** registration SHALL fail with an error naming the branching chained loop
