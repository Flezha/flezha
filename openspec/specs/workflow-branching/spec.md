# workflow-branching Specification

## Purpose
Tree-shaped workflow tasklet chains: fan-out registration, per-channel handoff
routing, branch scheduling, the leaf-completion rule, reverse-topological
compensation, and subtree-precise crash-resume.
## Requirements
### Requirement: Branches run after their parent commits; the operative completes when all leaves commit

A child tasklet SHALL become runnable only after **every** parent tasklet
feeding it has had its commit acknowledged — for a single-parent child this is
the existing rule unchanged; a join node waits for all of its producers.
Sibling branches SHALL be independent and MAY interleave. Each tasklet SHALL
keep the existing per-task barrier, commit gating, staged outbox, and
cell-retry semantics unchanged. The operative SHALL terminate with status
`ok` exactly when every **leaf** tasklet has committed.

#### Scenario: Parent commit releases both branches

- **WHEN** tasklet A commits in an A→(B,C) tree
- **THEN** B and C SHALL both become runnable, each proceeding through its own barrier and commit independently

#### Scenario: A join waits for every producer

- **WHEN** in an A→(B,C)→D graph, B has committed but C has not
- **THEN** D SHALL NOT be dispatched; it SHALL become runnable only once C's commit is also acknowledged, and SHALL then run exactly once

#### Scenario: DONE requires every leaf

- **WHEN** leaf B has committed but leaf C has not
- **THEN** the operative SHALL NOT be complete; it SHALL complete only once C also commits

### Requirement: Compensation over a tree is reverse topological

On abnormal termination, completed tasklets SHALL be compensated in reverse
topological order over the DAG: a tasklet SHALL NOT be compensated before
every compensated descendant of it has been compensated — in particular a join
node SHALL be compensated before any of its producers — and the relative order
among independent sibling subtrees is unspecified. For a linear chain this
SHALL degenerate to the existing exact reverse order. Per-cell compensation
semantics and the intent/done marker machinery SHALL be unchanged.

#### Scenario: Parent is undone last

- **WHEN** an A→(B,C) tree fails after A, B, and C completed
- **THEN** A SHALL be compensated only after both B and C have been compensated

#### Scenario: A join is undone before its producers

- **WHEN** an A→(B,C)→D graph fails after all four completed
- **THEN** D SHALL be compensated before either B or C, and A SHALL be compensated last

#### Scenario: Linear chain order is preserved

- **WHEN** a linear chain A→B→C fails after all three completed
- **THEN** compensation SHALL run C, then B, then A — identical to pre-change behavior

### Requirement: Crash-resume re-runs exactly the uncommitted subtrees

On respawn, the driver SHALL derive committed nodes from the journal and SHALL
schedule only the uncommitted subgraphs, seeding each uncommitted node from
its parents' journaled per-edge handoffs. A join node with one committed and
one uncommitted producer SHALL run only after the uncommitted producer
completes on the resumed run, with an input batch accumulating the committed
producer's **journaled** handoff and the resumed producer's fresh output.
Committed branches SHALL NOT re-run.

#### Scenario: One committed branch survives a crash

- **WHEN** in an A→(B,C) tree the journal holds `task_committed` for A and B when the runner crashes
- **THEN** on respawn only C (seeded from A's journaled handoff on the channel C consumes) SHALL run; A and B SHALL NOT re-run

#### Scenario: A join resumes with one journaled and one fresh contribution

- **WHEN** in an A→(B,C)→D graph the journal holds `task_committed` for A and B when the runner crashes
- **THEN** on respawn C SHALL re-run and D SHALL then run exactly once with an input batch containing B's journaled handoff and C's fresh output; B SHALL NOT re-run

### Requirement: A handoff-chained loop requires a single terminal tasklet

A class declaring a handoff-chained loop over a branching tree SHALL be
rejected at registration: a chained loop seeds iteration N+1 from iteration N's
terminal batch, which a tree with several leaves does not define. Classes whose
loop is per-input, or which do not loop at all, MAY branch freely.

#### Scenario: A branching chained loop is refused

- **WHEN** a class declares `LoopMode::Chained` over an A→(B,C) tree
- **THEN** registration SHALL fail with an error naming the branching chained loop

### Requirement: A workflow chain may fan out and join into a static DAG

Registration SHALL accept a workflow tasklet topology shaped as a static
acyclic graph (DAG). An **edge** is a tasklet's output channel paired with
one of that channel's consumers, so a tasklet MAY feed several downstream
tasklets by declaring more than one consumer on the channel it emits on
(fan-out), and a tasklet MAY consume more than one incoming channel
(fan-in / join). Handoff routing SHALL be per-channel: a downstream tasklet
receives exactly the values released on the channels it consumes, and a join
node's input batch SHALL **accumulate** every producer's released output — no
producer's contribution may be overwritten or dropped.

A channel's consumer count SHALL NOT change what its producer releases. One
emit SHALL release one value on the producing channel whether that channel has
one consumer or several; the fan-out happens once, where the driver hands the
released batch to each child. A topology SHALL therefore produce the same
per-tasklet invocation count, the same journaled handoff and the same plugin
output whatever its fan-out width, in every SDK.

A tasklet declaring more than one **outgoing** channel SHALL be rejected at
registration with an error naming the node (the emit surface carries one
output channel per tasklet, so a second one could never be routed), as SHALL
a cycle (with an error identifying a node on it). Validation SHALL order
tasklets topologically — every producer of a node precedes it — replacing
the former BFS-from-entry walk. A linear chain SHALL remain valid as the
single-branch case with unchanged behavior.

#### Scenario: Fan-out registers and routes per channel

- **WHEN** tasklet A emits on a channel declaring both B and C as consumers
- **THEN** registration SHALL succeed and both B and C SHALL receive the values A released on that channel

#### Scenario: Join registers and accumulates

- **WHEN** a class declares tasklets B and C each feeding tasklet D on its own channel
- **THEN** registration SHALL succeed in both SDKs, and D's input batch SHALL contain both B's and C's released outputs

#### Scenario: Cycle is rejected

- **WHEN** a class declares a topology in which a tasklet's output reaches itself transitively
- **THEN** registration SHALL fail with an error identifying the cycle

#### Scenario: Fan-out does not multiply the released batch

- **WHEN** the root of a diamond A→(B1→B2, C)→Join emits once on a channel with two consumers
- **THEN** its journaled handoff SHALL hold ONE value, each of B1, C, B2 and the join SHALL run once per contribution, and the sink SHALL receive exactly the five payloads a single-consumer chain would produce — on every SDK that carries the `workflow` capability

