# workflow-operative-structure Specification

## Purpose
TBD - created by archiving change workflow-operative-functionality. Update Purpose after archive.
## Requirements
### Requirement: A workflow operative is structured but runs its tasklets sequentially

A workflow operative SHALL be authorable with the same Tasklet, Channel, and Cell surface as a stream operative, on the same per-cell state engine. The kind difference SHALL be **scheduling**: a workflow operative's tasklets SHALL run sequentially **along each path** of a static **tree-shaped chain** — the linear chain (`Op1 → Op2 → … → TaskN`) as the single-branch case, and fan-out into independent branches per the `workflow-branching` capability (fan-in remains rejected) — where each tasklet's per-channel output is its downstream tasklet's input and a child runs only after its parent commits. **Within a single tasklet, cells SHALL run in parallel** exactly as in a stream operative (per-cell mailbox, worker, and state). The identity of a workflow operative's durable state SHALL use the same `{operative_class, tasklet_id, cell_key}` form — no synthetic tasklet/cell placeholders.

#### Scenario: A workflow operative declares tasklets and cells

- **WHEN** a workflow operative is authored with multiple Tasklets chained by Channels and a Cell key on a stateful Tasklet
- **THEN** each Tasklet SHALL run on the same per-cell engine as a stream operative, with per-Cell state behaving identically, but the Tasklets SHALL execute in topological order along their path rather than concurrently with their ancestors

#### Scenario: Cells within one tasklet run in parallel

- **WHEN** a Tasklet in a workflow operative fans its input across multiple cell keys
- **THEN** those cells SHALL be processed in parallel, each with its own state, and the Tasklet SHALL be considered finished only when every one of its cells has finished

#### Scenario: Workflow state identity has no sentinels

- **WHEN** a durable workflow operative writes Cell state
- **THEN** the on-disk/on-wire identity SHALL be `{operative_class, tasklet_id, cell_key}` with real tasklet and cell values, not reserved placeholders

### Requirement: A tasklet completes when all its cells finish

A workflow operative SHALL advance from one tasklet to the next via a **barrier**: tasklet `Task(i+1)` SHALL NOT begin until tasklet `Task(i)` is **complete**, defined as every item of `Task(i)`'s input having been processed across all of its cells, with no cell mid-run and no pending work. The previous tasklet's collected output SHALL be released to the next tasklet only at this barrier. The first tasklet's input SHALL be seeded from the spawn `args` (`init_values`).

#### Scenario: The next tasklet waits for the barrier

- **WHEN** tasklet A in a workflow operative fans work across cells and tasklet B consumes A's output
- **THEN** B SHALL NOT start processing until all of A's cells have finished, and B SHALL then run on A's complete output

#### Scenario: Completion is countable, not a timed guess

- **WHEN** a tasklet processes a finite, known batch of items across its cells
- **THEN** "tasklet complete" SHALL be determined by all dispatched items having been processed (a count), not by an idle-timeout heuristic

### Requirement: A keyed edge into a workflow tasklet runs one Cell per key

A workflow tasklet reached by a keyed consumer edge SHALL run in dynamic cell mode exactly as
a stream tasklet reached by `key_by` / `keyBy` does: registration SHALL accept the edge in
every SDK, the instance SHALL own no Cell for the tasklet before a pass,
and a Cell SHALL be created on the first item of a pass whose key has no live Cell, by the
same routing that delivers a stream's items, at the moment the item is addressed to the Cell.
A tasklet's `max_cells` SHALL bind as it binds on a stream: an item whose key would mint a
Cell past the cap SHALL be dropped and counted, and the pass SHALL run without it, a Cell that
never ran being recorded nowhere. A stage SHALL be complete when every item it addressed has
been processed by the Cell it was addressed to, whether that Cell was declared or minted, and
its commit SHALL record the Cells that ran by their `{operative_class, tasklet_id, cell_key}`
identity, never by a position in a declared list.

A keyed Cell of a workflow SHALL be evicted by its tasklet's idle TTL only while no pass of
the instance runs, and never while it is named by a completed stage that the instance's
rollback mode would still undo and whose tasklet declares a compensation hook: under
all-or-nothing every such Cell of every committed iteration SHALL be kept until the instance
ends; under keep-progress a committed iteration's Cells SHALL be free once the next pass has
not started. A stream's eviction rules are unchanged.

#### Scenario: Keys mint Cells during a pass

- **WHEN** a workflow whose second tasklet is reached by `.key_by(f)` starts a pass whose
  first tasklet releases values keyed `a`, `b` and `c`
- **THEN** three Cells of the second tasklet exist by the time the stage runs, each processes
  its own values, the stage commits once all three have processed theirs, and the commit
  names the three identities

#### Scenario: The cap drops a new key and the pass completes without it

- **WHEN** the keyed tasklet declares `max_cells = 2` and a pass releases three distinct keys
- **THEN** two Cells are minted, the third key's items are dropped and counted as capped, the
  stage commits naming two Cells, and the instance completes

#### Scenario: A Cell is not evicted mid-pass

- **WHEN** a keyed workflow Cell with a one-second idle TTL finished its items and the stage
  is still waiting on its commit acknowledgement for longer than the TTL
- **THEN** the sweep leaves the Cell in place, and it is swept only once the instance is
  waiting for its next message

#### Scenario: All-or-nothing keeps a compensable Cell across passes

- **WHEN** an all-or-nothing `per_input` workflow committed an iteration in which a keyed
  compensable tasklet ran for key `a`, and the instance is then idle past the TTL
- **THEN** `a`'s Cell is kept, and a later failure compensates it, while the same class under
  keep-progress evicts `a`'s Cell on that sweep

#### Scenario: Every SDK accepts the edge

- **WHEN** a workflow class in any of the seven SDKs declares `input(...).into(Split).key_by(f)
  .into(Keyed)` with `Keyed` declaring no static keys
- **THEN** registration succeeds, the declaration reaching the controller carries `Keyed` in
  dynamic mode, and no SDK reports the tasklet as refused for its mode

