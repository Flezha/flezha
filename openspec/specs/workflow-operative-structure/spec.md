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

