# bounded-operative-structure Specification

## Purpose
TBD - created by archiving change bounded-operative-functionality. Update Purpose after archive.
## Requirements
### Requirement: A bounded operative is structured but runs its operations sequentially

A bounded operative SHALL be authorable with the same Operation, Channel, and Segment surface as a continuous operative, on the same per-segment state engine. The kind difference SHALL be **scheduling**: a bounded operative's operations SHALL run **one at a time, in a linear chain** (`Op1 → Op2 → … → OpN`, no branching in v1), where each operation's output is the next operation's input. **Within a single operation, segments SHALL run in parallel** exactly as in a continuous operative (per-segment mailbox, worker, and state). The identity of a bounded operative's durable state SHALL use the same `{operative_class, operation_id, segment_key}` form — no synthetic operation/segment placeholders.

#### Scenario: A bounded operative declares operations and segments

- **WHEN** a bounded operative is authored with multiple Operations chained by Channels and a Segment key on a stateful Operation
- **THEN** each Operation SHALL run on the same per-segment engine as a continuous operative, with per-Segment state behaving identically, but the Operations SHALL execute in declared order rather than concurrently

#### Scenario: Segments within one operation run in parallel

- **WHEN** an Operation in a bounded operative fans its input across multiple segment keys
- **THEN** those segments SHALL be processed in parallel, each with its own state, and the Operation SHALL be considered finished only when every one of its segments has finished

#### Scenario: Bounded state identity has no sentinels

- **WHEN** a durable bounded operative writes Segment state
- **THEN** the on-disk/on-wire identity SHALL be `{operative_class, operation_id, segment_key}` with real operation and segment values, not reserved placeholders

### Requirement: An operation completes when all its segments finish

A bounded operative SHALL advance from one operation to the next via a **barrier**: operation `Op(i+1)` SHALL NOT begin until operation `Op(i)` is **complete**, defined as every item of `Op(i)`'s input having been processed across all of its segments, with no segment mid-run and no pending work. The previous operation's collected output SHALL be released to the next operation only at this barrier. The first operation's input SHALL be seeded from the spawn `args` (`init_values`).

#### Scenario: The next operation waits for the barrier

- **WHEN** operation A in a bounded operative fans work across segments and operation B consumes A's output
- **THEN** B SHALL NOT start processing until all of A's segments have finished, and B SHALL then run on A's complete output

#### Scenario: Completion is countable, not a timed guess

- **WHEN** an operation processes a finite, known batch of items across its segments
- **THEN** "operation complete" SHALL be determined by all dispatched items having been processed (a count), not by an idle-timeout heuristic

### Requirement: `run` is sugar for a single-Operation bounded operative

The bare `run(ctx, args) -> Result<Output>` authoring form SHALL be retained as **sugar** that expands to a bounded operative with one implicit Operation and one implicit Segment, whose body is `run` and whose completion is `run` returning. Authoring `run` SHALL be equivalent to hand-writing the one-Operation form; an author SHALL be able to start from `run` and grow into explicit Operations/Segments without a rewrite of unrelated code.

#### Scenario: `run` expands to the one-Operation form

- **WHEN** a bounded operative is authored with only `run(ctx, args)`
- **THEN** it SHALL run as a single-Operation, single-Segment bounded operative, and SHALL behave identically to the equivalent hand-written one-Operation bounded operative

#### Scenario: Growing from `run` to operations is additive

- **WHEN** an author replaces a `run`-form bounded operative with an explicit multi-Operation form
- **THEN** no change to the controller, protocol, or WAL format SHALL be required for it to run

