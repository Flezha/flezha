# bounded-operative-structure Specification

## Purpose
TBD - created by archiving change bounded-operative-functionality. Update Purpose after archive.
## Requirements
### Requirement: A bounded operative is structured but runs its procedures sequentially

A bounded operative SHALL be authorable with the same Procedure, Channel, and Entity surface as an unbounded operative, on the same per-entity state engine. The kind difference SHALL be **scheduling**: a bounded operative's procedures SHALL run sequentially **along each path** of a static **tree-shaped chain** — the linear chain (`Op1 → Op2 → … → ProcN`) as the single-branch case, and fan-out into independent branches per the `bounded-branching` capability (fan-in remains rejected) — where each procedure's per-channel output is its downstream procedure's input and a child runs only after its parent commits. **Within a single procedure, entities SHALL run in parallel** exactly as in an unbounded operative (per-entity mailbox, worker, and state). The identity of a bounded operative's durable state SHALL use the same `{operative_class, procedure_id, entity_key}` form — no synthetic procedure/entity placeholders.

#### Scenario: A bounded operative declares procedures and entities

- **WHEN** a bounded operative is authored with multiple Procedures chained by Channels and an Entity key on a stateful Procedure
- **THEN** each Procedure SHALL run on the same per-entity engine as an unbounded operative, with per-Entity state behaving identically, but the Procedures SHALL execute in topological order along their path rather than concurrently with their ancestors

#### Scenario: Entities within one procedure run in parallel

- **WHEN** a Procedure in a bounded operative fans its input across multiple entity keys
- **THEN** those entities SHALL be processed in parallel, each with its own state, and the Procedure SHALL be considered finished only when every one of its entities has finished

#### Scenario: Bounded state identity has no sentinels

- **WHEN** a durable bounded operative writes Entity state
- **THEN** the on-disk/on-wire identity SHALL be `{operative_class, procedure_id, entity_key}` with real procedure and entity values, not reserved placeholders

### Requirement: A procedure completes when all its entities finish

A bounded operative SHALL advance from one procedure to the next via a **barrier**: procedure `Proc(i+1)` SHALL NOT begin until procedure `Proc(i)` is **complete**, defined as every item of `Proc(i)`'s input having been processed across all of its entities, with no entity mid-run and no pending work. The previous procedure's collected output SHALL be released to the next procedure only at this barrier. The first procedure's input SHALL be seeded from the spawn `args` (`init_values`).

#### Scenario: The next procedure waits for the barrier

- **WHEN** procedure A in a bounded operative fans work across entities and procedure B consumes A's output
- **THEN** B SHALL NOT start processing until all of A's entities have finished, and B SHALL then run on A's complete output

#### Scenario: Completion is countable, not a timed guess

- **WHEN** a procedure processes a finite, known batch of items across its entities
- **THEN** "procedure complete" SHALL be determined by all dispatched items having been processed (a count), not by an idle-timeout heuristic

### Requirement: `run` is sugar for a single-Procedure bounded operative

The bare `run(ctx, args) -> Result<Output>` authoring form SHALL be retained as **sugar** that expands to a bounded operative with one implicit Procedure and one implicit Entity, whose body is `run` and whose completion is `run` returning. Authoring `run` SHALL be equivalent to hand-writing the one-Procedure form; an author SHALL be able to start from `run` and grow into explicit Procedures/Entities without a rewrite of unrelated code.

#### Scenario: `run` expands to the one-Procedure form

- **WHEN** a bounded operative is authored with only `run(ctx, args)`
- **THEN** it SHALL run as a single-Procedure, single-Entity bounded operative, and SHALL behave identically to the equivalent hand-written one-Procedure bounded operative

#### Scenario: Growing from `run` to procedures is additive

- **WHEN** an author replaces a `run`-form bounded operative with an explicit multi-Procedure form
- **THEN** no change to the controller, protocol, or WAL format SHALL be required for it to run

