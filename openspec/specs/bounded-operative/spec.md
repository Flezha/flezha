# bounded-operative Specification

## Purpose

The bounded-operative capability is the contract for the **Bounded Operative** kind — the operative that runs once to completion and finishes (formerly "Task"). It defines the `BoundedOperative` authoring surface (a single run-once `run(ctx, args)` with no channels, entity keys, or per-entity state), the `register_bounded` entrypoint sibling to `register_unbounded`, the bounded-kind spawn path through one sealed per-runner operative registry, and the terminal `BoundedCompleted` control message that drives standard teardown with no retry and no convergence action. Behaviour is identical to the former task kind; only the contract's names change. Because the bounded kind does not declare `persistent_state?`, per-Entity state queries against a bounded operative are rejected through the kind-unsupported gate. Duration bounds, self/external stop, internal loops, and retry/replay are out of scope.
## Requirements
### Requirement: A bounded operative is authored with a distinct run-once trait

The SDK SHALL expose a `BoundedOperative` authoring surface distinct from the unbounded one, registered through an entrypoint that is a sibling of unbounded registration (`register_bounded`, alongside `register_unbounded`), making the bounded class spawnable by its `factory_id`. The bare `run(ctx, args) -> Result<Output>` form SHALL be the **trivial** authoring case — sugar for a single-Procedure, single-Entity bounded operative (see `bounded-operative-structure`) — and an author MAY grow into explicit Procedures, Channels, and Entities (run sequentially) without a rewrite of unrelated code. Registering a bounded operative SHALL NOT require the author to enumerate capability flags — choosing the bounded kind is sufficient.

#### Scenario: A registered bounded operative is spawnable by factory id

- **WHEN** a bounded class is registered with the SDK before the runner starts
- **THEN** its `factory_id` SHALL be resolvable, and an `AddOperative` naming that `factory_id` with the bounded kind SHALL spawn a bounded instance

#### Scenario: The bounded trait scales from `run` to procedures

- **WHEN** a bounded operative is authored with only `run(ctx, args)`, and later re-authored with explicit Procedures/Channels/Entities
- **THEN** both forms SHALL be valid `BoundedOperative` authorings, the first behaving as a single-Procedure, single-Entity case of the second

### Requirement: Starting an operative with the bounded kind spawns a bounded instance

When the Controller issues `AddOperative` with `kind` resolving to `:bounded`, the SDK SHALL spawn a **bounded** instance for the named `factory_id`; when `kind` resolves to `:unbounded` (the default), it SHALL spawn an unbounded instance. The two variants SHALL be held in one sealed per-runner operative registry whose entry is resolved once per inbound frame at the routing boundary. Spawning SHALL be idempotent on a duplicate instance id.

#### Scenario: Bounded kind spawns a bounded instance

- **WHEN** the SDK handles `AddOperative` whose `kind` resolves to `:bounded` for a registered bounded `factory_id`
- **THEN** a bounded instance SHALL be created and registered under the instance id

#### Scenario: Default kind still spawns an unbounded instance

- **WHEN** the SDK handles `AddOperative` whose `kind` is empty or `:unbounded`
- **THEN** an unbounded instance SHALL be created

#### Scenario: Unknown bounded factory is reported

- **WHEN** `AddOperative` names a `factory_id` with bounded kind that is not registered as bounded
- **THEN** the SDK SHALL report an error and SHALL NOT create an instance

### Requirement: A bounded operative runs to an exit trigger and deregisters terminally

A bounded operative SHALL be spawned on demand with `args` (`init_values`), representing **one job/session**. It MAY be **structured** — authoring Procedures, Channels, and Entities on the same per-entity engine as an unbounded operative, with its procedures run **sequentially in a linear chain** and each procedure **done when all its entities finish** (see `bounded-operative-structure`). A bounded operative MAY **receive input after it starts** (an inbox), **loop** with an intrinsic terminal condition, be bounded by a declared maximum duration, be stopped externally, declare per-procedure compensation hooks with an author-selected rollback mode (see `bounded-operative-saga`), and — being **durable by default** (opt-out per class) — resume from durable state across a crash (see `bounded-operative-durability`). It SHALL terminate on the first exit trigger (all procedures done, `ctx.complete`, loop terminal condition, duration, or external stop). On termination, the SDK SHALL emit a `BoundedCompleted` control message carrying the terminal result/error payload and a status (`ok | error | cancelled`); the Controller SHALL then drive the **single** standard operative teardown — running compensation first when terminating abnormally — so the bounded operative is deregistered, its plugins disabled, and its quota released, terminal in every case, with convergence never acting on it, and its data retained for the finished-job window. A `run`/procedure returning an error SHALL be terminal with no automatic platform retry.

#### Scenario: Clean completion deregisters the bounded operative

- **WHEN** a bounded operative reaches an exit trigger with all work successful
- **THEN** the SDK SHALL emit `BoundedCompleted` with status `ok` and the result payload, and the Controller SHALL deregister it, disable its plugins, release its quota, and retain its data for the finished-job window

#### Scenario: Failure compensates then terminates with no auto-retry

- **WHEN** a bounded operative's procedure returns an error after partial progress
- **THEN** the SDK SHALL fire compensation for completed procedures in reverse order (per the operative's rollback mode), emit `BoundedCompleted` with status `error` and the error payload, and the Controller SHALL tear it down terminally without convergence rebinding it

#### Scenario: External stop is terminal

- **WHEN** a running bounded operative is stopped via `flz operative stop`
- **THEN** the SDK SHALL cooperatively cancel in-flight work, run compensation if partial progress was made, emit `BoundedCompleted` with status `cancelled`, and the Controller SHALL tear it down terminally

#### Scenario: `run` form still works as the trivial case

- **WHEN** a bounded operative is authored with only `run(ctx, args)` and `run` returns
- **THEN** it SHALL complete exactly as a single-Procedure bounded operative, preserving today's run-once ergonomics

