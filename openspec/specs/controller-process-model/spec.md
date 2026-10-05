# controller-process-model Specification

## Purpose
TBD - created by archiving change bound-every-call-find-every-process-once. Update Purpose after archive.
## Requirements
### Requirement: Every call into a resolved process SHALL be bounded and SHALL declare its refusal

A controller-side call into a process reached by pid, or by a variable that holds one, SHALL pass through `Flezha.SDK.SafeCall`, the bounded call the controller imports from the Elixir SDK, with an explicit bound, and its answer on a timeout, a `:noproc` or any other exit SHALL be a value the caller declared, never an exit the caller inherits.

The reason → result mapping SHALL be data supplied at the call site, not a `catch` clause written there, so that a call site adds a declaration and never a policy. `SafeCall` SHALL supply no default bound: a nested pair of calls is only bounded when the inner bound is strictly smaller than the outer one, and a seam that invented a shared default would make both equal and abandon the caller at the instant the callee gives up.

A call into a registered node-wide singleton is a different risk and is NOT covered by this requirement: a store or manager that is gone is a fault to propagate, and such a call SHALL be free to crash its caller.

The telemetry event `[:flezha, :convergence, :safe_call]` is a published surface and SHALL NOT be renamed with the module that emits it.

#### Scenario: A wedged callee refuses within its bound

- **GIVEN** a caller with a declared bound and a declared refusal value
- **WHEN** the callee does not answer inside that bound
- **THEN** the caller SHALL receive its declared value, SHALL remain alive, and exactly one `[:flezha, :convergence, :safe_call]` event SHALL be emitted

#### Scenario: A callee killed mid-call is the same answer as one that never existed

- **WHEN** a callee is killed after the call is in flight, and separately when the target was never started
- **THEN** the caller SHALL receive its declared refusal value in both cases, and the two SHALL NOT differ in shape

#### Scenario: A nested pair keeps its inner bound smaller

- **GIVEN** a caller that bounds an outer call and the callee that bounds an inner one
- **WHEN** the innermost target is wedged
- **THEN** the inner call SHALL refuse first and the outer caller SHALL act on that value rather than time out itself

### Requirement: One resolver SHALL answer which process serves a relay

The question "which process serves this relay" SHALL be answered by one seam that declares a resolver per key kind, and no consumer SHALL name a registry, an ETS table or a process-hub call to answer it for itself.

A resolver SHALL answer `{:ok, pid}` or a reason that distinguishes not-found, dead and hub-unavailable, in one vocabulary. A cache in front of a resolver SHALL be invalidated by a monitor rather than by a per-use liveness probe, so the liveness-checked path is never sat behind an unchecked one; a per-frame `Process.alive?/1` is a message the target must answer and measures its mailbox, not its liveness.

The key SHALL have one spelling per kind: a caller that holds a relay's class SHALL NOT reach the same resolver through an argument named for its id.

#### Scenario: A dead target is refused, not returned

- **WHEN** the process a cache holds for a relay has died
- **THEN** the next resolution SHALL answer from the registry and SHALL NOT return the dead pid

#### Scenario: A hub that is unavailable is named as such

- **WHEN** a relay's namespace hub is gone, terminating or mid-restart
- **THEN** the resolver SHALL answer a hub-unavailable reason distinct from not-found, and SHALL NOT raise into its caller

### Requirement: A watched process's death SHALL be recorded once by one watch seam

Monitor bookkeeping SHALL live in one seam that maps a watched pid to the tags its owner keyed on it, and a module SHALL NOT keep a monitor-reference table of its own beside its real work.

The seam SHALL own `watch`, `unwatch` and the translation of a `:DOWN` into the set of tags that named the dead pid, and SHALL flush on the last tag so no stale `:DOWN` arrives afterwards. The REACTION to a death SHALL stay with the owner: what a dead pid means is the owner's decision, and the seam SHALL NOT decide it.

#### Scenario: One death drops every tag exactly once

- **GIVEN** several tags keyed on one watched pid
- **WHEN** that pid dies
- **THEN** every tag SHALL be dropped exactly once and no further `:DOWN` for it SHALL be delivered

### Requirement: A timer SHALL be armed, cancelled and re-armed through one helper

A module that holds a timer SHALL declare its timer keys and SHALL use one helper to arm, cancel and re-arm them; it SHALL NOT write its own arming function, its own reference field or its own cancel guard.

Arming a key that is already armed SHALL leave exactly one live timer, and a message from a cancelled or superseded timer SHALL be dropped rather than acted on. The message tags a module sends itself are its own and are not folded.

#### Scenario: Arming twice leaves one live timer

- **WHEN** a key is armed and then armed again before it fires
- **THEN** exactly one timer SHALL remain live and the superseded one's message SHALL NOT be acted on

### Requirement: A supervised child SHALL declare its restart kind from one table

A child specification SHALL be built from a declared restart kind rather than written as a literal map at each site, and the kind SHALL be a row of one policy table.

A process that is torn down deliberately SHALL NOT be declared with a restart policy that brings it back: an intentional disconnect is not a fault. `use GenServer, restart: …` IS the declaration form for a module that has one and SHALL be left as it is; only hand-written specification maps are folded. A process-hub child specification is a durable, persisted format and SHALL only be read through one accessor, never re-shaped by this seam.

#### Scenario: A kind that is not in the table is refused

- **WHEN** a child specification is built with a restart kind that no row declares
- **THEN** the build SHALL fail rather than produce a specification

### Requirement: A fan-out over many targets SHALL take a bound and SHALL NOT relocate caller-owned state

A concurrent walk over independent targets SHALL be made through one fan-out seam that accepts a bound and an on-timeout policy, so a caller needing a bounded fan-out extends the seam instead of building a pipeline beside it.

A step that owns state scoped to the calling process SHALL NOT be moved into a spawned task: an ETS table belongs to the process that created it, an unlink acts on the caller, and a route seeded by a step names the caller's own pid — such a step SHALL run inline with the budget still remaining.

A walk over many targets SHALL step over one that cannot be reached, SHALL report which ones it could not reach, and SHALL cost one bound rather than one bound per unreachable target.

#### Scenario: A late item is killed and reported in order

- **GIVEN** a bounded fan-out over several items where one does not finish in time
- **THEN** that item SHALL be reported as a timeout, the others SHALL be returned in input order, and the walk SHALL NOT wait past its bound

#### Scenario: A teardown step keeping caller-owned state runs inline

- **WHEN** a teardown step creates an ETS table, unlinks a process or seeds a route naming the caller
- **THEN** that step SHALL run in the calling process and SHALL NOT be spawned, and the walk SHALL hand it the remaining budget

