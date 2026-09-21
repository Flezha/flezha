# sdk-local-mode Specification

## Purpose
TBD - created by archiving change add-sdk-local-mode. Update Purpose after archive.
## Requirements
### Requirement: A declared operative graph SHALL run in process with no controller

An SDK SHALL provide a local-mode entry point that accepts the same operative
class a user deploys and runs its declared graph entirely in the host process,
with no controller, no transport connection and no socket. The class SHALL be
accepted unchanged: local mode consumes the host-class declaration the authoring
DSL already produces, and SHALL NOT require a separate declaration, a local-only
builder surface, or any edit to the tasklets themselves.

Every stage of the declared graph SHALL execute — entry-channel decode, edge
transforms (`map`, `filter`, `flat_map`), edge keying (`key_by`), merged inputs,
one cell per resolved key, mailboxes and their overflow policy, the retry
ladder, and inter-tasklet delivery.

#### Scenario: A two-tasklet pipeline runs locally

- **WHEN** an operative declaring `entry → Normalize → (inter-tasklet) → Total → sink` is started in local mode and fed two values on the entry channel
- **THEN** both tasklets SHALL run, the inter-tasklet hop SHALL be performed by the runtime rather than by caller code, and `Total`'s state SHALL reflect both values

#### Scenario: Edge transforms and keying run

- **WHEN** an entry chain declares `.filter(...)`, `.flat_map(...)` and `.key_by(...)` before `.into(P)` and is fed values in local mode
- **THEN** filtered values SHALL NOT reach `P`, each produced value SHALL be routed to the cell its key names, and one cell SHALL exist per distinct key

#### Scenario: The deployed class is accepted unchanged

- **WHEN** the same operative class is run in local mode and then registered against a controller
- **THEN** both SHALL accept the identical declaration, and no local-mode-specific member SHALL be required on the class

### Requirement: Local mode SHALL share the production code path

Local mode MUST reuse the runtime's existing dispatch, routing, cell and
mailbox machinery, and SHALL NOT introduce a second implementation of cell
lifecycle, key resolution, channel routing, retry or overflow semantics. Input
supplied to local mode SHALL enter through the same dispatch entry point a frame
arriving on a transport uses, and an inter-tasklet value SHALL travel the same
routing seam that live delivery and replay travel.

Where local mode differs from a deployed runner, the difference SHALL be
expressed by supplying a different outbound sink, never by branching the routing
or cell path on whether a controller is attached.

#### Scenario: The routing seam is shared

- **WHEN** a value is emitted by an upstream tasklet onto an inter-tasklet channel in local mode
- **THEN** it SHALL be delivered through the same routing function used for live delivery and for a replay slice

#### Scenario: No parallel cell implementation exists

- **WHEN** the local-mode implementation is inspected
- **THEN** it SHALL contain no separate cell registry, key resolver, mailbox or retry ladder of its own

### Requirement: Local mode SHALL state at runtime that nothing is durable

Local mode MUST emit a notice on every run stating that state is held in memory
and that nothing is persisted. The notice SHALL be produced by the runtime core
rather than by each host SDK, so that no host can ship local mode without it, and
SHALL NOT be suppressible by ordinary configuration.

Documentation that introduces local mode SHALL state the same limitation before
any example, and SHALL name what requires a controller: durability, plugins,
placement and migration, the dead-letter queue, observability export, authz and
multi-runner scale.

#### Scenario: The notice appears on every local run

- **WHEN** an operative is started in local mode
- **THEN** a notice SHALL be emitted stating that state is in memory and nothing is durable

#### Scenario: A host SDK cannot omit the notice

- **WHEN** a host SDK's local-mode entry point is invoked
- **THEN** the notice SHALL originate from the shared runtime core, so that adding a new host SDK cannot produce a local mode that is silent about durability

### Requirement: Sink output SHALL be delivered to a caller-supplied callback

A graph terminating in a plugin-bound sink SHALL run in local mode, and each
value released on that sink SHALL be delivered to a callback the caller registers
by the sink's declared name. The value SHALL carry the identity and metadata a
deployed run would stamp on it, because local mode intercepts output at the point
production frames it rather than before.

#### Scenario: A registered callback receives sink output

- **WHEN** a caller registers a callback for the sink named `totals` and the graph emits two values on it
- **THEN** the callback SHALL be invoked twice, in release order, with those values

#### Scenario: Output carries production identity stamps

- **WHEN** a keyed sink releases a value in local mode
- **THEN** the delivered value SHALL carry the same cell, sequence and key stamps a deployed run would attach

### Requirement: Input SHALL be supplied by the caller on a named entry channel

Local mode SHALL accept input addressed to an entry channel by its declared name,
and SHALL apply the channel's declared decode, entry parse ladder and admission
exactly as a deployed runner does. Feeding a channel the declaration does not
name SHALL fail with an error naming the channel and the operative.

Local mode SHALL NOT connect to, poll or otherwise obtain input from any external
source of its own.

#### Scenario: Input reaches the declared entry channel

- **WHEN** the caller feeds values to the entry channel named `lines`
- **THEN** the values SHALL be decoded by that channel's declared decoder and routed to its consumers

#### Scenario: An unknown channel name is refused

- **WHEN** the caller feeds a channel name the operative does not declare
- **THEN** the call SHALL fail with an error naming both the channel and the operative

### Requirement: Local mode SHALL refuse what it cannot honour

Local mode MUST fail loudly rather than degrade silently when a declaration
requires a capability it does not provide. A declared sink with no registered
callback SHALL be reported rather than discarded, and a declaration requiring a
controller-side capability — an installed plugin, partitioning, a remote
durability backend — SHALL fail with an error naming the capability and stating
that it requires a controller.

A local run SHALL NOT report success for work it did not perform.

#### Scenario: An unregistered sink is reported

- **WHEN** a graph declares a sink for which the caller registered no callback and a value is released on it
- **THEN** the run SHALL report the unhandled sink by name rather than discarding the value silently

#### Scenario: A controller-only declaration is refused

- **WHEN** an operative whose declaration requires a controller-side capability is started in local mode
- **THEN** startup SHALL fail with an error naming the capability and stating that it requires a controller

### Requirement: The local-mode surface SHALL NOT be named as testing material

Every user-facing local-mode symbol, module, file and documentation page MUST be
named for what it does — running an operative locally — and SHALL NOT carry
`test`, `testing`, `harness`, `mock`, `fake`, `stub`, `dummy` or `sample` in its
name. Local mode is a way to run an application, not a way to test one, and a
user reaching for it SHALL NOT be asked to import something that reads as test
scaffolding.

Kernel code promoted into the local-mode path SHALL be moved into a module named
for its subject, and SHALL NOT remain in or be re-exported from a module named
for test support; the test-support module MAY call the promoted surface, never
the reverse.

Documentation introducing local mode SHALL be filed as user-facing material —
quickstart, getting started, or the SDK's own guide — and SHALL NOT be placed in
a testing section.

This requirement SHALL NOT rename the tasklet harness. `TaskletTestHarness`
and its per-SDK equivalents keep their names because they are for writing tests,
which is the one place the word is correct.

#### Scenario: The public entry point carries no testing word

- **WHEN** a user reaches local mode through any in-scope SDK
- **THEN** no symbol, module or file they must name SHALL contain `test`, `testing`, `harness`, `mock`, `fake`, `stub`, `dummy` or `sample`

#### Scenario: Promoted kernel code leaves the test-support module

- **WHEN** the topology-construction path used by local mode is located
- **THEN** it SHALL live in a module named for its subject, and the test-support module SHALL call it rather than own it

#### Scenario: A gate fails a reintroduced testing word

- **WHEN** a testing word is introduced into a local-mode public symbol or a local-mode documentation page
- **THEN** a repository gate SHALL fail and name the offending file and line

#### Scenario: Tests of local mode are unaffected

- **WHEN** a test file exercising local mode is named
- **THEN** the gate SHALL NOT flag it, because the rule governs the public surface and its documentation, not the tests that cover them

### Requirement: Local mode SHALL be available in every SDK

The capability SHALL ship in every SDK this repository publishes — Rust, Java,
Elixir, Node, Python, Ruby and PHP — and the behaviour observed through each
SHALL be equivalent for an equivalent declaration. The Elixir SDK SHALL express
the capability in its own core, which inherits nothing from the shared Rust
kernel; the remaining SDKs reach one implementation through `flz_core`.

No SDK is deferred once this change lands, so no documentation may name one as
deferred.

#### Scenario: Equivalent behaviour across the in-scope SDKs

- **WHEN** an equivalent two-tasklet declaration is run in local mode on each SDK with the same inputs
- **THEN** each SHALL execute both tasklets, perform the inter-tasklet hop in the runtime, and deliver the same sink output

#### Scenario: No SDK claims a deferral

- **WHEN** any SDK's documentation is read
- **THEN** it SHALL NOT name local mode as deferred, because every SDK carries it

### Requirement: Local mode proves every capability the language layer owns

Each SDK SHALL run, in its own suite and in local mode, the fixture class its
conformance manifest names for each of `graph_builder`, `keyed_channels`,
`dynamic_cell`, `retry_ladder` and `cell_timers`, and SHALL assert the
outputs and refusals the end-to-end scenario for that capability asserts, as
far as they are observable without a controller. A capability the controller
owns SHALL stay refused by name and SHALL NOT be approximated locally; adding a
sixth capability to this list needs its own proposal.

#### Scenario: A language-layer capability runs locally in every SDK
- **WHEN** an SDK's suite runs
- **THEN** it SHALL contain a local-mode run for each of the five capabilities, or a refusal assertion where every fixture class for that capability declares something controller-only

#### Scenario: A controller capability is not faked
- **WHEN** a local-mode test is added for `state_persistence`, `effectively_once`, `workflow` or another controller-owned capability
- **THEN** it SHALL assert the refusal by name and nothing else

### Requirement: A local run fires the timers a class declares

A local run SHALL fire processing-time and event-time timers through the
core's own timer path, so that `on_timer` runs in local mode with the
scheduled timestamp exactly as it does under a controller. It SHALL NOT hold a
timer implementation of its own.

#### Scenario: A processing-time timer fires locally
- **WHEN** a cell in a local run schedules a timer after a duration and the run continues past it
- **THEN** the timer callback SHALL run with the scheduled timestamp

#### Scenario: An event-time timer fires locally
- **WHEN** a cell in a local run schedules a timer at an event time and a later input carries a later event time
- **THEN** the timer callback SHALL run before that input is processed

