# generated-operator-ids Specification

## Purpose

An operator can be started without inventing a name for it. When a start names no id, the
controller generates one, proves it free with the same claim a named start uses, and answers it;
this capability defines the generated id's shape, the claim and redraw that prove it free, where the
id is assigned, what the answer and the audit record carry, where a caller's own id stays required,
and the dangers a start without an id carries.
## Requirements
### Requirement: A start without an id SHALL be given a generated id

Every door that starts an operator SHALL accept a start that names no id: `flz operator start`,
the consumer surface's `Operator.start` and every SDK wrapper of it, a tasklet's spawn request in
every SDK, and a partitioned start. The controller SHALL generate the id. A start that names an id
SHALL run under exactly that id, as before.

#### Scenario: The CLI starts an operator without an id

- **WHEN** `flz operator start echo` is run and a runner registers the factory `echo`
- **THEN** an operator SHALL start under a generated id beginning with `echo-`
- **AND** `flz operator list` SHALL show that id

#### Scenario: An application starts an operator without an id

- **WHEN** application code calls the administration surface's start with the factory `echo` and no id
- **THEN** an operator SHALL start under a generated id beginning with `echo-`

#### Scenario: A tasklet spawns a steps operator without an id

- **WHEN** a tasklet requests a spawn of the steps factory `triage` with a payload and no id
- **THEN** a new steps instance SHALL start under a generated id beginning with `triage-`, with the
  payload as its first input

#### Scenario: A named start keeps its id

- **WHEN** `flz operator start echo --id my_echo` is run
- **THEN** the operator SHALL start under the id `my_echo`

### Requirement: A generated id SHALL be the trimmed factory id and a time-and-random suffix within the id limit

A generated id SHALL be the factory id, a `-`, and 16 lowercase base32 characters encoding a
millisecond time and random bits drawn from a cryptographically strong source. The factory part
SHALL be trimmed, without a trailing `-` or `_`, so the whole id passes the operator id limit. For a
partitioned start the trim SHALL also leave room for the largest member suffix, so every member id
passes the limit. The generator SHALL be the one that mints the controller's other steps ids.

#### Scenario: A short factory id is kept whole

- **WHEN** an id is generated for the factory `echo`
- **THEN** it SHALL match `echo-[a-z2-7]{16}` and be 21 bytes long

#### Scenario: A long factory id is trimmed to fit

- **WHEN** an id is generated for a factory id of 30 bytes
- **THEN** the id SHALL pass the operator id validation and SHALL begin with the first bytes of the
  factory id

#### Scenario: A partitioned base leaves room for its members

- **WHEN** a partitioned start with 16 partitions and no id is issued for a factory id of 30 bytes
- **THEN** every member id, up to `#p15`, SHALL pass the operator id validation

#### Scenario: Two ids minted in the same millisecond differ

- **WHEN** many ids are generated for one factory in a tight loop
- **THEN** no two of them SHALL be equal

### Requirement: A generated id SHALL be proven free by the start's claim and redrawn on a clash

A generated id SHALL go through the same admission and the same atomic id claim as a named start;
no separate availability question SHALL be asked before it. When the start meets a refusal that means
the id is taken (the claim is held by a live instance or by a stale registry row, the id collides with a
partition group base or a live singleton, or a state folder for that id already exists in the
namespace), the start SHALL draw a new id and try again, at most three draws in all, and SHALL then refuse with a reason stating that no free id was found. A
start that names its own id SHALL never be redrawn.

#### Scenario: A clash on a generated id draws again

- **GIVEN** the generator is made to produce an id a live operator already holds, then a free one
- **WHEN** a start without an id is issued
- **THEN** the operator SHALL start under the free id and the live operator SHALL be untouched

#### Scenario: A leftover state folder counts as taken

- **GIVEN** a finished steps operator whose state folder is still inside its retention window
- **AND** the generator is made to produce that steps operator's id, then a free one
- **WHEN** a start without an id is issued
- **THEN** the operator SHALL start under the free id and the old folder SHALL be untouched

#### Scenario: Every draw clashes

- **GIVEN** the generator is made to produce only ids that are taken
- **WHEN** a start without an id is issued
- **THEN** the start SHALL be refused after three draws, with a reason stating that no free id was found

#### Scenario: A named start is not redrawn

- **WHEN** `flz operator start echo --id my_echo` is run while `my_echo` is live
- **THEN** the start SHALL answer `already_running` for `my_echo` and SHALL start nothing

### Requirement: A generated id SHALL be assigned once, before the start is forwarded

The id SHALL be assigned by the node that receives the start, before it decides whether to start
locally or forward to a node with a capable runner, so every node the request touches acts on the
same id.

#### Scenario: A forwarded start keeps the origin's id

- **GIVEN** a two-node cluster where only node B has a runner for the factory `echo`
- **WHEN** a start without an id is issued on node A
- **THEN** the operator SHALL run on node B under the id node A's answer reports

### Requirement: The answer and the audit record SHALL name the id the start resolved to

Every start and spawn answer SHALL carry the id it resolved to, generated or given. The audit record
of a start SHALL name that id. A start refused before an id was assigned SHALL be recorded with no id
rather than an invented one.

#### Scenario: The answer carries a generated id

- **WHEN** a start without an id succeeds
- **THEN** its answer SHALL be `{id, outcome}` with the generated id and the outcome `started`

#### Scenario: The audit record names a generated id

- **WHEN** a start without an id succeeds with authorization enforced
- **THEN** the `operator_start` audit record SHALL name the generated id as its operator

### Requirement: An id SHALL stay required where the act names an existing instance

A verb that acts on an existing operator SHALL keep requiring its id: `stop`, `send`, `listen`,
`info`, `rewind`, `scale`, and plugin enable and disable. A schedule SHALL keep building its own
per-slot instance id. Delivering into a running steps operator is governed by the spawn capability, which
requires an id for it.

#### Scenario: Stop without an id is refused

- **WHEN** `flz operator stop` is run with no id
- **THEN** the CLI SHALL refuse it naming the missing `<operator_id>`, and nothing SHALL be stopped

#### Scenario: A schedule slot keeps its own id

- **WHEN** a schedule `nightly` fires a slot
- **THEN** the instance SHALL start under the schedule's per-slot id, not a generated one

### Requirement: The documentation SHALL state the retry danger of a start without an id

`docs/guides/running_operators.md`, the `flz operator start` help screen and `docs/sdk/admin.md` SHALL
state, next to the start call, that a start without an id retried after a lost reply starts a second operator, that a second
flow operator runs until it is stopped, and that passing an id prevents it. The dangers of a spawn without an
id are documented by the spawn capability.

#### Scenario: The CLI and administration pages state the retry danger

- **WHEN** a reader opens `docs/guides/running_operators.md`, `flz operator start --help` or `docs/sdk/admin.md`
- **THEN** each SHALL state that a retried start without an id starts a second operator and that
  passing an id prevents it

