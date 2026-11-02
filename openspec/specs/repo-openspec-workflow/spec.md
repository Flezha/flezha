# repo-openspec-workflow Specification

## Purpose

How OpenSpec changes and specs are kept in order. Open changes record the order they may be
implemented in, as a `Depends-On:` first line in each proposal, and a change is not started before
its predecessors are archived. The spec tree is grouped into areas declared once in
`openspec/config.yaml`, every spec folder is named `<area>-<subject>`, and every spec states its
purpose. A change extends an existing spec of its area unless none fits, and a proposed new
capability names the closest spec it could not extend and why. The queue gate
(`scripts/check-openspec-order.sh`), reached like every other gate, checks all of this.
## Requirements
### Requirement: An open change SHALL name the changes that must precede it

Every change in `openspec/changes/` SHALL carry a `Depends-On:` line as the first line of its
`proposal.md`, naming the word `none` or one or more change ids separated by commas. A list rather
than a single id, because a change can be blocked by two unrelated things at once: one that settles
a decision it needs and one that rewrites the code it touches.

Every named id SHALL resolve, either to a directory `openspec/changes/<id>/` or to an archived
directory `openspec/changes/archive/<date>-<id>/`, where `<date>` is the `YYYY-MM-DD` prefix the
archive writes. A name that resolves to neither is a typo or a change someone deleted, and both are
refused rather than read as "no predecessor". A change naming itself SHALL be refused with that
stated, because a copied template is how it happens. A cycle among open changes SHALL be refused
naming the changes on it, because a queue on a cycle can never drain and "still open" would never
say why.

#### Scenario: A change with no predecessor

- **WHEN** a change's `proposal.md` opens with `Depends-On: none`
- **THEN** the gate SHALL accept it

#### Scenario: A predecessor that is archived

- **GIVEN** change `b` whose `proposal.md` opens with `Depends-On: a`
- **WHEN** a directory `openspec/changes/archive/<date>-a/` exists
- **THEN** the gate SHALL accept `b`

#### Scenario: A missing line is refused

- **WHEN** an open change's `proposal.md` carries no `Depends-On:` first line
- **THEN** the gate SHALL refuse, naming the change and the line it needs

#### Scenario: A predecessor that does not exist is refused

- **WHEN** an open change names a predecessor that is in neither `openspec/changes/` nor the archive
- **THEN** the gate SHALL refuse, naming the unresolvable id

#### Scenario: A change naming itself is refused

- **WHEN** an open change's `Depends-On:` line names that same change
- **THEN** the gate SHALL refuse, stating that a change cannot depend on itself

#### Scenario: A cycle is refused

- **GIVEN** open changes `a` and `b` where `a` names `b` and `b` names `a`
- **WHEN** the gate runs
- **THEN** it SHALL refuse, naming both changes as a cycle

### Requirement: The gate SHALL refuse a change that starts before its predecessors are archived

The gate SHALL answer two questions. Over the whole queue, it SHALL refuse a change that has been
started, which is any `- [x]` line in its `tasks.md`, while a change its line names is still a
directory in `openspec/changes/`; a change with no checked task SHALL be accepted whatever its
predecessors, because a queue is allowed to hold work that is not ready yet. For one change, named
as its argument, it SHALL refuse while any named predecessor is still open, naming each one, so a
worker asks it before checking the first box.

#### Scenario: A change waiting behind an open predecessor is accepted

- **GIVEN** change `b` naming `a`, with no task checked in `b`
- **WHEN** `a` is still a directory in `openspec/changes/` and the gate runs over the queue
- **THEN** the gate SHALL accept `b`

#### Scenario: A change started behind an open predecessor is refused

- **GIVEN** change `b` naming `a`, with at least one task checked in `b`
- **WHEN** `a` is still a directory in `openspec/changes/` and the gate runs over the queue
- **THEN** the gate SHALL refuse, naming both changes and stating that `a` must be archived first

#### Scenario: Picking up a change behind an open predecessor is refused

- **GIVEN** change `b` naming `a`
- **WHEN** the gate is run with the argument `b` while `a` is still open
- **THEN** the gate SHALL refuse, naming `a` and stating that it must be archived first

#### Scenario: One open predecessor among several is named alone

- **GIVEN** change `c` whose `proposal.md` opens with `Depends-On: a, b`
- **WHEN** the gate is run with the argument `c` while `a` is archived and `b` is still open
- **THEN** the gate SHALL refuse, naming `b` and not `a`

#### Scenario: Picking up a change whose predecessors are archived is accepted

- **GIVEN** change `b` naming `a`
- **WHEN** the gate is run with the argument `b` and `a` is archived
- **THEN** the gate SHALL accept `b` with no output

### Requirement: The ordering gate SHALL be reached the way every other gate is

The whole-queue mode SHALL be a row in the gate registry in `scripts/run_gates.py`, in the group
`.github/workflows/naming.yml` runs on every pull request, so no one has to know it exists to run
it. Where it judges is a working tree rather than that job: `openspec/changes/` is untracked, so a
fresh checkout carries no queue at all, and over a tree with no open change the gate SHALL report
that it had nothing to judge rather than answer as though the queue were clean. It SHALL print
every refusal it found rather than only the first, because a queue is usually wrong in more than
one place at once, and SHALL exit non-zero when it printed any.

#### Scenario: The gate runs with the naming group

- **WHEN** `python3 scripts/run_gates.py --run @naming` is run
- **THEN** the ordering gate SHALL run and its refusal SHALL fail the run

#### Scenario: A tree carrying no open change is reported, not passed over

- **WHEN** the gate runs over a tree whose `openspec/changes/` is absent or holds only the archive
- **THEN** it SHALL say that it has no open change to judge, rather than report a clean queue

#### Scenario: Every refusal is reported

- **GIVEN** three open changes, two of which have a checked task and name an open predecessor
- **WHEN** the gate runs over the queue
- **THEN** it SHALL print both refusals and exit non-zero

### Requirement: Every spec SHALL belong to one declared area

The spec areas SHALL be declared once, in `openspec/config.yaml`, where OpenSpec shows them to
whoever drafts a change. Every folder in `openspec/specs/` SHALL be named `<area>-<subject>`,
where `<area>` is a declared area, so that a listing of the folder reads grouped by area. The
queue gate (`scripts/check-openspec-order.sh`) SHALL refuse a folder whose name starts with no
declared area, naming the folder and the declared areas. This check reads the tracked spec tree,
so it SHALL run and reach a verdict on a checkout that carries no open change.

#### Scenario: A spec in a declared area is accepted

- **GIVEN** `relay` is a declared area
- **WHEN** `openspec/specs/relay-calls/` exists
- **THEN** the gate SHALL accept it

#### Scenario: A spec outside every area is refused

- **WHEN** `openspec/specs/cell-timers/` exists and `cell` is not a declared area
- **THEN** the gate SHALL refuse, naming `cell-timers` and listing the declared areas

#### Scenario: The check runs with no open change

- **GIVEN** a checkout with no `openspec/changes/` directory
- **WHEN** the gate runs with no argument
- **THEN** it SHALL judge the spec tree and SHALL report its verdict on the folders it read

### Requirement: Every spec SHALL state its purpose

Every `openspec/specs/<capability>/spec.md` SHALL carry a `## Purpose` that says what the
capability covers. The placeholder the archive writes for a spec it creates
(`TBD - created by archiving change ...`) SHALL be refused by the queue gate, naming the spec.
Whoever archives a change that creates a spec SHALL write its purpose in the same step.

#### Scenario: A written purpose is accepted

- **WHEN** a spec's Purpose describes the capability
- **THEN** the gate SHALL accept it

#### Scenario: The archive placeholder is refused

- **WHEN** a spec's Purpose reads `TBD - created by archiving change x. Update Purpose after archive.`
- **THEN** the gate SHALL refuse, naming the spec

### Requirement: A new capability SHALL be proposed only when no existing spec can hold it

A change SHALL put new requirements into an existing spec of their area unless none fits. Every
bullet under `### New Capabilities` in an open change's `proposal.md` SHALL name a capability in
a declared area and SHALL carry `Not in \`<spec>\`: <reason>`, where `<spec>` is an existing
folder in `openspec/specs/`: the closest spec the change could have extended, and why it could
not. The queue gate SHALL refuse a bullet with no such line, or one naming a spec that does not
exist, naming the change and the capability. A proposal whose New Capabilities section lists
nothing SHALL be accepted.

#### Scenario: A new capability with its reason is accepted

- **WHEN** a proposal lists `` - `relay-history`: ... Not in `relay-inspection`: it is a durable record, not a live read ``
- **AND** `openspec/specs/relay-inspection/` exists
- **THEN** the gate SHALL accept the bullet

#### Scenario: A new capability without a reason is refused

- **WHEN** a proposal lists `` - `relay-history`: the relay history kind `` with no `Not in` line
- **THEN** the gate SHALL refuse, naming the change and `relay-history`

#### Scenario: A reason naming no existing spec is refused

- **WHEN** a bullet's `Not in` line names `relay-journal` and `openspec/specs/relay-journal/` does not exist
- **THEN** the gate SHALL refuse, naming the change, the capability and `relay-journal`

#### Scenario: A new capability outside every area is refused

- **WHEN** a proposal lists a new capability `history-log` and `history` is not a declared area
- **THEN** the gate SHALL refuse, naming the change and `history-log`

#### Scenario: A change that adds no capability is accepted

- **WHEN** a proposal's New Capabilities section lists no capability
- **THEN** the gate SHALL accept it

