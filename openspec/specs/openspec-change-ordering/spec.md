# openspec-change-ordering Specification

## Purpose

The openspec-change-ordering capability records the order in which open changes may be
implemented, and makes that record checkable. Several changes can only be worked in one
sequence: one settles a decision another needs, or rewrites the lines another is about to move.
OpenSpec has no dependency field, so the fact lives on the first line of each `proposal.md` as
`Depends-On: <id>[, <id>...]` or `Depends-On: none`, which the CLI's section parser ignores and
`openspec validate --strict` therefore accepts. `scripts/check-openspec-order.sh` reads those
lines in two modes: with no argument it judges whether the whole queue's record is honest, and
with a change id it answers whether that change may be picked up yet. The capability governs the
queue directory only, never the shipped system, and its enforcement is a working tree rather
than continuous integration, because `openspec/changes/` is untracked and a fresh checkout
carries no queue to judge.
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

