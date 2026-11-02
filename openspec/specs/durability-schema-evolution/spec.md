# durability-schema-evolution Specification

## Purpose

Reading state written by an older version of a tasklet. Every persisted record carries the tasklet's
schema version and is checked before it is decoded; a tasklet declares the oldest version it can
read, a mismatch is answered by a declared migration, and a migrating restore is made durable once.
The stamped version is per tasklet in every SDK, and both SDK cores decide a record's schema from
one table of cases.

## Requirements
### Requirement: Every persisted record is version-verified before it is decoded

An SDK SHALL verify a persisted record's 16-byte header before handing the
record's bytes to any codec. Verification covers `sdk_lang`, `format_id` and
`schema_version`, and applies to every record kind the restore payload carries —
the snapshot, plain change records, effect records and timer-op records alike.

Verification SHALL be implemented once, in the shared core, and SHALL be reached
by both the typed in-process restore fold and the host-crossing restore fold, so
an SDK that runs its Tasklets out-of-process inherits the same guard as one
that runs them in-process.

#### Scenario: the snapshot is verified, not just the change log

- **GIVEN** a Cell whose snapshot was written by a Tasklet declaring `SCHEMA_VERSION = 1`
- **WHEN** that Cell is restored by a Tasklet declaring `SCHEMA_VERSION = 2` with no migration declared
- **THEN** the restore SHALL fail with a schema error naming both versions, and SHALL NOT decode the snapshot bytes into the running `State` type

#### Scenario: a host-run Tasklet gets the same guard as a typed one

- **GIVEN** a Tasklet run through the host crossing (Java or any FFI SDK)
- **WHEN** a restore payload carries a record whose `schema_version` differs from the running Tasklet's
- **THEN** the core SHALL apply the same version decision it applies on the typed path, and SHALL NOT hand the record's bytes to the host's codec as if the versions matched

### Requirement: A Tasklet declares the oldest schema version it can read

A Tasklet SHALL declare `MIN_READABLE_SCHEMA_VERSION`, defaulting to its own
`SCHEMA_VERSION` — that is, "I read only what I wrote". Per record, the runtime
SHALL resolve the stamped version against the running `SCHEMA_VERSION` (`V`) and
`MIN_READABLE_SCHEMA_VERSION` (`M`) as:

- stamped `== V` — decode directly;
- `M <= stamped < V` — migrate (see "A version mismatch is answered by a declared migration");
- stamped `< M` — refuse with an error naming the stamped version and `M`;
- stamped `> V` — refuse with an error naming the stamped version and `V`.

A record stamped above the running version SHALL never be offered to a
migration: older code meeting newer state is a downgrade, not an evolution.

The decision SHALL be made per record, because one WAL legitimately holds
records written by more than one version — the tail written before a deploy sits
below the records written after it.

#### Scenario: a mixed-version WAL folds in order

- **GIVEN** a Cell whose WAL holds records stamped version 1 followed by records stamped version 2
- **WHEN** it is restored by a Tasklet at `SCHEMA_VERSION = 2` with `MIN_READABLE_SCHEMA_VERSION = 1`
- **THEN** the version-1 records SHALL be migrated, the version-2 records SHALL be decoded directly, and all of them SHALL fold in seq order into one state

#### Scenario: state from the future is refused

- **GIVEN** a Cell whose records were written by a Tasklet at `SCHEMA_VERSION = 3`
- **WHEN** a Tasklet at `SCHEMA_VERSION = 2` attempts to restore it
- **THEN** the restore SHALL fail with a from-the-future schema error, and no migration hook SHALL be invoked

#### Scenario: a version below the declared floor is refused

- **GIVEN** a Tasklet at `SCHEMA_VERSION = 4` declaring `MIN_READABLE_SCHEMA_VERSION = 3`
- **WHEN** a record stamped version 2 is restored
- **THEN** the restore SHALL fail with an error naming both 2 and the floor 3

### Requirement: A version mismatch is answered by a declared migration

A Tasklet SHALL be able to declare, for its `State` and for its `Change`, how
to read bytes stamped at an older schema version. Each migration entry point
SHALL receive the stamped `from_version` and the record's bytes — or, in an
SDK whose durable body codec is the language's own term format and offers no
second codec to choose, the decoded term — and SHALL return the running
version's typed value or an error.

Both entry points SHALL default to refusing, so a Tasklet that declares no
migration keeps fail-closed behaviour rather than inheriting a guess. A
Tasklet that declares a `MIN_READABLE_SCHEMA_VERSION` below its
`SCHEMA_VERSION` without implementing the corresponding entry point SHALL
therefore fail at the first affected record, with an error naming the migration
as the missing piece.

A migration SHALL be deterministic: the same bytes and the same `from_version`
SHALL produce the same value on every invocation, for the same reason
a change's `apply_to` must — crash-resume, retry-replay and rewind all re-enter the
restore fold.

#### Scenario: a declared migration restores state across a shape change

- **GIVEN** a Tasklet at `SCHEMA_VERSION = 2` declaring `MIN_READABLE_SCHEMA_VERSION = 1` and a state migration that reads the version-1 encoding
- **WHEN** a Cell whose snapshot was written at version 1 is restored
- **THEN** the migration SHALL be invoked once with `from_version = 1` and the snapshot bytes, and the Cell SHALL resume with the value the migration returned

#### Scenario: claiming readability without implementing it fails loudly

- **GIVEN** a Tasklet declaring `MIN_READABLE_SCHEMA_VERSION` below its `SCHEMA_VERSION` but no migration entry point
- **WHEN** a record stamped at the older version is restored
- **THEN** the restore SHALL fail with an error identifying the absent migration and the `from_version` it was asked for, and SHALL NOT fall back to decoding the bytes directly

#### Scenario: a failing migration does not produce partial state

- **GIVEN** a migration that returns an error for a particular record
- **WHEN** that record is reached during the fold
- **THEN** the restore SHALL fail with an error carrying the migration's reason and the record's `from_version`, and the Cell SHALL NOT be reseated on a partially folded state

### Requirement: The stamped schema version is per Tasklet in every SDK

The `schema_version` stamped into a record SHALL be the declaring Tasklet's
own, in every SDK. An SDK SHALL NOT collapse the schema versions of a class's
Tasklets to a single class-wide value, because doing so re-stamps a
Tasklet's records with a version it never declared and makes every sibling
Tasklet's history unreadable when any one of them is bumped.

`MIN_READABLE_SCHEMA_VERSION` SHALL likewise be declared and carried per
Tasklet. The Elixir SDK spells the pair `schema_version/0` and
`min_readable_schema_version/0` on `Flezha.SDK.Tasklet`, and the migrations
`migrate_state/2` and `migrate_change/2`, each resolved per Tasklet module at
the Cell that stamps and folds the record.

#### Scenario: bumping one Tasklet does not disturb its siblings

- **GIVEN** a class whose Tasklet A declares `SCHEMA_VERSION = 2` and Tasklet B declares `SCHEMA_VERSION = 1`
- **WHEN** each Tasklet's Cells persist a change record
- **THEN** A's records SHALL be stamped 2 and B's records SHALL be stamped 1, and B's Cells SHALL restore without a migration

#### Scenario: an Elixir Tasklet's version is its own

- **GIVEN** an Elixir class whose Tasklet A declares `schema_version/0` as 2 and Tasklet B declares none
- **WHEN** each Tasklet's Cell ships a change record
- **THEN** A's header stamps 2 and B's stamps 1, and B restores a record stamped 1 without a migration

### Requirement: A migrating restore is made durable once

When a restore migrated at least one record, the SDK SHALL report that fact in
its restore acknowledgement, and the controller SHALL trigger the same
`CompactCell` handshake it already drives on WAL size and age. The resulting
snapshot is written by the running version, and the migrated records truncate
away with it.

A Cell SHALL therefore pay its migration on one restore rather than on every
restart for the remainder of its life.

#### Scenario: the next restore after a migration needs no migration

- **GIVEN** a Cell restored with a migration, whose acknowledgement reported the migration and whose compaction completed
- **WHEN** the Cell is restored again
- **THEN** every record in the payload SHALL be stamped at the running version and no migration entry point SHALL be invoked

### Requirement: The two SDK cores SHALL decide a record's schema from one table of cases

The schema decision — decode, migrate from a named version, or refuse — SHALL be expressed as one table of cases that both SDK cores read, so the kernel's decision and the Elixir SDK's decision are tested as substitutable rather than as two independent implementations.

The kernel (`flz_core`) and the Elixir SDK are two runtimes by the cross-SDK rule and SHALL NOT share code. They SHALL share the table: a fixture listing, per case, the record's stamped `schema_version`, the running Tasklet's declared version and readable floor, and the expected verdict. Each core's tests SHALL read that fixture.

The refusal text SHALL be one sentence per condition, character-for-character identical between the two cores. The Controller matches error tokens by prefix and the veneers assert on substrings, so a re-worded refusal in one runtime is a behaviour change even when the verdict is unchanged.

The order of the gates SHALL be the same in both: the language byte first, then the format, then the schema. A record written by another SDK is neither a format question nor a schema one.

#### Scenario: Both cores reach the same verdict for every listed case

- **WHEN** the shared decision fixture is read by the kernel's tests and by the Elixir SDK's tests
- **THEN** every case SHALL produce the same verdict in both, and a case that differs SHALL fail both suites

#### Scenario: A refusal reads the same in either runtime

- **GIVEN** a record stamped below the running Tasklet's readable floor
- **WHEN** each core refuses it
- **THEN** the two refusal sentences SHALL be identical, so a caller matching on the text cannot tell which runtime produced it

#### Scenario: A new case is added once

- **WHEN** a schema condition is added
- **THEN** it SHALL be a row of the shared fixture, and neither core SHALL need a case its own tests alone describe

