# postgres-cdc-plugin Specification

## Purpose
Postgres change capture as a plugin package: every committed row change of a publication's
tables, read from the write-ahead log through a logical replication slot after a first copy taken
in the slot's own snapshot, delivered onto a relay's entry channel in commit order, with the
read position confirmed as the relay's input durability says.

## Requirements
### Requirement: Postgres change capture SHALL be a plugin package built outside the core

The provider `postgres_cdc` SHALL be the provider of the plugin package `flezha_postgres_cdc`,
built from the repository `flezha_postgres_cdc_plugin` with `mix flezha.plugin.package`,
bundling no library besides its own application, and loading in a release of the core at boot.

#### Scenario: The archive holds only the plugin

- **WHEN** the package is built
- **THEN** its archive's `lib/` SHALL hold only `flezha_postgres_cdc`

### Requirement: The plugin SHALL check its database before it reads

At start the plugin SHALL report unhealthy with `postgres_cdc.wal_level_not_logical` when the
server's `wal_level` is not `logical`, SHALL report unhealthy with `postgres_cdc.server_too_old` naming the version on a server older than 14, SHALL create the
publication `FOR TABLE` its `tables` when the publication is missing and `tables` is given, and
SHALL report unhealthy with `postgres_cdc.publication_missing` when it is missing and `tables`
is not given, and SHALL report unhealthy with `postgres_cdc.replica_identity_missing` naming a
published table, or one of `tables`, with no usable replica identity (no primary key under the
default identity, or identity `NOTHING`). These checks run when the plugin starts, since a bind
opens no connection: the binding SHALL be accepted, the plugin SHALL report unhealthy with the
code, and it SHALL create no slot and no publication until every check passes. `publication` and each table name SHALL be validated as
identifiers and quoted, and `slot` SHALL hold only lower-case letters, digits and underscores.

#### Scenario: A server without logical replication

- **WHEN** the plugin is bound against a server with `wal_level` `replica`
- **THEN** it SHALL be unhealthy with `postgres_cdc.wal_level_not_logical` and SHALL create no
  slot

#### Scenario: A table with no replica identity

- **GIVEN** a publication holding a table with no primary key and the default replica identity
- **WHEN** the plugin starts
- **THEN** the binding SHALL be accepted, the plugin SHALL be unhealthy with
  `postgres_cdc.replica_identity_missing` naming the table, and SHALL create no slot and no
  publication

### Requirement: The first copy and the stream SHALL meet with nothing missed or doubled

When its slot does not exist and `snapshot` is `initial` (the default), the plugin SHALL create
a temporary slot with an exported snapshot, SHALL read every table of the publication in that
snapshot and inject each row as an event with `op` `r`, SHALL keep that slot as its permanent
slot, at the same position, only when the copy ends, and SHALL then stream changes from the
slot's starting point. The slot's existence SHALL be the one record that the copy ended: a
plugin started again, bound again or moved SHALL stream on from an existing slot without
copying. A refused batch of the copy SHALL be handed over again from where the copy stands. A
copy cut short SHALL leave no slot, and the next start SHALL copy again with a new one, whose
rows SHALL carry new message ids. With `snapshot: never` it SHALL create the slot and stream
from it only.

#### Scenario: Writes during the copy

- **GIVEN** a published table of 50 000 rows and a writer inserting and updating rows of it
  while the copy runs
- **WHEN** the plugin copies and then streams
- **THEN** the relay SHALL hold, after applying every event in order, exactly the table's
  final contents

#### Scenario: A copy cut short

- **GIVEN** a relay with `effectively_once` whose runner is stalled
- **WHEN** the controller is killed in the middle of the copy and started again
- **THEN** no slot SHALL remain, the plugin SHALL create a new one and copy again, and the
  relay SHALL receive every row of the second copy, at its value then, when its runner
  resumes

#### Scenario: Bound again after the copy

- **GIVEN** a plugin whose copy ended and which streams
- **WHEN** it is unbound and bound again
- **THEN** its slot SHALL still exist, no copy SHALL run, and streaming SHALL continue after the
  last confirmed position

### Requirement: Every committed change SHALL become one event, in commit order

The plugin SHALL inject one event per inserted, updated or deleted row and one per truncate,
in commit order, each a JSON object with `op` (`c`, `u`, `d`, `t`, or `r` for the copy),
`schema`, `table`, `after`, `before` (what Postgres sends: the key columns, or all under
`REPLICA IDENTITY FULL`, else `null`), `unchanged` (unchanged TOAST columns left out of
`after`), `lsn`, `xid`, `commit_time` and `seq`. Values SHALL follow the JSON mapping of the
`relay_postgres_poll` source, array columns included, so a row copied and the same row
streamed SHALL give the same JSON; an unknown type SHALL stay text. Each event's metadata SHALL
carry `flz.msg_id` `<plugin id>/<lsn>/<seq>` (`<plugin id>/snapshot/<consistent point>/<JSON array of schema, table and key>` for a
copied row), `pg.table` and `pg.op`.

#### Scenario: Insert, update, delete

- **GIVEN** a published table `orders` with `REPLICA IDENTITY FULL`
- **WHEN** one transaction inserts row 1, updates its `status` and deletes it
- **THEN** the relay SHALL receive three events of one `lsn` and `xid`, `seq` 1 to 3, with
  `op` `c`, `u` (`before.status` the old value) and `d`

#### Scenario: A deleted row is seen

- **WHEN** a row is deleted from a table with the default replica identity
- **THEN** the relay SHALL receive an event with `op` `d` and `before` holding the row's
  primary key

### Requirement: The read position SHALL move only past changes the relay took

The plugin SHALL hand changes over with `inject_many/4` and wait for the verdicts, and SHALL
confirm to Postgres a transaction's end as read only when every change of it was accepted:
after every such transaction when the relay's input is durable, at most every
`commit_interval_ms` when it is not, and whenever the stream is idle. A refused change SHALL stop the stream, which SHALL restart
from the last confirmed position.

#### Scenario: A crash with durable input loses nothing

- **GIVEN** a relay with `effectively_once` bound to the plugin while 10 000 transactions
  are written
- **WHEN** the controller is killed mid-way and started again
- **THEN** every change SHALL have reached the relay at least once by its message id

#### Scenario: A re-streamed change is absorbed

- **GIVEN** a relay with `effectively_once` whose runner is stalled, and 50 changes handed
  over whose transaction end was not yet confirmed
- **WHEN** the replication connection is killed and reconnects
- **THEN** Postgres SHALL stream those changes again and the relay SHALL receive each once
  when its runner resumes

### Requirement: The plugin SHALL report the log its slot holds back

The plugin SHALL read its slot's lag in bytes every `lag_check_ms`, SHALL emit it as the metric
`lag_bytes`, and SHALL report unhealthy with `postgres_cdc.slot_lag` past `max_lag_bytes`. A
lost replication connection SHALL make it unhealthy with `postgres_cdc.replication_failed`
until it reconnects.

#### Scenario: A stalled relay

- **GIVEN** a plugin with `max_lag_bytes` 1048576 whose relay's runner is stalled
- **WHEN** more than 1 MiB of changes are written
- **THEN** the plugin SHALL report unhealthy with `postgres_cdc.slot_lag`, and healthy again
  once the runner resumes and the plugin catches up

### Requirement: The slot SHALL outlive a disable

Disabling or unbinding the plugin SHALL keep its slot; the plugin's docs SHALL say how to drop
an abandoned slot and how `max_slot_wal_keep_size` bounds the log a slot holds. The plugin
SHALL declare itself not shareable and SHALL
refuse a partitioned relay with `plugin.partitioned_host`. No log line, telemetry event,
worker state or listing of ours SHALL carry the password.

#### Scenario: Disable keeps the position

- **GIVEN** a streaming plugin
- **WHEN** it is disabled and enabled again with the same `slot`
- **THEN** the slot SHALL still exist, no copy SHALL run, and streaming SHALL continue after the
  last confirmed position

