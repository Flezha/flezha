# plugin-postgres Specification

## Purpose

The Postgres plugins. The `relay_postgres_poll` source reads the new or changed rows of one table
past a cursor owned by its binding and injects each as a JSON object with a stable message id.
Postgres change capture, a package built outside the core, reads every committed row change through
a logical replication slot after a first copy taken in the slot's snapshot, delivers them in commit
order, and moves its read position only past changes the relay took. The Postgres output writes the
effectively-once tier's outputs as transactional upserts with a watermark, so re-drives are no-ops.
Each follows the relay's input durability where it reads, validates its options and names, and
reports its health without showing its password.

## Requirements

### Requirement: `relay_postgres_poll` SHALL read one table past its cursor, in order

The first-party source `relay_postgres_poll` SHALL read the rows of its `table` whose cursor
is past the last one it delivered, ordered by the cursor, at most `batch_size` per read, and
SHALL inject each row as one input on its entry channel. With only `id_column` the cursor SHALL
be the id; with `timestamp_column` it SHALL be the pair (timestamp, id), compared as a row,
and rows whose timestamp is not older than `timestamp_delay_ms` SHALL be left for a later read;
a row whose timestamp is NULL SHALL NOT be read. After a full batch it SHALL read again at once;
after a short one, and after a failed read, it SHALL wait `interval_ms`.

#### Scenario: New rows in id order

- **GIVEN** a table `orders` with rows of id 1 to 3 and a plugin with `id_column: id`
- **WHEN** the plugin starts with `start_at: beginning` and rows 4 and 5 are inserted later
- **THEN** the relay SHALL receive rows 1, 2, 3, 4, 5, in that order, each once

#### Scenario: An updated row comes again in timestamp mode

- **GIVEN** a plugin with `timestamp_column: updated_at` and `id_column: id` that has
  delivered row 7
- **WHEN** row 7 is updated and its `updated_at` set to now
- **THEN** the relay SHALL receive row 7 again with its new values

#### Scenario: A row inside the delay is held back

- **GIVEN** `timestamp_delay_ms` 2000
- **WHEN** a row is written with `updated_at` now
- **THEN** it SHALL NOT be delivered within the first 2 seconds and SHALL be delivered after

### Requirement: The plugin SHALL build its query and validate every name

The plugin SHALL accept `table` as `name` or `schema.name`, and `id_column`,
`timestamp_column` and every entry of `columns` as names matching
`^[A-Za-z_][A-Za-z0-9_$]{0,62}$`, refusing any other with `plugin.option_invalid` before
opening a connection. It SHALL quote every name in the query it builds, so names are
case-sensitive, SHALL bind every value as a parameter, and SHALL run every read in a read-only
transaction. A table or a column that does not exist SHALL make the plugin unhealthy naming it,
not crash it.

#### Scenario: A name that is not an identifier

- **WHEN** the plugin is enabled with `table` `orders; drop table x`
- **THEN** the enable SHALL be refused with `plugin.option_invalid` naming `table`, and no
  connection SHALL be opened

#### Scenario: Reads cannot write

- **WHEN** the plugin reads a view that selects `current_setting('transaction_read_only')`
- **THEN** every delivered row SHALL carry `on`

### Requirement: A row SHALL become a JSON object with a stable message id

Each injected payload SHALL be a JSON object from the read columns' names to their values:
integers, floats and booleans as JSON values, `numeric` as a string, text types as strings,
date and time types as ISO 8601 strings, `uuid` in canonical form, `json` and `jsonb` as
JSON, `bytea` as base64, arrays as JSON arrays of the same mapping, `NULL` as `null`, and
any other type as Postgres's text form. Its metadata SHALL carry `flz.msg_id`
`<plugin_id>/<schema.table>/<id>` with only an id column, and
`<plugin_id>/<schema.table>/<timestamp>/<id>` with a timestamp column, naming the schema the
table resolved to and the cursor values in Postgres's text form.

#### Scenario: Types become JSON

- **GIVEN** a row with an `int`, a `numeric(12,4)`, a `timestamptz`, a `uuid`, a `jsonb`, a
  `bytea` and a `NULL` text column
- **WHEN** it is delivered
- **THEN** the payload SHALL be a JSON object holding a number, a string with all four
  decimals, an ISO 8601 string, a canonical uuid string, the JSON value, a base64 string and
  `null`

### Requirement: The plugin SHALL follow the relay's input durability

The plugin SHALL hand every batch over with `inject_many/4` and wait for its verdicts with no
timeout, SHALL move its cursor only past the longest prefix of rows accepted, and SHALL read a
refused row again on its next read. When the relay's input is durable it SHALL commit its
cursor after every confirmed batch; when it is not, once it has caught up or a read failed, and at
most every `commit_interval_ms` while it reads full batches.

#### Scenario: A restart with durable input loses nothing

- **GIVEN** a relay with `effectively_once` bound to the plugin, reading a table of 10 000
  rows
- **WHEN** the controller is killed while the plugin is reading, and started again
- **THEN** the relay SHALL have received every row at least once by its `flz.msg_id`

#### Scenario: A row read again while the log holds it is absorbed

- **GIVEN** a relay with `effectively_once` whose runner is stalled, so its input log
  still holds every delivered row, and a plugin whose cursor was committed before the last
  50 delivered rows
- **WHEN** the plugin's worker is killed and brought up again
- **THEN** it SHALL read those 50 rows again, and the relay SHALL receive each of them
  once when its runner resumes

#### Scenario: Without durable input the cursor is committed on a timer

- **GIVEN** a plain flow relay bound to the plugin with `commit_interval_ms` 5000
- **WHEN** the plugin delivers three full batches within one second
- **THEN** the cursor SHALL be committed at most once in that second

### Requirement: The cursor SHALL belong to the binding and survive restarts

The plugin SHALL commit its cursor to its binding as the levels say and when its worker
stops, and a bring-up SHALL continue from the committed cursor. A binding with no
cursor SHALL start at the first row with `start_at: beginning`, and after the rows present at
start with `start_at: end`. A cursor committed under another `table`, `id_column` or
`timestamp_column` SHALL be dropped, and the plugin SHALL start from `start_at` again.

#### Scenario: A move continues where it stopped

- **GIVEN** a plugin that has delivered rows 1 to 500 and committed its cursor
- **WHEN** its relay moves to another controller
- **THEN** the plugin there SHALL continue with row 501

#### Scenario: A changed table starts again

- **GIVEN** a binding that committed a cursor reading `orders`
- **WHEN** it is brought up with `table` `orders_v2` and `start_at: beginning`
- **THEN** it SHALL read `orders_v2` from its first row

#### Scenario: Starting at the end

- **GIVEN** a table with 100 rows and a new binding with `start_at: end`
- **WHEN** the plugin starts and one row is inserted
- **THEN** the relay SHALL receive only that row

### Requirement: The plugin SHALL NOT be shared or bound to a partitioned relay

`relay_postgres_poll` SHALL declare itself not shareable, and SHALL refuse a partitioned
relay with `plugin.partitioned_host`.

#### Scenario: A partitioned relay

- **WHEN** the plugin is bound to a member of a partition group
- **THEN** the bind SHALL be refused with `plugin.partitioned_host`

### Requirement: The plugin SHALL report failures and keep its password out of sight

The plugin SHALL report unhealthy after `failure_threshold` consecutive failed reads, and when
its table or a read column is missing, naming the reason, and healthy after the next
successful read. The password SHALL never appear in worker state, logs, telemetry, the printed
prepared options or `flz plugin list`, and a literal `password` SHALL be the one the connection
authenticates with.

#### Scenario: Three failed reads and a good one

- **GIVEN** a running plugin with `failure_threshold` 3
- **WHEN** three reads in a row fail and the next one succeeds
- **THEN** the plugin SHALL be unhealthy after the third failed read and healthy after the
  next successful one

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

### Requirement: PostgreSQL output plugin with transactional upsert + watermark

The controller SHALL provide a `postgres` relay output plugin
(`Flezha.Plugin.Provider.Postgres`, backed by `postgrex`) that, per flush batch,
executes ONE database transaction containing (a) an upsert of every record keyed
by `flz.key` and (b) an upsert of the per-Cell progress row
(`flezha_sink_progress`: cell, last outbox seq, epoch). Data and progress
SHALL never be committed separately.

#### Scenario: Batch commits atomically

- **WHEN** a flush batch of 3 records for cell S is delivered to the plugin
- **THEN** the 3 row upserts and the `flezha_sink_progress` update for S commit
  in one transaction, or none of them do

#### Scenario: Connection failure delays, never loses

- **WHEN** the database is unreachable during a flush
- **THEN** the transaction is retried with backoff, the outbox entries remain
  durable at the controller, and no record is dropped

### Requirement: Monotonicity guard makes re-drives no-ops

Row upserts SHALL be guarded by `(epoch, seq)` monotonicity: an incoming record
whose `(epoch, outbox_seq)` is not newer than the stored row's SHALL leave the
row unchanged. A re-driven batch (controller restart after flush, before cursor
advance) SHALL therefore be a no-op.

#### Scenario: Re-driven batch changes nothing

- **WHEN** the same batch is delivered twice (crash between flush and cursor
  write)
- **THEN** after the second delivery every affected row and the progress row are
  byte-identical to their state after the first delivery

#### Scenario: Stale epoch cannot regress a row

- **WHEN** a record with epoch 1 arrives for a key whose stored row has epoch 2
- **THEN** the stored row is unchanged

### Requirement: Default schema, opt-in DDL, secure configuration

The plugin SHALL ship a fixed default schema (`flezha_records(key text primary
key, cell text, seq text, epoch bigint, payload bytea, updated_at
timestamptz)` and `flezha_sink_progress(cell text primary key, last_seq text,
epoch bigint)`). Automatic DDL creation SHALL be gated by an `auto_create` config
flag defaulting to false. Credentials SHALL come from plugin configuration /
environment and SHALL never appear in logs or telemetry.

#### Scenario: Auto-create off by default

- **WHEN** the plugin starts against a database missing the tables and
  `auto_create` is not set
- **THEN** the plugin fails health with a clear error and creates no DDL

#### Scenario: Credentials never logged

- **WHEN** the plugin logs a connection failure
- **THEN** the log line contains host/database but no password material

### Requirement: Health participates in the plugin health surface

The plugin SHALL implement the provider `health/1` callback reporting connection
pool state and last successful commit age, so the existing plugin-health →
convergence escalation path covers the output plugin.

#### Scenario: Unhealthy on sustained commit failure

- **WHEN** no flush transaction has committed within the configured unhealthy
  threshold while batches are pending
- **THEN** `health/1` reports unhealthy with the failure reason

### Requirement: Record identity is the admission test, not the epoch

A record SHALL be admitted to the identified (guarded) upsert path when it carries a
**replay-stable identity** — a non-empty `flz.cell` and a numeric dotted
`flz.seq`. The presence of `flz.epoch` SHALL NOT be part of that test.

An absent `flz.epoch` SHALL resolve to epoch `0`. Zero is the base epoch of the
controller's outbox lineage, and nothing bumps it outside the effectively-once
tier, so a record with no epoch is correctly ordered against every other record of
its key by `seq` alone under the existing `(epoch, seq)` guard.

This admits records emitted on a `.keyed()` channel **without** the effectively-once
tier — which the SDK stamps with `flz.cell`, `flz.seq` and `flz.key` but not
`flz.epoch` — into the changelog upsert those stamps exist to enable.

A record carrying neither a cell nor a sequence remains unidentified and SHALL be
inserted under a unique synthetic key, unchanged.

#### Scenario: A tier-off keyed record updates its row on every emit

- **WHEN** a relay on a `.keyed()` channel without the effectively-once tier
  emits twice for the same `flz.key`, stamped `flz.seq` `"4.0"` then `"5.0"`, with
  no `flz.epoch`
- **THEN** the row for that key holds the second record's payload and `seq = "5.0"`

#### Scenario: A tier-off keyed record is still guarded against regression

- **WHEN** a record stamped `flz.seq` `"4.0"` with no `flz.epoch` arrives for a key
  whose stored row already holds `seq = "5.0"` at epoch `0`
- **THEN** the stored row is unchanged

#### Scenario: A tier-on record still outranks a tier-off record for the same key

- **WHEN** a record stamped `flz.epoch = 1` arrives for a key whose stored row holds
  epoch `0`
- **THEN** the stored row is overwritten, regardless of either record's `flz.seq`

#### Scenario: A fully unstamped record is still a plain insert

- **WHEN** a record with no `flz.cell` and no `flz.seq` is delivered
- **THEN** it is inserted under a unique synthetic key and no existing row is
  touched

#### Scenario: An identified record advances its Cell's progress row

- **WHEN** a tier-off keyed record carrying `flz.cell` and `flz.seq` is committed
- **THEN** the `flezha_sink_progress` row for that Cell advances to its
  `(epoch, seq)`, under the same monotonicity guard as the record rows

#### Scenario: A row left degenerate by the epoch gate heals itself

- **WHEN** a row written before this change holds a stable key with an empty cell
  and an empty seq, and a new identified record arrives for that key
- **THEN** the row is overwritten, because a non-empty sequence outranks an empty
  one — no backfill or migration is required

### Requirement: The output plugin's option set is strict and its tunables are validated at preparation

`init_provider/1` SHALL refuse an option key the provider does not offer, and SHALL refuse a tunable whose value it cannot interpret, before the output plugin worker or its connection pool is started. Both refusals SHALL be returned as `{:error, binary()}` naming the key or the option and the offered value.

The strict option set SHALL be declared, so that the check runs on the path a user's stored configuration travels. A plugin spec and a stored capability row reach `init_provider/1` without ever constructing the provider struct, so a check reachable only from the CLI string-map builder does not satisfy this requirement.

The health threshold SHALL be validated at preparation and read as a plain value by the health probe. A conversion performed on the probe path is a conversion performed on every probe: when it raises, the probe's uncertainty handling reports `{:fail_open, …}`, which reads as healthy, so a single unreadable value disables that plugin's health verdict for as long as it stays configured — silently, and for ever.

#### Scenario: An unknown option is refused at enable time

- **GIVEN** a plugin configured with a key the output plugin does not offer
- **WHEN** the plugin is enabled
- **THEN** `init_provider/1` SHALL return `{:error, message}` naming the key
- **AND** no output plugin worker and no connection pool SHALL have been started

#### Scenario: An uninterpretable health threshold is refused at enable time

- **GIVEN** a plugin configured with a `unhealthy_after_ms` value that is not an integer and does not name one
- **WHEN** the plugin is enabled
- **THEN** `init_provider/1` SHALL return `{:error, message}` naming the option
- **AND** the plugin SHALL NOT reach a state where its health probe reports an uncertain verdict for that reason

#### Scenario: Host-injected keys are still accepted

- **GIVEN** a plugin enabled on a relay, so that the host merges its identity keys into the provider's opts
- **WHEN** the plugin is enabled
- **THEN** those keys SHALL be accepted, and the output plugin SHALL start exactly as it does today
