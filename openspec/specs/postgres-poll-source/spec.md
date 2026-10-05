# postgres-poll-source Specification

## Purpose
The first-party `relay_postgres_poll` source reads the new, and with a timestamp column the changed, rows of one Postgres table past a cursor it commits to its binding, injects each row as one JSON object with a stable message id, and commits its cursor as its relay's input durability says.
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

