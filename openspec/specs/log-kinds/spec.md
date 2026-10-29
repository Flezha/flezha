# log-kinds Specification

## Purpose
The shared log core every durable controller log is a kind of, and the cluster-wide `Flezha.Service.Logs` surface and `flz logs` CLI every kind is read through.
## Requirements
### Requirement: Every durable log SHALL be a kind of one shared log core

The controller SHALL keep each durable, append-only log as a *kind* of one log core. A kind
SHALL be declared once, as data, naming: its id; its title and one-sentence description; its
directory; its config keys; how records reach it (`{:bus, allow_list}`, a sink on the
observability bus, or `:direct`, appended by the code that produces them); its write policy
(`:per_record`, `:group_commit` or `:interval` fsync, the bound on queued appends and the
append timeout); its rotation and retention limits; the record fields it can be filtered by;
and the columns a reader shows. The writer, rotation, retention sweep, index, reader and codec
SHALL be the same modules for every kind, started once per enabled kind under its own names.
No kind SHALL have a writer, reader or index of its own.

The kinds SHALL be `audit` (the audit log, fed from the bus) and `relay_history` (fed directly).

#### Scenario: Both kinds run on the same modules

- **WHEN** the controller boots with both kinds enabled
- **THEN** one writer, one rotation worker and one retention worker SHALL run per kind, each an
  instance of the shared log core modules, writing under the kind's own directory

#### Scenario: A kind declares its filters and columns

- **WHEN** the kind registry is read
- **THEN** each kind SHALL list its filter fields (field, label, type, and allowed values when
  the field is an enumeration) and its columns (field, label, type)

#### Scenario: A disabled kind starts nothing

- **WHEN** a kind's `enabled` key is false
- **THEN** no process SHALL start for it, no directory SHALL be created, and every read of it
  SHALL answer `logs.kind_disabled` naming the key that enables it

### Requirement: Every record SHALL carry a wall-clock time and the controller that wrote it

Every record the log core writes SHALL carry `ts_us`, microseconds of wall-clock (system)
time, and `node`, the controller that wrote it. The index SHALL record each file's first and
last `ts_us` and whether the file's times are wall-clock. A file written before this
requirement, whose `ts_us` is the writer's monotonic time, SHALL be read with its time taken
from the record's `ts_iso`, so old and new files order together.

#### Scenario: Two controllers' records merge in time order

- **WHEN** controller A writes a record at 10:00:00.100 and controller B one at 10:00:00.050
- **THEN** a newest-first read SHALL answer A's record before B's

#### Scenario: A file written before the change is ordered by its ISO time

- **WHEN** an audit file whose index entry is not marked wall-clock is read
- **THEN** each record's order key SHALL be its `ts_iso` converted to microseconds, and
  `since_us` / `until_us` SHALL be compared against that

### Requirement: Appends SHALL be group-committed and bounded

A kind written with `:per_record` or `:group_commit` SHALL answer an append only after the
record is on disk and synced, and SHALL serve every append queued at the same moment with one
write and one `fsync`. A kind written with `:interval` SHALL answer once the record is written
and sync within its interval. When a kind's queued appends reach its bound, a further append
SHALL be refused at once with `{:error, :overloaded}`, SHALL increment the kind's
`dropped_count`, and SHALL emit one `log_overloaded` event (kind, node) per second at most. An
append that waits longer than the kind's append timeout SHALL return `{:error, :timeout}` to
its caller and increment `timeout_count`; the record SHALL still be written if the writer
reaches it.

#### Scenario: Concurrent appends share one fsync

- **WHEN** 50 processes append to a `:group_commit` kind at the same moment
- **THEN** every append SHALL return `:ok` after its record is synced, and the writer SHALL have
  issued fewer `fsync` calls than appends

#### Scenario: A full queue refuses rather than blocks

- **WHEN** a kind's queue is at its bound and a process appends
- **THEN** the append SHALL return `{:error, :overloaded}` without waiting, `dropped_count`
  SHALL rise by one, and `stats` SHALL show it

### Requirement: The logs service SHALL describe every kind

`Flezha.Service.Logs.kinds(caller_context)` SHALL answer
`{:ok, [%{id, title, description, enabled, disabled_reason, filters, columns}]}` for every kind
the caller may read, where `filters` is `[%{field, label, type, values}]` (`type` one of
`text`, `enum`, `time`; `values` the allowed values of an `enum`, else nil) and `columns` is
`[%{field, label, type}]`. A reader SHALL be able to show a kind it has never seen from this
answer alone.

#### Scenario: A viewer lists the kinds

- **WHEN** a viewer calls `kinds`
- **THEN** the answer SHALL include `audit` and `relay_history` with their filters and columns,
  and `relay_history`'s `outcome` filter SHALL be an `enum` listing every outcome

### Requirement: A read SHALL answer newest first across the whole cluster

`Flezha.Service.Logs.read(params, caller_context)` SHALL take `kind`, `filters` (a map of
filter field to value, only fields the kind declares), `since_us`, `until_us`, `limit`
(default 100, at most 1,000) and `before` (the cursor a previous read answered), and SHALL
answer `{:ok, %{records: [...], next: cursor | nil, unreachable: [node]}}`. Records SHALL be
newest first, merged from every controller through the cluster read core, each carrying
`node`. `next` SHALL be nil when no older matching record exists on any answering controller.
`unreachable` SHALL name each controller that did not answer within the read bound, and the
read SHALL still answer what the others hold. Only records of the caller's namespace SHALL be
answered, except to `admin@_system`. A read SHALL NOT scan files whose index time range lies
wholly outside the window, SHALL read files newest first, and SHALL stop once `limit` records
are found. Optional `node` and `file` params SHALL restrict the read to one controller's one
file, answered oldest first, as the audit file view needs.

#### Scenario: The last ten runs of a factory

- **WHEN** a caller reads `kind: "relay_history"`, `filters: %{event: "ended", factory: "fan_trade"}`,
  `limit: 10`
- **THEN** the answer SHALL hold the ten newest `ended` records of `fan_trade` across every
  controller, newest first, and the read SHALL NOT have opened files older than the tenth
  record on any controller

#### Scenario: Paging back

- **WHEN** a caller passes the `next` cursor of a previous read
- **THEN** the answer SHALL start with the newest matching record older than the last record
  of the previous page, with no record repeated or skipped

#### Scenario: One controller is down

- **WHEN** one of three controllers does not answer
- **THEN** the read SHALL answer the other two controllers' records and name the third in
  `unreachable`

#### Scenario: A filter the kind does not declare is refused

- **WHEN** a caller filters `relay_history` by a field the kind does not declare
- **THEN** the read SHALL be refused with `logs.unknown_filter` naming the field and the
  fields the kind declares

### Requirement: A tail SHALL stream a kind from every controller

`Flezha.Service.Logs.tail(params, caller_context)` with `kind` and `filters` SHALL stream each
newly written matching record of the caller's namespace from every controller, each carrying
`node`, until the caller ends it, the window closes or the watched process exits, exactly as
the audit tail ends today. A controller that joins while a tail is open SHALL be included.

#### Scenario: A record written on another controller reaches the tail

- **WHEN** a console connected to controller A tails `relay_history` and a relay ends on
  controller B
- **THEN** the tail SHALL deliver B's `ended` record carrying `node` B

### Requirement: Files, stats and rotation SHALL answer per controller

`Flezha.Service.Logs.files(%{kind}, ctx)` SHALL answer
`{:ok, %{files: [%{node, filename, first_ts_us, last_ts_us, record_count, size_bytes, sha256}],
unreachable}}`. `stats(%{kind}, ctx)` SHALL answer `{:ok, %{controllers: [%{node, current_file,
current_file_bytes, total_bytes, total_files, oldest_ts_us, record_count, dropped_count,
timeout_count}], unreachable}}`. `rotate(%{kind, node}, ctx)` SHALL rotate the kind on the named
controller, or on every controller when `node` is omitted, and answer
`{:ok, %{rotated: [%{node, rotated_to}], unreachable}}`. `rotate` SHALL require `ops`; the reads
SHALL require `viewer`.

#### Scenario: Stats of a three-controller cluster

- **WHEN** a caller asks `stats` of `relay_history` in a three-controller cluster
- **THEN** the answer SHALL hold one row per controller, each naming its `node`

### Requirement: Reads of the audit kind SHALL stay audited; other kinds SHALL NOT

Every call of the logs service on the `audit` kind SHALL be recorded in the audit log exactly
as audit reads are today. Calls on any other kind SHALL be authorized but SHALL NOT write an
audit record, so a screen that reads relay history does not fill the security trail.

#### Scenario: Reading relay history writes no audit record

- **WHEN** a caller reads `relay_history`
- **THEN** no record SHALL be appended to the audit kind for that read

#### Scenario: Reading the audit log is audited

- **WHEN** a caller reads the `audit` kind
- **THEN** one audit record SHALL name the read, its caller and its request id

### Requirement: `flz logs` SHALL read every kind

The CLI SHALL provide `flz logs kinds`, `flz logs read <kind>`, `flz logs tail <kind>`,
`flz logs files <kind>`, `flz logs stats <kind>` and `flz logs rotate <kind>`, with a filter
option per declared filter field, `--since`, `--until`, `--limit`, `--before`, `--node` and
`--file`. `flz audit <verb>` SHALL be a spelling of `flz logs <verb> audit`, resolved in the
verb table with no implementation of its own.

#### Scenario: Reading a factory's last runs from the terminal

- **WHEN** the user runs `flz logs read relay_history --event ended --factory fan_trade --limit 10`
- **THEN** the CLI SHALL print the ten newest matching records, newest first, each with its
  controller

#### Scenario: The audit spelling still works

- **WHEN** the user runs `flz audit read --source platform_authz --limit 5`
- **THEN** the CLI SHALL answer exactly what `flz logs read audit --source platform_authz
  --limit 5` answers

