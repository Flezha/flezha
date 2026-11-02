# observability-logs Specification

## Purpose

The controller's durable logs. Every durable log is a kind of one shared log core: records carry a
wall-clock time and the writing controller, appends are group-committed and bounded, files rotate
and are swept by size and age, and the cluster-wide `Flezha.Service.Logs` surface and `flz logs`
read every kind newest first, tail it from every controller, and answer files, stats and rotation
per controller. The audit kind is an append-only sink of the signals on an allow-list, such as
service calls, shutdown phases and plugin package loads, whose own reads are audited. The relay
history kind records each relay run's start and end once, with its origin, outcome and duration,
without stalling a relay, and answers whether a run already happened.

## Requirements

### Requirement: Durable append-only audit sink

The system SHALL keep the audit log as the `audit` kind of the shared log core (see
`observability-logs`), fed by a Sink that implements the foundation's `Flezha.Observability.Sink`
behaviour and writes one durable record per accepted signal to an append-only file on disk
under the configured audit directory. The sink SHALL use the foundation's `:block` overflow
policy. The sink SHALL register with the foundation Registry only when the `:audit_log,
:enabled` config flag is `true`, AND when the master `:observability, :enabled` flag is also
`true`.

#### Scenario: Sink off by default
- **WHEN** the application boots with default configuration
- **THEN** `Flezha.Observability.Registry.list_sinks/0` SHALL NOT include a sink named `:audit_log`, no `storage/app/audit/` directory SHALL be created, and `Flezha.Service.Logs.stats(%{kind: "audit"}, ctx)` SHALL return `{:error, %Flezha.Error{code: "audit.disabled"}}`

#### Scenario: Sink registers when enabled
- **WHEN** the application boots with `:audit_log, :enabled: true`
- **THEN** `Flezha.Observability.Registry.list_sinks/0` SHALL include a sink named `:audit_log` with overflow policy `:block`, AND `storage/app/audit/audit-current.jsonl` SHALL exist (possibly empty)

#### Scenario: Master observability flag transitively disables audit
- **WHEN** the application boots with `:observability, :enabled: false` AND `:audit_log, :enabled: true`
- **THEN** the audit kind SHALL NOT start, no sink SHALL register, no disk I/O SHALL occur for it, and every `Flezha.Service.Logs` call on the `audit` kind SHALL return `{:error, %Flezha.Error{code: "observability.disabled"}}` (master flag wins)

### Requirement: Configurable subscription allow-list

The sink's `(source, name | :any)` allow-list SHALL decide what it receives: the sink's bus
filter SHALL name exactly the sources its allow-list names, so the bus never hands it a signal
of another source, and the handler SHALL apply the `name` part. Signals not matching the
allow-list SHALL be discarded without being written. The default allow-list SHALL be derived,
not written by hand: it SHALL include `{:flezha_shutdown, :any}`, `{:flezha_plugin_packages,
:any}`, `{:platform_authz, :any}`, and `{source, :any}` for every audit source the user-verb
declaration table names (at minimum `:observability_service` and `:audit_log`), so that a verb
declared with an audit reaches the durable log by construction and no verb's audit can be
volatile. Users SHALL be able to extend it via `:audit_log, :extra_subscriptions` runtime
config.

#### Scenario: Default allow-list captures observability-service audit events
- **WHEN** the audit subsystem is enabled and a caller invokes `Flezha.Service.Observability.disable_source/2`
- **THEN** within one second a JSON Lines record SHALL appear in `audit-current.jsonl` with `source: "observability_service"` and `name: "disable_source"`

#### Scenario: Default allow-list captures shutdown phase events
- **WHEN** the graceful shutdown coordinator emits a `:phase_completed` `:event` on `:flezha_shutdown`
- **THEN** a record SHALL appear in `audit-current.jsonl` with `source: "flezha_shutdown"` and `name: "phase_completed"`

#### Scenario: Default allow-list captures the packages a boot loaded and refused
- **WHEN** the controller boots with the audit subsystem enabled and one package it loads and one it refuses
- **THEN** `audit-current.jsonl` SHALL hold a record with `source: "flezha_plugin_packages"` and `name: "loaded"` for the first, and one with `name: "refused"` carrying the refusal code for the second

#### Scenario: Default allow-list captures auth-state mutations
- **WHEN** the platform-authz subsystem emits any `:event` on `:platform_authz` (token issue, role bind, namespace create, bootstrap completion, authorization denial)
- **THEN** within one second a JSON Lines record SHALL appear in `audit-current.jsonl` with `source: "platform_authz"` carrying the verified `caller.principal`

#### Scenario: Non-allow-listed signal is discarded
- **WHEN** a signal arrives at the sink whose `(source, name)` pair is not in the allow-list AND does not match an `:any` wildcard for that source
- **THEN** no record SHALL be written for that signal AND the file size SHALL NOT change

#### Scenario: A signal of a source outside the allow-list never reaches the sink
- **WHEN** a relay emits a `:metric` on a source the allow-list does not name
- **THEN** the bus SHALL NOT call the audit sink for it, and the emitting process SHALL NOT wait on the sink

#### Scenario: User-extended allow-list is honoured
- **WHEN** the application boots with `:audit_log, :extra_subscriptions: [{:my_source, :critical}]` and a signal `(source: :my_source, name: :critical)` is emitted
- **THEN** a record for that signal SHALL appear in `audit-current.jsonl`

#### Scenario: A declared audit source is durable by construction
- **WHEN** an administration verb declared with an audit on a source (a `schedule add` on `:schedule_service`, a `dlq drop` on `:dlq_service`) succeeds with the audit subsystem enabled and no `:extra_subscriptions` configured
- **THEN** within one second exactly one record for it SHALL appear in `audit-current.jsonl`, carrying `metadata.action`, `metadata.outcome`, the verified `caller.principal` and the resolved `namespace`

#### Scenario: One record shape for a user's act
- **WHEN** the same verb is audited after an invocation from the terminal and after one from the published service API
- **THEN** both records SHALL carry the same `source`, `name` and metadata keys, differing only in `caller.source` and the actor

### Requirement: JSON Lines record format with stable field order

Each record SHALL be a single UTF-8 JSON object on its own line, terminated by `\n`. The top-level fields SHALL appear in this exact order: `ts_us`, `ts_iso`, `source`, `name`, `measurements`, `metadata`, `caller`, `namespace`, `node`. `ts_us` SHALL be wall-clock microseconds and `ts_iso` the same instant. `node` SHALL be the controller that wrote the record. The `caller` field SHALL be denormalised from `metadata.caller_source`, `metadata.principal`, and `metadata.request_id` so a reader does not need to know the metadata convention to identify the actor. When `:authn_authz, :enabled` is `true` the `caller.principal` value SHALL be the verified principal bound by the authentication layer; when `false` the `caller.principal` value SHALL be `null` and SHALL NOT carry any caller-supplied placeholder. The `namespace` field SHALL be the resolved namespace from `caller_context.namespace`; for pre-`security-access-control` records or signals without a namespace context the field SHALL be `"_system"`.

#### Scenario: Single-line record for a signal
- **WHEN** the sink writes a signal with `source: :observability_service`, `name: :enable_source`, `metadata: %{caller_source: :cli, request_id: "req-1"}`
- **THEN** the written line SHALL be valid JSON, SHALL end with `\n`, SHALL contain top-level fields in the order `ts_us, ts_iso, source, name, measurements, metadata, caller, namespace, node`, AND `caller.source` SHALL equal `"cli"` AND `caller.request_id` SHALL equal `"req-1"`

#### Scenario: Verified principal lands in caller.principal
- **WHEN** `:authn_authz, :enabled` is `true` AND a signal is written whose `metadata.principal` was bound by the authentication layer
- **THEN** the written record's `caller.principal` SHALL equal that verified principal as a JSON string

#### Scenario: Authn-disabled principal renders as the caller-source atom
- **WHEN** `:authn_authz, :enabled` is `false` AND the signal's metadata contains no verified `:principal`
- **THEN** the written record's `caller.principal` SHALL be the caller-source atom (`"cli"`, `"ui"`, `"sdk"`, or `"internal"`) so existing records remain attributable (preserving pre-change behaviour, per the security-access-control compatibility-mode requirement)

#### Scenario: Namespace defaults to _system
- **WHEN** a signal is written whose `caller_context.namespace` is unset
- **THEN** the written record's `namespace` field SHALL equal `"_system"`

#### Scenario: ts_iso is UTC ISO-8601 with millisecond precision
- **WHEN** any record is written
- **THEN** its `ts_iso` field SHALL match the regex `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$`

#### Scenario: ts_us is wall-clock time
- **WHEN** a record is written at wall-clock time T
- **THEN** its `ts_us` SHALL equal T in microseconds since the Unix epoch, within the writer's clock resolution, and SHALL agree with `ts_iso` to the millisecond

### Requirement: Fsync semantics

The audit kind SHALL support two `:fsync_mode` values. In `:per_record` (default) every caller's record SHALL be written and synced with `:file.sync/1` before the writer answers that caller; records queued at the same moment SHALL share one write and one sync (group commit). In `:batched` the writer SHALL answer once the record is written and call `:file.sync/1` no less frequently than every `:fsync_interval_ms` (default `100`). In either mode the writer SHALL never answer a signal until the record is durably written (`:per_record`) or buffered in the OS page cache (`:batched`).

#### Scenario: Per-record fsync default
- **WHEN** the audit subsystem is enabled with default config
- **THEN** every signal the sink accepts SHALL be synced to disk before the sink answers the producer

#### Scenario: Concurrent records share a sync
- **WHEN** ten producers' signals reach the writer at the same moment under `:per_record`
- **THEN** each producer SHALL be answered only after its record is synced, and the writer SHALL have issued fewer than ten syncs

#### Scenario: Batched fsync flushes within configured interval
- **WHEN** the audit subsystem is enabled with `:fsync_mode: :batched, :fsync_interval_ms: 50`
- **THEN** any record written SHALL be fsynced to disk no later than `50` milliseconds after acceptance

### Requirement: Size + age file rotation

The writer SHALL rotate the active file when either the active file size reaches `:max_file_bytes` (default `67_108_864` — 64 MiB) OR the active file age reaches `:max_file_age_seconds` (default `86_400` — 1 day), whichever comes first. On rotation the active file SHALL be renamed `audit-YYYYMMDDTHHMMSSZ-N.jsonl` where the timestamp is the rotation moment in UTC and `-N` is a 1-based monotonic counter to disambiguate sub-second rotations. A new empty `audit-current.jsonl` SHALL be opened immediately. An `:audit_rotated` `:event` SHALL be emitted on the `:audit_log` source carrying the rotated filename and the active-file size at rotation.

#### Scenario: Rotation on size
- **WHEN** the active file reaches `:max_file_bytes` after a record write
- **THEN** the file SHALL be rotated immediately, a new `audit-current.jsonl` SHALL be opened, AND an `:audit_rotated` event SHALL be emitted within one second

#### Scenario: Rotation on age
- **WHEN** the active file has existed for `:max_file_age_seconds` since its creation
- **THEN** the file SHALL be rotated on the next periodic check (no later than `:rotation_check_interval_ms`, default `60_000`), AND an `:audit_rotated` event SHALL be emitted

#### Scenario: Suffix monotonicity within a second
- **WHEN** two rotations occur in the same UTC second
- **THEN** the second rotated file SHALL be named with a `-N+1` suffix where `-N` is the prior rotation's suffix value

### Requirement: Size + age retention sweep

The retention sweep SHALL run every `:retention_check_interval_ms` (default `3_600_000` — 1 hour). It SHALL delete rotated files older than `:retention_seconds` (default `2_592_000` — 30 days) AND SHALL delete the oldest rotated files until the total bytes under the audit directory are below `:max_total_bytes` (default `4_294_967_296` — 4 GiB), whichever bound is more restrictive. The active file SHALL NEVER be deleted by the retention sweep regardless of age or size. An `:audit_retention_sweep` `:event` SHALL be emitted each run on the `:audit_log` source carrying `deleted_count`, `deleted_bytes`, `remaining_count`, and `total_bytes`.

#### Scenario: Age-based deletion
- **WHEN** the sweep runs and a rotated file is older than `:retention_seconds`
- **THEN** that file SHALL be deleted, the `INDEX` SHALL be rewritten to omit it, and the sweep event's `deleted_count` SHALL include it

#### Scenario: Size-based deletion
- **WHEN** the sweep runs and the directory's total bytes exceed `:max_total_bytes`
- **THEN** the OLDEST rotated files SHALL be deleted until the total is at or below the cap

#### Scenario: Active file is never deleted by retention
- **WHEN** the sweep runs and `audit-current.jsonl` is older than `:retention_seconds`
- **THEN** the active file SHALL NOT be deleted

### Requirement: INDEX catalog and rebuild

The system SHALL maintain a small binary catalog `storage/app/audit/INDEX` containing one entry per rotated file with the filename, first record ts (microseconds, monotonic), last record ts, record count, and SHA-256 hex digest of the file content at rotation time. The catalog SHALL be rewritten atomically (write `.tmp`, rename over `INDEX`) on every rotation. On boot, if `INDEX` is missing or `:erlang.binary_to_term/1` fails, the writer SHALL rebuild it by walking the audit directory and reading every rotated file.

#### Scenario: Catalog is created on first rotation
- **WHEN** the first rotation occurs after a fresh enable
- **THEN** `storage/app/audit/INDEX` SHALL exist AND a `flz audit list` SHALL return one entry

#### Scenario: Catalog rebuild on missing INDEX
- **WHEN** the application boots and `storage/app/audit/` contains rotated files but no `INDEX` file
- **THEN** the writer SHALL rebuild `INDEX` from the on-disk files before accepting any signals AND `flz audit list` SHALL show every rotated file

#### Scenario: Catalog rebuild on corrupt INDEX
- **WHEN** `INDEX` exists but its content does not decode via `:erlang.binary_to_term/1`
- **THEN** the writer SHALL log a single warning, rebuild the catalog from the on-disk files, and overwrite the corrupt `INDEX`

### Requirement: Service-API surface

The audit log SHALL be read through `Flezha.Service.Logs` with `kind: "audit"` (see
`observability-logs`), callable from both the CLI and the console directly.
<!-- docs-citation-record:begin -->`Flezha.Service.AuditLog`<!-- docs-citation-record:end -->
SHALL NOT exist. Every public function SHALL accept a `caller_context` argument with the same
shape and validation as `Flezha.Service.Observability` (`source` in `[:cli, :ui, :sdk,
:internal]`). When `:authn_authz, :enabled` is `true` every public function SHALL authorize
before any side effect. Reads (`kinds`, `read`, `tail`, `files`, `stats`) SHALL require at
least `viewer`; `rotate` SHALL require at least `ops`. Reads SHALL answer only records whose
`namespace` field matches the caller's bound namespace, except that `admin@_system` SHALL see
all records across all namespaces. Reads SHALL cover every controller in the cluster.

#### Scenario: list_files returns catalog entries
- **WHEN** the caller invokes `Flezha.Service.Logs.files(%{kind: "audit"}, ctx)` with a viewer context in a cluster of two controllers holding three rotated files each
- **THEN** the call SHALL return `{:ok, %{files: [...], unreachable: []}}` with six entries each carrying `node`, `filename`, `first_ts_us`, `last_ts_us`, `record_count`, `size_bytes`, and `sha256`

#### Scenario: read returns records with filter pushdown
- **WHEN** the caller invokes `read(%{kind: "audit", node: n, file: "audit-...jsonl", filters: %{source: "observability_service"}, since_us: T0, limit: 100}, ctx)`
- **THEN** the call SHALL return up to `100` records from the named file on controller `n` whose `source` equals `"observability_service"` AND whose `ts_us >= T0`

#### Scenario: Viewer in team-orders sees only team-orders records
- **WHEN** `:authn_authz, :enabled` is `true` AND a viewer bound to `team-orders` reads the audit kind holding records from both `team-orders` and `team-payments`
- **THEN** the returned list SHALL contain only records whose `namespace` field is `"team-orders"`

#### Scenario: admin@_system sees all namespaces
- **WHEN** `admin@_system` reads the audit kind holding records from multiple namespaces
- **THEN** the returned list SHALL contain records from every namespace

#### Scenario: Viewer cannot rotate
- **WHEN** `:authn_authz, :enabled` is `true` AND a viewer invokes `rotate(%{kind: "audit"}, ctx)`
- **THEN** the call SHALL return `{:error, %Flezha.Error{code: "authz.role_insufficient", class: :denied}}` AND no rotation SHALL occur

#### Scenario: tail streams live records
- **WHEN** the caller invokes `tail(%{kind: "audit", filters: %{}}, ctx)` and a record is subsequently written on any controller
- **THEN** the caller SHALL receive a streaming message carrying the new record and its `node`

#### Scenario: stats returns operational snapshot
- **WHEN** the caller invokes `stats(%{kind: "audit"}, ctx)`
- **THEN** the call SHALL return `{:ok, %{controllers: [...], unreachable: [...]}}` whose rows carry at minimum `node`, `current_file`, `current_file_bytes`, `total_bytes`, `total_files`, `oldest_ts_us`, `record_count`, `dropped_count`

#### Scenario: rotate forces immediate rotation
- **WHEN** an `ops` caller invokes `rotate(%{kind: "audit", node: n}, ctx)`
- **THEN** controller `n`'s active audit file SHALL be rotated immediately AND the call SHALL return `{:ok, %{rotated: [%{node: n, rotated_to: filename}], unreachable: []}}` AND a fresh empty active file SHALL be opened

#### Scenario: Missing caller_context source is rejected
- **WHEN** any public function is invoked with `caller_context: %{}`
- **THEN** the call SHALL return `{:error, %Flezha.Error{code: "authz.invalid_caller_context"}}`

#### Scenario: Master flag disabled returns disabled error
- **WHEN** any public function is invoked on the audit kind while the master observability flag is `false`
- **THEN** the call SHALL return `{:error, %Flezha.Error{code: "observability.disabled"}}`

#### Scenario: Audit subsystem disabled returns disabled error
- **WHEN** any public function is invoked on the audit kind while the master flag is `true` but `:audit_log, :enabled: false`
- **THEN** the call SHALL return `{:error, %Flezha.Error{code: "audit.disabled"}}`

### Requirement: `flz audit` CLI verb family

`flz audit list | tail | read [<file>] | stats | rotate` SHALL remain available as spellings of
`flz logs files audit`, `flz logs tail audit`, `flz logs read audit`, `flz logs stats audit`
and `flz logs rotate audit` (see `observability-logs`), resolved in the CLI verb table with no
implementation of their own. Text output SHALL be the default; `--json` SHALL render
machine-parseable output (newline-delimited JSON for `tail` and `read`).

#### Scenario: `flz audit list` shows catalog
- **WHEN** the user runs `flz audit list`
- **THEN** one row per catalog entry of every controller SHALL be printed with at minimum controller, filename, first ts, last ts, record count, size

#### Scenario: `flz audit read` applies filters
- **WHEN** the user runs `flz audit read audit-20260518T071455Z-1.jsonl --node flezha@a --source observability_service --since 1h --limit 50`
- **THEN** the command SHALL print up to `50` records of that file matching the filters, one per line, sorted by `ts_us` ascending

#### Scenario: `flz audit read` without a file reads the cluster newest first
- **WHEN** the user runs `flz audit read --source platform_authz --limit 20`
- **THEN** the command SHALL print the 20 newest matching records of every controller, newest first, each naming its controller

#### Scenario: `flz audit tail` streams live
- **WHEN** the user runs `flz audit tail --json` and a record is subsequently written on any controller
- **THEN** the command SHALL print the record as a single JSON line carrying its `node`

#### Scenario: `flz audit rotate` forces rotation
- **WHEN** the user runs `flz audit rotate`
- **THEN** the command SHALL print the new rotated filename of each controller on success, exit zero, AND the next `flz audit list` SHALL include the new entries

#### Scenario: `flz audit stats` shows snapshot
- **WHEN** the user runs `flz audit stats`
- **THEN** the command SHALL print, per controller, current file path, current file size, total bytes, total file count, oldest record ts, record count, and drop count

#### Scenario: `flz audit *` against disabled audit emits clear error
- **WHEN** the user runs any `flz audit *` verb while audit is disabled
- **THEN** the command SHALL exit with non-zero status and a stderr message naming the disabled flag and how to enable it (`FLEZHA_AUDIT_ENABLED=true` or `:audit_log, :enabled: true`)

### Requirement: Audit reads are themselves auditable

Every call of `Flezha.Service.Logs` on the `audit` kind SHALL emit an `:event` signal on the `:audit_log` source recording the called function name, the caller `:source`, and the optional `:request_id`. The emit SHALL happen synchronously inside the service call so the record reaches the sink in the same logical tasklet.

#### Scenario: `tail` invocation is audited
- **WHEN** the caller invokes `tail(%{kind: "audit"}, ctx)` with a `caller_context` of `%{source: :cli, request_id: "r"}`
- **THEN** within one second a record SHALL appear in `audit-current.jsonl` with `source: "audit_log"`, `name: "tail"`, `caller.source: "cli"`, `caller.request_id: "r"`

#### Scenario: `read` invocation is audited
- **WHEN** the caller invokes `read(%{kind: "audit"}, ctx)`
- **THEN** a `:read` audit event SHALL be recorded

### Requirement: Gzipped rotated files are readable

The Reader SHALL transparently support reading rotated files whose names end in `.jsonl.gz`. The Reader SHALL detect the suffix and stream-decompress via `:zlib` before line-splitting. The writer itself SHALL NOT compress rotated files — users that wish to compress old files do so out-of-band.

#### Scenario: Reading a gzipped rotated file
- **WHEN** the user manually `gzip`s a rotated file and then runs `flz audit read audit-...-1.jsonl.gz --limit 10`
- **THEN** the command SHALL return the first `10` records as if the file were uncompressed

#### Scenario: Catalog tracks gzipped files
- **WHEN** the user `gzip`s a rotated file in-place and the next retention sweep runs
- **THEN** the `INDEX` SHALL be updated to reflect the new filename (`.jsonl.gz` suffix) AND `flz audit list` SHALL show it

### Requirement: Runtime configuration knobs and env-var overrides

The system SHALL expose a runtime configuration map `:audit_log` with keys `:enabled` (bool), `:dir` (binary path), `:max_file_bytes` (positive integer), `:max_file_age_seconds` (positive integer), `:retention_seconds` (positive integer), `:max_total_bytes` (positive integer), `:fsync_mode` (`:per_record | :batched`), `:fsync_interval_ms` (positive integer), `:rotation_check_interval_ms` (positive integer), `:retention_check_interval_ms` (positive integer), `:extra_subscriptions` (list of `{atom(), atom() | :any}` tuples). Env-var overrides `FLEZHA_AUDIT_ENABLED`, `FLEZHA_AUDIT_DIR`, `FLEZHA_AUDIT_RETENTION_DAYS` SHALL set the corresponding keys at runtime-config evaluation time.

#### Scenario: Env var enables audit
- **WHEN** the application is booted with `FLEZHA_AUDIT_ENABLED=true` and no other audit config
- **THEN** `Flezha.Observability.Registry.list_sinks/0` SHALL include `:audit_log` AND `storage/app/audit/audit-current.jsonl` SHALL exist

#### Scenario: Env var redirects directory
- **WHEN** the application is booted with `FLEZHA_AUDIT_ENABLED=true FLEZHA_AUDIT_DIR=/var/log/flezha`
- **THEN** `/var/log/flezha/audit-current.jsonl` SHALL be created AND `storage/app/audit/` SHALL NOT be created

#### Scenario: Test config disables audit by default
- **WHEN** tests run via `mix test`
- **THEN** `:audit_log, :enabled` SHALL be `false` so no disk I/O occurs unless a test explicitly opts in

### Requirement: Shutdown coordinator closes the writer cleanly

The graceful-shutdown coordinator SHALL close every enabled kind's writer (with a bounded timeout) during its finalise phase BEFORE the foundation's `FsyncBatcher` flush. The close SHALL drain any in-flight queued appends, fsync the active file, and close the file descriptor. Any signal or append that arrives AFTER close SHALL be discarded with a per-kind counter increment but SHALL NOT crash the producer.

#### Scenario: Clean close on shutdown
- **WHEN** the graceful shutdown coordinator drives `finalise` and the audit subsystem is enabled
- **THEN** the active file SHALL be fsynced and its FD closed before the foundation's `FsyncBatcher` flush, AND no records SHALL be lost from before the close

#### Scenario: Post-close signals are discarded
- **WHEN** a signal arrives at the sink after the writer's close has returned
- **THEN** the signal SHALL be discarded, a per-sink `:post_close_dropped` counter SHALL increment, AND no exception SHALL propagate to the producer

### Requirement: A record the audit log produced SHALL never raise when the audit log writes it

Metadata SHALL be rendered by one rule for every sink, and a binary that is not
valid UTF-8 SHALL be rendered rather than passed through to an encoder that raises
on it. The audit log SHALL NOT be able to fail on a record it produced itself.

#### Scenario: A relay's metadata carries a non-UTF-8 binary

- **WHEN** a signal's metadata holds a binary for which `String.valid?/1` is false
- **THEN** the shared typed rule SHALL render it the way the text renderer already
  does, and the JSON encoder SHALL NOT be handed a value it cannot encode

#### Scenario: The text and JSON renderings agree on what a binary is

- **WHEN** the same metadata is rendered for the text sink and for the audit log
- **THEN** neither SHALL accept a value the other refuses

### Requirement: A failed act SHALL record its error code

The audit record of an authorized act that failed SHALL carry `metadata.error_code`, the catalogue code of the failure, beside `outcome: "error"`. This holds whether the act returned the failure or a `flz` verb refused through the CLI. The `authorization_denied` record's `reason` SHALL be the `authz.*` code of the denial.

#### Scenario: A failed delete names why

- **WHEN** an authorized caller deletes a namespace `ghost` that does not exist
- **THEN** the audit record SHALL carry `outcome: "error"` and `error_code: "namespace.not_found"`

#### Scenario: A denial names its code

- **WHEN** a caller whose role does not grant an action is refused
- **THEN** the `authorization_denied` record's `reason` SHALL be `authz.role_insufficient`

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

### Requirement: The relay history SHALL record each run's start and end once

The controller SHALL append to the `relay_history` kind:

- one `started` record when a relay instance is deliberately started — by an operator, a
  partition group start, a schedule slot or manual run, a tasklet's spawn, a relay-to-relay
  call that creates its callee, or a member a rescale adds;
- one `ended` record when that instance ends for good — completion, a cooperative or hard
  stop, a stop of a halted instance, a namespace deletion, or a member a rescale removes;
- one `start_failed` record when a deliberate start fails after the controller accepted it.

A crash restart, a failover to another controller, a cold boot, a move, a sleep, a wake, a
rebind and a rescale's restart of a member it keeps SHALL write nothing. A start answered as a
duplicate of a live instance SHALL write nothing. The relay history SHALL NOT depend on the
observability subsystem being enabled.

#### Scenario: A spawned run leaves two records

- **WHEN** a tasklet spawns a steps relay that completes 40 seconds later
- **THEN** the relay history SHALL hold exactly one `started` and one `ended` record for it

#### Scenario: A crash restart is not a new run

- **WHEN** a relay's Overwatch crashes and its supervisor restarts it
- **THEN** no `started` or `ended` record SHALL be written for the restart

#### Scenario: A failover is not a new run

- **WHEN** the controller hosting a relay dies and the relay is restarted on another
- **THEN** no record SHALL be written for the move, and the relay's eventual `ended` record
  SHALL carry the `first_started_at` of its original start

#### Scenario: The convergence loop's restart of an unplaced relay is not a start

- **WHEN** the convergence loop starts a declared relay that has run nowhere past its grace
- **THEN** no `started` record SHALL be written and no `relay.started` event SHALL be emitted

#### Scenario: A run-once relay that ends before its start is read

- **WHEN** a steps relay completes before the starter reads its binding
- **THEN** its `started` record SHALL still be written, before or after its `ended` record,
  and both SHALL exist

### Requirement: Every history record SHALL stand alone

Every record SHALL carry `ts_us`, `node`, `event`, `namespace`, `relay`, `factory`, `run`
(`flow` or `steps`), `origin` and `first_started_at`, and `spawn_identity` when the instance
was spawned with one. An `ended` record SHALL also carry `outcome`, `halted`, `duration_ms`
(from `first_started_at`) and `error` (at most 512 bytes of text, empty when none). A
`start_failed` record SHALL carry `error`. A reader SHALL NOT need the matching `started`
record to show an `ended` one.

#### Scenario: An ended record without its start

- **WHEN** retention has deleted the file holding a run's `started` record
- **THEN** its `ended` record SHALL still name its factory, origin, start time, outcome and
  duration

### Requirement: The origin SHALL name who or what started the run

`origin` SHALL be a map with `kind` and the fields the kind knows:

- `operator`: `principal`, `source` (`cli`, `ui`, `http`, `mcp`, `sdk`), `request_id`;
- `group`: as `operator`, for a member of a partition group start;
- `schedule`: `schedule` (its name) and `slot`, or `slot: "manual"` with the manual run's
  `principal` and `source`;
- `spawn`: `parent_relay` when the requesting runner named one and the controller verified it,
  and the runner's `principal`;
- `call`: `parent_relay`, the calling relay;
- `rescale`: `principal` and `source` of the rescale's caller;
- `reconcile`: reserved for a start the controller makes on its own.

The origin SHALL be stamped by the controller at start into the relay's declared spec, with
`first_started_at`, so it survives restarts, failovers and moves. A start option named
`origin` or `first_started_at` written by a caller SHALL be discarded.

#### Scenario: An operator start

- **WHEN** principal `alice` starts a relay from the console
- **THEN** its `started` record's origin SHALL be `%{kind: "operator", principal: "alice",
  source: "ui", request_id: ...}`

#### Scenario: A forged origin is ignored

- **WHEN** a caller starts a relay with `opts: %{"origin" => %{"kind" => "schedule"}}`
- **THEN** the start SHALL proceed with the origin the controller derives from the caller, and
  the forged value SHALL NOT appear in any record

#### Scenario: A spawned relay names its parent

- **WHEN** relay `ma_fan` running on runner R spawns a `fan_trade` instance and R's SDK sends
  `parent_relay_id: "ma_fan"`
- **THEN** the `started` record's origin SHALL be `%{kind: "spawn", parent_relay: "ma_fan", ...}`

### Requirement: The outcome SHALL say how the run ended

An `ended` record's `outcome` SHALL be:

- `completed` when a steps run reported `ok` or a flow instance reported `completed`;
- `undone` when a steps run reported `error` and its runtime reported `undo: "complete"`;
- `failed` when a steps run reported `error` otherwise; when the runtime reported
  `undo: "partial"` the `error` SHALL end with `; rollback incomplete`;
- `stopped` when the instance ended on a stop someone requested — a cooperative stop that
  reported `cancelled`, a hard stop, a stop of a halted instance or a namespace deletion;
- `timed_out` when the instance reported `cancelled` and no stop was requested (its
  `max_duration` elapsed);
- `rescaled_away` when a rescale removed the member;
- `cancelled` when an instance reported `cancelled` for any other reason.

`halted` SHALL be true when the instance was halted at the moment it ended.

#### Scenario: A rolled-back run

- **WHEN** a `fan_trade` instance with `on_failure: undo_all` fails and every compensate hook ran
- **THEN** its `ended` record SHALL carry `outcome: "undone"` and the run's error

#### Scenario: A run that hit its time limit

- **WHEN** a steps instance's `max_duration` elapses and it ends `cancelled`
- **THEN** its `ended` record SHALL carry `outcome: "timed_out"`

#### Scenario: An operator stop

- **WHEN** an operator stops a running steps instance and it ends `cancelled`
- **THEN** its `ended` record SHALL carry `outcome: "stopped"`

### Requirement: Writing the history SHALL never stall a relay

The `started` and `start_failed` records SHALL be appended by the process that started the
relay, never inside a call the Overwatch is serving. The `ended` record SHALL be appended by
the Overwatch's first teardown step, which no budget overrun can skip, and only when the
Overwatch leaves for good (a stop, a completion or a namespace deletion; not a failed start,
not a move, not a rescale's restart of a member it keeps). The `relay_history` kind SHALL use
the `:group_commit` write policy with a queue bound (`relay_history_max_pending`, default
10,000) and an append timeout (`relay_history_append_ms`, default 1,000 ms). The
control channel SHALL NOT append.

#### Scenario: A slow disk does not fail a start

- **WHEN** the history writer takes three seconds to sync while a relay starts
- **THEN** the start SHALL succeed within its own bound, and the `started` record SHALL be
  written or counted as a timeout, never turn the start into a failed one

#### Scenario: A saturated history is counted

- **WHEN** appends arrive faster than the disk syncs and the queue reaches its bound
- **THEN** further appends SHALL be refused and counted in `dropped_count`, and a
  `log_overloaded` event SHALL name the kind and controller

### Requirement: The history SHALL answer whether a run already happened

The relay history SHALL answer `Flezha.Logs.RelayHistory.seen(namespace, key)`, with `key`
either `{:relay, id}` or `{:spawn_identity, identity}`, with `{:ended, relay_id, ts_us}`,
`{:started, relay_id, ts_us}`, `:none`, `{:unknown, unreachable_nodes}` or `:disabled`. Each
controller SHALL hold an in-memory index of its own records within the dedup window
(`relay_history_dedup_window_hours`, default 24) and at most `relay_history_dedup_max_entries`
(default 2,000,000) keys, rebuilt from its own files at boot before the kind accepts appends.
A lookup SHALL ask the local index first and, on a miss, every other controller through the
cluster read core; `:none` SHALL be answered only when every controller answered. When the
index is at its cap the oldest keys SHALL be evicted and `dedup_evicted_count` incremented.

#### Scenario: A re-run of the parent finds its child

- **WHEN** a parent re-runs a spawn whose identity started a child that has since ended on
  another controller
- **THEN** `seen(ns, {:spawn_identity, identity})` SHALL answer `{:ended, child_id, ts}` or
  `{:started, child_id, ts}` naming that child

#### Scenario: A controller does not answer

- **WHEN** the local index misses and one controller does not answer the lookup
- **THEN** `seen` SHALL answer `{:unknown, [that_node]}` and never `:none`

#### Scenario: The index survives a restart

- **WHEN** a controller restarts
- **THEN** keys of records it wrote inside the dedup window SHALL be found again before it
  accepts its first append
