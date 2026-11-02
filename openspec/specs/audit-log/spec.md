# audit-log Specification

## Purpose

The audit-log capability provides a durable, append-only observability sink on top of the foundation bus. It is a Sink subscribed to a configurable allow-list of `(source, name)` pairs — by default capturing every `:event` signal on the foundation's `:observability_service` audit seam (subscribe / enable / disable calls), every `:event` on `:flezha_shutdown` (graceful shutdown phase boundaries), and every `:event` on `:flezha_plugin_packages` (which plugin packages each boot loaded and refused). The sink writes one structured JSON Lines record per accepted signal to disk under `storage/app/audit/`, with a stable top-level field order so downstream readers do not need to know the metadata convention to identify the actor. The subsystem ships size + age file rotation, size + age retention sweep, a small binary `INDEX` catalog (rebuildable from on-disk files), per-record or batched `fsync` modes, and is read through the `audit` kind of `Flezha.Service.Logs`, callable from both the CLI (`flz logs ... audit`, spelled `flz audit list | read | tail | stats | rotate`) and the console directly. The sink uses the foundation's `:block` overflow policy so the audit chain is never silently broken under load. Authentication / authorization of audit readers, remote shipping, and tamper-evident inter-file hash chains are explicitly out of scope and deferred to follow-up capabilities.
## Requirements
### Requirement: Durable append-only audit sink

The system SHALL keep the audit log as the `audit` kind of the shared log core (see
`log-kinds`), fed by a Sink that implements the foundation's `Flezha.Observability.Sink`
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

Each record SHALL be a single UTF-8 JSON object on its own line, terminated by `\n`. The top-level fields SHALL appear in this exact order: `ts_us`, `ts_iso`, `source`, `name`, `measurements`, `metadata`, `caller`, `namespace`, `node`. `ts_us` SHALL be wall-clock microseconds and `ts_iso` the same instant. `node` SHALL be the controller that wrote the record. The `caller` field SHALL be denormalised from `metadata.caller_source`, `metadata.principal`, and `metadata.request_id` so a reader does not need to know the metadata convention to identify the actor. When `:authn_authz, :enabled` is `true` the `caller.principal` value SHALL be the verified principal bound by the authentication layer; when `false` the `caller.principal` value SHALL be `null` and SHALL NOT carry any caller-supplied placeholder. The `namespace` field SHALL be the resolved namespace from `caller_context.namespace`; for pre-`platform-authn-authz` records or signals without a namespace context the field SHALL be `"_system"`.

#### Scenario: Single-line record for a signal
- **WHEN** the sink writes a signal with `source: :observability_service`, `name: :enable_source`, `metadata: %{caller_source: :cli, request_id: "req-1"}`
- **THEN** the written line SHALL be valid JSON, SHALL end with `\n`, SHALL contain top-level fields in the order `ts_us, ts_iso, source, name, measurements, metadata, caller, namespace, node`, AND `caller.source` SHALL equal `"cli"` AND `caller.request_id` SHALL equal `"req-1"`

#### Scenario: Verified principal lands in caller.principal
- **WHEN** `:authn_authz, :enabled` is `true` AND a signal is written whose `metadata.principal` was bound by the authentication layer
- **THEN** the written record's `caller.principal` SHALL equal that verified principal as a JSON string

#### Scenario: Authn-disabled principal renders as the caller-source atom
- **WHEN** `:authn_authz, :enabled` is `false` AND the signal's metadata contains no verified `:principal`
- **THEN** the written record's `caller.principal` SHALL be the caller-source atom (`"cli"`, `"ui"`, `"sdk"`, or `"internal"`) so existing records remain attributable (preserving pre-change behaviour, per the platform-authn-authz compatibility-mode requirement)

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
`log-kinds`), callable from both the CLI and the console directly.
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
and `flz logs rotate audit` (see `log-kinds`), resolved in the CLI verb table with no
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

