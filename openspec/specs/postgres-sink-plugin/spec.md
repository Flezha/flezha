# postgres-sink-plugin Specification

## Purpose

Transactional upsert + watermark PostgreSQL sink adapter consuming the
effectively-once tier (`Flezha.Plugin.Provider.Postgres`).

## Requirements

### Requirement: PostgreSQL sink plugin with transactional upsert + watermark

The controller SHALL provide a `postgres` `io_binding` sink plugin
(`Flezha.Plugin.Provider.Postgres`, backed by `postgrex`) that, per flush batch,
executes ONE database transaction containing (a) an upsert of every record keyed
by `flz.key` and (b) an upsert of the per-Segment progress row
(`flezha_sink_progress`: segment, last outbox seq, epoch). Data and progress
SHALL never be committed separately.

#### Scenario: Batch commits atomically

- **WHEN** a flush batch of 3 records for segment S is delivered to the plugin
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
key, segment text, seq text, epoch bigint, payload bytea, updated_at
timestamptz)` and `flezha_sink_progress(segment text primary key, last_seq text,
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
convergence escalation path covers the sink.

#### Scenario: Unhealthy on sustained commit failure

- **WHEN** no flush transaction has committed within the configured unhealthy
  threshold while batches are pending
- **THEN** `health/1` reports unhealthy with the failure reason
