# postgres-sink-plugin Specification

## Purpose

Transactional upsert + watermark PostgreSQL sink adapter consuming the
effectively-once tier (`Flezha.Plugin.Provider.Postgres`).
## Requirements
### Requirement: PostgreSQL sink plugin with transactional upsert + watermark

The controller SHALL provide a `postgres` operator sink plugin
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
convergence escalation path covers the sink.

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

- **WHEN** an operator on a `.keyed()` channel without the effectively-once tier
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

### Requirement: The sink's option set is strict and its tunables are validated at preparation

`init_provider/1` SHALL refuse an option key the provider does not offer, and SHALL refuse a tunable whose value it cannot interpret, before the sink worker or its connection pool is started. Both refusals SHALL be returned as `{:error, binary()}` naming the key or the option and the offered value.

The strict option set SHALL be declared, so that the check runs on the path a user's stored configuration travels. A plugin spec and a stored capability row reach `init_provider/1` without ever constructing the provider struct, so a check reachable only from the CLI string-map builder does not satisfy this requirement.

The health threshold SHALL be validated at preparation and read as a plain value by the health probe. A conversion performed on the probe path is a conversion performed on every probe: when it raises, the probe's uncertainty handling reports `{:fail_open, …}`, which reads as healthy, so a single unreadable value disables that plugin's health verdict for as long as it stays configured — silently, and for ever.

#### Scenario: An unknown option is refused at enable time

- **GIVEN** a plugin configured with a key the sink does not offer
- **WHEN** the plugin is enabled
- **THEN** `init_provider/1` SHALL return `{:error, message}` naming the key
- **AND** no sink worker and no connection pool SHALL have been started

#### Scenario: An uninterpretable health threshold is refused at enable time

- **GIVEN** a plugin configured with a `unhealthy_after_ms` value that is not an integer and does not name one
- **WHEN** the plugin is enabled
- **THEN** `init_provider/1` SHALL return `{:error, message}` naming the option
- **AND** the plugin SHALL NOT reach a state where its health probe reports an uncertain verdict for that reason

#### Scenario: Host-injected keys are still accepted

- **GIVEN** a plugin enabled on an operator, so that the host merges its identity keys into the provider's opts
- **WHEN** the plugin is enabled
- **THEN** those keys SHALL be accepted, and the sink SHALL start exactly as it does today

