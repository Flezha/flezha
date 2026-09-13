# file-sink-provider Specification

## Purpose
TBD - created by archiving change add-a-file-plugin. Update Purpose after archive.
## Requirements
### Requirement: A file sink SHALL append one line per record and confirm the hold only once the batch is durable

A built-in provider `operative_file_sink` SHALL be attachable as a plugin with options `{path,
channel_out?, commit? (append), linger_ms? (default 25), max_batch? (default 500), queue_max?
(default 10000), retry_backoff_ms?, retry_max_backoff_ms?, unhealthy_after_ms? (default
30000), plugin_id?}` and subscriptions `on_input` and `on_output`. It SHALL NOT declare that it
feeds the operative: `channel_out` is the record filter by the rule in `operative-extensions`,
absent means every record, and `channel` or `channel_in` SHALL be refused as options this
provider does not offer.

Each record SHALL be written as its message bytes followed by one newline, and nothing else.
Records SHALL be batched by `linger_ms` or `max_batch`, written in one call, and made durable
with a data sync before any outbox hold in the batch is confirmed. A write failure SHALL keep
the batch and retry under the shared backoff ladder; no record SHALL be dropped. A caller
enqueuing past `queue_max` SHALL park until a flush frees space.

A record whose message contains a newline SHALL NOT be written and SHALL be parked in the
dead-letter store under the plugin id with reason `embedded_newline`; the sink SHALL NOT
rewrite bytes.

#### Scenario: Three records become three lines

- **WHEN** records `a`, `b`, `c` pass through the sink
- **THEN** the file SHALL end with `a\nb\nc\n`

#### Scenario: The hold is confirmed after the sync

- **GIVEN** a delivery-guaranteed record carrying an outbox token
- **WHEN** it is flushed
- **THEN** its hold SHALL be confirmed only after the batch's data sync returns

#### Scenario: A write failure drops nothing

- **GIVEN** a batch whose write fails
- **WHEN** the failure clears
- **THEN** every record of the batch SHALL be written exactly as if the failure had not
  happened, and the holds SHALL be confirmed then

#### Scenario: Only the filtered channel is written

- **GIVEN** a sink with `channel_out: "alerts"` on an operative also producing `metrics`
- **WHEN** one record is released on each
- **THEN** only the `alerts` record SHALL be written

#### Scenario: An embedded newline is parked

- **WHEN** a record whose message is `a\nb` reaches the sink
- **THEN** nothing SHALL be written for it and it SHALL be parked with reason
  `embedded_newline`

#### Scenario: The entry-side keys are refused

- **WHEN** a plugin is attached with `channel` or `channel_in`
- **THEN** the attach SHALL fail, naming the option as one this provider does not offer

### Requirement: The `commit` option SHALL name the file layout, and `append` SHALL be its only value

`commit` SHALL be an enumerated option whose only value in this capability is `append`: one
growing file, batches appended in place. Its documentation SHALL state the crash window: a
batch written and synced before a crash, whose confirmation did not reach the outbox, is
re-driven after the restart and appended a second time. No record is lost. A value the option
does not declare SHALL be refused at attach naming the option.

#### Scenario: The default is append

- **WHEN** a plugin is attached without `commit`
- **THEN** its resolved options SHALL carry `commit: "append"`

#### Scenario: An unknown layout is refused

- **WHEN** a plugin is attached with `commit: "rename"` while only `append` is declared
- **THEN** the attach SHALL fail naming `commit`

### Requirement: A file sink SHALL self-report health

The sink SHALL report `{:unhealthy, :no_sink}` with no worker, `{:unhealthy, :worker_dead}`
with a dead one, `{:unhealthy, {:open_failed, reason}}` when the file cannot be opened, and
`{:unhealthy, {:write_stalled, error}}` when records are pending, the failure run is non-zero,
and no durable flush has completed within `unhealthy_after_ms`. Otherwise it SHALL report
`:healthy`.

#### Scenario: A stalled flush is unhealthy

- **GIVEN** pending records and a write that has failed for longer than `unhealthy_after_ms`
- **WHEN** health is queried
- **THEN** it SHALL report `{:unhealthy, {:write_stalled, error}}`

#### Scenario: An idle sink is healthy

- **GIVEN** no pending records
- **WHEN** health is queried
- **THEN** it SHALL report `:healthy`

