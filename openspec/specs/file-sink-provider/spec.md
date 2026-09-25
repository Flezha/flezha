# file-sink-provider Specification

## Purpose
TBD - created by archiving change add-a-file-plugin. Update Purpose after archive.
## Requirements
### Requirement: A file sink SHALL append one line per record and confirm the hold only once the batch is durable

A built-in provider `operator_file_sink` SHALL be attachable as a plugin with options `{path,
channel_out?, commit? (append), linger_ms? (default 25), max_batch? (default 500), queue_max?
(default 10000), retry_backoff_ms?, retry_max_backoff_ms?, unhealthy_after_ms? (default
30000), plugin_id?}` and the hooks `on_input` and `on_output`. It SHALL NOT declare that it
feeds the operator: `channel_out` is the record filter by the rule in `operator-extensions`,
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

- **GIVEN** a sink with `channel_out: "alerts"` on an operator also producing `metrics`
- **WHEN** one record is released on each
- **THEN** only the `alerts` record SHALL be written

#### Scenario: An embedded newline is parked

- **WHEN** a record whose message is `a\nb` reaches the sink
- **THEN** nothing SHALL be written for it and it SHALL be parked with reason
  `embedded_newline`

#### Scenario: The entry-side keys are refused

- **WHEN** a plugin is attached with `channel` or `channel_in`
- **THEN** the attach SHALL fail, naming the option as one this provider does not offer

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

### Requirement: The `commit` option SHALL name the file layout

`commit` SHALL be an enumerated option with two values. `append`, the default, is one growing
file with batches appended in place; its documentation SHALL state the crash window: a batch
written and synced before a crash, whose confirmation did not reach the outbox, is re-driven
after the restart and appended a second time. No record is lost. `rename` is a directory of
immutable part files committed by rename with a progress sidecar, under which a record the
outbox guarantees, carrying a confirmation token and a replay-stable identity, SHALL be written
exactly once across a sink crash, a plugin restart and a controller restart, and any other
record is written at least once. A value the option does not declare SHALL be refused at attach
naming the option.

Under an S3 root, `append`, whether given or taken as the default, SHALL be refused at attach
naming `commit`, and `rename` SHALL commit parts as `file-plugin-s3` requires.

#### Scenario: The default is append

- **GIVEN** a folder root
- **WHEN** a plugin is attached without `commit`
- **THEN** its resolved options SHALL carry `commit: "append"`

#### Scenario: An unknown layout is refused

- **WHEN** a plugin is attached with `commit: "truncate"`
- **THEN** the attach SHALL fail naming `commit`

#### Scenario: Rename is accepted

- **GIVEN** a folder root
- **WHEN** a plugin is attached with `commit: "rename"` and a `path` that is a directory or does
  not exist
- **THEN** the attach SHALL succeed and the directory SHALL exist afterwards with mode `0700`

#### Scenario: A regular file under rename is refused

- **GIVEN** a folder root
- **WHEN** a plugin is attached with `commit: "rename"` and a `path` that is a regular file
- **THEN** the attach SHALL fail naming the layout

#### Scenario: Append under an S3 root is refused

- **GIVEN** the root is `s3://events/flezha`
- **WHEN** a sink is attached without `commit`, or with `commit: "append"`
- **THEN** the attach SHALL fail naming `commit`

### Requirement: Under `rename` the sink SHALL commit immutable parts and reconcile the directory by one rule

Under `commit: "rename"` the sink SHALL write records into a hidden temp file in the directory
`path` names, syncing each batch as it flushes. It SHALL close a roll when `roll_after_ms`
(default 10 000) or `roll_bytes` (default 64 MiB) is reached, whichever first, by renaming the
temp file to `part-<N>` with `N` zero-padded and strictly increasing, then writing a sidecar
`.progress` temp-then-rename carrying `N` and the per-Cell `(epoch, seq)` watermark of every
part up to `N`, and only then confirming every hold the part carries. A temp file SHALL NOT be
visible under a final name.

On start the sink SHALL delete every temp file and every part whose number is greater than the
sidecar's, because their records were never confirmed and the outbox re-drive returns them. A
part more than one ahead of the sidecar SHALL refuse the start naming both numbers, and SHALL
delete nothing. A guaranteed record whose `(epoch, seq)` is at or below the sidecar's watermark
for its Cell SHALL NOT be written and its hold SHALL be confirmed; one at or below the open
roll's watermark SHALL NOT be written and its hold SHALL be confirmed with the roll. Only a
record carrying an outbox confirmation token is judged, because any other record's seq may
repeat. The identity a record carries SHALL be read by the same function the Postgres sink
reads it with.

#### Scenario: A roll becomes one immutable part

- **GIVEN** `roll_after_ms: 100`
- **WHEN** records `a`, `b`, `c` are flushed and the roll closes
- **THEN** the directory SHALL hold `part-00000001` ending `a\nb\nc\n`, `.progress` naming `1`,
  and no temp file

#### Scenario: Holds are confirmed at the roll

- **GIVEN** a guaranteed record flushed into the open temp file
- **WHEN** health and the outbox are observed before the roll closes
- **THEN** the hold SHALL still be open, and SHALL be confirmed after `.progress` is renamed

#### Scenario: A crash before the part rename writes once

- **GIVEN** a temp file holding synced records whose holds are open
- **WHEN** the worker crashes and restarts, and the records are re-driven
- **THEN** the temp file SHALL be gone and the records SHALL appear in exactly one part

#### Scenario: A crash between the two renames writes once

- **GIVEN** `part-00000005` present and `.progress` naming `4`
- **WHEN** the sink starts and the part's records are re-driven
- **THEN** `part-00000005` SHALL be deleted at start and its records SHALL appear in exactly
  one part afterwards

#### Scenario: A crash after the sidecar writes once

- **GIVEN** `.progress` naming `5` with a watermark covering `part-00000005`
- **WHEN** the sink starts and `part-00000005`'s records are re-driven
- **THEN** nothing SHALL be written for them and each hold SHALL be confirmed

#### Scenario: A part two ahead refuses the start

- **GIVEN** `part-00000007` present and `.progress` naming `5`
- **WHEN** the sink starts
- **THEN** it SHALL refuse naming `7` and `5`, health SHALL report
  `{:unhealthy, {:open_failed, reason}}`, and every part SHALL remain

#### Scenario: A record the outbox does not guarantee is still written

- **WHEN** a record carrying no `flz.cell`, and two keyed records sharing one `flz.seq` and
  carrying no confirmation token, are flushed under `rename`
- **THEN** all three SHALL be written, and the watermark SHALL NOT mention them

#### Scenario: The roll closes by size

- **GIVEN** `roll_bytes: 64` and `roll_after_ms: 60000`
- **WHEN** 100 bytes of records are flushed
- **THEN** a part SHALL be committed before `roll_after_ms` elapses

