# plugin-file Specification

## Purpose

The file plugins: a source that feeds a relay from files and an output that writes a relay's records
to files. Every path resolves inside one configured root folder, created files are private and never
made through a symlink, and file contents are never logged. The source injects one entry frame per
line through the confirming ingest, follows a growing file, resumes at its committed offset and
bounds its memory; the output appends one line per record and confirms only once the batch is
durable, or under `rename` commits immutable parts. Both report their health, and under an `s3://`
root every path is confined to one bucket prefix, objects are read by ranged requests and parts are
committed with a guarded sidecar.

## Requirements

### Requirement: Every file plugin path SHALL resolve inside one configured root folder

The controller SHALL declare one configuration row, `FLEZHA_FILE_PLUGIN_ROOT`, naming the one
folder, or the one `s3://` bucket prefix, under which file plugins read and write. An `s3://`
root SHALL follow `plugin-file`, and the folder rules of this requirement SHALL NOT apply to
it. Its default SHALL be the folder `storage/app/files`, which the first file plugin enable SHALL
create when it is missing. A folder the user sets SHALL NOT be created, and while it is
missing every file plugin enable SHALL be refused naming it.

Each namespace SHALL have its own folder, `<root>/<namespace>`, and a plugin SHALL read and write
only inside the folder of the namespace it is enabled in. The namespace folder SHALL be created,
readable by the controller's user only, on the first enable that needs it, when the root exists.

Under a folder root, a plugin's `path` SHALL be expanded against its namespace folder, so a
relative path names a file under it and an absolute path is taken as written, and SHALL then be
resolved with symlinks followed, component by component under a bounded number of hops. The
resolved path SHALL start with the resolved namespace folder, or the enable SHALL be refused
naming that folder. The file itself need not exist at enable time, but its parent directory SHALL
exist and SHALL resolve inside the namespace folder.

The resolved absolute path SHALL replace `path` in the plugin's options, so every log line,
telemetry event and health verdict names one spelling.

#### Scenario: The root is not set

- **WHEN** a file plugin is enabled in namespace `orders` and `FLEZHA_FILE_PLUGIN_ROOT` is not set
- **THEN** the root SHALL be `storage/app/files`, created if it is missing, the folder
  `storage/app/files/orders` SHALL be created, and the enable SHALL succeed

#### Scenario: A configured root that is missing is refused

- **GIVEN** the root is set to `/srv/flezha/files`, which does not exist
- **WHEN** a file plugin is enabled
- **THEN** the enable SHALL be refused naming `/srv/flezha/files`, and the folder SHALL NOT be
  created

#### Scenario: An S3 root is not treated as a folder

- **GIVEN** the root is set to `s3://events/flezha`
- **WHEN** a file plugin is enabled with `path: "echo"`
- **THEN** no folder SHALL be looked for or created, and the path SHALL be resolved by
  `plugin-file`

#### Scenario: A relative path lands under the root

- **GIVEN** the root is `/srv/flezha/files`
- **WHEN** a plugin is enabled in namespace `orders` with `path: "events.ndjson"`
- **THEN** the enable SHALL succeed and the plugin's resolved path SHALL be
  `/srv/flezha/files/orders/events.ndjson`

#### Scenario: A path that climbs out is refused

- **GIVEN** the root is `/srv/flezha/files`
- **WHEN** a plugin is enabled in namespace `orders` with `path: "../certs/controller.key"`
- **THEN** the enable SHALL be refused naming `/srv/flezha/files/orders`

#### Scenario: An absolute path outside the root is refused

- **GIVEN** the root is `/srv/flezha/files`
- **WHEN** a plugin is enabled with `path: "/etc/passwd"`
- **THEN** the enable SHALL be refused naming the namespace folder under `/srv/flezha/files`

#### Scenario: A symlink pointing out of the root is refused

- **GIVEN** `/srv/flezha/files/orders/out` is a symlink to `/etc`
- **WHEN** a plugin is enabled in namespace `orders` with `path: "out/hosts"`
- **THEN** the enable SHALL be refused naming the namespace folder, because the resolved path is
  outside it

#### Scenario: A symlink pointing inside the root is allowed

- **GIVEN** `/srv/flezha/files/orders/current` is a symlink to `/srv/flezha/files/orders/2026-09`
- **WHEN** a plugin is enabled in namespace `orders` with `path: "current/events.ndjson"`
- **THEN** the enable SHALL succeed and the resolved path SHALL be
  `/srv/flezha/files/orders/2026-09/events.ndjson`

#### Scenario: A missing parent is refused

- **WHEN** a plugin is enabled with a path whose parent directory does not exist
- **THEN** the enable SHALL be refused naming the missing directory

#### Scenario: Another namespace's folder is refused

- **GIVEN** the root is `/srv/flezha/files`
- **WHEN** a plugin is enabled in namespace `payments` with `path: "../orders/in/trades.jsonl"`
  or with `path: "/srv/flezha/files/orders/in/trades.jsonl"`
- **THEN** the enable SHALL be refused naming `/srv/flezha/files/payments`

#### Scenario: A symlink into another namespace's folder is refused

- **GIVEN** `/srv/flezha/files/payments/peek` is a symlink to `/srv/flezha/files/orders`
- **WHEN** a plugin is enabled in namespace `payments` with `path: "peek/events.ndjson"`
- **THEN** the enable SHALL be refused naming `/srv/flezha/files/payments`

### Requirement: A file the output plugin creates SHALL be private and SHALL NOT be created through a symlink

A file the output plugin creates SHALL have mode `0600`. A file that already exists SHALL keep the mode
it has. If the path names a symlink at open time, the open SHALL be refused rather than
followed, even when the link resolves inside the root.

#### Scenario: A created file is private

- **WHEN** the output plugin opens a path that does not exist
- **THEN** the file SHALL exist afterwards with mode `0600`

#### Scenario: An existing file keeps its mode

- **GIVEN** a file with mode `0644` at the output plugin's path
- **WHEN** the output plugin opens it and appends
- **THEN** its mode SHALL still be `0644`

#### Scenario: A symlink is not opened

- **GIVEN** the output plugin's path is a symlink to a file inside the root
- **WHEN** the output plugin attempts to open it
- **THEN** the open SHALL be refused and the plugin SHALL report unhealthy naming the path

### Requirement: A file plugin SHALL NOT log file contents

A log line, telemetry event or health verdict SHALL name the path, an offset or a byte count.
It SHALL NOT carry the bytes of any line read or written.

#### Scenario: A refused line names its offset

- **WHEN** the reader refuses a line at byte offset 4096
- **THEN** the warning and the health verdict SHALL name the path and `4096`, and SHALL NOT
  contain the line's bytes

### Requirement: A restored file plugin outside its namespace folder SHALL be refused naming the new location

A restored file plugin binding SHALL be refused when its stored path lies outside its namespace
folder, after a restart or an upgrade alike, and the refusal SHALL name both the stored path and
the path it would have under the namespace folder. Nothing SHALL be moved,
read or written for it.

#### Scenario: A binding stored under the shared folder is refused on restore

- **GIVEN** a file sink in namespace `orders` was stored with path `/srv/flezha/files/out.ndjson`
- **WHEN** the controller restores it
- **THEN** the binding SHALL be refused naming `/srv/flezha/files/out.ndjson` and
  `/srv/flezha/files/orders/out.ndjson`, and no file SHALL be created or opened

### Requirement: A file source SHALL inject one entry frame per line through the confirming ingest

A built-in provider `relay_file_input` SHALL be attachable as a plugin with options
`{path, channel | channel_in, start_at? (beginning | end, default beginning), poll_interval_ms?
(default 1000), max_line_bytes? (default 1 MiB), read_chunk_bytes? (default 64 KiB),
commit_interval_ms? (default 1000), failure_threshold? (default 3), idle_after_ms? (default 0,
derived), plugin_id?}`. It SHALL declare that it feeds the relay, so `channel` resolves as
the entry channel by the rule in `plugin-providers`; `channel_in` SHALL be the explicit
spelling of the same name, one of the two SHALL be required, and `channel_out` SHALL be refused
as an option this provider does not offer.

A record SHALL be the bytes of one line up to and excluding its newline. Each record SHALL be
injected on the entry channel through the relay's confirming ingest, and the reader SHALL
NOT read past one chunk ahead of the last confirmed line, so a payload the relay did not
take is offered again and the controller's memory is bounded by one chunk and one line. A
trailing fragment with no newline SHALL NOT be delivered until a newline follows it. An empty
line SHALL be delivered as an empty string. A line that is not valid UTF-8 SHALL stop the
reader at that offset.

`init_provider/1` SHALL perform no reading: the worker opens the file at activation.

#### Scenario: Three lines become three frames in order

- **GIVEN** a file holding `a\nb\nc\n`
- **WHEN** the plugin is enabled with `channel_in: "events"`
- **THEN** exactly three frames `a`, `b`, `c` SHALL be injected on `events`, in that order

#### Scenario: A fragment waits for its newline

- **GIVEN** a file holding `a\nb`
- **WHEN** the plugin has delivered `a`
- **THEN** no second frame SHALL be injected until a newline is appended, and `b` SHALL be
  injected once it is

#### Scenario: A refused ingest is re-offered

- **WHEN** the relay refuses an injected line
- **THEN** the reader SHALL offer the same line again on its next attempt and SHALL NOT advance
  past it

#### Scenario: The chain-side key is refused

- **WHEN** a plugin is attached with `channel_out`
- **THEN** the attach SHALL fail, naming `channel_out` as an option this provider does not offer

#### Scenario: Neither channel spelling is refused

- **WHEN** a plugin is attached with neither `channel` nor `channel_in`
- **THEN** the attach SHALL fail naming the two spellings

### Requirement: A file source SHALL follow a growing file and resume at its committed offset

The reader SHALL poll the file every `poll_interval_ms` for new bytes. It SHALL commit the byte
offset of the end of the last confirmed line through the plugin's persisted option seam, after a
batch of confirmations or `commit_interval_ms`, whichever comes first, and SHALL read the
committed value back from an internal `offset` option on every restart path: a plugin restart,
a live move to another controller, and a controller restart. A committed offset SHALL win over
`start_at`; with no committed offset, `beginning` SHALL start at byte 0 and `end` at the file's
current size.

Delivery SHALL be at-least-once: a crash after a confirmation and before its commit re-delivers
the lines confirmed since the last commit, and SHALL NOT skip one.

A file whose size is below the committed offset, or whose inode has changed, SHALL be treated as
truncated or replaced: the reader SHALL reopen at byte 0, commit 0, and log a warning naming the
path and the old offset. A missing file SHALL NOT be treated as truncation: the reader SHALL
keep polling and report unhealthy until it appears.

#### Scenario: Appended lines are delivered

- **GIVEN** a plugin that has delivered every line of its file
- **WHEN** `d\n` is appended
- **THEN** `d` SHALL be injected within one `poll_interval_ms`

#### Scenario: A plugin restart resumes after the last committed line

- **GIVEN** a plugin that has confirmed and committed three lines
- **WHEN** the plugin is restarted and a fourth line is appended
- **THEN** only the fourth line SHALL be injected

#### Scenario: A controller restart resumes after the last committed line

- **GIVEN** a plugin that has confirmed and committed three lines
- **WHEN** the controller restarts and the relay is rebuilt from its declared manifest
- **THEN** the reader SHALL start at the committed offset and inject nothing until a new line
  is appended

#### Scenario: A crash before the commit re-delivers, never skips

- **GIVEN** a plugin that has confirmed five lines and committed through the third
- **WHEN** the worker crashes and restarts
- **THEN** the fourth and fifth lines SHALL be injected again and the sixth SHALL NOT be
  skipped

#### Scenario: `start_at: end` ignores existing content

- **GIVEN** a file holding three lines and no committed offset
- **WHEN** the plugin is enabled with `start_at: "end"` and a fourth line is appended
- **THEN** only the fourth line SHALL be injected

#### Scenario: A truncated file restarts at zero

- **GIVEN** a plugin with a committed offset of 300
- **WHEN** the file is truncated to 0 and `x\n` is written
- **THEN** `x` SHALL be injected, the committed offset SHALL be 2, and a warning SHALL name the
  path and `300`

### Requirement: A file source SHALL bound memory and self-report health

A line longer than `max_line_bytes` SHALL stop the reader at that line's offset and report
`{:unhealthy, {:line_too_long, offset}}`; the committed offset SHALL NOT advance past it. A
missing file SHALL report `{:unhealthy, {:file_missing, path}}`. `failure_threshold`
consecutive read failures SHALL report `{:unhealthy, {:read_failed, error}}`, the reason every source reports for this condition. A reader whose
last settled poll is older than the idle window (`idle_after_ms`, or three `poll_interval_ms`
when 0) SHALL report `{:unhealthy, {:poll_idle, age_ms}}`. A worker that does not answer the
probe inside its budget SHALL report `{:fail_open, :probe_timeout}`, per the provider health
contract. A file that does not grow SHALL read healthy.

#### Scenario: An oversized line stops the reader

- **GIVEN** `max_line_bytes: 16`
- **WHEN** the file holds a 20-byte line at offset 40
- **THEN** no frame SHALL be injected for it, health SHALL report
  `{:unhealthy, {:line_too_long, 40}}`, and the committed offset SHALL be at most 40

#### Scenario: A missing file is unhealthy and recovers when it appears

- **WHEN** the file is absent at activation
- **THEN** health SHALL report `{:unhealthy, {:file_missing, path}}`, and once the file is
  created with `a\n` the plugin SHALL inject `a` and report healthy

#### Scenario: A quiet file is healthy

- **GIVEN** a file that has not grown for many poll intervals
- **WHEN** health is queried
- **THEN** it SHALL report `:healthy`

#### Scenario: A reader that stopped polling is unhealthy

- **GIVEN** a reader whose last settled poll is older than the idle window
- **WHEN** health is queried
- **THEN** it SHALL report `{:unhealthy, {:poll_idle, age_ms}}`

#### Scenario: Repeated read failures are reported as read_failed

- **GIVEN** `failure_threshold: 3` and a file whose reads fail
- **WHEN** three consecutive reads fail
- **THEN** health SHALL report `{:unhealthy, {:read_failed, error}}` naming the last error

### Requirement: A file output plugin SHALL append one line per record and confirm the hold only once the batch is durable

A built-in provider `relay_file_output` SHALL be attachable as a plugin with options `{path,
channel_out?, commit? (append), linger_ms? (default 25), max_batch? (default 500), queue_max?
(default 10000), retry_backoff_ms?, retry_max_backoff_ms?, unhealthy_after_ms? (default
30000), plugin_id?}` and the hooks `on_input` and `on_output`. It SHALL NOT declare that it
feeds the relay: `channel_out` is the record filter by the rule in `plugin-providers`,
absent means every record, and `channel` or `channel_in` SHALL be refused as options this
provider does not offer.

Each record SHALL be written as its message bytes followed by one newline, and nothing else.
Records SHALL be batched by `linger_ms` or `max_batch`, written in one call, and made durable
with a data sync before any outbox hold in the batch is confirmed. A write failure SHALL keep
the batch and retry under the shared backoff ladder; no record SHALL be dropped. A caller
enqueuing past `queue_max` SHALL park until a flush frees space.

A record whose message contains a newline SHALL NOT be written and SHALL be parked in the
dead-letter store under the plugin id with reason `embedded_newline`; the output plugin SHALL NOT
rewrite bytes.

#### Scenario: Three records become three lines

- **WHEN** records `a`, `b`, `c` pass through the output plugin
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

- **GIVEN** an output plugin with `channel_out: "alerts"` on a relay also producing `metrics`
- **WHEN** one record is released on each
- **THEN** only the `alerts` record SHALL be written

#### Scenario: An embedded newline is parked

- **WHEN** a record whose message is `a\nb` reaches the output plugin
- **THEN** nothing SHALL be written for it and it SHALL be parked with reason
  `embedded_newline`

#### Scenario: The entry-side keys are refused

- **WHEN** a plugin is attached with `channel` or `channel_in`
- **THEN** the attach SHALL fail, naming the option as one this provider does not offer

### Requirement: A file output plugin SHALL self-report health

The output plugin SHALL report `{:unhealthy, :no_worker}` with no worker, `{:unhealthy, :worker_dead}`
with a dead one, `{:unhealthy, {:open_failed, reason}}` when the file cannot be opened, and
`{:unhealthy, {:write_stalled, error}}` when records are pending, the failure run is non-zero,
and no durable flush has completed within `unhealthy_after_ms`. Otherwise it SHALL report
`:healthy`.

#### Scenario: A stalled flush is unhealthy

- **GIVEN** pending records and a write that has failed for longer than `unhealthy_after_ms`
- **WHEN** health is queried
- **THEN** it SHALL report `{:unhealthy, {:write_stalled, error}}`

#### Scenario: An idle output plugin is healthy

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
exactly once across an output plugin crash, a plugin restart and a controller restart, and any other
record is written at least once. A value the option does not declare SHALL be refused at attach
naming the option.

Under an S3 root, `append`, whether given or taken as the default, SHALL be refused at attach
naming `commit`, and `rename` SHALL commit parts as `plugin-file` requires.

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
- **WHEN** an output plugin is attached without `commit`, or with `commit: "append"`
- **THEN** the attach SHALL fail naming `commit`

### Requirement: Under `rename` the output plugin SHALL commit immutable parts and reconcile the directory by one rule

Under `commit: "rename"` the output plugin SHALL write records into a hidden temp file in the directory
`path` names, syncing each batch as it flushes. It SHALL close a roll when `roll_after_ms`
(default 10 000) or `roll_bytes` (default 64 MiB) is reached, whichever first, by renaming the
temp file to `part-<N>` with `N` zero-padded and strictly increasing, then writing a sidecar
`.progress` temp-then-rename carrying `N` and the per-Cell `(epoch, seq)` watermark of every
part up to `N`, and only then confirming every hold the part carries. A temp file SHALL NOT be
visible under a final name.

On start the output plugin SHALL delete every temp file and every part whose number is greater than the
sidecar's, because their records were never confirmed and the outbox re-drive returns them. A
part more than one ahead of the sidecar SHALL refuse the start naming both numbers, and SHALL
delete nothing. A guaranteed record whose `(epoch, seq)` is at or below the sidecar's watermark
for its Cell SHALL NOT be written and its hold SHALL be confirmed; one at or below the open
roll's watermark SHALL NOT be written and its hold SHALL be confirmed with the roll. Only a
record carrying an outbox confirmation token is judged, because any other record's seq may
repeat. The identity a record carries SHALL be read by the same function the Postgres output plugin
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
- **WHEN** the output plugin starts and the part's records are re-driven
- **THEN** `part-00000005` SHALL be deleted at start and its records SHALL appear in exactly
  one part afterwards

#### Scenario: A crash after the sidecar writes once

- **GIVEN** `.progress` naming `5` with a watermark covering `part-00000005`
- **WHEN** the output plugin starts and `part-00000005`'s records are re-driven
- **THEN** nothing SHALL be written for them and each hold SHALL be confirmed

#### Scenario: A part two ahead refuses the start

- **GIVEN** `part-00000007` present and `.progress` naming `5`
- **WHEN** the output plugin starts
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

### Requirement: An S3 root SHALL confine every file plugin path to one bucket prefix

The controller SHALL accept `FLEZHA_FILE_PLUGIN_ROOT` in the form `s3://<bucket>/<prefix>`. Each
namespace SHALL have its own prefix, `<prefix>/<namespace>`, and a plugin's `path` SHALL then
name a key under the prefix of the namespace it is enabled in: a relative path SHALL be joined to
that prefix with its `.` and `..` segments removed lexically, an `s3://` path SHALL be taken as
written, and a path starting with `/` SHALL be refused naming the root. The resulting key SHALL
lie under the namespace prefix, compared segment by segment, or the enable SHALL be refused naming
it. The resolved `s3://` URL SHALL replace `path` in the plugin's options. The bucket SHALL NOT be
created.

#### Scenario: A relative path becomes a key under the prefix

- **GIVEN** the root is `s3://events/flezha`
- **WHEN** a plugin is enabled in namespace `orders` with `path: "out/echo"`
- **THEN** the enable SHALL succeed and the plugin's resolved path SHALL be
  `s3://events/flezha/orders/out/echo`

#### Scenario: A path that climbs out is refused

- **GIVEN** the root is `s3://events/flezha`
- **WHEN** a plugin is enabled in namespace `orders` with `path: "../other/echo"`
- **THEN** the enable SHALL be refused naming `s3://events/flezha/orders`

#### Scenario: Another bucket is refused

- **GIVEN** the root is `s3://events/flezha`
- **WHEN** a plugin is enabled with `path: "s3://audit/flezha/echo"`
- **THEN** the enable SHALL be refused naming the namespace prefix under `s3://events/flezha`

#### Scenario: A filesystem path is refused

- **GIVEN** the root is `s3://events/flezha`
- **WHEN** a plugin is enabled with `path: "/etc/passwd"`
- **THEN** the enable SHALL be refused naming `s3://events/flezha`

#### Scenario: Another namespace's prefix is refused

- **GIVEN** the root is `s3://events/flezha`
- **WHEN** a plugin is enabled in namespace `payments` with
  `path: "s3://events/flezha/orders/out/echo"`
- **THEN** the enable SHALL be refused naming `s3://events/flezha/payments`

### Requirement: An S3 root SHALL use its own connection settings and SHALL NOT reach durable state

The controller SHALL declare four configuration rows: `FLEZHA_FILE_PLUGIN_S3_ENDPOINT`,
`FLEZHA_FILE_PLUGIN_S3_REGION` (default `us-east-1`), `FLEZHA_FILE_PLUGIN_S3_ACCESS_KEY_ID`, and
`FLEZHA_FILE_PLUGIN_S3_SECRET_ENV`, which names the environment variable holding the secret key.
A file plugin enabled under an S3 root while the endpoint, the access key id or the secret
variable's name is unset SHALL be refused naming the unset variable. The secret SHALL NOT appear
in a log line, a plugin option, a health verdict, the boot line or `flz controller status`.

When the durability tier's backend is `s3` on the same endpoint and bucket as the root, and
either prefix contains the other, every file plugin enable SHALL be refused naming both
prefixes.

#### Scenario: A missing setting is refused naming it

- **GIVEN** the root is `s3://events/flezha` and `FLEZHA_FILE_PLUGIN_S3_ENDPOINT` is unset
- **WHEN** a file plugin is enabled
- **THEN** the enable SHALL be refused naming `FLEZHA_FILE_PLUGIN_S3_ENDPOINT`

#### Scenario: The durability tier's prefix is refused

- **GIVEN** the durability tier stores under bucket `events`, prefix `flezha`, on the same
  endpoint
- **AND** the root is `s3://events/flezha/files`
- **WHEN** a file plugin is enabled
- **THEN** the enable SHALL be refused naming both `flezha` and `flezha/files`

#### Scenario: The secret is never printed

- **GIVEN** the secret variable holds `s3cr3t-value`
- **WHEN** `flz controller status` is read and a request fails with status `403`
- **THEN** neither the status output, the logged failure nor the health verdict SHALL contain
  `s3cr3t-value`

### Requirement: A file source under an S3 root SHALL read one object by ranged requests and resume at its committed offset

While caught up, the reader SHALL ask the object's size and ETag every `poll_interval_ms`. It
SHALL read new bytes in ranged requests of at most `read_chunk_bytes`, each conditioned on the
ETag it read, and SHALL inject and commit lines exactly as `plugin-file` requires for a
file. An object whose ETag changes, or whose ranged read fails its condition, SHALL be read
again from byte 0, and so SHALL an object shorter than the offset. A missing object SHALL report
`{:unhealthy, {:file_missing, path}}`, keep the offset, and recover when the object appears. A
request failing with any status other than `404` SHALL keep the offset and report a failure
naming the status; no line SHALL be skipped.

#### Scenario: An object's lines become frames in order

- **GIVEN** the object at the plugin's path holds `a\nb\n`
- **WHEN** the reader runs
- **THEN** frames `a` then `b` SHALL be injected and the committed offset SHALL reach 4

#### Scenario: A replaced object is read from the start

- **GIVEN** a reader whose committed offset is the end of `a\nb\n`
- **WHEN** the object is replaced by one holding `c\n`
- **THEN** frame `c` SHALL be injected, read from byte 0

#### Scenario: An object replaced during a read is read from the start

- **GIVEN** a ranged read in progress against ETag `e1`
- **WHEN** the object is replaced and the read fails its condition
- **THEN** the reader SHALL read the new object from byte 0

#### Scenario: A missing object is unhealthy and recovers

- **WHEN** the object at the plugin's path does not exist
- **THEN** health SHALL be `{:unhealthy, {:file_missing, path}}`
- **AND** once the object appears, its lines SHALL be delivered and health SHALL return to
  `:healthy`

#### Scenario: A plugin restart resumes after the last committed line

- **GIVEN** a reader that committed the offset after line 2 of a 3-line object
- **WHEN** the plugin restarts
- **THEN** only line 3 SHALL be injected

#### Scenario: A forbidden request is a failure, not a missing object

- **WHEN** a request for the object fails with status `403`
- **THEN** health SHALL report a failure naming `403`, not `file_missing`, and the offset SHALL
  be unchanged

### Requirement: A file output plugin under an S3 root SHALL commit each roll as one object and guard the sidecar

Under an S3 root the output plugin SHALL accept only `commit: rename`, and `path` SHALL name the key
prefix that stands for the directory. A `roll_bytes` above 5 GiB SHALL be refused at attach
naming `roll_bytes`. The open roll's records SHALL be held in memory with their holds open. A
roll SHALL be committed by one PUT of `<path>/part-<N>`, with `N` zero-padded to 8 digits, then
the sidecar `<path>/.progress` SHALL be written by a conditional put carrying `N` that is refused
when the stored part number is higher, and the roll's holds SHALL be confirmed only after both
writes succeed.

On start the output plugin SHALL list the keys under `<path>/`, delete a part numbered one above the
sidecar, and refuse the start naming both numbers when a part is two or more above it. A worker
whose sidecar write is refused because the stored number is higher SHALL NOT confirm the roll's
holds, and SHALL stop reporting the stored number.

#### Scenario: A roll becomes one object

- **GIVEN** an output plugin under `s3://events/flezha` with `path: "out"` whose open roll holds `a` and `b`
- **WHEN** the roll closes as part 1
- **THEN** the object `out/part-00000001` SHALL hold `a\nb\n`, `out/.progress` SHALL name part 1,
  and the holds SHALL be confirmed only after both writes

#### Scenario: A crash between the part and the sidecar writes once

- **GIVEN** `out/part-00000004` was written and `out/.progress` still names part 3
- **WHEN** the output plugin starts
- **THEN** `out/part-00000004` SHALL be deleted, and the re-driven records SHALL be written once

#### Scenario: A part two ahead refuses the start

- **GIVEN** `out/.progress` names part 3 and `out/part-00000005` exists
- **WHEN** the output plugin starts
- **THEN** the start SHALL be refused naming parts 5 and 3, and nothing SHALL be deleted

#### Scenario: A worker behind a newer sidecar stops

- **GIVEN** `out/.progress` names part 7, written by another worker
- **WHEN** this worker commits its roll as part 5
- **THEN** the sidecar write SHALL be refused, the roll's holds SHALL NOT be confirmed, and the
  worker SHALL stop reporting part 7

#### Scenario: An oversized roll is refused

- **GIVEN** the root is `s3://events/flezha`
- **WHEN** an output plugin is attached with `commit: "rename"` and a `roll_bytes` of 6 GiB
- **THEN** the attach SHALL fail naming `roll_bytes`
