# File plugin

For users who want a file as an operator's input, to replay a log or load an
export, or as its output, to watch what an operator emits land somewhere `tail` can
read. `Flezha.Plugin.Provider.File` is one family with two provider ids:
`operator_file_source` (`File.Source`) reads a file onto an entry channel, one line
per frame, and `operator_file_sink` (`File.Sink`) writes one line per record. This
page covers the root folder both share, then each direction: its contract, parameters,
delivery, failure semantics and health, then telemetry and limits.

## The root folder

A plugin that can read or write any path the controller can reach would hand whoever
enables plugins the controller's certificates, its state and its tokens. So a file
plugin may touch one folder and nothing outside it: `app_data/files` by default,
beside the controller's other data, created by the first file plugin enable. To use
another, name it with `FLEZHA_FILE_PLUGIN_ROOT` when the controller starts:

```bash
FLEZHA_FILE_PLUGIN_ROOT=/srv/flezha/files flz controller start
```

`flz controller status` prints the folder in use, as an absolute path, on its
`file_plugin_root` line.

- **A folder you name must exist.** It is read at boot and never created, because a
  missing one may be a share that is not mounted. Until it exists, every file plugin
  enable is refused naming it.
- **Every path resolves inside it.** A relative `path` names a file under the root; an
  absolute one is taken as written. The path is then resolved one component at a time
  with every symbolic link followed, and the result must lie inside the resolved root,
  or the enable is refused naming the root. `../certs/controller.key`, `/etc/passwd`
  and a link `out -> /etc` all fail the same way. A link that lands inside the root is
  allowed, judged by where it lands.
- **The parent must exist.** The file itself need not, but its directory must; a
  missing one is refused naming it. A missing directory outside the root is not named.
- **One spelling.** The resolved absolute path replaces `path` in the plugin's options,
  so every log line, telemetry event and health verdict names the same path.
- **Contents are never logged.** A log line, event or verdict carries the path, an
  offset or a count, never the bytes of a line.

The check runs when the plugin is prepared, before any worker exists, so a refusal
reaches the user who typed the path rather than surfacing later as an unhealthy
plugin.

### A local path, on a cluster

The root is a folder on the controller the operator runs on. On one controller, or
where the root is a shared mount at the same path on every controller, that is enough.
Where it is not, an operator moved onto a controller that does not have the file
reports `{:unhealthy, {:file_missing, path}}` within one health probe rather than
reading nothing silently, or the root can be [an S3 bucket](#an-s3-bucket-instead-of-a-folder)
every controller reaches.

Both providers print their path and their current verdict beneath the plugin in
`flz operator info`, so the screen that shows an operator `degraded` also says why:

```text
  Plugins:
    - reader: Flezha.Plugin.Provider.File.Source
        health: {:unhealthy, {:file_missing, "/srv/flezha/files/events.ndjson"}}
        path: /srv/flezha/files/events.ndjson
```

### An S3 bucket instead of a folder

Where an operator moves between controllers and no folder is shared, the root can be a
bucket prefix on an S3-compatible endpoint such as AWS S3 or MinIO:

```bash
FLEZHA_FILE_PLUGIN_ROOT=s3://events/flezha \
FLEZHA_FILE_PLUGIN_S3_ENDPOINT=https://s3.eu-north-1.amazonaws.com \
FLEZHA_FILE_PLUGIN_S3_REGION=eu-north-1 \
FLEZHA_FILE_PLUGIN_S3_ACCESS_KEY_ID=AKIAEXAMPLE \
FLEZHA_FILE_PLUGIN_S3_SECRET_ENV=EVENTS_S3_SECRET \
flz controller start
```

Every file plugin on the controller then reads and writes objects under `flezha/` in the
bucket `events`, and nothing in a plugin's options says so.

- **A path is a key under the prefix.** A relative `path` is joined to the prefix, an
  `s3://` path is taken as written, and either has its `.` and `..` segments removed
  without a request being made. The key must lie under the prefix in the root's bucket, so
  `../other`, `s3://audit/flezha/echo` and `/etc/passwd` are refused naming the root. The
  resolved `s3://` URL replaces `path`. The bucket is never created.
- **Four settings of its own.** `FLEZHA_FILE_PLUGIN_S3_ENDPOINT`,
  `FLEZHA_FILE_PLUGIN_S3_REGION` (default `us-east-1`), `FLEZHA_FILE_PLUGIN_S3_ACCESS_KEY_ID`
  and `FLEZHA_FILE_PLUGIN_S3_SECRET_ENV`, which names the variable holding the secret key.
  An enable while the endpoint, the key id or the secret variable's name is unset is refused
  naming the variable. None of them falls back to the durability tier's
  `FLEZHA_DURABILITY_S3_*` settings: give the plugins credentials whose bucket policy cannot
  reach durable state. The secret is never logged, stored in a plugin's options or printed.
- **Not the durability tier's objects.** When the durability backend is `s3` on the same
  endpoint and bucket, and either prefix contains the other, every file plugin enable is
  refused naming both prefixes, because a plugin writing there could overwrite a manifest.
- **The endpoint must be strongly consistent.** A part is listed and read straight after it
  is written. AWS S3 and MinIO are; an endpoint that is only eventually consistent can miss
  a part or read an object that is no longer there.
- `flz controller status` and the startup line print the root as the URL it was given.

What each direction does differently is under
[reading an object](#reading-an-object-under-an-s3-root) and
[writing parts to a bucket](#writing-parts-to-a-bucket).

## Reading a file: `operator_file_source`

### Contract

| | |
| --- | --- |
| Plugin type | `:io_binding` |
| Hooks | `:on_terminate`: the reader drives itself; the hook is there because a plugin spec must name at least one |
| Shared | no: a second operator bound to it is refused (`plugin.not_shareable`) |
| Upstream | one file inside the root folder, or one object under an S3 root, polled every `poll_interval_ms` |
| Injection | each line, without its newline, on the entry channel `channel` or `channel_in` names |
| Delivery | the confirming ingest: the offset moves only once the operator has taken the line |
| Resume | the byte offset of the last committed line, on a plugin restart, a move and a controller restart |
| Guarantee | at-least-once: a crash can re-deliver lines confirmed since the last commit, and never skips one |
| Failure posture | stop at a line it cannot deliver; keep polling a missing file |

```bash
flz plugin enable reader operator_file_source \
  --operator_id=op1 \
  --Xpath=events.ndjson \
  --Xchannel_in=events
```

As a plugin spec at operator start (`flz operator start --plugins`):

```json
[
  {
    "name": "reader",
    "provider": "operator_file_source",
    "opts": {"path": "events.ndjson", "channel_in": "events"},
    "hooks": ["on_terminate"]
  }
]
```

`channel` names the entry channel every line is injected on, and `channel_in` is the
explicit spelling of the same name; one of the two is required, and writing both is
refused. Either takes exactly one name. `channel_out` is not a parameter here: the
reader never acts on the records passing through the operator.

### Parameters

<!-- provider-options:begin file_source -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `path` | string | &mdash; | yes | The file to read: relative to FLEZHA_FILE_PLUGIN_ROOT, or absolute inside it. Under an s3:// root, an object under its prefix |
| `channel` | names | &mdash; | no | The channel this plugin is attached to: the entry channel every line is injected on, and one name. Required, unless `channel_in` names it |
| `channel_in` | names | &mdash; | no | Entry channel every line is injected on, and one name; the explicit spelling of `channel`. Required, unless `channel` names it |
| `plugin_id` | string | &mdash; | no | Plugin id; defaults to the id the plugin is enabled under |
| `start_at` | enum | `"beginning"` | no | Where a reader with no committed offset starts: byte 0, or the size of the file when it is first opened |
| `poll_interval_ms` | int | `1000` | no | Milliseconds between looks for new bytes |
| `max_line_bytes` | int | `1048576` | no | Longest line accepted; a longer one stops the reader at the offset it starts at |
| `read_chunk_bytes` | int | `65536` | no | Bytes read at a time, and never more than one read ahead of the last delivered line |
| `commit_interval_ms` | int | `1000` | no | Longest a delivered line's offset waits to be committed while the reader has not caught up; what a crash can re-deliver |
| `failure_threshold` | int | `3` | no | Consecutive failed reads or refused ingests before the plugin reports unhealthy |
| `idle_after_ms` | int | `0` | no | Age of the last settled poll past which the reader is idle; 0 derives it from `poll_interval_ms` |
<!-- provider-options:end -->

**One of `channel` and `channel_in` is required.** The Required column speaks of one
row at a time, so it reads `no` for both.

### One line, one frame

A record is the bytes of one line up to its newline, and nothing is interpreted:

- **The last line waits for its newline.** A writer may be mid-line, and an unfinished
  record cannot be told from a finished one without the terminator, so a trailing
  fragment is delivered only once a newline follows it.
- **An empty line is an empty string.** Deciding that an empty record is not a record
  would be interpreting it.
- **A line over `max_line_bytes`, or one that is not valid UTF-8, stops the reader**
  at the byte where that line starts, with `{:unhealthy, {:line_too_long, offset}}` or
  `{:unhealthy, {:invalid_utf8, offset}}` and a warning naming the path and the offset.
  Nothing is skipped: the committed offset never passes the line, so a plugin restart
  after raising the cap or fixing the file lands on it again. Entry payloads are
  strings in every SDK, which is why a line that is not UTF-8 cannot be a frame.

Memory is bounded by one chunk and one line whatever the file does: a chunk is read
only once every whole line before it has been delivered.

### Delivery is confirmed

Each line is `GenServer.call(overwatch, {:ingest, line, %{"channel" => entry_channel}})`,
never the fire-and-forget cast. The reader waits for the answer as long as the
operator takes: an operator parked at zero credit parks the reader with it, which is
the back-pressure a source exists to feel. A refused ingest counts toward health and
the same line is offered again on the next poll. While a reader is waiting on a parked
operator its health probe cannot be answered, which reports `{:fail_open,
:probe_timeout}`, an uncertain probe the convergence loop does not escalate on.

### Where it resumes

The reader commits the byte offset where the last delivered line ended through the
plugin's persisted option, `offset`. The value lands on the operator's live plugin
table, which a plugin restart and a move to another controller prepare the plugin
from, and in the declared plugin manifest, which a controller restart rebuilds the
operator from. A committed offset wins over `start_at`.

The commit is batched, not per line: once the reader has caught up with the file,
stopped, or has to wait, and otherwise every `commit_interval_ms` while it is still
reading. **Delivery is at-least-once.** The commit is asynchronous, so a crash between a
delivery and its commit re-delivers the lines confirmed since the last commit, at most
`commit_interval_ms` of them while the reader is behind. Treat a re-delivered line
idempotently. No line is skipped.

- **`start_at: end`** starts a reader with no committed offset at the file's size when
  it first opens the file, and commits that at once, so a restart does not measure the
  end again and miss what was appended in between.
- **Truncation and replacement.** A file shorter than the delivered offset, or one
  with a different inode, was truncated or replaced: the reader reopens it at byte 0,
  commits 0, logs a warning naming the path and the old offset, and emits `truncated`.
  A rotation that renames the old file away and creates a new one is read from byte 0.
  A truncation followed by writes past the old offset before the next poll cannot be
  seen by polling, as with `tail -f`.
- **A missing file is not a truncation.** The reader closes the file, keeps its offset
  and polls until the file is back, reporting `{:unhealthy, {:file_missing, path}}`
  meanwhile. The same file moved back resumes where it stopped; a new file at the path
  is read from byte 0.

### Reading an object under an S3 root

`path` names one object. While caught up the reader asks for its size and ETag every
`poll_interval_ms`, and it reads new bytes in ranged requests of at most
`read_chunk_bytes`, each conditioned on the ETag it read. Delivery, the committed offset and
health are the file's.

- **A replaced object is read from the start.** A new ETag, or a ranged read whose ETag no
  longer matches, means the object was replaced: it is read again from byte 0 and reported
  as `truncated`, as a replaced file is. So is an object shorter than the offset. A producer
  that rewrites one growing object makes every rewrite re-deliver it from the start, so
  write new objects instead.
- **A missing object**, a `404`, reports `{:unhealthy, {:file_missing, path}}` and keeps the
  offset; the same object coming back resumes where the reader stopped.
- **Any other failed request** keeps the offset and counts as a failed read whose reason
  names the status, such as `{:unhealthy, {:read_failing, {:stat, {:http, 403}}}}`. A `403`
  is a credentials fault and is never reported as a missing object.
- **Requests.** Each look is a `HEAD`, and a reader that is behind looks again before each
  chunk, so a larger `read_chunk_bytes` reads a large object in fewer requests.
- After a plugin restart the reader cannot tell a replaced object from the original and
  resumes at the committed offset, as a folder reader does.

### Health

| Verdict | When |
| --- | --- |
| `{:unhealthy, {:line_too_long, offset}}` / `{:unhealthy, {:invalid_utf8, offset}}` | the reader stopped at that line |
| `{:unhealthy, {:file_missing, path}}` | the file is not there |
| `{:unhealthy, {:read_failing, error}}` | `failure_threshold` consecutive failed reads or refused ingests |
| `{:unhealthy, {:poll_idle, age_ms}}` | the last settled poll is older than `idle_after_ms`, or three `poll_interval_ms` when it is 0 |
| `{:fail_open, :probe_timeout}` | the reader did not answer the probe, which is not a verdict |
| `{:unhealthy, :no_reader}` / `{:unhealthy, :reader_dead}` | no worker, or a dead one |
| `:healthy` | otherwise, including a file that does not grow |

## Writing a file: `operator_file_sink`

### Contract

| | |
| --- | --- |
| Plugin type | `:io_binding` |
| Hooks | `:on_input`, `:on_output` |
| Shared | no: a second operator bound to it is refused (`plugin.not_shareable`) |
| Downstream | one file inside the root folder, appended to; under `commit: rename`, a directory of parts, or under an S3 root a key prefix of parts |
| Record | the message bytes followed by one newline, nothing else |
| Delivery | a batch is written in one call and data-synced before any outbox hold in it is confirmed; under `rename`, once its part is committed |
| Overflow | delay, never loss: an enqueue past `queue_max` parks until a flush frees space |
| Failure posture | a failed write is cut back and retried under the backoff ladder; nothing is dropped |

```bash
flz plugin enable writer operator_file_sink \
  --operator_id=op1 \
  --hook=on_output \
  --Xpath=out/alerts.ndjson \
  --Xchannel_out=alerts
```

`channel_out` restricts the sink to records whose metadata channel matches; absent, it
writes every record its hook hands it. Several names, comma-separated, write them
all. `channel` and `channel_in` are not parameters here: the sink feeds nothing.

### Parameters

<!-- provider-options:begin file_sink -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `path` | string | &mdash; | yes | The file to append to, or under `commit: rename` the directory parts are committed into: relative to FLEZHA_FILE_PLUGIN_ROOT, or absolute inside it. Under an s3:// root, the key prefix parts are committed under |
| `channel_out` | names | &mdash; | no | Write only records whose metadata channel matches. Several names, comma-separated, write them all |
| `plugin_id` | string | &mdash; | no | Plugin id; defaults to the id the plugin is enabled under |
| `commit` | enum | `"append"` | no | The file layout. `append` grows one file in place, and a batch synced just before a crash is appended again after it; `rename` commits immutable parts into a directory, and writes a record the outbox guarantees once. Under an s3:// root only `rename` |
| `roll_after_ms` | int | `10000` | no | Under `rename`, the longest a part stays open, and so the longest a guaranteed record waits for its confirmation |
| `roll_bytes` | int | `67108864` | no | Under `rename`, the size at which a part is committed before `roll_after_ms`. Under an s3:// root the open part is held in memory, and at most 5368709120 |
| `linger_ms` | int | `25` | no | How long a batch waits to fill |
| `max_batch` | int | `500` | no | Records in one write |
| `queue_max` | int | `10000` | no | Records held before enqueue blocks |
| `retry_backoff_ms` | int | `250` | no | First delay after a failed write |
| `retry_max_backoff_ms` | int | `5000` | no | Ceiling for that delay |
| `unhealthy_after_ms` | int | `30000` | no | Age of the last durable write past which a backed-up sink is unhealthy |
<!-- provider-options:end -->

### One record, one line

A record whose message contains a newline cannot be one line. It is not written and
not rewritten: it is parked in the dead-letter store under the plugin id with the
reason `embedded_newline`, where `flz dlq list` shows it, and a delivery-guaranteed
record's hold is answered once the park is durable. Escaping the newline would make
the reader on the other side deliver a different record from the one written.

### Durability, and the two layouts `commit` names

Records are batched by `linger_ms` or `max_batch`, written in one call, and made
durable with a data sync. A write or sync that fails is cut back to the byte the batch
began at and retried under the backoff ladder from `retry_backoff_ms` to
`retry_max_backoff_ms`, so the retry appends the whole batch rather than after half a
line, and nothing is dropped. What the sync is followed by is the layout's.

**`commit: append`, the default, has a crash window.** `path` is one file, and each
delivery-guaranteed record's outbox hold is confirmed once its batch is synced, which is
what lets the operator's cursor move past it. A batch written and synced just before
the controller crashes, whose confirmation did not reach the outbox, is re-driven after
the restart and appended a second time. No record is lost; one batch can be written
twice. Choose `rename` where a duplicate line matters.

### `commit: rename`: parts written once

```bash
flz plugin enable writer operator_file_sink \
  --operator_id=op1 \
  --hook=on_output \
  --Xpath=out/alerts \
  --Xcommit=rename \
  --Xroll_after_ms=5000
```

Under `rename`, `path` names a directory, made with mode `0700` when it does not exist;
a `path` that is a regular file is refused at enable, naming the layout. The sink owns
everything in the directory:

```text
out/alerts/
  part-00000041          committed, never changed again
  part-00000042          committed, never changed again
  .progress              {"part":42,"watermarks":{...}}
  .part-00000043.tmp     the open roll, invisible to a listing of part-*
```

- **A roll, then a part.** Batches are synced into the open roll, a hidden temp file.
  The roll is committed when `roll_after_ms` has passed since its first bytes or it
  reaches `roll_bytes`, whichever comes first: the temp file is renamed to the next
  `part-<N>`, then `.progress` is written temp-then-rename naming `N` and each Cell's
  `(epoch, seq)` watermark, and only then are the roll's holds confirmed. Parts and the
  sidecar are created `0600`.
- **What a roll costs.** A guaranteed record waits up to `roll_after_ms` for its
  confirmation, so an entry on the effectively-once tier completes that much later. At
  the default of 10 000 ms a steadily busy sink commits a part every 10 seconds, 8 640
  parts a day; raise `roll_after_ms` for fewer, larger parts, lower it for a shorter
  wait. Nothing merges or removes old parts.
- **The reconcile.** When the sink starts it deletes every temp file and a part numbered
  one past `.progress`, because their records were never confirmed and the outbox brings
  them back to be written once. A part further ahead is nothing a crash leaves, so it
  refuses the start, deleting nothing, with
  `{:unhealthy, {:open_failed, {{:part_ahead_of_progress, newest, last}, path}}}`.
- **Which records are written once.** A record the outbox guarantees, one carrying a
  confirmation token and a replay-stable identity (`flz.cell` and a numeric
  `flz.seq`), is written once: a re-driven copy at or below its Cell's watermark is
  confirmed without being written. Every other record is written at least once, because
  a seq off the effectively-once tier can repeat and a watermark over it would drop a
  record.
- **The directory rename is not synced.** Erlang cannot open a directory to sync it, so a
  rename is as durable as the filesystem makes it. A rename lost to a power failure
  loses a part whose records were not confirmed yet, and the re-drive writes them again.
- **Reading only what is committed.** A crash between a part's rename and its sidecar
  leaves a part the next start deletes and writes again. A consumer that must not see
  it reads, from inside the directory, only the parts at or below the sidecar's number:

```bash
last=$(printf 'part-%08d' "$(jq .part .progress)"); for f in part-*; do [[ -f $f && ! $f > $last ]] && cat "$f"; done
```

### Writing parts to a bucket

Under an S3 root `path` names the key prefix that stands for the directory, and `rename` is
the only layout. `commit: append`, given or taken as the default, is refused at enable
naming `commit`, because an object cannot be appended to:

```bash
flz plugin enable writer operator_file_sink \
  --operator_id=op1 \
  --hook=on_output \
  --Xpath=echo-out \
  --Xcommit=rename
```

- **The open roll is held in memory.** Batches join the roll in the controller's memory
  with their holds open, rather than a temp file, so `roll_bytes` is memory the sink holds:
  64 MiB by default, and at most 5 GiB, the most one upload carries. A larger `roll_bytes`
  is refused at enable naming it.
- **A roll, then a part.** The roll is committed by one `PUT` of `<path>/part-<N>`, then the
  sidecar `<path>/.progress` is written by a conditional put carrying `N`, and only then are
  the roll's holds confirmed. No temp object is ever written.
- **The reconcile** is the folder's: on start the sink lists the keys under `<path>/`,
  deletes a part one past the sidecar, and refuses a start with a part further ahead.
- **A worker left behind stops.** The sidecar write is refused while the stored part number
  is higher, which is what another worker leaves when a move started it while this one
  lingered. This worker then confirms nothing and stops, logging the stored number, and the
  outbox re-drives its records to the live worker. Both may have written the same
  `part-<N>` meanwhile, and the live worker's rewrite is the one that stays.

### The file it opens

- **A file the sink creates is private.** Erlang opens a file with no creation mode, so
  the sink creates it exclusively and then sets mode `0600`. For the moment between the
  two, the empty file carries the controller's own umask.
- **A file that already exists keeps its mode.** The sink does not own a file it did
  not create.
- **A symbolic link is not opened.** The root check follows a link that is there when
  the plugin is enabled and writes to where it lands; a path that is a link when the
  sink opens it is refused, reported as `{:unhealthy, {:open_failed, {:symlink, path}}}`.
- **A file moved away is reopened.** Before each batch the sink checks that the path
  still names the file it holds, and opens the path again when it does not, so a
  rotated or deleted file does not take later writes somewhere no reader looks.
- **One sink per path.** Two sinks appending to one file, from one controller or two,
  interleave their batches, and under `rename` each sink's start deletes the other's
  open roll. Nothing prevents it: a plugin cannot see another controller's writer.

### Health

| Verdict | When |
| --- | --- |
| `{:unhealthy, {:open_failed, reason}}` | the file cannot be opened, or under `rename` the directory refuses its reconcile |
| `{:unhealthy, {:write_stalled, error}}` | records are pending, in the queue or in an open roll, writes or a roll's commit are failing, and no batch has synced within `unhealthy_after_ms` |
| `{:unhealthy, :no_sink}` / `{:unhealthy, :worker_dead}` | no worker, or a dead one |
| `:healthy` | otherwise, including an idle sink |

## Telemetry

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:flezha, :plugin, :file_source, :shipped]` | `bytes`, `offset` | `plugin_id`, `path`, `channel` |
| `[:flezha, :plugin, :file_source, :truncated]` | `offset` (the one given up) | `plugin_id`, `path`, `channel` |
| `[:flezha, :plugin, :file_source, :failed]` | `consecutive_failures` or `offset` | `plugin_id`, `path`, `channel`, `reason` |
| `[:flezha, :plugin, :file_sink, :shipped]` | `count` | `plugin_id`, `path` |
| `[:flezha, :plugin, :file_sink, :refused]` | `bytes` | `plugin_id`, `path`, `reason` (a record holding a newline, parked in the dead-letter store) |
| `[:flezha, :plugin, :file_sink, :failed]` | `consecutive_failures` | `plugin_id`, `path`, `reason` |

The source's `shipped` is bridged to the observability bus as the metric `injected`
under source `:flezha_file_source`, and the sink's as `flushed` under
`:flezha_file_sink`; the rest are events, because a `reason` is read one at a time.

## Limits

- **One record per line.** No CSV, Parquet or other format, and no line format carrying
  metadata.
- **One file per plugin**, or under `commit: rename` one directory of parts. No directory
  watching, and nothing merges or removes old parts.
- **Polling.** Growth is noticed within one `poll_interval_ms`; there is no filesystem
  watcher.
- **A folder, or one S3 bucket prefix per controller.** No bucket or credentials per
  plugin, no multipart upload, and no reading every object under a prefix.
- **`channel` is not validated at attach.** As for every source, channel names are an
  SDK-side fact the controller never sees; an undeclared name surfaces on the runner as
  a dead-letter park under that name.

## See also

- [http_poll.md](http_poll.md): the smallest source provider, which the reader follows
- [postgres_sink.md](postgres_sink.md): the batching sink the writer follows
- [../configuration.md](../configuration.md): `FLEZHA_FILE_PLUGIN_ROOT` among every other knob
