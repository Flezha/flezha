# Postgres poll source

The Postgres poll source delivers the new, and optionally the changed, rows of one Postgres
table onto a relay's entry channel, with no replication rights: it works on a managed
database and on a read replica. It reads the table past its cursor every `interval_ms`,
oldest first, and injects each row as one JSON object.
`Flezha.Plugin.Provider.PostgresPoll` is its provider, under the provider id
`relay_postgres_poll`.

| | |
| --- | --- |
| Type | relay plugin |
| Ships | built in |
| Shareable | no: one cursor per binding, so a second relay bound to it is refused (`plugin.not_shareable`) |
| Partitioned relays | refused (`plugin.partitioned_host`): a row's key decides which member must take it |
| Durability | each batch waits for the relay's verdicts and the cursor moves only past the rows it took; with durable input the cursor is committed after every batch, otherwise when the reader catches up and at most every `commit_interval_ms` |

## Options

<!-- provider-options:begin -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `database` | string | none | yes | Database name |
| `username` | string | none | yes | Role the plugin connects as |
| `password` | string | none | no | Password, redacted once the plugin is prepared |
| `password_env` | string | none | no | Environment variable holding the password instead |
| `hostname` | string | `"localhost"` | no | Database host |
| `port` | int | `5432` | no | TCP port |
| `ssl` | bool | `false` | no | TLS to the server |
| `channel` | names | none | no | The channel this plugin is attached to: the entry channel every row is injected on, and one name. Required, unless `channel_in` names it |
| `channel_in` | names | none | no | Entry channel every row is injected on, and one name; the explicit spelling of `channel`. Required, unless `channel` names it |
| `plugin_id` | string | none | no | Plugin id; defaults to the id it was enabled under |
| `table` | string | none | yes | The table to read: `name` or `schema.name`, case-sensitive |
| `id_column` | string | none | yes | A column that is unique and grows with new rows |
| `timestamp_column` | string | none | no | A column the writer sets on insert and on update; updated rows come again |
| `columns` | names | none | no | The columns to read, comma-separated; every column when unset |
| `start_at` | enum | `"beginning"` | no | Where a binding with no committed cursor starts: the first row, or after the rows present when it first reads |
| `batch_size` | int | `500` | no | Rows in one read |
| `interval_ms` | int | `1000` | no | Pause between reads once caught up, and after a failed read |
| `timestamp_delay_ms` | int | `0` | no | Rows whose timestamp is younger than this are left for a later read |
| `commit_interval_ms` | int | `1000` | no | Longest a new cursor waits to be committed while full batches are read, unless the relay's input is durable |
| `query_timeout_ms` | int | `30000` | no | Statement timeout of one read |
| `failure_threshold` | int | `3` | no | Consecutive failed reads or refused rows before the plugin reports unhealthy |
| `idle_after_ms` | int | `0` | no | Age of the last settled read past which the source is idle; 0 derives it from `interval_ms` |
<!-- provider-options:end -->

## Example

```sh
$ flz plugin enable orders-in relay_postgres_poll --relay_id=orders \
  --Xhostname=db.internal --Xdatabase=shop --Xusername=flezha_reader \
  --Xpassword_env=SHOP_DB_PASSWORD --Xtable=sales.orders --Xid_column=id \
  --Xtimestamp_column=updated_at --Xtimestamp_delay_ms=5000 --Xchannel=orders
```

## Contract

| | |
| --- | --- |
| Hooks | none needed: the reader drives itself, so a binding names no hook (the provider also defines `:on_terminate`) |
| Shared | no: one cursor per binding, so a second relay bound to it is refused (`plugin.not_shareable`), and a member of a partition group is refused (`plugin.partitioned_host`), because a row's key decides which member must take it |
| Upstream | one table or view, read by a role that may `SELECT` it, on one connection the plugin owns |
| Injection | one row as one JSON object, on the entry channel `:channel` (or `:channel_in`) names, with `flz.msg_id` set |
| Delivery | each batch waits for the relay's verdicts, and the cursor moves only past the rows it took; a refused row is read again |
| Failure posture | inject nothing, keep the cursor, read again after `interval_ms`, count toward health |

As a plugin spec at relay start (`flz relay start --plugins`):

```json
[
  {
    "name": "orders-in",
    "provider": "relay_postgres_poll",
    "opts": {
      "hostname": "db.internal",
      "database": "shop",
      "username": "flezha_reader",
      "password_env": "SHOP_DB_PASSWORD",
      "table": "sales.orders",
      "id_column": "id",
      "timestamp_column": "updated_at",
      "timestamp_delay_ms": "5000",
      "channel": "orders"
    },
    "hooks": ["on_terminate"]
  }
]
```

## Options in detail

`table`, `id_column`, `timestamp_column` and every entry of `columns` must match
`^[A-Za-z_][A-Za-z0-9_$]{0,62}$`, `table` with one optional `schema.` in front. Anything else
is refused when the plugin is enabled (`plugin.option_invalid`), before any connection is
opened. Names are double-quoted in the query, so they are case-sensitive: `Orders` is not
`orders`. A `table` with no schema is resolved through the role's `search_path` when the
plugin first reads.

The password is resolved inside the connection closure at every connect and never lands in
the reader's state, a log line or a telemetry label; the prepared options carry
`**redacted**` in its place. Prefer `password_env`: an inline `password` is still part of the
plugin's stored options.

## The two cursor modes

**Id only.** With `id_column` alone the cursor is the id: each read takes the rows whose id is
above the last one delivered, in id order. New rows are read, updates are not. The id must
grow with new rows: a row whose lower id commits after a higher one was read (two writers, a
long transaction) is skipped.

**Timestamp and id.** With `timestamp_column` as well the cursor is the pair, compared as a row:
each read takes the rows whose `(timestamp, id)` is past the last one delivered, in that order,
so a row updated later comes again, with a new message id. This asks two things of the writer:
it sets the timestamp on every insert and every update (an update that leaves it alone is not
seen), and a row is not committed with a timestamp older than one already read.
`timestamp_delay_ms` holds back rows younger than that much, which is the room a transaction
that commits late gets. Rows whose timestamp is NULL are never read.

Index the cursor: `(id_column)`, or `(timestamp_column, id_column)`. The plugin creates no
index, and without one every read scans the table.

Deletes are not seen in either mode: polling reads rows that exist. Change capture from the
write-ahead log is the answer when deletes matter.

`start_at` places a binding that has no cursor yet: `beginning` reads from the first row, and
`end` reads the largest id (and timestamp) once and delivers only what comes after. The
placed cursor is committed at once, so a restart never places it again.

## The query

The plugin builds the query; there is no way to write one. One catalog read when it starts
resolves the table and reads each column's type, which is also how a missing table or column
is noticed. Then each read is:

```sql
-- id only
SELECT <columns>, r.<id>::text FROM <schema>.<table> r
WHERE (r.<id>) > (($1::text)::<id type>) ORDER BY r.<id> LIMIT $2
-- timestamp and id
SELECT <columns>, r.<ts>::text, r.<id>::text FROM <schema>.<table> r
WHERE (r.<ts>, r.<id>) > (($1::text)::<ts type>, ($2::text)::<id type>)
  AND r.<ts> < now() - $3::bigint * interval '1 millisecond'
ORDER BY r.<ts>, r.<id> LIMIT $4
```

Every value is a parameter. Each read runs in one transaction made read-only, with a statement
timeout of `query_timeout_ms`, in a session set to UTC. A full batch is followed by the next
read at once; a short one means the reader caught up, and it waits `interval_ms`.

## A row as an input

Each row is injected as one JSON object, column name to value, in column order:

| Postgres type | JSON |
| --- | --- |
| `smallint`, `integer`, `bigint` | number |
| `real`, `double precision` | number; `NaN` and the infinities as the strings `"NaN"`, `"Infinity"`, `"-Infinity"` |
| `boolean` | `true` / `false` |
| `numeric` | string, every decimal kept (`"12.3400"`) |
| `text`, `varchar`, `char` | string |
| `date`, `time`, `timestamp`, `timestamptz` | ISO 8601 string; `timestamptz` in UTC with a `Z` |
| `uuid` | string, canonical form |
| `json`, `jsonb` | the JSON value |
| `bytea` | base64 string |
| an array of any of these | array of the same mapping |
| `NULL` | `null` |
| anything else | Postgres's text form, as a string |

Its metadata carries the entry channel and `flz.msg_id`: `<plugin_id>/<schema.table>/<id>`
with only an id column, `<plugin_id>/<schema.table>/<timestamp>/<id>` with a timestamp column,
the cursor values in Postgres's text form. A row read twice has one id, and a row updated later
has a new one.

## Delivery

Every batch is handed to the relay with `Host.inject_many/4` and its verdicts waited for,
with no timeout. The cursor moves past the longest run of rows the relay took; a row it
refused is read again on the next read, and so are the rows after it, up to
`FLEZHA_PLUGIN_INGEST_WINDOW` of which may already have been taken.

The cursor is committed to the binding (its internal option `cursor`), so a plugin restart, a
move to another controller and a controller restart continue where it stopped. How often
follows the relay's input:

- **Durable input** (effectively-once, sleeping or durable steps): a verdict means the row is in
  the relay's input log, and the cursor is committed after every batch. A row read again
  after a restart is absorbed by its `flz.msg_id` while the input log still holds it; with
  effectively-once the relay's outputs are effectively once on top of that.
- **Otherwise**: a verdict means the relay accepted the row, and the cursor is committed once
  the reader catches up or a read fails, and at most every `commit_interval_ms` while it reads
  full batches. A crash can lose rows taken but not processed, and reads again up to
  `commit_interval_ms` of rows.

A commit is written in the background, so two close together can land out of order and the
stored cursor can move back: that reads rows again, it never skips one. A commit at stop does
not land when the relay itself is already stopping.

The cursor is stored with the `table`, `id_column` and `timestamp_column` it was read under. A
binding brought up with others (after `flz plugin update`, say) drops it and starts from
`start_at`.

## Health

| Verdict | When |
| --- | --- |
| `{:unhealthy, {:table_missing, table}}` | the table does not exist, or the role cannot see it; at once, until a read succeeds |
| `{:unhealthy, {:column_missing, column}}` | a cursor column or a `columns` entry does not exist; at once, until a read succeeds |
| `{:unhealthy, {:read_failed, error}}` | `failure_threshold` reads in a row failed, or the relay refused a row as many times; `error` is the server's code and message |
| `{:unhealthy, {:poll_idle, age_ms}}` | no read has settled within `idle_after_ms` (three `interval_ms` when 0) |
| `{:unhealthy, :no_reader}` / `{:unhealthy, :reader_dead}` | no reader, or a dead one |
| `:healthy` | otherwise, including a table that does not grow |

A failed read is tried again after `interval_ms`; the connection reconnects by itself.

## Telemetry

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:flezha, :plugin, :postgres_poll, :shipped]` | `count` (rows taken in one batch) | `plugin_id`, `table`, `channel` |
| `[:flezha, :plugin, :postgres_poll, :failed]` | `consecutive_failures` | `plugin_id`, `table`, `channel`, `reason` |
| `[:flezha, :plugin, :postgres_poll, :caught_up]` | `count` (1) | `plugin_id`, `table`, `channel` |

`caught_up` is emitted once each time the reader reaches the end of the table after reading
full batches. `shipped` is bridged to the observability bus as the metric `injected` under the
source `:flezha_postgres_poll`; the other two are events. No lag is measured: a count per read
would cost more than the read.

## Limits

- **One table per plugin**, and no `WHERE` filter or query of your own.
- **No deletes**, and in id-only mode no updates.
- **Polling.** A new row is seen within one `interval_ms` once the reader has caught up.
- **A partitioned relay is refused.** Bind the source to a relay that is not
  partitioned and route from there.
- **`channel` is checked at bind.** As for every source, a name the relay's runners did not
  announce as an entry channel is refused with `api.channel_unknown`; a relay started with
  `--plugins`, or whose runners announced no channels, is not checked.

## See also

- [postgres_output.md](postgres_output.md): the Postgres output plugin, which shares the connection options
- [file.md](file.md): the file source, whose cursor and commit cadence this reader follows
- [plugins.md](../topics/plugins/plugins.md): the plugin model and every source and output plugin
