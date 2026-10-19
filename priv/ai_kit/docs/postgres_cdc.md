# Postgres change capture

The Postgres change capture plugin delivers every inserted, updated and deleted row of
Postgres tables onto a relay's entry channel, in commit order, after a first copy of the
rows already there. It reads the database's write-ahead log through a logical replication
slot with Postgres's built-in `pgoutput` format. `FlezhaPostgresCdc` is its provider, under
the provider id `postgres_cdc`. To read a table by polling instead, which sees no deletes,
see [postgres_poll.md](postgres_poll.md).

| | |
| --- | --- |
| Type | relay plugin |
| Ships | package `flezha_postgres_cdc` |
| Shareable | no: one slot per binding, so a second relay bound to it is refused (`plugin.not_shareable`) |
| Partitioned relays | refused (`plugin.partitioned_host`) |
| Durability | each hand-over waits for the relay's verdicts; the position confirmed to Postgres moves after every accepted transaction with durable input, otherwise at most every `commit_interval_ms` |

## Options

<!-- provider-options:begin postgres_cdc -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `database` | string | none | yes | Database name |
| `username` | string | none | yes | Role the plugin connects as |
| `password` | string | none | no | Password, redacted once the plugin is prepared |
| `password_env` | string | none | no | Environment variable holding the password instead |
| `hostname` | string | `"localhost"` | no | Database host |
| `port` | int | `5432` | no | TCP port |
| `ssl` | bool | `false` | no | TLS to the server |
| `channel` | names | none | no | The channel this plugin is attached to: the entry channel every change is injected on, and one name. Required, unless `channel_in` names it |
| `channel_in` | names | none | no | Entry channel every change is injected on, and one name; the explicit spelling of `channel`. Required, unless `channel` names it |
| `plugin_id` | string | none | no | Plugin id; defaults to the id it was enabled under |
| `slot` | string | none | yes | The replication slot, one per binding: lower-case letters, digits and underscores, at most 58; the first copy reads through a temporary `<slot>_copy` |
| `publication` | string | none | yes | The publication to read |
| `tables` | names | none | no | With it, the publication is created `FOR TABLE` these, `name` or `schema.name`, when it does not exist |
| `snapshot` | enum | `"initial"` | no | `initial` copies the published tables before streaming; `never` only streams |
| `snapshot_batch` | int | `1000` | no | Rows per fetch of the copy |
| `batch_size` | int | `500` | no | Most changes of one transaction handed over at once |
| `commit_interval_ms` | int | `5000` | no | Longest the read position waits to be confirmed while changes stream, unless the relay's input is durable |
| `max_lag_bytes` | int | `1073741824` | no | Write-ahead log the slot may hold back before the plugin reports unhealthy |
| `lag_check_ms` | int | `30000` | no | How often the slot's lag is read |
| `failure_threshold` | int | `3` | no | Refused hand-overs in a row before the plugin reports unhealthy |
<!-- provider-options:end -->

## Example

```sh
$ flz plugin enable rates-cdc postgres_cdc --relay_id=pricer \
  --Xhostname=db --Xdatabase=market --Xusername=cdc --Xpassword_env=CDC_PASSWORD \
  --Xslot=rates_cdc --Xpublication=rates_pub --Xchannel=rates
```

## Installing

Build the archive from the package (its README: `mix flezha.plugin.package`), then install
`flezha_postgres_cdc-0.1.0.fpz` on the controller as
[`installing_plugins.md`](../topics/plugins/installing_plugins.md#install-uninstall-and-sync)
describes.

The package bundles no library: it uses the Postgres driver and connection options the
release ships.

## Preparing the database

- **Logical replication.** The server runs with `wal_level = logical` (a restart is needed to
  change it), and `max_replication_slots` and `max_wal_senders` leave room for one more slot
  and one more sender per binding. Postgres 14 or newer.
- **The role** the plugin connects as has the `REPLICATION` attribute and `SELECT` on every
  published table. To have the plugin create the publication from `tables`, it also needs
  `CREATE` on the database and ownership of those tables; otherwise create the publication
  yourself:

  ```sql
  CREATE PUBLICATION rates_pub FOR TABLE public.rates;
  ```

- **Replica identity.** Every published table has a primary key, an identity index
  (`REPLICA IDENTITY USING INDEX`), or `REPLICA IDENTITY FULL`. Under `FULL` an update and a
  delete carry the whole old row; under the other two, the key alone.

The plugin checks all of this when it starts. A binding is accepted either way: a failed check
makes the plugin report unhealthy with the code below, and it creates no slot and no
publication until the check passes. It checks again along its backoff curve.

| Code | When |
| --- | --- |
| `postgres_cdc.wal_level_not_logical` | the server's `wal_level` is not `logical` |
| `postgres_cdc.server_too_old` | the server is older than 14 |
| `postgres_cdc.publication_missing` | the publication does not exist and `tables` is not given |
| `postgres_cdc.replica_identity_missing` | a published table, or one of `tables`, has no primary key under the default identity, an identity index that was dropped, or identity `NOTHING` |

## The plugin

| | |
| --- | --- |
| Hooks | none needed: the plugin drives itself, so a binding names no hook (the provider also defines `:on_terminate`) |
| Shared | no: one slot per binding, so a second relay bound to it is refused (`plugin.not_shareable`), and a member of a partition group is refused (`plugin.partitioned_host`) |
| Upstream | one publication, read through one logical replication slot |
| Injection | one row change as one input, a JSON object, on the entry channel `:channel` (or `:channel_in`) names |
| Delivery | each hand-over waits for the relay's verdicts; the read position moves only past changes the relay took |

Enabled and bound in two commands, with the settings written without their dashes:

```sh
$ flz plugin enable rates-cdc postgres_cdc Xhostname=db Xdatabase=market Xusername=cdc \
  Xpassword_env=CDC_PASSWORD Xslot=rates_cdc Xpublication=rates_pub Xchannel=rates
$ flz plugin bind rates-cdc --relay_id pricer --hook=on_terminate
```

A literal `password` is redacted once the plugin is prepared and held in memory only;
`password_env` is read again at every connect. Neither is written to a log line, a telemetry
label, `flz plugin list` or `flz relay info`. The replication connection keeps the
password in its own process, which is marked sensitive, so no process inspection or crash
report shows it.

## Options in detail

`publication` and each `tables` entry are identifiers, quoted when they reach a statement, so
their case is kept.

## A change as an input

The payload is one JSON object:

```json
{"op": "u", "schema": "public", "table": "rates",
 "before": {"pair": "EUR.USD"}, "after": {"pair": "EUR.USD", "bid": 1.0842, "notes": null},
 "unchanged": [], "lsn": "0/1A2B3C4D", "xid": 1234,
 "commit_time": "2026-10-01T12:00:00.123456Z", "seq": 3}
```

| Field | Value |
| --- | --- |
| `op` | `c` insert, `u` update, `d` delete, `t` truncate, `r` a row of the first copy |
| `schema`, `table` | the row's table |
| `before` | what Postgres sends of the old row: the key columns, or every column under `REPLICA IDENTITY FULL`; `null` for an insert, and for an update that keeps the key under the other identities |
| `after` | the new row; `null` for a delete |
| `unchanged` | the TOAST columns the change left alone, which Postgres does not send again; they are left out of `after` |
| `lsn` | the transaction's commit LSN; for a copied row, the slot's consistent point |
| `xid`, `commit_time` | the transaction's id and commit time; `null` for a copied row |
| `seq` | the change's position in its transaction, from 1; for a copied row, its position in the copy of its table |

A truncate carries `tables` (`schema.table` each) in place of `schema`, `table`, `before`,
`after` and `unchanged`.

Column values follow [postgres_poll.md](postgres_poll.md)'s JSON mapping, arrays included:
the same row gives the same JSON whether it was copied or streamed. A type the mapping does not
know is its text.

The metadata carries the entry channel and:

| Key | Value |
| --- | --- |
| `flz.msg_id` | `<plugin_id>/<lsn>/<seq>` for a streamed change; `<plugin_id>/snapshot/<lsn>/<row>` for a copied row, `<lsn>` the copy's consistent point and `<row>` the JSON array of its schema, its table and its key columns' text, or its position in the copy for a table under `FULL` with no primary key: `["public","rates","EUR.USD"]` |
| `pg.table` | `schema.table`; for a truncate, every table it cut, comma-separated |
| `pg.op` | the `op` |

A change streamed again carries the same `flz.msg_id`, so the input log absorbs the copy
while it holds the first one.

## The first copy

When `snapshot` is `initial` (the default) and the slot does not exist, the plugin creates a
temporary slot, `<slot>_copy`, with an exported snapshot and reads every published table in
that snapshot, `snapshot_batch` rows at a time through a server-side cursor, ordered by the key
when there is one, handing each batch to the relay as events with `op` `r`. The copy holds
exactly the transactions committed before the slot's consistent point and the stream every one
after it, so the two meet with nothing missed or doubled. Once the copy ends, the temporary
slot is kept as `slot`, at the same position, and the stream starts from it.

The slot itself is the record that the copy ended: an unbind, a disable, a restart or a move to
another controller finds it and streams on, without copying again, whatever became of the
binding.

- A batch the relay refuses during the copy is handed over again along the backoff curve,
  from where the copy stands; nothing starts over.
- A copy cut short (a crash, a lost connection) leaves no slot: the temporary one ends with its
  connection. The next start copies every table again, from the start.
- Each copied row's message id names the copy's consistent point, so the rows of a later copy
  (after a copy cut short, after the slot was dropped or invalidated) are new inputs: the
  relay receives every row again, at its value in that copy, and a row it received from an
  earlier copy is not absorbed in its place. Rows copied before the cut reach the relay
  twice, once per copy.
- The copy runs in one `REPEATABLE READ` transaction, which holds back vacuum on the whole
  database for as long as it runs. Copy a large database at a quiet time.

With `snapshot: never` the plugin creates the slot and streams from it, copying nothing.

## Acknowledgement

The stream hands each transaction's changes to the relay in batches of at most
`batch_size`, and at its commit, and waits for every verdict. A transaction whose every change
was taken is accepted; the position confirmed to Postgres (which lets it free the log behind
it) moves to its end:

- **With durable input** (an `effectively_once` relay, or one that journals its input),
  after every accepted transaction. A change streamed again after a crash is absorbed by its
  message id while the input log holds it, so nothing is lost and nothing is doubled.
- **Without it**, at most every `commit_interval_ms`, and whenever the stream is idle. A crash
  loses what the relay had not processed, and the transactions accepted since the last
  confirmed position are streamed again and reach the relay twice.

A refused change, or a hand-over the relay does not answer within 20 seconds (well under
Postgres's `wal_sender_timeout`), stops the stream. The plugin starts it again from the last
confirmed position along its backoff curve, from half a second doubling to half a minute, and
the accepted changes before the refusal come again: absorbed with durable input, doubled
without.

## The slot

The slot keeps the write-ahead log the plugin has not confirmed, on the database server's
disk. The plugin reads how much every `lag_check_ms` and reports unhealthy with
`postgres_cdc.slot_lag` past `max_lag_bytes`.

The slot is kept when the plugin is disabled, unbound or its relay stops: dropping it would
lose the position, and the next start would copy every table again. Binding the plugin again,
to the same relay or another one, streams on from the slot's confirmed position. A slot nobody reads any
more keeps log until it is dropped by hand:

```sql
SELECT pg_drop_replication_slot('rates_cdc');
```

To bound what any slot may hold, set `max_slot_wal_keep_size` on the server: a slot past it is
invalidated and its plugin cannot stream again until the slot is dropped, and then copies
again.

Only one connection may read a slot. A plugin that starts while the connection before it is
still open on the server (a restart, a move to another controller) is refused with
`object_in_use`, and tries again along its backoff curve.

## Health

| Verdict | When |
| --- | --- |
| the four codes of the start checks | a check failed; see [Preparing the database](#preparing-the-database) |
| `postgres_cdc.replication_failed` | the replication connection was lost, or a statement of the copy or the checks failed; cleared once the stream runs again |
| `postgres_cdc.slot_lag` | the slot holds back more than `max_lag_bytes` |
| `{:read_failed, error}` | the last `failure_threshold` hand-overs were refused |

The worker is asked for its facts with a bounded probe, and runs its checks, its copy and its
lag reads off its own process, so it answers while they run.

## Telemetry

Under `[:flezha, :plugin, :postgres_cdc, verb]`, with the plugin id and the slot: `shipped`
(`%{count: n}`) after each hand-over the relay took, and `failed` (`%{count: n}`, with a
`reason`), the verbs every provider shares; `lag_bytes` (a metric, `%{bytes: n}`) after each
lag read; and `snapshot_done` (an event, `%{count: rows}`) when the first copy ends.

## Limits

- No DDL events: a column change shows up as the table's changed columns in the next change.
- Transactions are streamed once committed, never while in progress, and column values are
  read as text.
- Sequences, large objects and failover slots across a change of primary are not covered.
- One publication per plugin, and one slot per binding.

## See also

- [postgres_poll.md](postgres_poll.md): reading a table by polling, whose JSON mapping this plugin shares
- [postgres_output.md](postgres_output.md): Postgres as an output
- [kafka_consumer.md](kafka_consumer.md) and [rabbitmq.md](rabbitmq.md): the other source packages
- [plugins.md](../topics/plugins/plugins.md): the plugin model and every source and output plugin
