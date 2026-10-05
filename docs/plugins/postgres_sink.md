# Postgres sink

The Postgres sink writes what a relay emits into a PostgreSQL database, in
transactions. Per flush batch, one database transaction upserts every record into
`flezha_records`, keyed by `flz.key`, and advances the per-cell `flezha_sink_progress`
watermark row. That single transaction is the guarantee: effectively-once into the
database for an effectively-once relay, plain inserts for every other one. Its provider
id is `relay_postgres`.

| | |
| --- | --- |
| Type | relay plugin |
| Ships | built in |
| Shareable | no: a second relay bound to it is refused (`plugin.not_shareable`) |
| Partitioned relays | allowed: each member runs its own copy |
| Durability | the committed transaction confirms each delivery-guaranteed output; with `effectively_once` each output is written once |

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
| `plugin_id` | string | none | no | Plugin id; defaults to a generated one |
| `pool_size` | int | `2` | no | Connections in the sink's pool |
| `linger_ms` | int | `25` | no | How long a batch waits to fill |
| `max_batch` | int | `500` | no | Records in one flush transaction |
| `queue_max` | int | `10000` | no | Records held before enqueue blocks |
| `retry_backoff_ms` | int | `250` | no | First delay after a failed flush |
| `retry_max_backoff_ms` | int | `5000` | no | Ceiling for that delay |
| `unhealthy_after_ms` | int | `30000` | no | Age of the last commit past which a backed-up sink is unhealthy |
| `auto_create` | bool | `false` | no | Create the target tables when missing |
<!-- provider-options:end -->

## Example

```sh
$ flz plugin enable store relay_postgres --relay_id=orders --hook=on_output \
  --Xdatabase=flezha --Xusername=flezha --Xpassword_env=PGPASSWORD --Xauto_create=true
```

## Contract

| | |
| --- | --- |
| Hooks | `:on_input`, `:on_output` |
| Downstream | one PostgreSQL database, over a pool the plugin owns |
| Delivery | the committed transaction IS the confirmation; it answers the outbox hold each guaranteed record carries |
| Overflow | delay, never loss: an enqueue past `queue_max` parks until a flush frees space |
| Failure posture | an unreachable database delays the flush; nothing is dropped |

Delivery semantics in general are in [`durability.md`](../topics/basics/durability.md).

## Options in detail

Every other key is refused at enable rather than ignored, so a config asking for
something the sink does not offer fails loudly instead of reading as accepted.

The password is resolved inside the connection closure at every connect: it never
lands in the worker's state, the supervisor's child arguments, a log line or a
telemetry label, and the prepared options carry `**redacted**` in its place. Prefer
`password_env`, which names an environment variable: an inline `password` is still
part of the plugin's stored options. The connection options are the ones the
[Postgres poll source](postgres_poll.md) takes too.

## Schema

Two tables, created for you when `auto_create` is on:

```sql
CREATE TABLE flezha_records (
  key text PRIMARY KEY, cell text, seq text, epoch bigint,
  payload bytea, updated_at timestamptz
);

CREATE TABLE flezha_sink_progress (
  cell text PRIMARY KEY, last_seq text, epoch bigint
);
```

Every upsert is guarded by `(epoch, seq)` monotonicity, so a re-driven batch and
a stale epoch are both no-ops. With `auto_create` off and either table missing,
the plugin reports `{:unhealthy, {:missing_tables, …}}` and the convergence loop
escalates.

## Health

| Verdict | When |
| --- | --- |
| `{:unhealthy, :no_sink}` | the worker was never started |
| `{:unhealthy, :worker_dead}` | the worker died and has not come back |
| `{:unhealthy, {:missing_tables, tables}}` | `auto_create` is off and a target table is absent |
| `{:unhealthy, {:commit_stalled, error}}` | records are pending, flushes are failing, and nothing has committed within `unhealthy_after_ms` |
| `:healthy` | anything else, including an idle sink |

## Lifecycle

The sink worker is this provider's declared worker: the extension table starts it
at activation, under the provider's own family, keyed by `plugin_id`, with
`restart: :permanent`. A `:prepared` plugin holds no connection and no process,
so a sink on a relay whose SDK side never binds costs nothing.

## See also

- [`postgres_poll.md`](postgres_poll.md) and [`postgres_cdc.md`](postgres_cdc.md): Postgres as an input
- [`plugins.md`](../topics/plugins/plugins.md): the plugin model
- [`effectively_once.md`](../topics/advanced/effectively_once.md): writing each output once
