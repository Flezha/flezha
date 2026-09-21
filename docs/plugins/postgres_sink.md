# Postgres sink

Transactional PostgreSQL sink (`:io_binding`). Per flush batch, ONE database
transaction upserts every record into `flezha_records`, keyed by `flz.key`, and
advances the per-Cell `flezha_sink_progress` watermark row. That single
transaction is the guarantee: effectively-once into the database for tier
operatives, plain inserts for everything else. See
[`../resilience/data_durability.md`](../resilience/data_durability.md),
"Delivery semantics".

## Contract

| | |
| --- | --- |
| Plugin type | `:io_binding` |
| Hooks | `:on_input`, `:on_output` |
| Downstream | one PostgreSQL database, over a pool the plugin owns |
| Delivery | the committed transaction IS the confirmation; it answers the outbox hold each guaranteed record carries |
| Overflow | delay, never loss: an enqueue past `queue_max` parks until a flush frees space |
| Failure posture | an unreachable database delays the flush; nothing is dropped |

## Parameters

<!-- provider-options:begin -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `database` | string | &mdash; | yes | Database name |
| `username` | string | &mdash; | yes | Role the sink connects as |
| `password` | string | &mdash; | no | Password, redacted once the plugin is prepared |
| `password_env` | string | &mdash; | no | Environment variable holding the password instead |
| `plugin_id` | string | &mdash; | no | Plugin id; defaults to a generated one |
| `hostname` | string | `"localhost"` | no | Database host |
| `port` | int | `5432` | no | TCP port |
| `pool_size` | int | `2` | no | Connections in the sink's pool |
| `linger_ms` | int | `25` | no | How long a batch waits to fill |
| `max_batch` | int | `500` | no | Records in one flush transaction |
| `queue_max` | int | `10000` | no | Records held before enqueue blocks |
| `retry_backoff_ms` | int | `250` | no | First delay after a failed flush |
| `retry_max_backoff_ms` | int | `5000` | no | Ceiling for that delay |
| `unhealthy_after_ms` | int | `30000` | no | Age of the last commit past which a backed-up sink is unhealthy |
| `ssl` | bool | `false` | no | TLS to the server |
| `auto_create` | bool | `false` | no | Create the target tables when missing |
<!-- provider-options:end -->

Every other key is refused at enable rather than ignored, so a config asking for
something the sink does not offer fails loudly instead of reading as accepted.

The password is read once, inside the connection closure: it never lands in the
worker's state, the supervisor's child arguments, a log line or a telemetry
label. Prefer `password_env`, which names an environment variable, because an
inline password cannot survive a restart from the redacted stored options.

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
so a sink on an operative whose SDK side never binds costs nothing. See
[`lifecycle.md`](lifecycle.md).
