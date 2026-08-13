# Scheduled operative spawns (`flz schedule`)

The controller can spawn a fresh **workflow** operative instance on a cron
schedule — reports, cleanups, periodic analyses — without an external
scheduler. A schedule is a durable registry entry; at each matching slot the
controller starts an instance through the exact same path as
`flz operative start` (quota, plugins, teardown and retention all
inherited).

## Command surface

```
flz schedule add <name> --cron "<expr>" --tz <iana> --factory <factory_id>
                 [--namespace <ns> | -n <ns>] [--init_values <json> | -iv <json>]
                 [--plugins <json> | -p <json>] [--kind workflow]
flz schedule list
flz schedule remove <name>
flz schedule run <name>
```

- `add` validates the cron expression, the time zone and the schedule name
  (letters/digits/`_`/`-`, at most 11 bytes so the composed instance id
  `<name>@<slot-iso>` fits the 32-byte operative id limit) before storing
  anything; a factory with no live runner is accepted with a warning (the
  runner may connect later — until it does, slots are retried on backoff).
- `list` shows every entry with the node currently running it, its next
  fire time (UTC), the last fired instance (`LAST FIRE (UTC)`) and its
  result (`LAST RESULT`: `ok`, `duplicate`, or `retrying: <reason>`;
  `-` before the first fire). `NODE` is `-` when the schedule is not
  registered in the hub (its owner has not asserted it yet, or the hub is
  down on this node); a job that does not answer the status query in time
  still lists with its node.
- `run` fires one instance immediately with the stored configuration, under
  a manual id (`<name>@manual-<unix-ts>`) distinct from cron slots.
- `--plugins` takes the same JSON array as `flz operative start --plugins`
  and is validated against the enabled providers at `add` time. See
  [Output plugins](#output-plugins) — a workflow job that produces anything
  needs it.

Example:

```
flz schedule add digest --cron "0 8,20 * * *" --tz Europe/Tallinn --factory ai_digest
```

## Output plugins

A workflow instance runs to completion in milliseconds, so its plugins must be
attached **at spawn**; enabling one afterwards misses the whole run. A
schedule without `--plugins` therefore fires a job with no output channel —
it executes and everything it emits is dropped.

```
flz schedule add digest --cron "0 8,20 * * *" --tz Europe/Tallinn \
  --factory ai_digest --init_values '{"window_hours":12}' \
  --plugins '[{"id":"ai_reports","provider":"operative_kafka_producer",
               "subscriptions":["on_output"],
               "opts":{"channel":"report_out","hosts":"kafka:9092",
                       "topic":"ai.reports.v1","required_acks":"-1"}}]'
```

- The spec is stored verbatim and **decoded at every firing**, against the
  providers the running controller has enabled — a decoded provider module is
  runtime state and is never frozen into the registry.
- Each firing stamps its own namespace and instance id on every plugin, and
  **prefixes the plugin id with the instance id**: the `ai_reports` above
  becomes `digest@2026-07-27T05:00:00Z_ai_reports`. Overlapping slots
  therefore never claim one plugin id, and you never have to encode the slot
  yourself.
- If the spec no longer decodes at fire time (say the provider was disabled),
  **no instance is started**: the slot is logged and reported on
  `[:flezha, :schedule, :spawn_failed]`. A run that spends its work and drops
  the result is worse than a visibly skipped slot.

## Cron expressions

Standard five fields — `minute hour day-of-month month day-of-week` —
numeric only, with `*`, values, ranges (`a-b`), steps (`*/n`, `a-b/n`) and
comma lists. Day-of-week is `0-7` (both `0` and `7` are Sunday). When both
day-of-month and day-of-week are restricted, a day matches if **either**
matches (vixie rule).

Slots are evaluated in the entry's IANA time zone:

- a wall time erased by a spring-forward transition **skips** that day;
- a wall time repeated by a fall-back transition fires **once**, at the
  first occurrence.

## Semantics

- **Deterministic instance id `<name>@<slot-iso>`** (UTC), e.g.
  `digest@2026-07-27T05:00:00Z`. The id doubles as the duplicate-fire
  guard: a second fire attempt for the same slot gets "operative id already
  in use" and no-ops. It also makes `flz operative list` output legible.
- **Missed slots are skipped, never back-filled.** Each schedule process
  holds an armed timer for its next slot, so the only gap is its own absence
  (controller restart, migration, node takeover). On start it fires the
  single most recent slot inside `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS`
  (default 120 s) and drops everything older. Periodic jobs must be
  self-contained per firing. Set the grace to `0` to opt out of the
  look-back entirely.
- **Overlap is permitted.** If `<name>@<t1>` is still running at slot
  `t2`, `<name>@<t2>` spawns normally (the ids differ). Bounding runtime is
  the author's responsibility (`max_duration`).
- **The fire is routed at fire time, not pinned to the schedule's node.**
  The schedule process lives wherever the `:flz_schedules` hub hashes it —
  possibly a controller node with no runner for the entry's namespace. The
  spawn goes through the shared location-transparent start path: if the
  local node cannot satisfy it, the cluster directory names the nodes whose
  runners advertise the factory and the start executes on one of them,
  chosen deterministically. A slot fails only when **no** cluster node
  holds such a runner, and the refusal says so cluster-wide. Where the
  schedule process happens to live never affects the outcome — a rolling
  update that moves the runner between nodes needs no operator action.
- **A failed slot is retried until its next slot supersedes it.** Retries
  use exponential backoff (1 s base, capped at one minute — the finest cron
  granularity) under the SAME `<name>@<slot-iso>` id, so a retry racing any
  other fire attempt no-ops on the duplicate-id guard (`duplicate` counts
  as success). When the next slot fires, any pending retry is cancelled:
  at most one slot is ever in flight, and abandoned slots are never
  back-filled. The schedule never parks and needs no operator action — a
  schedule added before its runner deploys simply keeps probing.
- **Workflow only.** Stream operatives are owned by convergence; the
  scheduler refuses any other kind.
- **Quotas bound each executing node separately.** Quota checks run on the
  node the start executes on, against its own stores — a routed start is
  not counted against the origin node's view.

## Durability and multi-controller clusters

Two layers, deliberately separated:

- **The definition** persists in node-local DETS at
  `<FLEZHA_SCHEDULE_DIR>/schedules.dets` (default `app_data/schedules`) —
  the same posture as the other control-plane registries — and survives
  controller restarts.
- **Who runs it** is decided by the dedicated `:flz_schedules` ProcessHub.
  Every controller asserts the definitions its own store holds; the hub's
  `Singularity` redundancy leaves **exactly one schedule process cluster-wide**
  per name, and the losing assertions no-op. There is no leader election and
  no cross-node state sync. The hub is transient (in-memory registry, nothing
  on disk) — it arbitrates, it does not remember.

If the node running a schedule leaves the cluster, ProcessHub re-places the
job on a survivor and the next slot fires there.

**Define every schedule on every controller.** The store is node-local, so a
definition that exists only on node A is lost if A is decommissioned. Adding
it everywhere costs nothing — the hub still runs exactly one — and buys
redundancy.

**Migration and restart can re-fire one slot.** A restarted or migrated job
does the grace look-back described above. If that slot's instance already
completed, the `<name>@<slot>` id guard no longer holds and the slot fires
again. This is the same at-least-once posture as before; set
`FLEZHA_SCHEDULE_MISFIRE_GRACE_MS=0` if a duplicate is worse than a miss.

A network partition is not protected: the hub uses divergence tolerance, so
both sides keep running their schedules.

## Configuration

| Env var | Default | Meaning |
| --- | --- | --- |
| `FLEZHA_SCHEDULE_DIR` | `app_data/schedules` | Registry directory |
| `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS` | `120000` | Oldest a slot may be and still fire on process start |

## Observability

Telemetry events `[:flezha, :schedule, :fired]`,
`[:flezha, :schedule, :spawn_failed]` (carries the `attempt` number),
`[:flezha, :schedule, :dormant]`
(a schedule whose cron has no future slot, or is unparsable — the process
stays up holding its name but never fires) and
`[:flezha, :schedule, :hub_start_failed]`. Each carries `schedule` and,
where relevant, `instance_id` metadata; dormancy and spawn failures are
also logged with the schedule name. A persistently failing schedule is
also visible in `flz schedule list`'s `LAST RESULT` column.
