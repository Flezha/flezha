# Scheduled workflow spawns

For operators who run periodic jobs — reports, cleanups, analyses — as
**workflow** operatives on a cron schedule, without an external scheduler. A
schedule is a durable registry entry; at each matching slot the Controller
starts one instance through the same path as `flz operative start`, so quota,
plugins, teardown and retention are all inherited. This page covers the
`flz schedule` verbs, cron semantics, firing guarantees, durability and the
multi-controller model.

## Command surface

```text
flz schedule add <name> --cron "<expr>" --tz <iana> --factory <factory_id>
                 [--namespace <ns> | -n <ns>] [--init_values <json> | -iv <json>]
                 [--plugins <json> | -p <json>] [--kind workflow]
flz schedule list
flz schedule remove <name>
flz schedule run <name>
```

| Verb     | Behaviour                                                                                                                                                                                                                                                       |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `add`    | Validates name, cron, time zone, `--init_values` (a JSON object) and `--plugins` (against the enabled providers) before storing anything. Namespace defaults to `default`. Any `--kind` other than `workflow` is refused. A factory with no live runner is accepted with a warning. |
| `list`   | Columns `NAME CRON TZ FACTORY NAMESPACE NODE NEXT FIRE (UTC) LAST FIRE (UTC) LAST RESULT`. `NODE` is `-` when the hub has no owner for the entry (not asserted yet, or the hub is down on this node). `NEXT FIRE` is `never` for a cron with no future slot. `LAST RESULT` is `ok`, `duplicate`, `retrying: <reason>` (cut at 48 chars) or `-` before the first fire; a job that does not answer within 1 s still lists with its node. |
| `remove` | Deletes the row, then stops the job cluster-wide and waits for it (up to 5 s) so no further slot fires after the row is gone.                                                                                                                                  |
| `run`    | Fires one instance now with the stored configuration under the id `<name>@manual-<unix-ts>`. Works on any node whether or not the schedule hub is up.                                                                                                          |

Names match `[A-Za-z0-9][A-Za-z0-9_-]*` and are at most 11 bytes, so the
composed instance id `<name>@<slot-iso>` fits the 32-byte operative id limit.

```bash
flz schedule add digest --cron "0 8,20 * * *" --tz Europe/Tallinn --factory ai_digest
```

## Output plugins

A workflow instance finishes in milliseconds, so its plugins must be attached
**at spawn**; a schedule without `--plugins` fires a job with no output channel
and everything it emits is dropped.

```bash
flz schedule add digest --cron "0 8,20 * * *" --tz Europe/Tallinn \
  --factory ai_digest --init_values '{"window_hours":12}' \
  --plugins '[{"id":"ai_reports","provider":"operative_kafka_producer",
               "subscriptions":["on_output"],
               "opts":{"channel":"report_out","hosts":"kafka:9092",
                       "topic":"ai.reports.v1","required_acks":"-1"}}]'
```

- `--plugins` takes the same JSON array as `flz operative start --plugins`.
  The spec is stored verbatim and **decoded at every firing** against the
  providers the running Controller has enabled; a decoded provider module is
  never frozen into the registry.
- Each firing prefixes every plugin id with the instance id
  (`digest@2026-07-27T05:00:00Z_ai_reports`), so overlapping slots never claim
  one plugin id.
- If the spec no longer decodes at fire time (the provider was disabled),
  **no instance starts**: the slot is logged and reported on
  `[:flezha, :schedule, :spawn_failed]`.

## Cron expressions

Five numeric fields — `minute hour day-of-month month day-of-week` — with `*`,
values, ranges (`a-b`), steps (`*/n`, `a-b/n`) and comma lists. A step on a
single value (`30/10`) is rejected. Day-of-week is `0-7`, both `0` and `7`
Sunday. When day-of-month and day-of-week are both restricted a day matches if
**either** does (vixie rule). An expression that never matches (`0 0 30 2 *`)
is accepted and the schedule goes dormant.

Slots are evaluated in the entry's IANA time zone: a wall time erased by a
spring-forward transition **skips** that day; a wall time repeated by a
fall-back transition fires **once**, at the first occurrence.

## Firing semantics

- **Deterministic instance id `<name>@<slot-iso>`** (UTC), e.g.
  `digest@2026-07-27T05:00:00Z`. The id is the duplicate-fire guard: a second
  attempt for the same slot gets the registry's duplicate verdict and no-ops.
- **Missed slots are skipped, never back-filled.** Each schedule process holds
  an armed timer for its next slot; the only gap is its own absence (restart,
  migration, node takeover). On start it fires the single most recent slot
  inside `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS` (default 120 s), never older than
  the entry's creation — a freshly added schedule never fires history. Set the
  grace to `0` to opt out of the look-back.
- **Overlap is permitted.** `<name>@<t2>` spawns while `<name>@<t1>` still
  runs; bounding runtime is the author's job (`max_duration`).
- **Routed at fire time, not pinned.** The schedule process lives wherever the
  `:flz_schedules` hub hashes it. The spawn goes through the location-
  transparent start path: if the local node has no runner for the factory, the
  `:pg` cluster directory names the nodes whose runners advertise it and the
  start executes on one of them, chosen deterministically. A slot fails only
  when **no** node holds such a runner, and the refusal says so cluster-wide.
- **A failed slot is retried until its next slot supersedes it.** Exponential
  backoff, 1 s base, capped at one minute, under the SAME instance id — so a
  retry racing another attempt no-ops on the duplicate guard (`duplicate`
  counts as success). The next slot cancels any pending retry: at most one
  slot is ever in flight. The schedule never parks; one added before its
  runner deploys simply keeps probing.
- **Dormant, not dead.** An unparsable cron or one with no future slot keeps
  its process (and its hub claim) but never fires; `[:flezha, :schedule,
  :dormant]` reports it.
- **Workflow only.** Stream operatives are owned by convergence.
- **Quotas bound each executing node separately.** A routed start is checked
  on the node it executes on, against that node's stores.

## Durability and multi-controller clusters

Two layers, deliberately separate:

- **The definition** lives in node-local DETS at
  `<FLEZHA_SCHEDULE_DIR>/schedules.dets` (default `app_data/schedules`) and
  survives restarts. With a remote durability backend configured the file also
  ships as a control-plane store and is hydrated at boot, so a replacement
  controller comes back with its own schedules; the tier does not sync
  definitions between nodes.
- **Who runs it** is decided by the dedicated `:flz_schedules` ProcessHub:
  `Singularity` redundancy (exactly one job process cluster-wide per name),
  consistent-hash distribution, cold-swap migration, `Divergence` partition
  tolerance, in-memory `:ets` registry. Every controller asserts the entries
  its own store holds (at hub start and on a 60 s sweep); losing assertions
  no-op. There is no leader election and no cross-node state sync — the hub
  arbitrates, it does not remember.

If the node running a schedule leaves the cluster, ProcessHub re-places the
job on a survivor and the next slot fires there.

**Define every schedule on every controller.** A definition that exists only on
node A disappears with A. Adding it everywhere costs nothing — the hub still
runs exactly one.

**Migration and restart can re-fire one slot.** A restarted or migrated job
does the grace look-back; if that slot's instance already completed, the id
guard no longer holds and the slot fires again. Set
`FLEZHA_SCHEDULE_MISFIRE_GRACE_MS=0` if a duplicate is worse than a miss.

A network partition is not protected: under `Divergence` both sides keep
running their schedules.

## Configuration

| Env var                            | Default              | Meaning                                             |
| ---------------------------------- | -------------------- | --------------------------------------------------- |
| `FLEZHA_SCHEDULE_DIR`              | `app_data/schedules` | Registry directory (`schedules.dets`)               |
| `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS` | `120000`             | Oldest a slot may be and still fire on process start |

## Observability

| Telemetry event                         | Metadata                           | When                                                   |
| --------------------------------------- | ---------------------------------- | ------------------------------------------------------ |
| `[:flezha, :schedule, :fired]`          | `schedule`, `instance_id`          | An instance started                                    |
| `[:flezha, :schedule, :spawn_failed]`   | `schedule`, `instance_id`, `attempt` | A slot attempt produced no instance (also logged)    |
| `[:flezha, :schedule, :dormant]`        | `schedule`                         | Unparsable cron or no future slot (also logged)        |
| `[:flezha, :schedule, :hub_start_failed]` | `reason`                         | The `:flz_schedules` hub failed to start; retried every 5 s |

A persistently failing schedule is also visible in `flz schedule list`'s
`LAST RESULT` column.

## See also

- [`remote_durability_tier.md`](remote_durability_tier.md) — how `schedules.dets` ships and hydrates.
- [`../plugins/lifecycle.md`](../plugins/lifecycle.md) — the `--plugins` spec format.
- [`../resilience/workflow_operative.md`](../resilience/workflow_operative.md) — what a spawned workflow instance guarantees.
- [`../internals/process_hub.md`](../internals/process_hub.md) — hub strategies and re-placement.
