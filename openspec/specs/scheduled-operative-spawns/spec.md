# scheduled-operative-spawns Specification

## Purpose
TBD - created by archiving change add-scheduled-operative-spawns. Update Purpose after archive.
## Requirements
### Requirement: The controller SHALL maintain a durable registry of cron schedules for bounded spawns

`flz schedule add <name> --cron "<expr>" --tz <iana> --factory <factory_id> [--namespace ns] [--init_values json] [--plugins json]` SHALL create a durable schedule entry; `flz schedule list` SHALL show entries (with next fire time) and `flz schedule remove <name>` SHALL delete them. Entries SHALL persist across controller restarts and warm restarts. Only `kind: bounded` SHALL be schedulable; scheduling an unbounded factory SHALL be rejected at `add`. Cron syntax, timezone, kind, and the plugin spec SHALL be validated at `add`. The plugin spec SHALL use the same JSON array `flz operative start --plugins` accepts and SHALL be stored verbatim; an entry stored without one SHALL fire with no plugins.

#### Scenario: Schedule survives restart

- **WHEN** a schedule is added and every controller restarts
- **THEN** `flz schedule list` SHALL still show it and subsequent slots SHALL fire

#### Scenario: Invalid entry rejected

- **WHEN** `schedule add` is issued with a malformed cron expression or unknown timezone
- **THEN** the command SHALL fail with a message naming the invalid field and no entry SHALL be stored

#### Scenario: Unusable plugin spec rejected at add time

- **WHEN** `schedule add --plugins` is issued with JSON that is not an array, names an unknown provider, or carries a malformed subscription
- **THEN** the command SHALL fail naming the fault and no entry SHALL be stored

### Requirement: Each slot SHALL fire exactly once cluster-wide via the standard bounded spawn path

At each matching slot (evaluated in the entry's IANA timezone by a single schedule process that exists on exactly one node cluster-wide, hosted in the dedicated `:flz_schedules` hub), the controller SHALL spawn a fresh bounded instance with deterministic id `<name>@<slot>` and the stored `init_values`, through the same start path as `flz operative start` (quota, plugins, teardown, retention inherited). The entry's stored plugin spec SHALL be decoded at each firing and attached at spawn, with the fire's namespace and instance id stamped on every plugin and each plugin's id prefixed by the instance id so concurrent slots never share one. A firing whose plugin spec cannot be decoded SHALL NOT spawn an instance and SHALL be logged and reported on telemetry as a spawn failure. A duplicate fire attempt for the same slot SHALL no-op on the deterministic id. DST: nonexistent local times skip; ambiguous local times fire once.

#### Scenario: A fired instance produces output through its scheduled plugins

- **WHEN** a schedule carrying a producer plugin spec fires
- **THEN** the spawned instance SHALL have that plugin attached from the start of its run, with `operative_id` equal to the slot's instance id

#### Scenario: A spec that no longer decodes does not fire a plugin-less run

- **WHEN** a slot fires while the spec's provider is no longer enabled
- **THEN** no instance SHALL be started and the failure SHALL be logged and emitted on the spawn-failure telemetry event

#### Scenario: One instance per slot despite duplicate fire attempts

- **WHEN** two fire attempts for the same slot race while the first instance is alive
- **THEN** exactly one instance `<name>@<slot>` SHALL exist and the losing attempt SHALL no-op

#### Scenario: Exactly one node runs a given schedule

- **WHEN** several controllers each hold the same schedule definition in their node-local store and all assert it
- **THEN** exactly one schedule process SHALL exist cluster-wide and the losing assertions SHALL no-op

#### Scenario: The owning node leaves the cluster

- **WHEN** the node running a schedule leaves the cluster
- **THEN** a surviving node SHALL take the schedule over, subsequent slots SHALL fire there, and no duplicate schedule process SHALL exist

#### Scenario: Manual immediate run

- **WHEN** `flz schedule run <name>` is issued
- **THEN** one instance SHALL spawn immediately with the stored configuration and an id distinct from cron slots, whether or not the schedule hub is running on that node

### Requirement: Missed slots SHALL be skipped, and overlapping runs SHALL be permitted by distinct slot ids

Slots that pass while no schedule process is running SHALL NOT be back-filled, except that on process start the single most recent slot inside `misfire_grace_ms` SHALL fire — covering controller restart, migration and node-takeover gaps — floored at the entry's creation time so a newly added schedule never fires history. Older slots SHALL be dropped and the skip SHALL be logged. A grace of `0` SHALL disable the look-back entirely. If a previous slot's instance is still running at the next slot, the new slot SHALL still spawn (ids differ); bounding runtime is the author's responsibility (`max_duration`).

#### Scenario: Down cluster skips cleanly

- **WHEN** the cluster is down across two slots older than `misfire_grace_ms` and then recovers
- **THEN** no catch-up spawns SHALL occur for those slots and the skip SHALL be logged

#### Scenario: A restart inside the grace window resumes the latest slot

- **WHEN** a schedule process restarts and exactly one slot passed inside `misfire_grace_ms`
- **THEN** that slot SHALL fire once and no earlier slot SHALL be back-filled

#### Scenario: Long-running previous slot does not block

- **WHEN** `<name>@<t1>` is still running at slot `t2`
- **THEN** `<name>@<t2>` SHALL spawn normally

