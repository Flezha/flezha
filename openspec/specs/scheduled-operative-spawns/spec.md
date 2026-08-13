# scheduled-operative-spawns Specification

## Purpose
TBD - created by archiving change add-scheduled-operative-spawns. Update Purpose after archive.
## Requirements
### Requirement: The controller SHALL maintain a durable registry of cron schedules for workflow spawns

`flz schedule add <name> --cron "<expr>" --tz <iana> --factory <factory_id> [--namespace ns] [--init_values json] [--plugins json]` SHALL create a durable schedule entry; `flz schedule list` SHALL show entries (with next fire time, the node running each schedule, the last fired instance and its result) and `flz schedule remove <name>` SHALL delete them. Entries SHALL persist across controller restarts and warm restarts. Only `kind: workflow` SHALL be schedulable; scheduling a stream factory SHALL be rejected at `add`. Cron syntax, timezone, kind, and the plugin spec SHALL be validated at `add`. The plugin spec SHALL use the same JSON array `flz operative start --plugins` accepts and SHALL be stored verbatim; an entry stored without one SHALL fire with no plugins.

#### Scenario: Schedule survives restart

- **WHEN** a schedule is added and every controller restarts
- **THEN** `flz schedule list` SHALL still show it and subsequent slots SHALL fire

#### Scenario: Invalid entry rejected

- **WHEN** `schedule add` is issued with a malformed cron expression or unknown timezone
- **THEN** the command SHALL fail with a message naming the invalid field and no entry SHALL be stored

#### Scenario: Unusable plugin spec rejected at add time

- **WHEN** `schedule add --plugins` is issued with JSON that is not an array, names an unknown provider, or carries a malformed subscription
- **THEN** the command SHALL fail naming the fault and no entry SHALL be stored

#### Scenario: A failing schedule is visible in the list

- **WHEN** a schedule's most recent slot failed to spawn and is being retried
- **THEN** `flz schedule list` SHALL show that slot's instance id as the last fire with a non-ok last result, without the operator reading logs

#### Scenario: A schedule whose job does not answer still lists

- **WHEN** a schedule's job process does not answer the status query within its bounded timeout
- **THEN** `flz schedule list` SHALL still render the entry (placement known, last fire unknown) rather than failing the command

### Requirement: Each slot SHALL fire exactly once cluster-wide via the standard workflow spawn path

At each matching slot (evaluated in the entry's IANA timezone by a single schedule process that exists on exactly one node cluster-wide, hosted in the dedicated `:flz_schedules` hub), the controller SHALL spawn a fresh workflow instance with deterministic id `<name>@<slot>` and the stored `init_values`, through the same start path as `flz operative start` (quota, plugins, teardown, retention inherited). The fire SHALL succeed whenever any controller node in the cluster holds a runner advertising the entry's factory in the entry's namespace: the node hosting the schedule process SHALL NOT affect the outcome, and the spawn SHALL be routed to a capable node at fire time against the fleet state current at that moment. A fire SHALL be refused as unsatisfiable only when no node in the cluster holds such a runner, and the refusal SHALL say so cluster-wide (not "no runners" on the deciding node alone). The entry's stored plugin spec SHALL be decoded at each firing and attached at spawn, with the fire's namespace and instance id stamped on every plugin and each plugin's id prefixed by the instance id so concurrent slots never share one. A firing whose plugin spec cannot be decoded SHALL NOT spawn an instance and SHALL be logged and reported on telemetry as a spawn failure. A duplicate fire attempt for the same slot SHALL no-op on the deterministic id. DST: nonexistent local times skip; ambiguous local times fire once.

#### Scenario: The runner lives on a different node than the schedule process

- **WHEN** a slot fires on a controller node holding no runner for the entry's namespace while another cluster node holds a connected runner advertising the factory
- **THEN** the instance SHALL be started on the capable node and the fire SHALL be reported as fired, not failed

#### Scenario: A rolling update moves the runner between nodes

- **WHEN** the namespace's only runner disconnects from node A and reconnects on node B between two slots, with the schedule process staying where it is
- **THEN** the next slot SHALL fire successfully on node B with no operator action

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
- **THEN** one instance SHALL spawn immediately with the stored configuration and an id distinct from cron slots, whether or not the schedule hub is running on that node — routed to a capable node exactly like a cron fire

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

### Requirement: A failed slot SHALL be retried with backoff until its next slot supersedes it

A slot whose spawn fails SHALL be retried by the schedule process with exponential backoff (capped at one minute, the finest cron granularity) using the SAME deterministic instance id `<name>@<slot>`, so a retry racing any other fire attempt no-ops on the registry's duplicate-id refusal. A `duplicate` answer SHALL count as success. When the next slot fires, any pending retry for an earlier slot SHALL be cancelled — at most one slot is ever in flight, extending the misfire-grace rule ("only the most recent slot") from process start to steady state. The schedule SHALL NOT park, go dormant, or require operator intervention because of spawn failures; a schedule for a factory whose runner has not connected yet SHALL simply keep probing. A schedule process restarting or migrating mid-retry SHALL recover through the existing misfire-grace look-back, which re-fires the most recent slot inside the grace window. Each failed attempt SHALL be reported on the existing spawn-failure telemetry event carrying the attempt number.

#### Scenario: A runner appearing between slots is picked up by the retry

- **WHEN** a slot fails because no cluster node holds a capable runner, and such a runner connects before the next slot
- **THEN** a retry SHALL spawn that slot's instance under its original id, and the schedule's last result SHALL report the fire as successful

#### Scenario: The next slot supersedes a still-failing retry

- **WHEN** a slot is still being retried when the schedule's next slot fires
- **THEN** the pending retry SHALL be cancelled, only the new slot SHALL be attempted, and no back-fill for the abandoned slot SHALL ever occur

#### Scenario: A retry racing a duplicate fire no-ops

- **WHEN** a retry fires for a slot whose instance was concurrently started by another fire attempt
- **THEN** the retry SHALL treat the duplicate-id answer as success and stop retrying

#### Scenario: Spawn failures never park the schedule

- **WHEN** every slot of a schedule fails for days because its factory's runner is never deployed
- **THEN** the schedule SHALL remain registered and probing on backoff, `flz schedule list` SHALL show the failing last result, and no operator action SHALL be needed for it to fire once the runner appears
