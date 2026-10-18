# scheduled-relay-spawns Specification

## Purpose
TBD - created by archiving change add-scheduled-relay-spawns. Update Purpose after archive.
## Requirements
### Requirement: The controller SHALL maintain a durable registry of cron schedules for steps spawns

`flz schedule add <name> --cron "<expr>" --tz <iana> --factory <factory_id> [--namespace ns] [--init-values json] [--text payload] [--channel ch] [--plugins json]` SHALL create a durable schedule entry; `flz schedule list` SHALL show entries (with next fire time, the node running each schedule, the last fired instance and its result) and `flz schedule remove <name>` SHALL delete them. The listed next fire SHALL be the slot the schedule's running job is armed for; a job that has gone dormant SHALL be listed with no next fire and as dormant, and only an entry no job answers for SHALL have its next fire computed from its cron. Entries SHALL persist across controller restarts and warm restarts. Only a steps factory SHALL be schedulable, and the kind is the factory's own, announced by its runner: `add` SHALL take no `--kind`, SHALL reject a factory the namespace has established as a flow relay, and SHALL accept a factory no runner has yet announced; every fire SHALL resolve the factory's established kind through the standard start path, where a flow factory fails the slot as any refused start does. Cron syntax, timezone, and the plugin spec SHALL be validated at `add`. `--init-values` SHALL be the initial state per tasklet the fired instance starts with, as it is for `flz relay start`; `--text` SHALL be the first message every fired instance receives, on the entry channel `--channel` names or on the first declared one, stored verbatim; an entry stored without `--text` SHALL fire an instance that waits for its first message. `--channel` without `--text` SHALL be refused at `add`. The plugin spec SHALL use the same JSON array `flz relay start --plugins` accepts and SHALL be stored verbatim; an entry stored without one SHALL fire with no plugins.

#### Scenario: Schedule survives restart

- **WHEN** a schedule is added and every controller restarts
- **THEN** `flz schedule list` SHALL still show it and subsequent slots SHALL fire

#### Scenario: A dormant schedule lists no next fire

- **WHEN** a schedule's job has gone dormant, for example because its cron has no future slot
- **THEN** `flz schedule list` SHALL show no next fire time for it and its JSON form SHALL carry `next_fire: null` and `dormant?: true`

#### Scenario: Invalid entry rejected

- **WHEN** `schedule add` is issued with a malformed cron expression or unknown timezone
- **THEN** the command SHALL fail with a message naming the invalid field and no entry SHALL be stored

#### Scenario: A flow factory is rejected at add

- **WHEN** `schedule add` names a factory a connected runner has announced as a flow relay
- **THEN** the command SHALL fail stating that only a steps factory is schedulable, naming the factory, and no entry SHALL be stored

#### Scenario: A factory that changed kind fails its slot, not the schedule

- **GIVEN** a schedule added for a factory announced as a steps relay
- **WHEN** at a later fire the only runner announcing that id has established it as a flow relay
- **THEN** the slot SHALL fail with a result naming the kind, SHALL be retried and listed as any failed slot is, and the entry SHALL remain

#### Scenario: A first message is stored with the entry

- **WHEN** `flz schedule add nightly --cron "0 2 * * *" --tz UTC --factory report --text '{"day":"today"}' --channel requests` is issued
- **THEN** the stored entry SHALL hold the text and the channel verbatim, and `flz schedule list` SHALL show the entry

#### Scenario: Unusable plugin spec rejected at add time

- **WHEN** `schedule add --plugins` is issued with JSON that is not an array, names an unknown provider, or carries a malformed hook
- **THEN** the command SHALL fail naming the fault and no entry SHALL be stored

#### Scenario: A failing schedule is visible in the list

- **WHEN** a schedule's most recent slot failed to spawn and is being retried
- **THEN** `flz schedule list` SHALL show that slot's instance id as the last fire with a non-ok last result, without the user reading logs

#### Scenario: A schedule whose job does not answer still lists

- **WHEN** a schedule's job process does not answer the status query within its bounded timeout
- **THEN** `flz schedule list` SHALL still render the entry (placement known, last fire unknown) rather than failing the command

### Requirement: Each slot SHALL fire exactly once cluster-wide via the standard steps spawn path

At each matching slot (evaluated in the entry's IANA timezone by a single schedule process that exists on exactly one node cluster-wide, hosted in the dedicated `:flz_schedules` hub), the controller SHALL spawn a fresh steps instance with deterministic id `<name>@<slot>`, the stored `init_values` as its initial state and the stored first message, when one is stored, delivered as the instance's first input on the mechanism a tasklet's spawn uses, through the same start path as `flz relay start` (quota, plugins, teardown, retention inherited). The fire SHALL succeed whenever any controller node in the cluster holds a runner advertising the entry's factory in the entry's namespace: the node hosting the schedule process SHALL NOT affect the outcome, and the spawn SHALL be routed to a capable node at fire time against the fleet state current at that moment. A fire SHALL be refused as unsatisfiable only when no node in the cluster holds such a runner, and the refusal SHALL say so cluster-wide (not "no runners" on the deciding node alone). The entry's stored plugin spec SHALL be decoded at each firing and attached at spawn, with the fire's namespace and instance id stamped on every plugin and each plugin's id prefixed by the instance id so concurrent slots never share one. A firing whose plugin spec cannot be decoded SHALL NOT spawn an instance and SHALL be logged and reported on telemetry as a spawn failure. A duplicate fire attempt for the same slot SHALL no-op on the deterministic id. DST: nonexistent local times skip; ambiguous local times fire once.

#### Scenario: The runner lives on a different node than the schedule process

- **WHEN** a slot fires on a controller node holding no runner for the entry's namespace while another cluster node holds a connected runner advertising the factory
- **THEN** the instance SHALL be started on the capable node and the fire SHALL be reported as fired, not failed

#### Scenario: A rolling update moves the runner between nodes

- **WHEN** the namespace's only runner disconnects from node A and reconnects on node B between two slots, with the schedule process staying where it is
- **THEN** the next slot SHALL fire successfully on node B with no user action

#### Scenario: A fired instance runs on the stored first message

- **WHEN** a schedule storing `--text '{"day":"today"}'` fires a `once` steps relay
- **THEN** the spawned instance SHALL run its pass over that message on the stored channel or the first declared entry channel, and SHALL end with status `ok`

#### Scenario: A fired instance produces output through its scheduled plugins

- **WHEN** a schedule carrying a producer plugin spec fires
- **THEN** the spawned instance SHALL have that plugin attached from the start of its run, with `relay_id` equal to the slot's instance id

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
- **THEN** one instance SHALL spawn immediately with the stored configuration, its stored first message included, and an id distinct from cron slots, whether or not the schedule hub is running on that node — routed to a capable node exactly like a cron fire

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

A slot whose spawn fails SHALL be retried by the schedule process with exponential backoff (capped at one minute, the finest cron granularity) using the SAME deterministic instance id `<name>@<slot>`, so a retry racing any other fire attempt no-ops on the registry's duplicate-id refusal. A `duplicate` answer SHALL count as success. When the next slot fires, any pending retry for an earlier slot SHALL be cancelled — at most one slot is ever in flight, extending the misfire-grace rule ("only the most recent slot") from process start to steady state. The schedule SHALL NOT park, go dormant, or require user intervention because of spawn failures; a schedule for a factory whose runner has not connected yet SHALL simply keep probing. A schedule process restarting or migrating mid-retry SHALL recover through the existing misfire-grace look-back, which re-fires the most recent slot inside the grace window. Each failed attempt SHALL be reported on the existing spawn-failure telemetry event carrying the attempt number.

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
- **THEN** the schedule SHALL remain registered and probing on backoff, `flz schedule list` SHALL show the failing last result, and no user action SHALL be needed for it to fire once the runner appears

### Requirement: A manual fire SHALL be an authorized relay start

`flz schedule run <name>` SHALL authorize `relay.start` in the namespace the
schedule was added under before it spawns an instance, and SHALL accept the
caller's credential the way every other gated schedule verb does. It SHALL stay
hub-free: the authorization is judged from the stored row, so a manual fire still
works on a node whose schedule hub is down.

#### Scenario: A manual fire carries a credential

- **WHEN** `flz schedule run <name> --token <t>` is issued
- **THEN** the token SHALL reach the controller with the request, and the spawn SHALL
  be judged as `relay.start` for that token in the schedule's namespace

#### Scenario: A manual fire with no credential is refused under enforcement

- **WHEN** authorization is enforced and `schedule run` is issued with no credential
- **THEN** no instance SHALL spawn and the refusal SHALL name the action

### Requirement: The schedule verbs SHALL carry their optional flags as one options document

A schedule verb's optional flags SHALL travel to the controller as one options
document (the namespace, the initial values, the first message's text and channel, the
plugin spec and the caller's credential), the keyword list every other gated schedule verb already
carries its credential in, rather than as positional arguments with defaults.
There SHALL be no `kind` entry in that document, because a schedule names a factory and
the factory's runner names its kind. This rule governs the request the CLI builds, not the
spelling of the user-facing flags. A value that must reach the controller as text, such as
the plugin spec the decoder reads verbatim and the first message, SHALL be carried as text whatever it
parses as.

#### Scenario: An optional flag is absent

- **WHEN** `flz schedule add` is issued without `--namespace`, `--init-values`,
  `--text`, `--channel` or `--plugins`
- **THEN** the controller SHALL apply the same defaults it applies today, and the
  request SHALL carry no positional placeholder for the absent flag

#### Scenario: The plugin spec arrives as the text the user wrote

- **WHEN** `flz schedule add --plugins '[{"provider": …}]'` is issued
- **THEN** the controller SHALL receive the JSON text, and the stored entry SHALL
  hold it verbatim as the registry requirement demands

#### Scenario: The first message arrives as the text the user wrote

- **WHEN** `flz schedule add --text 'hello' --channel requests` is issued
- **THEN** the request SHALL carry `text` and `channel` in the options document as text, and the stored entry SHALL hold both verbatim

#### Scenario: No schedule verb exceeds the arity ceiling

- **WHEN** the lint gate reads `lib/flezha/cli/schedule.ex`
- **THEN** no function SHALL take more parameters than the ceiling the gate enforces

