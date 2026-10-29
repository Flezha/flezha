# finished-relay-retention Specification

## Purpose
What a relay that has ended keeps: nothing unless its class declares `keep_finished` (or the controller keeps by default), where its outcome is read from, and how a kept relay's state is reclaimed.
## Requirements
### Requirement: A relay that ends SHALL keep its state only when its class asks

When a relay ends by itself, its state directory SHALL be deleted at teardown unless its class
keeps finished state. Ending by itself means a steps run ending `ok`, `error` or `cancelled`,
or a flow relay completing or reaching its `max_duration`. A kept relay's directory SHALL be kept
for its resolved window from its finish time and then deleted by the retention sweeper. A
stopped flow relay, a namespace deletion and a failed start SHALL delete the state as they do
today. A rescale's move SHALL keep the files it hands on as it does today.

#### Scenario: A spawned run that declares nothing

- **WHEN** a `fan_trade` instance whose class declares no `keep_finished` ends `ok`
- **THEN** its state directory SHALL no longer exist once its teardown has returned

#### Scenario: A class that keeps its finished runs

- **WHEN** an instance of a class declaring `durable: {local: {keep_finished: 24}}` ends `error`
- **THEN** its state directory and completion record SHALL remain, and the sweeper SHALL
  delete them once 24 hours have passed since its finish

#### Scenario: A flow relay that completes

- **WHEN** a flow relay whose class declares nothing calls `ctx.complete`
- **THEN** its state directory SHALL be deleted at teardown

### Requirement: `keep_finished` SHALL be a member of the class's durability

A class SHALL declare finished-state retention as the member `keep_finished` of its
`durable` level `local` or `remote`, beside `sleep_after`. It is either a positive whole number of
hours or a boolean: `true` keeps for the controller's window
(`:default_relay_completed_retention_hours`), and `false` keeps nothing. Any other value, and
the member under `durable: false`, SHALL be refused when the class is registered, naming the
member. `flz_core` SHALL read it from the `durable` tree it already parses and SHALL announce
it in the class metadata as `keep_finished_h`. The value is the hours, or `default` for
`true`, and the key is absent when nothing is kept. The metadata is shared by the `AddRelay`
answer and, where the factory announcement carries declarations, the announcement. The
controller SHALL decode it in `ClassRegistry.decode_declarations/2` and persist it with the
class's other declarations.

The resolved window SHALL be:
- the class's hours;
- for `true`, the controller's window;
- for a class that declares nothing, the controller's window when `:keep_finished_by_default`
  (env `FLEZHA_KEEP_FINISHED_BY_DEFAULT`, default `false`) is set, and nothing otherwise;
- for a memory-only class, nothing, whatever the default.

A directory that already holds a completion record SHALL expire after its class's declared
window, or the controller's window when its class declares none, because a record exists only
for state that was kept.

The sweeper, `Relay.info` and the state-directory teardown SHALL read the one resolution.

#### Scenario: A directory kept before the upgrade

- **GIVEN** a finished run's directory and completion record written by a controller without
  this change, 10 hours ago, and `FLEZHA_STEPS_RETENTION_HOURS=72`
- **WHEN** the upgraded controller's sweeper runs
- **THEN** it SHALL keep the directory and delete it once 72 hours have passed since its finish

#### Scenario: Every spelling the SDKs pass through

- **WHEN** a Python class declares `durable={"remote": {"keep_finished": 6}}` and an Elixir
  class declares `durable: {:local, keep_finished: true}`
- **THEN** the `AddRelay` metadata SHALL carry `keep_finished_h: 6` and
  `keep_finished_h: default` respectively

#### Scenario: A memory class cannot keep

- **WHEN** a class declares `keep_finished` under `durable: false`
- **THEN** its registration SHALL be refused naming `keep_finished`

#### Scenario: The controller-wide default

- **GIVEN** `FLEZHA_KEEP_FINISHED_BY_DEFAULT=true` and `FLEZHA_STEPS_RETENTION_HOURS=72`
- **WHEN** an instance of a class that declares nothing ends
- **THEN** its state SHALL be kept for 72 hours

### Requirement: The completion record SHALL exist only for a kept relay

The terminal status, error, result and finish time of a run that ends by itself SHALL reach its
Overwatch in memory, with the call the teardown already makes to set the stop's cause, before
any teardown step runs. Callers waiting on the instance SHALL be answered with that status, and
the relay history's `ended` record SHALL read that same value. The completion record SHALL be
written only when the class keeps finished state. It SHALL be written by the state-directory
step on the node that holds the relay's state, before that step decides to keep the directory,
and SHALL remain the marker that the sweeper, the teardown and the convergence intent check
read. A memory-only class SHALL never get a state directory for a completion record. The
control channel SHALL write no record.

#### Scenario: A caller waiting on a run that keeps nothing

- **WHEN** relay `ma_fan` is waiting on a call answered by a `fan_trade` instance that ends
  `cancelled` and keeps nothing
- **THEN** `ma_fan`'s call SHALL end with status `cancelled`, not `stopped`

#### Scenario: A memory-only run leaves nothing on disk

- **WHEN** a `durable: false` steps instance ends `ok`
- **THEN** no directory SHALL exist for it under the state root

#### Scenario: The record is written where the state is

- **WHEN** a kept instance's completion arrives through a runner session on controller `a`
  while its Overwatch and state are on controller `b`
- **THEN** its completion record SHALL be written on `b`

### Requirement: A finished relay's outcome SHALL be read from its record or the relay history

A finished relay's outcome SHALL be answered by `Relay.info` for one relay id,
`Steps.completion`, the outcome `Steps.journal` reports and `Relay.diagnose` as follows:
- from its kept completion record when there is one;
- otherwise from the relay history's newest `ended` record for that id in the namespace,
  read cluster-wide, answering its status, error, finish time and that the state was not
  kept.

A relay with neither SHALL be answered as unknown, as today. `Relay.info`'s listing SHALL list
as finished only the relays whose state is kept, each with its expiry. The journal of a finished
relay whose state was not kept SHALL report its outcome, not "not ended".

#### Scenario: Info for a run that kept nothing

- **WHEN** `flz relay info fan_trade-01` is run after that instance ended `error` and kept
  nothing
- **THEN** the answer SHALL report it finished with status `error`, its error and finish time,
  and that its state was not kept

#### Scenario: The listing shows kept relays only

- **WHEN** one kept and one not-kept instance have finished in a namespace
- **THEN** `Relay.info`'s listing SHALL list the kept one as finished and not the other

### Requirement: Nothing a finished relay held SHALL outlive it

When a relay's state directory is deleted, at teardown or by the sweeper, the controller SHALL
also drop that relay's per-Cell counters, its Cells' input-log watermarks, and its fsync-batch
and remote-gate entries. It does this through the single path that deletes one Cell today.

A delete that skips a tasklet directory because a write is in flight SHALL mark the directory
for deletion and finish it on a later sweep. A marked directory SHALL count neither as a kept
relay nor as lost intent.

A read of a finished relay's Cells (time travel, outbox) SHALL NOT re-hydrate them from remote
storage unless the relay is live or kept.

#### Scenario: Counters go with the run

- **WHEN** ten thousand single-Cell instances have ended and kept nothing
- **THEN** the per-Cell counter table SHALL hold no row for any of them

#### Scenario: A delete raced by a write is finished later

- **WHEN** a finished relay's delete skips a tasklet directory because a write was in flight
- **THEN** the next sweep SHALL delete the directory, and the convergence loop SHALL report no
  lost intent for it

#### Scenario: Reading a deleted run does not bring it back

- **WHEN** `flz state at` is run for a finished relay that kept nothing, on a controller with a
  remote tier
- **THEN** it SHALL answer that no persisted state exists and SHALL write nothing under the
  state root

