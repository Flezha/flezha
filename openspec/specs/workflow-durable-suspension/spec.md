# workflow-durable-suspension Specification

## Purpose

The workflow-durable-suspension capability lets a waiting durable workflow instance cost nothing while losing nothing. Post-start input for a durable workflow instance goes to the one durable input log a stream on the effectively-once tier uses, stamped `flz.offset` and deduplicated by a retained `flz.msg_id`, **before** it is forwarded, so a message sent while the instance is suspended, crashed or migrating is delivered on rehydrate rather than dropped. Each iteration commit reports its consumed offset into the log's watermark, the instance's floor. A looping durable instance that finds nothing to consume at a committed iteration boundary parks: the controller journals `parked(N[, wake_deadline])` and evicts it from its runner, registry state `suspended`. A new input-log append or the journaled wake deadline rehydrates it through the normal bind path, which re-forwards the log's records above the floor. Every input to that decision is durable, so suspension survives a controller restart, and racing wake sources resolve to exactly one rehydration.
## Requirements
### Requirement: Post-start input is durably journaled before delivery

The controller SHALL append every post-start input for an instance whose input is journaled
(a durable workflow, a stream on the effectively-once tier, or a class of either kind that
declares `sleep_when_idle`) to the instance's durable input log (`durable-input-log`, the same
module, file and offset space for every such instance) with a monotonic offset **before**
forwarding it to the runner, stamped `flz.offset`. An input carrying a `flz.msg_id` already
retained in the log SHALL be absorbed at append; an input carrying no id SHALL always be
appended. The log's floor SHALL be the strictest durable consumption the class records: the
tier's per-Cell consumed offsets when the class declares the tier, the journal's iteration
commit when the instance journals, and otherwise the offset the controller has forwarded to a
bound runner, reported by the Overwatch once the forward is done. Entries at or below the floor
SHALL be released to the log's retention. Message payloads SHALL live only in the input log,
never in `task.journal`. A class that declares none of the three SHALL have no durable input
and SHALL pay no append.

#### Scenario: Input survives the instance being down

- **WHEN** a message arrives for a suspended (or crashed, not-yet-respawned) instance whose input is journaled
- **THEN** it SHALL be durably appended to the instance's input log and delivered when the
  instance next runs, not dropped

#### Scenario: A sleeping stream's message is held and wakes it

- **WHEN** a message arrives for a stream class that declares `sleep_when_idle` while the instance is suspended
- **THEN** it SHALL be appended to the instance's input log, the instance SHALL be rehydrated, and the message SHALL be delivered by the bind's re-forward, once

#### Scenario: A plain sleeping stream's log holds only what nobody could take

- **WHEN** a stream that declares `sleep_when_idle` and neither the tier nor a journal is awake and bound, and the controller forwards the record at offset 9 to its runner
- **THEN** the Overwatch SHALL report 9 consumed, retention MAY release records at or below 9, and a later rehydrate SHALL re-forward only records above 9

#### Scenario: Duplicate send is absorbed

- **WHEN** the same message, carrying the same `flz.msg_id`, is sent twice before the instance
  consumes it
- **THEN** exactly one log record SHALL exist and the operative SHALL receive it once

#### Scenario: A message without an id is never absorbed

- **WHEN** the same payload is sent twice with no `flz.msg_id`
- **THEN** two records SHALL exist and the operative SHALL receive both

#### Scenario: Consumed entries are released

- **WHEN** iteration N commits with consumed offset 7
- **THEN** records at or below 7 SHALL be eligible for the log's retention and later rehydrates
  SHALL NOT re-deliver them

#### Scenario: A class that does not sleep pays nothing

- **WHEN** a stream class declares neither `sleep_when_idle` nor the effectively-once tier
- **THEN** no input log SHALL be written for its instances and every input SHALL be forwarded as today

### Requirement: A suspended instance rehydrates on new input or its durable timer

The controller SHALL rehydrate a `suspended` instance of either kind on a new input-log append
or on the elapse of its recorded wake deadline: respawn through the normal bind path on any
live runner (existing session fencing applies), restore its Cells from their snapshots and WAL
tails and, for a workflow, its journal state, deliver the log's retained tail above the
instance's floor through the one re-forward every bind runs, and continue: a workflow enters
iteration N+1, a stream resumes folding input. Rehydration SHALL survive controller
restart/redeploy: at boot an instance whose park record exists and whose log owes nothing
SHALL enter `suspended` without binding, and one whose log owes input SHALL bind. Concurrent
wake sources SHALL resolve to exactly one rehydration. Whether a suspended instance still owes
input SHALL be read from the log's tail and the instance's floor, not from a decode of the file.

#### Scenario: New message wakes a suspended instance

- **WHEN** a message arrives for an instance suspended at iteration N
- **THEN** the instance SHALL be respawned (possibly on a different runner), restored from
  journal + iter-N snapshots, and SHALL process the message as iteration N+1

#### Scenario: A sleeping stream wakes with its state

- **WHEN** a message arrives for a stream instance that slept with a keyed Cell holding a running total
- **THEN** the instance SHALL be respawned on a live runner, that Cell SHALL be restored to the total it held before the sleep, and the message SHALL be folded onto it

#### Scenario: Suspension survives controller redeploy

- **WHEN** the controller restarts while an instance of either kind is suspended and a message then arrives
- **THEN** the instance SHALL rehydrate exactly as if no restart had happened

#### Scenario: Durable timer fires without input

- **WHEN** an instance parked with a wake deadline reaches that deadline with no input
- **THEN** it SHALL be rehydrated and the loop SHALL observe a timer wake (not an input item)

#### Scenario: A park landing during an un-park keeps the suspension

- **WHEN** the rehydrated instance parks again before the un-park task has reported its bound state
- **THEN** the Overwatch SHALL stay suspended (the runner has already torn the instance down),
  the park record written by that park SHALL stand, and the Overwatch SHALL rehydrate again only
  if the log still holds records above the instance's floor; the task's stale bound state SHALL
  NOT be adopted

#### Scenario: Racing wake sources rehydrate once

- **WHEN** an input-log append and a timer deadline fire for the same suspended instance
  concurrently
- **THEN** exactly one rehydration SHALL occur

### Requirement: An idle instance sleeps durably and is evicted from memory

Sleeping when idle SHALL be a class setting of both kinds, `sleep_when_idle`, declared in every
SDK, announced by the runner in the `AddOperative` response and recorded by the controller
beside the class's other declarations: on by default for a workflow whose persistence is on,
off by default for a stream, and refused at registration as `SleepWithoutPersistence` on a
class of either kind whose persistence is off. The idle threshold SHALL be
`FLEZHA_PARK_IDLE_MS`, default 600 000 ms, one knob for both kinds; no knob SHALL carry a kind
in its name.

The SDK SHALL, when an instance of a class that sleeps has been idle past the threshold, send
`IterPark` (the iteration a workflow sleeps at, `0` for a stream, and an optional wake
deadline) and remove the instance from runner memory; the controller SHALL write the park
record durably before the eviction, then tear the instance down without a terminal completion:
registry state `suspended`, zero live runner resources. An instance is idle when no cell of it
holds a queued, in-flight, held-back, retrying or unflushed item, no cell timer is pending, no
input tail is owed and, for a workflow, its pass sheet is `Waiting` with an empty queue at a
committed iteration boundary, the boundary before its first pass included. The threshold SHALL
be timed as one deadline on the core's deferred-wake list, armed once and re-armed from the
cells' latest activity when it comes due early, promoted by a thread already waiting in `poll`,
and SHALL hold no thread and touch no input path; an instance found busy when it comes due
SHALL NOT sleep and the deadline SHALL re-arm. An iteration boundary SHALL be the only place a
workflow waits for a message. An eviction-hysteresis knob SHALL avoid park/rehydrate thrash
under trickling input.

The park record SHALL be a per-instance fact in the class registry's durable sidecar for either
kind, holding the wake deadline when one was given; it SHALL NOT be a task-journal entry. The
bind that wakes the instance SHALL clear it before the runner is told to start the instance.

The suspended Overwatch SHALL hibernate: a sleeper holds nothing of its bind on
its heap (the restore frames and plugin setup are garbage once it parks).

#### Scenario: A sleeper costs the controller a minimal heap

- **WHEN** an instance parks
- **THEN** its Overwatch SHALL be hibernated, and a resting population SHALL cost the controller on the order of tens of KiB per sleeper, not the bind's working set

#### Scenario: Idle chat operative frees the runner

- **WHEN** a looping instance commits iteration N and nothing arrives past the idle threshold
- **THEN** the park record SHALL be written, the instance SHALL be removed from runner memory, and the registry SHALL report it `suspended`

#### Scenario: An idle stream that declares sleeping frees the runner

- **WHEN** a stream class declares `sleep_when_idle` and an instance of it receives nothing for the idle threshold with every cell drained and flushed and no timer pending
- **THEN** the runner SHALL send `IterPark` with iteration `0`, the park record SHALL be written, the instance SHALL be removed from runner memory, and the registry SHALL report it `suspended`

#### Scenario: A stream that does not declare sleeping never sleeps

- **WHEN** a stream class leaves `sleep_when_idle` at its default and an instance of it receives nothing for hours
- **THEN** no `IterPark` SHALL be sent and the instance SHALL stay bound

#### Scenario: A busy instance does not sleep at the deadline

- **WHEN** the idle deadline comes due while a cell of the instance still holds an item in a retry backoff, an unflushed buffer or a pending timer
- **THEN** the instance SHALL NOT sleep and the deadline SHALL be re-armed

#### Scenario: Sleeping without persistence is refused

- **WHEN** a class of either kind declares `sleep_when_idle` with its persistence switch off, in any SDK
- **THEN** registration SHALL fail with an error containing `SleepWithoutPersistence` and nothing SHALL be declared

#### Scenario: A once instance waiting for its first message parks

- **WHEN** a durable `once` instance is started without a message and none arrives within the idle threshold
- **THEN** the park record SHALL be written for iteration `0`, the instance SHALL be removed from runner memory, the registry SHALL report it `suspended`, and the next message SHALL rehydrate it into pass 0

#### Scenario: A message arriving mid-iteration waits for the boundary

- **WHEN** a message reaches a `PerInput` instance while an iteration is still running
- **THEN** it SHALL be journaled to the input log, the runner SHALL hold it in the instance's queue, it SHALL seed the next iteration after the current one commits, and no tasklet SHALL receive it mid-iteration

#### Scenario: A waiting population holds no thread

- **WHEN** ten thousand instances wait inside the idle threshold
- **THEN** no thread SHALL exist on their behalf and the runner SHALL consume no measurable CPU
  for the wait

#### Scenario: The park record is not in the journal

- **WHEN** a durable loop has parked and `flz state journal` is run for it
- **THEN** the journal SHALL show its committed entries and no `parked` entry, and the instance SHALL still be reported `suspended`

#### Scenario: A message parked unread does not wake the instance again

- **WHEN** a durable instance waiting for a message receives one it cannot read, parks it to the dead-letter queue, and then waits past the idle threshold
- **THEN** its park SHALL carry that message's offset, the message SHALL no longer be owed, and the instance SHALL stay `suspended` rather than be rehydrated to receive it again; a rehydrated instance that was owed the message SHALL count it as received and park on the idle threshold

### Requirement: A rehydrated instance waits for the input tail it is owed before it sleeps

The controller SHALL tell a rehydrating bind of either kind how many journaled, undelivered
input records it will forward after the bind (`AddOperative.inbox_tail_count`, the log's tail
above the instance's floor), and the SDK SHALL NOT start the instance's idle clock until that
many records have arrived (bounded by an owed-tail wait of 60 s, after which it sleeps and the
controller's rehydrate-if-pending forwards the tail again). A tail forwarded after the bind
under load would otherwise reach an instance that has already slept and be dropped at the socket.

#### Scenario: The tail arrives after the park idle would have expired
- **WHEN** a suspended instance with one undelivered record is rehydrated and the controller's forward of that record reaches the runner later than the idle window after the bind
- **THEN** the instance SHALL still be awake, consume the record, and SHALL NOT have slept in between

#### Scenario: Nothing owed
- **WHEN** an instance binds with `inbox_tail_count = 0`
- **THEN** its idle clock runs from the bind as before

