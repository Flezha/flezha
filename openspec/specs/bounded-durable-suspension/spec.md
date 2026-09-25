# bounded-durable-suspension Specification

## Purpose

The bounded-durable-suspension capability lets a waiting durable bounded instance cost nothing while losing nothing. Post-start input for a durable bounded instance goes to the one durable input log an unbounded operator on the effectively-once tier uses, stamped `flz.offset` and deduplicated by a retained `flz.msg_id`, **before** it is forwarded, so a message sent while the instance is suspended, crashed or migrating is delivered on rehydrate rather than dropped. Each iteration commit reports its consumed offset into the log's watermark, the instance's floor. A looping durable instance that finds nothing to consume at a committed iteration boundary parks: the controller journals `parked(N[, wake_deadline])` and evicts it from its runner, registry state `suspended`. A new input-log append or the journaled wake deadline rehydrates it through the normal bind path, which re-forwards the log's records above the floor. Every input to that decision is durable, so suspension survives a controller restart, and racing wake sources resolve to exactly one rehydration.
## Requirements
### Requirement: Post-start input is durably journaled before delivery

The controller SHALL append every post-start input for an instance whose input is journaled
(a durable bounded operator, an unbounded operator on the effectively-once tier, or a class of either kind that
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

#### Scenario: A sleeping unbounded operator's message is held and wakes it

- **WHEN** a message arrives for an unbounded class that declares `sleep_when_idle` while the instance is suspended
- **THEN** it SHALL be appended to the instance's input log, the instance SHALL be rehydrated, and the message SHALL be delivered by the bind's re-forward, once

#### Scenario: A plain sleeping unbounded operator's log holds only what nobody could take

- **WHEN** an unbounded operator that declares `sleep_when_idle` and neither the tier nor a journal is awake and bound, and the controller forwards the record at offset 9 to its runner
- **THEN** the Overwatch SHALL report 9 consumed, retention MAY release records at or below 9, and a later rehydrate SHALL re-forward only records above 9

#### Scenario: Duplicate send is absorbed

- **WHEN** the same message, carrying the same `flz.msg_id`, is sent twice before the instance
  consumes it
- **THEN** exactly one log record SHALL exist and the operator SHALL receive it once

#### Scenario: A message without an id is never absorbed

- **WHEN** the same payload is sent twice with no `flz.msg_id`
- **THEN** two records SHALL exist and the operator SHALL receive both

#### Scenario: Consumed entries are released

- **WHEN** iteration N commits with consumed offset 7
- **THEN** records at or below 7 SHALL be eligible for the log's retention and later rehydrates
  SHALL NOT re-deliver them

#### Scenario: A class that does not sleep pays nothing

- **WHEN** an unbounded class declares neither `sleep_when_idle` nor the effectively-once tier
- **THEN** no input log SHALL be written for its instances and every input SHALL be forwarded as today

### Requirement: A suspended instance rehydrates on new input or its durable timer

The controller SHALL rehydrate a `suspended` instance of either kind on a new input-log append
or on the elapse of its recorded wake deadline, which for a bounded operator is the earliest pending
processing-time timer its Cells held when it parked: respawn through the normal bind path on any
live runner (existing session fencing applies), restore its Cells from their snapshots and WAL
tails and, for a bounded operator, its journal state, deliver the log's retained tail above the
instance's floor through the one re-forward every bind runs, and continue: a bounded operator enters
iteration N+1, an unbounded operator resumes folding input. A wake on the deadline SHALL append nothing to the input log:
the restored Cells re-arm their pending timers, the due timer fires on the runner, and its
firing opens the pass at the tasklet that set it. An elapsed wake deadline SHALL count as
pending work wherever undelivered input does, so a wake that found no runner to rehydrate onto
SHALL be retried on the next convergence tick. Rehydration SHALL survive controller
restart/redeploy: at boot an instance whose park record exists, whose log owes nothing and
whose wake deadline has not elapsed SHALL enter `suspended` without binding, and any other
SHALL bind. Concurrent
wake sources SHALL resolve to exactly one rehydration. Whether a suspended instance still owes
input SHALL be read from the log's tail and the instance's floor, not from a decode of the file.

#### Scenario: New message wakes a suspended instance

- **WHEN** a message arrives for an instance suspended at iteration N
- **THEN** the instance SHALL be respawned (possibly on a different runner), restored from
  journal + iter-N snapshots, and SHALL process the message as iteration N+1

#### Scenario: A sleeping unbounded operator wakes with its state

- **WHEN** a message arrives for an unbounded instance that slept with a keyed Cell holding a running total
- **THEN** the instance SHALL be respawned on a live runner, that Cell SHALL be restored to the total it held before the sleep, and the message SHALL be folded onto it

#### Scenario: Suspension survives controller redeploy

- **WHEN** the controller restarts while an instance of either kind is suspended and a message then arrives
- **THEN** the instance SHALL rehydrate exactly as if no restart had happened

#### Scenario: Durable timer fires without input

- **WHEN** a bounded instance parked with a wake deadline, the earliest pending processing-time timer its Cells held, reaches that deadline with no input
- **THEN** it SHALL be rehydrated, nothing SHALL be appended to its input log, and the tasklet that set the timer SHALL run `on_timer` on its Cell as iteration N+1

#### Scenario: A wake with no runner is retried

- **WHEN** an instance's wake deadline elapses while no runner is active, and a runner then connects
- **THEN** the instance SHALL be rehydrated on the next convergence tick and its due timer SHALL fire

#### Scenario: A park landing during an un-park keeps the suspension

- **WHEN** the rehydrated instance parks again before the un-park task has reported its bound state
- **THEN** the Overwatch SHALL stay suspended (the runner has already torn the instance down),
  the park record written by that park SHALL stand, and the Overwatch SHALL rehydrate again only
  if the log still holds records above the instance's floor or its wake deadline has elapsed; the task's stale bound state SHALL
  NOT be adopted

#### Scenario: Racing wake sources rehydrate once

- **WHEN** an input-log append and a timer deadline fire for the same suspended instance
  concurrently
- **THEN** exactly one rehydration SHALL occur

### Requirement: An idle instance sleeps durably and is evicted from memory

Sleeping when idle SHALL be a class setting of both kinds, `sleep_when_idle`, declared in every
SDK, announced by the runner in the `AddOperator` response and recorded by the controller
beside the class's other declarations: on by default for a bounded operator whose persistence is on,
off by default for an unbounded operator, and refused at registration as `SleepWithoutPersistence` on a
class of either kind whose persistence is off. The idle threshold SHALL be
`FLEZHA_PARK_IDLE_MS`, default 600 000 ms, one knob for both kinds; no knob SHALL carry a kind
in its name.

The SDK SHALL, when an instance of a class that sleeps has been idle past the threshold, send
`IterPark` (the iteration a bounded operator sleeps at, `0` for an unbounded operator, and, for a bounded operator, a wake
deadline: the earliest pending processing-time timer across the instance's Cells, none when no
Cell holds one) and remove the instance from runner memory; the controller SHALL write the park
record durably before the eviction, then tear the instance down without a terminal completion:
registry state `suspended`, zero live runner resources. An instance is idle when no cell of it
holds a queued, in-flight, held-back, retrying or unflushed item, no
input tail is owed, for an unbounded operator no cell timer is pending (a bounded operator sleeps with its timers
pending, its wake deadline naming the earliest) and, for a bounded operator, its pass sheet is `Waiting` with an empty queue at a
committed iteration boundary, the boundary before its first pass included. The threshold SHALL
be timed as one deadline on the core's deferred-wake list, armed once and re-armed from the
cells' latest activity when it comes due early, promoted by a thread already waiting in `poll`,
and SHALL hold no thread and touch no input path; an instance found busy when it comes due
SHALL NOT sleep and the deadline SHALL re-arm. A bounded operator's processing-time timer that comes
due inside the idle window SHALL fire and open a pass, as a message does, so the instance is
not idle when the deadline comes due. No class-level wake setting SHALL exist: a bounded operator's
wake deadline is what its tasklets' timers say. An iteration boundary SHALL be the only place a
bounded operator waits for a message. An eviction-hysteresis knob SHALL avoid park/rehydrate thrash
under trickling input.

The park record SHALL be a per-instance fact in the class registry's durable sidecar for either
kind, holding the wake deadline when one was given; it SHALL NOT be a task-journal entry. The
bind that wakes the instance SHALL clear it before the runner is told to start the instance.

The suspended Overwatch SHALL hibernate: a sleeper holds nothing of its bind on
its heap (the restore frames and plugin setup are garbage once it parks).

#### Scenario: A sleeper costs the controller a minimal heap

- **WHEN** an instance parks
- **THEN** its Overwatch SHALL be hibernated, and a resting population SHALL cost the controller on the order of tens of KiB per sleeper, not the bind's working set

#### Scenario: Idle chat operator frees the runner

- **WHEN** a looping instance commits iteration N and nothing arrives past the idle threshold
- **THEN** the park record SHALL be written, the instance SHALL be removed from runner memory, and the registry SHALL report it `suspended`

#### Scenario: An idle unbounded operator that declares sleeping frees the runner

- **WHEN** an unbounded class declares `sleep_when_idle` and an instance of it receives nothing for the idle threshold with every cell drained and flushed and no timer pending
- **THEN** the runner SHALL send `IterPark` with iteration `0`, the park record SHALL be written, the instance SHALL be removed from runner memory, and the registry SHALL report it `suspended`

#### Scenario: An unbounded operator that does not declare sleeping never sleeps

- **WHEN** an unbounded class leaves `sleep_when_idle` at its default and an instance of it receives nothing for hours
- **THEN** no `IterPark` SHALL be sent and the instance SHALL stay bound

#### Scenario: A busy instance does not sleep at the deadline

- **WHEN** the idle deadline comes due while a cell of the instance still holds an item in a retry backoff, an unflushed buffer or, on an unbounded operator, a pending timer
- **THEN** the instance SHALL NOT sleep and the deadline SHALL be re-armed

#### Scenario: Sleeping without persistence is refused

- **WHEN** a class of either kind declares `sleep_when_idle` with its persistence switch off, in any SDK
- **THEN** registration SHALL fail with an error containing `SleepWithoutPersistence` and nothing SHALL be declared

#### Scenario: A park records the earliest pending timer as its deadline

- **WHEN** a looping bounded instance's Cells hold processing-time timers at 90 000 and 60 000 when it sleeps
- **THEN** its `IterPark` SHALL carry 60 000 as its wake deadline and the park record SHALL hold it

#### Scenario: A timer due inside the idle window fires instead of parking

- **WHEN** a looping bounded instance commits iteration N holding a timer due before the idle threshold elapses
- **THEN** the timer SHALL fire and open iteration N+1, and no park record SHALL be written for the boundary

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
input records it will forward after the bind (`AddOperator.inbox_tail_count`, the log's tail
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

