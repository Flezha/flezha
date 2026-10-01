# durable-suspension Specification

## Purpose

The durable-suspension capability lets a waiting durable steps instance cost nothing while losing nothing. Post-start input for a durable steps instance goes to the one durable input log a flow operator on the effectively-once tier uses, stamped `flz.offset` and deduplicated by a retained `flz.msg_id`, **before** it is forwarded, so a message sent while the instance is suspended, crashed or migrating is delivered on rehydrate rather than dropped. Each iteration commit reports its consumed offset into the log's watermark, the instance's floor. A looping durable instance that finds nothing to consume at a committed iteration boundary parks: the controller journals `parked(N[, wake_deadline])` and evicts it from its runner, registry state `suspended`. A new input-log append or the journaled wake deadline rehydrates it through the normal bind path, which re-forwards the log's records above the floor. Every input to that decision is durable, so suspension survives a controller restart, and racing wake sources resolve to exactly one rehydration.
## Requirements
### Requirement: Post-start input is durably journaled before delivery

The controller SHALL append every post-start input for an instance whose input is journaled
(a durable steps operator, a flow operator on the effectively-once tier, or a class of either run
that sleeps) to the instance's durable input log (`durable-input-log`, the same
module, file and offset space for every such instance) with a monotonic offset **before**
forwarding it to the runner, stamped `flz.offset`. An input carrying a `flz.msg_id` already
retained in the log SHALL be absorbed at append; an input carrying no id SHALL always be
appended. The log's floor SHALL be the durable consumption the instance's own records carry
(`durable-input-log`): for a flow operator, whether it declares the tier or only sleeps,
its Cells' durable consumed offsets and the processed prefix the controller confirms; for a
steps operator, the journal's iteration commits and released inputs. What the controller has
forwarded SHALL NOT move the floor. Entries at or below the floor
SHALL be released to the log's retention. Message payloads SHALL live only in the input log,
never in `task.journal`. A class that declares none of the three SHALL have no durable input
and SHALL pay no append.

#### Scenario: Input survives the instance being down

- **WHEN** a message arrives for a suspended (or crashed, not-yet-respawned) instance whose input is journaled
- **THEN** it SHALL be durably appended to the instance's input log and delivered when the
  instance next runs, not dropped

#### Scenario: A sleeping flow operator's message is held and wakes it

- **WHEN** a message arrives for a flow class that sleeps while the instance is suspended
- **THEN** it SHALL be appended to the instance's input log, the instance SHALL be rehydrated, and the message SHALL be delivered by the bind's re-forward, once

#### Scenario: A plain sleeping flow operator folds nothing twice after a controller restart

- **WHEN** a flow operator that sleeps and is not on the tier has folded `a:100`, slept, woken on `a:1` and `a:2`, and the controller restarts while it is awake, then `a:10` is sent
- **THEN** its total for `a` SHALL be 113, and none of the three earlier inputs SHALL be folded again

#### Scenario: A forward alone does not release input

- **WHEN** the controller forwards the record at offset 9 to the runner of a plain sleeping flow operator and the runner dies before the record's change is durable
- **THEN** offset 9 SHALL NOT be released and the rebind SHALL re-forward it

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

- **WHEN** a flow class declares `sleep_after: false` and not the effectively-once tier
- **THEN** no input log SHALL be written for its instances and every input SHALL be forwarded as today

### Requirement: A suspended instance rehydrates on new input or its durable timer

The controller SHALL rehydrate a `suspended` instance of either run on a new input-log append
or on the elapse of its recorded wake deadline, which for a steps operator is the earliest pending
processing-time timer its Cells held when it parked: respawn through the normal bind path on any
live runner (existing session fencing applies), restore its Cells from their snapshots and WAL
tails and, for a steps operator, its journal state, deliver the log's retained tail above the
instance's floor through the one re-forward every bind runs, and continue: a steps operator enters
iteration N+1, a flow operator resumes folding input. A wake on the deadline SHALL append nothing to the input log:
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

#### Scenario: A sleeping flow operator wakes with its state

- **WHEN** a message arrives for a flow instance that slept with a keyed Cell holding a running total
- **THEN** the instance SHALL be respawned on a live runner, that Cell SHALL be restored to the total it held before the sleep, and the message SHALL be folded onto it

#### Scenario: Suspension survives controller redeploy

- **WHEN** the controller restarts while an instance of either run is suspended and a message then arrives
- **THEN** the instance SHALL rehydrate exactly as if no restart had happened

#### Scenario: Durable timer fires without input

- **WHEN** a steps instance parked with a wake deadline, the earliest pending processing-time timer its Cells held, reaches that deadline with no input
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

Sleeping when idle SHALL be a class setting of every class that keeps its work, its
`durable` tree's `sleep_after`, declared in every SDK, announced by the runner in the
`AddOperator` response (`sleep_after_ms`) and recorded by the controller beside the class's
other declarations. The idle threshold SHALL be that interval, for a flow instance and a
steps instance alike; no knob SHALL carry a run in its name.

The SDK SHALL, when an instance of a class that sleeps has been idle past the threshold, send
`IterPark` (the iteration a steps operator sleeps at, `0` for a flow operator, and, for a steps operator, a wake
deadline: the earliest pending processing-time timer across the instance's Cells, none when no
Cell holds one) and remove the instance from runner memory; the controller SHALL write the park
record durably before the eviction, then tear the instance down without a terminal completion:
registry state `suspended`, zero live runner resources. An instance is idle when no cell of it
holds a queued, in-flight, held-back, retrying or unflushed item, no
input tail is owed, for a flow operator no cell timer is pending (a steps operator sleeps with its timers
pending, its wake deadline naming the earliest) and, for a steps operator, its pass sheet is `Waiting` with an empty queue at a
committed iteration boundary, the boundary before its first pass included. The threshold SHALL
be timed as one deadline on the core's deferred-wake list, armed once and re-armed from the
cells' latest activity when it comes due early, promoted by a thread already waiting in `poll`,
and SHALL hold no thread and touch no input path; an instance found busy when it comes due
SHALL NOT sleep and the deadline SHALL re-arm. A steps operator's processing-time timer that comes
due inside the idle window SHALL fire and open a pass, as a message does, so the instance is
not idle when the deadline comes due. No class-level wake setting SHALL exist: a steps operator's
wake deadline is what its tasklets' timers say. An iteration boundary SHALL be the only place a
steps operator waits for a message. An eviction-hysteresis knob SHALL avoid park/rehydrate thrash
under trickling input.

The park record SHALL be a per-instance fact in the class registry's durable sidecar for either
run, holding the wake deadline when one was given and nothing else; it SHALL NOT be a
task-journal entry and SHALL hold no consumed offset. The bind that wakes the instance SHALL
clear it before the runner is told to start the instance; clearing it SHALL release nothing and
lower no floor.

The suspended Overwatch SHALL hibernate: a sleeper holds nothing of its bind on
its heap (the restore frames and plugin setup are garbage once it parks).

#### Scenario: A sleeper costs the controller a minimal heap

- **WHEN** an instance parks
- **THEN** its Overwatch SHALL be hibernated, and a resting population SHALL cost the controller on the order of tens of KiB per sleeper, not the bind's working set

#### Scenario: Idle chat operator frees the runner

- **WHEN** a looping instance commits iteration N and nothing arrives past the idle threshold
- **THEN** the park record SHALL be written, the instance SHALL be removed from runner memory, and the registry SHALL report it `suspended`

#### Scenario: An idle flow operator that declares sleeping frees the runner

- **WHEN** an instance of a flow class that sleeps receives nothing for its class's `sleep_after` with every cell drained and flushed and no timer pending
- **THEN** the runner SHALL send `IterPark` with iteration `0`, the park record SHALL be written, the instance SHALL be removed from runner memory, and the registry SHALL report it `suspended`

#### Scenario: A flow operator declaring `sleep_after: false` never sleeps

- **WHEN** a flow class declares `sleep_after: false` and an instance of it receives nothing for hours
- **THEN** no `IterPark` SHALL be sent and the instance SHALL stay bound

#### Scenario: A busy instance does not sleep at the deadline

- **WHEN** the idle deadline comes due while a cell of the instance still holds an item in a retry backoff, an unflushed buffer or, on a flow operator, a pending timer
- **THEN** the instance SHALL NOT sleep and the deadline SHALL be re-armed

#### Scenario: A park records the earliest pending timer as its deadline

- **WHEN** a looping steps instance's Cells hold processing-time timers at 90 000 and 60 000 when it sleeps
- **THEN** its `IterPark` SHALL carry 60 000 as its wake deadline and the park record SHALL hold it

#### Scenario: A timer due inside the idle window fires instead of parking

- **WHEN** a looping steps instance commits iteration N holding a timer due before the idle threshold elapses
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
- **THEN** the journal SHALL record that message's offset as released, the message SHALL no longer be owed, and the instance SHALL stay `suspended` rather than be rehydrated to receive it again; a rehydrated instance that was owed the message SHALL count it as received and park on the idle threshold

#### Scenario: A woken instance keeps its release across a controller restart

- **WHEN** a durable instance that dead-lettered a message unread and slept is woken, and the controller restarts before its next iteration commits
- **THEN** the message SHALL NOT be owed again and SHALL NOT be dead-lettered a second time

### Requirement: A rehydrated instance waits for the input tail it is owed before it sleeps

The controller SHALL tell a rehydrating bind of either run how many journaled, undelivered
input records it will forward after the bind (`AddOperator.inbox_tail_count`, the log's tail
above the instance's floor), and SHALL forward them after that bind, on every bind the
instance takes: a rehydrate from suspension, a rewind, the convergence loop's rebind after its
runner died, and the takeover by a restarted runner presenting the same stable identity. The
SDK SHALL NOT start the instance's idle clock until that many records have arrived (bounded by
an owed-tail wait of 60 s, after which it sleeps and the controller's rehydrate-if-pending
forwards the tail again). A tail forwarded after the bind under load would otherwise reach an
instance that has already slept and be dropped at the socket. The controller SHALL NOT forward
the tail to a process that still runs the instance: a reconnect of the same process moves its
session and forwards nothing.

#### Scenario: The tail arrives after the park idle would have expired
- **WHEN** a suspended instance with one undelivered record is rehydrated and the controller's forward of that record reaches the runner later than the idle window after the bind
- **THEN** the instance SHALL still be awake, consume the record, and SHALL NOT have slept in between

#### Scenario: A crashed instance rebound elsewhere is told and sent its tail
- **GIVEN** a durable per-input steps operator whose runner is killed with two journaled input records not yet consumed
- **WHEN** the instance is bound onto a replacement runner by the convergence loop or by a same-identity takeover
- **THEN** the `AddOperator` SHALL carry `inbox_tail_count = 2`, both records SHALL be forwarded after the bind with their original offsets, and the instance SHALL run them as its next iterations without sleeping first

#### Scenario: Nothing owed
- **WHEN** an instance binds with `inbox_tail_count = 0`
- **THEN** its idle clock runs from the bind as before

### Requirement: The sleep interval is the class's `sleep_after`, on by default for a steps class

An instance of a class whose `durable` is `local` or `remote` SHALL sleep once it has been
idle at a committed boundary for the class's `sleep_after`. A class that declares no
`sleep_after` SHALL sleep after six hours when it runs in steps and SHALL never sleep when it
runs as a flow, because a sleeping flow instance has its input journaled before it is
forwarded. A class that declares `sleep_after: false` SHALL never sleep. No environment variable SHALL set or override the interval;
<!-- docs-citation-record:begin -->`FLEZHA_PARK_IDLE_MS` SHALL not exist<!-- docs-citation-record:end -->. A class whose `durable` is `false` SHALL have no
`sleep_after` to declare. The sleep, the eviction, the wake on input or on a durable timer
and the input tail owed on wake SHALL be what the other requirements of this capability
describe, for a flow instance and a steps instance alike.

#### Scenario: A steps class that says nothing sleeps after six hours

- **WHEN** a durable steps class declares no `sleep_after` and an instance is idle at a committed boundary
- **THEN** the instance SHALL ship its park at six hours of idleness and no sooner, whatever the environment carries

#### Scenario: A flow class that says nothing never sleeps

- **WHEN** a durable flow class declares no `sleep_after` and an instance is idle
- **THEN** the class SHALL announce no `sleep_after_ms` and the instance SHALL stay bound, its input forwarded without the input log

#### Scenario: A short interval on the class

- **WHEN** a class declares `durable: {local, sleep_after: 2 seconds}` and an instance goes idle
- **THEN** the instance SHALL be shown `suspended` by `flz operator info` within a few seconds and SHALL answer the next message with its cells restored

#### Scenario: A flow instance sleeps like a steps instance

- **WHEN** a flow class declares `sleep_after: 2 seconds` and its instance goes idle
- **THEN** it SHALL park, be evicted and wake on its next input exactly as a steps instance does

