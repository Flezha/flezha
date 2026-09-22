# workflow-durable-suspension Specification

## Purpose

The workflow-durable-suspension capability lets a waiting durable workflow instance cost nothing while losing nothing. Post-start input for a durable workflow instance goes to the one durable input log a stream on the effectively-once tier uses, stamped `flz.offset` and deduplicated by a retained `flz.msg_id`, **before** it is forwarded, so a message sent while the instance is suspended, crashed or migrating is delivered on rehydrate rather than dropped. Each iteration commit reports its consumed offset into the log's watermark, the instance's floor. A looping durable instance that finds nothing to consume at a committed iteration boundary parks: the controller journals `parked(N[, wake_deadline])` and evicts it from its runner, registry state `suspended`. A new input-log append or the journaled wake deadline rehydrates it through the normal bind path, which re-forwards the log's records above the floor. Every input to that decision is durable, so suspension survives a controller restart, and racing wake sources resolve to exactly one rehydration.
## Requirements
### Requirement: Post-start input is durably journaled before delivery

The controller SHALL append every post-start input for a durable workflow instance to the
instance's durable input log (`durable-input-log`, the same module, file and offset space a
stream on the effectively-once tier uses) with a monotonic offset **before** forwarding it to
the runner, stamped `flz.offset`. An input carrying a `flz.msg_id` already retained in the log
SHALL be absorbed at append; an input carrying no id SHALL always be appended. Entries at or
below the offset the latest `iter_committed` recorded SHALL be released to the log's retention.
Message payloads SHALL live only in the input log, never in `task.journal`.

#### Scenario: Input survives the instance being down

- **WHEN** a message arrives for a suspended (or crashed, not-yet-respawned) workflow instance
- **THEN** it SHALL be durably appended to the instance's input log and delivered when the
  instance next runs, not dropped

#### Scenario: Duplicate send is absorbed

- **WHEN** the same message, carrying the same `flz.msg_id`, is sent twice before the instance
  consumes it
- **THEN** exactly one log record SHALL exist and the operative SHALL receive it once

#### Scenario: A message without an id is never absorbed

- **WHEN** the same payload is sent twice with no `flz.msg_id`
- **THEN** two records SHALL exist and the operative SHALL receive both

#### Scenario: Consumed entries are truncated

- **WHEN** iteration N commits with consumed offset 7
- **THEN** records at or below 7 SHALL be eligible for the log's retention and later rehydrates
  SHALL NOT re-deliver them

### Requirement: A waiting loop parks durably and is evicted from memory

The SDK SHALL, when a durable workflow instance of any loop mode has nothing queued at a
committed iteration boundary past the idle threshold, the boundary before its first pass
included, send a park notification (iteration N, optional wake deadline) and the controller
SHALL journal `parked(N[, deadline])`, then tear the instance down without a terminal
completion: registry state `suspended`, zero live runner resources. The idle threshold SHALL be
timed as a deadline on the core's deferred-wake list, promoted by a thread already waiting in
`poll`, and SHALL hold no thread; a message that arrives before it comes due SHALL make the
deadline a no-op. An iteration boundary SHALL be the only place an instance waits for a
message: no tasklet SHALL block on its inbox mid-iteration, so parking SHALL only occur at a
committed iteration boundary. An eviction-hysteresis knob SHALL avoid park/rehydrate thrash
under trickling input.

The suspended Overwatch SHALL hibernate: a sleeper holds nothing of its bind on
its heap (the restore frames and plugin setup are garbage once it parks).

#### Scenario: A sleeper costs the controller a minimal heap

- **WHEN** an instance parks
- **THEN** its Overwatch SHALL be hibernated, and a resting population SHALL cost the controller on the order of tens of KiB per sleeper, not the bind's working set

#### Scenario: Idle chat operative frees the runner

- **WHEN** a looping instance commits iteration N and nothing arrives past the idle threshold
- **THEN** `parked(N)` SHALL be journaled, the instance SHALL be removed from runner memory, and the registry SHALL report it `suspended`

#### Scenario: A once instance waiting for its first message parks

- **WHEN** a durable `once` instance is started without a message and none arrives within the idle threshold
- **THEN** `parked(0)` SHALL be journaled, the instance SHALL be removed from runner memory, the registry SHALL report it `suspended`, and the next message SHALL rehydrate it into pass 0

#### Scenario: A message arriving mid-iteration waits for the boundary

- **WHEN** a message reaches a `PerInput` instance while an iteration is still running
- **THEN** it SHALL be journaled to the inbox, the runner SHALL hold it in the instance's queue, it SHALL seed the next iteration after the current one commits, and no tasklet SHALL receive it mid-iteration

#### Scenario: A waiting population holds no thread

- **WHEN** ten thousand instances wait at a committed boundary inside the idle threshold
- **THEN** no thread SHALL exist on their behalf and the runner SHALL consume no measurable CPU
  for the wait

#### Scenario: A message parked unread does not wake the instance again

- **WHEN** a durable instance waiting for a message receives one it cannot read, parks it to the dead-letter queue, and then waits past the idle threshold
- **THEN** its park SHALL carry that message's offset, the message SHALL no longer be owed, and the instance SHALL stay `suspended` rather than be rehydrated to receive it again; a rehydrated instance that was owed the message SHALL count it as received and park on the idle threshold

### Requirement: A suspended instance rehydrates on new input or its durable timer

The controller SHALL rehydrate a `suspended` instance on a new input-log append or on the elapse
of its journaled wake deadline: respawn through the normal workflow spawn path on any live runner
(existing session fencing applies), restore journal state and iteration-boundary snapshots,
deliver the log's retained tail above the instance's floor through the one re-forward every bind
runs, and enter iteration N+1. Rehydration SHALL survive controller restart/redeploy (all inputs
are durable: journal, snapshots, input log). Concurrent wake sources SHALL resolve to exactly one
rehydration. Whether a suspended instance still owes input SHALL be read from the log's tail and
the instance's floor, not from a decode of the file.

#### Scenario: New message wakes a suspended instance

- **WHEN** a message arrives for an instance suspended at iteration N
- **THEN** the instance SHALL be respawned (possibly on a different runner), restored from
  journal + iter-N snapshots, and SHALL process the message as iteration N+1

#### Scenario: Suspension survives controller redeploy

- **WHEN** the controller restarts while an instance is suspended and a message then arrives
- **THEN** the instance SHALL rehydrate exactly as if no restart had happened

#### Scenario: Durable timer fires without input

- **WHEN** an instance parked with a wake deadline reaches that deadline with no input
- **THEN** it SHALL be rehydrated and the loop SHALL observe a timer wake (not an input item)

#### Scenario: A park landing during an un-park keeps the suspension

- **WHEN** the rehydrated driver parks again before the un-park task has reported its bound state
- **THEN** the Overwatch SHALL stay suspended (the runner has already torn the instance down)
  and SHALL rehydrate again only if the log still holds records above the instance's floor; the
  task's stale bound state SHALL NOT be adopted

#### Scenario: Racing wake sources rehydrate once

- **WHEN** an input-log append and a timer deadline fire for the same suspended instance
  concurrently
- **THEN** exactly one rehydration SHALL occur

### Requirement: A rehydrated loop waits for the inbox tail it is owed before it parks

The controller SHALL tell a rehydrating bind how many journaled, undelivered
inbox items it will forward after the bind (`AddOperative.inbox_tail_count`),
and the SDK loop SHALL NOT start its park-idle clock until that many items have
arrived (bounded by an owed-tail wait of 60 s, after which it parks and the
controller's rehydrate-if-pending forwards the tail again). A tail forwarded
after the bind under load would otherwise reach a driver that has already
parked and be dropped at the socket.

#### Scenario: The tail arrives after the park idle would have expired
- **WHEN** a suspended loop with one undelivered inbox item is rehydrated and the controller's forward of that item reaches the runner later than the park idle after the bind
- **THEN** the loop SHALL still be waiting, consume the item, run the iteration and reply, and SHALL NOT have parked in between

#### Scenario: Nothing owed
- **WHEN** a loop binds with `inbox_tail_count = 0`
- **THEN** its park-idle clock runs from the bind as before

