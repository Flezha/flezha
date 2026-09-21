# workflow-durable-suspension Specification

## Purpose

The workflow-durable-suspension capability lets a waiting durable workflow instance cost nothing while losing nothing. Post-start input for a durable workflow instance is no longer an in-memory channel: the controller appends every message to a per-instance append-only `inbox.wal` — same WalWriter/FsyncBatcher substrate as `task.journal` — with a monotonic offset and a dedup id (sender-supplied message id, else a payload hash) **before** forwarding it to the runner, so a duplicate send inside the undelivered window is absorbed at append and a message sent while the instance is suspended, crashed, or migrating is delivered on rehydrate rather than dropped. Payloads live only in `inbox.wal`; `task.journal` carries offsets, and the consumed offset recorded by `iter_committed` (`workflow-durable-loop`) is what makes consumed entries truncatable. When a looping durable instance blocks on an empty inbox at a committed iteration boundary, the SDK parks: the controller journals `parked(N[, wake_deadline])` and tears the instance down with no terminal completion — registry state `suspended`, zero live runner resources — while a mid-iteration `recv` still blocks in memory as before, and an eviction-hysteresis knob keeps trickling input from thrashing the cycle. A new inbox append or the elapse of the journaled wake deadline rehydrates the instance through the normal workflow spawn path on any live runner (existing session fencing applies): journal state and iteration-boundary snapshots are restored, the undelivered inbox tail is delivered from the recorded offset, and the loop enters iteration N+1. Every input to that decision is durable, so suspension survives controller restart or redeploy, and racing wake sources resolve to exactly one rehydration.
## Requirements
### Requirement: Post-start input is durably journaled before delivery

The controller SHALL append every post-start input for a durable workflow
instance to a per-instance append-only `inbox.wal` (same WalWriter/FsyncBatcher
substrate as `task.journal`) with a monotonic offset and a dedup id — the
sender-supplied message id when present, else a payload hash — **before**
forwarding it to the runner. A duplicate dedup id within the undelivered
window SHALL be dropped at append. Entries at or below the consumed offset
recorded by `iter_committed` SHALL be truncated. Message payloads SHALL live
only in `inbox.wal`, never in `task.journal`.

#### Scenario: Input survives the instance being down

- **WHEN** a message arrives for a suspended (or crashed, not-yet-respawned) workflow instance
- **THEN** it SHALL be durably appended to the instance's `inbox.wal` and delivered when the instance next runs — not dropped

#### Scenario: Duplicate send is absorbed

- **WHEN** the same message (same dedup id) is sent twice before the instance consumes it
- **THEN** exactly one inbox entry SHALL exist and the operative SHALL receive it once

#### Scenario: Consumed entries are truncated

- **WHEN** iteration N commits with consumed offset 7
- **THEN** inbox entries with offset ≤ 7 SHALL be eligible for truncation and later rehydrates SHALL NOT re-deliver them

### Requirement: A waiting loop parks durably and is evicted from memory

The SDK SHALL, when a looping durable workflow instance blocks on an empty
inbox at a committed iteration boundary, send a park notification (iteration N,
optional wake deadline) and the controller SHALL journal `parked(N[,
deadline])`, then tear the instance down without a terminal completion:
registry state `suspended`, zero live runner resources. An iteration boundary
SHALL be the only place an instance waits for input: no tasklet SHALL block
on its inbox mid-iteration, so parking SHALL only occur at a committed
iteration boundary. An eviction-hysteresis knob SHALL avoid park/rehydrate
thrash under trickling input.

The suspended Overwatch SHALL hibernate: a sleeper holds nothing of its bind on
its heap (the restore frames and plugin setup are garbage once it parks).

#### Scenario: A sleeper costs the controller a minimal heap

- **WHEN** an instance parks
- **THEN** its Overwatch SHALL be hibernated, and a resting population SHALL cost the controller on the order of tens of KiB per sleeper, not the bind's working set

#### Scenario: Idle chat operative frees the runner

- **WHEN** a looping instance commits iteration N and its inbox stays empty past the idle threshold
- **THEN** `parked(N)` SHALL be journaled, the instance SHALL be removed from runner memory, and the registry SHALL report it `suspended`

#### Scenario: A message arriving mid-iteration waits for the boundary

- **WHEN** a message reaches a `PerInput` instance while an iteration is still running
- **THEN** it SHALL be journaled to the inbox and SHALL seed the next iteration after the current one commits, and no tasklet SHALL receive it mid-iteration

### Requirement: A suspended instance rehydrates on new input or its durable timer

The controller SHALL rehydrate a `suspended` instance on a new inbox append or
on the elapse of its journaled wake deadline: respawn through the normal workflow spawn
path on any live runner (existing session fencing applies), restore journal
state and iteration-boundary snapshots, deliver the undelivered inbox tail from
the recorded offset, and enter iteration N+1. Rehydration SHALL survive
controller restart/redeploy (all inputs are durable: journal, snapshots,
inbox). Concurrent wake sources SHALL resolve to exactly one rehydration.

#### Scenario: New message wakes a suspended instance

- **WHEN** a message arrives for an instance suspended at iteration N
- **THEN** the instance SHALL be respawned (possibly on a different runner), restored from journal + iter-N snapshots, and SHALL process the message as iteration N+1

#### Scenario: Suspension survives controller redeploy

- **WHEN** the controller restarts while an instance is suspended and a message then arrives
- **THEN** the instance SHALL rehydrate exactly as if no restart had happened

#### Scenario: Durable timer fires without input

- **WHEN** an instance parked with a wake deadline reaches that deadline with no input
- **THEN** it SHALL be rehydrated and the loop SHALL observe a timer wake (not an input item)

#### Scenario: A park landing during an un-park keeps the suspension

- **WHEN** the rehydrated driver parks again before the un-park task has reported its bound state
- **THEN** the Overwatch SHALL stay suspended (the runner has already torn the instance down) and SHALL rehydrate again only if the inbox still owes undelivered input — the task's stale bound state SHALL NOT be adopted

#### Scenario: Racing wake sources rehydrate once

- **WHEN** an inbox append and a timer deadline fire for the same suspended instance concurrently
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

