# bounded-durable-suspension Specification

## Purpose

The bounded-durable-suspension capability lets a waiting durable bounded instance cost nothing while losing nothing. Post-start input for a durable bounded instance is no longer an in-memory channel: the controller appends every message to a per-instance append-only `inbox.wal` — same WalWriter/FsyncBatcher substrate as `op.journal` — with a monotonic offset and a dedup id (sender-supplied message id, else a payload hash) **before** forwarding it to the executor, so a duplicate send inside the undelivered window is absorbed at append and a message sent while the instance is suspended, crashed, or migrating is delivered on rehydrate rather than dropped. Payloads live only in `inbox.wal`; `op.journal` carries offsets, and the consumed offset recorded by `iter_committed` (`bounded-durable-loop`) is what makes consumed entries truncatable. When a looping durable instance blocks on an empty inbox at a committed iteration boundary, the SDK parks: the controller journals `parked(N[, wake_deadline])` and tears the instance down with no terminal completion — registry state `suspended`, zero live executor resources — while a mid-iteration `recv` still blocks in memory as before, and an eviction-hysteresis knob keeps trickling input from thrashing the cycle. A new inbox append or the elapse of the journaled wake deadline rehydrates the instance through the normal bounded spawn path on any live executor (existing session fencing applies): journal state and iteration-boundary snapshots are restored, the undelivered inbox tail is delivered from the recorded offset, and the loop enters iteration N+1. Every input to that decision is durable, so suspension survives controller restart or redeploy, and racing wake sources resolve to exactly one rehydration.

## Requirements

### Requirement: Post-start input is durably journaled before delivery

The controller SHALL append every post-start input for a durable bounded
instance to a per-instance append-only `inbox.wal` (same WalWriter/FsyncBatcher
substrate as `op.journal`) with a monotonic offset and a dedup id — the
sender-supplied message id when present, else a payload hash — **before**
forwarding it to the executor. A duplicate dedup id within the undelivered
window SHALL be dropped at append. Entries at or below the consumed offset
recorded by `iter_committed` SHALL be truncated. Message payloads SHALL live
only in `inbox.wal`, never in `op.journal`.

#### Scenario: Input survives the instance being down

- **WHEN** a message arrives for a suspended (or crashed, not-yet-respawned) bounded instance
- **THEN** it SHALL be durably appended to the instance's `inbox.wal` and delivered when the instance next runs — not dropped

#### Scenario: Duplicate send is absorbed

- **WHEN** the same message (same dedup id) is sent twice before the instance consumes it
- **THEN** exactly one inbox entry SHALL exist and the operative SHALL receive it once

#### Scenario: Consumed entries are truncated

- **WHEN** iteration N commits with consumed offset 7
- **THEN** inbox entries with offset ≤ 7 SHALL be eligible for truncation and later rehydrates SHALL NOT re-deliver them

### Requirement: A waiting loop parks durably and is evicted from memory

The SDK SHALL, when a looping durable bounded instance blocks on an empty
inbox at a committed iteration boundary, send a park notification (iteration N,
optional wake deadline) and the controller SHALL journal `parked(N[,
deadline])`, then tear the instance down without a terminal completion —
registry state `suspended`, zero live executor resources. Parking SHALL only
occur at a committed iteration boundary; a mid-iteration `recv` SHALL block in
memory as today. An eviction-hysteresis knob SHALL avoid park/rehydrate thrash
under trickling input.

#### Scenario: Idle chat operative frees the executor

- **WHEN** a looping instance commits iteration N and its inbox stays empty past the idle threshold
- **THEN** `parked(N)` SHALL be journaled, the instance SHALL be removed from executor memory, and the registry SHALL report it `suspended`

#### Scenario: Mid-iteration wait does not park

- **WHEN** an operation inside an uncommitted iteration waits on `recv`
- **THEN** the instance SHALL keep blocking in memory and SHALL NOT be suspended

### Requirement: A suspended instance rehydrates on new input or its durable timer

The controller SHALL rehydrate a `suspended` instance on a new inbox append or
on the elapse of its journaled wake deadline: respawn through the normal bounded spawn
path on any live executor (existing session fencing applies), restore journal
state and iteration-boundary snapshots, deliver the undelivered inbox tail from
the recorded offset, and enter iteration N+1. Rehydration SHALL survive
controller restart/redeploy (all inputs are durable: journal, snapshots,
inbox). Concurrent wake sources SHALL resolve to exactly one rehydration.

#### Scenario: New message wakes a suspended instance

- **WHEN** a message arrives for an instance suspended at iteration N
- **THEN** the instance SHALL be respawned (possibly on a different executor), restored from journal + iter-N snapshots, and SHALL process the message as iteration N+1

#### Scenario: Suspension survives controller redeploy

- **WHEN** the controller restarts while an instance is suspended and a message then arrives
- **THEN** the instance SHALL rehydrate exactly as if no restart had happened

#### Scenario: Durable timer fires without input

- **WHEN** an instance parked with a wake deadline reaches that deadline with no input
- **THEN** it SHALL be rehydrated and the loop SHALL observe a timer wake (not an input item)

#### Scenario: Racing wake sources rehydrate once

- **WHEN** an inbox append and a timer deadline fire for the same suspended instance concurrently
- **THEN** exactly one rehydration SHALL occur
