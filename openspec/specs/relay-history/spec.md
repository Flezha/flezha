# relay-history Specification

## Purpose
The `relay_history` log kind: one `started` and one `ended` record per relay run with its origin, outcome and duration, written without stalling a relay, and the lookup that answers whether a run already happened.
## Requirements
### Requirement: The relay history SHALL record each run's start and end once

The controller SHALL append to the `relay_history` kind:

- one `started` record when a relay instance is deliberately started — by an operator, a
  partition group start, a schedule slot or manual run, a tasklet's spawn, a relay-to-relay
  call that creates its callee, or a member a rescale adds;
- one `ended` record when that instance ends for good — completion, a cooperative or hard
  stop, a stop of a halted instance, a namespace deletion, or a member a rescale removes;
- one `start_failed` record when a deliberate start fails after the controller accepted it.

A crash restart, a failover to another controller, a cold boot, a move, a sleep, a wake, a
rebind and a rescale's restart of a member it keeps SHALL write nothing. A start answered as a
duplicate of a live instance SHALL write nothing. The relay history SHALL NOT depend on the
observability subsystem being enabled.

#### Scenario: A spawned run leaves two records

- **WHEN** a tasklet spawns a steps relay that completes 40 seconds later
- **THEN** the relay history SHALL hold exactly one `started` and one `ended` record for it

#### Scenario: A crash restart is not a new run

- **WHEN** a relay's Overwatch crashes and its supervisor restarts it
- **THEN** no `started` or `ended` record SHALL be written for the restart

#### Scenario: A failover is not a new run

- **WHEN** the controller hosting a relay dies and the relay is restarted on another
- **THEN** no record SHALL be written for the move, and the relay's eventual `ended` record
  SHALL carry the `first_started_at` of its original start

#### Scenario: The convergence loop's restart of an unplaced relay is not a start

- **WHEN** the convergence loop starts a declared relay that has run nowhere past its grace
- **THEN** no `started` record SHALL be written and no `relay.started` event SHALL be emitted

#### Scenario: A run-once relay that ends before its start is read

- **WHEN** a steps relay completes before the starter reads its binding
- **THEN** its `started` record SHALL still be written, before or after its `ended` record,
  and both SHALL exist

### Requirement: Every history record SHALL stand alone

Every record SHALL carry `ts_us`, `node`, `event`, `namespace`, `relay`, `factory`, `run`
(`flow` or `steps`), `origin` and `first_started_at`, and `spawn_identity` when the instance
was spawned with one. An `ended` record SHALL also carry `outcome`, `halted`, `duration_ms`
(from `first_started_at`) and `error` (at most 512 bytes of text, empty when none). A
`start_failed` record SHALL carry `error`. A reader SHALL NOT need the matching `started`
record to show an `ended` one.

#### Scenario: An ended record without its start

- **WHEN** retention has deleted the file holding a run's `started` record
- **THEN** its `ended` record SHALL still name its factory, origin, start time, outcome and
  duration

### Requirement: The origin SHALL name who or what started the run

`origin` SHALL be a map with `kind` and the fields the kind knows:

- `operator`: `principal`, `source` (`cli`, `ui`, `http`, `mcp`, `sdk`), `request_id`;
- `group`: as `operator`, for a member of a partition group start;
- `schedule`: `schedule` (its name) and `slot`, or `slot: "manual"` with the manual run's
  `principal` and `source`;
- `spawn`: `parent_relay` when the requesting runner named one and the controller verified it,
  and the runner's `principal`;
- `call`: `parent_relay`, the calling relay;
- `rescale`: `principal` and `source` of the rescale's caller;
- `reconcile`: reserved for a start the controller makes on its own.

The origin SHALL be stamped by the controller at start into the relay's declared spec, with
`first_started_at`, so it survives restarts, failovers and moves. A start option named
`origin` or `first_started_at` written by a caller SHALL be discarded.

#### Scenario: An operator start

- **WHEN** principal `alice` starts a relay from the console
- **THEN** its `started` record's origin SHALL be `%{kind: "operator", principal: "alice",
  source: "ui", request_id: ...}`

#### Scenario: A forged origin is ignored

- **WHEN** a caller starts a relay with `opts: %{"origin" => %{"kind" => "schedule"}}`
- **THEN** the start SHALL proceed with the origin the controller derives from the caller, and
  the forged value SHALL NOT appear in any record

#### Scenario: A spawned relay names its parent

- **WHEN** relay `ma_fan` running on runner R spawns a `fan_trade` instance and R's SDK sends
  `parent_relay_id: "ma_fan"`
- **THEN** the `started` record's origin SHALL be `%{kind: "spawn", parent_relay: "ma_fan", ...}`

### Requirement: The outcome SHALL say how the run ended

An `ended` record's `outcome` SHALL be:

- `completed` when a steps run reported `ok` or a flow instance reported `completed`;
- `undone` when a steps run reported `error` and its runtime reported `undo: "complete"`;
- `failed` when a steps run reported `error` otherwise; when the runtime reported
  `undo: "partial"` the `error` SHALL end with `; rollback incomplete`;
- `stopped` when the instance ended on a stop someone requested — a cooperative stop that
  reported `cancelled`, a hard stop, a stop of a halted instance or a namespace deletion;
- `timed_out` when the instance reported `cancelled` and no stop was requested (its
  `max_duration` elapsed);
- `rescaled_away` when a rescale removed the member;
- `cancelled` when an instance reported `cancelled` for any other reason.

`halted` SHALL be true when the instance was halted at the moment it ended.

#### Scenario: A rolled-back run

- **WHEN** a `fan_trade` instance with `on_failure: undo_all` fails and every compensate hook ran
- **THEN** its `ended` record SHALL carry `outcome: "undone"` and the run's error

#### Scenario: A run that hit its time limit

- **WHEN** a steps instance's `max_duration` elapses and it ends `cancelled`
- **THEN** its `ended` record SHALL carry `outcome: "timed_out"`

#### Scenario: An operator stop

- **WHEN** an operator stops a running steps instance and it ends `cancelled`
- **THEN** its `ended` record SHALL carry `outcome: "stopped"`

### Requirement: Writing the history SHALL never stall a relay

The `started` and `start_failed` records SHALL be appended by the process that started the
relay, never inside a call the Overwatch is serving. The `ended` record SHALL be appended by
the Overwatch's first teardown step, which no budget overrun can skip, and only when the
Overwatch leaves for good (a stop, a completion or a namespace deletion; not a failed start,
not a move, not a rescale's restart of a member it keeps). The `relay_history` kind SHALL use
the `:group_commit` write policy with a queue bound (`relay_history_max_pending`, default
10,000) and an append timeout (`relay_history_append_ms`, default 1,000 ms). The
control channel SHALL NOT append.

#### Scenario: A slow disk does not fail a start

- **WHEN** the history writer takes three seconds to sync while a relay starts
- **THEN** the start SHALL succeed within its own bound, and the `started` record SHALL be
  written or counted as a timeout, never turn the start into a failed one

#### Scenario: A saturated history is counted

- **WHEN** appends arrive faster than the disk syncs and the queue reaches its bound
- **THEN** further appends SHALL be refused and counted in `dropped_count`, and a
  `log_overloaded` event SHALL name the kind and controller

### Requirement: The history SHALL answer whether a run already happened

The relay history SHALL answer `Flezha.Logs.RelayHistory.seen(namespace, key)`, with `key`
either `{:relay, id}` or `{:spawn_identity, identity}`, with `{:ended, relay_id, ts_us}`,
`{:started, relay_id, ts_us}`, `:none`, `{:unknown, unreachable_nodes}` or `:disabled`. Each
controller SHALL hold an in-memory index of its own records within the dedup window
(`relay_history_dedup_window_hours`, default 24) and at most `relay_history_dedup_max_entries`
(default 2,000,000) keys, rebuilt from its own files at boot before the kind accepts appends.
A lookup SHALL ask the local index first and, on a miss, every other controller through the
cluster read core; `:none` SHALL be answered only when every controller answered. When the
index is at its cap the oldest keys SHALL be evicted and `dedup_evicted_count` incremented.

#### Scenario: A re-run of the parent finds its child

- **WHEN** a parent re-runs a spawn whose identity started a child that has since ended on
  another controller
- **THEN** `seen(ns, {:spawn_identity, identity})` SHALL answer `{:ended, child_id, ts}` or
  `{:started, child_id, ts}` naming that child

#### Scenario: A controller does not answer

- **WHEN** the local index misses and one controller does not answer the lookup
- **THEN** `seen` SHALL answer `{:unknown, [that_node]}` and never `:none`

#### Scenario: The index survives a restart

- **WHEN** a controller restarts
- **THEN** keys of records it wrote inside the dedup window SHALL be found again before it
  accepts its first append

