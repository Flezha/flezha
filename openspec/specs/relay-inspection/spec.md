# relay-inspection Specification

## Purpose

Reading what a running relay is doing. `Relay.diagnose` answers a verdict and catalogued findings
from facts the controller already keeps, for a person or an AI assistant. A runner reports each
tasklet's activity totals, which do not fall when a Cell is evicted, and lists one tasklet's Cells
on request. One named Cell's live in-memory state can be read without a serving tier, bounded in
size and marked with the version it was read at; a Cell that is not resident is refused with its
reason.

## Requirements

### Requirement: `Relay.diagnose` SHALL answer a verdict and findings from existing facts

`Relay.diagnose(namespace, relay_id, caller)` SHALL be a published, remote, non-streaming
catalogue operation judged as `relay.list`, writing no audit record. It SHALL read only the
convergence loop's published verdicts for the relay, its runner and its bound plugins, the
runners announcing the relay's factory, the relay's dead letters, its input log's waiting
inputs and, for a finished relay, its kept completion record or, when its state was not kept,
its newest `ended` record in the relay history, and SHALL answer
`{relay, verdict, findings}` with `verdict` one of `ok`, `warning`, `problem` (the highest finding level, `ok` when only notes) and each finding
`{level, code, message, details, hint}` with `message` and `hint` from the error catalogue. A
relay neither live, nor kept, nor in the relay history SHALL answer `relay.not_found`. A relay its hub holds whose
Overwatch does not answer SHALL be answered from its published verdict, with `live_read` naming
the failed read's error code and its runner, factory and plugins as the hub's registry recorded
them; a runner, node or provider nobody recorded SHALL be absent from the details and named
`unknown` in the message. Such a relay SHALL never be reported `healthy`: a `diagnose.unread`
warning naming the `live_read` code SHALL be among its findings, in place of the healthy note. With no published verdict it SHALL answer the read's error. The dead letters' error texts
SHALL be given only to a caller also allowed `dlq.list` in the namespace, and the input log and
dead letters SHALL be read only when the relay runs on the controller that answers.

#### Scenario: A healthy relay

- **WHEN** a running relay with a connected runner, no dead letters and healthy plugins is
  diagnosed
- **THEN** the verdict is `ok` and the only finding is `diagnose.healthy` naming its runner

#### Scenario: No runner for the factory

- **WHEN** the only runner announcing factory `orders` is gone and `orders-eu` is diagnosed
  after the convergence loop judged it
- **THEN** the verdict is `problem` with `diagnose.no_runner`, whose hint says to start a runner
  that defines `orders`

#### Scenario: Dead letters are summarised

- **WHEN** a caller allowed `dlq.list` diagnoses a relay with three dead letters, all with the
  error `call.timeout`
- **THEN** a `diagnose.dead_letters` warning carries `count: 3`, `errors: ["call.timeout"]` and
  the newest one's age, and its hint names `flz dlq list`

#### Scenario: A viewer sees the count only

- **WHEN** a `viewer` diagnoses a relay with dead letters
- **THEN** the `diagnose.dead_letters` warning carries their count and no error texts, and no
  `:authorization_denied` record is written for `dlq.list`, which the viewer never asked for

#### Scenario: An unhealthy plugin

- **WHEN** a Kafka consumer bound to the relay cannot reach its broker
- **THEN** a `diagnose.plugin_unhealthy` problem names the plugin, its provider and the reason

#### Scenario: A disabled plugin is a note

- **WHEN** a relay bound to a plugin its operator disabled, and otherwise healthy, is diagnosed
- **THEN** the verdict is `ok`, with `diagnose.healthy` and a `diagnose.plugin_disabled` note
  saying the plugin is disabled and its guaranteed outputs are held

#### Scenario: Suspended is not a problem

- **WHEN** a suspended relay that a connected runner can take is diagnosed
- **THEN** the verdict is `ok` with a `diagnose.suspended` note saying it is suspended at a
  committed boundary and the next message wakes it on a runner

#### Scenario: A suspended relay nothing can run is a problem

- **WHEN** a suspended relay is diagnosed and no connected runner on any node announces its
  factory
- **THEN** the verdict is `problem` with `diagnose.no_runner` naming the factory, and no
  `diagnose.suspended` note
- **AND** when runners announce the factory but none can take the relay, the finding is
  `diagnose.runner_mismatch` with the reason

#### Scenario: Unknown relay

- **WHEN** an id that never ran is diagnosed
- **THEN** the call answers `relay.not_found`

#### Scenario: A relay whose Overwatch does not answer

- **WHEN** a relay its hub holds, judged wedged by the convergence loop, is diagnosed and the
  read of its Overwatch times out
- **THEN** the verdict is `problem` with `diagnose.data_plane_wedge`, `live_read` is
  `controller.timeout`, and no message carries a `{` placeholder or `null`

#### Scenario: A healthy verdict is not trusted without the live state

- **WHEN** a relay whose published verdict is healthy is diagnosed and the read of its
  Overwatch times out
- **THEN** the verdict is `warning` with the single finding `diagnose.unread`, whose details
  carry `live_read: controller.timeout`

#### Scenario: A relay its hub no longer holds

- **WHEN** a relay with a published verdict, no longer held by its hub, with no completion
  record and no `ended` record in the relay history, is diagnosed
- **THEN** the call answers `relay.not_found`

#### Scenario: A finished run that kept nothing

- **WHEN** a `fan_trade` instance that ended `error` and kept nothing is diagnosed
- **THEN** the answer carries the `diagnose.finished` finding with the status and error from
  its `ended` record in the relay history

### Requirement: Every verdict reason and state SHALL map to one finding

Each convergence verdict state and reason SHALL map to exactly one `diagnose.*` finding code, and
the suite SHALL fail when a state or reason has none. Every `diagnose.*` code SHALL be in the
error catalogue with a message, and every hint that names a `flz` command SHALL name a verb
the `flz` command table has.

#### Scenario: A new reason without a finding fails the suite

- **WHEN** a verdict reason is added to the judge's relay rules and no finding maps it
- **THEN** the mapping test fails naming the reason

### Requirement: Waiting and stuck inputs SHALL be told apart

When the relay's input log holds inputs above its floor, diagnose SHALL give a
`diagnose.inputs_waiting` note with their count, and SHALL give a `diagnose.inputs_stuck` warning
instead when the watermark has not moved under the same condition the input log's sweep uses to
emit its frozen-watermark event.

#### Scenario: A stuck relay

- **WHEN** a relay's input log holds waiting inputs, its watermark stands where the last sweep
  left it and the log is past the size threshold
- **THEN** diagnose gives `diagnose.inputs_stuck` with the waiting count and the log's size

### Requirement: `flz relay diagnose` SHALL render the findings

`flz relay diagnose <id>` SHALL print one block per finding (level, code, message, then its
hint indented), or one line for `ok`, and with `--json` the answer as data. It SHALL exit 0 when
the diagnosis succeeded, whatever its verdict, and by the error's class when it did not.

#### Scenario: Problem output

- **WHEN** `flz relay diagnose orders-eu` is run while no runner announces `orders`
- **THEN** it prints a `PROBLEM diagnose.no_runner` block with its hint and exits 0

### Requirement: Assistants SHALL reach diagnose as a tool and be told to use it

The operation SHALL be served as the MCP tool `relay_diagnose` on `flz mcp` and `/mcp` and as
`POST /v1/call/Relay.diagnose`, and the knowledge rules SHALL tell an assistant to call it
first when a relay misbehaves.

#### Scenario: An assistant diagnoses

- **WHEN** an MCP client with an `ops` token lists tools
- **THEN** `relay_diagnose` is listed as read-only with the required arguments `namespace` and
  `relay_id`, and calling it returns the findings as structured content

### Requirement: A runner SHALL report a relay's activity per tasklet on request

On a `TaskletActivity` request naming a relay instance it hosts, the runner SHALL answer one row per
tasklet of the instance's class, carrying the number of live Cells, the items queued in their
mailboxes in total and in the deepest one, and the monotonic totals of items admitted, blocked waits,
and items dropped as newest, as oldest and at the Cell cap. A dynamic tasklet's row SHALL also carry
its Cells created and evicted. The answer SHALL carry the runner's monotonic read time and the
instance's sequence number. A figure the runtime does not keep SHALL be absent, never zero. The answer
SHALL be built only from figures the data path already maintains and SHALL add no work per admitted or
dispatched item. A request naming a relay the runner does not host SHALL be refused by name.

#### Scenario: A tasklet's admitted items are reported

- **WHEN** 100 inputs have been admitted to the Cells of tasklet `ohlc` of relay `demo#p0` on a `flz_core` runner and its activity is requested
- **THEN** the row for `ohlc` reports `admitted_total` of 100 and the number of `ohlc` Cells alive

#### Scenario: A backlog is visible

- **WHEN** one Cell of tasklet `book` holds 40 queued items and the others hold none
- **THEN** the row for `book` reports `queued` 40 and `queued_max` 40

#### Scenario: Shedding is visible

- **WHEN** a Cell of tasklet `ticks` whose input channel drops oldest discards 3 items
- **THEN** the row for `ticks` reports `dropped_oldest_total` 3 higher than before

#### Scenario: The answer does not grow with Cells

- **WHEN** a dynamic tasklet holds 50,000 live Cells
- **THEN** the answer still holds one row for that tasklet

#### Scenario: A new instance is told apart

- **WHEN** a relay is rebound between two reads
- **THEN** the second answer carries a different instance sequence number

#### Scenario: An unknown relay is refused

- **WHEN** the request names a relay the runner does not host
- **THEN** the runner refuses it naming the relay

### Requirement: Per-tasklet totals SHALL NOT fall when a Cell is evicted

When a dynamic tasklet evicts a Cell, that Cell's admitted, blocked and dropped totals SHALL remain in
its tasklet's reported totals for the life of the instance. The fold SHALL happen once per evicted
Cell, on the eviction path.

#### Scenario: Eviction keeps the total

- **WHEN** a dynamic Cell that admitted 10 inputs is evicted between two reads and nothing else is admitted
- **THEN** the tasklet's `admitted_total` is the same in both reads and `cells_evicted_total` is one higher

### Requirement: A relay's activity SHALL be readable through the published surface

The controller SHALL expose `Flezha.Service.Relay.activity/4`, taking a namespace, a relay id, options
(`cells_of` and `limit`, both optional) and a caller context, bound to the `relay_info` verb. It SHALL reach the runner hosting the relay through the
relay's Overwatch, off the Overwatch's process, and answer the rows with the read time, the instance
sequence number and the runner id. It SHALL keep nothing between calls and do no work when nobody
calls it. An unbound or suspended relay SHALL be answered with that reason without asking a runner. A
partition group id SHALL be refused as `Relay.cell_state` refuses it. A runner that does not answer
within the read's timeout SHALL be reported as `activity.no_answer` naming the runner, not as an
internal error. The CLI SHALL expose the function as `flz relay activity <relay_id>`, printing one line
per tasklet.

#### Scenario: Reading a bound relay

- **WHEN** a caller holding `relay_info` reads the activity of bound relay `demo#p0`
- **THEN** the answer holds one row per tasklet of `demo#p0`'s class, the runner's read time and the instance sequence number

#### Scenario: Reading a relay that keeps no state

- **WHEN** the relay's class keeps no state
- **THEN** its activity is answered as for any bound relay

#### Scenario: Reading a suspended relay

- **WHEN** the relay is suspended
- **THEN** the answer states that the relay is not resident because it is suspended, and no runner is asked

#### Scenario: A runner that does not answer

- **WHEN** the hosting runner predates the request or does not answer in time
- **THEN** the answer is `activity.no_answer` naming the runner

#### Scenario: The relay keeps serving while the read is in flight

- **WHEN** an activity read waits on a slow runner
- **THEN** the relay's Overwatch keeps handling its other requests

### Requirement: A runner SHALL answer one tasklet's Cells on request

A `TaskletActivity` request MAY name one tasklet in `cells_of`, with an optional `limit` (default 50, clamped to 500). The runner SHALL then also answer that tasklet's Cells:
- **Ordering:** at most `limit` rows, ordered by most recent admission first.
- **Each row:** the Cell's key (absent for an unkeyed Cell), its queued items, its admitted, blocked, dropped-newest and dropped-oldest totals, and its last admission time on the same monotonic clock as `read_at_ms`.
- **Count:** the tasklet's total number of live Cells.

The rows SHALL be built on the same walk that builds the per-tasklet rows. They SHALL come only from figures the data path already maintains, and SHALL add no work per admitted or dispatched item.

A tasklet the instance does not declare SHALL be refused by name. A runtime that keeps no per-Cell counters SHALL leave the Cell answer absent, never empty.

#### Scenario: A static tasklet's Cells are listed

- **WHEN** tasklet `static_entry_tasklet` with keys `a`, `b`, `c` and `d` has admitted 3, 0, 5 and 1 inputs and its Cells are requested
- **THEN** the answer holds four rows keyed `a` to `d` with those admitted totals, ordered by most recent admission, and a Cell count of 4

#### Scenario: A large dynamic tasklet is capped

- **WHEN** a dynamic tasklet holds 50,000 live Cells and its Cells are requested with no limit
- **THEN** the answer holds the 50 most recently admitted Cells and a Cell count of 50,000

#### Scenario: A backlog is visible per Cell

- **WHEN** one Cell of tasklet `book` holds 40 queued items
- **THEN** that Cell's row reports `queued` 40

#### Scenario: An unknown tasklet is refused

- **WHEN** `cells_of` names a tasklet the relay's class does not declare
- **THEN** the runner refuses the request naming the tasklet

#### Scenario: No Cell request, no Cell answer

- **WHEN** a request names no `cells_of`
- **THEN** the answer carries no Cell answer, and the walk does no per-Cell selection

### Requirement: A tasklet's Cells SHALL be readable through the published surface

`Flezha.Service.Relay.activity/4` SHALL accept `cells_of` and `limit` options and pass them to the runner. It SHALL answer the Cell rows and the Cell count beside the per-tasklet rows, or no Cell answer when the runner gives none. `flz relay activity <relay_id> --cells <tasklet> [--limit N]` SHALL print one line per Cell after the per-tasklet lines.

#### Scenario: Reading a tasklet's Cells

- **WHEN** a caller holding `relay_info` reads the activity of bound relay `testnet_chain` with `cells_of: "static_entry_tasklet"`
- **THEN** the answer holds the per-tasklet rows and the four Cells of `static_entry_tasklet`

#### Scenario: An older runner

- **WHEN** the hosting runner predates the field
- **THEN** the answer holds the per-tasklet rows and no Cell answer, not an error

### Requirement: One named Cell's live state SHALL be readable without a serving tier

The controller SHALL expose `Flezha.Service.CliManager.state_get/2` taking a
`<relay/tasklet/cell>` path, resolving the runner holding that Relay,
and returning the named Cell's current in-memory state as JSON together with the
`change_seq` it was serialized at. The read SHALL travel the existing control
request/reply seam and SHALL be authorized through the same `AuthZ.authorize/3`
seam as the other `state_*` verbs. The CLI SHALL expose it as
`flz state get <relay/tasklet/cell>`.

#### Scenario: A single Cell is read by name

- **WHEN** a user runs `flz state get ohlc/candle/BTCUSD@1m` against a Relay whose `candle` Tasklet holds a live Cell keyed `BTCUSD@1m`
- **THEN** the CLI SHALL print that Cell's state as JSON and its `change_seq`, and SHALL NOT serialize any other Cell

#### Scenario: A dynamic-mode Cell is readable

- **WHEN** the named Cell was created lazily by a key function rather than declared in a static keyset
- **THEN** the read SHALL succeed identically to a static-mode Cell

#### Scenario: A Cell key without a Tasklet is refused

- **WHEN** a caller supplies a Cell key but no Tasklet name
- **THEN** the read SHALL be refused with an error stating that a Cell key is unique only within its Tasklet

### Requirement: A live read SHALL return the freshest value and SHALL identify its version

The value returned SHALL be the Cell's in-memory state, which includes changes
applied but not yet flushed to the WAL, and SHALL therefore be at or ahead of the
durable state. The reply SHALL carry the `change_seq` at which the state was
serialized, on the frame metadata rather than inside the JSON body. A read SHALL
NOT drain pending changes and SHALL NOT trigger compaction, a snapshot, or a WAL
truncation.

#### Scenario: A read reflects an unflushed change

- **WHEN** a Tasklet applies a change and the flush interval has not yet elapsed, and the Cell is then read
- **THEN** the returned state SHALL include that change and the reported `change_seq` SHALL be greater than the Cell's last persisted seq

#### Scenario: A read has no durable side effects

- **WHEN** a Cell is read
- **THEN** its WAL byte length, snapshot count, and `snapshot_seq` SHALL be unchanged, and no `compaction_trigger` SHALL be emitted

#### Scenario: Two reads separated by work report different seqs

- **WHEN** a Cell is read, then processes an input, then is read again
- **THEN** the second reply's `change_seq` SHALL be greater than the first's

### Requirement: A non-resident Cell SHALL be refused with its reason

A Cell that is not held in memory SHALL NOT be served from the durable store.
The read SHALL return a refusal naming which of the following applies: the Cell
was idle-evicted, the instance is suspended, the Relay holds no Cell by that
key, or the Relay is not bound to a runner. The refusal SHALL name
`flz state at` as the durable-read path.

#### Scenario: An evicted Cell is refused, not resurrected

- **WHEN** a dynamic-mode Cell has been idle-evicted and is then read
- **THEN** the read SHALL be refused naming eviction, and SHALL NOT create a Cell, bind a runner, or restore state from disk

#### Scenario: An unbound Relay is refused

- **WHEN** the target Relay is not currently bound to a runner
- **THEN** the read SHALL be refused naming the unbound Relay rather than blocking until a bind occurs

#### Scenario: An unknown key is refused distinctly from an evicted one

- **WHEN** a key that never had a Cell is read
- **THEN** the refusal SHALL state that no Cell by that key exists, distinguishably from the eviction refusal

### Requirement: A live read SHALL be bounded in size

The serialized reply SHALL be bounded by the configured
`:wal_high_watermark_bytes`. A reply exceeding it SHALL be refused with
`{:error, {:oversized_state, bytes}}` naming the selector that would narrow the
read. No new configuration key SHALL be introduced for this bound.

#### Scenario: An unselected read of a large Relay is refused with guidance

- **WHEN** a whole-Relay read would serialize more than the watermark
- **THEN** the read SHALL be refused reporting the byte count and naming the Tasklet/Cell selector that would narrow it

#### Scenario: The narrowed read of the same Relay succeeds

- **WHEN** the same Relay is read with a Tasklet and Cell selector whose serialized state is under the watermark
- **THEN** the read SHALL succeed
