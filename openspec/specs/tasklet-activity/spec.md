# tasklet-activity Specification

## Purpose
The per-tasklet activity a runner reports on request, and how a caller reads it.
## Requirements
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

