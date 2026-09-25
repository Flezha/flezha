# cell-state-live-query Specification

## Purpose

The cell-state-live-query capability makes the current in-memory state of one
named Cell readable without a serving tier. The controller asks the runner
holding the Operator through the existing `CellState` control round-trip,
narrowed by a `tasklet` / `cell_key` selector, and renders the returned JSON
together with the `change_seq` it was serialized at. The value is the freshest
one there is — it includes changes applied but not yet flushed — so a reader is
always told which version it got. A Cell that is not held in memory is
refused naming why; `flz state at` remains the durable-read path.

## Requirements

### Requirement: One named Cell's live state SHALL be readable without a serving tier

The controller SHALL expose `Flezha.Service.CliManager.state_get/2` taking an
`<operator/tasklet/cell>` path, resolving the runner holding that Operator,
and returning the named Cell's current in-memory state as JSON together with the
`change_seq` it was serialized at. The read SHALL travel the existing control
request/reply seam and SHALL be authorized through the same `AuthZ.authorize/3`
seam as the other `state_*` verbs. The CLI SHALL expose it as
`flz state get <operator/tasklet/cell>`.

#### Scenario: A single Cell is read by name

- **WHEN** a user runs `flz state get ohlc/candle/BTCUSD@1m` against an Operator whose `candle` Tasklet holds a live Cell keyed `BTCUSD@1m`
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
was idle-evicted, the instance is suspended, the Operator holds no Cell by that
key, or the Operator is not bound to a runner. The refusal SHALL name
`flz state at` as the durable-read path.

#### Scenario: An evicted Cell is refused, not resurrected

- **WHEN** a dynamic-mode Cell has been idle-evicted and is then read
- **THEN** the read SHALL be refused naming eviction, and SHALL NOT create a Cell, bind a runner, or restore state from disk

#### Scenario: An unbound Operator is refused

- **WHEN** the target Operator is not currently bound to a runner
- **THEN** the read SHALL be refused naming the unbound Operator rather than blocking until a bind occurs

#### Scenario: An unknown key is refused distinctly from an evicted one

- **WHEN** a key that never had a Cell is read
- **THEN** the refusal SHALL state that no Cell by that key exists, distinguishably from the eviction refusal

### Requirement: A live read SHALL be bounded in size

The serialized reply SHALL be bounded by the configured
`:wal_high_watermark_bytes`. A reply exceeding it SHALL be refused with
`{:error, {:oversized_state, bytes}}` naming the selector that would narrow the
read. No new configuration key SHALL be introduced for this bound.

#### Scenario: An unselected read of a large Operator is refused with guidance

- **WHEN** a whole-Operator read would serialize more than the watermark
- **THEN** the read SHALL be refused reporting the byte count and naming the Tasklet/Cell selector that would narrow it

#### Scenario: The narrowed read of the same Operator succeeds

- **WHEN** the same Operator is read with a Tasklet and Cell selector whose serialized state is under the watermark
- **THEN** the read SHALL succeed
