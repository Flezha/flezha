# entity-state-live-query Specification

## Purpose

The entity-state-live-query capability makes the current in-memory state of one
named Entity readable without a serving tier. The controller asks the runner
holding the Operative through the existing `EntityState` control round-trip,
narrowed by a `procedure` / `entity_key` selector, and renders the returned JSON
together with the `change_seq` it was serialized at. The value is the freshest
one there is — it includes changes applied but not yet flushed — so a reader is
always told which version it got. An Entity that is not held in memory is
refused naming why; `flz state at` remains the durable-read path.

## Requirements

### Requirement: One named Entity's live state SHALL be readable without a serving tier

The controller SHALL expose `Flezha.Service.CliManager.state_get/2` taking an
`<operative/procedure/entity>` path, resolving the runner holding that Operative,
and returning the named Entity's current in-memory state as JSON together with the
`change_seq` it was serialized at. The read SHALL travel the existing control
request/reply seam and SHALL be authorized through the same `AuthZ.authorize/3`
seam as the other `state_*` verbs. The CLI SHALL expose it as
`flz state get <operative/procedure/entity>`.

#### Scenario: A single Entity is read by name

- **WHEN** an operator runs `flz state get ohlc/candle/BTCUSD@1m` against an Operative whose `candle` Procedure holds a live Entity keyed `BTCUSD@1m`
- **THEN** the CLI SHALL print that Entity's state as JSON and its `change_seq`, and SHALL NOT serialize any other Entity

#### Scenario: A dynamic-mode Entity is readable

- **WHEN** the named Entity was created lazily by a key function rather than declared in a static keyset
- **THEN** the read SHALL succeed identically to a static-mode Entity

#### Scenario: An Entity key without a Procedure is refused

- **WHEN** a caller supplies an Entity key but no Procedure name
- **THEN** the read SHALL be refused with an error stating that an Entity key is unique only within its Procedure

### Requirement: A live read SHALL return the freshest value and SHALL identify its version

The value returned SHALL be the Entity's in-memory state, which includes changes
applied but not yet flushed to the WAL, and SHALL therefore be at or ahead of the
durable state. The reply SHALL carry the `change_seq` at which the state was
serialized, on the frame metadata rather than inside the JSON body. A read SHALL
NOT drain pending changes and SHALL NOT trigger compaction, a snapshot, or a WAL
truncation.

#### Scenario: A read reflects an unflushed change

- **WHEN** a Procedure applies a change and the flush interval has not yet elapsed, and the Entity is then read
- **THEN** the returned state SHALL include that change and the reported `change_seq` SHALL be greater than the Entity's last persisted seq

#### Scenario: A read has no durable side effects

- **WHEN** an Entity is read
- **THEN** its WAL byte length, snapshot count, and `snapshot_seq` SHALL be unchanged, and no `compaction_trigger` SHALL be emitted

#### Scenario: Two reads separated by work report different seqs

- **WHEN** an Entity is read, then processes an input, then is read again
- **THEN** the second reply's `change_seq` SHALL be greater than the first's

### Requirement: A non-resident Entity SHALL be refused with its reason

An Entity that is not held in memory SHALL NOT be served from the durable store.
The read SHALL return a refusal naming which of the following applies: the Entity
was idle-evicted, the instance is suspended, the Operative holds no Entity by that
key, or the Operative is not bound to a runner. The refusal SHALL name
`flz state at` as the durable-read path.

#### Scenario: An evicted Entity is refused, not resurrected

- **WHEN** a dynamic-mode Entity has been idle-evicted and is then read
- **THEN** the read SHALL be refused naming eviction, and SHALL NOT create an Entity, bind a runner, or restore state from disk

#### Scenario: An unbound Operative is refused

- **WHEN** the target Operative is not currently bound to a runner
- **THEN** the read SHALL be refused naming the unbound Operative rather than blocking until a bind occurs

#### Scenario: An unknown key is refused distinctly from an evicted one

- **WHEN** a key that never had an Entity is read
- **THEN** the refusal SHALL state that no Entity by that key exists, distinguishably from the eviction refusal

### Requirement: A live read SHALL be bounded in size

The serialized reply SHALL be bounded by the configured
`:wal_high_watermark_bytes`. A reply exceeding it SHALL be refused with
`{:error, {:oversized_state, bytes}}` naming the selector that would narrow the
read. No new configuration key SHALL be introduced for this bound.

#### Scenario: An unselected read of a large Operative is refused with guidance

- **WHEN** a whole-Operative read would serialize more than the watermark
- **THEN** the read SHALL be refused reporting the byte count and naming the Procedure/Entity selector that would narrow it

#### Scenario: The narrowed read of the same Operative succeeds

- **WHEN** the same Operative is read with a Procedure and Entity selector whose serialized state is under the watermark
- **THEN** the read SHALL succeed
