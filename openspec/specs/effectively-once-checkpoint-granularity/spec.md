# effectively-once-checkpoint-granularity Specification

## Purpose

The effectively-once tier acknowledges an input by making its state durable —
one atomic record per invocation. Relays that fold many inputs into one
state push (a window that only closes on its boundary) cannot use that
granularity coherently: the offset would advance with state they never published,
and a restore would resume past inputs the restored state never saw. This
capability is the opt-in that moves the acknowledgement to where such a
relay's state actually becomes kernel-visible — the checkpoint.
## Requirements
### Requirement: Consumed-offset advances only at checkpoint boundaries

In checkpoint mode the Cell's durable `consumed_offset` SHALL advance only
when the relay calls `ctx.checkpoint()`. Changes applied between checkpoints
SHALL be held in the Cell's flush buffer and SHALL NOT be made durable on
their own. The record written at the checkpoint SHALL carry the interval's
accumulated changes and the closing input's consumed-offset as one fsynced unit
— state and offset advance together or not at all.

A checkpoint that accumulated no changes SHALL still write its record: the
record exists to carry the offset.

#### Scenario: Inputs between checkpoints advance nothing durable

- **WHEN** a Cell processes offsets 4701..4710 without checkpointing
- **THEN** its durable consumed-offset remains at the last checkpoint's offset
  4700, and none of the changes those inputs applied are durable

#### Scenario: Checkpoint closes the interval atomically

- **WHEN** the relay checkpoints after processing offset 4710
- **THEN** one record carrying the accumulated changes and consumed-offset 4710
  is fsynced, and only then does the durable offset advance

### Requirement: Mid-interval outputs flush when they are produced

By default an emit made before the interval closes SHALL be flushed by the
controller as soon as its record is fsynced, in a record carrying NO
consumed-offset. Checkpointing moves the acknowledgement, not the output. An
interval's outputs are therefore emitted again when the interval replays, and
the keyed-output plugin dedup the tier already requires SHALL absorb them.

A checkpoint-ack relay MAY declare `outputs: :at_checkpoint` with a
`max_delay_ms`: its mid-interval outputs SHALL then ride the interval's
checkpoint record and be flushed as one batch after that record is fsynced,
and the interval SHALL close after `every` inputs or `max_delay_ms` since its
first un-checkpointed input, whichever comes first, so a quiet Cell never
holds an output longer than `max_delay_ms`. Replay semantics SHALL be
unchanged by the option: outputs of an interval that did not checkpoint are
re-produced when the interval replays, outputs of a checkpointed record are
re-driven with identical identity from the cursor, and a conforming keyed output plugin
absorbs both.

#### Scenario: An emit does not wait for the checkpoint

- **WHEN** a checkpoint-mode invocation emits on a keyed channel and does not
  checkpoint
- **THEN** the output is flushed to the plugin after its record is fsynced, and
  the Cell's durable consumed-offset is unchanged

#### Scenario: Batched outputs flush with the checkpoint record

- **WHEN** a relay declared `outputs: :at_checkpoint` emits 300 outputs
  across an interval of 500 inputs
- **THEN** no output reaches a plugin before the checkpoint, and after the
  checkpoint record is fsynced all 300 are handed to the chain as one batch
  with identities `<record seq>.0` … `<record seq>.299`

#### Scenario: A quiet Cell's outputs flush within the delay

- **WHEN** an `outputs: :at_checkpoint` Cell with `max_delay_ms: 50` emits
  once and receives no further input
- **THEN** its interval closes within 50 ms and the output is flushed, with the
  consumed-offset acknowledged at that close

#### Scenario: A crash before the checkpoint loses nothing and duplicates nothing durable

- **WHEN** an `outputs: :at_checkpoint` Cell crashes after emitting but
  before its interval checkpoints
- **THEN** no output of that interval reached a plugin, the interval replays
  from the checkpointed offset, and the re-produced outputs reach the output plugin once

### Requirement: A dead-letter park closes the interval

An invocation that exhausts its retries and parks to the dead-letter queue SHALL
close the interval as if it had checkpointed: its record carries the accumulated
changes, the park, and the consumed-offset together. The poison input is thereby
acknowledged and is never re-forwarded onto the tasklet by a later interval
replay.

#### Scenario: A parked poison input is not replayed back onto the tasklet

- **GIVEN** a checkpoint-mode Cell that has processed offsets 4701..4707
  without checkpointing
- **WHEN** offset 4708 exhausts its retries and parks
- **THEN** one record carries the 4701..4708 changes, the DLQ entry, and
  consumed-offset 4708 — and a subsequent recovery replays from 4709, not 4701

### Requirement: Recovery restores the checkpoint and replays the interval

On recovery, a checkpoint-mode Cell SHALL be restored from its durable
records and the controller SHALL re-forward the input log from the restored
consumed-offset through the relay in order. The replay SHALL keep the
current epoch. The Cell SHALL discard any open interval accumulated before
the restore — the restored state does not contain it, and the re-forwarded
inputs produce it again.

#### Scenario: Crash mid-interval replays from the checkpoint

- **GIVEN** a Cell checkpointed at offset 4700 that crashed after processing
  offset 4708
- **WHEN** the Cell is restored
- **THEN** state is restored to the 4700 checkpoint and offsets 4701..4708 (and
  any newer retained inputs) are re-forwarded in order under the same epoch

#### Scenario: State never resumes past inputs it did not see

- **WHEN** a checkpoint-mode Cell is restored at any point in its interval
- **THEN** the offset it resumes from is one the restored state has already
  folded in

### Requirement: Retention holds at least one checkpoint interval and the watermark steps

The input-log retention floor for a checkpoint-mode Cell SHALL be its last
checkpointed offset — at least one full checkpoint interval is always retained.
The relay's min-watermark SHALL advance in steps at checkpoint cadence, and
the source-ingestion credit loop SHALL grant accordingly (burst grants at
watermark steps, park in between when credit is exhausted).

#### Scenario: Truncation never crosses a live interval

- **WHEN** a checkpoint-mode Cell's last checkpoint is at offset 4700
- **THEN** input-log truncation removes only offsets strictly below 4700
  regardless of how far the Cell has processed ahead

### Requirement: A held watermark is not read as a stall

The controller's liveness detectors SHALL NOT report a checkpoint-mode relay
as a stalled or dead data-flow merely because it made nothing durable for an
interval. Silence between checkpoints is the mode's steady state.

#### Scenario: A quiet interval does not trip the input-silence sweep

- **WHEN** a checkpoint-mode relay that has produced output before makes
  nothing durable for longer than the input-silence timeout
- **THEN** no `input_silence` is reported for it

### Requirement: Checkpoint acknowledgement is the `ack` setting under `effectively_once`

`effectively_once` SHALL carry `ack`, one of `per_input` (the default) and `checkpoint`
with `every` (a positive count) and `max_delay_ms` (a positive delay), and the checkpoint
behaviour SHALL be what the retired requirement described: the consumed offset advances at
checkpoint boundaries, mid-interval outputs flush when produced, a park closes the interval,
recovery replays the interval. `ack` SHALL be writable nowhere else. A zero `every` or
`max_delay_ms` SHALL stay refused by the existing value codes.

#### Scenario: A checkpointing flow class

- **WHEN** a flow class declares `exactly: {effectively_once, ack: {checkpoint, every: 100, max_delay_ms: 1000}}`
- **THEN** its consumed offset SHALL advance only at `ctx.checkpoint()` or at the hundredth input or after a second, whichever the retired requirement's rules reach first, and a crash SHALL replay from the last checkpoint

#### Scenario: `ack` cannot be written under `at_least_once` or under `steps`

- **WHEN** a class writes `ack` under `exactly: at_least_once`, or under `run: steps`
- **THEN** the SDK SHALL fail to compile where its language can and otherwise refuse registration with `topology.unknown_field` naming `ack` and its parent

