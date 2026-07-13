# effectively-once-checkpoint-granularity Specification

## Purpose

The effectively-once tier acknowledges an input by making its state durable —
one atomic record per invocation. Operatives that fold many inputs into one
state push (a window that only closes on its boundary) cannot use that
granularity coherently: the offset would advance with state they never published,
and a restore would resume past inputs the restored state never saw. This
capability is the opt-in that moves the acknowledgement to where such an
operative's state actually becomes kernel-visible — the checkpoint.

## Requirements

### Requirement: Checkpoint ack-granularity is a per-operative opt-in on the effectively-once tier

An operative with the effectively-once tier enabled SHALL be able to opt into
`checkpoint` ack-granularity (`ChannelBuilder::checkpoint_ack()`). The default
SHALL remain per-input granularity, byte-identical to the tier's existing
behaviour. The declaration SHALL be refused at build time without the tier: off
the tier there is no durable consumed-offset to hold back, and no durable input
log to replay the interval from.

The mode's other precondition — every plugin-bound channel keyed, so the
interval's re-emitted outputs are absorbed by sink dedup — needs no new check:
the tier already refuses an unkeyed plugin-bound channel for every opted-in
operative.

#### Scenario: Default is unchanged

- **WHEN** a tier-enabled operative does not opt into checkpoint granularity
- **THEN** its consumed-offset advances per `run` invocation exactly as before

#### Scenario: Opt-in without the tier is refused

- **WHEN** an operative declares `checkpoint_ack()` without `effectively_once()`
- **THEN** the build fails before the operative starts

#### Scenario: Opt-in without keyed channels is refused

- **WHEN** a checkpoint-ack operative declares an unkeyed plugin-bound channel
- **THEN** the build fails before the operative starts

### Requirement: Consumed-offset advances only at checkpoint boundaries

In checkpoint mode the Segment's durable `consumed_offset` SHALL advance only
when the operative calls `ctx.checkpoint()`. Changes applied between checkpoints
SHALL be held in the Segment's flush buffer and SHALL NOT be made durable on
their own. The record written at the checkpoint SHALL carry the interval's
accumulated changes and the closing input's consumed-offset as one fsynced unit
— state and offset advance together or not at all.

A checkpoint that accumulated no changes SHALL still write its record: the
record exists to carry the offset.

#### Scenario: Inputs between checkpoints advance nothing durable

- **WHEN** a Segment processes offsets 4701..4710 without checkpointing
- **THEN** its durable consumed-offset remains at the last checkpoint's offset
  4700, and none of the changes those inputs applied are durable

#### Scenario: Checkpoint closes the interval atomically

- **WHEN** the operative checkpoints after processing offset 4710
- **THEN** one record carrying the accumulated changes and consumed-offset 4710
  is fsynced, and only then does the durable offset advance

### Requirement: Mid-interval outputs flush when they are produced

An emit made before the interval closes SHALL be flushed by the controller as
soon as its record is fsynced, in a record carrying NO consumed-offset.
Checkpointing moves the acknowledgement, not the output. An interval's outputs
are therefore emitted again when the interval replays, and the keyed-sink dedup
the tier already requires SHALL absorb them.

#### Scenario: An emit does not wait for the checkpoint

- **WHEN** a checkpoint-mode invocation emits on a keyed channel and does not
  checkpoint
- **THEN** the output is flushed to the plugin after its record is fsynced, and
  the Segment's durable consumed-offset is unchanged

### Requirement: A dead-letter park closes the interval

An invocation that exhausts its retries and parks to the dead-letter queue SHALL
close the interval as if it had checkpointed: its record carries the accumulated
changes, the park, and the consumed-offset together. The poison input is thereby
acknowledged and is never re-forwarded onto the operation by a later interval
replay.

#### Scenario: A parked poison input is not replayed back onto the operation

- **GIVEN** a checkpoint-mode Segment that has processed offsets 4701..4707
  without checkpointing
- **WHEN** offset 4708 exhausts its retries and parks
- **THEN** one record carries the 4701..4708 changes, the DLQ entry, and
  consumed-offset 4708 — and a subsequent recovery replays from 4709, not 4701

### Requirement: Recovery restores the checkpoint and replays the interval

On recovery, a checkpoint-mode Segment SHALL be restored from its durable
records and the controller SHALL re-forward the input log from the restored
consumed-offset through the operative in order. The replay SHALL keep the
current epoch. The Segment SHALL discard any open interval accumulated before
the restore — the restored state does not contain it, and the re-forwarded
inputs produce it again.

#### Scenario: Crash mid-interval replays from the checkpoint

- **GIVEN** a Segment checkpointed at offset 4700 that crashed after processing
  offset 4708
- **WHEN** the Segment is restored
- **THEN** state is restored to the 4700 checkpoint and offsets 4701..4708 (and
  any newer retained inputs) are re-forwarded in order under the same epoch

#### Scenario: State never resumes past inputs it did not see

- **WHEN** a checkpoint-mode Segment is restored at any point in its interval
- **THEN** the offset it resumes from is one the restored state has already
  folded in

### Requirement: Retention holds at least one checkpoint interval and the watermark steps

The input-log retention floor for a checkpoint-mode Segment SHALL be its last
checkpointed offset — at least one full checkpoint interval is always retained.
The operative's min-watermark SHALL advance in steps at checkpoint cadence, and
the source-ingestion credit loop SHALL grant accordingly (burst grants at
watermark steps, park in between when credit is exhausted).

#### Scenario: Truncation never crosses a live interval

- **WHEN** a checkpoint-mode Segment's last checkpoint is at offset 4700
- **THEN** input-log truncation removes only offsets strictly below 4700
  regardless of how far the Segment has processed ahead

### Requirement: A held watermark is not read as a stall

The controller's liveness detectors SHALL NOT report a checkpoint-mode operative
as a stalled or dead data-flow merely because it made nothing durable for an
interval. Silence between checkpoints is the mode's steady state.

#### Scenario: A quiet interval does not trip the dataflow-stop watchdog

- **WHEN** a checkpoint-mode operative that has produced output before makes
  nothing durable for longer than the dataflow-stop timeout
- **THEN** no `catastrophic_dataflow_stop` is reported for it
