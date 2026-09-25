# effectively-once-checkpoint-granularity Specification

## Purpose

The effectively-once tier acknowledges an input by making its state durable —
one atomic record per invocation. Operators that fold many inputs into one
state push (a window that only closes on its boundary) cannot use that
granularity coherently: the offset would advance with state they never published,
and a restore would resume past inputs the restored state never saw. This
capability is the opt-in that moves the acknowledgement to where such an
operator's state actually becomes kernel-visible — the checkpoint.
## Requirements
### Requirement: Checkpoint ack-granularity is a per-operator opt-in on the effectively-once tier

An operator with the effectively-once tier enabled SHALL be able to opt into
`checkpoint` ack-granularity (`ChannelBuilder::checkpoint_ack()`). The default
SHALL remain per-input granularity, byte-identical to the tier's existing
behaviour. The declaration SHALL be refused at build time without the tier: off
the tier there is no durable consumed-offset to hold back, and no durable input
log to replay the interval from.

The mode's other precondition — every plugin-bound channel keyed, so the
interval's re-emitted outputs are absorbed by sink dedup — needs no new check:
the tier already refuses an unkeyed plugin-bound channel for every opted-in
operator.

#### Scenario: Default is unchanged

- **WHEN** a tier-enabled operator does not opt into checkpoint granularity
- **THEN** its consumed-offset advances per `run` invocation exactly as before

#### Scenario: Opt-in without the tier is refused

- **WHEN** an operator declares `checkpoint_ack()` without `effectively_once()`
- **THEN** the build fails before the operator starts

#### Scenario: Opt-in without keyed channels is refused

- **WHEN** a checkpoint-ack operator declares an unkeyed plugin-bound channel
- **THEN** the build fails before the operator starts

### Requirement: Consumed-offset advances only at checkpoint boundaries

In checkpoint mode the Cell's durable `consumed_offset` SHALL advance only
when the operator calls `ctx.checkpoint()`. Changes applied between checkpoints
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

- **WHEN** the operator checkpoints after processing offset 4710
- **THEN** one record carrying the accumulated changes and consumed-offset 4710
  is fsynced, and only then does the durable offset advance

### Requirement: Mid-interval outputs flush when they are produced

By default an emit made before the interval closes SHALL be flushed by the
controller as soon as its record is fsynced, in a record carrying NO
consumed-offset. Checkpointing moves the acknowledgement, not the output. An
interval's outputs are therefore emitted again when the interval replays, and
the keyed-sink dedup the tier already requires SHALL absorb them.

A checkpoint-ack operator MAY declare `outputs: :at_checkpoint` with a
`max_delay_ms`: its mid-interval outputs SHALL then ride the interval's
checkpoint record and be flushed as one batch after that record is fsynced,
and the interval SHALL close after `every` inputs or `max_delay_ms` since its
first un-checkpointed input, whichever comes first, so a quiet Cell never
holds an output longer than `max_delay_ms`. Replay semantics SHALL be
unchanged by the option: outputs of an interval that did not checkpoint are
re-produced when the interval replays, outputs of a checkpointed record are
re-driven with identical identity from the cursor, and a conforming keyed sink
absorbs both.

#### Scenario: An emit does not wait for the checkpoint

- **WHEN** a checkpoint-mode invocation emits on a keyed channel and does not
  checkpoint
- **THEN** the output is flushed to the plugin after its record is fsynced, and
  the Cell's durable consumed-offset is unchanged

#### Scenario: Batched outputs flush with the checkpoint record

- **WHEN** an operator declared `outputs: :at_checkpoint` emits 300 outputs
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
  from the checkpointed offset, and the re-produced outputs reach the sink once

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
consumed-offset through the operator in order. The replay SHALL keep the
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
The operator's min-watermark SHALL advance in steps at checkpoint cadence, and
the source-ingestion credit loop SHALL grant accordingly (burst grants at
watermark steps, park in between when credit is exhausted).

#### Scenario: Truncation never crosses a live interval

- **WHEN** a checkpoint-mode Cell's last checkpoint is at offset 4700
- **THEN** input-log truncation removes only offsets strictly below 4700
  regardless of how far the Cell has processed ahead

### Requirement: A held watermark is not read as a stall

The controller's liveness detectors SHALL NOT report a checkpoint-mode operator
as a stalled or dead data-flow merely because it made nothing durable for an
interval. Silence between checkpoints is the mode's steady state.

#### Scenario: A quiet interval does not trip the input-silence sweep

- **WHEN** a checkpoint-mode operator that has produced output before makes
  nothing durable for longer than the input-silence timeout
- **THEN** no `input_silence` is reported for it

### Requirement: The checkpoint opt-in is one seam every SDK exposes, and each option turns the granularity on itself

Every SDK that carries the effectively-once tier SHALL expose the bare checkpoint opt-in on its channel builder, spelled in that language's casing (`checkpoint_ack()` / `checkpointAck()`), so a class whose interval is closed only by an explicit `ctx.checkpoint()` is expressible without declaring a cadence it does not want. Each option that presumes the checkpoint interval — the bare opt-in, the declared cadence, and holding outputs until the interval closes — SHALL set the checkpoint granularity itself rather than requiring a second call, and setting it SHALL be idempotent, so the options compose in any order and any combination and each preserves the others' policy. An SDK SHALL NOT be able to produce a declaration that holds outputs at the checkpoint, or closes the interval on a cadence, while acknowledging per input: the core refuses that declaration at registration, and a builder that can express it hands its author a class that cannot start.

An SDK that carries the tier SHALL prove the opt-in through its own authoring surface rather than by writing the declaration by hand, so the refusal a wrong declaration earns is one an author can actually reach. An SDK that cannot yet carry the tier SHALL defer `effectively_once` by name in its own conformance manifest, naming the follow-up that owes it. The Elixir SDK carries it in its own core: `Flezha.SDK.Channels.checkpoint_ack/1`, `checkpoint_every/2` and `outputs_at_checkpoint/2` route through one private policy amendment that sets the granularity, as the reference's do, and `Flezha.SDK.Topology.build/1` refuses `CheckpointAckWithoutTier` before the operator starts.

#### Scenario: Holding outputs at the checkpoint acknowledges at the checkpoint

- **WHEN** a tier-enabled class declares only that its outputs are held until the interval closes, with a maximum delay
- **THEN** its declaration carries `checkpoint` ack-granularity, and the core registers the class

#### Scenario: The bare opt-in needs no cadence

- **WHEN** a tier-enabled class declares the checkpoint opt-in and no cadence
- **THEN** its declaration carries `checkpoint` ack-granularity and no `checkpoint_every`, and the interval closes only on an explicit `ctx.checkpoint()`

#### Scenario: The options compose in any order

- **WHEN** a class declares the bare opt-in, a cadence, and outputs held at the checkpoint, in any order
- **THEN** the declaration carries all three, and no later call clears an earlier one's policy

#### Scenario: The opt-in does not imply the tier

- **WHEN** a class declares any of the three checkpoint options without declaring effectively-once
- **THEN** the build is refused before the operator starts, naming the missing tier

#### Scenario: An SDK that defers the tier says so in its manifest

- **WHEN** an SDK's channel builder does not carry the checkpoint options
- **THEN** its `conformance.json` lists `effectively_once` under `deferred`, and the conformance gate refuses a manifest that carries the capability without the reference's fixture set

#### Scenario: The Elixir builder's options imply the granularity and compose
- **WHEN** an Elixir class declares `effectively_once/1` then `outputs_at_checkpoint/2` alone, and another declares the three options in any order
- **THEN** both register with `checkpoint` ack-granularity, the first with no cadence and the second carrying all three, and a class declaring any of them without `effectively_once/1` is refused at `Topology.build/1` naming the missing tier

