//! What one invocation ships: [`settle_effects`], the one settlement rule the
//! typed dispatcher and the erased host both call, and [`EffectBuffer`], the
//! one per-Cell buffer it settles into, generic over the change type
//! through a [`Bodies`] marker. The invariant: a park rides the same record
//! as the consumed-offset advance it acknowledges, and nothing in an open
//! checkpoint interval is flushable on its own.

use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::flusher::{build_effect_record_frame, frame_change_bodies, FlushFrame, FlushableBuffer};
use crate::header::HeaderTemplate;
use crate::operator::topology::{CheckpointOutputs, CheckpointPolicy};
/// A park inside an effect record: it rides the SAME record as the
/// consumed-offset advance. One definition — the boundary schema's.
pub use crate::schema::DlqEntry as DlqEntryBody;
use crate::schema::TimerOp;

/// One plugin output inside an effect record. Field order is the wire
/// contract; `metadata` is the caller's, key-sorted — the controller stamps
/// the identity keys at flush time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRecord {
    pub channel: String,
    pub key: String,
    pub metadata: Vec<(String, String)>,
    pub payload: Vec<u8>,
}

/// Wire body of a `cell_effect_record` frame (`format_id = 2`): one
/// invocation's changes, outputs, consumed offset, park and timer ops, ONE
/// WAL record under ONE fsync. `event_time` is the declared clock's value —
/// the correction baseline's lookup key; a firing's removal rides beside its
/// effects, which is what makes it exactly-once under the tier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectRecordBody {
    pub changes: Vec<Vec<u8>>,
    pub outputs: Vec<OutputRecord>,
    pub consumed_offset: Option<u64>,
    pub dlq: Option<DlqEntryBody>,
    pub event_time: u64,
    pub timers: Vec<crate::schema::TimerOp>,
}

/// One invocation's buffered effect entry, on the class's own change type,
/// serialized into an [`EffectRecordBody`] at drain.
pub struct EffectEntry<C> {
    pub changes: Vec<C>,
    pub outputs: Vec<OutputRecord>,
    pub consumed_offset: Option<u64>,
    pub dlq: Option<DlqEntryBody>,
    pub event_time: u64,
    pub timers: Vec<TimerOp>,
}

impl<C> EffectEntry<C> {
    /// The record a fold or a deadline close produced, plus its park.
    pub fn from_shipped(record: ShippedRecord<C>, dlq: Option<DlqEntryBody>) -> Self {
        Self {
            changes: record.changes,
            outputs: record.outputs,
            consumed_offset: record.offset,
            dlq,
            event_time: record.event_time,
            timers: record.timers,
        }
    }

    pub fn into_body<B: Bodies<Change = C>>(self) -> EffectRecordBody {
        EffectRecordBody {
            changes: B::into_bodies(self.changes),
            outputs: self.outputs,
            consumed_offset: self.consumed_offset,
            dlq: self.dlq,
            event_time: self.event_time,
            timers: self.timers,
        }
    }
}

/// What one invocation produced, before the ack granularity decides how
/// much of it ships now.
pub struct InvocationEffects<C> {
    pub changes: Vec<C>,
    pub timers: Vec<TimerOp>,
    pub outputs: Vec<OutputRecord>,
    pub offset: Option<u64>,
    pub event_time: u64,
    /// The body called `ctx.checkpoint()`; meaningless outside that granularity.
    pub checkpointed: bool,
    /// The invocation parked: its record MUST ship, atomic with the offset
    /// advance, and under the checkpoint granularity it closes the interval so
    /// the parked input never replays.
    pub parked: bool,
}

pub struct ShippedRecord<C> {
    pub changes: Vec<C>,
    pub timers: Vec<TimerOp>,
    pub outputs: Vec<OutputRecord>,
    pub offset: Option<u64>,
    pub event_time: u64,
}

/// One open checkpoint interval's accumulation. Nothing here is flushable
/// on its own: it ships in the record that closes the interval, or a restore
/// discards it together.
pub struct StagedInterval<C> {
    pub changes: Vec<C>,
    pub timers: Vec<TimerOp>,
    /// Held only under [`CheckpointOutputs::AtCheckpoint`].
    pub outputs: Vec<OutputRecord>,
    pub offset: Option<u64>,
    pub inputs: u32,
    /// `None` exactly when the interval is closed.
    pub opened_at_ms: Option<u64>,
    /// The last folded invocation's clock value, which a deadline close
    /// carries too.
    pub event_time: u64,
}

impl<C> Default for StagedInterval<C> {
    fn default() -> Self {
        Self {
            changes: Vec::new(),
            timers: Vec::new(),
            outputs: Vec::new(),
            offset: None,
            inputs: 0,
            opened_at_ms: None,
            event_time: 0,
        }
    }
}

impl<C> StagedInterval<C> {
    /// Fold one invocation in and answer what its record ships, or `None`
    /// when an open interval swallowed it. The one place the ack
    /// granularity's rules live.
    pub fn fold(
        &mut self,
        policy: Option<CheckpointPolicy>,
        effects: InvocationEffects<C>,
        now_ms: u64,
    ) -> Option<ShippedRecord<C>> {
        let InvocationEffects {
            changes,
            timers,
            outputs,
            offset,
            event_time,
            checkpointed,
            parked,
        } = effects;

        let Some(policy) = policy else {
            // Per-input: one record per effectful invocation, each
            // acknowledging its own input. An effect-free invocation
            // contributes nothing durable unless it parked.
            let ship = parked || !(changes.is_empty() && outputs.is_empty() && timers.is_empty());
            return ship.then_some(ShippedRecord {
                changes,
                timers,
                outputs,
                offset,
                event_time,
            });
        };

        self.event_time = event_time;

        self.changes.extend(changes);
        self.timers.extend(timers);
        if offset.is_some() {
            self.offset = offset;
            self.inputs += 1;
            self.opened_at_ms.get_or_insert(now_ms);
        }

        // Held outputs wait for the close; as-produced ones leave now.
        let flushed_now = if policy.outputs == CheckpointOutputs::AtCheckpoint {
            self.outputs.extend(outputs);
            Vec::new()
        } else {
            outputs
        };

        if checkpointed || parked || self.due(policy, now_ms) {
            // Even a close that accumulated nothing ships: the record exists
            // to carry the offset.
            let (changes, timers, mut outputs, offset) = self.close();
            outputs.extend(flushed_now);
            return Some(ShippedRecord {
                changes,
                timers,
                outputs,
                offset,
                event_time: self.event_time,
            });
        }

        // Mid-interval: an as-produced emit still flushes, but in a record
        // acknowledging nothing — the state it was computed from is not
        // durable yet, so a record carrying its offset would let a restore
        // resume past inputs it never saw.
        (!flushed_now.is_empty()).then(|| ShippedRecord {
            changes: Vec::new(),
            timers: Vec::new(),
            outputs: flushed_now,
            offset: None,
            event_time,
        })
    }

    /// Due to close on its own: the input count reached, or the delay passed.
    pub fn due(&self, policy: CheckpointPolicy, now_ms: u64) -> bool {
        policy.every.is_some_and(|every| self.inputs >= every) || self.delay_elapsed(policy, now_ms)
    }

    /// Whether `max_delay_ms` passed — what the deferred wake asks.
    pub fn delay_elapsed(&self, policy: CheckpointPolicy, now_ms: u64) -> bool {
        match (policy.max_delay_ms, self.opened_at_ms) {
            (Some(delay), Some(opened)) => now_ms.saturating_sub(opened) >= delay,
            _ => false,
        }
    }

    pub fn deadline_ms(&self, policy: CheckpointPolicy) -> Option<u64> {
        Some(self.opened_at_ms? + policy.max_delay_ms?)
    }

    pub fn close(&mut self) -> (Vec<C>, Vec<TimerOp>, Vec<OutputRecord>, Option<u64>) {
        self.inputs = 0;
        self.opened_at_ms = None;
        (
            std::mem::take(&mut self.changes),
            std::mem::take(&mut self.timers),
            std::mem::take(&mut self.outputs),
            self.offset.take(),
        )
    }

    pub fn is_open(&self) -> bool {
        self.opened_at_ms.is_some()
    }

    /// Close on the delay, with no invocation of its own; `None` when nothing
    /// was open.
    pub fn close_on_deadline(&mut self) -> Option<ShippedRecord<C>> {
        self.is_open().then(|| {
            let (changes, timers, outputs, offset) = self.close();
            ShippedRecord {
                changes,
                timers,
                outputs,
                offset,
                event_time: self.event_time,
            }
        })
    }

    /// Drop the interval: a restore re-seats the Cell on a past in which
    /// none of it was applied.
    pub fn clear(&mut self) {
        let _ = self.close();
        self.offset = None;
    }
}

/// How a class's buffered changes become record bodies, and whether a drain
/// coalesces them first. A marker per class kind, not a trait on the change:
/// the typed SDK's changes are its authors' own types.
pub trait Bodies: Send + 'static {
    type Change: Send + 'static;
    fn coalesce(changes: Vec<Self::Change>) -> Vec<Self::Change>;
    fn into_bodies(changes: Vec<Self::Change>) -> Vec<Vec<u8>>;
}

/// The host's pre-encoded bytes: both steps are a move.
pub struct RawBodies;

impl Bodies for RawBodies {
    type Change = Vec<u8>;

    fn coalesce(changes: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        changes
    }

    fn into_bodies(changes: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        changes
    }
}

/// The per-Cell flush buffer. Exactly one of `changes` / `effects` is ever
/// populated: plain changes outside the effectively-once tier, effect entries
/// inside it. `timer_ops` is the tier-off home for timer mutations (their own
/// `format_id = 3` record); under the tier they ride `EffectEntry::timers`.
pub struct EffectBuffer<B: Bodies> {
    pub changes: Vec<B::Change>,
    /// An open checkpoint interval's accumulation, flushable only as the
    /// record that closes it.
    pub staged: StagedInterval<B::Change>,
    pub effects: Vec<EffectEntry<B::Change>>,
    pub timer_ops: Vec<TimerOp>,
    /// Changes a failed-but-retryable attempt applied, held from the flusher
    /// until the item resolves so durable state cannot diverge from memory.
    pub pending_attempt: Vec<B::Change>,
}

impl<B: Bodies> Default for EffectBuffer<B> {
    fn default() -> Self {
        Self {
            changes: Vec::new(),
            staged: StagedInterval::default(),
            effects: Vec::new(),
            timer_ops: Vec::new(),
            pending_attempt: Vec::new(),
        }
    }
}

impl<B: Bodies> EffectBuffer<B> {
    /// Close an open interval on its delay; `false` when nothing was open.
    pub fn close_interval(&mut self) -> bool {
        let Some(record) = self.staged.close_on_deadline() else {
            return false;
        };
        self.effects.push(EffectEntry::from_shipped(record, None));
        true
    }
}

impl<B: Bodies> FlushableBuffer for EffectBuffer<B> {
    fn drain_to_frames(
        &mut self,
        cell_identity: &[u8],
        change_seq: &AtomicU64,
        header: &HeaderTemplate,
    ) -> (usize, Vec<FlushFrame>) {
        let bodies = B::into_bodies(B::coalesce(std::mem::take(&mut self.changes)));
        // Timer ops fold into the pending set, never into the state, so their
        // order relative to the change records is immaterial — only their
        // order among themselves is, and that is the Vec's.
        let mut frames = frame_change_bodies(
            bodies,
            std::mem::take(&mut self.timer_ops),
            cell_identity,
            change_seq,
            header,
        );
        // Effect records are never coalesced or merged: a coalesced-away
        // record would drop its outputs and consumed-offset.
        for entry in std::mem::take(&mut self.effects) {
            let seq = change_seq.fetch_add(1, Ordering::Relaxed);
            frames.push(FlushFrame {
                payload_id: crate::state_sink::CELL_EFFECT_RECORD_PAYLOAD_ID,
                bytes: build_effect_record_frame(
                    cell_identity,
                    seq,
                    header,
                    &entry.into_body::<B>(),
                ),
            });
        }
        (frames.len(), frames)
    }

    fn is_empty(&self) -> bool {
        self.changes.is_empty() && self.effects.is_empty() && self.timer_ops.is_empty()
    }

    fn discard_staged(&mut self) {
        self.staged.clear();
        // A mid-ladder attempt's changes belong to the same superseded past.
        self.pending_attempt.clear();
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SettlePolicy {
    /// One effect record per invocation: the effectively-once tier, or a
    /// durable bounded operator Cell ([`OperatorTopology::records_effects`]).
    ///
    /// [`OperatorTopology::records_effects`]: crate::operator::topology::OperatorTopology::records_effects
    pub records_effects: bool,
    pub checkpoint: Option<CheckpointPolicy>,
    /// Whether tier-off changes and timer ops are kept for a drain at all —
    /// false for a host class with no flusher entry.
    pub buffered: bool,
}

/// Settle one invocation into its Cell's buffer per its policy: a Cell that
/// records effects folds everything into ONE effect record, park included;
/// any other keeps the changes and timer ops for the flusher and hands the
/// park back for the caller's blocking tier-off park. A retryable attempt's
/// held changes are re-taken first.
pub fn settle_effects<B: Bodies>(
    buffer: &mut EffectBuffer<B>,
    policy: SettlePolicy,
    mut effects: InvocationEffects<B::Change>,
    dlq: Option<DlqEntryBody>,
) -> Option<DlqEntryBody> {
    let mut changes = std::mem::take(&mut buffer.pending_attempt);
    if changes.is_empty() {
        changes = effects.changes;
    } else {
        changes.extend(effects.changes);
    }
    effects.changes = changes;
    if policy.records_effects {
        // An effect-free invocation contributes nothing durable — unless it
        // parked, in which case the record MUST ship so the dead-letter park
        // and the consumed-offset advance stay atomic.
        effects.parked = dlq.is_some();
        if let Some(record) =
            buffer
                .staged
                .fold(policy.checkpoint, effects, crate::util::wall_now_ms())
        {
            buffer.effects.push(EffectEntry::from_shipped(record, dlq));
        }
        return None;
    }
    if policy.buffered {
        if buffer.changes.is_empty() {
            buffer.changes = effects.changes;
        } else {
            buffer.changes.extend(effects.changes);
        }
        buffer.timer_ops.extend(effects.timers);
    }
    dlq
}

#[cfg(test)]
mod effect_buffer_tests {
    use super::*;
    use crate::flusher::encode_cell_identity;

    /// The host's bodies are already bytes: the drain moves them, the same
    /// allocation going in and coming out.
    #[test]
    fn a_bytes_buffer_drains_its_bodies_by_move() {
        let bodies: Vec<Vec<u8>> = (0..64u8).map(|i| vec![i, i, i]).collect();
        let allocation = bodies.as_ptr();
        let coalesced = RawBodies::coalesce(bodies);
        assert_eq!(coalesced.as_ptr(), allocation, "coalesce is a move");
        let out = RawBodies::into_bodies(coalesced);
        assert_eq!(out.as_ptr(), allocation, "into_bodies is a move");
        assert_eq!(out.len(), 64);
    }

    /// The generic drain frames exactly as `frame_change_bodies` does.
    #[test]
    fn a_bytes_buffer_drain_is_frame_change_bodies() {
        let identity = encode_cell_identity("op", "task", "k");
        let header = HeaderTemplate::new(0x010203, 1);
        let bodies: Vec<Vec<u8>> = vec![vec![1, 2], vec![3], vec![4, 5, 6]];
        let ops = vec![TimerOp::register(
            crate::schema::TimerKind::ProcessingTime,
            7,
        )];

        let mut buffer = EffectBuffer::<RawBodies> {
            changes: bodies.clone(),
            timer_ops: ops.clone(),
            ..Default::default()
        };
        let (count, frames) = buffer.drain_to_frames(&identity, &AtomicU64::new(3), &header);
        let expected = frame_change_bodies(bodies, ops, &identity, &AtomicU64::new(3), &header);

        assert_eq!(count, 4);
        assert!(buffer.is_empty());
        assert_eq!(
            frames
                .iter()
                .map(|f| (f.payload_id, &f.bytes))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|f| (f.payload_id, &f.bytes))
                .collect::<Vec<_>>()
        );
    }

    /// Outside the tier the park comes back to the caller; inside it the
    /// park rides the one effect entry.
    #[test]
    fn settle_effects_keeps_the_tier_off_park_for_the_caller_and_folds_the_tier_on_one() {
        let dlq = DlqEntryBody {
            channel: "in".into(),
            error: "poison".into(),
            attempts: 3,
            payload: Some(vec![9]),
        };
        let effects = || InvocationEffects {
            changes: vec![vec![1u8]],
            timers: Vec::new(),
            outputs: Vec::new(),
            offset: Some(4),
            event_time: 0,
            checkpointed: false,
            parked: false,
        };

        let mut off = EffectBuffer::<RawBodies>::default();
        off.pending_attempt.push(vec![0]);
        let policy = SettlePolicy {
            records_effects: false,
            checkpoint: None,
            buffered: true,
        };
        let park = settle_effects(&mut off, policy, effects(), Some(dlq.clone()));
        assert_eq!(park.as_ref().map(|d| d.attempts), Some(3));
        assert_eq!(
            off.changes,
            vec![vec![0u8], vec![1u8]],
            "the held attempt comes first"
        );
        assert!(off.effects.is_empty() && off.pending_attempt.is_empty());

        let mut on = EffectBuffer::<RawBodies>::default();
        let policy = SettlePolicy {
            records_effects: true,
            checkpoint: None,
            buffered: true,
        };
        assert!(settle_effects(&mut on, policy, effects(), Some(dlq)).is_none());
        assert!(on.changes.is_empty());
        assert_eq!(on.effects.len(), 1);
        assert_eq!(on.effects[0].consumed_offset, Some(4));
        assert_eq!(on.effects[0].dlq.as_ref().map(|d| d.attempts), Some(3));
    }
}

#[cfg(test)]
mod staged_interval_tests {
    use super::*;
    use crate::operator::topology::{CheckpointOutputs, CheckpointPolicy};

    fn output(payload: &str) -> OutputRecord {
        OutputRecord {
            channel: "out".to_string(),
            key: "k".to_string(),
            metadata: Vec::new(),
            payload: payload.as_bytes().to_vec(),
        }
    }

    fn invocation(
        changes: Vec<u8>,
        outputs: Vec<OutputRecord>,
        offset: Option<u64>,
    ) -> InvocationEffects<u8> {
        InvocationEffects {
            changes,
            timers: Vec::new(),
            outputs,
            offset,
            event_time: 7,
            checkpointed: false,
            parked: false,
        }
    }

    fn at_checkpoint(every: Option<u32>, max_delay_ms: Option<u64>) -> CheckpointPolicy {
        CheckpointPolicy {
            every,
            outputs: CheckpointOutputs::AtCheckpoint,
            max_delay_ms,
        }
    }

    fn as_produced(every: Option<u32>) -> CheckpointPolicy {
        CheckpointPolicy {
            every,
            outputs: CheckpointOutputs::AsProduced,
            max_delay_ms: None,
        }
    }

    #[test]
    fn per_input_ships_each_effectful_invocation_and_nothing_else() {
        let mut staged: StagedInterval<u8> = StagedInterval::default();

        let shipped = staged
            .fold(None, invocation(vec![1], vec![output("a")], Some(4)), 0)
            .expect("an effectful invocation ships its own record");
        assert_eq!(shipped.changes, vec![1]);
        assert_eq!(shipped.offset, Some(4));
        assert_eq!(shipped.outputs.len(), 1);

        assert!(
            staged
                .fold(None, invocation(Vec::new(), Vec::new(), Some(5)), 0)
                .is_none(),
            "an effect-free invocation contributes nothing durable"
        );
    }

    #[test]
    fn as_produced_flushes_mid_interval_outputs_acknowledging_nothing() {
        let mut staged: StagedInterval<u8> = StagedInterval::default();
        let policy = as_produced(None);

        let shipped = staged
            .fold(
                Some(policy),
                invocation(vec![1], vec![output("a")], Some(4)),
                0,
            )
            .expect("an as-produced emit still leaves mid-interval");
        assert!(
            shipped.changes.is_empty() && shipped.offset.is_none(),
            "a mid-interval record acknowledges nothing: the state it came from is not durable"
        );
        assert_eq!(shipped.outputs.len(), 1);
        assert_eq!(staged.changes, vec![1], "the change stays staged");
    }

    #[test]
    fn at_checkpoint_holds_every_output_until_the_interval_closes() {
        let mut staged: StagedInterval<u8> = StagedInterval::default();
        let policy = at_checkpoint(Some(3), Some(50));

        for (n, offset) in [(1u8, 10u64), (2, 11)] {
            assert!(
                staged
                    .fold(
                        Some(policy),
                        invocation(vec![n], vec![output("held")], Some(offset)),
                        0
                    )
                    .is_none(),
                "no output may reach a plugin before the checkpoint"
            );
        }

        let closing = staged
            .fold(
                Some(policy),
                invocation(vec![3], vec![output("last")], Some(12)),
                0,
            )
            .expect("the third input reaches `every` and closes the interval");

        assert_eq!(closing.changes, vec![1, 2, 3]);
        assert_eq!(
            closing
                .outputs
                .iter()
                .map(|o| o.payload.clone())
                .collect::<Vec<_>>(),
            vec![b"held".to_vec(), b"held".to_vec(), b"last".to_vec()],
            "the whole interval's outputs ship as one batch, in production order"
        );
        assert_eq!(
            closing.offset,
            Some(12),
            "the closing record carries the interval's last offset"
        );
        assert!(!staged.is_open(), "the interval closed");
    }

    #[test]
    fn an_explicit_checkpoint_closes_early() {
        let mut staged: StagedInterval<u8> = StagedInterval::default();
        let policy = at_checkpoint(Some(500), Some(50));

        assert!(staged
            .fold(
                Some(policy),
                invocation(vec![1], vec![output("a")], Some(1)),
                0
            )
            .is_none());

        let mut closing = invocation(vec![2], vec![output("b")], Some(2));
        closing.checkpointed = true;
        let shipped = staged
            .fold(Some(policy), closing, 0)
            .expect("an explicit checkpoint closes the interval well before `every`");
        assert_eq!(shipped.changes, vec![1, 2]);
        assert_eq!(shipped.outputs.len(), 2);
    }

    #[test]
    fn the_delay_closes_a_quiet_entitys_interval() {
        let mut staged: StagedInterval<u8> = StagedInterval::default();
        let policy = at_checkpoint(None, Some(50));

        assert!(staged
            .fold(
                Some(policy),
                invocation(vec![1], vec![output("a")], Some(9)),
                1_000
            )
            .is_none());
        assert_eq!(staged.deadline_ms(policy), Some(1_050));
        assert!(!staged.delay_elapsed(policy, 1_049));
        assert!(staged.delay_elapsed(policy, 1_050));

        let shipped = staged
            .close_on_deadline()
            .expect("the deadline ships what the interval accumulated");
        assert_eq!(shipped.outputs.len(), 1);
        assert_eq!(
            shipped.offset,
            Some(9),
            "the close acknowledges the interval's last input"
        );
        assert_eq!(
            shipped.event_time, 7,
            "the closing record is dated by its last invocation"
        );

        assert!(
            staged.close_on_deadline().is_none(),
            "a spent deadline entry finds nothing open and does nothing"
        );
    }

    #[test]
    fn a_park_closes_the_interval_so_the_poison_input_never_replays() {
        let mut staged: StagedInterval<u8> = StagedInterval::default();
        let policy = at_checkpoint(Some(500), Some(50));

        assert!(staged
            .fold(
                Some(policy),
                invocation(vec![1], vec![output("a")], Some(1)),
                0
            )
            .is_none());

        let mut parking = invocation(Vec::new(), Vec::new(), Some(2));
        parking.parked = true;
        let shipped = staged
            .fold(Some(policy), parking, 0)
            .expect("a park must ship its record atomically with the offset advance");
        assert_eq!(shipped.offset, Some(2));
        assert_eq!(shipped.changes, vec![1]);
    }

    #[test]
    fn a_restore_discards_the_open_interval_entirely() {
        let mut staged: StagedInterval<u8> = StagedInterval::default();
        let policy = at_checkpoint(Some(500), Some(50));

        assert!(staged
            .fold(
                Some(policy),
                invocation(vec![1], vec![output("a")], Some(1)),
                0
            )
            .is_none());
        staged.clear();

        assert!(staged.changes.is_empty());
        assert!(staged.outputs.is_empty());
        assert!(staged.offset.is_none());
        assert!(!staged.is_open());
    }
}
