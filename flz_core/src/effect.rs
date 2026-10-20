//! What one invocation ships: [`settle_effects`], the one settlement rule the
//! typed dispatcher and the erased host both call, and [`EffectBuffer`], the
//! one per-Cell buffer it settles into, generic over the change type
//! through a [`Bodies`] marker. The invariant: a park rides the same record
//! as the consumed-offset advance it acknowledges, and nothing in an open
//! checkpoint interval is flushable on its own.

use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::flusher::{build_effect_record_frame, frame_change_bodies, FlushFrame, FlushableBuffer};
use crate::header::HeaderTemplate;
use crate::prefix::Hold;
use crate::relay::topology::{CheckpointOutputs, CheckpointPolicy};
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

/// One inter-tasklet value inside an effect record: the channel it was
/// emitted on and the consuming tasklet, the input-log offset of the input it
/// derives from, and the value through the channel's codec. A restore delivers it again to a
/// consumer whose own records do not acknowledge that offset yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeRecord {
    pub channel: u32,
    pub consumer: u32,
    pub origin: Option<u64>,
    pub payload: Vec<u8>,
}

/// Wire body of a `cell_effect_record` frame (`format_id = 2`): one
/// invocation's changes, outputs, consumed offset, park, timer ops and
/// inter-tasklet values, ONE WAL record under ONE fsync. `event_time` is the
/// declared clock's value — the correction baseline's lookup key; a firing's
/// removal rides beside its effects, which is what makes it exactly-once
/// under the tier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectRecordBody {
    pub changes: Vec<Vec<u8>>,
    pub outputs: Vec<OutputRecord>,
    pub consumed_offset: Option<u64>,
    pub dlq: Option<DlqEntryBody>,
    pub event_time: u64,
    pub timers: Vec<crate::schema::TimerOp>,
    pub edges: Vec<EdgeRecord>,
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
    pub edges: Vec<EdgeRecord>,
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
            edges: record.edges,
        }
    }

    /// A record of nothing but the offset.
    fn offset_only(consumed_offset: Option<u64>, event_time: u64) -> Self {
        Self {
            changes: Vec::new(),
            outputs: Vec::new(),
            consumed_offset,
            dlq: None,
            event_time,
            timers: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// The record body; the changes coalesce inside it, since a record folds
    /// back whole.
    pub fn into_body<B: Bodies<Change = C>>(self) -> EffectRecordBody {
        EffectRecordBody {
            changes: B::into_bodies(B::coalesce(self.changes)),
            outputs: self.outputs,
            consumed_offset: self.consumed_offset,
            dlq: self.dlq,
            event_time: self.event_time,
            timers: self.timers,
            edges: self.edges,
        }
    }
}

/// What one invocation produced, before the ack granularity decides how
/// much of it ships now.
pub struct InvocationEffects<C> {
    pub changes: Vec<C>,
    pub timers: Vec<TimerOp>,
    pub outputs: Vec<OutputRecord>,
    /// Inter-tasklet values, recorded only where the Cell records effects.
    pub edges: Vec<EdgeRecord>,
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
    pub edges: Vec<EdgeRecord>,
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
    /// Always held: an edge value is recorded with the offset that closes
    /// its interval.
    pub edges: Vec<EdgeRecord>,
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
            edges: Vec::new(),
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
            edges,
            offset,
            event_time,
            checkpointed,
            parked,
        } = effects;

        let Some(policy) = policy else {
            // Per-input: one record per effectful invocation, each
            // acknowledging its own input. An effect-free invocation
            // contributes nothing durable unless it parked.
            let ship = parked
                || !(changes.is_empty()
                    && outputs.is_empty()
                    && timers.is_empty()
                    && edges.is_empty());
            return ship.then_some(ShippedRecord {
                changes,
                timers,
                outputs,
                edges,
                offset,
                event_time,
            });
        };

        self.event_time = event_time;

        self.changes.extend(changes);
        self.timers.extend(timers);
        self.edges.extend(edges);
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
            let mut record = self.close();
            record.outputs.extend(flushed_now);
            return Some(record);
        }

        // Mid-interval: an as-produced emit still flushes, but in a record
        // acknowledging nothing — the state it was computed from is not
        // durable yet, so a record carrying its offset would let a restore
        // resume past inputs it never saw.
        (!flushed_now.is_empty()).then(|| ShippedRecord {
            changes: Vec::new(),
            timers: Vec::new(),
            outputs: flushed_now,
            edges: Vec::new(),
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

    /// Close the interval into the record that ships it.
    pub fn close(&mut self) -> ShippedRecord<C> {
        self.inputs = 0;
        self.opened_at_ms = None;
        ShippedRecord {
            changes: std::mem::take(&mut self.changes),
            timers: std::mem::take(&mut self.timers),
            outputs: std::mem::take(&mut self.outputs),
            edges: std::mem::take(&mut self.edges),
            offset: self.offset.take(),
            event_time: self.event_time,
        }
    }

    pub fn is_open(&self) -> bool {
        self.opened_at_ms.is_some()
    }

    /// Close on the delay, with no invocation of its own; `None` when nothing
    /// open is due, as when a checkpoint closed the interval a wake was armed
    /// for and the next one opened since.
    pub fn close_on_deadline(
        &mut self,
        policy: CheckpointPolicy,
        now_ms: u64,
    ) -> Option<ShippedRecord<C>> {
        self.delay_elapsed(policy, now_ms).then(|| self.close())
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

/// The per-Cell flush buffer. Plain changes (`changes`, with `timer_ops` as
/// their own `format_id = 3` record) are a Cell's that consumes no input
/// offset; `effects` are the tier's, one entry per invocation, and those of a
/// Cell outside the tier that does consume offsets, whose invocations fold
/// into one entry per drain so the offset rides the record holding the state
/// it produced.
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
    /// The settled inputs' holds on the instance's processed prefix, released
    /// once the drain that ships their records has written them.
    pub holds: Vec<Arc<Hold>>,
    /// Holds of the inputs an open checkpoint interval folded, released with
    /// the record that closes it.
    pub staged_holds: Vec<Arc<Hold>>,
    /// The consumed offset and event time of the last record shipped that
    /// carried one: what a compaction re-states above its snapshot.
    pub shipped: Option<(u64, u64)>,
}

impl<B: Bodies> Default for EffectBuffer<B> {
    fn default() -> Self {
        Self {
            changes: Vec::new(),
            staged: StagedInterval::default(),
            effects: Vec::new(),
            timer_ops: Vec::new(),
            pending_attempt: Vec::new(),
            holds: Vec::new(),
            staged_holds: Vec::new(),
            shipped: None,
        }
    }
}

impl<B: Bodies> EffectBuffer<B> {
    /// Close an open interval whose delay passed; `false` when nothing open
    /// is due.
    pub fn close_interval(&mut self, policy: CheckpointPolicy) -> bool {
        let Some(record) = self
            .staged
            .close_on_deadline(policy, crate::util::wall_now_ms())
        else {
            return false;
        };
        self.effects.push(EffectEntry::from_shipped(record, None));
        self.holds.append(&mut self.staged_holds);
        true
    }

    /// Fold one invocation of a Cell outside the tier into the drain's one
    /// entry. A park closes that entry, so its dead letter keeps the offset of
    /// the input it parked.
    fn fold_offset_entry(
        &mut self,
        effects: InvocationEffects<B::Change>,
        dlq: Option<DlqEntryBody>,
    ) {
        let durable = !effects.changes.is_empty()
            || !effects.timers.is_empty()
            || !effects.edges.is_empty()
            || dlq.is_some();
        match self.effects.last_mut().filter(|entry| entry.dlq.is_none()) {
            Some(entry) => {
                entry.changes.extend(effects.changes);
                entry.timers.extend(effects.timers);
                entry.edges.extend(effects.edges);
                entry.consumed_offset = entry.consumed_offset.max(effects.offset);
                entry.event_time = entry.event_time.max(effects.event_time);
                entry.dlq = dlq;
            }
            // An input that changed nothing is owed no record of its own.
            None if !durable => {}
            None => self.effects.push(EffectEntry {
                changes: effects.changes,
                timers: effects.timers,
                edges: effects.edges,
                dlq,
                ..EffectEntry::offset_only(effects.offset, effects.event_time)
            }),
        }
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
            if let Some(offset) = entry.consumed_offset {
                self.shipped = Some((offset, entry.event_time));
            }
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

    fn discard_unshipped(&mut self) {
        self.staged.clear();
        self.staged_holds.clear();
        // Everything unshipped, a mid-ladder attempt's changes included,
        // belongs to the same superseded past.
        self.changes.clear();
        self.effects.clear();
        self.timer_ops.clear();
        self.pending_attempt.clear();
        self.holds.clear();
    }

    fn nothing_unshipped(&self) -> bool {
        self.is_empty() && !self.staged.is_open() && self.holds.is_empty()
    }

    fn keep_hold(&mut self, hold: Arc<Hold>) {
        if self.staged.is_open() {
            self.staged_holds.push(hold);
        } else {
            self.holds.push(hold);
        }
    }

    fn take_holds(&mut self) -> Vec<Arc<Hold>> {
        std::mem::take(&mut self.holds)
    }

    fn restate_above_snapshot(&mut self, register_ops: Vec<TimerOp>) -> bool {
        let owed = !register_ops.is_empty() || self.shipped.is_some();
        self.timer_ops = register_ops;
        if let Some((offset, event_time)) = self.shipped {
            self.effects
                .push(EffectEntry::offset_only(Some(offset), event_time));
        }
        owed
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SettlePolicy {
    /// One effect record per invocation: the effectively-once tier, or a
    /// durable steps relay Cell ([`RelayTopology::records_effects`]).
    ///
    /// [`RelayTopology::records_effects`]: crate::relay::topology::RelayTopology::records_effects
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
            if !buffer.staged.is_open() {
                buffer.holds.append(&mut buffer.staged_holds);
            }
        }
        return None;
    }
    // A Cell that consumes offsets outside the tier keeps each one in the
    // record of the state it produced; a firing between two inputs folds into
    // the same entry, so the record order stays the invocation order.
    if policy.buffered && (effects.offset.is_some() || !buffer.effects.is_empty()) {
        buffer.fold_offset_entry(effects, dlq);
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

    fn parked() -> DlqEntryBody {
        DlqEntryBody {
            channel: "in".into(),
            error: "poison".into(),
            attempts: 3,
            payload: Some(vec![9]),
        }
    }

    fn invocation(changes: Vec<Vec<u8>>, offset: Option<u64>) -> InvocationEffects<Vec<u8>> {
        InvocationEffects {
            changes,
            timers: Vec::new(),
            outputs: Vec::new(),
            offset,
            event_time: 0,
            checkpointed: false,
            parked: false,
            edges: Vec::new(),
        }
    }

    const TIER_OFF: SettlePolicy = SettlePolicy {
        records_effects: false,
        checkpoint: None,
        buffered: true,
    };

    /// A Cell that consumes no offset outside the tier hands its park back
    /// to the caller; inside the tier the park rides the one effect entry.
    #[test]
    fn settle_effects_keeps_the_tier_off_park_for_the_caller_and_folds_the_tier_on_one() {
        let mut off = EffectBuffer::<RawBodies>::default();
        off.pending_attempt.push(vec![0]);
        let park = settle_effects(
            &mut off,
            TIER_OFF,
            invocation(vec![vec![1]], None),
            Some(parked()),
        );
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
            ..TIER_OFF
        };
        let effects = invocation(vec![vec![1]], Some(4));
        assert!(settle_effects(&mut on, policy, effects, Some(parked())).is_none());
        assert!(on.changes.is_empty());
        assert_eq!(on.effects.len(), 1);
        assert_eq!(on.effects[0].consumed_offset, Some(4));
        assert_eq!(on.effects[0].dlq.as_ref().map(|d| d.attempts), Some(3));
    }

    /// Outside the tier a Cell that consumes offsets folds its inputs into one
    /// record carrying the highest, a firing between them included; a park
    /// closes that record with its own offset.
    #[test]
    fn a_cell_consuming_offsets_folds_a_drain_into_one_record_with_the_highest_offset() {
        let mut buffer = EffectBuffer::<RawBodies>::default();
        for (offset, change) in [(Some(10), 1u8), (None, 2), (Some(11), 3), (Some(12), 4)] {
            assert!(settle_effects(
                &mut buffer,
                TIER_OFF,
                invocation(vec![vec![change]], offset),
                None
            )
            .is_none());
        }
        assert!(settle_effects(
            &mut buffer,
            TIER_OFF,
            invocation(Vec::new(), Some(13)),
            Some(parked())
        )
        .is_none());
        settle_effects(
            &mut buffer,
            TIER_OFF,
            invocation(vec![vec![5]], Some(14)),
            None,
        );

        let records: Vec<(Vec<Vec<u8>>, Option<u64>, bool)> = buffer
            .effects
            .iter()
            .map(|e| (e.changes.clone(), e.consumed_offset, e.dlq.is_some()))
            .collect();
        assert_eq!(
            records,
            vec![
                (vec![vec![1], vec![2], vec![3], vec![4]], Some(13), true),
                (vec![vec![5]], Some(14), false),
            ]
        );
        assert!(buffer.changes.is_empty());
    }

    /// An input that changed nothing starts no record of its own.
    #[test]
    fn an_input_that_changed_nothing_starts_no_record() {
        let mut buffer = EffectBuffer::<RawBodies>::default();
        settle_effects(&mut buffer, TIER_OFF, invocation(Vec::new(), Some(3)), None);
        assert!(buffer.is_empty());
    }

    /// Outside the tier an input's inter-tasklet values ride the record that
    /// carries its offset, so a restore can deliver them again.
    #[test]
    fn a_cell_consuming_offsets_keeps_its_edge_values_in_the_record() {
        let edge = |payload: u8| EdgeRecord {
            channel: 1,
            consumer: 2,
            origin: Some(3),
            payload: vec![payload],
        };
        let mut buffer = EffectBuffer::<RawBodies>::default();
        let mut only_edges = invocation(Vec::new(), Some(3));
        only_edges.edges = vec![edge(1)];
        settle_effects(&mut buffer, TIER_OFF, only_edges, None);
        let mut more = invocation(vec![vec![9]], Some(4));
        more.edges = vec![edge(2)];
        settle_effects(&mut buffer, TIER_OFF, more, None);

        assert_eq!(buffer.effects.len(), 1);
        assert_eq!(buffer.effects[0].edges, vec![edge(1), edge(2)]);
        assert_eq!(buffer.effects[0].consumed_offset, Some(4));
    }

    /// A compaction re-states the offset of the state its snapshot holds.
    #[test]
    fn a_snapshot_re_states_the_offset_it_holds() {
        let identity = encode_cell_identity("op", "task", "k");
        let header = HeaderTemplate::new(0x010203, 1);
        let mut buffer = EffectBuffer::<RawBodies>::default();
        assert!(
            !buffer.restate_above_snapshot(Vec::new()),
            "nothing shipped yet"
        );

        settle_effects(
            &mut buffer,
            TIER_OFF,
            invocation(vec![vec![1]], Some(7)),
            None,
        );
        buffer.drain_to_frames(&identity, &AtomicU64::new(0), &header);
        assert!(buffer.restate_above_snapshot(Vec::new()));
        assert_eq!(buffer.effects.len(), 1);
        assert_eq!(buffer.effects[0].consumed_offset, Some(7));
        assert!(buffer.effects[0].changes.is_empty());
    }
}

#[cfg(test)]
#[path = "effect_staged_interval_tests.rs"]
mod staged_interval_tests;
