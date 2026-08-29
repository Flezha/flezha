//! What one invocation ships: [`settle_effects`], the one settlement rule the
//! typed dispatcher and the erased host both call, and [`EffectBuffer`], the
//! one per-Entity buffer it settles into, generic over the change type
//! through a [`Bodies`] marker. The invariant: a park rides the same record
//! as the consumed-offset advance it acknowledges, and nothing in an open
//! checkpoint interval is flushable on its own.

use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::flusher::{build_effect_record_frame, frame_change_bodies, FlushFrame, FlushableBuffer};
use crate::header::HeaderTemplate;
use crate::operative::topology::{CheckpointOutputs, CheckpointPolicy};
/// Dead-letter park carried inside an effect record (effectively-once
/// tier): the poisoned invocation's park entry rides the SAME atomic WAL
/// record as its consumed-offset advance. One definition — the boundary
/// schema's — shared with completions.
pub use crate::schema::DlqEntry as DlqEntryBody;
use crate::schema::TimerOp;

/// One plugin output captured inside an effect record. Field order is the
/// wire contract (postcard is field-order-sensitive); `metadata` carries the
/// caller's user metadata as key-sorted pairs — the controller derives the
/// identity stamps (`flz.entity` / `outbox_seq` / epoch) at flush time, so
/// none ride here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRecord {
    pub channel: String,
    pub key: String,
    pub metadata: Vec<(String, String)>,
    pub payload: Vec<u8>,
}

/// Wire body of an `entity_effect_record` frame (header `format_id = 2`):
/// one invocation's state change(s), plugin emits, consumed input offset,
/// and (for a parked poison payload) its dead-letter entry, persisted by
/// the controller as ONE WAL record under ONE fsync. `changes` entries are
/// the postcard `S::Change` bytes a plain change record would carry, so
/// restore folds them identically.
///
/// `event_time` is the invocation's declared clock value. For an
/// `EventTimeFrom` clock it is the extracted event time, which is what makes
/// the controller's event-time → seq → input-offset lookup a read rather than
/// a guess — the correction baseline rests on it.
///
/// `timers` carries the invocation's mutations of the Entity's pending-timer
/// set. A firing's removal rides here beside the Changes and outputs it
/// produced, which is what makes the firing exactly-once under the tier: a
/// crash between "fired" and "durable" replays the whole thing, because
/// neither the effects nor the removal landed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectRecordBody {
    pub changes: Vec<Vec<u8>>,
    pub outputs: Vec<OutputRecord>,
    pub consumed_offset: Option<u64>,
    pub dlq: Option<DlqEntryBody>,
    pub event_time: u64,
    pub timers: Vec<crate::schema::TimerOp>,
}

/// One invocation's buffered effect entry (effectively-once tier), held on the
/// change type the class buffers — `S::Change` for a typed class, bytes for a
/// host class — and serialized into an [`EffectRecordBody`] at drain.
pub struct EffectEntry<C> {
    pub changes: Vec<C>,
    pub outputs: Vec<OutputRecord>,
    pub consumed_offset: Option<u64>,
    pub dlq: Option<DlqEntryBody>,
    pub event_time: u64,
    pub timers: Vec<TimerOp>,
}

impl<C> EffectEntry<C> {
    /// The record a fold or a deadline close produced, plus the park that rides
    /// it. One place the shipped-record fields become an effect entry, so the
    /// two cannot drift apart field by field.
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

    /// The wire body: the changes become bytes, everything else moves.
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

/// What one invocation produced, before the class's ack granularity decides
/// how much of it ships now.
pub struct InvocationEffects<C> {
    pub changes: Vec<C>,
    pub timers: Vec<TimerOp>,
    pub outputs: Vec<OutputRecord>,
    /// The input offset this invocation acknowledges, `None` for a firing.
    pub offset: Option<u64>,
    /// The invocation's declared clock value.
    pub event_time: u64,
    /// The body called `ctx.checkpoint()`. Meaningless outside the checkpoint
    /// granularity, which is why it is not folded into `parked` here.
    pub checkpointed: bool,
    /// The invocation parked a poison payload. Its record MUST ship so the
    /// dead-letter park and the consumed-offset advance stay atomic — and,
    /// under the checkpoint granularity, it closes the interval, because the
    /// input it is about to acknowledge into the DLQ must never replay.
    pub parked: bool,
}

/// The durable record one fold produced.
pub struct ShippedRecord<C> {
    pub changes: Vec<C>,
    pub timers: Vec<TimerOp>,
    pub outputs: Vec<OutputRecord>,
    pub offset: Option<u64>,
    pub event_time: u64,
}

/// One open checkpoint interval's unacknowledged accumulation.
///
/// Nothing here is flushable on its own: draining the changes would put state
/// ahead of the offset that acknowledges it, and draining held outputs would
/// deliver output the interval may still replay. It all ships in the record
/// that closes the interval, or a restore discards it together.
pub struct StagedInterval<C> {
    pub changes: Vec<C>,
    pub timers: Vec<TimerOp>,
    /// Held only under [`CheckpointOutputs::AtCheckpoint`]; empty otherwise,
    /// where an emit is flushed as it is produced.
    pub outputs: Vec<OutputRecord>,
    /// The furthest offset folded in — what the closing record acknowledges.
    pub offset: Option<u64>,
    /// Inputs folded in, against [`CheckpointPolicy::every`].
    pub inputs: u32,
    /// When the interval opened, against [`CheckpointPolicy::max_delay_ms`].
    /// `None` exactly when the interval is closed.
    pub opened_at_ms: Option<u64>,
    /// The last folded invocation's clock value — what the closing record
    /// carries, including when a deadline closes the interval with no
    /// invocation of its own to date it.
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
    /// Fold one invocation in and answer what its durable record ships:
    /// `(changes, timers, outputs, consumed_offset)`, or `None` when an open
    /// interval swallowed it.
    ///
    /// The one place the ack granularity's rules live — the typed dispatcher
    /// and the C-ABI host both call it, so the two can never drift.
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

    /// Whether the interval is due to close on its own — the input count is
    /// reached, or it has been open longer than the declared delay.
    pub fn due(&self, policy: CheckpointPolicy, now_ms: u64) -> bool {
        policy.every.is_some_and(|every| self.inputs >= every) || self.delay_elapsed(policy, now_ms)
    }

    /// Whether `max_delay_ms` has passed since the interval opened. Separate
    /// from [`Self::due`] because the deferred wake asks only this.
    pub fn delay_elapsed(&self, policy: CheckpointPolicy, now_ms: u64) -> bool {
        match (policy.max_delay_ms, self.opened_at_ms) {
            (Some(delay), Some(opened)) => now_ms.saturating_sub(opened) >= delay,
            _ => false,
        }
    }

    /// When this interval must close by, for the deferred wake to arm against.
    pub fn deadline_ms(&self, policy: CheckpointPolicy) -> Option<u64> {
        Some(self.opened_at_ms? + policy.max_delay_ms?)
    }

    /// Take everything the closing record ships, leaving the interval closed.
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

    /// Whether anything is owed — what the deferred wake checks before
    /// shipping a record for an interval an input already closed.
    pub fn is_open(&self) -> bool {
        self.opened_at_ms.is_some()
    }

    /// Close the interval because its delay elapsed, with no invocation of its
    /// own: the accumulation is already computed, so this only decides it
    /// ships now. `None` when nothing was open.
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

    /// Drop the interval. A restore re-seats the Entity on a past in which
    /// none of it was applied, and the controller re-forwards the inputs.
    pub fn clear(&mut self) {
        let _ = self.close();
        self.offset = None;
    }
}

/// How a class's buffered changes become the bodies of its durable records,
/// and whether a drain coalesces them first. A marker type per class kind
/// rather than a trait on the change itself: the typed SDK's changes are its
/// authors' own types, which it cannot implement a core trait for.
pub trait Bodies: Send + 'static {
    type Change: Send + 'static;
    fn coalesce(changes: Vec<Self::Change>) -> Vec<Self::Change>;
    fn into_bodies(changes: Vec<Self::Change>) -> Vec<Vec<u8>>;
}

/// The host's pre-encoded bytes: they carry no key and are already bodies, so
/// both steps are a move — the erased drain pays nothing it did not pay before.
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

/// The per-Entity flush buffer, generic over the change it holds. Exactly one
/// of `changes` / `effects` is ever populated: plain changes outside the
/// effectively-once tier, per-invocation effect entries inside it.
///
/// `staged` is neither: under checkpoint ack-granularity it holds the changes
/// an open interval has applied, which are not flushable yet — they become
/// durable only inside the effect record the closing `ctx.checkpoint()` ships,
/// together with the offset that acknowledges them. Draining them on their own
/// would put the state ahead of its offset, which is the tear the mode exists
/// to prevent.
/// `timer_ops` is the tier-off home for pending-timer mutations: outside the
/// tier there is no effect record to ride, so they drain as their own
/// `format_id = 3` record. Under the tier they ride `EffectEntry::timers`
/// instead and this stays empty.
pub struct EffectBuffer<B: Bodies> {
    pub changes: Vec<B::Change>,
    /// An open checkpoint interval's unacknowledged accumulation — changes,
    /// timer registrations and (under `CheckpointOutputs::AtCheckpoint`)
    /// outputs. Shipping any of it alone would leave state, a pending timer or
    /// a delivered output ahead of the offset that acknowledges them.
    pub staged: StagedInterval<B::Change>,
    pub effects: Vec<EffectEntry<B::Change>>,
    pub timer_ops: Vec<TimerOp>,
    /// Changes a failed-but-retryable attempt applied to in-memory state,
    /// held out of the flusher's reach until the item resolves; the next
    /// attempt re-takes them so durable state cannot diverge from memory.
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
    /// Close an open checkpoint interval whose delay elapsed, shipping its
    /// accumulation as the record it owes; `false` when nothing was open.
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
        entity_identity: &[u8],
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
            entity_identity,
            change_seq,
            header,
        );
        // Effect records are never coalesced or merged: a coalesced-away
        // record would drop its outputs and consumed-offset.
        for entry in std::mem::take(&mut self.effects) {
            let seq = change_seq.fetch_add(1, Ordering::Relaxed);
            frames.push(FlushFrame {
                payload_id: crate::state_sink::ENTITY_EFFECT_RECORD_PAYLOAD_ID,
                bytes: build_effect_record_frame(
                    entity_identity,
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

/// The class facts that decide how an invocation's effects settle.
#[derive(Debug, Clone, Copy)]
pub struct SettlePolicy {
    pub effectively_once: bool,
    pub checkpoint: Option<CheckpointPolicy>,
    /// Whether tier-off changes and timer ops are kept for a drain at all —
    /// false for a host class with no flusher entry, whose bodies nobody
    /// would ever ship.
    pub buffered: bool,
}

/// Settle one resolved invocation into its Entity's buffer per the class's
/// tier. Under effectively-once its changes, outputs and consumed offset fold
/// into ONE effect record — a park rides that record, atomic with the offset
/// advance — and nothing is returned. Outside the tier the changes and timer
/// ops are kept for the flusher and the park is handed back for the caller's
/// blocking tier-off park, once the buffer lock is released. A
/// failed-but-retryable attempt's held changes are re-taken first, so durable
/// state cannot diverge from memory.
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
    if policy.effectively_once {
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
    use crate::flusher::encode_entity_identity;

    /// The host's bodies are already bytes: the drain's coalesce and
    /// serialisation steps are moves — the same allocation goes in and comes
    /// out — so the erased path pays nothing the typed path's key needs.
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

    /// A drained bytes buffer produces exactly the frames `frame_change_bodies`
    /// produces from the same bodies — the generic drain adds no step.
    #[test]
    fn a_bytes_buffer_drain_is_frame_change_bodies() {
        let identity = encode_entity_identity("op", "proc", "k");
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

    /// Outside the tier the changes land in the buffer for the flusher and the
    /// park comes back for the blocking tier-off park; inside it they fold
    /// into one effect entry that carries the park.
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
            effectively_once: false,
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
            effectively_once: true,
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
    use crate::operative::topology::{CheckpointOutputs, CheckpointPolicy};

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
