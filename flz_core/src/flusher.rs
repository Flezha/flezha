//! Per-Entity flusher per `operative-state-persistence/design.md`
//! Decision 2.
//!
//! Single OS thread per process, lazy-initialized via `OnceLock` on the
//! first `define_stream::<R>()` for a Procedure with a non-`NoChange`
//! Change. The thread runs an infinite loop: sleep for `linger_ms`,
//! iterate registered Entities, drain non-empty buffers, build wire
//! frames, hand them to the per-Entity `send_frame` fn pointer.
//!
//! Frame layout (matches controller `Flezha.State.Wire.decode_frame/1`):
//!
//! ```text
//! [entity_id_len: u16 BE]
//! [entity_id_bytes]   // [rc_len:u8][rc][sh_len:u8][sh][pk_len:u16 BE][pk]
//! [seq: u64 BE]
//! [header: 16B]
//! [body_len: u32 BE]
//! [body]                 // change_bytes (or snapshot_bytes for compactions)
//! [crc32: u32 BE over body]
//! ```
//!
//! The structured `entity_id_bytes` carries `(operative_class, procedure_id,
//! entity_key)` so the controller can route the record to the right
//! per-Entity WAL file without a side-table mapping. Header is the
//! 16-byte `HeaderTemplate::stamp` output (per Decision 8); the trailing
//! CRC32 covers the body so the controller can validate framing without
//! re-decoding the header.

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::header::HeaderTemplate;
use crate::util::LockRecover;

/// Global identifier for a registered Entity entry. Assigned at
/// flusher-registration time; opaque to callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlusherEntryId(u64);

/// Errors returned from the wire stub or its controller-wired replacement.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("transport down: {0}")]
    TransportDown(String),
    #[error("send failure: {0}")]
    Other(String),
}

/// Ship one already-built wire frame under a given data-stream payload-id. The
/// installed sink wraps the frame in the payloader envelope with that id, so the
/// same reliable sink carries `entity_state_change` and the saga
/// `compensation_intent` / `compensation_done` records.
pub type SendFrameFn = Arc<dyn Fn(&str, &[u8]) -> Result<(), SendError> + Send + Sync + 'static>;

/// Per-Entity entry registered with the flusher.
struct FlusherEntry {
    entity_identity: Arc<Vec<u8>>,
    buffer: Arc<Mutex<Box<dyn FlushableBuffer>>>,
    persistence_enabled: bool,
    change_seq: Arc<AtomicU64>,
    header: HeaderTemplate,
}

/// Encode `(operative_class, procedure_id, entity_key)` into the wire-format
/// `entity_id_bytes` consumed by the controller's `decode_entity_id`.
pub fn encode_entity_identity(
    operative_class: &str,
    procedure_id: &str,
    entity_key: &str,
) -> Vec<u8> {
    let rc = operative_class.as_bytes();
    let sh = procedure_id.as_bytes();
    let pk = entity_key.as_bytes();
    assert!(
        rc.len() <= 255 && sh.len() <= 255,
        "operative_class / procedure_id ≤ 255 bytes"
    );
    assert!(pk.len() <= 0xFFFF, "entity_key ≤ 65535 bytes");
    let mut out = Vec::with_capacity(1 + rc.len() + 1 + sh.len() + 2 + pk.len());
    out.push(rc.len() as u8);
    out.extend_from_slice(rc);
    out.push(sh.len() as u8);
    out.extend_from_slice(sh);
    out.extend_from_slice(&(pk.len() as u16).to_be_bytes());
    out.extend_from_slice(pk);
    out
}

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

use crate::operative::topology::{CheckpointOutputs, CheckpointPolicy};
/// Dead-letter park carried inside an effect record (effectively-once
/// tier): the poisoned invocation's park entry rides the SAME atomic WAL
/// record as its consumed-offset advance. One definition — the boundary
/// schema's — shared with completions.
pub use crate::schema::DlqEntry as DlqEntryBody;
use crate::schema::TimerOp;

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

impl EffectRecordBody {
    /// The record a fold or a deadline close produced, plus the park that rides
    /// it. One place the shipped-record fields become an effect record, so the
    /// two cannot drift apart field by field.
    pub fn from_shipped(record: ShippedRecord<Vec<u8>>, dlq: Option<DlqEntryBody>) -> Self {
        Self {
            changes: record.changes,
            outputs: record.outputs,
            consumed_offset: record.offset,
            dlq,
            event_time: record.event_time,
            timers: record.timers,
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

/// One drained wire frame plus the data-stream payload id it ships under
/// (`entity_state_change` for plain changes, `entity_effect_record` for
/// effect records).
pub struct FlushFrame {
    pub payload_id: &'static str,
    pub bytes: Vec<u8>,
}

/// Type-erased per-Entity flush buffer. The dispatcher downcasts to
/// `TypedBuffer<S>` when constructing a `StateHandle`; the flusher only
/// sees the trait surface.
pub trait FlushableBuffer: Send + Any {
    /// Drain the buffer. Applies coalescing and produces wire frames in
    /// emission order. Returns `(post_coalesce_count, frames)`.
    ///
    /// `change_seq` is fetched-and-added once per produced frame; the
    /// returned frames carry seq values in increasing order.
    fn drain_to_frames(
        &mut self,
        entity_identity: &[u8],
        change_seq: &AtomicU64,
        header: &HeaderTemplate,
    ) -> (usize, Vec<FlushFrame>);

    /// Whether the underlying buffer is empty. The flusher uses this to
    /// avoid building frames when an Entity has had no `state.apply`
    /// calls in the linger window. An open checkpoint interval does not
    /// count: its changes are not flushable on their own.
    fn is_empty(&self) -> bool;

    /// Drop an open checkpoint interval's accumulated changes. A restore
    /// re-seats the Entity on a past in which they were never applied, and
    /// the controller re-forwards the inputs that produced them.
    fn discard_staged(&mut self);

    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Build one `format_id = 3` timer-ops record: the body is the postcard
/// `Vec<TimerOp>` the restore fold applies to the Entity's pending set. Ships
/// under the ordinary `entity_state_change` payload-id — the controller stores
/// it, truncates it and hands it back with every other record, and never
/// decodes it.
pub fn build_entity_timer_ops_frame(
    entity_identity: &[u8],
    seq: u64,
    header: &HeaderTemplate,
    ops: &[crate::schema::TimerOp],
) -> Vec<u8> {
    let body = postcard::to_allocvec(ops).expect("postcard serialization of TimerOp cannot fail");
    let timer_header = HeaderTemplate {
        format_id: crate::header::FORMAT_ID_TIMER_OPS,
        ..*header
    };
    let header_bytes = timer_header.stamp(&body);
    build_entity_frame(entity_identity, seq, &header_bytes, &body)
}

/// Build one wire frame from already-serialized body bytes. Used by the
/// snapshot path (where the body is the postcard-encoded `Self::State`).
pub fn build_entity_frame(
    entity_identity: &[u8],
    seq: u64,
    header: &[u8; 16],
    body: &[u8],
) -> Vec<u8> {
    let crc = crc32fast::hash(body);
    let pid_len = entity_identity.len();
    let mut frame = Vec::with_capacity(2 + pid_len + 8 + 16 + 4 + body.len() + 4);
    frame.extend_from_slice(&(pid_len as u16).to_be_bytes());
    frame.extend_from_slice(entity_identity);
    frame.extend_from_slice(&seq.to_be_bytes());
    frame.extend_from_slice(header);
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(body);
    frame.extend_from_slice(&crc.to_be_bytes());
    frame
}

/// Single per-process flusher. Spawns one OS thread named `flezha-flusher`
/// on first registration of a non-`NoChange` Entity. Programs without
/// any non-`NoChange` Procedures never reach `instance()`, so no thread is
/// spawned.
pub struct EntityFlusher {
    entries: Arc<DashMap<FlusherEntryId, FlusherEntry>>,
    next_entry_id: AtomicU64,
    shutdown: Arc<AtomicBool>,
    linger_ms: u64,
}

static FLUSHER: OnceLock<Arc<EntityFlusher>> = OnceLock::new();

const ENV_LINGER_MS: &str = "FLEZHA_FLUSH_LINGER_MS";
const DEFAULT_LINGER_MS: u64 = 5;

impl EntityFlusher {
    /// Lazily-initialized per-process flusher. Spawns the
    /// `flezha-flusher` OS thread on first call.
    pub fn instance() -> Arc<EntityFlusher> {
        FLUSHER
            .get_or_init(|| {
                let linger_ms = crate::util::env_parse(ENV_LINGER_MS, DEFAULT_LINGER_MS);
                let flusher = Arc::new(EntityFlusher {
                    entries: Arc::new(DashMap::new()),
                    next_entry_id: AtomicU64::new(0),
                    shutdown: Arc::new(AtomicBool::new(false)),
                    linger_ms,
                });
                let handle = Arc::clone(&flusher);
                thread::Builder::new()
                    .name("flezha-flusher".to_string())
                    .spawn(move || handle.run_loop())
                    .expect("spawning flezha-flusher thread should not fail");
                flusher
            })
            .clone()
    }

    /// Drop an Entity's flusher entry. Called when a dynamic-mode
    /// Entity is evicted by the reaper so its (now-orphaned) flush
    /// buffer stops being drained every linger tick. Idempotent — a
    /// missing id is a no-op.
    pub fn deregister(&self, id: FlusherEntryId) {
        self.entries.remove(&id);
    }

    /// Register an Entity with the flusher. Returns an opaque id used
    /// to deregister at Entity teardown (dynamic-mode eviction calls
    /// [`Self::deregister`]; static Entities live for the process).
    pub fn register(
        &self,
        entity_identity: Arc<Vec<u8>>,
        buffer: Arc<Mutex<Box<dyn FlushableBuffer>>>,
        persistence_enabled: bool,
        change_seq: Arc<AtomicU64>,
        header: HeaderTemplate,
    ) -> FlusherEntryId {
        let id = FlusherEntryId(self.next_entry_id.fetch_add(1, Ordering::Relaxed));
        self.entries.insert(
            id,
            FlusherEntry {
                entity_identity,
                buffer,
                persistence_enabled,
                change_seq,
                header,
            },
        );
        id
    }

    /// Test-only: synchronously drive one drain pass so unit tests can
    /// observe drained state without sleeping for the linger window.
    #[doc(hidden)]
    pub fn drain_now_for_tests(&self) {
        self.drain_pass();
    }

    fn run_loop(self: Arc<Self>) {
        while !self.shutdown.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(self.linger_ms));
            self.drain_pass();
        }
    }

    fn drain_pass(&self) {
        for entry_ref in self.entries.iter() {
            let entry = entry_ref.value();
            let mut guard = entry.buffer.lock_recover();
            flush_one(
                &mut **guard,
                &entry.entity_identity,
                &entry.change_seq,
                &entry.header,
                entry.persistence_enabled,
            );
        }
    }
}

/// Drain ONE entity's buffer to frames and ship them through the live
/// global sink. Shared between the background `drain_pass` (one call per
/// registered entry per linger tick) and the synchronous flush-before-emit
/// path on the dispatch thread. The caller holds the buffer lock; this
/// function does not re-lock, so it cannot self-deadlock against the
/// singleton flusher thread.
///
/// The sink resolves the current secondary stream each call (so a
/// reconnect's freshly-wired stream is used without re-registering this
/// entity) and buffers frames in its reconnect ring when the stream is
/// down. Frames carry seq values fetched from the same `change_seq`, so
/// concurrent background + inline drains stay gap-free and never duplicate
/// a record (each change is moved out of the buffer exactly once).
pub fn flush_one(
    buffer: &mut dyn FlushableBuffer,
    entity_identity: &[u8],
    change_seq: &AtomicU64,
    header: &HeaderTemplate,
    persistence_enabled: bool,
) {
    if buffer.is_empty() {
        return;
    }
    let (_count, frames) = buffer.drain_to_frames(entity_identity, change_seq, header);
    if !persistence_enabled {
        tracing::trace!(
            target: "flezha::flusher",
            "drain-and-discard {} frames (persistence_enabled = false)",
            frames.len(),
        );
        return;
    }
    for frame in frames {
        // Credit debit-on-send. Parks here (under the caller's buffer lock,
        // which is what keeps per-Entity seq order across the background and
        // inline drain paths) when the window is exhausted; a producer pushing
        // into this buffer then blocks on the lock — the emit-path park.
        crate::credit::global().acquire(frame.bytes.len());
        crate::state_sink::send(frame.payload_id, frame.bytes);
    }
}

/// Build one `entity_effect_record` frame from an already-bytes body
/// (effectively-once tier), stamped `format_id = 2` so mixed WALs stay
/// readable. The typed layer's generic builder serializes its `S::Change`
/// values first and produces this exact layout.
pub fn build_effect_record_frame(
    entity_identity: &[u8],
    seq: u64,
    header: &HeaderTemplate,
    body: &EffectRecordBody,
) -> Vec<u8> {
    let body_bytes =
        postcard::to_stdvec(body).expect("postcard serialization of EffectRecordBody cannot fail");
    let effect_header = HeaderTemplate {
        format_id: crate::header::FORMAT_ID_EFFECT_RECORD,
        ..*header
    };
    let header_bytes = effect_header.stamp(&body_bytes);
    build_entity_frame(entity_identity, seq, &header_bytes, &body_bytes)
}

/// Frame pre-encoded change bodies plus a trailing timer-ops record — the
/// drain tail both buffer kinds (`TypedBuffer<S>` after serializing, the
/// host's `BytesBuffer` directly) produce identically.
pub fn frame_change_bodies(
    bodies: Vec<Vec<u8>>,
    timer_ops: Vec<crate::schema::TimerOp>,
    entity_identity: &[u8],
    change_seq: &AtomicU64,
    header: &HeaderTemplate,
) -> Vec<FlushFrame> {
    let mut frames: Vec<FlushFrame> = Vec::with_capacity(bodies.len() + 1);
    for body in bodies {
        let seq = change_seq.fetch_add(1, Ordering::Relaxed);
        let header_bytes = header.stamp(&body);
        frames.push(FlushFrame {
            payload_id: crate::state_sink::ENTITY_STATE_CHANGE_PAYLOAD_ID,
            bytes: build_entity_frame(entity_identity, seq, &header_bytes, &body),
        });
    }
    if !timer_ops.is_empty() {
        let seq = change_seq.fetch_add(1, Ordering::Relaxed);
        frames.push(FlushFrame {
            payload_id: crate::state_sink::ENTITY_STATE_CHANGE_PAYLOAD_ID,
            bytes: build_entity_timer_ops_frame(entity_identity, seq, header, &timer_ops),
        });
    }
    frames
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
