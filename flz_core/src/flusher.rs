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

/// Dead-letter park carried inside an effect record (effectively-once
/// tier): the poisoned invocation's park entry rides the SAME atomic WAL
/// record as its consumed-offset advance. One definition — the boundary
/// schema's — shared with completions.
pub use crate::schema::DlqEntry as DlqEntryBody;

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
