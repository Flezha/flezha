//! The per-process drain thread and the wire frames it builds: every linger
//! tick it drains each registered Cell's buffer into `cell_state_change`
//! / `cell_effect_record` frames (layout in `docs/sdk/state_persistence.md`)
//! and ships them through the state sink. The invariant: a change leaves its
//! buffer exactly once, under a seq fetched from the Cell's own counter, so
//! a background drain and an inline flush never gap or duplicate.

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use dashmap::DashMap;

pub use crate::effect::*;
use crate::header::HeaderTemplate;
use crate::util::LockRecover;

/// A registered Cell's flusher entry, opaque to callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlusherEntryId(u64);

/// Why the sink could not ship a frame.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("transport down: {0}")]
    TransportDown(String),
    #[error("send failure: {0}")]
    Other(String),
}

/// Ship one built frame under a data-stream payload id.
pub type SendFrameFn = Arc<dyn Fn(&str, &[u8]) -> Result<(), SendError> + Send + Sync + 'static>;

struct FlusherEntry {
    cell_identity: Arc<Vec<u8>>,
    /// The instance's processed prefix, reported after each drain.
    prefix: Option<Arc<crate::prefix::PrefixLedger>>,
    buffer: Arc<Mutex<Box<dyn FlushableBuffer>>>,
    persistence_enabled: bool,
    change_seq: Arc<AtomicU64>,
    header: HeaderTemplate,
    /// The Cell's `push_deferred`: while set the drain skips the buffer, or
    /// a consumed offset could be durable ahead of an undelivered edge output.
    push_deferred: Arc<AtomicBool>,
}

/// The wire `cell_id_bytes` the controller's `decode_cell_id` reads.
pub fn encode_cell_identity(operator_class: &str, tasklet_id: &str, cell_key: &str) -> Vec<u8> {
    let rc = operator_class.as_bytes();
    let sh = tasklet_id.as_bytes();
    let pk = cell_key.as_bytes();
    assert!(
        rc.len() <= 255 && sh.len() <= 255,
        "operator_class / tasklet_id ≤ 255 bytes"
    );
    assert!(pk.len() <= 0xFFFF, "cell_key ≤ 65535 bytes");
    let mut out = Vec::with_capacity(1 + rc.len() + 1 + sh.len() + 2 + pk.len());
    out.push(rc.len() as u8);
    out.extend_from_slice(rc);
    out.push(sh.len() as u8);
    out.extend_from_slice(sh);
    out.extend_from_slice(&(pk.len() as u16).to_be_bytes());
    out.extend_from_slice(pk);
    out
}

/// One drained frame and the payload id it ships under.
pub struct FlushFrame {
    pub payload_id: &'static str,
    pub bytes: Vec<u8>,
}

/// The erased per-Cell flush buffer as the flusher sees it.
pub trait FlushableBuffer: Send + Any {
    /// Coalesce and frame everything flushable, in emission order, one
    /// `change_seq` fetch per frame; `(post_coalesce_count, frames)`.
    fn drain_to_frames(
        &mut self,
        cell_identity: &[u8],
        change_seq: &AtomicU64,
        header: &HeaderTemplate,
    ) -> (usize, Vec<FlushFrame>);

    /// Nothing flushable; an open checkpoint interval does not count.
    fn is_empty(&self) -> bool;

    /// Drop everything not yet shipped, an open checkpoint interval's
    /// accumulation included, for a restore.
    fn discard_unshipped(&mut self);

    /// Keep a settled input's hold on its instance's processed prefix until
    /// the record it settled into is written.
    fn keep_hold(&mut self, hold: Arc<crate::prefix::Hold>);

    /// The holds the next drain releases once its frames are written.
    fn take_holds(&mut self) -> Vec<Arc<crate::prefix::Hold>>;

    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// One `format_id = 3` timer-ops record, shipped under the ordinary
/// `cell_state_change` id; the controller never decodes it.
pub fn build_cell_timer_ops_frame(
    cell_identity: &[u8],
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
    build_cell_frame(cell_identity, seq, &header_bytes, &body)
}

/// One frame from an already-serialized body.
pub fn build_cell_frame(cell_identity: &[u8], seq: u64, header: &[u8; 16], body: &[u8]) -> Vec<u8> {
    let crc = crc32fast::hash(body);
    let pid_len = cell_identity.len();
    let mut frame = Vec::with_capacity(2 + pid_len + 8 + 16 + 4 + body.len() + 4);
    frame.extend_from_slice(&(pid_len as u16).to_be_bytes());
    frame.extend_from_slice(cell_identity);
    frame.extend_from_slice(&seq.to_be_bytes());
    frame.extend_from_slice(header);
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(body);
    frame.extend_from_slice(&crc.to_be_bytes());
    frame
}

/// The per-process flusher; its `flezha-flusher` thread starts on the first
/// registration.
pub struct CellFlusher {
    entries: Arc<DashMap<FlusherEntryId, FlusherEntry>>,
    next_entry_id: AtomicU64,
    shutdown: Arc<AtomicBool>,
    linger_ms: u64,
}

static FLUSHER: OnceLock<Arc<CellFlusher>> = OnceLock::new();

const ENV_LINGER_MS: &str = "FLEZHA_FLUSH_LINGER_MS";
const DEFAULT_LINGER_MS: u64 = 5;

impl CellFlusher {
    pub fn instance() -> Arc<CellFlusher> {
        FLUSHER
            .get_or_init(|| {
                let linger_ms = crate::util::env_parse(ENV_LINGER_MS, DEFAULT_LINGER_MS);
                let flusher = Arc::new(CellFlusher {
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

    /// Drop an evicted Cell's entry. Idempotent.
    pub fn deregister(&self, id: FlusherEntryId) {
        self.entries.remove(&id);
    }

    /// Register a Cell's buffer; the id deregisters it on eviction.
    #[allow(clippy::too_many_arguments)]
    pub fn register(
        &self,
        cell_identity: Arc<Vec<u8>>,
        prefix: Option<Arc<crate::prefix::PrefixLedger>>,
        buffer: Arc<Mutex<Box<dyn FlushableBuffer>>>,
        persistence_enabled: bool,
        change_seq: Arc<AtomicU64>,
        header: HeaderTemplate,
        push_deferred: Arc<AtomicBool>,
    ) -> FlusherEntryId {
        let id = FlusherEntryId(self.next_entry_id.fetch_add(1, Ordering::Relaxed));
        self.entries.insert(
            id,
            FlusherEntry {
                cell_identity,
                prefix,
                buffer,
                persistence_enabled,
                change_seq,
                header,
                push_deferred,
            },
        );
        id
    }

    /// Whether a Cell of `operator_class` is registered, for tests.
    #[cfg(test)]
    pub(crate) fn holds_class(&self, operator_class: &str) -> bool {
        self.entries.iter().any(|entry| {
            decode_cell_identity(&entry.cell_identity)
                .is_some_and(|(class, _, _)| class == operator_class)
        })
    }

    /// One synchronous drain pass, for tests.
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
            // A Cell owing an undelivered inter-tasklet push keeps its
            // records: the offset must not become durable ahead of the edge
            // delivery it derives from. Bounded — a gated Cell is handed no
            // further work, so at most one item's record waits here.
            if entry.push_deferred.load(Ordering::Acquire) {
                continue;
            }
            {
                let mut guard = entry.buffer.lock_recover();
                flush_one(
                    &mut **guard,
                    &entry.cell_identity,
                    &entry.change_seq,
                    &entry.header,
                    entry.persistence_enabled,
                );
            }
            // Behind this Cell's records; the first Cell of an instance to
            // report takes the instance's move, the rest find nothing to send.
            if let Some(prefix) = &entry.prefix {
                prefix.report();
            }
        }
    }
}

/// Drain ONE cell's buffer and ship the frames, for the background pass
/// and the inline flush-before-emit alike, answering the seq of the last
/// effect record it shipped. The caller holds the buffer lock, which is what
/// makes the seqs the drain assigned the frames' own.
pub fn flush_one(
    buffer: &mut dyn FlushableBuffer,
    cell_identity: &[u8],
    change_seq: &AtomicU64,
    header: &HeaderTemplate,
    persistence_enabled: bool,
) -> Option<u64> {
    // Released only once this drain's frames are written: an input stops
    // holding its instance's processed prefix when its record is on the wire.
    let _holds = buffer.take_holds();
    if buffer.is_empty() {
        return None;
    }
    let first_seq = change_seq.load(Ordering::Relaxed);
    let (_count, frames) = buffer.drain_to_frames(cell_identity, change_seq, header);
    if !persistence_enabled {
        tracing::trace!(
            target: "flezha::flusher",
            "drain-and-discard {} frames (persistence_enabled = false)",
            frames.len(),
        );
        return None;
    }
    let record_seq = frames
        .iter()
        .rposition(|f| f.payload_id == crate::state_sink::CELL_EFFECT_RECORD_PAYLOAD_ID)
        .map(|index| first_seq + index as u64);
    for frame in frames {
        // Credit debit-on-send. Parks here (under the caller's buffer lock,
        // which is what keeps per-Cell seq order across the background and
        // inline drain paths) when the window is exhausted; a producer pushing
        // into this buffer then blocks on the lock — the emit-path park.
        crate::credit::global().acquire(frame.bytes.len());
        crate::state_sink::send(frame.payload_id, frame.bytes);
    }
    record_seq
}

/// One `cell_effect_record` frame from a serialized body, stamped
/// `format_id = 2`.
pub fn build_effect_record_frame(
    cell_identity: &[u8],
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
    build_cell_frame(cell_identity, seq, &header_bytes, &body_bytes)
}

/// Frame encoded change bodies plus a trailing timer-ops record — the drain
/// tail every buffer kind produces.
pub fn frame_change_bodies(
    bodies: Vec<Vec<u8>>,
    timer_ops: Vec<crate::schema::TimerOp>,
    cell_identity: &[u8],
    change_seq: &AtomicU64,
    header: &HeaderTemplate,
) -> Vec<FlushFrame> {
    let mut frames: Vec<FlushFrame> = Vec::with_capacity(bodies.len() + 1);
    for body in bodies {
        let seq = change_seq.fetch_add(1, Ordering::Relaxed);
        let header_bytes = header.stamp(&body);
        frames.push(FlushFrame {
            payload_id: crate::state_sink::CELL_STATE_CHANGE_PAYLOAD_ID,
            bytes: build_cell_frame(cell_identity, seq, &header_bytes, &body),
        });
    }
    if !timer_ops.is_empty() {
        let seq = change_seq.fetch_add(1, Ordering::Relaxed);
        frames.push(FlushFrame {
            payload_id: crate::state_sink::CELL_STATE_CHANGE_PAYLOAD_ID,
            bytes: build_cell_timer_ops_frame(cell_identity, seq, header, &timer_ops),
        });
    }
    frames
}

/// The inverse of `flusher::encode_cell_identity`.
pub(crate) fn decode_cell_identity(bytes: &[u8]) -> Option<(String, String, String)> {
    let take = |offset: &mut usize, len: usize| -> Option<String> {
        let end = offset.checked_add(len)?;
        let slice = bytes.get(*offset..end)?;
        *offset = end;
        std::str::from_utf8(slice).ok().map(str::to_owned)
    };

    let mut offset = 0usize;
    let class_len = *bytes.first()? as usize;
    offset += 1;
    let class = take(&mut offset, class_len)?;

    let tasklet_len = *bytes.get(offset)? as usize;
    offset += 1;
    let tasklet = take(&mut offset, tasklet_len)?;

    let key_len = u16::from_be_bytes([*bytes.get(offset)?, *bytes.get(offset + 1)?]) as usize;
    offset += 2;
    let key = take(&mut offset, key_len)?;

    if offset == bytes.len() {
        Some((class, tasklet, key))
    } else {
        None
    }
}

#[cfg(test)]
mod identity_tests {
    use super::{decode_cell_identity, encode_cell_identity};

    #[test]
    fn round_trips_the_identity_a_restore_names() {
        let encoded = encode_cell_identity("ohlc#p1", "candle_tasklet", "BTC-USD");

        assert_eq!(
            decode_cell_identity(&encoded),
            Some((
                "ohlc#p1".to_string(),
                "candle_tasklet".to_string(),
                "BTC-USD".to_string()
            ))
        );
    }

    #[test]
    fn refuses_a_truncated_or_over_long_identity() {
        let encoded = encode_cell_identity("class", "tasklet", "key");

        assert_eq!(decode_cell_identity(&encoded[..encoded.len() - 1]), None);

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert_eq!(decode_cell_identity(&trailing), None);
    }
}
