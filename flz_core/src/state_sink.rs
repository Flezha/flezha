//! The one seam every writer on the outbound wire goes through: [`write`]
//! answers a refused write by the frame's declared [`Class`] rather than at the
//! write site, and [`send`] is the same answer for a writer with no stream in
//! hand. A frame a class holds goes into a drop-oldest ring that
//! [`install_global_sink`] replays onto the stream a reconnect wires, so
//! nothing produced mid-reconnect is lost. A sustained run of write failures is
//! a dead data stream under a live control channel, and asks the coordinator
//! for a full reconnect. An eviction is the one outcome that loses a frame, and
//! it is counted as a drop-oldest shed.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The coordinator's reconnect request; absent in transport-less runs.
static RECONNECT_HOOK: std::sync::OnceLock<fn(&'static str)> = std::sync::OnceLock::new();

/// First install wins.
pub fn install_reconnect_hook(hook: fn(&'static str)) {
    let _ = RECONNECT_HOOK.set(hook);
}

use flz_transport::transport::FlezhaStream;

use crate::flusher::SendFrameFn;

/// A per-Cell change record.
pub const CELL_STATE_CHANGE_PAYLOAD_ID: &str = "cell_state_change";

/// An effect record (effectively-once tier): `cell_state_change`'s frame
/// with a `format_id = 2` header and an `EffectRecordBody`.
pub const CELL_EFFECT_RECORD_PAYLOAD_ID: &str = "cell_effect_record";

/// The dynamic-mode hard delete; the body is the `cell_id` bytes.
pub const DELETE_CELL_STATE_PAYLOAD_ID: &str = "delete_cell_state";

/// The saga markers: a tasklet's completion, and a compensation hook's.
pub const COMPENSATION_INTENT_PAYLOAD_ID: &str = "compensation_intent";
pub const COMPENSATION_DONE_PAYLOAD_ID: &str = "compensation_done";

/// A tier-off park: `cell_state_change`'s frame with a `ParkBody`,
/// answered by `ParkPayloadAck` once fsynced.
pub const PARK_PAYLOAD_PAYLOAD_ID: &str = "park_payload";

/// A compaction snapshot at a `change_seq`; the controller truncates the WAL
/// up to it.
pub const CELL_STATE_SNAPSHOT_PAYLOAD_ID: &str = "cell_state_snapshot";

const ENV_RING_CAPACITY: &str = "FLEZHA_RECONNECT_RING_CAPACITY";
const DEFAULT_RING_CAPACITY: usize = 1024;
const ENV_FLOOD_THRESHOLD: &str = "FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD";
const DEFAULT_FLOOD_THRESHOLD: usize = 10;
const ENV_FLOOD_WINDOW_MS: &str = "FLEZHA_BROKEN_PIPE_WINDOW_MS";
const DEFAULT_FLOOD_WINDOW_MS: u64 = 5_000;
const ENV_RECONNECT_RETRY_MS: &str = "FLEZHA_RECONNECT_REQUEST_RETRY_MS";
const DEFAULT_RECONNECT_RETRY_MS: u64 = 30_000;

const RECONNECT_REASON: &str = "broken_pipe_flood";

/// A frame the wire could not take. A record is replayed through the sink that
/// frames it; a framed payload carries its own payload id and metadata, so it
/// is replayed onto the live secondary exactly as it stands.
enum Pending {
    Record { payload_id: String, frame: Vec<u8> },
    Framed(Vec<u8>),
}

impl Pending {
    fn record(payload_id: &str, frame: Vec<u8>) -> Self {
        Pending::Record {
            payload_id: payload_id.to_string(),
            frame,
        }
    }
}

struct SinkState {
    sink: Option<SendFrameFn>,
    /// The sink discards every frame, so nothing behind it answers one.
    discards: bool,
    /// Frames held while no sink was installed or a write failed; drop-oldest.
    ring: VecDeque<Pending>,
    /// Frames the ring dropped since the last install — a real loss, reported
    /// when the stream comes back.
    evicted_since_install: u64,
    broken_pipe_at: VecDeque<Instant>,
    /// When the last flood reconnect was requested; one per dead stream, but
    /// re-armed after `FLEZHA_RECONNECT_REQUEST_RETRY_MS` so an unanswered
    /// request cannot disarm the detector for good.
    reconnect_requested_at: Option<Instant>,
}

impl SinkState {
    const fn new() -> Self {
        SinkState {
            sink: None,
            discards: false,
            ring: VecDeque::new(),
            evicted_since_install: 0,
            broken_pipe_at: VecDeque::new(),
            reconnect_requested_at: None,
        }
    }

    fn push_ring(&mut self, entry: Pending) {
        if self.ring.len() >= ring_capacity() {
            // The ring holds the only copy: an eviction is a lost frame, and
            // it is counted where every drop-oldest shed on this wire is. A
            // lost record's input must never be covered by a marker.
            if let Some(Pending::Record { frame, .. }) = self.ring.pop_front() {
                crate::prefix::frame_evicted(&frame);
            }
            crate::cell::note_dropped_oldest();
            self.evicted_since_install += 1;
            if self.evicted_since_install == 1 {
                flz_transport::notice!(
                    Warn,
                    "reconnect ring full ({} frames); dropping the oldest outbound frame",
                    ring_capacity()
                );
            }
        }
        self.ring.push_back(entry);
    }

    /// The posture every writer on this wire shares for a failed write: one
    /// line to open the burst, the frame held, and the flood verdict back.
    fn hold_failed(&mut self, kind: &str, entry: Pending, error: &dyn std::fmt::Display) -> bool {
        if self.broken_pipe_at.is_empty() {
            flz_transport::notice!(
                Warn,
                "outbound {kind} write failed; buffering into reconnect ring ({} frames): {error}",
                self.ring.len() + 1
            );
        }
        self.push_ring(entry);
        self.note_broken_pipe(Instant::now())
    }

    /// Record a write failure; `true` when the caller should ask for a
    /// reconnect.
    fn note_broken_pipe(&mut self, now: Instant) -> bool {
        let window = Duration::from_millis(flood_window_ms());
        while let Some(&front) = self.broken_pipe_at.front() {
            if now.duration_since(front) > window {
                self.broken_pipe_at.pop_front();
            } else {
                break;
            }
        }
        self.broken_pipe_at.push_back(now);
        // A prior request stops counting only while it is still plausibly in
        // flight; past the retry window an unanswered request must not keep
        // the detector disarmed.
        let armed = match self.reconnect_requested_at {
            None => true,
            Some(at) => now.duration_since(at) >= Duration::from_millis(reconnect_retry_ms()),
        };
        if armed && self.broken_pipe_at.len() >= flood_threshold() {
            self.reconnect_requested_at = Some(now);
            self.broken_pipe_at.clear();
            true
        } else {
            false
        }
    }
}

static STATE: Mutex<SinkState> = Mutex::new(SinkState::new());

/// The live secondary stream an install wires before the sink, so the replay
/// can put an already-framed entry back on the wire unchanged.
static SECONDARY_STREAM: Mutex<Option<Arc<FlezhaStream>>> = Mutex::new(None);

/// Installing a stream arms the timers of Cells restored while none was.
pub fn set_secondary_stream(stream: Option<Arc<FlezhaStream>>) {
    let installed = stream.is_some();
    *SECONDARY_STREAM
        .lock()
        .expect("secondary stream lock poisoned") = stream;
    if installed {
        crate::timer::arm_awaiting_stream();
    }
}

/// The live secondary stream; what a restored Cell's timers re-arm on.
pub fn secondary_stream() -> Option<Arc<FlezhaStream>> {
    SECONDARY_STREAM
        .lock()
        .expect("secondary stream lock poisoned")
        .as_ref()
        .map(Arc::clone)
}

/// One best-effort write to the secondary stream; `false` when it failed.
/// Private to the replay: every other writer names a class through [`write`] or [`send`].
fn send_oneshot(frame: &[u8]) -> bool {
    match SECONDARY_STREAM
        .lock()
        .expect("secondary stream lock poisoned")
        .as_ref()
    {
        Some(stream) => stream.write_frame(frame).is_ok(),
        None => false,
    }
}

fn ring_capacity() -> usize {
    crate::util::env_parse_positive(ENV_RING_CAPACITY, DEFAULT_RING_CAPACITY)
}

fn flood_threshold() -> usize {
    crate::util::env_parse_positive(ENV_FLOOD_THRESHOLD, DEFAULT_FLOOD_THRESHOLD)
}

fn flood_window_ms() -> u64 {
    crate::util::env_parse(ENV_FLOOD_WINDOW_MS, DEFAULT_FLOOD_WINDOW_MS)
}

fn reconnect_retry_ms() -> u64 {
    crate::util::env_parse(ENV_RECONNECT_RETRY_MS, DEFAULT_RECONNECT_RETRY_MS)
}

/// Whether a controller is behind the sink to answer a park or a correction:
/// `false` in a transport-less run and behind [`install_discarding_sink`].
pub fn controller_attached() -> bool {
    let st = STATE.lock().expect("state_sink lock poisoned");
    st.sink.is_some() && !st.discards
}

/// [`Class::Durable`] for a writer with no stream in hand: ship one frame
/// through the live sink, or hold it in the ring as a record the replay
/// re-frames. Never blocks the flusher on a dead stream.
pub fn send(payload_id: &str, frame: Vec<u8>) {
    let should_reconnect = {
        let mut st = STATE.lock().expect("state_sink lock poisoned");
        let outcome = st.sink.as_ref().map(|f| f(payload_id, &frame));
        match outcome {
            Some(Ok(())) => false,
            Some(Err(ref e)) => st.hold_failed("cell-state", Pending::record(payload_id, frame), e),
            None => {
                // Reconnect window: hold the frame, but don't count it as a
                // broken pipe — the stream is intentionally absent.
                st.push_ring(Pending::record(payload_id, frame));
                false
            }
        }
    };
    drive_reconnect_if(should_reconnect);
}

/// Ship one frame only if it can go out now, behind everything the ring holds:
/// `false`, and nothing held, when no sink is installed, the ring still holds
/// a frame, or the write fails. A processed-prefix marker must never overtake
/// a record it covers, nor wait in the ring beside one that may be evicted.
pub fn send_unheld(payload_id: &str, frame: Vec<u8>) -> bool {
    let st = STATE.lock().expect("state_sink lock poisoned");
    match st.sink.as_ref() {
        Some(sink) if st.ring.is_empty() => sink(payload_id, &frame).is_ok(),
        _ => false,
    }
}

/// The guarantee a frame is owed. A writer names the class of the frame it is
/// writing and never what happens when the wire refuses it: [`write`] answers
/// that, once, for every writer on this wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// Refused to its caller and never held: a reply re-driven after its waiter
    /// timed out is a stale answer, and the reconnect re-issues the request it
    /// belonged to.
    Control,
    /// A change record, a snapshot, a delete, a park payload or a shutdown
    /// state ship: held in the bounded ring, drop-oldest counted, replayed in
    /// write order on the next bound stream.
    Durable,
    /// A plugin or steps relay emit: held **already framed**, so a keyed output's
    /// channel and its `flz.*` identity stamps survive the replay — the ring
    /// re-frames a record with empty metadata and would strip them.
    Output,
    /// A heartbeat: owed no delivery at all, because it reports a moment that
    /// has passed. Its verdict is evidence for the caller's flood ladder.
    Probe,
    /// A part: a [`Probe`](Self::Probe) that is also never waited on, so a
    /// stream that cannot take it at once refuses it.
    Preview,
}

impl Class {
    /// What the burst's first line calls this writer.
    const fn kind(self) -> &'static str {
        match self {
            Class::Control => "control",
            Class::Durable => "durable",
            Class::Output => "output",
            Class::Probe => "probe",
            Class::Preview => "preview",
        }
    }
}

/// What a write answered: it reached the wire, the class held it for the next
/// bound stream, or the class holds nothing and this is the refusal.
#[derive(Debug)]
pub enum Verdict {
    Written,
    Held,
    Refused(std::io::Error),
}

impl Verdict {
    /// The refusal a class that holds nothing owes its caller; `Ok` for a frame
    /// that reached the wire or one the class kept.
    pub fn refused(self) -> std::io::Result<()> {
        match self {
            Verdict::Refused(e) => Err(e),
            Verdict::Written | Verdict::Held => Ok(()),
        }
    }
}

/// Write `frame` under its guarantee class — the one seam every writer on this
/// wire goes through, so none of them chooses a policy of its own. `stream` is
/// `None` when nothing is bound, which a class that holds answers exactly as it
/// answers a refused write.
pub fn write(class: Class, stream: Option<&FlezhaStream>, frame: Vec<u8>) -> Verdict {
    let refusal = match stream {
        Some(stream) if class == Class::Preview => stream.try_write_frame(&frame).err(),
        Some(stream) => stream.write_frame(&frame).err(),
        None => Some(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "no stream bound",
        )),
    };

    match (class, refusal) {
        (_, None) => Verdict::Written,
        (Class::Control | Class::Probe | Class::Preview, Some(e)) => Verdict::Refused(e),
        (Class::Durable | Class::Output, Some(e)) => {
            hold_failed_frame(class.kind(), frame, &e);
            Verdict::Held
        }
    }
}

/// Hold a frame a writer framed itself and could not write — `kind` names that
/// writer in the burst's first line. The same ring, the same flood ladder and
/// the same reconnect request [`send`] uses: one wire, one policy for a failed
/// write.
fn hold_failed_frame(kind: &str, frame: Vec<u8>, error: &dyn std::fmt::Display) {
    let should_reconnect = {
        let mut st = STATE.lock().expect("state_sink lock poisoned");
        st.hold_failed(kind, Pending::Framed(frame), error)
    };
    drive_reconnect_if(should_reconnect);
}

/// A frameless transport failure (a heartbeat that reached no stream) on the
/// same flood ladder as [`send`], so an idle runner recovers too.
pub(crate) fn note_transport_failure() {
    let should_reconnect = {
        let mut st = STATE.lock().expect("state_sink lock poisoned");
        st.note_broken_pipe(Instant::now())
    };
    drive_reconnect_if(should_reconnect);
}

fn drive_reconnect_if(should_reconnect: bool) {
    if !should_reconnect {
        return;
    }
    // Re-armed requests re-enter here, so a secondary that stays dead keeps
    // reporting every retry window instead of going silent after one line.
    flz_transport::notice!(
        Warn,
        "outbound write-failure flood crossed threshold ({} frames held); requesting control \
         reconnect ({RECONNECT_REASON})",
        STATE.lock().map(|st| st.ring.len()).unwrap_or(0)
    );
    if let Some(request_reconnect) = RECONNECT_HOOK.get() {
        request_reconnect(RECONNECT_REASON);
    }
}

/// Install or replace the sink, then replay the ring through it.
pub fn install_global_sink(send_fn: SendFrameFn) {
    install(send_fn, false);
}

/// Install a sink that discards every frame, for a run with no controller:
/// durability is removed by the sink, never by branching, so the change log,
/// the fold and the effect path all still run and cost what they cost.
pub fn install_discarding_sink() {
    install(Arc::new(|_payload_id: &str, _frame: &[u8]| Ok(())), true);
}

fn install(send_fn: SendFrameFn, discards: bool) {
    let mut st = STATE.lock().expect("state_sink lock poisoned");
    st.sink = Some(send_fn.clone());
    st.discards = discards;
    st.broken_pipe_at.clear();
    st.reconnect_requested_at = None;
    if st.evicted_since_install > 0 {
        flz_transport::notice!(
            Warn,
            "reconnect ring overflowed while the stream was down; {} frames are lost",
            st.evicted_since_install
        );
        st.evicted_since_install = 0;
    }

    let pending: Vec<Pending> = st.ring.drain(..).collect();
    let mut it = pending.into_iter();
    for entry in it.by_ref() {
        if !replay_held(&send_fn, &entry) {
            // The new stream is already failing — re-buffer this frame and
            // everything after it; a later install replays again.
            st.push_ring(entry);
            for rest in it.by_ref() {
                st.push_ring(rest);
            }
            break;
        }
    }
}

/// One held frame back onto the wire; `false` when the fresh stream refused
/// it. A framed entry rides the live secondary, which an install wires before
/// the sink — the only lock taken while `STATE` is held, and no path takes the
/// two the other way round.
fn replay_held(send_fn: &SendFrameFn, entry: &Pending) -> bool {
    match entry {
        Pending::Record { payload_id, frame } => send_fn(payload_id, frame).is_ok(),
        Pending::Framed(frame) => send_oneshot(frame),
    }
}

/// Ship one saga marker for a Cell: an empty body, the identity and the
/// payload id are the record.
pub fn send_compensation_record(payload_id: &str, cell_identity: &[u8]) {
    let frame = crate::flusher::build_cell_frame(cell_identity, 0, &[0u8; 16], &[]);
    send(payload_id, frame);
}

/// Serializes every test that installs the process-global sink, in this
/// crate and downstream. Take it through [`test_guard`], never directly.
#[doc(hidden)]
pub static SINK_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Take [`SINK_TEST_LOCK`] for the calling test, tolerating a lock a panicking
/// test poisoned: the state it guards is reset by the next `reset_for_test`, so
/// unwrapping the poison would turn one failure into a failure in every test
/// that runs after it. Every `*_for_test` accessor below reads through the same
/// tolerance.
#[doc(hidden)]
pub fn test_guard() -> std::sync::MutexGuard<'static, ()> {
    SINK_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Clear the process-global sink state; hold [`SINK_TEST_LOCK`].
#[cfg(test)]
#[doc(hidden)]
pub(crate) fn reset_for_test() {
    let mut st = STATE.lock().unwrap_or_else(|p| p.into_inner());
    st.sink = None;
    st.discards = false;
    st.ring.clear();
    st.evicted_since_install = 0;
    st.broken_pipe_at.clear();
    st.reconnect_requested_at = None;
}

/// How many frames the ring is holding.
#[cfg(test)]
#[doc(hidden)]
pub(crate) fn ring_len_for_test() -> usize {
    STATE.lock().unwrap_or_else(|p| p.into_inner()).ring.len()
}

/// Whether a reconnect is currently requested.
#[cfg(test)]
#[doc(hidden)]
pub(crate) fn reconnect_requested_for_test() -> bool {
    STATE
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .reconnect_requested_at
        .is_some()
}

#[cfg(test)]
#[path = "state_sink_tests.rs"]
mod tests;
