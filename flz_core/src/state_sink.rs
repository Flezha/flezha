//! The process-global send path for durable records, and the holding pen every
//! writer on the outbound wire shares: [`send`] resolves the live sink on every
//! call, a frame that cannot be written goes into a drop-oldest ring — through
//! [`send`] for a record, through [`write_or_hold`] for a writer that framed its
//! own payload and [`hold_failed_frame`] for one with no stream to try — and
//! [`install_global_sink`] replays the ring onto
//! the stream a reconnect wires, so nothing produced mid-reconnect is lost. A
//! sustained run of write failures is a dead data stream under a live control
//! channel, and asks the coordinator for a full reconnect. An eviction is the
//! one outcome that loses a frame, and it is counted as a drop-oldest shed.

use std::collections::VecDeque;
use std::io::Write;
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

/// A per-Entity change record.
pub const ENTITY_STATE_CHANGE_PAYLOAD_ID: &str = "entity_state_change";

/// An effect record (effectively-once tier): `entity_state_change`'s frame
/// with a `format_id = 2` header and an `EffectRecordBody`.
pub const ENTITY_EFFECT_RECORD_PAYLOAD_ID: &str = "entity_effect_record";

/// The dynamic-mode hard delete; the body is the `entity_id` bytes.
pub const DELETE_ENTITY_STATE_PAYLOAD_ID: &str = "delete_entity_state";

/// The saga markers: a procedure's completion, and a compensation hook's.
pub const COMPENSATION_INTENT_PAYLOAD_ID: &str = "compensation_intent";
pub const COMPENSATION_DONE_PAYLOAD_ID: &str = "compensation_done";

/// A tier-off park: `entity_state_change`'s frame with a `ParkBody`,
/// answered by `ParkPayloadAck` once fsynced.
pub const PARK_PAYLOAD_PAYLOAD_ID: &str = "park_payload";

/// A compaction snapshot at a `change_seq`; the controller truncates the WAL
/// up to it.
pub const ENTITY_STATE_SNAPSHOT_PAYLOAD_ID: &str = "entity_state_snapshot";

const ENV_RING_CAPACITY: &str = "FLEZHA_RECONNECT_RING_CAPACITY";
const DEFAULT_RING_CAPACITY: usize = 1024;
const ENV_FLOOD_THRESHOLD: &str = "FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD";
const DEFAULT_FLOOD_THRESHOLD: usize = 10;
const ENV_FLOOD_WINDOW_MS: &str = "FLEZHA_BROKEN_PIPE_WINDOW_MS";
const DEFAULT_FLOOD_WINDOW_MS: u64 = 5_000;
const ENV_RECONNECT_RETRY_MS: &str = "FLEZHA_RECONNECT_REQUEST_RETRY_MS";
const DEFAULT_RECONNECT_RETRY_MS: u64 = 30_000;

const RECONNECT_REASON: &str = "broken_pipe_flood";

/// A frame the wire could not take. A record is replayed through the sink
/// that frames it; a framed payload already carries its own payload id and
/// metadata, so it is replayed onto the live secondary exactly as it stands.
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
            ring: VecDeque::new(),
            evicted_since_install: 0,
            broken_pipe_at: VecDeque::new(),
            reconnect_requested_at: None,
        }
    }

    fn push_ring(&mut self, entry: Pending) {
        if self.ring.len() >= ring_capacity() && self.ring.pop_front().is_some() {
            // The ring holds the only copy: an eviction is a lost frame, and
            // it is counted where every drop-oldest shed on this wire is.
            crate::entity::note_dropped_oldest();
            self.evicted_since_install += 1;
            if self.evicted_since_install == 1 {
                tracing::warn!(
                    target: "flezha::transport",
                    capacity = ring_capacity(),
                    "reconnect ring full; dropping the oldest outbound frame"
                );
            }
        }
        self.ring.push_back(entry);
    }

    /// The posture every writer on this wire shares for a failed write: one
    /// line to open the burst, the frame held, and the flood verdict back.
    fn hold_failed(&mut self, kind: &str, entry: Pending, error: &dyn std::fmt::Display) -> bool {
        if self.broken_pipe_at.is_empty() {
            tracing::warn!(
                target: "flezha::transport",
                ring_len = self.ring.len() + 1,
                "outbound {} write failed; buffering into reconnect ring: {}",
                kind,
                error
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

/// The live secondary stream, for fire-and-forget sends: no ring, no flood
/// detection.
static SECONDARY_STREAM: Mutex<Option<Arc<FlezhaStream>>> = Mutex::new(None);

pub fn set_secondary_stream(stream: Option<Arc<FlezhaStream>>) {
    *SECONDARY_STREAM
        .lock()
        .expect("secondary stream lock poisoned") = stream;
}

/// The live secondary stream; what a restored Entity's timers re-arm on.
pub fn secondary_stream() -> Option<Arc<FlezhaStream>> {
    SECONDARY_STREAM
        .lock()
        .expect("secondary stream lock poisoned")
        .as_ref()
        .map(Arc::clone)
}

/// One best-effort write to the secondary stream; `false` when it failed.
pub fn send_oneshot(frame: &[u8]) -> bool {
    match SECONDARY_STREAM
        .lock()
        .expect("secondary stream lock poisoned")
        .as_ref()
    {
        Some(stream) => (&**stream).write_all(frame).is_ok(),
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

/// Whether a sink was ever installed — `false` only in transport-less runs.
pub fn sink_installed() -> bool {
    STATE
        .lock()
        .expect("state_sink lock poisoned")
        .sink
        .is_some()
}

/// Ship one frame through the live sink, or hold it in the ring; never
/// blocks the flusher on a dead stream.
pub fn send(payload_id: &str, frame: Vec<u8>) {
    let should_reconnect = {
        let mut st = STATE.lock().expect("state_sink lock poisoned");
        let outcome = st.sink.as_ref().map(|f| f(payload_id, &frame));
        match outcome {
            Some(Ok(())) => false,
            Some(Err(ref e)) => {
                st.hold_failed("entity-state", Pending::record(payload_id, frame), e)
            }
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

/// Write a framed payload to `stream`, or hold it — the whole posture a writer
/// on this wire takes, so none of them re-implements the failure half. `kind`
/// names the writer in the burst's first line.
pub fn write_or_hold(kind: &str, stream: &FlezhaStream, frame: Vec<u8>) {
    let mut w: &FlezhaStream = stream;
    if let Err(e) = w.write_all(&frame) {
        hold_failed_frame(kind, frame, &e);
    }
}

/// Hold a frame a writer framed itself and could not write — `kind` names that
/// writer in the burst's first line. The same ring, the same flood ladder and
/// the same reconnect request [`send`] uses: one wire, one policy for a failed
/// write. [`write_or_hold`] is the form to reach for with a stream in hand.
pub fn hold_failed_frame(kind: &str, frame: Vec<u8>, error: &dyn std::fmt::Display) {
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
    tracing::warn!(
        target: "flezha::transport",
        reason = RECONNECT_REASON,
        ring_len = STATE.lock().map(|st| st.ring.len()).unwrap_or(0),
        "outbound write-failure flood crossed threshold; requesting control reconnect"
    );
    if let Some(request_reconnect) = RECONNECT_HOOK.get() {
        request_reconnect(RECONNECT_REASON);
    }
}

/// Install or replace the sink, then replay the ring through it.
pub fn install_global_sink(send_fn: SendFrameFn) {
    let mut st = STATE.lock().expect("state_sink lock poisoned");
    st.sink = Some(send_fn.clone());
    st.broken_pipe_at.clear();
    st.reconnect_requested_at = None;
    if st.evicted_since_install > 0 {
        tracing::warn!(
            target: "flezha::transport",
            dropped = st.evicted_since_install,
            "reconnect ring overflowed while the stream was down; those frames are lost"
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

/// Ship one saga marker for an Entity: an empty body, the identity and the
/// payload id are the record.
pub fn send_compensation_record(payload_id: &str, entity_identity: &[u8]) {
    let frame = crate::flusher::build_entity_frame(entity_identity, 0, &[0u8; 16], &[]);
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
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    use super::{reset_for_test as reset_state, *};
    use crate::flusher::SendError;

    /// A sink that records every frame; `fail` flips it to `TransportDown`.
    fn recording_sink(fail: Arc<AtomicBool>) -> (SendFrameFn, Arc<StdMutex<Vec<Vec<u8>>>>) {
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let seen_c = Arc::clone(&seen);
        let f: SendFrameFn = Arc::new(move |_payload_id: &str, frame: &[u8]| {
            if fail.load(Ordering::SeqCst) {
                Err(SendError::TransportDown("test".into()))
            } else {
                seen_c.lock().unwrap().push(frame.to_vec());
                Ok(())
            }
        });
        (f, seen)
    }

    const PID: &str = super::ENTITY_STATE_CHANGE_PAYLOAD_ID;

    #[test]
    fn frames_with_no_sink_buffer_and_replay_on_install() {
        let _g = super::test_guard();
        reset_state();

        send(PID, b"a".to_vec());
        send(PID, b"b".to_vec());

        let (sink, seen) = recording_sink(Arc::new(AtomicBool::new(false)));
        install_global_sink(sink);

        assert_eq!(
            *seen.lock().unwrap(),
            vec![b"a".to_vec(), b"b".to_vec()],
            "ring replays in order"
        );
        reset_state();
    }

    #[test]
    fn live_sink_sends_directly_without_buffering() {
        let _g = super::test_guard();
        reset_state();

        let (sink, seen) = recording_sink(Arc::new(AtomicBool::new(false)));
        install_global_sink(sink);
        send(PID, b"x".to_vec());

        assert_eq!(*seen.lock().unwrap(), vec![b"x".to_vec()]);
        assert!(
            STATE.lock().unwrap().ring.is_empty(),
            "no buffering on success"
        );
        reset_state();
    }

    #[test]
    fn failed_writes_buffer_then_replay_when_stream_recovers() {
        let _g = super::test_guard();
        reset_state();

        let fail = Arc::new(AtomicBool::new(true));
        let (sink, _seen) = recording_sink(Arc::clone(&fail));
        install_global_sink(sink);

        send(PID, b"1".to_vec());
        send(PID, b"2".to_vec());
        assert_eq!(STATE.lock().unwrap().ring.len(), 2);

        // Stream recovers; a fresh install replays the ring in order.
        let (sink2, seen2) = recording_sink(Arc::new(AtomicBool::new(false)));
        install_global_sink(sink2);

        assert_eq!(*seen2.lock().unwrap(), vec![b"1".to_vec(), b"2".to_vec()]);
        assert!(STATE.lock().unwrap().ring.is_empty());
        reset_state();
    }

    #[test]
    fn ring_drops_oldest_beyond_capacity() {
        let _g = super::test_guard();
        std::env::set_var(ENV_RING_CAPACITY, "3");
        reset_state();

        for i in 0..5u8 {
            send(PID, vec![i]); // no sink → ring
        }
        let ring: Vec<Vec<u8>> = STATE
            .lock()
            .unwrap()
            .ring
            .iter()
            .map(|entry| match entry {
                Pending::Record { frame, .. } => frame.clone(),
                Pending::Framed(frame) => frame.clone(),
            })
            .collect();
        assert_eq!(
            ring,
            vec![vec![2u8], vec![3u8], vec![4u8]],
            "oldest dropped"
        );

        std::env::remove_var(ENV_RING_CAPACITY);
        reset_state();
    }

    /// The ring holds the frame's last copy, so an eviction is a real loss —
    /// and the heartbeat's `drops_oldest` is where this wire's shedding is
    /// already reported.
    #[test]
    fn a_ring_eviction_counts_as_a_drop_oldest_shed() {
        let _g = super::test_guard();
        std::env::set_var(ENV_RING_CAPACITY, "1");
        reset_state();

        let before = crate::entity::drops_oldest_total();
        send(PID, b"first".to_vec());
        send(PID, b"second".to_vec());

        assert!(
            crate::entity::drops_oldest_total() > before,
            "an evicted frame must reach the counter the heartbeat reports"
        );
        assert_eq!(
            STATE.lock().unwrap().evicted_since_install,
            1,
            "one eviction, reported once the stream comes back"
        );

        std::env::remove_var(ENV_RING_CAPACITY);
        reset_state();
    }

    #[test]
    fn broken_pipe_flood_crosses_threshold_once_per_window() {
        let _g = super::test_guard();
        std::env::set_var(ENV_FLOOD_THRESHOLD, "3");
        std::env::set_var(ENV_FLOOD_WINDOW_MS, "60000");
        reset_state();

        let mut st = STATE.lock().unwrap();
        let now = Instant::now();
        assert!(!st.note_broken_pipe(now));
        assert!(!st.note_broken_pipe(now));
        assert!(st.note_broken_pipe(now), "3rd failure crosses threshold");
        // Already requested: no second trigger while that request is still
        // plausibly in flight (see the re-arm test for what happens after).
        assert!(!st.note_broken_pipe(now));
        drop(st);

        std::env::remove_var(ENV_FLOOD_THRESHOLD);
        std::env::remove_var(ENV_FLOOD_WINDOW_MS);
        reset_state();
    }

    #[test]
    fn send_through_failing_sink_drives_flood_branch_without_trigger() {
        // Exercises send()'s flood path end-to-end: a failing sink past the
        // threshold returns `should_reconnect`, and the request_reconnect
        // call is a safe no-op when no coordinator trigger is installed.
        let _g = super::test_guard();
        std::env::set_var(ENV_FLOOD_THRESHOLD, "3");
        std::env::set_var(ENV_FLOOD_WINDOW_MS, "60000");
        reset_state();

        let (sink, _seen) = recording_sink(Arc::new(AtomicBool::new(true)));
        install_global_sink(sink);
        for i in 0..5u8 {
            send(PID, vec![i]); // every write fails → buffered + counted
        }
        // All five frames buffered (drop-oldest not reached at default cap).
        assert_eq!(STATE.lock().unwrap().ring.len(), 5);

        std::env::remove_var(ENV_FLOOD_THRESHOLD);
        std::env::remove_var(ENV_FLOOD_WINDOW_MS);
        reset_state();
    }

    /// A requested reconnect that never lands must not disarm the detector
    /// forever: the ring would absorb every later record silently.
    #[test]
    fn unanswered_reconnect_request_rearms_so_the_stall_cannot_be_permanent() {
        let _g = super::test_guard();
        std::env::set_var(ENV_FLOOD_THRESHOLD, "3");
        std::env::set_var(ENV_FLOOD_WINDOW_MS, "60000");
        reset_state();

        let mut st = STATE.lock().unwrap();
        let t0 = Instant::now();
        assert!(!st.note_broken_pipe(t0));
        assert!(!st.note_broken_pipe(t0));
        assert!(st.note_broken_pipe(t0), "3rd failure requests a reconnect");

        // No `install_global_sink` follows — the reconnect never landed. Ten
        // minutes later the sink is still dead and writes are still failing.
        let much_later = t0 + Duration::from_secs(600);
        st.note_broken_pipe(much_later);
        st.note_broken_pipe(much_later);
        assert!(
            st.note_broken_pipe(much_later),
            "a still-dead sink long past the retry window must request again"
        );
        drop(st);

        std::env::remove_var(ENV_FLOOD_THRESHOLD);
        std::env::remove_var(ENV_FLOOD_WINDOW_MS);
        reset_state();
    }

    #[test]
    fn stale_broken_pipes_age_out_of_window() {
        let _g = super::test_guard();
        std::env::set_var(ENV_FLOOD_THRESHOLD, "2");
        std::env::set_var(ENV_FLOOD_WINDOW_MS, "100");
        reset_state();

        let mut st = STATE.lock().unwrap();
        let t0 = Instant::now();
        assert!(!st.note_broken_pipe(t0));
        // A failure past the window evicts t0, so this counts as the 1st.
        let later = t0 + Duration::from_millis(200);
        assert!(
            !st.note_broken_pipe(later),
            "stale failure aged out, no flood"
        );
        drop(st);

        std::env::remove_var(ENV_FLOOD_THRESHOLD);
        std::env::remove_var(ENV_FLOOD_WINDOW_MS);
        reset_state();
    }
}
