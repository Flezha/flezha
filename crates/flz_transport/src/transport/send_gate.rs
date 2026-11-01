//! The send-buffer gate: one bit per stream, shared by every driver.
//!
//! A driver's send buffer fills when the peer stops draining. The bound is not
//! a policy the transport can decide — the channel policies above it already
//! own what happens to a frame — so the transport's job is to *signal*: it
//! raises the gate when the buffer crosses its high-water mark and lowers it
//! when a drain takes the buffer back under the low-water mark, and the sender
//! reads that one bit before it writes.
//!
//! Below the gate sits a last-resort ceiling at
//! [`crate::transport::config::max_write_buffer_bytes`]. A sender that honours
//! the gate never reaches it, which is what makes [`SendGate::refused`] an
//! invariant rather than a metric: a non-zero value means a frame the sender
//! was told about arrived anyway, and that is a defect to report — not a
//! policy that quietly discarded data.
//!
//! Only the ceiling counts. Being turned away at the gate is the signal
//! working: the sender is told, holds, and offers the frame again — nothing
//! is lost, so nothing is counted.
//!
//! The depth the gate weighs is not the buffer's alone. A driver takes its
//! frames off a queue, so between a sender's write and the buffer that
//! receives it sit however many frames the sender got in while the driver
//! thread was descheduled — a burst can queue hundreds. A gate weighing only
//! what the driver has already buffered is therefore raised after the sender
//! has passed it, and the frames in between arrive at a full buffer and are
//! refused at the ceiling under an answer that said they were sent.
//!
//! So both halves are counted: [`SendGate::queued`] adds a frame's bytes to
//! `inflight` on the sender's own thread and raises the bit there, and the
//! driver [`SendGate::release`]s them as it takes each frame. The gate weighs
//! `buffered + inflight`, which no sender can outrun, because the count the
//! sender is racing is the one it writes itself.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use super::config::{max_write_buffer_bytes, would_overflow_write_buffer};

/// The buffer must cross this fraction of the cap before the gate rises.
pub const DEFAULT_HIGH_WATER_FRACTION: f64 = 0.8;
/// …and fall back under this one before it lowers again.
pub const DEFAULT_LOW_WATER_FRACTION: f64 = 0.5;

/// The marks are fractions of the cap rather than byte counts, so lowering
/// `FLEZHA_MAX_WRITE_BUFFER_BYTES` moves them with it — an absolute mark above
/// a lowered cap is a gate that never rises.
pub const HIGH_WATER_VAR: &str = "FLEZHA_SEND_GATE_HIGH_WATER";
pub const LOW_WATER_VAR: &str = "FLEZHA_SEND_GATE_LOW_WATER";

/// The two marks a gate runs with, read from the environment.
fn configured_fractions() -> (f64, f64) {
    fractions_from(
        crate::env::env_raw(HIGH_WATER_VAR).as_deref(),
        crate::env::env_raw(LOW_WATER_VAR).as_deref(),
    )
}

/// A mark means nothing on its own — hysteresis is the pair — so the pair is
/// refused as a pair: anything but two fractions with `0 < low < high < 1`
/// leaves both defaults standing. One configured mark beside one default is
/// how a low-water mark ends up above its high-water one, which is a gate that
/// raises and lowers on the same byte.
fn fractions_from(high: Option<&str>, low: Option<&str>) -> (f64, f64) {
    match (parse_fraction(high), parse_fraction(low)) {
        (Some(high), Some(low)) if low < high => (high, low),
        _ => (DEFAULT_HIGH_WATER_FRACTION, DEFAULT_LOW_WATER_FRACTION),
    }
}

/// A fraction strictly inside `(0, 1)`. A mark at either end is a gate that
/// cannot signal, so both ends are refused along with anything unparseable.
fn parse_fraction(raw: Option<&str>) -> Option<f64> {
    raw?.trim()
        .parse::<f64>()
        .ok()
        .filter(|f| f.is_finite() && *f > 0.0 && *f < 1.0)
}

/// What one observation of the buffer's depth did to the gate. `None` is the
/// common case: the depth moved and the gate did not.
pub type Transition = Option<GateChange>;

/// The gate crossed a watermark. `Raised` means senders must stop; `Lowered`
/// means the parked sender is woken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateChange {
    Raised,
    Lowered,
}

/// One stream's send-buffer gate, shared between the driver thread that fills
/// the buffer and the sender that feeds it.
#[derive(Debug)]
pub struct SendGate {
    raised: AtomicBool,
    /// Set once the driver has let the stream go. It is read only after a
    /// refusal, so it costs the sender nothing while the gate is down.
    closed: AtomicBool,
    refused: AtomicU64,
    /// Bytes senders have queued that the driver has not yet taken. Written
    /// by both sides — `+` on the sending thread, `-` on the driver's — and
    /// balanced by construction: every [`SendGate::queued`] has exactly one
    /// [`SendGate::release`].
    inflight: AtomicUsize,
    /// The depth the driver last saw, from [`SendGate::observe`].
    buffered: AtomicUsize,
    high_water: usize,
    low_water: usize,
    cap: usize,
}

impl SendGate {
    /// The gate a driver stream runs with: watermarks derived from the
    /// configured per-stream cap, so lowering the cap moves both with it.
    pub fn new() -> SendGate {
        let cap = max_write_buffer_bytes();
        let (high, low) = configured_fractions();
        SendGate::with_watermarks(cap, fraction_of(cap, high), fraction_of(cap, low))
    }

    /// A gate with watermarks and a ceiling given outright, for a test that
    /// needs marks it can reach in a few bytes. [`SendGate::ungated`] is the
    /// one every gate test also needs: a gate that never engages looks exactly
    /// like a gate that works, so the control has to be built too.
    pub fn with_watermarks(cap: usize, high_water: usize, low_water: usize) -> SendGate {
        SendGate {
            raised: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            refused: AtomicU64::new(0),
            inflight: AtomicUsize::new(0),
            buffered: AtomicUsize::new(0),
            high_water,
            low_water,
            cap,
        }
    }

    /// A gate that cannot raise and cannot refuse: every mark and the ceiling
    /// at `usize::MAX`. Two callers need exactly that — the negative control
    /// a gate test must have, and a stream handle whose writer holds no send
    /// buffer of the drivers' kind and so has no depth to signal about.
    pub fn ungated() -> SendGate {
        SendGate::with_watermarks(usize::MAX, usize::MAX, usize::MAX)
    }

    /// The sender's read: one relaxed load, on a path that already crosses
    /// into the NIF per frame.
    pub fn is_raised(&self) -> bool {
        self.raised.load(Ordering::Relaxed)
    }

    /// The driver has released the stream: every later [`SendGate::reserve`]
    /// is refused, and [`SendGate::is_closed`] tells that refusal from a full
    /// buffer. Nothing lowers a raised gate once its driver forgot the stream,
    /// so the refusal is final.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.raise();
    }

    /// Whether a refusal from [`SendGate::reserve`] is the stream being gone
    /// rather than its buffer being full.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// What the gate weighs: the driver's last reading plus what senders have
    /// queued past it.
    fn pending(&self) -> usize {
        self.buffered
            .load(Ordering::Relaxed)
            .saturating_add(self.inflight.load(Ordering::Relaxed))
    }

    /// A sender has handed `len` bytes to the driver's queue. One `fetch_add`
    /// and one relaxed load, on the sending thread — which is the only place
    /// the crossing can be seen before the frames that caused it arrive.
    ///
    /// The frame that crosses the mark is taken; the sender learns from
    /// [`SendGate::is_raised`] on its next one, which is the same answer it
    /// gets when the driver raises the gate on its own depth.
    pub fn queued(&self, len: usize) -> Transition {
        self.inflight.fetch_add(len, Ordering::Relaxed);
        if !self.is_raised() && self.pending() > self.high_water {
            self.raised.store(true, Ordering::Relaxed);
            return Some(GateChange::Raised);
        }
        None
    }

    /// The driver has taken `len` queued bytes off the sender's hands — or the
    /// queue refused them and they were never sent. Saturating, so a ledger
    /// broken by a future caller reads as zero rather than as `usize::MAX`.
    /// Reserve `len` bytes for a frame a sender is about to queue, or refuse
    /// it: while the gate is raised, and when the frame would not fit under
    /// the ceiling beside what is already pending, which the driver would take
    /// and then drop behind an answer that said it was sent. A frame refused
    /// for the ceiling raises the gate, so its sender waits for the next drain.
    pub fn reserve(&self, len: usize) -> bool {
        if self.is_raised() {
            return false;
        }
        if self.over_ceiling(self.pending(), len) {
            self.raise();
            return false;
        }
        self.queued(len);
        true
    }

    pub fn release(&self, len: usize) {
        if len == 0 {
            return;
        }
        let _ = self
            .inflight
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
                Some(held.saturating_sub(len))
            });
    }

    /// Record a frame this stream discarded at the ceiling. Private because
    /// [`SendGate::offer`] is the one place a buffer can turn a frame away,
    /// which is what makes the count an invariant rather than a tally several
    /// callers contribute to.
    fn refuse(&self) {
        self.refused.fetch_add(1, Ordering::Relaxed);
    }

    pub fn refused(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    /// Raise the gate on a signal that is not a byte count — QUIC's
    /// `WriteError::Blocked` is the peer's flow-control window being full, so
    /// the protocol has already computed the backpressure.
    pub fn raise(&self) -> Transition {
        match self.raised.swap(true, Ordering::Relaxed) {
            false => Some(GateChange::Raised),
            true => None,
        }
    }

    /// Take one reading of the buffer's depth and move the gate if it crossed
    /// a watermark. Hysteresis is the point: raising at `high` and lowering at
    /// `low` keeps a buffer hovering at the mark from flapping the sender.
    pub fn observe(&self, buffered: usize) -> Transition {
        self.buffered.store(buffered, Ordering::Relaxed);
        let pending = self.pending();
        if self.is_raised() {
            if pending <= self.low_water {
                self.raised.store(false, Ordering::Relaxed);
                return Some(GateChange::Lowered);
            }
        } else if pending > self.high_water {
            self.raised.store(true, Ordering::Relaxed);
            return Some(GateChange::Raised);
        }
        None
    }

    /// The last-resort ceiling, in the words of the one predicate every driver
    /// shares. A sender that honours the gate never reaches it.
    pub fn over_ceiling(&self, buffered: usize, incoming: usize) -> bool {
        would_overflow_write_buffer(buffered, incoming, self.cap)
    }

    /// Offer `data` to a send buffer, refusing it at the ceiling. Answers
    /// whether the buffer took it. This is the whole of what a driver does
    /// with an outbound frame before it flushes, and both drivers do it here.
    pub fn offer(&self, buffer: &mut Vec<u8>, data: &[u8]) -> bool {
        if self.over_ceiling(buffer.len(), data.len()) {
            self.refuse();
            return false;
        }
        buffer.extend_from_slice(data);
        true
    }
}

impl Default for SendGate {
    fn default() -> Self {
        SendGate::new()
    }
}

/// A watermark from a fraction of the cap, never zero and never the cap
/// itself: a mark at either end is a gate that cannot signal.
fn fraction_of(cap: usize, fraction: f64) -> usize {
    let mark = (cap as f64 * fraction) as usize;
    mark.clamp(1, cap.saturating_sub(1).max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_gate_rises_at_the_high_mark_and_lowers_only_at_the_low_one() {
        let gate = SendGate::with_watermarks(1000, 800, 500);

        assert_eq!(gate.observe(799), None, "under the mark, nothing happens");
        assert!(!gate.is_raised());

        assert_eq!(gate.observe(801), Some(GateChange::Raised));
        assert!(gate.is_raised());

        // Hysteresis: back under the HIGH mark is not enough to lower it.
        assert_eq!(gate.observe(600), None);
        assert!(gate.is_raised());

        assert_eq!(gate.observe(500), Some(GateChange::Lowered));
        assert!(!gate.is_raised());
    }

    #[test]
    fn raising_an_already_raised_gate_reports_no_transition() {
        let gate = SendGate::with_watermarks(1000, 800, 500);

        assert_eq!(gate.raise(), Some(GateChange::Raised));
        assert_eq!(gate.raise(), None, "one wake per crossing, not per signal");
        assert_eq!(gate.observe(400), Some(GateChange::Lowered));
    }

    #[test]
    fn the_ceiling_is_the_shared_predicate_and_counts_what_it_refuses() {
        let gate = SendGate::with_watermarks(1000, 800, 500);

        assert!(
            !gate.over_ceiling(0, 4096),
            "an empty buffer always accepts"
        );
        assert!(!gate.over_ceiling(500, 500));
        assert!(gate.over_ceiling(500, 501));

        assert_eq!(gate.refused(), 0);
        gate.refuse();
        assert_eq!(gate.refused(), 1);
    }

    #[test]
    fn an_offer_over_the_ceiling_is_refused_and_leaves_the_buffer_alone() {
        let gate = SendGate::with_watermarks(1000, 800, 500);
        let mut buffer = Vec::new();

        assert!(gate.offer(&mut buffer, &[0u8; 900]));
        assert_eq!(buffer.len(), 900);

        assert!(!gate.offer(&mut buffer, &[0u8; 200]));
        assert_eq!(buffer.len(), 900, "the refused frame did not land");
        assert_eq!(gate.refused(), 1);
    }

    #[test]
    fn a_sender_raises_the_gate_on_what_it_has_queued_not_on_what_arrived() {
        let gate = SendGate::with_watermarks(1000, 800, 500);

        // The driver has seen nothing at all: every one of these frames is
        // still sitting in its command queue.
        for _ in 0..8 {
            assert_eq!(gate.queued(100), None);
            assert!(!gate.is_raised());
        }

        assert_eq!(gate.queued(100), Some(GateChange::Raised));
        assert!(
            gate.is_raised(),
            "900 bytes queued past an 800-byte mark raises it before any arrive"
        );
    }

    // fails when: a sender is told its frame went while what is pending
    // leaves it no room under the ceiling, and the driver then drops it.
    #[test]
    fn a_frame_that_would_not_fit_beside_the_pending_bytes_is_refused_and_raises() {
        let gate = SendGate::with_watermarks(1000, 800, 500);
        let mut buffer = Vec::new();
        assert!(gate.offer(&mut buffer, &[0u8; 900]));
        assert_eq!(gate.observe(400), None);
        assert!(!gate.is_raised(), "400 pending is under the low mark");

        assert!(
            !gate.reserve(700),
            "400 pending + 700 is over the 1000 ceiling"
        );
        assert!(
            gate.is_raised(),
            "the refusal holds the sender until a drain"
        );

        assert_eq!(gate.observe(0), Some(GateChange::Lowered));
        assert!(gate.reserve(700), "an empty buffer takes it");
    }

    #[test]
    fn a_released_frame_stops_being_counted_twice() {
        let gate = SendGate::with_watermarks(1000, 800, 500);
        let mut buffer = Vec::new();

        gate.queued(600);
        gate.release(600);
        assert!(gate.offer(&mut buffer, &[0u8; 600]));

        // 600 buffered, nothing queued: were the released frame still counted
        // as in flight this would read 1200 and raise.
        assert_eq!(gate.observe(buffer.len()), None);
        assert!(!gate.is_raised());

        gate.queued(300);
        assert!(gate.is_raised(), "600 buffered + 300 queued crosses 800");
    }

    #[test]
    fn a_gate_lowers_only_once_the_queue_behind_it_has_drained_too() {
        let gate = SendGate::with_watermarks(1000, 800, 500);

        gate.queued(900);
        assert!(gate.is_raised());

        // The driver's buffer is empty — but 900 bytes are still queued for it.
        assert_eq!(
            gate.observe(0),
            None,
            "an empty buffer is not an empty gate"
        );
        assert!(gate.is_raised());

        gate.release(900);
        assert_eq!(gate.observe(0), Some(GateChange::Lowered));
    }

    #[test]
    fn a_closed_gate_refuses_every_frame_and_says_why() {
        let gate = SendGate::with_watermarks(1000, 800, 500);
        assert!(gate.reserve(10));
        assert!(!gate.is_closed());

        gate.close();

        assert!(!gate.reserve(10), "a released stream takes nothing");
        assert!(gate.is_closed());
    }

    #[test]
    fn an_ungated_gate_never_raises_however_much_is_queued() {
        let gate = SendGate::ungated();

        for _ in 0..1000 {
            assert_eq!(gate.queued(1 << 20), None);
        }
        assert!(!gate.is_raised(), "the negative control has to stay open");

        let mut buffer = vec![0u8; 1 << 20];
        assert!(gate.offer(&mut buffer, &[0u8; 1 << 20]));
        assert_eq!(gate.refused(), 0);
    }

    #[test]
    fn releasing_more_than_is_held_reads_as_nothing_held() {
        let gate = SendGate::with_watermarks(1000, 800, 500);

        gate.queued(100);
        gate.release(400);

        assert_eq!(gate.observe(400), None, "not 400 + a wrapped-around usize");
        assert!(!gate.is_raised());
    }

    #[test]
    fn a_watermark_pair_is_taken_whole_or_refused_whole() {
        let defaults = (DEFAULT_HIGH_WATER_FRACTION, DEFAULT_LOW_WATER_FRACTION);

        assert_eq!(fractions_from(Some("0.9"), Some("0.2")), (0.9, 0.2));

        // The refusals, which are the half worth testing.
        assert_eq!(
            fractions_from(Some("0.9"), None),
            defaults,
            "one mark alone"
        );
        assert_eq!(fractions_from(None, Some("0.2")), defaults, "the other one");
        assert_eq!(
            fractions_from(Some("0.2"), Some("0.9")),
            defaults,
            "a low mark above the high one raises and lowers on the same byte"
        );
        assert_eq!(
            fractions_from(Some("0.5"), Some("0.5")),
            defaults,
            "and marks that meet are the same gate with extra steps"
        );
        assert_eq!(
            fractions_from(Some("1.0"), Some("0.5")),
            defaults,
            "at the cap"
        );
        assert_eq!(fractions_from(Some("0.9"), Some("0")), defaults, "at zero");
        assert_eq!(
            fractions_from(Some("0.9"), Some("-0.1")),
            defaults,
            "below it"
        );
        assert_eq!(
            fractions_from(Some("high"), Some("0.2")),
            defaults,
            "unparseable"
        );
        assert_eq!(
            fractions_from(Some("0.9"), Some("NaN")),
            defaults,
            "not finite"
        );
    }

    #[test]
    fn watermarks_stay_inside_a_cap_small_enough_to_collapse_them() {
        let gate = SendGate::with_watermarks(
            1,
            fraction_of(1, DEFAULT_HIGH_WATER_FRACTION),
            fraction_of(1, DEFAULT_LOW_WATER_FRACTION),
        );

        // A one-byte cap cannot express two marks; it must still signal rather
        // than sit permanently raised or permanently lowered.
        assert_eq!(gate.observe(0), None);
        assert_eq!(gate.observe(2), Some(GateChange::Raised));
    }
}
