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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::config::{max_write_buffer_bytes, would_overflow_write_buffer};

/// The buffer must cross this fraction of the cap before the gate rises.
pub const DEFAULT_HIGH_WATER_FRACTION: f64 = 0.8;
/// …and fall back under this one before it lowers again.
pub const DEFAULT_LOW_WATER_FRACTION: f64 = 0.5;

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
    refused: AtomicU64,
    high_water: usize,
    low_water: usize,
    cap: usize,
}

impl SendGate {
    /// The gate a driver stream runs with: watermarks derived from the
    /// configured per-stream cap, so lowering the cap moves both with it.
    pub fn new() -> SendGate {
        let cap = max_write_buffer_bytes();
        SendGate::with_watermarks(
            cap,
            fraction_of(cap, DEFAULT_HIGH_WATER_FRACTION),
            fraction_of(cap, DEFAULT_LOW_WATER_FRACTION),
        )
    }

    /// A gate with watermarks and a ceiling given outright. Tests use it to
    /// build the *ungated* driver — `usize::MAX` throughout — which is the
    /// negative control every gate test needs: a gate that never engages looks
    /// exactly like a gate that works.
    pub fn with_watermarks(cap: usize, high_water: usize, low_water: usize) -> SendGate {
        SendGate {
            raised: AtomicBool::new(false),
            refused: AtomicU64::new(0),
            high_water,
            low_water,
            cap,
        }
    }

    /// The sender's read: one relaxed load, on a path that already crosses
    /// into the NIF per frame.
    pub fn is_raised(&self) -> bool {
        self.raised.load(Ordering::Relaxed)
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
        if self.is_raised() {
            if buffered <= self.low_water {
                self.raised.store(false, Ordering::Relaxed);
                return Some(GateChange::Lowered);
            }
        } else if buffered > self.high_water {
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
