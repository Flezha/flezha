//! Late-input detection and the correction request. Only the SDK can see an
//! event time, so detection is here and orchestration is the controller's;
//! the seam is one fire-and-forget frame carrying no offsets, because the
//! late input is already in the durable input log and the controller
//! re-forwards the slice in event-time order.

use serde::{Deserialize, Serialize};

use crate::flusher::build_entity_frame;
use crate::state_sink;

/// A correction request: `park_payload`'s frame shape with a postcard body.
pub const CORRECTION_REQUEST_PAYLOAD_ID: &str = "correction_request";

/// Wire body of a `correction_request`; the identity rides the envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrectionRequestBody {
    /// The late input's event time; the controller rewinds to before it.
    pub target_event_time: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lateness {
    /// At or ahead of the high-water.
    OnTime,
    /// Behind the high-water, inside the window: request a correction; the
    /// input comes back in the replayed slice.
    Correctable,
    /// Behind by more than the window; the declared policy decides.
    BeyondWindow,
}

/// Classify an input against the high-water; `-1` before anything was
/// dispatched, so a first input is never late.
pub fn classify(event_time: u64, high_water: i64, window_ms: u64) -> Lateness {
    if high_water < 0 || event_time >= high_water as u64 {
        return Lateness::OnTime;
    }
    if high_water as u64 - event_time <= window_ms {
        Lateness::Correctable
    } else {
        Lateness::BeyondWindow
    }
}

/// Ship a correction request, fire-and-forget: the controller coalesces
/// duplicates and a lost one is re-raised on re-delivery. `false` in a
/// transport-less run, where the caller dispatches the input as an arrival.
pub fn request(entity_identity: &[u8], target_event_time: u64) -> bool {
    if !state_sink::sink_installed() {
        return false;
    }
    let body = CorrectionRequestBody { target_event_time };
    let body_bytes = postcard::to_allocvec(&body)
        .expect("postcard serialization of CorrectionRequestBody cannot fail");
    let frame = build_entity_frame(entity_identity, 0, &[0u8; 16], &body_bytes);
    state_sink::send(CORRECTION_REQUEST_PAYLOAD_ID, frame);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: u64 = 600_000;

    #[test]
    fn a_fresh_entity_has_no_late_inputs() {
        assert_eq!(classify(1_000, -1, WINDOW), Lateness::OnTime);
    }

    #[test]
    fn at_or_ahead_of_the_high_water_is_on_time() {
        assert_eq!(classify(5_000, 5_000, WINDOW), Lateness::OnTime);
        assert_eq!(classify(5_001, 5_000, WINDOW), Lateness::OnTime);
    }

    #[test]
    fn inside_the_window_is_correctable() {
        assert_eq!(classify(4_999, 5_000, WINDOW), Lateness::Correctable);
        // Exactly at the window edge is still correctable.
        assert_eq!(classify(1, 600_001, WINDOW), Lateness::Correctable);
    }

    #[test]
    fn beyond_the_window_is_beyond_the_window() {
        assert_eq!(classify(1, 600_002, WINDOW), Lateness::BeyondWindow);
    }
}
