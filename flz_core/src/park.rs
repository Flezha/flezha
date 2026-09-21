//! The dead-letter park outside the effectively-once tier: a `park_payload`
//! frame plus a wait for the controller's `ParkPayloadAck`, re-sent until
//! acked. The invariant: the cell advances only once the park is durable
//! — delay, never loss. Under the tier the park rides the effect record.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::flusher::build_cell_frame;
use crate::state_sink::{self, PARK_PAYLOAD_PAYLOAD_ID};

/// Retry attempts, process-wide; rides the heartbeat.
pub static RETRIES_TOTAL: AtomicU64 = AtomicU64::new(0);

pub static PARKED_TOTAL: AtomicU64 = AtomicU64::new(0);

static PARK_SEQ: AtomicU64 = AtomicU64::new(0);

/// Wire body of a `park_payload` frame; field order is the contract, and
/// the entry is the same [`DlqEntryBody`] a tier-on park embeds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParkBody {
    /// Echoed in `ParkPayloadAck`; the controller's dedup handle for re-sends.
    pub park_id: String,
    pub origin_offset: Option<u64>,
    pub entry: crate::flusher::DlqEntryBody,
}

/// Acked park ids, consumed as their waiters wake.
static ACKED: Mutex<Option<HashSet<String>>> = Mutex::new(None);
static ACKED_CV: Condvar = Condvar::new();

const ACK_RESEND_INTERVAL: Duration = Duration::from_secs(2);
/// Warn every this many re-sends.
const WARN_EVERY_RESENDS: u64 = 5;

pub fn mint_park_id(cell_label: &str) -> String {
    format!(
        "{}#{}",
        cell_label,
        PARK_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// Record an ack and wake its waiter; an ack ahead of its waiter is kept.
pub fn complete_ack(park_id: &str) {
    let mut guard = ACKED.lock().expect("park ack registry poisoned");
    guard
        .get_or_insert_with(HashSet::new)
        .insert(park_id.to_string());
    ACKED_CV.notify_all();
}

/// `true` exactly once per ack.
fn take_ack(park_id: &str) -> bool {
    let mut guard = ACKED.lock().expect("park ack registry poisoned");
    guard
        .as_mut()
        .map(|set| set.remove(park_id))
        .unwrap_or(false)
}

/// The wire bytes of an input for the DLQ entry; only `Vec<u8>` / `String`
/// entries round-trip, the rest park without bytes.
pub fn extract_payload_bytes<T: std::any::Any>(input: &T) -> Option<Vec<u8>> {
    let any: &dyn std::any::Any = input;
    if let Some(bytes) = any.downcast_ref::<Vec<u8>>() {
        Some(bytes.clone())
    } else {
        any.downcast_ref::<String>().map(|s| s.as_bytes().to_vec())
    }
}

/// Ship a park and block until it is acked durable, re-sending forever; a
/// transport-less run logs and drops.
pub fn park_blocking(cell_identity: &[u8], body: &ParkBody) {
    if !state_sink::sink_installed() {
        eprintln!(
            "[flezha] DLQ park impossible without a controller transport; dropping poison \
             payload on channel `{}` after {} attempts: {}",
            body.entry.channel, body.entry.attempts, body.entry.error
        );
        return;
    }

    let body_bytes =
        postcard::to_allocvec(body).expect("postcard serialization of ParkBody cannot fail");
    let seq = PARK_SEQ.load(Ordering::Relaxed);
    let frame = build_cell_frame(cell_identity, seq, &[0u8; 16], &body_bytes);

    let mut resends: u64 = 0;
    loop {
        state_sink::send(PARK_PAYLOAD_PAYLOAD_ID, frame.clone());
        let deadline = std::time::Instant::now() + ACK_RESEND_INTERVAL;
        loop {
            if take_ack(&body.park_id) {
                return;
            }
            let guard = ACKED.lock().expect("park ack registry poisoned");
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (_guard, _timeout) = ACKED_CV
                .wait_timeout(guard, remaining)
                .expect("park ack registry poisoned");
        }
        resends += 1;
        if resends.is_multiple_of(WARN_EVERY_RESENDS) {
            tracing::warn!(
                target: "flezha::dlq",
                park_id = %body.park_id,
                channel = %body.entry.channel,
                resends,
                "dead-letter park still unacked; cell held (re-sending)"
            );
        }
    }
}

/// Mint, build and ship one tier-off park, blocking for its ack.
pub fn park_tier_off(
    cell_identity: &[u8],
    cell_label: &str,
    origin_offset: Option<u64>,
    entry: crate::flusher::DlqEntryBody,
) {
    let body = ParkBody {
        park_id: mint_park_id(cell_label),
        origin_offset,
        entry,
    };
    park_blocking(cell_identity, &body);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_registry_round_trips_and_consumes_once() {
        complete_ack("seg#1");
        assert!(take_ack("seg#1"));
        assert!(!take_ack("seg#1"), "ack is consumed exactly once");
        assert!(!take_ack("seg#never"), "unknown id is not acked");
    }

    #[test]
    fn payload_bytes_extraction_covers_entry_types() {
        assert_eq!(
            extract_payload_bytes(&b"raw".to_vec()),
            Some(b"raw".to_vec())
        );
        assert_eq!(
            extract_payload_bytes(&"text".to_string()),
            Some(b"text".to_vec())
        );
        #[derive(Clone)]
        struct Opaque;
        assert_eq!(extract_payload_bytes(&Opaque), None);
    }

    #[test]
    fn park_body_postcard_bytes_are_the_cross_language_fixture() {
        // Shared fixture with the controller's ParkBody decode
        // (wire_test.exs); an encoding change breaks loudly here.
        let body = ParkBody {
            park_id: "s#0".into(),
            origin_offset: Some(7),
            entry: crate::flusher::DlqEntryBody {
                channel: "in".into(),
                error: "boom".into(),
                attempts: 3,
                payload: Some(vec![1, 2]),
            },
        };
        let bytes = postcard::to_allocvec(&body).unwrap();
        assert_eq!(
            bytes,
            vec![
                0x03, b's', b'#', b'0', // park_id "s#0"
                0x01, 0x07, // Some(7)
                0x02, b'i', b'n', // entry.channel "in"
                0x04, b'b', b'o', b'o', b'm', // entry.error "boom"
                0x03, // entry.attempts 3
                0x01, 0x02, 0x01, 0x02, // entry.payload Some([1,2])
            ]
        );
    }
}
