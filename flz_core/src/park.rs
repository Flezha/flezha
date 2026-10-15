//! The dead-letter park outside the effectively-once tier: a `park_payload`
//! frame plus a wait for the controller's `ParkPayloadAck`, re-sent until
//! acked. The invariant: the cell advances only once the park is durable
//! — delay, never loss. Under the tier the park rides the effect record.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::flusher::build_cell_frame;
use crate::routing::EntryView;
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
    /// The token of the outside call waiting on the parked input, if any
    /// (`flz.reply_to`), so the controller can answer it `call.failed`.
    pub reply_to: Option<String>,
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

/// Ship a park and block until it is acked durable, re-sending until then; a
/// run with no controller, local mode included, logs and drops. The controller
/// acks a park only from the runner its relay is bound to, so the wait ends
/// unacked once `relay` has left this one: the input is its new owner's.
fn park_blocking(cell_identity: &[u8], body: &ParkBody, relay: &dyn EntryView) {
    if !state_sink::controller_attached() {
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
    while !relay.removed() {
        state_sink::send(PARK_PAYLOAD_PAYLOAD_ID, frame.clone());
        let deadline = std::time::Instant::now() + ACK_RESEND_INTERVAL;
        loop {
            if take_ack(&body.park_id) {
                return;
            }
            let guard = ACKED.lock().expect("park ack registry poisoned");
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() || relay.removed() {
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
    tracing::warn!(
        target: "flezha::dlq",
        park_id = %body.park_id,
        channel = %body.entry.channel,
        "dead-letter park dropped unacked: its relay left this runner"
    );
}

/// Wake every park waiting on an ack to look again at its relay. A waiter
/// looks under the registry lock and sleeps without releasing it in between,
/// so this wake cannot fall between the two.
pub(crate) fn wake_waiters() {
    let _guard = ACKED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    ACKED_CV.notify_all();
}

/// Mint, build and ship one tier-off park, blocking for its ack: a Cell's
/// poison input, or a payload a retry ladder could not deliver.
pub fn park_tier_off(
    cell_identity: &[u8],
    cell_label: &str,
    origin_offset: Option<u64>,
    reply_to: Option<&Arc<str>>,
    entry: crate::flusher::DlqEntryBody,
    relay: &dyn EntryView,
) {
    let body = ParkBody {
        park_id: mint_park_id(cell_label),
        origin_offset,
        entry,
        reply_to: reply_to.map(|token| token.to_string()),
    };
    park_blocking(cell_identity, &body, relay);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fails when: a park whose relay left the runner re-sends forever and
    /// holds its Cell's worker, because the controller acks a park only from
    /// the runner the relay is bound to.
    #[test]
    fn a_park_stops_waiting_once_its_relay_leaves_the_runner() {
        let _guard = state_sink::test_guard();
        state_sink::reset_for_test();
        let sends = Arc::new(AtomicU64::new(0));
        let sent = Arc::clone(&sends);
        state_sink::install_global_sink(Arc::new(move |payload_id: &str, _frame: &[u8]| {
            if payload_id == PARK_PAYLOAD_PAYLOAD_ID {
                sent.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }));
        let relay = crate::test_support::host_class::instance("moved", &["in"], &[], &[]);
        let parking = Arc::clone(&relay);
        let entry = crate::flusher::DlqEntryBody {
            channel: "input".to_string(),
            error: "poison".to_string(),
            attempts: 3,
            payload: None,
        };
        let parked = std::thread::spawn(move || {
            park_tier_off(b"moved", "moved/tasklet/key", None, None, entry, &*parking);
        });
        while sends.load(Ordering::SeqCst) == 0 {
            std::thread::yield_now();
        }

        relay.shutdown_background_threads();
        let woken = std::time::Instant::now();
        while !parked.is_finished() && woken.elapsed() < ACK_RESEND_INTERVAL / 2 {
            std::thread::sleep(Duration::from_millis(5));
        }

        assert!(
            parked.is_finished(),
            "the park still waits on a relay that is gone"
        );
        assert_eq!(
            sends.load(Ordering::SeqCst),
            1,
            "a gone relay's park is not re-sent"
        );
        state_sink::reset_for_test();
    }

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
            reply_to: Some("w:t".into()),
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
                0x01, 0x03, b'w', b':', b't', // reply_to Some("w:t")
            ]
        );
    }
}
