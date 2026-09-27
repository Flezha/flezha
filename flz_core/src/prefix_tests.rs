use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use super::*;
use crate::flusher::{build_cell_frame, encode_cell_identity, SendError, SendFrameFn};

fn ledger(instance_id: &str) -> Arc<PrefixLedger> {
    PrefixLedger::new(Arc::from(instance_id))
}

/// The report's own bookkeeping, without a wire: what `report` would send,
/// recorded as sent.
fn take(ledger: &PrefixLedger) -> Option<u64> {
    let prefix = ledger.candidate()?;
    ledger.state.lock_recover().reported = Some(prefix);
    Some(prefix)
}

fn record_of(instance_id: &str) -> Vec<u8> {
    build_cell_frame(
        &encode_cell_identity(instance_id, "tasklet", "key"),
        0,
        &[0u8; 16],
        b"change",
    )
}

/// One key took offset 1 and went quiet while another took 2..=1005: the
/// quiet Cell holds nothing once its input is settled.
#[test]
fn an_idle_cell_does_not_hold_the_prefix() {
    let ledger = ledger("idle-cell-op");
    drop(ledger.hold(1));
    for offset in 2..=1005 {
        drop(ledger.hold(offset));
    }
    assert_eq!(take(&ledger), Some(1005));
    assert_eq!(take(&ledger), None, "nothing moved since the last report");
}

#[test]
fn a_pending_input_holds_the_prefix_below_it() {
    let ledger = ledger("pending-op");
    let seven = ledger.hold(7);
    drop(ledger.hold(8));
    assert_eq!(take(&ledger), Some(6));

    // A clone is the same claim: the input stays pending until both go.
    let retried = Arc::clone(&seven);
    drop(seven);
    assert_eq!(take(&ledger), None);
    drop(retried);
    assert_eq!(take(&ledger), Some(8));
}

#[test]
fn the_first_offset_is_an_offset() {
    let ledger = ledger("first-offset-op");
    let zero = ledger.hold(0);
    assert_eq!(take(&ledger), None, "nothing is below offset 0");
    drop(zero);
    assert_eq!(take(&ledger), Some(0));
}

/// A restore discards what the Cell took above its records; the re-forward
/// of that is ahead on the wire, so nothing is reported until an input past
/// everything seen before arrives.
#[test]
fn a_restore_reports_nothing_until_the_re_forward_catches_up() {
    let ledger = ledger("restored-op");
    for offset in 1..=3 {
        drop(ledger.hold(offset));
    }
    assert_eq!(take(&ledger), Some(3));

    ledger.restored();
    let reforwarded = ledger.hold(2);
    drop(reforwarded);
    assert_eq!(take(&ledger), None, "the re-forward has not caught up");

    drop(ledger.hold(4));
    assert_eq!(take(&ledger), Some(4));
}

/// The ring evicted a record of this instance: its input's record is lost,
/// so no marker may cover it until a restore re-seats the Cell.
#[test]
fn an_evicted_record_freezes_its_instance_until_a_restore() {
    let frozen = ledger("evicted-record-op");
    let other = ledger("untouched-op");
    frame_evicted(&record_of("evicted-record-op"));

    drop(frozen.hold(5));
    drop(other.hold(5));
    assert_eq!(take(&frozen), None);
    assert_eq!(take(&other), Some(5));

    frozen.restored();
    drop(frozen.hold(6));
    assert_eq!(take(&frozen), Some(6));
}

/// A marker never waits in the reconnect ring and never overtakes a record
/// that is waiting there.
#[test]
fn a_marker_goes_out_only_behind_an_empty_ring() {
    let _g = crate::state_sink::test_guard();
    crate::state_sink::reset_for_test();

    assert!(
        !crate::state_sink::send_unheld(PROCESSED_PREFIX_PAYLOAD_ID, b"m".to_vec()),
        "no sink installed"
    );

    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let fail = Arc::new(AtomicBool::new(true));
    let (seen_c, fail_c) = (Arc::clone(&seen), Arc::clone(&fail));
    let sink: SendFrameFn = Arc::new(move |payload_id: &str, _frame: &[u8]| {
        if fail_c.load(Ordering::SeqCst) {
            return Err(SendError::TransportDown("down".into()));
        }
        seen_c.lock().unwrap().push(payload_id.to_string());
        Ok(())
    });
    crate::state_sink::install_global_sink(sink);

    crate::state_sink::send(
        crate::state_sink::CELL_STATE_CHANGE_PAYLOAD_ID,
        record_of("op"),
    );
    assert_eq!(
        crate::state_sink::ring_len_for_test(),
        1,
        "the record is held"
    );
    fail.store(false, Ordering::SeqCst);
    assert!(!crate::state_sink::send_unheld(
        PROCESSED_PREFIX_PAYLOAD_ID,
        b"m".to_vec()
    ));
    assert_eq!(
        crate::state_sink::ring_len_for_test(),
        1,
        "a refused marker is not held"
    );
    assert!(seen.lock().unwrap().is_empty());

    crate::state_sink::reset_for_test();
}

/// A ring eviction of a record is what freezes the record's instance.
#[test]
fn a_ring_eviction_of_a_record_freezes_its_instance() {
    let _g = crate::state_sink::test_guard();
    std::env::set_var("FLEZHA_RECONNECT_RING_CAPACITY", "1");
    crate::state_sink::reset_for_test();

    let ledger = ledger("ring-evicted-op");
    crate::state_sink::send(
        crate::state_sink::CELL_STATE_CHANGE_PAYLOAD_ID,
        record_of("ring-evicted-op"),
    );
    crate::state_sink::send(
        crate::state_sink::CELL_STATE_CHANGE_PAYLOAD_ID,
        record_of("another-op"),
    );
    drop(ledger.hold(1));
    assert_eq!(take(&ledger), None, "a marker would cover the lost record");

    std::env::remove_var("FLEZHA_RECONNECT_RING_CAPACITY");
    crate::state_sink::reset_for_test();
    ledger.restored();
}
