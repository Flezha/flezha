//! An edge output is the one output kind with no durable copy, so the only
//! thing that can re-produce it after a crash is the producer re-running
//! its input. That makes the order of two statements a durability
//! guarantee: fold the effect record (offset included) after the pushes
//! land, never before. The flusher drains every ~5 ms, so "before" is a
//! real window, not a theoretical one.

use std::any::Any;
use std::sync::Arc;

use super::activation::{refuse_completion, settle_item};
use super::HostPayload;
use crate::cell::{CellItem, CellPayload, CellSlot};
use crate::poll_core::ItemAttempt;
use crate::schema;
use crate::test_support::{host_class, saturate, spin_until, stream_pair};

/// Fill one consumer's single mailbox slot and claim its round, so nothing
/// drains it until the test says so.
fn saturate_slot(slot: &Arc<CellSlot>) {
    saturate(
        &slot.mailbox,
        CellItem::Input(CellPayload {
            value: Arc::new(0usize) as Arc<dyn Any + Send + Sync>,
            stream: stream_pair().0,
            consumed_offset: None,
            replay: false,
            forwarded: false,
            hold: None,
        }),
    );
    assert!(slot.mailbox.claim_inflight());
}

/// One input at `offset`, and the completion that emits one edge value to
/// each consumer in `consumers`.
fn work_and_effect(offset: u64, consumers: &[u32]) -> (ItemAttempt, schema::ItemEffect) {
    let work = ItemAttempt {
        item: CellItem::Input(CellPayload {
            value: Arc::new(HostPayload {
                bytes: b"input".to_vec(),
                channel: 0,
                keys: Vec::new(),
                event_time: None,
                metadata: Vec::new(),
            }) as Arc<dyn Any + Send + Sync>,
            stream: stream_pair().0,
            consumed_offset: Some(offset),
            replay: false,
            forwarded: false,
            hold: None,
        }),
        now: None,
        attempt: 1,
        is_final: false,
    };
    let outputs = consumers
        .iter()
        .map(|&consumer| schema::Output {
            target: schema::OutputTarget::Edge {
                channel: 1,
                consumer,
            },
            payload: b"edge".to_vec(),
            key: None,
            metadata: Vec::new(),
            event_time: None,
        })
        .collect();
    let effect = schema::ItemEffect {
        verdict: schema::Verdict::Ok,
        changes: vec![b"change".to_vec()],
        outputs,
        consumed_offset: None,
        event_time: 0,
        timers: Vec::new(),
        checkpoint: false,
        dlq: None,
        complete: None,
    };
    (work, effect)
}

/// Records this Cell has actually SHIPPED. `drain_to_frames` fetch-adds
/// the per-Cell seq once per drained record, so the counter advancing is
/// exactly "a record left for the controller" — the one observable that
/// means the same thing whichever mechanism carried the backpressure. A
/// parked producer folds nothing; a deferring producer folds into a buffer
/// the flusher will not drain while the push is outstanding.
fn shipped(slot: &Arc<CellSlot>) -> u64 {
    crate::flusher::CellFlusher::instance().drain_now_for_tests();
    slot.change_seq.load(std::sync::atomic::Ordering::Acquire)
}

/// Pushes this consumer has refused for lack of room — its own counter, so a
/// sibling test's blocked push cannot end the wait early.
fn blocked_on(slot: &Arc<CellSlot>) -> u64 {
    slot.mailbox
        .counters()
        .pushes_blocked_total
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// One free slot on the consumer, announced exactly as a drain announces
/// it: pop, then serve the waiter the room is owed to.
fn free_one_slot(slot: &Arc<CellSlot>) {
    assert!(slot.mailbox.pop().is_some());
    slot.waiters.serve_one();
}

#[test]
fn no_record_carries_the_offset_until_the_edge_push_lands() {
    let id = format!("edge_order_{}", std::process::id());
    let instance = host_class::instance(&id, &["in"], &[], &[1]);
    let producer = Arc::clone(&instance.cells[0][0]);
    let consumer = Arc::clone(&instance.cells[1][0]);
    saturate_slot(&consumer);
    let before = shipped(&producer);
    let blocked_before = blocked_on(&consumer);

    let (work, effect) = work_and_effect(7, &[1]);
    let resolving = {
        let producer = Arc::clone(&producer);
        std::thread::spawn(move || settle_item(&producer, &work, effect, None))
    };

    // The push has nowhere to go — it is held back — so nothing this item
    // produced is durable, including the offset that would make a replay
    // skip its input.
    spin_until("the push to be held back", || {
        blocked_on(&consumer) > blocked_before || !producer.push_holdback.lock().unwrap().is_empty()
    });
    assert_eq!(
        shipped(&producer),
        before,
        "the offset must not be durable while its edge output is undelivered"
    );
    assert_eq!(
        consumer.mailbox.len(),
        1,
        "only the saturating item is queued"
    );

    // Room appears: the push lands and the record follows it.
    free_one_slot(&consumer);
    resolving.join().expect("resolution completes");
    crate::poll_core::global().rearm_deferred_pushes(&producer);
    assert_eq!(
        consumer.mailbox.len(),
        1,
        "the edge value reached the consumer's mailbox"
    );
    assert!(shipped(&producer) > before, "the record follows the push");
}

#[test]
fn a_fan_out_holds_its_record_until_every_consumer_has_the_value() {
    let id = format!("edge_fanout_{}", std::process::id());
    let instance = host_class::instance(&id, &["in"], &[], &[1, 2, 3]);
    let producer = Arc::clone(&instance.cells[0][0]);
    let full = Arc::clone(&instance.cells[3][0]);
    saturate_slot(&full);
    let before = shipped(&producer);
    let blocked_before = blocked_on(&full);

    let (work, effect) = work_and_effect(11, &[1, 2, 3]);
    let resolving = {
        let producer = Arc::clone(&producer);
        std::thread::spawn(move || settle_item(&producer, &work, effect, None))
    };

    spin_until("the fan-out push to be held back", || {
        blocked_on(&full) > blocked_before || !producer.push_holdback.lock().unwrap().is_empty()
    });
    assert_eq!(
        instance.cells[1][0].mailbox.len(),
        1,
        "the consumers with room took theirs"
    );
    assert_eq!(instance.cells[2][0].mailbox.len(), 1);
    assert_eq!(
        shipped(&producer),
        before,
        "one undelivered consumer holds the whole record"
    );

    free_one_slot(&full);
    resolving.join().expect("resolution completes");
    crate::poll_core::global().rearm_deferred_pushes(&producer);
    assert_eq!(full.mailbox.len(), 1, "the last consumer got its value");
    assert!(shipped(&producer) > before);
}

/// Outside the tier the record that carries the offset keeps the edge value
/// beside it, so a restore can deliver it again to a consumer that lost it.
#[test]
fn outside_the_tier_the_record_carrying_the_offset_keeps_the_edge_value() {
    use std::sync::Mutex;

    let _guard = crate::state_sink::test_guard();
    crate::state_sink::reset_for_test();
    let records: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
    let seen = Arc::clone(&records);
    crate::state_sink::install_global_sink(Arc::new(move |payload_id: &str, frame: &[u8]| {
        if payload_id == crate::state_sink::CELL_EFFECT_RECORD_PAYLOAD_ID {
            seen.lock().unwrap().push(frame.to_vec());
        }
        Ok(())
    }));

    let id = format!("edge_off_tier_{}", std::process::id());
    let instance = host_class::instance_of(&id, &["in"], &[], &[1], false);
    let producer = Arc::clone(&instance.cells[0][0]);
    let (work, effect) = work_and_effect(7, &[1]);
    settle_item(&producer, &work, effect, None);
    crate::flusher::CellFlusher::instance().drain_now_for_tests();

    let body = records
        .lock()
        .unwrap()
        .iter()
        .find_map(|frame| {
            let id_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
            let at = 2 + id_len + 8 + 16 + 4;
            postcard::from_bytes::<crate::effect::EffectRecordBody>(&frame[at..frame.len() - 4])
                .ok()
                .filter(|body| body.consumed_offset == Some(7))
        })
        .expect("the producer's record for offset 7");
    assert_eq!(body.edges.len(), 1);
    assert_eq!((body.edges[0].consumer, body.edges[0].origin), (1, Some(7)));
    crate::state_sink::reset_for_test();
}

/// An unbounded operator never completes: an effect asking to fails as the author's own
/// non-retryable error, recording nothing, and an item that already failed
/// keeps its verdict.
#[test]
fn an_unbounded_effect_carrying_a_completion_fails_non_retryable() {
    let (_, mut asked) = work_and_effect(7, &[2]);
    asked.consumed_offset = Some(7);
    asked.event_time = 42;
    asked.checkpoint = true;
    asked.complete = Some(b"3".to_vec());
    refuse_completion(&mut asked);
    assert_eq!(
        asked,
        schema::ItemEffect {
            verdict: schema::Verdict::NonRetryableError {
                message: crate::bounded::complete_outside_bounded().text()
            },
            consumed_offset: Some(7),
            event_time: 42,
            ..super::activation::empty_effect()
        },
        "the changes, outputs and checkpoint the item recorded go with it"
    );

    let (_, mut failed) = work_and_effect(0, &[]);
    failed.complete = Some(b"3".to_vec());
    failed.verdict = schema::Verdict::RetryableError {
        message: "own".to_string(),
    };
    refuse_completion(&mut failed);
    assert_eq!(
        failed.verdict,
        schema::Verdict::RetryableError {
            message: "own".to_string()
        }
    );
}
