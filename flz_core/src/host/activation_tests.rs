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
use crate::entity::{EntityItem, EntityPayload, EntitySlot};
use crate::poll_core::ItemAttempt;
use crate::schema;
use crate::test_support::{host_class, saturate, spin_until, stream_pair};

/// Fill one consumer's single mailbox slot and claim its round, so nothing
/// drains it until the test says so.
fn saturate_slot(slot: &Arc<EntitySlot>) {
    saturate(
        &slot.mailbox,
        EntityItem::Input(EntityPayload {
            value: Arc::new(0usize) as Arc<dyn Any + Send + Sync>,
            stream: stream_pair().0,
            consumed_offset: None,
            replay: false,
        }),
    );
    assert!(slot.mailbox.claim_inflight());
}

/// One input at `offset`, and the completion that emits one edge value to
/// each consumer in `consumers`.
fn work_and_effect(offset: u64, consumers: &[u32]) -> (ItemAttempt, schema::ItemEffect) {
    let work = ItemAttempt {
        item: EntityItem::Input(EntityPayload {
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

/// Records this Entity has actually SHIPPED. `drain_to_frames` fetch-adds
/// the per-Entity seq once per drained record, so the counter advancing is
/// exactly "a record left for the controller" — the one observable that
/// means the same thing whichever mechanism carried the backpressure. A
/// parked producer folds nothing; a deferring producer folds into a buffer
/// the flusher will not drain while the push is outstanding.
fn shipped(slot: &Arc<EntitySlot>) -> u64 {
    crate::flusher::EntityFlusher::instance().drain_now_for_tests();
    slot.change_seq.load(std::sync::atomic::Ordering::Acquire)
}

/// Pushes this consumer has refused for lack of room — its own counter, so a
/// sibling test's blocked push cannot end the wait early.
fn blocked_on(slot: &Arc<EntitySlot>) -> u64 {
    slot.mailbox
        .counters()
        .pushes_blocked_total
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// One free slot on the consumer, announced exactly as a drain announces
/// it: pop, then serve the waiter the room is owed to.
fn free_one_slot(slot: &Arc<EntitySlot>) {
    assert!(slot.mailbox.pop().is_some());
    slot.waiters.serve_one();
}

#[test]
fn no_record_carries_the_offset_until_the_edge_push_lands() {
    let id = format!("edge_order_{}", std::process::id());
    let instance = host_class::instance(&id, &["in"], &[], &[1]);
    let producer = Arc::clone(&instance.entities[0][0]);
    let consumer = Arc::clone(&instance.entities[1][0]);
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
    let producer = Arc::clone(&instance.entities[0][0]);
    let full = Arc::clone(&instance.entities[3][0]);
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
        instance.entities[1][0].mailbox.len(),
        1,
        "the consumers with room took theirs"
    );
    assert_eq!(instance.entities[2][0].mailbox.len(), 1);
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

/// A stream never completes: an effect asking to fails as the author's own
/// non-retryable error, recording nothing, and an item that already failed
/// keeps its verdict.
#[test]
fn a_stream_effect_carrying_a_completion_fails_non_retryable() {
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
                message: crate::workflow::COMPLETE_OUTSIDE_WORKFLOW.to_string()
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
