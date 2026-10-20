//! A hosted item's timer ops are judged at settle, against the Cell's live
//! set, before its verdict is matched: a refusal fails the item down the path
//! every failed item takes, and a commit moves the Tasklet's counters. A
//! restored Cell's timers arm once a secondary stream is installed.

use std::sync::Arc;

use super::activation_tests::{recorded, work_and_effect};
use crate::cell::{CellItem, CellSlot};
use crate::poll_core::{ItemAttempt, PollCore};
use crate::relay::instance::RelayInstance;
use crate::schema::{self, TimerOp};
use crate::test_support::{spin_until, stream_pair};
use crate::timer::{counters_snapshot, timers_fired_total, TimerCountersView, TimerKind};
use crate::util::LockRecover;

const PT: TimerKind = TimerKind::ProcessingTime;
/// Far enough ahead that nothing a test arms comes due while it runs.
const LATER: u64 = 1 << 42;

/// A tier class whose one tasklet, named `tasklet` so its counters are its
/// own, holds at most `cap` timers per Cell; its Cell starts with `pending`.
fn capped(tasklet: &str, cap: usize, pending: &[u64]) -> (Arc<RelayInstance>, Arc<CellSlot>) {
    let id = format!("{tasklet}_{}", std::process::id());
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0",
             "durable":{{"local":{{"sleep_after":false}}}},
             "run":{{"flow":{{"exactly":"effectively_once"}}}},
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}},
                         {{"name":"out","kind":"plugin","producer":0,"keyed":true,
                           "chains":[[{{"link":"sink","name":"out"}}]]}}],
             "tasklets":[{{"name":"{tasklet}","max_timers_per_cell":{cap}}}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    let instance = RelayInstance::of_class(&id, &format!("{id}_inst")).expect("class registered");
    let slot = Arc::clone(&instance.cells[0][0]);
    for &at in pending {
        slot.timers.lock_recover().apply(&TimerOp::register(PT, at));
    }
    (instance, slot)
}

/// One input at `offset` whose effect registers a timer at each of `ats`.
fn registering(offset: u64, ats: &[u64]) -> (ItemAttempt, schema::ItemEffect) {
    let (work, mut effect) = work_and_effect(offset, &[]);
    effect.timers = ats.iter().map(|&at| TimerOp::register(PT, at)).collect();
    (work, effect)
}

/// Hand `works` out as one activation and answer it with `effects`.
fn settle_batch(slot: &Arc<CellSlot>, works: Vec<ItemAttempt>, effects: Vec<schema::ItemEffect>) {
    let core = PollCore::new();
    let count = works.len();
    for work in works {
        assert!(slot.mailbox.try_push(work.item).is_ok());
    }
    assert!(slot.mailbox.claim_inflight());
    let handed = core.try_assemble(slot).expect("the work is handed out");
    assert_eq!(
        handed.items().len(),
        count,
        "one activation carries every item"
    );
    let token = handed.token;
    let outcome = schema::ActivationOutcome::Work(effects);
    crate::host::apply_result(&core, handed, schema::ActivationResult { token, outcome })
        .expect("answered");
}

fn counters_of(tasklet: &str) -> TimerCountersView {
    counters_snapshot()
        .into_iter()
        .find(|view| view.tasklet == tasklet)
        .expect("the tasklet's counters exist from its registration")
}

fn pending_of(slot: &CellSlot) -> Vec<u64> {
    slot.timers.lock_recover().all(PT)
}

/// A hosted firing's removal is committed through the Tasklet's policy, so
/// the firing is counted as one, per Tasklet and process-wide.
#[test]
fn a_settled_firing_moves_the_fired_counters() {
    let (_instance, slot) = capped("fired_probe", 10, &[500]);
    let fired_before = timers_fired_total();
    let work = ItemAttempt {
        item: CellItem::Timer(crate::timer::TimerFire {
            firing: crate::timer::TimerFiring {
                kind: PT,
                scheduled_at: 500,
            },
            stream: stream_pair().0,
        }),
        now: None,
        attempt: 1,
        is_final: false,
    };
    super::activation::settle_item(&slot, &work, super::activation::empty_effect(), None);

    assert!(pending_of(&slot).is_empty(), "the firing left the set");
    let view = counters_of("fired_probe");
    assert_eq!((view.fired_total, view.cancelled_total), (1, 0));
    assert!(timers_fired_total() > fired_before);
}

/// An `Ok` item whose registration passes the cap settles as a non-retryable
/// failure: its record carries its changes and its park, and no output, no
/// timer and no end; the live set keeps what it held.
#[test]
fn an_item_past_the_cap_fails_as_non_retryable_with_its_changes_kept() {
    let (instance, slot) = capped("cap_refusal_probe", 1, &[LATER]);
    let (work, mut effect) = registering(1, &[LATER + 1]);
    effect.outputs = vec![schema::Output {
        target: schema::OutputTarget::Plugin {
            channel: "out".to_string(),
        },
        payload: b"out".to_vec(),
        key: Some("k".to_string()),
        metadata: Vec::new(),
        event_time: None,
    }];
    effect.complete = Some(b"done".to_vec());

    let body = recorded(
        || settle_batch(&slot, vec![work], vec![effect]),
        |body| body.dlq.is_some(),
    );

    let park = body.dlq.expect("the item parked");
    assert!(
        park.error.contains("runtime.timer_cap_exceeded"),
        "{}",
        park.error
    );
    assert_eq!(body.changes, vec![b"change".to_vec()]);
    assert!(body.outputs.is_empty() && body.timers.is_empty());
    assert!(
        !instance.end.requested(),
        "a refused item does not complete"
    );
    assert_eq!(pending_of(&slot), vec![LATER]);
    let view = counters_of("cap_refusal_probe");
    assert_eq!((view.refused_total, view.registered_total), (1, 0));
}

/// The judge borrows the live set under its lock; held here, a settle that
/// reached it could not finish.
#[test]
fn an_item_with_no_timer_ops_never_reaches_the_judge() {
    let (_instance, slot) = capped("no_ops_probe", 1, &[]);
    let (work, effect) = work_and_effect(1, &[]);
    let held = slot.timers.lock_recover();
    let settling = {
        let slot = Arc::clone(&slot);
        std::thread::spawn(move || settle_batch(&slot, vec![work], vec![effect]))
    };
    spin_until("the settle to finish", || settling.is_finished());
    drop(held);
    settling.join().expect("the settle ran");
}

/// One batch's items settle in order, so the second is judged against the
/// set the first committed.
#[test]
fn a_batch_judges_each_item_against_what_the_one_before_committed() {
    let (_instance, slot) = capped("batch_order_probe", 2, &[LATER]);
    let (first, first_effect) = registering(1, &[LATER + 1]);
    let (second, second_effect) = registering(2, &[LATER + 2]);

    let body = recorded(
        || {
            settle_batch(
                &slot,
                vec![first, second],
                vec![first_effect, second_effect],
            )
        },
        |body| body.dlq.is_some(),
    );

    assert_eq!(body.consumed_offset, Some(2), "the second item is refused");
    assert_eq!(pending_of(&slot), vec![LATER, LATER + 1]);
}

/// A Cell restored before the secondary stream exists has no stream to arm
/// its processing-time timers on; installing the stream arms them, and a due
/// one fires.
#[test]
fn a_timer_restored_before_the_secondary_stream_fires_once_it_is_installed() {
    let _sink = crate::state_sink::test_guard();
    crate::state_sink::set_secondary_stream(None);
    let (_instance, slot) = capped("restored_early", 10, &[1]);
    // The test holds the Cell's round, so the firing stays in its mailbox.
    assert!(slot.mailbox.claim_inflight());
    let core = crate::poll_core::global();

    // What a restore runs once its fold rebuilt the pending set.
    crate::timer::rearm_restored(&slot);
    core.promote_due_wakes();
    assert!(slot.mailbox.is_empty(), "no stream, nothing armed yet");

    crate::state_sink::set_secondary_stream(Some(stream_pair().0));
    core.promote_due_wakes();
    crate::state_sink::set_secondary_stream(None);

    match slot.mailbox.pop() {
        Some(CellItem::Timer(fire)) => assert_eq!(fire.firing.scheduled_at, 1),
        _ => panic!("the restored timer did not fire after the install"),
    }
}
