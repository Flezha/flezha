//! A Cell or a relay leaving this runner reaches the host as a leave: an
//! evicted Cell's while its key still names it, a stopped relay's once its
//! running work is answered, and a start of the same id only after that.

use std::sync::Arc;

use super::activation_tests::work_and_effect;
use crate::schema;
use crate::test_support::spin_until;
use crate::util::LockRecover;

/// The next leave for `relay`, taken off the direct queue unanswered, or
/// `None` once `done` holds first; another test's queued work stays where it is.
fn take_leave_unless(
    relay: &str,
    done: impl Fn() -> bool,
) -> Option<crate::poll_core::EngineActivation> {
    let taken = std::cell::RefCell::new(None);
    spin_until("a leave", || {
        *taken.borrow_mut() = crate::poll_core::global().take_leave(relay);
        taken.borrow().is_some() || done()
    });
    taken.into_inner()
}

pub(crate) fn take_leave(relay: &str) -> crate::poll_core::EngineActivation {
    take_leave_unless(relay, || false).expect("taken")
}

/// One input queued on `slot`.
fn push_one(slot: &crate::cell::CellSlot) {
    assert!(slot
        .mailbox
        .try_push(work_and_effect(1, &[]).0.item)
        .is_ok());
}

/// A taken leave, answered as a host answers one.
fn answer_leave(act: crate::poll_core::EngineActivation) -> schema::Activation {
    let leave = crate::host::to_schema(&act);
    let (token, outcome) = (act.token, schema::ActivationOutcome::StateApplied);
    crate::host::apply_result(
        crate::poll_core::global(),
        act,
        schema::ActivationResult { token, outcome },
    )
    .expect("answered");
    leave
}

fn next_leave(relay: &str) -> schema::Activation {
    answer_leave(take_leave(relay))
}

/// A dynamic host class `id` started as `relay`, its Cells evicted idle after
/// `ttl_ms` when one is given.
fn leaving_relay(
    node: &crate::runner::RunnerNode,
    id: &str,
    relay: &str,
    ttl_ms: Option<u64>,
) -> Arc<crate::dynamic::DynamicCellRegistry> {
    let ttl = ttl_ms.map_or(String::new(), |ms| format!(r#","idle_ttl_ms":{ms}"#));
    let json = format!(
        r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,"run":"flow",
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"key"}},{{"link":"into","tasklet":0}}]]}}],
             "tasklets":[{{"name":"p0","cells_for_overridden":true{ttl}}}]}}"#
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    let instance = node
        .start_relay(
            id.to_string(),
            relay.to_string(),
            crate::relay::instance::KeyGroupRange::UNPARTITIONED,
            Default::default(),
        )
        .expect("the relay starts");
    instance.dynamic_registries[0].clone().expect("dynamic")
}

#[test]
fn an_evicted_cell_and_a_stopped_relay_each_tell_the_host_they_left() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_leave_{}", std::process::id());
    let node = crate::runner::RunnerNode::new("test-ns".to_string());
    let slot = leaving_relay(&node, &id, "leave-1", Some(1))
        .get_or_create("k1")
        .expect("the Cell is minted");
    let target = crate::host::cell_ref_of(&slot);
    drop(slot);

    let evicted = next_leave("leave-1");
    assert_eq!(evicted.target, target);
    assert_eq!(
        evicted.kind,
        schema::ActivationKind::Leave { whole_relay: false }
    );
    assert_eq!(evicted.factory, id);

    assert!(node.stop_relay("leave-1"));
    let stopped = next_leave("leave-1");
    assert_eq!(
        (stopped.target.tasklet, stopped.target.cell_key.as_deref()),
        (0, None)
    );
    assert_eq!(
        stopped.kind,
        schema::ActivationKind::Leave { whole_relay: true }
    );
}

/// The identity crosses once per host Cell: on its first activation, on the
/// first after its leave, and on no other.
#[test]
fn the_identity_crosses_once_per_host_cell() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_leave_identity_{}", std::process::id());
    let node = crate::runner::RunnerNode::new("test-ns".to_string());
    let slot = leaving_relay(&node, &id, "leave-12", None)
        .get_or_create("k1")
        .expect("the Cell is minted");
    let core = crate::poll_core::global();
    let mut offset = 0;
    let mut handout = || {
        offset += 1;
        let (work, effect) = work_and_effect(offset, &[]);
        assert!(slot.mailbox.try_push(work.item).is_ok());
        assert!(slot.mailbox.claim_inflight());
        let act = core.try_assemble(&slot).expect("the work is handed out");
        let identity = crate::host::to_schema(&act).cell_identity;
        let (token, outcome) = (act.token, schema::ActivationOutcome::Work(vec![effect]));
        let result = schema::ActivationResult { token, outcome };
        crate::host::apply_result(core, act, result).expect("answered");
        identity
    };
    let bytes = Some(slot.cell_identity.to_vec());
    assert_eq!(
        [handout(), handout(), handout()],
        [bytes.clone(), None, None]
    );

    assert!(slot.claim_drained(), "the Cell is idle");
    let leaving = Arc::clone(&slot);
    let left = std::thread::spawn(move || {
        crate::host::cell_left_locked(&leaving);
        leaving.release_drained();
    });
    assert_eq!(answer_leave(take_leave("leave-12")).cell_identity, None);
    left.join().expect("the leave is answered");

    assert_eq!([handout(), handout()], [bytes, None]);
    assert!(node.stop_relay("leave-12"));
    next_leave("leave-12");
}

#[test]
fn an_evicted_cells_key_names_it_until_the_host_answers_its_leave() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_leave_evicted_{}", std::process::id());
    let node = crate::runner::RunnerNode::new("test-ns".to_string());
    let registry = leaving_relay(&node, &id, "leave-3", Some(1));
    let slot = Arc::downgrade(&registry.get_or_create("k1").expect("the Cell is minted"));

    let leave = take_leave("leave-3");
    let named = registry.get_or_create("k1").expect("the key names a Cell");
    assert!(
        std::ptr::eq(Arc::as_ptr(&named), slot.as_ptr()),
        "a Cell minted again before the host dropped the old state"
    );
    drop(named);
    answer_leave(leave);

    spin_until("the eviction", || !registry.cells.contains_key("k1"));
    assert!(node.stop_relay("leave-3"));
    next_leave("leave-3");
}

#[test]
fn a_stopped_relay_leaves_only_once_its_running_work_is_answered() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_leave_running_{}", std::process::id());
    let node = crate::runner::RunnerNode::new("test-ns".to_string());
    let slot = leaving_relay(&node, &id, "leave-2", None)
        .get_or_create("k1")
        .expect("the Cell is minted");
    let core = crate::poll_core::global();
    push_one(&slot);
    assert!(slot.mailbox.claim_inflight());
    let running = core.try_assemble(&slot).expect("the work is handed out");

    assert!(node.stop_relay("leave-2"));
    assert!(
        core.take_leave("leave-2").is_none(),
        "a leave while the host still runs the relay's work"
    );
    crate::host::abandon_activation(core, running).expect("answered");
    assert_eq!(
        next_leave("leave-2").kind,
        schema::ActivationKind::Leave { whole_relay: true }
    );
}

#[test]
fn a_push_during_an_evictions_leave_keeps_the_cell_started_again_in_place() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_leave_pushed_{}", std::process::id());
    let node = crate::runner::RunnerNode::new("test-ns".to_string());
    let registry = leaving_relay(&node, &id, "leave-4", Some(1));
    let slot = Arc::downgrade(&registry.get_or_create("k1").expect("the Cell is minted"));

    let leave = take_leave("leave-4");
    push_one(&slot.upgrade().expect("the Cell is live"));
    answer_leave(leave);

    // Read through the key, so this test holds nothing the eviction counts.
    spin_until("the eviction gives the gate back", || {
        let gate = |s: &Arc<crate::cell::CellSlot>| {
            s.drain_active.load(std::sync::atomic::Ordering::Acquire)
        };
        registry.cells.get("k1").is_some_and(|s| !gate(s.value()))
    });
    let named = registry.get_or_create("k1").expect("the key names a Cell");
    assert!(
        std::ptr::eq(Arc::as_ptr(&named), slot.as_ptr()),
        "the pushed Cell was dropped"
    );
    let evicted = &registry.counters.cells_evicted_total;
    assert_eq!(evicted.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert!(node.stop_relay("leave-4"));
    next_leave("leave-4");
}

#[test]
fn a_start_of_an_id_whose_leave_is_pending_runs_once_the_host_answers_it() {
    let _env = crate::runner::env_test_guard();
    let (tx, rx) = crossbeam::channel::unbounded::<crate::runner::RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    let id = format!("host_leave_restart_{}", std::process::id());
    let mut node = crate::runner::RunnerNode::new("test-ns".to_string());
    let slot = leaving_relay(&node, &id, "leave-5", None)
        .get_or_create("k1")
        .expect("the Cell is minted");
    let core = crate::poll_core::global();
    push_one(&slot);
    assert!(slot.mailbox.claim_inflight());
    let running = core.try_assemble(&slot).expect("the work is handed out");
    assert!(node.stop_relay("leave-5"));

    let start = crate::command::request_bytes(
        "ref-restart".into(),
        crate::pb::command_request::Command::AddRelay(crate::pb::AddRelay {
            relay_id: "leave-5".into(),
            factory_id: id.clone(),
            ..Default::default()
        }),
    );
    let (stream, _controller) = crate::test_support::stream_pair();
    crate::command::handle_command(&mut node, &stream, "control_command", &start);
    assert!(
        !node.has_relay("leave-5"),
        "the start ran before the host dropped the old state"
    );

    crate::host::abandon_activation(core, running).expect("answered");
    next_leave("leave-5");
    let held = loop {
        match rx.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(crate::runner::RunnerMessage::HandleCommandAction(payload_id, frame, _)) => {
                break (payload_id, frame)
            }
            Ok(_) => continue,
            Err(e) => panic!("the held start never came back: {e}"),
        }
    };
    assert_eq!(held.1, start);
    crate::command::handle_command(&mut node, &stream, &held.0, &held.1);
    assert!(node.has_relay("leave-5"));
    assert!(node.stop_relay("leave-5"));
    next_leave("leave-5");
}

#[test]
fn a_stopped_relays_queued_work_is_never_handed_out_after_its_leave() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_leave_queued_{}", std::process::id());
    let node = crate::runner::RunnerNode::new("test-ns".to_string());
    let slot = leaving_relay(&node, &id, "leave-6", None)
        .get_or_create("k1")
        .expect("the Cell is minted");
    push_one(&slot);
    assert!(slot.mailbox.claim_inflight());

    assert!(node.stop_relay("leave-6"));
    next_leave("leave-6");
    assert!(
        crate::poll_core::global().try_assemble(&slot).is_none(),
        "a stopped relay's queued work handed out after its leave"
    );
    assert!(slot.mailbox.is_empty());
}

#[test]
fn an_eviction_never_drops_a_hold_whose_record_has_not_shipped() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_leave_held_{}", std::process::id());
    let node = crate::runner::RunnerNode::new("test-ns".to_string());
    let registry = leaving_relay(&node, &id, "leave-7", None);
    let slot = registry.get_or_create("k1").expect("the Cell is minted");
    let hold = crate::prefix::PrefixLedger::new("leave-7".into()).hold(1);
    slot.flush_buffer
        .lock_recover()
        .keep_hold(Arc::clone(&hold));
    slot.last_activity_ms
        .store(i64::MIN / 2, std::sync::atomic::Ordering::Relaxed);

    let sweeping = Arc::clone(&registry);
    let sweep = std::thread::spawn(move || sweeping.reap_sweep(std::time::Duration::ZERO));
    if let Some(leave) = take_leave_unless("leave-7", || sweep.is_finished()) {
        push_one(&slot);
        answer_leave(leave);
    }
    sweep.join().expect("the sweep ends");
    assert_eq!(
        Arc::strong_count(&hold),
        2,
        "an eviction dropped a hold whose record never shipped"
    );
    assert!(node.stop_relay("leave-7"));
    next_leave("leave-7");
}

#[test]
fn a_start_held_once_its_leave_is_answered_comes_back_to_run_at_once() {
    let pending = super::leave::PendingLeave::new();
    pending.answer();
    let start = ("control_command".to_string(), vec![1, 2]);
    assert_eq!(pending.hold(start.clone()), Some(start));
}

#[test]
fn a_stop_does_not_wait_out_an_evictions_unanswered_leave() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_leave_reaper_{}", std::process::id());
    let node = crate::runner::RunnerNode::new("test-ns".to_string());
    let registry = leaving_relay(&node, &id, "leave-10", Some(1));
    drop(registry.get_or_create("k1").expect("the Cell is minted"));
    // The reaper holds the Cell's gate for its leave, which no host takes.
    spin_until("the eviction's leave", || {
        let gate = |s: &Arc<crate::cell::CellSlot>| {
            s.drain_active.load(std::sync::atomic::Ordering::Acquire)
        };
        registry.cells.get("k1").is_some_and(|s| gate(s.value()))
    });
    let started = std::time::Instant::now();
    assert!(node.stop_relay("leave-10"));
    let took = started.elapsed();
    assert!(
        took < std::time::Duration::from_secs(5),
        "the stop waited {took:?} on the reaper's unanswered leave"
    );
    let evicted = &registry.counters.cells_evicted_total;
    assert_eq!(evicted.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(
        next_leave("leave-10").kind,
        schema::ActivationKind::Leave { whole_relay: true }
    );
    assert!(
        crate::poll_core::global().take_leave("leave-10").is_none(),
        "the Cell's withdrawn leave reached the host"
    );
}

#[test]
fn a_stopped_relays_leave_waits_for_a_cell_leave_a_host_took() {
    let _env = crate::runner::env_test_guard();
    let id = format!("host_leave_sweep_stop_{}", std::process::id());
    let node = crate::runner::RunnerNode::new("test-ns".to_string());
    let registry = leaving_relay(&node, &id, "leave-11", None);
    for key in ["k1", "k2"] {
        let slot = registry.get_or_create(key).expect("the Cell is minted");
        slot.last_activity_ms
            .store(i64::MIN / 2, std::sync::atomic::Ordering::Relaxed);
    }
    let sweeping = Arc::clone(&registry);
    let sweep = std::thread::spawn(move || sweeping.reap_sweep(std::time::Duration::ZERO));
    let held = take_leave("leave-11");
    assert!(node.stop_relay("leave-11"));
    assert!(
        crate::poll_core::global().take_leave("leave-11").is_none(),
        "a leave while the host still holds a Cell's leave"
    );
    answer_leave(held);
    sweep.join().expect("the sweep ends");
    // The sweep is done, so a Cell leave it sent after the stop is queued now.
    assert_eq!(
        next_leave("leave-11").kind,
        schema::ActivationKind::Leave { whole_relay: true }
    );
    assert!(
        crate::poll_core::global().take_leave("leave-11").is_none(),
        "a Cell leave sent after the relay stopped"
    );
}
