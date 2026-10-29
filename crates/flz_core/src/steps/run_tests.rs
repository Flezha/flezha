//! A steps deadline on the deferred-wake list: re-arming one replaces its
//! entry, and a promotion for a due time since re-armed or disarmed changes
//! nothing. The restore gate: neither its timeout nor a stop opens it.

use std::time::Duration;

use super::*;
use crate::steps::events::deadline_due;
use crate::test_support::host_class;

fn armed(run: &StepsRun, deadline: Deadline) -> Option<u64> {
    run.sheet().armed.get(&deadline).copied()
}

#[test]
fn re_arming_a_steps_deadline_replaces_its_wake() {
    let inst = host_class::steps("wake_rearm", &["in"]);
    let r = Run::of(&inst).expect("a steps instance");
    let core = crate::poll_core::global();

    r.decide(|d| d.arm(Deadline::ParkIdle, Duration::from_secs(60)));
    let first = armed(r.run, Deadline::ParkIdle).expect("armed");
    r.decide(|d| d.arm(Deadline::ParkIdle, Duration::from_secs(120)));
    let second = armed(r.run, Deadline::ParkIdle).expect("re-armed");

    assert!(second > first);
    assert_eq!(
        core.instance_deadlines(r.inst.seq),
        vec![(Deadline::ParkIdle, second)],
        "one wake, at the later due time"
    );

    r.decide(|d| d.disarm(Deadline::ParkIdle));
    assert!(core.instance_deadlines(r.inst.seq).is_empty());
}

#[test]
fn a_stale_steps_deadline_is_a_no_op() {
    let inst = host_class::steps("wake_stale", &["in"]);
    let r = Run::of(&inst).expect("a steps instance");
    r.decide(|d| d.arm(Deadline::RestoreGate, Duration::from_secs(60)));
    let first = armed(r.run, Deadline::RestoreGate).expect("armed");
    r.decide(|d| d.arm(Deadline::RestoreGate, Duration::from_secs(120)));
    let current = armed(r.run, Deadline::RestoreGate).expect("re-armed");

    deadline_due(&inst, Deadline::RestoreGate, first);

    assert_eq!(
        armed(r.run, Deadline::RestoreGate),
        Some(current),
        "the re-armed deadline still stands"
    );
    assert_eq!(r.run.sheet().phase, Phase::Gated, "the gate did not open");

    deadline_due(&inst, Deadline::RestoreGate, current);
    assert_eq!(armed(r.run, Deadline::RestoreGate), None);
    assert_eq!(
        r.run.sheet().phase,
        Phase::Done,
        "the current one ends the run instead of opening the gate"
    );
    crate::poll_core::global().disarm_instance(r.inst.seq, Deadline::RestoreGate, current);
}

#[test]
fn a_refused_undo_resent_within_its_first_sends_millisecond_keeps_its_ack_wake() {
    const ACK_TIMEOUT: &str = "FLEZHA_STEPS_ACK_TIMEOUT_MS";
    let _env = crate::runner::env_test_guard();
    // Every ack due saturates to one time, as two sends in one millisecond
    // share theirs.
    std::env::set_var(ACK_TIMEOUT, u64::MAX.to_string());
    let inst = host_class::steps("undo_resend_ack", &["in"]);
    let r = Run::of(&inst).expect("a steps instance");
    let id = 7;
    let reference_id = sent_barrier(&r, id, BarrierFor::Undo);

    crate::steps::events::barrier_acked(&Arc::downgrade(&inst), id, Err(refusal("not_owner")));
    std::env::remove_var(ACK_TIMEOUT);

    let due = armed(r.run, Deadline::Ack(id)).expect("the re-send's ack is armed");
    let core = crate::poll_core::global();
    assert_eq!(
        core.instance_deadlines(r.inst.seq),
        vec![(Deadline::Ack(id), due)],
        "the re-send's ack wake survives the refusal's disarm"
    );
    crate::shutdown::drop_pending_ack(&reference_id);
    core.disarm_instance(r.inst.seq, Deadline::Ack(id), due);
}

/// Barrier `id` on `target`, sent once and waiting for its ack; its
/// reference id.
fn sent_barrier(r: &Run<'_>, id: u64, target: BarrierFor) -> String {
    let reference_id = format!("{}-{id}", r.inst.instance_id);
    r.decide(|d| {
        let command = crate::pb::command_request::Command::TaskCommit(Default::default());
        let barrier = Barrier {
            target,
            reference_id: reference_id.clone(),
            command: Some(command),
            sends: 1,
        };
        d.s.barriers.insert(id, barrier);
        d.arm(Deadline::Ack(id), ack_timeout());
    });
    reference_id
}

/// The controller's refusal by its catalogue code's last segment.
fn refusal(reason: &str) -> crate::FlezhaError {
    let details = [
        ("relay", "r".into()),
        ("cell", "c".into()),
        ("seq", 3.into()),
    ];
    crate::FlezhaError::with(&format!("commit.{reason}"), details)
}

/// A stage commit refused because its records were not durable in time waits
/// out a back-off and goes out again under its reference id, until its sends
/// are used up; any other refusal fails the stage at once.
#[test]
fn a_record_not_durable_refusal_is_sent_again_after_a_back_off_and_a_real_one_is_not() {
    const ACK_TIMEOUT: &str = "FLEZHA_STEPS_ACK_TIMEOUT_MS";
    let _env = crate::runner::env_test_guard();
    // An ack wait saturates far past any back-off, so the two are told apart.
    std::env::set_var(ACK_TIMEOUT, u64::MAX.to_string());
    let inst = host_class::steps("refusal_retry", &["in"]);
    let r = Run::of(&inst).expect("a steps instance");
    let weak = Arc::downgrade(&inst);
    let back_off_bound =
        crate::relay::due_after(Duration::from_millis(crate::channel::RETRY_BACKOFF_MAX_MS));

    let reference_id = sent_barrier(&r, 1, BarrierFor::Stage(0));
    for sends in 1..crate::steps::commit::BARRIER_SENDS {
        crate::steps::events::barrier_acked(&weak, 1, Err(refusal("record_not_durable")));
        let due = armed(r.run, Deadline::Ack(1)).expect("the back-off is armed");
        assert!(due <= back_off_bound, "send {sends} waits a back-off");
        assert_eq!(r.run.sheet().barriers[&1].sends, sends, "nothing sent yet");
        deadline_due(&inst, Deadline::Ack(1), due);
        assert_eq!(r.run.sheet().barriers[&1].sends, sends + 1, "sent again");
        assert!(armed(r.run, Deadline::Ack(1)).expect("the ack wait") > back_off_bound);
    }
    crate::steps::events::barrier_acked(&weak, 1, Err(refusal("record_not_durable")));
    assert!(
        !r.run.sheet().barriers.contains_key(&1),
        "its sends used up, it failed"
    );

    crate::shutdown::drop_pending_ack(&reference_id);

    // A fresh run, so the failed stage above cannot stand in for this one.
    let inst = host_class::steps("refusal_real", &["in"]);
    let r = Run::of(&inst).expect("a steps instance");
    let reference_id = sent_barrier(&r, 2, BarrierFor::Stage(0));
    crate::steps::events::barrier_acked(&Arc::downgrade(&inst), 2, Err(refusal("not_owner")));
    std::env::remove_var(ACK_TIMEOUT);
    assert!(
        !r.run.sheet().barriers.contains_key(&2),
        "a real refusal fails at once"
    );
    assert_eq!(armed(r.run, Deadline::Ack(2)), None);
    crate::shutdown::drop_pending_ack(&reference_id);
}

/// What `f` decides for a run bound expecting two restores, one landed
/// carrying its Cell's saga markers so a gate that opened would compensate:
/// checked to start no pass, run no undo and leave the run done with nothing
/// armed, its completion returned.
fn at_the_gate(
    id: &str,
    f: impl FnOnce(&mut Decide<'_, '_, '_>),
) -> (String, String, Vec<u8>, String) {
    let inst = host_class::steps(id, &["in"]);
    let r = Run::of(&inst).expect("a steps instance");
    r.run.bind(crate::steps::Resume {
        expected: 2,
        ..Default::default()
    });
    crate::steps::open(&inst);
    let cell = Arc::clone(&inst.all_slots()[0].cell_identity);
    crate::steps::restore_landed(&inst, &cell, true, true);

    let mut fx = Effects::default();
    f(&mut Decide {
        r: &r,
        s: &mut r.run.sheet(),
        fx: &mut fx,
    });
    assert!(fx.start.is_none() && fx.admit.is_empty(), "no pass starts");
    assert!(
        fx.run_undo.is_none() && fx.frames.is_empty(),
        "nothing is compensated"
    );
    let completed = fx.completed.take().expect("the run ended");
    r.act(fx);
    assert_eq!(r.run.sheet().phase, Phase::Done);
    assert!(crate::poll_core::global()
        .instance_deadlines(r.inst.seq)
        .is_empty());
    completed
}

#[test]
fn a_restore_gate_timeout_ends_the_run_error() {
    assert_eq!(
        at_the_gate("gate_timeout", |d| d.deadline(Deadline::RestoreGate)),
        (
            "error".to_string(),
            "steps restore gate timed out: 1 of 2 restores landed within 30 s".to_string(),
            Vec::new(),
            String::new()
        )
    );
}

#[test]
fn a_stop_at_the_restore_gate_ends_the_run_cancelled() {
    assert_eq!(
        at_the_gate("gate_stop", |d| d.stop()),
        (
            "cancelled".to_string(),
            String::new(),
            Vec::new(),
            String::new()
        )
    );
}

#[test]
fn the_bind_arms_max_duration_as_far_out_as_its_window() {
    let id = "wake_window";
    let json = crate::test_support::host_class::steps_json(id, &["in"], 1).replacen(
        r#""sdk_version":"0.1.0","#,
        r#""sdk_version":"0.1.0","max_duration_ms":60000,"#,
        1,
    );
    crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
    // The global poll core is shared with the tests running beside this one,
    // and one of them may fire a deadline that is due: `None` is a deadline
    // fired before it was read back.
    let due = |remaining: Option<u64>| {
        let inst = RelayInstance::of_class(id, &format!("{id}_{remaining:?}")).expect("registered");
        inst.bind_window(remaining);
        crate::steps::open(&inst);
        let run = inst.steps.as_ref().expect("a steps run");
        let due = armed(run, Deadline::MaxDuration);
        if let Some(due) = due {
            crate::poll_core::global().disarm_instance(inst.seq, Deadline::MaxDuration, due);
        }
        due
    };

    let whole = due(None).expect("the whole window is not due yet");
    let left = due(Some(30_000)).expect("30 s left is not due yet");
    let spent = due(Some(0));

    assert!(
        whole.abs_diff(left + 30_000) < 1_000,
        "30 s left arms half as far out as the whole 60 s window: {whole} vs {left}"
    );
    assert!(
        spent.is_none_or(|spent| spent < left),
        "a spent window is due at once"
    );
}
