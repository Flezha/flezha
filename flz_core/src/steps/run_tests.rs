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

/// What `f` decides for a run bound expecting two restores, one landed
/// carrying its Cell's saga markers so a gate that opened would compensate:
/// checked to start no pass, run no undo and leave the run done with nothing
/// armed, its completion returned.
fn at_the_gate(id: &str, f: impl FnOnce(&mut Decide<'_, '_, '_>)) -> (String, String, Vec<u8>) {
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
            Vec::new()
        )
    );
}

#[test]
fn a_stop_at_the_restore_gate_ends_the_run_cancelled() {
    assert_eq!(
        at_the_gate("gate_stop", |d| d.stop()),
        ("cancelled".to_string(), String::new(), Vec::new())
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
    let due = |remaining: Option<u64>| {
        let inst = RelayInstance::of_class(id, &format!("{id}_{remaining:?}")).expect("registered");
        inst.bind_window(remaining);
        crate::steps::open(&inst);
        let run = inst.steps.as_ref().expect("a steps run");
        let due = armed(run, Deadline::MaxDuration).expect("max_duration armed");
        crate::poll_core::global().disarm_instance(inst.seq, Deadline::MaxDuration, due);
        due
    };

    let whole = due(None);
    let left = due(Some(500));
    let spent = due(Some(0));

    assert!(
        whole.abs_diff(left + 59_500) < 1_000,
        "500 left arms about 0.5 s out, the whole window 60 s: {whole} vs {left}"
    );
    assert!(spent < left, "a spent window is due at once");
}
