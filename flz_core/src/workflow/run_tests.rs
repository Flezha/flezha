//! A workflow deadline on the deferred-wake list: re-arming one replaces its
//! entry, and a promotion for a due time since re-armed or disarmed changes
//! nothing.

use std::time::Duration;

use super::*;
use crate::test_support::host_class;

fn armed(run: &WorkflowRun, deadline: Deadline) -> Option<u64> {
    run.sheet().armed.get(&deadline).copied()
}

#[test]
fn re_arming_a_workflow_deadline_replaces_its_wake() {
    let inst = host_class::workflow("wake_rearm", &["in"]);
    let r = Run::of(&inst).expect("a workflow instance");
    let core = crate::poll_core::global();

    r.decide(|d| d.arm(Deadline::ParkIdle, Duration::from_secs(60)));
    let first = armed(r.run, Deadline::ParkIdle).expect("armed");
    r.decide(|d| d.arm(Deadline::ParkIdle, Duration::from_secs(120)));
    let second = armed(r.run, Deadline::ParkIdle).expect("re-armed");

    assert!(second > first);
    assert_eq!(
        core.workflow_deadlines(r.run.seq),
        vec![(Deadline::ParkIdle, second)],
        "one wake, at the later due time"
    );

    r.decide(|d| d.disarm(Deadline::ParkIdle));
    assert!(core.workflow_deadlines(r.run.seq).is_empty());
}

#[test]
fn a_stale_workflow_deadline_is_a_no_op() {
    let inst = host_class::workflow("wake_stale", &["in"]);
    let r = Run::of(&inst).expect("a workflow instance");
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
    assert_ne!(
        r.run.sheet().phase,
        Phase::Gated,
        "the current one opens it"
    );
    crate::poll_core::global().disarm_workflow(r.run.seq, Deadline::RestoreGate, current);
}
