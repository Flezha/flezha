//! A flow operator ends from inside: a tasklet's `complete`, or its class's
//! `max_duration`. It runs what its Cells already hold to its outputs, then
//! reports one `OperatorCompleted`, and the controller stops it as it stops a
//! finished steps operator. Nothing here is on the input path: the end is
//! checked from one deadline on the poll core's deferred-wake list.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::cell::CellSlot;
use crate::operator::instance::OperatorInstance;
use crate::steps::Deadline;
use crate::util::LockRecover;

/// How long a completing instance waits before it looks at its Cells again.
const DRAIN_RECHECK: Duration = Duration::from_millis(5);

/// The end a flow instance asked for, until it is reported.
#[derive(Default)]
pub struct FlowEnd {
    /// `(status, result)`: `completed` from a tasklet, `cancelled` from
    /// `max_duration`. A later `complete` replaces the result.
    asked: Mutex<Option<(&'static str, Vec<u8>)>>,
    reported: AtomicBool,
}

/// A flow tasklet's `complete`: the instance ends with `result` once its
/// Cells hold nothing.
pub fn complete(slot: &Arc<CellSlot>, result: Vec<u8>) {
    if let Some(inst) = slot.instance.upgrade() {
        end(&inst, "completed", result);
    }
}

/// The bind is complete: a class with a wall-clock stop arms it.
pub fn bound(inst: &Arc<OperatorInstance>) {
    if inst.steps.is_some() {
        return;
    }
    if let Some(limit) = inst.topology.max_duration {
        arm(inst, Deadline::MaxDuration, limit);
    }
}

fn end(inst: &Arc<OperatorInstance>, status: &'static str, result: Vec<u8>) {
    if inst.end.reported.load(Ordering::Acquire) {
        return;
    }
    let first = {
        let mut asked = inst.end.asked.lock_recover();
        let first = asked.is_none();
        if first || status == "completed" {
            *asked = Some((status, result));
        }
        first
    };
    if first {
        arm(inst, Deadline::Complete, Duration::ZERO);
    }
}

fn arm(inst: &Arc<OperatorInstance>, deadline: Deadline, after: Duration) {
    let due = crate::util::wall_now_ms().saturating_add(after.as_millis() as u64);
    crate::poll_core::global().arm_instance(Arc::downgrade(inst), inst.seq, deadline, due);
}

/// A flow instance's end deadline came due.
pub(crate) fn deadline_due(inst: &Arc<OperatorInstance>, deadline: Deadline) {
    match deadline {
        Deadline::MaxDuration => end(inst, "cancelled", Vec::new()),
        Deadline::Complete if !inst.cells_idle() => arm(inst, Deadline::Complete, DRAIN_RECHECK),
        Deadline::Complete => report(inst),
        _ => {}
    }
}

fn report(inst: &OperatorInstance) {
    if inst.end.reported.swap(true, Ordering::AcqRel) {
        return;
    }
    let Some((status, result)) = inst.end.asked.lock_recover().take() else {
        return;
    };
    crate::runner::emit_operator_completed(
        inst.instance_id.to_string(),
        status.to_string(),
        String::new(),
        result,
    );
}
