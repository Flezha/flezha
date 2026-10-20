//! A flow relay ends from inside: a tasklet's `complete`, or its class's
//! `max_duration`. It runs what its Cells already hold to its outputs, then
//! reports one `RelayCompleted`, and the controller stops it as it stops a
//! finished steps relay. Nothing here is on the input path: the end is
//! checked from one deadline on the poll core's deferred-wake list.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::cell::CellSlot;
use crate::relay::instance::RelayInstance;
use crate::relay::Deadline;
use crate::util::LockRecover;

/// How long a completing instance waits before it looks at its Cells again.
const DRAIN_RECHECK: Duration = Duration::from_millis(5);

/// The end a flow instance asked for, until it is reported, and for either
/// run what is left of its `max_duration` window, as its bind carried it.
#[derive(Default)]
pub struct FlowEnd {
    /// `(status, result)`: `completed` from a tasklet, `cancelled` from
    /// `max_duration`. A later `complete` replaces the result.
    asked: Mutex<Option<(&'static str, Vec<u8>)>>,
    reported: AtomicBool,
    window_left: OnceLock<Duration>,
    /// Due time of the armed `MaxDuration`, `0` when none is armed.
    max_due: AtomicU64,
}

#[cfg(test)]
impl FlowEnd {
    /// An end was asked for, whether or not it was reported yet.
    pub(crate) fn requested(&self) -> bool {
        self.asked.lock_recover().is_some() || self.reported.load(Ordering::Acquire)
    }
}

/// A flow tasklet's `complete`: the instance ends with `result` once its
/// Cells hold nothing.
pub fn complete(slot: &Arc<CellSlot>, result: Vec<u8>) {
    if let Some(inst) = slot.instance.upgrade() {
        end(&inst, "completed", result);
    }
}

impl RelayInstance {
    /// What is left of the run's `max_duration` window, which the controller
    /// counts from the run's first bind; `None` leaves the class's whole one.
    pub(crate) fn bind_window(&self, remaining_ms: Option<u64>) {
        if let Some(ms) = remaining_ms {
            let _ = self.end.window_left.set(Duration::from_millis(ms));
        }
    }

    /// How far out this bind arms `MaxDuration`: the window the bind carried,
    /// else the class's whole one; `None` for a class with no stop.
    pub(crate) fn max_duration(&self) -> Option<Duration> {
        let whole = self.topology.max_duration?;
        Some(self.end.window_left.get().copied().unwrap_or(whole))
    }
}

/// The bind is complete: a class with a wall-clock stop arms it.
pub fn bound(inst: &Arc<RelayInstance>) {
    if inst.steps.is_some() {
        return;
    }
    if let Some(limit) = inst.max_duration() {
        crate::relay::rearm(inst, Deadline::MaxDuration, &inst.end.max_due, limit);
    }
}

fn end(inst: &Arc<RelayInstance>, status: &'static str, result: Vec<u8>) {
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

fn arm(inst: &Arc<RelayInstance>, deadline: Deadline, after: Duration) {
    let due = crate::util::wall_now_ms().saturating_add(after.as_millis() as u64);
    crate::poll_core::global().arm_instance(Arc::downgrade(inst), inst.seq, deadline, due);
}

/// A flow instance's end deadline came due.
pub(crate) fn deadline_due(inst: &Arc<RelayInstance>, deadline: Deadline) {
    match deadline {
        Deadline::MaxDuration => end(inst, "cancelled", Vec::new()),
        Deadline::Complete if !inst.cells_idle() => arm(inst, Deadline::Complete, DRAIN_RECHECK),
        Deadline::Complete => report(inst),
        _ => {}
    }
}

fn report(inst: &RelayInstance) {
    if inst.end.reported.swap(true, Ordering::AcqRel) {
        return;
    }
    let Some((status, result)) = inst.end.asked.lock_recover().take() else {
        return;
    };
    crate::runner::emit_relay_completed(
        inst.instance_id.to_string(),
        status.to_string(),
        String::new(),
        result,
    );
}
