//! Sleeping when idle, for an instance of either kind whose class declares
//! it: one idle deadline on the poll core's deferred-wake list, one idle
//! question over the instance's Cells, one `IterPark` frame. A bounded operator asks
//! the question from its pass sheet (it re-arms on every committed boundary);
//! an unbounded operator arms once at bind and re-arms from its Cells' latest activity, so
//! nothing on the input path touches the wake list.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::bounded::Deadline;
use crate::operator::instance::OperatorInstance;
use crate::util::LockRecover;

/// How long an instance stays idle before it sleeps (`FLEZHA_PARK_IDLE_MS`).
const PARK_IDLE_DEFAULT_MS: u64 = 600_000;
const ENV_PARK_IDLE_MS: &str = "FLEZHA_PARK_IDLE_MS";

/// How long a rehydrated instance waits for the owed input tail before it
/// may sleep regardless; a lost forward must not wedge it.
pub(crate) const OWED_INBOX_WAIT: Duration = Duration::from_secs(60);

pub(crate) fn park_idle() -> Duration {
    Duration::from_millis(crate::util::env_parse(
        ENV_PARK_IDLE_MS,
        PARK_IDLE_DEFAULT_MS,
    ))
}

/// What an instance's sleep keeps between its deadlines: the input tail the
/// bind still owes it, and, for an unbounded operator, the armed due time and whether it
/// already asked to sleep.
#[derive(Default)]
pub struct IdleWatch {
    owed_inbox: AtomicU32,
    /// Due time of the armed idle deadline, `0` when none is armed.
    due: AtomicU64,
    bound_at_ms: AtomicI64,
    asleep: AtomicBool,
}

impl OperatorInstance {
    /// The bind's owed input tail: the idle clock waits for it.
    pub fn owe_inbox(&self, count: u32) {
        self.idle.owed_inbox.store(count, Ordering::Release);
    }

    pub(crate) fn owed_inbox(&self) -> u32 {
        self.idle.owed_inbox.load(Ordering::Acquire)
    }

    /// One owed record arrived at the entrance.
    pub(crate) fn owed_arrived(&self) {
        let _ = self
            .idle
            .owed_inbox
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
    }

    /// Whether this instance may sleep: its class declares it and its state
    /// is durable.
    pub(crate) fn sleeps(&self) -> bool {
        self.topology.sleep_when_idle && self.durable()
    }

    /// Whether every Cell holds nothing queued, in flight, held back,
    /// retrying, staged or unflushed. A pending timer keeps an unbounded operator awake;
    /// a bounded operator sleeps with it, its park naming the earliest.
    pub(crate) fn cells_idle(&self) -> bool {
        let timers_hold = self.bounded.is_none();
        !self.ladders_running()
            && self
                .all_slots()
                .iter()
                .all(|slot| slot.quiescent(timers_hold))
    }

    /// The earliest processing-time timer pending on any Cell: what a
    /// sleeping bounded operator wakes at. Event-time timers wait for an input,
    /// which wakes a sleeper anyway.
    pub(crate) fn earliest_processing_timer(&self) -> Option<u64> {
        self.all_slots()
            .iter()
            .filter_map(|slot| {
                let pending = slot.timers.lock_recover();
                pending
                    .all(crate::timer::TimerKind::ProcessingTime)
                    .first()
                    .copied()
            })
            .min()
    }
}

/// The instance is bound: an unbounded operator whose class sleeps arms its idle
/// deadline once. A bounded operator arms its own on every committed boundary.
pub fn bound(inst: &Arc<OperatorInstance>) {
    if inst.bounded.is_some() || !inst.sleeps() {
        return;
    }
    inst.idle
        .bound_at_ms
        .store(crate::util::monotonic_ms(), Ordering::Release);
    arm(inst, park_idle());
}

fn arm(inst: &Arc<OperatorInstance>, after: Duration) {
    let due = crate::util::wall_now_ms().saturating_add(after.as_millis() as u64);
    let old = inst.idle.due.swap(due, Ordering::AcqRel);
    let core = crate::poll_core::global();
    if old != 0 && old != due {
        core.disarm_instance(inst.seq, Deadline::ParkIdle, old);
    }
    core.arm_instance(Arc::downgrade(inst), inst.seq, Deadline::ParkIdle, due);
}

/// A deadline the deferred-wake list promoted, for either kind.
pub(crate) fn deadline_due(inst: &Arc<OperatorInstance>, deadline: Deadline, due: u64) {
    if inst.bounded.is_some() {
        return crate::bounded::deadline_due(inst, deadline, due);
    }
    if deadline != Deadline::ParkIdle
        || inst
            .idle
            .due
            .compare_exchange(due, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
    {
        return;
    }
    unbounded_idle_due(inst);
}

/// An unbounded operator's idle deadline came due: re-arm while the owed tail is still
/// awaited or a Cell was active inside the window, else sleep if every Cell
/// is idle.
fn unbounded_idle_due(inst: &Arc<OperatorInstance>) {
    if inst.idle.asleep.load(Ordering::Acquire) {
        return;
    }
    let window = park_idle();
    let now = crate::util::monotonic_ms();
    let bound_at = inst.idle.bound_at_ms.load(Ordering::Acquire);
    let owed_wait = OWED_INBOX_WAIT.as_millis() as i64;
    if inst.owed_inbox() > 0 && now - bound_at < owed_wait {
        let left = Duration::from_millis((owed_wait - (now - bound_at)) as u64);
        return arm(inst, window.min(left));
    }
    let latest = inst
        .all_slots()
        .iter()
        .map(|slot| slot.last_activity_ms.load(Ordering::Relaxed))
        .fold(bound_at, i64::max);
    let quiet = Duration::from_millis(now.saturating_sub(latest).max(0) as u64);
    if quiet < window {
        return arm(inst, window - quiet);
    }
    if !inst.cells_idle() {
        return arm(inst, window);
    }
    inst.idle.asleep.store(true, Ordering::Release);
    let (reference_id, command) = iter_park(&inst.instance_id, 0, None, None);
    crate::runner::emit_control_request(reference_id, command);
}

/// `IterPark` for the boundary an instance sleeps at (`0` for an unbounded operator),
/// with its wake deadline and the last message it parked unread, so the
/// controller releases it; the controller evicts the instance and wakes it
/// on the next input or the deadline.
pub(crate) fn iter_park(
    instance_id: &str,
    iter: u32,
    wake_deadline_ms: Option<u64>,
    parked_offset: Option<u64>,
) -> (String, crate::pb::command_request::Command) {
    (
        format!(
            "iter-park-{}-{}-{}",
            instance_id,
            iter,
            crate::util::nonce_suffix()
        ),
        crate::pb::command_request::Command::IterPark(crate::pb::IterPark {
            operator_id: instance_id.to_string(),
            iter,
            wake_deadline_ms: wake_deadline_ms.unwrap_or(0),
            consumed_offset: parked_offset,
        }),
    )
}
