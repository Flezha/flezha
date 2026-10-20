//! Per-Cell timers: the pending set, its durable ops, and the arming of
//! processing-time deadlines. The invariant: a timer is folded state — every
//! registration, cancellation and firing is a [`TimerOp`] in the Cell's own
//! record stream, so a restore rebuilds the set through the ordinary fold and
//! a firing's removal rides the same record as its effects. Event-time timers
//! fire off the high-water on the dispatch path; processing-time ones are
//! entries on the poll core's wake list, promoted into the mailbox so
//! `on_timer` is serialized against `run`. No timer owns a thread.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use dashmap::DashMap;
use flz_transport::errors::{ErrorModel, FlezhaError};
use flz_transport::transport::FlezhaStream;
use serde::{Deserialize, Serialize};

use crate::cell::CellSlot;
use crate::error::Code;
use crate::host::HostClock;
pub use crate::schema::{TimerFiring, TimerKind, TimerOp};
use crate::util::LockRecover;

/// A refused timer registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerRefusal {
    EventTimeWithoutEventTimeClock { tasklet: String, clock: String },
    CapExceeded { tasklet: String, max: usize },
}

impl ErrorModel for TimerRefusal {
    fn to_error(&self) -> FlezhaError {
        match self {
            TimerRefusal::EventTimeWithoutEventTimeClock { tasklet, clock } => {
                Code::EventTimeTimerWithoutEventTimeClock.with([
                    ("tasklet", tasklet.as_str().into()),
                    ("clock", clock.as_str().into()),
                ])
            }
            TimerRefusal::CapExceeded { tasklet, max } => Code::TimerCapExceeded
                .with([("tasklet", tasklet.as_str().into()), ("max", (*max).into())]),
        }
    }
}

impl std::fmt::Display for TimerRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_error().message)
    }
}

/// A Cell's pending timers, one ordered set per kind; registering the
/// same timestamp twice is idempotent.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTimers {
    event_time: BTreeSet<u64>,
    processing_time: BTreeSet<u64>,
}

impl PendingTimers {
    fn set_mut(&mut self, kind: TimerKind) -> &mut BTreeSet<u64> {
        match kind {
            TimerKind::EventTime => &mut self.event_time,
            TimerKind::ProcessingTime => &mut self.processing_time,
        }
    }

    fn set(&self, kind: TimerKind) -> &BTreeSet<u64> {
        match kind {
            TimerKind::EventTime => &self.event_time,
            TimerKind::ProcessingTime => &self.processing_time,
        }
    }

    /// Fold one op — the single mutation seam, live and on restore. Whether
    /// the set changed.
    pub fn apply(&mut self, op: &TimerOp) -> bool {
        if op.register {
            self.set_mut(op.kind).insert(op.scheduled_at)
        } else {
            self.set_mut(op.kind).remove(&op.scheduled_at)
        }
    }

    pub fn contains(&self, kind: TimerKind, scheduled_at: u64) -> bool {
        self.set(kind).contains(&scheduled_at)
    }

    pub fn len(&self) -> usize {
        self.event_time.len() + self.processing_time.len()
    }

    pub fn is_empty(&self) -> bool {
        self.event_time.is_empty() && self.processing_time.is_empty()
    }

    /// Timestamps at or below `upto`, ascending.
    pub fn due(&self, kind: TimerKind, upto: u64) -> Vec<u64> {
        self.set(kind).range(..=upto).copied().collect()
    }

    pub fn all(&self, kind: TimerKind) -> Vec<u64> {
        self.set(kind).iter().copied().collect()
    }

    /// One kind as registrations.
    pub fn register_ops(&self, kind: TimerKind) -> Vec<TimerOp> {
        self.all(kind)
            .into_iter()
            .map(|at| TimerOp::register(kind, at))
            .collect()
    }

    /// The full set as registrations — what a compaction re-states ABOVE the
    /// snapshot's seq so truncation cannot lose them.
    pub fn as_register_ops(&self) -> Vec<TimerOp> {
        let mut ops = self.register_ops(TimerKind::EventTime);
        ops.extend(self.register_ops(TimerKind::ProcessingTime));
        ops
    }
}

/// A validated firing on a Cell's work path, carrying the connection
/// `on_timer`'s emits leave on.
#[derive(Clone)]
pub struct TimerFire {
    pub firing: TimerFiring,
    pub stream: Arc<FlezhaStream>,
}

// ----------------------------------------------------------------- session

/// The pending timers a Cell holds unless its Tasklet declares otherwise. A
/// per-Cell timer set is author-controlled memory multiplied by an unbounded
/// Cell count; the cap keeps a runaway registration a refusal, not an OOM.
pub const DEFAULT_MAX_TIMERS_PER_CELL: usize = 10_000;

/// What a Tasklet declares about its timers, resolved once: the cap, the
/// name a refusal reports, its clock, its counters.
#[derive(Clone)]
pub struct TimerPolicy {
    max_per_cell: usize,
    tasklet: &'static str,
    clock: HostClock,
    counters: Arc<TimerCounters>,
}

impl TimerPolicy {
    pub fn new(max_per_cell: usize, tasklet: &'static str, clock: HostClock) -> Self {
        Self {
            max_per_cell,
            tasklet,
            clock,
            counters: counters_for(tasklet),
        }
    }

    /// A session over a COPY of `pending`, seeded with the firing's removal
    /// when there is one.
    pub fn session(
        &self,
        pending: &PendingTimers,
        now_ms: u64,
        firing: Option<(TimerKind, u64)>,
    ) -> RefCell<TimerSession> {
        let mut session = TimerSession {
            pending: pending.clone(),
            ops: Vec::new(),
            policy: self.clone(),
            now_ms,
            firing: None,
        };
        if let Some((kind, scheduled_at)) = firing {
            session.open_firing(kind, scheduled_at);
        }
        RefCell::new(session)
    }

    /// The rules every registration answers to: the clock, then the cap over
    /// a set of `len` that does not already hold it.
    fn admit(&self, kind: TimerKind, pending: bool, len: usize) -> Result<(), TimerRefusal> {
        let refusal = if kind == TimerKind::EventTime && self.clock != HostClock::EventTime {
            TimerRefusal::EventTimeWithoutEventTimeClock {
                tasklet: self.tasklet.to_string(),
                clock: self.clock.name().to_string(),
            }
        } else if !pending && len >= self.max_per_cell {
            TimerRefusal::CapExceeded {
                tasklet: self.tasklet.to_string(),
                max: self.max_per_cell,
            }
        } else {
            return Ok(());
        };
        self.counters.refused_total.fetch_add(1, Ordering::Relaxed);
        Err(refusal)
    }

    /// A hosted item's ops judged in order by the rules a session applies at
    /// call time, against the borrowed live set plus a delta of the keys the
    /// ops touched, so the set is never copied.
    pub fn judge(&self, live: &PendingTimers, ops: &[TimerOp]) -> Result<(), TimerRefusal> {
        let mut delta: HashMap<(TimerKind, u64), bool> = HashMap::new();
        let mut len = live.len();
        for op in ops {
            let key = (op.kind, op.scheduled_at);
            let pending = delta
                .get(&key)
                .copied()
                .unwrap_or_else(|| live.contains(op.kind, op.scheduled_at));
            if op.register {
                self.admit(op.kind, pending, len)?;
            }
            match (pending, op.register) {
                (false, true) => len += 1,
                (true, false) => len -= 1,
                _ => {}
            }
            delta.insert(key, op.register);
        }
        Ok(())
    }

    /// Count one op that changed the set; the firing's removal counts as
    /// fired, once.
    fn count(&self, op: &TimerOp, firing: &mut Option<(TimerKind, u64)>) {
        let counters = &self.counters;
        if op.register {
            counters.registered_total.fetch_add(1, Ordering::Relaxed);
            counters.pending.fetch_add(1, Ordering::Relaxed);
            return;
        }
        counters.pending.fetch_sub(1, Ordering::Relaxed);
        if *firing == Some((op.kind, op.scheduled_at)) {
            *firing = None;
            counters.fired_total.fetch_add(1, Ordering::Relaxed);
            TIMERS_FIRED_TOTAL.fetch_add(1, Ordering::Relaxed);
        } else {
            counters.cancelled_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A hosted item's settled ops onto the Cell's live set, counting each
    /// that changed it, then armed.
    pub fn commit(
        &self,
        slot: &Arc<CellSlot>,
        ops: &[TimerOp],
        mut firing: Option<(TimerKind, u64)>,
        stream: &Arc<FlezhaStream>,
    ) {
        if ops.is_empty() {
            return;
        }
        {
            let mut live = slot.timers.lock_recover();
            for op in ops {
                if live.apply(op) {
                    self.count(op, &mut firing);
                }
            }
        }
        apply_ops(slot, ops, stream);
    }
}

/// The timer surface of ONE invocation, over a COPY of the pending set: a
/// failed attempt's registrations must not survive its retry, so only the
/// attempt that succeeds [`Self::commit`]s — where the counters move too.
pub struct TimerSession {
    pending: PendingTimers,
    ops: Vec<TimerOp>,
    policy: TimerPolicy,
    /// The invocation's `ctx.now()`, which [`Self::register_after`] anchors
    /// to so a replay registers the same absolute timestamp.
    now_ms: u64,
    /// The firing the session opened on: op 0 is its removal, counted as a
    /// firing rather than a cancel.
    firing: Option<(TimerKind, u64)>,
}

impl TimerSession {
    /// Seed the session with the firing's removal, so it rides the SAME
    /// record as what `on_timer` applies: either both land or the whole
    /// firing repeats.
    fn open_firing(&mut self, kind: TimerKind, scheduled_at: u64) {
        let op = TimerOp::cancel(kind, scheduled_at);
        self.pending.apply(&op);
        self.ops.push(op);
        self.firing = Some((kind, scheduled_at));
    }

    pub fn now(&self) -> u64 {
        self.now_ms
    }

    pub fn register_at_event_time(&mut self, scheduled_at: u64) -> Result<u64, TimerRefusal> {
        self.register(TimerKind::EventTime, scheduled_at)
    }

    pub fn register_after(&mut self, delay: Duration) -> Result<u64, TimerRefusal> {
        let at = self.now_ms.saturating_add(delay.as_millis() as u64);
        self.register(TimerKind::ProcessingTime, at)
    }

    fn register(&mut self, kind: TimerKind, scheduled_at: u64) -> Result<u64, TimerRefusal> {
        let pending = self.pending.contains(kind, scheduled_at);
        self.policy.admit(kind, pending, self.pending.len())?;
        let op = TimerOp::register(kind, scheduled_at);
        if self.pending.apply(&op) {
            self.ops.push(op);
        }
        Ok(scheduled_at)
    }

    /// Cancel by scheduled timestamp; `false` when none was pending.
    pub fn cancel(&mut self, kind: TimerKind, scheduled_at: u64) -> bool {
        let op = TimerOp::cancel(kind, scheduled_at);
        let changed = self.pending.apply(&op);
        if changed {
            self.ops.push(op);
        }
        changed
    }

    /// Publish onto the live pending set and answer the ops to make durable
    /// and arm; only for the attempt that completed.
    pub fn commit(self, live: &mut PendingTimers) -> Vec<TimerOp> {
        let mut firing = self.firing;
        for op in &self.ops {
            self.policy.count(op, &mut firing);
        }
        *live = self.pending;
        self.ops
    }
}

// ---------------------------------------------------------------- counters

/// Per-Tasklet timer counters: monotonic totals plus a signed `pending`
/// gauge, all `Relaxed`.
#[derive(Debug, Default)]
pub struct TimerCounters {
    pub pending: AtomicI64,
    pub registered_total: AtomicU64,
    pub fired_total: AtomicU64,
    pub cancelled_total: AtomicU64,
    pub refused_total: AtomicU64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerCountersView {
    pub tasklet: &'static str,
    pub pending: i64,
    pub registered_total: u64,
    pub fired_total: u64,
    pub cancelled_total: u64,
    pub refused_total: u64,
}

static PER_TASKLET: std::sync::OnceLock<DashMap<&'static str, Arc<TimerCounters>>> =
    std::sync::OnceLock::new();

fn registry() -> &'static DashMap<&'static str, Arc<TimerCounters>> {
    PER_TASKLET.get_or_init(DashMap::new)
}

pub fn counters_for(tasklet: &'static str) -> Arc<TimerCounters> {
    if let Some(existing) = registry().get(tasklet) {
        return Arc::clone(existing.value());
    }
    Arc::clone(
        registry()
            .entry(tasklet)
            .or_insert_with(|| Arc::new(TimerCounters::default()))
            .value(),
    )
}

#[cfg(test)]
pub fn counters_snapshot() -> Vec<TimerCountersView> {
    let mut out: Vec<TimerCountersView> = registry()
        .iter()
        .map(|e| {
            let c = e.value();
            TimerCountersView {
                tasklet: e.key(),
                pending: c.pending.load(Ordering::Relaxed),
                registered_total: c.registered_total.load(Ordering::Relaxed),
                fired_total: c.fired_total.load(Ordering::Relaxed),
                cancelled_total: c.cancelled_total.load(Ordering::Relaxed),
                refused_total: c.refused_total.load(Ordering::Relaxed),
            }
        })
        .collect();
    out.sort_by_key(|v| v.tasklet);
    out
}

/// Timers fired, process-wide; rides the heartbeat.
pub static TIMERS_FIRED_TOTAL: AtomicU64 = AtomicU64::new(0);

pub fn timers_fired_total() -> u64 {
    TIMERS_FIRED_TOTAL.load(Ordering::Relaxed)
}

/// Processing-time timers armed, counted off the wake list that holds them.
pub fn timers_armed() -> usize {
    crate::poll_core::global().timers_armed()
}

// ------------------------------------------------ arming from committed ops

/// Reflect committed timer ops onto the wake list; event-time timers need
/// nothing here. A registration a steps relay stage's item made waits on that
/// stage instead, so the timer never comes due inside the stage that set it.
pub fn apply_ops(slot: &Arc<CellSlot>, ops: &[TimerOp], stream: &Arc<FlezhaStream>) {
    let core = crate::poll_core::global();
    for op in ops.iter().filter(|op| op.kind == TimerKind::ProcessingTime) {
        if slot.topology.steps.is_some() && crate::steps::hold_arm(slot, op, stream) {
            continue;
        }
        if op.register {
            core.arm_timer(slot, op.scheduled_at, Arc::clone(stream));
        } else {
            core.disarm_timer(&slot.cell_identity, op.scheduled_at);
        }
    }
}

/// Arm the wake that closes an open checkpoint interval at `max_delay_ms`. A
/// close leaves its spent entry armed, so the wake closes only an interval
/// whose own delay passed.
pub fn arm_interval_deadline(slot: &Arc<CellSlot>) {
    let deadline = {
        let mut buffer = slot.flush_buffer.lock_recover();
        slot.dispatcher.interval_deadline(&mut buffer)
    };
    if let Some(at) = deadline {
        crate::poll_core::global().arm_interval_close(slot, at);
    }
}

/// A steps relay firing served inside a pass leaves the pending set durably
/// when its stage commits, as a message leaves the input log at the commit
/// that consumed it: its removal is taken out of the ops an attempt makes
/// durable and held on the pass until the stage's ack. The live set drops it
/// at once, so it never fires twice while the pass runs.
pub fn hold_firing_removal(
    slot: &Arc<CellSlot>,
    ops: &mut Vec<TimerOp>,
    firing: Option<(TimerKind, u64)>,
) {
    let Some((kind, scheduled_at)) = firing else {
        return;
    };
    let Some(at) = ops
        .iter()
        .position(|op| !op.register && op.kind == kind && op.scheduled_at == scheduled_at)
    else {
        return;
    };
    if crate::steps::hold_removal(slot, ops[at]) {
        ops.remove(at);
    }
}

/// The pending set a compaction re-states: the live one plus every firing
/// whose removal a steps stage still holds, which is not durable yet.
pub fn restated(slot: &Arc<CellSlot>, live: &PendingTimers) -> PendingTimers {
    let mut pending = live.clone();
    for op in crate::steps::held_removals(slot) {
        pending.apply(&TimerOp::register(op.kind, op.scheduled_at));
    }
    pending
}

/// Re-arm a restored Cell's processing-time timers; anything past its
/// deadline fires on the next promotion, ascending. A steps relay Cell first
/// drops every firing its journal records as committed.
pub fn rearm_restored(slot: &Arc<CellSlot>) {
    drop_committed_firings(slot);
    arm_restored(slot);
}

/// Restored Cells whose processing-time timers wait for a secondary stream.
static AWAITING_STREAM: Mutex<Vec<Weak<CellSlot>>> = Mutex::new(Vec::new());

fn arm_restored(slot: &Arc<CellSlot>) {
    let ops = slot
        .timers
        .lock_recover()
        .register_ops(TimerKind::ProcessingTime);
    if ops.is_empty() {
        return;
    }
    // Held across the read so an install between the read and the push
    // still finds this Cell in the list.
    let mut awaiting = AWAITING_STREAM.lock_recover();
    // A restore has no originating input to inherit a connection from; the live
    // secondary stream is the same one the flusher ships on.
    let Some(stream) = crate::state_sink::secondary_stream() else {
        awaiting.push(Arc::downgrade(slot));
        return;
    };
    drop(awaiting);
    // Armed at once, not held: a timer set in an earlier pass joins a stage
    // that is running.
    let core = crate::poll_core::global();
    for op in ops {
        core.arm_timer(slot, op.scheduled_at, Arc::clone(&stream));
    }
}

/// Arm the timers of every Cell restored before a secondary stream existed;
/// an install calls it once.
pub(crate) fn arm_awaiting_stream() {
    let awaiting = std::mem::take(&mut *AWAITING_STREAM.lock_recover());
    for slot in awaiting.iter().filter_map(Weak::upgrade) {
        arm_restored(&slot);
    }
}

/// A firing whose stage committed before the crash, while its removal was
/// still unwritten: it must not fire again.
fn drop_committed_firings(slot: &Arc<CellSlot>) {
    if slot.topology.steps.is_none() {
        return;
    }
    let fired = crate::steps::committed_firings(slot);
    if fired.is_empty() {
        return;
    }
    let removals: Vec<TimerOp> = {
        let mut pending = slot.timers.lock_recover();
        fired
            .into_iter()
            .map(|(kind, at)| TimerOp::cancel(kind, at))
            .filter(|op| pending.apply(op))
            .collect()
    };
    if !removals.is_empty() {
        slot.dispatcher.write_timer_ops(slot, removals);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_is_idempotent_and_cancellation_removes() {
        let mut pending = PendingTimers::default();
        assert!(pending.apply(&TimerOp::register(TimerKind::ProcessingTime, 31_000)));
        assert!(!pending.apply(&TimerOp::register(TimerKind::ProcessingTime, 31_000)));
        assert_eq!(pending.len(), 1);
        assert!(pending.apply(&TimerOp::cancel(TimerKind::ProcessingTime, 31_000)));
        assert!(pending.is_empty());
    }

    #[test]
    fn the_two_kinds_are_separate_sets() {
        let mut pending = PendingTimers::default();
        pending.apply(&TimerOp::register(TimerKind::EventTime, 100));
        pending.apply(&TimerOp::register(TimerKind::ProcessingTime, 100));
        assert_eq!(pending.len(), 2);
        pending.apply(&TimerOp::cancel(TimerKind::EventTime, 100));
        assert!(pending.contains(TimerKind::ProcessingTime, 100));
        assert!(!pending.contains(TimerKind::EventTime, 100));
    }

    #[test]
    fn due_is_ascending_and_inclusive_of_the_bound() {
        let mut pending = PendingTimers::default();
        for at in [300u64, 100, 200] {
            pending.apply(&TimerOp::register(TimerKind::EventTime, at));
        }
        assert_eq!(pending.due(TimerKind::EventTime, 200), vec![100, 200]);
        assert_eq!(pending.due(TimerKind::EventTime, 99), Vec::<u64>::new());
    }

    /// The compaction re-statement is the only record of the timers once the
    /// ops below the snapshot are truncated.
    #[test]
    fn register_ops_round_trip_the_whole_set() {
        let mut pending = PendingTimers::default();
        pending.apply(&TimerOp::register(TimerKind::EventTime, 5));
        pending.apply(&TimerOp::register(TimerKind::ProcessingTime, 9));
        pending.apply(&TimerOp::register(TimerKind::ProcessingTime, 7));

        let mut rebuilt = PendingTimers::default();
        for op in pending.as_register_ops() {
            rebuilt.apply(&op);
        }
        assert_eq!(rebuilt, pending);
    }

    /// A registration, a cancellation, a firing and a refusal each move
    /// exactly one counter.
    #[test]
    fn the_per_tasklet_counters_follow_the_committed_ops() {
        let policy = TimerPolicy {
            max_per_cell: 1,
            tasklet: "counter_probe_tasklet",
            clock: HostClock::Wall,
            counters: counters_for("counter_probe_tasklet"),
        };
        let mut live = PendingTimers::default();

        let session = policy.session(&live, 1_000, None);
        session
            .borrow_mut()
            .register_after(Duration::from_secs(30))
            .expect("first registration fits the cap");
        session
            .borrow_mut()
            .register_after(Duration::from_secs(60))
            .expect_err("the second exceeds it");
        session.into_inner().commit(&mut live);

        let session = policy.session(&live, 1_000, None);
        assert!(session
            .borrow_mut()
            .cancel(TimerKind::ProcessingTime, 31_000));
        session.into_inner().commit(&mut live);

        let session = policy.session(&live, 9_000, Some((TimerKind::ProcessingTime, 9_000)));
        session.into_inner().commit(&mut live);

        let view = counters_snapshot()
            .into_iter()
            .find(|v| v.tasklet == "counter_probe_tasklet")
            .expect("the Tasklet appears in the registry on first use");
        assert_eq!(view.registered_total, 1);
        assert_eq!(view.cancelled_total, 1);
        assert_eq!(view.refused_total, 1);
        assert_eq!(view.fired_total, 1, "a firing is not counted as a cancel");
        assert_eq!(
            view.pending, -1,
            "the firing removed a timer this probe never registered"
        );
    }

    const PT: TimerKind = TimerKind::ProcessingTime;

    fn holding(ats: &[u64]) -> PendingTimers {
        let mut pending = PendingTimers::default();
        for &at in ats {
            pending.apply(&TimerOp::register(PT, at));
        }
        pending
    }

    /// At the cap a new registration is refused, both at call time and by
    /// the settle's judge, and the set keeps what it held.
    #[test]
    fn at_the_cap_a_new_registration_is_refused_and_the_set_unchanged() {
        let policy = TimerPolicy::new(1, "cap_probe_tasklet", HostClock::Wall);
        let mut live = holding(&[100]);
        let refusal = Err(TimerRefusal::CapExceeded {
            tasklet: "cap_probe_tasklet".to_string(),
            max: 1,
        });

        let session = policy.session(&live, 0, None);
        assert_eq!(session.borrow_mut().register(PT, 200).map(drop), refusal);
        assert!(session.into_inner().commit(&mut live).is_empty());
        assert_eq!(live, holding(&[100]));
        let ops = [TimerOp::register(PT, 200)];
        assert_eq!(policy.judge(&live, &ops), refusal);
    }

    /// Registering a timer the set already holds adds nothing, so the cap
    /// cannot refuse it.
    #[test]
    fn re_registering_a_pending_timer_at_the_cap_is_not_refused() {
        let policy = TimerPolicy::new(1, "cap_probe_tasklet", HostClock::Wall);
        let live = holding(&[100]);
        let session = policy.session(&live, 0, None);
        assert_eq!(session.borrow_mut().register(PT, 100), Ok(100));
        let ops = [TimerOp::register(PT, 100)];
        assert_eq!(policy.judge(&live, &ops), Ok(()));
    }

    /// An event-time timer needs an event-time clock, whatever room is left.
    #[test]
    fn an_event_time_registration_without_an_event_time_clock_is_refused() {
        let policy = TimerPolicy::new(10, "clock_probe_tasklet", HostClock::Wall);
        let live = PendingTimers::default();
        let refusal = Err(TimerRefusal::EventTimeWithoutEventTimeClock {
            tasklet: "clock_probe_tasklet".to_string(),
            clock: "wall".to_string(),
        });
        let session = policy.session(&live, 0, None);
        assert_eq!(
            session.borrow_mut().register_at_event_time(5).map(drop),
            refusal
        );
        let ops = [TimerOp::register(TimerKind::EventTime, 5)];
        assert_eq!(policy.judge(&live, &ops), refusal);
    }

    fn draw(seed: &mut u64, bound: u64) -> u64 {
        flz_transport::utility::noise::xorshift64(seed);
        *seed % bound
    }

    /// Few timestamps over both kinds, so ops collide with the set and with
    /// each other.
    fn random_op(seed: &mut u64) -> TimerOp {
        let kind = [PT, TimerKind::EventTime][draw(seed, 2) as usize];
        let at = draw(seed, 6) * 100;
        if draw(seed, 3) == 0 {
            TimerOp::cancel(kind, at)
        } else {
            TimerOp::register(kind, at)
        }
    }

    /// The judge over the borrowed set and a delta answers every op list as a
    /// session over a copy does, and on success the folded ops leave the set
    /// the session commits.
    #[test]
    fn the_judge_answers_as_a_session_over_a_copy() {
        let mut seed = 0x9E37_79B9_7F4A_7C15;
        for round in 0..2_000 {
            let clock = [HostClock::Wall, HostClock::EventTime][draw(&mut seed, 2) as usize];
            let policy =
                TimerPolicy::new(draw(&mut seed, 6) as usize, "judge_probe_tasklet", clock);
            let mut live = PendingTimers::default();
            for _ in 0..draw(&mut seed, 6) {
                let op = random_op(&mut seed);
                live.apply(&TimerOp::register(op.kind, op.scheduled_at));
            }
            let ops: Vec<TimerOp> = (0..draw(&mut seed, 8))
                .map(|_| random_op(&mut seed))
                .collect();

            let session = policy.session(&live, 0, None);
            let called = ops.iter().try_for_each(|op| {
                let mut session = session.borrow_mut();
                if op.register {
                    session.register(op.kind, op.scheduled_at).map(drop)
                } else {
                    session.cancel(op.kind, op.scheduled_at);
                    Ok(())
                }
            });
            let judged = policy.judge(&live, &ops);
            assert_eq!(judged, called, "round {round}: {ops:?} over {live:?}");
            if judged.is_ok() {
                let mut folded = live.clone();
                for op in &ops {
                    folded.apply(op);
                }
                session.into_inner().commit(&mut live);
                assert_eq!(folded, live, "round {round}: {ops:?}");
            }
        }
    }
}
