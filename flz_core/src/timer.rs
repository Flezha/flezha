//! Per-Entity timers: the pending set, its durable ops, and the arming of
//! processing-time deadlines.
//!
//! A timer is state, not a scheduler entry. Registration, cancellation and
//! firing each append a [`TimerOp`] to the Entity's durable record stream, so
//! the pending set is rebuilt by the ordinary change-log fold on restore — no
//! side-file, no crash marker, no completeness oracle. Ops ride the WAL two
//! ways, both of which the controller already stores verbatim:
//!
//!   * outside the effectively-once tier, as a `format_id = 3` record whose
//!     body is a postcard `Vec<TimerOp>`;
//!   * inside it, as `EffectRecordBody::timers`, so a firing's effects and the
//!     removal of the timer that produced them land in ONE record.
//!
//! Event-time timers need nothing here: they fire off the Entity's event-time
//! high-water, on the dispatch path. Only the processing-time kind needs a
//! clock of its own — an entry on the poll core's shared deferred-wake list,
//! whose expiry pushes a [`TimerFire`] onto the target Entity's existing
//! mailbox so `on_timer` is serialized against `run` by the
//! single-active-drainer gate already there. No timer owns a thread.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use flz_transport::transport::FlezhaStream;
use serde::{Deserialize, Serialize};

use crate::entity::EntitySlot;
use crate::util::LockRecover;

pub use crate::schema::{TimerKind, TimerOp};

/// A refused timer registration. The typed layer converts these into its
/// `ProcedureError` variants; the core knows only the facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerRefusal {
    /// An event-time timer under a clock that is not an event time.
    EventTimeWithoutEventTimeClock { procedure: String, clock: String },
    /// The per-entity pending cap was reached.
    CapExceeded { procedure: String, max: usize },
}

impl std::fmt::Display for TimerRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimerRefusal::EventTimeWithoutEventTimeClock { procedure, clock } => write!(
                f,
                "procedure `{procedure}` declared clock `{clock}`; an event-time timer needs an event-time clock"
            ),
            TimerRefusal::CapExceeded { procedure, max } => {
                write!(f, "procedure `{procedure}` reached its timer cap ({max})")
            }
        }
    }
}

/// An Entity's pending timers: one ordered set per kind. Pending timers are a
/// **set**, so registering the same kind and timestamp twice is idempotent.
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

    /// Fold one op. The single mutation seam: registration, cancellation,
    /// firing and the restore replay all go through it, so the live set and a
    /// rebuilt one cannot drift. Returns whether the set actually changed.
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

    /// Scheduled timestamps at or below `upto`, ascending — what a high-water
    /// crossing (event time) or a due promotion (processing time) must fire.
    pub fn due(&self, kind: TimerKind, upto: u64) -> Vec<u64> {
        self.set(kind).range(..=upto).copied().collect()
    }

    /// Every pending timer of one kind, ascending.
    pub fn all(&self, kind: TimerKind) -> Vec<u64> {
        self.set(kind).iter().copied().collect()
    }

    /// One kind expressed as registrations — what the restore re-arm hands the
    /// wake list, and half of what a compaction re-states.
    pub fn register_ops(&self, kind: TimerKind) -> Vec<TimerOp> {
        self.all(kind)
            .into_iter()
            .map(|at| TimerOp::register(kind, at))
            .collect()
    }

    /// The full set expressed as registrations. A compaction re-states the
    /// pending timers this way at a seq ABOVE the snapshot's, so truncating
    /// the ops the snapshot covers cannot lose them.
    pub fn as_register_ops(&self) -> Vec<TimerOp> {
        let mut ops = self.register_ops(TimerKind::EventTime);
        ops.extend(self.register_ops(TimerKind::ProcessingTime));
        ops
    }
}

/// A validated timer firing on an Entity's work path. Processing-time firings
/// are promoted off the wake list onto the mailbox; event-time firings are
/// generated at batch assembly when an admitted input advances the
/// high-water. Carries the secondary stream captured at arming (wake list) or
/// inherited from the admitting input (assembly) so `on_timer`'s plugin
/// emits leave on the same connection an input's would.
#[derive(Clone)]
pub struct TimerFire {
    pub kind: TimerKind,
    pub scheduled_at: u64,
    pub stream: Arc<FlezhaStream>,
}

// ----------------------------------------------------------------- session

/// What a Procedure declares about its timers, resolved once per Procedure
/// rather than at every invocation: the bound, the names a refusal reports, and
/// whether its Clock can serve an event-time deadline at all. Carries the
/// counter set too, so a session costs no registry lookup.
///
/// [`ProcedureCtx`]: crate::procedure::ProcedureCtx
#[derive(Clone)]
pub struct TimerPolicy {
    max_per_entity: usize,
    procedure: &'static str,
    event_time_clock: bool,
    clock: &'static str,
    counters: Arc<TimerCounters>,
}

impl TimerPolicy {
    /// Construct from the class's declared timer facts (the typed layer's
    /// `entity::timer::timer_policy_for::<S>()` reads them off `S`).
    pub fn new(
        max_per_entity: usize,
        procedure: &'static str,
        event_time_clock: bool,
        clock: &'static str,
    ) -> Self {
        Self {
            max_per_entity,
            procedure,
            event_time_clock,
            clock,
            counters: counters_for(procedure),
        }
    }

    /// Open a session over a COPY of `pending`, optionally seeded with the
    /// removal of the timer that is firing.
    pub fn session(
        &self,
        pending: &PendingTimers,
        now: u64,
        firing: Option<(TimerKind, u64)>,
    ) -> RefCell<TimerSession> {
        let mut session = TimerSession {
            pending: pending.clone(),
            ops: Vec::new(),
            policy: self.clone(),
            now,
            firing: false,
        };
        if let Some((kind, scheduled_at)) = firing {
            session.open_firing(kind, scheduled_at);
        }
        RefCell::new(session)
    }
}

/// The timer surface for ONE invocation, borrowed by [`ProcedureCtx`].
///
/// It works on a **copy** of the Entity's pending set: the retry ladder re-runs
/// a failed attempt, and a registration that attempt made must not survive it.
/// The dispatcher calls [`Self::commit`] only on the attempt that succeeded —
/// which is also where the counters move, so a refused-then-retried invocation
/// is counted once.
///
/// [`ProcedureCtx`]: crate::procedure::ProcedureCtx
pub struct TimerSession {
    pending: PendingTimers,
    ops: Vec<TimerOp>,
    policy: TimerPolicy,
    /// The invocation's `ctx.now()`. [`Self::register_after`] is anchored to
    /// it, which is what makes a replayed invocation register the same
    /// absolute timestamp the live one did.
    now: u64,
    /// Set when the session opens on a firing: op 0 is the fired timer's
    /// removal, so [`Self::commit`] counts it as a firing rather than an author
    /// cancel.
    firing: bool,
}

impl TimerSession {
    /// Seed the session with the removal of the timer that is firing, so it
    /// rides the SAME durable record as whatever `on_timer` goes on to apply
    /// and emit. A crash between the two is therefore impossible: either both
    /// landed, or the timer is still pending and the whole firing repeats.
    fn open_firing(&mut self, kind: TimerKind, scheduled_at: u64) {
        let op = TimerOp::cancel(kind, scheduled_at);
        self.pending.apply(&op);
        self.ops.push(op);
        self.firing = true;
    }

    pub fn now(&self) -> u64 {
        self.now
    }

    /// Register an event-time timer at an absolute timestamp.
    pub fn register_at_event_time(&mut self, scheduled_at: u64) -> Result<u64, TimerRefusal> {
        if !self.policy.event_time_clock {
            self.refused();
            return Err(TimerRefusal::EventTimeWithoutEventTimeClock {
                procedure: self.policy.procedure.to_string(),
                clock: self.policy.clock.to_string(),
            });
        }
        self.register(TimerKind::EventTime, scheduled_at)
    }

    /// Register a processing-time timer `delay` after `ctx.now()`.
    pub fn register_after(&mut self, delay: Duration) -> Result<u64, TimerRefusal> {
        let at = self.now.saturating_add(delay.as_millis() as u64);
        self.register(TimerKind::ProcessingTime, at)
    }

    fn register(&mut self, kind: TimerKind, scheduled_at: u64) -> Result<u64, TimerRefusal> {
        if !self.pending.contains(kind, scheduled_at)
            && self.pending.len() >= self.policy.max_per_entity
        {
            self.refused();
            return Err(TimerRefusal::CapExceeded {
                procedure: self.policy.procedure.to_string(),
                max: self.policy.max_per_entity,
            });
        }
        let op = TimerOp::register(kind, scheduled_at);
        if self.pending.apply(&op) {
            self.ops.push(op);
        }
        Ok(scheduled_at)
    }

    /// Cancel a pending timer by its scheduled timestamp. `false` when none
    /// was pending — a cancellation nobody registered changes nothing.
    pub fn cancel(&mut self, kind: TimerKind, scheduled_at: u64) -> bool {
        let op = TimerOp::cancel(kind, scheduled_at);
        let changed = self.pending.apply(&op);
        if changed {
            self.ops.push(op);
        }
        changed
    }

    fn refused(&self) {
        self.policy
            .counters
            .refused_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Publish the session onto the Entity's live pending set and return the
    /// ops to make durable (and to arm/disarm on the wake list). Called by the
    /// dispatcher only for the attempt that completed.
    pub fn commit(self, live: &mut PendingTimers) -> Vec<TimerOp> {
        let counters = &self.policy.counters;
        for (idx, op) in self.ops.iter().enumerate() {
            if op.register {
                counters.registered_total.fetch_add(1, Ordering::Relaxed);
                counters.pending.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            counters.pending.fetch_sub(1, Ordering::Relaxed);
            if self.firing && idx == 0 {
                counters.fired_total.fetch_add(1, Ordering::Relaxed);
                TIMERS_FIRED_TOTAL.fetch_add(1, Ordering::Relaxed);
            } else {
                counters.cancelled_total.fetch_add(1, Ordering::Relaxed);
            }
        }
        *live = self.pending;
        self.ops
    }
}

// ---------------------------------------------------------------- counters

/// Per-Procedure timer counters. Monotonic totals plus a signed `pending`
/// gauge, all `Relaxed` — read by observability at its leisure, exactly like
/// [`crate::entity::EntityCounters`].
#[derive(Debug, Default)]
pub struct TimerCounters {
    pub pending: AtomicI64,
    pub registered_total: AtomicU64,
    pub fired_total: AtomicU64,
    pub cancelled_total: AtomicU64,
    pub refused_total: AtomicU64,
}

/// Immutable view of one Procedure's counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerCountersView {
    pub procedure: &'static str,
    pub pending: i64,
    pub registered_total: u64,
    pub fired_total: u64,
    pub cancelled_total: u64,
    pub refused_total: u64,
}

static PER_PROCEDURE: std::sync::OnceLock<DashMap<&'static str, Arc<TimerCounters>>> =
    std::sync::OnceLock::new();

fn registry() -> &'static DashMap<&'static str, Arc<TimerCounters>> {
    PER_PROCEDURE.get_or_init(DashMap::new)
}

/// The counter set for one Procedure, created on first use.
pub fn counters_for(procedure: &'static str) -> Arc<TimerCounters> {
    if let Some(existing) = registry().get(procedure) {
        return Arc::clone(existing.value());
    }
    Arc::clone(
        registry()
            .entry(procedure)
            .or_insert_with(|| Arc::new(TimerCounters::default()))
            .value(),
    )
}

/// Every Procedure's counters, for `flz`-side observability and tests.
pub fn counters_snapshot() -> Vec<TimerCountersView> {
    let mut out: Vec<TimerCountersView> = registry()
        .iter()
        .map(|e| {
            let c = e.value();
            TimerCountersView {
                procedure: e.key(),
                pending: c.pending.load(Ordering::Relaxed),
                registered_total: c.registered_total.load(Ordering::Relaxed),
                fired_total: c.fired_total.load(Ordering::Relaxed),
                cancelled_total: c.cancelled_total.load(Ordering::Relaxed),
                refused_total: c.refused_total.load(Ordering::Relaxed),
            }
        })
        .collect();
    out.sort_by_key(|v| v.procedure);
    out
}

/// Process-global count of timers that have fired. Shipped in every
/// `data_heartbeat` frame beside `dispatches` / `blocked_parks`.
pub static TIMERS_FIRED_TOTAL: AtomicU64 = AtomicU64::new(0);

pub fn timers_fired_total() -> u64 {
    TIMERS_FIRED_TOTAL.load(Ordering::Relaxed)
}

/// Processing-time timers currently armed — the occupancy a runaway
/// registration shows up in before it is fatal. Counted off the poll core's
/// wake list, which holds them, rather than tracked beside it.
pub fn timers_armed() -> usize {
    crate::poll_core::global().timers_armed()
}

// ------------------------------------------------ arming from committed ops

/// Reflect an invocation's committed timer ops onto the poll core's wake list.
/// Event-time timers need nothing here — the Entity's high-water fires them.
/// The Entity's identity is the list key, so timers armed by two instances
/// sharing a numeric `ProcedureId` stay each other's business.
pub fn apply_ops(slot: &Arc<EntitySlot>, ops: &[TimerOp], stream: &Arc<FlezhaStream>) {
    let core = crate::poll_core::global();
    for op in ops.iter().filter(|op| op.kind == TimerKind::ProcessingTime) {
        if op.register {
            core.arm_timer(slot, op.scheduled_at, Arc::clone(stream));
        } else {
            core.disarm_timer(&slot.entity_identity, op.scheduled_at);
        }
    }
}

/// Re-arm a restored Entity's processing-time timers. The pending set came back
/// through the change-log fold; anything already past its deadline fires on the
/// next promotion, in ascending order. Event-time timers need nothing — the
/// restored high-water is their trigger.
pub fn rearm_restored(slot: &Arc<EntitySlot>) {
    let ops = slot
        .timers
        .lock_recover()
        .register_ops(TimerKind::ProcessingTime);
    if ops.is_empty() {
        return;
    }
    // A restore has no originating input to inherit a connection from; the live
    // secondary stream is the same one the flusher ships on.
    let Some(stream) = crate::state_sink::secondary_stream() else {
        tracing::debug!(
            target: "flezha::timer",
            entity = slot.dispatcher.entity_label(),
            pending = ops.len(),
            "restored processing-time timers not re-armed: no secondary stream installed"
        );
        return;
    };
    apply_ops(slot, &ops, &stream);
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

    /// The compaction re-statement must reproduce the set exactly: it is the
    /// only record of the timers once the ops below the snapshot are truncated.
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

    /// The counters an operator reads: a registration, a cancellation, a firing
    /// and a refusal each move exactly one of them.
    #[test]
    fn the_per_procedure_counters_follow_the_committed_ops() {
        let policy = TimerPolicy {
            max_per_entity: 1,
            procedure: "counter_probe_procedure",
            event_time_clock: false,
            clock: "WallClock",
            counters: counters_for("counter_probe_procedure"),
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
            .find(|v| v.procedure == "counter_probe_procedure")
            .expect("the Procedure appears in the registry on first use");
        assert_eq!(view.registered_total, 1);
        assert_eq!(view.cancelled_total, 1);
        assert_eq!(view.refused_total, 1);
        assert_eq!(view.fired_total, 1, "a firing is not counted as a cancel");
        assert_eq!(
            view.pending, -1,
            "the firing removed a timer this probe never registered"
        );
    }

    /// Pins the postcard bytes the controller's `Flezha.State.Wire`
    /// decodes — the two sides share this fixture, so an encoding change
    /// breaks loudly here rather than silently on the wire.
    #[test]
    fn timer_op_postcard_bytes_are_the_cross_language_fixture() {
        let ops = vec![
            TimerOp::register(TimerKind::ProcessingTime, 31_000),
            TimerOp::cancel(TimerKind::EventTime, 100),
        ];
        assert_eq!(
            postcard::to_allocvec(&ops).unwrap(),
            vec![
                0x02, // 2 ops
                0x01, 0x98, 0xF2, 0x01, 0x01, // ProcessingTime, 31000, register
                0x00, 0x64, 0x00, // EventTime, 100, cancel
            ]
        );
    }
}
