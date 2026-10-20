//! The core's one deferred-wake list: retry backoffs, processing-time timer
//! deadlines, interval closes, push re-arms and instance deadlines, keyed by
//! clock lane, then due time. It owns no
//! thread — entries are promoted by whichever poller next enters or wakes —
//! and nothing in a promotion waits for mailbox room: a firing that cannot be
//! admitted is re-recorded strictly later, so the list IS the holdback.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use flz_transport::transport::FlezhaStream;

use super::PollCore;
use crate::cell::{CellItem, CellSlot};
use crate::dynamic::DynamicCellRegistry;
use crate::relay::Deadline;
use crate::timer::{TimerFire, TimerKind};
use crate::util::LockRecover;

/// How long a wake whose target mailbox was full waits before the next try;
/// strictly non-zero so a pass cannot re-service the entry it just deferred.
const WAKE_RETRY_BACKOFF_MS: u64 = 5;

/// The clock a wake's due time is on. A processing-time timer's
/// `scheduled_at` is wall time, which the controller re-arms across restarts;
/// every other wake is a span from now, kept on the monotonic clock so a
/// wall-clock step cannot move it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Lane {
    Wall,
    Mono,
}

impl Lane {
    const ALL: [Lane; 2] = [Lane::Wall, Lane::Mono];

    pub(super) fn now_ms(self) -> u64 {
        match self {
            Lane::Wall => crate::util::wall_now_ms(),
            Lane::Mono => crate::util::monotonic_ms() as u64,
        }
    }

    /// The least key due at `due_at_ms` on this lane: `Retry(0)` is the
    /// least tie-break.
    fn floor(self, due_at_ms: u64) -> WakeKey {
        WakeKey {
            lane: self,
            due_at_ms,
            tiebreak: WakeTiebreak::Retry(0),
        }
    }
}

/// A deferred wake's place: its lane, due time, then a total tie-break.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct WakeKey {
    lane: Lane,
    pub(super) due_at_ms: u64,
    pub(super) tiebreak: WakeTiebreak,
}

impl WakeKey {
    /// Due at `due_at_ms` on the tie-break's lane.
    pub(super) fn at(due_at_ms: u64, tiebreak: WakeTiebreak) -> Self {
        Self {
            lane: tiebreak.lane(),
            due_at_ms,
            tiebreak,
        }
    }

    /// Due `delay_ms` from now on the tie-break's lane.
    pub(super) fn after(delay_ms: u64, tiebreak: WakeTiebreak) -> Self {
        let due_at_ms = tiebreak.lane().now_ms().saturating_add(delay_ms);
        Self::at(due_at_ms, tiebreak)
    }
}

/// `Retry` comes first, so `Retry(0)` is the least value ([`Lane::floor`]).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum WakeTiebreak {
    /// Recording order, for a wake no re-arm replaces: a retry (a slot's
    /// gates stay held across its backoff, so a second for the same cell
    /// cannot exist), a push re-arm or an idle sweep.
    Retry(u64),
    /// The Cell's identity: arming `(cell, scheduled_at)` is an idempotent
    /// replace, and the instance id inside it keeps two instances sharing a
    /// `TaskletId` from disarming each other's timers.
    Timer(Arc<Vec<u8>>),
    /// The Cell whose open checkpoint interval is due to close; distinct
    /// from `Timer` so the two cannot evict each other on one millisecond.
    Interval(Arc<Vec<u8>>),
    /// One deadline of one instance, by its seq: re-arming it replaces the
    /// entry.
    Instance(u64, Deadline),
}

impl WakeTiebreak {
    fn lane(&self) -> Lane {
        match self {
            WakeTiebreak::Timer(_) => Lane::Wall,
            _ => Lane::Mono,
        }
    }
}

pub(super) enum Wake {
    /// Finish a deferred re-arm; the slot's gates are still held.
    Retry(Arc<CellSlot>),
    /// Re-attempt a gated producer's pushes: the lost-wake fallback behind the
    /// consumer's wait list. A no-op on a producer that already drained.
    PushRetry(Arc<CellSlot>),
    /// A processing-time deadline. `Weak` so an evicted Cell's timers die
    /// with it; the stream is the one captured at arming. `scheduled_at` is
    /// what the firing is stamped with and what cancels it — the key's due
    /// time is only when to attempt admission.
    Timer {
        slot: Weak<CellSlot>,
        stream: Arc<FlezhaStream>,
        scheduled_at: u64,
    },
    /// A checkpoint interval's `max_delay_ms` elapsed; `Weak` as a timer is.
    CloseInterval { slot: Weak<CellSlot> },
    /// An instance's deadline came due; `Weak` so a removed instance's
    /// deadlines die with it.
    Instance {
        instance: Weak<crate::relay::instance::RelayInstance>,
        deadline: Deadline,
    },
    /// A dynamic tasklet's idle sweep came due; `Weak` as an instance's
    /// deadline is.
    Sweep {
        registry: Weak<DynamicCellRegistry>,
        idle_ttl: Duration,
    },
}

/// The deferred-wake list; recording an entry returns at once.
pub(super) struct DeferredWake {
    entries: Mutex<BTreeMap<WakeKey, Wake>>,
    /// Due time of each lane's earliest entry (`u64::MAX` when empty), so
    /// the poll path decides "nothing is due" with two loads and reads a
    /// lane's clock only while it holds an entry.
    earliest: [AtomicU64; 2],
}

impl DeferredWake {
    pub(super) fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            earliest: [AtomicU64::new(u64::MAX), AtomicU64::new(u64::MAX)],
        }
    }

    pub(super) fn record(&self, key: WakeKey, wake: Wake) {
        let lane = key.lane;
        let mut entries = self.entries.lock_recover();
        entries.insert(key, wake);
        self.refresh(&entries, lane);
    }

    fn remove(&self, key: &WakeKey) {
        let mut entries = self.entries.lock_recover();
        entries.remove(key);
        self.refresh(&entries, key.lane);
    }

    /// Drop every timer armed for one Cell: eviction is destructive, so
    /// the timers go with the state.
    fn discard_cell(&self, cell: &Arc<Vec<u8>>) {
        let mut entries = self.entries.lock_recover();
        let doomed: Vec<WakeKey> = entries
            .keys()
            .filter(|k| match &k.tiebreak {
                WakeTiebreak::Timer(armed) | WakeTiebreak::Interval(armed) => armed == cell,
                WakeTiebreak::Retry(_) | WakeTiebreak::Instance(..) => false,
            })
            .cloned()
            .collect();
        for key in doomed {
            entries.remove(&key);
        }
        for lane in Lane::ALL {
            self.refresh(&entries, lane);
        }
    }

    /// Remove `slot`'s pending retry, the twin of [`Self::discard_cell`] for
    /// the one entry a Cell's retry leaves; `false` when none was pending.
    pub(super) fn take_retry(&self, slot: &Arc<CellSlot>) -> bool {
        let mut entries = self.entries.lock_recover();
        let key = entries
            .iter()
            .find(|(_, wake)| matches!(wake, Wake::Retry(armed) if Arc::ptr_eq(armed, slot)))
            .map(|(key, _)| key.clone());
        let taken = key.is_some_and(|key| entries.remove(&key).is_some());
        self.refresh(&entries, Lane::Mono);
        taken
    }

    /// `lane`'s cached earliest, and the lane's `clock` read beside it; `None`
    /// when the lane is empty, without a clock read.
    fn lane_clock(&self, lane: Lane, clock: &impl Fn(Lane) -> u64) -> Option<(u64, u64)> {
        let earliest = self.earliest[lane as usize].load(Ordering::Acquire);
        (earliest != u64::MAX).then(|| (earliest, clock(lane)))
    }

    /// Whether anything came due on either lane.
    pub(super) fn any_due(&self) -> bool {
        Lane::ALL.into_iter().any(|lane| {
            self.lane_clock(lane, &Lane::now_ms)
                .is_some_and(|(earliest, now)| earliest <= now)
        })
    }

    fn take_due(&self, clock: &impl Fn(Lane) -> u64) -> Vec<(WakeKey, Wake)> {
        let mut due = Vec::new();
        for lane in Lane::ALL {
            let Some((earliest, now)) = self.lane_clock(lane, clock) else {
                continue;
            };
            if earliest > now {
                continue;
            }
            let mut entries = self.entries.lock_recover();
            let range = lane.floor(0)..lane.floor(now.saturating_add(1));
            due.extend(entries.extract_if(range, |_, _| true));
            self.refresh(&entries, lane);
        }
        due
    }

    /// Until the earliest entry comes due; `None` when empty.
    fn next_due_in(&self) -> Option<Duration> {
        Lane::ALL
            .into_iter()
            .filter_map(|lane| self.lane_clock(lane, &Lane::now_ms))
            .map(|(earliest, now)| Duration::from_millis(earliest.saturating_sub(now)))
            .min()
    }

    /// Processing-time timers armed, counted from the entries so the number
    /// cannot drift from what is scheduled.
    fn timers_armed(&self) -> usize {
        self.entries
            .lock_recover()
            .keys()
            .filter(|k| matches!(k.tiebreak, WakeTiebreak::Timer(_)))
            .count()
    }

    fn refresh(&self, entries: &BTreeMap<WakeKey, Wake>, lane: Lane) {
        let earliest = entries
            .range(lane.floor(0)..)
            .next()
            .filter(|(k, _)| k.lane == lane)
            .map_or(u64::MAX, |(k, _)| k.due_at_ms);
        self.earliest[lane as usize].store(earliest, Ordering::Release);
    }
}

impl PollCore {
    /// How long to park: until `deadline`, never past the nearest due wake.
    /// Takes no lock — a recorder takes the list's lock and then `ready`,
    /// which both callers hold, so locking here would close a cycle.
    pub(super) fn park_slice(&self, deadline: Instant, now: Instant) -> Duration {
        let mut wait = deadline.saturating_duration_since(now);
        if let Some(until_due) = self.wakes.next_due_in() {
            wait = wait.min(until_due);
        }
        wait
    }

    /// Hand every due entry to its effect, once per [`Self::poll`] pass.
    /// Nothing here waits for mailbox room: a promoter counts as a poller
    /// inside, so one held on a full mailbox would stop the runner and hide
    /// it. A firing that cannot be admitted is re-recorded later instead.
    pub(super) fn promote_due(&self) {
        self.promote_due_by(&Lane::now_ms);
    }

    /// [`Self::promote_due`] at the times `clock` reads for each lane.
    pub(super) fn promote_due_by(&self, clock: &impl Fn(Lane) -> u64) {
        for (key, wake) in self.wakes.take_due(clock) {
            let Some((slot, item, retry)) = self.wake_target(key.due_at_ms, wake) else {
                continue;
            };
            if crate::admission::try_admit(&slot, item).is_err() {
                self.defer_wake(key.tiebreak, retry);
            }
        }
    }

    /// What one wake due at `due_at_ms` admits, and the entry to put back if
    /// it will not fit; a re-arm, a deadline or a sweep has no item and
    /// finishes here, a wake on an evicted Cell is dropped.
    fn wake_target(&self, due_at_ms: u64, wake: Wake) -> Option<(Arc<CellSlot>, CellItem, Wake)> {
        match wake {
            Wake::Retry(slot) => {
                self.release_and_rearm(&slot);
                None
            }
            Wake::PushRetry(slot) => {
                self.rearm_deferred_pushes(&slot);
                None
            }
            // A close silently dropped would hold the interval's outputs
            // until the next input — exactly what the delay exists to
            // prevent.
            Wake::CloseInterval { slot } => {
                let live = slot.upgrade()?;
                Some((live, CellItem::CloseInterval, Wake::CloseInterval { slot }))
            }
            Wake::Timer {
                slot,
                stream,
                scheduled_at,
            } => timer_target(slot, stream, scheduled_at),
            Wake::Instance { instance, deadline } => {
                let instance = instance.upgrade()?;
                crate::relay::sleep::deadline_due(&instance, deadline, due_at_ms);
                None
            }
            Wake::Sweep { registry, idle_ttl } => {
                crate::dynamic::sweep_due(registry.upgrade()?, idle_ttl);
                None
            }
        }
    }
}

/// A due processing-time timer's firing, and the entry to put back. A timer
/// silently lost is a bug the author cannot see. The firing keeps its armed
/// `scheduled_at` across every deferral: that is what the pending set cancels
/// by, and what `ctx.now()` reads inside `on_timer`.
fn timer_target(
    slot: Weak<CellSlot>,
    stream: Arc<FlezhaStream>,
    scheduled_at: u64,
) -> Option<(Arc<CellSlot>, CellItem, Wake)> {
    let live = slot.upgrade()?;
    let fire = TimerFire {
        kind: TimerKind::ProcessingTime,
        scheduled_at,
        stream: Arc::clone(&stream),
    };
    // A steps relay's firing is a trigger of its run: the pass sheet decides
    // when it reaches the Cell, and admits it itself.
    if live.topology.steps.is_some() {
        crate::steps::firing_promoted(&live, fire);
        return None;
    }
    let wake = Wake::Timer {
        slot,
        stream,
        scheduled_at,
    };
    Some((live, CellItem::Timer(fire), wake))
}

impl PollCore {
    /// Put a wake the target had no room for back on the list, due after
    /// [`WAKE_RETRY_BACKOFF_MS`] — strictly later, or `park_slice` would
    /// return zero and spin the promoter.
    fn defer_wake(&self, tiebreak: WakeTiebreak, wake: Wake) {
        self.wakes
            .record(WakeKey::after(WAKE_RETRY_BACKOFF_MS, tiebreak), wake);
    }

    /// Arm one processing-time timer; re-arming `(cell, scheduled_at)`
    /// replaces the entry.
    pub fn arm_timer(&self, slot: &Arc<CellSlot>, scheduled_at: u64, stream: Arc<FlezhaStream>) {
        self.wakes.record(
            WakeKey::at(
                scheduled_at,
                WakeTiebreak::Timer(Arc::clone(&slot.cell_identity)),
            ),
            Wake::Timer {
                slot: Arc::downgrade(slot),
                stream,
                scheduled_at,
            },
        );
        self.notify_waiters();
    }

    /// Arm a Cell's interval close at `deadline_ms` on the monotonic clock;
    /// re-arming replaces the entry.
    pub fn arm_interval_close(&self, slot: &Arc<CellSlot>, deadline_ms: u64) {
        self.wakes.record(
            WakeKey::at(
                deadline_ms,
                WakeTiebreak::Interval(Arc::clone(&slot.cell_identity)),
            ),
            Wake::CloseInterval {
                slot: Arc::downgrade(slot),
            },
        );
        self.notify_waiters();
    }

    /// Disarm one timer. Idempotent; no notify, since dropping an entry can
    /// only lengthen a wait, never miss one.
    pub fn disarm_timer(&self, cell: &Arc<Vec<u8>>, scheduled_at: u64) {
        self.wakes.remove(&WakeKey::at(
            scheduled_at,
            WakeTiebreak::Timer(Arc::clone(cell)),
        ));
    }

    /// Arm one of the deadlines of the instance numbered `seq`, due at
    /// `due_at_ms` on the monotonic clock, in place of the entry armed for
    /// `old`, so a re-arm leaves one entry. A promotion decides whether it
    /// still applies.
    pub fn rearm_instance(
        &self,
        instance: Weak<crate::relay::instance::RelayInstance>,
        seq: u64,
        deadline: Deadline,
        old: Option<u64>,
        due_at_ms: u64,
    ) {
        if let Some(old) = old.filter(|&old| old != due_at_ms) {
            self.disarm_instance(seq, deadline, old);
        }
        self.wakes.record(
            WakeKey::at(due_at_ms, WakeTiebreak::Instance(seq, deadline)),
            Wake::Instance { instance, deadline },
        );
        self.notify_waiters();
    }

    /// Arm `registry`'s next idle sweep, for Cells idle past `idle_ttl`,
    /// `every` from now.
    pub(crate) fn arm_sweep(
        &self,
        registry: &Arc<DynamicCellRegistry>,
        idle_ttl: Duration,
        every: Duration,
    ) {
        let tiebreak = WakeTiebreak::Retry(self.next_wake.fetch_add(1, Ordering::Relaxed));
        self.wakes.record(
            WakeKey::after(every.as_millis() as u64, tiebreak),
            Wake::Sweep {
                registry: Arc::downgrade(registry),
                idle_ttl,
            },
        );
        self.notify_waiters();
    }

    /// Disarm one instance deadline armed for `due_at_ms`. Idempotent.
    pub fn disarm_instance(&self, seq: u64, deadline: Deadline, due_at_ms: u64) {
        self.wakes.remove(&WakeKey::at(
            due_at_ms,
            WakeTiebreak::Instance(seq, deadline),
        ));
    }

    /// The deadlines armed for the instance numbered `seq`, with their due
    /// times.
    #[cfg(test)]
    pub(crate) fn instance_deadlines(&self, seq: u64) -> Vec<(Deadline, u64)> {
        self.wakes
            .entries
            .lock_recover()
            .keys()
            .filter_map(|key| match key.tiebreak {
                WakeTiebreak::Instance(armed, deadline) if armed == seq => {
                    Some((deadline, key.due_at_ms))
                }
                _ => None,
            })
            .collect()
    }

    /// Drop every timer armed for one Cell, as the reaper evicts it.
    pub fn discard_cell_timers(&self, cell: &Arc<Vec<u8>>) {
        self.wakes.discard_cell(cell);
    }

    /// Processing-time timers armed — the `timers_armed` heartbeat key.
    pub fn timers_armed(&self) -> usize {
        self.wakes.timers_armed()
    }

    /// Wake blocked pollers to re-compute their wait. Taking `ready` makes the
    /// notify unmissable; called with the list's lock already released, the
    /// order the two are always taken in.
    pub(super) fn notify_waiters(&self) {
        let _ready = self.ready.lock_recover();
        self.cv.notify_all();
    }
}
