//! The core's one deferred-wake list: retry backoffs, processing-time timer
//! deadlines, interval closes, push re-arms and instance deadlines, keyed by
//! due time. It owns no
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
use crate::timer::{TimerFire, TimerKind};
use crate::util::LockRecover;

/// How long a wake whose target mailbox was full waits before the next try;
/// strictly non-zero so a pass cannot re-service the entry it just deferred.
const WAKE_RETRY_BACKOFF_MS: u64 = 5;

/// A deferred wake's place: due time, then a total tie-break.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct WakeKey {
    pub(super) due_at_ms: u64,
    pub(super) tiebreak: WakeTiebreak,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum WakeTiebreak {
    /// The Cell's identity: arming `(cell, scheduled_at)` is an idempotent
    /// replace, and the instance id inside it keeps two instances sharing a
    /// `TaskletId` from disarming each other's timers.
    Timer(Arc<Vec<u8>>),
    /// Recording order; a slot's gates stay held across its backoff, so a
    /// second retry for the same cell cannot exist.
    Retry(u64),
    /// The Cell whose open checkpoint interval is due to close; distinct
    /// from `Timer` so the two cannot evict each other on one millisecond.
    Interval(Arc<Vec<u8>>),
    /// One deadline of one instance, by its seq: re-arming it replaces the
    /// entry.
    Instance(u64, crate::steps::Deadline),
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
        instance: Weak<crate::operator::instance::OperatorInstance>,
        deadline: crate::steps::Deadline,
    },
}

/// The deferred-wake list; recording an entry returns at once.
pub(super) struct DeferredWake {
    entries: Mutex<BTreeMap<WakeKey, Wake>>,
    /// Due time of the earliest entry (`u64::MAX` when empty), so the poll
    /// path decides "nothing is due" with one load.
    earliest: AtomicU64,
}

impl DeferredWake {
    pub(super) fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            earliest: AtomicU64::new(u64::MAX),
        }
    }

    pub(super) fn record(&self, key: WakeKey, wake: Wake) {
        let mut entries = self.entries.lock_recover();
        entries.insert(key, wake);
        self.refresh(&entries);
    }

    fn remove(&self, key: &WakeKey) {
        let mut entries = self.entries.lock_recover();
        entries.remove(key);
        self.refresh(&entries);
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
        self.refresh(&entries);
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
        self.refresh(&entries);
        taken
    }

    /// Whether anything came due, as one load on the cached earliest.
    pub(super) fn any_due(&self, now: u64) -> bool {
        self.earliest.load(Ordering::Acquire) <= now
    }

    fn take_due(&self, now: u64) -> Vec<(WakeKey, Wake)> {
        if !self.any_due(now) {
            return Vec::new();
        }
        let mut entries = self.entries.lock_recover();
        let mut due = Vec::new();
        while let Some((key, wake)) = entries.pop_first() {
            if key.due_at_ms > now {
                entries.insert(key, wake);
                break;
            }
            due.push((key, wake));
        }
        self.refresh(&entries);
        due
    }

    /// Until the earliest entry comes due; `None` when empty.
    fn next_due_in(&self, now: u64) -> Option<Duration> {
        let due = self.earliest.load(Ordering::Acquire);
        (due != u64::MAX).then(|| Duration::from_millis(due.saturating_sub(now)))
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

    fn refresh(&self, entries: &BTreeMap<WakeKey, Wake>) {
        let earliest = entries
            .first_key_value()
            .map_or(u64::MAX, |(k, _)| k.due_at_ms);
        self.earliest.store(earliest, Ordering::Release);
    }
}

impl PollCore {
    /// How long to park: until `deadline`, never past the nearest due wake.
    /// Takes no lock — a recorder takes the list's lock and then `ready`,
    /// which both callers hold, so locking here would close a cycle.
    pub(super) fn park_slice(&self, deadline: Instant, now: Instant) -> Duration {
        let mut wait = deadline.saturating_duration_since(now);
        if let Some(until_due) = self.wakes.next_due_in(crate::util::wall_now_ms()) {
            wait = wait.min(until_due);
        }
        wait
    }

    /// Hand every due entry to its effect, once per [`Self::poll`] pass.
    /// Nothing here waits for mailbox room: a promoter counts as a poller
    /// inside, so one held on a full mailbox would stop the runner and hide
    /// it. A firing that cannot be admitted is re-recorded later instead.
    pub(super) fn promote_due(&self) {
        let now = crate::util::wall_now_ms();
        for (key, wake) in self.wakes.take_due(now) {
            if let Wake::Instance { instance, deadline } = wake {
                if let Some(instance) = instance.upgrade() {
                    crate::operator::sleep::deadline_due(&instance, deadline, key.due_at_ms);
                }
                continue;
            }
            let Some((slot, item, retry)) = self.wake_target(wake) else {
                continue;
            };
            if crate::admission::try_admit(&slot, item).is_err() {
                self.defer_wake(now, key.tiebreak, retry);
            }
        }
    }

    /// What one due wake admits, and the entry to put back if it will not
    /// fit; a re-arm has no item and finishes here, a wake on an evicted
    /// Cell is dropped.
    fn wake_target(&self, wake: Wake) -> Option<(Arc<CellSlot>, CellItem, Wake)> {
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
            // A timer silently lost is a bug the author cannot see. The
            // firing keeps its armed `scheduled_at` across every deferral:
            // that is what the pending set cancels by, and what `ctx.now()`
            // reads inside `on_timer`.
            Wake::Timer {
                slot,
                stream,
                scheduled_at,
            } => {
                let live = slot.upgrade()?;
                let fire = TimerFire {
                    kind: TimerKind::ProcessingTime,
                    scheduled_at,
                    stream: Arc::clone(&stream),
                };
                // A steps operator's firing is a trigger of its run: the pass sheet
                // decides when it reaches the Cell, and admits it itself.
                if live.topology.steps.is_some() {
                    crate::steps::firing_promoted(&live, fire);
                    return None;
                }
                let fire = CellItem::Timer(fire);
                Some((
                    live,
                    fire,
                    Wake::Timer {
                        slot,
                        stream,
                        scheduled_at,
                    },
                ))
            }
            Wake::Instance { .. } => None,
        }
    }

    /// Put a wake the target had no room for back on the list, due after
    /// [`WAKE_RETRY_BACKOFF_MS`] — strictly later, or `park_slice` would
    /// return zero and spin the promoter.
    fn defer_wake(&self, now: u64, tiebreak: WakeTiebreak, wake: Wake) {
        self.wakes.record(
            WakeKey {
                due_at_ms: now + WAKE_RETRY_BACKOFF_MS,
                tiebreak,
            },
            wake,
        );
    }

    /// Arm one processing-time timer; re-arming `(cell, scheduled_at)`
    /// replaces the entry.
    pub fn arm_timer(&self, slot: &Arc<CellSlot>, scheduled_at: u64, stream: Arc<FlezhaStream>) {
        self.wakes.record(
            WakeKey {
                due_at_ms: scheduled_at,
                tiebreak: WakeTiebreak::Timer(Arc::clone(&slot.cell_identity)),
            },
            Wake::Timer {
                slot: Arc::downgrade(slot),
                stream,
                scheduled_at,
            },
        );
        self.notify_waiters();
    }

    /// Arm a Cell's interval close; re-arming replaces the entry.
    pub fn arm_interval_close(&self, slot: &Arc<CellSlot>, deadline_ms: u64) {
        self.wakes.record(
            WakeKey {
                due_at_ms: deadline_ms,
                tiebreak: WakeTiebreak::Interval(Arc::clone(&slot.cell_identity)),
            },
            Wake::CloseInterval {
                slot: Arc::downgrade(slot),
            },
        );
        self.notify_waiters();
    }

    /// Disarm an interval close that closed on its own. Idempotent.
    pub fn disarm_interval_close(&self, cell: &Arc<Vec<u8>>, deadline_ms: u64) {
        self.wakes.remove(&WakeKey {
            due_at_ms: deadline_ms,
            tiebreak: WakeTiebreak::Interval(Arc::clone(cell)),
        });
    }

    /// Disarm one timer. Idempotent; no notify, since dropping an entry can
    /// only lengthen a wait, never miss one.
    pub fn disarm_timer(&self, cell: &Arc<Vec<u8>>, scheduled_at: u64) {
        self.wakes.remove(&WakeKey {
            due_at_ms: scheduled_at,
            tiebreak: WakeTiebreak::Timer(Arc::clone(cell)),
        });
    }

    /// Arm one of the deadlines of the instance numbered `seq`. A promotion
    /// decides whether it still applies.
    pub fn arm_instance(
        &self,
        instance: Weak<crate::operator::instance::OperatorInstance>,
        seq: u64,
        deadline: crate::steps::Deadline,
        due_at_ms: u64,
    ) {
        self.wakes.record(
            WakeKey {
                due_at_ms,
                tiebreak: WakeTiebreak::Instance(seq, deadline),
            },
            Wake::Instance { instance, deadline },
        );
        self.notify_waiters();
    }

    /// Disarm one instance deadline armed for `due_at_ms`. Idempotent.
    pub fn disarm_instance(&self, seq: u64, deadline: crate::steps::Deadline, due_at_ms: u64) {
        self.wakes.remove(&WakeKey {
            due_at_ms,
            tiebreak: WakeTiebreak::Instance(seq, deadline),
        });
    }

    /// The deadlines armed for the instance numbered `seq`, with their due
    /// times.
    #[cfg(test)]
    pub(crate) fn instance_deadlines(&self, seq: u64) -> Vec<(crate::steps::Deadline, u64)> {
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
