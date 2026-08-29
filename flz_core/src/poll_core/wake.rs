//! The core's one deferred-wake list: retry backoffs, processing-time timer
//! deadlines, interval closes and push re-arms, keyed by due time. It owns no
//! thread — entries are promoted by whichever poller next enters or wakes —
//! and nothing in a promotion waits for mailbox room: a firing that cannot be
//! admitted is re-recorded strictly later, so the list IS the holdback.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use flz_transport::transport::FlezhaStream;

use super::PollCore;
use crate::entity::{EntityItem, EntitySlot};
use crate::timer::{TimerFire, TimerKind};
use crate::util::LockRecover;

/// How long a wake whose target mailbox was full waits before the promotion
/// pass tries it again. Short enough that a firing is not visibly delayed,
/// strictly non-zero so the pass cannot re-service the same entry it just
/// deferred.
const WAKE_RETRY_BACKOFF_MS: u64 = 5;

/// A deferred wake's place in the list: due time first, then a total
/// tie-break so entries due at the same instant have a reproducible order.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct WakeKey {
    pub(super) due_at_ms: u64,
    pub(super) tiebreak: WakeTiebreak,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum WakeTiebreak {
    /// The Entity's identity. It is also what makes arming
    /// `(entity, scheduled_at)` an idempotent replace — pending timers are a
    /// set — and, because the identity carries the owning instance's id, two
    /// instances sharing a numeric `ProcedureId` cannot disarm each other's
    /// timers.
    Timer(Arc<Vec<u8>>),
    /// Recording order. A slot's gates stay held across its backoff window,
    /// so a second retry for the same entity cannot exist; a plain sequence
    /// says so without a key collision able to drop an owed re-arm.
    Retry(u64),
    /// The Entity whose open checkpoint interval is due to close. Distinct
    /// from `Timer` so an interval deadline and an author's processing-time
    /// timer landing on the same millisecond cannot evict each other.
    Interval(Arc<Vec<u8>>),
}

pub(super) enum Wake {
    /// Finish a deferred re-arm: the slot's holdback is already filled and
    /// both its gates are still held.
    Retry(Arc<EntitySlot>),
    /// Re-attempt a gated producer's undelivered pushes. The consumer's wait
    /// list is the primary wake; this is the time-boxed fallback the entry-path
    /// park has in `ROOM_WAIT_TIMEOUT`, so a lost wake degrades to a retry
    /// loop rather than a permanent hold. Firing on a producer that already
    /// drained is a no-op — the claim lives in the flag, not here.
    PushRetry(Arc<EntitySlot>),
    /// A processing-time deadline. `Weak` so an idle-evicted Entity's timers
    /// die with it: the upgrade fails and the firing is dropped, which is the
    /// D6 contract. The stream is the one captured at arming, so `on_timer`'s
    /// plugin emits leave on the same connection an input's would.
    ///
    /// `scheduled_at` is the deadline the author armed — what the firing is
    /// stamped with and what cancels it — kept beside the entry because the
    /// key's due time is only *when to attempt admission*, and a firing that
    /// could not be admitted is re-recorded at a later one.
    Timer {
        slot: Weak<EntitySlot>,
        stream: Arc<FlezhaStream>,
        scheduled_at: u64,
    },
    /// A checkpoint interval's `max_delay_ms` has elapsed. `Weak` for the same
    /// reason a timer is: an evicted Entity's interval died with its state.
    CloseInterval { slot: Weak<EntitySlot> },
}

/// The core's one deferred-wake list. It owns no thread: entries are promoted
/// by whichever thread next enters or wakes inside [`PollCore::poll`], and
/// recording one returns the recorder immediately.
pub(super) struct DeferredWake {
    entries: Mutex<BTreeMap<WakeKey, Wake>>,
    /// Due time of the earliest entry, `u64::MAX` when empty. Maintained
    /// under `entries`' lock so the poll path decides "nothing is due" with
    /// one relaxed load instead of taking the lock per activation.
    earliest: AtomicU64,
}

impl DeferredWake {
    pub(super) fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            earliest: AtomicU64::new(u64::MAX),
        }
    }

    /// Insert one entry, replacing any that shares its key.
    pub(super) fn record(&self, key: WakeKey, wake: Wake) {
        let mut entries = self.entries.lock_recover();
        entries.insert(key, wake);
        self.refresh(&entries);
    }

    /// Remove one entry by key. A no-op when it is already gone.
    fn remove(&self, key: &WakeKey) {
        let mut entries = self.entries.lock_recover();
        entries.remove(key);
        self.refresh(&entries);
    }

    /// Drop every timer armed for one Entity. Dynamic-mode eviction is
    /// destructive, so the timers go with the state — released here rather
    /// than left to come due against an Entity that no longer exists.
    fn discard_entity(&self, entity: &Arc<Vec<u8>>) {
        let mut entries = self.entries.lock_recover();
        let doomed: Vec<WakeKey> = entries
            .keys()
            .filter(|k| match &k.tiebreak {
                WakeTiebreak::Timer(armed) | WakeTiebreak::Interval(armed) => armed == entity,
                WakeTiebreak::Retry(_) => false,
            })
            .cloned()
            .collect();
        for key in doomed {
            entries.remove(&key);
        }
        self.refresh(&entries);
    }

    /// Whether anything has come due. One relaxed load on the cached
    /// earliest, so both the promotion path and the starvation predicate ask
    /// it without taking the list's lock.
    pub(super) fn any_due(&self, now: u64) -> bool {
        self.earliest.load(Ordering::Acquire) <= now
    }

    /// Pop every entry due at `now`, ascending.
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

    /// How long until the earliest entry comes due; `None` when the list is
    /// empty. What shortens a poller's wait.
    fn next_due_in(&self, now: u64) -> Option<Duration> {
        let due = self.earliest.load(Ordering::Acquire);
        (due != u64::MAX).then(|| Duration::from_millis(due.saturating_sub(now)))
    }

    /// Processing-time timers currently armed. Counted from the entries
    /// themselves, so the number cannot drift from what is scheduled: a timer
    /// counts while its entry sits here and stops the moment it leaves,
    /// whether by disarm, eviction or firing.
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
    /// How long to park: until `deadline`, but never past the nearest due
    /// wake. Reads the list's cached earliest and takes no lock — `ready` is
    /// held by both callers, and a recorder takes the list's lock and then
    /// `ready`, so locking the list here would close a cycle.
    pub(super) fn park_slice(&self, deadline: Instant, now: Instant) -> Duration {
        let mut wait = deadline.saturating_duration_since(now);
        if let Some(until_due) = self.wakes.next_due_in(crate::util::wall_now_ms()) {
            wait = wait.min(until_due);
        }
        wait
    }

    /// Hand every entry that has come due to its effect. Called once per
    /// [`Self::poll`] pass — the list has no thread of its own.
    ///
    /// **Nothing here waits for mailbox room.** A promoter is a thread inside
    /// [`Self::poll`], counted by `pollers_inside`, which makes
    /// [`Self::starving`] answer "a thread is about to take work" — so a
    /// promoter held on a full mailbox both stops the runner and hides that it
    /// has, and no supervisor can see it. A firing that cannot be admitted is
    /// re-recorded at a later due time instead: the wake list IS the holdback,
    /// and an entry still sitting there is exactly what `starving()` reports
    /// with no dwell.
    pub(super) fn promote_due(&self) {
        let now = crate::util::wall_now_ms();
        for (key, wake) in self.wakes.take_due(now) {
            let Some((slot, item, retry)) = self.wake_target(wake) else {
                continue;
            };
            if crate::admission::try_admit(&slot, item).is_err() {
                self.defer_wake(now, key.tiebreak, retry);
            }
        }
    }

    /// What one due wake admits, and the entry to put back if it will not
    /// fit. One tail for every wake kind, so "no promotion waits" is a single
    /// statement rather than a rule each arm has to remember; a re-arm has no
    /// item and is finished here, and a wake on an evicted Entity is dropped.
    fn wake_target(&self, wake: Wake) -> Option<(Arc<EntitySlot>, EntityItem, Wake)> {
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
                Some((
                    live,
                    EntityItem::CloseInterval,
                    Wake::CloseInterval { slot },
                ))
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
                let fire = EntityItem::Timer(TimerFire {
                    kind: TimerKind::ProcessingTime,
                    scheduled_at,
                    stream: Arc::clone(&stream),
                });
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
        }
    }

    /// Put a wake the target's mailbox had no room for back on the list, due
    /// again after [`WAKE_RETRY_BACKOFF_MS`].
    ///
    /// Strictly later than the pass that observed it, or every pass would
    /// re-service it and `park_slice` — which already shortens a poller's wait
    /// to the nearest due entry — would return zero and spin the promoter.
    /// That same shortening is the retry cadence, so the deferral needs no
    /// timer of its own.
    fn defer_wake(&self, now: u64, tiebreak: WakeTiebreak, wake: Wake) {
        self.wakes.record(
            WakeKey {
                due_at_ms: now + WAKE_RETRY_BACKOFF_MS,
                tiebreak,
            },
            wake,
        );
    }

    /// Arm one processing-time timer. Re-arming the same
    /// `(entity, scheduled_at)` replaces the entry — pending timers are a
    /// set, so this is the idempotence the surface promises.
    pub fn arm_timer(&self, slot: &Arc<EntitySlot>, scheduled_at: u64, stream: Arc<FlezhaStream>) {
        self.wakes.record(
            WakeKey {
                due_at_ms: scheduled_at,
                tiebreak: WakeTiebreak::Timer(Arc::clone(&slot.entity_identity)),
            },
            Wake::Timer {
                slot: Arc::downgrade(slot),
                stream,
                scheduled_at,
            },
        );
        self.notify_waiters();
    }

    /// Arm the close of an Entity's open checkpoint interval. Re-arming
    /// replaces the entry, so an interval whose deadline moved (a fresh
    /// interval opened) never has two.
    pub fn arm_interval_close(&self, slot: &Arc<EntitySlot>, deadline_ms: u64) {
        self.wakes.record(
            WakeKey {
                due_at_ms: deadline_ms,
                tiebreak: WakeTiebreak::Interval(Arc::clone(&slot.entity_identity)),
            },
            Wake::CloseInterval {
                slot: Arc::downgrade(slot),
            },
        );
        self.notify_waiters();
    }

    /// Disarm an Entity's interval close — the interval closed on its own
    /// (an input, a count, an explicit checkpoint). Idempotent.
    pub fn disarm_interval_close(&self, entity: &Arc<Vec<u8>>, deadline_ms: u64) {
        self.wakes.remove(&WakeKey {
            due_at_ms: deadline_ms,
            tiebreak: WakeTiebreak::Interval(Arc::clone(entity)),
        });
    }

    /// Disarm one processing-time timer. Idempotent — a firing that already
    /// took the entry makes this a no-op. No notify: dropping an entry can
    /// only leave a waiter waiting longer than it needed to, never miss one.
    pub fn disarm_timer(&self, entity: &Arc<Vec<u8>>, scheduled_at: u64) {
        self.wakes.remove(&WakeKey {
            due_at_ms: scheduled_at,
            tiebreak: WakeTiebreak::Timer(Arc::clone(entity)),
        });
    }

    /// Drop every timer armed for one Entity, at the moment the dynamic-mode
    /// reaper evicts it.
    pub fn discard_entity_timers(&self, entity: &Arc<Vec<u8>>) {
        self.wakes.discard_entity(entity);
    }

    /// Processing-time timers currently armed on this core — what the
    /// `timers_armed` heartbeat metric reports.
    pub fn timers_armed(&self) -> usize {
        self.wakes.timers_armed()
    }

    /// Wake blocked pollers so they re-compute their wait against a newly
    /// recorded entry. Taking `ready` is what makes the notify unmissable: a
    /// waiter holds it from before it reads the list until `wait_timeout`
    /// releases it. Called with the wake-list lock already released — the two
    /// are taken in this order and never the other way round.
    pub(super) fn notify_waiters(&self) {
        let _ready = self.ready.lock_recover();
        self.cv.notify_all();
    }
}
