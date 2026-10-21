//! The starvation contract: a host thread parks in
//! [`PollCore::wait_starvation`] and learns that runnable work has sat with no
//! thread entering `poll` for a whole dwell. Starvation is *stuck*, not
//! *busy* — a saturated runner re-enters `poll` once per batch, orders of
//! magnitude more often than the dwell.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::{PollCore, Starvation};
use crate::util::LockRecover;

/// How long queued work must sit with no thread entering `poll` before it
/// counts as starvation, and the longest a starvation waiter parks between
/// re-evaluations. A batch takes microseconds, so a busy runner re-enters
/// `poll` orders of magnitude more often than this; only a runner whose
/// threads are all *stuck* inside user code shows no entry at all for the
/// window.
pub(super) const STARVATION_DWELL: Duration = Duration::from_millis(100);

impl PollCore {
    /// Park until the core observes starvation — runnable work with nobody
    /// left to run it — or `timeout` elapses.
    ///
    /// Starvation is *stuck*, not *busy*. Queued work — a cell round, or a
    /// direct activation a caller is blocked on — counts only once it has sat
    /// for [`STARVATION_DWELL`] with **no thread entering `poll` at all**: a
    /// saturated runner enters `poll` once per batch, i.e. orders of magnitude
    /// more often, and answering that with more threads would add contention
    /// to a runner that is merely working.
    ///
    /// The core still calls no host code: this is the same
    /// bring-your-own-thread shape as `poll`, and a host that cannot starve
    /// (one that never lets a fetch thread run user code) simply never calls
    /// it.
    pub fn wait_starvation(&self, timeout: Duration) -> Starvation {
        let deadline = Instant::now() + timeout;
        let mut ready = self.ready.lock_recover();
        // The dwell observation, carried across parks: when the condition
        // started holding and the poll-entry count at that moment.
        let mut held: Option<(Instant, u64)> = None;
        loop {
            if self.shutting_down.load(Ordering::Acquire) {
                return Starvation::ShuttingDown;
            }
            let now = Instant::now();
            if self.starving(ready.is_empty(), now, &mut held) {
                return Starvation::Observed;
            }
            if now >= deadline {
                return Starvation::Timeout;
            }
            // Re-evaluate at least once per dwell: nothing signals this
            // condvar on the hot path.
            let wait = self.park_slice(deadline, now).min(STARVATION_DWELL);
            let (guard, _) = self
                .starve_cv
                .wait_timeout(ready, wait)
                .unwrap_or_else(|p| p.into_inner());
            ready = guard;
        }
    }

    /// Whether the host owes a thread right now. `held` carries the dwell
    /// observation between calls; anything that interrupts the condition —
    /// the queue draining, the cap filling, a thread entering `poll` —
    /// restarts it.
    fn starving(&self, ready_empty: bool, now: Instant, held: &mut Option<(Instant, u64)>) -> bool {
        // Could a thread supplied now even take work? Not while one is
        // already inside `poll`.
        if self.pollers_inside.load(Ordering::Acquire) > 0 {
            *held = None;
            return false;
        }
        // A due wake needs no dwell: every `poll` pass promotes what has come
        // due, so an entry still sitting there proves no pass has happened
        // since it did.
        if self.wakes.any_due() {
            return self.owed_for_wake(now);
        }
        // A direct activation — a host crossing, a steps relay undo — is runnable
        // whatever the cap says, and something waits on its answer; queued cell
        // work at the cap is not runnable and is owed no thread.
        let runnable = !self.direct.lock_recover().is_empty()
            || (!ready_empty && self.outstanding_count.load(Ordering::Acquire) < self.max_inflight);
        if !runnable {
            *held = None;
            return false;
        }
        let entries = self.poll_entries.load(Ordering::Acquire);
        match *held {
            // Unchanged entry count: no thread has been inside `poll` since
            // this work queued, so the runner is stuck rather than working.
            Some((since, seen)) if seen == entries => now - since >= STARVATION_DWELL,
            _ => {
                *held = Some((now, entries));
                false
            }
        }
    }

    /// One thread for a due wake, not one per ask: the thread answered last
    /// needs time to reach `poll`, so until a thread enters it or a dwell
    /// passes, the wake is owed none again.
    fn owed_for_wake(&self, now: Instant) -> bool {
        let entries = self.poll_entries.load(Ordering::Acquire);
        let mut answered = self.wake_answered.lock_recover();
        if matches!(*answered, Some((at, seen)) if seen == entries && now - at < STARVATION_DWELL) {
            return false;
        }
        *answered = Some((now, entries));
        true
    }
}
