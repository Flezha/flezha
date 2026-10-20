//! The push holdback's claim: a Cell whose round ended owing a push keeps
//! its gates, and exactly one thread hands them back once the holdback
//! drains. `push_gated` is the claim; whoever swaps it from `true` holds it.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use super::wake::{Wake, WakeKey, WakeTiebreak};
use super::PollCore;
use crate::cell::CellSlot;

pub(super) struct PendingPushes {
    since: Instant,
}

/// The lost-wake fallback for a gated producer (the consumer's wait list is
/// the primary re-arm), mirroring the entry path's `ROOM_WAIT_TIMEOUT`.
const PUSH_RETRY_BACKOFF_MS: u64 = 50;

impl PollCore {
    /// What a Cell's holdback owes its gates: `true` keeps them held, for
    /// the holdback or because another thread took the claim. `push_gated` is
    /// published on EVERY entry before the holdback is judged; a claim
    /// published only on the deferred branch would let a consumer racing the
    /// two reads leave both sides answering "held" and the gates leaked.
    pub(super) fn hold_or_release_pushes(&self, slot: &Arc<CellSlot>) -> bool {
        // Counted before the claim is published, so the thread that takes it
        // and releases uncounts it after this, never before.
        if slot.push_deferred.load(Ordering::Acquire) {
            self.deferred_pushes
                .entry(Arc::clone(&slot.cell_identity))
                .or_insert_with(|| PendingPushes {
                    since: Instant::now(),
                });
        }
        slot.push_gated.store(true, Ordering::Release);
        if slot.push_deferred.load(Ordering::Acquire) {
            crate::admission::drain_push_holdback(slot);
            if slot.push_deferred.load(Ordering::Acquire) {
                // The consumer's wait list is what normally brings us back;
                // this only bounds how long a lost wake could hold the Cell.
                self.wakes.record(
                    WakeKey {
                        due_at_ms: crate::util::wall_now_ms() + PUSH_RETRY_BACKOFF_MS,
                        tiebreak: WakeTiebreak::Retry(
                            self.next_wake.fetch_add(1, Ordering::Relaxed),
                        ),
                    },
                    Wake::PushRetry(Arc::clone(slot)),
                );
                self.notify_waiters();
                return true;
            }
        }
        if !slot.push_gated.swap(false, Ordering::AcqRel) {
            // Another thread took the claim and holds or releases in our
            // place. Sound only because the store above is unconditional: a
            // `false` here cannot mean "no claim was ever made".
            return true;
        }
        self.deferred_pushes.remove(&slot.cell_identity);
        false
    }

    /// Re-attempt a producer's held pushes (a consumer freed room, or the
    /// fallback wake came due); releases the gates only when they are held
    /// FOR the holdback.
    pub(crate) fn rearm_deferred_pushes(&self, slot: &Arc<CellSlot>) {
        crate::admission::drain_push_holdback(slot);
        // Taking the claim is the swap, and the holder's protocol then runs as
        // ours: reading the claim and releasing later could free the gates of
        // a round handed out after another thread released. The load first
        // keeps an absent claim a plain read. When the protocol answers
        // "release", `release_and_rearm` releases, or holds again through its
        // own guard if the holdback is owed once more.
        if !slot.push_deferred.load(Ordering::Acquire)
            && slot.push_gated.load(Ordering::Acquire)
            && slot.push_gated.swap(false, Ordering::AcqRel)
            && !self.hold_or_release_pushes(slot)
        {
            self.release_and_rearm(slot);
        }
    }

    pub fn pending_push_count(&self) -> usize {
        self.deferred_pushes.len()
    }

    /// Milliseconds the oldest held push has waited; `0` when none.
    pub fn pending_push_oldest_age_ms(&self) -> u64 {
        let now = Instant::now();
        self.deferred_pushes
            .iter()
            .map(|e| now.saturating_duration_since(e.since).as_millis() as u64)
            .max()
            .unwrap_or(0)
    }
}
