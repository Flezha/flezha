//! Per-operative mailbox: bounded lock-free FIFO + inflight flag + counters.
//!
//! Invariants:
//!
//! - **Sticky ordering**: for a given mailbox, at most one worker thread is
//!   inside the drain loop at any wall-clock instant. Items are processed in
//!   FIFO arrival order. The inflight CAS guarantees mutual exclusion.
//! - **Bounded capacity**: the queue is a fixed-size [`ArrayQueue`] sized at
//!   construction (`Mailbox::with_capacity`). Pushes that would exceed the
//!   cap return the rejected payload to the caller; the caller decides what
//!   to do based on the channel's declared `OverflowPolicy` (drop nothing,
//!   drop the newest, or replace the oldest via `force_push`).
//!
//! Wake protocol (unchanged from the stream shape):
//!
//!   - Producer calls `try_push(item)` → on success, CASes `inflight false→true`.
//!     On a winning CAS the caller schedules the entity's round on the poll
//!     core; on a losing CAS another drain is already in flight and will pop
//!     the new item.
//!   - The drain loop pops items until the queue is empty, stores
//!     `inflight = false`, then re-checks `is_empty`. If a producer's push
//!     raced our store (their CAS failed because we still held inflight, but
//!     their push made it onto the queue), the re-check sees the new item and
//!     we re-CAS to resume ownership.
//!
//!   T1 (drainer):                    T2 (producer):
//!     pop() -> None                    push(item)            // succeeds
//!                                      CAS false→true -> FAILS (still true)
//!     store(inflight = false)
//!     pop() -> Some(item)  <-- rescues the missed wake
//!
//! This is the standard "actor dispatch" pattern used in Erlang's run queue
//! and Akka's Dispatcher; it remains correct under [`ArrayQueue`]'s
//! lock-free push/pop because the queue's internal ordering guarantees that
//! a successful push is observable to a subsequent `is_empty()` /  `pop()`
//! on any thread.

use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam::queue::ArrayQueue;

use crate::entity::EntityCounters;

/// Per-Entity mailbox: a workflow `ArrayQueue<Payload>` plus an
/// `AtomicBool` inflight flag and a [`EntityCounters`] block.
///
/// `Payload` is the user-defined work item shape. For Flezha, it's
/// [`crate::entity::EntityPayload`].
pub struct Mailbox<Payload: Send + 'static> {
    queue: ArrayQueue<Payload>,
    inflight: AtomicBool,
    counters: EntityCounters,
}

impl<Payload: Send + 'static> Mailbox<Payload> {
    /// Allocate a bounded mailbox with the given capacity. Capacity must
    /// be `>= 1`; the `ArrayQueue` constructor panics on 0, so callers
    /// (i.e. `operative::builder::resolve_mailbox_capacity`) must validate
    /// the value before calling here.
    pub fn with_capacity(capacity: usize) -> Self {
        Mailbox {
            queue: ArrayQueue::new(capacity),
            inflight: AtomicBool::new(false),
            counters: EntityCounters::default(),
        }
    }

    /// Bounded capacity declared at construction. Constant for the
    /// lifetime of the mailbox.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Snapshot of the queue's current length. Approximate under MPMC
    /// contention; exposed for diagnostics, not as a synchronization
    /// primitive.
    #[inline]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// True when the bounded queue is at capacity. Used by DropNothing-policy
    /// producers to recheck saturation after registering on the parker's
    /// wait list, closing the missed-wake window between a failed
    /// `try_push` and the deque push inside `BlockParker::wait_one`.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.queue.len() >= self.queue.capacity()
    }

    /// Per-Entity counter set. Producer-side enqueue paths bump these
    /// on push success / block / drop events; readers (future metrics
    /// export) use [`Ordering::Relaxed`] loads.
    #[inline]
    pub fn counters(&self) -> &EntityCounters {
        &self.counters
    }

    // ---- Low-level primitives for callers that want to manage the drain
    //      task themselves (avoiding per-dispatch closure allocation). ----

    /// Try to enqueue a payload onto the bounded queue without scheduling
    /// any drain. Returns the rejected payload on full so the caller can
    /// apply the channel's overflow policy (block-and-retry, drop-newest,
    /// drop-oldest via [`Self::force_push`]).
    #[inline]
    pub fn try_push(&self, payload: Payload) -> Result<(), Payload> {
        self.queue.push(payload)
    }

    /// Atomically pop the oldest item if the queue is full, then push the
    /// new payload. Returns the dropped payload to the caller for
    /// accounting (`drops_oldest_total` counter increments). Best-effort
    /// under MPMC contention per Decision 9.
    #[inline]
    pub fn force_push(&self, payload: Payload) -> Option<Payload> {
        self.queue.force_push(payload)
    }

    /// CAS inflight false→true. Returns true on success (caller owns the
    /// drain responsibility) and false if someone else is already draining.
    #[inline]
    pub fn try_mark_inflight(&self) -> bool {
        self.inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Clear the inflight flag. Caller is responsible for re-checking the
    /// queue and re-acquiring inflight if more items arrived.
    #[inline]
    pub fn release_inflight(&self) {
        self.inflight.store(false, Ordering::Release);
    }

    /// Pop the next item. Returns None if empty.
    #[inline]
    pub fn pop(&self) -> Option<Payload> {
        self.queue.pop()
    }

    /// Discard every queued item, returning how many were dropped.
    ///
    /// Used by the restore path, and only for a class whose inputs the
    /// controller journals before forwarding — it re-forwards them, so the
    /// queue is a subset of the durable log. A journal-less class's queue is
    /// the only copy of its input; see
    /// [`EntitySlot::discard_queued_input_on_restore`].
    ///
    /// [`EntitySlot::discard_queued_input_on_restore`]: crate::entity::EntitySlot::discard_queued_input_on_restore
    pub fn clear(&self) -> usize {
        let mut discarded = 0;
        while self.queue.pop().is_some() {
            discarded += 1;
        }
        discarded
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn try_push_returns_payload_when_full() {
        let mbox: Arc<Mailbox<usize>> = Arc::new(Mailbox::with_capacity(2));
        assert_eq!(mbox.try_push(1), Ok(()));
        assert_eq!(mbox.try_push(2), Ok(()));
        assert_eq!(mbox.try_push(3), Err(3));
        // Drain one, room is back.
        assert_eq!(mbox.pop(), Some(1));
        assert_eq!(mbox.try_push(3), Ok(()));
    }

    #[test]
    fn force_push_returns_dropped_oldest() {
        let mbox: Arc<Mailbox<usize>> = Arc::new(Mailbox::with_capacity(2));
        assert_eq!(mbox.try_push(1), Ok(()));
        assert_eq!(mbox.try_push(2), Ok(()));
        // Full: force_push pops the oldest (1) and accepts 3.
        assert_eq!(mbox.force_push(3), Some(1));
        assert_eq!(mbox.pop(), Some(2));
        assert_eq!(mbox.pop(), Some(3));
        assert_eq!(mbox.pop(), None);
    }

    #[test]
    fn capacity_and_len_track_pushes() {
        let mbox: Arc<Mailbox<usize>> = Arc::new(Mailbox::with_capacity(4));
        assert_eq!(mbox.capacity(), 4);
        assert_eq!(mbox.len(), 0);
        assert!(mbox.is_empty());
        mbox.try_push(10).unwrap();
        mbox.try_push(20).unwrap();
        assert_eq!(mbox.len(), 2);
        assert!(!mbox.is_empty());
    }
}
