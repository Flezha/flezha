//! Per-operative mailbox: bounded lock-free FIFO + inflight flag + counters,
//! and the wait list of whoever is owed its next free slot.
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

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crossbeam::queue::ArrayQueue;
use crossbeam::sync::{Parker, Unparker};

use crate::entity::{EntityCounters, EntitySlot};

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
    /// producers to recheck saturation after registering on the wait list,
    /// closing the missed-wake window between a failed `try_push` and the
    /// deque push inside [`Waiters::wait_for_room`].
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
    pub fn claim_inflight(&self) -> bool {
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

/// Multi-waiter FIFO wait list of whoever is owed this mailbox's next free
/// slot under `DropNothing`, served one entry per successful pop.
///
/// A waiter is either a parked thread — the entry-path transport reader, which
/// has no producing Entity to withhold work from — or a producing Entity whose
/// undelivered push is held in its own holdback. One list, because "room
/// appeared on this consumer" is one event and both kinds are owed it, and one
/// pop is one unit of room.
///
/// A parked thread pushes its own entry, **drops the lock**, **rechecks that
/// the mailbox is still full**, parks, and takes its entry back out on the way
/// through. The lock is never held across `park()`. Every entry in the list
/// therefore corresponds to somebody who is still waiting — which is what makes
/// "one pop serves one waiter" true rather than approximately true.
///
/// FIFO ordering at the API level is best-effort once threads start
/// observing their own wakeups; thread scheduling can reorder which
/// woken thread re-attempts the push first.
pub struct Waiters {
    waiters: Mutex<VecDeque<Waiter>>,
    /// Identifies a parked thread's own entry so it can take it back out.
    next_ticket: AtomicU64,
}

/// Who is owed the next free slot on a consumer's mailbox.
enum Waiter {
    /// A parked thread, woken by its `Unparker`. The ticket is how the thread
    /// removes its own entry when it stops waiting.
    Thread { ticket: u64, unparker: Unparker },
    /// A producing Entity gated on its push holdback, re-armed by draining it.
    /// `Weak` so a consumer's wait list cannot keep an evicted producer alive.
    Producer(Weak<EntitySlot>),
}

impl Waiters {
    pub fn new() -> Self {
        Self {
            waiters: Mutex::new(VecDeque::new()),
            next_ticket: AtomicU64::new(0),
        }
    }

    /// Register on this slot's wait list and park the calling thread until a
    /// pop frees a slot, or `timeout` elapses (`None` = until woken).
    ///
    /// After registering, the producer **rechecks** `mailbox.is_full()`. If a
    /// drain freed a slot (or fully drained and exited) between the caller's
    /// failed `try_push` and our deque push, the recheck observes
    /// `!is_full()` and we return immediately without parking; the outer
    /// admission loop will retry `try_push`. Without this recheck, a producer
    /// that loses that race parks forever.
    ///
    /// **The entry is removed on the way out, on every exit path.** An entry
    /// whose thread has moved on still consumes a `serve_one` the next live
    /// waiter was owed, and each consumed wake makes that waiter's next
    /// timeout more likely — a deficit that only ever grows, until every wake
    /// is absorbed by the dead and a live producer advances one 50 ms park at
    /// a time.
    pub fn wait_for_room_until<P: Send + 'static>(
        &self,
        mailbox: &Mailbox<P>,
        timeout: Option<Duration>,
    ) {
        let parker = Parker::new();
        let ticket = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        {
            let mut deque = self.waiters.lock().expect("Waiters mutex poisoned");
            deque.push_back(Waiter::Thread {
                ticket,
                unparker: parker.unparker().clone(),
            });
        }
        if mailbox.is_full() {
            match timeout {
                Some(t) => parker.park_timeout(t),
                None => parker.park(),
            }
        }
        let mut deque = self.waiters.lock().expect("Waiters mutex poisoned");
        if let Some(at) = deque
            .iter()
            .position(|w| matches!(w, Waiter::Thread { ticket: t, .. } if *t == ticket))
        {
            deque.remove(at);
        }
    }

    /// Register on this slot's wait list and park until a pop frees a slot.
    pub fn wait_for_room<P: Send + 'static>(&self, mailbox: &Mailbox<P>) {
        self.wait_for_room_until(mailbox, None);
    }

    /// [`wait_for_room`](Self::wait_for_room) with a bounded park. Used by
    /// `DropNothing`-policy producers so that even an unforeseen lost-wake
    /// degrades to a retry loop with warnings instead of a permanent silent
    /// freeze.
    pub fn wait_for_room_timeout<P: Send + 'static>(
        &self,
        mailbox: &Mailbox<P>,
        timeout: Duration,
    ) {
        self.wait_for_room_until(mailbox, Some(timeout));
    }

    /// Register a producing Entity as owed the next free slot on this
    /// consumer. Unlike [`Self::wait_for_room`] it holds no thread: the
    /// producer returns and its holdback is drained when the room arrives.
    ///
    /// At most one entry per producer, because a producer re-registers after
    /// every failed drain and duplicates would absorb wakes a parked thread is
    /// owed. The caller re-checks `is_full()` after registering for the same
    /// reason a parking waiter does — a drain that emptied the mailbox between
    /// the failed admission and this registration would otherwise never wake
    /// it. A registration left behind by a producer that drained on its own is
    /// inert: re-arming an un-gated Entity is a no-op.
    pub fn wait_producer(&self, producer: &Arc<EntitySlot>) {
        let mut deque = self.waiters.lock().expect("Waiters mutex poisoned");
        let already = deque
            .iter()
            .any(|w| matches!(w, Waiter::Producer(p) if p.as_ptr() == Arc::as_ptr(producer)));
        if !already {
            deque.push_back(Waiter::Producer(Arc::downgrade(producer)));
        }
    }

    /// Serve the oldest waiter, if any. Called by the consumer's drain loop
    /// after a successful pop. A waiter whose Entity has been dropped is
    /// discarded and the next one served — a dead entry must not absorb the
    /// room it can no longer use.
    pub fn serve_one(&self) {
        loop {
            let waiter = {
                let mut deque = self.waiters.lock().expect("Waiters mutex poisoned");
                deque.pop_front()
            };
            match waiter {
                Some(Waiter::Thread { unparker, .. }) => return unparker.unpark(),
                // Not a wake but the work itself: the freed slot is admitted
                // from the producer's holdback on this thread. Nothing here
                // runs user code — it is one `try_push` per held item — so the
                // consumer's drain pays only what the producer would have paid
                // on its own.
                Some(Waiter::Producer(weak)) => {
                    if let Some(producer) = weak.upgrade() {
                        return crate::poll_core::global().rearm_deferred_pushes(&producer);
                    }
                }
                None => return,
            }
        }
    }

    /// Number of currently registered waiters. Approximate under contention;
    /// exposed for diagnostics only.
    pub fn waiter_count(&self) -> usize {
        self.waiters.lock().map(|d| d.len()).unwrap_or(0)
    }
}

impl Default for Waiters {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

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

    /// Yield until `done` holds; a 2 s deadline names `what` on failure.
    fn spin_until(what: &str, done: impl Fn() -> bool) {
        let started = Instant::now();
        while !done() {
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "timed out waiting for {what}"
            );
            std::thread::yield_now();
        }
    }

    /// Build a capacity-1 mailbox pre-filled so `is_full()` reports true.
    /// Required because [`Waiters::wait_for_room`] rechecks the mailbox
    /// after registering and returns without parking when it isn't full.
    fn saturated_mailbox() -> Arc<Mailbox<u8>> {
        let mbox = Arc::new(Mailbox::<u8>::with_capacity(1));
        mbox.try_push(0).expect("pre-fill");
        mbox
    }

    #[test]
    fn serving_one_releases_the_parked_waiter() {
        let waiters = Arc::new(Waiters::new());
        let mbox = saturated_mailbox();
        let p = Arc::clone(&waiters);
        let m = Arc::clone(&mbox);
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s = Arc::clone(&started);
        let t = std::thread::spawn(move || {
            s.store(true, Ordering::Release);
            p.wait_for_room(&m);
        });
        // Wait for the thread to register itself before serving, so we
        // exercise the wake-after-park path rather than the no-op
        // unpark-before-park (Parker handles both, but we want the
        // observable path here).
        while !started.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        // The producer has reached `parker.park()` once it is counted.
        spin_until("the producer to park", || waiters.waiter_count() == 1);
        waiters.serve_one();
        spin_until("the waiter to be served", || t.is_finished());
        t.join().unwrap();
    }

    #[test]
    fn serving_with_no_waiters_is_a_noop() {
        let waiters = Waiters::new();
        // Should not block, panic, or otherwise misbehave.
        waiters.serve_one();
        waiters.serve_one();
        assert_eq!(waiters.waiter_count(), 0);
    }

    #[test]
    fn waiters_are_served_in_fifo_order() {
        // Push three unparkers in order A, B, C; verify pop_front sees
        // A, B, C. Test the deque-level ordering only — once threads
        // observe their wakeups, scheduling can reorder them.
        let waiters = Waiters::new();
        let mbox = saturated_mailbox();
        // Use the internal API by parking three threads serialized via
        // sleeps so we can assert the wait_count grows monotonically.
        let p = Arc::new(waiters);
        let order = Arc::new(Mutex::new(Vec::<u8>::new()));
        let mut threads = Vec::new();
        for (parked, tag) in [b'A', b'B', b'C'].into_iter().enumerate() {
            let waiters = Arc::clone(&p);
            let m = Arc::clone(&mbox);
            let order = Arc::clone(&order);
            threads.push(std::thread::spawn(move || {
                waiters.wait_for_room(&m);
                order.lock().unwrap().push(tag);
            }));
            // Sequence the parkers so they enter the deque in order: each is
            // counted parked before the next is spawned.
            spin_until("the parker to park", || p.waiter_count() == parked + 1);
        }
        for woken in 1..=3 {
            p.serve_one();
            // The woken thread records its wake before the next is issued, so
            // the record is the deque order and not the scheduler's.
            spin_until("the woken thread to record its wake", || {
                order.lock().unwrap().len() == woken
            });
        }
        for t in threads {
            t.join().unwrap();
        }
        let observed = order.lock().unwrap().clone();
        assert_eq!(observed, vec![b'A', b'B', b'C']);
    }

    /// Regression for the missed-wake deadlock: if drain pops every item
    /// and exits between a producer's failed `try_push` and its deque
    /// push inside `wait_for_room`, the producer must NOT block forever. The
    /// fix re-checks `mailbox.is_full()` after registering and returns
    /// immediately when the mailbox has space.
    #[test]
    fn waiting_for_room_does_not_block_when_mailbox_has_space() {
        let waiters = Waiters::new();
        let mbox = Arc::new(Mailbox::<u8>::with_capacity(2));
        // Mailbox has 2 free slots — wait_for_room must NOT park.
        let start = Instant::now();
        waiters.wait_for_room(&mbox);
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(50),
            "wait_for_room parked despite non-full mailbox; elapsed = {:?}",
            elapsed
        );
        // And it leaves NOTHING behind. An entry whose thread has moved on
        // absorbs a `serve_one` the next live waiter was owed, which makes
        // that waiter's next timeout more likely, which leaves another dead
        // entry: a deficit that only grows until every wake is absorbed and a
        // live producer advances one bounded park at a time.
        assert_eq!(waiters.waiter_count(), 0);
    }

    /// A timed-out waiter leaves nothing behind either — the path a
    /// `DropNothing` entry-path producer takes on every 50 ms retry, and the
    /// one that accumulates if it does not clean up.
    #[test]
    fn a_timed_out_waiter_removes_its_own_entry() {
        let waiters = Waiters::new();
        let mbox = saturated_mailbox();
        for _ in 0..5 {
            waiters.wait_for_room_timeout(&mbox, Duration::from_millis(1));
        }
        assert_eq!(waiters.waiter_count(), 0);
    }
}
