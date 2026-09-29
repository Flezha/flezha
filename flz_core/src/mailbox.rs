//! Per-Cell mailbox: a bounded lock-free FIFO, its inflight flag, its
//! counters, and the wait list of whoever is owed its next free slot. The
//! invariant: a winning `claim_inflight` CAS is the one drainer, and a drainer
//! that stores `inflight = false` re-checks the queue afterwards, so a push
//! that lost the CAS while the store was pending is still popped. A push into
//! a full queue hands the item back; the channel's `OverflowPolicy` decides.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crossbeam::queue::ArrayQueue;
use crossbeam::sync::{Parker, Unparker};

use crate::cell::{CellCounters, CellSlot};

pub struct Mailbox<Payload: Send + 'static> {
    queue: ArrayQueue<Payload>,
    inflight: AtomicBool,
    counters: CellCounters,
}

impl<Payload: Send + 'static> Mailbox<Payload> {
    /// `capacity >= 1`; the queue panics on 0, so callers validate first.
    pub fn with_capacity(capacity: usize) -> Self {
        Mailbox {
            queue: ArrayQueue::new(capacity),
            inflight: AtomicBool::new(false),
            counters: CellCounters::default(),
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Approximate under contention; diagnostics only.
    #[inline]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// The recheck a `DropNothing` producer makes after registering on the
    /// wait list, closing the missed-wake window.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.queue.len() >= self.queue.capacity()
    }

    #[inline]
    pub fn counters(&self) -> &CellCounters {
        &self.counters
    }

    // ---- Low-level primitives for callers that want to manage the drain
    //      task themselves (avoiding per-dispatch closure allocation). ----

    /// Enqueue without scheduling a drain; hands the payload back when full.
    #[inline]
    pub fn try_push(&self, payload: Payload) -> Result<(), Payload> {
        self.queue.push(payload)
    }

    /// Pop the oldest when full, then push; hands back what was dropped.
    /// Best-effort under contention.
    #[inline]
    pub fn force_push(&self, payload: Payload) -> Option<Payload> {
        self.queue.force_push(payload)
    }

    /// CAS inflight false→true; `true` makes the caller the drainer.
    #[inline]
    pub fn claim_inflight(&self) -> bool {
        self.inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Whether a drainer holds this mailbox's round — queued for a poller or
    /// already inside one. With an empty queue, `false` is the whole of "this
    /// Cell has nothing left to do".
    #[inline]
    pub fn is_inflight(&self) -> bool {
        self.inflight.load(Ordering::Acquire)
    }

    /// Clear inflight; the caller re-checks the queue afterwards.
    #[inline]
    pub fn release_inflight(&self) {
        self.inflight.store(false, Ordering::Release);
    }

    #[inline]
    pub fn pop(&self) -> Option<Payload> {
        self.queue.pop()
    }

    /// Discard every queued item, answering how many. Only for a class whose
    /// inputs the controller re-forwards from a durable log.
    pub fn clear(&self) -> usize {
        let mut discarded = 0;
        while self.queue.pop().is_some() {
            discarded += 1;
        }
        discarded
    }
}

/// FIFO of whoever is owed this mailbox's next free slot under `DropNothing`
/// — a parked entry-path thread or a producing Cell holding its push — served
/// one entry per successful pop. Every entry is somebody still waiting: a
/// parked thread registers, rechecks the mailbox, parks without the lock, and
/// removes its own entry on every exit. Wake order is best-effort.
pub struct Waiters {
    waiters: Mutex<VecDeque<Waiter>>,
    next_ticket: AtomicU64,
}

enum Waiter {
    /// A parked thread; the ticket lets it remove its own entry.
    Thread { ticket: u64, unparker: Unparker },
    /// A producer gated on its holdback; `Weak` so the list cannot keep an
    /// evicted producer alive.
    Producer(Weak<CellSlot>),
}

impl Waiters {
    pub fn new() -> Self {
        Self {
            waiters: Mutex::new(VecDeque::new()),
            next_ticket: AtomicU64::new(0),
        }
    }

    /// Register and park until a pop frees a slot or `timeout` elapses
    /// (`None` = until woken). The recheck after registering is what keeps a
    /// producer that lost the race with a draining consumer from parking
    /// forever; the entry is removed on every exit path so a departed waiter
    /// never absorbs a wake a live one was owed.
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

    pub fn wait_for_room<P: Send + 'static>(&self, mailbox: &Mailbox<P>) {
        self.wait_for_room_until(mailbox, None);
    }

    /// [`wait_for_room`](Self::wait_for_room) with a bounded park, so a lost
    /// wake degrades to a retry loop rather than a silent freeze.
    pub fn wait_for_room_timeout<P: Send + 'static>(
        &self,
        mailbox: &Mailbox<P>,
        timeout: Duration,
    ) {
        self.wait_for_room_until(mailbox, Some(timeout));
    }

    /// Register a producing Cell as owed the next free slot; it holds no
    /// thread. At most one entry per producer, or its re-registrations would
    /// absorb wakes a parked thread is owed; the caller rechecks `is_full()`
    /// afterwards like a parking waiter does.
    pub fn wait_producer(&self, producer: &Arc<CellSlot>) {
        let mut deque = self.waiters.lock().expect("Waiters mutex poisoned");
        let already = deque
            .iter()
            .any(|w| matches!(w, Waiter::Producer(p) if p.as_ptr() == Arc::as_ptr(producer)));
        if !already {
            deque.push_back(Waiter::Producer(Arc::downgrade(producer)));
        }
    }

    /// Serve the oldest live waiter after a successful pop; a dropped
    /// producer's entry is skipped so it cannot absorb the room.
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

    /// Approximate under contention; diagnostics only.
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
    use crate::test_support::{saturate, spin_until};

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

    #[test]
    fn serving_one_releases_the_parked_waiter() {
        let waiters = Arc::new(Waiters::new());
        let mbox = Arc::new(Mailbox::<u8>::with_capacity(1));
        saturate(&mbox, 0);
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
        let mbox = Arc::new(Mailbox::<u8>::with_capacity(1));
        saturate(&mbox, 0);
        // Use the internal API by parking three threads serialized via
        // sleeps so we can assert the wait_count grows monotonically.
        let p = Arc::new(waiters);
        let order = Arc::new(Mutex::new(Vec::<u8>::new()));
        let mut threads = Vec::new();
        for (parked, tag) in (*b"ABC").into_iter().enumerate() {
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

    /// A producer whose consumer drained between its failed push and its
    /// registration does not park: the recheck after registering returns.
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

    /// A timed-out waiter leaves no entry behind either.
    #[test]
    fn a_timed_out_waiter_removes_its_own_entry() {
        let waiters = Waiters::new();
        let mbox = Arc::new(Mailbox::<u8>::with_capacity(1));
        saturate(&mbox, 0);
        for _ in 0..5 {
            waiters.wait_for_room_timeout(&mbox, Duration::from_millis(1));
        }
        assert_eq!(waiters.waiter_count(), 0);
    }
}
