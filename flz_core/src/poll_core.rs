//! The poll/complete work engine: per-entity activation handout.
//!
//! The single-active-drainer invariant, expressed at a boundary: an entity
//! with an outstanding (handed-out, uncompleted) activation never appears in
//! another handout until [`PollCore::complete`] releases it. The engine owns
//! everything that used to run inside the drain loop except user code:
//! consumed-floor skip, replay-slice buffering and event-time ordering,
//! late-input admission and classification, event-time due-timer generation,
//! stale-timer validation, and the retry→park ladder's counting/backoff/
//! re-hand. The host half (`operative::poll_pool::serve_slot`) runs one
//! attempt per item and reports the disposition.
//!
//! An event-time-clocked class is handed ONE admitted input per activation:
//! a chained timer registration lands only at completion, so batching more
//! would make due-firing interleaving depend on batch timing — and a
//! correction replay must fold firings at the same positions the live run
//! did. Wall/logical classes batch freely.
//!
//! The core never runs host code: scheduled rounds queue on `ready` and host
//! workers — the typed layer's poll threads
//! (`operative::poll_pool`), or a C-ABI host's own —
//! block in [`PollCore::poll`] and answer with completions. A host may also
//! serve one slot inline (`try_assemble` + complete), which is how a
//! Block-policy producer drains a full consumer on its own thread.
//!
//! It also owns the one answer to "do something with this entity at time T":
//! the [`DeferredWake`] list, holding retry backoffs and processing-time timer
//! deadlines. No thread waits out either — the list is serviced by whichever
//! poller next enters or wakes, and its nearest entry shortens that wait.
//!
//! And it owns the one limit on how much work may be underway
//! ([`PollCore::max_inflight`]), decoupled from any host's thread count: the
//! core decides what runs and how much of it, each host brings the threads its
//! runtime does best. [`PollCore::wait_starvation`] is the other half of that
//! contract — a host thread parks in it and learns that work is queued with
//! nobody left to take it, in the same bring-your-own-thread shape as `poll`.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use flz_transport::transport::FlezhaStream;

use crate::channel::OverflowPolicy;
use crate::correction::Lateness;
use crate::entity::{EntityItem, EntityPayload, EntitySlot};
use crate::timer::{TimerFire, TimerKind};
use crate::util::LockRecover;

/// One work item as the engine hands it out: the mailbox item plus what the
/// engine resolved about it — its core-evaluated `now`, and where it stands
/// on the retry ladder. Cloning is shallow (`Arc` bumps).
#[derive(Clone)]
pub struct WorkItem {
    pub item: EntityItem,
    /// Core-evaluated `now`: the extracted event time for an event-time
    /// class's input, the scheduled timestamp for a firing. `None` = the
    /// typed layer evaluates its declared clock (wall or logical) at
    /// invocation — ambient reads stay in-process today; the FFI boundary
    /// stamps them core-side.
    pub now: Option<u64>,
    pub attempt: u32,
    /// A failing final attempt parks instead of returning `RetryLater`.
    pub is_final: bool,
}

/// One handed-out unit of work. For a [`EnginePayload::Work`] batch,
/// holding it IS the exclusivity token — the slot's `drain_active`/inflight
/// gates stay held until `complete(token)` / `retry_after_failure(token, ..)`.
pub struct EngineActivation {
    pub token: u64,
    pub payload: EnginePayload,
}

pub enum EnginePayload {
    /// An ordered per-entity batch of inputs and timer firings.
    Work {
        slot: Arc<EntitySlot>,
        items: Vec<WorkItem>,
    },
    /// A host-participating command interaction (restore fold, snapshot
    /// request, live-state read, apply-state, migration serialize) or a
    /// bounded crossing (stage batch, compensation, run-sugar seed) queued
    /// as a real activation for a C-ABI host. It carries no engine items;
    /// its exclusivity is the caller's — the entity gate a command thread
    /// holds inside [`PollCore::with_entity_exclusive`], or the bounded
    /// driver's own stage serialization — and its completion answers the
    /// enqueuer's rendezvous instead of releasing a slot.
    Command {
        target: crate::schema::EntityRef,
        change_seq: u64,
        factory: String,
        kind: crate::schema::ActivationKind,
        reply: crossbeam::channel::Sender<crate::schema::ActivationOutcome>,
    },
    /// A batched prepare crossing for one entry channel — pre-keying work,
    /// bound to no entity yet.
    Prepare(crate::host::PrepareCrossing),
}

impl EngineActivation {
    /// The work batch's slot. The typed poll workers and the engine tests
    /// only ever see work activations.
    pub fn slot(&self) -> &Arc<EntitySlot> {
        match &self.payload {
            EnginePayload::Work { slot, .. } => slot,
            _ => unreachable!("only work activations are bound to a live slot"),
        }
    }

    /// The work batch's items (empty for command/prepare payloads).
    pub fn items(&self) -> &[WorkItem] {
        match &self.payload {
            EnginePayload::Work { items, .. } => items,
            _ => &[],
        }
    }

    /// Consume into the work parts, or `None` for a non-work payload.
    pub fn into_work(self) -> Option<(u64, Arc<EntitySlot>, Vec<WorkItem>)> {
        match self.payload {
            EnginePayload::Work { slot, items } => Some((self.token, slot, items)),
            _ => None,
        }
    }
}

pub enum Polled {
    Activation(EngineActivation),
    Timeout,
    ShuttingDown,
}

/// What [`PollCore::wait_starvation`] answers — the same three-way shape as
/// [`Polled`], so a host's supervisor loop reads like its worker loop and a
/// shutdown ends both.
pub enum Starvation {
    /// Runnable work with nobody left to run it: the host owes a thread.
    Observed,
    Timeout,
    ShuttingDown,
}

/// A deferred wake's place in the list: due time first, then a total
/// tie-break so entries due at the same instant have a reproducible order.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct WakeKey {
    due_at_ms: u64,
    tiebreak: WakeTiebreak,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum WakeTiebreak {
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
}

enum Wake {
    /// Finish a deferred re-arm: the slot's holdback is already filled and
    /// both its gates are still held.
    Retry(Arc<EntitySlot>),
    /// A processing-time deadline. `Weak` so an idle-evicted Entity's timers
    /// die with it: the upgrade fails and the firing is dropped, which is the
    /// D6 contract. The stream is the one captured at arming, so `on_timer`'s
    /// plugin emits leave on the same connection an input's would.
    Timer {
        slot: Weak<EntitySlot>,
        stream: Arc<FlezhaStream>,
    },
}

/// The core's one deferred-wake list. It owns no thread: entries are promoted
/// by whichever thread next enters or wakes inside [`PollCore::poll`], and
/// recording one returns the recorder immediately.
struct DeferredWake {
    entries: Mutex<BTreeMap<WakeKey, Wake>>,
    /// Due time of the earliest entry, `u64::MAX` when empty. Maintained
    /// under `entries`' lock so the poll path decides "nothing is due" with
    /// one relaxed load instead of taking the lock per activation.
    earliest: AtomicU64,
}

impl DeferredWake {
    fn new() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            earliest: AtomicU64::new(u64::MAX),
        }
    }

    /// Insert one entry, replacing any that shares its key.
    fn record(&self, key: WakeKey, wake: Wake) {
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
            .filter(|k| matches!(&k.tiebreak, WakeTiebreak::Timer(armed) if armed == entity))
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
    fn any_due(&self, now: u64) -> bool {
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

pub struct PollCore {
    ready: Mutex<VecDeque<Arc<EntitySlot>>>,
    /// Command activations for host classes, handed out ahead of `ready`.
    /// Their entity gate is held by the enqueuing command thread, so handing
    /// them out never races a work assembly for the same entity.
    direct: Mutex<VecDeque<EngineActivation>>,
    cv: Condvar,
    shutting_down: AtomicBool,
    next_token: AtomicU64,
    outstanding: DashMap<u64, Arc<EntitySlot>>,
    wakes: DeferredWake,
    next_wake: AtomicU64,
    /// Work activations underway. `outstanding.len()` would answer it too,
    /// but a `DashMap` counts by read-locking every shard, and this is read
    /// on the handout path.
    outstanding_count: AtomicUsize,
    /// The cap on work activations underway at once. Enforced at handout, so
    /// a host may run far more threads than this without ever exceeding it —
    /// and far fewer without the cap mattering.
    max_inflight: usize,
    /// Pollers currently inside [`Self::poll`]. Presence is counted, not just
    /// the condvar wait: a poller mid-assembly is about to take the queued
    /// work, so it must not read as starvation.
    pollers_inside: AtomicUsize,
    /// Monotonic count of [`Self::poll`] entries. What separates a runner
    /// that is merely *busy* — threads churning through batches, each one
    /// briefly leaving `poll` — from one whose every thread is *stuck* inside
    /// user code. See [`Self::wait_starvation`].
    poll_entries: AtomicU64,
    /// Where a host thread parks in [`Self::wait_starvation`]. Paired with
    /// `ready`'s mutex so the predicate is evaluated under the same lock
    /// producers take. Deliberately NOT signalled by `schedule` or by a
    /// poller leaving: those are the hottest paths in the engine, and one
    /// futex wake each would cost more than the whole feature is worth. The
    /// waiter re-evaluates on its own [`STARVATION_DWELL`] tick instead.
    starve_cv: Condvar,
}

static GLOBAL: OnceLock<PollCore> = OnceLock::new();

pub fn global() -> &'static PollCore {
    GLOBAL.get_or_init(PollCore::new)
}

/// How many work activations may be underway at once when
/// `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` says nothing. High enough that a healthy
/// blocking workload (hundreds of concurrent slow calls on host threads that
/// cost nothing to block) never notices; low enough to bound the memory held
/// by that many assembled batches.
const MAX_INFLIGHT_DEFAULT: usize = 1024;
const ENV_MAX_INFLIGHT: &str = "FLEZHA_MAX_INFLIGHT_ACTIVATIONS";

/// How long queued work must sit with no thread entering `poll` before it
/// counts as starvation, and the longest a starvation waiter parks between
/// re-evaluations. A batch takes microseconds, so a busy runner re-enters
/// `poll` orders of magnitude more often than this; only a runner whose
/// threads are all *stuck* inside user code shows no entry at all for the
/// window.
const STARVATION_DWELL: Duration = Duration::from_millis(100);

impl PollCore {
    pub fn new() -> Self {
        Self::with_max_inflight(crate::util::env_parse_positive(
            ENV_MAX_INFLIGHT,
            MAX_INFLIGHT_DEFAULT,
        ))
    }

    /// A core with an explicit jobs-underway cap. The engine's own tests use
    /// it to reach the cap without queuing a thousand entities.
    pub fn with_max_inflight(max_inflight: usize) -> Self {
        Self {
            ready: Mutex::new(VecDeque::new()),
            direct: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            shutting_down: AtomicBool::new(false),
            next_token: AtomicU64::new(0),
            outstanding: DashMap::new(),
            wakes: DeferredWake::new(),
            next_wake: AtomicU64::new(0),
            outstanding_count: AtomicUsize::new(0),
            max_inflight,
            pollers_inside: AtomicUsize::new(0),
            poll_entries: AtomicU64::new(0),
            starve_cv: Condvar::new(),
        }
    }

    /// Make a slot's round servable: queue it and wake one blocked poller.
    /// Callers hold the round's inflight token (a winning
    /// `try_mark_inflight`), exactly as they did when this call was
    /// `pool.submit(drain_entity)`. The core never runs host code — hosts
    /// poll.
    pub fn schedule(&self, slot: &Arc<EntitySlot>) {
        self.ready.lock_recover().push_back(Arc::clone(slot));
        self.cv.notify_one();
    }

    /// Block until an activation is available, the timeout elapses, or
    /// shutdown wakes every poller. Any host thread may call this.
    ///
    /// Also the deferred-wake list's only service: every pass promotes what
    /// has come due, and the nearest entry shortens the wait.
    pub fn poll(&self, timeout: Duration) -> Polled {
        self.poll_entries.fetch_add(1, Ordering::Release);
        self.pollers_inside.fetch_add(1, Ordering::Release);
        let polled = self.poll_until(Instant::now() + timeout);
        self.pollers_inside.fetch_sub(1, Ordering::Release);
        polled
    }

    fn poll_until(&self, deadline: Instant) -> Polled {
        loop {
            // First thing in the pass, before any lock is taken: a firing
            // enqueues under the Block policy and may drain a saturated
            // consumer inline, and peers must keep serving `ready` while it
            // does.
            self.promote_due();
            let mut ready = self.ready.lock_recover();
            if self.shutting_down.load(Ordering::Acquire) {
                return Polled::ShuttingDown;
            }
            if let Some(act) = self.direct.lock_recover().pop_front() {
                return Polled::Activation(act);
            }
            // At the cap, entity work stays queued and pollers wait as if
            // `ready` were empty; the completion that drops the count below it
            // notifies. Command activations were already served above — they
            // are barrier-bounded by their own drivers, and holding one behind
            // blocked entity work would deadlock the driver waiting for it.
            while self.outstanding_count.load(Ordering::Acquire) < self.max_inflight {
                let Some(slot) = ready.pop_front() else { break };
                drop(ready);
                if let Some(act) = self.try_assemble(&slot) {
                    return Polled::Activation(act);
                }
                ready = self.ready.lock_recover();
            }
            let now = Instant::now();
            if now >= deadline {
                return Polled::Timeout;
            }
            // The guard dies with the pass; the next one re-locks after
            // promoting.
            let _ = self.cv.wait_timeout(ready, self.park_slice(deadline, now));
        }
    }

    /// Park until the core observes starvation — runnable work with nobody
    /// left to run it — or `timeout` elapses.
    ///
    /// Starvation is *stuck*, not *busy*. Queued entity work counts only once
    /// it has sat for [`STARVATION_DWELL`] with **no thread entering `poll`
    /// at all**: a saturated runner enters `poll` once per batch, i.e. orders
    /// of magnitude more often, and answering that with more threads would
    /// add contention to a runner that is merely working.
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
        // already inside `poll`, nor at the cap.
        if self.pollers_inside.load(Ordering::Acquire) > 0
            || self.outstanding_count.load(Ordering::Acquire) >= self.max_inflight
        {
            *held = None;
            return false;
        }
        // A due wake needs no dwell: every `poll` pass promotes what has come
        // due, so an entry still sitting there proves no pass has happened
        // since it did.
        if self.wakes.any_due(crate::util::wall_now_ms()) {
            return true;
        }
        if ready_empty {
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

    /// How long to park: until `deadline`, but never past the nearest due
    /// wake. Reads the list's cached earliest and takes no lock — `ready` is
    /// held by both callers, and a recorder takes the list's lock and then
    /// `ready`, so locking the list here would close a cycle.
    fn park_slice(&self, deadline: Instant, now: Instant) -> Duration {
        let mut wait = deadline.saturating_duration_since(now);
        if let Some(until_due) = self.wakes.next_due_in(crate::util::wall_now_ms()) {
            wait = wait.min(until_due);
        }
        wait
    }

    /// Hand every entry that has come due to its effect. Called once per
    /// [`Self::poll`] pass — the list has no thread of its own.
    fn promote_due(&self) {
        for (key, wake) in self.wakes.take_due(crate::util::wall_now_ms()) {
            match wake {
                Wake::Retry(slot) => self.release_and_rearm(&slot),
                Wake::Timer { slot, stream } => {
                    let Some(slot) = slot.upgrade() else {
                        continue;
                    };
                    // Block policy: a saturated mailbox delays the firing
                    // (and drains the slot inline) rather than dropping it —
                    // a timer silently lost is a bug the author cannot see.
                    crate::routing::enqueue(
                        &slot,
                        EntityItem::Timer(TimerFire {
                            kind: TimerKind::ProcessingTime,
                            scheduled_at: key.due_at_ms,
                            stream,
                        }),
                        OverflowPolicy::Block,
                    );
                }
            }
        }
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
            },
        );
        self.notify_waiters();
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
    fn notify_waiters(&self) {
        let _ready = self.ready.lock_recover();
        self.cv.notify_all();
    }

    /// Try to become the slot's active drainer and assemble its batch.
    /// `None`: another thread owns the round (it will run the tail
    /// protocol), or nothing is currently runnable.
    pub fn try_assemble(&self, slot: &Arc<EntitySlot>) -> Option<EngineActivation> {
        if slot.drain_active.swap(true, Ordering::AcqRel) {
            return None;
        }
        loop {
            let items = assemble_batch(slot);
            if !items.is_empty() {
                let token = self.next_token.fetch_add(1, Ordering::Relaxed) + 1;
                self.outstanding.insert(token, Arc::clone(slot));
                self.outstanding_count.fetch_add(1, Ordering::Release);
                return Some(EngineActivation {
                    token,
                    payload: EnginePayload::Work {
                        slot: Arc::clone(slot),
                        items,
                    },
                });
            }
            // Nothing runnable (skipped, or buffered into an open slice):
            // the drain-tail protocol, verbatim. Drop the active gate BEFORE
            // releasing inflight so a producer that observes inflight=false
            // can immediately become the next drainer.
            slot.drain_active.store(false, Ordering::Release);
            slot.mailbox.release_inflight();
            if slot.mailbox.is_empty() {
                return None;
            }
            if !slot.mailbox.try_mark_inflight() {
                return None;
            }
            if slot.drain_active.swap(true, Ordering::AcqRel) {
                // Another thread became the active drainer for the round we
                // just marked; it releases the inflight token when done.
                return None;
            }
        }
    }

    /// Release the entity and, if work remains, schedule the next round.
    /// Any host thread may call this — including a different one than the
    /// thread that polled the activation.
    pub fn complete(&self, token: u64) {
        let Some(slot) = self.take_outstanding(token) else {
            return;
        };
        self.release_and_rearm(&slot);
    }

    /// Drop one activation from the jobs-underway count. A count that was at
    /// the cap had pollers parked as if `ready` were empty, so falling below
    /// it is the one event that makes them servable again.
    fn take_outstanding(&self, token: u64) -> Option<Arc<EntitySlot>> {
        let (_, slot) = self.outstanding.remove(&token)?;
        if self.outstanding_count.fetch_sub(1, Ordering::AcqRel) >= self.max_inflight {
            self.notify_waiters();
        }
        Some(slot)
    }

    /// A retryable failure stopped the batch at `items[0]`: bump its attempt,
    /// hold it and the unprocessed tail at the head of the line, and record
    /// the backoff as a deferred wake. The completing thread returns at once.
    ///
    /// The entity's gates stay held across the window, reproducing today's
    /// in-line sleep exactly: nothing else could drain the entity then
    /// either, producers just enqueued. The promoting poller runs the
    /// release-and-reclaim when the entry comes due.
    pub fn retry_after_failure(&self, token: u64, mut items: Vec<WorkItem>, error: String) {
        let Some(slot) = self.take_outstanding(token) else {
            return;
        };
        let mut delay = Duration::ZERO;
        if let Some(head) = items.first_mut() {
            let policy = slot.dispatcher.retry_policy();
            delay = crate::retry::note_failed_attempt(
                &format!("procedure {}", slot.dispatcher.procedure_name()),
                head.attempt,
                policy,
                &error,
            );
            head.attempt += 1;
            head.is_final = head.attempt >= policy.max_attempts;
        }
        self.hold_head_of_line(&slot, items);
        if delay.is_zero() {
            self.release_and_rearm(&slot);
            return;
        }
        self.wakes.record(
            WakeKey {
                due_at_ms: crate::util::wall_now_ms().saturating_add(delay.as_millis() as u64),
                tiebreak: WakeTiebreak::Retry(self.next_wake.fetch_add(1, Ordering::Relaxed)),
            },
            Wake::Retry(slot),
        );
        self.notify_waiters();
    }

    /// Put back items the host never processed — they sat behind a failed
    /// head that parked. No attempt bump and no backoff: these items are not
    /// failures, they are simply owed a fresh round.
    pub fn rehand_unprocessed(&self, token: u64, items: Vec<WorkItem>) {
        let Some(slot) = self.take_outstanding(token) else {
            return;
        };
        self.hold_head_of_line(&slot, items);
        self.release_and_rearm(&slot);
    }

    /// Park re-handed items at the head of the entity's line. Only ever
    /// called while the slot is handed out, which is what the assertion
    /// states; `assemble_batch` drains the holdback first on the next round,
    /// so per-entity FIFO and head-retry ordering are unchanged.
    fn hold_head_of_line(&self, slot: &Arc<EntitySlot>, items: Vec<WorkItem>) {
        if items.is_empty() {
            return;
        }
        let mut held = slot.pending_retry.lock_recover();
        debug_assert!(held.is_empty(), "holdback refilled while handed out");
        held.extend(items);
    }

    /// Release the entity's gates and claim the round anything left owes.
    /// Drop `drain_active` BEFORE releasing inflight so a producer that
    /// observes inflight=false can immediately become the next drainer; a
    /// losing CAS means a producer already scheduled the round.
    fn release_and_rearm(&self, slot: &Arc<EntitySlot>) {
        slot.drain_active.store(false, Ordering::Release);
        slot.mailbox.release_inflight();
        if !slot.mailbox.is_empty() {
            if slot.mailbox.try_mark_inflight() {
                self.schedule(slot);
            }
            return;
        }
        // The mailbox is dry, but work only assembly can see may remain: a
        // re-handed head of line, a chained event-time registration already
        // behind the high-water, or the still-buffered remainder of a closed
        // replay slice (drained one member per round for an event-time
        // class).
        if (!slot.pending_retry.lock_recover().is_empty()
            || event_time_due_pending(slot)
            || closed_slice_pending(slot)
            || !slot.deferred_live.lock_recover().is_empty())
            && slot.mailbox.try_mark_inflight()
        {
            self.schedule(slot);
        }
    }

    /// Queue a non-work payload (command interaction, prepare crossing) for
    /// a host poller, ahead of the ready queue.
    pub fn submit_direct(&self, payload: EnginePayload) -> u64 {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed) + 1;
        self.direct
            .lock_recover()
            .push_back(EngineActivation { token, payload });
        self.cv.notify_one();
        token
    }

    /// Queue a command interaction as a real activation for a host poller
    /// and block for its completion. MUST be called while the slot's handout
    /// gate is held (inside [`Self::with_entity_exclusive`]) — that gate is
    /// what keeps input activations out while the command runs, and it makes
    /// handing this activation out race-free by construction.
    pub fn host_command(
        &self,
        slot: &Arc<EntitySlot>,
        kind: crate::schema::ActivationKind,
    ) -> Result<crate::schema::ActivationOutcome, String> {
        let describe = command_kind_name(&kind);
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(format!("{describe}: runner is shutting down"));
        }
        let (reply, rx) = crossbeam::channel::bounded(1);
        self.submit_direct(EnginePayload::Command {
            target: crate::host::entity_ref_of(slot),
            change_seq: slot.change_seq.load(Ordering::Acquire),
            factory: slot.topology.factory_id.to_string(),
            kind,
            reply,
        });
        match rx.recv_timeout(HOST_COMMAND_TIMEOUT) {
            Ok(outcome) => Ok(outcome),
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => Err(format!(
                "{describe}: host did not complete the activation within {}s",
                HOST_COMMAND_TIMEOUT.as_secs()
            )),
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                Err(format!("{describe}: host context dropped the activation"))
            }
        }
    }

    /// Run a host-participating command interaction (restore fold, snapshot
    /// request, live-state read, apply-state, migration serialize) under the
    /// entity's handout exclusivity: it waits out any outstanding activation
    /// and no activation is handed out while it runs — the same gate an
    /// input batch holds, so a fold completes before inputs flow. The
    /// in-process form of the command activation kinds; the C ABI queues
    /// them as real activations via [`Self::host_command`].
    pub fn with_entity_exclusive<R>(&self, slot: &Arc<EntitySlot>, f: impl FnOnce() -> R) -> R {
        let started = Instant::now();
        let mut warned = false;
        while slot.drain_active.swap(true, Ordering::AcqRel) {
            if !warned && started.elapsed() > Duration::from_secs(5) {
                warned = true;
                tracing::warn!(
                    target: "flezha::poll_core",
                    entity = slot.dispatcher.entity_label(),
                    "command interaction waiting >5s for an outstanding activation"
                );
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let result = f();
        // The winner's duty: release the gates and hand anything that queued
        // meanwhile to a fresh round (a serve task that lost the gate to us
        // left its inflight token for us to release).
        self.release_and_rearm(slot);
        result
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        self.cv.notify_all();
        self.starve_cv.notify_all();
    }
}

/// How long a command thread waits for a host to answer its queued
/// activation before reporting the interaction failed.
const HOST_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

fn command_kind_name(kind: &crate::schema::ActivationKind) -> &'static str {
    use crate::schema::ActivationKind;
    match kind {
        ActivationKind::Work(_) => "work",
        ActivationKind::RestoreFold(_) => "restore_fold",
        ActivationKind::SnapshotRequest => "snapshot_request",
        ActivationKind::LiveStateRead => "live_state_read",
        ActivationKind::ApplyState { .. } => "apply_state",
        ActivationKind::MigrationSerialize => "migration_serialize",
        ActivationKind::KeyRequest(_) => "key_request",
        ActivationKind::BoundedRun { .. } => "bounded_run",
        ActivationKind::CompensateRun { .. } => "compensate_run",
    }
}

fn closed_slice_pending(slot: &Arc<EntitySlot>) -> bool {
    slot.replay_depth.load(Ordering::Relaxed) != 0 && !slot.replay_open.load(Ordering::Acquire)
}

fn event_time_due_pending(slot: &Arc<EntitySlot>) -> bool {
    if !slot.dispatcher.is_event_time() {
        return false;
    }
    let high_water = slot.event_time_high_water.load(Ordering::Acquire);
    if high_water < 0 {
        return false;
    }
    !slot
        .timers
        .lock_recover()
        .due(TimerKind::EventTime, high_water as u64)
        .is_empty()
}

/// Batch assembly: everything the drain pop-loop decided, minus user code.
/// Frees one blocked producer per pop; skips inputs at or below the durably
/// consumed floor; buffers replay-slice members until the slice closes;
/// admits (or drops, or asks the controller for a correction of) late
/// inputs; generates due event-time firings ahead of the input that advanced
/// the high-water; validates timer firings against the pending set.
fn assemble_batch(slot: &Arc<EntitySlot>) -> Vec<WorkItem> {
    let policy = slot.dispatcher.retry_policy();
    let event_time_class = slot.dispatcher.is_event_time();
    let mut items: Vec<WorkItem> = Vec::new();
    // Event-time deadlines already included in this batch — an input's
    // advance must not re-fire what an earlier line of the same batch fires.
    let mut fired: HashSet<u64> = HashSet::new();

    {
        let mut held = slot.pending_retry.lock_recover();
        if !held.is_empty() {
            items.extend(held.drain(..));
            if event_time_class {
                // The held items already contain this round's one admitted
                // input (an event-time batch ends with it); admit no more.
                return items;
            }
        }
    }

    if event_time_class && !closed_slice_pending(slot) {
        // Deadlines a previous batch's completion chained behind the
        // high-water. No admitting input to inherit a connection from, so
        // use the live secondary stream (the `rearm_restored` precedent);
        // without one the firing waits for the next input.
        if let Some(stream) = crate::state_sink::secondary_stream() {
            push_due_event_timers(slot, &stream, &mut items, &mut fired, policy);
        }
    }

    // Live items deferred by an earlier round because a closed slice was
    // still draining: they are older than anything in the mailbox, so they
    // go first — through the same staging as a fresh pop.
    if !closed_slice_pending(slot) {
        let deferred: Vec<EntityItem> = {
            let mut held = slot.deferred_live.lock_recover();
            held.drain(..).collect()
        };
        for item in deferred {
            if let Staged::AdmittedInput =
                stage_popped_item(slot, item, &mut items, &mut fired, policy, event_time_class)
            {
                if event_time_class {
                    return items;
                }
            }
        }
    }

    loop {
        let Some(item) = slot.mailbox.pop() else {
            break;
        };
        slot.block_parker.unpark_one();
        if closed_slice_pending(slot) {
            if let EntityItem::Input(payload) = &item {
                if payload.replay {
                    // A member that raced the broadcast: it extends the
                    // closed slice, never splits it.
                    let EntityItem::Input(payload) = item else {
                        unreachable!()
                    };
                    slot.replay_buffer.lock_recover().push(payload);
                    slot.replay_depth.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
            // A non-replay item behind a closed slice: the slice runs first —
            // whole where it can, one member per round where pending
            // event-time timers force member-granular determinism. The item
            // waits its turn behind the remainder.
            flush_closed_slice(slot, &mut items, &mut fired, policy, event_time_class);
            if closed_slice_pending(slot) {
                slot.deferred_live.lock_recover().push_back(item);
                return items;
            }
            match stage_popped_item(slot, item, &mut items, &mut fired, policy, event_time_class) {
                Staged::AdmittedInput if event_time_class => break,
                _ => continue,
            }
        }
        match stage_popped_item(slot, item, &mut items, &mut fired, policy, event_time_class) {
            Staged::AdmittedInput if event_time_class => break,
            _ => continue,
        }
    }
    // Mailbox dry (or an event-time input closed the batch): a CLOSED slice
    // buffered behind it flushes now, under the same gate; an open one keeps
    // waiting for its broadcast.
    if closed_slice_pending(slot)
        && !(event_time_class && items.iter().any(|w| matches!(w.item, EntityItem::Input(_))))
    {
        flush_closed_slice(slot, &mut items, &mut fired, policy, event_time_class);
    }
    items
}

/// What one popped (or deferred) mailbox item became.
enum Staged {
    /// A live input was admitted and staged (an event-time batch ends here).
    AdmittedInput,
    /// Buffered, skipped, dropped, or a timer item — the batch stays open.
    Other,
}

/// Classify and stage one mailbox item — the shared body for fresh pops and
/// re-served deferred items.
fn stage_popped_item(
    slot: &Arc<EntitySlot>,
    item: EntityItem,
    items: &mut Vec<WorkItem>,
    fired: &mut HashSet<u64>,
    policy: crate::channel::RetryPolicy,
    event_time_class: bool,
) -> Staged {
    let payload = match item {
        EntityItem::Timer(fire) => {
            // The wake list holds no lock on the pending set, so an entry can
            // outlive the timer: a cancellation, a re-fire after a restore
            // re-armed it, an Entity restored without it. The set is the
            // authority — a stale entry fires nothing.
            if slot
                .timers
                .lock_recover()
                .contains(fire.kind, fire.scheduled_at)
            {
                items.push(WorkItem {
                    now: Some(fire.scheduled_at),
                    attempt: 1,
                    is_final: 1 >= policy.max_attempts,
                    item: EntityItem::Timer(fire),
                });
            } else {
                crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
            }
            return Staged::Other;
        }
        EntityItem::Input(payload) => payload,
    };
    // Effectively-once crash-replay: the controller re-forwards the
    // journaled input tail; offsets this Entity already durably consumed
    // (restored effect records) are skipped, never re-run.
    if let Some(offset) = payload.consumed_offset {
        if (offset as i64) <= slot.consumed_floor.load(Ordering::Acquire) {
            crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
            return Staged::Other;
        }
    }
    // A replayed slice is held whole, ordered by event time, and only
    // then dispatched. Re-running it in arrival order would put the late
    // input back after the inputs that overtook it and recompute the
    // same wrong answer.
    if payload.replay {
        slot.replay_buffer.lock_recover().push(payload);
        slot.replay_depth.fetch_add(1, Ordering::Relaxed);
        return Staged::Other;
    }
    if stage_input(slot, payload, items, fired, policy, event_time_class) {
        Staged::AdmittedInput
    } else {
        Staged::Other
    }
}

/// Admit one live (or replayed, via `flush_closed_slice`) input and stage it
/// — preceded by whatever event-time deadlines its high-water advance made
/// due. Returns whether the input was admitted.
fn stage_input(
    slot: &Arc<EntitySlot>,
    payload: EntityPayload,
    items: &mut Vec<WorkItem>,
    fired: &mut HashSet<u64>,
    policy: crate::channel::RetryPolicy,
    event_time_class: bool,
) -> bool {
    let is_final = 1 >= policy.max_attempts;
    if !event_time_class {
        items.push(WorkItem {
            now: None,
            attempt: 1,
            is_final,
            item: EntityItem::Input(payload),
        });
        return true;
    }
    let Some(event_time) = slot.dispatcher.event_time_of(&payload) else {
        // Payload type mismatch — hand it through so the typed layer reports
        // it on the dispatch path, as it always has.
        items.push(WorkItem {
            now: None,
            attempt: 1,
            is_final,
            item: EntityItem::Input(payload),
        });
        return true;
    };
    if !admit_and_track(slot, event_time, payload.replay) {
        crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
        return false;
    }
    // Event-time timers fire on the high-water this input just advanced,
    // ascending, and BEFORE the input itself — otherwise a window would
    // close after the record that belongs to the next one.
    push_due_event_timers(slot, &payload.stream, items, fired, policy);
    items.push(WorkItem {
        now: Some(event_time),
        attempt: 1,
        is_final,
        item: EntityItem::Input(payload),
    });
    true
}

/// Decide whether an input should be dispatched at all, and keep the
/// Entity's event-time high-water.
///
/// An input behind the high-water but inside the correction window is
/// dropped *here* and asked for back: it is already durable in the input
/// log, so the correction re-delivers it inside the replayed slice, in its
/// rightful place. Running it now would fold it into the state at the wrong
/// position — the very mistake the correction undoes.
///
/// A `replayed` input is exempt from classification: the engine has just put
/// it back into event-time order, so it is in order by construction. It
/// still advances the high-water — the replay is what rebuilds it.
///
/// The high-water is kept for EVERY event-time-clocked class, not only a
/// correcting one: it is also what event-time timers fire against.
fn admit_and_track(slot: &Arc<EntitySlot>, event_time: u64, replayed: bool) -> bool {
    let advance = || {
        slot.event_time_high_water
            .fetch_max(event_time as i64, Ordering::AcqRel);
    };
    let Some(policy) = slot.dispatcher.correction_policy() else {
        advance();
        return true;
    };
    if replayed {
        advance();
        return true;
    }
    let high_water = slot.event_time_high_water.load(Ordering::Acquire);
    match crate::correction::classify(event_time, high_water, policy.window_ms) {
        Lateness::OnTime => {
            advance();
            true
        }
        // Requested: drop it here, the slice brings it back. Not requested
        // (transport-less run, nobody to ask): fall back to the
        // no-correction behaviour and process it as an arrival.
        Lateness::Correctable => !crate::correction::request(&slot.entity_identity, event_time),
        // The high-water deliberately does NOT advance for a beyond-window
        // arrival: a very old event must not drag the window backward and
        // make the inputs after it look late.
        Lateness::BeyondWindow => match policy.beyond_window {
            crate::operative::topology::BeyondWindowPolicy::ProcessAsArrival => true,
            crate::operative::topology::BeyondWindowPolicy::Drop => false,
        },
    }
}

/// Stage every pending event-time deadline at or below the high-water,
/// ascending, deduplicated against what this batch already fires.
fn push_due_event_timers(
    slot: &Arc<EntitySlot>,
    stream: &Arc<flz_transport::transport::FlezhaStream>,
    items: &mut Vec<WorkItem>,
    fired: &mut HashSet<u64>,
    policy: crate::channel::RetryPolicy,
) {
    let high_water = slot.event_time_high_water.load(Ordering::Acquire);
    if high_water < 0 {
        return;
    }
    for scheduled_at in slot
        .timers
        .lock_recover()
        .due(TimerKind::EventTime, high_water as u64)
    {
        if fired.insert(scheduled_at) {
            items.push(WorkItem {
                now: Some(scheduled_at),
                attempt: 1,
                is_final: 1 >= policy.max_attempts,
                item: EntityItem::Timer(TimerFire {
                    kind: TimerKind::EventTime,
                    scheduled_at,
                    stream: Arc::clone(stream),
                }),
            });
        }
    }
}

/// The end-of-slice broadcast arrived: order the buffered slice by event
/// time — stable, tie-broken by ascending input-log offset, so a re-run
/// folds the same inputs in the same order (reproducible correction) — and
/// stage it. An event-time class stages ONE member per batch (chained
/// due-timer determinism); the remainder re-buffers, still closed, and
/// `release_and_rearm` drives the next member's round.
///
/// The event-time high-water is deliberately NOT reset: the restore
/// preceding every replay already rebuilt it, and zeroing it would be lossy
/// on a crash resume whose unconsumed tail carries a lower max event time.
fn flush_closed_slice(
    slot: &Arc<EntitySlot>,
    items: &mut Vec<WorkItem>,
    fired: &mut HashSet<u64>,
    policy: crate::channel::RetryPolicy,
    event_time_class: bool,
) {
    let mut slice = std::mem::take(&mut *slot.replay_buffer.lock_recover());
    slot.replay_depth.store(0, Ordering::Relaxed);
    slice.sort_by_key(|p| {
        (
            slot.dispatcher.event_time_of(p).unwrap_or(0),
            p.consumed_offset.unwrap_or(0),
        )
    });
    // Member-granular draining is only needed when pending event-time
    // deadlines could interleave with the members (a chained registration
    // lands at completion, and a crash-resumed correction must fold the same
    // interleave). With none pending — the overwhelmingly common case — the
    // whole ordered slice is one batch, exactly the pre-poll drain.
    let member_granular = event_time_class
        && !slot
            .timers
            .lock_recover()
            .due(TimerKind::EventTime, u64::MAX)
            .is_empty();
    if member_granular {
        let mut members = slice.into_iter();
        if let Some(first) = members.next() {
            let rest: Vec<EntityPayload> = members.collect();
            if !rest.is_empty() {
                slot.replay_depth
                    .store(rest.len() as u64, Ordering::Relaxed);
                *slot.replay_buffer.lock_recover() = rest;
            }
            stage_input(slot, first, items, fired, policy, true);
        }
    } else {
        for payload in slice {
            stage_input(slot, payload, items, fired, policy, event_time_class);
        }
    }
}
