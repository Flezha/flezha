//! The poll/complete work engine: per-entity activation handout.
//!
//! The single-active-drainer invariant, expressed at a boundary: an entity
//! with an outstanding (handed-out, uncompleted) activation never appears in
//! another handout until it is answered. There are exactly three answers —
//! [`PollCore::complete`], [`PollCore::retry_after_failure`] and
//! [`PollCore::abandon`] — and a host that gives none of them retires that
//! entity for the life of the process, which is why every host makes the
//! answer structural (`docs/sdk/poll_core.md`). The core reports how long a
//! handout has been out ([`PollCore::outstanding_oldest_age_ms`]) and never
//! reclaims one on a timer: from here a slow Procedure and a lost handout are
//! identical. The engine owns
//! everything that used to run inside the drain loop except user code:
//! consumed-floor skip, replay-slice buffering and event-time ordering,
//! late-input admission and classification, event-time due-timer generation,
//! stale-timer validation, and the retry→park ladder's counting/backoff/
//! re-hand. The host half (`operative::dispatcher::run_activation`) runs one
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
//! block in [`PollCore::poll`] and answer with completions. **No call into the
//! core blocks a host thread**: a `DropNothing` push a producing entity cannot
//! deliver is held in that entity's push holdback and the entity is handed no
//! further activation until it lands ([`PollCore::release_and_rearm`]), which
//! is the same state a retry backoff puts it in. The one park the core keeps
//! is the transport reader's on the entry path — a core-owned thread with no
//! producing entity, whose stalling carries ingress backpressure to the
//! controller.
//!
//! It also owns the one answer to "do something with this entity at time T":
//! the [`wake::DeferredWake`] list, holding retry backoffs and processing-time
//! timer deadlines. No thread waits out either — the list is serviced by
//! whichever poller next enters or wakes, and its nearest entry shortens that
//! wait.
//!
//! And it owns the one limit on how much work may be underway
//! ([`PollCore::max_inflight`]), decoupled from any host's thread count: the
//! core decides what runs and how much of it, each host brings the threads its
//! runtime does best. [`PollCore::wait_starvation`] is the other half of that
//! contract — a host thread parks in it and learns that work is queued with
//! nobody left to take it, in the same bring-your-own-thread shape as `poll`.

mod assembly;
mod starvation;
mod wake;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use assembly::{assemble_batch, closed_slice_pending, event_time_due_pending};
use dashmap::DashMap;
use wake::{DeferredWake, Wake, WakeKey, WakeTiebreak};

use crate::entity::{EntityItem, EntitySlot};
use crate::util::LockRecover;

/// One item on its ladder, as the engine hands it out: the mailbox item plus
/// what the engine resolved about it — its core-evaluated `now`, and where it
/// stands on the retry ladder. Cloning is shallow (`Arc` bumps).
#[derive(Clone)]
pub struct ItemAttempt {
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
        items: Vec<ItemAttempt>,
    },
    /// A host-participating command interaction (restore fold, snapshot
    /// request, live-state read, apply-state, migration serialize, run-sugar
    /// seed) queued as a real activation for a C-ABI host. It carries no
    /// engine items; its exclusivity is the caller's — the entity gate a
    /// command thread holds inside [`PollCore::with_entity_exclusive`] — and
    /// its completion answers the enqueuer's rendezvous instead of releasing
    /// a slot.
    Command {
        target: crate::schema::EntityRef,
        change_seq: u64,
        factory: String,
        kind: crate::schema::ActivationKind,
        reply: crossbeam::channel::Sender<crate::schema::ActivationOutcome>,
    },
    /// One entity partition's share of a workflow stage (or of a saga undo).
    /// Its exclusivity is the workflow driver's own stage serialization, and
    /// its completion answers that driver's barrier; a typed class runs it in
    /// process, a C-ABI host receives it as a `Work` / `CompensateRun`
    /// activation.
    WorkflowStep(crate::workflow::step::WorkflowStep),
    /// A batched prepare crossing for one entry channel — pre-keying work,
    /// bound to no entity yet.
    Prepare(crate::host::PrepareCrossing),
}

impl EngineActivation {
    /// The work batch's slot. Only a work activation is bound to a slot the
    /// engine owns the gates of — a step carries its own.
    pub fn slot(&self) -> &Arc<EntitySlot> {
        match &self.payload {
            EnginePayload::Work { slot, .. } => slot,
            _ => unreachable!("only work activations are bound to a live slot"),
        }
    }

    /// The work batch's items (empty for every other payload).
    pub fn items(&self) -> &[ItemAttempt] {
        match &self.payload {
            EnginePayload::Work { items, .. } => items,
            _ => &[],
        }
    }

    /// Consume into the work parts, or `None` for a non-work payload.
    pub fn into_work(self) -> Option<(u64, Arc<EntitySlot>, Vec<ItemAttempt>)> {
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

/// What answering a handout did. A token the core does not hold means the
/// activation was already answered, or was never handed out — a host defect
/// that a silent no-op would hide until an entity stopped progressing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answered {
    /// The handout was taken and the entity released.
    Released,
    /// No live handout carries this token.
    UnknownToken,
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

/// One handed-out work activation, from the core's side.
struct Outstanding {
    slot: Arc<EntitySlot>,
    handed_at: Instant,
}

/// One Entity gated on its push holdback.
struct PendingPushes {
    since: Instant,
}

pub struct PollCore {
    ready: Mutex<VecDeque<Arc<EntitySlot>>>,
    /// Command and workflow-step activations, handed out ahead of `ready`.
    /// Their entity gate is held by the enqueuer — the command thread, or the
    /// workflow driver serializing its own stages — so handing them out never
    /// races a work assembly for the same entity.
    direct: Mutex<VecDeque<EngineActivation>>,
    cv: Condvar,
    shutting_down: AtomicBool,
    next_token: AtomicU64,
    /// Live handouts by token. The instant rides alongside the slot because
    /// the only thing that separates a ten-minute Procedure from a handout
    /// the host lost is how long it has been out — which the core reports
    /// and never acts on (`outstanding_oldest_age_ms`).
    outstanding: DashMap<u64, Outstanding>,
    /// Entities whose gates are held for an undelivered push, by identity.
    /// The same shape as `outstanding` and reported beside it: a gated
    /// producer holds no handout, so `inflight` cannot see it, and without its
    /// own report backpressure that withholds work would be invisible where
    /// backpressure that parks was at least visible as a growing handout age.
    deferred_pushes: DashMap<Arc<Vec<u8>>, PendingPushes>,
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
    /// waiter re-evaluates on its own [`starvation::STARVATION_DWELL`] tick
    /// instead.
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

/// The time-boxed fallback for a gated producer, mirroring the entry path's
/// `ROOM_WAIT_TIMEOUT`: the consumer's wait list is what normally re-arms it,
/// and this only caps how long a lost wake could hold it.
const PUSH_RETRY_BACKOFF_MS: u64 = 50;

impl Default for PollCore {
    fn default() -> Self {
        Self::new()
    }
}

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
            deferred_pushes: DashMap::new(),
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
    /// `claim_inflight`), exactly as they did when this call was
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
            // First thing in the pass, before any lock is taken: promotion
            // admits without waiting and re-records what it could not place,
            // so it holds nothing peers need.
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
            // notifies. Direct activations were already served above — they
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
                self.outstanding.insert(
                    token,
                    Outstanding {
                        slot: Arc::clone(slot),
                        handed_at: Instant::now(),
                    },
                );
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
            if !slot.mailbox.claim_inflight() {
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
    pub fn complete(&self, token: u64) -> Answered {
        self.release(token, Vec::new(), "complete")
    }

    /// The one body behind [`Self::complete`] (nothing owed) and
    /// [`Self::abandon`] (the items the host never processed): take the
    /// handout, park anything owed back at the head of the entity's line, and
    /// release its gates. `answer` names the caller in the unknown-token
    /// warning, which is the only thing that makes a double answer diagnosable.
    fn release(&self, token: u64, owed: Vec<ItemAttempt>, answer: &'static str) -> Answered {
        let Some(slot) = self.take_outstanding(token, answer) else {
            return Answered::UnknownToken;
        };
        self.hold_head_of_line(&slot, owed);
        self.release_and_rearm(&slot);
        Answered::Released
    }

    /// Drop one activation from the jobs-underway count. A count that was at
    /// the cap had pollers parked as if `ready` were empty, so falling below
    /// it is the one event that makes them servable again.
    ///
    /// `None` is a host answering a token twice (or one it never held). It is
    /// reported rather than ignored: the second answer would otherwise release
    /// an entity the first answer already re-armed.
    fn take_outstanding(&self, token: u64, answer: &'static str) -> Option<Arc<EntitySlot>> {
        let Some((_, out)) = self.outstanding.remove(&token) else {
            tracing::warn!(
                target: "flezha::dispatch",
                token,
                answer,
                "handout answered with an unknown or already-answered token"
            );
            return None;
        };
        if self.outstanding_count.fetch_sub(1, Ordering::AcqRel) >= self.max_inflight {
            self.notify_waiters();
        }
        Some(out.slot)
    }

    /// Threads currently inside [`Self::poll`] — the settle a test reads before
    /// arming what a blocked poller must wake for.
    pub fn pollers_inside(&self) -> usize {
        self.pollers_inside.load(Ordering::Acquire)
    }

    /// Work activations handed out and not yet answered.
    pub fn outstanding_count(&self) -> usize {
        self.outstanding_count.load(Ordering::Acquire)
    }

    /// How long the oldest unanswered handout has been out, in milliseconds;
    /// `0` when none is. Reporting only — see [`Self::abandon`] for why the
    /// core never acts on it.
    pub fn outstanding_oldest_age_ms(&self) -> u64 {
        let now = Instant::now();
        self.outstanding
            .iter()
            .map(|e| now.saturating_duration_since(e.handed_at).as_millis() as u64)
            .max()
            .unwrap_or(0)
    }

    /// A retryable failure stopped the batch at `items[0]`: bump its attempt,
    /// hold it and the unprocessed tail at the head of the line, and record
    /// the backoff as a deferred wake. The completing thread returns at once.
    ///
    /// The entity's gates stay held across the window, reproducing today's
    /// in-line sleep exactly: nothing else could drain the entity then
    /// either, producers just enqueued. The promoting poller runs the
    /// release-and-reclaim when the entry comes due.
    pub fn retry_after_failure(
        &self,
        token: u64,
        mut items: Vec<ItemAttempt>,
        error: String,
    ) -> Answered {
        let Some(slot) = self.take_outstanding(token, "retry_after_failure") else {
            return Answered::UnknownToken;
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
            return Answered::Released;
        }
        self.wakes.record(
            WakeKey {
                due_at_ms: crate::util::wall_now_ms().saturating_add(delay.as_millis() as u64),
                tiebreak: WakeTiebreak::Retry(self.next_wake.fetch_add(1, Ordering::Relaxed)),
            },
            Wake::Retry(slot),
        );
        self.notify_waiters();
        Answered::Released
    }

    /// Give up a handout without having processed `items`: park them back at
    /// the head of the entity's line and release it, exactly as if the
    /// activation had never been polled. The third and last way to answer a
    /// handout, beside completing it and failing it.
    ///
    /// No attempt bump and no backoff — these items are not failures. Two
    /// callers, one contract: the tail that sat behind a head the host parked,
    /// and a host whose worker died holding the batch. Because per-entity
    /// exclusivity is the handout itself, an activation that is never answered
    /// retires its entity for the life of the process; abandoning is what a
    /// host's unwind guard calls so that cannot happen.
    ///
    /// Only items the host did NOT process may be passed: re-handing one it
    /// already ran would execute it twice, which is the one thing this change
    /// may not do. For the same reason the core never abandons on its own —
    /// a slow Procedure and a lost handout are identical from here, and a
    /// deadline would re-run the slowest work in the system.
    pub fn abandon(&self, token: u64, items: Vec<ItemAttempt>) -> Answered {
        self.release(token, items, "abandon")
    }

    /// Park re-handed items at the head of the entity's line. Only ever
    /// called while the slot is handed out, which is what the assertion
    /// states; `assemble_batch` drains the holdback first on the next round,
    /// so per-entity FIFO and head-retry ordering are unchanged.
    fn hold_head_of_line(&self, slot: &Arc<EntitySlot>, items: Vec<ItemAttempt>) {
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
        // An Entity owing an undelivered push is handed no new activation: the
        // gates it already holds stay held, exactly as they do across a retry
        // backoff, and nothing else could have drained it then either. The
        // producing thread is long gone — this is what replaces holding it.
        if (slot.push_gated.load(Ordering::Acquire) || slot.push_deferred.load(Ordering::Acquire))
            && self.hold_or_release_pushes(slot)
        {
            return;
        }
        slot.drain_active.store(false, Ordering::Release);
        slot.mailbox.release_inflight();
        if !slot.mailbox.is_empty() {
            if slot.mailbox.claim_inflight() {
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
            && slot.mailbox.claim_inflight()
        {
            self.schedule(slot);
        }
    }

    /// Decide what an Entity's holdback owes its gates. `true` means the gates
    /// stay held — either for the holdback, or because another thread has
    /// taken the claim and is releasing them itself.
    ///
    /// `push_gated` is the single claim on releasing them, and it is published
    /// on EVERY entry, unconditionally, before anything is read. A consumer
    /// freeing room can empty the holdback at any instant — including between
    /// this call's caller testing `push_deferred` and this call re-testing it —
    /// and a claim published only on the deferred branch would leave that
    /// interleaving with a `swap(false)` of `false` meaning "nobody ever
    /// claimed it" rather than "somebody else is releasing". Both sides then
    /// return "held" and the Entity's gates leak: `drain_active` stays true
    /// with no handout outstanding, and every later push to it queues behind a
    /// round that will never run.
    fn hold_or_release_pushes(&self, slot: &Arc<EntitySlot>) -> bool {
        slot.push_gated.store(true, Ordering::Release);
        if slot.push_deferred.load(Ordering::Acquire) {
            self.deferred_pushes
                .entry(Arc::clone(&slot.entity_identity))
                .or_insert_with(|| PendingPushes {
                    since: Instant::now(),
                });
            crate::admission::drain_push_holdback(slot);
            if slot.push_deferred.load(Ordering::Acquire) {
                // The consumer's wait list is what normally brings us back;
                // this only bounds how long a lost wake could hold the Entity.
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
            // Another thread took the claim and is doing the release. Sound
            // only because the store above is unconditional: a `false` here
            // can no longer mean "no claim was ever made".
            return true;
        }
        self.deferred_pushes.remove(&slot.entity_identity);
        false
    }

    /// Re-attempt a producer's held pushes — a consumer freed room, or the
    /// fallback wake came due. A producer still inside its own activation
    /// releases itself at completion, so nothing is released here unless the
    /// gates are already being held FOR the holdback.
    pub(crate) fn rearm_deferred_pushes(&self, slot: &Arc<EntitySlot>) {
        crate::admission::drain_push_holdback(slot);
        if !slot.push_deferred.load(Ordering::Acquire) && slot.push_gated.load(Ordering::Acquire) {
            self.release_and_rearm(slot);
        }
    }

    /// Entities currently holding an undelivered inter-procedure push.
    pub fn pending_push_count(&self) -> usize {
        self.deferred_pushes.len()
    }

    /// How long the oldest held push has gone undelivered, in milliseconds;
    /// `0` when none is.
    pub fn pending_push_oldest_age_ms(&self) -> u64 {
        let now = Instant::now();
        self.deferred_pushes
            .iter()
            .map(|e| now.saturating_duration_since(e.since).as_millis() as u64)
            .max()
            .unwrap_or(0)
    }

    /// Queue a non-work payload (command interaction, workflow step, prepare
    /// crossing) for a host worker, ahead of the ready queue.
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
        ActivationKind::WorkflowRun { .. } => "workflow_run",
        ActivationKind::CompensateRun { .. } => "compensate_run",
    }
}
