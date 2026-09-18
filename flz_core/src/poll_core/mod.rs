//! The poll/complete engine: per-entity activation handout, the deferred-wake
//! list (retry backoffs, processing-time deadlines) and the cap on work
//! underway. The invariant: an entity with an outstanding activation never
//! appears in another handout until that one is answered — by
//! [`PollCore::complete`], [`PollCore::retry_after_failure`] or
//! [`PollCore::abandon`] — and the core never reclaims a handout on a timer.
//! The core runs no host code and blocks no host thread: hosts block in
//! [`PollCore::poll`] and answer with completions; a `DropNothing` push a
//! producing entity cannot deliver is held in its holdback and the entity is
//! handed nothing until it lands. An event-time class is handed ONE input per
//! activation so a correction replay folds firings where the live run did.

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
use crate::error::Error;
use crate::util::LockRecover;

/// One item on its ladder, as the engine hands it out. Cloning is shallow.
#[derive(Clone)]
pub struct ItemAttempt {
    pub item: EntityItem,
    /// Core-evaluated `now` (an event-time input's event time, a firing's
    /// scheduled time); `None` when the typed layer evaluates its own clock.
    pub now: Option<u64>,
    pub attempt: u32,
    /// A failing final attempt parks instead of returning `RetryLater`.
    pub is_final: bool,
}

/// One handed-out unit of work; for a [`EnginePayload::Work`] batch, holding
/// it IS the entity's exclusivity until the token is answered.
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
    /// A command interaction crossed to a C-ABI host. Its exclusivity is the
    /// enqueuer's gate ([`PollCore::with_entity_exclusive`]); its completion
    /// answers that rendezvous, never releases a slot.
    Command {
        target: crate::schema::EntityRef,
        change_seq: u64,
        factory: String,
        kind: crate::schema::ActivationKind,
        reply: crossbeam::channel::Sender<crate::schema::ActivationOutcome>,
    },
    /// One entity's share of a workflow stage or undo; its exclusivity is the
    /// driver's stage serialization, its completion answers the barrier.
    WorkflowStep(crate::workflow::step::WorkflowStep),
    /// A batched prepare crossing for one entry channel, bound to no entity.
    Prepare(crate::host::PrepareCrossing),
}

impl EngineActivation {
    /// The work batch's slot; only a work activation is bound to one.
    pub fn slot(&self) -> &Arc<EntitySlot> {
        match &self.payload {
            EnginePayload::Work { slot, .. } => slot,
            _ => unreachable!("only work activations are bound to a live slot"),
        }
    }

    pub fn items(&self) -> &[ItemAttempt] {
        match &self.payload {
            EnginePayload::Work { items, .. } => items,
            _ => &[],
        }
    }

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

/// What answering a handout did; an unknown token is a host answering twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answered {
    Released,
    UnknownToken,
}

/// What [`PollCore::wait_starvation`] answers, shaped like [`Polled`] so a
/// supervisor loop reads like a worker loop.
pub enum Starvation {
    /// Runnable work with nobody to run it: the host owes a thread.
    Observed,
    Timeout,
    ShuttingDown,
}

struct Outstanding {
    slot: Arc<EntitySlot>,
    handed_at: Instant,
}

struct PendingPushes {
    since: Instant,
}

pub struct PollCore {
    ready: Mutex<VecDeque<Arc<EntitySlot>>>,
    /// Command and workflow-step activations, ahead of `ready`; the enqueuer
    /// holds their entity gate, so they never race a work assembly.
    direct: Mutex<VecDeque<EngineActivation>>,
    cv: Condvar,
    shutting_down: AtomicBool,
    next_token: AtomicU64,
    /// Live handouts by token, with when each went out — reported, never
    /// acted on.
    outstanding: DashMap<u64, Outstanding>,
    /// Entities gated on an undelivered push, by identity; reported beside
    /// `outstanding`, which cannot see them (a gated producer holds no
    /// handout).
    deferred_pushes: DashMap<Arc<Vec<u8>>, PendingPushes>,
    wakes: DeferredWake,
    next_wake: AtomicU64,
    /// `outstanding.len()` without locking every shard on the handout path.
    outstanding_count: AtomicUsize,
    /// Cap on work activations underway, enforced at handout.
    max_inflight: usize,
    /// Pollers inside [`Self::poll`], mid-assembly included: one is about to
    /// take the queued work, so it must not read as starvation.
    pollers_inside: AtomicUsize,
    /// [`Self::poll`] entries: what separates busy (threads cycling through
    /// `poll`) from stuck (every thread inside user code).
    poll_entries: AtomicU64,
    /// Where [`Self::wait_starvation`] parks, under `ready`'s mutex. Never
    /// signalled from the handout path: the waiter re-evaluates on its own
    /// [`starvation::STARVATION_DWELL`] tick.
    starve_cv: Condvar,
}

static GLOBAL: OnceLock<PollCore> = OnceLock::new();

pub fn global() -> &'static PollCore {
    GLOBAL.get_or_init(PollCore::new)
}

/// The `FLEZHA_MAX_INFLIGHT_ACTIVATIONS` default: above any healthy blocking
/// workload, low enough to bound the memory that many batches hold.
const MAX_INFLIGHT_DEFAULT: usize = 1024;
const ENV_MAX_INFLIGHT: &str = "FLEZHA_MAX_INFLIGHT_ACTIVATIONS";

/// The lost-wake fallback for a gated producer (the consumer's wait list is
/// the primary re-arm), mirroring the entry path's `ROOM_WAIT_TIMEOUT`.
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
    /// The caller holds the round's inflight token (a winning `claim_inflight`).
    pub fn schedule(&self, slot: &Arc<EntitySlot>) {
        self.ready.lock_recover().push_back(Arc::clone(slot));
        self.cv.notify_one();
    }

    /// Block until an activation is available, the timeout elapses, or
    /// shutdown wakes every poller. Also the deferred-wake list's only
    /// service: every pass promotes what came due.
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

    /// Become the slot's active drainer and assemble its batch; `None` when
    /// another thread owns the round or nothing is runnable.
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

    /// Release the entity and, if work remains, schedule the next round; any
    /// host thread may answer, not only the one that polled.
    pub fn complete(&self, token: u64) -> Answered {
        self.release(token, Vec::new(), "complete")
    }

    /// The one body behind [`Self::complete`] and [`Self::abandon`]: take the
    /// handout, hold anything owed at the head of the line, release the gates.
    fn release(&self, token: u64, owed: Vec<ItemAttempt>, answer: &'static str) -> Answered {
        let Some(slot) = self.take_outstanding(token, answer) else {
            return Answered::UnknownToken;
        };
        self.hold_head_of_line(&slot, owed);
        self.release_and_rearm(&slot);
        Answered::Released
    }

    /// Drop one activation from the count; at the cap that is what wakes the
    /// parked pollers. `None` is a double answer, reported rather than ignored
    /// because the second answer would release an entity the first re-armed.
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

    /// Threads inside [`Self::poll`] — a test's settle before arming a wake.
    pub fn pollers_inside(&self) -> usize {
        self.pollers_inside.load(Ordering::Acquire)
    }

    pub fn outstanding_count(&self) -> usize {
        self.outstanding_count.load(Ordering::Acquire)
    }

    /// Milliseconds the oldest unanswered handout has been out; `0` when none.
    pub fn outstanding_oldest_age_ms(&self) -> u64 {
        let now = Instant::now();
        self.outstanding
            .iter()
            .map(|e| now.saturating_duration_since(e.handed_at).as_millis() as u64)
            .max()
            .unwrap_or(0)
    }

    /// A retryable failure stopped the batch at `items[0]`: bump its attempt,
    /// hold it and the tail at the head of the line, and defer the backoff as
    /// a wake. The gates stay held across the window; the promoting poller
    /// releases them when it comes due.
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

    /// Give up a handout without processing `items`: hold them at the head of
    /// the line and release the entity as if never polled — no attempt bump,
    /// no backoff. Only items the host did NOT run may be passed (re-handing
    /// one it ran executes it twice), which is also why the core never
    /// abandons on its own: from here a slow Procedure and a lost handout are
    /// identical.
    pub fn abandon(&self, token: u64, items: Vec<ItemAttempt>) -> Answered {
        self.release(token, items, "abandon")
    }

    /// Hold re-handed items at the head of the entity's line; only while the
    /// slot is handed out, which the assertion states.
    fn hold_head_of_line(&self, slot: &Arc<EntitySlot>, items: Vec<ItemAttempt>) {
        if items.is_empty() {
            return;
        }
        let mut held = slot.pending_retry.lock_recover();
        debug_assert!(held.is_empty(), "holdback refilled while handed out");
        held.extend(items);
    }

    /// Release the gates and claim the round anything left owes.
    /// `drain_active` drops BEFORE inflight so a producer observing
    /// inflight=false can become the next drainer at once.
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

    /// What an Entity's holdback owes its gates: `true` keeps them held, for
    /// the holdback or because another thread took the claim. `push_gated` is
    /// published on EVERY entry before anything is read; a claim published
    /// only on the deferred branch would let a consumer racing the two reads
    /// leave both sides answering "held" and the gates leaked.
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
            // cannot mean "no claim was ever made".
            return true;
        }
        self.deferred_pushes.remove(&slot.entity_identity);
        false
    }

    /// Re-attempt a producer's held pushes (a consumer freed room, or the
    /// fallback wake came due); releases the gates only when they are held
    /// FOR the holdback.
    pub(crate) fn rearm_deferred_pushes(&self, slot: &Arc<EntitySlot>) {
        crate::admission::drain_push_holdback(slot);
        if !slot.push_deferred.load(Ordering::Acquire) && slot.push_gated.load(Ordering::Acquire) {
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

    /// Queue a non-work payload for a host worker, ahead of the ready queue.
    pub fn submit_direct(&self, payload: EnginePayload) -> u64 {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed) + 1;
        self.direct
            .lock_recover()
            .push_back(EngineActivation { token, payload });
        self.cv.notify_one();
        token
    }

    /// Cross a command interaction to a host poller and block for its answer.
    /// MUST run inside [`Self::with_entity_exclusive`]: that gate keeps input
    /// activations out while the command runs.
    pub fn host_command(
        &self,
        slot: &Arc<EntitySlot>,
        kind: crate::schema::ActivationKind,
    ) -> Result<crate::schema::ActivationOutcome, Error> {
        self.host_command_within(slot, kind, HOST_COMMAND_TIMEOUT)
    }

    /// [`Self::host_command`] with the wait chosen by the caller.
    pub fn host_command_within(
        &self,
        slot: &Arc<EntitySlot>,
        kind: crate::schema::ActivationKind,
        wait: Duration,
    ) -> Result<crate::schema::ActivationOutcome, Error> {
        let describe = command_kind_name(&kind);
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(Error::HostCrossing(format!(
                "{describe}: runner is shutting down"
            )));
        }
        let (reply, rx) = crossbeam::channel::bounded(1);
        self.submit_direct(EnginePayload::Command {
            target: crate::host::entity_ref_of(slot),
            change_seq: slot.change_seq.load(Ordering::Acquire),
            factory: slot.topology.factory_id.to_string(),
            kind,
            reply,
        });
        match rx.recv_timeout(wait) {
            Ok(outcome) => Ok(outcome),
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                Err(Error::HostCrossing(format!(
                    "{describe}: host did not complete the activation within {}s",
                    wait.as_secs()
                )))
            }
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => Err(Error::HostCrossing(
                format!("{describe}: host context dropped the activation"),
            )),
        }
    }

    /// Run `f` under the entity's handout exclusivity: it waits out any
    /// outstanding activation and none is handed out while it runs.
    pub fn with_entity_exclusive<R>(&self, slot: &Arc<EntitySlot>, f: impl FnOnce() -> R) -> R {
        self.try_with_entity_exclusive(slot, Duration::MAX, f)
            .expect("a wait of Duration::MAX never elapses")
    }

    /// [`Self::with_entity_exclusive`] with a bound: `None` when the
    /// outstanding activation is still running at `wait`, and the gate — not
    /// won — is left to whoever holds it.
    pub fn try_with_entity_exclusive<R>(
        &self,
        slot: &Arc<EntitySlot>,
        wait: Duration,
        f: impl FnOnce() -> R,
    ) -> Option<R> {
        let started = Instant::now();
        let mut warned = false;
        while slot.drain_active.swap(true, Ordering::AcqRel) {
            if started.elapsed() >= wait {
                return None;
            }
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
        Some(result)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        self.cv.notify_all();
        self.starve_cv.notify_all();
    }
}

/// How long a command thread waits for the host's answer.
pub(crate) const HOST_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

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
        ActivationKind::CompensateRun { .. } => "compensate_run",
    }
}
