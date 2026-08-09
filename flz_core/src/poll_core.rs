//! The poll/complete work engine: per-entity activation handout.
//!
//! The single-active-drainer invariant, expressed at a boundary: an entity
//! with an outstanding (handed-out, uncompleted) activation never appears in
//! another handout until [`PollCore::complete`] releases it. The engine owns
//! everything that used to run inside the drain loop except user code:
//! consumed-floor skip, replay-slice buffering and event-time ordering,
//! late-input admission and classification, event-time due-timer generation,
//! stale-timer validation, and the retry→park ladder's counting/backoff/
//! re-hand. The host half (`operative::dispatcher::serve_slot`) runs one
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
//! (`operative::dispatcher::ensure_poll_workers`), or a C-ABI host's own —
//! block in [`PollCore::poll`] and answer with completions. A host may also
//! serve one slot inline (`try_assemble` + complete), which is how a
//! Block-policy producer drains a full consumer on its own thread.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;

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
}

static GLOBAL: OnceLock<PollCore> = OnceLock::new();

pub fn global() -> &'static PollCore {
    GLOBAL.get_or_init(PollCore::new)
}

impl PollCore {
    pub fn new() -> Self {
        Self {
            ready: Mutex::new(VecDeque::new()),
            direct: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            shutting_down: AtomicBool::new(false),
            next_token: AtomicU64::new(0),
            outstanding: DashMap::new(),
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
    pub fn poll(&self, timeout: Duration) -> Polled {
        let deadline = Instant::now() + timeout;
        let mut ready = self.ready.lock_recover();
        loop {
            if self.shutting_down.load(Ordering::Acquire) {
                return Polled::ShuttingDown;
            }
            if let Some(act) = self.direct.lock_recover().pop_front() {
                return Polled::Activation(act);
            }
            while let Some(slot) = ready.pop_front() {
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
            ready = match self.cv.wait_timeout(ready, deadline - now) {
                Ok((guard, _)) => guard,
                Err(poisoned) => poisoned.into_inner().0,
            };
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
                self.outstanding.insert(token, Arc::clone(slot));
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
        let Some((_, slot)) = self.outstanding.remove(&token) else {
            return;
        };
        self.release_and_rearm(&slot);
    }

    /// A retryable failure stopped the batch at `items[0]`: back off, bump
    /// its attempt, hold the unprocessed tail at the head of the line, and
    /// schedule the re-hand. Runs on the completing thread — the backoff
    /// blocks it exactly as the in-dispatch ladder used to block the drain.
    pub fn retry_after_failure(&self, token: u64, mut items: Vec<WorkItem>, error: String) {
        let Some((_, slot)) = self.outstanding.remove(&token) else {
            return;
        };
        if let Some(head) = items.first_mut() {
            let policy = slot.dispatcher.retry_policy();
            let delay = crate::retry::note_failed_attempt(
                &format!("procedure {}", slot.dispatcher.procedure_name()),
                head.attempt,
                policy,
                &error,
            );
            crate::util::sleep_interruptible(delay, None);
            head.attempt += 1;
            head.is_final = head.attempt >= policy.max_attempts;
        }
        self.hold_and_reschedule(&slot, items);
    }

    /// Put back items the host never processed — they sat behind a failed
    /// head that parked. No attempt bump and no backoff: these items are not
    /// failures, they are simply owed a fresh round.
    pub fn rehand_unprocessed(&self, token: u64, items: Vec<WorkItem>) {
        let Some((_, slot)) = self.outstanding.remove(&token) else {
            return;
        };
        if items.is_empty() {
            self.release_and_rearm(&slot);
            return;
        }
        self.hold_and_reschedule(&slot, items);
    }

    /// The holdback tail both re-hand flavors share: park the items in the
    /// slot's holdback, release the gates, and claim the owed round. The
    /// holdback is non-empty, so a round is owed regardless of the mailbox;
    /// a losing CAS means a producer already scheduled one.
    fn hold_and_reschedule(&self, slot: &Arc<EntitySlot>, items: Vec<WorkItem>) {
        {
            let mut held = slot.pending_retry.lock_recover();
            debug_assert!(held.is_empty(), "holdback refilled while handed out");
            held.extend(items);
        }
        slot.drain_active.store(false, Ordering::Release);
        slot.mailbox.release_inflight();
        if slot.mailbox.try_mark_inflight() {
            self.schedule(slot);
        }
    }

    fn release_and_rearm(&self, slot: &Arc<EntitySlot>) {
        slot.drain_active.store(false, Ordering::Release);
        slot.mailbox.release_inflight();
        if !slot.mailbox.is_empty() {
            if slot.mailbox.try_mark_inflight() {
                self.schedule(slot);
            }
            return;
        }
        // The mailbox is dry, but the completed batch may leave work only
        // assembly can see: a chained event-time registration already behind
        // the high-water, or the still-buffered remainder of a closed replay
        // slice (drained one member per round for an event-time class).
        if (event_time_due_pending(slot)
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
            // The wheel holds no lock on the pending set, so an entry can
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
