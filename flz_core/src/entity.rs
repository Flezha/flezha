//! `EntitySlot`: per-(procedure, entity) state + bounded mailbox +
//! dispatcher + producer-block parker + per-Entity counters.
//!
//! The inflight CAS on `Mailbox` guarantees that at most one drain task
//! is active per slot; under that exclusion the typed dispatcher
//! downcasts state and runs `Procedure::run`. The mailbox is bounded
//! (`crossbeam::queue::ArrayQueue`); when the channel's declared
//! `OverflowPolicy` is `DropNothing`, a producer that cannot deliver waits on
//! the slot's [`BlockParker`] — the entry-path transport reader by parking its
//! own thread, a producing Entity by holding the push in its
//! [`EntitySlot::push_holdback`] and standing down until room appears. The
//! drain loop serves one waiter per popped item, whichever kind it is.
//! Per-Entity push / drop / block counters live on
//! [`EntityCounters`]; the producer accounts for an overflow through
//! [`EntitySlot::note_blocked`] / [`EntitySlot::note_shed`], which move the
//! per-Entity counter and the process-wide aggregate the heartbeat reports
//! together so the two cannot drift.
//!
//! State persistence (`operative-state-persistence`):
//!
//! Each `EntitySlot` whose Procedure's `Change` type is not `NoChange`
//! also carries a type-erased flush buffer (`Arc<Mutex<Box<dyn FlushableBuffer>>>`),
//! a per-instance `persistence_enabled: bool` flag, and a `change_seq:
//! AtomicU64` counter. The per-process [`flusher::EntityFlusher`]
//! drains all registered buffers on its linger cadence.

use std::any::Any;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crossbeam::sync::{Parker, Unparker};
use flz_transport::transport::FlezhaStream;

use crate::channel::{OverflowPolicy, ProcedureId};
pub use crate::header::RestoreError;
use crate::mailbox::Mailbox;
pub use crate::restore::RestoreInitPayload;
use crate::util::LockRecover;

/// Erased payload moving through the inter-procedure mailbox. The `value`
/// is `Arc<dyn Any + Send + Sync>` (the consumer downcasts to its own
/// `S::Input`); the `stream` is the secondary FlezhaStream of the
/// connection that the original wire input arrived on, propagated
/// downstream so any plugin-bound emit lands on the same connection.
#[derive(Clone)]
pub struct EntityPayload {
    pub value: Arc<dyn Any + Send + Sync>,
    pub stream: Arc<FlezhaStream>,
    /// Durable input-log offset this payload derives from (`flz.offset` on
    /// the inbound frame, inherited across inter-procedure emits). `None`
    /// outside the effectively-once tier; the invocation's atomic effect
    /// record carries it as the consumed-offset.
    pub consumed_offset: Option<u64>,
    /// Set when the controller re-forwarded this payload as part of a replayed
    /// slice (`flz.replay` on the inbound frame). The drain holds such payloads
    /// until the slice closes, orders them by event time, and only then
    /// dispatches — an arrival-ordered replay would recompute the same wrong
    /// answer the correction exists to repair.
    ///
    /// Never inherited by an inter-procedure emit: a downstream Procedure
    /// receives the corrected outputs already in order, and a slice it could
    /// never see the end of would hold its drain forever.
    pub replay: bool,
}

/// Construct a [`EntityPayload`], cloning both shared handles. Shared by
/// the inter-procedure emit path and the inbound wire-dispatch path.
pub fn make_entity_payload(
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
) -> EntityPayload {
    EntityPayload {
        value: Arc::clone(value),
        stream: Arc::clone(stream),
        consumed_offset,
        replay,
    }
}

/// One item in an Entity's mailbox. A timer firing is queued here rather than
/// invoked where it expires, so the single-active-drainer gate that already
/// serializes inputs serializes `on_timer` against `run` too — no second lock,
/// no second concurrency model.
///
/// Cloning is shallow (`Arc` bumps): the engine's retry ladder hands a clone
/// to each non-final attempt so the original survives for the re-hand.
#[derive(Clone)]
pub enum EntityItem {
    Input(EntityPayload),
    Timer(crate::timer::TimerFire),
    /// An open checkpoint interval has reached its `max_delay_ms` and must
    /// close. It rides the mailbox rather than firing on the wake list's
    /// thread so the close is serialized with the Entity's own invocations —
    /// and it runs NO author code: the interval's accumulation is already
    /// computed, the close only ships it.
    CloseInterval,
}

pub type EntityMailbox = Arc<Mailbox<EntityItem>>;

/// Producer-side observability for one Entity. All counters are monotonic and
/// written by producers under [`std::sync::atomic::Ordering::Relaxed`].
///
/// Per-Entity breakdown, read by tests and by anything holding a slot. What
/// leaves the process is the process-wide aggregate beside it
/// ([`PUSHES_BLOCKED_TOTAL`], [`DROPS_NEWEST_TOTAL`], [`DROPS_OLDEST_TOTAL`]),
/// which the data heartbeat carries: a per-Entity breakdown on that path would
/// need a registry walk per heartbeat.
#[derive(Debug, Default)]
pub struct EntityCounters {
    /// Number of payloads successfully enqueued onto this Entity's
    /// mailbox.
    pub pushes_total: AtomicU64,
    /// Number of times a `DropNothing`-policy producer parked waiting for the
    /// consumer to free a slot. Multiple parks per push (under
    /// multi-producer contention) each increment.
    pub pushes_blocked_total: AtomicU64,
    /// Number of incoming payloads discarded under `DropNewest`.
    pub drops_newest_total: AtomicU64,
    /// Number of head-of-queue payloads evicted by `DropOldest`'s
    /// `force_push`.
    pub drops_oldest_total: AtomicU64,
}

/// Multi-waiter, multi-unparker FIFO parker used by `DropNothing`-policy
/// producers when the consumer's mailbox is saturated.
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
pub struct BlockParker {
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

impl BlockParker {
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
    /// `enqueue` loop will retry `try_push`. Without this recheck, a producer
    /// that loses that race parks forever.
    ///
    /// **The entry is removed on the way out, on every exit path.** An entry
    /// whose thread has moved on still consumes an `unpark_one` the next live
    /// waiter was owed, and each consumed wake makes that waiter's next
    /// timeout more likely — a deficit that only ever grows, until every wake
    /// is absorbed by the dead and a live producer advances one 50 ms park at
    /// a time.
    pub fn wait_one_until<P: Send + 'static>(
        &self,
        mailbox: &Mailbox<P>,
        timeout: Option<Duration>,
    ) {
        let parker = Parker::new();
        let ticket = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        {
            let mut deque = self.waiters.lock().expect("BlockParker mutex poisoned");
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
        let mut deque = self.waiters.lock().expect("BlockParker mutex poisoned");
        if let Some(at) = deque
            .iter()
            .position(|w| matches!(w, Waiter::Thread { ticket: t, .. } if *t == ticket))
        {
            deque.remove(at);
        }
    }

    /// Register on this slot's wait list and park until a pop frees a slot.
    pub fn wait_one<P: Send + 'static>(&self, mailbox: &Mailbox<P>) {
        self.wait_one_until(mailbox, None);
    }

    /// [`wait_one`] with a bounded park. Used by `DropNothing`-policy producers
    /// so that even an unforeseen lost-wake degrades to a retry loop with
    /// warnings instead of a permanent silent freeze.
    pub fn wait_one_timeout<P: Send + 'static>(&self, mailbox: &Mailbox<P>, timeout: Duration) {
        self.wait_one_until(mailbox, Some(timeout));
    }

    /// Register a producing Entity as owed the next free slot on this
    /// consumer. Unlike [`Self::wait_one`] it holds no thread: the producer
    /// returns and its holdback is drained when the room arrives.
    ///
    /// At most one entry per producer, because a producer re-registers after
    /// every failed drain and duplicates would absorb wakes a parked thread is
    /// owed. The caller re-checks `is_full()` after registering for the same
    /// reason a parking waiter does — a drain that emptied the mailbox between
    /// the failed admission and this registration would otherwise never wake
    /// it. A registration left behind by a producer that drained on its own is
    /// inert: re-arming an un-gated Entity is a no-op.
    pub fn wait_producer(&self, producer: &Arc<EntitySlot>) {
        let mut deque = self.waiters.lock().expect("BlockParker mutex poisoned");
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
    pub fn unpark_one(&self) {
        loop {
            let waiter = {
                let mut deque = self.waiters.lock().expect("BlockParker mutex poisoned");
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

impl Default for BlockParker {
    fn default() -> Self {
        Self::new()
    }
}

/// Process-global count of payloads dispatched through `drain_entity`.
/// Shipped in every `data_heartbeat` frame's metadata so the controller can
/// tell "runner alive but dispatch wedged" apart from "runner healthy" —
/// the 2026-06-10 freeze was invisible to every heartbeat-level signal.
pub static DISPATCHES_COMPLETED: AtomicU64 = AtomicU64::new(0);

/// Process-global count of DropNothing-policy wait iterations (a producer
/// found a full mailbox and went round again). Shipped in heartbeat metadata
/// as `blocked_parks`.
///
/// Per ITERATION, not per contiguous episode: a runner held in one long
/// episode would otherwise show a frozen dispatch count beside a frozen block
/// count — byte for byte the picture a wedged runner shows. `credit.rs`'s
/// `CREDIT_PARKED` advances per wait iteration for the same reason, and the
/// controller's stall classifier tells backpressure from a wedge by reading
/// the two together.
pub static PUSHES_BLOCKED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Process-global count of payloads discarded under `DropNewest`.
pub static DROPS_NEWEST_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Process-global count of queue heads evicted under `DropOldest`.
pub static DROPS_OLDEST_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Process-global count of queued inputs a restore discarded. Also shipped in
/// heartbeat metadata: a discard is correct only where the controller
/// re-forwards from a durable log, so a climbing value is the operator's one
/// handle on how much a rebind threw away.
pub static RESTORE_DISCARDED_INPUTS: AtomicU64 = AtomicU64::new(0);

pub fn dispatches_completed() -> u64 {
    DISPATCHES_COMPLETED.load(Ordering::Relaxed)
}

pub fn pushes_blocked_total() -> u64 {
    PUSHES_BLOCKED_TOTAL.load(Ordering::Relaxed)
}

pub fn drops_newest_total() -> u64 {
    DROPS_NEWEST_TOTAL.load(Ordering::Relaxed)
}

pub fn drops_oldest_total() -> u64 {
    DROPS_OLDEST_TOTAL.load(Ordering::Relaxed)
}

pub fn restore_discarded_inputs() -> u64 {
    RESTORE_DISCARDED_INPUTS.load(Ordering::Relaxed)
}

pub struct EntitySlot {
    pub state: Mutex<Box<dyn Any + Send>>,
    pub mailbox: EntityMailbox,
    pub dispatcher: Arc<dyn ProcedureDispatcher>,
    pub procedure_id: ProcedureId,
    pub entity_key: Option<Arc<str>>,
    pub instance: Weak<crate::operative::instance::StreamInstance>,
    pub topology: Arc<crate::operative::topology::OperativeTopology>,
    /// FIFO wait list for whoever is owed this mailbox's next free slot under
    /// `DropNothing` — a parked entry-path thread, or a producing Entity
    /// holding an undelivered push. Served one entry at a time after every
    /// successful pop.
    pub block_parker: BlockParker,
    /// Single-ACTIVE-drainer gate. `mailbox`'s inflight flag means "a round is
    /// scheduled or running" (it is held from the moment the slot is queued on
    /// the poll core, before any worker takes it); this flag is true only while
    /// a thread holds the handed-out activation — or while the Entity is gated
    /// on its own push holdback, which is the same exclusion for the same
    /// reason a retry backoff holds it.
    pub drain_active: AtomicBool,
    /// Per-Entity flush buffer holding `Vec<S::Change>` behind the
    /// type-erased `FlushableBuffer` trait object. The dispatcher locks
    /// this Mutex for the duration of `Procedure::run` and constructs a
    /// `StateHandle` from the borrowed Vec inside; the per-process
    /// flusher locks it on each linger tick to drain.
    pub flush_buffer: Arc<Mutex<Box<dyn crate::flusher::FlushableBuffer>>>,
    /// Frozen-at-instance-start persistence flag. Resolved by
    /// `operative::persistence::resolve_persistence_enabled` from the
    /// runner override + StreamOperative class const at `StreamInstance::new`
    /// time and propagated here.
    pub persistence_enabled: bool,
    /// Per-Entity WAL seq counter. The flusher fetches-and-adds once
    /// per drained record; the seq is baked into the frame before it is
    /// handed to the sink, so a frame buffered during a reconnect replays
    /// with its original seq — the controller's WAL view stays gap-free.
    pub change_seq: Arc<std::sync::atomic::AtomicU64>,
    /// Effectively-once tier: highest input-log offset this Entity
    /// durably consumed before a restore (max `consumed_offset` across the
    /// restored effect records; `-1` = none). `drain_entity` skips
    /// re-forwarded inputs at or below it so a crash-replay never
    /// double-applies. `None`-offset payloads (every non-tier input) pay
    /// one branch.
    pub consumed_floor: std::sync::atomic::AtomicI64,
    /// Late-input correction: the highest event time this Entity has
    /// dispatched (`-1` = none yet). Only an `EventTimeFrom` clock advances
    /// it. An input behind it is *late*; how far behind decides whether it
    /// is correctable or falls to the beyond-window policy. Rebuilt on
    /// restore from the snapshot header so a resumed Entity does not
    /// mistake its whole history for a flood of late inputs. Shared with the
    /// dispatcher, which is the only place the extractor can run.
    pub event_time_high_water: Arc<AtomicI64>,
    /// Late-input correction: payloads of a replayed slice (`flz.replay`),
    /// held so the drain can order them by event time before dispatching any.
    /// Empty whenever no replay is in flight, which is the overwhelmingly
    /// common case — a normal drain pays one `is_empty` check.
    pub replay_buffer: Mutex<Vec<EntityPayload>>,
    /// True from the moment a replayed payload is enqueued until the
    /// controller's end-of-slice broadcast closes the slice. The drain flushes
    /// the buffer only once it is false, so a mailbox that momentarily runs dry
    /// mid-slice cannot dispatch half an ordering.
    ///
    /// It is set on the frame-handling thread (in `enqueue`) and cleared by the
    /// broadcast, which arrives on that same thread after every slice frame —
    /// so the close can never overtake a member it was meant to include.
    pub replay_open: AtomicBool,
    /// Number of payloads currently held in `replay_buffer`. A plain counter so
    /// the drain's hot path can ask "is a slice pending?" with one relaxed load
    /// instead of taking the buffer's mutex on every payload.
    pub replay_depth: AtomicU64,
    /// Stable per-Entity identity (`(operative_class, procedure_id, entity_key)`)
    /// encoded for the wire by `flusher::encode_entity_identity`. The
    /// flusher reads this verbatim into each frame's
    /// `entity_id_bytes` field; the controller decodes the same structure
    /// to route the frame to the right per-Entity WAL.
    pub entity_identity: Arc<Vec<u8>>,
    /// Monotonic millis-since-process-start of the last successful push
    /// (`dynamic::now_millis`). Read by the dynamic-mode reaper to decide
    /// idle eviction; inert for single- and static-mode Entities.
    pub last_activity_at: AtomicI64,
    /// Flusher registration id, captured when this Entity's flush
    /// buffer was registered (`Some` iff `Change != NoChange`). The
    /// dynamic-mode reaper passes it to `EntityFlusher::deregister` on
    /// eviction so an evicted Entity's buffer is no longer drained.
    pub flusher_entry_id: Option<crate::flusher::FlusherEntryId>,
    /// Pending timers, folded from the `TimerOp` records in this Entity's own
    /// change log. Held beside the state and taken under the same lock order
    /// (state → flush buffer → timers) so a firing observes exactly the set the
    /// invocation that registered it left behind.
    pub timers: Mutex<crate::timer::PendingTimers>,
    /// Head-of-line items the engine re-hands before the mailbox: a retrying
    /// item (with its attempt count) and the unprocessed tail of a batch that
    /// stopped at a retryable failure. Only touched under the drain gates, so
    /// the lock is never contended.
    pub pending_retry: Mutex<VecDeque<crate::poll_core::WorkItem>>,
    /// Live items popped behind a closed replay slice that is draining
    /// member-per-round: they run after the remainder, ahead of the mailbox.
    /// Only touched under the drain gates.
    pub deferred_live: Mutex<VecDeque<EntityItem>>,
    /// Inter-procedure pushes this Entity produced and could not deliver, in
    /// emit order. Backpressure withholds work from the producer instead of
    /// holding its caller: the push returns at once, the Entity's handout
    /// gates stay held until the list drains, and the consumer's next free
    /// slot drains it ([`crate::routing::admit_from_entity`]).
    pub push_holdback: Mutex<VecDeque<PendingPush>>,
    /// Whether [`Self::push_holdback`] is non-empty. Read on EVERY push,
    /// including the overwhelming majority that succeed, so it is an atomic
    /// load and never a peek under the holdback's lock: measured, the load is
    /// free (14.39 ns against 14.79 ns for a bare `try_push`) and a mutex
    /// acquisition costs +64%. Written only while that lock is held, which is
    /// what makes "observed set ⇒ append behind what is already held" total.
    ///
    /// Shared with this Entity's flusher entry: a durable record must not
    /// ship while a push derived from the same input is still undelivered.
    pub push_deferred: Arc<AtomicBool>,
    /// Whether this Entity's gates are being held FOR the holdback (rather
    /// than by a live activation). Exactly one thread may release them, and
    /// this is the claim: the deferral sets it at completion, and whoever
    /// drains the holdback to empty takes it.
    pub push_gated: AtomicBool,
}

/// One inter-procedure push withheld from a full consumer. Shallow — the item
/// already holds its value behind an `Arc`, and the consumer is the slot the
/// push was routed to.
pub struct PendingPush {
    pub consumer: Arc<EntitySlot>,
    pub item: EntityItem,
}

impl EntitySlot {
    /// Convenience accessor for the per-Entity counter set: four `Relaxed`
    /// loads, no internal-state poke. The heartbeat reports the process-wide
    /// aggregates instead; this is the per-Entity view for a holder of the
    /// slot.
    pub fn counters(&self) -> &EntityCounters {
        self.mailbox.counters()
    }

    /// Account for one `DropNothing` wait iteration. The per-Entity counter
    /// and the process-wide aggregate move together here so they cannot drift:
    /// the breakdown is read from a slot, the aggregate is what leaves on the
    /// heartbeat, and both describe the same event.
    pub fn note_blocked(&self) {
        self.counters()
            .pushes_blocked_total
            .fetch_add(1, Ordering::Relaxed);
        PUSHES_BLOCKED_TOTAL.fetch_add(1, Ordering::Relaxed);
    }

    /// Account for one item a shedding policy discarded: both counters, and
    /// the identity of what was lost. This is the last place that identity
    /// exists — `force_push` hands the evicted item back and nothing
    /// downstream ever sees it — so the counters say how much and this line
    /// says which. `debug`, because a shedding policy sheds at input rates:
    /// the operator's signal is the counter on the heartbeat.
    ///
    /// `evicted` is `None` only for a `DropOldest` force-push that found the
    /// queue already drained under contention: the policy applied, so the
    /// count moves, but there is no identity to name.
    pub fn note_shed(&self, policy: OverflowPolicy, evicted: Option<&EntityItem>) {
        let policy = match policy {
            OverflowPolicy::DropNewest => {
                self.counters()
                    .drops_newest_total
                    .fetch_add(1, Ordering::Relaxed);
                DROPS_NEWEST_TOTAL.fetch_add(1, Ordering::Relaxed);
                "drop_newest"
            }
            OverflowPolicy::DropOldest => {
                self.counters()
                    .drops_oldest_total
                    .fetch_add(1, Ordering::Relaxed);
                DROPS_OLDEST_TOTAL.fetch_add(1, Ordering::Relaxed);
                "drop_oldest"
            }
            // The policy that discards nothing sheds nothing.
            OverflowPolicy::DropNothing => return,
        };
        let Some(item) = evicted else { return };
        tracing::debug!(
            target: "flezha::mailbox",
            policy,
            procedure_id = self.procedure_id,
            entity_key = ?self.entity_key,
            item = %match item {
                EntityItem::Input(p) => match p.consumed_offset {
                    Some(offset) => format!("input at offset {offset}"),
                    None => "input".to_string(),
                },
                EntityItem::Timer(fire) => {
                    format!("{:?} timer for {}", fire.kind, fire.scheduled_at)
                }
                EntityItem::CloseInterval => "checkpoint interval close".to_string(),
            },
            "shed an item from a full entity mailbox"
        );
    }

    /// Drop an open checkpoint interval's staged changes. A restore re-seats
    /// the Entity on a past in which they were never applied, and the
    /// controller re-forwards the inputs that produced them.
    pub fn discard_staged_interval(&self) {
        self.flush_buffer.lock_recover().discard_staged();
    }

    /// Drop a half-filled correction slice: it belongs to a past the restore
    /// replaced, and releasing it later would order its inputs behind the very
    /// replay meant to re-order them. Unlike the mailbox this applies to every
    /// class — a slice only ever exists for one the controller re-forwards.
    pub fn discard_replay_slice(&self) {
        self.replay_buffer.lock_recover().clear();
        self.replay_depth.store(0, Ordering::Relaxed);
        self.replay_open.store(false, Ordering::Release);
    }

    /// Restore-time disposition of queued input: drop it only where the
    /// controller will hand it back. It journals inputs before forwarding for
    /// the effectively-once tier (input log) and for durable workflow instances
    /// (proc inbox), and for nothing else — so a plain stateful Operative's
    /// mailbox is the only copy its input has, and clearing it is data loss.
    /// Where the log does exist, keeping the queue would double-apply it and,
    /// during a correction, re-order it behind the replayed slice.
    ///
    /// A workflow Entity reaches this too, and is inert: its driver feeds
    /// stages directly, so the mailbox is never pushed to.
    ///
    /// Returns the number of items discarded, counted into
    /// [`RESTORE_DISCARDED_INPUTS`].
    pub fn discard_queued_input_on_restore(&self) -> usize {
        if !self.topology.effectively_once {
            return 0;
        }
        let discarded = self.mailbox.clear()
            + {
                let mut deferred = self.deferred_live.lock_recover();
                let n = deferred.len();
                deferred.clear();
                n
            }
            + {
                // An undelivered push is derived from an input whose offset is
                // therefore not durable — the flusher holds this Entity's
                // records while `push_deferred` is set — so replay re-produces
                // exactly what is dropped here. Order matters: clear the flag
                // only while the list's lock is held, or a concurrent push
                // could append behind a list nobody will drain.
                let mut held = self.push_holdback.lock_recover();
                let n = held.len();
                held.clear();
                self.push_deferred.store(false, Ordering::Release);
                n
            };
        RESTORE_DISCARDED_INPUTS.fetch_add(discarded as u64, Ordering::Relaxed);
        discarded
    }
}

/// What one attempt at a work item resolved to. `Done` covers success AND a
/// park (both consume the item); `RetryLater` hands the item back so the
/// engine's ladder can re-hand it after backoff. An `is_final` attempt never
/// returns `RetryLater` — it parks inline instead.
pub enum ItemDisposition {
    Done { ops: Vec<crate::schema::TimerOp> },
    RetryLater { item: EntityItem, error: String },
}

pub trait ProcedureDispatcher: Send + Sync + 'static {
    /// `Some` for host-run procedures (C-ABI classes). Their command
    /// interactions — restore fold, snapshot, live read, apply-state,
    /// migration serialize — cross as queued activations
    /// ([`crate::poll_core::PollCore::host_command`]) instead of the
    /// in-process calls the typed methods below implement.
    fn host_facts(&self) -> Option<&crate::host::HostProcedureFacts> {
        None
    }

    /// Close an open checkpoint interval whose `max_delay_ms` has elapsed,
    /// shipping its accumulation as the record it owes. Runs NO author code:
    /// the accumulation is already computed, and the close only decides that
    /// it ships now instead of waiting for an input that may never come.
    ///
    /// `false` when there was no open interval — an input closed it between
    /// the wake being armed and its promotion, which is the common case on a
    /// busy Entity.
    fn close_interval(&self, flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>) -> bool {
        let _ = flush_buffer;
        false
    }

    /// When this Entity's open checkpoint interval must close by, for the
    /// caller to arm the deferred wake against. `None` when nothing is open
    /// or the class declared no delay.
    fn interval_deadline(
        &self,
        flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>,
    ) -> Option<u64> {
        let _ = flush_buffer;
        None
    }

    /// Run ONE attempt of one work item — an input through `Procedure::run`,
    /// or a timer firing through `Procedure::on_timer`. Admission,
    /// classification, due-timer generation and the retry ladder live in the
    /// engine (`runner::poll_core`); this is the host half: user code plus
    /// the effect record it produces. `Done.ops` are the committed timer ops
    /// for the caller to arm on the Operative's wheel.
    /// `slot` is the PRODUCING Entity: an inter-procedure emit made during
    /// this invocation withholds work from it rather than holding the calling
    /// thread, so the push site needs it in hand.
    fn dispatch_item(
        &self,
        work: crate::poll_core::WorkItem,
        slot: &Arc<EntitySlot>,
        state: &mut Box<dyn Any + Send>,
        flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>,
        timers: &mut crate::timer::PendingTimers,
        instance: &Weak<crate::operative::instance::StreamInstance>,
    ) -> ItemDisposition;

    fn procedure_id(&self) -> ProcedureId;

    /// The procedure's declared name — what the engine's ladder logs.
    fn procedure_name(&self) -> &'static str;

    /// The input channel's retry policy, driving the engine's ladder.
    fn retry_policy(&self) -> crate::channel::RetryPolicy;

    /// Whether the declared clock is an event time — gates admission
    /// classification and event-time due-timer generation at assembly.
    fn is_event_time(&self) -> bool;

    /// The late-input correction policy, if the class declared one.
    fn correction_policy(&self) -> Option<crate::operative::topology::CorrectionPolicy>;

    /// This Entity's readable `instance/procedure/key` identity — the same
    /// string the `flz.entity` stamp carries, built once at construction.
    fn entity_label(&self) -> &str;

    /// The payload's event time, or `None` when this Procedure's clock is not
    /// `EventTimeFrom` — i.e. when there is no event time to order by.
    ///
    /// The type-erased seam that lets `drain_entity`, which sees only an
    /// opaque `EntityPayload`, sort a replayed slice by a key that only the
    /// user's `TimeExtractor` can produce. Same erasure trick as
    /// `compensate_collecting`.
    fn event_time_of(&self, payload: &EntityPayload) -> Option<u64>;

    /// Workflow-driver entry: run `Procedure::run` once on `input` against the
    /// type-erased entity `state`, collecting inter-procedure output and
    /// routing plugin-bound output through `sink` — instead of the stream
    /// push-dispatch mailbox path. Used only by the sequential workflow driver
    /// (`crate::workflow`); stream dispatch never calls it. `topology`
    /// resolves the producer-edge channel's plugin serializer. Changes are
    /// folded into in-memory state via `StateHandle::apply`; Phase-3 durability
    /// wires the WAL flush here.
    fn run_collecting(
        &self,
        input: Arc<dyn Any + Send + Sync>,
        state: &mut Box<dyn Any + Send>,
        topology: &crate::operative::topology::OperativeTopology,
        sink: &mut dyn crate::workflow::WorkflowCollectSink,
    ) -> Result<(), crate::workflow::WorkflowError>;

    /// Workflow-saga rollback entry: invoke `Procedure::compensate` once for this
    /// entity against its type-erased `state`, handing back the `outputs` the
    /// entity collected during its forward run (downcast to `Arc<S::Output>`)
    /// and routing any compensation emit through `sink`. Used only by the
    /// workflow driver's reverse-order rollback; stream dispatch never calls
    /// it. `topology` resolves the producer-edge channel's plugin serializer.
    fn compensate_collecting(
        &self,
        outputs: Vec<Arc<dyn Any + Send + Sync>>,
        state: &mut Box<dyn Any + Send>,
        topology: &crate::operative::topology::OperativeTopology,
        sink: &mut dyn crate::workflow::WorkflowCollectSink,
    ) -> Result<(), crate::workflow::WorkflowError>;

    /// Warm-restart: rebuild the type-erased entity `state` in place from a
    /// persisted `RestoreInitState` payload (snapshot + WAL change-log),
    /// returning the terminal seq applied and whether any record was read
    /// through a migration. Type-erased entry point so the
    /// control-channel `RestoreInitState` handler — which has no `S` — can
    /// re-seed a live entity after a runner restart. Mirrors the
    /// downcast pattern used by `dispatch`.
    /// `timers` is rebuilt in place from the same change log: the pending set
    /// is folded state, so it comes back through the fold rather than through
    /// a side-file of its own.
    /// `snapshot_header` is the snapshot's stored 16-byte header; it carries
    /// the schema version the snapshot was written at.
    fn restore_state(
        &self,
        state: &mut Box<dyn Any + Send>,
        timers: &mut crate::timer::PendingTimers,
        snapshot_header: &[u8],
        snapshot_bytes: &[u8],
        change_log_bytes: &[u8],
        entity_key: Option<&str>,
    ) -> Result<crate::restore::RestoreOutcome, crate::restore::RestoreError>;

    /// Compaction: serialize the type-erased entity `state` into a
    /// `entity_state_snapshot` wire frame the restore path decodes. Drains any
    /// pending buffered changes first so the snapshot's state and its stamped
    /// seq cannot tear (design D3): after the drain the in-memory state equals
    /// the WAL the controller holds, and the frame is stamped with the highest
    /// change seq the snapshot already contains — the controller keeps only WAL
    /// records strictly above it. Returns `(frame, snapshot_seq, body_len)`.
    /// Type-erased entry point so the control-channel `CompactEntity` handler
    /// — which has no `S` — can produce a snapshot without the JSON migration
    /// serializer. Mirrors `restore_state`; the body is postcard, the codec
    /// `restore.rs` decodes.
    /// The snapshot covers `S::State` only. `timers` is re-stated as a fresh
    /// full-set record at a seq ABOVE `snapshot_seq`, so the truncation the
    /// snapshot authorizes cannot take the pending timers with it — and a
    /// crash between the two leaves the untruncated log, which still holds
    /// them.
    fn snapshot_frame(
        &self,
        state: &mut Box<dyn Any + Send>,
        flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>,
        timers: &crate::timer::PendingTimers,
    ) -> Result<(Vec<u8>, u64, usize), String>;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use super::*;

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
    /// Required because [`BlockParker::wait_one`] rechecks the mailbox
    /// after registering and returns without parking when it isn't full.
    fn saturated_mailbox() -> Arc<Mailbox<u8>> {
        let mbox = Arc::new(Mailbox::<u8>::with_capacity(1));
        mbox.try_push(0).expect("pre-fill");
        mbox
    }

    #[test]
    fn block_parker_unpark_releases_waiter() {
        let parker = Arc::new(BlockParker::new());
        let mbox = saturated_mailbox();
        let p = Arc::clone(&parker);
        let m = Arc::clone(&mbox);
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s = Arc::clone(&started);
        let t = std::thread::spawn(move || {
            s.store(true, Ordering::Release);
            p.wait_one(&m);
        });
        // Wait for the thread to register itself before unparking, so we
        // exercise the wake-after-park path rather than the no-op
        // unpark-before-park (Parker handles both, but we want the
        // observable path here).
        while !started.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        // The producer has reached `parker.park()` once it is counted.
        spin_until("the producer to park", || parker.waiter_count() == 1);
        parker.unpark_one();
        spin_until("the waiter to be unparked", || t.is_finished());
        t.join().unwrap();
    }

    #[test]
    fn block_parker_unpark_with_no_waiters_is_noop() {
        let parker = BlockParker::new();
        // Should not block, panic, or otherwise misbehave.
        parker.unpark_one();
        parker.unpark_one();
        assert_eq!(parker.waiter_count(), 0);
    }

    #[test]
    fn block_parker_fifo_order_at_unpark_time() {
        // Push three unparkers in order A, B, C; verify pop_front sees
        // A, B, C. Test the deque-level ordering only — once threads
        // observe their wakeups, scheduling can reorder them.
        let parker = BlockParker::new();
        let mbox = saturated_mailbox();
        // Use the internal API by parking three threads serialized via
        // sleeps so we can assert the wait_count grows monotonically.
        let p = Arc::new(parker);
        let order = Arc::new(Mutex::new(Vec::<u8>::new()));
        let mut threads = Vec::new();
        for (parked, tag) in [b'A', b'B', b'C'].into_iter().enumerate() {
            let parker = Arc::clone(&p);
            let m = Arc::clone(&mbox);
            let order = Arc::clone(&order);
            threads.push(std::thread::spawn(move || {
                parker.wait_one(&m);
                order.lock().unwrap().push(tag);
            }));
            // Sequence the parkers so they enter the deque in order: each is
            // counted parked before the next is spawned.
            spin_until("the parker to park", || p.waiter_count() == parked + 1);
        }
        for woken in 1..=3 {
            p.unpark_one();
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
    /// push inside `wait_one`, the producer must NOT block forever. The
    /// fix re-checks `mailbox.is_full()` after registering and returns
    /// immediately when the mailbox has space.
    #[test]
    fn wait_one_does_not_block_when_mailbox_has_space() {
        let parker = BlockParker::new();
        let mbox = Arc::new(Mailbox::<u8>::with_capacity(2));
        // Mailbox has 2 free slots — wait_one must NOT park.
        let start = Instant::now();
        parker.wait_one(&mbox);
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(50),
            "wait_one parked despite non-full mailbox; elapsed = {:?}",
            elapsed
        );
        // And it leaves NOTHING behind. An entry whose thread has moved on
        // absorbs an `unpark_one` the next live waiter was owed, which makes
        // that waiter's next timeout more likely, which leaves another dead
        // entry: a deficit that only grows until every wake is absorbed and a
        // live producer advances one bounded park at a time.
        assert_eq!(parker.waiter_count(), 0);
    }

    /// A timed-out waiter leaves nothing behind either — the path a
    /// `DropNothing` entry-path producer takes on every 50 ms retry, and the
    /// one that accumulates if it does not clean up.
    #[test]
    fn a_timed_out_waiter_removes_its_own_entry() {
        let parker = BlockParker::new();
        let mbox = saturated_mailbox();
        for _ in 0..5 {
            parker.wait_one_timeout(&mbox, Duration::from_millis(1));
        }
        assert_eq!(parker.waiter_count(), 0);
    }
}

/// The derivations and the assembly both slot builders share — the typed
/// layer's (`sdk/rust` dispatcher) and the host layer's (`crate::host`).
/// The caller supplies what genuinely differs: the state cell, the flush
/// buffer, the dispatcher, and its flusher registration.
pub struct SlotParts {
    pub entity_identity: Arc<Vec<u8>>,
    pub entity_label: Arc<str>,
    /// The routing key itself, `None` in single-entity mode. The one rendering
    /// a Procedure reads back (`ctx.entity_key()`, and the default key of a
    /// keyed-channel emit).
    pub entity_key: Option<Arc<str>>,
    pub change_seq: Arc<AtomicU64>,
    pub event_time_high_water: Arc<AtomicI64>,
    /// Built here rather than in `assemble_slot` because the flusher entry is
    /// registered before the slot exists and shares this flag.
    pub push_deferred: Arc<AtomicBool>,
}

/// Derive the per-Entity identity cells — every rendering of "which Entity is
/// this" comes from here, so they cannot disagree. The WAL identity is keyed by
/// the operative INSTANCE id, not the factory id: two operatives of the same
/// factory must not share one Entity WAL.
pub fn slot_parts(instance_id: &str, procedure_name: &str, entity_key: Option<&str>) -> SlotParts {
    SlotParts {
        entity_identity: Arc::new(crate::flusher::encode_entity_identity(
            instance_id,
            procedure_name,
            entity_key.unwrap_or(""),
        )),
        entity_label: Arc::from(format!(
            "{}/{}/{}",
            instance_id,
            procedure_name,
            entity_key.unwrap_or("")
        )),
        entity_key: entity_key.map(Arc::from),
        change_seq: Arc::new(AtomicU64::new(0)),
        event_time_high_water: Arc::new(AtomicI64::new(-1)),
        push_deferred: Arc::new(AtomicBool::new(false)),
    }
}

/// Assemble the slot: every field that is pure kernel plumbing in one place.
#[allow(clippy::too_many_arguments)]
pub fn assemble_slot(
    args: crate::channel::BuildEntityArgs<'_>,
    parts: SlotParts,
    state: Box<dyn Any + Send>,
    flush_buffer: Arc<Mutex<Box<dyn crate::flusher::FlushableBuffer>>>,
    dispatcher: Arc<dyn ProcedureDispatcher>,
    flusher_entry_id: Option<crate::flusher::FlusherEntryId>,
    mailbox_capacity: usize,
) -> Arc<EntitySlot> {
    Arc::new(EntitySlot {
        state: Mutex::new(state),
        mailbox: Arc::new(Mailbox::with_capacity(mailbox_capacity)),
        dispatcher,
        procedure_id: args.procedure_id,
        entity_key: parts.entity_key,
        instance: args.instance,
        topology: args.topology,
        block_parker: BlockParker::new(),
        drain_active: AtomicBool::new(false),
        flush_buffer,
        persistence_enabled: args.persistence_enabled,
        change_seq: parts.change_seq,
        consumed_floor: AtomicI64::new(-1),
        event_time_high_water: parts.event_time_high_water,
        replay_buffer: Mutex::new(Vec::new()),
        replay_open: AtomicBool::new(false),
        replay_depth: AtomicU64::new(0),
        entity_identity: parts.entity_identity,
        last_activity_at: AtomicI64::new(crate::dynamic::now_millis()),
        flusher_entry_id,
        timers: Mutex::new(crate::timer::PendingTimers::default()),
        pending_retry: Mutex::new(VecDeque::new()),
        deferred_live: Mutex::new(VecDeque::new()),
        push_holdback: Mutex::new(VecDeque::new()),
        push_deferred: parts.push_deferred,
        push_gated: AtomicBool::new(false),
    })
}
