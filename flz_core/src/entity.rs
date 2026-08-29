//! `EntitySlot`: one (procedure, entity) — its erased state, bounded mailbox,
//! dispatcher, wait list, flush buffer and counters. The invariant: at most
//! one drain is active per slot (the mailbox's inflight CAS plus
//! `drain_active`), so user code, timer firings and command crossings on one
//! Entity never overlap. Under `DropNothing` a producer that cannot deliver
//! waits on the slot's [`crate::mailbox::Waiters`]: the entry-path reader
//! parks its thread, a producing Entity holds the push in
//! [`EntitySlot::push_holdback`] and is handed no work until it lands.

use std::any::Any;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use flz_transport::transport::FlezhaStream;

use crate::channel::{OverflowPolicy, ProcedureId};
pub use crate::header::RestoreError;
use crate::mailbox::{Mailbox, Waiters};
pub use crate::restore::RestoreInitPayload;
use crate::util::LockRecover;

/// Erased payload in a mailbox: the consumer downcasts `value` to its own
/// input type; `stream` is the connection the wire input arrived on, so a
/// plugin-bound emit downstream lands on the same connection.
#[derive(Clone)]
pub struct EntityPayload {
    pub value: Arc<dyn Any + Send + Sync>,
    pub stream: Arc<FlezhaStream>,
    /// Input-log offset this payload derives from (`flz.offset`, inherited
    /// across inter-procedure emits); `None` outside the effectively-once tier.
    pub consumed_offset: Option<u64>,
    /// A member of a replayed slice (`flz.replay`): held until the slice
    /// closes and dispatched in event-time order. Never inherited by an
    /// inter-procedure emit — a downstream Procedure could not see the
    /// slice's end and would hold its drain forever.
    pub replay: bool,
}

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

/// One item in an Entity's mailbox. A timer firing is queued rather than
/// invoked where it expires, so the drain gate serializes `on_timer` against
/// `run`. Cloning is shallow: the retry ladder hands a clone to each
/// non-final attempt so the original survives for the re-hand.
#[derive(Clone)]
pub enum EntityItem {
    Input(EntityPayload),
    Timer(crate::timer::TimerFire),
    /// An open checkpoint interval reached its `max_delay_ms`. Rides the
    /// mailbox so the close is serialized with the Entity's invocations; runs
    /// no author code.
    CloseInterval,
}

pub type EntityMailbox = Arc<Mailbox<EntityItem>>;

/// Per-Entity producer-side counters, monotonic and `Relaxed`. The heartbeat
/// carries the process-wide aggregates beside them, never this breakdown.
#[derive(Debug, Default)]
pub struct EntityCounters {
    pub pushes_total: AtomicU64,
    /// `DropNothing` wait iterations, one per round of a producer's wait.
    pub pushes_blocked_total: AtomicU64,
    pub drops_newest_total: AtomicU64,
    pub drops_oldest_total: AtomicU64,
}

/// Payloads dispatched, process-wide; rides every `data_heartbeat` so the
/// controller can tell a wedged dispatch from a healthy runner.
pub static DISPATCHES_COMPLETED: AtomicU64 = AtomicU64::new(0);

/// `DropNothing` wait iterations, process-wide (heartbeat key
/// `blocked_parks`). Per iteration, not per episode: one long episode would
/// otherwise freeze beside a frozen dispatch count, which is what a wedge
/// looks like. `CREDIT_PARKED` advances the same way.
pub static PUSHES_BLOCKED_TOTAL: AtomicU64 = AtomicU64::new(0);

pub static DROPS_NEWEST_TOTAL: AtomicU64 = AtomicU64::new(0);

pub static DROPS_OLDEST_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Queued inputs a restore discarded, process-wide; on the heartbeat because
/// it is the operator's one handle on what a rebind threw away.
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
    /// The operative INSTANCE id — what a schema address names.
    pub instance_id: Arc<str>,
    pub instance: Weak<crate::operative::instance::StreamInstance>,
    pub topology: Arc<crate::operative::topology::OperativeTopology>,
    /// Whoever is owed this mailbox's next free slot under `DropNothing`,
    /// served one per successful pop.
    pub waiters: Waiters,
    /// True only while a thread holds the handed-out activation, or while the
    /// Entity is gated on its own push holdback. `mailbox`'s inflight flag is
    /// the wider "a round is scheduled or running".
    pub drain_active: AtomicBool,
    /// Locked by the dispatcher for the whole invocation and by the flusher
    /// on every linger tick.
    pub flush_buffer: Arc<Mutex<Box<dyn crate::flusher::FlushableBuffer>>>,
    pub persistence_enabled: bool,
    /// Per-Entity WAL seq, stamped into a frame before it reaches the sink so
    /// a frame buffered across a reconnect replays with its original seq.
    pub change_seq: Arc<std::sync::atomic::AtomicU64>,
    /// Highest input-log offset durably consumed before a restore (`-1` =
    /// none); re-forwarded inputs at or below it are skipped.
    pub consumed_floor: std::sync::atomic::AtomicI64,
    /// Highest event time dispatched (`-1` = none); an input behind it is
    /// late. Restored from the snapshot header, never reset on a replay.
    pub event_time_high_water: Arc<AtomicI64>,
    /// Members of a replayed slice, held until the slice closes.
    pub replay_buffer: Mutex<Vec<EntityPayload>>,
    /// Set by the first replayed payload, cleared by the end-of-slice
    /// broadcast — both on the frame thread, so the close cannot overtake a
    /// member it was meant to include.
    pub replay_open: AtomicBool,
    /// `replay_buffer.len()` as one relaxed load, for the drain's hot path.
    pub replay_depth: AtomicU64,
    /// Wire identity `(operative instance, procedure, entity_key)` per
    /// `flusher::encode_entity_identity`; the controller routes on it.
    pub entity_identity: Arc<Vec<u8>>,
    /// `util::monotonic_ms` of the last successful push; the dynamic reaper's
    /// idle clock.
    pub last_activity_ms: AtomicI64,
    /// `Some` iff the class persists changes; eviction deregisters it.
    pub flusher_entry_id: Option<crate::flusher::FlusherEntryId>,
    /// Pending timers, folded from this Entity's own change log. Lock order:
    /// state → flush buffer → timers.
    pub timers: Mutex<crate::timer::PendingTimers>,
    /// Items re-handed ahead of the mailbox: a retrying head and the
    /// unprocessed tail of a batch that stopped. Touched only under the gates.
    pub pending_retry: Mutex<VecDeque<crate::poll_core::ItemAttempt>>,
    /// Live items popped behind a closed replay slice; they run after its
    /// remainder, ahead of the mailbox. Touched only under the gates.
    pub deferred_live: Mutex<VecDeque<EntityItem>>,
    /// Pushes this Entity produced and could not deliver, in emit order. The
    /// gates stay held until the list drains; the consumer's next free slot
    /// drains it ([`crate::admission::admit_from_entity`]).
    pub push_holdback: Mutex<VecDeque<PendingPush>>,
    /// Whether [`Self::push_holdback`] is non-empty — an atomic read on every
    /// push, written only under the holdback's lock so "observed set ⇒ append
    /// behind what is held" is total. Shared with the flusher entry: no record
    /// ships while a push derived from the same input is undelivered.
    pub push_deferred: Arc<AtomicBool>,
    /// The gates are held FOR the holdback, not by an activation. Whoever
    /// drains the holdback to empty takes this claim and releases them.
    pub push_gated: AtomicBool,
}

/// One inter-procedure push withheld from a full consumer.
pub struct PendingPush {
    pub consumer: Arc<EntitySlot>,
    pub item: EntityItem,
}

impl EntitySlot {
    pub fn counters(&self) -> &EntityCounters {
        self.mailbox.counters()
    }

    /// One `DropNothing` wait iteration: the per-Entity counter and the
    /// heartbeat's aggregate move together so they cannot drift.
    pub fn note_blocked(&self) {
        self.counters()
            .pushes_blocked_total
            .fetch_add(1, Ordering::Relaxed);
        PUSHES_BLOCKED_TOTAL.fetch_add(1, Ordering::Relaxed);
    }

    /// One item a shedding policy discarded: both counters, and a `debug` line
    /// naming it — the last place its identity exists. `evicted` is `None`
    /// when a `DropOldest` force-push found the queue already drained.
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

    /// Drop an open checkpoint interval's staged changes: a restore re-seats
    /// the Entity on a past in which they were never applied.
    pub fn discard_staged_interval(&self) {
        self.flush_buffer.lock_recover().discard_staged();
    }

    /// Drop a half-filled correction slice: releasing it later would order its
    /// inputs behind the replay meant to re-order them.
    pub fn discard_replay_slice(&self) {
        self.replay_buffer.lock_recover().clear();
        self.replay_depth.store(0, Ordering::Relaxed);
        self.replay_open.store(false, Ordering::Release);
    }

    /// Drop queued input only where the controller re-forwards it from a
    /// durable log (the effectively-once tier, durable workflows); anywhere
    /// else the mailbox is the input's only copy. Returns the count, added to
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

/// What one attempt resolved to. `Done` covers success and a park (both
/// consume the item); an `is_final` attempt never answers `RetryLater`.
pub enum ItemDisposition {
    Done { ops: Vec<crate::schema::TimerOp> },
    RetryLater { item: EntityItem, error: String },
}

pub trait ProcedureDispatcher: Send + Sync + 'static {
    /// `Some` for a host-run procedure, whose command interactions cross as
    /// activations ([`crate::poll_core::PollCore::host_command`]).
    fn host_facts(&self) -> Option<&crate::host::HostProcedureFacts> {
        None
    }

    /// Close an open checkpoint interval whose `max_delay_ms` elapsed,
    /// shipping its accumulation; `false` when an input already closed it.
    fn close_interval(&self, flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>) -> bool {
        let _ = flush_buffer;
        false
    }

    /// When the open checkpoint interval must close by; `None` when nothing
    /// is open or the class declared no delay.
    fn interval_deadline(
        &self,
        flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>,
    ) -> Option<u64> {
        let _ = flush_buffer;
        None
    }

    /// Run ONE attempt of one item: user code plus the effect record it
    /// produces; the ladder is the engine's. `slot` is the producing Entity —
    /// an inter-procedure emit withholds work from it rather than holding the
    /// calling thread.
    fn dispatch_item(
        &self,
        work: crate::poll_core::ItemAttempt,
        slot: &Arc<EntitySlot>,
        state: &mut Box<dyn Any + Send>,
        flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>,
        timers: &mut crate::timer::PendingTimers,
        instance: &Weak<crate::operative::instance::StreamInstance>,
    ) -> ItemDisposition;

    fn procedure_id(&self) -> ProcedureId;

    fn procedure_name(&self) -> &'static str;

    fn retry_policy(&self) -> crate::channel::RetryPolicy;

    fn is_event_time(&self) -> bool;

    fn correction_policy(&self) -> Option<crate::operative::topology::CorrectionPolicy>;

    /// `instance/procedure/key`, the string the `flz.entity` stamp carries.
    fn entity_label(&self) -> &str;

    /// The payload's event time, `None` unless the clock is `EventTimeFrom`;
    /// how the drain sorts a replayed slice by a key only the author's
    /// extractor can produce.
    fn event_time_of(&self, payload: &EntityPayload) -> Option<u64>;

    /// Workflow-driver entry: one `run` on `input`, inter-procedure output
    /// collected and plugin-bound output staged through `sink` instead of the
    /// mailbox path.
    fn run_collecting(
        &self,
        input: Arc<dyn Any + Send + Sync>,
        state: &mut Box<dyn Any + Send>,
        topology: &crate::operative::topology::OperativeTopology,
        sink: &mut dyn crate::workflow::WorkflowCollectSink,
    ) -> Result<(), crate::workflow::WorkflowError>;

    /// Workflow rollback entry: one `compensate` over the `outputs` this
    /// entity collected forward, any emit through `sink`.
    fn compensate_collecting(
        &self,
        outputs: Vec<Arc<dyn Any + Send + Sync>>,
        state: &mut Box<dyn Any + Send>,
        topology: &crate::operative::topology::OperativeTopology,
        sink: &mut dyn crate::workflow::WorkflowCollectSink,
    ) -> Result<(), crate::workflow::WorkflowError>;

    /// Rebuild `state` and `timers` in place from a snapshot (with its stored
    /// header) plus change log; answers the terminal seq and whether any
    /// record was migrated.
    fn restore_state(
        &self,
        state: &mut Box<dyn Any + Send>,
        timers: &mut crate::timer::PendingTimers,
        snapshot_header: &[u8],
        snapshot_bytes: &[u8],
        change_log_bytes: &[u8],
        entity_key: Option<&str>,
    ) -> Result<crate::restore::RestoreOutcome, crate::restore::RestoreError>;

    /// Compaction: `state` as an `entity_state_snapshot` frame stamped with
    /// the highest seq it contains, after draining the buffer so the two
    /// cannot tear. `timers` is re-stated ABOVE that seq so the truncation the
    /// snapshot authorizes cannot take them. Returns `(frame, seq, body_len)`.
    fn snapshot_frame(
        &self,
        state: &mut Box<dyn Any + Send>,
        flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>,
        timers: &crate::timer::PendingTimers,
    ) -> Result<(Vec<u8>, u64, usize), String>;
}

/// What both slot builders (typed and host) derive the same way; the caller
/// supplies the state cell, the buffer, the dispatcher and its registration.
pub struct SlotParts {
    pub entity_identity: Arc<Vec<u8>>,
    pub entity_label: Arc<str>,
    /// The routing key, `None` in single-entity mode.
    pub entity_key: Option<Arc<str>>,
    pub change_seq: Arc<AtomicU64>,
    pub event_time_high_water: Arc<AtomicI64>,
    /// Shared with the flusher entry, which is registered before the slot.
    pub push_deferred: Arc<AtomicBool>,
}

/// Every rendering of "which Entity is this", derived once. The WAL identity
/// is keyed by the INSTANCE id: two operatives of one factory never share a
/// WAL.
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
        instance_id: args.instance_id,
        instance: args.instance,
        topology: args.topology,
        waiters: Waiters::new(),
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
        last_activity_ms: AtomicI64::new(crate::util::monotonic_ms()),
        flusher_entry_id,
        timers: Mutex::new(crate::timer::PendingTimers::default()),
        pending_retry: Mutex::new(VecDeque::new()),
        deferred_live: Mutex::new(VecDeque::new()),
        push_holdback: Mutex::new(VecDeque::new()),
        push_deferred: parts.push_deferred,
        push_gated: AtomicBool::new(false),
    })
}
