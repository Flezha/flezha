//! `CellSlot`: one (tasklet, cell) — its erased state, bounded mailbox,
//! dispatcher, wait list, flush buffer and counters. The invariant: at most
//! one drain is active per slot (the mailbox's inflight CAS plus
//! `drain_active`), so user code, timer firings and command crossings on one
//! Cell never overlap. Under `DropNothing` a producer that cannot deliver
//! waits on the slot's [`crate::mailbox::Waiters`]: the entry-path reader
//! parks its thread, a producing Cell holds the push in
//! [`CellSlot::push_holdback`] and is handed no work until it lands.

use std::any::Any;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use flz_transport::transport::FlezhaStream;

use crate::channel::{OverflowPolicy, TaskletId};
pub use crate::header::RestoreError;
use crate::mailbox::{Mailbox, Waiters};
pub use crate::restore::RestoreInitPayload;
use crate::util::LockRecover;

/// Erased payload in a mailbox: the consumer downcasts `value` to its own
/// input type; `stream` is the connection the wire input arrived on, so a
/// plugin-bound emit downstream lands on the same connection.
#[derive(Clone)]
pub struct CellPayload {
    pub value: Arc<dyn Any + Send + Sync>,
    pub stream: Arc<FlezhaStream>,
    /// Input-log offset this payload derives from (`flz.offset`, inherited
    /// across inter-tasklet emits); `None` outside the effectively-once tier.
    pub consumed_offset: Option<u64>,
    /// The outside call waiting on the input this payload derives from
    /// (`flz.reply_to`), inherited across inter-tasklet emits like the
    /// offset; never on a timer firing.
    pub call: Option<Arc<str>>,
    /// A member of a replayed slice (`flz.replay`): held until the slice
    /// closes and dispatched in event-time order. Never inherited by an
    /// inter-tasklet emit — a downstream Tasklet could not see the
    /// slice's end and would hold its drain forever.
    pub replay: bool,
    /// Whether the controller forwarded this payload, rather than a Tasklet
    /// of the instance emitting it: only such an input advances the Cell's
    /// live consumed floor, since one input may emit several values.
    pub forwarded: bool,
    /// Holds the instance's processed prefix below `consumed_offset` until
    /// the record this input settles into is written.
    pub hold: Option<Arc<crate::prefix::Hold>>,
}

pub fn make_cell_payload(
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
) -> CellPayload {
    CellPayload {
        value: Arc::clone(value),
        stream: Arc::clone(stream),
        consumed_offset,
        call: None,
        replay,
        forwarded: false,
        hold: None,
    }
}

/// Keep a settled input's hold with the Cell's buffer until the drain that
/// ships its record; a Cell the flusher never drains has no record to wait
/// for, and releases it at once.
pub fn keep_hold(
    slot: &CellSlot,
    buffer: &mut dyn crate::flusher::FlushableBuffer,
    hold: Option<Arc<crate::prefix::Hold>>,
) {
    if let Some(hold) = hold {
        if slot.flusher_entry_id.is_some() {
            buffer.keep_hold(hold);
        }
    }
}

/// One item in a Cell's mailbox. A timer firing is queued rather than
/// invoked where it expires, so the drain gate serializes `on_timer` against
/// `run`. Cloning is shallow: the retry ladder hands a clone to each
/// non-final attempt so the original survives for the re-hand.
#[derive(Clone)]
pub enum CellItem {
    Input(CellPayload),
    Timer(crate::timer::TimerFire),
    /// An open checkpoint interval reached its `max_delay_ms`. Rides the
    /// mailbox so the close is serialized with the Cell's invocations; runs
    /// no author code.
    CloseInterval,
}

pub type CellMailbox = Arc<Mailbox<CellItem>>;

/// Per-Cell producer-side counters, monotonic and `Relaxed`. The heartbeat
/// carries the process-wide aggregates beside them, never this breakdown.
#[derive(Debug, Default)]
pub struct CellCounters {
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
/// it is the user's one handle on what a rebind threw away.
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

/// One item a drop-oldest queue shed. The Cell mailboxes and the outbound
/// reconnect ring share this counter, so the heartbeat's `drops_oldest` is
/// every drop-oldest shed on the runner rather than one queue's.
pub(crate) fn note_dropped_oldest() {
    DROPS_OLDEST_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn restore_discarded_inputs() -> u64 {
    RESTORE_DISCARDED_INPUTS.load(Ordering::Relaxed)
}

pub struct CellSlot {
    pub state: Mutex<Box<dyn Any + Send>>,
    pub mailbox: CellMailbox,
    pub dispatcher: Arc<dyn TaskletDispatcher>,
    pub tasklet_id: TaskletId,
    pub cell_key: Option<Arc<str>>,
    /// The relay INSTANCE id — what a schema address names.
    pub instance_id: Arc<str>,
    pub instance: Weak<crate::relay::instance::RelayInstance>,
    pub topology: Arc<crate::relay::topology::RelayTopology>,
    /// Whoever is owed this mailbox's next free slot under `DropNothing`,
    /// served one per successful pop.
    pub waiters: Waiters,
    /// True only while a thread holds the handed-out activation, or while the
    /// Cell is gated on its own push holdback. `mailbox`'s inflight flag is
    /// the wider "a round is scheduled or running".
    pub drain_active: AtomicBool,
    /// Locked by the dispatcher for the whole invocation and by the flusher
    /// on every linger tick.
    pub flush_buffer: Arc<Mutex<Box<dyn crate::flusher::FlushableBuffer>>>,
    pub keeps_state: bool,
    /// Per-Cell WAL seq, stamped into a frame before it reaches the sink so
    /// a frame buffered across a reconnect replays with its original seq.
    pub change_seq: Arc<std::sync::atomic::AtomicU64>,
    /// Highest input-log offset this Cell took (`-1` = none): restored from
    /// its durable records, advanced as it takes a forwarded input. An input
    /// at or below it is dropped unrun.
    pub consumed_floor: std::sync::atomic::AtomicI64,
    /// Highest event time dispatched (`-1` = none); an input behind it is
    /// late. Restored from the snapshot header, never reset on a replay.
    pub event_time_high_water: Arc<AtomicI64>,
    /// Members of a replayed slice, held until the slice closes.
    pub replay_buffer: Mutex<Vec<CellPayload>>,
    /// Set by the first replayed payload, cleared by the end-of-slice
    /// broadcast — both on the frame thread, so the close cannot overtake a
    /// member it was meant to include.
    pub replay_open: AtomicBool,
    /// `replay_buffer.len()` as one relaxed load, for the drain's hot path.
    pub replay_depth: AtomicU64,
    /// Wire identity `(relay instance, tasklet, cell_key)` per
    /// `flusher::encode_cell_identity`; the controller routes on it.
    pub cell_identity: Arc<Vec<u8>>,
    /// `util::monotonic_ms` of the last successful push; the idle clock of
    /// the dynamic reaper and of an instance's sleep.
    pub last_activity_ms: AtomicI64,
    /// `Some` iff the class persists changes; eviction deregisters it.
    pub flusher_entry_id: Option<crate::flusher::FlusherEntryId>,
    /// Pending timers, folded from this Cell's own change log. Lock order:
    /// state → flush buffer → timers.
    pub timers: Mutex<crate::timer::PendingTimers>,
    /// Items re-handed ahead of the mailbox: a retrying head and the
    /// unprocessed tail of a batch that stopped. Touched only under the gates.
    pub pending_retry: Mutex<VecDeque<crate::poll_core::ItemAttempt>>,
    /// Live items popped behind a closed replay slice; they run after its
    /// remainder, ahead of the mailbox. Touched only under the gates.
    pub deferred_live: Mutex<VecDeque<CellItem>>,
    /// Pushes this Cell produced and could not deliver, in emit order. The
    /// gates stay held until the list drains; the consumer's next free slot
    /// drains it ([`crate::admission::admit_from_cell`]).
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

/// One inter-tasklet push withheld from a full consumer.
pub struct PendingPush {
    pub consumer: Arc<CellSlot>,
    pub item: CellItem,
}

impl CellSlot {
    pub fn counters(&self) -> &CellCounters {
        self.mailbox.counters()
    }

    /// Take the drain gate when nothing is queued: `false` while a round is
    /// scheduled or running (a retry backoff and a push holdback keep it
    /// held). The dynamic reaper evicts under the gate this leaves held.
    pub(crate) fn claim_drained(self: &Arc<Self>) -> bool {
        if !self.mailbox.claim_inflight() {
            return false;
        }
        if self.mailbox.is_empty() {
            return true;
        }
        self.release_drained();
        false
    }

    /// Give back the gate [`Self::claim_drained`] took; a push that landed
    /// meanwhile found it taken and scheduled nothing, so it is scheduled here.
    pub(crate) fn release_drained(self: &Arc<Self>) {
        self.mailbox.release_inflight();
        if !self.mailbox.is_empty() && self.mailbox.claim_inflight() {
            crate::poll_core::global().schedule(self);
        }
    }

    /// Whether this Cell has nothing left to do: drained, nothing staged
    /// behind a replay slice, nothing unflushed and, where `timers_hold`, no
    /// timer pending. The sleep of an instance asks it of every Cell.
    pub(crate) fn quiescent(self: &Arc<Self>, timers_hold: bool) -> bool {
        if !self.claim_drained() {
            return false;
        }
        let unflushed = !self
            .flush_buffer
            .try_lock()
            .is_ok_and(|buffer| buffer.is_empty());
        let idle = !unflushed
            && self.replay_depth.load(Ordering::Acquire) == 0
            && self.deferred_live.lock_recover().is_empty()
            && (!timers_hold || self.timers.lock_recover().is_empty());
        self.release_drained();
        idle
    }

    /// One `DropNothing` wait iteration: the per-Cell counter and the
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
    pub fn note_shed(&self, policy: OverflowPolicy, evicted: Option<&CellItem>) {
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
                note_dropped_oldest();
                "drop_oldest"
            }
            // The policy that discards nothing sheds nothing.
            OverflowPolicy::DropNothing => return,
        };
        let Some(item) = evicted else { return };
        tracing::debug!(
            target: "flezha::mailbox",
            policy,
            tasklet_id = self.tasklet_id,
            cell_key = ?self.cell_key,
            item = %match item {
                CellItem::Input(p) => match p.consumed_offset {
                    Some(offset) => format!("input at offset {offset}"),
                    None => "input".to_string(),
                },
                CellItem::Timer(fire) => {
                    format!("{:?} timer for {}", fire.kind, fire.scheduled_at)
                }
                CellItem::CloseInterval => "checkpoint interval close".to_string(),
            },
            "shed an item from a full cell mailbox"
        );
    }

    /// Whether this Cell's record for an input at `offset` keeps the input's
    /// inter-tasklet values: under a class that records effects, and wherever
    /// the record carries the offset the Cell took, whose floor then skips the
    /// re-forwarded input.
    pub fn keeps_edges(&self, offset: Option<u64>) -> bool {
        self.topology.records_effects(self.keeps_state)
            || (self.flusher_entry_id.is_some() && offset.is_some())
    }

    /// Drop what the flush buffer has not shipped: a restore re-seats the
    /// Cell on a past in which none of it was applied.
    pub fn discard_unshipped(&self) {
        self.flush_buffer.lock_recover().discard_unshipped();
    }

    /// Drop a half-filled correction slice: releasing it later would order its
    /// inputs behind the replay meant to re-order them.
    pub fn discard_replay_slice(&self) {
        self.replay_buffer.lock_recover().clear();
        self.replay_depth.store(0, Ordering::Relaxed);
        self.replay_open.store(false, Ordering::Release);
    }

    /// Drop queued input only where the controller re-forwards it from a
    /// durable log (the effectively-once tier, a class that sleeps); anywhere
    /// else the mailbox is the input's only copy. Returns the count, added to
    /// [`RESTORE_DISCARDED_INPUTS`].
    pub fn discard_queued_input_on_restore(&self) -> usize {
        if !self.topology.effectively_once && self.topology.sleep_after().is_none() {
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
                // therefore not durable — the flusher holds this Cell's
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
    RetryLater { item: CellItem, error: String },
}

pub trait TaskletDispatcher: Send + Sync + 'static {
    /// `Some` for a host-run tasklet, whose command interactions cross as
    /// activations ([`crate::poll_core::PollCore::host_command`]).
    fn host_facts(&self) -> Option<&crate::host::HostTaskletFacts> {
        None
    }

    /// Close an open checkpoint interval whose `max_delay_ms` elapsed,
    /// shipping its accumulation; `false` when an input already closed it.
    fn close_interval(&self, flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>) -> bool {
        let _ = flush_buffer;
        false
    }

    /// Append timer ops committed outside an invocation to this Cell's
    /// record stream and ship them at once: a steps relay firing's removal, at
    /// its stage's commit.
    fn write_timer_ops(&self, slot: &Arc<CellSlot>, ops: Vec<crate::timer::TimerOp>);

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
    /// produces; the ladder is the engine's. `slot` is the producing Cell —
    /// an inter-tasklet emit withholds work from it rather than holding the
    /// calling thread.
    fn dispatch_item(
        &self,
        work: crate::poll_core::ItemAttempt,
        slot: &Arc<CellSlot>,
        state: &mut Box<dyn Any + Send>,
        flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>,
        timers: &mut crate::timer::PendingTimers,
        instance: &Weak<crate::relay::instance::RelayInstance>,
    ) -> ItemDisposition;

    fn tasklet_id(&self) -> TaskletId;

    fn tasklet_name(&self) -> &'static str;

    fn retry_policy(&self) -> crate::channel::RetryPolicy;

    fn is_event_time(&self) -> bool;

    fn correction_policy(&self) -> Option<crate::relay::topology::CorrectionPolicy>;

    /// `instance/tasklet/key`, the string the `flz.cell` stamp carries.
    fn cell_label(&self) -> &str;

    /// The payload's event time, `None` unless the clock is event time;
    /// how the drain sorts a replayed slice by a key only the author's
    /// extractor can produce.
    fn event_time_of(&self, payload: &CellPayload) -> Option<u64>;

    /// A steps relay undo served in process: `compensate` over the outputs this
    /// Cell recorded forward, its emits through the Cell's own sink. A host
    /// serves the `CompensateRun` activation itself and never reaches this.
    fn compensate(&self, slot: &Arc<CellSlot>, outputs: Vec<Arc<dyn Any + Send + Sync>>) {
        let _ = (slot, outputs);
    }

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
        cell_key: Option<&str>,
    ) -> Result<crate::restore::RestoreOutcome, crate::restore::RestoreError>;

    /// Compaction: `state` as a `cell_state_snapshot` frame stamped with
    /// the highest seq it contains, after draining the buffer so the two
    /// cannot tear. `timers` is re-stated ABOVE that seq so the truncation the
    /// snapshot authorizes cannot take them. Returns `(frame, seq, body_len)`.
    fn snapshot_frame(
        &self,
        state: &mut Box<dyn Any + Send>,
        flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>,
        timers: &crate::timer::PendingTimers,
    ) -> Result<(Vec<u8>, u64, usize), crate::Error>;
}

/// What both slot builders (typed and host) derive the same way; the caller
/// supplies the state, the buffer, the dispatcher and its registration.
pub struct SlotParts {
    pub cell_identity: Arc<Vec<u8>>,
    pub cell_label: Arc<str>,
    /// The routing key, `None` in single-cell mode.
    pub cell_key: Option<Arc<str>>,
    pub change_seq: Arc<AtomicU64>,
    pub event_time_high_water: Arc<AtomicI64>,
    /// Shared with the flusher entry, which is registered before the slot.
    pub push_deferred: Arc<AtomicBool>,
}

/// Every rendering of "which Cell is this", derived once. The WAL identity
/// is keyed by the INSTANCE id: two relays of one factory never share a
/// WAL.
pub fn slot_parts(instance_id: &str, tasklet_name: &str, cell_key: Option<&str>) -> SlotParts {
    SlotParts {
        cell_identity: Arc::new(crate::flusher::encode_cell_identity(
            instance_id,
            tasklet_name,
            cell_key.unwrap_or(""),
        )),
        cell_label: Arc::from(format!(
            "{}/{}/{}",
            instance_id,
            tasklet_name,
            cell_key.unwrap_or("")
        )),
        cell_key: cell_key.map(Arc::from),
        change_seq: Arc::new(AtomicU64::new(0)),
        event_time_high_water: Arc::new(AtomicI64::new(-1)),
        push_deferred: Arc::new(AtomicBool::new(false)),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn assemble_slot(
    args: crate::channel::BuildCellArgs<'_>,
    parts: SlotParts,
    state: Box<dyn Any + Send>,
    flush_buffer: Arc<Mutex<Box<dyn crate::flusher::FlushableBuffer>>>,
    dispatcher: Arc<dyn TaskletDispatcher>,
    flusher_entry_id: Option<crate::flusher::FlusherEntryId>,
    mailbox_capacity: usize,
) -> Arc<CellSlot> {
    Arc::new(CellSlot {
        state: Mutex::new(state),
        mailbox: Arc::new(Mailbox::with_capacity(mailbox_capacity)),
        dispatcher,
        tasklet_id: args.tasklet_id,
        cell_key: parts.cell_key,
        instance_id: args.instance_id,
        instance: args.instance,
        topology: args.topology,
        waiters: Waiters::new(),
        drain_active: AtomicBool::new(false),
        flush_buffer,
        keeps_state: args.keeps_state,
        change_seq: parts.change_seq,
        consumed_floor: AtomicI64::new(-1),
        event_time_high_water: parts.event_time_high_water,
        replay_buffer: Mutex::new(Vec::new()),
        replay_open: AtomicBool::new(false),
        replay_depth: AtomicU64::new(0),
        cell_identity: parts.cell_identity,
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
