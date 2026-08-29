//! The host-run Entity: the payload it carries, the declared facts its
//! dispatcher answers with, and the slot assembly over the shared kernel. Its
//! invariant: no host code runs in-process — every seam a typed dispatcher
//! serves in-process is a crossing here, and the metadata half is all the
//! `HostDispatcher` answers.

use std::any::Any;
use std::sync::{Arc, Mutex, Weak};

use super::decl::HostClock;
use crate::channel::{BuildEntityArgs, RetryPolicy};
use crate::entity::{EntitySlot, ItemDisposition, ProcedureDispatcher};
use crate::flusher::{EntityFlusher, FlushableBuffer};
use crate::header::HeaderTemplate;
use crate::operative::topology::{CorrectionPolicy, OperativeTopology};
use crate::poll_core::ItemAttempt;
use crate::schema;
use crate::timer::PendingTimers;

/// One entry payload as the host layer carries it between the wire, the
/// prepare crossing and the mailbox: the raw bytes plus everything the core
/// resolved about them. This is the `Arc<dyn Any>` value inside a host
/// class's `EntityPayload`.
pub struct HostPayload {
    pub bytes: Vec<u8>,
    /// The channel the value arrived on (entry) or was produced onto
    /// (inter-proc edge).
    pub channel: crate::channel::ChannelId,
    /// Routing keys: exactly one for a dynamic consumer, any number for a
    /// static fan-out (empty = every declared entity), unused for single.
    pub keys: Vec<String>,
    /// The extracted event time (event-time classes; from the prepare
    /// crossing for entry payloads, from the producing completion for
    /// inter-proc values).
    pub event_time: Option<u64>,
    /// Wire metadata worth carrying to the host, if any.
    pub metadata: Vec<(String, String)>,
}

/// The per-procedure facts a [`HostDispatcher`] answers with. `Some` from
/// [`crate::entity::ProcedureDispatcher::host_facts`] is what marks a slot as
/// host-run: its command interactions cross as queued activations instead of
/// in-process calls.
pub struct HostProcedureFacts {
    pub name: &'static str,
    /// The declaring class — stamped on every activation for this procedure.
    pub factory: &'static str,
    pub retry: RetryPolicy,
    /// Stamped into this procedure's durable records; per procedure, never a
    /// class-wide maximum.
    pub schema_version: u32,
    /// The oldest stamped version this procedure still reads; equal to
    /// `schema_version` unless the host declares migrations.
    pub min_readable_schema_version: u32,
    pub persistent: bool,
    /// Effectively-once tier: completions produce effect records (changes +
    /// outputs + consumed offset in ONE durable record) instead of plain
    /// change frames, and outputs ride the transactional outbox.
    pub effectively_once: bool,
    /// Checkpoint ack-granularity: the open interval's changes stage until a
    /// completion carries `checkpoint` (or a park closes the interval).
    pub checkpoint: Option<crate::operative::topology::CheckpointPolicy>,
    /// The declared clock; drives `now` stamping and event-time admission.
    pub clock: HostClock,
    /// Header lang byte the host's durable records claim (Rust 1, Java 2).
    pub sdk_lang: u8,
    /// Header format byte for plain change bodies (postcard 1, Kryo 4).
    pub change_format_id: u8,
    /// Late-input correction, present only on event-time-clocked procedures.
    pub correction: Option<CorrectionPolicy>,
}

impl HostProcedureFacts {
    /// The durable-record header for this procedure's frames — the host's
    /// declared lang/format instead of the typed constructor's Rust ones.
    pub fn header(&self) -> HeaderTemplate {
        HeaderTemplate {
            sdk_lang: self.sdk_lang,
            sdk_version: crate::header::parse_pkg_version(),
            schema_version: self.schema_version,
            format_id: self.change_format_id,
        }
    }
}

/// Mirror of the typed `build_entity_slot` for a host-run procedure: the
/// shared kernel assembly (`entity::assemble_slot`), a [`BytesBuffer`]
/// instead of a `TypedBuffer<S>`, a [`HostDispatcher`] instead of the typed
/// impl.
pub(super) fn build_host_entity_slot(
    args: BuildEntityArgs<'_>,
    facts: Arc<HostProcedureFacts>,
) -> Arc<EntitySlot> {
    let procedure_desc = args
        .topology
        .procedure(args.procedure_id)
        .expect("build_host_entity_slot: procedure id missing in topology");
    let mailbox_capacity = procedure_desc.mailbox_capacity;
    let procedure_name = procedure_desc.name.to_string();
    let parts =
        crate::entity::slot_parts(args.instance_id.as_ref(), &procedure_name, args.entity_key);
    let flush_buffer: Arc<Mutex<Box<dyn FlushableBuffer>>> = Arc::new(Mutex::new(
        Box::<BytesBuffer>::default() as Box<dyn FlushableBuffer>,
    ));
    let header = facts.header();
    let flusher_entry_id = facts.persistent.then(|| {
        EntityFlusher::instance().register(
            Arc::clone(&parts.entity_identity),
            Arc::clone(&flush_buffer),
            args.persistence_enabled,
            Arc::clone(&parts.change_seq),
            header,
            Arc::clone(&parts.push_deferred),
        )
    });
    let dispatcher = Arc::new(HostDispatcher {
        procedure_id: args.procedure_id,
        entity_label: Arc::clone(&parts.entity_label),
        facts,
    }) as Arc<dyn ProcedureDispatcher>;
    crate::entity::assemble_slot(
        args,
        parts,
        Box::new(()) as Box<dyn Any + Send>,
        flush_buffer,
        dispatcher,
        flusher_entry_id,
        mailbox_capacity,
    )
}

/// The metadata half of the dispatcher trait for a host-run procedure.
/// `dispatch_item` is unreachable in a host process — no in-process poll
/// worker exists to call it; work leaves through the C ABI's poll.
struct HostDispatcher {
    procedure_id: u32,
    entity_label: Arc<str>,
    facts: Arc<HostProcedureFacts>,
}

/// Why the in-process workflow seams are unreachable for a host class: its work
/// leaves as a step activation the host polls, and the answer folds through
/// [`super::fold_workflow_outcome`].
fn host_workflow_unreachable(what: &str) -> crate::workflow::WorkflowError {
    crate::workflow::WorkflowError::non_retryable(format!(
        "host-run procedure: {what} reaches the host as a workflow step activation, \
         never this in-process seam"
    ))
}

impl ProcedureDispatcher for HostDispatcher {
    fn dispatch_item(
        &self,
        _work: ItemAttempt,
        _slot: &Arc<EntitySlot>,
        _state: &mut Box<dyn Any + Send>,
        _flush_buffer: &mut Box<dyn FlushableBuffer>,
        _timers: &mut PendingTimers,
        _instance: &Weak<crate::operative::instance::StreamInstance>,
    ) -> ItemDisposition {
        tracing::error!(
            target: "flezha::host",
            entity = %self.entity_label,
            "in-process dispatch reached a host-run procedure; item dropped"
        );
        ItemDisposition::Done { ops: Vec::new() }
    }

    fn host_facts(&self) -> Option<&HostProcedureFacts> {
        Some(self.facts.as_ref())
    }

    fn close_interval(&self, flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>) -> bool {
        // Only a checkpoint class has an interval to close.
        if self.facts.checkpoint.is_none() {
            return false;
        }
        let Some(bytes) = flush_buffer.as_any_mut().downcast_mut::<BytesBuffer>() else {
            return false;
        };
        bytes.close_interval()
    }

    fn interval_deadline(
        &self,
        flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>,
    ) -> Option<u64> {
        let policy = self.facts.checkpoint?;
        let bytes = flush_buffer.as_any_mut().downcast_mut::<BytesBuffer>()?;
        bytes.staged.deadline_ms(policy)
    }

    fn procedure_id(&self) -> u32 {
        self.procedure_id
    }

    fn procedure_name(&self) -> &'static str {
        self.facts.name
    }

    fn retry_policy(&self) -> RetryPolicy {
        self.facts.retry
    }

    fn is_event_time(&self) -> bool {
        self.facts.clock == HostClock::EventTime
    }

    fn correction_policy(&self) -> Option<CorrectionPolicy> {
        self.facts.correction
    }

    fn entity_label(&self) -> &str {
        &self.entity_label
    }

    fn event_time_of(&self, payload: &crate::entity::EntityPayload) -> Option<u64> {
        payload
            .value
            .downcast_ref::<HostPayload>()
            .and_then(|p| p.event_time)
    }

    fn run_collecting(
        &self,
        _input: Arc<dyn Any + Send + Sync>,
        _state: &mut Box<dyn Any + Send>,
        _topology: &OperativeTopology,
        _sink: &mut dyn crate::workflow::WorkflowCollectSink,
    ) -> Result<(), crate::workflow::WorkflowError> {
        Err(host_workflow_unreachable("a stage batch"))
    }

    fn compensate_collecting(
        &self,
        _outputs: Vec<Arc<dyn Any + Send + Sync>>,
        _state: &mut Box<dyn Any + Send>,
        _topology: &OperativeTopology,
        _sink: &mut dyn crate::workflow::WorkflowCollectSink,
    ) -> Result<(), crate::workflow::WorkflowError> {
        Err(host_workflow_unreachable("a saga undo"))
    }

    fn restore_state(
        &self,
        _state: &mut Box<dyn Any + Send>,
        _timers: &mut PendingTimers,
        _snapshot_header: &[u8],
        _snapshot_bytes: &[u8],
        _change_log_bytes: &[u8],
        _entity_key: Option<&str>,
    ) -> Result<crate::restore::RestoreOutcome, crate::header::RestoreError> {
        Err(crate::header::RestoreError::SnapshotDecode(
            "host-held state: restore reaches the host through its RestoreFold \
             activation (`restore_fold_exclusive`), never this in-process seam"
                .to_string(),
        ))
    }

    fn snapshot_frame(
        &self,
        _state: &mut Box<dyn Any + Send>,
        _flush_buffer: &mut Box<dyn FlushableBuffer>,
        _timers: &PendingTimers,
    ) -> Result<(Vec<u8>, u64, usize), String> {
        Err("host-held state: compaction reaches the host through its \
             SnapshotRequest activation (`snapshot_frame_crossing`), never \
             this in-process seam"
            .to_string())
    }
}

/// Host-encoded per-Entity flush buffer: change bodies arrive pre-encoded in
/// completions; the core frames and ships them exactly as it does a typed
/// drain — the one buffer, on bytes.
pub type BytesBuffer = crate::flusher::EffectBuffer<crate::flusher::RawBodies>;

/// The schema address of a slot — the instance id, the numeric procedure id,
/// the key as declared.
pub fn entity_ref_of(slot: &EntitySlot) -> schema::EntityRef {
    schema::EntityRef {
        operative: slot.instance_id.to_string(),
        procedure: slot.procedure_id,
        entity_key: slot.entity_key.as_deref().map(str::to_string),
    }
}
