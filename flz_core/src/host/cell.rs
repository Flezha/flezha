//! The host-run Cell: the payload it carries, the declared facts its
//! dispatcher answers with, and the slot assembly over the shared kernel. Its
//! invariant: no host code runs in-process — every seam a typed dispatcher
//! serves in-process is a crossing here, and the metadata half is all the
//! `HostDispatcher` answers.

use std::any::Any;
use std::sync::{Arc, Mutex, Weak};

use super::decl::HostClock;
use crate::cell::{CellSlot, ItemDisposition, TaskletDispatcher};
use crate::channel::{BuildCellArgs, RetryPolicy};
use crate::flusher::{CellFlusher, FlushableBuffer};
use crate::header::HeaderTemplate;
use crate::poll_core::ItemAttempt;
use crate::relay::topology::CorrectionPolicy;
use crate::schema;
use crate::timer::PendingTimers;
use crate::util::LockRecover;

/// An entry payload as the host layer carries it — the bytes plus what the
/// core resolved about them; the erased value inside a host class's
/// `CellPayload`.
pub struct HostPayload {
    pub bytes: Vec<u8>,
    pub channel: crate::channel::ChannelId,
    /// The Cells the payload reaches: one per key its host answered.
    pub keys: Vec<String>,
    pub event_time: Option<u64>,
    pub metadata: Vec<(String, String)>,
}

/// The per-tasklet facts a [`HostDispatcher`] answers with; `Some` from
/// `host_facts` is what marks a slot as host-run.
pub struct HostTaskletFacts {
    pub name: &'static str,
    pub factory: &'static str,
    pub retry: RetryPolicy,
    /// Stamped into this tasklet's durable records.
    pub schema_version: u32,
    /// The oldest stamped version this tasklet still reads.
    pub min_readable_schema_version: u32,
    pub persistent: bool,
    /// Completions produce effect records and outputs ride the outbox.
    pub effectively_once: bool,
    /// The open interval's changes stage until a completion checkpoints.
    pub checkpoint: Option<crate::relay::topology::CheckpointPolicy>,
    pub clock: HostClock,
    pub sdk_lang: u8,
    /// The writing SDK's own version, as declared; never the core's.
    pub sdk_version: u32,
    pub change_format_id: u8,
    pub correction: Option<CorrectionPolicy>,
}

impl HostTaskletFacts {
    /// The durable-record header, on the host's declared lang and format.
    pub fn header(&self) -> HeaderTemplate {
        HeaderTemplate {
            sdk_lang: self.sdk_lang,
            sdk_version: self.sdk_version,
            schema_version: self.schema_version,
            format_id: self.change_format_id,
        }
    }
}

/// The typed `build_cell_slot`'s mirror: the shared assembly with a
/// [`BytesBuffer`] and a [`HostDispatcher`].
pub(super) fn build_host_cell_slot(
    args: BuildCellArgs<'_>,
    facts: Arc<HostTaskletFacts>,
) -> Arc<CellSlot> {
    let tasklet_desc = args
        .topology
        .tasklet(args.tasklet_id)
        .expect("build_host_cell_slot: tasklet id missing in topology");
    let mailbox_capacity = tasklet_desc.mailbox_capacity;
    let tasklet_name = tasklet_desc.name.to_string();
    let parts = crate::cell::slot_parts(args.instance_id.as_ref(), &tasklet_name, args.cell_key);
    let flush_buffer: Arc<Mutex<Box<dyn FlushableBuffer>>> = Arc::new(Mutex::new(
        Box::<BytesBuffer>::default() as Box<dyn FlushableBuffer>,
    ));
    let header = facts.header();
    let flusher_entry_id = facts.persistent.then(|| {
        CellFlusher::instance().register(
            Arc::clone(&parts.cell_identity),
            args.prefix.clone(),
            Arc::clone(&flush_buffer),
            args.keeps_state,
            Arc::clone(&parts.change_seq),
            header,
            Arc::clone(&parts.push_deferred),
        )
    });
    let dispatcher = Arc::new(HostDispatcher {
        tasklet_id: args.tasklet_id,
        cell_label: Arc::clone(&parts.cell_label),
        facts,
    }) as Arc<dyn TaskletDispatcher>;
    crate::cell::assemble_slot(
        args,
        parts,
        Box::new(()) as Box<dyn Any + Send>,
        flush_buffer,
        dispatcher,
        flusher_entry_id,
        mailbox_capacity,
    )
}

/// The metadata half of the dispatcher trait; `dispatch_item` is
/// unreachable, since work leaves through the C ABI's poll.
struct HostDispatcher {
    tasklet_id: u32,
    cell_label: Arc<str>,
    facts: Arc<HostTaskletFacts>,
}

impl TaskletDispatcher for HostDispatcher {
    fn dispatch_item(
        &self,
        _work: ItemAttempt,
        _slot: &Arc<CellSlot>,
        _state: &mut Box<dyn Any + Send>,
        _timers: &mut PendingTimers,
        _instance: &Weak<crate::relay::instance::RelayInstance>,
    ) -> ItemDisposition {
        tracing::error!(
            target: "flezha::host",
            cell = %self.cell_label,
            "in-process dispatch reached a host-run tasklet; item dropped"
        );
        ItemDisposition::Done { ops: Vec::new() }
    }

    fn host_facts(&self) -> Option<&HostTaskletFacts> {
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

    fn write_timer_ops(&self, slot: &Arc<CellSlot>, ops: Vec<crate::timer::TimerOp>) {
        let mut buffer = slot.flush_buffer.lock_recover();
        if let Some(bytes) = buffer.as_any_mut().downcast_mut::<BytesBuffer>() {
            bytes.timer_ops.extend(ops);
        }
        crate::flusher::flush_one(
            &mut **buffer,
            &slot.cell_identity,
            &slot.change_seq,
            &self.facts.header(),
            slot.keeps_state,
        );
    }

    fn interval_deadline(
        &self,
        flush_buffer: &mut Box<dyn crate::flusher::FlushableBuffer>,
    ) -> Option<u64> {
        let policy = self.facts.checkpoint?;
        let bytes = flush_buffer.as_any_mut().downcast_mut::<BytesBuffer>()?;
        bytes.staged.deadline_ms(policy)
    }

    fn tasklet_id(&self) -> u32 {
        self.tasklet_id
    }

    fn tasklet_name(&self) -> &'static str {
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

    fn cell_label(&self) -> &str {
        &self.cell_label
    }

    fn event_time_of(&self, payload: &crate::cell::CellPayload) -> Option<u64> {
        payload
            .value
            .downcast_ref::<HostPayload>()
            .and_then(|p| p.event_time)
    }

    fn restore_state(
        &self,
        _state: &mut Box<dyn Any + Send>,
        _timers: &mut PendingTimers,
        _snapshot_header: &[u8],
        _snapshot_bytes: &[u8],
        _change_log_bytes: &[u8],
        _cell_key: Option<&str>,
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
    ) -> Result<(Vec<u8>, u64, usize), crate::Error> {
        // Compaction reaches the host through its SnapshotRequest activation
        // (`snapshot_frame_crossing`), never this in-process seam.
        Err(crate::Error::HostCrossing(
            crate::Code::HostStateInProcess.with([]),
        ))
    }
}

/// The one flush buffer, on the host's pre-encoded bytes.
pub type BytesBuffer = crate::flusher::EffectBuffer<crate::flusher::RawBodies>;

/// The schema address of a slot.
pub fn cell_ref_of(slot: &CellSlot) -> schema::CellRef {
    schema::CellRef {
        relay: slot.instance_id.to_string(),
        tasklet: slot.tasklet_id,
        cell_key: slot.cell_key.as_deref().map(str::to_string),
    }
}

pub(super) fn host_serialize_unreachable(
    _: &Box<dyn Any + Send>,
) -> Result<serde_json::Value, String> {
    Err("host-held state: a live read reaches the host through its \
         LiveStateRead activation, not an in-process serializer"
        .to_string())
}

pub(super) fn host_deserialize_unreachable(
    _: &serde_json::Value,
) -> Result<Box<dyn Any + Send>, String> {
    Err("host-held state: apply-state reaches the host through its \
         ApplyState activation"
        .to_string())
}

/// The journal codec of a host inter-task channel: the payload's routing
/// facts, so a re-seed rebuilds the exact `HostPayload`.
pub(super) fn host_payload_codec(channel_id: u32) -> crate::channel::ChannelCodec {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct JournalForm {
        bytes: Vec<u8>,
        keys: Vec<String>,
        event_time: Option<u64>,
    }
    crate::channel::ChannelCodec {
        encode: Arc::new(|value: &dyn Any| {
            let payload = value
                .downcast_ref::<HostPayload>()
                .ok_or("host codec: value is not a host payload")?;
            postcard::to_stdvec(&JournalForm {
                bytes: payload.bytes.clone(),
                keys: payload.keys.clone(),
                event_time: payload.event_time,
            })
            .map_err(|e| e.to_string())
        }),
        decode: Arc::new(move |bytes: Vec<u8>| {
            let form: JournalForm = postcard::from_bytes(&bytes).map_err(|e| e.to_string())?;
            Ok(Arc::new(HostPayload {
                bytes: form.bytes,
                channel: channel_id,
                keys: form.keys,
                event_time: form.event_time,
                metadata: Vec::new(),
            }) as Arc<dyn Any + Send + Sync>)
        }),
    }
}
