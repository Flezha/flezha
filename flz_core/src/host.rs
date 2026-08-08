//! The erased host surface: what a non-Rust SDK declares and drives through
//! the C ABI (`flz_core_ffi`).
//!
//! A host class is a topology declared as data — no generics, no host
//! closures beyond the bytes-passthrough entry wrapper. Its entities carry a
//! [`BytesBuffer`] (host-encoded change bytes, core-framed) and a
//! [`HostDispatcher`] that answers the engine's metadata questions;
//! invocations never run in-process — the host polls
//! [`crate::poll_core::PollCore`], runs its own user code, and completes with
//! a [`crate::schema::ActivationResult`] this module applies.
//!
//! v1 scope: single-mode procedures fed by entry channels, wall-clock `now`,
//! processing-time timers, plugin-bound outputs. Restore folds, live state
//! reads, event-time clocks and inter-procedure edges for host classes land
//! with the first full host SDK (they need the remaining activation kinds
//! wired through poll).

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::channel::{
    BuildEntityArgs, ChannelDescriptor, ChannelKind, OverflowPolicy, RetryPolicy,
};
use crate::entity::{EntityItem, EntitySlot, ItemDisposition, ProcedureDispatcher};
use crate::flusher::{EntityFlusher, FlushFrame, FlushableBuffer};
use crate::header::HeaderTemplate;
use crate::operative::topology::{
    AckGranularity, CorrectionPolicy, EntityMode, OperativeTopology, ProcedureDescriptor,
};
use crate::poll_core::{EngineActivation, PollCore, WorkItem};
use crate::schema::{self, TimerOp};
use crate::timer::PendingTimers;

/// One procedure of a host class.
pub struct HostProcedureDecl {
    pub name: String,
    /// The entry channel feeding it (v1: exactly one, bytes passthrough).
    pub entry_channel: String,
    pub retry: RetryPolicy,
    pub mailbox_capacity: usize,
    /// Plugin-bound output channel, when the procedure emits to the wire.
    pub output_plugin: Option<String>,
}

/// A host class declaration — the C ABI's JSON deserializes into this.
pub struct HostClassDecl {
    pub factory_id: String,
    pub procedures: Vec<HostProcedureDecl>,
    /// Stamped into every durable record header (byte 5..9).
    pub schema_version: u32,
    /// Whether change bytes in completions are flushed to the controller's
    /// per-Entity WAL. When false they are dropped (stateless class).
    pub persistent: bool,
}

/// Build and register a host class under its factory id. The controller's
/// `AddOperative` then instantiates it exactly like a typed class.
pub fn register_host_class(decl: HostClassDecl) -> Result<(), String> {
    if decl.procedures.is_empty() {
        return Err("a host class declares at least one procedure".to_string());
    }
    let factory_id: &'static str = Box::leak(decl.factory_id.clone().into_boxed_str());
    let schema_version = decl.schema_version;
    let persistent = decl.persistent;

    let mut channels: Vec<ChannelDescriptor> = Vec::new();
    let mut name_to_id: HashMap<&'static str, u32> = HashMap::new();
    let mut procedures: Vec<ProcedureDescriptor> = Vec::new();
    let mut procedure_name_to_id: HashMap<&'static str, u32> = HashMap::new();

    for (proc_id, proc_decl) in decl.procedures.iter().enumerate() {
        let proc_id = proc_id as u32;
        let entry_name: &'static str = Box::leak(proc_decl.entry_channel.clone().into_boxed_str());
        let entry_id = channels.len() as u32;
        channels.push(ChannelDescriptor {
            name: Arc::from(entry_name),
            kind: ChannelKind::Entry,
            producer: None,
            consumers: vec![crate::channel::ConsumerEdge {
                procedure: proc_id,
                transform: None,
                key: None,
            }],
            plugin_name: None,
            // Bytes passthrough: the host decodes its own payloads.
            wrap_input: Some(Arc::new(|bytes: Vec<u8>| {
                Ok(Arc::new(bytes) as Arc<dyn Any + Send + Sync>)
            })),
            entry_parsed: false,
            plugin_serializer: None,
            plugin_sink: None,
            keyed: false,
            overflow_policy: OverflowPolicy::DropNewest,
            retry: proc_decl.retry,
            codec: None,
        });
        name_to_id.insert(entry_name, entry_id);

        let output_channel_id = proc_decl.output_plugin.as_ref().map(|plugin| {
            let out_name: &'static str = Box::leak(plugin.clone().into_boxed_str());
            let out_id = channels.len() as u32;
            channels.push(ChannelDescriptor {
                name: Arc::from(out_name),
                kind: ChannelKind::PluginBound,
                producer: Some(proc_id),
                consumers: Vec::new(),
                plugin_name: Some(Arc::from(out_name)),
                wrap_input: None,
                entry_parsed: false,
                // The host completes with already-encoded bytes; nothing to
                // serialize core-side.
                plugin_serializer: Some(Arc::new(|value: &dyn Any| {
                    value.downcast_ref::<Vec<u8>>().cloned().unwrap_or_default()
                })),
                plugin_sink: None,
                keyed: false,
                overflow_policy: OverflowPolicy::DropNewest,
                retry: RetryPolicy::default(),
                codec: None,
            });
            name_to_id.insert(out_name, out_id);
            out_id
        });

        let proc_name: &'static str = Box::leak(proc_decl.name.clone().into_boxed_str());
        let facts = Arc::new(HostProcedureFacts {
            name: proc_name,
            retry: proc_decl.retry,
            schema_version,
            persistent,
        });
        procedures.push(ProcedureDescriptor {
            name: Arc::from(proc_name),
            entity_keys: Vec::new(),
            entities_for_overridden: false,
            compensate_overridden: false,
            mode: EntityMode::Single,
            idle_ttl: None,
            max_entities: usize::MAX,
            input_channel_ids: vec![entry_id],
            output_channel_id,
            input_type_id: TypeId::of::<Vec<u8>>(),
            output_type_id: TypeId::of::<Vec<u8>>(),
            mailbox_capacity: proc_decl.mailbox_capacity,
            mailbox_capacity_overridden: false,
            compact_after_bytes: crate::persistence::DEFAULT_COMPACT_AFTER_BYTES,
            build_entity: Box::new(move |args: BuildEntityArgs<'_>| {
                build_host_entity_slot(args, Arc::clone(&facts))
            }),
            entities_for_erased: Arc::new(|_| Vec::new()),
            serialize_state: Arc::new(|_| {
                Err("host-held state: a live read reaches the host through its \
                     LiveStateRead activation, not an in-process serializer"
                    .to_string())
            }),
            deserialize_state: Arc::new(|_| {
                Err("host-held state: apply-state reaches the host through its \
                     ApplyState activation"
                    .to_string())
            }),
        });
        procedure_name_to_id.insert(proc_name, proc_id);
    }

    let topology = Arc::new(OperativeTopology {
        channels,
        name_to_id,
        procedures,
        procedure_name_to_id,
        factory_id,
        persistence_enabled_const: persistent,
        effectively_once: false,
        correction: None,
        ack_granularity: AckGranularity::default(),
    });
    crate::operative::registry::register_class(factory_id, topology);
    Ok(())
}

/// The per-procedure facts a [`HostDispatcher`] answers with.
struct HostProcedureFacts {
    name: &'static str,
    retry: RetryPolicy,
    schema_version: u32,
    persistent: bool,
}

/// Mirror of the typed `build_entity_slot` for a host-run procedure: the
/// shared kernel assembly (`entity::assemble_slot`), a [`BytesBuffer`]
/// instead of a `TypedBuffer<S>`, a [`HostDispatcher`] instead of the typed
/// impl.
fn build_host_entity_slot(
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
    let header = HeaderTemplate::new(crate::header::parse_pkg_version(), facts.schema_version);
    let flusher_entry_id = facts.persistent.then(|| {
        EntityFlusher::instance().register(
            Arc::clone(&parts.entity_identity),
            Arc::clone(&flush_buffer),
            args.persistence_enabled,
            Arc::clone(&parts.change_seq),
            header,
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

impl ProcedureDispatcher for HostDispatcher {
    fn dispatch_item(
        &self,
        _work: WorkItem,
        _state: &mut Box<dyn Any + Send>,
        _flush_buffer: &mut Box<dyn FlushableBuffer>,
        _timers: &mut PendingTimers,
        _instance: &Weak<crate::operative::instance::UnboundedInstance>,
    ) -> ItemDisposition {
        tracing::error!(
            target: "flezha::host",
            entity = %self.entity_label,
            "in-process dispatch reached a host-run procedure; item dropped"
        );
        ItemDisposition::Done { ops: Vec::new() }
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
        false
    }

    fn correction_policy(&self) -> Option<CorrectionPolicy> {
        None
    }

    fn entity_label(&self) -> &str {
        &self.entity_label
    }

    fn event_time_of(&self, _payload: &crate::entity::EntityPayload) -> Option<u64> {
        None
    }

    fn run_collecting(
        &self,
        _input: Arc<dyn Any + Send + Sync>,
        _state: &mut Box<dyn Any + Send>,
        _topology: &OperativeTopology,
        _sink: &mut dyn crate::bounded::BoundedCollectSink,
    ) -> Result<(), crate::bounded::BoundedError> {
        Err(crate::bounded::BoundedError::from(
            "host-run procedures have no in-process collector",
        ))
    }

    fn compensate_collecting(
        &self,
        _outputs: Vec<Arc<dyn Any + Send + Sync>>,
        _state: &mut Box<dyn Any + Send>,
        _topology: &OperativeTopology,
        _sink: &mut dyn crate::bounded::BoundedCollectSink,
    ) -> Result<(), crate::bounded::BoundedError> {
        Err(crate::bounded::BoundedError::from(
            "host-run procedures have no in-process compensator",
        ))
    }

    fn restore_state(
        &self,
        _state: &mut Box<dyn Any + Send>,
        _timers: &mut PendingTimers,
        _snapshot_bytes: &[u8],
        _change_log_bytes: &[u8],
        _entity_key: Option<&str>,
    ) -> Result<u64, crate::header::RestoreError> {
        Err(crate::header::RestoreError::SnapshotDecode(
            "host-held state: restore reaches the host through its RestoreFold \
             activation (not yet wired for host classes)"
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
             SnapshotRequest activation (not yet wired for host classes)"
            .to_string())
    }
}

/// Host-encoded per-Entity flush buffer: change bodies arrive pre-encoded in
/// completions; the core frames and ships them exactly as it does a typed
/// drain.
#[derive(Default)]
pub struct BytesBuffer {
    pub changes: Vec<Vec<u8>>,
    pub timer_ops: Vec<TimerOp>,
}

impl FlushableBuffer for BytesBuffer {
    fn drain_to_frames(
        &mut self,
        entity_identity: &[u8],
        change_seq: &AtomicU64,
        header: &HeaderTemplate,
    ) -> (usize, Vec<FlushFrame>) {
        let frames = crate::flusher::frame_change_bodies(
            std::mem::take(&mut self.changes),
            std::mem::take(&mut self.timer_ops),
            entity_identity,
            change_seq,
            header,
        );
        (frames.len(), frames)
    }

    fn is_empty(&self) -> bool {
        self.changes.is_empty() && self.timer_ops.is_empty()
    }

    fn discard_staged(&mut self) {}

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Translate a handed-out engine activation into the schema form the C ABI
/// ships. Wall-clock `now` is stamped here — the host never reads ambient
/// time for `ctx.now()`.
pub fn to_schema(act: &EngineActivation) -> schema::Activation {
    let label = act.slot.dispatcher.entity_label();
    let mut parts = label.splitn(3, '/');
    let operative = parts.next().unwrap_or("").to_string();
    let _procedure_name = parts.next();
    let entity_key = act.slot.entity_key.as_deref().map(str::to_string);
    let now_wall = crate::util::wall_now_ms();
    let items = act
        .items
        .iter()
        .map(|work| match &work.item {
            EntityItem::Input(payload) => schema::WorkItem::Input(schema::InputItem {
                channel: input_channel_of(&act.slot),
                payload: payload
                    .value
                    .downcast_ref::<Vec<u8>>()
                    .cloned()
                    .unwrap_or_default(),
                metadata: Vec::new(),
                consumed_offset: payload.consumed_offset,
                replay: payload.replay,
                now: work.now.unwrap_or(now_wall),
            }),
            EntityItem::Timer(fire) => schema::WorkItem::Timer(schema::TimerItem {
                kind: fire.kind,
                scheduled_at: fire.scheduled_at,
            }),
        })
        .collect();
    schema::Activation {
        token: act.token,
        target: schema::EntityRef {
            operative,
            procedure: act.slot.procedure_id,
            entity_key,
        },
        kind: schema::ActivationKind::Work(items),
    }
}

fn input_channel_of(slot: &EntitySlot) -> u32 {
    slot.topology
        .procedure(slot.procedure_id)
        .and_then(|p| p.input_channel_ids.first().copied())
        .unwrap_or(0)
}

/// Apply a host completion to its outstanding activation: buffer the change
/// bytes, fold and arm timer ops, ship plugin outputs, then drive the
/// engine's completion protocol (release, ladder, or park).
pub fn apply_result(
    core: &PollCore,
    act: EngineActivation,
    result: schema::ActivationResult,
) -> Result<(), String> {
    let EngineActivation { token, slot, items } = act;
    let effects = match result.outcome {
        schema::ActivationOutcome::Work(effects) => effects,
        schema::ActivationOutcome::Failed(err) => {
            // Whole-activation failure: ladder the head item with the rest
            // behind it, or park it when non-retryable/final.
            let head_final = items.first().map(|w| w.is_final).unwrap_or(false);
            if err.non_retryable || head_final {
                if let Some(head) = items.first() {
                    park_item(&slot, head, &err.message, 1);
                }
                let rest: Vec<WorkItem> = items.into_iter().skip(1).collect();
                if rest.is_empty() {
                    core.complete(token);
                } else {
                    core.retry_after_failure(token, rest, err.message);
                }
            } else {
                core.retry_after_failure(token, items, err.message);
            }
            return Ok(());
        }
        _ => return Err("work activation answered with a non-work outcome".to_string()),
    };

    let mut queue: std::collections::VecDeque<WorkItem> = items.into();
    for effect in effects {
        let Some(work) = queue.pop_front() else {
            return Err("completion carries more effects than items".to_string());
        };
        match effect.verdict {
            schema::Verdict::Ok => {
                apply_item_effect(&slot, &work, effect);
                crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
            }
            schema::Verdict::RetryableError { message } => {
                if work.is_final {
                    park_item(&slot, &work, &message, work.attempt);
                    crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let mut rest = Vec::with_capacity(queue.len() + 1);
                rest.push(work);
                rest.extend(queue);
                core.retry_after_failure(token, rest, message);
                return Ok(());
            }
            schema::Verdict::NonRetryableError { message } => {
                park_item(&slot, &work, &message, work.attempt);
                crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    core.complete(token);
    Ok(())
}

/// One item's effects: change bytes into the flush buffer (persistent
/// classes), timer ops folded into the pending set and armed on the wheel,
/// outputs shipped to their plugin channels.
fn apply_item_effect(slot: &Arc<EntitySlot>, work: &WorkItem, effect: schema::ItemEffect) {
    let stream = match &work.item {
        EntityItem::Input(p) => Arc::clone(&p.stream),
        EntityItem::Timer(f) => Arc::clone(&f.stream),
    };
    {
        use crate::util::LockRecover;
        let mut buffer = slot.flush_buffer.lock_recover();
        if let Some(bytes) = buffer.as_any_mut().downcast_mut::<BytesBuffer>() {
            if slot.flusher_entry_id.is_some() {
                bytes.changes.extend(effect.changes);
                bytes.timer_ops.extend(effect.timers.iter().copied());
            }
        }
    }
    if !effect.timers.is_empty() {
        {
            let mut pending = {
                use crate::util::LockRecover;
                slot.timers.lock_recover()
            };
            for op in &effect.timers {
                pending.apply(op);
            }
        }
        crate::timer::apply_ops(slot, &effect.timers, &stream);
    }
    for output in effect.outputs {
        match output.target {
            schema::OutputTarget::Plugin { channel } => {
                let mut metadata: HashMap<String, String> = output.metadata.into_iter().collect();
                metadata.insert("channel".to_string(), channel);
                crate::routing::send_plugin_frame(
                    &stream,
                    slot.dispatcher.entity_label(),
                    &output.payload,
                    &metadata,
                );
            }
            schema::OutputTarget::Edge { .. } => {
                tracing::warn!(
                    target: "flezha::host",
                    entity = slot.dispatcher.entity_label(),
                    "inter-procedure outputs for host classes are not wired yet; output dropped"
                );
            }
        }
    }
}

/// Park a poison item through the tier-off DLQ path, exactly as the typed
/// layer does on a final failed attempt.
fn park_item(slot: &Arc<EntitySlot>, work: &WorkItem, error: &str, attempts: u32) {
    crate::park::PARKED_TOTAL.fetch_add(1, Ordering::Relaxed);
    let (channel, payload, consumed_offset) = match &work.item {
        EntityItem::Input(p) => (
            slot.topology
                .procedure(slot.procedure_id)
                .and_then(|d| d.input_channel_ids.first())
                .and_then(|&id| slot.topology.channel(id))
                .map(|c| c.name.to_string())
                .unwrap_or_else(|| "<unwired>".to_string()),
            p.value.downcast_ref::<Vec<u8>>().cloned(),
            p.consumed_offset,
        ),
        EntityItem::Timer(fire) => {
            // A poison firing still records its removal so it parks once.
            let op = TimerOp::cancel(fire.kind, fire.scheduled_at);
            {
                use crate::util::LockRecover;
                slot.timers.lock_recover()
            }
            .apply(&op);
            ("<timer>".to_string(), None, None)
        }
    };
    crate::park::park_tier_off(
        &slot.entity_identity,
        slot.dispatcher.entity_label(),
        consumed_offset,
        crate::flusher::DlqEntryBody {
            channel,
            error: error.to_string(),
            attempts,
            payload,
        },
    );
}
