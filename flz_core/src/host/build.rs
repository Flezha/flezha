//! Building the checked declaration: the descriptors, the facts and the DAG a
//! validated record becomes.
//!
//! `register` judges and registers; this is the half that materialises. They
//! are apart because they answer different questions — "may this topology
//! exist" and "what does it become" — and the first is the seam every SDK
//! submits to.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use super::decl::{
    HostAckGranularity, HostBeyondWindow, HostChannelDecl, HostChannelKind, HostCheckpointOutputs,
    HostClassDecl, HostClock, HostEntityModeDecl, HostOverflow, HostProcedureDecl,
    HostRollbackMode, HostWorkflowDecl,
};
use super::entity::{
    build_host_entity_slot, host_deserialize_unreachable, host_payload_codec,
    host_serialize_unreachable, HostPayload, HostProcedureFacts,
};
use super::register::DerivedChannel;
use crate::channel::{
    ChannelDescriptor, ChannelKind, EntryWrapFn, OverflowPolicy, ProcedureId, RetryPolicy,
};
use crate::error::Error;
use crate::operative::topology::{
    AckGranularity, BeyondWindowPolicy, CheckpointOutputs, CheckpointPolicy, CorrectionPolicy,
    EntityMode, OperativeTopology, ProcedureDescriptor,
};
use crate::workflow::spec::{graph_order, GraphOrder, GraphSeed, ProcDag, WorkflowSpec};
use crate::workflow::{LoopMode, RollbackMode};

/// The class facts every procedure and the topology read, resolved once so
/// they agree.
pub(super) struct ClassFacts {
    pub(super) factory_id: &'static str,
    /// The declared version packed as the durable header carries it, resolved
    /// once so every procedure of the class stamps the same bytes.
    pub(super) sdk_version: u32,
    pub(super) correction: Option<CorrectionPolicy>,
    pub(super) ack_granularity: AckGranularity,
    pub(super) compact_after_bytes: usize,
}

pub(super) fn resolve_class_facts(decl: &HostClassDecl, ack: HostAckGranularity) -> ClassFacts {
    let correction = decl.correction_window_ms.map(|window_ms| CorrectionPolicy {
        window_ms,
        beyond_window: match decl.beyond_window_policy {
            HostBeyondWindow::ProcessAsArrival => BeyondWindowPolicy::ProcessAsArrival,
            HostBeyondWindow::Drop => BeyondWindowPolicy::Drop,
        },
    });
    let ack_granularity = match ack {
        HostAckGranularity::PerInput => AckGranularity::PerInput,
        HostAckGranularity::Checkpoint => AckGranularity::Checkpoint(CheckpointPolicy {
            every: decl.checkpoint_every,
            outputs: match decl.checkpoint_outputs {
                HostCheckpointOutputs::AsProduced => CheckpointOutputs::AsProduced,
                HostCheckpointOutputs::AtCheckpoint => CheckpointOutputs::AtCheckpoint,
            },
            max_delay_ms: decl.checkpoint_max_delay_ms,
        }),
    };
    ClassFacts {
        factory_id: Box::leak(decl.factory_id.clone().into_boxed_str()),
        sdk_version: crate::header::pack_version(&decl.sdk_version),
        correction,
        ack_granularity,
        compact_after_bytes: decl
            .compact_after_bytes
            .unwrap_or(crate::operative::topology::DEFAULT_COMPACT_AFTER_BYTES),
    }
}
/// Every channel descriptor, and the per-operative name index the runtime
/// resolves a channel by. The names are the DERIVED ones, so a `named`
/// override reaches the runtime and the index at once.
pub(super) fn build_channels(
    decl: &HostClassDecl,
    derived: &[DerivedChannel],
) -> (Vec<ChannelDescriptor>, HashMap<&'static str, u32>) {
    let mut channels = Vec::with_capacity(derived.len());
    let mut name_to_id = HashMap::new();
    for (channel_id, derived_channel) in derived.iter().enumerate() {
        let channel_id = channel_id as u32;
        let name: &'static str = Box::leak(derived_channel.name.clone().into_boxed_str());
        channels.push(build_channel(
            channel_id,
            name,
            &decl.channels[channel_id as usize],
            derived_channel,
        ));
        name_to_id.insert(name, channel_id);
    }
    (channels, name_to_id)
}

/// Every procedure descriptor and its name index, built after the channels
/// because a procedure's retry ladder is its first input channel's.
pub(super) fn build_procedures(
    decl: &HostClassDecl,
    class: &ClassFacts,
    channels: &[ChannelDescriptor],
    derived: &[DerivedChannel],
) -> (Vec<ProcedureDescriptor>, HashMap<&'static str, u32>) {
    let mut procedures = Vec::with_capacity(decl.procedures.len());
    let mut name_to_id = HashMap::new();
    for (proc_id, proc_decl) in decl.procedures.iter().enumerate() {
        let proc_id = proc_id as u32;
        let proc_name: &'static str = Box::leak(proc_decl.name.clone().into_boxed_str());
        procedures.push(build_procedure(
            derived, class, channels, proc_id, proc_name, proc_decl, decl,
        ));
        name_to_id.insert(proc_name, proc_id);
    }
    (procedures, name_to_id)
}

fn build_channel(
    channel_id: u32,
    name: &'static str,
    channel_decl: &HostChannelDecl,
    derived: &DerivedChannel,
) -> ChannelDescriptor {
    let kind = match derived.kind {
        HostChannelKind::Entry => ChannelKind::Entry,
        HostChannelKind::InterProc => ChannelKind::InterProcedure,
        HostChannelKind::Plugin => ChannelKind::PluginBound,
    };
    ChannelDescriptor {
        name: Arc::from(name),
        kind,
        producer: derived.producer,
        consumers: derived
            .consumers
            .iter()
            .map(|edge| crate::channel::ConsumerEdge {
                procedure: edge.procedure,
                transform: None,
                key: None,
            })
            .collect(),
        // Entry payloads wrap into a HostPayload naming their channel; the
        // prepare crossing fills keys/event time before delivery when a
        // consumer needs them.
        wrap_input: (derived.kind == HostChannelKind::Entry).then(|| entry_wrap(channel_id)),
        entry_parsed: false,
        // A host class serializes on its own side and ships bytes, so the
        // descriptor carries no serializer — the kind is the whole record
        // of its plugin egress.
        plugin_sink: None,
        keyed: channel_decl.keyed,
        overflow_policy: match channel_decl.overflow {
            HostOverflow::DropNothing => OverflowPolicy::DropNothing,
            HostOverflow::DropNewest => OverflowPolicy::DropNewest,
            HostOverflow::DropOldest => OverflowPolicy::DropOldest,
        },
        retry: RetryPolicy {
            max_attempts: channel_decl.retry_max,
            backoff_base_ms: channel_decl.retry_backoff_ms,
        },
        // Host inter-proc values are bytes already; the journal codec is
        // the lossless HostPayload wrapper, so a durable chained loop
        // can re-seed exactly what the live handoff carried.
        codec: (derived.kind == HostChannelKind::InterProc).then(|| host_payload_codec(channel_id)),
    }
}

fn entry_wrap(channel_id: u32) -> EntryWrapFn {
    Arc::new(move |bytes: Vec<u8>| {
        Ok(Arc::new(HostPayload {
            bytes,
            channel: channel_id,
            keys: Vec::new(),
            event_time: None,
            metadata: Vec::new(),
        }) as Arc<dyn Any + Send + Sync>)
    })
}

#[allow(clippy::too_many_arguments)]
fn build_procedure(
    derived_channels: &[DerivedChannel],
    class: &ClassFacts,
    channels: &[ChannelDescriptor],
    proc_id: u32,
    proc_name: &'static str,
    proc_decl: &HostProcedureDecl,
    decl: &HostClassDecl,
) -> ProcedureDescriptor {
    let (input_channel_ids, output_channel_id) = procedure_wiring(derived_channels, proc_id);
    let retry = input_channel_ids
        .first()
        .and_then(|&id| channels.get(id as usize))
        .map(|c| c.retry)
        .unwrap_or_default();
    let facts = Arc::new(host_facts(decl, class, proc_name, proc_decl, retry));
    ProcedureDescriptor {
        name: Arc::from(proc_name),
        entity_keys: proc_decl
            .entity_keys
            .iter()
            .map(|k| Box::leak(k.clone().into_boxed_str()) as &'static str)
            .collect(),
        entities_for_overridden: proc_decl.entities_for_overridden,
        compensate_overridden: proc_decl.compensate_overridden,
        mode: match proc_decl.entity_mode {
            HostEntityModeDecl::Single => EntityMode::Single,
            HostEntityModeDecl::Static => EntityMode::Static,
            HostEntityModeDecl::Dynamic => EntityMode::Dynamic,
        },
        idle_ttl: proc_decl.idle_ttl_ms.map(std::time::Duration::from_millis),
        max_entities: proc_decl.max_entities.unwrap_or(usize::MAX),
        input_channel_ids,
        output_channel_id,
        input_type_id: TypeId::of::<HostPayload>(),
        output_type_id: TypeId::of::<HostPayload>(),
        mailbox_capacity: proc_decl.mailbox_capacity,
        mailbox_capacity_overridden: false,
        compact_after_bytes: class.compact_after_bytes,
        host_facts: Some(Arc::clone(&facts)),
        build_entity: Box::new(move |args: crate::channel::BuildEntityArgs<'_>| {
            build_host_entity_slot(args, Arc::clone(&facts))
        }),
        entities_for_erased: Arc::new(|_| Vec::new()),
        serialize_state: Arc::new(host_serialize_unreachable),
        deserialize_state: Arc::new(host_deserialize_unreachable),
    }
}

/// The channels a procedure consumes and the one it produces.
fn procedure_wiring(channels: &[DerivedChannel], proc_id: u32) -> (Vec<u32>, Option<u32>) {
    let input_channel_ids = channels
        .iter()
        .filter(|c| c.consumers.iter().any(|e| e.procedure == proc_id))
        .map(|c| c.index)
        .collect();
    let output_channel_id = channels
        .iter()
        .find(|c| c.producer == Some(proc_id))
        .map(|c| c.index);
    (input_channel_ids, output_channel_id)
}

fn host_facts(
    decl: &HostClassDecl,
    class: &ClassFacts,
    proc_name: &'static str,
    proc_decl: &HostProcedureDecl,
    retry: RetryPolicy,
) -> HostProcedureFacts {
    HostProcedureFacts {
        name: proc_name,
        factory: class.factory_id,
        retry,
        schema_version: proc_decl.schema_version,
        min_readable_schema_version: proc_decl
            .min_readable_schema_version
            .unwrap_or(proc_decl.schema_version),
        persistent: decl.persistent,
        effectively_once: decl.effectively_once,
        checkpoint: class.ack_granularity.checkpoint(),
        clock: proc_decl.clock,
        sdk_lang: decl.sdk_lang,
        sdk_version: class.sdk_version,
        change_format_id: decl.change_format_id,
        correction: if proc_decl.clock == HostClock::EventTime {
            class.correction
        } else {
            None
        },
    }
}

/// The workflow half: the chain's spec over the topology the stream half built.
pub(super) fn register_workflow_class(
    decl: &HostClassDecl,
    workflow: &HostWorkflowDecl,
    topology: Arc<OperativeTopology>,
    dag: ProcDag,
    loop_mode: LoopMode,
) {
    crate::workflow::registry::register_class(
        topology.factory_id,
        WorkflowSpec {
            topology,
            dag,
            max_duration: workflow
                .max_duration_ms
                .map(std::time::Duration::from_millis),
            rollback_mode: rollback_of(workflow.rollback_mode),
            loop_mode,
            wake_after: workflow.wake_after_ms.map(std::time::Duration::from_millis),
            retry: RetryPolicy {
                max_attempts: workflow.retry_attempts,
                backoff_base_ms: workflow.retry_backoff_ms,
            },
            barrier_retry_attempts: workflow.barrier_retry_attempts,
            persistence_enabled: decl.persistent,
            sync_durability: decl.sync_durability,
        },
    );
}

fn rollback_of(mode: HostRollbackMode) -> RollbackMode {
    match mode {
        HostRollbackMode::AllOrNothing => RollbackMode::AllOrNothing,
        HostRollbackMode::KeepProgress => RollbackMode::KeepProgress,
    }
}

/// Kahn toposort from the entry root, refusing what the typed builder
/// refuses: several entry consumers, several outputs, cycles, disconnected
/// procedures.
pub(super) fn build_host_dag(topology: &OperativeTopology) -> Result<ProcDag, Error> {
    let root = refuse_dag_shape(topology)?;
    let n = topology.procedures.len();
    let GraphOrder {
        order,
        parents,
        children,
        on_cycle,
        ..
    } = graph_order(
        n,
        &inter_proc_edges(topology),
        &[root as ProcedureId],
        GraphSeed::Roots,
    );
    if order.len() != n {
        let missing = on_cycle
            .map(|i| topology.procedures[i as usize].name.to_string())
            .unwrap_or_default();
        return Err(Error::Declaration(format!(
            "procedure `{missing}` is on a cycle or not reachable from the entry channel"
        )));
    }
    // Re-index parents/children positionally by order (the driver's shape).
    let index_of: HashMap<usize, usize> = order
        .iter()
        .enumerate()
        .map(|(pos, &p)| (p as usize, pos))
        .collect();
    let dag_parents: Vec<Vec<usize>> = order
        .iter()
        .map(|&p| parents[p as usize].iter().map(|pp| index_of[pp]).collect())
        .collect();
    let dag_children: Vec<Vec<usize>> = order
        .iter()
        .map(|&p| children[p as usize].iter().map(|cc| index_of[cc]).collect())
        .collect();
    Ok(ProcDag {
        order,
        parents: dag_parents,
        children: dag_children,
    })
}

/// Producer → consumer pairs over the inter-proc channels only: a workflow's
/// stages are wired to each other, never through the entry channel.
fn inter_proc_edges(topology: &OperativeTopology) -> Vec<(ProcedureId, ProcedureId)> {
    let mut edges = Vec::new();
    for channel in &topology.channels {
        if channel.kind != ChannelKind::InterProcedure {
            continue;
        }
        let Some(producer) = channel.producer else {
            continue;
        };
        for edge in &channel.consumers {
            edges.push((producer, edge.procedure));
        }
    }
    edges
}

/// The root of a well-shaped chain: one entry channel feeding one procedure,
/// no procedure producing two channels.
fn refuse_dag_shape(topology: &OperativeTopology) -> Result<usize, Error> {
    let entry = topology
        .channels
        .iter()
        .find(|c| c.kind == ChannelKind::Entry)
        .ok_or_else(|| {
            Error::Declaration("a structured workflow class declares one entry channel".to_string())
        })?;
    if entry.consumers.len() != 1 {
        return Err(Error::Declaration(format!(
            "a bounded topology has a single root; entry channel `{}` feeds {} procedures",
            entry.name,
            entry.consumers.len()
        )));
    }
    for (proc_id, _) in topology.procedures.iter().enumerate() {
        let produced = topology
            .channels
            .iter()
            .filter(|c| c.producer == Some(proc_id as u32))
            .count();
        if produced > 1 {
            return Err(Error::Declaration(format!(
                "procedure `{}` produces more than one channel; a procedure emits on exactly \
                 one output channel",
                topology.procedures[proc_id].name
            )));
        }
    }
    Ok(entry.consumers[0].procedure as usize)
}
