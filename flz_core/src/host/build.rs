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

use super::cell::{
    build_host_cell_slot, host_deserialize_unreachable, host_payload_codec,
    host_serialize_unreachable, HostPayload, HostTaskletFacts,
};
use super::decl::{
    HostAckGranularity, HostBeyondWindow, HostCellModeDecl, HostChannelDecl, HostChannelKind,
    HostCheckpointOutputs, HostClassDecl, HostClock, HostOverflow, HostRollbackMode,
    HostTaskletDecl, HostWorkflowDecl,
};
use super::register::DerivedChannel;
use crate::channel::{
    ChannelDescriptor, ChannelKind, EntryWrapFn, OverflowPolicy, RetryPolicy, TaskletId,
};
use crate::error::Error;
use crate::operative::topology::{
    AckGranularity, BeyondWindowPolicy, CellMode, CheckpointOutputs, CheckpointPolicy,
    CorrectionPolicy, OperativeTopology, TaskletDescriptor,
};
use crate::workflow::spec::{graph_order, GraphSeed, TaskDag, WorkflowSpec};
use crate::workflow::{LoopMode, RollbackMode};

/// The class facts every tasklet and the topology read, resolved once so
/// they agree.
pub(super) struct ClassFacts {
    pub(super) factory_id: &'static str,
    /// The declared version packed as the durable header carries it, resolved
    /// once so every tasklet of the class stamps the same bytes.
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

/// Every tasklet descriptor and its name index, built after the channels
/// because a tasklet's retry ladder is its first input channel's.
pub(super) fn build_tasklets(
    decl: &HostClassDecl,
    class: &ClassFacts,
    channels: &[ChannelDescriptor],
    derived: &[DerivedChannel],
) -> (Vec<TaskletDescriptor>, HashMap<&'static str, u32>) {
    let mut tasklets = Vec::with_capacity(decl.tasklets.len());
    let mut name_to_id = HashMap::new();
    for (task_id, task_decl) in decl.tasklets.iter().enumerate() {
        let task_id = task_id as u32;
        let task_name: &'static str = Box::leak(task_decl.name.clone().into_boxed_str());
        tasklets.push(build_tasklet(
            derived, class, channels, task_id, task_name, task_decl, decl,
        ));
        name_to_id.insert(task_name, task_id);
    }
    (tasklets, name_to_id)
}

fn build_channel(
    channel_id: u32,
    name: &'static str,
    channel_decl: &HostChannelDecl,
    derived: &DerivedChannel,
) -> ChannelDescriptor {
    let kind = match derived.kind {
        HostChannelKind::Entry => ChannelKind::Entry,
        HostChannelKind::InterTask => ChannelKind::InterTasklet,
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
                tasklet: edge.tasklet,
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
        // Host inter-task values are bytes already; the journal codec is
        // the lossless HostPayload wrapper, so a durable chained loop
        // can re-seed exactly what the live handoff carried.
        codec: (derived.kind == HostChannelKind::InterTask).then(|| host_payload_codec(channel_id)),
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
fn build_tasklet(
    derived_channels: &[DerivedChannel],
    class: &ClassFacts,
    channels: &[ChannelDescriptor],
    task_id: u32,
    task_name: &'static str,
    task_decl: &HostTaskletDecl,
    decl: &HostClassDecl,
) -> TaskletDescriptor {
    let (input_channel_ids, output_channel_id) = tasklet_wiring(derived_channels, task_id);
    let retry = input_channel_ids
        .first()
        .and_then(|&id| channels.get(id as usize))
        .map(|c| c.retry)
        .unwrap_or_default();
    let facts = Arc::new(host_facts(decl, class, task_name, task_decl, retry));
    TaskletDescriptor {
        name: Arc::from(task_name),
        cell_keys: task_decl
            .cell_keys
            .iter()
            .map(|k| Box::leak(k.clone().into_boxed_str()) as &'static str)
            .collect(),
        cells_for_overridden: task_decl.cells_for_overridden,
        compensate_overridden: task_decl.compensate_overridden,
        mode: match task_decl.cell_mode {
            HostCellModeDecl::Single => CellMode::Single,
            HostCellModeDecl::Static => CellMode::Static,
            HostCellModeDecl::Dynamic => CellMode::Dynamic,
        },
        idle_ttl: task_decl.idle_ttl_ms.map(std::time::Duration::from_millis),
        max_cells: task_decl.max_cells.unwrap_or(usize::MAX),
        input_channel_ids,
        output_channel_id,
        input_type_id: TypeId::of::<HostPayload>(),
        output_type_id: TypeId::of::<HostPayload>(),
        mailbox_capacity: task_decl.mailbox_capacity,
        mailbox_capacity_overridden: false,
        compact_after_bytes: class.compact_after_bytes,
        host_facts: Some(Arc::clone(&facts)),
        build_cell: Box::new(move |args: crate::channel::BuildCellArgs<'_>| {
            build_host_cell_slot(args, Arc::clone(&facts))
        }),
        cells_for_erased: Arc::new(|_| Vec::new()),
        serialize_state: Arc::new(host_serialize_unreachable),
        deserialize_state: Arc::new(host_deserialize_unreachable),
    }
}

/// The channels a tasklet consumes and the one it produces.
fn tasklet_wiring(channels: &[DerivedChannel], task_id: u32) -> (Vec<u32>, Option<u32>) {
    let input_channel_ids = channels
        .iter()
        .filter(|c| c.consumers.iter().any(|e| e.tasklet == task_id))
        .map(|c| c.index)
        .collect();
    let output_channel_id = channels
        .iter()
        .find(|c| c.producer == Some(task_id))
        .map(|c| c.index);
    (input_channel_ids, output_channel_id)
}

fn host_facts(
    decl: &HostClassDecl,
    class: &ClassFacts,
    task_name: &'static str,
    task_decl: &HostTaskletDecl,
    retry: RetryPolicy,
) -> HostTaskletFacts {
    HostTaskletFacts {
        name: task_name,
        factory: class.factory_id,
        retry,
        schema_version: task_decl.schema_version,
        min_readable_schema_version: task_decl
            .min_readable_schema_version
            .unwrap_or(task_decl.schema_version),
        persistent: decl.persistent,
        effectively_once: decl.effectively_once,
        checkpoint: class.ack_granularity.checkpoint(),
        clock: task_decl.clock,
        sdk_lang: decl.sdk_lang,
        sdk_version: class.sdk_version,
        change_format_id: decl.change_format_id,
        correction: if task_decl.clock == HostClock::EventTime {
            class.correction
        } else {
            None
        },
    }
}

/// The workflow half: the chain's spec over the topology the stream half built.
pub(super) fn workflow_spec(
    decl: &HostClassDecl,
    workflow: &HostWorkflowDecl,
    dag: TaskDag,
    loop_mode: LoopMode,
) -> WorkflowSpec {
    WorkflowSpec {
        dag,
        max_duration: workflow
            .max_duration_ms
            .map(std::time::Duration::from_millis),
        rollback_mode: rollback_of(workflow.rollback_mode),
        loop_mode,
        wake_after: workflow.wake_after_ms.map(std::time::Duration::from_millis),
        sync_durability: decl.sync_durability,
    }
}

fn rollback_of(mode: HostRollbackMode) -> RollbackMode {
    match mode {
        HostRollbackMode::AllOrNothing => RollbackMode::AllOrNothing,
        HostRollbackMode::KeepProgress => RollbackMode::KeepProgress,
    }
}

/// Kahn toposort from every entry channel's consumers, refusing what the
/// typed builder refuses: no entry channel, several outputs, cycles,
/// disconnected tasklets.
pub(super) fn build_host_dag(topology: &OperativeTopology) -> Result<TaskDag, Error> {
    let entries = topology.entry_consumers();
    if entries.is_empty() {
        return Err(Error::Declaration(
            "a structured workflow class declares an entry channel its messages arrive on"
                .to_string(),
        ));
    }
    refuse_several_outputs(topology)?;
    let n = topology.tasklets.len();
    let roots: Vec<TaskletId> = entries
        .iter()
        .flat_map(|(_, c)| c.iter().copied())
        .collect();
    let walk = graph_order(n, &inter_task_edges(topology), &roots, GraphSeed::Roots);
    if walk.order.len() != n {
        let missing = walk
            .on_cycle
            .map(|i| topology.tasklets[i as usize].name.to_string())
            .unwrap_or_default();
        return Err(Error::Declaration(format!(
            "tasklet `{missing}` is on a cycle or not reachable from any entry channel"
        )));
    }
    Ok(TaskDag::from_walk(walk, entries))
}

/// Producer → consumer pairs over the inter-task channels only: a workflow's
/// stages are wired to each other, never through the entry channel.
fn inter_task_edges(topology: &OperativeTopology) -> Vec<(TaskletId, TaskletId)> {
    let mut edges = Vec::new();
    for channel in &topology.channels {
        if channel.kind != ChannelKind::InterTasklet {
            continue;
        }
        let Some(producer) = channel.producer else {
            continue;
        };
        for edge in &channel.consumers {
            edges.push((producer, edge.tasklet));
        }
    }
    edges
}

/// No tasklet producing two channels: a stage has one output a child reads.
fn refuse_several_outputs(topology: &OperativeTopology) -> Result<(), Error> {
    for (task_id, _) in topology.tasklets.iter().enumerate() {
        let produced = topology
            .channels
            .iter()
            .filter(|c| c.producer == Some(task_id as u32))
            .count();
        if produced > 1 {
            return Err(Error::Declaration(format!(
                "tasklet `{}` produces more than one channel; a tasklet emits on exactly \
                 one output channel",
                topology.tasklets[task_id].name
            )));
        }
    }
    Ok(())
}
