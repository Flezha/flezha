//! Registration of a host class: refuse what the core cannot honour, build
//! the topology, register it as a typed class registers. Every refusal text
//! is a contract the veneer suites assert substrings of.

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::decl::{
    HostAckGranularity, HostBeyondWindow, HostChannelDecl, HostChannelKind, HostCheckpointOutputs,
    HostClassDecl, HostClock, HostEntityModeDecl, HostLoopMode, HostOverflow, HostProcedureDecl,
    HostRollbackMode, HostWorkflowDecl,
};
use super::entity::{
    build_host_entity_slot, host_deserialize_unreachable, host_payload_codec,
    host_serialize_unreachable, HostPayload, HostProcedureFacts,
};
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

/// The body of `flz_core_declare_class`.
pub fn register_host_class_json(bytes: &[u8]) -> Result<(), Error> {
    let decl: HostClassDecl = serde_json::from_slice(bytes).map_err(|e| {
        Error::Declaration(format!(
            "class declaration is not valid JSON for the schema: {e}"
        ))
    })?;
    register_host_class(decl)
}

pub fn register_host_class(decl: HostClassDecl) -> Result<(), Error> {
    refuse_class_facts(&decl)?;
    if let Some(workflow) = &decl.workflow {
        if decl.procedures.is_empty() {
            // Run-sugar: one imperative body, no topology.
            register_workflow_class(&decl, workflow, None, ProcDag::default(), LoopMode::Once);
            return Ok(());
        }
    }
    let mut seen = HashSet::new();
    for channel in &decl.channels {
        if !seen.insert(channel.name.as_str()) {
            return Err(Error::Declaration(format!(
                "duplicate channel name `{}`",
                channel.name
            )));
        }
        refuse_channel(&decl, channel)?;
    }
    let mut seen_procs = HashSet::new();
    for (idx, proc_decl) in decl.procedures.iter().enumerate() {
        if !seen_procs.insert(proc_decl.name.as_str()) {
            return Err(Error::Declaration(format!(
                "duplicate procedure name `{}`",
                proc_decl.name
            )));
        }
        refuse_procedure(&decl, idx as u32, proc_decl)?;
    }
    register(decl)
}

/// The class-level refusals; a run-sugar class is refused only on its
/// workflow facts.
fn refuse_class_facts(decl: &HostClassDecl) -> Result<(), Error> {
    if !crate::header::sdk_lang_allocated(decl.sdk_lang) {
        return Err(Error::Declaration(format!(
            "sdk_lang: {} is not an allocated SDK language; every class declares \
             the byte its SDK holds in the allocation table (flz_core::header), \
             because the controller places an operative only on a runner of its \
             factory's language",
            decl.sdk_lang
        )));
    }
    if decl.procedures.is_empty() && decl.workflow.is_none() {
        return Err(Error::Declaration(
            "a host class declares at least one procedure".to_string(),
        ));
    }
    if let Some(workflow) = &decl.workflow {
        refuse_workflow_facts(decl, workflow)?;
        if decl.procedures.is_empty() {
            return Ok(());
        }
    }
    if decl.sync_durability && decl.workflow.is_none() {
        return Err(Error::Declaration(format!(
            "class `{}` declares sync_durability, which only a workflow class may: a stream \
             class acknowledges at input rates, and a remote round trip per acknowledgement is \
             not a guarantee it can keep",
            decl.factory_id
        )));
    }
    refuse_tier_facts(decl)
}

fn refuse_workflow_facts(decl: &HostClassDecl, workflow: &HostWorkflowDecl) -> Result<(), Error> {
    if decl.procedures.is_empty() && !decl.channels.is_empty() {
        return Err(Error::Declaration(
            "a run-sugar workflow class (no procedures) declares no channels".to_string(),
        ));
    }
    if decl.effectively_once {
        return Err(Error::Declaration(
            "a workflow class cannot declare effectively_once".to_string(),
        ));
    }
    if decl.correction_window_ms.is_some() {
        return Err(Error::Declaration(
            "a workflow class cannot declare correction_window_ms".to_string(),
        ));
    }
    if decl.sync_durability && !decl.persistent {
        return Err(Error::Declaration(
            "sync_durability requires persistent: there is no commit barrier to ship \
             for a class that journals nothing"
                .to_string(),
        ));
    }
    if workflow.retry_attempts > 1 && workflow.retry_backoff_ms == 0 {
        return Err(Error::Declaration(format!(
            "workflow retry ladder ({} attempts) declares a zero backoff base; retries \
             would re-run the failing entity in a hot loop",
            workflow.retry_attempts
        )));
    }
    for proc_decl in &decl.procedures {
        if proc_decl.entity_mode == HostEntityModeDecl::Dynamic {
            return Err(Error::Declaration(format!(
                "workflow procedure `{}` declares dynamic entity mode, which workflow \
                 operatives do not support",
                proc_decl.name
            )));
        }
    }
    Ok(())
}

/// The tier, the checkpoint interval and the correction window each need
/// what they rest on declared with them.
fn refuse_tier_facts(decl: &HostClassDecl) -> Result<(), Error> {
    if decl.effectively_once && !decl.persistent {
        return Err(Error::Declaration(
            "effectively_once requires persistent: the tier IS durable state".to_string(),
        ));
    }
    if decl.ack_granularity == HostAckGranularity::Checkpoint && !decl.effectively_once {
        return Err(Error::Declaration(
            "ack_granularity checkpoint requires effectively_once".to_string(),
        ));
    }
    if decl.ack_granularity != HostAckGranularity::Checkpoint
        && (decl.checkpoint_every.is_some()
            || decl.checkpoint_outputs == HostCheckpointOutputs::AtCheckpoint
            || decl.checkpoint_max_delay_ms.is_some())
    {
        return Err(Error::Declaration(
            "checkpoint_every / checkpoint_outputs / checkpoint_max_delay_ms require \
             ack_granularity checkpoint"
                .to_string(),
        ));
    }
    if decl.checkpoint_outputs == HostCheckpointOutputs::AtCheckpoint
        && decl.checkpoint_max_delay_ms.is_none()
    {
        return Err(Error::Declaration(
            "checkpoint_outputs at_checkpoint requires checkpoint_max_delay_ms: an interval that \
             never closes would hold its outputs forever on a quiet Entity"
                .to_string(),
        ));
    }
    if decl.checkpoint_every == Some(0) || decl.checkpoint_max_delay_ms == Some(0) {
        return Err(Error::Declaration(
            "checkpoint_every / checkpoint_max_delay_ms must be greater than zero".to_string(),
        ));
    }
    if decl.correction_window_ms.is_some()
        && !decl
            .procedures
            .iter()
            .any(|p| p.clock == HostClock::EventTime)
    {
        return Err(Error::Declaration(
            "correction_window_ms requires at least one event_time-clocked procedure".to_string(),
        ));
    }
    Ok(())
}

fn refuse_channel(decl: &HostClassDecl, channel: &HostChannelDecl) -> Result<(), Error> {
    refuse_shedding(decl, channel)?;
    let arity = |idx: u32| (idx as usize) < decl.procedures.len();
    if let Some(p) = channel.producer {
        if !arity(p) {
            return Err(Error::Declaration(format!(
                "channel `{}` names producer {} but the class declares {} procedure(s)",
                channel.name,
                p,
                decl.procedures.len()
            )));
        }
    }
    if let Some(&c) = channel.consumers.iter().find(|&&c| !arity(c)) {
        return Err(Error::Declaration(format!(
            "channel `{}` names consumer {} but the class declares {} procedure(s)",
            channel.name,
            c,
            decl.procedures.len()
        )));
    }
    refuse_channel_ends(channel)
}

/// A shedding policy is refused where the loss it permits cannot be
/// accounted for: which items survive depends on timing, and both
/// durability contracts promise an answer that does not.
fn refuse_shedding(decl: &HostClassDecl, channel: &HostChannelDecl) -> Result<(), Error> {
    let sheds = match channel.overflow {
        HostOverflow::DropNothing => None,
        HostOverflow::DropNewest => Some("drop_newest"),
        HostOverflow::DropOldest => Some("drop_oldest"),
    };
    let Some(policy) = sheds else {
        return Ok(());
    };
    if decl.effectively_once {
        // A dropped input's offset is skipped by a later item's
        // watermark advance: the input is gone for good while the tier
        // reports effectively-once.
        return Err(Error::Declaration(format!(
            "channel `{}` declares overflow `{}` on a class that declares \
             effectively_once; a shed input's offset is still advanced past by \
             the tier's watermark, so the loss is permanent and unreported — \
             declare one or the other, not both",
            channel.name, policy
        )));
    }
    if decl.correction_window_ms.is_some() {
        // A correction's answer may not depend on arrival timing.
        return Err(Error::Declaration(format!(
            "channel `{}` declares overflow `{}` on a class that declares \
             correction_window_ms; a correction re-folds a replayed slice and \
             its answer may not depend on which items a full mailbox happened \
             to keep",
            channel.name, policy
        )));
    }
    Ok(())
}

/// Each channel kind names exactly the ends its kind has.
fn refuse_channel_ends(channel: &HostChannelDecl) -> Result<(), Error> {
    match channel.kind {
        HostChannelKind::Entry => {
            if channel.producer.is_some() {
                return Err(Error::Declaration(format!(
                    "entry channel `{}` cannot name a producer",
                    channel.name
                )));
            }
            if channel.consumers.is_empty() {
                return Err(Error::Declaration(format!(
                    "entry channel `{}` names no consumer",
                    channel.name
                )));
            }
        }
        HostChannelKind::InterProc => {
            if channel.producer.is_none() {
                return Err(Error::Declaration(format!(
                    "inter_proc channel `{}` names no producer",
                    channel.name
                )));
            }
            if channel.consumers.is_empty() {
                return Err(Error::Declaration(format!(
                    "inter_proc channel `{}` names no consumer",
                    channel.name
                )));
            }
        }
        HostChannelKind::Plugin => {
            if !channel.consumers.is_empty() {
                return Err(Error::Declaration(format!(
                    "plugin channel `{}` cannot name consumers",
                    channel.name
                )));
            }
        }
    }
    Ok(())
}

fn refuse_procedure(
    decl: &HostClassDecl,
    idx: u32,
    proc_decl: &HostProcedureDecl,
) -> Result<(), Error> {
    match proc_decl.entity_mode {
        HostEntityModeDecl::Static if proc_decl.entity_keys.is_empty() => {
            return Err(Error::Declaration(format!(
                "static procedure `{}` declares no entity_keys",
                proc_decl.name
            )));
        }
        HostEntityModeDecl::Single | HostEntityModeDecl::Dynamic
            if !proc_decl.entity_keys.is_empty() =>
        {
            return Err(Error::Declaration(format!(
                "procedure `{}` declares entity_keys without static mode",
                proc_decl.name
            )));
        }
        HostEntityModeDecl::Dynamic if proc_decl.max_entities == Some(0) => {
            return Err(Error::Declaration(format!(
                "dynamic procedure `{}` declares max_entities 0",
                proc_decl.name
            )));
        }
        _ => {}
    }
    let has_input = decl
        .channels
        .iter()
        .any(|c| c.kind != HostChannelKind::Plugin && c.consumers.contains(&idx));
    if !has_input {
        return Err(Error::Declaration(format!(
            "procedure `{}` has no input channel",
            proc_decl.name
        )));
    }
    Ok(())
}

/// The class facts every procedure and the topology read, resolved once so
/// they agree.
struct ClassFacts {
    factory_id: &'static str,
    correction: Option<CorrectionPolicy>,
    ack_granularity: AckGranularity,
    compact_after_bytes: usize,
}

fn resolve_class_facts(decl: &HostClassDecl) -> ClassFacts {
    let correction = decl.correction_window_ms.map(|window_ms| CorrectionPolicy {
        window_ms,
        beyond_window: match decl.beyond_window_policy {
            HostBeyondWindow::ProcessAsArrival => BeyondWindowPolicy::ProcessAsArrival,
            HostBeyondWindow::Drop => BeyondWindowPolicy::Drop,
        },
    });
    let ack_granularity = match decl.ack_granularity {
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
        correction,
        ack_granularity,
        compact_after_bytes: decl
            .compact_after_bytes
            .unwrap_or(crate::operative::topology::DEFAULT_COMPACT_AFTER_BYTES),
    }
}

/// Build the validated declaration's topology and register it.
fn register(decl: HostClassDecl) -> Result<(), Error> {
    let class = resolve_class_facts(&decl);
    let mut channels: Vec<ChannelDescriptor> = Vec::with_capacity(decl.channels.len());
    let mut name_to_id: HashMap<&'static str, u32> = HashMap::new();
    for (channel_id, channel_decl) in decl.channels.iter().enumerate() {
        let channel_id = channel_id as u32;
        let name: &'static str = Box::leak(channel_decl.name.clone().into_boxed_str());
        channels.push(build_channel(channel_id, name, channel_decl));
        name_to_id.insert(name, channel_id);
    }
    let mut procedures: Vec<ProcedureDescriptor> = Vec::with_capacity(decl.procedures.len());
    let mut procedure_name_to_id: HashMap<&'static str, u32> = HashMap::new();
    for (proc_id, proc_decl) in decl.procedures.iter().enumerate() {
        let proc_id = proc_id as u32;
        let proc_name: &'static str = Box::leak(proc_decl.name.clone().into_boxed_str());
        procedures.push(build_procedure(
            &decl, &class, &channels, proc_id, proc_name, proc_decl,
        ));
        procedure_name_to_id.insert(proc_name, proc_id);
    }
    let topology = Arc::new(OperativeTopology {
        sdk_lang: decl.sdk_lang,
        channels,
        name_to_id,
        procedures,
        procedure_name_to_id,
        factory_id: class.factory_id,
        persistence_enabled_const: decl.persistent,
        effectively_once: decl.effectively_once,
        correction: class.correction,
        ack_granularity: class.ack_granularity,
    });
    match &decl.workflow {
        Some(workflow) => register_structured_workflow(&decl, workflow, topology),
        None => {
            crate::operative::registry::register_class(class.factory_id, topology);
            Ok(())
        }
    }
}

/// A structured chain: its DAG from the topology, a chained loop decided
/// against it.
fn register_structured_workflow(
    decl: &HostClassDecl,
    workflow: &HostWorkflowDecl,
    topology: Arc<OperativeTopology>,
) -> Result<(), Error> {
    let dag = build_host_dag(&topology)?;
    if workflow.loop_mode == HostLoopMode::Chained && dag.single_leaf().is_none() {
        return Err(Error::Declaration(format!(
            "a handoff-chained loop needs exactly one terminal procedure; the graph \
             has {}",
            dag.leaves().count()
        )));
    }
    let loop_mode = match workflow.loop_mode {
        HostLoopMode::Once => LoopMode::Once,
        HostLoopMode::PerInput => LoopMode::PerInput,
        HostLoopMode::Chained => LoopMode::Chained,
    };
    register_workflow_class(decl, workflow, Some(topology), dag, loop_mode);
    Ok(())
}

fn build_channel(
    channel_id: u32,
    name: &'static str,
    channel_decl: &HostChannelDecl,
) -> ChannelDescriptor {
    let kind = match channel_decl.kind {
        HostChannelKind::Entry => ChannelKind::Entry,
        HostChannelKind::InterProc => ChannelKind::InterProcedure,
        HostChannelKind::Plugin => ChannelKind::PluginBound,
    };
    ChannelDescriptor {
        name: Arc::from(name),
        kind,
        producer: channel_decl.producer,
        consumers: channel_decl
            .consumers
            .iter()
            .map(|&procedure| crate::channel::ConsumerEdge {
                procedure,
                transform: None,
                key: None,
            })
            .collect(),
        // Entry payloads wrap into a HostPayload naming their channel; the
        // prepare crossing fills keys/event time before delivery when a
        // consumer needs them.
        wrap_input: (channel_decl.kind == HostChannelKind::Entry).then(|| entry_wrap(channel_id)),
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
        codec: (channel_decl.kind == HostChannelKind::InterProc)
            .then(|| host_payload_codec(channel_id)),
        entry_observation: crate::channel::entry_observation_for(kind, name),
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

fn build_procedure(
    decl: &HostClassDecl,
    class: &ClassFacts,
    channels: &[ChannelDescriptor],
    proc_id: u32,
    proc_name: &'static str,
    proc_decl: &HostProcedureDecl,
) -> ProcedureDescriptor {
    let (input_channel_ids, output_channel_id) = procedure_wiring(decl, proc_id);
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
fn procedure_wiring(decl: &HostClassDecl, proc_id: u32) -> (Vec<u32>, Option<u32>) {
    let input_channel_ids = decl
        .channels
        .iter()
        .enumerate()
        .filter(|(_, c)| c.consumers.contains(&proc_id))
        .map(|(id, _)| id as u32)
        .collect();
    let output_channel_id = decl
        .channels
        .iter()
        .position(|c| c.producer == Some(proc_id))
        .map(|id| id as u32);
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
        change_format_id: decl.change_format_id,
        correction: if proc_decl.clock == HostClock::EventTime {
            class.correction
        } else {
            None
        },
    }
}

/// The workflow half: run-sugar and a structured chain build the same spec.
fn register_workflow_class(
    decl: &HostClassDecl,
    workflow: &HostWorkflowDecl,
    topology: Option<Arc<OperativeTopology>>,
    dag: ProcDag,
    loop_mode: LoopMode,
) {
    let factory_id: &'static str = match &topology {
        Some(topology) => topology.factory_id,
        None => Box::leak(decl.factory_id.clone().into_boxed_str()),
    };
    crate::workflow::registry::register_class(
        factory_id,
        WorkflowSpec {
            sdk_lang: decl.sdk_lang,
            run: host_run_unreachable,
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
            host: true,
        },
    );
}

fn rollback_of(mode: HostRollbackMode) -> RollbackMode {
    match mode {
        HostRollbackMode::AllOrNothing => RollbackMode::AllOrNothing,
        HostRollbackMode::KeepProgress => RollbackMode::KeepProgress,
    }
}

/// The driver branches on `spec.host` before ever calling `run`.
fn host_run_unreachable(
    _ctx: &mut crate::workflow::WorkflowCtx,
    _args: &str,
) -> Result<(), crate::workflow::WorkflowError> {
    Err(crate::workflow::WorkflowError::from(
        "host workflow classes run through the poll crossing",
    ))
}

/// Kahn toposort from the entry root, refusing what the typed builder
/// refuses: several entry consumers, several outputs, cycles, disconnected
/// procedures.
fn build_host_dag(topology: &OperativeTopology) -> Result<ProcDag, Error> {
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
