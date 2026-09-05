//! Judging a host class: the record every SDK submits, the check that reads
//! it, and the registration that follows. `check` holds every named refusal
//! and every derivation rule once, so a builder records what its author
//! declared and decides nothing. Every refusal text is a contract the veneer
//! suites assert substrings of.
//!
//! What a validated record BECOMES — the descriptors, the facts, the DAG — is
//! [`super::build`].

use std::collections::HashSet;
use std::sync::Arc;

use super::build::{
    build_channels, build_host_dag, build_procedures, register_workflow_class, resolve_class_facts,
};
use super::chains::derive_channels;
use super::decl::{
    HostAckGranularity, HostChannelKind, HostCheckpointOutputs, HostClassDecl, HostClock,
    HostEntityModeDecl, HostLoopMode, HostOverflow, HostProcedureDecl, HostWorkflowDecl,
};
use super::issue::{render_issues, TopologyIssue};
use crate::error::Error;
use crate::operative::topology::OperativeTopology;
use crate::workflow::spec::{graph_order, GraphSeed, ProcDag};
use crate::workflow::LoopMode;

/// Hard upper bound on a per-Entity mailbox: ~1M items is past sanity for any
/// expected workload, and a capacity above it is a typo, not a posture.
pub const MAX_MAILBOX_CAPACITY: usize = 1_048_576;

/// The derived declaration: what a record means once its chains are read.
/// Everything here is the core's answer, never the builder's claim — which is
/// why one rule can be stated once and every SDK gets it.
#[derive(Debug, serde::Serialize)]
pub struct Derived {
    pub channels: Vec<DerivedChannel>,
    pub ack_granularity: HostAckGranularity,
    #[serde(skip)]
    pub decl: HostClassDecl,
}

#[derive(Debug, serde::Serialize)]
pub struct DerivedChannel {
    pub index: u32,
    /// The last `named` link if the record carried one, else the builder's
    /// derived default. Read once, for every terminator (KI-120).
    pub name: String,
    pub kind: HostChannelKind,
    pub producer: Option<u32>,
    pub consumers: Vec<DerivedEdge>,
    /// The plugin descriptor this channel serializes onto.
    pub sink: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct DerivedEdge {
    pub procedure: u32,
    /// Which of the channel's chains produced this edge. The host held that
    /// chain's closures aside and attaches them here — so which edge survives a
    /// re-declaration is the core's answer, not a rule each builder repeats.
    pub chain: usize,
    #[serde(skip)]
    pub transform: bool,
    #[serde(skip)]
    pub key: bool,
}

/// The body of `flz_core_declare_class`.
pub fn register_host_class_json(bytes: &[u8]) -> Result<(), Error> {
    register_host_class(parse_decl(bytes)?)
}

/// The body of `flz_core_check_class`: the verdict and the derived
/// declaration, with nothing registered.
pub fn check_class_json(bytes: &[u8]) -> Result<Derived, Vec<TopologyIssue>> {
    match parse_decl(bytes) {
        Ok(decl) => check(decl),
        Err(Error::Declaration(text)) => Err(vec![TopologyIssue::Declaration(text)]),
        Err(other) => Err(vec![TopologyIssue::Declaration(other.to_string())]),
    }
}

fn parse_decl(bytes: &[u8]) -> Result<HostClassDecl, Error> {
    serde_json::from_slice(bytes).map_err(|e| {
        Error::Declaration(format!(
            "class declaration is not valid JSON for the schema: {e}"
        ))
    })
}

pub fn register_host_class(decl: HostClassDecl) -> Result<(), Error> {
    let derived = check(decl).map_err(|issues| Error::Declaration(render_issues(&issues)))?;
    register(derived)
}

/// Judge a record and derive its declaration. Three stages, accumulating:
/// the chains each channel was declared with, the derivation those chains
/// determine, then the topology and class facts over the derived shape. The
/// one seam every SDK's builder submits to.
pub fn check(decl: HostClassDecl) -> Result<Derived, Vec<TopologyIssue>> {
    let mut issues = Vec::new();
    refuse_class_facts(&decl, &mut issues);

    // Run-sugar: one imperative body, no topology to derive.
    let run_sugar = decl.workflow.is_some() && decl.procedures.is_empty();
    let channels = if run_sugar {
        Vec::new()
    } else {
        derive_channels(&decl, &mut issues)
    };

    if !run_sugar {
        refuse_names(&decl, &channels, &mut issues);
        for proc_decl in &decl.procedures {
            refuse_procedure(proc_decl, &mut issues);
        }
        for channel in &channels {
            refuse_channel(&decl, channel, &mut issues);
        }
        refuse_edge_keying(&decl, &channels, &mut issues);
        refuse_graph_shape(&decl, &channels, &mut issues);
    }

    if issues.is_empty() {
        Ok(Derived {
            ack_granularity: derive_ack_granularity(&decl),
            channels,
            decl,
        })
    } else {
        Err(issues)
    }
}

/// Any checkpoint key implies the granularity — the rule six per-SDK helpers
/// each carried before it was stated once here (KI-122).
fn derive_ack_granularity(decl: &HostClassDecl) -> HostAckGranularity {
    let asked = decl.checkpoint_ack
        || decl.checkpoint_every.is_some()
        || decl.checkpoint_outputs == HostCheckpointOutputs::AtCheckpoint
        || decl.checkpoint_max_delay_ms.is_some();
    if asked {
        HostAckGranularity::Checkpoint
    } else {
        HostAckGranularity::PerInput
    }
}

/// One namespace per operative for channels, one for nodes: a derived name,
/// a `named(...)` override and an entry name all collide here.
fn refuse_names(
    decl: &HostClassDecl,
    channels: &[DerivedChannel],
    issues: &mut Vec<TopologyIssue>,
) {
    let mut seen = HashSet::new();
    for channel in channels {
        if !seen.insert(channel.name.as_str()) {
            issues.push(TopologyIssue::DuplicateChannel(channel.name.clone()));
        }
    }
    let mut seen_procs = HashSet::new();
    for proc_decl in &decl.procedures {
        if !seen_procs.insert(proc_decl.name.as_str()) {
            issues.push(TopologyIssue::NodeIdentityCollision {
                name: proc_decl.name.clone(),
            });
        }
    }
}

/// The class-level refusals; a run-sugar class is refused only on its
/// workflow facts.
fn refuse_class_facts(decl: &HostClassDecl, issues: &mut Vec<TopologyIssue>) {
    if !crate::header::sdk_lang_allocated(decl.sdk_lang) {
        issues.push(TopologyIssue::Declaration(format!(
            "sdk_lang: {} is not an allocated SDK language; every class declares \
             the byte its SDK holds in the allocation table (flz_core::header), \
             because the controller places an operative only on a runner of its \
             factory's language",
            decl.sdk_lang
        )));
    }
    if crate::header::pack_version(&decl.sdk_version) == 0 {
        issues.push(TopologyIssue::Declaration(format!(
            "sdk_version: class `{}` declares no usable SDK version ({:?}); every class \
             declares the released version of the SDK that writes its records, because \
             the durable header names the writer and a veneer releases apart from the \
             core it links against",
            decl.factory_id, decl.sdk_version
        )));
    }
    if decl.procedures.is_empty() && decl.workflow.is_none() {
        issues.push(TopologyIssue::Declaration(
            "a host class declares at least one procedure".to_string(),
        ));
    }
    if let Some(workflow) = &decl.workflow {
        refuse_workflow_facts(decl, workflow, issues);
        if decl.procedures.is_empty() {
            return;
        }
    }
    if decl.sync_durability && decl.workflow.is_none() {
        issues.push(TopologyIssue::SyncDurabilityOnStream {
            factory_id: decl.factory_id.clone(),
        });
    }
    refuse_tier_facts(decl, issues);
}

fn refuse_workflow_facts(
    decl: &HostClassDecl,
    workflow: &HostWorkflowDecl,
    issues: &mut Vec<TopologyIssue>,
) {
    if decl.procedures.is_empty() && !decl.channels.is_empty() {
        issues.push(TopologyIssue::Declaration(
            "a run-sugar workflow class (no procedures) declares no channels".to_string(),
        ));
    }
    if decl.effectively_once {
        issues.push(TopologyIssue::EffectivelyOnceOnWorkflow);
    }
    if decl.correction_window_ms.is_some() {
        issues.push(TopologyIssue::CorrectionWindowOnWorkflow);
    }
    if decl.sync_durability && !decl.persistent {
        issues.push(TopologyIssue::Declaration(
            "sync_durability requires persistent: there is no commit barrier to ship \
             for a class that journals nothing"
                .to_string(),
        ));
    }
    if workflow.retry_attempts > 1 && workflow.retry_backoff_ms == 0 {
        issues.push(TopologyIssue::Declaration(format!(
            "workflow retry ladder ({} attempts) declares a zero backoff base; retries \
             would re-run the failing entity in a hot loop",
            workflow.retry_attempts
        )));
    }
    for proc_decl in &decl.procedures {
        if proc_decl.entity_mode == HostEntityModeDecl::Dynamic {
            issues.push(TopologyIssue::Declaration(format!(
                "workflow procedure `{}` declares dynamic entity mode, which workflow \
                 operatives do not support",
                proc_decl.name
            )));
        }
    }
}

/// The tier, the checkpoint interval and the correction window each need
/// what they rest on declared with them.
fn refuse_tier_facts(decl: &HostClassDecl, issues: &mut Vec<TopologyIssue>) {
    if decl.effectively_once && !decl.persistent {
        issues.push(TopologyIssue::Declaration(
            "effectively_once requires persistent: the tier IS durable state".to_string(),
        ));
    }
    if derive_ack_granularity(decl) == HostAckGranularity::Checkpoint && !decl.effectively_once {
        issues.push(TopologyIssue::CheckpointAckWithoutTier);
    }
    if decl.checkpoint_outputs == HostCheckpointOutputs::AtCheckpoint
        && decl.checkpoint_max_delay_ms.is_none()
    {
        issues.push(TopologyIssue::AtCheckpointWithoutMaxDelay);
    }
    if decl.checkpoint_every == Some(0) {
        issues.push(TopologyIssue::CheckpointEveryZero);
    }
    if decl.checkpoint_max_delay_ms == Some(0) {
        issues.push(TopologyIssue::Declaration(
            "checkpoint_max_delay_ms must be greater than zero".to_string(),
        ));
    }
    if decl.correction_window_ms.is_some() && !decl.effectively_once {
        issues.push(TopologyIssue::CorrectionWithoutTier);
    }
    if decl.correction_window_ms == Some(0) {
        issues.push(TopologyIssue::CorrectionWindowZero);
    }
    if decl.correction_window_ms.is_some() {
        for proc_decl in &decl.procedures {
            if proc_decl.clock != HostClock::EventTime {
                issues.push(TopologyIssue::CorrectionWithoutEventTime {
                    procedure: proc_decl.name.clone(),
                    clock_type: clock_name(proc_decl.clock).to_string(),
                });
            }
        }
    }
}

fn clock_name(clock: HostClock) -> &'static str {
    match clock {
        HostClock::Wall => "wall",
        HostClock::Logical => "logical",
        HostClock::EventTime => "event_time",
    }
}

/// What a channel's declarations may claim about its plugin wire, its ends
/// and what it discards.
fn refuse_channel(decl: &HostClassDecl, channel: &DerivedChannel, issues: &mut Vec<TopologyIssue>) {
    let channel_decl = &decl.channels[channel.index as usize];
    if channel_decl.keyed && channel.sink.is_none() {
        issues.push(TopologyIssue::KeyedChannelWithoutPluginWire {
            channel: channel.name.clone(),
        });
    }
    if decl.effectively_once && !channel_decl.keyed && channel.sink.is_some() {
        issues.push(TopologyIssue::UnkeyedChannelUnderEffectivelyOnce {
            channel: channel.name.clone(),
        });
    }
    if channel_decl.retry_max == 0 {
        issues.push(TopologyIssue::RetryMaxZero {
            channel: channel.name.clone(),
        });
    }
    // Promising effectively-once and asking to shed is a contradiction, not a
    // trade-off: a dropped input's offset is advanced past by a later item's
    // watermark. A correction window needs the tier, so this one arm covers
    // it too.
    if decl.effectively_once {
        // The policy is named as the record spells it, so an author reads
        // back the literal they wrote.
        let policy = match channel_decl.overflow {
            HostOverflow::DropNothing => None,
            HostOverflow::DropNewest => Some("drop_newest"),
            HostOverflow::DropOldest => Some("drop_oldest"),
        };
        if let Some(policy) = policy {
            issues.push(TopologyIssue::DroppingPolicyUnderEffectivelyOnce {
                channel: channel.name.clone(),
                policy: policy.to_string(),
            });
        }
    }
    // The consumer end is judged where its link is read (`wire_consumer`); only
    // the producer is a channel fact of its own.
    if let Some(producer) = channel_decl.producer {
        if producer as usize >= decl.procedures.len() {
            issues.push(TopologyIssue::Declaration(format!(
                "channel `{}` names producer {} but the class declares {} procedure(s)",
                channel.name,
                producer,
                decl.procedures.len()
            )));
        }
    }
    refuse_channel_ends(channel, issues);
}

/// Each channel kind names exactly the ends its kind has. A produced
/// inter_proc channel with no consumer is NOT one of them: it is a leaf's
/// retained output — what the typed SDK's `sealed()` declares, what a saga
/// archive leaf's undo reverses, what a handoff-chained loop feeds back — and
/// the driver, not another procedure, is what reads it.
fn refuse_channel_ends(channel: &DerivedChannel, issues: &mut Vec<TopologyIssue>) {
    let text = match channel.kind {
        HostChannelKind::Entry if channel.producer.is_some() => {
            format!("entry channel `{}` cannot name a producer", channel.name)
        }
        HostChannelKind::Entry if channel.consumers.is_empty() => {
            format!("entry channel `{}` names no consumer", channel.name)
        }
        HostChannelKind::InterProc if channel.producer.is_none() => {
            format!("inter_proc channel `{}` names no producer", channel.name)
        }
        HostChannelKind::Plugin if !channel.consumers.is_empty() => {
            format!("plugin channel `{}` cannot name consumers", channel.name)
        }
        _ => return,
    };
    issues.push(TopologyIssue::Declaration(text));
}

/// What one procedure's declaration may claim about its Entities, its clock
/// and its mailbox.
fn refuse_procedure(proc_decl: &HostProcedureDecl, issues: &mut Vec<TopologyIssue>) {
    match proc_decl.entity_mode {
        HostEntityModeDecl::Static if proc_decl.entity_keys.is_empty() => {
            issues.push(TopologyIssue::Declaration(format!(
                "static procedure `{}` declares no entity_keys",
                proc_decl.name
            )));
        }
        HostEntityModeDecl::Single | HostEntityModeDecl::Dynamic
            if !proc_decl.entity_keys.is_empty() =>
        {
            issues.push(TopologyIssue::Declaration(format!(
                "procedure `{}` declares entity_keys without static mode",
                proc_decl.name
            )));
        }
        HostEntityModeDecl::Dynamic if proc_decl.max_entities == Some(0) => {
            issues.push(TopologyIssue::Declaration(format!(
                "dynamic procedure `{}` declares max_entities 0",
                proc_decl.name
            )));
        }
        _ => {}
    }
    if !proc_decl.entity_keys.is_empty() && !proc_decl.entities_for_overridden {
        issues.push(TopologyIssue::EntitiesForMissing {
            name: proc_decl.name.clone(),
        });
    }
    // Logical time is the durable change seq, which never advances for a
    // procedure that journals nothing — fail fast instead of serving a clock
    // frozen at zero.
    if proc_decl.clock == HostClock::Logical && proc_decl.change_is_nochange {
        issues.push(TopologyIssue::UnservableClock {
            procedure: proc_decl.name.clone(),
            clock_type: clock_name(proc_decl.clock).to_string(),
        });
    }
    if proc_decl.mailbox_capacity == 0 {
        issues.push(TopologyIssue::MailboxCapacityZero {
            procedure: proc_decl.name.clone(),
        });
    } else if proc_decl.mailbox_capacity > MAX_MAILBOX_CAPACITY {
        issues.push(TopologyIssue::MailboxCapacityTooLarge {
            procedure: proc_decl.name.clone(),
            value: proc_decl.mailbox_capacity,
        });
    }
}

/// What an edge may declare about its consumer's Entity mode. The mode is
/// read off the WIRING: a procedure reached by a keyed edge is dynamic, so
/// every other edge into it must name an Entity too.
fn refuse_edge_keying(
    decl: &HostClassDecl,
    channels: &[DerivedChannel],
    issues: &mut Vec<TopologyIssue>,
) {
    let keyed: HashSet<u32> = channels
        .iter()
        .flat_map(|c| c.consumers.iter())
        .filter(|e| e.key)
        .map(|e| e.procedure)
        .collect();
    for channel in channels {
        for edge in &channel.consumers {
            let Some(proc_decl) = decl.procedures.get(edge.procedure as usize) else {
                continue;
            };
            if edge.key && !proc_decl.entity_keys.is_empty() {
                issues.push(TopologyIssue::KeyByOnUnkeyableTarget {
                    channel: channel.name.clone(),
                    procedure: proc_decl.name.clone(),
                });
            }
            if !edge.key && keyed.contains(&edge.procedure) {
                issues.push(TopologyIssue::UnkeyedEdgeIntoDynamicProcedure {
                    channel: channel.name.clone(),
                    procedure: proc_decl.name.clone(),
                });
            }
        }
    }
}

/// The shared graph-shape rule (`sdk-graph-authoring`): one or more entry
/// channels feed the graph, every node reachable from some entry, no cycles.
fn refuse_graph_shape(
    decl: &HostClassDecl,
    channels: &[DerivedChannel],
    issues: &mut Vec<TopologyIssue>,
) {
    if decl.procedures.is_empty() {
        return;
    }
    let edges: Vec<_> = channels
        .iter()
        .filter_map(|c| c.producer.map(|p| (p, c)))
        .flat_map(|(p, c)| c.consumers.iter().map(move |e| (p, e.procedure)))
        .collect();
    let roots: Vec<_> = channels
        .iter()
        .filter(|c| c.kind == HostChannelKind::Entry)
        .flat_map(|c| c.consumers.iter().map(|e| e.procedure))
        .collect();
    let shape = graph_order(
        decl.procedures.len(),
        &edges,
        &roots,
        GraphSeed::EveryIndegreeZero,
    );
    if let Some(node) = shape.on_cycle {
        issues.push(TopologyIssue::Cycle {
            procedure: decl.procedures[node as usize].name.clone(),
        });
    } else if let Some(node) = shape.unreachable {
        issues.push(TopologyIssue::UnreachableProcedure {
            procedure: decl.procedures[node as usize].name.clone(),
        });
    }
}
/// Build the checked declaration's topology and register it.
fn register(derived: Derived) -> Result<(), Error> {
    let Derived {
        channels: derived_channels,
        ack_granularity,
        decl,
    } = derived;
    if let Some(workflow) = &decl.workflow {
        if decl.procedures.is_empty() {
            // Run-sugar: one imperative body, no topology.
            register_workflow_class(&decl, workflow, None, ProcDag::default(), LoopMode::Once);
            return Ok(());
        }
    }
    let class = resolve_class_facts(&decl, ack_granularity);
    let (channels, name_to_id) = build_channels(&decl, &derived_channels);
    let (procedures, procedure_name_to_id) =
        build_procedures(&decl, &class, &channels, &derived_channels);
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
