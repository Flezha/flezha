//! Judging a host class: the record every SDK submits, the check that reads
//! it, and the registration that follows. `check` holds every named refusal
//! and every derivation rule once, so a builder records what its author
//! declared and decides nothing. Every refusal is a [`TopologyIssue`] and
//! answers its `topology.*` code.
//!
//! What a validated record BECOMES — the descriptors, the facts, the DAG — is
//! [`super::build`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use flz_transport::service::payloader::MAX_METADATA_FIELD_LEN;

use super::build::{
    bounded_spec, build_channels, build_host_dag, build_tasklets, resolve_class_facts,
};
use super::chains::derive_channels;
use super::decl::{
    HostAckGranularity, HostBoundedDecl, HostCellModeDecl, HostChannelKind, HostCheckpointOutputs,
    HostClassDecl, HostClock, HostLoopMode, HostOverflow, HostTaskletDecl,
};
use super::issue::{render_issues, TopologyIssue};
use crate::bounded::spec::{graph_order, GraphSeed, TaskDag};
use crate::bounded::LoopMode;
use crate::error::Error;
use crate::operator::topology::OperatorTopology;
use crate::ErrorModel;

/// Hard upper bound on a per-Cell mailbox: ~1M items is past sanity for any
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
    /// The last `named` link if the record carried one, else the name a `sink`
    /// link carried, else the builder's derived default. Read once, for every
    /// terminator (KI-120).
    pub name: String,
    pub kind: HostChannelKind,
    pub producer: Option<u32>,
    pub consumers: Vec<DerivedEdge>,
    /// The plugin descriptor this channel serializes onto.
    pub sink: Option<String>,
    /// The channel ends in `response()`: its outputs answer the caller whose
    /// input caused them. A plugin end like a sink, never beside one.
    #[serde(skip)]
    pub response: bool,
    /// Which rung of [`name`](Self::name) answered: a `sink(...)` with no
    /// `named(...)` beside it. Recorded where the naming links are read rather
    /// than walked again, so a refusal about a name and the name itself cannot
    /// come from different links. Not part of the derived answer a host parses.
    #[serde(skip)]
    pub named_by_sink: bool,
}

#[derive(Debug, serde::Serialize)]
pub struct DerivedEdge {
    pub tasklet: u32,
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
    register_host_class(parse_decl(bytes).map_err(|issue| Error::Declaration(issue.to_error()))?)
}

/// The body of `flz_core_check_class`: the verdict and the derived
/// declaration, with nothing registered.
pub fn check_class_json(bytes: &[u8]) -> Result<Derived, Vec<TopologyIssue>> {
    check(parse_decl(bytes).map_err(|issue| vec![issue])?)
}

fn parse_decl(bytes: &[u8]) -> Result<HostClassDecl, TopologyIssue> {
    serde_json::from_slice(bytes).map_err(|e| TopologyIssue::DeclarationUnreadable {
        reason: e.to_string(),
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

    let channels = derive_channels(&decl, &mut issues);
    refuse_names(&decl, &channels, &mut issues);
    for task_decl in &decl.tasklets {
        refuse_tasklet(task_decl, &mut issues);
    }
    for channel in &channels {
        refuse_channel(&decl, channel, &mut issues);
    }
    refuse_edge_keying(&decl, &channels, &mut issues);
    refuse_graph_shape(&decl, &channels, &mut issues);

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

/// One namespace per operator for channels, one for nodes: a derived name,
/// a `named(...)` override and an entry name all collide here. A name also has
/// to fit the frame it rides, and this is where the author can still be told.
fn refuse_names(
    decl: &HostClassDecl,
    channels: &[DerivedChannel],
    issues: &mut Vec<TopologyIssue>,
) {
    // Per name, how many of the channels claiming it were named by their own
    // `sink(...)`: two of those is a shape that looks nothing like a naming
    // collision to the author who wrote it, so it is refused under its own
    // name rather than as a bare duplicate.
    let mut from_sink: HashMap<&str, usize> = HashMap::new();
    for channel in channels.iter().filter(|c| c.named_by_sink) {
        *from_sink.entry(channel.name.as_str()).or_default() += 1;
    }
    let mut seen = HashSet::new();
    for channel in channels {
        if !seen.insert(channel.name.as_str()) {
            issues.push(if from_sink.get(channel.name.as_str()) >= Some(&2) {
                TopologyIssue::DuplicateSinkName {
                    channel: channel.name.clone(),
                }
            } else {
                TopologyIssue::DuplicateChannel(channel.name.clone())
            });
        }
        if channel.name.len() > MAX_METADATA_FIELD_LEN {
            issues.push(TopologyIssue::ChannelNameTooLong {
                channel: channel.name.clone(),
                len: channel.name.len(),
                max: MAX_METADATA_FIELD_LEN,
            });
        }
    }
    let mut seen_tasks = HashSet::new();
    for task_decl in &decl.tasklets {
        if !seen_tasks.insert(task_decl.name.as_str()) {
            issues.push(TopologyIssue::NodeIdentityCollision {
                name: task_decl.name.clone(),
            });
        }
    }
}

/// The class-level refusals.
fn refuse_class_facts(decl: &HostClassDecl, issues: &mut Vec<TopologyIssue>) {
    if !crate::header::sdk_lang_allocated(decl.sdk_lang) {
        issues.push(TopologyIssue::UnknownSdkLang {
            sdk_lang: decl.sdk_lang,
        });
    }
    if crate::header::pack_version(&decl.sdk_version) == 0 {
        issues.push(TopologyIssue::UnusableSdkVersion {
            factory_id: decl.factory_id.clone(),
            sdk_version: decl.sdk_version.clone(),
        });
    }
    if decl.tasklets.is_empty() {
        issues.push(TopologyIssue::NoTasklets);
    }
    if decl.sleep_when_idle && !decl.persistent {
        issues.push(TopologyIssue::SleepWithoutPersistence {
            factory_id: decl.factory_id.clone(),
        });
    }
    // Each kind is judged on its own settings; a setting of the other kind is
    // refused by its own name rather than by the rule that assumes its kind.
    match &decl.bounded {
        Some(_) => refuse_bounded_facts(decl, issues),
        None => {
            if decl.sync_durability {
                issues.push(TopologyIssue::SyncDurabilityOnUnbounded {
                    factory_id: decl.factory_id.clone(),
                });
            }
            refuse_tier_facts(decl, issues);
        }
    }
}

fn refuse_bounded_facts(decl: &HostClassDecl, issues: &mut Vec<TopologyIssue>) {
    if decl.effectively_once {
        issues.push(TopologyIssue::EffectivelyOnceOnBounded);
    }
    if decl.correction_window_ms.is_some() {
        issues.push(TopologyIssue::CorrectionWindowOnBounded);
    }
    if derive_ack_granularity(decl) == HostAckGranularity::Checkpoint {
        issues.push(TopologyIssue::CheckpointOnBounded);
    }
    if decl.sync_durability && !decl.persistent {
        issues.push(TopologyIssue::SyncDurabilityWithoutPersistence);
    }
}

/// The tier, the checkpoint interval and the correction window each need
/// what they rest on declared with them.
fn refuse_tier_facts(decl: &HostClassDecl, issues: &mut Vec<TopologyIssue>) {
    if decl.effectively_once && !decl.persistent {
        issues.push(TopologyIssue::EffectivelyOnceWithoutPersistence);
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
        issues.push(TopologyIssue::CheckpointMaxDelayZero);
    }
    if decl.correction_window_ms.is_some() && !decl.effectively_once {
        issues.push(TopologyIssue::CorrectionWithoutTier);
    }
    if decl.correction_window_ms == Some(0) {
        issues.push(TopologyIssue::CorrectionWindowZero);
    }
    if decl.correction_window_ms.is_some() {
        for task_decl in &decl.tasklets {
            if task_decl.clock != HostClock::EventTime {
                issues.push(TopologyIssue::CorrectionWithoutEventTime {
                    tasklet: task_decl.name.clone(),
                    clock_type: task_decl.clock.name().to_string(),
                });
            }
        }
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
    // A sink de-duplicates by a natural key; an answer is identified by its
    // call, so a response end needs none.
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
        if producer as usize >= decl.tasklets.len() {
            issues.push(TopologyIssue::ProducerOutOfRange {
                channel: channel.name.clone(),
                producer,
                tasklets: decl.tasklets.len(),
            });
        }
    }
    refuse_channel_ends(channel, issues);
}

/// Each channel kind names exactly the ends its kind has. A produced
/// inter_task channel with no consumer is NOT one of them: it is a leaf's
/// retained output — what the typed SDK's `sealed()` declares, what a saga
/// archive leaf's undo reverses, what a handoff-chained loop feeds back — and
/// the bounded run, not another tasklet, is what reads it.
fn refuse_channel_ends(channel: &DerivedChannel, issues: &mut Vec<TopologyIssue>) {
    let channel_name = channel.name.clone();
    issues.push(match channel.kind {
        HostChannelKind::Entry if channel.producer.is_some() => {
            TopologyIssue::EntryChannelWithProducer {
                channel: channel_name,
            }
        }
        HostChannelKind::Entry if channel.consumers.is_empty() => {
            TopologyIssue::EntryChannelWithoutConsumer {
                channel: channel_name,
            }
        }
        HostChannelKind::InterTask if channel.producer.is_none() => {
            TopologyIssue::InterTaskChannelWithoutProducer {
                channel: channel_name,
            }
        }
        HostChannelKind::Plugin if !channel.consumers.is_empty() => {
            TopologyIssue::PluginChannelWithConsumers {
                channel: channel_name,
            }
        }
        _ => return,
    });
}

/// What one tasklet's declaration may claim about its Cells, its clock
/// and its mailbox.
fn refuse_tasklet(task_decl: &HostTaskletDecl, issues: &mut Vec<TopologyIssue>) {
    match task_decl.cell_mode {
        HostCellModeDecl::Static if task_decl.cell_keys.is_empty() => {
            issues.push(TopologyIssue::StaticTaskletWithoutCellKeys {
                tasklet: task_decl.name.clone(),
            });
        }
        HostCellModeDecl::Single | HostCellModeDecl::Dynamic if !task_decl.cell_keys.is_empty() => {
            issues.push(TopologyIssue::CellKeysWithoutStaticMode {
                tasklet: task_decl.name.clone(),
            });
        }
        HostCellModeDecl::Dynamic if task_decl.max_cells == Some(0) => {
            issues.push(TopologyIssue::MaxCellsZero {
                tasklet: task_decl.name.clone(),
            });
        }
        _ => {}
    }
    if !task_decl.cell_keys.is_empty() && !task_decl.cells_for_overridden {
        issues.push(TopologyIssue::CellsForMissing {
            name: task_decl.name.clone(),
        });
    }
    // Logical time is the durable change seq, which never advances for a
    // tasklet that journals nothing — fail fast instead of serving a clock
    // frozen at zero.
    if task_decl.clock == HostClock::Logical && task_decl.change_is_nochange {
        issues.push(TopologyIssue::UnservableClock {
            tasklet: task_decl.name.clone(),
            clock_type: task_decl.clock.name().to_string(),
        });
    }
    if task_decl.mailbox_capacity == 0 {
        issues.push(TopologyIssue::MailboxCapacityZero {
            tasklet: task_decl.name.clone(),
        });
    } else if task_decl.mailbox_capacity > MAX_MAILBOX_CAPACITY {
        issues.push(TopologyIssue::MailboxCapacityTooLarge {
            tasklet: task_decl.name.clone(),
            value: task_decl.mailbox_capacity,
        });
    }
}

/// What an edge may declare about its consumer's Cell mode. The mode is
/// read off the WIRING: a tasklet reached by a keyed edge is dynamic, so
/// every other edge into it must name a Cell too.
fn refuse_edge_keying(
    decl: &HostClassDecl,
    channels: &[DerivedChannel],
    issues: &mut Vec<TopologyIssue>,
) {
    let keyed: HashSet<u32> = channels
        .iter()
        .flat_map(|c| c.consumers.iter())
        .filter(|e| e.key)
        .map(|e| e.tasklet)
        .collect();
    for channel in channels {
        for edge in &channel.consumers {
            let Some(task_decl) = decl.tasklets.get(edge.tasklet as usize) else {
                continue;
            };
            if edge.key && !task_decl.cell_keys.is_empty() {
                issues.push(TopologyIssue::KeyByOnUnkeyableTarget {
                    channel: channel.name.clone(),
                    tasklet: task_decl.name.clone(),
                });
            }
            if !edge.key && keyed.contains(&edge.tasklet) {
                issues.push(TopologyIssue::UnkeyedEdgeIntoDynamicTasklet {
                    channel: channel.name.clone(),
                    tasklet: task_decl.name.clone(),
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
    if decl.tasklets.is_empty() {
        return;
    }
    let edges: Vec<_> = channels
        .iter()
        .filter_map(|c| c.producer.map(|p| (p, c)))
        .flat_map(|(p, c)| c.consumers.iter().map(move |e| (p, e.tasklet)))
        .collect();
    let roots: Vec<_> = channels
        .iter()
        .filter(|c| c.kind == HostChannelKind::Entry)
        .flat_map(|c| c.consumers.iter().map(|e| e.tasklet))
        .collect();
    let shape = graph_order(
        decl.tasklets.len(),
        &edges,
        &roots,
        GraphSeed::EveryIndegreeZero,
    );
    if let Some(node) = shape.on_cycle {
        issues.push(TopologyIssue::Cycle {
            tasklet: decl.tasklets[node as usize].name.clone(),
        });
    } else if let Some(node) = shape.unreachable {
        issues.push(TopologyIssue::UnreachableTasklet {
            tasklet: decl.tasklets[node as usize].name.clone(),
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
    let class = resolve_class_facts(&decl, ack_granularity);
    let (channels, name_to_id) = build_channels(&decl, &derived_channels);
    let (tasklets, tasklet_name_to_id) =
        build_tasklets(&decl, &class, &channels, &derived_channels);
    let mut topology = OperatorTopology {
        sdk_lang: decl.sdk_lang,
        channels,
        name_to_id,
        tasklets,
        tasklet_name_to_id,
        factory_id: class.factory_id,
        persistence_enabled_const: decl.persistent,
        sleep_when_idle: decl.sleep_when_idle,
        effectively_once: decl.effectively_once,
        correction: class.correction,
        ack_granularity: class.ack_granularity,
        bounded: None,
    };
    if let Some(bounded) = &decl.bounded {
        topology.bounded = Some(structured_bounded(&decl, bounded, &topology)?);
    }
    crate::operator::registry::register_class(class.factory_id, Arc::new(topology));
    Ok(())
}

/// A durable handoff-chained loop seeds iteration N+1 from iteration N's
/// terminal handoff, and only an inter-tasklet channel carries one: the
/// journal codec rides that kind and no other (`build::build_channel`). A
/// terminal that owns no output channel, or one whose only wiring is a plugin
/// sink, would therefore ship an empty handoff at the iteration commit, and
/// `resume::seed_for_mode` reads an empty seed as convergence — the loop stops
/// and the run reports completed, truncated and silent.
///
/// It lives here rather than in each SDK because this is the one registration
/// every host reaches: Java, Node, Python, Ruby and PHP all declare through
/// `declare_class`. The typed Rust SDK refuses the same shape in its own
/// vocabulary, because it never passes through here.
fn refuse_loop_terminal_without_handoff(
    decl: &HostClassDecl,
    bounded: &HostBoundedDecl,
    topology: &OperatorTopology,
    dag: &TaskDag,
) -> Result<(), Error> {
    if !decl.persistent || bounded.loop_mode != HostLoopMode::Chained {
        return Ok(());
    }
    match dag.terminal_without_handoff(topology) {
        Some(terminal) => Err(Error::Declaration(
            TopologyIssue::ChainedLoopTerminalWithoutHandoff {
                tasklet: terminal.name.to_string(),
            }
            .to_error(),
        )),
        None => Ok(()),
    }
}

/// A structured chain: its DAG from the topology, a chained loop decided
/// against it.
fn structured_bounded(
    decl: &HostClassDecl,
    bounded: &HostBoundedDecl,
    topology: &OperatorTopology,
) -> Result<crate::bounded::spec::BoundedSpec, Error> {
    let dag = build_host_dag(topology)?;
    if bounded.loop_mode == HostLoopMode::Chained {
        if let Some((channel_id, branching)) = dag.branching_chain() {
            return Err(Error::Declaration(
                TopologyIssue::BranchingChainedLoop {
                    channel: topology
                        .channel(channel_id)
                        .map(|c| c.name.to_string())
                        .unwrap_or_default(),
                    leaves: branching.leaves(&dag).count(),
                }
                .to_error(),
            ));
        }
    }
    refuse_loop_terminal_without_handoff(decl, bounded, topology, &dag)?;
    let loop_mode = match bounded.loop_mode {
        HostLoopMode::Once => LoopMode::Once,
        HostLoopMode::PerInput => LoopMode::PerInput,
        HostLoopMode::Chained => LoopMode::Chained,
    };
    Ok(bounded_spec(decl, bounded, dag, loop_mode))
}
