//! Stage one and two of the check: the chains a channel was declared with,
//! judged left to right, and the consumers, kind, name and sink they
//! determine — read off in the same pass, because deriving IS reading them.
//!
//! This is the stage new to the core. The declaration used to carry a
//! builder's CONCLUSION (`consumers`) instead of its evidence, so every rule
//! about the order an author called things in had to live in the builder, once
//! per SDK.

use std::collections::{HashMap, HashSet};

use super::decl::{HostChannelDecl, HostChannelKind, HostClassDecl, HostLink};
use super::issue::TopologyIssue;
use super::register::{DerivedChannel, DerivedEdge};

/// Stage 1 and 2, per channel and in one walk: the chain rules judged
/// left to right while the consumers, the kind, the name and the sink they
/// determine are read off.
pub(super) fn derive_channels(
    decl: &HostClassDecl,
    issues: &mut Vec<TopologyIssue>,
) -> Vec<DerivedChannel> {
    let mut merges: HashMap<u32, (u32, Vec<usize>)> = HashMap::new();
    let mut channels = Vec::with_capacity(decl.channels.len());
    for (index, channel_decl) in decl.channels.iter().enumerate() {
        channels.push(derive_channel(
            decl,
            index,
            channel_decl,
            &mut merges,
            issues,
        ));
    }
    refuse_merges(decl, &channels, &merges, issues);
    channels
}

fn derive_channel(
    decl: &HostClassDecl,
    index: usize,
    channel_decl: &HostChannelDecl,
    merges: &mut HashMap<u32, (u32, Vec<usize>)>,
    issues: &mut Vec<TopologyIssue>,
) -> DerivedChannel {
    // The two ways a record can name a channel, read once. The name the
    // channel takes, the refusal two declarations raise, and which rung
    // supplied the name are three readings of this one pair.
    let (named, from_sink) = naming_declarations(channel_decl);
    refuse_double_name(channel_decl, named, from_sink, issues);
    let mut derived = Chains {
        // Three rungs: the LAST `named` link, else the name a `sink` link
        // carries, else the builder's derived default. The first two never
        // both apply — a channel a record names twice is refused above.
        name: named
            .or(from_sink)
            .cloned()
            .unwrap_or_else(|| channel_decl.name.clone()),
        sink: None,
        consumers: Vec::new(),
    };
    let mut walk = Walk {
        decl,
        index,
        chain: 0,
        merges,
    };
    for (chain, links) in channel_decl.chains.iter().enumerate() {
        walk.chain = chain;
        walk_chain(&mut walk, links, &mut derived, issues);
    }

    DerivedChannel {
        index: index as u32,
        kind: derived_kind(channel_decl, &derived),
        name: derived.name,
        producer: channel_decl.producer,
        consumers: derived.consumers,
        sink: derived.sink,
        named_by_sink: named.is_none() && from_sink.is_some(),
    }
}

/// Terminality is read off the wiring, never off which method was called: a
/// sink with nothing consuming behind it IS the plugin-bound terminal, and a
/// channel carrying both stays what it was declared.
fn derived_kind(channel_decl: &HostChannelDecl, derived: &Chains) -> HostChannelKind {
    if derived.sink.is_some() && derived.consumers.is_empty() {
        HostChannelKind::Plugin
    } else {
        channel_decl.kind
    }
}

/// A channel whose record carries both a `named` link and a `sink` that names
/// it has been named twice. Refused rather than resolved by precedence, and
/// refused even when the two strings agree: an exception for the consistent
/// case would leave a redundant pair nobody can remember the rule for.
fn refuse_double_name(
    channel_decl: &HostChannelDecl,
    named: Option<&String>,
    from_sink: Option<&String>,
    issues: &mut Vec<TopologyIssue>,
) {
    if let (Some(named), Some(sink)) = (named, from_sink) {
        issues.push(TopologyIssue::ChannelNamedTwice {
            // The record's own `name` field: neither declared name wins here,
            // and the author still has to be told which channel this is. The
            // core does not choose between the two, it reports the name the
            // declaration arrived with.
            channel: channel_decl.name.clone(),
            named: named.clone(),
            sink: sink.clone(),
        });
    }
}

/// The two ways a record can name a channel: the LAST `named` link, and the
/// FIRST `sink` that carries a name (a second sink is its own refusal).
///
/// The one reader of a channel's naming links. The name the channel takes, the
/// refusal two declarations raise, and which rung supplied the name all come
/// off this pair, so they cannot be read from different links. The omission
/// KI-120 closed was a pending name consumed by n−1 terminators, and reading
/// the pair once per channel is what keeps that shape impossible.
fn naming_declarations(channel_decl: &HostChannelDecl) -> (Option<&String>, Option<&String>) {
    let (mut named, mut sink) = (None, None);
    for link in channel_decl.chains.iter().flatten() {
        match link {
            HostLink::Named { name } => named = Some(name),
            HostLink::Sink { name: Some(name) } => sink = sink.or(Some(name)),
            _ => {}
        }
    }
    (named, sink)
}

/// Where in the record the walk is: the class it reads tasklet names from,
/// the channel it is on, the chain it is in, and the merge groups it fills.
/// Carried together because every step of the walk needs all four.
struct Walk<'a> {
    decl: &'a HostClassDecl,
    index: usize,
    chain: usize,
    merges: &'a mut HashMap<u32, (u32, Vec<usize>)>,
}

/// What one channel's chains have determined so far. The sink and the
/// consumers accumulate ACROSS chains, which is what makes a second `sink`
/// and a re-declared edge decidable at all.
struct Chains {
    name: String,
    sink: Option<String>,
    consumers: Vec<DerivedEdge>,
}

/// One fluent sequence, left to right. The links are judged in the order the
/// author called them, and what they determine is read off in the same pass.
fn walk_chain(
    walk: &mut Walk<'_>,
    links: &[HostLink],
    derived: &mut Chains,
    issues: &mut Vec<TopologyIssue>,
) {
    let channel = derived.name.clone();
    let mut shape = Shape::default();

    for link in links {
        // A link declared behind a terminator would reshape a value already
        // serialized or already delivered.
        if (shape.closed || derived.sink.is_some())
            && matches!(link, HostLink::Transform | HostLink::Key)
        {
            issues.push(TopologyIssue::TransformAfterTerminal {
                channel: channel.clone(),
            });
            continue;
        }
        match link {
            HostLink::Transform => {
                if shape.key {
                    issues.push(TopologyIssue::TransformAfterKeyBy {
                        channel: channel.clone(),
                    });
                }
                shape.transform = true;
            }
            HostLink::Key => {
                if shape.key {
                    issues.push(TopologyIssue::DuplicateKeyBy {
                        channel: channel.clone(),
                    });
                }
                shape.key = true;
            }
            HostLink::Named { .. } => {}
            _ => close_chain(walk, link, derived, &mut shape, issues),
        }
    }

    // A chain is stored on the consumer edge it terminates in, so one nothing
    // closed would silently do nothing.
    if !shape.closed && (shape.transform || shape.key) {
        issues.push(TopologyIssue::DanglingTransformChain { channel });
    }
}

/// What one chain has declared so far: the links that reshape a value, and
/// whether anything has closed it.
#[derive(Default)]
struct Shape {
    transform: bool,
    key: bool,
    closed: bool,
}

/// The terminators: a sink, an edge, a merge source, a branch arity. Each ends
/// the chain, and the first three are what a channel's derivation is read off.
fn close_chain(
    walk: &mut Walk<'_>,
    link: &HostLink,
    derived: &mut Chains,
    shape: &mut Shape,
    issues: &mut Vec<TopologyIssue>,
) {
    let channel = derived.name.clone();
    match link {
        // The sink's descriptor IS the channel's name: `sink("x")` named the
        // channel `x` two rungs up, and `sink()` leaves it derived. One string,
        // so local mode's "the sink's declared name" and the wire's channel
        // name cannot be two different answers.
        HostLink::Sink { .. } => {
            if derived.sink.is_some() {
                issues.push(TopologyIssue::DuplicatePluginSink { channel });
            } else {
                derived.sink = Some(channel);
            }
        }
        HostLink::Into { tasklet } => {
            wire_consumer(walk, &channel, *tasklet, shape, derived, issues)
        }
        HostLink::Merge { tasklet, group } => {
            walk.merges
                .entry(*group)
                .or_insert_with(|| (*tasklet, vec![]))
                .1
                .push(walk.index);
            wire_consumer(walk, &channel, *tasklet, shape, derived, issues);
        }
        HostLink::Branch { arms } => {
            if *arms < 2 {
                issues.push(TopologyIssue::BranchTooFewArms { arms: *arms });
            }
        }
        // The reshaping links never reach here.
        HostLink::Transform | HostLink::Key | HostLink::Named { .. } => return,
    }
    shape.closed = true;
}

/// Close one chain onto a consumer edge. Re-declaring an existing bare edge
/// is the legal fan-in no-op; re-declaring one where either side carries a
/// transform or a key is refused rather than silently keeping the first.
fn wire_consumer(
    walk: &Walk<'_>,
    channel: &str,
    tasklet: u32,
    shape: &Shape,
    derived: &mut Chains,
    issues: &mut Vec<TopologyIssue>,
) {
    let Some(task_decl) = walk.decl.tasklets.get(tasklet as usize) else {
        issues.push(TopologyIssue::ConsumerOutOfRange {
            channel: channel.to_string(),
            consumer: tasklet,
            tasklets: walk.decl.tasklets.len(),
        });
        return;
    };
    match derived.consumers.iter().find(|e| e.tasklet == tasklet) {
        Some(existing) if existing.transform || existing.key || shape.transform || shape.key => {
            issues.push(TopologyIssue::EdgeAlreadyWired {
                channel: channel.to_string(),
                tasklet: task_decl.name.clone(),
            });
        }
        Some(_) => {}
        None => derived.consumers.push(DerivedEdge {
            tasklet,
            chain: walk.chain,
            transform: shape.transform,
            key: shape.key,
        }),
    }
}

/// A merge consumes two or more distinct channels; one source is a plain
/// `.into(...)` and a repeated one has no second mapping to carry.
fn refuse_merges(
    decl: &HostClassDecl,
    channels: &[DerivedChannel],
    merges: &HashMap<u32, (u32, Vec<usize>)>,
    issues: &mut Vec<TopologyIssue>,
) {
    let mut groups: Vec<_> = merges.iter().collect();
    groups.sort_by_key(|(group, _)| **group);
    for (_, (tasklet, sources)) in groups {
        let mut seen = HashSet::new();
        for &source in sources {
            if !seen.insert(source) {
                issues.push(TopologyIssue::DuplicateMergeSource {
                    channel: channels[source].name.clone(),
                });
            }
        }
        if seen.len() < 2 {
            issues.push(TopologyIssue::MergeTooFewSources {
                tasklet: decl
                    .tasklets
                    .get(*tasklet as usize)
                    .map(|p| p.name.clone())
                    .unwrap_or_default(),
                sources: seen.len(),
            });
        }
    }
}
