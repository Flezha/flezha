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
    let mut derived = Chains {
        name: derived_name(channel_decl),
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

    // Terminality is read off the wiring, never off which method was called:
    // a sink with nothing consuming behind it IS the plugin-bound terminal,
    // and a channel carrying both stays what it was declared.
    let kind = if derived.sink.is_some() && derived.consumers.is_empty() {
        HostChannelKind::Plugin
    } else {
        channel_decl.kind
    };

    DerivedChannel {
        index: index as u32,
        name: derived.name,
        kind,
        producer: channel_decl.producer,
        consumers: derived.consumers,
        sink: derived.sink,
    }
}

/// The channel's name: the LAST `named` link if it carries one, else the
/// builder's derived default. Read once for the whole channel — the omission
/// KI-120 closed was a pending name consumed by n−1 terminators, so a name
/// read per terminator is the shape to keep impossible.
fn derived_name(channel_decl: &HostChannelDecl) -> String {
    channel_decl
        .chains
        .iter()
        .flatten()
        .filter_map(|link| match link {
            HostLink::Named { name } => Some(name.clone()),
            _ => None,
        })
        .next_back()
        .unwrap_or_else(|| channel_decl.name.clone())
}

/// Where in the record the walk is: the class it reads procedure names from,
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
        HostLink::Sink { name: descriptor } => {
            if derived.sink.is_some() {
                issues.push(TopologyIssue::DuplicatePluginSink { channel });
            } else {
                derived.sink = Some(descriptor.clone());
            }
        }
        HostLink::Into { procedure } => {
            wire_consumer(walk, &channel, *procedure, shape, derived, issues)
        }
        HostLink::Merge { procedure, group } => {
            walk.merges
                .entry(*group)
                .or_insert_with(|| (*procedure, vec![]))
                .1
                .push(walk.index);
            wire_consumer(walk, &channel, *procedure, shape, derived, issues);
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
    procedure: u32,
    shape: &Shape,
    derived: &mut Chains,
    issues: &mut Vec<TopologyIssue>,
) {
    let Some(proc_decl) = walk.decl.procedures.get(procedure as usize) else {
        issues.push(TopologyIssue::Declaration(format!(
            "channel `{channel}` names consumer {procedure} but the class declares {} procedure(s)",
            walk.decl.procedures.len()
        )));
        return;
    };
    match derived.consumers.iter().find(|e| e.procedure == procedure) {
        Some(existing) if existing.transform || existing.key || shape.transform || shape.key => {
            issues.push(TopologyIssue::EdgeAlreadyWired {
                channel: channel.to_string(),
                procedure: proc_decl.name.clone(),
            });
        }
        Some(_) => {}
        None => derived.consumers.push(DerivedEdge {
            procedure,
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
    for (_, (procedure, sources)) in groups {
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
                procedure: decl
                    .procedures
                    .get(*procedure as usize)
                    .map(|p| p.name.clone())
                    .unwrap_or_default(),
                sources: seen.len(),
            });
        }
    }
}
