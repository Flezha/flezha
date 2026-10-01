//! The judgment of an edge that ends in an operator class: a steps class the
//! core holds, judged against its registered topology, and a flow class a
//! builder inlines, judged against what the inlining declared.

use std::sync::Arc;

use crate::channel::{ChannelId, ChannelKind, TaskletId};
use crate::host::TopologyIssue;
use crate::operator::topology::OperatorTopology;

/// An edge into the class `inner` inside the class `outer`, judged: `inner`
/// is registered and runs in steps, `outer` does not, the edge feeds one entry
/// channel (`input`, or the only one declared) and the inner graph ends in at
/// most one tasklet whose output stays inside it (a leaf that writes to a sink
/// is not one). Answers the class, the entry fed and the exit. The one
/// judgment every SDK's builder submits an embedding to.
pub fn judge_embedding(
    outer: &str,
    outer_steps: bool,
    inner: &str,
    input: Option<&str>,
) -> Result<(Arc<OperatorTopology>, ChannelId, Option<TaskletId>), TopologyIssue> {
    let unusable = |value: &str| TopologyIssue::InvalidSetting {
        declaration: format!("the edge of operator '{outer}' into '{inner}'"),
        setting: "operator".to_string(),
        value: value.to_string(),
        expected: "a steps class declared before the class embedding it".to_string(),
    };
    let topology =
        crate::operator::registry::get_class(inner).ok_or_else(|| unusable("undeclared"))?;
    let spec = topology
        .steps
        .as_ref()
        .ok_or_else(|| unusable("a flow class"))?;
    if outer_steps {
        return Err(TopologyIssue::NestedSteps {
            outer: outer.to_string(),
            inner: inner.to_string(),
        });
    }
    let entries = topology.declared_names(ChannelKind::Entry);
    let entry = match (input, entries.as_slice()) {
        (Some(name), _) if entries.contains(&name) => name,
        (None, [only]) => only,
        _ => {
            return Err(TopologyIssue::EmbeddedEntryAmbiguous {
                inner: inner.to_string(),
                channels: entries.join(", "),
            })
        }
    };
    let exits: Vec<TaskletId> = spec
        .dag
        .leaves()
        .map(|node| spec.dag.order[node])
        .filter(|&id| {
            topology
                .tasklet(id)
                .and_then(|t| t.output_channel_id)
                .and_then(|c| topology.channel(c))
                .is_none_or(|c| c.kind == ChannelKind::InterTasklet)
        })
        .collect();
    let names = exits
        .iter()
        .filter_map(|&id| topology.tasklet(id).map(|t| &*t.name));
    if let Some(issue) = exit_issue(inner, names.collect()) {
        return Err(issue);
    }
    let entry = topology
        .lookup_channel_id(entry)
        .expect("a declared entry name");
    let exit = exits.first().copied();
    Ok((topology, entry, exit))
}

/// A flow class a builder inlined, judged: the edge fed exactly the entry
/// `input` names (or the class declared one), and the nodes it added end in
/// at most one exit.
pub fn judge_inlining(
    inner: &str,
    entries: &[&str],
    input: Option<&str>,
    exits: Vec<&str>,
) -> Vec<TopologyIssue> {
    let mut entries = entries.to_vec();
    entries.sort_unstable();
    entries.dedup();
    let chosen = match input {
        Some(input) => entries == [input],
        None => entries.len() == 1,
    };
    let mut issues = Vec::new();
    if !chosen {
        issues.push(TopologyIssue::EmbeddedEntryAmbiguous {
            inner: inner.to_string(),
            channels: entries.join(", "),
        });
    }
    issues.extend(exit_issue(inner, exits));
    issues
}

/// An inlined edge, `declaration`, whose value type is not the one the class
/// declares at `setting` (`input` or `exit`).
pub fn mistyped_inlining(
    declaration: String,
    setting: &str,
    value: &str,
    expected: String,
) -> TopologyIssue {
    TopologyIssue::InvalidSetting {
        declaration,
        setting: setting.to_string(),
        value: value.to_string(),
        expected,
    }
}

fn exit_issue(inner: &str, mut exits: Vec<&str>) -> Option<TopologyIssue> {
    (exits.len() > 1).then(|| {
        exits.sort_unstable();
        TopologyIssue::EmbeddedExitAmbiguous {
            inner: inner.to_string(),
            channels: exits.join(", "),
        }
    })
}
